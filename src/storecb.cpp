/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "fio.h"
#include "buffer.h"
#include "storecb.h"
#include "logmgr.h"
#include "classifier.h"
#include "startup.h"

using namespace MVStoreKernel;

RC StoreCB::open(StoreCtx *ctx,const char *fname,const char *pwd,bool fForce)
{
	assert(ctx!=NULL&&ctx->fileMgr!=NULL&&ctx->cryptoMgr!=NULL&&fname!=NULL);
	RC rc = RC_OK; FileID fid = 0; TIMESTAMP ts; getTimestamp(ts);
	if ((rc = ctx->fileMgr->open(fid,fname))!=RC_OK) return rc;
	size_t lPage=getPageSize(); ctx->bufSize=(uint32_t)ceil(STORECBSIZE,lPage);
	StoreCB *theCB = ctx->theCB = (StoreCB*)allocAligned(ctx->bufSize,lPage);
	if (theCB==NULL) rc=RC_NORESOURCES;
	else if ((rc=ctx->fileMgr->io(FIO_READ,PageIDFromPageNum(fid,0),theCB,ctx->bufSize))==RC_OK) {
		const char *key=pwd!=NULL&&*pwd!='\0'?pwd:DEFAULTHMACKEY; byte keybuf[PWD_ENC_SIZE];
		memcpy(keybuf,theCB->hdr.salt1,sizeof(uint32_t)+PWD_LSALT);
		PWD_ENCRYPT crypt((byte*)key,strlen(key),keybuf); 
		memcpy(ctx->HMACKey0,crypt.encrypted()+sizeof(uint32_t)+PWD_LSALT,HMAC_KEY_SIZE);
		HMAC hmac(ctx->HMACKey0,HMAC_KEY_SIZE); hmac.add((byte*)theCB+HMAC_SIZE,STORECBSIZE-HMAC_SIZE);
		if (memcmp(theCB->hdr.hmac,hmac.result(),HMAC_SIZE)!=0) rc=RC_CORRUPTED;
		else {
			if (pwd!=NULL && *pwd!='\0') {
				memcpy(keybuf,theCB->hdr.salt2,sizeof(uint32_t)+PWD_LSALT);
				PWD_ENCRYPT crypt2((byte*)pwd,strlen(pwd),keybuf); 
				memcpy(ctx->encKey0,crypt2.encrypted()+sizeof(uint32_t)+PWD_LSALT,ENC_KEY_SIZE);
				AES aes(ctx->encKey0,ENC_KEY_SIZE);
				aes.decrypt((byte*)theCB+sizeof(CryptInfo),STORECBSIZE-sizeof(CryptInfo),(uint32_t*)theCB->hdr.salt1+1);
				if ((ctx->theCBEnc=(StoreCB*)allocAligned(ctx->bufSize,lPage))==NULL) {close(ctx); return RC_NORESOURCES;}
			}
			char buf[40]; int l; convDateTime(NULL,theCB->timestamp,buf,l); report(MSG_INFO,"Store timestamp: %s UTC\n",buf);
			if (theCB->magic!=STORECBMAGIC || theCB->timestamp>ts && !fForce) rc=RC_CORRUPTED;
			else if (theCB->version/100!=STORE_VERSION/100 || theCB->version%100>STORE_VERSION%100) rc=RC_VERSION;
			else {
				ctx->storeID=theCB->storeID; ctx->keyPrefix=StoreCtx::genPrefix(theCB->storeID);
				memcpy(ctx->HMACKey,theCB->HMACKey,HMAC_KEY_SIZE);
				memcpy(ctx->encKey,theCB->encKey,ENC_KEY_SIZE);
				ctx->fEncrypted = theCB->fIsEncrypted!=0;
			}
		}
	}
	if (rc!=RC_OK) close(ctx);
	return rc;
}

RC StoreCB::create(StoreCtx *ctx,const char *fname,const StoreCreationParameters& cpar)
{
	assert(ctx!=NULL&&ctx->fileMgr!=NULL&&ctx->cryptoMgr!=NULL&&fname!=NULL);
	RC rc=RC_OK; FileID fid=0; off64_t addr;
	if ((rc=ctx->fileMgr->open(fid,fname,FIO_CREATE|FIO_NEW))!=RC_OK) {
		if (rc!=RC_ALREADYEXISTS) return rc;
		bool fInit=open(ctx,fname,cpar.password,false)==RC_OK&&ctx->theCB->state==SST_INIT;
		if (ctx->theCB!=NULL) {freeAligned(ctx->theCB); ctx->theCB=NULL;}
		if (!fInit) {ctx->fileMgr->close(0); return rc;}
		ctx->fileMgr->truncate(0,0);
	}
	ctx->bufSize=(uint32_t)ceil(STORECBSIZE,cpar.pageSize);
	StoreCB *theCB=ctx->theCB=(StoreCB*)allocAligned(ctx->bufSize,cpar.pageSize);
	if (theCB==NULL) rc=RC_NORESOURCES;
	else if ((rc=ctx->fileMgr->allocateExtent(fid,1,addr))==RC_OK) {
		assert(addr==0); assert(sizeof(StoreCB)<=STORECBSIZE);
		memset(theCB,0,ctx->bufSize);
		theCB->magic = STORECBMAGIC;
		theCB->version = STORE_VERSION;
		theCB->lPage = cpar.pageSize;
		theCB->nPagesPerExtent = cpar.fileExtentSize;
		theCB->maxSize = cpar.maxSize;
		theCB->logSegSize = (uint32_t)cpar.logSegSize;	// round to sectors (**2?)
		theCB->nMaster = cpar.nControlRecords;
		theCB->pctFree = cpar.pctFree<=0.f?DEFAULTPCTFREE:cpar.pctFree;
		theCB->storeID = ctx->storeID = cpar.storeId;
		theCB->fIsEncrypted = ushort(cpar.fEncrypted ? ~0 : 0);
		theCB->filler = 0;
		if (cpar.fEncrypted) {
			ctx->cryptoMgr->randomBytes(theCB->encKey,ENC_KEY_SIZE);
			memcpy(ctx->encKey,theCB->encKey,ENC_KEY_SIZE); ctx->fEncrypted=true;
		}
		ctx->cryptoMgr->randomBytes(theCB->HMACKey,HMAC_KEY_SIZE);
		memcpy(ctx->HMACKey,theCB->HMACKey,HMAC_KEY_SIZE);
		const char *key=cpar.password!=NULL&&*cpar.password!='\0'?cpar.password:DEFAULTHMACKEY;
		PWD_ENCRYPT crypt((byte*)key,strlen(key));
		memcpy(theCB->hdr.salt1,crypt.encrypted(),sizeof(uint32_t)+PWD_LSALT);
		memcpy(ctx->HMACKey0,crypt.encrypted()+sizeof(uint32_t)+PWD_LSALT,HMAC_KEY_SIZE);
		if (cpar.password!=NULL && *cpar.password!='\0') {
			if ((ctx->theCBEnc=(StoreCB*)allocAligned(ctx->bufSize,cpar.pageSize))==NULL) rc=RC_NORESOURCES;
			else {
				PWD_ENCRYPT crypt2((byte*)cpar.password,strlen(cpar.password));
				memcpy(theCB->hdr.salt2,crypt2.encrypted(),sizeof(uint32_t)+PWD_LSALT);
				memcpy(ctx->encKey0,crypt2.encrypted()+sizeof(uint32_t)+PWD_LSALT,ENC_KEY_SIZE);
			}
		}
		ctx->keyPrefix = StoreCtx::genPrefix(cpar.storeId);
		theCB->nDataFiles = 1;
		theCB->state = ~0u;
		theCB->xPropID = PROP_SPEC_LAST;
		for (int i=0; i<MA_ALL; i++) theCB->mapRoots[i] = INVALID_PAGEID;
		if (rc==RC_OK) rc=update(ctx,false);
	}
	if (rc!=RC_OK) close(ctx);
	return rc;
}

void StoreCB::preload(StoreCtx *ctx) const
{
	static const PGID mapRootsPGIDs[MA_ALL] = {
		PGID_INDEX,PGID_INDEX,PGID_INDEX,PGID_INDEX,PGID_INDEX,PGID_INDEX,PGID_INDEX,
		PGID_INDEX,PGID_INDEX,PGID_HEAPDIR,PGID_HEAPDIR,PGID_HEAPDIR,PGID_HEAPDIR,
		PGID_ALL,PGID_ALL,PGID_ALL,PGID_ALL,PGID_ALL,PGID_ALL,PGID_ALL,PGID_ALL};
	PageID pages[MA_ALL]; PageMgr *pmgrs[MA_ALL]; ulong cnt=0;
	for (ulong i=0; i<MA_ALL; i++) if (mapRoots[i]!=INVALID_PAGEID)
		{pages[cnt]=mapRoots[i]; pmgrs[cnt]=ctx->getPageMgr(mapRootsPGIDs[i]); cnt++;}
	if (cnt>0) ctx->bufMgr->prefetch(pages,cnt,NULL,pmgrs);
}

RC StoreCB::update(StoreCtx *ctx,bool fSetLogEnd)
{
	StoreCB *theCB=ctx->theCB; RC rc; assert(theCB!=NULL);
	if (theCB->state==~0u) theCB->state=SST_INIT;
	else if (theCB->state==SST_INIT) return RC_OK;
	if (fSetLogEnd) {
		if ((rc=ctx->logMgr->flushTo(ctx->cbLSN,&theCB->logEnd))!=RC_OK) return rc;
		theCB->lastTXID=ctx->txMgr->getLastTXID();
	}
	RWLockP lck(&ctx->cbLock,RW_X_LOCK); getTimestamp(theCB->timestamp);
	if (ctx->theCBEnc!=NULL) {
		memcpy(theCB=ctx->theCBEnc,ctx->theCB,STORECBSIZE); AES aes(ctx->encKey0,ENC_KEY_SIZE);
		aes.encrypt((byte*)theCB+sizeof(CryptInfo),STORECBSIZE-sizeof(CryptInfo),(uint32_t*)theCB->hdr.salt1+1);
	}
	HMAC hmac(ctx->HMACKey0,HMAC_KEY_SIZE);
	hmac.add((byte*)theCB+HMAC_SIZE,STORECBSIZE-HMAC_SIZE);
	memcpy(theCB->hdr.hmac,hmac.result(),HMAC_SIZE);
	rc=ctx->fileMgr->io(FIO_WRITE,0,theCB,ctx->bufSize,true);
	return rc;
}

RC StoreCB::changePassword(class StoreCtx *ctx,const char *pwd)
{
	const char *key=pwd!=NULL&&*pwd!='\0'?pwd:DEFAULTHMACKEY;
	PWD_ENCRYPT crypt((byte*)key,strlen(key)); RWLockP lck(&ctx->cbLock,RW_S_LOCK);
	memcpy(ctx->theCB->hdr.salt1,crypt.encrypted(),sizeof(uint32_t)+PWD_LSALT);
	memcpy(ctx->HMACKey0,crypt.encrypted()+sizeof(uint32_t)+PWD_LSALT,HMAC_KEY_SIZE);
	if (pwd==NULL || *pwd=='\0') {
		if (ctx->theCBEnc!=NULL) {freeAligned(ctx->theCBEnc); ctx->theCBEnc=NULL;}
	} else if (ctx->theCBEnc==NULL && (ctx->theCBEnc=(StoreCB*)allocAligned(ctx->bufSize,ctx->fileMgr->getPageSize()))==NULL) 
		return RC_NORESOURCES;
	else {
		PWD_ENCRYPT crypt2((byte*)pwd,strlen(pwd));
		memcpy(ctx->theCB->hdr.salt2,crypt2.encrypted(),sizeof(uint32_t)+PWD_LSALT);
		memcpy(ctx->encKey0,crypt2.encrypted()+sizeof(uint32_t)+PWD_LSALT,ENC_KEY_SIZE);
	}
	return RC_OK;
}

void StoreCB::close(StoreCtx *ctx)
{
	if (ctx->theCBEnc!=NULL) {freeAligned(ctx->theCBEnc); ctx->theCBEnc=NULL;}
	if (ctx->theCB!=NULL) {freeAligned(ctx->theCB); ctx->theCB=NULL;}
	ctx->fileMgr->close(0);
}

RC StoreCB::update(StoreCtx *ctx,ulong info,const byte *rec,size_t lrec,bool fUndo)
{
	RWLockP lck(&ctx->cbLock,RW_S_LOCK);
	const MapAnchorUpdate *mau=(const MapAnchorUpdate*)rec;
	if (mau==NULL || info>=MA_ALL || lrec!=sizeof(MapAnchorUpdate) || 
		mapRoots[info]!=(fUndo?mau->newPageID:mau->oldPageID)) return RC_OTHER;
	mapRoots[info]=fUndo?mau->oldPageID:mau->newPageID; return RC_OK;
}
