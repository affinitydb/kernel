/**************************************************************************************

Copyright © 2004-2014 GoPivotal, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,  WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.

Written by Mark Venguerov 2004-2012

**************************************************************************************/

#include "fio.h"
#include "buffer.h"
#include "storecb.h"
#include "logmgr.h"
#include "classifier.h"
#include "startup.h"

using namespace AfyKernel;

RC StoreCB::open(StoreCtx *ctx,const char *fname,const char *pwd,unsigned mode)
{
	assert(ctx!=NULL&&ctx->cryptoMgr!=NULL&&(fname!=NULL||ctx->fileMgr==NULL));
	RC rc=RC_OK; FileID fid=0; TIMESTAMP ts; getTimestamp(ts); byte keybuf[PWD_ENC_SIZE]; StoreCB *theCB=NULL;
	size_t lPage=getPageSize(); ctx->bufSize=(uint32_t)ceil(STORECBSIZE,lPage);
	if (ctx->fileMgr==NULL) {
		theCB=ctx->theCB=(StoreCB*)ctx->memory; assert(ctx->memory!=NULL && ctx->lMemory>sizeof(StoreCB));
		if (theCB->totalMemUsed>ctx->lMemory) return RC_CORRUPTED;
	} else if ((theCB=ctx->theCB=(StoreCB*)allocAligned(ctx->bufSize,lPage))==NULL) return RC_NORESOURCES;
	else if ((rc=ctx->fileMgr->open(fid,fname))!=RC_OK || (rc=ctx->fileMgr->io(FIO_READ,PageIDFromPageNum(fid,0),theCB,ctx->bufSize))!=RC_OK) return rc;
	if (pwd!=NULL && *pwd!='\0' || (theCB->flags&STORE_CREATE_PAGE_INTEGRITY)!=0) {
		const char *key=pwd!=NULL&&*pwd!='\0'?pwd:DEFAULTHMACKEY;
		memcpy(keybuf,theCB->hdr.salt1,sizeof(uint32_t)+PWD_LSALT);
		PWD_ENCRYPT crypt((byte*)key,strlen(key),keybuf);
		memcpy(ctx->HMACKey0,crypt.encrypted()+sizeof(uint32_t)+PWD_LSALT,HMAC_KEY_SIZE);
		HMAC hmac(ctx->HMACKey0,HMAC_KEY_SIZE); hmac.add((byte*)theCB+HMAC_SIZE,STORECBSIZE-HMAC_SIZE);
		if (memcmp(theCB->hdr.hmac,hmac.result(),HMAC_SIZE)!=0) rc=RC_CORRUPTED;
	}
	if (rc==RC_OK) {
		if (pwd!=NULL && *pwd!='\0') {
			memcpy(keybuf,theCB->hdr.salt2,sizeof(uint32_t)+PWD_LSALT);
			PWD_ENCRYPT crypt2((byte*)pwd,strlen(pwd),keybuf); 
			memcpy(ctx->encKey0,crypt2.encrypted()+sizeof(uint32_t)+PWD_LSALT,ENC_KEY_SIZE);
			AES aes(ctx->encKey0,ENC_KEY_SIZE,true);
			aes.decrypt((byte*)theCB+sizeof(CryptInfo),STORECBSIZE-sizeof(CryptInfo),(uint32_t*)theCB->hdr.salt1+1);
			if ((ctx->theCBEnc=(StoreCB*)allocAligned(ctx->bufSize,lPage))==NULL) {close(ctx); return RC_NORESOURCES;}
		}
		if (theCB->magic!=STORECBMAGIC) rc=RC_CORRUPTED;
		else if (theCB->version/100!=STORE_VERSION/100 || theCB->version%100>STORE_VERSION%100) rc=RC_VERSION;
		else {
			if (theCB->timestamp>ts) {
				report((mode&STARTUP_FORCE_OPEN)!=0?MSG_WARNING:MSG_CRIT,"Incorrect store last modification time\n");
				if ((mode&STARTUP_FORCE_OPEN)==0) rc=RC_CORRUPTED;
			}
			if (rc==RC_OK) {
				ctx->storeID=theCB->storeID; ctx->keyPrefix=StoreCtx::genPrefix(theCB->storeID);
				memcpy(ctx->HMACKey,theCB->HMACKey,HMAC_KEY_SIZE);
				memcpy(ctx->encKey,theCB->encKey,ENC_KEY_SIZE);
				SHA256 pub; pub.add(ctx->encKey,ENC_KEY_SIZE);
				memcpy(ctx->pubKey,pub.result(),SHA_DIGEST_BYTES);
			}
		}
	}
	if (rc!=RC_OK) close(ctx);
	return rc;
}

RC StoreCB::create(StoreCtx *ctx,const char *fname,const StoreCreationParameters& cpar)
{
	assert(ctx!=NULL&&ctx->cryptoMgr!=NULL&&(fname!=NULL||ctx->fileMgr==NULL)); assert(sizeof(StoreCB)<=STORECBSIZE);
	RC rc=RC_OK; FileID fid=0; off64_t addr; StoreCB *theCB=NULL; ctx->bufSize=(uint32_t)ceil(STORECBSIZE,cpar.pageSize);
	if (ctx->fileMgr==NULL) {theCB=ctx->theCB=(StoreCB*)ctx->memory; assert(ctx->memory!=NULL && ctx->lMemory>sizeof(StoreCB));}
	else if ((theCB=ctx->theCB=(StoreCB*)allocAligned(ctx->bufSize,cpar.pageSize))==NULL) return RC_NORESOURCES;
	else {
		if ((rc=ctx->fileMgr->open(fid,fname,FIO_CREATE|FIO_NEW))!=RC_OK) {
			if (rc!=RC_ALREADYEXISTS) return rc;
			bool fInit=open(ctx,fname,cpar.password,false)==RC_OK&&ctx->theCB->state==SST_INIT;
			if (!fInit) {ctx->fileMgr->close(0); return rc;}
			ctx->fileMgr->growFile(0,0);
		}
		if ((rc=ctx->fileMgr->allocateExtent(fid,1,addr))!=RC_OK) {ctx->fileMgr->close(fid); return rc;}
		assert(addr==0);
	}
	memset(theCB,0,ctx->bufSize);
	theCB->magic=STORECBMAGIC;
	theCB->version=STORE_VERSION;
	theCB->lPage=cpar.pageSize;
	theCB->nPagesPerExtent=cpar.fileExtentSize;
	theCB->maxSize=cpar.maxSize;
	theCB->logSegSize=(uint32_t)cpar.logSegSize;	// round to sectors (**2?)
	theCB->nMaster=cpar.nControlRecords;
	theCB->pctFree=cpar.pctFree<=0.f?DEFAULTPCTFREE:cpar.pctFree;
	theCB->storeID=ctx->storeID=cpar.storeId;
	if ((cpar.mode&STORE_CREATE_ENCRYPTED)!=0) theCB->flags|=STFLG_ENCRYPTED;
	if ((cpar.mode&STORE_CREATE_PAGE_INTEGRITY)!=0) theCB->flags|=STFLG_PAGEHMAC;
	if ((cpar.mode&STORE_CREATE_NO_PREFIX)!=0) theCB->flags|=STFLG_NO_PREFIX;
	ctx->cryptoMgr->randomBytes(theCB->encKey,ENC_KEY_SIZE);
	memcpy(ctx->encKey,theCB->encKey,ENC_KEY_SIZE);
	SHA256 pub; pub.add(ctx->encKey,ENC_KEY_SIZE);
	memcpy(ctx->pubKey,pub.result(),SHA_DIGEST_BYTES);
	if ((theCB->flags&(STFLG_ENCRYPTED|STFLG_PAGEHMAC))!=0 || cpar.password!=NULL && *cpar.password!='\0') {
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
	}
	ctx->keyPrefix=StoreCtx::genPrefix(cpar.storeId);
	theCB->totalMemUsed=ctx->bufSize;
	theCB->nDataFiles=1;
	theCB->state=~0u;
	theCB->xPropID=MAX_BUILTIN_URIID;
	theCB->xSyncStack=cpar.xSyncStack;
	theCB->xOnCommit=cpar.xOnCommit;
	theCB->xSesObjects=cpar.xObjSession;
	for (int i=0; i<MA_ALL; i++) theCB->mapRoots[i]=INVALID_PAGEID;
	if (rc!=RC_OK || (rc=update(ctx,false))!=RC_OK) close(ctx);
	return rc;
}

void StoreCB::preload(StoreCtx *ctx) const
{
	static const PGID mapRootsPGIDs[MA_ALL]={
		PGID_INDEX,PGID_INDEX,PGID_INDEX,PGID_INDEX,PGID_INDEX,PGID_INDEX,PGID_INDEX,
		PGID_INDEX,PGID_INDEX,PGID_HEAPDIR,PGID_HEAPDIR,PGID_HEAPDIR,PGID_HEAPDIR,
		PGID_ALL,PGID_ALL,PGID_ALL,PGID_ALL,PGID_ALL,PGID_ALL,PGID_ALL,PGID_ALL};
	PageID pages[MA_ALL]; PageMgr *pmgrs[MA_ALL]; unsigned cnt=0;
	for (unsigned i=0; i<MA_ALL; i++) if (mapRoots[i]!=INVALID_PAGEID)
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
		memcpy(theCB=ctx->theCBEnc,ctx->theCB,STORECBSIZE); AES aes(ctx->encKey0,ENC_KEY_SIZE,false);
		aes.encrypt((byte*)theCB+sizeof(CryptInfo),STORECBSIZE-sizeof(CryptInfo),(uint32_t*)theCB->hdr.salt1+1);
	}
	if ((theCB->flags&STFLG_PAGEHMAC)!=0) {
		HMAC hmac(ctx->HMACKey0,HMAC_KEY_SIZE);
		hmac.add((byte*)theCB+HMAC_SIZE,STORECBSIZE-HMAC_SIZE);
		memcpy(theCB->hdr.hmac,hmac.result(),HMAC_SIZE);
	}
	return ctx->fileMgr!=NULL?ctx->fileMgr->io(FIO_WRITE,0,theCB,ctx->bufSize,true):RC_OK;
}

RC StoreCB::changePassword(class StoreCtx *ctx,const char *pwd)
{
	const char *key=pwd!=NULL&&*pwd!='\0'?pwd:DEFAULTHMACKEY;
	PWD_ENCRYPT crypt((byte*)key,strlen(key)); RWLockP lck(&ctx->cbLock,RW_S_LOCK);
	memcpy(ctx->theCB->hdr.salt1,crypt.encrypted(),sizeof(uint32_t)+PWD_LSALT);
	memcpy(ctx->HMACKey0,crypt.encrypted()+sizeof(uint32_t)+PWD_LSALT,HMAC_KEY_SIZE);
	if (pwd==NULL || *pwd=='\0') {
		if (ctx->theCBEnc!=NULL) {freeAligned(ctx->theCBEnc); ctx->theCBEnc=NULL;}
	} else if (ctx->theCBEnc==NULL && (ctx->theCBEnc=(StoreCB*)allocAligned(ctx->bufSize,ctx->bufMgr->getPageSize()))==NULL) 
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
	if (ctx->theCB!=NULL) {if (ctx->fileMgr!=NULL) freeAligned(ctx->theCB); ctx->theCB=NULL;}
	if (ctx->fileMgr!=NULL) ctx->fileMgr->close(0);
}

RC StoreCB::update(StoreCtx *ctx,unsigned info,const byte *rec,size_t lrec,bool fUndo)
{
	RWLockP lck(&ctx->cbLock,RW_S_LOCK);
	const MapAnchorUpdate *mau=(const MapAnchorUpdate*)rec;
	if (mau==NULL || info>=MA_ALL || lrec!=sizeof(MapAnchorUpdate) || 
		mapRoots[info]!=(fUndo?mau->newPageID:mau->oldPageID)) return RC_OTHER;
	mapRoots[info]=fUndo?mau->oldPageID:mau->newPageID; return RC_OK;
}
