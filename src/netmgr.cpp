/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "netmgr.h"
#include "fio.h"
#include "buffer.h"
#include "txmgr.h"
#include "startup.h"
#include "mvstoreimpl.h"
#include "queryprc.h"
#include "logmgr.h"

using namespace MVStoreKernel;

static const IndexFormat rpinIndexFmt(KT_BIN,PIDKeySize,PageAddrSize+sizeof(TIMESTAMP));

NetMgr::NetMgr(StoreCtx *ct,IStoreNet *nt,int hashSize,int cacheSize,TIMESTAMP dExp)
: RPINHashTab(*new(ct) RPINHashTab::QueueCtrl<STORE_HEAP>(cacheSize),hashSize),ctx(ct),
	net(nt),xPINs(cacheSize),defExpiration(dExp),map(MA_RCACHE,rpinIndexFmt,ct,TF_WITHDEL)
{
		ctx->registerPageMgr(PGID_NETMGR,this); if (&ctrl==NULL) throw RC_NORESOURCES;
}

NetMgr::~NetMgr()
{
	if (&ctrl!=NULL) ctrl.~QueueCtrl();
}

void NetMgr::close()
{
}

bool NetMgr::isOnline()
{
	return net!=NULL && net->isOnline();
}

bool NetMgr::isCached(const PID& id)
{
	PID pid=id; RPIN *rp; RC rc=get(rp,pid,0,RW_S_LOCK|QMGR_INMEM);
	if (rc==RC_OK) {release(rp); return true;}
	byte rbuf[PIDKeySize],dbuf[PageAddrSize+sizeof(TIMESTAMP)];
	memcpy(rbuf,&pid.ident,sizeof(IdentityID));
	memcpy(rbuf+sizeof(IdentityID),&pid.pid,sizeof(OID));
	SearchKey key(rbuf,PIDKeySize); size_t size=sizeof(dbuf);
	return map.find(key,dbuf,size);
}

RC NetMgr::insert(PIN *pin)
{
	assert(isRemote(pin->id) && pin->addr.defined());
	byte rbuf[PIDKeySize],dbuf[PageAddrSize+sizeof(TIMESTAMP)];
	*(PageID*)dbuf=pin->addr.pageID; *(PageIdx*)(dbuf+sizeof(PageID))=pin->addr.idx;
	const static TIMESTAMP noExp=~TIMESTAMP(0); memcpy(dbuf+PageAddrSize,&noExp,sizeof(TIMESTAMP));
	memcpy(rbuf,&pin->id.ident,sizeof(IdentityID)); memcpy(rbuf+sizeof(IdentityID),&pin->id.pid,sizeof(OID));
	SearchKey key(rbuf,PIDKeySize); RPIN *rp; RC rc;
	if ((rc=map.insert(key,dbuf,sizeof(dbuf)))==RC_OK && (rc=get(rp,pin->id,0,QMGR_NOLOAD|RW_X_LOCK))==RC_OK) 
		{rp->addr=pin->addr; rp->expiration=noExp; release(rp);}
	return rc;
}

RC NetMgr::updateAddr(const PID& pid,const PageAddr &addr)
{
	assert(isRemote(pid)); PID id=pid; RPIN *rp; RC rc;
	if ((rc=get(rp,id,0,RPIN_NOFETCH|RW_X_LOCK))!=RC_OK) return rc;
	if (!rp->addr.defined()) rc=RC_FALSE;
	else {
		byte buf[PIDKeySize+PageAddrSize]; SearchKey key(buf,PIDKeySize);
		memcpy(buf,&id.ident,sizeof(IdentityID)); memcpy(buf+sizeof(IdentityID),&id.pid,sizeof(OID));
		*(PageID*)(buf+PIDKeySize)=addr.pageID; *(PageIdx*)(buf+PIDKeySize+sizeof(PageID))=addr.idx;
		if ((rc=map.edit(key,buf+PIDKeySize,PageAddrSize,PageAddrSize,0))==RC_OK) {
			*(PageID*)(buf+PIDKeySize)=rp->addr.pageID;
			*(PageIdx*)(buf+PIDKeySize+sizeof(PageID))=rp->addr.idx;
			ctx->logMgr->insert(Session::getSession(),LR_LUNDO,PGID_NETMGR,INVALID_PAGEID,NULL,buf,sizeof(buf));
			rp->addr=addr;
		}
	}
	release(rp); return rc;
}

RC NetMgr::remove(const PID& pid,PageAddr &addr)
{
	RPIN *rp; assert(isRemote(pid));
	if (get(rp,pid,0,RPIN_NOFETCH|RW_X_LOCK)!=RC_OK) return RC_FALSE;
	assert(rp->addr.defined());
	byte rbuf[PIDKeySize]; SearchKey key(rbuf,PIDKeySize);
	memcpy(rbuf,&pid.ident,sizeof(IdentityID));
	memcpy(rbuf+sizeof(IdentityID),&pid.pid,sizeof(OID));
	RC rc=map.remove(key,NULL,0); if (rc!=RC_OK) {release(rp); return rc;}
	addr=rp->addr; if (drop(rp)) rp->destroy();
	return RC_OK;
}

RC NetMgr::getPage(const PID& id,ulong flags,PageIdx& idx,PBlockP& pb,Session *ses)
{
	PID pid=id; RPIN *rp; RC rc=get(rp,pid,0,RW_S_LOCK); if (rc!=RC_OK) {pb.release(); return rc;}
	if (!rp->addr.defined()) {release(rp); pb.release(); return rc;}
	TIMESTAMP now; getTimestamp(now);
	if (rp->expiration<now && net!=NULL && net->isOnline()) rp->refresh(NULL,ses);
	PBlock *pb2=ses!=NULL?ses->getLatched(rp->addr.pageID):NULL;
	if (pb2==NULL || pb2==pb) pb=ctx->bufMgr->getPage(rp->addr.pageID,ctx->heapMgr,flags,pb);
	else {
		if ((flags&PGCTL_XLOCK)!=0 && pb2->isULocked()) pb2->upgradeLock();
		else if ((flags&(PGCTL_ULOCK|PGCTL_XLOCK))!=0 && !pb2->isULocked() && !pb2->isXLocked()) 
			{release(rp); return RC_DEADLOCK;}
		pb.release(); pb=pb2; pb.set(PGCTL_NOREL);
	}
	idx=rp->addr.idx; release(rp); return pb.isNull()?RC_CORRUPTED:RC_OK;
}

RC NetMgr::setExpiration(const PID& pid,unsigned long exp)
{
	PID id=pid; RPIN *rp; TIMESTAMP expTime;
	RC rc=get(rp,id,0,RW_X_LOCK); if (rc!=RC_OK) return rc;
	assert(rp->addr.defined());
	getTimestamp(expTime); rp->expiration=expTime+=TIMESTAMP(exp)*1000;
	byte rbuf[PIDKeySize]; SearchKey key(rbuf,PIDKeySize);
	memcpy(rbuf,&id.ident,sizeof(IdentityID)); memcpy(rbuf+sizeof(IdentityID),&id.pid,sizeof(uint64_t));
	rc=map.edit(key,&expTime,sizeof(TIMESTAMP),sizeof(TIMESTAMP),PageAddrSize);
	release(rp);
	return rc;
}

RC NetMgr::refresh(PIN *pin,Session *ses)
{
	PID pid=pin->id; RPIN *rp; RC rc=get(rp,pid,0,RW_S_LOCK);
	if (rc==RC_OK) {rc=rp->refresh(pin,ses); release(rp);} 
	return rc;
}

RC NetMgr::undo(ulong info,const byte *rec,size_t lrec,PageID pid)
{
	if (pid!=INVALID_PAGEID || lrec!=PIDKeySize+PageAddrSize) return RC_CORRUPTED;
	if (!ctx->logMgr->isRecovery()) {
		PID id; RPIN *rp;
		memcpy(&id.ident,rec,sizeof(IdentityID)); memcpy(&id.pid,rec+sizeof(IdentityID),sizeof(OID));
		if (get(rp,id,0,QMGR_INMEM|RW_X_LOCK)==RC_OK) {
			rp->addr.pageID=*(PageID*)(rec+PIDKeySize);
			rp->addr.idx=*(PageIdx*)(rec+PIDKeySize+sizeof(PageID));
			release(rp);
		}
	}
	return RC_OK;
}

//-------------------------------------------------------------------------------------------------

RPIN::RPIN(const PID& id,class NetMgr& mg) : ID(id),qe(NULL),mgr(mg),expiration(0),addr(PageAddr::invAddr) {++mg.nPINs;}
	
RC RPIN::load(int,ulong flags)
{
	addr.pageID=INVALID_PAGEID; addr.idx=INVALID_INDEX;
	byte rbuf[PIDKeySize],dbuf[PageAddrSize+sizeof(TIMESTAMP)];
	memcpy(rbuf,&ID.ident,sizeof(IdentityID));
	memcpy(rbuf+sizeof(IdentityID),&ID.pid,sizeof(OID));
	SearchKey key(rbuf,PIDKeySize); size_t size=sizeof(dbuf);
	bool rc=mgr.map.find(key,dbuf,size),fExpired=false; assert(!rc||size==sizeof(dbuf));
	if (rc) {
		addr.pageID=*(PageID*)dbuf; addr.idx=*(PageIdx*)(dbuf+sizeof(PageID));
		memcpy(&expiration,dbuf+PageAddrSize,sizeof(TIMESTAMP));
	}
	if ((!rc || fExpired) && mgr.net!=NULL && (flags&RPIN_NOFETCH)==0) {
		Session *ses=Session::getSession();
		PIN *pin=new(ses) PIN(ses,ID,PageAddr::invAddr);
		if (pin==NULL) rc=false;
		else {
			try {
				if (mgr.net->getPIN(ID.pid,ushort(ID.pid>>48),ID.ident,pin)) {
					MiniTx tx(ses); pin->mode=pin->mode&(PIN_NOTIFY|PIN_NO_INDEX)|PIN_READONLY; size_t sz;
					if (mgr.ctx->queryMgr->commitPINs(ses,&pin,1,MODE_NO_RINDEX,NULL,&sz)!=RC_OK) rc=false;
					else {
						addr=pin->addr; *(PageID*)dbuf=addr.pageID; *(PageIdx*)(dbuf+sizeof(PageID))=addr.idx; getTimestamp(expiration);
						expiration+=ses!=NULL&&ses->getDefaultExpiration()!=0?ses->getDefaultExpiration():mgr.defExpiration;
						memcpy(dbuf+PageAddrSize,&expiration,sizeof(TIMESTAMP));
						rc=mgr.map.insert(key,dbuf,sizeof(dbuf))==RC_OK; if (rc) tx.ok();
						if (rc && fExpired) {
							// delete old one
						}
					}
				}
			} catch (...) {
				// report exception
			}
			pin->destroy();
		}
	}
	return rc?RC_OK:RC_NOTFOUND;	// ???
}

RC RPIN::refresh(PIN *pin,Session *ses)
{
	if (mgr.net==NULL) return RC_OK; RC rc=RC_OK; bool fDestroy=false; Value *val=NULL; ulong nProps=0;
	if (pin!=NULL) {val=pin->properties; nProps=pin->nProperties; pin->properties=NULL; pin->nProperties=0;}
	else if ((pin=new(ses) PIN(ses,ID,PageAddr::invAddr))!=NULL) fDestroy=true; else return RC_NORESOURCES;
	try {
		const_cast<PageAddr&>(pin->addr)=PageAddr::invAddr;
		if (mgr.net->getPIN(ID.pid,ushort(ID.pid>>48),ID.ident,pin)) {		// && pin->stamp!=oldStamp
			PINEx cb(ses,ID); cb=addr; MiniTx tx(ses); Value *diffProps=NULL; ulong nDiffProps=0;
			if ((rc=mgr.ctx->queryMgr->diffPIN(pin,cb,diffProps,nDiffProps,ses))==RC_OK) {
				if (nDiffProps>0 && (rc=mgr.ctx->queryMgr->modifyPIN(ses,ID,diffProps,nDiffProps,&cb,NULL,MODE_FORCE_EIDS|MODE_REFRESH))==RC_OK) {
					byte rbuf[PIDKeySize],dbuf[PageAddrSize+sizeof(TIMESTAMP)],dold[PageAddrSize+sizeof(TIMESTAMP)];
					*(PageID*)dold=addr.pageID; *(PageIdx*)(dold+sizeof(PageID))=addr.idx; 
					*(PageID*)dbuf=cb.getAddr().pageID; *(PageIdx*)(dbuf+sizeof(PageID))=cb.getAddr().idx; 
					memcpy(dold+PageAddrSize,&expiration,sizeof(TIMESTAMP)); TIMESTAMP exp; getTimestamp(exp);
					exp+=ses!=NULL&&ses->getDefaultExpiration()!=0?ses->getDefaultExpiration():mgr.defExpiration;
					memcpy(dbuf+PageAddrSize,&exp,sizeof(TIMESTAMP)); memcpy(rbuf,&ID.ident,sizeof(IdentityID)); 
					memcpy(rbuf+sizeof(IdentityID),&ID.pid,sizeof(uint64_t)); SearchKey key(rbuf,PIDKeySize); 
					if ((rc=mgr.map.update(key,dold,sizeof(dold),dbuf,sizeof(dbuf)))==RC_OK) {addr=cb.getAddr(); expiration=exp; tx.ok();}
				}
				freeV(diffProps,nDiffProps,ses);
			}
		}
		if (rc!=RC_OK) const_cast<PageAddr&>(pin->addr)=addr;
	} catch (RC rc2) {rc=rc2;} catch (...) {
		// ..report exception
		rc=RC_OTHER;
	}
	if (fDestroy) pin->destroy();
	else if (rc!=RC_OK) {pin->properties=val; pin->nProperties=nProps;}
	else if (val!=NULL) {for (ulong i=0; i<nProps; i++) freeV(val[i]); free(val,SES_HEAP);}
	return rc;
}

RPIN *RPIN::createNew(const PID& id,void *mg)
{
	NetMgr &mgr=*(NetMgr*)(RPINHashTab*)mg; 
	return mgr.nPINs<mgr.xPINs?new(mgr.freeRPINs.alloc(sizeof(RPIN))) RPIN(id,mgr):NULL;
}

void RPIN::waitResource(void*)
{
	threadYield();
}

void RPIN::signal(void*)
{
}

void RPIN::destroy()
{
	--mgr.nPINs; mgr.freeRPINs.dealloc(this);
}
