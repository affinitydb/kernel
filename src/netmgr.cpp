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

Written by Mark Venguerov 2004-2014

**************************************************************************************/

#include "netmgr.h"
#include "fio.h"
#include "buffer.h"
#include "txmgr.h"
#include "startup.h"
#include "pin.h"
#include "queryprc.h"
#include "logmgr.h"
#include "service.h"

using namespace AfyKernel;

#define PIDKeySize		(sizeof(uint64_t)+sizeof(IdentityID))

static const IndexFormat rpinIndexFmt(KT_BIN,PIDKeySize,PageAddrSize+sizeof(TIMESTAMP));

NetMgr::NetMgr(StoreCtx *ct,int hashSize,int cacheSize,TIMESTAMP dExp)
: RPINHashTab(*new(ct) RPINHashTab::QueueCtrl(cacheSize,ct),hashSize,ct),PageMgr(ct),
	xPINs(cacheSize),defExpiration(dExp),map(MA_RCACHE,rpinIndexFmt,ct,TF_WITHDEL),freeRPINs((MemAlloc*)ct,RPIN_BLOCK)
{
		ctx->registerPageMgr(PGID_NETMGR,this); if (&ctrl==NULL) throw RC_NOMEM;
}

NetMgr::~NetMgr()
{
	if (&ctrl!=NULL) ctrl.~QueueCtrl();
}

void NetMgr::close()
{
}

bool NetMgr::isCached(const PID& id)
{
	PID pid=id; RPIN *rp; RC rc=get(rp,pid,0,RW_S_LOCK|QMGR_INMEM);
	if (rc==RC_OK) {release(rp); return true;}
	byte rbuf[PIDKeySize],dbuf[PageAddrSize+sizeof(TIMESTAMP)];
	*(IdentityID*)rbuf=pid.ident; __una_set((uint64_t*)(rbuf+sizeof(IdentityID)),pid.pid);
	SearchKey key(rbuf,PIDKeySize); size_t size=sizeof(dbuf);
	return map.find(key,dbuf,size)==RC_OK;
}

RC NetMgr::insert(PIN *pin)
{
	assert(isRemote(pin->id) && pin->addr.defined());
	byte rbuf[PIDKeySize],dbuf[PageAddrSize+sizeof(TIMESTAMP)];
	*(PageID*)dbuf=pin->addr.pageID; *(PageIdx*)(dbuf+sizeof(PageID))=pin->addr.idx;
	const static TIMESTAMP noExp=~TIMESTAMP(0); memcpy(dbuf+PageAddrSize,&noExp,sizeof(TIMESTAMP));
	*(IdentityID*)rbuf=pin->id.ident; __una_set((uint64_t*)(rbuf+sizeof(IdentityID)),pin->id.pid);
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
		memcpy(buf,&id.ident,sizeof(IdentityID)); memcpy(buf+sizeof(IdentityID),&id.pid,sizeof(uint64_t));
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
	memcpy(rbuf+sizeof(IdentityID),&pid.pid,sizeof(uint64_t));
	RC rc=map.remove(key,NULL,0); if (rc!=RC_OK) {release(rp); return rc;}
	addr=rp->addr; if (drop(rp)) rp->destroy();
	return RC_OK;
}

RC NetMgr::getPage(const PID& id,unsigned flags,PageIdx& idx,PBlockP& pb,Session *ses)
{
	PID pid=id; RPIN *rp; RC rc=get(rp,pid,0,RW_S_LOCK); if (rc!=RC_OK) {pb.release(ses); return rc;}
	if (!rp->addr.defined()) {release(rp); pb.release(ses); return rc;}
	TIMESTAMP now; getTimestamp(now); if (rp->expiration<now) rp->refresh(NULL,ses);
	pb.getPage(rp->addr.pageID,ctx->heapMgr,flags|PGCTL_RLATCH,ses);
	idx=rp->addr.idx; release(rp); return pb.isNull()?RC_CORRUPTED:RC_OK;
}

RC NetMgr::setExpiration(const PID& pid,unsigned exp)
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

RC NetMgr::undo(unsigned info,const byte *rec,size_t lrec,PageID pid)
{
	if (pid!=INVALID_PAGEID || lrec!=PIDKeySize+PageAddrSize) return RC_CORRUPTED;
	if (!ctx->logMgr->isRecovery()) {
		PID id; RPIN *rp;
		memcpy(&id.ident,rec,sizeof(IdentityID)); memcpy(&id.pid,rec+sizeof(IdentityID),sizeof(uint64_t));
		if (get(rp,id,0,QMGR_INMEM|RW_X_LOCK)==RC_OK) {
			rp->addr.pageID=*(PageID*)(rec+PIDKeySize);
			rp->addr.idx=*(PageIdx*)(rec+PIDKeySize+sizeof(PageID));
			release(rp);
		}
	}
	return RC_OK;
}

//-------------------------------------------------------------------------------------------------

RPIN::RPIN(const PID& id,class NetMgr& mg) : ID(id),qe(NULL),mgr(mg),expiration(0),addr(PageAddr::noAddr) {++mg.nPINs;}

RC RPIN::read(Session *ses,PIN *&pin)
{
	IServiceCtx *srv=NULL; Value vv[2],res; res.setEmpty(); pin=NULL; RC rc;
	vv[0].set(ID); vv[0].setPropID(PROP_SPEC_PINID); vv[1].setURIID(SERVICE_REMOTE); vv[1].setPropID(PROP_SPEC_SERVICE);
	if ((rc=ses->createServiceCtx(vv,2,srv))==RC_OK && srv!=NULL) {
		if ((rc=srv->invoke(vv,2,&res))==RC_OK) {if (res.type==VT_REF) pin=(PIN*)res.pin; else {freeV(res); rc=RC_TYPE;}}
		srv->destroy();
	}
	return rc;
}
	
RC RPIN::load(int,unsigned flags)
{
	addr.pageID=INVALID_PAGEID; addr.idx=INVALID_INDEX;
	byte rbuf[PIDKeySize],dbuf[PageAddrSize+sizeof(TIMESTAMP)];
	memcpy(rbuf,&ID.ident,sizeof(IdentityID));
	memcpy(rbuf+sizeof(IdentityID),&ID.pid,sizeof(uint64_t));
	SearchKey key(rbuf,PIDKeySize); size_t size=sizeof(dbuf);
	RC rc=mgr.map.find(key,dbuf,size); bool fExpired=false; assert(!rc||size==sizeof(dbuf));
	if (rc==RC_OK) {
		addr.pageID=*(PageID*)dbuf; addr.idx=*(PageIdx*)(dbuf+sizeof(PageID));
		memcpy(&expiration,dbuf+PageAddrSize,sizeof(TIMESTAMP));
	}
	if ((rc!=RC_OK || fExpired) && (flags&RPIN_NOFETCH)==0) try {
		PIN *pin=NULL; Session *ses=Session::getSession();
		if (read(ses,pin)==RC_OK && pin!=NULL) {
			MiniTx tx(ses); pin->id=ID; const_cast<PIN*>(pin)->addr=PageAddr::noAddr;
			pin->mode=pin->mode&PIN_NOTIFY|PIN_IMMUTABLE; size_t sz;
			if ((rc=mgr.ctx->queryMgr->persistPINs(EvalCtx(ses,ECT_INSERT),&pin,1,MODE_NO_RINDEX,NULL,&sz))==RC_OK) {
				addr=pin->addr; *(PageID*)dbuf=addr.pageID; *(PageIdx*)(dbuf+sizeof(PageID))=addr.idx; getTimestamp(expiration);
				expiration+=ses!=NULL&&ses->getDefaultExpiration()!=0?ses->getDefaultExpiration():mgr.defExpiration;
				memcpy(dbuf+PageAddrSize,&expiration,sizeof(TIMESTAMP));
				if ((rc=mgr.map.insert(key,dbuf,sizeof(dbuf)))==RC_OK) {
					tx.ok();
					if (fExpired) {
						// delete old one
					}
				}
			}
			pin->destroy();
		}
	} catch (...) {
		// report exception
	}
	return rc;
}

RC RPIN::refresh(PIN *pin,Session *ses)
{
	RC rc=RC_OK;
	try {
		PIN *rp=NULL;
		if (read(ses,rp)==RC_OK && rp!=NULL) {
			PINx cb(ses,ID); cb=addr; MiniTx tx(ses); Value *diffProps=NULL; unsigned nDiffProps=0; rp->id=ID;
			if ((rc=mgr.ctx->queryMgr->diffPIN(rp,cb,diffProps,nDiffProps,ses))==RC_OK) {
				if (nDiffProps>0 && (rc=mgr.ctx->queryMgr->modifyPIN(EvalCtx(ses,ECT_INSERT),ID,diffProps,nDiffProps,&cb,NULL,MODE_FORCE_EIDS|MODE_REFRESH))==RC_OK) {
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
				if (pin!=NULL) {
					const_cast<PageAddr&>(pin->addr)=addr; freeV(pin->properties,pin->nProperties,ses);
					pin->properties=rp->properties; pin->nProperties=rp->nProperties;
					rp->properties=NULL; rp->nProperties=0;
				}
			}
			rp->destroy();
		}
	} catch (RC rc2) {rc=rc2;} catch (...) {
		// ..report exception
		rc=RC_OTHER;
	}
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
