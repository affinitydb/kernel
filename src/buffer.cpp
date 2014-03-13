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

#include "buffer.h"
#include "fio.h"
#include "logmgr.h"
#include "logchkp.h"
#include "session.h"

using namespace AfyKernel;

bool BufMgr::fInit = false;
Mutex BufMgr::initLock;
SLIST_HEADER BufMgr::freeBuffers;
unsigned BufMgr::nBuffers = 0;
unsigned BufMgr::xBuffers = 0;
volatile long BufMgr::nStores = 0;

namespace AfyKernel
{
BufQMgr::QueueCtrl bufCtrl(0);
FreeQ asyncWriteReqs;
};

BufMgr::BufMgr(StoreCtx *ct,int initNumberOfBlocks,size_t lpage) 
	: BufQMgr(bufCtrl,PAGE_HASH_SIZE,ct),ctx(ct),lPage(nextP2((unsigned)lpage)),nStoreBuffers(initNumberOfBlocks),
	fInMem(ctx->memory!=NULL),fRT((ctx->mode&STARTUP_RT)!=0),pageList(NULL),flushList(NULL),depList(NULL)
{	
	InterlockedIncrement(&nStores); assert((lPage&getPageSize()-1)==0);
}

BufMgr::~BufMgr() 
{
	cleanup(); assert(!pageList.isInList());
#ifdef _DEBUG
	reportHighwatermark("BufMgr");
#endif
	InterlockedDecrement(&nStores);
}

void *BufMgr::operator new(size_t s,StoreCtx *ctx)
{
	void *p=ctx->malloc(s); if (p==NULL) throw RC_NORESOURCES; return p;
}

RC BufMgr::init()
{
	if (ctx->getEncKey()!=NULL) setLockType(RW_X_LOCK);
	MutexP lck(&initLock);
	if (!fInit) {InitializeSListHead(&freeBuffers); fInit=true;}
	if (nStoreBuffers>xBuffers) xBuffers=nStoreBuffers;
	unsigned nBufNew=xBuffers;		//...*log10(nStores)
	if (nBufNew>nBuffers) {
		nBufNew-=nBuffers; unsigned nBufOld=nBuffers;
		if (fInMem) {
			PBlock *pb=(PBlock*)::malloc(nBufNew*sizeof(PBlock));
			if (pb!=NULL) for (unsigned i=0; i<nBufNew; ++pb,++i)
				InterlockedPushEntrySList(&freeBuffers,(SLIST_ENTRY*)new(pb) PBlock(this,NULL,NULL));
			nBuffers+=nBufNew;
		} else for (unsigned n=nBufNew; nBufNew!=0; nBufNew-=n) {
			byte *pg=NULL; if (n>nBufNew) n=nBufNew;
			while (n!=0 && (pg=(byte*)allocAligned(n*lPage,lPage))==NULL) n>>=1;
			if (pg==NULL) break;
		
			PBlock *pb=(PBlock*)::malloc(n*(sizeof(PBlock)+sizeof(myaio)));
			if (pb==NULL) {freeAligned(pg); break;}

			myaio *aio=(myaio*)(pb+n); memset(aio,0,n*sizeof(myaio));
			for (unsigned i=0; i<n; ++pb,++i,++aio,pg+=lPage)
				InterlockedPushEntrySList(&freeBuffers,(SLIST_ENTRY*)new(pb) PBlock(this,pg,aio));
			nBuffers+=n;
		}
		if (nBuffers!=0) {
			setNElts(nBuffers);
			if (nBuffers>nBufOld) report(MSG_INFO,"Number of allocated buffers: %u\n",nBuffers-nBufOld);
		}
	}
	return nBuffers==0?RC_NORESOURCES:RC_OK;
}

#ifdef _DEBUG
void BufMgr::checkState()
{
	RWLockP lck(&pageQLock,RW_S_LOCK);
	for (HChain<PBlock>::it it(&pageList); ++it;) {
		PBlock *pb=it.get();
		if ((pb->pageID&0xFF000000)==0 && pb->QE!=NULL && (pb->QE->isFixed() || pb->QE->isLocked()))
			report(MSG_DEBUG,"BufMgr::checkState: block is locked for %s\n",pb->QE->isXLocked()?"write":pb->QE->isULocked()?"update":"read");
	}
}
#endif

PBlock* BufMgr::getPage(PageID pid,PageMgr *pageMgr,unsigned flags,PBlock *old,Session *ses)
{
	PBlock *ret=NULL; LatchedPage *lp; if (ses==NULL) ses=Session::getSession();
	if (ses!=NULL && (lp=(LatchedPage*)BIN<LatchedPage,PageID,LatchedPage::Cmp>::find(pid,ses->latched,ses->nLatched))!=NULL) {
		if (old!=NULL) {
			if (old->pageID==pid) {
				assert(lp->pb==old); 
				if ((old->isXLocked() || old->isULocked()) && (flags&QMGR_UFORCE)!=0) {
					assert(lp->cntX!=0);
					if ((flags&PGCTL_XLOCK)!=0) {if (!old->isXLocked()) old->upgradeLock();}
					else if ((flags&PGCTL_ULOCK)!=0) {if (old->isXLocked()) old->downgradeLock(RW_U_LOCK);}
					else {if (--lp->cntX==0) old->downgradeLock(RW_S_LOCK); if (lp->cntS!=0xFFFF) lp->cntS++; else old=NULL;}
				} else if ((flags&(PGCTL_XLOCK|PGCTL_ULOCK))!=0) {
					assert(lp->cntS!=0); lp->cntS--; if (lp->cntX==0xFFFF) return NULL;
					if (lp->cntX==0) relock(old,(flags&PGCTL_XLOCK)!=0?RW_X_LOCK:RW_U_LOCK); lp->cntX++;
				}
				return old;
			}
			const bool fAfter=pid>old->pageID; old->release(flags,ses); old=NULL;
			if (fAfter) {lp=(LatchedPage*)BIN<LatchedPage,PageID,LatchedPage::Cmp>::find(pid,ses->latched,ses->nLatched); assert(lp!=NULL);}
		}
		assert(lp->pb->pageID==pid);
		if ((flags&(PGCTL_ULOCK|PGCTL_XLOCK))==0) {if (lp->cntS==0xFFFF) return NULL; lp->cntS++; return lp->pb;}
		if (lp->cntX!=0) {if (lp->cntX==0xFFFF) return NULL; lp->cntX++; return lp->pb;}
		assert(lp->cntS!=0); ses->releaseLatches(pid,pageMgr,true);
		if (ses->nLatched!=0 && BIN<LatchedPage,PageID,LatchedPage::Cmp>::find(pid,ses->latched,ses->nLatched)!=NULL) return NULL;
	}
	if (old!=NULL && ses!=NULL && (flags&PGCTL_COUPLE)==0 && !ses->unlatch(old,flags)) old=NULL;
	if ((flags&PGCTL_RLATCH)!=0 && ses!=NULL) ses->releaseLatches(pid,pageMgr,(flags&(PGCTL_ULOCK|PGCTL_XLOCK))!=0);
	unsigned flg=((flags&PGCTL_XLOCK)!=0?RW_X_LOCK:(flags&PGCTL_ULOCK)!=0?RW_U_LOCK:RW_S_LOCK)|(flags&(QMGR_TRY|QMGR_UFORCE|QMGR_INMEM));
	if (pid==INVALID_PAGEID || ctx->theCB->nMaster==0 && PageNumFromPageID(pid)==0 && FileIDFromPageID(pid)==0) flags|=PGCTL_COUPLE;
	else if (get(ret,pid,pageMgr,flg,(flags&PGCTL_COUPLE)==0?old:NULL)!=RC_OK) {assert(ret==0);}
	else {
		assert(ret->QE->getKey()==ret->pageID && ret->QE->isFixed());
		if (pageMgr!=ret->pageMgr) {
			if (ret->pageMgr==NULL) ret->pageMgr=pageMgr;
			else if (pageMgr!=NULL) {
				if (ses==NULL || ses->getExtAddr().pageID!=ret->pageID)
					report(MSG_ERROR,"PageMgr error: request=%d, page type=%d, page %X\n",pageMgr->getPGID(),ret->pageMgr->getPGID(),ret->pageID);
				release(ret,(flags&PGCTL_ULOCK)!=0); ret=NULL;
			}
		}
	}
	if (old!=NULL && (flags&PGCTL_COUPLE)!=0) old->release(flags,ses);
	if (ret!=NULL && ses!=NULL && ses->latch(ret,flags)!=RC_OK) {release(ret,(flags&PGCTL_ULOCK)!=0); ret=NULL;}
	return ret;
}

PBlock *BufMgr::newPage(PageID pid,PageMgr *pageMgr,PBlock *old,unsigned flags,Session *ses)
{
	if (pid==INVALID_PAGEID || ctx->theCB->nMaster==0 && PageNumFromPageID(pid)==0 && FileIDFromPageID(pid)==0)
		{if (old!=NULL) old->release(flags); return NULL;}
	PBlock *pb=NULL;
	if (ses!=NULL || (ses=Session::getSession())!=NULL) {
		if (BIN<LatchedPage,PageID,LatchedPage::Cmp>::find(pid,ses->latched,ses->nLatched)!=NULL)	// shouldn't happen
			{if (old!=NULL) old->release(flags,ses); return NULL;}
		if (old!=NULL && !ses->unlatch(old,flags)) old=NULL;
	}
	switch (get(pb,pid,pageMgr,QMGR_NEW|RW_X_LOCK|(flags&QMGR_UFORCE),old)) {
	default: assert(pb==NULL); break;
	case RC_ALREADYEXISTS: 
		report(MSG_ERROR,"BufMgr::newPage: page %X already exists\n",pid);
	case RC_OK:
		if (fInMem) {const_cast<byte*&>(pb->frame)=(byte*)ctx->memory+pid*lPage; assert(ctx->memory!=NULL);}
		pb->state=BLOCK_NEW_PAGE; assert(pb->QE->getKey()==pb->pageID && pb->QE->isFixed());
		if ((pb->pageMgr=pageMgr)!=NULL) pageMgr->initPage(pb->frame,lPage,pid);
		if (ses!=NULL && ses->latch(pb,flags|PGCTL_XLOCK)!=RC_OK) {release(pb,(flags&PGCTL_ULOCK)!=0); pb=NULL;}
		break;
	}
	return pb;
}

static int __cdecl sortPages(const void *pv1,const void *pv2)
{
	const myaio **ma1=(const myaio **)pv1,**ma2=(const myaio **)pv2;
	int c=cmp3((*ma1)->aio_fid,(*ma2)->aio_fid); return c!=0?c:cmp3((*ma1)->aio_offset,(*ma2)->aio_offset);
}

RC BufMgr::flushAll(uint64_t timeout)
{
	if (fInMem||ctx->theCB->state==SST_NO_SHUTDOWN) return RC_OK; flushLock.lock(RW_X_LOCK);
	myaio **pcbs; RC rc=RC_OK; int cnt,ncbs; TIMESTAMP start,current; getTimestamp(start);
	if (dirtyCount!=0) {
		if ((pcbs=(myaio**)ctx->malloc((ncbs=dirtyCount)*sizeof(myaio*)))==NULL) {flushLock.unlock(); return RC_NORESOURCES;}
		do {
			LSN flushLSN(0); cnt=0; pageQLock.lock(RW_S_LOCK); PBlock *pb;
			for (HChain<PBlock>::it it(&pageList); cnt<ncbs && rc==RC_OK && ++it;)
				if ((pb=lockForSave(it.get()->getPageID(),true))!=NULL) {
					if ((pb->state&(BLOCK_DIRTY|BLOCK_IO_WRITE))!=BLOCK_DIRTY || pb->isDependent()) endSave(pb);
					else {
						if (pb->pageMgr!=NULL) {
							LSN lsn(pb->pageMgr->getLSN(pb->frame,lPage)); if (lsn>flushLSN) flushLSN=lsn;
							if (!pb->pageMgr->beforeFlush(pb->frame,lPage,pb->pageID)) rc=RC_CORRUPTED;
						}
						if (rc!=RC_OK) endSave(pb); else {pb->fillaio(LIO_WRITE,NULL); pcbs[cnt++]=pb->aio;}
					}
				}
			pageQLock.unlock();
			if (cnt!=0) {
				if (cnt>1) qsort(pcbs,cnt,sizeof(myaio*),sortPages);
				if (flushLSN.isNull() || (rc=ctx->logMgr->flushTo(flushLSN))==RC_OK) rc=ctx->fileMgr->listIO(LIO_WAIT,cnt,pcbs);
				for (int i=0; i<cnt; i++) pcbs[i]->aio_pb->writeResult(rc);
			} else {
				getTimestamp(current);
				if (current-start>timeout) rc=RC_TIMEOUT; else if (asyncWriteCount!=0) threadYield();
			}
		} while (rc==RC_OK && dirtyCount!=0);
		ctx->free(pcbs);
	}
	while (asyncWriteCount+asyncReadCount!=0) {
		getTimestamp(current);
		if (current-start>timeout) {rc=RC_TIMEOUT; break;}
		threadYield();
	}
	if (!ctx->inShutdown()) flushLock.unlock();
	return rc;
}

RC BufMgr::close(FileID fid,bool fAll)
{
	{RWLockP lck(fRT?NULL:&pageQLock,RW_X_LOCK);
	for (HChain<PBlock>::it_r it(&pageList); ++it;) {
		PBlock *pb=it.get();
		if (fAll || FileIDFromPageID(pb->pageID)==fid) {
			bool fDel=drop(pb,false,false); pb->pageList.remove(); 
			if (pb->flushList.isInList()) {RWLockP lck(&flushQLock,RW_X_LOCK); pb->flushList.remove(); --dirtyCount;}
			if (pb->dependent!=NULL) {assert(pb->dependent->dependCnt>0); --pb->dependent->dependCnt;}
			if (fDel) {pb->pageID=INVALID_PAGEID; pb->mgr=NULL; InterlockedPushEntrySList(&freeBuffers,(SLIST_ENTRY*)pb);}
		}
	}
	assert(!fAll || !pageList.isInList());
	}
	return fAll||fInMem?RC_OK:ctx->fileMgr->close(fid);
}

void BufMgr::prefetch(const PageID *pages,int nPages,PageMgr *pageMgr,PageMgr *const *mgrs)
{
	if (pages==NULL || nPages==0 || fInMem) return;
	myaio **pcbs=(myaio**)malloc(nPages*sizeof(myaio*),SES_HEAP); if (pcbs==NULL) return;
	
	int cnt=0; PBlock *pb=NULL;
	for (int i=0; i<nPages; i++) {
		if (get(pb,pages[i],pageMgr,QMGR_NEW|RW_X_LOCK)!=RC_OK) continue;
		assert(pb->pageID==pages[i] && pb->QE->getKey()==pb->pageID && pb->QE->isFixed());
		pb->pageMgr=mgrs!=NULL?mgrs[i]:pageMgr; pb->setStateBits(BLOCK_IO_READ|BLOCK_ASYNC_IO);
		pb->fillaio(LIO_READ,BufMgr::asyncReadNotify); ++asyncReadCount; pcbs[cnt++]=pb->aio;
	}
	if (cnt>0) {
		if (cnt>1) qsort(pcbs,cnt,sizeof(myaio*),sortPages);
		RC rc=ctx->fileMgr->listIO(LIO_NOWAIT,cnt,pcbs);
		if (rc!=RC_OK) report(MSG_ERROR,"Read-ahead failed: %d\n",rc);
	}
	free(pcbs,SES_HEAP);
}

void BufMgr::asyncReadNotify(void *p,RC rc,bool fAsync)
{
	PBlock *pb=(PBlock*)p; assert((pb->state&BLOCK_ASYNC_IO)!=0);
	pb->readResult(rc,fAsync); pb->mgr->endLoad(pb); --pb->mgr->asyncReadCount;
}

void BufMgr::asyncWriteNotify(void *p,RC rc,bool fAsync) 
{
	PBlock *pb=(PBlock*)p; BufMgr *mgr=pb->mgr; assert(mgr->asyncWriteCount>0); pb->writeResult(rc,fAsync); --mgr->asyncWriteCount;
}

LogDirtyPages *BufMgr::getDirtyPageInfo(LSN old,LSN& redo,PageID *asyncPages,unsigned& nAsyncPages,unsigned maxAsyncPages)
{
	LogDirtyPages *ldp=(LogDirtyPages*)ctx->malloc(sizeof(LogDirtyPages)+int(dirtyCount-1)*sizeof(LogDirtyPages::LogDirtyPage));
	if (ldp!=NULL) {
		unsigned cnt=0; nAsyncPages=0; LSN *flushLSN=asyncPages!=NULL?(LSN*)alloca(maxAsyncPages*sizeof(LSN)):(LSN*)0;
		{RWLockP lck(&flushLock,RW_S_LOCK); RWLockP flck(&flushQLock,RW_S_LOCK);
		for (HChain<PBlock>::it it(&flushList); ++it;) {
			PBlock *pb=it.get(); if ((pb->state&BLOCK_DIRTY)==0) continue;
			assert(cnt<(unsigned)dirtyCount);
			if ((ldp->pages[cnt].redo=pb->redoLSN)<redo) redo=pb->redoLSN;
			ldp->pages[cnt].pageID=pb->pageID; cnt++;
			if (flushLSN!=NULL && !pb->isDependent() && pb->redoLSN<=old && (nAsyncPages<maxAsyncPages||pb->redoLSN<flushLSN[nAsyncPages-1])) {
				unsigned n=nAsyncPages,base=0,k=0;
				while (n!=0) {
					LSN lsn=flushLSN[(k=n>>1)+base];
					if (lsn>pb->redoLSN || lsn==pb->redoLSN && asyncPages[k+base]<pb->pageID) n=k; 
					else {base+=k+1; n-=k+1; k=0;}
				}
				if ((k+=base)<nAsyncPages && (n=nAsyncPages<maxAsyncPages?nAsyncPages-k:nAsyncPages-k-1)>0) {
					memmove(&asyncPages[k+1],&asyncPages[k],n*sizeof(PageID));
					memmove(&flushLSN[k+1],&flushLSN[k],n*sizeof(LSN));
				}
				if (k<nAsyncPages || nAsyncPages<maxAsyncPages) {
					asyncPages[k]=pb->pageID; flushLSN[k]=pb->redoLSN;
					if (nAsyncPages<maxAsyncPages) nAsyncPages++;
				}
			}
		}
		ldp->nPages=cnt; assert(cnt<=(unsigned)dirtyCount);}
	}
	return ldp;
}

void BufMgr::writeAsyncPages(const PageID *asyncPages,unsigned nAsyncPages)
{
	if (fInMem) return;
	unsigned cnt=0; LSN flushLSN(0); RC rc; assert(asyncPages!=NULL && nAsyncPages>0);
	myaio **pcbs=(myaio**)alloca(nAsyncPages*sizeof(myaio*));
	if (pcbs==NULL||!flushLock.trylock(RW_S_LOCK)) return; RWLockP flck(&flushLock);
	for (unsigned i=0; i<nAsyncPages; i++) {
		PBlock *pb=lockForSave(asyncPages[i],true);
		if (pb!=NULL) {
			bool fOK=true;
			if ((pb->state&(BLOCK_DIRTY|BLOCK_IO_WRITE))!=BLOCK_DIRTY || pb->isDependent()) fOK=false;
			else if (pb->pageMgr!=NULL) {
				LSN lsn(pb->pageMgr->getLSN(pb->frame,lPage)); if (lsn>flushLSN) flushLSN=lsn;
				fOK=pb->pageMgr->beforeFlush(pb->frame,lPage,pb->pageID);
			}
			if (!fOK) endSave(pb); else {pb->fillaio(LIO_WRITE,BufMgr::asyncWriteNotify); pcbs[cnt++]=pb->aio; ++asyncWriteCount;}
		}
	}
	if (cnt>0) {
		if (cnt>1) qsort(pcbs,cnt,sizeof(myaio*),sortPages);
		if (!flushLSN.isNull() && (rc=ctx->logMgr->flushTo(flushLSN))!=RC_OK)
			for (unsigned i=0; i<cnt; i++) {pcbs[i]->aio_pb->writeResult(rc); --asyncWriteCount;}
		 else 
			 ctx->fileMgr->listIO(LIO_NOWAIT,cnt,pcbs);
	}
}

PBlock::PBlock(BufMgr *bm,byte *frm,myaio *ai)
	: pageID(INVALID_PAGEID),state(0),frame(frm),pageMgr(NULL),redoLSN(0),aio(ai),
	QE(NULL),dependent(NULL),vb(NULL),mgr(bm),pageList(this),flushList(this),depList(this)
{
} 

PBlock::~PBlock() 
{
	assert(0);
}

void PBlock::setRedo(LSN lsn) {
	assert(isXLocked()); if (mgr->fInMem) return;
	if ((state&(BLOCK_REDO_SET|BLOCK_DISCARDED))==0) {
		RWLockP flck(&mgr->flushQLock,RW_X_LOCK); 
		if ((state&(BLOCK_REDO_SET|BLOCK_DISCARDED|BLOCK_DIRTY))==0) {
			assert(!flushList.isInList());
			redoLSN=lsn; setStateBits(BLOCK_DIRTY|BLOCK_REDO_SET);
			mgr->flushList.insertLast(&flushList); ++mgr->dirtyCount;
		}
	}
#ifdef _DEBUG
	else assert((state&BLOCK_DIRTY)!=0);
#endif
}

void PBlock::setDependency(PBlock *dp)
{
	assert(dp!=NULL && dp->isXLocked() && dp->dependent==NULL && isXLocked()); 
	if (!mgr->fInMem) {dp->dependent=this; ++dependCnt;}
}

void PBlock::removeDependency(PBlock *dp) {
	assert(dp!=NULL && dp->isXLocked() && isXLocked()); 
	if (!mgr->fInMem && dp->dependent!=NULL) {assert(dp->dependent==this); --dependCnt; dp->dependent=NULL;}
}

void PBlock::fillaio(int op,void (*callback)(void*,RC,bool)) const
{
	aio->aio_buf=frame; aio->aio_nbytes=mgr->lPage; aio->aio_pb=(PBlock*)this;
	aio->aio_ctx=mgr->ctx; aio->aio_notify=callback; aio->aio_lio_opcode=op;
	aio->aio_fid=FileIDFromPageID(pageID); aio->aio_offset=PageIDToOffset(pageID,mgr->lPage);
}

RW_LockType PBlock::lockType(RW_LockType lt)
{
	return lt!=RW_S_LOCK?lt:(state&BLOCK_DIRTY)==0||redoLSN>=mgr->ctx->getOldLSN()?RW_S_LOCK:RW_U_LOCK;
}

RC PBlock::load(PageMgr *pm,unsigned flags)
{
	pageMgr=pm; RC rc=RC_OK;
	if (!mgr->fInMem) {setStateBits(BLOCK_IO_READ); rc=readResult(mgr->ctx->fileMgr->io(FIO_READ,pageID,frame,mgr->lPage));}
	else {const_cast<byte*&>(frame)=(byte*)mgr->ctx->memory+mgr->lPage*pageID; assert(mgr->ctx->memory!=NULL);}
	return rc;
}

namespace AfyKernel
{
	class AsyncWriteReq : public Request
	{
		PBlock	*const	pb;
	public:
		AsyncWriteReq(PBlock *p) : pb(p) {}
		void process() {pb->saveAsync();}
		void destroy() {asyncWriteReqs.dealloc(this);}
	};
};

void PBlock::saveAsync()
{
	if (mgr->fInMem || !mgr->flushLock.trylock(RW_S_LOCK)) return;
	RWLockP flck(&mgr->flushLock);
	if (!mgr->prepareForSave(this)) {mgr->endSave(this); return;}
	if (pageMgr!=NULL) {
		LSN lsn(pageMgr->getLSN(frame,mgr->lPage));
		if (!lsn.isNull() && mgr->ctx->logMgr->flushTo(lsn)!=RC_OK  || !pageMgr->beforeFlush(frame,mgr->lPage,pageID)) {mgr->endSave(this); return;}
	}
	unsigned bits=BLOCK_IO_WRITE|BLOCK_ASYNC_IO,cnt=0;
	for (PBlock *pb=this; pb->dependent!=NULL; pb=pb->dependent) if (++cnt>=FLUSH_CHAIN_THR) {bits|=BLOCK_FLUSH_CHAIN; break;}
	setStateBits(bits); fillaio(LIO_WRITE,BufMgr::asyncWriteNotify); ++mgr->asyncWriteCount; assert(QE->isFixed());
	mgr->ctx->fileMgr->listIO(LIO_NOWAIT,1,&aio);
}

bool PBlock::save()
{
	if ((state&BLOCK_DIRTY)==0) return false; assert(!mgr->fInMem && QE->getKey()==pageID && QE->isFixed());
	if (isDependent()) {
		if (!depList.isInList()) {RWLockP lck(&mgr->depQLock,RW_X_LOCK); mgr->depList.insertLast(&depList);}
		return false;
	}
	if (depList.isInList()) {RWLockP lck(&mgr->depQLock,RW_X_LOCK); depList.remove();}
	if (!mgr->flushLock.trylock(RW_S_LOCK)) return false; RWLockP flck(&mgr->flushLock);
	return RequestQueue::postRequest(new(asyncWriteReqs.alloc(sizeof(AsyncWriteReq))) AsyncWriteReq(this),mgr->ctx,RQ_IO);
}

void PBlock::release(unsigned flags,Session *ses)
{
	assert(QE->getKey()==pageID && QE->isFixed());
	if ((ses!=NULL || (ses=Session::getSession())!=NULL) && !ses->unlatch(this,flags)) return;
	if ((flags&PGCTL_DISCARD)!=0 || (state&(BLOCK_NEW_PAGE|BLOCK_REDO_SET))==BLOCK_NEW_PAGE)
		{assert(!isDependent()); if (mgr->drop(this,(flags&QMGR_UFORCE)!=0)) destroy(); return;}
	if (!isDependent() && (isXLocked() || isULocked() && (flags&QMGR_UFORCE)!=0)) {
		bool fS=redoLSN<mgr->ctx->getOldLSN(); unsigned cnt=0;
		if (!fS && dependent!=NULL) for (PBlock *pb=this; pb->dependent!=NULL; pb=pb->dependent)
			if (++cnt>=FLUSH_CHAIN_THR) {fS=true; setStateBits(BLOCK_FLUSH_CHAIN); break;}
		if (fS && save()) return;
	}
	mgr->release(this,(flags&QMGR_UFORCE)!=0);
}

RC PBlock::flushBlock()
{
	RC rc=RC_OK; if (mgr->fInMem) return RC_OK;
	setStateBits(BLOCK_IO_WRITE); assert(!isDependent());
	const bool fChain=(state&BLOCK_FLUSH_CHAIN)!=0;
	if (pageMgr!=NULL) {
		LSN lsn(pageMgr->getLSN(frame,mgr->lPage)); 
		if (!lsn.isNull()) rc=mgr->ctx->logMgr->flushTo(lsn);
		if (!pageMgr->beforeFlush(frame,mgr->lPage,pageID)) rc=RC_CORRUPTED;
	}
	if (rc==RC_OK) {
		if ((rc=mgr->ctx->fileMgr->io(FIO_WRITE,pageID,frame,mgr->lPage))!=RC_OK) {
			report(rc==RC_REPEAT?MSG_WARNING:MSG_ERROR,"Write error %d for page %08X\n",rc,pageID);
			//error processing
		}
		if (pageMgr!=NULL) {
			if (mgr->ctx->getEncKey()!=NULL) pageMgr->afterIO(this,mgr->lPage,false);
			if (rc==RC_OK && !pageMgr->getLSN(frame,mgr->lPage).isNull()) 
				mgr->ctx->logMgr->insert(NULL,LR_FLUSH,pageMgr->getPGID(),pageID);
		}
	}
	assert((state&(BLOCK_IO_READ|BLOCK_IO_WRITE))==BLOCK_IO_WRITE);
	if (rc!=RC_OK) resetStateBits(BLOCK_IO_WRITE|BLOCK_FLUSH_CHAIN);
	else {
		resetStateBits(BLOCK_IO_WRITE|BLOCK_DIRTY|BLOCK_REDO_SET|BLOCK_NEW_PAGE|BLOCK_FLUSH_CHAIN);
		if (flushList.isInList()) {RWLockP flck(&mgr->flushQLock,RW_X_LOCK); flushList.remove(); --mgr->dirtyCount;}
		if (dependent!=NULL) {
			PBlock *pb=fChain&&dependent->dependCnt==1?mgr->lockForSave(dependent->pageID,true):NULL;
			assert(dependent->dependCnt>0); --dependent->dependCnt; dependent=NULL;
			if (pb!=NULL) {
				if (pb->depList.isInList()) {RWLockP lck(&mgr->depQLock,RW_X_LOCK); pb->depList.remove();}
				if (!RequestQueue::postRequest(new(asyncWriteReqs.alloc(sizeof(AsyncWriteReq))) AsyncWriteReq(pb),mgr->ctx,RQ_IO)) mgr->endSave(pb);
			}
		}
	}
	return rc;
}

void PBlock::checkDepth()
{
	unsigned cnt=0;
	if (!isDependent()) for (PBlock *pb=this; pb->dependent!=NULL; pb=pb->dependent) if (++cnt>=FLUSH_CHAIN_THR) {
		if (mgr->saveLock==RW_X_LOCK && !isXLocked()) {assert(isULocked()); upgradeLock();} 
		setStateBits(BLOCK_FLUSH_CHAIN); flushBlock(); break;
	}
}

void PBlock::destroy()
{
	if (vb!=NULL) {vb->release(); vb=NULL;}
	if (flushList.isInList()) {RWLockP lck(&mgr->flushQLock,RW_X_LOCK); flushList.remove(); --mgr->dirtyCount;}
	if (dependent!=NULL) {assert(dependent->dependCnt>0); --dependent->dependCnt;}
	if (pageList.isInList()) {RWLockP lck(mgr->fRT?NULL:&mgr->pageQLock,RW_X_LOCK); pageList.remove();}
	pageID=INVALID_PAGEID; mgr=NULL; InterlockedPushEntrySList(&BufMgr::freeBuffers,(SLIST_ENTRY*)this);
}

RC PBlock::readResult(RC rc,bool fAsync)
{
	assert(isXLocked() && (state&(BLOCK_IO_READ|BLOCK_IO_WRITE|BLOCK_DIRTY))==BLOCK_IO_READ);
	if (rc!=RC_OK) {
		Session *ses=Session::getSession();
		if (ses==NULL || ses->getExtAddr().pageID!=pageID)
			report(rc==RC_REPEAT?MSG_WARNING:MSG_ERROR, "Read%s error %d for page %08X\n",(state&BLOCK_ASYNC_IO)!=0?"-ahead":"",rc,pageID);
	} else if (pageMgr!=NULL && !pageMgr->afterIO(this,mgr->lPage,true)) rc=RC_CORRUPTED;
	resetStateBits(BLOCK_IO_READ|BLOCK_ASYNC_IO);
	return rc;
}

void PBlock::writeResult(RC rc,bool fAsync)
{
	assert(QE->isFixed());
	if ((state&BLOCK_DISCARDED)!=0) {
		if (mgr->drop(this)) destroy();		// destroy inside drop() ???
	} else {
		PBlock *dep=NULL; const bool fChain=(state&BLOCK_FLUSH_CHAIN)!=0;
		if (pageMgr!=NULL && mgr->ctx->getEncKey()!=NULL) pageMgr->afterIO(this,mgr->lPage,false);
		if (rc!=RC_OK) {
			report(rc==RC_REPEAT?MSG_WARNING:MSG_ERROR,"Write error %d for page %08X\n",rc,pageID);
			resetStateBits(BLOCK_IO_WRITE|BLOCK_ASYNC_IO|BLOCK_FLUSH_CHAIN);
			// error processing
		} else {
			resetStateBits(BLOCK_IO_WRITE|BLOCK_DIRTY|BLOCK_REDO_SET|BLOCK_ASYNC_IO|BLOCK_NEW_PAGE|BLOCK_FLUSH_CHAIN);
			if (flushList.isInList()) {RWLockP flck(&mgr->flushQLock,RW_X_LOCK); flushList.remove(); --mgr->dirtyCount;}
			if (pageMgr!=NULL && !pageMgr->getLSN(frame,mgr->lPage).isNull()) mgr->ctx->logMgr->insert(NULL,LR_FLUSH,pageMgr->getPGID(),pageID);
			dep=dependent; dependent=NULL;
		}
		BufMgr *mg=mgr; mg->endSave(this);
		if (dep!=NULL) {
			assert(dep->dependCnt!=0);
			if (dep->dependCnt>1 || !fChain) --dep->dependCnt;
			else {
				PBlock *pb=mg->lockForSave(dep->pageID,!fAsync); dep->setStateBits(BLOCK_FLUSH_CHAIN);
				--dep->dependCnt; if (pb!=NULL && !pb->save()) mg->endSave(pb);
			}
		}
	}
}

PBlock *PBlock::createNew(PageID pid,void *mg)
{
	PBlock *ret=(PBlock*)InterlockedPopEntrySList(&BufMgr::freeBuffers);
	if (ret!=NULL) {new(ret) PBlock((BufMgr*)(BufQMgr*)mg,ret->frame,ret->aio); ret->pageID=pid;}
	return ret;
}

void PBlock::waitResource(void *mg)
{
	//no available pages in buffer
	BufMgr *mgr=(BufMgr*)(BufQMgr*)mg;
	if (mgr->asyncWriteCount==0 && mgr->flushLock.trylock(RW_S_LOCK)) {
		RWLockP flck(&mgr->flushLock); RWLockP lck(&mgr->depQLock,RW_X_LOCK);
		for (HChain<PBlock>::it_r it(&mgr->depList); ++it;) {
			PBlock *pb=it.get(); assert((pb->state&BLOCK_DIRTY)!=0);
			if (!pb->isDependent() && (pb=mgr->trylock(pb,RW_X_LOCK))!=NULL) {
				pb->depList.remove(); RequestQueue::postRequest(new(asyncWriteReqs.alloc(sizeof(AsyncWriteReq))) AsyncWriteReq(pb),mgr->ctx,RQ_IO);
			}
		}
#if 0
		RWLockP flck(&mgr->flushLock); RWLockP lck(&mgr->pageQLock,RW_X_LOCK);
		for (HChain<PBlock>::it it(&mgr->pageList); ++it;) {
			PBlock *pb=it.get();
			if (!pb->isDependent()) {
				for (; pb!=NULL; pb=pb->dependent) {
					printf("%04X(%d:%s:%s:%s) %s",pb->pageID,pb->dependCnt,pb->isXLocked()?"X":pb->isULocked()?"U":"",pb->QE->isInList()?"^":"",pb->depList.isInList()?"?":"",pb->dependent!=NULL?"-> ":"\n");
				}
			}
		}
		printf("-----------------------------------------\n");
#endif
	}
	threadYield();
}

void PBlock::signal(void*)
{
}

void PBlock::initNew()
{
	RWLockP lck(mgr->fRT?NULL:&mgr->pageQLock,RW_X_LOCK); mgr->pageList.insertFirst(&pageList);
}

void PBlock::setKey(PageID pid,void *mg)
{
	if (vb!=NULL) {vb->release(); vb=NULL;}
	if (mgr!=NULL && pageList.isInList()) {RWLockP lck(mgr->fRT?NULL:&mgr->pageQLock,RW_X_LOCK); pageList.remove();}
	pageID=pid; mgr=(BufMgr*)(BufQMgr*)mg;
	RWLockP lck(mgr->fRT?NULL:&mgr->pageQLock,RW_X_LOCK); mgr->pageList.insertFirst(&pageList);
}

PBlock*	PBlockP::getPage(PageID pid,PageMgr *mgr,unsigned f,Session *ses)
{
	if (pb==NULL || pb->getPageID()!=pid || (f&(PGCTL_XLOCK|PGCTL_ULOCK))!=0 && !pb->isXLocked()) {
		PBlock *tmp=(flags&PGCTL_NOREL)==0?pb:(PBlock*)0; pb=NULL; if (ses==NULL) ses=Session::getSession();
		pb=(ses!=NULL?ses->getStore():StoreCtx::get())->bufMgr->getPage(pid,mgr,(f&~QMGR_UFORCE)|(flags&QMGR_UFORCE),tmp,ses);
		flags=f|((f&(PGCTL_XLOCK|PGCTL_ULOCK))!=0?QMGR_UFORCE:0);
	}
	return pb;
}

PBlock*	PBlockP::newPage(PageID pid,PageMgr *mgr,unsigned f,Session *ses)
{
	PBlock *tmp=(flags&PGCTL_NOREL)==0?pb:(PBlock*)0; pb=NULL; if (ses==NULL) ses=Session::getSession();
	pb=(ses!=NULL?ses->getStore():StoreCtx::get())->bufMgr->newPage(pid,mgr,tmp,(f&~QMGR_UFORCE)|(flags&QMGR_UFORCE),ses);
	flags=f|((f&(PGCTL_XLOCK|PGCTL_ULOCK))!=0?QMGR_UFORCE:0); return pb;
}
