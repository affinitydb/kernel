/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "buffer.h"
#include "fio.h"
#include "logmgr.h"
#include "logchkp.h"
#include "session.h"

using namespace MVStoreKernel;

bool BufMgr::fInit = false;
Mutex BufMgr::initLock;
SLIST_HEADER BufMgr::freeBuffers;
ulong BufMgr::nBuffers = 0;
ulong BufMgr::xBuffers = 0;
volatile long BufMgr::nStores = 0;

namespace MVStoreKernel
{
BufQMgr::QueueCtrl<SERVER_HEAP> bufCtrl(0);
FreeQ<> asyncWriteReqs;
};

BufMgr::BufMgr(StoreCtx *ct,int initNumberOfBlocks,size_t lpage) 
: BufQMgr(bufCtrl,PAGE_HASH_SIZE),ctx(ct),lPage(nextP2((unsigned)lpage)),nStoreBuffers(initNumberOfBlocks),
	pageList(NULL),flushList(NULL),dirtyCount(0),maxDepDepth(initNumberOfBlocks/4)
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
	ulong nBufNew=xBuffers;		//...*log10(nStores)
	if (nBufNew>nBuffers) {
		nBufNew-=nBuffers; ulong nBufOld=nBuffers;
		for (ulong n=nBufNew; nBufNew!=0; nBufNew-=n) {
			byte *pg=NULL; if (n>nBufNew) n=nBufNew;
			while (n!=0 && (pg=(byte*)allocAligned(n*lPage,lPage))==NULL) n>>=1;
			if (pg==NULL) break;
		
			PBlock *pb=(PBlock*)::malloc(n*sizeof(PBlock));
			if (pb==NULL) {freeAligned(pg); break;}

			for (ulong i=0; i<n; ++pb,++i,pg+=lPage) 
				InterlockedPushEntrySList(&freeBuffers,(SLIST_ENTRY*)new(pb) PBlock(this,pg));
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
	MutexP lck(&pageLock);
	for (HChain<PBlock>::it it(&pageList); ++it;) {
		PBlock *pb=it.get();
		if ((pb->pageID&0xFF000000)==0 && pb->QE!=NULL && (pb->QE->isFixed() || pb->QE->isLocked()))
			report(MSG_DEBUG,"BufMgr::checkState: block is locked for %s\n",
				pb->QE->isXLocked()?"write":pb->QE->isULocked()?"update":"read");
	}
}
#endif

PBlock* BufMgr::getPage(PageID pid,PageMgr *pageMgr,ulong flags,PBlock *old)
{
	assert(old==NULL || (flags&PGCTL_NOREG)!=0 || Session::getSession()->isLatched(old));
	if (old!=NULL && old->pageID==pid) {
		if ((flags&PGCTL_XLOCK)!=0) {
			if (old->isULocked() && (flags&QMGR_UFORCE)!=0) old->upgradeLock();
			else if (!old->isXLocked()) relock(old,RW_X_LOCK);
		} else if ((flags&PGCTL_ULOCK)!=0) {
			if (old->isXLocked()) old->downgradeLock(RW_U_LOCK);
			else if (!old->isULocked() || (flags&QMGR_UFORCE)==0) relock(old,RW_U_LOCK);
		} else if (old->isXLocked() || old->isULocked() && (flags&QMGR_UFORCE)!=0)
			old->downgradeLock(RW_S_LOCK);
		return old;
	}
	PBlock *ret=NULL; Session *ses=Session::getSession();
	if (ses!=NULL && (flags&QMGR_TRY)!=0 && ses->getLatched(pid)!=NULL) {
		if (old!=NULL) {if (ses!=NULL && (flags&PGCTL_NOREG)==0) ses->unregLatched(old); old->release(flags|PGCTL_NOREG);}
		return NULL;
	}
	assert(ses==NULL || ses->getLatched(pid)==NULL);
	if (old!=NULL && ses!=NULL && (flags&(PGCTL_COUPLE|PGCTL_NOREG))==0) ses->unregLatched(old);
	ulong flg=((flags&PGCTL_XLOCK)!=0?RW_X_LOCK:(flags&PGCTL_ULOCK)!=0?RW_U_LOCK:RW_S_LOCK)|(flags&(QMGR_TRY|QMGR_UFORCE|QMGR_INMEM));
	if (pid==INVALID_PAGEID || ctx->theCB->nMaster==0 && PageNumFromPageID(pid)==0 && FileIDFromPageID(pid)==0) flags|=PGCTL_COUPLE;
	else if (get(ret,pid,pageMgr,flg,(flags&PGCTL_COUPLE)==0?old:NULL)!=RC_OK) {assert(ret==0);}
	else {
		assert(ret->QE->getKey()==ret->pageID && ret->QE->isFixed());
		if (pageMgr!=ret->pageMgr) {
			if (ret->pageMgr==NULL) ret->pageMgr=pageMgr;
			else if (pageMgr!=NULL) {
				if (ses==NULL || ses->getExtAddr().pageID!=ret->pageID)
					report(MSG_ERROR,"PageMgr error: request=%d, page type=%d, page %X\n",
								pageMgr->getPGID(),ret->pageMgr->getPGID(),ret->pageID);
				release(ret); ret=NULL;
			}
		}
	}
	if (old!=NULL && (flags&PGCTL_COUPLE)!=0) {
		if (ses!=NULL && (flags&PGCTL_NOREG)==0) ses->unregLatched(old); old->release(flags|PGCTL_NOREG);
	}
	if (ret!=NULL && ses!=NULL && (flags&PGCTL_NOREG)==0) ses->regLatched(ret);
	return ret;
}

PBlock *BufMgr::newPage(PageID pid,PageMgr *pageMgr,PBlock *old,ulong flags)
{
	PBlock *pb=NULL; Session *ses=Session::getSession();
	if (pid==INVALID_PAGEID || ctx->theCB->nMaster==0 && PageNumFromPageID(pid)==0 && FileIDFromPageID(pid)==0)
		{if (old!=NULL) old->release(flags); return NULL;}
	if (old!=NULL && ses!=NULL) ses->unregLatched(old);
	switch (get(pb,pid,pageMgr,QMGR_NEW|RW_X_LOCK|(flags&QMGR_UFORCE),old)) {
	case RC_ALREADYEXISTS: 
		report(MSG_ERROR,"BufMgr::newPage: page %X already exists\n",pid);
	default: assert(pb==NULL); break;
	case RC_OK:
		assert(pb->QE->getKey()==pb->pageID && pb->QE->isFixed());
		if ((pb->pageMgr=pageMgr)!=NULL) pageMgr->initPage(pb->frame,lPage,pid);
		pb->state=BLOCK_NEW_PAGE; if (ses!=NULL) ses->regLatched(pb); break;
	}
	return pb;
}

RC BufMgr::flushAll(bool fWait)
{
	myaio **pcbs; RC rc=RC_OK; int cnt,ncbs;
	if (dirtyCount!=0) {
		if ((pcbs=(myaio**)ctx->malloc((ncbs=dirtyCount)*sizeof(myaio*)))==NULL) return RC_NORESOURCES;
		do {
			LSN flushLSN(0); cnt=0; pageLock.lock(); PBlock *pb;
			for (HChain<PBlock>::it it(&pageList); cnt<ncbs && rc==RC_OK && ++it;)
				if ((pb=lockForSave(it.get()->getPageID(),true))!=NULL) {
					if ((pb->state&(BLOCK_DIRTY|BLOCK_IO_WRITE))!=BLOCK_DIRTY || pb->isDependent()) endSave(pb);
					else {
						if (pb->aio==NULL && !pb->setaio()) rc=RC_NORESOURCES;
						else if (pb->pageMgr!=NULL) {
							LSN lsn(pb->pageMgr->getLSN(pb->frame,lPage)); if (lsn>flushLSN) flushLSN=lsn;
							if (!pb->pageMgr->beforeFlush(pb->frame,lPage,pb->pageID)) rc=RC_CORRUPTED;
						}
						if (rc!=RC_OK) endSave(pb); else {pb->fillaio(LIO_WRITE,NULL); pcbs[cnt++]=pb->aio;}
					}
				}
			pageLock.unlock(); if (cnt==0) break;
			if (flushLSN.isNull() || (rc=ctx->logMgr->flushTo(flushLSN))==RC_OK) rc=ctx->fileMgr->listIO(LIO_WAIT,cnt,pcbs);
			for (int i=0; i<cnt; i++) pcbs[i]->aio_pb->writeResult(rc);
		} while (rc==RC_OK && dirtyCount!=0);
		ctx->free(pcbs);
	}
	if (fWait) while (asyncWriteCount+asyncReadCount!=0) threadYield();	// check timeout too!
	return rc;
}

RC BufMgr::close(FileID fid,bool fAll)
{
	pageLock.lock();
	for (HChain<PBlock>::it_r it(&pageList); ++it;) {
		PBlock *pb=it.get();
		if (fAll || FileIDFromPageID(pb->pageID)==fid) {
			bool fDel=drop(pb,false,false); pb->pageList.remove(); 
			if (pb->flushList.isInList()) {MutexP lck(&flushLock); pb->flushList.remove(); dirtyCount--;}
			if (pb->dependent!=NULL) {assert(pb->dependent->dependCnt>0); --pb->dependent->dependCnt;}
			if (fDel) {pb->pageID=INVALID_PAGEID; pb->mgr=NULL; InterlockedPushEntrySList(&freeBuffers,(SLIST_ENTRY*)pb);}
		}
	}
	assert(!fAll || !pageList.isInList());
	pageLock.unlock(); return fAll?RC_OK:ctx->fileMgr->close(fid);
}

void BufMgr::prefetch(const PageID *pages,int nPages,PageMgr *pageMgr,PageMgr *const *mgrs)
{
	if (pages==NULL || nPages==0 || !ctx->fileMgr->asyncIOEnabled()) return;
	myaio **pcbs=(myaio**)malloc(nPages*sizeof(myaio*),SES_HEAP); if (pcbs==NULL) return;
	
	int cnt=0; PBlock *pb=NULL;
	for (int i=0; i<nPages; i++) {
		if (get(pb,pages[i],pageMgr,QMGR_NEW|RW_X_LOCK)!=RC_OK) continue;
		assert(pb->pageID==pages[i] && pb->QE->getKey()==pb->pageID && pb->QE->isFixed());
		pb->pageMgr=mgrs!=NULL?mgrs[i]:pageMgr; pb->setStateBits(BLOCK_IO_READ|BLOCK_ASYNC_IO);
		if (pb->aio==NULL && !pb->setaio()) {pb->release(PGCTL_DISCARD); continue;}	//????
		pb->fillaio(LIO_READ,BufMgr::asyncReadNotify); ++asyncReadCount; pcbs[cnt++]=pb->aio;
	}
	if (cnt>0) {
		RC rc=ctx->fileMgr->listIO(LIO_NOWAIT,cnt,pcbs);
		if (rc!=RC_OK) report(MSG_ERROR,"Read-ahead failed: %d\n",rc);
	}
	free(pcbs,SES_HEAP);
}

void BufMgr::asyncReadNotify(void *p,RC rc)
{
	PBlock *pb=(PBlock*)p; assert((pb->state&BLOCK_ASYNC_IO)!=0);
	pb->readResult(rc); pb->mgr->endLoad(pb); --pb->mgr->asyncReadCount;
}

void BufMgr::asyncWriteNotify(void *p,RC rc) 
{
	PBlock *pb=(PBlock*)p; BufMgr *mgr=pb->mgr; assert(mgr->asyncWriteCount>0); pb->writeResult(rc); --mgr->asyncWriteCount;
}

LogDirtyPages *BufMgr::getDirtyPageInfo(LSN old,LSN& redo,PageID *asyncPages,ulong& nAsyncPages,ulong maxAsyncPages)
{
	LogDirtyPages *ldp=(LogDirtyPages*)ctx->malloc(sizeof(LogDirtyPages)+int(dirtyCount-1)*sizeof(LogDirtyPages::LogDirtyPage));
	if (ldp!=NULL) {
		ulong cnt=0; nAsyncPages=0; LSN *flushLSN=asyncPages!=NULL?(LSN*)alloca(maxAsyncPages*sizeof(LSN)):(LSN*)0;
		{MutexP flck(&flushLock);
		for (HChain<PBlock>::it it(&flushList); ++it;) {
			PBlock *pb=it.get(); if ((pb->state&BLOCK_DIRTY)==0) continue;
			assert(cnt<dirtyCount);
			if ((ldp->pages[cnt].redo=pb->redoLSN)<redo) redo=pb->redoLSN;
			ldp->pages[cnt].pageID=pb->pageID; cnt++;
			if (!pb->isDependent() && pb->redoLSN<=old && (flushLSN!=NULL && nAsyncPages<maxAsyncPages||pb->redoLSN<flushLSN[nAsyncPages-1])) {
				ulong n=nAsyncPages,base=0,k=0;
				while (n!=0) {
					LSN lsn=flushLSN[(k=n>>1)+base];
					if (lsn>pb->redoLSN || lsn==pb->redoLSN && asyncPages[k+base]<pb->pageID) n=k; 
					else {base+=k+1; n-=k+1;}
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
		ldp->nPages=cnt; assert(cnt<=dirtyCount);}
	}
	return ldp;
}

void BufMgr::writeAsyncPages(const PageID *asyncPages,ulong nAsyncPages)
{
	ulong cnt=0; LSN flushLSN(0); RC rc; assert(asyncPages!=NULL && nAsyncPages>0);
	myaio **pcbs=(myaio**)alloca(nAsyncPages*sizeof(myaio*)); if (pcbs==NULL) return;
	for (ulong i=0; i<nAsyncPages; i++) {
		PBlock *pb=lockForSave(asyncPages[i],true);
		if (pb!=NULL) {
			bool fOK=true;
			if ((pb->state&(BLOCK_DIRTY|BLOCK_IO_WRITE))!=BLOCK_DIRTY || pb->isDependent() || pb->aio==NULL && !pb->setaio()) fOK=false;
			else if (pb->pageMgr!=NULL) {
				LSN lsn(pb->pageMgr->getLSN(pb->frame,lPage)); if (lsn>flushLSN) flushLSN=lsn;
				fOK=pb->pageMgr->beforeFlush(pb->frame,lPage,pb->pageID);
			}
			if (!fOK) endSave(pb); else {pb->fillaio(LIO_WRITE,BufMgr::asyncWriteNotify); pcbs[cnt++]=pb->aio; ++asyncWriteCount;}
		}
	}
	if (cnt>0) {
		if (!flushLSN.isNull() && (rc=ctx->logMgr->flushTo(flushLSN))!=RC_OK)
			for (ulong i=0; i<cnt; i++) {--asyncWriteCount; pcbs[i]->aio_pb->writeResult(rc);}
		else
			ctx->fileMgr->listIO(LIO_NOWAIT,cnt,pcbs);
	}
}

PBlock::PBlock(BufMgr *bm,byte *frm,myaio *ai)
	: pageID(INVALID_PAGEID),state(0),frame(frm),pageMgr(NULL),redoLSN(0),aio(ai),
	QE(NULL),dependent(NULL),vb(NULL),mgr(bm),pageList(this),flushList(this)
{
} 

PBlock::~PBlock() 
{
	assert(0);
}

void PBlock::setRedo(LSN lsn) {
	assert(isXLocked()); 
	if ((state&(BLOCK_REDO_SET|BLOCK_DISCARDED))==0) {
		MutexP flck(&mgr->flushLock); 
		if ((state&(BLOCK_REDO_SET|BLOCK_DISCARDED|BLOCK_DIRTY))==0) {
			assert(!flushList.isInList());
			redoLSN=lsn; setStateBits(BLOCK_DIRTY|BLOCK_REDO_SET);
			mgr->flushList.insertLast(&flushList); mgr->dirtyCount++;
		}
	}
#ifdef _DEBUG
	else assert((state&BLOCK_DIRTY)!=0);
#endif
}

bool PBlock::setaio()
{
	if ((aio=(myaio*)::malloc(sizeof(myaio)))==NULL) return false;
	memset(aio,0,sizeof(myaio)); aio->aio_param=aio->aio_pb=this;
	aio->aio_buf=frame; aio->aio_nbytes=mgr->lPage;
	return true;
}

void PBlock::fillaio(int op,void (*callback)(void*,RC)) const
{
	aio->aio_param=(void*)this; aio->aio_notify=callback; aio->aio_lio_opcode=op;
	aio->aio_fildes=FileIDFromPageID(pageID); aio->aio_offset=PageIDToOffset(pageID,mgr->lPage);
}

RW_LockType PBlock::lockType(RW_LockType lt)
{
	return lt!=RW_S_LOCK?lt:(state&BLOCK_DIRTY)==0||redoLSN>=mgr->ctx->getOldLSN()?RW_S_LOCK:RW_U_LOCK;
}

RC PBlock::load(PageMgr *pm,ulong flags)
{
	pageMgr=pm; setStateBits(BLOCK_IO_READ); return readResult(mgr->ctx->fileMgr->io(FIO_READ,pageID,frame,mgr->lPage));
}

namespace MVStoreKernel
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
	if (!mgr->prepareForSave(this)) {mgr->endSave(this); return;}
	if (pageMgr!=NULL) {
		LSN lsn(pageMgr->getLSN(frame,mgr->lPage));
		if (!lsn.isNull() && mgr->ctx->logMgr->flushTo(lsn)!=RC_OK  || !pageMgr->beforeFlush(frame,mgr->lPage,pageID)) {mgr->endSave(this); return;}
	}
	ulong bits=BLOCK_IO_WRITE|BLOCK_ASYNC_IO,cnt=0;
	if (dependent!=NULL) for (PBlock *pb=this; pb->dependent!=NULL; pb=pb->dependent) if (++cnt>=FLUSH_CHAIN_THR) {bits|=BLOCK_FLUSH_CHAIN; break;}
	setStateBits(bits); fillaio(LIO_WRITE,BufMgr::asyncWriteNotify); ++mgr->asyncWriteCount; assert(QE->isFixed());
	mgr->ctx->fileMgr->listIO(LIO_NOWAIT,1,&aio);
}

bool PBlock::save()
{
	assert(QE->getKey()==pageID && QE->isFixed());
	if ((state&BLOCK_DIRTY)==0 || aio==NULL && !setaio()) return false;
	if (isDependent()) {if (pageMgr!=NULL) pageMgr->unchain(this); return false;}	// ctx? move to asyncSave?
	return RequestQueue::postRequest(new(asyncWriteReqs.alloc(sizeof(AsyncWriteReq))) AsyncWriteReq(this),mgr->ctx,RQ_IO);
}

void PBlock::release(ulong flags) 
{
	if ((flags&PGCTL_NOREG)==0) {Session *ses=Session::getSession(); if (ses!=NULL) ses->unregLatched(this);}
	assert(QE->getKey()==pageID && QE->isFixed());
	if ((flags&PGCTL_DISCARD)!=0 || (state&(BLOCK_NEW_PAGE|BLOCK_REDO_SET))==BLOCK_NEW_PAGE)
		{assert(!isDependent()); if (mgr->drop(this,(flags&QMGR_UFORCE)!=0)) destroy(); return;}
	bool fSaved=false;
	if (isDependent()) {
		if (pageMgr!=NULL && redoLSN<mgr->ctx->getOldLSN()) pageMgr->unchain(this);
	} else if ((flags&PGCTL_NOREG)==0 && (isXLocked() || isULocked() && (flags&QMGR_UFORCE)!=0)) {
		bool fS=redoLSN<mgr->ctx->getOldLSN(); ulong cnt=0;
		if (!fS && dependent!=NULL) for (PBlock *pb=this; pb->dependent!=NULL; pb=pb->dependent)
			if (++cnt>=FLUSH_CHAIN_THR) {fS=true; setStateBits(BLOCK_FLUSH_CHAIN); break;}
		if (fS) fSaved=save();
	}
	if (!fSaved) mgr->release(this,(flags&QMGR_UFORCE)!=0);
}

RC PBlock::flushBlock()
{
	RC rc=RC_OK; setStateBits(BLOCK_IO_WRITE); assert(!isDependent());
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
	if (rc!=RC_OK) resetStateBits(BLOCK_IO_WRITE);
	else {
		resetStateBits(BLOCK_IO_WRITE|BLOCK_DIRTY|BLOCK_REDO_SET|BLOCK_NEW_PAGE);
		if (flushList.isInList()) {MutexP flck(&mgr->flushLock); flushList.remove(); mgr->dirtyCount--;}
		if (dependent!=NULL) {assert(dependent->dependCnt>0); --dependent->dependCnt; dependent=NULL;}
	}
	return rc;
}

void PBlock::destroy()
{
	if (vb!=NULL) {vb->release(); vb=NULL;}
	if (flushList.isInList()) {MutexP lck(&mgr->flushLock); flushList.remove(); mgr->dirtyCount--;}
	if (dependent!=NULL) {assert(dependent->dependCnt>0); --dependent->dependCnt;}
	if (pageList.isInList()) {MutexP lck(&mgr->pageLock); pageList.remove();}
	pageID=INVALID_PAGEID; mgr=NULL; InterlockedPushEntrySList(&BufMgr::freeBuffers,(SLIST_ENTRY*)this);
}

RC PBlock::readResult(RC rc)
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

void PBlock::writeResult(RC rc)
{
	assert(QE->isFixed());
	if ((state&BLOCK_DISCARDED)!=0) {
		if (mgr->drop(this)) destroy();		// destroy inside drop() ???
	} else {
		PBlock *chain=NULL;
		if (pageMgr!=NULL && mgr->ctx->getEncKey()!=NULL) pageMgr->afterIO(this,mgr->lPage,false);
		if (rc!=RC_OK) {
			report(rc==RC_REPEAT?MSG_WARNING:MSG_ERROR,"Write error %d for page %08X\n",rc,pageID);
			resetStateBits(BLOCK_IO_WRITE|BLOCK_ASYNC_IO|BLOCK_FLUSH_CHAIN);
			// error processing
		} else {
			bool fChain=(state&BLOCK_FLUSH_CHAIN)!=0;
			resetStateBits(BLOCK_IO_WRITE|BLOCK_DIRTY|BLOCK_REDO_SET|BLOCK_ASYNC_IO|BLOCK_NEW_PAGE|BLOCK_FLUSH_CHAIN);
			if (flushList.isInList()) {MutexP flck(&mgr->flushLock); flushList.remove(); mgr->dirtyCount--;}
			if (pageMgr!=NULL && !pageMgr->getLSN(frame,mgr->lPage).isNull()) mgr->ctx->logMgr->insert(NULL,LR_FLUSH,pageMgr->getPGID(),pageID);
			if (dependent!=NULL) {if (fChain) chain=mgr->trylock(dependent,RW_U_LOCK); assert(dependent->dependCnt>0); --dependent->dependCnt; dependent=NULL;}
		}
		if (chain==NULL) mgr->endSave(this);
		else if (chain->dependCnt==0) {mgr->endSave(this); if (!chain->save()) mgr->release(chain);}
		else {if (chain->pageMgr!=NULL) {downgradeLock(RW_S_LOCK); chain->pageMgr->unchain(chain,this);} mgr->endSave(this); mgr->release(chain);}
	}
}

PBlock *PBlock::createNew(PageID pid,void *mg)
{
	PBlock *ret=(PBlock*)InterlockedPopEntrySList(&BufMgr::freeBuffers);
	if (ret!=NULL) {new(ret) PBlock((BufMgr*)(BufQMgr*)mg,ret->frame,ret->aio); ret->pageID=pid;}
	return ret;
}

void PBlock::waitResource(void*)
{
	// wait on semaphore in bufCtrl
	threadYield();
}

void PBlock::signal(void*)
{
}

void PBlock::initNew()
{
	MutexP lck(&mgr->pageLock); mgr->pageList.insertFirst(&pageList);
}

void PBlock::setKey(PageID pid,void *mg)
{
	if (vb!=NULL) {vb->release(); vb=NULL;}
	if (mgr!=NULL && pageList.isInList()) {MutexP lck(&mgr->pageLock); pageList.remove();}
	pageID=pid; mgr=(BufMgr*)(BufQMgr*)mg;
	MutexP lck(&mgr->pageLock); mgr->pageList.insertFirst(&pageList);
}
