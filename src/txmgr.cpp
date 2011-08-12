/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "logmgr.h"
#include "session.h"
#include "logchkp.h"
#include "lock.h"
#include "queryprc.h"
#include "startup.h"
#include "fsmgr.h"
#include "classifier.h"

using namespace MVStore;
using namespace MVStoreKernel;

TxMgr::TxMgr(StoreCtx *cx,TXID startTXID,IStoreNotification *notItf,ulong xSnap) 
	: ctx(cx),notification(notItf),nextTXID(startTXID),nActive(0),lastTXCID(0),snapshots(NULL),nSS(0),xSS(xSnap)
{
}

void TxMgr::setTXID(TXID txid)
{
	nextTXID = txid;
}

//---------------------------------------------------------------------------------

inline static ulong txiFlags(ulong txl,bool fRO)
{
	ulong flags=fRO?TX_READONLY:0;
	switch (txl) {
	case TXI_READ_UNCOMMITTED: flags|=TX_UNCOMMITTED; break;
	case TXI_READ_COMMITTED: break;
	case TXI_SERIALIZABLE:
		//...
	case TXI_DEFAULT: case TXI_REPEATABLE_READ: if (!fRO) flags|=TX_READLOCKS; break;
	}
	return flags;
}

RC TxMgr::startTx(Session *ses,ulong txt,ulong txl)
{
	assert(ses!=NULL); RC rc;
	if (ses->getStore()->theCB->state==SST_SHUTDOWN_IN_PROGRESS) return RC_SHUTDOWN;
	switch (ses->getTxState()) {
	default: break;	// ???
	case TX_ABORTING:
		if ((rc=abort(ses))!=RC_OK) return rc;
	case TX_NOTRAN: 
		if (txl==TXI_DEFAULT) txl=ses->txil;
		if (txt==TXT_MODIFYCLASS) ses->lockClass(RW_X_LOCK);
		else if (txt==TXT_READONLY && (txl==TXI_DEFAULT||txl>=TXI_REPEATABLE_READ)) ses->txcid=assignSnapshot();
		return start(ses,txiFlags(txl,txt==TXT_READONLY));
	}
	if (txt==TXT_MODIFYCLASS && ses->classLocked!=RW_X_LOCK) return RC_DEADLOCK;
	if ((rc=ses->pushTx())!=RC_OK) return rc;
	ses->tx.subTxID=++ses->subTxCnt;
	if (notification!=NULL && (ses->txState&TX_NONOTIFY)==0)
		notification->txNotify(IStoreNotification::NTX_SAVEPOINT,ses->txid);
	return RC_OK;
}

RC TxMgr::commitTx(Session *ses,bool fAll)
{
	assert(ses!=NULL);
	switch (ses->getTxState()) {
	case TX_NOTRAN: return RC_OK;
	case TX_ABORTING: return abort(ses);
	default: break;
	}
	return commit(ses,fAll);
}

RC TxMgr::abortTx(Session *ses,bool fAll)
{
	// check TX_ABORTING, distinguish between explicit rollback and internal
	assert(ses!=NULL);
	return ses->getTxState()==TX_NOTRAN ? RC_OK : abort(ses,fAll);
}

TXCID TxMgr::assignSnapshot()
{
	Snapshot *ss=NULL; MutexP lck(&lock);
	if (nSS!=0 && snapshots!=NULL && (snapshots->txcid==lastTXCID||nSS>=xSS)) ss=snapshots;
	else if ((ss=new(ctx) Snapshot(lastTXCID,snapshots))!=NULL) {++nSS; snapshots=ss;}
	else return NO_TXCID;
	ss->txCnt++; return ss->txcid;
}

void TxMgr::releaseSnapshot(TXCID txcid)
{
	MutexP lck(&lock); assert(txcid!=NO_TXCID);
	for (Snapshot **pss=&snapshots,*ss; (ss=*pss)!=NULL; pss=&ss->next) if (ss->txcid==txcid) {
		if (--ss->txCnt==0) {
			*pss=ss->next; nSS--;
			if (ss->next!=NULL) {
				//copy data to ss->next
				lck.set(NULL);
			} else {
				lck.set(NULL);
				//free data
			}
			ctx->free(ss);
		}
		break;
	}
}

//--------------------------------------------------------------------------------------------------------

RC TxMgr::start(Session *ses,ulong flags)
{
	lock.lock();
	ses->txid=++nextTXID;
	ses->txState=TX_START|flags;
	ses->nLogRecs=0;
	if ((flags&TX_READONLY)==0) {
		assert(!ses->list.isInList());
		activeList.insertFirst(&ses->list);
		nActive++;
	}
	lock.unlock();
	if ((flags&TX_READONLY)==0) {
		RC rc=ctx->logMgr->init(); if (rc!=RC_OK) return rc;
		ses->tx.lastLSN=ses->firstLSN=ses->undoNextLSN=LSN(0);
	}
	ses->txState=TX_ACTIVE|flags;
	if (notification!=NULL && (flags&(TX_NONOTIFY|TX_READONLY))==0) 
		notification->txNotify(IStoreNotification::NTX_START,ses->txid);
	return RC_OK;
}

RC TxMgr::commit(Session *ses,bool fAll,bool fFlush)
{
	TXID txid=ses->txid; assert(ses->getTxState()==TX_ACTIVE||ses->getTxState()==TX_ABORTING);
	const bool fNotify=notification!=NULL && (ses->txState&(TX_NONOTIFY|TX_READONLY))==0;
	IStoreNotification::TxEventType type=IStoreNotification::NTX_COMMIT; RC rc; LSN commitLSN(0);
	if (ses->tx.next==NULL) fAll=true;
	else if ((rc=ses->popTx(true,fAll))!=RC_OK) return rc;
	else if (!fAll) type=IStoreNotification::NTX_COMMIT_SP;
	if (fAll) {
		assert(ses->tx.next==NULL);
		if ((ses->txState&TX_READONLY)==0 && ses->getTxState()!=TX_ABORTING) {
			ses->txState=ses->txState&~0xFFFFul|TX_COMMITTING;
			while (ses->tx.txDelete!=NULL) {
				TxDelete *td=ses->tx.txDelete; ses->tx.txDelete=td->next;
				RC rc=td->deleteData(); if (rc!=RC_OK) {cleanup(ses); return rc;}							// rollback?
				ses->free(td);
			}
			if ((ulong)ses->tx.defHeap!=0) {
				if ((rc=ctx->heapMgr->addPagesToMap(ses->tx.defHeap,ses))!=RC_OK) {cleanup(ses); return rc;}	// rollback?
				ses->tx.defHeap.cleanup(); assert(!ses->firstLSN.isNull());
				if ((ulong)ses->tx.defClass!=0) {
					if ((rc=ctx->heapMgr->addPagesToMap(ses->tx.defClass,ses,true))!=RC_OK) {cleanup(ses); return rc;}	// rollback?
					ses->tx.defClass.cleanup(); assert(!ses->firstLSN.isNull());
				}
			}
			bool fUnlock=false;
			if ((ulong)ses->tx.defFree!=0) {
				if ((rc=ctx->fsMgr->freeTxPages(ses->tx.defFree))!=RC_OK) {cleanup(ses); return rc;}			// rollback?
				ses->tx.defFree.cleanup(); fUnlock=true; assert(!ses->firstLSN.isNull());
			}
			if (ses->tx.txClass!=NULL && (rc=ses->getStore()->classMgr->classTx(ses,ses->tx.txClass))!=RC_OK) {cleanup(ses); return rc;}			// rollback?
			if (ses->tx.txIndex!=NULL) {
				// commit index changes!!!
			}
			if (!ses->firstLSN.isNull()) commitLSN=ctx->logMgr->insert(ses,LR_COMMIT);
			if (fUnlock) ctx->fsMgr->txUnlock();
	// unlock dirHeap
			if (ses->reuse.pinPages!=NULL) for (ulong i=0; i<ses->reuse.nPINPages; i++)
				ctx->heapMgr->HeapPageMgr::reuse(ses->reuse.pinPages[i].pid,ses->reuse.pinPages[i].space,ctx);
			if (ses->reuse.ssvPages!=NULL) for (ulong i=0; i<ses->reuse.nSSVPages; i++)
				ctx->ssvMgr->HeapPageMgr::reuse(ses->reuse.ssvPages[i].pid,ses->reuse.ssvPages[i].space,ctx);
			ses->txState=ses->txState&~0xFFFFul|TX_COMMITTED;
		}
		cleanup(ses);
	}
	if (fNotify) notification->txNotify(type,txid);
	if (fFlush && !commitLSN.isNull() && (ctx->mode&STARTUP_REDUCED_DURABILITY)==0) ctx->logMgr->flushTo(commitLSN);	// check rc?
	return RC_OK;
}

RC TxMgr::abort(Session *ses,bool fAll)
{
	ulong save=ses->txState; RC rc; assert(ses->getTxState()!=TX_NOTRAN);
	if ((save&TX_READONLY)==0) {
		ses->txState=ses->txState&~0xFFFFul|TX_ABORTING;
		if (!ses->firstLSN.isNull() && (rc=ctx->logMgr->rollback(ses,!fAll && ses->tx.next!=NULL))!=RC_OK) {
			report(MSG_ERROR,"Couldn't rollback transaction "_LX_FM"\n",ses->txid); 
			cleanup(ses); return rc;
		}
	}
	const TXID txid=ses->txid; LSN abortLSN(0); 
	const bool fNotify=notification!=NULL && (save&(TX_NONOTIFY|TX_READONLY))==0;
	IStoreNotification::TxEventType type=IStoreNotification::NTX_ABORT;
	if (ses->tx.next==NULL) fAll=true;
	else if ((rc=ses->popTx(false,fAll))!=RC_OK) return rc;
	else if (!fAll) {type=IStoreNotification::NTX_ABORT_SP; ses->txState=save;}
	if (fAll) {
		assert(ses->tx.next==NULL);
		if ((ses->txState&TX_READONLY)==0 && !ses->firstLSN.isNull()) abortLSN=ctx->logMgr->insert(ses,LR_ABORT);
		ses->txState=ses->txState&~0xFFFFul|TX_ABORTED; cleanup(ses,true);
	}
	if (fNotify) notification->txNotify(type,txid);
	if (!abortLSN.isNull() && (ctx->mode&STARTUP_REDUCED_DURABILITY)==0) ctx->logMgr->flushTo(abortLSN); //check rc?
	return RC_OK;
}

void TxMgr::cleanup(Session *ses,bool fAbort)
{
	if (ses->heldLocks!=NULL) ctx->lockMgr->releaseLocks(ses,0,fAbort); ses->unlockClass();
	if (ses->tx.next!=NULL) ses->popTx(false,true); ses->tx.defFree.cleanup(); ses->tx.cleanup(); ses->reuse.cleanup();
	ses->xHeapPage=INVALID_PAGEID; ses->nTotalIns=0;
	if (ses->getTxState()!=TX_NOTRAN) {
		if ((ses->txState&TX_READONLY)==0) {
			MutexP lck(&lock); assert(ses->txcid==NO_TXCID); assert(nActive>0 && ses->list.isInList()); 
			ses->list.remove(); nActive--; if ((ses->txState&TX_SYS)!=0) {ses->mini->cleanup(this); return;}
		} else if (ses->txcid!=NO_TXCID) releaseSnapshot(ses->txcid);
	}
	assert(!ses->list.isInList());
	ses->txid=INVALID_TXID; ses->txState=TX_NOTRAN; ses->txcid=NO_TXCID; ses->subTxCnt=ses->nLogRecs=0;
}

LogActiveTransactions *TxMgr::getActiveTx(LSN& start)
{
	lock.lock(); 
	LogActiveTransactions *pActive = (LogActiveTransactions*)ctx->malloc(sizeof(LogActiveTransactions) + int(nActive-1)*sizeof(LogActiveTransactions::LogActiveTx));	// ???
	if (pActive!=NULL) {
		ulong cnt = 0;
		for (HChain<Session>::it it(&activeList); ++it; ) {
			Session *ses=it.get(); assert(ses->getTxState()!=TX_NOTRAN && ses->txid!=INVALID_TXID);
			if (ses->getTxState()!=TX_COMMITTED) {
				pActive->transactions[cnt].txid=ses->txid; 
				pActive->transactions[cnt].lastLSN=ses->tx.lastLSN;
				pActive->transactions[cnt].firstLSN=ses->firstLSN;
				if (ses->firstLSN<start) start=ses->firstLSN;
				cnt++; assert(cnt<=nActive);
			}
			for (MiniTx *mtx=ses->mini; mtx!=NULL; mtx=mtx->next) {
				if ((mtx->state&TX_WASINLIST)!=0 && (mtx->state&0xFFFF)!=TX_COMMITTED) {
					pActive->transactions[cnt].txid=mtx->oldId; 
					pActive->transactions[cnt].lastLSN=mtx->tx.lastLSN;
					pActive->transactions[cnt].firstLSN=mtx->firstLSN;
					if (mtx->firstLSN<start) start=mtx->firstLSN;
					cnt++; assert(cnt<=nActive);
				}
			}
		}
		pActive->nTransactions=cnt;
	}
	lock.unlock();
	return pActive;
}

//---------------------------------------------------------------------------------

RC TxMgr::update(PBlock *pb,PageMgr *pageMgr,ulong info,const byte *rec,size_t lrec,uint32_t flags,PBlock *newp) const
{
	if (pageMgr==NULL) return RC_NOTFOUND;
	Session *ses=Session::getSession(); if (ses!=NULL && (ses->txState&TX_READONLY)!=0) return RC_READTX;
	if (pb->isULocked()) pb->upgradeLock(); assert(pb->isWritable() && (newp==NULL||newp->isXLocked()));
	RC rc=pageMgr->update(pb,ctx->bufMgr->getPageSize(),info,rec,(ulong)lrec,0,newp);		// size_t?
	if (rc==RC_OK) ctx->logMgr->insert(ses,pb->isFirstPageOp()?LR_CREATE:LR_UPDATE,info<<PGID_SHIFT|pageMgr->getPGID(),
																		pb->getPageID(),NULL,rec,lrec,flags,pb,newp);
#ifdef STRICT_UPDATE_CHECK
	else
		report(MSG_ERROR,"TxMgr::update: page %08X, pageMgr %d, error %d\n",pb->getPageID(),pageMgr->getPGID(),rc);
#endif
	return rc;
}

//----------------------------------------------------------------------------------

MiniTx::MiniTx(Session *s,ulong mtxf) : ses(s),mtxFlags(mtxf&MTX_FLUSH),tx(s)
{
	StoreCtx *ctx;
	if ((mtxf&MTX_SKIP)==0 && ses!=NULL && (ses->txState&TX_GSYS)==0 && (ctx=ses->getStore())->logMgr->init()==RC_OK) {
		oldId=ses->txid; txcid=ses->txcid; state=ses->txState|(ses->list.isInList()?TX_WASINLIST:0); identity=ses->identity; 
		memcpy(&tx,&s->tx,sizeof(SubTx)); firstLSN=ses->firstLSN; undoNextLSN=ses->undoNextLSN; 
		classLocked=ses->classLocked; reuse=ses->reuse; locks=ses->heldLocks; next=ses->mini;
		ctx->txMgr->lock.lock(); 
		ses->mini=this; newId=ses->txid=++ctx->txMgr->nextTXID; ses->txcid=NO_TXCID; ses->classLocked=RW_NO_LOCK;
		ses->txState=TX_START|TX_NONOTIFY; ses->firstLSN=ses->tx.lastLSN=ses->undoNextLSN=LSN(0);
		ses->tx.next=NULL; ses->heldLocks=NULL; ses->identity=0; new(&s->tx) SubTx(s);
		if (!ses->list.isInList()) ctx->txMgr->activeList.insertFirst(&ses->list); ctx->txMgr->nActive++; ctx->txMgr->lock.unlock();
		ses->txState=TX_ACTIVE|TX_NONOTIFY|TX_SYS|((mtxFlags&MTX_GLOB)!=0?TX_GSYS:0); mtxFlags|=MTX_STARTED;
	}
}

MiniTx::~MiniTx()
{
	if ((mtxFlags&MTX_STARTED)!=0) {
		assert(ses!=NULL && ses->txid==newId && ses->getTxState()!=TX_NOTRAN && ses->list.isInList());
		if ((mtxFlags&MTX_OK)==0) ses->getStore()->txMgr->abort(ses);				// if failed???
		else ses->getStore()->txMgr->commit(ses,false,(mtxFlags&MTX_FLUSH)!=0);		// if failed???
	}
	mtxFlags=0;
}

void MiniTx::cleanup(TxMgr *txMgr)
{
	if (ses->mini==this) ses->mini=next; assert(!ses->list.isInList());
	ses->txid=oldId; ses->txcid=txcid; ses->txState=state&~TX_WASINLIST; ses->identity=identity;
	ses->tx.cleanup(); memcpy(&ses->tx,&tx,sizeof(SubTx)); new(&tx) SubTx(ses); ses->reuse.cleanup(); ses->reuse=reuse;
	ses->firstLSN=firstLSN; ses->undoNextLSN=undoNextLSN; ses->classLocked=classLocked;
	ses->heldLocks=locks; if ((state&TX_WASINLIST)!=0) txMgr->activeList.insertFirst(&ses->list);
}

TxSP::~TxSP()
{
	if ((flags&MTX_STARTED)!=0) {
		assert(ses->getTxState()!=TX_NOTRAN && ses->list.isInList());
		if ((flags&MTX_OK)==0) ses->getStore()->txMgr->abort(ses); else ses->getStore()->txMgr->commit(ses);		// if failed ???
		flags=0;
	}
}

RC TxSP::start()
{
	return start(TXI_DEFAULT,0);
}

RC TxSP::start(ulong txl,ulong txf)
{
	if ((flags&MTX_STARTED)==0) {
		StoreCtx *ctx=ses->getStore(); RC rc;
		if (ctx->theCB->state==SST_SHUTDOWN_IN_PROGRESS) return RC_SHUTDOWN;
		if (ses->getTxState()!=TX_NOTRAN) {
			if ((rc=ses->pushTx())!=RC_OK) return rc; ses->tx.subTxID=++ses->subTxCnt;
			if (ctx->txMgr->notification!=NULL && (ses->txState&TX_NONOTIFY)==0) 
				ctx->txMgr->notification->txNotify(IStoreNotification::NTX_SAVEPOINT,ses->txid);
		} else if ((rc=ctx->txMgr->start(ses,txiFlags(txl==TXI_DEFAULT?ses->txil:txl,false)|txf))!=RC_OK) return rc;
		flags|=MTX_STARTED;
	}
	return RC_OK;
}
