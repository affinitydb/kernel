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

#include "logmgr.h"
#include "session.h"
#include "logchkp.h"
#include "lock.h"
#include "queryprc.h"
#include "startup.h"
#include "fsmgr.h"
#include "classifier.h"

using namespace Afy;
using namespace AfyKernel;

TxMgr::TxMgr(StoreCtx *cx,TXID startTXID,IStoreNotification *notItf,unsigned xSnap) 
	: ctx(cx),notification(notItf),nextTXID(startTXID),nActive(0),lastTXCID(0),snapshots(NULL),nSS(0),xSS(xSnap)
{
}

void TxMgr::setTXID(TXID txid)
{
	nextTXID = txid;
}

//---------------------------------------------------------------------------------

inline static unsigned txiFlags(unsigned txl,bool fRO)
{
	unsigned flags=fRO?TX_READONLY:0;
	switch (txl) {
	case TXI_READ_UNCOMMITTED: flags|=TX_UNCOMMITTED; break;
	case TXI_READ_COMMITTED: break;
	case TXI_SERIALIZABLE:
		//...
	case TXI_DEFAULT: case TXI_REPEATABLE_READ: if (!fRO) flags|=TX_READLOCKS; break;
	}
	return flags;
}

RC TxMgr::startTx(Session *ses,unsigned txt,unsigned txl)
{
	assert(ses!=NULL); RC rc;
	if (ses->getStore()->theCB->state==SST_SHUTDOWN_IN_PROGRESS) return RC_SHUTDOWN;
	switch (ses->getTxState()) {
	default: break;	// ???
	case TX_ABORTING:
		if ((rc=abort(ses))!=RC_OK) return rc;
	case TX_NOTRAN: 
		if (txt==TXT_MODIFYCLASS) ses->lockClass(RW_X_LOCK);
		else if (txt==TXT_READONLY && (txl==TXI_DEFAULT||txl>=TXI_REPEATABLE_READ)) ses->txcid=assignSnapshot();
		return start(ses,txiFlags(txl,txt==TXT_READONLY));
	}
	if (txt==TXT_MODIFYCLASS && ses->classLocked!=RW_X_LOCK) return RC_DEADLOCK;
	if ((rc=ses->pushTx())!=RC_OK) return rc;
	ses->tx.subTxID=++ses->subTxCnt;
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

RC TxMgr::abortTx(Session *ses,AbortType at)
{
	// check TX_ABORTING, distinguish between explicit rollback and internal
	assert(ses!=NULL);
	return ses->getTxState()==TX_NOTRAN ? RC_OK : abort(ses,at);
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

RC TxMgr::start(Session *ses,unsigned flags)
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
	return RC_OK;
}

RC TxMgr::commit(Session *ses,bool fAll,bool fFlush)
{
	RC rc; LSN commitLSN(0); assert(ses->getTxState()==TX_ACTIVE||ses->getTxState()==TX_ABORTING);
	if (ses->tx.next==NULL) fAll=true; else if ((rc=ses->popTx(true,fAll))!=RC_OK) return rc;
	if (fAll) {
		assert(ses->tx.next==NULL);
		if ((ses->txState&TX_READONLY)==0 && ses->getTxState()!=TX_ABORTING) {
			while (ses->tx.onCommit.head!=NULL) {
				OnCommit *oc=ses->tx.onCommit.head; ses->tx.onCommit.head=oc->next; ses->tx.onCommit.count--;
				rc=oc->process(ses); oc->destroy(ses); if (rc!=RC_OK) {abort(ses); return rc;}		// message???
			}
			ses->txState=ses->txState&~0xFFFFul|TX_COMMITTING; ses->tx.onCommit.count=0;
			uint32_t nPurge=0; TxPurge *tpa=ses->tx.txPurge.get(nPurge); rc=RC_OK;
			if (tpa!=NULL) {
				for (unsigned i=0; i<nPurge; i++) {if (rc==RC_OK) rc=tpa[i].purge(ses); ses->free(tpa[i].bmp); tpa[i].bmp=NULL;}
				ses->free(tpa); if (rc!=RC_OK) {cleanup(ses); return rc;}											// rollback?
			}
			if ((unsigned)ses->tx.defHeap!=0) {
				if ((rc=ctx->heapMgr->addPagesToMap(ses->tx.defHeap,ses))!=RC_OK) {cleanup(ses); return rc;}	// rollback?
				ses->tx.defHeap.cleanup(); assert(!ses->firstLSN.isNull() || (ctx->mode&STARTUP_NO_RECOVERY)!=0);
				if ((unsigned)ses->tx.defClass!=0) {
					if ((rc=ctx->heapMgr->addPagesToMap(ses->tx.defClass,ses,true))!=RC_OK) {cleanup(ses); return rc;}	// rollback?
					ses->tx.defClass.cleanup(); assert(!ses->firstLSN.isNull() || (ctx->mode&STARTUP_NO_RECOVERY)!=0);
				}
			}
			bool fUnlock=false;
			if ((unsigned)ses->tx.defFree!=0) {
				if ((rc=ctx->fsMgr->freeTxPages(ses->tx.defFree))!=RC_OK) {cleanup(ses); return rc;}			// rollback?
				ses->tx.defFree.cleanup(); fUnlock=true; assert(!ses->firstLSN.isNull() || (ctx->mode&STARTUP_NO_RECOVERY)!=0);
			}
			if (ses->tx.txIndex!=NULL) {
				// commit index changes!!!
			}
			if (!ses->firstLSN.isNull()) commitLSN=ctx->logMgr->insert(ses,LR_COMMIT);
			if (fUnlock) ctx->fsMgr->txUnlock();
	// unlock dirHeap
			if (ses->reuse.pinPages!=NULL) for (unsigned i=0; i<ses->reuse.nPINPages; i++)
				ctx->heapMgr->HeapPageMgr::reuse(ses->reuse.pinPages[i].pid,ses->reuse.pinPages[i].space,ctx);
			if (ses->reuse.ssvPages!=NULL) for (unsigned i=0; i<ses->reuse.nSSVPages; i++)
				ctx->ssvMgr->HeapPageMgr::reuse(ses->reuse.ssvPages[i].pid,ses->reuse.ssvPages[i].space,ctx);
			ses->txState=ses->txState&~0xFFFFul|TX_COMMITTED;
			if (ses->repl!=NULL) {
				// pass replication stream to ctx->queryMgr->replication
			}
		}
		cleanup(ses);
	}
	if (fFlush && !commitLSN.isNull() && (ctx->mode&STARTUP_REDUCED_DURABILITY)==0) ctx->logMgr->flushTo(commitLSN);	// check rc?
	return RC_OK;
}

RC TxMgr::abort(Session *ses,AbortType at)
{
	RC rc; unsigned save; LSN abortLSN(0); assert(ses->getTxState()!=TX_NOTRAN);
	do {
		save=ses->txState;
		if ((save&TX_READONLY)==0) {
			ses->txState=ses->txState&~0xFFFFul|TX_ABORTING;
			if (!ses->firstLSN.isNull() && (rc=ctx->logMgr->rollback(ses,at!=TXA_ALL && ses->tx.next!=NULL))!=RC_OK) {
				report(MSG_ERROR,"Couldn't rollback transaction " _LX_FM "\n",ses->txid); 
				cleanup(ses); return rc;
			}
		}
		if (ses->tx.next==NULL) at=TXA_ALL;
		else if ((rc=ses->popTx(false,at==TXA_ALL))!=RC_OK) return rc;
		else if (at!=TXA_ALL) ses->txState=save;
	} while (at==TXA_EXTERNAL && (save&TX_INTERNAL)!=0);
	if (at==TXA_ALL) {
		assert(ses->tx.next==NULL);
		if ((ses->txState&TX_READONLY)==0 && !ses->firstLSN.isNull()) abortLSN=ctx->logMgr->insert(ses,LR_ABORT);
		ses->txState=ses->txState&~0xFFFFul|TX_ABORTED; cleanup(ses,true);
	}
	if (!abortLSN.isNull() && (ctx->mode&STARTUP_REDUCED_DURABILITY)==0) ctx->logMgr->flushTo(abortLSN); //check rc?
	return RC_OK;
}

void TxMgr::cleanup(Session *ses,bool fAbort)
{
	if (ses->heldLocks!=NULL) ctx->lockMgr->releaseLocks(ses,0,fAbort); ses->unlockClass();
	if (ses->tx.next!=NULL) ses->popTx(false,true); ses->tx.defFree.cleanup(); ses->tx.cleanup(); ses->reuse.cleanup();
	ses->xHeapPage=INVALID_PAGEID; ses->nTotalIns=0; delete ses->repl; ses->repl=NULL;
	if (ses->getTxState()!=TX_NOTRAN) {
		if ((ses->txState&TX_READONLY)==0) {
			MutexP lck(&lock); assert(ses->txcid==NO_TXCID); assert(nActive>0 && ses->list.isInList()); 
			ses->list.remove(); nActive--; if ((ses->txState&TX_SYS)!=0) {ses->mini->cleanup(this); return;}
		} else if (ses->txcid!=NO_TXCID) releaseSnapshot(ses->txcid);
	}
	assert(!ses->list.isInList());
	ses->txid=INVALID_TXID; ses->txState=TX_NOTRAN; ses->txcid=NO_TXCID; ses->subTxCnt=ses->nLogRecs=0;
	ses->nSyncStack=ses->tx.onCommit.count=0;
	ses->xSyncStack=ses->ctx!=NULL?ses->ctx->theCB->xSyncStack:DEFAULT_MAX_SYNC_ACTION;
	ses->xOnCommit=ses->ctx!=NULL?ses->ctx->theCB->xOnCommit:DEFAULT_MAX_ON_COMMIT;
}

LogActiveTransactions *TxMgr::getActiveTx(LSN& start)
{
	lock.lock(); 
	LogActiveTransactions *pActive = (LogActiveTransactions*)ctx->malloc(sizeof(LogActiveTransactions) + int(nActive-1)*sizeof(LogActiveTransactions::LogActiveTx));	// ???
	if (pActive!=NULL) {
		unsigned cnt = 0;
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

RC TxMgr::update(PBlock *pb,PageMgr *pageMgr,unsigned info,const byte *rec,size_t lrec,uint32_t flags,PBlock *newp) const
{
	if (pageMgr==NULL) return RC_NOTFOUND;
	Session *ses=Session::getSession(); if (ses!=NULL && (ses->txState&TX_READONLY)!=0) return RC_READTX;
	if (pb->isULocked()) pb->upgradeLock(); assert(pb->isWritable() && (newp==NULL||newp->isXLocked()));
	RC rc=pageMgr->update(pb,ctx->bufMgr->getPageSize(),info,rec,(unsigned)lrec,0,newp);		// size_t?
	if (rc==RC_OK) {
		if (ctx->memory!=NULL && pb->isFirstPageOp()) {pb->resetNewPage(); assert(newp==NULL);}
		else {
			if (ctx->memory!=NULL && newp!=NULL && newp->isFirstPageOp()) {newp->resetNewPage(); /* ???? */}
			ctx->logMgr->insert(ses,pb->isFirstPageOp()?LR_CREATE:LR_UPDATE,info<<PGID_SHIFT|pageMgr->getPGID(),
																	pb->getPageID(),NULL,rec,lrec,flags,pb,newp);
		}
	}
#ifdef STRICT_UPDATE_CHECK
	else
		report(MSG_ERROR,"TxMgr::update: page %08X, pageMgr %d, error %d\n",pb->getPageID(),pageMgr->getPGID(),rc);
#endif
	return rc;
}

//----------------------------------------------------------------------------------

MiniTx::MiniTx(Session *s,unsigned mtxf) : ses(s),mtxFlags(mtxf&MTX_FLUSH),tx(s)
{
	StoreCtx *ctx;
	if ((mtxf&MTX_SKIP)==0 && ses!=NULL && (ses->txState&TX_GSYS)==0 && (ctx=ses->getStore())->logMgr->init()==RC_OK) {
		oldId=ses->txid; txcid=ses->txcid; state=ses->txState|(ses->list.isInList()?TX_WASINLIST:0); identity=ses->identity; 
		memcpy(&tx,&s->tx,sizeof(SubTx)); firstLSN=ses->firstLSN; undoNextLSN=ses->undoNextLSN; 
		classLocked=ses->classLocked; reuse=ses->reuse; locks=ses->heldLocks; next=ses->mini;
		ctx->txMgr->lock.lock(); 
		ses->mini=this; newId=ses->txid=++ctx->txMgr->nextTXID; ses->txcid=NO_TXCID; ses->classLocked=RW_NO_LOCK;
		ses->txState=TX_START; ses->firstLSN=ses->tx.lastLSN=ses->undoNextLSN=LSN(0);
		ses->tx.next=NULL; ses->heldLocks=NULL; ses->identity=0; new(&s->tx) SubTx(s);
		if (!ses->list.isInList()) ctx->txMgr->activeList.insertFirst(&ses->list); ctx->txMgr->nActive++; ctx->txMgr->lock.unlock();
		ses->txState=TX_ACTIVE|TX_SYS|((mtxFlags&MTX_GLOB)!=0?TX_GSYS:0); mtxFlags|=MTX_STARTED;
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
		if (ses->getTxState()!=TX_NOTRAN && ses->tx.subTxID==subTxID) {
			assert(ses->list.isInList());
			if ((flags&MTX_OK)==0) ses->getStore()->txMgr->abort(ses); else ses->getStore()->txMgr->commit(ses);		// if failed ???
		}
		flags=0;
	}
}

RC TxSP::start()
{
	return start(TXI_DEFAULT,0);
}

RC TxSP::start(unsigned txl,unsigned txf)
{
	if ((flags&MTX_STARTED)==0) {
		StoreCtx *ctx=ses->getStore(); RC rc;
		if (ctx->theCB->state==SST_SHUTDOWN_IN_PROGRESS) return RC_SHUTDOWN;
		if (ses->getTxState()!=TX_NOTRAN) {if ((rc=ses->pushTx())!=RC_OK) return rc; ses->tx.subTxID=++ses->subTxCnt;}
		else if ((rc=ctx->txMgr->start(ses,txiFlags(txl,false)|txf))!=RC_OK) return rc;
		flags|=MTX_STARTED; subTxID=ses->tx.subTxID;
	}
	return RC_OK;
}

//----------------------------------------------------------------------------------------

ClassCreate *OnCommit::getClass()
{
	return NULL;
}

RC Session::pushTx()
{
	SubTx *st=new(this) SubTx(this); if (st==NULL) return RC_NOMEM;
	memcpy(st,&tx,sizeof(SubTx)); new(&tx) SubTx(this); 
	tx.next=st; tx.lastLSN=st->lastLSN; tx.subTxID=++subTxCnt;
	if (repl!=NULL) repl->mark(tx.rmark);
	return RC_OK;
}

RC Session::popTx(bool fCommit,bool fAll)
{
	for (SubTx *st=tx.next; st!=NULL; st=tx.next) {
		if (fCommit) {
			st->defHeap+=tx.defHeap; st->defClass+=tx.defClass; st->defFree+=tx.defFree; st->nInserted+=tx.nInserted;
			if (tx.txPurge!=0) {RC rc=st->txPurge.merge(tx.txPurge); if (rc!=RC_OK) return rc;}
			if (tx.onCommit.head!=NULL) {st->onCommit+=tx.onCommit; tx.onCommit.reset();}
			if (tx.txIndex!=NULL) {
				// txIndex!!! merge to st
				tx.txIndex=NULL;
			}
		} else if (fAll) st->nInserted=nTotalIns=0;
		else {
			ctx->lockMgr->releaseLocks(this,tx.subTxID,true); st->nInserted-=tx.nInserted; nTotalIns-=tx.nInserted;
			if (repl!=NULL) repl->truncate(TR_REL_ALL,&tx.rmark);
		}
		st->lastLSN=tx.lastLSN;	//???
		tx.next=NULL; tx.~SubTx(); memcpy(&tx,st,sizeof(SubTx)); free(st); if (!fAll) break;
	}
	return RC_OK;
}

SubTx::SubTx(Session *s) : next(NULL),ses(s),subTxID(0),lastLSN(0),txIndex(NULL),txPurge((MemAlloc*)s),defHeap(s),defClass(s),defFree(s),nInserted(0)
{
}

SubTx::~SubTx()
{
	for (unsigned i=0,j=(unsigned)txPurge; i<j; i++) if (txPurge[i].bmp!=NULL) ses->free(txPurge[i].bmp);
	for (OnCommit *oc=onCommit.head,*oc2; oc!=NULL; oc=oc2) {oc2=oc->next; oc->destroy(ses);}
	//delete txIndex;
}

void SubTx::cleanup()
{
	if (next!=NULL) {next->cleanup(); next->~SubTx(); ses->free(next); next=NULL;}
	for (OnCommit *oc=onCommit.head,*oc2; oc!=NULL; oc=oc2) {oc2=oc->next; oc->destroy(ses);}
	for (unsigned i=0,j=(unsigned)txPurge; i<j; i++) if (txPurge[i].bmp!=NULL) ses->free(txPurge[i].bmp);
	txPurge.clear(); onCommit.reset();
	if (txIndex!=NULL) {
		//...
	}
	if ((unsigned)defHeap!=0) {
		//???
		defHeap.cleanup();
	}
	if ((unsigned)defClass!=0) {
		//???
		defClass.cleanup();
	}
	if ((unsigned)defFree!=0) {
		if (ses->getStore()->fsMgr->freeTxPages(defFree)==RC_OK) ses->getStore()->fsMgr->txUnlock(); defFree.cleanup();
	}
	lastLSN=LSN(0);
}

RC SubTx::queueForPurge(const PageAddr& addr,PurgeType pt,const void *data)
{
	TxPurge prg={addr.pageID,0,NULL},*tp=NULL; RC rc=txPurge.add(prg,&tp); uint32_t flg=pt==TXP_SSV?0x80000000:0;
	if (rc==RC_FALSE) {
		if (data!=NULL || (tp->range&0x80000000)!=flg || tp->range==uint32_t(~0u)) return RC_CORRUPTED;	// report error
		ushort idx=addr.idx/(sizeof(uint32_t)*8),l=tp->range>>16&0x3fff; assert(tp->bmp!=NULL);
		if (idx<ushort(tp->range)) {
			ushort d=ushort(tp->range)-idx;
			if ((tp->bmp=(uint32_t*)ses->realloc(tp->bmp,(l+d)*sizeof(uint32_t)))==NULL) return RC_NOMEM;
			memmove(tp->bmp+d,tp->bmp,l*sizeof(uint32_t)); if (d==1) tp->bmp[0]=0; else memset(tp->bmp,0,d*sizeof(uint32_t));
			tp->range=(tp->range&0x80000000)+(uint32_t(l+d)<<16)+idx;
		} else if (idx>=ushort(tp->range)+l) {
			ushort d=idx+1-ushort(tp->range)-l;
			if ((tp->bmp=(uint32_t*)ses->realloc(tp->bmp,(l+d)*sizeof(uint32_t)))==NULL) return RC_NOMEM;
			if (d==1) tp->bmp[l]=0; else memset(tp->bmp+l,0,d*sizeof(uint32_t)); tp->range+=uint32_t(d)<<16;
		}
		tp->bmp[idx-ushort(tp->range)]|=1<<addr.idx%(sizeof(uint32_t)*8); rc=RC_OK;
	} else if (rc==RC_OK) {
		if (data!=NULL) {
			ushort l=HeapPageMgr::collDescrSize((HeapPageMgr::HeapExtCollection*)data);
			if ((tp->bmp=(uint32_t*)ses->malloc(l))==NULL) return RC_NOMEM;
			memcpy(tp->bmp,data,l); tp->range=uint32_t(~0u);
		} else {
			tp->range=(addr.idx/(sizeof(uint32_t)*8))|flg|0x00010000;
			if ((tp->bmp=new(ses) uint32_t)==NULL) return RC_NOMEM;
			tp->bmp[0]=1<<addr.idx%(sizeof(uint32_t)*8);
		}
	}
	return rc;
}

RC TxPurge::purge(Session *ses)
{
	return ses->getStore()->queryMgr->purge(pageID,range&0xFFFF,range>>16&0x3FFF,bmp,(range&0x80000000)!=0?TXP_SSV:TXP_PIN,ses);
}

RC TxPurge::Cmp::merge(TxPurge& dst,TxPurge& src,MemAlloc *ma)
{
	unsigned ss=src.range&0xFFFF,ds=dst.range&0xFFFF; assert(src.pageID==dst.pageID);
	if (ds==0xFFFF) {ma->free(src.bmp); src.bmp=NULL; return ss==0xFFFF?RC_OK:RC_CORRUPTED;}
	unsigned si=ss+(src.range>>16&0x3FFF),di=ds+(dst.range>>16&0x3FFF);
	if (ss<ds) {unsigned t=ss; ss=ds; ds=t; t=si; si=di; di=t; uint32_t *p=src.bmp; src.bmp=dst.bmp; dst.bmp=p;}
	if (di<si) {
		if ((dst.bmp=(uint32_t*)ma->realloc(dst.bmp,(si-ds)*sizeof(uint32_t)))==NULL) return RC_OK;
		if (ss<=di) memcpy(dst.bmp+di-ds,src.bmp+di-ss,(si-di)*sizeof(uint32_t));
		else {memset(dst.bmp+di-ds,0,(ss-di)*sizeof(uint32_t)); memcpy(dst.bmp+ss-ds,src.bmp,(si-ss)*sizeof(uint32_t));}
	}
	for (unsigned i=ss,end=min(si,di); i<end; i++) dst.bmp[i-ds]|=src.bmp[i-ss];
	ma->free(src.bmp); src.bmp=NULL; dst.range=(dst.range&0x80000000)|(max(di,si)-ds)<<16|ds; return RC_OK;
}

TxGuard::~TxGuard()
{
	assert(ses!=NULL);
	if (ses->getTxState()==TX_ABORTING) ses->ctx->txMgr->abortTx(ses,TXA_ALL);
}
