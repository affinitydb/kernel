/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "lock.h"
#include "txmgr.h"
#include "buffer.h"
#include "startup.h"
#include "queryop.h"
#include "mvstoreimpl.h"

using namespace MVStoreKernel;

const ulong LockMgr::lockConflictMatrix[LOCK_ALL] = {
	0<<LOCK_IS|0<<LOCK_IX|0<<LOCK_SHARED|1<<LOCK_SIX|1<<LOCK_UPDATE|1<<LOCK_EXCLUSIVE,	//	IS
	0<<LOCK_IS|0<<LOCK_IX|1<<LOCK_SHARED|1<<LOCK_SIX|1<<LOCK_UPDATE|1<<LOCK_EXCLUSIVE,	//	IX
	0<<LOCK_IS|1<<LOCK_IX|0<<LOCK_SHARED|1<<LOCK_SIX|1<<LOCK_UPDATE|1<<LOCK_EXCLUSIVE,	//	SHARED
	0<<LOCK_IS|1<<LOCK_IX|1<<LOCK_SHARED|1<<LOCK_SIX|1<<LOCK_UPDATE|1<<LOCK_EXCLUSIVE,	//	SIX
	1<<LOCK_IS|1<<LOCK_IX|0<<LOCK_SHARED|1<<LOCK_SIX|1<<LOCK_UPDATE|1<<LOCK_EXCLUSIVE,	//	UPDATE
	1<<LOCK_IS|1<<LOCK_IX|1<<LOCK_SHARED|1<<LOCK_SIX|1<<LOCK_UPDATE|1<<LOCK_EXCLUSIVE	//	EXCLUSIVE
};

LockStoreHdr LockMgr::lockStoreHdr;

#ifdef WIN32
static DWORD WINAPI _lockDaemon(void *param) {((LockStoreHdr*)param)->lockDaemon(); return 0;}
#else
static void *_lockDaemon(void *param) {((LockStoreHdr*)param)->lockDaemon(); return NULL;}
#endif

LockMgr::LockMgr(StoreCtx *ct,ILockNotification *lno)
: ctx(ct),lockNotification(lno),nFreeBlocks(0),pageVTab(VB_HASH_SIZE),topmost(NULL),oldSes(NULL),oldTimestamp(0)
{
	if ((lockStore=new(lockStoreHdr.freeLS.alloc(sizeof(LockStore))) LockStore(this))==NULL) throw RC_NORESOURCES;
	InterlockedPushEntrySList(&lockStoreHdr.stores,lockStore); InitializeSListHead(&freeHeaders); InitializeSListHead(&freeGranted);
}

LockMgr::~LockMgr()
{
	if (lockStore!=NULL) 
		for (LockMgr *mgr=lockStore->mgr; mgr!=NULL && 
			!casP((void* volatile *)&lockStore->mgr,(void*)this,(void*)0); mgr=lockStore->mgr) threadYield();
}

template<class T> inline T* LockMgr::alloc(SLIST_HEADER& sHdr)
{
	SLIST_ENTRY *se=InterlockedPopEntrySList(&sHdr); if (se!=NULL) return (T*)se;
	MutexP lck(&blockLock); LockBlock *lb;
	if ((se=InterlockedPopEntrySList(&sHdr))!=NULL) return (T*)se;
#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
	if ((lb=(LockBlock*)ctx->memalign(16,FREE_BLOCK_SIZE))==NULL) return NULL;
#else
	if ((lb=(LockBlock*)ctx->malloc(FREE_BLOCK_SIZE))==NULL) return NULL;
#endif
	freeBlocks.insertFirst(&lb->list); nFreeBlocks++; T *t0=(T*)(lb+1),*t=t0+1;
	lb->nFree=lb->nBlocks=(FREE_BLOCK_SIZE-sizeof(LockBlock))/sizeof(T);
	for (ulong i=1; i<lb->nBlocks; ++i,++t) InterlockedPushEntrySList(&sHdr,(SLIST_ENTRY*)t);
	return t0;
}

RC LockMgr::lock(LockType lt,PINEx& pe,ulong flags)
{
	RC rc=RC_OK; assert(lt<LOCK_ALL);
	Session *ses=pe.getSes(); if (ses==NULL) return RC_NOSESSION;
	if ((ses->getStore()->mode&STARTUP_SINGLE_SESSION)!=0) return RC_OK;
	if (!ses->inWriteTx()) {
		report(MSG_ERROR,"LockMgr: an attempt to lock a resource outside of a transaction\n");
		return RC_READTX;
	}
	if (pe.tv==NULL) {
		if (pe.pb.isNull()) {
			//???
			return RC_OK;
		} else if ((rc=getTVers(pe,lt==LOCK_SHARED?TVO_READ:TVO_UPD))!=RC_OK) return rc;
		assert(pe.tv!=NULL);
	}
	bool fLocked=false; GrantedLock *gl=NULL,*og=NULL; if (lt>=LOCK_UPDATE) ses->lockClass();
	LockHdr *lh=pe.tv->hdr;
	if (lh==NULL) {
		RWLockP tlck(&pe.tv->lock,RW_X_LOCK);
		if ((lh=pe.tv->hdr)!=NULL) ++lh->fixCount;
		else if ((pe.tv->hdr=lh=new(alloc<LockHdr>(freeHeaders)) LockHdr(pe.tv))==NULL) return RC_NORESOURCES;
	} else {
		++lh->fixCount; lh->sem.lock(ses->lockReq.sem);
		fLocked=true; ulong mask=lockConflictMatrix[lt];
		for (og=(GrantedLock*)lh->grantedLocks.next; ;og=(GrantedLock*)og->next)
			if (og==&lh->grantedLocks) {og=NULL; break;} else if (og->ses==ses) break;
		ulong grantedCnts[LOCK_ALL]; memset(grantedCnts,0,sizeof(grantedCnts));
		if ((gl=og)!=NULL) do {
			ulong ty=gl->lt;
			if (ty==(ulong)lt) {
				if (ses->tx.subTxID>gl->subTxID) {mask=0; break;}
				gl->count++; lh->grantedCnts[lt]++; --lh->fixCount;
				lh->sem.unlock(ses->lockReq.sem); return RC_OK;
			}
			if ((grantedCnts[ty]+=gl->count)==lh->grantedCnts[ty]) mask&=~(1<<ty);
		} while ((gl=gl->other)!=NULL);
		while ((lh->grantedMask&mask)!=0) {
			//if (lockNotification!=NULL && (rc=lockNotification->beforeWait(ses,pe.id,ILockNotification::LT_SHARED))!=RC_OK) ...
			bool fDL=ses->releaseLatches()!=RC_OK; if (!fDL && !pe.pb.isNull()) pe.pb.release();
			if (fDL || ses->nLatched>0) {lh->release(this,ses->lockReq.sem); return RC_DEADLOCK;}	//  rollback???
			lh->conflictMask|=lockConflictMatrix[lt]; ses->lockReq.lt=lt; ses->lockReq.rc=RC_REPEAT;
			ses->lockReq.next=lh->waiting; lh->waiting=ses;
			waitQLock.lock(); ses->lockReq.lh=lh; getTimestamp(ses->lockReq.stamp);
			if (waitQ.getFirst()==NULL) {oldSes=ses; oldTimestamp=ses->lockReq.stamp;}
			waitQ.insertFirst(&ses->lockReq.wait); waitQLock.unlock(); lh->sem.unlock(ses->lockReq.sem);
			if (lockStoreHdr.lockDaemonThread==(HTHREAD)0) {HTHREAD h; while (createThread(_lockDaemon,&lockStoreHdr,h)==RC_REPEAT);}
			do ses->lockReq.sem.wait(); while ((rc=ses->lockReq.rc)==RC_REPEAT);
			assert(!ses->lockReq.wait.isInList() && ses->lockReq.lh==NULL);
			//if (lockNotification!=NULL && (rc=lockNotification->afterWait(ses,pe.id,ILockNotification::LT_SHARED,rc))!=RC_OK) ...
			if (rc!=RC_OK) {--lh->fixCount; if (rc==RC_DEADLOCK) ses->abortTx(); return rc;}
			lh->sem.lock(ses->lockReq.sem);
			if ((lh->grantedMask&mask)!=0 && (gl=og)!=NULL) {
				memset(grantedCnts,0,sizeof(grantedCnts));
				do {ulong ty=gl->lt; if ((grantedCnts[ty]+=gl->count)==lh->grantedCnts[ty]) mask&=~(1<<ty);}
				while ((gl=gl->other)!=NULL);
			}
		}
	}
	if ((gl=alloc<GrantedLock>(freeGranted))==NULL) {rc=RC_NORESOURCES; --lh->fixCount;}
	else {
		gl->header=(LockHdr*)lh; gl->ses=ses; gl->other=og; gl->lt=lt; gl->count=1; // gl->fDel=???
		gl->txNext=ses->heldLocks; ses->heldLocks=gl; gl->subTxID=ses->tx.subTxID;
		lh->grantedCnts[lt]++; lh->grantedMask|=1<<lt; lh->grantedLocks.insertFirst(gl); 
	}
	if (fLocked) lh->sem.unlock(ses->lockReq.sem);
	return rc;
}

void LockMgr::releaseLocks(Session *ses,ulong subTxID,bool fAbort)
{
	for (GrantedLock *lock=ses->heldLocks; lock!=NULL && lock->subTxID>=subTxID; lock=ses->heldLocks) {
		LockHdr *lh=lock->header; ses->heldLocks=lock->txNext; assert(ses==lock->ses);
		lh->sem.lock(ses->lockReq.sem); ulong ty=lock->lt;
#ifdef _DEBUG
		for (int i=0; i<LOCK_ALL; i++) assert((lh->grantedCnts[i]==0)==((lh->grantedMask&1<<i)==0));
		assert((lh->grantedMask&1<<ty)!=0 && lock->count>0 && lh->grantedCnts[ty]>=lock->count);
#endif
		if ((lh->grantedCnts[ty]-=lock->count)==0) lh->grantedMask&=~(1<<ty);
		assert(lh->grantedMask!=0 || lh->grantedLocks.next==lock && lock->next==&lh->grantedLocks);
		if ((lh->conflictMask&1<<ty)!=0) {
			lh->conflictMask=0;
			for (Session **ps=&lh->waiting,*ws; (ws=*ps)!=NULL; ) {
				ulong mask=lockConflictMatrix[ws->lockReq.lt]; bool fConflict=true;
				if ((lh->grantedMask&mask)==0) fConflict=false;
				else if ((1<<ty&mask)!=0) for (GrantedLock *gl=(GrantedLock*)lh->grantedLocks.next; gl!=&lh->grantedLocks; gl=(GrantedLock*)gl->next)
					if (gl->ses==ws) {
						ulong grantedCnts[LOCK_ALL]; memset(grantedCnts,0,sizeof(grantedCnts));
						do {
							ulong ty=gl->lt;
							if ((grantedCnts[ty]+=gl->count)==lh->grantedCnts[ty]) mask&=~(1<<ty);
						} while ((gl=gl->other)!=NULL);
						if ((lh->grantedMask&mask)==0) fConflict=false;
						break;
					}
				if (fConflict) {lh->conflictMask|=mask; ps=&ws->lockReq.next;}
				else {
					ws->lockReq.rc=lock->fDel!=0&&!fAbort?RC_DELETED:RC_OK;
					*ps=ws->lockReq.next; waitQLock.lock(); ws->lockReq.lh=NULL; ws->lockReq.wait.remove();
					if (oldSes==ws) oldTimestamp=(oldSes=waitQ.getLast())!=NULL?oldSes->lockReq.stamp:0;
					for (Session *s=topmost; s!=NULL; s=s->lockReq.back) if (s==ws) {topmost=ws->lockReq.back; break;}
					waitQLock.unlock(); ws->lockReq.sem.wakeup();
				}
			}
		}
		lock->remove(); InterlockedPushEntrySList(&freeGranted,(SLIST_ENTRY*)lock); lh->release(this,ses->lockReq.sem);
	}
	ses->unlockClass();
}

void LockMgr::releaseSession(Session *ses)
{
	if (ses->heldLocks!=NULL) releaseLocks(ses,0,true);
	MutexP lck(&waitQLock); ses->lockReq.wait.remove(); LockHdr *lh; SemData sem;
	for (Session *s=topmost; s!=NULL; s=s->lockReq.back) if (s==ses) {topmost=ses->lockReq.back; break;}
	if ((lh=ses->lockReq.lh)!=NULL) {
		++lh->fixCount; ses->lockReq.lh=NULL; lck.set(NULL); lh->sem.lock(sem);
		for (Session **ps=&lh->waiting; *ps!=NULL; ps=&(*ps)->lockReq.next) if (*ps==ses) {*ps=ses->lockReq.next; break;}
		--lh->fixCount; lh->release(this,sem);
	}	
}

void LockHdr::release(LockMgr *mgr,SemData& sd)
{
	assert(fixCount>0);
	if (--fixCount>0) sem.unlock(sd);
	else {
		++fixCount; sem.unlock(sd); RWLockP lck(&tv->lock,RW_X_LOCK); sem.lock(sd);
		if (fixCount!=1) {--fixCount; sem.unlock(sd);}
		else {tv->hdr=NULL; InterlockedPushEntrySList(&mgr->freeHeaders,(SLIST_ENTRY*)this);}
	}
}

//-------------------------------------------------------------------------------------------------

void PageV::release()
{
	assert(fixCnt>0 && list.isInList() && list.getIndex()!=~0u);
	mgr.pageVTab.lock(this,RW_X_LOCK); assert(fixCnt>0);
	if (--fixCnt!=0 || vArray!=NULL) mgr.pageVTab.unlock(this);
	else {mgr.pageVTab.removeNoLock(this); mgr.ctx->free(this);}
}

RC LockMgr::getTVers(PINEx& pe,TVOp tvo)
{
	assert(!pe.pb.isNull() && (tvo==TVO_READ || pe.pb->isXLocked() || pe.pb->isULocked()));
	if (pe.tv==NULL) {
		Session *ses=pe.getSes(); if (ses==NULL) return RC_NOSESSION;
		PageV *pv=(PageV*)pe.pb->getVBlock();
		if (pv==NULL) {
			if (tvo==TVO_READ && !ses->inWriteTx()) return RC_OK;
			PageVTab::Find findPV(pageVTab,pe.pb->getPageID());
			if ((pv=findPV.findLock(RW_X_LOCK))!=NULL) ++pv->fixCnt;
			else if ((pv=new(ctx) PageV(pe.pb->getPageID(),*this))==NULL) return RC_NORESOURCES;
			else {++pv->fixCnt; pageVTab.insertNoLock(pv); pe.pb->setVBlock(pv);}
		}
		RWLockP lck(&pv->lock,RW_S_LOCK);
		pe.tv=(TVers*)BIN<TVers,PageIdx,TVers::TVersCmp>::find(pe.getAddr().idx,(const TVers**)pv->vArray,pv->nTV);
		if (pe.tv==NULL && (tvo!=TVO_READ || ses->inWriteTx())) {
			lck.set(NULL); lck.set(&pv->lock,RW_X_LOCK); const TVers **ins=NULL;
			if ((pe.tv=(TVers*)BIN<TVers,PageIdx,TVers::TVersCmp>::find(pe.getAddr().idx,(const TVers**)pv->vArray,pv->nTV,&ins))==NULL) {
				LockHdr *lh=tvo!=TVO_INS?new(alloc<LockHdr>(freeHeaders)) LockHdr(pe.tv):(LockHdr*)0;
				if ((pe.tv=new(ctx) TVers(pe.getAddr().idx,lh,NULL))==NULL) return RC_NORESOURCES;
				if (pv->vArray==NULL || pv->nTV>=pv->xTV) {
					ptrdiff_t sht=ins-(const TVers**)pv->vArray;
					if ((pv->vArray=(TVers**)ctx->realloc(pv->vArray,(pv->xTV+=(pv->xTV==0?10:pv->xTV/2))*sizeof(TVers*)))==NULL) 
						{pv->nTV=pv->xTV=0; return RC_NORESOURCES;}
					ins=(const TVers**)pv->vArray+sht;
				}
				if (ins<&pv->vArray[pv->nTV]) memmove(ins+1,ins,(byte*)&pv->vArray[pv->nTV]-(byte*)ins); *ins=pe.tv; pv->nTV++;
			}
		}
	}
	return RC_OK;
}

//-------------------------------------------------------------------------------------------------

void LockStoreHdr::lockDaemon()
{
	if (!casP((void *volatile*)&lockDaemonThread,(void*)0,(void*)getThread())) return;

	for (;;) {
		TIMESTAMP ts1,ts2; getTimestamp(ts1);
#ifdef _M_X64
		for (SLIST_ENTRY **pse=NULL,*se=(SLIST_ENTRY*)(stores.HeaderX64.NextEntry<<4); se!=NULL; se=*pse) {
#elif defined(_M_IA64)
		for (SLIST_ENTRY **pse=NULL,*se=(SLIST_ENTRY*)(stores.Header16.NextEntry<<4); se!=NULL; se=*pse) {
#else
		for (SLIST_ENTRY **pse=NULL,*se=stores.Next.Next; se!=NULL; se=*pse) {
#endif
			LockMgr *mgr=((LockStore*)se)->mgr;
			if (mgr!=NULL && casP((void *volatile*)&((LockStore*)se)->mgr,(void*)mgr,(void*)(~0ULL))) {
				if (mgr->oldTimestamp!=0) {getTimestamp(ts2); if ((ts2-mgr->oldTimestamp)/500000>0) RequestQueue::postRequest(mgr,mgr->ctx);}
				((LockStore*)se)->mgr=mgr;
			} else if (pse!=NULL) {*pse=se->Next; freeLS.dealloc(se); continue;}
			pse=&se->Next;
		}
		getTimestamp(ts2); if ((ts2-ts1)/1000000==0) threadSleep(1000-long((ts2-ts1)/1000));
	}
}

void LockMgr::process()
{
	MutexP lck(&waitQLock); Session *ses=waitQ.getLast(); if (ses==NULL || ses->lockReq.lh==NULL) return;
	if (ses!=oldSes || ses->lockReq.stamp!=oldTimestamp) {oldSes=ses; oldTimestamp=ses->lockReq.stamp; return;}
	SemData sem; ses->lockReq.back=NULL;
	for (LockHdr *lh=NULL;;) {
		LockHdr *lh2=ses->lockReq.lh; assert(lh2!=NULL); topmost=ses; ++lh2->fixCount; lck.set(NULL);
		if (lh!=NULL) lh->release(this,sem); (lh=lh2)->sem.lock(sem); ulong mask=lockConflictMatrix[ses->lockReq.lt];
		for (GrantedLock *gl=(GrantedLock*)lh->grantedLocks.next; ;gl=(GrantedLock*)gl->next)
			if (gl==&lh->grantedLocks || ses->lockReq.lh!=lh) {												// ses ???
				lh->release(this,sem); lh=NULL; lck.set(&waitQLock); ses=topmost==ses?topmost=ses->lockReq.back:topmost;
			pop:
				if (ses!=NULL) {lh2=ses->lockReq.lh; ++lh2->fixCount;}
				lck.set(NULL); if (lh!=NULL) lh->release(this,sem); if (ses!=NULL) (lh=lh2)->sem.lock(sem); else return;
				for (gl=(GrantedLock*)lh->grantedLocks.next; gl!=&lh->grantedLocks && gl!=ses->lockReq.gl; gl=(GrantedLock*)gl->next);
			} else if (gl->ses!=ses && gl->ses->lockReq.lh!=NULL && (1<<gl->lt&mask)!=0) {
				lck.set(&waitQLock); if (topmost!=ses) {ses=topmost; goto pop;}
				for (Session *s=ses->lockReq.back; s!=NULL; s=s->lockReq.back) if (s==gl->ses) {
#ifdef _DEBUG_DEADLOCK_DETECTION
					fprintf(stderr,"\nCycle found:\n");
					for (s=topmost; s!=NULL; s=s->lockReq.back) {
						fprintf(stderr,"\t\t%X(%s) <- %X: ",(ulong)*(uint64_t*)s->lockReq.lh->keybuf&0xFFFF,s->lockReq.lt==LOCK_SHARED?"r":"w",(ulong)s->getTXID());
						for (GrantedLock *g=s->heldLocks; g!=NULL; g=g->txNext) fprintf(stderr,"%s%X(%s)",g==s->heldLocks?"":",",(ulong)*(uint64_t*)g->header->keybuf&0xFFFF,g->lt==LOCK_SHARED?"r":"w");
						fprintf(stderr,"\n");
					}
					fprintf(stderr,"\n");
#endif
					Session *victim=ses;
					for (s=ses->lockReq.back; s!=NULL; s=s->lockReq.back) {
						if (s->nLogRecs<victim->nLogRecs && victim->lockReq.lh!=NULL) victim=s;
						if (s==gl->ses) break;
					}
					if (lh!=victim->lockReq.lh) {
						LockHdr *vlh=victim->lockReq.lh; ++vlh->fixCount; lck.set(NULL);
						lh->release(this,sem); (lh=vlh)->sem.lock(sem); 
						lck.set(&waitQLock); if (topmost!=ses) {ses=topmost; goto pop;}
					}
					for (Session **ps=&lh->waiting; *ps!=NULL; ps=&(*ps)->lockReq.next)
						if (*ps==victim) {*ps=victim->lockReq.next; break;}
					victim->lockReq.lh=NULL; victim->lockReq.wait.remove();
					victim->lockReq.rc=RC_DEADLOCK; victim->lockReq.sem.wakeup();
					lck.set(NULL); lh->release(this,sem); return;
				}
				gl->ses->lockReq.back=ses; ses->lockReq.gl=gl; ses=gl->ses; break;
			}
	}
}

void LockMgr::destroy()
{
}

void LockMgr::stopThreads()
{
#ifdef WIN32
	TerminateThread(lockStoreHdr.lockDaemonThread,0);
#else
	if (lockStoreHdr.lockDaemonThread!=0) pthread_cancel(lockStoreHdr.lockDaemonThread);
#endif
}
