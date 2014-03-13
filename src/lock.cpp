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

#include "lock.h"
#include "txmgr.h"
#include "buffer.h"
#include "startup.h"
#include "queryop.h"
#include "pin.h"

using namespace AfyKernel;

const unsigned LockMgr::lockConflictMatrix[LOCK_ALL] = {
	0<<LOCK_IS|0<<LOCK_IX|0<<LOCK_SHARED|1<<LOCK_SIX|1<<LOCK_UPDATE|1<<LOCK_EXCLUSIVE,	//	IS
	0<<LOCK_IS|0<<LOCK_IX|1<<LOCK_SHARED|1<<LOCK_SIX|1<<LOCK_UPDATE|1<<LOCK_EXCLUSIVE,	//	IX
	0<<LOCK_IS|1<<LOCK_IX|0<<LOCK_SHARED|1<<LOCK_SIX|1<<LOCK_UPDATE|1<<LOCK_EXCLUSIVE,	//	SHARED
	0<<LOCK_IS|1<<LOCK_IX|1<<LOCK_SHARED|1<<LOCK_SIX|1<<LOCK_UPDATE|1<<LOCK_EXCLUSIVE,	//	SIX
	1<<LOCK_IS|1<<LOCK_IX|0<<LOCK_SHARED|1<<LOCK_SIX|1<<LOCK_UPDATE|1<<LOCK_EXCLUSIVE,	//	UPDATE
	1<<LOCK_IS|1<<LOCK_IX|1<<LOCK_SHARED|1<<LOCK_SIX|1<<LOCK_UPDATE|1<<LOCK_EXCLUSIVE	//	EXCLUSIVE
};

LockMgr::LockMgr(StoreCtx *ct) : ctx(ct),nFreeBlocks(0),pageVTab(VB_HASH_SIZE,(MemAlloc*)ct),topmost(NULL),oldSes(NULL),oldTimestamp(0)
{
	InitializeSListHead(&freeHeaders); InitializeSListHead(&freeGranted);
	if ((ct->mode&STARTUP_RT)==0) {RC rc=ct->tqMgr->add(new(ct) DLD(ct)); if (rc!=RC_OK) throw rc;}
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
	for (unsigned i=1; i<lb->nBlocks; ++i,++t) InterlockedPushEntrySList(&sHdr,(SLIST_ENTRY*)t);
	return t0;
}

RC LockMgr::lock(LockType lt,PINx& pe,unsigned flags)
{
	RC rc=RC_OK; assert(lt<LOCK_ALL);
	Session *ses=pe.getSes(); if (ses==NULL) return RC_NOSESSION;
	if (!ses->inWriteTx() || (ses->getStore()->mode&STARTUP_RT)!=0) return RC_OK;
	if (pe.tv==NULL && (rc=getTVers(pe,lt==LOCK_SHARED?TVO_READ:TVO_UPD))!=RC_OK) return rc==RC_NOTFOUND?RC_OK:rc;
	bool fLocked=false; GrantedLock *gl=NULL,*og=NULL; if (lt>=LOCK_UPDATE) ses->lockClass(); assert(pe.tv!=NULL);
	LockHdr *lh=pe.tv->hdr;
	if (lh==NULL) {
		RWLockP tlck(&pe.tv->lock,RW_X_LOCK);
		if ((lh=pe.tv->hdr)!=NULL) ++lh->fixCount;
		else if ((pe.tv->hdr=lh=new(alloc<LockHdr>(freeHeaders)) LockHdr(pe.tv))==NULL) return RC_NORESOURCES;
	} else {
		++lh->fixCount; lh->sem.lock(ses->lockReq.sem);
		fLocked=true; unsigned mask=lockConflictMatrix[lt];
		for (og=(GrantedLock*)lh->grantedLocks.next; ;og=(GrantedLock*)og->next)
			if (og==&lh->grantedLocks) {og=NULL; break;} else if (og->ses==ses) break;
		unsigned grantedCnts[LOCK_ALL]; memset(grantedCnts,0,sizeof(grantedCnts));
		if ((gl=og)!=NULL) do {
			unsigned ty=gl->lt;
			if (ty==(unsigned)lt) {
				if (ses->tx.subTxID>gl->subTxID) {mask=0; break;}
				gl->count++; lh->grantedCnts[lt]++; --lh->fixCount;
				lh->sem.unlock(ses->lockReq.sem); return RC_OK;
			}
			if ((grantedCnts[ty]+=gl->count)==lh->grantedCnts[ty]) mask&=~(1<<ty);
		} while ((gl=gl->other)!=NULL);
		while ((lh->grantedMask&mask)!=0) {
			bool fDL=ses->releaseAllLatches()!=RC_OK; if (!fDL && !pe.pb.isNull()) pe.pb.release(ses);
			if (fDL || ses->nLatched>0) {lh->release(this,ses->lockReq.sem); return RC_DEADLOCK;}	//  rollback???
			lh->conflictMask|=lockConflictMatrix[lt]; ses->lockReq.lt=lt; ses->lockReq.rc=RC_REPEAT;
			ses->lockReq.next=lh->waiting; lh->waiting=ses;
			waitQLock.lock(); ses->lockReq.lh=lh; getTimestamp(ses->lockReq.stamp);
			if (waitQ.getFirst()==NULL) {oldSes=ses; oldTimestamp=ses->lockReq.stamp;}
			waitQ.insertFirst(&ses->lockReq.wait); waitQLock.unlock(); lh->sem.unlock(ses->lockReq.sem);
			do ses->lockReq.sem.wait(); while ((rc=ses->lockReq.rc)==RC_REPEAT);
			assert(!ses->lockReq.wait.isInList() && ses->lockReq.lh==NULL);
			if (rc!=RC_OK) {--lh->fixCount; if (rc==RC_DEADLOCK) ses->abortTx(); return rc;}
			lh->sem.lock(ses->lockReq.sem);
			if ((lh->grantedMask&mask)!=0 && (gl=og)!=NULL) {
				memset(grantedCnts,0,sizeof(grantedCnts));
				do {unsigned ty=gl->lt; if ((grantedCnts[ty]+=gl->count)==lh->grantedCnts[ty]) mask&=~(1<<ty);}
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

void LockMgr::releaseLocks(Session *ses,unsigned subTxID,bool fAbort)
{
	for (GrantedLock *lock=ses->heldLocks; lock!=NULL && lock->subTxID>=subTxID; lock=ses->heldLocks) {
		LockHdr *lh=lock->header; ses->heldLocks=lock->txNext; assert(ses==lock->ses);
		lh->sem.lock(ses->lockReq.sem); unsigned ty=lock->lt;
#ifdef _DEBUG
		for (int i=0; i<LOCK_ALL; i++) assert((lh->grantedCnts[i]==0)==((lh->grantedMask&1<<i)==0));
		assert((lh->grantedMask&1<<ty)!=0 && lock->count>0 && lh->grantedCnts[ty]>=lock->count);
#endif
		if ((lh->grantedCnts[ty]-=lock->count)==0) lh->grantedMask&=~(1<<ty);
		assert(lh->grantedMask!=0 || lh->grantedLocks.next==lock && lock->next==&lh->grantedLocks);
		if ((lh->conflictMask&1<<ty)!=0) {
			lh->conflictMask=0;
			for (Session **ps=&lh->waiting,*ws; (ws=*ps)!=NULL; ) {
				unsigned mask=lockConflictMatrix[ws->lockReq.lt]; bool fConflict=true;
				if ((lh->grantedMask&mask)==0) fConflict=false;
				else if ((1<<ty&mask)!=0) for (GrantedLock *gl=(GrantedLock*)lh->grantedLocks.next; gl!=&lh->grantedLocks; gl=(GrantedLock*)gl->next)
					if (gl->ses==ws) {
						unsigned grantedCnts[LOCK_ALL]; memset(grantedCnts,0,sizeof(grantedCnts));
						do {
							unsigned ty=gl->lt;
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

RC LockMgr::getTVers(PINx& pe,TVOp tvo)
{
	assert(tvo==TVO_READ || pe.pb.isNull() || pe.pb->isXLocked() || pe.pb->isULocked());
	if (pe.tv==NULL && (ctx->mode&STARTUP_RT)==0) {
		Session *ses=pe.getSes(); if (ses==NULL) return RC_NOSESSION;
		PageV *pv=NULL; PageID pageID=INVALID_PAGEID;
		if (!pe.pb.isNull()) {pageID=pe.pb->getPageID(); pv=(PageV*)pe.pb->getVBlock();}
		else {
			PageAddr ad;
			if ((pe.epr.flags&PINEX_ADDRSET)!=0) ad=pe.getAddr();
			else {
				PID id; RC rc=pe.getID(id); if (rc!=RC_OK) return rc;
				if (!ad.convert(id.pid)) return RC_NOTFOUND;
			}
			pv=(PageV*)getVBlock(pageID=ad.pageID);
		}
		if (pv==NULL) {
			if (tvo==TVO_READ && !ses->inWriteTx()) return RC_OK;
			PageVTab::Find findPV(pageVTab,pageID);
			if ((pv=findPV.findLock(RW_X_LOCK))!=NULL) ++pv->fixCnt;
			else if ((pv=new(ctx) PageV(pageID,*this))==NULL) return RC_NORESOURCES;
			else {++pv->fixCnt; pageVTab.insertNoLock(pv); if (!pe.pb.isNull()) pe.pb->setVBlock(pv);}
		}
		RWLockP lck(&pv->lock,RW_S_LOCK);
		pe.tv=(TVers*)BIN<TVers,PageIdx,TVers::TVersCmp>::find(pe.getAddr().idx,(const TVers**)pv->vArray,pv->nTV);
		if (pe.tv==NULL && (tvo!=TVO_READ || ses->inWriteTx())) {
			lck.set(NULL); lck.set(&pv->lock,RW_X_LOCK); const TVers **ins=NULL;
			if ((pe.tv=(TVers*)BIN<TVers,PageIdx,TVers::TVersCmp>::find(pe.getAddr().idx,(const TVers**)pv->vArray,pv->nTV,&ins))==NULL) {
				LockHdr *lh=tvo!=TVO_INS?new(alloc<LockHdr>(freeHeaders)) LockHdr(pe.tv):(LockHdr*)0;
				if ((pe.tv=new(ctx) TVers(pe.getAddr().idx,lh,NULL,tvo==TVO_INS?TV_INS:TV_UPD,tvo!=TVO_INS))==NULL) return RC_NORESOURCES;
				if (pv->vArray==NULL || pv->nTV>=pv->xTV) {
					ptrdiff_t sht=ins-(const TVers**)pv->vArray;
					if ((pv->vArray=(TVers**)ctx->realloc(pv->vArray,(pv->xTV+=(pv->xTV==0?10:pv->xTV/2))*sizeof(TVers*)))==NULL) 
						{pv->nTV=pv->xTV=0; return RC_NORESOURCES;}
					ins=(const TVers**)pv->vArray+sht;
				}
				if (ins<(const TVers**)&pv->vArray[pv->nTV]) memmove(ins+1,ins,(byte*)&pv->vArray[pv->nTV]-(byte*)ins); *ins=pe.tv; pv->nTV++;
			}
		}
	}
	return RC_OK;
}

//-------------------------------------------------------------------------------------------------

void DLD::processTimeRQ()
{
	ctx->lockMgr->process();
}

void DLD::destroyTimeRQ()
{
	ctx->free(this);
}

void LockMgr::process()
{
	MutexP lck(&waitQLock); Session *ses=waitQ.getLast(); if (ses==NULL || ses->lockReq.lh==NULL) return;
	if (ses!=oldSes || ses->lockReq.stamp!=oldTimestamp) {oldSes=ses; oldTimestamp=ses->lockReq.stamp; return;}
	SemData sem; ses->lockReq.back=NULL;
	for (LockHdr *lh=NULL;;) {
		LockHdr *lh2=ses->lockReq.lh; assert(lh2!=NULL); topmost=ses; ++lh2->fixCount; lck.set(NULL);
		if (lh!=NULL) lh->release(this,sem); (lh=lh2)->sem.lock(sem); unsigned mask=lockConflictMatrix[ses->lockReq.lt];
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
						fprintf(stderr,"\t\t%X(%s) <- %X: ",(unsigned)*(uint64_t*)s->lockReq.lh->keybuf&0xFFFF,s->lockReq.lt==LOCK_SHARED?"r":"w",(unsigned)s->getTXID());
						for (GrantedLock *g=s->heldLocks; g!=NULL; g=g->txNext) fprintf(stderr,"%s%X(%s)",g==s->heldLocks?"":",",(unsigned)*(uint64_t*)g->header->keybuf&0xFFFF,g->lt==LOCK_SHARED?"r":"w");
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
