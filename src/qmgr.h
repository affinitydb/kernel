/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _QMGR_H_
#define _QMGR_H_

#include "utils.h"

namespace MVStoreKernel 
{
#define	QMGR_NOLOAD		0x80000000ul
#define QMGR_UFORCE		0x40000000ul
#define	QMGR_TRY		0x20000000ul
#define	QMGR_INMEM		0x10000000ul
#define	QMGR_NEW		0x08000000ul
#define	QMGR_SCAN		0x04000000ul

#ifndef QE_ALLOC_BLOCK_SIZE
#define QE_ALLOC_BLOCK_SIZE	0x200
#endif

enum QID {_NONE,_T1,_T2,_B1,_B2};

template<class T,typename Key,typename KeyArg> struct QElt : public DLList {
	void			*mgr;
	Key				key;
	RWLock			lock;
	T				*rsrc;
	HChain<QElt>	hash;
	SharedCounter	fixCount;
	QID				qid;
	RC				rc;
	bool			fDiscard;
	QElt(void *mg,KeyArg k,QID qi,T *r=NULL) : mgr(mg),key(k),rsrc(r),hash(this),fixCount(1),qid(qi),rc(RC_OK),fDiscard(false) {}
	KeyArg			getKey() const {return key;}
	bool			isFixed() const {return fixCount>0;}
	bool			isLocked() const {return lock.isLocked();}
	bool			isULocked() const {return lock.isULocked();}
	bool			isXLocked() const {return lock.isXLocked();}
	void			upgradeLock(RW_LockType lt) {lock.upgradelock(lt);}
	void			downgradeLock(RW_LockType lt) {lock.downgradelock(lt);}
	bool			tryupgrade() {return lock.tryupgrade();}
};

template<class T,typename Key,typename KeyArg,typename Info,HEAP_TYPE allc> class QMgr
{
	typedef QElt<T,Key,KeyArg> QE;
	typedef SyncHashTab<QE,KeyArg,&QE::hash> QEHash;
	struct Queue : public DLList {
		long l; 
		const QID type;
		Queue(QID ty) : l(0),type(ty) {}
		QE* removeLast() {if (prev==this) return NULL; QE *dt=(QE*)prev; dt->remove(); return dt;}
	};
public:
	template<HEAP_TYPE alc> struct QueueCtrl {
		long						nElts;
		long						T1TargetL;
		Queue						T1;
		Queue						T2;
		Queue						B1;
		Queue						B2;
		Mutex						lock;
		FreeQ<QE_ALLOC_BLOCK_SIZE,Std_Alloc<alc> >	freeQE;
		QueueCtrl(int nb) : nElts(nb),T1TargetL(0),T1(_T1),T2(_T2),B1(_B1),B2(_B2) {}
	};
protected:
	QueueCtrl<allc>			&ctrl;
	RW_LockType				saveLock;
	QEHash					hashTable;
public:
	QMgr(QueueCtrl<allc>& ct,int lH) : ctrl(ct),saveLock(RW_U_LOCK),hashTable(nextP2(lH)) {}
protected:
	RC get(T* &rsrc,KeyArg key,Info info,ulong flags=RW_S_LOCK,T *old=NULL) {
		typename QEHash::Find findQE(hashTable,key); rsrc=NULL; Queue *pA=NULL;
		if (old!=NULL) old->getQE()->lock.unlock((flags&QMGR_UFORCE)!=0);
		QE *qe; QEHash *ht; RW_LockType lt=(RW_LockType)(flags&RW_MASK); bool fT1=false;
		for (;;) {
			if ((qe=findQE.findLock(RW_S_LOCK))==NULL) {findQE.unlock(); qe=findQE.findLock(RW_X_LOCK);}
			if (qe==NULL) {
				if ((flags&QMGR_INMEM)!=0) return RC_NOTFOUND;
				if ((qe=new(ctrl.freeQE.alloc(sizeof(QE))) QE(this,key,_T1))==NULL) return RC_NORESOURCES;
				qe->lock.lock(RW_X_LOCK);
				hashTable.insertNoLock(qe,findQE.getIdx()); findQE.unlock();
				ctrl.lock.lock(); ctrl.T1.l++; if (old!=NULL) releaseNoLock(old);
				if (ctrl.T1.l+ctrl.T2.l+ctrl.B1.l+ctrl.B2.l>=ctrl.nElts) {
					if (ctrl.T1.l+ctrl.B1.l!=ctrl.nElts) {
						assert(ctrl.T1.l+ctrl.T2.l+ctrl.B1.l+ctrl.B2.l>=ctrl.nElts);
						if (ctrl.T1.l+ctrl.T2.l+ctrl.B1.l+ctrl.B2.l==ctrl.nElts*2) {
							bool fB1=true; QE *qe2=NULL;
							for (; (qe2=ctrl.B2.removeLast())!=NULL; ht->unlock(qe2)) {
								assert(qe2->qid==_B2 && qe2->rsrc==NULL); ht=&((QMgr*)qe2->mgr)->hashTable; ht->lock(qe2,RW_X_LOCK);
								if (qe2->fixCount==0) {ht->removeNoLock(qe2); ctrl.B2.l--; ctrl.freeQE.dealloc(qe2); fB1=false; break;}
							}
							if (fB1) for (; (qe2=ctrl.B1.removeLast())!=NULL; ht->unlock(qe2)) {
								assert(qe2->qid==_B1 && qe2->rsrc==NULL); ht=&((QMgr*)qe2->mgr)->hashTable; ht->lock(qe2,RW_X_LOCK);
								if (qe2->fixCount==0) {ht->removeNoLock(qe2); ctrl.B1.l--; ctrl.freeQE.dealloc(qe2); break;}
							}
						}
					} else if (ctrl.T1.l<ctrl.nElts) {
						QE *qe2=ctrl.B1.removeLast(); assert(qe2!=NULL && qe2->qid==_B1 && qe2->rsrc==NULL);
						ctrl.B1.l--; ((QMgr*)qe2->mgr)->hashTable.remove(qe2); assert(qe2->fixCount==0); ctrl.freeQE.dealloc(qe2);
					} else 
						fT1=true;
				}
			} else {
				bool fLoad=qe->rsrc==NULL; //assert(!fLoad || qe->fDiscard || qe->fixCount==0 || qe->fixCount==1 && qe->lock.isXLocked());
				if ((flags&QMGR_NEW)!=0 && !fLoad && !qe->fDiscard) return RC_ALREADYEXISTS;
				if (fLoad && (flags&QMGR_INMEM)!=0) return RC_NOTFOUND;
				++qe->fixCount;
				if ((flags&QMGR_TRY)!=0 && (!fLoad||qe->fixCount>1)) {
					RC rc=qe->rc;
					if (rc!=RC_OK) {assert(qe->fixCount>0); --qe->fixCount;}
					else if (qe->lock.trylock(lt)) {rc=qe->rc; rsrc=qe->rsrc; assert(rsrc!=NULL);}
					else {assert(qe->fixCount>0); --qe->fixCount; rc=RC_NOACCESS;}
					findQE.unlock(); if (old!=NULL) release(old,old->getQE());
					if (rc==RC_OK && qe->fDiscard) {rsrc=NULL; continue;}
					return rc;
				}
				if (fLoad) if (qe->fixCount==1) qe->lock.lock(RW_X_LOCK); else fLoad=false;
				findQE.unlock(); if (!fLoad) {qe->lock.lock(qe->rsrc!=NULL?qe->rsrc->lockType(lt):lt); assert(qe->rsrc!=NULL);}
				if (qe->fDiscard) continue;
				if (qe->rsrc!=NULL) {
					if (old!=NULL) release(old,old->getQE());
					if (qe->rc==RC_OK) rsrc=qe->rsrc; 
					return qe->rc;
				}
				ctrl.lock.lock(); assert(qe->lock.isXLocked());
				if (qe->isInList()) {
					if (qe->qid==_B1) {ctrl.T1TargetL=min(ctrl.T1TargetL+max(ctrl.B2.l/ctrl.B1.l,1l),ctrl.nElts); --ctrl.B1.l;}
					else {assert(qe->qid==_B2); ctrl.T1TargetL=max(ctrl.T1TargetL-max(ctrl.B1.l/ctrl.B2.l,1l),0l); --ctrl.B2.l;}
					qe->remove();
				}
				qe->qid=_T2; ctrl.T2.l++; if (old!=NULL) releaseNoLock(old);
			}
			break;
		}
		bool fNew=true; assert(qe->rsrc==NULL);
		if (fT1 || (rsrc=T::createNew(key,this))==NULL) for (;;fT1=false) {
			pA = fT1 || ctrl.T1.l>=max(ctrl.T1TargetL,1l) ? &ctrl.T1 : &ctrl.T2;
			if (pA->prev==pA) {
				pA=pA==&ctrl.T1?&ctrl.T2:&ctrl.T1; 
				if (pA->prev==pA) {
					ctrl.lock.unlock(); T::waitResource(this); ctrl.lock.lock();
					if ((rsrc=T::createNew(key,this))==NULL) continue; else break;
				}
			}
			QE *stolen=pA->removeLast(); assert(stolen->rsrc!=NULL); ht=&((QMgr*)stolen->mgr)->hashTable;
			ht->lock(stolen,RW_X_LOCK); if (stolen->fixCount!=0) {ht->unlock(stolen); continue;}
			if (!stolen->rsrc->isDirty()) {
				pA->l--; rsrc=stolen->rsrc; stolen->rsrc=NULL; 
				if (fT1) {ht->removeNoLock(stolen); ctrl.freeQE.dealloc(stolen);}
				else {pA=pA==&ctrl.T1?&ctrl.B1:&ctrl.B2; stolen->qid=pA->type; pA->insertFirst(stolen); pA->l++; ht->unlock(stolen);}
				fNew=false; break;
			}
			stolen->lock.lock(saveLock); ++stolen->fixCount; ht->unlock(stolen); ctrl.lock.unlock(); 
			bool fSaved=stolen->rsrc->save();
			if (fSaved) ctrl.lock.lock();
			else {
				stolen->lock.unlock(); ctrl.lock.lock(); ht->lock(stolen,RW_X_LOCK);
				bool fDiscard=--stolen->fixCount==0&&stolen->fDiscard; ht->unlock(stolen);
				if (fDiscard) {stolen->rsrc->destroy(); ctrl.freeQE.dealloc(stolen);}
			}
		}
		qe->rsrc=rsrc; rsrc->setQE(qe); ctrl.lock.unlock(); assert(!qe->isInList());
		if (fNew) rsrc->initNew(); else rsrc->setKey(key,this);
		if ((flags&(QMGR_NOLOAD|QMGR_NEW))==0 && (qe->rc=rsrc->load(info,flags))!=RC_OK) {
			if (drop(rsrc)) rsrc->destroy(); rsrc=NULL;
		} else {
			qe->rc=RC_OK; if (lt!=RW_X_LOCK) qe->lock.downgradelock(lt);
		}
		return qe->rc;
	}
	void relock(T *t,RW_LockType lt) {
		QE *qe=t->getQE(); assert(qe!=NULL); qe->lock.unlock(); qe->lock.lock(lt);
	}
	void setNElts(long nE) {if (nE>ctrl.nElts) ctrl.nElts=nE;}
	void setLockType(RW_LockType lt) {saveLock=lt;}
	void cleanup() {
		ctrl.lock.lock(); QE *qe,*qe2;
		for (qe=(QE*)ctrl.B1.next; qe!=(QE*)&ctrl.B1; qe=qe2) {
			qe2=(QE*)qe->next;
			if (qe->mgr==this) {assert(ctrl.B1.l>0); qe->remove(); ctrl.B1.l--; hashTable.remove(qe); ctrl.freeQE.dealloc(qe);}
		}
		for (qe=(QE*)ctrl.B2.next; qe!=(QE*)&ctrl.B2; qe=qe2) {
			qe2=(QE*)qe->next;
			if (qe->mgr==this) {assert(ctrl.B2.l>0); qe->remove(); ctrl.B2.l--; hashTable.remove(qe); ctrl.freeQE.dealloc(qe);}
		}
		ctrl.lock.unlock();
	}
public:
#ifdef _DEBUG
	void reportHighwatermark(const char *s) {
		report(MSG_DEBUG, "%s highwatermark: %d\n",s,hashTable.getHighwatermark());
	}
#endif
	bool exists(KeyArg key) {
		typename QEHash::Find findQE(hashTable,key); 
		QE *qe=findQE.findLock(RW_S_LOCK); if (qe==NULL) return false;
		bool f=qe->rsrc!=NULL&&!qe->fDiscard; findQE.unlock(); return f;
	}
	void release(T *t,bool fUF=false) {QE *qe=t->getQE(); assert(qe!=NULL); qe->lock.unlock(fUF); release(t,qe);}
	bool drop(T* t,bool fUF=false,bool fFixed=true) {
		QE *qe=t->getQE(); assert(qe!=NULL&&qe->mgr==this); bool fDel;
		if (fFixed) qe->lock.unlock(fUF);
		if (qe->fDiscard) fDel=(fFixed?--qe->fixCount:(long)qe->fixCount)==0;
		else {
			ctrl.lock.lock(); qe->remove();
			switch (qe->qid) {
			default: assert(0);
			case _T1: --ctrl.T1.l; break;
			case _T2: --ctrl.T2.l; break;
			}
			assert(qe->hash.isInList() && qe->hash.getIndex()!=~0u);
			hashTable.lock(qe,RW_X_LOCK); assert(!fFixed||qe->fixCount>0);
			fDel=(fFixed?--qe->fixCount:(long)qe->fixCount)==0; 
			qe->fDiscard=true; hashTable.removeNoLock(qe); ctrl.lock.unlock(); 
		}
		if (fDel) ctrl.freeQE.dealloc(qe); return fDel;
	}
	void drop(KeyArg key) {
		typename QEHash::Find findQE(hashTable,key);
		MutexP lck(&ctrl.lock); QE *qe=findQE.findLock(RW_X_LOCK);
		if (qe!=NULL) {
			qe->remove();
			switch (qe->qid) {
			default: assert(0);
			case _T1: --ctrl.T1.l; break;
			case _T2: --ctrl.T2.l; break;
			case _B1: --ctrl.B1.l; break;
			case _B2: --ctrl.B2.l; break;
			}
			bool fDel=qe->fixCount==0; qe->fDiscard=true; findQE.remove(qe); lck.set(NULL);
			if (fDel) {if (qe->rsrc!=NULL) qe->rsrc->destroy(); ctrl.freeQE.dealloc(qe);}
		}
	}
	bool prepareForSave(T *t) {
		QE *qe=t->getQE(); assert(qe!=NULL && (qe->lock.isULocked()||qe->lock.isXLocked()));
		if (saveLock==RW_X_LOCK && !qe->lock.isXLocked()) qe->lock.upgradelock(RW_X_LOCK);
		return !qe->fDiscard;
	}
	T* lockForSave(KeyArg key,bool fTry=false) {
		typename QEHash::Find findQE(hashTable,key);
		T *t=NULL; QE *qe=findQE.findLock(RW_X_LOCK);
		if (qe!=NULL && (t=qe->rsrc)!=NULL) {
			++qe->fixCount; findQE.unlock();
			if (!fTry) qe->lock.lock(saveLock);
			else if (!qe->lock.trylock(saveLock)) {
				hashTable.lock(qe,RW_X_LOCK); assert(qe->fixCount>0);
				--qe->fixCount; hashTable.unlock(qe); t=NULL;
			}
		}
		return t;
	}
	T* trylock(T *t,RW_LockType lt) {
		QE *qe=t->getQE(); assert(qe!=NULL && !qe->fDiscard && qe->mgr==this);
		hashTable.lock(qe,RW_X_LOCK); ++qe->fixCount;
		if (!qe->lock.trylock(lt)) {--qe->fixCount; t=NULL;}
		hashTable.unlock(qe); return t;
	}
	void endLoad(T *t) {
		QE *qe=t->getQE(); assert(qe!=NULL && !qe->fDiscard && qe->mgr==this);
		qe->lock.unlock(); ctrl.lock.lock(); hashTable.lock(qe,RW_X_LOCK); assert(qe->fixCount>0);
		if (--qe->fixCount==0) {qe->remove(); (qe->qid==_T1?&ctrl.T1:&ctrl.T2)->insertFirst(qe); T::signal(this);}
		hashTable.unlock(qe); ctrl.lock.unlock();
	}
	void endSave(T *t) {
		QE *qe=t->getQE(); assert(qe!=NULL && qe->mgr==this);
		qe->lock.unlock(saveLock==RW_U_LOCK&&qe->lock.isULocked());
		if (qe->fDiscard) {
			assert(qe->fixCount>0 && !qe->isInList() && !qe->hash.isInList() && qe->hash.getIndex()==~0u);
			if (--qe->fixCount==0) {t->destroy(); ctrl.freeQE.dealloc(qe);}
		} else {
			ctrl.lock.lock(); hashTable.lock(qe,RW_X_LOCK); assert(qe->fixCount>0);
			if (--qe->fixCount==0) {qe->remove(); (qe->qid==_T1?&ctrl.T1:&ctrl.T2)->insertFirst(qe); T::signal(this);}
			hashTable.unlock(qe); ctrl.lock.unlock();
		}
	}
private:
	void release(T *t,QE *qe) {
		bool fDel=false;
		if (qe->fDiscard) {
			assert(qe->fixCount>0 && !qe->isInList() && !qe->hash.isInList() && qe->hash.getIndex()==~0u);
			fDel=--qe->fixCount==0;
		} else {
			assert(qe->rsrc==t && qe->fixCount>0 && qe->hash.isInList() && qe->hash.getIndex()!=~0u);
			hashTable.lock(qe,RW_X_LOCK); assert(qe->fixCount>0);
			if (--qe->fixCount!=0) hashTable.unlock(qe);
			else {
				++qe->fixCount; hashTable.unlock(qe);
				ctrl.lock.lock(); qe->remove(); hashTable.lock(qe,RW_X_LOCK);
				fDel=qe->fDiscard; assert(qe->fixCount>0);
				if (--qe->fixCount!=0) fDel=false;
				else if (!fDel) {(qe->qid==_T1?&ctrl.T1:&ctrl.T2)->insertFirst(qe); T::signal(this);}
				hashTable.unlock(qe); ctrl.lock.unlock();
			}
		}
		if (fDel) {t->destroy(); ctrl.freeQE.dealloc(qe);}
	}
	void releaseNoLock(T *t) {
		QE *qe=t->getQE(); assert(qe!=NULL); bool fDel;
		if (qe->fDiscard) {
			assert(qe->fixCount>0 && !qe->isInList() && !qe->hash.isInList() && qe->hash.getIndex()==~0u);
			fDel=--qe->fixCount==0;
		} else {
			assert(qe->hash.isInList() && qe->hash.getIndex()!=~0u);
			qe->remove(); hashTable.lock(qe,RW_X_LOCK);
			fDel=qe->fDiscard; assert(qe->fixCount>0);
			if (--qe->fixCount!=0) fDel=false;
			else if (!fDel) {(qe->qid==_T1?&ctrl.T1:&ctrl.T2)->insertFirst(qe); T::signal(this);}
			hashTable.unlock(qe); 
		}
		if (fDel) {t->destroy(); ctrl.freeQE.dealloc(qe);}
	}
};

};

#endif
