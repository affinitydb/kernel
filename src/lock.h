/**************************************************************************************

Copyright © 2004-2013 GoPivotal, Inc. All rights reserved.

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

/**
 * transactional locking structures
 */
#ifndef _LOCK_H_
#define _LOCK_H_

#include "pinex.h"
#include "affinity.h"
#include "timerq.h"

using namespace Afy;

namespace AfyKernel
{

#define	FREE_BLOCK_SIZE			0x1000				/**< block containing free LockHdr structures */
#define	MAX_FREE_BLOCKS			0x0100				/**< maximum number of blocks for LockHdr structures */
#define	VB_HASH_SIZE			0x0100				/**< transient versioning descriptor hash table size */

class TVers;

/**
 * LockHdr structure - transactional PIN lock descriptor, header of the list of indiviadual transaction locks
 */
#ifdef _WIN64
__declspec(align(16))
#endif
struct LockHdr
{
	TVers				*const tv;					/**< transient versioning info descriptor */
	DLList				grantedLocks;				/**< list of granted locks */
	SimpleSem			sem;						/**< wait queue semaphor */
	Session				*waiting;					/**< list of sessions waiting for this lock */
	SharedCounter		fixCount;					/**< number of sessions accessing this structure (for deallocation) */
	uint32_t			conflictMask;				/**< bitmap of conflicting lock requests */
	uint32_t			grantedMask;				/**< bitmap of granted lock requests */
	uint32_t			grantedCnts[LOCK_ALL];		/**< vector of counters of granted lock requests */
	LockHdr(TVers *t) : tv(t),waiting(NULL),fixCount(1),conflictMask(0),grantedMask(0) {memset(grantedCnts,0,sizeof(grantedCnts));}
	void				release(class LockMgr *mgr,SemData&);
#if defined(__x86_64__) || defined(__arm__)
}__attribute__((aligned(16)));
#else
};
#endif

/**
 * GrantedLock - granted lock request descriptor
 */
#ifdef _WIN64
__declspec(align(16))
#endif
struct GrantedLock : public DLList
{
	GrantedLock		*txNext;			/**< next lock in this transaction */
	GrantedLock		*other;				/**< next granted request for this resource */
	LockHdr			*header;			/**< header record for this resource */
	Session			*ses;				/**< session requested this lock */
	uint32_t		count;				/**< counter of lock requests of the same type for this transaction */
	uint32_t		lt		:6;			/**< lock type */
	uint32_t		fDel	:1;			/**< delete flag */
	uint32_t		subTxID	:25;		/**< sub-tx ID within this transaction (for partial rollbacks and commits) */
#if defined(__x86_64__) || defined(__arm__)
}__attribute__((aligned(16)));
#else
};
#endif

/**
 * Block of free lock structures descriptor
 */
#ifdef _WIN64
__declspec(align(16))
#endif
struct LockBlock
{
	DLList				list;
	unsigned				nBlocks;
	unsigned				nFree;
#if defined(__x86_64__) || defined(__arm__)
}__attribute__((aligned(16)));
#else
};
#endif

/**
 * deadlock detector - timer queue element
 */
struct DLD : public TimeRQ
{
	DLD(StoreCtx *ct) : TimeRQ(253,500000ULL,ct) {}
	void	processTimeRQ();
	void	destroyTimeRQ();
};

/**
 * transient versioning table operations
 */
enum TVOp
{
	TVO_READ, TVO_INS, TVO_UPD
};

/**
 * transient versioning resource states
 */
enum TVState
{
	TV_INS, TV_UPD, TV_DEL, TV_PRG, TV_UDL
};

/**
 * snapshot data element descriptor
 */
struct DataSS
{
	DataSS				*nextD;
	DataSS				*nextSS;
	TVers				*tv;
//	ValueV				data;
	uint32_t			stamp;
	uint16_t			dscr;
	bool				fDelta;
	bool				fNew;
};

/**
 * transient versioning descriptor
 */
class TVers
{
	const	PageIdx		idx;
	RWLock				lock;
	LockHdr	*volatile	hdr;
	DataSS	*volatile	stack;
	TVState	volatile	state;
	bool	volatile	fCommited;
public:
	TVers(PageIdx i,LockHdr *h,DataSS *st,TVState s=TV_UPD,bool fC=false) : idx(i),hdr(h),stack(st),state(s),fCommited(fC) {}
	~TVers();
	class TVersCmp {public: __forceinline static int cmp(const TVers *tv,PageIdx i) {return cmp3(tv->idx,i);}};
	friend	struct		LockHdr;
	friend	class		LockMgr;
	friend	class		QueryPrc;
};

/**
 * page descriptor for transient versioning
 * implements VBlock interface (see buffer.h)
 */
struct PageV : public VBlock {
	const PageID		pageID;
	LockMgr				&mgr;
	HChain<PageV>		list;
	RWLock				lock;
	SharedCounter		fixCnt;
	size_t				uncommittedSpace;
	TVers** volatile	vArray;
	unsigned volatile	nTV,xTV;
	PageV(PageID pid,LockMgr &mg) : pageID(pid),mgr(mg),list(this),uncommittedSpace(0),vArray(NULL),nTV(0),xTV(0) {}
	PageID				getKey() const {return pageID;}
	void				release();
};

typedef SyncHashTab<PageV,PageID,&PageV::list> PageVTab;

/**
 * transaction lock manager
 * controls transaction level locking for r/w transactions, snapshot creation and deallocation, snapshot access for r/o transaction
 * implements deadlock detection
 */
class LockMgr
{
	class	StoreCtx	*const ctx;

	Mutex				blockLock;
	SLIST_HEADER		freeHeaders;
	SLIST_HEADER		freeGranted;
	DLList				freeBlocks;
	unsigned			nFreeBlocks;
	PageVTab			pageVTab;
	
	HChain<Session>		waitQ;
	Mutex				waitQLock;
	Session* volatile	topmost;
	Session* volatile	oldSes;
	TIMESTAMP volatile	oldTimestamp;

	static	const unsigned	lockConflictMatrix[LOCK_ALL];
	template<class T> inline T* alloc(SLIST_HEADER&);
	void	checkDeadlock(Session *ses,SemData& sem);
	friend	struct	LockHdr;
	friend	struct	PageV;
public:
	LockMgr(class StoreCtx *ct);
	void	*operator new(size_t s,StoreCtx *ctx) {void *p=ctx->malloc(s); if (p==NULL) throw RC_NORESOURCES; return p;}
	RC		lock(LockType,PINx& pe,unsigned flags=0);
	RC		getTVers(PINx& pe,TVOp tvo=TVO_READ);
	VBlock	*getVBlock(PageID pid) {PageVTab::Find findVB(pageVTab,pid); PageV *pv=findVB.findLock(RW_S_LOCK); if (pv!=NULL) ++pv->fixCnt; findVB.unlock(); return pv;}

	void	releaseLocks(Session *ses,unsigned subTxID=0,bool fAbort=false);
	void	releaseSession(Session *ses);
	void	process();
};

};

#endif
