/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _LOCK_H_
#define _LOCK_H_

#include "pinex.h"
#include "affinity.h"

using namespace AfyDB;

class ILockNotification;

namespace AfyKernel
{

#define	FREE_BLOCK_SIZE			0x1000
#define	MAX_FREE_BLOCKS			0x0100
#define	VB_HASH_SIZE			0x0100

class TVers;

#ifdef _WIN64
__declspec(align(16))
#endif
struct LockHdr
{
	TVers				*const tv;
	DLList				grantedLocks;
	SimpleSem			sem;
	Session				*waiting;
	SharedCounter		fixCount;
	uint32_t			conflictMask;
	uint32_t			grantedMask;
	uint32_t			grantedCnts[LOCK_ALL];
	LockHdr(TVers *t) : tv(t),waiting(NULL),fixCount(1),conflictMask(0),grantedMask(0) {memset(grantedCnts,0,sizeof(grantedCnts));}
	void				release(class LockMgr *mgr,SemData&);
#if defined(__x86_64__) || defined(__arm__)
}__attribute__((aligned(16)));
#else
};
#endif


#ifdef _WIN64
__declspec(align(16))
#endif
struct GrantedLock : public DLList
{
	GrantedLock		*txNext;
	GrantedLock		*other;
	LockHdr			*header;
	Session			*ses;
	uint32_t		count;
	uint32_t		lt		:6;
	uint32_t		fDel	:1;
	uint32_t		subTxID	:25;
#if defined(__x86_64__) || defined(__arm__)
}__attribute__((aligned(16)));
#else
};
#endif

#ifdef _WIN64
__declspec(align(16))
#endif
struct LockBlock
{
	DLList				list;
	ulong				nBlocks;
	ulong				nFree;
#if defined(__x86_64__) || defined(__arm__)
}__attribute__((aligned(16)));
#else
};
#endif

struct LockStore : public SLIST_ENTRY
{
	class	LockMgr		*volatile mgr;
	LockStore(class LockMgr *mg) : mgr(mg) {}
};

struct LockStoreHdr
{
	SLIST_HEADER stores;
	FreeQ<>		freeLS;
	volatile	HTHREAD		lockDaemonThread;
	LockStoreHdr() : lockDaemonThread(0) {InitializeSListHead(&stores);}
	void		lockDaemon();
};

enum TVOp
{
	TVO_READ, TVO_INS, TVO_UPD
};

enum TVState
{
	TV_INS, TV_UPD, TV_DEL, TV_PRG, TV_UDL
};

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

class LockMgr : public Request
{
	class	StoreCtx	*const ctx;
	ILockNotification	*const lockNotification;

	Mutex				blockLock;
	SLIST_HEADER		freeHeaders;
	SLIST_HEADER		freeGranted;
	DLList				freeBlocks;
	ulong				nFreeBlocks;
	PageVTab			pageVTab;
	
	HChain<Session>		waitQ;
	Mutex				waitQLock;
	Session* volatile	topmost;
	Session* volatile	oldSes;
	TIMESTAMP volatile	oldTimestamp;
	LockStore			*lockStore;

	static	const ulong	lockConflictMatrix[LOCK_ALL];
	template<class T> inline T* alloc(SLIST_HEADER&);
	void	checkDeadlock(Session *ses,SemData& sem);
	static	LockStoreHdr lockStoreHdr;
	friend	struct	LockStoreHdr;
	friend	struct	LockHdr;
	friend	struct	PageV;
public:
	LockMgr(class StoreCtx *ct,ILockNotification *lno);
	~LockMgr();
	void	*operator new(size_t s,StoreCtx *ctx) {void *p=ctx->malloc(s); if (p==NULL) throw RC_NORESOURCES; return p;}
	RC		lock(LockType,PINEx& pe,ulong flags=0);
	RC		getTVers(class PINEx& pe,TVOp tvo=TVO_READ);
	VBlock	*getVBlock(PageID pid) {PageVTab::Find findVB(pageVTab,pid); PageV *pv=findVB.findLock(RW_S_LOCK); if (pv!=NULL) ++pv->fixCnt; findVB.unlock(); return pv;}

	void	releaseLocks(Session *ses,ulong subTxID=0,bool fAbort=false);
	void	releaseSession(Session *ses);
	void	process();
	void	destroy();
	static	void stopThreads();
};

};

#endif
