/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov, Max Windish, Michael Andronov 2004 - 2010

**************************************************************************************/

#ifndef _SYNC_H_
#define _SYNC_H_

#include "types.h"

#ifdef __arm__
extern "C" {
bool __sync_bool_compare_and_swap_8(volatile void* destination, long long unsigned comperand, long long unsigned exchange);
}
#endif

namespace MVStoreKernel
{

#ifdef WIN32
	#define THREAD_SIGNATURE DWORD WINAPI
	#define	WAIT_SPIN_COUNT	18
	#define	SPIN_COUNT		1000
#else
	#define THREAD_SIGNATURE void *
	#define	WAIT_SPIN_COUNT	18
	#define	SPIN_COUNT		1000
#endif
inline void threadsWaitFor(int pNumThreads, HTHREAD * pThreads)
{
	#ifdef WIN32
		::WaitForMultipleObjects(pNumThreads, pThreads, true, INFINITE);
	#else
		void * lRet;
		for (int i = 0; i < pNumThreads; i++)
			::pthread_join(pThreads[i], &lRet);
	#endif
}

#ifdef WIN32
template<typename T> __forceinline bool cas(volatile T *ptr,T oldV,T newV) {return InterlockedCompareExchange(ptr,newV,oldV)==oldV;}
template<typename T> __forceinline bool casP(T *volatile *ptr,T *oldV,T *newV) {return InterlockedCompareExchangePointer(ptr,newV,oldV)==oldV;}
template<typename T> __forceinline T casV(volatile T *ptr,T oldV,T newV) {return InterlockedCompareExchange(ptr,newV,oldV);}
#elif !defined(__arm__)
#define	cas(a,b,c)									__sync_bool_compare_and_swap(a,b,c)
#define	casP(a,b,c)									__sync_bool_compare_and_swap(a,b,c)
#define	casV(a,b,c)									__sync_val_compare_and_swap(a,b,c)
#define	InterlockedIncrement(a)						__sync_add_and_fetch(a,1)
#define	InterlockedDecrement(a)						__sync_sub_and_fetch(a,1)
struct SLIST_ENTRY
{
	SLIST_ENTRY		*Next;
};
union SLIST_HEADER {
#ifdef __x86_64__
	volatile __uint128_t dword;
    struct {
        SLIST_ENTRY	Next;
		volatile uint64_t cnt;
    };
#else
	volatile uint64_t dword;
    struct {
        SLIST_ENTRY	Next;
		volatile uint32_t cnt;
    };
#endif
};

inline void InitializeSListHead(SLIST_HEADER *head)
{
	head->dword=0;
}
inline void InterlockedPushEntrySList(SLIST_HEADER *pHeader,SLIST_ENTRY *entry)
{
	SLIST_HEADER nHdr,oHdr; nHdr.Next.Next=entry;
	do {oHdr.dword=pHeader->dword; nHdr.cnt=oHdr.cnt+1; entry->Next=oHdr.Next.Next;}
	while (!__sync_bool_compare_and_swap(&pHeader->dword,oHdr.dword,nHdr.dword));
}
inline SLIST_ENTRY *InterlockedPopEntrySList(SLIST_HEADER *pHeader)
{
	SLIST_HEADER nHdr,oHdr; SLIST_ENTRY *pe;
	do {
		oHdr.dword=pHeader->dword; if ((pe=oHdr.Next.Next)==NULL) return NULL;
		nHdr.cnt=oHdr.cnt+1; nHdr.Next.Next=pe->Next;
	} while (!__sync_bool_compare_and_swap(&pHeader->dword,oHdr.dword,nHdr.dword));
	return pe;
}
#else		//__arm__
#define	cas(a,b,c)									__sync_bool_compare_and_swap(a,b,c)
#define	casP(a,b,c)									__sync_bool_compare_and_swap(a,b,c)
#define	casV(a,b,c)									__sync_val_compare_and_swap(a,b,c)
#define	InterlockedIncrement(a)						__sync_add_and_fetch(a,1)
#define	InterlockedDecrement(a)						__sync_sub_and_fetch(a,1)

struct SLIST_ENTRY
{
	SLIST_ENTRY		*Next;
};
union SLIST_HEADER {
	volatile uint64_t dword;
    struct {
        SLIST_ENTRY	Next;
		volatile uint32_t cnt;
    };
}__attribute__((aligned(16)));

inline void InitializeSListHead(SLIST_HEADER *head)
{
	head->dword=0;
}
inline void InterlockedPushEntrySList(SLIST_HEADER *pHeader,SLIST_ENTRY *entry)
{
	SLIST_HEADER nHdr,oHdr; nHdr.Next.Next=entry;
	do {oHdr.dword=pHeader->dword; nHdr.cnt=oHdr.cnt+1; entry->Next=oHdr.Next.Next;}
	while (!__sync_bool_compare_and_swap(&pHeader->dword,oHdr.dword,nHdr.dword));
}
inline SLIST_ENTRY *InterlockedPopEntrySList(SLIST_HEADER *pHeader)
{
	SLIST_HEADER nHdr,oHdr; SLIST_ENTRY *pe;
	do {
		oHdr.dword=pHeader->dword; if ((pe=oHdr.Next.Next)==NULL) return NULL;
		nHdr.cnt=oHdr.cnt+1; nHdr.Next.Next=pe->Next;
	} while (!__sync_bool_compare_and_swap(&pHeader->dword,oHdr.dword,nHdr.dword));
	return pe;
}
#endif

inline void threadYield()
{
	#ifdef WIN32
		::SwitchToThread();
	#elif defined(Darwin)
		::sched_yield();
	#else
		::pthread_yield();
	#endif
}
inline void threadSleep(long pMilli)
{
	#ifdef WIN32
		::Sleep(pMilli);
	#else
		struct timespec tm={pMilli/1000,(pMilli%1000)*1000000},rtm={0,0};
		while (::nanosleep(&tm, &rtm)<0 && errno==EINTR) tm=rtm;
	#endif
}

struct SpinC {
	const long	spinCount;
	SpinC() : spinCount(getNProcessors()>1?SPIN_COUNT:0) {}
	static SpinC SC;
};

struct SemData {
	SemData		*next;
#ifdef WIN32
	HTHREAD		thread;
	SemData() :	thread(0) {}
	~SemData()	{detach();}
	void		detach() {if (thread!=0) {CloseHandle(thread); thread=0;}}
	void		wait() {if (thread==0) {HANDLE hProc=GetCurrentProcess(); DuplicateHandle(hProc,GetCurrentThread(),hProc,&thread,THREAD_SUSPEND_RESUME,FALSE,0);} SuspendThread(thread);}
	void		wakeup() {for (long spinCount=SpinC::SC.spinCount; thread==(HTHREAD)0||ResumeThread(thread)!=1;) if (--spinCount<0) threadYield();}
#elif defined(Darwin)
	//OS X does not support un-named pthread semaphores. The named phtread
	//semaphores are implemented on base of MACH semaphores... 
	semaphore_t machsem;
	SemData() {kern_return_t kr;
			  kr = semaphore_create(mach_task_self(), &machsem,SYNC_POLICY_FIFO, 0);
			  if( kr != KERN_SUCCESS) mach_error( "semaphore create: ",kr);
    }
	~SemData()	{semaphore_destroy(mach_task_self(), machsem);}
	void		detach() {}
	void		wait() {semaphore_wait(machsem);}
	void		wakeup() {semaphore_signal(machsem);}
#else
	sem_t		sem;
	SemData()	{sem_init(&sem,0,0);}
	~SemData()	{sem_destroy(&sem);}
	void		detach() {}
	void		wait() {while (sem_wait(&sem)<0 && errno==EINTR);}
	void		wakeup() {while (sem_post(&sem)<0 && errno==EINTR);}
#endif
};

class SimpleSem
{
	SemData* volatile	chain;
public:
	SimpleSem() : chain(NULL) {}
	~SimpleSem();
	void		lock(SemData&);
	bool		trylock(SemData&);
	void		unlock(SemData&);
};

class SimpleSemNC
{
	SemData* volatile	chain;
public:
	SimpleSemNC() : chain(NULL) {}
	~SimpleSemNC();
	void		lock(SemData&);
	bool		trylock(SemData&);
	bool		waitlock(SemData&,class RWLock *);
	void		unlock(SemData&);
};

class Semaphore
{
	SemData*	volatile	chain;
	SemData*	volatile	owner;
	long		volatile	recCnt;
public:
	Semaphore() : chain(NULL),owner(NULL),recCnt(0) {}
	~Semaphore();
	void		lock(SemData&);
	bool		trylock(SemData&);
	void		unlock(SemData&,bool fWakeupAll=false);
};

class Mutex
{
#ifdef WIN32
	CRITICAL_SECTION	CS;
public:
	Mutex() {InitializeCriticalSection(&CS);}				//Spin??
	Mutex(bool) {InitializeCriticalSection(&CS);}			//Spin??
	~Mutex() {DeleteCriticalSection(&CS);}	
	void lock() {EnterCriticalSection(&CS);}
	bool trylock() {return TryEnterCriticalSection(&CS)!=0;}
	void unlock() {LeaveCriticalSection(&CS);}
#elif defined(POSIX)
	pthread_mutex_t				mutex;
	static volatile long		fInit;
	static pthread_mutexattr_t	mutexAttrs;
public:
	Mutex();
	~Mutex() {pthread_mutex_destroy(&mutex);}
	void lock() {pthread_mutex_lock(&mutex);}
	bool trylock() {return pthread_mutex_trylock(&mutex)==0;}
	void unlock() {pthread_mutex_unlock(&mutex);}
	operator pthread_mutex_t*() const {return const_cast<pthread_mutex_t*>(&mutex);}
#endif
};

class MutexP
{
	Mutex	*lock;
public:
	MutexP(Mutex *m=NULL) : lock(m) {if (m!=NULL) m->lock();}
	~MutexP() {if (lock!=NULL) lock->unlock();}
	void set(Mutex *m) {if (lock!=NULL) lock->unlock(); if ((lock=m)!=NULL) m->lock();}
};

class SLock
{
	volatile	long	count;
#ifdef _DEBUG
	THREADID			threadID;
#endif
public:
	SLock() : count(0) {}
	void lock() {
		for (long spinCount=SpinC::SC.spinCount; !cas(&count,0L,1L); ) if (--spinCount<0) threadYield();
#ifdef _DEBUG
		threadID=getThreadId();
#endif
	}
	bool trylock() {return cas(&count,0L,1L);}
	void unlock() {assert(count!=0); count=0;}
	bool isLocked() const {return count!=0;}
};

class Event
{
#ifdef WIN32
	HANDLE			event;
public:
	Event() {event=CreateEvent(NULL,TRUE,FALSE,NULL);}
	~Event() {if (event!=NULL) CloseHandle(event);}
	void wait(Mutex& lock,ulong timeout) {lock.unlock(); /* ??? */ WaitForSingleObject(event,timeout==0?INFINITE:timeout); lock.lock();}
	void signal() {PulseEvent(event);}
	void signalAll() {SetEvent(event);}
	void reset() {ResetEvent(event);}
	operator HANDLE() const {return event;}
#elif defined(POSIX)
	pthread_cond_t	cond;
public:
	Event() {pthread_cond_init(&cond,NULL);}
	~Event() {pthread_cond_destroy(&cond);}
	void wait(Mutex& lock,ulong timeout) {
		if (timeout==0) pthread_cond_wait(&cond,lock);
		else {
			struct timespec ts; 
#ifdef Darwin
			struct timeval tv; gettimeofday(&tv, NULL);
			ts.tv_sec=tv.tv_sec; ts.tv_nsec=tv.tv_usec*1000;
			if (timeout%1000!=0) {
				ts.tv_nsec+=timeout%1000*1000000;
				ts.tv_sec+=ts.tv_nsec/1000000000;
				ts.tv_nsec%=1000000000;
			}
#else
			clock_gettime(CLOCK_REALTIME,&ts);
			if (timeout%1000!=0) {
				ts.tv_nsec+=timeout%1000*1000000;
				ts.tv_sec+=ts.tv_nsec/1000000000;
				ts.tv_nsec%=1000000000;
			}
#endif
			ts.tv_sec+=timeout/1000;
			pthread_cond_timedwait(&cond,lock,&ts);
		}
	}
	void signal() {pthread_cond_signal(&cond);}
	void signalAll() {pthread_cond_broadcast(&cond);}
	void reset() const {}
#endif
};

enum RW_LockType {RW_NO_LOCK, RW_S_LOCK, RW_X_LOCK, RW_U_LOCK};
#define	RW_MASK	0x0F

#define	RW_WAIT					if (--spinCount<0) if (--waitSpinCount>=0) threadYield();\
		else if (ptrdiff_t(q)!=ptrdiff_t(~0ULL)) {wait(q); q=(SemData*)~0ULL;}\
		else for (q=queue; ;q=queue)\
			if (ptrdiff_t(q)==ptrdiff_t(~0ULL)) {threadYield(); break;}\
			else if (casP((void* volatile *)&queue,(void*)q,(void*)~0ULL)) break;

#define	RW_RESTORE				if (ptrdiff_t(q)!=ptrdiff_t(~0ULL)) {queue=q; q=(SemData*)~0ULL;}

#define	RW_LOCK(cnd,mod)		{for (long c=count; ;c=count)\
		if (cnd) {RW_WAIT} else if (cas(&count,c,mod)) break;}\
		RW_RESTORE

#define RW_TRYLOCK(cnd,mod)		for (long c=count; ;c=count)\
		if (cnd) return false; else if (cas(&count,c,mod)) break;

class RWLock
{
	enum {
		RU_BIT	= 0x10000000,	// requested update lock
		U_BIT	= 0x20000000,	// confirmed update lock
		RX_BIT	= 0x40000000,	// requested exclusive lock
		X_BIT	= 0x80000000	// confirmed exclusive lock
	};

	volatile	long	count;
	SemData* volatile	queue;
	void				wait(SemData*);
public:
#ifdef _DEBUG
	THREADID			threadID;
#endif
	RWLock() : count(0),queue(NULL) {}
	void lock(RW_LockType lock) {
		// For U/X locks, first set the RU_BIT/RX_BIT (don't wait for open lock),
		// to reserve the lock asap, then wait.
		// Note: Let X compete with X by setting RX_BIT even when X_BIT is already set.
		long spinCount=SpinC::SC.spinCount,waitSpinCount=WAIT_SPIN_COUNT; SemData *volatile q=(SemData*)~0ULL;
		switch (lock) {
		case RW_NO_LOCK: break;
		case RW_S_LOCK: RW_LOCK((c&(RX_BIT|X_BIT))!=0,c+1); break;
		case RW_U_LOCK:
			RW_LOCK((c&RU_BIT)!=0,c|RU_BIT);
			spinCount=SpinC::SC.spinCount; waitSpinCount=WAIT_SPIN_COUNT;
			RW_LOCK((c&(U_BIT|RX_BIT|X_BIT))!=0,((c|U_BIT)&~RU_BIT)+1);
#ifdef _DEBUG
			threadID=getThreadId();
#endif
			break;
		case RW_X_LOCK:
			RW_LOCK((c&RX_BIT)!=0,c|RX_BIT);
			spinCount=SpinC::SC.spinCount; waitSpinCount=WAIT_SPIN_COUNT;
			RW_LOCK(c!=RX_BIT&&c!=(RU_BIT|RX_BIT),((c|X_BIT)&~RX_BIT)+1);
#ifdef _DEBUG
			threadID=getThreadId();
#endif
			break;
		}
	}
	bool trylock(RW_LockType lock) {
		switch (lock) {
		case RW_NO_LOCK: break;
		case RW_S_LOCK: RW_TRYLOCK((c&(RX_BIT|X_BIT))!=0,c+1); break;
		case RW_U_LOCK:
			RW_TRYLOCK((c&(RU_BIT|U_BIT|RX_BIT|X_BIT))!=0,(c|U_BIT)+1);
#ifdef _DEBUG
			threadID=getThreadId();
#endif
			break;
		case RW_X_LOCK:
			RW_TRYLOCK(c!=0,long(X_BIT|1));
#ifdef _DEBUG
			threadID=getThreadId();
#endif
			break;
		}
		return true;
	}
	bool upgradelock(RW_LockType lock) {
		bool unset=false; SemData *volatile q=(SemData*)~0ULL;
		long mask=0,spinCount=SpinC::SC.spinCount,waitSpinCount=WAIT_SPIN_COUNT;
		switch (lock) {
		case RW_NO_LOCK: break;
		case RW_S_LOCK: assert(false); return false;
		case RW_U_LOCK: assert(false); return false;
		case RW_X_LOCK:
			assert((count&U_BIT)!=0);
			for (long c=count; ;c=count)
				if ((c&(RX_BIT|X_BIT))!=0 || (unset=cas(&count,c,c|RX_BIT))) break; else {RW_WAIT}
			mask=unset?(U_BIT|RX_BIT):U_BIT; spinCount=SpinC::SC.spinCount; waitSpinCount=WAIT_SPIN_COUNT;
			RW_LOCK((c&~(RU_BIT|U_BIT|RX_BIT|X_BIT))!=1,(c&~mask)|X_BIT);
#ifdef _DEBUG
			threadID=getThreadId();
#endif
			break;
		}
		return true;
	}
	bool tryupgrade() {
		assert((count&U_BIT)!=0); RW_TRYLOCK(c!=(U_BIT|1),long(X_BIT|1)); return true;
	}
	bool downgradelock(RW_LockType lock) {
		SemData *volatile q;
		switch (lock) {
		case RW_NO_LOCK: if (count!=0) unlock(); break;
		case RW_S_LOCK:
		case RW_U_LOCK:
			for (long c=count; ;c=count) {
				assert((c&~(RU_BIT|U_BIT|RX_BIT|X_BIT))!=0&&(c&(lock==RW_S_LOCK?U_BIT|X_BIT:X_BIT))!=0);
				for (q=queue; ptrdiff_t(q)!=ptrdiff_t(~0ULL) && !casP((void* volatile *)&queue,(void*)q,(void*)~0ULL); q=queue);
				if (ptrdiff_t(q)==ptrdiff_t(~0ULL)) {threadYield(); continue;}
				if (cas(&count,c,lock==RW_S_LOCK?c&~(X_BIT|U_BIT):c&~X_BIT|U_BIT)) {
#ifdef _DEBUG
					if (lock==RW_S_LOCK) cas(&threadID,getThreadId(),0UL);
#endif
					if (ptrdiff_t(q)!=ptrdiff_t(~0ULL)) {
						assert(ptrdiff_t(queue)==ptrdiff_t(~0ULL)); queue=NULL;
						for (SemData *sd=q,*sd2; sd!=NULL; sd=sd2) {sd2=sd->next; sd->wakeup();}
					}
					break;
				}
				if (ptrdiff_t(q)!=ptrdiff_t(~0ULL)) {assert(ptrdiff_t(queue)==ptrdiff_t(~0ULL)); queue=q;}
			}
			break;
		case RW_X_LOCK: assert(false); return false;
		}
		return true;
	}
	void unlock(bool fForce=false) {
		assert(!fForce || (count&(U_BIT|X_BIT))!=0);
		for (long c=count; ;c=count) {
			SemData *volatile q=(SemData*)~0ULL;
			bool const fReset=fForce||(c&(RU_BIT|U_BIT|RX_BIT|X_BIT))!=0&&(c&~(RU_BIT|U_BIT|RX_BIT|X_BIT))==1;
			if (fReset||(c&~(RU_BIT|X_BIT))==(RX_BIT|U_BIT|2)) {
				for (q=queue; ptrdiff_t(q)!=ptrdiff_t(~0ULL) && !casP((void* volatile *)&queue,(void*)q,(void*)~0ULL); q=queue);
				if (ptrdiff_t(q)==ptrdiff_t(~0ULL)) {threadYield(); continue;}
			}
			if (cas(&count,c,(fReset?c&~(U_BIT|X_BIT):c)-1)) {
				if (ptrdiff_t(q)!=ptrdiff_t(~0ULL)) {
					assert(ptrdiff_t(queue)==ptrdiff_t(~0ULL)); queue=NULL;
					for (SemData *sd=q,*sd2; sd!=NULL; sd=sd2) {sd2=sd->next; sd->wakeup();}
				}
				break;
			}
			if (ptrdiff_t(q)!=ptrdiff_t(~0ULL)) {assert(ptrdiff_t(queue)==ptrdiff_t(~0ULL)); queue=q;}
		}
	}
	bool	isXLocked() const {return (count&X_BIT)!=0;}
	bool	isULocked() const {return (count&U_BIT)!=0;}
	bool	isLocked() const {return count!=0;}
};

#undef	RW_WAIT
#undef	RW_RESTORE
#define RW_WAIT	if (--spinCount<0) threadYield();
#define	RW_RESTORE

class RWSpin
{
	enum {
		RU_BIT	= 0x10000000,	// requested update lock
		U_BIT	= 0x20000000,	// confirmed update lock
		RX_BIT	= 0x40000000,	// requested exclusive lock
		X_BIT	= 0x80000000	// confirmed exclusive lock
	};

	volatile	long	count;
#ifdef _DEBUG
	THREADID			threadID;
#endif
public:
	RWSpin() : count(0) {}
	void lock(RW_LockType lock) {
		// For U/X locks, first set the RU_BIT/RX_BIT (don't wait for open lock),
		// to reserve the lock asap, then wait.
		// Note: Let X compete with X by setting RX_BIT even when X_BIT is already set.
		assert((count&(X_BIT|U_BIT))!=(X_BIT|U_BIT));
		long spinCount=SpinC::SC.spinCount;
		switch (lock) {
		case RW_NO_LOCK: break;
		case RW_S_LOCK: RW_LOCK((c&(RX_BIT|X_BIT))!=0,c+1); break;
		case RW_U_LOCK:
			RW_LOCK((c&RU_BIT)!=0,c|RU_BIT);
			spinCount=SpinC::SC.spinCount;
			RW_LOCK((c&(U_BIT|RX_BIT|X_BIT))!=0,(c&~RU_BIT|U_BIT)+1);
#ifdef _DEBUG
			threadID=getThreadId();
#endif
			break;
		case RW_X_LOCK:
			RW_LOCK((c&RX_BIT)!=0,c|RX_BIT);
			spinCount=SpinC::SC.spinCount;
			RW_LOCK(c!=RX_BIT&&c!=(RU_BIT|RX_BIT),(c&~RX_BIT|X_BIT)+1);
#ifdef _DEBUG
			threadID=getThreadId();
#endif
			break;
		}
		assert((count&(X_BIT|U_BIT))!=(X_BIT|U_BIT));
	}
	bool trylock(RW_LockType lock) {
		assert((count&(X_BIT|U_BIT))!=(X_BIT|U_BIT));
		switch (lock) {
		case RW_NO_LOCK: break;
		case RW_S_LOCK: RW_TRYLOCK((c&(RX_BIT|X_BIT))!=0,c+1); break;
		case RW_U_LOCK:
			RW_TRYLOCK((c&(RU_BIT|U_BIT|RX_BIT|X_BIT))!=0,(c|U_BIT)+1);
#ifdef _DEBUG
			threadID=getThreadId();
#endif
			break;
		case RW_X_LOCK:
			RW_TRYLOCK(c!=0,long(X_BIT|1));
#ifdef _DEBUG
			threadID=getThreadId();
#endif
			break;
		}
		assert((count&(X_BIT|U_BIT))!=(X_BIT|U_BIT));
		return true;
	}
	bool upgradelock(RW_LockType lock) {
		long mask=0,spinCount=SpinC::SC.spinCount; bool unset=false;
		assert((count&(X_BIT|U_BIT))!=(X_BIT|U_BIT));
		switch (lock) {
		case RW_NO_LOCK: break;
		case RW_S_LOCK: assert(false); return false;
		case RW_U_LOCK: assert(false); return false;
		case RW_X_LOCK:
			assert((count&U_BIT)!=0);
			for (long c=count; ;c=count)
				if ((c&(RX_BIT|X_BIT))!=0 || (unset=cas(&count,c,c|RX_BIT))) break;
				else if (--spinCount<0) threadYield();
			mask=unset?(U_BIT|RX_BIT):U_BIT; spinCount=SpinC::SC.spinCount;
			RW_LOCK((c&~(RU_BIT|U_BIT|RX_BIT|X_BIT))!=1,(c&~mask)|X_BIT);
#ifdef _DEBUG
			threadID=getThreadId();
#endif
			break;
		}
		assert((count&(X_BIT|U_BIT))!=(X_BIT|U_BIT));
		return true;
	}
	bool downgradelock(RW_LockType lock) {
		assert((count&(X_BIT|U_BIT))!=(X_BIT|U_BIT));
		switch (lock) {
		case RW_NO_LOCK: if (count!=0) unlock(); break;
		case RW_S_LOCK:
			RW_TRYLOCK((c&~(RU_BIT|U_BIT|RX_BIT|X_BIT))==0||(c&(U_BIT|X_BIT))==0,c&~(X_BIT|U_BIT));
#ifdef _DEBUG
			threadID=0;
#endif
			break;
		case RW_U_LOCK:
			RW_TRYLOCK((c&~(RU_BIT|U_BIT|RX_BIT|X_BIT))==0||(c&X_BIT)==0,c&~X_BIT|U_BIT);
			break;
		case RW_X_LOCK: assert(false); return false;
		}
		assert((count&(X_BIT|U_BIT))!=(X_BIT|U_BIT));
		return true;
	}
	void unlock(bool fForce=false) {
		assert((count&(X_BIT|U_BIT))!=(X_BIT|U_BIT));
		assert(!fForce || (count&(U_BIT|X_BIT))!=0);
		for (long c=count; ;c=count) 
			if (cas(&count,c,(fForce||(c&~(RU_BIT|U_BIT|RX_BIT|X_BIT))==1?c&~(U_BIT|X_BIT):c)-1)) break;
	}
	bool	isXLocked() const {return (count&X_BIT)!=0;}
	bool	isULocked() const {return (count&U_BIT)!=0;}
	bool	isLocked() const {return count!=0;}
};

class RWLockP
{
	RWLock	*lock;
public:
	RWLockP(RWLock *r=NULL,RW_LockType type=RW_NO_LOCK) : lock(r) {if (r!=NULL&&type!=RW_NO_LOCK) r->lock(type);}
	~RWLockP() {if (lock!=NULL) lock->unlock();}
	void set(RWLock *r,RW_LockType type=RW_NO_LOCK) {if (lock!=NULL) lock->unlock(); if ((lock=r)!=NULL&&type!=RW_NO_LOCK) r->lock(type);}
};

class SharedCounter
{
	volatile long	cnt;
public:
	SharedCounter(int c=0) : cnt(c) {}
	long operator++() {return InterlockedIncrement(&cnt);}
	long operator--() {return InterlockedDecrement(&cnt);}
	long operator+=(long v) {long c; for (c=cnt; !cas(&cnt,c,c+v); c=cnt); return c+v;} 
	long operator-=(long v) {long c; for (c=cnt; !cas(&cnt,c,c-v); c=cnt); return c-v;} 
	long operator=(long val) {return cnt=val;}
	operator long() const {return cnt;}
};

class Tls
{
#ifdef WIN32
	DWORD	key;
public:
	Tls() {key=TlsAlloc();}
	~Tls() {TlsFree(key);}
	void *get() const {return TlsGetValue(key);}
	void set(void *p) const {TlsSetValue(key,p);}
#elif defined(POSIX)
	pthread_key_t key;
public:
	Tls() {pthread_key_create(&key,NULL);}	// can return NULL!
	~Tls() {pthread_key_delete(key);}
	void *get() const {return pthread_getspecific(key);}
	void set(void *p) const {pthread_setspecific(key,p);}
#endif
};

template<HEAP_TYPE allc> class Std_Alloc
{
public:
	static void *alloc(size_t s) {
#if defined(_WIN64) || defined(__x86_64__) || defined(__arm__)
		return memalign(16,s,allc);
#else
		return malloc(s,allc);
#endif
	}
};

template<unsigned blockSize=0x200,class Alloc=Std_Alloc<SERVER_HEAP> > class FreeQ
{
	SLIST_HEADER	qfree;
	SLock			lock;
public:
	FreeQ() {InitializeSListHead(&qfree);}
	void *alloc(size_t s) {
		SLIST_ENTRY *se=InterlockedPopEntrySList(&qfree); 
		if (se==NULL) {
			lock.lock();
			if ((se=InterlockedPopEntrySList(&qfree))==NULL && (se=(SLIST_ENTRY*)Alloc::alloc(blockSize*(s=s+(sizeof(void*)*2-1)&~(sizeof(void*)*2-1))))!=NULL)
				for (byte *q=(byte*)se+s,*end=(byte*)se+blockSize*s; q<end; q+=s) InterlockedPushEntrySList(&qfree,(SLIST_ENTRY*)q);
			lock.unlock();
		}
		return se;
	}
	void dealloc(void *se) {InterlockedPushEntrySList(&qfree,(SLIST_ENTRY*)se);}
#if defined(__x86_64__) || defined(__arm__)
}__attribute__((aligned(16)));
#else
};
#endif

#define	RQ_SKIP			0x0001
#define	RQ_IN_PROGRESS	0x0002
#define	RQ_IN_QUEUE		0x0004

struct StoreRef
{
	class	StoreCtx&	ctx;
	volatile	long	cnt;
	SharedCounter		cntRef;
	StoreRef(class StoreCtx& ct) : ctx(ct),cnt(0) {++cntRef;}
};

class Request
{
	volatile	long	state;	
protected:
	Request() : state(0) {}
	virtual				~Request();
	virtual	void		process() = 0;
	virtual	void		destroy() = 0;
public:
	bool				isMarked() const {return (state&(RQ_SKIP|RQ_IN_PROGRESS))==RQ_SKIP;}
	bool				markSkip(bool fSkip=true) {return (casV(&state,state&~RQ_IN_PROGRESS,long(fSkip?RQ_SKIP:0))&RQ_IN_PROGRESS)==0;}
	friend	class		RequestQueue;
	friend	class		ThreadGroup;
};

enum RQType {RQ_NORMAL, RQ_HIGHPRTY, RQ_IO};

class RQ_Alloc {
public:
	static void *alloc(size_t s);
};

class ThreadGroup {
	volatile	long	nThreads;
	volatile	long	nPendingRequests;
	const		int		nProcessors;
	const		RQType	rqt;
#ifdef WIN32
	HANDLE				completionPort;
#else
	struct	RQElt {
		RQElt		*next;
		Request		*req;
		StoreRef	*ref;
	};
	pthread_mutex_t	lock;
	pthread_cond_t	wait;
	RQElt			*first;
	RQElt			*last;
	static	FreeQ<512,RQ_Alloc>	freeQE;
#endif
	ThreadGroup(RQType);
	~ThreadGroup();
	friend	class	RequestQueue;
public:
	void			processRequests();
};

class RequestQueue
{
	ThreadGroup		normal;
	ThreadGroup		higher;
	ThreadGroup		io;
	static RequestQueue	reqQ;
	static volatile long fStart;
public:
	RequestQueue() : normal(RQ_NORMAL),higher(RQ_HIGHPRTY),io(RQ_IO) {}
	static	RC		addStore(class StoreCtx& ctx);
	static	void	removeStore(class StoreCtx& ctx,ulong timeout);
	static	bool	postRequest(Request *req,class StoreCtx *ctx,RQType=RQ_NORMAL);
	static	void	startThreads();
	static	void	stopThreads();
};

};

#endif
