/*************************************************************
*************************

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

Written by Mark Venguerov, Max Windish, Michael Andronov 2004-2012

**************************************************************************************/

/**
 * synchronisation functions and data structures
 * partially platform dependent
 */
#ifndef _AFYSYNC_H_
#define _AFYSYNC_H_

#include "affinity.h"
#include <stdlib.h>

using namespace Afy;

#ifdef WIN32
	#ifndef _WIN32_WINNT
		#define _WIN32_WINNT 0x0600
	#endif
	#ifndef WIN32_LEAN_AND_MEAN
		#define WIN32_LEAN_AND_MEAN
	#endif
	#include <windows.h>
	typedef HANDLE			HTHREAD;
	typedef	DWORD			THREADID;
	#define	getThreadId()	GetCurrentThreadId()
	inline	HTHREAD	getThread() {return GetCurrentThread();}
	inline	int				getNProcessors() {SYSTEM_INFO si; GetSystemInfo(&si); return si.dwNumberOfProcessors;}
	#define THREAD_SIGNATURE DWORD WINAPI
	#define	WAIT_SPIN_COUNT	18					/**< threshold between spin count and wait queue */
	#define	SPIN_COUNT		1000				/**< number of cycles before testing for lock in spin mode */
#else
	#include <pthread.h>
	#include <semaphore.h>
	#include <unistd.h>
	#include <errno.h>
	#include <sys/time.h>
	#include <new>
	#ifdef __APPLE__
		#include <TargetConditionals.h>
		#include <mach/mach.h>
	#else
		#include <malloc.h>
	#endif
	typedef pthread_t		HTHREAD;
	typedef	pthread_t		THREADID;
	#define	getThreadId()	pthread_self()
	inline	HTHREAD			getThread() {return pthread_self();}
	inline	int				getNProcessors() {return (int)sysconf(_SC_NPROCESSORS_ONLN);}
	#define THREAD_SIGNATURE void *
	#define	WAIT_SPIN_COUNT	18					/**< threshold between spin count and wait queue */
	#define	SPIN_COUNT		1000				/**< number of cycles before testing for lock in spin mode */
#endif

#if defined __arm__ && !defined __llvm__
extern "C" {
bool __sync_bool_compare_and_swap_8(volatile void* destination, long long unsigned comperand, long long unsigned exchange);		// on ARM this function doesn't exists and implemented via assembler
}
#endif

namespace Afy
{

/**
 * platform dependent wait functions
 */
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
/**
 * compare-and-swap functions on Windows implemented as InterlockedCompareExhange()
 * for non-blocking lists standard InterlockedSList functions are used
 */
template<typename T> __forceinline bool cas(volatile T *ptr,T oldV,T newV) {return InterlockedCompareExchange(ptr,newV,oldV)==oldV;}
template<typename T> __forceinline bool casP(T *volatile *ptr,T *oldV,T *newV) {return InterlockedCompareExchangePointer((volatile PVOID *)ptr,newV,oldV)==oldV;}
template<typename T> __forceinline T casV(volatile T *ptr,T oldV,T newV) {return InterlockedCompareExchange(ptr,newV,oldV);}
#elif !defined(__arm__)
/**
 * compare-and-swap functions in glibc
 */
#define	cas(a,b,c)									__sync_bool_compare_and_swap(a,b,c)
#define	casP(a,b,c)									__sync_bool_compare_and_swap(a,b,c)
#define	casV(a,b,c)									__sync_val_compare_and_swap(a,b,c)
#define	InterlockedIncrement(a)						__sync_add_and_fetch(a,1)
#define	InterlockedDecrement(a)						__sync_sub_and_fetch(a,1)
#else		//__arm__
/**
 * ARM compare-and-swap functions use the same names as glibc but implemented in assembler
 */
#define	cas(a,b,c)									__sync_bool_compare_and_swap(a,b,c)
#define	casP(a,b,c)									__sync_bool_compare_and_swap(a,b,c)
#define	casV(a,b,c)									__sync_val_compare_and_swap(a,b,c)
#define	InterlockedIncrement(a)						__sync_add_and_fetch(a,1)
#define	InterlockedDecrement(a)						__sync_sub_and_fetch(a,1)
#endif

/**
 * platform dependent yield() function
 */
inline void threadYield()
{
	#ifdef WIN32
		::SwitchToThread();
	#elif defined(__APPLE__)
		::sched_yield();
	#else
		::pthread_yield();
	#endif
}
/**
 * platform dependent sleep() function
 */
inline void threadSleep(long pMilli)
{
	#ifdef WIN32
		::Sleep(pMilli);
	#else
		struct timespec tm={pMilli/1000,(pMilli%1000)*1000000},rtm={0,0};
		while (::nanosleep(&tm, &rtm)<0 && errno==EINTR) tm=rtm;
	#endif
}

/**
 * spin counter for spin locks
 */
class AFY_EXP SC {
public:
	static long getSC();
};

/**
 * semaphore data used for queue of threads waiting for resource
 */
struct AFY_EXP SemData {
	SemData		*next;
#ifdef WIN32
	/**
	 * Windows: to put a thread to wait state/to wakeup a thread SuspendThread()/ResumeThread() API calls are used
	 * it's much faster than using standard WaitForSingleObject calls and they don't use Windows kernel resources
	 */
	HTHREAD		thread;
	SemData() :	thread(0) {}
	~SemData()	{detach();}
	void		detach() {if (thread!=0) {CloseHandle(thread); thread=0;}}
	void		wait() {if (thread==0) {HANDLE hProc=GetCurrentProcess(); DuplicateHandle(hProc,GetCurrentThread(),hProc,&thread,THREAD_SUSPEND_RESUME,FALSE,0);} SuspendThread(thread);}
	void		wakeup() {for (long spinCount=SC::getSC(); thread==(HTHREAD)0||ResumeThread(thread)!=1;) if (--spinCount<0) threadYield();}
#elif defined(__APPLE__)
	//OS X does not support un-named pthread semaphores. The named phtread semaphores are implemented on base of MACH semaphores... 
	semaphore_t machsem;
	SemData() {kern_return_t kr;
			  kr = semaphore_create(mach_task_self(), &machsem,SYNC_POLICY_FIFO, 0);
			  if( kr != KERN_SUCCESS) mach_error( "semaphore create: ",kr);
    }
	~SemData()	{semaphore_destroy(mach_task_self(), machsem);}
	void		detach() {}
	void		wait()   {kern_return_t kr;  while( KERN_SUCCESS != (kr = semaphore_wait(machsem)) ){ /*mach_error( "semaphore_wait: ", kr); printf("errno = %d\n",errno);*/}}
	void		wakeup() {kern_return_t kr;  while( KERN_SUCCESS != (kr = semaphore_signal(machsem)) ){/* mach_error( "semaphore_signal: ", kr); printf("errno = %d\n",errno);*/}}
#else
	/**
	 * on Linux standard semaphores are used to put a thread to wait state/to wakeup a thread
	 */
	sem_t		sem;
	SemData()	{sem_init(&sem,0,0);}
	~SemData()	{sem_destroy(&sem);}
	void		detach() {}
	void		wait() {while (sem_wait(&sem)<0 && errno==EINTR);}
	void		wakeup() {while (sem_post(&sem)<0 && errno==EINTR);}
#endif
};

/**
 * simple semaphore implementation
 * no protection against long waits as any random thread is woken up when resource becomes available
 */
class AFY_EXP SimpleSem
{
	SemData* volatile	chain;
public:
	SimpleSem() : chain(NULL) {}
	~SimpleSem();
	void		lock(SemData&);
	bool		trylock(SemData&);
	void		unlock(SemData&);
};

/**
 * variation of simple semaphore
 */
class AFY_EXP SimpleSemNC
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

/**
 * semaphore implementation with ordered waiting queue
 * protected against long waits but more overhead than simple semaphore
 */
class AFY_EXP Semaphore
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

/**
 * platform-dependent Mutex implementation
 */
class AFY_EXP Mutex
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
#else
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

/**
 * Mutex holder
 * releases Mutex when goes out of scope
 */
class AFY_EXP MutexP
{
	Mutex	*lock;
public:
	MutexP(Mutex *m=NULL) : lock(m) {if (m!=NULL) m->lock();}
	~MutexP() {if (lock!=NULL) lock->unlock();}
	void set(Mutex *m) {if (lock!=m) {if (lock!=NULL) lock->unlock(); if ((lock=m)!=NULL) m->lock();}}
};

/**
 * Spin lock implementation
 */
class AFY_EXP SLock
{
	volatile	long	count;
#ifdef _DEBUG
	THREADID			threadID;
#endif
public:
	SLock() : count(0) {}
	void lock() {
		for (long spinCount=SC::getSC(); !cas(&count,0L,1L); ) if (--spinCount<0) threadYield();
#ifdef _DEBUG
		threadID=getThreadId();
#endif
	}
	bool trylock() {return cas(&count,0L,1L);}
	void unlock() {assert(count!=0); count=0;}
	bool isLocked() const {return count!=0;}
};

/**
 * Event object used to signal certain event to multiple threads
 * supports waits with timeouts
 * platform-dependent implementation
 */
class AFY_EXP Event
{
#ifdef WIN32
	HANDLE			event;
public:
	Event() {event=CreateEvent(NULL,TRUE,FALSE,NULL);}
	~Event() {if (event!=NULL) CloseHandle(event);}
	void wait(Mutex& lock,unsigned timeout) {lock.unlock(); /* ??? */ WaitForSingleObject(event,timeout==0?INFINITE:timeout); lock.lock();}
	void signal() {PulseEvent(event);}
	void signalAll() {SetEvent(event);}
	void reset() {ResetEvent(event);}
	operator HANDLE() const {return event;}
#elif defined(POSIX)
	pthread_cond_t	cond;
	volatile unsigned	nsig;
public:
	Event() : nsig(0) {pthread_cond_init(&cond,NULL);}
	~Event() {pthread_cond_destroy(&cond);}
	void wait(Mutex& lock,unsigned timeout) {
		if (nsig==0) {
			if (timeout==0) pthread_cond_wait(&cond,lock);
			else {
				struct timespec ts;
#ifdef __APPLE__
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
		if (nsig!=0) --nsig;
		
	}
	void signal() {++nsig; pthread_cond_signal(&cond);}
	void signalAll() {++nsig; pthread_cond_broadcast(&cond);}
	void reset() const {}
#endif
};

/**
 * Read-write lock
 * main synchronization primitive
 */

/**
 * RW lock types
 */
enum RW_LockType
{
	RW_NO_LOCK,		/**< lock is not locked */
	RW_S_LOCK,		/**< lock is acquired for read access, potentially by multiple threads */
	RW_X_LOCK,		/**< lock is acquired by write access, only one thread */
	RW_U_LOCK		/**< 'update' access, one thread in RW_U_LOCK mode, multiple - in RW_S_LOCK; can be upgraded to RW_X_LOCK */
};

#define	RW_MASK	0x0F

/**
 * various macros for RW lock implementation
 */
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

/**
 * RW lock with a waiting queue
 */
class AFY_EXP RWLock
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
		long spinCount=SC::getSC(),waitSpinCount=WAIT_SPIN_COUNT; SemData *volatile q=(SemData*)~0ULL;
		switch (lock) {
		case RW_NO_LOCK: break;
		case RW_S_LOCK: RW_LOCK((c&(RX_BIT|X_BIT))!=0,c+1); break;
		case RW_U_LOCK:
			RW_LOCK((c&RU_BIT)!=0,c|RU_BIT);
			spinCount=SC::getSC(); waitSpinCount=WAIT_SPIN_COUNT;
			RW_LOCK((c&(U_BIT|RX_BIT|X_BIT))!=0,((c|U_BIT)&~RU_BIT)+1);
#ifdef _DEBUG
			threadID=getThreadId();
#endif
			break;
		case RW_X_LOCK:
			RW_LOCK((c&RX_BIT)!=0,c|RX_BIT);
			spinCount=SC::getSC(); waitSpinCount=WAIT_SPIN_COUNT;
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
		long mask=0,spinCount=SC::getSC(),waitSpinCount=WAIT_SPIN_COUNT;
		switch (lock) {
		case RW_NO_LOCK: break;
		case RW_S_LOCK: assert(false); return false;
		case RW_U_LOCK: assert(false); return false;
		case RW_X_LOCK:
			assert((count&U_BIT)!=0);
			for (long c=count; ;c=count)
				if ((c&(RX_BIT|X_BIT))!=0 || (unset=cas(&count,c,c|RX_BIT))) break; else {RW_WAIT}
			mask=unset?(U_BIT|RX_BIT):U_BIT; spinCount=SC::getSC(); waitSpinCount=WAIT_SPIN_COUNT;
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

/**
 * RW spin lock, no waiting queue
 */
class AFY_EXP RWSpin
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
		long spinCount=SC::getSC();
		switch (lock) {
		case RW_NO_LOCK: break;
		case RW_S_LOCK: RW_LOCK((c&(RX_BIT|X_BIT))!=0,c+1); break;
		case RW_U_LOCK:
			RW_LOCK((c&RU_BIT)!=0,c|RU_BIT);
			spinCount=SC::getSC();
			RW_LOCK((c&(U_BIT|RX_BIT|X_BIT))!=0,(c&~RU_BIT|U_BIT)+1);
#ifdef _DEBUG
			threadID=getThreadId();
#endif
			break;
		case RW_X_LOCK:
			RW_LOCK((c&RX_BIT)!=0,c|RX_BIT);
			spinCount=SC::getSC();
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
		long mask=0,spinCount=SC::getSC(); bool unset=false;
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
			mask=unset?(U_BIT|RX_BIT):U_BIT; spinCount=SC::getSC();
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

/**
 * RW lock holder
 * releases the lock when goes out of scope
 */
class AFY_EXP RWLockP
{
	RWLock	*lock;
public:
	RWLockP(RWLock *r=NULL,RW_LockType type=RW_NO_LOCK) : lock(r) {if (r!=NULL&&type!=RW_NO_LOCK) r->lock(type);}
	~RWLockP() {if (lock!=NULL) lock->unlock();}
	void set(RWLock *r,RW_LockType type=RW_NO_LOCK) {if (lock!=NULL) lock->unlock(); if ((lock=r)!=NULL&&type!=RW_NO_LOCK) r->lock(type);}
};

/**
 * concurrent counter
 */
class AFY_EXP SharedCounter
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

#ifdef WIN32
// uses WIN32 provided SLIST API
#elif defined(_NO_DCAS)
struct SLIST_ENTRY
{
	SLIST_ENTRY		*Next;
};
struct SLIST_HEADER
{
	uint8_t	lock[sizeof(RWSpin)];
	SLIST_ENTRY	Next;
};

inline void InitializeSListHead(SLIST_HEADER *head)
{
	new((void*)head->lock) RWSpin; head->Next.Next=NULL;
}
inline void InterlockedPushEntrySList(SLIST_HEADER *pHeader,SLIST_ENTRY *entry)
{
	((RWSpin*)pHeader->lock)->lock(RW_X_LOCK);
	entry->Next=pHeader->Next.Next; pHeader->Next.Next=entry;
	((RWSpin*)pHeader->lock)->unlock();
}
inline SLIST_ENTRY *InterlockedPopEntrySList(SLIST_HEADER *pHeader)
{
	((RWSpin*)pHeader->lock)->lock(RW_X_LOCK);
	SLIST_ENTRY *pe=pHeader->Next.Next;
	if (pe!=NULL) pHeader->Next.Next=pe->Next;
	((RWSpin*)pHeader->lock)->unlock();
	return pe;
}
#elif !defined(__arm__)
/**
 * non-blocking list implementation using cas() functions for Linux and OSX
 * names are preserved from WIN32 API
 */
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
/**
 * non-blocking list implementation using cas() functions for ARM
 */
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

/**
 * thread local storage
 * used to associate Session and StoreCtx references with a thread
 * platform-dependent implementation
 */
class AFY_EXP Tls
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

/**
 * template for non-blocking concurrent queue of objects
 */
class AFY_EXP FreeQ
{
	IMemAlloc	*const	ma;
	const unsigned		blockSize;
	SLIST_HEADER		qfree;
	SLock				lock;
public:
	FreeQ(IMemAlloc *m=NULL,unsigned blkSize=0x200) : ma(m),blockSize(blkSize) {InitializeSListHead(&qfree);}
	void *alloc(size_t s) {
		SLIST_ENTRY *se=InterlockedPopEntrySList(&qfree); 
		if (se==NULL) {
			lock.lock();
			if ((se=InterlockedPopEntrySList(&qfree))==NULL) {
				size_t l=blockSize*(s=s+(sizeof(void*)*2-1)&~(sizeof(void*)*2-1));
				if (ma!=NULL) se=(SLIST_ENTRY*)ma->malloc(l);
				else
#ifdef __APPLE__
					{void *tmp; se=posix_memalign(&tmp,16,l)?NULL:(SLIST_ENTRY*)tmp;}
#elif defined(__x86_64__) || defined(__arm__)
					se=(SLIST_ENTRY*)::memalign(16,l);
#else
					se=(SLIST_ENTRY*)::malloc(l);
#endif
				if (se!=NULL) for (uint8_t *q=(uint8_t*)se+s,*end=(uint8_t*)se+blockSize*s; q<end; q+=s) InterlockedPushEntrySList(&qfree,(SLIST_ENTRY*)q);
			}
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

};

#endif
