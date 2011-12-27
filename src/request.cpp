/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "mvstoreimpl.h"
#include "startup.h"

using namespace	MVStore;
using namespace MVStoreKernel;

#define	MAX_THREADS		100
#define	THREAD_FACTOR	10
#define	THREAD_DELTA	1

RequestQueue	RequestQueue::reqQ;
volatile long	RequestQueue::fStart = 0;

#ifdef WIN32
static DWORD WINAPI callProcessRequests(void *param) {((ThreadGroup*)param)->processRequests(); return 0;}
#else
#include <sys/mman.h>
#include "storeio.h"
#ifdef Darwin
#define MAP_ANONYMOUS MAP_ANON
#endif

void *RQ_Alloc::alloc(size_t s) {
	size_t pagesize=sysconf(_SC_PAGE_SIZE);
	return mmap(0, s+(pagesize-1)&~(pagesize-1), PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
}
FreeQ<512,RQ_Alloc> ThreadGroup::freeQE;
static void *callProcessRequests(void *param) {((ThreadGroup*)param)->processRequests(); return NULL;}
static sigset_t sigSIO,sigAIO;
#endif

ThreadGroup::ThreadGroup(RQType r) : nThreads(0),nPendingRequests(0),nProcessors(getNProcessors()),rqt(r)
{
#ifdef WIN32
	completionPort=CreateIoCompletionPort(INVALID_HANDLE_VALUE,NULL,(ULONG_PTR)0,0);
//	if (completionPort==NULL) throw ...
#else
	first=last=NULL; pthread_mutex_init(&lock,NULL); pthread_cond_init(&wait,NULL);
	sigemptyset(&sigSIO); sigaddset(&sigSIO,SIGPISIO); sigemptyset(&sigAIO); sigaddset(&sigAIO,SIGPIAIO);
#endif
}

ThreadGroup::~ThreadGroup()
{
#ifdef WIN32
	if (completionPort!=NULL) ::CloseHandle(completionPort);
#else
	//....
#endif
}

void RequestQueue::startThreads()
{
	if (cas(&fStart,0L,1L)) {
		HTHREAD thread; 
		while (createThread(callProcessRequests,&reqQ.io,thread)==RC_REPEAT);
		while (createThread(callProcessRequests,&reqQ.higher,thread)==RC_REPEAT);
		while (createThread(callProcessRequests,&reqQ.normal,thread)==RC_REPEAT);
	}
}

void RequestQueue::stopThreads()
{
#ifdef WIN32
	CloseHandle(reqQ.higher.completionPort);
	CloseHandle(reqQ.normal.completionPort);
	CloseHandle(reqQ.io.completionPort);
#else
	//...
#endif
}

RC RequestQueue::addStore(StoreCtx& ctx)
{
	return (ctx.ref=new(::malloc(sizeof(StoreRef))) StoreRef(ctx))!=NULL?RC_OK:RC_NORESOURCES;
}

volatile static long nPendingHW = 0;
volatile static long nThreadHW = 0;

void RequestQueue::removeStore(StoreCtx& ctx,ulong timeout)
{
	if (ctx.ref!=NULL) {
		for (long cnt=ctx.ref->cnt; !cas(&ctx.ref->cnt,cnt,long(cnt|0x80000000)); cnt=ctx.ref->cnt);
		if ((ctx.ref->cnt&0x7FFFFFFF)!=0) {
			TIMESTAMP endWait,ts; getTimestamp(endWait); endWait+=timeout*1000;
			while ((ctx.ref->cnt&0x7FFFFFFF)!=0 && (getTimestamp(ts),ts)<endWait) threadYield();
		}
		if (--ctx.ref->cntRef==0) ::free(ctx.ref);
//		report(MSG_INFO,"\n\n>>> Threads: %d\tMax Threads: %d\tTotal Threads: %d\tMax Requests: %d\n\n",reqQ.normal.nThreads+reqQ.higher.nThreads,nThreadHW,nTotalThreads,nPendingHW);
	}
}

void ThreadGroup::processRequests()
{
	Session *ses=Session::createSession(NULL); if (ses==NULL) return;
	long nth=InterlockedIncrement(&nThreads);
	for (long nt=nThreadHW; nth>nt && !cas(&nThreadHW,nt,nth); nt=nThreadHW);
	if (rqt==RQ_HIGHPRTY) {
#ifdef WIN32
		BOOL rc=::SetThreadPriority(GetCurrentThread(),THREAD_PRIORITY_ABOVE_NORMAL);
		if (rc==FALSE) report(MSG_ERROR,"Thread set priority error: %d\n",GetLastError());
#else
		int policy=0; struct sched_param param;
		if (pthread_getschedparam(pthread_self(),&policy,&param)==0)
			{param.sched_priority++; pthread_setschedparam(pthread_self(),policy,&param);}
		pthread_sigmask(SIG_UNBLOCK,&sigSIO,0);
#endif
	}
	for (;;) {
		StoreRef *ref; Request *req;
#ifdef WIN32
		DWORD l; BOOL rc=GetQueuedCompletionStatus(completionPort,&l,(ULONG_PTR*)&ref,(LPOVERLAPPED*)&req,INFINITE);
		if (rc==FALSE||req==NULL) {InterlockedDecrement(&nThreads); break;}
#else
		pthread_sigmask(SIG_BLOCK,&sigAIO,0); pthread_mutex_lock(&lock); RQElt *qe;
		while ((qe=first)==NULL) pthread_cond_wait(&wait,&lock);
		if ((first=qe->next)==NULL) last=NULL; pthread_mutex_unlock(&lock); 
		ref=qe->ref; req=qe->req; freeQE.dealloc(qe); pthread_sigmask(SIG_UNBLOCK,&sigAIO,0);
#endif
		assert(nPendingRequests>0); InterlockedDecrement(&nPendingRequests); HTHREAD thread;
		if (nThreads<MAX_THREADS && nThreads<nPendingRequests/THREAD_FACTOR) createThread(callProcessRequests,this,thread);
		if (ref!=NULL) {
			long cnt=ref->cnt;
			while ((cnt&0x80000000)==0 && !cas(&ref->cnt,cnt,cnt+1)) cnt=ref->cnt;
			if (--ref->cntRef==0) {::free(ref); continue;} 
			if ((cnt&0x80000000)==0) ses->set(&ref->ctx); else continue;
		}
		for (long s=req->state; !cas(&req->state,s,s|RQ_IN_PROGRESS); s=req->state);
		if ((req->state&RQ_SKIP)==0 && (ref==NULL||!ref->ctx.inShutdown())) try {req->process();} catch (...) {}
		for (long s=req->state; !cas(&req->state,s,s&~(RQ_IN_PROGRESS|RQ_IN_QUEUE)); s=req->state);
		req->destroy(); assert(ses->getTxState()==TX_NOTRAN && !ses->hasLatched()); ses->set(NULL);
		if (ref!=NULL) InterlockedDecrement(&ref->cnt);
		nth=InterlockedDecrement(&nThreads); if (rqt!=RQ_IO && nth>nProcessors && nth>nPendingRequests/THREAD_FACTOR+THREAD_DELTA) break;
		InterlockedIncrement(&nThreads);
	}
	Session::terminateSession();
#ifndef WIN32
	pthread_detach(pthread_self());
#endif
}

bool RequestQueue::postRequest(Request *req,StoreCtx *ctx,RQType rqt)
{
	if (req==NULL||ctx!=NULL&&(ctx->ref==NULL||ctx->inShutdown())) return false;
	for (long s=req->state; ;s=req->state) {
		if ((s&RQ_IN_QUEUE)!=0) return true;
		if (cas(&req->state,s,s|RQ_IN_QUEUE)) break;
	}
	// check for stuck threads!!!
	ThreadGroup& tg=rqt==RQ_IO?reqQ.io:rqt==RQ_HIGHPRTY?reqQ.higher:reqQ.normal;
#ifdef WIN32
	if (ctx!=NULL) ++ctx->ref->cntRef; long np=InterlockedIncrement(&tg.nPendingRequests);
	if (PostQueuedCompletionStatus(tg.completionPort,0,(ULONG_PTR)(ctx!=NULL?ctx->ref:0),(OVERLAPPED*)req)==FALSE)
		{InterlockedDecrement(&tg.nPendingRequests); if (ctx!=NULL) --ctx->ref->cntRef; return false;}
#else
	sigset_t omask; pthread_sigmask(SIG_BLOCK,&sigAIO,&omask);
	ThreadGroup::RQElt *qe=(ThreadGroup::RQElt*)ThreadGroup::freeQE.alloc(sizeof(ThreadGroup::RQElt));
	if (qe==NULL) {pthread_sigmask(SIG_SETMASK,&omask,0); return false;}
	if (ctx!=NULL) ++ctx->ref->cntRef; long np=InterlockedIncrement(&tg.nPendingRequests);
	qe->next=NULL; qe->req=req; qe->ref=ctx!=NULL?ctx->ref:0;
	pthread_mutex_lock(&tg.lock); if (tg.last==NULL) tg.first=tg.last=qe; else tg.last=tg.last->next=qe;
	pthread_cond_signal(&tg.wait); pthread_mutex_unlock(&tg.lock); pthread_sigmask(SIG_SETMASK,&omask,0);
#endif
	for (long n=nPendingHW; np>n && !cas(&nPendingHW,n,np); n=nPendingHW);
	return true;
}

Request::~Request()
{
}

