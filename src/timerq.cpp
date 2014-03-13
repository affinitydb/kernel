/**************************************************************************************

Copyright � 2004-2014 GoPivotal, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,  WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.

Written by Mark Venguerov 2012

**************************************************************************************/

#include "timerq.h"
#include "affinity.h"
#include "queryprc.h"
#include "pinex.h"
#include "maps.h"

using namespace Afy;
using namespace AfyKernel;

TimerQHdr TimerQ::timerQHdr;

#ifdef WIN32
static DWORD WINAPI _timerDaemon(void *param) {((TimerQHdr*)param)->timerDaemon(); return 0;}
#else
static void *_timerDaemon(void *param) {((TimerQHdr*)param)->timerDaemon(); return NULL;}
#endif

TimerQ::TimerQ(StoreCtx *ct,unsigned sz) : PQueue<TimeRQ,TIMESTAMP>(*(MemAlloc*)ct),ctx(ct),requests(sz,(MemAlloc*)ct),fStarted(false)
{
	if ((tqStore=new(timerQHdr.freeLS.alloc(sizeof(TimerQStore))) TimerQStore(this))==NULL) throw RC_NORESOURCES;
	InterlockedPushEntrySList(&timerQHdr.stores,tqStore);
}

TimerQ::~TimerQ()
{
	if (tqStore!=NULL) for (TimerQ *tq=tqStore->tq; tq!=NULL && 
			!casP((void* volatile *)&tqStore->tq,(void*)this,(void*)0); tq=tqStore->tq) threadYield();
}

RC TimerQ::loadTimer(PINx &cb)
{
	RC rc; Value v[2]; if ((ctx->mode&STARTUP_RT)!=0) return RC_OK;
	if ((rc=cb.getV(PROP_SPEC_OBJID,v[0],0,NULL))!=RC_OK) return rc;
	if (v[0].type!=VT_URIID || v[0].uid==STORE_INVALID_URIID) {freeV(v[0]); return RC_CORRUPTED;}
	URIID cid=v[0].uid; assert((cb.getMetaType()&PMT_TIMER)!=0);
	if ((rc=cb.getV(PROP_SPEC_INTERVAL,v[0],LOAD_SSV,cb.getSes()))==RC_OK) {
		if (v[0].type!=VT_INTERVAL) {freeV(v[0]); return RC_CORRUPTED;}
		if ((rc=cb.getV(PROP_SPEC_ACTION,v[1],LOAD_SSV,cb.getSes()))!=RC_OK) return rc;
		rc=add(new(ctx) TimerQElt(cid,v[0].i64,cb.getPID(),v[1],ctx));
		freeV(v[1]);
	}
	return rc;
}

RC TimerQ::add(TimeRQ *tr,TIMESTAMP start)
{
	if (tr==NULL) return RC_NORESOURCES;
	requests.insert(tr); TIMESTAMP now; getTimestamp(now);
	RC rc=push(tr,start<now?now+tr->interval:start); 
	if (rc==RC_TRUE) {
		if (fStarted) {TimerQ::timerQHdr.lock.lock(); TimerQ::timerQHdr.sleep.signal(); TimerQ::timerQHdr.lock.unlock();}
		rc=RC_OK;
	}
	return rc;
}

RC TimerQ::remove(uint32_t id)
{
	//
	return RC_OK;
}

RC TimerQ::reset(uint32_t id)
{
	//
	return RC_OK;
}

void TimeRQ::process()
{
	processTimeRQ();
	if (interval==0) fDel=true;
	else {
		TIMESTAMP now; getTimestamp(now); 
		if (ctx->tqMgr->push(this,now+interval)==RC_TRUE)
			{TimerQ::timerQHdr.lock.lock(); TimerQ::timerQHdr.sleep.signal(); TimerQ::timerQHdr.lock.unlock();}
	}
}

void TimeRQ::destroy()
{
	if (fDel) destroyTimeRQ();
}

void TimerQHdr::timerDaemon()
{
	if (!casP((void *volatile*)&timerThread,(void*)0,(void*)getThread())) return;

	Session *ses=Session::createSession(NULL); if (ses==NULL) return;

	for (;;) {
		TIMESTAMP ts1=~0ULL,now;
#ifdef _M_X64
		for (SLIST_ENTRY **pse=NULL,*se=(SLIST_ENTRY*)((stores.HeaderX64.HeaderType!=0?stores.HeaderX64.NextEntry:stores.Header8.NextEntry)<<4); se!=NULL; se=*pse) {
#elif defined(_M_IA64)
		for (SLIST_ENTRY **pse=NULL,*se=(SLIST_ENTRY*)((stores.Header16.HeaderType!=0?stores.Header16.NextEntry:stores.Header8.NextEntry)<<4); se!=NULL; se=*pse) {
#else
		for (SLIST_ENTRY **pse=NULL,*se=stores.Next.Next; se!=NULL; se=*pse) {
#endif
			TimerQ *tq=((TimerQStore*)se)->tq;
			if (tq!=NULL && casP((void *volatile*)&((TimerQStore*)se)->tq,(void*)tq,(void*)(~0ULL))) {
				TIMESTAMP next=~0ULL; getTimestamp(now);
				for (TimeRQ *tr; tq->top(&next)!=NULL && next<=now; getTimestamp(now),next=~0ULL)
					if ((tr=tq->pop())!=NULL) RequestQueue::postRequest(tr,tq->ctx/*,prty*/);
				if (next<ts1) ts1=next; ((TimerQStore*)se)->tq=tq;
			} else if (pse!=NULL) {*pse=se->Next; freeLS.dealloc(se); continue;}
			pse=&se->Next;
		}
		getTimestamp(now);
		if (ts1>now && (ts1-now)/1000>10) {
			lock.lock(); getTimestamp(now);
			if (ts1>now && (ts1-now)/1000>10) sleep.wait(lock,ts1!=~0ULL?(unsigned)((ts1-now)/1000):10000);
			lock.unlock();
		}
	}
}

void TimerQElt::processTimeRQ()
{
	Session *ses=Session::getSession();
	if (ses!=NULL) {
		ses->setIdentity(STORE_OWNER,true);
		PINx self(ses,pid); PIN autop(ses); PIN *pp[5]={NULL,&self,NULL,NULL,&autop};
		if ((ses->getTraceMode()&TRACE_TIMERS)!=0) trace(ses,0);
		RC rc=ses->getStore()->queryMgr->eval(&act,EvalCtx(ses,pp,5,NULL,0,NULL,0,NULL,NULL,ECT_ACTION));
		if ((ses->getTraceMode()&TRACE_TIMERS)!=0) trace(ses,1,rc);
		ses->setIdentity(STORE_INVALID_IDENTITY,false);
	}
}

void TimerQElt::trace(Session *ses,int code,RC rc)
{
	TIMESTAMP ts; char buf[64]; size_t l; getTimestamp(ts); ses->convDateTime(ts,buf,l);
	URI *uri=(URI*)ctx->uriMgr->ObjMgr::find(id); const char *nm=ses->getStore()->namedMgr->getTraceName(uri,l);
	ses->trace(0,code==0?"Timer \"%.*s\": START at %s\n":"Timer \"%.*s\": END at %s -> %s\n",l,nm,buf,getErrMsg(rc));
	if (uri!=NULL) uri->release();
}

void TimerQElt::destroyTimeRQ()
{
	freeV(act); ctx->free(this);
}

StartTimer::StartTimer(uint32_t t,uint64_t intv,PID i,const Value *act,MemAlloc *ma) : tid(t),interval(intv),id(i)
{
	RC rc; if (act!=NULL && (rc=copyV(*act,action,ma))!=RC_OK) throw rc;
}

RC StartTimer::process(Session *ses)
{
	StoreCtx *ctx=ses->getStore(); return (ctx->mode&STARTUP_RT)==0?ctx->tqMgr->add(new(ctx) TimerQElt(tid,interval,id,action,ctx)):RC_OK;
}

void StartTimer::destroy(Session *ses)
{
	ses->free(this);
}

RC TimerQ::startThread()
{
	RC rc=RC_OK;
	if (TimerQ::timerQHdr.timerThread==(HTHREAD)0) {HTHREAD h; while ((rc=createThread(_timerDaemon,&TimerQ::timerQHdr,h))==RC_REPEAT);}
	fStarted=true; return rc;
}

void TimerQ::stopThreads()
{
#ifdef WIN32
	TerminateThread(timerQHdr.timerThread,0);
#elif defined(ANDROID)
	//????
#else
	if (timerQHdr.timerThread!=0) pthread_cancel(timerQHdr.timerThread);
#endif
}
