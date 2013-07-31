/**************************************************************************************

Copyright � 2004-2013 GoPivotal, Inc. All rights reserved.

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

#ifndef _TIMERQ_H_
#define _TIMERQ_H_

#include "session.h"
#include "request.h"

namespace AfyKernel
{

/**
 * Timer queue - priority queue based on Request-s and TIMESTAMP
 */
class TimerQ;

struct TimeRQ : public Request
{
	const	uint32_t	id;
	const	uint64_t	interval;
	StoreCtx	*const	ctx;
	HChain<TimeRQ>		list;
	bool				fDel;
public:
	TimeRQ(uint32_t i,uint64_t itv,StoreCtx *ct) : id(i),interval(itv),ctx(ct),list(this),fDel(false) {}
	uint32_t		getKey() const {return id;}
	virtual	void	processTimeRQ() = 0;
	virtual	void	destroyTimeRQ() = 0;
	void			process();
	void			destroy();
};

/**
 * store specific TimerQ reference
 */
struct TimerQStore : public SLIST_ENTRY
{
	TimerQ	*volatile tq;
	TimerQStore(TimerQ *t) : tq(t) {}
};

/**
 * kernel-wide TimerQ descriptor for various stores
 */
struct TimerQHdr
{
	SLIST_HEADER stores;
	FreeQ		freeLS;
	volatile	HTHREAD		timerThread;
	Mutex		lock;
	Event		sleep;
	TimerQHdr() : timerThread(0) {InitializeSListHead(&stores);}
	void		timerDaemon();
};

class TimerQ : public PQueue<TimeRQ,TIMESTAMP>
{
	StoreCtx						*const		ctx;
	SyncHashTab<TimeRQ,uint32_t,&TimeRQ::list>	requests;
	TimerQStore									*tqStore;
	bool										fStarted;
public:
	TimerQ(StoreCtx *ct,unsigned sz=100);
	~TimerQ();
	RC	loadTimer(PINx& cb);
	RC	add(TimeRQ *rq,TIMESTAMP start=0ULL);
	RC	remove(uint32_t id);
	RC	reset(uint32_t id);
	RC	startThread();
	static	void		stopThreads();
	static	TimerQHdr	timerQHdr;
	friend	struct		TimerQHdr;
};

struct TimerQElt : public TimeRQ
{
	const	PID		pid;
	Value			act;
	TimerQElt(uint32_t i,uint64_t itv,const PID& pi,const Value &ac,StoreCtx *ct) : TimeRQ(i,itv,ct),pid(pi) {copyV(ac,act,ct);}
	void			processTimeRQ();
	void			destroyTimeRQ();
};

/**
 * timer start on tx commit
 */
class StartTimer : public OnCommit
{
	uint32_t	tid;
	uint64_t	interval;
	PID			id;
	ValueC		action;
public:
	StartTimer(uint32_t t,uint64_t intv,PID i,const Value *act,MemAlloc *ma);
	RC		process(Session *ses);
	void	destroy(Session *ses);
};

};

#endif
