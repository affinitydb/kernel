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

Written by Mark Venguerov, Max Windish, Michael Andronov 2004-2012

**************************************************************************************/
#ifndef _REQUEST_H_
#define _REQUEST_H_

#include "afysync.h"
#include "session.h"

/**
 * pool of working threads implementation
 * used for async requests like log checkpoints, tree repair requests, i/o completion requests, etc.
 */

namespace AfyKernel
{

#define	RQ_SKIP			0x0001				/**< skip this request: not necessary to process */
#define	RQ_IN_PROGRESS	0x0002				/**< request processing in progress */
#define	RQ_IN_QUEUE		0x0004				/**< request is queued for execution */

/**
 * request abstract class
 */
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

/**
 * request priority types
 */
enum RQType {RQ_NORMAL, RQ_HIGHPRTY, RQ_IO};

/**
 * ThreadGroup defines a queue of requests with the same priority
 */
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
	static FreeQ	freeQE;
#endif
	ThreadGroup(RQType);
	~ThreadGroup();
	friend	class	RequestQueue;
public:
	void			processRequests();
};

/**
 * request queue implementation
 * consists of a few ThreadGroup's
 */
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
	static	void	removeStore(class StoreCtx& ctx,unsigned timeout);
	static	bool	postRequest(Request *req,class StoreCtx *ctx,RQType=RQ_NORMAL);
	static	void	startThreads();
	static	void	stopThreads();
};

#ifdef WIN32
typedef HANDLE	HTIMER;
#else
typedef	int		HTIMER;
#endif

class ITimerCallback
{
public:
	virtual	RC	callback() = 0;
};

}

#endif
