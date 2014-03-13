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

Written by Mark Venguerov, Michael Andronov 2004-2012

**************************************************************************************/

/**
 * OSX specific file i/o
 */
#ifndef _FIOOSX_H_
#define _FIOOSX_H_
#ifdef __APPLE__       // jst a reminder that the file will be used only on OSX

#include "fio.h"
#include <fcntl.h>
#include <unistd.h>
#include <aio.h>
#include <signal.h>

#define	SIGAFYAIO	(SIGUSR1)
#define	SIGAFYSIO	(SIGUSR2)

#define INVALID_FD	INVALID_HANDLE_VALUE
typedef union sigval sigval_t;

namespace AfyKernel
{
/*
 * The element of the ring buffer, issued for remembering aiocb
 */
class AIOElt : public DLList
{	
public:
	typedef enum _state {
		NOT_IN_ASYNC_QUEUE = 13,
		QUEUED,
		DONE
	} AIORqStatus;
   
	AIOElt( void *p): state(NOT_IN_ASYNC_QUEUE), payload(p){};
    inline void * getPayload(){return payload;}
	inline AIORqStatus getState() { return state;}
	inline AIORqStatus setQueued() { return state = QUEUED; }
	inline AIORqStatus setDone() { return state = DONE; }
	inline AIORqStatus setNotQueued() { return state = NOT_IN_ASYNC_QUEUE; }
	  
private:	
    AIORqStatus state;  
    void * payload;	
};

/* Used for counting available AIO -
 * in other words, preventing more then AIO_LISTIO_MAX requests be submitted at a time
 */
struct AioCnt
{
	long mx;
	long mn;
	volatile long cn;

	AioCnt(long mx = (AIO_LISTIO_MAX), long mi = 0 ) : mx(mx), mn(mi), cn(mx) {}
    ~AioCnt()	{}
    void		detach() {}
    bool operator --() {
		long tmp;
		for (tmp = cn; mn != tmp; tmp = cn) {
			assert( tmp > 0);
			if(__sync_bool_compare_and_swap(&cn, tmp, tmp-1)) return true;
		}
		return false;
	}
    bool operator ++() {
		long tmp; 
		for (tmp = cn; mx != tmp; tmp = cn) {
			if(__sync_bool_compare_and_swap(&cn, tmp, tmp+1)) return true;
		}
		return false;
	}		
    operator long() const{ return cn;} 
};
    
/* OSX signal notification on aio completion does not provide 'signal value'. 
 * The class below is the utility to track the queue of the aiocbs...
 */
class AsyncReqQ
{
public: 
	static  FreeQ	lioReqs;
	mutable	RWLock	lock;
	AIOElt * q;

	AsyncReqQ() {
		pthread_create(&thSigProc, NULL, asyncOSXFinalize, NULL);
		nSig = 0;
	};

    AIOElt * add( void * p);
	void * remove();
    friend	class IOCompletionRequest;
    static void * asyncOSXFinalize(void *);
    pthread_t thSigProc;
    SemData sigSem;
    AioCnt  aioCnt;
	SharedCounter nSig;
};   
  
/**
 * OSX specific file i/o manager
 * controls file open, close, delete opeartions
 */
class FileMgr : public GFileMgr
{
public:
#ifndef STORE_AIO_THREAD
	static	FreeQ	freeIORequests;
	static  FreeQ	asyncReqs;
#endif
	FileMgr(class StoreCtx *ct,int maxOpenFiles,const char *ldDir);
	RC		open(FileID& fid,const char *fname,unsigned flags=0);
	RC		listIO(int mode,int nent,myaio* const* pcbs,bool fSync=false);
    RC      aio_listIO( int mode, myaio *const *pcbs, int nent);
	static	void _asyncAIOCompletion(int signal, siginfo_t *info, void *uap);
	static	void _asyncSIOCompletion(int signal, siginfo_t *info, void *uap);
	friend	class IOCompletionRequest;
	friend	class AsyncReqQ;
public:
	static  AsyncReqQ   lioAQueue;  //async queue... 
};
	
};
#endif
#endif
