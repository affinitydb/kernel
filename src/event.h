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
 
 Written by Mark Venguerov 2014
 
 **************************************************************************************/

/**
 * event processing classes
 */
#ifndef _EVENT_H_
#define _EVENT_H_

#include "session.h"

namespace AfyKernel
{

class Event
{
};

enum EventHandlerState
{
		EHS_ENTRY, EHS_UNREG, EHS_REG, EHS_WAIT, EHS_PROC
};

#define	EH_BLOCK_EVENTS		0x0001

class EventHandler
{
	volatile EventHandlerState	state;
	volatile unsigned			flags;
	RWLock						lock;
	Queue<Event*>				evq;
	static	Pool				evqPool;
public:
	EventHandler() : state(EHS_ENTRY),flags(0),evq(evqPool) {}
	RC addEvent(unsigned idx/*,event descr*/) {
		RWLockP lck(&lock,RW_S_LOCK);
		if ((flags&EH_BLOCK_EVENTS)!=0) return RC_FALSE;
		// add event to idx queue
		return RC_OK;
	}
	void blockEvents(bool fCleanup) {
		RWLockP lck(&lock,RW_X_LOCK);
		flags|=EH_BLOCK_EVENTS;
		if (fCleanup) {
			// cleanup queues
		}
	}
	void unblockEvents() {
		RWLockP lck(&lock,RW_X_LOCK);
		flags&=~EH_BLOCK_EVENTS;
	}
};

class FSM : public EventHandler
{
	//
};

/**
 * start an FSM on tx commit
 */
class StartFSM : public OnCommit
{
	Value	ctx;
public:
	StartFSM(const Value& ct);
	RC		process(Session *ses);
	void	destroy(Session *ses);
	static	RC	loadFSM(PINx&cb);
};
	
class EventMgr
{
	StoreCtx	*const ctx;
public:
	EventMgr(StoreCtx *ct) : ctx(ct) {}
	RC	process(Session *ses,PIN *fsm,const Value *event=NULL,ElementID tid=STORE_COLLECTION_ID);
private:
	RC	addTransition(const Value *trans,DynArray<Value> *table);
};

}
#endif
