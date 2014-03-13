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

Written by Mark Venguerov 2013

**************************************************************************************/

/**
 * Finite State Machines definitions
 */
#ifndef _FSM_H_
#define _FSM_H_

#include "session.h"

using namespace Afy;

namespace AfyKernel
{

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

class FSMMgr
{
	StoreCtx	*const ctx;
public:
	FSMMgr(StoreCtx *ct) : ctx(ct) {}
	RC	process(Session *ses,PIN *fsm,const Value *event=NULL,ElementID tid=STORE_COLLECTION_ID);
private:
	RC	addTransition(const Value *trans,DynArray<Value> *table);
};

}

#endif