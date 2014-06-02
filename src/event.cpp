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

#include "event.h"
#include "txmgr.h"
#include "queryprc.h"
#include "stmt.h"
#include "parser.h"
#include "expr.h"
#include "maps.h"

using namespace AfyKernel;

LIFO EventHandler::evqPool(&sharedAlloc);

static EventHandler eh;

RC Session::createEventHandler(const EventSpec evdesc[],unsigned nDesc,ActionDescriptor[])
{
	try {
		//...
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {return RC_INTERNAL;}
}

RC Session::createEventHandler(const EventSpec evdesc[],unsigned nDesc,RC (*callback)(/*???*/))
{
	try {
		//...
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {return RC_INTERNAL;}
}

StartFSM::StartFSM(const Value& fsm)
{
	ctx=fsm;
}

RC StartFSM::process(Session *ses)
{
	PINx cb(ses); PIN *fsm=&cb; RC rc;
	if (ctx.type==VT_REF) fsm=(PIN*)ctx.pin;
	else {
		assert(ctx.type==VT_REFID); cb=ctx.id;
		if ((rc=cb.getBody(TVO_UPD))!=RC_OK) return rc;
	}
	return ses->getStore()->eventMgr->process(ses,fsm,NULL,STORE_COLLECTION_ID);
}

void StartFSM::destroy(Session *ses)
{
	freeV(ctx); ses->free(this);
}

RC StartFSM::loadFSM(PINx &cb)
{
	RC rc;
	if ((rc=cb.load(LOAD_SSV))!=RC_OK) return rc; assert((cb.getMetaType()&PMT_FSMCTX)!=0);
	// start machine
	//cb.properties=NULL; cb.nProperties=0;
	return RC_OK;
}

RC EventMgr::addTransition(const Value *tr,DynArray<Value> *table)
{
	switch (tr->type) {
		case VT_STRUCT:
			//...
			break;
	}
	return RC_OK;
}

RC EventMgr::process(Session *ses,PIN *fsm,const Value *event,ElementID tid)
{
#if 0
	PIN *state=NULL; if (ses==NULL && (ses=Session::getSession())==NULL) return RC_NOSESSION;
	PINx cb(ses); const Value *pv,*trans=NULL; ValueC trns; RC rc; assert(fsm!=NULL);
	if ((rc=fsm->getV(PROP_SPEC_STATE,pv))!=RC_OK) return rc;
	if (pv==NULL || (pv->type!=VT_REF && pv->type!=VT_REFID)) return RC_CORRUPTED;
	if (pv->type==VT_REF) state=(PIN*)pv->pin; else {cb=pv->id; state=&cb;}
	// locctx
	PIN *spec[5]={NULL,fsm,NULL,event->type==VT_REF?(PIN*)event->pin:(PIN*)0,NULL};
	EvalCtx ectx(ses,spec,sizeof(spec)/sizeof(spec[0]),NULL,0);
	if (tid!=STORE_COLLECTION_ID) {
		if ((rc=state->getV(PROP_SPEC_TRANSITION,trns,LOAD_SSV,ses,tid))!=RC_OK) return rc==RC_NOTFOUND?RC_CORRUPTED:rc;
		if (trns.type==VT_STRUCT && (pv=Value::find(PROP_SPEC_CONDITION,trns.varray,trns.length))!=NULL &&
			pv->type==VT_EXPR && !((Expr*)pv->expr)->condSatisfied(ectx)) return RC_FALSE;
		trans=&trns;
	}
	for (const Value *pv;;) {
		// tx ??? EV_ASYNC???
		if (trans!=NULL) {
			assert(state!=NULL);
			if ((rc=state->getV(PROP_SPEC_ONLEAVE,pv))==RC_OK) {
				if ((rc=ctx->queryMgr->eval(pv,ectx))!=RC_OK) return rc;
			} else if (rc!=RC_NOTFOUND) return rc;
			const Value *ref=NULL;
			if (trans->type!=VT_STRUCT) ref=trans;
			else {
				if ((pv=Value::find(PROP_SPEC_ACTION,trans->varray,trans->length))!=NULL && (rc=ctx->queryMgr->eval(pv,ectx))!=RC_OK) return rc;
				ref=Value::find(PROP_SPEC_REF,trans->varray,trans->length);
			}
			if (ref==NULL) break;
			if (ref->type==VT_REFID) {cb.cleanup(); cb=ref->id; state=&cb;}
			else if (ref->type==VT_REF) state=(PIN*)ref->pin;
			else return RC_CORRUPTED;
			Value vref; vref=*ref; vref.setPropID(PROP_SPEC_STATE); setHT(vref); vref.op=OP_SET;
			if ((rc=ctx->queryMgr->modifyPIN(ectx,fsm->id,&vref,1,NULL,fsm,MODE_FSM))!=RC_OK) return rc;
		}
		if ((rc=state->getV(PROP_SPEC_ONENTER,pv))==RC_OK) {
			if ((rc=ctx->queryMgr->eval(pv,ectx))!=RC_OK) return rc;
		} else if (rc!=RC_NOTFOUND) return rc;
		if ((rc=state->getV(PROP_SPEC_TRANSITION,trans))!=RC_OK) {if (rc==RC_NOTFOUND) break; return rc;}
		// tx.ok() + force commit
		assert(trans!=NULL);
		uint32_t i=0; const Value *cv; rc=RC_TRUE;
		switch (trans->type) {
			default: return RC_TYPE;	//???	CORRUPTED
			case VT_REF: case VT_REFID: break;
			case VT_STRUCT:
				rc=addTransition(trans,NULL);
				break;
			case VT_COLLECTION:
				if (!trans->isNav()) for (i=0; i<trans->length; i++) {
					if ((rc=addTransition(&trans->varray[i],NULL))!=RC_OK) {trans=&trans->varray[i]; break;}	// release later
				} else for (cv=trans->nav->navigate(GO_FIRST); cv!=NULL; cv=trans->nav->navigate(GO_NEXT)) {
					if ((rc=addTransition(cv,NULL))!=RC_OK) {trans=cv; break;}					// release later
				}
				break;
			case VT_MAP:
				//???
				break;
		}
		if (rc!=RC_TRUE) {
			if (rc==RC_OK) {
				//check not in transaction
				//set wait for multiple events - difference with previous
			}
			return rc;
		}
	}
	// remove from tables
	if ((rc=state->getV(PROP_SPEC_ONLEAVE,pv))==RC_OK) {
		if ((rc=ctx->queryMgr->eval(pv,ectx))!=RC_OK) return rc;
	} else if (rc!=RC_NOTFOUND) return rc;
	Value vst; vst.setDelete(PROP_SPEC_STATE);
	rc=ctx->queryMgr->modifyPIN(ectx,fsm->id,&vst,1,NULL,fsm,MODE_FSM);
	// if (rc==RC_OK) tx.ok();
	// check not in transaction
	return rc;
#else
	return RC_OK;
#endif
}
