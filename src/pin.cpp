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

Written by Mark Venguerov 2004-2014

**************************************************************************************/

#include "affinity.h"
#include "txmgr.h"
#include "dataevent.h"
#include "queryprc.h"
#include "ftindex.h"
#include "netmgr.h"
#include "expr.h"
#include "maps.h"
#include "parser.h"
#include "service.h"
#include "fsmgr.h"
#include "stmt.h"
#include <string.h>

using namespace AfyKernel;

const PID PIN::noPID = {STORE_INVALID_PID,STORE_INVALID_IDENTITY};

PIN::~PIN()
{
	if (fNoFree==0 && ma!=NULL) freeV(properties,nProperties,ma);
}

RC PIN::refresh(bool fNet)
{
	try {
		if (!addr.defined()) return RC_OK; Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION;
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		RC rc=RC_OK; TxGuard txg(ses);
		if (fNet && isRemote(id) && (mode&PIN_REPLICATED)==0) rc=ctx->netMgr->refresh(this,ses);
		else {PINx cb(ses,id); cb=addr; rc=id.isPID()?ses->getStore()->queryMgr->reload(this,&cb):RC_NOTFOUND;}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::refresh()\n"); return RC_INTERNAL;}
}

RC PIN::setExpiration(uint32_t exp)
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION; if (ses->inReadTx()) return RC_READTX;
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		TxGuard txg(ses); return addr.defined()&&isRemote(id)&&(mode&PIN_REPLICATED)==0?ctx->netMgr->setExpiration(id,exp):RC_INVOP;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::setExpiration()\n"); return RC_INTERNAL;}
}

const PID& PIN::getPID() const
{
	return id;
}

bool PIN::isLocal() const
{
	try {return !isRemote(id);} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::isLocal()\n");}
	return true;
}

unsigned PIN::getFlags() const
{
	try {return mode&(PIN_NO_REPLICATION|PIN_REPLICATED|PIN_HIDDEN|PIN_PERSISTENT|PIN_TRANSIENT|PIN_IMMUTABLE|PIN_DELETED);}
	catch (...) {report(MSG_ERROR,"Exception in IPIN::getFlags()\n"); return 0;}
}

unsigned PIN::getMetaType() const
{
	try {return meta;} catch (...) {report(MSG_ERROR,"Exception in IPIN::getMetaType()\n"); return 0;}
}

uint32_t PIN::getNumberOfProperties() const
{
	try {return nProperties;} catch (...) {report(MSG_ERROR,"Exception in IPIN::getNumberOfProperties()\n"); return 0;}
}

const Value	*PIN::getValueByIndex(unsigned idx) const
{
	try {return properties==NULL||idx>=nProperties?NULL:&properties[idx];} catch (...) {report(MSG_ERROR,"Exception in IPIN::getValueByIndex()\n"); return NULL;}
}

const Value	*PIN::getValue(PropertyID pid) const
{
	try {return VBIN::find(pid,properties,nProperties);} catch (...) {report(MSG_ERROR,"Exception in IPIN::getValue()\n"); return NULL;}
}

RC PIN::setReplicated()
{
	try {
		mode|=PIN_REPLICATED;
		if (addr.defined()) {
			Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION; if (ses->inReadTx()) return RC_READTX;
			StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
			TxGuard txg(ses); Value w; w.set((unsigned)PIN_REPLICATED); w.setPropID(PROP_SPEC_SELF); w.setOp(OP_OR);
			RC rc=ctx->queryMgr->modifyPIN(EvalCtx(ses,ECT_INSERT),id,&w,1,NULL,this,0); if (rc!=RC_OK) return rc;
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::setReplicated()\n"); return RC_INTERNAL;}
}

RC PIN::hide()
{
	try {
		mode|=PIN_HIDDEN;
		if (addr.defined()) {
			Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION; if (ses->inReadTx()) return RC_READTX;
			StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
			TxGuard txg(ses); Value w; w.set((unsigned)PIN_HIDDEN); w.setPropID(PROP_SPEC_SELF); w.setOp(OP_OR);
			RC rc=ctx->queryMgr->modifyPIN(EvalCtx(ses,ECT_INSERT),id,&w,1,NULL,this,0); if (rc!=RC_OK) return rc;
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::setNoIndex()\n"); return RC_INTERNAL;}
}

RC PIN::deletePIN()
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION; if (ses->inReadTx()) return RC_READTX;
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		TxGuard txg(ses); if (addr.defined()) {PIN *p=this; RC rc=ctx->queryMgr->deletePINs(EvalCtx(ses),&p,&id,1,MODE_DEVENT); if (rc!=RC_OK) return rc;}
		destroy(); return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::deletePIN()\n"); return RC_INTERNAL;}
}

RC PIN::undelete()
{
	try {
		if (addr.defined()) {
			Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION; if (ses->inReadTx()) return RC_READTX;
			StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
			TxGuard txg(ses); Value w; w.set((unsigned)~PIN_DELETED); w.setPropID(PROP_SPEC_SELF); w.setOp(OP_AND);
			RC rc=ctx->queryMgr->modifyPIN(EvalCtx(ses,ECT_INSERT),id,&w,1,NULL,this,MODE_DELETED); if (rc!=RC_OK) return rc;
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::undelete()\n"); return RC_INTERNAL;}
}

RC PIN::getPINValue(Value& res) const 
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION;
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(ses); return getPINValue(res,ses);
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::getPINValue(Value&)\n"); return RC_INTERNAL;}
}

RC PIN::getPINValue(Value& res,Session *ses) const
{
	const Value *pv=VBIN::find(PROP_SPEC_VALUE,properties,nProperties);
	if (ses==NULL||pv==NULL) {res.set(id); return RC_OK;}
	for (int cnt=0;;cnt++) {
		RC rc; Expr *exp; HEAP_TYPE save;
		switch (pv->type) {
		default: return pv!=&res?copyV(*pv,res,ses):RC_OK;
		case VT_EXPR:
			{exp=(Expr*)pv->expr; save=(HEAP_TYPE)(pv->flags&HEAP_TYPE_MASK);
			res.type=VT_ERROR; const PIN *pp=this;
			rc=exp->eval(res,EvalCtx(ses,(PIN**)&pp,1,NULL,0));
			if (pv==&res && save>=SES_HEAP) if (save==SES_HEAP) ses->free(exp); else free(exp,save);}
// leaking???
			switch (rc) {
			default: return rc;
			case RC_TRUE: res.set(true); return RC_OK;
			case RC_FALSE: res.set(false); return RC_OK;
			}
		case VT_REFPROP:
		case VT_REFELT:
		case VT_REFIDPROP:
		case VT_REFIDELT:
			if (cnt>0) return RC_NOTFOUND;
			if ((rc=derefValue(*pv,res,ses))!=RC_OK) return rc;
			pv=&res; continue;
		case VT_URIID:
			if (cnt>0 || (pv=VBIN::find(pv->uid,properties,nProperties))==NULL) return RC_NOTFOUND;
			continue;
		}
	}
}

IPIN *PIN::clone(const Value *overwriteValues,unsigned nOverwriteValues,unsigned md)
{
	try {
		unsigned pmd=(md&(PIN_NO_REPLICATION|PIN_REPLICATED|PIN_HIDDEN|PIN_TRANSIENT))|(mode&(PIN_REPLICATED|PIN_NO_REPLICATION|PIN_HIDDEN));
		PIN *newp=new(ma) PIN(ma,pmd); if (newp==NULL) return NULL;
		newp->properties=(Value*)ma->malloc(nProperties*sizeof(Value));
		if (newp->properties==NULL) {delete newp; return NULL;}
		for (unsigned i=0; (newp->nProperties=i)<nProperties; i++)
			if (properties[i].type!=VT_COLLECTION||!properties[i].isNav()||properties[i].nav==NULL) {
				if (copyV(properties[i],newp->properties[i],ma)!=RC_OK) {delete newp; return NULL;}
			} else {
				INav *nav=properties[i].nav; unsigned nElts=nav->count(),cnt=0;
				Value *pv=new(ma) Value[nElts]; if (pv==NULL) {delete newp; return NULL;}
				for (const Value *cv=nav->navigate(GO_FIRST); cnt<nElts && cv!=NULL; ++cnt,cv=nav->navigate(GO_NEXT))
					if (copyV(*cv,pv[cnt],ma)!=RC_OK) {delete newp; return NULL;} 
				Value *nv=&newp->properties[i]; nv->set(pv,cnt); nv->setPropID(properties[i].property);
				nv->op=properties[i].op; nv->meta=properties[i].meta; setHT(*nv,SES_HEAP);
			}
		if (overwriteValues!=NULL && nOverwriteValues>0) for (unsigned j=0; j<nOverwriteValues; j++)
			{const Value *pv=&overwriteValues[j]; newp->modify(pv,pv->eid,STORE_COLLECTION_ID,0);}
		if ((md&MODE_PERSISTENT)!=0) {
			Session *ses=Session::getSession();
			if (ses!=NULL && !ses->inReadTx()) {
				StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown() || ctx->isServerLocked()) {delete newp; return NULL;}
				TxGuard txg(ses); assert(newp->id.isEmpty());
				if (ctx->queryMgr->persistPINs(EvalCtx(ses,ECT_INSERT),&newp,1,md)!=RC_OK) {delete newp; newp=NULL;}
			}
		}
		return newp;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::clone(...)\n");}
	return NULL;
}

IPIN *PIN::project(const PropertyID *props,unsigned nProps,const PropertyID *newProps,unsigned md)
{
	try {
		unsigned pmd=(md&(PIN_NO_REPLICATION|PIN_REPLICATED|PIN_HIDDEN|PIN_TRANSIENT))|(mode&(PIN_REPLICATED|PIN_NO_REPLICATION|PIN_HIDDEN));
		PIN *newp=new(ma) PIN(ma,pmd); if (newp==NULL) return NULL;
		if (props!=NULL && nProps>0) {
			newp->properties=(Value*)ma->malloc(nProps*sizeof(Value));
			if (newp->properties==NULL) {delete newp; return NULL;}
			const Value *oldProp;
			for (unsigned i=0; i<nProps; i++) if ((oldProp=findProperty(props[i]))!=NULL) {
				PropertyID newpid=newProps!=NULL?newProps[i]:props[i];
				Value *newProp=NULL; VBIN::find(newpid,newp->properties,newp->nProperties,&newProp);
				if (newProp<&newp->properties[newp->nProperties]) memmove(newProp+1,newProp,(byte*)&newp->properties[newp->nProperties]-(byte*)newProp);
				if (oldProp->type!=VT_COLLECTION||!oldProp->isNav()||oldProp->nav==NULL) {
					if (copyV(*oldProp,*newProp,ma)!=RC_OK) {delete newp; return NULL;}
				} else {
					INav *nav=oldProp->nav; unsigned nElts=nav->count(),cnt=0;
					Value *pv=new(ma) Value[nElts]; if (pv==NULL) {delete newp; return NULL;}
					for (const Value *cv=nav->navigate(GO_FIRST); cnt<nElts && cv!=NULL; ++cnt,cv=nav->navigate(GO_NEXT))
						if (copyV(*cv,pv[cnt],ma)!=RC_OK) {delete newp; return NULL;} 
					newProp->set(pv,cnt); newProp->op=oldProp->op; newProp->meta=oldProp->meta; setHT(*newProp,SES_HEAP);
				}
				newProp->setPropID(newpid); newp->nProperties++;
			}
		}
		if ((md&MODE_PERSISTENT)!=0) {
			Session *ses=Session::getSession();
			if (ses!=NULL && !ses->inReadTx()) {
				StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown() || ctx->isServerLocked()) {delete newp; return NULL;}
				TxGuard txg(ses); assert(newp->id.isEmpty());
				if (ctx->queryMgr->persistPINs(EvalCtx(ses,ECT_INSERT),&newp,1,md)!=RC_OK) {delete newp; newp=NULL;}
			}
		}
		return newp;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::project(...)\n");}
	return NULL;
}

RC PIN::modify(const Value *values,unsigned nValues,unsigned md,const ElementID *eids,unsigned *pNFailed)
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION; if (ses->inReadTx()) return RC_READTX;
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		if (!ses->isReplication() && (md&MODE_FORCE_EIDS)!=0 && eids!=NULL) return RC_INVPARAM;
		TxGuard txg(ses); return ctx->queryMgr->modifyPIN(EvalCtx(ses,ECT_INSERT),id,values,nValues,NULL,this,md|MODE_CHECKBI,eids,pNFailed);
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::modify(...)\n"); return RC_INTERNAL;}
}

RC PIN::modify(const Value *pv,unsigned epos,unsigned eid,unsigned flags)
{
	bool fAdd=pv->op==OP_ADD||pv->op==OP_ADD_BEFORE;
	if (pv->property==PROP_SPEC_SELF) {
		if (pv->type!=VT_UINT) return RC_TYPE;
		switch (pv->op) {
		case OP_AND: mode&=~(~pv->ui&(PIN_TRANSIENT|PIN_DELETED|PIN_IMMUTABLE|PIN_PERSISTENT|PIN_HIDDEN|PIN_REPLICATED|PIN_NO_REPLICATION)); return RC_OK;
		case OP_OR: mode|=pv->ui&(PIN_TRANSIENT|PIN_DELETED|PIN_IMMUTABLE|PIN_PERSISTENT|PIN_HIDDEN|PIN_REPLICATED|PIN_NO_REPLICATION); return RC_OK;
		default: return RC_INVOP;
		}
	}
	Value *ins=NULL,*pval=(Value*)VBIN::find(pv->property,properties,nProperties,&ins),save,*pv2;
	if (pval==NULL) {if (pv->op==OP_SET) fAdd=true; else if (!fAdd) return RC_NOTFOUND;}
	RC rc=RC_OK;
	if (fAdd) {
		if (pval==NULL) {
			ptrdiff_t sht=ins==NULL?0:ins-properties;
			if (fNoFree==0) {
				if ((properties=(Value*)ma->realloc(properties,(nProperties+1)*sizeof(Value),nProperties*sizeof(Value)))==NULL) {nProperties=0; return RC_NOMEM;}
			} else if ((pv2=new(ma) Value[nProperties+1])!=NULL) {
				memcpy(pv2,properties,nProperties*sizeof(Value));
				for (unsigned i=0; i<nProperties; i++) setHT(pv2[i]);
				properties=pv2; fNoFree=0;
			} else {nProperties=0; return RC_NOMEM;}
			ins=&properties[sht];
			if ((unsigned)sht<nProperties) memmove(ins+1,ins,sizeof(Value)*(nProperties-sht));
			if ((flags&MODP_NCPY)!=0) *ins=*pv; else rc=copyV(*pv,*ins,ma);
			if (rc==RC_OK) {
				ins->eid=eid=(flags&MODP_EIDS)!=0?eid:isCollection((ValueType)pv->type)?STORE_COLLECTION_ID:getPrefix(StoreCtx::get());
				ins->op=OP_SET; nProperties++;
			}
		} else if (pval->type==VT_COLLECTION && pval->isNav()) {
			// aggregate CNAVIGATOR
			return RC_INTERNAL;
		} else if (pv->type==VT_COLLECTION && pv->isNav()) {
			// aggregate CNAVIGATOR (Value + CNAV)
			for (const Value *cv=pv->nav->navigate(GO_FIRST); cv!=NULL; cv=pv->nav->navigate(GO_NEXT)) {
				// ???
				Value w=*cv; setHT(w); w.op=pv->op;
				if ((rc=modify(&w,epos,cv->eid,MODP_EIDS|MODP_NEID))!=RC_OK) break;
			}
			return rc;
		} else if (pval->type!=VT_COLLECTION) {		// create collection
			unsigned nElts=pv->type==VT_COLLECTION?pv->length+1:2,idx; uint8_t meta=pval->meta; PropertyID pid=pval->property;
			Value *coll=(Value*)ma->malloc(nElts*sizeof(Value)); if (coll==NULL) return RC_NOMEM;
			if (pv->op==OP_ADD) {coll[0]=*pval; idx=1;} else {coll[nElts-1]=*pval; idx=0;}
			if ((flags&MODP_EIDS)==0 && (eid=getPrefix(StoreCtx::get()))==pval->eid) eid++;
			if (pv->type!=VT_COLLECTION) {
				if ((flags&MODP_NCPY)!=0) coll[idx]=*pv;
				else if ((rc=copyV(*pv,coll[idx],ma))!=RC_OK) {ma->free(coll); return rc;}
				coll[idx].op=OP_SET; coll[idx].eid=eid;
			} else if ((flags&MODP_NCPY)==0) for (unsigned i=0; i<pv->length; i++) {
				if ((rc=copyV(pv->varray[i],coll[idx+i],ma))!=RC_OK) {ma->free(coll); return rc;}
				coll[idx+i].op=OP_SET; if ((flags&MODP_EIDS)==0) coll[idx+i].eid=eid++;
			} else {
				memcpy(&coll[idx],pv->varray,pv->length*sizeof(Value)); free((void*)pv->varray,(HEAP_TYPE)(pv->flags&HEAP_TYPE_MASK));
			}
			pval->set(coll,nElts); pval->setPropID(pid); pval->setMeta(meta); setHT(*pval,SES_HEAP);
		} else {								// add to collection
			assert(pval->varray!=NULL && pval->length>0);
			unsigned nElts=pv->type==VT_COLLECTION?pv->length:1; if (epos==STORE_COLLECTION_ID) epos=STORE_LAST_ELEMENT;
			pval->varray=(const Value*)ma->realloc(const_cast<Value*>(pval->varray),(pval->length+nElts)*sizeof(Value),pval->length*sizeof(Value));
			if (pval->varray==NULL) {pval->type=VT_ERROR; return RC_NOMEM;}
			Value *v=(Value*)findElement(pval,epos);
			if (v==NULL) v=const_cast<Value*>(&pval->varray[pval->length]);
			else {
				if (pv->op!=OP_ADD_BEFORE) v++;
				if (v<&pval->varray[pval->length]) memmove(v+nElts,v,(pval->length-(v-pval->varray))*sizeof(Value));
			}
			if ((flags&MODP_EIDS)==0) eid=getPrefix(StoreCtx::get())+pval->length;		// find max of existing!!!
			if (pv->type!=VT_COLLECTION) {
				if ((flags&MODP_NCPY)!=0) *v=*pv; else if ((rc=copyV(*pv,*v,ma))==RC_OK) v->op=OP_SET; 
				v->eid=eid;
			} else if ((flags&MODP_NCPY)==0) for (unsigned i=0; i<pv->length; ++v,++i) {
				if ((rc=copyV(pv->varray[i],*v,ma))==RC_OK) {v->op=OP_SET; if ((flags&MODP_EIDS)==0) v->eid=eid++;}
			} else {
				memcpy(v,pv->varray,pv->length*sizeof(Value)); free((void*)pv->varray,(HEAP_TYPE)(pv->flags&HEAP_TYPE_MASK));
			}
			pval->length+=nElts;
		}
		if (rc==RC_OK && (flags&MODP_NEID)==0 && pv->type!=VT_COLLECTION) pv->eid=eid;
	} else switch (pv->op) {
	case OP_SET:
		if (epos==STORE_COLLECTION_ID || pval->type!=VT_COLLECTION) {
			freeV(*pval); if ((flags&MODP_NCPY)==0) rc=copyV(*pv,*pval,ma); else *pval=*pv;
			if (pval->type!=VT_COLLECTION) pval->eid=(flags&MODP_EIDS)!=0?eid:getPrefix(StoreCtx::get());
		} else if (!pval->isNav()) {
			Value *elt=(Value*)findElement(pval,epos); if (elt==NULL) return RC_NOTFOUND;
			freeV(*elt); if ((flags&MODP_NCPY)==0) rc=copyV(*pv,*elt,ma); else *elt=*pv;
		} else {
			//...???
			return RC_INTERNAL;
		}
		break;
	case OP_DELETE:
		if (epos==STORE_COLLECTION_ID || pval->type!=VT_COLLECTION) {
			freeV(*pval); --nProperties;
			if (pval!=properties+nProperties) memmove(pval,pval+1,(nProperties-(pval-properties))*sizeof(Value));
		} else if (pval->isNav()) {
			//... aggregate (CNAV-Value)
			return RC_INTERNAL;
		} else {
			const Value *elt = findElement(pval,epos); if (elt==NULL) return RC_NOTFOUND;
			if (pval->length==1) {
				freeV(*pval); --nProperties;
				if (pval!=properties+nProperties) memmove(pval,pval+1,(nProperties-(pval-properties))*sizeof(Value));
			} else {
				freeV(*const_cast<Value*>(elt));
				if (elt<&pval->varray[--pval->length]) 
					memmove(const_cast<Value*>(elt),elt+1,sizeof(Value)*(pval->length-(elt-pval->varray)));
				pval->varray=(Value*)ma->realloc(const_cast<Value*>(pval->varray),pval->length*sizeof(Value),(pval->length+1)*sizeof(Value));
				assert(pval->varray!=NULL);
			}
		}
		break;
	case OP_RENAME:
		if (pv->type!=VT_URIID) return RC_TYPE;
		VBIN::find(pv->uid,properties,nProperties,&ins); assert(ins!=NULL);
		if (ins!=pval && ins!=pval+1) {
			save=*pval;
			if (ins<pval) memmove(ins+1,ins,(byte*)pval-(byte*)ins);
			else {memmove(pval,pval+1,(byte*)ins-(byte*)pval-sizeof(Value)); ins--;}
			*(pval=ins)=save;
		}
		pval->property=pv->uid; break;
	case OP_MOVE:
	case OP_MOVE_BEFORE:
		if (pval->type==VT_COLLECTION && pval->isNav()) {
			//???
			return RC_INTERNAL;
		}
		if (pval->type!=VT_COLLECTION||pv->type!=VT_UINT) return RC_TYPE;
		pv2=(Value*)findElement(pval,pv->ui); pval=(Value*)findElement(pval,epos);
		if (pval==NULL||pv2==NULL) return RC_NOTFOUND;
		if (pv->op==OP_MOVE) pv2++;
		if (pv2!=pval && pv2!=pval+1) {
			save=*pval;
			if (pv2<pval) memmove(pv2+1,pv2,(byte*)pval-(byte*)pv2);
			else {memmove(pval,pval+1,(byte*)pv2-(byte*)pval-sizeof(Value)); pv2--;}
			*pv2=save;
		}
		break;
	default:
		if (pv->op>OP_LAST_MODOP) return RC_INVPARAM;
		save=*pval; rc=Expr::calc((ExprOp)pv->op,*pval,pv,2,0,EvalCtx(Session::getSession())); if ((flags&MODP_NCPY)!=0) freeV(*(Value*)pv);
		pval->property=save.property; pval->eid=save.eid; pval->meta=save.meta; pval->op=save.op;
		break;
	}
	return rc;
}

bool PIN::testDataEvent(DataEventID classID,const Value *params,unsigned nParams) const
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL || ses->getStore()->inShutdown()) return false; 
		Values vv(params,nParams); TxGuard txg(ses); return ses->getStore()->queryMgr->test((PIN*)this,classID,EvalCtx(ses,NULL,0,NULL,0,&vv,1),true);
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::testDataEvent(DataEventID=%08X)\n",classID);}
	return false;
}

bool PIN::defined(const PropertyID *pids,unsigned nProps) const
{
	try {
		if (pids!=NULL) for (unsigned i=0; i<nProps; i++) if (VBIN::find(pids[i],properties,nProperties)==NULL) return false;
		return true;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::defined()\n");}
	return false;
}

bool ::PIN::checkProps(const PropertyID *pids,unsigned nProps)
{
	return PIN::defined(pids,nProps);
}

RC PIN::load(unsigned md,const PropertyID *pids,unsigned nPids)
{
	if (pids!=NULL && nPids!=0) {
		for (unsigned i=0; i<nPids; i++) if (findProperty(pids[i])==NULL) return RC_NOTFOUND;		// check prop flags
	}
	return RC_OK;
}

const Value *PIN::loadProperty(PropertyID)
{
	return NULL;
}

const void *PIN::getPropTab(unsigned& nProps) const
{
	nProps=0; return NULL;
}

RC PIN::getV(PropertyID pid,const Value *&pv)
{
	assert(pid!=STORE_INVALID_URIID && (pid&STORE_MAX_URIID)!=PROP_SPEC_PINID);
	if ((pv=VBIN::find(pid&=STORE_MAX_URIID,properties,nProperties))!=NULL) return RC_OK;
	if (fPartial!=0 && load(LOAD_SSV/*,&pid,1*/)==RC_OK && 
		(pv=VBIN::find(pid,properties,nProperties))!=NULL) return RC_OK;
	return RC_NOTFOUND;
}

RC PIN::getV(PropertyID pid,Value& v,unsigned md,MemAlloc *ma,ElementID eid)
{
	const Value *pv;
	if (pid==STORE_INVALID_URIID) {if ((md&LOAD_CARDINALITY)==0) {v.setError(); return RC_INVPARAM;} else {v.set(0u); return RC_OK;}}
	if ((pid&=STORE_MAX_URIID)==PROP_SPEC_PINID) {
		if (!id.isPID()||eid!=STORE_COLLECTION_ID) {
			if (fPINx==0||eid!=STORE_COLLECTION_ID) goto notfound;
			return ((PINx*)this)->getVx(pid,v,md,ma,eid);
		}
		if ((md&LOAD_CARDINALITY)!=0) v.set(1u); else v.set(id); v.property=pid; return RC_OK;
	}
	if ((pv=VBIN::find(pid,properties,nProperties))==NULL) 
		{if (fPartial==0) goto notfound; return ((PINx*)this)->getVx(pid,v,md,ma,eid);}
	if ((md&LOAD_CARDINALITY)!=0) {v.set(pv->count()); return RC_OK;}
	if (eid!=STORE_COLLECTION_ID) switch (pv->type) {
	default: if (pv->eid!=eid && eid!=STORE_FIRST_ELEMENT && eid!=STORE_LAST_ELEMENT) goto notfound; break;
	case VT_COLLECTION:
		if (pv->isNav()) return pv->nav->getElementByID(eid,v);
		if (eid==STORE_FIRST_ELEMENT) pv=&pv->varray[0];
		else if (eid==STORE_LAST_ELEMENT) pv=&pv->varray[pv->length-1];
		else for (unsigned i=0; ;i++) if (i>=pv->length) goto notfound; else if (pv->varray[i].eid==eid) {pv=&pv->varray[i]; break;}
		break;
	case VT_STRUCT:
		if ((pv=VBIN::find(eid&STORE_MAX_URIID,pv->varray,pv->length))==NULL) goto notfound;
	case VT_ARRAY:
	case VT_MAP:
		//??????????????????????????????????????????????
		break;
	}
	v=*pv; setHT(v); return RC_OK;
notfound:
	if ((md&LOAD_CARDINALITY)!=0) {v.set(0u); return RC_OK;} else {v.setError(pid); return RC_NOTFOUND;}
}

RC PIN::getV(PropertyID pid,Value& v,unsigned md,const Value &idx,MemAlloc *ma)
{
	if (pid!=STORE_INVALID_URIID&&(pid&=STORE_MAX_URIID)!=PROP_SPEC_PINID) {
		const Value *pv=VBIN::find(pid,properties,nProperties); ElementID eid;
		if (pv==NULL) {
			if (fPartial!=0) return ((PINx*)this)->getVx(pid,v,md,idx,ma);
		} else {
			switch (pv->type) {
			default: pv=NULL; break;
			case VT_COLLECTION:
				if (!idx.isEmpty()&&idx.type!=VT_UINT&&(idx.type!=VT_INT||idx.i<0)) pv=NULL;
				else if ((eid=idx.isEmpty()?idx.eid:idx.ui)!=STORE_COLLECTION_ID) {
					if (pv->isNav()) pv=pv->nav->getElementByID(eid,v)==RC_OK?&v:NULL;
					else if (eid==STORE_FIRST_ELEMENT) pv=&pv->varray[0];
					else if (eid==STORE_LAST_ELEMENT) pv=&pv->varray[pv->length-1];
					else for (unsigned i=0; ;i++) if (i>=pv->length) {pv=NULL; break;} else if (pv->varray[i].eid==eid) {pv=&pv->varray[i]; break;}
				}
				break;
			case VT_STRUCT:
				pv=(idx.type==VT_UINT||idx.type==VT_INT&&idx.i>=0) && idx.ui!=STORE_INVALID_URIID?VBIN::find(idx.ui&STORE_MAX_URIID,pv->varray,pv->length):NULL;
				break;
			case VT_MAP:
				pv=pv->map->find(idx); break;
			case VT_ARRAY:
				//????
				pv=NULL; break;
			}
			if (pv!=NULL) {v=*pv; setHT(v); return RC_OK;}
		}
	}
	if ((md&LOAD_CARDINALITY)==0) {v.setError(); return RC_INVPARAM;} else {v.set(0u); return RC_OK;}
}

RC PIN::getAllProps(Value*& props,unsigned& nProps,MemAlloc *pma)
{
	RC rc; 
	if (fPartial!=0) {assert(fPINx!=0); if ((rc=load(LOAD_SSV))!=RC_OK) return rc;}
	nProps=nProperties;
	if ((props=properties)!=NULL) {
		if (fNoFree==0 && ma->getAType()==pma->getAType()) {properties=NULL; nProperties=0; if (fPINx!=0) fPartial=1;}
		else if ((rc=copyV(properties,nProperties,props,pma))!=RC_OK) return rc;
	}
	return RC_OK;
}

RC PIN::isMemberOf(DataEventID *&devs,unsigned& ndevs)
{
	try {
		devs=NULL; ndevs=0; Session *ses=Session::getSession();
		if ((mode&PIN_HIDDEN)==0 && ses!=NULL) {
			DetectedEvents clr(ses,ses->getStore());
			RC rc=ses->getStore()->classMgr->detect(this,clr,ses); if (rc!=RC_OK) return rc;
			if (clr.ndevs!=0) {
				if ((devs=new(ses) DataEventID[clr.ndevs])==NULL) return RC_NOMEM;
				for (unsigned i=0; i<clr.ndevs; i++) devs[i]=clr.devs[i]->cid;
				ndevs=clr.ndevs;
			}
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::isMemberOf()\n"); return RC_INTERNAL;}
}

PIN *PIN::getPIN(const PID& id,VersionID vid,Session *ses,unsigned md)
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL||ses->getStore()->inShutdown()) return NULL;
		TxGuard txg(ses); bool fRemote=isRemote(id); PageAddr addr=PageAddr::noAddr;
		if (!fRemote && (md&LOAD_EXT_ADDR)!=0 && addr.convert(uint64_t(id.pid))) ses->setExtAddr(addr);
		PIN *pin=NULL; PINx cb(ses,id); if (cb.loadPIN(pin,md|LOAD_CLIENT,vid)!=RC_OK) {delete pin; pin=NULL;}
		ses->setExtAddr(PageAddr::noAddr); return pin;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::getPIN(PID=" _LX_FM ",IdentityID=%08X)\n",id.pid,id.ident);}
	return NULL;
}

RC PIN::checkSet(const Value *&pv,const Value& w)
{
	assert(properties!=NULL && nProperties!=0);
	PropertyID pid=pv->property; uint8_t meta=pv->meta,op=pv->op;
	if (fNoFree==0) freeV(*(Value*)pv);
	else {
		ptrdiff_t sht=pv-properties;
		Value *pp=new(ma) Value[nProperties]; if (pp==NULL) return RC_NOMEM;
		memcpy(pp,properties,nProperties*sizeof(Value));
		for (unsigned i=0; i<nProperties; i++) setHT(pp[i]);
		properties=pp; fNoFree=0; pv=pp+sht;
	}
	if (!w.isEmpty()) {*(Value*)pv=w; ((Value*)pv)->property=pid; ((Value*)pv)->meta=meta; ((Value*)pv)->op=op;}
	else if (--nProperties==0) {ma->free((Value*)properties); properties=NULL;}
	else if (pv<&properties[nProperties]) memmove((Value*)pv,pv+1,(byte*)&properties[nProperties]-(byte*)pv);
	return RC_OK;
}

size_t PIN::serSize() const
{
	size_t l=afy_len32(nProperties);
	for (unsigned i=0; i<nProperties; i++) l+=afy_len32(properties[i].property)+1+AfyKernel::serSize(properties[i]);
	return l;
}

byte *PIN::serialize(byte *buf) const
{
	afy_enc32(buf,nProperties);
	for (unsigned i=0; i<nProperties; i++) {afy_enc32(buf,properties[i].property); *buf++=properties[i].meta; buf=AfyKernel::serialize(properties[i],buf);}
	return buf;
}

RC PIN::deserialize(PIN *&pin,const byte *&buf,const byte *const ebuf,MemAlloc *ma,bool fInPlace)
{
	Value *props=NULL; uint32_t nProps; CHECK_dec32(buf,nProps,ebuf); 
	if (nProps!=0) {
		if ((props=(Value*)ma->malloc(sizeof(Value)*nProps))==NULL) return RC_NOMEM;
		for (unsigned i=0; i<nProps; i++) {
			uint32_t propID; CHECK_dec32(buf,propID,ebuf);
			if (buf>=ebuf) return RC_CORRUPTED; uint8_t meta=*buf++; RC rc;
			if ((rc=AfyKernel::deserialize(props[i],buf,ebuf,ma,fInPlace))!=RC_OK) return rc;
			props[i].property=propID; props[i].meta=meta;
		}
	}
	if ((pin=new(ma) PIN(ma,0,props,nProps))==NULL) {freeV(props,nProps,ma); return RC_NOMEM;}
	return RC_OK;
}

IMemAlloc *PIN::getAlloc() const
{
	try {return ma;} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::getAlloc()\n");}
	return NULL;
}

RC PIN::allocSubPIN(unsigned nProps,IPIN *&pin,Value *&values,unsigned mode) const
{
	try {
		if (ma==NULL) return RC_INVOP;
		//...
		return RC_INTERNAL; // tmp
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::allocSubPIN()\n"); return RC_INTERNAL;}
}

void PIN::destroy()
{
	try {this->~PIN(); if (ma!=NULL) ma->free(this);} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::destroy()\n");}
}

//-----------------------------------------------------------------------------

void PINx::releaseLatches(PageID pid,PageMgr *mgr,bool fX)
{
	if (!pb.isNull() && (pid==INVALID_PAGEID || fX && pid==pb->getPageID() || mgr!=NULL && mgr->getPGID()==PGID_HEAP && pid<pb->getPageID())) {
		if (fReload!=0 && fPartial!=0 && hpin!=NULL) loadProps(0/*,flt,nFlt*/);	// LOAD_SSV ??? if failed?
		hpin=NULL; pb.release(ses);
	}
}

void PINx::checkNotHeld(PBlock *p)
{
	assert(pb.isNull() || pb.isSet(PGCTL_INREL) || (PBlock*)pb!=p);
}

void PINx::moveTo(PINx& cb)
{
	cb.id=id; cb.addr=addr; cb.properties=properties; cb.nProperties=nProperties; cb.mode=mode; cb.meta=meta; pb.moveTo(cb.pb); cb.hpin=hpin; cb.epr=epr; cb.tv=tv;
	id=PIN::noPID; addr=PageAddr::noAddr; properties=NULL; nProperties=0; mode=0; meta=0; hpin=NULL; tv=NULL; epr.buf[0]=0; epr.flags=0;
}

bool PINx::defined(const PropertyID *pids,unsigned nP) const
{
	if (pids!=NULL && nP!=0) {
		if (properties==NULL && hpin==NULL) return false;
		for (unsigned i=0; i<nP; i++) {
			const PropertyID pid=pids[i]&STORE_MAX_URIID;
			if (pid==PROP_SPEC_PINID) {if (!id.isPID()) return false;}
			else if (VBIN::find(pid,properties,nProperties)==NULL && (fPartial==0 || hpin==NULL || hpin->findProperty(pid)==NULL)) return false;
		}
	}
	return true;
}

bool PINx::checkProps(const PropertyID *pids,unsigned nP)
{
	if (pids!=NULL && nP!=0 && properties==NULL && hpin==NULL && (fPartial==0 || getBody()!=RC_OK)) return false;
	return PINx::defined(pids,nP);
}

bool PINx::isCollection(PropertyID pid) const
{
	if (properties!=NULL) {
		const Value *cv=VBIN::find(pid,properties,nProperties);
		return cv!=NULL && cv->type==VT_COLLECTION;
	}
	if (hpin!=NULL) {
		const HeapPageMgr::HeapV *hprop=hpin->findProperty(pid);
		return hprop!=NULL && hprop->type.isCollection();
	}
	return false;
}

RC PINx::getVx(PropertyID pid,Value& v,unsigned md,MemAlloc *ma,ElementID eid)
{
	RC rc=RC_NOTFOUND; assert(fPartial!=0);
	if (pid!=PROP_SPEC_PINID) {
		if (!pb.isNull() || (rc=getBody())==RC_OK)
			{const HeapPageMgr::HeapV *hprop=hpin->findProperty(pid); if (hprop!=NULL) return loadVH(v,hprop,md,ma,eid);}
	} else if (!id.isEmpty() || (rc=unpack())==RC_OK) {
		if ((md&LOAD_CARDINALITY)!=0) v.set(1u); else v.set(id);
		return RC_OK;
	}
	if ((md&LOAD_CARDINALITY)!=0) {v.set(0u); return RC_OK;} else {v.setError(pid); return rc;}
}

RC PINx::getVx(PropertyID pid,Value& v,unsigned md,const Value& idx,MemAlloc *ma)
{
	RC rc=RC_NOTFOUND; assert(fPartial!=0);
	if (!pb.isNull() || (rc=getBody())==RC_OK) {
		const HeapPageMgr::HeapV *hprop=hpin->findProperty(pid); 
		if (hprop!=NULL) switch (hprop->type.getType()) {
		case VT_MAP: return loadMapElt(v,hprop,md,idx,ma);
		case VT_COLLECTION: if (idx.isEmpty()) return loadVH(v,hprop,md,ma,idx.eid);
		default:
			if (idx.type==VT_UINT||idx.type==VT_INT&&idx.i>=0) return loadVH(v,hprop,md,ma,(ElementID)idx.ui);
			if (idx.isEmpty()) return loadVH(v,hprop,md,ma,STORE_COLLECTION_ID);
			break;
		}
	}
	if ((md&LOAD_CARDINALITY)!=0) {v.set(0u); return RC_OK;} else {v.setError(pid); return rc;}
}

RC PINx::unpack() const
{
	try {
		PINRef pr(ses->getStore()->storeID,epr.buf);
		if ((pr.def&PR_ADDR)!=0) {addr=pr.addr; epr.flags|=PINEX_ADDRSET;}
		const_cast<PID&>(id)=pr.id; return RC_OK;
	} catch (RC rc) {return rc;}
}

RC PINx::pack() const
{
	if (epr.buf[0]==0) {
		if (!id.isPID()) return RC_NOTFOUND;
		PINRef pr(ses->getStore()->storeID,id);
		if ((meta&PMT_COMM)!=0) pr.def|=PR_SPECIAL;
		if ((mode&PIN_HIDDEN)!=0) pr.def|=PR_HIDDEN;
		if (addr.defined()) {pr.def|=PR_ADDR; pr.addr=addr;}
		pr.enc(epr.buf);
	}
	return RC_OK;
}

void PINx::copyFlags()
{
	assert(!pb.isNull() && hpin!=NULL);
	mode&=~(MODE_DELETED|PIN_DELETED); meta=hpin->meta; const uint16_t dscr=hpin->hdr.descr; mode|=PIN_PERSISTENT;
	if (isRemote(id)) {
		if ((dscr&HOH_REPLICATED)==0) mode|=PIN_IMMUTABLE; else mode=mode&~PIN_IMMUTABLE|PIN_REPLICATED;
	} else if ((dscr&HOH_NOREPLICATION)!=0) mode|=PIN_NO_REPLICATION;
	else if ((dscr&HOH_REPLICATED)!=0) mode|=PIN_REPLICATED;
	if ((dscr&HOH_HIDDEN)!=0) mode|=PIN_HIDDEN;
	if ((dscr&HOH_DELETED)!=0) mode|=PIN_DELETED;
	if (hpin->nProps!=0) fPartial=1;		// cb.tv ????
}

uint32_t PINx::estimateSize() const
{
	assert(hpin!=NULL);
	uint32_t l=sizeof(PIN)+hpin->nProps*sizeof(Value),extra=hpin->hdr.getLength()-hpin->nProps*sizeof(HeapPageMgr::HeapV)-sizeof(HeapPageMgr::HeapPIN);
	l+=(hpin->hdr.descr&HOH_COMPOUND)!=0?extra*3:extra;
	if ((hpin->hdr.descr&HOH_SSVS)!=0) {
		l=~0u;	//???
	}
	return l;
}

RC PINx::getBody(TVOp tvo,unsigned flags,VersionID vid)
{
	RC rc; PageAddr extAddr; StoreCtx *ctx=ses->getStore();
	bool fRemote=false,fTry=true,fWrite=tvo!=TVO_READ; epr.flags&=~PINEX_ADDRSET;
	if (id.isEmpty() && (rc=unpack())!=RC_OK) return rc;
	if (!addr.defined()) {fTry=false; if (isRemote(id)) fRemote=true; else if (!addr.convert(id.pid)) return RC_CORRUPTED;}
	if ((epr.flags&PINEX_EXTPID)!=0 && extAddr.convert(uint64_t(id.pid))) ses->setExtAddr(extAddr);
	for (unsigned fctl=PGCTL_RLATCH|(fWrite?PGCTL_ULOCK:0);;) {
		if (!fRemote) pb.getPage(addr.pageID,ctx->heapMgr,fctl,ses);
		else if ((rc=ctx->netMgr->getPage(id,fctl,addr.idx,pb,ses))!=RC_OK) return rc;
		else {fRemote=fTry=false; addr.pageID=pb->getPageID();}

		if (pb.isNull()) return RC_NOTFOUND;
		if (fWrite && pb->isULocked()) {fctl|=QMGR_UFORCE; pb.set(QMGR_UFORCE);}
		
		if (fill()==NULL) {
			if (ctx->lockMgr->getTVers(*this,tvo)==RC_OK && tv!=NULL) {
				// deleted in uncommitted tx?
			}
		} else if (hpin->hdr.getType()==HO_FORWARD) {
			if ((flags&GB_FORWARD)!=0) {if ((epr.flags&PINEX_EXTPID)!=0) ses->setExtAddr(PageAddr::noAddr); return RC_OK;}
			memcpy(&addr,(const byte*)hpin+sizeof(HeapPageMgr::HeapObjHeader),PageAddrSize); continue;
		} else {
			PID id; IdentityID iid;
			if ((epr.flags&PINEX_EXTPID)!=0) ses->setExtAddr(PageAddr::noAddr);
			if (hpin->hdr.getType()!=HO_PIN) return RC_CORRUPTED;
			if (!hpin->getAddr(id)) {id.pid=uint64_t(addr); id.ident=STORE_OWNER;}
			if (id==id) {
				epr.flags|=PINEX_ADDRSET; rc=RC_OK;
				if (isRemote(id)) {
					//???
				} else if ((rc=ctx->lockMgr->getTVers(*this,tvo))!=RC_OK) return rc;
				if (!ses->inWriteTx()) {
					if (tvo!=TVO_READ) return RC_READTX;
					if (tv!=NULL) {
						// check hpin->hdr.dscr
						//...
						//flags|=PINEX_TVERSION;
					}
				} else if ((flags&GB_REREAD)==0) {
					const unsigned lck=tvo==TVO_READ?PINEX_LOCKED:PINEX_XLOCKED;
					if ((epr.flags&lck)==0) {
						if ((rc=ctx->lockMgr->lock(tvo==TVO_READ?LOCK_SHARED:LOCK_EXCLUSIVE,*this))!=RC_OK) {pb.release(ses); return rc;}
						epr.flags|=lck|PINEX_LOCKED; if (pb.isNull()) continue;
					}
				}
				copyFlags();
				if ((flags&GB_DELETED)==0 && (hpin->hdr.descr&HOH_DELETED)!=0)
					return RC_DELETED;
				if (vid!=STORE_CURRENT_VERSION) {
					//???continue???
				}
				if ((flags&GB_REREAD)==0 && (iid=ses->getIdentity())!=STORE_OWNER) rc=checkACLs(iid,tvo,flags);
				if (rc!=RC_OK) {pb.release(ses); return rc;} if (pb.isNull()) continue;
				return RC_OK;
			}
		}
		if (!fTry) {
			if (hpin!=NULL && (epr.flags&PINEX_EXTPID)==0) 
				report(MSG_ERROR,"getBody: page %08X corruption\n",pb->getPageID());
			return RC_NOTFOUND;
		}
		if (isRemote(id)) fRemote=true; else if (!addr.convert(id.pid)) return RC_CORRUPTED;
		fTry=false;
	}
}

RC PINx::checkLockAndACL(TVOp tvo,QueryOp *qop)
{
	if ((epr.flags&(PINEX_DERIVED|PINEX_COMM))!=0) return RC_OK; if (ses==NULL) return RC_NOSESSION;
	RC rc=RC_OK; IdentityID iid; bool fWasNull=pb.isNull();
	if ((epr.flags&PINEX_ADDRSET)==0) addr=PageAddr::noAddr;
	if ((mode&PIN_DELETED)==0 && ses->inWriteTx() && (epr.flags&(tvo!=TVO_READ?PINEX_XLOCKED:PINEX_LOCKED))==0) {
		if (id.isEmpty() && (rc=unpack())!=RC_OK) return rc;
		if ((rc=ses->getStore()->lockMgr->lock(tvo!=TVO_READ?LOCK_EXCLUSIVE:LOCK_SHARED,*this))==RC_OK) {
			epr.flags|=tvo!=TVO_READ?PINEX_XLOCKED|PINEX_LOCKED:PINEX_LOCKED;
			if (!fWasNull && pb.isNull()) rc=getBody(tvo,GB_REREAD);	//???
		}
	}
	if (rc==RC_OK && (epr.flags&PINEX_ACL_CHKED)==0 && (iid=ses->getIdentity())!=STORE_OWNER &&
		(!pb.isNull() || (rc=getBody(tvo,GB_REREAD))==RC_OK) &&
		((rc=checkACLs(iid,tvo))==RC_OK||rc==RC_NOACCESS)) epr.flags|=PINEX_ACL_CHKED;
	return rc;
}

RC PINx::checkACLs(IdentityID iid,TVOp tvo,unsigned flags,bool fProp)
{
	assert(!pb.isNull()); const bool fWrite=tvo!=TVO_READ;
	Value v; RC rc=RC_NOACCESS; RefTrace rt={NULL,id,PROP_SPEC_ACL,STORE_COLLECTION_ID};
	if (getV(PROP_SPEC_ACL,v,0,ses)==RC_OK) {
		rc=checkACL(v,iid,fWrite?META_PROP_WRITE:META_PROP_READ,&rt,fProp); freeV(v);
	} else if (getV(PROP_SPEC_DOCUMENT,v,0,ses)==RC_OK) {
		if (v.type==VT_REFID) {
			if (!fProp) rc=RC_FALSE;
			else {
				RefVID ref={v.id,PROP_SPEC_ACL,STORE_COLLECTION_ID,STORE_CURRENT_VERSION}; 
				v.set(ref); rt.id=v.id; rc=checkACL(v,iid,fWrite?META_PROP_WRITE:META_PROP_READ,&rt);
			}
		}
		freeV(v);
	}
	return rc==RC_REPEAT?getBody(tvo,GB_REREAD):rc;
}

RC PINx::checkACLs(PINx *pin,IdentityID iid,TVOp tvo,unsigned flags,bool fProp)
{
	RC rc=RC_NOACCESS; const bool fWrite=tvo!=TVO_READ;
	RefTrace rt={NULL,pin->id,PROP_SPEC_ACL,STORE_COLLECTION_ID}; Value v; v.setError();
	if (pin->getV(PROP_SPEC_ACL,v,LOAD_SSV,pin->ses)==RC_OK) rc=checkACL(v,iid,fWrite?META_PROP_WRITE:META_PROP_READ,&rt,fProp);
	else if (pin->getV(PROP_SPEC_DOCUMENT,v,LOAD_SSV,pin->ses)==RC_OK && v.type==VT_REFID) {
		if (!fProp) rc=RC_FALSE;
		else {
			RefVID ref={v.id,PROP_SPEC_ACL,STORE_COLLECTION_ID,STORE_CURRENT_VERSION}; 
			Value v; v.set(ref); rt.id=v.id; rc=checkACL(v,iid,fWrite?META_PROP_WRITE:META_PROP_READ,&rt);
		}
	}
	id=pin->id; freeV(v);
	return rc==RC_REPEAT?getBody(tvo,flags|GB_REREAD):rc;
}

RC PINx::checkACL(const Value& v,IdentityID iid,uint8_t mask,const RefTrace *rt,bool fProp)
{
	unsigned i; const Value *cv; Value pv,*ppv; RC rc=RC_OK; RefTrace refTrc; bool fFalse=false;
	switch (v.type) {
	default: break;
	case VT_IDENTITY: return v.iid==iid && (v.meta&mask)!=0 ? RC_OK : RC_NOACCESS;
	case VT_REFIDPROP: case VT_REFIDELT:
		if (!fProp) return RC_FALSE;
		for (refTrc.next=rt; rt!=NULL; rt=rt->next) if (rt->id==v.refId->id && rt->pid==v.refId->pid)
			if (rt->eid==STORE_COLLECTION_ID || v.type==VT_REFIDELT && rt->eid==v.refId->eid) return RC_NOACCESS;
		refTrc.pid=pv.property=v.refId->pid; refTrc.eid=pv.eid=v.type==VT_REFIDELT?v.refId->eid:STORE_COLLECTION_ID;
		refTrc.id=v.refId->id; ppv=&pv; i=1; rc=getRefSafe(v.refId->id,ppv,i,0);
		if (rc==RC_OK || rc==RC_REPEAT) {RC rc2=checkACL(pv,iid,mask,&refTrc); freeV(pv); return rc2==RC_OK?rc:rc2;}
		break;
	case VT_COLLECTION:
		if (v.isNav()) {
			for (cv=v.nav->navigate(GO_FIRST); cv!=NULL; cv=v.nav->navigate(GO_NEXT))
				if ((rc=checkACL(*cv,iid,mask,rt,fProp))==RC_OK) break; else if (rc==RC_FALSE) fFalse=true;
			v.nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID); if (rc==RC_OK||rc!=RC_NOACCESS&&!fFalse) return rc;
		} else {
			for (i=0; i<v.length; i++) if ((rc=checkACL(v.varray[i],iid,mask,rt,fProp))==RC_OK) return rc; else if (rc==RC_FALSE) fFalse=true;
		}
		break;
	case VT_STRUCT:
		//???
		break;
	}
	return fFalse?RC_FALSE:RC_NOACCESS;
}

RC PINx::getRefSafe(const PID& id,Value *&vals,unsigned& nValues,unsigned flags)
{
	if (pb.isNull()) return RC_INVPARAM; const bool fU=pb->isULocked();
	const PageID pid=pb->getPageID(); PINx cb(ses); RC rc=RC_OK; StoreCtx *ctx=ses->getStore();
	if (isRemote(id)) {
		if (ctx->netMgr->getPage(id,QMGR_TRY,cb.addr.idx,cb.pb,cb.ses)!=RC_OK) {
			pb.moveTo(cb.pb); 
			if ((rc=ctx->netMgr->getPage(id,fU?QMGR_UFORCE:0,cb.addr.idx,cb.pb,cb.ses))==RC_OK) rc=RC_REPEAT;
		}
	} else if (!cb.addr.convert(id.pid)) return RC_CORRUPTED;
	else if (cb.pb.getPage(cb.addr.pageID,ctx->heapMgr,QMGR_TRY,cb.ses)==NULL) {
		rc=pb.getPage(cb.addr.pageID,ctx->heapMgr,PGCTL_RLATCH,ses)==NULL?RC_CORRUPTED:RC_REPEAT; pb.moveTo(cb.pb);
	}
	if (rc==RC_OK||rc==RC_REPEAT) while (!cb.pb.isNull()) {
		if (cb.fill()==NULL) {rc=RC_NOTFOUND; break;}
		if (cb.hpin->hdr.getType()==HO_FORWARD) {
			memcpy(&cb.addr,(const byte*)cb.hpin+sizeof(HeapPageMgr::HeapObjHeader),PageAddrSize);
			if (cb.pb.getPage(cb.addr.pageID,ctx->heapMgr,QMGR_TRY,cb.ses)==NULL) {
				if (!pb.isNull()) pb.moveTo(cb.pb);
				rc=cb.pb.getPage(cb.addr.pageID,ctx->heapMgr,PGCTL_RLATCH,cb.ses)==NULL?RC_CORRUPTED:RC_REPEAT;
			}
			continue;
		}
		if (cb.hpin->hdr.getType()!=HO_PIN||(cb.hpin->hdr.descr&HOH_DELETED)!=0) {rc=RC_NOTFOUND; break;}	// tv???
		if (vals==NULL && (nValues=cb.hpin->nProps)>0) {
			if ((vals=(Value*)ses->malloc(sizeof(Value)*nValues))==NULL) {rc=RC_NOMEM; break;}
			const HeapPageMgr::HeapV *hprop=cb.hpin->getPropTab();
			for (unsigned i=0; i<nValues; ++hprop,++i) vals[i].property=hprop->getID();
		}
		if (vals!=NULL) for (unsigned i=0; i<nValues; i++)
			if ((rc=cb.getV(vals[i].property,vals[i],flags,ses,vals[i].eid))!=RC_OK) break;
		break;
	}
	if (pb.isNull()) {cb.pb.getPage(pid,ctx->heapMgr,fU?PGCTL_ULOCK:0,cb.ses); cb.pb.moveTo(pb);}
	return rc;
}

RC PINx::load(unsigned md,const PropertyID *flt,unsigned nFlt)
{
	if (id.isEmpty()) {RC rc=unpack(); if (rc!=RC_OK) return rc;}
	if (fPartial==0) return RC_OK; // check LOAD_CLIENT, LOAD_SSV/PIN_SSV, MODE_FORCED_SSV_AS_STREAM
	if (properties!=NULL && flt!=NULL && nProperties>=nFlt) for (unsigned i=0,j=0; i<nProperties; i++)
		{int c=cmp3(properties[i].property,flt[j]&STORE_MAX_URIID); if (c>0) break; if (c==0 && ++j>=nFlt) return RC_OK;}
	if (fNoFree==0) freeV(properties,nProperties,ma); properties=NULL; nProperties=0; fNoFree=0;
	if (hpin==NULL) {
		if (!id.isPID()) return RC_NOTFOUND;
		RC rc=getBody(TVO_READ,(md&MODE_DELETED)!=0?GB_DELETED:0);
		if (rc!=RC_OK) {if (rc==RC_DELETED) mode|=PIN_DELETED; return rc;}
	}
	return loadProps(md,flt,nFlt);
}

const Value *PINx::loadProperty(PropertyID pid)
{
	Value v,*pv; assert(fPartial!=0);
	if (getV(pid,v,LOAD_SSV,ma)!=RC_OK) return NULL;
	if (VBIN::insert(properties,nProperties,pid,v,ma,NULL,&pv)!=RC_OK) {freeV(v); return NULL;}
	return pv;
}

const void *PINx::getPropTab(unsigned& nProps) const
{
	if (hpin==NULL) {nProps=0; return NULL;}
	nProps=hpin->nProps; return hpin->getPropTab();
}

void PINx::copy(const PIN *pin,unsigned flags)
{
	if (pin->id.isPID()||pin->id.isPtr()) id=pin->id; else id.setPtr((PIN*)pin);
	addr=pin->addr; properties=pin->properties; nProperties=pin->nProperties; mode=pin->mode; fNoFree=1; fPartial=0; meta=pin->meta; epr.buf[0]=0;
	if (!id.isPID() && (pin->fPINx==0 || (((PINx*)pin)->epr.flags&(PINEX_DERIVED|PINEX_COMM))!=0)) epr.flags=PINEX_DERIVED;
	else if (pin->fPINx!=0) {
		const PINx *px=(const PINx *)pin; fPartial=px->fPartial; epr=px->epr; tv=px->tv; if (id.isEmpty() && epr.buf[0]!=0) unpack();
		if (!px->pb.isNull() && pb.getPage(px->pb->getPageID(),px->pb->getPageMgr(),flags,ses)!=NULL) fill();
	}
}

void PINx::free()
{
	if (properties!=NULL && fNoFree==0) freeV((Value*)properties,nProperties,ma);
	properties=NULL; nProperties=0; fPartial=1; fNoFree=fSSV=fReload=0; mode=0; meta=0;
	//if (tv!=NULL) ???
}

//------------------------------------------------------------------------------------------------

static int __cdecl cmpValues(const void *v1, const void *v2)
{
	const Value *pv1=(const Value*)v1,*pv2=(const Value*)v2; int c=cmp3(pv1->property,pv2->property);
	return c!=0?c:pv1->eid==STORE_FIRST_ELEMENT&&pv1<pv2?-1:pv2->eid==STORE_FIRST_ELEMENT?1:
				pv2->eid==STORE_LAST_ELEMENT&&pv2>pv1?-1:pv1->eid==STORE_LAST_ELEMENT?1:pv1<pv2?-1:1;
}

RC Session::normalize(const Value *&pv,uint32_t& nv,unsigned f,ElementID prefix,MemAlloc *ma,bool fNF)
{
	if (pv==NULL || nv==0) return RC_OK; if (ma==NULL) ma=this;
	bool fCV=(f&MODE_COPY_VALUES)!=0,fEID=(f&MODE_FORCE_EIDS)==0;
	Value *pn; unsigned sort=0; HEAP_TYPE ht=ma->getAType(); HEAP_TYPE flags=fCV?NO_HEAP:ht;
	TIMESTAMP ts=0ULL; RC rc; unsigned nBI=0;

	for (unsigned j=0; j<nv; j++) {
		Value &v=*(Value*)&pv[j]; setHT(v,flags);
		if (fCV&&v.type!=VT_VARREF) if (fEID||v.eid==STORE_COLLECTION_ID) v.eid=prefix; else if (v.eid>=STORE_ALL_ELEMENTS) return RC_INVPARAM;
		if (j!=0&&pv[j-1].property>=v.property) sort|=pv[j-1].property==v.property?1:2;
		if ((f&MODE_COMPOUND)==0 && v.property<=MAX_BUILTIN_URIID) nBI++;
		if (v.type==VT_COLLECTION && !v.isNav()) {
			for (uint32_t k=0; k<v.length; k++) {
				Value &vv=*(Value*)&v.varray[k]; setHT(vv,flags);
				if (fEID && vv.type!=VT_VARREF) {
					vv.eid=prefix+k; if (!fCV && vv.type==VT_STRUCT && (rc=normalize(vv.varray,vv.length,f|MODE_COMPOUND,prefix,ma))!=RC_OK) return rc;
				}
			}
			v.eid=STORE_COLLECTION_ID;
		} else if (!fCV && v.type==VT_STRUCT && (rc=normalize(v.varray,v.length,f|MODE_COMPOUND,prefix,ma))!=RC_OK) return rc;
	}

	if (!fCV) pn=(Value*)pv;
	else if ((pn=new(ma) Value[nv])==NULL) return RC_NOMEM;
	else {memcpy(pn,pv,nv*sizeof(Value)); pv=pn;}
	if (fCV || sort!=0) {
		if ((sort&2)!=0) qsort(pn,nv,sizeof(Value),cmpValues);
		if (fCV) {
			if (!fNF) {
				rc=pn[0].type!=VT_STRUCT?copyV(pn[0],pn[0],ma):normalize(pn[0].varray,pn[0].length,f|MODE_COMPOUND,prefix,ma);
				if (rc!=RC_OK) {ma->free(pn); return rc;}
			}
			if (pn[0].type!=VT_VARREF) if (fEID||pn[0].eid==STORE_COLLECTION_ID) pn[0].eid=prefix; else if (pn[0].eid>=STORE_ALL_ELEMENTS) return RC_INVPARAM;
		}
		for (unsigned i=1; i<nv; ) {
			Value *pv1=&pn[i-1],*pv2=&pn[i];
			if (fCV) {
				if (!fNF) {
					rc=pv2->type!=VT_STRUCT?copyV(*pv2,*pv2,ma):normalize(pv2->varray,pv2->length,f|MODE_COMPOUND,prefix,ma);
					if (rc!=RC_OK) {while (i!=0) freeV(pn[--i]); ma->free(pn); return rc;}
				}
				if ((fEID||pv2->eid==STORE_COLLECTION_ID) && pv2->type!=VT_VARREF) pv2->eid=prefix;
			}
			if (pv1->property<pv2->property) {
				if ((f&MODE_COMPOUND)==0 && pv1->property<=MAX_BUILTIN_URIID && (rc=checkBuiltinProp(*pv1,ts,true))!=RC_OK) return rc;
				i++;
			} else if (pv1->type==VT_COLLECTION && pv1->isNav() || pv2->type==VT_COLLECTION && pv2->isNav()) {
				// ???
				i++;
			} else {
				unsigned l1=pv1->type==VT_COLLECTION?pv1->length:1,l2=0,ii=0; assert(pv1->property==pv2->property);
				do l2+=pv2[ii].type==VT_COLLECTION?pv2[ii].length:1;
				while (++ii+i<nv && pv2[ii].property==pv1->property);
				Value *vals=pv1->type!=VT_COLLECTION||fCV?(Value*)ma->malloc((l1+l2)*sizeof(Value)):
					(Value*)ma->realloc(const_cast<Value*>(pv1->varray),(l1+l2)*sizeof(Value),l1*sizeof(Value));
				if (pv1->type!=VT_COLLECTION) {vals[0]=*pv1; vals[0].property=STORE_INVALID_URIID;}
				else if (fCV) memcpy(vals,pv1->varray,l1*sizeof(Value));
				pv1->varray=vals; pv1->length=l1+l2; pv1->type=VT_COLLECTION; pv1->eid=STORE_COLLECTION_ID;
				setHT(*pv1,ht); vals+=l1; unsigned ll; ElementID eid=prefix+l1;
				do {
					ll=pv2->type==VT_COLLECTION?pv2->length:1; pv1->meta|=pv2->meta;
					if (!fCV || fNF) {
						memcpy(vals,pv2->type==VT_COLLECTION?pv2->varray:pv2,ll*sizeof(Value));
						for (unsigned k=0; k<ll; k++) if (vals[k].type!=VT_VARREF)
							if (fEID||vals[k].eid==STORE_COLLECTION_ID) vals[k].eid=eid++; else if (vals[k].eid>=STORE_ALL_ELEMENTS) return RC_INVPARAM;
						if (!fCV && pv2->type==VT_COLLECTION && (pv2->flags&HEAP_TYPE_MASK)>=SES_HEAP) 
							{AfyKernel::free(const_cast<Value*>(pv2->varray),(HEAP_TYPE)(pv2->flags&HEAP_TYPE_MASK)); setHT(*pv2);}
					} else for (unsigned j=0; j<ll; j++) if ((rc=copyV(pv2->type==VT_COLLECTION?pv2->varray[j]:*pv2,vals[j],ma))!=RC_OK) {
						//...free
						return rc;
					} else if (vals[j].type!=VT_VARREF) {if (fEID||vals[j].eid==STORE_COLLECTION_ID) vals[j].eid=eid++; else if (vals[j].eid>=STORE_ALL_ELEMENTS) return RC_INVPARAM;}
					pv2++; vals+=ll; --nv; assert(nv>=i);
				} while ((l2-=ll)!=0);
				if (nv>i && ii>0) memmove(pv1+1,pv1+ii+1,(nv-i)*sizeof(Value));
			}
		}
		if ((f&MODE_COMPOUND)==0 && pn[nv-1].property<=MAX_BUILTIN_URIID && (rc=checkBuiltinProp(pn[nv-1],ts,true))!=RC_OK) return rc;
	} else if (nBI!=0) for (unsigned i=0; i<nv; i++) if ((pn=(Value*)&pv[i])->property<=MAX_BUILTIN_URIID) {
		if ((rc=checkBuiltinProp(*pn,ts,true))!=RC_OK) return rc; if (--nBI==0) break;
	}
	return RC_OK;
}

RC Session::createPIN(Value *values,unsigned nValues,IPIN **result,unsigned md,const PID *orig)
{
	try {
		if (result!=NULL) *result=NULL; if (ctx->inShutdown()) return RC_SHUTDOWN;
		unsigned pmd=md; RC rc=newPINFlags(pmd,orig); if (rc!=RC_OK) return rc;
		PIN *pin=NULL;	//active!=NULL?active->createPIN(values,nValues,pmd,orig):NULL;
		if ((pin=new(this) PIN(this,pmd))==NULL) rc=RC_NOMEM; else if (orig!=NULL) *pin=*orig;
		if (values!=NULL && nValues!=0) {
			if (pin!=NULL) {
				uint32_t nv=(uint32_t)nValues; const Value *pv=values;
				if ((rc=normalize(pv,nv,md,orig!=NULL?ctx->genPrefix(ushort(orig->pid>>48)):ctx->getPrefix(),this))==RC_OK) {pin->properties=(Value*)pv; pin->nProperties=nv;}
			} else if ((md&MODE_COPY_VALUES)==0) {
				for (unsigned i=0; i<nValues; i++) {
					if (values[i].type==VT_COLLECTION && !values[i].isNav()) for (unsigned j=0; j<values[i].length; j++) setHT(values[i].varray[i],SES_HEAP);
					// struct ???
					setHT(values[i],SES_HEAP); freeV(values[i]);
				}
				free(values);
			}
		}
		if (pin!=NULL) {
			if (rc==RC_OK && (md&MODE_PERSISTENT)!=0)
				{if (!ctx->isServerLocked()) {TxGuard txg(this); rc=ctx->queryMgr->persistPINs(EvalCtx(this,ECT_INSERT),&pin,1,md);} else rc=RC_READONLY;}
			if (rc==RC_OK && result!=NULL) *result=pin; else pin->destroy();
		}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::createPIN(...)\n"); return RC_INTERNAL;}
}

RC Session::allocPIN(size_t maxSize,unsigned nProps,IPIN *&pin,Value *&values,unsigned mode)
{
	try {
		pin=NULL; values=NULL;
		maxSize=sizeof(PIN)+max(maxSize,nProps*sizeof(Value)); if (maxSize>0x10000) return RC_TOOBIG;
		BlockAlloc *ba=BlockAlloc::allocBlock(maxSize,this); if (ba==NULL) return RC_NOMEM;
		PIN *p=new(ba) PIN(ba,mode,NULL,0,true); assert(p!=NULL);
		p->properties=new(ba) Value[nProps]; p->nProperties=nProps; assert(p->properties!=NULL);
		for (unsigned i=0; i<nProps; i++) {p->properties[i].type=VT_ANY; p->properties[i].flags=NO_HEAP;}
		pin=p; values=p->properties; return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::allocPIN(...)\n"); return RC_INTERNAL;}
}

RC Session::inject(IPIN *ip)
{
	try {
		// ???
		return RC_INTERNAL;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::inject(...)\n"); return RC_INTERNAL;}
}

RC Session::checkBuiltinProp(Value &v,TIMESTAMP &ts,bool fInsert)
{
	if (fInsert) {
		if (v.op!=OP_SET && v.op!=OP_ADD) return RC_INVOP;
	} else if (v.op==OP_DELETE) {
		return v.property==PROP_SPEC_PINID||v.property==PROP_SPEC_SELF?RC_INVPARAM:RC_OK;
	}
	Value w; RC rc; const Value *cv; Stmt *qry; const bool fE=isEval(v),fE1=fE&&!isComposite((ValueType)v.type);
	switch (v.property) {
	default: if (v.property>MAX_BUILTIN_URIID) return RC_INVPARAM; break;
	case PROP_SPEC_SPECIALIZATION:
	case PROP_SPEC_ABSTRACTION:
	case PROP_SPEC_INDEX_INFO:
	case PROP_SPEC_PROPERTIES:
	case PROP_SPEC_VERSION:
	case PROP_SPEC_PINID: return RC_INVPARAM;
	case PROP_SPEC_SELF: if (fInsert) return RC_INVPARAM; break;
	case PROP_SPEC_STAMP:
		if (!fInsert) return RC_INVPARAM;
		if ((sFlags&S_REPLICATION)==0) {v.type=VT_UINT; v.length=sizeof(uint32_t); v.ui=0;} else if (v.type!=VT_UINT && v.type!=VT_UINT64) return RC_TYPE;
		break;
	case PROP_SPEC_DOCUMENT:
		if (v.op<=OP_ADD_BEFORE && !fE && v.type!=VT_REF && v.type!=VT_REFID) return RC_TYPE;
		break;
	case PROP_SPEC_PARENT:
		if (v.op<=OP_ADD_BEFORE && !fE && v.type!=VT_REF && v.type!=VT_REFID) return RC_TYPE;
		//check mutual link
		break;
	case PROP_SPEC_CREATED:
		if (!fInsert) return (sFlags&S_REPLICATION)==0||v.op>=OP_ADD_BEFORE?RC_CONSTRAINT:v.type!=VT_DATETIME?RC_TYPE:RC_OK;
	case PROP_SPEC_UPDATED:
		if ((sFlags&S_REPLICATION)==0) {if (ts==0ULL) getTimestamp(ts); v.type=VT_DATETIME; v.length=sizeof(uint64_t); v.ui64=ts;} else if (v.type!=VT_DATETIME) return RC_TYPE;
		break;
	case PROP_SPEC_CREATEDBY:
		if (!fInsert) return (sFlags&S_REPLICATION)==0||v.op>=OP_ADD_BEFORE?RC_CONSTRAINT:v.type!=VT_IDENTITY?RC_TYPE:RC_OK;
	case PROP_SPEC_UPDATEDBY:
		if ((sFlags&S_REPLICATION)==0) {if (fInsert) {v.type=VT_IDENTITY; v.length=sizeof(uint32_t); v.iid=getIdentity();}} else if (v.type!=VT_IDENTITY) return RC_TYPE;
		break;
	case PROP_SPEC_OBJID:
		if (!fInsert) return RC_INVPARAM;
		if (v.type==VT_STRING) {
			const char *p=v.str,*pref; size_t l=v.length,lPref;
			if (!hasPrefix(p,l) && (pref=ctx->namedMgr->getStorePrefix(lPref))!=NULL) {
				if ((p=(char*)alloca(lPref+l))==NULL) return RC_NOMEM;
				memcpy((char*)p,pref,lPref); memcpy((char*)p+lPref,v.str,l); l+=lPref;
			}
			URI *uri=ctx->uriMgr->insert(p,l); if (uri==NULL) return RC_NOMEM;
			w.setURIID(uri->getID()); w.setPropID(PROP_SPEC_OBJID); w.setMeta(v.meta); uri->release(); v=w;
		} else if (v.type!=VT_URIID) {if (fE1) break; else  return RC_TYPE;}
		if (v.uid==STORE_INVALID_URIID || v.uid<=MAX_BUILTIN_PROPID && v.uid>MAX_BUILTIN_CLASSID) return RC_INVPARAM;
		break;
	case PROP_SPEC_PREDICATE:
		if (v.type==VT_STRING) {
			if ((rc=convV(v,w,VT_STMT,(MemAlloc*)this))!=RC_OK) return rc;
			w.property=PROP_SPEC_PREDICATE; w.meta=v.meta; v=w;
		} else if (v.type!=VT_STMT||v.stmt==NULL) {if (fE1) break; return RC_TYPE;}
		if ((qry=(Stmt*)v.stmt)->getOp()!=STMT_QUERY || qry->getTop()==NULL || qry->getTop()->getType()!=QRY_SIMPLE || (qry->getMode()&QRY_CPARAMS)!=0)	// UNION/INTERSECT/EXCEPT ??? -> condSatisfied
			return RC_INVPARAM;
		if ((v.meta&META_PROP_INDEXED)!=0 && !qry->isClassOK()) v.meta&=~META_PROP_INDEXED;
		break;
	case PROP_SPEC_WINDOW:
		if (v.type!=VT_INTERVAL && v.type!=VT_UINT && v.type!=VT_INT && !fE1) return RC_TYPE;
		if (v.type==VT_INTERVAL && v.i64<0 || v.type==VT_INT && v.i<0) return RC_INVPARAM;
		break;
	case PROP_SPEC_SERVICE:
	case PROP_SPEC_LISTEN:
		switch (v.type) {
		default: if (!fE) return RC_TYPE; break;
		//case VT_STRING: // cv to URIID
		case VT_REF: if ((((PIN*)v.pin)->meta&PMT_COMM)==0) return RC_TYPE;
		case VT_URIID: case VT_REFID: break;
		case VT_STRUCT:
			if ((cv=VBIN::find(PROP_SPEC_SERVICE,v.varray,v.length))==NULL) return RC_INVPARAM;
			if (cv->type!=VT_MAP && cv->type!=VT_URIID && cv->type!=VT_STRING) return RC_TYPE;
			break;
		case VT_COLLECTION:
			if (!fE && !v.isNav()) for (unsigned k=0; k<v.length; k++) {
				const Value *pv2=&v.varray[k];
				switch (pv2->type) {
				default: return RC_TYPE;
				//case VT_STRING: // cv to URIID
				case VT_REF: if ((((PIN*)pv2->pin)->meta&PMT_COMM)==0) return RC_TYPE;
				case VT_URIID: case VT_REFID: break;
				case VT_STRUCT:
					if ((cv=VBIN::find(PROP_SPEC_SERVICE,pv2->varray,pv2->length))==NULL) return RC_INVPARAM;
					if (cv->type!=VT_MAP && cv->type!=VT_URIID && cv->type!=VT_STRING) return RC_TYPE;
					break;
				}
			}
			break;
		}
		break;
	case PROP_SPEC_INTERVAL:
		if (v.type!=VT_INTERVAL && !fE1) return RC_TYPE;
		break;
	case PROP_SPEC_ACTION:
	case PROP_SPEC_ONENTER:
	case PROP_SPEC_ONUPDATE:
	case PROP_SPEC_ONLEAVE:
		if (v.type==VT_STRING) {
			if ((rc=convV(v,w,VT_STMT,this))!=RC_OK) return rc;
			w.property=PROP_SPEC_ACTION; w.meta=v.meta; v=w;
		}
		if (v.type==VT_COLLECTION && !v.isNav()) {
			// check all VT_STMT
		} else if (v.type!=VT_STMT && !fE) return RC_TYPE;
		break;
	case PROP_SPEC_ENUM:
		if (!fInsert) return RC_INVPARAM;
		if (v.type!=VT_STRING) {
			if (v.type!=VT_COLLECTION || v.isNav()) return RC_TYPE;
			for (unsigned i=0; i<v.length; i++) if (v.varray[i].type!=VT_STRING) return RC_TYPE;
		}
		break;
	case PROP_SPEC_IDENTITY:
		if (v.type!=VT_IDENTITY && !fE1) return RC_TYPE;
		break;
	case PROP_SPEC_LOAD:
	case PROP_SPEC_NAMESPACE:
		if (v.type!=VT_STRING && !fE1) return RC_TYPE;
		break;
	case PROP_SPEC_TRANSITION:
		//???
		break;
	case PROP_SPEC_STATE:
		if (v.type!=VT_REF && v.type!=VT_REFID) return RC_TYPE;		// VT_COLLECTION?
		break;
	}
	return RC_OK;
}

RC Session::newPINFlags(unsigned& md,const PID *&orig)
{
	unsigned m=md; md&=(PIN_NO_REPLICATION|PIN_REPLICATED|PIN_HIDDEN|PIN_TRANSIENT);
	if ((itf&(ITF_DEFAULT_REPLICATION|ITF_REPLICATION))!=0 && (md&PIN_NO_REPLICATION)==0) md|=PIN_REPLICATED;
	if (orig!=NULL && !orig->isPID()) orig=NULL;
	if (isRestore()) {if (orig==NULL) return RC_INVPARAM; else if ((m&MODE_DELETED)!=0) md|=PIN_DELETED;}
	else {
		if (orig!=NULL && (itf&ITF_CATCHUP)==0) {
			if (!isRemote(*orig)) {report(MSG_ERROR,"Can't load PIN " _LX_FM " in non-loading session\n",orig->pid); return RC_INVPARAM;}
			if (active==NULL && !isReplication()) {report(MSG_ERROR,"Can't create replicated PIN " _LX_FM ":%08X in non-replication session\n",orig->pid,orig->ident); return RC_INVPARAM;}
		}
		md&=~PIN_DELETED;
	}
	return RC_OK;
}

IBatch *Session::createBatch()
{
	try {return ctx->inShutdown()?NULL:new(this) BatchInsert(this);}
	catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IBatch::createBatch(...)\n");}
	return NULL;
}

unsigned BatchInsert::getNumberOfPINs() const
{
	try {return nTs;} catch (...) {report(MSG_ERROR,"Exception in IBatch::getNumberOfPINs(...)\n"); return ~0u;}
}

size_t BatchInsert::getSize() const
{
	try {return getTotal();} catch (...) {report(MSG_ERROR,"Exception in IBatch::getSize(...)\n"); return ~size_t(0);}
}

Value *BatchInsert::createValues(uint32_t nValues)
{
	try {
		if (ses->getStore()->inShutdown()) return NULL;
		Value *pv=(Value*)StackAlloc::malloc(nValues*sizeof(Value));
		if (pv!=NULL) memset(pv,0,nValues*sizeof(Value));
		return pv;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IBatch::createValues(...)\n");}
	return NULL;
}

RC BatchInsert::createPIN(Value *values,unsigned nValues,unsigned md,const PID *orig)
{
	try {
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN;
		unsigned pmd=md; uint32_t nv=(uint32_t)nValues; RC rc=ses->newPINFlags(pmd,orig); if (rc!=RC_OK) return rc;
		if (values!=NULL && nValues!=0 && (rc=ses->normalize(*(const Value**)&values,nv,md,orig!=NULL?ctx->genPrefix(ushort(orig->pid>>48)):ctx->getPrefix(),this))!=RC_OK) return rc;
		PIN *pin=new(this) PIN(this,pmd,values,nv); if (pin==NULL) {freeV(values,nv,this); return RC_NOMEM;}
		if (orig!=NULL) *pin=*orig; return (*this)+=pin;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IBatch::createPIN(...)\n"); return RC_INTERNAL;}
}

RC BatchInsert::addRef(unsigned from,const Value& to,bool fAdd)
{
	try {
		if (from>=nTs || to.property==STORE_INVALID_URIID) return RC_INVPARAM;
		Value v;
		switch (to.type) {
		default: return RC_TYPE;
		case VT_INT: if (to.i<0) return RC_INVPARAM;
		case VT_UINT: if (to.ui>=nTs) return RC_INVPARAM; v.set(ts[to.ui]); break;
		case VT_REF: v.set(to.pin->getPID()); break;
		case VT_REFID: v.set(to.id); break;
		}
		v.setPropID(to.property); v.setOp(fAdd?OP_ADD:OP_SET);
		return ts[from]->modify(&v,to.eid==STORE_COLLECTION_ID&&fAdd?STORE_LAST_ELEMENT:to.eid,ses->getStore()->getPrefix(),MODP_NCPY);
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IBatch::addRef(...)\n"); return RC_INTERNAL;}
}

RC BatchInsert::clone(IPIN *p,const Value *values,unsigned nValues,unsigned md)
{
	try {
		if (ses->getStore()->inShutdown()) return RC_SHUTDOWN; PIN *pin=(PIN*)p; RC rc;
		unsigned pmd=(md&(PIN_NO_REPLICATION|PIN_REPLICATED|PIN_HIDDEN|PIN_TRANSIENT))|(pin->mode&(PIN_REPLICATED|PIN_NO_REPLICATION|PIN_HIDDEN));
		PIN *newp=new(this) PIN(this,pmd); if (newp==NULL) return RC_NOMEM;
		newp->properties=(Value*)malloc(pin->nProperties*sizeof(Value)); if (newp->properties==NULL) return RC_NOMEM;
		for (unsigned i=0; (newp->nProperties=i)<pin->nProperties; i++)
			if (pin->properties[i].type!=VT_COLLECTION||!pin->properties[i].isNav()||pin->properties[i].nav==NULL) {
				if ((rc=copyV(pin->properties[i],newp->properties[i],this))!=RC_OK) return rc;
			} else {
				INav *nav=pin->properties[i].nav; unsigned nElts=nav->count(),cnt=0;
				Value *pv=new(this) Value[nElts]; if (pv==NULL) return RC_NOMEM;
				for (const Value *cv=nav->navigate(GO_FIRST); cnt<nElts && cv!=NULL; ++cnt,cv=nav->navigate(GO_NEXT))
					if ((rc=copyV(*cv,pv[cnt],this))!=RC_OK) return rc;
				Value *nv=&newp->properties[i]; nv->set(pv,cnt); nv->setPropID(pin->properties[i].property);
				nv->op=pin->properties[i].op; nv->meta=pin->properties[i].meta; setHT(*nv);
			}
		if (values!=NULL && nValues>0) for (unsigned j=0; j<nValues; j++)
			{const Value *pv=&values[j]; newp->modify(pv,pv->eid,STORE_COLLECTION_ID,0);}
		return (*this)+=newp;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IBatch::clone(...)\n"); return RC_INTERNAL;}
}

RC BatchInsert::process(bool fDestroy,unsigned md,const AllocCtrl *actrl,const IntoClass *into,unsigned nInto)
{
	try {
		RC rc=RC_OK;
		if (nTs!=0) {
			// check in-mem; process in-mem
			TxGuard txg(ses); rc=ses->getStore()->queryMgr->persistPINs(EvalCtx(ses,ECT_INSERT),ts,nTs,md,actrl,NULL,into,nInto);
		}
		if (fDestroy) {Session *s=ses; this->~BatchInsert(); s->free(this);}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IBatch::process(...)\n"); return RC_INTERNAL;}
}

RC BatchInsert::getPIDs(PID *pids,unsigned& nPids,unsigned start) const
{
	try {
		if (pids==NULL || nPids==0) return RC_INVPARAM;
		if (start>=nTs) nPids=0;
		else {
			unsigned n=min(nPids,nTs-start); nPids=n;
			for (unsigned i=0; i<n; i++) pids[i]=ts[i+start]->id;
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IBatch::getPIDs(...)\n"); return RC_INTERNAL;}
}

RC BatchInsert::getPID(unsigned idx,PID& pid) const
{
	try {if (idx>=nTs) {pid=PIN::noPID; return RC_INVPARAM;} pid=ts[idx]->id; return RC_OK;}
	catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IBatch::getPID(...)\n"); return RC_INTERNAL;}
}
	
const Value *BatchInsert::getProperty(unsigned idx,URIID pid) const
{
	try {return idx>=nTs?NULL:ts[idx]->findProperty(pid);}
	catch (RC) {return NULL;} catch (...) {report(MSG_ERROR,"Exception in IBatch::getProperty(...)\n"); return NULL;}
}

ElementID BatchInsert::getEIDBase() const
{
	try {return ses->getStore()->getPrefix();} catch (...) {report(MSG_ERROR,"Exception in IBatch::getEIDBase(...)\n"); return STORE_COLLECTION_ID;}
}

void *BatchInsert::malloc(size_t s)
{
	return StackAlloc::malloc(s);
}

void *BatchInsert::realloc(void *p,size_t s,size_t old)
{
	return StackAlloc::realloc(p,s,old);
}

void BatchInsert::free(void *)
{
}

void BatchInsert::clear()
{
	DynArray<PIN*>::clear();
	truncate(TR_REL_ALL);
}

void BatchInsert::destroy()
{
	Session *s=ses; this->~BatchInsert(); s->free(this);
}
