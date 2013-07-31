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

Written by Mark Venguerov 2004-2012

**************************************************************************************/

#include "affinity.h"
#include "txmgr.h"
#include "classifier.h"
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
	if (fNoFree==0) {
		assert(ses==NULL || ses==Session::getSession());
		freeV(properties,nProperties,ses!=NULL?(MemAlloc*)ses:(MemAlloc*)StoreCtx::get());
	}
}

RC PIN::refresh(bool fNet)
{
	try {
		if (!addr.defined()) return RC_OK; if (ses==NULL) return RC_NOSESSION; assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		RC rc=RC_OK; TxGuard txg(ses);
		if (fNet && isRemote(id) && (mode&PIN_REPLICATED)==0) rc=ctx->netMgr->refresh(this,ses);
		else {
			Value *val=properties; unsigned nProps=nProperties; properties=NULL; nProperties=0; PIN *p=this;
			if ((rc=ctx->queryMgr->loadPIN(ses,id,p,mode|LOAD_ENAV))==RC_OK) freeV(val,nProps,ses);
			else {properties=val; nProperties=nProps;}
		}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::refresh()\n"); return RC_INTERNAL;}
}

RC PIN::setExpiration(uint32_t exp)
{
	try {
		if (ses==NULL) return RC_NOSESSION; if (ses->inReadTx()) return RC_READTX; assert(ses==Session::getSession());
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
	try {return mode&(PIN_NO_REPLICATION|PIN_NOTIFY|PIN_REPLICATED|PIN_HIDDEN|PIN_PERSISTENT|PIN_TRANSIENT|PIN_IMMUTABLE|PIN_DELETED);}
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
		if (ses==NULL) return RC_NOSESSION;
		mode|=PIN_REPLICATED; if (ses->inReadTx()) return RC_READTX; assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		TxGuard txg(ses); return addr.defined()?ctx->queryMgr->setFlag(EvalCtx(ses,ECT_INSERT),id,&addr,HOH_REPLICATED,false):RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::setReplicated()\n"); return RC_INTERNAL;}
}

RC PIN::hide()
{
	try {
		if (ses==NULL) return RC_NOSESSION;
		mode|=PIN_HIDDEN; if (ses->inReadTx()) return RC_READTX; assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		TxGuard txg(ses); return addr.defined()?ctx->queryMgr->setFlag(EvalCtx(ses,ECT_INSERT),id,&addr,HOH_HIDDEN,false):RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::setNoIndex()\n"); return RC_INTERNAL;}
}

RC PIN::setNotification(bool fReset)
{
	try {
		if (ses==NULL) return RC_NOSESSION; if (ses->inReadTx()) return RC_READTX;
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		if (fReset) mode&=~PIN_NOTIFY; else mode|=PIN_NOTIFY;
		TxGuard txg(ses); return addr.defined()?ctx->queryMgr->setFlag(EvalCtx(ses,ECT_INSERT),id,&addr,HOH_NOTIFICATION,fReset):RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::setNotification(...)\n"); return RC_INTERNAL;}
}

RC PIN::deletePIN()
{
	try {
		if (ses==NULL) return RC_NOSESSION; if (ses->inReadTx()) return RC_READTX; assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		TxGuard txg(ses); if (addr.defined()) {PIN *p=this; RC rc=ctx->queryMgr->deletePINs(EvalCtx(ses),&p,&id,1,MODE_CLASS); if (rc!=RC_OK) return rc;}
		destroy(); return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::deletePIN()\n"); return RC_INTERNAL;}
}

RC PIN::undelete()
{
	try {
		if (ses==NULL) return RC_NOSESSION; if (ses->inReadTx()) return RC_READTX; assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		TxGuard txg(ses); return addr.defined()?ctx->queryMgr->setFlag(EvalCtx(ses,ECT_INSERT),id,&addr,HOH_DELETED,true):RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::undelete()\n"); return RC_INTERNAL;}
}

RC PIN::getPINValue(Value& res) const 
{
	try {
		if (ses==NULL) return RC_NOSESSION; assert(ses==Session::getSession());
		if (ses->getStore()->inShutdown()) return RC_SHUTDOWN; TxGuard txg(ses); return getPINValue(res,ses);
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
			rc=Expr::eval(&exp,1,res,EvalCtx(ses,(PIN**)&pp,1,NULL,0));
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
		if (ses==NULL || ses->getStore()->inShutdown()) return NULL;
		unsigned pmd=(md&(PIN_NO_REPLICATION|PIN_NOTIFY|PIN_REPLICATED|PIN_HIDDEN|PIN_TRANSIENT))|(mode&(PIN_REPLICATED|PIN_NO_REPLICATION|PIN_HIDDEN));
		PIN *newp=new(ses) PIN(ses,pmd); if (newp==NULL) return NULL;
		newp->properties=(Value*)ses->malloc(nProperties*sizeof(Value));
		if (newp->properties==NULL) {delete newp; return NULL;}
		for (unsigned i=0; (newp->nProperties=i)<nProperties; i++)
			if (properties[i].type!=VT_COLLECTION||properties[i].nav==NULL) {
				if (copyV(properties[i],newp->properties[i],ses)!=RC_OK) {delete newp; return NULL;}
			} else {
				INav *nav=properties[i].nav; unsigned nElts=nav->count(),cnt=0;
				Value *pv=new(ses) Value[nElts]; if (pv==NULL) {delete newp; return NULL;}
				for (const Value *cv=nav->navigate(GO_FIRST); cnt<nElts && cv!=NULL; ++cnt,cv=nav->navigate(GO_NEXT))
					if (copyV(*cv,pv[cnt],ses)!=RC_OK) {delete newp; return NULL;} 
				Value *nv=&newp->properties[i]; nv->set(pv,cnt); nv->setPropID(properties[i].property);
				nv->op=properties[i].op; nv->meta=properties[i].meta; setHT(*nv,SES_HEAP);
			}
		if (overwriteValues!=NULL && nOverwriteValues>0) for (unsigned j=0; j<nOverwriteValues; j++)
			{const Value *pv=&overwriteValues[j]; newp->modify(pv,pv->eid,STORE_COLLECTION_ID,0,ses);}
		if ((md&MODE_PERSISTENT)!=0 && ses!=NULL) {
			if (ses->getStore()->isServerLocked()) {delete newp; return NULL;}
			TxGuard txg(ses); assert(newp->id.pid==STORE_INVALID_PID && ses==Session::getSession());
			if (ses->getStore()->queryMgr->persistPINs(EvalCtx(ses,ECT_INSERT),&newp,1,md)!=RC_OK) {delete newp; newp=NULL;}
		}
		return newp;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::clone(...)\n");}
	return NULL;
}

IPIN *PIN::project(const PropertyID *props,unsigned nProps,const PropertyID *newProps,unsigned md)
{
	try {
		if (ses==NULL || ses->getStore()->inShutdown()) return NULL;
		unsigned pmd=(md&(PIN_NO_REPLICATION|PIN_NOTIFY|PIN_REPLICATED|PIN_HIDDEN|PIN_TRANSIENT))|(mode&(PIN_REPLICATED|PIN_NO_REPLICATION|PIN_HIDDEN));
		PIN *newp=new(ses) PIN(ses,pmd); if (newp==NULL) return NULL;
		if (props!=NULL && nProps>0) {
			newp->properties=(Value*)ses->malloc(nProps*sizeof(Value));
			if (newp->properties==NULL) {delete newp; return NULL;}
			const Value *oldProp;
			for (unsigned i=0; i<nProps; i++) if ((oldProp=findProperty(props[i]))!=NULL) {
				PropertyID newpid=newProps!=NULL?newProps[i]:props[i];
				Value *newProp=NULL; VBIN::find(newpid,newp->properties,newp->nProperties,&newProp);
				if (newProp<&newp->properties[newp->nProperties]) memmove(newProp+1,newProp,(byte*)&newp->properties[newp->nProperties]-(byte*)newProp);
				if (oldProp->type!=VT_COLLECTION||oldProp->nav==NULL) {
					if (copyV(*oldProp,*newProp,ses)!=RC_OK) {delete newp; return NULL;}
				} else {
					INav *nav=oldProp->nav; unsigned nElts=nav->count(),cnt=0;
					Value *pv=new(ses) Value[nElts]; if (pv==NULL) {delete newp; return NULL;}
					for (const Value *cv=nav->navigate(GO_FIRST); cnt<nElts && cv!=NULL; ++cnt,cv=nav->navigate(GO_NEXT))
						if (copyV(*cv,pv[cnt],ses)!=RC_OK) {delete newp; return NULL;} 
					newProp->set(pv,cnt); newProp->op=oldProp->op; newProp->meta=oldProp->meta; setHT(*newProp,SES_HEAP);
				}
				newProp->setPropID(newpid); newp->nProperties++;
			}
		}
		if ((md&MODE_PERSISTENT)!=0 && ses!=NULL) {
			if (ses->getStore()->isServerLocked()) {delete newp; return NULL;}
			TxGuard txg(ses); assert(newp->id.pid==STORE_INVALID_PID && ses==Session::getSession());
			if (ses->getStore()->queryMgr->persistPINs(EvalCtx(ses,ECT_INSERT),&newp,1,md)!=RC_OK) {delete newp; newp=NULL;}
		}
		return newp;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::project(...)\n");}
	return NULL;
}

RC PIN::modify(const Value *values,unsigned nValues,unsigned md,const ElementID *eids,unsigned *pNFailed)
{
	try {
		if (ses==NULL) return RC_NOSESSION; if (ses->inReadTx()) return RC_READTX; assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		if (!ses->isReplication() && (md&MODE_FORCE_EIDS)!=0 && eids!=NULL) return RC_INVPARAM;
		TxGuard txg(ses); return ctx->queryMgr->modifyPIN(EvalCtx(ses,ECT_INSERT),id,values,nValues,NULL,this,md|MODE_CHECKBI,eids,pNFailed);
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::modify(...)\n"); return RC_INTERNAL;}
}

RC PIN::modify(const Value *pv,unsigned epos,unsigned eid,unsigned flags,MemAlloc *ma)
{
	bool fAdd=pv->op==OP_ADD||pv->op==OP_ADD_BEFORE; if (ma==NULL) ma=ses;
	Value *ins=NULL,*pval=(Value*)VBIN::find(pv->property,properties,nProperties,&ins),save,*pv2;
	if (pval==NULL) {if (pv->op==OP_SET) fAdd=true; else if (!fAdd) return RC_NOTFOUND;}
	RC rc=RC_OK; StoreCtx *ctx=ses->getStore();
	if (fAdd) {
		if (pval==NULL) {
			ptrdiff_t sht=ins==NULL?0:ins-properties;
			if (fNoFree==0) {
				if ((properties=(Value*)ma->realloc(properties,(nProperties+1)*sizeof(Value),nProperties*sizeof(Value)))==NULL) {nProperties=0; return RC_NORESOURCES;}
			} else if ((pv2=new(ma) Value[nProperties+1])!=NULL) {
				memcpy(pv2,properties,nProperties*sizeof(Value));
				for (unsigned i=0; i<nProperties; i++) setHT(pv2[i]);
				properties=pv2; fNoFree=0;
			} else {nProperties=0; return RC_NORESOURCES;}
			ins=&properties[sht];
			if ((unsigned)sht<nProperties) memmove(ins+1,ins,sizeof(Value)*(nProperties-sht));
			if ((flags&MODP_NCPY)!=0) *ins=*pv; else rc=copyV(*pv,*ins,ma);
			if (rc==RC_OK) {
				ins->eid=eid=(flags&MODP_EIDS)!=0?eid:isCollection((ValueType)pv->type)?STORE_COLLECTION_ID:getPrefix(ctx);
				ins->op=OP_SET; nProperties++;
			}
		} else if (pval->type==VT_COLLECTION) {
			// aggregate CNAVIGATOR
			return RC_INTERNAL;
		} else if (pv->type==VT_COLLECTION) {
			// aggregate CNAVIGATOR (Value + CNAV)
			for (const Value *cv=pv->nav->navigate(GO_FIRST); cv!=NULL; cv=pv->nav->navigate(GO_NEXT)) {
				// ???
				Value w=*cv; setHT(w); w.op=pv->op;
				if ((rc=modify(&w,epos,cv->eid,MODP_EIDS|MODP_NEID,ma))!=RC_OK) break;
			}
			return rc;
		} else if (pval->type!=VT_ARRAY) {		// create collection
			unsigned nElts=pv->type==VT_ARRAY?pv->length+1:2,idx;
			Value *coll=(Value*)ma->malloc(nElts*sizeof(Value)); if (coll==NULL) return RC_NORESOURCES;
			if (pv->op==OP_ADD) {coll[0]=*pval; idx=1;} else {coll[nElts-1]=*pval; idx=0;}
			if ((flags&MODP_EIDS)==0 && (eid=getPrefix(ctx))==pval->eid) eid++;
			if (pv->type!=VT_ARRAY) {
				if ((flags&MODP_NCPY)!=0) coll[idx]=*pv;
				else if ((rc=copyV(*pv,coll[idx],ma))!=RC_OK) {ma->free(coll); return rc;}
				coll[idx].op=OP_SET; coll[idx].eid=eid;
			} else if ((flags&MODP_NCPY)==0) for (unsigned i=0; i<pv->length; i++) {
				if ((rc=copyV(pv->varray[i],coll[idx+i],ma))!=RC_OK) {ma->free(coll); return rc;}
				coll[idx+i].op=OP_SET; if ((flags&MODP_EIDS)==0) coll[idx+i].eid=eid++;
			} else {
				memcpy(&coll[idx],pv->varray,pv->length*sizeof(Value)); free((void*)pv->varray,(HEAP_TYPE)(pv->flags&HEAP_TYPE_MASK));
			}
			setHT(*pval,SES_HEAP); pval->eid=STORE_COLLECTION_ID; pval->op=OP_SET;
			pval->type=VT_ARRAY; pval->length=nElts; pval->varray=coll;
		} else {								// add to collection
			assert(pval->varray!=NULL && pval->length>0);
			unsigned nElts=pv->type==VT_ARRAY?pv->length:1; if (epos==STORE_COLLECTION_ID) epos=STORE_LAST_ELEMENT;
			pval->varray=(const Value*)ma->realloc(const_cast<Value*>(pval->varray),(pval->length+nElts)*sizeof(Value),pval->length*sizeof(Value));
			if (pval->varray==NULL) {pval->type=VT_ERROR; return RC_NORESOURCES;}
			Value *v=(Value*)findElement(pval,epos);
			if (v==NULL) v=const_cast<Value*>(&pval->varray[pval->length]);
			else {
				if (pv->op!=OP_ADD_BEFORE) v++;
				if (v<&pval->varray[pval->length]) memmove(v+nElts,v,(pval->length-(v-pval->varray))*sizeof(Value));
			}
			if ((flags&MODP_EIDS)==0) eid=getPrefix(ctx)+pval->length;		// find max of existing!!!
			if (pv->type!=VT_ARRAY) {
				if ((flags&MODP_NCPY)!=0) *v=*pv; else if ((rc=copyV(*pv,*v,ma))==RC_OK) v->op=OP_SET; 
				v->eid=eid;
			} else if ((flags&MODP_NCPY)==0) for (unsigned i=0; i<pv->length; ++v,++i) {
				if ((rc=copyV(pv->varray[i],*v,ma))==RC_OK) {v->op=OP_SET; if ((flags&MODP_EIDS)==0) v->eid=eid++;}
			} else {
				memcpy(v,pv->varray,pv->length*sizeof(Value)); free((void*)pv->varray,(HEAP_TYPE)(pv->flags&HEAP_TYPE_MASK));
			}
			pval->length+=nElts;
		}
		if (rc==RC_OK && (flags&MODP_NEID)==0 && pv->type!=VT_ARRAY) pv->eid=eid;
	} else switch (pv->op) {
	case OP_SET:
		if (epos==STORE_COLLECTION_ID || pval->type!=VT_ARRAY && pval->type!=VT_COLLECTION) {
			freeV(*pval); if ((flags&MODP_NCPY)==0) rc=copyV(*pv,*pval,ma); else *pval=*pv;
			if (pval->type!=VT_ARRAY && pval->type!=VT_COLLECTION) pval->eid=(flags&MODP_EIDS)!=0?eid:getPrefix(ctx);
		} else if (pval->type==VT_ARRAY) {
			Value *elt=(Value*)findElement(pval,epos); if (elt==NULL) return RC_NOTFOUND;
			freeV(*elt); if ((flags&MODP_NCPY)==0) rc=copyV(*pv,*elt,ma); else *elt=*pv;
		} else {
			//...???
			return RC_INTERNAL;
		}
		break;
	case OP_DELETE:
		if (epos==STORE_COLLECTION_ID || pval->type!=VT_ARRAY && pval->type!=VT_COLLECTION) {
			freeV(*pval); --nProperties;
			if (pval!=properties+nProperties) memmove(pval,pval+1,(nProperties-(pval-properties))*sizeof(Value));
		} else if (pval->type==VT_COLLECTION) {
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
		if (pval->type==VT_COLLECTION) {
			//???
			return RC_INTERNAL;
		}
		if (pval->type!=VT_ARRAY||pv->type!=VT_UINT) return RC_TYPE;
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
		save=*pval; rc=Expr::calc((ExprOp)pv->op,*pval,pv,2,0,EvalCtx(ses)); if ((flags&MODP_NCPY)!=0) freeV(*(Value*)pv);
		pval->property=save.property; pval->eid=save.eid; pval->meta=save.meta; pval->op=save.op;
		break;
	}
	return rc;
}

bool PIN::testClassMembership(ClassID classID,const Value *params,unsigned nParams) const
{
	try {
		if (ses==NULL) return false; assert(ses==Session::getSession());
		if (ses->getStore()->inShutdown()) return false; ValueV vv(params,nParams);
		TxGuard txg(ses); return ses->getStore()->queryMgr->test((PIN*)this,classID,EvalCtx(ses,NULL,0,NULL,0,&vv,1),true);
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::testClassMembership(ClassID=%08X)\n",classID);}
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
	if (pid==STORE_INVALID_URIID) {if ((md&LOAD_CARDINALITY)==0) {v.setError(); return RC_INVPARAM;} else {v.set(0u); return RC_OK;}}
	if ((pid&=STORE_MAX_URIID)==PROP_SPEC_PINID) {
		if (id.pid==STORE_INVALID_PID) {
			if (fPartial!=0) return ((PINx*)this)->getVx(pid,v,md,ma,eid);
			if ((md&LOAD_CARDINALITY)!=0) {v.set(0u); return RC_OK;}
			v.setError(pid); return RC_NOTFOUND;
		}
		if ((md&LOAD_CARDINALITY)!=0) v.set(1u); else v.set(id);
		v.property=pid; return RC_OK;
	}
	const Value *pv=VBIN::find(pid,properties,nProperties);
	if (pv==NULL) {
		if (fPartial!=0) return ((PINx*)this)->getVx(pid,v,md,ma,eid);
		if ((md&LOAD_CARDINALITY)!=0) {v.set(0u); return RC_OK;}
		v.setError(pid); return RC_NOTFOUND;
	}
	if ((md&LOAD_CARDINALITY)!=0) {
		switch (pv->type) {
		case VT_ARRAY: v.set((unsigned)pv->length); break;
		case VT_COLLECTION: v.set((unsigned)pv->nav->count()); break;
		default: v.set(1u); break;
		}
		return RC_OK;
	}
	if (eid!=STORE_COLLECTION_ID) switch (pv->type) {
	default: break;
	case VT_ARRAY:
		if (eid==STORE_FIRST_ELEMENT) pv=&pv->varray[0];
		else if (eid==STORE_LAST_ELEMENT) pv=&pv->varray[pv->length-1];
		else for (unsigned i=0; ;i++)
			if (i>=pv->length) return RC_NOTFOUND;
			else if (pv->varray[i].eid==eid) pv=&pv->varray[i]; break;
		break;
	case VT_COLLECTION:
		return pv->nav->getElementByID(eid,v);
	}
	v=*pv; setHT(v); return RC_OK;
}

RC PIN::isMemberOf(ClassID *&clss,unsigned& nclss)
{
	try {
		clss=NULL; nclss=0; if (ses==NULL) return RC_NOSESSION;
		if (id.pid==STORE_INVALID_PID) return RC_INVPARAM;
		if ((mode&PIN_HIDDEN)==0) {
			ClassResult clr(ses,ses->getStore());
			RC rc=ses->getStore()->classMgr->classify(this,clr); if (rc!=RC_OK) return rc;
			if (clr.nClasses!=0) {
				if ((clss=new(ses) ClassID[clr.nClasses])==NULL) return RC_NORESOURCES;
				for (unsigned i=0; i<clr.nClasses; i++) clss[i]=clr.classes[i]->cid;
				nclss=clr.nClasses;
			}
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::isMemberOf()\n"); return RC_INTERNAL;}
}

PIN *PIN::getPIN(const PID& id,VersionID vid,Session *ses,unsigned md)
{
	try {
		if (ses==NULL||ses->getStore()->inShutdown()) return NULL; assert(ses==Session::getSession());
		TxGuard txg(ses); bool fRemote=isRemote(id); PageAddr addr=PageAddr::noAddr; PIN *pin=NULL;
		if (!fRemote && (md&LOAD_EXT_ADDR)!=0 && addr.convert(uint64_t(id.pid))) ses->setExtAddr(addr);
		if (ses->getStore()->queryMgr->loadPIN(ses,id,pin,md|LOAD_ENAV,NULL,vid)!=RC_OK) {delete pin; pin=NULL;}
		ses->setExtAddr(PageAddr::noAddr); return pin;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::getPIN(PID="_LX_FM",IdentityID=%08X)\n",id.pid,id.ident);}
	return NULL;
}

RC PIN::checkSet(const Value *&pv,const Value& w)
{
	assert(properties!=NULL && nProperties!=0);
	PropertyID pid=pv->property; uint8_t meta=pv->meta,op=pv->op;
	if (fNoFree==0) freeV(*(Value*)pv);
	else {
		ptrdiff_t sht=pv-properties;
		Value *pp=new(ses) Value[nProperties]; if (pp==NULL) return RC_NORESOURCES;
		memcpy(pp,properties,nProperties*sizeof(Value));
		for (unsigned i=0; i<nProperties; i++) setHT(pp[i]);
		properties=pp; fNoFree=0; pv=pp+sht;
	}
	if (!w.isEmpty()) {*(Value*)pv=w; ((Value*)pv)->property=pid; ((Value*)pv)->meta=meta; ((Value*)pv)->op=op;}
	else if (--nProperties==0) {ses->free((Value*)properties); properties=NULL;}
	else if (pv<&properties[nProperties]) memmove((Value*)pv,pv+1,(byte*)&properties[nProperties]-(byte*)pv);
	return RC_OK;
}

void PIN::destroy()
{
	try {delete this;} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::destroy()\n");}
}

//-----------------------------------------------------------------------------

void PINx::releaseLatches(PageID pid,PageMgr *mgr,bool fX)
{
	if (!pb.isNull() && (pid==INVALID_PAGEID || fX && pid==pb->getPageID() || mgr!=NULL && mgr->getPGID()==PGID_HEAP && pid<pb->getPageID())) {
		if (fReload!=0 && fPartial!=0 && hpin!=NULL) ses->getStore()->queryMgr->loadProps(this,0/*,flt,nFlt*/);	// LOAD_SSV ??? if failed?
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
	id=PIN::noPID; addr=PageAddr::noAddr; properties=NULL; nProperties=0; mode=0; meta=0; hpin=NULL; tv=NULL; epr.lref=0; epr.flags=0;
}

bool PINx::defined(const PropertyID *pids,unsigned nP) const
{
	if (pids!=NULL && nP!=0) {
		if (properties==NULL && hpin==NULL) return false;
		for (unsigned i=0; i<nP; i++) {
			const PropertyID pid=pids[i]&STORE_MAX_URIID;
			if (pid==PROP_SPEC_PINID) {if (id.pid==STORE_INVALID_PID || id.ident==STORE_INVALID_IDENTITY) return false;}
			else if (VBIN::find(pid,properties,nProperties)==NULL && (fPartial==0 || hpin->findProperty(pid)==NULL)) return false;
		}
	}
	return true;
}

bool PINx::isCollection(PropertyID pid) const
{
	if (properties!=NULL) {
		const Value *cv=VBIN::find(pid,properties,nProperties);
		return cv!=NULL && (cv->type==VT_ARRAY||cv->type==VT_COLLECTION);
	}
	if (hpin!=NULL) {
		const HeapPageMgr::HeapV *hprop=hpin->findProperty(pid);
		return hprop!=NULL && hprop->type.isCollection();
	}
	return false;
}

RC PINx::getVx(PropertyID pid,Value& v,unsigned md,MemAlloc *ma,ElementID eid)
{
	RC rc; assert(fPartial!=0);
	if (pid!=PROP_SPEC_PINID) {
		if (!pb.isNull() || (rc=ses->getStore()->queryMgr->getBody(*this))==RC_OK)
			return ses->getStore()->queryMgr->loadV(v,pid,*this,md,ma,eid);
	} else if (id.pid!=STORE_INVALID_PID || (rc=unpack())==RC_OK) {
		if ((md&LOAD_CARDINALITY)!=0) v.set(1u); else v.set(id);
		return RC_OK;
	}
	if ((md&LOAD_CARDINALITY)!=0) {v.set(0u); return RC_OK;}
	v.setError(pid); return rc;
}

#if 0
RC PINx::loadV(const PropertyID *pids,Value *res,unsigned nP) const
{
	RC rc=RC_OK; const Value *pv=properties,*v; unsigned np=nProperties,nhp=0;
	const HeapPageMgr::HeapV *hprop=!pb.isNull() && hpin!=NULL?(nhp=hpin->nProps,hpin->getPropTab()):(HeapPageMgr::HeapV*)0,*hp;
	for (unsigned i=0; i<nP; i++) {
		const PropertyID pid=pids[i]&STORE_MAX_URIID; unsigned flg=pids[i];
		if (res!=NULL && (flg&PROP_NO_LOAD)!=0) (res++)->setError(pid);
		switch (pid) {
		default: break;
		case STORE_INVALID_URIID:
			if ((flg&PROP_OPTIONAL)==0) return RC_INVPARAM;
			if (res!=NULL && (flg&PROP_NO_LOAD)==0) {if ((flg&PROP_ORD)!=0) res->set(0u); else res->setError(); res++;}
			continue;
		case PROP_SPEC_PINID:
			if (res!=NULL && (flg&PROP_NO_LOAD)==0) {
				if ((flg&PROP_ORD)!=0) res->set(1u); else if (id.pid!=STORE_INVALID_PID || (rc=unpack())==RC_OK) res->set(id); else return rc;
				res->property=pid; res++;
			}
			continue;
		}
		if (pv!=NULL && (v=VBIN::find(pid,pv,np))!=NULL) {
			if (res!=NULL && (flg&PROP_NO_LOAD)==0) {
				if ((flg&PROP_ORD)!=0) res->set((unsigned)(v->type==VT_ARRAY?v->length:v->type==VT_COLLECTION?v->nav->count():1u)); else {*res=*v; setHT(*res);}
				res->property=pid; res++;
			}
			pv=v+1; np=nProperties-unsigned(pv-properties);
		} else if (hprop!=NULL && (hp=BIN<HeapPageMgr::HeapV,PropertyID,HeapPageMgr::HeapV::HVCmp>::find(pid,hprop,nhp))!=NULL) {
			if (res!=NULL && (flg&PROP_NO_LOAD)==0) {
				//loadV
				res++;
			}
			// adjust hprop, nhp
		} else {
			if ((flg&PROP_OPTIONAL)==0) return RC_NOTFOUND;
			if (res!=NULL && (flg&PROP_NO_LOAD)==0) {if ((flg&PROP_ORD)!=0) res->set(0u); else res->setError(pid); res++;}
		}
	}
	return rc;
}
#endif

RC PINx::unpack() const
{
	try {
		PINRef pr(ses->getStore()->storeID,epr.buf,epr.lref);
		if ((pr.def&PR_ADDR)!=0) {addr=pr.addr; epr.flags|=PINEX_ADDRSET;}
		const_cast<PID&>(id)=pr.id; return RC_OK;
	} catch (RC rc) {return rc;}
}

RC PINx::pack() const
{
	if (epr.lref==0) {
		if (id.pid==STORE_INVALID_PID) return RC_NOTFOUND;
		else {
			PINRef pr(ses->getStore()->storeID,id);
			if ((meta&PMT_COMM)!=0) pr.def|=PR_SPECIAL;
			if (addr.defined()) {pr.def|=PR_ADDR; pr.addr=addr;}
			epr.lref=pr.enc(epr.buf);
		}
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
	if ((dscr&HOH_NOTIFICATION)!=0) mode|=PIN_NOTIFY;
	if ((dscr&HOH_HIDDEN)!=0) mode|=PIN_HIDDEN;
	if ((dscr&HOH_DELETED)!=0) mode|=PIN_DELETED;
	if (hpin->nProps!=0) fPartial=1;		// cb.tv ????
}

RC PINx::load(unsigned md,const PropertyID *flt,unsigned nFlt)
{
	if (id.pid==STORE_INVALID_PID) {RC rc=unpack(); if (rc!=RC_OK) return rc;}
	if (properties!=NULL || fPartial==0) return RC_OK; // check LOAD_ENAV, LOAD_SSV/PIN_SSV, MODE_FORCED_SSV_AS_STREAM
	if (hpin==NULL) {
		if (id.pid==STORE_INVALID_PID) return RC_NOTFOUND;
		RC rc=ses->getStore()->queryMgr->getBody(*this,TVO_READ,(md&MODE_DELETED)!=0?GB_DELETED:0);
		if (rc!=RC_OK) {if (rc==RC_DELETED) mode|=PIN_DELETED; return rc;}
	}
	return ses->getStore()->queryMgr->loadProps(this,md,flt,nFlt);
}

const Value *PINx::loadProperty(PropertyID pid)
{
	const HeapPageMgr::HeapV *hprop; pid&=STORE_MAX_URIID; Value v,*pv; assert(fPartial!=0);
	if (hpin==NULL || (hprop=hpin->findProperty(pid))==NULL) return NULL;
	if (fNoFree!=0) {
		// copy properties
	}
	if (ses->getStore()->queryMgr->loadV(v,pid,*this,LOAD_SSV,ses)!=RC_OK) return NULL;
	if (VBIN::insert(properties,nProperties,pid,v,(MemAlloc*)ses,NULL,&pv)!=RC_OK) {freeV(v); return NULL;}
	return pv;
}

const void *PINx::getPropTab(unsigned& nProps) const
{
	if (hpin==NULL) {nProps=0; return NULL;}
	nProps=hpin->nProps; return hpin->getPropTab();
}

void PINx::copy(const PIN *pin,unsigned flags)
{
	id=pin->id; addr=pin->addr; properties=pin->properties; nProperties=pin->nProperties; mode=pin->mode; fNoFree=1; meta=pin->meta; epr.lref=0;
	if (!addr.defined()) epr.flags=PINEX_DERIVED;
	else if (pin->fPartial!=0) {
		const PINx *px=(const PINx *)pin; epr=px->epr; tv=px->tv;
		if (!px->pb.isNull() && pb.getPage(px->pb->getPageID(),px->pb->getPageMgr(),flags,pin->ses)!=NULL) fill();
	}
}

void PINx::free()
{
	if (properties!=NULL && fNoFree==0) freeV((Value*)properties,nProperties,ses);
	properties=NULL; nProperties=0; fPartial=1; fNoFree=fSSV=fReload=0; meta=0;
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
		Value &v=*(Value*)&pv[j]; setHT(v,flags); if (fEID&&!fCV && v.type!=VT_VARREF) v.eid=prefix;
		if (j!=0&&pv[j-1].property>=v.property) sort|=pv[j-1].property==v.property?1:2;
		if ((f&MODE_COMPOUND)==0 && v.property<=MAX_BUILTIN_URIID) nBI++;
		if (v.type==VT_ARRAY) {
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
	else if ((pn=new(ma) Value[nv])==NULL) return RC_NORESOURCES;
	else {memcpy(pn,pv,nv*sizeof(Value)); pv=pn;}
	if (fCV || sort!=0) {
		if ((sort&2)!=0) qsort(pn,nv,sizeof(Value),cmpValues);
		if (fCV) {
			if (!fNF) {
				rc=pn[0].type!=VT_STRUCT?copyV(pn[0],pn[0],ma):normalize(pn[0].varray,pn[0].length,f|MODE_COMPOUND,prefix,ma);
				if (rc!=RC_OK) {ma->free(pn); return rc;}
			}
			if (fEID && pn[0].type!=VT_VARREF) pn[0].eid=prefix;
		}
		for (unsigned i=1; i<nv; ) {
			Value *pv1=&pn[i-1],*pv2=&pn[i];
			if (fCV) {
				if (!fNF) {
					rc=pv2->type!=VT_STRUCT?copyV(*pv2,*pv2,ma):normalize(pv2->varray,pv2->length,f|MODE_COMPOUND,prefix,ma);
					if (rc!=RC_OK) {while (i!=0) freeV(pn[--i]); ma->free(pn); return rc;}
				}
				if (fEID && pv2->type!=VT_VARREF) pv2->eid=prefix;
			}
			if (pv1->property<pv2->property) {
				if ((f&MODE_COMPOUND)==0 && pv1->property<=MAX_BUILTIN_URIID && (rc=checkBuiltinProp(*pv1,ts,true))!=RC_OK) return rc;
				i++;
			} else if (pv1->type==VT_COLLECTION || pv2->type==VT_COLLECTION) {
				// ???
				i++;
			} else {
				unsigned l1=pv1->type==VT_ARRAY?pv1->length:1,l2=0,ii=0; assert(pv1->property==pv2->property);
				do l2+=pv2[ii].type==VT_ARRAY?pv2[ii].length:1;
				while (++ii+i<nv && pv2[ii].property==pv1->property);
				Value *vals=pv1->type!=VT_ARRAY||fCV?(Value*)ma->malloc((l1+l2)*sizeof(Value)):
					(Value*)ma->realloc(const_cast<Value*>(pv1->varray),(l1+l2)*sizeof(Value),l1*sizeof(Value));
				if (pv1->type!=VT_ARRAY) {vals[0]=*pv1; vals[0].property=STORE_INVALID_URIID;}
				else if (fCV) memcpy(vals,pv1->varray,l1*sizeof(Value));
				pv1->varray=vals; pv1->length=l1+l2; pv1->type=VT_ARRAY; pv1->eid=STORE_COLLECTION_ID;
				setHT(*pv1,ht); vals+=l1; unsigned ll; ElementID eid=prefix+l1;
				do {
					ll=pv2->type==VT_ARRAY?pv2->length:1; pv1->meta|=pv2->meta;
					if (!fCV || fNF) {
						memcpy(vals,pv2->type==VT_ARRAY?pv2->varray:pv2,ll*sizeof(Value));
						if (fEID) for (unsigned k=0; k<ll; k++) if (vals[k].type!=VT_VARREF) vals[k].eid=eid++;
						if (!fCV && pv2->type==VT_ARRAY && (pv2->flags&HEAP_TYPE_MASK)>=SES_HEAP) 
							{AfyKernel::free(const_cast<Value*>(pv2->varray),(HEAP_TYPE)(pv2->flags&HEAP_TYPE_MASK)); setHT(*pv2);}
					} else for (unsigned j=0; j<ll; j++) if ((rc=copyV(pv2->type==VT_ARRAY?pv2->varray[j]:*pv2,vals[j],ma))!=RC_OK) {
						//...free
						return rc;
					} else if (fEID && vals[j].type!=VT_VARREF) vals[j].eid=eid++;
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
		if (ctx->inShutdown()) return RC_SHUTDOWN;
		unsigned pmd=md; RC rc=newPINFlags(pmd,orig); if (rc!=RC_OK) return rc;
		PIN *pin=active!=NULL?active->createPIN(values,nValues,pmd,orig):NULL;
		if (pin==NULL) {
			if ((pin=new(this) PIN(this,pmd))==NULL) rc=RC_NORESOURCES; else if (orig!=NULL) *pin=*orig;
			if (values!=NULL && nValues!=0) {
				if (pin!=NULL) {
					uint32_t nv=(uint32_t)nValues; const Value *pv=values;
					if ((rc=normalize(pv,nv,md,orig!=NULL?ctx->genPrefix(ushort(orig->pid>>48)):ctx->getPrefix(),this))==RC_OK) {pin->properties=(Value*)pv; pin->nProperties=nv;}
				} else if ((md&MODE_COPY_VALUES)==0) {
					for (unsigned i=0; i<nValues; i++) {
						if (values[i].type==VT_ARRAY) for (unsigned j=0; j<values[i].length; j++) setHT(values[i].varray[i],SES_HEAP);
						// struct ???
						setHT(values[i],SES_HEAP); freeV(values[i]);
					}
					free(values);
				}
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

RC Session::checkBuiltinProp(Value &v,TIMESTAMP &ts,bool fInsert)
{
	Value w; RC rc; const Value *cv; Stmt *qry;
	if (fInsert) {
		if (v.op!=OP_SET && v.op!=OP_ADD) return RC_INVOP;
	} else if (v.op==OP_DELETE) {
		return v.property==PROP_SPEC_PINID||v.property==PROP_SPEC_SELF?RC_INVPARAM:RC_OK;
	}
	switch (v.property) {
	default: if (v.property>MAX_BUILTIN_URIID) return RC_INVPARAM; break;
	case PROP_SPEC_SUBCLASSES:
	case PROP_SPEC_SUPERCLASSES:
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
		if (v.op<=OP_ADD_BEFORE && v.type!=VT_REF && v.type!=VT_REFID) return RC_TYPE;
		break;
	case PROP_SPEC_PARENT:
		if (v.op<=OP_ADD_BEFORE && v.type!=VT_REF && v.type!=VT_REFID) return RC_TYPE;
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
		if (v.type==VT_STRING || v.type==VT_URL) {
			const char *p=v.str,*pref; size_t l=v.length,lPref;
			if (!hasPrefix(p,l) && (pref=ctx->namedMgr->getStorePrefix(lPref))!=NULL) {
				if ((p=(char*)alloca(lPref+l))==NULL) return RC_NORESOURCES;
				memcpy((char*)p,pref,lPref); memcpy((char*)p+lPref,v.str,l); l+=lPref;
			}
			URI *uri=ctx->uriMgr->insert(p,l); if (uri==NULL) return RC_NORESOURCES;
			w.setURIID(uri->getID()); w.setPropID(PROP_SPEC_OBJID); w.setMeta(v.meta); uri->release(); v=w;
		} else if (v.type!=VT_URIID) return RC_TYPE;
		if (v.uid==STORE_INVALID_CLASSID || v.uid<=MAX_BUILTIN_PROPID && v.uid>MAX_BUILTIN_CLASSID) return RC_INVPARAM;
		break;
	case PROP_SPEC_PREDICATE:
		if (v.type==VT_STRING) {
			if ((rc=convV(v,w,VT_STMT,(MemAlloc*)this))!=RC_OK) return rc;
			w.property=PROP_SPEC_PREDICATE; w.meta=v.meta; v=w;
		} else if (v.type!=VT_STMT||v.stmt==NULL) return RC_TYPE;
		if ((qry=(Stmt*)v.stmt)->getOp()!=STMT_QUERY || qry->getTop()==NULL || qry->getTop()->getType()!=QRY_SIMPLE || (qry->getMode()&QRY_CPARAMS)!=0)	// UNION/INTERSECT/EXCEPT ??? -> condSatisfied
			return RC_INVPARAM;
		if ((v.meta&META_PROP_INDEXED)!=0 && !qry->isClassOK()) v.meta&=~META_PROP_INDEXED;
		break;
	case PROP_SPEC_WINDOW:
		if (v.type!=VT_INTERVAL && v.type!=VT_UINT && v.type!=VT_INT) return RC_TYPE;
		if (v.type==VT_INTERVAL && v.i64<0 || v.type==VT_INT && v.i<0) return RC_INVPARAM;
		break;
	case PROP_SPEC_SERVICE:
	case PROP_SPEC_LISTEN:
		switch (v.type) {
		default: return RC_TYPE;
		//case VT_STRING: case VT_URL:// cv to URIID
		case VT_REF: if ((((PIN*)v.pin)->meta&PMT_COMM)==0) return RC_TYPE;
		case VT_URIID: case VT_REFID: break;
		case VT_STRUCT:
			if ((cv=VBIN::find(PROP_SPEC_SERVICE,v.varray,v.length))==NULL) return RC_INVPARAM;
			if (cv->type!=VT_MAP && cv->type!=VT_URIID && cv->type!=VT_STRING && cv->type!=VT_URL) return RC_TYPE;
			break;
		case VT_ARRAY:
			for (unsigned k=0; k<v.length; k++) {
				const Value *pv2=&v.varray[k];
				switch (pv2->type) {
				default: return RC_TYPE;
				//case VT_STRING: case VT_URL:// cv to URIID
				case VT_REF: if ((((PIN*)pv2->pin)->meta&PMT_COMM)==0) return RC_TYPE;
				case VT_URIID: case VT_REFID: break;
				case VT_STRUCT:
					if ((cv=VBIN::find(PROP_SPEC_SERVICE,pv2->varray,pv2->length))==NULL) return RC_INVPARAM;
					if (cv->type!=VT_MAP && cv->type!=VT_URIID && cv->type!=VT_STRING && cv->type!=VT_URL) return RC_TYPE;
					break;
				}
			}
			break;
		}
		break;
	case PROP_SPEC_INTERVAL:
		if (v.type!=VT_INTERVAL) return RC_TYPE;
		break;
	case PROP_SPEC_ACTION:
	case PROP_SPEC_ONENTER:
	case PROP_SPEC_ONUPDATE:
	case PROP_SPEC_ONLEAVE:
		if (v.type==VT_STRING) {
			if ((rc=convV(v,w,VT_STMT,this))!=RC_OK) return rc;
			w.property=PROP_SPEC_ACTION; w.meta=v.meta; v=w;
		}
		if (v.type==VT_ARRAY) {
			// check all VT_STMT
		} else if (v.type!=VT_STMT) return RC_TYPE;
		break;
	case PROP_SPEC_ENUM:
		if (!fInsert) return RC_INVPARAM;
		if (v.type!=VT_STRING) {
			if (v.type!=VT_ARRAY) return RC_TYPE;
			for (unsigned i=0; i<v.length; i++) if (v.varray[i].type!=VT_STRING) return RC_TYPE;
		}
		break;
	case PROP_SPEC_IDENTITY:
		if (v.type!=VT_IDENTITY) return RC_TYPE;
		break;
	case PROP_SPEC_LOAD:
	case PROP_SPEC_NAMESPACE:
		if (v.type!=VT_STRING) return RC_TYPE;
		break;
	case PROP_SPEC_TRANSITION:
		//???
		break;
	case PROP_SPEC_STATE:
		if ((v.meta&META_PROP_IMMUTABLE)!=0) return RC_INVPARAM;
		if (v.type!=VT_REF && v.type!=VT_REFID) return RC_TYPE;
		break;
	}
	return RC_OK;
}

RC Session::newPINFlags(unsigned& md,const PID *&orig)
{
	unsigned m=md; md&=(PIN_NO_REPLICATION|PIN_NOTIFY|PIN_REPLICATED|PIN_HIDDEN|PIN_TRANSIENT);
	if ((itf&(ITF_DEFAULT_REPLICATION|ITF_REPLICATION))!=0 && (md&PIN_NO_REPLICATION)==0) md|=PIN_REPLICATED;
	if (orig!=NULL && (orig->ident==STORE_INVALID_IDENTITY||orig->pid==STORE_INVALID_PID)) orig=NULL;
	if (isRestore()) {if (orig==NULL) return RC_INVPARAM; else if ((m&MODE_DELETED)!=0) md|=PIN_DELETED;}
	else {
		if (orig!=NULL && (itf&ITF_CATCHUP)==0) {
			if (!isRemote(*orig)) {report(MSG_ERROR,"Can't load PIN "_LX_FM" in non-loading session\n",orig->pid); return RC_INVPARAM;}
			if (active==NULL && !isReplication()) {report(MSG_ERROR,"Can't create replicated PIN "_LX_FM":%08X in non-replication session\n",orig->pid,orig->ident); return RC_INVPARAM;}
		}
		md&=~PIN_DELETED;
	}
	return RC_OK;
}

IBatch *Session::createBatch()
{
	try {return ctx->inShutdown()?NULL:new(this) BatchInsert(this);}
	catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IBatch::createValues(...)\n");}
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
		Value *pv=(Value*)SubAlloc::malloc(nValues*sizeof(Value));
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
		PIN *pin=new(this) PIN(ses,pmd,values,nv); if (pin==NULL) {freeV(values,nv,this); return RC_NORESOURCES;}
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
		return ts[from]->modify(&v,to.eid==STORE_COLLECTION_ID&&fAdd?STORE_LAST_ELEMENT:to.eid,ses->getStore()->getPrefix(),MODP_NCPY,this);
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IBatch::addRef(...)\n"); return RC_INTERNAL;}
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

RC BatchInsert::getPIDs(PID *pids,unsigned& nPids,unsigned start)
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

ElementID BatchInsert::getEIDBase() const
{
	try {return ses->getStore()->getPrefix();} catch (...) {report(MSG_ERROR,"Exception in IBatch::getEIDBase(...)\n"); return STORE_COLLECTION_ID;}
}

void *BatchInsert::malloc(size_t s)
{
	return SubAlloc::malloc(s);
}

void *BatchInsert::realloc(void *p,size_t s,size_t old)
{
	return SubAlloc::realloc(p,s,old);
}

void BatchInsert::free(void *)
{
}

void BatchInsert::destroy()
{
	Session *s=ses; this->~BatchInsert(); s->free(this);
}
