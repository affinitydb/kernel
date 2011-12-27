/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "mvstore.h"
#include "txmgr.h"
#include "classifier.h"
#include "queryprc.h"
#include "ftindex.h"
#include "netmgr.h"
#include "expr.h"
#include "maps.h"
#include "parser.h"
#include <string.h>

using namespace MVStoreKernel;

const PID PIN::defPID = {STORE_INVALID_PID,STORE_INVALID_IDENTITY};

PIN::~PIN()
{
	if ((mode&PIN_NO_FREE)==0) {
		assert(ses!=NULL && ses==Session::getSession());
		freeV(properties,nProperties,ses);
	}
}

RC PIN::refresh(bool fNet)
{
	try {
		if (!addr.defined()) return RC_OK; if (ses==NULL) return RC_NOSESSION; assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		RC rc=RC_OK; TxGuard txg(ses);
		if (fNet && isRemote(id) && (mode&PIN_REPLICATED)==0 && ctx->netMgr->isOnline()) 
			rc=ctx->netMgr->refresh(this,ses);
		else {
			Value *val=properties; ulong nProps=nProperties; properties=NULL; nProperties=0; PIN *p=this;
			if ((rc=ctx->queryMgr->loadPIN(ses,id,p,mode|LOAD_ENAV))!=RC_OK) {properties=val; nProperties=nProps;}
			else if (val!=NULL) {for (ulong i=0; i<nProps; i++) freeV(val[i]); free(val,SES_HEAP);}
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

bool PIN::isCommitted() const
{
	try {return addr.defined();} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::isCommitted()\n");}
	return false;
}

bool PIN::isReadonly() const
{
	return (mode&PIN_READONLY)!=0;
}

bool PIN::canNotify() const
{
	return (mode&PIN_NOTIFY)!=0;
}

bool PIN::isReplicated() const
{
	return (mode&PIN_REPLICATED)!=0;
}

bool PIN::canBeReplicated() const 
{
	return (mode&PIN_NO_REPLICATION)==0;
}

bool PIN::isHidden() const
{
	return (mode&PIN_HIDDEN)!=0;
}

bool PIN::isDeleted() const
{
	return (mode&PIN_DELETED)!=0;
}

bool PIN::isClass() const
{
	return (mode&PIN_CLASS)!=0;
}

bool PIN::isDerived() const
{
	return (mode&PIN_DERIVED)!=0;
}

bool PIN::isProjected() const
{
	return (mode&PIN_PROJECTED)!=0;
}

uint32_t PIN::getNumberOfProperties() const
{
	return nProperties;
}

const Value	*PIN::getValueByIndex(unsigned idx) const
{
	return properties==NULL||idx>=nProperties?NULL:&properties[idx];
}

const Value	*PIN::getValue(PropertyID pid) const
{
	return pid==PROP_SPEC_URI?(const Value*)0:BIN<Value,PropertyID,ValCmp>::find(pid,properties,nProperties);
}

char *PIN::getURI() const
{
	try {
#if 0
		const Value *v=findProperty(PROP_SPEC_URI);
		const char *p=NULL; ulong len=0;
		switch (v!=NULL?(ValueType)v->type:VT_ERROR) {
		default: break;
		case VT_STRING: case VT_URL:
			if (!addr.defined()) {
				// copy, return
			}
			p=v->str; len=v->length; break;
		case VT_BSTR:
			//...
			break;
		}
		if (ses==NULL) return NULL; assert(ses==Session::getSession());
		return ses->getStore()->uriMgr->getURI(id,p,len);
#else
		return NULL;
#endif
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::getURI()\n");}
	return NULL;
}

uint32_t PIN::getStamp() const
{
	try {return addr.defined()?stamp:0;} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::getStamp()\n");}
	return ~0u;
}

RC PIN::setReplicated()
{
	try {
		if ((mode&PIN_CLASS)!=0) return RC_INVOP; if (ses==NULL) return RC_NOSESSION;
		mode|=PIN_REPLICATED; if (ses->inReadTx()) return RC_READTX; assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		TxGuard txg(ses); return addr.defined()?ctx->queryMgr->setFlag(ses,id,&addr,HOH_REPLICATED,false):RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::setReplicated()\n"); return RC_INTERNAL;}
}

RC PIN::setNoIndex()
{
	try {
		if ((mode&PIN_CLASS)!=0) return RC_INVOP; if (ses==NULL) return RC_NOSESSION;
		mode|=PIN_NO_INDEX; if (ses->inReadTx()) return RC_READTX; assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		TxGuard txg(ses); return addr.defined()?ctx->queryMgr->setFlag(ses,id,&addr,HOH_NOINDEX,false):RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::setNoIndex()\n"); return RC_INTERNAL;}
}

RC PIN::deletePIN()
{
	try {
		if (ses==NULL) return RC_NOSESSION; if (ses->inReadTx()) return RC_READTX; assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		TxGuard txg(ses); if (addr.defined()) {PIN *p=this; RC rc=ctx->queryMgr->deletePINs(ses,&p,&id,1,MODE_CLASS); if (rc!=RC_OK) return rc;}
		destroy(); return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::deletePIN()\n"); return RC_INTERNAL;}
}

RC PIN::undelete()
{
	try {
		if ((mode&PIN_CLASS)!=0) return RC_INVOP; if (ses==NULL) return RC_NOSESSION; if (ses->inReadTx()) return RC_READTX; assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		TxGuard txg(ses); return addr.defined()?ctx->queryMgr->setFlag(ses,id,&addr,HOH_DELETED,true):RC_OK;
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
	const Value *pv=findProperty(PROP_SPEC_VALUE); if (pv==NULL) return RC_NOTFOUND;
	for (int cnt=0;;cnt++) {
		RC rc; Expr *exp; HEAP_TYPE save;
		switch (pv->type) {
		default: return pv!=&res?copyV(*pv,res,ses):RC_OK;
		case VT_EXPR:
			{exp=(Expr*)pv->expr; save=(HEAP_TYPE)(pv->flags&HEAP_TYPE_MASK);
			res.type=VT_ERROR; PINEx pex(this),*pp=&pex;
			rc=Expr::eval(&exp,1,res,&pp,1,NULL,0,ses);
			if (pv==&res && save!=NO_HEAP) if (save==SES_HEAP) ses->free(exp); else free(exp,save);}
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
			if (cnt>0 || (pv=findProperty(pv->uid))==NULL) return RC_NOTFOUND;
			continue;
		}
	}
}

IPIN *PIN::clone(const Value *overwriteValues,unsigned nOverwriteValues,unsigned mode)
{
	try {
		if (ses!=NULL && ses->getStore()->inShutdown()) return NULL; if ((this->mode&PIN_CLASS)!=0) return NULL;
		mode|=this->mode&(PIN_REPLICATED|PIN_NO_REPLICATION|PIN_NO_INDEX);
		PIN *newp=new(ses) PIN(ses,defPID,PageAddr::invAddr,mode); if (newp==NULL) return NULL;
		newp->properties=(Value*)malloc(nProperties*sizeof(Value),SES_HEAP);
		if (newp->properties==NULL) {delete newp; return NULL;}
		for (unsigned i=0; (newp->nProperties=i)<nProperties; i++)
			if (properties[i].type!=VT_COLLECTION||properties[i].nav==NULL) {
				if (copyV(properties[i],newp->properties[i],ses)!=RC_OK) {delete newp; return NULL;}
			} else {
				INav *nav=properties[i].nav; ulong nElts=nav->count(),cnt=0;
				Value *pv=new(ses) Value[nElts]; if (pv==NULL) {delete newp; return NULL;}
				for (const Value *cv=nav->navigate(GO_FIRST); cnt<nElts && cv!=NULL; ++cnt,cv=nav->navigate(GO_NEXT))
					if (copyV(*cv,pv[cnt],ses)!=RC_OK) {delete newp; return NULL;} 
				Value *nv=&newp->properties[i]; nv->set(pv,cnt); nv->setPropID(properties[i].property);
				nv->op=properties[i].op; nv->meta=properties[i].meta; nv->flags=SES_HEAP;
			}
		if (overwriteValues!=NULL && nOverwriteValues>0) for (unsigned j=0; j<nOverwriteValues; j++)
			{const Value *pv=&overwriteValues[j]; newp->modify(pv,pv->eid,STORE_COLLECTION_ID,0,ses);}
		if ((mode&MODE_NEW_COMMIT)!=0 && ses!=NULL) {
			if (ses->getStore()->isServerLocked()) {delete newp; return NULL;}
			TxGuard txg(ses); assert(newp->id.pid==STORE_INVALID_PID && ses==Session::getSession());
			if (ses->getStore()->queryMgr->commitPINs(ses,&newp,1,mode,ValueV(NULL,0))!=RC_OK) {delete newp; newp=NULL;}
		}
		return newp;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::clone(...)\n");}
	return NULL;
}

IPIN *PIN::project(const PropertyID *props,unsigned nProps,const PropertyID *newProps,unsigned mode)
{
	try {
		if (ses!=NULL && ses->getStore()->inShutdown()) return NULL; if ((this->mode&PIN_CLASS)!=0) return NULL;
		mode|=this->mode&(PIN_REPLICATED|PIN_NO_REPLICATION|PIN_NO_INDEX);
		PIN *newp=new(ses) PIN(ses,defPID,PageAddr::invAddr,mode|PIN_PROJECTED); if (newp==NULL) return NULL;
		if (props!=NULL && nProps>0) {
			newp->properties=(Value*)malloc(nProps*sizeof(Value),SES_HEAP);
			if (newp->properties==NULL) {delete newp; return NULL;}
			const Value *oldProp;
			for (unsigned i=0; i<nProps; i++) if ((oldProp=findProperty(props[i]))!=NULL) {
				PropertyID newpid=newProps!=NULL?newProps[i]:props[i];
				Value *newProp=NULL; BIN<Value,PropertyID,ValCmp>::find(newpid,newp->properties,newp->nProperties,&newProp);
				if (newProp<&newp->properties[newp->nProperties]) memmove(newProp+1,newProp,(byte*)&newp->properties[newp->nProperties]-(byte*)newProp);
				if (oldProp->type!=VT_COLLECTION||oldProp->nav==NULL) {
					if (copyV(*oldProp,*newProp,ses)!=RC_OK) {delete newp; return NULL;}
				} else {
					INav *nav=oldProp->nav; ulong nElts=nav->count(),cnt=0;
					Value *pv=new(ses) Value[nElts]; if (pv==NULL) {delete newp; return NULL;}
					for (const Value *cv=nav->navigate(GO_FIRST); cnt<nElts && cv!=NULL; ++cnt,cv=nav->navigate(GO_NEXT))
						if (copyV(*cv,pv[cnt],ses)!=RC_OK) {delete newp; return NULL;} 
					newProp->set(pv,cnt); newProp->op=oldProp->op; newProp->meta=oldProp->meta; newProp->flags=SES_HEAP;
				}
				newProp->setPropID(newpid); newp->nProperties++;
			}
		}
		if ((mode&MODE_NEW_COMMIT)!=0 && ses!=NULL) {
			if (ses->getStore()->isServerLocked()) {delete newp; return NULL;}
			TxGuard txg(ses); assert(newp->id.pid==STORE_INVALID_PID && ses==Session::getSession());
			if (ses->getStore()->queryMgr->commitPINs(ses,&newp,1,mode,ValueV(NULL,0))!=RC_OK) {delete newp; newp=NULL;}
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
		if (addr.defined()) {TxGuard txg(ses); return ctx->queryMgr->modifyPIN(ses,id,values,nValues,NULL,ValueV(NULL,0),this,md,eids,pNFailed);}
		ulong flags=((md&MODE_FORCE_EIDS)!=0&&eids!=NULL?MODP_EIDS:0)|((md&MODE_NO_EID)!=0?MODP_NEID:0);
		ElementID prefix=getPrefix(ses->getStore()); assert((mode&PIN_CLASS)==0);
		for (unsigned i=0; i<nValues; i++) {
			const Value *pv=&values[i]; pv->flags=NO_HEAP;
			RC rc=modify(pv,pv->eid,(flags&MODP_EIDS)!=0?eids[i]:prefix,flags,ses);
			if (rc!=RC_OK) {if (pNFailed!=NULL) *pNFailed=i; return rc;}
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::modify(...)\n"); return RC_INTERNAL;}
}

RC PIN::modify(const Value *pv,ulong epos,ulong eid,ulong flags,Session *ses)
{
	bool fAdd=pv->op==OP_ADD||pv->op==OP_ADD_BEFORE;
	Value *ins=NULL,*pval=(Value*)BIN<Value,PropertyID,ValCmp>::find(pv->property,properties,nProperties,&ins),save,*pv2;
	if (pval==NULL) {if (pv->op==OP_SET) fAdd=true; else if (!fAdd) return RC_NOTFOUND;}
	if ((pv->meta&META_PROP_IFNOTEXIST)!=0 && pval!=NULL) return RC_OK;
	RC rc=RC_OK; StoreCtx *ctx=ses->getStore();
	if (fAdd) {
		if (pval==NULL) {
			ptrdiff_t sht=ins==NULL?0:ins-properties;
			if ((mode&PIN_NO_FREE)==0) {
				if ((properties=(Value*)ses->realloc(properties,(nProperties+1)*sizeof(Value)))==NULL) {nProperties=0; return RC_NORESOURCES;}
			} else if ((pv2=new(ses) Value[nProperties+1])!=NULL) {
				memcpy(pv2,properties,nProperties*sizeof(Value));
				for (unsigned i=0; i<nProperties; i++) pv2[i].flags=pv2[i].flags&~HEAP_TYPE_MASK|NO_HEAP;
				properties=pv2; mode&=~PIN_NO_FREE;
			} else {nProperties=0; return RC_NORESOURCES;}
			ins=&properties[sht];
			if ((unsigned)sht<nProperties) memmove(ins+1,ins,sizeof(Value)*(nProperties-sht));
			if ((flags&MODP_NCPY)!=0) *ins=*pv; else rc=copyV(*pv,*ins,ses);
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
				Value w=*cv; w.flags=w.flags&~HEAP_TYPE_MASK|NO_HEAP; w.op=pv->op;
				if ((rc=modify(&w,epos,cv->eid,MODP_EIDS|MODP_NEID,ses))!=RC_OK) break;
			}
			return rc;
		} else if (pval->type!=VT_ARRAY) {		// create collection
			ulong nElts=pv->type==VT_ARRAY?pv->length+1:2,idx;
			Value *coll=(Value*)malloc(nElts*sizeof(Value),SES_HEAP); if (coll==NULL) return RC_NORESOURCES;
			if (pv->op==OP_ADD) {coll[0]=*pval; idx=1;} else {coll[nElts-1]=*pval; idx=0;}
			if ((flags&MODP_EIDS)==0 && (eid=getPrefix(ctx))==pval->eid) eid++;
			if (pv->type!=VT_ARRAY) {
				if ((flags&MODP_NCPY)!=0) coll[idx]=*pv;
				else if ((rc=copyV(*pv,coll[idx],ses))!=RC_OK) {free(coll,SES_HEAP); return rc;}
				coll[idx].op=OP_SET; coll[idx].eid=eid;
			} else if ((flags&MODP_NCPY)==0) for (ulong i=0; i<pv->length; i++) {
				if ((rc=copyV(pv->varray[i],coll[idx+i],ses))!=RC_OK) {free(coll,SES_HEAP); return rc;}
				coll[idx+i].op=OP_SET; if ((flags&MODP_EIDS)==0) coll[idx+i].eid=eid++;
			} else {
				memcpy(&coll[idx],pv->varray,pv->length*sizeof(Value)); free((void*)pv->varray,(HEAP_TYPE)(pv->flags&HEAP_TYPE_MASK));
			}
			pval->flags=SES_HEAP; pval->eid=STORE_COLLECTION_ID; pval->op=OP_SET;
			pval->type=VT_ARRAY; pval->length=nElts; pval->varray=coll;
		} else {								// add to collection
			assert(pval->varray!=NULL && pval->length>0);
			ulong nElts=pv->type==VT_ARRAY?pv->length:1; if (epos==STORE_COLLECTION_ID) epos=STORE_LAST_ELEMENT;
			pval->varray=(const Value*)realloc(const_cast<Value*>(pval->varray),(pval->length+nElts)*sizeof(Value),SES_HEAP);
			if (pval->varray==NULL) {pval->type=VT_ERROR; return RC_NORESOURCES;}
			Value *v=(Value*)findElement(pval,epos);
			if (v==NULL) v=const_cast<Value*>(&pval->varray[pval->length]);
			else {
				if (pv->op!=OP_ADD_BEFORE) v++;
				if (v<&pval->varray[pval->length]) memmove(v+nElts,v,(pval->length-(v-pval->varray))*sizeof(Value));
			}
			if ((flags&MODP_EIDS)==0) eid=getPrefix(ctx)+pval->length;		// find max of existing!!!
			if (pv->type!=VT_ARRAY) {
				if ((flags&MODP_NCPY)!=0) *v=*pv;
				else if ((rc=copyV(*pv,*v,ses))==RC_OK) {v->op=OP_SET; v->eid=eid;}
			} else if ((flags&MODP_NCPY)==0) for (ulong i=0; i<pv->length; ++v,++i) {
				if ((rc=copyV(pv->varray[i],*v,ses))==RC_OK) {v->op=OP_SET; if ((flags&MODP_EIDS)==0) v->eid=eid++;}
			} else {
				memcpy(v,pv->varray,pv->length*sizeof(Value)); free((void*)pv->varray,(HEAP_TYPE)(pv->flags&HEAP_TYPE_MASK));
			}
			pval->length+=nElts;
		}
		if (rc==RC_OK && (flags&MODP_NEID)==0 && pv->type!=VT_ARRAY) pv->eid=eid;
	} else switch (pv->op) {
	case OP_SET:
		if (epos==STORE_COLLECTION_ID || pval->type!=VT_ARRAY && pval->type!=VT_COLLECTION) {
			freeV(*pval); if ((flags&MODP_NCPY)==0) rc=copyV(*pv,*pval,ses); else *pval=*pv;
			if (pval->type!=VT_ARRAY && pval->type!=VT_COLLECTION) pval->eid=(flags&MODP_EIDS)!=0?eid:getPrefix(ctx);
		} else if (pval->type==VT_ARRAY) {
			Value *elt=(Value*)findElement(pval,epos); if (elt==NULL) return RC_NOTFOUND;
			freeV(*elt); if ((flags&MODP_NCPY)==0) rc=copyV(*pv,*elt,ses); else *elt=*pv;
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
				pval->varray=(Value*)realloc(const_cast<Value*>(pval->varray),pval->length*sizeof(Value),SES_HEAP);
				assert(pval->varray!=NULL);
			}
		}
		break;
	case OP_RENAME:
		if (pv->type!=VT_URIID) return RC_TYPE;
		BIN<Value,PropertyID,ValCmp>::find(pv->uid,properties,nProperties,&ins); assert(ins!=NULL);
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
		save=*pval; rc=Expr::calc((ExprOp)pv->op,*pval,pv,2,0,ses); if ((flags&MODP_NCPY)!=0) freeV(*(Value*)pv);
		pval->property=save.property; pval->eid=save.eid; pval->meta=save.meta; pval->op=save.op;
		break;
	}
	return rc;
}

RC PIN::setNotification(bool fReset)
{
	try {
		if (ses==NULL) return RC_NOSESSION; if (ses->inReadTx()) return RC_READTX;
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		if (fReset) mode&=~PIN_NOTIFY; else mode|=PIN_NOTIFY;
		TxGuard txg(ses); return addr.defined()?ctx->queryMgr->setFlag(ses,id,&addr,HOH_NOTIFICATION,fReset):RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IPIN::setNotification(...)\n"); return RC_INTERNAL;}
}

bool PIN::testClassMembership(ClassID classID,const Value *params,unsigned nParams) const
{
	try {
		if (ses==NULL) return false; assert(ses==Session::getSession());
		if (ses->getStore()->inShutdown()) return false;
		TxGuard txg(ses); PINEx pex(this);
		return ses->getStore()->queryMgr->test(&pex,classID,ValueV(params,nParams),true);
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::testClassMembership(ClassID=%08X)\n",classID);}
	return false;
}

bool PIN::defined(const PropertyID *pids,unsigned nProps) const
{
	try {
		if (pids!=NULL) for (unsigned i=0; i<nProps; i++) if (findProperty(pids[i])==NULL) return false;
		return true;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::defined()\n");}
	return false;
}

PIN *PIN::getPIN(const PID& id,VersionID vid,Session *ses,ulong mode)
{
	try {
		if (ses==NULL||ses->getStore()->inShutdown()) return NULL; assert(ses==Session::getSession());
		TxGuard txg(ses); bool fRemote=isRemote(id); PageAddr addr=PageAddr::invAddr; PIN *pin=NULL;
		if (!fRemote && (mode&LOAD_EXT_ADDR)!=0 && addr.convert(OID(id.pid))) ses->setExtAddr(addr);
		if (ses->getStore()->queryMgr->loadPIN(ses,id,pin,mode|LOAD_ENAV,NULL,vid)!=RC_OK) {delete pin; pin=NULL;}
		ses->setExtAddr(PageAddr::invAddr); return pin;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::getPIN(PID="_LX_FM",IdentityID=%08X)\n",id.pid,id.ident);}
	return NULL;
}

void PIN::destroy()
{
	try {delete this;} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IPIN::destroy()\n");}
}

//-----------------------------------------------------------------------------------------------------

void SessionX::setTrace(ITrace *trc)
{
	if (ses!=NULL) {assert(ses==Session::getSession()); ses->setTrace(trc);}
}

void SessionX::changeTraceMode(unsigned mask,bool fReset)
{
	ses->changeTraceMode(mask,fReset); assert(ses==Session::getSession());
}

RC SessionX::mapURIs(unsigned nURIs,URIMap URIs[])
{
	try {
		StoreCtx *ctx=ses->getStore(); assert(ses==Session::getSession());
		if (ctx->inShutdown()) return RC_SHUTDOWN;
		for (unsigned i=0; i<nURIs; i++) {
			const char *URIName=URIs[i].URI; if (URIName==NULL || *URIName=='\0') return RC_INVPARAM;
			if (ses->URIBase!=NULL && ses->lURIBase!=0) {
				size_t lName = strlen(URIName);
				if (!Session::hasPrefix(URIName,lName)) {
					if (ses->lURIBase+lName+1>ses->lURIBaseBuf && (ses->URIBase=(char*)
						ses->realloc(ses->URIBase,ses->lURIBaseBuf=ses->lURIBase+lName+1))==NULL) return RC_NORESOURCES;
					memcpy(ses->URIBase+ses->lURIBase,URIName,lName+1); URIName=ses->URIBase;
				}
			}
			if (ctx->isServerLocked()) return RC_READONLY;
			URI *uri=(URI*)ctx->uriMgr->insert(URIName);
			if (uri==NULL) return RC_NORESOURCES;		// ???
			URIs[i].uid=uri->getID(); uri->release();
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::mapURIs(...)\n"); return RC_INTERNAL;}
}

void SessionX::setInterfaceMode(unsigned md)
{
	try {
		assert(ses==Session::getSession()); ses->itf=md;
		if (ses->getIdentity()==STORE_OWNER) ses->setReplication((md&ITF_REPLICATION)!=0);
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::setInterfaceMode(...)\n");}
}

unsigned SessionX::getInterfaceMode() const
{
	try {assert(ses==Session::getSession()); return ses->itf;} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getInterfaceMode(...)\n");}
	return ~0u;
}

RC SessionX::setURIBase(const char *ns) 
{
	try {
		assert(ses==Session::getSession());
		if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		return ses->setBase(ns,ns!=NULL?strlen(ns):0);
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::setURIBase(...)\n"); return RC_INTERNAL;}
}

RC SessionX::addURIPrefix(const char *name,const char *URIprefix)
{
	try {
		assert(ses==Session::getSession());
		if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		if (name==NULL || *name=='\0' || *name==':') return RC_INVPARAM;
		return ses->setPrefix(name,strlen(name),URIprefix,URIprefix!=NULL?strlen(URIprefix):0);
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::addURIPrefix(...)\n"); return RC_INTERNAL;}
}

void SessionX::setDefaultExpiration(uint64_t de)
{
	try {if (ses!=NULL) {assert(ses==Session::getSession()); ses->setDefaultExpiration(de);}}
	catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::setDefaultExpiration(...)\n");}
}

IdentityID SessionX::storeIdentity(const char *identName,const char *pwd,bool fMayInsert,const unsigned char *cert,unsigned lcert)
{
	try {
		IdentityID iid=STORE_INVALID_IDENTITY; assert(ses==Session::getSession());
		if (!ses->getStore()->inShutdown() && !ses->getStore()->isServerLocked() && checkAdmin()) {
			size_t lpwd=pwd!=NULL?strlen(pwd):0;
			PWD_ENCRYPT epwd((byte*)pwd,lpwd); const byte *enc=pwd!=NULL&&lpwd>0?epwd.encrypted():NULL;
			Identity *ident=(Identity*)ses->getStore()->identMgr->insert(identName,enc,cert,lcert,fMayInsert);
			if (ident!=NULL) {iid=ident->getID(); ident->release();}
		}
		return iid;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::storeIdentity(...)\n");}
	return STORE_INVALID_IDENTITY;
}

IdentityID SessionX::loadIdentity(const char *identName,const unsigned char *pwd,unsigned lPwd,bool fMayInsert,const unsigned char *cert,unsigned lcert)
{
	try {
		IdentityID iid=STORE_INVALID_IDENTITY; assert(ses==Session::getSession());
		if (!ses->getStore()->inShutdown() && !ses->getStore()->isServerLocked() && checkAdmin() && (lPwd==PWD_ENC_SIZE || lPwd==0)) {
			Identity *ident=(Identity*)ses->getStore()->identMgr->insert(identName,lPwd!=0?pwd:NULL,cert,lcert,fMayInsert);
			if (ident!=NULL) {iid=ident->getID(); ident->release();}
		}
		return iid;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::loadIdentity(...)\n");}
	return STORE_INVALID_IDENTITY;
}

RC SessionX::setInsertPermission(IdentityID iid,bool fMayInsert)
{
	try {
		assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		return checkAdmin()?ctx->identMgr->setInsertPermission(iid,fMayInsert):RC_NOACCESS;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::setInsertPermission(...)\n"); return RC_INTERNAL;}
}

RC SessionX::changePassword(IdentityID iid,const char *oldPwd,const char *newPwd)
{
	try {
		assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		return checkAdmin()?ctx->identMgr->changePassword(iid,oldPwd,newPwd):RC_NOACCESS;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::changePassword(...)\n"); return RC_INTERNAL;}
}

RC SessionX::changeCertificate(IdentityID iid,const char *pwd,const unsigned char *cert,unsigned lcert)
{
	try {
		assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		return checkAdmin()?ctx->identMgr->changeCertificate(iid,pwd,cert,lcert):RC_NOACCESS;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::changeCertificate(...)\n"); return RC_INTERNAL;}
}

IdentityID SessionX::getCurrentIdentityID() const
{
	try {assert(ses==Session::getSession()); return ses->getIdentity();}
	catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getCurrentIdentityID(...)\n");}
	return STORE_INVALID_IDENTITY;
}

IdentityID SessionX::getIdentityID(const char *identityName) 
{
	try {
		IdentityID iid=STORE_INVALID_IDENTITY; assert(ses==Session::getSession());
		if (!ses->getStore()->inShutdown()) {
			Identity *ident = (Identity*)ses->getStore()->identMgr->find(identityName);
			if (ident!=NULL) {iid=ident->getID(); ident->release();}
		}
		return iid;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getIdentityID(...)\n");}
	return STORE_INVALID_IDENTITY;
}

RC SessionX::impersonate(const char *identityName) 
{
	try {
		assert(ses==Session::getSession());
		if (!checkAdmin()) return RC_NOACCESS; if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		Identity *ident=(Identity*)ses->getStore()->identMgr->find(identityName);
		if (ident==NULL) return RC_NOTFOUND;
		ses->identity=ident->getID();
		ident->release();
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::impersonate(...)\n"); return RC_INTERNAL;}
}

size_t SessionX::getStoreIdentityName(char buf[],size_t lbuf)
{
	try {return getIdentityName(STORE_OWNER,buf,lbuf);}
	catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getStoreIdentityName(...)\n");}
	return 0;
}

size_t SessionX::getIdentityName(IdentityID iid,char buf[],size_t lbuf)
{
	try {
		size_t len=0; assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return 0;
		Identity *ident = (Identity*)ctx->identMgr->ObjMgr::find(ulong(iid));
		if (ident!=NULL) {
			if (ident->getName()!=NULL) {
				len = strlen(ident->getName());
				if (buf!=NULL && lbuf>0) strncpy(buf,ident->getName(),lbuf-1)[lbuf-1]=0;
			}
			ident->release();
		}
		return len;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getIdentityName(IdentityID=%08X,...)\n",iid);}
	return 0;
}

size_t SessionX::getCertificate(IdentityID iid,unsigned char buf[],size_t lbuf)
{
	try {
		size_t keyLength=0; byte *cert; assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()||!checkAdmin()) return 0;
		Identity *ident = (Identity*)ctx->identMgr->ObjMgr::find(ulong(iid));
		if (ident!=NULL) {
			if ((keyLength=ident->getCertLength())>0 && buf!=NULL && lbuf!=0 && (cert=ident->getCertificate(ses))!=NULL)
				{if (lbuf>keyLength) lbuf=keyLength; memcpy(buf,cert,lbuf); ses->free(cert);}
			ident->release();
		}
		return keyLength;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getIdentityKey(IdentityID=%08X,...)\n",iid);}
	return 0;
}

RC SessionX::changeStoreIdentity(const char *newIdentity)
{
	try {
		assert(ses==Session::getSession()); 
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		return checkAdmin()?ctx->identMgr->rename(STORE_OWNER,newIdentity):RC_NOACCESS;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::changeStoreIdentity(...)\n"); return RC_INTERNAL;}
}

static int __cdecl cmpValues(const void *v1, const void *v2)
{
	const Value *pv1=*(const Value**)v1,*pv2=*(const Value**)v2; int c=cmp3(pv1->property,pv2->property);
	return c!=0?c:pv1->eid==STORE_FIRST_ELEMENT&&pv1<pv2?-1:pv2->eid==STORE_FIRST_ELEMENT?1:
				pv2->eid==STORE_LAST_ELEMENT&&pv2>pv1?-1:pv1->eid==STORE_LAST_ELEMENT?1:pv1<pv2?-1:1;
}

Value *PIN::normalize(const Value *pv,uint32_t& nv,ulong f,ElementID prefix,MemAlloc *ma)
{
	if (pv==NULL || nv==0) return NULL;
	bool fCV=(f&MODE_COPY_VALUES)!=0,fNF=(f&PIN_NO_FREE)!=0,fEID=(f&MODE_FORCE_EIDS)==0;
	Value *pn; unsigned sort=0; HEAP_TYPE ht=ma->getAType(); uint8_t flags=fCV?NO_HEAP:ht;

	for (unsigned j=0; j<nv; j++) {
		Value &v=*(Value*)&pv[j]; v.flags=flags; if (fEID&&!fCV) v.eid=prefix; 
		sort|=j==0||pv[j-1].property<v.property?0:pv[j-1].property==v.property?1:2;
		if (v.type==VT_ARRAY) {
			for (uint32_t k=0; k<v.length; k++) {Value &vv=*(Value*)&v.varray[k]; vv.flags=flags; if (fEID) vv.eid=prefix+k;}
			v.eid=STORE_COLLECTION_ID;
		}
	}

	if (!fCV && sort==0) return (Value*)pv; 
	if ((pn=new(ma) Value[nv])==NULL) return NULL;
	if ((sort&2)==0) memcpy(pn,pv,nv*sizeof(Value));
	else {
		bool fFree=false;
		const Value **ppv=nv<0x1000?(const Value**)alloca(nv*sizeof(Value*)):(fFree=true,new(ma) const Value*[nv]);
		if (ppv==NULL) {ma->free(pn); return NULL;}
		for (unsigned j=0; j<nv; j++) ppv[j]=&pv[j];
		qsort(ppv,nv,sizeof(Value*),cmpValues);
		for (unsigned k=0; k<nv; k++) pn[k]=*ppv[k];
		if (fFree) ma->free(ppv); if (!fCV) ma->free((void*)pv);
	}

	if (fCV) {
		if (!fNF && copyV(pn[0],pn[0],ma)!=RC_OK) {ma->free(pn); return NULL;}
		if (fEID) pn[0].eid=prefix;
	}

	for (unsigned i=1; i<nv; ) {
		Value *pv1=&pn[i-1],*pv2=&pn[i];
		if (fCV) {
			if (!fNF && copyV(*pv2,*pv2,ma)!=RC_OK) {while (i!=0) freeV(pn[--i]); ma->free(pn); return NULL;}
			if (fEID) pv2->eid=prefix;
		}
		if (pv1->property<pv2->property) i++;
		else if (pv1->type==VT_COLLECTION || pv2->type==VT_COLLECTION) {
			// ???
			i++;
		} else {
			ulong l1=pv1->type==VT_ARRAY?pv1->length:1,l2=0,ii=0; assert(pv1->property==pv2->property);
			do l2+=pv2[ii].type==VT_ARRAY?pv2[ii].length:1;
			while (++ii+i<nv && pv2[ii].property==pv1->property);
			Value *vals=pv1->type!=VT_ARRAY||fCV?(Value*)ma->malloc((l1+l2)*sizeof(Value)):
				(Value*)ma->realloc(const_cast<Value*>(pv1->varray),(l1+l2)*sizeof(Value));
			if (pv1->type!=VT_ARRAY) {vals[0]=*pv1; vals[0].property=STORE_INVALID_PROPID;}
			else if (fCV) memcpy(vals,pv1->varray,l1*sizeof(Value));
			pv1->varray=vals; pv1->length=l1+l2; pv1->type=VT_ARRAY; pv1->eid=STORE_COLLECTION_ID;
			pv1->flags=pv1->flags&~HEAP_TYPE_MASK|ht; vals+=l1; ulong ll; ElementID eid=prefix+l1;
			do {
				ll=pv2->type==VT_ARRAY?pv2->length:1; pv1->meta|=pv2->meta;
				if (!fCV || fNF) {
					memcpy(vals,pv2->type==VT_ARRAY?pv2->varray:pv2,ll*sizeof(Value));
					if (fEID) for (ulong k=0; k<ll; k++) vals[k].eid=eid++;
					if (!fCV && pv2->type==VT_ARRAY && (pv2->flags&HEAP_TYPE_MASK)!=NO_HEAP) 
						{MVStoreKernel::free(const_cast<Value*>(pv2->varray),(HEAP_TYPE)(pv2->flags&HEAP_TYPE_MASK)); pv2->flags=NO_HEAP;}
				} else for (ulong j=0; j<ll; j++) if (copyV(pv2->type==VT_ARRAY?pv2->varray[j]:*pv2,vals[j],ma)!=RC_OK) {
					//...free
					return NULL;
				} else if (fEID) vals[j].eid=eid++;
				pv2++; vals+=ll; --nv; assert(nv>=i);
			} while ((l2-=ll)!=0);
			if (nv>i && ii>0) memmove(pv1+1,pv1+ii+1,(nv-i)*sizeof(Value));
		}
	}
	return pn;
}

RC SessionX::createPIN(PID& res,const Value values[],unsigned nValues,unsigned md,const AllocCtrl *actrl)
{
	try {
		uint32_t nv=(uint32_t)nValues; assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		Value *pv=PIN::normalize(values,nv,MODE_COPY_VALUES|PIN_NO_FREE,ctx->getPrefix(),ses);
		if (pv==NULL && values!=NULL && nv!=0) return RC_NORESOURCES;
		ulong pm=md&(PIN_NO_REPLICATION|PIN_REPLICATED|PIN_NO_INDEX|PIN_HIDDEN|PIN_NOTIFY);
		if ((ses->itf&ITF_DEFAULT_REPLICATION)!=0 && (md&PIN_NO_REPLICATION)==0) pm|=PIN_REPLICATED;
		PIN pn(ses,PIN::defPID,PageAddr::invAddr,pm,pv,nv),*ppn=&pn; TxGuard txg(ses);
		RC rc=ctx->queryMgr->commitPINs(ses,&ppn,1,md,ValueV(NULL,0),actrl);
		if (rc==RC_OK) {
			res=pn.id;
			if ((md&MODE_NO_EID)==0 && pv!=NULL) for (unsigned i=0; i<nv; i++) if (pv[i].type==VT_ARRAY) {
				if (values[i].property==pv[i].property && values[i].type==VT_ARRAY && values[i].length==pv[i].length)
					for (ulong j=0; j<pv[i].length; j++) values[i].varray[j].eid=pv[i].varray[j].eid;
				else 
					for (ulong j=0,k=0; j<nValues && k<pv[i].length; j++) if (values[j].property==pv[i].property)
						if (values[j].type!=VT_ARRAY) values[j].eid=pv[i].varray[k++].eid;
						else for (ulong m=0; m<values[j].length; m++) {assert(k<pv[i].length); values[j].varray[m].eid=pv[i].varray[k++].eid;}
			}
		}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::createPIN(...)\n"); return RC_INTERNAL;}
}

IPIN *SessionX::createUncommittedPIN(Value* values,unsigned nValues,unsigned md,const PID *orig)
{
	try {
		assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return NULL;
		if ((ses->itf&(ITF_DEFAULT_REPLICATION|ITF_REPLICATION))!=0 && (md&PIN_NO_REPLICATION)==0) md|=PIN_REPLICATED;
		if (orig!=NULL && (orig->ident==STORE_INVALID_IDENTITY||orig->pid==STORE_INVALID_PID)) orig=NULL;
		if (ses->isRestore()) {if (orig==NULL) return NULL; else if ((md&MODE_DELETED)!=0) md|=PIN_DELETED;}
		else {
			if (orig!=NULL && (ses->itf&ITF_CATCHUP)==0) {
				if (!isRemote(*orig)) {report(MSG_ERROR,"Can't load PIN "_LX_FM" in non-loading session\n",orig->pid); return NULL;}
				if (!ses->isReplication()) {report(MSG_ERROR,"Can't create replicated PIN "_LX_FM":%08X in non-replication session\n",orig->pid,orig->ident); return NULL;}
				if ((md&MODE_FORCE_EIDS)==0 && nValues>0) {report(MSG_ERROR,"No MODE_FORCE_EIDS for replicated PIN "_LX_FM":%08X\n",orig->pid,orig->ident); return NULL;}
			}
			md&=~(MODE_DELETED|PIN_DELETED);
		}
		PIN *pin = new(ses) PIN(ses,orig==NULL?PIN::defPID:*orig,PageAddr::invAddr,md);
		if (values!=NULL && nValues!=0) {
			if (pin!=NULL) {
				uint32_t nv=(uint32_t)nValues; Value *pv=PIN::normalize(values,nv,md,ctx->getPrefix(),ses);
				if (pv==NULL && values!=NULL && nv!=0) {pin->destroy(); pin=NULL;} else {pin->properties=pv; pin->nProperties=nv;}
			} else if ((md&MODE_COPY_VALUES)==0) {
				for (unsigned i=0; i<nValues; i++) {
					if (values[i].type==VT_ARRAY) for (ulong j=0; j<values[i].length; j++) values[i].varray[i].flags=SES_HEAP;
					values[i].flags=SES_HEAP; freeV(values[i]);
				}
				ses->free(values);
			}
		}
		return pin;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::createUncommittedPIN(...)\n");}
	return NULL;
}

RC SessionX::commitPINs(IPIN *const *pins,unsigned nPins,unsigned md,const AllocCtrl *actrl,const Value *params,unsigned nParams)
{
	try {
		assert(ses==Session::getSession()); 
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		TxGuard txg(ses); return ctx->queryMgr->commitPINs(ses,(PIN*const*)pins,nPins,md,ValueV(params,nParams),actrl);	// MODE_ENAV???
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::commitPINs(...)\n"); return RC_INTERNAL;}
}

RC SessionX::modifyPIN(const PID& id,const Value *values,unsigned nValues,unsigned md,const ElementID *eids,unsigned *pNFailed,const Value *params,unsigned nParams)
{
	try {
		PageAddr addr; assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		if (!isRemote(id) && addr.convert(id.pid)) ses->setExtAddr(addr); TxGuard txg(ses);
		RC rc=ctx->queryMgr->modifyPIN(ses,id,values,nValues,NULL,ValueV(params,nParams),NULL,md,eids,pNFailed);
		ses->setExtAddr(PageAddr::invAddr); return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::modifyPIN(...)\n"); return RC_INTERNAL;}
}

RC SessionX::deletePINs(IPIN **pins,unsigned nPins,unsigned md)
{
	try {
		assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		PID *pids=(PID*)alloca(nPins*sizeof(PID)); if (pids==NULL) return RC_NORESOURCES;
		for (unsigned j=0; j<nPins; j++) pids[j]=pins[j]->getPID();
		TxGuard txg(ses); RC rc=ctx->queryMgr->deletePINs(ses,(PIN**)pins,pids,nPins,md|MODE_CLASS);
		if (rc==RC_OK) for (unsigned i=0; i<nPins; i++) {pins[i]->destroy(); pins[i]=NULL;}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::deletePINs(...)\n"); return RC_INTERNAL;}
}

RC SessionX::deletePINs(const PID *pids,unsigned nPids,unsigned md)
{
	try {
		assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		TxGuard txg(ses); return ctx->queryMgr->deletePINs(ses,NULL,pids,nPids,md|MODE_CLASS);
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::deletePINs(...)\n"); return RC_INTERNAL;}
}

RC SessionX::undeletePINs(const PID *pids,unsigned nPids)
{
	try {
		assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		TxGuard txg(ses); return ctx->queryMgr->undeletePINs(ses,pids,nPids);
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::undeletePINs(...)\n"); return RC_INTERNAL;}
}

RC SessionX::setPINAllocationParameters(const AllocCtrl *ac)
{
	try {assert(ses==Session::getSession()); return ses->getStore()->inShutdown()?RC_SHUTDOWN:ses->setAllocCtrl(ac);}
	catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::setPINAllocationParameters(...)\n"); return RC_INTERNAL;}
}

RC SessionX::setIsolationLevel(TXI_LEVEL txl)
{
	try {assert(ses==Session::getSession()); return ses->getStore()->inShutdown()?RC_SHUTDOWN:ses->isRestore()?RC_OTHER:ses->setIsolationLevel(txl);}
	catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::setIsolationLevel()\n"); return RC_INTERNAL;}
}

RC SessionX::startTransaction(TX_TYPE txt,TXI_LEVEL txl)
{
	try {
		assert(ses==Session::getSession()); 
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; 
		if (txt!=TXT_READONLY && ctx->isServerLocked()) return RC_READONLY;
		return ses->isRestore()?RC_OTHER:ctx->txMgr->startTx(ses,txt,txl);
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::startTransaction()\n"); return RC_INTERNAL;}
}

RC SessionX::commit(bool fAll)
{
	try {assert(ses==Session::getSession()); return ses->isRestore()?RC_OTHER:ses->getStore()->txMgr->commitTx(ses,fAll);}
	catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::commit()\n"); return RC_INTERNAL;}
}

RC SessionX::rollback(bool fAll)
{
	try {assert(ses==Session::getSession()); return ses->isRestore()?RC_OTHER:ses->getStore()->txMgr->abortTx(ses,fAll);}
	catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::rollback()\n"); return RC_INTERNAL;}
}

RC SessionX::getURI(uint32_t id,char buf[],size_t& lbuf)
{
	try {
		assert(ses==Session::getSession());
		if (id==STORE_INVALID_PROPID || buf==NULL || lbuf==0) return RC_INVPARAM;
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN;
		if (id<=PROP_SPEC_MAX) {
			size_t l=min(lbuf,ses->fStdOvr?sizeof(STORE_STD_URI_PREFIX):sizeof(STORE_STD_QPREFIX))-1;
			memcpy(buf,ses->fStdOvr?STORE_STD_URI_PREFIX:STORE_STD_QPREFIX,l);
			if (lbuf>l) {
				size_t ll=min(lbuf-l-1,Classifier::builtinURIs[id].lname);
				memcpy(buf+l,Classifier::builtinURIs[id].name,ll); l+=ll;
			}
			buf[lbuf=l]=0;
		} else {
			URI *uri=(URI*)ctx->uriMgr->ObjMgr::find(id); if (uri==NULL) {lbuf=0; return RC_NOTFOUND;}
			const char *s=uri->getURI();
			if (s==NULL) lbuf=0;
			else if (buf!=NULL && lbuf>0) {
				size_t l=strlen(s);
				if (!ses->fStdOvr && l>sizeof(STORE_STD_URI_PREFIX)-1 && memcmp(s,STORE_STD_URI_PREFIX,sizeof(STORE_STD_URI_PREFIX)-1)==0) {
					size_t ll=min(lbuf,sizeof(STORE_STD_QPREFIX))-1; memcpy(buf,STORE_STD_QPREFIX,ll);
					if (lbuf>ll) {size_t l2=min(lbuf-l-1,l-sizeof(STORE_STD_URI_PREFIX)+1); memcpy(buf+ll,s+sizeof(STORE_STD_URI_PREFIX)-1,l2); ll+=l2;}
					buf[lbuf=ll]=0;
				} else {memcpy(buf,s,min(l+1,lbuf)); if (lbuf>l) lbuf=l; else buf[--lbuf]=0;}
			}
			uri->release();
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::getURI(URIID=%08X,...)\n",id); return RC_INTERNAL;}
}

IPIN *SessionX::getPIN(const PID& id,unsigned md) 
{
	try {
		assert(ses==Session::getSession());
		if (ses->getStore()->inShutdown()) return NULL;
		PIN *pin=PIN::getPIN(id,STORE_CURRENT_VERSION,ses,md|LOAD_EXT_ADDR|LOAD_ENAV);
		return pin;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getPIN(PID="_LX_FM",IdentityID=%08X)\n",id.pid,id.ident);}
	return NULL;
}

IPIN *SessionX::getPIN(const Value& v,unsigned md) 
{
	try {
		assert(ses==Session::getSession());
		if (ses->getStore()->inShutdown()) return NULL;
		PIN *pin=v.type==VT_REFID?PIN::getPIN(v.id,/*(v.meta&META_PROP_FIXEDVERSION)!=0?v.vpid.vid:*/STORE_CURRENT_VERSION,ses,md|LOAD_EXT_ADDR|LOAD_ENAV):NULL;
		return pin;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getPIN(Value&)\n");}
	return NULL;
}

IPIN *SessionX::getPINByURI(const char *uri,unsigned md)
{
	try {
#if 0
		Value w; w.set(uri); PID id; assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return NULL;
		PIN *pin=uri!=NULL && ctx->uriMgr->URItoPID(w,id)==RC_OK?PIN::getPIN(id,STORE_CURRENT_VERSION,ses,md|mode):NULL;
		return pin;
#else
		return NULL;
#endif
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getPINByURI()\n");}
	return NULL;
}

RC SessionX::getValues(Value *pv,unsigned nv,const PID& id)
{
	try {
		assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(ses); PageAddr addr; if (addr.convert(OID(id.pid))) ses->setExtAddr(addr);
		RC rc=ctx->queryMgr->loadValues(pv,nv,id,ses,LOAD_EXT_ADDR|LOAD_ENAV);
		ses->setExtAddr(PageAddr::invAddr); return rc;
	} catch (RC rc) {return rc;} 
	catch (...) {report(MSG_ERROR,"Exception in ISession::getValues(PID="_LX_FM",IdentityID=%08X)\n",id.pid,id.ident); return RC_INTERNAL;}
}

RC SessionX::getValue(Value& res,const PID& id,PropertyID pid,ElementID eid)
{
	try {
		assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(ses); PageAddr addr; if (addr.convert(OID(id.pid))) ses->setExtAddr(addr);
		RC rc=ctx->queryMgr->loadValue(ses,id,pid,eid,res,LOAD_EXT_ADDR|LOAD_ENAV);
		ses->setExtAddr(PageAddr::invAddr); return rc;
	} catch (RC rc) {return rc;}
	catch (...) {report(MSG_ERROR,"Exception in ISession::getValue(PID="_LX_FM",IdentityID=%08X,PropID=%08X)\n",id.pid,id.ident,pid); return RC_INTERNAL;}
}

RC SessionX::getValue(Value& res,const PID& id)
{
	try {
		assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(ses); PageAddr addr; if (addr.convert(OID(id.pid))) ses->setExtAddr(addr);
		RC rc=ctx->queryMgr->getPINValue(id,res,LOAD_EXT_ADDR|LOAD_ENAV,ses);
		ses->setExtAddr(PageAddr::invAddr); return rc;
	} catch (RC rc) {return rc;}
	catch (...) {report(MSG_ERROR,"Exception in ISession::getValue(PID="_LX_FM",IdentityID=%08X)\n",id.pid,id.ident); return RC_INTERNAL;}
}

bool SessionX::isCached(const PID& id)
{
	try {
		assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return false;
		return isRemote(id)?ctx->netMgr->isCached(id):true;	// check correct local addr
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::isCached(PID="_LX_FM",IdentityID=%08X)\n",id.pid,id.ident);}
	return false;
}

unsigned SessionX::getStoreID(const PID& id)
{
	return ushort(id.pid>>48);
}

unsigned SessionX::getLocalStoreID()
{
	try {assert(ses==Session::getSession()); return ses->getStore()->storeID;}
	catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getLocalStoreID()\n");}
	return ~0u;
}

RC SessionX::getClassID(const char *className,ClassID& cid)
{
	try {
		assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN;
		URI *uri=(URI*)ctx->uriMgr->find(className); if (uri==NULL) return RC_NOTFOUND; cid=uri->getID(); uri->release();
		Class *cls=ctx->classMgr->getClass(cid); if (cls==NULL) return RC_NOTFOUND; else {cls->release(); return RC_OK;}
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::getClassID()\n"); return RC_INTERNAL;}
}

RC SessionX::enableClassNotifications(ClassID cid,unsigned notifications)
{
	try {
		assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN;
		Class *cls=NULL; ctx->classMgr->initClasses(ses);
		if ((cls=ctx->classMgr->getClass(cid))==NULL) return RC_NOTFOUND;
		RC rc=ctx->classMgr->enable(ses,cls,notifications); cls->release(); return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::enableClassNotifications()\n"); return RC_INTERNAL;}
}

RC SessionX::rebuildIndices(const ClassID *cidx,unsigned nClasses)
{
	try {
		assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		if (!checkAdmin()) return RC_NOACCESS;
		//...
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::rebuildFamilyIndices()\n"); return RC_INTERNAL;}
}

RC SessionX::rebuildIndexFT()
{
	try {
		assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		return checkAdmin()?ctx->ftMgr->rebuildIndex(ses):RC_NOACCESS;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::rebuildIndexFT()\n"); return RC_INTERNAL;}
}

RC SessionX::createIndexNav(ClassID cid,IndexNav *&nav)
{
	try {
		assert(ses==Session::getSession()); nav=NULL;
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN;
		Class *cls=ctx->classMgr->getClass(cid); if (cls==NULL) return RC_NOTFOUND;
		ClassIndex *cidx=cls->getIndex(); RC rc=RC_OK;
		if (cidx!=NULL /*&& !qry->vars[0].condIdx->isExpr() && (qry->vars[0].condIdx->pid==pid || pid==STORE_INVALID_PROPID)*/) {
			if ((nav=new(cidx->getNSegs(),ses) IndexNavImpl(ses,cidx))==NULL) rc=RC_NORESOURCES;
		} else {
		//...
			rc=RC_INVPARAM;
		}
		if (cidx==NULL || rc!=RC_OK) cls->release();
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::createIndexNav()\n"); return RC_INTERNAL;}
}

RC SessionX::listValues(ClassID cid,PropertyID pid,IndexNav *&ven)
{
	try {
		assert(ses==Session::getSession()); ven=NULL; if (!checkAdmin()) return RC_NOACCESS;
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN;
		Class *cls=ctx->classMgr->getClass(cid); if (cls==NULL) return RC_NOTFOUND;
		ClassIndex *cidx=cls->getIndex(); RC rc=RC_OK;
		if (cidx!=NULL /*&& !qry->vars[0].condIdx->isExpr() && (qry->vars[0].condIdx->pid==pid || pid==STORE_INVALID_PROPID)*/) {
			if ((ven=new(cidx->getNSegs(),ses) IndexNavImpl(ses,cidx,/*qry->vars[0].condIdx->*/pid))==NULL) rc=RC_NORESOURCES;
		} else {
		//...
			rc=RC_INVPARAM;
		}
		if (cidx==NULL || rc!=RC_OK) cls->release();
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::listValues()\n"); return RC_INTERNAL;}
}

RC SessionX::listWords(const char *query,StringEnum *&sen)
{
	try {
		assert(ses==Session::getSession()); sen=NULL;
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(ses); return checkAdmin()?ctx->ftMgr->listWords(ses,query,sen):RC_NOACCESS;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::listWords()\n"); return RC_INTERNAL;}
}

RC SessionX::getClassInfo(ClassID cid,IPIN *&ret)
{
	try {
		ret=NULL; StoreCtx *ctx=ses->getStore(); assert(ses==Session::getSession()); if (ctx->inShutdown()) return RC_SHUTDOWN;
		Class *cls=ctx->classMgr->getClass(cid); if (cls==NULL) return RC_NOTFOUND;
		PID id=cls->getPID(); PINEx cb(ses,id); cb=cls->getAddr(); cls->release(); PIN *pin=NULL;
		RC rc=ctx->queryMgr->loadPIN(ses,id,pin,LOAD_ENAV,&cb); if (rc==RC_OK) rc=ctx->queryMgr->getClassInfo(ses,pin);
		ret=pin; return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::getClassInfo()\n"); return RC_INTERNAL;}
}

RC SessionX::copyValue(const Value& src,Value& dest)
{
	try {assert(ses==Session::getSession()); src.flags=NO_HEAP; return ses->getStore()->inShutdown()?RC_SHUTDOWN:copyV(src,dest,ses);}
	catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::copyValue()\n"); return RC_INVPARAM;}
}

RC SessionX::convertValue(const Value& src,Value& dest,ValueType vt)
{
	try {assert(ses==Session::getSession()); src.flags=NO_HEAP; return ses->getStore()->inShutdown()?RC_SHUTDOWN:convV(src,dest,vt);}
	catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::convertValue()\n"); return RC_INVPARAM;}
}

int SessionX::compareValues(const Value& v1,const Value& v2,bool fNCase)
{
	try {
		assert(ses==Session::getSession()); return ses->getStore()->inShutdown()?-1000:cmp(v1,v2,fNCase?CND_NCASE:0);
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::compareValues()\n");}
	return -100;
}

void SessionX::freeValues(Value *vals,unsigned nvals)
{
	try {assert(ses==Session::getSession()); freeV(vals,nvals,ses);} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::freeValues()\n");}
}

void SessionX::freeValue(Value& val)
{
	try {assert(ses==Session::getSession()); freeV(val);} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::freeValue()\n");}
}

void SessionX::setTimeZone(int64_t tzShift)
{
	try {assert(ses==Session::getSession()); ses->tzShift=tzShift;} catch (RC) {}  
	catch (...) {report(MSG_ERROR,"Exception in ISession::setTimeZone()");}
}

RC SessionX::convDateTime(uint64_t dt,DateTime& dts,bool fUTC) const
{
	assert(ses==Session::getSession());
	return MVStoreKernel::convDateTime(ses,dt,dts,fUTC);
}

RC SessionX::convDateTime(const DateTime& dts,uint64_t& dt,bool fUTC) const
{
	assert(ses==Session::getSession());
	return MVStoreKernel::convDateTime(ses,dts,dt,fUTC);
}

RC SessionX::setStopWordTable(const char **words,uint32_t nWords,PropertyID pid,bool fOverwriteDefault,bool fSessionOnly)
{
	try {
		assert(ses==Session::getSession()); if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		//...
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::setStopWordTable()");}
	return RC_INTERNAL;
}

#include "fsmgr.h"

RC SessionX::reservePage(uint32_t pageID)
{
	try {return ses->isRestore()?ses->ctx->fsMgr->reservePage((PageID)pageID):RC_NOACCESS;}
	catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::reservePage(%08X)\n",pageID); return RC_INTERNAL;}
}

void *SessionX::alloc(size_t s)
{
	return ses->malloc(s);
}

void *SessionX::realloc(void *p,size_t s)
{
	return ses->realloc(p,s);
}

void SessionX::free(void *p)
{
	return ses->free(p);
}

bool SessionX::login(const char *id,const char *pwd)
{
	Identity *ident=id!=NULL&&*id!='\0'?(Identity*)ses->getStore()->identMgr->find(id):
					(Identity*)ses->getStore()->identMgr->ObjMgr::find(STORE_OWNER);
	if (ident==NULL) return false;
	const byte *spwd; bool fOK=true;
	if ((spwd=ident->getPwd())!=NULL) {
		if (pwd==NULL||*pwd=='\0') fOK=false;
		else {PWD_ENCRYPT pwd_enc((const byte*)pwd,strlen(pwd),spwd); fOK=pwd_enc.isOK();}
	} else if (pwd!=NULL && *pwd!='\0') fOK=false;
	if (fOK) ses->setIdentity(ident->getID(),ident->mayInsert());
	ident->release();
	return fOK;
}

ISession *SessionX::clone(const char *id) const
{
	try {
		if (ses==NULL||ses->isRestore()) return NULL; assert(ses==Session::getSession());
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return NULL;
		IdentityID iid=ses->getIdentity(); Identity *ident=NULL; bool fInsert=ses->mayInsert();
		if (id!=NULL) {
			if (iid!=STORE_OWNER || (ident=(Identity*)ctx->identMgr->find(id))==NULL) return NULL;
			iid=ident->getID(); fInsert=ident->mayInsert(); ident->release();
		}
		Session *ns=Session::createSession(ctx); if (ns==NULL) return NULL;
		ns->setIdentity(iid,fInsert); return new(ns) SessionX(ns);
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::clone()\n");}
	return NULL;
}

void SessionX::terminate() 
{
	try {assert(ses==Session::getSession()); ses->free((char*)ses->URIBase); delete this; Session::terminateSession();}
	catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::terminate()\n");}
}

RC SessionX::attachToCurrentThread()
{
	return this!=NULL && ses!=NULL ? ses->attach() : RC_INVPARAM;
}

RC SessionX::detachFromCurrentThread()
{
	return this!=NULL && ses!=NULL ? ses->detach() : RC_INVPARAM;
}

ISession *ISession::startSession(MVStoreCtx ctx,const char *ident,const char *pwd)
{
	try {
		if (ctx==NULL||ctx->inShutdown()||ctx->theCB->state==SST_RESTORE) return NULL;
		Session *s=Session::createSession(ctx); if (s==NULL) return NULL;
		SessionX *ses=new(s) SessionX(s);
		if (ses!=NULL && !ses->login(ident,pwd)) {ses->terminate(); ses=NULL;}
		return ses;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::startSession()\n");}
	return NULL;
}
