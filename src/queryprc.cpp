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

#include "txmgr.h"
#include "lock.h"
#include "queryprc.h"
#include "netmgr.h"
#include "stmt.h"
#include "parser.h"
#include "expr.h"
#include "maps.h"
#include "blob.h"

using namespace AfyKernel;

QueryPrc::QueryPrc(StoreCtx *c,IStoreNotification *notItf) 
	: notification(notItf),ctx(c),bigThreshold(c->bufMgr->getPageSize()*SKEW_PAGE_PCT/100),calcProps(NULL),nCalcProps(0)
{
}

const static char *stmtOpName[STMT_OP_ALL] = {"QUERY","INSERT","UPDATE","DELETE","UNDELETE","START","COMMIT","ROLLBACK","PERSIST"};

RC Session::execute(const char *str,size_t lstr,char **result,const URIID *ids,unsigned nids,const Value *params,unsigned nParams,CompilationError *ce,
																								uint64_t *nProcessed,unsigned nProcess,unsigned nSkip)
{
	try {
		if (ce!=NULL) {memset(ce,0,sizeof(CompilationError)); ce->msg="";}
		if (str==NULL||lstr==0) return RC_INVPARAM; if (ctx->inShutdown()) return RC_SHUTDOWN;
		SInCtx in(this,str,lstr,ids,nids); RC rc=RC_OK;
		try {in.exec(params,nParams,result,nProcessed,nProcess,nSkip); in.checkEnd(true);}
		catch (SynErr sy) {in.getErrorInfo(RC_SYNTAX,sy,ce); rc=RC_SYNTAX;}
		catch (RC rc2) {in.getErrorInfo(rc=rc2,SY_ALL,ce);}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::execute()\n"); return RC_INTERNAL;}
}

RC Stmt::execute(ICursor **pResult,const Value *pars,unsigned nPars,unsigned nProcess,unsigned nSkip,unsigned md,uint64_t *nProcessed,TXI_LEVEL txl) const
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION; if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(ses); ses->resetAbortQ(); ValueV vpar(pars,nPars); EvalCtx ectx(ses,NULL,0,NULL,0,&vpar,1,NULL,NULL,op==STMT_INSERT||op==STMT_UPDATE?ECT_INSERT:ECT_QUERY);
		RC rc=execute(ectx,NULL,nProcess,nSkip,md,nProcessed,txl,(Cursor**)pResult); ses->releaseAllLatches(); return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::execute()\n"); return RC_INTERNAL;}
}

RC Stmt::execute(const EvalCtx& ectx,Value *res,unsigned nProcess,unsigned nSkip,unsigned md,uint64_t *nProcessed,TXI_LEVEL txl,Cursor **pResult) const
{
	if (res!=NULL) res->setEmpty(); if (nProcessed!=NULL) *nProcessed=0ULL; if (pResult!=NULL) *pResult=NULL;
	RC rc=RC_OK; QueryOp *qop=NULL; uint64_t cnt=0; STMT_OP sop=op; const EvalCtx *ec; StoreCtx *ctx=ectx.ses->getStore(); 
	switch (op) {
	case STMT_START_TX:
		if ((mode&MODE_READONLY)==0 && ctx->isServerLocked()) return RC_READONLY;
		return ectx.ses->isRestore()?RC_OTHER:ctx->txMgr->startTx(ectx.ses,(mode&MODE_READONLY)!=0?TXT_READONLY:TXT_READWRITE,txl);
	case STMT_COMMIT:
		return ectx.ses->isRestore()?RC_OTHER:ctx->txMgr->commitTx(ectx.ses,(mode&MODE_ALL)!=0);
	case STMT_ROLLBACK:
		return ectx.ses->isRestore()?RC_OTHER:ctx->txMgr->abortTx(ectx.ses,(mode&MODE_ALL)!=0?TXA_ALL:TXA_EXTERNAL);
	case STMT_PERSIST:
		for (ec=&ectx; ec!=NULL; ec=ec->stack) if (ec->env!=NULL && ec->nEnv!=0 && ec->env[0]!=NULL && (ec->env[0]->mode&PIN_TRANSIENT)!=0)
			{ec->env[0]->mode&=~PIN_TRANSIENT; if ((mode&MODE_ALL)==0) break;}
		return RC_OK;
	case STMT_QUERY:
		md&=MODE_HOLD_RESULT|MODE_ALL_WORDS|MODE_DELETED|MODE_FOR_UPDATE|MODE_COPY_VALUES|MODE_PID;
		if (ectx.ses->inReadTx()) md&=~MODE_FOR_UPDATE; break;
	case STMT_INSERT: case STMT_UPDATE: //case STMT_DELETE: case STMT_UNDELETE:
		if (ectx.ect!=ECT_INSERT && ectx.ect!=ECT_ACTION) return RC_INVOP;
	default:
		if (ectx.ses->inReadTx()) return RC_READTX; if (ctx->isServerLocked()) return RC_READONLY;
		md&=MODE_FORCE_EIDS|MODE_CHECK_STAMP|MODE_ALL_WORDS|MODE_DELETED|MODE_PURGE|MODE_COPY_VALUES|MODE_TEMP_ID|MODE_PID;
		md|=op==STMT_INSERT?MODE_RAW:op==STMT_UNDELETE?MODE_FOR_UPDATE|MODE_DELETED:MODE_FOR_UPDATE;
		if (top==NULL) {
			if (with.vals!=NULL && with.nValues!=0) {
				// put to ectx
			}
			Value w,*ids=&w; w.set(PIN::noPID); PIN *pin; unsigned cnt=0;
			if (op==STMT_INSERT) {
				if ((mode&MODE_MANY_PINS)!=0 && nValues>1 && (ids=new(ectx.ses) Value[nValues+nNested])==NULL) return RC_NORESOURCES;
				if ((rc=insert(ectx,ids,cnt))==RC_OK && cnt!=0) {
					if (res!=NULL) {
						if (ids==&w) {if ((ids=new(ectx.ses) Value)==NULL) return RC_NORESOURCES; *ids=w;}
						res->set(ids,cnt); setHT(*res,SES_HEAP);
					} else if (pResult!=NULL && ids!=&w) {w.set(ids,(uint32_t)cnt); setHT(w,SES_HEAP);}
					if (nProcessed!=NULL) *nProcessed=cnt; 
				}
			} else if (op==STMT_UPDATE) {
				if (ectx.env==NULL || pmode>=ectx.nEnv || (pin=ectx.env[pmode])==NULL) return RC_NOTFOUND;
				EvalCtx ectx2(ectx.ses,ectx.env,ectx.nEnv,&pin,1,ectx.params,ectx.nParams,ectx.stack,ectx.ma);
				if ((rc=ctx->queryMgr->modifyPIN(ectx2,pin->id,vals,nValues,pin->fPINx!=0?(PINx*)pin:NULL,pin->fPINx==0?pin:NULL,mode|md|MODE_NO_EID))==RC_OK) {
					if (res!=NULL) {if (pin->addr.defined()) res->set(ectx.env[0]->id); else res->set(pin);} else if (pResult!=NULL) w.set(pin->id);		//??? what if inmem?
					if (nProcessed!=NULL) *nProcessed=1;
				}
			} else rc=RC_INVOP;
			if (rc!=RC_OK || pResult==NULL) return rc;
			QCtx *qc=new(ectx.ses) QCtx(ectx.ses); sop=STMT_QUERY; 
			if (qc==NULL || (qop=new(ectx.ses) ExprScan(qc,w,QO_RAW))==NULL) {freeV(w); return RC_NORESOURCES;}
		} else if ((mode&MODE_MANY_PINS)!=0) return RC_INTERNAL;
		break;
	}
	if (qop==NULL) {QBuildCtx qctx(ectx.ses,(EvalCtx*)&ectx,this,nSkip,md); if ((rc=qctx.process(qop))!=RC_OK && rc!=RC_EOF) {delete qop; return rc;}}
	if (txl==TXI_DEFAULT) txl=TXI_REPEATABLE_READ;
	if (pResult!=NULL || res!=NULL) {
		Value *values=NULL; unsigned nVals=0;
		if (sop==STMT_INSERT || sop==STMT_UPDATE) {assert((mode&MODE_MANY_PINS)==0); if ((rc=copyV(vals,nVals=nValues,values,ectx.ses))!=RC_OK) return rc;}
		Cursor *result=new(ectx.ses) Cursor(ectx,qop,nProcess,mode|md,values,nVals,sop,top!=NULL?top->stype:SEL_PINSET,txl>=TXI_REPEATABLE_READ);
		if (result==NULL) {freeV(values,nVals,ectx.ses); rc=RC_NORESOURCES;}
		else {
			qop->setECtx(&result->ectx); qop=NULL;
			if ((rc=result->connect())!=RC_OK) {result->destroy(); freeV(values,nVals,ectx.ses);}
			else {
				if (with.vals!=NULL && with.nValues!=0) {
					if ((mode&MODE_COPY_VALUES)!=0) {
						//???
					}
					if (result->ectx.params!=NULL && result->ectx.nParams!=0) {
						//???
					} else {
						//???
					}
				}
				if (res!=NULL) {
					CursorNav *cn;
					switch (result->selectType()) {
					case SEL_CONST: case SEL_COUNT: case SEL_VALUE: case SEL_DERIVED:
						rc=result->next(*res); result->destroy(); break;
					default:
						if ((cn=new(ectx.ma) CursorNav(result,(md&MODE_PID)!=0))==NULL) {result->destroy(); return RC_NORESOURCES;}	// page locked???
						res->set(cn); setHT(*res,SES_HEAP); if (op==STMT_INSERT && (mode&MODE_PART)!=0) res->meta|=META_PROP_PART;
						break;
					}
				} else *pResult=result;
			}
		}
	} else if (op==STMT_QUERY) return nProcessed!=NULL?ctx->queryMgr->count(qop,*nProcessed,nProcess):RC_INVPARAM;
	else if (rc!=RC_EOF) {
		PINx qr(ectx.ses),*pqr=&qr; qop->connect(&pqr); TxSP tx(ectx.ses); if ((rc=tx.start(txl))!=RC_OK) return rc;
		if (with.vals!=NULL && with.nValues!=0) qop->getQCtx()->vals[QV_PARAMS]=with;
		EvalCtx ectx2(ectx.ses,ectx.env,ectx.nEnv,(PIN**)&pqr,1,qop->getQCtx()->vals,QV_ALL,ectx.stack,ectx.ma);
		while (rc==RC_OK && (nProcess==~0u || cnt<nProcess) && (rc=qop->next())==RC_OK) {
			if ((qr.epr.flags&PINEX_DERIVED)!=0) {if (qr.properties==NULL && (rc=qop->loadData(qr,NULL,0))!=RC_OK) break;}
			else if (qr.id.pid==STORE_INVALID_PID && (rc=qr.unpack())!=RC_OK) break; assert(qr.pb.isNull() || qr.addr.defined());
			if (sop!=STMT_UNDELETE && ((rc=ctx->queryMgr->checkLockAndACL(qr,sop==STMT_INSERT?TVO_READ:TVO_UPD,qop))==RC_NOACCESS || rc==RC_NOTFOUND)) rc=RC_OK;
			else if (rc==RC_OK && (op!=STMT_INSERT || (qr.meta&PMT_COMM)==0 || (qop->getQFlags()&QO_RAW)!=0 || (rc=qop->createCommOp(&qr))==RC_OK))
				{if ((rc=ctx->queryMgr->apply(ectx2,sop,qr,vals,nValues,mode|md))==RC_OK) cnt++; else if (rc==RC_NOACCESS || rc==RC_DELETED) rc=RC_OK;}
		}
		if (rc==RC_EOF || rc==RC_OK) {rc=RC_OK; tx.ok();} else cnt=~0ULL;
	}
	delete qop; if (nProcessed!=NULL) *nProcessed=cnt; 
	if ((ectx.ses->getTraceMode()&TRACE_SESSION_QUERIES)!=0) trace(ectx,stmtOpName[op],rc,unsigned(cnt));
	return rc;
}

RC Stmt::cmp(const EvalCtx& ectx,const Value& v,ExprOp op,unsigned flags/* pars??? */)
{
	if (this->op!=STMT_QUERY || top==NULL) return RC_INVOP;
	Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION; 
	if (top->stype==SEL_PID) {
		if (v.type!=VT_REFID && (v.type!=VT_REF || v.pin->getPID().pid==STORE_INVALID_PID)) return op==OP_EQ||op==OP_IN?RC_FALSE:RC_TRUE;
	} else if (top->stype==SEL_PINSET && (v.type==VT_REFID || v.type==VT_REF) && top->type==QRY_SIMPLE) {
		if (v.type==VT_REF) return isSatisfied(v.pin)?RC_TRUE:RC_FALSE;
		PINx cb(ses,v.id); RC rc; PIN *pp[2]={(PIN*)&cb,NULL}; unsigned np=1;
		if (ectx.env!=NULL && ectx.nEnv>=2 && (pp[1]=ectx.env[1])!=NULL) np++;
		if ((rc=ses->getStore()->queryMgr->getBody(cb))!=RC_OK) return rc;
		rc=checkConditions(EvalCtx(ses,pp,np,pp,1))?RC_TRUE:RC_FALSE;
		return op==OP_EQ||op==OP_IN?rc:rc==RC_TRUE?RC_FALSE:RC_TRUE;
	}
	QueryOp *qop=NULL; QBuildCtx qctx(ectx.ses,(EvalCtx*)&ectx,this,0,0); RC rc=qctx.process(qop);
	if (rc==RC_OK) {
		if (top->stype==SEL_COUNT) {
			uint64_t cnt; rc=ses->getStore()->queryMgr->count(qop,cnt,~0u); delete qop;
			if (rc!=RC_OK) return rc!=RC_EOF?rc:op==OP_EQ||op==OP_IN?RC_FALSE:RC_TRUE;
			Value w; w.setU64(cnt); int c=AfyKernel::cmp(v,w,flags,ectx.ma);
			return op!=OP_IN?Expr::condRC(c,op):c==0?RC_TRUE:RC_FALSE;
		}
		PINx qr(ses),*pqr=&qr; qop->connect(&pqr); const bool fChecked=top->stype!=SEL_PID && top->stype!=SEL_PINSET && top->stype!=SEL_AUGMENTED && top->stype!=SEL_COMPOUND;
		while ((rc=qop->next())==RC_OK) if (fChecked || (rc=ses->getStore()->queryMgr->checkLockAndACL(qr,TVO_READ,qop))!=RC_NOACCESS && rc!=RC_NOTFOUND && rc!=RC_DELETED) {
			if (rc!=RC_OK) break;
			if (top->stype==SEL_PID) {
				if (qr.id.pid==STORE_INVALID_PID && (rc=qr.unpack())!=RC_OK) break;
				rc=v.type==VT_REFID && qr.id==v.id || v.type==VT_REF && qr.id==v.pin->getPID()?RC_TRUE:RC_FALSE;
				if (op==OP_EQ||op==OP_IN) {if (rc==RC_TRUE) break;} else if (op==OP_NE && rc==RC_TRUE) {rc=RC_FALSE; break;}
			} else if (top->stype==SEL_VALUE || top->stype==SEL_VALUESET) {
				if (qr.properties!=NULL && qr.nProperties==1) {
					int c=AfyKernel::cmp(v,*qr.properties,flags,ectx.ma);
					if (op==OP_EQ||op==OP_IN) {if (c==0) {rc=RC_TRUE; break;}} else if ((rc=Expr::condRC(c,op))!=RC_TRUE) break;
				}
				if (top->stype==SEL_VALUE) {rc=RC_EOF; break;}
			} else {
				//???
				if (top->stype==SEL_DERIVED || top->stype==SEL_CONST) {rc=RC_EOF; break;}
			}
		}
	}
	delete qop; return rc!=RC_EOF?rc:op==OP_EQ||op==OP_IN?RC_FALSE:RC_TRUE;
}

RC Stmt::insert(const EvalCtx& ectx,Value *ids,unsigned& cnt) const
{
	RC rc=RC_OK; cnt=0;
	if (vals==NULL || (mode&MODE_MANY_PINS)==0 && nNested==0) {
		PID tid={tpid,STORE_INVALID_IDENTITY}; PIN pin(ectx.ses,pmode,vals,nValues,true),*pp=&pin; pin=tid;
		if ((rc=ectx.ses->getStore()->queryMgr->persistPINs(ectx,&pp,1,mode,NULL,NULL,into,nInto))==RC_OK && (pin.mode&PIN_TRANSIENT)==0)
			{cnt=1; if (ids!=NULL) ids->set(pin.id);}
	} else {
		unsigned nV=(mode&MODE_MANY_PINS)!=0?nValues:1;
		PIN **ppins=(PIN**)ectx.ses->malloc((sizeof(PIN)+sizeof(PIN*))*(nNested+nV)); if (ppins==NULL) return RC_NORESOURCES;
		if ((rc=getNested(ppins,(PIN*)(ppins+nNested+nV),cnt,ectx.ses))==RC_OK) {
			assert(cnt==nNested+nV);
			if ((rc=ectx.ses->getStore()->queryMgr->persistPINs(ectx,ppins,cnt,mode,NULL,NULL,into,nInto))==RC_OK && ids!=NULL) {
				if (nV==1) ids->set(ppins[0]->id);
				else for (unsigned i=0; i<cnt; i++) if ((ppins[i]->mode&PIN_TRANSIENT)==0) ids[i].set(ppins[i]->id);	// ids cnt?
			}
		}
		for (unsigned i=0; i<cnt; i++) if (ppins[i]!=NULL) {if (cnt!=0 && (ppins[i]->mode&PIN_TRANSIENT)!=0) cnt--; ppins[i]->~PIN();}	//???
		ectx.ses->free(ppins); if (rc!=RC_OK) cnt=0;
	}
	return rc;
}

RC QueryPrc::eval(const Value *pv,const EvalCtx& ectx,Value *res,unsigned mode)
{
	RC rc; Expr *expr; unsigned pidx; Value *pv2,w; TIMESTAMP ts;
	//if ((mode&MODE_ASYNC)!=0) {
		//create sync eval block + copy ectx
		//return ectx.ses->addOnCommit(...);
	//}
	PropertyID pid=pv->property; uint8_t meta=pv->meta,op=pv->op; CursorNav *cn=NULL; Cursor *crs=NULL;
	switch (pv->type) {
	default: return RC_TYPE;
	case VT_VARREF:
		if (res!=NULL) switch (pv->refV.flags&VAR_TYPE_MASK) {
		default:
			pidx=((pv->refV.flags&VAR_TYPE_MASK)>>13)-1;
			if (ectx.params==NULL||pidx>=ectx.nParams||pv->refV.refN>=ectx.params[pidx].nValues) return RC_NOTFOUND;
			*res=ectx.params[pidx].vals[pv->refV.refN]; setHT(*res); break;
		case VAR_REXP:
			if (ectx.rctx==NULL) return RC_NOTFOUND;
			if ((pidx=pv->refV.id)>*ectx.rctx || ectx.rctx->rxstr==NULL || (*ectx.rctx)[pidx].len==0) res->setError();
			else res->set(ectx.rctx->rxstr+(*ectx.rctx)[pidx].sht,(uint32_t)(*ectx.rctx)[pidx].len);
			break;
		case VAR_CTX:
			if (pv->refV.refN>=ectx.nEnv) return RC_INVPARAM;
			if (pv->length==0 || pv->refV.id==PROP_SPEC_SELF) res->set(ectx.env[pv->refV.refN]);
			else if ((rc=ectx.env[pv->refV.refN]->getV(pv->refV.id,*res,LOAD_SSV,ectx.ma,pv->eid))!=RC_OK) return rc;
			break;
		case 0:
			if (pv->refV.refN>=ectx.nVars) return RC_INVPARAM;
			if (pv->length==0 || pv->refV.id==PROP_SPEC_SELF) res->set(ectx.vars[pv->refV.refN]);
			else if ((rc=ectx.vars[pv->refV.refN]->getV(pv->refV.id,*res,LOAD_SSV,ectx.ma,pv->eid))!=RC_OK) return rc;
			break;
		}
		break;
	case VT_EXPRTREE:
		if (res!=NULL && ((rc=Expr::compile((ExprTree*)pv->exprt,expr,ectx.ses,false))!=RC_OK || (rc=Expr::eval(&expr,1,*res,ectx))!=RC_OK)) return rc;
		break;
	case VT_EXPR:
		if (res!=NULL && (rc=Expr::eval((Expr**)&pv->expr,1,*res,ectx))!=RC_OK) return rc;
		break;
	case VT_STMT:
		if ((rc=((Stmt*)pv->stmt)->execute(ectx,res,~0u,0,mode&(MODE_COPY_VALUES|MODE_PID)))!=RC_OK) return rc;
		break;
	case VT_ARRAY: case VT_STRUCT:
		if (res==NULL) pv2=(Value*)pv->varray;
		else if ((pv2=new(ectx.ma) Value[pv->length])==NULL) return RC_NORESOURCES;
		else memcpy(pv2,pv->varray,pv->length*sizeof(Value));
		for (unsigned i=0; i<pv->length; i++) {
			switch (pv2[i].type) {
			case VT_VARREF: case VT_CURRENT: case VT_EXPRTREE: break;
			case VT_STMT: case VT_EXPR: case VT_STRUCT: if (pv2[i].fcalc!=0||ectx.ect==ECT_ACTION) break;
			default: if (res!=NULL) setHT(pv2[i]); continue;
			}
			RC rc=eval(&pv2[i],ectx,res!=NULL?&w:NULL);
			if (rc!=RC_OK) {
				if (res!=NULL && i!=0) freeV(pv2,i-1,ectx.ma);
				return rc;
			}
			if (res!=NULL) {
				if (pv->type==VT_ARRAY) w.eid=pv2[i].eid; else w.property=pv2[i].property;
				w.meta=pv2[i].meta; pv2[i]=w;
			}
		}
		if (res!=NULL && (rc=pv->type==VT_ARRAY?ExprTree::normalizeArray(pv2,pv->length,*res,ectx.ma,ectx.ses->getStore()):
												ExprTree::normalizeStruct(pv2,pv->length,*res,ectx.ma))!=RC_OK) return rc;
		break;
	case VT_CURRENT:
		if (res!=NULL) switch (pv->i) {
		default: break;
		case CVT_TIMESTAMP: getTimestamp(ts); res->setDateTime(ts); break;
		case CVT_USER: res->setIdentity(ectx.ses!=NULL?ectx.ses->getIdentity():STORE_OWNER); break;
		case CVT_STORE: res->set((unsigned)ectx.ses->getStore()->storeID); break;
		}
		break;
	case VT_REFIDPROP: case VT_REFIDELT:
		if (res!=NULL && (rc=loadValue(ectx.ses,pv->refId->id,pv->refId->pid,pv->type==VT_REFIDELT?pv->refId->eid:STORE_COLLECTION_ID,*res,LOAD_SSV))!=RC_OK) return rc;
		break;
	case VT_REF:
		//???
		break;
	case VT_COLLECTION:
		//???
		break;
	}
	if (res!=NULL) {
		if ((mode&MODE_INMEM)!=0 && res->type==VT_COLLECTION) {
			DynArray<Value,32> arr((MemAlloc*)ectx.ses); uint32_t nVals; Value *pv,w;
			for (const Value *cv=res->nav->navigate(GO_FIRST); cv!=NULL; cv=res->nav->navigate(GO_NEXT))
				if ((rc=copyV(*cv,w,ectx.ses))!=RC_OK || (rc=arr+=w)!=RC_OK) {pv=(Value*)arr.get(nVals); freeV(pv,nVals,ectx.ses); return rc;}
			freeV(*res); if (arr==1) *res=arr[0]; else {pv=(Value*)arr.get(nVals); if (pv!=NULL && nVals!=0) res->set(pv,nVals);}
		}
		res->property=pid; res->meta=meta; res->op=op; res->fcalc=0;
	}
	return RC_OK;
}

RC QueryPrc::apply(const EvalCtx& ectx,STMT_OP op,PINx& qr,const Value *values,unsigned nValues,unsigned mode,PIN *pin)
{
	RC rc=RC_OK; bool f=pin!=NULL; EvalCtxType sct=ectx.ect; ectx.ect=sct==ECT_ACTION?ECT_ACTION:ECT_INSERT;
	const bool fEnv=ectx.env!=NULL&&ectx.nEnv!=0,fRestore=!fEnv||ectx.env[0]==NULL; PIN *pq=&qr;
	if (fRestore) {if (fEnv) ectx.env[0]=pq; else {const_cast<PIN**&>(ectx.env)=(PIN**)&pq; const_cast<unsigned&>(ectx.nEnv)=1;}}
	switch (op) {
	default: rc=RC_INTERNAL; break;
	case STMT_INSERT:
		if (!f) {
			if (qr.fPartial!=0) rc=loadPIN(ectx.ses,qr.id,pin,mode,&qr);
			else if ((pin=new(ectx.ses) PIN(ectx.ses,0,qr.properties,qr.nProperties,true))==NULL) rc=RC_NORESOURCES;
		}
		if (rc==RC_OK) {
			const_cast<PID&>(pin->id)=PIN::noPID; pin->addr=PageAddr::noAddr;
			for (unsigned i=0; i<nValues; i++)	{// epos ???
				// evaluate expr with params
				if ((rc=pin->modify(&values[i],values[i].eid,STORE_COLLECTION_ID,0,ectx.ses))!=RC_OK) break;
			}
			if (rc==RC_OK) rc=persistPINs(ectx,&pin,1,mode|MODE_NO_EID);
			if (!f) pin->destroy();
		}
		break;
	case STMT_UPDATE:
		rc=modifyPIN(ectx,qr.id,values,nValues,&qr,pin,mode|MODE_NO_EID); break;
	case STMT_DELETE:
		rc=deletePINs(ectx,NULL,&qr.id,1,mode,qr.pb.isNull()?(PINx*)0:&qr); break;
	case STMT_UNDELETE:
		rc=setFlag(ectx,qr.id,&qr.addr,HOH_DELETED,true); break;
	}
	if (fRestore) {if (fEnv) ectx.env[0]=NULL; else {const_cast<PIN**&>(ectx.env)=NULL; const_cast<unsigned&>(ectx.nEnv)=0;}}
	ectx.ect=sct; return rc;
}

namespace AfyKernel
{
	class StmtRequest : public Request {
		Stmt			*const	stmt;
		IStmtCallback	*const	cb;
		const Value		*const	params;
		const unsigned			nParams;
		const unsigned			nProcess;
		const unsigned			nSkip;
		const unsigned			mode;
		const TXI_LEVEL			txl;
		StoreCtx		*const	ctx;
		ICursor					*res;
	public:
		StmtRequest(Stmt *st,IStmtCallback *c,const Value *pars,unsigned nPars,unsigned nP,unsigned nS,unsigned md,TXI_LEVEL t,StoreCtx *ct) 
			: stmt(st),cb(c),params(pars),nParams(nPars),nProcess(nP),nSkip(nS),mode(md),txl(t),ctx(ct),res(NULL) {}
		~StmtRequest() {if (stmt!=NULL) stmt->destroy(); if (params!=NULL) freeV((Value*)params,nParams,ctx); if (res!=NULL) res->destroy();}
		void process() {
			RC rc=stmt->execute(&res,params,nParams,nProcess,nSkip,mode&~MODE_COPY_VALUES,NULL,txl);
			//if (rc==RC_OK && res!=NULL) rc=res->getFirst();
			cb->result(res,rc);
		}
		void destroy() {this->~StmtRequest(); ctx->free(this);}
	};
};

RC Stmt::asyncexec(IStmtCallback *cb,const Value *params,unsigned nParams,unsigned nProcess,unsigned nSkip,unsigned mode,TXI_LEVEL txl) const
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION; 
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN;
		RC rc=RC_OK; StmtRequest *rq; Stmt *st; Value *pv=NULL;
		switch (op) {
		case STMT_QUERY:
		case STMT_INSERT:
		case STMT_UPDATE:
		case STMT_DELETE:
		case STMT_UNDELETE:
			if ((st=clone(op,ctx))==NULL) {rc=RC_NORESOURCES; break;}
			if (params!=NULL && (rc=copyV(params,nParams,pv,ctx))!=RC_OK) {st->destroy(); break;}
			if ((rq=new(ctx) StmtRequest(st,cb,pv,nParams,nProcess,nSkip,mode,txl,ctx))==NULL)
				{st->destroy(); freeV(pv,nParams,ctx); rc=RC_NORESOURCES; break;}
			if (!RequestQueue::postRequest(rq,ctx)) {rq->destroy(); rc=RC_OTHER;}
			break;
		default: rc=RC_INVOP; break;
		}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::asyncexec()\n"); return RC_INTERNAL;}
}

RC QueryPrc::count(QueryOp *qop,uint64_t& cnt,unsigned nAbort,const OrderSegQ *os,unsigned nos)
{
	RC rc=RC_OK; cnt=0ULL;
	if (qop->getSession()->getIdentity()==STORE_OWNER) rc=qop->count(cnt,nAbort);
	else {
		// nulls in sort?
		PINx qr(qop->getSession()),*pqr=&qr; qop->connect(&pqr);
		while ((rc=qop->next())==RC_OK)
			if ((rc=checkLockAndACL(qr,TVO_READ,qop))!=RC_NOACCESS && rc!=RC_NOTFOUND) {
				if (rc!=RC_OK) break;
				cnt++;
			}
		if (rc==RC_EOF) rc=RC_OK;
	}
	return rc;
}

RC Stmt::count(uint64_t& cnt,const Value *pars,unsigned nPars,unsigned nAbort,unsigned md,TXI_LEVEL txl) const
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION;
		if (ses->getStore()->inShutdown()) return RC_SHUTDOWN; ses->resetAbortQ();
		TxGuard txg(ses); md&=MODE_ALL_WORDS|MODE_DELETED; 
		ValueV vp(pars,nPars); EvalCtx ectx(ses,NULL,0,NULL,0,&vp,1);
		QBuildCtx qctx(ses,&ectx,this,0,md|MODE_COUNT); QueryOp *qop=NULL; RC rc=qctx.process(qop);
		if (rc==RC_OK) {rc=ses->getStore()->queryMgr->count(qop,cnt,nAbort,orderBy,nOrderBy); delete qop;}
		switch (rc) {
		default: cnt=~0ULL; break;
		case RC_EOF: cnt=0; rc=RC_OK; break;
		case RC_OK: break;
		}
		if ((ses->getTraceMode()&TRACE_SESSION_QUERIES)!=0) trace(ses,"COUNT",rc,(unsigned)cnt);
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::count()\n"); return RC_INTERNAL;}
}

RC Stmt::exist(const Value *pars,unsigned nPars,unsigned md,TXI_LEVEL txi) const
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION;
		if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(ses); ses->resetAbortQ(); md&=MODE_ALL_WORDS|MODE_DELETED;
		ValueV vp(pars,nPars); EvalCtx ectx(ses,NULL,0,NULL,0,&vp,1);
		QBuildCtx qctx(ses,&ectx,this,0,md); QueryOp *qop=NULL; RC rc=qctx.process(qop);
		if (rc==RC_OK) {
			assert(qop!=NULL); StoreCtx *ctx=ses->getStore();
			PINx qr(ses),*pqr=&qr; qop->connect(&pqr);
			while ((rc=qop->next())==RC_OK)
				if ((rc=ctx->queryMgr->checkLockAndACL(qr,TVO_READ,qop))!=RC_NOACCESS && rc!=RC_NOTFOUND) break;
			qr.cleanup(); delete qop;
		}
		if ((ses->getTraceMode()&TRACE_SESSION_QUERIES)!=0) trace(ses,"EXIST",rc,1);
		return rc==RC_OK?RC_TRUE:RC_FALSE;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::exists()\n"); return RC_INTERNAL;}
}

RC Stmt::analyze(char *&plan,const Value *pars,unsigned nPars,unsigned md) const
{
	try {
		Session *ses=Session::getSession(); plan=NULL;
		if (ses==NULL) return RC_NOSESSION; if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(ses); ses->resetAbortQ();
		ValueV vp(pars,nPars); EvalCtx ectx(ses,NULL,0,NULL,0,&vp,1);
		QBuildCtx qctx(ses,&ectx,this,0,md&(MODE_ALL_WORDS|MODE_DELETED));
		QueryOp *qop=NULL; RC rc=qctx.process(qop);
		if (rc==RC_OK && qop!=NULL) {SOutCtx buf(ses); qop->print(buf,0); plan=(char*)buf;}
		delete qop; return rc==RC_EOF?RC_OK:rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::analyze()\n"); return RC_INTERNAL;}
}

void Stmt::trace(const EvalCtx& ectx,const char *op,RC rc,unsigned cnt) const
{
	char *str=toString();
	if (str==NULL) ectx.ses->trace(-1,"%s: BAD QUERY",op); else {ectx.ses->trace(0,"%s",str); ectx.ses->free(str);}
	if (ectx.params!=NULL && ectx.nParams!=0 && ectx.params[0].vals!=NULL && ectx.params[0].nValues!=0) {
		// trace params
	}
	if (rc!=RC_OK) ectx.ses->trace(0,"%s",getErrMsg(rc)); else if (cnt!=~0u) ectx.ses->trace(0," -> %lu",cnt);
	ectx.ses->trace(0,"\n");
}

bool Stmt::isSatisfied(const IPIN *p,const Value *pars,unsigned nPars,unsigned) const
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL||ses->getStore()->inShutdown()) return false;
		TxGuard txg(ses); ValueV vv(pars,nPars); PIN *pp=(PIN*)p;
		return checkConditions(EvalCtx(ses,&pp,1,&pp,1,&vv,1));
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IStmt::isSatisfied()\n");}
	return false;
}

bool Stmt::checkConditions(const EvalCtx& ectx,unsigned start,bool fIgnore) const
{
	QVar *qv=top; if (qv==NULL) return true;
	if (qv->type==QRY_SIMPLE) {
		const SimpleVar *cv=(SimpleVar*)qv;
		if (cv->classes!=NULL) {
			for (unsigned i=start; i<cv->nClasses; i++) {
				SourceSpec& cs=cv->classes[i]; Class *cls;
				if ((cls=ectx.ses->getStore()->classMgr->getClass(cs.objectID))==NULL) return false;
				const Stmt *cqry=cls->getQuery(); const QVar *cqv; ValueV vv(cs.params,cs.nParams);
				if (cqry==NULL || (cqv=cqry->top)==NULL ||
					!Expr::condSatisfied(cqv->nConds==1?&cqv->cond:cqv->conds,cqv->nConds,EvalCtx(ectx.ses,ectx.env,ectx.nEnv,ectx.vars,ectx.nVars,&vv,1))) 
						{cls->release(); return false;}
				cls->release();
			}
		}
		if (!fIgnore || ectx.nParams!=0 && ectx.params!=NULL && ectx.params[0].vals!=NULL && ectx.params[0].nValues!=0) 
			for (CondIdx *ci=cv->condIdx; ci!=NULL; ci=ci->next) {
				if (ci->param>=ectx.params[0].nValues) return false;
				Value v; const Value *par=&ectx.params[0].vals[ci->param];
				PIN *pin=ectx.env!=NULL&&ectx.nEnv!=0&&ectx.env[0]!=NULL?ectx.env[0]:ectx.vars!=NULL&&ectx.nVars!=0?ectx.vars[0]:NULL;
				if (pin==NULL) return false;
				if (ci->expr!=NULL) {
					// ???
				} else if (pin->getV(ci->ks.propID,v,LOAD_SSV,NULL)!=RC_OK) return false;
				RC rc=Expr::calc((ExprOp)ci->ks.op,v,par,2,(ci->ks.flags&ORD_NCASE)!=0?CND_NCASE:0,ectx.ses);
				freeV(v); if (rc!=RC_TRUE) return false;
			}
	}
	return qv->nConds==0 || Expr::condSatisfied(qv->nConds==1?&qv->cond:qv->conds,qv->nConds,ectx);
}

bool QueryPrc::test(PIN *pin,ClassID classID,const EvalCtx& ectx,bool fIgnore)
{
	Class *cls; bool rc=false; 
	if ((cls=ctx->classMgr->getClass(classID))!=NULL) {
		const Stmt *cqry=cls->getQuery(); 
		if (cqry!=NULL) {
			PINx pc(ectx.ses,cls->getPID()); PIN *pp[2]={pin,(PIN*)&pc};
			EvalCtx ectx2(ectx.ses,pp,2,pp,1,ectx.params,ectx.nParams,ectx.stack,NULL,ectx.ect);
			rc=cqry->checkConditions(ectx2,0,fIgnore);
		}
		cls->release();
	}
	return rc;
}

//---------------------------------------------------------------------------------------------------------

Cursor::Cursor(const EvalCtx &ec,QueryOp *qop,uint64_t nRet,unsigned md,const Value *vals,unsigned nV,STMT_OP sop,SelectType ste,bool fSS)
	: ectx(ec,ECT_QUERY),queryOp(qop),nReturn(nRet),values(vals),nValues(nV),mode(md),stype(ste),op(sop),results(NULL),nResults(0),qr(qop->getQCtx()->ses),pqr(&qr),
	txid(INVALID_TXID),txcid(NO_TXCID),cnt(0),tx(qop->getQCtx()->ses),fSnapshot(fSS),fProc(false),fAdvance(true)
{
	if (ec.params!=NULL && ec.nParams==1) {
		params=ec.params[0]; params.fFree=false;
		if ((md&MODE_COPY_VALUES)!=0) {
			// copy params, params.fFree=true;
		}
		const_cast<ValueV*&>(ectx.params)=&params;
	}
}

Cursor::~Cursor()
{
	if (txcid!=NO_TXCID) ectx.ses->getStore()->txMgr->releaseSnapshot(txcid);
	delete queryOp;
	if (values!=NULL) freeV((Value*)values,nValues,ectx.ses);
	if (params.vals!=NULL && params.fFree) freeV((Value*)params.vals,params.nValues,ectx.ses);
	if (results!=NULL) {
		for (unsigned i=1; i<nResults; i++) if (results[i]!=NULL) {results[i]->~PINx(); ectx.ses->free(results[i]);}
		ectx.ses->free(results);
	}
}

RC Cursor::connect()
{
	if (queryOp!=NULL && (nResults=queryOp->getNOuts())!=0) {
		if (nResults==1) queryOp->connect(&pqr);
		else if ((results=new(ectx.ses) PINx*[nResults])==NULL) return RC_NORESOURCES;
		else {
			memset(results,0,nResults*sizeof(PINx*)); results[0]=pqr;
			for (unsigned i=1; i<nResults; i++)
				if ((results[i]=new(ectx.ses) PINx(ectx.ses))==NULL) return RC_NORESOURCES;
			queryOp->connect(results,nResults);
		}
	}
	return RC_OK;
}

uint64_t Cursor::getCount() const
{
	try {return cnt;} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ICursor::getCount()\n");}
	return ~0ULL;
}

RC Cursor::skip()
{
	unsigned ns=queryOp->getSkip(),c=0; StoreCtx *ctx=ectx.ses->getStore(); RC rc=RC_OK;
	while (c<ns && (rc=queryOp->next())==RC_OK)
		if ((rc=ctx->queryMgr->checkLockAndACL(qr,TVO_READ,queryOp))!=RC_NOACCESS && rc!=RC_NOTFOUND)		// results?? lock multiple
			if (rc==RC_OK) c++; else break;
	queryOp->setSkip(ns-c); return rc;
}

RC Cursor::advance(bool fRet)
{
	if (!fAdvance) {fAdvance=true; return RC_OK;}
	RC rc=RC_EOF;
	if (ectx.ses!=NULL && cnt<nReturn && queryOp!=NULL) {
		if (ectx.ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		if (op!=STMT_QUERY && !tx.isStarted()) {if ((rc=tx.start())==RC_OK) tx.ok(); else return rc;}
		if (txid==INVALID_TXID) txid=ectx.ses->getTXID();
		else if (ectx.ses->getTXID()!=txid) {
			if ((mode&MODE_HOLD_RESULT)==0) return RC_CLOSED;
			// re-init & re-position
		}
		if (fSnapshot && txcid==NO_TXCID) if (ectx.ses->getTxState()==TX_NOTRAN) txcid=ectx.ses->getStore()->txMgr->assignSnapshot(); else fSnapshot=false;
		TxGuard txg(ectx.ses); ectx.ses->resetAbortQ();
		if (ectx.ses->getIdentity()==STORE_OWNER || queryOp->getSkip()==0 || (rc=skip())==RC_OK) {	//??????
			if (fProc && (stype==SEL_COUNT || stype==SEL_VALUE || stype==SEL_DERIVED || stype==SEL_CONST)) rc=RC_EOF;
			else if (stype==SEL_COUNT) {rc=ectx.ses->getStore()->queryMgr->count(queryOp,cnt,~0u); fProc=true;}
			else for (TVOp tvo=op==STMT_INSERT||op==STMT_QUERY?TVO_READ:TVO_UPD; (rc=queryOp->next())==RC_OK;) {
				if (stype==SEL_CONST) {fProc=true; break;}
				if ((rc=ectx.ses->getStore()->queryMgr->checkLockAndACL(qr,tvo,queryOp))!=RC_NOACCESS && rc!=RC_NOTFOUND) {
					if ((qr.epr.flags&PINEX_DERIVED)!=0 && qr.properties==NULL) rc=queryOp->loadData(qr,NULL,0);
					if (rc==RC_OK) {
						PINx qpin(ectx.ses),*pq=&qr;
						if (!pq->pb.isNull()) {if (fRet && op!=STMT_QUERY) rc=pq->load(LOAD_ENAV|LOAD_SSV);}
						else if (stype==SEL_PINSET || stype==SEL_COMPOUND) {
							qpin.id=pq->id; qpin.epr=pq->epr; qpin.addr=pq->addr; pq=&qpin;
							if (op!=STMT_QUERY) {
								rc=ectx.ses->getStore()->queryMgr->getBody(qpin,tvo,(mode&MODE_DELETED)!=0?GB_DELETED:0);
								if (rc==RC_OK && fRet && (rc=qpin.load(LOAD_ENAV|LOAD_SSV))==RC_OK)
									{qr.properties=qpin.properties; qr.nProperties=qpin.nProperties; qr.addr=qpin.addr; qr.mode=qpin.mode; qr.meta=qpin.meta; qpin.fNoFree=1;}
							}
						} else {
							// check property!=NULL || !PIN_PARTIAL
						}
						if (rc==RC_OK) {
							if (op!=STMT_QUERY) {
								if (op==STMT_INSERT && (pq->meta&PMT_COMM)!=0 && (queryOp->getQFlags()&QO_RAW)==0 && (rc=queryOp->createCommOp(pq))!=RC_OK) break;
								rc=ectx.ses->getStore()->queryMgr->apply(ectx,op,*pq,values,nValues,mode,fRet?(PIN*)&qr:(PIN*)0);
								if (rc==RC_NOACCESS || rc==RC_DELETED) {if (fRet) qr.cleanup(); continue;}
							}
							if (rc==RC_OK) {fProc=true; cnt++;}
						}
					}
					break;
				}
			}
		}
		if (rc!=RC_OK) {
			if (rc!=RC_EOF) tx.resetOk(); 
			if (txcid!=NO_TXCID) {ectx.ses->getStore()->txMgr->releaseSnapshot(txcid); txcid=NO_TXCID;}
		}
	}
	return rc;
}

RC Cursor::extract(PIN *&pin,unsigned idx,bool fCopy)
{
	assert(stype==SEL_PINSET||stype==SEL_AUGMENTED||stype==SEL_COMPOUND);
	if (idx>=nResults) return RC_INVPARAM;
	PINx *pex=results!=NULL?results[idx]:pqr; RC rc=RC_OK; assert(pex!=NULL);
	if ((pex->id.ident!=STORE_INVALID_IDENTITY || pex->epr.lref!=0) && (pex->epr.flags&PINEX_DERIVED)==0)
		{if ((rc=pex->load(LOAD_SSV|(fCopy?LOAD_ENAV:0)))!=RC_OK) {pin=NULL; return rc;}}
	if ((mode&MODE_RAW)==0 && idx==0 && (pex->meta&PMT_COMM)!=0 && (queryOp->getQFlags()&QO_RAW)==0) {
		if (op==STMT_UPDATE || op==STMT_INSERT && (pex->mode&PIN_TRANSIENT)!=0) return RC_EOF;
		if ((rc=queryOp->createCommOp(pex))!=RC_OK) return rc;
	}
	if (!fCopy) pin=pex;
	else if ((pin=new(ectx.ses) PIN(ectx.ses,pqr->mode))==NULL) return RC_NORESOURCES;
	else {
		*pin=pqr->id; *pin=pqr->addr; pin->meta=pex->meta;
		if (pex->properties!=NULL && pex->nProperties!=0) {
			if (pex->fNoFree==0) {pin->properties=(Value*)pex->properties; pin->nProperties=pex->nProperties; pex->properties=NULL; pex->nProperties=0;}
			else if ((rc=copyV(pex->properties,pin->nProperties=pex->nProperties,pin->properties,ectx.ses))!=RC_OK) return rc;
		}
	}
	if ((pin->meta&PMT_CLASS)!=0 && (mode&MODE_CLASS)!=0 && pin->id.ident!=STORE_INVALID_IDENTITY) ectx.ses->getStore()->queryMgr->getClassInfo(ectx.ses,pin);
	return RC_OK;
}

RC Cursor::next(const PINx *&ret)
{
	ret=NULL; RC rc=advance(true); if (rc!=RC_OK) return rc;
	unsigned i; PIN *pin;
	switch (stype) {
	case SEL_COUNT:
		if (qr.properties==NULL && (qr.properties=new(ectx.ses) Value)==NULL) return RC_NORESOURCES;
		qr.nProperties=1; qr.properties->setU64(cnt); qr.properties->setPropID(PROP_SPEC_VALUE); ret=pqr;
		break;
	case SEL_PINSET: 
		while ((rc=extract(pin,0))!=RC_OK) if (rc!=RC_EOF || (rc=advance(true))!=RC_OK) return rc;
		ret=pqr; break;
	case SEL_COMPOUND:
		for (i=0; i<nResults; i++) if ((rc=extract(pin,i))!=RC_OK) return rc;
	default:
		ret=pqr; break;
	}
	return RC_OK;
}

RC Cursor::next(Value& ret)
{
	try {
		ret.setEmpty(); RC rc=advance(true); PIN *pin; Value *pv; PID pid;
		if (rc==RC_OK) {
			switch (stype) {
			case SEL_COUNT: ret.setU64(cnt); ret.setPropID(PROP_SPEC_VALUE); break;
			case SEL_CONST: case SEL_VALUE: case SEL_VALUESET: case SEL_DERIVED: case SEL_DERIVEDSET: case SEL_AUGMENTED:
				if (op!=STMT_QUERY) ret.set((IPIN*)pqr);
				else {
					freeV(retres);
					if (pqr->properties!=NULL) {
						if (pqr->nProperties>1) {
							retres.setStruct(pqr->properties,pqr->nProperties); setHT(retres,SES_HEAP);
							if (pqr->fPartial!=0) rc=copyV(retres,retres,ectx.ses); else {pqr->properties=NULL; pqr->nProperties=0;}
							ret=retres; setHT(ret);
						} else if (pqr->fNoFree!=0) rc=copyV(pqr->properties[0],ret,ectx.ses); else {ret=pqr->properties[0]; pqr->properties[0].setError();}
					}
				}
				break;
			case SEL_PID:
				if ((rc=qr.getID(pid))==RC_OK) {ret.set(pid); ret.setPropID(PROP_SPEC_PINID);}
				break;
			case SEL_PINSET:
				while ((rc=extract(pin,0,true))!=RC_OK) if (rc!=RC_EOF || (rc=advance(true))!=RC_OK) break;
				if (rc==RC_OK) {ret.set(pin); setHT(ret,SES_HEAP);}
				break;
			case SEL_COMPOUND: case SEL_COMP_DERIVED:
				if (retres.isEmpty()) {
					if ((pv=new(ectx.ses) Value[nResults])==NULL) return RC_NORESOURCES;
					memset(pv,0,nResults*sizeof(Value)); retres.set(pv,nResults); setHT(retres,SES_HEAP);
				}
				for (unsigned i=0; i<nResults; i++) {
					if ((rc=extract(pin,i,true))!=RC_OK) break;
					freeV(*(Value*)&retres.varray[i]); ((Value*)&retres.varray[i])->set(pin);
				}
				if (rc==RC_OK) {ret=retres; setHT(ret);}
				break;
			}
		}
		ectx.ses->releaseAllLatches(); return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ICursor::next(Value&)\n"); return RC_INTERNAL;}
}

RC Cursor::next(PID& pid)
{
	try {
		if (stype!=SEL_PINSET && stype!=SEL_AUGMENTED && stype!=SEL_COMPOUND) return RC_INVOP;
		RC rc=advance(false);  if (rc==RC_OK) rc=qr.getID(pid); else pid=PIN::noPID;
		ectx.ses->releaseAllLatches(); return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ICursor::next(PID&)\n"); return RC_INTERNAL;}
}

IPIN *Cursor::next()
{
	try {
		if (stype!=SEL_PINSET && stype!=SEL_COMPOUND) return NULL; PIN *pin=NULL;
		if (advance(true)==RC_OK) {
			extract(pin,0,true);	// check RC_EOF
		}
		ectx.ses->releaseAllLatches(); return pin;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ICursor::next()\n");}
	return NULL;
}

RC Cursor::rewindInt()
{
	RC rc=RC_EOF;
	if (ectx.ses!=NULL && queryOp!=NULL) {
		if (!fProc || op!=STMT_QUERY && !tx.isStarted()) return RC_OK;
		if ((rc=queryOp->rewind())==RC_OK) {cnt=0; fProc=false;}
	}
	return rc;
	
}

RC Cursor::rewind()
{
	try {
		RC rc=RC_EOF;
		if (ectx.ses!=NULL && queryOp!=NULL) {
			if (ectx.ses->getStore()->inShutdown()) return RC_SHUTDOWN;
			if (!fProc || op!=STMT_QUERY && !tx.isStarted()) return RC_OK;
			if ((rc=rewindInt())!=RC_OK) {
				if (rc!=RC_EOF) tx.resetOk(); 
				if (txcid!=NO_TXCID) {ectx.ses->getStore()->txMgr->releaseSnapshot(txcid); txcid=NO_TXCID;}
			}
			ectx.ses->releaseAllLatches();
		}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ICursor::rwind()\n"); return RC_INTERNAL;}
}

void Cursor::destroy()
{
	try {delete this;} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ICursor::destroy()\n");}
}

//--------------------------------------------------------------------------------------------------------------------------------

const Value *CursorNav::navigate(GO_DIR dir,ElementID ei)
{
	PID id; const Value *cv;
	if (curs!=NULL) switch (dir) {
	default: break;
	case GO_FINDBYID: /*if (ei==STORE_COLLECTION_ID) curs->release();*/ break;			// ????
	case GO_FIRST:
		idx=0; freeV(v); v.setError(); if (!fCnt) cnt=0; if (curs->rewindInt()!=RC_OK) break;
	case GO_NEXT:
		if (v.type==VT_COLLECTION) {
			if ((cv=v.nav->navigate(GO_NEXT))!=NULL) {if (!fCnt) cnt++; return cv;}
			v.setEmpty();
		} else if (v.type==VT_ARRAY) {
			if (idx<v.length) return &v.varray[idx++];
			idx=0; v.setEmpty();
		}
		while (curs->advance(curs->stype!=SEL_PINSET)==RC_OK) {
			switch (curs->stype) {
			case SEL_COUNT: v.setU64(curs->cnt); cnt=1; fCnt=true; return &v;
			case SEL_VALUE: case SEL_VALUESET:
				if (curs->qr.properties!=NULL && curs->qr.nProperties==1) switch (curs->qr.properties[0].type) {
				default: if (!fCnt) cnt++; return curs->qr.properties;
				case VT_ARRAY: if (curs->qr.properties[0].length==0) continue;
					*(Value*)&v=curs->qr.properties[0]; setHT(v); if (!fCnt) cnt+=v.length; return &v.varray[idx++];
				case VT_COLLECTION: if ((cv=curs->qr.properties[0].nav->navigate(GO_FIRST))==NULL) continue;
					*(Value*)&v=curs->qr.properties[0]; setHT(v); if (!fCnt) cnt++; return cv;
				}
				break;
			case SEL_DERIVED: case SEL_CONST: if (!fCnt) {cnt=1; fCnt=true;}
			case SEL_AUGMENTED: case SEL_DERIVEDSET:
				//if (curs->qr.load(LOAD_SSV)==RC_OK) return curs->qr.getValueByIndex(0);
				//break;
			default:
				if (fPID) {if (curs->qr.getID(id)==RC_OK) {if (!fCnt) cnt++; v.set(id); return &v;}}
				else if (curs->qr.load(LOAD_SSV)==RC_OK) {if (!fCnt) cnt++; v.set(&curs->qr); return &v;}
				break;
			}
		}
		fCnt=true;
		break;			// release??
	}
	return NULL;
}

ElementID CursorNav::getCurrentID()
{
	return STORE_COLLECTION_ID;
}

const Value *CursorNav::getCurrentValue()
{
	return NULL;
}

RC CursorNav::getElementByID(ElementID,Value&)
{
	return RC_INVOP;
}

INav *CursorNav::clone() const
{
	//???
	return NULL;
}

unsigned CursorNav::count() const
{
	if (!fCnt) {
		if (curs!=NULL) switch (curs->stype) {
		case SEL_COUNT: case SEL_CONST: case SEL_DERIVED: return 1;
		default:
			while (curs->advance(curs->stype!=SEL_PINSET)==RC_OK) {
				if (curs->stype!=SEL_VALUE && curs->stype!=SEL_VALUESET) cnt++;
				else if (curs->qr.properties!=NULL && curs->qr.nProperties==1) switch (curs->qr.properties[0].type) {
				default: cnt++; break;
				case VT_ARRAY: cnt+=curs->qr.properties[0].length; break;
				case VT_COLLECTION: cnt+=curs->qr.properties[0].nav->count(); break;
				}
			}
			fCnt=true;
			break;
		}
	}
	return cnt>=0x100000000ULL?0xFFFFFFFF:(unsigned)cnt;
}

void CursorNav::destroy()
{
	Session *ses=curs!=NULL?curs->ectx.ses:Session::getSession(); this->~CursorNav(); ses->free(this);
}

//--------------------------------------------------------------------------------------------------------------------------------

RC QueryPrc::getBody(PINx& cb,TVOp tvo,unsigned flags,VersionID vid)
{
	RC rc; bool fRemote=false,fTry=true,fWrite=tvo!=TVO_READ; cb.epr.flags&=~PINEX_ADDRSET; PageAddr extAddr;
	if (cb.id.pid==STORE_INVALID_PID && (rc=cb.unpack())!=RC_OK) return rc;
	if (!cb.addr.defined()) {fTry=false; if (isRemote(cb.id)) fRemote=true; else if (!cb.addr.convert(cb.id.pid)) return RC_CORRUPTED;}
	if ((cb.epr.flags&PINEX_EXTPID)!=0 && extAddr.convert(uint64_t(cb.id.pid))) cb.ses->setExtAddr(extAddr);
	for (unsigned fctl=PGCTL_RLATCH|(fWrite?PGCTL_ULOCK:0);;) {
		if (!fRemote) cb.pb.getPage(cb.addr.pageID,ctx->heapMgr,fctl,cb.ses);
		else if ((rc=ctx->netMgr->getPage(cb.id,fctl,cb.addr.idx,cb.pb,cb.ses))!=RC_OK) return rc;
		else {fRemote=fTry=false; cb.addr.pageID=cb.pb->getPageID();}

		if (cb.pb.isNull()) return RC_NOTFOUND;
		if (fWrite && cb.pb->isULocked()) {fctl|=QMGR_UFORCE; cb.pb.set(QMGR_UFORCE);}
		
		if (cb.fill()==NULL) {
			if (ctx->lockMgr->getTVers(cb,tvo)==RC_OK && cb.tv!=NULL) {
				// deleted in uncommitted tx?
			}
		} else if (cb.hpin->hdr.getType()==HO_FORWARD) {
			if ((flags&GB_FORWARD)!=0) {if ((cb.epr.flags&PINEX_EXTPID)!=0) cb.ses->setExtAddr(PageAddr::noAddr); return RC_OK;}
			memcpy(&cb.addr,(const byte*)cb.hpin+sizeof(HeapPageMgr::HeapObjHeader),PageAddrSize); continue;
		} else {
			PID id; IdentityID iid;
			if ((cb.epr.flags&PINEX_EXTPID)!=0) cb.ses->setExtAddr(PageAddr::noAddr);
			if (cb.hpin->hdr.getType()!=HO_PIN) return RC_CORRUPTED;
			if (!cb.hpin->getAddr(id)) {id.pid=uint64_t(cb.addr); id.ident=STORE_OWNER;}
			if (cb.id==id) {
				cb.epr.flags|=PINEX_ADDRSET; rc=RC_OK;
				if (isRemote(cb.id)) {
					//???
				} else if ((rc=ctx->lockMgr->getTVers(cb,tvo))!=RC_OK) return rc;
				if (!cb.ses->inWriteTx()) {
					if (tvo!=TVO_READ) return RC_READTX;
					if (cb.tv!=NULL) {
						// check hpin->hdr.dscr
						//...
						//cb.flags|=PINEX_TVERSION;
					}
				} else if ((flags&GB_REREAD)==0) {
					const unsigned lck=tvo==TVO_READ?PINEX_LOCKED:PINEX_XLOCKED;
					if ((cb.epr.flags&lck)==0) {
						if ((rc=ctx->lockMgr->lock(tvo==TVO_READ?LOCK_SHARED:LOCK_EXCLUSIVE,cb))!=RC_OK) {cb.pb.release(cb.ses); return rc;}
						cb.epr.flags|=lck|PINEX_LOCKED; if (cb.pb.isNull()) continue;
					}
				}
				if ((flags&GB_DELETED)==0 && (cb.hpin->hdr.descr&HOH_DELETED)!=0) return RC_DELETED;
				if (vid!=STORE_CURRENT_VERSION) {
					//???continue???
				}
				if ((flags&GB_REREAD)==0 && (iid=cb.ses->getIdentity())!=STORE_OWNER) rc=checkACLs(cb,iid,tvo,flags);
				if (rc!=RC_OK) {cb.pb.release(cb.ses); return rc;} if (cb.pb.isNull()) continue;
				cb.copyFlags(); return RC_OK;
			}
		}
		if (!fTry) {
			if (cb.hpin!=NULL && (cb.epr.flags&PINEX_EXTPID)==0) 
				report(MSG_ERROR,"getBody: page %08X corruption\n",cb.pb->getPageID());
			return RC_NOTFOUND;
		}
		if (isRemote(cb.id)) fRemote=true; else if (!cb.addr.convert(cb.id.pid)) return RC_CORRUPTED;
		fTry=false;
	}
}

RC QueryPrc::checkLockAndACL(PINx& qr,TVOp tvo,QueryOp *qop)
{
	if ((qr.epr.flags&PINEX_DERIVED)!=0) return RC_OK; if (qr.ses==NULL) return RC_NOSESSION;
	RC rc=RC_OK; IdentityID iid; bool fWasNull=qr.pb.isNull();
	if ((qr.epr.flags&PINEX_ADDRSET)==0) qr.addr=PageAddr::noAddr;
	if ((qr.mode&PIN_DELETED)==0 && qr.ses->inWriteTx() && (qr.epr.flags&(tvo!=TVO_READ?PINEX_XLOCKED:PINEX_LOCKED))==0) {
		if (qr.id.pid==STORE_INVALID_PID && (rc=qr.unpack())!=RC_OK) return rc;
		if ((rc=ctx->lockMgr->lock(tvo!=TVO_READ?LOCK_EXCLUSIVE:LOCK_SHARED,qr))==RC_OK) {
			qr.epr.flags|=tvo!=TVO_READ?PINEX_XLOCKED|PINEX_LOCKED:PINEX_LOCKED;
			if (!fWasNull && qr.pb.isNull()) rc=getBody(qr,tvo,GB_REREAD);	//???
		}
	}
	if (rc==RC_OK && (qr.epr.flags&PINEX_ACL_CHKED)==0 && (iid=qr.ses->getIdentity())!=STORE_OWNER &&
		(!qr.pb.isNull() || (rc=getBody(qr,tvo,GB_REREAD))==RC_OK) &&
		((rc=checkACLs(qr,iid,tvo))==RC_OK||rc==RC_NOACCESS)) qr.epr.flags|=PINEX_ACL_CHKED;
	return rc;
}

RC QueryPrc::checkACLs(PINx& cb,IdentityID iid,TVOp tvo,unsigned flags,bool fProp)
{
	assert(!cb.pb.isNull()); const bool fWrite=tvo!=TVO_READ;
	Value v; RC rc=RC_NOACCESS; RefTrace rt={NULL,cb.id,PROP_SPEC_ACL,STORE_COLLECTION_ID};
	if (loadV(v,PROP_SPEC_ACL,cb,0,cb.ses)==RC_OK) {
		rc=checkACL(v,cb,iid,fWrite?META_PROP_WRITE:META_PROP_READ,&rt,fProp); freeV(v);
	} else if (loadV(v,PROP_SPEC_DOCUMENT,cb,0,cb.ses)==RC_OK) {
		if (v.type==VT_REFID) {
			if (!fProp) rc=RC_FALSE;
			else {
				RefVID ref={v.id,PROP_SPEC_ACL,STORE_COLLECTION_ID,STORE_CURRENT_VERSION}; 
				v.set(ref); rt.id=v.id; rc=checkACL(v,cb,iid,fWrite?META_PROP_WRITE:META_PROP_READ,&rt);
			}
		}
		freeV(v);
	}
	return rc==RC_REPEAT?getBody(cb,tvo,GB_REREAD):rc;
}

RC QueryPrc::checkACLs(PINx *pin,PINx& cb,IdentityID iid,TVOp tvo,unsigned flags,bool fProp)
{
	RC rc=RC_NOACCESS; const bool fWrite=tvo!=TVO_READ;
	RefTrace rt={NULL,pin->id,PROP_SPEC_ACL,STORE_COLLECTION_ID}; Value v; v.setError();
	if (pin->getV(PROP_SPEC_ACL,v,LOAD_SSV,pin->ses)==RC_OK) rc=checkACL(v,cb,iid,fWrite?META_PROP_WRITE:META_PROP_READ,&rt,fProp);
	else if (pin->getV(PROP_SPEC_DOCUMENT,v,LOAD_SSV,pin->ses)==RC_OK && v.type==VT_REFID) {
		if (!fProp) rc=RC_FALSE;
		else {
			RefVID ref={v.id,PROP_SPEC_ACL,STORE_COLLECTION_ID,STORE_CURRENT_VERSION}; 
			Value v; v.set(ref); rt.id=v.id; rc=checkACL(v,cb,iid,fWrite?META_PROP_WRITE:META_PROP_READ,&rt);
		}
	}
	cb=pin->id; freeV(v);
	return rc==RC_REPEAT?getBody(cb,tvo,flags|GB_REREAD):rc;
}

RC QueryPrc::checkACL(const Value& v,PINx& cb,IdentityID iid,uint8_t mask,const RefTrace *rt,bool fProp)
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
		refTrc.id=v.refId->id; ppv=&pv; i=1; rc=getRefSafe(v.refId->id,ppv,i,0,cb);
		if (rc==RC_OK || rc==RC_REPEAT) {RC rc2=checkACL(pv,cb,iid,mask,&refTrc); freeV(pv); return rc2==RC_OK?rc:rc2;}
		break;
	case VT_ARRAY:
		for (i=0; i<v.length; i++) if ((rc=checkACL(v.varray[i],cb,iid,mask,rt,fProp))==RC_OK) return rc; else if (rc==RC_FALSE) fFalse=true;
		break;
	case VT_COLLECTION:
		for (cv=v.nav->navigate(GO_FIRST); cv!=NULL; cv=v.nav->navigate(GO_NEXT))
			if ((rc=checkACL(*cv,cb,iid,mask,rt,fProp))==RC_OK) break; else if (rc==RC_FALSE) fFalse=true;
		v.nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID); if (rc==RC_OK||rc!=RC_NOACCESS&&!fFalse) return rc;
		break;
	case VT_STRUCT:
		//???
		break;
	}
	return fFalse?RC_FALSE:RC_NOACCESS;
}

RC QueryPrc::getRefSafe(const PID& id,Value *&vals,unsigned& nValues,unsigned mode,PINx& cb)
{
	if (cb.pb.isNull()) return RC_INVPARAM; const bool fU=cb.pb->isULocked();
	const PageID pid=cb.pb->getPageID(); PINx cb2(cb.ses); RC rc=RC_OK;
	if (isRemote(id)) {
		if (ctx->netMgr->getPage(id,QMGR_TRY,cb2.addr.idx,cb2.pb,cb2.ses)!=RC_OK) {
			cb.pb.moveTo(cb2.pb); 
			if ((rc=ctx->netMgr->getPage(id,fU?QMGR_UFORCE:0,cb2.addr.idx,cb2.pb,cb2.ses))==RC_OK) rc=RC_REPEAT;
		}
	} else if (!cb2.addr.convert(id.pid)) return RC_CORRUPTED;
	else if (cb2.pb.getPage(cb2.addr.pageID,ctx->heapMgr,QMGR_TRY,cb2.ses)==NULL) {
		rc=cb.pb.getPage(cb2.addr.pageID,ctx->heapMgr,PGCTL_RLATCH,cb.ses)==NULL?RC_CORRUPTED:RC_REPEAT; cb.pb.moveTo(cb2.pb);
	}
	if (rc==RC_OK||rc==RC_REPEAT) while (!cb2.pb.isNull()) {
		if (cb2.fill()==NULL) {rc=RC_NOTFOUND; break;}
		if (cb2.hpin->hdr.getType()==HO_FORWARD) {
			memcpy(&cb2.addr,(const byte*)cb2.hpin+sizeof(HeapPageMgr::HeapObjHeader),PageAddrSize);
			if (cb2.pb.getPage(cb2.addr.pageID,ctx->heapMgr,QMGR_TRY,cb2.ses)==NULL) {
				if (!cb.pb.isNull()) cb.pb.moveTo(cb2.pb);
				rc=cb2.pb.getPage(cb2.addr.pageID,ctx->heapMgr,PGCTL_RLATCH,cb2.ses)==NULL?RC_CORRUPTED:RC_REPEAT;
			}
			continue;
		}
		if (cb2.hpin->hdr.getType()!=HO_PIN||(cb2.hpin->hdr.descr&HOH_DELETED)!=0) {rc=RC_NOTFOUND; break;}	// tv???
		if (vals==NULL && (nValues=cb2.hpin->nProps)>0) {
			if ((vals=(Value*)cb.ses->malloc(sizeof(Value)*nValues))==NULL) {rc=RC_NORESOURCES; break;}
			const HeapPageMgr::HeapV *hprop=cb2.hpin->getPropTab();
			for (unsigned i=0; i<nValues; ++hprop,++i) vals[i].property=hprop->getID();
		}
		if (vals!=NULL) for (unsigned i=0; i<nValues; i++)
			if ((rc=loadV(vals[i],vals[i].property,cb2,mode,cb.ses,vals[i].eid))!=RC_OK) break;
		break;
	}
	if (cb.pb.isNull()) {cb2.pb.getPage(pid,ctx->heapMgr,fU?PGCTL_ULOCK:0,cb2.ses); cb2.pb.moveTo(cb.pb);}
	return rc;
}

RC QueryPrc::getPINValue(const PID& id,Value& res,unsigned mode,Session *ses)
{
	PINx cb(ses,id),*pcb=&cb; Expr *exp; RC rc; Value v;
	if ((rc=getBody(cb))!=RC_OK || (rc=loadV(res,PROP_SPEC_VALUE,cb,mode|LOAD_SSV,ses))!=RC_OK) return rc;
	for (int cnt=0;;cnt++) {
		assert((res.flags&VF_SSV)==0);
		switch (res.type) {
		default: return RC_OK;
		case VT_EXPR:
			exp=(Expr*)res.expr; rc=Expr::eval(&exp,1,v,EvalCtx(ses,(PIN**)&pcb,1,NULL,0)); freeV(res); res=v;
			switch (rc) {
			default: return rc;
			case RC_TRUE: res.set(true); return RC_OK;
			case RC_FALSE: res.set(false); return RC_OK;
			}
		case VT_REFIDPROP:
		case VT_REFIDELT:
			if (cnt>0 || (rc=derefValue(res,res,ses))!=RC_OK) return rc;
			continue;
		case VT_URIID:
			if (cnt>0 || (rc=loadV(res,res.uid,cb,mode|LOAD_SSV,ses))!=RC_OK) return rc;
			continue;
		}
	}
	return rc;
}

RC QueryPrc::getCalcPropID(unsigned n,PropertyID& pid)
{
	RWLockP lck(&cPropLock,RW_S_LOCK);
	if (calcProps==NULL || n>=nCalcProps) {
		lck.set(NULL); lck.set(&cPropLock,RW_X_LOCK);
		if (calcProps==NULL || n>=nCalcProps) {
			if ((calcProps=(PropertyID*)ctx->realloc(calcProps,(n+1)*sizeof(PropertyID)))==NULL) {nCalcProps=0; return RC_NORESOURCES;}
			while (nCalcProps<=n) {
				char buf[100]; size_t l=sprintf(buf,AFFINITY_STD_URI_PREFIX CALC_PROP_NAME "%d",nCalcProps+1);
				URI *uri=ctx->uriMgr->insert(buf,l); if (uri==NULL) return RC_NORESOURCES;
				calcProps[nCalcProps++]=uri->getID(); uri->release();
			}
		}
	}
	pid=calcProps[n]; return RC_OK;
}

bool QueryPrc::checkCalcPropID(PropertyID pid)
{
	RWLockP lck(&cPropLock,RW_S_LOCK);
	if (calcProps!=NULL) for (unsigned i=0; i<nCalcProps; i++) if (calcProps[i]==pid) return true;
	return false;
}
