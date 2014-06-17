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

#include "txmgr.h"
#include "lock.h"
#include "queryprc.h"
#include "netmgr.h"
#include "stmt.h"
#include "cursor.h"
#include "parser.h"
#include "maps.h"
#include "blob.h"

using namespace AfyKernel;

QueryPrc::QueryPrc(StoreCtx *c,IStoreNotification *notItf) 
	: notification(notItf),ctx(c),bigThreshold(c->bufMgr->getPageSize()*SKEW_PAGE_PCT/100),calcProps(NULL),nCalcProps(0)
{
}

const static char *stmtOpName[STMT_OP_ALL] = {"QUERY","INSERT","UPDATE","DELETE","UNDELETE","START","COMMIT","ROLLBACK"};

RC Session::execute(const char *str,size_t lstr,char **result,const URIID *ids,unsigned nids,const Value *params,unsigned nParams,CompilationError *ce,
																			uint64_t *nProcessed,unsigned nProcess,unsigned nSkip,const char *importBase)
{
	try {
		if (ce!=NULL) {memset(ce,0,sizeof(CompilationError)); ce->msg="";}
		if (str==NULL||lstr==0) return RC_INVPARAM; if (ctx->inShutdown()) return RC_SHUTDOWN;
		SInCtx in(this,str,lstr,ids,nids,importBase,true); RC rc=RC_OK;
		try {in.exec(params,nParams,result,nProcessed,nProcess,nSkip); in.checkEnd(true);}
		catch (SynErr sy) {in.getErrorInfo(RC_SYNTAX,sy,ce); rc=RC_SYNTAX;}
		catch (RC rc2) {in.getErrorInfo(rc=rc2,SY_ALL,ce);}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::execute()\n"); return RC_INTERNAL;}
}

RC Stmt::execute(ICursor **pResult,const Value *pars,unsigned nPars,unsigned nProcess,unsigned nSkip,unsigned md,uint64_t *nProcessed,TXI_LEVEL txl)
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION; if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(ses); ses->resetAbortQ(); Values vpar(pars,nPars); EvalCtx ectx(ses,NULL,0,NULL,0,&vpar,1,NULL,NULL,op>STMT_QUERY||op<=STMT_UNDELETE?ECT_INSERT:ECT_QUERY);
		RC rc=execute(ectx,NULL,nProcess,nSkip,md,nProcessed,txl,(Cursor**)pResult); 
		ses->releaseAllLatches(); return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::execute()\n"); return RC_INTERNAL;}
}

RC Stmt::execute(const EvalCtx& ectx,Value *res,unsigned nProcess,unsigned nSkip,unsigned md,uint64_t *nProcessed,TXI_LEVEL txl,Cursor **pResult)
{
	if (res!=NULL) res->setEmpty(); RC rc=RC_OK; StoreCtx *ctx=ectx.ses->getStore(); uint64_t cnt=0ULL;
	if (nProcessed!=NULL) *nProcessed=0ULL; if (pResult!=NULL) *pResult=NULL;
	switch (op) {
	case STMT_START_TX:
		if ((mode&MODE_READONLY)==0 && ctx->isServerLocked()) return RC_READONLY;
		return ectx.ses->isRestore()?RC_OTHER:ctx->txMgr->startTx(ectx.ses,(mode&MODE_READONLY)!=0?TXT_READONLY:TXT_READWRITE,txl);
	case STMT_COMMIT:
		return ectx.ses->isRestore()?RC_OTHER:ctx->txMgr->commitTx(ectx.ses,(mode&MODE_ALL)!=0);
	case STMT_ROLLBACK:
		return ectx.ses->isRestore()?RC_OTHER:ctx->txMgr->abortTx(ectx.ses,(mode&MODE_ALL)!=0?TXA_ALL:TXA_EXTERNAL);
	case STMT_QUERY:
		if (top==NULL) return RC_OK;
		md&=MODE_HOLD_RESULT|MODE_ALL_WORDS|MODE_DELETED|MODE_FOR_UPDATE|MODE_PID; if (ectx.ses->inReadTx()) md&=~MODE_FOR_UPDATE;
		if (top->stype==SEL_CONST) {
			cnt=1;
			if (pResult!=NULL) {
				Cursor *cu=new(ectx.ma) Cursor(ectx,ectx.ect);
				if (cu==NULL) rc=RC_NOMEM;
//				else if ((rc=cu->init(this,nProcess,nSkip,mode|md,txl>=TXI_REPEATABLE_READ))!=RC_OK) cu->destroy();
				else {*pResult=cu; res=&cu->retres;}
				pResult=NULL;
			}
			if (res!=NULL) {
				assert(rc==RC_OK);
				if (top->nOuts>1) {
					//???
				} else if (top->outs->nValues>1) {
					Value w; w.set((Value*)top->outs->vals,top->outs->nValues);
					setHT(w); rc=ctx->queryMgr->eval(&w,ectx,res,md);
				} else switch(top->outs->vals->type) {
				case VT_STMT: case VT_EXPR: case VT_STRUCT:
					if (top->outs->vals->fcalc!=0) {
				case VT_VARREF: case VT_CURRENT: case VT_EXPRTREE:
						rc=ctx->queryMgr->eval(top->outs->vals,ectx,res,md); break;
					}
				default:
					if ((md&MODE_COPY_VALUES)!=0) rc=copyV(*top->outs->vals,*res,ectx.ma); else {*res=*top->outs->vals; setHT(*res);}
					break;
				}
				res=NULL;
			}
		}
		break;
	default:
		if (ectx.ect!=ECT_INSERT && ectx.ect!=ECT_ACTION) return RC_INVOP;
		if (ectx.ses->inReadTx()) return RC_READTX; if (ctx->isServerLocked()) return RC_READONLY;
		md&=MODE_FORCE_EIDS|MODE_CHECK_STAMP|MODE_ALL_WORDS|MODE_DELETED|MODE_PURGE|MODE_TEMP_ID|MODE_PID;
		md|=op==STMT_INSERT?MODE_RAW:op==STMT_UNDELETE?MODE_FOR_UPDATE|MODE_DELETED:MODE_FOR_UPDATE;
		if (top==NULL) {
			Value w,*ids=&w; w.set(PIN::noPID); PIN *pin;
			if (op==STMT_INSERT) {
				if ((mode&MODE_MANY_PINS)!=0 && nValues+nNested>1 && (ids=new(ectx.ma) Value[nValues+nNested])==NULL) rc=RC_NOMEM;
				else if ((rc=insert(ectx,ids,cnt))==RC_OK && cnt!=0 && (res!=NULL || pResult!=NULL)) {
					if (ids!=&w) {w.set(ids,(uint32_t)cnt); setHT(w,ectx.ma->getAType());}
					if (res!=NULL) {if ((w.flags&HEAP_TYPE_MASK)==ectx.ma->getAType()) *res=w; else rc=copyV(w,*res,ectx.ma);}
				}
			} else if (op!=STMT_UPDATE) rc=RC_INVOP;
			else if (ectx.env==NULL || pmode>=ectx.nEnv || (pin=ectx.env[pmode])==NULL) rc=RC_NOTFOUND;
			else if ((rc=ctx->queryMgr->modifyPIN(EvalCtx(ectx.ses,ectx.env,ectx.nEnv,&pin,1,ectx.params,ectx.nParams,ectx.stack,ectx.ma,ECT_INSERT),
											pin->id,vals,nValues,pin->fPINx!=0?(PINx*)pin:NULL,pin->fPINx==0?pin:NULL,mode|md|MODE_NO_EID))==RC_OK) {
				if (res!=NULL) {if (pin->addr.defined()) res->set(ectx.env[0]->id); else res->set(pin);} else if (pResult!=NULL) w.set(pin->id);		//??? what if inmem?
				cnt=1;
			}
			if (rc==RC_OK && pResult!=NULL) {
				Cursor *cu=new(ectx.ma) Cursor(ectx,ectx.ect);
				if (cu==NULL) rc=RC_NOMEM; else if ((rc=cu->init(w,nProcess,nSkip,md,txl>=TXI_REPEATABLE_READ||txl==TXI_DEFAULT))==RC_OK) *pResult=cu;
			}
			pResult=NULL; res=NULL;
		}
		break;
	}
	if (res!=NULL||pResult!=NULL) {
		if ((mode&MODE_MANY_PINS)!=0) return RC_INTERNAL;	//???
		Cursor *cu=new(ectx.ma) Cursor(ectx,ectx.ect);
		if (cu==NULL) rc=RC_NOMEM;
		else if ((rc=cu->init(this,nProcess,nSkip,md,txl>=TXI_REPEATABLE_READ||txl==TXI_DEFAULT))!=RC_OK) cu->destroy();
		else if (pResult!=NULL) *pResult=cu;
		else if (res!=NULL) {
			SelectType sty=top!=NULL?top->stype:cnt>1?SEL_PINSET:SEL_PIN; res->setEmpty();
			if (sty==SEL_COUNT || sty==SEL_VALUE || sty==SEL_DERIVED || sty==SEL_PIN || sty==SEL_FIRSTPID || top!=NULL&&(top->qvf&QVF_FIRST)!=0) {rc=cu->next(*res); cu->destroy();}
			else if ((rc=CursorNav::create(cu,*res,md,ectx.ma))==RC_FALSE||rc==RC_EOF) rc=RC_OK;
			if (rc==RC_OK && op==STMT_INSERT && (mode&MODE_PART)!=0) res->meta|=META_PROP_PART;
		}
	} else if (top!=NULL && (op!=STMT_QUERY || nProcessed!=NULL)) {
		Cursor cu(ectx,ectx.ect); 
		if ((rc=cu.init(this,nProcess,nSkip,md))==RC_OK)
			{if (op==STMT_QUERY) rc=cu.count(cnt,nProcess); else while ((rc=cu.advance(false))==RC_OK) cnt++;}
	}
	if (rc==RC_EOF) rc=RC_OK; if (nProcessed!=NULL) *nProcessed=cnt;
	if ((ectx.ses->getTraceMode()&TRACE_SESSION_QUERIES)!=0) trace(ectx,stmtOpName[op],rc,unsigned(cnt));
	return rc;
}

RC QueryPrc::apply(const EvalCtx& ectx,STMT_OP op,PINx& qr,const Value *values,unsigned nValues,unsigned mode,PIN *pin)
{
	RC rc=RC_OK; EvalCtxType sct=ectx.ect; ectx.ect=sct==ECT_ACTION?ECT_ACTION:ECT_INSERT;
	const bool fEnv=ectx.env!=NULL&&ectx.nEnv!=0,fRestore=!fEnv||ectx.env[0]==NULL; PIN *pq=&qr; Value w; TIMESTAMP ts=0ULL;
	if (fRestore) {if (fEnv) ectx.env[0]=pq; else {const_cast<PIN**&>(ectx.env)=(PIN**)&pq; const_cast<unsigned&>(ectx.nEnv)=1;}}
	switch (op) {
	default: rc=RC_INTERNAL; break;
	case STMT_INSERT:
		if ((qr.epr.flags&(PINEX_DERIVED|PINEX_COMM))!=0 || qr.fPartial==0 || (rc=qr.load(LOAD_SSV))==RC_OK) {
			PIN lpin(ectx.ma,0,qr.properties,qr.nProperties,true); pin=&lpin;
			for (unsigned i=0; i<nValues; i++)	{// epos ???
				// evaluate expr with params
				if ((rc=lpin.modify(&values[i],values[i].eid,STORE_COLLECTION_ID,0))!=RC_OK) break;
			}
			if (rc==RC_OK) {
				if ((qr.epr.flags&(PINEX_DERIVED|PINEX_COMM))!=0)
					for (unsigned i=0; i<lpin.nProperties && lpin.properties[i].property<=MAX_BUILTIN_URIID; i++)
						if ((rc=ectx.ses->checkBuiltinProp(lpin.properties[i],ts,true))!=RC_OK) break;
				if (rc==RC_OK) rc=persistPINs(ectx,&pin,1,mode|MODE_NO_EID);
			}
		}
		break;
	case STMT_UPDATE:
		rc=modifyPIN(ectx,qr.id,values,nValues,&qr,pin,mode|MODE_NO_EID); break;
	case STMT_DELETE:
		rc=deletePINs(ectx,NULL,&qr.id,1,mode,qr.pb.isNull()?(PINx*)0:&qr); break;
	case STMT_UNDELETE:
		w.set((unsigned)~PIN_DELETED); w.setPropID(PROP_SPEC_SELF); w.setOp(OP_AND);
		rc=modifyPIN(ectx,qr.id,&w,1,&qr,pin,MODE_DELETED); break;
	}
	if (fRestore) {if (fEnv) ectx.env[0]=NULL; else {const_cast<PIN**&>(ectx.env)=NULL; const_cast<unsigned&>(ectx.nEnv)=0;}}
	ectx.ect=sct; return rc;
}

RC Stmt::cmp(const EvalCtx& ectx,const Value& v,ExprOp op,unsigned flags/* pars??? */)
{
	if (this->op!=STMT_QUERY || top==NULL) return RC_INVOP;
	Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION; 
	if (top->stype==SEL_PID) {
		if (v.type!=VT_REFID && (v.type!=VT_REF || !v.pin->getPID().isPID())) return op==OP_EQ||op==OP_IN?RC_FALSE:RC_TRUE;
	} else if (top->stype==SEL_PINSET && (v.type==VT_REFID || v.type==VT_REF) && top->type==QRY_SIMPLE) {
		if (v.type==VT_REF) return isSatisfied(v.pin)?RC_TRUE:RC_FALSE;
		PINx cb(ses,v.id); RC rc; PIN *pp[2]={(PIN*)&cb,NULL}; unsigned np=1;
		if (ectx.env!=NULL && ectx.nEnv>=2 && (pp[1]=ectx.env[1])!=NULL) np++;
		if ((rc=cb.getBody())!=RC_OK) return rc;
		rc=checkConditions(EvalCtx(ses,pp,np,pp,1))?RC_TRUE:RC_FALSE;
		return op==OP_EQ||op==OP_IN?rc:rc==RC_TRUE?RC_FALSE:RC_TRUE;
	}
	Cursor cu(ectx); RC rc=cu.init(this,~0ULL,0,0); PINx *pin;
	if (rc==RC_OK) {
		if (top->stype==SEL_COUNT) {
			uint64_t cnt; rc=cu.count(cnt,~0u);
			if (rc!=RC_OK) return rc!=RC_EOF?rc:op==OP_EQ||op==OP_IN?RC_FALSE:RC_TRUE;
			Value w; w.setU64(cnt); int c=AfyKernel::cmp(v,w,flags,ectx.ma);
			return op!=OP_IN?Expr::condRC(c,op):c==0?RC_TRUE:RC_FALSE;
		}
		while ((rc=cu.next(pin))==RC_OK) {
			if (top->stype==SEL_PID || top->stype==SEL_FIRSTPID) {
				if (pin->id.isEmpty() && (rc=pin->unpack())!=RC_OK) break;
				rc=v.type==VT_REFID && pin->id==v.id || v.type==VT_REF && pin->id==v.pin->getPID()?RC_TRUE:RC_FALSE;
				if (op==OP_EQ||op==OP_IN) {if (rc==RC_TRUE) break;} else if (op==OP_NE && rc==RC_TRUE) {rc=RC_FALSE; break;}
				if (top->stype==SEL_FIRSTPID) {rc=RC_EOF; break;}
			} else if (top->stype==SEL_VALUE || top->stype==SEL_VALUESET) {
				if (pin->properties!=NULL && pin->nProperties==1) {
					if (isCollection((ValueType)v.type) || isCollection((ValueType)pin->properties->type)) {
						Value w=v; setHT(w); rc=Expr::calc(op,w,pin->properties,2,flags,ectx); freeV(w);
						if (rc==RC_TRUE) {if (op==OP_EQ||op==OP_IN) break;} else if (rc!=RC_FALSE || op!=OP_EQ && op!=OP_IN) break;
					} else {
						int c=AfyKernel::cmp(v,*pin->properties,flags,ectx.ma);
						if (op==OP_EQ||op==OP_IN) {if (c==0) {rc=RC_TRUE; break;}} else if ((rc=Expr::condRC(c,op))!=RC_TRUE) break;
					}
				}
				if (top->stype==SEL_VALUE) {rc=RC_EOF; break;}
			} else {
				//???
				if (top->stype==SEL_DERIVED) {rc=RC_EOF; break;}
			}
		}
	}
	return rc!=RC_EOF?rc:op==OP_EQ||op==OP_IN?RC_FALSE:RC_TRUE;
}

RC Stmt::insert(const EvalCtx& ectx,Value *ids,uint64_t& cnt) const
{
	RC rc=RC_OK; cnt=0;
	if (vals==NULL || (mode&MODE_MANY_PINS)==0 && nNested==0 || top!=NULL && top->stype==SEL_CONST && top->nOuts==1) {
		Value *pv=vals; unsigned nv=nValues; if (pv==NULL && top!=NULL) {pv=(Value*)top->outs->vals; nv=top->outs->nValues;}
		PIN pin(ectx.ses,pmode,pv,nv,true),*pp=&pin; pin.id.setTPID(tpid);
		if ((rc=ectx.ses->getStore()->queryMgr->persistPINs(ectx,&pp,1,mode|MODE_NO_EID,NULL,NULL,into,nInto))==RC_OK && (pin.mode&PIN_TRANSIENT)==0)
			{cnt=1; if (ids!=NULL) ids->set(pin.id);}
	} else {
		unsigned nV=(mode&MODE_MANY_PINS)!=0?nValues:1;
		PIN **ppins=(PIN**)ectx.ses->malloc((sizeof(PIN)+sizeof(PIN*))*(nNested+nV)); if (ppins==NULL) return RC_NOMEM;
		if ((rc=getNested(ppins,(PIN*)(ppins+nNested+nV),cnt,ectx.ses))==RC_OK) {
			assert(cnt==nNested+nV);
			if ((rc=ectx.ses->getStore()->queryMgr->persistPINs(ectx,ppins,(unsigned)cnt,mode|MODE_NO_EID,NULL,NULL,into,nInto))==RC_OK && ids!=NULL) {
				if (nV==1) ids->set(ppins[0]->id);
				else for (unsigned i=0; i<cnt; i++) if ((ppins[i]->mode&PIN_TRANSIENT)==0) ids[i].set(ppins[i]->id);	// ids cnt?
			}
		}
		for (unsigned i=0; i<cnt; i++) if (ppins[i]!=NULL) {if (cnt!=0 && (ppins[i]->mode&PIN_TRANSIENT)!=0) cnt--; ppins[i]->~PIN();}	//???
		ectx.ses->free(ppins); if (rc!=RC_OK) cnt=0;
	}
	return rc;
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

RC Stmt::asyncexec(IStmtCallback *cb,const Value *params,unsigned nParams,unsigned nProcess,unsigned nSkip,unsigned mode,TXI_LEVEL txl)
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION; 
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN;
		if ((ctx->mode&STARTUP_RT)!=0) return RC_INVOP;
		RC rc=RC_OK; StmtRequest *rq; Stmt *st; Value *pv=NULL;
		switch (op) {
		case STMT_QUERY:
		case STMT_INSERT:
		case STMT_UPDATE:
		case STMT_DELETE:
		case STMT_UNDELETE:
			if ((st=clone(op,ctx))==NULL) {rc=RC_NOMEM; break;}
			if (params!=NULL && (rc=copyV(params,nParams,pv,ctx))!=RC_OK) {st->destroy(); break;}
			if ((rq=new(ctx) StmtRequest(st,cb,pv,nParams,nProcess,nSkip,mode,txl,ctx))==NULL)
				{st->destroy(); freeV(pv,nParams,ctx); rc=RC_NOMEM; break;}
			if (!RequestQueue::postRequest(rq,ctx)) {rq->destroy(); rc=RC_OTHER;}
			break;
		default: rc=RC_INVOP; break;
		}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::asyncexec()\n"); return RC_INTERNAL;}
}

RC Stmt::count(uint64_t& cnt,const Value *pars,unsigned nPars,unsigned nAbort,unsigned md,TXI_LEVEL txl) const
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION; if (top==NULL) return RC_INVPARAM;
		if (ses->getStore()->inShutdown()) return RC_SHUTDOWN; ses->resetAbortQ();
		TxGuard txg(ses); md&=MODE_ALL_WORDS|MODE_DELETED; Values vp(pars,nPars); 
		Cursor cu(EvalCtx(ses,NULL,0,NULL,0,&vp,1)); RC rc; PINx *pin;
		if ((top->qvf&QVF_FIRST)==0) {
			if ((rc=cu.init((Stmt*)this,~0ULL,0,md|MODE_COUNT))==RC_OK) rc=cu.count(cnt,nAbort,orderBy,nOrderBy);
		} else if ((rc=cu.init((Stmt*)this,~0ULL,0,md))==RC_OK && (rc=cu.next(pin))==RC_OK) cnt=1ULL;
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
		TxGuard txg(ses); ses->resetAbortQ(); md&=MODE_ALL_WORDS|MODE_DELETED; Values vp(pars,nPars);
		Cursor cu(EvalCtx(ses,NULL,0,NULL,0,&vp,1)); RC rc; PINx *pin;
		if ((rc=cu.init((Stmt*)this,~0ULL,0,md))==RC_OK) rc=cu.next(pin);
		if ((ses->getTraceMode()&TRACE_SESSION_QUERIES)!=0) trace(ses,"EXIST",rc,1);
		return rc==RC_OK?RC_TRUE:RC_FALSE;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::exists()\n"); return RC_INTERNAL;}
}

RC Stmt::analyze(char *&plan,const Value *pars,unsigned nPars,unsigned md) const
{
	try {
		Session *ses=Session::getSession(); plan=NULL;
		if (ses==NULL) return RC_NOSESSION; if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(ses); ses->resetAbortQ(); Values vp(pars,nPars); 
		Cursor cu(EvalCtx(ses,NULL,0,NULL,0,&vp,1)); RC rc;
		if ((rc=cu.init((Stmt*)this,~0ULL,0,md&(MODE_ALL_WORDS|MODE_DELETED)))==RC_OK && cu.queryOp!=NULL)
			{SOutCtx buf(ses); cu.queryOp->print(buf,0); plan=(char*)buf;}
		return rc==RC_EOF?RC_OK:rc;
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
		TxGuard txg(ses); Values vv(pars,nPars); PIN *pp=(PIN*)p;
		return checkConditions(EvalCtx(ses,&pp,1,&pp,1,&vv,1));
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IStmt::isSatisfied()\n");}
	return false;
}

bool Stmt::checkConditions(const EvalCtx& ectx,unsigned start,bool fIgnore) const
{
	QVar *qv=top; if (qv==NULL) return true;
	PIN *pin=ectx.env!=NULL&&ectx.nEnv!=0&&ectx.env[0]!=NULL?ectx.env[0]:ectx.vars!=NULL&&ectx.nVars!=0?ectx.vars[0]:NULL;
	if (qv->props!=NULL && qv->nProps!=0 && (pin==NULL||!pin->defined(qv->props,qv->nProps))) return false;
	if (qv->type==QRY_SIMPLE) {
		const SimpleVar *cv=(SimpleVar*)qv;
		if (cv->srcs!=NULL) for (unsigned i=start; i<cv->nSrcs; i++) {
			SourceSpec& cs=cv->srcs[i]; DataEvent *dev;
			if ((dev=ectx.ses->getStore()->classMgr->getDataEvent(cs.objectID))==NULL) return false;
			const Stmt *cqry=dev->getQuery(); const QVar *cqv; Values vv(cs.params,cs.nParams);
			if (cqry==NULL || (cqv=cqry->top)==NULL || cqv->cond!=NULL && !cqv->cond->condSatisfied(EvalCtx(ectx.ses,ectx.env,ectx.nEnv,ectx.vars,ectx.nVars,&vv,1)) ||
				cqv->props!=NULL && cqv->nProps!=0 && (pin==NULL || !pin->defined(cqv->props,cqv->nProps))) {dev->release(); return false;}
			dev->release();
		}
		if (!fIgnore || ectx.nParams!=0 && ectx.params!=NULL && ectx.params[0].vals!=NULL && ectx.params[0].nValues!=0) 
			for (CondIdx *ci=cv->condIdx; ci!=NULL; ci=ci->next) {
				if (ci->param>=ectx.params[0].nValues||pin==NULL) return false;
				Value v; const Value *par=&ectx.params[0].vals[ci->param];
				if (ci->expr!=NULL) {
					// ???
				} else if (pin->getV(ci->ks.propID,v,LOAD_SSV,NULL)!=RC_OK) return false;
				RC rc=Expr::calc((ExprOp)ci->ks.op,v,par,2,(ci->ks.flags&ORD_NCASE)!=0?CND_NCASE:0,ectx.ses);
				freeV(v); if (rc!=RC_TRUE) return false;
			}
	}
	return qv->cond->condSatisfied(ectx);
}

RC QueryPrc::eval(const Value *pv,const EvalCtx& ectx,Value *res,unsigned mode)
{
	RC rc; Expr *expr; unsigned pidx; Value *pv2; TIMESTAMP ts;
	//if ((mode&MODE_ASYNC)!=0) {
		//create sync eval block + copy ectx
		//return ectx.ses->addOnCommit(...);
	//}
	PropertyID pid=pv->property; uint8_t meta=pv->meta,op=pv->op;
	switch (pv->type) {
	default: return RC_TYPE;
	case VT_VARREF:
		if (res!=NULL) switch (pv->refV.flags&VAR_TYPE_MASK) {
		case VAR_PARAM:
			if (pv->length!=0) {
				rc=RC_NOTFOUND;
				if (ectx.params!=NULL&&ectx.nParams>QV_PARAMS) for (unsigned i=0; i<ectx.params[QV_PARAMS].nValues; i++)
					if (ectx.params[QV_PARAMS].vals[i].property==pv->refV.id) {*res=ectx.params[QV_PARAMS].vals[i]; setHT(*res); rc=RC_OK; break;}
				if (rc!=RC_OK) return rc; break;
			}
		default:
			pidx=((pv->refV.flags&VAR_TYPE_MASK)>>13)-1;
			if (ectx.params==NULL||pidx>=ectx.nParams||pv->refV.refN>=ectx.params[pidx].nValues) return RC_NOTFOUND;
			*res=ectx.params[pidx].vals[pv->refV.refN]; setHT(*res); break;
		case VAR_NAMED:
			assert(pv->property!=STORE_INVALID_URIID);
			if (pv->length==0||pv->refV.id==PROP_SPEC_SELF||pv->refV.id==PROP_SPEC_PINID) rc=ctx->namedMgr->getNamedPID(pv->property,*res);
			else {PINx cb(ectx.ses); if ((rc=ctx->namedMgr->getNamed(pv->property,cb))==RC_OK) rc=cb.getV(pv->refV.id,*res,LOAD_SSV,ectx.ma);}
			if (rc!=RC_OK) return rc; break;
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
		if (res!=NULL && ((rc=Expr::compile((ExprNode*)pv->exprt,expr,ectx.ses,false))!=RC_OK || (rc=expr->eval(*res,ectx))!=RC_OK)) return rc;
		break;
	case VT_EXPR:
		if (res!=NULL && (rc=((Expr*)pv->expr)->eval(*res,ectx))!=RC_OK) return rc;
		break;
	case VT_STMT:
		if ((rc=((Stmt*)pv->stmt)->execute(ectx,res,~0u,0,mode&(MODE_COPY_VALUES|MODE_PID)))!=RC_OK) return rc;
		break;
	case VT_COLLECTION:
		if (pv->isNav()) {
			//????
			break;
		}
	case VT_STRUCT:
		if (res==NULL) pv2=(Value*)pv->varray;
		else if ((pv2=new(ectx.ma) Value[pv->length])==NULL) return RC_NOMEM;
		else memcpy(pv2,pv->varray,pv->length*sizeof(Value));
		for (unsigned i=0; i<pv->length; i++) {
			switch (pv2[i].type) {
			case VT_VARREF: case VT_CURRENT: case VT_EXPRTREE: break;
			case VT_STMT: case VT_EXPR: case VT_STRUCT: if (pv2[i].fcalc!=0||ectx.ect==ECT_ACTION) break;
			default: if (res!=NULL) setHT(pv2[i]); continue;
			}
			Value w; RC rc=eval(&pv2[i],ectx,res!=NULL?&w:NULL);
			if (rc!=RC_OK) {
				if (res!=NULL && i!=0) freeV(pv2,i-1,ectx.ma);
				return rc;
			}
			if (res!=NULL) {
				if (pv->type==VT_COLLECTION) w.eid=pv2[i].eid; else w.property=pv2[i].property;
				w.meta=pv2[i].meta; pv2[i]=w;
			}
		}
		if (res!=NULL && (rc=pv->type==VT_COLLECTION?ExprNode::normalizeCollection(pv2,pv->length,*res,ectx.ma,ectx.ses->getStore()):
												ExprNode::normalizeStruct(ectx.ses,pv2,pv->length,*res,ectx.ma,false))!=RC_OK) return rc;
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
	}
	if (res!=NULL) {
		if ((mode&MODE_INMEM)!=0 && res->type==VT_COLLECTION && res->isNav()) {
			DynArray<Value,32> arr((MemAlloc*)ectx.ses); uint32_t nVals; Value *pv,w;
			for (const Value *cv=res->nav->navigate(GO_FIRST); cv!=NULL; cv=res->nav->navigate(GO_NEXT))
				if ((rc=copyV(*cv,w,ectx.ses))!=RC_OK || (rc=arr+=w)!=RC_OK) {pv=(Value*)arr.get(nVals); freeV(pv,nVals,ectx.ses); return rc;}
			freeV(*res); if (arr==1) *res=arr[0]; else {pv=(Value*)arr.get(nVals); if (pv!=NULL && nVals!=0) res->set(pv,nVals);}
		}
		res->property=pid; res->meta=meta; res->op=op; res->fcalc=0;
	}
	return RC_OK;
}

RC QueryPrc::getPINValue(const PID& id,Value& res,unsigned mode,Session *ses)
{
	PINx cb(ses,id),*pcb=&cb; RC rc; Value v;
	if ((rc=cb.getBody())!=RC_OK || (rc=cb.getV(PROP_SPEC_VALUE,res,mode|LOAD_SSV,ses))!=RC_OK) return rc;
	for (int cnt=0;;cnt++) {
		assert((res.flags&VF_SSV)==0);
		switch (res.type) {
		default: return RC_OK;
		case VT_EXPR:
			rc=((Expr*)res.expr)->eval(v,EvalCtx(ses,(PIN**)&pcb,1,NULL,0)); freeV(res); res=v;
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
			if (cnt>0 || (rc=cb.getV(res.uid,res,mode|LOAD_SSV,ses))!=RC_OK) return rc;
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
			if ((calcProps=(PropertyID*)ctx->realloc(calcProps,(n+1)*sizeof(PropertyID),nCalcProps*sizeof(PropertyID)))==NULL) {nCalcProps=0; return RC_NOMEM;}
			while (nCalcProps<=n) {
				char buf[100]; size_t l=sprintf(buf,AFFINITY_STD_URI_PREFIX CALC_PROP_NAME "%d",nCalcProps+1);
				URI *uri=ctx->uriMgr->insert(buf,l); if (uri==NULL) return RC_NOMEM;
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

bool QueryPrc::test(PIN *pin,DataEventID classID,const EvalCtx& ectx,bool fIgnore)
{
	DataEvent *dev; bool rc=false; 
	if ((dev=ctx->classMgr->getDataEvent(classID))!=NULL) {
		const Stmt *cqry=dev->getQuery(); 
		if (cqry!=NULL) {
			PINx pc(ectx.ses,dev->getPID()); PIN *pp[2]={pin,(PIN*)&pc};
			EvalCtx ectx2(ectx.ses,pp,2,pp,1,ectx.params,ectx.nParams,ectx.stack,NULL,ectx.ect);
			rc=cqry->checkConditions(ectx2,0,fIgnore);
		}
		dev->release();
	}
	return rc;
}
