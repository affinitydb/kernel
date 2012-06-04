/**************************************************************************************

Copyright © 2004-2012 VMware, Inc. All rights reserved.

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

QueryPrc::QueryPrc(StoreCtx *c,IStoreNotification *notItf,IStoreReplication *repl) 
	: notification(notItf),replication(repl),ctx(c),bigThreshold(c->bufMgr->getPageSize()*SKEW_PAGE_PCT/100),calcProps(NULL),nCalcProps(0)
{
}

const static char *stmtOpName[STMT_OP_ALL] = {"QUERY","INSERT","UPDATE","DELETE","UNDELETE","START","COMMIT","ROLLBACK","ISOLATION"};


RC SessionX::execute(const char *str,size_t lstr,char **result,const URIID *ids,unsigned nids,const Value *params,unsigned nParams,CompilationError *ce,
																								uint64_t *nProcessed,unsigned nProcess,unsigned nSkip)
{
	try {
		assert(ses==Session::getSession()); if (ce!=NULL) {memset(ce,0,sizeof(CompilationError)); ce->msg="";}
		if (str==NULL||lstr==0) return RC_INVPARAM; if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		SInCtx in(ses,str,lstr,ids,nids,(ses->itf&ITF_SPARQL)!=0?SQ_SPARQL:SQ_PATHSQL); RC rc=RC_OK;
		try {in.exec(params,nParams,result,nProcessed,nProcess,nSkip); in.checkEnd(true);}
		catch (SynErr sy) {in.getErrorInfo(RC_SYNTAX,sy,ce); rc=RC_SYNTAX;}
		catch (RC rc2) {in.getErrorInfo(rc=rc2,SY_ALL,ce);}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::execute()\n"); return RC_INTERNAL;}
}


RC Stmt::execute(ICursor **pResult,const Value *pars,unsigned nPars,unsigned nProcess,unsigned nSkip,unsigned long md,uint64_t *nProcessed,TXI_LEVEL txl) const
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION;
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return RC_SHUTDOWN;
		if (nProcessed!=NULL) *nProcessed=0ULL; if (pResult!=NULL) *pResult=NULL;
		TxGuard txg(ses); ses->resetAbortQ(); RC rc=RC_OK; QueryOp *qop=NULL; uint64_t cnt=0; STMT_OP sop=op; 
		Value *vals=NULL; unsigned nVals=0; if (ei!=NULL) {vals=ei->values; nVals=ei->nValues;}
		switch (op) {
		case STMT_START_TX:
			if ((mode&MODE_READONLY)==0 && ctx->isServerLocked()) return RC_READONLY;
			return ses->isRestore()?RC_OTHER:ctx->txMgr->startTx(ses,(mode&MODE_READONLY)!=0?TXT_READONLY:TXT_READWRITE,txl);
		case STMT_COMMIT:
			return ses->isRestore()?RC_OTHER:ctx->txMgr->commitTx(ses,(mode&MODE_ALL)!=0);
		case STMT_ROLLBACK:
			return ses->isRestore()?RC_OTHER:ctx->txMgr->abortTx(ses,(mode&MODE_ALL)!=0);
		case STMT_ISOLATION:
			//????
			return RC_OK;
		case STMT_PREFIX:
			if (vals==NULL || nVals!=2) return RC_INTERNAL;
			return vals[0].type==VT_ANY?ses->setBase(vals[1].str,vals[1].length):ses->setPrefix(vals[0].str,vals[0].length,vals[1].str,vals[1].length);
		case STMT_QUERY:
			if (pResult==NULL) return nProcessed!=NULL?count(*nProcessed,pars,nPars,nProcess,md,txl):RC_INVPARAM;
			md&=MODE_HOLD_RESULT|MODE_ALL_WORDS|MODE_SSV_AS_STREAM|MODE_DELETED|MODE_FORCED_SSV_AS_STREAM|MODE_VERBOSE|MODE_FOR_UPDATE|MODE_COPY_VALUES;
			if (ses->inReadTx()) md&=~MODE_FOR_UPDATE; break;
		default:
			if (ses->inReadTx()) return RC_READTX; if (ctx->isServerLocked()) return RC_READONLY;
			md&=MODE_FORCE_EIDS|MODE_CHECK_STAMP|MODE_ALL_WORDS|MODE_DELETED|MODE_VERBOSE|MODE_PURGE|MODE_COPY_VALUES|MODE_TEMP_ID;
			if (op!=STMT_INSERT) md|=MODE_FOR_UPDATE; if (op==STMT_UNDELETE) md|=MODE_DELETED;
			if (op==STMT_INSERT && top==NULL) {
				PID id; unsigned cnt;
				if ((rc=insert(ses,id,cnt,pars,nPars))==RC_OK && nProcessed!=NULL) *nProcessed=cnt;
				if (rc!=RC_OK || pResult==NULL) return rc; sop=STMT_QUERY; vals=NULL; nVals=0; QCtx *qc;
				if ((qc=new(ses) QCtx(ses))==NULL || (qop=new(ses,1) ArrayScan(qc,&id,1,0))==NULL) return RC_NORESOURCES;
			}
			break;
		}
		if (qop==NULL) {QBuildCtx qctx(ses,ValueV(pars,nPars),this,nSkip,md); if ((rc=qctx.process(qop))!=RC_OK && rc!=RC_EOF) {delete qop; return rc;}}
		if (txl==TXI_DEFAULT && (txl=(TXI_LEVEL)ses->getIsolationLevel())==TXI_DEFAULT) txl=TXI_REPEATABLE_READ;
		if (pResult!=NULL) {
			if (sop==STMT_QUERY || vals==NULL || op==STMT_INSERT && top==NULL || (rc=copyV(vals,nVals,vals,ses))==RC_OK) {
				Cursor *result=new(ses) Cursor(qop,nProcess,mode|md,vals,nVals,ses,sop,top!=NULL?top->stype:SEL_PINSET,txl>=TXI_REPEATABLE_READ);
				if (result==NULL) {if (vals!=NULL) freeV(vals,nVals,ses); rc=RC_NORESOURCES;}
				else if ((rc=result->connect())!=RC_OK) {result->destroy(); if (vals!=NULL) freeV(vals,nVals,ses);}
				else {*pResult=result; qop=NULL;}
			}
		} else if (rc!=RC_EOF) {
			PINEx qr(ses),*pqr=&qr; qop->connect(&pqr); TxSP tx(ses); if ((rc=tx.start(txl))!=RC_OK) return rc;
			while (rc==RC_OK && (nProcess==~0u || cnt<nProcess) && (rc=qop->next())==RC_OK) {
				assert(qr.pb.isNull() || qr.addr.defined());
				if (sop!=STMT_UNDELETE && ((rc=ctx->queryMgr->checkLockAndACL(qr,sop==STMT_INSERT?TVO_READ:TVO_UPD,qop))==RC_NOACCESS || rc==RC_NOTFOUND)) rc=RC_OK;
				else if (rc==RC_OK && (rc=ctx->queryMgr->apply(ses,sop,qr,vals,nVals,mode|md,qop->getQCtx()->vals[QV_PARAMS]))==RC_OK) cnt++;
				else if (rc==RC_NOACCESS || rc==RC_DELETED) rc=RC_OK;
			}
			if (rc==RC_EOF || rc==RC_OK) {rc=RC_OK; tx.ok();} else cnt=~0ULL;
		}
		delete qop; ses->releaseAllLatches();
		if ((ses->getTraceMode()&TRACE_SESSION_QUERIES)!=0) trace(ses,stmtOpName[op],rc,ulong(cnt),pars,nPars,vals,nVals);
		if (nProcessed!=NULL) *nProcessed=cnt; return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::execute()\n"); return RC_INTERNAL;}
}

RC Stmt::cmp(const Value& v,ExprOp op,unsigned flags/* pars??? */)
{
	if (this->op!=STMT_QUERY || top==NULL) return RC_INVOP;
	Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION; 
	if (top->stype==SEL_PINSET && (v.type==VT_REFID || v.type==VT_REF) && top->type==QRY_SIMPLE) {
		if (v.type==VT_REF) return isSatisfied(v.pin)?RC_TRUE:RC_FALSE;
		PINEx cb(ses,v.id); RC rc;
		return (rc=ses->getStore()->queryMgr->getBody(cb))!=RC_OK?rc:checkConditions(&cb,ValueV(NULL,0),ma)?RC_TRUE:RC_FALSE;
	}
	QueryOp *qop=NULL; QBuildCtx qctx(ses,ValueV(NULL,0),this,0,0); RC rc=qctx.process(qop);
	if (rc==RC_OK) {
		if (top->stype==SEL_COUNT) {
			uint64_t cnt; rc=ses->getStore()->queryMgr->count(qop,cnt,~0u); delete qop;
			if (rc!=RC_OK) return rc!=RC_EOF?rc:op==OP_EQ||op==OP_IN?RC_FALSE:RC_TRUE;
			Value w; w.setU64(cnt); int c=AfyKernel::cmp(v,w,flags);
			return op!=OP_IN?Expr::condRC(c,op):c==0?RC_TRUE:RC_FALSE;
		}
		PINEx qr(ses),*pqr=&qr; qop->connect(&pqr); const bool fChecked=top->stype!=SEL_PINSET && top->stype!=SEL_AUGMENTED && top->stype!=SEL_COMPOUND;
		while ((rc=qop->next())==RC_OK) if (fChecked || (rc=ses->getStore()->queryMgr->checkLockAndACL(qr,TVO_READ,qop))!=RC_NOACCESS && rc!=RC_NOTFOUND && rc!=RC_DELETED) {
			if (rc!=RC_OK) break;
			if (top->stype==SEL_VALUE || top->stype==SEL_VALUESET) {
				if (qr.properties!=NULL && qr.nProperties==1) {
					int c=AfyKernel::cmp(v,*qr.properties,flags);
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

RC Stmt::insert(Session *ses,PID& id,unsigned& cnt,const Value *pars,unsigned nPars) const
{
	RC rc=RC_OK; id=PIN::defPID; cnt=0; 
	if (ei==NULL) {
		PIN pin(ses,PIN::defPID,PageAddr::invAddr,PIN_NO_FREE,NULL,0),*pp=&pin;
		if ((rc=ses->getStore()->queryMgr->commitPINs(ses,&pp,1,mode,ValueV(pars,nPars)))==RC_OK) {id=pin.id; cnt=1;}
	} else if (ei->nNested!=0) {
		PIN **ppins=(PIN**)ses->malloc((sizeof(PIN)+sizeof(PIN*))*(ei->nNested+1)); if (ppins==NULL) return RC_NORESOURCES;
		if ((rc=getNested(ppins,(PIN*)(ppins+ei->nNested+1),/*insParams!=NULL?insParams->mode:0,*/cnt,ses))==RC_OK) {
			assert(cnt==ei->nNested+1);
			if ((rc=ses->getStore()->queryMgr->commitPINs(ses,ppins,cnt,mode,ValueV(pars,nPars),NULL,NULL,ei->into,ei->nInto))==RC_OK) id=ppins[0]->id;
		}
		for (unsigned i=0; i<cnt; i++) if (ppins[i]!=NULL) ppins[i]->~PIN();
		ses->free(ppins);
	} else {
		Value *pv=ei->values; ulong flg=PIN_NO_FREE|ei->mode;
		if (pv!=NULL && ei->nValues!=0 && (mode&MODE_WITH_EVAL)!=0) {
			if ((pv=new(ses) Value[ei->nValues])==NULL) return RC_NORESOURCES;
			memcpy(pv,ei->values,ei->nValues*sizeof(Value)); flg&=~PIN_NO_FREE;
			for (unsigned i=0; i<ei->nValues; i++) pv[i].flags=NO_HEAP;
		}
		PIN pin(ses,PIN::defPID,PageAddr::invAddr,flg,pv,ei->nValues),*pp=&pin;
		if ((rc=ses->getStore()->queryMgr->commitPINs(ses,&pp,1,mode,ValueV(pars,nPars),NULL,NULL,ei->into,ei->nInto))==RC_OK) {id=pin.id; cnt=1;}
	}
	return rc;
}

RC QueryPrc::eval(Session *ses,const Value *pv,Value& res,PINEx **vars,ulong nVars,const ValueV *params,unsigned nParams,MemAlloc *ma,bool fInsert)
{
	RC rc; Expr *expr; ICursor *ic; CursorNav *cn; const Value *pars; unsigned nPars;
	switch (pv->type) {
	default: return RC_TYPE;
	case VT_VARREF:
		if ((pv->refV.flags&VAR_TYPE_MASK)!=0) {
			unsigned pidx=((pv->refV.flags&VAR_TYPE_MASK)>>13)-1;
			if (params==NULL||pidx>=nParams||pv->refV.refN>=params[pidx].nValues) return RC_NOTFOUND;
			res=params[pidx].vals[pv->refV.refN]; res.flags=NO_HEAP;
		} else {
			if (pv->refV.refN>=nVars) return RC_INVPARAM;
			if (pv->length!=0 && pv->refV.id!=PROP_SPEC_SELF) return loadV(res,pv->refV.id,*vars[pv->refV.refN],LOAD_SSV,ma,pv->eid);
			//loadProps???
			res.set((PIN*)vars[pv->refV.refN]); return RC_OK;
		}
		break;
	case VT_EXPRTREE:
		if ((rc=Expr::compile((ExprTree*)pv->exprt,expr,ses,false))!=RC_OK ||
			(rc=Expr::eval(&expr,1,res,vars,nVars,params,nParams,ses))!=RC_OK) return rc;
		break;
	case VT_EXPR:
		if ((rc=Expr::eval((Expr**)&pv->expr,1,res,vars,nVars,params,nParams,ses))!=RC_OK) return rc;
		break;
	case VT_STMT:
		if (params!=NULL && nParams!=0) {pars=params->vals; nPars=params->nValues; } else {pars=NULL; nPars=0;}
		switch (((Stmt*)pv->stmt)->op) {
		default: rc=RC_INVOP; break;
		case STMT_INSERT:
			if (!fInsert) return RC_INVOP;
			if (((Stmt*)pv->stmt)->top==NULL) {
				PID id; unsigned cnt;
				if ((rc=((Stmt*)pv->stmt)->insert(ses,id,cnt,pars,nPars))!=RC_OK) return rc;
				res.set(id); break;
			}
		case STMT_QUERY:
			if (((Stmt*)pv->stmt)->top==NULL) return RC_INVOP;
			if ((rc=pv->stmt->execute(&ic,pars,nPars))!=RC_OK) return rc;
			switch (((Cursor*)ic)->selectType()) {
			case SEL_CONST: case SEL_COUNT: case SEL_VALUE: case SEL_DERIVED:
				rc=ic->next(res); ic->destroy(); break;
			default:
				if ((cn=new(ses) CursorNav((Cursor*)ic))==NULL) return RC_NORESOURCES;	// page locked???
				res.set(cn); res.flags=SES_HEAP; break;
			}
			break;
		}
		break;
	}
	return RC_OK;
}

RC QueryPrc::apply(Session *ses,STMT_OP op,PINEx& qr,const Value *values,unsigned nValues,unsigned mode,const ValueV& params,PIN *pin)
{
	RC rc=RC_INTERNAL; bool f;
	switch (op) {
	default: break;
	case STMT_INSERT:
		rc=RC_OK;
		if ((f=pin!=NULL) || (rc=ctx->queryMgr->loadPIN(ses,qr.id,pin,mode,&qr))==RC_OK) {
			const_cast<PID&>(pin->id)=PIN::defPID; pin->addr=PageAddr::invAddr;
			for (unsigned i=0; i<nValues; i++)	{// epos ???
				// evaluate expr with params
				if ((rc=pin->modify(&values[i],values[i].eid,STORE_COLLECTION_ID,0,ses))!=RC_OK) break;
			}
			if (rc==RC_OK) rc=ctx->queryMgr->commitPINs(ses,&pin,1,mode|MODE_NO_EID,params);
			if (!f) pin->destroy();
		}
		break;
	case STMT_UPDATE:
		rc=ctx->queryMgr->modifyPIN(ses,qr.id,values,nValues,&qr,params,pin,mode|MODE_NO_EID); break;
	case STMT_DELETE:
		rc=ctx->queryMgr->deletePINs(ses,NULL,&qr.id,1,mode,qr.pb.isNull()?(PINEx*)0:&qr); break;
	case STMT_UNDELETE:
		rc=ctx->queryMgr->setFlag(ses,qr.id,&qr.addr,HOH_DELETED,true); 	break;
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
		const unsigned	long	mode;
		const TXI_LEVEL			txl;
		StoreCtx		*const	ctx;
		ICursor					*res;
	public:
		StmtRequest(Stmt *st,IStmtCallback *c,const Value *pars,unsigned nPars,unsigned nP,unsigned nS,unsigned long md,TXI_LEVEL t,StoreCtx *ct) 
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

RC Stmt::asyncexec(IStmtCallback *cb,const Value *params,unsigned nParams,unsigned nProcess,unsigned nSkip,unsigned long mode,TXI_LEVEL txl) const
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
			if ((rq=new(ctx) StmtRequest(st,cb,pv,nParams,nProcess,nSkip,mode,txl==TXI_DEFAULT?(TXI_LEVEL)ses->getIsolationLevel():txl,ctx))==NULL)
				{st->destroy(); freeV(pv,nParams,ctx); rc=RC_NORESOURCES; break;}
			if (!RequestQueue::postRequest(rq,ctx)) {rq->destroy(); rc=RC_OTHER;}
			break;
		default: rc=RC_INVOP; break;
		}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::asyncexec()\n"); return RC_INTERNAL;}
}

RC QueryPrc::count(QueryOp *qop,uint64_t& cnt,unsigned long nAbort,const OrderSegQ *os,unsigned nos)
{
	RC rc=RC_OK; cnt=0ULL;
	if (qop->getSession()->getIdentity()==STORE_OWNER) rc=qop->count(cnt,nAbort);
	else {
		// nulls in sort?
		PINEx qr(qop->getSession()),*pqr=&qr; qop->connect(&pqr);
		while ((rc=qop->next())==RC_OK)
			if ((rc=checkLockAndACL(qr,TVO_READ,qop))!=RC_NOACCESS && rc!=RC_NOTFOUND) {
				if (rc!=RC_OK) break;
				cnt++;
			}
		if (rc==RC_EOF) rc=RC_OK;
	}
	return rc;
}

RC Stmt::count(uint64_t& cnt,const Value *pars,unsigned nPars,unsigned long nAbort,unsigned long md,TXI_LEVEL txl) const
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION;
		if (ses->getStore()->inShutdown()) return RC_SHUTDOWN; ses->resetAbortQ();
		TxGuard txg(ses); md&=MODE_ALL_WORDS|MODE_DELETED|MODE_VERBOSE;
		QBuildCtx qctx(ses,ValueV(pars,nPars),this,0,md|MODE_COUNT); QueryOp *qop=NULL; RC rc=qctx.process(qop);
		if (rc==RC_OK) {rc=ses->getStore()->queryMgr->count(qop,cnt,nAbort,orderBy,nOrderBy); delete qop;}
		switch (rc) {
		default: cnt=~0ULL; break;
		case RC_EOF: cnt=0; rc=RC_OK; break;
		case RC_OK: break;
		}
		if ((ses->getTraceMode()&TRACE_SESSION_QUERIES)!=0) trace(ses,"COUNT",rc,(ulong)cnt,pars,nPars);
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::count()\n"); return RC_INTERNAL;}
}

RC Stmt::exist(const Value *pars,unsigned nPars,unsigned long md,TXI_LEVEL txi) const
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION;
		if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(ses); ses->resetAbortQ(); md&=MODE_ALL_WORDS|MODE_DELETED|MODE_VERBOSE;
		QBuildCtx qctx(ses,ValueV(pars,nPars),this,0,md); QueryOp *qop=NULL; RC rc=qctx.process(qop);
		if (rc==RC_OK) {
			assert(qop!=NULL); StoreCtx *ctx=ses->getStore();
			PINEx qr(ses),*pqr=&qr; qop->connect(&pqr);
			while ((rc=qop->next())==RC_OK)
				if ((rc=ctx->queryMgr->checkLockAndACL(qr,TVO_READ,qop))!=RC_NOACCESS && rc!=RC_NOTFOUND) break;
			qr.cleanup(); delete qop;
		}
		if ((ses->getTraceMode()&TRACE_SESSION_QUERIES)!=0) trace(ses,"EXIST",rc,1,pars,nPars);
		return rc==RC_OK?RC_TRUE:RC_FALSE;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::exists()\n"); return RC_INTERNAL;}
}

RC Stmt::analyze(char *&plan,const Value *pars,unsigned nPars,unsigned long md) const
{
	try {
		Session *ses=Session::getSession(); plan=NULL;
		if (ses==NULL) return RC_NOSESSION; if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(ses); ses->resetAbortQ();
		QBuildCtx qctx(ses,ValueV(pars,nPars),this,0,md&(MODE_ALL_WORDS|MODE_DELETED));
		QueryOp *qop=NULL; RC rc=qctx.process(qop);
		if (rc==RC_OK && qop!=NULL) {SOutCtx buf(ses); qop->print(buf,0); plan=(char*)buf;}
		delete qop; return rc==RC_EOF?RC_OK:rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::analyze()\n"); return RC_INTERNAL;}
}

void Stmt::trace(Session *ses,const char *op,RC rc,ulong cnt,const Value *pars,unsigned nPars,const Value *mods,unsigned nMods) const
{
	char *str=toString();
	if (str==NULL) ses->trace(-1,"%s: BAD QUERY",op); else {ses->trace(0,"%s %s",op,str); free(str,SES_HEAP);}
	if (pars!=NULL && nPars>0) {
		// trace params
	}
	if (mods!=NULL && nMods>0) {
		//...
	}
	if (rc!=RC_OK) {
		//...  rc
	} else if (cnt!=~0ul) ses->trace(0," -> %lu",cnt);
	ses->trace(0,"\n");
}

bool Stmt::isSatisfied(const IPIN *p,const Value *pars,unsigned nPars,unsigned long) const
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL||ses->getStore()->inShutdown()) return false;
		TxGuard txg(ses); PINEx pex(ses); pex=(PIN*)p; return checkConditions(&pex,ValueV(pars,nPars),ses);
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IStmt::isSatisfied()\n");}
	return false;
}

bool Stmt::checkConditions(PINEx *pex,const ValueV& pars,MemAlloc *ma,ulong start,bool fIgnore) const
{
	QVar *qv=top; if (qv==NULL) return true;
	if (qv->type==QRY_SIMPLE) {
		const SimpleVar *cv=(SimpleVar*)qv;
		if (cv->classes!=NULL) {
			StoreCtx *ctx=StoreCtx::get();
			for (ulong i=start; i<cv->nClasses; i++) {
				ClassSpec& cs=cv->classes[i]; Class *cls;
				if ((cls=ctx->classMgr->getClass(cs.classID))==NULL) return false;
				const Stmt *cqry=cls->getQuery(); const QVar *cqv; ValueV vv(cs.params,cs.nParams);
				if (cqry==NULL || (cqv=cqry->top)==NULL ||
					!Expr::condSatisfied(cqv->nConds==1?&cqv->cond:cqv->conds,cqv->nConds,&pex,1,&vv,1,ma)) 
						{cls->release(); return false;}
				cls->release();
			}
		}
		if (!fIgnore || pars.vals!=NULL && pars.nValues!=0) for (CondIdx *ci=cv->condIdx; ci!=NULL; ci=ci->next) {
			if (ci->param>=pars.nValues) return false;
			Value v; const Value *par=&pars.vals[ci->param]; 
			if (ci->expr!=NULL) {
				// ???
			} else if (pex->getValue(ci->ks.propID,v,LOAD_SSV,NULL)!=RC_OK) return false;
			bool rc=Expr::calc((ExprOp)ci->ks.op,v,par,2,(ci->ks.flags&ORD_NCASE)!=0?CND_NCASE:0,ma)==RC_TRUE;
			freeV(v); if (!rc) return false;
		}
	}
	return qv->nConds==0 || Expr::condSatisfied(qv->nConds==1?&qv->cond:qv->conds,qv->nConds,&pex,1,&pars,1,ma,fIgnore);
}

bool QueryPrc::test(PINEx *pex,ClassID classID,const ValueV& pars,bool fIgnore)
{
	Class *cls; if ((cls=ctx->classMgr->getClass(classID))==NULL) return false;
	const Stmt *cqry=cls->getQuery();
	bool rc=cqry!=NULL && cqry->checkConditions(pex,pars,Session::getSession(),0,fIgnore);
	cls->release();
	return rc;
}

//---------------------------------------------------------------------------------------------------------

Cursor::~Cursor()
{
	if (txcid!=NO_TXCID) ses->getStore()->txMgr->releaseSnapshot(txcid);
	delete queryOp;
	if (values!=NULL) freeV((Value*)values,nValues,ses);
	if (results!=NULL) {
		for (unsigned i=1; i<nResults; i++) if (results[i]!=NULL) {results[i]->~PINEx(); ses->free(results[i]);}
		ses->free(results);
	}
}

RC Cursor::connect()
{
	if (queryOp!=NULL && (nResults=queryOp->getNOuts())!=0) {
		if (nResults==1) queryOp->connect(&pqr);
		else if ((results=new(ses) PINEx*[nResults])==NULL) return RC_NORESOURCES;
		else {
			memset(results,0,nResults*sizeof(PINEx*)); results[0]=pqr;
			for (unsigned i=1; i<nResults; i++) {
				if ((results[i]=new(ses) PINEx(ses))==NULL) return RC_NORESOURCES;
				results[i-1]->sibling=results[i];
			}
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
	ulong ns=queryOp->getSkip(),c=0; StoreCtx *ctx=ses->getStore(); RC rc=RC_OK;
	while (c<ns && (rc=queryOp->next())==RC_OK)
		if ((rc=ctx->queryMgr->checkLockAndACL(qr,TVO_READ,queryOp))!=RC_NOACCESS && rc!=RC_NOTFOUND)		// results?? lock multiple
			if (rc==RC_OK) c++; else break;
	queryOp->setSkip(ns-c); return rc;
}

RC Cursor::advance(bool fRet)
{
	if (!fAdvance) {fAdvance=true; return RC_OK;} RC rc=RC_EOF;
	if (ses!=NULL && cnt<nReturn && queryOp!=NULL) {
		if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		if (op!=STMT_QUERY && !tx.isStarted()) {if ((rc=tx.start())==RC_OK) tx.ok(); else return rc;}
		if (txid==INVALID_TXID) txid=ses->getTXID();
		else if (ses->getTXID()!=txid) {
			if ((mode&MODE_HOLD_RESULT)==0) return RC_CLOSED;
			// re-init & re-position
		}
		if (fSnapshot && txcid==NO_TXCID) if (ses->getTxState()==TX_NOTRAN) txcid=ses->getStore()->txMgr->assignSnapshot(); else fSnapshot=false;
		TxGuard txg(ses); ses->resetAbortQ();
		if (ses->getIdentity()==STORE_OWNER || queryOp->getSkip()==0 || (rc=skip())==RC_OK) {	//??????
			if (fProc && (stype==SEL_COUNT || stype==SEL_VALUE || stype==SEL_DERIVED || stype==SEL_CONST)) rc=RC_EOF;
			else if (stype==SEL_COUNT) {rc=ses->getStore()->queryMgr->count(queryOp,cnt,~0UL); fProc=true;}
			else for (TVOp tvo=op==STMT_INSERT||op==STMT_QUERY?TVO_READ:TVO_UPD; (rc=queryOp->next())==RC_OK;) {
				if (stype==SEL_CONST) {fProc=true; break;}
				if ((mode&MODE_DELETED)!=0 || stype!=SEL_PINSET && stype!=SEL_AUGMENTED && stype!=SEL_COMPOUND || (rc=ses->getStore()->queryMgr->checkLockAndACL(qr,tvo,queryOp))!=RC_NOACCESS && rc!=RC_NOTFOUND) {
					if (rc==RC_OK) {
						PINEx qpin(ses),*pq=&qr;
						if (!pq->pb.isNull()) {if (fRet && op!=STMT_QUERY) rc=pq->load(LOAD_ENAV|LOAD_SSV);}
						else if (stype==SEL_PINSET || stype==SEL_AUGMENTED || stype==SEL_COMPOUND) {
							qpin.id=pq->id; qpin.epr=pq->epr; qpin.addr=pq->addr; pq=&qpin;
							if (op!=STMT_QUERY) {
								rc=ses->getStore()->queryMgr->getBody(qpin,tvo,(mode&MODE_DELETED)!=0?GB_DELETED:0);
								if (rc==RC_OK && fRet && (rc=qpin.load(LOAD_ENAV|LOAD_SSV))==RC_OK)
									{qr.properties=qpin.properties; qr.nProperties=qpin.nProperties; qr.stamp=qpin.stamp; qr.mode=qpin.mode; qpin.properties=NULL; qpin.nProperties=0;}
							}
						} else {
							// check property!=NULL || PIN_EMPTY set
						}
						if (rc==RC_OK) {
							if (op!=STMT_QUERY && (rc=ses->getStore()->queryMgr->apply(ses,op,*pq,values,nValues,mode,queryOp->getQCtx()->vals[QV_PARAMS],fRet?(PIN*)&qr:(PIN*)0))==RC_NOACCESS || rc==RC_DELETED)
								{if (fRet) qr.cleanup(); continue;}
							if (rc==RC_OK) {fProc=true; cnt++;}
						}
					}
					break;
				}
			}
		}
		if (rc!=RC_OK) {
			if (rc!=RC_EOF) tx.resetOk(); 
			if (txcid!=NO_TXCID) {ses->getStore()->txMgr->releaseSnapshot(txcid); txcid=NO_TXCID;}
		}
	}
	return rc;
}

RC Cursor::extract(PIN *&pin,unsigned idx,bool fCopy)
{
	assert(stype==SEL_PINSET||stype==SEL_AUGMENTED||stype==SEL_COMPOUND);
	if (idx>=nResults) return RC_INVPARAM;
	PINEx *pex=results!=NULL?results[idx]:pqr;
	RC rc=pex->load((mode&(MODE_SSV_AS_STREAM|MODE_FORCED_SSV_AS_STREAM))|LOAD_SSV|(fCopy?LOAD_ENAV:0));
	if (rc!=RC_OK) {pin=NULL; return rc;}
	if (!fCopy) pin=pex;
	else if ((pin=new(ses) PIN(ses,pqr->id,pqr->addr,pqr->mode&~PIN_NO_FREE,NULL,0))==NULL) return RC_NORESOURCES;
	else if (pex->properties!=NULL && pex->nProperties!=0) {
		if ((pex->mode&PIN_NO_FREE)==0) {pin->properties=(Value*)pex->properties; pin->nProperties=pex->nProperties; pex->properties=NULL; pex->nProperties=0;}
		else if ((rc=copyV(pex->properties,pin->nProperties=pex->nProperties,pin->properties,ses))!=RC_OK) return rc;
	}
	if ((pin->mode&PIN_CLASS)!=0 && (mode&MODE_CLASS)!=0) ses->getStore()->queryMgr->getClassInfo(ses,pin);
	pin->stamp=pex->stamp; return RC_OK;
}

RC Cursor::next(const PINEx *&ret)
{
	ret=NULL; RC rc=advance(true); if (rc!=RC_OK) return rc;
	unsigned i; PIN *pin;
	switch (stype) {
	case SEL_COUNT:
		if (qr.properties==NULL && (qr.properties=new(ses) Value)==NULL) return RC_NORESOURCES;
		qr.nProperties=1; qr.properties->setU64(cnt); qr.properties->setPropID(PROP_SPEC_VALUE); ret=pqr;
		break;
	case SEL_PINSET: case SEL_COMPOUND:
		for (i=0; i<nResults; i++) if ((rc=extract(pin,i))!=RC_OK) return rc;
	default:
		ret=pqr; break;
	}
	return RC_OK;
}

RC Cursor::next(Value& ret)
{
	try {
		ret.setError(); RC rc=advance(true); PIN *pin;
		if (rc==RC_OK) {
			switch (stype) {
			case SEL_COUNT: ret.setU64(cnt); ret.setPropID(PROP_SPEC_VALUE); break;
			case SEL_CONST: case SEL_VALUE: case SEL_VALUESET: case SEL_DERIVED: case SEL_DERIVEDSET: case SEL_AUGMENTED:
				if (pqr->properties==NULL) break;
				if (pqr->nProperties>1) {
					ret.setStruct(pqr->properties,pqr->nProperties); ret.flags=SES_HEAP;
					if ((pqr->mode&PIN_NO_FREE)!=0) rc=copyV(ret,ret,ses); else {pqr->properties=NULL; pqr->nProperties=0;}
				} else if ((pqr->mode&PIN_NO_FREE)!=0) rc=copyV(pqr->properties[0],ret,ses); else {ret=pqr->properties[0]; pqr->properties[0].setError();}
				break;
			case SEL_PINSET:
				if ((rc=extract(pin,0,true))==RC_OK) {ret.set(pin); ret.flags=SES_HEAP;}
				break;
			case SEL_COMPOUND: case SEL_COMP_DERIVED:
				if ((rc=extract(pin,0,true))==RC_OK) {
					PIN *pin0=pin,*pin2=NULL;
					for (unsigned i=1; i<nResults; i++) {
						if ((rc=extract(pin2,i,true))!=RC_OK) break;
						pin->sibling=pin2;
					}
					if (rc==RC_OK) {ret.set(pin0); ret.flags=SES_HEAP;}
					else for (; pin!=NULL; pin=pin2) {pin2=pin->sibling; pin->destroy();}
				}
				break;
			}
		}
		ses->releaseAllLatches(); return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ICursor::next(Value&)\n"); return RC_INTERNAL;}
}

RC Cursor::next(PID& pid)
{
	try {
		if (stype!=SEL_PINSET && stype!=SEL_AUGMENTED && stype!=SEL_COMPOUND) return RC_INVOP;
		RC rc=advance(false);  if (rc==RC_OK) rc=qr.getID(pid); else pid=PIN::defPID;
		ses->releaseAllLatches(); return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ICursor::next(PID&)\n"); return RC_INTERNAL;}
}

IPIN *Cursor::next()
{
	try {
		if (stype!=SEL_PINSET && stype!=SEL_COMPOUND) return NULL;
		PIN *pin=NULL; if (advance(true)==RC_OK) extract(pin,0,true);
		ses->releaseAllLatches(); return pin;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ICursor::next()\n");}
	return NULL;
}

RC Cursor::rewind()
{
	try {
		RC rc=RC_EOF;
		if (ses!=NULL && queryOp!=NULL) {
			if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
			if (!fProc || op!=STMT_QUERY && !tx.isStarted()) return RC_OK;
			if ((rc=queryOp->rewind())==RC_OK) {cnt=0; fProc=false;}
			if (rc!=RC_OK) {
				if (rc!=RC_EOF) tx.resetOk(); 
				if (txcid!=NO_TXCID) {ses->getStore()->txMgr->releaseSnapshot(txcid); txcid=NO_TXCID;}
			}
			ses->releaseAllLatches();
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
	PID id;
	if (curs!=NULL) switch (dir) {
	default: break;
	case GO_FINDBYID: /*if (ei==STORE_COLLECTION_ID) curs->release();*/ break;			// ????
	case GO_FIRST: if (curs->rewind()!=RC_OK) break;	// remove array if walking
	case GO_NEXT:
		// check walking ARRAY
		while (curs->advance(curs->stype!=SEL_PINSET)==RC_OK) {
			switch (curs->stype) {
			case SEL_COUNT: v.setU64(curs->cnt); return &v;
			case SEL_VALUE: case SEL_VALUESET:
				if (curs->qr.properties!=NULL && curs->qr.nProperties==1) {
					if (curs->qr.properties[0].type!=VT_ARRAY) return curs->qr.properties;
					// set array
				}
				break;
			case SEL_DERIVED: case SEL_AUGMENTED: case SEL_DERIVEDSET: case SEL_CONST:
				//if (curs->qr.load(LOAD_SSV)==RC_OK) return curs->qr.getValueByIndex(0);
				//break;
			default: if (curs->qr.getID(id)==RC_OK) {v.set(id); return &v;} break;
			}
		}
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
	return &v;
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

unsigned long CursorNav::count() const
{
	return (unsigned long)curs->cnt;
}

void CursorNav::destroy()
{
	Session *ses=curs!=NULL?curs->ses:Session::getSession(); this->~CursorNav(); ses->free(this);
}

//--------------------------------------------------------------------------------------------------------------------------------

RC QueryPrc::getBody(PINEx& cb,TVOp tvo,ulong flags,VersionID vid)
{
	RC rc; bool fRemote=false,fTry=true,fWrite=tvo!=TVO_READ; cb.epr.flags&=~PINEX_ADDRSET; PageAddr extAddr;
	if (cb.id.pid==STORE_INVALID_PID && (rc=cb.unpack())!=RC_OK) return rc;
	if (!cb.addr.defined()) {fTry=false; if (isRemote(cb.id)) fRemote=true; else if (!cb.addr.convert(cb.id.pid)) return RC_CORRUPTED;}
	if ((cb.epr.flags&PINEX_EXTPID)!=0 && extAddr.convert(OID(cb.id.pid))) cb.ses->setExtAddr(extAddr);
	for (ulong fctl=PGCTL_RLATCH|(fWrite?PGCTL_ULOCK:0);;) {
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
			if ((flags&GB_FORWARD)!=0) {if ((cb.epr.flags&PINEX_EXTPID)!=0) cb.ses->setExtAddr(PageAddr::invAddr); return RC_OK;}
			memcpy(&cb.addr,(const byte*)cb.hpin+sizeof(HeapPageMgr::HeapObjHeader),PageAddrSize); continue;
		} else {
			PID id; IdentityID iid;
			if ((cb.epr.flags&PINEX_EXTPID)!=0) cb.ses->setExtAddr(PageAddr::invAddr);
			if (cb.hpin->hdr.getType()!=HO_PIN) return RC_CORRUPTED;
			if (!cb.hpin->getAddr(id)) {id.pid=OID(cb.addr); id.ident=STORE_OWNER;}
			if (cb.id==id) {
				cb.epr.flags|=PINEX_ADDRSET; rc=RC_OK; 
				uint16_t dscr=cb.hpin->hdr.descr; cb.stamp=cb.hpin->getStamp();
				if (isRemote(cb.id)) {
					//???
				} else if ((rc=ctx->lockMgr->getTVers(cb,tvo))!=RC_OK) return rc;
				if (!cb.ses->inWriteTx()) {
					if (tvo!=TVO_READ) return RC_READTX;
					if (cb.tv!=NULL) {
						// check dscr,stamp modified
						//...
						//cb.flags|=PINEX_TVERSION;
					}
				} else if ((flags&GB_REREAD)==0) {
					const ulong lck=tvo==TVO_READ?PINEX_LOCKED:PINEX_XLOCKED;
					if ((cb.epr.flags&lck)==0) {
						if ((rc=ctx->lockMgr->lock(tvo==TVO_READ?LOCK_SHARED:LOCK_EXCLUSIVE,cb))!=RC_OK) {cb.pb.release(cb.ses); return rc;}
						cb.epr.flags|=lck|PINEX_LOCKED; if (cb.pb.isNull()) continue;
					}
				}
				if ((flags&GB_DELETED)==0 && (cb.hpin->hdr.descr&HOH_DELETED)!=0) return RC_DELETED;
				if (vid!=STORE_CURRENT_VERSION && vid<cb.hpin->getStamp()) {
					//???continue???
				}
				if ((flags&GB_REREAD)==0 && (iid=cb.ses->getIdentity())!=STORE_OWNER) rc=checkACLs(cb,iid,tvo,flags);
				if (rc!=RC_OK) {cb.pb.release(cb.ses); return rc;} if (cb.pb.isNull()) continue;
				cb.mode&=~(MODE_DELETED|PIN_DELETED);
				if (isRemote(id)) {
					if ((dscr&HOH_REPLICATED)==0) cb.mode|=PIN_READONLY; else cb.mode=cb.mode&~PIN_READONLY|PIN_REPLICATED;
				} else if ((dscr&HOH_NOREPLICATION)!=0) cb.mode|=PIN_NO_REPLICATION;
				else if ((dscr&HOH_REPLICATED)!=0) cb.mode|=PIN_REPLICATED;
				if ((dscr&HOH_NOTIFICATION)!=0) cb.mode|=PIN_NOTIFY;
				if ((dscr&HOH_NOINDEX)!=0) cb.mode|=PIN_NO_INDEX;
				if ((dscr&HOH_HIDDEN)!=0) cb.mode|=PIN_HIDDEN;
				if ((dscr&HOH_DELETED)!=0) cb.mode|=PIN_DELETED;
				if ((dscr&HOH_CLASS)!=0) cb.mode|=PIN_CLASS;
				if (cb.hpin->nProps==0) cb.mode|=PIN_EMPTY;		// cb.tv ????
				return RC_OK;
			}
		}
		if (!fTry) {
			if (cb.hpin!=NULL && (cb.epr.flags&PINEX_EXTPID)==0) 
				report(MSG_ERROR,"findPage: page %08X corruption\n",cb.pb->getPageID());
			return RC_NOTFOUND;
		}
		if (isRemote(cb.id)) fRemote=true; else if (!cb.addr.convert(cb.id.pid)) return RC_CORRUPTED;
		fTry=false;
	}
}

RC QueryPrc::checkLockAndACL(PINEx& qr,TVOp tvo,QueryOp *qop)
{
	if (qr.ses==NULL) return RC_NOSESSION;
	RC rc=RC_OK; IdentityID iid; bool fWasNull=qr.pb.isNull();
	if ((qr.epr.flags&PINEX_ADDRSET)==0) qr.addr=PageAddr::invAddr;
	if (qr.ses->inWriteTx() && (qr.epr.flags&(tvo!=TVO_READ?PINEX_XLOCKED:PINEX_LOCKED))==0) {
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

RC QueryPrc::checkACLs(PINEx& cb,IdentityID iid,TVOp tvo,ulong flags,bool fProp)
{
	assert(!cb.pb.isNull()); const bool fWrite=tvo!=TVO_READ;
	Value v; RC rc=RC_NOACCESS; RefTrace rt={NULL,cb.id,PROP_SPEC_ACL,STORE_COLLECTION_ID};
	if (loadV(v,PROP_SPEC_ACL,cb,0,cb.ses)==RC_OK) {
		rc=checkACL(v,cb,iid,fWrite?ACL_WRITE:ACL_READ,&rt,fProp); freeV(v);
	} else if (loadV(v,PROP_SPEC_DOCUMENT,cb,0,cb.ses)==RC_OK) {
		if (v.type==VT_REFID) {
			if (!fProp) rc=RC_FALSE;
			else {
				RefVID ref={v.id,PROP_SPEC_ACL,STORE_COLLECTION_ID,STORE_CURRENT_VERSION}; 
				v.set(ref); rt.id=v.id; rc=checkACL(v,cb,iid,fWrite?ACL_WRITE:ACL_READ,&rt);
			}
		}
		freeV(v);
	}
	return rc==RC_REPEAT?getBody(cb,tvo,GB_REREAD):rc;
}

RC QueryPrc::checkACLs(const PINEx *pin,PINEx& cb,IdentityID iid,TVOp tvo,ulong flags,bool fProp)
{
	RC rc=RC_NOACCESS; const bool fWrite=tvo!=TVO_READ;
	RefTrace rt={NULL,pin->id,PROP_SPEC_ACL,STORE_COLLECTION_ID}; Value v; v.setError();
	if (pin->getValue(PROP_SPEC_ACL,v,LOAD_SSV,pin->ses)==RC_OK) rc=checkACL(v,cb,iid,fWrite?ACL_WRITE:ACL_READ,&rt,fProp);
	else if (pin->getValue(PROP_SPEC_DOCUMENT,v,LOAD_SSV,pin->ses)==RC_OK && v.type==VT_REFID) {
		if (!fProp) rc=RC_FALSE;
		else {
			RefVID ref={v.id,PROP_SPEC_ACL,STORE_COLLECTION_ID,STORE_CURRENT_VERSION}; 
			Value v; v.set(ref); rt.id=v.id; rc=checkACL(v,cb,iid,fWrite?ACL_WRITE:ACL_READ,&rt);
		}
	}
	cb=pin->id; freeV(v);
	return rc==RC_REPEAT?getBody(cb,tvo,flags|GB_REREAD):rc;
}

RC QueryPrc::checkACL(const Value& v,PINEx& cb,IdentityID iid,uint8_t mask,const RefTrace *rt,bool fProp)
{
	ulong i; const Value *cv; Value pv,*ppv; RC rc=RC_OK; RefTrace refTrc; bool fFalse=false;
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

RC QueryPrc::getRefSafe(const PID& id,Value *&vals,ulong& nValues,ulong mode,PINEx& cb)
{
	if (cb.pb.isNull()) return RC_INVPARAM; const bool fU=cb.pb->isULocked();
	const PageID pid=cb.pb->getPageID(); PINEx cb2(cb.ses); RC rc=RC_OK;
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
			for (ulong i=0; i<nValues; ++hprop,++i) vals[i].property=hprop->getID();
		}
		if (vals!=NULL) for (ulong i=0; i<nValues; i++)
			if ((rc=loadV(vals[i],vals[i].property,cb2,mode,cb.ses,vals[i].eid))!=RC_OK) break;
		break;
	}
	if (cb.pb.isNull()) {cb2.pb.getPage(pid,ctx->heapMgr,fU?PGCTL_ULOCK:0,cb2.ses); cb2.pb.moveTo(cb.pb);}
	return rc;
}

RC QueryPrc::getPINValue(const PID& id,Value& res,ulong mode,Session *ses)
{
	PINEx cb(ses,id),*pcb=&cb; Expr *exp; RC rc; Value v;
	if ((rc=getBody(cb))!=RC_OK || (rc=loadV(res,PROP_SPEC_VALUE,cb,mode|LOAD_SSV,ses))!=RC_OK) return rc;
	for (int cnt=0;;cnt++) {
		assert((res.flags&VF_SSV)==0);
		switch (res.type) {
		default: return RC_OK;
		case VT_EXPR:
			exp=(Expr*)res.expr; rc=Expr::eval(&exp,1,v,&pcb,1,NULL,0,ses); freeV(res); res=v;
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
				char buf[100]; sprintf(buf,STORE_STD_URI_PREFIX CALC_PROP_NAME "%d",nCalcProps+1);
				URI *uri=ctx->uriMgr->insert(buf); if (uri==NULL) return RC_NORESOURCES;
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

//---------------------------------------------------------------------------------------------------------------------

void PINEx::operator=(const PIN *pin)
{
	assert(ses==pin->getSes()); pb.release();
	id=pin->id; addr=pin->addr; mode=pin->mode|PIN_NO_FREE|PIN_PINEX; stamp=pin->stamp; properties=pin->properties; nProperties=pin->nProperties; hpin=NULL; tv=NULL; epr.flags=0; epr.lref=0;
}

void PINEx::releaseLatches(PageID pid,PageMgr *mgr,bool)
{
	if (!pb.isNull() && (pid==INVALID_PAGEID || mgr!=NULL && mgr->getPGID()==PGID_HEAP && pid<pb->getPageID())) {
		if ((epr.flags&PINEX_RLOAD)!=0 && hpin!=NULL && properties==NULL) ses->getStore()->queryMgr->loadProps(this,0/*,flt,nFlt*/);	// LOAD_SSV ??? if failed?
		hpin=NULL; pb.release(ses);
	}
}

void PINEx::checkNotHeld(PBlock *p)
{
	assert(pb.isNull() || pb.isSet(PGCTL_INREL) || (PBlock*)pb!=p);
}

void PINEx::moveTo(PINEx& cb)
{
	cb.id=id; cb.addr=addr; cb.properties=properties; cb.nProperties=nProperties; cb.mode=mode; cb.stamp=stamp; pb.moveTo(cb.pb); cb.hpin=hpin; cb.epr=epr; cb.tv=tv;
	id=PIN::defPID; addr=PageAddr::invAddr; properties=NULL; nProperties=0; mode=0; hpin=NULL; tv=NULL; epr.lref=0; epr.flags=0;
}

bool PINEx::defined(const PropertyID *pids,unsigned nP) const
{
	if (pids!=NULL && nP!=0) {
		if (properties==NULL && hpin==NULL) return false;
		for (ulong i=0; i<nP; i++) {
			if (pids[i]==PROP_SPEC_PINID || pids[i]==PROP_SPEC_STAMP) {if ((mode&PIN_DERIVED)!=0) return false;}
			else if (properties==NULL) {if (hpin->findProperty(pids[i]&STORE_MAX_URIID)==NULL) return false;}
			else if (BIN<Value,PropertyID,ValCmp>::find(pids[i]&STORE_MAX_URIID,properties,nProperties)==NULL) return false;
		}
	}
	return true;
}

bool PINEx::isCollection(PropertyID pid) const
{
	if (properties!=NULL) {
		const Value *cv=BIN<Value,PropertyID,ValCmp>::find(pid,properties,nProperties);
		return cv!=NULL && (cv->type==VT_ARRAY||cv->type==VT_COLLECTION);
	}
	if (hpin!=NULL) {
		const HeapPageMgr::HeapV *hprop=hpin->findProperty(pid);
		return hprop!=NULL && hprop->type.isCollection();
	}
	return false;
}

RC PINEx::getValue(PropertyID pid,Value& v,ulong mode,MemAlloc *ma,ElementID eid) const
{
	RC rc; pid&=STORE_MAX_URIID;
	if (properties==NULL && pid!=PROP_SPEC_PINID) {
		if (!pb.isNull()) return ses->getStore()->queryMgr->loadV(v,pid,*this,mode,ma,eid);
		v.setError(pid); return RC_NOTFOUND;
	}
	switch (pid&STORE_MAX_URIID) {
	default: break;
	case STORE_INVALID_PROPID: 
		if ((mode&LOAD_CARDINALITY)==0) {v.setError(); return RC_INVPARAM;} else {v.set(0u); return RC_OK;}
	case PROP_SPEC_PINID:
		if ((mode&LOAD_CARDINALITY)!=0) v.set(1u);
		else if (id.pid!=STORE_INVALID_PID || (rc=unpack())==RC_OK) v.set(id);
		else return rc;
		v.property=pid; return RC_OK;
	case PROP_SPEC_STAMP:
		if ((mode&LOAD_CARDINALITY)!=0) v.set(1u); else v.set((unsigned int)stamp); v.property=pid; return RC_OK;
	}
	const Value *pv=BIN<Value,PropertyID,ValCmp>::find(pid,properties,nProperties);
	if ((mode&LOAD_CARDINALITY)!=0) {
		if (pv==NULL) v.set(0u); else switch (pv->type) {
		case VT_ARRAY: v.set((unsigned)pv->length); break;
		case VT_COLLECTION: v.set((unsigned)pv->nav->count()); break;
		default: v.set(1u); break;
		}
		return RC_OK;
	}
	if (pv==NULL) {v.setError(pid); return RC_NOTFOUND;}
	if (eid!=STORE_COLLECTION_ID) switch (pv->type) {
	default: break;
	case VT_ARRAY:
		//...
		assert(0); //tmp
	case VT_COLLECTION:
		return pv->nav->getElementByID(eid,v);
	}
	v=*pv; v.flags=NO_HEAP; return RC_OK;
}

#if 0
RC PINEx::loadV(const PropertyID *pids,Value *res,unsigned nP) const
{
	RC rc=RC_OK; const Value *pv=properties,*v; unsigned np=nProperties,nhp=0;
	const HeapPageMgr::HeapV *hprop=!pb.isNull() && hpin!=NULL?(nhp=hpin->nProps,hpin->getPropTab()):(HeapPageMgr::HeapV*)0,*hp;
	for (unsigned i=0; i<nP; i++) {
		const PropertyID pid=pids[i]&STORE_MAX_URIID; unsigned flg=pids[i];
		if (res!=NULL && (flg&PROP_NO_LOAD)!=0) (res++)->setError(pid);
		switch (pid) {
		default: break;
		case STORE_INVALID_PROPID:
			if ((flg&PROP_OPTIONAL)==0) return RC_INVPARAM;
			if (res!=NULL && (flg&PROP_NO_LOAD)==0) {if ((flg&PROP_ORD)!=0) res->set(0u); else res->setError(); res++;}
			continue;
		case PROP_SPEC_PINID:
			if (res!=NULL && (flg&PROP_NO_LOAD)==0) {
				if ((flg&PROP_ORD)!=0) res->set(1u); else if (id.pid!=STORE_INVALID_PID || (rc=unpack())==RC_OK) res->set(id); else return rc;
				res->property=pid; res++;
			}
			continue;
//		case PROP_SPEC_STAMP:
//			if ((mode&LOAD_CARDINALITY)!=0) v.set(1u); else v.set((unsigned int)stamp); v.property=pid; return RC_OK;
		}
		if (pv!=NULL && (v=BIN<Value,PropertyID,ValCmp>::find(pid,pv,np))!=NULL) {
			if (res!=NULL && (flg&PROP_NO_LOAD)==0) {
				if ((flg&PROP_ORD)!=0) res->set((unsigned)(v->type==VT_ARRAY?v->length:v->type==VT_COLLECTION?v->nav->count():1u)); else {*res=*v; res->flags=NO_HEAP;}
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

RC PINEx::unpack() const
{
	try {
		PINRef pr(ses->getStore()->storeID,epr.buf,epr.lref);
		if ((pr.def&PR_ADDR)!=0) {addr=pr.addr; epr.flags|=PINEX_ADDRSET;}
		const_cast<PID&>(id)=pr.id; return RC_OK;
	} catch (RC rc) {return rc;}
}

RC PINEx::pack() const
{
	if (epr.lref==0) {
		if (id.pid==STORE_INVALID_PID) return RC_NOTFOUND;
		else {
			PINRef pr(ses->getStore()->storeID,id); if (addr.defined()) {pr.def|=PR_ADDR; pr.addr=addr;}
			epr.lref=pr.enc(epr.buf);
		}
	}
	return RC_OK;
}

RC PINEx::load(unsigned md,const PropertyID *flt,unsigned nFlt)
{
	if ((mode&PIN_DERIVED)==0 && id.pid==STORE_INVALID_PID) {RC rc=unpack(); if (rc!=RC_OK) return rc;}
	if (properties!=NULL || (mode&PIN_EMPTY)!=0) return RC_OK; // check LOAD_ENAV, LOAD_SSV/PIN_SSV, MODE_SSV_AS_STREAM, MODE_FORCED_SSV_AS_STREAM
	if (hpin==NULL) {
		if (id.pid==STORE_INVALID_PID) return RC_NOTFOUND;
		RC rc=ses->getStore()->queryMgr->getBody(*this,TVO_READ,(md&MODE_DELETED)!=0?GB_DELETED:0);
		if (rc!=RC_OK) {if (rc==RC_DELETED) mode|=PIN_DELETED; return rc;}
	}
	return ses->getStore()->queryMgr->loadProps(this,md,flt,nFlt);
}

void PINEx::free()
{
	if (properties!=NULL && (mode&PIN_NO_FREE)==0) freeV((Value*)properties,nProperties,ses);
	properties=NULL; nProperties=0;
	//if (tv!=NULL) ???
}
