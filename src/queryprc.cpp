/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

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

using namespace MVStoreKernel;

QueryPrc::QueryPrc(StoreCtx *c,IStoreNotification *notItf) 
	: notification(notItf),ctx(c),bigThreshold(c->bufMgr->getPageSize()*SKEW_PAGE_PCT/100)
{
}

const static char *stmtOpName[STMT_OP_ALL] = {"QUERY","INSERT","UPDATE","DELETE","UNDELETE","START","COMMIT","ROLLBACK","ISOLATION"};


RC SessionX::execute(const char *str,size_t lstr,char **result,const URIID *ids,unsigned nids,const Value *params,unsigned nParams,CompilationError *ce,
																								uint64_t *nProcessed,unsigned nProcess,unsigned nSkip)
{
	try {
		assert(ses==Session::getSession()); if (ce!=NULL) memset(ce,0,sizeof(CompilationError));
		if (str==NULL||lstr==0) return RC_INVPARAM; if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		SInCtx in(ses,str,lstr,ids,nids,(ses->itf&ITF_SPARQL)!=0?SQ_SPARQL:SQ_SQL); RC rc=RC_OK;
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
			if (values==NULL || nValues!=2) return RC_INTERNAL;
			return values[0].type==VT_ANY?ses->setBase(values[1].str,values[1].length):ses->setPrefix(values[0].str,values[0].length,values[1].str,values[1].length);
		case STMT_QUERY:
			if (pResult==NULL) return nProcessed!=NULL?count(*nProcessed,pars,nPars,nProcess,md,txl):RC_INVPARAM;
			md&=MODE_HOLD_RESULT|MODE_ALL_WORDS|MODE_SSV_AS_STREAM|MODE_DELETED|MODE_FORCED_SSV_AS_STREAM|MODE_VERBOSE|MODE_FOR_UPDATE|MODE_COPY_VALUES;
			if (ses->inReadTx()) md&=~MODE_FOR_UPDATE; break;
		default:
			if (ses->inReadTx()) return RC_READTX; if (ctx->isServerLocked()) return RC_READONLY; ctx->classMgr->initClasses(ses);
			md&=MODE_FORCE_EIDS|MODE_CHECK_STAMP|MODE_ALL_WORDS|MODE_DELETED|MODE_VERBOSE|MODE_PURGE|MODE_PURGE_IDS|MODE_COPY_VALUES;
			if (op!=STMT_INSERT) md|=MODE_FOR_UPDATE; if (op==STMT_UNDELETE) md|=MODE_DELETED;
			if (op==STMT_INSERT && top==NULL) {
				PID id=PIN::defPID; unsigned cnt=0;
				if (nNested!=0) {
					PIN **ppins=(PIN**)ses->malloc((sizeof(PIN)+sizeof(PIN*))*(nNested+1)); if (ppins==NULL) return RC_NORESOURCES;
					if ((rc=getNested(ppins,(PIN*)(ppins+nNested+1),cnt,ses))==RC_OK) {
						assert(cnt==nNested+1);
						if ((rc=ctx->queryMgr->commitPINs(ses,ppins,cnt,mode|MODE_NO_EID,NULL,NULL,pars,nPars))==RC_OK)
							{id=ppins[0]->id; if (nProcessed!=NULL) *nProcessed=cnt;}
					}
					for (unsigned i=0; i<cnt; i++) if (ppins[i]!=NULL) ppins[i]->~PIN();
					ses->free(ppins);
				} else {
					Value *pv=values; ulong flg=PIN_NO_FREE;
					if (pv!=NULL && nValues!=0 && (mode&MODE_WITH_EVAL)!=0) {
						if ((pv=new(ses) Value[nValues])==NULL) return RC_NORESOURCES;
						memcpy(pv,values,nValues*sizeof(Value)); flg=0;
						for (unsigned i=0; i<nValues; i++) pv[i].flags=NO_HEAP;
					}
					PIN pin(ses,PIN::defPID,PageAddr::invAddr,flg,pv,nValues),*pp=&pin;
					if ((rc=ctx->queryMgr->commitPINs(ses,&pp,1,mode|MODE_NO_EID,NULL,NULL,pars,nPars))==RC_OK)
						{id=pin.id; if (nProcessed!=NULL) *nProcessed=1;}
				}
				if (rc!=RC_OK || pResult==NULL) return rc; sop=STMT_QUERY;
				if ((qop=new(ses,1) ArrayScan(ses,&id,1,0))==NULL) return RC_NORESOURCES;
			}
			break;
		}
		if (qop==NULL) {QueryCtx qctx(ses,pars,nPars,this,nSkip,md); if ((rc=qctx.process(qop))!=RC_OK && rc!=RC_EOF) {delete qop; return rc;}}
		if (txl==TXI_DEFAULT && (txl=(TXI_LEVEL)ses->getIsolationLevel())==TXI_DEFAULT) txl=TXI_REPEATABLE_READ;
		if (pResult!=NULL) {
			Value *vals=NULL; unsigned nVals=0;
			if (sop==STMT_QUERY || values==NULL || op==STMT_INSERT && top==NULL || (rc=copyV(values,nVals=nValues,vals,ses))==RC_OK) {
				Cursor *result=new(ses) Cursor(qop,nProcess,mode|md,vals,nVals,ses,sop,top!=NULL?top->stype:SEL_PINSET,txl>=TXI_REPEATABLE_READ);
				if (result==NULL) {if (vals!=NULL) freeV(vals,nVals,ses); rc=RC_NORESOURCES;}
				else if ((rc=result->connect())!=RC_OK) {result->destroy(); if (vals!=NULL) freeV(vals,nVals,ses);}
				else {*pResult=result; if (qop!=NULL) {qop->release(); qop=NULL;}}
			}
		} else if (rc!=RC_EOF) {
			PINEx qr(ses); qop->connect(&qr); TxSP tx(ses); if ((rc=tx.start(txl))!=RC_OK) return rc; ses->rlatch=qop;
			while (rc==RC_OK && (nProcess==~0u || cnt<nProcess) && (rc=qop->next())==RC_OK) {
				assert(qr.pb.isNull() || qr.addr.defined());
				if (qr.pb.isNull() || !qr.pb.isSet(PGCTL_NOREL)) qop->release();
				if (sop!=STMT_UNDELETE && ((rc=ctx->queryMgr->checkLockAndACL(qr,sop==STMT_INSERT?TVO_READ:TVO_UPD,qop))==RC_NOACCESS || rc==RC_NOTFOUND)) rc=RC_OK;
				else if (rc==RC_OK && (rc=ctx->queryMgr->apply(ses,sop,qr,values,nValues,mode|md,NULL,pars,nPars))==RC_OK) cnt++;
				else if (rc==RC_NOACCESS || rc==RC_DELETED) rc=RC_OK;
			}
			if (rc==RC_EOF || rc==RC_OK) {rc=RC_OK; tx.ok();} else cnt=~0ULL;
			ses->rlatch=NULL;
		}
		delete qop;
		if ((ses->getTraceMode()&TRACE_SESSION_QUERIES)!=0) trace(ses,stmtOpName[op],rc,ulong(cnt),pars,nPars,values,nValues);
		if (nProcessed!=NULL) *nProcessed=cnt; return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::execute()\n"); return RC_INTERNAL;}
}

RC QueryPrc::eval(Session *ses,const Value *pv,Value& res,const PINEx **vars,ulong nVars,const Value *params,unsigned nParams,MemAlloc *ma,bool fInsert)
{
	RC rc; Expr *expr; ICursor *ic; CursorNav *cn;
	switch (pv->type) {
	default: return RC_TYPE;
	case VT_VARREF:
		if (pv->refPath.refN>=nVars) return RC_INVPARAM;
		//if (pv->length==0) {PIN *p=NULL; if ((rc=loadPIN(ses,id,p,0,pcb,&md))==RC_OK) mi->newV->set(p);}
		return loadV(res,pv->refPath.id,*vars[pv->refPath.refN],LOAD_SSV,ma);
	case VT_PARAM:
		if (pv->refPath.refN>=nParams) return RC_NOTFOUND;
		res=params[pv->refPath.refN]; res.flags=NO_HEAP;
		break;
	case VT_EXPRTREE:
		if ((rc=Expr::compile((ExprTree*)pv->exprt,expr,ses))!=RC_OK ||
			(rc=Expr::eval(&expr,1,res,vars,nVars,params,nParams,ses))!=RC_OK) return rc;
		break;
	case VT_EXPR:
		if ((rc=Expr::eval((Expr**)pv->expr,1,res,vars,nVars,params,nParams,ses))!=RC_OK) return rc;
		break;
	case VT_STMT:
		switch (pv->stmt->getOp()) {
		default: rc=RC_INVOP; break;
		case STMT_INSERT:
			if (!fInsert) return RC_INVOP;
			// insert, page?, set ID
			break;
		case STMT_QUERY:
			if ((rc=pv->stmt->execute(&ic,params,nParams))!=RC_OK) return rc;
			if ((cn=new(ses) CursorNav((Cursor*)ic))==NULL) return RC_NORESOURCES;
			//cn->setType(VT_REFID);
			res.set(cn); break;
		}
		break;
	}
	return RC_OK;
}

RC QueryPrc::apply(Session *ses,STMT_OP op,PINEx& qr,const Value *values,unsigned nValues,unsigned mode,PIN *pin,const Value *params,unsigned nParams)
{
	RC rc=RC_INTERNAL; bool f;
	switch (op) {
	default: break;
	case STMT_INSERT:
		if ((f=pin!=NULL) || (rc=ctx->queryMgr->loadPIN(ses,qr.id,pin,mode,&qr,ses))==RC_OK) {
			const_cast<PID&>(pin->id)=PIN::defPID; pin->addr=PageAddr::invAddr;
			for (unsigned i=0; i<nValues; i++)	{// epos ???
				// evaluate expr with params
				if ((rc=pin->modify(&values[i],values[i].eid,STORE_COLLECTION_ID,0,ses))!=RC_OK) break;
			}
			if (rc==RC_OK) rc=ctx->queryMgr->commitPINs(ses,&pin,1,mode|MODE_NO_EID,NULL,NULL,params,nParams);
			if (!f) pin->destroy();
		}
		break;
	case STMT_UPDATE:
		rc=ctx->queryMgr->modifyPIN(ses,qr.id,values,nValues,&qr,pin,mode|MODE_NO_EID,NULL,NULL,params,nParams); break;
	case STMT_DELETE:
		rc=ctx->queryMgr->deletePINs(ses,NULL,&qr.id,1,mode,qr.pb.isNull()?(PINEx*)0:&qr); break;
	case STMT_UNDELETE:
		rc=ctx->queryMgr->setFlag(ses,qr.id,&qr.addr,HOH_DELETED,true); 	break;
	}
	return rc;
}

namespace MVStoreKernel
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
	RC rc=RC_OK; bool fCheckNulls=false; cnt=0ULL;
	if (os!=NULL) for (unsigned i=0; i<nos; i++) if ((os[i].flags&(ORD_NULLS_BEFORE|ORD_NULLS_AFTER))==0) {fCheckNulls=true; break;}
	PINEx qr(qop->getSession()); qop->connect(&qr);
	if (!fCheckNulls && qop->getSession()->getIdentity()==STORE_OWNER) rc=qop->count(cnt,nAbort);
	else {
		while ((rc=qop->next())==RC_OK)
			if ((rc=checkLockAndACL(qr,TVO_READ,qop))!=RC_NOACCESS && rc!=RC_NOTFOUND) {
				if (rc!=RC_OK) break;
				if (fCheckNulls) {
					// check them
				}
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
		QueryCtx qctx(ses,pars,nPars,this,0,md|MODE_COUNT); QueryOp *qop=NULL; RC rc=qctx.process(qop);
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
		QueryCtx qctx(ses,pars,nPars,this,0,md); QueryOp *qop=NULL; RC rc=qctx.process(qop);
		if (rc==RC_OK) {
			assert(qop!=NULL); StoreCtx *ctx=ses->getStore();
			PINEx qr(ses); qop->connect(&qr);
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
		QueryCtx qctx(ses,pars,nPars,this,0,md&(MODE_ALL_WORDS|MODE_DELETED));
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
		TxGuard txg(ses); PINEx pex((PIN*)p); return checkConditions(&pex,pars,nPars,ses);
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IStmt::isSatisfied()\n");}
	return false;
}

bool Stmt::isSatisfied(const IPIN *const *pins,unsigned nPins,const Value *pars,unsigned nPars,unsigned long mode) const
{
	try {
		if (nPins==1) return isSatisfied(pins[0],pars,nPars,mode);
		Session *ses=Session::getSession(); if (ses==NULL||ses->getStore()->inShutdown()) return false;
		TxGuard txg(ses); //PINEx cb(p->getPID(),(const PIN *)p); return checkConditions(cb,vars,pars,nPars,ses);
		return false;	// niy
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IStmt::isSatisfied(IPIN **pins,...)\n");}
	return false;
}

bool Stmt::checkConditions(const PINEx *pin,const Value *pars,ulong nPars,MemAlloc *ma,ulong start,bool fIgnore) const
{
	QVar *qv=top; if (qv==NULL) return true;
	if (qv->condProps!=NULL && !qv->condProps->test(pin,qv->lProps)) return false;
	StoreCtx *ctx=StoreCtx::get();
	if (qv->type==QRY_SIMPLE) {
		const SimpleVar *cv=(SimpleVar*)qv;
		if (cv->classes!=NULL) for (ulong i=start; i<cv->nClasses; i++) {
			ClassSpec& cs = cv->classes[i]; Class *cls;
			if ((cls=ctx->classMgr->getClass(cs.classID))==NULL) return false;
			const Stmt *cqry=cls->getQuery(); const QVar *cqv;
			if (cqry==NULL || (cqv=cqry->top)==NULL || cqv->condProps!=NULL && !cqv->condProps->test(pin,cqv->lProps) ||
				!ctx->queryMgr->condSatisfied(cqv->nConds==1?&cqv->cond:cqv->conds,cqv->nConds,&pin,1,cs.params,cs.nParams,ma)) 
					{cls->release(); return false;}
			cls->release();
		}
		if (!fIgnore || pars!=NULL && nPars!=0) for (CondIdx *ci=cv->condIdx; ci!=NULL; ci=ci->next) {
			if (ci->param>=nPars) return false;
			Value v; const Value *par=&pars[ci->param]; 
			if (ci->expr!=NULL) {
				// ???
			} else if (pin->getValue(ci->ks.propID,v,LOAD_SSV,NULL)!=RC_OK) return false;
			bool rc=Expr::calc((ExprOp)ci->ks.op,v,par,2,(ci->ks.flags&ORD_NCASE)!=0?CND_NCASE:0,ma)==RC_TRUE;
			freeV(v); if (!rc) return false;
		}
	}
	return qv->nConds==0 || ctx->queryMgr->condSatisfied(qv->nConds==1?&qv->cond:qv->conds,qv->nConds,&pin,1,pars,nPars,ma,fIgnore);
}

bool QueryPrc::condSatisfied(const Expr *const *exprs,ulong nExp,const PINEx **vars,unsigned nVars,const Value *pars,unsigned nPars,MemAlloc *ma,bool fIgnore)
{
	try {
		Value res; 
		if (exprs==NULL || nExp==0 || exprs[0]==NULL) return true;
		switch (Expr::eval(exprs,nExp,res,vars,nVars,pars,nPars,ma,fIgnore)) {
		case RC_TRUE: return true;
		case RC_FALSE: return false;
		case RC_OK: if (res.type==VT_BOOL) return res.b;
		default: break;
		}
	} catch (...) {}
	return false;
}

bool QueryPrc::test(const PINEx *pin,ClassID classID,const Value *params,unsigned nParams,bool fIgnore)
{
	Class *cls; if ((cls=ctx->classMgr->getClass(classID))==NULL) return false;
	const Stmt *cqry=cls->getQuery();
	bool rc=cqry!=NULL && cqry->checkConditions(pin,params,nParams,Session::getSession(),0,fIgnore);
	cls->release();
	return rc;
}

//---------------------------------------------------------------------------------------------------------

Cursor::~Cursor()
{
	if (ses->rlatch==queryOp) ses->rlatch=NULL;
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
		if (nResults==1) queryOp->connect(&qr);
		else if ((results=new(ses) PINEx*[nResults])==NULL) return RC_NORESOURCES;
		else {
			memset(results,0,nResults*sizeof(PINEx*)); results[0]=&qr;
			for (unsigned i=1; i<nResults; i++) if ((results[i]=new(ses) PINEx(ses))==NULL) return RC_NORESOURCES;
			queryOp->connect(results,nResults);
		}
	}
	return RC_OK;
}

RC Cursor::skip()
{
	ulong ns=queryOp->getSkip(),c=0; StoreCtx *ctx=ses->getStore(); RC rc=RC_OK; ses->rlatch=queryOp;
	while (c<ns && (rc=queryOp->next())==RC_OK)
		if ((rc=ctx->queryMgr->checkLockAndACL(qr,TVO_READ,queryOp))!=RC_NOACCESS && rc!=RC_NOTFOUND)		// results?? lock multiple
			if (rc==RC_OK) c++; else break;
	queryOp->setSkip(ns-c); ses->rlatch=NULL; return rc;
}

IPIN *Cursor::next()
{
	try {
		PIN *pin=NULL; RC rc;
		if (ses!=NULL && queryOp!=NULL && cnt<nReturn && !ses->getStore()->inShutdown()) {
			if (op!=STMT_QUERY && !tx.isStarted()) {if ((rc=tx.start())==RC_OK) tx.ok(); else return NULL;}
			if (txid==INVALID_TXID) txid=ses->getTXID();
			else if (ses->getTXID()!=txid) {
				if ((mode&MODE_HOLD_RESULT)==0) return NULL;
				// re-init & re-position
			}
			if (fSnapshot && queryOp!=NULL && txcid==NO_TXCID) if (ses->getTxState()==TX_NOTRAN) txcid=ses->getStore()->txMgr->assignSnapshot(); else fSnapshot=false;
			TxGuard txg(ses); ses->resetAbortQ(); PageAddr extAddr; ses->rlatch=queryOp; bool fRel=true; uint64_t c; 	//unsigned nout=0;
			if (ses->getIdentity()==STORE_OWNER || queryOp->getSkip()==0 || skip()==RC_OK) {
				if (cnt!=0 && (stype==SEL_COUNT || stype==SEL_VALUE || stype==SEL_DERIVED)) rc=RC_EOF;
				else if (stype==SEL_COUNT) {
					Value *pv=new(ses) Value;
					if (pv==NULL) rc=RC_NORESOURCES;
					else if ((pin=new(ses) PIN(ses,PIN::defPID,PageAddr::invAddr,PIN_READONLY|PIN_TRANSFORMED,pv,1))==NULL) {ses->free(pv); rc=RC_NORESOURCES;}
					else if ((rc=ses->getStore()->queryMgr->count(queryOp,c,~0UL))==RC_OK) {pv->setU64(c); pv->setPropID(PROP_SPEC_VALUE); cnt++;}
				} else for (TVOp tvo=op==STMT_INSERT||op==STMT_QUERY?TVO_READ:TVO_UPD; (rc=queryOp->next())==RC_OK; fRel=true,ses->rlatch=queryOp) {
					if (op!=STMT_QUERY && (qr.pb.isNull() || !qr.pb.isSet(PGCTL_NOREL))) {queryOp->release(); ses->rlatch=NULL; fRel=false;}
					if ((mode&MODE_DELETED)!=0 || (rc=ses->getStore()->queryMgr->checkLockAndACL(qr,tvo,queryOp))!=RC_NOACCESS && rc!=RC_NOTFOUND) {
						if (rc==RC_OK) {
							PINEx qpin(ses),*pq=&qr;
							if ((qr.epr.flags&PINEX_EXTPID)!=0 && extAddr.convert(OID(qr.id.pid))) ses->setExtAddr(extAddr);
							if (qr.pb.isNull()) {
								qpin.id=qr.id; qpin.epr=qr.epr; qpin.addr=qr.addr; pq=&qpin;
								if (op!=STMT_QUERY) rc=ses->getStore()->queryMgr->getBody(qpin,tvo,(mode&MODE_DELETED)!=0?GB_DELETED:0);
							}
//							if (trs!=NULL) rc=trs->transform(pq,1,&pin,1,nout,ses);
//							else 
							if (rc==RC_OK) rc=ses->getStore()->queryMgr->loadPIN(ses,qr.id,pin,mode|LOAD_ENAV,pq,ses);
							if ((qr.epr.flags&PINEX_EXTPID)!=0) ses->setExtAddr(PageAddr::invAddr);
							if (op!=STMT_QUERY && ((rc=ses->getStore()->queryMgr->apply(ses,op,*pq,values,nValues,mode,pin/*,params,nParams*/))==RC_NOACCESS || rc==RC_DELETED)) continue;
						}
						break;
					}
				}
			}
			if (fRel && !fNoRel) {queryOp->release(); ses->rlatch=NULL;}
			if (rc!=RC_OK) {
				if (rc!=RC_EOF) tx.resetOk(); 
				if (txcid!=NO_TXCID) {ses->getStore()->txMgr->releaseSnapshot(txcid); txcid=NO_TXCID;}
			}
			if (pin!=NULL) if (rc==RC_OK) cnt++; else {pin->destroy(); pin=NULL;}
		}
		return pin;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ICursor::next()\n");}
	return NULL;
}

RC Cursor::next(Value& res)
{
	try {
		RC rc=RC_EOF; res.setError();
		if (ses!=NULL && cnt<nReturn && queryOp!=NULL) {
			if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
			if (op!=STMT_QUERY && !tx.isStarted()) {if ((rc=tx.start())==RC_OK) tx.ok(); else return rc;}
			if (txid==INVALID_TXID) txid=ses->getTXID();
			else if (ses->getTXID()!=txid) {
				if ((mode&MODE_HOLD_RESULT)==0) return RC_CLOSED;
				// re-init & re-position
			}
			if (fSnapshot && txcid==NO_TXCID) if (ses->getTxState()==TX_NOTRAN) txcid=ses->getStore()->txMgr->assignSnapshot(); else fSnapshot=false;
			TxGuard txg(ses); ses->resetAbortQ(); bool fRel=true; uint64_t c; ses->rlatch=queryOp; PIN *pin;
			if (ses->getIdentity()==STORE_OWNER || queryOp->getSkip()==0 && (rc=skip())==RC_OK) {
				if (cnt!=0 && (stype==SEL_COUNT || stype==SEL_VALUE || stype==SEL_DERIVED)) rc=RC_EOF;
				else if (stype==SEL_COUNT) {
					if ((rc=ses->getStore()->queryMgr->count(queryOp,c,~0UL))==RC_OK) {res.setU64(c); res.setPropID(PROP_SPEC_VALUE); cnt++;}
				} else for (TVOp tvo=op==STMT_INSERT||op==STMT_QUERY?TVO_READ:TVO_UPD;(rc=queryOp->next())==RC_OK; fRel=true,ses->rlatch=queryOp) {
					assert(qr.pb.isNull() || qr.addr.defined());
					if ((mode&MODE_DELETED)!=0 || (rc=ses->getStore()->queryMgr->checkLockAndACL(qr,tvo,queryOp))!=RC_NOACCESS && rc!=RC_NOTFOUND) {
						if (qr.id.pid!=STORE_INVALID_PID || qr.unpack()==RC_OK) {
							if (qr.pb.isNull()||ses->getStore()->queryMgr->loadPIN(ses,qr.id,pin,mode|LOAD_ENAV,&qr,ses)!=RC_OK) res.set(qr.id); else res.set(pin);
							if (op==STMT_QUERY) cnt++;
							else {
								if (qr.pb.isNull() || !qr.pb.isSet(PGCTL_NOREL)) {queryOp->release(); fRel=false; ses->rlatch=NULL;}
								if ((rc=ses->getStore()->queryMgr->apply(ses,op,qr,values,nValues,mode,NULL/*,params,nParams*/))==RC_OK) cnt++;
								else if (rc==RC_NOACCESS || rc==RC_DELETED) continue;
							}
							break;
						}
					}
				}
			}
			if (fRel && !fNoRel) {queryOp->release(); ses->rlatch=NULL;}
			if (rc!=RC_OK) {
				if (rc!=RC_EOF) tx.resetOk(); 
				if (txcid!=NO_TXCID) {ses->getStore()->txMgr->releaseSnapshot(txcid); txcid=NO_TXCID;}
			}
		}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ICursor::next(PID&)\n"); return RC_INTERNAL;}
}

RC Cursor::next(PID& pid)
{
	try {
		RC rc=RC_EOF; pid=PIN::defPID;
		if (ses!=NULL && cnt<nReturn && queryOp!=NULL) {
			if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
			if (op!=STMT_QUERY && !tx.isStarted()) {if ((rc=tx.start())==RC_OK) tx.ok(); else return rc;}
			if (txid==INVALID_TXID) txid=ses->getTXID();
			else if (ses->getTXID()!=txid) {
				if ((mode&MODE_HOLD_RESULT)==0) return RC_CLOSED;
				// re-init & re-position
			}
			if (fSnapshot && txcid==NO_TXCID) if (ses->getTxState()==TX_NOTRAN) txcid=ses->getStore()->txMgr->assignSnapshot(); else fSnapshot=false;
			TxGuard txg(ses); ses->resetAbortQ(); ses->rlatch=queryOp; bool fRel=true;
			if (ses->getIdentity()==STORE_OWNER || queryOp->getSkip()==0 && (rc=skip())==RC_OK) {
				if (stype==SEL_COUNT||stype==SEL_VALUE||stype==SEL_DERIVED||stype==SEL_DERIVEDSET) rc=RC_INVOP;
				else for (TVOp tvo=op==STMT_INSERT||op==STMT_QUERY?TVO_READ:TVO_UPD;(rc=queryOp->next())==RC_OK; fRel=true,ses->rlatch=queryOp) {
					assert(qr.pb.isNull() || qr.addr.defined());
					if ((mode&MODE_DELETED)!=0 || (rc=ses->getStore()->queryMgr->checkLockAndACL(qr,tvo,queryOp))!=RC_NOACCESS && rc!=RC_NOTFOUND) {
						if (qr.id.pid!=STORE_INVALID_PID || qr.unpack()==RC_OK) {
							pid=qr.id;
							if (op==STMT_QUERY) cnt++;
							else {
								if (qr.pb.isNull() || !qr.pb.isSet(PGCTL_NOREL)) {queryOp->release(); fRel=false; ses->rlatch=NULL;}
								if ((rc=ses->getStore()->queryMgr->apply(ses,op,qr,values,nValues,mode,NULL/*,params,nParams*/))==RC_OK) cnt++;
								else if (rc==RC_NOACCESS || rc==RC_DELETED) continue;
							}
							break;
						}
					}
				}
			}
			if (fRel && !fNoRel) {queryOp->release(); ses->rlatch=NULL;}
			if (rc!=RC_OK) {
				if (rc!=RC_EOF) tx.resetOk(); 
				if (txcid!=NO_TXCID) {ses->getStore()->txMgr->releaseSnapshot(txcid); txcid=NO_TXCID;}
			}
		}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ICursor::next(PID&)\n"); return RC_INTERNAL;}
}

RC Cursor::next(IPIN *pins[],unsigned nPins,unsigned& nRet)
{
	try {
		RC rc=RC_EOF; nRet=0;
		if (op!=STMT_QUERY) return RC_INVOP; if (pins==NULL || nPins==0) return RC_INVPARAM;
		if (ses!=NULL && cnt<nReturn && queryOp!=NULL) {
			if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
			if (op!=STMT_QUERY && !tx.isStarted()) {if ((rc=tx.start())==RC_OK) tx.ok(); else return rc;}
			if (txid==INVALID_TXID) txid=ses->getTXID();
			else if (ses->getTXID()!=txid) {
				if ((mode&MODE_HOLD_RESULT)==0) return RC_CLOSED;
				// re-init & re-position
			}
			if (fSnapshot && txcid==NO_TXCID) if (ses->getTxState()==TX_NOTRAN) txcid=ses->getStore()->txMgr->assignSnapshot(); else fSnapshot=false;
			TxGuard txg(ses); uint64_t c; ses->resetAbortQ(); ses->rlatch=queryOp;
			if (ses->getIdentity()==STORE_OWNER || queryOp->getSkip()==0 || (rc=skip())==RC_OK) {
				if (cnt!=0 && (stype==SEL_COUNT || stype==SEL_VALUE || stype==SEL_DERIVED)) rc=RC_EOF;
				else if (stype==SEL_COUNT) {
					Value *pv=new(ses) Value; PIN *pin=NULL;
					if (pv==NULL) rc=RC_NORESOURCES;
					else if ((pin=new(ses) PIN(ses,PIN::defPID,PageAddr::invAddr,PIN_READONLY|PIN_TRANSFORMED,pv,1))==NULL) {ses->free(pv); rc=RC_NORESOURCES;}
					else if ((rc=ses->getStore()->queryMgr->count(queryOp,c,~0UL))!=RC_OK) pin->destroy();
					else {pv->setU64(c); pv->setPropID(PROP_SPEC_VALUE); cnt++; pins[0]=pin; nRet=1;}
				} else for (TVOp tvo=op==STMT_INSERT||op==STMT_QUERY?TVO_READ:TVO_UPD; (rc=queryOp->next())==RC_OK;) {
					if ((mode&MODE_DELETED)!=0 || (rc=ses->getStore()->queryMgr->checkLockAndACL(qr,tvo,queryOp))!=RC_NOACCESS && rc!=RC_NOTFOUND) {
						if (rc==RC_OK) {
							unsigned nP=min(nPins,nResults);
							for (unsigned i=0; i<nP; i++) {
								PIN *pin=NULL; PINEx *pe=results!=NULL?results[i]:&qr;
								if (pe->id.pid==STORE_INVALID_PID) {
									if (pe->props==NULL || pe->nProps==0) pins[i]=NULL;
									else if ((pin=new(ses) PIN(ses,PIN::defPID,PageAddr::invAddr,PIN_READONLY|PIN_TRANSFORMED,(Value*)pe->props,pe->nProps))==NULL) {rc=RC_NORESOURCES; break;}
									else {pins[i]=pin; pe->props=NULL; pe->nProps=0;}
								} else if ((rc=ses->getStore()->queryMgr->loadPIN(ses,pe->id,pin,mode|LOAD_ENAV,pe,ses))==RC_OK) pins[i]=pin; else break;
							}
							if (rc==RC_OK) {cnt++; nRet=nP;}
						}
						break;
					}
				}
			}
			if (!fNoRel) {queryOp->release(); ses->rlatch=NULL;}
			if (rc!=RC_OK) {
				if (rc!=RC_EOF) tx.resetOk(); 
				if (txcid!=NO_TXCID) {ses->getStore()->txMgr->releaseSnapshot(txcid); txcid=NO_TXCID;}
			}
		}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ICursor::next(IPIN *pins[]...)\n"); return RC_INTERNAL;}
}

RC Cursor::rewind()
{
	try {
		RC rc=RC_EOF;
		if (ses!=NULL && queryOp!=NULL) {
			if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
			if (cnt==0 || op!=STMT_QUERY && !tx.isStarted()) return RC_OK;
			ses->rlatch=queryOp;
			if ((rc=queryOp->rewind())==RC_OK) {cnt=0; queryOp->release();}
			ses->rlatch=NULL;
			if (rc!=RC_OK) {
				if (rc!=RC_EOF) tx.resetOk(); 
				if (txcid!=NO_TXCID) {ses->getStore()->txMgr->releaseSnapshot(txcid); txcid=NO_TXCID;}
			}
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
	if (curs!=NULL) switch (dir) {
	default: break;
	case GO_FINDBYID: if (ei==STORE_COLLECTION_ID && curs!=NULL) curs->release(); break;
	case GO_FIRST: if (curs->rewind()!=RC_OK) break;
	case GO_NEXT: freeV(v); v.setError(); if (curs->next(v)==RC_OK) return &v; break;			// release??
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
	//???
	return 0;
}

void CursorNav::destroy()
{
	delete this;
}

//--------------------------------------------------------------------------------------------------------------------------------

RC QueryPrc::getBody(PINEx& cb,TVOp tvo,ulong flags,VersionID vid)
{
	bool fRemote=false,fTry=true,fWrite=tvo!=TVO_READ; RC rc; PID id; IdentityID iid; cb.epr.flags&=~PINEX_ADDRSET;
	if (cb.id.pid==STORE_INVALID_PID && (rc=cb.unpack())!=RC_OK) return rc;
	if (!cb.addr.defined()) {fTry=false; if (isRemote(cb.id)) fRemote=true; else if (!cb.addr.convert(cb.id.pid)) return RC_CORRUPTED;}
	for (ulong fctl=(fWrite?PGCTL_ULOCK:0)|((flags&GB_SAFE)!=0?QMGR_TRY:0);;) {
		if (!fRemote) {
			PBlock *pb=cb.ses->getLatched(cb.addr.pageID);
			if (pb==NULL || pb==cb.pb) cb.pb=ctx->bufMgr->getPage(cb.addr.pageID,ctx->heapMgr,fctl,!cb.pb.isSet(PGCTL_NOREL)?(PBlock*)cb.pb:(PBlock*)0);
			else if (fWrite && !pb->isULocked() && !pb->isXLocked()) return RC_DEADLOCK;
			else {cb.pb.release(); cb.pb=pb; cb.pb.set(PGCTL_NOREL);}
		} else if ((rc=ctx->netMgr->getPage(cb.id,fctl,cb.addr.idx,cb.pb,cb.ses))!=RC_OK) return rc;
		else {fRemote=fTry=false; cb.addr.pageID=cb.pb->getPageID();}

		if (cb.pb.isNull()) return (flags&GB_SAFE)!=0?RC_FALSE:RC_NOTFOUND;
		if (fWrite && cb.pb->isULocked()) {fctl|=QMGR_UFORCE; cb.pb.set(QMGR_UFORCE);}
		
		cb.hp=(const HeapPageMgr::HeapPage *)cb.pb->getPageBuf();
		cb.hpin=(const HeapPageMgr::HeapPIN *)cb.hp->getObject(cb.hp->getOffset(cb.addr.idx));
		if (cb.hpin==NULL) {
			if (ctx->lockMgr->getTVers(cb,tvo)==RC_OK && cb.tv!=NULL) {
				// deleted in uncommitted tx?
			}
		} else if (cb.hpin->hdr.getType()==HO_FORWARD) {
			if ((flags&GB_FORWARD)!=0) return RC_OK;
			memcpy(&cb.addr,(const byte*)cb.hpin+sizeof(HeapPageMgr::HeapObjHeader),PageAddrSize); continue;
		} else {
			if (cb.hpin->hdr.getType()!=HO_PIN) return RC_CORRUPTED;
			if (!cb.hpin->getAddr(id)) {id.pid=OID(cb.addr); id.ident=STORE_OWNER;}
			if (cb.id==id) {
				cb.epr.flags|=PINEX_ADDRSET; rc=RC_OK;
				if (isRemote(cb.id)) {
					//???
				} else if ((rc=ctx->lockMgr->getTVers(cb,tvo))!=RC_OK) return rc;
				if (!cb.ses->inWriteTx()) {
					if (tvo!=TVO_READ) return RC_READTX;
					if (cb.tv!=NULL) {
						//...
						//cb.flags|=PINEX_TVERSION;
					}
					if ((flags&GB_DELETED)==0 && (cb.hpin->hdr.descr&HOH_DELETED)!=0) return RC_DELETED;		// tv???
				} else {
					const ulong lck=tvo==TVO_READ?PINEX_LOCKED:PINEX_XLOCKED;
					if ((cb.epr.flags&lck)==0) {
						if ((rc=ctx->lockMgr->lock(tvo==TVO_READ?LOCK_SHARED:LOCK_EXCLUSIVE,cb))!=RC_OK) {cb.pb.release(); return rc;}
						cb.epr.flags|=lck|PINEX_LOCKED; if (cb.pb.isNull()) continue;
					}
					if ((flags&GB_DELETED)==0 && (cb.hpin->hdr.descr&HOH_DELETED)!=0) return RC_DELETED;
				}
				if (vid!=STORE_CURRENT_VERSION && vid<cb.hpin->getStamp()) {
					//???continue???
				}
				if ((iid=cb.ses->getIdentity())!=STORE_OWNER) rc=checkACLs(cb,iid,tvo,flags);
				if (rc!=RC_OK) cb.pb.release(); else if (cb.pb.isNull()) continue;
				return rc;
			}
		}
		if (!fTry) {
			if (cb.hpin!=NULL && (!cb.addr.convert(cb.id.pid) || cb.ses->getExtAddr()!=cb.addr)) 
				report(MSG_ERROR,"findPage: page %08X corruption\n",cb.hp->hdr.pageID);
			return RC_NOTFOUND;
		}
		fTry=false;
		if (isRemote(cb.id)) fRemote=true; else if (!cb.addr.convert(cb.id.pid)) return RC_CORRUPTED;
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
		refTrc.id=v.refId->id; ppv=&pv; i=1; rc=getRefValues(v.refId->id,ppv,i,0,cb);
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

RC QueryPrc::getRefValues(const PID& id,Value *&vals,ulong& nValues,ulong mode,PINEx& cb)
{
	if (cb.pb.isNull()) return RC_INVPARAM;
	PageAddr addr; RC rc=RC_OK; PBlockP pb; PBlock *pb2;
	PageID pid=cb.pb->getPageID(); bool fU=cb.pb->isULocked();	// our ULock???
	if (isRemote(id)) {
		if (ctx->netMgr->getPage(id,QMGR_TRY,addr.idx,pb,cb.ses)!=RC_OK) {
			pb=cb.pb; if (cb.pb.isSet(PGCTL_NOREL)) pb.set(PGCTL_NOREL); cb.pb=NULL;
			if ((rc=ctx->netMgr->getPage(id,fU?QMGR_UFORCE:0,addr.idx,pb,cb.ses))==RC_OK) rc=RC_REPEAT;
		}
	} else if (!addr.convert(id.pid)) return RC_CORRUPTED;
	else if (cb.ses!=NULL && (pb2=cb.ses->getLatched(addr.pageID))!=NULL) {pb=pb2; pb.set(PGCTL_NOREL);}
	else if ((pb=ctx->bufMgr->getPage(addr.pageID,ctx->heapMgr,QMGR_TRY))==NULL) {
		rc=(pb=ctx->bufMgr->getPage(addr.pageID,ctx->heapMgr,fU?QMGR_UFORCE:0,cb.pb))==NULL?RC_CORRUPTED:RC_REPEAT;
		cb.pb=NULL;
	}
	if (rc==RC_OK||rc==RC_REPEAT) while (pb!=NULL) {
		PINEx cb2(cb.ses,pb,addr,false); if (cb2.hpin==NULL) {rc=RC_NOTFOUND; break;}
		if (cb2.hpin->hdr.getType()==HO_FORWARD) {
			memcpy(&addr,(const byte*)cb2.hpin+sizeof(HeapPageMgr::HeapObjHeader),PageAddrSize);
			if (pb.isSet(PGCTL_NOREL)) pb=NULL;
			if (cb.ses!=NULL && (pb2=cb.ses->getLatched(addr.pageID))!=NULL) {pb.release(); pb=pb2; pb.set(PGCTL_NOREL);}
			else if ((pb=ctx->bufMgr->getPage(addr.pageID,ctx->heapMgr,QMGR_TRY,pb))==NULL) {
				if (!cb.pb.isNull()) {if (!cb.pb.isSet(PGCTL_NOREL)) {pb.release(); pb=cb.pb;} cb.pb=NULL;}
				rc=(pb=ctx->bufMgr->getPage(addr.pageID,ctx->heapMgr,fU?QMGR_UFORCE:0,pb))==NULL?RC_CORRUPTED:RC_REPEAT;
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
	if (cb.pb.isNull()) {cb.pb=ctx->bufMgr->getPage(pid,ctx->heapMgr,fU?PGCTL_ULOCK:0,pb); pb=NULL;}
	return rc;
}

RC QueryPrc::getPINValue(const PID& id,Value& res,ulong mode,Session *ses)
{
	PINEx cb(ses,id); const PINEx *pcb=&cb; Expr *exp; RC rc; Value v;
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

//---------------------------------------------------------------------------------------------------------------------


PINEx::PINEx(Session *s,PBlock *p,const PageAddr &ad,bool fRel)
	:	ses(s),id(PIN::defPID),props(NULL),nProps(0),mode(0),addr(ad),pb(p,fRel?0:PGCTL_NOREL),hp((const HeapPageMgr::HeapPage*)p->getPageBuf()),
		hpin((const HeapPageMgr::HeapPIN *)hp->getObject(hp->getOffset(ad.idx))),tv(NULL),degree(0.)
{
	epr.flags=0; epr.lref=0; if (hpin!=NULL && !hpin->getAddr(id)) {id.pid=ad; id.ident=STORE_OWNER;}
}

PINEx::PINEx(const PIN *p)
	: ses(p->ses),id(p->id),props(p->properties),nProps(p->nProperties),mode(p->mode),addr(p->addr),hp(NULL),hpin(NULL),tv(NULL),degree(0.)
{
	epr.flags=0; epr.lref=0;
}

void PINEx::operator=(const PINEx& cb)
{
	cleanup(); *this=cb.id; *this=cb.addr; epr.flags=cb.epr.flags; tv=cb.tv; epr.lref=0;
	if (cb.props==NULL) {pb=cb.pb; hp=cb.hp; hpin=cb.hpin;}
	else {props=cb.props; nProps=cb.nProps; cb.epr.flags&=~PINEX_DESTROY;} 
}

bool PINEx::defined(const PropertyID *pids,unsigned nP) const
{
	if (pids!=NULL && nP!=0) {
		if (props==NULL && hpin==NULL) return false;
		for (ulong i=0; i<nP; i++) 
			if (props==NULL) {if (hpin->findProperty(pids[i])==NULL) return false;}
			else if (BIN<Value,PropertyID,ValCmp>::find(pids[i],props,nProps)==NULL) return false;
	}
	return true;
}

bool PINEx::isCollection(PropertyID pid) const
{
	if (props!=NULL) {
		const Value *cv=BIN<Value,PropertyID,ValCmp>::find(pid,props,nProps);
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
	RC rc;
	if (props==NULL && pid!=PROP_SPEC_PINID) {
		if (!pb.isNull()) return ses->getStore()->queryMgr->loadV(v,pid,*this,mode,ma,eid);
		v.setError(pid); return RC_NOTFOUND;
	}
	switch (pid) {
	default: break;
	case STORE_INVALID_PROPID: 
		if ((mode&LOAD_CARDINALITY)==0) {v.setError(); return RC_INVPARAM;} else {v.set(0u); return RC_OK;}
	case PROP_SPEC_PINID:
		if ((mode&LOAD_CARDINALITY)!=0) v.set(1u);
		else if (id.pid!=STORE_INVALID_PID || (rc=unpack())==RC_OK) v.set(id);
		else return rc;
		v.property=pid; return RC_OK;
//	case PROP_SPEC_STAMP:
//		if ((mode&LOAD_CARDINALITY)!=0) v.set(1u); else v.set((unsigned int)stamp); v.property=pid; return RC_OK;
	}
	const Value *pv=BIN<Value,PropertyID,ValCmp>::find(pid,props,nProps);
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
	case VT_COLLECTION:
		return pv->nav->getElementByID(eid,v);
	}
	v=*pv; v.flags=NO_HEAP; return RC_OK;
}

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

void PINEx::free()
{
	if (props!=NULL && (epr.flags&PINEX_DESTROY)!=0) {
		for (unsigned i=0; i<nProps; i++) freeV(*(Value*)&props[i]);
		ses->free((Value*)props);
	}
	//if (tv!=NULL) ???
}
