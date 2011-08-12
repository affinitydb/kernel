/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "maps.h"
#include "queryprc.h"
#include "parser.h"
#include "stmt.h"
#include "expr.h"
#include <stdio.h>

using namespace MVStoreKernel;

void SessionX::abortQuery()
{
	try {ses->abortQ();} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::abortQuery()\n");}
}

Stmt::~Stmt()
{
	for (QVar *var=vars,*vnxt; var!=NULL; var=vnxt) {vnxt=var->next; delete var;}
	if (orderBy!=NULL) {
		for (unsigned i=0; i<nOrderBy; i++) if ((orderBy[i].flags&ORDER_EXPR)!=0) ma->free(orderBy[i].expr);
		ma->free(orderBy);
	}
	if (values!=NULL) freeV(values,nValues,ma);
}

STMT_OP Stmt::getOp() const
{
	try {return op;} catch (...) {report(MSG_ERROR,"Exception in IStmt::getOp()\n"); return STMT_OP_ALL;}
}

QVar::QVar(QVarID i,byte ty,MemAlloc *m)
	: next(NULL),id(i),type(ty),stype(SEL_PINSET),dtype(DT_DEFAULT),ma(m),name(NULL),dscr(NULL),nDscr(0),varProps(NULL),nVarProps(0),
	nConds(0),nHavingConds(0),groupBy(NULL),nGroupBy(0),condProps(NULL),lProps(0),fHasParent(false)
{
	cond=havingCond=NULL;
}

QVar::~QVar()
{
	if (name!=NULL) ma->free(name);
	if (dscr!=NULL) {
		for (ulong i=0; i<nDscr; i++) if (dscr[i].vals!=NULL) {
			for (ulong j=0; j<dscr[i].nValues; j++) freeV(dscr[i].vals[j]);
			ma->free(dscr[i].vals);
		}
		ma->free(dscr);
	}
	if (varProps!=NULL) {
		for (ulong i=0; i<nVarProps; i++) if (varProps[i].props!=NULL) ma->free(varProps[i].props);
		ma->free(varProps);
	}
	if (nConds==1) ma->free(cond);
	else if (conds!=NULL) {
		for (unsigned i=0; i<nConds; i++) ma->free(conds[i]);
		ma->free(conds);
	}
	if (nHavingConds==1) ma->free(havingCond);
	else if (havingConds!=NULL) {
		for (unsigned i=0; i<nHavingConds; i++) ma->free(havingConds[i]);
		ma->free(havingConds);
	}
	if (groupBy!=NULL) {
		for (unsigned i=0; i<nGroupBy; i++) if ((groupBy[i].flags&ORDER_EXPR)!=0) ma->free(groupBy[i].expr);
		ma->free(groupBy);
	}
	ma->free(condProps);
}

SimpleVar::~SimpleVar()
{
	if (classes!=NULL) {
		for (ulong i=0; i<nClasses; i++) {ClassSpec& cs=classes[i]; if (cs.params!=NULL) freeV((Value*)cs.params,cs.nParams,ma);}
		ma->free(classes);
	}
	for (CondIdx *icond=condIdx,*icnext; icond!=NULL; icond=icnext) {
		if (icond->expr!=NULL) ma->free(icond->expr);
		icnext=icond->next; ma->free(icond);
	}
	for (CondFT *ftcond=condFT,*ftnext; ftcond!=NULL; ftcond=ftnext)
		{ftnext=ftcond->next; ma->free((char*)ftcond->str); ma->free(ftcond);}
	ma->free(pids); ma->free(props);
	if (subq!=NULL) subq->destroy();
	if (path!=NULL) {
		for (unsigned i=0; i<nPathSeg; i++) if (path[i].filter!=NULL) path[i].filter->destroy();
		ma->free(path);
	}
}

JoinVar::~JoinVar()
{
	for (CondEJ *cej=condEJ,*cej2; cej!=NULL; cej=cej2) {cej2=cej->next; ma->free(cej);}
}

QVarID Stmt::addVariable(const ClassSpec *classes,unsigned nClasses,IExprTree *cond)
{
	try {
		// shutdown ???
		if (nVars>=255) return INVALID_QVAR_ID;
		SimpleVar *var = new(ma) SimpleVar((QVarID)nVars,ma); if (var==NULL) return INVALID_QVAR_ID;
		if (classes!=NULL && nClasses!=0) {
			var->classes=new(ma) ClassSpec[nClasses];
			if (var->classes==NULL) {delete var; return INVALID_QVAR_ID;}
			for (var->nClasses=0; var->nClasses<nClasses; var->nClasses++) {
				ClassSpec &to=var->classes[var->nClasses]; const ClassSpec &from=classes[var->nClasses];
				to.classID=from.classID; to.params=NULL; to.nParams=0;
				if ((to.classID&CLASS_PARAM_REF)!=0) mode|=QRY_CPARAMS;
				if (from.params!=NULL && from.nParams!=0) {
					Value *pv=new(ma) Value[to.nParams=from.nParams]; 
					if (pv==NULL) {delete var; return INVALID_QVAR_ID;}
					for (ulong j=0; j<to.nParams; j++) {
						from.params[j].flags=VF_EXT|NO_HEAP;
						if (copyV(from.params[j],pv[j],ma)!=RC_OK) {delete var; return INVALID_QVAR_ID;}
					}
					to.params=pv;
				}
			}
		}
		if (cond!=NULL && processCondition((ExprTree*)cond,var)!=RC_OK) {delete var; return INVALID_QVAR_ID;}
		var->next=vars; vars=var; nVars++; top=++nTop==1?var:(QVar*)0; return byte(var->id);
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IStmt::addVariable()\n");}
	return INVALID_QVAR_ID;
}

QVarID Stmt::addVariable(const PID& pid,PropertyID propID,IExprTree *cond)
{
	try {
		// shutdown ???
		if (nVars>=255) return INVALID_QVAR_ID;
		SimpleVar *var = new(ma) SimpleVar((QVarID)nVars,ma); if (var==NULL) return INVALID_QVAR_ID;
		if ((var->pids=new(ma) PID)==NULL) return RC_NORESOURCES; var->pids[0]=pid; var->nPids=1;
		if ((var->path=new(ma) PathSeg)==NULL) return RC_NORESOURCES;
		var->path[0].pid=propID; var->path[0].eid=STORE_COLLECTION_ID;
		var->path[0].filter=NULL; var->path[0].cls=STORE_INVALID_CLASSID;
		var->path[0].rmin=var->path[0].rmax=1; var->path[0].fLast=false; var->nPathSeg=1;
		if (cond!=NULL && processCondition((ExprTree*)cond,var)!=RC_OK) {delete var; return INVALID_QVAR_ID;}
		var->next=vars; vars=var; nVars++; top=++nTop==1?var:(QVar*)0; return byte(var->id);
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IStmt::addVariable(collection)\n");}
	return INVALID_QVAR_ID;
}

QVarID Stmt::addVariable(IStmt *qry)
{
	try {
		// shutdown ???
		if (nVars>=255) return INVALID_QVAR_ID;
		SimpleVar *var = new(ma) SimpleVar((QVarID)nVars,ma); if (var==NULL) return INVALID_QVAR_ID;
		var->subq=(Stmt*)qry->clone(); var->next=vars; vars=var; nVars++; top=++nTop==1?var:(QVar*)0;
		return byte(var->id);
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IStmt::addVariable(query)\n");}
	return INVALID_QVAR_ID;
}

QVarID Stmt::setOp(QVarID leftVar,QVarID rightVar,QUERY_SETOP op)
{
	try {
		// shutdown ???
		if (op<QRY_UNION || op>=QRY_ALL_SETOP || nVars>=255) return INVALID_QVAR_ID;
		SetOpVar *var=new(2,ma) SetOpVar(2,(QVarID)nVars,op,ma); if (var==NULL) return INVALID_QVAR_ID;
		var->vars[0].var=findVar(leftVar); var->vars[1].var=findVar(rightVar);
		if (var->vars[0].var==NULL || var->vars[0].var->fHasParent || var->vars[1].var==NULL || var->vars[1].var->fHasParent)
			{delete var; return INVALID_QVAR_ID;}
		var->next=vars; vars=var; nVars++; var->vars[0].var->fHasParent=var->vars[1].var->fHasParent=true;
		assert(nTop>=2); if (--nTop==1) top=var; return var->id;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IStmt::setOp(left,right)\n");}
	return INVALID_QVAR_ID;
}

QVarID Stmt::setOp(const QVarID *varids,unsigned nv,QUERY_SETOP op)
{
	try {
		// shutdown ???
		if (op<QRY_UNION || op>=QRY_ALL_SETOP || nv>2 && op==QRY_EXCEPT || nVars>=255) return INVALID_QVAR_ID;
		SetOpVar *var=new(nv,ma) SetOpVar(nv,(QVarID)nVars,op,ma); if (var==NULL) return INVALID_QVAR_ID;
		for (unsigned i=0; i<nv; i++) {
			QVar *v=var->vars[i].var=findVar(varids[i]);
			if (v==NULL || v->fHasParent) {delete var; return INVALID_QVAR_ID;}
		}
		var->next=vars; vars=var; nVars++;
		for (unsigned i=0; i<nv; i++) var->vars[i].var->fHasParent=true;
		assert(nTop>=nv); if ((nTop-=nv-1)==1) top=var; return var->id;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IStmt::setOp(vars[])\n");}
	return INVALID_QVAR_ID;
}

QVarID Stmt::join(QVarID leftVar,QVarID rightVar,IExprTree *cond,QUERY_SETOP op,PropertyID vPropID)
{
	try {
		// shutdown ???
		if (op>=QRY_UNION || nVars>=255) return INVALID_QVAR_ID;
		JoinVar *var=new(2,ma) JoinVar(2,(QVarID)nVars,op,ma); if (var==NULL) return INVALID_QVAR_ID;
		var->vars[0].var=findVar(leftVar); var->vars[1].var=findVar(rightVar);
		if (var->vars[0].var==NULL || var->vars[0].var->fHasParent || var->vars[1].var==NULL || var->vars[1].var->fHasParent)
			{delete var; return INVALID_QVAR_ID;}
		if (cond!=NULL) {
			if (processCondition((ExprTree*)cond,var)!=RC_OK) {delete var; return INVALID_QVAR_ID;}
			// save vPropID too ???
		}
		var->next=vars; vars=var; nVars++; var->vars[0].var->fHasParent=var->vars[1].var->fHasParent=true;
		assert(nTop>=2); if (--nTop==1) top=var; return var->id;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IStmt::join(left,right)\n");}
	return INVALID_QVAR_ID;
}

QVarID Stmt::join(const QVarID *varids,unsigned nv,IExprTree *cond,QUERY_SETOP op,PropertyID vPropID)
{
	try {
		// shutdown ???
		if (op>=QRY_UNION || nVars>=255) return INVALID_QVAR_ID;
		JoinVar *var=new(nv,ma) JoinVar(nv,(QVarID)nVars,op,ma); if (var==NULL) return INVALID_QVAR_ID;
		for (unsigned i=0; i<nv; i++) {
			QVar *v=var->vars[i].var=findVar(varids[i]);
			if (v==NULL || v->fHasParent) {delete var; return INVALID_QVAR_ID;}
		}
		if (cond!=NULL) {
			if (processCondition((ExprTree*)cond,var)!=RC_OK) {delete var; return INVALID_QVAR_ID;}
			// save vPropID too ???
		}
		var->next=vars; vars=var; nVars++;
		for (unsigned i=0; i<nv; i++) var->vars[i].var->fHasParent=true;
		assert(nTop>=nv); if ((nTop-=nv-1)==1) top=var; return var->id;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IStmt::join(vars[])\n");}
	return INVALID_QVAR_ID;
}

RC Stmt::setName(QVarID var,const char *name)
{
	try {
		// shutdown?
		QVar *qv=findVar(var); if (qv==NULL) return RC_NOTFOUND;
		if (qv->name!=NULL) {ma->free(qv->name); qv->name=NULL;}
		return name==NULL || *name=='\0' || (qv->name=strdup(name,ma))!=NULL?RC_OK:RC_NORESOURCES;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::setName()\n"); return RC_INTERNAL;}
}

RC Stmt::setDistinct(QVarID var,DistinctType dt)
{
	try {
		// shutdown?
		QVar *qv=findVar(var); if (qv==NULL) return RC_NOTFOUND;
		if (dt!=DT_DEFAULT) qv->dtype=dt; return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::setDistinct()\n"); return RC_INTERNAL;}
}

RC Stmt::addOutput(QVarID var,const Value *ds,unsigned nD)
{
	try {
		// shutdown?
		QVar *qv=findVar(var); if (qv==NULL) return RC_NOTFOUND;
		if (ds==NULL || nD==0 || qv->nDscr>=255) return RC_INVPARAM;
		qv->dscr=(TDescriptor*)ma->realloc(qv->dscr,(qv->nDscr+1)*sizeof(TDescriptor));
		if (qv->dscr==NULL) {qv->nDscr=0; return RC_NORESOURCES;}
		if (qv->groupBy!=NULL && qv->nGroupBy!=0) {
			qv->stype=nD==1?SEL_VALUESET:SEL_DERIVEDSET;
			for (unsigned i=0; i<nD; i++) {
				const Value &vv=ds[i]; vv.flags=VF_EXT|NO_HEAP; //byte op;
				switch (vv.type) {
				default: break;
				case VT_EXPRTREE:
					break;
				case VT_VARREF:
					break;
				}
			}
		} else {
			ds[0].flags=VF_EXT|NO_HEAP; byte op;
			switch (ds[0].type) {
			case VT_EXPRTREE:
				qv->stype=(op=ds[0].exprt->getOp())==OP_COUNT && ds[0].exprt->getOperand(0).type==VT_ANY?SEL_COUNT:
												op<OP_ALL && (SInCtx::opDscr[op].flags&_A)!=0?SEL_VALUE:SEL_VALUESET;
				break;
			case VT_VARREF:
				if (ds[0].length==1 && ds[0].property==STORE_INVALID_PROPID) {qv->stype=SEL_PROJECTED; break;}
			default:
				qv->stype=SEL_VALUESET;
			}
			for (unsigned i=1; i<nD; i++) {
				const Value &vv=ds[i]; vv.flags=VF_EXT|NO_HEAP;
				switch (vv.type) {
				case VT_EXPRTREE:
					if ((op=vv.exprt->getOp())<OP_ALL && (SInCtx::opDscr[op].flags&_A)!=0) {
						if (op==OP_COUNT && vv.exprt->getOperand(0).type==VT_ANY) return RC_INVPARAM;
					} else {
						//...
					}
					break;
					// SEL_DERIVEDSET, SEL_DERIVED, SEL_PROJECTED
				case VT_VARREF:
					if (ds[0].length==1 && ds[0].property==STORE_INVALID_PROPID) break;
				default:
					//???
					break;
				}
			}
		}
		TDescriptor& td=qv->dscr[qv->nDscr];
		if (++qv->nDscr>1) {
			// check others
			qv->stype=SEL_COMP_DERIVED;
		}
		return copyV(ds,td.nValues=nD,td.vals,ma);
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::addOutput()\n"); return RC_INTERNAL;}
}

RC Stmt::addCondition(QVarID var,IExprTree *cond,bool fHaving)
{
	try {
		// shutdown ???
		QVar *qv=findVar(var); if (qv==NULL) return RC_NOTFOUND;
		return cond!=NULL && (isBool(((ExprTree*)cond)->op) || ((ExprTree*)cond)->op==OP_CON && ((ExprTree*)cond)->operands[0].type==VT_BOOL) ?
			processCondition((ExprTree*)cond,qv) : RC_INVPARAM;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::addCondition()\n"); return RC_INTERNAL;}
}

RC Stmt::setPIDs(QVarID var,const PID *pids,unsigned nPids)
{
	try {
		// shutdown ???
		SimpleVar *qv=(SimpleVar*)findVar(var);
		if (qv==NULL) return RC_NOTFOUND; if (qv->type!=QRY_SIMPLE) return RC_INVOP;
		if (qv->pids!=NULL) {ma->free(qv->pids); qv->pids=0; qv->nPids=0;}
		if (pids!=NULL && nPids!=0) {
			if ((qv->pids=new(ma) PID[nPids])==NULL) return RC_NORESOURCES;
			memcpy(qv->pids,pids,nPids*sizeof(PID)); qv->nPids=nPids;
			if (nPids>1) qsort(qv->pids,nPids,sizeof(PID),cmpPIDs);
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::setPIDs()\n"); return RC_INTERNAL;}
}

RC Stmt::setPath(QVarID var,const PathSeg *segs,unsigned nSegs)
{
	try {
		// shutdown ???
		return setPath(var,segs,nSegs,true);
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::setPath()\n"); return RC_INTERNAL;}
}

RC Stmt::setPath(QVarID var,const PathSeg *segs,unsigned nSegs,bool fCopy)
{
	SimpleVar *qv=(SimpleVar*)findVar(var);
	if (qv==NULL) return RC_NOTFOUND; if (qv->type!=QRY_SIMPLE) return RC_INVOP;
	if (qv->path!=NULL) {
		for (unsigned i=0; i<qv->nPathSeg; i++) if (qv->path[i].filter!=NULL) qv->path[i].filter->destroy();
		ma->free(qv->path); qv->path=0; qv->nPathSeg=0;
	}
	if (segs!=NULL && nSegs!=0) {
		if ((qv->path=new(ma) PathSeg[nSegs])==NULL) return RC_NORESOURCES;
		memcpy(qv->path,segs,nSegs*sizeof(PathSeg)); qv->nPathSeg=nSegs;
		if (fCopy) for (unsigned i=0; i<qv->nPathSeg; i++) if (qv->path[i].filter!=NULL) 
				qv->path[i].filter=Expr::clone((Expr*)qv->path[i].filter,ma);
	}
	return RC_OK;
}

RC Stmt::addConditionFT(QVarID var,const char *str,unsigned flags,const PropertyID *pids,unsigned nPids)
{
	try {
		// shutdown ???
		if (str==NULL) return RC_INVPARAM;
		SimpleVar *qv=(SimpleVar*)findVar(var); if (qv==NULL) return RC_NOTFOUND; if (qv->type!=QRY_SIMPLE) return RC_INVOP;
		CondFT *ftcond=new(nPids,ma) CondFT(qv->condFT,strdup(str,ma),flags,pids,nPids);
		if (ftcond==NULL) return RC_NORESOURCES; qv->condFT=ftcond; return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::addConditionFT()\n"); return RC_INTERNAL;}
}

static int __cdecl cmpProps(const void *v1, const void *v2)
{
	return cmp3(*(const PropertyID*)v1,*(const PropertyID*)v2);
}

RC Stmt::setPropCondition(QVarID var,const PropertyID *props,unsigned nProps,bool fOr)
{
	try {
		// shutdown ???
		if (props==NULL || nProps==0 || nProps>255) return RC_INVPARAM;
		SimpleVar *qv=(SimpleVar*)findVar(var);
		if (qv==NULL) return RC_NOTFOUND; if (qv->type!=QRY_SIMPLE) return RC_INVOP;
		if (nProps>1) {
			PropertyID *buf=(PropertyID*)alloca(nProps*sizeof(PropertyID)); if (buf==NULL) return RC_NORESOURCES;
			memcpy(buf,props,nProps*sizeof(PropertyID)); qsort(buf,nProps,sizeof(PropertyID),cmpProps); props=buf;
		}
		if (qv->props==NULL) {
			if ((qv->props=new(ma) PropertyID[qv->nProps=nProps])==NULL) return RC_NORESOURCES;
			memcpy(qv->props,props,sizeof(PropertyID)*nProps);
		} else {
			// add one by one
		}
		return PropDNF::andP(qv->condProps,qv->lProps,props,nProps,ma,false);
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::setPropCondition()\n"); return RC_INTERNAL;}
}

RC Stmt::setJoinProperties(QVarID var,const PropertyID *props,unsigned nProps)
{
	try {
		// shutdown ???
		QVar *qv=findVar(var); if (qv==NULL) return RC_NOTFOUND; if (qv->type>QRY_OUTERJOIN) return RC_INVOP;
		JoinVar *jv=(JoinVar*)qv; if (jv->nVars!=2) return RC_INVOP; CondEJ *ej;
		for (unsigned i=nProps; i--!=0; ) {
			if ((ej=new(ma) CondEJ(props[i],props[i],0))==NULL) return RC_NORESOURCES;
			ej->next=jv->condEJ; jv->condEJ=ej;
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::setJoinProperties()\n"); return RC_INTERNAL;}
}

RC Stmt::setGroup(QVarID var,const OrderSeg *segs,unsigned nSegs)
{
	try {
		// shutdown ???
		QVar *qv=findVar(var); if (qv==NULL) return RC_NOTFOUND;
		if (qv->groupBy!=NULL) {
			for (unsigned i=0; i<qv->nGroupBy; i++) if ((qv->groupBy[i].flags&ORDER_EXPR)!=0) qv->groupBy[i].expr->destroy();
			ma->free(qv->groupBy); qv->groupBy=NULL;
		}
		if (segs!=NULL && nSegs!=0) {
			if ((qv->groupBy=new(ma) OrderSegQ[nSegs])==NULL) return RC_NORESOURCES;
			for (unsigned i=0; i<nSegs; i++) {
				OrderSegQ& qs=qv->groupBy[i]; qs.flags=segs[i].flags&~ORDER_EXPR; qs.lPref=segs[i].lPrefix; RC rc;
				if (segs[i].expr==NULL) qs.pid=segs[i].pid;
				else if ((rc=Expr::compile((ExprTree*)segs[i].expr,qs.expr,ma,&qv->condProps,&qv->lProps))==RC_OK) qs.flags|=ORDER_EXPR;
				else {qv->nGroupBy=i; return rc;}
			}
			qv->nGroupBy=nSegs;
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::setGroup()\n"); return RC_INTERNAL;}
}

RC Stmt::setOrder(const OrderSeg *segs,unsigned nSegs)
{
	try {
		// shutdown ???
		if (orderBy!=NULL) {
			for (unsigned i=0; i<nOrderBy; i++) if ((orderBy[i].flags&ORDER_EXPR)!=0) orderBy[i].expr->destroy();
			ma->free(orderBy); orderBy=NULL; nOrderBy=0;
		}
		if (segs!=NULL && nSegs!=0) {
			if ((orderBy=new(ma) OrderSegQ[nSegs])==NULL) return RC_NORESOURCES;
			for (unsigned i=0; i<nSegs; i++) {
				OrderSegQ &os=orderBy[i]; RC rc; PropDNF *props=NULL; size_t lProps=0;
				os.flags=segs[i].flags&~ORDER_EXPR; os.lPref=segs[i].lPrefix;
				if (segs[i].expr==NULL) os.pid=segs[i].pid;
				else if ((rc=Expr::compile((ExprTree*)segs[i].expr,os.expr,ma,&props,&lProps))!=RC_OK) {nOrderBy=i; return rc;}
				else {
					os.flags|=ORDER_EXPR;	// props, lProps -> ???
				}
			}
			nOrderBy=nSegs;
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::setOrder()\n"); return RC_INTERNAL;}
}

RC Stmt::setValues(const Value *vals,unsigned nVals)
{
	try {
		RC rc=RC_OK; Expr *exp;
		if (values!=NULL) {freeV(values,nValues,ma); values=NULL; nValues=0;}
		if (vals!=NULL && nVals!=0) {
			if ((values=new(ma) Value[nVals])==NULL) rc=RC_NORESOURCES;
			else for (nValues=0; rc==RC_OK && nValues<nVals; nValues++) {
				const Value& from=vals[nValues]; Value& to=values[nValues];
				if (from.type!=VT_EXPRTREE) rc=copyV(from,to,ma);
				else if ((rc=Expr::compile((ExprTree*)from.exprt,exp,ma))==RC_OK) {
					to.set(exp); to.flags=ma->getAType(); to.setPropID(from.property); 
					to.meta=from.meta; to.op=from.op;
				}
			}
		}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::setValues()\n"); return RC_INTERNAL;}
}

RC Stmt::setValuesNoCopy(const Value *vals,unsigned nVals)
{
	RC rc=RC_OK; Expr *exp;
	if (values!=NULL) {freeV(values,nValues,ma); values=NULL; nValues=0;}
	values=(Value*)vals; nValues=nVals;
	for (unsigned i=0; i<nVals; i++) if (values[i].type==VT_EXPRTREE) {
		ExprTree *et=(ExprTree*)values[i].exprt;
		if ((rc=Expr::compile(et,exp,ma))!=RC_OK) break;
		values[i].expr=exp; values[i].type=VT_EXPR;
		values[i].meta|=META_PROP_EVAL; et->destroy();
	}
	return rc;
}

RC Stmt::processCondition(ExprTree *node,QVar *qv,int level)
{
	if (node!=NULL && node->nops>0) {
		const Value &v=node->operands[0],*pv=node->nops>1?&node->operands[1]:(Value*)0; RC rc; CondIdx *ci;
		switch (node->op) {
		default: break;
		case OP_EXISTS:
			if (node->nops==1 && v.type==VT_VARREF && v.length==1 && v.refPath.refN==0 && qv->type==QRY_SIMPLE) {
				if (v.refPath.id==PROP_SPEC_PINID || v.refPath.id==PROP_SPEC_STAMP) return RC_OK;
				const bool fNot=(node->flags&NOT_BOOLEAN_OP)!=0;
				if (!fNot && (rc=mv_bins<PropertyID,unsigned>(((SimpleVar*)qv)->props,((SimpleVar*)qv)->nProps,v.refPath.id,ma))!=RC_OK) return rc;
				if (PropDNF::andP(qv->condProps,qv->lProps,&v.refPath.id,1,ma,fNot)==RC_OK && !fNot) return RC_OK;
			}
			break;
		case OP_EQ:
			// commutativity ?
			if (v.type==VT_VARREF) {
				if (pv->type==VT_REFID||pv->type==VT_REF) {
					if (qv->type==QRY_SIMPLE && (v.length==0 || v.length==1 && v.refPath.id==PROP_SPEC_PINID) && (((SimpleVar*)qv)->pids=(PID*)ma->malloc(sizeof(PID)))!=NULL)
						{((SimpleVar*)qv)->pids[((SimpleVar*)qv)->nPids++]=pv->type==VT_REFID?pv->id:pv->pin->getPID(); return RC_OK;}
					break;
				}
				// multijoin!
				if (qv->type<QRY_UNION && v.length<2 && v.refPath.refN<2 && pv->type==VT_VARREF && pv->length<2 && pv->refPath.refN<2 && pv->refPath.refN!=v.refPath.refN) {
					PropertyID pids[2]; pids[v.refPath.refN]=v.length==0?PROP_SPEC_PINID:v.refPath.id; pids[pv->refPath.refN]=pv->length==0?PROP_SPEC_PINID:pv->refPath.id;
					CondEJ *cnd=new(ma) CondEJ(pids[0],pids[1],node->flags&(FOR_ALL_LEFT_OP|EXISTS_LEFT_OP|FOR_ALL_RIGHT_OP|EXISTS_RIGHT_OP));
					if (cnd!=NULL) {cnd->next=((JoinVar*)qv)->condEJ; ((JoinVar*)qv)->condEJ=cnd; return RC_OK;}
					break;
				}
			}
		case OP_IN:
			if (v.type==VT_VARREF && qv->type==QRY_SIMPLE && (v.length==0 || v.length==1 && v.refPath.id==PROP_SPEC_PINID)) {
				ulong i,l; const Value *cv; bool fIDs=true;
				switch (pv->type) {
				case VT_ARRAY:
					for (i=0,l=pv->length; i<l; i++) 
						if (pv->varray[i].type!=VT_REFID && pv->varray[i].type!=VT_REF) {fIDs=false; break;}
					if (fIDs && (((SimpleVar*)qv)->pids=(PID*)ma->malloc(l*sizeof(PID)))!=NULL) {
						for (i=0; i<l; i++) ((SimpleVar*)qv)->pids[((SimpleVar*)qv)->nPids++]=pv->varray[i].type==VT_REFID?pv->varray[i].id:pv->varray[i].pin->getPID();
						if (((SimpleVar*)qv)->nPids>1) qsort(((SimpleVar*)qv)->pids,((SimpleVar*)qv)->nPids,sizeof(PID),cmpPIDs); return RC_OK;
					}
					break;
				case VT_COLLECTION:
					for (cv=pv->nav->navigate(GO_FIRST),l=0; cv!=NULL; ++l,cv=pv->nav->navigate(GO_NEXT))
						if (cv->type!=VT_REFID && cv->type!=VT_REF) {fIDs=false; break;}
					if (fIDs && (((SimpleVar*)qv)->pids=(PID*)ma->malloc(l*sizeof(PID)))!=NULL) {
						for (cv=pv->nav->navigate(GO_FIRST); cv!=NULL; cv=pv->nav->navigate(GO_NEXT))
							((SimpleVar*)qv)->pids[((SimpleVar*)qv)->nPids++]=cv->type==VT_REFID?cv->id:cv->pin->getPID();
						if (((SimpleVar*)qv)->nPids>1) qsort(((SimpleVar*)qv)->pids,((SimpleVar*)qv)->nPids,sizeof(PID),cmpPIDs); return RC_OK;
					}
					break;
				case VT_REFID:
					if ((((SimpleVar*)qv)->pids=(PID*)ma->malloc(sizeof(PID)))!=NULL) {((SimpleVar*)qv)->pids[0]=pv->id; ((SimpleVar*)qv)->nPids=1; return RC_OK;}
					break;
				case VT_REF:
					if ((((SimpleVar*)qv)->pids=(PID*)ma->malloc(sizeof(PID)))!=NULL) {((SimpleVar*)qv)->pids[0]=pv->pin->getPID(); ((SimpleVar*)qv)->nPids=1; return RC_OK;}
					break;
				}
			}
		case OP_BEGINS:
			if ((node->flags&NOT_BOOLEAN_OP)!=0) break;			// no OP_NE !
		case OP_LT: case OP_LE: case OP_GT: case OP_GE:
			assert((node->flags&NOT_BOOLEAN_OP)==0);
			if (qv->type==QRY_SIMPLE && ((SimpleVar*)qv)->nCondIdx<255 && pv->type==VT_PARAM && pv->length==0) {
				ushort flags=((node->flags&CASE_INSENSITIVE_OP)!=0?ORD_NCASE:0)|(pv->refPath.flags&(ORD_DESC|ORD_NCASE|ORD_NULLS_BEFORE|ORD_NULLS_AFTER));
				if (node->op==OP_GT || node->op==OP_IN && (node->flags&EXCLUDE_LBOUND_OP)!=0) flags|=SCAN_EXCLUDE_START;
				if (node->op==OP_LT || node->op==OP_IN && (node->flags&EXCLUDE_RBOUND_OP)!=0) flags|=SCAN_EXCLUDE_END;
				else if (node->op==OP_BEGINS) flags|=SCAN_PREFIX; else if (node->op==OP_EQ) flags|=SCAN_EXACT;
				bool fExpr=true; PropertyID pid=STORE_INVALID_PROPID; ushort lPref=0; long lstr;
				for (const Value *vv=&v; ;vv=&((ExprTree*)vv->exprt)->operands[0]) {
					if (vv->type==VT_VARREF && vv->refPath.refN==0 && vv->length==1) {fExpr=false; pid=vv->refPath.id; break;}
					if (vv->type==VT_EXPRTREE) {
						if (((ExprTree*)vv->exprt)->op==OP_UPPER || ((ExprTree*)vv->exprt)->op==OP_LOWER) {flags|=ORD_NCASE; continue;}
						if (((ExprTree*)vv->exprt)->op==OP_SUBSTR && Expr::getI(((ExprTree*)vv->exprt)->operands[1],lstr)==RC_OK && lstr>=0) {
							if ((((ExprTree*)vv->exprt)->nops==2 || ((ExprTree*)vv->exprt)->nops==3 && lstr==0 && 
								Expr::getI(((ExprTree*)vv->exprt)->operands[2],lstr)==RC_OK) && lstr>0) {if (lPref==0 || lstr<lPref) lPref=(ushort)lstr; continue;}
						}
					}
					lPref=0; flags&=~ORD_NCASE; break;
				}
				if (fExpr) {
					// expr ???
					// mode|=QRY_IDXEXPR;
				} else if ((flags&(ORD_NULLS_BEFORE|ORD_NULLS_AFTER))!=0 || PropDNF::andP(qv->condProps,qv->lProps,&pid,1,ma,false)==RC_OK)
					for (ci=((SimpleVar*)qv)->condIdx; ;ci=ci->next)
						if (ci==NULL) {
							IndexSeg ks={pid,flags,lPref,(ValueType)pv->refPath.type,node->op};
							if ((ci=new(qv->ma) CondIdx(ks,pv->refPath.refN,ma,NULL))!=NULL) {
								if (((SimpleVar*)qv)->condIdx==NULL) ((SimpleVar*)qv)->condIdx=ci; else ((SimpleVar*)qv)->lastCondIdx->next=ci;
								((SimpleVar*)qv)->lastCondIdx=ci; ((SimpleVar*)qv)->nCondIdx++; return RC_OK;
							}
							break;
						} else if (ci->expr==NULL && ci->ks.propID==pid) {
							if ((node->op==OP_GT||node->op==OP_GE) && (ci->ks.op==OP_LT||ci->ks.op==OP_LE)) {
								// convert to OP_IN ???
							} else if ((node->op==OP_LT||node->op==OP_LE) && (ci->ks.op==OP_GT||ci->ks.op==OP_GE)) {
								// convert to OP_IN ???
							}
							break;
						}
			}
			break;
		case OP_LAND:
			if (level==0 && v.type==VT_EXPRTREE && pv->type==VT_EXPRTREE)
				return (rc=processCondition((ExprTree*)v.exprt,qv))==RC_OK?processCondition((ExprTree*)pv->exprt,qv):rc;
			break;
		case OP_LOR:
			// ???
			break;
		}
	}
	if (node!=NULL && level==0 && qv!=NULL) {
		if (qv->isMulti() && qv->type>=QRY_UNION) return RC_INVPARAM; PropDNF *dnf=NULL; size_t ldnf=0; Expr *expr,**pp;
		RC rc=Expr::compile(node,expr,ma,!qv->isMulti()?&dnf:(PropDNF**)0,&ldnf);
		if (rc!=RC_OK || dnf!=NULL && (rc=PropDNF::andP(qv->condProps,qv->lProps,dnf,ldnf,ma))!=RC_OK) {ma->free(expr); return rc;}
		if (qv->nConds==0) qv->cond=expr;
		else if (qv->nConds==1) {
			if ((pp=(Expr**)ma->malloc(sizeof(Expr*)*2))==NULL) {ma->free(expr); return RC_NORESOURCES;}
			pp[0]=qv->cond; pp[1]=expr; qv->conds=pp;
		} else {
			if ((qv->conds=(Expr**)ma->realloc(qv->conds,(qv->nConds+1)*sizeof(Expr*)))==NULL) {ma->free(expr); return RC_NORESOURCES;}
			qv->conds[qv->nConds]=expr;
		}
		qv->nConds++; if ((expr->getFlags()&EXPR_PARAMS)!=0) mode|=QRY_PARAMS;
	}
	return RC_OK;
}

RC Stmt::normalize()
{
	if (op==STMT_QUERY && top!=NULL) {
		if (top->type==QRY_SIMPLE && top->lProps==0 && ((SimpleVar*)top)->condIdx!=NULL) {
			PropertyID *pids=(PropertyID*)alloca(((SimpleVar*)top)->nCondIdx*sizeof(PropertyID));
			if (pids!=NULL) {
				unsigned npids=0;
				for (CondIdx *ci=((SimpleVar*)top)->condIdx; ci!=NULL; ci=ci->next)
					if (ci->expr==NULL) pids[npids++]=ci->ks.propID;
				if (npids!=0) PropDNF::orP(top->condProps,top->lProps,pids,npids,ma);
			}
		}
	}
	return RC_OK;
}

bool Stmt::classOK(const QVar *qv)
{
	switch (qv->type) {
	default: return false;
	case QRY_SIMPLE:
		if (((SimpleVar*)qv)->condIdx!=NULL && ((SimpleVar*)qv)->nCondIdx>0) return true;
		if (((SimpleVar*)qv)->pids!=NULL && ((SimpleVar*)qv)->nPids!=0) return true;
		if (((SimpleVar*)qv)->subq!=NULL) return  ((SimpleVar*)qv)->subq->isClassOK();		//???
		if (((SimpleVar*)qv)->nClasses>0 && ((SimpleVar*)qv)->classes!=NULL && ((SimpleVar*)qv)->classes[0].classID!=STORE_INVALID_CLASSID && ((SimpleVar*)qv)->classes[0].nParams==0) {
			Class *cls=StoreCtx::get()->classMgr->getClass(((SimpleVar*)qv)->classes[0].classID);
			if (cls!=NULL) {Stmt *cqry=cls->getQuery(); bool fOK=cqry!=NULL && cqry->isClassOK(); cls->release(); if (fOK) return true;}
		}
		break;
	case QRY_JOIN: case QRY_LEFTJOIN: case QRY_RIGHTJOIN: case QRY_OUTERJOIN: case QRY_UNION: case QRY_EXCEPT: case QRY_INTERSECT:
		return false; //(qv->mv.leftVar==NULL || classOK(qv->mv.leftVar)) && (qv->mv.rightVar==NULL || classOK(qv->mv.rightVar));
	}
	if (qv->condProps!=NULL && qv->lProps>0) return true;
	if (qv->nConds==1) {if ((qv->cond->getFlags()&EXPR_PARAMS)==0) return true;}
	else if (qv->conds!=NULL) for (unsigned i=0; i<qv->nConds; i++) if ((qv->conds[i]->getFlags()&EXPR_PARAMS)==0) return true;
	return false;
}

IStmt *Stmt::clone(STMT_OP sop) const
{
	try {
		Session *ses=Session::getSession(); 
		return ses!=NULL && !ses->getStore()->inShutdown()?clone(sop,ses):(IStmt*)0;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IStmt::clone()\n");}
	return NULL;
}

CondIdx *CondIdx::clone(MemAlloc *ma) const
{
	IndexExpr *pie=NULL;
	if (expr!=NULL) {
		IndexExpr *pi=new(expr->nProps,ma) IndexExpr; if (pi==NULL) return NULL;
		if ((pi->expr=Expr::clone(expr->expr,ma))==NULL) {ma->free(pi); return NULL;}
		if ((pi->nProps=expr->nProps)!=0) memcpy(pi->props,expr->props,pi->nProps*sizeof(PropertyID));
	}
	return new(ma) CondIdx(ks,param,ma,pie);
}

Stmt *Stmt::clone(STMT_OP sop,MemAlloc *nma,bool fClass) const
{
	Stmt *stmt=new(nma) Stmt(mode,nma,sop==STMT_OP_ALL?op:sop); if (stmt==NULL) return NULL;

	QVar **ppVar=&stmt->vars,*qv; bool fAdjust=false;
	for (const QVar *sv=vars; sv!=NULL; sv=sv->next) {
		if (sv->type<QRY_ALL_SETOP) fAdjust=true;
		if (sv->clone(nma,qv,fClass)!=RC_OK) {stmt->destroy(); return NULL;}
		*ppVar=qv; ppVar=&qv->next; stmt->nVars++; stmt->nTop++;
	}
	if (nVars==1) stmt->top=stmt->vars;
	else if (fAdjust && stmt->connectVars()!=RC_OK) {stmt->destroy(); return NULL;}
	if (orderBy!=NULL && nOrderBy>0) {
		if ((stmt->orderBy=new(nma) OrderSegQ[stmt->nOrderBy=nOrderBy])==NULL) {stmt->destroy(); return NULL;}
		memcpy(stmt->orderBy,orderBy,sizeof(OrderSegQ)*nOrderBy);
		for (unsigned i=0; i<nOrderBy; i++) 
			if ((orderBy[i].flags&ORDER_EXPR)!=0 && (stmt->orderBy[i].expr=Expr::clone(orderBy[i].expr,nma))==NULL) 
				{stmt->destroy(); return NULL;}
	}
	if (values!=NULL && nValues>0) {
		RC rc=copyV(values,nValues,stmt->values,nma); if (rc!=RC_OK) {stmt->destroy(); return NULL;}
		stmt->nValues=nValues;
	}
	return stmt;
}

RC Stmt::connectVars()
{
	SetOpVar *sv; JoinVar *jv; unsigned i; QVar *qv;
	for (qv=vars; qv!=NULL; qv=qv->next) switch (qv->type) {
	case QRY_JOIN: case QRY_LEFTJOIN: case QRY_RIGHTJOIN: case QRY_OUTERJOIN:
		for (i=0,jv=(JoinVar*)qv; i<jv->nVars; i++) {
			QVar *q=jv->vars[i].var=findVar(jv->vars[i].varID);
			if (q==NULL || q->fHasParent) return RC_CORRUPTED;
			q->fHasParent=true;
		}
		break;
	case QRY_UNION: case QRY_EXCEPT: case QRY_INTERSECT:
		for (i=0,sv=(SetOpVar*)qv; i<sv->nVars; i++) {
			QVar *q=sv->vars[i].var=findVar(sv->vars[i].varID);
			if (q==NULL || q->fHasParent) return RC_CORRUPTED;
			q->fHasParent=true;
		}
		break;
	}
	for (nTop=0,qv=vars; qv!=NULL; qv=qv->next) if (!qv->fHasParent) top=++nTop==1?qv:(QVar*)0;
	return RC_OK;
}

RC QVar::clone(QVar *cloned,bool fClass) const
{
	RC rc=RC_OK; cloned->stype=stype; cloned->dtype=dtype;
	if (name!=NULL && (cloned->name=strdup(name,cloned->ma))==NULL) rc=RC_NORESOURCES;
	else if ((cloned->nConds=nConds)==1) {
		if ((cloned->cond=Expr::clone(cond,cloned->ma))==NULL) rc=RC_NORESOURCES;
	} else if (nConds>0) {
		if ((cloned->conds=new(cloned->ma) Expr*[nConds])==NULL) rc=RC_NORESOURCES;
		else {
			memset(cloned->conds,0,nConds*sizeof(Expr*));
			for (unsigned i=0; i<nConds; i++)
				if ((cloned->conds[i]=Expr::clone(conds[i],cloned->ma))==NULL) {rc=RC_NORESOURCES; break;}
		}
	}
	if (rc==RC_OK && dscr!=NULL && nDscr!=0) {
		if ((cloned->dscr=new(cloned->ma) TDescriptor[cloned->nDscr=nDscr])==NULL) rc=RC_NORESOURCES;
		else for (unsigned i=0; i<nDscr; i++)
			if ((rc=copyV(dscr[i].vals,cloned->dscr[i].nValues=dscr[i].nValues,cloned->dscr[i].vals,cloned->ma))!=RC_OK) break;
	}
	if (rc==RC_OK && varProps!=NULL && nVarProps!=0) {
		if ((cloned->varProps=new(cloned->ma) PropertyList[cloned->nVarProps=nVarProps])==NULL) rc=RC_NORESOURCES;
		else for (unsigned i=0; i<nVarProps; i++)
			if ((cloned->varProps[i].props=new(cloned->ma) PropertyID[cloned->varProps[i].nProps=varProps[i].nProps])==NULL) {rc=RC_NORESOURCES; break;}
			else memcpy(cloned->varProps[i].props,varProps[i].props,varProps[i].nProps*sizeof(PropertyID));
	}
	if (rc==RC_OK && condProps!=NULL && (cloned->lProps=lProps)!=0) {
		if ((cloned->condProps=new(lProps,cloned->ma) PropDNF)==NULL) rc=RC_NORESOURCES;
		else memcpy(cloned->condProps,condProps,lProps);
	}
	if (!fClass) {
		if (groupBy!=NULL && nGroupBy>0) {
			if ((cloned->groupBy=new(cloned->ma) OrderSegQ[cloned->nGroupBy=nGroupBy])==NULL) rc=RC_NORESOURCES;
			else {
				memcpy(cloned->groupBy,groupBy,sizeof(OrderSegQ)*nGroupBy);
				for (unsigned i=0; i<nGroupBy; i++) if ((groupBy[i].flags&ORDER_EXPR)!=0)
					{if ((cloned->groupBy[i].expr=Expr::clone(groupBy[i].expr,cloned->ma))==NULL) {rc=RC_NORESOURCES; break;}}
			}
		}
		if ((cloned->nHavingConds=nHavingConds)==1) {
			if ((cloned->havingCond=Expr::clone(havingCond,cloned->ma))==NULL) rc=RC_NORESOURCES;
		} else if (nHavingConds>0) {
			if ((cloned->havingConds=new(cloned->ma) Expr*[nHavingConds])==NULL) rc=RC_NORESOURCES;
			else {
				memset(cloned->havingConds,0,nHavingConds*sizeof(Expr*));
				for (unsigned i=0; i<nHavingConds; i++)
					if ((cloned->havingConds[i]=Expr::clone(havingConds[i],cloned->ma))==NULL) {rc=RC_NORESOURCES; break;}
			}
		}
	}
	if (rc!=RC_OK) delete cloned;
	return rc;
}

RC SimpleVar::clone(MemAlloc *m,QVar *&res,bool fClass) const
{
	SimpleVar *cv=new(m) SimpleVar(id,m); if (cv==NULL) return RC_NORESOURCES;
	if (subq!=NULL) {
		//Stmt *qry=subq->clone(STMT_QUERY,m); if (qry==NULL) return RC_NORESOURCES;
		//return (res=new(m) SubQVar(qry,id,m))!=NULL?QVar::clone(res):(qry->destroy(),RC_NORESOURCES);
	}
	if (classes!=NULL && nClasses!=0) {
		if ((cv->classes=new(m) ClassSpec[nClasses])==NULL) {delete cv; return RC_NORESOURCES;}
		for (cv->nClasses=0; cv->nClasses<nClasses; cv->nClasses++) {
			ClassSpec &to=cv->classes[cv->nClasses],&from=classes[cv->nClasses]; RC rc;
			to.classID=from.classID; to.params=NULL; to.nParams=0;
			if (from.params!=NULL && from.nParams!=0) {
				Value *pv=new(m) Value[to.nParams=from.nParams]; to.params=pv;
				if (pv==NULL) {delete cv; return RC_NORESOURCES;}
				for (ulong j=0; j<to.nParams; j++) if ((rc=copyV(from.params[j],pv[j],m))!=RC_OK) {delete cv; return rc;}
			}
		}
	}
	CondIdx **ppCondIdx=&cv->condIdx,*ci2; cv->nCondIdx=nCondIdx;
	for (const CondIdx *ci=condIdx; ci!=NULL; ci=ci->next) {
		if ((ci2=ci->clone(m))==NULL) {delete cv; return RC_NORESOURCES;}
		*ppCondIdx=cv->lastCondIdx=ci2; ppCondIdx=&ci2->next;
	}
	if (props!=NULL && nProps>0) {
		if ((cv->props=new(m) PropertyID[cv->nProps=nProps])==NULL) {delete cv; return RC_NORESOURCES;}
		memcpy(cv->props,props,nProps*sizeof(PropertyID));
	}
	if (path!=NULL && nPathSeg!=0) {
		if ((cv->path=new(m) PathSeg[cv->nPathSeg=nPathSeg])==NULL) {delete cv; return RC_NORESOURCES;}
		memcpy(cv->path,path,nPathSeg*sizeof(PathSeg));
		for (unsigned i=0; i<nPathSeg; i++) if (cv->path[i].filter!=NULL) 
			if ((cv->path[i].filter=Expr::clone((Expr*)cv->path[i].filter,m))==NULL) {delete cv; return RC_NORESOURCES;}
	}
	if (!fClass) {
		if (pids!=NULL && nPids!=0) {
			if ((cv->pids=new(m) PID[cv->nPids=nPids])==NULL) {delete cv; return RC_NORESOURCES;}
			memcpy(cv->pids,pids,nPids*sizeof(PID));
		}
		CondFT **ppCondFT=&cv->condFT;
		for (const CondFT *ft=condFT; ft!=NULL; ft=ft->next) {
			char *s=strdup(ft->str,m); if (s==NULL) {delete cv; return RC_NORESOURCES;}
			*ppCondFT=new(ft->nPids,m) CondFT(NULL,s,ft->flags,ft->pids,ft->nPids);
			if (*ppCondFT==NULL) {delete cv; return RC_NORESOURCES;}
			ppCondFT=&(*ppCondFT)->next;
		}
	}
	return QVar::clone(res=cv,fClass);
}

RC SetOpVar::clone(MemAlloc *m,QVar *&res,bool fClass) const
{
	if (fClass) return RC_INVPARAM;
	SetOpVar *sv=new(nVars,m) SetOpVar(nVars,id,type,m); if (sv==NULL) return RC_NORESOURCES;
	for (unsigned i=0; i<nVars; i++) sv->vars[i].varID=vars[i].var->getID();
	return QVar::clone(res=sv);
}

RC JoinVar::clone(MemAlloc *m,QVar *&res,bool fClass) const
{
	if (fClass) return RC_INVPARAM;
	JoinVar *jv=new(nVars,m) JoinVar(nVars,id,type,m); if (jv==NULL) return RC_NORESOURCES;
	for (unsigned i=0; i<nVars; i++) jv->vars[i].varID=vars[i].var->getID();
	CondEJ **ppCondEJ=&jv->condEJ;
	for (const CondEJ *cndEJ=condEJ; cndEJ!=NULL; cndEJ=cndEJ->next) {
		CondEJ *condEJ=*ppCondEJ=new(m) CondEJ(cndEJ->propID1,cndEJ->propID2,cndEJ->flags);
		if (condEJ==NULL) {delete jv; return RC_NORESOURCES;}
		condEJ->next=NULL; ppCondEJ=&condEJ->next;
	}
	return QVar::clone(res=jv);
}

//--------------------------------------------------------------------------------------------

size_t QVar::serSize() const
{
	unsigned i;
	size_t len=4+1+mv_len32(nDscr)+mv_len32(nVarProps)+mv_len32(nConds)+mv_len32(nHavingConds)+mv_len32(nGroupBy)+mv_len32(lProps)+lProps;
	if (name!=NULL) len+=min(strlen(name),size_t(255));
	if (dscr!=NULL) for (unsigned i=0; i<nDscr; i++) {
		const TDescriptor& td=dscr[i]; len+=mv_len32(td.nValues);
		for (unsigned j=0; j<td.nValues; j++) len+=MVStoreKernel::serSize(td.vals[j],true);
	}
	if (varProps!=NULL) for (unsigned i=0; i<nVarProps; i++) {
		const PropertyList& pl=varProps[i]; len+=mv_len32(pl.nProps);
		for (unsigned j=0; j<pl.nProps; j++) len+=mv_len32(pl.props[j]);
	}
	if (nConds==1) len+=cond->serSize(); else for (i=0; i<nConds; i++) len+=conds[i]->serSize();
	if (nHavingConds==1) len+=havingCond->serSize(); else for (i=0; i<nHavingConds; i++) len+=havingConds[i]->serSize();
	if (groupBy!=NULL && nGroupBy!=0) for (unsigned i=0; i<nGroupBy; i++) {
		const OrderSegQ& sq=groupBy[i]; len+=mv_len16(sq.flags)+mv_len16(sq.lPref)+((sq.flags&ORDER_EXPR)!=0?sq.expr->serSize():mv_len32(sq.pid));
	}
	return len;
}

byte *QVar::serQV(byte *buf) const
{
	unsigned i;
	buf[0]=id; buf[1]=type; buf[2]=stype; buf[3]=dtype; buf=serialize(buf+4);
	size_t l=name!=NULL?min(strlen(name),size_t(255)):0;
	buf[0]=(byte)l; if (l!=0) memcpy(buf+1,name,l); buf+=l+1;
	mv_enc32(buf,nDscr);
	if (dscr!=NULL) for (unsigned i=0; i<nDscr; i++) {
		const TDescriptor& td=dscr[i]; mv_enc32(buf,td.nValues);
		for (unsigned j=0; j<td.nValues; j++) buf=MVStoreKernel::serialize(td.vals[j],buf,true);
	}
	mv_enc32(buf,nVarProps);
	if (varProps!=NULL) for (unsigned i=0; i<nVarProps; i++) {
		const PropertyList& pl=varProps[i]; mv_enc32(buf,pl.nProps);
		for (unsigned j=0; j<pl.nProps; j++) mv_enc32(buf,pl.props[j]);
	}
	mv_enc32(buf,nConds);
	if (nConds==1) buf=cond->serialize(buf);
	else for (i=0; i<nConds; i++) buf=conds[i]->serialize(buf);
	mv_enc32(buf,nHavingConds);
	if (nHavingConds==1) buf=havingCond->serialize(buf);
	else for (i=0; i<nHavingConds; i++) buf=havingConds[i]->serialize(buf);
	mv_enc32(buf,nGroupBy);
	if (groupBy!=NULL && nGroupBy!=0) for (unsigned i=0; i<nGroupBy; i++) {
		const OrderSegQ& sq=groupBy[i]; mv_enc16(buf,sq.flags); mv_enc16(buf,sq.lPref);
		if ((sq.flags&ORDER_EXPR)!=0) buf=sq.expr->serialize(buf); else mv_enc32(buf,sq.pid);
	}
	mv_enc32(buf,lProps);
	if (condProps!=NULL && lProps!=0) {memcpy(buf,condProps,lProps); buf+=lProps;}
	return buf;
}

RC QVar::deserialize(const byte *&buf,const byte *const ebuf,MemAlloc *ma,QVar *&res)
{
	res=NULL; if (ebuf-buf<4) return RC_CORRUPTED;
	QVarID id=buf[0]; byte type=buf[1],stype=buf[2],dtype=buf[3]; buf+=4; size_t l; RC rc;
	switch (type) {
	default: return RC_CORRUPTED;
	case QRY_SIMPLE: rc=SimpleVar::deserialize(buf,ebuf,id,ma,res); break;
	case QRY_JOIN: case QRY_LEFTJOIN: case QRY_RIGHTJOIN: case QRY_OUTERJOIN:
		rc=JoinVar::deserialize(buf,ebuf,id,type,ma,res); break;
	case QRY_UNION: case QRY_EXCEPT: case QRY_INTERSECT:
		rc=SetOpVar::deserialize(buf,ebuf,id,type,ma,res); break;
	}
	if (rc==RC_OK) {
		res->stype=(SelectType)stype; res->dtype=(DistinctType)dtype;
		if (buf>=ebuf) rc=RC_CORRUPTED;
		else if ((l=*buf++)!=0) {
			if (l>255 || buf+l>ebuf) rc=RC_CORRUPTED;
			else if ((res->name=new(ma) char[l+1])==NULL) rc=RC_NORESOURCES;
			else {memcpy(res->name,buf,l); res->name[l]=0; buf+=l;}
		}

		// check rc or try/catch !
		
		CHECK_dec32(buf,res->nDscr,ebuf);
		if (res->nDscr!=0) {
			if ((res->dscr=new(ma) TDescriptor[res->nDscr])==NULL) rc=RC_NORESOURCES;
			else {
				memset(res->dscr,0,res->nDscr*sizeof(TDescriptor));
				for (unsigned i=0; rc==RC_OK && i<res->nDscr; i++) {
					CHECK_dec32(buf,res->dscr[i].nValues,ebuf);
					if ((res->dscr[i].vals=new(ma) Value[res->dscr[i].nValues])==NULL) rc=RC_NORESOURCES;
					else for (unsigned j=0; j<res->dscr[i].nValues; j++) if ((rc=MVStoreKernel::deserialize(res->dscr[i].vals[j],buf,ebuf,ma,false,true))!=RC_OK) break;
				}
			}
		}
		CHECK_dec32(buf,res->nVarProps,ebuf);
		if (res->nVarProps!=0) {
			if ((res->varProps=new(ma) PropertyList[res->nVarProps])==NULL) rc=RC_NORESOURCES;
			else {
				memset(res->varProps,0,res->nVarProps*sizeof(PropertyList));
				for (unsigned i=0; rc==RC_OK && i<res->nVarProps; i++) {
					CHECK_dec32(buf,res->varProps[i].nProps,ebuf);
					if ((res->varProps[i].props=new(ma) PropertyID[res->varProps[i].nProps])==NULL) rc=RC_NORESOURCES;
					else for (unsigned j=0; j<res->varProps[i].nProps; j++) {CHECK_dec32(buf,res->varProps[i].props[j],ebuf);}
				}
			}
		}
		CHECK_dec32(buf,res->nConds,ebuf);
		if (res->nConds==1) rc=Expr::deserialize(res->cond,buf,ebuf,ma);
		else if (res->nConds>1) {
			if ((res->conds=new(ma) Expr*[res->nConds])==NULL) rc=RC_NORESOURCES;
			else for (unsigned i=0; i<res->nConds; i++)
				if ((rc=Expr::deserialize(res->conds[i],buf,ebuf,ma))!=RC_OK) break;
		}
		CHECK_dec32(buf,res->nHavingConds,ebuf);
		if (res->nHavingConds==1) rc=Expr::deserialize(res->havingCond,buf,ebuf,ma);
		else if (res->nHavingConds>1) {
			if ((res->havingConds=new(ma) Expr*[res->nHavingConds])==NULL) rc=RC_NORESOURCES;
			else for (unsigned i=0; i<res->nHavingConds; i++)
				if ((rc=Expr::deserialize(res->havingConds[i],buf,ebuf,ma))!=RC_OK) break;
		}
		CHECK_dec32(buf,res->nGroupBy,ebuf);
		if (res->nGroupBy!=0) {
			if ((res->groupBy=new(ma) OrderSegQ[res->nGroupBy])==NULL) rc=RC_NORESOURCES;
			else for (unsigned i=0; i<res->nGroupBy; i++) {
				OrderSegQ &sq=res->groupBy[i]; 
				CHECK_dec16(buf,sq.flags,ebuf); CHECK_dec16(buf,sq.lPref,ebuf);
				if ((sq.flags&ORDER_EXPR)==0) {CHECK_dec32(buf,sq.pid,ebuf);}
				else if ((rc=Expr::deserialize(sq.expr,buf,ebuf,ma))!=RC_OK) break;
			}
		}
		CHECK_dec32(buf,res->lProps,ebuf);
		if (res->lProps!=0) {
			if (buf+res->lProps>ebuf) return RC_CORRUPTED;
			if ((res->condProps=(PropDNF*)ma->malloc(res->lProps))==NULL) return RC_NORESOURCES;
			memcpy((PropDNF*)res->condProps,buf,res->lProps); buf+=res->lProps;
		}
	}
	if (rc!=RC_OK && res!=NULL) {delete res; res=NULL;}
	return rc;
}

size_t SimpleVar::serSize() const
{
	size_t len=QVar::serSize()+mv_len32(nClasses)+mv_len32(nCondIdx)+mv_len32(nPids)+mv_len32(nProps)+1;
	if (classes!=NULL) for (unsigned i=0; i<nClasses; i++) {
		const ClassSpec &cs=classes[i]; len+=mv_len32(cs.classID)+mv_len32(cs.nParams);
		for (unsigned i=0; i<cs.nParams; i++) len+=MVStoreKernel::serSize(cs.params[i]);
	}
	for (CondIdx *ci=condIdx; ci!=NULL; ci=ci->next) {
		len+=mv_len32(ci->ks.propID)+mv_len16(ci->ks.flags)+mv_len16(ci->ks.lPrefix)+2+mv_len16(ci->param);
		if (ci->expr!=NULL) {
			//...
		}
	}
	if (pids!=0) for (unsigned i=0; i<nPids; i++) len+=MVStoreKernel::serSize(pids[i]);
	uint32_t cnt=0;
	for (CondFT *cf=condFT; cf!=NULL; cf=cf->next) if (cf->str!=NULL) {
		size_t l=strlen(cf->str); cnt++; 
		len+=mv_len32(l)+l+mv_len32(cf->flags)+mv_len32(cf->nPids);
		for (unsigned i=0; i<cf->nPids; i++) len+=mv_len32(cf->pids[i]);
	}
	len+=mv_len32(cnt);
	if (props!=NULL) for (unsigned i=0; i<nProps; i++) len+=mv_len32(props[i]);
	if (path!=NULL) {
		//???
	}
	return len;
}

byte *SimpleVar::serialize(byte *buf) const
{
	mv_enc32(buf,nClasses);
	if (classes!=NULL) for (unsigned i=0; i<nClasses; i++) {
		const ClassSpec &cs=classes[i]; mv_enc32(buf,cs.classID); mv_enc32(buf,cs.nParams);
		for (unsigned i=0; i<cs.nParams; i++) buf=MVStoreKernel::serialize(cs.params[i],buf);
	}
	mv_enc32(buf,nCondIdx);
	for (CondIdx *ci=condIdx; ci!=NULL; ci=ci->next) {
		mv_enc32(buf,ci->ks.propID); mv_enc16(buf,ci->ks.flags); mv_enc16(buf,ci->ks.lPrefix);
		buf[0]=ci->ks.type; buf[1]=ci->ks.op; buf+=2;
		mv_enc16(buf,ci->param);
		if (ci->expr!=NULL) {
			//...
		}
	}
	mv_enc32(buf,nPids);
	if (pids!=0) for (unsigned i=0; i<nPids; i++) buf=MVStoreKernel::serialize(pids[i],buf);
	uint32_t cnt=0; CondFT *cf;
	for (cf=condFT; cf!=NULL; cf=cf->next) if (cf->str!=NULL) cnt++;
	mv_enc32(buf,cnt);
	for (cf=condFT; cf!=NULL; cf=cf->next) if (cf->str!=NULL) {
		size_t l=strlen(cf->str); mv_enc32(buf,l); memcpy(buf,cf->str,l); buf+=l;
		mv_enc32(buf,cf->flags); mv_enc32(buf,cf->nPids);
		for (unsigned i=0; i<cf->nPids; i++) mv_enc32(buf,cf->pids[i]);
	}
	mv_enc32(buf,nProps);
	if (props!=NULL) for (unsigned i=0; i<nProps; i++) mv_enc32(buf,props[i]);
	*buf++=fOrProps;
	if (path!=NULL) {
		//???
	}
	return buf;
}

RC SimpleVar::deserialize(const byte *&buf,const byte *const ebuf,QVarID id,MemAlloc *ma,QVar *&res)
{
	SimpleVar *cv=new(ma) SimpleVar(id,ma); res=cv; RC rc;
	CHECK_dec32(buf,cv->nClasses,ebuf);
	if (cv->nClasses!=0) {
		if ((cv->classes=new(ma) ClassSpec[cv->nClasses])==NULL) return RC_NORESOURCES;
		memset(cv->classes,0,cv->nClasses*sizeof(ClassSpec));
		for (unsigned i=0; i<cv->nClasses; i++) {
			ClassSpec &cs=cv->classes[i];
			CHECK_dec32(buf,cs.classID,ebuf); CHECK_dec32(buf,cs.nParams,ebuf);
			if (cs.nParams!=0) {
				if ((cs.params=new(ma) Value[cs.nParams])==NULL) return RC_NORESOURCES;
				memset((void*)cs.params,0,cs.nParams*sizeof(Value));
				for (unsigned j=0; j<cs.nParams; j++)
					if ((rc=MVStoreKernel::deserialize(*(Value*)&cs.params[j],buf,ebuf,ma,false))!=RC_OK) return rc;
			}
		}
	}
	CHECK_dec32(buf,cv->nCondIdx,ebuf);
	for (unsigned i=0; i<cv->nCondIdx; i++) {
		IndexSeg ks; ushort param; IndexExpr *iexp=NULL;
		CHECK_dec32(buf,ks.propID,ebuf); CHECK_dec16(buf,ks.flags,ebuf); CHECK_dec16(buf,ks.lPrefix,ebuf);
		if (buf+2>ebuf) return RC_CORRUPTED; ks.type=buf[0]; ks.op=buf[1]; buf+=2;
		CHECK_dec16(buf,param,ebuf);
		// expr????
		CondIdx *ci=new(ma) CondIdx(ks,param,ma,iexp); if (ci==NULL) return RC_NORESOURCES;
		cv->lastCondIdx=cv->condIdx==NULL?cv->condIdx=ci:cv->lastCondIdx->next=ci; 
	}
	CHECK_dec32(buf,cv->nPids,ebuf);
	if (cv->nPids!=0) {
		if ((cv->pids=new(ma) PID[cv->nPids])==NULL) return RC_NORESOURCES;
		for (unsigned i=0; i<cv->nPids; i++)
			if ((rc=MVStoreKernel::deserialize(cv->pids[i],buf,ebuf))!=RC_OK) return rc;
	}
	uint32_t cntFT=0; CHECK_dec32(buf,cntFT,ebuf); CondFT **pft=&cv->condFT,*cf;
	for (uint32_t i=0; i<cntFT; i++) {
		uint32_t l,flg,np;
		CHECK_dec32(buf,l,ebuf); if (buf+l>ebuf) return RC_CORRUPTED;
		char *str=new(ma) char[l+1]; if (str==NULL) return RC_NORESOURCES;
		memcpy(str,buf,l); str[l]='\0'; buf+=l;
		CHECK_dec32(buf,flg,ebuf); CHECK_dec32(buf,np,ebuf);
		if ((cf=new(np,ma) CondFT(NULL,str,flg,NULL,np))==NULL) return RC_NORESOURCES;
		for (unsigned i=0; i<np; i++) {CHECK_dec32(buf,flg,ebuf); cf->pids[i]=flg;}
		*pft=cf; pft=&cf->next;
	}
	CHECK_dec32(buf,cv->nProps,ebuf);
	if (cv->nProps!=0) {
		if ((cv->props=new(ma) PropertyID[cv->nProps])==NULL) return RC_NORESOURCES;
		for (unsigned i=0; i<cv->nProps; i++) CHECK_dec32(buf,cv->props[i],ebuf);
	}
	if (buf<ebuf) cv->fOrProps=*buf++!=0; else return RC_CORRUPTED;
	// path ???
	return RC_OK;
}

#if 0
size_t SubQVar::serSize() const
{
	return QVar::serSize()+subq->serSize();
}

byte *SubQVar::serialize(byte *buf) const
{
	return buf; //subq->serialize(buf,lbuf)==RC_OK?lbuf:0;
}

RC SubQVar::deserialize(const byte *&buf,const byte *const ebuf,QVarID id,MemAlloc *ma,QVar *&res)
{
	Stmt *qry=NULL;
	// get qry from buf
	return (res=new(ma) SubQVar(qry,id,ma))!=NULL?RC_OK:RC_NORESOURCES;
}
#endif

size_t SetOpVar::serSize() const
{
	return 2+1+nVars;
}

byte *SetOpVar::serialize(byte *buf) const
{
	*buf++=byte(nVars); for (unsigned i=0; i<nVars; i++) *buf++=vars[i].var->getID();
	return buf;
}

RC SetOpVar::deserialize(const byte *&buf,const byte *const ebuf,QVarID id,byte type,MemAlloc *ma,QVar *&res)
{
	assert(buf<ebuf); unsigned nv=*buf++; if (size_t(ebuf-buf)<nv) return RC_CORRUPTED;
	SetOpVar *sv=new(nv,ma) SetOpVar(nv,id,type,ma); if (sv==NULL) return RC_NORESOURCES;
	for (unsigned i=0; i<nv; i++) sv->vars[i].varID=*buf++;
	return RC_OK;
}

size_t JoinVar::serSize() const
{
	size_t len=QVar::serSize()+1+nVars; unsigned nej=0;
	for (CondEJ *ej=condEJ; ej!=NULL; ej=ej->next,++nej)
		len+=mv_len32(ej->propID1)+mv_len32(ej->propID2)+mv_len16(ej->flags);
	return len+mv_len32(nej);
}

byte *JoinVar::serialize(byte *buf) const
{ 
	*buf++=nVars; for (unsigned i=0; i<nVars; i++) *buf++=vars[i].var->getID();
	unsigned nej=0; CondEJ *ej; for (ej=condEJ; ej!=NULL; ej=ej->next) nej++;
	mv_enc32(buf,nej);
	for (ej=condEJ; ej!=NULL; ej=ej->next)
		{mv_enc32(buf,ej->propID1); mv_enc32(buf,ej->propID2); mv_enc16(buf,ej->flags);}
	return buf;
}

RC JoinVar::deserialize(const byte *&buf,const byte *const ebuf,QVarID id,byte type,MemAlloc *ma,QVar *&res)
{
	assert(buf<ebuf); unsigned nv=*buf++; if (size_t(ebuf-buf)<=nv) return RC_CORRUPTED;
	JoinVar *jv=new(nv,ma) JoinVar(nv,id,type,ma); if (jv==NULL) return RC_NORESOURCES;
	for (unsigned i=0; i<nv; i++) jv->vars[i].varID=*buf++;
	unsigned nej=*buf++; CondEJ **pej=&jv->condEJ,*ej;
	for (unsigned i=0; i<nej; i++,pej=&ej->next) {
		uint32_t pid1,pid2; uint16_t flg;
		CHECK_dec32(buf,pid1,ebuf); CHECK_dec32(buf,pid2,ebuf); CHECK_dec16(buf,flg,ebuf);
		if ((*pej=ej=new(ma) CondEJ(pid1,pid2,flg))==NULL) {delete jv; return RC_NORESOURCES;}
	}
	res=jv; return RC_OK;
}

size_t Stmt::serSize() const {
	size_t len=1+mv_len32(mode)+mv_len32(nVars)+mv_len32(nOrderBy)+mv_len32(nValues);
	for (QVar *qv=vars; qv!=NULL; qv=qv->next) len+=qv->serSize();
	if (orderBy!=NULL && nOrderBy!=0) for (unsigned i=0; i<nOrderBy; i++) {
		const OrderSegQ& sq=orderBy[i]; len+=mv_len16(sq.flags)+mv_len16(sq.lPref)+((sq.flags&ORDER_EXPR)!=0?sq.expr->serSize():mv_len32(sq.pid));
	}
	if (values!=NULL && nValues!=0) for (unsigned i=0; i<nValues; i++) len+=MVStoreKernel::serSize(values[i],true);
	return len;
}

byte *Stmt::serialize(byte *buf) const
{
	*buf++=op; mv_enc32(buf,mode); mv_enc32(buf,nVars);
	for (QVar *qv=vars; qv!=NULL; qv=qv->next) buf=qv->serQV(buf);
	mv_enc32(buf,nOrderBy);
	if (orderBy!=NULL && nOrderBy!=0) for (unsigned i=0; i<nOrderBy; i++) {
		const OrderSegQ& sq=orderBy[i]; mv_enc16(buf,sq.flags); mv_enc16(buf,sq.lPref);
		if ((sq.flags&ORDER_EXPR)!=0) buf=sq.expr->serialize(buf); else mv_enc32(buf,sq.pid);
	}
	mv_enc32(buf,nValues);
	if (values!=NULL && nValues!=0) for (unsigned i=0; i<nValues; i++) buf=MVStoreKernel::serialize(values[i],buf,true);
	return buf;
}

RC Stmt::deserialize(Stmt *&res,const byte *&buf,const byte *const ebuf,MemAlloc *ma)
{
	STMT_OP op; ulong mode; 
	if (buf>=ebuf || (op=(STMT_OP)*buf++)<STMT_QUERY || op>=STMT_OP_ALL) return RC_CORRUPTED;
	CHECK_dec32(buf,mode,ebuf);
	Stmt *stmt=res=new(ma) Stmt(mode,ma,op);  if (stmt==NULL) return RC_NORESOURCES;
	QVar **pqv=&stmt->vars,*qv; unsigned nv; RC rc=RC_OK;
	CHECK_dec32(buf,nv,ebuf);
	for (unsigned i=0; i<nv; i++) {
		if ((rc=QVar::deserialize(buf,ebuf,ma,qv))!=RC_OK) {stmt->destroy(); res=NULL; return rc;}
		*pqv=qv; pqv=&qv->next; stmt->nVars++; stmt->nTop++;
	}
	if (stmt->nVars==1) stmt->top=stmt->vars; else rc=stmt->connectVars();
	if (rc==RC_OK) {
		CHECK_dec32(buf,stmt->nOrderBy,ebuf);
		if (stmt->nOrderBy!=0) {
			if ((stmt->orderBy=new(ma) OrderSegQ[stmt->nOrderBy])==NULL) rc=RC_NORESOURCES;
			else {
				memset(stmt->orderBy,0,sizeof(OrderSegQ)*stmt->nOrderBy);
				for (unsigned i=0; i<stmt->nOrderBy; i++) {
					OrderSegQ &sq=stmt->orderBy[i];
					CHECK_dec16(buf,sq.flags,ebuf); CHECK_dec16(buf,sq.lPref,ebuf);
					if ((sq.flags&ORDER_EXPR)==0) {CHECK_dec32(buf,sq.pid,ebuf);}
					else if ((rc=Expr::deserialize(sq.expr,buf,ebuf,ma))!=RC_OK) break;
				}
			}
		}
		if (rc==RC_OK) {
			CHECK_dec32(buf,stmt->nValues,ebuf);
			if (stmt->nValues!=0) {
				if ((stmt->values=new(ma) Value[stmt->nValues])==NULL) rc=RC_NORESOURCES;
				else {
					memset(stmt->values,0,sizeof(Value)*stmt->nValues);
					for (unsigned i=0; i<stmt->nValues; i++) if ((rc=MVStoreKernel::deserialize(stmt->values[i],buf,ebuf,ma,false,true))!=RC_OK) break;	
				}
			}
		}
	}
	if (rc!=RC_OK) {stmt->destroy(); res=NULL;}
	return rc;
}

void Stmt::destroy()
{
	try {this->~Stmt(); ma->free(this);} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IStmt::destroy()\n");}
}
