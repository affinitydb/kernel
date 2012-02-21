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

using namespace AfyKernel;

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
	: next(NULL),id(i),type(ty),stype(SEL_PINSET),dtype(DT_DEFAULT),ma(m),name(NULL),
	outs(NULL),nOuts(0),nConds(0),groupBy(NULL),nGroupBy(0),fHasParent(false)
{
	cond=having=NULL;
}

QVar::~QVar()
{
	if (name!=NULL) ma->free(name);
	if (outs!=NULL) {
		for (ulong i=0; i<nOuts; i++) if (outs[i].vals!=NULL && outs[i].fFree) freeV((Value*)outs[i].vals,outs[i].nValues,ma);
		ma->free(outs);
	}
	if (nConds==1) ma->free(cond);
	else if (conds!=NULL) {
		for (unsigned i=0; i<nConds; i++) ma->free(conds[i]);
		ma->free(conds);
	}
	if (groupBy!=NULL) {
		for (unsigned i=0; i<nGroupBy; i++) if ((groupBy[i].flags&ORDER_EXPR)!=0) ma->free(groupBy[i].expr);
		ma->free(groupBy);
	}
	if (having!=NULL) ma->free(having);
	if (aggrs.vals!=NULL && aggrs.fFree) freeV((Value*)aggrs.vals,aggrs.nValues,ma);
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
						from.params[j].flags=NO_HEAP;
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
		var->path[0].filter=NULL; var->path[0].cid=STORE_INVALID_CLASSID;
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

RC Stmt::addOutput(QVarID var,const Value *os,unsigned nO)
{
	try {
		// shutdown?
		Value *v; RC rc=copyV(os,nO,v,ma); return rc==RC_OK?addOutputNoCopy(var,v,nO):rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::addOutput()\n"); return RC_INTERNAL;}
}

static int __cdecl cmpPropIDs(const void *v1, const void *v2)
{
	return cmp3(((const Value*)v1)->property,((const Value*)v2)->property);
}

RC Stmt::addOutputNoCopy(QVarID var,Value *os,unsigned nO)
{
	QVar *qv=findVar(var); if (qv==NULL) return RC_NOTFOUND;
	if (os==NULL || nO==0 || qv->nOuts>=255) return RC_INVPARAM;
	qv->outs=(ValueV*)ma->realloc(qv->outs,(qv->nOuts+1)*sizeof(ValueV));
	if (qv->outs==NULL) {qv->nOuts=0; return RC_NORESOURCES;}
	const bool fGroup=qv->groupBy!=NULL && qv->nGroupBy!=0;
	if (!fGroup && qv->nOuts==0 && nO==1 && os[0].type==VT_EXPRTREE && ((ExprTree*)os[0].exprt)->op==OP_COUNT && ((ExprTree*)os[0].exprt)->operands[0].type==VT_ANY) {
		qv->stype=SEL_COUNT; freeV(os[0]);
	} else {
		unsigned nConst=0,nAgg=0,nVRef=0; StoreCtx *ctx=NULL;
		for (unsigned i=0; i<nO; i++) {
			Value &vv=os[i]; RC rc; assert(vv.type!=VT_VARREF || vv.refV.flags!=0xFFFF);
			if (vv.type==VT_EXPRTREE) {
				ExprTree *et=(ExprTree*)vv.exprt; Expr *exp;
				if (et->op==OP_CAST && et->operands[0].type==VT_VARREF) {vv=et->operands[0]; vv.refV.type=(ushort)et->operands[1].ui; ma->free(et);}
				else if (et->nops==1 && et->op<OP_ALL && (SInCtx::opDscr[et->op].flags&_A)!=0 && qv->aggrs.nValues<256) {
					if ((qv->aggrs.vals=(Value*)ma->realloc((Value*)qv->aggrs.vals,(qv->aggrs.nValues+1)*sizeof(Value)))==NULL) return RC_NORESOURCES;
					Value &to=((Value*)qv->aggrs.vals)[qv->aggrs.nValues];
					if (et->operands[0].type==VT_EXPRTREE) {
						Expr *ag; if ((rc=Expr::compile((ExprTree*)et->operands[0].exprt,ag,ma,false))!=RC_OK) return rc;
						to.set(ag); to.meta|=META_PROP_EVAL;
					} else if ((rc=copyV(et->operands[0],to,ma))!=RC_OK) return rc;
					to.op=et->op; //if ((et->flags&DISTINCT_OP)!=0) -> META_PROP_DISTINCT
					et->destroy(); vv.type=VT_VARREF; vv.length=0; vv.refV.refN=byte(qv->aggrs.nValues); vv.refV.type=VT_ANY; vv.refV.flags=VAR_AGGS;
					qv->aggrs.nValues++; qv->aggrs.fFree=true;
				} else if ((rc=Expr::compile(et,exp,ma,false,&qv->aggrs))==RC_OK) {et->destroy(); vv.expr=exp; vv.type=VT_EXPR; vv.meta|=META_PROP_EVAL;}
				else return rc;
			}
			if (vv.property==PROP_SPEC_ANY) {
				if (vv.type==VT_VARREF && vv.length==1) vv.property=vv.refV.id;
				else if (nO==1) vv.property=PROP_SPEC_VALUE;
				else if ((rc=(ctx!=NULL?ctx:(ctx=StoreCtx::get()))->queryMgr->getCalcPropID(i,vv.property))!=RC_OK) return rc;
			}
			if (vv.type==VT_VARREF && (vv.refV.flags&VAR_TYPE_MASK)!=VAR_PARAM) {if (vv.length==1 && vv.refV.id==vv.property) nVRef++;}
			else if (vv.op!=OP_COUNT && (vv.type!=VT_STMT && vv.type!=VT_EXPR || (vv.meta&META_PROP_EVAL)==0)) nConst++;	// if no vars
		}
		if (nO>1) {
			qsort(os,nO,sizeof(Value),cmpPropIDs);
			for (unsigned i=1; i<nO; i++) if (os[i-1].property==os[i].property) return RC_INVPARAM;
		}
		qv->outs[qv->nOuts].vals=os; qv->outs[qv->nOuts].nValues=nO; ++qv->nOuts;
		if (qv->nOuts>1) qv->stype=SEL_COMP_DERIVED;
		else if (nConst==nO) qv->stype=SEL_CONST;
		else if (nAgg==nO) qv->stype=fGroup?nO==1?SEL_VALUESET:SEL_DERIVEDSET:nO==1?SEL_VALUE:SEL_DERIVED;
		else qv->stype=nO==1?SEL_VALUESET:nVRef==nO?SEL_PROJECTED:SEL_DERIVEDSET;
	}
	return RC_OK;
}

RC Stmt::addCondition(QVarID var,IExprTree *cond)
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
		return qv->addPropRefs(props,nProps);
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::setPropCondition()\n"); return RC_INTERNAL;}
}

RC Stmt::setJoinProperties(QVarID var,const PropertyID *props,unsigned nProps)
{
	try {
		// shutdown ???
		QVar *qv=findVar(var); if (qv==NULL) return RC_NOTFOUND; if (qv->type>QRY_FULL_OUTER_JOIN) return RC_INVOP;
		JoinVar *jv=(JoinVar*)qv; if (jv->nVars!=2) return RC_INVOP; CondEJ *ej;
		for (unsigned i=nProps; i--!=0; ) {
			if ((ej=new(ma) CondEJ(props[i],props[i],0))==NULL) return RC_NORESOURCES;
			ej->next=jv->condEJ; jv->condEJ=ej;
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::setJoinProperties()\n"); return RC_INTERNAL;}
}

RC OrderSegQ::conv(const OrderSeg& sg,MemAlloc *ma)
{
	flags=sg.flags; var=sg.var; aggop=OP_SET; lPref=sg.lPrefix; 
	if (sg.expr==NULL) {pid=sg.pid; return RC_OK;}
	for (const IExprTree *et=sg.expr;;) {
		uint8_t op=et->getOp(); long lstr; const Value *pv;
		switch (op) {
		default: break;
		case OP_UPPER: case OP_LOWER: flags|=ORD_NCASE; goto check_con;
		case OP_SUBSTR:
			if (Expr::getI(et->getOperand(0),lstr)==RC_OK && lstr>=0) {
				if ((et->getNumberOfOperands()==2 || et->getNumberOfOperands()==3 && lstr==0 && 
					Expr::getI(et->getOperand(1),lstr)==RC_OK) && lstr>0) {if (lPref==0 || (uint16_t)lstr<lPref) lPref=(uint16_t)lstr; goto check_con;}
			}
			break;
		case OP_MAX: case OP_MIN: case OP_SUM: case OP_AVG: case OP_VAR_POP: case OP_VAR_SAMP: case OP_STDDEV_POP: case OP_STDDEV_SAMP:
			if (aggop==OP_SET) aggop=op; else break;
		case OP_CON:
		check_con:
			switch ((pv=&et->getOperand(0))->type) {
			default: break;
			case VT_URIID: pid=pv->uid; return RC_OK;
			case VT_VARREF: if (pv->length==1) {pid=pv->refV.id; var=pv->refV.refN; return RC_OK;} break;
			case VT_EXPRTREE: et=pv->exprt; continue;
			}
			break;
		}
		lPref=0; aggop=OP_SET; flags=flags&~ORD_NCASE|ORDER_EXPR;
		return Expr::compile((ExprTree*)sg.expr,expr,ma,false);
	}
}

RC Stmt::setGroup(QVarID var,const OrderSeg *segs,unsigned nSegs,IExprTree *having)
{
	try {
		// shutdown ???
		RC rc; QVar *qv=findVar(var); if (qv==NULL) return RC_NOTFOUND;
		if (having!=NULL && !isBool(having->getOp())) return RC_INVPARAM;
		if (qv->groupBy!=NULL) {
			for (unsigned i=0; i<qv->nGroupBy; i++) if ((qv->groupBy[i].flags&ORDER_EXPR)!=0) qv->groupBy[i].expr->destroy();
			ma->free(qv->groupBy); qv->groupBy=NULL; qv->nGroupBy=0; if (qv->having!=NULL) {qv->having->destroy(); qv->having=NULL;}
		}
		if (segs!=NULL && nSegs!=0) {
			if ((qv->groupBy=new(ma) OrderSegQ[nSegs])==NULL) return RC_NORESOURCES;
			for (unsigned i=0; i<nSegs; i++) if ((rc=qv->groupBy[i].conv(segs[i],ma))!=RC_OK) {qv->nGroupBy=i; return rc;}
			qv->nGroupBy=nSegs;
			if (qv->stype==SEL_COUNT) qv->stype=SEL_VALUESET; else if (qv->stype==SEL_DERIVED) qv->stype=SEL_DERIVEDSET;
			if (having!=NULL && (rc=Expr::compile((ExprTree*)having,qv->having,ma,true,&qv->aggrs))!=RC_OK) return rc;
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::setGroup()\n"); return RC_INTERNAL;}
}

RC Stmt::setOrder(const OrderSeg *segs,unsigned nSegs)
{
	try {
		// shutdown ???
		RC rc;
		if (orderBy!=NULL) {
			for (unsigned i=0; i<nOrderBy; i++) if ((orderBy[i].flags&ORDER_EXPR)!=0) orderBy[i].expr->destroy();
			ma->free(orderBy); orderBy=NULL; nOrderBy=0;
		}
		if (segs!=NULL && nSegs!=0) {
			if ((orderBy=new(ma) OrderSegQ[nSegs])==NULL) return RC_NORESOURCES;
			for (unsigned i=0; i<nSegs; i++) if ((rc=orderBy[i].conv(segs[i],ma))!=RC_OK) {nOrderBy=i; return rc;}
			nOrderBy=nSegs;
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::setOrder()\n"); return RC_INTERNAL;}
}

RC Stmt::setValues(const Value *vals,unsigned nVals)
{
	try {
		RC rc=RC_OK; Expr *exp;
		if (values!=NULL) {freeV(values,nValues,ma); values=NULL; nValues=nNested=0;}
		if (vals!=NULL && nVals!=0) {
			if ((values=new(ma) Value[nVals])==NULL) rc=RC_NORESOURCES;
			else for (nValues=0; rc==RC_OK && nValues<nVals; nValues++) {
				const Value& from=vals[nValues]; Value& to=values[nValues]; unsigned j;
				if (from.type!=VT_EXPRTREE) {
					if ((from.meta&META_PROP_EVAL)!=0) switch (from.type) {
					case VT_STMT: if (((Stmt*)from.stmt)->op==STMT_INSERT) nNested+=((Stmt*)from.stmt)->nNested+1;
					case VT_EXPR: case VT_VARREF: mode|=MODE_WITH_EVAL; break;
					case VT_ARRAY:
						for (j=0; j<to.length; j++) {
							const Value *pv=&to.varray[j];
							if ((pv->meta&META_PROP_EVAL)!=0) switch (pv->type) {
							case VT_STMT: if (pv->stmt->getOp()==STMT_INSERT) nNested+=((Stmt*)pv->stmt)->nNested+1;
							case VT_EXPR: case VT_VARREF: mode|=MODE_WITH_EVAL; break;
							}
						}
						break;
					}
					rc=copyV(from,to,ma);
				} else if ((rc=Expr::compile((ExprTree*)from.exprt,exp,ma,false))==RC_OK) {
					to.set(exp); to.flags=ma->getAType(); to.setPropID(from.property); 
					to.meta=from.meta|META_PROP_EVAL; to.op=from.op; mode|=MODE_WITH_EVAL;
				}
			}
		}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::setValues()\n"); return RC_INTERNAL;}
}

RC Stmt::setValuesNoCopy(Value *vals,unsigned nVals)
{
	RC rc=RC_OK; Expr *exp;
	if (values!=NULL) {freeV(values,nValues,ma); values=NULL; nValues=nNested=0;}
	for (unsigned i=0,j; i<nVals; i++) if (vals[i].type==VT_EXPRTREE) {
		ExprTree *et=(ExprTree*)vals[i].exprt;
		if ((rc=Expr::compile(et,exp,ma,false))!=RC_OK) return rc;
		vals[i].expr=exp; vals[i].type=VT_EXPR;
		vals[i].meta|=META_PROP_EVAL; mode|=MODE_WITH_EVAL; et->destroy();
	} else if ((vals[i].meta&META_PROP_EVAL)!=0) switch (vals[i].type) {
	case VT_STMT: if (vals[i].stmt->getOp()==STMT_INSERT) nNested+=((Stmt*)vals[i].stmt)->nNested+1;
	case VT_EXPR: case VT_VARREF: mode|=MODE_WITH_EVAL; break;
	case VT_ARRAY:
		for (j=0; j<vals[i].length; j++) {
			const Value *pv=&vals[i].varray[j];
			if ((pv->meta&META_PROP_EVAL)!=0) switch (pv->type) {
			case VT_STMT: if (pv->stmt->getOp()==STMT_INSERT) nNested+=((Stmt*)pv->stmt)->nNested+1;
			case VT_EXPR: case VT_VARREF: mode|=MODE_WITH_EVAL; break;
			}
		}
		break;
	}
	if (rc==RC_OK) {values=(Value*)vals; nValues=nVals;}
	return rc;
}

RC Stmt::getNested(PIN **ppins,PIN *pins,unsigned& cnt,Session *ses,PIN *parent) const
{
	Value *pv=new(ses) Value[nValues]; if (pv==NULL) return RC_NORESOURCES; RC rc;
	unsigned nVals=nValues; memcpy(pv,values,nVals*sizeof(Value)); 
	for (unsigned i=0; i<nVals; i++) pv[i].flags=NO_HEAP;
	if ((mode&MODE_PART)!=0 && parent!=NULL) {
		assert(cnt!=0); Value prnt; prnt.set(parent); prnt.setPropID(PROP_SPEC_PARENT);
		if ((rc=BIN<Value,PropertyID,ValCmp>::insert(pv,nVals,PROP_SPEC_PARENT,prnt,ses))!=RC_OK) return rc;
	}
	PIN *pin=ppins[cnt]=new(&pins[cnt]) PIN(ses,PIN::defPID,PageAddr::invAddr,0,pv,nVals); cnt++;
	for (unsigned i=0,j,cnt0; i<nVals; i++) if ((pv[i].meta&META_PROP_EVAL)!=0) switch (pv[i].type) {
	default: break;
	case VT_STMT:
		if (pv[i].stmt->getOp()==STMT_INSERT) {
			cnt0=cnt; if ((rc=((Stmt*)pv[i].stmt)->getNested(ppins,pins,cnt,ses,pin))!=RC_OK) return rc;
			if (cnt>cnt0) {
				assert(ppins[cnt0]!=NULL); pv[i].pin=ppins[cnt0]; pv[i].type=VT_REF;
				if ((((Stmt*)pv[i].stmt)->mode&MODE_PART)!=0) pv[i].meta|=META_PROP_PART;
			}
		}
		break;
	case VT_ARRAY: case VT_STRUCT:
		for (j=0; j<pv[i].length; j++) {
			Value &vv=*(Value*)&pv[i].varray[j];
			if ((vv.meta&META_PROP_EVAL)!=0 && vv.type==VT_STMT && vv.stmt->getOp()==STMT_INSERT) {
				cnt0=cnt; if ((rc=((Stmt*)vv.stmt)->getNested(ppins,pins,cnt,ses,pin))!=RC_OK) return rc;
				if (cnt>cnt0) {
					assert(ppins[cnt0]!=NULL); vv.pin=ppins[cnt0]; vv.type=VT_REF;
					if (pv[i].type==VT_ARRAY && (((Stmt*)vv.stmt)->mode&MODE_PART)!=0) vv.meta|=META_PROP_PART;
				}
			}
		}
		break;
	}
	return RC_OK;
}

RC Stmt::processCondition(ExprTree *node,QVar *qv)
{
	DynArray<const ExprTree*> exprs(ma); RC rc=processCond(node,qv,&exprs);
	if (rc==RC_OK) exprs+=node; else if (rc==RC_FALSE) rc=RC_OK; else return rc;
	if (exprs!=0 && qv!=NULL) {
		if (qv->isMulti() && qv->type>=QRY_UNION) return RC_INVPARAM; Expr *expr,**pp;
		RC rc=Expr::compileConds(exprs,exprs,expr,ma); if (rc!=RC_OK) return rc;
		if (qv->nConds==0) qv->cond=expr;
		else if (qv->nConds==1) {
			if ((pp=(Expr**)ma->malloc(sizeof(Expr*)*2))==NULL) {ma->free(expr); return RC_NORESOURCES;}
			pp[0]=qv->cond; pp[1]=expr; qv->conds=pp;
		} else {
			if ((qv->conds=(Expr**)ma->realloc(qv->conds,(qv->nConds+1)*sizeof(Expr*)))==NULL) {ma->free(expr); return RC_NORESOURCES;}
			qv->conds[qv->nConds]=expr;
		}
		if ((expr->getFlags()&EXPR_PARAMS)!=0) mode|=QRY_PARAMS;
		if ((expr->getFlags()&EXPR_PATH)!=0) mode|=QRY_PATH;
		qv->nConds++;
	}
	return RC_OK;
}

RC Stmt::processCond(ExprTree *node,QVar *qv,DynArray<const ExprTree*> *exprs)
{
	if (node!=NULL && node->nops>0) {
		const Value &v=node->operands[0],*pv=node->nops>1?&node->operands[1]:(Value*)0; CondIdx *ci; RC rc,rc2;
		switch (node->op) {
		default: break;
		case OP_LAND:
			assert (v.type==VT_EXPRTREE && pv->type==VT_EXPRTREE);
			if ((rc=processCond((ExprTree*)v.exprt,qv,exprs))!=RC_OK && rc!=RC_FALSE) return rc;
			if ((rc2=processCond((ExprTree*)pv->exprt,qv,exprs))!=RC_OK && rc2!=RC_FALSE || rc==rc2) return rc2;
			return (rc=(*exprs)+=(ExprTree*)(rc==RC_OK?v.exprt:pv->exprt))!=RC_OK?rc:RC_FALSE;
		case OP_EXISTS:
			if (node->nops==1 && v.type==VT_VARREF && (v.refV.flags&VAR_TYPE_MASK)==0 && v.length==1 && v.refV.refN==0 && qv->type==QRY_SIMPLE) {
				if (v.refV.id==PROP_SPEC_PINID || v.refV.id==PROP_SPEC_STAMP) return RC_FALSE;
				if ((node->flags&NOT_BOOLEAN_OP)==0) return (rc=qv->addPropRefs(&v.refV.id,1))==RC_OK?RC_FALSE:rc;
			}
			break;
		case OP_IS_A:
			if (qv->type==QRY_SIMPLE && (node->flags&NOT_BOOLEAN_OP)==0 && node->operands[1].type==VT_URIID) {
				SimpleVar *sv=(SimpleVar*)qv; ClassSpec *cs=(ClassSpec*)sv->classes;
				for (unsigned i=0; ;++i,++cs)
					if (i>=sv->nClasses) {cs=NULL; break;}
					else if (cs->classID==node->operands[1].uid) {
						if (node->nops<=2) return RC_FALSE;
						if (cs->params!=NULL && cs->nParams!=0) cs=NULL;
						break;
					}
				if (cs==NULL) {
					if ((sv->classes=(ClassSpec*)sv->ma->realloc((void*)sv->classes,(sv->nClasses+1)*sizeof(ClassSpec)))==NULL) return RC_NORESOURCES;
					cs=(ClassSpec*)&sv->classes[sv->nClasses]; cs->classID=node->operands[1].uid; cs->params=NULL; cs->nParams=0; sv->nClasses++;
				}
				if (node->nops>2 && (rc=copyV(&node->operands[2],cs->nParams=node->nops-2,*(Value**)&cs->params,sv->ma))!=RC_OK) return rc;
				return RC_FALSE;
			}
			break;
		case OP_EQ:
			// commutativity ?
			if (v.type==VT_VARREF && (v.refV.flags&VAR_TYPE_MASK)==0) {
				if (pv->type==VT_REFID||pv->type==VT_REF) {
					if (qv->type==QRY_SIMPLE && ((SimpleVar*)qv)->pids==NULL && (v.length==0 || v.length==1 && v.refV.id==PROP_SPEC_PINID) && (((SimpleVar*)qv)->pids=(PID*)ma->malloc(sizeof(PID)))!=NULL)
						{((SimpleVar*)qv)->pids[((SimpleVar*)qv)->nPids++]=pv->type==VT_REFID?pv->id:pv->pin->getPID(); return RC_FALSE;}
					break;
				}
				// multijoin!
				if (qv->type<QRY_UNION && v.refV.refN<2 && pv->type==VT_VARREF && (pv->refV.flags&VAR_TYPE_MASK)==0 && pv->refV.refN<2 && pv->refV.refN!=v.refV.refN) {
					PropertyID pids[2]; pids[v.refV.refN]=v.length==0?PROP_SPEC_PINID:v.refV.id; pids[pv->refV.refN]=pv->length==0?PROP_SPEC_PINID:pv->refV.id;
					CondEJ *cnd=new(ma) CondEJ(pids[0],pids[1],node->flags&(FOR_ALL_LEFT_OP|EXISTS_LEFT_OP|FOR_ALL_RIGHT_OP|EXISTS_RIGHT_OP));
					if (cnd!=NULL) {cnd->next=((JoinVar*)qv)->condEJ; ((JoinVar*)qv)->condEJ=cnd; return RC_FALSE;}
					break;
				}
			}
		case OP_IN:
			if (v.type==VT_VARREF && (v.refV.flags&VAR_TYPE_MASK)==0 && qv->type==QRY_SIMPLE && (v.length==0 || v.length==1 && v.refV.id==PROP_SPEC_PINID)) {
				ulong i,l; const Value *cv; bool fIDs=true;
				switch (pv->type) {
				case VT_ARRAY:
					for (i=0,l=pv->length; i<l; i++) 
						if (pv->varray[i].type!=VT_REFID && pv->varray[i].type!=VT_REF) {fIDs=false; break;}
					if (fIDs && (((SimpleVar*)qv)->pids=(PID*)ma->malloc(l*sizeof(PID)))!=NULL) {
						for (i=0; i<l; i++) ((SimpleVar*)qv)->pids[((SimpleVar*)qv)->nPids++]=pv->varray[i].type==VT_REFID?pv->varray[i].id:pv->varray[i].pin->getPID();
						if (((SimpleVar*)qv)->nPids>1) qsort(((SimpleVar*)qv)->pids,((SimpleVar*)qv)->nPids,sizeof(PID),cmpPIDs); return RC_FALSE;
					}
					break;
				case VT_COLLECTION:
					for (cv=pv->nav->navigate(GO_FIRST),l=0; cv!=NULL; ++l,cv=pv->nav->navigate(GO_NEXT))
						if (cv->type!=VT_REFID && cv->type!=VT_REF) {fIDs=false; break;}
					if (fIDs && (((SimpleVar*)qv)->pids=(PID*)ma->malloc(l*sizeof(PID)))!=NULL) {
						for (cv=pv->nav->navigate(GO_FIRST); cv!=NULL; cv=pv->nav->navigate(GO_NEXT))
							((SimpleVar*)qv)->pids[((SimpleVar*)qv)->nPids++]=cv->type==VT_REFID?cv->id:cv->pin->getPID();
						if (((SimpleVar*)qv)->nPids>1) qsort(((SimpleVar*)qv)->pids,((SimpleVar*)qv)->nPids,sizeof(PID),cmpPIDs); return RC_FALSE;
					}
					break;
				case VT_REFID:
					if ((((SimpleVar*)qv)->pids=(PID*)ma->malloc(sizeof(PID)))!=NULL) {((SimpleVar*)qv)->pids[0]=pv->id; ((SimpleVar*)qv)->nPids=1; return RC_FALSE;}
					break;
				case VT_REF:
					if ((((SimpleVar*)qv)->pids=(PID*)ma->malloc(sizeof(PID)))!=NULL) {((SimpleVar*)qv)->pids[0]=pv->pin->getPID(); ((SimpleVar*)qv)->nPids=1; return RC_FALSE;}
					break;
				}
			}
		case OP_BEGINS:
			if ((node->flags&NOT_BOOLEAN_OP)!=0) break;			// no OP_NE !
		case OP_LT: case OP_LE: case OP_GT: case OP_GE:
			assert((node->flags&NOT_BOOLEAN_OP)==0);
			if (qv->type==QRY_SIMPLE && ((SimpleVar*)qv)->nCondIdx<255 && pv->type==VT_VARREF && (pv->refV.flags&VAR_TYPE_MASK)==VAR_PARAM) {
				ushort flags=((node->flags&CASE_INSENSITIVE_OP)!=0?ORD_NCASE:0)|(pv->refV.flags&(ORD_DESC|ORD_NCASE|ORD_NULLS_BEFORE|ORD_NULLS_AFTER));
				if (node->op==OP_GT || node->op==OP_IN && (node->flags&EXCLUDE_LBOUND_OP)!=0) flags|=SCAN_EXCLUDE_START;
				if (node->op==OP_LT || node->op==OP_IN && (node->flags&EXCLUDE_RBOUND_OP)!=0) flags|=SCAN_EXCLUDE_END;
				else if (node->op==OP_BEGINS) flags|=SCAN_PREFIX; else if (node->op==OP_EQ) flags|=SCAN_EXACT;
				bool fExpr=true; PropertyID pid=STORE_INVALID_PROPID; ushort lPref=0; long lstr;
				for (const Value *vv=&v; ;vv=&((ExprTree*)vv->exprt)->operands[0]) {
					if (vv->type==VT_VARREF && (vv->refV.flags&VAR_TYPE_MASK)==0 && vv->refV.refN==0 && vv->length==1) {fExpr=false; pid=vv->refV.id; break;}
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
				} else for (ci=((SimpleVar*)qv)->condIdx; ;ci=ci->next) {
					if (ci==NULL) {
						IndexSeg ks={pid,flags,lPref,pv->refV.type!=VT_ANY?(ValueType)pv->refV.type:(flags&ORD_NCASE)!=0||node->op==OP_BEGINS?VT_STRING:VT_ANY,node->op};
						if ((ci=new(qv->ma) CondIdx(ks,pv->refV.refN,ma,NULL))!=NULL) {
							if (((SimpleVar*)qv)->condIdx==NULL) ((SimpleVar*)qv)->condIdx=ci; else ((SimpleVar*)qv)->lastCondIdx->next=ci;
							((SimpleVar*)qv)->lastCondIdx=ci; ((SimpleVar*)qv)->nCondIdx++; return RC_FALSE;
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
			}
			break;
		}
	}
	return RC_OK;
}

RC QVar::addPropRefs(const PropertyID *props,unsigned nProps)
{
	Expr *exp=NULL,**pex=&exp;
	if (nConds!=0 && ((*(pex=nConds==1?&cond:&conds[0]))->getFlags()&EXPR_NO_CODE)==0) pex=&exp;
	RC rc=Expr::addPropRefs(pex,props,nProps,ma);
	if (rc==RC_OK && pex==&exp) {
		if (nConds==0) cond=exp;
		else if (nConds==1) {
			if ((pex=(Expr**)ma->malloc(sizeof(Expr*)*2))!=NULL) {pex[0]=cond; pex[1]=exp; conds=pex;}
			else {ma->free(exp); rc=RC_NORESOURCES;}
		} else {
			if ((conds=(Expr**)ma->realloc(conds,(nConds+1)*sizeof(Expr*)))!=NULL) conds[nConds]=exp;
			else {ma->free(exp); rc=RC_NORESOURCES;}
		}
		nConds++;
	}
	return rc;
}

RC QVar::mergeProps(PropListP& plp,bool fForce,bool fFlags) const
{
	RC rc; if (nConds==1) return cond->mergeProps(plp,fForce,fFlags);
	for (unsigned i=0; i<nConds; i++) if ((rc=conds[i]->mergeProps(plp,fForce,fFlags))!=RC_OK) return rc;
	return RC_OK;
}

RC SimpleVar::mergeProps(PropListP& plp,bool fForce,bool fFlags) const
{
	RC rc=nConds!=0?QVar::mergeProps(plp,fForce,fFlags):RC_OK;
	for (CondIdx *ci=condIdx; rc==RC_OK && ci!=NULL; ci=ci->next)
		rc=ci->expr!=NULL?ci->expr->mergeProps(plp,fForce,fFlags):plp.merge(0,&ci->ks.propID,1,fForce,fFlags);
	return rc;
}

RC 	SimpleVar::getPropDNF(PropDNF *&dnf,size_t& ldnf,MemAlloc *ma) const
{
	dnf=NULL; ldnf=0; RC rc;
	if (nConds==1) {
		if ((rc=cond->getPropDNF(0,dnf,ldnf,ma))!=RC_OK) return rc;
	} else if (nConds!=0) {
		if ((rc=conds[0]->getPropDNF(0,dnf,ldnf,ma))!=RC_OK) return rc;
		for (unsigned i=1; i<nConds; i++) {
			PropDNF *dnf2=NULL; size_t ldnf2=0;
			if ((rc=conds[i]->getPropDNF(0,dnf2,ldnf2,ma))!=RC_OK) {ma->free(dnf); dnf=NULL; return rc;}
			if ((rc=PropDNF::andP(dnf,ldnf,dnf2,ldnf2,ma))!=RC_OK) {ma->free(dnf2); ma->free(dnf); dnf=NULL; return rc;}
		}
	}
	if (condIdx!=0) {
		PropertyID *pids=(PropertyID*)alloca(nCondIdx*sizeof(PropertyID));
		if (pids!=NULL) {
			unsigned npids=0; bool fAnd=false;
			for (CondIdx *ci=condIdx; ci!=NULL; ci=ci->next) if (ci->expr==NULL) {
				if (!fAnd && (ci->ks.flags&(ORD_NULLS_BEFORE|ORD_NULLS_AFTER))==0) {npids=0; fAnd=true;}
				if (!fAnd || (ci->ks.flags&(ORD_NULLS_BEFORE|ORD_NULLS_AFTER))==0) pids[npids++]=ci->ks.propID;
			}
			if (npids!=0 && (fAnd || ldnf==0) && (rc=fAnd?PropDNF::andP(dnf,ldnf,pids,npids,ma):PropDNF::orP(dnf,ldnf,pids,npids,ma))!=RC_OK) return rc;
		}
	}
	return RC_OK;
}

bool SimpleVar::checkXPropID(PropertyID xp) const
{
	const PropertyID *props; unsigned nProps;
	if (condIdx!=NULL) {
		bool f=false;
		for (CondIdx *ci=condIdx; ci!=NULL; ci=ci->next) if (ci->expr!=NULL) {
			ci->expr->getExtRefs(0,props,nProps);
			if (props!=NULL) for (unsigned i=0; i<nProps; i++) 
				if ((props[i]&PROP_OPTIONAL)==0 && (props[i]&STORE_MAX_URIID)>xp) return false;	//???
		} else if (ci->ks.propID<=xp) {f=true; break;}
		if (!f) return false;
	}
	if (nConds==1) {
		cond->getExtRefs(0,props,nProps);
		if (props!=NULL) for (unsigned i=0; i<nProps; i++) if ((props[i]&PROP_OPTIONAL)==0 && (props[i]&STORE_MAX_URIID)>xp) return false;
	} else if (nConds>1) for (unsigned j=0; j<nConds; j++) {
		conds[j]->getExtRefs(0,props,nProps);
		if (props!=NULL) for (unsigned i=0; i<nProps; i++) if ((props[i]&PROP_OPTIONAL)==0 && (props[i]&STORE_MAX_URIID)>xp) return false;
	}
	return true;
}

bool Stmt::classOK(const QVar *qv)
{
	if (qv->type==QRY_SIMPLE) {
		if (((SimpleVar*)qv)->condIdx!=NULL && ((SimpleVar*)qv)->nCondIdx>0) return true;
		if (((SimpleVar*)qv)->pids!=NULL && ((SimpleVar*)qv)->nPids!=0) return true;
		if (((SimpleVar*)qv)->subq!=NULL) return  ((SimpleVar*)qv)->subq->isClassOK();		//???
		if (((SimpleVar*)qv)->nClasses>0 && ((SimpleVar*)qv)->classes!=NULL && ((SimpleVar*)qv)->classes[0].classID!=STORE_INVALID_CLASSID && ((SimpleVar*)qv)->classes[0].nParams==0) {
			Class *cls=StoreCtx::get()->classMgr->getClass(((SimpleVar*)qv)->classes[0].classID);
			if (cls!=NULL) {Stmt *cqry=cls->getQuery(); bool fOK=cqry!=NULL && cqry->isClassOK(); cls->release(); if (fOK) return true;}
		}
		if (qv->nConds==1) {if ((qv->cond->getFlags()&EXPR_PARAMS)==0) return true;}
		else if (qv->conds!=NULL) for (unsigned i=0; i<qv->nConds; i++) if ((qv->conds[i]->getFlags()&EXPR_PARAMS)==0) return true;
	}
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
	Expr *exp=NULL;
	if (expr!=NULL && (exp=Expr::clone(expr,ma))==NULL) return NULL;
	return new(ma) CondIdx(ks,param,ma,exp);
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
		stmt->nValues=nValues; stmt->nNested=nNested;
	}
	return stmt;
}

RC Stmt::connectVars()
{
	SetOpVar *sv; JoinVar *jv; unsigned i; QVar *qv;
	for (qv=vars; qv!=NULL; qv=qv->next) switch (qv->type) {
	case QRY_SEMIJOIN: case QRY_JOIN: case QRY_LEFT_OUTER_JOIN: case QRY_RIGHT_OUTER_JOIN: case QRY_FULL_OUTER_JOIN:
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
	if (rc==RC_OK && outs!=NULL && nOuts!=0) {
		if ((cloned->outs=new(cloned->ma) ValueV[cloned->nOuts=nOuts])==NULL) rc=RC_NORESOURCES;
		else for (unsigned i=0; i<nOuts; i++)
			if ((rc=copyV(outs[i].vals,cloned->outs[i].nValues=outs[i].nValues,*(Value**)&cloned->outs[i].vals,cloned->ma))!=RC_OK) break;
			else cloned->outs[i].fFree=true;
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
		if (having!=NULL && (cloned->having=Expr::clone(having,cloned->ma))==NULL) rc=RC_NORESOURCES;
		else if (aggrs.vals!=NULL && (rc=copyV(aggrs.vals,cloned->aggrs.nValues=aggrs.nValues,*(Value**)&cloned->aggrs.vals,cloned->ma))==RC_OK) cloned->aggrs.fFree=true;
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
	size_t len=4+1+afy_len32(nOuts)+afy_len32(nConds)+afy_len32(nGroupBy)+1+afy_len16(aggrs.nValues);
	if (name!=NULL) len+=min(strlen(name),size_t(255));
	if (outs!=NULL) for (unsigned i=0; i<nOuts; i++) {
		const ValueV& td=outs[i]; len+=afy_len16(td.nValues);
		for (unsigned j=0; j<td.nValues; j++) len+=AfyKernel::serSize(td.vals[j],true);
	}
	if (nConds==1) len+=cond->serSize(); else for (i=0; i<nConds; i++) len+=conds[i]->serSize();
	if (groupBy!=NULL && nGroupBy!=0) for (unsigned i=0; i<nGroupBy; i++)
		{const OrderSegQ& sq=groupBy[i]; len+=2+afy_len16(sq.flags)+afy_len32(sq.lPref)+((sq.flags&ORDER_EXPR)!=0?sq.expr->serSize():afy_len32(sq.pid));}
	if (having!=NULL) len+=having->serSize();
	if (aggrs.vals!=NULL) for (unsigned i=0; i<aggrs.nValues; i++) len+=AfyKernel::serSize(aggrs.vals[i],true);
	return len;
}

byte *QVar::serQV(byte *buf) const
{
	unsigned i;
	buf[0]=id; buf[1]=type; buf[2]=stype; buf[3]=dtype; buf=serialize(buf+4);
	size_t l=name!=NULL?min(strlen(name),size_t(255)):0;
	buf[0]=(byte)l; if (l!=0) memcpy(buf+1,name,l); buf+=l+1;
	afy_enc32(buf,nOuts);
	if (outs!=NULL) for (unsigned i=0; i<nOuts; i++) {
		const ValueV& td=outs[i]; afy_enc16(buf,td.nValues);
		for (unsigned j=0; j<td.nValues; j++) buf=AfyKernel::serialize(td.vals[j],buf,true);
	}
	afy_enc32(buf,nConds);
	if (nConds==1) buf=cond->serialize(buf);
	else for (i=0; i<nConds; i++) buf=conds[i]->serialize(buf);
	afy_enc32(buf,nGroupBy);
	if (groupBy!=NULL && nGroupBy!=0) for (unsigned i=0; i<nGroupBy; i++) {
		const OrderSegQ& sq=groupBy[i]; buf[0]=sq.var; buf[1]=sq.aggop; buf+=2;
		afy_enc16(buf,sq.flags); afy_enc32(buf,sq.lPref);
		if ((sq.flags&ORDER_EXPR)!=0) buf=sq.expr->serialize(buf); else afy_enc32(buf,sq.pid);
	}
	*buf++=having!=NULL?1:0; if (having!=NULL) buf=having->serialize(buf);
	afy_enc16(buf,aggrs.nValues);
	if (aggrs.vals!=NULL) for (unsigned i=0; i<aggrs.nValues; i++) buf=AfyKernel::serialize(aggrs.vals[i],buf,true);
	return buf;
}

RC QVar::deserialize(const byte *&buf,const byte *const ebuf,MemAlloc *ma,QVar *&res)
{
	res=NULL; if (ebuf-buf<4) return RC_CORRUPTED;
	QVarID id=buf[0]; byte type=buf[1],stype=buf[2],dtype=buf[3]; buf+=4; size_t l; RC rc;
	switch (type) {
	default: return RC_CORRUPTED;
	case QRY_SIMPLE: rc=SimpleVar::deserialize(buf,ebuf,id,ma,res); break;
	case QRY_SEMIJOIN: case QRY_JOIN: case QRY_LEFT_OUTER_JOIN: case QRY_RIGHT_OUTER_JOIN: case QRY_FULL_OUTER_JOIN:
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
		
		CHECK_dec32(buf,res->nOuts,ebuf);
		if (res->nOuts!=0) {
			if ((res->outs=new(ma) ValueV[res->nOuts])==NULL) rc=RC_NORESOURCES;
			else {
				memset(res->outs,0,res->nOuts*sizeof(ValueV));
				for (unsigned i=0; rc==RC_OK && i<res->nOuts; i++) {
					CHECK_dec16(buf,res->outs[i].nValues,ebuf);
					if ((res->outs[i].vals=new(ma) Value[res->outs[i].nValues])==NULL) rc=RC_NORESOURCES;
					else {
						res->outs[i].fFree=true;
						for (unsigned j=0; j<res->outs[i].nValues; j++) if ((rc=AfyKernel::deserialize(*(Value*)&res->outs[i].vals[j],buf,ebuf,ma,false,true))!=RC_OK) break;
					}
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
		CHECK_dec32(buf,res->nGroupBy,ebuf);
		if (res->nGroupBy!=0) {
			if ((res->groupBy=new(ma) OrderSegQ[res->nGroupBy])==NULL) rc=RC_NORESOURCES;
			else for (unsigned i=0; i<res->nGroupBy; i++) {
				OrderSegQ &sq=res->groupBy[i]; if (ebuf-buf<2) {rc=RC_CORRUPTED; break;}
				sq.var=buf[0]; sq.aggop=buf[1]; buf+=2; 
				CHECK_dec16(buf,sq.flags,ebuf); CHECK_dec32(buf,sq.lPref,ebuf);
				if ((sq.flags&ORDER_EXPR)==0) {CHECK_dec32(buf,sq.pid,ebuf);}
				else if ((rc=Expr::deserialize(sq.expr,buf,ebuf,ma))!=RC_OK) break;
			}
		}
		rc=buf>=ebuf?RC_CORRUPTED:*buf++!=0?Expr::deserialize(res->having,buf,ebuf,ma):RC_OK;
		CHECK_dec16(buf,res->aggrs.nValues,ebuf);
		if (res->aggrs.nValues!=0) {
			if ((res->aggrs.vals=new(ma) Value[res->aggrs.nValues])==NULL) rc=RC_NORESOURCES;
			else {
				res->aggrs.fFree=true;
				for (unsigned j=0; j<res->aggrs.nValues; j++) if ((rc=AfyKernel::deserialize(*(Value*)&res->aggrs.vals[j],buf,ebuf,ma,false,true))!=RC_OK) break;
			}
		}
	}
	if (rc!=RC_OK && res!=NULL) {delete res; res=NULL;}
	return rc;
}

size_t SimpleVar::serSize() const
{
	size_t len=QVar::serSize()+afy_len32(nClasses)+afy_len32(nCondIdx)+afy_len32(nPids)+1+afy_len32(nPathSeg);
	if (classes!=NULL) for (unsigned i=0; i<nClasses; i++) {
		const ClassSpec &cs=classes[i]; len+=afy_len32(cs.classID)+afy_len32(cs.nParams);
		for (unsigned i=0; i<cs.nParams; i++) len+=AfyKernel::serSize(cs.params[i]);
	}
	for (CondIdx *ci=condIdx; ci!=NULL; ci=ci->next) {
		len+=afy_len32(ci->ks.propID)+afy_len16(ci->ks.flags)+afy_len16(ci->ks.lPrefix)+2+afy_len16(ci->param);
		if (ci->expr!=NULL) {
			//...
		}
	}
	if (pids!=0) for (unsigned i=0; i<nPids; i++) len+=AfyKernel::serSize(pids[i]);
	uint32_t cnt=0;
	for (CondFT *cf=condFT; cf!=NULL; cf=cf->next) if (cf->str!=NULL) {
		size_t l=strlen(cf->str); cnt++; 
		len+=afy_len32(l)+l+afy_len32(cf->flags)+afy_len32(cf->nPids);
		for (unsigned i=0; i<cf->nPids; i++) len+=afy_len32(cf->pids[i]);
	}
	len+=afy_len32(cnt);
	if (path!=NULL) for (unsigned i=0; i<nPathSeg; i++) {
		const PathSeg& ps=path[i];
		len+=afy_len32(ps.pid)+1;
		if (ps.eid!=STORE_COLLECTION_ID) len+=afy_len32(ps.eid);
		if (ps.filter!=NULL) len+=((Expr*)ps.filter)->serSize();
		if (ps.cid!=STORE_INVALID_CLASSID) len+=afy_len32(ps.cid);
		if (ps.rmin!=1) len+=afy_len32(ps.rmin);
		if (ps.rmax!=1) len+=afy_len32(ps.rmax);
	}
	return len;
}

byte *SimpleVar::serialize(byte *buf) const
{
	afy_enc32(buf,nClasses);
	if (classes!=NULL) for (unsigned i=0; i<nClasses; i++) {
		const ClassSpec &cs=classes[i]; afy_enc32(buf,cs.classID); afy_enc32(buf,cs.nParams);
		for (unsigned i=0; i<cs.nParams; i++) buf=AfyKernel::serialize(cs.params[i],buf);
	}
	afy_enc32(buf,nCondIdx);
	for (CondIdx *ci=condIdx; ci!=NULL; ci=ci->next) {
		afy_enc32(buf,ci->ks.propID); afy_enc16(buf,ci->ks.flags); afy_enc16(buf,ci->ks.lPrefix);
		buf[0]=ci->ks.type; buf[1]=ci->ks.op; buf+=2;
		afy_enc16(buf,ci->param);
		if (ci->expr!=NULL) {
			//...
		}
	}
	afy_enc32(buf,nPids);
	if (pids!=0) for (unsigned i=0; i<nPids; i++) buf=AfyKernel::serialize(pids[i],buf);
	uint32_t cnt=0; CondFT *cf;
	for (cf=condFT; cf!=NULL; cf=cf->next) if (cf->str!=NULL) cnt++;
	afy_enc32(buf,cnt);
	for (cf=condFT; cf!=NULL; cf=cf->next) if (cf->str!=NULL) {
		size_t l=strlen(cf->str); afy_enc32(buf,l); memcpy(buf,cf->str,l); buf+=l;
		afy_enc32(buf,cf->flags); afy_enc32(buf,cf->nPids);
		for (unsigned i=0; i<cf->nPids; i++) afy_enc32(buf,cf->pids[i]);
	}
	afy_enc32(buf,nPathSeg);
	if (path!=NULL) for (unsigned i=0; i<nPathSeg; i++) {
		const PathSeg& ps=path[i]; afy_enc32(buf,ps.pid);
		byte *pf=buf; *buf++=ps.fLast?0x80:0;
		if (ps.eid!=STORE_COLLECTION_ID) {*pf|=0x01; afy_enc32(buf,ps.eid);}
		if (ps.filter!=NULL) {*pf|=0x02; buf=((Expr*)ps.filter)->serialize(buf);}
		if (ps.cid!=STORE_INVALID_CLASSID) {*pf|=0x04; afy_enc32(buf,ps.cid);}
		if (ps.rmin!=1) {*pf|=0x08; afy_enc32(buf,ps.rmin);}
		if (ps.rmax!=1) {*pf|=0x10; afy_enc32(buf,ps.rmax);}
	}
	*buf++=fOrProps;
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
					if ((rc=AfyKernel::deserialize(*(Value*)&cs.params[j],buf,ebuf,ma,false))!=RC_OK) return rc;
			}
		}
	}
	CHECK_dec32(buf,cv->nCondIdx,ebuf);
	for (unsigned i=0; i<cv->nCondIdx; i++) {
		IndexSeg ks; ushort param; Expr *iexp=NULL;
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
			if ((rc=AfyKernel::deserialize(cv->pids[i],buf,ebuf))!=RC_OK) return rc;
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
	CHECK_dec32(buf,cv->nPathSeg,ebuf);
	if (cv->nPathSeg!=0) {
		if ((cv->path=new(ma) PathSeg[cv->nPathSeg])==NULL) return RC_NORESOURCES;
		memset(cv->path,0,cv->nPathSeg*sizeof(PathSeg));
		for (unsigned i=0; i<cv->nPathSeg; i++) {
			PathSeg& ps=cv->path[i]; CHECK_dec32(buf,ps.pid,ebuf);
			if (buf>=ebuf) return RC_CORRUPTED; byte f=*buf++;
			if ((f&0x01)!=0) {CHECK_dec32(buf,ps.eid,ebuf);}
			if ((f&0x02)!=0) {if ((rc=Expr::deserialize(*(Expr**)&ps.filter,buf,ebuf,ma))!=RC_OK) return rc;}
			if ((f&0x04)!=0) {CHECK_dec32(buf,ps.cid,ebuf);}
			if ((f&0x08)!=0) {CHECK_dec32(buf,ps.rmin,ebuf);}
			if ((f&0x10)!=0) {CHECK_dec32(buf,ps.rmax,ebuf);}
			ps.fLast=(f&0x80)!=0;
		}
	}
	if (buf<ebuf) cv->fOrProps=*buf++!=0; else return RC_CORRUPTED;
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
		len+=afy_len32(ej->propID1)+afy_len32(ej->propID2)+afy_len16(ej->flags);
	return len+afy_len32(nej);
}

byte *JoinVar::serialize(byte *buf) const
{ 
	*buf++=nVars; for (unsigned i=0; i<nVars; i++) *buf++=vars[i].var->getID();
	unsigned nej=0; CondEJ *ej; for (ej=condEJ; ej!=NULL; ej=ej->next) nej++;
	afy_enc32(buf,nej);
	for (ej=condEJ; ej!=NULL; ej=ej->next)
		{afy_enc32(buf,ej->propID1); afy_enc32(buf,ej->propID2); afy_enc16(buf,ej->flags);}
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
		ej->next=NULL;
	}
	res=jv; return RC_OK;
}

size_t Stmt::serSize() const {
	size_t len=1+afy_len32(mode)+afy_len32(nVars)+afy_len32(nOrderBy)+afy_len32(nValues)+afy_len32(nNested);
	for (QVar *qv=vars; qv!=NULL; qv=qv->next) len+=qv->serSize();
	if (orderBy!=NULL && nOrderBy!=0) for (unsigned i=0; i<nOrderBy; i++) {
		const OrderSegQ& sq=orderBy[i]; len+=2+afy_len16(sq.flags)+afy_len32(sq.lPref)+((sq.flags&ORDER_EXPR)!=0?sq.expr->serSize():afy_len32(sq.pid));
	}
	if (values!=NULL && nValues!=0) for (unsigned i=0; i<nValues; i++) len+=AfyKernel::serSize(values[i],true);
	return len;
}

byte *Stmt::serialize(byte *buf) const
{
	*buf++=op; afy_enc32(buf,mode); afy_enc32(buf,nVars);
	for (QVar *qv=vars; qv!=NULL; qv=qv->next) buf=qv->serQV(buf);
	afy_enc32(buf,nOrderBy);
	if (orderBy!=NULL && nOrderBy!=0) for (unsigned i=0; i<nOrderBy; i++) {
		const OrderSegQ& sq=orderBy[i]; buf[0]=sq.var; buf[1]=sq.aggop; buf+=2;
		afy_enc16(buf,sq.flags); afy_enc32(buf,sq.lPref);
		if ((sq.flags&ORDER_EXPR)!=0) buf=sq.expr->serialize(buf); else afy_enc32(buf,sq.pid);
	}
	afy_enc32(buf,nValues); afy_enc32(buf,nNested);
	if (values!=NULL && nValues!=0) for (unsigned i=0; i<nValues; i++) buf=AfyKernel::serialize(values[i],buf,true);
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
					OrderSegQ &sq=stmt->orderBy[i]; if (ebuf-buf<2) {rc=RC_CORRUPTED; break;}
					sq.var=buf[0]; sq.aggop=buf[1]; buf+=2;
					CHECK_dec16(buf,sq.flags,ebuf); CHECK_dec32(buf,sq.lPref,ebuf);
					if ((sq.flags&ORDER_EXPR)==0) {CHECK_dec32(buf,sq.pid,ebuf);}
					else if ((rc=Expr::deserialize(sq.expr,buf,ebuf,ma))!=RC_OK) break;
				}
			}
		}
		if (rc==RC_OK) {
			CHECK_dec32(buf,stmt->nValues,ebuf); CHECK_dec32(buf,stmt->nNested,ebuf);
			if (stmt->nValues!=0) {
				if ((stmt->values=new(ma) Value[stmt->nValues])==NULL) rc=RC_NORESOURCES;
				else {
					memset(stmt->values,0,sizeof(Value)*stmt->nValues);
					for (unsigned i=0; i<stmt->nValues; i++) if ((rc=AfyKernel::deserialize(stmt->values[i],buf,ebuf,ma,false,true))!=RC_OK) break;	
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
