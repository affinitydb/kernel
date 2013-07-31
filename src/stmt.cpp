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

#include "maps.h"
#include "queryprc.h"
#include "parser.h"
#include "stmt.h"
#include "expr.h"

using namespace AfyKernel;

void Session::abortQuery()
{
	try {abortQ();} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::abortQuery()\n");}
}

Stmt::~Stmt()
{
	for (QVar *var=vars,*vnxt; var!=NULL; var=vnxt) {vnxt=var->next; delete var;}
	if (orderBy!=NULL) {
		for (unsigned i=0; i<nOrderBy; i++) if ((orderBy[i].flags&ORDER_EXPR)!=0) ma->free(orderBy[i].expr);
		ma->free(orderBy);
	}
	if (with.vals!=NULL) freeV((Value*)with.vals,with.nValues,ma);
	if ((mode&MODE_MANY_PINS)==0) freeV(vals,nValues,ma);
	else if (pins!=NULL) {
		for (unsigned i=0; i<nValues; i++) freeV((Value*)pins[i].vals,pins[i].nValues,ma);
		ma->free(pins);
	}
	if (into!=NULL) ma->free(into);
}

STMT_OP Stmt::getOp() const
{
	try {return op;} catch (...) {report(MSG_ERROR,"Exception in IStmt::getOp()\n"); return STMT_OP_ALL;}
}

QVar::QVar(QVarID i,byte ty,MemAlloc *m)
	: next(NULL),id(i),type(ty),stype(SEL_PINSET),qvf(0),ma(m),name(NULL),
	outs(NULL),nOuts(0),nConds(0),groupBy(NULL),nGroupBy(0),fHasParent(false)
{
	cond=having=NULL;
}

QVar::~QVar()
{
	if (name!=NULL) ma->free(name);
	if (outs!=NULL) {
		for (unsigned i=0; i<nOuts; i++) if (outs[i].vals!=NULL && outs[i].fFree) freeV((Value*)outs[i].vals,outs[i].nValues,ma);
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
		for (unsigned i=0; i<nClasses; i++) {SourceSpec& cs=classes[i]; if (cs.params!=NULL) freeV((Value*)cs.params,cs.nParams,ma);}
		ma->free(classes);
	}
	for (CondIdx *icond=condIdx,*icnext; icond!=NULL; icond=icnext) {
		if (icond->expr!=NULL) ma->free(icond->expr);
		icnext=icond->next; ma->free(icond);
	}
	for (CondFT *ftcond=condFT,*ftnext; ftcond!=NULL; ftcond=ftnext)
		{ftnext=ftcond->next; ma->free((char*)ftcond->str); ma->free(ftcond);}
	destroyPath(path,nPathSeg,ma);
}

JoinVar::~JoinVar()
{
	for (CondEJ *cej=condEJ,*cej2; cej!=NULL; cej=cej2) {cej2=cej->next; ma->free(cej);}
}

QVarID Stmt::addVariable(const SourceSpec *classes,unsigned nClasses,IExprTree *cond)
{
	try {
		// shutdown ???
		if (nVars>=255) return INVALID_QVAR_ID;
		SimpleVar *var = new(ma) SimpleVar((QVarID)nVars,ma); if (var==NULL) return INVALID_QVAR_ID;
		if (classes!=NULL && nClasses!=0) {
			var->classes=new(ma) SourceSpec[nClasses];
			if (var->classes==NULL) {delete var; return INVALID_QVAR_ID;}
			for (var->nClasses=0; var->nClasses<nClasses; var->nClasses++) {
				SourceSpec &to=var->classes[var->nClasses]; const SourceSpec &from=classes[var->nClasses];
				to.objectID=from.objectID; to.params=NULL; to.nParams=0;
				if (from.params!=NULL && from.nParams!=0) {
					Value *pv=new(ma) Value[to.nParams=from.nParams]; 
					if (pv==NULL) {delete var; return INVALID_QVAR_ID;}
					for (unsigned j=0; j<to.nParams; j++) {
						setHT(from.params[j]);
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
		SimpleVar *var; if (nVars>=255 || (var=new(ma) SimpleVar((QVarID)nVars,ma))==NULL) return INVALID_QVAR_ID;
		var->expr.set(pid); if ((var->path=new(ma) PathSeg)==NULL) return RC_NORESOURCES;
		memset(var->path,0,sizeof(PathSeg)); var->path[0].pid=propID; var->path[0].nPids=1;
		var->path[0].eid=STORE_COLLECTION_ID; var->path[0].cid=STORE_INVALID_CLASSID;
		var->path[0].rmin=var->path[0].rmax=1; var->nPathSeg=1;
		if (cond!=NULL && processCondition((ExprTree*)cond,var)!=RC_OK) {delete var; return INVALID_QVAR_ID;}
		var->next=vars; vars=var; nVars++; top=++nTop==1?var:(QVar*)0; return byte(var->id);
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IStmt::addVariable(collection)\n");}
	return INVALID_QVAR_ID;
}

QVarID Stmt::addVariable(IStmt *qry)
{
	try {
		// shutdown ???
		SimpleVar *var; Stmt *q;
		if (nVars>=255 || (var=new(ma) SimpleVar((QVarID)nVars,ma))==NULL || (q=((Stmt*)qry)->clone(STMT_OP_ALL,ma))==NULL) return INVALID_QVAR_ID;
		var->expr.set(q,1); var->next=vars; vars=var; nVars++; top=++nTop==1?var:(QVar*)0; return byte(var->id);
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
		qv->qvf=qv->qvf&~(QVF_ALL|QVF_DISTINCT)|(dt==DT_ALL?QVF_ALL:dt==DT_DISTINCT?QVF_DISTINCT:0); return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::setDistinct()\n"); return RC_INTERNAL;}
}

void Stmt::setVarFlags(QVarID var,byte flg)
{
	QVar *qv=findVar(var); if (qv!=NULL) qv->qvf=flg;
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
	qv->outs=(ValueV*)ma->realloc(qv->outs,(qv->nOuts+1)*sizeof(ValueV),qv->nOuts*sizeof(ValueV));
	if (qv->outs==NULL) {qv->nOuts=0; return RC_NORESOURCES;}
	const bool fGroup=qv->groupBy!=NULL && qv->nGroupBy!=0;
	if (!fGroup && qv->nOuts==0 && nO==1) {
		if (os[0].type==VT_EXPRTREE && ((ExprTree*)os[0].exprt)->op==OP_COUNT && ((ExprTree*)os[0].exprt)->operands[0].type==VT_ANY) {qv->stype=SEL_COUNT; freeV(os[0]); return RC_OK;}
		if (os[0].type==VT_VARREF && (os[0].length==0 && (os[0].refV.flags&VAR_TYPE_MASK)==VAR_CTX && os[0].refV.refN==0 || os[0].length!=0 && os[0].refV.id==PROP_SPEC_SELF && (os[0].refV.flags&VAR_TYPE_MASK)==0)) {freeV(os[0]); return RC_OK;}
	}
	unsigned nConst=0,nAgg=0; StoreCtx *ctx=NULL; bool fSelf=false,fPID=false;
	for (unsigned i=0; i<nO; i++) {
		Value &vv=os[i]; RC rc; assert(vv.type!=VT_VARREF || vv.refV.flags!=0xFFFF);
		if (vv.type==VT_EXPRTREE) {
			ExprTree *et=(ExprTree*)vv.exprt; Expr *exp;
			if (et->op==OP_CAST && et->operands[0].type==VT_VARREF) {vv=et->operands[0]; vv.refV.type=(ushort)et->operands[1].ui; ma->free(et);}
			else if (et->nops==1 && et->op<OP_ALL && (SInCtx::opDscr[et->op].flags&_A)!=0 && qv->aggrs.nValues<256) {
				if ((qv->aggrs.vals=(Value*)ma->realloc((Value*)qv->aggrs.vals,(qv->aggrs.nValues+1)*sizeof(Value)))==NULL) return RC_NORESOURCES;
				Value &to=((Value*)qv->aggrs.vals)[qv->aggrs.nValues];
				if (et->operands[0].type==VT_EXPRTREE) {
					Expr *ag; if ((rc=Expr::compile((ExprTree*)et->operands[0].exprt,ag,ma,false))==RC_OK) to.set(ag,1); else return rc;
				} else if ((rc=copyV(et->operands[0],to,ma))!=RC_OK) return rc;
				to.op=et->op; //if ((et->flags&DISTINCT_OP)!=0) -> META_PROP_DISTINCT
				et->destroy(); vv.setVarRef(byte(qv->aggrs.nValues)); vv.refV.flags=VAR_AGGS;
				qv->aggrs.nValues++; qv->aggrs.fFree=true;
			} else if ((rc=Expr::compile(et,exp,ma,false,&qv->aggrs))==RC_OK) {et->destroy(); vv.expr=exp; vv.type=VT_EXPR; vv.fcalc=1;}
			else return rc;
		} else if (vv.type==VT_VARREF) {
			if (vv.length==1 && (vv.refV.flags&VAR_TYPE_MASK)==0 && vv.refV.id==PROP_SPEC_PINID) fPID=true;
			else if (vv.length==0 && (vv.refV.flags&VAR_TYPE_MASK)==VAR_CTX || vv.length!=0 && vv.refV.id==PROP_SPEC_SELF && (vv.refV.flags&VAR_TYPE_MASK)==0) fSelf=true;
		}
		if (vv.property==PROP_SPEC_ANY) {
			if (vv.type==VT_VARREF && vv.length==1) vv.property=vv.refV.id;
			else if (nO==1) vv.setPropID(PROP_SPEC_VALUE);
			else if ((rc=(ctx!=NULL?ctx:(ctx=StoreCtx::get()))->queryMgr->getCalcPropID(i,vv.property))!=RC_OK) return rc;
		}
		switch (vv.type) {
		default: nConst++; break;
		case VT_EXPR: if (vv.fcalc==0 || ((Expr*)vv.expr)->getNVars()==0) nConst++; break;
		case VT_VARREF: if ((vv.refV.flags&VAR_TYPE_MASK)==VAR_PARAM) nConst++; break;
		case VT_STMT: case VT_ARRAY: case VT_COLLECTION: case VT_STRUCT: case VT_REF: case VT_REFIDPROP: case VT_REFIDELT: if (vv.fcalc==0) nConst++; break;
		}
	}
	if (nO>1) {
		qsort(os,nO,sizeof(Value),cmpPropIDs);
		for (unsigned i=1; i<nO; i++) if (os[i-1].property==os[i].property) return RC_INVPARAM;
	}
	qv->outs[qv->nOuts].vals=os; qv->outs[qv->nOuts].nValues=nO; ++qv->nOuts;
	if (qv->nOuts>1) qv->stype=SEL_COMP_DERIVED;
	else if (nConst==nO) qv->stype=SEL_CONST;
	else if (nAgg==nO) qv->stype=fGroup?nO==1?SEL_VALUESET:SEL_DERIVEDSET:nO==1?SEL_VALUE:SEL_DERIVED;
	else if (qv->stype!=SEL_DERIVED) qv->stype=nO==1?fSelf?SEL_PINSET:fPID?SEL_PID:SEL_VALUESET:fSelf?SEL_AUGMENTED:SEL_DERIVEDSET;
	else if (nO==1) qv->stype=SEL_VALUE;
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

void Stmt::checkParams(const Value& v,bool fRecurs)
{
	switch (v.type) {
	case VT_VARREF:
		if ((v.refV.flags&VAR_TYPE_MASK)==VAR_PARAM) mode|=QRY_PARAMS; break;
	case VT_EXPR:
		if ((((Expr*)v.expr)->getFlags()&EXPR_PARAMS)!=0) mode|=QRY_PARAMS; break;
	case VT_STMT:
		mode|=((Stmt*)v.stmt)->mode&QRY_PARAMS; break;
	case VT_ARRAY: case VT_STRUCT:
		if (fRecurs) for (unsigned i=0; i<v.length; i++) checkParams(v.varray[i],true);
		break;
	case VT_COLLECTION:
		if (fRecurs) for (const Value *cv=v.nav->navigate(GO_FIRST); cv!=NULL; cv=v.nav->navigate(GO_NEXT)) checkParams(*cv,true);
		break;
	//case VT_MAP:
		//???
	}
}

RC Stmt::setExpr(QVarID var,const Value& v)
{
	try {
		// shutdown ???
		SimpleVar *qv=(SimpleVar*)findVar(var);
		if (qv==NULL) return RC_NOTFOUND; if (qv->type!=QRY_SIMPLE) return RC_INVOP;
		freeV(qv->expr); RC rc=copyV(v,qv->expr,ma); if (rc!=RC_OK) return rc;
		if (v.type==VT_RANGE) qv->stype=SEL_DERIVEDSET;
		else if (v.type==VT_VARREF && (v.refV.flags&VAR_TYPE_MASK)==VAR_CTX && v.length==0) qv->stype=SEL_DERIVED;
		if ((mode&QRY_PARAMS)==0) checkParams(qv->expr,true); return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::setExpr()\n"); return RC_INTERNAL;}
}

static int __cdecl cmpIDs(const void *p1,const void *p2)
{
	const Value *pv1=(const Value*)p1,*pv2=(const Value*)p2; PID id1,id2;
	if (pv1->type==VT_REFID) id1=pv1->id; else if (pv1->type==VT_REF) id1=pv1->pin->getPID(); else return -1;
	if (pv2->type==VT_REFID) id2=pv2->id; else if (pv2->type==VT_REF) id2=pv2->pin->getPID(); else return 1;
	return cmpPIDs(id1,id2);
}

RC Stmt::setPIDs(QVarID var,const PID *pids,unsigned nPids)
{
	try {
		// shutdown ???
		SimpleVar *qv=(SimpleVar*)findVar(var);
		if (qv==NULL) return RC_NOTFOUND; if (qv->type!=QRY_SIMPLE) return RC_INVOP;
		if (nPids==1) {freeV(qv->expr); qv->expr.set(*pids);}
		else {
			Value *pv=new(ma) Value[nPids]; if (pv==NULL) return RC_NORESOURCES;
			for (unsigned i=0; i<nPids; i++) pv[i].set(pids[i]);
			qsort(pv,nPids,sizeof(Value),cmpIDs);
			for (unsigned i=1; i<nPids;)
				if (pv[i-1].id!=pv[i].id) i++; else if (--nPids>i) memmove(&pv[i],&pv[i+1],(nPids-i)*sizeof(Value));
			freeV(qv->expr); qv->expr.set(pv,nPids);
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
	SimpleVar *qv=(SimpleVar*)findVar(var); RC rc;
	if (qv==NULL) return RC_NOTFOUND; if (qv->type!=QRY_SIMPLE) return RC_INVOP;
	destroyPath(qv->path,qv->nPathSeg,ma); qv->path=NULL; qv->nPathSeg=0;
	if (segs!=NULL && nSegs!=0) {
		if (!fCopy) qv->path=(PathSeg*)segs; else if ((rc=copyPath(segs,nSegs,qv->path,ma))!=RC_OK) return rc;
		qv->nPathSeg=nSegs;
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
	flags=sg.flags; var=sg.var; aggop=OP_SET; lPref=sg.lPrefix; Session *ses=NULL;
	if (sg.expr==NULL) {pid=sg.pid; return RC_OK;}
	for (const IExprTree *et=sg.expr;;) {
		uint8_t op=et->getOp(); long lstr; const Value *pv;
		switch (op) {
		default: break;
		case OP_UPPER: case OP_LOWER: flags|=ORD_NCASE; goto check_con;
		case OP_SUBSTR:
			if (ses!=NULL || (ses=Session::getSession())!=NULL) {
				EvalCtx ctx(ses);
				if (Expr::getI(et->getOperand(0),lstr,ctx)==RC_OK && lstr>=0) {
					if ((et->getNumberOfOperands()==2 || et->getNumberOfOperands()==3 && lstr==0 && 
						Expr::getI(et->getOperand(1),lstr,ctx)==RC_OK) && lstr>0) {if (lPref==0 || (uint16_t)lstr<lPref) lPref=(uint16_t)lstr; goto check_con;}
				}
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

RC Stmt::copyValues(Value *vals,unsigned nVals,unsigned& pn,DynOArrayBuf<uint64_t,uint64_t>& tids,Session *ses)
{
	TIMESTAMP ts=0ULL;
	if (vals!=NULL) for (pn=0; pn<nVals; pn++) {
		Value& v=vals[pn],*pv; RC rc; Expr *exp;
		switch (v.type) {
		case VT_EXPRTREE:
			if ((rc=Expr::compile((ExprTree*)v.exprt,exp,ma,false))!=RC_OK) return rc;
			v.expr=exp; v.type=VT_EXPR; v.fcalc=1; setHT(v,ma->getAType()); break;
		case VT_ARRAY: case VT_STRUCT:
			if ((pv=new(ma) Value[v.length])==NULL) return RC_NORESOURCES;
			memcpy(pv,v.varray,v.length*sizeof(Value)); for (unsigned i=0; i<v.length; i++) setHT(pv[i]);
			v.varray=pv; setHT(v,ma->getAType()); if ((rc=copyValues(pv,v.length,v.length,tids))!=RC_OK) return rc;
			break;
		case VT_STMT:
			if (v.fcalc!=0 && ((Stmt*)v.stmt)->op==STMT_INSERT) {
				if (op==STMT_INSERT || op==STMT_UPDATE) nNested+=((Stmt*)v.stmt)->nNested+1;
				if (((Stmt*)v.stmt)->tpid!=STORE_INVALID_PID && (rc=tids.add(((Stmt*)v.stmt)->tpid))!=RC_OK)
					return rc==RC_FALSE?RC_ALREADYEXISTS:rc;
			}
		default:
			if (v.type>=VT_STRING && (rc=copyV0(v,ma))!=RC_OK) return rc;
			break;
		}
		if ((mode&QRY_PARAMS)==0) checkParams(v);
		if (ses!=NULL && v.property<=MAX_BUILTIN_URIID && (op==STMT_INSERT||op==STMT_UPDATE) &&
			(rc=ses->checkBuiltinProp(v,ts,op==STMT_INSERT))!=RC_OK) return rc;
	}
	return RC_OK;
}

RC Stmt::setValues(const Value *values,unsigned nVals,const IntoClass *it,unsigned nIt,uint64_t tid)
{
	try {
		tpid=tid;
		if ((mode&MODE_MANY_PINS)==0) {freeV(vals,nValues,ma); vals=NULL;}
		else if (pins!=NULL) {
			for (unsigned i=0; i<nValues; i++) freeV((Value*)pins[i].vals,pins[i].nValues,ma);
			ma->free(pins); pins=NULL;
		}
		nValues=nNested=0; mode&=~MODE_MANY_PINS;
		if (into!=NULL) {ma->free((void*)into); into=NULL; nInto=0;}
		if (values!=NULL && nVals!=0) {
			Value *pv=new(ma) Value[nVals]; if (pv==NULL) return RC_NORESOURCES;
			memcpy(pv,values,nVals*sizeof(Value)); 
			Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION;
			DynOArrayBuf<uint64_t,uint64_t> tids((MemAlloc*)ses); RC rc;
			if (tid!=0 && (rc=tids.add(tid))!=RC_OK) return rc;
			if ((rc=copyValues(pv,nVals,nVals,tids,ses))!=RC_OK) return rc;
			vals=pv; nValues=nVals;
		}
		if (it!=NULL && nIt!=0) {
			if ((into=new(ma) IntoClass[nIt])==NULL) return RC_NORESOURCES;
			memcpy(into,it,nIt*sizeof(IntoClass));
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::setValues()\n"); return RC_INTERNAL;}
}

RC Stmt::countNestedNoCopy(Value *values,unsigned nVals)
{
	RC rc; Expr *exp;
	for (unsigned i=0; i<nVals; i++) {
		if (values[i].type==VT_EXPRTREE) {
			ExprTree *et=(ExprTree*)values[i].exprt; if ((rc=Expr::compile(et,exp,ma,false))!=RC_OK) return rc;
			values[i].expr=exp; values[i].type=VT_EXPR; values[i].fcalc=1; setHT(values[i],ma->getAType()); et->destroy();
		} else if (values[i].fcalc!=0) switch (values[i].type) {
		case VT_STMT:
			if (((Stmt*)values[i].stmt)->op==STMT_INSERT) nNested+=((Stmt*)values[i].stmt)->nNested+1;
			break;
		case VT_ARRAY: case VT_STRUCT:
			if ((rc=countNestedNoCopy((Value*)values[i].varray,values[i].length))!=RC_OK) return rc;
		}
		if ((mode&QRY_PARAMS)==0) checkParams(values[i]);
	}
	return RC_OK;
}

RC Stmt::setValuesNoCopy(PINDscr *pds,unsigned npds)
{
	if (pds==NULL) return RC_OK; assert(pins==NULL);
	for (unsigned i=0; i<npds; i++)
		{RC rc=countNestedNoCopy((Value*)pds[i].vals,pds[i].nValues); if (rc!=RC_OK) return rc;}
	pins=pds; nValues=npds; mode|=MODE_MANY_PINS; return RC_OK;
}

RC Stmt::setValuesNoCopy(Value *values,unsigned nVals)
{
	if (values==NULL) return RC_OK; assert(vals==NULL);
	RC rc=countNestedNoCopy(values,nVals); if (rc==RC_OK) {vals=values; nValues=nVals;}
	return RC_OK;
}

RC Stmt::getNested(const Value *pv,unsigned nV,PIN **ppins,PIN *pins,unsigned& cnt,Session *ses,PIN *parent)
{
	RC rc; Value *pp; assert(parent!=NULL && parent->fNoFree==0);
	for (unsigned i=0; i<nV; i++) if (pv[i].fcalc!=0) switch (pv[i].type) {
	default: break;
	case VT_STMT:
		if (pv[i].stmt->getOp()==STMT_INSERT) {
			const unsigned cnt0=cnt; const Stmt *st=(Stmt*)pv[i].stmt;
			if ((rc=st->getNested(ppins,pins,cnt,ses,parent))!=RC_OK) return rc;
			if (cnt>cnt0) {
				Value *pp=(Value*)&pv[i]; assert(ppins[cnt0]!=NULL);
				if ((st->mode&MODE_PART)!=0) pp->meta|=META_PROP_PART;
				pp->pin=ppins[cnt0]; pp->type=VT_REF; pp->fcalc=0; setHT(*pp);
			}
		}
		break;
	case VT_ARRAY: case VT_STRUCT:
		if ((pv[i].flags&HEAP_TYPE_MASK)==NO_HEAP) {
			if ((pp=new(ses) Value[pv[i].length])==NULL) return RC_NORESOURCES;
			memcpy(pp,pv[i].varray,pv[i].length*sizeof(Value)); 
			for (unsigned j=0; j<pv[i].length; j++) setHT(pp[j]);
			((Value*)pv)[i].varray=pp; setHT(pv[i],SES_HEAP);
		}
		if ((rc=getNested(pv[i].varray,pv[i].length,ppins,pins,cnt,ses,parent))!=RC_OK) return rc;
		break;
	}
	return RC_OK;
}

RC Stmt::getNested(PIN **ppins,PIN *npins,unsigned& cnt,Session *ses,PIN *parent) const
{
	RC rc; PID tid=PIN::noPID; tid.pid=tpid; unsigned n=0;
	do {
		Value *pv=vals,*pv2; unsigned nVals=nValues; bool fNF=true;
		if ((mode&MODE_MANY_PINS)!=0) {pv=(Value*)pins[n].vals; nVals=pins[n].nValues; tid.pid=pins[n].tpid;}
		if (pv!=NULL && nVals!=0 && (nNested!=0 || (mode&MODE_PART)!=0 && parent!=NULL)) {
			if ((pv2=new(ses) Value[nVals])==NULL) return RC_NORESOURCES;
			memcpy(pv2,pv,nVals*sizeof(Value)); for (unsigned i=0; i<nVals; i++) setHT(pv[i]);
			fNF=false; pv=pv2;
		}
		if ((mode&MODE_PART)!=0 && parent!=NULL) {
			assert(cnt!=0); Value prnt; prnt.set(parent); prnt.setPropID(PROP_SPEC_PARENT); fNF=false;
			if ((rc=VBIN::insert(pv,nVals,PROP_SPEC_PARENT,prnt,(MemAlloc*)ses))!=RC_OK && rc!=RC_FALSE) return rc;
		}
		PIN *pin=ppins[cnt]=new(&npins[cnt]) PIN(ses,0,pv,nVals,fNF); *pin=tid; cnt++;
		if (nNested!=0 && (rc=getNested(pv,nVals,ppins,npins,cnt,ses,pin))!=rc) return RC_OK;
	} while ((mode&MODE_MANY_PINS)!=0 && ++n<nValues);
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
		qv->nConds++;
	}
	return RC_OK;
}

RC Stmt::processCond(ExprTree *node,QVar *qv,DynArray<const ExprTree*> *exprs)
{
	if (node!=NULL && node->nops>0) {
		const Value &v=node->operands[0],*pv=node->nops>1?&node->operands[1]:(Value*)0; CondIdx *ci; RC rc,rc2; Value *pp;
		switch (node->op) {
		default: break;
		case OP_LAND:
			assert (v.type==VT_EXPRTREE && pv->type==VT_EXPRTREE);
			if ((rc=processCond((ExprTree*)v.exprt,qv,exprs))!=RC_OK && rc!=RC_FALSE) return rc;
			if ((rc2=processCond((ExprTree*)pv->exprt,qv,exprs))!=RC_OK && rc2!=RC_FALSE || rc==rc2) return rc2;
			return (rc=(*exprs)+=(ExprTree*)(rc==RC_OK?v.exprt:pv->exprt))!=RC_OK?rc:RC_FALSE;
		case OP_EXISTS:
			if (node->nops==1 && v.type==VT_VARREF && (v.refV.flags&VAR_TYPE_MASK)==0 && v.length==1 && v.refV.refN==0 && qv->type==QRY_SIMPLE) {
				if (v.refV.id==PROP_SPEC_PINID) return RC_FALSE;
				if ((node->flags&NOT_BOOLEAN_OP)==0) return (rc=qv->addPropRefs(&v.refV.id,1))==RC_OK?RC_FALSE:rc;
			}
			break;
		case OP_IS_A:
			if (qv->type==QRY_SIMPLE && (node->flags&NOT_BOOLEAN_OP)==0) {
				SimpleVar *sv=(SimpleVar*)qv; SourceSpec *cs=(SourceSpec*)sv->classes;
				if (node->operands[1].type==VT_URIID) {
					for (unsigned i=0; ;++i,++cs)
						if (i>=sv->nClasses) {cs=NULL; break;}
						else if (cs->objectID==node->operands[1].uid) {
							if (node->nops<=2) return RC_FALSE;
							if (cs->params!=NULL && cs->nParams!=0) cs=NULL;
							break;
						}
					if (cs==NULL) {
						if ((sv->classes=(SourceSpec*)sv->ma->realloc((void*)sv->classes,(sv->nClasses+1)*sizeof(SourceSpec)))==NULL) return RC_NORESOURCES;
						cs=(SourceSpec*)&sv->classes[sv->nClasses]; cs->objectID=node->operands[1].uid; cs->params=NULL; cs->nParams=0; sv->nClasses++;
					}
					if (node->nops>2 && (rc=copyV(&node->operands[2],cs->nParams=node->nops-2,*(Value**)&cs->params,sv->ma))!=RC_OK) return rc;
					return RC_FALSE;
				} else {
					// family with params?
				}
			}
			break;
		case OP_EQ:
			// commutativity ?
			if (qv->type<QRY_UNION) {
				ushort flags=(node->flags&CASE_INSENSITIVE_OP)!=0?CND_NCASE:0; const Value *pv1=&v,*pv2=pv;  //???(FOR_ALL_LEFT_OP|EXISTS_LEFT_OP|FOR_ALL_RIGHT_OP|EXISTS_RIGHT_OP)
				if (pv1->type==VT_EXPRTREE && pv2->type==VT_EXPRTREE && ((ExprTree*)pv1->exprt)->op==((ExprTree*)pv2->exprt)->op && (((ExprTree*)pv1->exprt)->op==OP_UPPER || ((ExprTree*)pv1->exprt)->op==OP_LOWER))
					{flags|=CND_NCASE; pv1=&((ExprTree*)pv1->exprt)->operands[0]; pv2=&((ExprTree*)pv2->exprt)->operands[0];}
				if (pv1->type==VT_VARREF && (pv1->refV.flags&VAR_TYPE_MASK)==0) {
					// multijoin!
					if (pv1->refV.refN<2 && pv2->type==VT_VARREF && (pv2->refV.flags&VAR_TYPE_MASK)==0 && pv2->refV.refN<2 && pv2->refV.refN!=pv1->refV.refN) {
						PropertyID pids[2]; pids[pv1->refV.refN]=pv1->length==0?PROP_SPEC_PINID:pv1->refV.id; pids[pv2->refV.refN]=pv2->length==0?PROP_SPEC_PINID:pv2->refV.id;
						CondEJ *cnd=new(ma) CondEJ(pids[0],pids[1],flags|CND_EQ); if (cnd!=NULL) {cnd->next=((JoinVar*)qv)->condEJ; ((JoinVar*)qv)->condEJ=cnd; return RC_FALSE;}
						break;
					}
				}
			}
			if (v.type==VT_VARREF && (v.refV.flags&VAR_TYPE_MASK)==0 && (pv->type==VT_REFID||pv->type==VT_REF)) {
				if (qv->type==QRY_SIMPLE && ((SimpleVar*)qv)->expr.isEmpty() && (v.length==0 || v.length==1 && v.refV.id==PROP_SPEC_PINID) && (pp=new(ma) Value)!=NULL)
					{pp->set(pv->type==VT_REFID?pv->id:pv->pin->getPID()); ((SimpleVar*)qv)->expr.set(pp,1); return RC_FALSE;}
				break;
			}
		case OP_IN:
			if (qv->type==QRY_SIMPLE && ((SimpleVar*)qv)->expr.isEmpty() && v.type==VT_VARREF && (v.refV.flags&VAR_TYPE_MASK)==0 && (v.length==0 || v.length==1 && v.refV.id==PROP_SPEC_PINID)) {
				unsigned i; const Value *cv; bool fIDs=false;
				switch (pv->type) {
				case VT_REF: ((SimpleVar*)qv)->expr.set(pv->pin->getPID()); return RC_FALSE;
				case VT_REFID: fIDs=true; break;
				case VT_ARRAY:
					for (i=0,fIDs=true; i<pv->length; i++) if (pv->varray[i].type!=VT_REFID && pv->varray[i].type!=VT_REF) {fIDs=false; break;}
					break;
				case VT_COLLECTION:
					for (cv=pv->nav->navigate(GO_FIRST),fIDs=true; cv!=NULL; cv=pv->nav->navigate(GO_NEXT)) if (cv->type!=VT_REFID && cv->type!=VT_REF) {fIDs=false; break;}
					break;
				case VT_STRUCT:
					fIDs=pv->varray[0].property==PROP_SPEC_REF || VBIN::find(PROP_SPEC_REF,pv->varray,pv->length)!=NULL;
					break;
				}
				if (fIDs && (rc=copyV(*pv,((SimpleVar*)qv)->expr,ma))==RC_OK) return RC_FALSE;
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
				bool fExpr=true; PropertyID pid=STORE_INVALID_URIID; ushort lPref=0; long lstr; Session *ses=NULL;
				for (const Value *vv=&v; ;vv=&((ExprTree*)vv->exprt)->operands[0]) {
					if (vv->type==VT_VARREF && (vv->refV.flags&VAR_TYPE_MASK)==0 && vv->refV.refN==0 && vv->length==1) {fExpr=false; pid=vv->refV.id; break;}
					if (vv->type==VT_EXPRTREE) {
						if (((ExprTree*)vv->exprt)->op==OP_UPPER || ((ExprTree*)vv->exprt)->op==OP_LOWER) {flags|=ORD_NCASE; continue;}
						if (ses==NULL && (ses=Session::getSession())==NULL) return RC_NOSESSION;
						if (((ExprTree*)vv->exprt)->op==OP_SUBSTR && Expr::getI(((ExprTree*)vv->exprt)->operands[1],lstr,EvalCtx(ses))==RC_OK && lstr>=0) {
							if ((((ExprTree*)vv->exprt)->nops==2 || ((ExprTree*)vv->exprt)->nops==3 && lstr==0 && 
								Expr::getI(((ExprTree*)vv->exprt)->operands[2],lstr,EvalCtx(ses))==RC_OK) && lstr>0) {if (lPref==0 || lstr<lPref) lPref=(ushort)lstr; continue;}
						}
					}
					lPref=0; flags&=~ORD_NCASE; break;
				}
				if (fExpr) {
					// expr ???
				} else for (ci=((SimpleVar*)qv)->condIdx; ;ci=ci->next) {
					if (ci==NULL) {
						IndexSeg ks={pid,flags,lPref,(uint8_t)(pv->refV.type!=VT_ANY?(ValueType)pv->refV.type:(flags&ORD_NCASE)!=0||node->op==OP_BEGINS?VT_STRING:VT_ANY),(uint8_t)node->op};
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

RC Stmt::substitute(const Value *params,unsigned nParams,MemAlloc *ma)
{
	RC rc; unsigned i;
	for (QVar *qv=vars; qv!=NULL; qv=qv->next) if ((rc=qv->substitute(params,nParams,ma))!=RC_OK) return rc;
	if (orderBy!=NULL) for (i=0; i<nOrderBy; i++) if ((orderBy[i].flags&ORDER_EXPR)!=0 && (orderBy[i].expr->getFlags()&EXPR_PARAMS)!=0) {
		// subs
	}
	if (vals!=NULL) for (i=0; i<nValues; i++) if ((mode&MODE_MANY_PINS)!=0) {
		for (unsigned j=0; j<pins[i].nValues; j++) if ((rc=AfyKernel::substitute(*(Value*)&pins[i].vals[j],params,nParams,ma))!=RC_OK) return rc;
	} else if ((rc=AfyKernel::substitute(vals[i],params,nParams,ma))!=RC_OK) return rc;
	return RC_OK;
}

RC QVar::substitute(const Value *params,unsigned nParams,MemAlloc *ma)
{
	RC rc; unsigned i;
	if (nConds==1) {
		if ((cond->getFlags()&EXPR_PARAMS)!=0 && (rc=Expr::substitute(cond,params,nParams,ma))!=RC_OK) return rc;
	} else for (i=0; i<nConds; i++) if ((conds[i]->getFlags()&EXPR_PARAMS)!=0) {
		if ((rc=Expr::substitute(conds[i],params,nParams,ma))!=RC_OK) return rc;
	}
	if (groupBy!=NULL) for (i=0; i<nGroupBy; i++) if ((groupBy[i].flags&ORDER_EXPR)!=0 && (groupBy[i].expr->getFlags()&EXPR_PARAMS)!=0) {
		// subs
	}
	if (having!=NULL && (having->getFlags()&EXPR_PARAMS)!=0 && (rc=Expr::substitute(having,params,nParams,ma))!=RC_OK) return rc;
	if (outs!=NULL) for (i=0; i<nOuts; i++) for (unsigned j=0; j<outs[i].nValues; j++)
		if ((rc=AfyKernel::substitute(*(Value*)&outs[i].vals[j],params,nParams,ma))!=RC_OK) return rc;
	if (aggrs.vals!=NULL) for (i=0; i<aggrs.nValues; i++) if ((rc=AfyKernel::substitute(*(Value*)&aggrs.vals[i],params,nParams,ma))!=RC_OK) return rc;
	return RC_OK;
}

RC SimpleVar::substitute(const Value *params,unsigned nParams,MemAlloc *ma)
{
	RC rc;
	if (expr.type!=VT_ANY && (rc=AfyKernel::substitute(expr,params,nParams,ma))!=RC_OK) return rc;
#if 0
	SourceSpec		*classes;
	unsigned		nClasses;
	CondIdx			*condIdx;
	unsigned		nCondIdx;
	PathSeg			*path;
	unsigned		nPathSeg;
#endif
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
		const SimpleVar *sv=(const SimpleVar*)qv;
		if (sv->condIdx!=NULL && sv->nCondIdx>0) return true;
		switch (sv->expr.type) {
		case VT_ARRAY: case VT_COLLECTION: case VT_REFID: case VT_REF: return true; break;
		case VT_STMT: if (((Stmt*)sv->expr.stmt)->top!=NULL && ((Stmt*)sv->expr.stmt)->classOK(((Stmt*)sv->expr.stmt)->top)) return true; break;
		case VT_STRUCT: if (sv->expr.varray[0].property==PROP_SPEC_REF || VBIN::find(PROP_SPEC_REF,sv->expr.varray,sv->expr.length)!=NULL) return true; break;
		}
		if (sv->nClasses>0 && sv->classes!=NULL && sv->classes[0].objectID!=STORE_INVALID_CLASSID && sv->classes[0].nParams==0) {
			Class *cls=StoreCtx::get()->classMgr->getClass(sv->classes[0].objectID);
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

Stmt *Stmt::clone(STMT_OP sop,MemAlloc *nma) const
{
	Stmt *stmt=new(nma) Stmt(mode,nma,sop!=STMT_OP_ALL?sop:op,txi); if (stmt==NULL) return NULL;

	QVar **ppVar=&stmt->vars,*qv; bool fAdjust=false;
	for (const QVar *sv=vars; sv!=NULL; sv=sv->next) {
		if (sv->type<QRY_ALL_SETOP) fAdjust=true;
		if (sv->clone(nma,qv)!=RC_OK) {stmt->destroy(); return NULL;}
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
	if (with.vals!=NULL && with.nValues!=0)
		if (copyV(with.vals,stmt->with.nValues=with.nValues,*(Value**)&stmt->with.vals,nma)!=RC_OK) {stmt->destroy(); return NULL;}
	if ((sop==STMT_OP_ALL || sop==op) && (op==STMT_INSERT || op==STMT_UPDATE)) {
		stmt->nNested=nNested; stmt->pmode=pmode; stmt->tpid=tpid;
		if (vals!=NULL && nValues!=0) {
			if ((mode&MODE_MANY_PINS)!=0) {
				if ((stmt->pins=new(stmt->ma) PINDscr[nValues])==NULL) {stmt->destroy(); return NULL;}
				for (unsigned i=0; (stmt->nValues=i)<nValues; i++) {
					if (copyV(pins[i].vals,stmt->pins[i].nValues=pins[i].nValues,*(Value**)&stmt->pins[i].vals,stmt->ma)!=RC_OK) {stmt->destroy(); return NULL;}
					stmt->pins[i].tpid=pins[i].tpid;
				}
			} else {
				if ((stmt->vals=new(stmt->ma) Value[nValues])==NULL) {stmt->destroy(); return NULL;}
				if (copyV(vals,stmt->nValues=nValues,stmt->vals,stmt->ma)!=RC_OK) {stmt->destroy(); return NULL;}
			}
		}
		if (into!=NULL && nInto!=0) {
			if ((stmt->into=new(stmt->ma) IntoClass[nInto])==NULL) {stmt->destroy(); return NULL;}
			memcpy(stmt->into,into,nInto*sizeof(IntoClass));
		}
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

RC QVar::clone(QVar *cloned) const
{
	RC rc=RC_OK; cloned->stype=stype; cloned->qvf=qvf;
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
	if (rc!=RC_OK) delete cloned;
	return rc;
}

RC SimpleVar::clone(MemAlloc *m,QVar *&res) const
{
	SimpleVar *cv=new(m) SimpleVar(id,m); if (cv==NULL) return RC_NORESOURCES;
	if (classes!=NULL && nClasses!=0) {
		if ((cv->classes=new(m) SourceSpec[nClasses])==NULL) {delete cv; return RC_NORESOURCES;}
		for (cv->nClasses=0; cv->nClasses<nClasses; cv->nClasses++) {
			SourceSpec &to=cv->classes[cv->nClasses],&from=classes[cv->nClasses]; RC rc;
			to.objectID=from.objectID; to.params=NULL; to.nParams=0;
			if (from.params!=NULL && from.nParams!=0) {
				Value *pv=new(m) Value[to.nParams=from.nParams]; to.params=pv;
				if (pv==NULL) {delete cv; return RC_NORESOURCES;}
				for (unsigned j=0; j<to.nParams; j++) if ((rc=copyV(from.params[j],pv[j],m))!=RC_OK) {delete cv; return rc;}
			}
		}
	}
	CondIdx **ppCondIdx=&cv->condIdx,*ci2; cv->nCondIdx=nCondIdx;
	for (const CondIdx *ci=condIdx; ci!=NULL; ci=ci->next) {
		if ((ci2=ci->clone(m))==NULL) {delete cv; return RC_NORESOURCES;}
		*ppCondIdx=cv->lastCondIdx=ci2; ppCondIdx=&ci2->next;
	}
	RC rc=copyV(expr,cv->expr,m); if (rc!=RC_OK) {delete cv; return RC_NORESOURCES;}
	if (path!=NULL && nPathSeg!=0) {
		if ((rc=copyPath(path,nPathSeg,cv->path,m))!=RC_OK) {delete cv; return rc;}
		cv->nPathSeg=nPathSeg;
	}
	CondFT **ppCondFT=&cv->condFT;
	for (const CondFT *ft=condFT; ft!=NULL; ft=ft->next) {
		char *s=strdup(ft->str,m); if (s==NULL) {delete cv; return RC_NORESOURCES;}
		*ppCondFT=new(ft->nPids,m) CondFT(NULL,s,ft->flags,ft->pids,ft->nPids);
		if (*ppCondFT==NULL) {delete cv; return RC_NORESOURCES;}
		ppCondFT=&(*ppCondFT)->next;
	}
	return QVar::clone(res=cv);
}

RC SetOpVar::clone(MemAlloc *m,QVar *&res) const
{
	SetOpVar *sv=new(nVars,m) SetOpVar(nVars,id,type,m); if (sv==NULL) return RC_NORESOURCES;
	for (unsigned i=0; i<nVars; i++) sv->vars[i].varID=vars[i].var->getID();
	return QVar::clone(res=sv);
}

RC JoinVar::clone(MemAlloc *m,QVar *&res) const
{
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
	buf[0]=id; buf[1]=type; buf[2]=stype; buf[3]=qvf; buf=serialize(buf+4);
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
	QVarID id=buf[0]; byte type=buf[1],stype=buf[2],qvf=buf[3]; buf+=4; size_t l; RC rc;
	switch (type) {
	default: return RC_CORRUPTED;
	case QRY_SIMPLE: rc=SimpleVar::deserialize(buf,ebuf,id,ma,res); break;
	case QRY_SEMIJOIN: case QRY_JOIN: case QRY_LEFT_OUTER_JOIN: case QRY_RIGHT_OUTER_JOIN: case QRY_FULL_OUTER_JOIN:
		rc=JoinVar::deserialize(buf,ebuf,id,type,ma,res); break;
	case QRY_UNION: case QRY_EXCEPT: case QRY_INTERSECT:
		rc=SetOpVar::deserialize(buf,ebuf,id,type,ma,res); break;
	}
	if (rc==RC_OK) {
		res->stype=(SelectType)stype; res->qvf=qvf;
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
	size_t len=QVar::serSize()+afy_len32(nClasses)+afy_len32(nCondIdx)+1+afy_len32(nPathSeg);
	if (classes!=NULL) for (unsigned i=0; i<nClasses; i++) {
		const SourceSpec &cs=classes[i]; len+=afy_len32(cs.objectID)+afy_len32(cs.nParams);
		for (unsigned i=0; i<cs.nParams; i++) len+=AfyKernel::serSize(cs.params[i]);
	}
	for (CondIdx *ci=condIdx; ci!=NULL; ci=ci->next) {
		len+=afy_len32(ci->ks.propID)+afy_len16(ci->ks.flags)+afy_len16(ci->ks.lPrefix)+2+afy_len16(ci->param);
		if (ci->expr!=NULL) {
			//...
		}
	}
	len+=AfyKernel::serSize(expr);
	uint32_t cnt=0;
	for (CondFT *cf=condFT; cf!=NULL; cf=cf->next) if (cf->str!=NULL) {
		size_t l=strlen(cf->str); cnt++; 
		len+=afy_len32(l)+l+afy_len32(cf->flags)+afy_len32(cf->nPids);
		for (unsigned i=0; i<cf->nPids; i++) len+=afy_len32(cf->pids[i]);
	}
	len+=afy_len32(cnt);
	if (path!=NULL) for (unsigned i=0; i<nPathSeg; i++) {
		const PathSeg& ps=path[i]; len+=afy_len32(ps.nPids)+1;
		if (ps.nPids==1) len+=afy_len32(ps.pid); else for (unsigned j=0; j<ps.nPids; j++) len+=afy_len32(ps.pids[j]);
		if (ps.eid!=STORE_COLLECTION_ID) len+=afy_len32(ps.eid);
		if (ps.filter!=NULL) len+=((Expr*)ps.filter)->serSize();
		if (ps.cid!=STORE_INVALID_CLASSID) {
			len+=afy_len32(ps.cid); 
			if (ps.params!=NULL && ps.nParams!=0) {
				len+=afy_len16(ps.nParams);
				for (unsigned j=0; j<ps.nParams; j++) len+=AfyKernel::serSize(ps.params[i]);
			}
		}
		if (ps.rmin!=1) len+=afy_len16(ps.rmin); if (ps.rmax!=1) len+=afy_len16(ps.rmax);
	}
	return len;
}

byte *SimpleVar::serialize(byte *buf) const
{
	afy_enc32(buf,nClasses);
	if (classes!=NULL) for (unsigned i=0; i<nClasses; i++) {
		const SourceSpec &cs=classes[i]; afy_enc32(buf,cs.objectID); afy_enc32(buf,cs.nParams);
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
	buf=AfyKernel::serialize(expr,buf);
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
		const PathSeg& ps=path[i]; byte *pf=buf; *buf++=ps.fLast?0x80:0; afy_enc32(buf,ps.nPids);
		if (ps.nPids==1) {afy_enc32(buf,ps.pid);} else for (unsigned j=0; j<ps.nPids; j++) {afy_enc32(buf,ps.pids[j]);}
		if (ps.eid!=STORE_COLLECTION_ID) {*pf|=0x01; afy_enc32(buf,ps.eid);}
		if (ps.filter!=NULL) {*pf|=0x02; buf=((Expr*)ps.filter)->serialize(buf);}
		if (ps.cid!=STORE_INVALID_CLASSID) {
			*pf|=0x04; afy_enc32(buf,ps.cid);
			if (ps.params!=NULL && ps.nParams!=0) {
				*pf|=0x08; afy_enc16(buf,ps.nParams);
				for (unsigned j=0; j<ps.nParams; j++) buf=AfyKernel::serialize(ps.params[j],buf);
			}
		}
		if (ps.rmin!=1) {*pf|=0x10; afy_enc16(buf,ps.rmin);}
		if (ps.rmax!=1) {*pf|=0x20; afy_enc16(buf,ps.rmax);}
	}
	*buf++=fOrProps;
	return buf;
}

RC SimpleVar::deserialize(const byte *&buf,const byte *const ebuf,QVarID id,MemAlloc *ma,QVar *&res)
{
	SimpleVar *cv=new(ma) SimpleVar(id,ma); RC rc;
	CHECK_dec32(buf,cv->nClasses,ebuf);
	if (cv->nClasses!=0) {
		if ((cv->classes=new(ma) SourceSpec[cv->nClasses])==NULL) return RC_NORESOURCES;
		memset(cv->classes,0,cv->nClasses*sizeof(SourceSpec));
		for (unsigned i=0; i<cv->nClasses; i++) {
			SourceSpec &cs=cv->classes[i];
			CHECK_dec32(buf,cs.objectID,ebuf); CHECK_dec32(buf,cs.nParams,ebuf);
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
	if ((rc=AfyKernel::deserialize(cv->expr,buf,ebuf,ma,false))!=RC_OK) return rc;
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
			PathSeg& ps=cv->path[i]; if (buf>=ebuf) return RC_CORRUPTED;
			byte f=*buf++; ps.fLast=(f&0x80)!=0;
			CHECK_dec32(buf,ps.nPids,ebuf);
			if (ps.nPids==0) ps.pid=PROP_SPEC_ANY;
			else if (ps.nPids==1) {CHECK_dec32(buf,ps.pid,ebuf);}
			else if ((ps.pids=new(ma) PropertyID[ps.nPids])==NULL) return RC_NORESOURCES;
			else for (unsigned j=0; j<ps.nPids; j++) {CHECK_dec32(buf,ps.pids[j],ebuf);}
			if ((f&0x01)!=0) {CHECK_dec32(buf,ps.eid,ebuf);} else ps.eid=STORE_COLLECTION_ID;
			if ((f&0x02)!=0) {if ((rc=Expr::deserialize(*(Expr**)&ps.filter,buf,ebuf,ma))!=RC_OK) return rc;}
			if ((f&0x04)!=0) {
				CHECK_dec32(buf,ps.cid,ebuf);
				if ((f&0x08)!=0) {
					CHECK_dec16(buf,ps.nParams,ebuf); if (ps.nParams==0) return RC_CORRUPTED;
					if ((ps.params=new(ma) Value[ps.nParams])==NULL) return RC_NORESOURCES;
					memset(ps.params,0,ps.nParams*sizeof(Value));
					for (uint16_t j=0; j<ps.nParams; j++)
						if ((rc=AfyKernel::deserialize(ps.params[j],buf,ebuf,ma,false))!=RC_OK) return rc;
				}
			} else ps.cid=STORE_INVALID_CLASSID;
			if ((f&0x10)!=0) {CHECK_dec16(buf,ps.rmin,ebuf);} else ps.rmin=1;
			if ((f&0x20)!=0) {CHECK_dec16(buf,ps.rmax,ebuf);} else ps.rmax=1;
		}
	}
	if (buf<ebuf) cv->fOrProps=*buf++!=0; else return RC_CORRUPTED;
	res=cv; return RC_OK;
}

size_t SetOpVar::serSize() const
{
	return QVar::serSize()+1+nVars;
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
	res=sv; return RC_OK;
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

size_t Stmt::serSize() const
{
	size_t len=2+afy_len32(mode)+afy_len32(nVars)+afy_len32(nOrderBy);
	for (QVar *qv=vars; qv!=NULL; qv=qv->next) len+=qv->serSize();
	if (orderBy!=NULL && nOrderBy!=0) for (unsigned i=0; i<nOrderBy; i++) {
		const OrderSegQ& sq=orderBy[i]; len+=2+afy_len16(sq.flags)+afy_len32(sq.lPref)+((sq.flags&ORDER_EXPR)!=0?sq.expr->serSize():afy_len32(sq.pid));
	}
	len+=afy_len16(with.nValues);
	for (unsigned i=0; i<with.nValues; i++) len+=AfyKernel::serSize(with.vals[i],true);
	if (op==STMT_INSERT || op==STMT_UPDATE) {
		len++;
		if (vals!=NULL && nValues!=0) {
			len+=afy_len32(nValues); if (nNested!=0) len+=afy_len32(nNested);
			if ((mode&MODE_MANY_PINS)!=0) {
				for (unsigned i=0; i<nValues; i++) {
					uint32_t nV=pins[i].nValues; len+=afy_len32(nV)+afy_len64(pins[i].tpid);
					for (unsigned j=0; j<nV; j++) len+=AfyKernel::serSize(pins[i].vals[j],true);
				}
			} else {
				for (unsigned i=0; i<nValues; i++) len+=AfyKernel::serSize(vals[i],true);
			}
		}
		if (pmode!=0) len+=afy_len32(pmode); if (tpid!=STORE_INVALID_PID) len+=afy_len64(tpid);
		if (into!=NULL && nInto!=0) {
			len+=afy_len32(nInto);
			for (unsigned i=0; i<nInto; i++) {const IntoClass &cs=into[i]; len+=afy_len32(cs.cid)+afy_len32(cs.flags);}
		}
	}
	return len;
}

byte *Stmt::serialize(byte *buf) const
{
	*buf++=(byte)op; *buf++=(byte)txi; afy_enc32(buf,mode); afy_enc32(buf,nVars);
	for (QVar *qv=vars; qv!=NULL; qv=qv->next) buf=qv->serQV(buf);
	afy_enc32(buf,nOrderBy);
	if (orderBy!=NULL && nOrderBy!=0) for (unsigned i=0; i<nOrderBy; i++) {
		const OrderSegQ& sq=orderBy[i]; buf[0]=sq.var; buf[1]=sq.aggop; buf+=2;
		afy_enc16(buf,sq.flags); afy_enc32(buf,sq.lPref);
		if ((sq.flags&ORDER_EXPR)!=0) buf=sq.expr->serialize(buf); else afy_enc32(buf,sq.pid);
	}
	afy_enc16(buf,with.nValues);
	for (unsigned i=0; i<with.nValues; i++) buf=AfyKernel::serialize(with.vals[i],buf,true);
	if (op==STMT_INSERT || op==STMT_UPDATE) {
		byte f=vals==NULL||nValues==0?0x00:nNested==0?0x01:0x03; if ((mode&MODE_MANY_PINS)!=0) f|=0x80;
		if (into!=NULL && nInto!=0) f|=0x04; if (pmode!=0) f|=0x08; if (tpid!=STORE_INVALID_PID) f|=0x10;
		*buf++=f;
		if ((f&0x01)!=0) {
			afy_enc32(buf,nValues); if ((f&0x02)!=0) afy_enc32(buf,nNested);
			for (unsigned i=0; i<nValues; i++) {
				if ((mode&MODE_MANY_PINS)==0) buf=AfyKernel::serialize(vals[i],buf,true);
				else {
					uint32_t nV=pins[i].nValues; afy_enc32(buf,nV);
					for (unsigned j=0; j<nV; j++) buf=AfyKernel::serialize(pins[i].vals[j],buf,true);
					afy_enc64(buf,pins[i].tpid);
				}
			}
		}
		if ((f&0x04)!=0) {
			afy_enc32(buf,nInto);
			for (unsigned i=0; i<nInto; i++) {const IntoClass &cs=into[i]; afy_enc32(buf,cs.cid); afy_enc32(buf,cs.flags);}
		}
		if ((f&0x08)!=0) afy_enc32(buf,pmode);
		if ((f&0x10)!=0) afy_enc64(buf,tpid);
	}
	return buf;
}

RC Stmt::deserialize(Stmt *&res,const byte *&buf,const byte *const ebuf,MemAlloc *ma)
{
	STMT_OP op; unsigned mode; TXI_LEVEL txi;
	if (buf+1>=ebuf || (op=(STMT_OP)*buf++)<STMT_QUERY || op>=STMT_OP_ALL
		|| (txi=(TXI_LEVEL)*buf++)<TXI_DEFAULT || txi>=TXI_SERIALIZABLE) return RC_CORRUPTED;
	CHECK_dec32(buf,mode,ebuf);
	Stmt *stmt=res=new(ma) Stmt(mode,ma,op,txi);  if (stmt==NULL) return RC_NORESOURCES;
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
			CHECK_dec16(buf,stmt->with.nValues,ebuf);
			if (stmt->with.nValues!=0) {
				if ((stmt->with.vals=new(ma) Value[stmt->with.nValues])==NULL) rc=RC_NORESOURCES;
				else {
					memset((void*)stmt->with.vals,0,stmt->with.nValues*sizeof(Value));
					for (unsigned i=0; i<stmt->with.nValues; i++)
						if ((rc=AfyKernel::deserialize(*(Value*)&stmt->with.vals[i],buf,ebuf,ma,false,true))!=RC_OK) break;
				}
			}
			if (rc==RC_OK && (op==STMT_INSERT || op==STMT_UPDATE)) {
				if (buf>=ebuf) return RC_CORRUPTED; const byte f=*buf++;
				if ((f&0x01)!=0) {
					CHECK_dec32(buf,stmt->nValues,ebuf); if ((f&0x02)!=0) {CHECK_dec32(buf,stmt->nNested,ebuf);}
					if ((f&0x80)!=0) {
						stmt->mode|=MODE_MANY_PINS;
						if ((stmt->pins=new(ma) PINDscr[stmt->nValues])==NULL) rc=RC_NORESOURCES;
						else {
							memset(stmt->pins,0,sizeof(PINDscr)*stmt->nValues);
							for (unsigned i=0; rc==RC_OK && i<stmt->nValues; i++) {
								uint32_t nV; CHECK_dec32(buf,nV,ebuf); stmt->pins[i].nValues=nV;
								if ((stmt->pins[i].vals=new(ma) Value[nV])==NULL) {rc=RC_NORESOURCES; break;}
								memset((void*)stmt->pins[i].vals,0,sizeof(Value)*nV);
								for (unsigned j=0; j<nV; j++) 
									if ((rc=AfyKernel::deserialize(*(Value*)&stmt->pins[i].vals[j],buf,ebuf,ma,false,true))!=RC_OK) break;
								CHECK_dec64(buf,stmt->pins[i].tpid,ebuf);
							}
						}
					} else {
						if ((stmt->vals=new(ma) Value[stmt->nValues])==NULL) rc=RC_NORESOURCES;
						else {
							memset(stmt->vals,0,sizeof(Value)*stmt->nValues);
							for (unsigned i=0; i<stmt->nValues; i++) if ((rc=AfyKernel::deserialize(stmt->vals[i],buf,ebuf,ma,false,true))!=RC_OK) break;	
						}
					}
				}
				if (rc==RC_OK && (f&0x04)!=0) {
					CHECK_dec32(buf,stmt->nInto,ebuf);
					if ((stmt->into=new(ma) IntoClass[stmt->nInto])==NULL) rc=RC_NORESOURCES;
					else {
						memset(stmt->into,0,stmt->nInto*sizeof(IntoClass));
						for (unsigned i=0; rc==RC_OK && i<stmt->nInto; i++)
							{IntoClass &cs=stmt->into[i]; CHECK_dec32(buf,cs.cid,ebuf); CHECK_dec32(buf,cs.flags,ebuf);}
					}
				}
				if (rc==RC_OK && (f&0x08)!=0) {CHECK_dec32(buf,stmt->pmode,ebuf);}
				if (rc==RC_OK && (f&0x10)!=0) {CHECK_dec64(buf,stmt->tpid,ebuf);}
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
