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

#include "qbuild.h"
#include "expr.h"
#include "ftindex.h"
#include "queryop.h"
#include "stmt.h"
#include "parser.h"

using namespace AfyKernel;

QBuildCtx::QBuildCtx(Session *s,const ValueV& prs,const Stmt *st,ulong nsk,ulong md)
	: ses(s),qx(new(s) QCtx(s)),stmt(st),nSkip(nsk),mode(md),flg(0),propsReq(s),sortReq(NULL),nSortReq(0),nqs(0),ncqs(0) 
{
	if (qx==NULL) throw RC_NORESOURCES; 
	qx->ref(); qx->vals[QV_PARAMS]=prs; qx->vals[QV_PARAMS].fFree=false;
	if ((md&MODE_COPY_VALUES)!=0) {
		flg|=QO_VCOPIED;
		if (prs.vals!=NULL && prs.nValues!=0) {
			RC rc=copyV(prs.vals,prs.nValues,*(Value**)&qx->vals[QV_PARAMS].vals,s); if (rc!=RC_OK) throw rc;
			qx->vals[QV_PARAMS].fFree=true;
		}
	}
	if ((md&MODE_FOR_UPDATE)!=0) flg|=QO_FORUPDATE; if ((md&MODE_DELETED)!=0) flg|=QO_DELETED; if ((md&MODE_CLASS)!=0) flg|=QO_CLASS;
}

QBuildCtx::~QBuildCtx()
{
	for (ulong i=0; i<nqs; i++) delete src[i];
	if (qx!=NULL) qx->destroy();
}

RC QBuildCtx::process(QueryOp *&qop)
{
	qop=NULL; if (stmt==NULL||stmt->top==NULL) return RC_EOF;

	QVar *qv=stmt->top; sortReq=stmt->orderBy; nSortReq=stmt->nOrderBy;

	RC rc=qv->build(*this,qop); if (rc!=RC_OK) return rc; assert(qop!=NULL);

	if (qv->groupBy==NULL || qv->nGroupBy==0) switch (qv->dtype) {
	case DT_DISTINCT: case DT_DEFAULT:
		if ((qop->qflags&QO_UNIQUE)==0 && (qv->stype==SEL_PINSET||qv->stype==SEL_COMPOUND/*||qv->stype==SEL_PROJECTED||qv->stype==SEL_PROJECTED1*/)) {
			QueryOp *q=new(ses,0,propsReq.nPls) Sort(qop,NULL,0,flg|QO_UNIQUE,0,NULL,0/*propsReq,nPropsReq*/);
			if (q==NULL) {delete qop; qop=NULL; return RC_NORESOURCES;}
			qop=q;
		}
		break;
#if 0
	case DT_DISTINCT_VALUES:
		if (propsReq.nPls!=0 || (qop->qflags&QO_UNIQUE)==0) {
			// calc total number -> nProps
			OrderSegQ *os=(OrderSegQ*)alloca(nPropsReq*sizeof(OrderSegQ));
			if (os==NULL) {delete qop; qop=NULL; return RC_NORESOURCES;}
			for (unsigned i=0; i<nPropsReq; i++) {os[i].expr=NULL; os[i].flags=0; os[i].aggop=OP_ALL; os[i].lPref=0; os[i].pid=propsReq[i];}
			QueryOp *q=new(ses,0,nPropsReq) Sort(qop,os,nPropsReq,flg|QO_VUNIQUE,0,NULL,0);
			if (q!=NULL) qop=q; else {delete qop; qop=NULL; return RC_NORESOURCES;}
		}
		break;
#endif
	case DT_ALL:
		qop->unique(false); break;
	}
	if (qv->stype!=SEL_CONST && qv->stype!=SEL_VALUE && qv->stype!=SEL_DERIVED) {
		if (stmt->orderBy!=NULL)
			rc=sort(qop,stmt->orderBy,stmt->nOrderBy);
		/*else if ((qop->qflags&QO_DEGREE)!=0) {
			OrderSegQ ks; ks.pid=PROP_SPEC_ANY; ks.flags=0; ks.var=0; ks.aggop=OP_SET; ks.lPref=0;
			QueryOp *q=new(ses,1,0) Sort(qop,&ks,1,flg,1,NULL,0); if (q==NULL) delete qop; qop=q;
		}*/
	}
	if (qop==NULL) rc=RC_NORESOURCES;
	else if (rc!=RC_OK) {delete qop; qop=NULL;}
	else {
		if (nSkip!=0) qop->setSkip(nSkip);
		if ((mode&MODE_VERBOSE)!=0 || (ses->getTraceMode()&TRACE_EXEC_PLAN)!=0)
			{SOutCtx buf(ses); qop->print(buf,0); size_t l; byte *p=buf.result(l); ses->trace(1,"%.*s\n",l,p);}
	}
	return rc;
}

RC SetOpVar::build(class QBuildCtx& qctx,class QueryOp *&q) const
{
	q=NULL;	assert(type==QRY_UNION||type==QRY_EXCEPT||type==QRY_INTERSECT);
	if (type==QRY_EXCEPT && nVars!=2) return RC_INTERNAL;
	RC rc=RC_OK; QueryOp *qq; const ulong nqs0=qctx.nqs;
	const OrderSegQ *const os=qctx.sortReq; const unsigned nos=qctx.nSortReq;
	OrderSegQ ids; ids.pid=PROP_SPEC_PINID; ids.flags=0; ids.var=0; ids.aggop=OP_SET; ids.lPref=0;
	qctx.sortReq=&ids; qctx.nSortReq=1;
	for (unsigned i=0; i<nVars; i++) {
		if (qctx.nqs>=sizeof(qctx.src)/sizeof(qctx.src[0])) {rc=RC_NORESOURCES; break;}
		if ((rc=vars[i].var->build(qctx,qq))==RC_OK) qctx.src[qctx.nqs++]=qq; else break;
		if ((rc=qctx.sort(qq,NULL,0))!=RC_OK) break;
	}
	qctx.sortReq=os; qctx.nSortReq=nos;
	if (rc==RC_OK && (rc=qctx.mergeN(q,&qctx.src[nqs0],nVars,(QUERY_SETOP)type))==RC_OK) qctx.nqs-=nVars;
	else while (qctx.nqs>nqs0) delete qctx.src[--qctx.nqs];
	return rc;
}

RC JoinVar::build(class QBuildCtx& qctx,class QueryOp *&q) const
{
	q=NULL;	RC rc=RC_OK; const ulong nqs0=qctx.nqs;
	assert(type==QRY_SEMIJOIN||type==QRY_JOIN||type==QRY_LEFT_OUTER_JOIN||type==QRY_RIGHT_OUTER_JOIN||type==QRY_FULL_OUTER_JOIN);
	const bool fTrans=groupBy!=NULL && nGroupBy!=0 || outs!=NULL && nOuts!=0;
	if (fTrans) {
		// merge props
	}
	if (condEJ==NULL) {
		if ((rc=vars[0].var->build(qctx,qctx.src[qctx.nqs]))==RC_OK) qctx.nqs++; else return rc;
		if ((rc=vars[1].var->build(qctx,qctx.src[qctx.nqs]))==RC_OK) qctx.nqs++; else return rc;
		rc=qctx.nested(q,&qctx.src[nqs0],(const Expr**)(nConds==1?&cond:conds),nConds);
	} else for (const CondEJ *ce=condEJ; ;ce=ce->next) {
		if (ce==NULL) {
			// choose order of ce
			ce=condEJ;
			const OrderSegQ *const os=qctx.sortReq; const unsigned no=qctx.nSortReq;
			OrderSegQ ids; ids.pid=ce->propID1; ids.flags=0; ids.var=0; ids.aggop=OP_SET; ids.lPref=0;
			qctx.sortReq=&ids; qctx.nSortReq=1;
			if ((rc=vars[0].var->build(qctx,qctx.src[qctx.nqs]))==RC_OK) qctx.nqs++; else return rc;
			ids.pid=ce->propID2;
			if ((rc=vars[1].var->build(qctx,qctx.src[qctx.nqs]))==RC_OK) qctx.nqs++; else return rc;
			qctx.sortReq=os; qctx.nSortReq=no;
			rc=qctx.merge2(q,&qctx.src[nqs0],ce,(QUERY_SETOP)type,(const Expr**)(nConds==1?&cond:conds),nConds);
			break;
		}
		if (ce->propID1==PROP_SPEC_PINID && ce->propID2==PROP_SPEC_PINID) {
			if ((rc=vars[0].var->build(qctx,qctx.src[qctx.nqs]))==RC_OK) qctx.nqs++; else return rc;
			if ((rc=vars[1].var->build(qctx,qctx.src[qctx.nqs]))==RC_OK) qctx.nqs++; else return rc;
#if 0
			if (vars[0].var->getType()==QRY_COLLECTION /*&& (qctx.req&QRQ_SORT)==0*/) {
				if ((q=new(qctx.ses) HashOp(qctx.ses,qctx.src[nqs0],qctx.src[nqs0+1]))==NULL) rc=RC_NORESOURCES;
			}
#endif
			if (qctx.sortReq!=NULL && qctx.nSortReq!=0) {
				unsigned nP=0;
				if (QBuildCtx::checkSort(qctx.src[nqs0],qctx.sortReq,qctx.nSortReq,nP)) {
					if ((q=new(qctx.ses) HashOp(qctx.src[nqs0],qctx.src[nqs0+1]))==NULL) rc=RC_NORESOURCES;
					break;
				}
				if (QBuildCtx::checkSort(qctx.src[nqs0+1],qctx.sortReq,qctx.nSortReq,nP)) {
					if ((q=new(qctx.ses) HashOp(qctx.src[nqs0+1],qctx.src[nqs0]))==NULL) rc=RC_NORESOURCES;
					break;
				}
			}
			rc=qctx.mergeN(q,&qctx.src[nqs0],nVars,QRY_INTERSECT); break;
		}
	}
	if (rc==RC_OK) {qctx.nqs-=nVars; if (fTrans) rc=qctx.out(q,this);}
	return rc;
}

RC SimpleVar::build(class QBuildCtx& qctx,class QueryOp *&q) const
{
	q=NULL;	assert(type==QRY_SIMPLE);
	RC rc=RC_OK; QueryOp *qq,*primary=NULL; const ulong nqs0=qctx.nqs,ncqs0=qctx.ncqs;
	const bool fTrans=groupBy!=NULL && nGroupBy!=0 || outs!=NULL && nOuts!=0;
	if (stype==SEL_CONST) {assert(fTrans); return qctx.out(q,this);}
	
	if (fTrans) {
		// merge props
	}

	if (subq!=NULL) {
		if (subq->op!=STMT_QUERY || subq->top==NULL) return RC_INVPARAM;
		const Stmt *saveQ=qctx.stmt; qctx.stmt=subq;
// merge propsReq with getProps
		rc=subq->top->build(qctx,q); qctx.stmt=saveQ; if (rc!=RC_OK) return rc;
		return rc!=RC_OK || nConds==0 ? rc : qctx.filter(q,nConds==1?&cond:conds,nConds);
	} else for (ulong i=0; rc==RC_OK && i<nClasses; i++) {
		const ClassSpec &cs=classes[i]; ClassID cid=cs.classID; PID *pids; unsigned nPids,j;
		if ((cid&CLASS_PARAM_REF)!=0) {
			ulong idx=cid&~CLASS_PARAM_REF; if (idx>qctx.qx->vals[QV_PARAMS].nValues) return RC_INVPARAM;
			const Value& par=qctx.qx->vals[QV_PARAMS].vals[idx];
			switch (par.type) {
			case VT_URIID: cid=par.uid; break;
			case VT_REFID:
				if (qctx.nqs>=sizeof(qctx.src)/sizeof(qctx.src[0])) rc=RC_NORESOURCES;
				else if ((qctx.src[qctx.nqs]=new(qctx.ses,1) ArrayScan(qctx.qx,&par.id,1,qctx.flg))==NULL) rc=RC_NORESOURCES;
				else {qctx.nqs++; continue;}
				break;
			case VT_ARRAY:
				pids=NULL;
				if (qctx.nqs>=sizeof(qctx.src)/sizeof(qctx.src[0])) rc=RC_NORESOURCES;
				else for (j=nPids=0; j<par.length; j++) if (par.varray[j].type==VT_REFID) {
					if (pids==NULL && (pids=(PID*)qctx.ses->malloc((par.length-j)*sizeof(PID)))==NULL) {rc=RC_NORESOURCES; break;}
					pids[nPids++]=par.varray[j].id;
				}
				if (rc==RC_OK) {
					if (pids==NULL) continue; if (nPids>1) qsort(pids,nPids,sizeof(PID),cmpPIDs);
					if ((qctx.src[qctx.nqs]=new(qctx.ses,nPids) ArrayScan(qctx.qx,pids,nPids,qctx.flg))==NULL) rc=RC_NORESOURCES;
					else {qctx.nqs++; qctx.ses->free(pids); continue;}
				}
				if (pids!=NULL) qctx.ses->free(pids);
				break;
			case VT_STMT:
				if (par.stmt->getOp()!=STMT_QUERY) rc=RC_INVPARAM;
				else if (qctx.nqs>=sizeof(qctx.src)/sizeof(qctx.src[0])) rc=RC_NORESOURCES;
				else rc=RC_INTERNAL;	// NIY
				break;
			case VT_REFIDPROP:
			case VT_REFPROP:
				//...
			default: rc=RC_TYPE; break;
			}
			if (rc!=RC_OK) break;
		}
		Class *cls=qctx.ses->getStore()->classMgr->getClass(cid); if (cls==NULL) {rc=RC_NOTFOUND; break;}
		const Stmt *cqry=cls->getQuery(); ClassIndex *cidx=cls->getIndex(); const ulong cflg=cls->getFlags(); IndexScan *is=NULL;
		if (qctx.nqs>=sizeof(qctx.src)/sizeof(qctx.src[0])) rc=RC_NORESOURCES;
		else if ((cflg&(CLASS_INDEXED|CLASS_VIEW))!=CLASS_INDEXED || (qctx.mode&MODE_DELETED)!=0 && ((cflg&CLASS_SDELETE)==0||cs.nParams!=0&&cidx!=NULL)) {
			if (qctx.ncqs>=sizeof(qctx.condQs)/sizeof(qctx.condQs[0])) rc=RC_NORESOURCES;
			else if (cqry!=NULL) {
#ifdef _DEBUG
				if ((qctx.mode&(MODE_CLASS|MODE_DELETED))==0) {char *s=qctx.stmt->toString(); report(MSG_WARNING,"Using non-indexed class: %.512s\n",s); qctx.ses->free(s);}
#endif
				QueryWithParams &qs=qctx.condQs[qctx.ncqs++]; qs.params=NULL; qs.nParams=0;
				if ((qs.qry=cqry->clone(STMT_QUERY,qctx.ses))==NULL) rc=RC_NORESOURCES; else {qs.params=(Value*)cs.params; qs.nParams=cs.nParams;}
			}
		} else if (cidx==NULL) {
			if ((qctx.src[qctx.nqs++]=new(qctx.ses) ClassScan(qctx.qx,cls,qctx.flg))==NULL) rc=RC_NORESOURCES;
			else if (cqry!=NULL && cqry->hasParams() && cs.params!=NULL && cs.nParams!=0) {
				if (qctx.ncqs>=sizeof(qctx.condQs)/sizeof(qctx.condQs[0])) rc=RC_NORESOURCES;
				else {
					QueryWithParams &qs=qctx.condQs[qctx.ncqs++]; qs.params=NULL; qs.nParams=0;
					if ((qs.qry=cqry->clone(STMT_QUERY,qctx.ses))==NULL) rc=RC_NORESOURCES; else {qs.params=(Value*)cs.params; qs.nParams=cs.nParams;}
				}
			}
		} else {
			assert(cqry!=NULL && cqry->top!=NULL && cqry->top->getType()==QRY_SIMPLE && ((SimpleVar*)cqry->top)->condIdx!=NULL);
			ushort flags=SCAN_EXACT; ulong i=0,nRanges=0; const Value *param; const unsigned nSegs=((SimpleVar*)cqry->top)->nCondIdx;
			struct IdxParam {const Value *param; uint32_t idx;} *iparams=(IdxParam*)alloca(nSegs*sizeof(IdxParam));
			const Value **curValues=(const Value**)alloca(nSegs*sizeof(Value*)),*cv;
			if (iparams==NULL || curValues==NULL) rc=RC_NORESOURCES;
			else {
				if (cs.nParams==0) flags&=~SCAN_EXACT;
				else for (CondIdx *pci=((SimpleVar*)cqry->top)->condIdx; pci!=NULL; pci=pci->next,++i) {
					if (pci->param>=cs.nParams || (param=&cs.params[pci->param])->type==VT_RANGE && param->varray[0].type==VT_ANY && param->varray[1].type==VT_ANY) {
						iparams[i].param=NULL; flags&=~SCAN_EXACT;
					} else if (param->type==VT_ANY) {
						if ((pci->ks.flags&(ORD_NULLS_BEFORE|ORD_NULLS_AFTER))!=0 && nRanges==0) nRanges=1;
						iparams[i].param=NULL; flags&=~SCAN_EXACT;
					} else {
						if (param->type==VT_VARREF && (param->refV.flags&VAR_TYPE_MASK)==VAR_PARAM) {
							if (param->length!=0 || param->refV.refN>=qctx.qx->vals[QV_PARAMS].nValues) {rc=RC_INVPARAM; break;}
							param=&qctx.qx->vals[QV_PARAMS].vals[param->refV.refN];
						}
						iparams[i].param=param; iparams[i].idx=0; ulong nVals=1;
						if (param->type==VT_ARRAY) nVals=param->length;
						else if (param->type==VT_COLLECTION) nVals=param->nav->count();
						if (nVals>1) {
							if (pci->ks.op!=OP_EQ && pci->ks.op!=OP_IN && pci->ks.op!=OP_BEGINS) {rc=RC_TYPE; break;}
							flags&=~SCAN_EXACT;
						}
						if (nVals>nRanges) nRanges=nVals;
					}
				}
				if (rc==RC_OK && (is=new(qctx.ses,nRanges,*cidx) IndexScan(qctx.qx,*cidx,flags,nRanges,qctx.flg))==NULL) rc=RC_NORESOURCES;
				for (ulong i=0; rc==RC_OK && i<nRanges; i++) {
					bool fRange=false; byte op;
					for (ulong k=0; k<nSegs; k++) if ((cv=curValues[k]=iparams[k].param)!=NULL) {
						if (cv->type==VT_ARRAY) {
							if (iparams[k].idx<cv->length) cv=&cv->varray[iparams[k].idx++]; else cv=iparams[k].param=NULL;
						} else if (cv->type==VT_COLLECTION) {
							if ((cv=cv->nav->navigate(i==0?GO_FIRST:GO_NEXT))==NULL) iparams[k].param=NULL;
						}
						if ((curValues[k]=cv)==NULL) is->flags&=~SCAN_EXACT;
						else switch (cv->type) {
						case VT_RANGE:
							if (cidx->getIndexSegs()[k].op==OP_IN) {fRange=true; is->flags&=~SCAN_EXACT;} else rc=RC_TYPE;
							break;
						case VT_EXPR: case VT_STMT: case VT_EXPRTREE: case VT_VARREF:
							rc=RC_TYPE; break;
						default:
							op=cidx->getIndexSegs()[k].op;
							if (op!=OP_EQ && op!=OP_IN) {is->flags&=~SCAN_EXACT; if (op!=OP_BEGINS) fRange=true;}
							break;
						}
					}
					if (rc!=RC_OK || (rc=((SearchKey*)(is+1))[i*2].toKey(curValues,nSegs,cidx->getIndexSegs(),0,qctx.ses))!=RC_OK) break;
					if (!fRange) ((SearchKey*)(is+1))[i*2+1].copy(((SearchKey*)(is+1))[i*2]);
					else if ((rc=((SearchKey*)(is+1))[i*2+1].toKey(curValues,nSegs,cidx->getIndexSegs(),1,qctx.ses))!=RC_OK) break;
				}
				if (rc==RC_OK) {
					QVar *cqv=cqry->top; is->initInfo();
					if (cqv->nConds>0 && cqry->hasParams()) {
						const Expr *const *pc=cqv->nConds==1?&cqv->cond:cqv->conds; cqry=NULL;
						for (unsigned i=0; i<cqv->nConds; i++) if ((pc[i]->getFlags()&EXPR_PARAMS)!=0) {
							if (cqry==NULL) {
								if (qctx.ncqs>=sizeof(qctx.condQs)/sizeof(qctx.condQs[0]) || (cqry=new(qctx.ses) Stmt(0,qctx.ses))==NULL) {rc=RC_NORESOURCES; break;}
								if (((Stmt*)cqry)->addVariable()==0xFF) {((Stmt*)cqry)->destroy(); rc=RC_NORESOURCES; break;}
								QueryWithParams &qs=qctx.condQs[qctx.ncqs++]; qs.qry=(Stmt*)cqry; qs.params=(Value*)cs.params; qs.nParams=cs.nParams;
							}
							if (i+1==cqv->nConds) {
								if ((cqry->top->cond=Expr::clone(pc[i],qctx.ses))!=NULL) cqry->top->nConds=1;
								else {((Stmt*)cqry)->destroy(); rc=RC_NORESOURCES; break;}
							} else {
								if (cqry->top->conds==NULL && (cqry->top->conds=new(qctx.ses) Expr*[cqv->nConds])==NULL 
									|| (cqry->top->conds[cqry->top->nConds]=Expr::clone(pc[i],qctx.ses))==NULL)
										{((Stmt*)cqry)->destroy(); rc=RC_NORESOURCES; break;}
								cqry->top->nConds++;
							}
						}
					}
					if (rc!=RC_OK) delete is;
					else {
						qctx.src[qctx.nqs++]=is;
#if 0
						if ((qctx.req&QRQ_SORT)!=0 && primary==NULL) {
							//assert(orderProps!=NULL);
							//&& ci->expr==NULL && ci->pid==orderProps[0] && ((modf[0]^ci->flags)&ORD_NCASE)==0) primary=is;		// ????????????????????????????????????????????????????????????????
							// check order -> if ok: primary=is; qctx.nqs--;
							// sort ranges!
						}
#endif
					}
				}
			}
		}
		if (cidx==NULL || rc!=RC_OK) cls->release();
	}
	if ((qctx.mode&MODE_DELETED)==0 && rc==RC_OK) for (CondFT *cf=condFT; cf!=NULL; cf=cf->next) {
		if ((rc=qctx.mergeFT(qq,cf))!=RC_OK) break;
		if (qctx.nqs<sizeof(qctx.src)/sizeof(qctx.src[0])) qctx.src[qctx.nqs++]=qq; else {rc=RC_NORESOURCES; break;}
	}
	if (rc==RC_OK) {
		bool fArrayFilter=pids!=NULL && nPids!=0;
		if (qctx.nqs>nqs0) {
			if (qctx.nqs>nqs0+1 && (rc=qctx.mergeN(qq,&qctx.src[nqs0],qctx.nqs-nqs0,QRY_INTERSECT))==RC_OK) {qctx.src[nqs0]=qq; qctx.nqs=nqs0+1;}
			if (rc==RC_OK && primary!=NULL) {if ((qq=new(qctx.ses) HashOp(primary,qctx.src[nqs0]))!=NULL) {qctx.src[nqs0]=qq; primary=NULL;} else rc=RC_NORESOURCES;}
		} else if (qctx.nqs>=sizeof(qctx.src)/sizeof(qctx.src[0])) rc=RC_NORESOURCES;
		else if (primary!=NULL) {qctx.src[qctx.nqs++]=primary; primary=NULL;}
		else if (fArrayFilter) {
			fArrayFilter=false;
			if ((qctx.src[qctx.nqs]=new(qctx.ses,nPids) ArrayScan(qctx.qx,pids,nPids,qctx.flg))!=NULL) qctx.nqs++; else rc=RC_NORESOURCES;
		} else {
#ifdef _DEBUG
			if ((qctx.mode&(MODE_CLASS|MODE_DELETED))==0) {char *s=qctx.stmt->toString(); report(MSG_WARNING,"Full scan query: %.512s\n",s); qctx.ses->free(s);}
#endif
			qctx.src[qctx.nqs]=new(qctx.ses) FullScan(qctx.qx,(qctx.mode&(MODE_CLASS|MODE_NODEL))==QO_CLASS?HOH_HIDDEN:(qctx.mode&MODE_DELETED)!=0?HOH_DELETED<<16|HOH_DELETED|HOH_HIDDEN:HOH_DELETED|HOH_HIDDEN,qctx.flg);
			if (qctx.src[qctx.nqs]!=NULL) qctx.nqs++; else rc=RC_NORESOURCES;
		}
		if (rc==RC_OK) {
			if (fArrayFilter) {if ((qq=new(nPids,qctx.ses) ArrayFilter(qctx.src[nqs0],pids,nPids))!=NULL) qctx.src[nqs0]=qq; else rc=RC_NORESOURCES;}
			if (rc==RC_OK && path!=NULL && nPathSeg!=0) {
				const PathSeg *ps=path;
				if ((qctx.flg&QO_VCOPIED)!=0) {
					if ((ps=new(qctx.ses) PathSeg[nPathSeg])==NULL) rc=RC_NORESOURCES;
					else {
						memcpy((PathSeg*)ps,path,nPathSeg*sizeof(PathSeg));
						for (unsigned i=0; i<nPathSeg; i++)
							if (ps[i].filter!=NULL && (((PathSeg*)ps)[i].filter=Expr::clone((Expr*)ps[i].filter,qctx.ses))==NULL) {qctx.ses->free((void*)ps); rc=RC_NORESOURCES; break;}
					}
				}
				if (rc==RC_OK) {
					if (qctx.src[nqs0]->props==NULL || BIN<PropertyID>::find(ps[0].pid,qctx.src[nqs0]->props[0].props,qctx.src[nqs0]->props[0].nProps)==NULL) {
						PropList pl; pl.props=(PropertyID*)&ps[0].pid; pl.nProps=1; pl.fFree=false;
						QueryOp *q=new(qctx.ses,1) LoadOp(qctx.src[nqs0],&pl,1,qctx.flg); if (q!=NULL) qctx.src[nqs0]=q; else rc=RC_NORESOURCES;
					}
					if ((qq=new(qctx.ses) PathOp(qctx.src[nqs0],ps,nPathSeg,qctx.flg))!=NULL) qctx.src[nqs0]=qq; else rc=RC_NORESOURCES;
				}
			}
		}
	}
	if (rc!=RC_OK) {
		if (primary!=NULL) delete primary;
		while (qctx.nqs>nqs0) delete qctx.src[--qctx.nqs];
		while (qctx.ncqs>ncqs0) {QueryWithParams &qs=qctx.condQs[--qctx.ncqs]; if (qs.qry!=NULL) qs.qry->destroy();}
	} else {
		if ((q=qctx.src[nqs0])!=NULL && (nConds!=0 || condIdx!=NULL || qctx.ncqs>ncqs0)) rc=qctx.filter(q,nConds==1?&cond:conds,nConds,condIdx,qctx.ncqs-ncqs0);
		if (rc==RC_OK && fTrans) rc=qctx.out(q,this);
	}
	qctx.nqs=nqs0; qctx.ncqs=ncqs0; return rc;		//???
}

bool QBuildCtx::checkSort(QueryOp *q,const OrderSegQ *req,unsigned nReq,unsigned& nP)
{
	if (q->sort==NULL || nReq==0) return 0; bool fRev=false;
	for (unsigned f=nP=0; ;nP++)
		if (nP>=nReq) {if (fRev) {nP=0; return false;} /*qop->reverse();*/ return true;}
		else if (nP>=q->nSegs || q->sort[nP].pid!=req[nP].pid || (q->sort[nP].flags&ORDER_EXPR)!=0) return false;
		else if ((f=(q->sort[nP].flags^req[nP].flags)&SORT_MASK)==0) {if (fRev) return false;}
		else if ((q->qflags&QO_REVERSIBLE)==0 || (f&~ORD_DESC)!=0) return false;
		else if (nP==0) fRev=true; else if (!fRev) return false;
}

RC QBuildCtx::sort(QueryOp *&qop,const OrderSegQ *os,unsigned no,PropListP *pl,bool fTmp)
{
	if (os==NULL || no==1 && (os->flags&ORDER_EXPR)==0 && os->pid==PROP_SPEC_PINID) no=0;
	if (no==0 && (qop->qflags&QO_IDSORT)!=0) return RC_OK;

	unsigned nP=0; RC rc=RC_OK; if (checkSort(qop,os,no,nP)) return RC_OK;

	try {
		PropListP plp(ses); if (pl!=NULL && pl->nPls!=0) plp+=*pl;
		if (!fTmp && no==1 && (os->flags&ORDER_EXPR)==0 && os->var==0 && (pl==NULL||pl->nPls==0)) {
			plp.pls[0].props=(PropertyID*)&os->pid; plp.pls[0].nProps=1; plp.pls[0].fFree=false; plp.nPls=1;
		} else if (no!=0) for (unsigned i=0; i<no; i++)
			if ((os[i].flags&ORDER_EXPR)!=0) {if ((rc=os[i].expr->mergeProps(plp,fTmp))!=RC_OK) return rc;}
			else if (os[i].pid==PROP_SPEC_PINID) {no=i+1; break;}
			else if ((rc=plp.merge(os[i].var,&os[i].pid,1,fTmp))!=RC_OK) return rc;
		if ((rc=load(qop,plp))==RC_OK) {
			Sort *srt=new(ses,no,plp.nPls) Sort(qop,os,no,flg,nP,plp.pls,plp.nPls);
			if (srt!=NULL) {qop=srt; for (unsigned i=0; i<plp.nPls; i++) plp.pls[i].fFree=false;} else rc=RC_NORESOURCES;
		}
	} catch (RC rc2) {rc=rc2;}
	return rc;
}

RC QBuildCtx::mergeN(QueryOp *&res,QueryOp **o,unsigned no,QUERY_SETOP op)
{
	res=NULL; RC rc; if (o==NULL || no<2) return RC_INTERNAL;
	for (unsigned i=0; i<no; i++) {if ((o[i]->getQFlags()&(QO_IDSORT|QO_UNIQUE))!=(QO_IDSORT|QO_UNIQUE) && (rc=sort(o[i],NULL,0))!=RC_OK) return rc;}
	return (res=new(ses,no) MergeIDs(qx,o,no,op,flg))!=NULL?RC_OK:RC_NORESOURCES;
}

RC QBuildCtx::merge2(QueryOp *&res,QueryOp **qs,const CondEJ *cej,QUERY_SETOP qo,const Expr *const *conds,unsigned nConds)
{
	res=NULL; if (qs[0]==NULL || qs[1]==NULL) return RC_EOF;
	ulong ff=qo==QRY_SEMIJOIN?QO_UNIQUE:0; RC rc; assert(qo<QRY_UNION);
	unsigned nej=0; bool fS1=qs[0]->sort!=NULL,fS2=qs[1]->sort!=NULL,fR1=false,fR2=false,fI1=false,fI2=false;
	for (const CondEJ *ce=cej; ce!=NULL; ce=ce->next,nej++) {
		if (ce->propID1==PROP_SPEC_PINID && (qs[0]->qflags&QO_IDSORT)!=0) fI1=true;
		else if (fS1) for (unsigned i=0; ;i++) 
			if (i>=qs[0]->nSegs||(qs[0]->sort[i].flags&ORDER_EXPR)!=0) {fS1=false; break;}
			else if (qs[0]->sort[i].pid==ce->propID1 && ((qs[0]->sort[i].flags^ce->flags)&SORT_MASK)==0) {if (i!=nej) fR1=true; break;}
		if (ce->propID2==PROP_SPEC_PINID && (qs[1]->qflags&QO_IDSORT)!=0) fI2=true;
		else if (fS2) for (unsigned i=0; ;i++) 
			if (i>=qs[1]->nSegs||(qs[1]->sort[i].flags&ORDER_EXPR)!=0) {fS2=false; break;}
			else if (qs[1]->sort[i].pid==ce->propID2 && ((qs[1]->sort[i].flags^ce->flags)&SORT_MASK)==0) {if (i!=nej) fR2=true; break;}
	}
	if (nej>1) fI1=fI2=false;
	else {
		if (fI1) {fS1=true; fR1=false;} else if (qo==QRY_SEMIJOIN) qs[0]->unique(false);
		if (fI2) {fS2=true; fR2=false;} else if (qo==QRY_SEMIJOIN) qs[1]->unique(false);
	}
	if (!fS1 || !fS2 || fR1 || fR2) {
		if (fS1 && fS2) {
			if (!fR1) fS2=false; else if (!fR2) fS1=false;
			else {
#if 0
				// try to re-order
				CondEJ *ej=(CondEJ*)ses->malloc(nej*sizeof(CondEJ)); if (ej==NULL) return RC_NORESOURCES;
				const OrderSegQ *os=qs[0]->sort;
				for (unsigned i=0; i<nej; i++,os++) {
					for (ce=cej; ;ce=ce->next)
						if (ce==NULL) return RC_INTERNAL;
						else {
						}
					memcpy(&ej[i],ce,sizeof(CondEJ)); ej[i].next=i+1<nej?&ej[i+1]:NULL;
				}
				cej=ej; ff|=QO_VCOPIED;
#else
				fS1=fS2=false;
#endif
			}
		}
		if (!fS1 || !fS2) {
			OrderSegQ *oss=(OrderSegQ*)alloca(nej*sizeof(OrderSegQ)); if (oss==NULL) return RC_NORESOURCES;
			if (!fS1 && !fI1) {
				OrderSegQ *os=oss; memset(oss,0,nej*sizeof(OrderSegQ));
				for (const CondEJ *ce=cej; ce!=NULL; ce=ce->next,os++) {os->pid=ce->propID1; os->flags=ce->flags; os->aggop=OP_ALL;}
				if ((rc=sort(qs[0],oss,nej,NULL,true))!=RC_OK) return rc;	// cleanup???
			}
			if (!fS2 && !fI2) {
				OrderSegQ *os=oss; memset(oss,0,nej*sizeof(OrderSegQ));
				for (const CondEJ *ce=cej; ce!=NULL; ce=ce->next,os++) {os->pid=ce->propID2; os->flags=ce->flags; os->aggop=OP_ALL;}
				if ((rc=sort(qs[1],oss,nej,NULL,true))!=RC_OK) return rc;	// cleanup???
			}
		}
	} else if ((flg&QO_VCOPIED)!=0 && cej!=NULL) {
		CondEJ *ej=(CondEJ*)ses->malloc(nej*sizeof(CondEJ)); if (ej==NULL) return RC_NORESOURCES;
		for (unsigned i=0; i<nej; i++,cej=cej->next) {memcpy(&ej[i],cej,sizeof(CondEJ)); ej[i].next=i+1<nej?&ej[i+1]:NULL;}
		cej=ej;
	}
	if ((qs[0]->qflags&QO_UNIQUE)!=0 && cej->propID1==PROP_SPEC_PINID) ff|=QO_UNI1;
	if ((qs[1]->qflags&QO_UNIQUE)!=0 && cej->propID2==PROP_SPEC_PINID) ff|=QO_UNI2;
	try {if ((res=new(ses,nej) MergeOp(qs[0],qs[1],cej,nej,qo,conds,nConds,flg|ff))==NULL) return RC_NORESOURCES;} catch (RC rc) {return rc;}
	return RC_OK;
}

RC QBuildCtx::nested(QueryOp *&res,QueryOp **qs,const Expr **conds,unsigned nConds)
{
	PropListP plp(ses); RC rc=RC_OK;
	for (unsigned i=0; i<nConds; i++) if ((rc=conds[i]->mergeProps(plp))!=RC_OK) return rc;
	const unsigned nLeft=qs[0]->getNOuts(),save=plp.nPls;
	if (save>nLeft) plp.nPls=nLeft;
	if ((rc=load(qs[0],plp))!=RC_OK) {plp.nPls=save; return rc;}
	if (save>nLeft) {
		plp.pls+=nLeft; plp.nPls=save-nLeft;
		rc=load(qs[1],plp);
		plp.pls-=nLeft; plp.nPls=save;
		if (rc!=RC_OK) return rc;
	}
	return (res=new(ses) NestedLoop(qs[0],qs[1],flg))!=NULL?RC_OK:RC_NORESOURCES;
}

RC QBuildCtx::mergeFT(QueryOp *&res,const CondFT *cft)
{
	StringTokenizer q(cft->str,strlen(cft->str),false); const FTLocaleInfo *loc=ses->getStore()->ftMgr->getLocale();
	const char *pW; size_t lW; char buf[256]; bool fStop=false,fFlt=(cft->flags&QFT_FILTER_SW)!=0; RC rc=RC_OK; res=NULL;
	QueryOp *qopsbuf[20],**qops=qopsbuf; ulong nqops=0,xqops=20;
	while ((pW=q.nextToken(lW,loc,buf,sizeof(buf)))!=NULL) {
#if 0
		if (*pW==DEFAULT_PHRASE_DEL) {
			if (lW>2) {
				if ((++pW)[--lW-1]==DEFAULT_PHRASE_DEL) lW--;
				// store exact phrase in lower case to phrase
				StringTokenizer phraseStr(pW,lW); FTQuery **pPhrase=NULL;
				while ((pW=reqStr.nextToken(lW,loc,buf,sizeof(buf)))!=NULL) if (!loc->isStopWord(StrLen(pW,lW))) {
					if (loc->stemmer!=NULL) pW=loc->stemmer->process(pW,lW,buf);
					if ((p=(char*)ses->malloc(lW+1))!=NULL) {
						memcpy(p,pW,lW); p[lW]='\0'; StrLen word(p,lW); 
						FTQuery *ft=new FTQuery(word);
						if (ft==NULL) ses->free(p);
						else if (pPhrase!=NULL) {*pPhrase=ft; pPhrase=&ft->next;}
						else {
							FTQuery *ph=*pQuery=new FTQuery(ft); 
							if (ph==NULL) delete ft; else {pQuery=&ph->next; pPhrase=&ft->next;}
						}
					}
				}
			}
		} else 
#endif
		if (lW>1 && (!fFlt || !(fStop=loc->isStopWord(StrLen(pW,lW)))) || q.isEnd() && (lW>1 || nqops==0)) {
			if (loc->stemmer!=NULL) pW=loc->stemmer->process(pW,lW,buf);
			FTScan *ft=new(ses,cft->nPids,lW+1) FTScan(qx,pW,lW,cft->pids,cft->nPids,flg,cft->flags,fStop);
			if (ft==NULL) {rc=RC_NORESOURCES; break;}
			if (nqops>=xqops) {
				if (qops!=qopsbuf) qops=(QueryOp**)ses->realloc(qops,(xqops*=2)*sizeof(QueryOp*));
				else if ((qops=(QueryOp**)ses->malloc((xqops*=2)*sizeof(QueryOp*)))!=NULL) memcpy(qops,qopsbuf,nqops*sizeof(QueryOp*));
				if (qops==NULL) return RC_NORESOURCES;
			}
			qops[nqops++]=ft;
		}
	}
	if (rc==RC_OK) {
		if (nqops<=1) {res=nqops!=0?qops[0]:(rc=RC_EOF,(QueryOp*)0); nqops=0;}
		else if ((rc=mergeN(res,qops,nqops,(mode&MODE_ALL_WORDS)==0?QRY_UNION:QRY_INTERSECT))==RC_OK) nqops=0;
	}
	for (ulong i=0; i<nqops; i++) delete qops[i];
	if (qops!=qopsbuf) ses->free(qops);
	return rc;
}

RC QBuildCtx::filter(QueryOp *&qop,const Expr *const *conds,unsigned nConds,const CondIdx *condIdx,unsigned ncq)
{
	if ((qop->qflags&QO_ALLPROPS)==0) {
		PropListP req(ses); flg|=QO_NODATA; RC rc=RC_OK; 		// force?
		if (conds!=NULL) for (unsigned i=0; i<nConds; i++) if ((rc=conds[i]->mergeProps(req))!=RC_OK) return rc;
		for (const CondIdx *ci=condIdx; ci!=NULL; ci=ci->next)
			if ((rc=ci->expr!=NULL?ci->expr->mergeProps(req):req.merge(0,&ci->ks.propID,1))!=RC_OK) return rc;
		for (unsigned i=0; i<ncq; i++) if (condQs[ncqs-i-1].qry->top!=NULL)
			if ((rc=condQs[ncqs-i-1].qry->top->mergeProps(req))!=RC_OK) return rc;
		if (req.nPls!=0) {flg&=~QO_NODATA; if ((rc=load(qop,req))!=RC_OK) return rc;}
	}
	try {
		Filter *flt=new(ses) Filter(qop,nqs,flg); if (flt==NULL) return RC_NORESOURCES;
		bool fOK=true; 
		if (conds!=NULL) {
			flt->nConds=nConds; Expr **pex;
			if ((flg&QO_VCOPIED)==0) flt->conds=conds;
			else if ((flt->conds=pex=new(ses) Expr*[nConds])==NULL) fOK=false;
			else {memset(pex,0,nConds*sizeof(Expr*)); for (ulong i=0; i<nConds; ++i) if ((pex[i]=Expr::clone(conds[i],ses))==NULL) {fOK=false; break;}}
		}
		if ((flg&QO_CLASS)==0) {
			if ((flg&QO_VCOPIED)==0) flt->condIdx=(CondIdx*)condIdx;
			else if (fOK && condIdx!=NULL) for (CondIdx **ppCondIdx=&flt->condIdx; condIdx!=NULL; condIdx=condIdx->next) {
				CondIdx *newCI=condIdx->clone(ses);
				if (newCI==NULL) {fOK=false; break;} else {newCI->next=NULL; *ppCondIdx=newCI; ppCondIdx=&newCI->next; flt->nCondIdx++;}
			}
		}
		if (fOK && ncq>0) {
			assert(ncq<=ncqs);
			if ((flt->queries=new(ses) QueryWithParams[ncq])==NULL) fOK=false;
			else {
				memcpy(flt->queries,&condQs[ncqs-ncq],ncq*sizeof(QueryWithParams)); ncqs-=ncq;
				for (unsigned i=0; i<ncq; i++) {
					QueryWithParams& qwp=flt->queries[i]; RC rc;
					if (qwp.params!=NULL && qwp.nParams!=0) {
						if ((rc=copyV(qwp.params,qwp.nParams,qwp.params,ses))!=RC_OK) {for (;i<ncq;i++) flt->queries[i].params=NULL; fOK=false; break;}
						for (unsigned j=0; j<qwp.nParams; j++) if (qwp.params[j].type==VT_VARREF && (qwp.params[j].refV.flags&VAR_TYPE_MASK)==VAR_PARAM)
							if (qwp.params[j].refV.refN<qx->vals[QV_PARAMS].nValues) {qwp.params[j]=qx->vals[QV_PARAMS].vals[qwp.params[j].refV.refN]; qwp.params[j].flags=NO_HEAP;} else qwp.params[j].setError();
					}
				}
			}
		}
		if (!fOK) {delete flt; return RC_NORESOURCES;} else qop=flt;
	} catch (RC rc) {return rc;}
	return RC_OK;
}

RC QBuildCtx::load(QueryOp *&qop,const PropListP& plp)
{
	if (plp.nPls==0 || (qop->qflags&QO_ALLPROPS)!=0) return RC_OK;
	if (qop->props!=NULL && qop->nProps>=plp.nPls) {
		bool fFound=true;
		for (unsigned j=0; fFound; j++) {
			if (j>=plp.nPls) return RC_OK;
			const PropertyID *pp,*qp=qop->props[j].props; unsigned np=qop->props[j].nProps;
			for (unsigned i=0; i<plp.pls[j].nProps; i++) {
				if ((pp=BIN<PropertyID,PropertyID,ExprPropCmp>::find(plp.pls[j].props[i]&STORE_MAX_URIID,qp,np))==NULL) {fFound=false; break;}
				np-=unsigned(pp-qp)+1; qp=pp+1;
			}
		}
	}
	if (propsReq.nPls!=0) {
		// if (!fDel) copy
		// merge to req, nReq
	}
	// all pins, all props
	QueryOp *q=new(ses,plp.nPls) LoadOp(qop,plp.pls,plp.nPls,flg); if (q==NULL) return RC_NORESOURCES;
	for (unsigned i=0; i<plp.nPls; i++) plp.pls[i].fFree=false;
	qop=q; return RC_OK;
}

RC QBuildCtx::out(QueryOp *&qop,const QVar *qv)
{
	RC rc=RC_OK; ValueV *outs=qv->outs,*vv; unsigned nOuts=qv->nOuts;
	if (outs!=NULL && nOuts!=0 && (flg&QO_VCOPIED)!=0) {
		if ((vv=new(ses) ValueV[nOuts])==NULL) return RC_NORESOURCES;
		for (unsigned i=0; i<nOuts; i++)
			if ((rc=copyV(outs[i].vals,vv[i].nValues=outs[i].nValues,*(Value**)&vv[i].vals,ses))!=RC_OK) return rc;	// cleanup?
			else vv[i].fFree=true;
		outs=vv;
	}
	if (qv->stype==SEL_CONST) {if (qop!=NULL) {delete qop; qop=NULL;} return (qop=new(ses) TransOp(qx,outs,nOuts,flg|QO_UNIQUE))!=NULL?RC_OK:RC_NORESOURCES;}
	
	PropListP plp(ses);
	for (unsigned i=0; i<nOuts; i++) for (unsigned j=0; j<outs[i].nValues; j++) {
		const Value& v=outs[i].vals[j];
		switch (v.type) {
		case VT_EXPR: if ((v.meta&META_PROP_EVAL)!=0 && (rc=((Expr*)v.expr)->mergeProps(plp))!=RC_OK) return rc; break;
		case VT_VARREF: if ((v.refV.flags&VAR_TYPE_MASK)==0 && v.length==1 && (rc=plp.merge(v.refV.refN,&v.refV.id,1))!=RC_OK) return rc; break;
		}
	}
	if (qv->aggrs.vals!=NULL) for (unsigned i=0; i<qv->aggrs.nValues; i++) {
		const Value& v=qv->aggrs.vals[i];
		switch (v.type) {
		case VT_EXPR: if ((v.meta&META_PROP_EVAL)!=0 && (rc=((Expr*)v.expr)->mergeProps(plp))!=RC_OK) return rc; break;
		case VT_VARREF: if ((v.refV.flags&VAR_TYPE_MASK)==0 && v.length==1 && (rc=plp.merge(v.refV.refN,&v.refV.id,1))!=RC_OK) return rc; break;
		}
	}
	if (qv->groupBy!=NULL && qv->nGroupBy!=0) {
		if ((rc=sort(qop,qv->groupBy,qv->nGroupBy,&plp))!=RC_OK) return rc;
		if (outs==NULL || nOuts==0) {
			// get them from gb/nG
		}
	} else if ((rc=load(qop,plp))!=RC_OK) return rc;
	unsigned f=flg; if (qv->stype!=SEL_PINSET && qv->stype!=SEL_PROJECTED && qv->stype!=SEL_COMPOUND) f|=QO_UNIQUE;
	try {return (qop=new(ses) TransOp(qop,outs,nOuts,qv->aggrs,qv->groupBy,qv->nGroupBy,qv->having,f))!=NULL?RC_OK:RC_NORESOURCES;}
	catch (RC rc) {return rc;}
}

RC PropListP::checkVar(uint16_t var)
{
	if (var>=nPls) {
		if (var>=sizeof(pbuf)/sizeof(pbuf[0])) {
			if (pls!=pbuf) {if ((pls=(PropList*)ma->realloc(pls,(var+1)*sizeof(PropList)))==NULL) return RC_NORESOURCES;}
			else {PropList *pp=new(ma) PropList[var+1]; if (pp!=NULL) {memcpy(pp,pbuf,nPls*sizeof(PropList)); pls=pp;} else return RC_NORESOURCES;}
		}
		memset(pls+nPls,0,(var+1-nPls)*sizeof(PropList)); nPls=var+1;
	}
	return RC_OK;
}

RC PropListP::operator+=(const PropListP &rhs)
{
	RC rc=RC_OK;
	if (rhs.nPls!=0 && (rc=checkVar(rhs.nPls))==RC_OK) for (unsigned i=0; i<rhs.nPls; i++)
		if ((rc=merge((uint16_t)i,rhs.pls[i].props,rhs.pls[i].nProps,true,true))!=RC_OK) break;
	return rc;
}

RC PropListP::merge(uint16_t var,const PropertyID *pids,unsigned nPids,bool fForce,bool fFlags)
{
	RC rc=checkVar(var); if (rc!=RC_OK) return rc;
	PropList &pl=pls[var]; assert(var<=nPls);
	if (pl.props==NULL) {
		if (!fForce) {
			pl.props=(PropertyID*)pids; pl.nProps=nPids; pl.fFree=false; if (fFlags) return RC_OK; 
			bool f=false; for (unsigned i=0; i<nPids; i++) if ((pids[i]&~STORE_MAX_URIID)!=0) {f=true; break;}
			if (!f) return rc;
		}
		if ((pl.props=new(ma) PropertyID[nPids])==NULL) return RC_NORESOURCES;
		memcpy(pl.props,pids,(pl.nProps=nPids)*sizeof(uint32_t)); pl.fFree=true;
		if (!fFlags) for (unsigned i=0; i<nPids; i++) pl.props[i]&=STORE_MAX_URIID;
	} else for (unsigned i=0,sht=0; i<nPids; i++) {
		const PropertyID pid=pids[i]; unsigned ex=0,s2;
		PropertyID *ins,*pp=(PropertyID*)BIN<PropertyID,PropertyID,ExprPropCmp>::find(pid&STORE_MAX_URIID,pl.props+sht,pl.nProps-sht,&ins);
		if (pp!=NULL) {sht=unsigned(pp+1-pl.props); if (!fFlags||*pp==pid) continue; if (pl.fFree) {*pp|=pid; continue;}}
		else {s2=unsigned(ins-pl.props); if (s2>=pl.nProps) ex=nPids-i; else ex=1;}							// more?
		if (pl.fFree) {if ((pl.props=(PropertyID*)ma->realloc(pl.props,(pl.nProps+ex)*sizeof(PropertyID)))==NULL) return RC_NORESOURCES;}
		else if ((ins=(PropertyID*)ma->malloc((pl.nProps+ex)*sizeof(PropertyID)))==NULL) return RC_NORESOURCES;
		else {memcpy(ins,pl.props,pl.nProps*sizeof(PropertyID)); pl.props=ins; pl.fFree=true;}
		if (pp!=NULL) pl.props[sht-1]|=pid;
		else {
			if (s2<pl.nProps) memmove(&pl.props[s2+ex],&pl.props[s2],(pl.nProps-s2)*sizeof(PropertyID));
			memcpy(&pl.props[s2],&pids[i],ex*sizeof(uint32_t)); pl.nProps+=ex; sht+=s2+ex;
		}
	}
	return RC_OK;
}
