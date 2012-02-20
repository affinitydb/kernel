/**************************************************************************************

Copyright © 2008-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2008 - 2010

**************************************************************************************/

#include "maps.h"
#include "queryprc.h"
#include "parser.h"
#include "expr.h"
#include "blob.h"

using namespace AfyKernel;

TransOp::TransOp(QueryOp *q,const ValueV *d,unsigned nD,const ValueV& ag,const OrderSegQ *gs,unsigned nG,const Expr *hv,ulong qf) 
	: QueryOp(q,qf|(q->getQFlags()&(QO_UNIQUE|QO_IDSORT|QO_REVERSIBLE))),dscr(d),ins(NULL),nIns(0),qr(qx->ses),pqr(&qr),res(NULL),nRes(0),aggs(ag),groupSeg(gs),nGroup(nG),having(hv),ac(NULL)
{
	nOuts=nD!=0?nD:1; sort=q->getSort(nSegs);
	if ((qf&QO_VCOPIED)!=0) {
		if (ag.vals!=NULL) {RC rc=copyV(ag.vals,ag.nValues,*(Value**)&aggs.vals,qx->ses); if (rc!=RC_OK) throw rc; aggs.fFree=true;}
		if (gs!=NULL) {
			if ((groupSeg=new(qx->ses) OrderSegQ[nG])==NULL) throw RC_NORESOURCES;
			memcpy((OrderSegQ*)groupSeg,gs,nG*sizeof(OrderSegQ));
			for (unsigned i=0; i<nG; i++) if ((gs[i].flags&ORDER_EXPR)!=0)
				if ((((OrderSegQ*)groupSeg)[i].expr=Expr::clone(gs[i].expr,qx->ses))==NULL) throw RC_NORESOURCES;
			if (hv!=NULL && (having=Expr::clone(hv,qx->ses))==NULL) throw RC_NORESOURCES;
		}
	}
}

TransOp::TransOp(QCtx *qc,const ValueV *d,unsigned nD,ulong qf) 
	: QueryOp(qc,qf|QO_UNIQUE),dscr(d),ins(NULL),nIns(0),qr(qc->ses),pqr(&qr),res(NULL),nRes(0),groupSeg(NULL),nGroup(0),having(NULL),ac(NULL)
{
	nOuts=nD!=0?nD:1;
}

TransOp::~TransOp()
{
	if (ac!=NULL) for (unsigned j=0; j<aggs.nValues; j++) if (ac[j].hist!=NULL) {ac[j].hist->~Histogram(); qx->ses->free(ac[j].hist);}
	if ((qflags&QO_VCOPIED)!=0) {
		if (dscr!=NULL) {
			for (ulong i=0; i<nOuts; i++) if (dscr[i].vals!=NULL && dscr[i].fFree) freeV((Value*)dscr[i].vals,dscr[i].nValues,qx->ses);
			qx->ses->free((void*)dscr);
		}
		if (groupSeg!=NULL) {
			for (unsigned i=0; i<nGroup; i++) if ((groupSeg[i].flags&ORDER_EXPR)!=0) groupSeg[i].expr->destroy();
			qx->ses->free((void*)groupSeg); if (having!=NULL) qx->ses->free((Expr*)having);
		}
		if (aggs.vals!=NULL && aggs.fFree) freeV((Value*)aggs.vals,aggs.nValues,qx->ses);
	}
	if (nIns>1) {
		for (unsigned i=1; i<nIns; i++) if (ins[i]!=NULL) {ins[i]->~PINEx(); qx->ses->free(ins[i]);}
		qx->ses->free(ins);
	}
}

void TransOp::connect(PINEx **results,unsigned nr)
{
	res=results; nRes=nr;
	if (queryOp!=NULL && (nIns=queryOp->getNOuts())!=0) {
		if (nIns==1) queryOp->connect(ins=&pqr);
		else if ((ins=new(qx->ses) PINEx*[nIns])==NULL) return;	//???
		else {
			memset(ins,0,nIns*sizeof(PINEx*)); ins[0]=pqr;
			for (unsigned i=1; i<nIns; i++) if ((ins[i]=new(qx->ses) PINEx(qx->ses))==NULL) return;		//???
			queryOp->connect(ins,nIns);
		}
	}
}

RC TransOp::next(const PINEx *)
{
	if ((state&QST_EOF)!=0) return RC_EOF;
	if ((state&QST_INIT)!=0) {
		state&=~QST_INIT;
		if (aggs.nValues!=0) {
			if ((qx->vals[QV_AGGS].vals=new(qx->ses) Value[qx->vals[QV_AGGS].nValues=aggs.nValues])==NULL) {state|=QST_EOF; return RC_NORESOURCES;}
			qx->vals[QV_AGGS].fFree=true; memset((Value*)qx->vals[QV_AGGS].vals,0,aggs.nValues*sizeof(Value));
			if ((ac=new(qx->ses) AggAcc[aggs.nValues])==NULL) {state|=QST_EOF; return RC_NORESOURCES;}
			for (unsigned i=0; i<aggs.nValues; i++) {
				*const_cast<MemAlloc**>(&ac[i].ma)=qx->ses; ac[i].op=(ExprOp)aggs.vals[i].op;	// flags???
				if (aggs.vals[i].op==OP_HISTOGRAM && (ac[i].hist=new(qx->ses) Histogram(*qx->ses,0))==NULL) {state|=QST_EOF; return RC_NORESOURCES;}		// flags ???
			}
		}
		if (nGroup!=NULL) {
			if ((qx->vals[QV_GROUP].vals=new(qx->ses) Value[qx->vals[QV_GROUP].nValues=nGroup])==NULL) {state|=QST_EOF; return RC_NORESOURCES;}
			qx->vals[QV_GROUP].fFree=true; memset((Value*)qx->vals[QV_GROUP].vals,0,nGroup*sizeof(Value));
		}
		state|=QST_BOF;
	}
	RC rc=RC_OK; Value *newV=NULL;
	if (queryOp==NULL) state|=QST_EOF;
	else for (;;) {
		if ((rc=queryOp->next())!=RC_OK) {
			state|=QST_EOF; if (rc!=RC_EOF || nGroup+aggs.nValues==0 || (state&QST_BOF)!=0) return rc;
			for (unsigned i=0; i<aggs.nValues; i++) {
				if (nGroup!=0) freeV(*(Value*)&qx->vals[QV_AGGS].vals[i]);
				if ((rc=ac[i].result(*(Value*)&qx->vals[QV_AGGS].vals[i]))!=RC_OK) return rc;
			}
			if (having!=NULL && !Expr::condSatisfied(&having,1,res,nRes,qx->vals,QV_ALL,qx->ses)) return RC_EOF;
			rc=RC_OK; newV=NULL; break;
		}
		bool fRepeat=false;
		if (nGroup!=0) {
			fRepeat=true; assert(qx->vals[QV_GROUP].vals!=NULL);
			if ((state&QST_BOF)!=0) {if ((rc=queryOp->loadData(*ins[0],(Value*)qx->vals[QV_GROUP].vals,nGroup,STORE_COLLECTION_ID,true))!=RC_OK) {state|=QST_EOF; return rc;}}
			else if (newV==NULL && (newV=(Value*)alloca(nGroup*sizeof(Value)))==NULL) {state|=QST_EOF; return RC_NORESOURCES;}
			else if ((rc=queryOp->loadData(*ins[0],newV,nGroup,STORE_COLLECTION_ID,true))!=RC_OK) {state|=QST_EOF; return rc;}
			else for (unsigned i=0; i<nGroup; i++) if (cmp(qx->vals[QV_GROUP].vals[i],newV[i],groupSeg[i].flags)!=0) {fRepeat=false; break;}
		}
		if (dscr!=NULL&&!fRepeat || ac!=NULL) for (unsigned i=0; i<nIns; i++) if ((rc=getData(*ins[i],NULL,0))!=RC_OK) {state|=QST_EOF; return rc;}
		for (unsigned i=0; i<aggs.nValues; i++) {
			if (nGroup!=0 && !fRepeat) {
				freeV(*(Value*)&qx->vals[QV_AGGS].vals[i]);
				if ((rc=ac[i].result(*(Value*)&qx->vals[QV_AGGS].vals[i]))!=RC_OK) {state|=QST_EOF; return rc;}
			}
			const Value &v=aggs.vals[i]; Value w; w.setError();
			if (v.type==VT_VARREF) {
				if (v.refV.refN>=nIns) continue; assert((v.refV.flags&VAR_TYPE_MASK)==0 && v.length!=0);
				rc=ins[v.refV.refN]->getValue(v.refV.id,w,LOAD_SSV,qx->ses,v.eid<STORE_ALL_ELEMENTS||v.eid>=STORE_FIRST_ELEMENT?v.eid:STORE_COLLECTION_ID);
			} else if (v.type==VT_EXPR && (v.meta&META_PROP_EVAL)!=0) rc=Expr::eval((const Expr**)&v.expr,1,w,ins,nIns,qx->vals,QV_ALL,qx->ses);
			else if (v.type!=VT_ANY) {w=v; w.flags=NO_HEAP;}
			if (rc==RC_OK) rc=ac[i].process(w); freeV(w);
			if (rc==RC_NOTFOUND) rc=RC_OK; else if (rc!=RC_OK) {state|=QST_EOF; return rc;}
		}
		state&=~QST_BOF;
		if (nGroup!=0) {
			if (fRepeat) {if (newV!=NULL) for (unsigned i=0; i<nGroup; i++) freeV(newV[i]);}
			else if (having==NULL || Expr::condSatisfied(&having,1,res,nRes,qx->vals,QV_ALL,qx->ses)) break;
			else {for (unsigned i=0; i<nGroup; i++) freeV(*(Value*)&qx->vals[QV_GROUP].vals[i]); memcpy((Value*)qx->vals[QV_GROUP].vals,newV,nGroup*sizeof(Value));}
		} else if (ac==NULL) break;
	}
	if (dscr==NULL) {
		assert(nGroup!=0);
		if (res!=NULL && res[0]!=NULL) {
			res[0]->setProps(qx->vals[QV_GROUP].vals,nGroup,0); res[0]->mode|=PIN_DERIVED;
			if (newV==NULL) {qx->vals[QV_GROUP].vals=NULL; qx->vals[QV_GROUP].fFree=false;}
			else if ((qx->vals[QV_GROUP].vals=new(qx->ses) Value[nGroup])==NULL) {state|=QST_EOF; return RC_NORESOURCES;}
		} else if (newV!=NULL) for (unsigned i=0; i<nGroup; i++) freeV(*(Value*)&qx->vals[QV_GROUP].vals[i]);
		if (newV!=NULL) memcpy((Value*)qx->vals[QV_GROUP].vals,newV,nGroup*sizeof(Value));
	} else {
		for (unsigned i=0; i<nOuts; i++) {
			const ValueV &td=dscr[i]; PINEx *re=i<nRes?res[i]:(PINEx*)0; unsigned md=0; Value w,*to=&w;
			if (re!=NULL) {
				if (re->properties==NULL || (re->mode&PIN_NO_FREE)!=0) {
					if ((re->properties=(Value*)qx->ses->malloc(td.nValues*sizeof(Value)))!=NULL) re->mode&=~PIN_NO_FREE; else {rc=RC_NORESOURCES; break;}
				} else {
					for (unsigned k=0; k<re->nProperties; k++) freeV(*(Value*)&re->properties[k]);
					if (re->nProperties<td.nValues && (re->properties=(Value*)qx->ses->realloc((void*)re->properties,td.nValues*sizeof(Value)))==NULL) {rc=RC_NORESOURCES; break;}
				}
				if (nGroup+aggs.nValues!=0 || i>=nIns) re->mode=re->mode&~PIN_PROJECTED|PIN_DERIVED; else {re->mode&=~(PIN_DERIVED|PIN_PROJECTED); ins[i]->getID(re->id);}
				re->nProperties=td.nValues; to=re->properties;
			}
			for (ulong j=0; j<td.nValues; j++) {
				const Value& v=td.vals[j],*cv; Value w; ValueType ty=VT_ANY; ExprOp op=OP_SET; TIMESTAMP ts;
//				if ((v.meta&META_PROP_IFNOTEXIST)!=0 && re->defined(&v.property,1)) continue;
				switch (v.type) {
				case VT_VARREF:
					if ((v.refV.flags&VAR_TYPE_MASK)!=0) {
						const ValueV *vals=&qx->vals[((v.refV.flags&VAR_TYPE_MASK)>>13)-1]; md|=PIN_DERIVED;
						if (v.refV.refN>=vals->nValues) rc=RC_NOTFOUND; else rc=copyV(vals->vals[v.refV.refN],*to,qx->ses);
					} else if (v.refV.refN>=nIns) rc=RC_NOTFOUND;
					else if (v.length==0) {
						//???
						md|=PIN_DERIVED;
					} else {
						rc=ins[v.refV.refN]->getValue(v.refV.id,*to,LOAD_SSV,qx->ses,v.eid<STORE_ALL_ELEMENTS||v.eid>=STORE_FIRST_ELEMENT?v.eid:STORE_COLLECTION_ID);
						if (v.refV.id!=v.property||v.refV.refN!=i) md|=PIN_DERIVED;
					}
					ty=(ValueType)v.refV.type; break;
				case VT_CURRENT:
					md|=PIN_DERIVED;
					switch (v.i) {
					default: rc=RC_CORRUPTED; break;
					case CVT_TIMESTAMP: getTimestamp(ts); to->setDateTime(ts); break;
					case CVT_USER: to->setIdentity(qx->ses->getIdentity()); break;
					case CVT_STORE: to->set((unsigned)qx->ses->getStore()->storeID); break;
					}
					break;
				case VT_EXPR:
					if ((v.meta&META_PROP_EVAL)!=0) {
						rc=Expr::eval((const Expr**)&v.expr,1,*to,ins,nIns,qx->vals,QV_ALL,qx->ses); md|=PIN_DERIVED; break;
					}
				default: *to=v; to->flags=NO_HEAP; md|=PIN_DERIVED; break;
				}
				if (rc==RC_OK && (to->type==VT_ARRAY || to->type==VT_COLLECTION)) switch (v.eid) {
				case STORE_COLLECTION_ID: break;
				case STORE_FIRST_ELEMENT:
				case STORE_LAST_ELEMENT:
					cv=to->type==VT_ARRAY?&to->varray[v.eid==STORE_FIRST_ELEMENT?0:to->length-1]:
						to->nav->navigate(v.eid==STORE_FIRST_ELEMENT?GO_FIRST:GO_LAST);
					if (cv==NULL) rc=RC_NOTFOUND; else if ((rc=copyV(*cv,w,qx->ses))==RC_OK) {freeV(*to); *to=w;}
					md|=PIN_DERIVED; break;
				case STORE_SUM_COLLECTION: op=OP_PLUS; goto aggregate;
				case STORE_AVG_COLLECTION: op=OP_AVG; goto aggregate;
				case STORE_CONCAT_COLLECTION: op=OP_CONCAT; goto aggregate;
				case STORE_MIN_ELEMENT: op=OP_MIN; goto aggregate;
				case STORE_MAX_ELEMENT: op=OP_MAX;
				aggregate: rc=Expr::calcAgg(op,*to,NULL,1,0,qx->ses); md|=PIN_DERIVED; break;
				}
				if (rc==RC_OK && ty!=VT_ANY && ty!=to->type) {
					if (to->type==VT_ARRAY) for (ulong k=0; k<to->length; k++) {
						if (to->varray[k].type!=ty) {
							//if (to!=&w) {
								//copy
							//}
							if ((rc=convV(to->varray[k],*(Value*)&to->varray[k],ty,qx->ses))!=RC_OK) break;
						}
					} else if (to->type==VT_COLLECTION) {
						((Navigator*)to->nav)->setType(ty);
					} else if ((rc=convV(*to,*to,ty,qx->ses))!=RC_OK) continue;
					md|=PIN_DERIVED;
				}
				to->property=v.property;
				if (rc!=RC_OK) {
					if (rc==RC_NOTFOUND) rc=RC_OK; else {state|=QST_EOF; return rc;}
					if (re!=NULL && --re->nProperties==0 && re->properties!=NULL) {qx->ses->free(re->properties); re->properties=NULL;}
				} else if (re!=NULL) to++;
			}
			if (re!=NULL) {
				if (((re->mode|=md)&PIN_DERIVED)!=0) {re->id=PIN::defPID; re->epr.lref=0;} //else if (re->nProperties<ins[i]->getNProperties()) re->mode|=PIN_PROJECTED;
				if (re->nProperties==0) re->mode|=PIN_EMPTY;
			}
		}
		if (newV!=NULL) {
			for (unsigned i=0; i<nGroup; i++) freeV(*(Value*)&qx->vals[QV_GROUP].vals[i]);
			memcpy((Value*)qx->vals[QV_GROUP].vals,newV,nGroup*sizeof(Value));
		}
	}
	return RC_OK;
}

RC TransOp::rewind()
{
	RC rc=queryOp!=NULL?queryOp->rewind():RC_OK;
	if (rc==RC_OK) {
		state=state&~QST_EOF|QST_BOF;
		for (unsigned i=0; i<aggs.nValues; i++) ac[i].reset();
		for (unsigned j=0; j<nGroup; j++) {freeV(*(Value*)&qx->vals[QV_GROUP].vals[j]); ((Value*)&qx->vals[QV_GROUP].vals[j])->setError();}
	}
	return rc;
}

void TransOp::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("transform: ",11);
	for (unsigned i=0; i<nOuts; i++) {
		//?????
	}
	buf.append("\n",1); if (queryOp!=NULL) queryOp->print(buf,level+1);
}
