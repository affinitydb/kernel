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

Written by Mark Venguerov 2010 - 2012

**************************************************************************************/

#include "maps.h"
#include "queryprc.h"
#include "parser.h"
#include "expr.h"
#include "blob.h"

using namespace AfyKernel;

TransOp::TransOp(QueryOp *q,const ValueV *d,unsigned nD,const ValueV& ag,const OrderSegQ *gs,unsigned nG,const Expr *hv,unsigned qf) 
	: QueryOp(q,qf|(q->getQFlags()&(QO_UNIQUE|QO_IDSORT|QO_REVERSIBLE))|QO_ALLPROPS),dscr(d),ins(NULL),nIns(0),qr(qx->ses),pqr(&qr),res(NULL),
	nRes(0),aggs(ag),groupSeg(gs),nGroup(nG),having(hv),ac(NULL),ectx(q->getQCtx()->ses)
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

TransOp::TransOp(QCtx *qc,const ValueV *d,unsigned nD,unsigned qf) 
	: QueryOp(qc,qf|QO_UNIQUE|QO_ALLPROPS),dscr(d),ins(NULL),nIns(0),qr(qc->ses),pqr(&qr),res(NULL),nRes(0),groupSeg(NULL),nGroup(0),having(NULL),ac(NULL),ectx(qc->ses)
{
	nOuts=nD!=0?nD:1;
}

TransOp::~TransOp()
{
	if (ac!=NULL) for (unsigned j=0; j<aggs.nValues; j++) if (ac[j].hist!=NULL) {ac[j].hist->~Histogram(); qx->ses->free(ac[j].hist);}
	if ((qflags&QO_VCOPIED)!=0) {
		if (dscr!=NULL) {
			for (unsigned i=0; i<nOuts; i++) if (dscr[i].vals!=NULL && dscr[i].fFree) freeV((Value*)dscr[i].vals,dscr[i].nValues,qx->ses);
			qx->ses->free((void*)dscr);
		}
		if (groupSeg!=NULL) {
			for (unsigned i=0; i<nGroup; i++) if ((groupSeg[i].flags&ORDER_EXPR)!=0) groupSeg[i].expr->destroy();
			qx->ses->free((void*)groupSeg); if (having!=NULL) qx->ses->free((Expr*)having);
		}
		if (aggs.vals!=NULL && aggs.fFree) freeV((Value*)aggs.vals,aggs.nValues,qx->ses);
	}
	if (nIns>1) {
		for (unsigned i=1; i<nIns; i++) if (ins[i]!=NULL) {ins[i]->~PINx(); qx->ses->free(ins[i]);}
		qx->ses->free(ins);
	}
}

void TransOp::connect(PINx **results,unsigned nr)
{
	res=results; nRes=nr;
	if (queryOp!=NULL && (nIns=queryOp->getNOuts())!=0) {
		if ((qflags&QO_AUGMENT)!=0) {nIns=0; queryOp->connect(results,nr);}
		else if (nIns==1) queryOp->connect(ins=&pqr);
		else if ((ins=new(qx->ses) PINx*[nIns])==NULL) return;	//???
		else {
			memset(ins,0,nIns*sizeof(PINx*)); ins[0]=pqr;
			for (unsigned i=1; i<nIns; i++) if ((ins[i]=new(qx->ses) PINx(qx->ses))==NULL) return;		//???
			queryOp->connect(ins,nIns);
		}
	}
}

RC TransOp::init()
{
	if (aggs.nValues!=0) {
		if ((qx->vals[QV_AGGS].vals=new(qx->ses) Value[qx->vals[QV_AGGS].nValues=aggs.nValues])==NULL) {state|=QST_EOF; return RC_NORESOURCES;}
		qx->vals[QV_AGGS].fFree=true; memset((Value*)qx->vals[QV_AGGS].vals,0,aggs.nValues*sizeof(Value));
		if ((ac=new(qx->ses) AggAcc[aggs.nValues])==NULL) {state|=QST_EOF; return RC_NORESOURCES;}
		for (unsigned i=0; i<aggs.nValues; i++) {
			*const_cast<EvalCtx**>(&ac[i].ctx)=&ectx; ac[i].op=(ExprOp)aggs.vals[i].op;	ac[i].ma=qx->ses;												// flags???
			if (aggs.vals[i].op==OP_HISTOGRAM && (ac[i].hist=new(qx->ses) Histogram(*qx->ses,0))==NULL) {state|=QST_EOF; return RC_NORESOURCES;}		// flags ???
		}
	}
	if (nGroup!=0) {
		if ((qx->vals[QV_GROUP].vals=new(qx->ses) Value[qx->vals[QV_GROUP].nValues=nGroup])==NULL) {state|=QST_EOF; return RC_NORESOURCES;}
		qx->vals[QV_GROUP].fFree=true; memset((Value*)qx->vals[QV_GROUP].vals,0,nGroup*sizeof(Value));
	}
	return RC_OK;
}

RC TransOp::advance(const PINx *)
{
	RC rc=RC_OK; Value *newV=NULL;
	if (queryOp==NULL) state|=QST_EOF;
	else for (;;) {
		if ((rc=queryOp->next())!=RC_OK) {
			state|=QST_EOF; if (rc!=RC_EOF || nGroup+aggs.nValues==0 || (state&QST_BOF)!=0) return rc;
			for (unsigned i=0; i<aggs.nValues; i++) {
				if (nGroup!=0) freeV(*(Value*)&qx->vals[QV_AGGS].vals[i]);
				if ((rc=ac[i].result(*(Value*)&qx->vals[QV_AGGS].vals[i]))!=RC_OK) return rc;
			}
			if (having!=NULL && !Expr::condSatisfied(&having,1,EvalCtx(qx->ses,NULL,0,(PIN**)res,nRes,qx->vals,QV_ALL))) return RC_EOF;
			rc=RC_OK; newV=NULL; break;
		}
		bool fRepeat=false;
		if (nGroup!=0) {
			fRepeat=true; assert(qx->vals[QV_GROUP].vals!=NULL);
			// QO_LOADALL ???
			if ((state&QST_BOF)!=0) {if ((rc=queryOp->loadData(*ins[0],(Value*)qx->vals[QV_GROUP].vals,nGroup,STORE_COLLECTION_ID,true))!=RC_OK) {state|=QST_EOF; return rc;}}
			else if (newV==NULL && (newV=(Value*)alloca(nGroup*sizeof(Value)))==NULL) {state|=QST_EOF; return RC_NORESOURCES;}
			else if ((rc=queryOp->loadData(*ins[0],newV,nGroup,STORE_COLLECTION_ID,true))!=RC_OK) {state|=QST_EOF; return rc;}
			else for (unsigned i=0; i<nGroup; i++) if (cmp(qx->vals[QV_GROUP].vals[i],newV[i],groupSeg[i].flags,qx->ses)!=0) {fRepeat=false; break;}
		}
		if (dscr!=NULL&&!fRepeat || ac!=NULL) {
			if ((qflags&QO_AUGMENT)!=0) {for (unsigned i=0; i<nRes; i++) if ((rc=res[i]->load(LOAD_SSV))!=RC_OK) {state|=QST_EOF; return rc;}}
			else for (unsigned i=0; i<nIns; i++) if ((rc=(qflags&QO_LOADALL)!=0?ins[i]->load(LOAD_SSV):getData(*ins[i],NULL,0))!=RC_OK) {state|=QST_EOF; return rc;}
		}
		for (unsigned i=0; i<aggs.nValues; i++) {
			if (nGroup!=0 && !fRepeat) {
				freeV(*(Value*)&qx->vals[QV_AGGS].vals[i]);
				if ((rc=ac[i].result(*(Value*)&qx->vals[QV_AGGS].vals[i],true))!=RC_OK) {state|=QST_EOF; return rc;}
			}
			const Value &v=aggs.vals[i]; Value w; w.setError();
			if (v.type==VT_VARREF) {
				if (v.refV.refN>=nIns) continue; assert((v.refV.flags&VAR_TYPE_MASK)==0 && v.length!=0);
				rc=ins[v.refV.refN]->getV(v.refV.id,w,LOAD_SSV,qx->ses,v.eid<STORE_ALL_ELEMENTS||v.eid>=STORE_FIRST_ELEMENT?v.eid:STORE_COLLECTION_ID);
			} else if (v.type==VT_EXPR && v.fcalc!=0) rc=Expr::eval((const Expr**)&v.expr,1,w,EvalCtx(qx->ses,NULL,0,(PIN**)ins,nIns,qx->vals,QV_ALL));
			else if (v.type!=VT_ANY) {w=v; setHT(w);}
			if (rc==RC_OK) rc=ac[i].process(w); freeV(w);
			if (rc==RC_NOTFOUND) rc=RC_OK; else if (rc!=RC_OK) {state|=QST_EOF; return rc;}
		}
		state&=~QST_BOF;
		if (nGroup!=0) {
			if (fRepeat) {if (newV!=NULL) for (unsigned i=0; i<nGroup; i++) freeV(newV[i]);}
			else if (having==NULL || Expr::condSatisfied(&having,1,EvalCtx(qx->ses,NULL,0,(PIN**)res,nRes,qx->vals,QV_ALL))) break;
			else {for (unsigned i=0; i<nGroup; i++) freeV(*(Value*)&qx->vals[QV_GROUP].vals[i]); memcpy((Value*)qx->vals[QV_GROUP].vals,newV,nGroup*sizeof(Value));}
		} else if (ac==NULL) break;
	}
	if (dscr==NULL) {
		assert(nGroup!=0);
		if (res!=NULL && res[0]!=NULL) {
			res[0]->setProps(qx->vals[QV_GROUP].vals,nGroup,0);
			if (newV==NULL) {qx->vals[QV_GROUP].vals=NULL; qx->vals[QV_GROUP].fFree=false;}
			else if ((qx->vals[QV_GROUP].vals=new(qx->ses) Value[nGroup])==NULL) {state|=QST_EOF; return RC_NORESOURCES;}
		} else if (newV!=NULL) for (unsigned i=0; i<nGroup; i++) freeV(*(Value*)&qx->vals[QV_GROUP].vals[i]);
		if (newV!=NULL) memcpy((Value*)qx->vals[QV_GROUP].vals,newV,nGroup*sizeof(Value));
		return RC_OK;
	}
	for (unsigned i=0; i<nOuts; i++) {
		const ValueV &td=dscr[i]; PINx *re=i<nRes?res[i]:(PINx*)0; Value w,*to=&w;
		if (re!=NULL && (qflags&QO_AUGMENT)==0) {
			if (re->properties==NULL || re->fNoFree!=0) {
				if ((re->properties=(Value*)qx->ses->malloc(td.nValues*sizeof(Value)))!=NULL) re->fNoFree=0; else {rc=RC_NORESOURCES; break;}
			} else {
				for (unsigned k=0; k<re->nProperties; k++) freeV(*(Value*)&re->properties[k]);
				if (re->nProperties<td.nValues && (re->properties=(Value*)qx->ses->realloc((void*)re->properties,td.nValues*sizeof(Value)))==NULL) {rc=RC_NORESOURCES; break;}
			}
			re->id=PIN::noPID; re->epr.flags=re->epr.flags&~(PINEX_LOCKED|PINEX_XLOCKED|PINEX_ACL_CHKED|PINEX_ADDRSET)|PINEX_DERIVED; re->fPartial=0; re->nProperties=td.nValues; to=re->properties;
		}
		for (unsigned j=0; j<td.nValues; j++) {
			const Value& v=td.vals[j],*cv; Value w; ValueType ty=VT_ANY; ExprOp op=OP_SET; TIMESTAMP ts; ushort vty;
			switch (v.type) {
			case VT_VARREF:
				vty=v.refV.flags&VAR_TYPE_MASK;
				if (vty==VAR_CTX && (qflags&QO_AUGMENT)!=0) continue;
				if (vty!=0) {
					const ValueV *vals=&qx->vals[(vty>>13)-1];
					if (v.refV.refN<vals->nValues) rc=copyV(vals->vals[v.refV.refN],*to,qx->ses); else rc=RC_NOTFOUND;
				} else if (v.length!=0 && v.refV.id==PROP_SPEC_SELF && (qflags&QO_AUGMENT)!=0) continue;
				else if (v.refV.refN>=nIns) rc=RC_NOTFOUND;
				else if (v.length==0) {
					//???
				} else if (v.refV.id==PROP_SPEC_PINID && (v.property==PROP_SPEC_PINID || v.property==STORE_INVALID_URIID) && re!=NULL && (qflags&QO_AUGMENT)==0) {
					re->id=ins[v.refV.refN]->id; if (--re->nProperties==0) {qx->ses->free((void*)re->properties); re->properties=NULL;} continue;
				} else 
					rc=ins[v.refV.refN]->getV(v.refV.id,*to,LOAD_SSV,qx->ses,v.eid<STORE_ALL_ELEMENTS||v.eid>=STORE_FIRST_ELEMENT?v.eid:STORE_COLLECTION_ID);
				ty=(ValueType)v.refV.type; break;
			case VT_CURRENT:
				switch (v.i) {
				default: rc=RC_CORRUPTED; break;
				case CVT_TIMESTAMP: getTimestamp(ts); to->setDateTime(ts); break;
				case CVT_USER: to->setIdentity(qx->ses->getIdentity()); break;
				case CVT_STORE: to->set((unsigned)qx->ses->getStore()->storeID); break;
				}
				break;
			case VT_EXPR:
				if (v.fcalc!=0) {
					EvalCtx ectx2(qx->ses,NULL,0,(qflags&QO_AUGMENT)!=0?(PIN**)&re:(PIN**)ins,(qflags&QO_AUGMENT)!=0?1:nIns,qx->vals,QV_ALL);
					rc=Expr::eval((const Expr**)&v.expr,1,*to,ectx2); break;
				}
			default: *to=v; setHT(*to); break;
			}
			if (rc==RC_OK && (to->type==VT_ARRAY || to->type==VT_COLLECTION)) switch (v.eid) {
			case STORE_COLLECTION_ID: break;
			case STORE_FIRST_ELEMENT:
			case STORE_LAST_ELEMENT:
				cv=to->type==VT_ARRAY?&to->varray[v.eid==STORE_FIRST_ELEMENT?0:to->length-1]:
					to->nav->navigate(v.eid==STORE_FIRST_ELEMENT?GO_FIRST:GO_LAST);
				if (cv==NULL) rc=RC_NOTFOUND; else if ((rc=copyV(*cv,w,qx->ses))==RC_OK) {freeV(*to); *to=w;}
				break;
			case STORE_SUM_COLLECTION: op=OP_PLUS; goto aggregate;
			case STORE_AVG_COLLECTION: op=OP_AVG; goto aggregate;
			case STORE_MIN_ELEMENT: op=OP_MIN; goto aggregate;
			case STORE_MAX_ELEMENT: op=OP_MAX;
			aggregate: rc=Expr::calcAgg(op,*to,NULL,1,0,qx->ses); break;
			case STORE_CONCAT_COLLECTION: op=OP_CONCAT; goto aggregate;
			}
			if (rc==RC_OK && ty!=VT_ANY && ty!=to->type) {
				if (to->type==VT_ARRAY) for (unsigned k=0; k<to->length; k++) {
					if (to->varray[k].type!=ty) {
						//if (to!=&w) {
							//copy
						//}
						if ((rc=convV(to->varray[k],*(Value*)&to->varray[k],ty,qx->ses))!=RC_OK) break;
					}
				} else if (to->type==VT_COLLECTION) {
					((Navigator*)to->nav)->setType(ty);
				} else if ((rc=convV(*to,*to,ty,qx->ses))!=RC_OK) continue;
			}
			to->property=v.property;
			if (rc!=RC_OK) {
				if (rc==RC_NOTFOUND) rc=RC_OK; else {state|=QST_EOF; return rc;}
				if ((qflags&QO_AUGMENT)==0 && re!=NULL && --re->nProperties==0 && re->properties!=NULL) {qx->ses->free(re->properties); re->properties=NULL;}
			} else if (re!=NULL) {
				if ((qflags&QO_AUGMENT)==0) to++; else {to->op=OP_ADD; rc=re->modify(to,STORE_LAST_ELEMENT,STORE_COLLECTION_ID,0,qx->ses);}
			}
		}
		if (re!=NULL && (qflags&QO_AUGMENT)==0) {
			//if (((re->mode|=md)&PIN_DERIVED)!=0) {re->id=PIN::noPID; re->epr.lref=0;} else if (re->nProperties<ins[i]->getNProperties()) re->mode|=PIN_PARTIAL;
			if (re->nProperties==0) re->fPartial=0;
		}
	}
	if (newV!=NULL) {
		for (unsigned i=0; i<nGroup; i++) freeV(*(Value*)&qx->vals[QV_GROUP].vals[i]);
		memcpy((Value*)qx->vals[QV_GROUP].vals,newV,nGroup*sizeof(Value));
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
