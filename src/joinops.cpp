/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "queryprc.h"
#include "idxcache.h"
#include "parser.h"
#include "expr.h"

using namespace MVStoreKernel;

MergeIDs::MergeIDs(QCtx *s,QueryOp **o,unsigned no,ulong qf,bool f) : QueryOp(s,qf|QO_JOIN|QO_IDSORT|QO_UNIQUE|QO_STREAM),fOr(f),nOps(no),cur(~0u)
{
	nOuts=1; for (unsigned i=0; i<no; i++) {ops[i].qop=o[i]; ops[i].state=QOS_ADV; ops[i].epr.lref=0;}
}

MergeIDs::~MergeIDs()
{
	for (ulong i=0; i<nOps; i++) delete ops[i].qop;
}

void MergeIDs::connect(PINEx **results,unsigned nRes)
{
	if (results!=NULL && nRes!=0) res=results[0];	///???
	for (ulong i=0; i<nOps; i++) if (ops[i].qop!=NULL) ops[i].qop->connect(results,nRes);	///???
}

RC MergeIDs::next(const PINEx *skip)
{
	RC rc=RC_OK; if ((state&QST_EOF)!=0) {res->cleanup(); return RC_EOF;}
	if ((state&QST_INIT)!=0) {state&=~QST_INIT; if (nSkip>0 && (rc=initSkip())!=RC_OK) return rc;}
	if (fOr) {
		ulong last=cur=~0u; int cmp=0;
		for (ulong i=0; i<nOps; i++) {
			QueryOpS& qs=ops[i]; if ((qs.state&QST_EOF)!=0) continue;
			if ((qs.state&QOS_ADV)!=0) {
				res->cleanup(); *res=PIN::defPID; last=~0u; qs.state&=~QOS_ADV;
				while ((rc=qs.qop->next(skip))==RC_OK /*&& skip!=NULL && skip->epr.cmp(res->id,skip->id,qx->ses->getStore()->storeID)<0*/) break;
				if (rc!=RC_OK) {qs.state|=QST_EOF; if (rc==RC_EOF) continue; return rc;}
				if (res->epr.lref==0 && (rc=res->pack())!=RC_OK) {res->cleanup(); *res=PIN::defPID; return rc;}
				last=i; qs.epr=res->epr; qs.epr.flags&=PINEX_LOCKED|PINEX_XLOCKED|PINEX_ACL_CHKED|PINEX_EXTPID;
			}
			if (cur==~0u || (cmp=ops[cur].epr.cmp(qs.epr))==0) {cur=i; qs.state|=QOS_ADV;}
			else if (cmp>0) {ops[cur].state&=~QOS_ADV; cur=i; qs.state|=QOS_ADV;}
		}
		if (cur==~0u) rc=RC_EOF;
		else {rc=RC_OK; if (cur!=last) {res->cleanup(); *res=PIN::defPID; res->epr=ops[cur].epr;}}
	} else {
		PINEx skp(qx->ses); if (cur!=~0u && (skip==NULL || ops[cur].epr.cmp(skip->epr)<=0)) {skp.epr=ops[cur].epr; skip=&skp;}
		for (ulong i=0,nOK=0,flags=0; nOK<nOps; ) {
			QueryOpS& qs=ops[i]; int cmp; res->cleanup(); if ((qs.state&QST_EOF)!=0) {rc=RC_EOF; break;}
			if ((rc=qs.qop->next(skip))!=RC_OK) {qs.state|=QST_EOF; break;}
			if (res->epr.lref==0 && res->pack()!=RC_OK) continue;
			if (cur==~0u||(cmp=res->epr.cmp(ops[cur].epr))>0) {cur=i; skp.epr=qs.epr=res->epr; skip=&skp; nOK=1;}
			else if (cmp==0) {nOK++; flags|=res->epr.flags&(PINEX_LOCKED|PINEX_XLOCKED|PINEX_ACL_CHKED|PINEX_EXTPID); res->epr.flags|=flags;}
			else continue;
			i=(i+1)%nOps;
		}
	}
	if (rc!=RC_OK) state|=QST_EOF;
	return rc;
}

RC MergeIDs::rewind()
{
	RC rc;
	for (ulong i=0; i<nOps; i++) if ((rc=ops[i].qop->rewind())!=RC_OK) return rc;
	cur=~0u; state=state&~QST_EOF|QST_BOF; return RC_OK;
}

RC MergeIDs::loadData(PINEx& qr,Value *pv,unsigned nv,ElementID eid,bool fSort,MemAlloc *ma)
{
	RC rc=RC_NOTFOUND;
	if (cur!=~0u) {
		if (fOr) rc=ops[cur].qop->getData(qr,pv,nv,NULL,eid,ma);
		else {
			// idxData?
		}
	}
	return RC_NOTFOUND;
}

void MergeIDs::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append(fOr?"union":"intersect",fOr?5:9); buf.append("\n",1);
	for (ulong i=0; i<nOps; i++) ops[i].qop->print(buf,level+1);
}

//------------------------------------------------------------------------------------------------

MergeOp::MergeOp(QueryOp *qop1,PropertyID pid1,QueryOp *qop2,PropertyID pid2,QUERY_SETOP qo,ulong qf)
	: QueryOp(qop1,qf|QO_JOIN|QO_UNIQUE),queryOp2(qop2),propID1(pid1),propID2(pid2),op(qo),didx(0),pexR(qx->ses),pR(&pexR),pids(NULL)
{
	const ulong qf1=qop1->getQFlags(); qflags|=qf1&QO_IDSORT;
	vals[0].setError(),vals[1].setError(),vals[2].setError(); if (qo==QRY_EXCEPT) nOuts=1; else nOuts+=qop2->getNOuts();
	if ((qf1&qop2->getQFlags()&QO_UNIQUE)!=0 && propID2==PROP_SPEC_PINID) qflags|=QO_UNIQUE;
	sort=qop1->getSort(nSegs);
	// props, nProps, qf1&QO_ALLPROPS
}

MergeOp::~MergeOp()
{
	if (pids!=NULL) delete pids;
	freeV(vals[0]); freeV(vals[1]); freeV(vals[2]);
	delete queryOp2;
}

void MergeOp::connect(PINEx **results,unsigned nRes)
{
	if (results!=NULL && nRes!=0) {
		res=results[0]; unsigned nOuts1=queryOp->getNOuts();
		queryOp->connect(results,nRes>nOuts1?nOuts1:nRes);
		if (nRes==1) queryOp2->connect(&pR);
		else if (nOuts1+queryOp2->getNOuts()<=nRes) {
			queryOp2->connect(results+nOuts1,nRes-nOuts1); pR=results[nOuts1];	//???
		} else {
			//???
		}
	}
}

RC MergeOp::next(const PINEx *skip)
{
	RC rc; PID id; if ((state&QST_EOF)!=0) return RC_EOF;
	if ((state&QST_INIT)!=0) {
		state&=~QST_INIT; if (nSkip>0 && (rc=initSkip())!=RC_OK) {state|=QST_EOF; return rc;}	// ???
		state|=QOS_ADV1|QOS_ADV2|QST_BOF; didx=~0u;
	}
	for (res->cleanup();;) {
		if ((state&QOS_EOF1)!=0) return RC_EOF;	// if not right/full outer
		if ((state&QOS_ADV1)!=0) {
			freeV(vals[0]); vals[0].setError(propID1); res->epr.flags&=~PINEX_RLOAD; pR->epr.flags|=PINEX_RLOAD; state&=~QST_BOF;
			if ((rc=queryOp->next(skip))!=RC_OK) {
				state=state&~QOS_ADV1|QOS_EOF1|QOS_EOF2; return rc;		// if not right/full outer
			} else if ((state&QOS_EOF2)==0) {
				state&=~QOS_ADV1;
				if (propID1==PROP_SPEC_PINID) {
					if ((rc=res->getID(id))==RC_OK) vals[0].set(id); else return rc;
				} else if (queryOp->getData(*res,&vals[0],1,pR)!=RC_OK) {
					if (rc!=RC_NOACCESS && rc!=RC_DELETED && rc!=RC_NOTFOUND) return rc;
					state|=QOS_ADV1; continue;
				}
			}
		}
		if ((state&QOS_EOF2)!=0) return op==QRY_EXCEPT?RC_OK:RC_EOF;	// if not left/full outer
		if ((state&QOS_ADV2)!=0) {
			freeV(vals[1]); vals[1].setError(propID2); res->epr.flags|=PINEX_RLOAD; pR->epr.flags&=~PINEX_RLOAD;
			if ((rc=queryOp2->next(skip))!=RC_OK) {
				state=state&~QOS_ADV2|QOS_EOF2; if (rc!=RC_EOF) {state|=QOS_EOF1; return rc;}
				if ((state&QOS_EOF1)!=0 || op!=QRY_EXCEPT) return RC_EOF;
				state|=QOS_ADV1; return RC_OK;
			}
			state&=~QOS_ADV2; assert((state&QOS_EOF1)==0);
			if (propID2==PROP_SPEC_PINID) {
				if ((rc=pR->getID(id))==RC_OK) vals[1].set(id); else return rc;
			} else if (queryOp2->getData(*pR,&vals[1],1)!=RC_OK) {
				if (rc!=RC_NOACCESS && rc!=RC_DELETED && rc!=RC_NOTFOUND) return rc;
				state|=QOS_ADV2; continue;
			}
		}
		assert((state&(QOS_ADV1|QOS_ADV2|QOS_ADVN))==0);
		int c=cmp(vals[0],vals[1],CND_SORT);
#if 0
		//if (c==0) {
			SOutCtx out(qx->ses); PID id2; pR->getID(id2);
			if (vals[0].type==VT_BSTR) vals[0].type=VT_STRING;
			if (vals[1].type==VT_BSTR) vals[1].type=VT_STRING;
			out.renderPID(saveID); if (propID1!=PROP_SPEC_PINID) {out.append(":",1); out.renderValue(vals[0]);}
			out.append(c<0?" < ":c==0?" = ":" > ",3);
			out.renderPID(id2); if (propID2!=PROP_SPEC_PINID) {out.append(":",1); out.renderValue(vals[1]);}
			size_t l; byte *p=out.result(l); report(MSG_DEBUG,"%.*s\n",l,p); qx->ses->free(p);
		//}
#endif
		if (c>0) state|=QOS_ADV2; 
		else if (c<0) {state|=QOS_ADV1; if (op==QRY_EXCEPT) return RC_OK;}
		else {
			if ((qflags&QO_SEMIJOIN)!=0) state|=(qflags&(QO_UNI1|QO_UNI2))==(QO_UNI1|QO_UNI2)?QOS_ADV1|QOS_ADV2:QOS_ADV1;
			else switch (qflags&(QO_UNI1|QO_UNI2)) {
			case QO_UNI1|QO_UNI2: state|=QOS_ADV1|QOS_ADV2; break;
			case QO_UNI1: state|=QOS_ADV2; break;
			case QO_UNI2: state|=QOS_ADV1; break;
			case 0:
				state|=QOS_ADV2;
#if 0
				if ((state&QOS_NEXT)==0) {
					vals[2].setError(propID1); res->moveTo(saveR);
					while ((rc=queryOp->next(skip))==RC_OK) {
						if (propID1==PROP_SPEC_PINID) {
							if ((rc=res->getID(id))!=RC_OK) return rc;
							vals[2].set(id); break;
						}
						if (queryOp->getData(*res,&vals[2],1)==RC_OK) break;
						if (rc!=RC_NOACCESS && rc!=RC_DELETED && rc!=RC_NOTFOUND) return rc;
					}
					if (rc==RC_OK) {if (cmp(vals[0],vals[2],CND_SORT)!=0) state|=QOS_NEXT; else state=state&~QOS_ADV2|QOS_ADVN;}
					PINEx tmp(qx->ses); res->moveTo(tmp); saveR.moveTo(*res); tmp.moveTo(saveR);
				}
#endif
				break;
			}
			if (op!=QRY_EXCEPT) {
				if (res->epr.lref!=0 && PINRef::isColl(res->epr.buf,res->epr.lref) && op<QRY_UNION && (qflags&QO_UNIQUE)!=0) {
					if (pids!=NULL) {if ((*pids)[*res]) continue;}
					else if ((pids=new(qx->ses) PIDStore(qx->ses))==NULL) return RC_NORESOURCES;
					if ((rc=((*pids)+=*res))!=RC_OK) return rc;
					PINRef::changeFColl(res->epr.buf,res->epr.lref,false);
				}
				res->epr.flags|=PINEX_RLOAD; pR->epr.flags|=PINEX_RLOAD; return RC_OK;
			}
		}
	}
}

RC MergeOp::rewind()
{
	if ((state&QST_BOF)!=0) return RC_OK;
	if (pids!=NULL) {delete pids; pids=NULL;}
	freeV(vals[0]); freeV(vals[1]); freeV(vals[2]);
	vals[0].setError(); vals[1].setError(); vals[2].setError();
	pR->cleanup();
	RC rc=queryOp!=NULL?queryOp->rewind():RC_EOF;
	if (rc==RC_OK) rc=queryOp2!=NULL?queryOp2->rewind():RC_EOF;
	state=rc==RC_OK?QST_BOF|QOS_ADV1|QOS_ADV2:QST_EOF|QOS_EOF1|QOS_EOF2;
	return rc;
}

RC MergeOp::loadData(PINEx& qr,Value *pv,unsigned nv,ElementID eid,bool fSort,MemAlloc *ma)
{
	if (didx!=~0u) {
		// return from vals[0] or vals[2] and, if required from vals[1]
	}
	return RC_NOTFOUND;
}

void MergeOp::unique(bool f)
{
	if (queryOp!=NULL) queryOp->unique(f); 
	if (f) qflags|=QO_UNIQUE; else {qflags&=~QO_UNIQUE; delete pids; pids=NULL;}
}

void MergeOp::print(SOutCtx& buf,int level) const
{
	const static char *ops[]={"semi-join", "join", "left-outer-join", "right-outer-join", "full-outer-join", "union", "except", "intersect"};
	buf.fill('\t',level); buf.append(ops[op-QRY_SEMIJOIN],strlen(ops[op-QRY_SEMIJOIN]));
	if (op!=QRY_EXCEPT) {
		buf.append(" on ",4); buf.renderName(propID1);
		buf.append(" = ",3); buf.renderName(propID2);
	}
	buf.append("\n",1);
	if (queryOp!=NULL) queryOp->print(buf,level+1);
	if (queryOp2!=NULL) queryOp2->print(buf,level+1);
}

//------------------------------------------------------------------------------------------------

HashOp::~HashOp()
{
	delete queryOp2; delete pids;
}

RC HashOp::next(const PINEx *skip)
{
	RC rc=RC_OK;
	if ((state&QST_INIT)!=0) {
		state&=~QST_INIT; assert(pids==NULL);
		if (queryOp2!=NULL) {
			PINEx qr(qx->ses),*pqr=&qr; queryOp2->connect(&pqr);
			for (PINEx qr2(qx->ses); (rc=queryOp2->next())==RC_OK; ) {
				if (pids==NULL && (pids=new(qx->ses) PIDStore(qx->ses))==NULL) return RC_NORESOURCES;
				(*pids)+=qr;
			}
		}
		if (rc!=RC_EOF) {delete pids; pids=NULL; return rc;}
		if (pids==NULL) return RC_EOF;
		if (nSkip>0 && (rc=initSkip())!=RC_OK) return rc;
	} else if (pids==NULL) return RC_EOF;
	if ((state&QST_EOF)!=0) rc=RC_EOF;
	else do if ((rc=queryOp->next(skip))==RC_OK) skip=NULL; else {state|=QST_EOF; break;} while (!(*pids)[*res]);
	return rc;
}

void HashOp::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("lookup\n",7);
	if (queryOp!=NULL) queryOp->print(buf,level+1);
	if (queryOp2!=NULL) queryOp2->print(buf,level+1);
}

//------------------------------------------------------------------------------------------------

NestedLoop::~NestedLoop()
{
	delete queryOp2;
	if (conds!=NULL) {
		if (nConds==1) cond->destroy();
		else {
			for (ulong i=0; i<nConds; i++) conds[i]->destroy();
			qx->ses->free(conds);
		}
	}
}

void NestedLoop::connect(PINEx **results,unsigned nRes)
{
	if (results!=NULL && nRes!=0) {
		res=results[0]; unsigned nOuts1=queryOp->getNOuts();
		queryOp->connect(results,nRes>nOuts1?nOuts1:nRes);
		if (nRes==1) queryOp2->connect(&pR);
		else if (nOuts1+queryOp2->getNOuts()<=nRes) {
			queryOp2->connect(results+nOuts1,nRes-nOuts1); pR=results[nOuts1];	//???
		} else {
			//???
		}
	}
}

RC NestedLoop::next(const PINEx *skip)
{
	RC rc; if ((state&QST_EOF)!=0) return RC_EOF;
	if ((state&QST_INIT)!=0) {
		state=state&~QST_INIT|QST_BOF|QOS_ADV1;
		if (nSkip>0) {
			// ???
		}
	}
	if (res!=NULL) res->cleanup();
	for (;;state|=QOS_ADV1) {
		if ((state&QOS_ADV1)!=0) {
			state&=~QOS_ADV1;
			if ((rc=queryOp->next())==RC_OK) {
				//...
				// rc=getData(*res,...);
			}
			if (rc!=RC_OK) {state|=QST_EOF; return rc;}
			// locking
			if ((state&QST_BOF)!=0) state&=~QST_BOF; else if ((rc=queryOp2->rewind())!=RC_OK) {state|=QST_EOF; return rc;}
		}
		while ((rc=queryOp2->next())!=RC_EOF) {
			if (rc==RC_OK) {
				//...
				//rc=getData()
			}
			if (rc!=RC_OK) {state|=QST_EOF; return rc;} PINEx *pp[2]={res,pR}; 
			if (conds==NULL || Expr::condSatisfied((const Expr* const*)(nConds>1?conds:&cond),nConds,pp,2,NULL,0,qx->ses)) return RC_OK;
		}
	}
}

RC NestedLoop::rewind()
{
	RC rc;
	if (queryOp!=NULL && (rc=queryOp->rewind())!=RC_OK) return rc;
	if (queryOp2!=NULL && (rc=queryOp2->rewind())!=RC_OK) return rc;
	state=rc==RC_OK?QST_BOF|QOS_ADV1|QOS_ADV2:QST_EOF|QOS_EOF1|QOS_EOF2;		//???
	return RC_OK;
}

void NestedLoop::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("nested loop\n",12);
	if (queryOp!=NULL) queryOp->print(buf,level+1);
	if (queryOp2!=NULL) queryOp2->print(buf,level+1);
}
