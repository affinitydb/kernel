/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "queryprc.h"
#include "idxcache.h"
#include "parser.h"
#include "expr.h"

using namespace MVStoreKernel;

MergeIDOp::~MergeIDOp()
{
	for (ulong i=0; i<nOps; i++) delete ops[i].qop;
}

void MergeIDOp::connect(PINEx **results,unsigned nRes)
{
	if (results!=NULL && nRes!=0) res=results[0];	///???
	for (ulong i=0; i<nOps; i++) if (ops[i].qop!=NULL) ops[i].qop->connect(results,nRes);	///???
}

RC MergeIDOp::next(const PINEx *skip)
{
	RC rc=RC_OK;
	if ((state&QST_INIT)!=0) {state&=~QST_INIT; if (nSkip>0 && (rc=initSkip())!=RC_OK) return rc;}
	if ((state&QOS_EOF)==0) state&=~QOS_FIRST; else {res->cleanup(); return RC_EOF;}
	if (fOr) {
		ulong last=cur=~0u; int cmp=0;
		for (ulong i=0; i<nOps; i++) {
			QueryOpS& qs=ops[i]; if ((qs.state&QOS_EOF)!=0) continue;
			if ((qs.state&QOS_ADV)!=0) {
				res->cleanup(); *res=PIN::defPID; last=~0u; qs.state&=~QOS_ADV;
				while ((rc=qs.qop->next(skip))==RC_OK /*&& skip!=NULL && skip->epr.cmp(res->id,skip->id,ses->getStore()->storeID)<0*/) break;
				qs.qop->release();
				if (rc!=RC_OK) {qs.state|=QOS_EOF; if (rc==RC_EOF) continue; return rc;}
				if (res->epr.lref==0 && (rc=res->pack())!=RC_OK) {res->cleanup(); *res=PIN::defPID; return rc;}
				last=i; qs.epr=res->epr; qs.epr.flags&=PINEX_LOCKED|PINEX_XLOCKED|PINEX_ACL_CHKED|PINEX_EXTPID;
			}
			if (cur==~0u || (cmp=ops[cur].epr.cmp(qs.epr))==0) {cur=i; qs.state|=QOS_ADV;}
			else if (cmp>0) {ops[cur].state&=~QOS_ADV; cur=i; qs.state|=QOS_ADV;}
		}
		if (cur==~0u) rc=RC_EOF;
		else {rc=RC_OK; if (cur!=last) {res->cleanup(); *res=PIN::defPID; res->epr=ops[cur].epr;}}
	} else {
		PINEx skp(ses); if (cur!=~0u && (skip==NULL || ops[cur].epr.cmp(skip->epr)<=0)) {skp.epr=ops[cur].epr; skip=&skp;}
		for (ulong i=0,nOK=0,flags=0; nOK<nOps; ) {
			QueryOpS& qs=ops[i]; int cmp; res->cleanup(); if ((qs.state&QOS_EOF)!=0) {rc=RC_EOF; break;}
			if ((rc=qs.qop->next(skip))!=RC_OK) {qs.state|=QOS_EOF; qs.qop->release(); break;}
			if (res->epr.lref==0 && res->pack()!=RC_OK) continue;
			if (cur==~0u||(cmp=res->epr.cmp(ops[cur].epr))>0) {cur=i; skp.epr=qs.epr=res->epr; skip=&skp; nOK=1;}
			else if (cmp==0) {nOK++; flags|=res->epr.flags&(PINEX_LOCKED|PINEX_XLOCKED|PINEX_ACL_CHKED|PINEX_EXTPID); res->epr.flags|=flags;}
			else continue;
			qs.qop->release(); i=(i+1)%nOps;
		}
	}
	if (rc!=RC_OK) {state|=QOS_EOF; release();}
	return rc;
}

RC MergeIDOp::rewind()
{
	RC rc;
	for (ulong i=0; i<nOps; i++) if ((rc=ops[i].qop->rewind())!=RC_OK) return rc;
	cur=~0u; state=state&~QOS_EOF|QOS_FIRST; return RC_OK;
}

RC MergeIDOp::loadData(PINEx& qr,Value *pv,unsigned nv,MemAlloc *ma,ElementID eid)
{
	RC rc=RC_NOTFOUND;
	if (cur!=~0u) {
		if (fOr) rc=ops[cur].qop->getData(qr,pv,nv,NULL,ma,eid);
		else {
			// idxData?
		}
	}
	return RC_NOTFOUND;
}

void MergeIDOp::reorder(bool)
{
}

RC MergeIDOp::release()
{
	RC rc=RC_OK; for (ulong i=0; i<nOps; i++) if ((rc=ops[i].qop->release())!=RC_OK) break; return rc;
}

void MergeIDOp::getOpDescr(QODescr& qop)
{
	qop.flags=QO_JOIN|QO_PIDSORT|QO_UNIQUE|QO_DEGREE|QO_STREAM;
	for (ulong i=0; i<nOps; i++) {
		QODescr qop1; ops[i].qop->getOpDescr(qop1);
		qop.flags&=qop1.flags&(QO_DEGREE|QO_STREAM)|~(QO_DEGREE|QO_STREAM);
		if (qop1.level+1>qop.level) qop.level=qop1.level+1;
		if (qop1.props!=NULL && qop1.nProps!=0) {
			if (fOr) {
				//...
			} else {
				//...
			}
		}
	}
}

void MergeIDOp::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append(fOr?"union":"intersect",fOr?5:9); buf.append("\n",1);
	for (ulong i=0; i<nOps; i++) ops[i].qop->print(buf,level+1);
}

//------------------------------------------------------------------------------------------------

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
		if (nRes==1) queryOp2->connect(pR);
		else if (nOuts1+queryOp2->getNOuts()<=nRes) {
			queryOp2->connect(results+nOuts1,nRes-nOuts1); pR=results[nOuts1];	//???
		} else {
			//???
		}
	}
}

RC MergeOp::next(const PINEx *skip)
{
	RC rc; bool fQ1=false,fQ2=false;
	if ((state&QST_INIT)!=0) {
		state&=~QST_INIT; if (nSkip>0 && (rc=initSkip())!=RC_OK) {state|=QOS_EOF; return rc;}	// ???
		state|=QOS_ADV1|QOS_ADV2; saveID=PIN::defPID; saveEPR.flags=0; saveEPR.lref=0; didx=~0u;
	}
	for (res->cleanup(),*res=saveID,res->epr=saveEPR;;) {
		if ((state&QOS_ADVN)!=0) {
			freeV(vals[0]); vals[0]=vals[2]; vals[2].setError(); *res=saveR; state&=~QOS_ADVN;
		} else if ((state&QOS_EOF1)!=0) return RC_EOF;	// if not right outer
		else if ((state&QOS_ADV1)!=0) {
			pR->release(); queryOp2->release(); fQ1=true;
			freeV(vals[0]); vals[0].setError(propID1);
			if ((rc=queryOp->next(skip))!=RC_OK) {
				state=state&~QOS_ADV1|QOS_EOF1|QOS_EOF2; return rc;		// if not right outer
			} else if ((state&QOS_EOF2)==0) {
				state&=~QOS_ADV1;
				if (propID1==PROP_SPEC_PINID) {
					if (res->id.pid==STORE_INVALID_PID && (rc=res->unpack())!=RC_OK) return rc;
					vals[0].set(res->id);
				} else if (queryOp->getData(*res,&vals[0],1,pR)!=RC_OK) {
					if (rc!=RC_DEADLOCK && rc!=RC_SHUTDOWN) {state|=QOS_ADV1; continue;}		// RC_NOACCESS || RC_DELETED || RC_NOTFOUND
					return rc;
				}
			}
			saveID=res->id; saveEPR=res->epr;
		}
		if ((state&QOS_EOF2)!=0) return op==QRY_EXCEPT?RC_OK:RC_EOF;	// if not left outer
		if ((state&QOS_ADV2)!=0) {
			assert((state&QOS_EOF1)==0);		//???
			res->release(); queryOp->release(); fQ2=true;
			freeV(vals[1]); vals[1].setError(propID2);
			if ((rc=queryOp2->next(skip))!=RC_OK) {
				state=state&~QOS_ADV2|QOS_EOF2; if (rc!=RC_EOF) {state|=QOS_EOF1; return rc;}
				if ((state&QOS_EOF1)!=0 || op!=QRY_EXCEPT) return RC_EOF;
				state|=QOS_ADV1; return RC_OK;
			}
			state&=~QOS_ADV2; assert((state&QOS_EOF1)==0);
			if (propID2==PROP_SPEC_PINID) {
				if (pR->id.pid==STORE_INVALID_PID && (rc=pR->unpack())!=RC_OK) return rc;
				vals[1].set(pR->id);
			} else if (queryOp2->getData(*pR,&vals[1],1)!=RC_OK) {
				if (rc!=RC_DEADLOCK && rc!=RC_SHUTDOWN) {state|=QOS_ADV2; continue;}			// RC_NOACCESS || RC_DELETED || RC_NOTFOUND
				return rc;
			}
		}
		assert((state&(QOS_ADV1|QOS_ADV2|QOS_ADVN))==0);
		int c=cmp(vals[0],vals[1],CND_SORT);
		if (c>0) state|=QOS_ADV2;
		else if (c<0) {
			if ((state&QOS_NEXT)==0) state|=QOS_ADV1;
			else {freeV(vals[0]); vals[0]=vals[2]; vals[2].setError(); *res=saveR; state&=~QOS_NEXT;}
			if (op==QRY_EXCEPT) return RC_OK;
		} else {
			if ((mode&QO_ALLSETS)==0) state|=(state&(QOS_UNI1|QOS_UNI2))==(QOS_UNI1|QOS_UNI2)?QOS_ADV1|QOS_ADV2:QOS_ADV1;
			else switch (state&(QOS_UNI1|QOS_UNI2)) {
			case QOS_UNI1|QOS_UNI2: state|=QOS_ADV1|QOS_ADV2; break;
			case QOS_UNI1: state|=QOS_ADV2; break;
			case QOS_UNI2: state|=QOS_ADV1; break;
			case 0:
				state|=QOS_ADV2;
				if ((state&QOS_NEXT)==0) {
					queryOp2->release(); vals[2].setError(propID1);
					// res -> saveR, cleanup()
					while ((rc=queryOp->next(skip))==RC_OK) {
						if (propID1==PROP_SPEC_PINID) {
							if (res->id.pid==STORE_INVALID_PID && (rc=res->unpack())!=RC_OK) return rc;
							vals[2].set(res->id); break;
						}
						if (queryOp->getData(*res,&vals[2],1)==RC_OK) break;
						if (rc==RC_DEADLOCK || rc==RC_SHUTDOWN) return rc;				// RC_NOACCESS || RC_DELETED || RC_NOTFOUND
					}
					if (rc==RC_OK) {if (cmp(vals[0],vals[2],CND_SORT)!=0) state|=QOS_NEXT; else state=state&~QOS_ADV2|QOS_ADVN;}
					// restore res from saveR
				}
				break;
			}
			if (op!=QRY_EXCEPT) {
				if (res->epr.lref!=0 && PINRef::isColl(res->epr.buf,res->epr.lref) && op<QRY_UNION && (mode&QO_UNIQUE)!=0) {
					if (pids!=NULL) {if ((*pids)[*res]) continue;}
					else if ((pids=new(ses) PIDStore(ses))==NULL) return RC_NORESOURCES;
					if ((rc=((*pids)+=*res))!=RC_OK) return rc;
					PINRef::changeFColl(res->epr.buf,res->epr.lref,false);
				}
				return RC_OK;
			}
		}
	}
}

RC MergeOp::rewind()
{
	if (pids!=NULL) {delete pids; pids=NULL;}
	freeV(vals[0]); freeV(vals[1]); freeV(vals[2]);
	vals[0].setError(); vals[1].setError(); vals[2].setError();
	pR->cleanup(); saveR.cleanup();
	RC rc=queryOp!=NULL?queryOp->rewind():RC_EOF;
	if (rc==RC_OK) {queryOp->release(); rc=queryOp2!=NULL?queryOp2->rewind():RC_EOF;}
	state=(state&(QOS_UNI1|QOS_UNI2))|(rc==RC_OK?QOS_ADV1|QOS_ADV2:QOS_EOF1|QOS_EOF2);
	return rc;
}

RC MergeOp::loadData(PINEx& qr,Value *pv,unsigned nv,MemAlloc *ma,ElementID eid)
{
	if (didx!=~0u) {
		// return from vals[0] or vals[2] and, if required from vals[1]
	}
	return RC_NOTFOUND;
}

RC MergeOp::release()
{
	pR->release(); saveR.release(); RC rc;
	if (queryOp!=NULL && (rc=queryOp->release())!=RC_OK) return rc;
	if (queryOp2!=NULL && (rc=queryOp2->release())!=RC_OK) return rc;
	return RC_OK;
}

void MergeOp::unique(bool f)
{
	if (queryOp!=NULL) queryOp->unique(f); 
	if (f) mode|=QO_UNIQUE; else {mode&=~QO_UNIQUE; delete pids; pids=NULL;}
}

void MergeOp::reorder(bool)
{
}

void MergeOp::getOpDescr(QODescr& qop)
{
	if (queryOp!=NULL) queryOp->getOpDescr(qop);
	QODescr qop2; if (queryOp2!=NULL) queryOp2->getOpDescr(qop2);
	ulong flags=QO_JOIN|(mode&QO_UNIQUE);	//|(qop.flags&(QO_DEGREE|QO_ALLPROPS));
	qop.flags=flags|((qop.flags&qop2.flags&QO_UNIQUE)!=0 && propID2==PROP_SPEC_PINID?QO_UNIQUE:0);
	qop.level=max(qop.level,qop2.level)+1;
	if (qop.props!=NULL && qop.nProps!=0) {
#if 1
		qop.props=NULL; qop.nProps=0;
#else
		if (qop2.props!=NULL && qop2.nProps!=0) {
			// merge, set mode flag to include vals[1] properties to getData
		}
#endif
	}
}

void MergeOp::print(SOutCtx& buf,int level) const
{
	const static char *ops[]={"join", "left-join", "right-join", "outer-join", "union", "except", "intersect"};
	buf.fill('\t',level); buf.append(ops[op-QRY_JOIN],strlen(ops[op-QRY_JOIN]));
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
			PINEx qr(ses); queryOp2->connect(&qr);
			for (PINEx qr2(ses); (rc=queryOp2->next())==RC_OK; ) {
				if (pids==NULL && (pids=new(ses) PIDStore(ses))==NULL) return RC_NORESOURCES;
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

RC HashOp::release()
{
	RC rc;
	if (queryOp!=NULL && (rc=queryOp->release())!=RC_OK) return rc;
	if (queryOp2!=NULL && (rc=queryOp2->release())!=RC_OK) return rc;
	return RC_OK;
}

void HashOp::reorder(bool)
{
}

void HashOp::getOpDescr(QODescr& qop)
{
	if (queryOp!=NULL) queryOp->getOpDescr(qop);
	QODescr qop2; if (queryOp2!=NULL) queryOp2->getOpDescr(qop2);		
	qop.flags=qop.flags&~QO_STREAM|QO_JOIN; 
	qop.level=max(qop.level,qop2.level)+1;
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
			ses->free(conds);
		}
	}
}

void NestedLoop::connect(PINEx **results,unsigned nRes)
{
	if (results!=NULL && nRes!=0) {
		res=results[0]; unsigned nOuts1=queryOp->getNOuts();
		queryOp->connect(results,nRes>nOuts1?nOuts1:nRes);
		if (nRes==1) queryOp2->connect(pR);
		else if (nOuts1+queryOp2->getNOuts()<=nRes) {
			queryOp2->connect(results+nOuts1,nRes-nOuts1); pR=results[nOuts1];	//???
		} else {
			//???
		}
	}
}

RC NestedLoop::next(const PINEx *skip)
{
	RC rc;
	if ((state&QST_INIT)!=0) {
		state&=~QST_INIT;
		if (nSkip>0) {
			// ???
		}
	}
	for (res->cleanup();;state|=QOS_ADV1) {
		if ((state&(QOS_EOF1|QOS_EOF2))!=0) return RC_EOF;
		if ((state&QOS_ADV1)!=0) {
			queryOp2->release(); state&=~QOS_ADV1;
			do {
				if ((rc=queryOp->next())==RC_OK) {
					//...
					// rc=getData(*res,...);
				}
				if (rc!=RC_OK) {queryOp->release(); queryOp2->release(); state|=QOS_EOF1|QOS_EOF2; return rc;}
			} while (!res->defined(props,nPropIDs1));
			// locking
			// get outer pin
			queryOp->release();
			if ((rc=queryOp2->rewind())!=RC_OK)		// not first time!
				{queryOp2->release(); state|=QOS_EOF1|QOS_EOF2; return rc;}
		}
		while ((rc=queryOp2->next())!=RC_EOF) {
			if (rc==RC_OK) {
				//...
				//rc=getData()
			}
			if (rc!=RC_OK) {state=QOS_EOF1|QOS_EOF2; queryOp2->release(); return rc;}
			bool fOK=pR->defined(&props[nPropIDs1],nPropIDs2);
			if (fOK && conds!=NULL) {
				const PINEx *pp[2]={res,pR};
				if (!ses->getStore()->queryMgr->condSatisfied((const Expr* const*)(nConds>1?conds:&cond),nConds,pp,2,NULL,0,ses)) fOK=false;
			}
			if (fOK) return RC_OK;
		}
	}
}

RC NestedLoop::rewind()
{
	RC rc;
	if (queryOp!=NULL && (rc=queryOp->rewind())!=RC_OK) return rc;
	if (queryOp2!=NULL && (rc=queryOp2->rewind())!=RC_OK) return rc;
	state=(state&(QOS_UNI1|QOS_UNI2))|(rc==RC_OK?QOS_ADV1|QOS_ADV2:QOS_EOF1|QOS_EOF2);		//???
	return RC_OK;
}

RC NestedLoop::release()
{
	RC rc;
	if (queryOp!=NULL && (rc=queryOp->release())!=RC_OK) return rc;
	if (queryOp2!=NULL && (rc=queryOp2->release())!=RC_OK) return rc;
	return RC_OK;
}

void NestedLoop::getOpDescr(QODescr& qop)
{
	QODescr qop1,qop2;
	if (queryOp!=NULL) queryOp->getOpDescr(qop1);
	if (queryOp2!=NULL) queryOp2->getOpDescr(qop2);
	qop.flags=qop1.flags&QO_DEGREE|QO_JOIN; qop.sort=qop1.sort;
	qop.level=max(qop1.level,qop2.level)+1;
}

void NestedLoop::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("nested loop\n",12);
	if (queryOp!=NULL) queryOp->print(buf,level+1);
	if (queryOp2!=NULL) queryOp2->print(buf,level+1);
}

RC NestedLoop::create(Session *ses,QueryOp *&res,QueryOp *qop1,QueryOp *qop2,const Expr *const *c,ulong nc,
		const PropertyID *pids1,ulong nPids1,const PropertyID *pids2,ulong nPids2,QUERY_SETOP jt,ulong md)
{
	NestedLoop *lj=new(ses,nPids1+nPids2) NestedLoop(ses,qop1,qop2,pids1,nPids1,pids2,nPids2,jt,md);
	if ((res=lj)==NULL) return RC_NORESOURCES;
	if (c!=NULL && nc>0) {
		// QOS_UNI1, QOS_UNI2
#if 0
		bool fOK=true; const PiCond *pc; ulong i;
		for (pc=c; pc!=NULL; ++lj->nConds,pc=pc->next);
		if (lj->nConds==1) {if ((lj->cond=Expr::clone(c->expr,SES_HEAP))==NULL) fOK=false;}
		else if ((lj->conds=new(ses) Expr*[lj->nConds])==NULL) fOK=false;
		else for (i=0,pc=c; i<lj->nConds; ++i,pc=pc->next) 
			if ((lj->conds[i]=Expr::clone(pc->expr,SES_HEAP))==NULL) {fOK=false; break;}
		if (!fOK) {delete lj; res=NULL; return RC_NORESOURCES;}
#endif
	}
	return RC_OK;
}
