/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "queryprc.h"
#include "idxcache.h"
#include "parser.h"
#include "stmt.h"
#include "expr.h"

using namespace AfyKernel;

const static char *j_ops[]={"semijoin", "join", "left outer join", "right outer join", "full outer join", "union", "except", "intersect"};

MergeIDs::MergeIDs(QCtx *s,QueryOp **o,unsigned no,QUERY_SETOP so,ulong qf) : QueryOp(s,qf|QO_JOIN|QO_IDSORT|QO_UNIQUE|QO_STREAM),nOps(no),op(so),cur(~0u),pqr(NULL)
{
	nOuts=1; for (unsigned i=0; i<no; i++) {ops[i].qop=o[i]; ops[i].state=QOS_ADV; ops[i].epr.lref=0;} assert(so==QRY_UNION||so==QRY_INTERSECT||so==QRY_EXCEPT);
}

MergeIDs::~MergeIDs()
{
	if (pqr!=NULL) {pqr->~PINEx(); qx->ses->free(pqr);}
	for (ulong i=0; i<nOps; i++) delete ops[i].qop;
}

void MergeIDs::connect(PINEx **results,unsigned nRes)
{
	if (results!=NULL && nRes!=0) res=results[0];
	if (op==QRY_EXCEPT) {
		ops[0].qop->connect(results,nRes);
		if ((pqr=new(qx->ses) PINEx(qx->ses))!=NULL) ops[1].qop->connect(&pqr,1);
	} else for (ulong i=0; i<nOps; i++) ops[i].qop->connect(results,nRes);	///???
}

RC MergeIDs::next(const PINEx *skip)
{
	RC rc=RC_OK; int cmp=0; if ((state&QST_EOF)!=0) return RC_EOF;
	if ((state&QST_INIT)!=0) {state&=~QST_INIT; if (nSkip>0 && (rc=initSkip())!=RC_OK) return rc;}
	if (op==QRY_UNION) {
		ulong last=cur=~0u;
		for (ulong i=0; i<nOps; i++) {
			QueryOpS& qs=ops[i]; if ((qs.state&QST_EOF)!=0) continue;
			if ((qs.state&QOS_ADV)!=0) {
				last=~0u; qs.state&=~QOS_ADV;
				while ((rc=qs.qop->next(skip))==RC_OK /*&& skip!=NULL && skip->epr.cmp(res->id,skip->id,qx->ses->getStore()->storeID)<0*/) break;
				if (rc!=RC_OK) {if (rc!=RC_DELETED && rc!=RC_NOACCESS && rc!=RC_NOTFOUND) {qs.state|=QST_EOF; if (rc!=RC_EOF) return rc;} continue;}
				if (res->epr.lref==0 && (rc=res->pack())!=RC_OK) {res->cleanup(); *res=PIN::defPID; return rc;}
				last=i; qs.epr=res->epr; qs.epr.flags&=PINEX_LOCKED|PINEX_XLOCKED|PINEX_ACL_CHKED|PINEX_EXTPID;
			}
			if (cur==~0u || (cmp=ops[cur].epr.cmp(qs.epr))==0) {cur=i; qs.state|=QOS_ADV;}
			else if (cmp>0) {ops[cur].state&=~QOS_ADV; cur=i; qs.state|=QOS_ADV;}
		}
		if (cur==~0u) rc=RC_EOF;
		else {rc=RC_OK; if (cur!=last) {res->cleanup(); *res=PIN::defPID; res->epr=ops[cur].epr;}}
	} else if (op==QRY_EXCEPT) for (;;) {
		if ((ops[1].state&QOS_ADV)!=0) for (;;) if ((rc=ops[1].qop->next())!=RC_DELETED && rc!=RC_NOACCESS && rc!=RC_NOTFOUND) {
			if (rc==RC_EOF) {ops[1].state=QOS_EOF2; if ((ops[0].state&QOS_ADV)!=0) break; ops[0].state|=QOS_ADV; return RC_OK;} 
			if (rc!=RC_OK) {state|=QST_EOF; return rc;} if (pqr->epr.lref!=0 || pqr->pack()==RC_OK) break;
		}
		if ((ops[0].state&QOS_ADV)!=0) for (;;) if ((rc=ops[0].qop->next())!=RC_DELETED && rc!=RC_NOACCESS && rc!=RC_NOTFOUND) {
			if (rc!=RC_OK) {state|=QST_EOF; return rc;} if ((ops[1].state&QOS_EOF2)!=0) return RC_OK;
			if (res->epr.lref!=0 || pqr->pack()==RC_OK) break;
		}
		ops[0].state|=QOS_ADV; ops[1].state|=QOS_ADV;
		if ((cmp=res->epr.cmp(pqr->epr))>0) ops[0].state&=~QOS_ADV; else if (cmp<0) {ops[1].state&=~QOS_ADV; return RC_OK;}
	} else {
		PINEx skp(qx->ses); if (cur!=~0u && (skip==NULL || ops[cur].epr.cmp(skip->epr)<=0)) {skp.epr=ops[cur].epr; skip=&skp;}
		for (ulong i=0,nOK=0,flags=0; nOK<nOps; ) {
			QueryOpS& qs=ops[i]; res->cleanup(); if ((qs.state&QST_EOF)!=0) {rc=RC_EOF; break;}
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
		if (op==QRY_UNION) rc=ops[cur].qop->getData(qr,pv,nv,NULL,eid,ma);
		else {
			// idxData?
		}
	}
	return RC_NOTFOUND;
}

void MergeIDs::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append(j_ops[op],strlen(j_ops[op])); buf.append("\n",1);
	for (ulong i=0; i<nOps; i++) ops[i].qop->print(buf,level+1);
}

//------------------------------------------------------------------------------------------------

MergeOp::MergeOp(QueryOp *qop1,QueryOp *qop2,const CondEJ *ce,unsigned ne,QUERY_SETOP qo,const Expr *const *cn,unsigned ncn,ulong qf)
	: QueryOp(qop1,qf|QO_JOIN|QO_UNIQUE),queryOp2(qop2),op(qo),ej(ce),nej(ne),conds(cn),nConds(ncn),didx(0),pexR(qx->ses),pR(&pexR),pids(NULL),
	index1(NULL),index2(NULL),pV1(vls),pV2(vls+ne),pVS(vls+ne*2)
{
	const ulong qf1=qop1->getQFlags(); qflags|=qf1&QO_IDSORT; nOuts+=qop2->getNOuts();
	if ((qf1&qop2->getQFlags()&QO_UNIQUE)!=0 && ce->propID2==PROP_SPEC_PINID) qflags|=QO_UNIQUE;
	sort=qop1->getSort(nSegs); props1.props=props2.props=NULL; props1.nProps=props2.nProps=0; props1.fFree=props2.fFree=false;
	if (ne>1) {
		if ((props1.props=(unsigned*)qx->ses->malloc(ne*(sizeof(unsigned)*2+sizeof(PropertyID)*2)))==NULL) throw RC_NORESOURCES;
		props2.props=props1.props+ne; index1=(unsigned*)(props2.props+ne); index2=index1+ne; props1.fFree=true;
		for (unsigned n=0; ce!=NULL; ce=ce->next,++n) {
			PropertyID *ins,*pi=(PropertyID*)BIN<PropertyID>::find(ce->propID1,props1.props,props1.nProps,&ins);
			if (pi==NULL) {if (ins<&props1.props[props1.nProps]) memmove(ins+1,ins,(byte*)&props1.props[props1.nProps]-(byte*)ins); *(pi=ins)=ce->propID1; props1.nProps++;}
			index1[n]=(unsigned)(pi-props1.props);
			pi=(PropertyID*)BIN<PropertyID>::find(ce->propID2,props2.props,props2.nProps,&ins);
			if (pi==NULL) {if (ins<&props2.props[props2.nProps]) memmove(ins+1,ins,(byte*)&props2.props[props2.nProps]-(byte*)ins); *(pi=ins)=ce->propID2; props2.nProps++;}
			index2[n]=(unsigned)(pi-props2.props);
		}
	}
	if ((qf&QO_VCOPIED)!=0 && cn!=NULL && ncn!=0) {
		Expr **pex; if ((conds=pex=new(qx->ses) Expr*[ncn])==NULL) throw RC_NORESOURCES;
		memset(pex,0,ncn*sizeof(Expr));
		for (unsigned i=0; i<ncn; i++) if ((pex[i]=Expr::clone(cn[i],qx->ses))==NULL) throw RC_NORESOURCES;
	}
	for (unsigned i=0; i<ne; i++) {pV1[i].setError(); pV2[i].setError(); pVS[i].setError();}
}

MergeOp::~MergeOp()
{
	if (pids!=NULL) delete pids;
	if (props1.props!=NULL) qx->ses->free(props1.props);
	cleanup(pV1); cleanup(pV2); cleanup(pVS);
	if ((qflags&QO_VCOPIED)!=0) {
		qx->ses->free((void*)ej);
		if (nConds!=0) {
			for (unsigned i=0; i<nConds; i++) if (conds[i]!=NULL) ((Expr*)conds[i])->destroy();
			qx->ses->free((void*)conds);
		}
	}
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
	for (;;) {
		if ((state&QOS_EOF1)!=0) return RC_EOF;	// if not right/full outer
		if ((state&QOS_ADV1)!=0) {
			cleanup(pV1); state&=~QST_BOF;
			res->epr.flags&=~PINEX_RLOAD; if (op!=QRY_SEMIJOIN) pR->epr.flags|=PINEX_RLOAD;
			if ((rc=queryOp->next(skip))!=RC_OK) {
				state=(state&~QOS_ADV1)|(QOS_EOF1|QOS_EOF2); return rc;		// if not right/full outer
			} else if ((state&QOS_EOF2)==0) {
				state&=~QOS_ADV1;
				if (nej>1) {
					if ((rc=getData(&res,1,&props1,1,pV1))!=RC_OK) return rc;
				} else if (ej->propID1==PROP_SPEC_PINID) {
					if ((rc=res->getID(id))==RC_OK) pV1->set(id); else return rc;
				} else if ((pV1->property=ej->propID1,queryOp->getData(*res,pV1,1,pR))!=RC_OK) {
					if (rc!=RC_NOACCESS && rc!=RC_DELETED && rc!=RC_NOTFOUND) return rc;
					state|=QOS_ADV1; continue;
				}
			}
		}
		if ((state&QOS_EOF2)!=0) return RC_EOF;	// if not left/full outer
		if ((state&QOS_ADV2)!=0) {
			cleanup(pV2); res->epr.flags|=PINEX_RLOAD; pR->epr.flags&=~PINEX_RLOAD;
			if ((rc=queryOp2->next(skip))!=RC_OK) {
				state=(state&~QOS_ADV2)|QOS_EOF2; if (rc!=RC_EOF) state|=QST_EOF; return rc;
			}
			state&=~QOS_ADV2; assert((state&QOS_EOF1)==0);
			if (nej>1) {
				if ((rc=getData(&pR,1,&props2,1,pV2))!=RC_OK) return rc;
			} else if (ej->propID2==PROP_SPEC_PINID) {
				if ((rc=pR->getID(id))==RC_OK) pV2->set(id); else return rc;
			} else if ((pV2->property=ej->propID2,queryOp2->getData(*pR,pV2,1))!=RC_OK) {
				if (rc!=RC_NOACCESS && rc!=RC_DELETED && rc!=RC_NOTFOUND) return rc;
				state|=QOS_ADV2; continue;
			}
		}
		int c=0; assert((state&(QOS_ADV1|QOS_ADV2))==0);
		if (nej==1) c=cmp(*pV1,*pV2,ej->flags|CND_SORT);
		else {
			const CondEJ *ce=ej; 
			for (unsigned i=0; i<nej; i++,ce=ce->next) if ((c=cmp(pV1[index1[i]],pV2[index2[i]],ce->flags|CND_SORT))!=0) break;
		}
#if 0
		//if (c==0) {
			SOutCtx out(qx->ses);
			if (pV1->type==VT_BSTR) pV1->type=VT_STRING;
			if (pV2->type==VT_BSTR) pV2->type=VT_STRING;
			res->getID(id); out.renderPID(id); 
			if (ej->propID1!=PROP_SPEC_PINID) {out.append(":",1); out.renderValue(*pV1);}
			out.append(c<0?" < ":c==0?" = ":" > ",3);
			pR->getID(id); out.renderPID(id);
			if (ej->propID2!=PROP_SPEC_PINID) {out.append(":",1); out.renderValue(*pV2);}
			size_t l; byte *p=out.result(l); report(MSG_DEBUG,"%.*s\n",l,p); qx->ses->free(p);
		//}
#endif
		if (c>0) state|=QOS_ADV2; 
		else if (c<0) state|=QOS_ADV1;
		else {
			if (op==QRY_SEMIJOIN) {
				state|=(qflags&(QO_UNI1|QO_UNI2))==(QO_UNI1|QO_UNI2)?QOS_ADV1|QOS_ADV2:QOS_ADV1;
				if (res->epr.lref!=0 && PINRef::isColl(res->epr.buf,res->epr.lref) && (qflags&QO_UNIQUE)!=0) {
					if (pids!=NULL) {if ((*pids)[*res]) continue;}
					else if ((pids=new(qx->ses) PIDStore(qx->ses))==NULL) return RC_NORESOURCES;
					if ((rc=((*pids)+=*res))!=RC_OK) return rc;
					PINRef::changeFColl(res->epr.buf,res->epr.lref,false);
				}
			} else switch (qflags&(QO_UNI1|QO_UNI2)) {
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
			if (conds!=NULL) {
				PINEx *pp[2]={res,pR};
				if (!Expr::condSatisfied(conds,nConds,pp,2,qx->vals,QV_ALL,qx->ses,(qflags&QO_CLASS)!=0)) continue;
			}
			res->epr.flags|=PINEX_RLOAD; pR->epr.flags|=PINEX_RLOAD; return RC_OK;
		}
	}
}

RC MergeOp::rewind()
{
	if ((state&QST_BOF)!=0) return RC_OK;
	if (pids!=NULL) {delete pids; pids=NULL;}
	cleanup(pV1); cleanup(pV2); cleanup(pVS);
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
	buf.fill('\t',level); buf.append(j_ops[op],strlen(j_ops[op])); 
	if (ej!=NULL) {
		buf.append(" on ",4);
		for (const CondEJ *ce=ej; ce!=NULL; ce=ce->next) {
			buf.renderName(ce->propID1); buf.append("=",1); buf.renderName(ce->propID2);	// ce->flags
			if (ce->next!=NULL) buf.append(" and ",5);
		}
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
