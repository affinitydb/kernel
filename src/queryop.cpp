/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "maps.h"
#include "lock.h"
#include "session.h"
#include "expr.h"
#include "ftindex.h"
#include "queryprc.h"
#include "stmt.h"
#include "parser.h"

using namespace MVStoreKernel;

QueryOp::QueryOp(Session *s,QueryOp *qop,ulong md) : ses(s),queryOp(qop),mode(md),state(QST_INIT),nSkip(0)
{
}

QueryOp::~QueryOp()
{
	delete queryOp;
}

RC QueryOp::initSkip()
{
	PINEx qr(ses); RC rc=RC_OK;
	while (nSkip!=0) if ((rc=next(qr))!=RC_OK || (--nSkip,rc=ses->testAbortQ())!=RC_OK) break;
	return rc;
}

RC QueryOp::rewind()
{
	return queryOp!=NULL?queryOp->rewind():RC_OK;
}

RC QueryOp::count(uint64_t& cnt,ulong nAbort)
{
	uint64_t c=0; RC rc; PINEx qr(ses);
	while ((rc=next(qr))==RC_OK) if (++c>nAbort) return RC_TIMEOUT;
	cnt=c; return rc==RC_EOF?RC_OK:rc;
}

RC QueryOp::loadData(PINEx& qr,Value *pv,unsigned nv,MemAlloc *ma,ElementID eid)
{
	return queryOp!=NULL?queryOp->loadData(qr,pv,nv,ma,eid):RC_NOTFOUND;
}

RC QueryOp::getData(PINEx& qr,Value *pv,unsigned nv,const PINEx *qr2,MemAlloc *ma,ElementID eid)
{
	RC rc=RC_OK;
	if (qr.hpin!=NULL || qr.props!=NULL) {
		if (pv!=NULL) for (unsigned i=0; i<nv; i++)
			if ((rc=qr.getValue(pv[i].property,pv[i],LOAD_SSV,ma,eid))==RC_NOTFOUND) rc=RC_OK; else if (rc!=RC_OK) break;
		return rc;
	}
	if (qr2!=NULL && !qr2->pb.isNull()) {
		// check same page
	}
	return loadData(qr,pv,nv,ma,eid);
}

void QueryOp::unique(bool f)
{
	if (queryOp!=NULL) queryOp->unique(f);
}

void QueryOp::reorder(bool f)
{
	if (queryOp!=NULL) queryOp->reorder(f);
}

void QueryOp::getOpDescr(QODescr& qop)
{
	if (queryOp!=NULL) {queryOp->getOpDescr(qop); qop.level++;}
}

RC QueryOp::release()
{
	return queryOp!=NULL?queryOp->release():RC_OK;
}

//------------------------------------------------------------------------------------------------

RC BodyOp::next(PINEx& qr,const PINEx *skip)
{
	RC rc=RC_OK; assert(ses!=NULL);
	if ((state&QST_INIT)!=0) {state&=~QST_INIT; if (nSkip>0 && (rc=initSkip())!=RC_OK) return rc;}
//	if (op==GO_FIRST || op==GO_LAST) state&=~(QST_BOF|QST_EOF); else if ((state&(op==GO_NEXT?QST_EOF:QST_BOF))!=0) return RC_EOF;
	for (; (rc=queryOp->next(qr,skip))==RC_OK; skip=NULL) {
		if ((qr.epr.flags&PINEX_ADDRSET)==0) qr=PageAddr::invAddr;
		if ((rc=ses->getStore()->queryMgr->getBody(qr,(mode&QO_FORUPDATE)!=0?TVO_UPD:TVO_READ,0,queryOp))!=RC_OK||(rc=ses->testAbortQ())!=RC_OK)
			{qr.cleanup(); if (rc==RC_NOACCESS || rc==RC_REPEAT || rc==RC_DELETED) {qr.cleanup(); continue;} else return rc;}
		assert(qr.getAddr().defined() && (qr.epr.flags&PINEX_ADDRSET)!=0);
		if ((qr.hpin->hdr.descr&HOH_HIDDEN)!=0) {qr.cleanup(); continue;}
#if 0
		if (ses->inWriteTx() && (qr.epr.flags&(fWrite?PINEX_XLOCKED:PINEX_LOCKED))==0 && (flt==NULL||!flt->ignore())) {
			if (flt!=NULL && ses->hasLatched()) flt->release();
			if (qr.id.pid==STORE_INVALID_PID && (rc=qr.unpack())!=RC_OK) return rc;
			if ((rc=ses->getStore()->lockMgr->lock(fWrite?LOCK_EXCLUSIVE:LOCK_SHARED,ses,qr.id,&RLQ))!=RC_OK
				|| (rc=ses->testAbortQ())!=RC_OK) return rc;
			if (fWrite && !qr.pb.isNull() && !qr.pb->isULocked() && !qr.pb->isXLocked()) qr.cleanup();
			qr.epr.flags|=fWrite?PINEX_XLOCKED|PINEX_LOCKED:PINEX_LOCKED;
		}
		if ((qr.epr.flags&PINEX_ACL_CHKED)==0 && (iid=ses->getIdentity())!=STORE_OWNER) {
			rc=ses->getStore()->queryMgr->checkACLs(qr,iid,fWrite?GP_FORUPDATE:0,false);
			if (rc==RC_OK) qr.epr.flags|=PINEX_ACL_CHKED; else fOK=rc==RC_FALSE; 
			if ((rc=ses->testAbortQ())!=RC_OK) return rc;
		}
#endif
		break;
	}
	if (rc!=RC_OK) state|=QST_EOF;
	return rc;
}

RC BodyOp::rewind()
{
	// ??? reset cash
	return queryOp!=NULL?queryOp->rewind():RC_OK;
}
	
RC BodyOp::loadData(PINEx& qr,Value *pv,unsigned nv,MemAlloc *ma,ElementID eid)
{
	//...
	return RC_OK;
}

void BodyOp::reorder(bool f)
{
	if (f) mode|=QO_REORDER; else mode&=~QO_REORDER;
}

void BodyOp::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("project: ",9);
	if (nProps==0) buf.append("*\n",2);
	else for (unsigned i=0; i<nProps; i++) {buf.renderName(props[i]); if (i+1<nProps) buf.append(", ",2); else buf.append("\n",1);}
	if (queryOp!=NULL) queryOp->print(buf,level+1);
}

//------------------------------------------------------------------------------------------------

PathOp::~PathOp()
{
	if (nav!=NULL) nav->destroy(); if (arr!=NULL) freeV(arr,lArr,ses); if (vals!=NULL) freeV(vals,nvals,ses);
	for (unsigned i=0; i<nPathSeg; i++) if (props[i].props!=NULL) ses->free(props[i].props);
	if (fCopied) {
		for (unsigned i=0; i<nPathSeg; i++) if (path[i].filter!=NULL) path[i].filter->destroy();
		ses->free((void*)path);
	}
}

RC PathOp::next(PINEx& qr,const PINEx *)
{
	RC rc; qr.cleanup();
	if ((state&QST_INIT)!=0) {
		state&=~QST_INIT; if (nSkip>0 && (rc=initSkip())!=RC_OK) return rc;
		for (unsigned i=0; i<nPathSeg; i++) {
			unsigned np=i+1<nPathSeg?2:1;
			if (path[i].filter!=NULL) {
				// get req props np+=
			}
			if (np!=0) {
				if ((props[i].props=new(ses) PropertyID[np])==NULL) return RC_NORESOURCES;
				np=0;
				if (path[i].filter!=NULL) {
					//...
				}
				if (i+1<nPathSeg && (rc=mv_bins<PropertyID,unsigned>(props[i].props,np,path[i+1].pid,NULL))!=RC_OK) return rc;
				if ((rc=mv_bins<PropertyID,unsigned>(props[i].props,np,path[i].pid,NULL))!=RC_OK) return rc;
				props[i].nProps=np; if (np>nvals) nvals=np;
			}
		}
		if (nvals!=0 && (vals=new(ses) Value[nvals])==NULL) return RC_NORESOURCES;
		for (unsigned i=0; i<nvals; i++) vals[i].setError();
	}
	for (;;) switch (pstate) {
	case 0:
		if (nav!=NULL || arr!=NULL) for (;;) {
			const Value *cv=nav!=NULL?nav->navigateNR(GO_NEXT):++cidx<lArr?&arr[cidx]:(const Value*)0;
			if (cv!=NULL) {
				switch (cv->type) {
				default: continue;
				case VT_REFID: pex=cv->id; break;
				case VT_REF: pex=cv->pin->getPID(); break;
				/* case VT_STRUCT: find PROP_SPEC_REF */
				}
				pstate++;
				if (path[idx].filter==NULL && idx+1>=nPathSeg && rep>=path[idx].rmin) {qr=pex; return RC_OK;}
			} else if (nav!=NULL) {nav->destroy(); nav=NULL;} else {freeV(arr,lArr,ses); arr=NULL; lArr=cidx=0;}
			break;
		}
		if (pstate==0) {
			pstate++;
			//if (queue not empty) {
				// get PID, idx, rep from queue
				//if fOut && ! filter -> fNext=false, save PID,idx,rep,  return it
			//} else {
				idx=rep=0;
				if ((state&QST_EOF)!=0) return RC_EOF;
				if ((rc=queryOp->next(pex))!=RC_OK) {state|=QST_EOF; return rc;}
			//}
		}
	case 1:
		if (props[idx].props!=NULL && props[idx].nProps!=0) {
			unsigned np=props[idx].nProps;
			for (unsigned i=0; i<nvals; i++) {freeV(vals[i]); vals[i].setError(i<np?props[idx].props[i]:STORE_INVALID_PROPID);}
			if ((rc=getData(pex,vals,np,NULL,ses,path[idx].eid))!=RC_OK) return rc;	//???
			if (path[idx].filter!=NULL) {
				// check, fail -> pstate=0; continue;
				pstate++; if (idx+1>=nPathSeg && rep>=path[idx].rmin) {qr=pex; return RC_OK;}
			}
		}
	case 2:
		if (idx+1<nPathSeg && rep>=path[idx].rmin) {
			const Value *pv=mv_bsrcmp<Value,PropertyID>(path[idx+1].pid,vals,props[idx].nProps); assert(pv!=NULL);
			switch (pv->type) {
			default: break;
			case VT_REF:
			case VT_REFID:
				//push(pv->id,idx+1,1);
			// case VT_STRUCT:
			case VT_ARRAY:
			case VT_COLLECTION:
				if ((!path[idx+1].fLast || idx+2==nPathSeg) && path[idx+1].eid!=STORE_COLLECTION_ID) {
					// find it, if VT_REFID, VT_STRUCT -> push
				} else {
					// scan all, if VT_REFID, VT_STRUCT -> push
				}
				break;
			}
		}
		if (rep<path[idx].rmax || path[idx].rmax==0xFFFF) {
			const Value *pv=mv_bsrcmp<Value,PropertyID>(path[idx].pid,vals,props[idx].nProps); assert(pv!=NULL);
			switch (pv->type) {
			default: break;
			case VT_REF:
				// put to qr
			case VT_REFID:
				//push(pv->id,idx,rep+1);
				break;
			// case VT_STRUCT:
			case VT_ARRAY:
			case VT_COLLECTION:
				if ((!path[idx].fLast || idx+1==nPathSeg) && path[idx].eid!=STORE_COLLECTION_ID) {
					// find it, if VT_REFID, VT_STRUCT -> push
				} else {
					// scan all, if VT_REFID, VT_STRUCT -> push
				}
				break;
			}
		}
	}
}

RC PathOp::rewind()
{
	idx=rep=cidx=pstate=0; pex.cleanup();
	if (nav!=NULL) {nav->destroy(); nav=NULL;}
	if (arr!=NULL) {freeV(arr,lArr,ses); arr=NULL; lArr=0;}
	if (vals!=NULL) for (unsigned i=0; i<nvals; i++) {freeV(vals[i]); vals[i].setError();}
	return queryOp!=NULL?queryOp->rewind():RC_OK;
}

RC PathOp::release()
{
	// ???
	return QueryOp::release();
}

void PathOp::getOpDescr(QODescr& qop)
{
	if (queryOp!=NULL) {
		queryOp->getOpDescr(qop); qop.level++;
		//...
	}
}

void PathOp::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("path: ",6);
	for (unsigned i=0; i<nPathSeg; i++) buf.renderPath(path[i]);
	buf.append("\n",1); if (queryOp!=NULL) queryOp->print(buf,level+1);
}

//------------------------------------------------------------------------------------------------

GroupOp::~GroupOp()
{
}

RC GroupOp::next(PINEx&,const PINEx*)
{
	//???
	return RC_OK;
}

RC GroupOp::rewind()
{
	if (queryOp!=NULL) queryOp->rewind();
	// reset currect group
	return RC_OK;
}

void GroupOp::getOpDescr(QODescr& dscr)
{
	if (queryOp!=NULL) queryOp->getOpDescr(dscr);
	//...
}

void GroupOp::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("group ",5);
#if 0
	if (nSegs==0) buf.renderName(PROP_SPEC_PINID);
	else for (unsigned i=0; i<nSegs; i++) {
		const OrderSegQ &os=sortSegs[i]; if (i!=0) buf.append(", ",2);
		if ((os.flags&ORDER_EXPR)!=0) os.expr->render(0,buf);
		else if (os.pid==PROP_SPEC_ANY) buf.append("#rank",5);
		else buf.renderName(os.pid);
		// flags!!!
	}
#endif
	buf.append("\n",1); if (queryOp!=NULL) queryOp->print(buf,level+1);
}

//------------------------------------------------------------------------------------------------

Filter::Filter(Session *s,QueryOp *qop,ulong nPars,size_t lp,ulong nqs,ulong md)
	: QueryOp(s,qop,md),params(NULL),nParams(nPars),conds(NULL),nConds(0),condIdx(NULL),nCondIdx(0),queries(NULL),nQueries(nqs),lProps(lp)
{
}

Filter::~Filter()
{
	if (queries!=NULL) {
		for (ulong i=0; i<nQueries; i++) {
			QueryWithParams& cs=queries[i]; cs.qry->destroy();
			if ((mode&QO_VCOPIED)!=0 && cs.params!=NULL) freeV((Value*)cs.params,cs.nParams,ses);
		}
		ses->free(queries);
	}
	if ((mode&QO_VCOPIED)!=0) {
		if (conds!=NULL) {
			if (nConds==1) cond->destroy();
			else {for (ulong i=0; i<nConds; i++) conds[i]->destroy(); ses->free(conds);}
		}
		for (CondIdx *ci=condIdx,*ci2; ci!=NULL; ci=ci2) {ci2=ci->next; ci->~CondIdx(); ses->free(ci);}
		if (params!=NULL) {for (ulong i=0; i<nParams; i++) freeV(params[i]); ses->free(params);}
	}
}

RC Filter::next(PINEx& qr,const PINEx *skip)
{
	RC rc=RC_OK; assert(ses!=NULL);
	if ((state&QST_INIT)!=0) {state&=~QST_INIT; if (nSkip>0 && (rc=initSkip())!=RC_OK) return rc;}
//	if (op==GO_FIRST || op==GO_LAST) state&=~(QST_BOF|QST_EOF); else if ((state&(op==GO_NEXT?QST_EOF:QST_BOF))!=0) return RC_EOF;
	if ((state&QST_EOF)!=0) return RC_EOF;
	for (; (rc=queryOp->next(qr,skip))==RC_OK; skip=NULL) {
		if ((mode&QO_NODATA)==0 && (rc=queryOp->getData(qr,NULL,0))!=RC_OK) break;
		if (lProps!=0 && !condProps.test(&qr,lProps)) continue; bool fOK=true;
		if ((mode&QO_CLASS)==0) {
			for (CondIdx *ci=condIdx; ci!=NULL; ci=ci->next) {
				if (ci->param>=nParams) {fOK=false; break;}
				Value *pv=&params[ci->param];
				if (ci->expr!=NULL) {
					// ???
				} else {
					Value vv; if (qr.getValue(ci->ks.propID,vv,LOAD_SSV,NULL)!=RC_OK) {fOK=false; break;}
					RC rc=Expr::calc((ExprOp)ci->ks.op,vv,pv,2,(ci->ks.flags&ORD_NCASE)!=0?CND_NCASE:0,ses);
					freeV(vv); if (rc!=RC_TRUE) {fOK=false; break;}
				}
			}
			if (!fOK) continue;
		}
		if (queries!=NULL && nQueries>0) {
			for (ulong i=0; i<nQueries; i++) if (!queries[i].qry->checkConditions(&qr,queries[i].params,queries[i].nParams,ses)) {fOK=false; break;}
			if (!fOK) continue;
		}
		const PINEx *pp=&qr;
		if (conds==NULL || ses->getStore()->queryMgr->condSatisfied(nConds>1?conds:&cond,nConds,&pp,1,params,nParams,ses,(mode&QO_CLASS)!=0)) break;
	}
	if (rc!=RC_OK) state|=QST_EOF;		//op==GO_FIRST||op==GO_LAST?QST_BOF|QST_EOF:op==GO_NEXT?QST_EOF:QST_BOF;
	return rc;
}

void Filter::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("filter: \n",9);		// property etc.
	if (queryOp!=NULL) queryOp->print(buf,level+1);
}

//------------------------------------------------------------------------------------------------

ArrayFilter::ArrayFilter(Session *s,QueryOp *q,const PID *pds,ulong nP) : QueryOp(s,q,0),pids((PID*)(this+1)),nPids(0)
{
	if (nP>0) {pids[0]=pds[0]; nPids++; for (ulong i=1; i<nP; i++) if (pds[i]!=pds[i-1]) pids[nPids++]=pds[i];}
}

ArrayFilter::~ArrayFilter()
{
}

RC ArrayFilter::next(PINEx& qr,const PINEx *skip)
{
	RC rc=RC_EOF;
	if ((state&QST_INIT)!=0) {state&=~QST_INIT; if (nSkip>0 && (rc=initSkip())!=RC_OK) return rc;}
	if (nPids!=0) for (; (rc=queryOp->next(qr,skip))==RC_OK; skip=NULL) {
		if (qr.id.pid==STORE_INVALID_PID && qr.unpack()!=RC_OK) continue;
		if (mv_bsrc<PID,const PID&>(qr.id,pids,nPids)!=NULL) return RC_OK;
	}
	return rc;
}

void ArrayFilter::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("array filter\n",13);
	if (queryOp!=NULL) queryOp->print(buf,level+1);
}

//-------------------------------------------------------------------------------------------------

bool PropDNF::test(const PINEx *pin,size_t lp) const
{
	for (const PropDNF *p=this,*end=(const PropDNF*)((byte*)this+lp); p<end; p=(const PropDNF*)((byte*)(p+1)+int(p->nIncl+p->nExcl-1)*sizeof(PropertyID))) {
		bool fOK=pin->defined(p->pids,p->nIncl); if (!fOK) continue; if (p->nExcl==0) return true;
		const PropertyID *pp=p->pids+p->nIncl;
		for (int i=p->nExcl; --i>=0; ++pp) if (pin->defined(pp,1)) {fOK=false; break;}
		if (fOK) return true;
	}
	return false;
}

bool PropDNF::hasExcl(size_t lp) const
{
	for (const PropDNF *p=this,*end=(const PropDNF*)((byte*)this+lp); p<end; p=(const PropDNF*)((byte*)(p+1)+int(p->nIncl+p->nExcl-1)*sizeof(PropertyID)))
		if (p->nExcl!=0) return true;
	return false;
}

RC PropDNF::andP(PropDNF *&dnf,size_t& ldnf,PropDNF *rhs,size_t lrhs,MemAlloc *ma)
{
	RC rc=RC_OK;
	if (dnf==NULL || ldnf==0) {ma->free(dnf); dnf=rhs; ldnf=lrhs;}
	else if (rhs!=NULL && lrhs!=0) {
		if (rhs->isConjunctive(lrhs)) {
			if (rhs->nIncl!=0) rc=andP(dnf,ldnf,rhs->pids,rhs->nIncl,ma,false);
			if (rc==RC_OK && rhs->nExcl!=0) rc=andP(dnf,ldnf,rhs->pids+rhs->nIncl,rhs->nExcl,ma,true);
		} else if (dnf->isConjunctive(ldnf)) {
			if (dnf->nIncl!=0) rc=andP(rhs,lrhs,dnf->pids,dnf->nIncl,ma,false);
			if (rc==RC_OK && dnf->nExcl!=0) rc=andP(rhs,lrhs,dnf->pids+dnf->nIncl,dnf->nExcl,ma,true);
			PropDNF *tmp=dnf; dnf=rhs; ldnf=lrhs; rhs=tmp;
		} else {
			PropDNF *res=NULL,*tmp=NULL; size_t lres=0,ltmp=0;
			for (const PropDNF *p=rhs,*end=(const PropDNF*)((byte*)rhs+lrhs); p<end; p=(const PropDNF*)((byte*)(p+1)+int(p->nIncl+p->nExcl-1)*sizeof(PropertyID))) {
				if (res==NULL) {
					if ((res=(PropDNF*)ma->malloc(ldnf))!=NULL) memcpy(res,dnf,lres=ldnf); else {rc=RC_NORESOURCES; break;}
					if (p->nIncl!=0 && (rc=andP(res,lres,p->pids,p->nIncl,ma,false))!=RC_OK) break;
					if (p->nExcl!=0 && (rc=andP(res,lres,p->pids+p->nIncl,p->nExcl,ma,true))!=RC_OK) break;
				} else {
					if ((tmp=(PropDNF*)ma->malloc(ltmp=ldnf))!=NULL) memcpy(tmp,dnf,ldnf); else {rc=RC_NORESOURCES; break;}
					if (p->nIncl!=0 && (rc=andP(tmp,ltmp,p->pids,p->nIncl,ma,false))!=RC_OK) break;
					if (p->nExcl!=0 && (rc=andP(tmp,ltmp,p->pids+p->nIncl,p->nExcl,ma,true))!=RC_OK) break;
					if ((rc=orP(res,lres,tmp,ltmp,ma))!=RC_OK) break;
				}
			}
			ma->free(dnf); dnf=res; ldnf=lres;
		}
		ma->free(rhs); if (rc==RC_OK) normalize(dnf,ldnf,ma);
	}
	return rc;
}

RC PropDNF::orP(PropDNF *&dnf,size_t& ldnf,PropDNF *rhs,size_t lrhs,MemAlloc *ma)
{
	if (dnf!=NULL && ldnf!=0) {
		if (rhs==NULL || lrhs==0) {ma->free(dnf); dnf=NULL; ldnf=0;}
		else {
			if ((dnf=(PropDNF*)ma->realloc(dnf,ldnf+lrhs))==NULL) return RC_NORESOURCES;
			memcpy((byte*)dnf+ldnf,rhs,lrhs); ldnf+=lrhs; ma->free(rhs);
			normalize(dnf,ldnf,ma);
		}
	}
	return RC_OK;
}

RC PropDNF::andP(PropDNF *&dnf,size_t& ldnf,const PropertyID *pids,ulong np,MemAlloc *ma,bool fNot)
{
	if (pids!=NULL && np!=0) {
		if (dnf==NULL || ldnf==0) {
			ma->free(dnf);
			if ((dnf=(PropDNF*)ma->malloc(sizeof(PropDNF)+int(np-1)*sizeof(PropertyID)))==NULL) return RC_NORESOURCES;
			if (fNot) {dnf->nExcl=(ushort)np; dnf->nIncl=0;} else {dnf->nIncl=(ushort)np; dnf->nExcl=0;}
			memcpy(dnf->pids,pids,np*sizeof(PropertyID)); ldnf=sizeof(PropDNF)+int(np-1)*sizeof(PropertyID);
		} else {
			bool fAdded=false;
			for (PropDNF *p=dnf,*end=(PropDNF*)((byte*)p+ldnf); p<end; p=(PropDNF*)((byte*)(p+1)+int(p->nIncl+p->nExcl-1)*sizeof(PropertyID))) {
				for (ulong i=fNot?p->nIncl:0,j=0,n=fNot?p->nExcl:p->nIncl;;) {
					if (i>=n) {
						if (j<np) {
							ulong l=(np-j)*sizeof(PropertyID); ptrdiff_t sht=(byte*)p-(byte*)dnf;
							if ((dnf=(PropDNF*)ma->realloc(dnf,ldnf+l))==NULL) return RC_NORESOURCES;
							p=(PropDNF*)((byte*)dnf+sht); end=(PropDNF*)((byte*)dnf+ldnf);
							if ((void*)&p->pids[i]!=(void*)end) memmove(&p->pids[i+np-j],&p->pids[i],(byte*)end-(byte*)&p->pids[i]);
							memcpy(&p->pids[i],&pids[j],l); ldnf+=l; end=(PropDNF*)((byte*)dnf+ldnf); fAdded=true;
							if (fNot) p->nExcl+=ushort(np-j); else p->nIncl+=ushort(np-j);
						}
						break;
					} else if (j>=np) break;
					else if (p->pids[i]<pids[j]) ++i;
					else if (p->pids[i]==pids[j]) ++i,++j;
					else {
						ulong k=j; while (++j<np && p->pids[i]>pids[j]); ulong m=j-k;
						ulong l=m*sizeof(PropertyID); ptrdiff_t sht=(byte*)p-(byte*)dnf;
						if ((dnf=(PropDNF*)ma->realloc(dnf,ldnf+l))==NULL) return RC_NORESOURCES;
						p=(PropDNF*)((byte*)dnf+sht); end=(PropDNF*)((byte*)dnf+ldnf);
						memmove(&p->pids[i+m],&p->pids[i],(byte*)end-(byte*)&p->pids[i]);
						memcpy(&p->pids[i],&pids[k],l); ldnf+=l; end=(PropDNF*)((byte*)dnf+ldnf);
						i+=m; n+=m; fAdded=true; if (fNot) p->nExcl+=ushort(m); else p->nIncl+=ushort(m);
					}
				}
			}
			if (fAdded && !dnf->isSimple(ldnf)) normalize(dnf,ldnf,ma);
		}
	}
	return RC_OK;
}

RC PropDNF::orP(PropDNF *&dnf,size_t& ldnf,const PropertyID *pids,ulong np,MemAlloc *ma,bool fNot)
{
	if (dnf!=NULL && ldnf!=0) {
		if (pids==NULL || np==0) {ma->free(dnf); dnf=NULL; ldnf=0;}
		else {
			if ((dnf=(PropDNF*)ma->realloc(dnf,ldnf+sizeof(PropDNF)+int(np-1)*sizeof(PropertyID)))==NULL) return RC_NORESOURCES;
			PropDNF *pd=(PropDNF*)((byte*)dnf+ldnf); ldnf+=sizeof(PropDNF)+int(np-1)*sizeof(PropertyID);
			memcpy(pd->pids,pids,np*sizeof(PropertyID)); if (fNot) pd->nExcl=(ushort)np; else pd->nIncl=(ushort)np;
			normalize(dnf,ldnf,ma);
		}
	}
	return RC_OK;
}

RC PropDNF::flatten(size_t ldnf,PropertyID *&ps,unsigned& nPids,MemAlloc *ma) const
{
	unsigned cnt=0; PropertyID *ins=NULL;
	for (const PropDNF *p=this,*end=(const PropDNF*)((byte*)this+ldnf); p<end; p=(const PropDNF*)((byte*)(p+1)+int(p->nIncl+p->nExcl-1)*sizeof(PropertyID))) {
		unsigned i=0;
		if (p==this && nIncl!=0) {
			if (nPids<nIncl && (ma==NULL || (ps=(PropertyID*)ma->realloc(ps,(nPids=nIncl)*sizeof(PropertyID)))==NULL)) return RC_NORESOURCES;
			memcpy(ps,pids,(i=cnt=nIncl)*sizeof(PropertyID)); if ((byte*)(p+1)+int(p->nIncl-1)*sizeof(PropertyID)==(byte*)end) break;
		}
		for (unsigned all=p->nIncl+p->nExcl; i<all; i++) if (mv_bsrc(p->pids[i],ps,cnt,&ins)==NULL) {
			if (nPids<=cnt) {
				ptrdiff_t sht=ins-ps;
				if (ma==NULL || (ps=(PropertyID*)ma->realloc(ps,(nPids=cnt+1)*sizeof(PropertyID)))==NULL) return RC_NORESOURCES;
				ins=ps+sht;
			}
			if (ins<&ps[cnt]) memmove(ins+1,ins,(byte*)&ps[cnt]-(byte*)ins);
			*ins=p->pids[i]; cnt++;
		}
	}
	nPids=cnt; return RC_OK;
}

void PropDNF::normalize(PropDNF *&dnf,size_t& ldnf,MemAlloc *ma)
{
	//...
}
