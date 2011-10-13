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

QueryOp::QueryOp(Session *s,QueryOp *qop,ulong md) : ses(s),queryOp(qop),mode(md),state(QST_INIT),nSkip(0),res(NULL),nOuts(qop!=NULL?qop->nOuts:1)
{
}

QueryOp::~QueryOp()
{
	delete queryOp;
}

RC QueryOp::initSkip()
{
	RC rc=RC_OK;
	while (nSkip!=0) if ((rc=next())!=RC_OK || (--nSkip,rc=ses->testAbortQ())!=RC_OK) break;
	return rc;
}

void QueryOp::connect(PINEx **results,unsigned nRes)
{
	if (results!=NULL && nRes==1) res=results[0];
	if (queryOp!=NULL) queryOp->connect(results,nRes);
}

RC QueryOp::rewind()
{
	return queryOp!=NULL?queryOp->rewind():RC_OK;
}

RC QueryOp::count(uint64_t& cnt,ulong nAbort)
{
	uint64_t c=0; RC rc;
	while ((rc=next())==RC_OK) if (++c>nAbort) return RC_TIMEOUT;
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

RC QueryOp::getBody(PINEx& pe)
{
	RC rc; if ((pe.epr.flags&PINEX_ADDRSET)==0) pe=PageAddr::invAddr;
	if ((rc=ses->getStore()->queryMgr->getBody(pe,(mode&QO_FORUPDATE)!=0?TVO_UPD:TVO_READ,0))==RC_OK&&(rc=ses->testAbortQ())==RC_OK) {
		assert(pe.getAddr().defined() && (pe.epr.flags&PINEX_ADDRSET)!=0);
	}
	return rc;
}

//------------------------------------------------------------------------------------------------

RC BodyOp::next(const PINEx *skip)
{
	RC rc=RC_OK; assert(ses!=NULL && res!=NULL);
	if ((state&QST_INIT)!=0) {state&=~QST_INIT; if (nSkip>0 && (rc=initSkip())!=RC_OK) return rc;}
//	if (op==GO_FIRST || op==GO_LAST) state&=~(QST_BOF|QST_EOF); else if ((state&(op==GO_NEXT?QST_EOF:QST_BOF))!=0) return RC_EOF;
	for (; (rc=queryOp->next(skip))==RC_OK; skip=NULL) {
		if ((rc=getBody(*res))!=RC_OK || (res->hpin->hdr.descr&HOH_HIDDEN)!=0) 
			{res->cleanup(); if (rc==RC_OK || rc==RC_NOACCESS || rc==RC_REPEAT || rc==RC_DELETED) continue; else return rc;}
#if 0
		if (ses->inWriteTx() && (res->epr.flags&(fWrite?PINEX_XLOCKED:PINEX_LOCKED))==0 && (flt==NULL||!flt->ignore())) {
			if (flt!=NULL && ses->hasLatched()) flt->release();
			if (res->id.pid==STORE_INVALID_PID && (rc=res->unpack())!=RC_OK) return rc;
			if ((rc=ses->getStore()->lockMgr->lock(fWrite?LOCK_EXCLUSIVE:LOCK_SHARED,ses,res->id,&RLQ))!=RC_OK
				|| (rc=ses->testAbortQ())!=RC_OK) return rc;
			if (fWrite && !res->pb.isNull() && !res->pb->isULocked() && !res->pb->isXLocked()) res->cleanup();
			res->epr.flags|=fWrite?PINEX_XLOCKED|PINEX_LOCKED:PINEX_LOCKED;
		}
		if ((res->epr.flags&PINEX_ACL_CHKED)==0 && (iid=ses->getIdentity())!=STORE_OWNER) {
			rc=ses->getStore()->queryMgr->checkACLs(qr,iid,fWrite?GP_FORUPDATE:0,false);
			if (rc==RC_OK) res->epr.flags|=PINEX_ACL_CHKED; else fOK=rc==RC_FALSE; 
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
	// ??? reset cache
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
	buf.fill('\t',level); buf.append("access: ",8);
	if (nProps==0) buf.append("*\n",2);
	else for (unsigned i=0; i<nProps; i++) {buf.renderName(props[i]); if (i+1<nProps) buf.append(", ",2); else buf.append("\n",1);}
	if (queryOp!=NULL) queryOp->print(buf,level+1);
}

//------------------------------------------------------------------------------------------------

PathOp::~PathOp()
{
}

void PathOp::connect(PINEx **results,unsigned nRes)
{
	res=results[0]; queryOp->connect(&pex);
}

RC PathOp::next(const PINEx *)
{
	RC rc; const Value *pv; bool fOK;
	if (res!=NULL) res->cleanup(); if ((state&QST_EOF)!=0) return RC_EOF;
	if ((state&QST_INIT)!=0) {state&=~QST_INIT; if (nSkip>0 && (rc=initSkip())!=RC_OK) return rc;}
	for (;;) {
		if (pst==NULL) {
			if ((rc=queryOp->next())!=RC_OK) {state|=QST_EOF; return rc;}
			if ((rc=push())!=RC_OK) return rc; pst->v[0].setError(path[0].pid);
			if ((rc=getData(pex,&pst->v[0],1,NULL,ses,path[0].eid))!=RC_OK) {state|=QST_EOF; return rc;}
			pst->state=2; pst->vidx=0; if (fThrough) {if (res!=NULL) *res=pex; return RC_OK;}
		}
		switch (pst->state) {
		case 0:
			pst->state=1; assert(pst!=NULL && pst->idx>0);
			if (pst->idx>=nPathSeg && pst->rcnt>=path[pst->idx-1].rmin && path[pst->idx-1].filter==NULL) {/*printf("->\n");*/ return RC_OK;}
		case 1:
			pst->state=2; //printf("%*s(%d,%d):"_LX_FM"\n",(pst->idx-1+pst->rcnt-1)*2,"",pst->idx,pst->rcnt,res->id.pid);
			if ((rc=getBody(*res))!=RC_OK || (res->hpin->hdr.descr&HOH_HIDDEN)!=0) 
				{res->cleanup(); if (rc==RC_OK || rc==RC_NOACCESS || rc==RC_REPEAT || rc==RC_DELETED) {pop(); continue;} else {state|=QST_EOF; return rc;}}
			fOK=path[pst->idx-1].filter==NULL || ses->getStore()->queryMgr->condSatisfied((const Expr* const*)&path[pst->idx-1].filter,1,(const PINEx**)&res,1,params,nParams,ses);
			if (!fOK && !path[pst->idx-1].fLast) {res->cleanup(); pop(); continue;}
			if (pst->rcnt<path[pst->idx-1].rmax||path[pst->idx-1].rmax==0xFFFF) {
				if ((rc=ses->getStore()->queryMgr->loadV(pst->v[1],path[pst->idx-1].pid,*res,LOAD_SSV|LOAD_REF,ses,path[pst->idx-1].eid))==RC_OK) {if (pst->v[1].type!=VT_ERROR) pst->vidx=1;}
				else if (rc!=RC_NOTFOUND) {res->cleanup(); state|=QST_EOF; return rc;}
			}
			if (fOK && pst->rcnt>=path[pst->idx-1].rmin) {
				//pst->nSucc++;
				if (pst->idx<nPathSeg) {
					assert(pst->rcnt<=path[pst->idx-1].rmax||path[pst->idx-1].rmax==0xFFFF);
					for (;;) {
						if ((rc=ses->getStore()->queryMgr->loadV(pst->v[0],path[pst->idx].pid,*res,LOAD_SSV|LOAD_REF,ses,path[pst->idx].eid))!=RC_OK && rc!=RC_NOTFOUND)
							{res->cleanup(); state|=QST_EOF; return rc;}
						if (rc==RC_OK && pst->v[0].type!=VT_ERROR) {pst->vidx=0; break;}
						if (path[pst->idx].rmin!=0) break; if (pst->idx+1>=nPathSeg) return RC_OK; 
						unsigned s=pst->vidx; pst->vidx=0; if ((rc=push())!=RC_OK) return rc; pst->next->vidx=s;
					}
				} else if (path[pst->idx-1].filter!=NULL) {/*printf("->\n");*/ return RC_OK;}
				}
			res->cleanup();
		case 2:
			if (pst->vidx>=2) {pop(); continue;}	// rmin==0 && pst->nSucc==0 -> goto next seg
			switch (pst->v[pst->vidx].type) {
			default: pst->vidx++; continue;		// rmin==0 -> goto next seg
			case VT_REF: if (res!=NULL) *res=pst->v[pst->vidx].pin->getPID(); if ((rc=push())!=RC_OK) return rc; pst->next->vidx++; continue;
			case VT_REFID: if (res!=NULL) *res=pst->v[pst->vidx].id; if ((rc=push())!=RC_OK) return rc; pst->next->vidx++; continue;
			case VT_STRUCT:
				//????
				continue;
			case VT_COLLECTION: case VT_ARRAY: pst->state=3; pst->cidx=0; break;
			}
		case 3:
			pv=pst->v[pst->vidx].type==VT_COLLECTION?pst->v[pst->vidx].nav->navigate(pst->cidx==0?GO_FIRST:GO_NEXT):pst->cidx<pst->v[pst->vidx].length?&pst->v[pst->vidx].varray[pst->cidx]:(const Value*)0;
			if (pv!=NULL) {
				pst->cidx++;
				switch (pv->type) {
				default: continue;
				case VT_REF: if (res!=NULL) *res=pv->pin->getPID(); if ((rc=push())!=RC_OK) return rc; continue;
				case VT_REFID: if (res!=NULL) *res=pv->id; if ((rc=push())!=RC_OK) return rc; continue;
				case VT_STRUCT:
					//????
					continue;
				}
			}
			pst->vidx++; pst->state=2; continue;
		}
	}
}

RC PathOp::rewind()
{
	pex.cleanup(); while(pst!=NULL) pop();
	return queryOp!=NULL?queryOp->rewind():RC_OK;
}

RC PathOp::release()
{
	for (PathState *ps=pst; ps!=NULL; ps=ps->next) {
		if (ps->v[0].type==VT_COLLECTION) ps->v[0].nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID);
		if (ps->v[1].type==VT_COLLECTION) ps->v[1].nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID);
	}
	// ???
	return QueryOp::release();
}

void PathOp::getOpDescr(QODescr& qop)
{
	if (queryOp!=NULL) {
		queryOp->getOpDescr(qop); qop.level++;
		qop.flags&=~(QO_UNIQUE|QO_ALLPROPS|QO_PIDSORT|QO_REVERSIBLE);
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

RC Filter::next(const PINEx *skip)
{
	RC rc=RC_OK; assert(ses!=NULL && res!=NULL);
	if ((state&QST_INIT)!=0) {state&=~QST_INIT; if (nSkip>0 && (rc=initSkip())!=RC_OK) return rc;}
//	if (op==GO_FIRST || op==GO_LAST) state&=~(QST_BOF|QST_EOF); else if ((state&(op==GO_NEXT?QST_EOF:QST_BOF))!=0) return RC_EOF;
	if ((state&QST_EOF)!=0) return RC_EOF;
	for (; (rc=queryOp->next(skip))==RC_OK; skip=NULL) {
		if ((mode&QO_NODATA)==0 && (rc=queryOp->getData(*res,NULL,0))!=RC_OK) break;
		if (lProps!=0 && !condProps.test(res,lProps)) continue; bool fOK=true;
		if ((mode&QO_CLASS)==0) {
			for (CondIdx *ci=condIdx; ci!=NULL; ci=ci->next) {
				if (ci->param>=nParams) {fOK=false; break;}
				Value *pv=&params[ci->param];
				if (ci->expr!=NULL) {
					// ???
				} else {
					Value vv; if (res->getValue(ci->ks.propID,vv,LOAD_SSV,NULL)!=RC_OK) {fOK=false; break;}
					RC rc=Expr::calc((ExprOp)ci->ks.op,vv,pv,2,(ci->ks.flags&ORD_NCASE)!=0?CND_NCASE:0,ses);
					freeV(vv); if (rc!=RC_TRUE) {fOK=false; break;}
				}
			}
			if (!fOK) continue;
		}
		if (queries!=NULL && nQueries>0) {
			for (ulong i=0; i<nQueries; i++) if (!queries[i].qry->checkConditions(res,queries[i].params,queries[i].nParams,ses)) {fOK=false; break;}
			if (!fOK) continue;
		}
		const PINEx *pp=res;
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

RC ArrayFilter::next(const PINEx *skip)
{
	RC rc=RC_EOF; assert(res!=NULL);
	if ((state&QST_INIT)!=0) {state&=~QST_INIT; if (nSkip>0 && (rc=initSkip())!=RC_OK) return rc;}
	if (nPids!=0) for (; (rc=queryOp->next(skip))==RC_OK; skip=NULL) {
		if (res->id.pid==STORE_INVALID_PID && res->unpack()!=RC_OK) continue;
		if (BIN<PID>::find(res->id,pids,nPids)!=NULL) return RC_OK;
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
	unsigned cnt=0; RC rc;
	for (const PropDNF *p=this,*end=(const PropDNF*)((byte*)this+ldnf); p<end; p=(const PropDNF*)((byte*)(p+1)+int(p->nIncl+p->nExcl-1)*sizeof(PropertyID))) {
		unsigned i=0;
		if (p==this && nIncl!=0) {
			if (nPids<nIncl && (ma==NULL || (ps=(PropertyID*)ma->realloc(ps,(nPids=nIncl)*sizeof(PropertyID)))==NULL)) return RC_NORESOURCES;
			memcpy(ps,pids,(i=cnt=nIncl)*sizeof(PropertyID)); if ((byte*)(p+1)+int(p->nIncl-1)*sizeof(PropertyID)==(byte*)end) break;
		}
		for (unsigned all=p->nIncl+p->nExcl; i<all; i++) if ((rc=BIN<PropertyID>::insert(ps,cnt,p->pids[i],p->pids[i],ma,&nPids))!=RC_OK) return rc;
	}
	nPids=cnt; return RC_OK;
}

void PropDNF::normalize(PropDNF *&dnf,size_t& ldnf,MemAlloc *ma)
{
	//...
}
