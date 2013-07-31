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
#include "lock.h"
#include "session.h"
#include "expr.h"
#include "ftindex.h"
#include "queryprc.h"
#include "stmt.h"
#include "parser.h"
#include "service.h"

using namespace AfyKernel;

void QCtx::destroy()
{
	if (--refc==0) {
		for (unsigned i=0; i<(int)QV_ALL; i++) if (vals[i].fFree) freeV((Value*)vals[i].vals,vals[i].nValues,ses);
		ses->free((void*)this);
	}
}

QueryOp::QueryOp(QCtx *qc,unsigned qf) : qx(qc),queryOp(NULL),state(QST_INIT),nSkip(0),res(NULL),nOuts(1),qflags(qf),sort(NULL),nSegs(0),props(NULL),nProps(0),extsrc(NULL)
{
	qc->ref();
}

QueryOp::QueryOp(QueryOp *qop,unsigned qf) : qx(qop->qx),queryOp(qop),state(QST_INIT),nSkip(0),res(NULL),nOuts(qop->nOuts),qflags(qf),sort(NULL),nSegs(0),props(NULL),nProps(0),extsrc(NULL)
{
	qx->ref();
}

QueryOp::~QueryOp()
{
	delete queryOp; delete extsrc;
}

RC QueryOp::initSkip()
{
	RC rc=RC_OK;
	while (nSkip!=0) if ((rc=next())!=RC_OK || (--nSkip,rc=qx->ses->testAbortQ())!=RC_OK) break;
	return rc;
}

void QueryOp::connect(PINx **results,unsigned nRes)
{
	if (results!=NULL && nRes==1) res=results[0];
	if (queryOp!=NULL) queryOp->connect(results,nRes);
}

RC QueryOp::init()
{
	return RC_OK;
}

RC QueryOp::next(const PINx *skip)
{
	RC rc;
	if (extsrc!=NULL) {
		if ((rc=extsrc->next(skip))!=RC_EOF) return rc;
		delete extsrc; extsrc=NULL;
	}
	if ((state&QST_EOF)!=0) return RC_EOF;
	if ((state&QST_INIT)!=0) {
		state&=~QST_INIT; if ((rc=init())!=RC_OK || nSkip>0 && (rc=initSkip())!=RC_OK) {state|=QST_EOF; return rc;}
		state|=QST_BOF;
	} else state&=~QST_BOF;
	return advance(skip);
}

RC QueryOp::rewind()
{
	RC rc=queryOp!=NULL?queryOp->rewind():RC_INVOP;
	if (rc==RC_OK) state=state&~QST_EOF|QST_BOF;
	return rc;
}

void QueryOp::print(SOutCtx& buf,int level) const
{
}

RC QueryOp::count(uint64_t& cnt,unsigned nAbort)
{
	uint64_t c=0; RC rc; PINx qr(qx->ses),*pqr=&qr; if (res==NULL) connect(&pqr);
	while ((rc=next())==RC_OK) if (++c>nAbort) return RC_TIMEOUT;
	cnt=c; return rc==RC_EOF?RC_OK:rc;
}

RC QueryOp::loadData(PINx& qr,Value *pv,unsigned nv,ElementID eid,bool fSort,MemAlloc *ma)
{
	return queryOp!=NULL?queryOp->loadData(qr,pv,nv,eid,fSort,ma):RC_NOTFOUND;
}

RC QueryOp::getData(PINx& qr,Value *pv,unsigned nv,const PINx *qr2,ElementID eid,MemAlloc *ma)
{
	RC rc=RC_OK;
	if ((qr.getState()&(PEX_PAGE|PEX_PROPS))!=0) {
		if (pv!=NULL) for (unsigned i=0; i<nv; i++)
			if ((rc=qr.getV(pv[i].property,pv[i],LOAD_SSV,ma,eid))==RC_NOTFOUND) rc=RC_OK; else if (rc!=RC_OK) break;
		return rc;
	}
	if (qr2!=NULL && (qr2->getState()&(PEX_PAGE|PEX_PROPS))!=0) {
		// check same page
	}
	return loadData(qr,pv,nv,eid,false,ma);
}

RC QueryOp::getData(PINx **pqr,unsigned npq,const PropList *pl,unsigned npl,Value *vals,MemAlloc *ma)
{
	RC rc;
	for (unsigned i=0; i<npl; ++pl,++i) {
		for (unsigned j=0; j<pl->nProps; j++) vals[j].setError(pl->props[j]);
		if (pqr!=NULL && i<npq && (rc=queryOp->getData(*pqr[i],vals,pl->nProps,NULL,STORE_COLLECTION_ID,ma))!=RC_OK) return rc;
		vals+=pl->nProps;
	}
	return RC_OK;
}

void QueryOp::unique(bool f)
{
	if (queryOp!=NULL) queryOp->unique(f);
}

void QueryOp::reverse()
{
	if (queryOp!=NULL) queryOp->reverse();
}

RC QueryOp::getBody(PINx& pe)
{
	RC rc; if ((pe.epr.flags&PINEX_ADDRSET)==0) pe=PageAddr::noAddr;
	if ((rc=qx->ses->getStore()->queryMgr->getBody(pe,(qflags&QO_FORUPDATE)!=0?TVO_UPD:TVO_READ,0))==RC_OK&&(rc=qx->ses->testAbortQ())==RC_OK) {
		assert(pe.getAddr().defined() && (pe.epr.flags&PINEX_ADDRSET)!=0);
	}
	return rc;
}

RC QueryOp::createCommOp(PINx *pcb,const byte *er,size_t l)
{
	assert(extsrc==NULL); PINx cb(qx->ses); RC rc; ServiceCtx *sctx=NULL;
	if (pcb==NULL) {if (er!=NULL && l!=0) {memcpy(cb.epr.buf,er,cb.epr.lref=(byte)l); pcb=&cb;} else if ((pcb=res)==NULL) return RC_INTERNAL;}
	if (pcb->isPartial() && (pcb->pb.isNull() && (rc=qx->ses->getStore()->queryMgr->getBody(*pcb))!=RC_OK || (rc=pcb->load(LOAD_SSV))!=RC_OK)) return rc;
	if ((pcb->getMetaType()&PMT_COMM)==0) return RC_EOF;
	ValueV vv(pcb->properties,pcb->nProperties); if (pcb==res) pcb->setProps(NULL,0);
	if ((rc=qx->ses->prepare(sctx,pcb->id,vv.vals,vv.nValues,0))!=RC_OK) return rc; assert(sctx!=NULL);	// params ->ISRV_NOCACHE
	CommOp *co=new(qx->ses) CommOp(qx,sctx,vv,0); if (co==NULL) {sctx->destroy(); return RC_NORESOURCES;}
	co->connect(res!=NULL?&res:&pcb); if ((rc=co->advance())==RC_OK) extsrc=co; else delete co;
	return rc;
}

//------------------------------------------------------------------------------------------------

CommOp::~CommOp()
{
	if (sctx!=NULL) sctx->destroy();
	if (params.fFree) freeV((Value*)params.vals,params.nValues,qx->ses);
}

RC CommOp::advance(const PINx *)
{
	if ((state&QST_EOF)!=0) return RC_EOF;
	state&=~QST_INIT; if (res!=NULL) res->cleanup();
	RC rc=sctx->invoke(params.vals,params.nValues,res);
	if (rc!=RC_OK) state|=QST_EOF; else if (res!=NULL) res->epr.flags|=PINEX_DERIVED;
	return rc;
}

//------------------------------------------------------------------------------------------------

LoadOp::LoadOp(QueryOp *q,const PropList *p,unsigned nP,unsigned qf) : QueryOp(q,qf),nPls(nP) {
	qf=q->getQFlags(); qflags|=qf&(QO_UNIQUE|QO_STREAM);
	if ((qflags&QO_REORDER)==0) {qflags|=qf&(QO_IDSORT|QO_REVERSIBLE); sort=q->getSort(nSegs);}
	if (p!=NULL && nP!=0) {
		memcpy(pls,p,nP*sizeof(PropList));
		for (unsigned i=0; i<nP; i++) if ((qflags&QO_VCOPIED)!=0 && pls[i].props!=NULL && !pls[i].fFree) {
			PropList& pl=*(PropList*)&pls[i];
			PropertyID *pp=new(qx->ses) PropertyID[pl.nProps]; if (pp==NULL) throw RC_NORESOURCES;
			memcpy(pp,pl.props,pl.nProps*sizeof(PropertyID)); pl.props=pp; pl.fFree=true;
		}
		props=pls; nProps=nP;
	}
	 /*else*/ qflags|=QO_ALLPROPS;
}

LoadOp::~LoadOp()
{
	if (pls!=NULL) for (unsigned i=0; i<nPls; i++) if (pls[i].props!=NULL && pls[i].fFree) qx->ses->free(pls[i].props);
}

void LoadOp::connect(PINx **rs,unsigned nR)
{
	results=rs; nResults=nR; queryOp->connect(rs,nR);
}

RC LoadOp::advance(const PINx *skip)
{
	RC rc=RC_OK; assert(qx->ses!=NULL);
	for (; (rc=queryOp->next(skip))==RC_OK; skip=NULL) {
		for (unsigned i=0; i<nResults; i++) if ((results[i]->epr.flags&PINEX_DERIVED)==0) {
			results[i]->resetProps(); results[i]->fReload=1; if ((rc=qx->ses->testAbortQ())!=RC_OK) return rc;
			if ((rc=getBody(*results[i]))!=RC_OK || (results[i]->mode&PIN_HIDDEN)!=0) 
				{results[i]->cleanup(); if (rc==RC_OK || rc==RC_NOACCESS || rc==RC_REPEAT || rc==RC_DELETED) {rc=RC_FALSE; break;} else return rc;}	// cleanup all
			if ((qflags&(QO_RAW|QO_FORUPDATE))==0 && nResults==1 && (results[i]->getMetaType()&PMT_COMM)!=0 && (rc=createCommOp())!=RC_EOF) return rc;
			if ((rc=qx->ses->getStore()->queryMgr->checkLockAndACL(*results[i],(qflags&QO_FORUPDATE)!=0?TVO_UPD:TVO_READ,this))!=RC_OK) break;
		}
		if (rc==RC_OK) break;
	}
	if (rc!=RC_OK) state|=QST_EOF;
	return rc;
}

RC LoadOp::count(uint64_t& cnt,unsigned nAbort)
{
	return queryOp->count(cnt,nAbort);
}

RC LoadOp::rewind()
{
	// ??? reset cache
	RC rc=queryOp!=NULL?queryOp->rewind():RC_OK;
	if (rc==RC_OK) state=state&~QST_EOF|QST_BOF;
	return rc;
}
	
RC LoadOp::loadData(PINx& qr,Value *pv,unsigned nv,ElementID eid,bool fSort,MemAlloc *ma)
{
	//...
	return RC_OK;
}

void LoadOp::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("access: ",8);
	if (nProps==0) buf.append("*\n",2);
	else if (nProps==1) {
		for (unsigned i=0; i<props[0].nProps; i++) {buf.renderName(props[0].props[i]); if (i+1<props[0].nProps) buf.append(", ",2); else buf.append("\n",1);}
	} else {
		buf.append("...\n",4);
	}
	if (queryOp!=NULL) queryOp->print(buf,level+1);
}

//------------------------------------------------------------------------------------------------
PathOp::PathOp(QueryOp *qop,const PathSeg *ps,unsigned nSegs,unsigned qf) 
	: QueryOp(qop,qf|(qop->getQFlags()&QO_STREAM)),Path(qx->ses,ps,nSegs,(qf&QO_VCOPIED)!=0),pex(qx->ses),ppx(&pex),saveID(PIN::noPID)
{
	saveEPR.lref=0; saveEPR.flags=0;
}

PathOp::~PathOp()
{
}

void PathOp::connect(PINx **results,unsigned nRes)
{
	res=results[0]; queryOp->connect(&ppx);
}

RC PathOp::advance(const PINx *)
{
	RC rc; const Value *pv; bool fOK; PID id; PathState *spst;
	if (res!=NULL) {res->cleanup(); *res=saveID; res->epr=saveEPR;}
	for (;;) {
		if (pst==NULL) {
			if ((rc=queryOp->next())!=RC_OK) {state|=QST_EOF; return rc;}
			if ((rc=pex.getID(id))!=RC_OK || (rc=push(id))!=RC_OK) return rc;
			if (path[0].nPids!=1) {
				pst->v[0].setError(path[0].nPids!=0?path[0].pids[0]:PROP_SPEC_ANY);	//multiple propID getData() or all props
			} else pst->v[0].setError(path[0].pid);
			if ((rc=getData(pex,&pst->v[0],1,NULL,path[0].eid))!=RC_OK) {state|=QST_EOF; return rc;}
			pst->state=2; pst->vidx=0; if (fThrough) {if (res!=NULL) {pex.moveTo(*res); save();} return RC_OK;}
		}
		switch (pst->state) {
		case 0:
			pst->state=1; assert(pst!=NULL && pst->idx>0);
			if (pst->idx>=nPathSeg && pst->rcnt>=path[pst->idx-1].rmin && path[pst->idx-1].filter==NULL) {
				if (pst->vidx>=2 && pst->rcnt>=path[pst->idx-1].rmax) pop();
				save(); /*printf("->\n");*/ return RC_OK;
			}
		case 1:
			pst->state=2; //res->getID(id); printf("%*s(%d,%d):"_LX_FM"\n",(pst->idx-1+pst->rcnt-1)*2,"",pst->idx,pst->rcnt,id.pid);
			if ((rc=getBody(*res))!=RC_OK || (res->getFlags()&PIN_HIDDEN)!=0 || (rc=res->getID(id))!=RC_OK)
				{res->cleanup(); if (rc==RC_OK || rc==RC_NOACCESS || rc==RC_REPEAT || rc==RC_DELETED) {pop(); continue;} else {state|=QST_EOF; return rc;}}
			fOK=path[pst->idx-1].filter==NULL || Expr::condSatisfied((const Expr* const*)&path[pst->idx-1].filter,1,EvalCtx(qx->ses,NULL,0,(PIN**)&res,1,qx->vals,QV_ALL));
			if (!fOK && !path[pst->idx-1].fLast) {res->cleanup(); pop(); continue;}
			if (pst->rcnt<path[pst->idx-1].rmax||path[pst->idx-1].rmax==0xFFFF) {
				if (path[pst->idx-1].nPids!=1) {
					//????
					return RC_INTERNAL;
				} else if ((rc=qx->ses->getStore()->queryMgr->loadV(pst->v[1],path[pst->idx-1].pid,*res,LOAD_SSV|LOAD_REF,qx->ses,path[pst->idx-1].eid))==RC_OK) {
					if (!pst->v[1].isEmpty()) pst->vidx=1;
				} else if (rc!=RC_NOTFOUND) {res->cleanup(); state|=QST_EOF; return rc;}
			}
			if (fOK && pst->rcnt>=path[pst->idx-1].rmin) {
				//pst->nSucc++;
				if (pst->idx<nPathSeg) {
					assert(pst->rcnt<=path[pst->idx-1].rmax||path[pst->idx-1].rmax==0xFFFF);
					for (;;) {
						if (path[pst->idx].nPids!=1) {
							//????
							return RC_INTERNAL;
						} else if ((rc=qx->ses->getStore()->queryMgr->loadV(pst->v[0],path[pst->idx].pid,*res,LOAD_SSV|LOAD_REF,qx->ses,path[pst->idx].eid))!=RC_OK && rc!=RC_NOTFOUND)
							{res->cleanup(); state|=QST_EOF; return rc;}
						if (rc==RC_OK && !pst->v[0].isEmpty()) {pst->vidx=0; break;}
						if (path[pst->idx].rmin!=0) break; if (pst->idx+1>=nPathSeg) {save(); return RC_OK;}
						unsigned s=pst->vidx; pst->vidx=0; if ((rc=push(id))!=RC_OK) return rc; pst->next->vidx=s;
					}
				} else if (path[pst->idx-1].filter!=NULL) {/*printf("->\n");*/ save(); return RC_OK;}
			}
			res->cleanup();
		case 2:
			if (pst->vidx>=2) {pop(); continue;}	// rmin==0 && pst->nSucc==0 -> goto next seg
			switch (pst->v[pst->vidx].type) {
			default: pst->vidx++; continue;		// rmin==0 -> goto next seg
			case VT_REF: id=pst->v[pst->vidx].pin->getPID(); spst=pst; if (res!=NULL) *res=id; if ((rc=push(id))!=RC_OK) return rc; spst->vidx++; continue;
			case VT_REFID: id=pst->v[pst->vidx].id; spst=pst; if (res!=NULL) *res=id; if ((rc=push(id))!=RC_OK) return rc; spst->vidx++; continue;
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
				case VT_REF: id=pv->pin->getPID(); if (res!=NULL) *res=id; if ((rc=push(id))!=RC_OK) return rc; continue;
				case VT_REFID: id=pv->id; if (res!=NULL) *res=id; if ((rc=push(id))!=RC_OK) return rc; continue;
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
	RC rc=queryOp!=NULL?queryOp->rewind():RC_OK;
	if (rc==RC_OK) state=state&~QST_EOF|QST_BOF;
	return rc;
}

void PathOp::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("path: ",6);
	for (unsigned i=0; i<nPathSeg; i++) buf.renderPath(path[i]);
	buf.append("\n",1); if (queryOp!=NULL) queryOp->print(buf,level+1);
}

//------------------------------------------------------------------------------------------------

Filter::Filter(QueryOp *qop,unsigned nqs,unsigned qf)
	: QueryOp(qop,qf|qop->getQFlags()),conds(NULL),nConds(0),condIdx(NULL),nCondIdx(0),queries(NULL),nQueries(nqs)
{
	sort=qop->getSort(nSegs); props=qop->getProps(nProps);
}

Filter::~Filter()
{
	if (queries!=NULL) {
		for (unsigned i=0; i<nQueries; i++) {QueryWithParams& cs=queries[i]; cs.qry->destroy(); if (cs.params!=NULL) freeV((Value*)cs.params,cs.nParams,qx->ses);}
		qx->ses->free(queries);
	}
	if ((qflags&QO_VCOPIED)!=0) {
		if (conds!=NULL) {
			for (unsigned i=0; i<nConds; i++) ((Expr*)conds[i])->destroy(); 
			qx->ses->free((void*)conds);
		}
		for (CondIdx *ci=condIdx,*ci2; ci!=NULL; ci=ci2) {ci2=ci->next; ci->~CondIdx(); qx->ses->free(ci);}
	}
}

void Filter::connect(PINx **rs,unsigned nR)
{
	results=rs; nResults=nR; queryOp->connect(rs,nR);
}

RC Filter::advance(const PINx *skip)
{
	RC rc=RC_OK; assert(qx->ses!=NULL && results!=NULL);
	EvalCtx ectx(qx->ses,qx->ectx!=NULL?qx->ectx->env:NULL,qx->ectx!=NULL?qx->ectx->nEnv:0,(PIN**)results,nResults,qx->vals,QV_ALL,qx->ectx,NULL,(qflags&QO_CLASS)!=0?ECT_CLASS:ECT_QUERY);		// move to FilterOp
	for (; (rc=queryOp->next(skip))==RC_OK; skip=NULL) {
		if ((qflags&QO_NODATA)==0 && (rc=queryOp->getData(*results[0],NULL,0))!=RC_OK) break;		// other vars ???
		if (conds==NULL || Expr::condSatisfied(conds,nConds,ectx)) {
			if ((qflags&QO_CLASS)==0) {
				bool fOK=true;
				for (CondIdx *ci=condIdx; ci!=NULL; ci=ci->next) {
					if (ci->param>=qx->vals[QV_PARAMS].nValues) {fOK=false; break;}
					const Value *pv=&qx->vals[QV_PARAMS].vals[ci->param];
					if (ci->expr!=NULL) {
						// ???
					} else {
						Value vv; if (results[0]->getV(ci->ks.propID,vv,LOAD_SSV,NULL)!=RC_OK) {fOK=false; break;}
						RC rc=Expr::calc((ExprOp)ci->ks.op,vv,pv,2,(ci->ks.flags&ORD_NCASE)!=0?CND_NCASE:0,qx->ses);
						freeV(vv); if (rc!=RC_TRUE) {fOK=false; break;}
					}
				}
				if (!fOK) continue;
			}
			if (queries!=NULL && nQueries>0) {
				bool fOK=true;
				for (unsigned i=0; i<nQueries; i++) {
					ValueV vv(queries[i].params,queries[i].nParams); 
					if (!queries[i].qry->checkConditions(EvalCtx(ectx.ses,ectx.env,ectx.nEnv,ectx.vars,ectx.nVars,&vv,1,ectx.stack,NULL,ectx.ect)))
						{fOK=false; break;}
				}
				if (!fOK) continue;
			}
			break;
		}
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

ArrayFilter::ArrayFilter(QueryOp *q,const Value *pds,unsigned nP) : QueryOp(q,q->getQFlags()),pids(pds),nPids(nP)
{
	sort=q->getSort(nSegs); props=q->getProps(nProps);
	if ((qflags&QO_VCOPIED)!=0) copyV(pds,nP,*(Value**)&pids,qx->ses);
}

ArrayFilter::~ArrayFilter()
{
	if ((qflags&QO_VCOPIED)!=0) freeV((Value*)pids,nPids,qx->ses);
}

RC ArrayFilter::advance(const PINx *skip)
{
	RC rc=RC_EOF; assert(qx->ses!=NULL && res!=NULL);
	if (nPids!=0) for (PID id; (rc=queryOp->next(skip))==RC_OK; skip=NULL)
		if (res->getID(id)==RC_OK) for (unsigned i=0; i<nPids; i++) if (pids[i].type==VT_REFID && pids[i].id==id) return RC_OK;
	state|=QST_EOF; return rc;
}

void ArrayFilter::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("array filter\n",13);
	if (queryOp!=NULL) queryOp->print(buf,level+1);
}
