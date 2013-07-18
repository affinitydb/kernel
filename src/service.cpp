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

Written by Mark Venguerov 2012-2013

**************************************************************************************/
#include "qmgr.h"
#include "service.h"
#include "pinex.h"
#include "queryprc.h"
#include "stmt.h"
#include "maps.h"
#include "blob.h"
#include "fio.h"

using namespace	Afy;
using namespace AfyKernel;

RC IService::create(IServiceCtx *ctx,uint32_t& dscr,IService::Processor *&ret)
{
	return RC_INVOP;
}

RC IService::listen(ISession *ses,const Value *,unsigned,const Value *,unsigned,unsigned,IListener *&)
{
	return RC_INVOP;
}

RC IService::resolve(ISession *ses,const Value *vals,unsigned nVals,AddrInfo& res)
{
	return RC_INVOP;
}

size_t IService::getBufSize() const
{
	return SRV_DEFAULT_BUFSIZE;
}

void IService::getEnvelope(size_t& lHeader,size_t& lTrailer) const
{
	lHeader=lTrailer=0;
}

void IService::shutdown()
{
}

ServiceCtx::ServiceCtx(Session *s,const Value *par,uint32_t nPar,bool fW,IListener *ls) 
	: PIN(s),list(this),id(PIN::defPID),fWrite(fW),lst(ls),cst(fW?CST_WRITE:CST_READ),sstate(0),procs(NULL),nProcs(0),cur(0),resume(0),
	toggleIdx(~0u),flushIdx(~0u),params(par),nParams(nPar),pR(NULL),bufCache(NULL),msgLength(0ULL),bufAlloc(s),pins((MemAlloc*)this),xpins(SRV_DEFAULT_BATCH),crs(NULL)
{
	action.setEmpty();
}

ServiceCtx::~ServiceCtx()
{
	remove(); list.remove(); cleanup(true);
}

void ServiceCtx::cleanup(bool fDestroy)
{
	sstate=0; flushIdx=~0u; resume=0; msgLength=0ULL;
	// cleanup PIN
	if (crs!=NULL) {crs->destroy(); crs=NULL;}
	if (pins!=0) {
		uint32_t np=0; PIN **pp=pins.get(np);
		for (uint32_t i=0; i<np; i++) if (pp[i]!=NULL) pp[i]->destroy();
		free(pp);
	}
	if (procs!=NULL) {
		for (unsigned i=0; i<nProcs; i++) {
			ProcDscr *prc=&procs[i]; if (prc->proc!=NULL) prc->proc->cleanup(this,fDestroy);
			prc->dscr&=ISRV_PROC_MODE; prc->buf=NULL; prc->lbuf=prc->oleft=0;
			//freeV(prc->inp);	//????
		}
		if (fDestroy) {free(procs); procs=NULL;}
	}
	if (fDestroy) freeV(action);
	bufCache=NULL; bufAlloc.release();
}

RC ServiceCtx::build(const Value *vals,unsigned nVals)
{
	StoreCtx *ctx=ses->getStore(); if (ctx->serviceProviderTab==NULL) return RC_INTERNAL;
	const Value *srv=VBIN::find(lst!=NULL?PROP_SPEC_LISTEN:PROP_SPEC_SERVICE,vals,nVals); RC rc=RC_OK;
	if (srv==NULL) {
		if (lst==NULL||procs!=NULL||cur!=0) rc=RC_INVPARAM;
		else if ((procs=new(this) ProcDscr[2])==NULL) rc=RC_NORESOURCES;
		else {
			nProcs=2; memset(procs,0,sizeof(ProcDscr)*2); 
			procs[0].dscr=ISRV_ENDPOINT|ISRV_READ; procs[0].srvInfo=vals; procs[0].nSrvInfo=nVals;
			if ((rc=lst->create(this,procs[0].dscr,procs[0].proc))==RC_OK) {
				fill(&procs[0],lst->getService(),vals,nVals); procs[1].sid=SERVICE_AFFINITY;
			}
		}
		return rc;
	}
	unsigned nSrv=1;
	if (srv->type==VT_ARRAY) {
		nSrv=srv->length; srv=srv->varray;
		if (procs==NULL) {
			if ((procs=new(this) ProcDscr[nSrv])==NULL) return RC_NORESOURCES;
			nProcs=nSrv; memset(procs,0,nSrv*sizeof(ProcDscr)); assert(cur==0);
		} else if (nSrv!=1) {
			assert(cur<nProcs);
			if ((procs=(ProcDscr*)realloc(procs,(nProcs+nSrv-1)*sizeof(ProcDscr),nProcs*sizeof(ProcDscr)))==NULL) return RC_NORESOURCES;
			nProcs+=nSrv; memset(procs+cur+1,0,(nSrv-1)*sizeof(ProcDscr));
		}
	}
	for (unsigned i=0; rc==RC_OK && i<nSrv; ++i,++srv) {
		URIID id=STORE_INVALID_CLASSID;
		switch (srv->type) {
		default: return RC_TYPE;
		case VT_URIID: id=srv->uid; break;
		case VT_STRUCT: if ((rc=build(srv->varray,srv->length))!=RC_OK) return rc; continue;
		case VT_REF: if ((rc=build(((PIN*)srv->pin)->properties,((PIN*)srv->pin)->nProperties))!=RC_OK) return rc; continue;
		case VT_REFID:
			{PINx ref(ses,srv->id);
			if ((rc=ref.load(LOAD_SSV))!=RC_OK || (rc=build(ref.properties,ref.nProperties))!=RC_OK) return rc;}
			continue;
		case VT_MAP:
			if (Value::find(PROP_SPEC_CONDITION,vals,nVals)==NULL) return RC_INVPARAM;
			break;
		}
		if (procs==NULL) {
			if ((procs=new(this) ProcDscr)==NULL) return RC_NORESOURCES;
			nProcs=1; memset(procs,0,sizeof(ProcDscr)); assert(cur==0);
		}
		if (id==STORE_INVALID_CLASSID || cur>=nProcs) return RC_INVPARAM;
		ProcDscr *prc=&procs[cur]; prc->sid=id; prc->meta=srv->meta;
		if (id==STORE_INVALID_CLASSID) {
			sstate|=ISRV_NOCACHE; prc->srvInfo=vals; prc->nSrvInfo=nVals;
		} else if (id==SERVICE_AFFINITY) {
			prc->srvInfo=vals; prc->nSrvInfo=nVals;
			if (cst==CST_READ && cur!=nProcs+1 && lst!=NULL) {cst=CST_SERVER; if (toggleIdx==~0u) toggleIdx=cur++;}
		} else if (lst!=NULL && (cur==0 || cst==CST_SERVER && cur+1==nProcs)) {
			prc->dscr=cur==0?ISRV_ENDPOINT|ISRV_READ:ISRV_ENDPOINT|ISRV_WRITE;
			if ((rc=lst->create(this,prc->dscr,prc->proc))==RC_OK) {
				fill(prc,lst->getService(),vals,nVals); cur++;
				if ((srv->meta&META_PROP_SYNC)!=0) prc->dscr|=ISRV_WAIT;
			}
		} else
			rc=getProcessor(prc,vals,nVals);
	}
	if (rc==RC_OK && lst!=NULL && toggleIdx==~0u && cur==nProcs) {
		const Value *act=VBIN::find(PROP_SPEC_ACTION,vals,nVals);
		if (act!=NULL) {action=*act; setHT(action);}
	}
	return rc;
}

RC ServiceCtx::getProcessor(ProcDscr *prc,const Value *vals,unsigned nVals)
{
	ServiceProviderTab::Find find(*ses->getStore()->serviceProviderTab,prc->sid); RC rc=RC_NOTFOUND;
	for (ServiceProvider *ah=find.findLock(RW_S_LOCK);;ah=ah->stack) {
		IService *isrv=ah!=NULL?ah->handler:ses->getStore()->defaultService;
		if (isrv!=NULL) {
			prc->srvInfo=vals; prc->nSrvInfo=nVals; if ((prc->meta&META_PROP_ALT)!=0) prc->dscr|=ISRV_ALT;
			switch (cst) {
			case CST_READ:
				prc->dscr=cur==0?ISRV_ENDPOINT|ISRV_READ:ISRV_READ;
				if ((rc=isrv->create(this,prc->dscr,prc->proc))==RC_OK) {
					if ((prc->dscr&ISRV_SERVER)!=0) if (lst!=NULL && cur!=nProcs+1) {cst=CST_SERVER; if (toggleIdx==~0u) toggleIdx=cur;}
				} else if (cur==0 && nProcs>1) {
					prc->dscr=ISRV_WRITE; if ((rc=isrv->create(this,prc->dscr,prc->proc))==RC_OK) cst=CST_REQUEST;
				}
				break;
			case CST_WRITE:
				prc->dscr=(sstate&ISRV_REVERSE)==0&&cur+1==nProcs?ISRV_ENDPOINT|ISRV_WRITE:ISRV_WRITE;
				if ((rc=isrv->create(this,prc->dscr,prc->proc))!=RC_OK && cur==0 && nProcs>1)
					{prc->dscr=ISRV_ENDPOINT|ISRV_WRITE; if ((rc=isrv->create(this,prc->dscr,prc->proc))==RC_OK) sstate|=ISRV_REVERSE;}
				break;
			case CST_REQUEST:
				prc->dscr=toggleIdx!=~0u?ISRV_READ:ISRV_WRITE;
				if ((rc=isrv->create(this,prc->dscr,prc->proc))!=RC_OK && toggleIdx==~0u)
					{prc->dscr=ISRV_ENDPOINT|ISRV_REQUEST; toggleIdx=cur; rc=isrv->create(this,prc->dscr,prc->proc);}
				break;
			case CST_SERVER:
				prc->dscr=cur+1==nProcs?ISRV_ENDPOINT|ISRV_WRITE:ISRV_WRITE;
				rc=isrv->create(this,prc->dscr,prc->proc); break;
			}
			if (rc==RC_REPEAT || prc->proc==NULL) rc=RC_NOTFOUND;
			else if (rc==RC_OK) {
				fill(prc,isrv,vals,nVals);
				if ((prc->dscr&ISRV_ENDPOINT)!=0 && (prc->meta&META_PROP_SYNC)!=0) prc->dscr|=ISRV_WAIT;
				if ((prc->dscr&ISRV_ENVELOPE)!=0) {
					if (cur==0) rc=RC_INVOP;
					else if ((prc->lHeader|prc->lTrailer)!=0) for (ProcDscr *prev=prc; --prev>=procs;)
						if ((prev->dscr&ISRV_ALLOCBUF)!=0 || prev==procs) {prev->lBuffer=max(prc->lBuffer,prev->lBuffer); prev->lHeader+=prc->lHeader; prev->lTrailer+=prc->lTrailer; break;}
				}
				cur++;
			}
			if (rc!=RC_OK && ah!=NULL) continue;
		}
		find.unlock(); break;
	}
	return rc;
}

void ServiceCtx::fill(ServiceCtx::ProcDscr *prc,IService *isrv,const Value *vals,unsigned nVals)
{
	sstate|=prc->dscr&ISRV_NOCACHE;
	if ((prc->dscr&ISRV_ENVELOPE)!=0) {
		isrv->getEnvelope(prc->lHeader,prc->lTrailer); prc->dscr&=~ISRV_ALLOCBUF;
	} else if ((prc->dscr&ISRV_PROC_MASK)==(ISRV_ENDPOINT|ISRV_WRITE))
		prc->dscr&=~ISRV_ALLOCBUF;
	else if ((prc->dscr&ISRV_ALLOCBUF)!=0) {
		const Value *bufsz=VBIN::find(PROP_SPEC_BUFSIZE,vals,nVals);
		if (bufsz!=NULL && (bufsz->type==VT_INT && bufsz->i>0 || bufsz->type==VT_UINT) && bufsz->ui!=0 && bufsz->ui!=~0u) prc->lBuffer=bufsz->ui;
		else if ((prc->lBuffer=isrv->getBufSize())==0) prc->lBuffer=SRV_DEFAULT_BUFSIZE;
	}
}

RC ServiceCtx::invoke(const Value *vals,unsigned nVals,Value *res)
{
	RC rc=RC_OK;
	try {
		ProcDscr *prc=&procs[cur=resume]; const Value *ext=NULL; unsigned vidx=0; Value inp,out; inp.setEmpty();
		if (cst==CST_WRITE || cst==CST_REQUEST && cur==0) {
			assert(cur==0 && (prc->dscr&ISRV_WRITE)!=0);
			if ((ext=VBIN::find(cst==CST_REQUEST?PROP_SPEC_REQUEST:PROP_SPEC_CONTENT,vals,nVals))!=NULL) {
				if (ext->type==VT_ARRAY) inp=*ext->varray;
				else if (ext->type!=VT_COLLECTION) inp=*ext;
				else {const Value *cv=ext->nav->navigate(GO_FIRST); if (cv!=NULL) inp=*cv; else ext=NULL;}
				setHT(inp);
			}
			sstate=sstate&~ISRV_PROC_MASK|ISRV_WRITE;
		} else {
			inp=prc->inp; sstate|=prc->dscr&ISRV_MOREOUT; prc->dscr&=~ISRV_MOREOUT; sstate=sstate&~ISRV_PROC_MASK|ISRV_READ;
		}
		for (unsigned prcst=sstate;;prcst=sstate) {
			if ((prc->dscr&ISRV_ALLOCBUF)==0 || cst==CST_REQUEST && cur==toggleIdx && (sstate&ISRV_WRITE)!=0) out.setEmpty();
			else {
				if (prc->buf==NULL) {
					size_t l=(uint32_t)(prc->lBuffer+prc->lHeader+prc->lTrailer),lb=~size_t(0u); BufCache **buf=NULL;
					for (BufCache **pbc=&bufCache,*bc; (bc=*pbc)!=NULL; pbc=&bc->next)
						if (bc->l==l) {buf=pbc; lb=l; break;} else if (bc->l>l && bc->l<lb) {buf=pbc; lb=bc->l;}
					if (buf!=NULL) {prc->buf=(byte*)(*buf); prc->lbuf=(uint32_t)lb; *buf=(*buf)->next;}
					else if ((prc->buf=(byte*)bufAlloc.malloc(l))!=NULL) prc->lbuf=(uint32_t)l;
					else {rc=RC_NORESOURCES; break;}
					prc->oleft=prc->lbuf-uint32_t(prc->lHeader+prc->lTrailer);
				}
				out.set(prc->buf+prc->lbuf-(uint32_t)prc->lTrailer-prc->oleft,prc->oleft);
			}
			if (prc->proc!=NULL) {
				prcst|=prc->dscr&ISRV_WAIT; ses->active=this; rc=prc->proc->invoke(this,inp,out,prcst); ses->active=NULL;
				if ((prcst&ISRV_FLUSH)!=0) {prc->dscr|=ISRV_FLUSH; prcst&=~ISRV_FLUSH; if (cur<flushIdx) flushIdx=cur;}
				if ((prcst&ISRV_MOREOUT)==0) {prc->dscr&=~ISRV_MOREOUT; if (resume==cur) resume=prc->prevIdx;}
				else {prc->dscr|=ISRV_MOREOUT; prc->inp=inp; inp.setEmpty(); if (resume!=cur) {prc->prevIdx=resume; resume=cur;}}
				prc->dscr|=prcst&ISRV_EOF; prcst|=sstate&ISRV_FLUSH;
			} else if (prc->sid==STORE_INVALID_CLASSID) {
				const Value *pv=Value::find(PROP_SPEC_CONDITION,prc->srvInfo,prc->nSrvInfo),*map=Value::find(PROP_SPEC_SERVICE,prc->srvInfo,prc->nSrvInfo);
				assert(pv!=NULL && map!=NULL && map->type==VT_MAP);
//				if ((pv=getData()->find(*pv))==NULL || (pv=map->map->find(*pv))==NULL || pv->type!=VT_URIID) rc=RC_NOTFOUND;
//				else {prc->sid=pv->uid; if ((rc=getProcessor(prc,vals,nVals,false))==RC_OK) continue;}
			} else {
				assert(prc->sid==SERVICE_AFFINITY); rc=server(prc,inp,out);
			}
			sstate&=~(ISRV_FLUSH|ISRV_MOREOUT|ISRV_WAIT|ISRV_SWITCH);
			if ((prcst&(ISRV_REFINP|ISRV_KEEPINP))==0 && cur>0) {
				if (!isString((ValueType)inp.type)) freeV(inp);
				else {
					ProcDscr *pfree=prc-1;
					if ((pfree->dscr&ISRV_ALLOCBUF)==0) {
						//???
					}
					if ((pfree->dscr&ISRV_ALLOCBUF)!=0) {
						if (pfree->lbuf>=sizeof(BufCache)) {BufCache *buf=(BufCache*)pfree->buf; buf->l=pfree->lbuf; buf->next=bufCache; bufCache=buf;}
						pfree->buf=NULL; pfree->oleft=pfree->lbuf=0;
					}
				}
			} else if ((prc->dscr&ISRV_PROC_MASK)==(ISRV_WRITE|ISRV_ENDPOINT)) {
				//???
			}
			if ((sstate&ISRV_WRITE)!=0 && (prc->dscr&(ISRV_ENDPOINT|ISRV_REQUEST))!=0 ||
				(prc->dscr&ISRV_SERVER)!=0 && cur+1>=nProcs) {out.setEmpty(); prcst|=ISRV_NEEDMORE;}
			if (rc!=RC_OK) {
				if (rc!=RC_EOF) {
					// check PROP_SPEC_EXCEPTION
					break;
				}
				if (flushIdx==~0u) {if (cst==CST_WRITE || cst==CST_SERVER) rc=RC_OK; break;}
				if (cst==CST_SERVER) sstate=(sstate&~ISRV_PROC_MASK)|(flushIdx<=toggleIdx?ISRV_READ:ISRV_WRITE);
				prc=flush(inp);
			} else {
				if (isString((ValueType)out.type) && out.length!=0) {
					if (out.length<=prc->oleft) prc->oleft-=out.length; else {rc=RC_INVPARAM; break;}
					if (prc->oleft!=0 && (prc->dscr&(ISRV_ENDPOINT|ISRV_ALLOCBUF))==ISRV_ALLOCBUF && (prcst&ISRV_FLUSH)==0 && cur+1<nProcs)
						{prc->dscr|=ISRV_DELAYED; out.setEmpty(); prcst|=ISRV_NEEDMORE; if (cur<flushIdx) flushIdx=cur;}
				}
				if ((prc->dscr&ISRV_DELAYED)!=0 && (prc->oleft==0 || (prcst&ISRV_FLUSH)!=0))
					{prc->dscr&=~ISRV_DELAYED; out.bstr=prc->buf+prc->lHeader; out.length=(uint32_t)prc->lBuffer-prc->oleft;}
				if ((prcst&ISRV_NEEDMORE)!=0) {
					if (!out.isEmpty()) {rc=RC_INVPARAM; break;}
					if ((prcst&ISRV_APPEND)!=0) {
						if (!isString((ValueType)inp.type)) {rc=RC_INVPARAM; break;}
						// like ISRV_DELAYED + expand buffer
					}
					prc=&procs[cur=resume]; inp=prc->inp; sstate|=prc->dscr&ISRV_MOREOUT; prc->dscr&=~ISRV_MOREOUT;
					if (cst==CST_SERVER) sstate=(sstate&~ISRV_PROC_MASK)|(cur<=toggleIdx?ISRV_READ:ISRV_WRITE);
					else if (cur==0 && (cst==CST_WRITE || cst==CST_REQUEST && (sstate&ISRV_WRITE)!=0)) {
						if (ext!=NULL) {
							if (ext->type==VT_ARRAY && ++vidx<ext->length) inp=ext->varray[vidx];
							else if (ext->type==VT_COLLECTION) {
								const Value *cv=ext->nav->navigate(GO_NEXT);
								if (cv!=NULL) inp=*cv; else ext->nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID);
							}
						}
						if (!inp.isEmpty()) setHT(inp);
						else if (flushIdx!=~0u) prc=flush(inp);
						else if (cst!=CST_REQUEST) break;
						else {prc=&procs[cur=resume=toggleIdx]; prc->prevIdx=resume; sstate=(sstate&~ISRV_PROC_MASK)|ISRV_READ|ISRV_WAIT|ISRV_SWITCH;}
					}
				} else {
					while (cur+1<nProcs && (prc[1].dscr&ISRV_EOF)!=0) {cur++; prc++;}
					if (cur+1==nProcs && (cst==CST_READ || cst==CST_REQUEST)) {
						rc=RC_OK;
						if (cst==CST_READ && !action.isEmpty()) {
							PIN **en=NULL,**v=NULL,*pin=NULL; unsigned ne=0,nv=0; ValueV *par=NULL; unsigned nP=0;
							if (out.type==VT_REF) {pin=(PIN*)out.pin; en=v=&pin; ne=nv=1;} else {par->vals=&out; nP=1;}
							EvalCtx ectx(ses,en,ne,v,nv,par,nP,NULL,NULL,ECT_ACTION);
							rc=ses->getStore()->queryMgr->eval(&action,ectx); freeV(out); if (rc!=RC_OK) return rc;
							prc=&procs[cur=resume]; sstate|=prc->dscr&ISRV_MOREOUT; prc->dscr&=~ISRV_MOREOUT;
							inp=prc->inp; out.setEmpty(); continue;
						}
						if (pR!=NULL) {
							if (out.type!=VT_REF || out.pin!=pR) {
								Value *pv;
								switch (out.type) {
								case VT_ERROR: throw RC_EOF;
								case VT_STRUCT: pR->setProps(out.varray,out.length,0); break;
								case VT_REF:
									pR->copy((PIN*)out.pin,0);
									pR->mode=(pR->mode&~(PIN_NO_FREE|PIN_PARTIAL))|(((PIN*)out.pin)->mode&PIN_NO_FREE);
									((PIN*)out.pin)->mode|=PIN_NO_FREE; freeV(out); break;
								default:
									if ((pv=new(this) Value)==NULL) rc=RC_NORESOURCES;
									else {
										if ((out.flags&HEAP_TYPE_MASK)==SES_HEAP) *pv=out;
										else if ((rc=copyV(out,*pv,this))!=RC_OK) {freeV(out); free(pv);}
										else {pv->setPropID(PROP_SPEC_CONTENT); pR->setProps(pv,1,0);}
									}
									break;
								}

							}
						} else if (res!=NULL) *res=out;		// flags?
						break;
					}
					inp=out; ++cur; ++prc; assert(cur<nProcs);
					if (cst==CST_SERVER && cur>toggleIdx) sstate=(sstate&~ISRV_PROC_MASK)|ISRV_WRITE;
				}
			}
			out.setEmpty();
			if (cur!=0 && (prc->dscr&ISRV_ENVELOPE)!=0 && !inp.isEmpty()) {
				if (!isString((ValueType)inp.type)) {rc=RC_TYPE; break;}
				//check buffer address
				inp.bstr-=prc->lHeader; inp.length+=uint32_t(prc->lHeader+prc->lTrailer);
			}
		}
	} catch (RC rc2) {rc=rc2;} catch (...) {rc=RC_INTERNAL;}
	if (rc!=RC_OK) cleanup(false);
	pR=NULL; return rc;
}

ISession *ServiceCtx::getSession() const
{
	return ses;
}

IPIN& ServiceCtx::getCtxPIN()
{
	return *this;
}

URIID ServiceCtx::getServiceID() const
{
	return cur<nProcs?procs[cur].sid:STORE_INVALID_URIID;
}

const Value *ServiceCtx::getParameter(URIID uid) const
{
	const Value *pv=Value::find(uid,params,nParams);
	return pv==NULL&&cur<nProcs?Value::find(uid,procs[cur].srvInfo,procs[cur].nSrvInfo):pv;
}

void ServiceCtx::getParameters(Value *vals,unsigned nVals) const
{
	for (unsigned i=0; i<nVals; ++vals,++i) {
		const Value *pv=Value::find(vals->property,params,nParams);
		if (pv==NULL && (cur>=nProcs || (pv=Value::find(vals->property,procs[cur].srvInfo,procs[cur].nSrvInfo))==NULL)) vals->setError(vals->property); else {*vals=*pv; setHT(*vals);}
	}
}

RC ServiceCtx::expandBuffer(Value& v,size_t extra)
{
	if (cur>=nProcs || (sstate&ISRV_PROC_MASK)==0) return RC_INVPARAM;
	ProcDscr *prc=&procs[cur]; while (prc!=procs && (prc->dscr&ISRV_ENVELOPE)!=0) prc--;
	if ((prc->dscr&ISRV_ALLOCBUF)==0) return RC_INVPARAM;
	if (v.type!=VT_BSTR || v.bstr<prc->buf || v.bstr+v.length>prc->buf+prc->lbuf) return RC_INVPARAM;
	// expand
	return RC_OK;
}

uint64_t ServiceCtx::getMsgLength() const
{
	return msgLength;
}

void ServiceCtx::setMsgLength(uint64_t lMsg)
{
	if (lMsg!=0 && lMsg!=~size_t(0)) msgLength=lMsg;
}

PIN *ServiceCtx::createPIN(Value* values,unsigned nValues,unsigned md,const PID *orig)
{
	if ((md&MODE_SUBPIN)==0 && cur+1==nProcs && pR!=NULL && pR->properties==NULL) {
		uint32_t nv=(uint32_t)nValues; const Value *pv=values; StoreCtx *ctx=ses->getStore();
		if (ses->normalize(pv,nv,md,orig!=NULL?ctx->genPrefix(ushort(orig->pid>>48)):ctx->getPrefix(),this)==RC_OK) {pR->setProps(pv,nv,0); return pR;}
	}
	return NULL;
}

RC ServiceCtx::commitPINs()
{
	RC rc=RC_OK; uint32_t nPins=0; PIN **pp=pins.get(nPins);
	if (nPins!=0) {
		rc=ses->getStore()->queryMgr->persistPINs(EvalCtx(ses,ECT_INSERT),pp,nPins,0);		// mode? allocCtrl? params? (pass in stream, special message)
		if (cur+1<nProcs) {
			//	if (rc!=RC_OK) {Result res={rc,nPins,MODOP_INSERT}; rc=resultOut(res);}
			//	else for (uint32_t i=0; i<nPins; i++) if ((rc=pinOut(pins[i],pinoi[i]))!=RC_OK) break;
		}
		for (unsigned i=0; i<nPins; i++) if (pp[i]!=NULL) pp[i]->destroy();
		free(pp);
	}
	return rc;
}

RC ServiceCtx::server(ProcDscr *prc,Value& inp,Value& out)
{
	RC rc=RC_OK;
	if ((sstate&ISRV_MOREOUT)!=0) {
		if (crs==NULL) {
			// report committed pins
		} else if ((rc=crs->next(out))==RC_OK) {
			prc->dscr|=ISRV_MOREOUT; /*prc->inp=inp; inp.setEmpty();*/ if (resume!=cur) {prc->prevIdx=resume; resume=cur;}
		} else {
			crs->destroy(); crs=NULL; prc->dscr&=~ISRV_MOREOUT; if (resume==cur) resume=prc->prevIdx;	// RC_EOF???
		}
	} else if ((sstate&ISRV_FLUSH)!=0) {
		sstate&=~ISRV_FLUSH; rc=commitPINs();
		// start continue if there are PINs to pass back
	} else switch (inp.type) {
	case VT_REF:
		if (inp.op!=OP_ADD) {
			PID id=inp.pin->getPID();
			if (id.pid==STORE_INVALID_PID || id.ident==STORE_INVALID_IDENTITY) rc=RC_INVPARAM;
#if 0
			else {
				PINx pex(ses,is.pin->id); rc=ses->getStore()->queryMgr->apply(EvalCtx(ses),stmtOp[is.op],pex,is.pin->properties,is.pin->nProperties,MODE_CLASS);
			}
			if (out!=NULL) {
				RC rc2=rc==RC_OK?pinOut(is.pin,is.oi):RC_OK;
				Result res={rc,1,(MODOP)is.op}; rc=resultOut(res);
				if (rc2!=RC_OK) rc=rc2;
			}
#endif
		} else if ((rc=pins+=(PIN*)inp.pin)==RC_OK) {
			if (pins<xpins) {prc->dscr|=ISRV_FLUSH; if (cur<flushIdx) flushIdx=cur;}
			else {
				rc=commitPINs();
				// report one-by one or cleanup
			}
		}
		break;
	case VT_STMT:
		if (((Stmt*)inp.stmt)->op<=STMT_UNDELETE) {
			// query and data manipulation
			rc=((Stmt*)inp.stmt)->execute(cur+1<nProcs/*&&is.oi.rtt!=RTT_COUNT*/?(ICursor**)&crs:(ICursor**)0/*,is.stmt->params,is.stmt->nParams,is.stmt->limit,is.stmt->offset,is.stmt->mode,&res.cnt*/);
			if (rc==RC_OK && crs!=NULL) {
//				if (rtt==RTT_DEFAULT) {SelectType st=((Cursor*)ic)->selectType(); rtt=st==SEL_COUNT?RTT_COUNT:st==SEL_PINSET||st==SEL_AUGMENTED||st==SEL_COMPOUND?RTT_PINS:RTT_VALUES;}
				if ((rc=crs->next(out))!=RC_OK) {crs->destroy(); crs=NULL;}		// RC_EOF ???
				else {prc->dscr|=ISRV_MOREOUT; /*prc->inp=inp; inp.setEmpty();*/ if (resume!=cur) {prc->prevIdx=resume; resume=cur;}}
			}
		} else if (((Stmt*)inp.stmt)->op<=STMT_ROLLBACK) {
			// tx control (start, commit, rollback)
		} else return RC_INVOP;
		break;
	case VT_EXPR:
		//???
		break;
	default:
		//???
		break;
	}
	return rc;
}

RC ServiceCtx::getOSError() const
{
	try {
#ifdef WIN32
		return convCode(GetLastError());
#else
		return convCode(errno);
#endif
	} catch (...) {report(MSG_ERROR,"Exception in IServiceCtx::getOSError()\n"); return RC_INTERNAL;}
}

void ServiceCtx::destroy()
{
	if (list.isInList()) cleanup(false); else {Session *s=ses; this->~ServiceCtx(); s->free(this);}
}

void *ServiceCtx::malloc(size_t s)
{
	return ses->malloc(s);
}

void *ServiceCtx::realloc(void *p,size_t s,size_t old)
{
	return ses->realloc(p,s,old);
}

void ServiceCtx::free(void *p)
{
	ses->free(p);
}

void *ServiceCtx::memalign(size_t align,size_t s)
{
	return ses->memalign(align,s);
}

HEAP_TYPE ServiceCtx::getAType() const
{
	return SES_HEAP;
}

void ServiceCtx::release()
{
}

RC Session::createServiceCtx(const Value *vals,unsigned nVals,IServiceCtx *&sctx,bool fWrite,IListener *ls)
{
	try {
		ServiceCtx *srv=new(this) ServiceCtx(this,vals,nVals,fWrite,ls); if (srv==NULL) return RC_NORESOURCES;
		RC rc=srv->build(vals,nVals); if (rc!=RC_OK) {srv->destroy(); return rc;}
		if ((srv->sstate&ISRV_REVERSE)!=0) {
			srv->sstate&=~ISRV_REVERSE; assert(srv->nProcs>1);
			for (unsigned i=0; i<srv->nProcs/2; i++) {ServiceCtx::ProcDscr tmp=srv->procs[i]; srv->procs[i]=srv->procs[srv->nProcs-i-1]; srv->procs[srv->nProcs-i-1]=tmp;}
		}
		sctx=srv; return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {return RC_INTERNAL;}
}

RC Session::prepare(ServiceCtx *&srv,const PID& id,const Value *vals,unsigned nVals,unsigned flags)
{
	const bool fWrite=(flags&ISRV_WRITE)!=0; RC rc=RC_OK;
	srv=id.pid!=STORE_INVALID_PID && id.ident!=STORE_INVALID_IDENTITY && serviceTab!=NULL?serviceTab->find(id):NULL;
	if (srv!=NULL && (flags&ISRV_NOCACHE)!=0) {srv->remove(); serviceTab->remove(srv); srv->destroy(); nSrvCtx--; srv=NULL;}
	if (srv!=NULL && (srv->fWrite==fWrite || (srv=srv->list.getNext())!=NULL && srv->id==id && srv->fWrite==fWrite)) {
		srv->params=vals; srv->nParams=nVals; srv->resume=0; srv->pR=NULL; srv->remove(); srvCtx.insertFirst(srv);
	} else if ((rc=createServiceCtx(vals,nVals,*(IServiceCtx**)&srv,fWrite))==RC_OK) {
		srv->id=id;
		if (((srv->sstate|flags)&ISRV_NOCACHE)==0 && id.pid!=STORE_INVALID_PID && id.ident!=STORE_INVALID_IDENTITY) {
			if (serviceTab==NULL && (serviceTab=new(this) ServiceTab(20,this,false))==NULL) rc=RC_NORESOURCES;
			else {
				if (nSrvCtx>=xSrvCtx) {
					ServiceCtx *discard=(ServiceCtx*)srvCtx.prev; discard->remove(); assert(serviceTab!=NULL);
					serviceTab->remove(discard); discard->destroy(); nSrvCtx--;
				}
				serviceTab->insert(srv); srvCtx.insertFirst(srv); nSrvCtx++;
			}
			if (rc!=RC_OK) {srv->destroy(); srv=NULL;}
		}
	}
	return rc;
}

RC Session::listen(Value *vals,unsigned nVals,unsigned mode)
{
	const Value *pv=VBIN::find(PROP_SPEC_OBJID,vals,nVals),*lst=NULL; unsigned nv=nVals;
	URIID lid=pv!=NULL && pv->type==VT_URIID?pv->uid:STORE_INVALID_URIID,propID=PROP_SPEC_LISTEN;
	for (pv=vals;;propID=PROP_SPEC_SERVICE) {
		RC rc=RC_OK; URIID id=STORE_INVALID_URIID;
		if (lst==NULL && (lst=VBIN::find(propID,pv,nv))==NULL) return RC_INVPARAM; 
		switch (lst->type) {
		case VT_URIID: id=lst->uid; break;
		case VT_REF:
			pv=((PIN*)lst->pin)->properties; nv=((PIN*)lst->pin)->nProperties; lst=NULL; continue;
		case VT_STRUCT:
			pv=lst->varray; nv=lst->length; lst=NULL; continue;
		case VT_ARRAY:
			lst=lst->varray; continue;
		case VT_REFID:
//			{PINx ref(this,lst->id);
//			if ((rc=ref.load(LOAD_SSV))!=RC_OK || (rc=build(ref.properties,ref.nProperties,ls))!=RC_OK) return rc;}
			continue;
		}
		unsigned md=(mode&PIN_TRANSIENT)!=0?ISRV_TRANSIENT:0;
		ServiceProviderTab::Find find(*ctx->serviceProviderTab,id); rc=RC_NOTFOUND;
		for (ServiceProvider *ah=find.findLock(RW_S_LOCK);;ah=ah->stack) {
			IService *isrv=ah!=NULL?ah->handler:ctx->defaultService;
			if (isrv!=NULL) {
				for (ListenerNotificationHolder *lhn=ctx->lstNotif; lhn!=NULL; lhn=lhn->next)
					if ((rc=lhn->lnot->onListener(this,id,vals,nVals,pv!=vals?pv:NULL,nv))!=RC_OK) {
						report(MSG_ERROR,"onListener call failed (%d)\n",rc);
						isrv=NULL; break;
					}
				if (isrv!=NULL) {
					IListener *li=NULL;
					if ((rc=isrv->listen(this,vals,nVals,pv!=vals?pv:NULL,nv,md,li))==RC_OK) {
						if (lid!=STORE_INVALID_URIID && li!=NULL) {
							// store in global table
						}
					} else if (ah!=NULL) {rc=RC_NOTFOUND; continue;}
				}
			}
			find.unlock(); break;
		}
		if (rc!=RC_OK) freeV(vals,nVals,this);
		return rc;
	}
}

RC Session::resolve(IServiceCtx *sctx,URIID sid,AddrInfo& ai)
{
	ServiceProviderTab::Find find(*ctx->serviceProviderTab,sid); RC rc=RC_NOTFOUND;
	for (ServiceProvider *ah=find.findLock(RW_S_LOCK);;ah=ah->stack) {
		IService *isrv=ah!=NULL?ah->handler:ctx->defaultService;
		if (isrv!=NULL && (rc=isrv->resolve(this,((ServiceCtx*)sctx)->params,((ServiceCtx*)sctx)->nParams,ai))!=RC_OK && ah!=NULL) continue;
		find.unlock(); break;
	}
	return rc;
}

RC Session::stopListener(URIID lid,bool fSuspend)
{
	//???
	return RC_OK;
}

void Session::removeServiceCtx(const PID& id)
{
	assert(id.pid!=STORE_INVALID_PID && id.ident!=STORE_INVALID_IDENTITY);
	if (serviceTab!=NULL) {ServiceCtx *srv=serviceTab->find(id); if (srv!=NULL) {serviceTab->remove(srv); srv->destroy();}}
}

StartListener::StartListener(const Value *p,unsigned n,MemAlloc *ma) : props(NULL),nProps(n)
{
	RC rc; if (p!=NULL && (rc=copyV(p,n,props,ma))!=RC_OK) throw rc;
}

RC StartListener::process(Session *ses)
{
	RC rc=ses->listen(props,nProps,0); props=NULL; nProps=0; return rc;
}

void StartListener::destroy(Session *ses)
{
	if (props!=NULL) freeV(props,nProps,ses);
	ses->free(this);
}

LoadService::LoadService(const Value *p,unsigned n,MemAlloc *ma) : props(NULL),nProps(n)
{
	RC rc; if (p!=NULL && (rc=copyV(p,n,props,ma))!=RC_OK) throw rc;
}

RC LoadService::process(Session *ses)
{
	const Value *pv=Value::find(PROP_SPEC_LOAD,props,nProps); assert(pv!=NULL && pv->type==VT_STRING); RC rc;
	if ((rc=ses->getStore()->fileMgr->loadExt(pv->str,pv->length,ses,props,nProps,true))!=RC_OK) {
		if ((pv->meta&META_PROP_OPTIONAL)!=0) rc=RC_OK; else report(MSG_ERROR,"Cannot load %.*s (%d)\n",pv->length,pv->str,rc);
	}
	return rc;
}

void LoadService::destroy(Session *ses)
{
	if (props!=NULL) freeV(props,nProps,ses);
	ses->free(this);
}

RC StoreCtx::registerLangExtension(const char *langID,IStoreLang *ext,URIID *pID)
{
	try {
		if (pID!=NULL) *pID=~0u;
		if (langID==NULL||*langID=='\0'||ext==NULL) return RC_INVPARAM;
		Session *ses=Session::getSession(); bool fDestroy=false;
		if (ses==NULL) {
			ses=Session::createSession(this); if (ses==NULL) return RC_NOSESSION;
			ses->setIdentity(STORE_OWNER,true); fDestroy=true;
		}
		URI *uri=(URI*)uriMgr->insert(langID); RC rc=RC_NORESOURCES;
		if (uri!=NULL) {
			URIID uid=uri->getID(); uri->release(); if (pID!=NULL) *pID=uid;
			rc=registerLangExtension(uid,ext);
		}
		if (fDestroy) Session::terminateSession();
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in registerLangExtension()\n"); return RC_INTERNAL;}
}

RC StoreCtx::registerService(const char *sname,IService *handler,URIID *puid,IListenerNotification *lnot)
{
	try {
		if (sname==NULL||*sname=='\0'||handler==NULL) return RC_INVPARAM;
		Session *ses=Session::getSession(); bool fDestroy=false;
		if (ses==NULL) {
			ses=Session::createSession(this); if (ses==NULL) return RC_NOSESSION;
			ses->setIdentity(STORE_OWNER,true); fDestroy=true;
		}
		URI *uri=(URI*)uriMgr->insert(sname); RC rc=RC_NORESOURCES;
		if (uri!=NULL) {URIID uid=uri->getID(); uri->release(); if (puid!=NULL) *puid=uid; rc=registerService(uid,handler,lnot);}
		if (fDestroy) Session::terminateSession();
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in regsiterService()\n"); return RC_INTERNAL;}
}

RC StoreCtx::registerService(URIID id,IService *handler,IListenerNotification *lnot)
{
	try {
		if (id==~0u) return RC_INVPARAM; if (serviceProviderTab==NULL) return RC_INTERNAL;
		ServiceProviderTab::Find find(*serviceProviderTab,id);
		ServiceProvider *ah=find.findLock(RW_X_LOCK);
		if (ah!=NULL && ah->handler==handler) return RC_OK;
		if ((ah=new(mem) ServiceProvider(id,handler,ah))==NULL) return RC_NORESOURCES;
		serviceProviderTab->insertNoLock(ah,find.getIdx());
		if (lnot!=NULL) {
			ListenerNotificationHolder *lnh=new(this) ListenerNotificationHolder(lnot);
			if (lnh==NULL) return RC_NORESOURCES; lnh->next=lstNotif; lstNotif=lnh;
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in regsiterService()\n"); return RC_INTERNAL;}
}

RC StoreCtx::unregisterService(const char *sname,IService *handler)
{
	try {
		if (sname==NULL||*sname=='\0') return RC_INVPARAM;
		Session *ses=Session::getSession(); bool fDestroy=false;
		if (ses==NULL) {
			ses=Session::createSession(this); if (ses==NULL) return RC_NOSESSION;
			ses->setIdentity(STORE_OWNER,true); fDestroy=true;
		}
		URI *uri=(URI*)uriMgr->insert(sname); RC rc=RC_NORESOURCES;
		if (uri!=NULL) {
			URIID uid=uri->getID(); uri->release();
			rc=unregisterService(uid,handler);
		}
		if (fDestroy) Session::terminateSession();
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in unregsiterService()\n"); return RC_INTERNAL;}
}

RC StoreCtx::unregisterService(URIID id,IService *handler)
{
	try {
		if (id==~0u) return RC_INVPARAM; if (serviceProviderTab==NULL) return RC_INTERNAL;
		ServiceProviderTab::Find find(*serviceProviderTab,id);
		ServiceProvider *ah=find.findLock(RW_X_LOCK);
		if (ah!=NULL) {
			if ((handler==NULL || ah->handler==handler) && ah->stack==NULL) find.remove(ah);
			else {
				// ???
			}
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in unregsiterService()\n"); return RC_INTERNAL;}
}

RC StoreCtx::registerLangExtension(URIID uid,IStoreLang *ext)
{
	//...
	return RC_INTERNAL;
}

void StoreCtx::shutdownServices()
{
	if (serviceProviderTab!=NULL) for (ServiceProviderTab::it it(*serviceProviderTab); ++it;) {
		for (ServiceProvider *sp=it.get(); sp!=NULL; sp=sp->stack) if (sp->handler!=NULL) sp->handler->shutdown();
	}
}
