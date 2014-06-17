/**************************************************************************************

Copyright © 2004-2014 GoPivotal, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,  WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.

Written by Mark Venguerov 2004-2014

**************************************************************************************/

#include "stmt.h"
#include "cursor.h"
#include "parser.h"
#include "ftindex.h"
#include "queryprc.h"

using namespace AfyKernel;

Cursor::Cursor(Session *ses) : alloc(ses),ectx(ses,NULL,0,(PIN**)&pqr,1,vctx,QV_ALL,NULL,&alloc,ECT_QUERY),stmt(NULL),stype(SEL_CONST),op(STMT_QUERY),queryOp(NULL),
	nReturn(~0ULL),mode(0),results(NULL),nResults(0),qr(ses),pqr(&qr),txid(INVALID_TXID),txcid(NO_TXCID),cnt(0),tx(ses),fSnapshot(false),fProc(false),fAdvance(true)
{
	memset(vctx,0,sizeof(vctx));
}

Cursor::Cursor(const EvalCtx &ec,EvalCtxType ect) : alloc(ec.ses),ectx(ec.ses,ec.env,ec.nEnv,(PIN**)&pqr,1,vctx,QV_ALL,ec.stack,&alloc,ect,ec.modp),
	stmt(NULL),stype(SEL_CONST),op(STMT_QUERY),queryOp(NULL),nReturn(~0ULL),mode(0),results(NULL),nResults(1),qr(ec.ses),pqr(&qr),
	txid(INVALID_TXID),txcid(NO_TXCID),cnt(0),tx(ec.ses),fSnapshot(false),fProc(false),fAdvance(true)
{
	memset(vctx,0,sizeof(vctx));
	if (ec.nParams>QV_PARAMS) vctx[QV_PARAMS]=ec.params[QV_PARAMS];
	if (ec.nParams>QV_CTX) vctx[QV_CTX]=ec.params[QV_CTX];
}

Cursor::~Cursor()
{
	if (txcid!=NO_TXCID) ectx.ses->getStore()->txMgr->releaseSnapshot(txcid);
	if (results!=NULL) for (unsigned i=1; i<nResults; i++) if (results[i]!=NULL) results[i]->~PINx();
	if (queryOp!=NULL) queryOp->~QueryOp(); if (stmt!=NULL) stmt->destroy();
}

RC Cursor::connect()
{
	if (queryOp!=NULL && (nResults=queryOp->getNOuts())!=0) {
		if (nResults==1) queryOp->connect(&pqr);
		else if ((results=new(&alloc) PINx*[nResults])==NULL) return RC_NOMEM;
		else {
			memset(results,0,nResults*sizeof(PINx*)); results[0]=pqr;
			for (unsigned i=1; i<nResults; i++) if ((results[i]=new(&alloc) PINx(ectx.ses))==NULL) return RC_NOMEM;
			queryOp->connect(results,nResults); (PIN**&)ectx.vars=(PIN**)results; (unsigned&)ectx.nVars=nResults;
		}
	}
	return RC_OK;
}

uint64_t Cursor::getCount() const
{
	try {return cnt;} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ICursor::getCount()\n");}
	return ~0ULL;
}

RC Cursor::skip()
{
	RC rc=RC_OK;
	if (queryOp!=NULL) {
		unsigned ns=queryOp->getSkip(),c=0;
		while (c<ns && (rc=queryOp->next())==RC_OK)
			if ((rc=qr.checkLockAndACL(TVO_READ,queryOp))!=RC_NOACCESS && rc!=RC_NOTFOUND)		// results?? lock multiple
				if (rc==RC_OK) c++; else break;
		queryOp->setSkip(ns-c);
	}
	return rc;
}

RC Cursor::advance(bool fRet)
{
	if (!fAdvance) {fAdvance=true; return RC_OK;}
	RC rc=RC_EOF; assert(stype!=SEL_CONST||op==STMT_INSERT);
	if (ectx.ses!=NULL && cnt<nReturn && queryOp!=NULL) {
		if (ectx.ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		if (op!=STMT_QUERY && !tx.isStarted()) {if ((rc=tx.start())==RC_OK) tx.ok(); else return rc;}
		if (txid==INVALID_TXID) txid=ectx.ses->getTXID();
		else if (ectx.ses->getTXID()!=txid) {
			if ((mode&MODE_HOLD_RESULT)==0) return RC_CLOSED;
			// re-init & re-position
		}
		if (fSnapshot && txcid==NO_TXCID) if (ectx.ses->getTxState()==TX_NOTRAN) txcid=ectx.ses->getStore()->txMgr->assignSnapshot(); else fSnapshot=false;
		TxGuard txg(ectx.ses); ectx.ses->resetAbortQ();
		if (ectx.ses->getIdentity()==STORE_OWNER || queryOp->getSkip()==0 || (rc=skip())==RC_OK) {	//??????
			if (fProc && (stype==SEL_COUNT || stype==SEL_VALUE || stype==SEL_DERIVED || stype==SEL_FIRSTPID || stype==SEL_PIN)) rc=RC_EOF;
			else if (stype==SEL_COUNT) {rc=count(cnt,~0u); fProc=true;}
			else for (TVOp tvo=op==STMT_INSERT||op==STMT_QUERY?TVO_READ:TVO_UPD; (rc=queryOp->next())==RC_OK;) {
				if (op!=STMT_UNDELETE && ((rc=qr.checkLockAndACL(tvo,queryOp))==RC_NOACCESS || rc==RC_NOTFOUND)) rc=RC_OK;
				else if ((qr.epr.flags&PINEX_DERIVED)==0 || qr.properties!=NULL || (rc=queryOp->loadData(qr,NULL,0))==RC_OK) {
					PINx qpin(ectx.ses),*pq=&qr;
					if (!pq->pb.isNull()) {if (fRet && op!=STMT_QUERY) rc=pq->load(LOAD_CLIENT|LOAD_SSV);}
					else if ((stype==SEL_PINSET || stype==SEL_COMPOUND) && (qr.epr.flags&(PINEX_DERIVED|PINEX_COMM))==0) {
						qpin.id=pq->id; qpin.epr=pq->epr; qpin.addr=pq->addr; pq=&qpin;
						if (op!=STMT_QUERY) {
							if ((rc=qpin.getBody(tvo,(mode&MODE_DELETED)!=0?GB_DELETED:0))==RC_DELETED && op==STMT_DELETE) {rc=RC_OK; continue;}	// purge???
							if (rc==RC_OK && fRet && (rc=qpin.load(LOAD_CLIENT|LOAD_SSV))==RC_OK) {
								qr=qpin.id; qr=qpin.addr; qr.properties=qpin.properties; qr.nProperties=qpin.nProperties;
								qr.mode=qpin.mode; qr.meta=qpin.meta; qr.fPartial=qpin.fPartial; qpin.fNoFree=1;
							}
						}
					} else {
						// check property!=NULL || !PIN_PARTIAL
					}
					if (rc==RC_OK) {
						if (op!=STMT_QUERY) {
							if (op==STMT_INSERT && (stype==SEL_PINSET || stype==SEL_AUGMENTED) &&
								(pq->meta&PMT_COMM)!=0 && (queryOp->getQFlags()&QO_RAW)==0 && (rc=queryOp->createCommOp(pq))!=RC_OK) break;
							rc=ectx.ses->getStore()->queryMgr->apply(ectx,op,*pq,stmt->vals,stmt->nValues,mode|stmt->mode,fRet?(PIN*)&qr:(PIN*)0);
							if (rc==RC_NOACCESS || rc==RC_DELETED) {if (fRet) qr.cleanup(); rc=RC_OK; continue;}
						}
						if (rc==RC_OK) {fProc=true; cnt++;}
					}
					break;
				}
			}
		}
		if (rc!=RC_OK) {
			if (rc!=RC_EOF) tx.resetOk(); 
			if (txcid!=NO_TXCID) {ectx.ses->getStore()->txMgr->releaseSnapshot(txcid); txcid=NO_TXCID;}
		}
	}
	return rc;
}

RC Cursor::extract(MemAlloc *ma,PIN *&pin,unsigned idx,bool fCopy)
{
	if (idx>=nResults) return RC_INVPARAM;
	PINx *pex=results!=NULL?results[idx]:pqr; RC rc=RC_OK; assert(pex!=NULL);
	if (stype==SEL_PIN||stype==SEL_PINSET||stype==SEL_AUGMENTED||stype==SEL_COMPOUND) {
		if ((pex->id.ident!=STORE_INVALID_IDENTITY || pex->epr.buf[0]!=0) && (pex->epr.flags&(PINEX_DERIVED|PINEX_COMM))==0)
			{if ((rc=pex->load(LOAD_SSV|(fCopy?LOAD_CLIENT:0)))!=RC_OK) {pin=NULL; return rc;}}
		if (op<STMT_DELETE && (mode&MODE_RAW)==0 && idx==0 && (pex->meta&PMT_COMM)!=0 && queryOp!=NULL && (queryOp->getQFlags()&QO_RAW)==0) {
			if (op==STMT_UPDATE || op==STMT_INSERT && (pex->mode&PIN_TRANSIENT)!=0) return RC_EOF;
			if ((rc=queryOp->createCommOp(pex))!=RC_OK) return rc;
		}
	}
	if (!fCopy) pin=pex;
	else if ((pin=new(ma) PIN(ma,pqr->mode))==NULL) return RC_NOMEM;
	else {
		*pin=pqr->id; *pin=pqr->addr; pin->meta=pex->meta;
		if (pex->properties!=NULL && pex->nProperties!=0) {
			if (pex->fNoFree==0) {pin->properties=(Value*)pex->properties; pin->nProperties=pex->nProperties; pex->properties=NULL; pex->nProperties=0;}
			else if ((rc=copyV(pex->properties,pin->nProperties=pex->nProperties,pin->properties,ma))!=RC_OK) return rc;
		}
	}
	if ((pin->meta&PMT_DATAEVENT)!=0 && (mode&MODE_DEVENT)!=0 && pin->id.ident!=STORE_INVALID_IDENTITY) ectx.ses->getStore()->queryMgr->getDataEventInfo(ectx.ses,pin);
	return RC_OK;
}

RC Cursor::next(PINx *&ret,bool fExtract)
{
	ret=pqr; RC rc=advance(true); if (rc!=RC_OK||!fExtract) return rc;
	unsigned i; PIN *pin;
	switch (stype) {
	default: break;
	case SEL_CONST:
		//???
		break;
	case SEL_COUNT:
		if (qr.properties==NULL && (qr.properties=new(ectx.ses) Value)==NULL) return RC_NOMEM;
		qr.nProperties=1; qr.properties->setU64(cnt); qr.properties->setPropID(PROP_SPEC_VALUE);
		break;
	case SEL_PINSET: case SEL_PIN:
		while ((rc=extract(ectx.ma,pin,0))!=RC_OK) if (rc!=RC_EOF || (rc=advance(true))!=RC_OK) return rc;
		break;
	case SEL_COMPOUND:
		for (i=0; i<nResults; i++) if ((rc=extract(ectx.ma,pin,i))!=RC_OK) return rc;
		break;
	}
	return RC_OK;
}

RC Cursor::next(Value& ret)
{
	try {
		ret.setEmpty(); RC rc=RC_OK;
		if (stype==SEL_CONST) {
			if (fProc) rc=RC_EOF; else {ret=retres; setHT(ret); fProc=true;}
		} else if ((rc=advance(true))==RC_OK) {
			PIN *pin; Value *pv; PID pid;
			switch (stype) {
			default: break;
			case SEL_COUNT: ret.setU64(cnt); ret.setPropID(PROP_SPEC_VALUE); break;
			case SEL_VALUE: case SEL_VALUESET: case SEL_DERIVED: case SEL_DERIVEDSET: case SEL_AUGMENTED:
				if (op==STMT_QUERY || op==STMT_INSERT && stmt!=NULL && stmt->top!=NULL) {
					freeV(retres);
					if (pqr->properties!=NULL) {
						if (pqr->nProperties>1) {
							retres.setStruct(pqr->properties,pqr->nProperties); setHT(retres,SES_HEAP);
							if (pqr->fPartial!=0) rc=copyV(retres,retres,ectx.ses); else {pqr->properties=NULL; pqr->nProperties=0;}
							ret=retres; setHT(ret);
						} else if (pqr->fNoFree!=0) rc=copyV(pqr->properties[0],ret,ectx.ses); else {ret=pqr->properties[0]; pqr->properties[0].setError();}
					}
				}
				break;
			case SEL_DERIVEDPINSET:
				ret.set((IPIN*)pqr); break;
			case SEL_PID: case SEL_FIRSTPID:
				if ((rc=qr.getID(pid))==RC_OK) {ret.set(pid); ret.setPropID(PROP_SPEC_PINID);}
				break;
			case SEL_PINSET: case SEL_PIN:
				while ((rc=extract(ectx.ses,pin,0,true))!=RC_OK) if (rc!=RC_EOF || (rc=advance(true))!=RC_OK) break;
				if (rc==RC_OK) {ret.set(pin); setHT(ret,SES_HEAP);}
				break;
			case SEL_COMPOUND: case SEL_COMP_DERIVED:
				if (retres.isEmpty()) {
					if ((pv=new(ectx.ses) Value[nResults])==NULL) return RC_NOMEM;
					memset(pv,0,nResults*sizeof(Value)); retres.set(pv,nResults); setHT(retres,SES_HEAP);
				}
				for (unsigned i=0; i<nResults; i++) {
					if ((rc=extract(ectx.ses,pin,i,true))!=RC_OK) break;
					freeV(*(Value*)&retres.varray[i]); ((Value*)&retres.varray[i])->set(pin);
				}
				if (rc==RC_OK) {ret=retres; setHT(ret);}
				break;
			}
		}
		ectx.ses->releaseAllLatches(); return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ICursor::next(Value&)\n"); return RC_INTERNAL;}
}

RC Cursor::next(PID& pid)
{
	try {
		if (stype<SEL_AUGMENTED || stype==SEL_COMP_DERIVED) return RC_INVOP;
		RC rc=advance(false);  if (rc==RC_OK) rc=qr.getID(pid); else pid=PIN::noPID;
		ectx.ses->releaseAllLatches(); return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ICursor::next(PID&)\n"); return RC_INTERNAL;}
}

IPIN *Cursor::next()
{
	try {
		if (stype<SEL_DERIVED) return NULL; PIN *pin=NULL;
		if (advance(true)==RC_OK) {
			extract(ectx.ses,pin,0,true);	// check RC_EOF
		}
		ectx.ses->releaseAllLatches(); return pin;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ICursor::next()\n");}
	return NULL;
}

RC Cursor::rewindInt()
{
	RC rc=RC_EOF;
	if (ectx.ses!=NULL && queryOp!=NULL) {
		if (!fProc || op!=STMT_QUERY && !tx.isStarted()) return RC_OK;
		if ((rc=queryOp->rewind())==RC_OK) {cnt=0; fProc=false;}
	}
	return rc;
	
}

RC Cursor::rewind()
{
	try {
		RC rc=RC_EOF;
		if (ectx.ses!=NULL && queryOp!=NULL) {
			if (ectx.ses->getStore()->inShutdown()) return RC_SHUTDOWN;
			if (!fProc || op!=STMT_QUERY && !tx.isStarted()) return RC_OK;
			if ((rc=rewindInt())!=RC_OK) {
				if (rc!=RC_EOF) tx.resetOk(); 
				if (txcid!=NO_TXCID) {ectx.ses->getStore()->txMgr->releaseSnapshot(txcid); txcid=NO_TXCID;}
			}
			ectx.ses->releaseAllLatches();
		}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ICursor::rwind()\n"); return RC_INTERNAL;}
}

RC Cursor::count(uint64_t& cnt,unsigned nAbort,const OrderSegQ *os,unsigned nos)
{
	RC rc=RC_OK; cnt=0ULL;
	if (ectx.ses->getIdentity()==STORE_OWNER) rc=queryOp->count(cnt,nAbort);
	else {
		// nulls in sort?
		while ((rc=queryOp->next())==RC_OK)
			if ((rc=qr.checkLockAndACL(TVO_READ,queryOp))!=RC_NOACCESS && rc!=RC_NOTFOUND) {
				if (rc!=RC_OK) break;
				cnt++;
			}
		if (rc==RC_EOF) rc=RC_OK;
	}
	return rc;
}

void Cursor::destroy()
{
	try {MemAlloc *ma=ectx.ma; this->~Cursor(); ma->free(this);} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ICursor::destroy()\n");}
}

//--------------------------------------------------------------------------------------------------------------------------------

RC CursorNav::create(Cursor *cr,Value &res,unsigned md,MemAlloc *ma)
{
	RC rc=getNext(cr,res,ma,(md&MODE_PID)!=0); Value res2;
	if (rc==RC_OK) {
		if (res.type==VT_REF && res.pin==(IPIN*)&cr->qr) {
			PIN *pin=NULL; if ((rc=cr->extract(ma,pin,0,true))!=RC_OK) return rc;
			res.set(pin); setHT(res,ma->getAType());
		}
		if ((rc=getNext(cr,res2,ma,(md&MODE_PID)!=0))==RC_OK) {
			CursorNav *cn=new(ma) CursorNav(ma,cr,(md&MODE_PID)!=0);
			if (cn==NULL) {freeV(res); freeV(res2); rc=RC_NOMEM;}
			else {cn->vals+=res; cn->vals+=res2; cn->cv=&cn->vals[0]; res.set(cn); setHT(res,ma->getAType());}
		} else if (rc==RC_EOF) rc=RC_FALSE; else freeV(res);
	}
	if (rc!=RC_OK) cr->destroy();
	return rc;
}

CursorNav::~CursorNav()
{
	for (unsigned i=0; i<vals; i++) freeV(vals[i]);
	if (curs!=NULL) curs->destroy();
}

RC CursorNav::getNext(Cursor *curs,Value& v,MemAlloc *ma,bool fPID)
{
	for (RC rc;;) {
		if ((rc=curs->advance(curs->stype!=SEL_PINSET))==RC_OK) switch (curs->stype) {
		case SEL_COUNT: case SEL_VALUE: case SEL_DERIVED: case SEL_PIN: case SEL_FIRSTPID: assert(0);
		case SEL_VALUESET:
			if (curs->qr.properties==NULL || curs->qr.nProperties!=1) continue;
			if (curs->qr.fNoFree==0) {v=curs->qr.properties[0]; static_cast<Value&>(curs->qr.properties[0]).setEmpty();}
			else if (copyV(curs->qr.properties[0],v,ma)==RC_OK) setHT(v,ma->getAType());
			break;
		case SEL_PINSET: case SEL_AUGMENTED:
			if (fPID) {PID id; if (curs->qr.getID(id)!=RC_OK) continue; v.set(id); break;}
			if (curs->qr.load(LOAD_SSV)!=RC_OK) continue;	//???
			v.set((IPIN*)&curs->qr); break;
		default:
			if (curs->qr.properties==NULL||curs->qr.nProperties==0) continue;
			v.setStruct(curs->qr.properties,curs->qr.nProperties); 
			if (curs->qr.fNoFree==0) {setHT(v,curs->qr.ma->getAType()); curs->qr.setProps(NULL,0);}
			else if (copyV(v,v,ma)==RC_OK) setHT(v,ma->getAType());
			break;
		}
		return rc;
	}
}

const Value *CursorNav::navigate(GO_DIR dir,ElementID ei)
{
	for (;;) switch (dir) {
	default: return NULL;
	case GO_FIRST: cv=&vals[0]; fColl=false;
	case GO_NEXT:
		if (cv==NULL) return NULL;
		if (fColl) {
			assert(cv!=NULL && cv->type==VT_COLLECTION);
			if (cv->isNav()) {const Value *pv=cv->nav->navigate(GO_NEXT); if (pv!=NULL) return pv;}
			else if (idx<cv->length) return &cv->varray[idx++];
			cv++; fColl=false;
		}
		if (cv>&vals[vals-1]) {
			if (curs==NULL) return cv=NULL; Value v;
			if (fPID) {v.setEmpty(); cv=&v;} else {freeV(vals[vals-1]); cv=&vals[vals-1];}
			if (getNext(curs,*(Value*)cv,ma,fPID)!=RC_OK) {curs->destroy(); curs=NULL; return cv=NULL;}
			// copy v?
			if (cv==&v) {if ((vals+=v)==RC_OK) cv=&vals[vals-1]; else {freeV(v); return cv=NULL;}}
		}
		const Value *pv=cv;
		if (pv->type!=VT_COLLECTION) cv++;
		else {
			if (!pv->isNav()) {if (pv->varray==NULL||pv->length==0) continue; pv=&pv->varray[0]; idx=1;}
			else if ((pv=pv->nav->navigate(GO_FIRST))==NULL) continue;
			fColl=true;
		}
		return pv;
	}
}

ElementID CursorNav::getCurrentID()
{
	return STORE_COLLECTION_ID;
}

const Value *CursorNav::getCurrentValue()
{
	return NULL;
}

RC CursorNav::getElementByID(ElementID,Value&)
{
	return RC_INVOP;
}

INav *CursorNav::clone() const
{
	//???
	return NULL;
}

unsigned CursorNav::count() const
{
	if (curs!=NULL) switch (curs->stype) {
	case SEL_COUNT: case SEL_DERIVED: case SEL_FIRSTPID: case SEL_VALUE: case SEL_PIN: return 1;
	default:
#if 0
		while (curs->advance(curs->stype!=SEL_PINSET)==RC_OK) {
			if (curs->stype!=SEL_VALUE && curs->stype!=SEL_VALUESET) cnt++;
			else if (curs->qr.properties!=NULL && curs->qr.nProperties==1) cnt+=curs->qr.properties[0].count();
		}
#endif
		return ~0u;
	} else return vals;
}

void CursorNav::destroy()
{
	this->~CursorNav(); ma->free(this);
}

//--------------------------------------------------------------------------------------------

RC Cursor::init(DataEventID dev,unsigned md)
{
	queryOp=dev==STORE_INVALID_URIID?(QueryOp*)new(&alloc) FullScan(&ectx,md):(QueryOp*)new(&alloc) ClassScan(&ectx,dev,md);
	stype=SEL_PINSET; return queryOp!=NULL?connect():RC_NOMEM;
}

RC Cursor::init(Value& v,uint64_t nRet,unsigned nSkip,unsigned md,bool fSS)
{
	if (v.isEmpty()) return RC_EOF; nReturn=nRet; mode=md; fSnapshot=fSS;
	if ((queryOp=new(&alloc) ExprScan(&ectx,v,QO_RAW))==NULL) return RC_NOMEM;
	v.setEmpty(); stype=SEL_PINSET; if (nSkip!=0) queryOp->setSkip(nSkip); return connect();
}

BuildCtx::BuildCtx(Cursor& c) : cu(c),flg(0),propsReq(&c.alloc),sortReq(NULL),nSortReq(0),nqs(0),ncqs(0) 
{
	if ((c.mode&MODE_FOR_UPDATE)!=0) flg|=QO_FORUPDATE;
	if ((c.mode&MODE_DELETED)!=0) flg|=QO_DELETED;
	if ((c.mode&MODE_DEVENT)!=0) flg|=QO_CLASS|QO_RAW;
}

RC Cursor::init(Stmt *st,uint64_t nRet,unsigned nSkip,unsigned md,bool fSS)
{
	if (st==NULL || st->top==NULL) return RC_EOF;
	stmt=st; st->ref(); op=st->op; nReturn=nRet; mode=md; fSnapshot=fSS;

	BuildCtx bctx(*this); bctx.sortReq=stmt->orderBy; bctx.nSortReq=stmt->nOrderBy; 
	
	QVar *qv=stmt->top; stype=qv->stype;
	if (stype==SEL_CONST) {
		if (op!=STMT_INSERT) return RC_INTERNAL;
		stype=qv->outs->nValues>1?SEL_DERIVEDSET:SEL_VALUESET;
	}

	RC rc=qv->build(bctx,queryOp); if (rc!=RC_OK) return rc; assert(queryOp!=NULL);

	if (qv->groupBy==NULL || qv->nGroupBy==0) switch (qv->qvf&(QVF_ALL|QVF_DISTINCT)) {
	default:
	case QVF_DISTINCT:
		if ((queryOp->qflags&QO_UNIQUE)==0 && (qv->stype==SEL_PINSET||qv->stype==SEL_COMPOUND||qv->stype==SEL_AUGMENTED) &&
			(queryOp=new(&alloc,0,bctx.propsReq.nPls) Sort(queryOp,NULL,0,bctx.flg|QO_UNIQUE,0,NULL,0/*bctx.propsReq,bctx.nPropsReq*/))==NULL) return RC_NOMEM;
		break;
#if 0
	case DT_DISTINCT_VALUES:
		if (bctx.propsReq.nPls!=0 || (queryOp->qflags&QO_UNIQUE)==0) {
			// calc total number -> nProps
			OrderSegQ *os=(OrderSegQ*)alloca(nPropsReq*sizeof(OrderSegQ)); if (os==NULL) return RC_NOMEM;
			for (unsigned i=0; i<nPropsReq; i++) {os[i].expr=NULL; os[i].flags=0; os[i].aggop=OP_ALL; os[i].lPref=0; os[i].pid=propsReq[i];}
			if ((queryOp=new(&alloc,0,nPropsReq) Sort(queryOp,os,bctx.nPropsReq,flg|QO_VUNIQUE,0,NULL,0))==NULL) return RC_NOMEM;
		}
		break;
#endif
	case QVF_ALL:
		queryOp->unique(false); break;
	}
	if (qv->stype!=SEL_VALUE && qv->stype!=SEL_DERIVED) {
		if (stmt->orderBy!=NULL) rc=bctx.sort(queryOp,stmt->orderBy,stmt->nOrderBy,NULL,false);
		/*else if ((queryOp->qflags&QO_DEGREE)!=0) {
			OrderSegQ ks; ks.pid=PROP_SPEC_ANY; ks.flags=0; ks.var=0; ks.aggop=OP_SET; ks.lPref=0;
			queryOp=new(ses,1,0) Sort(queryOp,&ks,1,flg,1,NULL,0);
		}*/
	}
	if (queryOp==NULL) rc=RC_NOMEM;
	else if (rc==RC_OK) {
		if (nSkip!=0) queryOp->setSkip(nSkip);
		if (st->with.vals!=NULL && st->with.nValues!=0) {
			if ((st->mode&QRY_CALCWITH)==0) vctx[QV_PARAMS]=st->with;
			else {
				Value *pv=new(&alloc) Value[st->with.nValues]; if (pv==NULL) return RC_NOMEM;
				memcpy(pv,st->with.vals,st->with.nValues*sizeof(Value));
				for (unsigned i=0; rc==RC_OK && i<st->with.nValues; i++) {
					setHT(pv[i]);
					switch (pv->type) {
					default: break;
					case VT_EXPR: case VT_STMT: case VT_COLLECTION: case VT_STRUCT: case VT_REF: case VT_REFIDPROP: case VT_REFIDELT:
						if (pv[i].fcalc==0) break;
					case VT_VARREF: case VT_EXPRTREE: case VT_CURRENT:
						rc=ectx.ses->getStore()->queryMgr->eval(&pv[i],ectx,&pv[i]); break;
					}
				}
				if (rc==RC_OK) vctx[QV_PARAMS]=Values(pv,st->with.nValues);
			}
		}
		if ((ectx.ses->getTraceMode()&TRACE_EXEC_PLAN)!=0)
			{SOutCtx buf(ectx.ses); queryOp->print(buf,0); size_t l; byte *p=buf.result(l); ectx.ses->trace(1,"%.*s\n",l,p); ectx.ses->free(p);}
		if (rc==RC_OK) rc=connect();
	}
	return rc;
}

RC SetOpVar::build(BuildCtx& qctx,QueryOp *&q) const
{
	q=NULL;	assert(type==QRY_UNION||type==QRY_EXCEPT||type==QRY_INTERSECT);
	if (type==QRY_EXCEPT && nVars!=2) return RC_INTERNAL;
	RC rc=RC_OK; QueryOp *qq; const unsigned nqs0=qctx.nqs;
	const OrderSegQ *const os=qctx.sortReq; const unsigned nos=qctx.nSortReq;
	OrderSegQ ids; ids.pid=PROP_SPEC_PINID; ids.flags=0; ids.var=0; ids.aggop=OP_SET; ids.lPref=0;
	qctx.sortReq=&ids; qctx.nSortReq=1;
	for (unsigned i=0; i<nVars; i++) {
		if (qctx.nqs>=sizeof(qctx.src)/sizeof(qctx.src[0])) {rc=RC_NOMEM; break;}
		if ((rc=vars[i].var->build(qctx,qq))==RC_OK) qctx.src[qctx.nqs++]=qq; else break;
		if ((rc=qctx.sort(qq,NULL,0))!=RC_OK) break;
	}
	qctx.sortReq=os; qctx.nSortReq=nos;
	if (rc==RC_OK && (rc=qctx.mergeN(q,&qctx.src[nqs0],nVars,(QUERY_SETOP)type))==RC_OK) qctx.nqs-=nVars;
	return rc;
}

RC JoinVar::build(BuildCtx& qctx,QueryOp *&q) const
{
	q=NULL;	RC rc=RC_OK; const unsigned nqs0=qctx.nqs;
	assert(type==QRY_SEMIJOIN||type==QRY_JOIN||type==QRY_LEFT_OUTER_JOIN||type==QRY_RIGHT_OUTER_JOIN||type==QRY_FULL_OUTER_JOIN);
	const bool fTrans=groupBy!=NULL && nGroupBy!=0 || outs!=NULL && nOuts!=0;
	if (fTrans) {
		// merge props
	}
	if (condEJ==NULL) {
		if ((rc=vars[0].var->build(qctx,qctx.src[qctx.nqs]))==RC_OK) qctx.nqs++; else return rc;
		if ((rc=vars[1].var->build(qctx,qctx.src[qctx.nqs]))==RC_OK) qctx.nqs++; else return rc;
		rc=qctx.nested(q,&qctx.src[nqs0],cond);
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
			rc=qctx.merge2(q,&qctx.src[nqs0],ce,(QUERY_SETOP)type,cond);
			break;
		}
		if (ce->propID1==PROP_SPEC_PINID && ce->propID2==PROP_SPEC_PINID) {
			if ((rc=vars[0].var->build(qctx,qctx.src[qctx.nqs]))==RC_OK) qctx.nqs++; else return rc;
			if ((rc=vars[1].var->build(qctx,qctx.src[qctx.nqs]))==RC_OK) qctx.nqs++; else return rc;
#if 0
			if (vars[0].var->getType()==QRY_COLLECTION /*&& (qctx.req&QRQ_SORT)==0*/) {
				if ((q=new(qctx.ses) HashOp(qctx.ses,qctx.src[nqs0],qctx.src[nqs0+1]))==NULL) rc=RC_NOMEM;
			}
#endif
			if (qctx.sortReq!=NULL && qctx.nSortReq!=0) {
				unsigned nP=0;
				if (BuildCtx::checkSort(qctx.src[nqs0],qctx.sortReq,qctx.nSortReq,nP)) {
					if ((q=new(&qctx.cu.alloc) HashOp(qctx.src[nqs0],qctx.src[nqs0+1]))==NULL) rc=RC_NOMEM;
					break;
				}
				if (BuildCtx::checkSort(qctx.src[nqs0+1],qctx.sortReq,qctx.nSortReq,nP)) {
					if ((q=new(&qctx.cu.alloc) HashOp(qctx.src[nqs0+1],qctx.src[nqs0]))==NULL) rc=RC_NOMEM;
					break;
				}
			}
			rc=qctx.mergeN(q,&qctx.src[nqs0],nVars,QRY_INTERSECT); break;
		}
	}
	if (rc==RC_OK) {qctx.nqs-=nVars; if (fTrans) rc=qctx.out(q,this);}
	return rc;
}

RC SimpleVar::build(BuildCtx& qctx,QueryOp *&q) const
{
	q=NULL;	assert(type==QRY_SIMPLE && (stype!=SEL_CONST||qctx.cu.op==STMT_INSERT));
	RC rc=RC_OK; QueryOp *qq,*primary=NULL; const unsigned nqs0=qctx.nqs,ncqs0=qctx.ncqs;
	const bool fTrans=groupBy!=NULL && nGroupBy!=0 || outs!=NULL && nOuts!=0;
	
	if (fTrans) {
		// merge props
	}

	unsigned sflg=qctx.flg; if ((qvf&QVF_RAW)!=0) qctx.flg|=QO_RAW; if ((qvf&QVF_FIRST)!=0) qctx.flg|=QO_FIRST;
	if (expr.type==VT_STMT) {
		if (((Stmt*)expr.stmt)->op!=STMT_QUERY || ((Stmt*)expr.stmt)->top==NULL) return RC_INVPARAM;
//?????????????????????????????????
		Stmt *saveQ=qctx.cu.stmt; qctx.cu.stmt=(Stmt*)expr.stmt;
// merge propsReq with getProps
		rc=((Stmt*)expr.stmt)->top->build(qctx,q); qctx.cu.stmt=saveQ;
		if (rc==RC_OK && (cond!=NULL||props!=NULL&&nProps!=0)) rc=qctx.filter(q,cond,props,nProps);
		qctx.flg=sflg; return rc;
	}
	for (unsigned i=0; rc==RC_OK && i<nSrcs; i++) {
		const SourceSpec &cs=srcs[i]; DataEventID cid=cs.objectID;
		if (cid<=MAX_BUILTIN_CLASSID) {
			if (qctx.nqs>=sizeof(qctx.src)/sizeof(qctx.src[0])) rc=RC_NOMEM;
			else {
				qq=cid==CLASS_OF_STORES || cid==CLASS_OF_SERVICES ? (QueryOp*)new(&qctx.cu.alloc) SpecClassScan(&qctx.cu.ectx,cid,qctx.flg) 
																	: (QueryOp*)new(&qctx.cu.alloc) ClassScan(&qctx.cu.ectx,cid,qctx.flg);
				if ((qctx.src[qctx.nqs]=qq)!=NULL) qctx.nqs++; else rc=RC_NOMEM;
			}
			continue;
		}
		DataEvent *dev=qctx.cu.ectx.ses->getStore()->classMgr->getDataEvent(cid); if (dev==NULL) {rc=RC_NOTFOUND; break;}
		const Stmt *cqry=dev->getQuery(); DataIndex *cidx=dev->getIndex(); const unsigned cflg=dev->getFlags(); IndexScan *is=NULL;
		if (qctx.nqs>=sizeof(qctx.src)/sizeof(qctx.src[0])) rc=RC_NOMEM;
		else if ((cflg&META_PROP_INDEXED)==0 || (qctx.cu.mode&MODE_DELETED)!=0) {
			if (qctx.ncqs>=sizeof(qctx.condQs)/sizeof(qctx.condQs[0])) rc=RC_NOMEM;
			else if (cqry!=NULL) {
				QueryWithParams &qs=qctx.condQs[qctx.ncqs++]; qs.params=NULL; qs.nParams=0;
				if ((qs.qry=cqry->clone(STMT_QUERY,&qctx.cu.alloc))==NULL) rc=RC_NOMEM; else {qs.params=(Value*)cs.params; qs.nParams=cs.nParams;}
			}
		} else if (cidx==NULL) {
			if ((qctx.src[qctx.nqs++]=new(&qctx.cu.alloc) ClassScan(&qctx.cu.ectx,cid,qctx.flg))==NULL) rc=RC_NOMEM;
			else if (cqry!=NULL && cqry->hasParams() && cs.params!=NULL && cs.nParams!=0) {
				if (qctx.ncqs>=sizeof(qctx.condQs)/sizeof(qctx.condQs[0])) rc=RC_NOMEM;
				else {
					QueryWithParams &qs=qctx.condQs[qctx.ncqs++]; qs.params=NULL; qs.nParams=0;
					if ((qs.qry=cqry->clone(STMT_QUERY,&qctx.cu.alloc))==NULL) rc=RC_NOMEM; else {qs.params=(Value*)cs.params; qs.nParams=cs.nParams;}
				}
			}
		} else {
			assert(cqry!=NULL && cqry->top!=NULL && cqry->top->getType()==QRY_SIMPLE && ((SimpleVar*)cqry->top)->condIdx!=NULL);
			ushort flags=SCAN_EXACT; unsigned i=0,nRanges=0; const Value *param; const unsigned nSegs=((SimpleVar*)cqry->top)->nCondIdx;
			struct IdxParam {const Value *param; uint32_t idx;} *iparams=(IdxParam*)alloca(nSegs*sizeof(IdxParam));
			const Value **curValues=(const Value**)alloca(nSegs*sizeof(Value*)),*cv;
			if (iparams==NULL || curValues==NULL) rc=RC_NOMEM;
			else {
				if (cs.nParams==0) flags&=~SCAN_EXACT;
				else for (CondIdx *pci=((SimpleVar*)cqry->top)->condIdx; pci!=NULL; pci=pci->next,++i) {
					if (pci->param>=cs.nParams) {iparams[i].param=NULL; flags&=~SCAN_EXACT;}
					else if ((param=&cs.params[pci->param])->type==VT_RANGE && param->varray[0].type==VT_ANY && param->varray[1].type==VT_ANY || param->type==VT_ANY && param->fcalc!=0) {
						iparams[i].param=param; iparams[i].idx=0; flags&=~SCAN_EXACT;
					} else {
						if (param->type==VT_VARREF && (param->refV.flags&VAR_TYPE_MASK)==VAR_PARAM) {
							if (param->length!=0 || param->refV.refN>=qctx.cu.vctx[QV_PARAMS].nValues) {rc=RC_INVPARAM; break;}
							param=&qctx.cu.vctx[QV_PARAMS].vals[param->refV.refN];
						}
						iparams[i].param=param; iparams[i].idx=0; unsigned nVals=param->count();
						if (nVals>1) {
							if (pci->ks.op!=OP_EQ && pci->ks.op!=OP_IN && pci->ks.op!=OP_BEGINS) {rc=RC_TYPE; break;}
							flags&=~SCAN_EXACT;
						}
						if (nVals>nRanges) nRanges=nVals;
					}
				}
				if (rc==RC_OK && (is=new(&qctx.cu.alloc,nRanges,*cidx) IndexScan(&qctx.cu.ectx,*cidx,flags,nRanges,qctx.flg))==NULL) rc=RC_NOMEM;
				for (unsigned i=0; rc==RC_OK && i<nRanges; i++) {
					bool fRange=false; byte op;
					for (unsigned k=0; k<nSegs; k++) if ((cv=curValues[k]=iparams[k].param)!=NULL) {
						if (cv->type==VT_COLLECTION) {
							if (!cv->isNav()) {
								if (iparams[k].idx<cv->length) cv=&cv->varray[iparams[k].idx++]; else cv=iparams[k].param=NULL;
							} else {
								if ((cv=cv->nav->navigate(i==0?GO_FIRST:GO_NEXT))==NULL) iparams[k].param=NULL;
							}
						}
						if ((curValues[k]=cv)==NULL) is->flags&=~SCAN_EXACT;
						else switch (cv->type) {
						case VT_RANGE:
							if (cv->varray[0].type==VT_ANY && cv->varray[1].type==VT_ANY) {fRange=true; break;}
							if (cidx->getIndexSegs()[k].op==OP_IN) {fRange=true; is->flags&=~SCAN_EXACT;} else rc=RC_TYPE;
							break;
						case VT_EXPR: case VT_STMT: case VT_EXPRTREE: case VT_VARREF:
							rc=RC_TYPE; break;
						case VT_ANY: if (cv->fcalc!=0) {fRange=true; break;}
						default:
							op=cidx->getIndexSegs()[k].op;
							if (op!=OP_EQ && op!=OP_IN) {is->flags&=~SCAN_EXACT; if (op!=OP_BEGINS) fRange=true;}
							break;
						}
					}
					if (rc!=RC_OK || (rc=((SearchKey*)(is+1))[i*2].toKey(curValues,nSegs,cidx->getIndexSegs(),0,qctx.cu.ectx.ses,&qctx.cu.alloc))!=RC_OK) break;
					if (!fRange) ((SearchKey*)(is+1))[i*2+1].copy(((SearchKey*)(is+1))[i*2]);
					else if ((rc=((SearchKey*)(is+1))[i*2+1].toKey(curValues,nSegs,cidx->getIndexSegs(),1,qctx.cu.ectx.ses,&qctx.cu.alloc))!=RC_OK) break;
				}
				if (rc==RC_OK) {
					QVar *cqv=cqry->top; is->initInfo();
					if (cqv->cond!=NULL && cqry->hasParams()) {
						cqry=NULL;
						if ((cqv->cond->getFlags()&EXPR_PARAMS)!=0) {
							if (qctx.ncqs>=sizeof(qctx.condQs)/sizeof(qctx.condQs[0]) || (cqry=new(&qctx.cu.alloc) Stmt(0,&qctx.cu.alloc))==NULL) {rc=RC_NOMEM; break;}
							if (((Stmt*)cqry)->addVariable()==0xFF) {rc=RC_NOMEM; break;}
							QueryWithParams &qs=qctx.condQs[qctx.ncqs++]; qs.qry=(Stmt*)cqry; qs.params=(Value*)cs.params; qs.nParams=cs.nParams;
							if ((cqry->top->cond=Expr::clone(cqv->cond,&qctx.cu.alloc))==NULL) {rc=RC_NOMEM; break;}
						}
					}
					if (rc==RC_OK) {
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
		if (cidx==NULL || rc!=RC_OK) dev->release();
	}
	if ((qctx.cu.mode&MODE_DELETED)==0 && rc==RC_OK) for (CondFT *cf=condFT; cf!=NULL; cf=cf->next) {
		if ((rc=qctx.mergeFT(qq,cf))!=RC_OK) break;
		if (qctx.nqs<sizeof(qctx.src)/sizeof(qctx.src[0])) qctx.src[qctx.nqs++]=qq; else {rc=RC_NOMEM; break;}
	}
	if (rc==RC_OK) {
		bool fArrayFilter=expr.type==VT_COLLECTION&&!expr.isNav();
		if (qctx.nqs>nqs0) {
			if (!expr.isEmpty() && expr.type!=VT_COLLECTION) {
				if (qctx.ncqs>=sizeof(qctx.condQs)/sizeof(qctx.condQs[0]) ||
											(qctx.src[qctx.nqs]=new(&qctx.cu.alloc) ExprScan(&qctx.cu.ectx,expr,qctx.flg))==NULL) rc=RC_NOMEM; else qctx.nqs++;
			}
			if (rc==RC_OK && qctx.nqs>nqs0+1 && (rc=qctx.mergeN(qq,&qctx.src[nqs0],qctx.nqs-nqs0,QRY_INTERSECT))==RC_OK) {qctx.src[nqs0]=qq; qctx.nqs=nqs0+1;}
			if (rc==RC_OK && primary!=NULL) {if ((qq=new(&qctx.cu.alloc) HashOp(primary,qctx.src[nqs0]))!=NULL) {qctx.src[nqs0]=qq; primary=NULL;} else rc=RC_NOMEM;}
		} else if (qctx.nqs>=sizeof(qctx.src)/sizeof(qctx.src[0])) rc=RC_NOMEM;
		else if (primary!=NULL) {qctx.src[qctx.nqs++]=primary; primary=NULL;}
		else if (!expr.isEmpty()) {
			if ((qctx.src[qctx.nqs]=new(&qctx.cu.alloc) ExprScan(&qctx.cu.ectx,expr,qctx.flg))!=NULL) {qctx.nqs++; fArrayFilter=false;} else rc=RC_NOMEM;
		} else {
#ifdef _DEBUG
			if ((qctx.cu.mode&(MODE_DEVENT|MODE_DELETED))==0) {char *s=qctx.cu.stmt->toString(); report(MSG_WARNING,"Full scan query: %.512s\n",s); qctx.cu.ectx.ses->free(s);}
#endif
			qctx.src[qctx.nqs]=new(&qctx.cu.alloc) FullScan(&qctx.cu.ectx,(qctx.cu.mode&(MODE_DEVENT|MODE_NODEL))==MODE_DEVENT?HOH_HIDDEN:(qctx.cu.mode&MODE_DELETED)!=0?HOH_DELETED<<16|HOH_DELETED|HOH_HIDDEN:HOH_DELETED|HOH_HIDDEN,qctx.flg);
			if (qctx.src[qctx.nqs]!=NULL) qctx.nqs++; else rc=RC_NOMEM;
		}
		if (rc==RC_OK) {
			if (fArrayFilter) {if ((qq=new(&qctx.cu.alloc) ArrayFilter(qctx.src[nqs0],expr.varray,expr.length))!=NULL) qctx.src[nqs0]=qq; else rc=RC_NOMEM;}
			if (rc==RC_OK && path!=NULL && nPathSeg!=0) {
				if (qctx.src[nqs0]->props==NULL || path[0].nPids!=0 && BIN<PropertyID>::find(path[0].nPids==1?path[0].pid:path[0].pids[0],qctx.src[nqs0]->props[0].props,qctx.src[nqs0]->props[0].nProps)==NULL) {
					PropList pl; pl.props=(PropertyID*)&path[0].pid; pl.nProps=1; pl.fFree=false;
					QueryOp *q=new(&qctx.cu.alloc,1) LoadOp(qctx.src[nqs0],&pl,1,qctx.flg); if (q!=NULL) qctx.src[nqs0]=q; else rc=RC_NOMEM;
				}
				if (rc==RC_OK && (qq=new(&qctx.cu.alloc) PathOp(qctx.src[nqs0],path,nPathSeg,qctx.flg))!=NULL) qctx.src[nqs0]=qq; else rc=RC_NOMEM;
			}
		}
	}
	if (rc==RC_OK) {
		if ((q=qctx.src[nqs0])!=NULL && (cond!=0 || props!=NULL&&nProps!=0 || condIdx!=NULL || qctx.ncqs>ncqs0)) rc=qctx.filter(q,cond,props,nProps,condIdx,qctx.ncqs-ncqs0);
		if (rc==RC_OK && fTrans) rc=qctx.out(q,this);
	}
	qctx.nqs=nqs0; qctx.ncqs=ncqs0; qctx.flg=sflg; return rc;
}

bool BuildCtx::checkSort(QueryOp *q,const OrderSegQ *req,unsigned nReq,unsigned& nP)
{
	if (q->sort==NULL || nReq==0) return 0; bool fRev=false;
	for (unsigned f=nP=0; ;nP++)
		if (nP>=nReq) {if (fRev) {nP=0; return false;} /*qop->reverse();*/ return true;}
		else if (nP>=q->nSegs || q->sort[nP].pid!=req[nP].pid || (q->sort[nP].flags&ORDER_EXPR)!=0) return false;
		else if ((f=(q->sort[nP].flags^req[nP].flags)&SORT_MASK)==0) {if (fRev) return false;}
		else if ((q->qflags&QO_REVERSIBLE)==0 || (f&~ORD_DESC)!=0) return false;
		else if (nP==0) fRev=true; else if (!fRev) return false;
}

RC BuildCtx::sort(QueryOp *&qop,const OrderSegQ *os,unsigned no,PropListP *pl,bool fTmp)
{
	if (os==NULL || no==1 && (os->flags&ORDER_EXPR)==0 && os->pid==PROP_SPEC_PINID) no=0;
	if (no==0 && (qop->qflags&QO_IDSORT)!=0) return RC_OK;

	unsigned nP=0; RC rc=RC_OK; if (checkSort(qop,os,no,nP)) return RC_OK;

	try {
		PropListP plp(&cu.alloc); if (pl!=NULL && pl->nPls!=0) plp+=*pl;
		if (!fTmp && no==1 && (os->flags&ORDER_EXPR)==0 && os->var==0 && (pl==NULL||pl->nPls==0)) {
			plp.pls[0].props=(PropertyID*)&os->pid; plp.pls[0].nProps=1; plp.pls[0].fFree=false; plp.nPls=1;
		} else if (no!=0) for (unsigned i=0; i<no; i++)
			if ((os[i].flags&ORDER_EXPR)!=0) {if ((rc=os[i].expr->mergeProps(plp,fTmp))!=RC_OK) return rc;}
			else if (os[i].pid==PROP_SPEC_PINID) {no=i+1; break;}
			else if ((rc=plp.merge(os[i].var,&os[i].pid,1,fTmp))!=RC_OK) return rc;
		if ((rc=load(qop,plp))==RC_OK) {
			Sort *srt=new(&cu.alloc,no,plp.nPls) Sort(qop,os,no,flg,nP,plp.pls,plp.nPls);
			if (srt!=NULL) {qop=srt; for (unsigned i=0; i<plp.nPls; i++) plp.pls[i].fFree=false;} else rc=RC_NOMEM;
		}
	} catch (RC rc2) {rc=rc2;}
	return rc;
}

RC BuildCtx::mergeN(QueryOp *&res,QueryOp **o,unsigned no,QUERY_SETOP op)
{
	res=NULL; RC rc; if (o==NULL || no<2) return RC_INTERNAL;
	for (unsigned i=0; i<no; i++) {if ((o[i]->getQFlags()&(QO_IDSORT|QO_UNIQUE))!=(QO_IDSORT|QO_UNIQUE) && (rc=sort(o[i],NULL,0))!=RC_OK) return rc;}
	return (res=new(&cu.alloc,no) MergeIDs(&cu.ectx,o,no,op,flg))!=NULL?RC_OK:RC_NOMEM;
}

RC BuildCtx::merge2(QueryOp *&res,QueryOp **qs,const CondEJ *cej,QUERY_SETOP qo,const Expr *cond)
{
	res=NULL; if (qs[0]==NULL || qs[1]==NULL) return RC_EOF;
	unsigned ff=qo==QRY_SEMIJOIN?QO_UNIQUE:0; RC rc; assert(qo<QRY_UNION);
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
				CondEJ *ej=(CondEJ*)ses->malloc(nej*sizeof(CondEJ)); if (ej==NULL) return RC_NOMEM;
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
			OrderSegQ *oss=(OrderSegQ*)alloca(nej*sizeof(OrderSegQ)); if (oss==NULL) return RC_NOMEM;
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
	}
	if ((qs[0]->qflags&QO_UNIQUE)!=0 && cej->propID1==PROP_SPEC_PINID) ff|=QO_UNI1;
	if ((qs[1]->qflags&QO_UNIQUE)!=0 && cej->propID2==PROP_SPEC_PINID) ff|=QO_UNI2;
	try {if ((res=new(&cu.alloc,nej) MergeOp(qs[0],qs[1],cej,nej,qo,cond,flg|ff))==NULL) return RC_NOMEM;} catch (RC rc) {return rc;}
	return RC_OK;
}

RC BuildCtx::nested(QueryOp *&res,QueryOp **qs,const Expr *cond)
{
	PropListP plp(&cu.alloc); RC rc=RC_OK;
	if (cond!=NULL && (rc=cond->mergeProps(plp))!=RC_OK) return rc;
	const unsigned nLeft=qs[0]->getNOuts(),save=plp.nPls;
	if (save>nLeft) plp.nPls=nLeft;
	if ((rc=load(qs[0],plp))!=RC_OK) {plp.nPls=save; return rc;}
	if (save>nLeft) {
		plp.pls+=nLeft; plp.nPls=save-nLeft;
		rc=load(qs[1],plp);
		plp.pls-=nLeft; plp.nPls=save;
		if (rc!=RC_OK) return rc;
	}
	return (res=new(&cu.alloc) NestedLoop(qs[0],qs[1],flg))!=NULL?RC_OK:RC_NOMEM;
}

RC BuildCtx::mergeFT(QueryOp *&res,const CondFT *cft)
{
	StringTokenizer q(cft->str,strlen(cft->str),false); const FTLocaleInfo *loc=cu.ectx.ses->getStore()->ftMgr->getLocale();
	const char *pW; size_t lW; char buf[256]; bool fStop=false,fFlt=(cft->flags&QFT_FILTER_SW)!=0; RC rc=RC_OK; res=NULL;
	QueryOp *qopsbuf[20],**qops=qopsbuf; unsigned nqops=0,xqops=20;
	while ((pW=q.nextToken(lW,loc,buf,sizeof(buf)))!=NULL) {
#if 0
		if (*pW==DEFAULT_PHRASE_DEL) {
			if (lW>2) {
				if ((++pW)[--lW-1]==DEFAULT_PHRASE_DEL) lW--;
				// store exact phrase in lower case to phrase
				StringTokenizer phraseStr(pW,lW); FTQuery **pPhrase=NULL;
				while ((pW=reqStr.nextToken(lW,loc,buf,sizeof(buf)))!=NULL) if (!loc->isStopWord(StrLen(pW,lW))) {
					if (loc->stemmer!=NULL) pW=loc->stemmer->process(pW,lW,buf);
					if ((p=(char*)cu.alloc.malloc(lW+1))!=NULL) {
						memcpy(p,pW,lW); p[lW]='\0'; StrLen word(p,lW); 
						FTQuery *ft=new(&cu.alloc) FTQuery(word);
						if (ft!=NULL) {
							if (pPhrase!=NULL) {*pPhrase=ft; pPhrase=&ft->next;}
							else {FTQuery *ph=*pQuery=new FTQuery(ft); if (ph!=NULL) {pQuery=&ph->next; pPhrase=&ft->next;}}
						}
					}
				}
			}
		} else 
#endif
		if (lW>1 && (!fFlt || !(fStop=loc->isStopWord(StrLen(pW,lW)))) || q.isEnd() && (lW>1 || nqops==0)) {
			if (loc->stemmer!=NULL) pW=loc->stemmer->process(pW,lW,buf);
			FTScan *ft=new(&cu.alloc,cft->nPids,lW+1) FTScan(&cu.ectx,pW,lW,cft->pids,cft->nPids,flg,cft->flags,fStop);
			if (ft==NULL) {rc=RC_NOMEM; break;}
			if (nqops>=xqops) {
				if (qops!=qopsbuf) {qops=(QueryOp**)cu.alloc.realloc(qops,(xqops*2)*sizeof(QueryOp*),xqops*sizeof(QueryOp*)); xqops*=2;}
				else if ((qops=(QueryOp**)cu.alloc.malloc((xqops*=2)*sizeof(QueryOp*)))!=NULL) memcpy(qops,qopsbuf,nqops*sizeof(QueryOp*));
				if (qops==NULL) return RC_NOMEM;
			}
			qops[nqops++]=ft;
		}
	}
	if (rc==RC_OK) {
		if (nqops<=1) {res=nqops!=0?qops[0]:(rc=RC_EOF,(QueryOp*)0); nqops=0;}
		else if ((rc=mergeN(res,qops,nqops,(cu.mode&MODE_ALL_WORDS)==0?QRY_UNION:QRY_INTERSECT))==RC_OK) nqops=0;
	}
	return rc;
}

RC BuildCtx::filter(QueryOp *&qop,const Expr *cond,const PropertyID *props,unsigned nProps,const CondIdx *condIdx,unsigned ncq)
{
	if ((qop->qflags&QO_ALLPROPS)==0) {
		PropListP req(&cu.alloc); flg|=QO_NODATA; RC rc=RC_OK; 		// force?
		if (cond!=NULL) if ((rc=cond->mergeProps(req))!=RC_OK) return rc;
		for (const CondIdx *ci=condIdx; ci!=NULL; ci=ci->next)
			if ((rc=ci->expr!=NULL?ci->expr->mergeProps(req):req.merge(0,&ci->ks.propID,1))!=RC_OK) return rc;
		for (unsigned i=0; i<ncq; i++) if (condQs[ncqs-i-1].qry->getTop()!=NULL)
			if ((rc=condQs[ncqs-i-1].qry->getTop()->mergeProps(req))!=RC_OK) return rc;
		if (req.nPls!=0) {flg&=~QO_NODATA; if ((rc=load(qop,req))!=RC_OK) return rc;}
	}
	try {
		Filter *flt=new(&cu.alloc) Filter(qop,nqs,flg); if ((qop=flt)==NULL) return RC_NOMEM;
		bool fOK=true; flt->cond=cond; flt->rprops=(PropertyID*)props; flt->nrProps=nProps;
		if ((flg&QO_CLASS)==0) flt->condIdx=(CondIdx*)condIdx;
		if (fOK && ncq>0) {
			assert(ncq<=ncqs);
			if ((flt->queries=new(&cu.alloc) QueryWithParams[ncq])==NULL) fOK=false;
			else {
				memcpy(flt->queries,&condQs[ncqs-ncq],ncq*sizeof(QueryWithParams)); ncqs-=ncq;
				for (unsigned i=0; i<ncq; i++) {
					QueryWithParams& qwp=flt->queries[i]; RC rc;
					if (qwp.params!=NULL && qwp.nParams!=0) {
						if ((rc=copyV(qwp.params,qwp.nParams,qwp.params,&cu.alloc))!=RC_OK) {for (;i<ncq;i++) flt->queries[i].params=NULL; fOK=false; break;}
						for (unsigned j=0; j<qwp.nParams; j++) if (qwp.params[j].type==VT_VARREF && (qwp.params[j].refV.flags&VAR_TYPE_MASK)==VAR_PARAM)
							if (qwp.params[j].refV.refN<cu.vctx[QV_PARAMS].nValues) {qwp.params[j]=cu.vctx[QV_PARAMS].vals[qwp.params[j].refV.refN]; setHT(qwp.params[j]);} else qwp.params[j].setError();
					}
				}
			}
		}
		if (!fOK) return RC_NOMEM;
	} catch (RC rc) {return rc;}
	return RC_OK;
}

RC BuildCtx::load(QueryOp *&qop,const PropListP& plp)
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
	QueryOp *q=new(&cu.alloc,plp.nPls) LoadOp(qop,plp.pls,plp.nPls,flg); if (q==NULL) return RC_NOMEM;
	for (unsigned i=0; i<plp.nPls; i++) plp.pls[i].fFree=false;
	qop=q; return RC_OK;
}

RC BuildCtx::out(QueryOp *&qop,const QVar *qv)
{
	RC rc=RC_OK; Values *outs=qv->outs; unsigned nOuts=qv->nOuts;
	if (qv->stype==SEL_PID) return RC_OK;
	if (nOuts==1 && outs[0].nValues==1 && qv->groupBy==NULL && qv->aggrs.vals==NULL) {
		const Value &v=outs[0].vals[0];
		if (v.type==VT_VARREF && (v.refV.flags&VAR_TYPE_MASK)==0 && (v.length==0 || v.refV.id==PROP_SPEC_SELF)) return RC_OK;
	}
	
	unsigned f=flg;
	if (qv->stype==SEL_AUGMENTED) f|=QO_AUGMENT|QO_ALLPROPS|QO_LOADALL;
	else {
		PropListP plp(&cu.alloc); if (qv->stype!=SEL_PINSET && qv->stype!=SEL_COMPOUND) f|=QO_UNIQUE;
		for (unsigned i=0; i<nOuts; i++) for (unsigned j=0; j<outs[i].nValues; j++) {
			const Value& v=outs[i].vals[j]; ushort vty;
			switch (v.type) {
			case VT_EXPR:
				if (v.fcalc!=0) {
					if ((((Expr*)v.expr)->getFlags()&EXPR_SELF)!=0) f|=QO_LOADALL;
					if ((rc=((Expr*)v.expr)->mergeProps(plp))!=RC_OK) return rc;
				}
				break;
			case VT_VARREF:
				vty=v.refV.flags&VAR_TYPE_MASK;
				if (vty==VAR_CTX || vty==0 && (v.length==0 || v.length==1 && v.refV.id==PROP_SPEC_SELF)) f|=QO_LOADALL;
				else if (vty==0 && v.length==1 && (rc=plp.merge(v.refV.refN,&v.refV.id,1))!=RC_OK) return rc;
				break;
			}
		}
		if (qv->aggrs.vals!=NULL) for (unsigned i=0; i<qv->aggrs.nValues; i++) {
			const Value& v=qv->aggrs.vals[i];
			switch (v.type) {
			case VT_EXPR: if (v.fcalc!=0 && (rc=((Expr*)v.expr)->mergeProps(plp))!=RC_OK) return rc; break;
			case VT_VARREF: if ((v.refV.flags&VAR_TYPE_MASK)==0 && v.length==1 && (rc=plp.merge(v.refV.refN,&v.refV.id,1))!=RC_OK) return rc; break;
			}
		}
		if (qv->groupBy!=NULL && qv->nGroupBy!=0) {
			if ((rc=sort(qop,qv->groupBy,qv->nGroupBy,&plp))!=RC_OK) return rc;
			if (outs==NULL || nOuts==0) {
				// get them from gb/nG
			}
		} else if ((rc=load(qop,plp))!=RC_OK) return rc;
	}
	try {return (qop=new(&cu.alloc) TransOp(qop,outs,nOuts,qv->aggrs,qv->groupBy,qv->nGroupBy,qv->having,f))!=NULL?RC_OK:RC_NOMEM;}
	catch (RC rc) {return rc;}
}

RC PropListP::checkVar(uint16_t var)
{
	if (var>=nPls) {
		if (var>=sizeof(pbuf)/sizeof(pbuf[0])) {
			if (pls!=pbuf) {if ((pls=(PropList*)ma->realloc(pls,(var+1)*sizeof(PropList),nPls*sizeof(PropList)))==NULL) return RC_NOMEM;}
			else {PropList *pp=new(ma) PropList[var+1]; if (pp!=NULL) {memcpy(pp,pbuf,nPls*sizeof(PropList)); pls=pp;} else return RC_NOMEM;}
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
		if ((pl.props=new(ma) PropertyID[nPids])==NULL) return RC_NOMEM;
		memcpy(pl.props,pids,(pl.nProps=nPids)*sizeof(uint32_t)); pl.fFree=true;
		if (!fFlags) for (unsigned i=0; i<nPids; i++) pl.props[i]&=STORE_MAX_URIID;
	} else for (unsigned i=0,sht=0; i<nPids; i++) {
		const PropertyID pid=pids[i]; unsigned ex=0,s2;
		PropertyID *ins,*pp=(PropertyID*)BIN<PropertyID,PropertyID,ExprPropCmp>::find(pid&STORE_MAX_URIID,pl.props+sht,pl.nProps-sht,&ins);
		if (pp!=NULL) {sht=unsigned(pp+1-pl.props); if (!fFlags||*pp==pid) continue; if (pl.fFree) {*pp|=pid; continue;}}
		else {s2=unsigned(ins-pl.props); if (s2>=pl.nProps) ex=nPids-i; else ex=1;}							// more?
		if (pl.fFree) {if ((pl.props=(PropertyID*)ma->realloc(pl.props,(pl.nProps+ex)*sizeof(PropertyID),pl.nProps*sizeof(PropertyID)))==NULL) return RC_NOMEM;}
		else if ((ins=(PropertyID*)ma->malloc((pl.nProps+ex)*sizeof(PropertyID)))==NULL) return RC_NOMEM;
		else {memcpy(ins,pl.props,pl.nProps*sizeof(PropertyID)); pl.props=ins; pl.fFree=true;}
		if (pp!=NULL) pl.props[sht-1]|=pid;
		else {
			if (s2<pl.nProps) memmove(&pl.props[s2+ex],&pl.props[s2],(pl.nProps-s2)*sizeof(PropertyID));
			memcpy(&pl.props[s2],&pids[i],ex*sizeof(uint32_t)); pl.nProps+=ex; sht+=s2+ex; i+=ex-1;
		}
	}
	return RC_OK;
}
