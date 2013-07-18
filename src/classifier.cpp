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

Written by Mark Venguerov 2004-2013

**************************************************************************************/

#include "classifier.h"
#include "queryprc.h"
#include "stmt.h"
#include "expr.h"
#include "txmgr.h"
#include "maps.h"
#include "fio.h"
#include "blob.h"

//#define REPORT_INDEX_CREATION_TIMES

using namespace AfyKernel;

static const IndexFormat classIndexFmt(KT_UINT,sizeof(uint64_t),KT_VARMDPINREFS);

Classifier::Classifier(StoreCtx *ct,unsigned timeout,unsigned hashSize,unsigned cacheSize) 
	: ClassHash(*new(ct) ClassHash::QueueCtrl(cacheSize,ct),hashSize,ct),ctx(ct),fInit(false),classIndex(ct),
	classMap(MA_CLASSINDEX,classIndexFmt,ct,TF_WITHDEL),nCached(0),xCached(cacheSize),xPropID(ct->theCB->xPropID)
{
	if (&ctrl==NULL) throw RC_NORESOURCES; ct->treeMgr->registerFactory(*this);
}

const BuiltinURI Classifier::builtinURIs[] = {
	{S_L("Classes"),		CLASS_OF_CLASSES},
	{S_L("Timers"),			CLASS_OF_TIMERS},
	{S_L("Loaders"),		CLASS_OF_LOADERS},
	{S_L("Listeners"),		CLASS_OF_LISTENERS},
	{S_L("Packages"),		CLASS_OF_PACKAGES},
	{S_L("NamedObjects"),	CLASS_OF_NAMED},
	{S_L("Enumerations"),	CLASS_OF_ENUMS},
	{S_L("Stores"),			CLASS_OF_STORES},
	{S_L("Services"),		CLASS_OF_SERVICES},
	{S_L("FSMs"),			CLASS_OF_FSMCTX},

	{S_L("pinID"),			PROP_SPEC_PINID},
	{S_L("document"),		PROP_SPEC_DOCUMENT},
	{S_L("parent"),			PROP_SPEC_PARENT},
	{S_L("value"),			PROP_SPEC_VALUE},
	{S_L("created"),		PROP_SPEC_CREATED},
	{S_L("createdBy"),		PROP_SPEC_CREATEDBY},
	{S_L("updated"),		PROP_SPEC_UPDATED},
	{S_L("updatedBy"),		PROP_SPEC_UPDATEDBY},
	{S_L("ACL"),			PROP_SPEC_ACL},
	{S_L("stamp"),			PROP_SPEC_STAMP},
	{S_L("objectID"),		PROP_SPEC_OBJID},
	{S_L("predicate"),		PROP_SPEC_PREDICATE},
	{S_L("count"),			PROP_SPEC_COUNT},
	{S_L("subclasses"),		PROP_SPEC_SUBCLASSES},
	{S_L("superclasses"),	PROP_SPEC_SUPERCLASSES},
	{S_L("indexInfo"),		PROP_SPEC_INDEX_INFO},
	{S_L("properties"),		PROP_SPEC_PROPERTIES},
	{S_L("onEnter"),		PROP_SPEC_ONENTER},
	{S_L("onUpdate"),		PROP_SPEC_ONUPDATE},
	{S_L("onLeave"),		PROP_SPEC_ONLEAVE},
	{S_L("namespace"),		PROP_SPEC_NAMESPACE},
	{S_L("ref"),			PROP_SPEC_REF},
	{S_L("service"),		PROP_SPEC_SERVICE},
	{S_L("version"),		PROP_SPEC_VERSION},
	{S_L("weight"),			PROP_SPEC_WEIGHT},
	{S_L("self"),			PROP_SPEC_SELF},
	{S_L("prototype"),		PROP_SPEC_PROTOTYPE},
	{S_L("window"),			PROP_SPEC_WINDOW},
	{S_L("timerInterval"),	PROP_SPEC_INTERVAL},
	{S_L("action"),			PROP_SPEC_ACTION},
	{S_L("address"),		PROP_SPEC_ADDRESS},
	{S_L("command"),		PROP_SPEC_COMMAND},
	{S_L("undo"),			PROP_SPEC_UNDO},
	{S_L("listen"),			PROP_SPEC_LISTEN},
	{S_L("condition"),		PROP_SPEC_CONDITION},
	{S_L("subpackage"),		PROP_SPEC_SUBPACKAGE},
	{S_L("enum"),			PROP_SPEC_ENUM},
	{S_L("bufferSize"),		PROP_SPEC_BUFSIZE},
	{S_L("pattern"),		PROP_SPEC_PATTERN},
	{S_L("exception"),		PROP_SPEC_EXCEPTION},
	{S_L("identity"),		PROP_SPEC_IDENTITY},
	{S_L("request"),		PROP_SPEC_REQUEST},
	{S_L("content"),		PROP_SPEC_CONTENT},
	{S_L("position"),		PROP_SPEC_POSITION},
	{S_L("load"),			PROP_SPEC_LOAD},
	{S_L("resolve"),		PROP_SPEC_RESOLVE},
	{S_L("transition"),		PROP_SPEC_TRANSITION},
	{S_L("state"),			PROP_SPEC_STATE},

	{S_L("encryption"),		SERVICE_ENCRYPTION},
	{S_L("serial"),			SERVICE_SERIAL},
	{S_L("bridge"),			SERVICE_BRIDGE},
	{S_L("affinity"),		SERVICE_AFFINITY},
	{S_L("regex"),			SERVICE_REGEX},
	{S_L("pathSQL"),		SERVICE_PATHSQL},
	{S_L("JSON"),			SERVICE_JSON},
	{S_L("protobuf"),		SERVICE_PROTOBUF},
	{S_L("sockets"),		SERVICE_SOCKETS},
	{S_L("IO"),				SERVICE_IO},
	{S_L("remoteRead"),		SERVICE_REMOTE},
	{S_L("replication"),	SERVICE_REPLICATION},
};

const SpecPINProps Classifier::specPINProps[9] = {
	{{1ULL<<PROP_SPEC_OBJID|1ULL<<PROP_SPEC_PREDICATE,0,0,0},								PMT_CLASS},
	{{1ULL<<PROP_SPEC_OBJID|1ULL<<PROP_SPEC_INTERVAL|1ULL<<PROP_SPEC_ACTION,0,0,0},			PMT_TIMER},
	{{1ULL<<PROP_SPEC_OBJID|1ULL<<PROP_SPEC_LISTEN,0,0,0},									PMT_LISTENER},
	{{1ULL<<PROP_SPEC_OBJID|1ULL<<PROP_SPEC_LOAD,0,0,0},									PMT_LOADER},
	{{1ULL<<PROP_SPEC_OBJID|PROP_SPEC_NAMESPACE,0,0,0},										PMT_PACKAGE},
	{{1ULL<<PROP_SPEC_SERVICE,0,0,0},														PMT_COMM},
	{{1ULL<<PROP_SPEC_OBJID|1ULL<<PROP_SPEC_ENUM,0,0,0},									PMT_ENUM},
	{{1ULL<<PROP_SPEC_OBJID,0,0,0},															PMT_NAMED},
	{{0,1ULL<<(PROP_SPEC_STATE-64),0,0},													PMT_FSMCTX},
};

#define	PREF_PMT	0x80000000

const static unsigned classMeta[MAX_BUILTIN_CLASSID+1] = {PMT_CLASS,PMT_TIMER,PMT_LISTENER,PMT_LOADER,PMT_PACKAGE|PREF_PMT,PMT_NAMED,PMT_ENUM|PREF_PMT,0,0,PMT_FSMCTX};

const char *Classifier::getBuiltinName(URIID uid,size_t& lname)
{
	for (unsigned i=0; i<sizeof(builtinURIs)/sizeof(builtinURIs[0]); i++)
		if (builtinURIs[i].uid==uid) {lname=builtinURIs[i].lname; return builtinURIs[i].name;}
	lname=0; return 0;
}

uint16_t Classifier::getMeta(ClassID cid)
{
	return cid<=MAX_BUILTIN_CLASSID?classMeta[cid]&~PREF_PMT:0;
}

URIID Classifier::getBuiltinURIID(const char *name,size_t lname,bool fSrv)
{
	for (unsigned i=0; i<sizeof(builtinURIs)/sizeof(builtinURIs[0]); i++) {
		if (!fSrv && builtinURIs[i].uid>=MIN_BUILTIN_SERVICE) break;
		if (builtinURIs[i].lname==lname && memcmp(name,builtinURIs[i].name,lname)==0)
			{if (!fSrv || builtinURIs[i].uid>=MIN_BUILTIN_SERVICE) return builtinURIs[i].uid; else break;}
	}
	return STORE_INVALID_URIID;
}

RC Classifier::initBuiltin(Session *ses)
{
	if (ses==NULL) return RC_NOSESSION;
	TxSP tx(ses); RC rc=tx.start(); if (rc!=RC_OK) return rc;
	static char namebuf[sizeof(AFFINITY_STD_URI_PREFIX)+40] = {AFFINITY_STD_URI_PREFIX};
	uint32_t idx=0; xPropID=MAX_BUILTIN_URIID+1; URI *uri; URIID uid;
	for (unsigned i=0; i<sizeof(builtinURIs)/sizeof(builtinURIs[0]); i++,idx++) {
		for (; idx<builtinURIs[i].uid; idx++) {
			sprintf(namebuf+sizeof(AFFINITY_STD_URI_PREFIX)-1,"reserved%u",(unsigned)idx);
			if ((uri=(URI*)ctx->uriMgr->insert(namebuf))==NULL) return RC_NORESOURCES;
			uid=uri->getID(); uri->release(); if (uid!=idx) return RC_INTERNAL;
		}
		if (builtinURIs[i].name==NULL) return RC_INTERNAL;
		memcpy(namebuf+sizeof(AFFINITY_STD_URI_PREFIX)-1,builtinURIs[i].name,builtinURIs[i].lname+1);
		if ((uri=(URI*)ctx->uriMgr->insert(namebuf))==NULL) return RC_NORESOURCES;
		uid=uri->getID(); uri->release(); if (uid!=idx) return RC_INTERNAL;
	}
	PIN *classPINs[MAX_BUILTIN_CLASSID+1]; unsigned nClasses=0;
	for (unsigned i=0; i<=MAX_BUILTIN_CLASSID; i++) {
		PropertyID props[20]; unsigned nProps=0;
		for (unsigned j=0; j<sizeof(specPINProps)/sizeof(specPINProps[0]); j++) if (specPINProps[j].meta==(classMeta[i]&~PREF_PMT)) {
			const SpecPINProps& sp=specPINProps[j]; uint64_t u;
			for (j=0; j<4; j++) for (u=sp.mask[j],idx=0; u!=0; u>>=1,idx++) if ((u&1)!=0) props[nProps++]=j*sizeof(uint64_t)*8+idx;
			break;
		}
		if (nProps==0) continue;
		Stmt *qry=new(ses) Stmt(0,ses); if (qry==NULL) {rc=RC_NORESOURCES; break;}
		byte var=qry->addVariable(); if (var==0xFF) {qry->destroy(); rc=RC_NORESOURCES; break;}
		if ((rc=qry->setPropCondition(var,props,nProps))!=RC_OK) {qry->destroy(); break;}
		Value *pv=new(ses) Value[2]; if (pv==NULL) {qry->destroy(); rc=RC_NORESOURCES; break;}
		pv[0].setURIID(CLASS_OF_CLASSES+i); pv[0].setPropID(PROP_SPEC_OBJID);
		pv[1].set(qry); pv[1].setPropID(PROP_SPEC_PREDICATE); setHT(pv[1],SES_HEAP);
		if (props[0]!=PROP_SPEC_OBJID || (classMeta[i]&PREF_PMT)!=0) pv[1].setMeta(META_PROP_INDEXED);
		PIN *pin=new(ses) PIN(ses,0,pv,2); if (pin==NULL) {ses->free(pv); qry->destroy(); rc=RC_NORESOURCES; break;}
		classPINs[nClasses++]=pin;
	}
	fInit=true;
	if (rc==RC_OK && nClasses!=0 && (rc=ctx->queryMgr->persistPINs(EvalCtx(ses,ECT_INSERT),classPINs,nClasses,0))==RC_OK) tx.ok();
	for (unsigned i=0; i<nClasses; i++) classPINs[i]->destroy();
	return rc;
}

RC Classifier::restoreXPropID(Session *ses)
{
	if (ses==NULL) return RC_NOSESSION; assert(ctx->theCB!=NULL);
	if (ctx->theCB->mapRoots[MA_URIID]!=INVALID_PAGEID) {
		TreeScan *scan=ctx->uriMgr->scan(ses); if (scan==NULL) return RC_NORESOURCES;
		if (scan->nextKey(GO_LAST)==RC_OK) {const SearchKey &key=scan->getKey(); if (key.type==KT_UINT) xPropID=(long)key.v.u;}
	}
	return RC_OK;
}

RC Classifier::initClasses(Session *ses)
{
	RC rc=RC_OK;
	if (!fInit) {
		MutexP lck(&lock); 
		if (!fInit) {
			PINx cb(ses),*pcb=&cb; Value v[3];
			{QCtx qc(ses); qc.ref(); ClassScan cs(&qc,CLASS_OF_CLASSES,QO_HIDDEN); cs.connect(&pcb);
			while ((rc=cs.next())==RC_OK && (rc=ctx->queryMgr->getBody(cb))==RC_OK) {
				if ((rc=cb.getV(PROP_SPEC_OBJID,v[0],0,NULL))!=RC_OK) break; assert((cb.meta&PMT_CLASS)!=0);
				if (v[0].type!=VT_URIID || v[0].uid==STORE_INVALID_CLASSID) {freeV(v[0]); rc=RC_CORRUPTED; break;}
				if ((rc=cb.getV(PROP_SPEC_PREDICATE,v[1],LOAD_SSV,ses))!=RC_OK) break;
				if (v[1].type!=VT_STMT || v[1].stmt==NULL) {rc=RC_CORRUPTED; break;}
				byte cwbuf[sizeof(ClassWindow)]; ClassWindow *cw=NULL;
				if ((rc=cb.getV(PROP_SPEC_WINDOW,v[2],LOAD_SSV,ses))==RC_NOTFOUND) rc=RC_OK;
				else if (rc==RC_OK) {cw=new(cwbuf) ClassWindow(v[2],(Stmt*)v[1].stmt); freeV(v[2]);} else break;
				ClassRef cr(v[0].uid,cb.id,0,0,v[1].meta,NULL,cw);
				if (((Stmt*)v[1].stmt)->top!=NULL && (rc=createActs(&cb,cr.acts))==RC_OK && (rc=add(ses,cr,(Stmt*)v[1].stmt))!=RC_OK)
					destroyActs(cr.acts);
				freeV(v[1]); if (rc!=RC_OK) break;
			}
			}
			if (rc==RC_OK || rc==RC_EOF) {
				QCtx qc(ses); qc.ref(); ClassScan cs(&qc,CLASS_OF_LOADERS,QO_HIDDEN); cs.connect(&pcb);
				while ((rc=cs.next())==RC_OK && (rc=ctx->queryMgr->getBody(cb))==RC_OK) {
					if ((rc=cb.load(LOAD_SSV))!=RC_OK) break; assert((cb.meta&PMT_LOADER)!=0);
					if ((rc=cb.getV(PROP_SPEC_LOAD,v[0],0,NULL))!=RC_OK) break;
					if (v[0].type!=VT_STRING) rc=RC_CORRUPTED;
					else if ((rc=ctx->fileMgr->loadExt(v[0].str,v[0].length,ses,cb.properties,cb.nProperties,false))!=RC_OK && (v[0].meta&META_PROP_OPTIONAL)!=0) rc=RC_OK;
					freeV(v[0]); if (rc!=RC_OK) break;
				}
			}
			if (rc==RC_OK || rc==RC_EOF) {
				QCtx qc(ses); qc.ref(); ClassScan cs(&qc,CLASS_OF_FSMCTX,QO_HIDDEN); cs.connect(&pcb);
				while ((rc=cs.next())==RC_OK && (rc=ctx->queryMgr->getBody(cb))==RC_OK) {
					if ((rc=cb.load(LOAD_SSV))!=RC_OK) break; assert((cb.meta&PMT_FSMCTX)!=0);
					// start machine
					cb.properties=NULL; cb.nProperties=0;
				}
			}
			if (rc==RC_OK || rc==RC_EOF) {
				QCtx qc(ses); qc.ref(); ClassScan cs(&qc,CLASS_OF_LISTENERS,QO_HIDDEN); cs.connect(&pcb);
				while ((rc=cs.next())==RC_OK && (rc=ctx->queryMgr->getBody(cb))==RC_OK) {
					if ((rc=cb.load(LOAD_SSV))!=RC_OK) break; assert((cb.meta&PMT_LISTENER)!=0);
					if ((rc=ses->listen(cb.properties,cb.nProperties,cb.mode))!=RC_OK) break;
					cb.properties=NULL; cb.nProperties=0;
				}
			}
			if (rc==RC_OK || rc==RC_EOF) {
				QCtx qc(ses); qc.ref(); ClassScan cs(&qc,CLASS_OF_TIMERS,QO_HIDDEN); cs.connect(&pcb);
				while ((rc=cs.next())==RC_OK && (rc=ctx->queryMgr->getBody(cb))==RC_OK) {
					if ((rc=cb.getV(PROP_SPEC_OBJID,v[0],0,NULL))!=RC_OK) break;
					if (v[0].type!=VT_URIID || v[0].uid==STORE_INVALID_CLASSID) {freeV(v[0]); rc=RC_CORRUPTED; break;}
					URIID cid=v[0].uid; assert((cb.meta&PMT_TIMER)!=0);
					if ((rc=cb.getV(PROP_SPEC_INTERVAL,v[0],LOAD_SSV,ses))==RC_OK) {
						if (v[0].type!=VT_INTERVAL) {freeV(v[0]); rc=RC_CORRUPTED; break;}
						if ((rc=cb.getV(PROP_SPEC_ACTION,v[1],LOAD_SSV,ses))!=RC_OK) break;
						rc=ctx->tqMgr->add(new(ctx) TimerQElt(cid,v[0].i64,cb.id,v[1],ctx));
						freeV(v[1]); if (rc!=RC_OK) break;
					}
				}
			}
			fInit=true; if (rc==RC_EOF) rc=RC_OK;
		}
	}
	return rc;
}

void Classifier::setMaxPropID(PropertyID id)
{
	for (PropertyID xid=(PropertyID)xPropID; xid<id && !cas(&xPropID,(long)xid,(long)id); xid=(PropertyID)xPropID);
}

RC Classifier::setFlags(ClassID cid,unsigned f,unsigned mask)
{
	Class *cls=getClass(cid,RW_X_LOCK); if (cls==NULL) return RC_NOTFOUND;
	cls->flags=cls->flags&~mask|f; cls->release(); return RC_OK;
}
	
RC Classifier::findEnumVal(Session *ses,URIID enumid,const char *name,size_t lname,ElementID& ei)
{
	SearchKey key((uint64_t)CLASS_OF_ENUMS); PINx pin(ses); Value enu;
	RC rc=classMap.findByPrefix(key,enumid,pin.epr.buf,pin.epr.lref); ei=STORE_COLLECTION_ID;
	if (rc==RC_OK && (rc=pin.unpack())==RC_OK && (rc=pin.getV(PROP_SPEC_ENUM,enu,LOAD_SSV,ses))==RC_OK) {
		rc=RC_NOTFOUND; unsigned i; const Value *cv;
		switch (enu.type) {
		case VT_STRING:
			if (enu.length==lname && memcmp(enu.str,name,lname)==0) {ei=enu.eid; rc=RC_OK;}
			break;
		case VT_ARRAY:
			for (i=0; i<enu.length; i++) 
				if (enu.varray[i].type==VT_STRING && enu.varray[i].length==lname && memcmp(enu.varray[i].str,name,lname)==0) {ei=enu.varray[i].eid; rc=RC_OK; break;}
			break;
		case VT_COLLECTION:
			for (cv=enu.nav->navigate(GO_FIRST); cv!=NULL; cv=enu.nav->navigate(GO_NEXT))
				if (cv->type==VT_STRING && cv->length==lname && memcmp(cv->str,name,lname)==0) {ei=cv->eid; rc=RC_OK; break;}
			break;
		}
		freeV(enu);
	}
	return rc;
}

RC Classifier::findEnumStr(Session *ses,URIID enumid,ElementID ei,char *buf,size_t& lbuf)
{
	if (ei==STORE_COLLECTION_ID) return RC_NOTFOUND;
	SearchKey key((uint64_t)CLASS_OF_ENUMS); PINx pin(ses); Value w;
	RC rc=classMap.findByPrefix(key,enumid,pin.epr.buf,pin.epr.lref);
	if (rc==RC_OK && (rc=pin.unpack())==RC_OK && (rc=pin.getV(PROP_SPEC_ENUM,w,LOAD_SSV,ses,ei))==RC_OK) {
		if (w.type==VT_STRING) {size_t ll=min(lbuf-1,(size_t)w.length); memcpy(buf,w.str,ll); buf[lbuf=ll]=0;} else rc=RC_TYPE;
		freeV(w);
	}
	return rc;
}


//--------------------------------------------------------------------------------------------------------

Class::Class(unsigned id,Classifier& cls,Session *s)
: cid(id),qe(NULL),mgr(cls),query(NULL),index(NULL),id(PIN::defPID),addr(PageAddr::invAddr),flags(0),txs(s)
{
}

Class::~Class()
{
	if (txs==NULL && query!=NULL) query->destroy();
	if (index!=NULL) {index->~ClassIndex(); if (txs!=NULL) txs->free(index); else mgr.ctx->free(index);}
}

Class *Class::createNew(ClassID id,void *mg)
{
	Class *cls=NULL; Classifier *mgr=(Classifier*)(ClassHash*)mg;
	if (mgr->nCached<mgr->xCached && (cls=new(mgr->ctx) Class(id,*mgr))!=NULL) ++mgr->nCached;
	return cls;
}

void Class::setKey(ClassID id,void *)
{
	cid=id;
	if (query!=NULL) {query->destroy(); query=NULL;}
	if (index!=NULL) {index->~ClassIndex(); if (txs!=NULL) txs->free(index); else mgr.ctx->free(index); index=NULL;}
	id=PIN::defPID; addr=PageAddr::invAddr; flags=0;
}

ClassWindow::ClassWindow(const Value& wnd,const Stmt *qry) : range(0),propID(PROP_SPEC_ANY),type(0)
{
	assert(qry!=NULL); 
	if (qry->orderBy!=NULL && qry->nOrderBy!=0) propID=qry->orderBy->pid;
	switch (wnd.type) {
	case VT_INT: case VT_UINT: range=wnd.ui; break;
	case VT_INTERVAL:
		range=wnd.i64; type=1; break;	//???
	}
}

RC Class::load(int,unsigned)
{
	SearchKey key((uint64_t)cid); RC rc=RC_NOTFOUND; Value v;
	PINx cb(Session::getSession()); size_t l=XPINREFSIZE;
	if ((rc=mgr.ctx->namedMgr->find(key,cb.epr.buf,l))==RC_OK) try {
		PINRef pr(mgr.ctx->storeID,cb.epr.buf,cb.epr.lref=byte(l)); id=pr.id;
		if ((pr.def&PR_U1)==0 || (pr.u1&PMT_CLASS)==0) throw RC_NOTFOUND;
		if ((pr.def&PR_ADDR)!=0) {cb=addr=pr.addr; cb.epr.flags|=PINEX_ADDRSET;}
		if ((rc=mgr.ctx->queryMgr->getBody(cb))==RC_OK) {
			if (((meta=cb.meta)&PMT_CLASS)==0) throw RC_NOTFOUND;
			if ((rc=cb.getV(PROP_SPEC_PREDICATE,v,LOAD_SSV,mgr.ctx))!=RC_OK) return rc;
			if (v.type!=VT_STMT || v.stmt==NULL || ((Stmt*)v.stmt)->top==NULL || ((Stmt*)v.stmt)->op!=STMT_QUERY) return RC_CORRUPTED;
			query=(Stmt*)v.stmt; flags=v.meta; v.setError();
			const static PropertyID propACL=PROP_SPEC_ACL; if (cb.defined(&propACL,1)) flags|=META_PROP_ACL;
			ClassRef info(cid,id,0,0,flags,NULL,NULL);
			if (!mgr.fInit && (flags&META_PROP_INDEXED)!=0 && (rc=mgr.createActs(&cb,info.acts))==RC_OK &&				//???????
				(rc=mgr.add(cb.getSes(),info,query))!=RC_OK) mgr.destroyActs(info.acts);
			if (rc==RC_OK && query->top!=NULL && query->top->getType()==QRY_SIMPLE && ((SimpleVar*)query->top)->condIdx!=NULL) {
				const unsigned nSegs=((SimpleVar*)query->top)->nCondIdx;
				PageID root=INVALID_PAGEID,anchor=INVALID_PAGEID; IndexFormat fmt=ClassIndex::ifmt; uint32_t height=0;
				if ((pr.def&PR_PID2)!=0) {root=uint32_t(pr.id2.pid>>16); height=uint16_t(pr.id2.pid); anchor=uint32_t(pr.id2.ident);}
				if ((pr.def&PR_U2)!=0) fmt.dscr=pr.u2;
				ClassIndex *cidx=index=new(nSegs,mgr.ctx) ClassIndex(*this,nSegs,root,anchor,fmt,height,mgr.ctx);
				if (cidx==NULL) return RC_NORESOURCES; unsigned i=0;
				for (const CondIdx *ci=((SimpleVar*)query->top)->condIdx; ci!=NULL && i<nSegs; ci=ci->next,++i)
					cidx->indexSegs[i]=ci->ks;
			}
		}
	} catch (RC rc2) {rc=rc2;}
	return rc;
}

RC Class::update(bool fForce)
{
	if (txs!=NULL || !fForce && index==NULL) return RC_OK;
	PINRef pr(mgr.ctx->storeID,id,addr);
	if (index!=NULL) {
		pr.u2=index->fmt.dscr; pr.def|=PR_U2;
		if (index->root!=INVALID_PAGEID) {
			pr.id2.pid=uint64_t(index->root)<<16|uint16_t(index->height); 
			pr.id2.ident=uint32_t(index->anchor); pr.def|=PR_PID2;
		}
	}
	return mgr.ctx->namedMgr->update(cid,pr,meta,false);
}

void Class::release()
{
	if (txs!=NULL) {this->~Class(); txs->free(this);} else mgr.release(this);
}

void Class::destroy()
{
	Classifier *mg=txs!=NULL?(Classifier*)0:&mgr; delete this; if (mg!=NULL) --mg->nCached;
}

//--------------------------------------------------------------------------------------------------

IndexFormat ClassIndex::ifmt(KT_ALL,0,0);

ClassIndex::~ClassIndex()
{
}

TreeFactory *ClassIndex::getFactory() const
{
	return &cls.mgr;
}

IndexFormat ClassIndex::indexFormat() const
{
	return fmt;
}

PageID ClassIndex::startPage(const SearchKey *key,int& level,bool fRead,bool fBefore)
{
	if (root==INVALID_PAGEID) {
		RWLockP rw(&rootLock,RW_X_LOCK);
		if (root==INVALID_PAGEID && !fRead) {
			Session *ses=Session::getSession(); MiniTx tx(ses,0);
			if (TreeStdRoot::startPage(key,level,fRead,fBefore)==INVALID_PAGEID || cls.update()!=RC_OK) 
				root=INVALID_PAGEID; else tx.ok();
		}
	}
	level=root==INVALID_PAGEID?-1:(int)height;
	return root;
}

PageID ClassIndex::prevStartPage(PageID)
{
	return INVALID_PAGEID;
}

RC ClassIndex::addRootPage(const SearchKey& key,PageID& pageID,unsigned level)
{
	RWLockP rw(&rootLock,RW_X_LOCK); PageID oldRoot=root;
	RC rc=TreeStdRoot::addRootPage(key,pageID,level);
	return rc==RC_OK && oldRoot!=root ? cls.update() : rc;
}

RC ClassIndex::removeRootPage(PageID pageID,PageID leftmost,unsigned level)
{
	RWLockP rw(&rootLock,RW_X_LOCK); PageID oldRoot=root;
	RC rc=TreeStdRoot::removeRootPage(pageID,leftmost,level);
	return rc==RC_OK && oldRoot!=root ? cls.update() : rc;
}

bool ClassIndex::lock(RW_LockType lty,bool fTry) const
{
	return fTry?rwlock.trylock(lty):(rwlock.lock(lty),true);
}

void ClassIndex::unlock() const
{
	rwlock.unlock();
}

TreeConnect *ClassIndex::persist(uint32_t& hndl) const
{
	hndl=cls.cid; return &cls.mgr;
}

void ClassIndex::destroy()
{
	cls.release();
}

Tree *Classifier::connect(uint32_t hndl)
{
	Class *cls=ctx->classMgr->getClass(hndl); if (cls==NULL) return NULL;
	if (cls->index==NULL) {cls->release(); return NULL;}
	return cls->index;
}

//------------------------------------------------------------------------------------------------------------

RC Classifier::classify(PIN *pin,ClassResult& res)
{
	res.classes=NULL; res.nClasses=res.xClasses=res.nIndices=0; RC rc; assert(fInit && pin!=NULL && (pin->addr.defined()||(pin->mode&PIN_TRANSIENT)!=0));
	if (pin->ses->classLocked==RW_X_LOCK) {
		PINx pc(pin->ses); PIN *pp[2]={pin,(PIN*)&pc}; EvalCtx ectx(pin->ses,pp,2,pp,1); ClassCreate *cd;
		for (const SubTx *st=&pin->ses->tx; st!=NULL; st=st->next) for (OnCommit *oc=st->onCommit; oc!=NULL; oc=oc->next)
			if ((cd=oc->getClass())!=NULL && cd->query!=NULL && cd->query->checkConditions(ectx,0,true) && (rc=res.insert(cd))!=RC_OK) return rc;
	}
	return classIndex.classify(pin,res);
}

void Classifier::findBase(SimpleVar *qv)
{
}

ClassPropIndex *Classifier::getClassPropIndex(const SimpleVar *qv,Session *ses,bool fAdd)
{
	ClassPropIndex *cpi=&classIndex; assert(qv->type==QRY_SIMPLE);
	if (qv->classes!=NULL && qv->nClasses>0) {
		Class *base=getClass(qv->classes[0].objectID);
		if (base!=NULL) {
			ClassRefT *cr=findBaseRef(base,ses); base->release();
			if (cr!=NULL) {
				assert(cr->cid==qv->classes[0].objectID);
				if (cr->sub!=NULL || fAdd && (cr->sub=new(ctx) ClassPropIndex(ctx))!=NULL) cpi=cr->sub;
			}
		}
	}
	return cpi;
}

RC Classifier::add(Session *ses,const ClassRef& cr,const Stmt *qry)
{
	const QVar *qv; PropDNF *dnf=NULL; size_t ldnf; RC rc;
	if (cr.cid==STORE_INVALID_CLASSID || qry==NULL || (qv=qry->top)==NULL || qv->type!=QRY_SIMPLE) return RC_INVPARAM;
	if ((rc=((SimpleVar*)qv)->getPropDNF(dnf,ldnf,ses))==RC_OK) {
		rc=getClassPropIndex((SimpleVar*)qv,ses)->add(cr,qry,dnf,ldnf,ctx)?RC_OK:RC_NORESOURCES;
		if (dnf!=NULL) ses->free(dnf);
	}
	return rc;
}

ClassRefT *Classifier::findBaseRef(const Class *cls,Session *ses)
{
	assert(cls!=NULL); Stmt *qry=cls->getQuery(); QVar *qv; if (qry==NULL || (qv=qry->top)==NULL) return NULL;
	const ClassPropIndex *cpi=&classIndex; PropDNF *dnf=NULL; size_t ldnf=0; ((SimpleVar*)qv)->getPropDNF(dnf,ldnf,ses);
	if (qv->type==QRY_SIMPLE && ((SimpleVar*)qv)->classes!=NULL && ((SimpleVar*)qv)->nClasses!=0) {
		Class *base=getClass(((SimpleVar*)qv)->classes[0].objectID);
		if (base!=NULL) {
			ClassRefT *cr=findBaseRef(base,ses); base->release();
			if (cr!=NULL && cr->cid==((SimpleVar*)qv)->classes[0].objectID && cr->sub!=NULL) cpi=cr->sub;
		}
	}
	ClassRefT *cr=(ClassRefT*)cpi->find(cls->getID(),dnf!=NULL?dnf->pids:NULL,dnf!=NULL?dnf->nIncl:0); 
	if (dnf!=NULL) ses->free(dnf); return cr;
}

RC ClassPropIndex::classify(PIN *pin,ClassResult& res)
{
	if (nClasses!=0) {
		const ClassRefT *const *cpp; RC rc;
		if ((pin->mode&PIN_PARTIAL)==0) {
			for (ClassPropIndex::it<Value> it(*this,pin->properties,pin->nProperties); (cpp=it.next())!=NULL; )
				if ((rc=classify(*cpp,pin,res))!=RC_OK) return rc;
		} else {
			unsigned nProps; const HeapPageMgr::HeapV *hprops=(const HeapPageMgr::HeapV *)pin->getPropTab(nProps);
			if (hprops!=NULL) for (ClassPropIndex::it<HeapPageMgr::HeapV> it(*this,hprops,nProps); (cpp=it.next())!=NULL; )
				if ((rc=classify(*cpp,pin,res))!=RC_OK) return rc;
		}
	}
	return RC_OK;
}

RC ClassResult::insert(const ClassRef *cr,const ClassRef **cins)
{
	if (cins==NULL && classes!=NULL) {BIN<ClassRef,ClassID,ClassRefT::ClassRefCmp>::find(cr->cid,classes,nClasses,&cins); assert(cins!=NULL);}
	if (nClasses>=xClasses) {
		ptrdiff_t sht=cins-classes; size_t old=xClasses*sizeof(ClassRef*);
		if ((classes=(const ClassRef**)ma->realloc(classes,(xClasses+=xClasses==0?16:xClasses/2)*sizeof(ClassRef*),old))==NULL) return RC_NORESOURCES;
		cins=classes+sht;
	}
	if (cins<&classes[nClasses]) memmove(cins+1,cins,(byte*)&classes[nClasses]-(byte*)cins);
	*cins=cr; nClasses++; notif|=cr->notifications; if (cr->nIndexProps!=0) nIndices++; if (cr->acts!=NULL) nActions++;
	return RC_OK;
}

RC ClassResult::checkConstraints(PIN *pin,const IntoClass *into,unsigned nInto)
{
	for (unsigned i=0; i<nInto; i++) {
		const ClassID cid=into[i].cid; bool fFound=false;
		for (unsigned j=0; j<nClasses; j++) if (classes[j]->cid==cid) {
			if ((into[i].flags&(IC_UNIQUE|IC_IDEMPOTENT))!=0) {
				// check uniquness
			}
			fFound=true; break;
		}
		if (!fFound) return RC_CONSTRAINT;
	}
	return RC_OK;
}

#if 0
const static char *actionName[] =
{
	"Action_On_Enter", "Action_On_Update", "Action_On_Leave"
};
#endif

RC ClassResult::invokeActions(PIN *pin,ClassIdxOp op,const EvalCtx *stk)
{
	const ClassActs *ac; unsigned cnt=0; RC rc; Session *ses=pin->getSes();
	if (ses==NULL || op>CI_DELETE) return RC_INTERNAL;
	PINx pc(ses); PIN *pp[2]={pin,(PIN*)&pc};
	EvalCtx ectx(ses,pp,2,NULL,0,NULL,0,stk,NULL,ECT_ACTION);
	unsigned subTxID=ses->getSubTxID();
	for (unsigned i=0; i<nClasses; i++) if ((ac=classes[i]->acts)!=NULL) {
		if (pc.id!=classes[i]->pid) {pc.cleanup(); pc.id=classes[i]->pid;}
		if (ac->acts[op].acts!=NULL) for (unsigned j=0; j<ac->acts[op].nActs; j++) {
			const Stmt *stmt=ac->acts[op].acts[j];
			if (stmt!=NULL) {
				Value v; v.set((IStmt*)stmt);
				if ((rc=ctx->queryMgr->eval(&v,ectx))==RC_OK) cnt++;
				if ((ses->getTraceMode()&TRACE_ACTIONS)!=0) {
//					ses->trace(ectx,actionName[op],rc,unsigned(cnt),NULL,0);
				}
				if (rc!=RC_OK) return rc;
				if (ses->getTxState()!=TX_ACTIVE || ses->getSubTxID()<subTxID) return RC_CONSTRAINT;
			}
		}
	}
	return RC_OK;
}

void TimerQElt::processTimeRQ()
{
	Session *ses=Session::getSession();
	if (ses!=NULL) {
		PINx self(ses,pid); PIN *pself=&self; ses->setIdentity(STORE_OWNER,true);
		EvalCtx ctx(ses,&pself,1,NULL,0,NULL,0,NULL,NULL,ECT_ACTION);
		ses->getStore()->queryMgr->eval(&act,ctx);
		ses->setIdentity(STORE_INVALID_IDENTITY,false);
	}
}

void TimerQElt::destroyTimeRQ()
{
	freeV(act); ctx->free(this);
}

RC ClassPropIndex::classify(const ClassRefT *cr,PIN *pin,ClassResult& res)
{
	const ClassRef **cins=NULL; RC rc;
	if (BIN<ClassRef,ClassID,ClassRefT::ClassRefCmp>::find(cr->cid,res.classes,res.nClasses,&cins)==NULL) {
		PINx pc(pin->ses,cr->pid); PIN *pp[2]={pin,(PIN*)&pc}; EvalCtx ectx(pin->ses,pp,2,&pin,1,NULL,0,NULL,NULL,ECT_CLASS);
		if (cr->nConds==0 || Expr::condSatisfied(cr->nConds==1?&cr->cond:cr->conds,cr->nConds,ectx)) {	// vars ??? or just self?
			if ((rc=res.insert(cr,cins))!=RC_OK || cr->sub!=NULL && (rc=cr->sub->classify(pin,res))!=RC_OK) return rc;
		}
	}
	return RC_OK;
}

RC Classifier::enable(Session *ses,Class *cls,unsigned notifications)
{
	ClassRef cr(cls->cid,cls->id,0,(ushort)notifications,cls->flags,NULL,NULL);
	MutexP lck(&lock); return add(ses,cr,cls->getQuery());
}

void Classifier::disable(Session *ses,Class *cls,unsigned notifications)
{
	//MutexP lck(&lock); classIndex.remove(cls,notifications);
}

RC Classifier::indexFormat(unsigned vt,IndexFormat& fmt) const
{
	switch (vt) {
	case VT_ANY:
		fmt=IndexFormat(KT_VAR,KT_VARKEY,KT_VARMDPINREFS); break;
	case VT_UINT: case VT_BOOL: case VT_URIID: case VT_IDENTITY: case VT_UINT64: case VT_DATETIME: case VT_CURRENT:
		fmt=IndexFormat(KT_UINT,sizeof(uint64_t),KT_VARMDPINREFS); break;
	case VT_INT: case VT_INT64: case VT_INTERVAL:
		fmt=IndexFormat(KT_INT,sizeof(int64_t),KT_VARMDPINREFS); break;
	case VT_FLOAT:
		fmt=IndexFormat(KT_FLOAT,sizeof(float),KT_VARMDPINREFS); break;
	case VT_DOUBLE:
		fmt=IndexFormat(KT_DOUBLE,sizeof(double),KT_VARMDPINREFS); break;
	case VT_STRING: case VT_URL: case VT_BSTR: case VT_STREAM:
		fmt=IndexFormat(KT_BIN,KT_VARKEY,KT_VARMDPINREFS); break;
	case VT_REF: case VT_REFID: case VT_REFPROP: case VT_REFIDPROP: case VT_REFELT: case VT_REFIDELT:
		fmt=IndexFormat(KT_REF,KT_VARKEY,KT_VARMDPINREFS); break;
	default: return RC_TYPE;
	}
	return RC_OK;
}

RC Classifier::rebuildAll(Session *ses)
{
	if (ses==NULL) return RC_NOSESSION; assert(fInit && ses->inWriteTx());
	RC rc=classMap.dropTree(); if (rc!=RC_OK) return rc;
	PINx qr(ses),*pqr=&qr; ses->resetAbortQ(); MutexP lck(&lock); ClassResult clr(ses,ses->getStore());
	QCtx qc(ses); qc.ref(); FullScan fs(&qc,0); fs.connect(&pqr);
	while (rc==RC_OK && (rc=fs.next())==RC_OK) {
		if ((qr.hpin->hdr.descr&HOH_DELETED)==0 && (rc=classify(&qr,clr))==RC_OK && clr.classes!=NULL && clr.nClasses>0) 
			rc=index(ses,&qr,clr,CI_INSERT); // data for other indices!!!
		clr.nClasses=clr.nIndices=0;
	}
	return rc==RC_EOF?RC_OK:rc;
}

namespace AfyKernel
{
	class IndexInit : public TreeStdRoot
	{
		Session	*const	ses;
		const unsigned		nSegs;
		IndexFormat		fmt;
		PageID			anchor;
		IndexSeg		indexSegs[1];
	public:
		IndexInit(Session *s,CondIdx *c,unsigned nS) : TreeStdRoot(INVALID_PAGEID,s->getStore()),ses(s),nSegs(nS),fmt(KT_ALL,0,0),anchor(INVALID_PAGEID) {
			for (unsigned i=0; i<nS; i++,c=c->next) {assert(c!=NULL); indexSegs[i]=c->ks;}
			if (nS==1) s->getStore()->classMgr->indexFormat(indexSegs[0].type==VT_ANY&&(indexSegs[0].lPrefix!=0||(indexSegs[0].flags&ORD_NCASE)!=0)?VT_STRING:indexSegs[0].type,fmt);
			else {
				// KT_BIN ?
				fmt=IndexFormat(KT_VAR,KT_VARKEY,KT_VARMDPINREFS);
			}
		}
		void			*operator new(size_t s,unsigned nSegs,Session *ses) {return ses->malloc(s+int(nSegs-1)*sizeof(IndexSeg));}
		unsigned			getMode() const {return TF_SPLITINTX|TF_NOPOST;}
		TreeFactory		*getFactory() const {return NULL;}
		IndexFormat		indexFormat() const {return fmt;}
		bool			lock(RW_LockType,bool fTry=false) const {return true;}
		void			unlock() const {}
		void			destroy() {}
		friend	class	Classifier;
		friend	class	ClassCreate;
	};
	struct ClassCtx
	{
		Session			*ses;
		IndexData		**cid;
		unsigned		nClasses;
		SubAlloc		*sa;
		SubAlloc::SubMark& mrk;
		unsigned		idx;
		size_t			limit;
		RC	insert(IndexData& id,const byte *ext,unsigned lext,SearchKey *key=NULL) {
			if (sa->getTotal()>limit) {
				RC rc; void *skey=NULL;
				if (key!=NULL && key->type>=KT_BIN && key->v.ptr.p!=NULL) {
					if ((skey=alloca(key->v.ptr.l))==NULL) return RC_NORESOURCES;
					memcpy(skey,key->v.ptr.p,key->v.ptr.l);
				}
				for (unsigned i=0; i<nClasses; i++) 
					if (cid[i]!=NULL && !cid[i]->fSkip && (cid[i]->cd->flags&META_PROP_INMEM)==0 && 
						(rc=cid[i]->flush(ses->getStore()->classMgr->getClassMap()))!=RC_OK) return rc;
				sa->truncate(mrk);
				if (skey!=NULL) {
					if ((key->v.ptr.p=sa->malloc(key->v.ptr.l))==NULL) return RC_NORESOURCES;
					memcpy((void*)key->v.ptr.p,skey,key->v.ptr.l);
				}
			}
			return id.insert(ext,lext,key);
		}
	};
};

SListOp RefBuf::compare(RefBuf& left,RefBuf& right,RefBuf *next,bool fFirst,MemAlloc& ma)
{
	unsigned n=TreePageMgr::nKeys(right.buf); RC rc;
	int c=PINRef::cmpPIDs((byte*)left.buf,(unsigned)left.lx,TreePageMgr::getK(right.buf,0),TreePageMgr::lenK(right.buf,0)),c2=c;
	if (c>=0) {
		if (n!=1) c2=PINRef::cmpPIDs((byte*)left.buf,(unsigned)left.lx,TreePageMgr::getK(right.buf,n-1),TreePageMgr::lenK(right.buf,n-1));
		if (c2>0 && next!=NULL && PINRef::cmpPIDs((byte*)left.buf,(unsigned)left.lx,TreePageMgr::getK(next->buf,0),TreePageMgr::lenK(next->buf,0))>=0) return SLO_GT;
	} else if (!fFirst) return SLO_LT;
	if ((rc=RefBuf::insert(right.buf,right.lx,(byte*)left.buf,(unsigned)left.lx,ma))!=RC_TOOBIG) return rc==RC_OK?SLO_NOOP:SLO_ERROR;
	const byte *const val=(byte*)left.buf; const unsigned lv=(unsigned)left.lx;
	if ((left.buf=(ushort*)ma.malloc(left.lx=right.lx))==NULL) return SLO_ERROR;
	if (c2>0 && next==NULL) {left.buf[0]=L_SHT*2; left.buf[1]=ushort(L_SHT*2+lv); memcpy(left.buf+2,val,lv);}
	else {
		ushort *p=right.buf+n/2,*pe=right.buf+n,delta1=ushort(n&~1),delta2=ushort((n+1)*L_SHT)-delta1;
		memcpy(left.buf,p,delta2); memcpy((byte*)left.buf+delta2,pe+1,TreePageMgr::lenKK(right.buf,n/2,n));
		for (ushort *pp=left.buf+(n-n/2+1); --pp>=left.buf; ) *pp-=delta1;
		memmove(p+1,pe+1,p[0]-right.buf[0]); for (pe=right.buf,delta2-=L_SHT; pe<=p; *pe++-=delta2);
		insert(PINRef::cmpPIDs(val,lv,TreePageMgr::getK(left.buf,0),TreePageMgr::lenK(left.buf,0))<0?right.buf:left.buf,val,lv);
	}
	return SLO_INSERT;
}

void RefBuf::insert(ushort *pb,const byte *ext,unsigned lext)
{
	ushort *p=pb,*p2; unsigned n=TreePageMgr::nKeys(p); size_t l=p[n];
	while (n>0) {
		unsigned k=n>>1; ushort *q=p+k; ushort sht=*q,ll=q[1]-sht;
		int cmp=PINRef::cmpPIDs((byte*)pb+sht,ll,ext,lext);
		if (cmp<0) {n-=k+1; p=q+1;} else if (cmp>0) n=k; else {
			byte buf[XPINREFSIZE]; size_t l2=ll,dl; assert(PINRef::getCount(ext,lext)==1);
			switch (PINRef::adjustCount((byte*)pb+sht,l2,1,buf)) {
			default: assert(0);
			case RC_OK: break;
			case RC_TRUE:
				dl=l2-(unsigned)ll; assert(l2>(unsigned)ll && dl<=unsigned(lext+L_SHT));
				if (size_t(sht+ll)<l) memmove((byte*)pb+sht+l2,(byte*)pb+sht+ll,l-sht-ll);
				memcpy((byte*)pb+sht,buf,l2); for (p2=pb+*pb/L_SHT;--p2>q;) *p2+=ushort(dl);
				break;
			}
			return;
		}
	}
	unsigned sht=*p; if (sht<l) memmove((byte*)pb+sht+lext,(byte*)pb+sht,l-sht);
	memcpy((byte*)pb+sht,ext,lext); memmove(p+1,p,l-((byte*)p-(byte*)pb)+lext);
	for (p2=pb+*pb/L_SHT+1;--p2>p;) *p2+=(ushort)lext+L_SHT; for (;p2>=pb;--p2) *p2+=L_SHT;
	//assert(pb[*pb/L_SHT-1]<=*ps);
}

RC RefBuf::insert(ushort *&buf,size_t& lx,const byte *val,unsigned lv,MemAlloc& sa,bool fSingle)
{
	unsigned l=buf==NULL?L_SHT:TreePageMgr::lenA(buf); if (l+L_SHT+lv>=0x10000) return RC_TOOBIG;
	if (l+L_SHT+lv>lx) {
		size_t nlx=lx==0&&fSingle?L_SHT*2+(lv+1&~1):lx<0x40?0x40:lx*2;
		if (nlx<l+L_SHT+lv) nlx=nextP2(l+L_SHT+lv);
		if ((buf=(ushort*)sa.realloc(buf,nlx,lx))==NULL) return RC_NORESOURCES;
		if (lx==0) *buf=L_SHT; lx=nlx;
	}
	RefBuf::insert(buf,val,lv); return RC_OK;
}

RC RefData::add(const byte *val,unsigned lv,bool fSingle)
{
	if ((lx&1)==0) {
		RC rc=RefBuf::insert(buf,lx,val,lv,*sa,fSingle); if (rc!=RC_TOOBIG) return rc;
		RefList *rl=new(sa) RefList(*sa,0); RefBuf rb(buf,lx);
		if (rl==NULL || rl->addInt(rb)==SLO_ERROR) return RC_NORESOURCES;
		lst=rl; lx=1;
	}
	RefBuf rb((ushort*)val,lv);
	return lst->addInt(rb)!=SLO_ERROR?RC_OK:RC_NORESOURCES;
}

RC RefData::flush(Tree& tree,const SearchKey& key)
{
	RC rc=RC_OK;
	if ((lx&1)!=0) {MultiKey mk(*this,key); rc=tree.insert(mk);} else if (lx!=0) rc=tree.insert(key,buf,TreePageMgr::lenA(buf),true);
	lx=0; buf=NULL; return rc;
}

RC RefData::MultiKey::nextKey(const SearchKey *&nk,const void *&value,ushort& lValue,bool& fMulti,bool fForceNext)
{
	assert((rd.lx&1)!=0); if (fForceNext) return RC_EOF;
	if (fAdv) {if ((rb=rd.lst->next())==NULL) {fAdv=false; return RC_EOF;}}
	else if (rb!=NULL) fAdv=true; else return RC_EOF;
	nk=&key; value=rb->buf; lValue=TreePageMgr::lenA(rb->buf); fMulti=true;
	return RC_OK;
}

void RefData::MultiKey::push_back()
{
	 fAdv=false;
}

RC ClassData::insert(const byte *ext,unsigned lext,SearchKey *key)
{
	return x.add(ext,lext);
}

RC ClassData::flush(Tree& tr)
{
	RC rc=RC_OK; if (x.lx!=0) {SearchKey key((uint64_t)cd->cid); rc=x.flush(tr,key);}
	return rc;
}

RC FamilyData::insert(const byte *ext,unsigned lext,SearchKey *key)
{
	if (key==NULL) return RC_INTERNAL;
	if (il==NULL && (il=new(sa) IndexList(*sa,ity))==NULL) return RC_NORESOURCES;
	IndexValue v(key->v,sa),*pi=NULL;
	if (il->add(v,&pi)==SLO_ERROR) return RC_NORESOURCES;
	return pi->refs.add(ext,lext,true);
}

RC FamilyData::flush(Tree&)
{
	if (il!=NULL) {
		MultiKey imk(*this); RC rc;
		if ((rc=cd->getIndex()->insert(imk))!=RC_OK) return rc;
		il=NULL;
	}
	return RC_OK;
}

RC FamilyData::MultiKey::nextKey(const SearchKey *&nk,const void *&value,ushort& lValue,bool& fMulti,bool fForceNext) {
	for (;;lst=NULL) {
		if (lst==NULL) {
			if (fAdv) {do if ((iv=ci.il->next())==NULL) {fAdv=false; return RC_EOF;} while (iv->refs.lx==0);}
			else if (iv!=NULL) fAdv=true; else return RC_EOF;
			nk=new(&key) SearchKey(iv->key,(TREE_KT)ci.ity,SearchKey::PLC_EMB);	// SPTR?
			if ((iv->refs.lx&1)==0) {value=iv->refs.buf; lValue=TreePageMgr::lenA(iv->refs.buf); fMulti=true; return RC_OK;}
			lst=iv->refs.lst; lst->start();
		}
		if (fForceNext) return RC_EOF; const RefBuf *rb=lst->next();
		if (rb!=NULL) {nk=&key; value=rb->buf; lValue=TreePageMgr::lenA(rb->buf); fMulti=true; return RC_OK;}
	}
}

void FamilyData::MultiKey::push_back()
{
	fAdv=false;
}

RC Classifier::classifyAll(PIN *const *pins,unsigned nPINs,Session *ses,bool fDrop)
{
	if (pins==NULL || nPINs==0) return RC_INVPARAM; if (ses==NULL) return RC_NOSESSION; 
	assert(fInit && ses->inWriteTx());

	IndexData **cid; PIN *pin; CondIdx *pci=NULL; OnCommit *cds=NULL,**pcd=&cds; SubAlloc sa(ses);
	if ((cid=new(&sa) IndexData*[nPINs])!=NULL) memset(cid,0,nPINs*sizeof(IndexData*)); else return RC_NORESOURCES;
	Value *indexed=NULL; unsigned nIndexed=0,xIndexed=0,xSegs=0; unsigned nIndex=0; RC rc=RC_OK;
	for (unsigned i=0; i<nPINs; i++) {
		pin=pins[i]; if ((pin->meta&PMT_CLASS)==0) continue;
		const Value *cv=pin->findProperty(PROP_SPEC_OBJID); assert(cv!=NULL && cv->type==VT_URIID); 
		ClassCreate *cd=new(ses) ClassCreate(cv->uid,pin->id,0,0,NULL,NULL); if (cd==NULL) {rc=RC_NORESOURCES; break;}
		cd->id=pin->id; cd->addr=pin->addr; *pcd=cd; pcd=&cd->next;
		cv=pin->findProperty(PROP_SPEC_PREDICATE); assert(cv!=NULL && cv->type==VT_STMT); Stmt *qry=(Stmt*)cv->stmt; cd->flags=cv->meta;
		if (qry==NULL || (pin->meta&(PMT_TIMER|PMT_LISTENER))==0 && (qry->op!=STMT_QUERY || qry->top==NULL) || (qry->mode&QRY_CPARAMS)!=0) rc=RC_INVPARAM;
		else if (qry->top->type==QRY_SIMPLE) for (unsigned i=0; i<((SimpleVar*)qry->top)->nClasses; i++) if (((SimpleVar*)qry->top)->classes[i].objectID==cd->cid) {rc=RC_INVPARAM; break;}
		if (rc!=RC_OK) break;
		if ((cd->query=qry->clone(STMT_OP_ALL,ses))==NULL) {rc=RC_NORESOURCES; break;}
		if ((rc=createActs(pin,cd->acts))!=RC_OK) break;
		if ((cv=pin->findProperty(PROP_SPEC_WINDOW))!=NULL && (cd->wnd=new(ses) ClassWindow(*cv,cd->query))==NULL) {rc=RC_NORESOURCES; break;}
		if (pin->findProperty(PROP_SPEC_ACL)!=NULL) cd->flags|=META_PROP_ACL;
		if (cd->query->top->type==QRY_SIMPLE) pci=((SimpleVar*)cd->query->top)->condIdx;
		if (pci==NULL) cid[i]=new(&sa) ClassData(cd,&sa);
		else {
			const unsigned nSegs=cd->nIndexProps=(ushort)((SimpleVar*)cd->query->top)->nCondIdx;
			if ((cd->cidx=new(nSegs,ses) IndexInit(ses,pci,nSegs))==NULL) {rc=RC_NORESOURCES; break;}
			// init fmt
			cid[i]=new(&sa) FamilyData(cd,cd->cidx->fmt.keyType(),&sa);
		}
		if (cid[i]==NULL) {rc=RC_NORESOURCES; break;}
		if ((cd->flags&META_PROP_INDEXED)!=0 && (pci!=NULL || Stmt::classOK(cd->query->top)) && cd->query->top->type==QRY_SIMPLE && ((SimpleVar*)cd->query->top)->checkXPropID((PropertyID)xPropID)) {
			cid[i]->fSkip=false; nIndex++;
			if (pci!=NULL) {
				if (cd->cidx->nSegs>xSegs) xSegs=cd->cidx->nSegs;
				for (unsigned i=0; i<cd->cidx->nSegs; i++) {
					Value v; v.setPropID(cd->cidx->indexSegs[i].propID);
					if ((rc=VBIN::insert(indexed,nIndexed,v.property,v,(MemAlloc*)ses,&xIndexed))!=RC_OK) break;
				}
			}
		}
		if (rc==RC_OK && fDrop) {
			// get old class here!
			//if (ci.cd->cidx!=NULL) {
			//	if ((rc=ci.cd->cidx->drop())==RC_OK) rc=ci.cd->cidx->cls.update();		// ???
			//} else 
			if ((cd->flags&META_PROP_INDEXED)!=0) {
				SearchKey key((uint64_t)cd->cid); if ((rc=classMap.remove(key,NULL,0))==RC_NOTFOUND) rc=RC_OK;
			}
			if (rc!=RC_OK) break;
		}
	}

#ifdef REPORT_INDEX_CREATION_TIMES
	TIMESTAMP st,mid,end; getTimestamp(st);
#endif

	SubAlloc::SubMark mrk; sa.mark(mrk); unsigned nS=StoreCtx::getNStores();
	ClassCtx cctx={ses,cid,nPINs,&sa,mrk,0,CLASS_BUF_LIMIT/max(nS,1u)};

	if (rc==RC_OK && nIndex>0) {
		MutexP lck(&lock); bool fTest=false; PINx qr(ses),*pqr=&qr; QueryOp *qop=NULL; ses->resetAbortQ(); QCtx qc(ses);
		if (nIndex==1 && cid[0]->cd->query!=NULL && !fDrop) {
			QBuildCtx qctx(ses,NULL,cid[0]->cd->query,0,MODE_CLASS|MODE_NODEL);
			if ((rc=qctx.process(qop))==RC_OK && qop!=NULL) qop->setHidden();
		} else {
			qc.ref(); rc=(qop=new(ses) FullScan(&qc,0,QO_RAW))!=NULL?RC_OK:RC_NORESOURCES; fTest=true;
		}
		if (qop!=NULL) qop->connect(&pqr);	//??? many ???
		const Value **vals=NULL; struct ArrayVal {ArrayVal *prev; const Value *cv; uint32_t idx,vidx;} *freeAV=NULL;
		if (xSegs>0 && (vals=(const Value**)alloca(xSegs*sizeof(Value*)))==NULL) rc=RC_NORESOURCES;
		PINx pc(ses); PIN *pp[2]={pqr,(PIN*)&pc}; EvalCtx ctx2(ses,pp,2,(PIN**)&pqr,1);
		while (rc==RC_OK && (rc=qop->next())==RC_OK) {
			byte extc[XPINREFSIZE]; bool fExtC=false,fLoaded=false; byte lextc=0;
			if (qr.epr.lref!=0) PINRef::changeFColl(qr.epr.buf,qr.epr.lref,false);
			for (cctx.idx=0; cctx.idx<nPINs; cctx.idx++) {
				IndexData &ci=*cid[cctx.idx]; if (ci.fSkip) continue;
				if (fTest && (qr.hpin->hdr.descr&HOH_DELETED)!=0) continue; assert(ci.cd!=NULL);
				if (pc.id!=ci.cd->pid) {pc.cleanup(); pc.id=ci.cd->pid;}
				if (!fTest || ci.cd->query->checkConditions(ctx2,0,true)) {
					if (ci.cd->cid<=MAX_BUILTIN_CLASSID && (classMeta[ci.cd->cid]&PREF_PMT)!=0) {
						assert(qr.id.pid!=STORE_INVALID_PID && qr.id.ident!=STORE_INVALID_IDENTITY);
						PINRef pr(ses->getStore()->storeID,qr.id,PageAddr::invAddr);
						Value w; rc=qr.getV(PROP_SPEC_OBJID,w,0,ses); assert(rc==RC_OK && w.type==VT_URIID);
						pr.prefix=w.uid; pr.def|=PR_PREF32; qr.epr.lref=pr.enc(qr.epr.buf);
					} else if (qr.epr.lref==0 && qr.pack()!=RC_OK) continue;
					if (ci.cd->cidx==NULL) rc=cctx.insert(ci,qr.epr.buf,qr.epr.lref,NULL);
					else {
						const Value *idxd; unsigned nidxd,nNulls=0;
						if (qr.properties!=NULL) {idxd=qr.properties; nidxd=qr.nProperties;}
						else {
							idxd=indexed; nidxd=nIndexed; assert(indexed!=NULL && nIndexed>0);
							if (!fLoaded) {
								if (qr.hpin!=NULL || (rc=ctx->queryMgr->getBody(qr))==RC_OK) fLoaded=true; else break;		// release?
								bool fSSV=false;
								for (unsigned i=0; i<nIndexed; i++) {
									RC rc=ctx->queryMgr->loadV(indexed[i],indexed[i].property,qr,0,NULL);
									if (rc!=RC_OK) {if (rc==RC_NOTFOUND) {rc=RC_OK; nNulls++;} else {fLoaded=false; break;}} 
									else if ((indexed[i].flags&VF_SSV)!=0) fSSV=true;
								}
								if (fSSV) rc=ctx->queryMgr->loadSSVs(indexed,nIndexed,0,ses,ses);
							}
						}
						if (rc==RC_OK && nNulls<nidxd) {
							const unsigned nSegs=ci.cd->cidx->nSegs; ArrayVal *avs0=NULL,*avs=NULL; assert(nSegs<=xSegs);
							for (unsigned k=nNulls=0; k<nSegs; k++) {
								IndexSeg& ks=ci.cd->cidx->indexSegs[k];
								const Value *cv=vals[k]=VBIN::find(ks.propID,indexed,nIndexed); assert(cv!=NULL);
								if (cv->type==VT_ANY) nNulls++;
								else if (cv->type==VT_ARRAY || cv->type==VT_COLLECTION) {
									ArrayVal *av=freeAV;
									if (av!=NULL) freeAV=av->prev;
									else if ((av=(ArrayVal*)ses->malloc(sizeof(ArrayVal)))==NULL) {rc=RC_NORESOURCES; break;}
									av->prev=avs; avs=av; av->idx=0; av->cv=cv; av->vidx=k; if (avs0==NULL) avs0=av;
									vals[k]=cv->type==VT_ARRAY?cv->varray:cv->nav->navigate(GO_FIRST);
									if (!fExtC) {memcpy(extc,qr.epr.buf,lextc=qr.epr.lref); PINRef::changeFColl(extc,lextc,true); fExtC=true;}
								}
							}
							if (nNulls<nSegs) for (;;) {		// || derived!
								SearchKey key;
								if ((rc=key.toKey(vals,nSegs,ci.cd->cidx->indexSegs,-1,ses,&sa))==RC_TYPE||rc==RC_SYNTAX) rc=RC_OK;
								else if (rc==RC_OK) {if ((rc=cctx.insert(ci,avs!=NULL?extc:qr.epr.buf,avs!=NULL?lextc:qr.epr.lref,&key))!=RC_OK) break;}
								else break;
								bool fNext=false;
								for (ArrayVal *av=avs; !fNext && av!=NULL; av=av->prev) {
									const Value *cv;
									if (av->cv->type==VT_ARRAY) {
										if (++av->idx>=av->cv->length) av->idx=0; else fNext=true;
										cv=&av->cv->varray[av->idx];
									} else {
										assert(av->cv->type==VT_COLLECTION);
										if ((cv=av->cv->nav->navigate(GO_NEXT))!=NULL) fNext=true;
										else if (av->prev!=NULL) {cv=av->cv->nav->navigate(GO_FIRST); assert(cv!=NULL);}
									}
									vals[av->vidx]=cv;
								}
								if (!fNext) break;
							}
							if (avs!=NULL) {avs0->prev=freeAV; freeAV=avs;}
						}
					}
				}
			}
			if (fLoaded) for (unsigned i=0; i<nIndexed; i++)
				{PropertyID pid=indexed[i].property; freeV(indexed[i]); indexed[i].setError(pid);}
		}
		while (freeAV!=NULL) {ArrayVal *av=freeAV; freeAV=av->prev; ses->free(av);}
		qr.cleanup(); if (qop!=NULL) delete qop; if (rc==RC_EOF) rc=RC_OK;
	}
	if (indexed!=NULL) ses->free(indexed);
#ifdef REPORT_INDEX_CREATION_TIMES
	getTimestamp(mid);
#endif
	if (rc==RC_OK) for (unsigned i=0; i<nPINs; i++) 
		if (cid[i]!=NULL && cid[i]->cd!=NULL && !cid[i]->fSkip && (cid[i]->cd->flags&META_PROP_INMEM)==0) cid[i]->flush(classMap);
	sa.release();

#ifdef REPORT_INDEX_CREATION_TIMES
	getTimestamp(end); report(MSG_DEBUG,"Index creation time: "_LD_FM", "_LD_FM"\n",mid-st,end-mid);
#endif

	if (cds!=NULL) {if (rc==RC_OK) {*pcd=ses->tx.onCommit; ses->tx.onCommit=cds;} else for (OnCommit *oc=cds; oc!=NULL; oc=cds) {cds=oc->next; oc->destroy(ses);}}
	return rc;
}

Class *Classifier::getClass(ClassID cid,RW_LockType lt)
{
	Class *cls=NULL; 
	if (cid!=STORE_INVALID_CLASSID && get(cls,cid,0,lt)==RC_NOTFOUND) {
		//Is this necessary?
		Session *ses=Session::getSession(); ClassCreate *cd;
		if (ses!=NULL) for (const SubTx *st=&ses->tx; st!=NULL; st=st->next)
			for (OnCommit *oc=st->onCommit; oc!=NULL; oc=oc->next) if ((cd=oc->getClass())!=NULL && cd->cid==cid) {
				if ((cls=new(ses) Class(cid,*this,ses))!=NULL) {
					cls->query=(Stmt*)cd->query; cls->id=cd->id; cls->addr=cd->addr; cls->flags=cd->flags;
					if (cd->cidx!=NULL) {
						ClassIndex *cidx=cls->index=new(cd->cidx->nSegs,ses) ClassIndex(*cls,cd->cidx->nSegs,cd->cidx->root,cd->cidx->anchor,cd->cidx->fmt,cd->cidx->height,ctx);
						if (cidx==NULL) {delete cls; cls=NULL;} else memcpy(cidx->indexSegs,cd->cidx->indexSegs,cd->cidx->nSegs*sizeof(IndexSeg));
					}
				}
				break;
			}
	}
	return cls;
}

RC ClassCreate::process(Session *ses)
{
	Class *cls=NULL; RC rc=RC_OK; StoreCtx *ctx=ses->getStore(); Classifier *mgr=ctx->classMgr;
	if ((rc=mgr->get(cls,cid,0,QMGR_NEW|RW_X_LOCK))==RC_OK) {
		assert(cls!=NULL); cls->id=id; cls->addr=addr; cls->flags=flags; cls->meta=PMT_CLASS;
		if ((cls->query=query->clone(STMT_OP_ALL,ctx))==NULL) rc=RC_NORESOURCES;
		else if (cidx!=NULL) {
			ClassIndex *ci=cls->index=new(cidx->nSegs,ctx) ClassIndex(*cls,cidx->nSegs,cidx->root,cidx->anchor,cidx->fmt,cidx->height,ctx);
			if (cidx==NULL) rc=RC_NORESOURCES; else memcpy(ci->indexSegs,cidx->indexSegs,cidx->nSegs*sizeof(IndexSeg));
		}
		if (rc==RC_OK) rc=cls->update(); mgr->release(cls); 
	} else if (rc==RC_ALREADYEXISTS) rc=RC_OK;
	if (rc==RC_OK && (rc=mgr->add(ses,*this,query))==RC_OK) acts=NULL;
	return rc;
}

ClassCreate *ClassCreate::getClass()
{
	return this;
}

void ClassCreate::destroy(Session *ses)
{
	if (acts!=NULL) ses->getStore()->classMgr->destroyActs(acts);
	if (cidx!=NULL) {cidx->~IndexInit(); ses->free(cidx);}
	if (wnd!=NULL) {wnd->~ClassWindow(); ses->free(wnd);}
	if (query!=NULL) ((Stmt*)query)->destroy();
	ses->free(this);
}

RC ClassDrop::process(Session *ses)
{
	RC rc=RC_OK; Class *cls; Classifier *mgr=ses->getStore()->classMgr;
	if ((cls=mgr->getClass(cid,RW_X_LOCK))==NULL) rc=RC_NOTFOUND;			// getNamed???
	else {
		if ((cls->meta&PMT_CLASS)!=0) {
			SearchKey key((uint64_t)cid);
			if ((cls->flags&META_PROP_INDEXED)!=0) {
				if (cls->index!=NULL) rc=cls->index->drop(); else if ((rc=mgr->classMap.remove(key,NULL,0))==RC_NOTFOUND) rc=RC_OK;
			}
			if ((rc=ses->getStore()->namedMgr->remove(key))==RC_OK) {
				const Stmt *qry=cls->getQuery(); const QVar *qv; ClassPropIndex *cpi;
				if (qry!=NULL && (qv=qry->getTop())!=NULL && qv->type==QRY_SIMPLE && (cpi=mgr->getClassPropIndex((SimpleVar*)qv,ses,false))!=NULL) {
					PropDNF *dnf=NULL; size_t ldnf=0;
					if ((rc=((SimpleVar*)qv)->getPropDNF(dnf,ldnf,ses))==RC_OK) {
						if (dnf==NULL) rc=cpi->remove(cls->getID(),NULL,0);
						else for (const PropDNF *p=dnf,*end=(const PropDNF*)((byte*)p+ldnf); p<end; p=(const PropDNF*)((byte*)(p+1)+int(p->nIncl+p->nExcl-1)*sizeof(PropertyID)))
						if ((rc=cpi->remove(cls->getID(),p->nIncl!=0?p->pids:(PropertyID*)0,p->nIncl))!=RC_OK) break;
					}
				}
			}
		}
		if (rc!=RC_OK) cls->release(); else if (mgr->drop(cls)) cls->destroy();
	}
	return rc;
}

void ClassDrop::destroy(Session *s)
{
	s->free(this);
}

#define	PHASE_INSERT	1
#define	PHASE_DELETE	2
#define	PHASE_UPDATE	4
#define	PHASE_UPDCOLL	8
#define	PHASE_DELNULLS	16

enum SubSetV {NewV,DelV,AllPrevV,AllCurV,UnchagedV};

RC Classifier::index(Session *ses,PIN *pin,const ClassResult& clr,ClassIdxOp op,const struct PropInfo **ppi,unsigned npi,const PageAddr *oldAddr)
{
	RC rc=RC_OK; const bool fMigrated=op==CI_UPDATE && pin->addr!=*oldAddr;
	Class *cls=NULL; ClassIndex *cidx; byte ext[XPINREFSIZE],ext2[XPINREFSIZE]; pin->mode|=PIN_RLOAD;
	PINRef pr(ctx->storeID,pin->id,pin->addr); if ((pin->meta&PMT_COMM)!=0) pr.def|=PR_SPECIAL; if ((pin->mode&PIN_HIDDEN)!=0) pr.def|=PR_HIDDEN;
	byte lext=pr.enc(ext),lext2=0; const Value *psegs[10],**pps=psegs; unsigned xSegs=10; bool fPrefix=false;
	struct SegInfo {PropertyID pid; const PropInfo *pi; SubSetV ssv; ModInfo *mi; Value v; const Value *cv; uint32_t flags,idx,prev; bool fLoaded;} keysegs[10],*pks=keysegs;
	for (unsigned i=0; rc==RC_OK && i<clr.nClasses; PINRef::changeFColl(ext,lext,false),i++) {
		const ClassRef *cr=clr.classes[i]; if ((cr->flags&META_PROP_INDEXED)==0) continue;
		if (cr->cid<=MAX_BUILTIN_CLASSID && (classMeta[cr->cid]&PREF_PMT)!=0) {
			if (!fPrefix) {
				Value w; rc=pin->getV(PROP_SPEC_OBJID,w,0,ses); assert(rc==RC_OK && w.type==VT_URIID);
				pr.prefix=w.uid; pr.def|=PR_PREF32; lext=pr.enc(ext); pr.def&=~PR_PREF32; fPrefix=true;
			}
		} else if (fPrefix) {lext=pr.enc(ext); fPrefix=false;}
		if (cr->nIndexProps==0) {
			SearchKey key((uint64_t)cr->cid);
			switch (op) {
			default: rc=RC_INVPARAM; break;
			case CI_PURGE: break;
			case CI_UDELETE: case CI_INSERT: rc=classMap.insert(key,ext,lext); break;
			case CI_UPDATE: if (fMigrated) {if (lext2==0) {pr.addr=*oldAddr; lext2=pr.enc(ext2);} rc=classMap.update(key,ext2,lext2,ext,lext);} break;	// check same?
			case CI_SDELETE: case CI_DELETE: rc=classMap.remove(key,ext,lext); break;
			}
			if (rc!=RC_OK) 
				report(MSG_ERROR,"Error %d updating(%d) class %d\n",rc,op,cr->cid);
		} else if (op==CI_PURGE) continue;
		else if ((cls=getClass(cr->cid))==NULL || (cidx=cls->getIndex())==NULL)
			report(MSG_ERROR,"Family %d not found\n",cr->cid);
		else {
//			if (pin->pb.isNull() && (rc=ctx->queryMgr->getBody(*pin,TVO_UPD,GB_REREAD))!=RC_OK) return rc;
			const unsigned nSegs=cidx->nSegs; ClassIdxOp kop=op;
			unsigned nModSegs=0,phaseMask=op!=CI_UPDATE||!fMigrated?0:PHASE_UPDATE,phaseMask2=PHASE_INSERT|PHASE_DELETE|PHASE_UPDATE|PHASE_UPDCOLL;
			if (nSegs>xSegs) {
				pks=(SegInfo*)ses->realloc(pks!=keysegs?pks:(SegInfo*)0,(xSegs=nSegs)*sizeof(SegInfo));
				pps=(const Value**)ses->realloc(pps!=psegs?pps:(const Value**)0,xSegs*sizeof(Value*));
				if (pks==NULL || pps==NULL) {ses->free(pks); ses->free(pps); return RC_NORESOURCES;}
			}
			if (fMigrated && lext2==0 && op!=CI_UDELETE && op!=CI_INSERT) {pr.addr=*oldAddr; lext2=pr.enc(ext2);}
			for (unsigned k=0; k<nSegs; k++) {
				IndexSeg& ks=cidx->indexSegs[k]; SegInfo &si=pks[k];
				si.pid=ks.propID; si.pi=NULL; si.ssv=NewV; si.mi=NULL; si.flags=ks.flags; si.idx=0; si.prev=~0u; si.fLoaded=false; si.v.setError();
				if (ppi!=NULL && (si.pi=BIN<PropInfo,PropertyID,PropInfo::PropInfoCmp>::find(si.pid,ppi,npi))!=NULL && si.pi->first!=NULL) {
					if ((si.pi->flags&PM_NEWPROP)!=0) {
						phaseMask=phaseMask&~PHASE_UPDATE|PHASE_INSERT; phaseMask2&=~PHASE_UPDCOLL;
						if ((si.flags&(ORD_NULLS_BEFORE|ORD_NULLS_AFTER))!=0) phaseMask|=PHASE_DELNULLS;
					} else {
						if ((si.pi->flags&PM_NEWVALUES)!=0) phaseMask|=PHASE_INSERT;
						if ((si.pi->flags&PM_OLDVALUES)!=0) phaseMask|=PHASE_DELETE;
						if ((si.pi->flags&PM_RESET)==0) phaseMask2|=PHASE_DELNULLS;
						if ((si.pi->flags&(PM_COLLECTION|PM_SCOLL))==PM_COLLECTION) phaseMask2&=~PHASE_UPDCOLL;
						else if ((si.pi->flags&(PM_RESET|PM_NEWCOLL))==PM_NEWCOLL && si.pi->single!=STORE_COLLECTION_ID) phaseMask|=PHASE_UPDCOLL;
					}
					nModSegs++;
				}
			}
			if (op==CI_UPDATE && (!fMigrated && nModSegs==0 || (phaseMask&=phaseMask2)==0)) {cls->release(); continue;}
			if (ppi!=NULL) {
				if (kop==CI_INSERT) phaseMask&=~PHASE_INSERT;
				else if (kop==CI_DELETE) phaseMask&=~PHASE_DELETE;
				else if ((phaseMask&PHASE_DELETE)!=0) {phaseMask&=~PHASE_DELETE; kop=CI_DELETE;}
				else if ((phaseMask&PHASE_INSERT)!=0) {phaseMask&=~PHASE_INSERT; kop=CI_INSERT;}
			}
			for (bool fAlt=false;;) {
				unsigned lastArr=~0u,nNulls=0; bool fNext;
				for (unsigned k=0; k<nSegs; k++) {
					SegInfo& si=pks[k]; bool fIt=false; si.cv=&si.v;
					if (si.pi==NULL || kop==CI_INSERT && (si.pi->flags&PM_NEWVALUES)==0) {
						if (si.pi!=NULL && (si.pi->flags&PM_RESET)!=0) rc=RC_NOTFOUND;
						else if (!si.fLoaded && (si.fLoaded=true,rc=pin->getV(si.pid,si.v,LOAD_SSV,ses))!=RC_NOTFOUND) {
							if (rc!=RC_OK) break;
							//...
						}
						si.ssv=AllCurV;
					} else if (kop==CI_INSERT) {
						if ((si.pi->flags&PM_NEWPROP)!=0 || nModSegs==1) {
							si.ssv=NewV; for (si.mi=si.pi->first; si.mi!=NULL && si.mi->pv->op==OP_DELETE; si.mi=si.mi->pnext);
							if (si.mi==NULL) rc=RC_NOTFOUND; else {fIt=si.mi->pnext!=NULL; si.cv=si.mi->pv;}
						} else {
							//...
						}
						if ((si.pi->flags&(PM_COLLECTION|PM_NEWCOLL))!=0) PINRef::changeFColl(ext,lext,true);
					} else if (kop==CI_DELETE) {
						if (fAlt) {
							// DELNULLS
						} else if ((si.pi->flags&(PM_RESET|PM_NEWVALUES))==PM_RESET || nModSegs==1) {
							si.ssv=DelV; for (si.mi=si.pi->first; si.mi!=NULL && si.mi->oldV==NULL; si.mi=si.mi->pnext);
							if (si.mi==NULL) rc=RC_NOTFOUND; else {fIt=si.mi->pnext!=NULL; si.cv=si.mi->oldV;}
						} else {
							//...
						}
					} else if (!fAlt) {
						// UPDATE
					} else if (si.pi->single!=STORE_COLLECTION_ID) {
						if (si.fLoaded) freeV(si.v); PINRef::changeFColl(ext,lext,true); if (lext2==0) lext2=pr.enc(ext2);
						if ((rc=pin->getV(si.pid,si.v,LOAD_SSV,ses,si.pi->single))!=RC_OK) break;
					}
					pps[k]=si.cv;
					if (si.cv->type==VT_ANY) {
						if ((si.flags&(ORD_NULLS_BEFORE|ORD_NULLS_AFTER))==0) {rc=RC_NOTFOUND; break;} else {rc=RC_OK; nNulls++;}
					} else if (si.cv->type==VT_ARRAY || si.cv->type==VT_COLLECTION) {
						fIt=true; pps[k]=si.cv->type==VT_ARRAY?si.cv->varray:si.cv->nav->navigate(GO_FIRST); PINRef::changeFColl(ext,lext,true);
					}
					if (fIt) {si.prev=lastArr; lastArr=k;}
				}
				if (nNulls==nSegs) rc=RC_NOTFOUND;
				if (rc!=RC_OK) break;
				do {
					SearchKey key;
					if ((rc=key.toKey(pps,nSegs,cidx->indexSegs,-1,ses))!=RC_OK) {
						if (rc==RC_TYPE || rc==RC_SYNTAX) rc=RC_OK; else break;
					} else {
						switch (kop) {
						default: assert(0);
						case CI_INSERT: case CI_UDELETE: rc=cidx->insert(key,ext,lext); break;
						case CI_DELETE: case CI_SDELETE: rc=cidx->remove(key,ext,lext); break;
						case CI_UPDATE: rc=cidx->update(key,ext2,lext2,ext,lext); break;
						}
						if (rc!=RC_OK) 
							report(MSG_ERROR,"Error %d updating(%d) index %d\n",rc,op,cr->cid);		// break???
						key.free(ses);
					}
					fNext=false;
					for (unsigned iidx=lastArr; !fNext && iidx!=~0u; ) {
						SegInfo& si=pks[iidx]; const Value *cv=NULL;
						if (si.cv->type==VT_ARRAY) {
							if (++si.idx>=si.cv->length) si.idx=0; else fNext=true;
							cv=&si.cv->varray[si.idx];
						} else if (si.cv->type==VT_COLLECTION) {
							if ((cv=si.cv->nav->navigate(GO_NEXT))!=NULL) fNext=true;
							else if (si.prev!=~0u) {cv=si.cv->nav->navigate(GO_FIRST); assert(cv!=NULL);}
						}
						if (!fNext) {
							switch (si.ssv) {
							case NewV:
								fNext=true; 
								do if ((si.mi=si.mi->pnext)==NULL) {si.mi=si.pi->first; fNext=false;} while ((cv=si.mi->pv)->op==OP_DELETE);
								si.cv=cv; break;
							case DelV:
								fNext=true; 
								do if ((si.mi=si.mi->pnext)==NULL) {si.mi=si.pi->first; fNext=false;} while ((cv=si.mi->oldV)==NULL);
								si.cv=cv; break;
							case AllCurV:
							case AllPrevV:
							case UnchagedV:
								//???
								cv=si.cv;
								break;
							}
							if (cv!=NULL) {if (cv->type==VT_ARRAY) cv=cv->varray; else if (cv->type==VT_COLLECTION) cv=cv->nav->navigate(GO_FIRST);}
						} else if (kop!=CI_INSERT && (si.flags&PM_NEWVALUES)!=0) {
							// check !new
						}
						pps[iidx]=cv; iidx=si.prev;
					}
				} while (fNext);
				if (phaseMask==0) break;
				if ((phaseMask&PHASE_INSERT)!=0) {phaseMask&=~PHASE_INSERT; kop=CI_INSERT;}
				else if ((phaseMask&PHASE_UPDATE)!=0) {phaseMask&=~PHASE_UPDATE; kop=CI_UPDATE;}
				else if ((phaseMask&PHASE_UPDCOLL)!=0) {phaseMask&=~PHASE_UPDCOLL; kop=CI_UPDATE; fAlt=true;}
				else {assert(phaseMask==PHASE_DELNULLS); phaseMask=0; kop=CI_DELETE; fAlt=true;}
			}
			cls->release(); if (rc==RC_NOTFOUND) rc=RC_OK;
		}
	}
	if (pks!=keysegs) ses->free(pks); if (pps!=psegs) ses->free(pps);
	return rc;
}

byte Classifier::getID() const
{
	return MA_HEAPDIRFIRST;
}

byte Classifier::getParamLength() const
{
	return sizeof(ClassID);
}

void Classifier::getParams(byte *buf,const Tree& tr) const
{
	ClassIndex *cidx=(ClassIndex*)&tr; __una_set((ClassID*)buf,cidx->cls.cid);
}

RC Classifier::createTree(const byte *params,byte lparams,Tree *&tree)
{
	if (lparams!=sizeof(ClassID)) return RC_CORRUPTED;
	ClassID cid=__una_get(*(ClassID*)params);
	Class *cls=getClass(cid); if (cls==NULL) return RC_NOTFOUND;
	if (cls->index==NULL) {cls->release(); return RC_NOTFOUND;}
	tree=cls->index; return RC_OK;
}

RC Classifier::getClassInfo(ClassID cid,Class *&cls,uint64_t& nPINs)
{
	nPINs=~0u; RC rc=RC_OK;
	if ((cls=getClass(cid))==NULL) return RC_NOTFOUND;
	if (cls->index==NULL) {
		SearchKey key((uint64_t)cid); if ((rc=classMap.countValues(key,nPINs))==RC_NOTFOUND) {rc=RC_OK; nPINs=0;}
	}
	return rc;
}

RC Classifier::copyActs(const Value *pv,ClassActs *&acts,unsigned idx)
{
	if (pv->isEmpty()) return RC_OK;
	if (acts==NULL) {
		if ((acts=new(ctx) ClassActs)==NULL) return RC_NORESOURCES;
		memset(acts,0,sizeof(ClassActs));
	}
	unsigned n=pv->type==VT_ARRAY?pv->length:1;
	if ((acts->acts[idx].acts=new(ctx) Stmt*[n])==NULL) {destroyActs(acts); acts=NULL; return RC_NORESOURCES;}
	if (pv->type==VT_STMT) acts->acts[idx].acts[0]=((Stmt*)pv->stmt)->clone(STMT_OP_ALL,ctx);		// null?
	else if (pv->type==VT_ARRAY) {
		for (unsigned i=n=0; i<pv->length; i++) if (pv->varray[i].type==VT_STMT) {
			acts->acts[idx].acts[n++]=((Stmt*)pv->varray[i].stmt)->clone(STMT_OP_ALL,ctx);		// null?
		}
		if (n==0) {destroyActs(acts); acts=NULL; return RC_TYPE;}
	} else {destroyActs(acts); acts=NULL; return RC_TYPE;}
	acts->acts[idx].nActs=n; return RC_OK;
}

RC Classifier::createActs(PIN *pin,ClassActs *&acts)
{
	RC rc; const Value *pv; acts=NULL;
	for (unsigned i=0; i<3; i++) if ((pv=pin->findProperty(PROP_SPEC_ONENTER+i))!=NULL && (rc=copyActs(pv,acts,i))!=RC_OK) return rc;
	if ((pv=pin->findProperty(PROP_SPEC_ACTION))!=NULL) {
		if ((pv->meta&META_PROP_ENTER)!=0 && (rc=copyActs(pv,acts,0))!=RC_OK) return rc;
		if ((pv->meta&META_PROP_UPDATE)!=0 && (rc=copyActs(pv,acts,1))!=RC_OK) return rc;
		if ((pv->meta&META_PROP_LEAVE)!=0 && (rc=copyActs(pv,acts,2))!=RC_OK) return rc;
	}
	return RC_OK;
}

void Classifier::destroyActs(ClassActs *acts)
{
	if (acts!=NULL) {
		for (unsigned i=0; i<3; i++) if (acts->acts[i].acts!=NULL) {for (unsigned j=0; j<acts->acts[i].nActs; j++) acts->acts[i].acts[j]->destroy(); ctx->free(acts->acts[i].acts);}
		ctx->free(acts);
	}
}

//--------------------------------------------------------------------------------------

ClassPropIndex::~ClassPropIndex()
{
	for (PIdxNode *pn=root,*pn2; pn!=NULL; pn=pn2) {
		while (pn->down!=NULL) pn=pn->down;
		if (pn->classes!=NULL) allc->free(pn->classes);
		if ((pn2=pn->up)!=NULL) pn2->down=pn->next; else pn2=pn->next;
		allc->free(pn);
	}
	if (other!=NULL) allc->free(other);
}

const ClassRefT *ClassPropIndex::find(ClassID cid,const PropertyID *pids,unsigned npids) const
{
	unsigned idx=0; const ClassRefT **pc; unsigned np;
	if (pids==NULL || npids==0) {pc=other; np=nOther;}
	else for (PIdxNode *pn=root;;) {
		PropertyID pid=pids[idx];
		if (pn==NULL || pn->pid>pid) return NULL;
		if (pn->pid<pid) pn=pn->next;
		else if (++idx<npids) pn=pn->down;
		else {pc=pn->classes; np=pn->nClasses; break;}
	}
	return pc!=NULL?BIN<ClassRefT,ClassID,ClassRefT::ClassRefCmp>::find(cid,pc,np):(ClassRefT*)0;
}

RC ClassPropIndex::remove(ClassID cid,const PropertyID *pids,unsigned npids)
{
	unsigned idx=0; const ClassRefT **pc,**del; unsigned *np; ClassRefT *cr;
	if (pids==NULL || npids==0) {pc=other; np=&nOther;}
	else for (PIdxNode *pn=root;;) {
		PropertyID pid=pids[idx];
		if (pn==NULL || pn->pid>pid) return RC_OK;	// NOTFOUND?
		if (pn->pid<pid) pn=pn->next;
		else if (++idx<npids) pn=pn->down;
		else {pc=pn->classes; np=&pn->nClasses; break;}
	}
	if (pc!=NULL && (cr=(ClassRefT*)BIN<ClassRefT,ClassID,ClassRefT::ClassRefCmp>::find(cid,pc,*np,&del))!=NULL) {
		--*np; assert(del!=NULL && cr==*del);
		if (del<&pc[*np]) memmove(del,del+1,(byte*)&pc[*np]-(byte*)del);
		if (--cr->refCnt==0) {
			if (cr->nConds==1) allc->free(cr->cond);
			else for (unsigned i=0; i<cr->nConds; i++) allc->free(cr->conds[i]);
//???			if (cr->acts!=NULL) destroyActs(cr->acts);
			//cr->sub ???
			allc->free(cr); --nClasses;
		}
	}
	return RC_OK;
}

bool ClassPropIndex::add(const ClassRef& info,const Stmt *qry,const PropDNF *dnf,size_t ldnf,StoreCtx *ctx)
{
	ClassRefT *cr=NULL; if (dnf==NULL || ldnf==0) return insert(info,qry,cr,other,nOther)==RC_OK;
	for (const PropDNF *p=dnf,*end=(const PropDNF*)((byte*)p+ldnf); p<end; p=(const PropDNF*)((byte*)(p+1)+int(p->nIncl+p->nExcl-1)*sizeof(PropertyID))) {
		unsigned idx=0,nProps=p->nIncl; if (nProps==0) return insert(info,qry,cr,other,nOther)==RC_OK;
		for (PIdxNode *pn=root,*par=NULL,*lsib=NULL;;) {
			if (pn==NULL) {
				assert(idx<nProps);
				do {
					pn=new(allc) PIdxNode; if (pn==NULL) return false;
					pn->classes=NULL; pn->nClasses=0; pn->pid=p->pids[idx];
					if (lsib!=NULL) {pn->next=lsib->next; lsib->next=pn;}
					else if (par!=NULL) {pn->next=par->down; par->down=pn;}
					else {pn->next=root; root=pn;}
					pn->down=lsib=NULL; pn->up=par; par=pn;
				} while (++idx<nProps);
				if (insert(info,qry,cr,pn->classes,pn->nClasses)!=RC_OK) return false;
				break;
			}
			PropertyID pid=p->pids[idx];
			if (pn->pid<pid) {lsib=pn; pn=pn->next;}
			else if (pn->pid>pid) pn=NULL;
			else if (++idx<nProps) {par=pn; lsib=NULL; pn=pn->down;}
			else if (insert(info,qry,cr,pn->classes,pn->nClasses)!=RC_OK) return false;
			else break;
		}
	}
	return true;
}

RC ClassPropIndex::insert(const ClassRef& info,const Stmt *qry,ClassRefT *&cr,const ClassRefT **&pc,unsigned& n)
{
	unsigned idx=0;
	if (pc!=NULL) for (unsigned nc=n,base=0; nc>0;) {
		unsigned k=nc>>1; ClassRefT *q=(ClassRefT*)pc[idx=base+k];
		if (q->cid==info.cid) {q->notifications|=info.notifications; return RC_OK;}
		if (q->cid>info.cid) nc=k; else {base+=k+1; nc-=k+1; idx++;}
	}
	if (cr==NULL) {
		QVar *qv=qry!=NULL?qry->top:(QVar*)0; CondIdx *ci=qv!=NULL && qv->type==QRY_SIMPLE?((SimpleVar*)qv)->condIdx:(CondIdx*)0;
		ushort nProps=ci!=NULL?(ushort)((SimpleVar*)qv)->nCondIdx:0; unsigned nConds=0; ClassWindow *cw=NULL;
		const Expr **cnd=qv->nConds==1?(const Expr**)&qv->cond:(const Expr**)qv->conds;
		for (unsigned i=0; i<qv->nConds; i++) if ((cnd[i]->getFlags()&EXPR_PARAMS)==0) nConds++;
		if (ci!=NULL && (qry->mode&QRY_IDXEXPR)!=0) {/* count expr properties*/}
		if (info.wnd!=NULL && (cw=new(allc) ClassWindow(*info.wnd))==NULL) return RC_NORESOURCES;
		if ((cr=new(nProps,allc) ClassRefT(info.cid,info.pid,nProps,ushort(nConds),ushort(info.notifications),info.flags,info.acts,cw))==NULL) return RC_NORESOURCES;
		if (nProps>0) for (unsigned i=0; i<nProps && ci!=NULL; ++i,ci=ci->next)
			if (ci->expr==NULL) cr->indexProps[i]=ci->ks.propID; else {/*...*/}
		if (nConds!=0) {
			Expr **ccnd=&cr->cond;
			if (nConds>1 && (cr->conds=ccnd=new(allc) Expr*[nConds])==NULL) {allc->free(cr); return RC_NORESOURCES;}
			for (unsigned i=0,cnt=0; i<qv->nConds; i++)
				if ((cnd[i]->getFlags()&EXPR_PARAMS)==0 && (ccnd[cnt++]=Expr::clone(cnd[i],allc))==NULL)
					{while (cnt>0) allc->free(ccnd[--cnt]); if (nConds>1) allc->free(ccnd); allc->free(cr); return RC_NORESOURCES;}
		}
		nClasses++;
	}
	pc=(const ClassRefT**)allc->realloc(pc,(n+1)*sizeof(ClassRefT*),n*sizeof(ClassRefT*));
	if (pc==NULL) {n=0; return RC_NORESOURCES;}
	if (idx<n) memmove(&pc[idx+1],&pc[idx],(n-idx)*sizeof(ClassRefT*)); 
	pc[idx]=cr; n++; cr->refCnt++; return RC_OK;
}

//-----------------------------------------------------------------------------------------------------------------------------

IndexNavImpl::IndexNavImpl(Session *s,ClassIndex *ci,PropertyID pi) 
	: ses(s),nVals(ci->getNSegs()),pid(pi),cidx(ci),scan(NULL),fFree(false)
{
	if (cidx!=NULL) scan=cidx->scan(s,NULL,NULL,0,ci->getIndexSegs(),ci->getNSegs(),this); for (unsigned i=0; i<nVals; i++) v[i].setError();
}

IndexNavImpl::~IndexNavImpl()
{
	if (scan!=NULL) scan->destroy();
	if (cidx!=NULL) cidx->release();
	if (fFree) for (unsigned i=0; i<nVals; i++) freeV(v[i]);
}

void IndexNavImpl::newKey()
{
	if (fFree) {for (unsigned i=0; i<nVals; i++) freeV(v[i]); fFree=false;}
}

RC IndexNavImpl::next(PID& id,GO_DIR dir)
{
	RC rc=RC_EOF;
	try {
		const byte *p; size_t l; 
		if (scan!=NULL && !ses->getStore()->inShutdown() && (p=(byte*)scan->nextValue(l,dir))!=NULL)
			rc=PINRef::getPID(p,l,ses->getStore()->storeID,id);
	} catch (RC rc2) {rc=rc2;} catch (...) {report(MSG_ERROR,"Exception in IndexNav::next()\n"); rc=RC_INTERNAL;}
	try {if (scan!=NULL) scan->release();} catch(...) {}
	return rc;
}

RC IndexNavImpl::position(const Value *pv,unsigned nValues)
{
	RC rc=RC_OK;
	try {
		if (scan!=NULL) {
			//...
		}
		return RC_EOF;
	} catch (RC rc2) {rc=rc2;} catch (...) {report(MSG_ERROR,"Exception in IndexNav::position()\n"); rc=RC_INTERNAL;}
	try {if (scan!=NULL) scan->release();} catch(...) {}
	return rc;
}

RC IndexNavImpl::getValue(const Value *&pv,unsigned& nValues)
{
	RC rc=RC_OK;
	try {
		if (scan==NULL||ses->getStore()->inShutdown()) return RC_EOF;
		if (!fFree && (rc=scan->getKey().getValues(v,nVals,cidx->getIndexSegs(),nVals,ses))==RC_OK) fFree=true;
		pv=v; nValues=nVals;
	} catch (RC rc2) {rc=rc2;} catch (...) {report(MSG_ERROR,"Exception in IndexNav::getValue()\n"); rc=RC_INTERNAL;}
	try {if (scan!=NULL) scan->release();} catch(...) {}
	return rc;
}

const Value *IndexNavImpl::next()
{
	try {
		if (scan!=NULL) {
			for (;;) {
				if (fFree) {fFree=false; for (unsigned i=0; i<nVals; i++) {freeV(v[i]); v[i].setError();}}
				if (ses->getStore()->inShutdown() || scan->nextKey()!=RC_OK) break;
				if (scan->hasValues() && scan->getKey().getValues(v,nVals,cidx->getIndexSegs(),nVals,ses,false)==RC_OK && v->type!=VT_ANY)
					{fFree=true; scan->release(); return v;}
			}
			scan->destroy(); scan=NULL; cidx->release(); cidx=NULL;
		}
		return NULL;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IndexNav::next()\n");}
	try {if (scan!=NULL) scan->release();} catch(...) {}
	return NULL;
}

unsigned IndexNavImpl::nValues()
{
	try {return nVals;} catch (...) {report(MSG_ERROR,"Exception in IndexNav::nValues()\n"); return ~0u;} 
}

void IndexNavImpl::destroy()
{
	try {delete this;} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IndexNav::destroy()\n");} 
}
