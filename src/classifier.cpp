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

Written by Mark Venguerov 2004-2013

**************************************************************************************/

#include "classifier.h"
#include "queryprc.h"
#include "stmt.h"
#include "expr.h"
#include "txmgr.h"
#include "maps.h"
#include "blob.h"

//#define REPORT_INDEX_CREATION_TIMES

using namespace AfyKernel;

static const IndexFormat classIndexFmt(KT_UINT,sizeof(uint64_t),KT_PINREFS);

Classifier::Classifier(StoreCtx *ct,unsigned timeout,unsigned hashSize,unsigned cacheSize) 
	: ClassHash(*new(ct) ClassHash::QueueCtrl(cacheSize,ct),hashSize,ct),ctx(ct),classIndex(ct),
	classMap(MA_CLASSINDEX,classIndexFmt,ct,TF_WITHDEL),nCached(0),xCached(cacheSize)
{
	if (&ctrl==NULL) throw RC_NORESOURCES; ct->treeMgr->registerFactory(*this);
}

RC Classifier::initClassPIN(Session *ses,ClassID cid,const PropertyID *props,unsigned nProps,PIN *&pin)
{
	RC rc;
	Stmt *qry=new(ses) Stmt(0,ses); if (qry==NULL) return RC_NORESOURCES;
	byte var=qry->addVariable(); if (var==0xFF) {qry->destroy(); return RC_NORESOURCES;}
	if ((rc=qry->setPropCondition(var,props,nProps))!=RC_OK) {qry->destroy(); return rc;}
	Value *pv=new(ses) Value[2]; if (pv==NULL) {qry->destroy(); return RC_NORESOURCES;}
	pv[0].setURIID(cid); pv[0].setPropID(PROP_SPEC_OBJID);
	pv[1].set(qry); pv[1].setPropID(PROP_SPEC_PREDICATE); setHT(pv[1],SES_HEAP);
	if (props[0]!=PROP_SPEC_OBJID) pv[1].setMeta(META_PROP_INDEXED);
	if ((pin=new(ses) PIN(ses,0,pv,2))==NULL) {ses->free(pv); qry->destroy(); return RC_NORESOURCES;}
	return RC_OK;
}

RC Classifier::setFlags(ClassID cid,unsigned f,unsigned mask)
{
	Class *cls=getClass(cid,RW_X_LOCK); if (cls==NULL) return RC_NOTFOUND;
	cls->flags=cls->flags&~mask|f; cls->release(); return RC_OK;
}
	
RC Classifier::findEnumVal(Session *ses,URIID enumid,const char *name,size_t lname,ElementID& ei)
{
	PINx pin(ses); Value enu; ei=STORE_COLLECTION_ID; RC rc=ctx->namedMgr->getNamed(enumid,pin);
	if (rc==RC_OK && (rc=pin.getV(PROP_SPEC_ENUM,enu,LOAD_SSV,ses))==RC_OK) {
		rc=RC_NOTFOUND;
		switch (enu.type) {
		case VT_STRING:
			if (enu.length==lname && memcmp(enu.str,name,lname)==0) {ei=enu.eid; rc=RC_OK;}
			break;
		case VT_COLLECTION:
			if (!enu.isNav()) {
				for (unsigned i=0; i<enu.length; i++) 
					if (enu.varray[i].type==VT_STRING && enu.varray[i].length==lname && memcmp(enu.varray[i].str,name,lname)==0) {ei=enu.varray[i].eid; rc=RC_OK; break;}
			} else {
				for (const Value *cv=enu.nav->navigate(GO_FIRST); cv!=NULL; cv=enu.nav->navigate(GO_NEXT))
					if (cv->type==VT_STRING && cv->length==lname && memcmp(cv->str,name,lname)==0) {ei=cv->eid; rc=RC_OK; break;}
			}
			break;
		}
		freeV(enu);
	}
	return rc;
}

RC Classifier::findEnumStr(Session *ses,URIID enumid,ElementID ei,char *buf,size_t& lbuf)
{
	if (ei==STORE_COLLECTION_ID) return RC_NOTFOUND;
	PINx pin(ses); Value w; RC rc=ctx->namedMgr->getNamed(enumid,pin);
	if (rc==RC_OK && (rc=pin.getV(PROP_SPEC_ENUM,w,LOAD_SSV,ses,ei))==RC_OK) {
		if (w.type==VT_STRING) {size_t ll=min(lbuf-1,(size_t)w.length); memcpy(buf,w.str,ll); buf[lbuf=ll]=0;} else rc=RC_TYPE;
		freeV(w);
	}
	return rc;
}


//--------------------------------------------------------------------------------------------------------

Class::Class(unsigned id,Classifier& cls,Session *s)
: cid(id),qe(NULL),mgr(cls),query(NULL),index(NULL),id(PIN::noPID),addr(PageAddr::noAddr),flags(0),txs(s)
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
	id=PIN::noPID; addr=PageAddr::noAddr; flags=0;
}

ClassWindow::ClassWindow(const Value& wnd,const Stmt *qry) : range(0),propID(PROP_SPEC_ANY),type(CWT_COUNT)
{
	switch (wnd.type) {
	case VT_INT: case VT_UINT: range=wnd.ui; break;
	case VT_INT64: case VT_UINT64: range=wnd.ui64; break;
	case VT_INTERVAL: 
		propID=qry!=NULL && qry->orderBy!=NULL && qry->nOrderBy!=0 && (qry->orderBy->flags&ORDER_EXPR)==0?qry->orderBy->pid:PROP_SPEC_CREATED;
		if (wnd.i64<0) {range=(uint64_t)-wnd.i64; type=CWT_REL_INTERVAL;} else {range=wnd.ui64; type=CWT_INTERVAL;} break;
	}
}

RC Class::load(int,unsigned)
{
	SearchKey key((uint64_t)cid); RC rc=RC_NOTFOUND; Value v;
	PINx cb(Session::getSession()); size_t l=XPINREFSIZE;
	if ((rc=mgr.ctx->namedMgr->find(key,cb.epr.buf,l))==RC_OK) try {
		PINRef pr(mgr.ctx->storeID,cb.epr.buf); id=pr.id;
		if ((pr.def&PR_U1)==0 || (pr.u1&PMT_CLASS)==0) throw RC_NOTFOUND;
		if ((pr.def&PR_ADDR)!=0) {cb=addr=pr.addr; cb.epr.flags|=PINEX_ADDRSET;}
		if ((rc=mgr.ctx->queryMgr->getBody(cb))==RC_OK) {
			if (((meta=cb.meta)&PMT_CLASS)==0) throw RC_NOTFOUND;
			if ((rc=cb.getV(PROP_SPEC_PREDICATE,v,LOAD_SSV,mgr.ctx))!=RC_OK) return rc;
			if (v.type!=VT_STMT || v.stmt==NULL || ((Stmt*)v.stmt)->top==NULL || ((Stmt*)v.stmt)->op!=STMT_QUERY) return RC_CORRUPTED;
			query=(Stmt*)v.stmt; flags=v.meta; v.setError();
			const static PropertyID propACL=PROP_SPEC_ACL; if (cb.defined(&propACL,1)) flags|=META_PROP_ACL;
			ClassRef info(cid,id,0,0,flags,NULL,NULL);
			if (!mgr.ctx->namedMgr->isInit() && (flags&META_PROP_INDEXED)!=0 && (rc=mgr.createActs(&cb,info.acts))==RC_OK &&				//???????
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

RC Classifier::classify(PIN *pin,ClassResult& res,Session *ses)
{
	RC rc; assert(ctx->namedMgr->fInit && pin!=NULL && (pin->addr.defined()||(pin->mode&PIN_TRANSIENT)!=0));
	res.classes=NULL; res.nClasses=res.xClasses=res.nIndices=0;
	if (ses->classLocked==RW_X_LOCK) {
		PINx pc(ses); PIN *pp[2]={pin,(PIN*)&pc}; EvalCtx ectx(ses,pp,2,pp,1); ClassCreate *cd;
		for (const SubTx *st=&ses->tx; st!=NULL; st=st->next) for (OnCommit *oc=st->onCommit.head; oc!=NULL; oc=oc->next)
			if ((cd=oc->getClass())!=NULL && cd->query!=NULL && cd->query->checkConditions(ectx,0,true) && (rc=res.insert(cd))!=RC_OK) return rc;
	}
	return classIndex.classify(pin,res,ses);
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
	if (cr.cid==STORE_INVALID_URIID || qry==NULL || (qv=qry->top)==NULL || qv->type!=QRY_SIMPLE) return RC_INVPARAM;
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

RC ClassPropIndex::classify(PIN *pin,ClassResult& res,Session *ses)
{
	if (nClasses!=0) {
		const ClassRefT *const *cpp; RC rc;
		if (pin->fPartial==0) {
			for (ClassPropIndex::it<Value> it(*this,pin->properties,pin->nProperties); (cpp=it.next())!=NULL; )
				if ((rc=classify(*cpp,pin,res,ses))!=RC_OK) return rc;
		} else {
			unsigned nProps; const HeapPageMgr::HeapV *hprops=(const HeapPageMgr::HeapV *)pin->getPropTab(nProps);
			if (hprops!=NULL) for (ClassPropIndex::it<HeapPageMgr::HeapV> it(*this,hprops,nProps); (cpp=it.next())!=NULL; )
				if ((rc=classify(*cpp,pin,res,ses))!=RC_OK) return rc;
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

RC ClassResult::invokeActions(Session *ses,PIN *pin,ClassIdxOp op,const EvalCtx *stk)
{
	RC rc=RC_OK; const ClassActs *ac; if (op>CI_DELETE) return RC_INTERNAL;
	try {
		SyncCall sc(ses); unsigned subTxID=ses->getSubTxID();
		PINx cls(ses); PIN autop(ses); PIN *pp[5]={pin,(PIN*)&cls,NULL,NULL,&autop};
		EvalCtx ectx(ses,pp,sizeof(pp)/sizeof(pp[0]),NULL,0,NULL,0,stk,NULL,ECT_ACTION);
		for (unsigned i=0; i<nClasses; i++) if ((ac=classes[i]->acts)!=NULL) {
			if (cls.id!=classes[i]->pid) {cls.cleanup(); cls.id=classes[i]->pid;}
			if (ac->acts[op].acts!=NULL) for (unsigned j=0; j<ac->acts[op].nActs; j++) {
				const Stmt *stmt=ac->acts[op].acts[j];
				if (stmt!=NULL) {
					Value v; v.set((IStmt*)stmt); rc=ctx->queryMgr->eval(&v,ectx/*,EV_ASYNC?*/);
					if ((ses->getTraceMode()&TRACE_ACTIONS)!=0) {
						for (unsigned k=1,m=ses->getSyncStack(); k<m; k++) ses->trace(0,"\t");
						const static char *actionName[] = {"ENTER", "UPDATE", "LEAVE"};
						URI *uri=(URI*)ctx->uriMgr->ObjMgr::find(classes[i]->cid); size_t ln; const char *nm=ctx->namedMgr->getTraceName(uri,ln);
						ses->trace(0,"Class \"%.*s\",PIN @" _LS_FM ": %s(%d) -> %s\n",ln,nm,pin->id.pid,actionName[op-CI_INSERT],j,getErrMsg(rc));
						if (uri!=NULL) uri->release();
					}
					if (rc!=RC_OK) break;
					if (ses->getTxState()!=TX_ACTIVE || ses->getSubTxID()<subTxID) {rc=RC_CONSTRAINT; break;}
				}
			}
		}
	} catch (RC rc2) {rc=rc2;}
	return rc;
}

RC ClassPropIndex::classify(const ClassRefT *cr,PIN *pin,ClassResult& res,Session *ses)
{
	const ClassRef **cins=NULL; RC rc;
	if (BIN<ClassRef,ClassID,ClassRefT::ClassRefCmp>::find(cr->cid,res.classes,res.nClasses,&cins)==NULL) {
		PINx pc(ses,cr->pid); PIN *pp[2]={pin,(PIN*)&pc}; EvalCtx ectx(ses,pp,2,&pin,1,NULL,0,NULL,NULL,ECT_CLASS);
		if (cr->nConds==0 || Expr::condSatisfied(cr->nConds==1?&cr->cond:cr->conds,cr->nConds,ectx)) {	// vars ??? or just self?
			if ((rc=res.insert(cr,cins))!=RC_OK || cr->sub!=NULL && (rc=cr->sub->classify(pin,res,ses))!=RC_OK) return rc;
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
		fmt=IndexFormat(KT_VAR,KT_VARKEY,KT_PINREFS); break;
	case VT_UINT: case VT_BOOL: case VT_URIID: case VT_IDENTITY: case VT_UINT64: case VT_DATETIME: case VT_CURRENT:
		fmt=IndexFormat(KT_UINT,sizeof(uint64_t),KT_PINREFS); break;
	case VT_INT: case VT_INT64: case VT_INTERVAL:
		fmt=IndexFormat(KT_INT,sizeof(int64_t),KT_PINREFS); break;
	case VT_FLOAT:
		fmt=IndexFormat(KT_FLOAT,sizeof(float),KT_PINREFS); break;
	case VT_DOUBLE:
		fmt=IndexFormat(KT_DOUBLE,sizeof(double),KT_PINREFS); break;
	case VT_STRING: case VT_URL: case VT_BSTR: case VT_STREAM:
		fmt=IndexFormat(KT_BIN,KT_VARKEY,KT_PINREFS); break;
	case VT_REF: case VT_REFID: case VT_REFPROP: case VT_REFIDPROP: case VT_REFELT: case VT_REFIDELT:
		fmt=IndexFormat(KT_REF,KT_VARKEY,KT_PINREFS); break;
	default: return RC_TYPE;
	}
	return RC_OK;
}

RC Classifier::rebuildAll(Session *ses)
{
	if (ses==NULL) return RC_NOSESSION; assert(ctx->namedMgr->fInit && ses->inWriteTx());
	RC rc=classMap.dropTree(); if (rc!=RC_OK) return rc;
	PINx qr(ses),*pqr=&qr; ses->resetAbortQ(); MutexP lck(&lock); ClassResult clr(ses,ses->getStore());
	QCtx qc(ses); qc.ref(); FullScan fs(&qc,0); fs.connect(&pqr);
	while (rc==RC_OK && (rc=fs.next())==RC_OK) {
		if ((qr.hpin->hdr.descr&HOH_DELETED)==0 && (rc=classify(&qr,clr,ses))==RC_OK && clr.classes!=NULL && clr.nClasses>0) 
			rc=index(ses,&qr,clr,CI_INSERT); // data for other indices!!!
		clr.nClasses=clr.nIndices=0;
	}
	return rc==RC_EOF?RC_OK:rc;
}

namespace AfyKernel
{
/**
 * PIN reference data element
 */
struct RefData
{
	StackAlloc	*const	sa;
	const		bool	fSorted;
	ushort				*cbuf;
	unsigned			lcbuf;
	ushort				nkeys;
	ushort				left;
	void				*tbuf;
	unsigned			nlevels;
	ushort				ntkeys[6];
	RefData(StackAlloc *s,bool fS) : sa(s),fSorted(fS),cbuf(NULL),lcbuf(0),nkeys(0),left(0),tbuf(NULL),nlevels(0) {}
	RC	add(const byte *ext) {
		const ushort lext=PINRef::len(ext);
		if (lext+sizeof(ushort)>left) {
			if (cbuf==NULL) {
				left=lcbuf=64; if ((cbuf=(ushort*)sa->malloc(64))==NULL) return RC_NORESOURCES;
			} else if (lcbuf<0x10000) {
				unsigned nlcbuf=lcbuf==64?0x1000:0x10000,ldata=lcbuf-left-nkeys*sizeof(ushort);
				unsigned le=nlcbuf==0x10000?sizeof(ushort)*2:0,sht=nlcbuf-lcbuf-le;
				if ((cbuf=(ushort*)sa->realloc(cbuf,nlcbuf,lcbuf))==NULL) return RC_NORESOURCES;
				memmove((byte*)cbuf+nlcbuf-ldata-le,(byte*)cbuf+lcbuf-ldata,ldata); left+=sht; lcbuf=nlcbuf;
				if (le!=0) {cbuf[0x10000/sizeof(ushort)-1]=nkeys; cbuf[0x10000/sizeof(ushort)-2]=left;}
				for (unsigned i=0; i<nkeys; i++) cbuf[i]+=sht;
			} else {
				if (!fSorted) QSort<RefData>::sort(*this,nkeys);
				if (nlevels==0) {
					assert(tbuf==NULL);
					if ((tbuf=sa->malloc(0x10000))==NULL) return RC_NORESOURCES;
					((void**)tbuf)[0]=cbuf; ntkeys[0]=1; nlevels=1;
				}
				lcbuf=0x10000; left=0x10000-sizeof(ushort)*2; nkeys=0;
				if ((cbuf=(ushort*)sa->malloc(0x10000))==NULL) return RC_NORESOURCES;
				void *stack[6]; unsigned i=0;
				for (void *tb=tbuf; i<nlevels; i++) {stack[i]=tb; tb=((void**)tb)[ntkeys[i]-1];}
				for (void *buf=cbuf,*pb;;) {
					if (i==0) {
						if (nlevels>=6 || (pb=sa->malloc(0x10000))==NULL) return RC_NORESOURCES;
						((void**)pb)[0]=tbuf; ((void**)pb)[1]=buf; ntkeys[0]=2; tbuf=pb; ntkeys[nlevels++]=1;
						if (!fSorted && ntkeys[0]>1 && cmp(((void**)pb)[0],((void**)pb)[1],0)>0) {((void**)pb)[1]=((void**)pb)[0]; ((void**)pb)[0]=buf;}
						break;
					} else if (ntkeys[--i]<0x10000/sizeof(void*)) {
						((void**)stack[i])[ntkeys[i]++]=buf;
						if (!fSorted) {
							// heap up
						}
						break;
					} else {
						if ((pb=sa->malloc(0x10000))==NULL) return RC_NORESOURCES;
						((void**)pb)[0]=buf; buf=pb; ntkeys[i]=1;
					}
				}
			}
		}
		ushort sht=nkeys*sizeof(ushort)+left-lext; cbuf[nkeys++]=sht; memcpy((byte*)cbuf+sht,ext,lext); left-=lext+sizeof(ushort);
		if (lcbuf==0x10000) {cbuf[0x10000/sizeof(ushort)-1]=nkeys; cbuf[0x10000/sizeof(ushort)-2]=left;}
		return RC_OK;
	}
	ushort compact(ushort& nk) {
		assert(nlevels==0); ushort le=lcbuf==0x10000?sizeof(ushort)*2:0;
		if (left!=0) {memmove(cbuf+nkeys,(byte*)(cbuf+nkeys)+left,lcbuf-nkeys*sizeof(ushort)-left-le); for (unsigned i=0; i<nkeys; i++) cbuf[i]-=left;}
		nk=nkeys; return lcbuf-left-le;
	}
	ushort compact(ushort *buf,ushort& nk) {
		nk=buf[0x10000/sizeof(ushort)-1]; ushort left=buf[0x10000/sizeof(ushort)-2]; assert(nlevels!=0);
		if (left!=0) {memmove(buf+nk,(byte*)(buf+nk)+left,0x10000-left-(nk+2)*sizeof(ushort)); for (unsigned i=0; i<nk; i++) buf[i]-=left;}
		return 0x10000-sizeof(ushort)*2-left;
	}
	int cmp(const void *t1,const void *t2,unsigned lvl) const {
		assert(lvl<nlevels);
		for (;lvl+1<nlevels;lvl++) {t1=((void**)t1)[0]; t2=((void**)t2)[0];}
		return PINRef::cmpPIDs((byte*)t1+((ushort*)t1)[0],(byte*)t2+((ushort*)t2)[0]);
	}
	int	cmp(unsigned i,unsigned j) const {assert(cbuf!=NULL&&i<nkeys&&j<nkeys); return PINRef::cmpPIDs((byte*)cbuf+cbuf[i],(byte*)cbuf+cbuf[j]);}
	void swap(unsigned i,unsigned j) {assert(cbuf!=NULL&&i<nkeys&&j<nkeys); ushort tmp=cbuf[i]; cbuf[i]=cbuf[j]; cbuf[j]=tmp;}
	class MultiKey : public IMultiKey {
	protected:
		RefData			*rd;
		ushort			*cbuf;
		ushort			lbuf;
		ushort			nkeys;
		ushort			idx[6];
		ushort			end[6];
		bool			fAdv;
	public:
		MultiKey() : rd(NULL),cbuf(NULL),nkeys(0),fAdv(false) {}
		bool setRefData(RefData *r,bool fAppend=false) {
			if ((rd=r)->nlevels==0) {cbuf=r->cbuf; lbuf=r->compact(nkeys); fAdv=false;}
			else {
				idx[0]=idx[1]=idx[2]=idx[3]=idx[4]=idx[5]=0; end[0]=r->ntkeys[0];
				end[1]=end[2]=end[3]=end[4]=end[5]=uint16_t(0x10000/sizeof(void*));
				fAdv=true; if (fAppend) return false;
			}
			return true;
		}
		virtual RC nextKey(const SearchKey *&nk,const void *&value,ushort& lValue,unsigned& multi,bool fAppend) {
			if (!fAdv) {assert(cbuf!=NULL); fAdv=true;}
			else if (rd==NULL||rd->nlevels==0) return RC_EOF;
			else {
				cbuf=(ushort*)rd->tbuf; assert(cbuf!=NULL);
				for (unsigned i=0,last=0;;) if (idx[i]<end[i]) {
					cbuf=(ushort*)((void**)cbuf)[idx[i]];
					if (last+1==i && idx[i]+1>=end[i]) last++;
					if (i+1<rd->nlevels) i++; else {idx[i]++; break;}
				} else if (last<i || i+1<rd->nlevels) {
					idx[i]=0; --i; if (++idx[i]+1>=end[i] && last+1==i) end[i+1]=rd->ntkeys[i+1];
				} else return RC_EOF;
				lbuf=rd->compact(cbuf,nkeys);
			}
			value=cbuf; lValue=lbuf; multi=nkeys; return RC_OK;
		}
		void push_back() {fAdv=false;}
	};
};

/**
 * class or family index container - common part
 */
struct IndexData
{
	Session	*const	ses;
	const unsigned	nSegs;
	const ClassID	cid;
	const	Stmt	*qry;
	unsigned		flags;
	bool			fSkip;
public:
	IndexData(Session *s,ClassID id,const Stmt *q,unsigned flg,unsigned nS) : ses(s),nSegs(nS),cid(id),qry(q),flags(flg),fSkip(true) {}
	virtual	RC initClassCreate(ClassCreate& cc,PIN *pin) {
		if ((cc.query=qry->clone(STMT_OP_ALL,ses))==NULL) return RC_NORESOURCES; RC rc;
		if ((rc=ses->getStore()->classMgr->createActs(pin,cc.acts))!=RC_OK) return rc;
		const Value *cv=pin->findProperty(PROP_SPEC_WINDOW);
		if (cv!=NULL && (cc.wnd=new(ses) ClassWindow(*cv,cc.query))==NULL) return RC_NORESOURCES;
		return RC_OK;
	}
	virtual	RC		insert(const byte *ext,SearchKey *key=NULL) = 0;
	virtual	RC		flush(Tree& tr,bool fFinal=false) = 0;
};


/**
 * class index container
 */
struct ClassData : public IndexData, public SubTreeInit
{
	RefData		refs;
	const bool	fSorted;
	bool		fFlushed;
	ClassData(Session *ses,StackAlloc *sa,ClassID id,const Stmt *q,unsigned flg,bool fS) : IndexData(ses,id,q,flg,0),SubTreeInit(sa),refs(sa,false),fSorted(fS),fFlushed(false) {}
	RC	insert(const byte *ext,SearchKey *key=NULL) {return fSorted && !fFlushed && !PINRef::isMoved(ext)?SubTreeInit::insert(ses,ext):refs.add(ext);}
	RC	flush(Tree& tr,bool fFinal) {
		RC rc=RC_OK;
		if (!fFlushed && (nKeys!=0 || depth!=0) && (fFinal || refs.cbuf!=NULL))
			{fFlushed=true; rc=tr.insert(SearchKey((uint64_t)cid),*this,ses);}
		if (rc==RC_OK && refs.cbuf!=NULL) {
			fFlushed=true;
			if (refs.tbuf==NULL) {
				if (refs.nkeys>1) QSort<RefData>::sort(refs,refs.nkeys);
				ushort nk,l=refs.compact(nk); rc=tr.insert(SearchKey((uint64_t)cid),refs.cbuf,l,nk);
			} else {
				//???
			}
		}
		return rc;
	}
};

/**
 * family value reference
 */
struct IndexValue
{
	IndexKeyV	key;
	RefData		refs;
	IndexValue(const IndexKeyV& k,StackAlloc *sa,bool fS) : key(k),refs(sa,fS) {}
	IndexValue() : refs(NULL,false) {}		// for SList:node0
	static SListOp compare(const IndexValue& left,IndexValue& right,unsigned ity,MemAlloc&) {
		int cmp=left.key.cmp(right.key,(TREE_KT)ity); return cmp<0?SLO_LT:cmp>0?SLO_GT:SLO_NOOP;
	}
};

/**
 * ordered SList of index values with reference data
 */
typedef SList<IndexValue,IndexValue> IndexList;

/**
 * family index container
 */
struct FamilyData : public IndexData, public TreeStdRoot
{
	StackAlloc	*sa;
	IndexList	*il;
	uint32_t	ity;
	PageID		anchor;
	IndexFormat	fmt;
	const bool	fSorted;
	bool		fFlushed;
	IndexSeg	indexSegs[1];
	FamilyData(Session *s,ClassID id,const Stmt *q,unsigned flg,const CondIdx *ci,unsigned nS,StackAlloc *ma,bool fS) 
		: IndexData(s,id,q,flg,nS),TreeStdRoot(INVALID_PAGEID,ses->getStore(),TF_SPLITINTX|TF_NOPOST),sa(ma),il(NULL),ity(KT_VAR),anchor(INVALID_PAGEID),fmt(KT_VAR,KT_VARKEY,KT_PINREFS),fSorted(fS),fFlushed(false) {
		for (unsigned i=0; i<nS; i++,ci=ci->next) {assert(ci!=NULL); indexSegs[i]=ci->ks;}
		if (nS==1) {
			s->getStore()->classMgr->indexFormat(indexSegs[0].type==VT_ANY&&(indexSegs[0].lPrefix!=0||(indexSegs[0].flags&ORD_NCASE)!=0)?VT_STRING:indexSegs[0].type,fmt);
			ity=fmt.keyType();
		}
	}
	void *operator new(size_t s,unsigned nSegs,MemAlloc *ma) {return ma->malloc(s+int(nSegs-1)*sizeof(IndexSeg));}
	RC initClassCreate(ClassCreate& cc,PIN *pin) {cc.fmt=fmt; cc.anchor=anchor; cc.root=root; cc.height=height; memcpy(cc.indexSegs,indexSegs,nSegs*sizeof(IndexSeg)); return IndexData::initClassCreate(cc,pin);}
	PageID startPage(const SearchKey *key,int& level,bool fRead,bool fBefore) {bool f=root==INVALID_PAGEID; PageID pid=TreeStdRoot::startPage(key,level,fRead,fBefore); if (f) anchor=pid; return pid;}
	TreeFactory		*getFactory() const {return NULL;}
	IndexFormat		indexFormat() const {return fmt;}
	bool			lock(RW_LockType,bool fTry=false) const {return true;}
	void			unlock() const {}
	void			destroy() {}
	RC insert(const byte *ext,SearchKey *key) {
		if (key==NULL) return RC_INTERNAL;
		if (il==NULL && (il=new(sa) IndexList(*sa,ity))==NULL) return RC_NORESOURCES;
		IndexValue v(key->v,sa,fSorted),*pi=NULL;
		if (il->add(v,&pi)==SLO_ERROR) return RC_NORESOURCES;
		return pi->refs.add(ext);
	}
	RC flush(Tree& tr,bool fFinal) {
		if (il!=NULL) {
			MultiKey imk(*this); RC rc;
			if ((rc=Tree::insert(imk))!=RC_OK) return rc;
			il=NULL; fFlushed=true;
		}
		return RC_OK;
	}
	class MultiKey : public RefData::MultiKey {
		FamilyData&		ci;
		SearchKey		key;
		IndexValue		*iv;
		bool			fSub;
	public:
		MultiKey(FamilyData& c) : ci(c),iv(NULL),fSub(false) {if (c.il!=NULL) {c.il->start(); fAdv=true;}}
		RC	nextKey(const SearchKey *&nk,const void *&value,ushort& lValue,unsigned& multi,bool fAppend) {
			RC rc=RC_OK; assert(ci.il!=NULL); // fSub? !fSorted?
			for (;;iv=NULL) {
				if (!fAdv) {if (iv==NULL) return RC_EOF;}
				else if (iv==NULL || iv->refs.nlevels==0) {
					do if ((iv=(IndexValue*)ci.il->next())==NULL) {fAdv=false; return RC_EOF;} while (iv->refs.cbuf==NULL);
					new(&key) SearchKey(iv->key,(TREE_KT)ci.ity,SearchKey::PLC_EMB);	// SPTR?
					if (!setRefData(&iv->refs)) return RC_EOF;
				}
				if ((rc=RefData::MultiKey::nextKey(nk,value,lValue,multi,fAppend))!=RC_EOF) {nk=&key; return rc;}
			}
		}
	};
};

struct ClassCtx
{
	Session			*ses;
	IndexData		**cid;
	unsigned		nClasses;
	StackAlloc		*sa;
	StackAlloc::SubMark& mrk;
	unsigned		idx;
	size_t			limit;
	RC	insert(IndexData& id,const byte *ext,SearchKey *key=NULL) {
		if (sa->getTotal()>limit) {
			RC rc; void *skey=NULL;
			if (key!=NULL && key->type>=KT_BIN && key->v.ptr.p!=NULL) {
				if ((skey=alloca(key->v.ptr.l))==NULL) return RC_NORESOURCES;
				memcpy(skey,key->v.ptr.p,key->v.ptr.l);
			}
			for (unsigned i=0; i<nClasses; i++) 
				if (cid[i]!=NULL && !cid[i]->fSkip && (cid[i]->flags&META_PROP_INMEM)==0 && 
					(rc=cid[i]->flush(ses->getStore()->classMgr->getClassMap()))!=RC_OK) return rc;
			sa->truncate(TR_REL_ALLBUTONE,&mrk);
			if (skey!=NULL) {
				if ((key->v.ptr.p=sa->malloc(key->v.ptr.l))==NULL) return RC_NORESOURCES;
				memcpy((void*)key->v.ptr.p,skey,key->v.ptr.l);
			}
		}
		return id.insert(ext,key);
	}
};
}

RC Classifier::classifyAll(PIN *const *pins,unsigned nPINs,Session *ses,bool fDrop)
{
	if (pins==NULL || nPINs==0) return RC_INVPARAM; if (ses==NULL) return RC_NOSESSION; 
	assert(ctx->namedMgr->fInit && ses->inWriteTx());

	IndexData **cid; StackAlloc sa(ses);
	if ((cid=new(&sa) IndexData*[nPINs])!=NULL) memset(cid,0,nPINs*sizeof(IndexData*)); else return RC_NORESOURCES;
	Value *indexed=NULL; unsigned nIndexed=0,xIndexed=0,xSegs=0; unsigned nIndex=0; RC rc=RC_OK;
	for (unsigned i=0; i<nPINs; i++) {
		PIN *pin=pins[i]; if ((pin->meta&PMT_CLASS)==0) continue;
		CondIdx *pci=NULL; unsigned nSegs=0; bool fSorted=true;
		const Value *cv=pin->findProperty(PROP_SPEC_OBJID); assert(cv!=NULL && cv->type==VT_URIID); const ClassID id=cv->uid; 
		cv=pin->findProperty(PROP_SPEC_PREDICATE); assert(cv!=NULL && cv->type==VT_STMT); Stmt *qry=(Stmt*)cv->stmt; uint8_t flags=cv->meta;
		if (qry==NULL || (pin->meta&(PMT_TIMER|PMT_LISTENER))==0 && (qry->op!=STMT_QUERY || qry->top==NULL) || (qry->mode&QRY_CPARAMS)!=0) rc=RC_INVPARAM;
		else if (qry->top->type==QRY_SIMPLE) for (unsigned i=0; i<((SimpleVar*)qry->top)->nClasses; i++) if (((SimpleVar*)qry->top)->classes[i].objectID==id) {rc=RC_INVPARAM; break;}
		if (rc!=RC_OK) break;
		if (qry->top->type==QRY_SIMPLE) {pci=((SimpleVar*)qry->top)->condIdx; nSegs=(ushort)((SimpleVar*)qry->top)->nCondIdx;}
		if (pin->findProperty(PROP_SPEC_ACL)!=NULL) flags|=META_PROP_ACL;
		cid[i]=nSegs!=0?(IndexData*)new(nSegs,&sa) FamilyData(ses,id,qry,flags,pci,nSegs,&sa,fSorted):
						(IndexData*)new(&sa) ClassData(ses,&sa,id,qry,flags,fSorted);
		if (cid[i]==NULL) {rc=RC_NORESOURCES; break;}
		if ((flags&META_PROP_INDEXED)!=0 && (pci!=NULL || Stmt::classOK(qry->top)) && qry->top->type==QRY_SIMPLE
													&& ((SimpleVar*)qry->top)->checkXPropID((PropertyID)ctx->namedMgr->xPropID)) {
			cid[i]->fSkip=false; nIndex++;
			if (pci!=NULL) {
				if (nSegs>xSegs) xSegs=nSegs;
				for (unsigned j=0; j<nSegs; j++) {
					Value v; v.setPropID(((FamilyData*)cid[i])->indexSegs[j].propID);
					if ((rc=VBIN::insert(indexed,nIndexed,v.property,v,(MemAlloc*)ses,&xIndexed))!=RC_OK) break;
				}
			}
		}
		if (rc==RC_OK && fDrop) {
			// get old class here!
			//if (ci.cd->cidx!=NULL) {
			//	if ((rc=ci.cd->cidx->drop())==RC_OK) rc=ci.cd->cidx->cls.update();		// ???
			//} else 
			if ((flags&META_PROP_INDEXED)!=0) {
				SearchKey key((uint64_t)id); if ((rc=classMap.remove(key,NULL,0))==RC_NOTFOUND) rc=RC_OK;
			}
			if (rc!=RC_OK) break;
		}
	}

#ifdef REPORT_INDEX_CREATION_TIMES
	TIMESTAMP st,mid,end; getTimestamp(st);
#endif

	StackAlloc::SubMark mrk; sa.mark(mrk); unsigned nS=StoreCtx::getNStores();
	ClassCtx cctx={ses,cid,nPINs,&sa,mrk,0,CLASS_BUF_LIMIT/max(nS,1u)};

	if (rc==RC_OK && nIndex!=0) {
		MutexP lck(&lock); bool fTest=false; PINx qr(ses),*pqr=&qr; QueryOp *qop=NULL; ses->resetAbortQ(); QCtx qc(ses);
		if (nIndex==1 && cid[0]->qry!=NULL && !fDrop) {
			QBuildCtx qctx(ses,NULL,cid[0]->qry,0,MODE_CLASS|MODE_NODEL);
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
			if (qr.epr.buf[0]!=0) PINRef::changeFColl(qr.epr.buf,false);
			for (cctx.idx=0; cctx.idx<nPINs; cctx.idx++) {
				IndexData &ci=*cid[cctx.idx]; if (ci.fSkip) continue;
				if (fTest && (qr.hpin->hdr.descr&HOH_DELETED)!=0) continue;
				if (pc.id!=pins[cctx.idx]->id) {pc.cleanup(); pc.id=pins[cctx.idx]->id;}
				if (!fTest || ci.qry->checkConditions(ctx2,0,true)) {
					if (qr.epr.buf[0]==0 && qr.pack()!=RC_OK) continue;
					if (ci.nSegs==0) rc=cctx.insert(ci,qr.epr.buf,NULL);
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
							FamilyData &fi=(FamilyData&)ci;
							const unsigned nSegs=fi.nSegs; ArrayVal *avs0=NULL,*avs=NULL; assert(nSegs<=xSegs);
							for (unsigned k=nNulls=0; k<nSegs; k++) {
								IndexSeg& ks=fi.indexSegs[k];
								const Value *cv=vals[k]=VBIN::find(ks.propID,indexed,nIndexed); assert(cv!=NULL);
								if (cv->type==VT_ANY) nNulls++;
								else if (cv->type==VT_COLLECTION) {
									ArrayVal *av=freeAV;
									if (av!=NULL) freeAV=av->prev;
									else if ((av=(ArrayVal*)ses->malloc(sizeof(ArrayVal)))==NULL) {rc=RC_NORESOURCES; break;}
									av->prev=avs; avs=av; av->idx=0; av->cv=cv; av->vidx=k; if (avs0==NULL) avs0=av;
									vals[k]=!cv->isNav()?cv->varray:cv->nav->navigate(GO_FIRST);
									if (!fExtC) {memcpy(extc,qr.epr.buf,PINRef::len(qr.epr.buf)); lextc=PINRef::changeFColl(extc,true); fExtC=true;}
								}
							}
							if (nNulls<nSegs) for (;;) {		// || derived!
								SearchKey key;
								if ((rc=key.toKey(vals,nSegs,fi.indexSegs,-1,ses,&sa))==RC_TYPE||rc==RC_SYNTAX) rc=RC_OK;
								else if (rc==RC_OK) {if ((rc=cctx.insert(fi,avs!=NULL?extc:qr.epr.buf,&key))!=RC_OK) break;}
								else break;
								bool fNext=false;
								for (ArrayVal *av=avs; !fNext && av!=NULL; av=av->prev) {
									const Value *cv; assert(av->cv->type==VT_COLLECTION);
									if (!av->cv->isNav()) {
										if (++av->idx>=av->cv->length) av->idx=0; else fNext=true;
										cv=&av->cv->varray[av->idx];
									} else {
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
	if (rc==RC_OK) for (unsigned i=0; i<nPINs; i++) if (cid[i]!=NULL) {
		IndexData &ci=*cid[i]; if (!ci.fSkip && (ci.flags&META_PROP_INMEM)==0 && (rc=ci.flush(classMap,true))!=RC_OK) break;
		ClassCreate *cc=new(ci.nSegs,(MemAlloc*)ses) ClassCreate(ci.cid,pins[i]->id,pins[i]->addr,(ushort)ci.nSegs,ci.flags);
		if (cc==NULL) {rc=RC_NORESOURCES; break;} 
		if ((rc=ci.initClassCreate(*cc,pins[i]))!=RC_OK || (rc=ses->addOnCommit(cc))!=RC_OK) {cc->destroy(ses); break;}
	}

#ifdef REPORT_INDEX_CREATION_TIMES
	getTimestamp(end); report(MSG_DEBUG,"Index creation time: " _LD_FM ", " _LD_FM "\n",mid-st,end-mid);
#endif

	return rc;
}

Class *Classifier::getClass(ClassID cid,RW_LockType lt)
{
	Class *cls=NULL; 
	if (cid!=STORE_INVALID_URIID && get(cls,cid,0,lt)==RC_NOTFOUND) {
		//Is this necessary?
		Session *ses=Session::getSession(); ClassCreate *cd;
		if (ses!=NULL) for (const SubTx *st=&ses->tx; st!=NULL; st=st->next)
			for (OnCommit *oc=st->onCommit.head; oc!=NULL; oc=oc->next) if ((cd=oc->getClass())!=NULL && cd->cid==cid) {
				if ((cls=new(ses) Class(cid,*this,ses))!=NULL && cd->copy(cls,true)!=RC_OK) {cls->destroy(); cls=NULL;}
			}
	}
	return cls;
}

RC ClassCreate::process(Session *ses)
{
	Class *cls=NULL; RC rc=RC_OK; StoreCtx *ctx=ses->getStore(); Classifier *mgr=ctx->classMgr;
	if ((rc=mgr->get(cls,cid,0,QMGR_NEW|RW_X_LOCK))==RC_OK) rc=copy(cls,true);
	else if (rc==RC_ALREADYEXISTS && (rc=mgr->get(cls,cid,0,RW_X_LOCK))==RC_OK && cls!=NULL) rc=copy(cls,false);
	if (cls!=NULL) {if (rc==RC_OK) rc=cls->update(); mgr->release(cls);}
	if (rc==RC_OK && (rc=mgr->add(ses,*this,query))==RC_OK) acts=NULL;
	return rc;
}

RC ClassCreate::copy(Class *cls,bool fFull) const
{
	RC rc=RC_OK; assert(cls!=NULL);
	if (fFull) {
		cls->id=pid; cls->addr=addr; cls->flags=flags; cls->meta=PMT_CLASS;
		if (cls->txs!=NULL) cls->query=(Stmt*)query; else if ((cls->query=query->clone(STMT_OP_ALL,cls->mgr.ctx))==NULL) return RC_NORESOURCES;
		if (nIndexProps!=0) {
			ClassIndex *ci=cls->index=new(nIndexProps,cls->mgr.ctx) ClassIndex(*cls,nIndexProps,root,anchor,fmt,height,cls->mgr.ctx);
			if (ci!=NULL) memcpy(ci->indexSegs,indexSegs,nIndexProps*sizeof(IndexSeg)); else rc=RC_NORESOURCES;
		}
	} else if (cls->index!=NULL) {cls->index->root=root; cls->index->anchor=anchor; cls->index->height=height; cls->index->fmt=fmt;}
	return rc;
}

ClassCreate *ClassCreate::getClass()
{
	return this;
}

void ClassCreate::destroy(Session *ses)
{
	if (acts!=NULL) ses->getStore()->classMgr->destroyActs(acts);
	if (wnd!=NULL) {wnd->~ClassWindow(); ses->free(wnd);}
	if (query!=NULL) ((Stmt*)query)->destroy();
	ses->free(this);
}

RC ClassCreate::loadClass(PINx &cb,bool fSafe)
{
	Value v[3]; RC rc; Session *ses=cb.getSes(); Classifier *classMgr=ses->getStore()->classMgr;
	if ((rc=cb.getV(PROP_SPEC_OBJID,v[0],0,NULL))!=RC_OK) return rc; assert((cb.getMetaType()&PMT_CLASS)!=0);
	if (v[0].type!=VT_URIID || v[0].uid==STORE_INVALID_URIID) {freeV(v[0]); return RC_CORRUPTED;}
	if ((rc=cb.getV(PROP_SPEC_PREDICATE,v[1],LOAD_SSV,ses))!=RC_OK) return rc;
	if (v[1].type!=VT_STMT || v[1].stmt==NULL) return RC_CORRUPTED;
	byte cwbuf[sizeof(ClassWindow)]; ClassWindow *cw=NULL;
	if ((rc=cb.getV(PROP_SPEC_WINDOW,v[2],LOAD_SSV,ses))==RC_NOTFOUND) rc=RC_OK;
	else if (rc==RC_OK) {cw=new(cwbuf) ClassWindow(v[2],(Stmt*)v[1].stmt); freeV(v[2]);}
	else return rc;
	ClassRef cr(v[0].uid,cb.getPID(),0,0,v[1].meta,NULL,cw);
	if (((Stmt*)v[1].stmt)->getTop()!=NULL && (fSafe || (rc=classMgr->createActs(&cb,cr.acts))==RC_OK) &&
		(rc=classMgr->add(ses,cr,(Stmt*)v[1].stmt))!=RC_OK && !fSafe) classMgr->destroyActs(cr.acts);
	freeV(v[1]);
	return rc;
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
	Class *cls=NULL; ClassIndex *cidx; byte ext[XPINREFSIZE],ext2[XPINREFSIZE]; pin->fReload=1;
	PINRef pr(ctx->storeID,pin->id,pin->addr); if ((pin->meta&PMT_COMM)!=0) pr.def|=PR_SPECIAL; if ((pin->mode&PIN_HIDDEN)!=0) pr.def|=PR_HIDDEN;
	byte lext=pr.enc(ext),lext2=0; const Value *psegs[10],**pps=psegs; unsigned xSegs=10;
	struct SegInfo {PropertyID pid; const PropInfo *pi; SubSetV ssv; ModInfo *mi; Value v; const Value *cv; uint32_t flags,idx,prev; bool fLoaded;} keysegs[10],*pks=keysegs;
	for (unsigned i=0; rc==RC_OK && i<clr.nClasses; lext=PINRef::changeFColl(ext,false),i++) {
		const ClassRef *cr=clr.classes[i]; if ((cr->flags&META_PROP_INDEXED)==0) continue;
		if (cr->nIndexProps==0) {
			SearchKey key((uint64_t)cr->cid);
			switch (op) {
			default: rc=RC_INVPARAM; break;
			case CI_PURGE: break;
			case CI_UDELETE: case CI_INSERT: 
				if (cr->wnd!=NULL) {
					TIMESTAMP ts; const Value *pv;
					switch (cr->wnd->type) {
					case CWT_COUNT: rc=classMap.truncate(key,cr->wnd->range,true); break;
					case CWT_INTERVAL:
						if ((pv=pin->findProperty(cr->wnd->propID))==NULL || pv->type!=VT_DATETIME) continue;
						ts=pv->ui64; goto itv_window;
					case CWT_REL_INTERVAL: 
						getTimestamp(ts);
					itv_window:
						if (ts<cr->wnd->range || (rc=classMap.truncate(key,ts-cr->wnd->range))==RC_OK) {
							// add prefix
						}
						break;
					}
				}
				if (rc==RC_OK) rc=classMap.insert(key,ext,lext); break;
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
						if ((si.pi->flags&(PM_COLLECTION|PM_NEWCOLL))!=0) lext=PINRef::changeFColl(ext,true);
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
						if (si.fLoaded) freeV(si.v); lext=PINRef::changeFColl(ext,true); if (lext2==0) lext2=pr.enc(ext2);
						if ((rc=pin->getV(si.pid,si.v,LOAD_SSV,ses,si.pi->single))!=RC_OK) break;
					}
					pps[k]=si.cv;
					if (si.cv->type==VT_ANY) {
						if ((si.flags&(ORD_NULLS_BEFORE|ORD_NULLS_AFTER))==0) {rc=RC_NOTFOUND; break;} else {rc=RC_OK; nNulls++;}
					} else if (si.cv->type==VT_COLLECTION) {
						fIt=true; pps[k]=!si.cv->isNav()?si.cv->varray:si.cv->nav->navigate(GO_FIRST); lext=PINRef::changeFColl(ext,true);
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
						if (si.cv->type==VT_COLLECTION) {
							if (!si.cv->isNav()) {
								if (++si.idx>=si.cv->length) si.idx=0; else fNext=true;
								cv=&si.cv->varray[si.idx];
							} else {
								if ((cv=si.cv->nav->navigate(GO_NEXT))!=NULL) fNext=true;
								else if (si.prev!=~0u) {cv=si.cv->nav->navigate(GO_FIRST); assert(cv!=NULL);}
							}
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
							if (cv!=NULL && cv->type==VT_COLLECTION) cv=cv->isNav()?cv->nav->navigate(GO_FIRST):cv->varray;
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
	unsigned n=pv->type==VT_COLLECTION?pv->length:1; if (n==~0u) return RC_INVPARAM;
	if ((acts->acts[idx].acts=new(ctx) Stmt*[n])==NULL) {destroyActs(acts); acts=NULL; return RC_NORESOURCES;}
	if (pv->type==VT_STMT) acts->acts[idx].acts[0]=((Stmt*)pv->stmt)->clone(STMT_OP_ALL,ctx);		// null?
	else if (pv->type==VT_COLLECTION) {
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
			rc=PINRef::getPID(p,ses->getStore()->storeID,id);
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
