/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "classifier.h"
#include "queryprc.h"
#include "stmt.h"
#include "expr.h"
#include "txmgr.h"
#include "maps.h"
#include "blob.h"

using namespace MVStoreKernel;

static const IndexFormat classIndexFmt(KT_UINT,sizeof(uint64_t),KT_VARMDPINREFS);
static const IndexFormat classPINsFmt(KT_UINT,sizeof(uint64_t),KT_VARDATA);

Classifier::Classifier(StoreCtx *ct,ulong timeout,ulong hashSize,ulong cacheSize) 
	: ClassHash(*new(ct) ClassHash::QueueCtrl<STORE_HEAP>(cacheSize),hashSize),ctx(ct),fInit(false),classIndex(ct),
	classMap(MA_CLASSINDEX,classIndexFmt,ct,TF_WITHDEL),classPINs(MA_CLASSPINS,classPINsFmt,ct,TF_WITHDEL),
	nCached(0),xCached(cacheSize),xPropID(ct->theCB->xPropID)
{
	if (&ctrl==NULL) throw RC_NORESOURCES;
	ct->treeMgr->registerFactory(*this);
}

const BuiltinURI Classifier::builtinURIs[PROP_SPEC_MAX+1] = {
	{S_L("ClassOfClasses"),		STORE_CLASS_OF_CLASSES},
	{S_L("pinID"),				PROP_SPEC_PINID},
	{S_L("document"),			PROP_SPEC_DOCUMENT},
	{S_L("parent"),				PROP_SPEC_PARENT},
	{S_L("value"),				PROP_SPEC_VALUE},
	{S_L("created"),			PROP_SPEC_CREATED},
	{S_L("createdBy"),			PROP_SPEC_CREATEDBY},
	{S_L("updated"),			PROP_SPEC_UPDATED},
	{S_L("updatedBy"),			PROP_SPEC_UPDATEDBY},
	{S_L("ACL"),				PROP_SPEC_ACL},
	{S_L("URI"),				PROP_SPEC_URI},
	{S_L("stamp"),				PROP_SPEC_STAMP},
	{S_L("classID"),			PROP_SPEC_CLASSID},
	{S_L("predicate"),			PROP_SPEC_PREDICATE},
	{S_L("nInstances"),			PROP_SPEC_NINSTANCES},
	{S_L("nDelInstances"),		PROP_SPEC_NDINSTANCES},
	{S_L("subclasses"),			PROP_SPEC_SUBCLASSES},
	{S_L("superclasses"),		PROP_SPEC_SUPERCLASSES},
	{S_L("classInfo"),			PROP_SPEC_CLASS_INFO},
	{S_L("indexInfo"),			PROP_SPEC_INDEX_INFO},
	{S_L("properties"),			PROP_SPEC_PROPERTIES},
	{S_L("joinTrigger"),		PROP_SPEC_JOIN_TRIGGER},
	{S_L("updateTrigger"),		PROP_SPEC_UPDATE_TRIGGER},
	{S_L("leaveTrigger"),		PROP_SPEC_LEAVE_TRIGGER},
	{S_L("refId"),				PROP_SPEC_REFID},
	{S_L("key"),				PROP_SPEC_KEY},
	{S_L("version"),			PROP_SPEC_VERSION},
	{S_L("weight"),				PROP_SPEC_WEIGHT},
	{S_L("prototype"),			PROP_SPEC_PROTOTYPE},
	{S_L("window"),				PROP_SPEC_WINDOW},
};

RC Classifier::initStoreMaps(Session *ses)
{
	TxSP tx(ses); RC rc=tx.start(); if (rc!=RC_OK) return rc;
	static char namebuf[sizeof(STORE_STD_URI_PREFIX)+40] = {STORE_STD_URI_PREFIX};
	ulong i; xPropID=PROP_SPEC_LAST+1;
	for (i=0; i<sizeof(builtinURIs)/sizeof(builtinURIs[0]); i++) {
		if (builtinURIs[i].name==NULL) return RC_INTERNAL;
		memcpy(namebuf+sizeof(STORE_STD_URI_PREFIX)-1,builtinURIs[i].name,builtinURIs[i].lname+1);
		URI *uri=(URI*)ctx->uriMgr->insert(namebuf); URIID uid=uri->getID(); uri->release();
		if (uid!=builtinURIs[i].uid) return RC_INTERNAL;
	}
	for (; i<=PROP_SPEC_LAST; i++) {
		sprintf(namebuf+sizeof(STORE_STD_URI_PREFIX)-1,"reserved%u",(unsigned)i);
		URI *uri=(URI*)ctx->uriMgr->insert(namebuf);
		URIID uid=uri->getID(); uri->release();
		if (uid!=i) return RC_INTERNAL;
	}
	Stmt *qry=new(ses) Stmt(0,ses); if (qry==NULL) return RC_NORESOURCES;
	byte var=qry->addVariable(); if (var==0xFF) {qry->destroy(); return RC_NORESOURCES;}
	static const PropertyID classProps[]={PROP_SPEC_CLASSID,PROP_SPEC_PREDICATE};
	qry->setPropCondition(var,classProps,sizeof(classProps)/sizeof(classProps[0]));
	Value vals[2]; fInit=true;
	vals[0].setURIID(STORE_CLASS_OF_CLASSES); vals[0].setPropID(PROP_SPEC_CLASSID);
	vals[1].set(qry); vals[1].setPropID(PROP_SPEC_PREDICATE);
	PIN pin(ses,PIN::defPID,PageAddr::invAddr,PIN_NO_FREE,vals,sizeof(vals)/sizeof(vals[0])),*pp=&pin;
	if ((rc=ctx->queryMgr->commitPINs(ses,&pp,1,0,ValueV(NULL,0)))==RC_OK) tx.ok();
	qry->destroy(); return rc;
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
#if 0
			FullScan fs(ses,HOH_HIDDEN,0,true); PINEx cb(ses); Value v;
			while ((rc=fs.next(cb))==RC_OK) {
				if (cb.hpin==NULL || (cb.hpin->hdr.descr&HOH_CLASS)==0) {rc=RC_CORRUPTED; break;}
#else
			Class *coc=getClass(STORE_CLASS_OF_CLASSES); if (coc==NULL) return RC_CORRUPTED;
			QCtx qc(ses); qc.ref(); ClassScan cs(&qc,coc,0); coc->release(); PINEx cb(ses),*pcb=&cb; cs.connect(&pcb); Value v;
			while ((rc=cs.next())==RC_OK && (rc=ctx->queryMgr->getBody(cb))==RC_OK) {
#endif
				ulong flags=cb.getValue(PROP_SPEC_CLASS_INFO,v,0,NULL)==RC_OK && (v.type==VT_UINT||v.type==VT_INT)?v.ui:CLASS_INDEXED;
				if ((flags&CLASS_INDEXED)!=0) {
					if ((rc=cb.getValue(PROP_SPEC_CLASSID,v,0,NULL))!=RC_OK) break;
					if (v.type!=VT_URIID || v.uid==STORE_INVALID_CLASSID) {freeV(v); rc=RC_CORRUPTED; break;}
					URIID cid=v.uid;
					if ((rc=cb.getValue(PROP_SPEC_PREDICATE,v,LOAD_SSV,ses))!=RC_OK) break;
					if (v.type!=VT_STMT || v.stmt==NULL || ((Stmt*)v.stmt)->top==NULL) {freeV(v); rc=RC_CORRUPTED; break;}
					rc=add(ses,cid,(Stmt*)v.stmt,flags); v.stmt->destroy(); if (rc!=RC_OK) break;
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

RC Classifier::setFlags(ClassID cid,ulong f,ulong mask)
{
	Class *cls=getClass(cid,RW_X_LOCK); if (cls==NULL) return RC_NOTFOUND;
	cls->flags=cls->flags&~mask|f; cls->release(); return RC_OK;
}

//--------------------------------------------------------------------------------------------------------

Class::Class(ulong id,Classifier& cls,Session *s)
: cid(id),qe(NULL),mgr(cls),query(NULL),index(NULL),id(PIN::defPID),addr(PageAddr::invAddr),flags(0),txs(s)
{
	cluster[0]=cluster[1]=INVALID_PAGEID;
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
	cluster[0]=cluster[1]=INVALID_PAGEID; id=PIN::defPID; addr=PageAddr::invAddr; flags=0;
}

RC Class::load(int,ulong)
{
	SearchKey key((uint64_t)cid); RC rc=RC_NOTFOUND; Value v;
	PINEx cb(Session::getSession()); size_t l=XPINREFSIZE;
	if (mgr.classPINs.find(key,cb.epr.buf,l)) try {
		PINRef pr(mgr.ctx->storeID,cb.epr.buf,cb.epr.lref=byte(l)); id=pr.id;
		if ((pr.def&PR_ADDR)!=0) {cb=addr=pr.addr; cb.epr.flags|=PINEX_ADDRSET;}
		if ((rc=mgr.ctx->queryMgr->getBody(cb))==RC_OK) {
			if ((rc=cb.getValue(PROP_SPEC_PREDICATE,v,LOAD_SSV,mgr.ctx))!=RC_OK) return rc;
			if (v.type!=VT_STMT || v.stmt==NULL || ((Stmt*)v.stmt)->top==NULL || ((Stmt*)v.stmt)->op!=STMT_QUERY) return RC_CORRUPTED;
			query=(Stmt*)v.stmt; v.setError();
			flags=cb.getValue(PROP_SPEC_CLASS_INFO,v,0,NULL)==RC_OK&&(v.type==VT_UINT||v.type==VT_INT)?v.ui:CLASS_INDEXED;
			const static PropertyID propACL=PROP_SPEC_ACL; if (cb.defined(&propACL,1)) flags|=CLASS_ACL;
			if (!mgr.fInit && (flags&CLASS_INDEXED)!=0) rc=mgr.add(cb.getSes(),cid,query,flags);
			if (rc==RC_OK && query->top!=NULL && query->top->getType()==QRY_SIMPLE && ((SimpleVar*)query->top)->condIdx!=NULL) {
				const unsigned nSegs=((SimpleVar*)query->top)->nCondIdx;
				PageID root=INVALID_PAGEID,anchor=INVALID_PAGEID; IndexFormat fmt=ClassIndex::ifmt; uint32_t height=0;
				if ((pr.def&PR_PID2)!=0) {root=uint32_t(pr.id2.pid>>16); height=uint16_t(pr.id2.pid); anchor=uint32_t(pr.id2.ident);}
				if ((pr.def&PR_U1)!=0) fmt.dscr=pr.u1;
				ClassIndex *cidx=index=new(nSegs,mgr.ctx) ClassIndex(*this,nSegs,root,anchor,fmt,height,mgr.ctx);
				if (cidx==NULL) return RC_NORESOURCES; unsigned i=0;
				for (const CondIdx *ci=((SimpleVar*)query->top)->condIdx; ci!=NULL && i<nSegs; ci=ci->next,++i)
					cidx->indexSegs[i]=ci->ks;
			}
		}
	} catch (RC rc2) {rc=rc2;}
	return rc;
}

RC Class::update(bool fInsert)
{
	if (txs!=NULL) return RC_OK;
	PINRef pr(mgr.ctx->storeID,id,addr); SearchKey key((uint64_t)cid); byte buf[XPINREFSIZE];
	if (index!=NULL) {
		pr.u1=index->fmt.dscr; pr.def|=PR_U1;
		if (index->root!=INVALID_PAGEID) {
			pr.id2.pid=uint64_t(index->root)<<16|uint16_t(index->height); 
			pr.id2.ident=uint32_t(index->anchor); pr.def|=PR_PID2;
		}
	}
	return fInsert?mgr.classPINs.insert(key,buf,pr.enc(buf)):mgr.classPINs.update(key,NULL,0,buf,pr.enc(buf));
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

RC ClassIndex::addRootPage(const SearchKey& key,PageID& pageID,ulong level)
{
	RWLockP rw(&rootLock,RW_X_LOCK); PageID oldRoot=root;
	RC rc=TreeStdRoot::addRootPage(key,pageID,level);
	return rc==RC_OK && oldRoot!=root ? cls.update() : rc;
}

RC ClassIndex::removeRootPage(PageID pageID,PageID leftmost,ulong level)
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

RC Classifier::classify(PINEx *pin,ClassResult& res)
{
	res.classes=NULL; res.nClasses=res.xClasses=res.nIndices=0; RC rc; assert(fInit && pin!=NULL && pin->addr.defined());
	if (pin->ses->classLocked==RW_X_LOCK) for (const SubTx *st=&pin->ses->tx; st!=NULL; st=st->next) for (ClassDscr *cd=st->txClass; cd!=NULL; cd=cd->next)
		if (cd->query!=NULL && cd->query->checkConditions(pin,ValueV(NULL,0),pin->ses,0,true) && (rc=res.insert(cd))!=RC_OK) return rc;
	return classIndex.classify(pin,res);
}

void Classifier::findBase(SimpleVar *qv)
{
}

ClassPropIndex *Classifier::getClassPropIndex(const SimpleVar *qv,Session *ses,bool fAdd)
{
	ClassPropIndex *cpi=&classIndex; assert(qv->type==QRY_SIMPLE);
	if (qv->classes!=NULL && qv->nClasses>0) {
		Class *base=getClass(qv->classes[0].classID);
		if (base!=NULL) {
			ClassRefT *cr=findBaseRef(base,ses); base->release();
			if (cr!=NULL) {
				assert(cr->cid==qv->classes[0].classID);
				if (cr->sub!=NULL || fAdd && (cr->sub=new(ctx) ClassPropIndex(ctx))!=NULL) cpi=cr->sub;
			}
		}
	}
	return cpi;
}

RC Classifier::add(Session *ses,ClassID cid,const Stmt *qry,unsigned flags,ulong notifications)
{
	const QVar *qv; PropDNF *dnf=NULL; size_t ldnf; RC rc;
	if (cid==STORE_INVALID_CLASSID || qry==NULL || (qv=qry->top)==NULL || qv->type!=QRY_SIMPLE) return RC_INVPARAM;
	if ((rc=((SimpleVar*)qv)->getPropDNF(dnf,ldnf,ses))==RC_OK) {
		rc=getClassPropIndex((SimpleVar*)qv,ses)->add(cid,qry,flags,dnf,ldnf,notifications,ctx)?RC_OK:RC_NORESOURCES;
		if (dnf!=NULL) ses->free(dnf);
	}
	return rc;
}

ClassRefT *Classifier::findBaseRef(const Class *cls,Session *ses)
{
	assert(cls!=NULL); Stmt *qry=cls->getQuery(); QVar *qv; if (qry==NULL || (qv=qry->top)==NULL) return NULL;
	const ClassPropIndex *cpi=&classIndex; PropDNF *dnf=NULL; size_t ldnf=0; ((SimpleVar*)qv)->getPropDNF(dnf,ldnf,ses);
	if (qv->type==QRY_SIMPLE && ((SimpleVar*)qv)->classes!=NULL && ((SimpleVar*)qv)->nClasses!=0) {
		Class *base=getClass(((SimpleVar*)qv)->classes[0].classID);
		if (base!=NULL) {
			ClassRefT *cr=findBaseRef(base,ses); base->release();
			if (cr!=NULL && cr->cid==((SimpleVar*)qv)->classes[0].classID && cr->sub!=NULL) cpi=cr->sub;
		}
	}
	ClassRefT *cr=(ClassRefT*)cpi->find(cls->getID(),dnf!=NULL?dnf->pids:NULL,dnf!=NULL?dnf->nIncl:0); 
	if (dnf!=NULL) ses->free(dnf); return cr;
}

RC ClassPropIndex::classify(PINEx *pin,ClassResult& res)
{
	if (nClasses!=0) {
		const ClassRefT *const *cpp; RC rc;
		if (pin->hpin!=NULL) {
			for (ClassPropIndex::it<HeapPageMgr::HeapV> it(*this,pin->hpin->getPropTab(),pin->hpin->nProps); (cpp=it.next())!=NULL; )
				if ((rc=classify(*cpp,pin,res))!=RC_OK) return rc;
		} else {
			for (ClassPropIndex::it<Value> it(*this,pin->properties,pin->nProperties); (cpp=it.next())!=NULL; )
				if ((rc=classify(*cpp,pin,res))!=RC_OK) return rc;
		}
	}
	return RC_OK;
}

RC ClassResult::insert(const ClassRef *cr,const ClassRef **cins)
{
	if ((cr->flags&CLASS_VIEW)!=0) return RC_OK;
	if (cins==NULL && classes!=NULL) {BIN<ClassRef,ClassID,ClassRefT::ClassRefCmp>::find(cr->cid,classes,nClasses,&cins); assert(cins!=NULL);}
	if (nClasses>=xClasses) {
		ptrdiff_t sht=cins-classes;
		if ((classes=(const ClassRef**)ma->realloc(classes,(xClasses+=xClasses==0?16:xClasses/2)*sizeof(ClassRef*)))==NULL) return RC_NORESOURCES;
		cins=classes+sht;
	}
	if (cins<&classes[nClasses]) memmove(cins+1,cins,(byte*)&classes[nClasses]-(byte*)cins);
	*cins=cr; nClasses++; notif|=cr->notifications; if (cr->nIndexProps!=0) nIndices++;
	return RC_OK;
}

RC ClassPropIndex::classify(const ClassRefT *cr,PINEx *pin,ClassResult& res)
{
	const ClassRef **cins=NULL; RC rc;
	if (BIN<ClassRef,ClassID,ClassRefT::ClassRefCmp>::find(cr->cid,res.classes,res.nClasses,&cins)==NULL) {
		if (cr->nConds==0 || Expr::condSatisfied(cr->nConds==1?&cr->cond:cr->conds,cr->nConds,&pin,1,NULL,0,pin->ses,true)) {
			if ((rc=res.insert(cr,cins))!=RC_OK) return rc;
			if (cr->sub!=NULL && (rc=cr->sub->classify(pin,res))!=RC_OK) return rc;
		}
	}
	return RC_OK;
}

RC Classifier::enable(Session *ses,Class *cls,ulong notifications)
{
	MutexP lck(&lock); return add(ses,cls->getID(),cls->getQuery(),cls->getFlags(),notifications);
}

void Classifier::disable(Session *ses,Class *cls,ulong notifications)
{
	//MutexP lck(&lock); classIndex.remove(cls,notifications);
}

RC Classifier::indexFormat(ulong vt,IndexFormat& fmt) const
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
	PINEx qr(ses),*pqr=&qr; ses->resetAbortQ(); MutexP lck(&lock); ClassResult clr(ses,ses->getStore());
	QCtx qc(ses); qc.ref(); FullScan fs(&qc,HOH_HIDDEN); fs.connect(&pqr);
	while (rc==RC_OK && (rc=fs.next())==RC_OK) {
		if ((rc=classify(&qr,clr))==RC_OK && clr.classes!=NULL && clr.nClasses>0) 
			rc=index(ses,&qr,clr,(qr.hpin->hdr.descr&HOH_DELETED)!=0?CI_INSERTD:CI_INSERT); // data for other indices!!!
		clr.nClasses=clr.nIndices=0;
	}
	return rc==RC_EOF?RC_OK:rc;
}

namespace MVStoreKernel
{
	class IndexInit : public TreeStdRoot
	{
		Session	*const	ses;
		const ulong		nSegs;
		IndexFormat		fmt;
		PageID			anchor;
		IndexSeg		indexSegs[1];
	public:
		IndexInit(Session *s,CondIdx *c,ulong nS) : TreeStdRoot(INVALID_PAGEID,s->getStore()),ses(s),nSegs(nS),fmt(KT_ALL,0,0),anchor(INVALID_PAGEID) {
			for (ulong i=0; i<nS; i++,c=c->next) {assert(c!=NULL); indexSegs[i]=c->ks;}
			if (nS==1) s->getStore()->classMgr->indexFormat(indexSegs[0].type==VT_ANY&&(indexSegs[0].lPrefix!=0||(indexSegs[0].flags&ORD_NCASE)!=0)?VT_STRING:indexSegs[0].type,fmt);
			else {
				// KT_BIN ?
				fmt=IndexFormat(KT_VAR,KT_VARKEY,KT_VARMDPINREFS);
			}
		}
		void			*operator new(size_t s,ulong nSegs,Session *ses) {return ses->malloc(s+int(nSegs-1)*sizeof(IndexSeg));}
		ulong			getMode() const {return TF_SPLITINTX|TF_NOPOST;}
		TreeFactory		*getFactory() const {return NULL;}
		IndexFormat		indexFormat() const {return fmt;}
		bool			lock(RW_LockType,bool fTry=false) const {return true;}
		void			unlock() const {}
		void			destroy() {}
		friend	class	Classifier;
	};
	struct IndexValue
	{
		IndexKeyV	key;
		size_t		lx;	
		ushort		*buf;
	};
	struct CmpIndexValue {
		static SListOp compare(const IndexValue& left,IndexValue& right,ulong ity) {
			int cmp=left.key.cmp(right.key,(TREE_KT)ity); return cmp<0?SLO_LT:cmp>0?SLO_GT:SLO_NOOP;
		}
	};
	typedef SList<IndexValue,CmpIndexValue> IndexList;
	struct ClassIndexData
	{
		ClassDscr		*cd;
		bool			fSkip;
		union {
			struct {
				size_t	lx,ldx;
				ushort	*buf,*bufd;
			};
			struct {
				SubAlloc	*sa;
				IndexList	*il;
				size_t		liv;
				uint32_t	ity;
			};
		};
	};
	struct ClassCtx
	{
		Session			*ses;
		ClassIndexData	*cid;
		unsigned		first;
		unsigned		last;
		unsigned		idx;
		size_t			total;
		size_t			limit;
	};
	class IndexMultiKey : public IMultiKey {
		ClassCtx&				cctx;
		ClassIndexData&			ci;
		SearchKey				key;
		IndexValue				*pi;
	public:
		IndexMultiKey(ClassCtx& ct,ClassIndexData& c) : cctx(ct),ci(c),pi(NULL) {if (c.il!=NULL) c.il->start();}
		RC	nextKey(const SearchKey *&nk,const void *&value,ushort& lValue,bool& fMulti) {
			if (ci.il==NULL) return RC_EOF;
			do {
				if (pi!=NULL) {cctx.ses->free(pi->buf); pi->buf=NULL; cctx.total-=pi->lx+ci.il->nodeSize(); if (ci.ity>=KT_BIN) cctx.total-=pi->key.ptr.l;}
				if ((pi=(IndexValue*)ci.il->next())==NULL) return RC_EOF;
			} while (pi->buf==NULL || *pi->buf<L_SHT*2);
			nk=new(&key) SearchKey(pi->key,(TREE_KT)ci.ity,SearchKey::PLC_EMB);	// SPTR?
			value=pi->buf; lValue=pi->buf[*pi->buf/L_SHT-1]; fMulti=true; return RC_OK;
		}
	};
};

RC Classifier::freeSpace(ClassCtx& cctx,size_t l,unsigned skip)
{
	while (cctx.total+l>cctx.limit) {
		unsigned idx=~0u; size_t ll=0,l2; RC rc;
		for (unsigned i=cctx.first; i<=cctx.last; i++) if (cctx.cid[i].cd!=NULL && i!=skip)
			{ClassIndexData& ci=cctx.cid[i]; if ((l2=ci.cd->cidx!=NULL?ci.liv:ci.lx+ci.ldx)>ll) {ll=l2; idx=i;}}
		if (idx==~0u) return RC_NORESOURCES;
		ClassIndexData& ci=cctx.cid[idx]; assert(ci.cd!=NULL);
		if (ci.cd->cidx!=NULL) {
			IndexMultiKey imk(cctx,ci); if ((rc=ci.cd->cidx->insert(imk))!=RC_OK) return rc;
			ci.sa->release(); ci.liv=0; if ((ci.il=new(ci.sa) IndexList(*ci.sa,ci.ity))==NULL) return RC_NORESOURCES;
		} else {
			if (ci.buf!=NULL) {
				if (*ci.buf>=L_SHT*2) {SearchKey key((uint64_t)ci.cd->cid); if ((rc=classMap.insert(key,ci.buf,ci.buf[*ci.buf/L_SHT-1],true))!=RC_OK) return rc;}
				if ((ci.buf=(ushort*)cctx.ses->realloc(ci.buf,ci.lx/=2))==NULL) return RC_NORESOURCES;
				cctx.total-=ci.lx;
			}
			if (ci.bufd!=NULL) {
				if (*ci.bufd>=L_SHT*2) {SearchKey key((uint64_t)(ci.cd->cid|SDEL_FLAG)); if ((rc=classMap.insert(key,ci.bufd,ci.bufd[*ci.bufd/L_SHT-1],true))!=RC_OK) return rc;}
				if ((ci.bufd=(ushort*)cctx.ses->realloc(ci.bufd,ci.ldx/=2))==NULL) return RC_NORESOURCES;
				cctx.total-=ci.ldx;
			}
		}
	}
	return RC_OK;
}

RC Classifier::insertRef(ClassCtx& cctx,ushort **ppb,size_t *ps,const byte *extb,ushort lext,IndexValue *pi)
{
	ushort *pb; size_t l=0,lx=*ps; RC rc;
	if ((pb=*ppb)==NULL || size_t((l=pb[*pb/L_SHT-1])+lext+L_SHT)>lx) {
		if (l+lext+L_SHT>0xFFFF) {
			ClassIndexData &ci=cctx.cid[cctx.idx]; assert(cctx.idx<=cctx.last);
			if (ci.cd->cidx!=NULL) {
				SearchKey key(pi->key,(TREE_KT)ci.ity,SearchKey::PLC_EMB); assert(pi!=NULL && pi->buf==pb); 
				if ((rc=ci.cd->cidx->insert(key,(byte*)pb,(ushort)l,true))!=RC_OK) return rc;
			} else {
				SearchKey key((uint64_t)(ci.cd->cid|(pb==ci.bufd?SDEL_FLAG:0)));
				if ((rc=classMap.insert(key,pb,(ushort)l,true))!=RC_OK) return rc;
			}
			*pb=L_SHT; l=L_SHT;
		} else {
			size_t extra=lx==0?lext+L_SHT*2:lx<0x40?0x20:lx/2; 
			if (lx+extra>=0x10000) extra=0x10000-lx-1; assert(extra>=size_t(lext+L_SHT));
			if ((rc=freeSpace(cctx,extra,pi!=NULL?cctx.idx:~0u))!=RC_OK) return rc;
			if ((pb=*ppb)==NULL || *pb>L_SHT) {
				if ((pb=(ushort*)cctx.ses->realloc(pb,*ps+=extra))==NULL) return RC_NORESOURCES;
				if (*ppb==NULL) {*pb=L_SHT; l=L_SHT;} *ppb=pb; cctx.total+=extra;
			}
		}
	}
// lenK, nKeys, etc.!!!
	ushort *p=pb,*p2; assert(p!=NULL && *p!=0 && l!=0);
	for (ulong n=*p/L_SHT-1;n>0;) {
		ulong k=n>>1; ushort *q=p+k; ushort sht=*q,ll=q[1]-sht;
		int cmp=PINRef::cmpPIDs((byte*)pb+sht,ll,extb,lext);
		if (cmp<0) {n-=k+1; p=q+1;} else if (cmp>0) n=k; else {
			byte buf[XPINREFSIZE]; size_t l2=ll,dl; assert(PINRef::getCount(extb,lext)==1);
			switch (PINRef::adjustCount((byte*)pb+sht,l2,1,buf)) {
			default: return RC_INTERNAL;
			case RC_OK: break;
			case RC_TRUE:
				dl=l2-(ulong)ll; assert(l2>(ulong)ll && dl<=ulong(lext+L_SHT));
				if (size_t(sht+ll)<l) memmove((byte*)pb+sht+l2,(byte*)pb+sht+ll,l-sht-ll);
				memcpy((byte*)pb+sht,buf,l2); for (p2=pb+*pb/L_SHT;--p2>q;) *p2+=ushort(dl);
				if (pi!=NULL) cctx.cid[cctx.idx].liv+=dl; break;
			}
			return RC_OK;
		}
	}
	ulong sht=*p; if (sht<l) memmove((byte*)pb+sht+lext,(byte*)pb+sht,l-sht);
	memcpy((byte*)pb+sht,extb,lext); memmove(p+1,p,l-((byte*)p-(byte*)pb)+lext);
	for (p2=pb+*pb/L_SHT+1;--p2>p;) *p2+=lext+L_SHT; for (;p2>=pb;--p2) *p2+=L_SHT;
	if (pi!=NULL) cctx.cid[cctx.idx].liv+=lext+L_SHT;
	assert(pb[*pb/L_SHT-1]<=*ps);
	return RC_OK;
}

RC Classifier::classifyAll(PIN *const *pins,unsigned nPINs,Session *ses,bool fDrop)
{
	if (pins==NULL || nPINs==0) return RC_INVPARAM; if (ses==NULL) return RC_NOSESSION; 
	assert(fInit && ses->inWriteTx());

	ClassIndexData *cid; PIN *pin; CondIdx *pci=NULL; ClassDscr *cds=NULL,**pcd=&cds;
	if ((cid=new(ses) ClassIndexData[nPINs])!=NULL) memset(cid,0,nPINs*sizeof(ClassIndexData)); else return RC_NORESOURCES;
	Value *indexed=NULL; unsigned nIndexed=0,xIndexed=0,xSegs=0; unsigned first=~0u,last=0,nIndex=0; RC rc=RC_OK;
	for (unsigned i=0; i<nPINs; i++) {
		ClassIndexData &ci=cid[i]; ci.fSkip=true; pin=pins[i]; assert(pin!=NULL && (pin->mode&PIN_CLASS)!=0);
		const Value *cv=pin->findProperty(PROP_SPEC_CLASSID); assert(cv!=NULL && cv->type==VT_URIID); 
		if ((ci.cd=new(ses) ClassDscr(cv->uid,0,0))==NULL) {rc=RC_NORESOURCES; break;}
		ci.cd->id=pin->id; ci.cd->addr=pin->addr; *pcd=ci.cd; pcd=&ci.cd->next;
		cv=pin->findProperty(PROP_SPEC_PREDICATE); assert(cv!=NULL && cv->type==VT_STMT); ci.cd->query=(Stmt*)cv->stmt;
		if (ci.cd->query==NULL || ci.cd->query->op!=STMT_QUERY || ci.cd->query->top==NULL || (ci.cd->query->mode&QRY_CPARAMS)!=0) {rc=RC_INVPARAM; break;}
		cv=pin->findProperty(PROP_SPEC_CLASS_INFO); ci.cd->flags=cv!=NULL&&(cv->type==VT_UINT||cv->type==VT_INT)?cv->ui:CLASS_INDEXED;
		if (pin->findProperty(PROP_SPEC_ACL)!=NULL) ci.cd->flags|=CLASS_ACL;
		if (ci.cd->query->top->type==QRY_SIMPLE) pci=((SimpleVar*)ci.cd->query->top)->condIdx;
		if ((ci.cd->query=ci.cd->query->clone(STMT_QUERY,ses,true))==NULL) {rc=RC_NORESOURCES; break;}
		if (pci!=NULL) {
			const unsigned nSegs=ci.cd->nIndexProps=(ushort)((SimpleVar*)ci.cd->query->top)->nCondIdx;
			if ((ci.cd->cidx=new(nSegs,ses) IndexInit(ses,pci,nSegs))==NULL) {rc=RC_NORESOURCES; break;}
			// init fmt
			ci.ity=ci.cd->cidx->fmt.keyType(); ci.sa=NULL; ci.il=0; ci.liv=0;
		}
		if ((ci.cd->flags&CLASS_VIEW)==0 && (pci!=NULL || Stmt::classOK(ci.cd->query->top)) && ci.cd->query->top->type==QRY_SIMPLE && ((SimpleVar*)ci.cd->query->top)->checkXPropID((PropertyID)xPropID)) {
			ci.fSkip=false; nIndex++; last=i; if (first==~0u) first=i;
			if (pci!=NULL) {
				if (ci.cd->cidx->nSegs>xSegs) xSegs=ci.cd->cidx->nSegs;
				for (ulong i=0; i<ci.cd->cidx->nSegs; i++) {
					Value v; v.setPropID(ci.cd->cidx->indexSegs[i].propID);
					if ((rc=BIN<Value,PropertyID,ValCmp>::insert(indexed,nIndexed,v.property,v,ses,&xIndexed))!=RC_OK) break;
				}
			}
		}
		if (rc==RC_OK && fDrop) {
			// get old class here!
			//if (ci.cd->cidx!=NULL) {
			//	if ((rc=ci.cd->cidx->drop())==RC_OK) rc=ci.cd->cidx->cls.update();		// ???
			//} else 
			if ((ci.cd->flags&CLASS_VIEW)==0) {
				SearchKey key((uint64_t)ci.cd->cid); SearchKey dkey((uint64_t)(ci.cd->cid|SDEL_FLAG));
				if ((rc=classMap.remove(key,NULL,0))==RC_NOTFOUND) rc=RC_OK;
				if (rc==RC_OK && (ci.cd->flags&CLASS_SDELETE)!=0 && (rc=classMap.remove(dkey,NULL,0))==RC_NOTFOUND) rc=RC_OK;
			}
			if (rc!=RC_OK) break;
		}
	}

#ifdef REPORT_INDEX_CREATION_TIMES
	TIMESTAMP st,mid,end; getTimestamp(st);
#endif

	ClassCtx cctx={ses,cid,first,last,0,0,CLASS_BUF_LIMIT/StoreCtx::getNStores()};

	if (rc==RC_OK && nIndex>0) {
		MutexP lck(&lock); bool fTest=false; PINEx qr(ses),*pqr=&qr; QueryOp *qop=NULL; ses->resetAbortQ(); QCtx qc(ses);
		if (nIndex==1 && cid[first].cd->query!=NULL && !fDrop) {
			QBuildCtx qctx(ses,ValueV(NULL,0),cid[first].cd->query,0,(cid[first].cd->flags&CLASS_SDELETE)!=0?MODE_CLASS:MODE_CLASS|MODE_NODEL);
			rc=qctx.process(qop);
		} else {
			qc.ref(); rc=(qop=new(ses) FullScan(&qc,HOH_HIDDEN))!=NULL?RC_OK:RC_NORESOURCES; fTest=true;
		}
		if (qop!=NULL) qop->connect(&pqr);	//??? many ???
		const Value **vals=NULL; struct ArrayVal {ArrayVal *prev; const Value *cv; uint32_t idx,vidx;} *freeAV=NULL;
		if (xSegs>0 && (vals=(const Value**)alloca(xSegs*sizeof(Value*)))==NULL) rc=RC_NORESOURCES;
		while (rc==RC_OK && (rc=qop->next())==RC_OK) {
			byte extc[XPINREFSIZE]; bool fExtC=false,fLoaded=false; byte lextc=0;
			if (qr.epr.lref!=0) PINRef::changeFColl(qr.epr.buf,qr.epr.lref,false);
			for (cctx.idx=first; cctx.idx<=last; cctx.idx++) {
				ClassIndexData &ci=cid[cctx.idx]; if (ci.fSkip) continue;
				const bool fDeleted=fTest && (qr.hpin->hdr.descr&HOH_DELETED)!=0; assert(ci.cd!=NULL);
				if (fDeleted && ((ci.cd->flags&CLASS_SDELETE)==0 || ci.cd->cidx!=NULL)) continue;
				if (!fTest || ci.cd->query->checkConditions(&qr,ValueV(NULL,0),ses,0,true)) {
					if (qr.epr.lref==0 && qr.pack()!=RC_OK) continue;
					if (ci.cd->cidx==NULL) rc=insertRef(cctx,fDeleted?&ci.bufd:&ci.buf,fDeleted?&ci.ldx:&ci.lx,qr.epr.buf,qr.epr.lref);
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
								const Value *cv=vals[k]=BIN<Value,PropertyID,ValCmp>::find(ks.propID,indexed,nIndexed); assert(cv!=NULL);
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
								if (ci.il==NULL && (ci.sa==NULL && (ci.sa=new(cctx.ses) SubAlloc(cctx.ses))==NULL ||
									(ci.il=new(ci.sa) IndexList(*ci.sa,ci.ity))==NULL)) {rc=RC_NORESOURCES; break;}
								if ((rc=freeSpace(cctx,ci.il->nodeSize()+(ci.ity>=KT_BIN&&ci.ity<KT_ALL?0x1000:0)))!=RC_OK) break;
								SearchKey key; assert(ci.sa!=NULL && ci.il!=NULL);
								if ((rc=key.toKey(vals,nSegs,ci.cd->cidx->indexSegs,-1,ses,ci.sa))==RC_TYPE||rc==RC_SYNTAX) rc=RC_OK;
								else if (rc==RC_OK) {
									IndexValue v,*pi=NULL; v.key=key.v; v.buf=NULL; v.lx=0;
									if (ci.il->add(v,&pi)==SLO_ERROR) return RC_NORESOURCES; assert(pi!=NULL);
									if (pi->buf==NULL) {
										ci.liv+=ci.il->nodeSize(); cctx.total+=ci.il->nodeSize();
										if (key.type>=KT_BIN && key.type<KT_ALL) {
											if (key.loc==SearchKey::PLC_ALLC) key.loc=SearchKey::PLC_SPTR;
											else if ((pi->key.ptr.p=ci.sa->malloc(key.v.ptr.l))==NULL) {rc=RC_NORESOURCES; break;}
											else memcpy((void*)pi->key.ptr.p,key.v.ptr.p,key.v.ptr.l);
											ci.liv+=key.v.ptr.l; cctx.total+=key.v.ptr.l;
										}
									}
									if ((rc=insertRef(cctx,&pi->buf,&pi->lx,avs!=NULL?extc:qr.epr.buf,avs!=NULL?lextc:qr.epr.lref,pi))!=RC_OK) break;
								} else break;
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
	for (unsigned i=first; i<=last; i++) {
		ClassIndexData &ci=cid[i]; if (ci.cd==NULL) continue;
		if (ci.cd->cidx==NULL) {
			class ClassesNextKey : public IMultiKey {
			// pass cid, i, last
			public:
				RC	nextKey(const SearchKey *&nk,const void *&value,ushort& lValue,bool& fMulti) {
					return RC_EOF;
				}
			};
			if (ci.buf!=NULL) {
				if (rc==RC_OK && *ci.buf>=L_SHT*2) {SearchKey key((uint64_t)ci.cd->cid); rc=classMap.insert(key,ci.buf,ci.buf[*ci.buf/L_SHT-1],true);}
				ses->free(ci.buf);
			}
			if (ci.bufd!=NULL) {
				if (rc==RC_OK && *ci.bufd>=L_SHT*2) {SearchKey key((uint64_t)(ci.cd->cid|SDEL_FLAG)); rc=classMap.insert(key,ci.bufd,ci.bufd[*ci.bufd/L_SHT-1],true);}
				ses->free(ci.bufd);
			}
		} else {
			if (ci.il!=NULL) {
				if (rc==RC_OK) {IndexMultiKey imk(cctx,ci); rc=ci.cd->cidx->insert(imk);}
				else {
					///
				}
			}
			delete ci.sa;
		}
	}

#ifdef REPORT_INDEX_CREATION_TIMES
	getTimestamp(end); report(MSG_DEBUG,"Index creation time: "_LD_FM", "_LD_FM"\n",mid-st,end-mid);
#endif

	if (cds!=NULL) {if (rc==RC_OK) {*pcd=ses->tx.txClass; ses->tx.txClass=cds;} else classTx(ses,cds,false);}
	return rc;
}

Class *Classifier::getClass(ClassID cid,RW_LockType lt)
{
	Class *cls=NULL; 
	if (cid!=STORE_INVALID_CLASSID && get(cls,cid,0,lt)==RC_NOTFOUND) {
		Session *ses=Session::getSession();
		if (ses!=NULL) for (const SubTx *st=&ses->tx; st!=NULL; st=st->next)
			for (ClassDscr *cd=st->txClass; cd!=NULL; cd=cd->next) if (cd->cid==cid) {
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

RC Classifier::classTx(Session *ses,ClassDscr *&cds,bool fCommit)
{
	RC rc=RC_OK;
	for (ClassDscr *cd=cds,*cd2; cd!=NULL; cd=cd2) {
		if (fCommit && rc==RC_OK) {
			Class *cls=NULL;
			if (cd->query==NULL) {
				if ((cls=getClass(cd->cid,RW_X_LOCK))==NULL) rc=RC_NOTFOUND;
				else {
					SearchKey key((uint64_t)cd->cid);
					if ((cls->getFlags()&CLASS_VIEW)==0) {
						if (cls->index!=NULL) rc=cls->index->drop();
						else {
							SearchKey dkey((uint64_t)(cd->cid|SDEL_FLAG));
							if ((rc=classMap.remove(key,NULL,0))==RC_NOTFOUND) rc=RC_OK;
							if (rc==RC_OK && (cls->getFlags()&CLASS_SDELETE)!=0 && (rc=classMap.remove(dkey,NULL,0))==RC_NOTFOUND) rc=RC_OK;
						}
					}
					if ((rc=classPINs.remove(key))==RC_OK) {
						const Stmt *qry=cls->getQuery(); const QVar *qv; ClassPropIndex *cpi;
						if (qry!=NULL && (qv=qry->top)!=NULL && qv->type==QRY_SIMPLE && (cpi=getClassPropIndex((SimpleVar*)qv,ses,false))!=NULL) {
							PropDNF *dnf=NULL; size_t ldnf=0;
							if ((rc=((SimpleVar*)qv)->getPropDNF(dnf,ldnf,ses))==RC_OK) {
								if (dnf==NULL) rc=cpi->remove(cls->getID(),NULL,0);
								else for (const PropDNF *p=dnf,*end=(const PropDNF*)((byte*)p+ldnf); p<end; p=(const PropDNF*)((byte*)(p+1)+int(p->nIncl+p->nExcl-1)*sizeof(PropertyID)))
								if ((rc=cpi->remove(cls->getID(),p->nIncl!=0?p->pids:(PropertyID*)0,p->nIncl))!=RC_OK) break;
							}
						}
					}
					if (rc!=RC_OK) cls->release(); else if (drop(cls)) cls->destroy();
				}
			} else if ((rc=get(cls,cd->cid,0,QMGR_NEW|RW_X_LOCK))==RC_OK) {
				assert(cls!=NULL); cls->id=cd->id; cls->addr=cd->addr; cls->flags=cd->flags;
				if ((cls->query=cd->query->clone(STMT_QUERY,ctx,true))==NULL) rc=RC_NORESOURCES;
				else if (cd->cidx!=NULL) {
					ClassIndex *cidx=cls->index=new(cd->cidx->nSegs,ctx) ClassIndex(*cls,cd->cidx->nSegs,cd->cidx->root,cd->cidx->anchor,cd->cidx->fmt,cd->cidx->height,ctx);
					if (cidx==NULL) rc=RC_NORESOURCES; else memcpy(cidx->indexSegs,cd->cidx->indexSegs,cd->cidx->nSegs*sizeof(IndexSeg));
				}
				if (rc==RC_OK) rc=cls->update(true); release(cls); if (rc==RC_OK) rc=add(ses,cd->cid,cd->query,cd->flags);
			}
		}
		if (cd->cidx!=NULL) {cd->cidx->~IndexInit(); ses->free(cd->cidx);}
		if (cd->query!=NULL) ((Stmt*)cd->query)->destroy();
		cd2=cd->next; ses->free(cd);
	}
	cds=NULL; return rc;
}

RC Classifier::remove(ClassID cid,Session *ses)
{
	ClassDscr *dtx=new(ses) ClassDscr(cid,0,0); if (dtx==NULL) return RC_NORESOURCES;
	dtx->next=ses->tx.txClass; ses->tx.txClass=dtx; return RC_OK;
}

void Classifier::merge(struct ClassDscr *from,struct ClassDscr *&to)
{
	if (to==NULL) to=from; else for (ClassDscr *cd=to;;cd=cd->next) if (cd->next==NULL) {cd->next=from; break;}
}

RC TxIndex::flush()
{
	const TxIndexElt *te; start();
	while ((te=next())!=NULL) {
		// if ClassIndex -> process all keys
		// else -> process all classes to the end
	}
	return RC_OK;
}

#define	PHASE_INSERT	1
#define	PHASE_DELETE	2
#define	PHASE_UPDATE	4
#define	PHASE_UPDCOLL	8
#define	PHASE_DELNULLS	16

enum SubSetV {NewV,DelV,AllPrevV,AllCurV,UnchagedV};

RC Classifier::index(Session *ses,PINEx *pin,const ClassResult& clr,ClassIdxOp op,const struct PropInfo **ppi,unsigned npi,const PageAddr *oldAddr)
{
	RC rc=RC_OK; const bool fMigrated=op==CI_UPDATE && pin->addr!=*oldAddr;
	Class *cls=NULL; ClassIndex *cidx; byte ext[XPINREFSIZE],ext2[XPINREFSIZE];
	PINRef pr(ctx->storeID,pin->id,pin->addr); byte lext=pr.enc(ext),lext2=0; const Value *psegs[10],**pps=psegs; ulong xSegs=10;
	struct SegInfo {PropertyID pid; const PropInfo *pi; SubSetV ssv; ModInfo *mi; Value v; const Value *cv; uint32_t flags,idx,prev; bool fLoaded;} keysegs[10],*pks=keysegs;
	for (ulong i=0; rc==RC_OK && i<clr.nClasses; PINRef::changeFColl(ext,lext,false),i++) {
		const ClassRef *cr=clr.classes[i];
		if (cr->nIndexProps==0) {
			SearchKey key((uint64_t)cr->cid),dkey((uint64_t)(cr->cid|SDEL_FLAG));
			switch (op) {
			default: rc=RC_INVPARAM; break;
			case CI_UDELETE: if ((cr->flags&CLASS_SDELETE)!=0 && (rc=classMap.remove(dkey,ext,lext))!=RC_OK) break;
			case CI_INSERT: rc=classMap.insert(key,ext,lext); break;
			case CI_INSERTD: if ((cr->flags&CLASS_SDELETE)!=0) rc=classMap.insert(dkey,ext,lext); break;
			case CI_UPDATE: if (fMigrated) {if (lext2==0) {pr.addr=*oldAddr; lext2=pr.enc(ext2);} rc=classMap.update(key,ext2,lext2,ext,lext);} break;	// check same?
			case CI_SDELETE: if ((cr->flags&CLASS_SDELETE)!=0 && (rc=classMap.insert(dkey,ext,lext))!=RC_OK) break;
			case CI_DELETE: rc=classMap.remove(key,ext,lext); break;
			case CI_PURGE: if ((cr->flags&CLASS_SDELETE)!=0) rc=classMap.remove(dkey,ext,lext); break;
			}
			if (rc!=RC_OK) 
				report(MSG_ERROR,"Error %d updating(%d) class %d\n",rc,op,cr->cid);
		} else if (op==CI_INSERTD || op==CI_PURGE) continue;
		else if ((cls=getClass(cr->cid))==NULL || (cidx=cls->getIndex())==NULL)
			report(MSG_ERROR,"Family %d not found\n",cr->cid);
		else {
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
						else if (!si.fLoaded && (si.fLoaded=true,rc=pin->getValue(si.pid,si.v,LOAD_SSV,ses))!=RC_NOTFOUND) {
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
						if ((rc=pin->getValue(si.pid,si.v,LOAD_SSV,ses,si.pi->single))!=RC_OK) break;
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
	ClassIndex *cidx=(ClassIndex*)&tr; memcpy(buf,&cidx->cls.cid,sizeof(ClassID));
}

RC Classifier::createTree(const byte *params,byte lparams,Tree *&tree)
{
	if (lparams!=sizeof(ClassID)) return RC_CORRUPTED;
	ClassID cid; memcpy(&cid,params,sizeof(ClassID));
	Class *cls=getClass(cid); if (cls==NULL) return RC_NOTFOUND;
	if (cls->index==NULL) {cls->release(); return RC_NOTFOUND;}
	tree=cls->index; return RC_OK;
}

RC Classifier::getClassInfo(ClassID cid,Class *&cls,uint64_t& nPINs,uint64_t& nDeletedPINs)
{
	nPINs=~0u; nDeletedPINs=0; RC rc=RC_OK;
	if ((cls=getClass(cid))==NULL) return RC_NOTFOUND;
	if (cls->index==NULL) {
		SearchKey key((uint64_t)cid); SearchKey dkey((uint64_t)(cid|SDEL_FLAG));
		if ((rc=classMap.countValues(key,nPINs))==RC_NOTFOUND) {rc=RC_OK; nPINs=0;}
		if (rc==RC_OK && (rc=classMap.countValues(dkey,nDeletedPINs))==RC_NOTFOUND) {rc=RC_OK; nDeletedPINs=0;}
	}
	return rc;
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

const ClassRefT *ClassPropIndex::find(ClassID cid,const PropertyID *pids,ulong npids) const
{
	ulong idx=0; const ClassRefT **pc; ulong np;
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

RC ClassPropIndex::remove(ClassID cid,const PropertyID *pids,ulong npids)
{
	ulong idx=0; const ClassRefT **pc,**del; ulong *np; ClassRefT *cr;
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
			else for (ulong i=0; i<cr->nConds; i++) allc->free(cr->conds[i]);
			//cr->sub ???
			allc->free(cr); --nClasses;
		}
	}
	return RC_OK;
}

bool ClassPropIndex::add(ClassID cid,const Stmt *qry,unsigned flags,const PropDNF *dnf,size_t ldnf,ulong notifications,StoreCtx *ctx)
{
	ClassRefT *cr=NULL; if (dnf==NULL || ldnf==0) return insert(cid,qry,flags,cr,notifications,other,nOther)==RC_OK;
	for (const PropDNF *p=dnf,*end=(const PropDNF*)((byte*)p+ldnf); p<end; p=(const PropDNF*)((byte*)(p+1)+int(p->nIncl+p->nExcl-1)*sizeof(PropertyID))) {
		ulong idx=0,nProps=p->nIncl; if (nProps==0) return insert(cid,qry,flags,cr,notifications,other,nOther)==RC_OK;
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
				if (insert(cid,qry,flags,cr,notifications,pn->classes,pn->nClasses)!=RC_OK) return false;
				break;
			}
			PropertyID pid=p->pids[idx];
			if (pn->pid<pid) {lsib=pn; pn=pn->next;}
			else if (pn->pid>pid) pn=NULL;
			else if (++idx<nProps) {par=pn; lsib=NULL; pn=pn->down;}
			else if (insert(cid,qry,flags,cr,notifications,pn->classes,pn->nClasses)!=RC_OK) return false;
			else break;
		}
	}
	return true;
}

RC ClassPropIndex::insert(ClassID cid,const Stmt *qry,unsigned flags,ClassRefT *&cr,ulong notifications,const ClassRefT **&pc,ulong& n)
{
	ulong idx=0;
	if (pc!=NULL) for (ulong nc=n,base=0; nc>0;) {
		ulong k=nc>>1; ClassRefT *q=(ClassRefT*)pc[idx=base+k];
		if (q->cid==cid) {q->notifications|=notifications; return RC_OK;}
		if (q->cid>cid) nc=k; else {base+=k+1; nc-=k+1; idx++;}
	}
	if (cr==NULL) {
		QVar *qv=qry!=NULL?qry->top:(QVar*)0; CondIdx *ci=qv!=NULL && qv->type==QRY_SIMPLE?((SimpleVar*)qv)->condIdx:(CondIdx*)0;
		ushort nProps=ci!=NULL?(ushort)((SimpleVar*)qv)->nCondIdx:0; ulong nConds=0;
		const Expr **cnd=qv->nConds==1?(const Expr**)&qv->cond:(const Expr**)qv->conds;
		for (unsigned i=0; i<qv->nConds; i++) if ((cnd[i]->getFlags()&EXPR_PARAMS)==0) nConds++;
		if (ci!=NULL && (qry->mode&QRY_IDXEXPR)!=0) {/* count expr properties*/}
		if ((cr=new(nProps,allc) ClassRefT(cid,nProps,ushort(nConds),ushort(notifications),flags))==NULL) return RC_NORESOURCES;
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
	pc=(const ClassRefT**)allc->realloc(pc,(n+1)*sizeof(ClassRefT*));
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
	} catch (RC rc2) {rc=rc2;} catch (...) {report(MSG_ERROR,"Exception in IndexNav::getKey()\n"); rc=RC_INTERNAL;}
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
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ValueEnum::next()\n");}
	try {if (scan!=NULL) scan->release();} catch(...) {}
	return NULL;
}

unsigned IndexNavImpl::nValues()
{
	try {return nVals;} catch (...) {report(MSG_ERROR,"Exception in IndexNav::destroy()\n"); return ~0u;} 
}

void IndexNavImpl::destroy()
{
	try {delete this;} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IndexNav::destroy()\n");} 
}
