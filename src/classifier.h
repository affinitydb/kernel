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

/**
 * Classification control
 * Implementation of IndexNav interface (see affinity.h)
 */
#ifndef _CLASSIFIER_H_
#define _CLASSIFIER_H_

#include "named.h"
#include "timerq.h"
#include "startup.h"

using namespace Afy;

namespace AfyKernel
{
#if defined(__x86_64__) || defined(_M_X64) || defined(_M_IA64)
#define	CLASS_BUF_LIMIT		0x100000000ULL
#else
#define	CLASS_BUF_LIMIT		0x40000000
#endif

#define	META_PROP_ACL				0x100

#define	DEFAULT_CLASS_HASH_SIZE		0x100
#define	DEFAULT_CLASS_CACHE_SIZE	0x400

/**
 * forward class declarations
 */
class Stmt;
class Expr;
class SimpleVar;
class IndexInit;
class ClassIndex;
class Classifier;
class ClassPropIndex;

/**
 * class/family index operations
 */
enum ClassIdxOp {CI_INSERT, CI_UPDATE, CI_DELETE, CI_SDELETE, CI_UDELETE, CI_PURGE};

struct SpecPINProps
{
	uint64_t	mask[4];
	unsigned	meta;
};

/**
 * window descriptor
 */
struct ClassWindow
{
	int64_t		range;
	PropertyID	propID;
	uint32_t	type;
	ClassWindow(const Value& wnd,const Stmt *qry);
};

/**
 * class descriptor
 */
class Class
{
	friend	class		Classifier;
	friend	class		ClassIndex;
	friend	class		ClassCreate;
	friend	class		ClassDrop;
	typedef	QElt<Class,ClassID,ClassID> QE;
	ClassID				cid;
	QE					*qe;
	Classifier&			mgr;
	Stmt				*query;
	ClassIndex			*index;
	PID					id;
	PageAddr			addr;
	uint16_t			meta;
	unsigned			flags;
	Session	*const		txs;
public:
	Class(unsigned id,Classifier& cls,Session *s=NULL);
	~Class();
	Stmt				*getQuery() const {return query;}
	ClassIndex			*getIndex() const {return index;}
	const PID&			getPID() const {return id;}
	const	PageAddr&	getAddr() const {return addr;}
	RC					setAddr(const PageAddr& ad) {addr=ad; return update(true);}
	uint16_t			getMeta() const {return meta;}
	ushort				getFlags() const {return (ushort)flags;}
	RC					update(bool fForce=false);
	ClassID				getID() const {return cid;}
	void				setKey(ClassID id,void*);
	QE					*getQE() const {return qe;}
	void				setQE(QE *q) {qe=q;}
	bool				isDirty() const {return false;}
	RW_LockType			lockType(RW_LockType lt) const {return lt;}
	bool				save() const {return true;}
	RC					load(int,unsigned);
	void				release();
	void				destroy();
	void				operator delete(void *p) {free(p,STORE_HEAP);}
	void				initNew() const {}
	static	Class		*createNew(ClassID id,void *mg);
	static	void		waitResource(void *mg) {threadYield();}
	static	void		signal(void *mg) {}
};

/**
 * index descriptor (associated with class family)
 */
class ClassIndex : public TreeStdRoot
{
	Class&			cls;
	IndexFormat		fmt;
	PageID			anchor;
	mutable RWLock	rwlock;
	mutable	RWLock	rootLock;
	unsigned		state;
	const unsigned	nSegs;
	IndexSeg		indexSegs[1];
public:
	ClassIndex(Class& cl,unsigned nS,PageID rt,PageID anc,IndexFormat fm,uint32_t h,StoreCtx *ct)
				: TreeStdRoot(rt,ct),cls(cl),fmt(fm),anchor(anc),state(0),nSegs(nS) {height=h;}
	virtual			~ClassIndex();
	void			*operator new(size_t s,unsigned nSegs,MemAlloc *ma) {return ma->malloc(s+int(nSegs-1)*sizeof(IndexSeg));}
	operator		Class&() const {return cls;}
	RC				drop() {PageID rt=root; root=INVALID_PAGEID; return rt!=INVALID_PAGEID?Tree::drop(rt,ctx):RC_OK;}

	unsigned		getNSegs() const {return nSegs;}
	const IndexSeg *getIndexSegs() const {return indexSegs;}
	void			release() {cls.release();}

	TreeFactory		*getFactory() const;
	IndexFormat		indexFormat() const;
	PageID			startPage(const SearchKey*,int& level,bool,bool=false);
	PageID			prevStartPage(PageID pid);
	RC				addRootPage(const SearchKey& key,PageID& pageID,unsigned level);
	RC				removeRootPage(PageID page,PageID leftmost,unsigned level);
	bool			lock(RW_LockType,bool fTry=false) const;
	void			unlock() const;
	TreeConnect		*persist(uint32_t& hndl) const;
	void			destroy();

	static	IndexFormat	ifmt;
	friend	class	IndexScan;
	friend	class	ClassCreate;
	friend	class	Classifier;
	friend	class	Class;
};

struct ClassActs
{
	struct {
		Stmt		**acts;
		unsigned	nActs;
	}				acts[3];
};

struct ClassRef
{
	const	ClassID		cid;
	const	PID			pid;
	ushort				nIndexProps;
	ushort				notifications;
	unsigned			flags;
	ClassActs			*acts;
	ClassWindow			*wnd;
	ClassRef(ClassID id,const PID& pd,ushort np,ushort nots,unsigned flg,ClassActs *ac,ClassWindow *w) : cid(id),pid(pd),nIndexProps(np),notifications(nots),flags(flg),acts(ac),wnd(w) {}
};

/**
 * reference to a class
 * stored in memory tables, used in classification
 */
struct ClassRefT : public ClassRef
{
	const	ushort		nConds;
	ushort				refCnt;
	ClassPropIndex		*sub;
	union {
		Expr			*cond;
		Expr			**conds;
	};
	PropertyID			indexProps[1];
	ClassRefT(ClassID id,const PID& pd,ushort np,ushort nc,ushort nots,unsigned flg,ClassActs *ac,ClassWindow *w) : ClassRef(id,pd,np,nots,flg,ac,w),nConds(nc),refCnt(0),sub(NULL) {cond=NULL;}
	void				*operator new(size_t s,unsigned np,MemAlloc *ma) {return ma->malloc(s+int(np-1)*sizeof(PropertyID));}
	class ClassRefCmp	{public: __forceinline static int cmp(const ClassRef *cr,ClassID ci) {return cmp3(cr->cid,ci);}};
};

/**
 * reference to a class for class creation/destruction
 */
class ClassCreate : public OnCommit, public ClassRef
{
	friend	class	Classifier;
	const Stmt	*query;
	IndexInit	*cidx;
	PID			id;
	PageAddr	addr;
public:
	ClassCreate(ClassID cid,const PID& pd,ushort np,unsigned flags,ClassActs *ac,ClassWindow *w) 
		: ClassRef(cid,pd,np,0,flags,ac,w),query(NULL),cidx(NULL),id(PIN::defPID),addr(PageAddr::invAddr) {}
	IndexInit	*getIndex() const {return cidx;}
	ClassCreate	*getClass();
	RC			process(Session *s);
	void		destroy(Session *s);
};

class ClassDrop : public OnCommit
{
	ClassID		cid;
public:
	ClassDrop(ClassID c) : cid(c) {}
	RC			process(Session *s);
	void		destroy(Session *s);
};

/**
 * an array of classes this PIN is a member of
 * @see Classifier::classify()
 */
struct ClassResult
{
	MemAlloc			*const ma;
	StoreCtx			*const ctx;
	const	ClassRef	**classes;
	unsigned			nClasses;
	unsigned			xClasses;
	unsigned			nIndices;
	unsigned			nActions;
	unsigned			notif;
	ClassResult(MemAlloc *m,StoreCtx *ct) : ma(m),ctx(ct),classes(NULL),nClasses(0),xClasses(0),nIndices(0),nActions(0),notif(0) {}
	~ClassResult() {if (classes!=NULL) ma->free(classes);}
	RC					insert(const ClassRef *cr,const ClassRef **cins=NULL);
	RC					checkConstraints(PIN *pin,const struct IntoClass *into,unsigned nInto);
	RC					invokeActions(PIN *,ClassIdxOp,const struct EvalCtx *stk);
};

/**
 * buffer containing PIN references (used in classifications)
 */
struct RefBuf
{
	ushort	*buf;
	size_t	lx;
	RefBuf() : buf(NULL),lx(0) {}
	RefBuf(ushort *p,size_t l) : buf(p),lx(l) {}
	static	SListOp compare(RefBuf& left,RefBuf& right,RefBuf *next,bool fFirst,MemAlloc& ma);
	static	void insert(ushort *pb,const byte *ext,unsigned lext);
	static	RC	insert(ushort *&buf,size_t& lx,const byte *val,unsigned lv,MemAlloc& sa,bool fSingle=false);
};

/**
 * SList containing (ordered) list of RefBuf blocks
 */
typedef SList<RefBuf,RefBuf> RefList;

/**
 * PIN reference data element - a union of simple buffer and SList
 */
struct RefData
{
	SubAlloc	*const	sa;
	size_t				lx;				//maximim buf size or (allocated size for RefList | 1)
	union {
		ushort			*buf;
		RefList			*lst;
	};
	RefData(SubAlloc *s) : sa(s),lx(0) {buf=NULL;}
	RC	add(const byte *val,unsigned lv,bool fSingle=false);
	RC	flush(Tree& tree,const SearchKey& key);
	class MultiKey : public IMultiKey {
		RefData&			rd;
		const	SearchKey&	key;
		const	RefBuf		*rb;
		bool				fAdv;
	public:
		MultiKey(RefData& r,const SearchKey& k) : rd(r),key(k),rb(NULL),fAdv(true) {assert((r.lx&1)!=0); r.lst->start();}
		RC	nextKey(const SearchKey *&nk,const void *&value,ushort& lValue,bool& fMulti,bool fForceNext);
		void push_back();
	};
};

/**
 * family value refernce
 */
struct IndexValue
{
	IndexKeyV	key;
	RefData		refs;
	IndexValue(const IndexKeyV& k,SubAlloc *sa) : key(k),refs(sa) {}
	IndexValue() : refs(NULL) {}		// for SList:node0
	static SListOp compare(const IndexValue& left,IndexValue& right,unsigned ity,MemAlloc&) {
		int cmp=left.key.cmp(right.key,(TREE_KT)ity); return cmp<0?SLO_LT:cmp>0?SLO_GT:SLO_NOOP;
	}
};

/**
 * ordered SList of index values with reference data
 */
typedef SList<IndexValue,IndexValue> IndexList;

/**
 * class or family index container - common part
 */
struct IndexData
{
	ClassCreate		*cd;
	bool			fSkip;
	IndexData(ClassCreate *c) : cd(c),fSkip(true) {}
	virtual	RC		insert(const byte *ext,unsigned lext,SearchKey *key=NULL) = 0;
	virtual	RC		flush(Tree& tr) = 0;
};

/**
 * class index container
 */
struct ClassData : public IndexData
{
	RefData		x;
	ClassData(ClassCreate *d,SubAlloc *sa) : IndexData(d),x(sa) {}
	RC			insert(const byte *ext,unsigned lext,SearchKey *key=NULL);
	RC			flush(Tree& tr);
};

/**
 * family index container
 */
struct FamilyData : public IndexData
{
	IndexList	*il;
	uint32_t	ity;
	SubAlloc	*sa;
	FamilyData(ClassCreate *d,uint32_t ty,SubAlloc *s) : IndexData(d),il(NULL),ity(ty),sa(s) {}
	RC			insert(const byte *ext,unsigned lext,SearchKey *key=NULL);
	RC			flush(Tree& tr);
	class MultiKey : public IMultiKey {
		FamilyData&			ci;
		SearchKey			key;
		RefList				*lst;
		const	IndexValue	*iv;
		bool				fAdv;
	public:
		MultiKey(FamilyData& c) : ci(c),lst(NULL),iv(NULL),fAdv(true) {if (c.il!=NULL) c.il->start();}
		RC	nextKey(const SearchKey *&nk,const void *&value,ushort& lValue,bool& fMulti,bool fForceNext);
		void push_back();
	};
};

/**
 * node in a tree of classes used for classification
 */
struct PIdxNode {
	PropertyID			pid;
	PIdxNode			*up;
	PIdxNode			*next;
	PIdxNode			*down;
	const	ClassRefT	**classes;
	unsigned			nClasses;
};

/**
 * in-memory tree of classes for classification
 */
class ClassPropIndex
{
	friend	class	Classifier;
	friend	class	ClassDrop;
	PIdxNode		*root;
	const ClassRefT	**other;
	unsigned		nOther;
	unsigned		nClasses;
	MemAlloc		*const allc;
private:
	ClassPropIndex(MemAlloc *ma) : root(NULL),other(NULL),nOther(0),nClasses(0),allc(ma) {}
	~ClassPropIndex();
	void operator delete(void *p) {if (p!=NULL)((ClassPropIndex*)p)->allc->free(p);}
	bool			add(const ClassRef& cr,const Stmt *qry,const struct PropDNF *dnf,size_t ldnf,StoreCtx *ctx);
	const ClassRefT	*find(ClassID cid,const PropertyID *pids,unsigned npids) const;
	RC				remove(ClassID cid,const PropertyID *pids,unsigned npids);
	RC				insert(const ClassRef& inf,const Stmt *qry,ClassRefT *&cr,const ClassRefT **&pc,unsigned& n);
	RC				classify(PIN *pin,ClassResult& res);
	RC				classify(const ClassRefT *cp,PIN *pin,ClassResult& res);
	template<typename T> class it {
		const	ClassPropIndex&	pidx;
		const	T				*pt;
		const	unsigned		nProps;
		const	PIdxNode		*node;
		const	ClassRefT		**cp;
				unsigned		nc;
				unsigned		idx;
	public:
		it(const ClassPropIndex& pi,const T *p,unsigned np) : pidx(pi),pt(p),nProps(np),node(pidx.root),cp(NULL),nc(0),idx(0) {}
		const ClassRefT **next() {
			if (cp==NULL) {
				if (pt==NULL || idx>=nProps || node==NULL) {nc=pidx.nOther; return cp=pidx.other;}
			} else if (nc>1) {--nc; return ++cp;}
			else if (node==NULL) return NULL;
			else {
			up:
				while (node->next==NULL || ++idx>=nProps) {
					if ((node=node->up)==NULL) {nc=pidx.nOther; return cp=pidx.other;}
					while (pt[--idx].getPropID()!=node->pid) assert(idx>0);
					if ((nc=node->nClasses)>0) return cp=node->classes;
				}
				node=node->next;
			}
			for (;;) {
				PropertyID pid = pt[idx].getPropID();
				do {
					for (; node->pid<pid; node=node->next) if (node->next==NULL) goto up;
					for (; node->pid>pid; pid=pt[++idx].getPropID()) if (idx+1>=nProps) goto up;
				} while (pid!=node->pid);
				if (node->down!=NULL && idx+1<nProps) {node=node->down; ++idx;} 
				else if ((nc=node->nClasses)==0) goto up; else return cp=node->classes;
			}
		}
	};
};

/**
 * implementation of IndexNav interface
 */
class IndexNavImpl : public IndexNav, public IKeyCallback
{
	Session		*const	ses;
	unsigned	const	nVals;
	PropertyID	const	pid;
	ClassIndex			*cidx;
	TreeScan			*scan;
	bool				fFree;
	Value				v[1];
public:
	IndexNavImpl(Session *ses,ClassIndex *cidx,PropertyID pi=STORE_INVALID_URIID);
	virtual	~IndexNavImpl();
	void	newKey();
	RC		next(PID& id,GO_DIR=GO_NEXT);
	RC		position(const Value *pv,unsigned nValues);
	RC		getValue(const Value *&pv,unsigned& nValues);
	const Value *next();
	unsigned nValues();
	void	destroy();
	void	*operator	new(size_t s,unsigned nV,Session *ses) {return ses->malloc(s+int(nV-1)*sizeof(Value));}
	void	operator	delete(void *p) {if (p!=NULL) ((IndexNavImpl*)p)->ses->free(p);}
};

struct BuiltinURI
{
	size_t		lname;
	const char	*name;
	URIID		uid;
};

typedef QMgr<Class,ClassID,ClassID,int> ClassHash;

/**
 * Classification manager
 */
class Classifier : public ClassHash, public TreeFactory, public TreeConnect
{
	friend	class		Class;
	friend	class		ClassIndex;
	friend	class		ClassPropIndex;
	friend	class		IndexInit;
	friend	class		ClassCreate;
	friend	class		ClassDrop;
	friend	class		TimerQueue;
	StoreCtx			*const ctx;
	RWLock				rwlock;
	Mutex				lock;
	bool				fInit;
	ClassPropIndex		classIndex;
	TreeGlobalRoot		classMap;
	SharedCounter		nCached;
	int					xCached;
	volatile long		xPropID;
public:
	Classifier(StoreCtx *ct,unsigned timeout,unsigned hashSize=DEFAULT_CLASS_HASH_SIZE,unsigned cacheSize=DEFAULT_CLASS_CACHE_SIZE);
	bool				isInit() const {return fInit;}
	RC					initBuiltin(Session *ses);
	RC					restoreXPropID(Session *ses);
	RC					classify(PIN *pin,ClassResult& res);
	RC					enable(Session *ses,Class *cls,unsigned notifications);
	void				disable(Session *ses,Class *cls,unsigned notifications);
	RWLock				*getLock() {return &rwlock;}
	void				setMaxPropID(PropertyID id);
	RC					getClassInfo(ClassID cid,Class *&cls,uint64_t& nPINs);
	RC					index(Session *ses,PIN *pin,const ClassResult& clr,ClassIdxOp op,const struct PropInfo **ppi=NULL,unsigned npi=0,const PageAddr *old=NULL);
	RC					initClasses(Session *ses);
	RC					rebuildAll(Session *ses);
	RC					classifyAll(PIN *const *pins,unsigned nPins,Session *ses,bool fDrop=false);
	RC					dropClass(Class *cls,Session *ses);
	void				findBase(SimpleVar *qv);
	TreeGlobalRoot&		getClassMap() {return classMap;}
	Class				*getClass(ClassID cid,RW_LockType lt=RW_S_LOCK);
	RC					setFlags(ClassID,unsigned,unsigned);
	PropertyID			getXPropID() const {return (PropertyID)xPropID;}
	RC					findEnumVal(Session *ses,URIID enumid,const char *name,size_t lname,ElementID& ei);
	RC					findEnumStr(Session *ses,URIID enumid,ElementID ei,char *buf,size_t& lbuf);
	
	byte				getID() const;
	byte				getParamLength() const;
	void				getParams(byte *buf,const Tree& tr) const;
	RC					createTree(const byte *params,byte lparams,Tree *&tree);
	static	const char	*getBuiltinName(URIID uid,size_t& lname);
	static	URIID		getBuiltinURIID(const char *name,size_t lname,bool fSrv);
	static	uint16_t	getMeta(ClassID cid);
	static	const		SpecPINProps specPINProps[9];
private:
	static	const		BuiltinURI	builtinURIs[];
	RC					add(Session *ses,const ClassRef& cr,const Stmt *qry);
	ClassRefT			*findBaseRef(const Class *base,Session *ses);
	ClassPropIndex		*getClassPropIndex(const SimpleVar *qv,Session *ses,bool fAdd=true);
	RC					indexFormat(unsigned vt,IndexFormat& fmt) const;
	Tree				*connect(uint32_t handle);
	RC					createActs(PIN *pin,ClassActs *&acts);
	RC					copyActs(const Value *pv,ClassActs *&acts,unsigned idx);
	void				destroyActs(ClassActs *acts);
};

};

#endif
