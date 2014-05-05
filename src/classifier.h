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

/**
 * Classification control
 * Implementation of IndexNav interface (see affinity.h)
 */
#ifndef _CLASSIFIER_H_
#define _CLASSIFIER_H_

#include "named.h"
#include "startup.h"
#include "pgtree.h"

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
class PIN;
class PINx;
class SimpleVar;
class IndexInit;
class ClassIndex;
class Classifier;
class ClassPropIndex;

/**
 * class/family index operations
 */
enum ClassIdxOp {CI_INSERT, CI_UPDATE, CI_DELETE, CI_SDELETE, CI_UDELETE, CI_PURGE};

/**
 * window descriptor
 */
enum ClassWindowType
{
	CWT_COUNT, CWT_INTERVAL, CWT_REL_INTERVAL
};

struct ClassWindow
{
	uint64_t		range;
	PropertyID		propID;
	ClassWindowType	type;
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
	friend	class		FamilyCreate;
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
	RC				drop() {PageID rt=root; mode|=TF_NOPOST; root=INVALID_PAGEID; return rt!=INVALID_PAGEID?Tree::drop(rt,ctx):RC_OK;}

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
	ushort				refCnt;
	ClassPropIndex		*sub;
	Expr				*cond;
	unsigned			nrProps;
	PropertyID			props[1];
	ClassRefT(ClassID id,const PID& pd,ushort np,ushort nots,unsigned flg,ClassActs *ac,ClassWindow *w) : ClassRef(id,pd,np,nots,flg,ac,w),refCnt(0),sub(NULL),cond(NULL),nrProps(0) {}
	void				*operator new(size_t s,unsigned np,MemAlloc *ma) {return ma->malloc(s+int(np-1)*sizeof(PropertyID));}
	class ClassRefCmp	{public: __forceinline static int cmp(const ClassRef *cr,ClassID ci) {return cmp3(cr->cid,ci);}};
};

/**
 * reference to a class for class creation/destruction
 */
class ClassCreate : public OnCommit, public ClassRef
{
	friend	class	Classifier;
	friend	struct	FamilyData;
	friend	struct	IndexData;
	const Stmt		*query;
	PageAddr		addr;
	IndexFormat		fmt;
	PageID			anchor;
	PageID			root;
	unsigned		height;
	IndexSeg		indexSegs[1];
public:
	ClassCreate(ClassID cid,const PID& pd,const PageAddr&ad,ushort np,unsigned flags) 
		: ClassRef(cid,pd,np,0,flags,NULL,NULL),query(NULL),addr(ad),fmt(KT_UINT,sizeof(uint64_t),KT_PINREFS),anchor(INVALID_PAGEID),root(INVALID_PAGEID),height(0) {}
	void		*operator new(size_t s,unsigned nSegs,MemAlloc *ma) {return ma->malloc(s+int(nSegs-1)*sizeof(IndexSeg));}
	ClassCreate	*getClass();
	RC			process(Session *s);
	void		destroy(Session *s);
	static RC	loadClass(PINx& cb,bool fSafe);
	RC			copy(Class *cls,bool fFull) const;
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
	RC					invokeActions(Session *ses,PIN *,ClassIdxOp,const struct EvalCtx *stk);
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
	RC				classify(PIN *pin,ClassResult& res,Session *ses);
	RC				classify(const ClassRefT *cp,PIN *pin,ClassResult& res,Session *ses);
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
	friend	class		FamilyCreate;
	friend	class		ClassDrop;
	friend	class		TimerQueue;
	StoreCtx			*const ctx;
	RWLock				rwlock;
	ClassPropIndex		classIndex;
	TreeGlobalRoot		classMap;
	SharedCounter		nCached;
	int					xCached;
	Mutex				lock;
public:
	Classifier(StoreCtx *ct,unsigned timeout,unsigned hashSize=DEFAULT_CLASS_HASH_SIZE,unsigned cacheSize=DEFAULT_CLASS_CACHE_SIZE);
	RC					classify(PIN *pin,ClassResult& res,Session *ses);
	RC					enable(Session *ses,Class *cls,unsigned notifications);
	void				disable(Session *ses,Class *cls,unsigned notifications);
	RWLock				*getLock() {return &rwlock;}
	RC					getClassInfo(ClassID cid,Class *&cls,uint64_t& nPINs);
	RC					index(Session *ses,PIN *pin,const ClassResult& clr,ClassIdxOp op,const struct PropInfo **ppi=NULL,unsigned npi=0,const PageAddr *old=NULL);
	RC					rebuildAll(Session *ses);
	RC					classifyAll(PIN *const *pins,unsigned nPins,Session *ses,bool fDrop=false);
	RC					dropClass(Class *cls,Session *ses);
	void				findBase(SimpleVar *qv);
	TreeGlobalRoot&		getClassMap() {return classMap;}
	Class				*getClass(ClassID cid,RW_LockType lt=RW_S_LOCK);
	RC					setFlags(ClassID,unsigned,unsigned);
	RC					findEnumVal(Session *ses,URIID enumid,const char *name,size_t lname,ElementID& ei);
	RC					findEnumStr(Session *ses,URIID enumid,ElementID ei,char *buf,size_t& lbuf);
	RC					initClassPIN(Session *ses,ClassID cid,const PropertyID *props,unsigned nProps,PIN *&pin);
	RC					indexFormat(unsigned vt,IndexFormat& fmt) const;
	RC					createActs(PIN *pin,ClassActs *&acts);
	
	byte				getID() const;
	byte				getParamLength() const;
	void				getParams(byte *buf,const Tree& tr) const;
	RC					createTree(const byte *params,byte lparams,Tree *&tree);
private:
	RC					add(Session *ses,const ClassRef& cr,const Stmt *qry);
	ClassRefT			*findBaseRef(const Class *base,Session *ses);
	ClassPropIndex		*getClassPropIndex(const SimpleVar *qv,Session *ses,bool fAdd=true);
	Tree				*connect(uint32_t handle);
	RC					copyActs(const Value *pv,ClassActs *&acts,unsigned idx);
	void				destroyActs(ClassActs *acts);
};

};

#endif
