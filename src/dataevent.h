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
 * Data modification events control
 * Implementation of IndexNav interface (see affinity.h)
 */
#ifndef _DATAEVENT_H_
#define _DATAEVENT_H_

#include "named.h"
#include "startup.h"
#include "pgtree.h"
#include "modinfo.h"

using namespace Afy;

namespace AfyKernel
{
#if defined(__x86_64__) || defined(_M_X64) || defined(_M_IA64)
#define	INDEX_BUF_LIMIT		0x100000000ULL
#else
#define	INDEX_BUF_LIMIT		0x40000000
#endif

#define	META_PROP_ACL				0x100

#define	DEFAULT_DATA_HASH_SIZE		0x100
#define	DEFAULT_DATA_CACHE_SIZE	0x400

/**
 * forward class declarations
 */
class Stmt;
class Expr;
class PIN;
class PINx;
class SimpleVar;
class IndexInit;
class DataIndex;
class DataEventMgr;
class DataEventRegistry;

/**
 * data index operations
 */
enum DataIndexOp {CI_INSERT, CI_UPDATE, CI_DELETE, CI_SDELETE, CI_UDELETE, CI_PURGE};

/**
 * window descriptor
 */
enum StreamWindowType
{
	SWT_COUNT, SWT_INTERVAL, SWT_REL_INTERVAL
};

struct StreamWindow
{
	uint64_t		range;
	PropertyID		propID;
	StreamWindowType	type;
	StreamWindow(const Value& wnd,const Stmt *qry);
};

/**
 * Data event descriptor
 */
class DataEvent
{
	friend	class		DataEventMgr;
	friend	class		DataIndex;
	friend	class		CreateDataEvent;
	friend	class		FamilyCreate;
	friend	class		DropDataEvent;
	typedef	QElt<DataEvent,DataEventID,DataEventID> QE;
	DataEventID			cid;
	QE					*qe;
	DataEventMgr&		mgr;
	Stmt				*query;
	DataIndex			*index;
	PID					id;
	PageAddr			addr;
	uint16_t			meta;
	unsigned			flags;
	Session	*const		txs;
public:
	DataEvent(unsigned id,DataEventMgr& dev,Session *s=NULL);
	~DataEvent();
	Stmt				*getQuery() const {return query;}
	DataIndex			*getIndex() const {return index;}
	const PID&			getPID() const {return id;}
	const	PageAddr&	getAddr() const {return addr;}
	RC					setAddr(const PageAddr& ad) {addr=ad; return update(true);}
	uint16_t			getMeta() const {return meta;}
	ushort				getFlags() const {return (ushort)flags;}
	RC					update(bool fForce=false);
	DataEventID			getID() const {return cid;}
	void				setKey(DataEventID id,void*);
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
	static	DataEvent	*createNew(DataEventID id,void *mg);
	static	void		waitResource(void *mg) {threadYield();}
	static	void		signal(void *mg) {}
};

/**
 * index descriptor (associated with indexed family)
 */
class DataIndex : public TreeStdRoot
{
	DataEvent&		dev;
	IndexFormat		fmt;
	PageID			anchor;
	mutable RWLock	rwlock;
	mutable	RWLock	rootLock;
	unsigned		state;
	const unsigned	nSegs;
	IndexSeg		indexSegs[1];
public:
	DataIndex(DataEvent& cl,unsigned nS,PageID rt,PageID anc,IndexFormat fm,uint32_t h,StoreCtx *ct)
				: TreeStdRoot(rt,ct),dev(cl),fmt(fm),anchor(anc),state(0),nSegs(nS) {height=h;}
	virtual			~DataIndex();
	void			*operator new(size_t s,unsigned nSegs,MemAlloc *ma) {return ma->malloc(s+int(nSegs-1)*sizeof(IndexSeg));}
	operator		DataEvent&() const {return dev;}
	RC				drop() {PageID rt=root; mode|=TF_NOPOST; root=INVALID_PAGEID; return rt!=INVALID_PAGEID?Tree::drop(rt,ctx):RC_OK;}

	unsigned		getNSegs() const {return nSegs;}
	const IndexSeg *getIndexSegs() const {return indexSegs;}
	void			release() {dev.release();}

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
	friend	class	CreateDataEvent;
	friend	class	DataEventMgr;
	friend	class	DataEvent;
};

struct DataEventActions
{
	struct {
		Stmt		**acts;
		unsigned	nActs;
	}				acts[3];
};

struct DataEventRef
{
	const	DataEventID	cid;
	const	PID			pid;
	ushort				nIndexProps;
	ushort				notifications;
	unsigned			flags;
	DataEventActions	*acts;
	StreamWindow		*wnd;
	DataEventRef(DataEventID id,const PID& pd,ushort np,ushort nots,unsigned flg,DataEventActions *ac,StreamWindow *w) : cid(id),pid(pd),nIndexProps(np),notifications(nots),flags(flg),acts(ac),wnd(w) {}
};

/**
 * reference to a class
 * stored in memory tables, used in classification
 */
struct DataEventRefT : public DataEventRef
{
	ushort				refCnt;
	DataEventRegistry	*sub;
	Expr				*cond;
	unsigned			nrProps;
	PropertyID			props[1];
	DataEventRefT(DataEventID id,const PID& pd,ushort np,ushort nots,unsigned flg,DataEventActions *ac,StreamWindow *w) : DataEventRef(id,pd,np,nots,flg,ac,w),refCnt(0),sub(NULL),cond(NULL),nrProps(0) {}
	void				*operator new(size_t s,unsigned np,MemAlloc *ma) {return ma->malloc(s+int(np-1)*sizeof(PropertyID));}
	class DataEventRefCmp	{public: __forceinline static int cmp(const DataEventRef *cr,DataEventID ci) {return cmp3(cr->cid,ci);}};
};

/**
 * reference to a class for class creation/destruction
 */
class CreateDataEvent : public OnCommit, public DataEventRef
{
	friend	class	DataEventMgr;
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
	CreateDataEvent(DataEventID cid,const PID& pd,const PageAddr&ad,ushort np,unsigned flags) 
		: DataEventRef(cid,pd,np,0,flags,NULL,NULL),query(NULL),addr(ad),fmt(KT_UINT,sizeof(uint64_t),KT_PINREFS),anchor(INVALID_PAGEID),root(INVALID_PAGEID),height(0) {}
	void		*operator new(size_t s,unsigned nSegs,MemAlloc *ma) {return ma->malloc(s+int(nSegs-1)*sizeof(IndexSeg));}
	CreateDataEvent	*getDataEvent();
	RC			process(Session *s);
	void		destroy(Session *s);
	static RC	loadClass(PINx& cb,bool fSafe);
	RC			copy(DataEvent *dev,bool fFull) const;
};

class DropDataEvent : public OnCommit
{
	DataEventID	cid;
public:
	DropDataEvent(DataEventID c) : cid(c) {}
	RC			process(Session *s);
	void		destroy(Session *s);
};

/**
 * an array of detected data events
 * @see DataEventMgr::detect()
 */
struct DetectedEvents
{
	MemAlloc				*const ma;
	StoreCtx				*const ctx;
	const	DataEventRef	**devs;
	unsigned				ndevs;
	unsigned				xdevs;
	unsigned				nIndices;
	unsigned				nActions;
	unsigned				notif;
	DetectedEvents(MemAlloc *m,StoreCtx *ct) : ma(m),ctx(ct),devs(NULL),ndevs(0),xdevs(0),nIndices(0),nActions(0),notif(0) {}
	~DetectedEvents() {if (devs!=NULL) ma->free(devs);}
	RC						insert(const DataEventRef *cr,const DataEventRef **cins=NULL);
	RC						checkConstraints(PIN *pin,const struct IntoClass *into,unsigned nInto);
	RC						publish(Session *ses,PIN *,DataIndexOp,const struct EvalCtx *stk);
};

/**
 * node in DataEventRegistry
 */
struct PIdxNode {
	PropertyID				pid;
	PIdxNode				*up;
	PIdxNode				*next;
	PIdxNode				*down;
	const	DataEventRefT	**devs;
	unsigned				ndevs;
};

/**
 * Registry of data events in memory
 */
class DataEventRegistry
{
	friend	class		DataEventMgr;
	friend	class		DropDataEvent;
	PIdxNode			*root;
	const DataEventRefT	**other;
	unsigned			nOther;
	unsigned			ndevs;
	MemAlloc	*const	allc;
private:
	DataEventRegistry(MemAlloc *ma) : root(NULL),other(NULL),nOther(0),ndevs(0),allc(ma) {}
	~DataEventRegistry();
	void operator delete(void *p) {if (p!=NULL)((DataEventRegistry*)p)->allc->free(p);}
	bool				add(const DataEventRef& cr,const Stmt *qry,const struct PropDNF *dnf,size_t ldnf,StoreCtx *ctx);
	const DataEventRefT	*find(DataEventID cid,const PropertyID *pids,unsigned npids) const;
	RC					remove(DataEventID cid,const PropertyID *pids,unsigned npids);
	RC					insert(const DataEventRef& inf,const Stmt *qry,DataEventRefT *&cr,const DataEventRefT **&pc,unsigned& n);
	RC					detect(PIN *pin,DetectedEvents& res,Session *ses,const ModProps *mp=NULL);
	RC					detect(const DataEventRefT *cp,PIN *pin,DetectedEvents& res,Session *ses,const ModProps *mp=NULL);
	template<typename T> class it {
		const	DataEventRegistry&	pidx;
		const	T					*pt;
		const	unsigned			nProps;
		const	PIdxNode			*node;
		const	DataEventRefT		**cp;
				unsigned			nc;
				unsigned			idx;
	public:
		it(const DataEventRegistry& pi,const T *p,unsigned np) : pidx(pi),pt(p),nProps(np),node(pidx.root),cp(NULL),nc(0),idx(0) {}
		const DataEventRefT **next() {
			if (cp==NULL) {
				if (pt==NULL || idx>=nProps || node==NULL) {nc=pidx.nOther; return cp=pidx.other;}
			} else if (nc>1) {--nc; return ++cp;}
			else if (node==NULL) return NULL;
			else {
			up:
				while (node->next==NULL || ++idx>=nProps) {
					if ((node=node->up)==NULL) {nc=pidx.nOther; return cp=pidx.other;}
					while (pt[--idx].getPropID()!=node->pid) assert(idx>0);
					if ((nc=node->ndevs)>0) return cp=node->devs;
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
				else if ((nc=node->ndevs)==0) goto up; else return cp=node->devs;
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
	DataIndex			*cidx;
	TreeScan			*scan;
	bool				fFree;
	Value				v[1];
public:
	IndexNavImpl(Session *ses,DataIndex *cidx,PropertyID pi=STORE_INVALID_URIID);
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

typedef QMgr<DataEvent,DataEventID,DataEventID,int> DataEventHash;

/**
 * Data events manager
 */
class DataEventMgr : public DataEventHash, public TreeFactory, public TreeConnect
{
	friend	class		DataEvent;
	friend	class		DataIndex;
	friend	class		DataEventRegistry;
	friend	class		IndexInit;
	friend	class		CreateDataEvent;
	friend	class		FamilyCreate;
	friend	class		DropDataEvent;
	friend	class		TimerQueue;
	StoreCtx			*const ctx;
	RWLock				rwlock;
	DataEventRegistry	dataEventIndex;
	TreeGlobalRoot		dataEventMap;
	SharedCounter		nCached;
	int					xCached;
	Mutex				lock;
public:
	DataEventMgr(StoreCtx *ct,unsigned timeout,unsigned hashSize=DEFAULT_DATA_HASH_SIZE,unsigned cacheSize=DEFAULT_DATA_CACHE_SIZE);
	RC					detect(PIN *pin,DetectedEvents& res,Session *ses,const ModProps *mp=NULL);
	RC					enable(Session *ses,DataEvent *dev,unsigned notifications);
	void				disable(Session *ses,DataEvent *dev,unsigned notifications);
	RWLock				*getLock() {return &rwlock;}
	RC					getDataEventInfo(DataEventID cid,DataEvent *&dev,uint64_t& nPINs);
	RC					updateIndex(Session *ses,PIN *pin,const DetectedEvents& clr,DataIndexOp op,const ModProps *mp=NULL,const PageAddr *old=NULL);
	RC					rebuildAll(Session *ses);
	RC					buildIndex(PIN *const *pins,unsigned nPins,Session *ses,bool fDrop=false);
	void				findBase(SimpleVar *qv);
	TreeGlobalRoot&		getDataEventMap() {return dataEventMap;}
	DataEvent			*getDataEvent(DataEventID cid,RW_LockType lt=RW_S_LOCK);
	RC					setFlags(DataEventID,unsigned,unsigned);
	RC					findEnumVal(Session *ses,URIID enumid,const char *name,size_t lname,ElementID& ei);
	RC					findEnumStr(Session *ses,URIID enumid,ElementID ei,char *buf,size_t& lbuf);
	RC					initClassPIN(Session *ses,DataEventID cid,const PropertyID *props,unsigned nProps,PIN *&pin);
	RC					indexFormat(unsigned vt,IndexFormat& fmt) const;
	RC					createActions(PIN *pin,DataEventActions *&acts);
	
	byte				getID() const;
	byte				getParamLength() const;
	void				getParams(byte *buf,const Tree& tr) const;
	RC					createTree(const byte *params,byte lparams,Tree *&tree);
private:
	RC					add(Session *ses,const DataEventRef& cr,const Stmt *qry);
	DataEventRefT		*findBaseRef(const DataEvent *base,Session *ses);
	DataEventRegistry	*getRegistry(const SimpleVar *qv,Session *ses,bool fAdd=true);
	Tree				*connect(uint32_t handle);
	RC					copyActions(const Value *pv,DataEventActions *&acts,unsigned idx);
	void				destroyActions(DataEventActions *acts);
};

};

#endif
