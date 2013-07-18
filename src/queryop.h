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

/**
 * query evaluation operators
 */
#ifndef _QUERYOP_H_
#define _QUERYOP_H_

#include "qbuild.h"
#include "expr.h"
#include "classifier.h"
#include "propdnf.h"

namespace AfyKernel
{

#define	DEFAULT_QUERY_MEM	0x100000ul		/**< default memory limit used by query */

#define	QO_UNIQUE		0x00000001			/**< operator doesn't return repeating PINs */
#define	QO_VUNIQUE		0x00000002			/**< for Sort: exclude repeating based on values rather than PIN ID */
#define	QO_JOIN			0x00000004			/**< join operator */
#define	QO_STREAM		0x00000008			/**< data streaming operator, e.g. a scan */
#define	QO_IDSORT		0x00000010			/**< result is sorted according to PIN IDs */
#define	QO_REVERSIBLE	0x00000020			/**< sort order is reversible, e.g. in index scan */
#define	QO_FORUPDATE	0x00000040			/**< lock for update */
#define	QO_CLASS		0x00000080			/**< initial classification data scan */
#define	QO_ALLPROPS		0x00000100			/**< all PIN properties should be returned */
#define	QO_DELETED		0x00000200			/**< return soft-deleted PINs */
#define	QO_CHECKED		0x00000400			/**< access permission is checked for this PIN */
#define QO_VCOPIED		0x00000800			/**< data is copied from Stmt to QueryOp, must be deallocated */
#define	QO_REORDER		0x00001000			/**< reordering of data is allowed */
#define	QO_NODATA		0x00002000			/**< no property values is necessary to filter PINs, e.g. when only local PINs are required */
#define	QO_UNI1			0x00004000			/**< first source is unique */
#define	QO_UNI2			0x00008000			/**< second source is unique */
#define	QO_RAW			0x00010000			/**< return PINs 'as-is', without communications, property calculation, etc. */
#define	QO_HIDDEN		0x00020000			/**< return hidden pins in initialization */
#define	QO_AUGMENT		0x00040000			/**< SEL_AUGMENTED for TransOp */
#define	QO_LOADALL		0x00080000			/**< load all properties for TransOp or in Sort */

/**
 * query operator state flags
 */
#define	QST_INIT		0x80000000			/**< query operator was not initialized yet */
#define	QST_BOF			0x40000000			/**< before first result received */
#define	QST_EOF			0x20000000			/**< end of result set is encountered */

/**
 * flags for merge-sort operators
 */
#define	QOS_ADV			0x0001				/**< next result is to be read */
#define	QOS_ADV1		0x0001				/**< next result is to be read from first source */
#define	QOS_ADV2		0x0002				/**< next result is to be read from second source */
#define	QOS_EOF1		0x0004				/**< end of results from first source */
#define	QOS_EOF2		0x0008				/**< end of results from second source */
#define	QOS_STR			0x0010				/**< store 1st source to process repeating */

struct	CondIdx;
class	ExtSortFile;
class	SOutCtx;

/**
 * query operator abstract class
 */
class QueryOp
{
	friend		class	QBuildCtx;
	friend		class	SimpleVar;
protected:
	QCtx		*const	qx;
	QueryOp		*const	queryOp;
	unsigned			state;
	unsigned			nSkip;
	PINx				*res;
	unsigned			nOuts;
	unsigned			qflags;
	const OrderSegQ		*sort;
	unsigned			nSegs;
	const PropList		*props;
	unsigned			nProps;
	QueryOp				*extsrc;
	RC					initSkip();
public:
						QueryOp(QCtx *qc,unsigned qf);
						QueryOp(QueryOp *qop,unsigned qf);
	virtual				~QueryOp();
	virtual	void		connect(PINx **results,unsigned nRes=1);
	virtual	RC			init();
	virtual	RC			advance(const PINx *skip=NULL) = 0;
	virtual	RC			rewind();
	virtual	RC			count(uint64_t& cnt,unsigned nAbort=~0u);
	virtual	RC			loadData(PINx& qr,Value *pv,unsigned nv,ElementID eid=STORE_COLLECTION_ID,bool fSort=false,MemAlloc *ma=NULL);
	virtual	void		unique(bool);
	virtual	void		reverse();
	virtual	void		print(SOutCtx& buf,int level) const;
	void	operator	delete(void *p) {if (p!=NULL) {QCtx *qx=((QueryOp*)p)->qx; qx->ses->free(p); qx->destroy();}}
	void				setECtx(const EvalCtx *ect) {qx->ectx=ect;}
	void				setSkip(unsigned n) {nSkip=n;}
	void				setHidden() {qflags|=QO_HIDDEN;}
	unsigned			getSkip() const {return nSkip;}
	QCtx				*getQCtx() const {return qx;}
	Session				*getSession() const {return qx->ses;}
	unsigned			getNOuts() const {return nOuts;}
	unsigned			getQFlags() const {return qflags;}
	const	OrderSegQ	*getSort(unsigned& nS) const {nS=nSegs; return sort;}
	const	PropList	*getProps(unsigned& nP) const {nP=nProps; return props;}
	RC					getData(PINx& qr,Value *pv,unsigned nv,const PINx *qr2=NULL,ElementID eid=STORE_COLLECTION_ID,MemAlloc *ma=NULL);
	RC					getData(PINx **pqr,unsigned npq,const PropList *pl,unsigned npl,Value *vals,MemAlloc *ma=NULL);
	RC					getBody(PINx& pe);
	RC					next(const PINx *skip=NULL);
	RC					createCommOp(PINx *pex=NULL,const byte *er=NULL,size_t l=0);
};

/**
 * full store scan operator
 */
class FullScan : public QueryOp
{
	const uint32_t	mask;
	const bool		fClasses;
	PageID			dirPageID;
	PageID			heapPageID;
	unsigned		idx;
	unsigned		slot;
	SubTx			*stx;
	PageSet::it		*it;
	PBlock			*initPB;
public:
	FullScan(QCtx *s,uint32_t msk=HOH_DELETED|HOH_HIDDEN,unsigned qf=0,bool fCl=false)
		: QueryOp(s,qf|QO_UNIQUE|QO_STREAM|QO_ALLPROPS),mask(msk),fClasses(fCl),dirPageID(INVALID_PAGEID),heapPageID(INVALID_PAGEID),idx(~0u),slot(0),stx(&s->ses->tx),it(NULL),initPB(NULL) {}
	virtual		~FullScan();
	RC			init();
	RC			advance(const PINx *skip=NULL);
	RC			rewind();
	void		print(SOutCtx& buf,int level) const;
};

/**
 * class scan operator
 * uses class index or named PIN index
 */
class ClassScan : public QueryOp
{
	SearchKey		key;
	TreeScan		*scan;
	const uint16_t	meta;
public:
	ClassScan(QCtx *ses,ClassID cid,unsigned md);
	virtual		~ClassScan();
	RC			init();
	RC			advance(const PINx *skip=NULL);
	RC			rewind();
	RC			count(uint64_t& cnt,unsigned nAbort=~0u);
	void		print(SOutCtx& buf,int level) const;
};

/**
 * special builtin class scan (e.g. CLASS_OF_STORES, CLASS_OF_SERVICES)
 */
class SpecClassScan : public QueryOp
{
	const ClassID	cls;
public:
	SpecClassScan(QCtx *qx,ClassID cl,unsigned qf) : QueryOp(qx,qf|QO_UNIQUE|QO_STREAM|QO_ALLPROPS),cls(cl) {}
	virtual		~SpecClassScan();
	RC			init();
	RC			advance(const PINx *skip=NULL);
	RC			rewind();
	RC			count(uint64_t& cnt,unsigned nAbort=~0u);
	void		print(SOutCtx& buf,int level) const;
};

/**
 * family scan operator
 * uses family index
 */
class IndexScan : public QueryOp
{
	ClassIndex&			index;
	const	ClassID		classID;
	unsigned			flags;
	unsigned			rangeIdx;
	TreeScan			*scan;
	class	PIDStore	*pids;
	PropList			pl;
	const	unsigned	nRanges;
	Value				*vals;
	void				initInfo();
	RC					setScan(unsigned=0);
	void				printKey(const SearchKey& key,SOutCtx& buf,const char *def,size_t ldef) const;
public:
	IndexScan(QCtx *ses,ClassIndex& idx,unsigned flg,unsigned np,unsigned md);
	virtual				~IndexScan();
	void				*operator new(size_t s,Session *ses,unsigned nRng,ClassIndex& idx) {return ses->malloc(s+nRng*2*sizeof(SearchKey)+idx.getNSegs()*(sizeof(OrderSegQ)+sizeof(PropertyID)));}
	RC					init();
	RC					advance(const PINx *skip=NULL);
	RC					rewind();
	RC					count(uint64_t& cnt,unsigned nAbort=~0u);
	RC					loadData(PINx& qr,Value *pv,unsigned nv,ElementID eid=STORE_COLLECTION_ID,bool fSort=false,MemAlloc *ma=NULL);
	void				unique(bool);
	void				reverse();
	void				print(SOutCtx& buf,int level) const;
	friend	class		SimpleVar;
};

/**
 * Evaluates an expression - returnes PIN IDs from the resulting array
 */
class ExprScan : public QueryOp
{
	Value		expr;
	Value		r;
	unsigned	idx;
public:
	ExprScan(QCtx *s,const Value& v,unsigned md);
	virtual		~ExprScan();
	RC			init();
	RC			advance(const PINx *skip=NULL);
	RC			rewind();
	RC			count(uint64_t& cnt,unsigned nAbort=~0u);
	void		print(SOutCtx& buf,int level) const;
};

/**
 * free-text index scan
 */
class FTScan : public QueryOp
{
	SearchKey				word;
	const	unsigned		flags;
	const	bool			fStop;
	struct	FTScanS {
		class	TreeScan	*scan;
		unsigned			state;
		PID					id;
	}						scans[4];
	unsigned				nScans;
	Value					current;
	unsigned				nPids;
	PropertyID				pids[1];
public:
	void	*operator new(size_t s,Session *ses,unsigned nps,size_t lw) throw() {return ses->malloc(s+lw+(nps==0?0:(nps-1)*sizeof(PropertyID)));}
	FTScan(QCtx *s,const char *w,size_t lW,const PropertyID *pids,unsigned nps,unsigned md,unsigned f,bool fStp);
	virtual		~FTScan();
	RC			init();
	RC			advance(const PINx *skip=NULL);
	RC			rewind();
	void		print(SOutCtx& buf,int level) const;
	friend	class	PhraseFlt;
};

/**
 * phrase filter
 * n.b. implementation is not finished!
 */
class PhraseFlt : public QueryOp
{
	struct	FTScanS {
		class	FTScan		*scan;
		unsigned			state;
		PID					id;
	};
	const	unsigned		nScans;
	Value					current;
	FTScanS					scans[1];
public:
	PhraseFlt(QCtx *s,FTScan *const *fts,unsigned ns,unsigned md);
	virtual		~PhraseFlt();
	void		*operator new(size_t s,Session *ses,unsigned ns) throw() {return ses->malloc(s+int(ns-1)*sizeof(FTScanS));}
	RC			advance(const PINx *skip=NULL);
	RC			rewind();
	void		print(SOutCtx& buf,int level) const;
};

/**
 * PIN set operations (UNION, INTERSECT, EXCEPT)
 * for UNION and INTERSECT more than 2 sources can be specified
 */
class MergeIDs : public QueryOp
{
protected:
	struct	QueryOpS {
		QueryOp		*qop;
		unsigned	state;
		EncPINRef	epr;	
	};
	const	QUERY_SETOP	op;
	unsigned			nOps;
	unsigned			cur;
	PINx				*pqr;
	QueryOpS			ops[1];
public:
	MergeIDs(QCtx *s,QueryOp **o,unsigned no,QUERY_SETOP op,unsigned qf);
	void	*operator new(size_t s,Session *ses,unsigned no) throw() {return ses->malloc(s+int(no-1)*sizeof(QueryOpS));}
	virtual	~MergeIDs();
	void	connect(PINx **results,unsigned nRes);
	RC		advance(const PINx *skip=NULL);
	RC		rewind();
	RC		loadData(PINx& qr,Value *pv,unsigned nv,ElementID eid=STORE_COLLECTION_ID,bool fSort=false,MemAlloc *ma=NULL);
	void	print(SOutCtx& buf,int level) const;
	friend	class	QBuildCtx;
};

/**
 * helper functions to store PIN ID and some property values in memory
 * used in MergeOp and Sort
 */

__forceinline bool storeEPR(EncPINRef **pep,PINx& qr)
{
#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
	if (qr.epr.lref<sizeof(EncPINRef*) && qr.epr.flags==0)
		{byte *p=(byte*)pep; p[0]=qr.epr.lref<<1|1; memcpy(p+1,qr.epr.buf,qr.epr.lref); return true;}
#endif
	return false;
}

__forceinline void loadEPR(const EncPINRef *ep,PINx &res)
{
#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
	if ((((ptrdiff_t)ep)&1)!=0) {res.epr.flags=0; memcpy(res.epr.buf,(byte*)&ep+1,res.epr.lref=byte((ptrdiff_t)ep)>>1);} else
#endif
	memcpy(&res.epr,ep,ep->trunc<DEFAULT_ALIGN>());
}

__forceinline const Value *getStoredValues(const EncPINRef *ep)
{
	return (const Value*)((byte*)ep+ep->trunc<DEFAULT_ALIGN>());
}

__forceinline EncPINRef *storeValues(const PINx& qr,unsigned nValues,SubAlloc& pinMem)
{
	EncPINRef *ep=(EncPINRef*)pinMem.malloc(qr.epr.trunc<DEFAULT_ALIGN>()+nValues*sizeof(Value));
	if (ep!=NULL) {ep->flags=qr.epr.flags; memcpy(ep->buf,qr.epr.buf,ep->lref=qr.epr.lref);}
	return ep;
}

struct CondEJ;

/**
 * merge sorted streams of PINs
 * equi-join implementation
 */
class MergeOp : public QueryOp
{
	QueryOp	*const		queryOp2;
	const	QUERY_SETOP	op;
	const	CondEJ		*ej;
	const	unsigned	nej;
	const	Expr *const	*conds;
	unsigned			nConds;
	unsigned			didx;
	PINx				pexR;
	PINx				*pR;
	class	PIDStore	*pids;
	PropList			props1;
	unsigned			*index1;
	PropList			props2;
	unsigned			*index2;
	DynArray<EncPINRef*> rvals;
	SubAlloc			rvbuf;
	SubAlloc::SubMark	rvmrk;
	unsigned			nRp;
	unsigned			iRp;
	Value				*pV1;
	Value				*pV2;
	Value				vls[1];
	void				cleanup(Value *pv) {for (unsigned i=0; i<nej; i++) freeV(pv[i]);}
public:
	MergeOp(QueryOp *qop1,QueryOp *qop2,const CondEJ *ce,unsigned ne,QUERY_SETOP qo,const Expr *const *conds,unsigned nConds,unsigned qf);
	void	*operator new(size_t s,MemAlloc *ma,unsigned ne) {return ma->malloc(s+(ne*2-1)*sizeof(Value));}
	virtual	~MergeOp();
	void	connect(PINx **results,unsigned nRes);
	RC		init();
	RC		advance(const PINx *skip=NULL);
	RC		rewind();
	RC		loadData(PINx& qr,Value *pv,unsigned nv,ElementID eid=STORE_COLLECTION_ID,bool fSort=false,MemAlloc *ma=NULL);
	void	unique(bool);
	void	print(SOutCtx& buf,int level) const;
	friend	class	QBuildCtx;
};

/**
 * hash-join
 */
class HashOp : public QueryOp
{
	QueryOp	*const		queryOp2;
	class	PIDStore	*pids;
public:
	HashOp(QueryOp *qop1,QueryOp *qop2) : QueryOp(qop1,QO_JOIN|(qop1->getQFlags()&(QO_IDSORT|QO_ALLPROPS|QO_REVERSIBLE))),queryOp2(qop2),pids(NULL) {sort=qop1->getSort(nSegs); props=qop1->getProps(nProps);}
	virtual	~HashOp();
	RC		init();
	RC		advance(const PINx *skip=NULL);
	void	print(SOutCtx& buf,int level) const;
};

/**
 * nested loop join implementation
 */
class NestedLoop : public QueryOp
{
	QueryOp	*const		queryOp2;
	PINx				pexR;
	PINx				*pR;
	union {
		class	Expr	**conds;
		class	Expr	*cond;
	};
	unsigned			nConds;
public:
	NestedLoop(QueryOp *qop1,QueryOp *qop2,unsigned qf)
		: QueryOp(qop1,qf|QO_JOIN|(qop1->getQFlags()&(QO_IDSORT|QO_ALLPROPS|QO_REVERSIBLE))),queryOp2(qop2),pexR(qx->ses),pR(&pexR),nConds(0)
		{conds=NULL; nOuts+=qop2->getNOuts(); sort=qop1->getSort(nSegs); props=qop1->getProps(nProps);}
	virtual	~NestedLoop();
	void	connect(PINx **results,unsigned nRes);
	RC		init();
	RC		advance(const PINx *skip=NULL);
	RC		rewind();
	void	print(SOutCtx& buf,int level) const;
};

/**
 * property load operator
 */
class LoadOp : public QueryOp
{
	PINx				**results;
	unsigned			nResults;
	const	unsigned	nPls;
	PropList			pls[1];
public:
	LoadOp(QueryOp *q,const PropList *p,unsigned nP,unsigned qf=0);
	virtual		~LoadOp();
	void*		operator new(size_t s,Session *ses,unsigned nP) throw() {return ses->malloc(s+nP*sizeof(PropList));}
	void		connect(PINx **results,unsigned nRes);
	RC			advance(const PINx *skip=NULL);
	RC			count(uint64_t& cnt,unsigned nAbort=~0u);
	RC			rewind();
	RC			loadData(PINx& qr,Value *pv,unsigned nv,ElementID eid=STORE_COLLECTION_ID,bool fSort=false,MemAlloc *ma=NULL);
	void		print(SOutCtx& buf,int level) const;
};

/**
 * filter operator
 */
class Filter : public QueryOp
{
	const Expr *const *conds;
	unsigned		nConds;
	CondIdx			*condIdx;
	unsigned		nCondIdx;
	QueryWithParams	*queries;
	const unsigned	nQueries;
	PINx			**results;
	unsigned		nResults;
public:
	Filter(QueryOp *qop,unsigned nqs,unsigned qf);
	virtual		~Filter();
	void		connect(PINx **results,unsigned nRes);
	RC			advance(const PINx *skip=NULL);
	void		print(SOutCtx& buf,int level) const;
	friend	class	QBuildCtx;
};

/**
 * filter operator based on an array of predefined PIN IDs
 */
class ArrayFilter : public QueryOp
{
	const	Value	*pids;
	unsigned		nPids;
public:
	ArrayFilter(QueryOp *q,const Value *pds,unsigned nP);
	virtual		~ArrayFilter();
	RC			advance(const PINx *skip=NULL);
	void		print(SOutCtx& buf,int level) const;
};

/**
 * sort operator
 * performs in-memory or external sort
 */
class Sort : public QueryOp
{
	const	unsigned		nPreSorted;
	mutable	bool			fRepeat;
	SubAlloc				pinMem;
	uint64_t				nAllPins;
	uint64_t				idx;
	EncPINRef				**pins;
	unsigned				lPins;
	ExtSortFile				*esFile;
	class	InRun			*esRuns;
	class	OutRun			*esOutRun;
	unsigned				nIns;
	unsigned				curRun;
	
	size_t					peakSortMem;
	size_t					maxSortMem;
	size_t					memUsed;

	unsigned				nValues;
	unsigned	*const		index;
	const		unsigned	nPls;
	const		PropList	*pls;
	OrderSegQ				sortSegs[1];

public:
	virtual		~Sort();
	void		connect(PINx **results,unsigned nRes);
	RC			init();
	RC			advance(const PINx *skip=NULL);
	RC			rewind();
	RC			count(uint64_t& cnt,unsigned nAbort=~0u);
	RC			loadData(PINx& qr,Value *pv,unsigned nv,ElementID eid=STORE_COLLECTION_ID,bool fSort=false,MemAlloc *ma=NULL);
	void		unique(bool);
	void		print(SOutCtx& buf,int level) const;
	Sort(QueryOp *qop,const OrderSegQ *os,unsigned nSegs,unsigned qf,unsigned nP,const PropList *pids,unsigned nPids);
	void*		operator new(size_t s,Session *ses,unsigned nSegs,unsigned nPids) throw() {return ses->malloc(s+int(nSegs-1)*sizeof(OrderSegQ)+nPids*sizeof(PropList)+nSegs*sizeof(unsigned));}
	const	Value	*getvalues() const;
private:
	__forceinline void	swap(unsigned i,unsigned j) {EncPINRef *tmp=pins[i]; pins[i]=pins[j]; pins[j]=tmp;}
	RC			sort(unsigned nAbort=~0u);
	void		quickSort(unsigned);
	int			cmp(const EncPINRef *ep1,const EncPINRef *ep2) const;
	RC			writeRun(unsigned nRunPins,size_t&);
	RC          prepMerge();
	RC			esnext(PINx&);
	RC			readRunPage(unsigned runid);
	void		esCleanup();
	friend	class	InRun;
	friend	class	OutRun;
};

/**
 * path expression operator
 */
class PathOp : public QueryOp, public Path
{
	PINx		pex,*ppx;
	PID			saveID;
	EncPINRef	saveEPR;
	void		save() {res->getID(saveID); saveEPR=res->epr;}
public:
	PathOp(QueryOp *qop,const PathSeg *ps,unsigned nSegs,unsigned qf);
	virtual		~PathOp();
	void		connect(PINx **results,unsigned nRes);
	RC			advance(const PINx *skip=NULL);
	RC			rewind();
	void		print(SOutCtx& buf,int level) const;
};

/**
 * transformation operator
 * optionally also performs grouping, aggregation and group filtering
 */
class TransOp : public QueryOp
{
	const	ValueV		*dscr;
	PINx				**ins;
	unsigned			nIns;
	PINx				qr,*pqr;
	PINx				**res;
	unsigned			nRes;
	ValueV				aggs;
	const	OrderSegQ	*groupSeg;
	const	unsigned	nGroup;
	const	Expr		*having;
	AggAcc				*ac;
	EvalCtx				ectx;
public:
	TransOp(QueryOp *q,const ValueV *d,unsigned nD,const ValueV& aggs,const OrderSegQ *gs,unsigned nG,const Expr *hv,unsigned qf);
	TransOp(QCtx *qc,const ValueV *d,unsigned nD,unsigned qf);
	virtual		~TransOp();
	void		connect(PINx **results,unsigned nRes);
	RC			init();
	RC			advance(const PINx *skip=NULL);
	RC			rewind();
	void		print(SOutCtx& buf,int level) const;
};

/**
 * communication operator; invokes external or built-in services
 */
class CommOp : public QueryOp
{
	ServiceCtx	*sctx;
	ValueV		params;
public:
	CommOp(QCtx *s,ServiceCtx *sc,const ValueV& vv,unsigned md) : QueryOp(s,md),sctx(sc) {params=vv;}
	virtual		~CommOp();
	RC			advance(const PINx *skip=NULL);
};

};

#endif
