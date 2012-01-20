/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _QUERYOP_H_
#define _QUERYOP_H_

#include "qbuild.h"
#include "expr.h"
#include "classifier.h"
#include "propdnf.h"

namespace MVStoreKernel
{

#define	DEFAULT_QUERY_MEM	0x100000ul

#define	QO_UNIQUE		0x00000001
#define	QO_VUNIQUE		0x00000002
#define	QO_JOIN			0x00000004
#define	QO_STREAM		0x00000008
#define	QO_IDSORT		0x00000010
#define	QO_REVERSIBLE	0x00000020
#define	QO_FORUPDATE	0x00000040
#define	QO_ALLSETS		0x00000080
#define	QO_CLASS		0x00000100
#define	QO_ALLPROPS		0x00000200
#define	QO_DELETED		0x00000400
#define	QO_CHECKED		0x00000800
#define QO_VCOPIED		0x00001000
#define	QO_REORDER		0x00002000
#define	QO_NODATA		0x00004000
#define	QO_UNI1			0x00008000
#define	QO_UNI2			0x00010000

#define	QST_INIT		0x80000000
#define	QST_BOF			0x40000000
#define	QST_EOF			0x20000000

#define	QOS_ADV			0x0001
#define	QOS_ADV1		0x0001
#define	QOS_ADV2		0x0002
#define	QOS_EOF1		0x0004
#define	QOS_EOF2		0x0008
#define	QOS_ADVN		0x0010
#define	QOS_NEXT		0x0020

struct	CondIdx;
class	ExtSortFile;
class	SOutCtx;

// Base query operator class/interface

class QueryOp
{
	friend		class	QBuildCtx;
	friend		class	SimpleVar;
protected:
	QCtx		*const	qx;
	QueryOp		*const	queryOp;
	ulong				state;
	ulong				nSkip;
	PINEx				*res;
	unsigned			nOuts;
	ulong				qflags;
	const OrderSegQ		*sort;
	ulong				nSegs;
	const PropList		*props;
	ulong				nProps;
	RC					initSkip();
public:
						QueryOp(QCtx *qc,ulong qf);
						QueryOp(QueryOp *qop,ulong qf);
	virtual				~QueryOp();
	virtual	void		connect(PINEx **results,unsigned nRes=1);
	virtual	RC			next(const PINEx *skip=NULL) = 0;
	virtual	RC			rewind();
	virtual	RC			count(uint64_t& cnt,ulong nAbort=~0ul);
	virtual	RC			loadData(PINEx& qr,Value *pv,unsigned nv,ElementID eid=STORE_COLLECTION_ID,bool fSort=false,MemAlloc *ma=NULL);
	virtual	void		unique(bool);
	virtual	void		reverse();
	virtual	void		print(SOutCtx& buf,int level) const = 0;
	void	operator	delete(void *p) {if (p!=NULL) {QCtx *qx=((QueryOp*)p)->qx; qx->ses->free(p); qx->destroy();}}
	void				setSkip(ulong n) {nSkip=n;}
	ulong				getSkip() const {return nSkip;}
	QCtx				*getQCtx() const {return qx;}
	Session				*getSession() const {return qx->ses;}
	unsigned			getNOuts() const {return nOuts;}
	ulong				getQFlags() const {return qflags;}
	const	OrderSegQ	*getSort(ulong& nS) const {nS=nSegs; return sort;}
	const	PropList	*getProps(ulong& nP) const {nP=nProps; return props;}
	RC					getData(PINEx& qr,Value *pv,unsigned nv,const PINEx *qr2=NULL,ElementID eid=STORE_COLLECTION_ID,MemAlloc *ma=NULL);
	RC					getData(PINEx **pqr,unsigned npq,const PropList *pl,unsigned npl,Value *vals,MemAlloc *ma=NULL);
	RC					getBody(PINEx& pe);
};

// Scan operators

class FullScan : public QueryOp
{
	const uint32_t	mask;
	const bool		fClasses;
	PageID			dirPageID;
	PageID			heapPageID;
	ulong			idx;
	ulong			slot;
	SubTx			*stx;
	PageSet::it		*it;
	PBlock			*init();
public:
	FullScan(QCtx *s,uint32_t msk=HOH_DELETED|HOH_HIDDEN,ulong qf=0,bool fCl=false)
		: QueryOp(s,qf|QO_UNIQUE|QO_STREAM|QO_ALLPROPS),mask(msk),fClasses(fCl),dirPageID(INVALID_PAGEID),heapPageID(INVALID_PAGEID),idx(~0u),slot(0),stx(&s->ses->tx),it(NULL) {}
	virtual		~FullScan();
	RC			next(const PINEx *skip=NULL);
	RC			rewind();
	void		print(SOutCtx& buf,int level) const;
};

class ClassScan : public QueryOp
{
	SearchKey		key;
	TreeScan		*scan;
public:
	ClassScan(QCtx *ses,class Class *cls,ulong md);
	virtual		~ClassScan();
	RC			next(const PINEx *skip=NULL);
	RC			rewind();
	RC			count(uint64_t& cnt,ulong nAbort=~0ul);
	void		print(SOutCtx& buf,int level) const;
};

class IndexScan : public QueryOp
{
	ClassIndex&			index;
	const	ClassID		classID;
	ulong				flags;
	ulong				rangeIdx;
	TreeScan			*scan;
	class	PIDStore	*pids;
	PropList			pl;
	const	ulong		nRanges;
	Value				*vals;
	void				initInfo();
	RC					init();
	RC					setScan(ulong=0);
	void				printKey(const SearchKey& key,SOutCtx& buf,const char *def,size_t ldef) const;
public:
	IndexScan(QCtx *ses,ClassIndex& idx,ulong flg,ulong np,ulong md);
	virtual				~IndexScan();
	void				*operator new(size_t s,Session *ses,ulong nRng,ClassIndex& idx) {return ses->malloc(s+nRng*2*sizeof(SearchKey)+idx.getNSegs()*(sizeof(OrderSegQ)+sizeof(PropertyID)));}
	RC					next(const PINEx *skip=NULL);
	RC					rewind();
	RC					count(uint64_t& cnt,ulong nAbort=~0ul);
	RC					loadData(PINEx& qr,Value *pv,unsigned nv,ElementID eid=STORE_COLLECTION_ID,bool fSort=false,MemAlloc *ma=NULL);
	void				unique(bool);
	void				reverse();
	void				print(SOutCtx& buf,int level) const;
	friend	class		SimpleVar;
};

class ArrayScan : public QueryOp
{
	PID				*pids;
	ulong			nPids;
	ulong			idx;
public:
	ArrayScan(QCtx *s,const PID *pds,ulong nP,ulong md);
	void*		operator new(size_t s,Session *ses,ulong nPids) throw() {return ses->malloc(s+nPids*sizeof(PID));}
	virtual		~ArrayScan();
	RC			next(const PINEx *skip=NULL);
	RC			rewind();
	RC			count(uint64_t& cnt,ulong nAbort=~0ul);
	void		print(SOutCtx& buf,int level) const;
};

class FTScan : public QueryOp
{
	SearchKey				word;
	const	ulong			flags;
	const	bool			fStop;
	struct	FTScanS {
		class	TreeScan	*scan;
		ulong				state;
		PID					id;
	}						scans[4];
	ulong					nScans;
	Value					current;
	ulong					nPids;
	PropertyID				pids[1];
public:
	void	*operator new(size_t s,Session *ses,ulong nps,size_t lw) throw() {return ses->malloc(s+lw+(nps==0?0:(nps-1)*sizeof(PropertyID)));}
	FTScan(QCtx *s,const char *w,size_t lW,const PropertyID *pids,ulong nps,ulong md,ulong f,bool fStp);
	virtual		~FTScan();
	RC			next(const PINEx *skip=NULL);
	RC			rewind();
	void		print(SOutCtx& buf,int level) const;
	friend	class	PhraseFlt;
};

class PhraseFlt : public QueryOp
{
	struct	FTScanS {
		class	FTScan		*scan;
		ulong				state;
		PID					id;
	};
	const	ulong			nScans;
	Value					current;
	FTScanS					scans[1];
public:
	PhraseFlt(QCtx *s,FTScan *const *fts,ulong ns,ulong md);
	virtual		~PhraseFlt();
	void		*operator new(size_t s,Session *ses,ulong ns) throw() {return ses->malloc(s+int(ns-1)*sizeof(FTScanS));}
	RC			next(const PINEx *skip=NULL);
	RC			rewind();
	void		print(SOutCtx& buf,int level) const;
};

// Join and set operators

class MergeIDs : public QueryOp
{
protected:
	struct	QueryOpS {
		QueryOp		*qop;
		ulong		state;
		EncPINRef	epr;	
	};
	const	bool	fOr;
	ulong			nOps;
	ulong			cur;
	QueryOpS		ops[1];
public:
	MergeIDs(QCtx *s,QueryOp **o,unsigned no,ulong qf,bool f);
	void	*operator new(size_t s,Session *ses,ulong no) throw() {return ses->malloc(s+int(no-1)*sizeof(QueryOpS));}
	virtual	~MergeIDs();
	void	connect(PINEx **results,unsigned nRes);
	RC		next(const PINEx *skip=NULL);
	RC		rewind();
	RC		loadData(PINEx& qr,Value *pv,unsigned nv,ElementID eid=STORE_COLLECTION_ID,bool fSort=false,MemAlloc *ma=NULL);
	void	print(SOutCtx& buf,int level) const;
	friend	class	QBuildCtx;
};

class MergeOp : public QueryOp
{
	QueryOp	*const		queryOp2;
	const	PropertyID	propID1;
	const	PropertyID	propID2;
	const	QUERY_SETOP	op;
	ulong				mstate;
	ulong				didx;
	Value				vals[3];
	PID					saveID;
	EncPINRef			saveEPR;
	PINEx				pexR;
	PINEx				saveR;
	PINEx				*pR;
	class	PIDStore	*pids;

public:
	MergeOp(QueryOp *qop1,PropertyID pid1,QueryOp *qop2,PropertyID pid2,QUERY_SETOP qo,ulong qf);
	virtual	~MergeOp();
	void	connect(PINEx **results,unsigned nRes);
	RC		next(const PINEx *skip=NULL);
	RC		rewind();
	RC		loadData(PINEx& qr,Value *pv,unsigned nv,ElementID eid=STORE_COLLECTION_ID,bool fSort=false,MemAlloc *ma=NULL);
	void	unique(bool);
	void	print(SOutCtx& buf,int level) const;
	friend	class	QBuildCtx;
};

class HashOp : public QueryOp
{
	QueryOp	*const		queryOp2;
	class	PIDStore	*pids;
public:
	HashOp(QueryOp *qop1,QueryOp *qop2) : QueryOp(qop1,QO_JOIN|(qop1->getQFlags()&(QO_IDSORT|QO_ALLPROPS|QO_REVERSIBLE))),queryOp2(qop2),pids(NULL) {sort=qop1->getSort(nSegs); props=qop1->getProps(nProps);}
	virtual	~HashOp();
	RC		next(const PINEx *skip=NULL);
	void	print(SOutCtx& buf,int level) const;
};

class NestedLoop : public QueryOp
{
	QueryOp	*const		queryOp2;
	PINEx				pexR;
	PINEx				*pR;
	union {
		class	Expr	**conds;
		class	Expr	*cond;
	};
	ulong				nConds;
public:
	NestedLoop(QueryOp *qop1,QueryOp *qop2,ulong qf)
		: QueryOp(qop1,qf|QO_JOIN|(qop1->getQFlags()&(QO_IDSORT|QO_ALLPROPS|QO_REVERSIBLE))),queryOp2(qop2),pexR(qx->ses),pR(&pexR),nConds(0)
		{conds=NULL; nOuts+=qop2->getNOuts(); sort=qop1->getSort(nSegs); props=qop1->getProps(nProps);}
	virtual	~NestedLoop();
	void	connect(PINEx **results,unsigned nRes);
	RC		next(const PINEx *skip=NULL);
	RC		rewind();
	void	print(SOutCtx& buf,int level) const;
};

class LoadOp : public QueryOp
{
	PINEx				**results;
	unsigned			nResults;
	const	unsigned	nPls;
	PropList			pls[1];
public:
	LoadOp(QueryOp *q,const PropList *p,unsigned nP,ulong qf=0);
	virtual		~LoadOp();
	void*		operator new(size_t s,Session *ses,unsigned nP) throw() {return ses->malloc(s+nP*sizeof(PropList));}
	void		connect(PINEx **results,unsigned nRes);
	RC			next(const PINEx *skip=NULL);
	RC			count(uint64_t& cnt,ulong nAbort=~0ul);
	RC			rewind();
	RC			loadData(PINEx& qr,Value *pv,unsigned nv,ElementID eid=STORE_COLLECTION_ID,bool fSort=false,MemAlloc *ma=NULL);
	void		print(SOutCtx& buf,int level) const;
};

class Filter : public QueryOp
{
	union {
		class	Expr	**conds;
		class	Expr	*cond;
	};
	ulong			nConds;
	CondIdx			*condIdx;
	ulong			nCondIdx;
	QueryWithParams	*queries;
	const	ulong	nQueries;
	PINEx			**results;
	unsigned		nResults;
public:
	Filter(QueryOp *qop,ulong nqs,ulong qf);
	virtual		~Filter();
	void		connect(PINEx **results,unsigned nRes);
	RC			next(const PINEx *skip=NULL);
	void		print(SOutCtx& buf,int level) const;
	friend	class	QBuildCtx;
};

class ArrayFilter : public QueryOp
{
	PID			*pids;
	ulong		nPids;
public:
	ArrayFilter(QueryOp *q,const PID *pds,ulong nP);
	void*		operator	new(size_t s,ulong nPids,Session *ses) throw() {return ses->malloc(s+nPids*sizeof(PID));}
	virtual		~ArrayFilter();
	RC			next(const PINEx *skip=NULL);
	void		print(SOutCtx& buf,int level) const;
};

class Sort : public QueryOp
{
	const	unsigned		nPreSorted;
	mutable	bool			fRepeat;
	SubAlloc				pinMem;
	uint64_t				nAllPins;
	uint64_t				idx;
	EncPINRef				**pins;
	ulong					lPins;
	ExtSortFile				*esFile;
	class	InRun			*esRuns;
	class	OutRun			*esOutRun;
	ulong					nIns;
	ulong					curRun;
	
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
	void		connect(PINEx **results,unsigned nRes);
	RC			next(const PINEx *skip=NULL);
	RC			rewind();
	RC			count(uint64_t& cnt,ulong nAbort=~0ul);
	RC			loadData(PINEx& qr,Value *pv,unsigned nv,ElementID eid=STORE_COLLECTION_ID,bool fSort=false,MemAlloc *ma=NULL);
	void		unique(bool);
	void		print(SOutCtx& buf,int level) const;
	Sort(QueryOp *qop,const OrderSegQ *os,unsigned nSegs,ulong qf,unsigned nP,const PropList *pids,unsigned nPids);
	void*		operator new(size_t s,Session *ses,unsigned nSegs,unsigned nPids) throw() {return ses->malloc(s+int(nSegs-1)*sizeof(OrderSegQ)+nPids*sizeof(PropList)+nSegs*sizeof(unsigned));}
	const	Value	*getvalues() const;
private:
	__forceinline void	swap(unsigned i,unsigned j) {EncPINRef *tmp=pins[i]; pins[i]=pins[j]; pins[j]=tmp;}
	__forceinline EncPINRef *storeSortData(const PINEx& qr,SubAlloc& pinMem);
	RC			sort(ulong nAbort=~0u);
	void		quickSort(ulong);
	int			cmp(const EncPINRef *ep1,const EncPINRef *ep2) const;
	RC			writeRun(ulong nRunPins,size_t&);
	RC          prepMerge();
	RC			esnext(PINEx&);
	RC			readRunPage(ulong runid);
	void		esCleanup();
	friend	class	InRun;
	friend	class	OutRun;
};

class PathOp : public QueryOp, public Path
{
	PINEx			pex,*ppx;
	PID				saveID;
	EncPINRef		saveEPR;
	void			save() {res->getID(saveID); saveEPR=res->epr;}
public:
	PathOp(QueryOp *qop,const PathSeg *ps,unsigned nSegs,unsigned qf);
	virtual		~PathOp();
	void		connect(PINEx **results,unsigned nRes);
	RC			next(const PINEx *skip=NULL);
	RC			rewind();
	void		print(SOutCtx& buf,int level) const;
};

class TransOp : public QueryOp
{
	const	ValueV		*dscr;
	PINEx				**ins;
	unsigned			nIns;
	PINEx				qr,*pqr;
	PINEx				**res;
	unsigned			nRes;
	ValueV				aggs;
	const	OrderSegQ	*groupSeg;
	const	unsigned	nGroup;
	const	Expr		*having;
	AggAcc				*ac;
public:
	TransOp(QueryOp *q,const ValueV *d,unsigned nD,const ValueV& aggs,const OrderSegQ *gs,unsigned nG,const Expr *hv,ulong qf);
	TransOp(QCtx *qc,const ValueV *d,unsigned nD,ulong qf);
	virtual		~TransOp();
	void		connect(PINEx **results,unsigned nRes);
	RC			next(const PINEx *skip=NULL);
	RC			rewind();
	void		print(SOutCtx& buf,int level) const;
};

};

#endif
