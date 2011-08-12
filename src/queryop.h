/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _QUERYOP_H_
#define _QUERYOP_H_

#include "mvstoreimpl.h"
#include "classifier.h"
#include "session.h"
#include "propdnf.h"
#include "pinex.h"

namespace MVStoreKernel
{

#define	DEFAULT_QUERY_MEM	0x100000ul

#define	QO_UNIQUE		0x00000001
#define	QO_JOIN			0x00000002
#define	QO_STREAM		0x00000004
#define	QO_TOPMOST		0x00000008
#define	QO_PIDSORT		0x00000010
#define	QO_REVERSIBLE	0x00000020
#define	QO_FORUPDATE	0x00000040
#define	QO_ALLSETS		0x00000080
#define	QO_CLASS		0x00000100
#define	QO_DEGREE		0x00000200
#define	QO_PREFIX		0x00000400
#define	QO_ALLPROPS		0x00000800
#define	QO_DELETED		0x00001000
#define	QO_CHECKED		0x00002000
#define QO_VCOPIED		0x00004000
#define	QO_REORDER		0x00008000
#define	QO_NODATA		0x00010000
#define	QO_VUNIQUE		0x00020000

#define	QST_INIT		0x8000
#define	QST_BOF			0x4000
#define	QST_EOF			0x2000
#define	QST_CURRENT		0x1000

#define	QOS_ADV			0x0001
#define	QOS_ADV1		0x0001
#define	QOS_ADV2		0x0002
#define	QOS_EOF			0x0004
#define	QOS_EOF1		0x0004
#define	QOS_BEG			0x0008
#define	QOS_EOF2		0x0008
#define	QOS_UNI1		0x0010
#define	QOS_UNI2		0x0020
#define	QOS_ADVN		0x0040
#define	QOS_NEXT		0x0080
#define	QOS_FIRST		0x0100

class ExtSortFile;
class SOutCtx;

// Base query operator class/interface

struct QODescr {
	ulong				flags;
	ulong				nIn;
	ulong				nOut;
	ulong				level;
	const OrderSegQ		*sort;
	ulong				nSegs;
	const PropertyID	*props;
	ulong				nProps;
	QODescr() : flags(0),nIn(0),nOut(1),level(0),sort(NULL),nSegs(0),props(NULL),nProps(0) {}
};

class ReleaseLatches
{
public: 
	virtual RC release()=0;
};

class QueryOp : public ReleaseLatches
{
	friend		class	QueryCtx;
protected:
	Session		*const	ses;
	QueryOp		*const	queryOp;
	ulong				mode;
	ulong				state;
	ulong				nSkip;
	RC					initSkip();
public:
						QueryOp(Session *ses,QueryOp *qop,ulong mode);
	virtual				~QueryOp();
	virtual	RC			next(PINEx&,const PINEx *skip=NULL) = 0;
	virtual	RC			rewind();
	virtual	RC			count(uint64_t& cnt,ulong nAbort=~0ul);
	virtual	RC			loadData(PINEx& qr,Value *pv,unsigned nv,MemAlloc *ma=NULL,ElementID eid=STORE_COLLECTION_ID);
	virtual	void		unique(bool);
	virtual	void		reorder(bool);
	virtual	void		getOpDescr(QODescr&);
	virtual	void		print(SOutCtx& buf,int level) const = 0;
	virtual	RC			release();
	void	operator	delete(void *p) {if (p!=NULL) ((QueryOp*)p)->ses->free(p);}
	void				setSkip(ulong n) {nSkip=n;}
	ulong				getSkip() const {return nSkip;}
	Session				*getSession() const {return ses;}
	RC					getData(PINEx& qr,Value *pv,unsigned nv,const PINEx *qr2=NULL,MemAlloc *ma=NULL,ElementID eid=STORE_COLLECTION_ID);
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
	PBlock			*init(bool& f,bool fLast=false);
public:
	FullScan(Session *s,uint32_t msk=HOH_DELETED|HOH_HIDDEN,ulong md=0,bool fCl=false)
		: QueryOp(s,NULL,md),mask(msk),fClasses(fCl),dirPageID(INVALID_PAGEID),heapPageID(INVALID_PAGEID),idx(~0u),slot(0) {}
	virtual		~FullScan();
	RC			next(PINEx&,const PINEx *skip=NULL);
	RC			rewind();
	void		getOpDescr(QODescr&);
	void		print(SOutCtx& buf,int level) const;
};

class ClassScan : public QueryOp
{
	SearchKey		key;
	TreeScan		*scan;
public:
	ClassScan(Session *ses,class Class *cls,ulong md);
	virtual		~ClassScan();
	RC			next(PINEx&,const PINEx *skip=NULL);
	RC			rewind();
	RC			count(uint64_t& cnt,ulong nAbort=~0ul);
	void		getOpDescr(QODescr&);
	void		print(SOutCtx& buf,int level) const;
	RC			release();
};

class IndexScan : public QueryOp
{
	ClassIndex&			index;
	const	ClassID		classID;
	ulong				flags;
	ulong				rangeIdx;
	TreeScan			*scan;
	class	PIDStore	*pids;
	OrderSegQ			*segs;
	PropertyID			*props;
	ulong				nProps;
	const	ulong		nRanges;
	Value				*vals;
	RC					init(bool=true);
	RC					setScan(ulong=0);
	void				printKey(const SearchKey& key,SOutCtx& buf,const char *def,size_t ldef) const;
public:
	IndexScan(Session *ses,ClassIndex& idx,ulong flg,ulong np,ulong md);
	virtual				~IndexScan();
	void				*operator new(size_t s,Session *ses,ulong nRng,ClassIndex& idx) {return ses->malloc(s+nRng*2*sizeof(SearchKey)+idx.getNSegs()*(sizeof(OrderSegQ)+sizeof(PropertyID)));}
	RC					next(PINEx&,const PINEx *skip=NULL);
	RC					rewind();
	RC					count(uint64_t& cnt,ulong nAbort=~0ul);
	RC					loadData(PINEx& qr,Value *pv,unsigned nv,MemAlloc *ma=NULL,ElementID eid=STORE_COLLECTION_ID);
	void				unique(bool);
	void				getOpDescr(QODescr&);
	void				print(SOutCtx& buf,int level) const;
	RC					release();
	friend	class		SimpleVar;
};

class ArrayScan : public QueryOp
{
	PID			*pids;
	ulong		nPids;
	ulong		idx;
public:
	ArrayScan(Session *ses,const PID *pds,ulong nP,ulong md);
	void*		operator new(size_t s,Session *ses,ulong nPids) throw() {return ses->malloc(s+nPids*sizeof(PID));}
	virtual		~ArrayScan();
	RC			next(PINEx&,const PINEx *skip=NULL);
	RC			rewind();
	RC			count(uint64_t& cnt,ulong nAbort=~0ul);
	void		getOpDescr(QODescr&);
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
	FTScan(Session *ses,const char *w,size_t lW,const PropertyID *pids,ulong nps,ulong md,ulong f,bool fStp);
	virtual		~FTScan();
	RC			next(PINEx&,const PINEx *skip=NULL);
	RC			rewind();
	void		getOpDescr(QODescr&);
	void		print(SOutCtx& buf,int level) const;
	RC			release();
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
	PhraseFlt(Session *ses,FTScan *const *fts,ulong ns,ulong md);
	virtual		~PhraseFlt();
	void		*operator new(size_t s,Session *ses,ulong ns) throw() {return ses->malloc(s+int(ns-1)*sizeof(FTScanS));}
	RC			next(PINEx&,const PINEx *skip=NULL);
	RC			rewind();
	void		getOpDescr(QODescr&);
	void		print(SOutCtx& buf,int level) const;
	RC			release();
};

// Join and set operators

class MergeIDOp : public QueryOp
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
	MergeIDOp(Session *s,ulong md,bool f) : QueryOp(s,NULL,md|QO_UNIQUE),fOr(f),nOps(0),cur(~0u) {}
	void	*operator new(size_t s,Session *ses,ulong no) throw() {return ses->malloc(s+int(no-1)*sizeof(QueryOpS));}
	virtual	~MergeIDOp();
	RC		next(PINEx&,const PINEx *skip=NULL);
	RC		rewind();
	RC		loadData(PINEx& qr,Value *pv,unsigned nv,MemAlloc *ma=NULL,ElementID eid=STORE_COLLECTION_ID);
	void	reorder(bool);
	void	getOpDescr(QODescr&);
	void	print(SOutCtx& buf,int level) const;
	RC		release();
	friend	class	QueryCtx;
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
	PINEx				saveR;
	PINEx				nextR;
	class	PIDStore	*pids;

public:
	MergeOp(Session *s,QueryOp *qop1,PropertyID pid1,QueryOp *qop2,PropertyID pid2,QUERY_SETOP qo,ulong md)
		: QueryOp(s,qop1,md|QO_UNIQUE),queryOp2(qop2),propID1(pid1),propID2(pid2),op(qo),mstate(0),didx(0),saveR(s),nextR(s),pids(NULL)
		{vals[0].setError(),vals[1].setError(),vals[2].setError();}
	virtual	~MergeOp();
	RC		next(PINEx&,const PINEx *skip=NULL);
	RC		rewind();
	RC		loadData(PINEx& qr,Value *pv,unsigned nv,MemAlloc *ma=NULL,ElementID eid=STORE_COLLECTION_ID);
	void	unique(bool);
	void	reorder(bool);
	void	getOpDescr(QODescr&);
	void	print(SOutCtx& buf,int level) const;
	RC		release();
	friend	class	QueryCtx;
};

class HashOp : public QueryOp
{
	QueryOp	*const		queryOp2;
	class	PIDStore	*pids;
public:
	HashOp(Session *s,QueryOp *qop1,QueryOp *qop2) : QueryOp(s,qop1,0),queryOp2(qop2),pids(NULL) {}
	virtual	~HashOp();
	RC		next(PINEx&,const PINEx *skip=NULL);
	void	getOpDescr(QODescr&);
	void	reorder(bool);
	void	print(SOutCtx& buf,int level) const;
	RC		release();
};

class LoopJoin : public QueryOp
{
	QueryOp	*const		queryOp2;
	const	QUERY_SETOP	joinType;
	const	ulong		nPropIDs1;
	const	ulong		nPropIDs2;
	ulong				lstate;
	union {
		class	Expr	**conds;
		class	Expr	*cond;
	};
	ulong				nConds;
	PropertyID			condProps[1];
	LoopJoin(Session *s,QueryOp *qop1,QueryOp *qop2,const PropertyID *p1,ulong np1,const PropertyID *p2,ulong np2,QUERY_SETOP jt,ulong mode)
		: QueryOp(s,qop1,mode|QO_UNIQUE),queryOp2(qop2),joinType(jt),nPropIDs1(np1),nPropIDs2(np2),lstate(0),nConds(0)
		{conds=NULL; memcpy(condProps,p1,np1*sizeof(PropertyID)); memcpy(condProps+np1,p2,np2*sizeof(PropertyID));}
	void* operator new(size_t s,Session *ses,ulong nCondProps) throw() {return ses->malloc(s+int(nCondProps-1)*sizeof(PropertyID));}
public:
	virtual	~LoopJoin();
	RC		next(PINEx&,const PINEx *skip=NULL);
	RC		rewind();
	void	getOpDescr(QODescr&);
	void	print(SOutCtx& buf,int level) const;
	RC		release();
	static	RC	create(Session *ses,QueryOp* &res,QueryOp *qop1,QueryOp *qop2,const Expr *const *c,ulong nc,
			const PropertyID *pids1,ulong nPids1,const PropertyID *pids2,ulong nPids2,QUERY_SETOP jt,ulong md);
};

class BodyOp : public QueryOp
{
	const	ulong		nProps;
	PropertyID			props[1];
public:
	BodyOp(Session *s,QueryOp *q,const PropertyID *pps,ulong nP,ulong mode=0) : QueryOp(s,q,mode),nProps(nP)
											{if (pps!=NULL && nP!=0) memcpy((PropertyID*)props,pps,nP*sizeof(PropertyID));}
	void*		operator new(size_t s,Session *ses,unsigned nProps) throw() {return ses->malloc(s+nProps*sizeof(PropertyID));}
	RC			next(PINEx&,const PINEx *skip=NULL);
	RC			rewind();
	RC			loadData(PINEx& qr,Value *pv,unsigned nv,MemAlloc *ma=NULL,ElementID eid=STORE_COLLECTION_ID);
	void		reorder(bool);
	void		print(SOutCtx& buf,int level) const;
};

class Filter : public QueryOp
{
	Value					*params;
	const	ulong			nParams;
	union {
		class	Expr		**conds;
		class	Expr		*cond;
	};
	ulong					nConds;
	struct	CondIdx			*condIdx;
	ulong					nCondIdx;
	struct	QueryWithParams	*queries;
	const	ulong			nQueries;
	const	size_t			lProps;
	PropDNF					condProps;
public:
	Filter(Session *ses,QueryOp *qop,ulong nPars,size_t lp,ulong nqs,ulong md);
	void*		operator new(size_t s,Session *ses,size_t lp) throw() {return ses->malloc(s+lp);}
	virtual		~Filter();
	RC			next(PINEx&,const PINEx *skip=NULL);
	void		print(SOutCtx& buf,int level) const;
	friend	class	QueryCtx;
};

class ArrayFilter : public QueryOp
{
	PID			*pids;
	ulong		nPids;
public:
	ArrayFilter(Session *ses,QueryOp *q,const PID *pds,ulong nP);
	void*		operator	new(size_t s,ulong nPids,Session *ses) throw() {return ses->malloc(s+nPids*sizeof(PID));}
	virtual		~ArrayFilter();
	RC			next(PINEx&,const PINEx *skip=NULL);
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
	const		unsigned	nPropIDs;
	const		PropertyID	*propIDs;
	const		unsigned	nSegs;
	OrderSegQ				sortSegs[1];

public:
	virtual		~Sort();
	RC			next(PINEx&,const PINEx *skip=NULL);
	RC			rewind();
	RC			count(uint64_t& cnt,ulong nAbort=~0ul);
	RC			loadData(PINEx& qr,Value *pv,unsigned nv,MemAlloc *ma=NULL,ElementID eid=STORE_COLLECTION_ID);
	void		unique(bool);
	void		reorder(bool);
	void		getOpDescr(QODescr&);
	void		print(SOutCtx& buf,int level) const;
	Sort(QueryOp *qop,const OrderSegQ *os,unsigned nSegs,ulong md,unsigned nP,const PropertyID *pids,unsigned nPids);
	void*		operator new(size_t s,Session *ses,unsigned nSegs,unsigned nProps) throw() {return ses->malloc(s+int(nSegs-1)*sizeof(OrderSegQ)+nProps*sizeof(PropertyID)+nSegs*sizeof(unsigned));}
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

class PathOp : public QueryOp
{
	const	PathSeg	*const	path;
	const	unsigned		nPathSeg;
	const	bool			fCopied;
	unsigned				idx;
	unsigned				rep;
	IntNav					*nav;
	Value					*arr;
	unsigned				lArr;
	unsigned				cidx;
	unsigned				pstate;
	PINEx					pex;
	Value					*vals;
	unsigned				nvals;
	PropertyList			props[1];
public:
	PathOp(Session *s,QueryOp *qop,const PathSeg *ps,unsigned nSegs,bool fC) : QueryOp(s,qop,0),path(ps),nPathSeg(nSegs),fCopied(fC),
				idx(0),rep(0),nav(NULL),arr(NULL),lArr(0),cidx(0),pstate(0),pex(s),vals(NULL),nvals(0) {memset(props,0,nPathSeg*sizeof(PropertyList));}
	virtual		~PathOp();
	void		*operator new(size_t s,Session *ses,unsigned nSegs) {return ses->malloc(s+int(nSegs-1)*sizeof(PropertyList));}
	RC			next(PINEx&,const PINEx *skip=NULL);
	RC			rewind();
	void		getOpDescr(QODescr&);
	void		print(SOutCtx& buf,int level) const;
	RC			release();
};

class GroupOp : public QueryOp
{
public:
	GroupOp(Session *s,QueryOp *q,unsigned nGroup,const Value *trs,unsigned nTrs,ulong mode) : QueryOp(s,q,mode) {}
	virtual		~GroupOp();
	void*		operator new(size_t s,Session *ses,unsigned nG,unsigned nTrs) throw() {return ses->malloc(s+nG*sizeof(OrderSegQ)+nTrs*sizeof(Value));}
	RC			next(PINEx&,const PINEx *skip=NULL);
	RC			rewind();
	void		getOpDescr(QODescr&);
	void		print(SOutCtx& buf,int level) const;
};

class TransOp : public QueryOp
{
	const	TDescriptor	*dscr;
	const	unsigned	nDscr;
	const	bool		fCopied;
public:
	TransOp(Session *s,QueryOp *q,const TDescriptor *td,unsigned nT,ulong mode,bool fC) : QueryOp(s,q,mode),dscr(td),nDscr(nT),fCopied(fC) {}
	virtual		~TransOp();
	RC			next(PINEx&,const PINEx *skip=NULL);
	void		getOpDescr(QODescr&);
	void		print(SOutCtx& buf,int level) const;
};

};

#endif
