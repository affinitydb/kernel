/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _QUERYPRC_H_
#define _QUERYPRC_H_

#include "qbuild.h"
#include "queryop.h"
#include "txmgr.h"
#include "lock.h"

class IStoreNotification;

namespace MVStoreKernel
{
/**
 * getBody() flags
 */
#define	GB_DELETED			0x0001
#define	GB_REREAD			0x0002
#define	GB_FORWARD			0x0004
#define	GB_SAFE				0x0008

/**
 * PIN skew calculation constants
 */
#define	SKEW_PAGE_PCT			10
#define	SKEW_FACTOR				2

#define	ARRAY_THRESHOLD			256
#define STRING_THRESHOLD		64
#define	QUERY_ARRAY_THRESHOLD	10

struct ChangeInfo
{
	PID				id;
	PID				docID;
	const Value		*oldV;
	const Value		*newV;
	PropertyID		propID;
	ElementID		eid;
};

#define	PM_PROCESSED	0x00000001
#define PM_COLLECTION	0x00000002
#define	PM_FTINDEXABLE	0x00000004
#define	PM_EXPAND		0x00000008
#define	PM_ARRAY		0x00000010
#define	PM_SSV			0x00000020
#define	PM_ESTREAM		0x00000040
#define	PM_BIGC			0x00000080
#define	PM_FORCESSV		0x00000100
#define	PM_NEWCOLL		0x00000200
#define	PM_SPILL		0x00000400
#define	PM_BCCAND		0x00000800
#define	PM_MOVE			0x00001000
#define	PM_NEWPROP		0x00002000
#define	PM_SWORDS		0x00004000
#define	PM_PUTOLD		0x00008000
#define	PM_LOCAL		0x00010000
#define	PM_INVALID		0x00020000
#define	PM_COMPACTREF	0x00040000
#define	PM_RESET		0x00080000
#define	PM_SCOLL		0x00100000
#define	PM_OLDFTINDEX	0x00200000
#define	PM_NEWVALUES	0x00400000
#define	PM_OLDVALUES	0x00800000

struct PropInfo
{
	PropertyID					propID;
	const HeapPageMgr::HeapV	*hprop;
	struct	ModInfo				*first;
	struct	ModInfo				*last;
	ulong						nElts;
	ulong						flags;
	ulong						maxKey;
	long						delta;
	long						nDelta;
	class	Collection			*pcol;
	class PropInfoCmp {public: __forceinline static int	cmp(const PropInfo *pi,PropertyID pid) {return cmp3(pi->propID,pid);}};
};

struct ModInfo
{
	ModInfo			*next;
	ModInfo			*pnext;
	const Value		*pv;
	unsigned		pvIdx;
	Value			*newV;
	Value			*oldV;
	ulong			flags;
	ElementID		eid;
	ElementID		eltKey;
	ElementID		epos;
	PropInfo		*pInfo;
	HRefSSV			*href;
};

class Cursor;
class Stmt;

struct CandidateSSV
{
	PropertyID		pid;
	const	Value	*pv;
	struct	ModInfo	*mi;
	size_t			length;
	size_t			dlen;
};

struct CandidateSSVs
{
	CandidateSSV	*candidates;
	ulong			nCandidates;
	ulong			xCandidates;
	CandidateSSVs() : candidates(NULL),nCandidates(0),xCandidates(0) {}
	~CandidateSSVs() {free(candidates,SES_HEAP);}
	RC	insert(const Value *pv,PropertyID pid,size_t length,struct ModInfo *mi,size_t dl) {
		if (nCandidates>=xCandidates) {
			candidates=(CandidateSSV*)(candidates==NULL?malloc((xCandidates=100)*sizeof(CandidateSSV),SES_HEAP):
								realloc(candidates,(xCandidates+=xCandidates)*sizeof(CandidateSSV),SES_HEAP));
			if (candidates==NULL) return RC_NORESOURCES;
		}
		CandidateSSV *ps=&candidates[nCandidates++]; assert(length>sizeof(HRefSSV));
		ps->pid=pid; ps->length=length; ps->pv=pv; ps->mi=mi; ps->dlen=dl; return RC_OK;
	}
};

struct RefTrace
{
	const	RefTrace	*next;
	PID					id;
	PropertyID			pid;
	ElementID			eid;
};

class Cursor;

class QueryPrc
{
	RC		loadV(Value& v,ulong pid,const PINEx& cb,ulong mode,MemAlloc *ma,ulong eid=STORE_COLLECTION_ID);
	RC		loadVH(Value& v,const HeapPageMgr::HeapV *hprop,const PINEx& cb,ulong mode,MemAlloc *ma,ulong eid=STORE_COLLECTION_ID);
	RC		loadVTx(Value& v,const HeapPageMgr::HeapV *hprop,const PINEx& cb,ulong mode,MemAlloc *ma,ulong eid=STORE_COLLECTION_ID);
	RC		loadV(Value& v,HType ty,PageOff offset,const HeapPageMgr::HeapPage *frame,ulong mode,MemAlloc *ma,ulong eid=STORE_COLLECTION_ID);
	RC		loadSSVs(Value *values,unsigned nValues,unsigned mode,Session *ses,MemAlloc *ma);
	RC		loadSSV(Value& val,ValueType ty,const HeapPageMgr::HeapObjHeader *hobj,unsigned mode,MemAlloc *ma);

	RC		getBody(PINEx& cb,TVOp tvo=TVO_READ,ulong flags=0,VersionID=STORE_CURRENT_VERSION);
	bool	checkRef(const Value& val,PIN *const *pins,unsigned nPins);
	RC		makeRoom(PIN *pin,ushort lxtab,PBlock *pb,Session *ses,size_t reserve);
	RC		rename(ChangeInfo& inf,PropertyID pid,ulong flags,bool fSync);
	bool	condSatisfied(const class Expr *const *exprs,ulong nExp,const PINEx **vars,unsigned nVars,const Value *pars,unsigned nPars,MemAlloc *ma,bool fIgnore=false);
	RC		checkLockAndACL(PINEx& qr,TVOp tvo,QueryOp *qop=NULL);
	RC		checkACLs(PINEx& cb,IdentityID iid,TVOp tvo,ulong flags=0,bool fProp=true);
	RC		checkACLs(const PINEx *pin,PINEx& cb,IdentityID iid,TVOp tvo,ulong flags=0,bool fProp=true);
	RC		checkACL(const Value&,PINEx&,IdentityID,uint8_t,const RefTrace*,bool=true);
	RC		getRefValues(const PID& id,Value *&vals,ulong& nValues,ulong mode,PINEx& cb);
	RC		apply(Session *ses,STMT_OP op,PINEx& qr,const Value *values,unsigned nValues,unsigned mode,PIN *pin=NULL,const Value *params=NULL,unsigned nParams=0);
	RC		count(QueryOp *qop,uint64_t& cnt,unsigned long nAbort,const OrderSegQ *os=NULL,unsigned nos=0);
	RC		eval(Session *ses,const Value *pv,Value& res,const PINEx **vars,ulong nVars,const Value *params,unsigned nParams,MemAlloc *ma,bool fInsert);

	size_t	splitLength(const Value *pv);
	RC		estimateLength(const Value& v,size_t& res,ulong mode,size_t threshold,MemAlloc *ma,PageID pageID=INVALID_PAGEID,size_t *rlen=NULL);
	RC		findCandidateSSVs(struct CandidateSSVs& cs,const Value *pv,ulong nv,bool fSplit,MemAlloc *ma,const AllocCtrl *act=NULL,PropertyID=STORE_INVALID_PROPID,struct ModInfo *mi=NULL);
	RC		persistValue(const Value& v,ushort& sht,HType& vt,ushort& offs,byte *buf,size_t *plrec,const PageAddr &addr);
	RC		putHeapMod(HeapPageMgr::HeapPropMod *hpm,struct ModInfo *pm,byte *buf,ushort& sht,const PINEx&,bool=false);
	static	int __cdecl cmpCandidateSSV(const void *v1, const void *v2);

private:
	IStoreNotification	*const	notification;
	StoreCtx			*const	ctx;
	size_t				bigThreshold;
public:
	QueryPrc(StoreCtx *,IStoreNotification *notItf);

	RC		loadPIN(Session *ses,const PID& id,PIN *&pin,unsigned mode=0,PINEx *pcb=NULL,MemAlloc *ma=NULL,VersionID=STORE_CURRENT_VERSION);
	RC		loadValue(Session *ses,const PID& id,PropertyID pid,ElementID eid,Value& res,ulong mode=0);
	RC		loadValues(Value *pv,unsigned nv,const PID& id,Session *ses,ulong mode=0);
	RC		getPINValue(const PID& id,Value& res,ulong mode,Session *ses);
	RC		diffPIN(const PIN *pin,PINEx& cb,Value *&diffProps,ulong& nDiffProps,Session *ses);
	RC		modifyPIN(Session *ses,const PID& id,const Value *v,unsigned nv,PINEx *pcb,PIN *pin=NULL,unsigned mode=0,const ElementID *eids=NULL,unsigned* =NULL,const Value *params=NULL,unsigned nParams=0);
	RC		commitPINs(Session *ses,PIN *const *pins,unsigned nPins,unsigned mode,const AllocCtrl *actrl=NULL,size_t *pSize=NULL,const Value *params=NULL,unsigned nParams=0);
	RC		deletePINs(Session *ses,const PIN *const *pins,const PID *pids,unsigned nPins,unsigned mode,PINEx *pcb=NULL);
	RC		undeletePINs(Session *ses,const PID *pids,unsigned nPins);
	RC		setFlag(Session *ses,const PID& id,PageAddr *addr,ushort flag,bool fReset);
	RC		loadData(const PageAddr& addr,byte *&p,size_t& len,MemAlloc *ma);
	RC		persistData(IStream *stream,const byte *str,size_t lstr,PageAddr& addr,uint64_t&,const PageAddr* =NULL,PBlockP* =NULL);
	RC		editData(Session *ses,PageAddr &addr,uint64_t& length,const Value&,PBlockP *pbp=NULL,byte *pOld=NULL);
	RC		deleteData(const PageAddr& addr,PBlockP *pbp=NULL);
	bool	test(const PINEx *,ClassID,const Value *params=NULL,unsigned nParams=0,bool fIgnore=false);
	RC		transform(const PINEx **vars,ulong nVars,PIN **pins,unsigned nPins,unsigned &nOut,Session*) const;
	RC		getClassInfo(Session *ses,PIN *pin);

	static	ulong	findPropID(PropertyID *pids,ulong nPids,PropertyID pid) {
					if (pids==NULL||nPids==0) return 0;
					for (ulong n=nPids,idx=0;;) {
						ulong k=n>>1; PropertyID q=pids[idx+k]; if (q==pid) return idx+k;
						if (q<pid) {idx+=k+1; if ((n-=k+1)==0) return idx;} else if ((n=k)==0) return idx;
					}}
	static	bool	merge(PropertyID *&pids,unsigned& nPids,PropertyID *&p2,unsigned np2,MemAlloc *ma) {
					bool rc=true; PropertyID pid=STORE_INVALID_PROPID;
					if (pids==NULL) {pids=p2; nPids=np2;} else if (p2!=NULL) {for (unsigned i=0,j=0; j<np2;) {
						if (i>=nPids || (pid=pids[i])>p2[j]) {
							unsigned l=j; if (i>=nPids) j=np2; else do j++; while (j<np2 && p2[j]<pid); l=j-l;
							if ((pids=(PropertyID*)ma->realloc(pids,(nPids+l)*sizeof(PropertyID)))==NULL) {rc=false; break;}
							if (i<nPids) memmove(&pids[i+l],&pids[i],(nPids-i)*sizeof(PropertyID));
							memcpy(&pids[i],&p2[j-l],l*sizeof(PropertyID)); i+=l; nPids+=l;
						} else {++i; if (p2[j]==pid) ++j;}
					} ma->free(p2);} p2=NULL; return rc;}

	friend	class	Expr;
	friend	class	Stmt;
	friend	class	Cursor;
	friend	class	Navigator;
	friend	class	PINEx;
	friend	class	FTIndexMgr;
	friend	struct	ModData;
	friend	class	Collection;
	friend	class	ClassPropIndex;
	friend	class	Classifier;
	friend	class	Class;
	friend	class	SyncStreamIn;
	friend	class	ServerStreamIn;
	friend	class	PathOp;
	friend	class	QueryOp;
	friend	class	MergeOp;
	friend	class	LoopJoin;
	friend	class	Filter;
	friend	class	BodyOp;
	friend	class	TransOp;
	friend	class	PathOp;
	friend	class	Sort;
};

};

#endif
