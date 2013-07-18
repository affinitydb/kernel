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
 * PIN loading and modification routines
 */
#ifndef _QUERYPRC_H_
#define _QUERYPRC_H_

#include "qbuild.h"
#include "queryop.h"
#include "txmgr.h"
#include "lock.h"

class IStoreNotification;

namespace AfyKernel
{
/**
 * getBody() flags
 */
#define	GB_DELETED			0x0001		/**< get soft-deleted PIN */
#define	GB_REREAD			0x0002		/**< re-read page, don't re-lock */
#define	GB_FORWARD			0x0004		/**< don't resolve FORWARD records */

/**
 * PIN skew calculation constants
 */
#define	SKEW_PAGE_PCT			10
#define	SKEW_FACTOR				2

#define	ARRAY_THRESHOLD			256		/**< threshold of 'big' collections */
#define STRING_THRESHOLD		64		/**< string length threshold for SSV data */

/**
 * bit flags for eval()
 */
#define EV_PID					0x0001
#define	EV_COPY					0x0002
#define	EV_ASYNC				0x0004

/**
 * passed to FT indexer
 */
struct ChangeInfo
{
	PID				id;
	PID				docID;
	const Value		*oldV;
	const Value		*newV;
	PropertyID		propID;
	ElementID		eid;
};

/**
 * bit flags used in modifyPIN()
 */
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
#define	PM_PUTOLD		0x00008000
#define	PM_LOCAL		0x00010000
#define	PM_INVALID		0x00020000
#define	PM_COMPACTREF	0x00040000
#define	PM_RESET		0x00080000
#define	PM_SCOLL		0x00100000
#define	PM_OLDFTINDEX	0x00200000
#define	PM_NEWVALUES	0x00400000
#define	PM_OLDVALUES	0x00800000
#define	PM_CALCULATED	0x01000000
#define	PM_GENEIDS		0x02000000

/**
 * property information used in modifyPIN()
 */
struct PropInfo
{
	PropertyID					propID;
	const HeapPageMgr::HeapV	*hprop;
	struct	ModInfo				*first;
	struct	ModInfo				*last;
	unsigned					nElts;
	unsigned					flags;
	ElementID					maxKey;
	ElementID					single;
	long						delta;
	long						nDelta;
	class	Collection			*pcol;
	class PropInfoCmp {public: __forceinline static int	cmp(const PropInfo *pi,PropertyID pid) {return cmp3(pi->propID,pid);}};
};

/**
 * individual modification information in modifyPIN()
 */
struct ModInfo
{
	ModInfo			*next;
	ModInfo			*pnext;
	const Value		*pv;
	unsigned		pvIdx;
	Value			*newV;
	Value			*oldV;
	unsigned		flags;
	ElementID		eid;
	ElementID		eltKey;
	ElementID		epos;
	PropInfo		*pInfo;
	HRefSSV			*href;
};

class Cursor;
class Stmt;

/**
 * candidate SSV descriptor
 */
struct CandidateSSV
{
	PropertyID		pid;
	const	Value	*pv;
	struct	ModInfo	*mi;
	size_t			length;
	size_t			dlen;
	CandidateSSV() {}
	CandidateSSV(const Value *p,PropertyID pi,size_t len,struct ModInfo *m,size_t dl) : pid(pi),pv(p),mi(m),length(len),dlen(dl) {}
	static	int __cdecl compare(const void *v1, const void *v2) {return cmp3(((const CandidateSSV*)v2)->length,((const CandidateSSV*)v1)->length);}
};

typedef DynArray<CandidateSSV>	CandidateSSVs;

struct RefTrace
{
	const	RefTrace	*next;
	PID					id;
	PropertyID			pid;
	ElementID			eid;
};

/**
 * map for resolution of mutual references of uncommitted pins
 */

struct PINMapElt
{
	uint64_t	tpid;
	PIN			*pin;
	__forceinline static int cmp(const PINMapElt& x,uint64_t y) {return cmp3(x.tpid,y);}
	__forceinline static RC merge(PINMapElt&,PINMapElt&,MemAlloc*) {return RC_OK;}
	operator uint64_t() const {return tpid;}
};

typedef	DynOArrayBuf<PINMapElt,uint64_t,PINMapElt> PINMap;

class Cursor;

/**
 * PIN loading and modifiaction routines
 */
class QueryPrc
{
	RC		loadProps(PINx *pcb,unsigned mode,const PropertyID *pids=NULL,unsigned nPids=0);
	RC		loadV(Value& v,unsigned pid,const PINx& cb,unsigned mode,MemAlloc *ma,unsigned eid=STORE_COLLECTION_ID,const Value *mkey=NULL);
	RC		loadVH(Value& v,const HeapPageMgr::HeapV *hprop,const PINx& cb,unsigned mode,MemAlloc *ma,unsigned eid=STORE_COLLECTION_ID,const Value *mkey=NULL);
	RC		loadS(Value& v,HType ty,PageOff offset,const HeapPageMgr::HeapPage *frame,unsigned mode,MemAlloc *ma,unsigned eid=STORE_COLLECTION_ID);
	RC		loadSSVs(Value *values,unsigned nValues,unsigned mode,Session *ses,MemAlloc *ma);
	RC		loadSSV(Value& val,ValueType ty,const HeapPageMgr::HeapObjHeader *hobj,unsigned mode,MemAlloc *ma);	// struct PropertyID?

	RC		getBody(PINx& cb,TVOp tvo=TVO_READ,unsigned flags=0,VersionID=STORE_CURRENT_VERSION);
	RC		resolveRef(Value& val,PIN *const *pins,unsigned nPins,PINMap *map);
	RC		makeRoom(PIN *pin,ushort lxtab,PBlock *pb,Session *ses,size_t reserve);
	RC		rename(ChangeInfo& inf,PropertyID pid,unsigned flags,bool fSync);
	RC		checkLockAndACL(PINx& qr,TVOp tvo,QueryOp *qop=NULL);
	RC		checkACLs(PINx& cb,IdentityID iid,TVOp tvo,unsigned flags=0,bool fProp=true);
	RC		checkACLs(PINx *pin,PINx& cb,IdentityID iid,TVOp tvo,unsigned flags=0,bool fProp=true);
	RC		checkACL(const Value&,PINx&,IdentityID,uint8_t,const RefTrace*,bool=true);
	RC		getRefSafe(const PID& id,Value *&vals,unsigned& nValues,unsigned mode,PINx& cb);
	RC		apply(const EvalCtx& ectx,STMT_OP op,PINx& qr,const Value *values,unsigned nValues,unsigned mode,PIN *pin=NULL);
	RC		count(QueryOp *qop,uint64_t& cnt,unsigned nAbort,const OrderSegQ *os=NULL,unsigned nos=0);
	RC		eval(const Value *pv,const EvalCtx& ctx,Value *res=NULL,unsigned mode=0);
	RC		purge(PageID pageID,unsigned start,unsigned len,const uint32_t *bmp,PurgeType pt,Session *ses);
	RC		updateComm(const EvalCtx &ectx,PIN *pin,const Value *v,unsigned nv,unsigned mode);

	size_t	splitLength(const Value *pv);
	RC		calcLength(const Value& v,size_t& res,unsigned mode,size_t threshold,MemAlloc *ma,PageID pageID=INVALID_PAGEID,size_t *rlen=NULL);
	RC		findCandidateSSVs(CandidateSSVs& cs,const Value *pv,unsigned nv,bool fSplit,MemAlloc *ma,const AllocCtrl *act=NULL,PropertyID=STORE_INVALID_URIID,struct ModInfo *mi=NULL);
	RC		persistValue(const Value& v,ushort& sht,HType& vt,ushort& offs,byte *buf,size_t *plrec,const PageAddr &addr,ElementID *keygen=NULL);
	RC		putHeapMod(HeapPageMgr::HeapPropMod *hpm,struct ModInfo *pm,byte *buf,ushort& sht,PINx&,bool=false);
	RC		reload(PIN *pin,PINx *pcb);

private:
	IStoreNotification	*const	notification;
	StoreCtx			*const	ctx;
	size_t				bigThreshold;
	PropertyID			*calcProps;
	unsigned			nCalcProps;
	RWLock				cPropLock;
public:
	QueryPrc(StoreCtx *,IStoreNotification *notItf);

	RC		loadPIN(Session *ses,const PID& id,PIN *&pin,unsigned mode=0,PINx *pcb=NULL,VersionID=STORE_CURRENT_VERSION);
	RC		loadValue(Session *ses,const PID& id,PropertyID pid,ElementID eid,Value& res,unsigned mode=0);
	RC		loadValues(Value *pv,unsigned nv,const PID& id,Session *ses,unsigned mode=0);
	RC		getPINValue(const PID& id,Value& res,unsigned mode,Session *ses);
	RC		diffPIN(const PIN *pin,PINx& cb,Value *&diffProps,unsigned& nDiffProps,Session *ses);
	RC		persistPINs(const EvalCtx& ectx,PIN *const *pins,unsigned nPins,unsigned mode,const AllocCtrl *actrl=NULL,size_t *pSize=NULL,const IntoClass *into=NULL,unsigned nInto=0);
	RC		modifyPIN(const EvalCtx& ectx,const PID& id,const Value *v,unsigned nv,PINx *pcb,PIN *pin=NULL,unsigned mode=0,const ElementID *eids=NULL,unsigned* =NULL);
	RC		deletePINs(const EvalCtx& ectx,const PIN *const *pins,const PID *pids,unsigned nPins,unsigned mode,PINx *pcb=NULL);
	RC		undeletePINs(const EvalCtx& ectx,const PID *pids,unsigned nPins);
	RC		setFlag(const EvalCtx& ectx,const PID& id,PageAddr *addr,ushort flag,bool fReset);
	RC		loadData(const PageAddr& addr,byte *&p,size_t& len,MemAlloc *ma);
	RC		persistData(IStream *stream,const byte *str,size_t lstr,PageAddr& addr,uint64_t&,const PageAddr* =NULL,PBlockP* =NULL);
	RC		editData(Session *ses,PageAddr &addr,uint64_t& length,const Value&,PBlockP *pbp=NULL,byte *pOld=NULL);
	RC		deleteData(const PageAddr& addr,Session *ses=NULL,PBlockP *pbp=NULL);
	bool	test(PIN *,ClassID,const EvalCtx& ectx,bool fIgnore=false);
	RC		transform(const PINx **vars,unsigned nVars,PIN **pins,unsigned nPins,unsigned &nOut,Session*) const;
	RC		getClassInfo(Session *ses,PIN *pin);
	RC		getCalcPropID(unsigned n,PropertyID& pid);
	bool	checkCalcPropID(PropertyID pid);

	friend	class	Expr;
	friend	class	Stmt;
	friend	class	Cursor;
	friend	class	Navigator;
	friend	class	PINx;
	friend	class	Session;
	friend	struct	TxPurge;
	friend	class	FTIndexMgr;
	friend	struct	ModCtx;
	friend	class	Collection;
	friend	struct	ClassResult;
	friend	class	ClassPropIndex;
	friend	struct	TimerQElt;
	friend	class	Classifier;
	friend	class	Class;
	friend	class	StartFSM;
	friend	class	FSMMgr;
	friend	class	ProcessStream;
	friend	class	NamedMgr;
	friend	class	ServiceCtx;
	friend	class	RegexService;
	friend	class	QueryOp;
	friend	class	MergeOp;
	friend	class	NestedLoop;
	friend	class	ExprScan;
	friend	class	Filter;
	friend	class	LoadOp;
	friend	class	TransOp;
	friend	class	PathOp;
	friend	class	Sort;
};

};

#endif
