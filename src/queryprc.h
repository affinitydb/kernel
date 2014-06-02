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
 * PIN loading and modification routines
 */
#ifndef _QUERYPRC_H_
#define _QUERYPRC_H_

#include "modinfo.h"
#include "qbuild.h"
#include "queryop.h"
#include "txmgr.h"
#include "lock.h"

class IStoreNotification;

namespace AfyKernel
{

/**
 * PIN skew calculation constants
 */
#define	SKEW_PAGE_PCT		10
#define	SKEW_FACTOR			2

#define	ARRAY_THRESHOLD		256		/**< threshold of 'big' collections */
#define STRING_THRESHOLD	64		/**< string length threshold for SSV data */

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
	RC		loadS(Value& v,HType ty,PageOff offset,const HeapPageMgr::HeapPage *frame,unsigned mode,MemAlloc *ma,unsigned eid=STORE_COLLECTION_ID);
	RC		resolveRef(Value& val,PIN *const *pins,unsigned nPins,PINMap *map);
	RC		makeRoom(PIN *pin,ushort lxtab,PBlock *pb,Session *ses,size_t reserve);
	RC		rename(ChangeInfo& inf,PropertyID pid,unsigned flags,bool fSync);
	RC		apply(const EvalCtx& ectx,STMT_OP op,PINx& qr,const Value *values,unsigned nValues,unsigned mode,PIN *pin=NULL);
	RC		count(QueryOp *qop,uint64_t& cnt,unsigned nAbort,const OrderSegQ *os=NULL,unsigned nos=0);
	RC		eval(const Value *pv,const EvalCtx& ctx,Value *res=NULL,unsigned mode=0);
	RC		purge(PageID pageID,unsigned start,unsigned len,const uint32_t *bmp,PurgeType pt,Session *ses);
	RC		updateComm(const EvalCtx &ectx,PIN *pin,const Value *v,unsigned nv,unsigned mode);

	size_t	splitLength(const Value *pv);
	RC		calcLength(const Value& v,size_t& res,unsigned mode,size_t threshold,MemAlloc *ma,PageID pageID=INVALID_PAGEID,size_t *rlen=NULL);
	RC		findCandidateSSVs(CandidateSSVs& cs,const Value *pv,unsigned nv,bool fSplit,MemAlloc *ma,const AllocCtrl *act=NULL,PropertyID=STORE_INVALID_URIID,struct ModInfo *mi=NULL);
	RC		persistValue(const Value& v,ushort& sht,HType& vt,ushort& offs,byte *buf,size_t *plrec,const PageAddr &addr,ElementID *keygen=NULL,MemAlloc *pinAlloc=NULL);
	RC		putHeapMod(HeapPageMgr::HeapPropMod *hpm,struct ModInfo *pm,byte *buf,ushort& sht,PINx&,bool=false);

private:
	IStoreNotification	*const	notification;
	StoreCtx			*const	ctx;
	size_t				bigThreshold;
	PropertyID			*calcProps;
	unsigned			nCalcProps;
	RWLock				cPropLock;
public:
	QueryPrc(StoreCtx *,IStoreNotification *notItf);

	RC		reload(PIN *pin,PINx *pcb);
	RC		loadValue(Session *ses,const PID& id,PropertyID pid,ElementID eid,Value& res,unsigned mode=0);
	RC		getPINValue(const PID& id,Value& res,unsigned mode,Session *ses);
	RC		diffPIN(const PIN *pin,PINx& cb,Value *&diffProps,unsigned& nDiffProps,Session *ses);
	RC		persistPINs(const EvalCtx& ectx,PIN *const *pins,unsigned nPins,unsigned mode,const AllocCtrl *actrl=NULL,size_t *pSize=NULL,const IntoClass *into=NULL,unsigned nInto=0);
	RC		modifyPIN(const EvalCtx& ectx,const PID& id,const Value *v,unsigned nv,PINx *pcb,PIN *pin=NULL,unsigned mode=0,const ElementID *eids=NULL,unsigned* =NULL);
	RC		deletePINs(const EvalCtx& ectx,const PIN *const *pins,const PID *pids,unsigned nPins,unsigned mode,PINx *pcb=NULL);
	RC		undeletePINs(const EvalCtx& ectx,const PID *pids,unsigned nPins);
	RC		loadData(const PageAddr& addr,byte *&p,size_t& len,MemAlloc *ma);
	RC		persistData(IStream *stream,const byte *str,size_t lstr,PageAddr& addr,uint64_t&,const PageAddr* =NULL,PBlockP* =NULL);
	RC		deleteData(const PageAddr& addr,Session *ses=NULL,PBlockP *pbp=NULL);
	bool	test(PIN *,DataEventID,const EvalCtx& ectx,bool fIgnore=false);
	RC		transform(const PINx **vars,unsigned nVars,PIN **pins,unsigned nPins,unsigned &nOut,Session*) const;
	RC		getDataEventInfo(Session *ses,PIN *pin);
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
	friend	struct	DetectedEvents;
	friend	class	DataEventRegistry;
	friend	struct	TimerQElt;
	friend	class	DataEventMgr;
	friend	class	DataEvent;
	friend	class	StartFSM;
	friend	class	EventMgr;
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
