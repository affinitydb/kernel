/**************************************************************************************

Copyright © 2004-2012 VMware, Inc. All rights reserved.

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

#include "txmgr.h"
#include "logmgr.h"
#include "lock.h"
#include "classifier.h"
#include "queryprc.h"
#include "pgheap.h"
#include "affinity.h"
#include "fsmgr.h"
#include <locale.h>

#include <stdio.h>

using namespace	AfyDB;
using namespace AfyKernel;

Tls Session::sessionTls;
Tls	StoreCtx::storeTls;
SharedCounter StoreCtx::nStores;

Session	*Session::createSession(StoreCtx *ctx)
{
	Session *ses=NULL; MemAlloc *ma;
	if (ctx!=NULL) for (long sc=ctx->sesCnt; ;sc=ctx->sesCnt)
		if (sc!=0 && (ctx->mode&STARTUP_SINGLE_SESSION)!=0) return NULL;
		else if (cas(&ctx->sesCnt,sc,sc+1)) break;
	if ((ma=createMemAlloc(SESSION_START_MEM,false))!=NULL) {
		if (ctx!=NULL) ctx->set(); 
		if ((ses=new(ma) Session(ctx,ma))!=NULL) {
			sessionTls.set(ses);
//			byte buf[sizeof(TIMESTAMP)+sizeof(ulong)]; getTimestamp(*(TIMESTAMP*)buf);
//			memcpy(buf+sizeof(TIMESTAMP),&ses->identity,sizeof(ulong));
//			ses->sesLSN = ctx->logMgr->insert(ses,LR_SESSION,0,INVALID_PAGEID,buf,sizeof(buf));
		}
	}
	return ses;
}

void Session::terminateSession()
{
	Session *ses = (Session*)sessionTls.get();
	if (ses!=NULL) {
		if (ses->isRestore()) {
			assert(ses->ctx!=NULL);
			if (ses->getTxState()!=TX_NOTRAN) ses->ctx->txMgr->commit(ses,true);
			ses->ctx->theCB->state=ses->ctx->logMgr->isInit()?SST_LOGGING:SST_READ_ONLY; 
			ses->ctx->theCB->update(ses->ctx);
		} else if (ses->getTxState()!=TX_NOTRAN) {
			report(MSG_WARNING,"Transaction "_LX_FM" was still active, when session was terminated\n",ses->getTXID());
			ses->ctx->txMgr->abort(ses,true);
		}
		ses->cleanup();
//		ses->ctx->logMgr->insert(ses,LR_SESSION,1,INVALID_PAGEID);
		MemAlloc *ma=ses->mem; StoreCtx *ctx=ses->ctx;
		ses->~Session(); sessionTls.set(NULL); if (ma!=NULL) ma->release();
		if (ctx!=NULL) InterlockedDecrement(&ctx->sesCnt);
	}
}

Session::Session(StoreCtx *ct,MemAlloc *ma)
	: ctx(ct),mem(ma),txid(INVALID_TXID),txcid(NO_TXCID),txState(TX_NOTRAN),sFlags(0),identity(STORE_INVALID_IDENTITY),
	list(this),lockReq(this),heldLocks(NULL),latched(new(ma) LatchedPage[INITLATCHED]),nLatched(0),xLatched(INITLATCHED),
	firstLSN(0),undoNextLSN(0),flushLSN(0),sesLSN(0),nLogRecs(0),tx(this),subTxCnt(0),mini(NULL),
	nTotalIns(0),xHeapPage(INVALID_PAGEID),forcedPage(INVALID_PAGEID),classLocked(RW_NO_LOCK),fAbort(false),
	txil(0),repl(NULL),itf(0),URIBase(NULL),lURIBaseBuf(0),lURIBase(0),qNames(NULL),nQNames(0),fStdOvr(false),
	iTrace(NULL),traceMode(0),defExpiration(0),allocCtrl(NULL),tzShift(0)
{
	extAddr.pageID=INVALID_PAGEID; extAddr.idx=INVALID_INDEX;
#ifdef WIN32
	TIME_ZONE_INFORMATION tzInfo; DWORD tzType=GetTimeZoneInformation(&tzInfo);
	tzShift=int64_t(tzType==TIME_ZONE_ID_STANDARD?tzInfo.Bias+tzInfo.StandardBias:
		tzType==TIME_ZONE_ID_DAYLIGHT?tzInfo.Bias+tzInfo.DaylightBias:
		tzType==TIME_ZONE_ID_UNKNOWN?tzInfo.Bias:0)*60000000L;
#else
	tm tt; time_t t=0;
	if (localtime_r(&t,&tt)!=NULL) tzShift=int64_t(-tt.tm_gmtoff)*1000000L;
#endif
	if (ct!=NULL) ++ct->nSessions;
}

Session::~Session()
{
	for (MiniTx *mtx=mini; mtx!=NULL; mtx=mtx->next) mtx->~MiniTx();
	list.remove(); if (ctx!=NULL) --ctx->nSessions;
}

void Session::cleanup()
{
	if (ctx!=NULL) {
		ctx->lockMgr->releaseSession(this);
		if (classLocked!=RW_NO_LOCK) {ctx->classMgr->getLock()->unlock(); classLocked=RW_NO_LOCK;}
		while (nLatched--!=0) ctx->bufMgr->release(latched[nLatched].pb,latched[nLatched].cntX!=0);
		tx.cleanup(); delete repl; repl=NULL;
		if (reuse.pinPages!=NULL) for (ulong i=0; i<reuse.nPINPages; i++)
			ctx->heapMgr->HeapPageMgr::reuse(reuse.pinPages[i].pid,reuse.pinPages[i].space,ctx);
		if (reuse.ssvPages!=NULL) for (ulong i=0; i<reuse.nSSVPages; i++)
			ctx->ssvMgr->HeapPageMgr::reuse(reuse.ssvPages[i].pid,reuse.ssvPages[i].space,ctx);
		reuse.cleanup();
	}
}

void Session::set(StoreCtx *ct)
{
	if (ctx!=NULL) --ctx->nSessions;
	if ((ctx=ct)!=NULL) {ct->set(); ++ct->nSessions;}
	flushLSN=0;
	//...
}

void Session::setRestore()
{
	sFlags|=S_RESTORE; identity=STORE_OWNER;
}

int LatchedPage::Cmp::cmp(const LatchedPage& lp,PageID pid)
{
	return cmp3(lp.pb->getPageID(),pid);
}

RC Session::latch(PBlock *pb,ulong mode)
{
	if (nLatched>=xLatched && (latched=(LatchedPage*)mem->realloc(latched,(xLatched*=2)*sizeof(LatchedPage)))==NULL) return RC_NORESOURCES;
	LatchedPage *ins=latched;
	if (BIN<LatchedPage,PageID,LatchedPage::Cmp>::find(pb->getPageID(),latched,nLatched,&ins)!=NULL) return RC_INTERNAL;
	if (ins<&latched[nLatched]) memmove(ins+1,ins,(byte*)&latched[nLatched]-(byte*)ins);
	if ((mode&(PGCTL_XLOCK|PGCTL_ULOCK))!=0) {ins->cntX=1; ins->cntS=0;} else {ins->cntS=1; ins->cntX=0;}
	ins->pb=pb; nLatched++; return RC_OK;
}

bool Session::relatch(PBlock *pb)
{
	LatchedPage *lp=(LatchedPage*)BIN<LatchedPage,PageID,LatchedPage::Cmp>::find(pb->getPageID(),latched,nLatched);
	if (lp!=NULL) {if (lp->cntS>1) return false; lp->cntS=0; lp->cntX=1;}
	return true;
}

bool Session::unlatch(PBlock *pb,ulong mode)
{
	LatchedPage *lp=(LatchedPage*)BIN<LatchedPage,PageID,LatchedPage::Cmp>::find(pb->getPageID(),latched,nLatched);
	if (lp!=NULL) {
		uint16_t *pcnt=(mode&QMGR_UFORCE)!=0||lp->cntS==0?&lp->cntX:&lp->cntS; assert(*pcnt!=0);
		if (--*pcnt!=0) return false;
		if ((lp->cntS|lp->cntX)!=0) {
			if ((mode&QMGR_UFORCE)!=0) pb->downgradeLock(RW_S_LOCK);
			return false;
		}
		if (lp<&latched[--nLatched]) memmove(lp,lp+1,(byte*)&latched[nLatched]-(byte*)lp);
#ifdef _DEBUG
		for (DLList *lh=latchHolderList.next; lh!=&latchHolderList; lh=lh->next) ((LatchHolder*)lh)->checkNotHeld(pb);
#endif
	}
	return true;
}

void Session::releaseLatches(PageID pid,PageMgr *mgr,bool fX)
{
	for (DLList *lh=latchHolderList.next; lh!=&latchHolderList; lh=lh->next) ((LatchHolder*)lh)->releaseLatches(pid,mgr,fX);
}

RC Session::releaseAllLatches()
{
	for (DLList *lh=latchHolderList.next; lh!=&latchHolderList; lh=lh->next) ((LatchHolder*)lh)->releaseLatches(INVALID_PAGEID,NULL,true);
	return nLatched==0?RC_OK:RC_DEADLOCK;
}

void Session::lockClass(RW_LockType lt)
{
	if (lt!=classLocked && classLocked!=RW_X_LOCK) {ctx->classMgr->getLock()->lock(lt); classLocked=lt;}
}

void Session::unlockClass()
{
	if (classLocked!=RW_NO_LOCK) {ctx->classMgr->getLock()->unlock(); classLocked=RW_NO_LOCK;}
}

void Session::trace(long code,const char *msg,...) 
{
	va_list va; va_start(va,msg);
	if (iTrace!=NULL) iTrace->trace(code,msg,va); else vfprintf(stderr,msg,va);
	va_end(va);
}

RC Session::setAllocCtrl(const AllocCtrl *ac)
{
	if (allocCtrl!=NULL) {free(allocCtrl); allocCtrl=NULL;}
	if (ac!=NULL) {
		if ((allocCtrl=new(this) AllocCtrl)==NULL) return RC_NORESOURCES;
		memcpy(allocCtrl,ac,sizeof(AllocCtrl));
	}
	return RC_OK;
}

RC Session::attach()
{
	if (ctx!=NULL) ctx->set();
	sessionTls.set(this);
	return RC_OK;
}

RC Session::detach()
{
	lockReq.sem.detach();
	sessionTls.set(NULL);
	StoreCtx::storeTls.set(NULL);
	return RC_OK;
}

void* Session::malloc(size_t s)
{
	return this!=NULL?mem->malloc(s):AfyKernel::malloc(s,SES_HEAP);
}

void* Session::memalign(size_t a,size_t s)
{
	return this!=NULL?mem->memalign(a,s):AfyKernel::memalign(a,s,SES_HEAP);
}

void* Session::realloc(void *p,size_t s)
{
	return this!=NULL?mem->realloc(p,s):AfyKernel::realloc(p,s,SES_HEAP);
}

void Session::free(void *p)
{
	if (this!=NULL) mem->free(p); else AfyKernel::free(p,SES_HEAP);
}

HEAP_TYPE Session::getAType() const 
{
	return SES_HEAP;
}

void Session::release()
{
	if (this!=NULL && mem!=NULL) mem->release();
}

bool Session::hasPrefix(const char *name,size_t lName)
{
	return name==NULL || memchr(name,'/',lName)!=NULL || memchr(name,'#',lName)!=NULL;
}

RC Session::setBase(const char *s,size_t l)
{
	if (URIBase!=NULL) {free((char*)URIBase); URIBase=NULL;}
	if (s!=NULL && l!=0) {
		size_t ll=s[l-1]!='/' && s[l-1]!='#' && s[l-1]!='?'?1:0;
		if ((URIBase=new(this) char[lURIBaseBuf=(lURIBase=l+ll)+200])==NULL) return RC_NORESOURCES;
		memcpy(URIBase,s,l+1); if (ll!=0) {URIBase[l]='/'; URIBase[l+1]='\0';}
	}
	return RC_OK;
}

RC Session::setPrefix(const char *qs,size_t lq,const char *str,size_t ls)
{
	if (qs==NULL || lq==0) return RC_INVPARAM; const bool fColon=qs[lq-1]!=':';
	//if ((lq+(fColon?1:0))==sizeof(STORE_STD_QPREFIX)-1 && memcmp(qs,STORE_STD_QPREFIX,lq)) fStdOvr=str!=NULL&&ls!=0;
	if (str==NULL || ls==0) {
		// delete if exists
		return RC_INTERNAL;
	}
	QName qn={strdup(qs,this),fColon?lq:lq-1,strdup(str,this),ls,false};
	qn.fDel=qn.lstr!=0 && (qn.str[qn.lstr-1]=='/'||qn.str[qn.lstr-1]=='#'||qn.str[qn.lstr-1]=='?');
	return BIN<QName,const QName&,QNameCmp>::insert(qNames,nQNames,qn,qn,this);
}

RC Session::pushTx()
{
	SubTx *st=new(this) SubTx(this); if (st==NULL) return RC_NORESOURCES;
	memcpy(st,&tx,sizeof(SubTx)); new(&tx) SubTx(this); 
	tx.next=st; tx.lastLSN=st->lastLSN; tx.subTxID=++subTxCnt;
	if (repl!=NULL) repl->mark(tx.rmark);
	return RC_OK;
}

RC Session::popTx(bool fCommit,bool fAll)
{
	for (SubTx *st=tx.next; st!=NULL; st=tx.next) {
		if (fCommit) {
			st->defHeap+=tx.defHeap; st->defClass+=tx.defClass; st->defFree+=tx.defFree; st->nInserted+=tx.nInserted;
			if (tx.txPurge!=NULL) {RC rc=st->txPurge.merge(tx.txPurge); if (rc!=RC_OK) return rc;}
			if (tx.txClass!=NULL) {Classifier::merge(tx.txClass,st->txClass); tx.txClass=NULL;}
			if (tx.txIndex!=NULL) {
				// txIndex!!! merge to st
				tx.txIndex=NULL;
			}
		} else if (fAll) st->nInserted=nTotalIns=0;
		else {
			ctx->lockMgr->releaseLocks(this,tx.subTxID,true); st->nInserted-=tx.nInserted; nTotalIns-=tx.nInserted;
			if (repl!=NULL) repl->truncate(tx.rmark);
		}
		st->lastLSN=tx.lastLSN;	//???
		tx.next=NULL; tx.~SubTx(); memcpy(&tx,st,sizeof(SubTx)); free(st); if (!fAll) break;
	}
	return RC_OK;
}

SubTx::SubTx(Session *s) : next(NULL),ses(s),subTxID(0),lastLSN(0),txClass(NULL),txIndex(NULL),txPurge(s),defHeap(s),defClass(s),defFree(s),nInserted(0)
{
}

SubTx::~SubTx()
{
	for (unsigned i=0,j=(unsigned)txPurge; i<j; i++) if (txPurge[i].bmp!=NULL) ses->free(txPurge[i].bmp);
	if (txClass!=NULL) ses->getStore()->classMgr->classTx(ses,txClass,false);
	//delete txIndex;
}

void SubTx::cleanup()
{
	if (next!=NULL) {next->cleanup(); next->~SubTx(); ses->free(next); next=NULL;}
	for (unsigned i=0,j=(unsigned)txPurge; i<j; i++) if (txPurge[i].bmp!=NULL) ses->free(txPurge[i].bmp);
	txPurge.clear();
	if (txClass!=NULL) ses->getStore()->classMgr->classTx(ses,txClass,false);
	if (txIndex!=NULL) {
		//...
	}
	if ((ulong)defHeap!=0) {
		//???
		defHeap.cleanup();
	}
	if ((ulong)defClass!=0) {
		//???
		defClass.cleanup();
	}
	if ((ulong)defFree!=0) {
		if (ses->getStore()->fsMgr->freeTxPages(defFree)==RC_OK) ses->getStore()->fsMgr->txUnlock(); defFree.cleanup();
	}
	lastLSN=LSN(0);
}

RC SubTx::queueForPurge(const PageAddr& addr,PurgeType pt,const void *data)
{
	TxPurge prg={addr.pageID,0,NULL},*tp=NULL; RC rc=txPurge.add(prg,&tp); uint32_t flg=pt==TXP_SSV?0x80000000:0;
	if (rc==RC_FALSE) {
		if (data!=NULL || (tp->range&0x80000000)!=flg || tp->range==uint32_t(~0u)) return RC_CORRUPTED;	// report error
		ushort idx=addr.idx/(sizeof(uint32_t)*8),l=tp->range>>16&0x3fff; assert(tp->bmp!=NULL);
		if (idx<ushort(tp->range)) {
			ushort d=ushort(tp->range)-idx;
			if ((tp->bmp=(uint32_t*)ses->realloc(tp->bmp,(l+d)*sizeof(uint32_t)))==NULL) return RC_NORESOURCES;
			memmove(tp->bmp+d,tp->bmp,l*sizeof(uint32_t)); if (d==1) tp->bmp[0]=0; else memset(tp->bmp,0,d*sizeof(uint32_t));
			tp->range=(tp->range&0x80000000)+(uint32_t(l+d)<<16)+idx;
		} else if (idx>=ushort(tp->range)+l) {
			ushort d=idx+1-ushort(tp->range)-l;
			if ((tp->bmp=(uint32_t*)ses->realloc(tp->bmp,(l+d)*sizeof(uint32_t)))==NULL) return RC_NORESOURCES;
			if (d==1) tp->bmp[l]=0; else memset(tp->bmp+l,0,d*sizeof(uint32_t)); tp->range+=uint32_t(d)<<16;
		}
		tp->bmp[idx-ushort(tp->range)]|=1<<addr.idx%(sizeof(uint32_t)*8); rc=RC_OK;
	} else if (rc==RC_OK) {
		if (data!=NULL) {
			ushort l=HeapPageMgr::collDescrSize((HeapPageMgr::HeapExtCollection*)data);
			if ((tp->bmp=(uint32_t*)ses->malloc(l))==NULL) return RC_NORESOURCES;
			memcpy(tp->bmp,data,l); tp->range=uint32_t(~0u);
		} else {
			tp->range=(addr.idx/(sizeof(uint32_t)*8))|flg|0x00010000;
			if ((tp->bmp=new(ses) uint32_t)==NULL) return RC_NORESOURCES;
			tp->bmp[0]=1<<addr.idx%(sizeof(uint32_t)*8);
		}
	}
	return rc;
}

RC TxPurge::purge(Session *ses)
{
	return ses->getStore()->queryMgr->purge(pageID,range&0xFFFF,range>>16&0x3FFF,bmp,(range&0x80000000)!=0?TXP_SSV:TXP_PIN,ses);
}

RC TxPurge::Cmp::merge(TxPurge& dst,TxPurge& src,MemAlloc *ma)
{
	unsigned ss=src.range&0xFFFF,ds=dst.range&0xFFFF; assert(src.pageID==dst.pageID);
	if (ds==0xFFFF) {ma->free(src.bmp); src.bmp=NULL; return ss==0xFFFF?RC_OK:RC_CORRUPTED;}
	unsigned si=ss+(src.range>>16&0x3FFF),di=ds+(dst.range>>16&0x3FFF);
	if (ss<ds) {unsigned t=ss; ss=ds; ds=t; t=si; si=di; di=t; uint32_t *p=src.bmp; src.bmp=dst.bmp; dst.bmp=p;}
	if (di<si) {
		if ((dst.bmp=(uint32_t*)ma->realloc(dst.bmp,(si-ds)*sizeof(uint32_t)))==NULL) return RC_OK;
		if (ss<=di) memcpy(dst.bmp+di-ds,src.bmp+di-ss,(si-di)*sizeof(uint32_t));
		else {memset(dst.bmp+di-ds,0,(ss-di)*sizeof(uint32_t)); memcpy(dst.bmp+ss-ds,src.bmp,(si-ss)*sizeof(uint32_t));}
	}
	for (unsigned i=ss,end=min(si,di); i<end; i++) dst.bmp[i-ds]|=src.bmp[i-ss];
	ma->free(src.bmp); src.bmp=NULL; dst.range=(dst.range&0x80000000)|(max(di,si)-ds)<<16|ds; return RC_OK;
}

TxGuard::~TxGuard()
{
	assert(ses!=NULL);
	if (ses->getTxState()==TX_ABORTING) ses->ctx->txMgr->abortTx(ses,true);
}

bool StoreCtx::isInit() const
{
	return theCB->state==SST_INIT||theCB->state==SST_IN_RECOVERY;
}

bool StoreCtx::inShutdown() const
{
	return (state&SSTATE_IN_SHUTDOWN)!=0;
}

bool StoreCtx::isReadOnly() const
{
	return (state&SSTATE_READONLY)!=0;
}

PageMgr *StoreCtx::getPageMgr(PGID pgid) const
{
	if (pgid>=PGID_ALL || pageMgrTab[pgid]==NULL) {
		report(MSG_ERROR,"Incorrect PGID %d in PageMgr::getPageMgr\n",pgid);
		return NULL;
	}
	return pageMgrTab[pgid];
}

LSN StoreCtx::getOldLSN() const
{
	return logMgr->getOldLSN();
}

void* StoreCtx::malloc(size_t s)
{
	return this!=NULL&&mem!=NULL?mem->malloc(s):AfyKernel::malloc(s,STORE_HEAP);
}

void* StoreCtx::memalign(size_t a,size_t s)
{
	return this!=NULL&&mem!=NULL?mem->memalign(a,s):AfyKernel::memalign(a,s,STORE_HEAP);
}

void* StoreCtx::realloc(void *p,size_t s)
{
	return this!=NULL&&mem!=NULL?mem->realloc(p,s):AfyKernel::realloc(p,s,STORE_HEAP);
}

void StoreCtx::free(void *p)
{
	if (this!=NULL && mem!=NULL) mem->free(p); else AfyKernel::free(p,STORE_HEAP);
}

HEAP_TYPE StoreCtx::getAType() const 
{
	return STORE_HEAP;
}

void StoreCtx::release()
{
	if (this!=NULL && mem!=NULL) {mem->release(); mem=NULL;}
}

const PageAddr PageAddr::invAddr={INVALID_PAGEID,INVALID_INDEX};

bool PageAddr::convert(OID oid) {
	if (oid==STORE_INVALID_PID) return false;
	pageID=PageID(oid>>16); idx=PageIdx(oid&0xFFFF);
	if (pageID==INVALID_PAGEID||(idx&0x8000)!=0||ushort(oid>>48)!=StoreCtx::get()->storeID) return false;
	// compare pageID with approx. store size (sum of length of open data files)
	return true;
}

PageAddr::operator OID() const 
{
	return (uint64_t(StoreCtx::get()->storeID)<<32|pageID)<<16|idx;
}
