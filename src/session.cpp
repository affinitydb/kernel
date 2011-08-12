/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "txmgr.h"
#include "logmgr.h"
#include "lock.h"
#include "classifier.h"
#include "queryprc.h"
#include "pgheap.h"
#include "mvstore.h"
#include "fsmgr.h"
#include <locale.h>

#include <stdio.h>

using namespace	MVStore;
using namespace MVStoreKernel;

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
		if (ctx!=NULL) ctx->set(); ses=new(ma) Session(ctx,ma); sessionTls.set(ses);
//		byte buf[sizeof(TIMESTAMP)+sizeof(ulong)]; getTimestamp(*(TIMESTAMP*)buf);
//		memcpy(buf+sizeof(TIMESTAMP),&ses->identity,sizeof(ulong));
//		ses->sesLSN = ctx->logMgr->insert(ses,LR_SESSION,0,INVALID_PAGEID,buf,sizeof(buf));
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
	: txid(INVALID_TXID),txcid(NO_TXCID),txState(TX_NOTRAN),sFlags(0),identity(STORE_INVALID_IDENTITY),
	list(this),lockReq(this),heldLocks(NULL),nLatched(0),firstLSN(0),undoNextLSN(0),flushLSN(0),sesLSN(0),nLogRecs(0),
	tx(this),subTxCnt(0),mini(NULL),nTotalIns(0),xHeapPage(INVALID_PAGEID),forcedPage(INVALID_PAGEID),classLocked(RW_NO_LOCK),fAbort(false),
	itf(0),URIBase(NULL),lURIBaseBuf(0),lURIBase(0),qNames(NULL),nQNames(0),fStdOvr(false),ctx(ct),mem(ma),iTrace(NULL),traceMode(0),
	defExpiration(0),allocCtrl(NULL),tzShift(0)
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
		while (nLatched>0) latched[--nLatched]->release(PGCTL_NOREG);
		tx.cleanup();
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

PBlock *Session::getLatched(PageID pid) const 
{
	for (int i=nLatched; --i>=0; ) if (latched[i]->getPageID()==pid) return latched[i];
	return NULL;
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
	return this!=NULL?mem->malloc(s):MVStoreKernel::malloc(s,SES_HEAP);
}

void* Session::memalign(size_t a,size_t s)
{
	return this!=NULL?mem->memalign(a,s):MVStoreKernel::memalign(a,s,SES_HEAP);
}

void* Session::realloc(void *p,size_t s)
{
	return this!=NULL?mem->realloc(p,s):MVStoreKernel::realloc(p,s,SES_HEAP);
}

void Session::free(void *p)
{
	if (this!=NULL) mem->free(p); else MVStoreKernel::free(p,SES_HEAP);
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
	return mv_binscmp<QName,unsigned>(qNames,nQNames,qn,this);
}

RC Session::pushTx()
{
	SubTx *st=new(this) SubTx(this); if (st==NULL) return RC_NORESOURCES;
	memcpy(st,&tx,sizeof(SubTx)); new(&tx) SubTx(this); 
	tx.next=st; tx.lastLSN=st->lastLSN; tx.subTxID=++subTxCnt;
	return RC_OK;
}

RC Session::popTx(bool fCommit,bool fAll)
{
	for (SubTx *st=tx.next; st!=NULL; st=tx.next) {
		if (fCommit) {
			st->defHeap+=tx.defHeap; st->defClass+=tx.defClass; st->defFree+=tx.defFree; st->nInserted+=tx.nInserted;
			if (tx.txDelete!=NULL) {
				if (st->txDelete==NULL) st->txDelete=tx.txDelete;
				else for (TxDelete *td=st->txDelete;;td=td->next) if (td->next==NULL) {td->next=tx.txDelete; break;}
				tx.txDelete=NULL;
			}
			if (tx.txIndex!=NULL) {
				// txIndex!!! merge to st
				tx.txIndex=NULL;
			}
			if (tx.txClass!=NULL) {Classifier::merge(tx.txClass,st->txClass); tx.txClass=NULL;}
		} else if (fAll) st->nInserted=nTotalIns=0;
		else {ctx->lockMgr->releaseLocks(this,tx.subTxID,true); st->nInserted-=tx.nInserted; nTotalIns-=tx.nInserted;}
		st->lastLSN=tx.lastLSN;	//???
		tx.next=NULL; tx.~SubTx(); memcpy(&tx,st,sizeof(SubTx)); free(st); if (!fAll) break;
	}
	return RC_OK;
}

SubTx::SubTx(Session *s) : next(NULL),ses(s),subTxID(0),lastLSN(0),txClass(NULL),txIndex(NULL),txDelete(NULL),defHeap(s),defClass(s),defFree(s),nInserted(0)
{
}

SubTx::~SubTx()
{
	while (txDelete!=NULL) {TxDelete *td=txDelete; txDelete=td->next; td->release(); ses->free(td);}
	if (txClass!=NULL) ses->getStore()->classMgr->classTx(ses,txClass,false);
	//delete txIndex;
}

void SubTx::cleanup()
{
	if (next!=NULL) {next->cleanup(); next->~SubTx(); ses->free(next); next=NULL;}
	while (txDelete!=NULL) {TxDelete *td=txDelete; txDelete=td->next; td->release(); ses->free(td);}
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

TxGuard::~TxGuard()
{
	assert(ses!=NULL);
	if (ses->getTxState()==TX_ABORTING) ses->ctx->txMgr->abortTx(ses,true);
}

StoreCtx *StoreCtx::createCtx(ulong f,bool fNew)
{
	StoreCtx *ctx=new(SERVER_HEAP) StoreCtx(f);
	if (ctx!=NULL) {ctx->mem=createMemAlloc(fNew?STORE_NEW_MEM:STORE_START_MEM,true); storeTls.set(ctx);}
	return ctx;
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
	return this!=NULL&&mem!=NULL?mem->malloc(s):MVStoreKernel::malloc(s,STORE_HEAP);
}

void* StoreCtx::memalign(size_t a,size_t s)
{
	return this!=NULL&&mem!=NULL?mem->memalign(a,s):MVStoreKernel::memalign(a,s,STORE_HEAP);
}

void* StoreCtx::realloc(void *p,size_t s)
{
	return this!=NULL&&mem!=NULL?mem->realloc(p,s):MVStoreKernel::realloc(p,s,STORE_HEAP);
}

void StoreCtx::free(void *p)
{
	if (this!=NULL && mem!=NULL) mem->free(p); else MVStoreKernel::free(p,STORE_HEAP);
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
