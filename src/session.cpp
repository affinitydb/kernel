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

#include "txmgr.h"
#include "logmgr.h"
#include "lock.h"
#include "dataevent.h"
#include "queryprc.h"
#include "pgheap.h"
#include "fsmgr.h"
#include "maps.h"
#include "netmgr.h"
#include "ftindex.h"
#include "blob.h"
#include <locale.h>

using namespace	Afy;
using namespace AfyKernel;

Tls Session::sessionTls;
Tls	StoreCtx::storeTls;

bool StoreCtx::isInit() const
{
	return theCB->state==SST_INIT||theCB->state==SST_IN_RECOVERY;
}

unsigned StoreCtx::getState() const
{
	return (unsigned)state;
}

uint64_t StoreCtx::getOccupiedMemory() const
{
	return theCB->totalMemUsed;
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

RC StoreCtx::registerPrefix(const char *qs,size_t lq,const char *str,size_t ls)
{
	if (qs==NULL || lq==0 || str==NULL || ls==0) return RC_INVPARAM;
	const bool fColon=qs[lq-1]!=':';
	QName qn={strdup(qs,this),fColon?lq:lq-1,strdup(str,this),ls,false};
	qn.fDel=qn.lstr!=0 && (qn.str[qn.lstr-1]=='/'||qn.str[qn.lstr-1]=='#'||qn.str[qn.lstr-1]=='?');
	return BIN<QName,const QName&,QNameCmp>::insert(qNames,nQNames,qn,qn,(MemAlloc*)this);
}

void* StoreCtx::malloc(size_t s)
{
	return mem!=NULL?mem->malloc(s):AfyKernel::malloc(s,STORE_HEAP);
}

void* StoreCtx::memalign(size_t a,size_t s)
{
	return mem!=NULL?mem->memalign(a,s):AfyKernel::memalign(a,s,STORE_HEAP);
}

void* StoreCtx::realloc(void *p,size_t s,size_t old)
{
	return mem!=NULL?mem->realloc(p,s,old):AfyKernel::realloc(p,s,STORE_HEAP);
}

void StoreCtx::free(void *p)
{
	if (mem!=NULL) mem->free(p); else AfyKernel::free(p,STORE_HEAP);
}

HEAP_TYPE StoreCtx::getAType() const 
{
	return STORE_HEAP;
}

void StoreCtx::truncate(TruncMode tm,const void *mrk)
{
	if (mem!=NULL) mem->truncate(tm,mrk);
}

size_t StoreCtx::getPublicKey(uint8_t *buf,size_t lbuf,bool fB64)
{
	if (!fB64) {memcpy(buf,pubKey,min(lbuf,sizeof(pubKey))); return sizeof(pubKey);}
	size_t l=min((sizeof(pubKey)+2)/3,lbuf/4)*4; base64enc(pubKey,sizeof(pubKey),(char*)buf,l);
	return l;
}

void StoreCtx::changeTraceMode(unsigned mask,bool fReset)
{
	try {if (fReset) traceMode&=~mask; else traceMode|=mask;} catch (...) {report(MSG_ERROR,"Exception in IAffinity::changeTraceMode()\n");}
}

//-------------------------------------------------------------------------------------

ISession *StoreCtx::startSession(const char *ident,const char *pwd,size_t initMem)
{
	try {
		if (inShutdown()||theCB->state==SST_RESTORE) return NULL;
		Session *s=Session::createSession(this,initMem,true); if (s==NULL) return NULL;
		if (!namedMgr->isInit()) {s->setIdentity(STORE_OWNER,true); namedMgr->loadObjects(s,false); s->setIdentity(STORE_INVALID_IDENTITY,false);}
		if (!s->login(ident,pwd)) {s->terminate(); s=NULL;}
		return s;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IAffinity::startSession()\n");}
	return NULL;
}

Session	*Session::createSession(StoreCtx *ctx,size_t initMem,bool fClient)
{
	if (ctx!=NULL) for (long sc=ctx->sesCnt; ;sc=ctx->sesCnt)
		if (sc!=0 && (ctx->mode&STARTUP_RT)!=0) return NULL;
		else if (cas(&ctx->sesCnt,sc,sc+1)) break;
	Session *ses=NULL; MemAlloc *ma=initMem==0?createMemAlloc(SESSION_START_MEM,false):new(ctx) StackAlloc(ctx,initMem);
	if (ma!=NULL) {
		if (ctx!=NULL) ctx->set(); 
		if ((ses=new(ma) Session(ctx,ma))!=NULL) {
			if (ctx!=NULL) ses->traceMode=ctx->traceMode; if (initMem!=0) ((StackAlloc*)ma)->mark(ses->sm);
			if (fClient) ses->sFlags|=S_CLIENT;
			else {
				ses->identity=STORE_OWNER; ses->sFlags|=S_INSERT;
				if ((ses->memCache=new(ma) SesMemCache*[SES_MEM_BLOCK_QUEUES])!=NULL) memset(ses->memCache,0,SES_MEM_BLOCK_QUEUES*sizeof(void*));
			}
			sessionTls.set(ses);
		}
	}
	return ses;
}

void Session::freeMemory()
{
	if (latched!=NULL) while (nLatched!=0) {--nLatched; ctx->bufMgr->release(latched[nLatched].pb,latched[nLatched].cntX!=0);}
	latched=NULL; nLatched=0; xLatched=0; latchHolderList.reset();
	tx.next=NULL; serviceTab=NULL; mini=NULL; srvCtx.reset();
	if (mem!=NULL) mem->truncate(TR_REL_ALLBUTONE,&sm);
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
			report(MSG_WARNING,"Transaction " _LX_FM " was still active, when session was terminated\n",ses->getTXID());
			ses->ctx->txMgr->abort(ses,TXA_ALL);
		}
		ses->cleanup();
//		ses->ctx->logMgr->insert(ses,LR_SESSION,1,INVALID_PAGEID);
		MemAlloc *ma=ses->mem; StoreCtx *ctx=ses->ctx;
		ses->~Session(); sessionTls.set(NULL); if (ma!=NULL) ma->truncate(TR_REL_ALL);
		if (ctx!=NULL) InterlockedDecrement(&ctx->sesCnt);
	}
}

Session::Session(StoreCtx *ct,MemAlloc *ma)
	: ctx(ct),mem(ma),txid(INVALID_TXID),txcid(NO_TXCID),txState(TX_NOTRAN),sFlags(0),identity(STORE_INVALID_IDENTITY),list(this),lockReq(this),heldLocks(NULL),latched(NULL),nLatched(0),xLatched(0),
	firstLSN(0),undoNextLSN(0),flushLSN(0),sesLSN(0),nLogRecs(0),tx(this),subTxCnt(0),mini(NULL),nTotalIns(0),xHeapPage(INVALID_PAGEID),forcedPage(INVALID_PAGEID),
	classLocked(RW_NO_LOCK),fAbort(false),repl(NULL),itf(0),xOnCommit(DEFAULT_MAX_ON_COMMIT),nSyncStack(0),xSyncStack(DEFAULT_MAX_SYNC_ACTION),
	nSesObjects(0),xSesObjects(DEFAULT_MAX_OBJ_SESSION),serviceTab(NULL),iTrace(NULL),traceMode(0),codeTrace(0),nSrvCtx(0),xSrvCtx(MAX_SERV_CTX),active(NULL),defExpiration(0),tzShift(0)
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
	if (ct!=NULL) {++ct->nSessions; xSyncStack=ct->theCB->xSyncStack; xOnCommit=ct->theCB->xOnCommit; xSesObjects=ct->theCB->xSesObjects;}
}

Session::~Session()
{
	for (MiniTx *mtx=mini; mtx!=NULL; mtx=mtx->next) mtx->~MiniTx();
	list.remove(); if (ctx!=NULL) --ctx->nSessions;
}

void Session::cleanup()
{
	nSyncStack=nSesObjects=0; traceMode=0; itf=0;
	if (ctx!=NULL) {
		ctx->lockMgr->releaseSession(this);
		if (classLocked!=RW_NO_LOCK) {ctx->classMgr->getLock()->unlock(); classLocked=RW_NO_LOCK;}
		if (latched!=NULL) while (nLatched!=0) {--nLatched; ctx->bufMgr->release(latched[nLatched].pb,latched[nLatched].cntX!=0);}
		tx.cleanup(); delete repl; repl=NULL;
		if (reuse.pinPages!=NULL) for (unsigned i=0; i<reuse.nPINPages; i++)
			ctx->heapMgr->HeapPageMgr::reuse(reuse.pinPages[i].pid,reuse.pinPages[i].space,ctx);
		if (reuse.ssvPages!=NULL) for (unsigned i=0; i<reuse.nSSVPages; i++)
			ctx->ssvMgr->HeapPageMgr::reuse(reuse.ssvPages[i].pid,reuse.ssvPages[i].space,ctx);
		reuse.cleanup();
		if (ctx->theCB!=NULL) {xSyncStack=ctx->theCB->xSyncStack; xOnCommit=ctx->theCB->xOnCommit; xSesObjects=ctx->theCB->xSesObjects;}
	}
}

void Session::set(StoreCtx *ct)
{
	if (ctx!=NULL) --ctx->nSessions;
	if ((ctx=ct)!=NULL) {ct->set(); ++ct->nSessions; traceMode=ct->traceMode;}
	flushLSN=0;
	//...
}

int LatchedPage::Cmp::cmp(const LatchedPage& lp,PageID pid)
{
	return cmp3(lp.pb->getPageID(),pid);
}

RC Session::latch(PBlock *pb,unsigned mode)
{
	if (nLatched>=xLatched && (latched=(LatchedPage*)mem->realloc(latched,(xLatched+=xLatched==0?INITLATCHED:xLatched)*sizeof(LatchedPage)))==NULL) return RC_NOMEM;
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

bool Session::unlatch(PBlock *pb,unsigned mode)
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
	if (ctx!=NULL && (ctx->mode&STARTUP_RT)==0)
		for (DLList *lh=latchHolderList.next; lh!=&latchHolderList; lh=lh->next) ((LatchHolder*)lh)->releaseLatches(pid,mgr,fX);
}

RC Session::releaseAllLatches()
{
	if (ctx==NULL || (ctx->mode&STARTUP_RT)!=0) return RC_OK;
	for (DLList *lh=latchHolderList.next; lh!=&latchHolderList; lh=lh->next) ((LatchHolder*)lh)->releaseLatches(INVALID_PAGEID,NULL,true);
	return nLatched==0?RC_OK:RC_DEADLOCK;
}

void Session::lockClass(RW_LockType lt)
{
	if (lt!=classLocked && classLocked!=RW_X_LOCK)
		{if (ctx!=NULL && (ctx->mode&STARTUP_RT)==0) ctx->classMgr->getLock()->lock(lt); classLocked=lt;}
}

void Session::unlockClass()
{
	if (classLocked!=RW_NO_LOCK) 
		{if (ctx!=NULL && (ctx->mode&STARTUP_RT)==0) ctx->classMgr->getLock()->unlock(); classLocked=RW_NO_LOCK;}
}

void Session::trace(long code,const char *msg,...) 
{
	va_list va; va_start(va,msg);
	if (iTrace!=NULL) iTrace->trace(code,msg,va); else vfprintf(stderr,msg,va);
	va_end(va);
}

void Session::setTrace(ITrace *trc)
{
	try {iTrace=trc;} catch (...) {report(MSG_ERROR,"Exception in ISession::setTrace()\n");}
}

void Session::changeTraceMode(unsigned mask,bool fReset)
{
	try {if (fReset) traceMode&=~mask; else traceMode|=mask;} catch (...) {report(MSG_ERROR,"Exception in ISession::changeTraceMode()\n");}
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

void* Session::realloc(void *p,size_t s,size_t old)
{
	return this!=NULL?mem->realloc(p,s,old):AfyKernel::realloc(p,s,SES_HEAP);
}

void Session::free(void *p)
{
	if (this!=NULL) mem->free(p); else AfyKernel::free(p,SES_HEAP);
}

HEAP_TYPE Session::getAType() const 
{
	return SES_HEAP;
}

void Session::truncate(TruncMode tm,const void *mrk)
{
	if (this!=NULL && mem!=NULL) mem->truncate(tm,mrk);
}

bool Session::hasPrefix(const char *name,size_t lName)
{
	return name==NULL || memchr(name,'/',lName)!=NULL || memchr(name,'#',lName)!=NULL;
}

RC Session::mapURIs(unsigned nURIs,URIMap URIs[],const char *base,bool fObj)
{
	try {
		if (ctx->inShutdown()) return RC_SHUTDOWN;
		char *URIBase=NULL; size_t lURIBase=base!=NULL?strlen(base):0,lURIBaseBuf=0,lPref; const char *pref;
		for (unsigned i=0; i<nURIs; i++) {
			const char *URIName=URIs[i].URI; if (URIName==NULL || *URIName=='\0') return RC_INVPARAM;
			size_t lName = strlen(URIName);
			if (!Session::hasPrefix(URIName,lName)) {
				if (lURIBase!=0) {
					if (lURIBase+lName>lURIBaseBuf) {
						if ((URIBase=(char*)realloc(URIBase,lURIBase+lName,lURIBaseBuf))==NULL) return RC_NOMEM;
						if (lURIBaseBuf==0) memcpy(URIBase,base,lURIBase); lURIBaseBuf=lURIBase+lName;
					}
					memcpy(URIBase+lURIBase,URIName,lName); URIName=URIBase; lName+=lURIBase;
				} else if (fObj && (pref=ctx->namedMgr->getStorePrefix(lPref))!=NULL) {
					if (lPref+lName>lURIBaseBuf) {
						if ((URIBase=(char*)realloc(URIBase,lPref+lName,lURIBaseBuf))==NULL) return RC_NOMEM;
						if (lURIBaseBuf==0) memcpy(URIBase,pref,lPref); lURIBaseBuf=lPref+lName;
					}
					memcpy(URIBase+lPref,URIName,lName); URIName=URIBase; lName+=lPref;
				}
			}
			URI *uri=ctx->isServerLocked()?(URI*)ctx->uriMgr->find(URIName,lName):(URI*)ctx->uriMgr->insert(URIName,lName);
			URIs[i].uid=uri!=NULL?uri->getID():STORE_INVALID_URIID; if (uri!=NULL) uri->release();
		}
		if (URIBase!=NULL) free(URIBase);
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::mapURIs(...)\n"); return RC_INTERNAL;}
}

void Session::setInterfaceMode(unsigned md)
{
	try {itf=md; if (identity==STORE_OWNER) setReplication((md&ITF_REPLICATION)!=0);}
	catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::setInterfaceMode(...)\n");}
}

unsigned Session::getInterfaceMode() const
{
	try {return itf;} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getInterfaceMode(...)\n");}
	return ~0u;
}

IdentityID Session::storeIdentity(const char *identName,const char *pwd,bool fMayInsert,const unsigned char *cert,unsigned lcert)
{
	try {
		IdentityID iid=STORE_INVALID_IDENTITY;
		if (identName!=NULL && !ctx->inShutdown() && !ctx->isServerLocked() && checkAdmin()) {
			size_t lpwd=pwd!=NULL?strlen(pwd):0;
			PWD_ENCRYPT epwd((byte*)pwd,lpwd); const byte *enc=pwd!=NULL&&lpwd>0?epwd.encrypted():NULL;
			Identity *ident=(Identity*)ctx->identMgr->insert(identName,strlen(identName),enc,cert,lcert,fMayInsert);
			if (ident!=NULL) {iid=ident->getID(); ident->release();}
		}
		return iid;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::storeIdentity(...)\n");}
	return STORE_INVALID_IDENTITY;
}

IdentityID Session::loadIdentity(const char *identName,const unsigned char *pwd,unsigned lPwd,bool fMayInsert,const unsigned char *cert,unsigned lcert)
{
	try {
		IdentityID iid=STORE_INVALID_IDENTITY;
		if (identName!=NULL && !ctx->inShutdown() && !ctx->isServerLocked() && checkAdmin() && (lPwd==PWD_ENC_SIZE || lPwd==0)) {
			Identity *ident=(Identity*)ctx->identMgr->insert(identName,strlen(identName),lPwd!=0?pwd:NULL,cert,lcert,fMayInsert);
			if (ident!=NULL) {iid=ident->getID(); ident->release();}
		}
		return iid;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::loadIdentity(...)\n");}
	return STORE_INVALID_IDENTITY;
}

RC Session::setInsertPermission(IdentityID iid,bool fMayInsert)
{
	try {
		if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		return checkAdmin()?ctx->identMgr->setInsertPermission(iid,fMayInsert):RC_NOACCESS;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::setInsertPermission(...)\n"); return RC_INTERNAL;}
}

RC Session::changePassword(IdentityID iid,const char *oldPwd,const char *newPwd)
{
	try {
		if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		return checkAdmin()?ctx->identMgr->changePassword(iid,oldPwd,newPwd):RC_NOACCESS;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::changePassword(...)\n"); return RC_INTERNAL;}
}

RC Session::changeCertificate(IdentityID iid,const char *pwd,const unsigned char *cert,unsigned lcert)
{
	try {
		if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		return checkAdmin()?ctx->identMgr->changeCertificate(iid,pwd,cert,lcert):RC_NOACCESS;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::changeCertificate(...)\n"); return RC_INTERNAL;}
}

IdentityID Session::getCurrentIdentityID() const
{
	try {return getIdentity();}
	catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getCurrentIdentityID(...)\n");}
	return STORE_INVALID_IDENTITY;
}

IdentityID Session::getIdentityID(const char *identityName) 
{
	try {
		IdentityID iid=STORE_INVALID_IDENTITY;
		if (identityName!=NULL && !ctx->inShutdown()) {
			Identity *ident = (Identity*)ctx->identMgr->find(identityName,strlen(identityName));
			if (ident!=NULL) {iid=ident->getID(); ident->release();}
		}
		return iid;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getIdentityID(...)\n");}
	return STORE_INVALID_IDENTITY;
}

RC Session::impersonate(const char *identityName) 
{
	try {
		if (identityName==NULL) return RC_INVPARAM; if (!checkAdmin()) return RC_NOACCESS; if (ctx->inShutdown()) return RC_SHUTDOWN;
		Identity *ident=(Identity*)ctx->identMgr->find(identityName,strlen(identityName)); if (ident==NULL) return RC_NOTFOUND;
		identity=ident->getID(); ident->release(); return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::impersonate(...)\n"); return RC_INTERNAL;}
}

size_t Session::getStoreIdentityName(char buf[],size_t lbuf)
{
	try {return getIdentityName(STORE_OWNER,buf,lbuf);}
	catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getStoreIdentityName(...)\n");}
	return 0;
}

size_t Session::getIdentityName(IdentityID iid,char buf[],size_t lbuf)
{
	try {
		size_t len=0; const StrLen *sl; if (ctx->inShutdown()) return 0;
		Identity *ident=(Identity*)ctx->identMgr->ObjMgr::find(unsigned(iid));
		if (ident!=NULL) {
			if ((sl=ident->getName())!=NULL) {len=sl->len; if (buf!=NULL && lbuf>0) {lbuf=min(len,lbuf-1); memcpy(buf,sl->str,lbuf); buf[lbuf]=0;}}
			ident->release();
		}
		return len;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getIdentityName(IdentityID=%08X,...)\n",iid);}
	return 0;
}

size_t Session::getCertificate(IdentityID iid,unsigned char buf[],size_t lbuf)
{
	try {
		size_t keyLength=0; byte *cert; if (ctx->inShutdown()||!checkAdmin()) return 0;
		Identity *ident = (Identity*)ctx->identMgr->ObjMgr::find(unsigned(iid));
		if (ident!=NULL) {
			if ((keyLength=ident->getCertLength())>0 && buf!=NULL && lbuf!=0 && (cert=ident->getCertificate(this))!=NULL)
				{if (lbuf>keyLength) lbuf=keyLength; memcpy(buf,cert,lbuf); free(cert);}
			ident->release();
		}
		return keyLength;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getIdentityKey(IdentityID=%08X,...)\n",iid);}
	return 0;
}

RC Session::changeStoreIdentity(const char *newIdentity)
{
	try {
		if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY; if (newIdentity==NULL) return RC_INVPARAM;
		return checkAdmin()?ctx->identMgr->rename(STORE_OWNER,newIdentity,strlen(newIdentity)):RC_NOACCESS;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::changeStoreIdentity(...)\n"); return RC_INTERNAL;}
}

RC Session::modifyPIN(const PID& id,const Value *values,unsigned nValues,unsigned md,const ElementID *eids,unsigned *pNFailed,const Value *params,unsigned nParams)
{
	try {
		PageAddr addr; if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		if (!isRemote(id) && addr.convert(id.pid)) setExtAddr(addr); 
		Values vv(params,nParams); EvalCtx ectx(this,NULL,0,NULL,0,&vv,1,NULL,NULL,ECT_INSERT); TxGuard txg(this);
		RC rc=ctx->queryMgr->modifyPIN(ectx,id,values,nValues,NULL,NULL,md|MODE_CHECKBI,eids,pNFailed);
		setExtAddr(PageAddr::noAddr); return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::modifyPIN(...)\n"); return RC_INTERNAL;}
}

RC Session::deletePINs(IPIN **pins,unsigned nPins,unsigned md)
{
	try {
		if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		PID *pids=(PID*)alloca(nPins*sizeof(PID)); if (pids==NULL) return RC_NOMEM;
		for (unsigned j=0; j<nPins; j++) pids[j]=pins[j]->getPID();
		TxGuard txg(this); RC rc=ctx->queryMgr->deletePINs(EvalCtx(this),(PIN**)pins,pids,nPins,md|MODE_DEVENT);
		if (rc==RC_OK) for (unsigned i=0; i<nPins; i++) {pins[i]->destroy(); pins[i]=NULL;}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::deletePINs(...)\n"); return RC_INTERNAL;}
}

RC Session::deletePINs(const PID *pids,unsigned nPids,unsigned md)
{
	try {
		if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		TxGuard txg(this); return ctx->queryMgr->deletePINs(EvalCtx(this),NULL,pids,nPids,md|MODE_DEVENT);
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::deletePINs(...)\n"); return RC_INTERNAL;}
}

RC Session::undeletePINs(const PID *pids,unsigned nPids)
{
	try {
		if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		TxGuard txg(this); return ctx->queryMgr->undeletePINs(EvalCtx(this),pids,nPids);
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::undeletePINs(...)\n"); return RC_INTERNAL;}
}

RC Session::startTransaction(TX_TYPE txt,TXI_LEVEL txl)
{
	try {
		if (ctx->inShutdown()) return RC_SHUTDOWN; 
		if (txt!=TXT_READONLY && ctx->isServerLocked()) return RC_READONLY;
		return isRestore()?RC_OTHER:ctx->txMgr->startTx(this,txt,txl);
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::startTransaction()\n"); return RC_INTERNAL;}
}

RC Session::commit(bool fAll)
{
	try {return isRestore()?RC_OTHER:ctx->txMgr->commitTx(this,fAll);}
	catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::commit()\n"); return RC_INTERNAL;}
}

RC Session::rollback(bool fAll)
{
	try {return isRestore()?RC_OTHER:ctx->txMgr->abortTx(this,fAll?TXA_ALL:TXA_EXTERNAL);}
	catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::rollback()\n"); return RC_INTERNAL;}
}

void Session::setLimits(unsigned xSS,unsigned xOC)
{
	try {if (xSS!=0) xSyncStack=xSS; if (xOC!=0) xOnCommit=xOC;}
	catch (...) {report(MSG_ERROR,"Exception in ISession::setLimits()\n");}
}

RC Session::getURI(URIID id,char *buf,size_t& lbuf,bool fFull)
{
	try {
		if (id==STORE_INVALID_URIID || buf!=NULL && lbuf==0) return RC_INVPARAM; if (ctx->inShutdown()) return RC_SHUTDOWN;
		const char *name; size_t lname;
		if (id<=MAX_BUILTIN_URIID && (name=NamedMgr::getBuiltinName(id,lname))!=NULL) {
			const char *prefix; size_t l;
			if (fFull) {
				if (id>=MIN_BUILTIN_SERVICE) {prefix=AFFINITY_SERVICE_PREFIX; l=sizeof(AFFINITY_SERVICE_PREFIX);} else {prefix=AFFINITY_STD_URI_PREFIX; l=sizeof(AFFINITY_STD_URI_PREFIX);}
			} else {
				if (id>=MIN_BUILTIN_SERVICE) {prefix=AFFINITY_SRV_QPREFIX; l=sizeof(AFFINITY_SRV_QPREFIX);} else {prefix=AFFINITY_STD_QPREFIX; l=sizeof(AFFINITY_STD_QPREFIX);}
			}
			if (buf==NULL) lbuf=l+lname;
			else {l=min(lbuf,l)-1; memcpy(buf,prefix,l); if (lbuf>l) {size_t ll=min(lbuf-l-1,lname); memcpy(buf+l,name,ll); l+=ll;} buf[lbuf=l]=0;}
		} else {
			URI *uri=(URI*)ctx->uriMgr->ObjMgr::find(id); if (uri==NULL) {lbuf=0; return RC_NOTFOUND;}
			const StrLen *s=uri->getURI();
			if (s==NULL || s->len==0) lbuf=0;
			else if (buf==NULL) lbuf=s->len;
			else if (!fFull && s->len>sizeof(AFFINITY_STD_URI_PREFIX)-1 && memcmp(s->str,AFFINITY_STD_URI_PREFIX,sizeof(AFFINITY_STD_URI_PREFIX)-1)==0) {
				size_t ll=min(lbuf,sizeof(AFFINITY_STD_QPREFIX))-1; memcpy(buf,AFFINITY_STD_QPREFIX,ll);
				if (lbuf>ll) {size_t l2=min(lbuf-ll-1,ll-sizeof(AFFINITY_STD_URI_PREFIX)+1); memcpy(buf+ll,s->str+sizeof(AFFINITY_STD_URI_PREFIX)-1,l2); ll+=l2;}
				buf[lbuf=ll]=0;
			} else if (!fFull && s->len>sizeof(AFFINITY_SERVICE_PREFIX)-1 && memcmp(s->str,AFFINITY_SERVICE_PREFIX,sizeof(AFFINITY_SERVICE_PREFIX)-1)==0) {
				size_t ll=min(lbuf,sizeof(AFFINITY_SRV_QPREFIX))-1; memcpy(buf,AFFINITY_SRV_QPREFIX,ll);
				if (lbuf>ll) {size_t l2=min(lbuf-ll-1,ll-sizeof(AFFINITY_SERVICE_PREFIX)+1); memcpy(buf+ll,s->str+sizeof(AFFINITY_SERVICE_PREFIX)-1,l2); ll+=l2;}
				buf[lbuf=ll]=0;
			} else {memcpy(buf,s->str,min(s->len,lbuf)); if (lbuf>s->len) lbuf=s->len+1; buf[--lbuf]=0;}
			uri->release();
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::getURI(URIID=%08X,...)\n",id); return RC_INTERNAL;}
}

IPIN *Session::getPIN(const PID& id,unsigned md) 
{
	try {return !ctx->inShutdown()?PIN::getPIN(id,STORE_CURRENT_VERSION,this,md|LOAD_EXT_ADDR|LOAD_CLIENT):NULL;}
	catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getPIN(PID=" _LX_FM ",IdentityID=%08X)\n",id.pid,id.ident);}
	return NULL;
}

IPIN *Session::getPIN(const Value& v,unsigned md) 
{
	try {return !ctx->inShutdown()&&v.type==VT_REFID?PIN::getPIN(v.id,/*(v.meta&META_PROP_FIXEDVERSION)!=0?v.vpid.vid:*/STORE_CURRENT_VERSION,this,md|LOAD_EXT_ADDR|LOAD_CLIENT):NULL;}
	catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getPIN(Value&)\n");}
	return NULL;
}

RC Session::getValue(Value& res,const PID& id,PropertyID pid,ElementID eid)
{
	try {
		if (!id.isPID()) return RC_INVPARAM; if (ctx->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(this); PageAddr addr; if (addr.convert(uint64_t(id.pid))) setExtAddr(addr);
		RC rc=ctx->queryMgr->loadValue(this,id,pid,eid,res,LOAD_EXT_ADDR|LOAD_CLIENT);
		setExtAddr(PageAddr::noAddr); return rc;
	} catch (RC rc) {return rc;}
	catch (...) {report(MSG_ERROR,"Exception in ISession::getValue(PID=" _LX_FM ",IdentityID=%08X,PropID=%08X)\n",id.pid,id.ident,pid); return RC_INTERNAL;}
}

RC Session::getPINEvents(DataEventID *&devs,unsigned& ndevs,const PID& id)
{
	try {
		devs=NULL; ndevs=0;
		if (!id.isPID()) return RC_INVPARAM; if (ctx->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(this); DetectedEvents clr(this,ctx); PINx pex(this,id); pex.epr.flags|=PINEX_EXTPID; RC rc;
		if ((rc=pex.getBody())==RC_OK && (pex.mode&PIN_HIDDEN)==0)
			if ((rc=ctx->classMgr->detect(&pex,clr,this))==RC_OK && clr.ndevs!=0) {
				if ((devs=new(this) DataEventID[clr.ndevs])==NULL) rc=RC_NOMEM;
				else {ndevs=clr.ndevs; for (unsigned i=0; i<clr.ndevs; i++) devs[i]=clr.devs[i]->cid;}
			}
		return rc;
	} catch (RC rc) {return rc;}
	catch (...) {report(MSG_ERROR,"Exception in ISession::getPINEvents(PID=" _LX_FM ",IdentityID=%08X)\n",id.pid,id.ident); return RC_INTERNAL;}
}

bool Session::isCached(const PID& id)
{
	try {
		if (ctx->inShutdown()) return false;
		return isRemote(id)?ctx->netMgr->isCached(id):true;	// check correct local addr
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::isCached(PID=" _LX_FM ",IdentityID=%08X)\n",id.pid,id.ident);}
	return false;
}

unsigned Session::getStoreID(const PID& id)
{
	return ushort(id.pid>>48);
}

unsigned Session::getLocalStoreID()
{
	try {return ctx->storeID;}
	catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getLocalStoreID()\n");}
	return ~0u;
}

RC Session::getDataEventID(const char *className,DataEventID& cid)
{
	try {
		if (className==NULL) return RC_INVPARAM; if (ctx->inShutdown()) return RC_SHUTDOWN;
		size_t ln=strlen(className),lPref; const char *pref;
		if (!hasPrefix(className,ln) && (pref=ctx->namedMgr->getStorePrefix(lPref))!=NULL) {
			char *p=(char*)alloca(lPref+ln+1); if (p==NULL) return RC_NOMEM;
			memcpy(p,pref,lPref); memcpy(p+lPref,className,ln+1); className=p; ln+=lPref;
		}
		URI *uri=(URI*)ctx->uriMgr->find(className,ln); if (uri==NULL) return RC_NOTFOUND; cid=uri->getID(); uri->release();
		DataEvent *dev=ctx->classMgr->getDataEvent(cid); if (dev==NULL) return RC_NOTFOUND; else {dev->release(); return RC_OK;}
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::getDataEventID()\n"); return RC_INTERNAL;}
}

RC Session::enableClassNotifications(DataEventID cid,unsigned notifications)
{
	try {
		if (ctx->inShutdown()) return RC_SHUTDOWN;
		DataEvent *dev=ctx->classMgr->getDataEvent(cid); if (dev==NULL) return RC_NOTFOUND;
		RC rc=ctx->classMgr->enable(this,dev,notifications); dev->release(); return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::enableClassNotifications()\n"); return RC_INTERNAL;}
}

RC Session::rebuildIndices(const DataEventID *cidx,unsigned ndevs)
{
	try {
		if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		if (!checkAdmin()) return RC_NOACCESS;
		//...
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::rebuildFamilyIndices()\n"); return RC_INTERNAL;}
}

RC Session::rebuildIndexFT()
{
	try {
		if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		return checkAdmin()?ctx->ftMgr->rebuildIndex(this):RC_NOACCESS;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::rebuildIndexFT()\n"); return RC_INTERNAL;}
}

RC Session::createIndexNav(DataEventID cid,IndexNav *&nav)
{
	try {
		nav=NULL; if (ctx->inShutdown()) return RC_SHUTDOWN;
		DataEvent *dev=ctx->classMgr->getDataEvent(cid); if (dev==NULL) return RC_NOTFOUND;
		DataIndex *cidx=dev->getIndex(); RC rc=RC_OK;
		if (cidx!=NULL /*&& !qry->vars[0].condIdx->isExpr() && (qry->vars[0].condIdx->pid==pid || pid==STORE_INVALID_URIID)*/) {
			if ((nav=new(cidx->getNSegs(),this) IndexNavImpl(this,cidx))==NULL) rc=RC_NOMEM;
		} else {
		//...
			rc=RC_INVPARAM;
		}
		if (cidx==NULL || rc!=RC_OK) dev->release();
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::createIndexNav()\n"); return RC_INTERNAL;}
}

RC Session::listValues(DataEventID cid,PropertyID pid,IndexNav *&ven)
{
	try {
		ven=NULL; if (!checkAdmin()) return RC_NOACCESS; if (ctx->inShutdown()) return RC_SHUTDOWN;
		DataEvent *dev=ctx->classMgr->getDataEvent(cid); if (dev==NULL) return RC_NOTFOUND;
		DataIndex *cidx=dev->getIndex(); RC rc=RC_OK;
		if (cidx!=NULL /*&& !qry->vars[0].condIdx->isExpr() && (qry->vars[0].condIdx->pid==pid || pid==STORE_INVALID_URIID)*/) {
			if ((ven=new(cidx->getNSegs(),this) IndexNavImpl(this,cidx,/*qry->vars[0].condIdx->*/pid))==NULL) rc=RC_NOMEM;
		} else {
		//...
			rc=RC_INVPARAM;
		}
		if (cidx==NULL || rc!=RC_OK) dev->release();
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::listValues()\n"); return RC_INTERNAL;}
}

RC Session::listWords(const char *query,StringEnum *&sen)
{
	try {
		sen=NULL; if (ctx->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(this); return checkAdmin()?ctx->ftMgr->listWords(this,query,sen):RC_NOACCESS;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::listWords()\n"); return RC_INTERNAL;}
}

RC Session::getDataEventInfo(DataEventID cid,IPIN *&ret)
{
	try {
		ret=NULL; if (ctx->inShutdown()) return RC_SHUTDOWN;
		DataEvent *dev=ctx->classMgr->getDataEvent(cid); if (dev==NULL) return RC_NOTFOUND;
		PID id=dev->getPID(); PINx cb(this,id); cb=dev->getAddr(); dev->release(); PIN *pin=NULL;
		RC rc=cb.loadPIN(pin,LOAD_CLIENT); if (rc==RC_OK) rc=ctx->queryMgr->getDataEventInfo(this,pin);
		ret=pin; return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::getDataEventInfo()\n"); return RC_INTERNAL;}
}

static int __cdecl cmpMapElts(const void *v1, const void *v2)
{
	return AfyKernel::cmp(((MapElt*)v1)->key,((MapElt*)v2)->key,CND_SORT,NULL);
}

RC Session::createMap(const MapElt *elts,unsigned nElts,IMap *&map,bool fCopy)
{
	try {
		map=NULL; if (elts==0 || nElts==0) return RC_INVPARAM;
		bool fSort=false; MapElt *me=NULL; RC rc;
		if (fCopy) {
			if ((me=new(this) MapElt[nElts])==NULL) return RC_NOMEM;
			memset(me,0,nElts*sizeof(MapElt));
			for (unsigned i=0; i<nElts; i++) {
				if ((rc=copyV(elts[i].key,me[i].key,this))!=RC_OK || (rc=copyV(elts[i].val,me[i].val,this))!=RC_OK)
					{for (unsigned j=0; j<=i; j++) {freeV(me[j].key); freeV(me[j].val);} free(me); return rc;}
				if (i!=0) {int c=MapCmp::cmp(elts[i-1],elts[i].key); if (c==0) return RC_ALREADYEXISTS; if (c>0) fSort=true;}
			}
		} else {
			me=(MapElt*)elts;
			for (unsigned i=1; i<nElts; i++)
				{int c=MapCmp::cmp(elts[i-1],elts[i].key); if (c==0) return RC_ALREADYEXISTS; if (c>0) {fSort=true; break;}}
		}
		if (fSort) qsort(me,nElts,sizeof(MapElt),cmpMapElts);
		return (map=new(this) MemMap(me,nElts,this))!=NULL?RC_OK:RC_NOMEM;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::createMap()\n"); return RC_INTERNAL;}
}

RC Session::copyValue(const Value& src,Value& dest)
{
	try {setHT(src); return ctx->inShutdown()?RC_SHUTDOWN:copyV(src,dest,this);}
	catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::copyValue()\n"); return RC_INVPARAM;}
}

RC Session::copyValues(const Value *src,unsigned nValues,Value *&dest)
{
	try {return ctx->inShutdown()?RC_SHUTDOWN:copyV(src,nValues,dest,this);}
	catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::copyValues()\n"); return RC_INVPARAM;}
}

RC Session::convertValue(const Value& src,Value& dest,ValueType vt)
{
	try {setHT(src); return ctx->inShutdown()?RC_SHUTDOWN:convV(src,dest,vt,this);}
	catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::convertValue()\n"); return RC_INVPARAM;}
}

int Session::compareValues(const Value& v1,const Value& v2,bool fNCase)
{
	try {return ctx->inShutdown()?-1000:cmp(v1,v2,fNCase?CND_NCASE:0,this);}
	catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::compareValues()\n");}
	return -100;
}

void Session::freeValues(Value *vals,unsigned nvals)
{
	try {freeV(vals,nvals,this);} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::freeValues()\n");}
}

void Session::freeValue(Value& val)
{
	try {freeV(val);} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::freeValue()\n");}
}

void Session::setTimeZone(int64_t tzShift)
{
	try {tzShift=tzShift;} catch (RC) {}  
	catch (...) {report(MSG_ERROR,"Exception in ISession::setTimeZone()");}
}

RC Session::reservePages(const uint32_t *pages,unsigned nPages)
{
	try {return isRestore()?ctx->fsMgr->reservePages(pages,nPages):RC_NOACCESS;}
	catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::reservePages()\n"); return RC_INTERNAL;}
}

bool Session::login(const char *id,const char *pwd)
{
	Identity *ident=id!=NULL&&*id!='\0'?(Identity*)ctx->identMgr->find(id,strlen(id)):(Identity*)ctx->identMgr->ObjMgr::find(STORE_OWNER);
	if (ident==NULL) return false;
	const byte *spwd; bool fOK=true;
	if ((spwd=ident->getPwd())!=NULL) {
		if (pwd==NULL||*pwd=='\0') fOK=false;
		else {PWD_ENCRYPT pwd_enc((const byte*)pwd,strlen(pwd),spwd); fOK=pwd_enc.isOK();}
	} else if (pwd!=NULL && *pwd!='\0') fOK=false;
	if (fOK) setIdentity(ident->getID(),ident->mayInsert());
	ident->release();
	return fOK;
}

IAffinity *Session::getAffinity() const
{
	return ctx;
}

ISession *Session::clone(const char *id) const
{
	try {
		if (ctx->inShutdown() || isRestore()) return NULL;
		IdentityID iid=getIdentity(); Identity *ident=NULL; bool fInsert=mayInsert();
		if (id!=NULL) {
			if (iid!=STORE_OWNER || (ident=(Identity*)ctx->identMgr->find(id,strlen(id)))==NULL) return NULL;
			iid=ident->getID(); fInsert=ident->mayInsert(); ident->release();
		}
		Session *ns=Session::createSession(ctx,0,true); if (ns!=NULL) ns->setIdentity(iid,fInsert);
		return ns;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::clone()\n");}
	return NULL;
}

void Session::terminate() 
{
	try {Session::terminateSession();}	//???
	catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::terminate()\n");}
}

RC Session::attachToCurrentThread()
{
	return this!=NULL ? attach() : RC_INVPARAM;
}

RC Session::detachFromCurrentThread()
{
	return this!=NULL ? detach() : RC_INVPARAM;
}

RC Session::addOnCommit(OnCommit *oc) {
	if (tx.onCommit.count+1>xOnCommit && xOnCommit!=0) return RC_NOMEM; assert(oc!=NULL);
	tx.onCommit+=oc; return RC_OK;
}

RC Session::addOnCommit(OnCommitQ& oq) {
	if (tx.onCommit.count+oq.count>xOnCommit && xOnCommit!=0) return RC_NOMEM;
	tx.onCommit+=oq; oq.reset(); return RC_OK;
}

uint64_t Session::getCodeTrace()
{
	uint64_t ct=codeTrace; codeTrace=0; return ct;
}

SyncCall::SyncCall(Session *s) : ses(s)
{
	if (s!=NULL && ++s->nSyncStack>s->xSyncStack) {--s->nSyncStack; throw RC_NOMEM;}
}

SyncCall::~SyncCall()
{
	if (ses!=NULL && ses->nSyncStack!=0) --ses->nSyncStack;
}

const PageAddr PageAddr::noAddr={INVALID_PAGEID,INVALID_INDEX};

bool PageAddr::convert(uint64_t pid) {
	if (pid==STORE_INVALID_PID) return false;
	pageID=PageID(pid>>16); idx=PageIdx(pid&0xFFFF);
	if (pageID==INVALID_PAGEID||(idx&0x8000)!=0||ushort(pid>>48)!=StoreCtx::get()->storeID) return false;
	// compare pageID with approx. store size (sum of length of open data files)
	return true;
}

PageAddr::operator uint64_t() const 
{
	return (uint64_t(StoreCtx::get()->storeID)<<32|pageID)<<16|idx;
}
