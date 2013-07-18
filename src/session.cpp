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

#include "txmgr.h"
#include "logmgr.h"
#include "lock.h"
#include "classifier.h"
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

void StoreCtx::release()
{
	if (mem!=NULL) {mem->release(); mem=NULL;}
}

size_t StoreCtx::getPublicKey(uint8_t *buf,size_t lbuf,bool fB64)
{
	if (!fB64) {memcpy(buf,pubKey,min(lbuf,sizeof(pubKey))); return sizeof(pubKey);}
	size_t l=min((sizeof(pubKey)+2)/3,lbuf/4)*4; base64enc(pubKey,sizeof(pubKey),(char*)buf,l);
	return l;
}

//-------------------------------------------------------------------------------------

ISession *StoreCtx::startSession(const char *ident,const char *pwd)
{
	try {
		if (inShutdown()||theCB->state==SST_RESTORE) return NULL;
		Session *s=Session::createSession(this); if (s==NULL) return NULL;
		if (!classMgr->isInit()) {s->setIdentity(STORE_OWNER,true); classMgr->initClasses(s); s->setIdentity(STORE_INVALID_IDENTITY,false);}
		if (!s->login(ident,pwd)) {s->terminate(); s=NULL;}
		return s;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IAffinity::startSession()\n");}
	return NULL;
}

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
//			byte buf[sizeof(TIMESTAMP)+sizeof(unsigned)]; getTimestamp(*(TIMESTAMP*)buf);
//			memcpy(buf+sizeof(TIMESTAMP),&ses->identity,sizeof(unsigned));
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
			ses->ctx->txMgr->abort(ses,TXA_ALL);
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
	firstLSN(0),undoNextLSN(0),flushLSN(0),sesLSN(0),nLogRecs(0),tx(this),subTxCnt(0),mini(NULL),nTotalIns(0),xHeapPage(INVALID_PAGEID),forcedPage(INVALID_PAGEID),
	classLocked(RW_NO_LOCK),fAbort(false),repl(NULL),itf(0),serviceTab(NULL),iTrace(NULL),traceMode(0),nSrvCtx(0),xSrvCtx(MAX_SERV_CTX),active(NULL),defExpiration(0),tzShift(0)
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
		if (reuse.pinPages!=NULL) for (unsigned i=0; i<reuse.nPINPages; i++)
			ctx->heapMgr->HeapPageMgr::reuse(reuse.pinPages[i].pid,reuse.pinPages[i].space,ctx);
		if (reuse.ssvPages!=NULL) for (unsigned i=0; i<reuse.nSSVPages; i++)
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

RC Session::latch(PBlock *pb,unsigned mode)
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

void Session::release()
{
	if (this!=NULL && mem!=NULL) mem->release();
}

bool Session::hasPrefix(const char *name,size_t lName)
{
	return name==NULL || memchr(name,'/',lName)!=NULL || memchr(name,'#',lName)!=NULL;
}

RC Session::mapURIs(unsigned nURIs,URIMap URIs[],const char *base)
{
	try {
		if (ctx->inShutdown()) return RC_SHUTDOWN;
		char *URIBase=NULL; size_t lURIBase=base!=NULL?strlen(base):0,lURIBaseBuf=0; 
		for (unsigned i=0; i<nURIs; i++) {
			const char *URIName=URIs[i].URI; if (URIName==NULL || *URIName=='\0') return RC_INVPARAM;
			if (lURIBase!=0) {
				size_t lName = strlen(URIName);
				if (!Session::hasPrefix(URIName,lName)) {
					if (lURIBase+lName+1>lURIBaseBuf) {
						if ((URIBase=(char*)realloc(URIBase,lURIBase+lName+1,lURIBaseBuf))==NULL) return RC_NORESOURCES;
						if (lURIBaseBuf==0) memcpy(URIBase,base,lURIBase); lURIBaseBuf=lURIBase+lName+1;
					}
					memcpy(URIBase+lURIBase,URIName,lName+1); URIName=URIBase;
				}
			}
			URI *uri=ctx->isServerLocked()?(URI*)ctx->uriMgr->find(URIName):(URI*)ctx->uriMgr->insert(URIName);
			URIs[i].uid=uri!=NULL?uri->getID():STORE_INVALID_URIID; if (uri!=NULL) uri->release();
		}
		if (URIBase!=NULL) free(URIBase);
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::mapURIs(...)\n"); return RC_INTERNAL;}
}

void Session::setInterfaceMode(unsigned md)
{
	try {
		itf=md; if (getIdentity()==STORE_OWNER) setReplication((md&ITF_REPLICATION)!=0);
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::setInterfaceMode(...)\n");}
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
		if (!ctx->inShutdown() && !ctx->isServerLocked() && checkAdmin()) {
			size_t lpwd=pwd!=NULL?strlen(pwd):0;
			PWD_ENCRYPT epwd((byte*)pwd,lpwd); const byte *enc=pwd!=NULL&&lpwd>0?epwd.encrypted():NULL;
			Identity *ident=(Identity*)ctx->identMgr->insert(identName,enc,cert,lcert,fMayInsert);
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
		if (!ctx->inShutdown() && !ctx->isServerLocked() && checkAdmin() && (lPwd==PWD_ENC_SIZE || lPwd==0)) {
			Identity *ident=(Identity*)ctx->identMgr->insert(identName,lPwd!=0?pwd:NULL,cert,lcert,fMayInsert);
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
		if (!ctx->inShutdown()) {
			Identity *ident = (Identity*)ctx->identMgr->find(identityName);
			if (ident!=NULL) {iid=ident->getID(); ident->release();}
		}
		return iid;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getIdentityID(...)\n");}
	return STORE_INVALID_IDENTITY;
}

RC Session::impersonate(const char *identityName) 
{
	try {
		if (!checkAdmin()) return RC_NOACCESS; if (ctx->inShutdown()) return RC_SHUTDOWN;
		Identity *ident=(Identity*)ctx->identMgr->find(identityName);
		if (ident==NULL) return RC_NOTFOUND;
		identity=ident->getID();
		ident->release();
		return RC_OK;
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
		size_t len=0; if (ctx->inShutdown()) return 0;
		Identity *ident=(Identity*)ctx->identMgr->ObjMgr::find(unsigned(iid));
		if (ident!=NULL) {
			if (ident->getName()!=NULL) {
				len=strlen(ident->getName());
				if (buf!=NULL && lbuf>0) strncpy(buf,ident->getName(),lbuf-1)[lbuf-1]=0;
			}
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
		if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		return checkAdmin()?ctx->identMgr->rename(STORE_OWNER,newIdentity):RC_NOACCESS;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::changeStoreIdentity(...)\n"); return RC_INTERNAL;}
}

RC Session::commitPINs(IPIN *const *pins,unsigned nPins,unsigned md,const AllocCtrl *actrl,const Value *params,unsigned nParams,const IntoClass *into,unsigned nInto)
{
	try {
		if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		ValueV vv(params,nParams); EvalCtx ectx(this,NULL,0,NULL,0,&vv,1,NULL,NULL,ECT_INSERT); TxGuard txg(this); 
		return ctx->queryMgr->persistPINs(ectx,(PIN*const*)pins,nPins,md,actrl,NULL,into,nInto);	// MODE_ENAV???
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::commitPINs(...)\n"); return RC_INTERNAL;}
}

RC Session::modifyPIN(const PID& id,const Value *values,unsigned nValues,unsigned md,const ElementID *eids,unsigned *pNFailed,const Value *params,unsigned nParams)
{
	try {
		PageAddr addr; if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		if (!isRemote(id) && addr.convert(id.pid)) setExtAddr(addr); 
		ValueV vv(params,nParams); EvalCtx ectx(this,NULL,0,NULL,0,&vv,1,NULL,NULL,ECT_INSERT); TxGuard txg(this);
		RC rc=ctx->queryMgr->modifyPIN(ectx,id,values,nValues,NULL,NULL,md|MODE_CHECKBI,eids,pNFailed);
		setExtAddr(PageAddr::invAddr); return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::modifyPIN(...)\n"); return RC_INTERNAL;}
}

RC Session::deletePINs(IPIN **pins,unsigned nPins,unsigned md)
{
	try {
		if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		PID *pids=(PID*)alloca(nPins*sizeof(PID)); if (pids==NULL) return RC_NORESOURCES;
		for (unsigned j=0; j<nPins; j++) pids[j]=pins[j]->getPID();
		TxGuard txg(this); RC rc=ctx->queryMgr->deletePINs(EvalCtx(this),(PIN**)pins,pids,nPins,md|MODE_CLASS);
		if (rc==RC_OK) for (unsigned i=0; i<nPins; i++) {pins[i]->destroy(); pins[i]=NULL;}
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::deletePINs(...)\n"); return RC_INTERNAL;}
}

RC Session::deletePINs(const PID *pids,unsigned nPids,unsigned md)
{
	try {
		if (ctx->inShutdown()) return RC_SHUTDOWN; if (ctx->isServerLocked()) return RC_READONLY;
		TxGuard txg(this); return ctx->queryMgr->deletePINs(EvalCtx(this),NULL,pids,nPids,md|MODE_CLASS);
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

RC Session::getURI(URIID id,char *buf,size_t& lbuf,bool fFull)
{
	try {
		if (id==STORE_INVALID_URIID || buf!=NULL && lbuf==0) return RC_INVPARAM; if (ctx->inShutdown()) return RC_SHUTDOWN;
		const char *name; size_t lname;
		if (id<=MAX_BUILTIN_URIID && (name=Classifier::getBuiltinName(id,lname))!=NULL) {
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
			const char *s=uri->getURI();
			if (s==NULL) lbuf=0;
			else {
				size_t l=strlen(s);
				if (buf==NULL) lbuf=l;
				else if (!fFull && l>sizeof(AFFINITY_STD_URI_PREFIX)-1 && memcmp(s,AFFINITY_STD_URI_PREFIX,sizeof(AFFINITY_STD_URI_PREFIX)-1)==0) {
					size_t ll=min(lbuf,sizeof(AFFINITY_STD_QPREFIX))-1; memcpy(buf,AFFINITY_STD_QPREFIX,ll);
					if (lbuf>ll) {size_t l2=min(lbuf-ll-1,ll-sizeof(AFFINITY_STD_URI_PREFIX)+1); memcpy(buf+ll,s+sizeof(AFFINITY_STD_URI_PREFIX)-1,l2); ll+=l2;}
					buf[lbuf=ll]=0;
				} else if (!fFull && l>sizeof(AFFINITY_SERVICE_PREFIX)-1 && memcmp(s,AFFINITY_SERVICE_PREFIX,sizeof(AFFINITY_SERVICE_PREFIX)-1)==0) {
					size_t ll=min(lbuf,sizeof(AFFINITY_SRV_QPREFIX))-1; memcpy(buf,AFFINITY_SRV_QPREFIX,ll);
					if (lbuf>ll) {size_t l2=min(lbuf-ll-1,ll-sizeof(AFFINITY_SERVICE_PREFIX)+1); memcpy(buf+ll,s+sizeof(AFFINITY_SERVICE_PREFIX)-1,l2); ll+=l2;}
					buf[lbuf=ll]=0;
				} else {memcpy(buf,s,min(l+1,lbuf)); if (lbuf>l) lbuf=l; else buf[--lbuf]=0;}
			}
			uri->release();
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::getURI(URIID=%08X,...)\n",id); return RC_INTERNAL;}
}

IPIN *Session::getPIN(const PID& id,unsigned md) 
{
	try {return !ctx->inShutdown()?PIN::getPIN(id,STORE_CURRENT_VERSION,this,md|LOAD_EXT_ADDR|LOAD_ENAV):NULL;}
	catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getPIN(PID="_LX_FM",IdentityID=%08X)\n",id.pid,id.ident);}
	return NULL;
}

IPIN *Session::getPIN(const Value& v,unsigned md) 
{
	try {return !ctx->inShutdown()&&v.type==VT_REFID?PIN::getPIN(v.id,/*(v.meta&META_PROP_FIXEDVERSION)!=0?v.vpid.vid:*/STORE_CURRENT_VERSION,this,md|LOAD_EXT_ADDR|LOAD_ENAV):NULL;}
	catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getPIN(Value&)\n");}
	return NULL;
}

IPIN *Session::getPINByURI(const char *uri,unsigned md)
{
	try {
#if 0
		Value w; w.set(uri); PID id; if (ctx->inShutdown()) return NULL;
		PIN *pin=uri!=NULL && ctx->uriMgr->URItoPID(w,id)==RC_OK?PIN::getPIN(id,STORE_CURRENT_VERSION,ses,md|mode):NULL;
		return pin;
#else
		return NULL;
#endif
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::getPINByURI()\n");}
	return NULL;
}

RC Session::getValues(Value *pv,unsigned nv,const PID& id)
{
	try {
		if (id.pid==STORE_INVALID_PID) return RC_INVPARAM; if (ctx->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(this); PageAddr addr; if (addr.convert(OID(id.pid))) setExtAddr(addr);
		RC rc=ctx->queryMgr->loadValues(pv,nv,id,this,LOAD_EXT_ADDR|LOAD_ENAV);
		setExtAddr(PageAddr::invAddr); return rc;
	} catch (RC rc) {return rc;} 
	catch (...) {report(MSG_ERROR,"Exception in ISession::getValues(PID="_LX_FM",IdentityID=%08X)\n",id.pid,id.ident); return RC_INTERNAL;}
}

RC Session::getValue(Value& res,const PID& id,PropertyID pid,ElementID eid)
{
	try {
		if (id.pid==STORE_INVALID_PID) return RC_INVPARAM; if (ctx->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(this); PageAddr addr; if (addr.convert(OID(id.pid))) setExtAddr(addr);
		RC rc=ctx->queryMgr->loadValue(this,id,pid,eid,res,LOAD_EXT_ADDR|LOAD_ENAV);
		setExtAddr(PageAddr::invAddr); return rc;
	} catch (RC rc) {return rc;}
	catch (...) {report(MSG_ERROR,"Exception in ISession::getValue(PID="_LX_FM",IdentityID=%08X,PropID=%08X)\n",id.pid,id.ident,pid); return RC_INTERNAL;}
}

RC Session::getValue(Value& res,const PID& id)
{
	try {
		if (id.pid==STORE_INVALID_PID) return RC_INVPARAM; if (ctx->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(this); PageAddr addr; if (addr.convert(OID(id.pid))) setExtAddr(addr);
		RC rc=ctx->queryMgr->getPINValue(id,res,LOAD_EXT_ADDR|LOAD_ENAV,this);
		setExtAddr(PageAddr::invAddr); return rc;
	} catch (RC rc) {return rc;}
	catch (...) {report(MSG_ERROR,"Exception in ISession::getValue(PID="_LX_FM",IdentityID=%08X)\n",id.pid,id.ident); return RC_INTERNAL;}
}

RC Session::getPINClasses(ClassID *&clss,unsigned& nclss,const PID& id)
{
	try {
		clss=NULL; nclss=0;
		if (id.pid==STORE_INVALID_PID) return RC_INVPARAM; if (ctx->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(this); ClassResult clr(this,ctx); PINx pex(this,id); pex.epr.flags|=PINEX_EXTPID; RC rc;
		if ((rc=ctx->queryMgr->getBody(pex))==RC_OK && (pex.mode&PIN_HIDDEN)==0)
			if ((rc=ctx->classMgr->classify(&pex,clr))==RC_OK && clr.nClasses!=0) {
				if ((clss=new(this) ClassID[clr.nClasses])==NULL) rc=RC_NORESOURCES;
				else {nclss=clr.nClasses; for (unsigned i=0; i<clr.nClasses; i++) clss[i]=clr.classes[i]->cid;}
			}
		return rc;
	} catch (RC rc) {return rc;}
	catch (...) {report(MSG_ERROR,"Exception in ISession::getPINClasses(PID="_LX_FM",IdentityID=%08X)\n",id.pid,id.ident); return RC_INTERNAL;}
}

bool Session::isCached(const PID& id)
{
	try {
		if (ctx->inShutdown()) return false;
		return isRemote(id)?ctx->netMgr->isCached(id):true;	// check correct local addr
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::isCached(PID="_LX_FM",IdentityID=%08X)\n",id.pid,id.ident);}
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

RC Session::getClassID(const char *className,ClassID& cid)
{
	try {
		if (ctx->inShutdown()) return RC_SHUTDOWN;
		URI *uri=(URI*)ctx->uriMgr->find(className); if (uri==NULL) return RC_NOTFOUND; cid=uri->getID(); uri->release();
		Class *cls=ctx->classMgr->getClass(cid); if (cls==NULL) return RC_NOTFOUND; else {cls->release(); return RC_OK;}
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::getClassID()\n"); return RC_INTERNAL;}
}

RC Session::enableClassNotifications(ClassID cid,unsigned notifications)
{
	try {
		if (ctx->inShutdown()) return RC_SHUTDOWN;
		Class *cls=ctx->classMgr->getClass(cid); if (cls==NULL) return RC_NOTFOUND;
		RC rc=ctx->classMgr->enable(this,cls,notifications); cls->release(); return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::enableClassNotifications()\n"); return RC_INTERNAL;}
}

RC Session::rebuildIndices(const ClassID *cidx,unsigned nClasses)
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

RC Session::createIndexNav(ClassID cid,IndexNav *&nav)
{
	try {
		nav=NULL; if (ctx->inShutdown()) return RC_SHUTDOWN;
		Class *cls=ctx->classMgr->getClass(cid); if (cls==NULL) return RC_NOTFOUND;
		ClassIndex *cidx=cls->getIndex(); RC rc=RC_OK;
		if (cidx!=NULL /*&& !qry->vars[0].condIdx->isExpr() && (qry->vars[0].condIdx->pid==pid || pid==STORE_INVALID_URIID)*/) {
			if ((nav=new(cidx->getNSegs(),this) IndexNavImpl(this,cidx))==NULL) rc=RC_NORESOURCES;
		} else {
		//...
			rc=RC_INVPARAM;
		}
		if (cidx==NULL || rc!=RC_OK) cls->release();
		return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::createIndexNav()\n"); return RC_INTERNAL;}
}

RC Session::listValues(ClassID cid,PropertyID pid,IndexNav *&ven)
{
	try {
		ven=NULL; if (!checkAdmin()) return RC_NOACCESS; if (ctx->inShutdown()) return RC_SHUTDOWN;
		Class *cls=ctx->classMgr->getClass(cid); if (cls==NULL) return RC_NOTFOUND;
		ClassIndex *cidx=cls->getIndex(); RC rc=RC_OK;
		if (cidx!=NULL /*&& !qry->vars[0].condIdx->isExpr() && (qry->vars[0].condIdx->pid==pid || pid==STORE_INVALID_URIID)*/) {
			if ((ven=new(cidx->getNSegs(),this) IndexNavImpl(this,cidx,/*qry->vars[0].condIdx->*/pid))==NULL) rc=RC_NORESOURCES;
		} else {
		//...
			rc=RC_INVPARAM;
		}
		if (cidx==NULL || rc!=RC_OK) cls->release();
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

RC Session::getClassInfo(ClassID cid,IPIN *&ret)
{
	try {
		ret=NULL; if (ctx->inShutdown()) return RC_SHUTDOWN;
		Class *cls=ctx->classMgr->getClass(cid); if (cls==NULL) return RC_NOTFOUND;
		PID id=cls->getPID(); PINx cb(this,id); cb=cls->getAddr(); cls->release(); PIN *pin=NULL;
		RC rc=ctx->queryMgr->loadPIN(this,id,pin,LOAD_ENAV,&cb); if (rc==RC_OK) rc=ctx->queryMgr->getClassInfo(this,pin);
		ret=pin; return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::getClassInfo()\n"); return RC_INTERNAL;}
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
			if ((me=new(this) MapElt[nElts])==NULL) return RC_NORESOURCES;
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
		return (map=new(this) MemMap(me,nElts,this))!=NULL?RC_OK:RC_NORESOURCES;
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

RC Session::reservePage(uint32_t pageID)
{
	try {return isRestore()?ctx->fsMgr->reservePage((PageID)pageID):RC_NOACCESS;}
	catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::reservePage(%08X)\n",pageID); return RC_INTERNAL;}
}

bool Session::login(const char *id,const char *pwd)
{
	Identity *ident=id!=NULL&&*id!='\0'?(Identity*)ctx->identMgr->find(id):(Identity*)ctx->identMgr->ObjMgr::find(STORE_OWNER);
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
			if (iid!=STORE_OWNER || (ident=(Identity*)ctx->identMgr->find(id))==NULL) return NULL;
			iid=ident->getID(); fInsert=ident->mayInsert(); ident->release();
		}
		Session *ns=Session::createSession(ctx); if (ns!=NULL) ns->setIdentity(iid,fInsert);
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
