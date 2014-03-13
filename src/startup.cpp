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

Written by Mark Venguerov 2004-2012

**************************************************************************************/

#include <stdio.h>
#ifdef WIN32
#include <intrin.h>
#else
#include <sys/stat.h>
#ifndef __APPLE__
#include <sys/sysinfo.h>
#endif
#endif

#include "startup.h"
#include "txmgr.h"
#include "fio.h"
#include "buffer.h"
#include "logmgr.h"
#include "fsmgr.h"
#include "pgheap.h"
#include "pgtree.h"
#include "idxtree.h"
#include "maps.h"
#include "lock.h"
#include "queryprc.h"
#include "stmt.h"
#include "parser.h"
#include "netmgr.h"
#include "ftindex.h"
#include "classifier.h"
#include "blob.h"
#include "fsm.h"
#include "service.h"
#include "ifacever.h"

using namespace AfyKernel;

namespace AfyKernel
{
SharedCounter StoreCtx::nStores;
StoreTable *volatile storeTable=NULL;
};

RC manageStores(const char *cmd,size_t lcmd,IAffinity *&store,const StartupParameters *sp,CompilationError *ce)
{
	try {
		store=NULL; if (cmd==NULL || lcmd==0) return RC_INVPARAM;
		if (ce!=NULL) {memset(ce,0,sizeof(CompilationError)); ce->msg="";}
		RequestQueue::startThreads(); initReport(); SInCtx::initKW();
		SInCtx in(NULL,cmd,lcmd,NULL,0,NULL);		//??? ma!!!
		try {in.parseManage(store,sp); return RC_OK;}
		catch (SynErr sy) {in.getErrorInfo(RC_SYNTAX,sy,ce); return RC_SYNTAX;}
		catch (RC rc) {in.getErrorInfo(rc,SY_ALL,ce); return rc;}
	} catch (RC rc) {return rc;} catch (...) {return RC_INTERNAL;}
}

char *StoreCtx::getDirString(const char *d,bool fRel)
{
	if (d==NULL || *d=='\0') return NULL;
	size_t i=strlen(d),f=d[i-1]!='/',l=0;
#ifdef WIN32
	if (f && (d[i-1]==':' || d[i-1]=='\\')) f=0;
#endif
	if (fRel && (const char*)directory!=NULL && memchr(".~/",d[0],3)==NULL
#ifdef WIN32
		&& d[0]!='\\' && (i<2 || d[1]!=':')
#endif
		) l=strlen(directory);
	char *s=(char*)malloc(l+i+f+1);
	if (s!=NULL) {
		if (l!=0) memcpy(s,directory,l);
		memcpy(s+l,d,i+1); if (f) {s[l+i]='/'; s[l+i+1]='\0';}
	}
	return s;
}

static const char *setDirectory(const char *dir,char *buf,size_t lbuf)
{
	bool fTryDot=true;
	if (dir!=NULL && *dir!='\0') {
		size_t i=strlen(dir),f=dir[i-1]!='/'; fTryDot=false;
#ifdef WIN32
		if (f && (dir[i-1]==':' || dir[i-1]=='\\')) f=0;
#endif
		if (f!=0) {
			if (i+f>=lbuf) return NULL;
			memcpy(buf,dir,i); buf[i]='/'; buf[i+1]='\0'; dir=buf;
		}
	} else if (((dir=getenv(HOME_ENV))!=NULL || (dir=getenv("HOME"))!=NULL) && *dir!='\0') {
		size_t l=strlen(dir); if (l+sizeof(STOREDIR)+2>=lbuf) return NULL; memcpy(buf,dir,l);
		if (buf[l-1]!='/'
#ifdef WIN32
			&& buf[l-1]!=':' && buf[l-1]!='\\'
#endif
			) buf[l++]='/';
		memcpy(buf+l,STOREDIR "/",sizeof(STOREDIR)+1); dir=buf;
	}
	for (;;dir=NULL) {
		if (dir==NULL || *dir=='\0') {if (!fTryDot) {dir=NULL; break;} dir="./"; fTryDot=false;}
#ifdef WIN32
		HANDLE hDir=CreateFile(dir,GENERIC_READ,FILE_SHARE_READ,NULL,OPEN_EXISTING,FILE_FLAG_BACKUP_SEMANTICS,NULL);
		if (hDir!=INVALID_HANDLE_VALUE) {::CloseHandle(hDir); break;}
#else
		struct stat64 st; if (stat64(dir,&st)==0 && S_ISDIR(st.st_mode)) break;
#endif
	}
	return dir;
}

static unsigned calcBuffers(unsigned nBuf,unsigned lPage)
{
	if (nBuf==0 || nBuf==~0u) {
#ifdef WIN32
		MEMORYSTATUSEX memStat={sizeof(MEMORYSTATUSEX)};
		if (GlobalMemoryStatusEx(&memStat)) nBuf=unsigned(min(memStat.ullAvailPhys,memStat.ullAvailVirtual)*7/8/lPage);
#elif defined(__APPLE__)
		//????
#elif defined(ANDROID)
		//???
#else
		struct sysinfo sysInfo;
		if (sysinfo(&sysInfo)==0) nBuf=unsigned(uint64_t(sysInfo.freeram)*7/8/lPage);
#endif
	}
	return nBuf==0||nBuf==~0u?0x1000:nBuf<MIN_BUFFERS?MIN_BUFFERS:nBuf;
}

ProcFlags ProcFlags::pf;

ProcFlags::ProcFlags() : flg(0)
{
#if defined(__arm__) || defined(__APPLE__) && !defined(TARGET_OS_MAC)
	flg|=PRCF_CMPXCHG16B;	//????
#else 
	int CPUInfo[4]; CPUInfo[0]=CPUInfo[1]=CPUInfo[2]=CPUInfo[3]=0;
#if defined(_M_IX86) || defined(_M_X64) || defined(_M_AMD64) || defined(IA64)
	__cpuid(CPUInfo,1);
#elif defined(__x86_64__)
	__asm__ __volatile__ ("cpuid":"=a" (CPUInfo[0]), "=b" (CPUInfo[1]), "=c" (CPUInfo[2]), "=d" (CPUInfo[3]) : "a" (1));
#elif defined(__i386__)
	__asm__ __volatile__ ( "pushl %%ebx \n\t cpuid \n\t movl %%ebx, %1 \n\t popl %%ebx \n\t" : "=a" (CPUInfo[0]), "=r" (CPUInfo[1]), "=c" (CPUInfo[2]), "=d" (CPUInfo[3]) : "a" (1));
#endif
	if ((CPUInfo[2]&0x2)!=0) flg|=PRCF_PCLMULQDQ;
	if ((CPUInfo[2]&0x2000)!=0) flg|=PRCF_CMPXCHG16B;
	if ((CPUInfo[2]&0x2000000)!=0) flg|=PRCF_AESNI;
#endif
}

static const char *getSrvDir(const char *dir,char *buf,size_t lbuf)
{
	if (dir==NULL || *dir=='\0') {
#ifdef WIN32
		DWORD dw=GetModuleFileName(NULL,buf,(DWORD)lbuf);
		if (dw!=0) for (dir=buf; dw--!=0; ) if (buf[dw]=='/' || buf[dw]=='\\') {buf[dw+1]='\0'; break;}
#else
		dir=getcwd(buf,lbuf);
		if (dir!=NULL && *dir!='\0') {size_t l=strlen(buf); if (buf[l-1]!='/' && l+1<lbuf) {buf[l]='/'; buf[l+1]='\0';}}
#endif
	}
	return dir;
}

static void checkStoreTable()
{
	if (storeTable==NULL) {
		StoreTable *st=new(&sharedAlloc) StoreTable(10,&sharedAlloc); if (st==NULL) throw RC_NORESOURCES;
		if (!casP(&storeTable,(StoreTable*)0,st)) {st->~StoreTable(); sharedAlloc.free(st);}
	}
}

RC openStore(const StartupParameters& params,IAffinity *&aff)
{
	StoreCtx *ctx=NULL; RC rc=RC_OK;
#if defined(_M_X64) || defined(_M_AMD64) || defined(IA64) || defined(__x86_64__)
	if ((ProcFlags::pf.flg&PRCF_CMPXCHG16B)==0) return RC_INVOP;
#endif
	try {
		initReport();
		if ((params.mode&STARTUP_RT)==0) RequestQueue::startThreads();

		if ((params.mode&STARTUP_WARM)!=0) {
			if ((ctx=(StoreCtx*)aff)==NULL) return RC_INVPARAM;
			HMAC hmac((byte*)&ctx,sizeof(StoreCtx*)); hmac.add((byte*)ctx,ctx->hmac-(byte*)ctx);
			if (memcmp(hmac.result(),ctx->hmac,HMAC_SIZE)==0) return RC_OK;
			if ((params.mode&STARTUP_FORCE_OPEN)==0) return RC_CORRUPTED;
			ctx=NULL;
		}

		checkStoreTable(); aff=NULL;

		char dirbuf[1024]; const char *dir=setDirectory(params.directory,dirbuf,sizeof(dirbuf));
		if (dir==NULL) {report(MSG_CRIT,"Directory doesn't exist or is protected\n"); return RC_NOTFOUND;}

		if ((ctx=storeTable->find(StrKey(dir)))!=NULL) {report(MSG_NOTICE,"Found open store in %s\n",dir); aff=ctx; return RC_OK;}

		report(MSG_NOTICE,"Affinity startup - version %d.%02d\n",STORE_VERSION/100,STORE_VERSION%100);

		if ((ctx=StoreCtx::createCtx(params.mode,dir,params.memory,params.lMemory))==NULL) return RC_NORESOURCES;

		dir=ctx->getDirectory();

		ctx->tqMgr=new(ctx) TimerQ(ctx);
		
		ctx->defaultService=params.service;

		if (ctx->memory==NULL && (ctx->fileMgr=new(ctx) FileMgr(ctx,params.maxFiles,getSrvDir(params.serviceDirectory,dirbuf,sizeof(dirbuf))))==NULL) throw RC_NORESOURCES;

		if ((ctx->cryptoMgr=CryptoMgr::get())==NULL) {report(MSG_CRIT,"Cannot initialize crypto\n"); throw RC_NORESOURCES;}

		SInCtx::initKW();

		int dataFileN=0; FileID lastFile=0; bool fForce=(params.mode&STARTUP_FORCE_OPEN)!=0;

		if (ctx->fileMgr==NULL) rc=((StoreCB*)ctx->memory)->magic!=0?StoreCB::open(ctx,NULL,params.password,params.mode):RC_NOTFOUND;
		else {
			if ((rc=StoreCB::open(ctx,STOREPREFIX MASTERFILESUFFIX,params.password,params.mode))==RC_OK) {
				if (ctx->theCB->nMaster!=1) {
					report(MSG_ERROR,"StoreCB: invalid nMaster field %d\n",ctx->theCB->nMaster); throw RC_CORRUPTED;
				} else {
					report(MSG_INFO,"Master record found in "STOREPREFIX MASTERFILESUFFIX"\n");
				}
			} 
			if (rc==RC_NOTFOUND && (rc=StoreCB::open(ctx,STOREPREFIX"1"MASTERFILESUFFIX,params.password,params.mode))==RC_OK) {
				if (ctx->theCB->nMaster<2 || ctx->theCB->nMaster>MAXMASTERFILES) {
					report(MSG_ERROR,"StoreCB: invalid nMaster field %d\n",ctx->theCB->nMaster); throw RC_CORRUPTED;
				} else {
					report(MSG_INFO,"Master record found in "STOREPREFIX"1"MASTERFILESUFFIX"\n");
					// open STOREPREFIX"2 to nMaster+1"MASTERFILESUFFIX
					// compare timestamp -> use latest
				}
			}
			if (rc==RC_NOTFOUND && (rc=StoreCB::open(ctx,STOREPREFIX DATAFILESUFFIX,params.password,params.mode))==RC_OK) {
				if (ctx->theCB->nMaster!=0) {
					report(MSG_ERROR,"StoreCB: invalid nMaster field %d\n",ctx->theCB->nMaster); throw RC_CORRUPTED;
				} else {
					report(MSG_INFO,"Master record found in "STOREPREFIX DATAFILESUFFIX"\n"); dataFileN=1;
				}
			}
			if (rc==RC_NOTFOUND) for (unsigned i=1; i<MAXMASTERFILES; i++) {
				char buf[100]; sprintf(buf,STOREPREFIX"%u"MASTERFILESUFFIX,i+1);
				if ((rc=StoreCB::open(ctx,buf,params.password,params.mode))==RC_OK) {
					if (ctx->theCB->nMaster<i+1 || ctx->theCB->nMaster>MAXMASTERFILES) {
						report(MSG_ERROR,"StoreCB: invalid nMaster field %d\n",ctx->theCB->nMaster); throw RC_CORRUPTED;
					} else {
						report(MSG_INFO,"Master record found in %.512s\n",buf);
						// create and open STOREPREFIX"1 to i"MASTERFILESUFFIX
						// copy from "i+1"
						break;
					}
				}
			}
		}
		if (rc!=RC_OK||ctx->theCB->state==SST_INIT) {
			switch (rc) {
			case RC_OK: report(MSG_WARNING,"Previous store initialization not finished\n"); rc=RC_NOTFOUND; break;
			case RC_VERSION: report(MSG_WARNING,"Invalid vesrion of Affinity in directory %.512s\n",dir); break;
			case RC_CORRUPTED: report(MSG_WARNING,"Corrupted or encrypted Affinity found in directory %.512s\n",dir); break;
			case RC_NOTFOUND: report(MSG_WARNING,"Affinity not found in directory %.512s\n",dir); break;
			default: report(MSG_WARNING,"Cannot open Affinity in directory %.512s(%d)\n",dir,rc); break;
			}
			throw rc;
		}

		assert(ctx->theCB!=NULL);

		if (ctx->fileMgr!=NULL) ctx->fileMgr->setPageSize(ctx->theCB->lPage);

		ctx->bufMgr=new(ctx) BufMgr(ctx,calcBuffers(params.nBuffers,ctx->theCB->lPage),ctx->theCB->lPage);
		if ((rc=ctx->bufMgr->init())!=RC_OK) {report(MSG_CRIT,"Cannot initialize buffer manager (%d)\n",rc); throw rc;}

		assert(ctx->theCB->lPage!=0);

		ctx->txMgr=new(ctx) TxMgr(ctx,ctx->theCB->lastTXID,params.notification);
	
		ctx->logMgr=new(ctx) LogMgr(ctx,params.logBufSize,(params.mode&STARTUP_ARCHIVE_LOGS)!=0,params.logDirectory);

		if (ctx->fileMgr==NULL) {
			//?????
		} else for (unsigned i=dataFileN; i<ctx->theCB->nDataFiles; i++) {
			lastFile=FileID(RESERVEDFILEIDS + i - dataFileN); char buf[100];
			if (i==0) strcpy(buf,STOREPREFIX DATAFILESUFFIX);
			else sprintf(buf,STOREPREFIX"%u"DATAFILESUFFIX,i);
			if ((rc=ctx->fileMgr->open(lastFile,buf))!=RC_OK)
				{report(MSG_CRIT,"Cannot open data file '%s' in directory '%.400s' (%d)\n",buf,dir,rc); throw rc;}
		}

		ctx->fsMgr=new(ctx) FSMgr(ctx); rc=ctx->fsMgr->init(lastFile);
		if (rc!=RC_OK) {report(MSG_CRIT,"Cannot initialize free space manager (%d)\n",rc); if (!fForce) throw rc;}

		ctx->treeMgr=new(ctx) TreeMgr(ctx,params.shutdownAsyncTimeout);
		ctx->heapMgr=new(ctx) PINPageMgr(ctx);
		ctx->hdirMgr=new(ctx) HeapDirMgr(ctx);
		ctx->ssvMgr=new(ctx) SSVPageMgr(ctx);
		ctx->trpgMgr=new(ctx) TreePageMgr(ctx);
		ctx->lockMgr=new(ctx) LockMgr(ctx);
		ctx->netMgr=new(ctx) NetMgr(ctx);
		ctx->identMgr=new(ctx) IdentityMgr(ctx);
		ctx->uriMgr=new(ctx) URIMgr(ctx);
		ctx->ftMgr=new(ctx) FTIndexMgr(ctx);
		ctx->queryMgr=new(ctx) QueryPrc(ctx,params.notification);
		ctx->namedMgr=new(ctx) NamedMgr(ctx);
		ctx->classMgr=new(ctx) Classifier(ctx,params.shutdownAsyncTimeout);
		ctx->bigMgr=new(ctx) BigMgr(ctx);
		ctx->fsmMgr=new(ctx) FSMMgr(ctx);

		if ((rc=RequestQueue::addStore(*ctx))!=RC_OK) {
			//...
			throw rc;
		}

		if ((rc=ctx->initBuiltinServices())!=RC_OK) throw rc;

		bool fRecv=ctx->theCB->state!=SST_SHUTDOWN_COMPLETE && ctx->theCB->state!=SST_READ_ONLY;
		if (fRecv || (params.mode&STARTUP_ROLLFORWARD)!=0) {
			report(MSG_NOTICE,fRecv ? "Affinity hasn't been properly shut down\n    automatic recovery in progress...\n" :
																					"Rollforward in progress...\n");
			Session *ses=Session::createSession(ctx); if (ses!=NULL) ses->setIdentity(STORE_OWNER,true);
			if ((rc=ctx->logMgr->recover(ses,(params.mode&STARTUP_ROLLFORWARD)!=0))==RC_OK && (rc=ctx->namedMgr->restoreXPropID(ses))==RC_OK)
				report(MSG_NOTICE,fRecv?"Recovery finished\n":"Rollforward finished\n");
			else {
				report(MSG_CRIT,fRecv?"Recovery failed (%d)\n":"Rollforward failed (%d)\n",rc);
				if (!fForce) {ctx->bufMgr->close(INVALID_FILEID,true); throw rc;}
			}
			Session::terminateSession();
		} else if ((params.mode&STARTUP_RT)!=0 && (rc=ctx->logMgr->init())!=RC_OK) {report(MSG_CRIT,"Cannot initialize logging(%d)\n",rc); throw rc;}
		else {ctx->theCB->preload(ctx); ctx->heapMgr->initPartial(); ctx->ssvMgr->initPartial();}

		if ((rc=ctx->namedMgr->initStorePrefix())!=RC_OK) {report(MSG_CRIT,"Cannot initialize store prefix(%d)\n",rc); throw rc;}

		HMAC hmac((byte*)&ctx,sizeof(StoreCtx*)); hmac.add((byte*)ctx,ctx->hmac-(byte*)ctx); memcpy(ctx->hmac,hmac.result(),HMAC_SIZE);

		if ((params.mode&STARTUP_RT)==0 && (rc=ctx->tqMgr->startThread())!=RC_OK) throw rc;
		
		if ((params.mode&STARTUP_NO_LOAD)==0) {
			Session *ses=Session::createSession(ctx); if (ses!=NULL) ses->setIdentity(STORE_OWNER,true);
			if ((rc=ctx->namedMgr->loadObjects(ses,(params.mode&STARTUP_SAFE)!=0))!=RC_OK)
				{report(MSG_CRIT,"Cannot load persisted objects(%d)\n",rc); throw rc;}
			Session::terminateSession();
		}

		if ((params.mode&STARTUP_TOUCH_FILE)!=0 || ctx->logMgr->isInit())
			{ctx->theCB->state=ctx->logMgr->isInit()?SST_LOGGING:SST_READ_ONLY; rc=ctx->theCB->update(ctx);}

		if (rc==RC_OK || fForce) {
			ctx->setState(SSTATE_OPEN); aff=ctx; storeTable->insert(ctx); ++StoreCtx::nStores;	// check unique?
			report(MSG_NOTICE,"Affinity running\n"); rc=RC_OK;
		}
		return rc;
	} catch (RC rc2) {
		rc=rc2; report(MSG_CRIT,"%s during store initialization\n",getErrMsg(rc));
	} catch (...) {
		report(MSG_CRIT, "Exception in openStore\n"); rc=RC_INTERNAL;
	}
	if (ctx!=NULL) {
		if (ctx->theCB!=NULL) ctx->theCB->close(ctx);
		delete ctx;
	}
	return rc;
}

RC createStore(const StoreCreationParameters& create,const StartupParameters& params,IAffinity *&aff,ISession **pLoad)
{
	StoreCtx *ctx=NULL; aff=NULL; RC rc=RC_OK;
#if defined(_M_X64) || defined(_M_AMD64) || defined(IA64) || defined(__x86_64__)
	if ((ProcFlags::pf.flg&PRCF_CMPXCHG16B)==0) return RC_INVOP;
#endif
	try {
		if (create.password!=params.password &&
			(create.password==NULL || params.password==NULL || strcmp(create.password,params.password)!=0)) return RC_INVPARAM;

		checkStoreTable(); initReport();
		if ((params.mode&STARTUP_RT)==0) RequestQueue::startThreads();

		char dirbuf[1024]; const char *dir=setDirectory(params.directory,dirbuf,sizeof(dirbuf));
		if (dir==NULL) {report(MSG_CRIT,"Directory doesn't exist or is protected\n"); return RC_NOTFOUND;}

		if ((ctx=storeTable->find(StrKey(dir)))!=NULL) {report(MSG_NOTICE,"Found open store in %s\n",dir); aff=ctx; return RC_OK;}

		if ((ctx=StoreCtx::createCtx(params.mode,dir,params.memory,params.lMemory,true))==NULL) return RC_NORESOURCES;

		dir=ctx->getDirectory();

		ctx->tqMgr=new(ctx) TimerQ(ctx);

		ctx->defaultService=params.service;

		ctx->bufMgr=new(ctx) BufMgr(ctx,calcBuffers(params.nBuffers,create.pageSize),create.pageSize);

		if (ctx->memory==NULL) {
			if ((ctx->fileMgr=new(ctx) FileMgr(ctx,params.maxFiles,getSrvDir(params.serviceDirectory,dirbuf,sizeof(dirbuf))))==NULL) throw RC_NORESOURCES;
			ctx->fileMgr->setPageSize(create.pageSize);
		}

		if ((ctx->cryptoMgr=CryptoMgr::get())==NULL) {report(MSG_CRIT,"Cannot initialize crypto\n"); throw RC_NORESOURCES;}

		SInCtx::initKW();

		unsigned nCtl=create.nControlRecords>MAXMASTERFILES ? MAXMASTERFILES : create.nControlRecords;

		const char *fname=nCtl <= 0 ? STOREPREFIX DATAFILESUFFIX : 
							nCtl==1 ? STOREPREFIX MASTERFILESUFFIX : 
									STOREPREFIX"1"MASTERFILESUFFIX;
		rc=StoreCB::create(ctx,fname,create);
	
		if (rc==RC_ALREADYEXISTS && (params.mode&STARTUP_FORCE_NEW)!=0) {
			// rename old, repeat StoreCB::create()
		}

		if (rc!=RC_OK) {report(MSG_CRIT,"Cannot create '%s' in directory '%.400s' (%d)\n",fname,dir,rc); throw rc;}

		if ((rc=ctx->bufMgr->init())!=RC_OK) {report(MSG_CRIT,"Cannot initialize buffer manager (%d)\n",rc); throw rc;}

		assert(ctx->theCB!=NULL && ctx->theCB->state==SST_INIT);

		FileID fid=0;

		if (nCtl>0 && ctx->fileMgr!=NULL) {
			for (unsigned i=1; i<nCtl; i++) {
				char buf[100]; sprintf(buf,STOREPREFIX"%d"MASTERFILESUFFIX,i+1); FileID fid=i;
				if ((rc=ctx->fileMgr->open(fid,buf,FIO_CREATE|FIO_NEW))!=RC_OK) {
					if (rc==RC_ALREADYEXISTS && (params.mode&STARTUP_FORCE_NEW)!=0) {
						// rename old, repeat open()
					}
					if (rc!=RC_OK) {report(MSG_CRIT,"Cannot create '%s' in directory '%.400s' (%d)\n",buf,dir,rc); throw rc;}
				}
			}
			fid=RESERVEDFILEIDS;
			if ((rc=ctx->fileMgr->open(fid,STOREPREFIX DATAFILESUFFIX,FIO_CREATE|FIO_NEW))!=RC_OK)
				{report(MSG_CRIT,"Cannot create '"STOREPREFIX DATAFILESUFFIX"' in directory '%.400s' (%d)\n",dir,rc); throw rc;}
		}

		ctx->txMgr=new(ctx) TxMgr(ctx,0,params.notification);

		ctx->logMgr=new(ctx) LogMgr(ctx,params.logBufSize,(params.mode&STARTUP_ARCHIVE_LOGS)!=0,params.logDirectory);
		if ((rc=ctx->logMgr->init())!=RC_OK) {report(MSG_CRIT,"Cannot allocate log file(s) (%d)\n",rc); throw rc;}

		assert(ctx->theCB->state==SST_INIT);

		ctx->fsMgr=new(ctx) FSMgr(ctx); rc=ctx->fsMgr->create(fid);
		if (rc!=RC_OK) {report(MSG_CRIT,"Cannot create free space manager (%d)\n",rc); throw rc;}

		ctx->treeMgr=new(ctx) TreeMgr(ctx,params.shutdownAsyncTimeout);
		ctx->heapMgr=new(ctx) PINPageMgr(ctx);
		ctx->hdirMgr=new(ctx) HeapDirMgr(ctx);
		ctx->ssvMgr=new(ctx) SSVPageMgr(ctx);
		ctx->trpgMgr=new(ctx) TreePageMgr(ctx);
		ctx->lockMgr=new(ctx) LockMgr(ctx);
		ctx->netMgr=new(ctx) NetMgr(ctx);
		ctx->identMgr=new(ctx) IdentityMgr(ctx);
		ctx->uriMgr=new(ctx) URIMgr(ctx);
		ctx->ftMgr=new(ctx) FTIndexMgr(ctx);
		ctx->queryMgr=new(ctx) QueryPrc(ctx,params.notification);
		ctx->namedMgr=new(ctx) NamedMgr(ctx);
		ctx->classMgr=new(ctx) Classifier(ctx,params.shutdownAsyncTimeout);
		ctx->bigMgr=new(ctx) BigMgr(ctx);
		ctx->fsmMgr=new(ctx) FSMMgr(ctx);
		
		if ((rc=RequestQueue::addStore(*ctx))!=RC_OK) {
			//...
			throw rc;
		}

		Session *ses=Session::createSession(ctx); if (ses!=NULL) ses->setIdentity(STORE_OWNER,true);

		if ((rc=ctx->namedMgr->createBuiltinObjects(ses))!=RC_OK) {report(MSG_CRIT,"Cannot create builtin classes and properties (%d)\n",rc); throw rc;}
		
		PWD_ENCRYPT pwd((const byte*)create.password,create.password!=NULL?strlen(create.password):0);
		const byte *enc=create.password!=NULL&&*create.password!='\0'?pwd.encrypted():NULL;
		const char *identity=create.identity;
		if (identity==NULL || identity[0]=='\0') {
#ifdef WIN32
			DWORD lbuf=sizeof(dirbuf);
			if (!::GetComputerName(dirbuf,&lbuf)) rc=convCode(GetLastError());
#else
			if (gethostname(dirbuf,sizeof(dirbuf))) rc=convCode(errno);
#endif
			if (rc!=RC_OK) {report(MSG_CRIT,"Cannot get host name (%d)\n",rc); throw rc;}
			identity=dirbuf;
		}
		Identity *ident=ctx->identMgr->insert(identity,strlen(identity),enc,NULL,0,true);
		if (ident==NULL) {report(MSG_CRIT,"Cannot initialize identity map\n"); throw RC_OTHER;}
		if ((rc=ctx->namedMgr->initStorePrefix(ident))!=RC_OK) {report(MSG_CRIT,"Cannot initialize store prefix(%d)\n",rc); throw rc;}
		assert(ident->getID()==STORE_OWNER);
		ident->release();

		Session::terminateSession();

		if ((rc=ctx->initBuiltinServices())!=RC_OK) throw rc;

		if ((params.mode&STARTUP_RT)==0 && (rc=ctx->tqMgr->startThread())!=RC_OK) throw rc;

		ctx->theCB->state=SST_LOGGING;
		if ((rc=ctx->theCB->update(ctx))==RC_OK || (params.mode&STARTUP_FORCE_OPEN)!=0) {
			rc=RC_OK;
			if (pLoad!=NULL) {
				Session *ses=Session::createSession(ctx);
				if ((*pLoad=ses)==NULL) {*pLoad=NULL; rc=RC_NORESOURCES;}
				else if ((rc=ctx->txMgr->startTx(ses,TXT_READWRITE,TXI_DEFAULT))==RC_OK) {ses->setRestore(); ctx->theCB->state=SST_RESTORE;}
			}
			if (rc==RC_OK) {ctx->setState(SSTATE_OPEN); aff=ctx; ++StoreCtx::nStores; storeTable->insert(ctx); report(MSG_NOTICE,"Affinity running\n");}	// unique???
		}
		return rc;
	} catch (RC rc2) {
		rc=rc2; report(MSG_CRIT,"%s during store creation\n",getErrMsg(rc));
	} catch (...) {
		report(MSG_CRIT, "Exception in createStore\n"); rc=RC_INTERNAL;
	}
	if (ctx!=NULL) {
		if (ctx->theCB!=NULL) ctx->theCB->close(ctx);
		delete ctx;
	}
	return rc;
}

RC getStoreCreationParameters(StoreCreationParameters& params,IAffinity *aff)
{
	try {
		StoreCtx *ctx=(StoreCtx*)aff;
		if (ctx!=NULL) ctx->set(); else if ((ctx=StoreCtx::get())==NULL) return RC_NOTFOUND;
		params.nControlRecords=ctx->theCB->nMaster;
		params.pageSize=ctx->theCB->lPage;
		params.fileExtentSize=ctx->theCB->nPagesPerExtent;
		params.identity=NULL;		// ???
		params.storeId=ctx->theCB->storeID;
		params.password=NULL;
		params.mode=(ctx->theCB->flags&STFLG_ENCRYPTED)!=0?STORE_CREATE_ENCRYPTED:0;
		if ((ctx->theCB->flags&STFLG_PAGEHMAC)!=0) params.mode|=STORE_CREATE_PAGE_INTEGRITY;
		params.maxSize=ctx->theCB->maxSize;
		params.pctFree=ctx->theCB->pctFree;
		params.logSegSize=ctx->theCB->logSegSize;
		params.xSyncStack=ctx->theCB->xSyncStack;
		params.xOnCommit=ctx->theCB->xOnCommit;
		params.xObjSession=ctx->theCB->xSesObjects;
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in getStoreCreationParameters\n"); return RC_INTERNAL;}
}

unsigned getVersion()
{
	return STORE_IFACE_VER;
}

//-------------------------------------------------------------------------------------------------------------------------------------------------

static void reportTree(PageID pid,const char *t,StoreCtx *ctx)
{
	if (pid!=INVALID_PAGEID) {
		CheckTreeReport rep,sec;
		unsigned cnt=Tree::checkTree(ctx,pid,rep,&sec),cnt2=ctx->trpgMgr->enumPages(pid);
		if (cnt!=cnt2) report(MSG_ERROR,"!!!!%s: cnt=%lu, enum=%lu\n",t,cnt,cnt2);
		if (rep.depth>0) {
			report(MSG_INFO," %s index report (%lu pages):\n",t,cnt);
			for (unsigned i=0; i<rep.depth; i++)
				report(MSG_INFO,"\t\t%lu: %lu total, %lu missing, %lu empty\n",i,rep.total[i],rep.missing[i],rep.empty[i]);
			for (unsigned i=cnt2=0; i<sec.depth; i++)
				{report(MSG_INFO,"\t\t\t%lu: %lu total, %lu missing, %lu empty\n",i,sec.total[i],sec.missing[i],sec.empty[i]); cnt2+=sec.total[i];}
			report(MSG_INFO," Primary leaf histogram:\n");
			for (unsigned i=0; i<TREE_N_HIST_BUCKETS; i++)
				report (MSG_INFO,"%d - %d%% full: %lu\n",int(i*(100./TREE_N_HIST_BUCKETS)),int((i+1)*(100./TREE_N_HIST_BUCKETS)),rep.histogram[i]);
			if (cnt2!=0) {
				report(MSG_INFO," Secondary leaf histogram:\n");
				for (unsigned i=0; i<TREE_N_HIST_BUCKETS; i++)
					report (MSG_INFO,"%d - %d%% full: %lu\n",int(i*(100./TREE_N_HIST_BUCKETS)),int((i+1)*(100./TREE_N_HIST_BUCKETS)),sec.histogram[i]);
			}
		}
	}
}

RC shutdownStore()
{
	StoreCtx *ctx=StoreCtx::get(); return ctx!=NULL?ctx->shutdown():RC_NOTFOUND;
}

RC StoreCtx::shutdown()
{
	try {
		for (long st=state; ;st=state) {
			if ((st&SSTATE_IN_SHUTDOWN)!=0) return RC_SHUTDOWN;
			if (cas(&state,st,st|SSTATE_IN_SHUTDOWN)) break;
		}

		report(MSG_NOTICE,"Affinity shutdown in progress\n");

		Session::terminateSession(); RC rc;

		// check return codes!!!

		if (theCB->state!=SST_SHUTDOWN_COMPLETE && theCB->state!=SST_READ_ONLY && theCB->state!=SST_NO_SHUTDOWN)
			{theCB->state=SST_SHUTDOWN_IN_PROGRESS; if ((rc=theCB->update(this))!=RC_OK) return rc;}

		if (txMgr->getNActive()>0) {
			report(MSG_NOTICE,"Rollback %d active transaction(s)\n",txMgr->getNActive());
			while (txMgr->getNActive()>0) threadYield();
		}

		shutdownServices();

		netMgr->close();		//make all close() -> RC
		fsMgr->close();
		RequestQueue::removeStore(*this,10000);						// ??? timeout
		if (bufMgr->flushAll(60000000)!=RC_OK) {				// timeout 1 minute (for slow ext. memory)
			theCB->state=SST_NO_SHUTDOWN;
		}

		if ((mode&STARTUP_PRINT_STATS)!=0) {
			Session *ses=Session::createSession(this); if (ses!=NULL) ses->setIdentity(STORE_OWNER,true);
			reportTree(theCB->mapRoots[MA_FTINDEX],"FT",this);
			reportTree(theCB->mapRoots[MA_CLASSINDEX],"Class",this);
			TreeScan *sc=namedMgr->scan(ses,NULL);
			if (sc!=NULL) {
				const void *er; size_t lD;
				while ((er=sc->nextValue(lD))!=NULL) {
					PINRef pr(storeID,(const byte*)er);
					if ((pr.def&PR_U1)!=0 && (pr.u1&PMT_CLASS)!=0 && (pr.def&PR_PID2)!=0) {
						const SearchKey& ky=sc->getKey(); assert(ky.type==KT_UINT);
						URI *uri=NULL; char buf[20]; const char *name=buf; size_t lname;
						if (ky.v.u>MAX_BUILTIN_URIID || (name=NamedMgr::getBuiltinName((URIID)ky.v.u,lname))==NULL)
							{if ((uri=(URI*)uriMgr->ObjMgr::find((uint32_t)ky.v.u))!=NULL) name=uri->getURI()->str; else sprintf(buf,"%u",(uint32_t)ky.v.u);}
						reportTree(PageID(pr.id2.pid>>16),name,this);
						if (uri!=NULL) uri->release();
					}
				}
				sc->destroy();
			}
			Session::terminateSession();
		}

		if (fileMgr!=NULL) fileMgr->closeAll(RESERVEDFILEIDS);
		if ((rc=bufMgr->close(0,true))!=RC_OK) return rc;
		if ((rc=logMgr->close())!=RC_OK) return rc;

		HeapPageMgr::savePartial(heapMgr,ssvMgr);
		theCB->xPropID=namedMgr->getXPropID();

		bool fDelLog=false;
		if (theCB->state!=SST_SHUTDOWN_COMPLETE && theCB->state!=SST_NO_SHUTDOWN)
			{theCB->state=SST_SHUTDOWN_COMPLETE; if ((rc=theCB->update(this))!=RC_OK) return rc; fDelLog=true;}

		theCB->close(this);

		if (fDelLog) logMgr->deleteLogs();

		assert(storeTable!=NULL); storeTable->remove(this); delete this;

		report(MSG_NOTICE,"Affinity shutdown complete\n");

		if (--StoreCtx::nStores==0) closeReport();
	
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in shutdownStore\n"); return RC_INTERNAL;}
}

void stopThreads()
{
	TimerQ::stopThreads();
	RequestQueue::stopThreads();
	stopSocketThreads();
}

StoreCtx::~StoreCtx()
{
	MemAlloc *m=mem; directory.reset();
	if (bufMgr!=NULL) bufMgr->~BufMgr();
	if (classMgr!=NULL) classMgr->~Classifier();
	if (namedMgr!=NULL) namedMgr->~NamedMgr();
	if (fileMgr!=NULL) fileMgr->~FileMgr();
	if (fsMgr!=NULL) fsMgr->~FSMgr();
	//if (ftMgr!=NULL) ftMgr->~FTIndexMgr();
	if (hdirMgr!=NULL) hdirMgr->~HeapDirMgr();
	if (lockMgr!=NULL) lockMgr->~LockMgr();
	if (logMgr!=NULL) logMgr->~LogMgr();
	if (uriMgr!=NULL) uriMgr->~URIMgr();
	if (identMgr!=NULL) identMgr->~IdentityMgr();
	if (netMgr!=NULL) netMgr->~NetMgr();
	if (heapMgr!=NULL) heapMgr->~PINPageMgr();
	if (ssvMgr!=NULL) ssvMgr->~SSVPageMgr();
	if (treeMgr!=NULL) treeMgr->~TreeMgr();
	if (txMgr!=NULL) txMgr->~TxMgr();
	if (bigMgr!=NULL) bigMgr->~BigMgr();
	if (fsmMgr!=NULL) fsmMgr->~FSMMgr();
	if (tqMgr!=NULL) tqMgr->~TimerQ();
	storeTls.set(NULL);
	if (m!=NULL) m->truncate(TR_REL_ALL);
}

StoreCtx *StoreCtx::createCtx(unsigned f,const char *dir,void *mem,uint64_t lMem,bool fNew)
{
	StoreCtx *ctx=new(&sharedAlloc) StoreCtx(f,mem,lMem);
	if (ctx!=NULL) {
		ctx->mem=createMemAlloc(fNew?STORE_NEW_MEM:STORE_START_MEM,true); storeTls.set(ctx);
		if ((mem==NULL || lMem==0) && dir!=NULL && *dir!='\0') ctx->directory=strdup(dir,ctx);
	}
	return ctx;
}

void StoreCtx::operator delete(void *p)
{
	sharedAlloc.free(p);
}
