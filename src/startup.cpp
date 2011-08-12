/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

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
#include "ifacever.h"
#include <stdio.h>
#ifndef WIN32
#include <sys/stat.h>
#include <sys/sysinfo.h>
#endif

using namespace MVStoreKernel;

RC manageStores(const char *cmd,size_t lcmd,MVStoreCtx &store,IMapDir *id,const StartupParameters *sp,CompilationError *ce)
{
	try {
		store=NULL; if (cmd==NULL || lcmd==0) return RC_INVPARAM;
		RequestQueue::startThreads(); initReport(); SInCtx::initKW();
		SInCtx in(NULL,cmd,lcmd,NULL,0,SQ_SQL,NULL);		//??? ma!!!
		try {in.parseManage(id,store,sp); return RC_OK;}
		catch (SynErr sy) {in.getErrorInfo(RC_SYNTAX,sy,ce); return RC_SYNTAX;}
		catch (RC rc) {in.getErrorInfo(rc,SY_ALL,ce); return rc;}
	} catch (RC rc) {return rc;} catch (...) {return RC_INTERNAL;}
}

static void setDirectory(FileMgr *fio, const char *dir,StoreCtx *ctx)
{
	if (dir!=NULL && *dir!='\0') {fio->setDirectory(dir); return;}

	const char *homeDir=getenv("HOME");
	if (homeDir!=NULL && *homeDir!='\0') {
		char *dir=(char*)ctx->malloc(strlen(homeDir)+1+sizeof(MVSTOREDIR)+1);
		if (dir!=NULL) {
			strcpy(dir,homeDir); strcat(dir,"/"MVSTOREDIR);
#ifdef WIN32
			HANDLE hDir=CreateFile(dir,GENERIC_READ,FILE_SHARE_READ,NULL,OPEN_EXISTING,FILE_FLAG_BACKUP_SEMANTICS,NULL);
			if (hDir!=INVALID_HANDLE_VALUE) {CloseHandle(hDir); fio->setDirectory(dir);}
#else
			struct stat64 st;
			if (stat64(dir,&st)==0 && S_ISDIR(st.st_mode)) fio->setDirectory(dir);
#endif
			ctx->free(dir);
		}
	}
}

static unsigned calcBuffers(unsigned nBuf,unsigned lPage)
{
	if (nBuf==0 || nBuf==~0u) {
#ifdef WIN32
		MEMORYSTATUSEX memStat={sizeof(MEMORYSTATUSEX)};
		if (GlobalMemoryStatusEx(&memStat)) nBuf=unsigned(min(memStat.ullAvailPhys,memStat.ullAvailVirtual)*7/8/lPage);
#else
		struct sysinfo sysInfo;
		if (sysinfo(&sysInfo)==0) nBuf=unsigned(uint64_t(sysInfo.freeram)*7/8/lPage);
#endif
	}
	return nBuf==0||nBuf==~0u?0x1000:nBuf<MIN_BUFFERS?MIN_BUFFERS:nBuf;
}

static RC testEnv()
{
	// check processor supports CMPXCHG16B in 64-bit mode
#ifdef _WIN64
	int CPUInfo[4]; __cpuid(CPUInfo,1); if ((CPUInfo[2]&0x2000)==0) return RC_INVOP;
#elif defined(__i86_64__)
	int CPUInfo[4];
	__asm__ __volatile__ ("cpuid":"=a" (CPUInfo[0]), "=b" (CPUInfo[0]), "=c" (CPUInfo[0]), "=d" (CPUInfo[0]) : "1"((unsigned int)idx));
	if ((CPUInfo[2]&0x2000)==0) return RC_INVOP;
#endif
	return RC_OK;
}

RC openStore(const StartupParameters& params,MVStoreCtx &cctx)
{
	StoreCtx *ctx=NULL; RC rc=testEnv(); if (rc!=RC_OK) return rc;
	try {
		RequestQueue::startThreads(); initReport(); cctx=NULL;
		report(MSG_NOTICE,"MVStore startup - version %d.%02d\n",STORE_VERSION/100,STORE_VERSION%100);

		if ((ctx=StoreCtx::createCtx(params.mode))==NULL) return RC_NORESOURCES;

		FileMgr *fio=ctx->fileMgr=new(ctx) FileMgr(ctx,params.maxFiles,params.io);
		setDirectory(fio,params.directory,ctx);

		if ((ctx->cryptoMgr=CryptoMgr::get())==NULL) {report(MSG_CRIT,"Cannot initialize crypto\n"); throw RC_NORESOURCES;}

		SInCtx::initKW();

		int dataFileN=0; FileID lastFile=0; bool fForce=(params.mode&STARTUP_FORCE_OPEN)!=0;

		if ((rc=StoreCB::open(ctx,MVSTOREPREFIX MASTERFILESUFFIX,params.password,fForce))==RC_OK) {
			if (ctx->theCB->nMaster!=1) {
				report(MSG_ERROR,"StoreCB: invalid nMaster field %d\n",ctx->theCB->nMaster); throw RC_CORRUPTED;
			} else {
				report(MSG_INFO,"Master record found in "MVSTOREPREFIX MASTERFILESUFFIX"\n");
			}
		} 
		if (rc==RC_NOTFOUND && (rc=StoreCB::open(ctx,MVSTOREPREFIX"1"MASTERFILESUFFIX,params.password,fForce))==RC_OK) {
			if (ctx->theCB->nMaster<2 || ctx->theCB->nMaster>MAXMASTERFILES) {
				report(MSG_ERROR,"StoreCB: invalid nMaster field %d\n",ctx->theCB->nMaster); throw RC_CORRUPTED;
			} else {
				report(MSG_INFO,"Master record found in "MVSTOREPREFIX"1"MASTERFILESUFFIX"\n");
				// open MVSTOREPREFIX"2 to nMaster+1"MASTERFILESUFFIX
				// compare timestamp -> use latest
			}
		}
		if (rc==RC_NOTFOUND && (rc=StoreCB::open(ctx,MVSTOREPREFIX DATAFILESUFFIX,params.password,fForce))==RC_OK) {
			if (ctx->theCB->nMaster!=0) {
				report(MSG_ERROR,"StoreCB: invalid nMaster field %d\n",ctx->theCB->nMaster); throw RC_CORRUPTED;
			} else {
				report(MSG_INFO,"Master record found in "MVSTOREPREFIX DATAFILESUFFIX"\n"); dataFileN=1;
			}
		}
		if (rc==RC_NOTFOUND) for (ulong i=1; i<MAXMASTERFILES; i++) {
			char buf[100]; sprintf(buf,MVSTOREPREFIX"%lu"MASTERFILESUFFIX,i+1);
			if ((rc=StoreCB::open(ctx,buf,params.password,fForce))==RC_OK) {
				if (ctx->theCB->nMaster<i+1 || ctx->theCB->nMaster>MAXMASTERFILES) {
					report(MSG_ERROR,"StoreCB: invalid nMaster field %d\n",ctx->theCB->nMaster); throw RC_CORRUPTED;
				} else {
					report(MSG_INFO,"Master record found in %.512s\n",buf);
					// create and open MVSTOREPREFIX"1 to i"MASTERFILESUFFIX
					// copy from "i+1"
					break;
				}
			}
		}
		if (rc!=RC_OK||ctx->theCB->state==SST_INIT) {
			switch (rc) {
			case RC_OK: report(MSG_WARNING,"Previous store initialization not finished\n"); rc=RC_NOTFOUND; break;
			case RC_VERSION: report(MSG_WARNING,"Invalid vesrion of MVStore in directory %.512s\n",fio->getDirectory()); break;
			case RC_CORRUPTED: report(MSG_WARNING,"Corrupted or encrypted MVStore found in directory %.512s\n",fio->getDirectory()); break;
			case RC_NOTFOUND: report(MSG_WARNING,"MVStore not found in directory %.512s\n",fio->getDirectory()); break;
			default: report(MSG_WARNING,"Cannot open MVStore in directory %.512s(%d)\n",fio->getDirectory(),rc); break;
			}
			throw rc;
		}

		assert(ctx->theCB!=NULL);

		ctx->fileMgr->setPageSize(ctx->theCB->lPage);

		ctx->bufMgr=new(ctx) BufMgr(ctx,calcBuffers(params.nBuffers,ctx->theCB->lPage),ctx->theCB->lPage);
		if ((rc=ctx->bufMgr->init())!=RC_OK) {report(MSG_CRIT,"Cannot initialize buffer manager (%d)\n",rc); throw rc;}

		assert(ctx->theCB->lPage!=0);

		ctx->txMgr=new(ctx) TxMgr(ctx,ctx->theCB->lastTXID,params.notification);
	
		ctx->logMgr=new(ctx) LogMgr(ctx,params.logBufSize,(params.mode&STARTUP_ARCHIVE_LOGS)!=0,params.logDirectory);

		for (ulong i=dataFileN; i<ctx->theCB->nDataFiles; i++) {
			lastFile=FileID(RESERVEDFILEIDS + i - dataFileN); char buf[100];
			if (i==0) strcpy(buf,MVSTOREPREFIX DATAFILESUFFIX);
			else sprintf(buf,MVSTOREPREFIX"%lu"DATAFILESUFFIX,i);
			if ((rc=fio->open(lastFile,buf))!=RC_OK)
				{report(MSG_CRIT,"Cannot open data file '%s' in directory '%.400s' (%d)\n",buf,fio->getDirectory(),rc); throw rc;}
		}

		ctx->fsMgr=new(ctx) FSMgr(ctx); rc=ctx->fsMgr->init(lastFile);
		if (rc!=RC_OK) {report(MSG_CRIT,"Cannot initialize free space manager (%d)\n",rc); if (!fForce) throw rc;}

		ctx->treeMgr=new(ctx) TreeMgr(ctx,params.shutdownAsyncTimeout);
		ctx->heapMgr=new(ctx) PINPageMgr(ctx);
		ctx->hdirMgr=new(ctx) HeapDirMgr(ctx);
		ctx->ssvMgr=new(ctx) SSVPageMgr(ctx);
		ctx->trpgMgr=new(ctx) TreePageMgr(ctx);
		ctx->lockMgr=new(ctx) LockMgr(ctx,params.lockNotification);
		ctx->netMgr=new(ctx) NetMgr(ctx,params.network);
		ctx->identMgr=new(ctx) IdentityMgr(ctx);
		ctx->uriMgr=new(ctx) URIMgr(ctx);
		ctx->ftMgr=new(ctx) FTIndexMgr(ctx);
		ctx->queryMgr=new(ctx) QueryPrc(ctx,params.notification);
		ctx->classMgr=new(ctx) Classifier(ctx,params.shutdownAsyncTimeout);
		ctx->bigMgr=new(ctx) BigMgr(ctx);

		if ((rc=RequestQueue::addStore(*ctx))!=RC_OK) {
			//...
			throw rc;
		}

		bool fRecv=ctx->theCB->state!=SST_SHUTDOWN_COMPLETE && ctx->theCB->state!=SST_READ_ONLY;
		if (fRecv || (params.mode&STARTUP_ROLLFORWARD)!=0) {
			report(MSG_NOTICE,fRecv ? "MVStore hasn't been properly shut down\n    automatic recovery in progress...\n" :
																					"Rollforward in progress...\n");
			Session *ses=Session::createSession(ctx); if (ses!=NULL) ses->setIdentity(STORE_OWNER,true);
			if ((rc=ctx->logMgr->recover(ses,(params.mode&STARTUP_ROLLFORWARD)!=0))==RC_OK && (rc=ctx->classMgr->restoreXPropID(ses))==RC_OK) 
				report(MSG_NOTICE,fRecv?"Recovery finished\n":"Rollforward finished\n");
			else {
				report(MSG_CRIT,fRecv?"Recovery failed (%d)\n":"Rollforward failed (%d)\n",rc);
				if (!fForce) {ctx->bufMgr->close(INVALID_FILEID,true); throw rc;}
			}
			Session::terminateSession();
		} else {
			ctx->theCB->preload(ctx); ctx->heapMgr->initPartial(); ctx->ssvMgr->initPartial();
		}

		if ((params.mode&STARTUP_TOUCH_FILE)!=0 || ctx->logMgr->isInit())
			{ctx->theCB->state=ctx->logMgr->isInit()?SST_LOGGING:SST_READ_ONLY; rc=ctx->theCB->update(ctx);}

		if (rc==RC_OK || fForce) report(MSG_NOTICE,"MVStore running\n");
		if (rc==RC_OK) {ctx->setState(SSTATE_OPEN); cctx=ctx;}
		return rc;
	} catch (RC rc2) {
		if ((rc=rc2)==RC_NORESOURCES) report(MSG_CRIT,"Out of memory during store initialization\n");
	} catch (...) {
		report(MSG_CRIT, "Exception in openStore\n"); rc=RC_INTERNAL;
	}
	if (ctx!=NULL) {
		if (ctx->theCB!=NULL) ctx->theCB->close(ctx);
		delete ctx;
	}
	return rc;
}

RC createStore(const StoreCreationParameters& create,const StartupParameters& params,MVStoreCtx &cctx,ISession **pLoad)
{
	StoreCtx *ctx=NULL; RC rc=testEnv(); if (rc!=RC_OK) return rc;
	try {
		RequestQueue::startThreads(); initReport(); cctx=NULL;

		if ((ctx=StoreCtx::createCtx(params.mode,true))==NULL) return RC_NORESOURCES;

		ctx->bufMgr=new(ctx) BufMgr(ctx,calcBuffers(params.nBuffers,create.pageSize),create.pageSize);

		FileMgr *fio=ctx->fileMgr=new(ctx) FileMgr(ctx,params.maxFiles,params.io);
		setDirectory(fio,params.directory,ctx); fio->setPageSize(create.pageSize);

		if ((ctx->cryptoMgr=CryptoMgr::get())==NULL) {report(MSG_CRIT,"Cannot initialize crypto\n"); throw RC_NORESOURCES;}

		SInCtx::initKW();

		unsigned nCtl=create.nControlRecords>MAXMASTERFILES ? MAXMASTERFILES : create.nControlRecords;

		const char *fname=nCtl <= 0 ? MVSTOREPREFIX DATAFILESUFFIX : 
							nCtl==1 ? MVSTOREPREFIX MASTERFILESUFFIX : 
									MVSTOREPREFIX"1"MASTERFILESUFFIX;
		rc=StoreCB::create(ctx,fname,create);
	
		if (rc==RC_ALREADYEXISTS && (params.mode&STARTUP_FORCE_NEW)!=0) {
			// rename old, repeat StoreCB::create()
		}

		if (rc!=RC_OK) {report(MSG_CRIT,"Cannot create '%s' in directory '%.400s' (%d)\n",fname,fio->getDirectory(),rc); throw rc;}

		if ((rc=ctx->bufMgr->init())!=RC_OK) {report(MSG_CRIT,"Cannot initialize buffer manager (%d)\n",rc); throw rc;}

		assert(ctx->theCB!=NULL && ctx->theCB->state==SST_INIT);

		FileID fid=0;

		if (nCtl>0) {
			for (unsigned i=1; i<nCtl; i++) {
				char buf[100]; sprintf(buf,MVSTOREPREFIX"%d"MASTERFILESUFFIX,i+1); FileID fid=i;
				if ((rc=ctx->fileMgr->open(fid,buf,FIO_CREATE|FIO_NEW))!=RC_OK) {
					if (rc==RC_ALREADYEXISTS && (params.mode&STARTUP_FORCE_NEW)!=0) {
						// rename old, repeat open()
					}
					if (rc!=RC_OK) {report(MSG_CRIT,"Cannot create '%s' in directory '%.400s' (%d)\n",buf,ctx->fileMgr->getDirectory(),rc); throw rc;}
				}
			}
			fid=RESERVEDFILEIDS;
			if ((rc=ctx->fileMgr->open(fid,MVSTOREPREFIX DATAFILESUFFIX,FIO_CREATE|FIO_NEW))!=RC_OK)
				{report(MSG_CRIT,"Cannot create '"MVSTOREPREFIX DATAFILESUFFIX"' in directory '%.400s' (%d)\n",ctx->fileMgr->getDirectory(),rc); throw rc;}
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
		ctx->lockMgr=new(ctx) LockMgr(ctx,params.lockNotification);
		ctx->netMgr=new(ctx) NetMgr(ctx,params.network);
		ctx->identMgr=new(ctx) IdentityMgr(ctx);
		ctx->uriMgr=new(ctx) URIMgr(ctx);
		ctx->ftMgr=new(ctx) FTIndexMgr(ctx);
		ctx->queryMgr=new(ctx) QueryPrc(ctx,params.notification);
		ctx->classMgr=new(ctx) Classifier(ctx,params.shutdownAsyncTimeout);
		ctx->bigMgr=new(ctx) BigMgr(ctx);

		if ((rc=RequestQueue::addStore(*ctx))!=RC_OK) {
			//...
			throw rc;
		}

		Session *ses=Session::createSession(ctx); if (ses!=NULL) ses->setIdentity(STORE_OWNER,true);

		if ((rc=ctx->classMgr->initStoreMaps(ses))!=RC_OK) {report(MSG_CRIT,"Cannot initialize maps (%d)\n",rc); throw rc;}
		
		PWD_ENCRYPT pwd((const byte*)create.password,create.password!=NULL?strlen(create.password):0);
		const byte *enc=create.password!=NULL&&*create.password!='\0'?pwd.encrypted():NULL;
		Identity *ident=ctx->identMgr->insert(create.identity==NULL?"":create.identity,enc,NULL,0,true);	//cert from params
		if (ident==NULL) {report(MSG_CRIT,"Cannot initialize identity map\n"); throw RC_OTHER;}
		assert(ident->getID()==STORE_OWNER);
		ident->release();

		Session::terminateSession();

		ctx->theCB->state=SST_LOGGING;
		if ((rc=ctx->theCB->update(ctx))==RC_OK || (params.mode&STARTUP_FORCE_OPEN)!=0) {
			report(MSG_NOTICE,"MVStore running\n"); rc=RC_OK;
			if (pLoad!=NULL) {
				Session *ses=Session::createSession(ctx);
				if (ses==NULL) {*pLoad=NULL; rc=RC_NORESOURCES;}
				else {
					SessionX *ps=SessionX::create(ses);
					if ((*pLoad=ps)==NULL) {Session::terminateSession(); rc=RC_NORESOURCES;}
					else if ((rc=ctx->txMgr->startTx(ses,TXT_READWRITE,TXI_DEFAULT))==RC_OK) {ses->setRestore(); ctx->theCB->state=SST_RESTORE;}
				}
			}
			if (rc==RC_OK) {ctx->setState(SSTATE_OPEN); cctx=ctx;}
		}
		return rc;
	} catch (RC rc2) {
		if ((rc=rc2)==RC_NORESOURCES) report(MSG_CRIT,"Out of memory during store initialization\n");
	} catch (...) {
		report(MSG_CRIT, "Exception in createStore\n"); rc=RC_INTERNAL;
	}
	if (ctx!=NULL) {
		if (ctx->theCB!=NULL) ctx->theCB->close(ctx);
		delete ctx;
	}
	return rc;
}

RC getStoreCreationParameters(StoreCreationParameters& params,MVStoreCtx ctx)
{
	try {
		if (ctx!=NULL) ctx->set(); else if ((ctx=StoreCtx::get())==NULL) return RC_NOTFOUND;
		params.nControlRecords=ctx->theCB->nMaster;
		params.pageSize=ctx->theCB->lPage;
		params.fileExtentSize=ctx->theCB->nPagesPerExtent;
		params.identity=NULL;		// ???
		params.storeId=ctx->theCB->storeID;
		params.password=NULL;
		params.fEncrypted=ctx->theCB->fIsEncrypted!=0;
		params.maxSize=ctx->theCB->maxSize;
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in getStoreCreationParameters\n"); return RC_INTERNAL;}
}

unsigned getVersion()
{
	return STORE_IFACE_VER;
}

static void reportTree(PageID pid,const char *t,StoreCtx *ctx)
{
	if (pid!=INVALID_PAGEID) {
		CheckTreeReport rep,sec;
		ulong cnt=Tree::checkTree(ctx,pid,rep,&sec),cnt2=ctx->trpgMgr->enumPages(pid);
		if (cnt!=cnt2) report(MSG_ERROR,"!!!!%s: cnt=%lu, enum=%lu\n",t,cnt,cnt2);
		if (rep.depth>0) {
			report(MSG_INFO," %s index report (%lu pages):\n",t,cnt);
			for (ulong i=0; i<rep.depth; i++)
				report(MSG_INFO,"\t\t%lu: %lu total, %lu missing, %lu empty\n",i,rep.total[i],rep.missing[i],rep.empty[i]);
			for (ulong i=cnt2=0; i<sec.depth; i++)
				{report(MSG_INFO,"\t\t\t%lu: %lu total, %lu missing, %lu empty\n",i,sec.total[i],sec.missing[i],sec.empty[i]); cnt2+=sec.total[i];}
			report(MSG_INFO," Primary leaf histogram:\n");
			for (ulong i=0; i<TREE_N_HIST_BUCKETS; i++)
				report (MSG_INFO,"%d - %d%% full: %lu\n",int(i*(100./TREE_N_HIST_BUCKETS)),int((i+1)*(100./TREE_N_HIST_BUCKETS)),rep.histogram[i]);
			if (cnt2!=0) {
				report(MSG_INFO," Secondary leaf histogram:\n");
				for (ulong i=0; i<TREE_N_HIST_BUCKETS; i++)
					report (MSG_INFO,"%d - %d%% full: %lu\n",int(i*(100./TREE_N_HIST_BUCKETS)),int((i+1)*(100./TREE_N_HIST_BUCKETS)),sec.histogram[i]);
			}
		}
	}
}

unsigned getStoreState(MVStoreCtx ctx)
{
	try {return ctx!=NULL || (ctx=StoreCtx::get())!=NULL ? ctx->getState() : 0u;}
	catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in getStoreState\n");}
	return 0u;
}

RC shutdownStore(MVStoreCtx ctx)
{
	try {
		if (ctx!=NULL) ctx->set(); else if ((ctx=StoreCtx::get())==NULL) return RC_NOTFOUND;
		for (long st=ctx->state; ;st=ctx->state) {
			if ((st&SSTATE_IN_SHUTDOWN)!=0) return RC_SHUTDOWN;
			if (cas(&ctx->state,st,st|SSTATE_IN_SHUTDOWN)) break;
		}

		report(MSG_NOTICE,"MVStore shutdown in progress\n");

		Session::terminateSession(); RC rc;

		// check return codes!!!

		if (ctx->theCB->state!=SST_SHUTDOWN_COMPLETE && ctx->theCB->state!=SST_READ_ONLY && ctx->theCB->state!=SST_NO_SHUTDOWN)
			{ctx->theCB->state=SST_SHUTDOWN_IN_PROGRESS; if ((rc=ctx->theCB->update(ctx))!=RC_OK) return rc;}

		if (ctx->txMgr->getNActive()>0) {
			report(MSG_NOTICE,"Rollback %d active transaction(s)\n",ctx->txMgr->getNActive());
			while (ctx->txMgr->getNActive()>0) threadYield();
		}

		ctx->netMgr->close();		//make all close() -> RC
		ctx->fsMgr->close();
		RequestQueue::removeStore(*ctx,10000);				// ??? timeout
		if ((rc=ctx->bufMgr->flushAll())!=RC_OK) return rc;

		if ((ctx->mode&STARTUP_PRINT_STATS)!=0) {
			Session *ses=Session::createSession(ctx); if (ses!=NULL) ses->setIdentity(STORE_OWNER,true);
			reportTree(ctx->theCB->mapRoots[MA_FTINDEX],"FT",ctx);
			reportTree(ctx->theCB->mapRoots[MA_CLASSINDEX],"Class",ctx);
			TreeScan *sc=ctx->classMgr->getClassPINs().scan(ses,NULL);
			if (sc!=NULL) {
				const void *er; size_t lD;
				while ((er=sc->nextValue(lD))!=NULL) {
					PINRef pr(ctx->storeID,(const byte*)er,byte(lD));
					if ((pr.def&PR_PID2)!=0) {
						const SearchKey& ky=sc->getKey(); assert(ky.type==KT_UINT);
						URI *uri=NULL; char buf[20]; const char *name=buf;
						if (ky.v.u32<=PROP_SPEC_LAST) name=Classifier::builtinURIs[ky.v.u32].name;
						else if ((uri=(URI*)ctx->uriMgr->ObjMgr::find(ky.v.u32))!=NULL) name=uri->getURI();
						else sprintf(buf,"%u",ky.v.u32);
						reportTree(PageID(pr.id2.pid>>16),name,ctx);
						if (uri!=NULL) uri->release();
					}
				}
				sc->destroy();
			}
			Session::terminateSession();
		}

		ctx->fileMgr->closeAll(RESERVEDFILEIDS);
		if ((rc=ctx->bufMgr->close(0,true))!=RC_OK) return rc;
		if ((rc=ctx->logMgr->close())!=RC_OK) return rc;

		HeapPageMgr::savePartial(ctx->heapMgr,ctx->ssvMgr);
		ctx->theCB->xPropID=ctx->classMgr->getXPropID();

		if (ctx->theCB->state!=SST_SHUTDOWN_COMPLETE && ctx->theCB->state!=SST_NO_SHUTDOWN)
			{ctx->theCB->state=SST_SHUTDOWN_COMPLETE; if ((rc=ctx->theCB->update(ctx))!=RC_OK) return rc;}

		ctx->theCB->close(ctx);

		ctx->logMgr->deleteLogs();

		delete ctx;

		report(MSG_NOTICE,"MVStore shutdown complete\n");
	// if last one ->
		closeReport();
	
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in shutdownStore\n"); return RC_INTERNAL;}
}

void stopThreads()
{
	RequestQueue::stopThreads();
	LockMgr::stopThreads();
}

StoreCtx::~StoreCtx()
{
	MemAlloc *m=mem;
	if (bufMgr!=NULL) bufMgr->~BufMgr();
	if (classMgr!=NULL) classMgr->~Classifier();
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
	storeTls.set(NULL);
	if (m!=NULL) m->release();
	--nStores;
}
