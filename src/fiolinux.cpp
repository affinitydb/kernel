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

Written by Mark Venguerov, Michael Andronov 2004-2012

**************************************************************************************/
#if (defined(_LINUX) || defined(ANDROID)) && !defined(__APPLE__)

#include <limits.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#ifndef ANDROID
#include <sys/signal.h>
#endif
#include "fiolinux.h"
#include "session.h"
#include "startup.h"
#include "request.h"

using namespace AfyKernel;

#ifndef SYNC_IO
struct afy_sync_io
{
	SharedCounter	cnt;
	int				errNo;
	pthread_mutex_t	lock;
	pthread_cond_t	wait;
	afy_sync_io() : errNo(0) {pthread_mutex_init(&lock,NULL); pthread_cond_init(&wait,NULL);}
};
#endif

static sigset_t sigSIO;

FileMgr::FileMgr(StoreCtx *ct,int maxOpenFiles,const char *ldDir) : GFileMgr(ct,maxOpenFiles,ldDir)
{
	sigemptyset(&sigSIO); sigaddset(&sigSIO,SIGAFYSIO);
#ifndef SYNC_IO
	struct sigaction action; memset(&action,0,sizeof(action));
	action.sa_flags = SA_SIGINFO|SA_RESTART;
	sigemptyset(&action.sa_mask);
	action.sa_sigaction = FileMgr::_asyncAIOCompletion;
	if (sigaction(SIGAFYAIO, &action, NULL)!=0) report(MSG_CRIT,"Cannot install AIO signal handler (%d)\n",errno);
	action.sa_sigaction = FileMgr::_asyncSIOCompletion;
	if (sigaction(SIGAFYSIO, &action, NULL)!=0) report(MSG_CRIT,"Cannot install SIO signal handler (%d)\n",errno);
#endif
}

inline off64_t getFileSz(int fd)
{
#ifndef __arm__	
	struct stat64 fileStats;
	return fstat64(fd,&fileStats)==0?fileStats.st_size:0;
#else
	struct stat fileStats;		
	return fstat(fd,&fileStats)==0?fileStats.st_size:0;
#endif
}

RC FileMgr::open(FileID& fid,const char *fname,unsigned flags)
{
	HANDLE fd; off64_t fileSize=0; RC rc=RC_OK;
	if ((fname==NULL || *fname=='\0') && (flags&FIO_TEMP)==0) return RC_INVPARAM;

	const char *dir=ctx->getDirectory(); bool fdel=false;
	if ((flags&FIO_TEMP)!=0) {
		char *p=(char*)ctx->malloc((dir!=NULL?strlen(dir):2)+sizeof(STOREPREFIX)+6+1);
		if (p==NULL) return RC_NORESOURCES; strcpy(p,dir!=NULL?dir:"./"); strcat(p,STOREPREFIX);
		fname=p; p+=strlen(p); memset(p,'X',6); p[6]='\0'; p=mktemp((char*)fname);
		if (p==NULL||*p=='\0') {ctx->free((char*)fname); return RC_NORESOURCES;}
		fdel=true;
	} else if (dir!=NULL && !strchr(fname,'/')) {
		char *p=(char*)ctx->malloc(strlen(dir)+strlen(fname)+1); if (p==NULL) return RC_NORESOURCES;
		strcpy(p,dir); strcat(p,fname); fname=p; fdel=true;
	}

	lock.lock(RW_X_LOCK);
	if (fid==INVALID_FILEID) {
		for(fid=0;fid<xSlotTab;fid++){
			if (!slotTab[fid].isOpen()) break;
		}
	}
	if (fid>=xSlotTab) {lock.unlock(); return RC_NORESOURCES;}
	if (slotTab[fid].isOpen()) { 
		if ((flags&FIO_REPLACE)==0)	{lock.unlock(); return RC_ALREADYEXISTS;}
		slotTab[fid].close();
	}
	
	char fullbuf[PATH_MAX+1]; const static struct flock flck={F_WRLCK,SEEK_SET,0,0,0};
	fd = ::open64(fname,((flags&FIO_TEMP)==0?O_DIRECT:0)|O_RDWR|(flags&(FIO_TEMP|FIO_CREATE)?flags&FIO_NEW?O_CREAT|O_EXCL|O_TRUNC:O_CREAT:0),S_IRUSR|S_IWUSR|S_IRGRP);


	if (fd==INVALID_FD || fcntl(fd,F_SETLK,(struct flock*)&flck)!=0) rc=convCode(errno);
	else {
		fileSize=getFileSz(fd);
		char *p=realpath(fname,fullbuf); 
		if (p) {if (fdel) {ctx->free((char*)fname); fdel=false;} fname=p;}
	}
	if (rc==RC_OK) {
		FileDesc &file=slotTab[fid];
		file.osFile=fd;
		file.fileSize=fileSize;
		file.filePath = strdup(fname,STORE_HEAP);
		file.fTemp=(flags&FIO_TEMP)!=0;
		file.fSize=true;
	}
	lock.unlock();
	if (fdel) ctx->free((char*)fname);
	return rc ;
}

off64_t GFileMgr::getFileSize(FileID fid)
{
	off64_t size=0; RWLockP rw(&lock,RW_S_LOCK);
	if (fid<xSlotTab && slotTab[fid].isOpen()) {
		if (!slotTab[fid].fSize) {
			rw.set(NULL); rw.set(&lock,RW_X_LOCK);
			if (slotTab[fid].isOpen() && !slotTab[fid].fSize) 
				{slotTab[fid].fileSize=getFileSz(slotTab[fid].osFile); slotTab[fid].fSize=true;}
		}
		size=slotTab[fid].fileSize;
	}
	return size;
}

RC GFileMgr::growFile(FileID file, off64_t newSize)
{
	lock.lock(RW_S_LOCK);
	if (file>=xSlotTab || !slotTab[file].isOpen()) {lock.unlock(); return RC_NOTFOUND;}
	HANDLE h=slotTab[file].osFile;
	off64_t currentSize=slotTab[file].fileSize;
	lock.unlock();
	RC rc = ftruncate64(h,newSize) ? convCode(errno) : RC_OK;
	if (rc==RC_OK) {lock.lock(RW_X_LOCK); if (slotTab[file].fileSize==currentSize) slotTab[file].fileSize=newSize; lock.unlock();}
	return rc;
}

#ifdef ANDROID
namespace AfyKernel
{
class GenericAIORequest : public Request
{
	FileMgr * const fio;
	myaio * aio; // apparently the caller always holds these until completion, now
public:
	GenericAIORequest(FileMgr *f, myaio *pcb) : fio(f), aio(pcb) {}
	void process() { aio->aio_rc=fio->doSyncIo(*aio); if (fio->asyncIOCallback!=NULL) fio->asyncIOCallback(aio); }
	void destroy() { FileMgr::freeIORequests.dealloc(this); }
};
}
#endif

RC FileMgr::listIO(int mode,int nent,myaio* const* pcbs,bool fSync)
{
	lock.lock(RW_S_LOCK); int i; RC rc=RC_OK;
	try {
		for (i=0; i<nent; i++) if (pcbs[i]!=NULL && pcbs[i]->aio_lio_opcode!=LIO_NOP) {
			assert(pcbs[i]->aio_nbytes>0);
			assert(pcbs[i]->aio_buf!=NULL);
			FileID fid=pcbs[i]->aio_fid;
			pcbs[i]->aio_rc=RC_OK;
			if (fid>=xSlotTab || !slotTab[fid].isOpen()) {rc=pcbs[i]->aio_rc=RC_INVPARAM; pcbs[i]->aio_lio_opcode=LIO_NOP;}
			else if (pcbs[i]->aio_lio_opcode==LIO_READ && (off64_t)(pcbs[i]->aio_nbytes+pcbs[i]->aio_offset)>slotTab[fid].fileSize
				&& (off64_t)(pcbs[i]->aio_nbytes+pcbs[i]->aio_offset)>(slotTab[fid].fileSize=getFileSz(slotTab[fid].osFile))) {
	             /*linux doesn't fail for reads beyond EOF*/
				rc=pcbs[i]->aio_rc=RC_EOF; pcbs[i]->aio_lio_opcode=LIO_NOP;
			} else { 
				pcbs[i]->aio_fildes=slotTab[fid].osFile;
				if ((off64_t)(pcbs[i]->aio_nbytes+pcbs[i]->aio_offset)>slotTab[fid].fileSize) slotTab[fid].fSize=false;
			}
#ifndef ANDROID
			pcbs[i]->aio_sigevent.sigev_notify = SIGEV_NONE;
#endif
#if defined(_POSIX_PRIORITIZED_IO) && defined(_POSIX_PRIORITY_SCHEDULING)
			if (mode==LIO_NOWAIT) pcbs[i]->aio_reqprio=AIO_PRIO_DELTA_MAX;
#endif
		}
		lock.unlock();
#ifdef ANDROID
	if (mode==LIO_WAIT) {
		for (int i=0; i<nent; i++) {
			aiocb64 &aio=*pcbs[i];
			pcbs[i]->aio_rc=doSyncIo(aio);
		}
	} else {
		for (i=0; i<nent; i++) if (pcbs[i]!=NULL) {
			aiocb64 &aio=*pcbs[i];
			if (aio.aio_lio_opcode==LIO_NOP) { asyncIOCallback(pcbs[i]); continue; }
			void *rq=freeIORequests.alloc(sizeof(GenericAIORequest));
			if (rq!=NULL) RequestQueue::postRequest(new(rq) GenericAIORequest(this,pcbs[i]),NULL,RQ_IO);
		}
	}
#else
#ifdef SYNC_IO
		if (mode==LIO_WAIT) {
		  	for (int i=0; i<nent; i++) {
				aiocb64 &aio=*pcbs[i]; int result; RC rcop=RC_OK;
				if (aio.aio_lio_opcode==LIO_NOP) continue;
				do {
					result=0;
					if (aio.aio_lio_opcode==LIO_WRITE) {
						if ((size_t)pwrite64(aio.aio_fildes,(const void *)aio.aio_buf,aio.aio_nbytes,aio.aio_offset)!=aio.aio_nbytes)
							result=errno;
					} else if (aio.aio_lio_opcode==LIO_READ) {
						if ((size_t)pread64(aio.aio_fildes,(void *)aio.aio_buf,aio.aio_nbytes,aio.aio_offset)!=aio.aio_nbytes)
							result=errno;
					}
					if (result==0) {rcop=RC_OK;} else {rcop=convCode(result);}
				} while (result==EINTR);	// Try again for interrupts (e.g. gdb attach)
				if (rcop!=RC_OK) rc=rcop; //Overall failure if any fail. aio_rc has individual success status.
				pcbs[i]->aio_rc=rcop;
#if 0
				if (result==0&&(flagsFS&FS_DIRECT)==0&&fSync) {
			       fdatasync(aio.aio_fildes);
				}
#endif
			}				
		} else
#else
		afy_sync_io sync; sigset_t omask; 
		if (mode==LIO_WAIT) pthread_sigmask(SIG_BLOCK,&sigSIO,&omask);
#endif
		for (i=0; i<nent; i++) if (pcbs[i]!=NULL) {
			aiocb64 &aio=*pcbs[i];
			if (aio.aio_lio_opcode==LIO_NOP) {asyncIOCallback(pcbs[i]); continue;}
#ifndef SYNC_IO
			if (mode==LIO_WAIT) {
		        aio.aio_sigevent.sigev_notify			 = SIGEV_SIGNAL;
				aio.aio_sigevent.sigev_signo			 = SIGAFYSIO;
				aio.aio_sigevent.sigev_value.sival_ptr	 = &sync;
				aio.aio_sigevent.sigev_notify_function   = (void(*)(union sigval))0;
				aio.aio_sigevent.sigev_notify_attributes = NULL;
				++sync.cnt;
			} else {
#endif
#ifdef STORE_AIO_THREAD
				aio.aio_sigevent.sigev_notify			 = SIGEV_THREAD;
				aio.aio_sigevent.sigev_notify_function	 = FileMgr::_asyncIOCompletion;
				aio.aio_sigevent.sigev_notify_attributes = NULL;
				aio.aio_sigevent.sigev_value.sival_ptr	 = pcbs[i];
#else
		        aio.aio_sigevent.sigev_notify			 = SIGEV_SIGNAL;
				aio.aio_sigevent.sigev_signo			 = SIGAFYAIO;
				aio.aio_sigevent.sigev_value.sival_ptr	 = pcbs[i];
				aio.aio_sigevent.sigev_notify_function   = (void(*)(union sigval))0;
				aio.aio_sigevent.sigev_notify_attributes = NULL;
#endif
#ifndef SYNC_IO
			}
#endif

			if ((aio.aio_lio_opcode==LIO_WRITE?aio_write64(&aio):aio_read64(&aio))!=0) {
				rc=pcbs[i]->aio_rc=convCode(errno);
#ifndef SYNC_IO
				if (mode==LIO_WAIT) --sync.cnt; else
#endif
				asyncIOCallback(pcbs[i]);
			}
		}
#ifndef SYNC_IO
		if (mode==LIO_WAIT) {
			pthread_mutex_lock(&sync.lock);
			while (sync.cnt>0) pthread_cond_wait(&sync.wait,&sync.lock);
			pthread_mutex_unlock(&sync.lock);
			pthread_sigmask(SIG_SETMASK,&omask,0);
			if (rc==RC_OK) rc=convCode(sync.errNo);
		}
#endif
#endif
	} catch (RC rc2) {
		rc=rc2;
		for (i=0; i<nent; i++) {
			pcbs[i]->aio_rc=rc2; if (mode!=LIO_WAIT) asyncIOCallback(pcbs[i]);
		}
	}
	return rc;
}

#ifdef ANDROID
RC FileMgr::doSyncIo(myaio& aio)
{
	int result; RC rcop=RC_OK;
	if (aio.aio_lio_opcode!=LIO_NOP) {
		do {
			result=0;
			if (aio.aio_lio_opcode==LIO_WRITE) {
				if ((size_t)pwrite(aio.aio_fildes,(const void*)aio.aio_buf,aio.aio_nbytes,aio.aio_offset)!=aio.aio_nbytes)
					result=errno;
				} else if (aio.aio_lio_opcode==LIO_READ) {
					if ((size_t)pread(aio.aio_fildes,(void*)aio.aio_buf,aio.aio_nbytes,aio.aio_offset)!=aio.aio_nbytes)
						result=errno;
				}
				if (result==0) {rcop=RC_OK;} else {rcop=convCode(result);}   
		} while (result==EINTR);
	}
	return rcop;
}
#endif

#ifdef STORE_AIO_THREAD
void FileMgr::_asyncIOCompletion(sigval_t val) 
{
	myaio *pcbs=(myaio*)val.sival_ptr; 
	if (pcbs!=NULL) GFileMgr::asyncIOCallback(pcbs,true);
}
#else
namespace AfyKernel
{
FreeQ FileMgr::freeIORequests;
class IOCompletionRequest : public Request
{
	myaio	*const	aio;
public:
	IOCompletionRequest(myaio *ai) : aio(ai) {}
	void process() {GFileMgr::asyncIOCallback(aio,true);}
	void destroy() {FileMgr::freeIORequests.dealloc(this);}
};
void FileMgr::_asyncAIOCompletion(int sig, siginfo_t *info, void *uap)
{
	if (info==NULL) return; assert(sig==SIGAFYAIO);
	myaio *pcbs=(myaio*)info->si_value.sival_ptr;
	if (pcbs!=NULL) {
		void *rq=freeIORequests.alloc(sizeof(IOCompletionRequest));
		if (rq!=NULL) RequestQueue::postRequest(new(rq) IOCompletionRequest(pcbs),NULL,RQ_IO);
	}
}

void FileMgr::_asyncSIOCompletion(int sig, siginfo_t *info, void *uap)
{
#ifndef ANDROID
	if (info==NULL) return; assert(sig==SIGAFYSIO);
	afy_sync_io *sio=(afy_sync_io*)info->si_value.sival_ptr;
	if (sio!=NULL) {
		pthread_mutex_lock(&sio->lock);
		if (info->si_errno!=0) sio->errNo=info->si_errno;
		if (--sio->cnt==0) pthread_cond_signal(&sio->wait);
		pthread_mutex_unlock(&sio->lock);
	}
#endif
}
};
#endif

RC GFileMgr::deleteFile(const char *fname)
{
	return unlink(fname)<0 ? convCode(errno) : RC_OK;
}

#include <dirent.h>
#include <fnmatch.h>

void GFileMgr::deleteLogFiles(const char *mask,unsigned maxFile,const char *lDir,bool fArchived)
{
	DIR *dirP=opendir(lDir!=NULL?lDir:"./");
	if (dirP!=NULL) {
		struct dirent *ep; char buf[PATH_MAX+1],*end;
		while ((ep=readdir(dirP))!=NULL) if (fnmatch(mask,ep->d_name,FNM_PATHNAME|FNM_PERIOD)==0) {
			if (fArchived) {
				// ???
			}
			else if (maxFile==~0u || strtoul(ep->d_name+sizeof(LOGPREFIX),&end,16)<=maxFile) {	// 1 more than prefix size (for 'A' or 'B')
				strcpy(buf,lDir!=NULL?lDir:"./"); strcat(buf,ep->d_name); unlink(buf);
			}
		}
		closedir(dirP);
	}
}

#include <dlfcn.h>

RC GFileMgr::loadExt(const char *path,size_t l,Session *ses,const Value *pars,unsigned nPars,bool fNew)
{
	if (path==NULL || l==0) return RC_INVPARAM;

	RC rc=RC_OK;
	size_t ld=loadDir!=NULL && !memchr(path,'/',l)?strlen(loadDir):0,le=l<4||memcmp(path+l-3,".so",3)!=0?3:0,lp=0,sht=0;
	for (const char *pp=path+l;;) {if (*--pp=='/') sht=++pp-path; else if (pp!=path) continue; if (sht+3>l || pp[0]!='l' || pp[1]!='i' || pp[2]!='b') lp=3; break;}
	char *p=(char*)((StoreCtx*)ctx)->malloc(ld+lp+l+le+1); if (p==NULL) return RC_NORESOURCES;
	if (ld!=0) memcpy(p,loadDir,ld);
	if (lp!=0) {memcpy(p+ld,path,sht); memcpy(p+ld+sht,"lib",3); memcpy(p+ld+sht+3,path+sht,l-sht);} else memcpy(p+ld,path,l);
	if (le!=0) memcpy(p+ld+lp+l,".so",3); p[ld+lp+l+le]='\0';

	report(MSG_INFO,"Loading %s...\n",p);

#ifdef AFFINITY_STATIC_LINK
	rc=((StoreCtx*)ctx)->initStaticService(path+sht+3-lp,l-sht+le-3,ses,pars,nPars,fNew);
#else
	void *fd = dlopen(p,RTLD_LAZY);
	if (fd==NULL) {report(MSG_ERROR,dlerror()); rc=RC_OTHER;}
	else {
		typedef bool (*InitFunc)(ISession*,const Value *pars,unsigned nPars,bool fNew);
		InitFunc init = (InitFunc)dlsym(fd,INIT_ENTRY_NAME);
		if (init==NULL) {report(MSG_ERROR,dlerror()); rc=RC_NOTFOUND;}
		else if (!init(ses,pars,nPars,fNew)) rc=RC_FALSE;
	}
#endif
	if (rc==RC_OK) report(MSG_INFO,"%s succesfully loaded\n",p); else report(MSG_ERROR,"%s: initialization failed (%d)\n",p,rc);
	ctx->free(p);
	return rc;
}

#endif
