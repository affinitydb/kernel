/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov, Andrew Skowronski, Michael Andronov 2004 - 2010

**************************************************************************************/
#ifdef _LINUX
#ifndef Darwin

#include "fiolinux.h"
#include "session.h"
#include "startup.h"
#include <limits.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/signal.h> 

using namespace MVStoreKernel;
	
FreeQ<> FileIOLinux::freeAio64;

#ifndef SYNC_IO
struct mv_sync_io
{
	SharedCounter	cnt;
	int				errNo;
	pthread_mutex_t	lock;
	pthread_cond_t	wait;
	mv_sync_io() : errNo(0) {pthread_mutex_init(&lock,NULL); pthread_cond_init(&wait,NULL);}
};
#endif

static sigset_t sigSIO;

FileIOLinux::FileIOLinux() : slotTab(NULL),xSlotTab(FIO_MAX_OPENFILES),flagsFS(0),asyncIOCallback(NULL)
{
	setFlagsFS();
	slotTab = (FileDescLinux*)malloc(sizeof(FileDescLinux)*xSlotTab,STORE_HEAP); 
	if (slotTab!=NULL){ for (int i=0;i<xSlotTab;i++){ slotTab[i].init();}}
	sigemptyset(&sigSIO); sigaddset(&sigSIO,SIGPISIO);
}


FileIOLinux::~FileIOLinux()
{
	if (slotTab!=NULL) {
		closeAll(0);
		free(slotTab,STORE_HEAP);
	}
}

void FileIOLinux::init(void (*cb)(iodesc*))
{
	asyncIOCallback=cb;
#ifndef SYNC_IO
	struct sigaction action; memset(&action,0,sizeof(action));
	action.sa_flags = SA_SIGINFO|SA_RESTART;
	sigemptyset(&action.sa_mask);
	action.sa_sigaction = FileIOLinux::_asyncAIOCompletion;
	if (sigaction(SIGPIAIO, &action, NULL)!=0) report(MSG_CRIT,"Cannot install AIO signal handler (%d)\n",errno);
	action.sa_sigaction = FileIOLinux::_asyncSIOCompletion;
	if (sigaction(SIGPISIO, &action, NULL)!=0) report(MSG_CRIT,"Cannot install SIO signal handler (%d)\n",errno);
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

RC FileIOLinux::open(FileID& fid,const char *fname,const char *dir,ulong flags)
{
	HANDLE fd; off64_t fileSize=0; RC rc = RC_OK;
	if ((fname==NULL || *fname=='\0') && (flags&FIO_TEMP)==0) return RC_INVPARAM;

	bool fdel = false;
	if ((flags&FIO_TEMP)!=0) {
		char *p=(char*)malloc((dir!=NULL?strlen(dir):2)+sizeof(MVSTOREPREFIX)+6+1,STORE_HEAP); 
		if (p==NULL) return RC_NORESOURCES; strcpy(p,dir!=NULL?dir:"./"); strcat(p,MVSTOREPREFIX);
		fname=p; p+=strlen(p); memset(p,'X',6); p[6]='\0'; p=mktemp((char*)fname);
		if (p==NULL||*p=='\0') {free((char*)fname,STORE_HEAP); return RC_NORESOURCES;}
		fdel=true;
	} else if (dir!=NULL && !strchr(fname,'/')) {
		char *p=(char*)malloc(strlen(dir)+strlen(fname)+1,STORE_HEAP); if (p==NULL) return RC_NORESOURCES;
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
	
	char fullbuf[PATH_MAX+1]; 
	const static struct flock flck={F_WRLCK,SEEK_SET,0,0,0};
	fd = open64(fname,((flagsFS&FS_DIRECT)!=0&&(flags&FIO_TEMP)==0?O_DIRECT:0)|O_RDWR|(flags&(FIO_TEMP|FIO_CREATE)?flags&FIO_NEW?O_CREAT|O_EXCL|O_TRUNC:O_CREAT:0),S_IRUSR|S_IWUSR|S_IRGRP);


	if (fd==INVALID_FD || fcntl(fd,F_SETLK,(struct flock*)&flck)!=0) rc=convCode(errno);
	else {
		fileSize=getFileSz(fd);
		char *p=realpath(fname,fullbuf); 
		if (p) {if (fdel) {free((char*)fname,STORE_HEAP); fdel=false;} fname=p;}
	}
	if (rc==RC_OK) {
		FileDescLinux &file=slotTab[fid];
		file.osFile=fd;
		file.fileSize=fileSize;
		file.filePath = strdup(fname,STORE_HEAP);
		file.fTemp=(flags&FIO_TEMP)!=0;
		file.fSize=true;
	}
	lock.unlock();
	if (fdel) free((char*)fname,STORE_HEAP);
	return rc ;
}

off64_t FileIOLinux::getFileSize(FileID fid) const
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

size_t FileIOLinux::getFileName(FileID fid,char buf[],size_t lbuf) const
{
	size_t len = 0;
	RWLockP rw(&lock,RW_S_LOCK);
	if (fid<xSlotTab && slotTab[fid].isOpen()) {
		if (buf!=NULL && lbuf>0) 
			strncpy(buf,slotTab[fid].filePath,lbuf-1)[lbuf-1]=0;
		len = strlen(slotTab[fid].filePath);
	}
	return len;
}

RC FileIOLinux::close(FileID fid)
{
	RWLockP rw(&lock,RW_X_LOCK); RC rc = RC_OK;
	if (fid>=xSlotTab) rc = RC_NOTFOUND;
	else if (slotTab[fid].isOpen()) {
		slotTab[fid].close();
	}
	return RC_OK;
}

void FileIOLinux::closeAll(FileID start)
{
	RWLockP rw(&lock,RW_X_LOCK); 
	for (FileID fid=start; fid<xSlotTab; fid++) if (slotTab[fid].isOpen()) {
		slotTab[fid].close();
	}
}


RC FileIOLinux::growFile(FileID file, off64_t newSize)
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

RC FileIOLinux::listIO(int mode,int nent,iodesc* const* pcbs)
{
	lock.lock(RW_S_LOCK); int i; RC rc=RC_OK;
	aiocb64 **adescs=(aiocb64**)alloca(sizeof(aiocb64*)*nent); if (adescs==NULL) {lock.unlock(); return RC_NORESOURCES;}
	for (i=0; i<nent; i++) if (pcbs[i]!=NULL && pcbs[i]->aio_lio_opcode!=LIO_NOP) {
		assert(pcbs[i]->aio_nbytes>0);
		assert(pcbs[i]->aio_buf!=NULL);
		FileID fid = pcbs[i]->aio_fildes;
		pcbs[i]->aio_rc = RC_OK;
		adescs[i]=(aiocb64*)freeAio64.alloc(sizeof(aiocb64)); if (adescs[i]==NULL){lock.unlock();return RC_NORESOURCES;}
		memset(adescs[i],0,sizeof(aiocb64));
		if (fid>=xSlotTab || !slotTab[fid].isOpen()) {rc=pcbs[i]->aio_rc=RC_INVPARAM; adescs[i]->aio_lio_opcode=LIO_NOP;}
		else if (pcbs[i]->aio_lio_opcode==LIO_READ && (off64_t)(pcbs[i]->aio_nbytes+pcbs[i]->aio_offset)>slotTab[fid].fileSize
			&& (off64_t)(pcbs[i]->aio_nbytes+pcbs[i]->aio_offset)>(slotTab[fid].fileSize=getFileSz(slotTab[fid].osFile))) {
             /*linux doesn't fail for reads beyond EOF*/
			rc=pcbs[i]->aio_rc=RC_EOF; adescs[i]->aio_lio_opcode=LIO_NOP;
		} else { 
		    adescs[i]->aio_fildes = slotTab[fid].osFile; 
			adescs[i]->aio_lio_opcode=pcbs[i]->aio_lio_opcode;
			if ((off64_t)(pcbs[i]->aio_nbytes+pcbs[i]->aio_offset)>slotTab[fid].fileSize) slotTab[fid].fSize=false;
		}
		adescs[i]->aio_sigevent.sigev_notify = SIGEV_NONE;
		adescs[i]->aio_nbytes=pcbs[i]->aio_nbytes;
		adescs[i]->aio_buf=pcbs[i]->aio_buf;
		adescs[i]->aio_offset=pcbs[i]->aio_offset;
#if defined(_POSIX_PRIORITIZED_IO) && defined(_POSIX_PRIORITY_SCHEDULING)
		if (mode==LIO_NOWAIT) adescs[i]->aio_reqprio=AIO_PRIO_DELTA_MAX;
#endif
	}
	lock.unlock();
#ifdef SYNC_IO
	if (mode==LIO_WAIT) {
	  	for (int i=0; i<nent; i++) {
			aiocb64 &aio=*(adescs[i]); int result; RC rcop=RC_OK;
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
			} while(result==EINTR);	// Try again for interrupts (e.g. gdb attach)
			if (rcop!=RC_OK) rc=rcop; //Overall failure if any fail. aio_rc has individual success status.
			pcbs[i]->aio_rc=rcop;
			if (result==0 && pcbs[i]->aio_bFlush){
			   if (result==0&&(flagsFS&FS_DIRECT)==0&&pcbs[i]->aio_bFlush){
			       fdatasync(aio.aio_fildes);
				};
			}
			freeAio64.dealloc(adescs[i]);
		}				
	} else
#else
	mv_sync_io sync; sigset_t omask; 
	if (mode==LIO_WAIT) pthread_sigmask(SIG_BLOCK,&sigSIO,&omask);
#endif
	for (i=0; i<nent; i++) if (pcbs[i]!=NULL && adescs[i]!=NULL) {
		if (adescs[i]->aio_lio_opcode==LIO_NOP) {asyncIOCallback(pcbs[i]); freeAio64.dealloc(adescs[i]); adescs[i]=NULL; continue;}
		aiocb64 &aio=*(adescs[i]);
#ifndef SYNC_IO
		if (mode==LIO_WAIT) {
	        aio.aio_sigevent.sigev_notify			 = SIGEV_SIGNAL;
			aio.aio_sigevent.sigev_signo			 = SIGPISIO;
			aio.aio_sigevent.sigev_value.sival_ptr	 = &sync;
			aio.aio_sigevent.sigev_notify_function   = (void(*)(union sigval))0;
			aio.aio_sigevent.sigev_notify_attributes = NULL;
			++sync.cnt;
		} else {
#endif
#ifdef STORE_AIO_THREAD
		aio.aio_sigevent.sigev_notify			 = SIGEV_THREAD;
		aio.aio_sigevent.sigev_notify_function	 = FileIOLinux::_asyncIOCompletion;
		aio.aio_sigevent.sigev_notify_attributes = NULL;
		aio.aio_sigevent.sigev_value.sival_ptr	 = pcbs[i];
#else
        aio.aio_sigevent.sigev_notify			 = SIGEV_SIGNAL;
		aio.aio_sigevent.sigev_signo			 = SIGPIAIO;
		aio.aio_sigevent.sigev_value.sival_ptr	 = pcbs[i];
		aio.aio_sigevent.sigev_notify_function   = (void(*)(union sigval))0;
		aio.aio_sigevent.sigev_notify_attributes = NULL;
#endif
		pcbs[i]->aio_ptr[pcbs[i]->aio_ptrpos++]	 = adescs[i];
		pcbs[i]->aio_ptr[pcbs[i]->aio_ptrpos++]	 = this;
#ifndef SYNC_IO
		}
#endif
		assert(pcbs[i]->aio_ptrpos<=FIO_MAX_PLUGIN_CHAIN);  

		if ((aio.aio_lio_opcode==LIO_WRITE?aio_write64(&aio):aio_read64(&aio))!=0) {
			rc=pcbs[i]->aio_rc=convCode(errno);
#ifndef SYNC_IO
			if (mode==LIO_WAIT) --sync.cnt; else
#endif
			{pcbs[i]->aio_ptrpos-=2; asyncIOCallback(pcbs[i]);}
			freeAio64.dealloc(adescs[i]);
		}
	}
#ifndef SYNC_IO
	if (mode==LIO_WAIT) {
		pthread_mutex_lock(&sync.lock);
		while (sync.cnt>0) pthread_cond_wait(&sync.wait,&sync.lock);
		pthread_mutex_unlock(&sync.lock);
		pthread_sigmask(SIG_SETMASK,&omask,0);
		if (rc==RC_OK) rc=convCode(sync.errNo);
		for (i=0; i<nent; i++) if (adescs[i]!=NULL) freeAio64.dealloc(adescs[i]);
	}
#endif
	return rc;
}

#ifdef STORE_AIO_THREAD
void FileIOLinux::_asyncIOCompletion(sigval_t val) 
{
	IStoreIO::iodesc *pcbs = (IStoreIO::iodesc*)val.sival_ptr; 
	if (pcbs!=NULL && pcbs->aio_ptrpos>0) {
		FileIOLinux* pThis=(FileIOLinux*)pcbs->aio_ptr[--(pcbs->aio_ptrpos)];
		aiocb64 *aio=(aiocb64 *)pcbs->aio_ptr[--(pcbs->aio_ptrpos)];
		if (pThis!=NULL&&pThis->asyncIOCallback!=NULL) pThis->asyncIOCallback(pcbs);
		freeAio64.dealloc(aio);
	}
}
#else
namespace MVStoreKernel
{
FreeQ<> FileIOLinux::freeIORequests;
class IOCompletionRequest : public Request
{
	FileIOLinux			*const	fio;
	IStoreIO::iodesc	*const	pcbs;
	aiocb64				*const	aio;
public:
	IOCompletionRequest(FileIOLinux *f,IStoreIO::iodesc *pc,aiocb64 *ai) : fio(f),pcbs(pc),aio(ai) {}
	void process() {if (fio->asyncIOCallback!=NULL) fio->asyncIOCallback(pcbs);}
	void destroy() {if (aio!=NULL) FileIOLinux::freeAio64.dealloc(aio); FileIOLinux::freeIORequests.dealloc(this);}
};
void FileIOLinux::_asyncAIOCompletion(int sig, siginfo_t *info, void *uap)
{
	if (info==NULL) return; assert(sig==SIGPIAIO);
	IStoreIO::iodesc *pcbs=(IStoreIO::iodesc*)info->si_value.sival_ptr;
	if (pcbs!=NULL && pcbs->aio_ptrpos>0) {
		FileIOLinux* pThis=(FileIOLinux*)pcbs->aio_ptr[--pcbs->aio_ptrpos];
		aiocb64 *aio=(aiocb64 *)pcbs->aio_ptr[--pcbs->aio_ptrpos];
		if (pThis!=NULL && pThis->asyncIOCallback!=NULL) {
			void *rq=freeIORequests.alloc(sizeof(IOCompletionRequest));
			if (rq!=NULL) RequestQueue::postRequest(new(rq) IOCompletionRequest(pThis,pcbs,aio),NULL,RQ_IO);
		} else if (aio!=NULL) freeAio64.dealloc(aio);
	}
}
void FileIOLinux::_asyncSIOCompletion(int sig, siginfo_t *info, void *uap)
{
	if (info==NULL) return; assert(sig==SIGPISIO);
	mv_sync_io *sio=(mv_sync_io*)info->si_value.sival_ptr;
	if (sio!=NULL) {
		pthread_mutex_lock(&sio->lock);
		if (info->si_errno!=0) sio->errNo=info->si_errno;
		if (--sio->cnt==0) pthread_cond_signal(&sio->wait);
		pthread_mutex_unlock(&sio->lock);
	}
}
};
#endif

bool FileIOLinux::asyncIOEnabled() const
{	
	return (flagsFS&FS_DIRECT)!=0;
}

#define TESTFILENAME	"chaosdb.tst"

void FileIOLinux::setFlagsFS()
{
	flagsFS = 0;
#if defined(O_DIRECT) && O_DIRECT!=0
	byte *buf=(byte*)allocAligned(0x1000,0x1000);
	int fd = ::open64(TESTFILENAME,O_DIRECT|O_RDWR|O_CREAT|O_TRUNC,S_IRUSR|S_IWUSR);
	if (fd<0) report(MSG_DEBUG,"Cannot open "TESTFILENAME"\n");
	else {
		if (pwrite64(fd,buf,0x1000,0)==0x1000) flagsFS|=FS_DIRECT;
		else report(MSG_INFO,"O_DIRECT failed, continue with fdatasync()\n");
		::close(fd); ::unlink(TESTFILENAME); 
	}
	freeAligned(buf);
#endif
}

RC FileIOLinux::deleteFile(const char *fname)
{
	if (unlink(fname)==-1)return convCode(errno);
	return RC_OK;
}


#include <dirent.h>
#include <fnmatch.h>

void FileIOLinux::deleteLogFiles(ulong maxFile,const char *lDir,bool fArchived)
{
	deleteLogFiles(MVSTOREPREFIX"*"LOGFILESUFFIX,maxFile,lDir,fArchived);
}

void FileIOLinux::deleteLogFiles(const char *mask,ulong maxFile,const char *lDir,bool fArchived)
{
	char *end;
	DIR *dirP=opendir(lDir!=NULL?lDir:"./");
	if (dirP!=NULL) {
		struct dirent *ep; char buf[PATH_MAX+1];
		while ((ep=readdir(dirP))!=NULL) if (fnmatch(mask,ep->d_name,FNM_PATHNAME|FNM_PERIOD)==0) {
			if (fArchived) {
				// ???
			}
			else if (maxFile==~0ul || strtoul(ep->d_name+sizeof(MVSTOREPREFIX),&end,16)<=maxFile) {	// 1 more than prefix size (for 'A' or 'B')
				strcpy(buf,lDir!=NULL?lDir:"./"); strcat(buf,ep->d_name); unlink(buf);
			}
		}
		closedir(dirP);
	}
}

IStoreIO *getStoreIO() 
{ 
	try {return new(STORE_HEAP) FileIOLinux;}
	catch (...) {report(MSG_ERROR,"Exception in getStoreIO\n"); return NULL;}
}

#endif
#endif
