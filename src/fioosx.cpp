/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov, Andrew Skowronski, Michael Andronov 2004 - 2010

**************************************************************************************/

#ifdef Darwin
#include "fioosx.h"
#include "session.h"
#include "startup.h"
/* from the manual page of Xcode Tools version 4.0:
   ...
   The fstat64, lstat64 and stat64 routines are equivalent to their corresponding non-64-suffixed routine,
    when 64-bit inodes are in effect.  They were added before there was support for the symbol variants,
    and so are now deprecated.  Instead of using these, set the _DARWIN_USE_64_BIT_INODE macro before
    including header files to force 64-bit inode support.
*/
#define _DARWIN_USE_64_BIT_INODE
#include <limits.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/signal.h> 

#ifdef Darwin
//The functions are called properly ( to 32/64 implementation) internally
#define open64 open
#define ftruncate64 ftruncate
#define pwrite64 pwrite
#define pread64 pread
#define aio_write64 aio_write 
#define aio_read64 aio_write 

#define aiocb64 aiocb
#define fdatasync(x) fsync(x)
#endif

using namespace MVStoreKernel;
		
FreeQ<> FileIOOSX::freeAio64;

/* Darwin specific ... */
FreeQ<> AsyncReqQ::lioReqs;
AsyncReqQ FileIOOSX::lioAQueue; //track for outstanding I/O(s).
 
namespace MVStoreKernel
{

  
/* Uner OSX, within the signal, the info does not provide signal value information. 
*   The signal is only setting the semaphore, 
*   and the real processing is done within 
*   void * AsyncReqQ::asyncOSXFinalize(void *pp)
*/
void FileIOOSX::_asyncAIOCompletion(int sig, siginfo_t *info, void *uap)
{
    ++FileIOOSX::lioAQueue.nSig; 
    FileIOOSX::lioAQueue.sigSem.wakeup();
}

/*
* Submitting a new aio request to the ring buffer
*/
AIOElt * AsyncReqQ::add( void * p){
    AIOElt * elt;
    lock.lock(RW_X_LOCK);
    //incapsulating the 'payload' into DLList element...
    void *rq=lioReqs.alloc(sizeof(AIOElt));
	if (rq!=NULL){
        elt = new(rq) AIOElt(p);
		elt->setNotQueued();
	   if( NULL == FileIOOSX::lioAQueue.q){
            FileIOOSX::lioAQueue.q = elt;
        }else{
            FileIOOSX::lioAQueue.q->insertLast(elt);
        }
        ++FileIOOSX::lioAQueue.nSig; 
		FileIOOSX::lioAQueue.sigSem.wakeup();
    }
    lock.unlock();
    return elt; 
};

/* 
 * Unchain and delete the element with the processed aio ... 
 */
AIOElt * unchain(AIOElt *elt){
    
    FileIOOSX::lioAQueue.lock.lock(RW_X_LOCK);	
	AIOElt * rc =  (AIOElt *)elt->prev;
	if(AIOElt::DONE == elt->getState() ){
		rc =  (AIOElt *)elt->prev;
    	if( rc == elt ){
			FileIOOSX::lioAQueue.q = NULL;  rc = NULL;
	    }else if ( FileIOOSX::lioAQueue.q == elt){	   
			FileIOOSX::lioAQueue.q = (AIOElt *)elt->prev;
		}
		elt->remove();
	    AsyncReqQ::lioReqs.dealloc(elt);
    }
    FileIOOSX::lioAQueue.lock.unlock();
    return rc;
};

static sigset_t sigSIO;

FreeQ<> FileIOOSX::freeIORequests;
class IOCompletionRequest : public Request
{
       FileIOOSX			*const	fio;
       IStoreIO::iodesc	*const	pcbs;
       aiocb64				*const	aio;
   public:
       IOCompletionRequest(FileIOOSX *f,IStoreIO::iodesc *pc,aiocb64 *ai) : fio(f),pcbs(pc),aio(ai) {}
       void process() {if (fio->asyncIOCallback!=NULL) fio->asyncIOCallback(pcbs);}
       void destroy() {if (aio!=NULL) FileIOOSX::freeAio64.dealloc(aio); FileIOOSX::freeIORequests.dealloc(this);}
};

/**
 * The major addition - to handle asynchronous operations on OS X. 
 * There is no notificaiton functions within the OS X. 
 * The signal support is limited ( no way to provide the context to signal). 
 * Plus the amoung of signal-sage operations/system function calls is very 
 * limited. 
 * The following strategy is implemented: 
 *  - `worker` thread is waiting for procesing async operation completion; 
 *  - the signal received as there are completed async. operation(s); 
 *    mach semaphore is signaled within the signal handler; 
 *  - above semaphore wakes up the `worker` thread, which completes the work 
 *    for async operation completion. 
 *
 *  void * FileIOOSX::asyncOSXFinalize(void *pp)
 *  pthread function, representing the `worker` thread for the async. operation
 *   completion.
 **/
void * AsyncReqQ::asyncOSXFinalize(void *pp)
{
	SharedCounter u,w;
	  
   while(true){
	   bool repeat;
	   FileIOOSX::lioAQueue.sigSem.wait();
	   
       do{
		   repeat = false; // set in case EAGAIN received during aio to OSX submition
		 
		   if (NULL != FileIOOSX::lioAQueue.q){  
		     for(AIOElt * pel = FileIOOSX::lioAQueue.q;  ; pel = (AIOElt *)pel->next){
				  bool  bunChn = false ;  
			   
		          // Getting access to aiocb, to check current status... 
				  IStoreIO::iodesc *pcbs;
				  pcbs=(IStoreIO::iodesc *)pel->getPayload();
				  aiocb64 *aio=(aiocb64 *)pcbs->aio_ptr[1];
		    
		          if( AIOElt::QUEUED  == pel->getState()){
			            	//Checking status:  the aio may be ready, in progress, or an error may happen... 
			                int st = aio_error(aio);
                            if( 0 == st ){  //ok, I/O completed normally...
			                   if (pcbs!=NULL && pcbs->aio_ptrpos>0) {
			                        FileIOOSX* pThis=(FileIOOSX*)pcbs->aio_ptr[--pcbs->aio_ptrpos];
			                        aiocb64 *aio=(aiocb64 *)pcbs->aio_ptr[--pcbs->aio_ptrpos];
			                        if (pThis!=NULL && pThis->asyncIOCallback!=NULL) {
			                            void *rq=pThis->freeIORequests.alloc(sizeof(IOCompletionRequest));
			                            if (rq!=NULL) RequestQueue::postRequest(new(rq) IOCompletionRequest(pThis,pcbs,aio),NULL,RQ_IO);
			                            else{
										}
			                        } else if (aio!=NULL){ 
			                                pThis->freeAio64.dealloc(aio);
			                        }else{
				                            report(MSG_DEBUG, "asyncFinalize:: stratge 1 ?????????????? errno: %d \n",errno);  
			                        }
			                   }
			                   aio_return(aio);	                    
						       ++FileIOOSX::lioAQueue.aioCnt; 
							   pel->setDone();	
						       bunChn =  true;  
			                }else if ( st < 0){
							    report(MSG_DEBUG, "asyncFinalize:: Attempt to process non-scheduled aiocb\n");
			                    assert(false);
			                }else if ( st > 0){
		                    	switch (st) {
			                        case EINPROGRESS:
			                            //If the request has not yet completed, EINPROGRESS is returned.
			                            //report(MSG_DEBUG, "asyncFinalize:: EINPROGRESS  errno: %d  aio %llx\n", errno, aio);
			                            break;
			                        default:
			                              report(MSG_DEBUG, "asyncFinalize:: st(%d) >0  errno: %d \n",st, errno);
			                            break;
			                    }
		                    }
		          }
		          else if( AIOElt::NOT_IN_ASYNC_QUEUE  == pel->getState()){
		                    // Submitting aio requests to OSX
		               		if( !(--FileIOOSX::lioAQueue.aioCnt)){
			                  //???
							}else {
								    pel->setQueued(); 
								    int era = (aio->aio_lio_opcode==LIO_WRITE?aio_write64(aio):aio_read64(aio));
									int e = errno; 
						            aio_error(aio);
					                if( era < 0 ){              
						                long t = FileIOOSX::lioAQueue.aioCnt;
		               	
								        switch (e){
						                    case EINVAL:
						                        report(MSG_DEBUG,"asyncFinalize:: I/O EINVAL %d %d\n", e,t);
						                        //??????
						                        break;
						                    case EAGAIN:                
						                        //report(MSG_DEBUG, "asyncFinalize:: I/O EAGAIN  %d %d\n", e,t);
					                            pel->setNotQueued();
					                            ++FileIOOSX::lioAQueue.aioCnt;
								                //aio_return(aio);
								                repeat = true;
						                        //::sched_yield();
								                break;
						                    default:
						                        report(MSG_DEBUG,"asyncFinalize:: I/O problem %d %d\n", e,t);
						                        //??????
												break;
						                } 
		                   
						            }else if( era == 0){
							            //report(MSG_DEBUG,"DONE:: ae(%llx) %d ern %d e %d\n",aio, ae, errno, e);
							        }else{
										report(MSG_DEBUG, "I/O Async problem unknown\n"); //??????
						            }
					        }
				     }
				    
				    if(AIOElt::DONE == pel->getState() ){
						pel = unchain(pel); 
						if(!pel) break;
					}
		      }
	       }
	   }while(repeat);	
   }
   return NULL;
}


FileIOOSX::FileIOOSX() : slotTab(NULL),xSlotTab(FIO_MAX_OPENFILES),flagsFS(0),asyncIOCallback(NULL)
{
	setFlagsFS();
	slotTab = (FileDescLinux*)malloc(sizeof(FileDescLinux)*xSlotTab,STORE_HEAP); 
	if (slotTab!=NULL){ for (int i=0;i<xSlotTab;i++){ slotTab[i].init();}}
	sigemptyset(&sigSIO); sigaddset(&sigSIO,SIGPISIO);

}

FileIOOSX::~FileIOOSX()
{
	if (slotTab!=NULL) {
		closeAll(0);
		free(slotTab,STORE_HEAP);
	}
}

void FileIOOSX::init(void (*cb)(iodesc*))
{
	asyncIOCallback=cb;
    
	struct sigaction action; memset(&action,0,sizeof(action));
	sigemptyset(&action.sa_mask);
	action.sa_flags = SA_SIGINFO|SA_RESTART;
	action.sa_sigaction = FileIOOSX::_asyncAIOCompletion;
	if (sigaction(SIGPIAIO, &action, NULL)!=0) report(MSG_CRIT,"Cannot install AIO signal handler (%d)\n",errno);
}

inline off64_t getFileSz(int fd)
{
	struct stat fileStats;		
	return fstat(fd,&fileStats)==0?fileStats.st_size:0;
}

RC FileIOOSX::open(FileID& fid,const char *fname,const char *dir,ulong flags)
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

	static struct flock flck; flck.l_type=F_WRLCK; flck.l_whence=SEEK_SET;
	int f_flags = (flagsFS&FS_DIRECT)!=0 && (flags&FIO_TEMP)==0?0:0 |O_RDWR |(flags&(FIO_TEMP|FIO_CREATE)?flags&FIO_NEW?O_CREAT|O_EXCL|O_TRUNC:O_CREAT:0); 
	fd = ::open64(fname, f_flags, S_IRUSR|S_IWUSR|S_IRGRP );
	//The following line - setting F_NOCACHE - is similar to `O_DIRECT`...
	if(fd==INVALID_FD || fcntl(fd, F_NOCACHE, 1) != 0)	rc=convCode(errno);

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

off64_t FileIOOSX::getFileSize(FileID fid) const
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

size_t FileIOOSX::getFileName(FileID fid,char buf[],size_t lbuf) const
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

RC FileIOOSX::close(FileID fid)
{
	RWLockP rw(&lock,RW_X_LOCK); RC rc = RC_OK;
	if (fid>=xSlotTab) rc = RC_NOTFOUND;
	else if (slotTab[fid].isOpen()) {
		slotTab[fid].close();
	}
	return RC_OK;
}

void FileIOOSX::closeAll(FileID start)
{
	RWLockP rw(&lock,RW_X_LOCK); 
	for (FileID fid=start; fid<xSlotTab; fid++) if (slotTab[fid].isOpen()) {
		slotTab[fid].close();
	}
}


RC FileIOOSX::growFile(FileID file, off64_t newSize)
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


RC FileIOOSX::listIO(int mode,int nent,iodesc* const* pcbs)
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
        
        adescs[i]->aio_nbytes=pcbs[i]->aio_nbytes;
		adescs[i]->aio_buf=pcbs[i]->aio_buf;
		adescs[i]->aio_offset=pcbs[i]->aio_offset;        
        // cout << "\n!!!DEBUG===> FileIOOSX::listIO :: prepare aio bytes: " << adescs[i]->aio_nbytes << " mode: " << mode << "\n";
        if (mode==LIO_WAIT)
		    adescs[i]->aio_sigevent.sigev_notify = SIGEV_NONE;
		else{
           // cout << "\n!!!DEBUG===> FileIOOSX::listIO :: prepare Async sig! \n";
            adescs[i]->aio_sigevent.sigev_notify			= SIGEV_SIGNAL;
            adescs[i]->aio_sigevent.sigev_signo			    = SIGPIAIO;
            adescs[i]->aio_sigevent.sigev_value.sival_ptr	= (void *)pcbs[i];
            pcbs[i]->aio_ptr[pcbs[i]->aio_ptrpos++]	        = adescs[i];      //aiocb64*...
            pcbs[i]->aio_ptr[pcbs[i]->aio_ptrpos++]	        = this;
            
            adescs[i]->aio_sigevent.sigev_notify_attributes = NULL;
        }

#if defined(_POSIX_PRIORITIZED_IO) && defined(_POSIX_PRIORITY_SCHEDULING)
//TODO?		if (mode==LIO_NOWAIT) adescs[i]->aio_reqprio=AIO_PRIO_DELTA_MAX;
#endif
	}
	lock.unlock();

    sigset_t omask; 
    
	if (mode==LIO_WAIT) pthread_sigmask(SIG_BLOCK,&sigSIO,&omask);

	for (i=0; i<nent; i++) if (pcbs[i]!=NULL && adescs[i]!=NULL) {
        
        if (adescs[i]->aio_lio_opcode==LIO_NOP) {asyncIOCallback(pcbs[i]); freeAio64.dealloc(adescs[i]); adescs[i]=NULL; continue;}
        
        if (mode==LIO_WAIT) {
            
            if( !(--FileIOOSX::lioAQueue.aioCnt)) //returns 0, if AIO_LIMIT_MAX is reached...
					threadYield(); 
			rc = aio_listIO(mode, &adescs[i], 1); //will return after physical IO is done
            ++FileIOOSX::lioAQueue.aioCnt;
            
            if( RC_OK != rc) return rc;
            
        }else if ( mode == LIO_NOWAIT) {
           // Remembering the pointer for later processing within AsyncReqQ::asyncOSXFinalize(...)
           // OSX specific since the later cannot provide the aio pointer within signal context.
		   // OSX allows only the limitted number of aio(s), submitted at the same time to the kernel;
		   // The ring queue below is taking care about that
           FileIOOSX::lioAQueue.add((void *)pcbs[i]);
		 }
    }
    return rc;
}
/*
* The function is performing 'SYNC' IO, one aio only
* (the name  - and parameters - are enherited from Linux, and is not perfect)
*/ 
RC FileIOOSX::aio_listIO( int mode, aiocb64 **adescs, int nent){
    RC rc = RC_OK;
    int i =0; int le;  
    
	if (NULL == adescs[i])  return RC_INVPARAM; //Have to find later a better code...    
   
    typedef enum _syncopstate{
		OP_SUBMIT,
		SUSPEND,
		AIO_AFTER_SUSPEND,
		CLEAN,
		AIO_RW_ERROR
	} SyncOpState;
	
	SyncOpState state;
		
    for(state = OP_SUBMIT ; ;){
		switch (state){
			case OP_SUBMIT:
				le = (adescs[i]->aio_lio_opcode==LIO_WRITE?aio_write(adescs[i]):aio_read(adescs[i]));
				if(!le) { state = SUSPEND; }
				else    { state = AIO_RW_ERROR; }
			break;
			case SUSPEND:
			    le = aio_suspend(adescs, nent, NULL);
				if (0 == le ){ state = AIO_AFTER_SUSPEND; break; }
				
				if ((EAGAIN == errno) || ( EINTR == errno)) break;  // to see those here is normal... just call aio_suspend() again            
                report(MSG_DEBUG, "!!!aio_susend()::erno: %d  nent: %d mode: %d\n", errno, nent, mode);
			break; 
			case AIO_AFTER_SUSPEND:
			    le = aio_error(adescs[i]);
				if(le == 0){ state = CLEAN; break; }
			    if(le > 0){
					int e = errno;
                    switch (e){
                        case EINPROGRESS: 
                            //The request not completed yet...
                            state = SUSPEND;
                            break;
                        case EAGAIN:
                            state = OP_SUBMIT;                            
                            break;
                        case ENOSYS:
                            report(MSG_DEBUG, "!Major problem: Systems reports - no aio calls!!!\n");
                            break;
                        default:
                            ::sched_yield();
                            break;
                    }
				}else{
					//???
				}
			break;
			case CLEAN:
			    le = aio_error(adescs[i]);
			    aio_return(adescs[i]);           //final aio... call, in order to free system resources... see 'man aio_return()' for details.
                freeAio64.dealloc(adescs[i]);
                rc=convCode(le); 
				return rc;
			break;
			case AIO_RW_ERROR:
			    le = aio_error(adescs[i]);
			    if ( 0 == le) { state = SUSPEND; break; } // aio done, proceed further
				if ( EINPROGRESS == le) break;            // request is in progress, just wait?
				if ( EAGAIN == le){
				    //aio_return(adescs[i]);   //??? shousd I do it?
				    state = OP_SUBMIT;
				    break;	
				}
				if(-1 == le){
					switch (errno){
						case EINVAL :
						   //  report(MSG_DEBUG, "EINVAL.cn := %d  le := %d \n", FileIOOSX::lioAQueue.aioCnt.cn, le);
						case EAGAIN :
							//aio_return(adescs[i]);   //??? shousd I do it?
						    state = OP_SUBMIT;
						    ::sched_yield();
						break;
					}
               	}
				if (ENOSYS == le ){
                    report(MSG_DEBUG,"!Major problem: Systems reports - no aio calls!!!\n");
					break;
				}
			break;
	}
  }
}

bool FileIOOSX::asyncIOEnabled() const
{	
	return (flagsFS&FS_DIRECT)!=0;
}

#define TESTFILENAME	"chaosdb.tst"

void FileIOOSX::setFlagsFS()
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

RC FileIOOSX::deleteFile(const char *fname)
{
	if (unlink(fname)==-1)return convCode(errno);
	return RC_OK;
}


#include <dirent.h>
#include <fnmatch.h>

void FileIOOSX::deleteLogFiles(ulong maxFile,const char *lDir,bool fArchived)
{
	deleteLogFiles(MVSTOREPREFIX"*"LOGFILESUFFIX,maxFile,lDir,fArchived);
}

void FileIOOSX::deleteLogFiles(const char *mask,ulong maxFile,const char *lDir,bool fArchived)
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
	}else{
#ifdef _DEBUG
        report(MSG_DEBUG, "Error ::: FileIOOSX::deleteLogFiles fails to open dir!\n");
#endif    
    }
}
};

IStoreIO *getStoreIO() 
{ 
	try {return new(STORE_HEAP) FileIOOSX;}
	catch (...) {report(MSG_ERROR,"Exception in getStoreIO\n"); return NULL;}
}




#endif
