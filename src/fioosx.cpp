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

Written by Mark Venguerov, Michael Andronov 2004 - 2012

**************************************************************************************/

#ifdef __APPLE__
/* from the manual page of Xcode Tools version 4.0:
   ...
   The fstat64, lstat64 and stat64 routines are equivalent to their corresponding non-64-suffixed routine,
    when 64-bit inodes are in effect.  They were added before there was support for the symbol variants,
    and so are now deprecated.  Instead of using these, set the _DARWIN_USE_64_BIT_INODE macro before
    including header files to force 64-bit inode support.
*/
#include "fioosx.h"
#include "session.h"
#include "startup.h"
#include "request.h"

#include <limits.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/signal.h> 

using namespace AfyKernel;

/* __APPLE__ specific ... */
LIFO AsyncReqQ::lioReqs;
AsyncReqQ FileMgr::lioAQueue; //track for outstanding I/O(s).
 
namespace AfyKernel
{

  
/* Uner OSX, within the signal, the info does not provide signal value information. 
*   The signal is only setting the semaphore, 
*   and the real processing is done within 
*   void * AsyncReqQ::asyncOSXFinalize(void *pp)
*/
void FileMgr::_asyncAIOCompletion(int sig, siginfo_t *info, void *uap)
{
    ++FileMgr::lioAQueue.nSig; 
    FileMgr::lioAQueue.sigSem.wakeup();
}

/*
* Submitting a new aio request to the ring buffer
*/
AIOElt * AsyncReqQ::add( void * p){
    AIOElt *elt=NULL;
    lock.lock(RW_X_LOCK);
    //incapsulating the 'payload' into DLList element...
    void *rq=lioReqs.alloc(sizeof(AIOElt));
	if (rq!=NULL){
        elt = new(rq) AIOElt(p);
		elt->setNotQueued();
	   if( NULL == FileMgr::lioAQueue.q){
            FileMgr::lioAQueue.q = elt;
        }else{
            FileMgr::lioAQueue.q->insertLast(elt);
        }
        ++FileMgr::lioAQueue.nSig; 
		FileMgr::lioAQueue.sigSem.wakeup();
    }
    lock.unlock();
    return elt; 
};

/* 
 * Unchain and delete the element with the processed aio ... 
 */
AIOElt * unchain(AIOElt *elt){
    
    FileMgr::lioAQueue.lock.lock(RW_X_LOCK);	
	AIOElt * rc =  (AIOElt *)elt->prev;
	if(AIOElt::DONE == elt->getState() ){
		rc =  (AIOElt *)elt->prev;
    	if( rc == elt ){
			FileMgr::lioAQueue.q = NULL;  rc = NULL;
	    }else if ( FileMgr::lioAQueue.q == elt){	   
			FileMgr::lioAQueue.q = (AIOElt *)elt->prev;
		}
		elt->remove();
	    AsyncReqQ::lioReqs.dealloc(elt);
    }
    FileMgr::lioAQueue.lock.unlock();
    return rc;
};

static sigset_t sigSIO;

LIFO FileMgr::freeIORequests;
class IOCompletionRequest : public Request
{
       myaio			*const	aio;
   public:
       IOCompletionRequest(myaio *ai) : aio(ai) {}
       void process() {GFileMgr::asyncIOCallback(aio,true);}
       void destroy() {FileMgr::freeIORequests.dealloc(this);}
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
 *  void * FileMgr::asyncOSXFinalize(void *pp)
 *  pthread function, representing the `worker` thread for the async. operation
 *   completion.
 **/
void * AsyncReqQ::asyncOSXFinalize(void *pp)
{
	SharedCounter u,w;
	  
   while(true){
	   bool repeat;
	   FileMgr::lioAQueue.sigSem.wait();
	   
       do{
		   repeat = false; // set in case EAGAIN received during aio to OSX submition
		 
		   if (NULL != FileMgr::lioAQueue.q){  
		     for(AIOElt * pel = FileMgr::lioAQueue.q;  ; pel = (AIOElt *)pel->next){
				  bool  bunChn = false ;  
			   
		          // Getting access to aiocb, to check current status... 
				  myaio *aio=(myaio *)pel->getPayload();
		    
		          if( AIOElt::QUEUED  == pel->getState()){
			            	//Checking status:  the aio may be ready, in progress, or an error may happen... 
			                int st = aio_error(aio);
                            if( 0 == st ){  //ok, I/O completed normally...
			                   if (aio!=NULL) {
		                            void *rq=FileMgr::freeIORequests.alloc(sizeof(IOCompletionRequest));
		                            if (rq!=NULL) RequestQueue::postRequest(new(rq) IOCompletionRequest(aio),NULL,RQ_IO);
		                        } else {
		                            report(MSG_DEBUG, "asyncFinalize:: stratge 1 ?????????????? errno: %d \n",errno);  
			                   }
			                   aio_return(aio);	                    
						       ++FileMgr::lioAQueue.aioCnt; 
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
		               		if( !(--FileMgr::lioAQueue.aioCnt)){
			                  //???
							}else {
								    pel->setQueued(); 
								    int era = (aio->aio_lio_opcode==LIO_WRITE?aio_write64(aio):aio_read64(aio));
									int e = errno; 
						            aio_error(aio);
					                if( era < 0 ){              
						                long t = FileMgr::lioAQueue.aioCnt;
		               	
								        switch (e){
						                    case EINVAL:
						                        report(MSG_DEBUG,"asyncFinalize:: I/O EINVAL %d %d\n", e,t);
						                        //??????
						                        break;
						                    case EAGAIN:                
						                        //report(MSG_DEBUG, "asyncFinalize:: I/O EAGAIN  %d %d\n", e,t);
					                            pel->setNotQueued();
					                            ++FileMgr::lioAQueue.aioCnt;
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

FileMgr::FileMgr(StoreCtx *ct,int maxOpenFiles,const char *ldDir) : GFileMgr(ct,maxOpenFiles,ldDir)
{
	sigemptyset(&sigSIO); sigaddset(&sigSIO,SIGAFYSIO);

	struct sigaction action; memset(&action,0,sizeof(action));
	sigemptyset(&action.sa_mask);
	action.sa_flags = SA_SIGINFO|SA_RESTART;
	action.sa_sigaction = FileMgr::_asyncAIOCompletion;
	if (sigaction(SIGAFYAIO, &action, NULL)!=0) report(MSG_CRIT,"Cannot install AIO signal handler (%d)\n",errno);
}

inline off64_t getFileSz(int fd)
{
	struct stat fileStats;
	for(;;){
		int lrc = fstat(fd,&fileStats);
		if ( lrc == 0 )
			return fileStats.st_size;
		else{
			int ler = errno;
			switch(ler){
				case EIO:
				report(MSG_DEBUG, "!!!getFileSz()::EIO: %d  fd: %d \n", errno, fd);
				::sched_yield();
				break;
				
				case EBADF:
				case EFAULT:
				default:
				report(MSG_DEBUG, "!!!getFileSz()::ERR: %d  fd: %d \n", errno, fd);
				return 0;
			}
	    }
    }
}

RC FileMgr::open(FileID& fid,const char *fname,unsigned flags)
{
	HANDLE fd; off64_t fileSize=0; RC rc = RC_OK;
	if ((fname==NULL || *fname=='\0') && (flags&FIO_TEMP)==0) return RC_INVPARAM;

	const char *dir=ctx->getDirectory(); bool fdel=false;
	if ((flags&FIO_TEMP)!=0) {
		char *p=(char*)ctx->malloc((dir!=NULL?strlen(dir):2)+sizeof(STOREPREFIX)+6+1); 
		if (p==NULL) return RC_NOMEM; strcpy(p,dir!=NULL?dir:"./"); strcat(p,STOREPREFIX);
		fname=p; p+=strlen(p); memset(p,'X',6); p[6]='\0'; p=mktemp((char*)fname);
		if (p==NULL||*p=='\0') {ctx->free((char*)fname); return RC_NOMEM;}
		fdel=true;
	} else if (dir!=NULL && !strchr(fname,'/')) {
		char *p=(char*)ctx->malloc(strlen(dir)+strlen(fname)+1); if (p==NULL) return RC_NOMEM;
		strcpy(p,dir); strcat(p,fname); fname=p; fdel=true;
	}

	lock.lock(RW_X_LOCK);
	if (fid==INVALID_FILEID) {
		for(fid=0;fid<xSlotTab;fid++){
			if (!slotTab[fid].isOpen()) break;
		}
	}
	if (fid>=xSlotTab) {lock.unlock(); return RC_NOMEM;}
	if (slotTab[fid].isOpen()) { 
		if ((flags&FIO_REPLACE)==0)	{lock.unlock(); return RC_ALREADYEXISTS;}
		slotTab[fid].close();
	}
	
	char fullbuf[PATH_MAX+1]; 

	static struct flock flck; flck.l_type=F_WRLCK; flck.l_whence=SEEK_SET;	
	fd = ::open64(fname, (O_RDWR|(flags&(FIO_TEMP|FIO_CREATE)?flags&FIO_NEW?O_CREAT|O_EXCL|O_TRUNC:O_CREAT:0)), S_IRUSR|S_IWUSR|S_IRGRP );
	
	//The following line - setting F_NOCACHE - is similar to `O_DIRECT`...
	if(((flags&FIO_TEMP)==0) && (fd==INVALID_FD || fcntl(fd, F_NOCACHE, 1) != 0)) rc=convCode(errno);

	if(fd==INVALID_FD || fcntl(fd,F_SETLK,(struct flock*)&flck)!=0) rc=convCode(errno);
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
	return rc;
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
	             /*OSX doesn't fail for reads beyond EOF*/
				rc=pcbs[i]->aio_rc=RC_EOF; pcbs[i]->aio_lio_opcode=LIO_NOP;
			} else { 
				pcbs[i]->aio_fildes=slotTab[fid].osFile; 
				if ((off64_t)(pcbs[i]->aio_nbytes+pcbs[i]->aio_offset)>slotTab[fid].fileSize) slotTab[fid].fSize=false;
			}
        
	        // cout << "\n!!!DEBUG===> FileMgr::listIO :: prepare aio bytes: " << pcbs[i]->aio_nbytes << " mode: " << mode << "\n";
		    if (mode==LIO_WAIT)
			    pcbs[i]->aio_sigevent.sigev_notify = SIGEV_NONE;
			else{
		       // cout << "\n!!!DEBUG===> FileMgr::listIO :: prepare Async sig! \n";
			    pcbs[i]->aio_sigevent.sigev_notify			= SIGEV_SIGNAL;
				pcbs[i]->aio_sigevent.sigev_signo			    = SIGAFYAIO;
	            pcbs[i]->aio_sigevent.sigev_value.sival_ptr	= (void *)pcbs[i];
				pcbs[i]->aio_sigevent.sigev_notify_attributes = NULL;
	        }

#if defined(_POSIX_PRIORITIZED_IO) && defined(_POSIX_PRIORITY_SCHEDULING)
//TODO?			if (mode==LIO_NOWAIT) pcbs[i]->aio_reqprio=AIO_PRIO_DELTA_MAX;
#endif
		}
		lock.unlock();

		sigset_t omask; 
    
		if (mode==LIO_WAIT) pthread_sigmask(SIG_BLOCK,&sigSIO,&omask);

		for (i=0; i<nent; i++) if (pcbs[i]!=NULL) {
			if (pcbs[i]->aio_lio_opcode==LIO_NOP) {asyncIOCallback(pcbs[i]); continue;}
        
	        if (mode==LIO_WAIT) {
	            if( !(--FileMgr::lioAQueue.aioCnt)) //returns 0, if AIO_LIMIT_MAX is reached...
						threadYield(); 
				rc = aio_listIO(mode, &pcbs[i], 1); //will return after physical IO is done
				++FileMgr::lioAQueue.aioCnt;
            
	            if( RC_OK != rc) return rc;
            
		    } else if (mode==LIO_NOWAIT) {
				// Remembering the pointer for later processing within AsyncReqQ::asyncOSXFinalize(...)
				// OSX specific since the later cannot provide the aio pointer within signal context.
				// OSX allows only the limitted number of aio(s), submitted at the same time to the kernel;
				// The ring queue below is taking care about that
	           FileMgr::lioAQueue.add((void *)pcbs[i]);
			 }
		}
	} catch (RC rc2) {
		rc=rc2;
		for (i=0; i<nent; i++) {
			pcbs[i]->aio_rc=rc2; if (mode!=LIO_WAIT) asyncIOCallback(pcbs[i]);
		}
	}
    return rc;
}
/*
* The function is performing 'SYNC' IO, one aio only
* (the name  - and parameters - are enherited from Linux, and is not perfect)
*/ 
RC FileMgr::aio_listIO( int mode, myaio *const *pcbs, int nent)
{
    RC rc = RC_OK;
    int i =0; int le;  
    
	if (NULL == pcbs[i])  return RC_INVPARAM; //Have to find later a better code...    
   
    typedef enum _syncopstate{
		OP_SUBMIT,
		SUSPEND,
		AIO_AFTER_SUSPEND,
		CLEAN,
		AIO_RW_ERROR
	} SyncOpState;
	
	SyncOpState state;
	struct timespec tout;
		
    for(state = OP_SUBMIT ; ;){
		switch (state){
			case OP_SUBMIT:
				le = (pcbs[i]->aio_lio_opcode==LIO_WRITE?aio_write(pcbs[i]):aio_read(pcbs[i]));
				if(!le) { state = SUSPEND; }
				else if ( errno != EAGAIN) { state = AIO_RW_ERROR; } 
			break;
			case SUSPEND:
			    tout.tv_sec = 5; tout.tv_nsec = 0;
			    le = aio_suspend((aiocb* const *)pcbs, nent, &tout);
				if (0 == le ){ state = AIO_AFTER_SUSPEND; break; }
				
				if ((EAGAIN == errno) || ( EINTR == errno)) break;  // to see those here is normal... just call aio_suspend() again            
                report(MSG_DEBUG, "!!!aio_susend()::erno: %d  nent: %d mode: %d\n", errno, nent, mode);
                state = OP_SUBMIT;
			break; 
			case AIO_AFTER_SUSPEND:
			    le = aio_error(pcbs[i]);
				if(le > 0){
					int e = errno;
                    switch (e){
                        case EINPROGRESS: 
                            //The request not completed yet...
                            report(MSG_DEBUG,"!AFTER_SUSPEND::SUSPEND!\n");
                            state = SUSPEND;
                            break;
                        case EAGAIN:
                            report(MSG_DEBUG,"!AFTER_SUSPEND::EAGAIN!\n");
                            state = OP_SUBMIT;                            
                            break;
                        case ENOSYS:
                            report(MSG_DEBUG, "!Major problem: Systems reports - no aio calls!!!\n");
                            break;
                        default:
                            ::sched_yield();
                            break;
                    }
				}
				// le == 0  -- successul return;  clean;
				// le < 0   -- error, so still clean... 
				state = CLEAN; break;
			break;
			case CLEAN:
			    le = aio_error(pcbs[i]);
			    aio_return(pcbs[i]);           //final aio... call, in order to free system resources... see 'man aio_return()' for details.
                rc=convCode(le); 
				return rc;
			break;
			case AIO_RW_ERROR:
			    le = aio_error(pcbs[i]);
			    if ( 0 == le) { 
				     state = SUSPEND; break; 
				} // aio done, proceed further
				if ( EINPROGRESS == le){
					break;            // request is in progress, just wait?
				} 
				if ( EAGAIN == le){
					//aio_return(pcbs[i]);   //??? shousd I do it?
				    state = OP_SUBMIT;
				    break;	
				}
				if(-1 == le){
					switch (errno){
						case EINVAL :
						case EAGAIN :
							//aio_return(pcbs[i]);   //??? shousd I do it?
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

RC GFileMgr::deleteFile(const char *fname)
{
	if (unlink(fname)==-1)return convCode(errno);
	return RC_OK;
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
	} else {
        report(MSG_DEBUG, "Error ::: FileMgr::deleteLogFiles fails to open dir!\n");
    }
}

#include <dlfcn.h>

RC GFileMgr::loadExt(const char *path,size_t l,Session *ses,const Value *pars,unsigned nPars,bool fNew)
{
	if (path==NULL || l==0) return RC_INVPARAM;

	RC rc=RC_OK;
	size_t ld=loadDir!=NULL && !memchr(path,'/',l)?strlen(loadDir):0,le=l<7||memcmp(path+l-6,".dylib",6)!=0?6:0,lp=0,sht=0;
	for (const char *pp=path+l;;) {if (*--pp=='/') sht=++pp-path; else if (pp!=path) continue; if (sht+3>l || pp[0]!='l' || pp[1]!='i' || pp[2]!='b') lp=3; break;}
	char *p=(char*)((StoreCtx*)ctx)->malloc(ld+lp+l+le+2); if (p==NULL) return RC_NOMEM;
	if (ld!=0) {memcpy(p,loadDir,ld); if (p[ld-1]!='/') p[ld++]='/';}
	if (lp!=0) {memcpy(p+ld,path,sht); memcpy(p+ld+sht,"lib",3); memcpy(p+ld+sht+3,path+sht,l-sht);} else memcpy(p+ld,path,l);
	if (le!=0) memcpy(p+ld+lp+l,".dylib",6); p[ld+lp+l+le]='\0';

	report(MSG_INFO,"Loading %s...\n",p);

#ifdef AFFINITY_STATIC_LINK
	rc=((StoreCtx*)ctx)->initStaticService(path+sht+3-lp,l-sht+le-6,ses,pars,nPars,fNew);
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

};


#endif
