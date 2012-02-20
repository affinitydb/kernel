/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov, Andrew Skowronski, Michael Andronov 2004 - 2010

**************************************************************************************/

#ifndef _FIOOSX_H_
#define _FIOOSX_H_
#ifdef Darwin       // jst a reminder that the file will be used only on OSX

#include "storeio.h"
#include "sync.h"

#define INVALID_FD (-1)

#include <aio.h>
typedef union sigval sigval_t;

namespace AfyKernel
{
	struct FileDescLinux
	{
		char				*filePath;
		int					osFile;
		volatile off64_t	fileSize;
		bool				fTemp;
		volatile bool		fSize;

		void init()
		{
			osFile=INVALID_FD;
			fileSize=0;
			filePath=NULL;
			fTemp=false;
		}
		bool isOpen() const { return osFile!=INVALID_FD; }
		void close()
		{
			if (osFile!=INVALID_FD) {
				::close(osFile); osFile=INVALID_FD;
			}
			if (filePath!=NULL) {
				if (fTemp) unlink(filePath);
				free(filePath,STORE_HEAP); filePath=NULL;
			}
		}		
	};

    
    /*
     * The element of the ring buffer, issued for remembering aiocb
     */
    class AIOElt : public DLList
    {	
	 public:
     typedef enum _state {
			NOT_IN_ASYNC_QUEUE = 13,
			QUEUED,
			DONE
			}AIORqStatus;
   
        AIOElt( void *p): state(NOT_IN_ASYNC_QUEUE), payload(p){};
        inline void * getPayload(){return payload;}
		inline AIORqStatus getState() { return state;}
		inline AIORqStatus setQueued() { return state = QUEUED; }
		inline AIORqStatus setDone() { return state = DONE; }
		inline AIORqStatus setNotQueued() { return state = NOT_IN_ASYNC_QUEUE; }
	  
	 private:	
	    AIORqStatus state;  
	    void * payload;	
    };

    /* Used for counting available AIO -
    * in other words, preventing more then AIO_LISTIO_MAX requests be submitted at a time
    */
    struct AioCnt {
        long mx;
		long mn;
		volatile long cn;

		AioCnt(long mx = (AIO_LISTIO_MAX), long mi = 0 ): mx(mx), mn(mi), cn(mx) {}
        ~AioCnt()	{}
        void		detach() {}
        bool operator --() {
		    long tmp;
			for( tmp = cn; mn != tmp; tmp = cn){
				assert( tmp > 0);
				if(__sync_bool_compare_and_swap(&cn, tmp, tmp-1))
					return true;
			}
			return false;
	    }
        bool operator ++() {
	        long tmp; 
		    for(tmp = cn; mx != tmp; tmp = cn){
					if(__sync_bool_compare_and_swap(&cn, tmp, tmp+1))
						return true;
			}
			return false;
	    }		
        operator long() const{ return cn;} 
    };
    
 	/* OSX signal notification on aio completion does not provide 'signal value'. 
     * The class below is the utility to track the queue of the aiocbs...
     */
    class AsyncReqQ {
      public: 
        static  FreeQ<> lioReqs;
        mutable	RWLock lock;
        AIOElt * q;

        AsyncReqQ() {
            pthread_create(&thSigProc, NULL, asyncOSXFinalize, NULL);
			nSig = 0;
        };

        AIOElt * add( void * p);
		void * remove();
        friend	class IOCompletionRequest;
        static void * asyncOSXFinalize(void *);
        pthread_t thSigProc;
        SemData sigSem;
        AioCnt  aioCnt;
		SharedCounter nSig;
    };   
  
    /* Affinity 'standard' interface 
     */
	class FileIOOSX : public IStoreIO
	{
		mutable RWLock		lock;
		FileDescLinux		*slotTab;
		int					xSlotTab;
		
    public:
        
#ifndef STORE_AIO_THREAD
		static	FreeQ<>		freeIORequests;
        static  FreeQ<>     asyncReqs;
#endif

		FileIOOSX();
		~FileIOOSX();
        
        void				(*asyncIOCallback)(iodesc*);
		static	FreeQ<>		freeAio64;

		void	init(void (*asyncCallback)(iodesc*)) ;
		const char *getType() const { return "fiolinux"; }
		RC		setParam(const char * /*key*/, const char * /*value*/, bool /*broadcast*/ ) { return RC_FALSE; }
		RC		open(FileID& fid,const char *fname,const char *dir,ulong flags);
		off64_t	getFileSize(FileID fid) const;
		size_t  getFileName(FileID fid,char buf[],size_t lbuf) const;
		RC		close(FileID fid);
		void	closeAll(FileID start);
		RC      growFile(FileID file, off64_t newsize);
		RC		listIO(int mode,int nent,iodesc* const* pcbs);
        RC      aio_listIO( int mode, aiocb **adescs, int nent);
        
		RC		deleteFile(const char *fname);
		void	deleteLogFiles(ulong maxFile,const char *lDir,bool fArchived);
		void	destroy() { this->~FileIOOSX(); }

		static	void _asyncAIOCompletion(int signal, siginfo_t *info, void *uap);
		static	void _asyncSIOCompletion(int signal, siginfo_t *info, void *uap);
		friend	class IOCompletionRequest;
        
	private:
		void	deleteLogFiles(const char * mask,ulong maxFile,const char *lDir,bool fArchived);

    public:
        static  AsyncReqQ   lioAQueue;  //async queue... 

	};	
	
} ;
#endif
#endif
 
