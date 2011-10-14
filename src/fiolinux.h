/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov, Andrew Skowronski, Michael Andronov 2004 - 2010

**************************************************************************************/

#ifndef _FIOLINUX_H_
#define _FIOLINUX_H_

#ifdef _LINUX
#ifndef Darwin

#include "storeio.h"
#include "sync.h"

#define INVALID_FD (-1)
#define	FS_DIRECT			0x0001

namespace MVStoreKernel
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

	class FileIOLinux : public IStoreIO
	{
		mutable RWLock		lock;
		FileDescLinux		*slotTab;
		int					xSlotTab;
		ulong				flagsFS;
		void				(*asyncIOCallback)(iodesc*);
		static	FreeQ<>		freeAio64;
#ifndef STORE_AIO_THREAD
		static	FreeQ<>		freeIORequests;
#endif

	public:
		FileIOLinux();
		~FileIOLinux();

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
		bool	asyncIOEnabled() const ;
		RC		deleteFile(const char *fname);
		void	deleteLogFiles(ulong maxFile,const char *lDir,bool fArchived);
		void	destroy() { this->~FileIOLinux(); }
#ifdef STORE_AIO_THREAD
		static	void _asyncIOCompletion(sigval_t val);
#else
		static	void _asyncAIOCompletion(int signal, siginfo_t *info, void *uap);
		static	void _asyncSIOCompletion(int signal, siginfo_t *info, void *uap);
		friend	class	IOCompletionRequest;
#endif
	private:
		void	setFlagsFS();
		void	deleteLogFiles(const char * mask,ulong maxFile,const char *lDir,bool fArchived);
	};
} ;

#endif
#endif
#endif
