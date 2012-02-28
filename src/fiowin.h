/**************************************************************************************

Copyright © 2004-2012 VMware, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,  WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.

Written by Mark Venguerov and Andrew Skowronski 2004-2012

**************************************************************************************/

/**
 * Windows specific file i/o
 */
#ifndef _FIOWIN_H_
#define _FIOWIN_H_

#ifdef WIN32

#include "storeio.h"
#include "sync.h"

namespace AfyKernel
{
	/**
	 * file descriptor
	 */
	struct FileDescWin
	{
		char				*filePath;
		HANDLE				osFile;
		volatile off64_t	fileSize;
		volatile bool		fSize;
		void init() 
		{
			filePath=NULL;		
			osFile=INVALID_HANDLE_VALUE;
			fileSize=0;
			fSize=false;
		}
		bool isOpen() const { return osFile!=INVALID_HANDLE_VALUE; }

		void close()
		{
			if (osFile!=INVALID_HANDLE_VALUE) {
				::CloseHandle(osFile); osFile=INVALID_HANDLE_VALUE; 
			}
			if (filePath!=NULL) {
				free(filePath,STORE_HEAP); filePath=NULL;
			}
		}
	};

	/**
	 * Windows specific file i/o manager
	 * controls file open, close, delete opeartions
	 */
	class FileIOWin : public IStoreIO
	{
		mutable RWLock		lock;
		FileDescWin			*slotTab;
		int					xSlotTab;
		void				(*asyncIOCallback)(iodesc*);

		static struct CompletionPort {
			static DWORD WINAPI _asyncIOCompletion(void *param) {((FileIOWin*)param)->asyncIOCompletion(); return 0;}
			HANDLE completionPort;
			CompletionPort() {
				completionPort = CreateIoCompletionPort(INVALID_HANDLE_VALUE,NULL,(ULONG_PTR)0,0);
				if (completionPort!=NULL) {
					HTHREAD thread; int nThreads = getNProcessors()*2; if (nThreads==0) nThreads=2;
					while (--nThreads>=0) createThread(_asyncIOCompletion,this,thread);
				}
			}
			~CompletionPort() {
				if (completionPort!=NULL) {
					PostQueuedCompletionStatus(completionPort,0,(ULONG_PTR)1,NULL);
					CloseHandle(completionPort);
				}
			}
		}					CP;
		static	FreeQ<>		freeIODesc;
	public:
		void	asyncIOCompletion();
		FileIOWin();
		~FileIOWin();

		void	init(void (*asyncCallback)(iodesc*)) ;
		const char *getType() const { return "fiowin"; }
		RC		setParam(const char * /*key*/, const char * /*value*/, bool /*broadcast*/ ) { return RC_FALSE; }
		RC		open(FileID& fid,const char *fname,const char *dir,ulong flags);
		off64_t	getFileSize(FileID fid) const;
		RC		truncate(FileID fid,off64_t offset);
		size_t	getFileName(FileID fid,char buf[],size_t lbuf) const;
		RC		close(FileID fid);
		void	closeAll(FileID start);
		RC      growFile(FileID file, off64_t newSize);
		RC		listIO(int mode,int nent,iodesc* const* pcbs);
		RC		deleteFile(const char *fname);
		void	deleteLogFiles(ulong maxFile,const char *lDir,bool fArchived);
		void	destroy() { this->~FileIOWin(); }

	private:
		void	deleteLogFiles(const char *mask,ulong maxFile,const char *lDir,bool fArchived);
	};
};


#endif
#endif