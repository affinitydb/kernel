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

/**
 * Windows specific file i/o
 */
#ifndef _FIOWIN_H_
#define _FIOWIN_H_

#ifdef WIN32

#include "fio.h"
#include "afysync.h"

namespace AfyKernel
{

enum LIOOP {LIO_READ, LIO_WRITE, LIO_NOP};
enum LIOMODE {LIO_WAIT, LIO_NOWAIT};

/**
 * Windows specific file i/o manager
 * controls file open, close, delete opeartions
 */
class FileMgr : public GFileMgr
{
	static struct CompletionPort {
		static DWORD WINAPI _asyncIOCompletion(void *param) {((FileMgr*)param)->asyncIOCompletion(); return 0;}
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
	}				CP;
	static	FreeQ	freeIODesc;
public:
	FileMgr(class StoreCtx *ct,int maxOpenFiles,const char *ldDir);
	RC		open(FileID& fid,const char *fname,unsigned flags=0);
	RC		listIO(int mode,int nent,myaio* const* pcbs,bool fSync=false);
	void	asyncIOCompletion();
};

};


#endif
#endif
