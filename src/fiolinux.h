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

/**
 * file i/o for Linux platforms
 */
#ifndef _FIOLINUX_H_
#define _FIOLINUX_H_

#if (defined(_LINUX) || defined(ANDROID)) && !defined(__APPLE__)

#include "fio.h"
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>

#define	SIGAFYAIO	(SIGRTMIN+6)
#define	SIGAFYSIO	(SIGRTMIN+7)

#define INVALID_FD	INVALID_HANDLE_VALUE

namespace AfyKernel
{
/**
 * linux specific file i/o manager
 * controls file open, close, delete opeartions
 */

class FileMgr : public GFileMgr
{
#if !defined(STORE_AIO_THREAD) || defined(ANDROID)
	static	FreeQ		freeIORequests;
#endif

public:
	FileMgr(class StoreCtx *ct,int maxOpenFiles,const char *ldDir);
	RC	open(FileID& fid,const char *fname,unsigned flags=0);
	RC	listIO(int mode,int nent,myaio* const* pcbs,bool fSync=false);
#ifdef ANDROID
	RC  doSyncIo(myaio& aio);
	friend class GenericAIORequest;
#endif
#ifdef STORE_AIO_THREAD
	static	void _asyncIOCompletion(sigval_t val);
#else
	static	void _asyncAIOCompletion(int signal, siginfo_t *info, void *uap);
	static	void _asyncSIOCompletion(int signal, siginfo_t *info, void *uap);
	friend	class	IOCompletionRequest;
#endif
};

};

#endif
#endif
