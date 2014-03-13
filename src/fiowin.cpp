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

Written by Mark Venguerov 2004-2012

**************************************************************************************/

#ifdef WIN32
#include "fiowin.h"
#include "session.h"
#include "startup.h"
#include <limits.h>

using namespace AfyKernel;

FileMgr::CompletionPort FileMgr::CP;
FreeQ	FileMgr::freeIODesc;

struct AsyncWait : public SemData
{
	/* Tracks async io calls, caller sits in wait until they are all complete*/
	volatile long	counter;
	AsyncWait() : counter(0) {}
	void operator++() {InterlockedIncrement(&counter);}
	void operator--() {InterlockedDecrement(&counter);}
	void wait() {for (long cnt=counter; cnt!=0; cnt=counter) if (InterlockedCompareExchange(&counter,cnt|0x80000000,cnt)==cnt) {SemData::wait(); break;}}
	static void notify(void *p) {AsyncWait *aw=(AsyncWait*)p; for (long cnt=aw->counter; ;cnt=aw->counter) if (InterlockedCompareExchange(&aw->counter,cnt-1,cnt)==cnt) {if (cnt==0x80000001) aw->wakeup(); break;}}
};

struct WinIODesc
{
	OVERLAPPED		     aio_ov;				
	myaio				*aio;
	AsyncWait			*av;
};

FileMgr::FileMgr(StoreCtx *ct,int maxOpenFiles,const char *ldDir) : GFileMgr(ct,maxOpenFiles,ldDir)
{		
}

RC FileMgr::open(FileID& fid,const char *fname,unsigned flags)
{
	HANDLE fd; off64_t fileSize=0; RC rc = RC_OK;
	if ((fname==NULL || *fname=='\0') && (flags&FIO_TEMP)==0) return RC_INVPARAM;

	const char *dir=ctx->getDirectory(); bool fdel=false;
	if ((flags&FIO_TEMP)!=0) {
		fname=(char*)malloc(MAX_PATH,STORE_HEAP); if (fname==NULL) return RC_NORESOURCES;
		if (GetTempFileName(dir!=NULL?dir:".",STOREPREFIX,0,(char*)fname)==0)
			{rc=convCode(GetLastError()); free((char*)fname,STORE_HEAP); return rc;}
		fdel=true;
	} else if (dir!=NULL && !strchr(fname,'/') && !strchr(fname,'\\') && !strchr(fname,':')) {
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

	char fullbuf[_MAX_PATH],*p;
	fd = CreateFile(fname,GENERIC_READ|GENERIC_WRITE,flags&FIO_TEMP?0:FILE_SHARE_READ,NULL,
		flags&FIO_CREATE?flags&FIO_NEW?CREATE_NEW:OPEN_ALWAYS:OPEN_EXISTING,
		(flags&FIO_TEMP?FILE_ATTRIBUTE_TEMPORARY|FILE_FLAG_DELETE_ON_CLOSE:FILE_FLAG_NO_BUFFERING)|
		FILE_ATTRIBUTE_ARCHIVE|FILE_FLAG_OVERLAPPED,NULL);
	if (fd==INVALID_HANDLE_VALUE) rc=convCode(GetLastError());
	else {
		CP.completionPort = CreateIoCompletionPort(fd,CP.completionPort,(ULONG_PTR)0,0);
		LARGE_INTEGER size; fileSize=GetFileSizeEx(fd,&size)?size.QuadPart:0;
		int lName=GetFullPathName(fname,sizeof(fullbuf),fullbuf,&p);
		if (lName) {
			if (lName<sizeof(fullbuf)) {if (fdel) {free((char*)fname,STORE_HEAP); fdel=false;} fname=fullbuf;}
			else {
				char *f2=(char*)malloc(lName+1,STORE_HEAP);
				if (f2!=NULL) {
					GetFullPathName(fname,lName+1,f2,&p); 
					if (fdel) free((char*)fname,STORE_HEAP); else fdel=true;
					fname=f2;
				}
			}
		}
	}
	if (rc==RC_OK) {
		FileDesc &file=slotTab[fid];
		file.osFile=fd;
		file.fileSize=fileSize;
		file.filePath=strdup(fname,STORE_HEAP);
		file.fSize=true;
	}

	lock.unlock();
	if (fdel) free((char*)fname,STORE_HEAP);
	return rc ;
}

off64_t GFileMgr::getFileSize(FileID fid)
{
	off64_t size=0; RWLockP rw(&lock,RW_S_LOCK);
	if (fid<xSlotTab && slotTab[fid].isOpen()) {
		if (!slotTab[fid].fSize) {
			rw.set(NULL); rw.set(&lock,RW_X_LOCK); LARGE_INTEGER size;
			if (slotTab[fid].isOpen() && !slotTab[fid].fSize && GetFileSizeEx(slotTab[fid].osFile,&size)) 
				{slotTab[fid].fileSize=size.QuadPart; slotTab[fid].fSize=true;}
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
	uint64_t currentSize=slotTab[file].fileSize; 
	lock.unlock();

	LARGE_INTEGER size; size.QuadPart=newSize;
	RC rc = !SetFilePointerEx(h,size,NULL,FILE_BEGIN) || !SetEndOfFile(h) ? convCode(GetLastError()) : RC_OK;
	if (rc==RC_OK) {
		lock.lock(RW_X_LOCK);
		if (slotTab[file].osFile==h) {
			if (slotTab[file].fileSize==currentSize) slotTab[file].fileSize=newSize;
			else if (GetFileSizeEx(h,&size)) slotTab[file].fileSize=size.QuadPart;
		}
		lock.unlock();
	}
	return rc;
}

RC FileMgr::listIO(int mode,int nent,myaio* const* pcbs,bool fSync)
{
	RC rc=RC_OK; int i,i0=0,i1=0; AsyncWait aw;
	try {
		{RWLockP lck(&lock,RW_S_LOCK);
		for (i=0; i<nent; i++) if (pcbs[i]!=NULL && pcbs[i]->aio_lio_opcode!=LIO_NOP) {
			myaio &aio=*pcbs[i];
			FileID fid=aio.aio_fid;   
			if (fid>=xSlotTab||!slotTab[fid].isOpen()) {i1=i; throw RC_INVPARAM;}
			aio.aio_rc=RC_OK;
			aio.aio_ctx=ctx;
			aio.aio_fildes=slotTab[fid].osFile;
			if (aio.aio_offset+aio.aio_nbytes>slotTab[fid].fileSize) slotTab[fid].fSize=false;
			assert(aio.aio_nbytes>0);
			assert(aio.aio_buf!=NULL);
			// Note: no check for file bounds - WRITE access 
			// allowed and read access (e.g. attempt to read invalid PID)
			// will fail at OS level
		}
		}
		for (i=0,i1=nent; i<nent; i++) if (pcbs[i]!=NULL) {
			myaio &aio = *(pcbs[i]);
			if (aio.aio_lio_opcode==LIO_NOP) {if (mode!=LIO_WAIT) asyncIOCallback(&aio); continue;}
			WinIODesc *ov=(WinIODesc*)freeIODesc.alloc(sizeof(WinIODesc)); if (ov==NULL) {i0=i; throw RC_NORESOURCES;}
			memset(ov,0,sizeof(WinIODesc)); ov->aio=pcbs[i]; if (mode==LIO_WAIT) {ov->av=&aw; ++aw;}
			ov->aio_ov.Offset=DWORD(aio.aio_offset); ov->aio_ov.OffsetHigh=DWORD(aio.aio_offset>>32);
			BOOL fOK = aio.aio_lio_opcode==LIO_WRITE ?
				WriteFile(aio.aio_fildes,aio.aio_buf,(DWORD)aio.aio_nbytes,NULL,&ov->aio_ov) :
				ReadFile(aio.aio_fildes,aio.aio_buf,(DWORD)aio.aio_nbytes,NULL,&ov->aio_ov);
			if (fOK==FALSE) {
				DWORD dwError=GetLastError();
				if (dwError!=ERROR_IO_PENDING) {
					rc=aio.aio_rc=convCode(dwError); freeIODesc.dealloc(ov);
					if (mode!=LIO_WAIT) asyncIOCallback(&aio); else --aw;
				}
			} // else if (fSync) FlushosFileBuffers(pcbs[i]->aio_fildes);
		}
		if (mode==LIO_WAIT) {
			aw.wait();
			for (i=0; i<nent; i++) if (pcbs[i]!=NULL && pcbs[i]->aio_rc!=RC_OK) {rc=pcbs[i]->aio_rc;} 	
		}
	} catch (RC rc2) {
		rc=rc2; 
		for (; i0<nent; i0++) {
			myaio &aio = *pcbs[i]; aio.aio_rc=rc2;
			if (mode!=LIO_WAIT) asyncIOCallback(&aio);
			// dealloc???
		}
	}
	return rc;
}

void FileMgr::asyncIOCompletion()
{
	for (;;) {
		DWORD l=0; ULONG_PTR key; myaio *aio=NULL; WinIODesc *ov=NULL;
		BOOL rc = GetQueuedCompletionStatus(CP.completionPort,&l,&key,(OVERLAPPED**)&ov,INFINITE);
		if (key!=(ULONG_PTR)0) break;
		if (ov!=NULL && (aio=ov->aio)!=NULL) {
			DWORD err=0; aio->aio_rc=RC_OK;
			if (rc==FALSE) {
				aio->aio_rc=convCode(err=GetLastError());
				if (err!=ERROR_OPERATION_ABORTED) report(MSG_ERROR,"Error in asyncIOCompletion: %ld\n",err);
			} else if (l!=aio->aio_nbytes) {
				report(MSG_ERROR,"Incorrect length in asyncIOCompletion: 0x%x instead of 0x%x. File %d, offset " _LX_FM "\n",l,aio->aio_nbytes,aio->aio_fid,aio->aio_offset);
				//??? see 10939
			}

			if (ov->av!=NULL) AsyncWait::notify(ov->av); else GFileMgr::asyncIOCallback(aio,true);
			freeIODesc.dealloc(ov);
		}
	}
	PostQueuedCompletionStatus(CP.completionPort,0,(ULONG_PTR)1,NULL);
}

void GFileMgr::deleteLogFiles(const char *mask,unsigned maxFile,const char *lDir,bool fArchived)
{
	char *end; 
	if (lDir!=NULL) {
		char *p=(char*)malloc(strlen(lDir)+strlen(mask)+1,STORE_HEAP); if (p==NULL) return;
		strcpy(p,lDir); strcat(p,mask); mask=p;
	}
	WIN32_FIND_DATA findData; HANDLE h=FindFirstFile(mask,&findData);
	if (h!=INVALID_HANDLE_VALUE) {
		char buf[MAX_PATH+1];
		do {
			if (lDir!=NULL) strcpy(buf,lDir); else buf[0]='\0'; 
			strcat(buf,findData.cFileName); 
			if (fArchived) {
				if ((findData.dwFileAttributes&FILE_ATTRIBUTE_ARCHIVE)!=0)
					SetFileAttributes(buf,findData.dwFileAttributes&(~FILE_ATTRIBUTE_ARCHIVE));
			}	
			else if (maxFile==~0u || strtoul(findData.cFileName+sizeof(LOGPREFIX),&end,16)<=maxFile) {	// 1 more than prefix size (for 'A' or 'B')
				::DeleteFile(buf);
			}
		} while (FindNextFile(h,&findData)==TRUE);
		FindClose(h);
	}
	if (lDir!=NULL) free((char*)mask,STORE_HEAP);
}

RC GFileMgr::deleteFile(const char *fname)
{
	return ::DeleteFile(fname)?RC_OK:convCode(GetLastError());
}

RC GFileMgr::loadExt(const char *path,size_t l,Session *ses,const Value *pars,unsigned nPars,bool fNew)
{
	if (path==NULL || l==0) return RC_INVPARAM;

	RC rc=RC_OK; size_t le=l<5 || !cmpncase(path+l-4,".DLL",4)!=0?4:0;
	size_t ld=loadDir!=NULL && !memchr(path,'/',l) && !memchr(path,'\\',l)?strlen(loadDir):0;
	char *p=(char*)ctx->malloc(ld+l+le+1); if (p==NULL) return RC_NORESOURCES;
	if (ld!=0) memcpy(p,loadDir,ld); memcpy(p+ld,path,l); if (le!=0) memcpy(p+ld+l,".DLL",4); p[ld+l+le]='\0';

	report(MSG_INFO,"Loading %s...\n",p);

	HMODULE hm=::LoadLibrary(p);
	if (hm==NULL) {
		DWORD err=GetLastError();
		report(MSG_ERROR,"Cannot load %s(%u)\n",p,err);
		rc=convCode(err);
	} else {
		typedef bool (*InitFunc)(ISession*,const Value *pars,unsigned nPars,bool fNew);
		InitFunc init = (InitFunc)::GetProcAddress(hm,INIT_ENTRY_NAME);
		if (init==NULL) {
			DWORD err=GetLastError();
			report(MSG_ERROR,"%s: cannot find '%s'(%d)\n",p,INIT_ENTRY_NAME,err);
			rc=convCode(err);
		} else if (!init(ses,pars,nPars,fNew)) {
			report(MSG_ERROR,"%s: initialization failed\n",p); rc=RC_FALSE;
		} else
			report(MSG_INFO,"%s succesfully loaded\n",p);
	}
	ctx->free(p);
	return rc;
}

#endif
