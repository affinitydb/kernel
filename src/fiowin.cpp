/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov and Andrew Skowronski 2004 - 2010

**************************************************************************************/

#ifdef WIN32
#include "fiowin.h"
#include "session.h"
#include "startup.h"
#include <limits.h>
#include <stdio.h>

using namespace AfyKernel;

FileIOWin::CompletionPort FileIOWin::CP;
FreeQ<>	FileIOWin::freeIODesc;

struct WinIODesc
{
	/* needed because OVERLAPPED has nowhere to stick a context pointer */
	OVERLAPPED		     aio_ov;				
	IStoreIO::iodesc * iodesc;
} ;

struct AsyncWait : public SemData
{
	/* Tracks async io calls, caller sits in wait until they are all complete*/
	SharedCounter	counter;
	bool			fWait;
	AsyncWait() : fWait(true) {}
	static void notify(void *p) {AsyncWait *aw=(AsyncWait*)p; if (--aw->counter==0) aw->wakeup();}
};

FileIOWin::FileIOWin() : slotTab(NULL),xSlotTab(FIO_MAX_OPENFILES),asyncIOCallback(NULL)
{		
	slotTab = (FileDescWin*)malloc(sizeof(FileDescWin)*xSlotTab,STORE_HEAP); 
	if (slotTab!=NULL){ for (int i=0;i<xSlotTab;i++){ slotTab[i].init();}}
}

FileIOWin::~FileIOWin()
{
	if (slotTab!=NULL) {
		closeAll(0) ;
		free(slotTab,STORE_HEAP);
	}
}

void FileIOWin::init(void (*cb)(iodesc*))
{
	asyncIOCallback=cb;
}

RC FileIOWin::open(FileID& fid,const char *fname,const char *dir,ulong flags)
{
	HANDLE fd; off64_t fileSize=0; RC rc = RC_OK;
	if ((fname==NULL || *fname=='\0') && (flags&FIO_TEMP)==0) return RC_INVPARAM;

	bool fdel = false;
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
		FileDescWin &file=slotTab[fid];
		file.osFile=fd;
		file.fileSize=fileSize;
		file.filePath=strdup(fname,STORE_HEAP);
		file.fSize=true;
	}

	lock.unlock();
	if (fdel) free((char*)fname,STORE_HEAP);
	return rc ;
}

off64_t FileIOWin::getFileSize(FileID fid) const
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

size_t FileIOWin::getFileName(FileID fid,char buf[],size_t lbuf) const
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


RC FileIOWin::close(FileID fid)
{
	RWLockP rw(&lock,RW_X_LOCK); RC rc = RC_OK;
	if (fid>=xSlotTab) rc = RC_NOTFOUND;
	else if (slotTab[fid].isOpen()) {
		slotTab[fid].close();
	}
	return RC_OK;
}

void FileIOWin::closeAll(FileID start)
{
	RWLockP rw(&lock,RW_X_LOCK); RC rc = RC_OK;
	for (FileID fid=start; fid<xSlotTab; fid++) if (slotTab[fid].isOpen()) {
		slotTab[fid].close();
	}
}

RC FileIOWin::growFile(FileID file, off64_t newSize)
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

RC FileIOWin::listIO(int mode,int nent,iodesc* const* pcbs)
{
	RC rc=RC_OK; int i,i0=0,i1=0; AsyncWait aw;
	try {
		HANDLE *ah=(HANDLE*)alloca(nent*sizeof(HANDLE)); if (ah==NULL) throw RC_NORESOURCES;
		{RWLockP lck(&lock,RW_S_LOCK);
		for (i=0; i<nent; i++) if (pcbs[i]!=NULL && pcbs[i]->aio_lio_opcode!=LIO_NOP) {
			iodesc &aio=*(pcbs[i]);
			FileID fid=aio.aio_fildes;   
			aio.aio_rc=RC_OK;
			if (fid>=xSlotTab||!slotTab[fid].isOpen()) {i1=i; throw RC_INVPARAM;}
			ah[i]=slotTab[fid].osFile;
			if (aio.aio_offset+aio.aio_nbytes>slotTab[fid].fileSize) slotTab[fid].fSize=false;
			assert(aio.aio_nbytes>0);
			assert(aio.aio_buf!=NULL);
			// Note: no check for file bounds - WRITE access 
			// allowed and read access (e.g. attempt to read invalid PID)
			// will fail at OS level
			if (aio.aio_lio_opcode!=LIO_NOP) {
				if (mode==LIO_WAIT) {
					++aw.counter;
					aio.aio_ptr[aio.aio_ptrpos++]=&aw;
					aio.aio_ptr[aio.aio_ptrpos++]=AsyncWait::notify;
				}else { aio.aio_ptr[aio.aio_ptrpos++]=this; }
			}
			assert(aio.aio_ptrpos<=FIO_MAX_PLUGIN_CHAIN);
		}
		}
		for (i=0,i1=nent; i<nent; i++) if (pcbs[i]!=NULL) {
			iodesc &aio = *(pcbs[i]);
			if (aio.aio_lio_opcode==LIO_NOP) {if (mode!=LIO_WAIT) asyncIOCallback(&aio); continue;}
			WinIODesc *ov=(WinIODesc*)freeIODesc.alloc(sizeof(WinIODesc)); if (ov==NULL) {i0=i; throw RC_NORESOURCES;}
			memset(ov,0,sizeof(OVERLAPPED)); ov->iodesc=pcbs[i];
			ov->aio_ov.Offset=DWORD(aio.aio_offset); ov->aio_ov.OffsetHigh=DWORD(aio.aio_offset>>32);
			BOOL fOK = aio.aio_lio_opcode==LIO_WRITE ?
				WriteFile(ah[i],aio.aio_buf,(DWORD)aio.aio_nbytes,NULL,&ov->aio_ov) :
				ReadFile(ah[i],aio.aio_buf,(DWORD)aio.aio_nbytes,NULL,&ov->aio_ov);
			if (fOK==FALSE) {
				DWORD dwError=GetLastError();
				if (dwError!=ERROR_IO_PENDING) {
					rc=aio.aio_rc=convCode(dwError); 
					freeIODesc.dealloc(ov);
					if (mode==LIO_WAIT) {if (--aw.counter==0) aw.fWait=false; aio.aio_ptrpos-=2;}
					else {--aio.aio_ptrpos; asyncIOCallback(&aio); /* notify no matter what */}
				}
			} // else if (fForceFlushToDisk) FlushosFileBuffers(pcbs[i]->aio_fildes);
		}
		if (mode==LIO_WAIT) {
			if (aw.fWait) aw.wait();
			for (i=0; i<nent; i++) if (pcbs[i]!=NULL && pcbs[i]->aio_rc!=RC_OK) {rc=pcbs[i]->aio_rc;} 	
		}
	} catch (RC rc2) {
		rc=rc2; 
		for (; i0<nent; i0++) {
			iodesc &aio = *(pcbs[i]); aio.aio_rc=rc2;
			if (mode!=LIO_WAIT) {
				if (i0<i1) aio.aio_ptrpos--; else aio.aio_ptr[aio.aio_ptrpos]=this;
				asyncIOCallback(&aio);
			}
		}
	}
	return rc;
}

void FileIOWin::asyncIOCompletion()
{
	for (;;) {
		DWORD l=0; ULONG_PTR key; iodesc *aio=NULL; WinIODesc *ov=NULL;
		BOOL rc = GetQueuedCompletionStatus(CP.completionPort,&l,&key,(OVERLAPPED**)&ov,INFINITE);
		if (key!=(ULONG_PTR)0) break;
		assert(ov!=NULL && ov->iodesc!=NULL);
		if (ov!=NULL && (aio=ov->iodesc)!=NULL) {
			DWORD err=0; aio->aio_rc=RC_OK;
			if (rc==FALSE) {
				aio->aio_rc=convCode(err=GetLastError());
				if (err!=ERROR_OPERATION_ABORTED) report(MSG_ERROR,"Error in asyncIOCompletion: %ld\n",err);
			} else if (l!=aio->aio_nbytes) {
				report(MSG_ERROR,"Incorrect length in asyncIOCompletion: 0x%x instead of 0x%x.  File %d, offset "_LX_FM"\n",l,aio->aio_nbytes,aio->aio_fildes,aio->aio_offset);
				//??? see 10939
			}

			assert(aio->aio_ptrpos>0); 
			void *ctx=aio->aio_ptr[--aio->aio_ptrpos];
			if (ctx==AsyncWait::notify) {
				assert(aio->aio_ptrpos>0); 
				AsyncWait::notify(aio->aio_ptr[--aio->aio_ptrpos]);
			}
			else {
				FileIOWin *pThis=(FileIOWin*)ctx;
				if (pThis && pThis->asyncIOCallback) {pThis->asyncIOCallback(aio);}
			}

			freeIODesc.dealloc(ov);
		}
	}
	PostQueuedCompletionStatus(CP.completionPort,0,(ULONG_PTR)1,NULL);
}

void FileIOWin::deleteLogFiles(ulong maxFile,const char *lDir,bool fArchived)
{
	deleteLogFiles(LOGPREFIX"*"LOGFILESUFFIX,maxFile,lDir,fArchived);
}

void FileIOWin::deleteLogFiles(const char *mask,ulong maxFile,const char *lDir,bool fArchived)
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
			else if (maxFile==~0ul || strtoul(findData.cFileName+sizeof(LOGPREFIX),&end,16)<=maxFile) {	// 1 more than prefix size (for 'A' or 'B')
				::DeleteFile(buf);
			}
		} while (FindNextFile(h,&findData)==TRUE);
		FindClose(h);
	}
	if (lDir!=NULL) free((char*)mask,STORE_HEAP);
}

RC FileIOWin::deleteFile(const char *fname)
{
	if (!::DeleteFile(fname)) return convCode(GetLastError());
	return RC_OK;
}


IStoreIO *getStoreIO() 
{ 
	try {return new(STORE_HEAP) FileIOWin;}
	catch (...) {report(MSG_ERROR,"Exception in getStoreIO\n"); return NULL;}
}

#endif
