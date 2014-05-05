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

Written by Mark Venguerov 2004-2014

**************************************************************************************/

#include "fio.h"
#include "session.h"
#include "startup.h"
#include <limits.h>

using namespace AfyKernel;

GFileMgr::GFileMgr(StoreCtx *ct,int,const char *ldDir) : ctx(ct),slotTab(NULL),xSlotTab(FIO_MAX_OPENFILES),lPage(0),loadDir(NULL)
{
	slotTab=(FileDesc*)ct->malloc(sizeof(FileDesc)*xSlotTab); if (slotTab==NULL) throw RC_NOMEM;
	for (int i=0; i<xSlotTab; i++) slotTab[i].init();
	if (ldDir!=NULL) loadDir=strdup(ldDir,ct);
}

GFileMgr::~GFileMgr()
{
	closeAll(0);
}

void *GFileMgr::operator new(size_t s,StoreCtx *ctx)
{
	void *p=ctx->malloc(s); if (p==NULL) throw RC_NOMEM; return p;
}

const char *GFileMgr::getDirectory() const
{
	return ctx->getDirectory();
}

RC	GFileMgr::io(FIOType type,PageID pid,void *buf,size_t len,bool fSync)
{
	unsigned pageN=PageNumFromPageID(pid); RC rc=RC_OK; 
	myaio ov,*pov=&ov; memset(&ov,0,sizeof(myaio));
	ov.aio_lio_opcode=type==FIO_READ?LIO_READ:LIO_WRITE;
	ov.aio_fid=FileIDFromPageID(pid);
	ov.aio_nbytes=len; 
	ov.aio_buf=buf;
	ov.aio_offset=off64_t(pageN)*lPage;
	ov.aio_rc=RC_OK;
	rc=((FileMgr*)this)->listIO(LIO_WAIT,1,&pov,fSync);
	return rc==RC_OK?ov.aio_rc:rc;
}

size_t GFileMgr::getFileName(FileID fid,char buf[],size_t lbuf) const
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

RC GFileMgr::close(FileID fid)
{
	RWLockP rw(&lock,RW_X_LOCK);
	if (fid>=xSlotTab) return RC_NOTFOUND;
	if (slotTab[fid].isOpen()) {
		slotTab[fid].close();
	}
	return RC_OK;
}

void GFileMgr::closeAll(FileID start)
{
	RWLockP rw(&lock,RW_X_LOCK);
	for (FileID fid=start; fid<xSlotTab; fid++) if (slotTab[fid].isOpen()) {
		slotTab[fid].close();
	}
}

RC GFileMgr::allocateExtent(FileID fid,unsigned nPages,off64_t& addr)
{
	addr = getFileSize(fid);
	uint64_t len=uint64_t(nPages)*lPage;
	return growFile(fid,addr+len);
}

RC GFileMgr::deleteStore(const char *path0)
{
	const char *path=path0; bool fDelP=false;
	if (path!=NULL && *path!='\0') {
		size_t l=strlen(path);
		const bool fDel=path[l-1]!='/'
#ifdef WIN32
			&& path[l-1]!='\\'
#endif
		;
		if ((path=(char*)::malloc(l+1+sizeof(STOREPREFIX DATAFILESUFFIX)))==NULL) return RC_NOMEM;
		memcpy((char*)path,path0,l); if (fDel) ((char*)path)[l++]='/'; fDelP=true;
		memcpy((char*)path+l,STOREPREFIX DATAFILESUFFIX,sizeof(STOREPREFIX DATAFILESUFFIX));
	} else path=STOREPREFIX DATAFILESUFFIX;
	RC rc=deleteFile(path); if (rc==RC_OK) deleteLogFiles(~0u,path0,false);
	if (fDelP) ::free((char*)path);
	return rc;
}

void GFileMgr::deleteLogFiles(unsigned maxFile,const char *lDir,bool fArchived)
{
	deleteLogFiles(LOGPREFIX"*"LOGFILESUFFIX,maxFile,lDir,fArchived);
}

RC GFileMgr::moveStore(const char *from,const char *to)
{
	RC rc=RC_OK;
	//...
	return rc;
}

void GFileMgr::asyncIOCallback(myaio *aio,bool fAsync)
{
	if (aio->aio_notify!=NULL) {if (aio->aio_ctx!=NULL) aio->aio_ctx->set(); aio->aio_notify(aio->aio_pb,aio->aio_rc,fAsync);}
} 
