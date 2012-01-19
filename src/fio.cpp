/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov and Andrew Skowronski 2004 - 2010

**************************************************************************************/

#include "fio.h"
#include "session.h"
#include "startup.h"
#include <limits.h>
#include <stdio.h>

using namespace MVStoreKernel;

FileMgr::FileMgr(StoreCtx *ct,int /*maxOpenFiles*/,IStoreIO *i_o) 
: dir(NULL),ctx(ct),pio(i_o),bDefaultIO(false)
{
	if (pio==NULL) {
		if ((pio=getStoreIO())==NULL) throw RC_NORESOURCES; 
		bDefaultIO=true;
	}
	pio->init(asyncIOCompletionCallback);
}

FileMgr::~FileMgr()
{
	pio->closeAll(0);
	if (bDefaultIO) pio->destroy();
	ctx->free(dir);
}

void *FileMgr::operator new(size_t s,StoreCtx *ctx)
{
	void *p=ctx->malloc(s); if (p==NULL) throw RC_NORESOURCES; return p;
}

char *FileMgr::getDirString(const char *d,bool fRel)
{
	if (d==NULL || *d=='\0') return NULL;
	size_t i=strlen(d),f=d[i-1]!='/',l=0;
#ifdef WIN32
	if (f && (d[i-1]==':' || d[i-1]=='\\')) f=0;
#endif
	if (fRel && dir!=NULL && memchr(".~/",d[0],3)==NULL
#ifdef WIN32
		&& d[0]!='\\' && (i<2 || d[1]!=':')
#endif
		) l=strlen(dir);
	char *s=(char*)ctx->malloc(l+i+f+1);
	if (s!=NULL) {
		if (l!=0) memcpy(s,dir,l);
		memcpy(s+l,d,i+1); if (f) {s[l+i]='/'; s[l+i+1]='\0';}
	}
	return s;
}

void FileMgr::setDirectory(const char *d)
{
	ctx->free(dir); dir=getDirString(d);
}

RC	FileMgr::open(FileID& fid,const char *fname,ulong flags)
{
	return pio->open(fid,fname,dir,flags);
}

RC	FileMgr::close(FileID fid)
{
	return pio->close(fid);
}

void FileMgr::closeAll(FileID fid)
{
	pio->closeAll(fid);
}

RC	FileMgr::io(FIOType type,PageID pid,void *buf,size_t len,bool fSync)
{
	ulong pageN=PageNumFromPageID(pid); RC rc=RC_OK;
	IStoreIO::iodesc *ov=(IStoreIO::iodesc*)ctx->malloc(sizeof(IStoreIO::iodesc)); if (ov==NULL) return RC_NORESOURCES;
	memset(ov,0,sizeof(IStoreIO::iodesc));
	ov->aio_lio_opcode=(type==FIO_READ)?LIO_READ:LIO_WRITE;
	ov->aio_fildes=FileIDFromPageID(pid);
	ov->aio_nbytes=len; 
	ov->aio_buf=buf;
	ov->aio_offset=off64_t(pageN)*lPage;
	ov->aio_rc=RC_OK; ov->aio_ptrpos=1; ov->aio_ptr[0]=NULL;
	ov->aio_bFlush=fSync;
	rc=pio->listIO(LIO_WAIT,1,&ov);
	if (rc==RC_OK) rc=ov->aio_rc;
	ctx->free(ov);
	return rc;
}

off64_t FileMgr::getFileSize(FileID fid)
{
	return pio->getFileSize(fid);
}

RC FileMgr::truncate(FileID fid,off64_t offset)
{
	return pio->growFile(fid,offset);
}

RC FileMgr::allocateExtent(FileID fid,ulong nPages,off64_t& addr)
{
	addr = pio->getFileSize(fid);
	uint64_t len=uint64_t(nPages)*lPage;
	return pio->growFile(fid,addr+len);
}

void FileMgr::deleteLogFiles(ulong maxFile,const char *lDir,bool fArchived)
{
	if (lDir==NULL) lDir=dir;
	pio->deleteLogFiles(maxFile,lDir,fArchived);
}

RC FileMgr::deleteStore(const char *path0,IStoreIO *io)
{
	const char *path=path0; bool fDelP=false;
	if (path!=NULL && *path!='\0') {
		size_t l=strlen(path);
		const bool fDel=path[l-1]!='/'
#ifdef WIN32
			&& path[l-1]!='\\'
#endif
		;
		if ((path=(char*)::malloc(l+1+sizeof(MVSTOREPREFIX DATAFILESUFFIX)))==NULL) return RC_NORESOURCES;
		memcpy((char*)path,path0,l); if (fDel) ((char*)path)[l++]='/'; fDelP=true;
		memcpy((char*)path+l,MVSTOREPREFIX DATAFILESUFFIX,sizeof(MVSTOREPREFIX DATAFILESUFFIX));
	} else path=MVSTOREPREFIX DATAFILESUFFIX;
	bool fRelease=false;
	if (io==NULL) {if ((io=getStoreIO())==NULL) {if (fDelP) ::free((char*)path); return RC_NORESOURCES;} fRelease=true;}
	RC rc=io->deleteFile(path); if (rc==RC_OK) io->deleteLogFiles(~0ul,path0,false);
	if (fRelease) io->destroy(); if (fDelP) ::free((char*)path);
	return rc;
}

RC FileMgr::moveStore(const char *from,const char *to,IStoreIO *pio)
{
	RC rc=RC_OK;
	//...
	return rc;
}

RC	FileMgr::listIO(int mode,int nent,myaio* const* pcbs,bool fSync)
{
	int i; 
	IStoreIO::iodesc **adescs=(IStoreIO::iodesc **)ctx->malloc(sizeof(IStoreIO::iodesc *)*nent); if (adescs==NULL) return RC_NORESOURCES;
	for (i=0; i<nent; i++) 
	{
		adescs[i]=(IStoreIO::iodesc *)ctx->malloc(sizeof(IStoreIO::iodesc)); if (adescs[i]==NULL) return RC_NORESOURCES;
		adescs[i]->aio_buf=pcbs[i]->aio_buf;
		adescs[i]->aio_fildes=pcbs[i]->aio_fildes;
		adescs[i]->aio_lio_opcode=pcbs[i]->aio_lio_opcode;
		adescs[i]->aio_nbytes=pcbs[i]->aio_nbytes;
		adescs[i]->aio_offset=pcbs[i]->aio_offset;
		adescs[i]->aio_ptr[0]=pcbs[i]; adescs[i]->aio_ptrpos=1;		
		adescs[i]->aio_rc=pcbs[i]->aio_rc=RC_OK;
		adescs[i]->aio_bFlush=fSync;
		pcbs[i]->aio_ctx=ctx;
	}
	RC rc=pio->listIO(mode,nent,adescs);
	if ( mode==LIO_WAIT ) {
		for (i=0; i<nent; i++) {		 
			pcbs[i]->aio_rc=adescs[i]->aio_rc;
			ctx->free(adescs[i]);
		}
	}

	ctx->free(adescs);
	return rc;
}

bool FileMgr::asyncIOEnabled() const
{
	return pio->asyncIOEnabled();
}

void FileMgr::asyncIOCompletionCallback(IStoreIO::iodesc *aiodesc)
{
	assert(aiodesc->aio_ptrpos==1);
	myaio *aio;
	if (aiodesc->aio_ptrpos==1 && (aio=(myaio *)aiodesc->aio_ptr[--aiodesc->aio_ptrpos])!= NULL ) {			
		if (aio->aio_ctx!=NULL) aio->aio_ctx->set(); else assert(0);
		aio->aio_rc=aiodesc->aio_rc;
		if (aio->aio_notify!=NULL) aio->aio_notify(aio->aio_param,aio->aio_rc); else assert(0);
		aio->aio_ctx->free(aiodesc);
	}
} 
