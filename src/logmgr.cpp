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

Written by Mark Venguerov 2004-2012

**************************************************************************************/

#include "session.h"
#include "logmgr.h"
#include "fio.h"
#include "startup.h"
#include <stdio.h>
#include <stdarg.h>

using namespace AfyKernel;

static int nLogOpen = 0;

LogMgr::LogMgr(StoreCtx *c,size_t logBufS,bool fAL,const char *lDir) : ctx(c),sectorSize(getSectorSize()),lPage(c->fileMgr->getPageSize()),
	logSegSize(max(ceil(c->theCB->logSegSize,sectorSize),(size_t)MINSEGSIZE)),bufLen(max(ceil(logBufS,sectorSize),sectorSize*4)),
	logBufBeg(NULL),logBufEnd(NULL),ptrWrite(NULL),ptrInsert(NULL),ptrRead(NULL),maxLSN(c->theCB->logEnd),minLSN(c->theCB->logEnd),prevLSN(0),
	writtenLSN(c->theCB->logEnd),wrapLSN(0),fFull(false),fRecovery(false),fAnalizing(false),recFileSize(0),maxAllocated(0),prevTruncate(~0),
	nRecordsSinceCheckpoint(0),newPage(NULL),currentLogFile(~0ul),logFile(INVALID_FILEID),nReadLogSegs(0),pcb(new(c) myaio),fArchive(fAL),
	fReadFromCurrent(false),logDirectory(c->fileMgr->getDirString(lDir,true)),fInit(false),checkpointRQ(this),segAllocRQ(this)
{
	if (pcb==NULL) throw RC_NORESOURCES;
}

LogMgr::~LogMgr()
{
	if ((ctx->mode&STARTUP_PRINT_STATS)!=0) report(MSG_INFO,"\tLogMgr stats: %ld/%ld\n",(long)nOverflow,(long)nWrites);
	for (int i=0; i<nReadLogSegs; i++) ctx->fileMgr->close(readLogSegs[i].fid);
	if (logBufBeg!=NULL) freeAligned(logBufBeg); 
//	free(logDirectory,STORE_HEAP); if (pcb!=NULL) free(pcb,STORE_HEAP);
//	report(MSG_INFO,"\n\n---- Log files: %d/%d\n\n",nNotPrealloc,nLogOpen);
}

RC LogMgr::init()
{
	RC rc=RC_OK; ctx->setState(SSTATE_MODIFIED);
	if (!fInit && (ctx->mode&STARTUP_NO_RECOVERY)==0) {
		MutexP lck(&initLock); assert(!fRecovery);
		if (!fInit && (rc=initLogBuf())==RC_OK) {
			uint32_t save=ctx->theCB->state; assert(ctx->theCB->state!=SST_LOGGING);
			lock.lock(RW_X_LOCK); off64_t fSize=0; bool fNew=true; size_t offset=0;
			if ((rc=createLogFile(ctx->theCB->checkpoint,fSize))==RC_OK) {
				fNew=save==SST_INIT || fSize==0 || fSize<(off64_t)ceil(LSNToFileOffset(ctx->theCB->logEnd),sectorSize);
				LSN lsn(fNew?ctx->theCB->checkpoint:ctx->theCB->logEnd);
				offset=LSNToFileOffset(lsn); prevLSN=maxLSN=writtenLSN=lsn; minLSN=maxLSN-offset%lPage;
				ptrInsert=logBufBeg+offset%lPage; ptrWrite=floor(ptrInsert,sectorSize);
			}
			lock.unlock();
			if (rc==RC_OK) {
				if (fNew||save==SST_READ_ONLY||save==SST_SHUTDOWN_COMPLETE) {
					ctx->theCB->checkpoint=ctx->logMgr->insert(NULL,LR_SHUTDOWN);
					rc=ctx->logMgr->flushTo(ctx->theCB->checkpoint,&ctx->theCB->logEnd);
				} else {
					pcb->aio_fildes=logFile; pcb->aio_buf=ptrWrite; pcb->aio_lio_opcode=LIO_READ;
					pcb->aio_offset=floor(offset,sectorSize); pcb->aio_nbytes=sectorSize;
					rc=ctx->fileMgr->listIO(LIO_WAIT,1,&pcb);
				}
			}
			if (save==SST_READ_ONLY||save==SST_SHUTDOWN_COMPLETE) 
				{ctx->theCB->state=SST_LOGGING; ctx->theCB->update(ctx,false);}
			fInit=true;
		}
	}
	return rc;
}

RC LogMgr::close()
{
	RC rc=RC_OK;
	if (fInit && (ctx->mode&STARTUP_NO_RECOVERY)==0) {
		ctx->theCB->checkpoint=insert(NULL,LR_SHUTDOWN);
		if ((rc=flushTo(ctx->theCB->checkpoint,&ctx->theCB->logEnd))==RC_OK)
			rc=ctx->fileMgr->close(logFile);
	}
	currentLogFile=~0ul;
	return rc;
}

void LogMgr::deleteLogs()
{
	ctx->fileMgr->deleteLogFiles(~0u,logDirectory,fArchive);
}

RC LogMgr::createLogFile(LSN lsn,off64_t& fSize)
{
	size_t lD=logDirectory!=NULL?strlen(logDirectory):0; fSize=0;
	char *buf=(char*)ctx->malloc(lD+100); if (buf==NULL) return RC_NORESOURCES;
	ulong fileN=LSNToFileN(lsn); MutexP lck(&openFile); RC rc=RC_OK; bool fFound=false;
	for (int i=0; i<nReadLogSegs; i++) if (readLogSegs[i].fid==logFile) {fFound=true; break;}
	if (!fFound && fReadFromCurrent) {ctx->bufMgr->close(logFile); logFile=INVALID_FILEID; fReadFromCurrent=false;}
	if ((rc=ctx->fileMgr->open(logFile,getLogFileName(fileN,buf),fFound?(logFile=INVALID_FILEID,FIO_CREATE):FIO_REPLACE|FIO_CREATE))==RC_OK) {
		fSize=ctx->fileMgr->getFileSize(logFile); size_t offset=LSNToFileOffset(lsn);
		if (fSize<(off64_t)offset || fSize>(off64_t)offset && (fSize!=logSegSize || (ctx->mode&STARTUP_LOG_PREALLOC)==0))
			rc=ctx->fileMgr->truncate(logFile,ceil(offset,lPage));
	}
	if (rc==RC_OK) nLogOpen++;
	else {ctx->fileMgr->close(logFile); logFile=INVALID_FILEID; if (rc!=RC_NOTFOUND) report(MSG_ERROR,"Cannot create log file %s: %d\n",buf,rc);}
	if (rc==RC_OK) {if ((currentLogFile=fileN)>maxAllocated) maxAllocated=fileN; fReadFromCurrent=false;}
	ctx->free(buf); return rc;
}

RC LogMgr::openLogFile(LSN lsn)
{
	size_t lD=logDirectory!=NULL?strlen(logDirectory):0;
	char *buf=(char*)ctx->malloc(lD+100); if (buf==NULL) return RC_NORESOURCES;
	ulong fileN=LSNToFileN(lsn); MutexP lck(&openFile); RC rc=RC_OK; bool fFound=false;
	for (int i=0; i<nReadLogSegs; i++) if (readLogSegs[i].fid==logFile) {fFound=true; break;}
	if (!fFound && fReadFromCurrent) {ctx->bufMgr->close(logFile); logFile=INVALID_FILEID; fReadFromCurrent=false;}
	ulong flags=fFound?(logFile=INVALID_FILEID,0):FIO_REPLACE;
	if ((rc=ctx->fileMgr->open(logFile,getLogFileName(fileN,buf),flags))==RC_OK) nLogOpen++;
	else {ctx->fileMgr->close(logFile); logFile=INVALID_FILEID; if (rc!=RC_NOTFOUND) report(MSG_ERROR,"Cannot open log file %s: %d\n",buf,rc);}
	if (rc==RC_OK) {if ((currentLogFile=fileN)>maxAllocated) maxAllocated=fileN; fReadFromCurrent=false;}
	ctx->free(buf); return rc;
}

RC LogMgr::allocLogFile(ulong fileN,char *buf)
{
	if (currentLogFile>=fileN && currentLogFile!=~0ul) 
		{if (currentLogFile>maxAllocated) maxAllocated=currentLogFile; return RC_OK;}
	bool fDel=false; size_t lD=logDirectory!=NULL?strlen(logDirectory):0;
	if (buf==NULL) {buf=(char*)ctx->malloc(lD+100); fDel=true; if (buf==NULL) return RC_NORESOURCES;}
	size_t bufSize=sectorSize*0x100,nBufs=logSegSize/bufSize;
	byte *zeroBuf=(byte*)allocAligned(bufSize,sectorSize);
	memset(zeroBuf,0,bufSize); RC rc=RC_OK;
	getLogFileName(fileN,buf); FileID fid=INVALID_FILEID;
	if ((rc=ctx->fileMgr->open(fid,buf,FIO_CREATE))==RC_OK) {
		for (size_t k=0; k<nBufs; k++)
			if ((rc=ctx->fileMgr->io(FIO_WRITE,PageIDFromPageNum(fid,(ulong)(bufSize*k/lPage)),zeroBuf,(ulong)bufSize))!=RC_OK) break;
		ctx->fileMgr->close(fid);
	}
	freeAligned(zeroBuf); if (fDel) ctx->free(buf);
	if (rc==RC_OK) maxAllocated=fileN;
	return rc;
}

char *LogMgr::getLogFileName(ulong logFileN,char *buf) const
{
	char *p=buf;
	if (logDirectory!=NULL) {size_t l=strlen(logDirectory); memcpy(p,logDirectory,l); p+=l;}
	sprintf(p,LOGPREFIX"A%08lX%s",logFileN,LOGFILESUFFIX);
	return buf;
}

//---------------------------------------------------------------------------------------

#define MAX_LOCAL_BUF_SIZE	0x200

LSN LogMgr::insert(Session *ses,LRType type,ulong extra,PageID pid,const LSN *undoNext,const void *pData,size_t lData,uint32_t flags,PBlock *pb,PBlock *pb2)
{	
	if (type==LR_FLUSH && fRecovery) return maxLSN;
	if (currentLogFile==~0ul || (ctx->mode&STARTUP_NO_RECOVERY)!=0)
		{if (pb!=NULL) pb->setRedo(maxLSN); if (pb2!=NULL) pb2->setRedo(maxLSN); return maxLSN;}

	assert((extra&0xF0000000)==0);
	assert(sizeof(LogRec)+lData<MAXLOGRECSIZE); 
	assert((fInit||type==LR_SHUTDOWN) && logBufBeg!=NULL);

	bool fSpec=fRecovery||type==LR_FLUSH||type==LR_SHUTDOWN||type==LR_CHECKPOINT;
	if (!fSpec) {
		if (ses==NULL) return maxLSN;
		if (ses->getTxState()!=TX_NOTRAN && ses->firstLSN.isNull()) switch (type) {
		case LR_BEGIN: break;
		case LR_COMMIT: case LR_ABORT: return maxLSN;
		default: ses->firstLSN=insert(ses,LR_BEGIN); break;
		}
	}

	LogRec logRec; 

	logRec.setInfo(type,extra);
	logRec.setLength((uint32_t)lData,flags);
	logRec.txid		= fRecovery?txid:fSpec||ses==NULL?INVALID_TXID:ses->txid;
	logRec.undoNext	= undoNext!=NULL?*undoNext:fSpec||ses==NULL?LSN(0):ses->tx.lastLSN;
	logRec.pageID	= pid;

	if (type!=LR_CHECKPOINT) {
		if (type!=LR_FLUSH) bufferLock.lock(); else if (!bufferLock.trylock()) return LSN(0);
	}

	HMAC hmac(ctx->getHMACKey(),HMAC_KEY_SIZE); bool fDel=false;
	if (pData==NULL || lData==0)
		hmac.add((const byte*)&logRec,sizeof(LogRec)-HMAC_SIZE);
	else {
		const byte *encKey=ctx->getEncKey();
		if (encKey!=NULL) {
			AES aes(encKey,ENC_KEY_SIZE); uint32_t IV[4];
			maxLSNLock.lock(RW_S_LOCK);
			IV[0]=uint32_t(maxLSN.lsn>>48); IV[1]=uint32_t(maxLSN.lsn>>32);
			IV[2]=uint32_t(maxLSN.lsn>>16); IV[3]=uint32_t(maxLSN.lsn);
			maxLSNLock.unlock();
			ulong lpad=(ceil((ulong)lData,AES_BLOCK_SIZE)-(ulong)lData-1&AES_BLOCK_SIZE-1)+1;
			ulong lbuf=(ulong)lData+lpad;
			byte *buf=lbuf<=MAX_LOCAL_BUF_SIZE?(byte*)alloca(lbuf):(byte*)0;
			if (buf==NULL) {
				buf=(byte*)malloc(lbuf,ses!=NULL?SES_HEAP:STORE_HEAP);
				if (buf==NULL) return LSN(0);	// ???
				fDel=true;
			}
			memcpy(buf,pData,lData); memset(buf+lData,lpad,lpad);
			aes.encrypt(buf,lbuf,IV); pData=buf; logRec.setLength((uint32_t)(lData=lbuf),flags);
		}
		hmac.add((const byte*)&logRec,sizeof(LogRec)-HMAC_SIZE);
		hmac.add((const byte*)pData,lData);
	}
	memcpy(logRec.hmac,hmac.result(),HMAC_SIZE);

	lock.lock(RW_X_LOCK);

	LSN saveMaxLSN(maxLSN); prevLSN=maxLSN; 
	if (!fSpec&&ses!=NULL) {ses->tx.lastLSN=maxLSN; ses->nLogRecs++;}
	if (pb!=NULL) {
		PageMgr *pm=pb->getPageMgr();
		if (pm!=NULL) pm->setLSN(maxLSN,pb->getPageBuf(),lPage);
		pb->setRedo(maxLSN);
		if (pb2!=NULL) {
			if ((pm=pb2->getPageMgr())!=NULL) pm->setLSN(maxLSN,pb2->getPageBuf(),lPage);
			pb2->setRedo(maxLSN);
		}
	}


	const void *pChunk=&logRec; size_t lChunk=sizeof(LogRec),lTotal=sizeof(LogRec)+lData; RC rc; bool fWrap=false;
	for (;;) {
		size_t available=ptrInsert<ptrWrite?ptrWrite-ptrInsert:ptrInsert>ptrWrite||!fFull?logBufEnd-ptrInsert:0;
		if (available>0) {
			size_t l=available<lChunk?available:lChunk; assert(ptrRead>=ptrInsert);
			if (fWrap && ptrInsert==logBufBeg) wrapLSN=saveMaxLSN;
			memcpy(ptrInsert,pChunk,l); ptrInsert+=l;
			maxLSNLock.lock(RW_X_LOCK); maxLSN+=l;
			if ((lTotal-=l)==0) {ptrInsert=ceil(ptrInsert,sizeof(LSN)); maxLSN.align();}
			maxLSNLock.unlock();
			assert(LSNToFileN(maxLSN)<=currentLogFile+1);
			if (ptrInsert==logBufEnd) {
				ptrInsert=ptrRead=logBufBeg; minLSN=maxLSN-bufLen; fWrap=lTotal!=0;
			} else if (ptrInsert>ptrRead) {
				minLSN+=ulong(ptrInsert-ptrRead); ptrRead=ptrInsert;
			}
			if (ptrInsert==ptrWrite) fFull=true;
			if (lTotal==0) break;
			if ((lChunk-=l)==0) {pChunk=pData; lChunk=lData;} else pChunk=(byte*)pChunk+l;
			if (!fFull) continue;
		}
		if ((rc=write())!=RC_OK) {ctx->theCB->state=SST_NO_SHUTDOWN; break;}
		if (ses!=NULL && writtenLSN>=saveMaxLSN) ses->flushLSN=saveMaxLSN;
		++nOverflow;
	}
	ulong nRecs=type==LR_CHECKPOINT?nRecordsSinceCheckpoint=0:++nRecordsSinceCheckpoint;
	if ((ctx->mode&STARTUP_LOG_PREALLOC)!=0 && LSNToFileOffset(maxLSN)>=logSegSize*LOGFILETHRESHOLD && maxAllocated==currentLogFile)
		RequestQueue::postRequest(&segAllocRQ,ctx,RQ_HIGHPRTY);
	if (!fRecovery && nRecs>=CHECKPOINTTHRESHOLD && type!=LR_CHECKPOINT) RequestQueue::postRequest(&checkpointRQ,ctx,RQ_HIGHPRTY);
	lock.unlock(); if (type!=LR_CHECKPOINT) bufferLock.unlock();
	if (fDel) free((byte*)pData,ses!=NULL?SES_HEAP:STORE_HEAP);
	return saveMaxLSN;
}

RC LogMgr::write()
{
	RC rc=RC_OK; bool oldFull=fFull; assert(lock.isXLocked());
	assert(ptrInsert!=logBufEnd && (ptrWrite-logBufBeg&sectorSize-1)==0 && ptrWrite<logBufEnd);

	size_t offset=floor(LSNToFileOffset(writtenLSN),sectorSize),lTransfer;
	byte *newPtrWrite=logBufBeg,*ptrWrt=ptrWrite; LSN newWritten(maxLSN);
	if (ptrWrt>=ptrInsert) {
		lTransfer=size_t(logBufEnd-ptrWrt);
		newWritten=LSNFromOffset(LSNToFileN(writtenLSN),offset+lTransfer);
	} else {
		lTransfer=ceil(ptrInsert-ptrWrt,sectorSize);
		newPtrWrite=floor(ptrInsert,sectorSize);
	}
	if (ptrWrt==logBufBeg) wrapLSN=0;

	assert(ptrRead>=ptrInsert);
	byte *newRead=ceil(ptrRead,sectorSize); ulong delta=ulong(newRead-ptrRead);
	if (delta>0) {minLSN+=delta; memset(ptrRead,0,delta); ptrRead=newRead;}
	
	if (offset+lTransfer>logSegSize) {
		lock.downgradelock(RW_U_LOCK);
		size_t lTran=logSegSize-offset; assert(lTran<lTransfer);
		pcb->aio_fildes		= logFile;
		pcb->aio_offset		= offset;
		pcb->aio_buf			= ptrWrt;
		pcb->aio_nbytes		= lTran;
		pcb->aio_lio_opcode	= LIO_WRITE;
		rc=ctx->fileMgr->listIO(LIO_WAIT,1,&pcb,true);
		lock.upgradelock(RW_X_LOCK); 
		if (rc!=RC_OK) {report(MSG_ERROR,"Error %d writting log file %d\n",rc,currentLogFile); return rc;}
		ptrWrt+=lTran; assert(ptrWrt!=logBufEnd);
		writtenLSN=LSNFromOffset(currentLogFile+1,0); 
		lTransfer-=lTran; offset=0; ++nWrites;
	}
	if (LSNToFileN(writtenLSN)>currentLogFile) {
		assert(offset==0 && LSNToFileN(writtenLSN)==currentLogFile+1); off64_t lf;
		if ((rc=createLogFile(writtenLSN,lf))!=RC_OK) return rc;
	}

	lock.downgradelock(RW_U_LOCK);
	
	pcb->aio_fildes		= logFile;
	pcb->aio_offset		= offset;
	pcb->aio_buf		= ptrWrt;
	pcb->aio_nbytes		= lTransfer;
	pcb->aio_lio_opcode	= LIO_WRITE;

	rc = ctx->fileMgr->listIO(LIO_WAIT,1,&pcb,true);

	lock.upgradelock(RW_X_LOCK);

	if (rc==RC_OK) {
		writtenLSN=newWritten; ptrWrite=newPtrWrite; if (oldFull) fFull=false;
		assert(writtenLSN<=maxLSN && (LSNToFileN(writtenLSN)==currentLogFile
				|| writtenLSN==LSNFromOffset(currentLogFile,logSegSize)));
		++nWrites;
	} else {
		report(MSG_ERROR,"Error %d writting log file %d\n",rc,currentLogFile);
		// retry ???
	}
	return rc;
}

RC LogMgr::flushTo(LSN lsn,LSN *ret)
{
	if ((ctx->mode&STARTUP_NO_RECOVERY)!=0) return RC_OK;
	Session *ses=Session::getSession(); if (ses!=NULL && ses->flushLSN>lsn && ret==NULL) return RC_OK;

	lock.lock(RW_X_LOCK); RC rc;
	while (lsn>=writtenLSN || !wrapLSN.isNull() && lsn>=wrapLSN)
		{assert(lsn<maxLSN); if ((rc=write())!=RC_OK) {lock.unlock(); return rc;}}
	if (ses!=NULL) ses->flushLSN=writtenLSN;
	if (ret!=NULL) *ret=writtenLSN;
	lock.unlock();
	return RC_OK;
}

LogReadCtx::LogReadCtx(LogMgr *mgr,Session *s) 
	: logMgr(mgr),ses(s),currentLogSeg(~0ul),fid(INVALID_FILEID),ptr(NULL),len(0),fCheck(true),fLocked(false),xlrec(0),rbuf(NULL),lrec(0)
{
}

LogReadCtx::~LogReadCtx()
{
	ses->free(rbuf);
	if (fLocked) logMgr->bufferLock.unlock();
	pb.release(ses);
	closeFile();
}

void LogReadCtx::release()
{
	pb.release(ses);
}

void LogReadCtx::closeFile()
{
	if (currentLogSeg!=~0ul) {
		MutexP lck(&logMgr->openFile);
		for (int i=0; i<logMgr->nReadLogSegs; i++) {
			PrevLogSeg &ps=logMgr->readLogSegs[i];
			if (ps.logFile==currentLogSeg) {
				if (--ps.nReads==0) {
					if (!pb.isNull() && FileIDFromPageID(pb->getPageID())==ps.fid) pb.release(ses);
					if (ps.fid!=logMgr->logFile) logMgr->ctx->bufMgr->close(ps.fid);
					if (--logMgr->nReadLogSegs>i) memmove(&ps,&ps+1,(logMgr->nReadLogSegs-i)*sizeof(PrevLogSeg));
					logMgr->waitLogSeg.signal();
				}
				break;
			}
		}
		currentLogSeg=~0ul;
	}
}

RC LogReadCtx::readChunk(LSN lsn,void *buf,size_t l)
{
	assert(logMgr->fInit && buf!=NULL && l!=0);
	for (;;) {
		if (fCheck) {
			if (!fLocked) {logMgr->lock.lock(RW_S_LOCK); fLocked=true;} fCheck=false;
			assert(lsn+l<=logMgr->maxLSN || logMgr->fRecovery);
			if (lsn>=logMgr->minLSN && lsn<logMgr->maxLSN) {
				pb.release(ses); closeFile(); fid=INVALID_FILEID; 
				if ((ptr=logMgr->ptrRead+ulong(lsn-logMgr->minLSN))>=logMgr->logBufEnd) ptr-=logMgr->bufLen;
				len=ptr>=logMgr->ptrInsert?ulong(logMgr->logBufEnd-ptr):ulong(logMgr->ptrInsert-ptr);
			} else if (logMgr->fAnalizing) {
				size_t offset=logMgr->LSNToFileOffset(logMgr->minLSN);
				if ((offset+=logMgr->bufLen)<logMgr->recFileSize) logMgr->minLSN+=logMgr->bufLen;
				else if (logMgr->recFileSize>=logMgr->logSegSize && logMgr->openLogFile(lsn)==RC_OK) {
					logMgr->recFileSize=(ulong)logMgr->ctx->fileMgr->getFileSize(logMgr->logFile); 
					offset=0; logMgr->minLSN=logMgr->LSNFromOffset(logMgr->LSNToFileN(lsn),0);
				} else {
					logMgr->lock.unlock(); fLocked=false; fCheck=true; return RC_EOF;
				}
				logMgr->maxLSN=logMgr->minLSN; logMgr->ptrInsert=logMgr->logBufBeg; logMgr->fFull=false;
				size_t lread=min(logMgr->recFileSize-offset,logMgr->bufLen);
				if (lread==0) {logMgr->lock.unlock(); fLocked=false; fCheck=true; return RC_EOF;}
				logMgr->pcb->aio_fildes=logMgr->logFile; logMgr->pcb->aio_buf=logMgr->logBufBeg; logMgr->pcb->aio_lio_opcode=LIO_READ; 
				logMgr->pcb->aio_offset=offset; logMgr->pcb->aio_nbytes=ceil(lread,logMgr->sectorSize);
				RC rc=logMgr->ctx->fileMgr->listIO(LIO_WAIT,1,&logMgr->pcb); if (rc!=RC_OK) {logMgr->lock.unlock(); fLocked=false; fCheck=true; return rc;}
				logMgr->maxLSN+=lread; logMgr->ptrInsert=logMgr->logBufBeg+lread; fCheck=true; continue;
			} else {
				LSN writtenLSN(logMgr->writtenLSN); logMgr->lock.unlock(); fLocked=false;
				ulong logSeg=logMgr->LSNToFileN(lsn); size_t offset=logMgr->LSNToFileOffset(lsn);
				if (logSeg!=currentLogSeg) {
					pb.release(ses); closeFile();
					MutexP lck(&logMgr->openFile); bool fFound=false;
					for (int i=0; i<logMgr->nReadLogSegs; i++) {
						PrevLogSeg &ps=logMgr->readLogSegs[i];
						if (ps.logFile==logSeg) {ps.nReads++; fid=ps.fid; fFound=true; break;}
					}
					while (!fFound && logMgr->nReadLogSegs>=MAXPREVLOGSEGS) {
						logMgr->waitLogSeg.wait(logMgr->openFile,1000);
						for (int i=0; i<logMgr->nReadLogSegs; i++) {
							PrevLogSeg &ps=logMgr->readLogSegs[i];
							if (ps.logFile==logSeg) {ps.nReads++; fid=ps.fid; fFound=true; break;}
						}
					}
					if (!fFound) {
						if (logSeg==logMgr->currentLogFile) {
							fid=logMgr->logFile; logMgr->fReadFromCurrent=true;
						} else {
							char fname[100]; fid=INVALID_FILEID;
							RC rc=logMgr->ctx->fileMgr->open(fid,logMgr->getLogFileName(logSeg,fname));
							if (rc!=RC_OK) {fCheck=true; return rc;}
						}
						PrevLogSeg &ps=logMgr->readLogSegs[logMgr->nReadLogSegs++]; 
						assert(logMgr->nReadLogSegs<=MAXPREVLOGSEGS);
						ps.fid=fid; ps.logFile=logSeg; ps.nReads=1;
					}
					currentLogSeg=logSeg;
				}
				if (lsn>writtenLSN || writtenLSN-lsn<(off64_t)logMgr->lPage && logMgr->LSNToFileOffset(lsn)/logMgr->lPage==logMgr->LSNToFileOffset(writtenLSN)/logMgr->lPage) {
					RWLockP lck(&logMgr->lock,RW_X_LOCK);
					if (lsn>logMgr->writtenLSN || logMgr->writtenLSN-lsn<(off64_t)logMgr->lPage && logMgr->LSNToFileOffset(lsn)/logMgr->lPage==logMgr->LSNToFileOffset(logMgr->writtenLSN)/logMgr->lPage)
						logMgr->write();
				}
				PageID pid=PageIDFromPageNum(fid,ulong(offset/logMgr->lPage)); offset%=logMgr->lPage;
				if ((pb.isNull() || pb->getPageID()!=pid) && pb.getPage(pid,NULL,0,ses)==NULL) {
					// cannot read
					fCheck=true; return RC_EOF;
				}
				ptr=pb->getPageBuf()+offset; len=logMgr->lPage-offset;
			}
		}
		assert(ptr!=NULL); size_t ll=min(len,l); memcpy(buf,ptr,ll);
		if ((l-=ll)==0) {if ((len-=ll)==0) fCheck=true; else ptr+=ll; return RC_OK;}
		buf=(byte*)buf+ll; lsn+=ll; fCheck=true;
	}
}

RC LogReadCtx::read(LSN& lsn)
{
	RC rc=readChunk(lsn,&logRec,sizeof(LogRec)); if (rc!=RC_OK) return rc;

	if (logRec.getType()==0) {
		rc = RC_EOF;
		for (unsigned i=0; i<sizeof(LogRec)/sizeof(uint32_t); i++)
			if (((uint32_t*)&logRec)[i]!=0) {rc = RC_CORRUPTED; break;}
	}

	ulong lbuf=logRec.getLength(); flags=logRec.getFlags();
	if (lbuf>logMgr->logSegSize) rc=RC_CORRUPTED;
	if (rc==RC_OK && lbuf!=0) {
		if (lbuf+sizeof(LogRec)>MAXLOGRECSIZE) rc=RC_CORRUPTED;
		else if (lbuf>xlrec) {
			if ((rbuf=(byte*)ses->realloc(rbuf,lbuf))==NULL) rc=RC_NORESOURCES; else xlrec=lbuf;
		}
		if (rc==RC_OK) rc=readChunk(lsn+sizeof(LogRec),rbuf,lbuf);
	}

	if (fLocked) {logMgr->lock.unlock(); fLocked=false;}
	if (!pb.isNull() && !logMgr->fRecovery) pb.release(ses);
	fCheck=true;

	if (rc==RC_OK) {
		HMAC hmac(logMgr->ctx->getHMACKey(),HMAC_KEY_SIZE);
		hmac.add((const byte*)&logRec,sizeof(LogRec)-HMAC_SIZE);
		if (lbuf!=0) hmac.add(rbuf,lbuf);
		if (memcmp(logRec.hmac,hmac.result(),HMAC_SIZE)==0 && (type=logRec.getType())<LR_ALL) {
			const byte *encKey=logMgr->ctx->getEncKey(); lrec=lbuf;
			if (encKey!=NULL && rbuf!=NULL && lbuf!=0) {
				AES aes(encKey,ENC_KEY_SIZE); uint32_t IV[4];
				IV[0]=uint32_t(lsn.lsn>>48); IV[1]=uint32_t(lsn.lsn>>32);
				IV[2]=uint32_t(lsn.lsn>>16); IV[3]=uint32_t(lsn.lsn);
				aes.decrypt(rbuf,lbuf,IV); lrec-=rbuf[lbuf-1];
			}
			lsn+=sizeof(LogRec)+lbuf; lsn.align();
		} else {
			rc=RC_CORRUPTED; LSN end(lsn); end+=sizeof(LogRec)+lbuf; end.align();
			size_t offset = floor(logMgr->LSNToFileOffset(end),logMgr->sectorSize);
			if (offset<sizeof(LogRec)+lbuf) {
				rc=RC_EOF; const uint32_t *p=(const uint32_t*)(offset>lbuf?rbuf:rbuf+lbuf-offset);
				for (ulong i=(ulong)min((ulong)offset,lbuf)/sizeof(uint32_t); i>0; i--) if (*p++!=0) {rc=RC_CORRUPTED; break;}
				if (rc==RC_EOF && offset>lbuf) {
					p=(const uint32_t*)((byte*)&logRec+sizeof(LogRec)+lbuf-offset);
					for (ulong j=ulong((offset-lbuf)/sizeof(uint32_t)); j>0; j--) if (*p++!=0) {rc=RC_CORRUPTED; break;}
				}
			}
		}
	}
	return rc;
}

void LogMgr::CheckpointRQ::process()
{
	RC rc=mgr->checkpoint();
	if (rc!=RC_OK) report(MSG_ERROR,"Checkpoint failed (%d)\n",rc);
}

void LogMgr::CheckpointRQ::destroy()
{
}

void LogMgr::SegAllocRQ::process()
{
	if (mgr->currentLogFile==mgr->maxAllocated) {
		mgr->openFile.lock(); 
		mgr->allocLogFile(mgr->maxAllocated+1);
		mgr->openFile.unlock();
	}
}

void LogMgr::SegAllocRQ::destroy()
{
}
