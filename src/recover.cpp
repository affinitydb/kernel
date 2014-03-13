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

#include "recover.h"
#include "logchkp.h"
#include "txmgr.h"
#include "pgheap.h"
#include "fsmgr.h"
#include "fio.h"

using namespace AfyKernel;

const static char *LR_Tab[LR_ALL] =
{
	"???", "LR_UPDATE", "LR_CREATE", "LR_BEGIN", "LR_COMMIT", "LR_ABORT", "LR_COMPENSATE", 
	"LR_RESTORE", "LR_CHECKPOINT", "LR_FLUSH", "LR_SHUTDOWN", "LR_SESSION", "LR_LUNDO", 
	"LR_COMPENSATE2", "LR_DISCARD", "LR_COMPENSATE3"
};

RC LogMgr::recover(Session *ses,bool fRollforward)
{
	if ((ctx->mode&STARTUP_NO_RECOVERY)!=0) return RC_OK;
	if (ses==NULL) return RC_NOSESSION; assert(ctx->theCB!=NULL); 

	RC rc=RC_OK; LSN chkp(ctx->theCB->checkpoint);

	initLock.lock();
	if (!fInit) {RWLockP lck(&lock,RW_X_LOCK); if ((rc=openLogFile(chkp))==RC_OK) {rc=initLogBuf(); fInit=true;}}
	if (rc==RC_OK) {
		prevLSN=maxLSN=writtenLSN=ctx->theCB->logEnd;
		size_t offset=floor(LSNToFileOffset(chkp),lPage),lread;
		if (ctx->fileMgr==NULL) {
			//????
		} else if ((recFileSize=(unsigned)ctx->fileMgr->getFileSize(logFile))!=0) {
			if (recFileSize<bufLen) {offset=0; lread=ceil(recFileSize,sectorSize);}
			else if (recFileSize-offset>=bufLen) lread=bufLen;
			else {offset=ceil(recFileSize,lPage)-bufLen; lread=recFileSize-offset;}
			minLSN=LSNFromOffset(LSNToFileN(chkp),offset); maxLSN=minLSN+lread;
			aio.aio_fid=logFile; aio.aio_buf=logBufBeg; aio.aio_lio_opcode=LIO_READ;
			aio.aio_offset=offset; aio.aio_nbytes=ceil(lread,sectorSize);
			rc=ctx->fileMgr->listIO(LIO_WAIT,1,&pcb);
		} else rc=RC_CORRUPTED;
	}
	initLock.unlock();
	if (rc!=RC_OK) {report(MSG_CRIT,"Recovery: cannot open log file(%d)\n",rc); return rc;}

	ctx->theCB->state=SST_IN_RECOVERY; ctx->theCB->update(ctx,false); fRecovery=fAnalizing=true;

	DirtyPg *dpg,*dp; LogReadCtx rctx(this,ses);
#ifdef _DEBUG
	TIMESTAMP startTime,endTime; getTimestamp(startTime);
#endif

	LSN lastLSN(chkp),prevLSN(chkp),sMinLSN;
	if ((rc=rctx.read(lastLSN))!=RC_OK)
		{report(MSG_CRIT,"Recovery: cannot read checkpoint record at " _LX_FM " (%d)\n",chkp.lsn,rc); fRecovery=fAnalizing=false; return RC_CORRUPTED;}

	TxSet losers(LOSERSETSIZE,(MemAlloc*)ses); LSN logEnd(ctx->theCB->logEnd);
	DirtyPageSet dirtyPages(DIRTYPAGESETSIZE,(MemAlloc*)ses); unsigned ntx=0,npg=0,extra; PageMgr *pageMgr;
	if (rctx.type==LR_CHECKPOINT) {
		if (rctx.rbuf==NULL || rctx.lrec==0)
			{report(MSG_CRIT,"Recovery: empty checkpoint record at " _LX_FM "\n",chkp.lsn); fRecovery=fAnalizing=false; return RC_CORRUPTED;}
		size_t ltx=sizeof(uint64_t)+(size_t)((LogActiveTransactions*)rctx.rbuf)->nTransactions*sizeof(LogActiveTransactions::LogActiveTx);
		if (ltx+sizeof(uint64_t)+(size_t)((LogDirtyPages*)(rctx.rbuf+ltx))->nPages*sizeof(LogDirtyPages::LogDirtyPage)!=rctx.lrec)
			{report(MSG_CRIT,"Recovery: invalid length of checkpoint record at " _LX_FM "\n",chkp.lsn); fRecovery=fAnalizing=false; return RC_CORRUPTED;}
		ntx=(unsigned)((LogActiveTransactions*)rctx.rbuf)->nTransactions; npg=(unsigned)((LogDirtyPages*)(rctx.rbuf+ltx))->nPages;
		const LogActiveTransactions::LogActiveTx *tx=(const LogActiveTransactions::LogActiveTx*)(rctx.rbuf+sizeof(uint64_t));
		for (unsigned i=0; i<ntx; ++tx,++i) {
			Trans *trans=new Trans(tx->txid,tx->lastLSN); if (trans!=NULL) losers.insert(trans);
#if defined(_DEBUG) && defined(DEBUG_PRINT_ACTIVE_TX)
			report(MSG_DEBUG,"\tActive tx: " _LX_FM "\n",tx->txid);
#endif
		}
		const LogDirtyPages::LogDirtyPage *dpg=(const LogDirtyPages::LogDirtyPage*)(rctx.rbuf+ltx+sizeof(uint64_t));
		for (unsigned j=0; j<npg; ++j,++dpg) {
			assert(dpg->redo<chkp); if ((dp=new DirtyPg((PageID)dpg->pageID,dpg->redo))!=NULL) dirtyPages.insert(dp);
#if defined(_DEBUG) && defined(DEBUG_PRINT_DIRTY_PAGES)
			report(MSG_DEBUG,"\tDirty page: %08X (" _LX_FM ")\n",(PageID)dpg->pageID,dpg->redo.lsn);
#endif
		}
	} else if (rctx.type!=LR_SHUTDOWN) {
		report(MSG_CRIT,"Recovery: corrupted checkpoint record at " _LX_FM "\n",chkp.lsn); fRecovery=fAnalizing=false; return RC_CORRUPTED;
	}
#ifdef _DEBUG
	report(MSG_DEBUG,"\t%s: checkpoint at " _LX_FM " (%dtx, %dpg)\n",fRollforward?"Rollforward":"Recovery",chkp.lsn,ntx,npg);
#endif

	// ANALYSE pass

	for (;;) {
		LSN save(lastLSN); sMinLSN=minLSN;
		if ((rc=rctx.read(lastLSN))!=RC_OK) {
			if (rc!=RC_EOF && rc!=RC_CORRUPTED) 
				report(MSG_ERROR,"Recovery:analyze: cannot read record at " _LX_FM " (%d)\n",save.lsn,rc);
			break;
		}
		prevLSN=save;
		if (rctx.logRec.txid!=INVALID_TXID && rctx.logRec.txid>ctx->theCB->lastTXID) ctx->theCB->lastTXID=rctx.logRec.txid;
		switch (rctx.type) {
		case LR_BEGIN:
			if (rctx.logRec.txid==INVALID_TXID)
				report(MSG_ERROR,"LR_BEGIN: invalid TXID " _LX_FM ", LSN: " _LX_FM "\n",rctx.logRec.txid,lastLSN.lsn);
			else if (losers.find(rctx.logRec.txid)==NULL) {
				Trans *trans=new Trans(rctx.logRec.txid,LSN(0)); if (trans!=NULL) losers.insert(trans);
			}
			break;
		case LR_ABORT:
		case LR_COMMIT:
			if (rctx.logRec.txid!=INVALID_TXID) losers.remove(rctx.logRec.txid,true);
			else report(MSG_ERROR,"%s: invalid TXID " _LX_FM ", LSN: " _LX_FM "\n",LR_Tab[rctx.type],rctx.logRec.txid,lastLSN.lsn);
			break;
		case LR_CREATE:
		case LR_UPDATE:
		case LR_COMPENSATE:
		case LR_COMPENSATE2:
		case LR_COMPENSATE3:
		case LR_RESTORE:
		case LR_DISCARD:
			if (rctx.logRec.txid!=INVALID_TXID) {
				Trans *tx=losers.find(rctx.logRec.txid);
				if (tx!=NULL) tx->lastLSN=save;
				else report(MSG_ERROR,"%s: TX " _LX_FM " not found, LSN: " _LX_FM "\n",LR_Tab[rctx.type],rctx.logRec.txid,lastLSN.lsn);
			}
			if (rctx.type==LR_RESTORE || TxMgr::isMaster(rctx.logRec.getExtra())) break;
			if (rctx.logRec.pageID!=INVALID_PAGEID) {
				if ((dpg=dirtyPages.find(rctx.logRec.pageID))==NULL && (dp=new DirtyPg(rctx.logRec.pageID,save))!=NULL) dirtyPages.insert(dp);
				extra=rctx.logRec.getExtra(); pageMgr=TxMgr::getPageMgr(extra,ctx); PageID pgID; bool fMerge=false;
				if (pageMgr!=NULL && (rctx.type==LR_UPDATE||rctx.type==LR_COMPENSATE) && (pgID=pageMgr->multiPage(extra>>PGID_SHIFT,rctx.rbuf,rctx.lrec,fMerge))!=INVALID_PAGEID) {
					if (fMerge) {
						// ???
					} else if ((dpg=dirtyPages.find(pgID))==NULL && (dp=new DirtyPg(pgID,save))!=NULL) dirtyPages.insert(dp);
				}
			} else if (rctx.type!=LR_COMPENSATE)
				report(MSG_ERROR,"%s: invalid pageID %08X, LSN: " _LX_FM "\n",LR_Tab[rctx.type],rctx.logRec.pageID,lastLSN.lsn);
			break;
		case LR_FLUSH:
			if (!fRollforward) {
				if (rctx.logRec.pageID!=INVALID_PAGEID && !TxMgr::isMaster(rctx.logRec.getExtra()))
					dirtyPages.remove(rctx.logRec.pageID,true);
				else
					report(MSG_ERROR,"LR_FLUSH: invalid pageID %08X, LSN: " _LX_FM "\n",rctx.logRec.pageID,lastLSN.lsn);
			}
			break;
		default:
			break;
		}
	}

	rctx.closeFile();

#ifdef _DEBUG
	getTimestamp(endTime);
	report(MSG_DEBUG,"\tRecovery: analyze pass finished, %.3fsec\n",double(endTime-startTime)/1000000.);
	getTimestamp(startTime);
#endif

	lock.lock(RW_X_LOCK); fAnalizing=false;
	if (sMinLSN!=minLSN && lastLSN!=minLSN) {
		size_t len=(size_t)(lastLSN-sMinLSN),offset=LSNToFileOffset(sMinLSN); rc=RC_OK; assert(lastLSN>=sMinLSN && len<bufLen);
		if (LSNToFileN(minLSN)==LSNToFileN(sMinLSN) || (rc=openLogFile(sMinLSN))==RC_OK) {
			if (ctx->fileMgr!=NULL) {
				aio.aio_fid=logFile; aio.aio_buf=logBufBeg; aio.aio_lio_opcode=LIO_READ; 
				aio.aio_offset=offset; aio.aio_nbytes=ceil(len,sectorSize); rc=ctx->fileMgr->listIO(LIO_WAIT,1,&pcb);
			} else {
				//????
			}
		}
		if (rc==RC_OK) minLSN=sMinLSN;
		else {lock.unlock(); report(MSG_CRIT,"Recovery: cannot re-read log file segment(%d)\n",rc); fRecovery=false; return rc;}
	}
	if (logFile==INVALID_FILEID && (rc=openLogFile(sMinLSN))!=RC_OK)
		{lock.unlock(); report(MSG_CRIT,"Recovery: cannot re-open log file segment(%d)\n",rc); fRecovery=false; return rc;}
	this->prevLSN=prevLSN; writtenLSN=maxLSN=lastLSN; assert(maxLSN>=minLSN);
	if ((ptrInsert=logBufBeg+(maxLSN-minLSN))==logBufEnd) ptrInsert=ptrRead=logBufBeg;
	ptrWrite=floor(ptrInsert,sectorSize);
	lock.unlock();

	// REDO pass

	LSN redo(logEnd); PBlock *pb=NULL,*pb2;
	for (DirtyPageSet::it it(dirtyPages); ++it;) {dpg=it.get(); if (redo>dpg->redoLSN) redo=dpg->redoLSN;}

#ifdef _DEBUG
	report(MSG_DEBUG,"\tRecovery: redo start at " _LX_FM ", end at " _LX_FM "\n",redo.lsn,lastLSN.lsn);
#endif
	
	while (redo < lastLSN) {
		ctx->logMgr->recv=redo;
		if ((rc=rctx.read(redo))!=RC_OK) {
			report(MSG_ERROR,"Recovery:redo: cannot read record at " _LX_FM " (%d)\n",ctx->logMgr->recv.lsn,rc); break;
		}
		rctx.type=rctx.logRec.getType(); bool fUndo=false;
		switch (rctx.type) {
		default: break;
		case LR_COMPENSATE2: case LR_DISCARD:
			if (rctx.logRec.pageID!=INVALID_PAGEID && (dpg=dirtyPages.find(rctx.logRec.pageID))!=NULL && dpg->redoLSN<redo) {
				if (pb!=NULL && pb->getPageID()==rctx.logRec.pageID) {pb->release(PGCTL_DISCARD|QMGR_UFORCE,ses); pb=NULL;}
				else if (rctx.logRec.pageID!=INVALID_PAGEID) ctx->bufMgr->drop(rctx.logRec.pageID);
			}
			break;
		case LR_COMPENSATE: if (rctx.logRec.pageID!=INVALID_PAGEID) fUndo=true; else break;
		case LR_UPDATE: case LR_CREATE: case LR_COMPENSATE3:
			if (TxMgr::isMaster(extra=rctx.logRec.getExtra())) {
				if (rctx.logRec.pageID==INVALID_PAGEID && logEnd<ctx->logMgr->recv) {
					RC rc=ctx->theCB->update(ctx,extra>>PGID_SHIFT,rctx.rbuf,rctx.lrec,fUndo);
					if (rc!=RC_OK) report(MSG_ERROR,"%s redo: master page update failed: %d, LSN: " _LX_FM "\n",LR_Tab[rctx.type],rc,ctx->logMgr->recv.lsn);
					ctx->theCB->logEnd=ctx->logMgr->recv;
				}
			} else if (rctx.logRec.pageID!=INVALID_PAGEID && (dpg=dirtyPages.find(rctx.logRec.pageID))!=NULL && dpg->redoLSN<redo) {
				pageMgr=TxMgr::getPageMgr(extra,ctx);
				if (pageMgr==NULL)
					report(MSG_ERROR,"Invalid PGID %d in recovery:redo, LSN: " _LX_FM "\n",extra&PGID_MASK,ctx->logMgr->recv.lsn);
				else {
					pb=rctx.type==LR_CREATE||rctx.type==LR_COMPENSATE3 ? ctx->bufMgr->newPage(rctx.logRec.pageID,pageMgr,pb,0,ses) :
															ctx->bufMgr->getPage(rctx.logRec.pageID,pageMgr,PGCTL_XLOCK|QMGR_UFORCE,pb,ses);
					if (pb==NULL)
						report(MSG_ERROR,"%s redo: cannot read page %08X , LSN: " _LX_FM "\n",LR_Tab[rctx.type],rctx.logRec.pageID,ctx->logMgr->recv.lsn);
					else if (pageMgr->getLSN(pb->getPageBuf(),lPage)<ctx->logMgr->recv) {
						if (rctx.type!=LR_COMPENSATE3||rctx.rbuf!=NULL&&rctx.lrec!=0) {
							RC rc=pageMgr->update(pb,lPage,extra>>PGID_SHIFT,rctx.rbuf,rctx.lrec,fUndo?TXMGR_UNDO:TXMGR_RECV);
							if (rc!=RC_OK) report(MSG_ERROR,"%s redo: page %08X update failed: %d, LSN: " _LX_FM "\n",LR_Tab[rctx.type],rctx.logRec.pageID,rc,ctx->logMgr->recv.lsn);
						}
						pageMgr->setLSN(ctx->logMgr->recv,pb->getPageBuf(),lPage); pb->setRedo(ctx->logMgr->recv);
						if ((pb2=ctx->logMgr->newPage)!=NULL) {
							bool fDiscard=true;
							if ((dpg=dirtyPages.find(pb2->getPageID()))!=NULL && dpg->redoLSN<redo)
								{pageMgr->setLSN(ctx->logMgr->recv,pb2->getPageBuf(),lPage); pb2->setRedo(ctx->logMgr->recv); fDiscard=false;}
							pb2->release(fDiscard?PGCTL_DISCARD|QMGR_UFORCE:QMGR_UFORCE,ses); ctx->logMgr->newPage=NULL;
						}
					}
				}
			}
		}
	}

#ifdef _DEBUG
	getTimestamp(endTime);
	report(MSG_DEBUG,"\tRecovery: redo pass finished, %.3fsec\n",double(endTime-startTime)/1000000.);
	getTimestamp(startTime);
#endif

	// UNDO pass

	TxSet activeTx(ACTIVESETSIZE,(MemAlloc*)ses);
	for (TxSet::it it(losers); ++it;) {
		Trans *trans=new Trans(*it.get()); if (trans!=NULL) activeTx.insert(trans);
	}

	for (;;) {
		Trans *undoTx=NULL; LSN maxLastLSN(0);
		for (TxSet::it it(losers); ++it;) {Trans *tx=it.get(); if (tx->lastLSN>maxLastLSN) {undoTx=tx; maxLastLSN=tx->lastLSN;}}
		if (undoTx==NULL) break;

		lastLSN=maxLastLSN; Trans *act=NULL;
		if ((rc=rctx.read(maxLastLSN))!=RC_OK) {report(MSG_ERROR,"Recovery:undo: cannot read record at " _LX_FM " (%d)\n",lastLSN.lsn,rc); break;}

		switch (rctx.type) {
		case LR_COMPENSATE: case LR_COMPENSATE2: case LR_COMPENSATE3: case LR_RESTORE: break;
		case LR_LUNDO:
			if ((pageMgr=TxMgr::getPageMgr(extra=rctx.logRec.getExtra(),ctx))==NULL)
				report(MSG_ERROR,"Invalid PGID %d in recovery:undo(LR_LUNDO), LSN: " _LX_FM "\n",extra&PGID_MASK,lastLSN.lsn);
			else {
				act=activeTx.find(rctx.logRec.txid); assert(act!=NULL); txid=act->txid;
				if (pb!=NULL) {pb->release(QMGR_UFORCE,ses); pb=NULL;}
				rc=pageMgr->undo(extra>>PGID_SHIFT,rctx.rbuf,rctx.lrec);
				if (rc!=RC_OK) report(MSG_ERROR,"LR_LUNDO: update failed: %d, LSN: " _LX_FM "\n",rc,lastLSN.lsn);
				act->lastLSN=insert(ses,LR_COMPENSATE,extra,INVALID_PAGEID,&rctx.logRec.undoNext,NULL,0);
			}
			break;
		case LR_UPDATE:
			act=activeTx.find(rctx.logRec.txid); assert(act!=NULL); txid=act->txid; extra=rctx.logRec.getExtra(); 
			if (TxMgr::isMaster(extra)) {
				if (rctx.logRec.pageID==INVALID_PAGEID && ctx->theCB->logEnd>=lastLSN) {
					act->lastLSN=insert(ses,LR_COMPENSATE,extra,rctx.logRec.pageID,&rctx.logRec.undoNext,rctx.rbuf,rctx.lrec);
					RC rc=ctx->theCB->update(ctx,extra>>PGID_SHIFT,rctx.rbuf,rctx.lrec,true);
					if (rc!=RC_OK) report(MSG_ERROR,"LR_UPDATE undo: master page update failed: %d, LSN: " _LX_FM "\n",rc,lastLSN.lsn);
					ctx->theCB->logEnd=act->lastLSN;
				}
			} else if ((pageMgr=TxMgr::getPageMgr(extra,ctx))==NULL)
				report(MSG_ERROR,"Invalid PGID %d in recovery:undo, LSN: " _LX_FM "\n",extra&PGID_MASK,lastLSN.lsn);
			else if ((rctx.flags&LRC_LUNDO)!=0) {
				if (pb!=NULL) {pb->release(QMGR_UFORCE,ses); pb=NULL;}
				if ((rc=pageMgr->undo(extra>>PGID_SHIFT,rctx.rbuf,rctx.lrec,rctx.logRec.pageID))!=RC_OK)
					report(MSG_ERROR,"LR_UPDATE(lundo): update failed: %d, page: %08X, tx: " _LX_FM ", LSN: " _LX_FM "\n",rc,rctx.logRec.pageID,rctx.logRec.txid,lastLSN.lsn);
				act->lastLSN=insert(ses,LR_COMPENSATE,extra,INVALID_PAGEID,&rctx.logRec.undoNext,NULL,0);
			} else {
				pb=ctx->bufMgr->getPage(rctx.logRec.pageID,pageMgr,PGCTL_XLOCK|QMGR_UFORCE,pb,ses);
				if (pb==NULL)
					report(MSG_ERROR,"Cannot read page %08X in recovery:undo, LSN: " _LX_FM "\n",rctx.logRec.pageID,lastLSN.lsn);
				else if (pageMgr->getLSN(pb->getPageBuf(),lPage)>=lastLSN) {
					act->lastLSN=insert(ses,LR_COMPENSATE,extra,rctx.logRec.pageID,&rctx.logRec.undoNext,rctx.rbuf,rctx.lrec);
					RC rc=pageMgr->update(pb,lPage,extra>>PGID_SHIFT,rctx.rbuf,rctx.lrec,TXMGR_UNDO);
					if (rc!=RC_OK) report(MSG_ERROR,"%s undo: page %08X update failed: %d, LSN: " _LX_FM "\n",LR_Tab[rctx.type],rctx.logRec.pageID,rc,lastLSN.lsn);
					pageMgr->setLSN(act->lastLSN,pb->getPageBuf(),lPage); pb->setRedo(act->lastLSN); bool fMerge;
					if (pageMgr->multiPage(extra>>PGID_SHIFT,rctx.rbuf,rctx.lrec,fMerge)!=INVALID_PAGEID && !fMerge) pb->flushPage();		// ????
				}
			}
			break;
		case LR_CREATE:
			assert(!TxMgr::isMaster(rctx.logRec.getExtra()));
			act=activeTx.find(rctx.logRec.txid); assert(act!=NULL);	txid=act->txid;			// ?????
			act->lastLSN=insert(ses,LR_COMPENSATE2,0,rctx.logRec.pageID,&rctx.logRec.undoNext,rctx.rbuf,rctx.lrec);
			if (pb!=NULL && pb->getPageID()==rctx.logRec.pageID) {pb->release(PGCTL_DISCARD|QMGR_UFORCE,ses); pb=NULL;}
			else ctx->bufMgr->drop(rctx.logRec.pageID);
			break;
		case LR_DISCARD:
			extra=rctx.logRec.getExtra(); assert(!TxMgr::isMaster(extra));
			act=activeTx.find(rctx.logRec.txid); assert(act!=NULL);	txid=act->txid;			// ?????
			act->lastLSN=insert(ses,LR_COMPENSATE3,extra,rctx.logRec.pageID,&rctx.logRec.undoNext,rctx.rbuf,rctx.lrec);
			if ((pageMgr=TxMgr::getPageMgr(extra,ctx))==NULL)
				report(MSG_ERROR,"Invalid PGID %d in recovery:undo, LSN: " _LX_FM "\n",extra&PGID_MASK,lastLSN.lsn);
			else if ((pb=ctx->bufMgr->newPage(rctx.logRec.pageID,pageMgr,pb,0,ses))==NULL)
				report(MSG_ERROR,"%s undo: cannot re-create page %08X , LSN: " _LX_FM "\n",LR_Tab[rctx.type],rctx.logRec.pageID,ctx->logMgr->recv.lsn);
			else {
				pageMgr->setLSN(act->lastLSN,pb->getPageBuf(),lPage); pb->setRedo(act->lastLSN);
				if (rctx.rbuf!=NULL && rctx.lrec!=0) {
					RC rc=pageMgr->update(pb,lPage,extra>>PGID_SHIFT,rctx.rbuf,rctx.lrec,TXMGR_UNDO);
					if (rc!=RC_OK) report(MSG_ERROR,"%s undo: page %08X DROP failed: %d, LSN: " _LX_FM "\n",LR_Tab[rctx.type],rctx.logRec.pageID,rc,lastLSN.lsn);
				}
			}
			break;
		case LR_BEGIN:
			act=activeTx.find(rctx.logRec.txid); assert(act!=NULL);		// ?????
			txid=act->txid; insert(ses,LR_ABORT,0,INVALID_PAGEID,&act->lastLSN);
			activeTx.remove(act,true); losers.remove(rctx.logRec.txid,true); continue;
		case LR_ABORT:					// ????
		case LR_COMMIT:
			activeTx.remove(rctx.logRec.txid,true); losers.remove(rctx.logRec.txid,true); continue;
		default:
			report(MSG_ERROR,"Invalid logrec rctx.type %d in recovery:undo, LSN: " _LX_FM "\n",rctx.type,lastLSN.lsn); break;
		}
		undoTx->lastLSN=rctx.logRec.undoNext;
	}
#ifdef _DEBUG
	getTimestamp(endTime);
	report(MSG_DEBUG,"\tRecovery: undo pass finished, %.3fsec\n",double(endTime-startTime)/1000000.);
#endif
	if (pb!=NULL) pb->release(QMGR_UFORCE,ses);
	ctx->txMgr->setTXID(ctx->theCB->lastTXID);
	ses->flushLSN=0;
	checkpoint();

	losers.clear();
	dirtyPages.clear();
	activeTx.clear();

	rctx.release();
	fRecovery=false;

	return RC_OK;
}

RC LogMgr::rollback(Session *ses,bool fSavepoint)
{
	if (ses==NULL) return RC_NOSESSION; if ((ctx->mode&STARTUP_NO_RECOVERY)!=0) return RC_FALSE;
	RC rc=RC_OK; PageMgr *mgr; LogReadCtx rctx(this,ses); unsigned extra; LSN nullLSN(0),lastLSN(ses->tx.lastLSN);
#ifdef _DEBUG
	TXID txid=ses->getTXID();
#endif
	for (; rc==RC_OK && !lastLSN.isNull() && (!fSavepoint || lastLSN>ses->tx.next->lastLSN); lastLSN=rctx.logRec.undoNext) {
		LSN save(lastLSN),redo; PBlockP pb;
		if ((rc=rctx.read(lastLSN))!=RC_OK) {
			report(MSG_ERROR,"Rollback: cannot read UNDO record at " _LX_FM " (%d)\n",save.lsn,rc);
			break;
		}
		assert(rctx.logRec.txid==txid);
		switch (rctx.type) {
		default: break;		// ???????????????????????
		case LR_COMPENSATE: case LR_COMPENSATE2: case LR_COMPENSATE3: case LR_RESTORE: break;
		case LR_LUNDO:
			if ((mgr=TxMgr::getPageMgr(extra=rctx.logRec.getExtra(),ctx))==NULL)
				report(MSG_ERROR,"Invalid PGID %d in rollback:undo(LR_LUNDO), LSN: " _LX_FM "\n",extra&PGID_MASK,lastLSN.lsn);
			else if ((rc=mgr->undo(extra>>PGID_SHIFT,rctx.rbuf,rctx.lrec))!=RC_OK)
				report(MSG_ERROR,"Rollback: LUNDO failed: %d, LSN: " _LX_FM "\n",rc,lastLSN.lsn);
			else
				insert(ses,LR_COMPENSATE,extra,INVALID_PAGEID,&rctx.logRec.undoNext,NULL,0);
			break;
		case LR_CREATE:
			extra=rctx.logRec.getExtra(); assert(!TxMgr::isMaster(extra));
			insert(ses,LR_COMPENSATE2,0,rctx.logRec.pageID,&rctx.logRec.undoNext,rctx.rbuf,rctx.lrec);
			ctx->bufMgr->drop(rctx.logRec.pageID);
			if (ses!=NULL && (mgr=TxMgr::getPageMgr(extra,ctx))!=NULL && (mgr->getPGID()==PGID_HEAP||mgr->getPGID()==PGID_SSV)) 
				((HeapPageMgr*)mgr)->discardPage(rctx.logRec.pageID,ses);
			break;
		case LR_DISCARD:
			extra=rctx.logRec.getExtra(); assert(!TxMgr::isMaster(extra));
			redo=insert(ses,LR_COMPENSATE3,extra,rctx.logRec.pageID,&rctx.logRec.undoNext,rctx.rbuf,rctx.lrec);
			if ((mgr=TxMgr::getPageMgr(extra,ctx))==NULL)
				report(MSG_ERROR,"Invalid PGID %d in rollback:undo(LR_DISCARD), LSN: " _LX_FM "\n",extra&PGID_MASK,lastLSN.lsn);
			else if (pb.newPage(rctx.logRec.pageID,mgr,0,ses)==NULL)
				report(MSG_ERROR,"%s rollback: cannot re-create page %08X , LSN: " _LX_FM "\n",LR_Tab[rctx.type],rctx.logRec.pageID,lastLSN.lsn);
			else {
				if (rctx.rbuf!=NULL && rctx.lrec!=0) {
					rc=mgr->update(pb,lPage,extra>>PGID_SHIFT,rctx.rbuf,rctx.lrec,TXMGR_UNDO);
					if (rc!=RC_OK) report(MSG_ERROR,"Rollback: page %08X DROP failed: %d, LSN: " _LX_FM "\n",rctx.logRec.pageID,rc,lastLSN.lsn);
				}
				if (rc==RC_OK) {mgr->setLSN(redo,pb->getPageBuf(),lPage); pb->setRedo(redo);}
			}
			break;
		case LR_UPDATE:
			extra=rctx.logRec.getExtra();
			if (TxMgr::isMaster(extra)) {
				insert(ses,LR_COMPENSATE,extra,rctx.logRec.pageID,&rctx.logRec.undoNext,rctx.rbuf,rctx.lrec);
				rc=ctx->theCB->update(ctx,extra>>PGID_SHIFT,rctx.rbuf,rctx.lrec,true); break;
			}
			if ((mgr=TxMgr::getPageMgr(extra,ctx))==NULL)
				{report(MSG_ERROR,"Invalid PGID %d in rollback:undo(LR_UPDATE), LSN: " _LX_FM "\n",extra&PGID_MASK,lastLSN.lsn); break;}
			if ((rctx.flags&LRC_LUNDO)!=0) {
				if ((rc=mgr->undo(extra>>PGID_SHIFT,rctx.rbuf,rctx.lrec,rctx.logRec.pageID))!=RC_OK)
					report(MSG_ERROR,"Rollback: LR_UPDATE(lundo) page %08X failed: %d, LSN: " _LX_FM "\n",rctx.logRec.pageID,rc,lastLSN.lsn);
				else 
					insert(ses,LR_COMPENSATE,extra,INVALID_PAGEID,&rctx.logRec.undoNext,NULL,0);
				break;
			}
			if (pb.getPage(rctx.logRec.pageID,mgr,PGCTL_XLOCK,ses)==NULL) {
				report(MSG_ERROR,"Cannot read page %08X in rollback, transaction " _LX_FM "\n",rctx.logRec.pageID,rctx.logRec.txid);
				rc=RC_CORRUPTED; break;
			}
			redo=insert(ses,LR_COMPENSATE,extra,rctx.logRec.pageID,&rctx.logRec.undoNext,rctx.rbuf,rctx.lrec);
			if ((rc=mgr->update(pb,lPage,extra>>PGID_SHIFT,rctx.rbuf,rctx.lrec,TXMGR_UNDO))!=RC_OK)
				report(MSG_ERROR,"Rollback: page %08X update failed: %d, LSN: " _LX_FM "\n",rctx.logRec.pageID,rc,lastLSN.lsn);
			else {
				mgr->setLSN(redo,pb->getPageBuf(),lPage); pb->setRedo(redo);
			}
			break;
		}
	}
	lastLSN=insert(ses,LR_RESTORE,0,INVALID_PAGEID,fSavepoint?&ses->tx.next->lastLSN:&nullLSN);
	if (!fSavepoint) flushTo(lastLSN);
	return rc;
}

RC LogMgr::checkpoint()
{
	if ((ctx->mode&STARTUP_NO_RECOVERY)!=0) return RC_OK;
	bufferLock.lock(); RC rc=RC_OK; LSN start(~0ULL);
	if (!fRecovery && ctx->theCB->checkpoint==prevLSN) {bufferLock.unlock(); return RC_OK;}
	PageID asyncPages[MAX_ASYNC_PAGES]; unsigned nAsyncPages=0;
	LogDirtyPages *ldp=ctx->bufMgr->getDirtyPageInfo(maxLSN<logSegSize?LSN(0):maxLSN-logSegSize,
								start,asyncPages,nAsyncPages,sizeof(asyncPages)/sizeof(asyncPages[0]));
	LogActiveTransactions *lat=ctx->txMgr->getActiveTx(start);
	if (ldp==NULL || lat==NULL) {bufferLock.unlock(); return RC_NORESOURCES;}
	assert(!fRecovery || lat->nTransactions==0);
	size_t lAt=(size_t)lat->nTransactions*sizeof(LogActiveTransactions::LogActiveTx),lDp=(size_t)ldp->nPages*sizeof(LogDirtyPages::LogDirtyPage);
	size_t lChkp=sizeof(uint64_t)+lAt+sizeof(uint64_t)+lDp; byte *pData=(byte*)ctx->malloc(lChkp);
	if (pData==NULL) {rc=RC_NORESOURCES; bufferLock.unlock();}
	else {
		memcpy(pData,lat,+sizeof(uint64_t)+lAt);
		memcpy(pData+sizeof(uint64_t)+lAt,ldp,sizeof(uint64_t)+lDp);
		LSN chkp(insert(NULL,LR_CHECKPOINT,0,INVALID_PAGEID,NULL,pData,lChkp));
		if ((rc=flushTo(chkp))==RC_OK) ctx->theCB->checkpoint=chkp; 
		assert(ctx->fileMgr==NULL || LSNToFileOffset(maxLSN)<=(unsigned)ctx->fileMgr->getFileSize(logFile));
		bufferLock.unlock();
		if (rc==RC_OK && (rc=ctx->theCB->update(ctx))==RC_OK && ctx->fileMgr!=NULL) {
			unsigned fileN=LSNToFileN(start);   
			if (fileN>0 && (--fileN>prevTruncate || prevTruncate==~0u) && fileN<currentLogFile)   
				ctx->fileMgr->deleteLogFiles(prevTruncate=fileN,logDirectory,fArchive);   
		} 
		ctx->free(pData);
	}
	if (nAsyncPages>0) ctx->bufMgr->writeAsyncPages(asyncPages,nAsyncPages);
	ctx->free(ldp); ctx->free(lat);
	return rc;
}
