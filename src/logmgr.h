/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _LOGMGR_H_
#define _LOGMGR_H_

#include "txmgr.h"
#include "buffer.h"

#define	MINSEGSIZE			0x10000
#define	MAXLOGRECSIZE		0x100000ul		// 1Mb
#define	INVALIDLOGFILE		(~0ul)
#define	LOGFILETHRESHOLD	0.75			// time to allocate new log file(s)
#define CHECKPOINTTHRESHOLD	0x1000			// records between checkpoints
#define	MAXPREVLOGSEGS		6				// max number of previous log segments open simultaneously
#define	LOGRECLENMASK		0x1FFFFFFul
#define	LOGRECFLAGSSHIFT	25

namespace AfyKernel
{

enum LRType {LR_UPDATE=1, LR_CREATE, LR_BEGIN, LR_COMMIT, LR_ABORT, LR_COMPENSATE, LR_RESTORE, 
			LR_CHECKPOINT, LR_FLUSH, LR_SHUTDOWN, LR_SESSION, LR_LUNDO, LR_COMPENSATE2, 
			LR_DISCARD, LR_COMPENSATE3, LR_ALL};

#define	LRC_LUNDO	0x01

struct LogRec
{
	uint32_t	len1;						// length of redo/undo data
	uint32_t	info;						// type, PGID & extra info
	TXID		txid;						// transaction ID
	LSN			undoNext;					// next log record in undo chain
	PageID		pageID;						// page ID
	uint8_t		hmac[HMAC_SIZE];			// HMAC for this record

	LRType		getType() const {return (LRType)(info&0x0F);}
	ulong		getExtra() const {return info>>4;}
	ulong		getFlags() const {return len1>>LOGRECFLAGSSHIFT;}
	uint32_t	getLength() const {return len1&LOGRECLENMASK;}
	void		setInfo(LRType type,ulong extra) {info=extra<<4|type;}
	void		setLength(uint32_t l,uint32_t flags) {len1=flags<<LOGRECFLAGSSHIFT|l&LOGRECLENMASK;}
};

struct PrevLogSeg
{
	ulong	logFile;
	FileID	fid;
	long	nReads;
};

class LogMgr
{
	class	StoreCtx	*const ctx;
	mutable	RWLock 		lock;
	mutable	Mutex		bufferLock;
	mutable	RWLock		maxLSNLock;

	const	size_t		sectorSize;
	const	size_t		lPage;
	const	size_t		logSegSize;
	const	size_t		bufLen;
	byte				*logBufBeg;
	byte				*logBufEnd;
	byte				*ptrWrite;
	byte				*ptrInsert;
	byte				*ptrRead;
	LSN					maxLSN;
	LSN					minLSN;
	LSN					prevLSN;
	LSN					writtenLSN;
	LSN					wrapLSN;
	bool				fFull;
	bool				fWriting;
	bool				fRecovery;
	bool				fAnalizing;
	size_t				recFileSize;
	ulong				maxAllocated;
	ulong				prevTruncate;
	ulong				nRecordsSinceCheckpoint;
	SharedCounter		nOverflow;
	SharedCounter		nWrites;
	
	TXID				txid;
	LSN					recv;
	PBlock				*newPage;

	mutable	Mutex		openFile;
	ulong				currentLogFile;
	FileID				logFile;
	PrevLogSeg			readLogSegs[MAXPREVLOGSEGS];
	int					nReadLogSegs;
	Event				waitLogSeg;
	struct myaio*		pcb;
	const	bool		fArchive;
	bool				fReadFromCurrent;
	char				*logDirectory;
	Mutex				initLock;
	volatile	bool	fInit;

	class CheckpointRQ	: public Request
	{
		LogMgr			*const	mgr;
	public:
		CheckpointRQ(LogMgr *mg) : mgr(mg) {}
		void process();
		void destroy();
	}					checkpointRQ;
	friend class		CheckpointRQ;
	class SegAllocRQ	: public Request
	{
		LogMgr			*const	mgr;
	public:
		SegAllocRQ(LogMgr *mg) : mgr(mg) {}
		void process();
		void destroy();
	}					segAllocRQ;
	friend class		SegAllocRQ;

public:
						LogMgr(class StoreCtx*,size_t logBufS,bool fArchiveLogs=false,const char *logDir=NULL);
						~LogMgr();
	void *operator		new(size_t s,StoreCtx *ctx) {void *p=ctx->malloc(s); if (p==NULL) throw RC_NORESOURCES; return p;}
	void				deleteLogs();
	RC					init();
	RC					allocLogFile(ulong fileN,char* buf=NULL);
	RC					flushTo(LSN lsn,LSN* =NULL);
	LSN					insert(Session *,LRType type,ulong extra=0,PageID pid=INVALID_PAGEID,const LSN *undoNext=NULL,const void *pData=NULL,
																				size_t lData=0,uint32_t flags=0,PBlock *pb=NULL,PBlock *pb2=NULL);
	LSN					getRecvLSN() const {return recv;}
	RC					rollback(Session *,bool fSavepoint);
	LSN					getOldLSN() const {RWLockP lck(&maxLSNLock,RW_S_LOCK); return maxLSN<logSegSize?LSN(0):maxLSN-logSegSize;}
	PBlock				*setNewPage(PBlock *newp) {return newPage=newp;}
	bool				isRecovery() const {return fRecovery;}
	bool				isInit() const {return fInit;}
	RC					recover(Session *ses,bool fRollforward);
	RC					close();
private:
	RC					initLogBuf() {return logBufBeg!=NULL?RC_OK:(ptrInsert=ptrWrite=logBufBeg=(byte*)allocAligned(bufLen,lPage))==NULL?RC_NORESOURCES:(ptrRead=logBufEnd=logBufBeg+bufLen,RC_OK);}
	RC					createLogFile(LSN fileStart,off64_t& fSize);
	RC					openLogFile(LSN fileStart);
	RC					write();
	RC					checkpoint();
	char				*getLogFileName(ulong logFileN,char *buf) const;
	LSN					LSNFromOffset(ulong fileN,size_t offset) {return off64_t(fileN)*logSegSize+offset;}
	ulong				LSNToFileN(LSN lsn) {return ulong(lsn.lsn/logSegSize);}
	size_t				LSNToFileOffset(LSN lsn) {return lsn.lsn%logSegSize;}
	friend	class		LogReadCtx;
};

class LogReadCtx
{
	LogMgr	*const		logMgr;
	Session	*const		ses;
	ulong				currentLogSeg;
	FileID				fid;
	PBlockP				pb;
	const	byte		*ptr;
	size_t				len;
	bool				fCheck;
	bool				fLocked;
	size_t				xlrec;
	RC					readChunk(LSN lsn,void *buf,size_t l);
	void				finish();
public:
	LogRec				logRec;
	LRType				type;
	ulong				flags;
	byte				*rbuf;
	size_t				lrec;
public:
	LogReadCtx(LogMgr *mgr,Session *s);
	~LogReadCtx();
	RC					read(LSN& lsn);
	void				closeFile();
	void				release();
};


};

#endif
