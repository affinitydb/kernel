/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _BUFFER_H_
#define _BUFFER_H_

#include "utils.h"
#include "pagemgr.h"
#include "qmgr.h"

#define	PAGE_HASH_SIZE		0x0400

#define	BLOCK_NEW_PAGE		0x0001
#define	BLOCK_IO_READ		0x0002
#define	BLOCK_IO_WRITE		0x0004
#define BLOCK_DIRTY			0x0008
#define	BLOCK_ASYNC_IO		0x0010
#define	BLOCK_REDO_SET		0x0020
#define	BLOCK_DISCARDED		0x0040
#define	BLOCK_TEMP			0x0080
#define	BLOCK_FLUSH_CHAIN	0x0100

#define	PGCTL_XLOCK			0x0001
#define	PGCTL_ULOCK			0x0002
#define	PGCTL_COUPLE		0x0004
#define	PGCTL_DISCARD		0x0008
#define	PGCTL_RLATCH		0x0010
#define	PGCTL_NOREL			0x0020
#define	PGCTL_INREL			0x0040

#define MAX_ASYNC_PAGES		32
#define	FLUSH_CHAIN_THR		12
#define	MIN_BUFFERS			8

namespace MVStoreKernel
{

class Session;
struct LogDirtyPages;
struct DirtyPageInfo
{
	PageID		pageID;
	LSN			redoLSN;
};

class PBlock
{
	typedef QElt<PBlock,PageID,PageID> QEPB;
	friend	class	BufMgr;
	PageID			pageID;
	volatile long	state;
	byte * const	frame;
	PageMgr*		pageMgr;
	LSN				redoLSN;
	struct myaio	*aio;
	QEPB			*QE;
	PBlock			*dependent;
	SharedCounter	dependCnt;
	VBlock			*vb;
	class  BufMgr	*mgr;
	HChain<PBlock>	pageList;
	HChain<PBlock>	flushList;
private:
	PBlock(class BufMgr *bm,byte *frm,struct myaio *ai=NULL);
	void			setStateBits(ulong v) {setStateBits(v,v);}
	void			resetStateBits(ulong v) {setStateBits(0,v);}
	void			setStateBits(ulong v,ulong mask) {for (long s=state; !cas(&state,s,long(s&~mask|v)); s=state) ;}
	RC				flushBlock();
	RC				readResult(RC rc);
	void			writeResult(RC rc);
	bool			setaio();
	void			fillaio(int,void (*callback)(void*,RC)) const;
public:
	~PBlock();
	void			release(ulong mode=0,Session *ses=NULL);
	PageID			getPageID() const {return pageID;}
	byte			*getPageBuf() const {return frame;}
	PageMgr			*getPageMgr() const {return pageMgr;}
	bool			isFirstPageOp() const {return (state&(BLOCK_NEW_PAGE|BLOCK_REDO_SET))==BLOCK_NEW_PAGE;}
	const LSN&		getRedoLSN() const {return redoLSN;}
	void			setRedo(LSN lsn);
	void			setDependency(PBlock *dp) {assert(dp!=NULL && dp->isXLocked() && dp->dependent==NULL && isXLocked()); dp->dependent=this; ++dependCnt;}
	void			removeDependency(PBlock *dp) {assert(dp!=NULL && dp->isXLocked() && isXLocked()); if (dp->dependent!=NULL) {assert(dp->dependent==this); --dependCnt; dp->dependent=NULL;}}
	PBlock			*getDependent() const {return dependent;}
	bool			isDependent() const {return dependCnt!=0;}
	bool			isWritable() const {return QE!=NULL && QE->isXLocked() && (state&(BLOCK_IO_READ|BLOCK_IO_WRITE|BLOCK_ASYNC_IO))==0;}
	bool			isULocked() const {return QE!=NULL && QE->isULocked();}
	bool			isXLocked() const {return QE!=NULL && QE->isXLocked();}
	void			upgradeLock() {if (QE!=NULL) QE->upgradeLock(RW_X_LOCK);}
	bool			tryupgrade() {return QE==NULL||QE->tryupgrade();}
	void			downgradeLock(RW_LockType lt) {if (QE!=NULL) QE->downgradeLock(lt);}
	void			flushPage() {assert(isXLocked() && !isDependent()); flushBlock();}
	VBlock			*getVBlock() const {return vb;}
	void			setVBlock(VBlock *v) {vb=v;}
	void			checkDepth();

	void			setKey(PageID pid,void *mg);
	QEPB			*getQE() const {return QE;}
	void			setQE(QEPB *qe) {QE=qe;}
	bool			isDirty() const {return (state&BLOCK_DIRTY)!=0;}
	RW_LockType		lockType(RW_LockType);
	RC				load(PageMgr*,ulong);
	bool			save();
	void			saveAsync();
	void			destroy();
	void			initNew();
	static	PBlock*	createNew(PageID pid,void *mg);
	static	void	waitResource(void *mg);
	static	void	signal(void *mg);
};

class PBlockP
{
	PBlock			*pb;
	ulong			flags;
public:
	PBlockP(PBlock *p,ulong flg) : pb(p),flags(flg) {}
	PBlockP(PBlockP& pbp) : pb(pbp.pb),flags(pbp.flags) {pbp.flags|=PGCTL_NOREL;}
	PBlockP() : pb(NULL),flags(0) {}
	~PBlockP() {if (pb!=NULL && (flags&PGCTL_NOREL)==0) {flags|=PGCTL_INREL; pb->release(flags&(QMGR_UFORCE|PGCTL_DISCARD));}}
	void release(Session *s=NULL) {if (pb!=NULL && (flags&PGCTL_NOREL)==0) {flags|=PGCTL_INREL; pb->release(flags&(QMGR_UFORCE|PGCTL_DISCARD),s);} pb=NULL; flags=0;}
	void moveTo(PBlockP& to) {if (to.pb!=NULL && (to.flags&PGCTL_NOREL)==0) {to.flags|=PGCTL_INREL; to.pb->release(to.flags&(QMGR_UFORCE|PGCTL_DISCARD));} to.pb=pb; to.flags=flags; pb=NULL; flags=0;}
	PBlock *operator=(PBlock *p) {pb=p; flags=0; return p;}
	void set(ulong flg) {flags|=flg;}
	void reset(ulong flg) {flags&=~flg;}
	operator PBlock*() const {return pb;}
	PBlock* operator->() const {return pb;}
	bool isNull() const {return pb==NULL;}
	bool isSet(ulong flg) const {return (flags&flg)!=0;}
	ulong uforce() const {return flags&QMGR_UFORCE;}
	PBlock*	newPage(PageID pid,PageMgr*,ulong flags=0,Session *ses=NULL);
	PBlock*	getPage(PageID pid,PageMgr*,ulong flags=0,Session *ses=NULL);
};

typedef QMgr<PBlock,PageID,PageID,PageMgr*,SERVER_HEAP> BufQMgr;

class BufMgr : public BufQMgr
{
	class	StoreCtx *const	ctx;
	const	size_t			lPage;
	const	ulong			nStoreBuffers;
	mutable	Mutex			pageLock;
	HChain<PBlock>			pageList;
	mutable	Mutex			flushLock;
	HChain<PBlock>			flushList;
	ulong					dirtyCount;
	SharedCounter			asyncWriteCount;
	SharedCounter			asyncReadCount;
	ulong					maxDepDepth;

	static ulong			nBuffers;
	static ulong			xBuffers;
	static volatile	long	nStores;
	static SLIST_HEADER		freeBuffers;
	static Mutex			initLock;
	static bool				fInit;
public:
	BufMgr(class StoreCtx *ct,int initNumberOfBlocks,size_t lpage);
	~BufMgr();
	void *operator new(size_t s,StoreCtx *ctx);
	RC					init();
	RC					flushAll(uint64_t timeout);
	size_t				getPageSize() const {return lPage;}
	PBlock*				newPage(PageID pid,PageMgr*,PBlock *old=NULL,ulong flags=0,Session *ses=NULL);
	PBlock*				getPage(PageID pid,PageMgr*,ulong flags=0,PBlock *old=NULL,Session *ses=NULL);
	void				prefetch(const PageID *pages,int nPages,PageMgr *mgr,PageMgr *const *mgrs=NULL);
	void				asyncWrite();
	RC					close(FileID fid,bool fAll=false);
	void				writeAsyncPages(const PageID *asyncPages,ulong nAsyncPages);
	LogDirtyPages		*getDirtyPageInfo(LSN old,LSN& redo,PageID *asyncPages,ulong& nAsyncPages,ulong maxAsyncPages);
#ifdef _DEBUG
	void				checkState();
#endif
private:
	static	void		asyncReadNotify(void*,RC);
	static	void		asyncWriteNotify(void*,RC);
	friend	class		PBlock;
	friend	class		Session;
};

};

#endif
