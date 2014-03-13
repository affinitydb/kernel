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

/**
 * Database pages memory caching control structures
 */
#ifndef _BUFFER_H_
#define _BUFFER_H_

#include "utils.h"
#include "pagemgr.h"
#include "qmgr.h"

#define	PAGE_HASH_SIZE		0x0400				/**< size of the page hash table */

#define	BLOCK_NEW_PAGE		0x0001				/**< page is just created, not saved to disk yet */
#define	BLOCK_IO_READ		0x0002				/**< page is being read from disk */
#define	BLOCK_IO_WRITE		0x0004				/**< page is being written to disk */
#define BLOCK_DIRTY			0x0008				/**< page is modified but not saved to persistemt memory yet */
#define	BLOCK_ASYNC_IO		0x0010				/**< asynchronous i/o operation (in conjunction with BLOCK_IO_READ or BLOCK_IO_WRITE */
#define	BLOCK_REDO_SET		0x0020				/**< redo LSN is set */
#define	BLOCK_DISCARDED		0x0040				/**< page is discarded, e.g. when index tree pages are merged */
#define	BLOCK_TEMP			0x0080				/**< temporary page, will be discarded at the end of transaction */
#define	BLOCK_FLUSH_CHAIN	0x0100				/**< flush chaining flag for pages with dependencies (e.g. split index tree pages) */

#define	PGCTL_XLOCK			0x0001				/**< exclusive page access request */
#define	PGCTL_ULOCK			0x0002				/**< 'update' page access request */
#define	PGCTL_COUPLE		0x0004				/**< page 'coupling': old page is released when access to the new one is acquired */
#define	PGCTL_DISCARD		0x0008				/**< request to discard a page */
#define	PGCTL_RLATCH		0x0010				/**< heap page latching synchronization (used in PBlockP::flags) */
#define	PGCTL_NOREL			0x0020				/**< don't release page (used in PBlockP::flags) */
#define	PGCTL_INREL			0x0040				/**< page being released by PBlockP */

#define MAX_ASYNC_PAGES		32					/**< maximum number of pages being asynchronously saved to disk */
#define	FLUSH_CHAIN_THR		32					/**< when dependency chain reaches this length, page flushing starts automatically */
#define	MIN_BUFFERS			8					/**< minimum number of page buffers in memory */

namespace AfyKernel
{

class Session;
struct LogDirtyPages;
struct DirtyPageInfo
{
	PageID		pageID;
	LSN			redoLSN;
};

/**
 * buffer page descriptor
 */
class PBlock
{
	typedef QElt<PBlock,PageID,PageID> QEPB;
	friend	class	BufMgr;
	PageID			pageID;
	volatile long	state;
	byte	* const	frame;
	PageMgr*		pageMgr;
	LSN				redoLSN;
	struct myaio	*aio;
	QEPB			*QE;
	PBlock* volatile dependent;
	SharedCounter	dependCnt;
	VBlock			*vb;
	class  BufMgr	*mgr;
	HChain<PBlock>	pageList;
	HChain<PBlock>	flushList;
	HChain<PBlock>	depList;
private:
	PBlock(class BufMgr *bm,byte *frm,struct myaio *ai=NULL);
	void			setStateBits(unsigned v) {setStateBits(v,v);}
	void			resetStateBits(unsigned v) {setStateBits(0,v);}
	void			setStateBits(unsigned v,unsigned mask) {for (long s=state; !cas(&state,s,long(s&~mask|v)); s=state) ;}
	RC				flushBlock();
	RC				readResult(RC rc,bool fAsync=false);
	void			writeResult(RC rc,bool fAsync=false);
	void			fillaio(int,void (*callback)(void*,RC,bool)) const;
public:
	~PBlock();
	void			release(unsigned mode=0,Session *ses=NULL);
	PageID			getPageID() const {return pageID;}
	byte			*getPageBuf() const {return frame;}
	PageMgr			*getPageMgr() const {return pageMgr;}
	bool			isFirstPageOp() const {return (state&(BLOCK_NEW_PAGE|BLOCK_REDO_SET))==BLOCK_NEW_PAGE;}
	void			resetNewPage() {resetStateBits(BLOCK_NEW_PAGE);}
	const LSN&		getRedoLSN() const {return redoLSN;}
	void			setRedo(LSN lsn);
	void			setDependency(PBlock *dp);
	void			removeDependency(PBlock *dp);
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
	RC				load(PageMgr*,unsigned);
	bool			save();
	void			saveAsync();
	void			destroy();
	void			initNew();
	static	PBlock*	createNew(PageID pid,void *mg);
	static	void	waitResource(void *mg);
	static	void	signal(void *mg);
};

/**
 * holder of a reference to page descriptor
 * when goes out of scope page is automatically released
 */
class PBlockP
{
	PBlock			*pb;
	unsigned		flags;
public:
	PBlockP(PBlock *p,unsigned flg) : pb(p),flags(flg) {}
	PBlockP(PBlockP& pbp) : pb(pbp.pb),flags(pbp.flags) {pbp.flags|=PGCTL_NOREL;}
	PBlockP() : pb(NULL),flags(0) {}
	~PBlockP() {if (pb!=NULL && (flags&PGCTL_NOREL)==0) {flags|=PGCTL_INREL; pb->release(flags&(QMGR_UFORCE|PGCTL_DISCARD));}}
	void release(Session *s=NULL) {if (pb!=NULL && (flags&PGCTL_NOREL)==0) {flags|=PGCTL_INREL; pb->release(flags&(QMGR_UFORCE|PGCTL_DISCARD),s);} pb=NULL; flags=0;}
	void moveTo(PBlockP& to) {if (to.pb!=NULL && (to.flags&PGCTL_NOREL)==0) {to.flags|=PGCTL_INREL; to.pb->release(to.flags&(QMGR_UFORCE|PGCTL_DISCARD));} to.pb=pb; to.flags=flags; pb=NULL; flags=0;}
	PBlock *operator=(PBlock *p) {pb=p; flags=0; return p;}
	void set(unsigned flg) {flags|=flg;}
	void reset(unsigned flg) {flags&=~flg;}
	operator PBlock*() const {return pb;}
	PBlock* operator->() const {return pb;}
	bool isNull() const {return pb==NULL;}
	bool isSet(unsigned flg) const {return (flags&flg)!=0;}
	unsigned uforce() const {return flags&QMGR_UFORCE;}
	PBlock*	newPage(PageID pid,PageMgr*,unsigned flags=0,Session *ses=NULL);
	PBlock*	getPage(PageID pid,PageMgr*,unsigned flags=0,Session *ses=NULL);
};

typedef QMgr<PBlock,PageID,PageID,PageMgr*> BufQMgr;

/**
 * Buffer manager
 * @see also QMgr
 */
class BufMgr : public BufQMgr
{
	class	StoreCtx *const	ctx;
	const	size_t			lPage;
	const	unsigned		nStoreBuffers;
	const	bool			fInMem;
	const	bool			fRT;
	RWLock					pageQLock;
	HChain<PBlock>			pageList;
	RWLock					flushQLock;
	HChain<PBlock>			flushList;
	RWLock					depQLock;
	HChain<PBlock>			depList;
	SharedCounter			dirtyCount;
	SharedCounter			asyncWriteCount;
	SharedCounter			asyncReadCount;
	RWLock					flushLock;

	static unsigned			nBuffers;
	static unsigned			xBuffers;
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
	PBlock*				newPage(PageID pid,PageMgr*,PBlock *old=NULL,unsigned flags=0,Session *ses=NULL);
	PBlock*				getPage(PageID pid,PageMgr*,unsigned flags=0,PBlock *old=NULL,Session *ses=NULL);
	void				prefetch(const PageID *pages,int nPages,PageMgr *mgr,PageMgr *const *mgrs=NULL);
	void				asyncWrite();
	RC					close(FileID fid,bool fAll=false);
	void				writeAsyncPages(const PageID *asyncPages,unsigned nAsyncPages);
	LogDirtyPages		*getDirtyPageInfo(LSN old,LSN& redo,PageID *asyncPages,unsigned& nAsyncPages,unsigned maxAsyncPages);
#ifdef _DEBUG
	void				checkState();
#endif
private:
	static	void		asyncReadNotify(void*,RC,bool);
	static	void		asyncWriteNotify(void*,RC,bool fAsync);
	friend	class		PBlock;
	friend	class		Session;
};

};

#endif
