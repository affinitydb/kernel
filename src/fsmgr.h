/**************************************************************************************

Copyright © 2004-2013 GoPivotal, Inc. All rights reserved.

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
 * data file page allocation/deallocation
 */
#ifndef _FSMGR_H_
#define _FSMGR_H_

#include "utils.h"
#include "pagemgr.h"

namespace AfyKernel
{

/**
 * free page bitmap constants
 */
#define BITSPERELT		(sizeof(uint32_t)*8)
#define	MAXBITNUMBER	0x0007FFFF
#define	RESETBIT		0x00080000
#define	FREEPAGEFLAG	0x80000000

/**
 * extent directory page descritptor
 * implements PageMgr interface
 */
class ExtentDirPage : public PageMgr
{
	friend	class	FSMgr;
	struct DirSlot {
		uint32_t	extentStart;
		uint32_t	nPages;
	};
	ExtentDirPage(class StoreCtx *ct) : PageMgr(ct) {}
	bool	afterIO(class PBlock *,size_t lPage,bool fLoad);
	bool	beforeFlush(byte *page,size_t lPage,PageID pid);
	PGID	getPGID() const;
	static	size_t		contentSize(size_t lPage) {return lPage - sizeof(PageHeader) - FOOTERSIZE;}
	static	unsigned	maxDirSlots(size_t lPage) {return unsigned(lPage-sizeof(PageHeader)-FOOTERSIZE)/sizeof(DirSlot);}
};

#define	EXT_HDR_SIZE			(sizeof(TxPageHeader)+sizeof(uint32_t))

/**
 * extent map page descritptor
 * map page contains a bitmap of free pages in this extent
 * implements TxPage interface
 */
class ExtentMapPage : public TxPage
{
	friend	class	FSMgr;
	unsigned		lExtHdr;
	ExtentMapPage(StoreCtx *ctx);
	void	initPage(byte *page,size_t lPage,PageID pid);
	bool	afterIO(class PBlock *,size_t lPage,bool fLoad);
	bool	beforeFlush(byte *frame,size_t len,PageID pid);
	RC		update(class PBlock *,size_t len,unsigned info,const byte *rec,size_t lrec,unsigned flags,class PBlock *newp=NULL);
	PGID	getPGID() const;
	uint32_t* getBMP(const TxPageHeader *emp) const {return (uint32_t*)((byte*)emp+lExtHdr);}
	bool	isFree(const TxPageHeader *emp,unsigned bitN) const {return bitN>=emp->nPages?false:(getBMP(emp)[bitN/BITSPERELT]&1<<bitN%BITSPERELT)==0;}
	static	size_t	contentSize(size_t lPage) {return lPage - sizeof(TxPageHeader) - FOOTERSIZE;}
};

/**
 * in-memory descriptor of free space in heap pages
 */
class FreeSpacePage : public TxPage
{
	friend	class	FSMgr;
	struct PageInfo {
		uint16_t	pgid;
		uint16_t	left;
		uint32_t	pageID;
	};
	FreeSpacePage(StoreCtx *ctx) : TxPage(ctx) {}
	RC		update(class PBlock *,size_t len,unsigned info,const byte *rec,size_t lrec,unsigned flags,class PBlock *newp=NULL);
	PGID	getPGID() const;
	static	size_t	contentSize(size_t lPage) {return lPage - sizeof(TxPageHeader) - FOOTERSIZE;}
};

class PBlock;

#define	EXTMAP_READ		0x0001
#define	EXTMAP_MODIFIED	0x0002
#define	EXTMAP_BAD		0x0004

/**
 * database free space manager
 * controls extent allocation
 */
class FSMgr
{
	struct ExtentInfo {
		HChain<ExtentInfo>	list;
		PageID				extentStart;
		PageID				dirPage;
		unsigned			pos;
		unsigned			nPages;
		unsigned			nFreePages;
		unsigned			maxContiguous;
		unsigned			firstFree;
		unsigned			state;
		ExtentInfo() : list(this),extentStart(INVALID_PAGEID),dirPage(INVALID_PAGEID),pos(0),nPages(0),nFreePages(0),maxContiguous(0),firstFree(0),state(0) {}
	};
	RWLock				lock;
	RWLock				txLock;
	ExtentInfo			**extentTable;
	unsigned			lExtentTable;
	unsigned			nExtents;
	unsigned			slotsLeft;
	HChain<ExtentInfo>	extentList;
	FileID				dataFile;
	ExtentMapPage		extentMapPage;
	ExtentDirPage		extentDirPage;
	FreeSpacePage		freeSpacePage;
	class StoreCtx		*const ctx;
public:
	FSMgr(class StoreCtx*);
	virtual ~FSMgr();
	void		*operator new(size_t s,StoreCtx *ctx);
	RC			init(FileID);
	RC			create(FileID);
	void		close();

	PBlock		*getNewPage(PageMgr *mgr);
	RC			allocPages(unsigned nPages,PageID *buf,PBlock **pAllocPage=NULL);
	RC			reservePage(PageID pid);
	PBlock		*getNewPage(PageMgr *mgr,PageID pad);
	bool		isFreePage(PageID pid);
	RC			freePage(PageID pid);
	RC			freeTxPages(const PageSet& ps);
	void		txUnlock() {txLock.unlock();}

private:
	RC			allocNewExtent(ExtentInfo*&ext,PBlock*&pb,bool fForce=false);
	ExtentInfo	*findExtent(PageID,bool=false);
	PBlock		*getExtentMapPage(ExtentInfo *ext,PBlock *pb);
	friend class ExtentMapPage;
};

};

#endif
