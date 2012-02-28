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

/**
 * data file page allocation/deallocation
 */
#ifndef _FSMGR_H_
#define _FSMGR_H_

#include "utils.h"
#include "txpage.h"

namespace AfyKernel
{

/**
 * free page bitmap constants
 */
#define BITSPERELT		(sizeof(uint32_t)*8)
#define	MAXBITNUMBER	0x0007FFFF
#define	RESETBIT		0x00080000
#define	EXTENTDIRMAGIC	0xEFAB
#define	FREEPAGEFLAG	0x80000000

/**
 * extent directory page descritptor
 * implements PageMgr interface
 */
class ExtentDirPage : public PageMgr
{
	friend	class	FSMgr;
	struct ExtentDirHeader {
		CRC			crc;
		uint32_t	pageID;
		uint16_t	magic;
		uint16_t	len;
		uint32_t	nslots;
	};
	struct ExtentDirFooter {
		CRC			crc;
		uint32_t	filler;
	};
	struct DirSlot {
		uint32_t	extentStart;
		uint32_t	nPages;
	};
	void	initPage(byte *page,size_t lPage,PageID pid);
	bool	afterIO(class PBlock *,size_t lPage,bool fLoad);
	bool	beforeFlush(byte *page,size_t lPage,PageID pid);
	PGID	getPGID() const;
	static	size_t	contentSize(size_t lPage) {return lPage - sizeof(ExtentDirHeader) - sizeof(ExtentDirFooter);}
	static	ulong	maxDirSlots(size_t lPage) {return ulong(lPage - sizeof(ExtentDirHeader) - sizeof(ExtentDirFooter))/sizeof(DirSlot);}
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
	ulong			lExtHdr;
	struct ExtentMapHeader {
		TxPageHeader	hdr;
		uint32_t		nPages;
	};
	ExtentMapPage(StoreCtx *ctx);
	void	initPage(byte *page,size_t lPage,PageID pid);
	bool	afterIO(class PBlock *,size_t lPage,bool fLoad);
	bool	beforeFlush(byte *frame,size_t len,PageID pid);
	RC		update(class PBlock *,size_t len,ulong info,const byte *rec,size_t lrec,ulong flags,class PBlock *newp=NULL);
	PGID	getPGID() const;
	uint32_t* getBMP(const ExtentMapHeader *emp) const {return (uint32_t*)((byte*)emp+lExtHdr);}
	bool	isFree(const ExtentMapHeader *emp,ulong bitN) const {return bitN>=emp->nPages?false:(getBMP(emp)[bitN/BITSPERELT]&1<<bitN%BITSPERELT)==0;}
	static	size_t	contentSize(size_t lPage) {return lPage - sizeof(ExtentMapHeader) - FOOTERSIZE;}
};

/**
 * in-memory descriptor of free space in heap pages
 */
class FreeSpacePage : public TxPage
{
	friend	class	FSMgr;
	struct FreeSpaceHeader {
		TxPageHeader	hdr;
		struct PageInfo {
			uint16_t	pgid;
			uint16_t	left;
			uint32_t	pageID;
		};
		uint32_t		nPages;
	};
	FreeSpacePage(StoreCtx *ctx) : TxPage(ctx) {}
	RC		update(class PBlock *,size_t len,ulong info,const byte *rec,size_t lrec,ulong flags,class PBlock *newp=NULL);
	PGID	getPGID() const;
	static	size_t	contentSize(size_t lPage) {return lPage - sizeof(FreeSpaceHeader) - FOOTERSIZE;}
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
		ulong				pos;
		ulong				nPages;
		ulong				nFreePages;
		ulong				maxContiguous;
		ulong				firstFree;
		ulong				state;
		ExtentInfo() : list(this),extentStart(INVALID_PAGEID),dirPage(INVALID_PAGEID),pos(0),nPages(0),nFreePages(0),maxContiguous(0),firstFree(0),state(0) {}
	};
	RWLock				lock;
	RWLock				txLock;
	ExtentInfo			**extentTable;
	ulong				lExtentTable;
	ulong				nExtents;
	ulong				slotsLeft;
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
	RC			allocPages(ulong nPages,PageID *buf,PBlock **pAllocPage=NULL);
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
