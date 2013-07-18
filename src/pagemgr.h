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
 * PageMgr and VBlock interfaces
 * used by buffer manager to interact with page managers for specific page types
 */
#ifndef _PAGEMGR_H_
#define _PAGEMGR_H_

#include "lsn.h"
#include "crypt.h"

namespace AfyKernel
{

#define	CURRENT_VERSION	111

/**
 * page type enumeration
 */
enum PGID
{
	PGID_MASTER,				/**< master page - control page of the store */
	PGID_EXTENT,				/**< extent map page */
	PGID_FSPACE,				/**< free space descriptor page */
	PGID_EXTDIR,				/**< directory of extents page */
	PGID_FSPCDIR,				/**< directory of free space pages */
	PGID_HEAP,					/**< page contains PINs */
	PGID_HEAPDIR,				/**< directory page for heap pages */
	PGID_SSV,					/**< page contains Separately Stored Values (SSVs) or BLOBs */
	PGID_INDEX,					/**< B-tree index page */
	PGID_NETMGR,				/**< NetMgr B-tree page */
	PGID_ALL
};

/**
 * generic page header
 */
struct PageHeader	
{
	uint8_t		IV[IVSIZE];		/**< inital random vector (for page encryption) */
	uint32_t	pageID;			/**< page ID */
	uint16_t	pglen;			/**< page length (same for all pages in the store) */
	uint8_t		pgid;			/**< PageMgr ID for this page (see pagemgr.h) */
	uint8_t		version;		/**< page format version (backward compatibility) */
	uint32_t	redirect;		/**< page ID of a heap page corresponding to this page */
	uint32_t	nPages;			/**< filler (used by directory pages to store number of pages) */
	size_t		length() const {return ((size_t)pglen-1&0xFFFF)+1;}	/**< length 0x10000 is stored as 0, this function returns correct length */
};

#define	FOOTERSIZE	HMACSIZE	/**< space reserved at the end of page */

/**
 * PageMgr interface
 * provides default implementations for all methods
 */
class PageMgr
{
protected:
	class	StoreCtx	*const ctx;
public:
	PageMgr(class StoreCtx *c) : ctx(c) {}
	virtual	void	initPage(byte *page,size_t lPage,PageID pid);
	virtual	bool	afterIO(class PBlock *,size_t lPage,bool fLoad);
	virtual	bool	beforeFlush(byte *page,size_t lPage,PageID pid);
	virtual	RC		update(class PBlock *,size_t len,unsigned info,const byte *rec,size_t lrec,unsigned flags,class PBlock *newp=NULL);
	virtual	PageID	multiPage(unsigned info,const byte *rec,size_t lrec,bool& fMerge);
	virtual	RC		undo(unsigned info,const byte *rec,size_t lrec,PageID=INVALID_PAGEID);
	virtual	LSN		getLSN(const byte *frame,size_t len) const;
	virtual	void	setLSN(LSN,byte *frame,size_t len);
	virtual	PGID	getPGID() const;
	void	*operator new(size_t s,class StoreCtx *ctx);
};

/**
 * VBlock interface for transient versioning of heap pages
 */
class VBlock
{
public:
	virtual void	release() = 0;
};

/**
 * transactinal page header
 */
struct TxPageHeader	: public PageHeader
{
	LSN			lsn;			/**< LSN of last page modification operation */
};

/**
 * partial implementation of PageMgr interface for transactional pages
 */
class TxPage : public PageMgr
{
public:
	TxPage(class StoreCtx *c) : PageMgr(c) {}
	virtual	void	initPage(byte *page,size_t lPage,PageID pid);
			LSN		getLSN(const byte *frame,size_t len) const;
			void	setLSN(LSN lsn,byte *frame,size_t len);
};

};

#endif
