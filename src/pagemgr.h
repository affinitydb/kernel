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
 * PageMgr and VBlock interfaces
 * used by buffer manager to interact with page managers for specific page types
 */
#ifndef _PAGEMGR_H_
#define _PAGEMGR_H_

#include "lsn.h"

namespace AfyKernel
{

/**
 * page type enumeration
 */
enum PGID
{
	PGID_MASTER,						/**< master page - control page of the store */
	PGID_EXTENT,						/**< extent map page */
	PGID_FSPACE,						/**< free space descriptor page */
	PGID_EXTDIR,						/**< directory of extents page */
	PGID_FSPCDIR,						/**< directory of free space pages */
	PGID_HEAP,							/**< page contains PINs */
	PGID_HEAPDIR,						/**< directory page for heap pages */
	PGID_SSV,							/**< page contains Separately Stored Values (SSVs) or BLOBs */
	PGID_INDEX,							/**< B-tree index page */
	PGID_NETMGR,						/**< NetMgr B-tree page */
	PGID_ALL
};

/**
 * PageMgr interface
 * provides default implementations for all methods
 */
class PageMgr
{
public:
	virtual	void	initPage(byte *page,size_t lPage,PageID pid);
	virtual	bool	afterIO(class PBlock *,size_t lPage,bool fLoad);
	virtual	bool	beforeFlush(byte *page,size_t lPage,PageID pid);
	virtual	RC		update(class PBlock *,size_t len,ulong info,const byte *rec,size_t lrec,ulong flags,class PBlock *newp=NULL);
	virtual	PageID	multiPage(ulong info,const byte *rec,size_t lrec,bool& fMerge);
	virtual	RC		undo(ulong info,const byte *rec,size_t lrec,PageID=INVALID_PAGEID);
	virtual	LSN		getLSN(const byte *frame,size_t len) const;
	virtual	void	setLSN(LSN,byte *frame,size_t len);
	virtual	void	unchain(class PBlock *,class PBlock* =NULL);
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

};

#endif
