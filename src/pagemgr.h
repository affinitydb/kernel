/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _PAGEMGR_H_
#define _PAGEMGR_H_

#include "lsn.h"

namespace AfyKernel
{

enum PGID
{
	PGID_MASTER, PGID_EXTENT, PGID_FSPACE, PGID_EXTDIR, PGID_FSPCDIR, PGID_HEAP, PGID_HEAPDIR, PGID_SSV, PGID_INDEX, PGID_NETMGR, PGID_ALL
};

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

class VBlock
{
public:
	virtual void	release() = 0;
};

};

#endif
