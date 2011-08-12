/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _TXPAGE_H_
#define _TXPAGE_H_

#include "pagemgr.h"

namespace MVStoreKernel
{

#define	CURRENT_VERSION	110

#define HMACSIZE	16
#define	IVSIZE		16

#define	FOOTERSIZE	HMACSIZE

struct TxPageHeader	
{
	uint8_t		IV[IVSIZE];
	LSN			lsn;
	uint32_t	pageID;
	uint16_t	pglen;
	uint8_t		pgid;
	uint8_t		version;
	size_t		length() const {return ((size_t)pglen-1&0xFFFF)+1;}
};

class TxPage : public PageMgr
{
protected:
	class	StoreCtx	*const ctx;
public:
	TxPage(class StoreCtx *c) : ctx(c) {}
	virtual	void	initPage(byte *page,size_t lPage,PageID pid);
	virtual	bool	afterIO(class PBlock *,size_t lPage,bool fLoad);
	virtual	bool	beforeFlush(byte *frame,size_t len,PageID pid);
			LSN		getLSN(const byte *frame,size_t len) const;
			void	setLSN(LSN lsn,byte *frame,size_t len);
};

};

#endif
