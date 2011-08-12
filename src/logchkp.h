/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _LOGCHKP_H_
#define _LOGCHKP_H_

#include "types.h"
#include "lsn.h"


namespace MVStoreKernel
{

struct LogDirtyPages
{
	uint32_t		nPages;
	struct LogDirtyPage {
		uint64_t	pageID;
		LSN			redo;
	}				pages[1];	
};

struct LogActiveTransactions
{
	uint32_t		nTransactions;
	struct LogActiveTx {
		TXID		txid;
		LSN			lastLSN;
		LSN			firstLSN;
	}				transactions[1];
};

};

#endif
