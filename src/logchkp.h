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
 * log file checkpoint structure descriptor
 * information communicated between transaction manager, page buffer and log manager
 */
#ifndef _LOGCHKP_H_
#define _LOGCHKP_H_

#include "types.h"
#include "lsn.h"

namespace AfyKernel
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
