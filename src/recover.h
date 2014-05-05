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

Written by Mark Venguerov 2004-2014

**************************************************************************************/

/**
 * structures used during recovery
 */
#ifndef _RECOVER_H_
#define _RECOVER_H_

#include "buffer.h"
#include "logmgr.h"
#include "utils.h"

#define LOSERSETSIZE		2000		/**< initial size of hash table for 'loser' transactions */
#define ACTIVESETSIZE		2000		/**< initial size of hash table for active transactions */
#define DIRTYPAGESETSIZE	3000		/**< initial size of dirty page hash table */

namespace AfyKernel
{

class TXIDKey
{
	TXID			txid;
public:
	TXIDKey(TXID tx) : txid(tx) {}
	operator uint32_t() const {return uint32_t(txid>>32)^uint32_t(txid);}
	bool operator==(TXIDKey key2) const {return txid==key2.txid;}
	bool operator!=(TXIDKey key2) const {return txid!=key2.txid;}
};

/**
 * recovery transaction bookkeeping structure
 */
struct Trans
{
	HChain<Trans>	list;
	const TXIDKey	txid;
	LSN				lastLSN;
	Trans(TXID tx,LSN lsn) : list(this),txid(tx),lastLSN(lsn) {}
	Trans(const Trans& tx) : list(this),txid(tx.txid),lastLSN(tx.lastLSN) {}
	TXIDKey			getKey() const {return txid;}
	void			*operator new(size_t s) throw() {return malloc(s,STORE_HEAP);}
	void			operator delete(void *p) {free(p,STORE_HEAP);}
};

typedef HashTab<Trans,TXIDKey,&Trans::list> TxSet;

/**
 * recovery 'dirty' page bookkeeping structure
 */
struct DirtyPg
{
	HChain<DirtyPg>	list;
	const PageID	pageID;
	unsigned			flag;
	LSN				redoLSN;
	DirtyPg(PageID pid,LSN lsn,unsigned f=0) : list(this),pageID(pid),flag(f),redoLSN(lsn) {}
	PageID			getKey() const {return pageID;}
	void			*operator new(size_t s) throw() {return malloc(s,STORE_HEAP);}
	void			operator delete(void *p) {free(p,STORE_HEAP);}
};

typedef HashTab<DirtyPg,PageID,&DirtyPg::list> DirtyPageSet;

};

#endif
