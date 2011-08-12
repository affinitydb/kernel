/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _RECOVER_H_
#define _RECOVER_H_

#include "buffer.h"
#include "logmgr.h"
#include "utils.h"

#define LOSERSETSIZE		2000
#define ACTIVESETSIZE		2000
#define DIRTYPAGESETSIZE	3000

namespace MVStoreKernel
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

struct DirtyPg
{
	HChain<DirtyPg>	list;
	const PageID	pageID;
	ulong			flag;
	LSN				redoLSN;
	DirtyPg(PageID pid,LSN lsn,ulong f=0) : list(this),pageID(pid),flag(f),redoLSN(lsn) {}
	PageID			getKey() const {return pageID;}
	void			*operator new(size_t s) throw() {return malloc(s,STORE_HEAP);}
	void			operator delete(void *p) {free(p,STORE_HEAP);}
};

typedef HashTab<DirtyPg,PageID,&DirtyPg::list> DirtyPageSet;

};

#endif
