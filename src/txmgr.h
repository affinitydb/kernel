/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _TXMGR_H_
#define _TXMGR_H_

#include "utils.h"
#include "pagemgr.h"
#include "session.h"

#define	MAX_PGID		15
#define	PGID_MASK		0x0F
#define	PGID_SHIFT		4

#define	TXMGR_UNDO		0x0001
#define	TXMGR_ATOMIC	0x0002
#define	TXMGR_RECV		0x0004

#define	DEFAULT_MAX_SS	20

class IStoreNotification;

namespace AfyKernel
{

class Snapshot
{
	const TXCID		txcid;
	Snapshot		*next;
	ulong			txCnt;
	DLList			data;
	Snapshot(TXCID txc,Snapshot *nxt) : txcid(txc),next(nxt),txCnt(0) {}
	friend class	TxMgr;
};

class TxMgr
{
	StoreCtx				*const	ctx;
	IStoreNotification		*const	notification;
	Mutex							lock;
	TXID							nextTXID;
	HChain<Session>					activeList;
	ulong							nActive;
	TXCID							lastTXCID;
	Snapshot						*snapshots;
	ulong							nSS;
	ulong							xSS;
public:
					TxMgr(StoreCtx *cx,TXID startTXID=0,IStoreNotification *notItf=NULL,ulong xSnap=DEFAULT_MAX_SS);
	void *operator	new(size_t s,StoreCtx *ctx) {void *p=ctx->malloc(s); if (p==NULL) throw RC_NORESOURCES; return p;}

	RC				startTx(Session *ses,ulong,ulong);
	RC				abortTx(Session *ses,bool fAll);
	RC				commitTx(Session *ses,bool fAll);
	TXCID			assignSnapshot();
	void			releaseSnapshot(TXCID);

	RC				update(class PBlock *pb,PageMgr *,ulong info,const byte *rec=NULL,size_t lrec=0,uint32_t f=0,class PBlock *newp=NULL) const;
	TXID			getLastTXID() {lock.lock(); TXID txid=++nextTXID; lock.unlock(); return txid;}
	void			setTXID(TXID);
	ulong			getNActive() const {return nActive;}
	struct LogActiveTransactions *getActiveTx(LSN&);
	static PageMgr	*getPageMgr(ulong info,StoreCtx *ctx) {return ctx->getPageMgr(PGID(info&PGID_MASK));}
	static bool		isMaster(ulong info) {return (info&PGID_MASK)==PGID_MASTER;}

private:
	RC				start(Session *ses,ulong flags);
	RC				commit(Session *ses,bool fAll=false,bool fFlush=true);
	RC				abort(Session *ses,bool fAll=false);
	void			cleanup(Session *ses,bool fAbort=false);
	friend	class	Session;
	friend	class	MiniTx;
	friend	class	TxSP;
};

#define	MTX_OK		0x0001
#define	MTX_FLUSH	0x0002
#define	MTX_STARTED	0x0004
#define	MTX_SKIP	0x0008
#define	MTX_GLOB	0x0010

class MiniTx
{
	friend	class	Session;
	friend	class	TxMgr;
	void	*operator new(size_t s) throw() {return NULL;}
	void	operator delete(void *p) {}
private:
	MiniTx					*next;
	Session		*const		ses;
	TXID					oldId;
	TXID					newId;
	TXCID					txcid;
	ulong					state;
	ulong					identity;
	ulong					mtxFlags;
	SubTx					tx;
	LSN						firstLSN;
	LSN						undoNextLSN;
	GrantedLock				*locks;
	TxReuse					reuse;
	RW_LockType				classLocked;
	void					cleanup(TxMgr *txMgr);
public:
	MiniTx(Session *s,ulong mtxf=MTX_FLUSH);
	~MiniTx();
	void ok() {mtxFlags|=MTX_OK;}
};

class TxSP
{
	void	*operator new(size_t s) throw() {return NULL;}
	void	operator delete(void *p) {}
private:
	Session	*const	ses;
	ulong			flags;
public:
	TxSP(Session *s) : ses(s),flags(0) {assert(s!=NULL);}
	~TxSP();
	RC		start(ulong,ulong txf=0);
	RC		start();
	void	ok() {flags|=MTX_OK;}
	void	resetOk() {flags&=~MTX_OK;}
	bool	isStarted() const {return (flags&MTX_STARTED)!=0;}
};

};

#endif
