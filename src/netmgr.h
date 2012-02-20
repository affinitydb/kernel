/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _NETMGR_H_
#define _NETMGR_H_

#include "qmgr.h"
#include "idxtree.h"
#include "pagemgr.h"
#include "affinityimpl.h"

class IStoreNet;

using namespace AfyDB;

namespace AfyKernel
{
#define	RPIN_NOFETCH			0x8000

#define	DEFAULT_RPINHASH_SIZE	0x200
#define	DEFAULT_CACHED_RPINS	0x1000
#define	DEFAULT_EXPIRATION		TIMESTAMP(3600000000ul)		// 1 hour
#define	RPIN_BLOCK				0x1000

class RPIN
{
	friend class	NetMgr;
	typedef	QElt<RPIN,PID,const PID&> QE;
	PID				ID;
	QE				*qe;
	class	NetMgr	&mgr;
	TIMESTAMP		expiration;
	PageAddr		addr;
public:
	RPIN(const PID& id,class NetMgr& mg);
	void			setKey(const PID& id,void*) {ID=id;}
	QE				*getQE() const {return qe;}
	void			setQE(QE *q) {qe=q;}
	RC				load(int,ulong);
	RC				refresh(PIN*,class Session*);
	bool			isDirty() const {return false;}
	RW_LockType		lockType(RW_LockType lt) const {return lt;}
	bool			save() const {return true;}
	void			destroy();
	void			initNew() const {}
	static	RPIN	*createNew(const PID& id,void *mg);
	static	void	waitResource(void *mg);
	static	void	signal(void *mg);
};

typedef QMgr<RPIN,PID,const PID&,int,STORE_HEAP> RPINHashTab;

class NetMgr : public RPINHashTab, public PageMgr
{
	friend	class		RPIN;
	StoreCtx			*const ctx;
	IStoreNet			*net;
	Mutex				lock;
	SharedCounter		nPINs;
	long				xPINs;
	TIMESTAMP			defExpiration;
	TreeGlobalRoot		map;
	FreeQ<RPIN_BLOCK,Std_Alloc<STORE_HEAP> > freeRPINs;
public:
	NetMgr(StoreCtx *ct,IStoreNet *n,int hashSize=DEFAULT_RPINHASH_SIZE,int cacheSize=DEFAULT_CACHED_RPINS,TIMESTAMP dExp=DEFAULT_EXPIRATION);
	virtual				~NetMgr();
	void *operator		new(size_t s,StoreCtx *ctx) {void *p=ctx->malloc(s); if (p==NULL) throw RC_NORESOURCES; return p;}
	void				close();
	RC					insert(PIN *pin);
	RC					updateAddr(const PID& id,const PageAddr& addr);
	RC					remove(const PID& id,PageAddr &addr);
	RC					refresh(PIN *pin,class Session *);
	RC					getPage(const PID& id,ulong flags,PageIdx& idx,class PBlockP& pb,Session *ses);
	RC					setExpiration(const PID& id,unsigned long);
	RC					undo(ulong info,const byte *rec,size_t lrec,PageID);
	bool				isCached(const PID& id);
	bool				isOnline();
};

};

#endif
