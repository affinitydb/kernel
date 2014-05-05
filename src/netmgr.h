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
 * remote PIN reading and caching
 * implements a cache of remote PINs (i.e. content created in other stores) similar to browser caches
 */
#ifndef _NETMGR_H_
#define _NETMGR_H_

#include "qmgr.h"
#include "idxtree.h"
#include "pagemgr.h"
#include "pin.h"

using namespace Afy;

namespace AfyKernel
{
#define	RPIN_NOFETCH			0x8000						/**< don't re-fetch PIN flag; prevents recursion in persistPINs() */

#define	DEFAULT_RPINHASH_SIZE	0x200						/**< default remote and replicated PIN hash size */
#define	DEFAULT_CACHED_RPINS	0x1000						/**< default number of cached remote and repliacted PIN page addresses */
#define	DEFAULT_EXPIRATION		TIMESTAMP(3600000000ul)		/**< default cache content expiration interval (1 hour) */
#define	RPIN_BLOCK				0x1000

/**
 * cached remote or replicated PIN in-memory descriptor
 */
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
	RC				load(int,unsigned);
	RC				refresh(PIN*,class Session*);
	RC				read(Session *ses,PIN *&pin);
	bool			isDirty() const {return false;}
	RW_LockType		lockType(RW_LockType lt) const {return lt;}
	bool			save() const {return true;}
	void			destroy();
	void			initNew() const {}
	static	RPIN	*createNew(const PID& id,void *mg);
	static	void	waitResource(void *mg);
	static	void	signal(void *mg);
};

typedef QMgr<RPIN,PID,const PID&,int> RPINHashTab;

/**
 * remote and replicated PIN manager
 * implements cache of PIN page addresses
 * synchronises access to remote PINs
 * controls cached content expiration
 */
class NetMgr : public RPINHashTab, public PageMgr
{
	friend	class	RPIN;
	Mutex			lock;
	SharedCounter	nPINs;
	long			xPINs;
	TIMESTAMP		defExpiration;
	TreeGlobalRoot	map;
	Pool			freeRPINs;
public:
	NetMgr(StoreCtx *ct,int hashSize=DEFAULT_RPINHASH_SIZE,int cacheSize=DEFAULT_CACHED_RPINS,TIMESTAMP dExp=DEFAULT_EXPIRATION);
	virtual			~NetMgr();
	void *operator	new(size_t s,StoreCtx *ctx) {void *p=ctx->malloc(s); if (p==NULL) throw RC_NOMEM; return p;}
	void			close();
	RC				insert(PIN *pin);
	RC				updateAddr(const PID& id,const PageAddr& addr);
	RC				remove(const PID& id,PageAddr &addr);
	RC				refresh(PIN *pin,class Session *);
	RC				getPage(const PID& id,unsigned flags,PageIdx& idx,class PBlockP& pb,Session *ses);
	RC				setExpiration(const PID& id,unsigned);
	RC				undo(unsigned info,const byte *rec,size_t lrec,PageID);
	bool			isCached(const PID& id);
};

};

#endif
