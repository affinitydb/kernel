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
 * extended PIN class
 * can store a reference to the page containing the PIN
 * can store PIN ID in compressed form (see pinref.h)
 * can contain reference to transient versioning descriptor
 */
#ifndef _PINEX_H_
#define _PINEX_H_

#include "affinityimpl.h"
#include "pinref.h"
#include "buffer.h"
#include "pgheap.h"

namespace AfyKernel
{

#define	PINEX_TVERSION		0x0001		/**< transient versioning set */
#define	PINEX_LOCKED		0x0002		/**< PIN is locked in transaction */
#define	PINEX_XLOCKED		0x0004		/**< PIN is locked for modification */
#define	PINEX_ACL_CHKED		0x0008		/**< ACL is checked for the PIN, access is permitted */
#define	PINEX_ADDRSET		0x0010		/**< page address is set */
#define	PINEX_EXTPID		0x0020		/**< external PIN ID, suppress error reporting if doesn't exist */
#define	PINEX_RLOAD			0x0040		/**< load PIN properties if PINs page is being force-unlatched */

#define	PEX_PID				0x0001
#define	PEX_PAGE			0x0002
#define	PEX_PROPS			0x0004
#define	PEX_ALLPROPS		0x0008

class PINEx : public PIN, public LatchHolder
{
	PBlockP							pb;
	const	HeapPageMgr::HeapPIN	*hpin;
	class	TVers					*tv;
public:
	mutable	EncPINRef				epr;
public:
	PINEx(Session *s) : PIN(s,PIN::defPID,PageAddr::invAddr),LatchHolder(s),hpin(NULL),tv(NULL) {epr.flags=0; epr.lref=0;}
	PINEx(Session *s,const PID& pid,const Value *pv=NULL,unsigned nv=0) : PIN(s,pid,PageAddr::invAddr,0,(Value*)pv,nv),LatchHolder(s),hpin(NULL),tv(NULL) {epr.flags=0; epr.lref=0;}
	PINEx(const PIN *pin) : PIN(pin->ses,pin->id,pin->addr,pin->mode|PIN_NO_FREE,pin->properties,pin->nProperties),LatchHolder(pin->ses),hpin(NULL),tv(NULL) {stamp=pin->stamp; epr.flags=0; epr.lref=0;}
	~PINEx()	{pb.release(ses); free();}
	void		cleanup() {id=PIN::defPID; addr=PageAddr::invAddr; pb.release(ses); hpin=NULL; free(); tv=NULL; epr.flags=0; epr.lref=0;}
	void		setProps(const Value *props,unsigned nProps,unsigned f=PIN_NO_FREE) {properties=(Value*)props; nProperties=nProps; mode|=f;}
	void		resetProps() {if (properties!=NULL) {if ((mode&PIN_NO_FREE)==0) freeV((Value*)properties,nProperties,ses); properties=NULL; nProperties=0;}}
	void		releaseLatches(PageID pid,PageMgr*,bool);
	void		checkNotHeld(PBlock*);
	RC			load(unsigned mode=0,const PropertyID *pids=NULL,unsigned nPids=0);
	void		moveTo(PINEx &);
	void		operator=(const PID& pid) const {id=pid;}
	void		operator=(const PageAddr& ad) const {addr=ad;}
	const		PageAddr& getAddr() const {return addr;}
	RC			getID(PID& pid) const {RC rc=RC_OK; if (id.pid==STORE_INVALID_PID) rc=epr.lref==0?RC_NOTFOUND:unpack(); pid=id; return rc;}
	unsigned	getState() const {return (epr.lref==0&&id.pid==STORE_INVALID_PID?0:hpin!=NULL?PEX_PAGE|PEX_PID:PEX_PID)|(properties==NULL?0:(mode&PIN_PROJECTED)!=0?PEX_PROPS:PEX_PROPS|PEX_ALLPROPS);}
	const		HeapPageMgr::HeapPIN *fill() {if (pb.isNull()) hpin=NULL; else {const HeapPageMgr::HeapPage *hp=(HeapPageMgr::HeapPage*)pb->getPageBuf(); hpin=(HeapPageMgr::HeapPIN*)hp->getObject(hp->getOffset(addr.idx));} return hpin;}
	bool		defined(const PropertyID *pids,unsigned nProps) const;
	RC			getValue(PropertyID pid,Value& v,ulong mode,MemAlloc *ma,ElementID=STORE_COLLECTION_ID) const;
	bool		isCollection(PropertyID pid) const;
	RC			pack() const;
private:
	RC			unpack() const;
	void		free();
	friend	class	Class;
	friend	class	ClassPropIndex;
	friend	class	Classifier;
	friend	class	QueryPrc;
	friend	class	MergeIDs;
	friend	class	MergeOp;
	friend	class	LockMgr;
	friend	class	Stmt;
	friend	class	Cursor;
	friend	class	CursorNav;
	friend	class	FullScan;
	friend	class	TransOp;
};

};

#endif
