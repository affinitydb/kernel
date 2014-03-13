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

#include "pin.h"
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
#define	PINEX_DERIVED		0x0040		/**< PIN derived in TransOp, no PID */

#define	PEX_PID				0x0001
#define	PEX_PAGE			0x0002
#define	PEX_PROPS			0x0004
#define	PEX_ALLPROPS		0x0008

class PINx : public PIN, public LatchHolder
{
	Session							*ses;
	PBlockP							pb;
	const	HeapPageMgr::HeapPIN	*hpin;
	class	TVers					*tv;
public:
	mutable	EncPINRef				epr;
public:
	PINx(Session *s,const PID& i) : PIN(s),LatchHolder(s),ses(s),hpin(NULL),tv(NULL) {id=i; fPINx=1; fPartial=1; epr.flags=0; epr.buf[0]=0;}
	PINx(Session *s,const Value *pv=NULL,unsigned nv=0) : PIN(s,0,(Value*)pv,nv),LatchHolder(s),ses(s),hpin(NULL),tv(NULL) {fPINx=1; if (pv==NULL) fPartial=1; epr.flags=0; epr.buf[0]=0;}
	~PINx()		{pb.release(ses); free();}
	void		cleanup() {id=PIN::noPID; addr=PageAddr::noAddr; pb.release(ses); hpin=NULL; free(); tv=NULL; epr.flags=0; epr.buf[0]=0; fPartial=1;}
	void		setProps(const Value *props,unsigned nProps,bool f=true) {properties=(Value*)props; nProperties=nProps; fPartial=0; fNoFree=f?1:0;}	// meta?
	void		resetProps() {if (properties!=NULL) {if (fNoFree==0) freeV((Value*)properties,nProperties,ses); properties=NULL; nProperties=0;} fPartial=1; meta=0;}
	void		releaseLatches(PageID pid,PageMgr*,bool);
	void		checkNotHeld(PBlock*);
	void		copy(const PIN *pin,unsigned flags);
	void		setPartial() {fPartial=1;}
	RC			load(unsigned mode=0,const PropertyID *pids=NULL,unsigned nPids=0);
	const Value	*loadProperty(PropertyID);
	const void	*getPropTab(unsigned& nProps) const;
	void		moveTo(PINx &);
	RC			getID(PID& pid) const {RC rc=RC_OK; if (id.isEmpty()) rc=epr.buf[0]==0?RC_NOTFOUND:unpack(); pid=id; return rc;}
	unsigned	getState() const {return (epr.buf[0]==0&&id.isEmpty()?0:hpin!=NULL?PEX_PAGE|PEX_PID:PEX_PID)|(properties==NULL?0:fPartial!=0?PEX_PROPS:PEX_PROPS|PEX_ALLPROPS);}
	const		HeapPageMgr::HeapPIN *fill() {if (pb.isNull()) hpin=NULL; else {const HeapPageMgr::HeapPage *hp=(HeapPageMgr::HeapPage*)pb->getPageBuf(); hpin=(HeapPageMgr::HeapPIN*)hp->getObject(hp->getOffset(addr.idx));} return hpin;}
	bool		defined(const PropertyID *pids,unsigned nProps) const;
	RC			getVx(PropertyID pid,Value& v,unsigned mode,MemAlloc *ma,ElementID=STORE_COLLECTION_ID);
	bool		isCollection(PropertyID pid) const;
	RC			pack() const;
	void		copyFlags();
	void		operator=(const PID& pid) const {id=pid;}
	void		operator=(const PageAddr& ad) const {addr=ad;}
	Session		*getSes() const {return ses;}
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
	friend	class	QueryOp;
};

};

#endif
