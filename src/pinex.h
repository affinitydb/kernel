/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _PINEX_H_
#define _PINEX_H_

#include "mvstoreimpl.h"
#include "pinref.h"
#include "buffer.h"
#include "pgheap.h"

namespace MVStoreKernel
{

#define	PINEX_TVERSION		0x0001
#define	PINEX_LOCKED		0x0002
#define	PINEX_XLOCKED		0x0004
#define	PINEX_ACL_CHKED		0x0008
#define	PINEX_ADDRSET		0x0010
#define	PINEX_EXTPID		0x0020

#define	PEX_PID				0x0001
#define	PEX_PAGE			0x0002
#define	PEX_PROPS			0x0004
#define	PEX_ALLPROPS		0x0008

class PINEx : public PIN
{
	PBlockP							pb;
	const	HeapPageMgr::HeapPage	*hp;
	const	HeapPageMgr::HeapPIN	*hpin;
	class	TVers					*tv;
public:
	mutable	EncPINRef				epr;
public:
	PINEx(Session *s) : PIN(s,PIN::defPID,PageAddr::invAddr),hp(NULL),hpin(NULL),tv(NULL) {epr.flags=0; epr.lref=0;}
	PINEx(Session *s,const PID& pid,const Value *pv=NULL,unsigned nv=0) : PIN(s,pid,PageAddr::invAddr,0,(Value*)pv,nv),hp(NULL),hpin(NULL),tv(NULL) {epr.flags=0; epr.lref=0;}
	PINEx(const PIN *pin) : PIN(pin->ses,pin->id,pin->addr,pin->mode|PIN_NO_FREE,pin->properties,pin->nProperties),hp(NULL),hpin(NULL),tv(NULL) {stamp=pin->stamp; epr.flags=0; epr.lref=0;}
	PINEx(Session *s,PBlock *p,const PageAddr &ad,bool fRel=true);
	~PINEx()	{free();}
	void		cleanup() {pb.release(); hp=NULL; hpin=NULL; addr=PageAddr::invAddr; free(); tv=NULL; epr.flags=0; epr.lref=0;}
	void		release() {pb.release(); hp=NULL; hpin=NULL; free(); tv=NULL; epr.flags&=~PINEX_TVERSION;}
	void		setProps(const Value *props,unsigned nProps,unsigned f=PIN_NO_FREE) {properties=(Value*)props; nProperties=nProps; mode|=f;}
	void		resetProps() {if (properties!=NULL) {if ((mode&PIN_NO_FREE)==0) freeV((Value*)properties,nProperties,ses); properties=NULL; nProperties=0;}}
	RC			releaseCopy(const PropertyID *flt=NULL,unsigned nFlt=0);
	RC			load(unsigned mode=0,const PropertyID *pids=NULL,unsigned nPids=0);
	void		operator=(const PINEx &);
	void		operator=(const PID& pid) const {id=pid;}
	void		operator=(const PageAddr& ad) const {addr=ad;}
	RC			getID(PID& pid) const {RC rc=RC_OK; if (id.pid==STORE_INVALID_PID) rc=epr.lref==0?RC_NOTFOUND:unpack(); pid=id; return rc;}
	const		PageAddr&	getAddr() const {return addr;}
	unsigned	getState() const {return (epr.lref==0&&id.pid==STORE_INVALID_PID?0:hpin!=NULL?PEX_PAGE|PEX_PID:PEX_PID)|(properties==NULL?0:(mode&PIN_PROJECTED)!=0?PEX_PROPS:PEX_PROPS|PEX_ALLPROPS);}
	bool		defined(const PropertyID *pids,unsigned nProps) const;
	RC			getValue(PropertyID pid,Value& v,ulong mode,MemAlloc *ma,ElementID=STORE_COLLECTION_ID) const;
	RC			loadV(const PropertyID *props,Value *res,unsigned nProps) const;
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
	friend	class	FullScan;
	friend	class	TransOp;
};

};

#endif
