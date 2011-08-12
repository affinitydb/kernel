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

#define	PINEX_DESTROY		0x0001
#define	PINEX_TVERSION		0x0002
#define	PINEX_LOCKED		0x0004
#define	PINEX_XLOCKED		0x0008
#define	PINEX_ACL_CHKED		0x0010
#define	PINEX_ADDRSET		0x0020
#define	PINEX_EXTPID		0x0040

class PINEx {
	Session		*const				ses;
	mutable		PID					id;
	const		Value				*props;
	uint32_t						nProps;
	uint32_t						mode;
	mutable	PageAddr				addr;
	PBlockP							pb;
	const	HeapPageMgr::HeapPage	*hp;
	const	HeapPageMgr::HeapPIN	*hpin;
	class	TVers					*tv;
	double							degree;
	mutable	EncPINRef				epr;
public:
	PINEx(Session *s) : ses(s),id(PIN::defPID),props(NULL),nProps(0),mode(0),addr(PageAddr::invAddr),hp(NULL),hpin(NULL),tv(NULL),degree(0.) {epr.flags=0; epr.lref=0;}
	PINEx(Session *s,const PID& pid,const Value *pv=NULL,unsigned nv=0) : ses(s),id(pid),props(pv),nProps(nv),mode(0),addr(PageAddr::invAddr),hp(NULL),hpin(NULL),tv(NULL),degree(0.) {epr.flags=0; epr.lref=0;}
	PINEx(Session *s,PBlock *p,const PageAddr &ad,bool fRel=true);
	PINEx(const PIN *pin);
	~PINEx()	{free();}
	void		cleanup() {pb.release(); hp=NULL; hpin=NULL; addr=PageAddr::invAddr; free(); props=NULL; nProps=0; tv=NULL; degree=0.; epr.flags=0; epr.lref=0;}
	void		release() {pb.release(); hp=NULL; hpin=NULL; free(); props=NULL; nProps=0; tv=NULL; epr.flags&=~(PINEX_DESTROY|PINEX_TVERSION);}
	void		operator=(const PINEx &);
	void		operator=(const PID& pid) const {id=pid;}
	void		operator=(const PageAddr& ad) {addr=ad;}
	RC			unpack() const;
	RC			pack() const;
	const		PageAddr&	getAddr() const {return addr;}
	bool		defined(const PropertyID *pids,unsigned nProps) const;
	RC			getValue(PropertyID pid,Value& v,ulong mode,MemAlloc *ma,ElementID=STORE_COLLECTION_ID) const;
	bool		isCollection(PropertyID pid) const;
private:
	void		free();
	friend	class	LockMgr;
	friend	class	PIDStore;
	friend	class	Cursor;
	friend	class	QueryOp;
	friend	class	MergeIDOp;
	friend	class	MergeOp;
	friend	class	FullScan;
	friend	class	ClassScan;
	friend	class	IndexScan;
	friend	class	PathOp;
	friend	class	ArrayScan;
	friend	class	FTScan;
	friend	class	ArrayFilter;
	friend	class	BodyOp;
	friend	class	Sort;
	friend	class	QueryPrc;
	friend	class	ClassPropIndex;
	friend	class	Classifier;
	friend	class	Class;
	friend	class	Stmt;
	friend	class	Expr;
};

};

#endif
