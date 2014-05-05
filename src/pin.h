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
 * This file contains:
 * 1. Class definitions for implementation of IPIN interface (see affinity.h)
 * 2. struct Value helper functions: copying, conversion, comparison, serialization/deserialization, deallocation
 * 3. measurement units helper functions
 * 4. Common code for path iterator in expressions and PathOp
 */

#ifndef _PIN_H_
#define _PIN_H_

#include "session.h"

using namespace Afy;

namespace AfyKernel
{

#define	MODP_EIDS	0x0001
#define	MODP_NEID	0x0002
#define MODP_NCPY	0x0004

/**
 * Common code for PathOp and PathIt
 */

class Path
{
protected:
	struct PathState {
		PathState	*next;
		int			state;
		unsigned	idx;
		unsigned	rcnt;
		unsigned	vidx;
		unsigned	cidx;
		PID			id;
		Value		v[2];
	};
	MemAlloc		*const	ma;
	const	PathSeg	*const	path;
	const	unsigned		nPathSeg;
	const	bool			fCopied;
	bool					fThrough;
	PathState				*pst;
	PathState				*freePst;
protected:
	Path(MemAlloc *m,const PathSeg *ps,unsigned nP,bool fC) 
		: ma(m),path(ps),nPathSeg(nP),fCopied(fC),fThrough(true),pst(NULL),freePst(NULL)
			{for (unsigned i=0; i<nP; i++) if (ps[i].rmin!=0) {fThrough=false; break;}}
	~Path() {
		PathState *ps,*ps2;
		for (ps=pst; ps!=NULL; ps=ps2) {ps2=ps->next; freeV(ps->v[0]); freeV(ps->v[1]); ma->free(ps);}
		for (ps=freePst; ps!=NULL; ps=ps2) {ps2=ps->next; ma->free(ps);}
		if (fCopied) destroyPath((PathSeg*)path,nPathSeg,ma);
	}
	RC push(const PID& id) {
		PathState *ps;
		if (pst!=NULL && pst->vidx!=0) for (ps=pst; ps!=NULL && ps->idx==pst->idx; ps=ps->next) if (ps->id==id) {if (pst->state==2) pst->vidx++; return RC_OK;}
		if ((ps=freePst)!=NULL) freePst=ps->next; else if ((ps=new(ma) PathState)==NULL) return RC_NOMEM;
		if (pst==NULL) ps->idx=0,ps->rcnt=1; else if (pst->vidx==0) ps->idx=pst->idx+1,ps->rcnt=1; else ps->idx=pst->idx,ps->rcnt=pst->rcnt+1;
		ps->state=0; ps->vidx=2; ps->cidx=0; ps->id=id; ps->v[0].setEmpty(); ps->v[1].setEmpty(); ps->next=pst; pst=ps; return RC_OK;
	}
	void pop() {PathState *ps=pst; if (ps!=NULL) {pst=ps->next; freeV(ps->v[0]); freeV(ps->v[1]); ps->next=freePst; freePst=ps;}}
};

/**
 * Class PIN implements IPIN interface
 * @see method descriptions in affinity.h
 */

class PIN : public IPIN
{
protected:
	MemAlloc			*ma;				/**< Memory allocator for this PIN */
	mutable	PID			id;					/**< PIN ID */
	Value				*properties;		/**< array of PIN properties */
	uint32_t			nProperties;		/**< number of properties */
	uint32_t			mode;				/**< PIN flags (PIN_XXX) */
	mutable	PageAddr	addr;				/**< page address of PIN */
	uint16_t			meta;				/**< PIN metatype flags (PMT_*) */
	uint16_t			fPINx		:1;		/**< this is PINx class */
	uint16_t			fNoFree		:1;		/**< don't free properties in destructor */
	uint16_t			fReload		:1;		/**< load PIN properties if PINs page is being force-unlatched */
	uint16_t			fPartial	:1;		/**< pin is a result of a projection of a stored pin or pin is not completely loaded from page */
	uint16_t			fSSV		:1;		/**< pin has SSV properties and they're not loaded yet */
	size_t				length;				/**< PIN length - calculated and used in persistPINs() */

public:
	PIN(MemAlloc *m,unsigned md=0,Value *vals=NULL,unsigned nvals=0,bool fNF=false)
		: ma(m),id(noPID),properties(vals),nProperties(nvals),mode(md),addr(PageAddr::noAddr),meta(0),
		fPINx(0),fNoFree(fNF?1:0),fReload(0),fPartial(0),fSSV(0),length(0) {}
	virtual		~PIN();

	// interface implementation
	const PID&	getPID() const;
	bool		isLocal() const;
	unsigned	getFlags() const;
	unsigned	getMetaType() const;
	uint32_t	getNumberOfProperties() const;
	const Value	*getValueByIndex(unsigned idx) const;
	const Value	*getValue(PropertyID pid) const;
	RC			getPINValue(Value& res) const;
	RC			getPINValue(Value& res,Session *ses) const;
	virtual	bool defined(const PropertyID *pids,unsigned nProps) const;
	bool		testClassMembership(ClassID,const Value *params=NULL,unsigned nParams=0) const;
	RC			isMemberOf(ClassID *&clss,unsigned& nclss);
	IPIN		*clone(const Value *overwriteValues=NULL,unsigned nOverwriteValues=0,unsigned mode=0);
	IPIN		*project(const PropertyID *properties,unsigned nProperties,const PropertyID *newProps=NULL,unsigned mode=0);
	RC			modify(const Value *values,unsigned nValues,unsigned mode,const ElementID *eids,unsigned*);
	RC			setExpiration(uint32_t);
	RC			setNotification(bool fReset=false);
	RC			setReplicated();
	RC			hide();
	RC			deletePIN();
	RC			undelete();
	RC			refresh(bool);
	IMemAlloc	*getAlloc() const;
	RC			allocSubPIN(unsigned nProps,IPIN *&pin,Value *&values,unsigned mode=0) const;
	void		destroy();

	virtual	bool	checkProps(const PropertyID *pids,unsigned nProps);
	virtual	RC		load(unsigned mode=0,const PropertyID *pids=NULL,unsigned nPids=0);
	virtual	const	Value	*loadProperty(PropertyID);
	virtual	const	void	*getPropTab(unsigned& nProps) const;

	// helper functions
	uint32_t	getMode() const {return mode;}
	bool		isPartial() const {return fPartial!=0;}
	const		PageAddr& getAddr() const {return addr;}
	void		operator delete(void *p) {if (((PIN*)p)->ma!=NULL) ((PIN*)p)->ma->free(p);}
	void		operator=(const PID& pid) const {id=pid;}
	void		operator=(const PageAddr& ad) const {addr=ad;}
	RC			getV(PropertyID pid,Value& v,unsigned mode=0,MemAlloc *ma=NULL,ElementID eid=STORE_COLLECTION_ID);
	RC			getV(PropertyID pid,Value& v,unsigned mode,const Value& idx,MemAlloc *ma=NULL);
	RC			getV(PropertyID pid,const Value *&pv);
	RC			modify(const Value *pv,unsigned epos,unsigned eid,unsigned flags);
	void		setProps(const Value *pv,unsigned nv) {properties=(Value*)pv; nProperties=nv;}
	void		setMode(uint32_t md) {mode=md;}
	RC			checkSet(const Value *&pv,const Value& w);
	RC			render(class SOutCtx&,bool fInsert=false) const;
	size_t		serSize() const;
	byte		*serialize(byte *buf) const;
	static	RC	deserialize(PIN*&,const byte *&,const byte *const ebuf,MemAlloc*,bool);
	__forceinline const Value *findProperty(PropertyID pid) {const Value *pv=VBIN::find(pid,properties,nProperties); return pv!=NULL||fPartial==0?pv:loadProperty(pid);}
	ElementID	getPrefix(StoreCtx *ctx) const {return !id.isPID()?ctx->getPrefix():StoreCtx::genPrefix(ushort(id.pid>>48));}
	static PIN*	getPIN(const PID& id,VersionID vid,Session *ses,unsigned mode=0);
	static const Value *findElement(const Value *pv,unsigned eid) {
		assert(pv!=NULL && pv->type==VT_COLLECTION && !pv->isNav());
		if (pv->length!=0) {
			if (eid==STORE_FIRST_ELEMENT) return &pv->varray[0];
			if (eid==STORE_LAST_ELEMENT) return &pv->varray[pv->length-1];
			for (unsigned i=0; i<pv->length; i++) if (pv->varray[i].eid==eid) return &pv->varray[i];
		}
		return NULL;
	}
	static	int		__cdecl cmpLength(const void *v1, const void *v2);
	static	unsigned	getCardinality(const PID& ref,unsigned propID);
	static	const	PID noPID;
	friend	class	ServiceCtx;
	friend	class	EncodePB;
	friend	class	DecodePB;
	friend	class	ProcessStream;
	friend	class	FullScan;
	friend	class	QueryPrc;
	friend	class	CursorNav;
	friend	class	Cursor;
	friend	class	Stmt;
	friend	class	Session;
	friend	class	Classifier;
	friend	class	ClassPropIndex;
	friend	struct	ClassResult;
	friend	class	EventMgr;
	friend	class	NamedMgr;
	friend	class	NetMgr;
	friend	class	RPIN;
	friend	class	PINx;
	friend	class	TransOp;
	friend	class	LoadOp;
	friend	class	CommOp;
	friend	class	BatchInsert;
	friend	class	LoadService;
	friend	class	StartListener;
	friend	class	SOutCtx;
};

inline bool isRemote(const PID& id) {return id.ident!=STORE_OWNER&&id.ident!=STORE_INVALID_IDENTITY&&id.ident!=STORE_INMEM_IDENTITY||!id.isEmpty()&&ushort(id.pid>>48)!=StoreCtx::get()->storeID;}

class BatchInsert : public IBatch, public StackAlloc, public DynArray<PIN*>
{
	Session *const ses;
public:
	BatchInsert(Session *s) : StackAlloc(s),DynArray<PIN*>((StackAlloc*)this),ses(s) {}
	unsigned	getNumberOfPINs() const;
	size_t		getSize() const;
	Value		*createValues(uint32_t nValues);
	RC			createPIN(Value *values,unsigned nValues,unsigned mode=0,const PID *original=NULL);
	RC			addRef(unsigned from,const Value &to,bool fAdd=true);
	RC			clone(IPIN *pin,const Value *values=NULL,unsigned nValues=0,unsigned mode=0);
	RC			process(bool fDestroy=true,unsigned mode=0,const AllocCtrl* =NULL,const IntoClass *into=NULL,unsigned nInto=0);
	RC			getPIDs(PID *pids,unsigned& nPIDs,unsigned start=0) const;
	RC			getPID(unsigned idx,PID& pid) const;
	const Value *getProperty(unsigned idx,URIID pid) const;
	ElementID	getEIDBase() const;
	void*		malloc(size_t s);
	void*		realloc(void *p,size_t s,size_t old=0);
	void		free(void *p);
	void		clear();
	void		destroy();
};

}

#endif
