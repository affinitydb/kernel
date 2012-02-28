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
 * This file contains:
 * 1. Class definitions for implementation of IPIN and ISession interfaces (see affinity.h)
 * 2. struct Value helper functions: copying, conversion, comparison, serialization/deserialization, deallocation
 * 3. measurement units helper functions
 * 4. Common code for path iterator in expressions and PathOp
 */

#ifndef _AFFINITYIMPL_H_
#define _AFFINITYIMPL_H_

#include "affinity.h"
#include "session.h"

using namespace AfyDB;

namespace AfyKernel
{

/*
 * convV flags
 */
#define	CV_NOTRUNC	0x0001
#define	CV_NODEREF	0x0002

/**
 * Value helper functions
 */

extern	MemAlloc *createMemAlloc(size_t,bool fMulti);																						/**< new heaps for sessions and stores */
extern	RC		copyV(const Value *from,ulong nv,Value *&to,MemAlloc *ma);																	/**< copy array of Value structures */
extern	RC		copyV0(Value& to,MemAlloc *ma);																								/**< deep data copy in one Value */
__forceinline	RC	copyV(const Value& from,Value& to,MemAlloc *ma) {return (to=from).type>=VT_STRING && ma!=NULL?copyV0(to,ma):RC_OK;}		/**< inline: copy Value and check if deep data copy is necessary */
extern	bool	operator==(const Value& lhs, const Value& rhs);																				/**< check equality of 2 Value sturcts */
inline	bool	operator!=(const Value& lhs, const Value& rhs) {return !(lhs==rhs);}														/**< check non-equality of 2 Value structs */
extern	size_t	serSize(const Value& val,bool full=false);																					/**< calculate size of serialized Value */
extern	size_t	serSize(const PID &id);																										/**< calculate size of serialized PIN ID */
extern	byte	*serialize(const Value& val,byte *buf,bool full=false);																		/**< serialize Value to buf */
extern	byte	*serialize(const PID &id,byte *buf);																						/**< serialize PIN ID to buf */
extern	RC		deserialize(Value& val,const byte *&buf,const byte *const ebuf,MemAlloc*,bool,bool full=false);								/**< deserialize Value from buf */
extern	RC		deserialize(PID& id,const byte *&buf,const byte *const ebuf);																/**< deserialize PIN ID from buf */
extern	RC		streamToValue(IStream *str,Value& val,MemAlloc*);																			/**< load a stream to memory and return as a string in Value */
extern	int		cmpNoConv(const Value&,const Value&,ulong u);																				/**< comparison of 2 Value structs of the same type */
extern	int		cmpConv(const Value&,const Value&,ulong u);																					/**< comparison of 2 Value structs of different types with conversion */
extern	bool	testStrNum(const char *s,size_t l,Value& res);																				/**< test if a string is a representation of a number */
extern	RC		convV(const Value& src,Value& dst,ushort type,MemAlloc *ma,unsigned mode=0);												/**< convert Value to another type */
extern	RC		derefValue(const Value& src,Value& dst,Session *ses);																		/**< deference Value (VT_REFID, VT_REFIDPROP, etc.) */
extern	RC		convURL(const Value& src,Value& dst,HEAP_TYPE alloc);																		/**< convert URL */
extern	void	freeV(Value *v,ulong nv,MemAlloc*);																							/**< free in array of Value structs */
extern	void	freeV0(Value& v);																											/**< free data in Value */
__forceinline	void freeV(Value& v) {if ((v.flags&HEAP_TYPE_MASK)!=NO_HEAP) freeV0(v);}													/**< inline check if data must be freed */
__forceinline	int	cmp(const Value& arg,const Value& arg2,ulong u) {return arg.type==arg2.type?cmpNoConv(arg,arg2,u):cmpConv(arg,arg2,u);}	/**< inline comparison of 2 Value structs */

extern	RC		convUnits(QualifiedValue& q, Units u);																						/**< conversion between various compatible measurment units */
extern	bool	compatible(QualifiedValue&,QualifiedValue&);																				/**< check if measurment units are compatible */
extern	bool	compatibleMulDiv(Value&,uint16_t units,bool fDiv);																			/**< check if measument units can be mulitplied/divided */
extern	Units	getUnits(const char *suffix,size_t l);																						/**< convert unit suffix to Units enumeration constant */
extern	const char *getUnitName(Units u);																									/**< get measument unit suffix */
extern	const char *getLongUnitName(Units u);																								/**< get full name of unit */

#define PIDKeySize		(sizeof(uint64_t)+sizeof(IdentityID))

class ValCmp
{
public:
	__forceinline static int cmp(const Value& v,PropertyID pid) {return cmp3(v.property,pid);}
};

class QNameCmp
{
public:
	__forceinline static int cmp(const QName& lhs,const QName& rhs) {int c=memcmp(lhs.qpref,rhs.qpref,lhs.lq<rhs.lq?lhs.lq:rhs.lq); return c!=0?c:cmp3(lhs.lq,rhs.lq);}
};

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
		if (fCopied) {
			for (unsigned i=0; i<nPathSeg; i++) if (path[i].filter!=NULL) path[i].filter->destroy();
			ma->free((void*)path);
		}
	}
	RC push(const PID& id) {
		PathState *ps;
		if (pst!=NULL && pst->vidx!=0) for (ps=pst; ps!=NULL && ps->idx==pst->idx; ps=ps->next) if (ps->id==id) {if (pst->state==2) pst->vidx++; return RC_OK;}
		if ((ps=freePst)!=NULL) freePst=ps->next; else if ((ps=new(ma) PathState)==NULL) return RC_NORESOURCES;
		if (pst==NULL) ps->idx=0,ps->rcnt=1; else if (pst->vidx==0) ps->idx=pst->idx+1,ps->rcnt=1; else ps->idx=pst->idx,ps->rcnt=pst->rcnt+1;
		ps->state=0; ps->vidx=2; ps->cidx=0; ps->id=id; ps->v[0].setError(); ps->v[1].setError(); ps->next=pst; pst=ps; return RC_OK;
	}
	void pop() {PathState *ps=pst; if (ps!=NULL) {pst=ps->next; freeV(ps->v[0]); freeV(ps->v[1]); ps->next=freePst; freePst=ps;}}
};

/**
 * PIN flags (continuation of flags in affinity.h)
 */

#define	PIN_NO_FREE					0x80000000	/**< don't free properties in destructor */
#define	PIN_READONLY				0x02000000	/**< readonly pin - from remote pin cache */
#define	PIN_DELETED					0x01000000	/**< soft-deleted pin (only with MODE_DELETED or IDumpStore) */
#define	PIN_CLASS					0x00800000	/**< pin represents a class or a relation (set in commitPINs) */
#define	PIN_DERIVED					0x00400000	/**< pin is a result of some transformation (other than projection) */
#define	PIN_PROJECTED				0x00200000	/**< pin is a result of a projection of a stored pin */
#define	PIN_SSV						0x00100000	/**< pin has SSV properties and they're not loaded yet */
#define	PIN_EMPTY					0x00080000	/**< pin with no properties */

/**
 * Class PIN implements IPIN interface
 * @see method descriptions in affinity.h
 */

class PIN : public IPIN
{
	Session				*const ses;			/**< Session this PIN was created in */
	mutable	PID			id;					/**< PIN ID */
	Value				*properties;		/**< array of PIN properties */
	uint32_t			nProperties;		/**< number of properties */
	uint32_t			mode;				/**< PIN flags (PIN_XXX) */
	mutable	PageAddr	addr;				/**< page address of PIN */
	PIN					*sibling;			/**< PIN's sibling (for results of joins) */
	union {
		uint32_t		stamp;				/**< PIN stamp after PIN is committed to persistent memory */
		size_t			length;				/**< PIN length - calculated and used in commitPINs() */
	};

public:
	PIN(Session *s,const PID& i,const PageAddr& a,ulong md=0,Value *vals=NULL,ulong nvals=0)
		: ses(s),id(i),properties(vals),nProperties(nvals),mode(md),addr(a),sibling(NULL) {length=0;}
	virtual		~PIN();

	// interface implementation
	const PID&	getPID() const;
	bool		isLocal() const;
	bool		isCommitted() const;
	bool		isReadonly() const;
	bool		canNotify() const;
	bool		isReplicated() const;
	bool		canBeReplicated() const;
	bool		isHidden() const;
	bool		isDeleted() const;
	bool		isClass() const;
	bool		isDerived() const;
	bool		isProjected() const;
	uint32_t	getNumberOfProperties() const;
	const Value	*getValueByIndex(unsigned idx) const;
	const Value	*getValue(PropertyID pid) const;
	IPIN		*getSibling() const;
	char		*getURI() const;
	uint32_t	getStamp() const;
	RC			getPINValue(Value& res) const;
	RC			getPINValue(Value& res,Session *ses) const;
	bool		testClassMembership(ClassID,const Value *params=NULL,unsigned nParams=0) const;
	bool		defined(const PropertyID *pids,unsigned nProps) const;
	RC			isMemberOf(ClassID *&clss,unsigned& nclss);
	IPIN		*clone(const Value *overwriteValues=NULL,unsigned nOverwriteValues=0,unsigned mode=0);
	IPIN		*project(const PropertyID *properties,unsigned nProperties,const PropertyID *newProps=NULL,unsigned mode=0);
	RC			modify(const Value *values,unsigned nValues,unsigned mode,const ElementID *eids,unsigned*);
	RC			setExpiration(uint32_t);
	RC			setNotification(bool fReset=false);
	RC			setReplicated();
	RC			setNoIndex();
	RC			deletePIN();
	RC			undelete();
	RC			refresh(bool);
	void		destroy();

	// helper functions
	Session		*getSes() const {return ses;}
	uint32_t	getMode() const {return mode;}
	void		operator delete(void *p) {if (((PIN*)p)->ses!=NULL) ((PIN*)p)->ses->free(p);}
	RC			modify(const Value *pv,ulong epos,ulong eid,ulong flags,Session *ses);
	void		setProps(const Value *pv,unsigned nv) {properties=(Value*)pv; nProperties=nv;}
	const PageAddr& getAddr() const {return addr;}
	__forceinline const Value *findProperty(PropertyID pid) const {return BIN<Value,PropertyID,ValCmp>::find(pid,properties,nProperties);}
	ElementID	getPrefix(StoreCtx *ctx) const {return id.pid==STORE_INVALID_PID||id.ident==STORE_INVALID_IDENTITY?ctx->getPrefix():StoreCtx::genPrefix(ushort(id.pid>>48));}
	static PIN*	getPIN(const PID& id,VersionID vid,Session *ses,ulong mode=0);
	static const Value *findElement(const Value *pv,ulong eid) {
		assert(pv!=NULL && pv->type==VT_ARRAY);
		if (pv->length!=0) {
			if (eid==STORE_FIRST_ELEMENT) return &pv->varray[0];
			if (eid==STORE_LAST_ELEMENT) return &pv->varray[pv->length-1];
			for (ulong i=0; i<pv->length; i++) if (pv->varray[i].eid==eid) return &pv->varray[i];
		}
		return NULL;
	}
	static	Value*	normalize(const Value *pv,uint32_t& nv,ulong f,ElementID prefix,MemAlloc *ma);
	static	int		__cdecl cmpLength(const void *v1, const void *v2);
	static	ulong	getCardinality(const PID& ref,ulong propID);
	static	const	PID defPID;
	friend	class	EncodePB;
	friend	class	ProtoBufStreamIn;
	friend	class	SyncStreamIn;
	friend	class	ServerStreamIn;
	friend	class	FullScan;
	friend	class	QueryPrc;
	friend	class	CursorNav;
	friend	class	Cursor;
	friend	class	Stmt;
	friend	class	SessionX;
	friend	class	Classifier;
	friend	class	ClassPropIndex;
	friend	class	NetMgr;
	friend	class	RPIN;
	friend	class	PINEx;
	friend	class	TransOp;
};

inline bool isRemote(const PID& id) {return id.ident!=STORE_OWNER&&id.ident!=STORE_INVALID_IDENTITY||id.pid!=STORE_INVALID_PID&&ushort(id.pid>>48)!=StoreCtx::get()->storeID;}

/**
 * Class SessionX implements ISession interface
 * @see method descriptions in affinity.h
 */

class SessionX : public ISession
{
	Session			*const ses;
private:
	void operator	delete(void *p) {}
	bool			login(const char *ident,const char *pwd);
	bool			checkAdmin() {return ses!=NULL&&ses->identity==STORE_OWNER;}
public:
	SessionX(Session *s) : ses(s) {}
	friend		ISession	*ISession::startSession(AfyDBCtx,const char*,const char*);
	static		SessionX	*create(Session *ses) {return new(ses) SessionX(ses);}
	ISession	*clone(const char* =NULL) const;
	RC			attachToCurrentThread();
	RC			detachFromCurrentThread();

	void		setInterfaceMode(unsigned);
	unsigned	getInterfaceMode() const;
	RC			setURIBase(const char *ns);
	RC			addURIPrefix(const char *name,const char *URIprefix);
	void		setDefaultExpiration(uint64_t defExp);
	void		changeTraceMode(unsigned mask,bool fReset=false);
	void		setTrace(ITrace *);
	void		terminate();

	RC			mapURIs(unsigned nURIs,URIMap URIs[]);
	RC			getURI(uint32_t,char buf[],size_t& lbuf);

	IdentityID	getIdentityID(const char *identity);
	RC			impersonate(const char *identity);
	IdentityID	storeIdentity(const char *ident,const char *pwd,bool fMayInsert=true,const unsigned char *cert=NULL,unsigned lcert=0);
	IdentityID	loadIdentity(const char *identity,const unsigned char *pwd,unsigned lPwd,bool fMayInsert=true,const unsigned char *cert=NULL,unsigned lcert=0);
	RC			setInsertPermission(IdentityID,bool fMayInsert=true);
	size_t		getStoreIdentityName(char buf[],size_t lbuf);
	size_t		getIdentityName(IdentityID,char buf[],size_t lbuf);
	size_t		getCertificate(IdentityID,unsigned char buf[],size_t lbuf);
	RC			changePassword(IdentityID,const char *oldPwd,const char *newPwd);
	RC			changeCertificate(IdentityID,const char *pwd,const unsigned char *cert,unsigned lcert);
	RC			changeStoreIdentity(const char *newIdentity);
	IdentityID	getCurrentIdentityID() const;

	unsigned	getStoreID(const PID&);
	unsigned	getLocalStoreID();

	IStmt		*createStmt(STMT_OP=STMT_QUERY,unsigned mode=0);
	IStmt		*createStmt(const char *queryStr,const URIID *ids=NULL,unsigned nids=0,CompilationError *ce=NULL);
	IExprTree	*expr(ExprOp op,unsigned nOperands,const Value *operands,unsigned flags=0);
	IExprTree	*createExprTree(const char *str,const URIID *ids=NULL,unsigned nids=0,CompilationError *ce=NULL);
	IExpr		*createExpr(const char *str,const URIID *ids=NULL,unsigned nids=0,CompilationError *ce=NULL);
	IExpr		*createExtExpr(uint16_t langID,const byte *body,uint32_t lBody,uint16_t flags);
	RC			getTypeName(ValueType type,char buf[],size_t lbuf);
	void		abortQuery();

	RC			execute(const char *str,size_t lstr,char **result=NULL,const URIID *ids=NULL,unsigned nids=0,
						const Value *params=NULL,unsigned nParams=0,CompilationError *ce=NULL,uint64_t *nProcessed=NULL,
						unsigned nProcess=~0u,unsigned nSkip=0);

	RC			createInputStream(IStreamIn *&in,IStreamIn *out=NULL,size_t lbuf=0);

	RC			getClassID(const char *className,ClassID& cid);
	RC			enableClassNotifications(ClassID,unsigned notifications);
	RC			rebuildIndices(const ClassID *cidx=NULL,unsigned nClasses=0);
	RC			rebuildIndexFT();
	RC			createIndexNav(ClassID,IndexNav *&nav);
	RC			listValues(ClassID cid,PropertyID pid,IndexNav *&ven);
	RC			listWords(const char *query,StringEnum *&sen);
	RC			getClassInfo(ClassID,IPIN*&);
	
	IPIN		*getPIN(const PID& id,unsigned=0);
	IPIN		*getPIN(const Value& id,unsigned=0);
	IPIN		*getPINByURI(const char *uri,unsigned mode=0);
	RC			getValues(Value *vals,unsigned nVals,const PID& id);
	RC			getValue(Value& res,const PID& id,PropertyID,ElementID=STORE_COLLECTION_ID);
	RC			getValue(Value& res,const PID& id);
	RC			getPINClasses(ClassID *&clss,unsigned& nclss,const PID& id);
	bool		isCached(const PID& id);
	IPIN		*createUncommittedPIN(Value *values=NULL,unsigned nValues=0,unsigned mode=0,const PID *original=NULL);
	RC			createPIN(PID& res,const Value values[],unsigned nValues,unsigned mode=0,const AllocCtrl* =NULL);
	RC			commitPINs(IPIN * const *newPins,unsigned nNew,unsigned mode=0,const AllocCtrl* =NULL,const Value *params=NULL,unsigned nParams=0);
	RC			modifyPIN(const PID& id,const Value *values,unsigned nValues,unsigned mode=0,const ElementID *eids=NULL,unsigned *pNFailed=NULL,const Value *params=NULL,unsigned nParams=0);
	RC			deletePINs(IPIN **pins,unsigned nPins,unsigned mode=0);
	RC			deletePINs(const PID *pids,unsigned nPids,unsigned mode=0);
	RC			undeletePINs(const PID *pids,unsigned nPids);
	RC			setPINAllocationParameters(const AllocCtrl *ac);

	RC			setIsolationLevel(TXI_LEVEL);
	RC			startTransaction(TX_TYPE=TXT_READWRITE,TXI_LEVEL=TXI_DEFAULT);
	RC			commit(bool fAll);
	RC			rollback(bool fAll);

	RC			reservePage(uint32_t);

	RC			copyValue(const Value& src,Value& dest);
	RC			convertValue(const Value& oldValue,Value& newValue,ValueType newType);
	RC			parseValue(const char *p,size_t l,Value& res,CompilationError *ce=NULL);
	RC			parseValues(const char *p,size_t l,Value *&res,unsigned& nValues,CompilationError *ce=NULL,char delimiter=',');
	int			compareValues(const Value& v1,const Value& v2,bool fNCase=false);
	void		freeValues(Value *vals,unsigned nVals);
	void		freeValue(Value& val);

	void		setTimeZone(int64_t tz);
	RC			convDateTime(uint64_t dt,DateTime& dts,bool fUTC=true) const;
	RC			convDateTime(const DateTime& dts,uint64_t& dt,bool fUTC=true) const;

	RC			setStopWordTable(const char **words,uint32_t nWords,PropertyID pid=STORE_INVALID_PROPID,
													bool fOverwriteDefault=false,bool fSessionOnly=false);

	void		*alloc(size_t);
	void		*realloc(void *,size_t);
	void		free(void *);
};

};

#endif
