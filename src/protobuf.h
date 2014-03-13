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

Written by Mark Venguerov 2010 - 2012

**************************************************************************************/

#ifndef _PROTOBUF_H_

#include "pin.h"
#include "stmt.h"
#include "expr.h"
#include "parser.h"

using namespace Afy;

namespace AfyKernel {
	
/**
 * various types of protobuf streams
 */
enum StreamInType
{
	SITY_NORMAL, SITY_DUMPLOAD, SITY_REPLICATION
};

enum MODOP 
{
	MODOP_QUERY=-1,MODOP_INSERT, MODOP_UPDATE, MODOP_DELETE, MODOP_COPY, MODOP_UNDELETE
};

enum TXOP
{
	TXOP_START=1, TXOP_COMMIT, TXOP_COMMIT_ALL, TXOP_ROLLBACK, TXOP_ROLLBACK_ALL, TXOP_START_READONLY, TXOP_START_MODIFYCLASS
};

enum RTTYPE
{
	RTT_DEFAULT, RTT_PINS, RTT_COUNT, RTT_PIDS, RTT_SRCPINS, RTT_VALUES
};

#define	DEFAULT_OBUF_SIZE		0x1000
#define	DEFAULT_PIN_BATCH_SIZE	0x2000

#define	STACK_DEPTH			20
#define	MAX_RESIDUAL_SIZE	32
#define	SAFETY_MARGIN_OUT	20

enum SType
{
	ST_PBSTREAM, ST_STRMAP, ST_PIN, ST_VALUE, ST_VARRAY, ST_MAP, ST_PID, ST_REF, ST_STREDIT, ST_STREAM,
	ST_STMT, ST_UID, ST_RESULT, ST_COMPOUND, ST_STATUS, ST_RESPAGES, ST_ENUM, ST_FLUSH, ST_TX
};

struct Result
{
	RC			rc;
	uint64_t	cnt;
	MODOP		op;
};

class EncodePB 
{
protected:
	Session	*const	ses;
	unsigned	mode;
	uint64_t	cid;
	RTTYPE		rtt;
	struct	OState {
		SType		type;
		bool		fCid;
		bool		fArray;
		uint16_t	tag;
		uint32_t	state;
		uint32_t	idx;
		union {
			const	void	*obj;
			const	PIN		*pin;
			const	Value	*pv;
			const	PID		*id;
			const	Result	*res;
			IStream			*str;
			const	Stmt	*stmt;
		};
	}	os,stateStack[STACK_DEPTH];
	uint32_t	sidx;
	StackAlloc	cache;
	struct IDCache {
		MemAlloc *const ma;	
		uint32_t *ids;
		unsigned nids;
		unsigned xids;
		uint32_t *newIds;
		unsigned nnids;
		unsigned xnids;
		IDCache(MemAlloc *m,unsigned x,unsigned xn) : ma(m),ids(NULL),nids(0),xids(x),newIds(NULL),nnids(0),xnids(xn) {}
	};
	IDCache		propCache;
	IDCache		identCache;
	size_t		lRes;
	const byte	*pRes;
	size_t		lRes2;
	const byte	*pRes2;
	byte		rbuf[MAX_RESIDUAL_SIZE];
	byte		idbuf[XPINREFSIZE];
	uint64_t	pinSize;
	byte		*copied;
	size_t		lCopied;
	size_t		xCopied;
	uint32_t	code;
	bool		fDone;
private:
	__forceinline	void	push_state(SType ot,const void *obj,uint16_t tag,bool fA=false) {assert(sidx<STACK_DEPTH); stateStack[sidx++]=os; os.type=ot; os.fCid=false; os.fArray=fA; os.tag=tag; os.state=0; os.idx=0; os.obj=obj;}
	__forceinline	static	uint16_t tag(const Value& v) {return v.type==VT_STREAM?tags[v.stream.is->dataType()]:v.type<VT_ALL?tags[v.type]:0;}
	__forceinline	static	byte wtype(const Value& v) {return types[v.type==VT_STREAM?(uint8_t)v.stream.is->dataType():v.type];}
	uint32_t length(const PID& id) {return 1+afy_len64(id.pid)+(id.ident!=STORE_OWNER?length(id.ident,false):0);}
	uint64_t length(const Value& v,bool fArray);
	uint32_t length(uint32_t id,bool fProp=true);
	void setID(uint32_t id,bool fProp=true);
	uint32_t length(const Expr *exp);
	uint32_t length(const Stmt *stmt);
	static const byte tags[VT_ALL];
	static const byte types[VT_ALL];
public:
	EncodePB(Session *s,unsigned md=0) : ses(s),mode(md),cid(0),rtt(RTT_DEFAULT),sidx(0),cache(s),propCache(&cache,100,30),identCache(&cache,10,5),
		lRes(0),pRes(NULL),lRes2(0),pRes2(0),copied(NULL),lCopied(0),xCopied(0),code(0),fDone(false) {os.type=ST_PBSTREAM; os.fCid=false; os.fArray=false; os.state=0; os.idx=0; os.obj=NULL;}
	~EncodePB() {}
	RC encode(unsigned char *buf,size_t& lbuf);
	void cleanup();
	bool isDone() const {return fDone;}
	void set(const PIN *p,uint64_t ci=0,bool fC=false,RTTYPE r=RTT_DEFAULT) {assert(sidx==0); os.type=ST_PIN; os.pin=p; os.state=0; cid=ci; os.fCid=fC; rtt=r;}
	void set(const Result *r,uint64_t ci=0,bool fC=false) {assert(sidx==0); os.type=ST_RESULT; os.res=r; os.state=0; cid=ci; os.fCid=fC;}
	void set(const Value *pv,uint64_t ci=0,bool fC=false) {assert(sidx==0); os.type=ST_VALUE; os.pv=pv; os.state=0; cid=ci; os.fCid=fC;}
	void set(const Stmt *st,uint64_t ci=0,bool fC=false) {assert(sidx==0); os.type=ST_STMT; os.stmt=st; os.state=0; cid=ci; os.fCid=fC;}
	// compound, status
};

enum IN_STATE {ST_TAG, ST_LEN, ST_READ, ST_RET};

struct URIIDMapElt
{
	HChain<URIIDMapElt>	list;
	URIID				id,mapped;
	URIIDMapElt() : list(this) {}
	URIID	getKey() const {return id;}
};

typedef HashTab<URIIDMapElt,URIID,&URIIDMapElt::list> URIIDMap;

struct IdentityIDMapElt
{
	HChain<IdentityIDMapElt>	list;
	IdentityID					id,mapped;
	IdentityIDMapElt() : list(this) {}
	IdentityID	getKey() const {return id;}
};

typedef HashTab<IdentityIDMapElt,IdentityID,&IdentityIDMapElt::list> IdentityIDMap;

struct PINIn
{
	PID			id;
	uint32_t	mode;
	Value		*properties;
	uint32_t	nProperties;
	uint64_t	cid;
	bool		fCid;
	RTTYPE		rtt;
	uint8_t		op;
};

struct ValueIn
{
	Value		v;
	uint64_t	cid;
	bool		fCid;
};

struct StmtIn
{
	Stmt		*stmt;
	char		*str;
	size_t		lstr;
	PropertyID	*uids;
	unsigned	nUids;
	Value		*params;
	unsigned	nParams;
	uint32_t	limit;
	uint32_t	offset;
	uint32_t	mode;
	uint64_t	cid;
	bool		fCid;
	RTTYPE		rtt;
	bool		fAbort;
};

struct IState
{
	SType		type;
	uint32_t	tag;
	uint32_t	idx;
	uint32_t	fieldMask;
	uint64_t	msgEnd;
	union {
		void	*obj;
		Value	*pv;
		PID		*id;
	};
};

class DecodePB
{
protected:
	StoreCtx 		*const	ctx;
	MemAlloc		*const	ma;
	StreamInType	const	stype;

	IN_STATE		inState;
	union {
		PINIn		pin;
		StmtIn		stmt;
		ValueIn		val;
		uint64_t	u64;
	}				u;
	size_t			ileft;

private:
	IState			is;
	IState			stateStack[STACK_DEPTH];
	uint32_t		sidx;
	uint64_t		lField;
	uint64_t		val;
	uint32_t		vsht;
	uint64_t		offset;
	uint64_t		left;
	byte			*sbuf;
	union {
		uint64_t	u64;
		uint32_t	u32;
		int64_t		i64;
		double		d;
		float		f;
		byte		id[XPINREFSIZE+2];
	}				buf;
	uint8_t			defType;
	const	byte	*savedIn;
	const	byte	*savedBuf;

	byte			*mapBuf;
	size_t			lMapBuf;
	uint64_t		lSave;
	IdentityID		owner;
	uint16_t		storeID;
	URIIDMap		uriMap;
	IdentityIDMap	identMap;
private:
	__forceinline void advance() {inState=ST_TAG; val=0; vsht=0;}
	__forceinline void set_skip() {if (inState==ST_LEN) {sbuf=NULL; left=lField;} else advance();}
	__forceinline void push_state(SType ot,void *obj,size_t sht) {
		assert(sidx<STACK_DEPTH); stateStack[sidx++]=is; is.type=ot; is.tag=~0u; is.idx=0; is.msgEnd=offset+sht+lField; is.fieldMask=0; is.obj=obj; advance();
	}
	__forceinline void save_state(const byte *in,const byte *buf,size_t lft) {savedIn=in; savedBuf=lft!=0?buf:NULL; ileft=lft; inState=ST_RET;}
	__forceinline void restore_state(const unsigned char *&in) {if (in==savedBuf) in=savedIn; inState=ST_TAG;}
	__forceinline Value *initV(Value *v) const {v->setEmpty(); return v;}
	uint32_t map(uint32_t id,bool fProp=true);
	RC addToMap(bool fProp);
public:
	DecodePB(StoreCtx *ct,MemAlloc *m,StreamInType st=SITY_NORMAL) : ctx(ct),ma(m),stype(st),inState(ST_TAG),sidx(0),lField(0),val(0),vsht(0),offset(0),
		left(0),sbuf(NULL),defType(VT_ANY),savedIn(NULL),savedBuf(NULL),ileft(0),mapBuf(NULL),lMapBuf(0),lSave(0),owner(STORE_OWNER),storeID(0),uriMap(64,m,false),identMap(32,m,false) {
		is.type=ST_PBSTREAM; is.tag=~0u; is.idx=0; is.msgEnd=~0ULL; is.fieldMask=0; is.obj=NULL;
	}
	SType getSType() const {return is.type;}
	RC decode(const unsigned char *in,size_t lbuf,IMemAlloc *ra);
	void cleanup();
};

class ProtoBufStreamOut : public IStreamOut 
{
	Session		*const	ses;
	EncodePB			enc;
	Cursor				*ic;
	ValueC				res;
	Result				result;
	bool				fRes;
public:
	ProtoBufStreamOut(Session *s,Cursor *pr=NULL,unsigned md=0) : ses(s),enc(s,md),ic(pr),fRes(false)
		{result.rc=RC_OK; result.cnt=0; result.op=MODOP_QUERY;}
	~ProtoBufStreamOut() {if (ic!=NULL) ic->destroy();}
	RC next(unsigned char *buf,size_t& lbuf);
	void destroy() {try {this->~ProtoBufStreamOut(); ses->free(this);} catch (...) {}}
};

class ProcessStream : public StackAlloc, public IStreamIn, public DecodePB
{
	struct OInfo {
		uint64_t	cid;
		bool		fCid;
		RTTYPE		rtt;
	};
protected:
	Session		*const	ses;
	IStreamIn	*const	outStr;
	const	size_t		lobuf;
	EncodePB			enc;
	byte				*obuf;
	size_t				obleft;
	BatchInsert			pins;
	DynArray<OInfo,100>	oi;
	unsigned			txLevel;
private:
	RC resultOut(const Result& res,uint64_t cid,bool fCid);
	RC pinOut(PIN *pin,uint64_t cid,bool fCid,RTTYPE rtt);
	RC valueOut(const Value *v,uint64_t cid,bool fCid);
	RC commitPINs();
public:
	ProcessStream(Session *s,IStreamIn *o=NULL,size_t lo=0,StreamInType st=SITY_NORMAL) : StackAlloc(s),DecodePB(s->getStore(),this,st),
				ses(s),outStr(o),enc(s,0),lobuf(lo==0?DEFAULT_OBUF_SIZE:lo),obuf(NULL),obleft(0),pins(s),oi((MemAlloc*)s),txLevel(0)
	{
		if (o!=NULL && (obuf=(byte*)StackAlloc::malloc(lobuf))==NULL) throw RC_NORESOURCES; obleft=lobuf;
	}
	void	operator delete(void *p) {if (p!=NULL) ((ProcessStream*)p)->ses->free(p);}
	RC		next(const unsigned char *in,size_t lbuf);
	void	destroy() {try {delete this;} catch (...) {}}
};
	
class ServiceEncodePB : public IService::Processor, public EncodePB
{
public:
	ServiceEncodePB(Session *s,unsigned md) : EncodePB(s,md) {}
	RC		invoke(IServiceCtx *ctx,const Value& inp,Value& out,unsigned& mode);
	void	cleanup(IServiceCtx *ctx,bool fDestroying);
};
	
class ServiceDecodePB : public IService::Processor, public DecodePB
{
public:
	ServiceDecodePB(StoreCtx *ct,MemAlloc *m,StreamInType st=SITY_NORMAL) : DecodePB(ct,m,st) {}
	RC		invoke(IServiceCtx *ctx,const Value& inp,Value& out,unsigned& mode);
	void	cleanup(IServiceCtx *ctx,bool fDestroying);
};

class ProtobufService : public IService
{
	StoreCtx	*const	ctx;
public:
	ProtobufService(StoreCtx *ct) : ctx(ct) {}
	RC create(IServiceCtx *ctx,uint32_t& dscr,IService::Processor *&ret);
};

};

#endif
