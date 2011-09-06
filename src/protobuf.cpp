/**************************************************************************************

Copyright © 2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2010

**************************************************************************************/

#include "maps.h"
#include "blob.h"
#include "txmgr.h"
#include "queryprc.h"
#include "stmt.h"
#include "expr.h"
#include "parser.h"

enum MODOP 
{
	MODOP_QUERY=-1,MODOP_INSERT, MODOP_UPDATE, MODOP_DELETE, MODOP_COPY, MODOP_UNDELETE
};

static const STMT_OP stmtOp[] =
{
	STMT_INSERT, STMT_UPDATE, STMT_DELETE, STMT_INSERT, STMT_UNDELETE
};

static const MODOP modOp[STMT_OP_ALL] =
{
	MODOP_QUERY, MODOP_INSERT, MODOP_UPDATE, MODOP_DELETE, MODOP_UNDELETE, MODOP_QUERY, MODOP_QUERY, MODOP_QUERY, MODOP_QUERY
};

enum TXOP
{
	TXOP_START=1, TXOP_COMMIT, TXOP_COMMIT_ALL, TXOP_ROLLBACK, TXOP_ROLLBACK_ALL, TXOP_START_READONLY, TXOP_START_MODIFYCLASS
};

enum RTTYPE
{
	RTT_PINS, RTT_COUNT, RTT_PIDS, RTT_SRCPINS, RTT_VALUES
};

#define	DEFAULT_OBUF_SIZE		0x1000
#define	DEFAULT_PIN_BATCH_SIZE	0x2000

using namespace MVStoreKernel;

#define	_V(a)		((a)<<3|0)
#define	_L(a)		((a)<<3|2)
#define	_S(a)		((a)<<3|5)
#define	_D(a)		((a)<<3|1)

#define	OWNER_TAG		_L(1)
#define	STOREID_TAG		_V(2)
#define	PIN_TAG			_L(3)
#define	STRPROP_TAG		_L(4)
#define	STRIDENT_TAG	_L(5)
#define	STMT_TAG		_L(6)
#define TXOP_TAG		_V(7)
#define RESULT_TAG		_L(8)
#define COMPOUND_TAG	_L(9)
#define STATUS_TAG		_L(10)
#define	FLUSH_TAG		_V(11)
#define	RESPAGES_TAG	_L(12)
#define	RVALUES_TAG		_L(13)

#define	MAP_STR_TAG		_L(1)
#define	MAP_ID_TAG		_V(2)

#define OP_TAG			_V(1)
#define	ID_TAG			_L(2)
#define	MODE_TAG		_V(3)
#define	STAMP_TAG		_V(4)
#define	NVALUES_TAG		_V(5)
#define	VALUES_TAG		_L(6)
#define	CID_TAG			_V(7)
#define	RTT_TAG			_V(8)

#define TYPE_TAG		_V(1)
#define	PROPERTY_TAG	_V(2)
#define	VALUE_OP_TAG	_V(17)
#define	EID_TAG			_V(18)
#define	META_TAG		_V(19)
#define	STREDIT_TAG		_L(20)
#define	UNITS_TAG		_V(21)

#define	PID_ID_TAG		_V(1)
#define	PID_IDENT_TAG	_V(2)

#define REF_PID_TAG		_L(1)
#define	REF_PROP_TAG	_V(2)
#define	REF_EID_TAG		_V(3)
#define	REF_VID_TAG		_V(4)

#define	VARRAY_L_TAG	_V(1)
#define	VARRAY_V_TAG	_L(2)

#define	STMT_STR_TAG	_L(1)
#define	STMT_CID_TAG	_V(2)
#define	STMT_RTT_TAG	_V(3)
#define	STMT_UID_TAG	_V(4)
#define	STMT_PARAM_TAG	_L(5)
#define	STMT_LIMIT_TAG	_V(6)
#define	STMT_OFFSET_TAG	_V(7)
#define	STMT_MODE_TAG	_V(8)
#define	STMT_ABORT_TAG	_V(9)

#define	RES_CID_TAG		_V(1)
#define	RES_ERR_TAG		_V(2)
#define	RES_COUNT_TAG	_V(3)
#define	RES_OP_TAG		_V(4)

#define	CMPD_NPINS_TAG	_V(1)
#define	CMPD_PIN_TAG	_L(2)
#define	CMPD_CID_TAG	_V(3)

#define	STA_CODE_TAG	_V(1)
#define	STA_CID_TAG		_V(2)

#define	RESPG_PAGE_TAG	_V(1)

const static byte tags[VT_ALL] = {
	0, _L(3), _L(4), _L(3), _V(6), _V(5), _V(6), _V(7), _V(8), _L(4), _S(9), _D(10), _V(16), _D(11), _D(12), _V(6), _V(6),
	_L(13), _L(13), _L(15), _L(15), _L(15), _L(15), _L(4), _L(4), _L(14), _L(14), _L(14), _L(14), 0, _V(6), 0, 0, 0
};
const static byte types[VT_ALL] = {
		VT_ERROR, VT_STRING, VT_BSTR, VT_URL, VT_ENUM, VT_INT, VT_UINT, VT_INT64, VT_UINT64, VT_DECIMAL, VT_FLOAT, VT_DOUBLE,
		VT_BOOL, VT_DATETIME, VT_INTERVAL, VT_URIID, VT_IDENTITY, VT_REFID, VT_REFID, VT_REFIDPROP, VT_REFIDPROP, VT_REFIDELT, VT_REFIDELT,
		VT_EXPR, VT_STMT, VT_ARRAY, VT_ARRAY, VT_STRUCT, VT_RANGE, VT_ERROR, VT_CURRENT, VT_REF, VT_ERROR, VT_ERROR,
};
#define	VT_REFCID	VT_PARAM

#define	STACK_DEPTH			20
#define	MAX_RESIDUAL_SIZE	32
#define	SAFETY_MARGIN_OUT	20

#define	VAR_OUT(a,b,c)	{if (po<ypo) {mv_enc16(po,a); b(po,c); assert(po<end);}											\
						else {byte *p=rbuf; mv_enc16(p,a); b(p,c); lRes=(size_t)(p-rbuf); size_t left=(size_t)(end-po);	\
						if (lRes<left) {memcpy(po,rbuf,lRes); po+=lRes; lRes=0;} else {memcpy(po,rbuf,left); lRes-=left; pRes=rbuf+left; return RC_OK;}}}
#define	FLO_OUT(a,b)	{if (po<ypo) {mv_enc16(po,a); memcpy(po,&b,4); po+=4; assert(po<end);}	\
						else {byte *p=rbuf; mv_enc16(p,a); memcpy(p,&b,4); p+=4;					\
						lRes=(size_t)(p-rbuf); size_t left=(size_t)(end-po); if (lRes<left) {memcpy(po,rbuf,lRes); po+=lRes; lRes=0;}	\
						else {memcpy(po,rbuf,left); lRes-=left; pRes=rbuf+left; return RC_OK;}}}
#define	DOU_OUT(a,b)	{if (po<ypo) {mv_enc16(po,a); memcpy(po,&b,8); po+=8; assert(po<end);}	\
						else {byte *p=rbuf; mv_enc16(p,a); memcpy(p,&b,8); p+=8;				\
						lRes=(size_t)(p-rbuf); size_t left=(size_t)(end-po); if (lRes<left) {memcpy(po,rbuf,lRes); po+=lRes; lRes=0;}	\
						else {memcpy(po,rbuf,left); lRes-=left; pRes=rbuf+left; return RC_OK;}}}
#define	F64_OUT(a,b)	{if (po<ypo) {mv_enc16(po,a); memcpy(po,&b,8); po+=8; assert(po<end);}\
						else {byte *p=rbuf; mv_enc16(p,a); memcpy(p,&b,8); p+=8;				\
						lRes=(size_t)(p-rbuf); size_t left=(size_t)(end-po); if (lRes<left) {memcpy(po,rbuf,lRes); po+=lRes; lRes=0;}	\
						else {memcpy(po,rbuf,left); lRes-=left; pRes=rbuf+left; return RC_OK;}}}
#define	BUF_OUT(a,b,c)	{if (po+c+7<=end) {mv_enc16(po,a); mv_enc32(po,c); memcpy(po,b,c); po+=c;}								\
						else if (po<ypo) {mv_enc16(po,a); mv_enc32(po,c); if (po+c<end) {memcpy(po,b,c); po+=c;}				\
						else {uint32_t l=uint32_t(end-po); if (l!=0) memcpy(po,b,l); lRes=c-l; pRes=(byte*)b+l; return RC_OK;}}	\
						else {byte *p=rbuf; mv_enc16(p,a); mv_enc32(p,c); lRes=(size_t)(p-rbuf); size_t left=(size_t)(end-po);	\
						if (lRes>left) {memcpy(po,rbuf,left); lRes-=left; pRes=rbuf+left; lRes2=c; pRes2=b; return RC_OK;}		\
						else {memcpy(po,rbuf,lRes); po+=lRes; if ((left-=lRes)>=c) {memcpy(po,b,c); po+=c;} else {				\
						if (left!=0) memcpy(po,b,left); lRes=c-left; pRes=(byte*)b+left; return RC_OK;}}}}

namespace MVStoreKernel
{

enum SType
{
	ST_MVSTREAM, ST_STRMAP, ST_PIN, ST_VALUE, ST_VARRAY, ST_PID, ST_REF, ST_STREDIT, ST_STREAM,
	ST_STMT, ST_UID, ST_RESULT, ST_COMPOUND, ST_STATUS, ST_RESPAGES
};

static	const uint32_t requiredFields[] = {0, 3, 0, 1, 1, 1, 3, 3, 4, 7, 0, 1, 1, 0, 0, 0, 0};
static	const uint32_t repeatedFields[] = {0x7FC, 0, 32, 0, 2, 0, 0, 2, 0, 0, 0, 12, 14, 0, 0, 2, 0};

struct Result
{
	RC			rc;
	uint64_t	cnt;
	MODOP		op;
};

class EncodePB 
{
	Session		*const	ses;
	ulong		mode;
	uint64_t	cid;
	bool		fCid;
	RTTYPE		rtt;
	struct	OState {
		SType		type;
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
		};
	}	os,stateStack[STACK_DEPTH];
	uint32_t	sidx;
	struct IDCache {
		Session	*const ses;
		uint32_t *ids;
		unsigned nids;
		unsigned xids;
		uint32_t *newIds;
		unsigned nnids;
		unsigned xnids;
		IDCache(Session *s,unsigned x,unsigned xn) : ses(s),ids(NULL),nids(0),xids(x),newIds(NULL),nnids(0),xnids(xn) {}
		~IDCache() {ses->free(ids); ses->free(newIds);}
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
	uint32_t	code;
private:
	__forceinline	void	push_state(SType ot,const void *obj,uint16_t tag) {assert(sidx<STACK_DEPTH); stateStack[sidx++]=os; os.type=ot; os.tag=tag; os.state=0; os.idx=0; os.obj=obj;}
	__forceinline	static	uint16_t tag(const Value& v) {return v.type==VT_STREAM?tags[v.stream.is->dataType()]:v.type<VT_ALL?tags[v.type]:0;}
	__forceinline	static	byte wtype(const Value& v) {return types[v.type==VT_STREAM?(uint8_t)v.stream.is->dataType():v.type];}
	uint32_t length(const PID& id) {return 1+mv_len64(id.pid)+(id.ident!=STORE_OWNER?length(id.ident,false):0);}
	uint64_t length(const Value& v,bool fArray) {
		uint64_t l=0ULL,ll; uint32_t u; uint64_t u64; const Value *cv;
		switch (v.type) {
		default: return 0ULL;
		case VT_STRING: case VT_BSTR: case VT_URL: l=v.length; break;
		case VT_ENUM:
		case VT_DECIMAL:
			//???
			return 0ULL;
		case VT_INT: u=mv_enc32zz(v.i); l=mv_len32(u); break;
		case VT_URIID: l=length(v.uid)-1; break;
		case VT_IDENTITY: l=length(v.iid,false)-1; break;
		case VT_UINT: l=mv_len32(v.ui); break;
		case VT_INT64: u64=mv_enc64zz(v.i64); l=mv_len64(u64); break;
		case VT_UINT64: l=mv_len64(v.ui64); break;
		case VT_FLOAT: l=4+(v.qval.units!=Un_NDIM?mv_len16(UNITS_TAG)+mv_len16(v.qval.units):0); break;
		case VT_DOUBLE: l=8+(v.qval.units!=Un_NDIM?mv_len16(UNITS_TAG)+mv_len16(v.qval.units):0); break;
		case VT_BOOL: l=1; break;
		case VT_DATETIME: l=8; break;
		case VT_INTERVAL: l=8; break;
		case VT_REF: l=length(v.pin->getPID()); break;
		case VT_REFID: l=length(v.id); break;
		case VT_REFELT: l=1+mv_len32(v.ref.eid);
		case VT_REFPROP: u=length(v.ref.pin->getPID()); l+=1+mv_len16(u)+u+length(v.ref.pid); break;
		case VT_REFIDELT: l=1+mv_len32(v.refId->eid);
		case VT_REFIDPROP: u=length(v.refId->id); l+=1+mv_len16(u)+u+length(v.refId->pid); break;
		case VT_STREAM: l=v.stream.is->length(); break;
		case VT_EXPR: l=length((Expr*)v.expr); break;
		case VT_STMT: l=length((Stmt*)v.stmt); break;
		case VT_ARRAY: case VT_RANGE:
			l=1+mv_len32(v.length);
			for (u=0; u<v.length; u++) {ll=length(v.varray[u],true); l+=1+mv_len64(ll)+ll;}
			if (fArray) return l; break;
		case VT_COLLECTION:
			l=v.nav->count(); l=1+mv_len32(l);
			for (cv=v.nav->navigate(GO_FIRST); cv!=NULL; cv=v.nav->navigate(GO_NEXT)) {ll=length(*cv,true); l+=1+mv_len64(ll)+ll;}
			if (fArray) return l; break;
		case VT_STRUCT:
			//???
			break;
		case VT_CURRENT: l=1; break;
		}
		uint32_t tg=tag(v);
		if ((tg&7)==2) l+=mv_len64(l);
		l+=fArray?v.eid!=~0u?mv_len16(EID_TAG)+mv_len32(v.eid):0:length(v.property)+(v.meta!=0?mv_len16(META_TAG)+mv_len16(v.meta):0);
		return l+mv_len16(tg)+mv_len16(TYPE_TAG)+1;
	}
	uint32_t length(uint32_t id,bool fProp=true) {
		if (id!=STORE_OWNER && (!fProp || id>PROP_SPEC_LAST)) {
			IDCache& cache=fProp?propCache:identCache; RC rc;
			if (BIN<uint32_t>::find(id,cache.ids,cache.nids)==NULL) {
				if ((rc=BIN<uint32_t>::insert(cache.newIds,cache.nnids,id,id,ses,&cache.xnids))!=RC_OK) {/*???*/}
			}
		}
		return 1+mv_len32(id);
	}
	void setID(uint32_t id,bool fProp=true) {
		if (fProp) {
			assert(id>PROP_SPEC_LAST);
			URI *uri=(URI*)ses->getStore()->uriMgr->ObjMgr::find(id);
			if (uri==NULL) {copied=(byte*)MVStoreKernel::strdup("???",ses); lCopied=3;}
			else {copied=(byte*)MVStoreKernel::strdup(uri->getName(),ses); lCopied=strlen((char*)copied); uri->release();}
		} else {
			assert(id!=STORE_OWNER);
			Identity *ident=(Identity*)ses->getStore()->identMgr->ObjMgr::find(id);
			if (ident==NULL) {copied=(byte*)MVStoreKernel::strdup("???",ses); lCopied=3;}
			else {copied=(byte*)MVStoreKernel::strdup(ident->getName(),ses); lCopied=strlen((char*)copied); ident->release();}
		}
		IDCache& cache=fProp?propCache:identCache; RC rc;
		if ((rc=BIN<uint32_t>::insert(cache.ids,cache.nids,id,id,ses,&cache.xids))!=RC_OK) {/* ??? */}
		code=id;
	}
	uint32_t length(const Expr *exp) {
		if (rtt==RTT_SRCPINS) {SOutCtx out(ses); return exp->render(0,out)==RC_OK?uint32_t(out.getCLen()):0;}
		// properties and identities ???
		return (uint32_t)exp->serSize();
	}
	uint32_t length(const Stmt *stmt) {
		if (rtt==RTT_SRCPINS) {SOutCtx out(ses,(ses->getItf()&ITF_SPARQL)!=0?SQ_SPARQL:SQ_SQL); return stmt->render(out)==RC_OK?(uint32_t)out.getCLen():0;}
		// properties and identities ???
		return (uint32_t)stmt->serSize();
	}
public:
	EncodePB(Session *s,ulong md=0) : ses(s),mode(md),cid(0),fCid(false),rtt(RTT_PINS),sidx(0),propCache(s,100,30),identCache(s,10,5),
		lRes(0),pRes(NULL),lRes2(0),pRes2(0),copied(NULL),lCopied(0),code(0) {os.type=ST_MVSTREAM; os.state=0; os.idx=0; os.obj=NULL;}
	~EncodePB() {if (copied!=NULL) ses->free(copied);}
	RC encode(unsigned char *buf,size_t& lbuf) {
		try {
			if (buf==NULL||lbuf==0) return RC_INVPARAM;
			byte *po=buf,*const end=buf+lbuf,*const ypo=end-SAFETY_MARGIN_OUT;
			if (lRes!=0) {
				if (lRes>lbuf) {memcpy(buf,pRes,lbuf); pRes+=lbuf; lRes-=lbuf; return RC_OK;}
				memcpy(po,pRes,lRes); po+=lRes; lRes=0;
				if (lRes2!=0) {
					size_t left=(size_t)(end-po);
					if (lRes2>left) {memcpy(po,pRes2,left); pRes=pRes2+left; lRes=lRes2-left; lRes2=0; return RC_OK;}
					memcpy(po,pRes2,lRes2); po+=lRes2; lRes2=0;
				}
			}
			Identity *ident; uint16_t sid,tg; uint64_t sz64; uint32_t sz,i; byte *p; const Value *cv; byte ty;
			for (;;) {
			again:
				switch (os.type) {
				case ST_MVSTREAM:
					assert(sidx==0);
					switch (os.state) {
					default: goto error;
					case 0:
						ident=(Identity*)ses->getStore()->identMgr->ObjMgr::find(STORE_OWNER); assert(ident!=NULL);
						copied=(byte*)MVStoreKernel::strdup(ident->getName(),ses); lCopied=strlen((char*)copied);
						os.state++; code=STORE_OWNER; push_state(ST_STRMAP,NULL,OWNER_TAG); continue;
					case 1:
						os.state++; if ((sid=ses->getStore()->storeID)!=0) VAR_OUT(STOREID_TAG,mv_enc16,sid);
					case 2:
						// reserved pages, if dumpload
						assert(sidx==0); lbuf-=po-buf; return RC_TRUE;
					}
				case ST_STRMAP:
					switch (os.state) {
					default: goto error;
					case 0:
						sz=1+mv_len16(lCopied)+(uint32_t)lCopied+1+mv_len32(code);	//??? which code
						os.state++; VAR_OUT(os.tag,mv_enc32,sz);
					case 1:
						assert(os.obj==NULL && copied!=NULL);
						os.state++; BUF_OUT(MAP_STR_TAG,copied,lCopied);
					case 2:
						ses->free(copied); copied=NULL;
						os.state++; VAR_OUT(MAP_ID_TAG,mv_enc32,code);
					case 3:
						break;
					}
					break;
				case ST_PIN:
					assert(os.pin!=NULL);
					switch (os.state) {
					default: goto error;
					case 0:
						sz=length(os.pin->id); pinSize=1+mv_len16(sz)+sz;
						if (rtt!=RTT_PIDS) {
							if (os.pin->mode!=0) pinSize+=1+mv_len32(os.pin->mode);
							if (os.pin->stamp!=0) pinSize+=1+mv_len32(os.pin->stamp);
							if (os.pin->nProperties!=0) pinSize+=1+mv_len32(os.pin->nProperties);
							if (os.pin->properties!=NULL) for (i=0; i<os.pin->nProperties; i++)
								{sz64=length(os.pin->properties[i],false); pinSize+=1+mv_len64(sz64)+sz64;}		// save ???
						}
						if (fCid && sidx==0) pinSize+=1+mv_len64(cid);
						os.state++; os.idx=0;
					case 1:
						while (os.idx<propCache.nnids)
							{setID(propCache.newIds[os.idx++]); push_state(ST_STRMAP,NULL,STRPROP_TAG); goto again;}
						os.state++; os.idx=0; propCache.nnids=0;
					case 2:
						while (os.idx<identCache.nnids)
							{setID(identCache.newIds[os.idx++],false); push_state(ST_STRMAP,NULL,STRIDENT_TAG); goto again;}
						os.idx=0; identCache.nnids=0;
						os.state++; VAR_OUT(PIN_TAG,mv_enc64,pinSize);
					case 3:
						os.state++; push_state(ST_PID,&os.pin->id,ID_TAG); continue;
					case 4:
						os.state++; if (rtt!=RTT_PIDS && os.pin->mode!=0) VAR_OUT(MODE_TAG,mv_enc32,os.pin->mode);
					case 5:
						os.state++; if (rtt!=RTT_PIDS && os.pin->stamp!=0) VAR_OUT(STAMP_TAG,mv_enc32,os.pin->stamp);
					case 6:
						os.state++; if (rtt!=RTT_PIDS && os.pin->nProperties!=0) VAR_OUT(NVALUES_TAG,mv_enc32,os.pin->nProperties);
					case 7:
						if (rtt!=RTT_PIDS && os.pin->properties!=NULL) while (os.idx<os.pin->nProperties)
							{push_state(ST_VALUE,&os.pin->properties[os.idx++],VALUES_TAG); goto again;}
						os.state++;
					case 8:
						os.state++; if (fCid && sidx==0) VAR_OUT(CID_TAG,mv_enc64,cid);
					case 9:
						fCid=false; rtt=RTT_PINS;
						if (sidx==0) {lbuf-=po-buf; return RC_TRUE;}
						break;
					}
				case ST_VALUE:
					switch (os.state) {
					default: goto error;
					case 0:
						sz64=length(*os.pv,os.tag==VARRAY_V_TAG);
						if (fCid && sidx==0) sz64+=1+mv_len64(cid);
						os.state++; VAR_OUT(os.tag,mv_enc64,sz64);
					case 1:
						os.state++; ty=wtype(*os.pv); VAR_OUT(TYPE_TAG,mv_enc8,ty);
					case 2:
						os.state++; 
						if (os.tag!=VARRAY_V_TAG) VAR_OUT(PROPERTY_TAG,mv_enc32,os.pv->property);
					case 3:
						tg=tag(*os.pv); os.state++; copied=NULL;
						switch (os.pv->type) {
						case VT_ANY: break;
						case VT_STRING: case VT_BSTR: case VT_URL:
							BUF_OUT(tg,os.pv->bstr,os.pv->length); break;
						case VT_INT:
							sz=mv_enc32zz(os.pv->i); VAR_OUT(tg,mv_enc32,sz); break;
						case VT_UINT: case VT_URIID: case VT_IDENTITY: case VT_CURRENT:
							VAR_OUT(tg,mv_enc32,os.pv->ui); break;
						case VT_INT64:
							sz64=mv_enc64zz(os.pv->i64); VAR_OUT(tg,mv_enc64,sz64); break;
						case VT_UINT64:
							VAR_OUT(tg,mv_enc64,os.pv->ui64); break;
						case VT_FLOAT:
							FLO_OUT(tg,os.pv->f); break;
						case VT_DOUBLE:
							DOU_OUT(tg,os.pv->d); break;
						case VT_BOOL:
							VAR_OUT(tg,mv_enc8,os.pv->b); break;
						case VT_DATETIME: case VT_INTERVAL:
							F64_OUT(tg,os.pv->ui64); break;
						case VT_REF:
							push_state(ST_PID,&os.pv->pin->getPID(),tg); continue;
						case VT_REFID:
							push_state(ST_PID,&os.pv->id,tg); continue;
						case VT_REFPROP:
						case VT_REFIDPROP:
						case VT_REFELT:
						case VT_REFIDELT:
							push_state(ST_REF,os.pv,tg); continue;
						case VT_EXPR:
							if (rtt!=RTT_SRCPINS) {
								lCopied=((Expr*)os.pv->expr)->serSize();
								if ((copied=(byte*)ses->malloc(lCopied))==NULL) return RC_NORESOURCES;
								p=((Expr*)os.pv->expr)->serialize(copied); assert(copied+lCopied==p);
							} else {
								SOutCtx out(ses); RC rc; size_t lC;
								if ((rc=((Expr*)os.pv->expr)->render(0,out))!=RC_OK) return rc;
								copied=out.result(lC); lCopied=lC; tg=tags[VT_STRING];
							}
							BUF_OUT(tg,copied,lCopied); break;
						case VT_STMT:
							if (rtt!=RTT_SRCPINS) {
								lCopied=((Stmt*)os.pv->stmt)->serSize();
								if ((copied=(byte*)ses->malloc(lCopied))==NULL) return RC_NORESOURCES;
								p=((Stmt*)os.pv->stmt)->serialize(copied); assert(copied+lCopied==p);
							} else {
								SOutCtx out(ses,(ses->getItf()&ITF_SPARQL)!=0?SQ_SPARQL:SQ_SQL); RC rc; size_t lC;
								if ((rc=((Stmt*)os.pv->stmt)->render(out))!=RC_OK) return rc;
								copied=out.result(lC); lCopied=lC; tg=tags[VT_STRING];
							}
							BUF_OUT(tg,copied,lCopied); break;
						case VT_ARRAY:
						case VT_COLLECTION:
						case VT_RANGE:
							push_state(ST_VARRAY,os.pv,tg); continue;
						case VT_STRUCT:
							//???
							break;
						case VT_STREAM:
							push_state(ST_STREAM,os.pv->stream.is,tg); continue;
						case VT_ENUM:
						case VT_DECIMAL:
							//???
							break;
						default:
							//???
							break;
						}
					case 4:
						os.state++; if (copied!=NULL) {ses->free(copied); copied=NULL;}
						if ((os.pv->type==VT_FLOAT||os.pv->type==VT_DOUBLE) && os.pv->qval.units!=Un_NDIM)
							VAR_OUT(UNITS_TAG,mv_enc16,os.pv->qval.units);
					case 5:
						os.state++;
						if (os.tag==VARRAY_V_TAG) {
							if (os.pv->eid!=~0u) VAR_OUT(EID_TAG,mv_enc32,os.pv->eid);
						} else {
							if (os.pv->meta!=0) VAR_OUT(META_TAG,mv_enc8,os.pv->meta);
						}
					case 6:
						os.state++; if (fCid && sidx==0) VAR_OUT(CID_TAG,mv_enc64,cid);
					case 7:
						fCid=false; if (sidx==0) {lbuf-=po-buf; return RC_TRUE;}
						break;
					}
					break;
				case ST_VARRAY:
					switch (os.state) {
					default: goto error;
					case 0:
						assert(os.pv->type==VT_ARRAY||os.pv->type==VT_COLLECTION||os.pv->type==VT_RANGE);
						os.state++; sz64=length(*os.pv,true); VAR_OUT(os.tag,mv_enc64,sz64);
					case 1:
						sz=os.pv->type==VT_COLLECTION?os.pv->nav->count():os.pv->length;
						os.state++; VAR_OUT(VARRAY_L_TAG,mv_enc32,sz);
					case 2:
						os.state++;
						if (os.pv->type!=VT_COLLECTION) os.idx=0;
						else {
							if ((cv=os.pv->nav->navigate(GO_FIRST))==NULL) break;
							push_state(ST_VALUE,cv,VARRAY_V_TAG); continue;
						}
					case 3:
						if (os.pv->type==VT_COLLECTION) {
							// release after each?
							while ((cv=os.pv->nav->navigate(GO_NEXT))!=NULL)
								{push_state(ST_VALUE,cv,VARRAY_V_TAG); goto again;}
							os.pv->nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID);
						} else {
							while (os.idx<os.pv->length)
								{push_state(ST_VALUE,&os.pv->varray[os.idx++],VARRAY_V_TAG); goto again;}
						}
						break;
					}
					break;
				case ST_STREAM:
					switch (os.state) {
					case 0: 
						os.state++; sz64=os.str->length(); VAR_OUT(os.tag,mv_enc64,sz64);
					case 1:
						sz=uint32_t(end-po); if ((i=(uint32_t)os.str->read(po,sz))>=sz) return RC_OK;
						po+=i; break;
					}
					break;
				case ST_PID:
					switch (os.state) {
					default: goto error;
					case 0:
						os.state++; sz=length(*os.id); VAR_OUT(os.tag,mv_enc16,sz);
					case 1:
						os.state++; VAR_OUT(PID_ID_TAG,mv_enc64,os.id->pid);
					case 2:
						os.state++; if (os.id->ident!=STORE_OWNER) VAR_OUT(PID_IDENT_TAG,mv_enc32,os.id->ident);
					case 3:
						break;
					}
					break;
				case ST_REF:
					assert(os.pv->type==VT_REFPROP||os.pv->type==VT_REFIDPROP||os.pv->type==VT_REFELT||os.pv->type==VT_REFIDELT);
					switch (os.state) {
					default: goto error;
					case 0:
						if (os.pv->type==VT_REFPROP||os.pv->type==VT_REFELT) {
							sz=2+length(os.pv->ref.pin->getPID())+length(os.pv->ref.pid);
							if (os.pv->type==VT_REFELT) sz+=1+mv_len32(os.pv->ref.eid);
						} else {
							sz=2+length(os.pv->refId->id)+length(os.pv->refId->pid);
							if (os.pv->type==VT_REFIDELT) sz+=1+mv_len32(os.pv->refId->eid);
						}
						os.state++; VAR_OUT(os.tag,mv_enc16,sz);
					case 1:
						os.state++;
						push_state(ST_PID,os.pv->type==VT_REFPROP||os.pv->type==VT_REFELT?&os.pv->ref.pin->getPID():&os.pv->refId->id,REF_PID_TAG);
						continue;
					case 2:
						os.state++; sz=os.pv->type==VT_REFPROP||os.pv->type==VT_REFELT?os.pv->ref.pid:os.pv->refId->pid;
						VAR_OUT(REF_PROP_TAG,mv_enc32,sz);
					case 3:
						if (os.pv->type==VT_REFELT) sz=os.pv->ref.eid; 
						else if (os.pv->type==VT_REFIDELT) sz=os.pv->refId->eid;
						else break;
						os.state++; VAR_OUT(REF_EID_TAG,mv_enc32,sz);
					case 4:
						break;
					}
					break;
				case ST_RESULT:
					assert(sidx==0 && os.res!=NULL);
					switch (os.state) {
					default: goto error;
					case 0:
						sz=os.res->rc==RC_OK?1+mv_len64(os.res->cnt):1+mv_len32(os.res->rc);
						if (fCid) sz+=1+mv_len64(cid);
						if (os.res->op!=MODOP_QUERY) sz+=1+mv_len32(os.res->op);
						os.state++; VAR_OUT(RESULT_TAG,mv_enc32,sz);
					case 1:
						os.state++; if (fCid) VAR_OUT(RES_CID_TAG,mv_enc64,cid);
					case 2:
						os.state++;
						if (os.res->rc==RC_OK) {VAR_OUT(RES_COUNT_TAG,mv_enc64,os.res->cnt);}
						else {VAR_OUT(RES_ERR_TAG,mv_enc32,os.res->rc);}
					case 3:
						os.state++; if (os.res->op!=MODOP_QUERY) VAR_OUT(RES_OP_TAG,mv_enc32,os.res->op);
					case 4:
						lbuf-=po-buf; return RC_TRUE;
					}
				case ST_COMPOUND:
					assert(sidx==0);
					//????
					lbuf-=po-buf; return RC_TRUE;
				case ST_STATUS:
					assert(sidx==0);
					//????
					lbuf-=po-buf; return RC_TRUE;
				case ST_STREDIT: case ST_STMT: case ST_UID: case ST_RESPAGES: goto error;
				}
				assert(sidx>0); os=stateStack[--sidx];
			}
error:
			// report
			return RC_INTERNAL;
		} catch (RC rc) {return rc;} catch(...) {/*...*/return RC_INTERNAL;}
	}
	void set(const PIN *p,uint64_t ci=0,bool fC=false,RTTYPE r=RTT_PINS) {assert(sidx==0); os.type=ST_PIN; os.pin=p; os.state=0; cid=ci; fCid=fC; rtt=r;}
	void set(const Result *r,uint64_t ci=0,bool fC=false) {assert(sidx==0); os.type=ST_RESULT; os.res=r; os.state=0; cid=ci; fCid=fC;}
	void set(const Value *pv,uint64_t ci=0,bool fC=false) {assert(sidx==0); os.type=ST_VALUE; os.pv=pv; os.state=0; cid=ci; fCid=fC;}
	// compound, status
	void operator delete(void *p) {if (p!=NULL) ((EncodePB*)p)->ses->free(p);}
};

class ProtoBufStreamOut : public IStreamOut 
{
	Session		*const	ses;
	EncodePB			enc;
	Cursor				*res;
	const		PIN		*pin;
	Result				result;
	bool				fRes;
public:
	ProtoBufStreamOut(Session *s,Cursor *pr=NULL,ulong md=0) : ses(s),enc(s,md),res(pr),pin(NULL),fRes(false) 
		{result.rc=RC_OK; result.cnt=0; result.op=MODOP_QUERY; if (pr!=NULL) pr->setNoRel();}
	~ProtoBufStreamOut() {if (res!=NULL) res->destroy(); if (pin!=NULL) ((PIN*)pin)->destroy();}
	RC next(unsigned char *buf,size_t& lbuf) {
		if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		size_t left=lbuf; RC rc;
		while (res!=NULL) {
			if ((rc=enc.encode(buf+lbuf-left,left))!=RC_TRUE) {res->release(); return rc;}
			if (pin!=NULL) ((PIN*)pin)->destroy();
			if ((pin=(PIN*)res->next())==NULL) {res->destroy(); res=NULL; break;}
			enc.set(pin); result.cnt++;
		}
		if (!fRes) {enc.set(&result); fRes=true;} //result.rc=???
		rc=enc.encode(buf+lbuf-left,left); lbuf-=left;
		return rc!=RC_TRUE?rc:lbuf!=0?RC_OK:RC_EOF;
	}
	void destroy() {try {this->~ProtoBufStreamOut(); ses->free(this);} catch (...) {}}
};

enum IN_STATE {ST_TAG, ST_LEN, ST_READ};

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

struct OInfo
{
	uint64_t	cid;
	bool		fCid;
	RTTYPE		rtt;
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
	bool		fAbort;
	StmtIn() : stmt(NULL),str(NULL),lstr(0),uids(NULL),nUids(0),params(NULL),nParams(0),limit(0),offset(0),mode(0),fAbort(false) {}
};

class ProtoBufStreamIn : public IStreamIn {
protected:
	Session	*const	ses;
	const	size_t	lobuf;
	SubAlloc		*ma;
	byte			*obuf;
	EncodePB		*enc;
	size_t			obleft;

	IN_STATE		inState;
	struct	IState {
		SType		type;
		uint8_t		op;
		OInfo		oi;
		uint32_t	tag;
		uint32_t	idx;
		uint32_t	fieldMask;
		uint64_t	msgEnd;
		union {
			void	*obj;
			PIN		*pin;
			Value	*pv;
			PID		*id;
			StmtIn	*stmt;
		};
	}	is,stateStack[STACK_DEPTH];
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

	PIN				**pins;
	OInfo			*pinoi;
	uint32_t		nPins;
	uint32_t		xPins;
	uint32_t		limit;
	byte			*mapBuf;
	size_t			lMapBuf;
	uint64_t		lSave;
	IdentityID		owner;
	uint16_t		storeID;
	SubAlloc		mapAlloc;
	URIIDMap		*uriMap;
	IdentityIDMap	*identMap;
private:
	__forceinline void advance() {inState=ST_TAG; val=0; vsht=0;}
	__forceinline void set_skip() {if (inState==ST_LEN) {sbuf=NULL; left=lField;} else advance();}
	__forceinline void push_state(SType ot,void *obj,size_t sht) {
		assert(sidx<STACK_DEPTH); stateStack[sidx++]=is; is.type=ot; is.op=OP_SET; is.oi.fCid=false; is.oi.rtt=RTT_PINS;
		is.tag=~0u; is.idx=0; is.msgEnd=offset+sht+lField; is.fieldMask=0; is.obj=obj; advance();
	}
	__forceinline Value *initV(Value *v) const {v->setError(); return v;}
	uint32_t map(uint32_t id,bool fProp=true) {
		if (fProp && id>PROP_SPEC_LAST) {
			const URIIDMapElt *uid=uriMap->find(id); if (uid!=NULL) id=uid->mapped;
		} else if (!fProp && id!=STORE_OWNER) {
			const IdentityIDMapElt *iid=identMap->find(id); if (iid!=NULL) id=iid->mapped;
		}
		return id;
	}
	RC addToMap(bool fProp) {
		if (fProp && is.idx>PROP_SPEC_LAST) {
			URIIDMap::Find find(*uriMap,is.idx);
			if (find.find()==NULL) {
				URIIDMapElt *u=new(&mapAlloc) URIIDMapElt; if (u==NULL) return RC_NORESOURCES;
				URI *uri=ses->getStore()->uriMgr->insert((char*)mapBuf); if (uri==NULL) return RC_NORESOURCES;
				u->id=is.idx; u->mapped=uri->getID(); uri->release();
				uriMap->insert(u,find.getIdx());
			}
		} else if (!fProp && is.idx!=STORE_OWNER) {
			IdentityIDMap::Find find(*identMap,is.idx);
			if (find.find()==NULL) {
				IdentityIDMapElt *ii=new(&mapAlloc) IdentityIDMapElt; if (ii==NULL) return RC_NORESOURCES;
				Identity *ident=(Identity*)ses->getStore()->identMgr->find((char*)mapBuf);
				if (ident==NULL) {
					//???
				} else {
					ii->id=is.idx; ii->mapped=ident->getID(); ident->release(); identMap->insert(ii,find.getIdx());
				}
			}
		}
		return RC_OK;
	}
	virtual RC flush() = 0;
	virtual	RC commitPINs() = 0;
	virtual	RC processPIN() = 0;
	virtual	RC process(Stmt *stmt) = 0;
	virtual	RC processTx(uint32_t code) = 0;
	virtual	RC allocMem() = 0;
	virtual void releaseMem() = 0;
protected:
	ProtoBufStreamIn(Session *s,SubAlloc *m,size_t lo=0) : ses(s),lobuf(lo==0?DEFAULT_OBUF_SIZE:lo),ma(m),obuf(NULL),enc(NULL),obleft(0),
		inState(ST_TAG),sidx(0),lField(0),val(0),vsht(0),offset(0),left(0),sbuf(NULL),defType(VT_ANY),pins(NULL),pinoi(NULL),nPins(0),xPins(0),limit(DEFAULT_PIN_BATCH_SIZE),
		mapBuf(NULL),lMapBuf(0),lSave(0),owner(STORE_OWNER),storeID(0),mapAlloc(s) {
		is.type=ST_MVSTREAM; is.op=MODOP_INSERT; is.oi.cid=0; is.oi.fCid=false; is.oi.rtt=RTT_PINS; is.tag=~0u; is.idx=0; is.msgEnd=~0ULL; is.fieldMask=0; is.obj=NULL;
		uriMap=new(&mapAlloc) URIIDMap(64,&mapAlloc,false); identMap=new(&mapAlloc) IdentityIDMap(32,&mapAlloc,false);
	}
	virtual ~ProtoBufStreamIn() {
		if (mapBuf!=NULL) ses->free(mapBuf); if (pins!=NULL) {ses->free(pins); ses->free(pinoi);}
	}
	RC next(const unsigned char *in,size_t lbuf) {
		try {
			if (in==NULL||lbuf==0) return sidx==0?flush():RC_CORRUPTED;
			if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
			const byte *const buf0=in,*const end=in+lbuf; RC rc; Stmt *stmt=NULL;
#if 0
			const byte *const yp=(end[-1]&0x80)==0?end:end-SAFETY_MARGIN32;
#endif
			for (;;) {
				if (left!=0) {
					size_t l=end-in;
					if (l<left) {
						if (l!=0) {left-=l; if (sbuf!=NULL) {memcpy(sbuf,in,l); sbuf+=l;}}
						offset+=lbuf; return RC_OK;
					}
					if (sbuf!=NULL) memcpy(sbuf,in,(size_t)left);
					in+=left; left=0;
					if (sbuf==NULL || inState==ST_LEN) goto check_end;
				} else {
					// if (in<yp) {mv_dec64(in,val);} else	// don't reset val!!!
					do {
						if (in>=end) {offset+=lbuf; return RC_OK;}
						val|=uint64_t(*in&0x7F)<<vsht; vsht+=7; 
					} while ((*in++&0x80)!=0);
					switch (inState) {
					case ST_TAG:
						is.tag=(uint32_t)val;
						switch (is.tag&7) {
						default: return RC_CORRUPTED;
						case 0: inState=ST_READ; val=0; vsht=0; continue;
						case 2: inState=ST_LEN; val=0; vsht=0; continue;
						case 1: left=8; break;
						case 5: left=4; break;
						}
						if (offset+(in-buf0)+left>is.msgEnd) return RC_CORRUPTED;
						sbuf=(byte*)&buf; inState=ST_READ; continue;
					case ST_LEN:
						assert((is.tag&7)==2);
						lField=val; if (offset+(in-buf0)+lField>is.msgEnd) return RC_CORRUPTED;
						break;
					case ST_READ:
						assert((is.tag&7)==0);
						if (offset+(in-buf0)>is.msgEnd) return RC_CORRUPTED;
						break;
					}
				}
				assert(inState==ST_LEN||inState==ST_READ);
				{const uint32_t fieldN=(is.tag>>3);
				if (fieldN<1 || fieldN>32) {set_skip(); continue;}
				const uint32_t fieldBit=1<<(fieldN-1); PIN *pin; StmtIn *qin; RefVID *rv;
				if ((is.fieldMask&fieldBit)!=0 && (repeatedFields[is.type]&fieldBit)==0) return RC_CORRUPTED;
				is.fieldMask|=fieldBit;
				switch (is.type) {
				default: assert(0);
				case ST_MVSTREAM:
					assert(sidx==0);
					switch (is.tag) {
					default: set_skip(); continue;
					case OWNER_TAG: case STRPROP_TAG: case STRIDENT_TAG:
						push_state(ST_STRMAP,NULL,in-buf0); continue;
					case STOREID_TAG:
						storeID=(uint16_t)val; advance(); continue;
					case PIN_TAG:
						if ((rc=allocMem())!=RC_OK) return rc;
						if ((pin=new(ma) PIN(ses,PIN::defPID,PageAddr::invAddr,PIN_NO_FREE))==NULL) {releaseMem(); return RC_NORESOURCES;}
						push_state(ST_PIN,pin,in-buf0); continue;
					case STMT_TAG:
						if ((rc=allocMem())!=RC_OK) return rc;
						if ((qin=new(ma) StmtIn())==NULL) {releaseMem(); return RC_NORESOURCES;}
						push_state(ST_STMT,qin,in-buf0); continue;
					case TXOP_TAG:
						if ((rc=processTx((uint32_t)val))!=RC_OK) return rc;
						advance(); continue;
					case FLUSH_TAG:
						if ((rc=flush())!=RC_OK) return rc;
						advance(); continue;
					}
				case ST_STRMAP:
					switch (is.tag) {
					default: set_skip(); continue;
					case MAP_STR_TAG:
						if (lField==0) return RC_CORRUPTED;
						if (lMapBuf<(size_t)lField+1 && (mapBuf=(byte*)ses->realloc(mapBuf,lMapBuf=(size_t)lField+1))==NULL) return RC_NORESOURCES;
						sbuf=mapBuf; mapBuf[(size_t)lField]=0; left=lSave=lField; continue;
					case MAP_ID_TAG: 
						is.idx=(uint32_t)val; break;
					}
					break;
				case ST_PIN:
					assert(is.pin!=NULL);
					switch (is.tag) {
					default: set_skip(); continue;
					case OP_TAG: is.op=(uint8_t)val; break;
					case ID_TAG:
						const_cast<PID&>(is.pin->id)=PIN::defPID;
						push_state(ST_PID,const_cast<PID*>(&is.pin->id),in-buf0); continue;
					case MODE_TAG: is.pin->mode=(uint32_t)val; break;
					case STAMP_TAG: is.pin->stamp=(uint32_t)val; break;
					case NVALUES_TAG:
						if (is.idx>(uint32_t)val) return RC_CORRUPTED;
						if ((is.pin->nProperties=(uint32_t)val)!=0) {
							if ((is.pin->properties=(Value*)ma->realloc((void*)is.pin->properties,is.pin->nProperties*sizeof(Value)))==NULL) return RC_NORESOURCES;
							if (is.idx<is.pin->nProperties) memset((void*)(is.pin->properties+is.idx),0,(is.pin->nProperties-is.idx)*sizeof(Value));
						}
						break;
					case VALUES_TAG:
						if ((is.fieldMask&1<<((NVALUES_TAG>>3)-1))==0) {
							if ((is.pin->properties=(Value*)ma->realloc((void*)is.pin->properties,++is.pin->nProperties*sizeof(Value)))==NULL) return RC_NORESOURCES;
						} else if (is.idx>=is.pin->nProperties) return RC_CORRUPTED;
						defType=VT_ANY; push_state(ST_VALUE,initV((Value*)&is.pin->properties[is.idx++]),in-buf0); continue;
					case CID_TAG: is.oi.cid=val; is.oi.fCid=true; break;
					case RTT_TAG: is.oi.rtt=(RTTYPE)val; break;		// check valid
					}
					break;
				case ST_VALUE:
					assert(sidx>0 && is.pv!=NULL);
					if (is.tag>PROPERTY_TAG	&& is.tag<VALUE_OP_TAG && defType!=VT_ANY) return RC_CORRUPTED;
					switch (is.tag) {
					default: set_skip(); continue;
					case TYPE_TAG: is.pv->type=(uint8_t)val; break;		// check range
					case PROPERTY_TAG: is.pv->property=map((uint32_t)val); break;
					case VALUE_OP_TAG: is.pv->op=(uint8_t)val; break;	// check range
					case EID_TAG: is.pv->eid=(ElementID)val; break;
					case META_TAG: is.pv->meta=(uint8_t)val; break;
					case UNITS_TAG: is.pv->qval.units=(uint16_t)val; break;
					case _L(3):
						defType=VT_STRING;
						if ((sbuf=(byte*)ma->malloc((size_t)lField+1))==NULL) return RC_NORESOURCES;
						sbuf[(size_t)lField]=0; is.pv->str=(char*)sbuf; is.pv->length=(uint32_t)lField;
						if ((left=lField)!=0) continue; else break;
					case _L(4):
						defType=VT_BSTR;
						if ((sbuf=(byte*)ma->malloc((size_t)lField))==NULL) return RC_NORESOURCES;
						is.pv->bstr=sbuf; is.pv->length=(uint32_t)lField;
						if ((left=lField)!=0) continue; else break;
					case _V(5): is.pv->i=mv_dec32zz((uint32_t)val); is.pv->length=sizeof(int32_t); defType=VT_INT; break;
					case _V(6): is.pv->ui=(uint32_t)val; is.pv->length=sizeof(uint32_t); defType=VT_UINT; break;
					case _V(7): is.pv->i64=mv_dec64zz(val); is.pv->length=sizeof(int64_t); defType=VT_INT64; break;
					case _V(8): is.pv->ui64=val; is.pv->length=sizeof(uint64_t); defType=VT_UINT64; break;
					case _S(9): is.pv->f=buf.f; is.pv->length=sizeof(float); defType=VT_FLOAT; break;		// byte order???
					case _D(10): is.pv->d=buf.d; is.pv->length=sizeof(double); defType=VT_DOUBLE; break;	// byte order???
					case _D(11): is.pv->ui64=buf.u64; is.pv->length=sizeof(uint64_t); defType=VT_DATETIME; break;	// byte order???
					case _D(12): is.pv->i64=buf.i64; is.pv->length=sizeof(int64_t); defType=VT_INTERVAL; break;	// byte order??? sfixed???
					case _L(13): is.pv->length=1; is.pv->id=PIN::defPID; push_state(ST_PID,&is.pv->id,in-buf0); defType=VT_REFID; continue;
					case _L(14): push_state(ST_VARRAY,is.pv,in-buf0); defType=VT_ARRAY; break;
					case _L(15):
						if ((is.pv->refId=rv=new(ma) RefVID)==NULL) return RC_NORESOURCES;
						rv->id=PIN::defPID; rv->pid=STORE_INVALID_PROPID; rv->eid=STORE_COLLECTION_ID; rv->vid=STORE_CURRENT_VERSION;
						is.pv->length=1; push_state(ST_REF,is.pv,in-buf0); defType=VT_REFIDPROP; continue;		// VT_REFIDELT?
					case _V(16): is.pv->b=val!=0; is.pv->length=1; defType=VT_BOOL; break;
					case STREDIT_TAG:
						//...
						break;
					}
					break;
				case ST_VARRAY:
					switch (is.tag) {
					default: set_skip(); continue;
					case VARRAY_L_TAG:
						if ((is.pv->length=(uint32_t)val)<is.idx) return RC_CORRUPTED;
						if (is.pv->length>is.idx) {
							if ((is.pv->varray=(Value*)ma->realloc((void*)is.pv->varray,is.pv->length*sizeof(Value)))==NULL) return RC_NORESOURCES;
							memset((void*)(is.pv->varray+is.idx),0,(is.pv->length-is.idx)*sizeof(Value));
						}
						break;
					case VARRAY_V_TAG:
						if ((is.fieldMask&1<<((VARRAY_L_TAG>>3)-1))==0) {
							// add 1 elt, set Value to 0
						} else if (is.idx>=is.pv->length) return RC_CORRUPTED;
						defType=VT_ANY; push_state(ST_VALUE,initV((Value*)&is.pv->varray[is.idx++]),in-buf0); continue;
					}
					break;
				case ST_PID:
					switch (is.tag) {
					default: set_skip(); continue;
					case PID_ID_TAG: is.id->pid=val; break;
					case PID_IDENT_TAG: is.id->ident=map((uint32_t)val,false); break;
					}
					break;
				case ST_REF:
					switch (is.tag) {
					default: set_skip(); continue;
					case REF_PID_TAG:
						*(PID*)&is.pv->refId->id=PIN::defPID;
						push_state(ST_PID,(PID*)&is.pv->refId->id,in-buf0); continue;
					case REF_PROP_TAG: ((RefVID*)is.pv->refId)->pid=map((uint32_t)val); break;
					case REF_EID_TAG: ((RefVID*)is.pv->refId)->eid=(ElementID)val; break;
					case REF_VID_TAG: ((RefVID*)is.pv->refId)->vid=(VersionID)val; break;
					}
					break;
				case ST_STMT:
					assert(is.stmt!=NULL);
					switch (is.tag) {
					default: set_skip(); continue;
					case STMT_STR_TAG:
						if ((sbuf=(byte*)ma->malloc((size_t)lField+1))==NULL) return RC_NORESOURCES;
						sbuf[(size_t)lField]=0; is.stmt->str=(char*)sbuf; is.stmt->lstr=(size_t)lField;
						if ((left=lField)!=0) continue; else break;
					case STMT_UID_TAG:
					case STMT_PARAM_TAG:
						//???
						break;
					case STMT_CID_TAG: is.oi.cid=val; is.oi.fCid=true; break;
					case STMT_RTT_TAG: is.oi.rtt=(RTTYPE)val; break;					// check valid
					case STMT_LIMIT_TAG: is.stmt->limit=(uint32_t)val; break;
					case STMT_OFFSET_TAG: is.stmt->offset=(uint32_t)val; break;
					case STMT_MODE_TAG: is.stmt->mode=(uint32_t)val; break;
					case STMT_ABORT_TAG: is.stmt->fAbort=val!=0; break;
					}
					break;
				case ST_RESPAGES:
					//...
					break;
				case ST_UID:
					//...
				case ST_STREDIT:
					return RC_INTERNAL;
				}
				}
		check_end:
				while (offset+(in-buf0)>=is.msgEnd) {
					if ((is.fieldMask&requiredFields[is.type])!=requiredFields[is.type]) return RC_CORRUPTED;	// missing required fields
					switch (is.type) {
					default: break;
					case ST_STRMAP:
						if (stateStack[sidx-1].tag==OWNER_TAG) {
							//???
						} else if ((rc=addToMap(stateStack[sidx-1].tag==STRPROP_TAG))!=RC_OK) return rc;
						break;
					case ST_PIN:
						if (is.idx!=is.pin->nProperties) {if (sidx==1) releaseMem(); return RC_CORRUPTED;}
						if (sidx!=1) {/*???*/}
						else {
							if (pins!=NULL && nPins!=0) {
								if (is.op!=MODOP_INSERT || nPins>=xPins && xPins>=limit) {if ((rc=commitPINs())!=RC_OK) return rc; nPins=0;}
								else if (nPins>=xPins && ((pins=(PIN**)ses->realloc(pins,(xPins*=2)*sizeof(PIN*)))==NULL
									|| (pinoi=(OInfo*)ses->realloc(pinoi,xPins*sizeof(OInfo)))==NULL)) return RC_NORESOURCES;
							}
							if (is.op==MODOP_INSERT) {
								if (is.pin->properties!=NULL) {
									is.pin->properties=PIN::normalize(is.pin->properties,is.pin->nProperties,PIN_NO_FREE,ses->getStore()->getPrefix(),ma);
									if (is.pin->properties==NULL) return RC_NORESOURCES;	//???
								}
								if (pins==NULL && ((pins=(PIN**)ses->malloc((xPins=1024)*sizeof(PIN*)))==NULL
									|| (pinoi=(OInfo*)ses->malloc(xPins*sizeof(OInfo)))==NULL)) return RC_NORESOURCES;
								assert(nPins<xPins); pins[nPins]=is.pin; pinoi[nPins]=is.oi; nPins++;
							} else if ((rc=processPIN())!=RC_OK) return rc;
						}
						break;
					case ST_VALUE:
						if (is.pv->type==VT_ANY) is.pv->type=defType;
						switch (is.pv->type) {
						case VT_URIID: is.pv->uid=map(is.pv->uid); break;
						case VT_IDENTITY: is.pv->iid=map(is.pv->iid,false); break;
						case VT_EXPR:
						case VT_STMT:
							// check string, parse
							break;
						case VT_REFCID:
							//...check inside compound
							// end of compound -> resolve references
							break;
						}
						break;
					case ST_VARRAY:
						if (is.idx!=is.pv->length) return RC_CORRUPTED;
						break;
					case ST_STMT:
						try {SInCtx qctx(ses,is.stmt->str,is.stmt->lstr,is.stmt->uids,is.stmt->nUids,SQ_SQL,ma); stmt=NULL; stmt=qctx.parse();}
						catch (SynErr) {if (stmt!=NULL) stmt->destroy(); if (sidx==1) releaseMem(); return RC_SYNTAX;}
						catch (RC rc) {if (stmt!=NULL) stmt->destroy(); if (sidx==1) releaseMem(); return rc;}
						// mark and release memeory (save in StmtIn)
						if (sidx!=1) {/*???*/}
						else if ((rc=process(stmt))!=RC_OK) return rc;
						break;
					}
					is=stateStack[--sidx]; 
				}
				advance();
			}
		} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStreamIn::next()\n"); return RC_INTERNAL;}
	}
};

class SyncStreamIn : public SubAlloc, public ProtoBufStreamIn {
	IStreamIn	*const	out;
	SubAlloc::SubMark	start;
	unsigned			txLevel;
private:
	RC resultOut(const Result& res) {
		assert(out!=NULL && obuf!=NULL && enc!=NULL);
		RC rc=RC_OK; enc->set(&res,is.oi.cid,is.oi.fCid);
		while (rc==RC_OK && (rc=enc->encode(obuf+lobuf-obleft,obleft))==RC_OK) {if ((rc=out->next(obuf,lobuf))==RC_TRUE) rc=RC_OK; obleft=lobuf;}
		return rc==RC_TRUE?RC_OK:rc;
	}
	RC pinOut(const PIN *pin,const OInfo& oi) {
		RC rc=RC_OK;
		if (oi.rtt!=RTT_COUNT) {
			enc->set(pin,oi.cid,oi.fCid,oi.rtt);
			while (rc==RC_OK && (rc=enc->encode(obuf+lobuf-obleft,obleft))==RC_OK) {if ((rc=out->next(obuf,lobuf))==RC_TRUE) rc=RC_OK; obleft=lobuf;}
			if (rc==RC_TRUE) rc=RC_OK;
		}
		return rc;
	}
	RC flush() {
		RC rc=RC_OK; if (pins!=NULL && nPins!=0) rc=commitPINs();
		if (rc==RC_OK && out!=NULL && obleft!=lobuf) {rc=out->next(obuf,lobuf-obleft); obleft=lobuf;}
		return rc;
	}
	RC commitPINs() {
		assert(pins!=NULL && nPins!=0);
		RC rc=ses->getStore()->queryMgr->commitPINs(ses,pins,nPins,0);		// mode? allocCtrl? params? (pass in stream, special message)
		if (out!=NULL) {
			if (rc!=RC_OK) {Result res={rc,nPins,MODOP_INSERT}; rc=resultOut(res);}
			else for (uint32_t i=0; i<nPins; i++) if ((rc=pinOut(pins[i],pinoi[i]))!=RC_OK) break;
		}
		ma->truncate(start); nPins=0; return rc;
	}
	RC processPIN() {
		RC rc=RC_OK; assert(is.op!=MODOP_INSERT);
		if (is.pin->id.pid==STORE_INVALID_PID || is.pin->id.ident==STORE_INVALID_IDENTITY) rc=RC_INVPARAM;
		else {PINEx pex(ses,is.pin->id); rc=ses->getStore()->queryMgr->apply(ses,stmtOp[is.op],pex,is.pin->properties,is.pin->nProperties,0);}
		if (out!=NULL) {
			RC rc2=rc==RC_OK?pinOut(is.pin,is.oi):RC_OK;
			Result res={rc,1,(MODOP)is.op}; rc=resultOut(res);
			if (rc2!=RC_OK) rc=rc2;
		}
		ma->truncate(start);
		return rc;
	}
	RC process(Stmt *stmt) {
		RC rc=RC_OK; ICursor *cursor=NULL; Result res={RC_OK,0,modOp[stmt->getOp()]};
		if ((rc=stmt->execute(out!=NULL&&is.oi.rtt!=RTT_COUNT?&cursor:(ICursor**)0,is.stmt->params,is.stmt->nParams,is.stmt->limit,is.stmt->offset,is.stmt->mode,&res.cnt))==RC_OK && cursor!=NULL) {
			PIN pin(ses,PIN::defPID,PageAddr::invAddr),*pp=&pin; PID pid; IPIN *ipin; ((Cursor*)cursor)->setNoRel(); assert(obuf!=NULL && enc!=NULL);
			for (unsigned nPins; ;res.cnt++) {
				if (is.oi.rtt==RTT_PINS) {
					if ((rc=cursor->next(&ipin,1,nPins))==RC_OK) pp=(PIN*)ipin; else break;
				} else {
					if ((rc=cursor->next(pid))==RC_OK) const_cast<PID&>(pin.id)=pid; else break;
				}
				if ((rc=pinOut(pp,is.oi))!=RC_OK) break;
			}
			cursor->destroy(); if (rc==RC_EOF) rc=RC_OK;
		}
		if (out!=NULL) {res.rc=rc; rc=resultOut(res);}
		ma->truncate(start);
		return rc;
	}
	RC processTx(uint32_t code) {
		RC rc;
		switch (code) {
		case TXOP_COMMIT:
			if (txLevel!=0) {if ((rc=ses->getStore()->txMgr->commitTx(ses,false))==RC_OK) txLevel--; else return rc;}
			break;
		case TXOP_COMMIT_ALL:
			if (txLevel!=0) {if ((rc=ses->getStore()->txMgr->commitTx(ses,true))==RC_OK) txLevel=0; else return rc;}
			break;
		case TXOP_START:
		case TXOP_START_READONLY:
		case TXOP_START_MODIFYCLASS:
			if ((rc=ses->getStore()->txMgr->startTx(ses,code==TXOP_START?TXT_READWRITE:code==TXOP_START_READONLY?TXT_READONLY:TXT_MODIFYCLASS,TXI_DEFAULT))!=RC_OK) return rc;
			txLevel++; break;
		case TXOP_ROLLBACK:
		case TXOP_ROLLBACK_ALL:
			//???
			break;
		}
		return RC_OK;
	}
	RC allocMem() {return RC_OK;}
	void releaseMem() {}
public:
	SyncStreamIn(Session *s,IStreamIn *o=NULL,size_t lo=0) : SubAlloc(s),ProtoBufStreamIn(s,this,lo),out(o),txLevel(0) {
		if (o!=NULL && ((obuf=(byte*)malloc(lobuf))==NULL || (enc=new(this) EncodePB(s,0))==NULL)) throw RC_NORESOURCES;
		obleft=lobuf; ma->mark(start);
	}
	void operator delete(void *p) {if (p!=NULL) ((SyncStreamIn*)p)->ses->free(p);}
	void destroy() {try {delete this;} catch (...) {}}
};

class ServerStreamIn : public ProtoBufStreamIn {
	enum StreamReqType {SRT_INSERT, SRT_PIN, SRT_STMT, SRT_TX};
	class StreamRequest : public Request {
		ServerStreamIn&			str;
		const	IdentityID		ident;
		const	StreamReqType	type;
		SubAlloc		*const	mem;
		OInfo					oi;
		union {
			struct {PIN *pin; uint8_t op;};
			struct {PIN **pins; OInfo *pinoi; unsigned nPins;};
			struct {Stmt *stmt; StmtIn *info;};
			//txop
		};
	public:
		StreamRequest(ServerStreamIn& si,PIN **pp,OInfo *poi,unsigned nP) : str(si),ident(si.ses->getIdentity()),type(SRT_STMT),mem(si.ma),oi(si.is.oi)
				{pins=pp; pinoi=poi; nPins=nP;}
		StreamRequest(ServerStreamIn& si,PIN *p,uint8_t o) : str(si),ident(si.ses->getIdentity()),type(SRT_STMT),mem(si.ma),oi(si.is.oi) {pin=p; op=o;}
		StreamRequest(ServerStreamIn& si,Stmt *st,StmtIn *in) : str(si),ident(si.ses->getIdentity()),type(SRT_STMT),mem(si.ma),oi(si.is.oi) {stmt=st; info=in;}
		void process() {
			Session *ses=Session::getSession();
			try {
				if (ident!=STORE_OWNER && ses!=NULL) ses->setIdentity(ident,true);	// fMayInsert???
				RC rc=RC_OK; ICursor *cursor=NULL; uint64_t nProcessed=0;
				switch (type) {
				default: break;
				case SRT_INSERT:
					rc=ses->getStore()->queryMgr->commitPINs(ses,pins,nPins,0);		// mode? allocCtrl? (pass in stream, special message)
					if (str.cb!=NULL) {
						if (rc!=RC_OK) {Result res={rc,nPins,MODOP_INSERT}; rc=resultOut(res);}
						else for (uint32_t i=0; i<nPins; i++) if ((rc=pinOut(pins[i],pinoi[i]))!=RC_OK) break;
					}
					break;
				case SRT_PIN:
					{PINEx pex(ses,pin->id); rc=ses->getStore()->queryMgr->apply(ses,stmtOp[op],pex,pin->properties,pin->nProperties,0);}
					if (str.cb!=NULL) {if (rc==RC_OK) pinOut(pin,oi); Result res={rc,1,(MODOP)op}; resultOut(res);}
					break;
				case SRT_STMT:
					if ((rc=stmt->execute(str.cb!=NULL&&oi.rtt!=RTT_COUNT?&cursor:(ICursor**)0,info->params,info->nParams,info->limit,info->offset,info->mode,&nProcessed))==RC_OK && cursor!=NULL) {
						PIN pin(ses,PIN::defPID,PageAddr::invAddr),*pp=&pin; ((Cursor*)cursor)->setNoRel(); PID pid; IPIN *ipin;
						for (unsigned nPins; ;nProcessed++) {
							if (oi.rtt==RTT_PINS) {
								if ((rc=cursor->next(&ipin,1,nPins))==RC_OK) pp=(PIN*)ipin; else break;
							} else {
								if ((rc=cursor->next(pid))==RC_OK) const_cast<PID&>(pin.id)=pid; else break;
							}
							if ((rc=pinOut(pp,oi))!=RC_OK) break;
						}
						cursor->destroy(); if (rc==RC_EOF) rc=RC_OK;
					}
					if (str.cb!=NULL) {Result res={rc,nProcessed,modOp[stmt->getOp()]}; rc=resultOut(res);}
					break;
				}
			} catch (RC) {
				// report
			} catch (...) {
				// report
			}
			if (ident!=STORE_OWNER && ses!=NULL) ses->setIdentity(ident,true);
			--str.active;
		}
		RC resultOut(const Result& res) {
			assert(str.cb!=NULL && str.obuf!=NULL && str.enc!=NULL);
			RC rc=RC_OK; MutexP lck(&str.lock); str.enc->set(&res,oi.cid,oi.fCid);
			while (rc==RC_OK && (rc=str.enc->encode(str.obuf+str.lobuf-str.obleft,str.obleft))==RC_OK) 
				{if ((rc=str.cb->send(str.obuf,str.lobuf))==RC_TRUE) rc=RC_OK; str.obleft=str.lobuf;}
			return rc==RC_TRUE?RC_OK:rc;
		}
		RC pinOut(const PIN *pin,const OInfo& oi) {
			RC rc=RC_OK;
			if (oi.rtt!=RTT_COUNT) {
				MutexP lck(&str.lock); str.enc->set(pin,oi.cid,oi.fCid,oi.rtt);
				while (rc==RC_OK && (rc=str.enc->encode(str.obuf+str.lobuf-str.obleft,str.obleft))==RC_OK) 
					{if ((rc=str.cb->send(str.obuf,str.lobuf))==RC_TRUE) rc=RC_OK; str.obleft=str.lobuf;}
				if (rc==RC_TRUE) rc=RC_OK;
			}
			return rc;
		}
		void destroy() {delete mem;}
	};
	RC flush() {
		RC rc=RC_OK; if (pins!=NULL && nPins!=0) rc=commitPINs();
		//...
		return rc;
	}
	RC commitPINs() {
		PIN **pp=new(ma) PIN*[nPins]; OInfo *oi=new(ma) OInfo[nPins]; RC rc=RC_OK;
		if (pp==NULL||oi==NULL) rc=RC_NORESOURCES;
		else {
			memcpy(pp,pins,sizeof(PIN*)*nPins); memcpy(oi,pinoi,sizeof(OInfo)*nPins);
			StreamRequest *sr=new(ma) StreamRequest(*this,pp,oi,nPins);
			if (sr==NULL) rc=RC_NORESOURCES;
			else if (!RequestQueue::postRequest(sr,ses->getStore())) rc=RC_OTHER; else ++active;
		}
		if (rc!=RC_OK) {delete ma; ma=NULL;}
		nPins=0; return rc;
	}
	RC processPIN() {
		RC rc=RC_OK; assert(is.op!=MODOP_INSERT);
		if (is.pin->id.pid==STORE_INVALID_PID || is.pin->id.ident==STORE_INVALID_IDENTITY) rc=RC_INVPARAM;
		else {
			StreamRequest *sr=new(ma) StreamRequest(*this,is.pin,is.op);
			if (sr==NULL) rc=RC_NORESOURCES;
			else if (!RequestQueue::postRequest(sr,ses->getStore())) rc=RC_OTHER; else ++active;
		}
		if (rc!=RC_OK) {delete ma; ma=NULL;}
		return rc;
	}
	RC process(Stmt *stmt) {
		RC rc=RC_OK; assert(ma!=NULL);
		StreamRequest *sr=new(ma) StreamRequest(*this,stmt,is.stmt);
		if (sr==NULL) rc=RC_NORESOURCES;
		else if (!RequestQueue::postRequest(sr,ses->getStore())) rc=RC_OTHER; else ++active;
		if (rc!=RC_OK) {delete ma; ma=NULL;}
		return rc;
	}
	RC processTx(uint32_t code) {
		// post
		return RC_OK;
	}
	RC allocMem() {return (ma=new(ses->getStore()) SubAlloc(ses->getStore()))!=NULL?RC_OK:RC_NORESOURCES;}
	void releaseMem() {delete ma; ma=NULL;}
	IStreamInCallback	*const	cb;
	Mutex						lock;
	SharedCounter				active;
public:
	ServerStreamIn(Session *s,IStreamInCallback *icb,size_t lo) : ProtoBufStreamIn(s,NULL,lo),cb(icb) {
		if (icb!=NULL) {
			if ((obuf=(byte*)ses->malloc(lobuf))==NULL || (enc=new(ses) EncodePB(s,0))==NULL) throw RC_NORESOURCES;
			obleft=lobuf;
		}
	}
	~ServerStreamIn() {delete ma; delete enc; if (obuf!=NULL) ses->free(obuf);}
	void operator delete(void *p) {if (p!=NULL) ((ServerStreamIn*)p)->ses->free(p);}
	void destroy() {try {delete this;} catch (...) {}}
};

};

RC Session::replicate(const PIN *pin)
{
	if (repl==NULL && (repl=new(this) SubAlloc(this))==NULL) return RC_NORESOURCES;
	EncodePB enc(this); enc.set(pin); size_t lbuf=0; RC rc=RC_OK;
	do {
		byte *buf=(byte*)repl->getBuffer(lbuf); if (buf==NULL || lbuf==0) return RC_NORESOURCES;
		rc=enc.encode(buf,lbuf); repl->setLeft(lbuf);
	} while (rc==RC_OK);
	return rc==RC_TRUE?RC_OK:rc;
}

RC Stmt::execute(IStreamOut*& result,const Value *params,unsigned nParams,unsigned nReturn,unsigned nSkip,unsigned long mode,TXI_LEVEL txi) const
{
	try {
		result=NULL; ICursor *ir; RC rc; uint64_t cnt=0;
		Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION; if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		if ((rc=execute(&ir,params,nParams,nReturn,nSkip,mode,&cnt,txi))!=RC_OK) return rc;
		ProtoBufStreamOut *pb=new(ses) ProtoBufStreamOut(ses,(Cursor*)ir,mode); if (pb==NULL) {ir->destroy(); return RC_NORESOURCES;}
		result=pb; return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::execute(IStream*&,...)\n"); return RC_INTERNAL;}
}
		
RC SessionX::createInputStream(IStreamIn *&in,IStreamIn *out,size_t lo)
{
	try {
		assert(ses==Session::getSession()); if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		return (in=new(ses) SyncStreamIn(ses,out,lo))!=NULL?RC_OK:RC_NORESOURCES;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::createInputStream()\n"); return RC_INTERNAL;}
}

RC createServerInputStream(MVStoreCtx ctx,const StreamInParameters *params,IStreamIn *&in,StreamInType stype)
{
	try {
		if (ctx!=NULL) ctx->set(); else if ((ctx=StoreCtx::get())==NULL) return RC_NOTFOUND;
		if (ctx->inShutdown()) return RC_SHUTDOWN;
		Session *ses=Session::createSession(ctx); if (ses==NULL) return RC_NORESOURCES;
		if (params!=NULL && params->identity!=NULL && params->lIdentity!=0) {
			// login
		}
		return (in=new(ses) ServerStreamIn(ses,params!=NULL?params->cb:(IStreamInCallback*)0,params!=NULL?params->lBuffer:0))!=NULL?RC_OK:RC_NORESOURCES;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in createServerInputStream()\n"); return RC_INTERNAL;}
}
