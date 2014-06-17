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

#include "protobuf.h"
#include "maps.h"
#include "blob.h"
#include "txmgr.h"
#include "queryprc.h"

using namespace AfyKernel;

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
#define	VALUE_TAG		_L(13)
#define	EOS_TAG			_V(14)

#define	MAP_STR_TAG		_L(1)
#define	MAP_ID_TAG		_V(2)

#define OP_TAG			_V(1)
#define	ID_TAG			_L(2)
#define	MODE_TAG		_V(3)
#define	NVALUES_TAG		_V(4)
#define	VALUES_TAG		_L(5)
#define	CID_TAG			_V(6)
#define	RTT_TAG			_V(7)

#define TYPE_TAG		_V(1)
#define	PROPERTY_TAG	_V(2)
#define	VALUE_OP_TAG	_V(17)
#define	EID_TAG			_V(18)
#define	META_TAG		_V(19)
#define	UNITS_TAG		_V(20)

#define	PID_ID_TAG		_V(1)
#define	PID_IDENT_TAG	_V(2)

#define REF_PID_TAG		_L(1)
#define	REF_PROP_TAG	_V(2)
#define	REF_EID_TAG		_V(3)
#define	REF_VID_TAG		_V(4)

#define	VARRAY_L_TAG	_V(1)
#define	VARRAY_V_TAG	_L(2)

#define	VMAP_L_TAG		_V(1)
#define	VMAP_V_TAG		_L(2)

#define	ENU_ENUID_TAG	_V(1)
#define	ENU_ELTID_TAG	_V(2)

#define BEDT_BITS_TAG	_V(1)
#define BEDT_MASK_TAG	_V(2)

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

#define	RESPG_NPGS_TAG	_V(1)
#define	RESPG_PAGE_TAG	_V(2)

const byte EncodePB::tags[VT_ALL] = 
{
	0, _V(5), _V(6), _V(7), _V(8), _S(9), _D(10), _V(16), _D(11), _D(12), _V(6), _V(6), _L(22), _L(3), _L(4), _L(13),
	_L(13), _L(15), _L(15), _L(15), _L(15), _L(4), _L(4), _L(14), _L(14), _L(22), _L(14), _L(23), 0, _V(6), 0, 0
};
const byte EncodePB::types[VT_ALL] = 
{
	VT_ERROR, VT_INT, VT_UINT, VT_INT64, VT_UINT64, VT_FLOAT, VT_DOUBLE, VT_BOOL, VT_DATETIME, VT_INTERVAL, VT_URIID, VT_IDENTITY, VT_ENUM,
	VT_STRING, VT_BSTR, VT_REFID, VT_REFID, VT_REFIDPROP, VT_REFIDPROP, VT_REFIDELT, VT_REFIDELT,
	VT_EXPR, VT_STMT, VT_COLLECTION, VT_STRUCT, VT_MAP, VT_RANGE, VT_ARRAY, VT_ERROR, VT_CURRENT, VT_REF, VT_ERROR,
};

static const STMT_OP stmtOp[] =
{
	STMT_INSERT, STMT_UPDATE, STMT_DELETE, STMT_INSERT, STMT_UNDELETE
};

static const MODOP modOp[STMT_OP_ALL] =
{
	MODOP_QUERY, MODOP_INSERT, MODOP_UPDATE, MODOP_DELETE, MODOP_UNDELETE, MODOP_QUERY, MODOP_QUERY, MODOP_QUERY
};

#define	VAR_OUT(a,b,c)	{if (po<ypo) {afy_enc16(po,a); b(po,c); assert(po<end);}											\
						else {byte *p=rbuf; afy_enc16(p,a); b(p,c); lRes=(size_t)(p-rbuf); size_t left=(size_t)(end-po);	\
						if (lRes<left) {memcpy(po,rbuf,lRes); po+=lRes; lRes=0;} else {memcpy(po,rbuf,left); lRes-=left; pRes=rbuf+left; return RC_OK;}}}
#define	FLO_OUT(a,b)	{if (po<ypo) {afy_enc16(po,a); memcpy(po,&b,4); po+=4; assert(po<end);}	\
						else {byte *p=rbuf; afy_enc16(p,a); memcpy(p,&b,4); p+=4;					\
						lRes=(size_t)(p-rbuf); size_t left=(size_t)(end-po); if (lRes<left) {memcpy(po,rbuf,lRes); po+=lRes; lRes=0;}	\
						else {memcpy(po,rbuf,left); lRes-=left; pRes=rbuf+left; return RC_OK;}}}
#define	DOU_OUT(a,b)	{if (po<ypo) {afy_enc16(po,a); memcpy(po,&b,8); po+=8; assert(po<end);}	\
						else {byte *p=rbuf; afy_enc16(p,a); memcpy(p,&b,8); p+=8;				\
						lRes=(size_t)(p-rbuf); size_t left=(size_t)(end-po); if (lRes<left) {memcpy(po,rbuf,lRes); po+=lRes; lRes=0;}	\
						else {memcpy(po,rbuf,left); lRes-=left; pRes=rbuf+left; return RC_OK;}}}
#define	F64_OUT(a,b)	{if (po<ypo) {afy_enc16(po,a); memcpy(po,&b,8); po+=8; assert(po<end);}\
						else {byte *p=rbuf; afy_enc16(p,a); memcpy(p,&b,8); p+=8;				\
						lRes=(size_t)(p-rbuf); size_t left=(size_t)(end-po); if (lRes<left) {memcpy(po,rbuf,lRes); po+=lRes; lRes=0;}	\
						else {memcpy(po,rbuf,left); lRes-=left; pRes=rbuf+left; return RC_OK;}}}
#define	BUF_OUT(a,b,c)	{if (po+c+7<=end) {afy_enc16(po,a); afy_enc32(po,c); memcpy(po,b,c); po+=c;}								\
						else if (po<ypo) {afy_enc16(po,a); afy_enc32(po,c); if (po+c<end) {memcpy(po,b,c); po+=c;}				\
						else {uint32_t l=uint32_t(end-po); if (l!=0) memcpy(po,b,l); lRes=c-l; pRes=(byte*)b+l; return RC_OK;}}	\
						else {byte *p=rbuf; afy_enc16(p,a); afy_enc32(p,c); lRes=(size_t)(p-rbuf); size_t left=(size_t)(end-po);	\
						if (lRes>left) {memcpy(po,rbuf,left); lRes-=left; pRes=rbuf+left; lRes2=c; pRes2=b; return RC_OK;}		\
						else {memcpy(po,rbuf,lRes); po+=lRes; if ((left-=lRes)>=c) {memcpy(po,b,c); po+=c; lRes=0;} else {		\
						if (left!=0) memcpy(po,b,left); lRes=c-left; pRes=(byte*)b+left; return RC_OK;}}}}

static	const uint32_t requiredFields[] = {0, 3, 0, 1, 1, 0, 1, 3, 3, 4, 7, 0, 1, 1, 0, 0, 3, 0, 0};
static	const uint32_t repeatedFields[] = {0x7FC, 0, 16, 0, 2, 2, 0, 0, 2, 0, 0, 0, 12, 14, 0, 0, 0, 2, 0};

uint64_t EncodePB::length(const Value& v,uint8_t vty)
{
	uint64_t l=0ULL,ll; uint32_t u; uint64_t u64; const Value *cv;
	switch (v.type) {
	default: return 0ULL;
	case VT_INT: u=afy_enc32zz(v.i); l=afy_len32(u); break;
	case VT_URIID: l=length(v.uid)-1; break;
	case VT_IDENTITY: l=length(v.iid,false)-1; break;
	case VT_UINT: l=afy_len32(v.ui); break;
	case VT_INT64: u64=afy_enc64zz(v.i64); l=afy_len64(u64); break;
	case VT_UINT64: l=afy_len64(v.ui64); break;
	case VT_FLOAT: l=4+(v.qval.units!=Un_NDIM?afy_len16(UNITS_TAG)+afy_len16(v.qval.units):0); break;
	case VT_DOUBLE: l=8+(v.qval.units!=Un_NDIM?afy_len16(UNITS_TAG)+afy_len16(v.qval.units):0); break;
	case VT_BOOL: l=1; break;
	case VT_DATETIME: l=8; break;
	case VT_INTERVAL: l=8; break;
	case VT_ENUM: l=length(v.enu.enumid)+afy_len32(v.enu.eltid); break;
	case VT_STRING: case VT_BSTR: l=v.length; break;
	case VT_REF: l=length(v.pin->getPID()); break;
	case VT_REFID: l=length(v.id); break;
	case VT_REFELT: l=1+afy_len32(v.ref.eid);
	case VT_REFPROP: u=length(v.ref.pin->getPID()); l+=1+afy_len16(u)+u+length(v.ref.pid); break;
	case VT_REFIDELT: l=1+afy_len32(v.refId->eid);
	case VT_REFIDPROP: u=length(v.refId->id); l+=1+afy_len16(u)+u+length(v.refId->pid); break;
	case VT_STREAM: l=v.stream.is->length(); break;
	case VT_EXPR: l=length((Expr*)v.expr); break;
	case VT_STMT: l=length((Stmt*)v.stmt); break;
	case VT_COLLECTION:
		if (v.isNav()) {
			l=v.nav->count(); l=1+afy_len32(l);
			for (cv=v.nav->navigate(GO_FIRST); cv!=NULL; cv=v.nav->navigate(GO_NEXT)) {ll=length(*cv,VT_ELT); l+=1+afy_len64(ll)+ll;}
			if (vty==VT_VARRAY) return l; break;
		}
	case VT_RANGE: case VT_STRUCT:
		l=1+afy_len32(v.length);
		for (u=0; u<v.length; u++) {ll=length(v.varray[u],v.type==VT_COLLECTION?VT_ELT:v.type==VT_STRUCT?VT_FLD:VT_MIN); l+=1+afy_len64(ll)+ll;}
		if (vty==VT_VARRAY) return l; break;
	case VT_MAP:
		//???
		return 0ULL;
	case VT_ARRAY:
		//???
		return 0ULL;
	case VT_CURRENT: l=1; break;
	}
	uint32_t tg=tag(v); if ((tg&7)==2) l+=afy_len64(l);
	if (vty==VT_ELT) {if (v.eid!=~0u) l+=afy_len16(EID_TAG)+afy_len32(v.eid);}
	else if (vty==VT_FULL||vty==VT_FLD) l+=length(v.property)+(v.meta!=0?afy_len16(META_TAG)+afy_len8(v.meta):0);
	return l+afy_len16(tg)+afy_len16(TYPE_TAG)+1;
}

uint32_t EncodePB::length(uint32_t id,bool fProp)
{
	if (id==STORE_INVALID_URIID) return 0;
	if (id!=STORE_OWNER && (!fProp || id>MAX_BUILTIN_URIID)) {
		IDCache& ca=fProp?propCache:identCache; RC rc;
		if (BIN<uint32_t>::find(id,ca.ids,ca.nids)==NULL) {
			if ((rc=BIN<uint32_t>::insert(ca.newIds,ca.nnids,id,id,&cache,&ca.xnids))!=RC_OK) {/*???*/}
		}
	}
	return 1+afy_len32(id);
}

void EncodePB::setID(uint32_t id,bool fProp)
{
	const static StrLen unk("???",3);
	if (fProp) {
		assert(id>MAX_BUILTIN_URIID);
		URI *uri=(URI*)ses->getStore()->uriMgr->ObjMgr::find(id); const StrLen *p=uri!=NULL?uri->getName():&unk;
		if (p->len>xCopied && (copied=(byte*)cache.realloc(copied,p->len,xCopied))!=NULL) xCopied=p->len;
		if (copied!=NULL) memcpy(copied,p->str,lCopied=p->len); else lCopied=0; if (uri!=NULL) uri->release();
	} else {
		assert(id!=STORE_OWNER);
		Identity *ident=(Identity*)ses->getStore()->identMgr->ObjMgr::find(id); const StrLen *p=ident!=NULL?ident->getName():&unk;
		if (p->len>xCopied && (copied=(byte*)cache.realloc(copied,p->len,xCopied))!=NULL) xCopied=p->len;
		if (copied!=NULL) memcpy(copied,p->str,lCopied=p->len); else lCopied=0; if (ident!=NULL) ident->release();
	}
	IDCache& ca=fProp?propCache:identCache; RC rc;
	if ((rc=BIN<uint32_t>::insert(ca.ids,ca.nids,id,id,&cache,&ca.xids))!=RC_OK) {/* ??? */}
	code=id;
}

uint32_t EncodePB::length(const Expr *exp)
{
	if (rtt==RTT_SRCPINS) {SOutCtx out(ses); return exp->render(0,out)==RC_OK?uint32_t(out.getCLen()):0;}
	// properties and identities ???
	return (uint32_t)exp->serSize();
}

uint32_t EncodePB::length(const Stmt *stmt)
{
	if (rtt==RTT_SRCPINS) {
		SOutCtx out(ses); return stmt->render(out)==RC_OK?(uint32_t)out.getCLen():0;
	}
	// properties and identities ???
	return (uint32_t)stmt->serSize();
}

void EncodePB::cleanup()
{
	sidx=0; cache.truncate(TR_REL_ALL);
	propCache.ids=propCache.newIds=NULL; propCache.nids=propCache.nnids=0;
	identCache.ids=identCache.newIds=NULL; identCache.nids=identCache.nnids=0;
	lRes=lRes2=0; pRes=pRes2=NULL; oSize=0; copied=NULL; lCopied=xCopied=0;
	code=0; fDone=false;
}

RC EncodePB::encode(unsigned char *buf,size_t& lbuf)
{
	try {
		if (buf==NULL||lbuf==0) return RC_INVPARAM;
		byte *po=buf,*const end=buf+lbuf,*const ypo=end-SAFETY_MARGIN_OUT; fDone=false;
		if (lRes!=0) {
			if (lRes>lbuf) {memcpy(buf,pRes,lbuf); pRes+=lbuf; lRes-=lbuf; return RC_OK;}
			memcpy(po,pRes,lRes); po+=lRes; lRes=0;
			if (lRes2!=0) {
				size_t left=(size_t)(end-po);
				if (lRes2>left) {memcpy(po,pRes2,left); pRes=pRes2+left; lRes=lRes2-left; lRes2=0; return RC_OK;}
				memcpy(po,pRes2,lRes2); po+=lRes2; lRes2=0;
			}
		}
		Identity *ident; uint16_t sid,tg; uint64_t sz64; uint32_t sz,i; byte *p; const Value *cv; byte ty; const StrLen *sl; RC rc;
		for (;;) {
		again:
			switch (os.type) {
			case ST_PBSTREAM:
				assert(sidx==0);
				switch (os.state) {
				default: goto error;
				case 0:
					ident=(Identity*)ses->getStore()->identMgr->ObjMgr::find(STORE_OWNER); assert(ident!=NULL);
					sl=ident->getName(); lCopied=(uint32_t)sl->len;
					if (lCopied>xCopied && (copied=(byte*)cache.realloc(copied,lCopied,xCopied))!=NULL) xCopied=lCopied;
					if (copied!=NULL) memcpy(copied,sl->str,lCopied); else lCopied=0;
					os.state++; code=STORE_OWNER; push_state(ST_STRMAP,NULL,OWNER_TAG); continue;
				case 1:
					os.state++; if ((sid=ses->getStore()->storeID)!=0) VAR_OUT(STOREID_TAG,afy_enc16,sid);
				case 2:
					if (!fDump) break;
					os.state++; os.nextDirPage=ses->getStore()->theCB->getRoot(MA_HEAPDIRFIRST);
				case 3:
					if ((rc=ses->getStore()->hdirMgr->getReserved(code,os.nextDirPage,copied,xCopied,&cache))!=RC_OK) {if (rc!=RC_EOF) return rc; break;}
					for (i=0,sz=1+afy_len32(code); i<code; i++) sz+=1+afy_len32(((uint32_t*)copied)[i]);
					os.state++; os.idx=0; VAR_OUT(RESPAGES_TAG,afy_enc32,sz);
				case 4:
					os.state++; VAR_OUT(RESPG_NPGS_TAG,afy_enc32,code);
				case 5:
					while (os.idx<code) {sz=((uint32_t*)copied)[os.idx++]; VAR_OUT(RESPG_NPGS_TAG,afy_enc32,sz);}
					os.state=3; continue;
				}
				fDone=true; lbuf-=po-buf; return RC_OK;
			case ST_STRMAP:
				switch (os.state) {
				default: goto error;
				case 0:
					sz=1+afy_len16(lCopied)+(uint32_t)lCopied+1+afy_len32(code);	//??? which code
					os.state++; VAR_OUT(os.tag,afy_enc32,sz);
				case 1:
					assert(os.obj==NULL && copied!=NULL && lCopied!=0);
					os.state++; BUF_OUT(MAP_STR_TAG,copied,lCopied);
				case 2:
					os.state++; lCopied=0; VAR_OUT(MAP_ID_TAG,afy_enc32,code);
				case 3:
					break;
				}
				break;
			case ST_PIN:
				assert(os.pin!=NULL);
				switch (os.state) {
				default: goto error;
				case 0:
					if (os.pin->id.isPID()) {sz=length(os.pin->id); oSize=1+afy_len16(sz)+sz;} else oSize=0;
					if (rtt!=RTT_PIDS) {
						sz=os.pin->mode&(PIN_NO_REPLICATION|PIN_REPLICATED|PIN_HIDDEN); if (sz!=0) oSize+=1+afy_len32(sz);
						if (os.pin->nProperties!=0) oSize+=1+afy_len32(os.pin->nProperties);
						if (os.pin->properties!=NULL) for (i=0; i<os.pin->nProperties; i++)
							{sz64=length(os.pin->properties[i],VT_FULL); oSize+=1+afy_len64(sz64)+sz64;}		// save ???
					}
					if (os.fCid && sidx==0) oSize+=1+afy_len64(cid);
					os.state++; os.idx=0;
				case 1:
					while (os.idx<propCache.nnids)
						{setID(propCache.newIds[os.idx++]); push_state(ST_STRMAP,NULL,STRPROP_TAG); goto again;}
					os.state++; os.idx=0; propCache.nnids=0;
				case 2:
					while (os.idx<identCache.nnids)
						{setID(identCache.newIds[os.idx++],false); push_state(ST_STRMAP,NULL,STRIDENT_TAG); goto again;}
					os.idx=0; identCache.nnids=0;
					os.state++; VAR_OUT(PIN_TAG,afy_enc64,oSize);
				case 3:
					os.state++;
					if (os.pin->id.isPID()) {push_state(ST_PID,&os.pin->id,ID_TAG); continue;}
				case 4:
					os.state++; 
					if (rtt!=RTT_PIDS && (sz=os.pin->mode&(PIN_NO_REPLICATION|PIN_REPLICATED|PIN_HIDDEN))!=0) VAR_OUT(MODE_TAG,afy_enc32,sz);
				case 5:
					os.state++; if (rtt!=RTT_PIDS && os.pin->nProperties!=0) VAR_OUT(NVALUES_TAG,afy_enc32,os.pin->nProperties);
				case 6:
					if (rtt!=RTT_PIDS && os.pin->properties!=NULL) while (os.idx<os.pin->nProperties)
						{push_state(ST_VALUE,&os.pin->properties[os.idx++],VALUES_TAG); goto again;}
					os.state++;
				case 7:
					os.state++; if (os.fCid && sidx==0) VAR_OUT(CID_TAG,afy_enc64,cid);
				case 8:
					rtt=RTT_PINS; if (sidx==0) {fDone=true; lbuf-=po-buf; return RC_OK;}
					break;
				}
			case ST_VALUE:
				switch (os.state) {
				default: goto error;
				case 0:
					if (sidx==0) {os.tag=VALUE_TAG; os.vtype=VT_FULL;} oSize=length(*os.pv,os.vtype); 
					if (sidx!=0) {os.state=3; continue;} if (os.fCid) oSize+=1+afy_len64(cid);
					os.state++; os.idx=0;
				case 1:
					while (os.idx<propCache.nnids)
						{setID(propCache.newIds[os.idx++]); push_state(ST_STRMAP,NULL,STRPROP_TAG); goto again;}
					os.state++; os.idx=0; propCache.nnids=0;
				case 2:
					while (os.idx<identCache.nnids)
						{setID(identCache.newIds[os.idx++],false); push_state(ST_STRMAP,NULL,STRIDENT_TAG); goto again;}
					os.state++; os.idx=0; identCache.nnids=0;
				case 3:
					os.state++; VAR_OUT(os.tag,afy_enc64,oSize);
				case 4:
					os.state++; ty=wtype(*os.pv); VAR_OUT(TYPE_TAG,afy_enc8,ty);
				case 5:
					os.state++; 
					if ((os.vtype==VT_FULL||os.vtype==VT_FLD) && os.pv->property!=STORE_INVALID_URIID) VAR_OUT(PROPERTY_TAG,afy_enc32,os.pv->property);
				case 6:
					tg=tag(*os.pv); os.state++; lCopied=0;
					switch (os.pv->type) {
					case VT_ANY: break;
					case VT_STRING: case VT_BSTR:
						BUF_OUT(tg,os.pv->bstr,os.pv->length); break;
					case VT_INT:
						sz=afy_enc32zz(os.pv->i); VAR_OUT(tg,afy_enc32,sz); break;
					case VT_UINT: case VT_URIID: case VT_IDENTITY: case VT_CURRENT:
						VAR_OUT(tg,afy_enc32,os.pv->ui); break;
					case VT_INT64:
						sz64=afy_enc64zz(os.pv->i64); VAR_OUT(tg,afy_enc64,sz64); break;
					case VT_UINT64:
						VAR_OUT(tg,afy_enc64,os.pv->ui64); break;
					case VT_FLOAT:
						FLO_OUT(tg,os.pv->f); break;
					case VT_DOUBLE:
						DOU_OUT(tg,os.pv->d); break;
					case VT_BOOL:
						VAR_OUT(tg,afy_enc8,os.pv->b); break;
					case VT_DATETIME: case VT_INTERVAL:
						F64_OUT(tg,os.pv->ui64); break;
					case VT_ENUM:
						push_state(ST_ENUM,os.pv,tg); break;
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
							if (lCopied>xCopied) {if ((copied=(byte*)cache.realloc(copied,lCopied,xCopied))!=NULL) xCopied=lCopied; else return RC_NOMEM;}
							p=((Expr*)os.pv->expr)->serialize(copied); assert(copied+lCopied==p);
						} else {
							SOutCtx out(ses);
							if ((rc=((Expr*)os.pv->expr)->render(0,out))!=RC_OK) return rc;
							byte *p=out.result(lCopied); tg=tags[VT_STRING];
							if (p!=NULL && lCopied!=0) {
								if (lCopied>xCopied) {if ((copied=(byte*)cache.realloc(copied,lCopied,xCopied))!=NULL) xCopied=lCopied; else return RC_NOMEM;}
								memcpy(copied,p,lCopied); ses->free(p);
							}
						}
						BUF_OUT(tg,copied,lCopied); break;
					case VT_STMT:
						if (rtt!=RTT_SRCPINS) {
							lCopied=((Stmt*)os.pv->stmt)->serSize();
							if (lCopied>xCopied) {if ((copied=(byte*)cache.realloc(copied,lCopied,xCopied))!=NULL) xCopied=lCopied; else return RC_NOMEM;}
							p=((Stmt*)os.pv->stmt)->serialize(copied); assert(copied+lCopied==p);
						} else {
							SOutCtx out(ses);
							if ((rc=((Stmt*)os.pv->stmt)->render(out))!=RC_OK) return rc;
							byte *p=out.result(lCopied); tg=tags[VT_STRING];
							if (p!=NULL && lCopied!=0) {
								if (lCopied>xCopied) {if ((copied=(byte*)cache.realloc(copied,lCopied,xCopied))!=NULL) xCopied=lCopied; else return RC_NOMEM;}
								memcpy(copied,p,lCopied); ses->free(p);
							}
						}
						BUF_OUT(tg,copied,lCopied); break;
					case VT_COLLECTION:
					case VT_RANGE:
					case VT_STRUCT:
						push_state(ST_VARRAY,os.pv,tg); continue;
					case VT_STREAM:
						push_state(ST_STREAM,os.pv->stream.is,tg); continue;
					case VT_MAP:
						push_state(ST_MAP,os.pv,tg); continue;
					case VT_ARRAY:
					default:
						//???
						break;
					}
				case 7:
					os.state++; lCopied=0;
					if ((os.pv->type==VT_FLOAT||os.pv->type==VT_DOUBLE) && os.pv->qval.units!=Un_NDIM)
						VAR_OUT(UNITS_TAG,afy_enc16,os.pv->qval.units);
				case 8:
					os.state++;
					if (os.vtype==VT_ELT) {
						if (os.pv->eid!=~0u) VAR_OUT(EID_TAG,afy_enc32,os.pv->eid);
					} else if (os.vtype==VT_FULL||os.vtype==VT_FLD) {
						if (os.pv->meta!=0) VAR_OUT(META_TAG,afy_enc8,os.pv->meta);
					}
				case 9:
					if (sidx!=0) break;
					os.state++; if (os.fCid) VAR_OUT(CID_TAG,afy_enc64,cid);
				case 10:
					fDone=true; lbuf-=po-buf; return RC_OK;
				}
				break;
			case ST_VARRAY:
				switch (os.state) {
				default: goto error;
				case 0:
					assert(os.pv->type==VT_COLLECTION||os.pv->type==VT_RANGE||os.pv->type==VT_STRUCT);
					os.state++; sz64=length(*os.pv,VT_VARRAY); VAR_OUT(os.tag,afy_enc64,sz64);
				case 1:
					sz=os.pv->type==VT_COLLECTION&&os.pv->isNav()?os.pv->nav->count():os.pv->length;
					os.state++; VAR_OUT(VARRAY_L_TAG,afy_enc32,sz);
				case 2:
					os.state++;
					if (os.pv->type!=VT_COLLECTION||!os.pv->isNav()) os.idx=0;
					else {
						if ((cv=os.pv->nav->navigate(GO_FIRST))==NULL) break;
						push_state(ST_VALUE,cv,VARRAY_V_TAG,VT_ELT); continue;
					}
				case 3:
					if (os.pv->type==VT_COLLECTION && os.pv->isNav()) {
						// release after each?
						while ((cv=os.pv->nav->navigate(GO_NEXT))!=NULL) {push_state(ST_VALUE,cv,VARRAY_V_TAG,VT_ELT); goto again;}
						os.pv->nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID);
					} else while (os.idx<os.pv->length)
						{push_state(ST_VALUE,&os.pv->varray[os.idx++],VARRAY_V_TAG,os.pv->type==VT_COLLECTION?VT_ELT:os.pv->type==VT_STRUCT?VT_FLD:VT_MIN); goto again;}
					break;
				}
				break;
			case ST_MAP:
				switch (os.state) {
				default: goto error;
				case 0:
					assert(os.pv->type==VT_MAP);
					os.state++; sz64=length(*os.pv,VT_VARRAY); VAR_OUT(os.tag,afy_enc64,sz64);		// VT_MAP?
				case 1:
					sz=os.pv->map->count();
					os.state++; VAR_OUT(VMAP_L_TAG,afy_enc32,sz);
				case 2:
					os.state++;
//					if ((cv=os.pv->nav->navigate(GO_FIRST))==NULL) break;
//					push_state(ST_VALUE,cv,VMAP_V_TAG,true); continue;
				case 3:
					// release after each?
//					while ((cv=os.pv->nav->navigate(GO_NEXT))!=NULL) {push_state(ST_VALUE,cv,VMAP_V_TAG,true); goto again;}
					break;
				}
				break;
			case ST_ENUM:
				assert(os.pv->type==VT_ENUM);
				switch (os.state) {
				default: goto error;
				case 0:
					sz=length(os.pv->enu.enumid)+afy_len32(os.pv->enu.eltid);
					os.state++; VAR_OUT(os.tag,afy_enc16,sz);
				case 1:
					os.state++; VAR_OUT(ENU_ENUID_TAG,afy_enc32,os.pv->enu.enumid);
				case 2:
					os.state++; VAR_OUT(ENU_ELTID_TAG,afy_enc32,os.pv->enu.eltid);
				case 3:
					break;
				}
				break;
			case ST_STREAM:
				switch (os.state) {
				case 0: 
					os.state++; sz64=os.str->length(); VAR_OUT(os.tag,afy_enc64,sz64);
				case 1:
					sz=uint32_t(end-po); if ((i=(uint32_t)os.str->read(po,sz))>=sz) return RC_OK;
					po+=i; break;
				}
				break;
			case ST_PID:
				switch (os.state) {
				default: goto error;
				case 0:
					os.state++; sz=length(*os.id); VAR_OUT(os.tag,afy_enc16,sz);
				case 1:
					os.state++; VAR_OUT(PID_ID_TAG,afy_enc64,os.id->pid);
				case 2:
					os.state++; if (os.id->ident!=STORE_OWNER) VAR_OUT(PID_IDENT_TAG,afy_enc32,os.id->ident);
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
						if (os.pv->type==VT_REFELT) sz+=1+afy_len32(os.pv->ref.eid);
					} else {
						sz=2+length(os.pv->refId->id)+length(os.pv->refId->pid);
						if (os.pv->type==VT_REFIDELT) sz+=1+afy_len32(os.pv->refId->eid);
					}
					os.state++; VAR_OUT(os.tag,afy_enc16,sz);
				case 1:
					os.state++;
					push_state(ST_PID,os.pv->type==VT_REFPROP||os.pv->type==VT_REFELT?&os.pv->ref.pin->getPID():&os.pv->refId->id,REF_PID_TAG);
					continue;
				case 2:
					os.state++; sz=os.pv->type==VT_REFPROP||os.pv->type==VT_REFELT?os.pv->ref.pid:os.pv->refId->pid;
					VAR_OUT(REF_PROP_TAG,afy_enc32,sz);
				case 3:
					if (os.pv->type==VT_REFELT) sz=os.pv->ref.eid; 
					else if (os.pv->type==VT_REFIDELT) sz=os.pv->refId->eid;
					else break;
					os.state++; VAR_OUT(REF_EID_TAG,afy_enc32,sz);
				case 4:
					break;
				}
				break;
			case ST_STMT:
				assert(sidx==0 && os.stmt!=NULL);
				switch (os.state) {
				default: goto error;
				case 0:
#if 0
					sz=os.res->rc==RC_OK?1+afy_len64(os.res->cnt):1+afy_len32(unsigned(os.res->rc));
					if (os.fCid) sz+=1+afy_len64(cid);
					if (os.res->op!=MODOP_QUERY) sz+=1+afy_len32(unsigned(os.res->op));
					os.state++; VAR_OUT(RESULT_TAG,afy_enc32,sz);
				case 1:
					os.state++; if (os.fCid) VAR_OUT(RES_CID_TAG,afy_enc64,cid);
				case 2:
					os.state++;
					if (os.res->rc==RC_OK) {VAR_OUT(RES_COUNT_TAG,afy_enc64,os.res->cnt);}
					else {VAR_OUT(RES_ERR_TAG,afy_enc32,unsigned(os.res->rc));}
				case 3:
					os.state++; if (os.res->op!=MODOP_QUERY) VAR_OUT(RES_OP_TAG,afy_enc32,unsigned(os.res->op));
#endif
				case 4:
					fDone=true; lbuf-=po-buf; return RC_OK;
				}
			case ST_RESULT:
				assert(sidx==0 && os.res!=NULL);
				switch (os.state) {
				default: goto error;
				case 0:
					sz=os.res->rc==RC_OK?1+afy_len64(os.res->cnt):1+afy_len32(unsigned(os.res->rc));
					if (os.fCid) sz+=1+afy_len64(cid);
					if (os.res->op!=MODOP_QUERY) sz+=1+afy_len32(unsigned(os.res->op));
					os.state++; VAR_OUT(RESULT_TAG,afy_enc32,sz);
				case 1:
					os.state++; if (os.fCid) VAR_OUT(RES_CID_TAG,afy_enc64,cid);
				case 2:
					os.state++;
					if (os.res->rc==RC_OK) {VAR_OUT(RES_COUNT_TAG,afy_enc64,os.res->cnt);}
					else {VAR_OUT(RES_ERR_TAG,afy_enc32,unsigned(os.res->rc));}
				case 3:
					os.state++; if (os.res->op!=MODOP_QUERY) VAR_OUT(RES_OP_TAG,afy_enc32,unsigned(os.res->op));
				case 4:
					fDone=true; lbuf-=po-buf; return RC_OK;
				}
			case ST_COMPOUND:
				assert(sidx==0);
				//????
				fDone=true; lbuf-=po-buf; return RC_OK;
			case ST_STATUS:
				assert(sidx==0);
				//????
				fDone=true; lbuf-=po-buf; return RC_OK;
			case ST_STREDIT: case ST_UID: case ST_RESPAGES: case ST_FLUSH: case ST_TX: goto error;
			case ST_EOS:
				assert(sidx==0);
				if (os.state==0) {os.state++; VAR_OUT(EOS_TAG,afy_enc8,0);}
				fDone=true; lbuf-=po-buf; return RC_OK;
			}
			assert(sidx>0); os=stateStack[--sidx];
		}
error:
		// report
		return RC_INTERNAL;
	} catch (RC rc) {return rc;} catch(...) {/*...*/return RC_INTERNAL;}
}

RC ProtoBufStreamOut::next(unsigned char *buf,size_t& lbuf)
{
	if (buf==NULL || lbuf==0) return RC_INVPARAM; if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
	size_t left=lbuf; RC rc=RC_OK;
	while (ic!=NULL) {
		if (left==0 || (rc=enc.encode(buf+lbuf-left,left))!=RC_OK || !enc.isDone()) {ses->releaseAllLatches(); return rc;}
		if ((rc=ic->next(res))!=RC_OK) {result.cnt=ic->getCount(); ic->destroy(); ic=NULL;}
		else if (res.type==VT_REF) enc.set((PIN*)res.pin); else enc.set(&res);		// join ???
	}
	if (!fRes) {enc.set(&result); fRes=true;} //result.rc=???
	if (left==0) rc=RC_OK; else if ((rc=enc.encode(buf+lbuf-left,left))==RC_OK) lbuf-=left;
	return rc!=RC_OK||!enc.isDone()||lbuf!=0?rc:RC_EOF;
}

//-----------------------------------------------------------------------------------------------------------------------

uint32_t DecodePB::map(uint32_t id,bool fProp)
{
	if (fProp && id>MAX_BUILTIN_URIID) {
		const URIIDMapElt *uid=uriMap.find(id); if (uid!=NULL) id=uid->mapped;
	} else if (!fProp && id!=STORE_OWNER) {
		const IdentityIDMapElt *iid=identMap.find(id); if (iid!=NULL) id=iid->mapped;
	}
	return id;
}

RC DecodePB::addToMap(bool fProp)
{
	if (fProp && is.idx>MAX_BUILTIN_URIID) {
		URIIDMap::Find find(uriMap,is.idx);
		if (find.find()==NULL) {
			URIIDMapElt *u=new(ma) URIIDMapElt; if (u==NULL) return RC_NOMEM;
			URI *uri=ctx->uriMgr->insert((char*)mapBuf,(size_t)lSave); if (uri==NULL) return RC_NOMEM;
			u->id=is.idx; u->mapped=uri->getID(); uri->release(); uriMap.insert(u,find.getIdx());
		}
	} else if (!fProp && is.idx!=STORE_OWNER) {
		IdentityIDMap::Find find(identMap,is.idx);
		if (find.find()==NULL) {
			IdentityIDMapElt *ii=new(ma) IdentityIDMapElt; if (ii==NULL) return RC_NOMEM;
			Identity *ident=(Identity*)ctx->identMgr->find((char*)mapBuf,(size_t)lSave);
			if (ident==NULL) {
				//???
			} else {
				ii->id=is.idx; ii->mapped=ident->getID(); ident->release(); identMap.insert(ii,find.getIdx());
			}
		}
	}
	return RC_OK;
}

RC DecodePB::decode(const unsigned char *in,size_t lbuf,IMemAlloc *ra)
{
	const byte *const buf0=in,*const end=in+lbuf; RC rc;
	if (inState==ST_RET) {
		restore_state(in);
		if (sidx!=0) {assert(sidx==1); is=stateStack[--sidx]; goto check_end;}
		advance(); is.type=ST_PBSTREAM;
	} else if (in==NULL||lbuf==0) return RC_INVPARAM;
#if 0
	const byte *const yp=(end[-1]&0x80)==0?end:end-SAFETY_MARGIN32;
#endif
	for (;;) {
		if (left!=0) {
			size_t l=end-in;
			if (l<left) {
				if (l!=0) {left-=l; if (sbuf!=NULL) {memcpy(sbuf,in,l); sbuf+=l;}}
				offset+=lbuf; ileft=0; return RC_OK;
			}
			if (sbuf!=NULL) memcpy(sbuf,in,(size_t)left);
			in+=left; left=0;
			if (sbuf==NULL || inState==ST_LEN) goto check_end;
		} else {
			// if (in<yp) {afy_dec64(in,val);} else	// don't reset val!!!
			do {
				if (in>=end) {offset+=lbuf; ileft=0; return RC_OK;}
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
				lField=val; assert((is.tag&7)==2);
				if (offset+(in-buf0)+lField>is.msgEnd) return RC_CORRUPTED;
				break;
			case ST_READ:
				assert((is.tag&7)==0);
				if (offset+(in-buf0)>is.msgEnd) return RC_CORRUPTED;
				break;
			case ST_RET: assert(0); break;
			}
		}
		assert(inState==ST_LEN||inState==ST_READ);
		{const uint32_t fieldN=(is.tag>>3);
		if (fieldN<1 || fieldN>32) {set_skip(); continue;}
		const uint32_t fieldBit=1<<(fieldN-1); RefVID *rv;
		if ((is.fieldMask&fieldBit)!=0 && (repeatedFields[is.type]&fieldBit)==0) return RC_CORRUPTED;
		is.fieldMask|=fieldBit;
		switch (is.type) {
		default: assert(0);
		case ST_PBSTREAM:
			assert(sidx==0);
			switch (is.tag) {
			default: set_skip(); continue;
			case OWNER_TAG: case STRPROP_TAG: case STRIDENT_TAG:
				push_state(ST_STRMAP,NULL,in-buf0); continue;
			case STOREID_TAG:
				storeID=(uint16_t)val; advance(); continue;
			case PIN_TAG:
				memset(&u,0,sizeof(u)); push_state(ST_PIN,NULL,in-buf0); continue;
			case STMT_TAG:
				memset(&u,0,sizeof(u)); push_state(ST_STMT,NULL,in-buf0); continue;
			case TXOP_TAG:
				is.type=ST_TX; u.u64=val; save_state(in,buf0,end-in); return RC_OK;
			case FLUSH_TAG:
				is.type=ST_FLUSH; save_state(in,buf0,end-in); return RC_OK;
			case EOS_TAG:
				is.type=ST_EOS; save_state(in,buf0,end-in); return RC_OK;
			case RESPAGES_TAG:
				if (stype!=SITY_DUMPLOAD) set_skip(); else push_state(ST_RESPAGES,NULL,in-buf0);
				continue;
			case VALUE_TAG:
				memset(&u,0,sizeof(u)); push_state(ST_VALUE,initV(&u.val.v),in-buf0); continue;
			}
		case ST_STRMAP:
			switch (is.tag) {
			default: set_skip(); continue;
			case MAP_STR_TAG:
				if (lField==0) return RC_CORRUPTED;
				if (lMapBuf<(size_t)lField+1) {if ((mapBuf=(byte*)ma->realloc(mapBuf,(size_t)lField+1,lMapBuf))!=NULL) lMapBuf=(size_t)lField+1; else return RC_NOMEM;}
				sbuf=mapBuf; left=lSave=lField; continue;
			case MAP_ID_TAG:
				is.idx=(uint32_t)val; break;
			}
			break;
		case ST_PIN:
			switch (is.tag) {
			default: set_skip(); continue;
			case OP_TAG: u.pin.op=(uint8_t)val; break;
			case ID_TAG: const_cast<PID&>(u.pin.id)=PIN::noPID; push_state(ST_PID,const_cast<PID*>(&u.pin.id),in-buf0); continue;
			case MODE_TAG: u.pin.mode=(uint32_t)val&(PIN_NO_REPLICATION|PIN_REPLICATED|PIN_HIDDEN); break;
			case NVALUES_TAG:
				if (is.idx>(uint32_t)val) return RC_CORRUPTED;
				if (val!=0) {
					if ((u.pin.properties=(Value*)ra->realloc(u.pin.properties,(uint32_t)val*sizeof(Value),u.pin.nProperties*sizeof(Value)))==NULL) return RC_NOMEM;
					if (is.idx<(u.pin.nProperties=(uint32_t)val)) memset(u.pin.properties+is.idx,0,(u.pin.nProperties-is.idx)*sizeof(Value));
				}
				break;
			case VALUES_TAG:
				if ((is.fieldMask&1<<((NVALUES_TAG>>3)-1))==0) {
					if ((u.pin.properties=(Value*)ra->realloc(u.pin.properties,(u.pin.nProperties+1)*sizeof(Value),u.pin.nProperties*sizeof(Value)))==NULL) return RC_NOMEM;
					u.pin.nProperties++;
				} else if (is.idx>=u.pin.nProperties) return RC_CORRUPTED;
				defType=VT_ANY; push_state(ST_VALUE,initV(&u.pin.properties[is.idx++]),in-buf0); continue;
			case CID_TAG: u.pin.cid=val; u.pin.fCid=true; break;
			case RTT_TAG: if (val<RTT_PINS || val>RTT_VALUES) return RC_CORRUPTED; u.pin.rtt=(RTTYPE)val; break;
			}
			break;
		case ST_VALUE:
			assert(is.pv!=NULL);
			if (is.tag>PROPERTY_TAG	&& is.tag<VALUE_OP_TAG && defType!=VT_ANY) return RC_CORRUPTED;
			switch (is.tag) {
			default: set_skip(); continue;
			case TYPE_TAG: if (val>=VT_CURRENT) return RC_CORRUPTED; is.pv->type=(uint8_t)val; break;
			case PROPERTY_TAG: if (val<=STORE_MAX_URIID) is.pv->property=map((uint32_t)val); else return RC_CORRUPTED; break;
			case VALUE_OP_TAG: if (val>=OP_ALL) return RC_CORRUPTED; is.pv->op=(uint8_t)val; break;
			case EID_TAG: is.pv->eid=(ElementID)val; break;
			case META_TAG: is.pv->meta=(uint8_t)val; break;
			case UNITS_TAG: is.pv->qval.units=(uint16_t)val; break;	// check valid unit
			case _L(3):
				defType=VT_STRING;
				if ((sbuf=(byte*)ra->malloc((size_t)lField+1))==NULL) return RC_NOMEM;
				sbuf[(size_t)lField]=0; is.pv->str=(char*)sbuf; is.pv->length=(uint32_t)lField;
				if ((left=lField)!=0) continue; else break;
			case _L(4):
				defType=VT_BSTR;
				if ((sbuf=(byte*)ra->malloc((size_t)lField))==NULL) return RC_NOMEM;
				is.pv->bstr=sbuf; is.pv->length=(uint32_t)lField;
				if ((left=lField)!=0) continue; else break;
			case _V(5): is.pv->i=afy_dec32zz((uint32_t)val); is.pv->length=sizeof(int32_t); defType=VT_INT; break;
			case _V(6): is.pv->ui=(uint32_t)val; is.pv->length=sizeof(uint32_t); defType=VT_UINT; break;
			case _V(7): is.pv->i64=afy_dec64zz(val); is.pv->length=sizeof(int64_t); defType=VT_INT64; break;
			case _V(8): is.pv->ui64=val; is.pv->length=sizeof(uint64_t); defType=VT_UINT64; break;
			case _S(9): is.pv->f=buf.f; is.pv->length=sizeof(float); defType=VT_FLOAT; break;		// byte order???
			case _D(10): is.pv->d=buf.d; is.pv->length=sizeof(double); defType=VT_DOUBLE; break;	// byte order???
			case _D(11): is.pv->ui64=buf.u64; is.pv->length=sizeof(uint64_t); defType=VT_DATETIME; break;	// byte order???
			case _D(12): is.pv->i64=buf.i64; is.pv->length=sizeof(int64_t); defType=VT_INTERVAL; break;	// byte order??? sfixed???
			case _L(13): is.pv->length=1; is.pv->id=PIN::noPID; is.pv->id.ident=STORE_OWNER; push_state(ST_PID,&is.pv->id,in-buf0); defType=VT_REFID; continue;
			case _L(14): push_state(ST_VARRAY,is.pv,in-buf0); defType=VT_COLLECTION; break;
			case _L(15):
				if ((is.pv->refId=rv=new(ra) RefVID)==NULL) return RC_NOMEM;
				rv->id=PIN::noPID; rv->id.ident=STORE_OWNER; rv->pid=STORE_INVALID_URIID; rv->eid=STORE_COLLECTION_ID; rv->vid=STORE_CURRENT_VERSION;
				is.pv->length=1; push_state(ST_REF,is.pv,in-buf0); defType=VT_REFIDPROP; continue;		// VT_REFIDELT?
			case _V(16): is.pv->b=val!=0; is.pv->length=1; defType=VT_BOOL; break;
			case _L(22):
				is.pv->length=sizeof(VEnum); push_state(ST_ENUM,&is.pv,in-buf0); defType=VT_ENUM; continue;
			case _L(23):
				is.pv->length=sizeof(IMap); push_state(ST_MAP,&is.pv,in-buf0); defType=VT_MAP; continue;
			}
			break;
		case ST_VARRAY:
			switch (is.tag) {
			default: set_skip(); continue;
			case VARRAY_L_TAG:
				if ((is.pv->length=(uint32_t)val)<is.idx) return RC_CORRUPTED;
				if (is.pv->length>is.idx) {
					if ((is.pv->varray=(Value*)ra->realloc((void*)is.pv->varray,is.pv->length*sizeof(Value),is.idx*sizeof(Value)))==NULL) return RC_NOMEM;
					memset((void*)(is.pv->varray+is.idx),0,(is.pv->length-is.idx)*sizeof(Value));
				}
				break;
			case VARRAY_V_TAG:
				if ((is.fieldMask&1<<((VARRAY_L_TAG>>3)-1))==0) {
					is.pv->varray=(Value*)ra->realloc((Value*)is.pv->varray,(is.idx+1)*sizeof(Value),is.idx*sizeof(Value));
					if (is.pv->varray==NULL) return RC_NOMEM;
				} else if (is.idx>=is.pv->length) return RC_CORRUPTED;
				defType=VT_ANY; push_state(ST_VALUE,initV((Value*)&is.pv->varray[is.idx++]),in-buf0); continue;
			}
			break;
		case ST_MAP:
#if 0
			// similar to ST_ARRAY
#endif
			break;
		case ST_ENUM:
			switch (is.tag) {
			default: set_skip(); continue;
			case ENU_ENUID_TAG:
				if ((is.pv->enu.enumid=map((uint32_t)val))==STORE_INVALID_URIID) return RC_CORRUPTED;
				break;
			case ENU_ELTID_TAG:
				if ((is.pv->enu.eltid=(uint32_t)val)==STORE_COLLECTION_ID) return RC_CORRUPTED;
				break;
			}
			break;
		case ST_PID:
			switch (is.tag) {
			default: set_skip(); continue;
			case PID_ID_TAG: is.id->pid=val; if (is.id->ident==STORE_INVALID_IDENTITY) is.id->ident=STORE_OWNER; break;
			case PID_IDENT_TAG: is.id->ident=(uint32_t)val==STORE_INVALID_IDENTITY?STORE_INVALID_IDENTITY:map((uint32_t)val,false); break;
			}
			break;
		case ST_REF:
			switch (is.tag) {
			default: set_skip(); continue;
			case REF_PID_TAG:
				const_cast<PID&>(is.pv->refId->id).pid=STORE_INVALID_PID;
				const_cast<PID&>(is.pv->refId->id).ident=STORE_OWNER;
				push_state(ST_PID,(PID*)&is.pv->refId->id,in-buf0); continue;
			case REF_PROP_TAG: ((RefVID*)is.pv->refId)->pid=map((uint32_t)val); break;
			case REF_EID_TAG: ((RefVID*)is.pv->refId)->eid=(ElementID)val; break;
			case REF_VID_TAG: ((RefVID*)is.pv->refId)->vid=(VersionID)val; break;
			}
			break;
		case ST_STMT:
			switch (is.tag) {
			default: set_skip(); continue;
			case STMT_STR_TAG:
				if ((sbuf=(byte*)ra->malloc((size_t)lField+1))==NULL) return RC_NOMEM;
				sbuf[(size_t)lField]=0; u.stmt.str=(char*)sbuf; u.stmt.lstr=(size_t)lField;
				if ((left=lField)!=0) continue; else break;
			case STMT_UID_TAG:
			case STMT_PARAM_TAG:
				//???
				break;
			case STMT_CID_TAG: u.stmt.cid=val; u.stmt.fCid=true; break;
			case STMT_RTT_TAG: if (val<RTT_PINS || val>RTT_VALUES) return RC_CORRUPTED; u.stmt.rtt=(RTTYPE)val; break;
			case STMT_LIMIT_TAG: u.stmt.limit=(uint32_t)val; break;
			case STMT_OFFSET_TAG: u.stmt.offset=(uint32_t)val; break;
			case STMT_MODE_TAG: u.stmt.mode=(uint32_t)val; break;
			case STMT_ABORT_TAG: u.stmt.fAbort=val!=0; break;
			}
			break;
		case ST_RESPAGES:
			switch (is.tag) {
			default: set_skip(); continue;
			case RESPG_NPGS_TAG:
//				if ((is.pv->length=(uint32_t)val)<is.idx) return RC_CORRUPTED;
//				if (is.pv->length>is.idx) {
//					if ((is.pv->varray=(Value*)dataAlloc.realloc((void*)is.pv->varray,is.pv->length*sizeof(Value)))==NULL) return RC_NOMEM;
//					memset((void*)(is.pv->varray+is.idx),0,(is.pv->length-is.idx)*sizeof(Value));
//				}
				break;
			case RESPG_PAGE_TAG:
//				if ((is.fieldMask&1<<((RESPG_NPGS_TAG>>3)-1))==0) {
//					// add 1 elt, set Value to 0
//				} else if (is.idx>=is.pv->length) return RC_CORRUPTED;
//				defType=VT_ANY; push_state(ST_VALUE,initV((Value*)&is.pv->varray[is.idx++]),in-buf0); continue;
				break;
			}
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
				if (is.idx!=u.pin.nProperties) return RC_CORRUPTED;
				assert(sidx==1); save_state(in,buf0,end-in); return RC_OK;
			case ST_PID:
				if (is.id->ident==STORE_OWNER) is.id->ident=owner;
				break;
			case ST_VALUE:
				if (is.pv->type==VT_ANY) is.pv->type=defType;
				switch (is.pv->type) {
				case VT_URIID: is.pv->uid=map(is.pv->uid); break;
				case VT_IDENTITY: is.pv->iid=map(is.pv->iid,false); break;
				case VT_EXPR:
				case VT_STMT:
					// check string, parse
					setHT(*is.pv); break;
				case VT_STRUCT:
					// check properties defined, sort, check no repeting properties
					// ExprNode::normalizeStruct() ???
					setHT(*is.pv); break;
				case VT_MAP:
					// check keys defined, sort, check no repeting keys
					// ExprNode::normalizeMap() ???
					setHT(*is.pv); break;
				case VT_COLLECTION:
					// ExprNode::normalizeCollection() ???
				case VT_STRING: case VT_BSTR: case VT_REF:
				case VT_REFPROP: case VT_REFIDPROP: case VT_REFELT: case VT_REFIDELT: case VT_RANGE:
					setHT(*is.pv); break;
				}
				if (sidx==1) {save_state(in,buf0,end-in); return RC_OK;}
				break;
			case ST_VARRAY:
				if (is.idx!=is.pv->length) return RC_CORRUPTED;
				break;
			case ST_MAP:
				if (is.idx!=is.pv->length) return RC_CORRUPTED;
				// convert to IMap
				break;
			case ST_STMT:
				if (sidx==1) {save_state(in,buf0,end-in); return RC_OK;}
				break;
			case ST_RESPAGES:
				//
				break;
			}
			is=stateStack[--sidx];
		}
		advance();
	}
}

void DecodePB::cleanup()
{
	inState=ST_TAG; sidx=0; lField=0; val=0; vsht=0; offset=0; left=0; sbuf=NULL; defType=VT_ANY; savedIn=NULL; savedBuf=NULL; ileft=0;
	mapBuf=NULL; lMapBuf=0; lSave=0; owner=STORE_OWNER; storeID=0; uriMap.clear(false); identMap.clear(false);
	is.type=ST_PBSTREAM; is.tag=~0u; is.idx=0; is.msgEnd=~0ULL; is.fieldMask=0; is.obj=NULL;
}

RC ProcessStream::next(const unsigned char *in,size_t lbuf)
{
	try {
		if (in==NULL || lbuf==0) {
			RC rc; if (pins!=0 && (rc=commitPINs())!=RC_OK) return rc;
		} else while (!ctx->inShutdown() && decode(in,lbuf,this)==RC_OK) {
			if (inState==ST_RET) {
				RC rc=RC_OK; uint32_t code;
				switch (getSType()) {
				default: rc=RC_INTERNAL; break;
				case ST_PIN:
					if (pins!=0 && (u.pin.op!=MODOP_INSERT || pins>=DEFAULT_PIN_BATCH_SIZE) && (rc=commitPINs())!=RC_OK) return rc;
					if (u.pin.op==MODOP_INSERT) {
						if ((rc=pins.createPIN(u.pin.properties,u.pin.nProperties,u.pin.mode,stype!=SITY_NORMAL?&u.pin.id:NULL))!=RC_OK) return rc;
						OInfo pinoi={u.pin.cid,u.pin.fCid,u.pin.rtt}; if ((rc=oi+=pinoi)!=RC_OK) return rc;
					} else {
						RC rc2=RC_OK;
						if (!u.pin.id.isPID()) rc=RC_INVPARAM;
						else {
							PINx pex(ses,u.pin.id); 
							if ((rc=ses->getStore()->queryMgr->apply(EvalCtx(ses),stmtOp[u.pin.op],pex,u.pin.properties,u.pin.nProperties,MODE_DEVENT))==RC_OK && outStr!=NULL)
								rc2=pinOut(&pex,u.pin.cid,u.pin.fCid,u.pin.rtt);
						}
						if (outStr!=NULL) {Result res={rc,1,(MODOP)u.pin.op}; rc=resultOut(res,u.pin.cid,u.pin.fCid);}
						if (rc2!=RC_OK) rc=rc2; truncate();
					}
					break;
				case ST_FLUSH:
					if (pins!=0) rc=commitPINs();
					if (rc==RC_OK && outStr!=NULL && obleft!=lobuf) {rc=outStr->next(obuf,lobuf-obleft); obleft=lobuf;}
					break;
				case ST_STMT:
					if (u.stmt.stmt==NULL) {
						//compile here
					}
					{Cursor *cr=NULL; Result res={RC_OK,0,modOp[u.stmt.stmt->getOp()]};
					Values vpar(u.stmt.params,u.stmt.nParams); EvalCtx ectx(ses,NULL,0,NULL,0,&vpar,1,NULL,NULL,u.stmt.stmt->getOp()==STMT_INSERT||u.stmt.stmt->getOp()==STMT_UPDATE?ECT_INSERT:ECT_QUERY);
					if ((rc=u.stmt.stmt->execute(ectx,NULL,u.stmt.limit,u.stmt.offset,u.stmt.mode,&res.cnt,TXI_DEFAULT,outStr!=NULL&&u.stmt.rtt!=RTT_COUNT?&cr:(Cursor**)0))==RC_OK && rc==RC_OK) {
						assert(obuf!=NULL); RTTYPE rtt=u.stmt.rtt; PINx *ret=NULL; Value w;
						if (rtt==RTT_DEFAULT) {SelectType st=cr->selectType(); rtt=st==SEL_COUNT?RTT_COUNT:st==SEL_PINSET||st==SEL_AUGMENTED||st==SEL_COMPOUND?RTT_PINS:RTT_VALUES;}
						while (rc==RC_OK && (rc=cr->next(ret,true))==RC_OK) {
							switch (rtt) {
							case RTT_DEFAULT:
							case RTT_COUNT: break;
							case RTT_PINS: case RTT_SRCPINS: case RTT_PIDS:
//								if ((sib=(PINx*)ret->getSibling())==NULL) 
								rc=pinOut((PIN*)ret,u.stmt.cid,u.stmt.fCid,u.stmt.rtt);
//								else {
									// compound
//								}
								break;
							case RTT_VALUES:
//								if ((sib=(PINx*)ret->getSibling())==NULL) {
									w.setStruct((Value*)ret->getValueByIndex(0),ret->getNumberOfProperties()); rc=valueOut(&w,u.stmt.cid,u.stmt.fCid);
//								} else {
									//????
//								}
								break;
							}
						}
						res.cnt=cr->getCount(); cr->destroy(); if (rc==RC_EOF) rc=RC_OK;
					}
					if (outStr!=NULL) {res.rc=rc; rc=resultOut(res,u.stmt.cid,u.stmt.fCid);}
					truncate();}
					break;
				case ST_VALUE:
					//???
					break;
				case ST_TX:
					switch (code=(uint32_t)DecodePB::u.u64) {
					case TXOP_COMMIT:
						if (txLevel!=0) {if ((rc=ctx->txMgr->commitTx(ses,false))==RC_OK) txLevel--; else return rc;}
						break;
					case TXOP_COMMIT_ALL:
						if (txLevel!=0) {if ((rc=ctx->txMgr->commitTx(ses,true))==RC_OK) txLevel=0; else return rc;}
						break;
					case TXOP_START:
					case TXOP_START_READONLY:
					case TXOP_START_MODIFYCLASS:
						if ((rc=ctx->txMgr->startTx(ses,code==TXOP_START?TXT_READWRITE:code==TXOP_START_READONLY?TXT_READONLY:TXT_MODIFYCLASS,TXI_DEFAULT))!=RC_OK) return rc;
						txLevel++; break;
					case TXOP_ROLLBACK:
					case TXOP_ROLLBACK_ALL:
						//???
						break;
					}
					break;
				}
				if (rc!=RC_OK) return rc;
			}
			if (ileft==0) break;
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in StreamIn::next()\n"); return RC_INTERNAL;}
}

RC ProcessStream::resultOut(const Result& res,uint64_t cid,bool fCid)
{
	RC rc=RC_OK; enc.set(&res,cid,fCid); assert(outStr!=NULL && obuf!=NULL);
	do if ((rc=enc.encode(obuf+lobuf-obleft,obleft))==RC_OK && obleft==0) {
		RC rc2=outStr->next(obuf,lobuf); obleft=lobuf; if (rc2!=RC_EOF && rc2!=RC_OK) rc=rc2;
	} while (rc==RC_OK && !enc.isDone());
	return rc;
}

RC ProcessStream::pinOut(PIN *pin,uint64_t cid,bool fCid,RTTYPE rtt)
{
	RC rc=RC_OK;
	if (rtt!=RTT_COUNT) {
		if (rtt==RTT_SRCPINS && (pin->meta&PMT_DATAEVENT)!=0) ses->getStore()->queryMgr->getDataEventInfo(ses,pin);
		enc.set(pin,cid,fCid,rtt);
		do if ((rc=enc.encode(obuf+lobuf-obleft,obleft))==RC_OK && obleft==0) {
			RC rc2=outStr->next(obuf,lobuf); obleft=lobuf; if (rc2!=RC_EOF && rc2!=RC_OK) rc=rc2;
		} while (rc==RC_OK && !enc.isDone());
	}
	return rc;
}

RC ProcessStream::valueOut(const Value *v,uint64_t cid,bool fCid)
{
	RC rc=RC_OK; enc.set(v,cid,fCid);
	do if ((rc=enc.encode(obuf+lobuf-obleft,obleft))==RC_OK && obleft==0) {
		RC rc2=outStr->next(obuf,lobuf); obleft=lobuf; if (rc2!=RC_EOF && rc2!=RC_OK) rc=rc2;
	} while (rc==RC_OK && !enc.isDone());
	return rc;
}

RC ProcessStream::commitPINs()
{
	RC rc=pins.process(false);
	if (outStr!=NULL) {
		if (rc!=RC_OK) {Result res={rc,pins,MODOP_INSERT}; rc=resultOut(res,oi[(unsigned)oi-1].cid,oi[(unsigned)oi-1].fCid);}
		else for (unsigned i=0,n=pins.getNumberOfPINs(); i<n; i++) if ((rc=pinOut(pins[i],oi[i].cid,oi[i].fCid,oi[i].rtt))!=RC_OK) break;
	}
	pins.clear(); oi.clear();
	return rc;
}

RC Stmt::execute(IStreamOut*& result,const Value *params,unsigned nParams,unsigned nReturn,unsigned nSkip,unsigned md,TXI_LEVEL txi)
{
	try {
		result=NULL; Cursor *cr; uint64_t nProcessed=0;
		Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION; if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		TxGuard txg(ses); ses->resetAbortQ(); 
		Values vpar(params,nParams); EvalCtx ectx(ses,NULL,0,NULL,0,&vpar,1,NULL,NULL,op==STMT_INSERT||op==STMT_UPDATE?ECT_INSERT:ECT_QUERY);
		RC rc=execute(ectx,NULL,nReturn,nSkip,md,&nProcessed,txi,(Cursor**)&cr); 
		if (rc==RC_OK) {ProtoBufStreamOut *pb=new(ses) ProtoBufStreamOut(ses,cr); result=pb; if (pb==NULL) {cr->destroy(); rc=RC_NOMEM;}}
		ses->releaseAllLatches(); return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IStmt::execute(IStream*&,...)\n"); return RC_INTERNAL;}
}
		
RC Session::createInputStream(IStreamIn *&in,IStreamIn *out,size_t lo)
{
	try {return ctx->inShutdown()?RC_SHUTDOWN:(in=new(this) ProcessStream(this,out,lo))!=NULL?RC_OK:RC_NOMEM;}
	catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::createInputStream()\n"); return RC_INTERNAL;}
}

RC ServiceEncodePB::invoke(IServiceCtx *sctx,const Value& inp,Value& out,unsigned& mode)
{
	if (out.type!=VT_BSTR || out.bstr==NULL || out.length==0 || (mode&ISRV_READ)!=0) return RC_INVPARAM;
	byte *p=(byte*)out.bstr; size_t l=out.length; RC rc;
	if (fInit) {
		if (os.type==ST_PBSTREAM && os.state==0 && (mode&ISRV_MOREOUT)==0) {
			Value v; v.set(MIME_PROTOBUF,sizeof(MIME_PROTOBUF)-1); v.setPropID(PROP_SPEC_CONTENTTYPE); 
			if ((rc=sctx->getCtxPIN()->modify(&v,1))!=RC_OK) return rc;
		}
		if ((rc=encode(p,l))!=RC_OK) return rc;
		if (!fDone) {mode=mode&~ISRV_EOM|ISRV_MOREOUT|ISRV_KEEPINP; return RC_OK;}
		fInit=false; mode&=~(ISRV_MOREOUT|ISRV_KEEPINP);
		p+=out.length-(uint32_t)l;
	}
	if ((mode&ISRV_MOREOUT)!=0) mode&=~(ISRV_MOREOUT|ISRV_KEEPINP);
	else if (inp.isEmpty()) {out.length=0; return RC_OK;}
	else switch (inp.type) {
	case VT_REF: set((const PIN*)inp.pin); break;		// cid, rettype?
	case VT_STMT: set((const Stmt*)inp.stmt); break;	// cid, rettype?
	default: set(&inp); break;							// cid, rettype?
	// flush, tx?
	}
	if ((rc=encode(p,l))!=RC_OK) return rc;
	if (fDone) {
		out.length-=(uint32_t)l;
		if ((mode&ISRV_EOM)!=0) {
			setEOS(); size_t ll=l;
			if ((rc=encode((byte*)out.bstr+out.length,l))!=RC_OK) return rc;
			out.length+=(uint32_t)(ll-l);
		}
	}
	if (!fDone) mode=mode&~ISRV_EOM|ISRV_MOREOUT|ISRV_KEEPINP;
	return RC_OK;
}

void ServiceEncodePB::cleanup(IServiceCtx *sctx,bool fDestroying)
{
	if (!fDestroying) EncodePB::cleanup(); fInit=true;
}

RC ServiceDecodePB::invoke(IServiceCtx *sctx,const Value& inp,Value& out,unsigned& mode)
{
	RC rc=RC_OK; mode&=~(ISRV_MOREOUT|ISRV_KEEPINP|ISRV_NEEDMORE);
	if (!out.isEmpty() || (mode&ISRV_WRITE)!=0) rc=RC_INVPARAM;
	else if (!inp.isEmpty() && inp.length!=0) {
		if (inp.type!=VT_BSTR) return RC_TYPE; if (inp.bstr==NULL) return RC_INVPARAM;
		IResAlloc *ra=sctx->getResAlloc();
		if ((rc=decode(inp.bstr,inp.length,ra))==RC_OK) {
			if (inState!=ST_RET) {mode|=ISRV_NEEDMORE; assert(ileft==0);}
			else {
				switch (getSType()) {
				default: rc=RC_CORRUPTED; break;
				case ST_VALUE: out=u.val.v; break;
				case ST_PIN: 
					out.op=u.pin.op==MODOP_INSERT?OP_ADD:u.pin.op==MODOP_DELETE?OP_DELETE:OP_SET; 
					rc=ra->createPIN(out,u.pin.properties,u.pin.nProperties,&u.pin.id,u.pin.mode); //??? cid?
					break;
				case ST_STMT:
					if (u.stmt.stmt==NULL) {
						//compile
					}
					out.set((IStmt*)u.stmt.stmt,1); setHT(out,SES_HEAP); break;	// cid, ...
				case ST_TX: //make stmt
					rc=RC_INTERNAL; break;
				case ST_FLUSH:
					mode|=ISRV_FLUSH; break;
				case ST_EOS:
					mode|=ISRV_EOM; break;
				}
				if (ileft!=0) mode|=ISRV_MOREOUT|ISRV_KEEPINP;
			}
		}
	}
	return rc;
}

void ServiceDecodePB::cleanup(IServiceCtx *sct,bool fDestroying)
{
	if (!fDestroying) DecodePB::cleanup();
}

RC ProtobufService::create(IServiceCtx *sctx,uint32_t& dscr,IService::Processor *&ret)
{
	Session *ses=(Session*)sctx->getSession();
	switch (dscr&ISRV_PROC_MASK) {
	default: return RC_INVOP;
	case ISRV_READ:
		// sub alloc?, type
		if ((ret=new(sctx) ServiceDecodePB(ses->getStore(),(StackAlloc*)(ServiceCtx*)sctx,SITY_NORMAL))==NULL) return RC_NOMEM;
		break;
	case ISRV_WRITE:
		// getServiceInfo -> fDump ?
		if ((ret=new(sctx) ServiceEncodePB(ses,false))==NULL) return RC_NOMEM;
		dscr|=ISRV_ALLOCBUF; break;
	}
	return RC_OK;
}
