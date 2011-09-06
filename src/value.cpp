/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "mvstoreimpl.h"
#include "queryprc.h"
#include "stmt.h"
#include "parser.h"
#include "expr.h"
#include "maps.h"
#include "blob.h"
#include <stdio.h>
#include <string.h>

using namespace MVStoreKernel;

RC MVStoreKernel::copyV(const Value &from,Value &to,MemAlloc *ma)
{
	try {
		ulong i; RC rc; Value v; size_t ll;
		if (&to!=&from) to=from; to.flags=NO_HEAP;
		if (ma!=NULL) switch (from.type) {
		default: break;
		case VT_STRING: case VT_BSTR: case VT_URL: case VT_DECIMAL:
			if (from.str==NULL) break;
			ll=from.length+(from.type==VT_BSTR?0:1);
			if ((v.bstr=(byte*)ma->malloc(ll))==NULL) {to.setError(from.property); return RC_NORESOURCES;}
			memcpy((byte*)v.bstr,from.bstr,to.length=from.length);
			if (from.type==VT_STRING||from.type==VT_URL) const_cast<char*>(v.str)[from.length]=0;
			to.bstr=v.bstr; to.flags=ma->getAType(); break;
		case VT_COLLECTION:
			if (from.nav!=NULL && (ma->getAType()!=SES_HEAP || (v.nav=from.nav->clone())==NULL))
				{to.setError(from.property); return RC_NORESOURCES;}
			to.nav=v.nav; to.flags=ma->getAType(); break;
		case VT_STRUCT:
			//???
		case VT_ARRAY:
			assert(from.varray!=NULL && from.length>0);
			if ((v.varray=(Value*)ma->malloc(from.length*sizeof(Value)))==NULL) {to.setError(from.property); return RC_NORESOURCES;}
			for (i=0; i<from.length; i++) {
				if ((rc=copyV(from.varray[i],const_cast<Value&>(v.varray[i]),ma))!=RC_OK) {
					for (ulong j=0; j<i; j++) freeV(const_cast<Value&>(v.varray[j]));
					ma->free((Value*)v.varray); to.setError(from.property); return rc;
				}
			}
			to.varray=v.varray; to.flags=ma->getAType(); break;
		case VT_RANGE:
			assert(from.range!=NULL && from.length==2);
			v.range=(Value*)ma->malloc(2*sizeof(Value));
			if (v.range==NULL) {to.setError(from.property); return RC_NORESOURCES;}
			if ((rc=copyV(from.range[0],v.range[0],ma))!=RC_OK)
				{ma->free((Value*)v.range); to.setError(from.property); return rc;}
			if ((rc=copyV(from.range[1],v.range[1],ma))!=RC_OK) 
				{freeV(const_cast<Value&>(v.range[0])); ma->free((Value*)v.range); to.setError(from.property); return rc;}
			to.range=v.range; to.flags=ma->getAType(); break;
		case VT_REFIDPROP: case VT_REFIDELT:
			v.refId=(RefVID*)ma->malloc(sizeof(RefVID));
			if (v.refId==NULL) {to.setError(from.property); return RC_NORESOURCES;}
			*const_cast<RefVID*>(v.refId)=*from.refId; to.refId=v.refId; to.flags=ma->getAType(); break;
		case VT_STMT:
			if (from.stmt!=NULL && (to.stmt=((Stmt*)from.stmt)->clone(STMT_OP_ALL,ma,false))==NULL)
				{to.setError(from.property); return RC_NORESOURCES;}
			to.flags=ma->getAType(); break;
		case VT_EXPR:
			if (from.expr!=NULL && (to.expr=Expr::clone((Expr*)from.expr,ma))==NULL)
				{to.setError(from.property); return RC_NORESOURCES;}
			to.flags|=ma->getAType(); break;
		case VT_EXPRTREE:
			if (from.exprt!=NULL && (to.exprt=((ExprTree*)from.exprt)->clone())==NULL)
				{to.setError(from.property); return RC_NORESOURCES;}
			to.flags=ma->getAType(); break;
		case VT_STREAM:
			to.stream.prefix=NULL;
			if (from.stream.is!=NULL && (ma->getAType()!=SES_HEAP || (to.stream.is=from.stream.is->clone())==NULL))
				{to.setError(from.property); return RC_NORESOURCES;}
			to.flags=ma->getAType(); break;
		}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in copyV(...)\n"); return RC_INTERNAL;}
}

RC MVStoreKernel::copyV(const Value *from,ulong nv,Value *&to,MemAlloc *ma)
{
	if (ma==NULL) ma=Session::getSession();
	if (from==NULL||nv==0) to=NULL;
	else if ((to=new(ma) Value[nv])==NULL) return RC_NORESOURCES;
	else for (ulong i=0; i<nv; i++) {
		RC rc=copyV(from[i],to[i],ma);
		if (rc!=RC_OK) {while (i--!=0) freeV(to[i]); ma->free(to); return rc;}
	}
	return RC_OK;
}

bool MVStoreKernel::operator==(const Value& lhs, const Value& rhs)
{
	ulong i;
	if (lhs.property!=rhs.property || lhs.eid!=rhs.eid || lhs.op!=rhs.op || lhs.meta!=rhs.meta) return false;		// flags???
	if (lhs.type!=rhs.type) switch (lhs.type) {
	default: return false;
	case VT_REF: return rhs.type==VT_REFID && lhs.pin->getPID()==rhs.id;
	case VT_REFID: return rhs.type==VT_REF && lhs.id==rhs.pin->getPID();
		//...
	}
	if (lhs.length!=rhs.length) return false;
	switch (lhs.type) {
	default: break;
	case VT_STRING: case VT_BSTR: case VT_URL: case VT_DECIMAL: return memcmp(lhs.bstr,rhs.bstr,lhs.length)==0;
	case VT_INT: case VT_UINT: case VT_URIID: case VT_IDENTITY: return lhs.ui==rhs.ui;
	case VT_INT64: case VT_UINT64: case VT_DATETIME: case VT_INTERVAL: return lhs.ui64==rhs.ui64;
	case VT_FLOAT: return lhs.f==rhs.f && lhs.qval.units==rhs.qval.units;
	case VT_DOUBLE: return lhs.d==rhs.d && lhs.qval.units==rhs.qval.units;
	case VT_BOOL: return lhs.b==rhs.b;
	case VT_REF: return lhs.pin->getPID()==rhs.pin->getPID();
	case VT_REFID: return lhs.id==rhs.id;
	case VT_REFPROP: return lhs.ref.pin->getPID()==rhs.ref.pin->getPID() && lhs.ref.pid==rhs.ref.pid && lhs.ref.vid==rhs.ref.vid;
	case VT_REFIDPROP: return lhs.refId->id==rhs.refId->id && lhs.refId->pid==rhs.refId->pid && lhs.refId->vid==rhs.refId->vid;
	case VT_REFELT: return lhs.ref.pin->getPID()==rhs.ref.pin->getPID() && lhs.ref.pid==rhs.ref.pid && lhs.ref.vid==rhs.ref.vid && lhs.ref.eid==rhs.ref.eid;
	case VT_REFIDELT: return lhs.refId->id==rhs.refId->id && lhs.refId->pid==rhs.refId->pid && lhs.refId->vid==rhs.refId->vid && lhs.refId->eid==rhs.refId->eid;
	case VT_STREAM: return lhs.stream.is==rhs.stream.is;
	case VT_EXPR: return false;		// ???
	case VT_STMT: return false;		// ???
	case VT_ARRAY:
		for (i=0; i<lhs.length; i++) if (lhs.varray[i]!=rhs.varray[i]) return false;
		break;
	case VT_COLLECTION:
		if (lhs.nav->count()!=rhs.nav->count()) return false;
		// ???
		return false;
	case VT_STRUCT:
		//???
		break;
	case VT_EXPRTREE: return *(ExprTree*)lhs.exprt==*(ExprTree*)rhs.exprt;
	case VT_PARAM: case VT_VARREF:
		return lhs.refPath.refN==rhs.refPath.refN && lhs.refPath.type==rhs.refPath.type && 
			(lhs.type==VT_PARAM || lhs.length==rhs.length && lhs.eid==rhs.eid && (lhs.length==0 || lhs.refPath.id==rhs.refPath.id));
		break;
	case VT_RANGE: return lhs.range[0]==rhs.range[0] && lhs.range[1]==rhs.range[1];
	}
	return true;
}

size_t MVStoreKernel::serSize(const PID& id)
{
	uint32_t pg=uint32_t(id.pid>>16); uint16_t idx=uint16_t(id.pid),st=uint16_t(id.pid>>48);
	return mv_len32(pg)+mv_len16(idx)+mv_len16(st)+mv_len32(id.ident);
}

size_t MVStoreKernel::serSize(const Value& v,bool full)
{
	size_t l=1; uint32_t i; uint64_t u64; const Value *pv;
	switch (v.type) {
	default: l=0; break;
	case VT_ERROR: break;
	case VT_STREAM: u64=v.stream.is->length(); l+=1+mv_len64(u64)+(size_t)u64; break;
	case VT_STRING: case VT_URL: l+=mv_len32(v.length+1)+v.length+1; break;
	case VT_BSTR: l+=mv_len32(v.length)+v.length; break;
	case VT_INT: i=mv_enc32zz(v.i); l+=mv_len32(i); break;
	case VT_UINT: case VT_URIID: case VT_IDENTITY: l+=mv_len32(v.ui); break;
	case VT_INT64: case VT_INTERVAL: u64=mv_enc64zz(v.i64); l+=mv_len64(u64); break;
	case VT_UINT64: case VT_DATETIME: l+=mv_len64(v.ui64); break;
	case VT_FLOAT: l=2+sizeof(float); break;
	case VT_DOUBLE: l=2+sizeof(double); break;
	case VT_BOOL: case VT_CURRENT: l=2; break;
	case VT_REF: l+=serSize(v.pin->getPID()); break;
	case VT_REFID: l+=serSize(v.id); break;
	case VT_REFPROP: l+=serSize(v.ref.pin->getPID())+mv_len32(v.ref.pid); break;
	case VT_REFIDPROP: l+=serSize(v.refId->id)+mv_len32(v.refId->pid); break;
	case VT_REFELT: l+=serSize(v.ref.pin->getPID())+mv_len32(v.ref.pid)+mv_len32(v.ref.eid); break;
	case VT_REFIDELT: l+=serSize(v.refId->id)+mv_len32(v.refId->pid)+mv_len32(v.refId->eid); break;
	case VT_RANGE: l+=serSize(v.range[0])+serSize(v.range[1]); break;
	case VT_PARAM: case VT_VARREF:
		l=5; if (v.length!=0) {l+=mv_len32(v.refPath.id); if (v.eid!=STORE_COLLECTION_ID) l+=mv_len32(v.eid);}
		break;
	case VT_ARRAY:
		l+=mv_len32(v.length);
		for (i=0; i<v.length; i++) l+=mv_len32(v.varray[i].eid)+serSize(v.varray[i]);
		break;
	case VT_COLLECTION:
		l=v.nav->count(); l=1+mv_len32(l);
		for (pv=v.nav->navigate(GO_FIRST); pv!=NULL; pv=v.nav->navigate(GO_NEXT)) l+=mv_len32(pv->eid)+serSize(*pv);
		break;
	case VT_STRUCT:
		//???
		break;
	case VT_EXPR: l+=((Expr*)v.expr)->serSize(); break;
	case VT_STMT: l=((Stmt*)v.stmt)->serSize(); l+=1+mv_len32(l); break;

	case VT_EXPRTREE:
	case VT_ENUM:
	case VT_DECIMAL:
		return 0;		// niy
	}
	if (full) {i=mv_enc32zz(v.eid); l+=2+mv_len32(i)+mv_len32(v.property);}
	return l;
}

byte *MVStoreKernel::serialize(const PID& id,byte *buf)
{
	uint32_t pg=uint32_t(id.pid>>16); uint16_t idx=uint16_t(id.pid),st=uint16_t(id.pid>>48);
	mv_enc32(buf,pg); mv_enc16(buf,idx); mv_enc16(buf,st); mv_enc32(buf,id.ident); return buf;
}

byte *MVStoreKernel::serialize(const Value& v,byte *buf,bool full)
{
	*buf++=v.type;
	unsigned i; uint32_t l; const Value *pv; uint64_t u64;
	switch (v.type) {
	default:
	case VT_ERROR: break;
	case VT_STREAM:
		*buf++=v.stream.is->dataType(); u64=v.stream.is->length();
		mv_enc64(buf,u64); buf+=v.stream.is->read(buf,(size_t)u64);
		break;
	case VT_STRING: case VT_URL: case VT_BSTR:
		l=v.type==VT_BSTR?v.length:v.length+1; mv_enc32(buf,l);
		if (v.bstr!=NULL) memcpy(buf,v.bstr,v.length); buf+=v.length;
		if (v.type!=VT_BSTR) *buf++='\0';
		break;
	case VT_INT: l=mv_enc32zz(v.i); mv_enc32(buf,l); break;
	case VT_UINT: case VT_URIID: case VT_IDENTITY: mv_enc32(buf,v.ui); break;
	case VT_INT64: case VT_INTERVAL: u64=mv_enc64zz(v.i64); mv_enc64(buf,u64); break;
	case VT_UINT64: case VT_DATETIME: mv_enc64(buf,v.ui64); break;
	case VT_FLOAT: *buf++=byte(v.qval.units); memcpy(buf,&v.f,sizeof(float)); buf+=sizeof(float); break;
	case VT_DOUBLE: *buf++=byte(v.qval.units); memcpy(buf,&v.d,sizeof(double)); buf+=sizeof(double); break;
	case VT_BOOL: *buf++=v.b; break;
	case VT_RANGE: buf=serialize(v.range[1],serialize(v.range[0],buf)); break;
	case VT_CURRENT: *buf++=byte(v.i); break;
	case VT_REF: buf[-1]=VT_REFID; buf=serialize(v.pin->getPID(),buf); break;
	case VT_REFPROP: buf[-1]=VT_REFIDPROP; buf=serialize(v.ref.pin->getPID(),buf); mv_enc32(buf,v.ref.pid); break;
	case VT_REFELT: buf[-1]=VT_REFIDELT; buf=serialize(v.ref.pin->getPID(),buf); mv_enc32(buf,v.ref.pid); mv_enc32(buf,v.ref.eid); break;
	case VT_REFID: buf=serialize(v.id,buf); break;
	case VT_REFIDPROP: buf=serialize(v.refId->id,buf); mv_enc32(buf,v.refId->pid); break;
	case VT_REFIDELT:  buf=serialize(v.refId->id,buf); mv_enc32(buf,v.refId->pid); mv_enc32(buf,v.refId->eid); break;
	case VT_PARAM: case VT_VARREF:
		*buf++=v.refPath.refN; *buf++=v.length; *buf++=v.refPath.type; 
		*buf++=byte(v.refPath.flags)|(v.eid!=STORE_COLLECTION_ID?0x80:0);
		if (v.length!=0) {mv_enc32(buf,v.refPath.id); if (v.eid!=STORE_COLLECTION_ID) mv_enc32(buf,v.eid);}
		break;
	case VT_ARRAY:
		mv_enc32(buf,v.length);
		for (i=0; i<v.length; i++) {mv_enc32(buf,v.varray[i].eid); buf=serialize(v.varray[i],buf);}
		break;
	case VT_COLLECTION:
		l=v.nav->count(); mv_enc32(buf,l);
		for (pv=v.nav->navigate(GO_FIRST); pv!=NULL; pv=v.nav->navigate(GO_NEXT)) {mv_enc32(buf,pv->eid); buf=serialize(*pv,buf);}
		break;
	case VT_STRUCT:
		//???
		break;
	case VT_EXPR:
		if (v.expr!=NULL) buf=((Expr*)v.expr)->serialize(buf);
		break;
	case VT_STMT:
		if (v.stmt!=NULL) {l=(uint32_t)((Stmt*)v.stmt)->serSize(); mv_enc32(buf,l); buf=((Stmt*)v.stmt)->serialize(buf);}
		break;
	case VT_EXPRTREE:
	case VT_ENUM:
	case VT_DECIMAL:
		//???
		break;		// niy
	}
	if (full) {buf[0]=v.op; buf[1]=v.meta; buf+=2; l=mv_enc32zz(v.eid); mv_enc32(buf,l); mv_enc32(buf,v.property);}
	return buf;
}

RC MVStoreKernel::deserialize(PID& id,const byte *&buf,const byte *const ebuf)
{
	uint32_t u32; uint16_t u16;
	CHECK_dec32(buf,u32,ebuf); CHECK_dec16(buf,u16,ebuf); id.pid=uint64_t(u32)<<16|u16;
	CHECK_dec16(buf,u16,ebuf); id.setStoreID(u16); CHECK_dec32(buf,id.ident,ebuf); return RC_OK;
}

RC MVStoreKernel::deserialize(Value& val,const byte *&buf,const byte *const ebuf,MemAlloc *ma,bool fInPlace,bool full)
{
	if (buf==ebuf) return RC_CORRUPTED; assert(ma!=NULL);
	uint32_t l,i; uint64_t u64; RefVID *rv; Expr *exp; Stmt *qry; RC rc;
	val.type=(ValueType)*buf++; val.flags=NO_HEAP; val.meta=0;
	val.property=STORE_INVALID_PROPID; val.eid=STORE_COLLECTION_ID;
	switch (val.type) {
	default: return RC_CORRUPTED;
	case VT_ERROR: break;
	case VT_STREAM:
		if (buf+1>ebuf) return RC_CORRUPTED;
		val.type=*buf++; CHECK_dec64(buf,u64,ebuf);
		if (fInPlace) val.bstr=buf;
		else if ((val.bstr=(byte*)ma->malloc((size_t)u64+1))==NULL) return RC_NORESOURCES;
		else {memcpy((byte*)val.bstr,buf,(size_t)u64); val.flags=ma->getAType(); if (val.type!=VT_BSTR) ((byte*)val.bstr)[(size_t)u64]=0;}
		val.length=(uint32_t)u64; buf+=(size_t)u64; break;
	case VT_STRING: case VT_URL: case VT_BSTR:
		CHECK_dec32(buf,l,ebuf); if (buf+l>ebuf) return RC_CORRUPTED;
		if (fInPlace) val.bstr=buf;
		else if ((val.bstr=(byte*)ma->malloc(l))==NULL) return RC_NORESOURCES;
		else {memcpy((byte*)val.bstr,buf,l); val.flags=ma->getAType();}
		val.length=val.type==VT_BSTR?l:l-1; buf+=l; break;
	case VT_INT:
		CHECK_dec32(buf,i,ebuf); val.i=mv_dec32zz(i); val.length=sizeof(int32_t); break;
	case VT_UINT: case VT_URIID: case VT_IDENTITY:
		CHECK_dec32(buf,val.ui,ebuf); val.length=sizeof(uint32_t); break;
	case VT_INT64: case VT_INTERVAL: 
		CHECK_dec64(buf,u64,ebuf); val.i64=mv_dec64zz(u64); val.length=sizeof(int64_t); break;
	case VT_UINT64: case VT_DATETIME:
		CHECK_dec64(buf,val.ui64,ebuf); val.length=sizeof(uint64_t); break;
	case VT_FLOAT:
		if (buf+sizeof(float)+1>ebuf) return RC_CORRUPTED;
		val.qval.units=*buf++; memcpy(&val.f,buf,sizeof(float));
		buf+=val.length=sizeof(float); break;
	case VT_DOUBLE: 
		if (buf+sizeof(double)+1>ebuf) return RC_CORRUPTED; 
		val.qval.units=*buf++; memcpy(&val.d,buf,sizeof(double));
		buf+=val.length=sizeof(double); break;
	case VT_BOOL:
		if (buf>=ebuf) return RC_CORRUPTED; val.b=*buf++!=0; val.length=1; break;
	case VT_REFID:
		if ((rc=deserialize(val.id,buf,ebuf))!=RC_OK) return rc;
		val.length=sizeof(PID); break;
	case VT_REFIDPROP: case VT_REFIDELT:
		rv=(RefVID*)ma->malloc(sizeof(RefVID)); if (rv==NULL) return RC_NORESOURCES;
		val.flags=ma->getAType(); val.refId=rv;
		if ((rc=deserialize(rv->id,buf,ebuf))!=RC_OK) return rc;
		CHECK_dec32(buf,rv->pid,ebuf);
		if (val.type==VT_REFIDELT) CHECK_dec32(buf,rv->eid,ebuf);
		val.length=1; break;
	case VT_PARAM: case VT_VARREF:
		if (ebuf-buf<4) return RC_CORRUPTED;
		val.refPath.refN=buf[0]; val.length=buf[1]; val.refPath.type=buf[2]; val.refPath.flags=buf[3]; buf+=4;
		if (val.length!=0) {
			if (val.length>1) return RC_CORRUPTED;
			CHECK_dec32(buf,val.refPath.id,ebuf);
			if ((val.refPath.flags&0x80)!=0) {val.refPath.flags&=0x7f; CHECK_dec32(buf,val.eid,ebuf);}
		}
		break;
	case VT_CURRENT:
		if (buf>=ebuf) return RC_CORRUPTED; val.i=*buf++; val.length=1; break;
	case VT_RANGE:
		if ((val.range=(Value*)ma->malloc(sizeof(Value)*2))==NULL) return RC_NORESOURCES;
		val.length=2; val.flags=ma->getAType();
		if ((rc=deserialize(*(Value*)&val.range[0],buf,ebuf,ma,fInPlace))!=RC_OK ||
			(rc=deserialize(*(Value*)&val.range[1],buf,ebuf,ma,fInPlace))!=RC_OK) return rc;
		break;
	case VT_ARRAY:
		CHECK_dec32(buf,val.length,ebuf);
		if (val.length==0) val.varray=NULL;
		else if ((val.varray=(Value*)ma->malloc(sizeof(Value)*val.length))==NULL) return RC_NORESOURCES;
		else for (i=0; i<val.length; i++) {
			uint32_t eid; CHECK_dec32(buf,eid,ebuf);
			if ((rc=deserialize(*(Value*)&val.varray[i],buf,ebuf,ma,fInPlace))!=RC_OK) return rc;
			((Value*)&val.varray[i])->eid=eid;
		}
		val.flags=ma->getAType(); break;
	case VT_STRUCT:
		//???
		break;
	case VT_EXPR:
		if ((rc=Expr::deserialize(exp,buf,ebuf,ma))!=RC_OK) return rc;
		val.expr=exp; val.flags=ma->getAType(); val.length=1; break;
	case VT_STMT:
		CHECK_dec32(buf,val.length,ebuf);
		if ((rc=Stmt::deserialize(qry,buf,buf+val.length,ma))!=RC_OK) return rc;
		val.stmt=qry; val.flags=ma->getAType(); break;
	case VT_ENUM:
	case VT_DECIMAL:
		return RC_CORRUPTED;	// niy
	}
	if (full) {
		if (buf+4>ebuf) return RC_CORRUPTED; val.op=buf[0]; val.meta=buf[1]; buf+=2;
		CHECK_dec32(buf,i,ebuf); val.eid=mv_dec32zz(i); CHECK_dec32(buf,val.property,ebuf);
	}
	return RC_OK;
}

RC MVStoreKernel::streamToValue(IStream *stream,Value& val,MemAlloc *ma)
{
	try {
		if (ma==NULL && (ma=Session::getSession())==NULL && (ma=StoreCtx::get())==NULL) return RC_NOSESSION;
		val.type=stream->dataType(); val.flags=ma->getAType();
		byte buf[256],*p; size_t l=stream->read(buf,sizeof(buf)),xl=1024,extra=val.type==VT_BSTR?0:1;
		if ((p=(byte*)ma->malloc(l>=sizeof(buf)?xl:l+extra))==NULL) return RC_NORESOURCES;
		memcpy(p,buf,l);
		if (l>=sizeof(buf)) {
			while ((l+=stream->read(p+l,xl-l))>=xl) if ((p=(byte*)ma->realloc(p,xl+=xl/2))==NULL) return RC_NORESOURCES;
			if (l+extra!=xl && (p=(byte*)ma->realloc(p,l+extra))==NULL) return RC_NORESOURCES;
		}
		if (val.type==VT_STRING) p[l]=0; val.length=(uint32_t)l; val.bstr=p; return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {return RC_INVPARAM;}
}

RC MVStoreKernel::convV(const Value& src,Value& dst,ValueType type)
{
	int l; char buf[256],*p; Value w; RC rc; TIMESTAMP ts; int64_t itv; URI *uri; Identity *ident;
	for (const Value *ps=&src;;) {if (ps->type==type) {
noconv:
		if (ps!=&dst) {
			MemAlloc *ma=Session::getSession(); if (ma==NULL) return RC_NOSESSION;
			if ((rc=copyV(*ps,dst,ma))!=RC_OK) {dst.setError(src.property); return rc;}
		}
	} else if (isRef((ValueType)ps->type) && type!=VT_URL && !isRef(type)) {
		if ((rc=derefValue(*ps,dst,Session::getSession()))!=RC_OK) return rc;
		ps=&dst; continue;
	} else {
		if (ps!=&dst) {dst.eid=ps->eid; dst.flags=NO_HEAP;}
		switch (type) {
		default: return RC_TYPE;
		case VT_STRING:
			switch (ps->type) {
			default: return RC_TYPE;
			case VT_URL: goto noconv;
			case VT_INT:
				if ((l=sprintf(buf,"%d",ps->i))<0) return RC_INTERNAL;
				if ((p=(char*)malloc(l+1,SES_HEAP))==NULL) return RC_NORESOURCES;
				memcpy(p,buf,l+1); dst.set(p,(unsigned long)l); dst.flags=SES_HEAP; break;
			case VT_UINT:
				if ((l=sprintf(buf,"%u",ps->ui))<0) return RC_INTERNAL;
				if ((p=(char*)malloc(l+1,SES_HEAP))==NULL) return RC_NORESOURCES;
				memcpy(p,buf,l+1); dst.set(p,(unsigned long)l); dst.flags=SES_HEAP; break;
			case VT_INT64:
				if ((l=sprintf(buf,_LD_FM,ps->i64))<0) return RC_INTERNAL;
				if ((p=(char*)malloc(l+1,SES_HEAP))==NULL) return RC_NORESOURCES;
				memcpy(p,buf,l+1); dst.set(p,(unsigned long)l); dst.flags=SES_HEAP; break;
			case VT_UINT64:
				if ((l=sprintf(buf,_LU_FM,ps->ui64))<0) return RC_INTERNAL;
				if ((p=(char*)malloc(l+1,SES_HEAP))==NULL) return RC_NORESOURCES;
				memcpy(p,buf,l+1); dst.set(p,(unsigned long)l); dst.flags=SES_HEAP; break;
			case VT_FLOAT:
				if ((l=sprintf(buf,"%g",ps->f))<0) return RC_INTERNAL;
				if ((p=(char*)malloc(l+1,SES_HEAP))==NULL) return RC_NORESOURCES;
				memcpy(p,buf,l+1); dst.set(p,(unsigned long)l); dst.flags=SES_HEAP; break;
			case VT_DOUBLE:
				if ((l=sprintf(buf,"%g",ps->d))<0) return RC_INTERNAL;
				if ((p=(char*)malloc(l+1,SES_HEAP))==NULL) return RC_NORESOURCES;
				memcpy(p,buf,l+1); dst.set(p,(unsigned long)l); dst.flags=SES_HEAP; break;
			case VT_BOOL: 
				if (ps->b) dst.set("true",4); else dst.set("false",5); break;
			case VT_STREAM:
				if ((rc=streamToValue(ps->stream.is,w,Session::getSession()))!=RC_OK) return rc;
				if (w.type==VT_BSTR) {freeV(w); return RC_TYPE;}
				if (ps==&dst) freeV(dst); dst=w;
				if (dst.type!=type) {ps=&dst; continue;}
				break;
			case VT_CURRENT:
				getTimestamp(ts); goto dt_to_str;	//???
			case VT_DATETIME:
				ts=ps->ui64;
			dt_to_str:
				if ((rc=convDateTime(Session::getSession(),ts,buf,l))!=RC_OK) return rc;
				if ((p=(char*)malloc(l+1,SES_HEAP))==NULL) return RC_NORESOURCES;
				memcpy(p,buf,l+1); dst.set(p,(unsigned long)l); dst.flags=SES_HEAP; break;
			case VT_INTERVAL:
				if ((rc=convInterval(ps->i64,buf,l))!=RC_OK) return rc;
				if ((p=(char*)malloc(l+1,SES_HEAP))==NULL) return RC_NORESOURCES;
				memcpy(p,buf,l+1); dst.set(p,(unsigned long)l); dst.flags=SES_HEAP; break;
				break;
			case VT_URIID:
				if (ps->uid==STORE_INVALID_PROPID) dst.set("",0);
				else if ((uri=(URI*)StoreCtx::get()->uriMgr->ObjMgr::find(ps->uid))==NULL) return RC_NOTFOUND;
				else {dst.set(strdup(uri->getName(),Session::getSession())); dst.flags=SES_HEAP; uri->release();}
				break;
			case VT_IDENTITY:
				if (ps->iid==STORE_INVALID_IDENTITY) dst.set("",0);
				else if ((ident=(Identity*)StoreCtx::get()->identMgr->ObjMgr::find(ps->iid))==NULL) return RC_NOTFOUND;
				else {dst.set(strdup(ident->getName(),Session::getSession())); dst.flags=SES_HEAP; ident->release();}
				break;
			case VT_STMT:
				if ((p=ps->stmt->toString())==NULL) return RC_NORESOURCES;
				if (&dst==ps && ps->flags!=NO_HEAP) freeV(dst);
				dst.set(p); dst.flags=SES_HEAP; break;
			case VT_EXPR:
				if ((p=ps->expr->toString())==NULL) return RC_NORESOURCES;
				if (&dst==ps && ps->flags!=NO_HEAP) freeV(dst);
				dst.set(p); dst.flags=SES_HEAP; break;
			case VT_ENUM:
			case VT_DECIMAL:
			case VT_ARRAY:
			case VT_COLLECTION:
			case VT_STRUCT:
				// ???
				return RC_TYPE;
			}
			break;
		case VT_URL:
			switch (ps->type) {
			default: return RC_TYPE;
			case VT_STRING: 
				// check format
				break;
			case VT_REF:
			case VT_REFID:
			case VT_REFPROP:
			case VT_REFELT:
			case VT_REFIDPROP:
			case VT_REFIDELT:
				// ???
				return RC_INTERNAL;
			}
			break;
		case VT_BSTR:
			switch (ps->type) {
			default: return RC_TYPE;
			case VT_STRING: case VT_URL: case VT_BSTR: goto noconv;
			case VT_INT: case VT_UINT: case VT_INT64: case VT_UINT64: case VT_FLOAT: case VT_DOUBLE:
			case VT_BOOL: case VT_DATETIME: case VT_INTERVAL:
				if ((p=(char*)malloc(ps->length,SES_HEAP))==NULL) return RC_NORESOURCES;
				memcpy(p,&ps->i,ps->length); dst.set((unsigned char*)p,ps->length); dst.flags=SES_HEAP; break;
			case VT_STREAM:
				if ((rc=streamToValue(ps->stream.is,w,Session::getSession()))!=RC_OK) return rc;
				if (ps==&dst) freeV(dst); dst=w; break;
			case VT_CURRENT:		//????
				if ((p=(char*)malloc(sizeof(TIMESTAMP),SES_HEAP))==NULL) return RC_NORESOURCES;
				getTimestamp(*(TIMESTAMP*)p); dst.set((unsigned char*)p,sizeof(TIMESTAMP)); dst.flags=SES_HEAP; break;
			case VT_ENUM:
			case VT_DECIMAL:
				return RC_INTERNAL;
			case VT_STRUCT:
				//???
				return RC_INTERNAL;
			}
			break;
		case VT_INT:
			switch (ps->type) {
			default: return RC_TYPE;
			case VT_UINT: goto noconv;
			case VT_STRING: 
				if ((rc=strToNum(ps->str,ps->length,w))!=RC_OK) return rc;
				if (&dst==ps && ps->flags!=NO_HEAP) freeV(dst);
				if (w.type!=VT_INT&&w.type!=VT_UINT) {ps=&w; continue;}
				dst=w; break;
			case VT_INT64: dst.i=int32_t(ps->i64); dst.length=sizeof(int32_t); break;
			case VT_UINT64: dst.i=int32_t(ps->ui64); dst.length=sizeof(int32_t); break;
			case VT_FLOAT: dst.i=int32_t(ps->f); dst.length=sizeof(int32_t); break;
			case VT_DOUBLE: dst.i=int32_t(ps->d); dst.length=sizeof(int32_t); break;
			}
			break;
		case VT_UINT:
			switch (ps->type) {
			default: return RC_TYPE;
			case VT_INT: goto noconv;
			case VT_STRING:
				if ((rc=strToNum(ps->str,ps->length,w))!=RC_OK) return rc;
				if (&dst==ps && ps->flags!=NO_HEAP) freeV(dst);
				if (w.type!=VT_INT&&w.type!=VT_UINT) {ps=&w; continue;}
				dst=w; break;
			case VT_INT64: dst.ui=uint32_t(ps->i64); dst.length=sizeof(uint32_t); break;
			case VT_UINT64: dst.ui=uint32_t(ps->ui64); dst.length=sizeof(uint32_t); break;
			case VT_FLOAT: dst.ui=uint32_t(ps->f); dst.length=sizeof(uint32_t); break;
			case VT_DOUBLE: dst.ui=uint32_t(ps->d); dst.length=sizeof(uint32_t); break;
			}
			break;
		case VT_INT64:
			switch (ps->type) {
			default: return RC_TYPE;
			case VT_UINT64: goto noconv;
			case VT_STRING:
				if ((rc=strToNum(ps->str,ps->length,w))!=RC_OK) return rc;
				if (&dst==ps && ps->flags!=NO_HEAP) freeV(dst);
				if (w.type!=VT_INT64&&w.type!=VT_UINT64) {ps=&w; continue;}
				dst=w; break;
			case VT_INT: dst.i64=int64_t(ps->i); dst.length=sizeof(int64_t); break;
			case VT_UINT: dst.i64=int64_t(ps->ui); dst.length=sizeof(int64_t); break;
			case VT_FLOAT: dst.i64=int64_t(ps->f); dst.length=sizeof(int64_t); break;
			case VT_DOUBLE: dst.i64=int64_t(ps->d); dst.length=sizeof(int64_t); break;
			}
			break;
		case VT_UINT64:
			switch (ps->type) {
			default: return RC_TYPE;
			case VT_INT64: goto noconv;
			case VT_STRING:
				if ((rc=strToNum(ps->str,ps->length,w))!=RC_OK) return rc;
				if (&dst==ps && ps->flags!=NO_HEAP) freeV(dst);
				if (w.type!=VT_INT64&&w.type!=VT_UINT64) {ps=&w; continue;}
				dst=w; break;
			case VT_INT: dst.ui64=uint64_t(ps->i); dst.length=sizeof(uint64_t); break;
			case VT_UINT: dst.ui64=uint64_t(ps->ui); dst.length=sizeof(uint64_t); break;
			case VT_FLOAT: dst.ui64=uint64_t(ps->f); dst.length=sizeof(uint64_t); break;
			case VT_DOUBLE: dst.ui64=uint64_t(ps->d); dst.length=sizeof(uint64_t); break;
			}
			break;
		case VT_FLOAT:
			dst.qval.units=Un_NDIM;
			switch (ps->type) {
			default: return RC_TYPE;
			case VT_STRING:
				if ((rc=strToNum(ps->str,ps->length,w))!=RC_OK) return rc;
				if (&dst==ps && ps->flags!=NO_HEAP) freeV(dst);
				if (w.type!=VT_FLOAT) {ps=&w; continue;}
				dst=w; break;
			case VT_INT: dst.f=float(ps->i); dst.length=sizeof(float); break;
			case VT_UINT: dst.f=float(ps->ui); dst.length=sizeof(float); break;
			case VT_INT64: dst.f=float(ps->i64); dst.length=sizeof(float); break;
			case VT_UINT64: dst.f=float(ps->ui64); dst.length=sizeof(float); break;
			case VT_DOUBLE: dst.f=float(ps->d); dst.qval.units=ps->qval.units; dst.length=sizeof(float); break;
			}
			break;
		case VT_DOUBLE:
			dst.qval.units=Un_NDIM;
			switch (ps->type) {
			default: return RC_TYPE;
			case VT_STRING:
				if ((rc=strToNum(ps->str,ps->length,w))!=RC_OK) return rc;
				if (&dst==ps && ps->flags!=NO_HEAP) freeV(dst);
				if (w.type!=VT_DOUBLE) {ps=&w; continue;}
				dst=w; break;
			case VT_INT: dst.d=double(ps->i); dst.length=sizeof(double); break;
			case VT_UINT: dst.d=double(ps->ui); dst.length=sizeof(double); break;
			case VT_INT64: dst.d=double(ps->i64); dst.length=sizeof(double); break;
			case VT_UINT64: dst.d=double(ps->ui64); dst.length=sizeof(double); break;
			case VT_FLOAT: dst.d=double(ps->f); dst.qval.units=ps->qval.units; dst.length=sizeof(double); break;
			}
			break;
		case VT_REF:
			switch (ps->type) {
			default: return RC_TYPE;
			case VT_REF: goto noconv;
			//case VT_URL:
				// ...
			//	if (&dst==ps && ps->flags!=NO_HEAP) freeV(dst);
				// ...
			//	break;
			//case VT_REFID:
				// ???
			//	break;
			//case VT_STRUCT:
			}
			break;
		case VT_REFID:
			switch (ps->type) {
			default: return RC_TYPE;
			case VT_REFID: goto noconv;
			//case VT_URL:
				// ...
			//	if (&dst==ps && ps->flags!=NO_HEAP) freeV(dst);
				// ...
			//	break;
			case VT_REF:
				w.id=ps->pin->getPID(); if (&dst==ps) freeV(dst);
				dst.id=w.id; dst.flags=NO_HEAP; break;
			//case VT_STRUCT:
			}
			break;
		case VT_BOOL:
			if (ps->type!=VT_STRING) return RC_TYPE;
			if (ps->length==4 && cmpncase(ps->str,"TRUE",4)==0) w.b=true;
			else if (ps->length==5 && cmpncase(ps->str,"FALSE",5)==0) w.b=false;
			else return RC_TYPE;
			if (&dst==ps && ps->flags!=NO_HEAP) freeV(dst);
			dst.b=w.b; dst.length=1; dst.flags=NO_HEAP; break;
			break;
		case VT_DATETIME:
			switch (ps->type) {
			default: return RC_TYPE;
			case VT_UINT64: goto noconv;
			case VT_STRING:
				if ((rc=strToTimestamp(ps->str,ps->length,ts))!=RC_OK) return rc;
				if (&dst==ps && ps->flags!=NO_HEAP) freeV(dst);
				dst.ui64=ts; break;
			case VT_CURRENT:
				getTimestamp(ts); dst.ui64=ts; break;
			}
			break;
		case VT_INTERVAL:
			switch (ps->type) {
			default: return RC_TYPE;
			case VT_INT64: goto noconv;
			case VT_STRING:
				if ((rc=strToInterval(ps->str,ps->length,itv))!=RC_OK) return rc;
				if (&dst==ps && ps->flags!=NO_HEAP) freeV(dst);
				dst.i64=itv; break;
			}
			break;
		case VT_URIID:
			if (ps->type!=VT_STRING && ps->type!=VT_URL) return RC_TYPE;
			uri=StoreCtx::get()->uriMgr->insert(ps->str);
			if (&dst==ps && ps->flags!=NO_HEAP) freeV(dst);
			dst.setURIID(uri!=NULL?uri->getID():STORE_INVALID_PROPID);
			if (uri!=NULL) uri->release(); break;
		case VT_IDENTITY:
			if (ps->type!=VT_STRING) return RC_TYPE;
			ident=(Identity*)StoreCtx::get()->identMgr->find(ps->str);
			if (&dst==ps && ps->flags!=NO_HEAP) freeV(dst);
			dst.setIdentity(ident!=NULL?ident->getID():STORE_INVALID_IDENTITY);
			if (ident!=NULL) ident->release(); break;
		case VT_ARRAY:
			switch (ps->type) {
			default: return RC_TYPE;
			case VT_STRING:
				// ...
			//	if (&dst==ps && ps->flags!=NO_HEAP) freeV(dst);
				// ...
			//	break;
				return RC_TYPE;
			//case VT_COLLECTION:
				// ???
			//	break;
			}
			break;
		case VT_COLLECTION:
			switch (ps->type) {
			default: return RC_TYPE;
			case VT_STRING:
				// ...
			//	if (&dst==ps && ps->flags!=NO_HEAP) freeV(dst);
				// ...
			//	break;
				return RC_TYPE;
			//case VT_ARRAY:
				// ...
			//	break;
			}
			break;
		case VT_STRUCT:
			switch (ps->type) {
			default: return RC_TYPE;
			case VT_STRING:
				// ...
			//	if (&dst==ps && ps->flags!=NO_HEAP) freeV(dst);
				// ...
			//	break;
				return RC_TYPE;
			}
			break;
		case VT_STMT:
			if (ps->type==VT_STRING) {
				Session *ses=Session::getSession(); RC rc=RC_OK;
				SInCtx in(ses,ps->str,ps->length,NULL,0,(Session::getSession()->getItf()&ITF_SPARQL)!=0?SQ_SPARQL:SQ_SQL);
				try {Stmt *st=in.parse(); if (&src==&dst) freeV(dst); dst.set(st); dst.flags=SES_HEAP; return RC_OK;}
				catch (SynErr) {rc=RC_SYNTAX;} catch (RC rc2) {rc=rc2;}
				return rc;
			}
			return RC_TYPE;
		case VT_EXPR:
			if (ps->type==VT_STRING) {
				Session *ses=Session::getSession();
				SInCtx in(ses,ps->str,ps->length,NULL,0); Expr *pe=NULL;
				try {
					ExprTree *et=in.parse(false); in.checkEnd(); rc=Expr::compile(et,pe,ses); et->destroy(); 
					if (rc!=RC_OK) return rc; if (&src==&dst) freeV(dst);
					dst.set(pe); dst.flags=SES_HEAP; return RC_OK;
				} catch (SynErr) {return RC_SYNTAX;} catch (RC rc) {return rc;}
			}
			return RC_TYPE;
		}
	}
	dst.type=type;
	return RC_OK;}
}

RC MVStoreKernel::derefValue(const Value &src,Value &dst,Session *ses)
{
	PIN *pin; const RefVID *refId; 
	RC rc=RC_OK; HEAP_TYPE save=(HEAP_TYPE)(src.flags&HEAP_TYPE_MASK);
	switch (src.type) {
	default: return RC_TYPE;
	case VT_REF:
		pin=(PIN*)src.pin; 
		if ((rc=pin->getPINValue(dst,ses))==RC_OK && &src==&dst && save!=NO_HEAP) pin->destroy();
		break;
	case VT_REFID:
		rc=ses!=NULL?ses->getStore()->queryMgr->getPINValue(src.id,dst,0,ses):RC_NOSESSION; 	break;
	case VT_REFPROP:
	case VT_REFELT:
		pin=(PIN*)src.ref.pin;
		rc=ses!=NULL?ses->getStore()->queryMgr->loadValue(ses,pin->getPID(),src.ref.pid,src.type==VT_REFELT?src.ref.eid:STORE_COLLECTION_ID,dst,0):RC_NOSESSION;
		if (rc==RC_OK && &src==&dst && save!=NO_HEAP) pin->destroy();
		break;
	case VT_REFIDPROP:
	case VT_REFIDELT:
		refId=src.refId;
		rc=ses!=NULL?ses->getStore()->queryMgr->loadValue(ses,refId->id,refId->pid,src.type==VT_REFIDELT?refId->eid:STORE_COLLECTION_ID,dst,0):RC_NOSESSION;
		if (rc==RC_OK && &src==&dst && save!=NO_HEAP) if (save==SES_HEAP) ses->free((void*)refId); else free((void*)refId,save);
		break;
	}
	return rc;
}

RC MVStoreKernel::convURL(const Value& src,Value& dst,HEAP_TYPE alloc)
{
	switch (src.type) {
	default: return RC_TYPE;
	case VT_URL:
		//...
		break;
	case VT_REF:
		//...
		break;
	case VT_REFID:
		//...
		break;
	case VT_REFPROP:
		// ...
		break;
	case VT_REFELT:
		// ...
		break;
	case VT_REFIDPROP:
		// ...
		break;
	case VT_REFIDELT:
		// ...
		break;
	}
	return RC_OK;
}

void MVStoreKernel::freeV(Value *v,ulong nv,MemAlloc *ma)
{
	for (ulong i=0; i<nv; i++) freeV(v[i]);
	ma->free(v);
}

void MVStoreKernel::freeV0(Value& v)
{
	try {
		HEAP_TYPE allc=(HEAP_TYPE)(v.flags&HEAP_TYPE_MASK); assert(allc!=NO_HEAP);
		switch (v.type) {
		default: break;
		case VT_STRING: case VT_BSTR: case VT_URL:
			if (v.length!=0 || (void*)v.str!=(void*)&v.length) free((char*)v.str,allc); break;
		case VT_REFIDPROP: case VT_REFIDELT: free((RefVID*)v.refId,allc); break;
		case VT_ARRAY:
			if (v.varray!=NULL) {
				for (ulong i=0; i<v.length; i++) freeV(const_cast<Value&>(v.varray[i]));
				free(const_cast<Value*>(v.varray),allc);
			}
			break;
		case VT_RANGE:
			if (v.range!=NULL) {
				assert(v.length==2);
				freeV(v.range[0]); freeV(v.range[1]); free(v.range,allc);
			}
			break;
		case VT_EXPRTREE: delete (ExprTree*)v.exprt; break;
		case VT_STREAM: if (v.stream.is!=NULL) v.stream.is->destroy(); break;
		case VT_STMT: if (v.stmt!=NULL) v.stmt->destroy(); break;
		case VT_COLLECTION: if (v.nav!=NULL) v.nav->destroy(); break;
		case VT_STRUCT:
			//????
			break;
		case VT_EXPR: free(v.expr,allc); break;
		}
	}  catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in freeV(...)\n");}
}
