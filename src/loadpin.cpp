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

#include "queryprc.h"
#include "maps.h"
#include "lock.h"
#include "stmt.h"
#include "expr.h"
#include "blob.h"

using namespace AfyKernel;

RC PINx::loadPIN(PIN *&pin,unsigned md,VersionID vid)
{
	RC rc=RC_OK; pin=NULL;
	if (pb.isNull() && (rc=getBody(TVO_READ,(md&MODE_DELETED)!=0?GB_DELETED:0,vid))!=RC_OK) return rc;		// tvo?

	if ((pin=new(ses) PIN(ses,mode))==NULL) return RC_NOMEM;
	*pin=id; *pin=addr; pin->meta=meta;

	if ((rc=loadProps(md|LOAD_SSV))==RC_OK) {		// load props directly to pin or copy from this
		assert(fSSV==0);
		if (fNoFree==0) {pin->properties=properties; pin->nProperties=nProperties; properties=NULL; nProperties=0; fPartial=1;}
		else if ((rc=copyV(properties,nProperties,pin->properties,pin->ma))==RC_OK) {pin->nProperties=nProperties; pin->fNoFree=0;}
	}
	return rc;
}

RC PINx::loadProps(unsigned md,const PropertyID *pids,unsigned nPids)
{
	assert(!pb.isNull() && hpin!=NULL);
	RC rc=RC_OK; unsigned nProps=hpin->nProps; const DataSS *dss=NULL; fPartial=0;
//	if (!ses->inWriteTx() && tv!=NULL && (!tv->fCommited || (dss=tv->stack)!=NULL && dss->fNew)) return RC_DELETED;
	if ((nProperties=nProps)!=0) {
		if (pids!=NULL) {if (nPids==0 || pids[0]==PROP_SPEC_ANY) pids=NULL; else if (nPids!=0 && nPids<nProperties) {nProperties=nPids; fPartial=1;}}
		if ((properties=(Value*)ses->realloc(properties,nProperties*sizeof(Value)))==NULL) return RC_NOMEM;
		const HeapPageMgr::HeapV *hprop=hpin->getPropTab(),*const hend=hprop+hpin->nProps,*hpr; fSSV=0;
		for (unsigned i=0; i<nProperties; ++i,++hprop) {
			const PropertyID pid=pids!=NULL?pids[i]&STORE_MAX_URIID:hprop->getPropID();
			if (dss) {
				if (pids==NULL) {
					// check if there are properties>=prev(i>0) && <pid
				}
				//bool fFound=false;
				for (const DataSS *ds=dss; ds!=NULL; ds=ds->nextSS) {
					// try to find pid
					// found -> fFound=true
					// if (fDelta && collection) {
					//	  if (!loaded && hprop valid) -> loadHV
					//	  apply delta
					//  } else {
					//	  remember old value
					//  }
					// if 
				}
			}
			if (pids!=NULL) {
				if (hprop>=hend || (hpr=hprop)->getPropID()!=pid && (hpr=hpin->findProperty(pid))==NULL) {
					//skip
				}
				if (hpr!=hprop) {hprop=hpr; fPartial=1;}
			}
			if ((rc=loadVH(properties[i],hprop,md&~LOAD_CARDINALITY,ma))!=RC_OK) break;
			if ((properties[i].flags&VF_SSV)!=0) fSSV=1;
		}
		if (rc==RC_OK && (md&LOAD_SSV)!=0 && fSSV!=0 && (rc=loadSSVs(properties,nProperties,md,ses,ma))==RC_OK) fSSV=0;
	}
	return rc;
}

RC QueryPrc::getClassInfo(Session *ses,PIN *pin)
{
	const Value *cv; Class *cls=NULL; uint64_t nPINs=0; RC rc=RC_OK; Value vv,*pv;
	if (pin==NULL || (pin->meta&PMT_CLASS)==0 || (cv=pin->findProperty(PROP_SPEC_OBJID))==NULL || cv->type!=VT_URIID) return RC_CORRUPTED;
	const ClassID clsid=cv->uid;
	if (rc!=RC_OK || (rc=ctx->classMgr->getClassInfo(clsid,cls,nPINs))!=RC_OK) return rc;
	assert(cls!=NULL); ClassIndex *ci; Stmt *qry; QVar *qv; PropListP plp(ses);
	if ((qry=cls->getQuery())!=NULL && (qv=qry->getTop())!=NULL && qv->mergeProps(plp)==RC_OK && plp.pls!=NULL && plp.nPls!=0) {
		Value *va=new(ses) Value[plp.pls[0].nProps];
		if (va==NULL) rc=RC_NOMEM;
		else {
			ElementID prefix=ctx->getPrefix();
			for (unsigned i=0; i<plp.pls[0].nProps; i++) {va[i].setURIID(plp.pls[0].props[i]&STORE_MAX_URIID); va[i].eid=prefix+i;}
			vv.set(va,plp.pls[0].nProps); vv.setPropID(PROP_SPEC_PROPERTIES);
			if ((rc=BIN<Value,PropertyID,ValCmp,uint32_t>::insert(pin->properties,pin->nProperties,vv.property,vv,(MemAlloc*)ses))!=RC_OK) ses->free(va);
		}
	}
	if (rc==RC_OK && (cls->getFlags()&META_PROP_INDEXED)!=0) {
		if ((ci=cls->getIndex())!=NULL) {
			if (pin->findProperty(PROP_SPEC_INDEX_INFO)!=NULL) rc=RC_CORRUPTED;
			else {
				const unsigned nFields=ci->getNSegs(); const IndexSeg *is=ci->getIndexSegs();
				if ((pv=new(ses) Value[nFields])==NULL) rc=RC_NOMEM;
				else {
					ElementID prefix=ctx->getPrefix();
					for (unsigned i=0; i<nFields; i++) {pv[i].setU64(0ULL); pv[i].iseg=*is++; pv[i].eid=prefix+i;}
					vv.set(pv,nFields); vv.setPropID(PROP_SPEC_INDEX_INFO);
					rc=BIN<Value,PropertyID,ValCmp,uint32_t>::insert(pin->properties,pin->nProperties,vv.property,vv,(MemAlloc*)ses);
				}
			}
		}
		if (rc==RC_OK) {
			vv.setU64(nPINs); vv.setPropID(PROP_SPEC_COUNT);
			rc=BIN<Value,PropertyID,ValCmp,uint32_t>::insert(pin->properties,pin->nProperties,vv.property,vv,(MemAlloc*)ses);
		}
	}
	cls->release(); return rc;
}

RC QueryPrc::loadValue(Session *ses,const PID& id,PropertyID propID,ElementID eid,Value& res,unsigned md)
{
	PINx cb(ses,id); RC rc; return (rc=cb.getBody())!=RC_OK?rc:cb.getV(propID,res,md|LOAD_SSV,ses,eid);
}

RC PINx::loadSSVs(Value *values,unsigned nValues,unsigned md,Session *ses,MemAlloc *ma)
{
	for (unsigned i=0; i<nValues; ++i) {
		Value& v=values[i]; RC rc;
		if ((v.flags&VF_SSV)!=0) switch (v.type) {
		case VT_COLLECTION:
			if (!v.isNav()) {
		case VT_STRUCT:
				if ((rc=loadSSVs(const_cast<Value*>(v.varray),v.length,md,ses,ma))!=RC_OK) return rc;
				v.flags&=~VF_SSV; break;
			}
		default:
			const HRefSSV *href=(const HRefSSV *)&v.id; assert(v.type==VT_STREAM||v.type==VT_STRUCT||v.type==VT_RANGE); StoreCtx *ctx=ses->getStore();
			PBlockP pb(ctx->bufMgr->getPage(href->pageID,ctx->ssvMgr,0,NULL,ses),0); if (pb.isNull()) {v.setError(v.property); return RC_NOTFOUND;}
			const HeapPageMgr::HeapPage *hp = (const HeapPageMgr::HeapPage *)pb->getPageBuf(); 
			if ((rc=loadSSV(v,href->type.getType(),hp->getObject(hp->getOffset(href->idx)),md,ma))!=RC_OK) return rc;
			for (unsigned j=i+1; j<nValues; j++) if ((values[j].flags&VF_SSV)!=0) {
				if (values[j].type==VT_COLLECTION || values[j].type==VT_STRUCT) {
					// ???
				} else {
					href=(const HRefSSV *)&values[j].id; assert(values[j].type==VT_STREAM);
					if (href->pageID==pb->getPageID() &&  (rc=loadSSV(values[j],href->type.getType(),
						hp->getObject(hp->getOffset(href->idx)),md,ma))!=RC_OK) {v.setError(v.property); return rc;}
				}
			}
			break;
		}
	}
	return RC_OK;
}

RC PINx::loadSSV(Value& v,ValueType ty,const HeapPageMgr::HeapObjHeader *hobj,unsigned md,MemAlloc *ma)
{
	assert((v.flags&VF_SSV)!=0 && v.type==VT_STREAM && ma!=NULL);
	if (hobj==NULL || (hobj->descr&HOH_DELETED)!=0) {v.setError(v.property); return RC_NOTFOUND;}
	size_t l=v.length=hobj->length-sizeof(HeapPageMgr::HeapObjHeader); byte *ps;
	v.flags=ma->getAType(); v.type=ty; Expr *exp; Stmt *qry; const byte *p; RC rc;
	if (hobj->getType()==HO_SSVALUE) switch (ty) {
	default: v.setError(v.property); return RC_CORRUPTED;
	case VT_STRING: l++;
	case VT_BSTR:
		if ((ps=(byte*)ma->malloc(l))==NULL) {v.setError(v.property); return RC_NOMEM;} 
		memcpy(ps,hobj+1,v.length); v.bstr=ps;
		if (ty==VT_STRING) ps[v.length]=0;
		break;
	case VT_EXPR:
		{p=(const byte*)(hobj+1); ExprHdr ehdr(0,0,0,0,0,0); memcpy(&ehdr,p,sizeof(ExprHdr));
		if ((rc=Expr::deserialize(exp,p,p+ehdr.lExpr,ma))!=RC_OK) return rc;
		v.expr=exp; v.type=VT_EXPR; v.length=1; break;}
	case VT_STMT:
		p=(byte*)(hobj+1); afy_dec16(p,l);
		if ((rc=Stmt::deserialize(qry,p,p+l,ma))!=RC_OK) return rc;
		v.stmt=qry; v.type=VT_STMT; v.length=1; break;
	} else if (hobj->getType()!=HO_BLOB) {
		report(MSG_ERROR,"loadSSV: property %08X isn't a blob\n",v.property);
		v.setError(v.property); return RC_CORRUPTED;
	} else {
		if (ty!=VT_STMT||ty!=VT_EXPR) {v.setError(v.property); return RC_CORRUPTED;}
		// niy
		v.setError(v.property); return RC_INTERNAL;
	}
	return RC_OK;
}

RC PINx::loadMapElt(Value& v,const HeapPageMgr::HeapV *hprop,unsigned md,const Value& idx,MemAlloc *ma) const
{
	assert(hprop->type.getType()==VT_MAP);
	if (hprop->type.getFormat()==HDF_LONG) {
		// extract by idx
	} else {
		// find idx
	}
	if ((md&LOAD_CARDINALITY)!=0) {v.set(0u); return RC_OK;} else {v.setError(hprop->getPropID()); return RC_NOTFOUND;}
}

RC PINx::loadVH(Value& v,const HeapPageMgr::HeapV *hprop,unsigned md,MemAlloc *ma,ElementID eid) const
{
	HType ty=hprop->type; ushort offset=hprop->offset; const PropertyID pid=hprop->getID(); PID id; Value val; RC rc;
	if (ty.isArray()) {
		if (ty.getFormat()==HDF_LONG) {
			//???
		} else {
			const HeapPageMgr::HeapArray *ha=(const HeapPageMgr::HeapArray*)(pb->getPageBuf()+offset);
			if (eid==STORE_COLLECTION_ID) {
				v.setArray(ha->data(),ha->nelts,ha->xdim,ha->ydim,(ValueType)ha->type,(Units)ha->units);
				v.fa.start=ha->start; v.fa.flags=ha->flags; if (ma!=NULL && (rc=copyV(v,v,ma))!=RC_OK) return rc;
			} else {
				const void *pd=(eid>>16)<ha->ydim && (uint16_t)eid<ha->xdim?(byte*)ha->data()+ha->lelt*((uint16_t)eid+(eid>>16)*ha->xdim):
																		ha->ydim==1 && eid<ha->nelts?(byte*)ha->data()+ha->lelt*eid:NULL;
				if (pd==NULL) {v.setError(pid); return RC_NOTFOUND;}
				switch (ha->type) {
				default: return RC_TYPE;
				case VT_INT: v.set(*(int*)pd); break;
				case VT_UINT: v.set(*(unsigned*)pd); break;
				case VT_INT64: case VT_INTERVAL: v.setI64(*(int64_t*)pd); break;
				case VT_UINT64: case VT_DATETIME: v.setU64(*(uint64_t*)pd); break;
				case VT_FLOAT: v.set(*(float*)pd,(Units)ha->units); break;
				case VT_DOUBLE: v.set(*(double*)pd,(Units)ha->units); break;
				case VT_REFID: v.set(*(PID*)pd); break;
				}
			}
			v.property=pid; v.meta=hprop->type.flags; return RC_OK;
		}
	} else if (!ty.isCompound()) {
		if (eid!=STORE_COLLECTION_ID && eid!=STORE_FIRST_ELEMENT && eid!=(hpin!=NULL && hpin->hasRemoteID() && 
			hpin->getAddr(id) && (hprop->type.flags&META_PROP_LOCAL)==0?HeapPageMgr::getPrefix(id):isRemote(id)?
									HeapPageMgr::getPrefix(id):ses->getStore()->getPrefix())) return RC_NOTFOUND;
	} else if (ty.getType()==VT_MAP && eid!=STORE_COLLECTION_ID) {
		val.set((unsigned)eid); return loadMapElt(v,hprop,md,val,ma);
	} else for (;;eid=STORE_COLLECTION_ID) {
		if (ty.getFormat()==HDF_LONG) {
			const HeapPageMgr::HeapExtCollection *coll;
			switch (ty.getType()) {
			case VT_COLLECTION:
				coll=(const HeapPageMgr::HeapExtCollection*)(pb->getPageBuf()+hprop->offset);
				if (eid!=STORE_COLLECTION_ID) {
					Navigator nav(addr,hprop->getID(),coll,md,ma);
					if ((rc=nav.getElementByID(eid,val))!=RC_OK) {
						if ((md&LOAD_CARDINALITY)!=0) {v.set(0u); v.property=pid; return RC_OK;}
						v.setError(pid); return rc;
					}
					if ((md&LOAD_CARDINALITY)!=0) v.set(1u); else {v=val;	v.eid=eid;} // fGlobal -> copyV
				} else {
					Navigator *nav;
					if ((md&LOAD_CARDINALITY)!=0) v.set(~0u);
					else if (ma==NULL && (ma=ses)==NULL) return RC_NOSESSION;
					else if ((nav=new(ma) Navigator(addr,hprop->getID(),coll,md,ma))==NULL) v.setError(pid);
					else {
						ma->addObj(nav); v.nav=nav; v.flags=ma->getAType(); v.type=VT_COLLECTION;
						v.eid=STORE_COLLECTION_ID; v.length=~0u; v.meta=ty.flags; v.op=OP_SET;
					}
				}
				break;
			case VT_MAP:
				//???
			case VT_RANGE:
			case VT_STRUCT:
				// loadS()
				return RC_INTERNAL;
			default: return RC_CORRUPTED;
			}
			v.property=pid; return RC_OK;
		}
		const HeapPageMgr::HeapVV *coll=(const HeapPageMgr::HeapVV*)(pb->getPageBuf()+offset); const ValueType vt=ty.getType();
		if (eid==STORE_COLLECTION_ID) { 
			if ((md&LOAD_CARDINALITY)!=0) {v.set(unsigned(vt==VT_MAP?coll->cnt/2:coll->cnt)); v.property=pid; return RC_OK;}
			if (vt!=VT_COLLECTION || ty.getFormat()!=HDF_SHORT || coll->cnt>1) {
				MemAlloc *al=ma!=NULL?ma:(MemAlloc*)ses; if (al==NULL) return RC_NOSESSION;
				v.type=vt; v.length=coll->cnt; v.eid=STORE_COLLECTION_ID; v.meta=ty.flags; v.op=OP_SET;
				v.varray=(Value*)al->malloc(v.length*sizeof(Value)); unsigned i,j; rc=RC_OK; uint8_t flg=0;
				if (v.varray==NULL) {v.type=VT_ERROR; v.property=pid; return RC_NOMEM;}
				for (i=j=0; i<v.length; i++) {
					const HeapPageMgr::HeapV *elt=&coll->start[i];
					if ((md&LOAD_REF)==0 || elt->type.getType()==VT_REFID || elt->type.getType()==VT_STRUCT /*&& ??? */) {
						const uint32_t id=elt->getID();
						if ((rc=elt->type.isCompound()?loadVH(const_cast<Value&>(v.varray[j]),elt,md,ma):
							ses->getStore()->queryMgr->loadS(const_cast<Value&>(v.varray[j]),elt->type,elt->offset,(HeapPageMgr::HeapPage*)pb->getPageBuf(),md,ma))!=RC_OK) break;
						const_cast<Value&>(v.varray[j]).property=vt==VT_STRUCT?id:STORE_INVALID_URIID; if (vt==VT_COLLECTION) const_cast<Value&>(v.varray[j]).eid=id;
						flg|=v.varray[j].flags&VF_SSV; j++;
					}
				}
				if (rc==RC_OK && (flg&VF_SSV)!=0 && (md&LOAD_SSV)!=0 && (rc=loadSSVs(const_cast<Value*>(v.varray),j,md,ses,al))==RC_OK) flg=0;
				if (rc==RC_OK && (md&LOAD_REF)!=0) {if ((v.length=j)==0) {al->free((void*)v.varray); v.setError();}}
				else if (j<v.length||rc!=RC_OK) {
					report(MSG_ERROR,"loadVH: cannot load compound type %d, prop %08X, %08X:%04X\n",vt,pid,pb->getPageID(),addr.idx);
					while (j--!=0) freeV(const_cast<Value&>(v.varray[j]));
					al->free(const_cast<Value*>(v.varray)); v.setError(pid);
					return rc==RC_OK?RC_CORRUPTED:rc;
				}
				if (vt==VT_MAP) {
					MemMap *mm=new(al) MemMap((MapElt*)v.varray,v.length/2,al);
					if (mm==NULL) {freeV((Value*)v.varray,v.length,al); v.type=VT_ERROR; v.property=pid; return RC_NOMEM;}
					v.type=VT_MAP; v.map=mm; v.length=1;
				}
				v.flags=al->getAType()|flg; v.property=pid; return RC_OK;
			}
			eid=STORE_FIRST_ELEMENT;
		}
		const HeapPageMgr::HeapV *elt=NULL;
		switch (vt) {
		default: assert(0);
		case VT_RANGE: break;
		case VT_COLLECTION: case VT_STRUCT: elt=coll->findElt(eid); break;
		}
		if ((md&LOAD_CARDINALITY)!=0) {v.set(elt!=NULL?1u:0u); v.property=pid; return RC_OK;}
		if (elt==NULL) {v.setError(pid); return RC_NOTFOUND;}
		offset=elt->offset; ty=elt->type; if (vt==VT_COLLECTION) eid=elt->getID(); if (!ty.isCompound()) break;
	}
	if (eid==STORE_COLLECTION_ID||eid==STORE_FIRST_ELEMENT)
		eid=hpin->hasRemoteID() && (hprop->type.flags&META_PROP_LOCAL)==0 && hpin->getAddr(id)?HeapPageMgr::getPrefix(id):ses->getStore()->getPrefix();
	if ((md&LOAD_REF)!=0 && ty.getType()!=VT_REFID && ty.getType()!=VT_STRUCT /* || ???*/) {v.setError(pid); return RC_OK;}
	rc=ses->getStore()->queryMgr->loadS(v,ty,offset,(HeapPageMgr::HeapPage*)pb->getPageBuf(),md,ma,eid); v.property=pid; return rc;
}

RC QueryPrc::loadS(Value& v,HType vt,PageOff offset,const HeapPageMgr::HeapPage *hp,unsigned mode,MemAlloc *ma,unsigned eid)
{
	ValueType ty=vt.getType(); 
	if (ty==VT_ERROR || ty>=VT_ALL) {
		if ((mode&LOAD_CARDINALITY)!=0) {v.set(0u); return RC_OK;}
		if (ty!=VT_ERROR) report(MSG_ERROR,"Invalid type 0x%04X, page: %08X\n",ty,hp->hdr.pageID);
		v.type=VT_ERROR; v.flags=0; v.length=0; v.meta=0; return RC_CORRUPTED;
	}
	if ((mode&LOAD_CARDINALITY)!=0) {v.set(1u); return RC_OK;}
	const byte *pData=(byte*)hp+offset; const HeapDataFmt fmt=vt.getFormat();
	v.flags=NO_HEAP; v.fcalc=0; v.type=ty; PageAddr addr; RC rc;
	uint64_t l64; StreamX *pstr; Expr *exp; Stmt *qry; size_t l;
	switch (ty) {
	default: break;
	case VT_BSTR:
		switch (fmt) {
		default: return RC_CORRUPTED;
		case HDF_COMPACT: v.length=0; pData=(byte*)&v.length; break;
		case HDF_SHORT: v.length=*pData++; break;
		case HDF_NORMAL: v.length=pData[0]<<8|pData[1]; pData+=2; break;
		}
		if (ma==NULL||v.length==0) {v.bstr=pData; setHT(v,PAGE_HEAP);}
		else {
			byte *p=(byte*)ma->malloc(v.length); if (p==NULL) return RC_NOMEM; 
			memcpy(p,pData,v.length); v.bstr=p; v.flags=ma->getAType();
		}
		break;
	case VT_STRING:
		switch (fmt) {
		default: return RC_CORRUPTED;
		case HDF_COMPACT:
			v.length=0;
			v.bstr=(byte*)&v.length; v.eid=eid; v.op=OP_SET; v.meta=vt.flags; return RC_OK;
		case HDF_SHORT: v.length=*pData++; break;
		case HDF_NORMAL: v.length=pData[0]<<8|pData[1]; pData+=2; break;
		}
		if (ma==NULL) {v.str=(const char*)pData; setHT(v,PAGE_HEAP);}
		else {
			char *p=(char*)ma->malloc(v.length+1); if (p==NULL) return RC_NOMEM; 
			memcpy(p,pData,v.length); p[v.length]=0; v.str=p; v.flags=ma->getAType();
		}
		break;
	case VT_INT:
		v.i=fmt==HDF_COMPACT?(short)offset:__una_get(*(int32_t*)pData); v.length=sizeof(int32_t); break;
	case VT_UINT:
		v.ui=fmt==HDF_COMPACT?(ushort)offset:__una_get(*(uint32_t*)pData); v.length=sizeof(uint32_t); break;
	case VT_INT64: case VT_INTERVAL:
		v.i64=fmt==HDF_COMPACT?(short)offset:__una_get(*(int64_t*)pData); v.length=sizeof(int64_t); break;
	case VT_UINT64: case VT_DATETIME:
		v.ui64=fmt==HDF_COMPACT?(ushort)offset:__una_get(*(uint64_t*)pData); v.length=sizeof(uint64_t); break;
	case VT_ENUM:
		v.enu.enumid=__una_get(((uint32_t*)pData)[0]); v.enu.eltid=__una_get(((uint32_t*)pData)[1]); v.length=sizeof(VEnum); break;
	case VT_FLOAT:
		v.f=__una_get(*(float*)pData);v.length=sizeof(float);
		v.qval.units=fmt==HDF_LONG?__una_get(*(uint16_t*)(pData+sizeof(float))):(uint16_t)Un_NDIM; break;
	case VT_DOUBLE:
		v.d=__una_get(*(double*)pData); v.length=sizeof(double); 
		v.qval.units=fmt==HDF_LONG?__una_get(*(uint16_t*)(pData+sizeof(double))):(uint16_t)Un_NDIM; break;
	case VT_BOOL:
		v.b=offset!=0; v.length=1; break;
	case VT_URIID:
		v.uid=fmt==HDF_COMPACT?(ushort)offset:__una_get(*(URIID*)pData); v.length=sizeof(URIID); break;
	case VT_IDENTITY:
		v.iid=fmt==HDF_COMPACT?(ushort)offset:__una_get(*(IdentityID*)pData); v.length=sizeof(IdentityID); break;
	case VT_REFID: case VT_REFIDPROP: case VT_REFIDELT:
		hp->getRef(v.id,fmt,offset);
		if (ty==VT_REFID) v.length=sizeof(PID);
		else {
			if (ma==NULL && (ma=Session::getSession())==NULL && (ma=StoreCtx::get())==NULL) return RC_NOSESSION;
			RefVID *rv=new(ma) RefVID; if (rv==NULL) return RC_NOMEM;
			rv->id=v.id; rv->pid=__una_get(*(PropertyID*)(pData+HeapPageMgr::refLength[fmt]));
			if (ty!=VT_REFIDELT) rv->eid=STORE_COLLECTION_ID;
			else rv->eid=__una_get(*(ElementID*)(pData+HeapPageMgr::refLength[fmt]+sizeof(PropertyID)));
			v.set(*rv); v.flags=ma->getAType();
		}
		break;
	case VT_STREAM:
		switch (fmt) {
		default:
			memcpy(&v.id,pData,v.length=sizeof(HRefSSV)); v.flags|=VF_SSV; 
			if ((mode&LOAD_SSV)!=0) {
				Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION;
				if ((rc=PINx::loadSSVs(&v,1,mode,ses,ma!=NULL?ma:ses))!=RC_OK) return rc;
			}
			break;
		case HDF_COMPACT:
			v.length=0; v.bstr=(byte*)&v.length; v.type=VT_STRING; break;
		case HDF_LONG:
			addr.pageID=__una_get(((HLOB*)pData)->ref.pageID); addr.idx=__una_get(((HLOB*)pData)->ref.idx); l64=__una_get(((HLOB*)pData)->len);
			if (ma==NULL && (ma=Session::getSession())==NULL && (ma=StoreCtx::get())==NULL) return RC_NOSESSION;
			if ((pstr=new(ma) StreamX(addr,l64,ty=((HLOB*)pData)->ref.type.getType(),ma))==NULL) return RC_NOMEM;
			switch (ty) {
			default: return RC_CORRUPTED;
			case VT_STRING: case VT_BSTR:
				v.set(pstr); setHT(v,ma->getAType()); ma->addObj(pstr); break;
			case VT_STMT: case VT_EXPR:
				rc=streamToValue(pstr,v,ma); pstr->destroy(); if (rc!=RC_OK) return rc;
				break;
			}
			break;
		}
		break;
	case VT_EXPR:
		if (ma==NULL && (ma=Session::getSession())==NULL && (ma=StoreCtx::get())==NULL) return RC_NOSESSION;
		{ExprHdr ehdr(0,0,0,0,0,0); memcpy(&ehdr,pData,sizeof(ExprHdr));
		if ((rc=Expr::deserialize(exp,pData,pData+ehdr.lExpr,ma))!=RC_OK) return rc;
		v.set(exp); v.flags=ma->getAType(); break;}
	case VT_STMT:
		if (ma==NULL && (ma=Session::getSession())==NULL && (ma=StoreCtx::get())==NULL) return RC_NOSESSION;
		afy_dec16(pData,l); if ((rc=Stmt::deserialize(qry,pData,pData+l,ma))!=RC_OK) return rc;
		v.set(qry); v.flags=ma->getAType(); break;
	case VT_CURRENT: v.i=offset; break;
	case VT_VARREF: memcpy(&v.refV,pData,sizeof(RefV)); break;
	}
	v.eid=eid; v.meta=vt.flags; v.op=OP_SET; return RC_OK;
}

RC QueryPrc::loadData(const PageAddr &addr,byte *&p,size_t &len,MemAlloc *ma)
{
	PBlockP pb(ctx->bufMgr->getPage(addr.pageID,ctx->ssvMgr),0); if (pb.isNull()) return RC_NOTFOUND;
	const HeapPageMgr::HeapPage *hp = (const HeapPageMgr::HeapPage *)pb->getPageBuf();
	const HeapPageMgr::HeapObjHeader *hobj=hp->getObject(hp->getOffset(addr.idx));
	if (hobj==NULL || (hobj->descr&HOH_DELETED)!=0) return RC_NOTFOUND;
	len=hobj->length-sizeof(HeapPageMgr::HeapObjHeader);
	if (hobj->getType()==HO_SSVALUE) {
		if ((p=(byte*)ma->malloc(len))==NULL) return RC_NOMEM;
		memcpy(p,hobj+1,len);
	} else if (hobj->getType()!=HO_BLOB) {
		report(MSG_ERROR,"loadData: not a blob, page: %08X, slot: %d\n",addr.pageID,addr.idx);
		return RC_CORRUPTED;
	} else {
		// niy
		return RC_INTERNAL;
	}
	return RC_OK;
}
