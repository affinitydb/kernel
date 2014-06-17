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

#include "expr.h"
#include "queryprc.h"
#include "stmt.h"
#include "ftindex.h"
#include "parser.h"
#include "service.h"
#include "regexp.h"
#include "blob.h"
#include "maps.h"
#include "crypt.h"
#include <math.h>

using namespace AfyKernel;

const byte Expr::compareCodeTab[] = {2,5,1,3,4,6};

static const byte cndAct[8][2] = {{0,1},{1,0},{0,2},{2,0},{2,1},{1,2},{2,3},{3,2}};

RC Expr::execute(Value& res,const Value *params,unsigned nParams) const
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION;
		if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		res.setEmpty(); Values vv(params,nParams);
		RC rc=eval(res,EvalCtx(ses,NULL,0,NULL,0,&vv,1));
		switch (rc) {
		default: return rc;
		case RC_TRUE: res.set(true); return RC_OK;
		case RC_FALSE: res.set(false); return RC_OK;
		}
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IExpr::execute()\n"); return RC_INTERNAL;}
}

bool Expr::condSatisfied(const EvalCtx& ctx) const
{
	try {
		Value res; if (this==NULL) return true;
		switch (eval(res,ctx)) {
		case RC_TRUE: return true;
		case RC_FALSE: return false;
		case RC_OK: if (res.type==VT_BOOL) return res.b;
		default: break;
		}
	} catch (...) {}
	return false;
}

static int __cdecl cmpProps(const void *v1, const void *v2)
{
	return cmp3(*(const PropertyID*)v1,*(const PropertyID*)v2);
}

const static ExprOp aggops[] = {OP_MAX, OP_MIN, OP_CONCAT, OP_AVG, OP_SUM};

namespace AfyKernel
{
class OnRelease : public LatchHolder
{
	const EvalCtx&	ectx;
	Value	*const	stack;
	Value	**const	pTop;
	bool			fCheck;
public:
	OnRelease(const EvalCtx& ct,Value *stk,Value **pT) : LatchHolder(ct.ses),ectx(ct),stack(stk),pTop(pT),fCheck(false) {}
	void releaseLatches(PageID,PageMgr*,bool) {
		// ectx?
		if (fCheck) {
			for (Value *pv=stack; pv<*pTop; pv++) if ((pv->flags&HEAP_TYPE_MASK)==PAGE_HEAP) copyV(*pv,*pv,ectx.ma);
			fCheck=false;
		}
	}
	void checkNotHeld(PBlock*) {}
	void setCheck() {fCheck=true;}
};
}
						
RC Expr::eval(Value& result,const EvalCtx& ctx) const
{
	if ((hdr.flags&EXPR_EXTN)!=0) {
		const void *itf=NULL;
		if (hdr.nStack>=nExtns || (itf=extnTab[hdr.nStack])==NULL) return RC_INVOP;
		// call external
		return RC_INTERNAL;
	}
	if ((hdr.flags&EXPR_PARAMS)!=0 && (ctx.params==NULL||ctx.nParams==0||ctx.params[0].nValues==0) && ctx.ect==ECT_DETECT) return RC_TRUE;
	Value *stack=(Value*)alloca((hdr.nStack+hdr.nSubx)*sizeof(Value)),*top=stack+hdr.nSubx; if (stack==NULL) return RC_NOMEM;
	const byte *codePtr=(const byte*)(&hdr+1)+hdr.nProps*sizeof(uint32_t),*codeEnd=(const byte*)&hdr+hdr.lExpr;
	uint64_t subXmask=0; unsigned cntCatch=(hdr.flags&EXPR_BOOL)!=0?1:0; PINx px(ctx.ses); RExpCtx rctx(ctx.ma);
	for (OnRelease onr(ctx,stack,&top); codePtr<codeEnd;) {
		assert(top>=stack && top<=stack+hdr.nStack+hdr.nSubx);
		TIMESTAMP ts; const Value *v; RC rc=RC_OK; Value w; const PIN *pin; ElementID eid; long num; const PropInfo *pi;
		byte op=*codePtr++; const bool ff=(op&0x80)!=0; int nops; unsigned fop; uint32_t u,vdx; PathSeg *path;
		switch (op&=0x7F) {
		case OP_CON:
			assert(top<stack+hdr.nStack+hdr.nSubx); top->flags=0;
			if ((rc=AfyKernel::deserialize(*top,codePtr,codeEnd,ctx.ma,true))!=RC_OK) break;
			assert(top->type!=VT_VARREF && top->type!=VT_CURRENT);
			if (top->fcalc!=0) switch (top->type) {
			default: break;
			case VT_STMT: case VT_EXPR: case VT_CURRENT: rc=ctx.ses->getStore()->queryMgr->eval(top,ctx,top); break;
			case VT_REFIDPROP: case VT_REFIDELT:
				if ((rc=derefValue(*top,*top,ctx.ses))==RC_NOTFOUND && cntCatch!=0) {freeV(*top); rc=RC_OK;}
				break;
			}
			top++; break;
		case OP_PARAM:
			if ((u=*codePtr++)!=0xFF) vdx=u>>6,u&=0x3F; else {vdx=codePtr[0]; u=codePtr[1]; codePtr+=2;}
			if (vdx>=ctx.nParams || u>=ctx.params[vdx].nValues) rc=RC_NOTFOUND;
			else {
				v=&ctx.params[vdx].vals[u];
				if (v->type==VT_VARREF) {
					//u=v->refV.refN; if (v->length==0) goto var; propID=v->refV.id; eid.eid=v->eid; goto prop;
				}
			}
		set_param:
			if (ff) top->set(unsigned(rc!=RC_OK?(rc=RC_OK,0u):v->count()));
			else if (rc==RC_OK) {*top=*v; setHT(*top);} else {top->setError(); if (cntCatch!=0) rc=RC_OK;}
			top++; break;
		case OP_NAMED_PRM:
			if ((u=*codePtr++)==0xFF) {u=codePtr[0]|codePtr[1]<<8; codePtr+=2;}
			u=((uint32_t*)(&hdr+1))[u]&STORE_MAX_URIID; rc=RC_NOTFOUND;
			if (ctx.nParams>QV_PARAMS) for (unsigned i=0; i<ctx.params[QV_PARAMS].nValues; i++)
				if (ctx.params[QV_PARAMS].vals[i].property==u) {v=&ctx.params[QV_PARAMS].vals[i]; rc=RC_OK; break;}
			goto set_param;
		case OP_VAR: case OP_ENVV:
			u=*codePtr++; pin=op==OP_VAR?u<ctx.nVars?ctx.vars[u]:NULL:u<ctx.nEnv?ctx.env[u]:NULL;
			if (pin==NULL && op==OP_ENVV && u==0 && ctx.nVars==1) pin=ctx.vars[0];
			if (pin!=NULL) {if (ff) top->set(1u); else top->set((IPIN *)pin);} else if (ff) top->set(0u); else {top->setError(); if (cntCatch==0) rc=RC_NOTFOUND;}
			top++; break;
		case OP_PROP: case OP_ELT: case OP_SETPROP: case OP_ENVV_PROP: case OP_ENVV_ELT:
			if ((u=*codePtr++)!=0xff) vdx=u>>6,u&=0x3f; else {vdx=codePtr[0]; u=codePtr[1]|codePtr[2]<<8; codePtr+=3;}
			u=((uint32_t*)(&hdr+1))[u]&STORE_MAX_URIID; eid=STORE_COLLECTION_ID;
			if (op==OP_SETPROP) {top[-1].property=u; if (ff) top[-1].meta=*codePtr++; continue;}
			if (op==OP_ELT||op==OP_ENVV_ELT) {afy_dec32(codePtr,eid); eid=afy_dec32zz(eid);}
			pin=op==OP_PROP||op==OP_ELT?vdx<ctx.nVars?ctx.vars[vdx]:NULL:vdx<ctx.nEnv?ctx.env[vdx]:NULL;
		get_prop:
			if (pin==NULL) rc=RC_NOTFOUND;
			else switch (eid) {
			case STORE_SUM_COLLECTION:
			case STORE_AVG_COLLECTION:
			case STORE_CONCAT_COLLECTION:
			case STORE_MIN_ELEMENT:
			case STORE_MAX_ELEMENT:
				if ((rc=((PIN*)pin)->getV(u,*top,LOAD_SSV,NULL))==RC_OK) rc=calcAgg(aggops[eid-STORE_MAX_ELEMENT],*top,NULL,1,0,ctx); break;
			case STORE_VAR_ELEMENT:
				--top; rc=((PIN*)pin)->getV(u,*top,ff?LOAD_CARDINALITY|LOAD_SSV:LOAD_SSV,*top,NULL); break;
			case STORE_VAR2_ELEMENT:
				top-=2;
				if ((rc=getI(top[0],num,ctx))!=RC_OK) break;
				if ((unsigned)num<0x10000) eid=(uint32_t)num; else {rc=RC_NOTFOUND; break;}
				if ((rc=getI(top[1],num,ctx))!=RC_OK) break;
				if ((unsigned)num<0x10000) eid|=(uint32_t)(num<<16); else {rc=RC_NOTFOUND; break;}
			default:
				rc=((PIN*)pin)->getV(u,*top,ff?LOAD_CARDINALITY|LOAD_SSV:LOAD_SSV,NULL,eid); break;
			}
			if (pin==&px) px.cleanup(); if (rc==RC_NOTFOUND && cntCatch!=0) {top->setError(); rc=RC_OK;}
			if (top->type==VT_EXPR && (((Expr*)top->expr)->hdr.flags&EXPR_PARAMS)==0) {		// params <-> WITH
				Expr *expr=(Expr*)top->expr; const bool fFree=(top->flags&HEAP_TYPE_MASK)>=SES_HEAP;
				if ((rc=expr->eval(*top,ctx))==RC_OK) {if (fFree) ctx.ma->free(expr);}
				else if (cntCatch!=0) {top->setError(); rc=RC_OK;}
			} else if ((top->flags&HEAP_TYPE_MASK)==PAGE_HEAP) onr.setCheck();
			top++; break;
		case OP_NAMED_PROP: case OP_NAMED_ELT:
			if ((u=*codePtr++)==0xFF) {u=codePtr[0]|codePtr[1]<<8; codePtr+=2;}
			if ((rc=ctx.ses->getStore()->namedMgr->getNamed(((uint32_t*)(&hdr+1))[u]&STORE_MAX_URIID,px))!=RC_OK)
				{if (cntCatch!=0 && rc==RC_NOTFOUND) {top->setError(); rc=RC_OK;} break;}
			pin=&px; eid=STORE_COLLECTION_ID;
			if ((u=*codePtr++)==0xFF) {u=codePtr[0]|codePtr[1]<<8; codePtr+=2;}
			if (op==OP_NAMED_ELT) {afy_dec32(codePtr,eid); eid=afy_dec32zz(eid);}
			u=((uint32_t*)(&hdr+1))[u]&STORE_MAX_URIID; goto get_prop;
		case OP_CALL:
			if (ff) {
				if ((u=*codePtr++)==0xFF) {u=codePtr[0]|codePtr[1]<<8; codePtr+=2;} 
				rc=ctx.ses->getStore()->namedMgr->getNamed(((uint32_t*)(&hdr+1))[u]&STORE_MAX_URIID,px);
				if ((u=*codePtr++)==0xFF) {u=codePtr[0]|codePtr[1]<<8; codePtr+=2;}
				u=((uint32_t*)(&hdr+1))[u]&STORE_MAX_URIID; nops=*codePtr++; w.setEmpty();
				if (rc==RC_OK) {rc=px.getV(u,w,LOAD_SSV,(MemAlloc*)ctx.ses); px.cleanup();}
			} else {
				if ((u=*codePtr++)!=0xff) vdx=u>>6,u&=0x3f; else {vdx=codePtr[0]; u=codePtr[1]|codePtr[2]<<8; codePtr+=3;}
				u=((uint32_t*)(&hdr+1))[u]&STORE_MAX_URIID; nops=*codePtr++; w.setEmpty();
				rc=vdx<ctx.nVars&&ctx.vars[vdx]!=NULL?ctx.vars[vdx]->getV(u,w,LOAD_SSV,NULL):RC_NOTFOUND;
				if (rc==RC_NOTFOUND && ctx.nEnv>=2 && ctx.env[1]!=NULL) rc=ctx.env[1]->getV(u,w,ff?LOAD_CARDINALITY|LOAD_SSV:LOAD_SSV,NULL);
				if (rc==RC_NOTFOUND) goto ext_call;
			}
			if (rc==RC_OK) switch (w.type) {
			default: freeV(w); rc=RC_TYPE; break;
			case VT_EXPR: case VT_STMT: case VT_COLLECTION:
				{Values vv(nops!=0?top-nops:NULL,nops); EvalCtx ectx(ctx.ses,ctx.env,ctx.nEnv,ctx.vars,ctx.nVars,&vv,1,&ctx,ctx.ma,ctx.ect);
				rc=ctx.ses->getStore()->queryMgr->eval(&w,ectx,&w); while (--nops>=0) freeV(*--top); if (rc==RC_OK) *top++=w; else freeV(w);}
				break;
			}
			break;
		case OP_EXTCALL:
			if ((u=*codePtr++)==0xFF) {u=codePtr[0]|codePtr[1]<<8; codePtr+=2;} u=((uint32_t*)(&hdr+1))[u]&STORE_MAX_URIID;
		ext_call:
			// try to call extFunc
			break;
		case OP_CONID:
			if ((u=*codePtr++)==0xFF) {u=codePtr[0]|codePtr[1]<<8; codePtr+=2;}
			// check u<hdr.nProps
			u=((uint32_t*)(&hdr+1))[u];
			if (ff) top->setIdentity(u); else top->setURIID(u&STORE_MAX_URIID);		// identity???
			top++; break;
		case OP_RXREF:
			if ((u=*codePtr++)==0xFF) {u=codePtr[0]|codePtr[1]<<8; codePtr+=2;}
			if (ctx.rctx==NULL || ctx.rctx->rxstr==NULL || u>=*ctx.rctx) {if (ff) top->set(0u); else {top->setError(); if (cntCatch==0) rc=RC_NOTFOUND;}}
			else if (ff) top->set(1u); else top->set(ctx.rctx->rxstr+(*ctx.rctx)[u].sht,(uint32_t)(*ctx.rctx)[u].len);		// 0 at the end?
			top++; break;
		case OP_PATH:
			u=*codePtr++; if (u==0) return RC_CORRUPTED; if (ff) {/* fff=*/codePtr++;}
			if ((path=new(ctx.ma) PathSeg[u])==NULL) return RC_NOMEM;
			memset(path,0,u*sizeof(PathSeg)); --top;
			for (nops=(int)u,rc=RC_OK; rc==RC_OK && --nops>=0; ) {
				PathSeg& sg=path[nops]; byte f=*codePtr++; Expr *filter=NULL; unsigned i;
				sg.eid.setEmpty(); sg.cid=STORE_INVALID_URIID; sg.rmin=sg.rmax=1; sg.fLast=(f&0x01)!=0;
				if ((f&0x02)!=0) v=top--; else if ((rc=AfyKernel::deserialize(w,codePtr,codeEnd,ctx.ma,true))==RC_OK) v=&w;
				switch (v->type) {
				default: rc=RC_TYPE; break;
				case VT_ANY: break;
				case VT_URIID: sg.pid=v->uid; sg.nPids=1; break;
				case VT_COLLECTION:
					if (v->isNav() || (sg.pids=new(ctx.ma) PropertyID[v->length])==NULL) rc=RC_NOMEM;
					else for (i=0; i<v->length; i++) if (v->varray[i].type==VT_URIID && (sg.pids[sg.nPids]=v->varray[i].uid)!=PROP_SPEC_ANY) sg.nPids++;
					if (sg.nPids==0) {ctx.ma->free(sg.pids); sg.pid=PROP_SPEC_ANY;} else if (sg.nPids==1) {PropertyID pid=sg.pids[0]; ctx.ma->free(sg.pids); sg.pid=pid;}
					else qsort(sg.pids,sg.nPids,sizeof(PropertyID),cmpProps);
					break;
				}
				freeV(*(Value*)v);
				if (rc==RC_OK) switch (f&0x18) {
				case 0: break;
				case 0x08: afy_dec32(codePtr,sg.eid.eid); sg.eid.eid=afy_dec32zz((uint32_t)sg.eid.eid); break;
				case 0x10: if (top->type!=VT_INT && top->type!=VT_UINT) rc=RC_TYPE; else sg.eid.eid=(top--)->ui; break;	// convert type?
				case 0x18: if ((rc=deserialize(filter,codePtr,codeEnd,ctx.ses))==RC_OK) sg.filter=filter; break; //classID ??
				}
				if (rc==RC_OK) switch (f&0xE0) {
				default: break;
				case 0x20: sg.rmin=0; break;
				case 0x40: sg.rmax=uint16_t(~0u); break;
				case 0x60: sg.rmin=0; sg.rmax=uint16_t(~0u); break;
				case 0x80: afy_dec16(codePtr,sg.rmin); afy_dec16(codePtr,sg.rmax); break;
				case 0xA0:
					if (top->type!=VT_RANGE || top->varray[0].type!=VT_UINT || top->varray[1].type!=VT_UINT || top->varray[0].ui>top->varray[1].ui || top->varray[1].ui==0) rc=RC_INVPARAM;
					else {sg.rmin=uint16_t(top->varray[0].ui); sg.rmax=uint16_t(min(top->varray[1].ui,(uint32_t)0xFFFF));}
					break;
				}
			}
			if (rc==RC_OK) {
				PathIt *pi=new(ctx.ma) PathIt(ctx.ma,path,u,*top,true);		// fff -> fDFS
				if (pi==NULL) rc=RC_NOMEM; else top->set(pi); top++;
			}
			if (rc!=RC_OK) destroyPath(path,u,ctx.ma);
			break;
		case OP_CURRENT:
			switch (*codePtr++) {
			default: rc=RC_CORRUPTED; break;
			case CVT_TIMESTAMP: getTimestamp(ts); top->setDateTime(ts); break;
			case CVT_USER: top->setIdentity(ctx.ses->getIdentity()); break;
			case CVT_STORE: top->set((unsigned)ctx.ses->getStore()->storeID); break;
			}
			top++; break;
		case OP_CATCH:
			if (ff) cntCatch++; else {assert(cntCatch>0); cntCatch--;} break;
		case OP_JUMP:
			u=codePtr[0]|codePtr[1]<<8; codePtr+=ff?-(int)u:(int)u; break;
		case OP_COALESCE:
			if (top[-1].isEmpty()) {freeV(*--top); codePtr+=2;} else codePtr+=codePtr[0]|codePtr[1]<<8;
			break;
		case OP_IN1:
			if (((u=*codePtr++)&CND_EXT)!=0) u|=*codePtr++<<8;
			rc=top[-2].type!=VT_ERROR&&top[-1].type!=VT_ERROR?calc(OP_EQ,top[-2],&top[-1],2,u,ctx):(u&CND_NOT)!=0?RC_TRUE:RC_FALSE;
			if (unsigned(rc-RC_TRUE)<=unsigned(RC_FALSE-RC_TRUE)) {freeV(*--top); if (cndAct[u&CND_MASK][rc-RC_TRUE]!=2) freeV(*--top); goto bool_op;}
			break;
		case OP_IS_A:
			nops=ff?*codePtr++:2; if (((u=*codePtr++)&CND_EXT)!=0) u|=*codePtr++<<8;
			rc=top[-nops].type!=VT_ERROR && top[1-nops].type!=VT_ERROR ? calc(OP_IS_A,top[-nops],&top[1-nops],nops,u,ctx):(u&CND_NOT)!=0?RC_TRUE:RC_FALSE;
			for (int i=nops; --i>=0;) freeV(*--top); goto bool_op;
		case OP_ISLOCAL:
			if (((u=*codePtr++)&CND_EXT)!=0) u|=*codePtr++<<8;
			switch (top[-1].type) {
			default: rc=(u&CND_NOT)!=0?RC_TRUE:RC_FALSE; freeV(*--top); goto bool_op;
			case VT_REF: rc=((PIN*)(top[-1].pin))->getV(PROP_SPEC_PINID,w,0,ctx.ses)!=RC_OK||isRemote(w.id)?RC_FALSE:RC_TRUE; freeV(*--top); goto bool_op;
			case VT_REFID: rc=isRemote(top[-1].id)?RC_FALSE:RC_TRUE; freeV(*--top); goto bool_op;
			}
		case OP_EXISTS:
			rc=top[-1].type==VT_STMT?top[-1].stmt->exist(ctx.nParams>0?ctx.params[0].vals:(Value*)0,ctx.nParams>0?ctx.params[0].nValues:0):top[-1].type==VT_UINT&&top[-1].ui>0?RC_TRUE:RC_FALSE;
			freeV(*--top); if (((u=*codePtr++)&CND_EXT)!=0) u|=*codePtr++<<8; goto bool_op;
		case OP_ISCHANGED:
			if ((vdx=*codePtr++)==0xFF) {vdx=codePtr[0]|codePtr[1]<<8; codePtr+=2;}
			if (ff) {afy_dec32(codePtr,eid); eid=afy_dec32zz(eid);}
			if (((u=*codePtr++)&CND_EXT)!=0) u|=*codePtr++<<8;
			rc=RC_TRUE; pi=NULL;
			if (ctx.ect!=ECT_DETECT || ctx.modp!=NULL && (pi=((ModProps*)ctx.modp)->find(((uint32_t*)(&hdr+1))[vdx]&STORE_MAX_URIID))==NULL) rc=RC_FALSE;
			else if (ff && pi!=NULL) {
				//???
			}
			goto bool_op;
		case OP_SIMILAR:
			if (ctx.rctx==NULL) ctx.rctx=&rctx;
		case OP_EQ: case OP_NE: case OP_LT: case OP_LE: case OP_GT: case OP_GE: case OP_CONTAINS: case OP_BEGINS: case OP_ENDS: case OP_IN: case OP_TESTBIT:
			if (((u=*codePtr++)&CND_EXT)!=0) u|=*codePtr++<<8;
			rc=top[-2].type!=VT_ERROR&&top[-1].type!=VT_ERROR?calc((ExprOp)op,top[-2],&top[-1],2,u,ctx):op==OP_NE||(u&CND_NOT)!=0?RC_TRUE:RC_FALSE;
			freeV(*--top); freeV(*--top);
		bool_op:
			if (unsigned(rc-RC_TRUE)<=unsigned(RC_FALSE-RC_TRUE)) {
				fop=cndAct[u&CND_MASK][rc-RC_TRUE];
				if ((u&CND_VBOOL)!=0) {
					top->set(rc==((u&CND_NOT)==0?RC_TRUE:RC_FALSE)); rc=RC_OK;
					switch (fop) {
					case 0: case 1: codePtr=codeEnd; break;
					case 2: if ((u&CND_MASK)>=6) codePtr+=2; break;
					case 3: codePtr+=codePtr[0]|codePtr[1]<<8; break;
					}
					if (codePtr==codeEnd) top++;
				} else switch (fop) {
				case 0: assert(top==stack); return RC_TRUE;
				case 1: assert(top==stack); return RC_FALSE;
				case 2: if ((u&CND_MASK)>=6) codePtr+=2; rc=RC_OK; break;
				case 3: codePtr+=codePtr[0]|codePtr[1]<<8; rc=RC_OK; break;
				}
			}
			break;
		default:
			nops=SInCtx::opDscr[op].nOps[0]; fop=SInCtx::opDscr[op].flags; u=0;
			if (ff) {
				u=*codePtr++;
				if ((fop&_I)!=0) nops=SInCtx::opDscr[op].nOps[1]-1; else if (nops!=SInCtx::opDscr[op].nOps[1]) nops=*codePtr++;
			}
			if (top[-nops].type!=VT_ERROR && (nops>1 && top[-nops+1].type==VT_ERROR 
				|| (rc=SInCtx::opDscr[op].func((ExprOp)op,top[-nops],&top[1-nops],nops,u,ctx))!=RC_OK && cntCatch!=0))
					{freeV(top[-nops]); top[-nops].setError(); rc=RC_OK;}
			for (int i=nops; --i>=1;) freeV(*--top);
			top[-1].property=STORE_INVALID_URIID; break;
		}
		if (rc!=RC_OK) {while (top>stack) freeV(*--top); return rc;}
	}
	assert(codePtr<=codeEnd && (top==stack || top==stack+1));
	return top==stack?RC_TRUE:(result=top[-1],result.isEmpty())?RC_NOTFOUND:RC_OK;
}

PathIt::~PathIt()
{
	if (path!=NULL) ma->free((void*)path);
}

const Value *PathIt::navigate(GO_DIR dir,ElementID eid)
{
	switch (dir) {
	case GO_FINDBYID:
		if (eid==STORE_COLLECTION_ID) {
			// release
		}
	default: return NULL;
	case GO_FIRST: sidx=0; while(pst!=NULL) pop();
	case GO_NEXT: break;
	}
	const Value *pv; bool fOK=false; //RC rc;
	for (;;) {
		if (pst==NULL) {
			if (src.type!=VT_COLLECTION) {if (sidx==0) pv=&src; else return NULL;}
			else if (!src.isNav()) {if (sidx<src.length) pv=&src.varray[sidx]; else return NULL;}
			else if ((pv=src.nav->navigate(sidx==0?GO_FIRST:GO_NEXT))==NULL) return NULL;
			sidx++; //pex.getID(id);
//			if ((rc=push(id))!=RC_OK) return NULL; pst->v[0].setError(path[0].pid);
//			if ((rc=getData(pex,&pst->v[0],1,NULL,ses,path[0].eid))!=RC_OK) {state|=QST_EOF; return rc;}
			pst->state=2; pst->vidx=0; if (fThrough) return &res;
		}
		switch (pst->state) {
		case 0:
			pst->state=1; assert(pst!=NULL && pst->idx>0);
			if (pst->idx>=nPathSeg && pst->rcnt>=path[pst->idx-1].rmin && path[pst->idx-1].filter==NULL) {/*printf("->\n");*/ return &res;}
		case 1:
			pst->state=2; //printf("%*s(%d,%d):" _LX_FM "\n",(pst->idx-1+pst->rcnt-1)*2,"",pst->idx,pst->rcnt,res->id.pid);
//			if ((rc=getBody(*res))!=RC_OK || (res->hpin->hdr.descr&HOH_HIDDEN)!=0)
//				{res->cleanup(); if (rc==RC_OK || rc==RC_NOACCESS || rc==RC_REPEAT || rc==RC_DELETED) {pop(); continue;} else {state|=QST_EOF; return rc;}}
//			fOK=path[pst->idx-1].filter==NULL || ses->getStore()->queryMgr->condSatisfied((const Expr* const*)&path[pst->idx-1].filter,1,(const PINx**)&res,1,params,nParams,ses);
			if (!fOK && !path[pst->idx-1].fLast) {AfyKernel::freeV(res); pop(); continue;}
			if (pst->rcnt<path[pst->idx-1].rmax||path[pst->idx-1].rmax==0xFFFF) {
//				if ((rc=res->getV(path[pst->idx-1].pid,pst->v[1],LOAD_SSV|LOAD_REF,ses,path[pst->idx-1].eid))==RC_OK) {if (!pst->v[1].isEmpty()) pst->vidx=1;}
//				else if (rc!=RC_NOTFOUND) {freeV(res); return NULL;}
			}
			if (fOK && pst->rcnt>=path[pst->idx-1].rmin) {
				//pst->nSucc++;
				if (pst->idx<nPathSeg) {
					assert(pst->rcnt<=path[pst->idx-1].rmax||path[pst->idx-1].rmax==0xFFFF);
					for (;;) {
//						if ((rc=res->getV(path[pst->idx].pid,pst->v[0],LOAD_SSV|LOAD_REF,ses,path[pst->idx].eid))!=RC_OK && rc!=RC_NOTFOUND)
//							{freeV(res); return NULL;}
//						if (rc==RC_OK && !pst->v[0].isEmpty()) {pst->vidx=0; break;}
						if (path[pst->idx].rmin!=0) break; if (pst->idx+1>=nPathSeg) return &res;
						//unsigned s=pst->vidx; pst->vidx=0; //if ((rc=push(id))!=RC_OK) return NULL; pst->next->vidx=s;
					}
				} else if (path[pst->idx-1].filter!=NULL) {/*printf("->\n");*/ return &res;}
			}
			freeV(res);
		case 2:
			if (pst->vidx>=2) {pop(); continue;}	// rmin==0 && pst->nSucc==0 -> goto next seg
			switch (pst->v[pst->vidx].type) {
			default: pst->vidx++; continue;		// rmin==0 -> goto next seg
//			case VT_REF: if (res!=NULL) *res=pst->v[pst->vidx].pin->getPID(); if ((rc=push())!=RC_OK) return rc; pst->next->vidx++; continue;
//			case VT_REFID: if (res!=NULL) *res=pst->v[pst->vidx].id; if ((rc=push())!=RC_OK) return rc; pst->next->vidx++; continue;
			case VT_STRUCT:
				//????
				continue;
			case VT_COLLECTION: pst->state=3; pst->cidx=0; break;
			}
		case 3:
			pv=pst->v[pst->vidx].isNav()?pst->v[pst->vidx].nav->navigate(pst->cidx==0?GO_FIRST:GO_NEXT):pst->cidx<pst->v[pst->vidx].length?&pst->v[pst->vidx].varray[pst->cidx]:(const Value*)0;
			if (pv!=NULL) {
				pst->cidx++;
				switch (pv->type) {
				default: continue;
//				case VT_REF: if (res!=NULL) *res=pv->pin->getPID(); if ((rc=push())!=RC_OK) return rc; continue;
//				case VT_REFID: if (res!=NULL) *res=pv->id; if ((rc=push())!=RC_OK) return rc; continue;
				case VT_STRUCT:
					//????
					continue;
				}
			}
			pst->vidx++; pst->state=2; continue;
		}
	}
}

ElementID PathIt::getCurrentID()
{
	return STORE_COLLECTION_ID;
}

const Value	*PathIt::getCurrentValue()
{
	//...
	return NULL;
}

RC PathIt::getElementByID(ElementID,Value&)
{
	return RC_NOTFOUND;
}

INav  *PathIt::clone() const
{
	return NULL;
}

unsigned PathIt::count() const
{
	return ~0L;
}

void PathIt::destroy()
{
	this->~PathIt(); free((void*)this,SES_HEAP);
}

RC Expr::calc(ExprOp op,Value& arg,const Value *moreArgs,int nargs,unsigned flags,const EvalCtx& ctx)
{
	int c,i; RC rc=RC_OK; long start,lstr; const Value *arg2,*arg3,*pv; unsigned dtPart; Value *rng,*args,val,val2; uint32_t len,sht; byte *p; char *s; uint8_t ty;
	if (op>=OP_FIRST_EXPR && op<OP_ALL && ctx.ses->getStore()->opTab!=NULL) {
		for (ExtOp *eo=ctx.ses->getStore()->opTab[op]; eo!=NULL; eo=eo->next) {
			// check nargs and param types, if not -> continue;
			if ((rc=eo->func(arg,moreArgs,nargs,flags,ctx.ses))!=RC_REPEAT) return rc;
		}
		rc=RC_OK;
	}
	switch (op) {
	case OP_PLUS:
		if (arg.type==VT_ARRAY || moreArgs->type==VT_ARRAY) return laAddSub(op,arg,*moreArgs,flags,ctx);
		if ((!isNumeric((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val,NO_FLT|NO_INT|NO_DAT1|NO_ITV1|NO_ITV2,ctx))==NULL) return RC_TYPE;
		if (moreArgs->type==VT_INTERVAL) {
			if (arg.type!=VT_DATETIME && arg.type!=VT_INTERVAL &&
				(rc=convV(arg,arg,VT_DATETIME,ctx.ma))!=RC_OK && (rc=convV(arg,arg,VT_INTERVAL,ctx.ma))!=RC_OK) return rc;
		}
		switch (arg.type) {
		default: assert(0);
		case VT_INT:		arg.i+=moreArgs->i; break;
		case VT_UINT:		arg.ui+=moreArgs->ui; break;
		case VT_INT64:		arg.i64+=moreArgs->i64; break;
		case VT_UINT64:		arg.ui64+=moreArgs->ui64; break;
		case VT_DATETIME:	
			if (moreArgs->type!=VT_INTERVAL) {
				if ((rc=convV(*moreArgs,val,VT_INTERVAL,ctx.ma))!=RC_OK) return rc;
				moreArgs=&val;
			}
			arg.ui64+=moreArgs->i64; break;
		case VT_INTERVAL:
			if (moreArgs->type!=VT_INTERVAL) {
				if ((rc=convV(*moreArgs,val,VT_INTERVAL,ctx.ma))!=RC_OK) return rc;
				moreArgs=&val;
			}
			arg.i64+=moreArgs->i64; break;	// interval + date -> date
		case VT_FLOAT:
			if (arg.qval.units==moreArgs->qval.units) arg.f+=moreArgs->f;
			else {
				val.qval.d=arg.f; val.qval.units=arg.qval.units; val2.qval.d=moreArgs->f; val2.qval.units=moreArgs->qval.units;
				if (compatible(val.qval,val2.qval)) {arg.f=(float)(val.d+val2.d); arg.qval.units=val.qval.units;} else return RC_TYPE;
			}
			break;
		case VT_DOUBLE:
			if (arg.qval.units==moreArgs->qval.units) arg.d+=moreArgs->d;
			else {val2.qval=moreArgs->qval; if (compatible(arg.qval,val2.qval)) arg.d+=val2.d; else return RC_TYPE;}
			break;
		}
		break;
	case OP_MINUS:
		if (arg.type==VT_ARRAY || moreArgs->type==VT_ARRAY) return laAddSub(op,arg,*moreArgs,flags,ctx);
		if ((!isNumeric((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val,NO_FLT|NO_INT|NO_DAT1|NO_ITV1|NO_DAT2|NO_ITV2,ctx))==NULL) return RC_TYPE;
		if (moreArgs->type==VT_INTERVAL && arg.type!=VT_INTERVAL && (rc=convV(arg,arg,VT_INTERVAL,ctx.ma))!=RC_OK) return rc;
		switch (arg.type) {
		default: assert(0);
		case VT_INT:		arg.i-=moreArgs->i; break;
		case VT_UINT:		arg.ui-=moreArgs->ui; break;
		case VT_INT64:		arg.i64-=moreArgs->i64; break;
		case VT_UINT64:		arg.ui64-=moreArgs->ui64; break;
		case VT_DATETIME:	
			if (moreArgs->type!=VT_DATETIME && moreArgs->type!=VT_INTERVAL) {
				if ((rc=convV(*moreArgs,val,VT_DATETIME,ctx.ma))!=RC_OK && (rc=convV(*moreArgs,val,VT_INTERVAL,ctx.ma))!=RC_OK) return rc;
				moreArgs=&val;
			}
			if (moreArgs->type==VT_INTERVAL) arg.ui64-=moreArgs->i64; 
			else {assert(moreArgs->type==VT_DATETIME); arg.i64=arg.ui64-moreArgs->ui64; arg.type=VT_INTERVAL;}
			break;
		case VT_INTERVAL:
			if (moreArgs->type!=VT_INTERVAL) {
				if ((rc=convV(*moreArgs,val,VT_INTERVAL,ctx.ma))!=RC_OK) return rc;
				moreArgs=&val;
			}
			arg.i64-=moreArgs->i64; break;
		case VT_FLOAT:
			if (arg.qval.units==moreArgs->qval.units) arg.f-=moreArgs->f;
			else {
				val.qval.d=arg.f; val.qval.units=arg.qval.units; val2.qval.d=moreArgs->f; val2.qval.units=moreArgs->qval.units;
				if (compatible(val.qval,val2.qval)) {arg.f=(float)(val.d-val2.d); arg.qval.units=val.qval.units;} else return RC_TYPE;
			}
			break;
		case VT_DOUBLE:
			if (arg.qval.units==moreArgs->qval.units) arg.d-=moreArgs->d;
			else {val2.qval=moreArgs->qval; if (compatible(arg.qval,val2.qval)) arg.d-=val2.d; else return RC_TYPE;}
			break;
		}
		break;
	case OP_MUL:
		if (arg.type==VT_ARRAY || moreArgs->type==VT_ARRAY) return laMul(arg,*moreArgs,flags,ctx);
		if ((!isNumeric((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val,NO_FLT|NO_INT,ctx))==NULL) return RC_TYPE;
		switch (arg.type) {
		default: assert(0);
		case VT_INT:	arg.i*=moreArgs->i; break;
		case VT_UINT:	arg.ui*=moreArgs->ui; break;
		case VT_INT64:	arg.i64*=moreArgs->i64; break;
		case VT_UINT64:	arg.ui64*=moreArgs->ui64; break;
		case VT_FLOAT:	arg.f*=moreArgs->f; goto check_mul;
		case VT_DOUBLE:	arg.d*=moreArgs->d;
		check_mul: if (moreArgs->qval.units!=Un_NDIM && !compatibleMulDiv(arg,moreArgs->qval.units,false)) return RC_TYPE; break;
		}
		break;
	case OP_DIV:
		if (arg.type==VT_ARRAY || moreArgs->type==VT_ARRAY) return laDiv(arg,*moreArgs,flags,ctx);
		if ((!isNumeric((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val,NO_FLT|NO_INT,ctx))==NULL) return RC_TYPE;
		switch (arg.type) {
		default: assert(0);
		case VT_INT:	if (moreArgs->i==0) return RC_DIV0; arg.i/=moreArgs->i; break;
		case VT_UINT:	if (moreArgs->ui==0) return RC_DIV0; arg.ui/=moreArgs->ui; break;
		case VT_INT64:	if (moreArgs->i64==0) return RC_DIV0; arg.i64/=moreArgs->i64; break;
		case VT_UINT64:	if (moreArgs->ui64==0) return RC_DIV0; arg.ui64/=moreArgs->ui64; break;
		case VT_FLOAT:	if (moreArgs->f==0.f) return RC_DIV0; arg.f/=moreArgs->f; goto check_div;
		case VT_DOUBLE:	if (moreArgs->d==0.) return RC_DIV0; arg.d/=moreArgs->d;
		check_div: if (moreArgs->qval.units!=Un_NDIM && !compatibleMulDiv(arg,moreArgs->qval.units,true)) return RC_TYPE; break;
		}
		break;
	case OP_MOD:
		if ((!isNumeric((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val,NO_FLT|NO_INT,ctx))==NULL) return RC_TYPE;
		switch (arg.type) {
		default: assert(0);
		case VT_INT:	if (moreArgs->i==0) return RC_DIV0; arg.i%=moreArgs->i; break;
		case VT_UINT:	if (moreArgs->ui==0) return RC_DIV0; arg.ui%=moreArgs->ui; break;
		case VT_INT64:	if (moreArgs->i64==0) return RC_DIV0; arg.i64%=moreArgs->i64; break;
		case VT_UINT64:	if (moreArgs->ui64==0) return RC_DIV0; arg.ui64%=moreArgs->ui64; break;
		case VT_FLOAT:	if (moreArgs->f==0.f) return RC_DIV0; arg.f=fmod(arg.f,moreArgs->f); goto check_mod;
		case VT_DOUBLE:	if (moreArgs->d==0.) return RC_DIV0; arg.d=fmod(arg.d,moreArgs->d);
		check_mod: if (moreArgs->qval.units!=Un_NDIM && !compatibleMulDiv(arg,moreArgs->qval.units,true)) return RC_TYPE; break;
		}
		break;
	case OP_NEG:
		if (!isNumeric((ValueType)arg.type) && arg.type!=VT_INTERVAL && !numOpConv(arg,NO_FLT|NO_INT|NO_ITV1,ctx)) return RC_TYPE;
		switch (arg.type) {
		default: assert(0);
		case VT_INT:		if ((uint32_t)arg.i==0x80000000) arg.type=VT_UINT; else arg.i=-arg.i; break;
		case VT_UINT:		if ((uint32_t)arg.i<=0x80000000) {arg.type=VT_INT; arg.i=-arg.i; break;} else {arg.type=VT_INT64; arg.i64=arg.ui;}
		case VT_INT64:		if ((uint64_t)arg.i64==0x8000000000000000ULL) arg.type=VT_UINT64; else arg.i64=-arg.i64; break;
		case VT_UINT64:		if ((uint64_t)arg.ui64>0x8000000000000000ULL) return RC_INVPARAM; arg.type=VT_INT64; arg.i64=-arg.i64; break;
		case VT_FLOAT:		arg.f=-arg.f; break;
		case VT_DOUBLE:		arg.d=-arg.d; break;
		case VT_INTERVAL:	if ((uint64_t)arg.i64==0x8000000000000000ULL) return RC_INVPARAM; arg.i64=-arg.i64; break;
		}
		break;
	case OP_ABS:
		if (!isNumeric((ValueType)arg.type) && arg.type!=VT_INTERVAL && !numOpConv(arg,NO_FLT|NO_INT|NO_ITV1,ctx)) return RC_TYPE;
		switch (arg.type) {
		default: assert(0);
		case VT_INT:		if ((uint32_t)arg.i==0x80000000) arg.type=VT_UINT; else arg.i=abs(arg.i); break;
		case VT_UINT:		break;
		case VT_INT64:		if ((uint64_t)arg.i64==0x8000000000000000ULL) arg.type=VT_UINT64; else arg.i64=abs(arg.i64); break;
		case VT_UINT64:		break;
		case VT_FLOAT:		arg.f=fabs(arg.f); break;
		case VT_DOUBLE:		arg.d=fabs(arg.d); break;
		case VT_INTERVAL:	if ((uint64_t)arg.i64!=0x8000000000000000ULL && arg.i64<0LL) arg.i64=-arg.i64; break;
		}
		break;
	case OP_LN:
		if (arg.type==VT_FLOAT) {if (arg.f==0.f) return RC_DIV0; arg.f=logf(arg.f);}
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE,ctx.ma)!=RC_OK) return RC_TYPE;
		else if (arg.d==0.) return RC_DIV0; else arg.d=log(arg.d);
		// Udim ???
		break;
	case OP_EXP:
		if (arg.type==VT_FLOAT) arg.f=expf(arg.f);
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE,ctx.ma)!=RC_OK) return RC_TYPE;
		else arg.d=exp(arg.d);
		// Udim ???
		break;
	case OP_POW:
		// integer power???
		if (arg.type==VT_FLOAT && moreArgs->type==VT_FLOAT) arg.f=powf(arg.f,moreArgs->f);
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE,ctx.ma)!=RC_OK) return RC_TYPE;
		else if ((arg2=moreArgs)->type!=VT_DOUBLE && (arg2=&val,convV(*moreArgs,val,VT_DOUBLE,ctx.ma))!=RC_OK) return RC_TYPE;
		else arg.d=pow(arg.d,arg2->d);
		// Udim ???
		break;
	case OP_SQRT:
		if (arg.type==VT_FLOAT) {if (arg.f<0.f) return RC_INVPARAM; arg.f=sqrtf(arg.f);}
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE,ctx.ma)!=RC_OK) return RC_TYPE;
		else if (arg.d<0.) return RC_INVPARAM; else arg.d=sqrt(arg.d);
		// Udim ???
		break;
	case OP_SIN:
		if (arg.type==VT_FLOAT) arg.f=sinf(arg.f);
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE,ctx.ma)!=RC_OK) return RC_TYPE;
		else arg.d=sin(arg.d);
		// Udim ???
		break;
	case OP_COS:
		if (arg.type==VT_FLOAT) arg.f=cosf(arg.f);
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE,ctx.ma)!=RC_OK) return RC_TYPE;
		else arg.d=cos(arg.d);
		// Udim ???
		break;
	case OP_TAN:
		if (arg.type==VT_FLOAT) arg.f=tanf(arg.f);
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE,ctx.ma)!=RC_OK) return RC_TYPE;
		else arg.d=tan(arg.d);
		// Udim ???
		break;
	case OP_ASIN:
		if (arg.type==VT_FLOAT) {if (arg.f<-1.f||arg.f>1.f) return RC_INVPARAM; arg.f=asinf(arg.f);}
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE,ctx.ma)!=RC_OK) return RC_TYPE;
		else if (arg.d<-1.||arg.d>1.) return RC_INVPARAM; else arg.d=asin(arg.d);
		// Udim ???
		break;
	case OP_ACOS:
		if (arg.type==VT_FLOAT) {if (arg.f<-1.f||arg.f>1.f) return RC_INVPARAM; arg.f=acosf(arg.f);}
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE,ctx.ma)!=RC_OK) return RC_TYPE;
		else if (arg.d<-1.||arg.d>1.) return RC_INVPARAM; else arg.d=acos(arg.d);
		// Udim ???
		break;
	case OP_ATAN:
		if (arg.type==VT_FLOAT&&nargs==1||moreArgs->type==VT_FLOAT) {arg.f=nargs==1?atanf(arg.f):atan2f(arg.f,moreArgs->f); break;}
		if (arg.type!=VT_DOUBLE || nargs==2 && arg.type!=moreArgs->type) {
			if (nargs==1) {if (convV(arg,arg,VT_DOUBLE,ctx.ma)!=RC_OK) return RC_TYPE;}
			else if ((moreArgs=numOpConv(arg,moreArgs,val,NO_FLT,ctx))==NULL) return RC_TYPE;
		}
		arg.d=nargs==1?atan(arg.d):atan2(arg.d,moreArgs->d);
		// Udim ???
		break;
	case OP_FLOOR:
		if (isInteger((ValueType)arg.type)) break;
		if (arg.type==VT_FLOAT) arg.f=floorf(arg.f);
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE,ctx.ma)!=RC_OK) return RC_TYPE;
		else arg.d=::floor(arg.d);
		break;
	case OP_CEIL:
		if (isInteger((ValueType)arg.type)) break;
		if (arg.type==VT_FLOAT) arg.f=ceilf(arg.f);
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE,ctx.ma)!=RC_OK) return RC_TYPE;
		else arg.d=::ceil(arg.d);
		break;
	case OP_NOT:
		if (arg.type==VT_BSTR) {
			if ((arg.flags&HEAP_TYPE_MASK)<SES_HEAP && (rc=expandStr(arg,arg.length,ctx.ma))!=RC_OK) return rc;
			for (uint32_t i=0; i<arg.length; i++) ((byte*)arg.bstr)[i]=~arg.bstr[i];
		} else if (!isInteger((ValueType)arg.type) && !numOpConv(arg,NO_INT,ctx)) return RC_TYPE;
		else switch (arg.type) {
		default: assert(0);
		case VT_INT:		arg.i=~arg.i; break;
		case VT_UINT:		arg.ui=~arg.ui; break;
		case VT_INT64:		arg.i64=~arg.i64; break;
		case VT_UINT64:		arg.ui64=~arg.ui64; break;
		}
		break;
	case OP_AND: case OP_OR: case OP_XOR:
		if (arg.type==VT_BSTR) {
			if (moreArgs->type!=VT_BSTR) return RC_TYPE;	// conv???
			if (arg.length<moreArgs->length && (op==OP_OR || op==OP_XOR) && (rc=expandStr(arg,moreArgs->length,ctx.ma,true))!=RC_OK) return rc;
			else if ((arg.flags&HEAP_TYPE_MASK)<SES_HEAP && (rc=expandStr(arg,arg.length,ctx.ma))!=RC_OK) return rc;
			uint32_t l=min(arg.length,moreArgs->length);
			if (op==OP_AND) {for (uint32_t i=0; i<l; i++) ((byte*)arg.bstr)[i]&=moreArgs->bstr[i]; arg.length=l;}
			else if (op==OP_OR) for (uint32_t i=0; i<l; i++) ((byte*)arg.bstr)[i]|=moreArgs->bstr[i];
			else for (uint32_t i=0; i<l; i++) ((byte*)arg.bstr)[i]^=moreArgs->bstr[i];
		} else if ((!isInteger((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val,NO_INT,ctx))==NULL) return RC_TYPE;
		else switch (arg.type) {
		default: assert(0);
		case VT_INT: case VT_UINT: if (op==OP_AND) arg.ui&=moreArgs->ui; else if (op==OP_OR) arg.ui|=moreArgs->ui; else arg.ui^=moreArgs->ui; break;
		case VT_INT64: case VT_UINT64: if (op==OP_AND) arg.ui64&=moreArgs->ui64; else if (op==OP_OR) arg.ui64|=moreArgs->ui64; else arg.ui64^=moreArgs->ui64; break;
		}
		break;
	case OP_LSHIFT: case OP_RSHIFT:
		if ((rc=getI(moreArgs[0],lstr,ctx))!=RC_OK) return rc;
		if (lstr<0) {lstr=-lstr; op=op==OP_LSHIFT?OP_RSHIFT:OP_LSHIFT;}
		if (arg.type==VT_BSTR) {
			if ((arg.flags&HEAP_TYPE_MASK)<SES_HEAP && (rc=expandStr(arg,arg.length,ctx.ma))!=RC_OK) return rc;
			//???
		} else if (!isInteger((ValueType)arg.type) && !numOpConv(arg,NO_INT,ctx)) return RC_TYPE;
		else switch (arg.type) {
		default: assert(0);
		case VT_INT: case VT_UINT:
			if (lstr>=sizeof(uint32_t)*8) return RC_TOOBIG;
			if (op==OP_LSHIFT) arg.ui<<=lstr; else if ((flags&CND_UNS)==0) arg.i>>=lstr; else arg.ui>>=lstr; break;
		case VT_INT64: case VT_UINT64:
			if (lstr>=sizeof(uint32_t)*8) return RC_TOOBIG;
			if (op==OP_LSHIFT) arg.ui64<<=lstr; else if ((flags&CND_UNS)==0) arg.i64>>=lstr; else arg.ui64>>=lstr; break;
		}
		break;
	case OP_SETBIT: case OP_RESETBIT:
		if ((rc=getI(moreArgs[0],lstr,ctx))!=RC_OK) return rc; if (lstr<0) return RC_INVPARAM;
		if (arg.type==VT_BSTR) {
			if (arg.length*8<=(uint32_t)lstr) {
				if (op==OP_RESETBIT) break;
				if ((rc=expandStr(arg,(lstr+7)/8,ctx.ma,true))!=RC_OK) return rc;
			} else if ((arg.flags&HEAP_TYPE_MASK)<SES_HEAP && (rc=expandStr(arg,arg.length,ctx.ma))!=RC_OK) return rc;
			if (op==OP_SETBIT) ((byte*)arg.bstr)[lstr/8]|=1<<lstr%8; else ((byte*)arg.bstr)[lstr/8]&=~(1<<lstr%8);
		} else if (!isInteger((ValueType)arg.type) && !numOpConv(arg,NO_INT,ctx)) return RC_TYPE;
		else switch (arg.type) {
		default: assert(0);
		case VT_INT: case VT_UINT: if (lstr>=sizeof(uint32_t)*8) return RC_TOOBIG; if (op==OP_SETBIT) arg.ui|=1<<lstr; else arg.ui&=~(1<<lstr); break;
		case VT_INT64: case VT_UINT64: if (lstr>=sizeof(uint64_t)*8) return RC_TOOBIG; if (op==OP_SETBIT) arg.ui64|=1ULL<<lstr; else arg.ui64&=~(1ULL<<lstr); break;
		}
		break;
	case OP_MIN: case OP_MAX:
		assert(nargs==2); c=cmp(arg,*moreArgs,flags,ctx.ma);
		if (c==-1 && op==OP_MAX || c==1 && op==OP_MIN) {freeV(arg); if ((rc=copyV(*moreArgs,arg,ctx.ma))!=RC_OK) return rc;}
		break;
	case OP_ARGMIN: case OP_ARGMAX:
		assert(nargs==2); c=cmp(arg,*moreArgs,flags,ctx.ma); freeV(arg);
		arg.set((unsigned)(c==-1&&op==OP_ARGMIN||c==1&&op==OP_ARGMAX?0:1));
		break;
	case OP_LENGTH:
		switch (arg.type) {
		case VT_STREAM: val.ui64=arg.stream.is->length(); freeV(arg); arg.setU64(val.ui64); return RC_OK;
		case VT_COLLECTION:
			if ((rc=calcAgg(OP_CONCAT,arg,moreArgs,nargs,flags,ctx))!=RC_OK) return rc;
		calc_length:
		case VT_STRING:
			// character_length in flags?
		case VT_BSTR: len=arg.length; break;
		default:
			if ((rc=convV(arg,arg,VT_STRING,ctx.ma))!=RC_OK) return rc;
			goto calc_length;
		}
		freeV(arg); arg.set(unsigned(len)); break;
	case OP_CONCAT:
		if (nargs==1) {
			if (arg.type==VT_COLLECTION) return calcAgg(OP_CONCAT,arg,moreArgs,nargs,flags,ctx);
			if (!isString((ValueType)arg.type)) return convV(arg,arg,VT_STRING,ctx.ma);
			break;
		}
		assert(nargs>=2&&nargs<255);
		if (!isString((ValueType)arg.type) && (rc=convV(arg,arg,VT_STRING,ctx.ma))!=RC_OK) return rc;
		ty=arg.type; len=arg.length;
		for (i=1,args=(Value*)moreArgs; i<nargs; i++) {
			Value *arg2=&args[i-1];
			if (arg2->type!=ty) {
				if (arg2->isEmpty()) {freeV(arg); return RC_OK;}
				if ((const Value*)args==moreArgs) {
					args = (Value*)alloca((nargs-1)*sizeof(Value));
					memcpy(args,moreArgs,(nargs-1)*sizeof(Value));
					for (int j=nargs-1; --j>=0;) args[j].flags=0;
					arg2=&args[i-1];
				}
				if ((rc=convV(*arg2,*arg2,(ValueType)ty,ctx.ma))!=RC_OK) {	// ???
					for (int j=nargs-1; --j>=0;) freeV(args[j]);
					return rc;
				}
			}
			len+=arg2->length;
		}
		p=(byte*)ctx.ma->malloc(len+(ty==VT_BSTR?0:1));
		if (p==NULL) return RC_NOMEM; if (ty!=VT_BSTR) p[len]=0;
		memcpy(p,arg.bstr,arg.length);
		for (i=1,len=arg.length; i<nargs; i++) {
			const Value *arg2=&args[i-1]; memcpy(p+len,arg2->bstr,arg2->length);
			len+=arg2->length; if (args!=moreArgs) freeV(const_cast<Value&>(*arg2));
		}
		freeV(arg); arg.type=ty; arg.bstr=p; arg.length=len; arg.flags=ctx.ma->getAType();
		break;
	case OP_SUBSTR:
		assert(nargs>=2&&nargs<=3);
		if (!isString((ValueType)arg.type) && (rc=convV(arg,arg,VT_STRING,ctx.ma))!=RC_OK || (rc=getI(moreArgs[0],lstr,ctx))!=RC_OK) return rc;
		if (nargs==3 && moreArgs[1].isEmpty()) {freeV(arg); return RC_OK;}
		if (nargs==2) start=0; else {start=lstr; if ((rc=getI(moreArgs[1],lstr,ctx))!=RC_OK) return rc;}
		if (lstr<0 || start<0 && -start>lstr) return RC_INVPARAM;
		if (lstr==0 || start==0 && arg.length>=(unsigned)lstr) {arg.length=lstr; break;}
		p=(byte*)arg.bstr;
		if ((arg.flags&HEAP_TYPE_MASK)<SES_HEAP) {
			arg.bstr=(uint8_t*)ctx.ma->malloc(lstr+(arg.type!=VT_BSTR?1:0)); if (arg.bstr==NULL) return RC_NOMEM;
			setHT(arg,ctx.ma->getAType());
		} else if ((unsigned)lstr>arg.length) {
			arg.bstr=(uint8_t*)ctx.ma->realloc(p,lstr+(arg.type!=VT_BSTR?1:0),arg.length); if ((p=(byte*)arg.bstr)==NULL) return RC_NOMEM;
		}
		if (start<0 && -start<lstr) memmove((byte*)arg.bstr-start,p,min(uint32_t(lstr+start),arg.length));
		else if (start>=0 && (unsigned)start<arg.length) memmove((byte*)arg.bstr,p+start,min(unsigned(lstr),unsigned(arg.length-start)));
		p=(byte*)arg.bstr; i=arg.type==VT_BSTR?0:' ';
		if (start>=(long)arg.length) memset(p,i,lstr);
		else {if (start<0) memset(p,i,-start); if (lstr>(long)arg.length-start) memset(p+arg.length-start,i,lstr-arg.length+start);}
		arg.length=lstr; if (arg.type!=VT_BSTR) ((char*)arg.str)[lstr]='\0'; break;
	case OP_REPLACE:
		if (moreArgs[1].isEmpty()) {freeV(arg); return RC_OK;}
		if ((arg2=strOpConv(arg,moreArgs,val,ctx))==NULL || (arg3=strOpConv(arg,moreArgs+1,val2,ctx))==NULL) return RC_TYPE;
		if (arg.length>0 && arg2->length>0) for (p=(byte*)arg.bstr,len=arg.length;;) {
			byte *q=(byte*)memchr(p,*arg2->bstr,len);
			if (q==NULL || (sht=len-unsigned(q-p))<arg2->length) break; 
			if (memcmp(q,arg2->bstr,arg2->length)!=0) {if ((len=sht-1)<arg2->length) break; p=q+1; continue;}
			unsigned l=arg.length+arg3->length-arg2->length,ll=unsigned(q-(byte*)arg.bstr);
			if ((arg.flags&HEAP_TYPE_MASK)<SES_HEAP) {
				p=(byte*)ctx.ma->malloc(l); if (p==NULL) return RC_NOMEM;
				memcpy(p,arg.bstr,ll); memcpy(p+ll,arg3->bstr,arg3->length);
				if (arg.length>ll+arg2->length) memcpy(p+ll+arg3->length,arg.bstr+ll+arg2->length,arg.length-ll-arg2->length);
				setHT(arg,ctx.ma->getAType()); arg.bstr=p; p+=ll+arg3->length;
			} else if (arg3->length>arg2->length) {
				arg.bstr=p=(byte*)ctx.ma->realloc((byte*)arg.bstr,l,arg.length); if (p==NULL) return RC_NOMEM;
				if (arg.length>ll+arg2->length) memmove(p+ll+arg3->length,p+ll+arg2->length,arg.length-ll-arg2->length);
				memcpy(p+ll,arg3->bstr,arg3->length); p+=ll+arg3->length;
			} else {
				memcpy(q,arg3->bstr,arg3->length); p=q+arg3->length;
				if (sht>arg2->length) memcpy(p,q+arg2->length,sht-arg2->length);
			}
			arg.length=l; if ((len=sht-arg2->length)<arg2->length) break;
		}
		if (arg2==&val) freeV(val); if (arg3==&val2) freeV(val2);
		break;
	case OP_PAD:
		if ((rc=getI(moreArgs[0],lstr,ctx))!=RC_OK) return rc; if (lstr<0) return RC_INVPARAM;
		if (nargs>23 && moreArgs[1].isEmpty()) {freeV(arg); return RC_OK;}		//???????
		if (nargs==2) arg2=NULL; else if ((arg2=strOpConv(arg,moreArgs+1,val,ctx))==NULL) return RC_TYPE;
		if ((unsigned)lstr>arg.length) {
			uint32_t l=arg.length;
			if ((rc=expandStr(arg,(uint32_t)lstr,ctx.ma))!=RC_OK) return rc;
			p=(byte*)arg.bstr+l;
			if (arg2!=NULL && arg2->length>1) {
				for (unsigned j=((unsigned)lstr-l)/arg2->length; j>0; --j) {memcpy(p,arg2->bstr,arg2->length); p+=arg2->length;}
				if ((len=((unsigned)lstr-l)%arg2->length)>0) memcpy(p,arg2->bstr,len);
			} else
				memset(p,arg2!=NULL&&arg2->length>0?*arg2->bstr:arg.type==VT_BSTR?0:' ',(unsigned)lstr-l);
		}
		if (arg2==&val) freeV(val);
		break;
	case OP_TRIM:
		if ((arg2=strOpConv(arg,moreArgs,val,ctx))!=NULL) {s=(char*)arg2->str; lstr=arg2->length;} else return RC_TYPE;
		if (nargs<3) start=flags; else if ((rc=getI(moreArgs[1],start,ctx))!=RC_OK) return rc;
		if ((start&2)==0) {
			const char *const end=arg.str+arg.length,*p=arg.str;
			if (lstr==1) {for (const char ch=*s; p<end; p++) if (*p!=ch) break;}
			else for (; end-p>=lstr; p+=lstr) if (memcmp(p,s,lstr)!=0) break;
			if (p!=arg.str) {
				arg.length=uint32_t(end-p);
				if ((arg.flags&HEAP_TYPE_MASK)<SES_HEAP) arg.str=p; else memmove((char*)arg.str,p,arg.length);
			}
		}
		if ((start&1)==0) {
			const char *const beg=arg.str,*p=arg.str+arg.length;
			if (lstr==1) {for (const char ch=*s; p>beg; --p) if (p[-1]!=ch) break;}
			else for (; p-beg>=lstr; p-=lstr) if (memcmp(p-lstr,s,lstr)!=0) break;
			if (p!=arg.str+arg.length) {
				// check UTF-8!!!
				arg.length=uint32_t(p-arg.str);
			}
		}
		break;
	case OP_EQ: flags|=CND_EQ; goto comp;
	case OP_NE: flags|=CND_NE; goto comp;
	case OP_LT: case OP_LE: case OP_GT: case OP_GE:
	comp:
		if (arg.type==VT_COLLECTION) {
			if (!arg.isNav()) {
				for (len=arg.length,rc=RC_FALSE; len!=0; ) {
					rc=calc(op,const_cast<Value&>(arg.varray[--len]),moreArgs,2,flags,ctx);
					if (rc==RC_TRUE) {if (op==OP_EQ) break;} else if (rc!=RC_FALSE || op!=OP_EQ) break;
				}
			} else {
				for (arg2=arg.nav->navigate(GO_FIRST),rc=RC_FALSE; arg2!=NULL; arg2=arg.nav->navigate(GO_NEXT)) {
					val=*arg2; setHT(val); rc=calc(op,val,moreArgs,2,flags,ctx); freeV(val);
					if (rc==RC_TRUE) {if (op==OP_EQ) break;} else if (rc!=RC_FALSE || op!=OP_EQ) break;
				}
				arg.nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID);
			}
			return rc;
		}
		switch (moreArgs->type) {
		default: break;
		case VT_COLLECTION:
			if (!moreArgs->isNav()) {
				for (len=moreArgs->length; len!=0;) {
					c=cmp(arg,moreArgs->varray[--len],flags,ctx.ma); if (op==OP_EQ) {if (c==0) return RC_TRUE;} else if ((rc=condRC(c,op))!=RC_TRUE) return rc;
				}
				return op!=OP_EQ?RC_TRUE:RC_FALSE;
			} else {
				for (arg2=moreArgs->nav->navigate(GO_FIRST); ;arg2=moreArgs->nav->navigate(GO_NEXT)) {
					if (arg2==NULL) {rc=op!=OP_EQ?RC_TRUE:RC_FALSE; break;}
					c=cmp(arg,*arg2,flags,ctx.ma); if (op==OP_EQ) {if (c==0) {rc=RC_TRUE; break;}} else if ((rc=condRC(c,op))!=RC_TRUE) break;
				}
				moreArgs->nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID); return rc;
			}
		case VT_STMT:
			return ((Stmt*)moreArgs->stmt)->cmp(ctx,arg,op,flags);
		}
		return condRC(cmp(arg,*moreArgs,flags,ctx.ma),op);
	case OP_IS_A:
	case OP_IN:
		if (arg.type==VT_COLLECTION) {
			if (!arg.isNav()) {
				for (len=arg.length,rc=RC_FALSE; rc==RC_FALSE && len!=0; )
					rc=calc(op,const_cast<Value&>(arg.varray[--len]),moreArgs,2,flags,ctx);
			} else {
				for (arg2=arg.nav->navigate(GO_FIRST),rc=RC_FALSE; rc==RC_FALSE && arg2!=NULL; arg2=arg.nav->navigate(GO_NEXT))
					{val=*arg2; setHT(val); rc=calc(op,val,moreArgs,2,flags,ctx); freeV(val);}
				arg.nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID);
			}
			return rc;
		}
		if (op==OP_IS_A) {
			if (moreArgs->type==VT_STMT) return ((Stmt*)moreArgs->stmt)->cmp(ctx,arg,op,flags);
			PINx pex(ctx.ses); PIN *ppe=&pex; if (arg.type!=VT_REFID && arg.type!=VT_REF) return RC_FALSE;
			if (arg.type==VT_REFID) {pex=arg.id; if ((rc=pex.getBody())!=RC_OK) return rc;} else ppe=(PINx*)(PIN*)arg.pin;
			Values vv(nargs>2?&moreArgs[1]:NULL,nargs-2); EvalCtx ctx2(ctx.ses,ctx.env,ctx.nEnv,ctx.vars,ctx.nVars,&vv,1,ctx.stack,ctx.ma);
			switch (moreArgs->type) {
			default: return RC_TYPE;
			case VT_URIID:
				return ctx.ses->getStore()->queryMgr->test(ppe,moreArgs->uid,ctx2,nargs<=2)?RC_TRUE:RC_FALSE;
			case VT_COLLECTION:
				for (arg2=moreArgs->isNav()?moreArgs->nav->navigate(GO_FIRST):&moreArgs->varray[len=0],rc=RC_FALSE; arg2!=NULL; arg2=moreArgs->isNav()?moreArgs->nav->navigate(GO_NEXT):++len<moreArgs->length?&moreArgs->varray[len]:NULL)
					if (arg2->type==VT_URIID && ctx.ses->getStore()->queryMgr->test(ppe,arg2->uid,ctx2,nargs<=2)) {rc=RC_TRUE; break;}
				if (moreArgs->isNav()) moreArgs->nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID); return rc;
			}
		} else switch (moreArgs->type) {
		default: return cmp(arg,*moreArgs,flags|CND_EQ,ctx.ma)==0?RC_TRUE:RC_FALSE;
		case VT_RANGE:
			if ((c=moreArgs->range[0].type==VT_ANY?1:cmp(arg,moreArgs->range[0],(flags&CND_IN_LBND)!=0?flags|CND_NE:flags|CND_EQ,ctx.ma))==-3) return RC_TYPE;
			if (c==-2 || (compareCodeTab[(flags&CND_IN_LBND)!=0?OP_GT-OP_EQ:OP_GE-OP_EQ]&1<<(c+1))==0) return RC_FALSE;
			if ((c=moreArgs->range[1].type==VT_ANY?-1:cmp(arg,moreArgs->range[1],(flags&CND_IN_RBND)!=0?flags|CND_NE:flags|CND_EQ,ctx.ma))==-3) return RC_TYPE; 
			return c==-2 || (compareCodeTab[(flags&CND_IN_RBND)!=0?OP_LT-OP_EQ:OP_LE-OP_EQ]&1<<(c+1))==0?RC_FALSE:RC_TRUE;
		case VT_STMT:
			return ((Stmt*)moreArgs->stmt)->cmp(ctx,arg,op,flags);
		case VT_COLLECTION:
			for (arg2=moreArgs->isNav()?moreArgs->nav->navigate(GO_FIRST):&moreArgs->varray[len=0],rc=RC_FALSE; arg2!=NULL; arg2=moreArgs->isNav()?moreArgs->nav->navigate(GO_NEXT):++len<moreArgs->length?&moreArgs->varray[len]:NULL) {
				if (arg2->type!=VT_RANGE) {if (cmp(arg,*arg2,flags|CND_EQ,ctx.ma)==0) {rc=RC_TRUE; break;}}
				else {
					c=cmp(arg,arg2->range[0],(flags&CND_IN_LBND)!=0?flags|CND_NE:flags|CND_EQ,ctx.ma);
					if (c>=-1 && (compareCodeTab[OP_GE-OP_EQ]&1<<(c+1))!=0) {
						c=cmp(arg,arg2->range[1],(flags&CND_IN_RBND)!=0?flags|CND_NE:flags|CND_EQ,ctx.ma);
						if (c>=-1 && (compareCodeTab[OP_LE-OP_EQ]&1<<(c+1))!=0) {rc=RC_TRUE; break;}
					}
				}
			}
			if (moreArgs->isNav()) moreArgs->nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID);
			return rc;
		case VT_ARRAY:
			if (arg.type!=moreArgs->fa.type||(arg.type==VT_DOUBLE||arg.type==VT_FLOAT)&&arg.qval.units!=moreArgs->fa.units) return RC_FALSE;
			for (len=0; len<moreArgs->length; len++) switch (arg.type) {
			default: return RC_FALSE;
			case VT_INT: if (arg.i==moreArgs->fa.i[len]) return RC_TRUE; break;
			case VT_UINT: if (arg.ui==moreArgs->fa.ui[len]) return RC_TRUE; break;
			case VT_INT64: case VT_INTERVAL: if (arg.i64==moreArgs->fa.i64[len]) return RC_TRUE; break;
			case VT_UINT64: case VT_DATETIME: if (arg.ui64==moreArgs->fa.ui64[len]) return RC_TRUE; break;
			case VT_FLOAT: if (arg.f==moreArgs->fa.f[len]) return RC_TRUE; break;
			case VT_DOUBLE: if (arg.d==moreArgs->fa.d[len]) return RC_TRUE; break;
			case VT_REFID: if (arg.id==moreArgs->fa.id[len]) return RC_TRUE; break;
				//...
			}
			return RC_FALSE;
		}
		break;
	case OP_TESTBIT:
		if ((rc=getI(*moreArgs,lstr,ctx))!=RC_OK) return rc; if (lstr<0) return RC_INVPARAM;
		switch (arg.type) {
		case VT_INT: case VT_UINT: if ((uint32_t)lstr>=sizeof(uint32_t)*8 || (arg.ui&1<<lstr)==0) return RC_FALSE; break;
		case VT_INT64: case VT_UINT64: if ((uint32_t)lstr>=sizeof(uint64_t)*8 || (arg.ui64&1ULL<<lstr)==0) return RC_FALSE; break;
		case VT_BSTR: if ((uint32_t)lstr>=arg.length*8 || (arg.bstr[lstr/8]&1<<lstr%8)==0) return RC_FALSE; break;
		default: return RC_TYPE;
		}
		return RC_TRUE;
	case OP_CONTAINS:
	case OP_BEGINS:
	case OP_ENDS:
		rc=RC_FALSE;
	case OP_POSITION:
		switch (arg.type) {
		case VT_STREAM:
			// special implementation
		default: break;
		case VT_COLLECTION:
			if (op==OP_POSITION) {
#if 1
				return RC_TYPE;
#else
				if (moreArgs->type!=VT_COLLECTION) {freeV(arg); arg.set(-1);}
				if (arg.length==1) {/* replace with arg.varray[0]*/}
#endif
			} else if (!arg.isNav()) {
				for (len=arg.length; rc==RC_FALSE && len!=0; ) rc=calc(op,const_cast<Value&>(arg.varray[--len]),moreArgs,2,flags,ctx);
			} else {
				for (arg2=arg.nav->navigate(GO_FIRST); rc==RC_FALSE && arg2!=NULL; arg2=arg.nav->navigate(GO_NEXT)) {
					val=*arg2; setHT(val); rc=calc(op,val,moreArgs,2,flags,ctx); freeV(val);
				}
				arg.nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID);
			}
			return rc;
		}
		switch (moreArgs->type) {
		default: break;
		case VT_COLLECTION:
			if (op==OP_POSITION) {
#if 1
				return RC_TYPE;
#else
				//???
#endif
			} else if (!moreArgs->isNav()) {
				for (len=moreArgs->length; rc==RC_FALSE && len!=0; ) rc=calc(op,arg,&moreArgs->varray[len],2,flags,ctx);
			} else {
				for (arg2=moreArgs->nav->navigate(GO_FIRST); rc==RC_FALSE && arg2!=NULL; arg2=moreArgs->nav->navigate(GO_NEXT)) rc=calc(op,val,arg2,2,flags,ctx);
				moreArgs->nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID);
			}
			return rc;
		}
		if ((arg2=strOpConv(arg,moreArgs,val,ctx))==NULL) return RC_TYPE;
		if (arg.length>=arg2->length) switch (arg.type) {
		default: assert(0);
		case VT_STRING:
			if (op!=OP_CONTAINS && op!=OP_POSITION) {
				const char *p=op==OP_BEGINS?arg.str:arg.str+arg.length-arg2->length;
				if (((flags&CND_NCASE)!=0?strncasecmp(p,arg2->str,arg2->length):strncmp(p,arg2->str,arg2->length))==0) rc=RC_TRUE;
			} else {
				const char *p=arg.str,*q=NULL; char ch=0; if ((flags&CND_NCASE)!=0) ch=toupper(*arg2->bstr);
				for (len=arg.length-arg2->length+1; ;len-=unsigned(q-p)+1,p=q+1,q=NULL) {
					if (len!=0) {
						q=(char*)memchr(p,*arg2->str,len);
						if ((flags&CND_NCASE)!=0) {const char *q2=(char*)memchr(p,ch,len); if (q==NULL || q2!=NULL && q2<q) q=q2;}
					}
					if (q==NULL) {if (op==OP_POSITION) {freeV(arg); arg.set(-1);} break;}
					if (((flags&CND_NCASE)!=0?strncasecmp(q,arg2->str,arg2->length):strncmp(q,arg2->str,arg2->length))==0) 
						{if (op==OP_POSITION) {freeV(arg); arg.set((unsigned)(q-arg.str));} else rc=RC_TRUE; break;}
				}
			}
			break;
		case VT_BSTR:
			if (op!=OP_CONTAINS && op!=OP_POSITION) {
				if (memcmp(op==OP_BEGINS?arg.bstr:arg.bstr+arg.length-arg2->length,arg2->bstr,arg2->length)==0) rc=RC_TRUE;
			} else {
				const uint8_t *p=arg.bstr,*q;
				for (len=arg.length-arg2->length+1; ;len-=unsigned(q-p)+1,p=q+1)
					if (len==0 || (q=(uint8_t*)memchr(p,*arg2->bstr,len))==NULL) {if (op==OP_POSITION) {freeV(arg); arg.set(-1);} break;}
					else if (!memcmp(q,arg2->bstr,arg2->length)) {if (op==OP_POSITION) {freeV(arg); arg.set((unsigned)(q-arg.bstr));} else rc=RC_TRUE; break;}
			}
			break;
		} else if (op==OP_POSITION) {freeV(arg); arg.set(-1);}
		if (arg2==&val) freeV(val);
		return rc;
	case OP_SIMILAR:
		if (!isString((ValueType)arg.type) && (rc=convV(arg,arg,VT_STRING,ctx.ma))!=RC_OK) return rc;	// +OP_SUBSTR -> deref pin (strOpConv 1 arg)
		if ((flags&CND_CMPLRX)==0) {
			p=(byte*)moreArgs->bstr; len=moreArgs->length; assert(moreArgs->type==VT_BSTR && len>=sizeof(RegExpHdr));
		} else {
			arg2=moreArgs; Value w; rc=RC_OK;
			if (!isString((ValueType)moreArgs->type) && (arg2=&val,rc=convV(*moreArgs,val,VT_STRING,ctx.ma))!=RC_OK) return rc;
			try {RxCtx::parse(w,ctx.ma,arg2->bstr,arg2->length,0/*,opts,opts!=NULL?ptr-opts:0*/);}
			catch (RC rc2) {rc=rc2;} catch (SynErr) {rc=RC_SYNTAX;} catch (...) {rc=RC_INTERNAL;}
			if (rc==RC_OK) {p=(byte*)w.bstr; len=w.length; assert(w.type==VT_BSTR && w.length>=sizeof(RegExpHdr));}
			if (arg2==&val) freeV(val); if (rc!=RC_OK) return rc;
		}
		{RExpCtx rx(ctx.ma,(char*)arg.str,(arg.flags&HEAP_TYPE_MASK)>=SES_HEAP); 
		rc=MatchCtx::match(arg.str,arg.length,p,rx); 
		if ((flags&CND_CMPLRX)!=0) ctx.ma->free(p); arg.type=VT_ERROR;
		if ((rc==RC_OK || rc==RC_TRUE) && ctx.rctx!=NULL) *ctx.rctx=rx;}
		return rc;
	case OP_COUNT:
		if (moreArgs->type!=VT_URIID) return RC_TYPE;
		switch (arg.type) {
		default: return RC_TYPE;
		case VT_REF:
			pv=arg.pin->getValue(moreArgs->uid);
			if (pv==NULL && op==OP_PATH) return RC_NOTFOUND;
			freeV(arg);
			if (op==OP_PATH) {if ((rc=copyV(*pv,arg,ctx.ma))!=RC_OK) return rc;} else arg.set(pv==NULL?0u:pv->count());
			break;
		case VT_REFID:
			if (ctx.ses->getStore()->queryMgr->loadValue(ctx.ses,arg.id,moreArgs->uid,STORE_COLLECTION_ID,arg,LOAD_CARDINALITY)!=RC_OK) {
				if (op==OP_COUNT) {freeV(arg); arg.set(0u);} else return RC_FALSE;
			}
			break;
		}
		break;
	case OP_UPPER:
	case OP_LOWER:
		if (arg.type==VT_STRING||(rc=convV(arg,arg,VT_STRING,ctx.ma))==RC_OK) {
			s=forceCaseUTF8(arg.str,arg.length,len,ctx.ma,NULL,op==OP_UPPER); freeV(arg);
			if (s!=NULL) arg.set(s,len); else {arg.setError(); rc=RC_NOMEM;}
		}
		break;
	case OP_TONUM:
	case OP_TOINUM:
		switch (arg.type) {
		default: return RC_TYPE;
		case VT_STRING: return numOpConv(arg,op==OP_TOINUM?NO_INT:NO_INT|NO_FLT,ctx)?RC_OK:RC_TYPE;
		case VT_INT: case VT_UINT: case VT_INT64: case VT_UINT64: break;
		case VT_FLOAT: if (op==OP_TOINUM) {arg.type=VT_INT; arg.i=int(arg.f);} break;
		case VT_DOUBLE: if (op==OP_TOINUM) {arg.type=VT_INT64; arg.i64=int64_t(arg.d);} break;
		case VT_BOOL: arg.type=VT_INT; arg.i=arg.b?1:0; break;
		case VT_DATETIME: arg.type=VT_UINT64; break;
		case VT_INTERVAL: arg.type=VT_INT64; break;
		}
		break;
	case OP_CAST:
		if (nargs==2) {
			if (moreArgs->type!=VT_UINT && moreArgs->type!=VT_INT) return RC_TYPE;		// conv???
			flags=moreArgs->ui;
		}
		if ((rc=convV(arg,arg,flags,ctx.ma))!=RC_OK) return rc;
		break;
	case OP_RANGE:
		if (arg.type!=(arg2=moreArgs)->type && arg.type!=VT_ANY && arg2->type!=VT_ANY) {
			/* ??? */ return RC_TYPE;
		}
		if ((rng=new(ctx.ma) Value[2])==NULL) return RC_NOMEM;
		rng[0]=arg;
		if (arg2==&val) rng[1]=val;
		else if ((rc=copyV(*arg2,rng[1],ctx.ma))!=RC_OK) {ctx.ma->free(rng); return rc;}
		rng[0].property=rng[1].property=STORE_INVALID_URIID;
		rng[0].eid=rng[1].eid=STORE_COLLECTION_ID;
		arg.setRange(rng); arg.flags=ctx.ma->getAType(); break;
	case OP_COLLECTION:
		if ((rc=ExprNode::normalizeCollection(&arg,nargs,val,ctx.ses,ctx.ses->getStore()))!=RC_OK) return rc;
		arg=val; assert(arg.type==VT_COLLECTION); break;
	case OP_STRUCT: case OP_PIN:
		if (nargs==1) switch (arg.type) {
		case VT_REF:
		case VT_REFID:
		case VT_STRUCT:
		case VT_COLLECTION:
			//???
			break;
		}
		if ((rc=ExprNode::normalizeStruct(ctx.ses,&arg,nargs,val,ctx.ses,op==OP_PIN))!=RC_OK) return rc;
		if (val.type==VT_EXPRTREE) {freeV(val); return RC_TYPE;}
		arg=val; break;
	case OP_EXTRACT:
		if (nargs==2) {
			if (moreArgs->type!=VT_INT && moreArgs->type!=VT_UINT) return RC_TYPE;
			flags=moreArgs->ui;
		}
		if (flags==EY_IDENTITY || flags==EY_STOREID) {
			if (arg.type!=VT_REFID && (rc=convV(arg,arg,VT_REFID,ctx.ma))!=RC_OK) return rc;
			if (flags==EY_IDENTITY) arg.setIdentity(arg.id.ident); else arg.set((unsigned)(arg.id.pid>>48));
		} else {
			if (arg.type!=VT_DATETIME && (rc=convV(arg,arg,VT_DATETIME,ctx.ma))!=RC_OK
				|| (rc=Session::getDTPart(arg.ui64,dtPart,flags))!=RC_OK) return rc;
			arg.set(dtPart);
		}
		break;
	case OP_BITFIELD:
		if ((rc=getI(moreArgs[0],lstr,ctx))!=RC_OK) return rc; if (lstr<0) return RC_INVPARAM; sht=(uint32_t)lstr;
		if ((rc=getI(moreArgs[1],lstr,ctx))!=RC_OK) return rc; if (lstr<=0) return RC_INVPARAM; len=(uint32_t)lstr;
		switch (arg.type) {
		case VT_INT: case VT_UINT:
			if (sht+len>sizeof(uint32_t)*8) return RC_TOOBIG; arg.ui=(arg.ui>>sht)&(1<<len)-1; break;
		case VT_INT64: case VT_UINT64:
			if (sht+len>sizeof(uint64_t)*8) return RC_TOOBIG; arg.ui64=(arg.ui64>>sht)&(1ULL<<len)-1; break;
		case VT_BSTR:
			if (sht>=arg.length*8) {freeV(arg); arg.set(0u);}
			else if (len<=sizeof(uint64_t)*8) {
				const byte *p=&arg.bstr[sht/8]; sht%=8; uint32_t l,s2=0; uint64_t u64=0ULL;
				do {l=min(8-sht,len); u64|=(*p++>>sht&(1ULL<<l)-1)<<s2; sht=0; s2+=l;} while ((len-=l)!=0);
				freeV(arg); if ((u64&0xFFFFFFFF00000000ULL)==0) arg.set((unsigned)u64); else arg.setU64(u64);
			} else return RC_TOOBIG;
		}
		break;
	case OP_DATAEVENTS:
		if (arg.type==VT_REFID) {
			DetectedEvents clr(ctx.ses,ctx.ses->getStore()); PINx pex(ctx.ses,arg.id); 
			if ((rc=pex.getBody())!=RC_OK) return rc;
			if ((rc=ctx.ses->getStore()->classMgr->detect(&pex,clr,ctx.ses))!=RC_OK) return rc;
			if (clr.ndevs==0) arg.setError();
			else if ((args=new(ctx.ma) Value[clr.ndevs])==NULL) return RC_NOMEM;
			else {
				for (unsigned i=0; i<clr.ndevs; i++) {args[i].setURIID(clr.devs[i]->cid); args[i].eid=i;}
				arg.set(args,clr.ndevs); arg.flags=ctx.ma->getAType();
			}
		} else if (arg.type!=VT_REF) return RC_TYPE;
		else if ((((PIN*)arg.pin)->getMode()&PIN_HIDDEN)!=0) freeV(arg);
		else {
			DetectedEvents clr(ctx.ses,ctx.ses->getStore());
			if ((rc=ctx.ses->getStore()->classMgr->detect((PIN*)arg.pin,clr,ctx.ses))!=RC_OK) return rc;
			freeV(arg);
			if (clr.ndevs!=0) {
				if ((args=new(ctx.ma) Value[clr.ndevs])==NULL) return RC_NOMEM;
				else {
					for (unsigned i=0; i<clr.ndevs; i++) {args[i].setURIID(clr.devs[i]->cid); args[i].eid=i;}
					arg.set(args,clr.ndevs); arg.flags=ctx.ma->getAType();
				}
			}
		}
		break;
	default:
		return RC_INVOP;
	}
	return RC_OK;
}

RC AggAcc::next(const Value& v)
{
	RC rc=RC_OK; assert(!v.isEmpty()); count++;
	if (op==OP_HISTOGRAM) {
		assert(hist!=NULL); Value *pv=NULL;
		switch (hist->add(v,&pv)) {
		case SLO_ERROR: rc=RC_NOMEM; break;
		case SLO_NOOP: break;
		case SLO_INSERT:
			if ((rc=copyV(v,*pv,ma))==RC_OK) {pv->property=0; pv->eid=1;}
			break;
		default: assert(0);
		}
	} else if (sum.isEmpty()) {
		*(Value*)&sum=v; setHT(sum); count=1;
		if (op==OP_SUM) {
			if (!isNumeric((ValueType)sum.type) && sum.type!=VT_INTERVAL && !Expr::numOpConv(sum,NO_INT|NO_FLT|NO_ITV1,*ctx)) sum.type=VT_ERROR;
		} else if (op>=OP_AVG) {
			if ((op!=OP_AVG || sum.type!=VT_DATETIME) && sum.type!=VT_DOUBLE && (rc=convV(sum,sum,VT_DOUBLE,ma))!=RC_OK) sum.type=VT_ERROR;
		} else if (op==OP_CONCAT && !isString((ValueType)v.type)) {
			if ((rc=convV(v,sum,VT_STRING,ma))!=RC_OK) sum.type=VT_ERROR;
		} else {
			if ((rc=copyV(v,sum,ma))==RC_OK) eid=v.eid==STORE_COLLECTION_ID?0:v.eid; else sum.type=VT_ERROR;
		}
	} else if (op==OP_MIN || op==OP_MAX || op==OP_ARGMIN || op==OP_ARGMAX) {
		int c=cmp(sum,v,flags,ma);
		if (c==-1 && (op==OP_MAX||op==OP_ARGMAX) || c==1 && (op==OP_MIN||op==OP_ARGMIN)) {freeV(sum); rc=copyV(v,sum,ma); eid=v.eid==STORE_COLLECTION_ID?(ElementID)count-1:v.eid;}
	} else if (op==OP_CONCAT) {
		assert(isString((ValueType)sum.type)); const Value *pv=&v; Value w;
		if ((v.type==sum.type || (pv=&w,rc=convV(v,w,sum.type,ma))==RC_OK) && pv->length!=0) {
			sum.bstr=(byte*)ma->realloc((byte*)sum.bstr,sum.length+pv->length+(sum.type==VT_BSTR?0:1),sum.length);
			if (sum.bstr==NULL) rc=RC_NOMEM;
			else {memcpy((byte*)sum.bstr+sum.length,pv->bstr,pv->length); sum.length+=pv->length; if (sum.type!=VT_BSTR) ((char*)sum.str)[sum.length]='\0';}
		}
		if (pv==&w) freeV(w);
	} else if (op!=OP_COUNT) {
		const Value *pv=&v; Value val;
		try {
			if (pv->type!=sum.type && (pv=Expr::numOpConv(sum,pv,val,NO_INT|NO_FLT|NO_ITV1|NO_DAT1|NO_DAT2|NO_ITV2,*ctx))==NULL) rc=RC_TYPE;
			else {
				switch (sum.type) {
				default: return RC_TYPE;
				case VT_INT: sum.i+=pv->i; assert(op==OP_SUM); break;
				case VT_UINT: sum.ui+=pv->ui; assert(op==OP_SUM); break;
				case VT_INT64: case VT_INTERVAL: sum.i64+=pv->i64; assert(op==OP_SUM); break;
				case VT_UINT64: case VT_DATETIME: sum.ui64+=pv->ui64; assert(op==OP_SUM||op==OP_AVG); break;
				case VT_FLOAT: sum.f+=pv->f; assert(op==OP_SUM); break;
				case VT_DOUBLE:	sum.d+=pv->d; if (op>=OP_VAR_POP) sum2+=pv->d*pv->d; break;
				}
			}
		} catch (...) {rc=RC_TOOBIG;}
	}
	return rc;
}

RC AggAcc::result(Value& res,bool fRestore)
{
	try {
		if (op==OP_COUNT) res.setU64(count);
		else if (count==0) res.setError();
		else switch (op) {
		case OP_HISTOGRAM:
			assert(hist!=NULL);
			{unsigned cnt=(unsigned)hist->getCount(); hist->start(); RC rc;
			Value *pv=new(ma) Value[cnt*3]; if (pv==NULL) return RC_NOMEM;
			for (unsigned i=0; i<cnt; i++) {
				const Value *vc=hist->next(); if (vc==NULL) {freeV(pv,i,ma); return RC_CORRUPTED;}
				if ((rc=copyV(*(const Value*)vc,pv[cnt+i*2],ma))!=RC_OK) return rc;	pv[cnt+i*2].setPropID(PROP_SPEC_VALUE);	// cleanup?
				pv[cnt+i*2+1].setU64(uint64_t(vc->property)<<32|vc->eid); pv[cnt+i*2+1].setPropID(PROP_SPEC_COUNT);
				pv[i].setStruct(&pv[cnt+i*2],2); pv[i].eid=i;
			}
			res.set(pv,cnt); res.flags=ma->getAType();
			unsigned flags=hist->getExtra(); hist->~Histogram(); ma->free(hist);		// cleanup!!!
			hist=fRestore?new(ma) Histogram(*ma,flags):NULL;}
			break;
		case OP_MIN: case OP_MAX: case OP_SUM: case OP_CONCAT:
			res=sum; sum.type=VT_ERROR; setHT(sum); break;
		case OP_ARGMIN: case OP_ARGMAX:
			res.set((unsigned)eid); freeV(sum); break;
		case OP_AVG:
			if (sum.type==VT_DATETIME) res.setDateTime(sum.ui64/count);
			else {assert(sum.type==VT_DOUBLE); res.set(sum.d/(double)count);}
			break;
		case OP_VAR_POP: case OP_VAR_SAMP: case OP_STDDEV_POP: case OP_STDDEV_SAMP:
			if (count==1 && (op==OP_VAR_SAMP || op==OP_STDDEV_SAMP)) res.setError();
			else {
				assert(sum.type==VT_DOUBLE);
				sum2-=sum.d*sum.d/(double)count;
				sum2/=op==OP_VAR_POP||op==OP_STDDEV_POP?(double)count:(double)(count-1);
				if (op>=OP_STDDEV_POP) {assert(sum2>=0.); sum2=sqrt(sum2);}
				res.set(sum2);
			}
			break;
		default:
			res.setError(); return RC_INTERNAL;
		}
		count=0; sum.setError(); sum2=0.;
	} catch (...) {res.setError(); return RC_TOOBIG;}
	return RC_OK;
}

RC AggAcc::process(const Value &v)
{
	unsigned i; const Value *cv; RC rc;
	switch (v.type) {
	case VT_ERROR: if (op==OP_COUNT) count++; break;
	case VT_COLLECTION:
		if (op==OP_COUNT && (flags&CND_DISTINCT)==0) count+=v.count();
		else if (!v.isNav()) {
			for (i=0; i<v.length; i++) if ((flags&CND_DISTINCT)!=0) {
				//...
			} else if ((rc=next(v.varray[i]))!=RC_OK) return rc;
		} else {
			for (cv=v.nav->navigate(GO_FIRST); cv!=NULL; cv=v.nav->navigate(GO_NEXT)) if ((flags&CND_DISTINCT)!=0) {
				//...
			} else if ((rc=next(*cv))!=RC_OK) return rc;
		}
		break;
	case VT_ARRAY:
		//????
		break;
	case VT_STMT:
		if (op==OP_COUNT && (flags&CND_DISTINCT)==0) {uint64_t cnt=0ULL; if (v.stmt->count(cnt)==RC_OK) count+=cnt;}
		else {
			RC rc; ICursor *ic=NULL; //const Value *vals; unsigned nVals;
			if ((rc=((Stmt*)v.stmt)->execute(&ic))!=RC_OK) return rc;
#if 0
			while ((rc=((Cursor*)ic)->next(vals,nVals))==RC_OK) if (vals!=NULL && nVals!=0) {
				if ((flags&CND_DISTINCT)!=0) {
					//...
				} else if ((rcaa.next(vals[0]))!=RC_OK) return rc;
			}
#endif
			ic->destroy(); if (rc!=RC_EOF) return rc;
		}
		break;
	default:
		v.eid=STORE_COLLECTION_ID;
		if ((flags&CND_DISTINCT)!=0) {
			//...
		} else if (op==OP_COUNT) count++; else if ((rc=next(v))!=RC_OK) return rc;
		break;
	}
	return RC_OK;
}

RC Expr::calcAgg(ExprOp op,Value& res,const Value *more,int nargs,unsigned flags,const EvalCtx& ctx)
{
	Histogram *h=NULL;
	if (op==OP_HISTOGRAM) { 
		if ((h=new(ctx.ma) Histogram(*ctx.ma,flags))==NULL) return RC_NOMEM;
		flags&=~CND_DISTINCT;
	} else if ((flags&CND_DISTINCT)!=0) {
		//...
	}
	AggAcc aa(op,flags,&ctx,h); const Value *pv=&res; RC rc;
	for (int idx=0; idx<nargs; pv=&more[idx++]) if ((rc=aa.process(*pv))!=RC_OK) return rc;
	if ((flags&CND_DISTINCT)!=0) {
		// sort
		// feed results to aa
	}
	freeV(res); return aa.result(res);
}

RC Expr::getI(const Value& v,long& num,const EvalCtx& ctx)
{
	RC rc; Value val; const char *s;
	for (const Value *pv=&v;;) {
		HEAP_TYPE allc=(HEAP_TYPE)(pv->flags&HEAP_TYPE_MASK);
		bool fFree=pv==&val && allc>=SES_HEAP; 
		switch (pv->type) {
		default: return RC_TYPE;
		case VT_STRING: 
			if ((rc=Session::strToNum(s=pv->str,pv->length,val))!=RC_OK) return rc;
			if (fFree) free((char*)s,allc); pv=&val; continue;
		case VT_INT: num=pv->i; break;
		case VT_UINT: num=long(pv->ui); break;
		case VT_INT64: num=long(pv->i64); break;
		case VT_UINT64: num=long(pv->ui64); break;
		case VT_FLOAT: num=long(pv->f); break;
		case VT_DOUBLE: num=long(pv->d); break;
		case VT_REF: case VT_REFID: case VT_REFPROP:
		case VT_REFELT: case VT_REFIDPROP: case VT_REFIDELT:
			if (pv==&val) return RC_INVPARAM;
			if ((rc=derefValue(*pv,val,ctx.ses))!=RC_OK) return rc;
			pv=&val; continue;
		case VT_COLLECTION: 
			if (pv->length!=1) return RC_TYPE;
			if (fFree) decoll(val); else pv=pv->varray;
			continue;
		}
		if (fFree) freeV(val);
		return RC_OK;
	}
}

const Value *Expr::strOpConv(Value& arg,const Value *arg2,Value& buf,const EvalCtx& ctx)
{
	for (;;) {
		if (isRef((ValueType)arg.type)) {if (derefValue(arg,arg,ctx.ses)!=RC_OK) return NULL;}
		else if (arg.type==VT_COLLECTION && arg.length==1) decoll(arg); else break;
	}
	for (;;arg2=&buf) {
		if (isRef((ValueType)arg2->type)) {if (derefValue(*arg2,buf,ctx.ses)!=RC_OK) return NULL;}
		else if (arg2->type!=VT_COLLECTION || arg2->length!=1) break;
		else {if (arg2!=&buf) {buf=*arg2; setHT(buf);} decoll(buf);}
	}
	ValueType vt1=(ValueType)arg.type,vt2=(ValueType)arg2->type;
	if (isString(vt1)) {
		if (isString(vt2)) {
			if (vt2==VT_BSTR) arg.type=VT_BSTR;
		} else {
			if (convV(*arg2,buf,vt1,ctx.ses)!=RC_OK) return NULL;
			arg2=&buf;
		}
	} else if (isString(vt2)) {
		if (convV(arg,arg,vt2,ctx.ses)!=RC_OK) return NULL;
	} else {
		if (convV(arg,arg,VT_STRING,ctx.ses)!=RC_OK || convV(*arg2,buf,VT_STRING,ctx.ses)!=RC_OK) return NULL;
		arg2=&buf;
	}
	return arg2;
}

const Value *Expr::numOpConv(Value& arg,const Value *arg2,Value& buf,unsigned flg,const EvalCtx& ctx)
{
	for (;;) {
		if (isRef((ValueType)arg.type)) {if (derefValue(arg,arg,ctx.ses)!=RC_OK) return NULL;}
		else if (arg.type==VT_COLLECTION && arg.length==1) decoll(arg); else break;
	}
	if (arg.type==VT_STRING) {
		if (Session::strToNum(arg.str,arg.length,buf)!=RC_OK) return NULL;
		freeV(arg); arg=buf;
	}
	for (;;arg2=&buf) {
		if (isRef((ValueType)arg2->type)) {if (derefValue(*arg2,buf,ctx.ses)!=RC_OK) return NULL;}
		else if (arg2->type!=VT_COLLECTION || arg2->length!=1) break;
		else {if (arg2!=&buf) {buf=*arg2; setHT(buf);} decoll(buf);}
	}
	if (arg2->type==VT_STRING) {
		const char *p=arg2->str; if (Session::strToNum(p,arg2->length,buf)!=RC_OK) return NULL;
		if (arg2!=&buf) arg2=&buf; else if ((arg2->flags&HEAP_TYPE_MASK)>=SES_HEAP) free((void*)p,(HEAP_TYPE)(arg2->flags&HEAP_TYPE_MASK));
	}
	switch (arg.type) {
	case VT_DATETIME: 
		if ((flg&NO_DAT1)!=0) {
			if (arg2->type==VT_DATETIME && (flg&NO_DAT2)!=0 || arg2->type==VT_INTERVAL && (flg&NO_ITV2)!=0) return arg2;
			if ((flg&NO_ITV2)!=0 && isInteger((ValueType)arg2->type)) {buf=*arg2; buf.type=VT_INTERVAL; return &buf;}
		}
		return NULL;
	case VT_INTERVAL: 
		if ((flg&NO_ITV1)!=0 && (flg&NO_ITV2)!=0) {
			if (arg2->type==VT_INTERVAL) return arg2;
			if (isInteger((ValueType)arg2->type)) {buf=*arg2; buf.type=VT_INTERVAL; return &buf;}
		}
		return NULL;
	default: if (!isNumeric((ValueType)arg.type)) return NULL; break;
	}
	if (!isNumeric((ValueType)arg2->type)) return NULL;
	if (arg.type!=arg2->type) {
		const Value *pm,*px; if (arg.type<arg2->type) pm=&arg,px=arg2; else pm=arg2,px=&arg;
		switch (px->type) {
		default: assert(0);
		case VT_ENUM: return NULL;
		case VT_FLOAT: case VT_DOUBLE:
			if ((flg&NO_FLT)==0) {
				// cv to int without truncation
				return NULL;
			}
			if (arg.type!=VT_DOUBLE) {if (arg.type==VT_FLOAT) arg.d=arg.f; else {if (arg.type==VT_INT) arg.d=arg.i; else if (arg.type==VT_UINT) arg.d=arg.ui; else if (arg.type==VT_INT64) arg.d=(double)arg.i64; else arg.d=(double)arg.ui64; arg.qval.units=Un_NDIM;} arg.type=VT_DOUBLE;}
			if (arg2->type!=VT_DOUBLE) {if (arg2->type==VT_FLOAT) buf.d=arg2->f,buf.qval.units=arg2->qval.units; else {if (arg2->type==VT_INT) buf.d=arg2->i; else if (arg2->type==VT_UINT) buf.d=arg2->ui; else if (arg2->type==VT_INT64) buf.d=(double)arg2->i64; else buf.d=(double)arg2->ui64; buf.qval.units=Un_NDIM;} buf.type=VT_DOUBLE; arg2=&buf;}
			break;
		case VT_UINT64:
			if (pm->type==VT_INT && pm->i<0 || pm->type==VT_INT64 && pm->i64<0) {
				if (px->ui64>=0x8000000000000000ULL) return NULL;
				if (arg.type!=VT_INT64) {arg.i64=arg.type==VT_INT?(int64_t)arg.i:arg.type==VT_UINT?(int64_t)arg.ui:(int64_t)arg.ui64; arg.type=VT_INT64;}
				if (arg2->type!=VT_INT64) {buf.i64=arg2->type==VT_INT?(int64_t)arg2->i:arg2->type==VT_UINT?(int64_t)arg2->ui:(int64_t)arg2->ui64; buf.type=VT_INT64; arg2=&buf;}
			} else if (pm==arg2) {buf.ui64=arg2->type==VT_INT?(uint64_t)arg2->i:arg2->type==VT_UINT?(uint64_t)arg2->ui:(uint64_t)arg2->i64; buf.type=VT_UINT64; arg2=&buf;}
			else {arg.ui64=arg.type==VT_INT?(uint64_t)arg.i:arg.type==VT_UINT?(uint64_t)arg.ui:(uint64_t)arg.i64; arg.type=VT_UINT64;}
			break;
		case VT_INT64:
			if (pm==&arg) {arg.i64=arg.type==VT_UINT?(int64_t)arg.ui:(int64_t)arg.i; arg.type=VT_INT64;}
			else {buf.i64=arg2->type==VT_UINT?(int64_t)arg2->ui:(int64_t)arg2->i; buf.type=VT_INT64; arg2=&buf;}
			break;
		case VT_UINT:
			assert(pm->type==VT_INT);
			if (pm->i<0) {
				arg.i64=arg.type==VT_UINT?(int64_t)arg.ui:(int64_t)arg.i; arg.type=VT_INT64;
				buf.i64=arg2->type==VT_UINT?(int64_t)arg2->ui:(int64_t)arg2->i; buf.type=VT_INT64; arg2=&buf;
			} else if (pm==arg2) {buf.ui=pm->i; buf.type=VT_UINT; arg2=&buf;} else arg.type=VT_UINT;
			break;
		}
	}
	if ((flg&NO_FLT)==0 && !isInteger((ValueType)arg.type)) {
		if (arg2!=&buf) {buf=*arg2; arg2=&buf;}
		switch (arg.type) {
		default: assert(0);
		case VT_FLOAT: arg.i=(int32_t)arg.f; buf.i=(int32_t)buf.f; arg.type=buf.type=VT_INT; break;				// check truncation?
		case VT_DOUBLE: arg.i64=(int64_t)arg.d; buf.i64=(int64_t)buf.d; arg.type=buf.type=VT_INT64; break;
		}
	}
	return arg2;
}

bool Expr::numOpConv(Value& arg,unsigned flg,const EvalCtx& ctx)
{
	Value val;
	for (;;) switch (arg.type) {
	default: return false;
	case VT_INT: case VT_UINT: case VT_INT64: case VT_UINT64: return true;
	case VT_FLOAT: if ((flg&NO_FLT)==0) {arg.i=(int32_t)arg.f; arg.type=VT_INT;} return true;
	case VT_DOUBLE: if ((flg&NO_FLT)==0) {arg.i64=(int64_t)arg.d; arg.type=VT_INT64;} return true;
	case VT_STRING:
		if (Session::strToNum(arg.str,arg.length,val)!=RC_OK) return false;
		freeV(arg); arg=val; if ((flg&NO_FLT)==0 && arg.type>=VT_FLOAT) continue;
		return true;
	case VT_REF: case VT_REFID: case VT_REFPROP: case VT_REFELT: case VT_REFIDPROP: case VT_REFIDELT:
		if (derefValue(arg,arg,ctx.ses)==RC_OK) continue;
		return false;
	case VT_COLLECTION:
		if (arg.length!=1) return false; decoll(arg); continue;
	}
}

RC Expr::expandStr(Value& v,uint32_t newl,MemAlloc *ma,bool fZero)
{
	assert(isString((ValueType)v.type) && newl>=v.length);
	if ((v.flags&HEAP_TYPE_MASK)<SES_HEAP) {
		uint8_t *p=(byte*)ma->malloc(newl); if (p==NULL) return RC_NOMEM;
		memcpy(p,v.bstr,v.length); v.bstr=p;
	} else {
		if ((v.bstr=(uint8_t*)ma->realloc((byte*)v.bstr,newl,v.length))==NULL) return RC_NOMEM;
	}
	if (fZero) memset((byte*)v.bstr+v.length,0,newl-v.length);
	v.length=newl; return RC_OK;
}

#define	HASH_PROPERTY		0x0001
#define	HASH_EID			0x0002

void Expr::hashValue(const Value &v,unsigned flags,SHA256& sha)
{
	uint32_t i; const Value *cv; PIN *pin;
	switch (v.type) {
	case VT_STRING: 
	case VT_BSTR: sha.add(v.bstr,v.length); break;
	case VT_BOOL: sha.add((byte*)&v.b,1); break;
	case VT_REF:
		pin=(PIN*)v.pin;
		if (pin->id.isPID()) sha.add((byte*)&pin->id,sizeof(PID));
		for (i=0; i<pin->nProperties; i++) hashValue(pin->properties[i],HASH_PROPERTY,sha);
		sha.add((byte*)&pin->properties,sizeof(pin->properties));
		i=pin->mode&(PIN_NO_REPLICATION|PIN_REPLICATED|PIN_HIDDEN|PIN_PERSISTENT|PIN_TRANSIENT|PIN_IMMUTABLE|PIN_DELETED);
		sha.add((byte*)&i,sizeof(i)); sha.add((byte*)&pin->meta,sizeof(pin->meta)); break;
	case VT_REFID: sha.add((byte*)&v.id,sizeof(PID)); break;
	case VT_COLLECTION:
		if (v.isNav()) {
			for (cv=v.nav->navigate(GO_FIRST),i=0; cv!=NULL; cv=v.nav->navigate(GO_NEXT),i++) hashValue(*cv,HASH_EID,sha);
		} else 
			for (i=0; i<v.length; i++) hashValue(v.varray[i],HASH_EID,sha);
		sha.add((byte*)&i,sizeof(uint32_t)); break;
	case VT_STRUCT:
		for (i=0; i<v.length; i++) hashValue(v.varray[i],HASH_PROPERTY,sha);
		sha.add((byte*)&v.length,sizeof(uint32_t)); break;
	case VT_RANGE:
		hashValue(v.varray[0],0,sha); hashValue(v.varray[1],0,sha); break;
	case VT_ARRAY:
		//????
		break;
	case VT_MAP:
		//????
		break;
	case VT_STREAM:
		//????
		break;
	case VT_REFPROP:
	case VT_REFIDPROP:
	case VT_REFELT:
	case VT_REFIDELT:
	case VT_EXPR:
	case VT_STMT:
	case VT_CURRENT:
	case VT_VARREF:
		//???
		break;
	case VT_DOUBLE: case VT_FLOAT:
		sha.add((byte*)&v.qval.units,sizeof(v.qval.units));
	default:
		if ((i=typeInfo[v.type].length)!=0) sha.add((byte*)&v.i,i);
		break;
	}
	if ((flags&HASH_PROPERTY)!=0) {sha.add((byte*)&v.property,sizeof(PropertyID)); sha.add((byte*)&v.meta,1);}
	if ((flags&HASH_EID)!=0) sha.add((byte*)&v.eid,sizeof(ElementID));
}

RC Expr::calcHash(ExprOp op,Value& res,const Value *more,int nargs,unsigned,const EvalCtx& ctx)
{
	SHA256 sha; assert(op==OP_HASH);
	hashValue(res,0,sha); freeV(res);
	if (nargs==2) {
		if (more->type!=VT_IDENTITY) return RC_TYPE;
		if (more->iid==STORE_OWNER) sha.add(ctx.ses->getStore()->theCB->encKey,ENC_KEY_SIZE);
		else {
			//???
		}
	}
	byte *r=new(ctx.ma) byte[SHA_DIGEST_BYTES]; if (r==NULL) return RC_NOMEM;
	memcpy(r,sha.result(),SHA_DIGEST_BYTES); res.set(r,SHA_DIGEST_BYTES); setHT(res,ctx.ma->getAType());
	return RC_OK;
}
