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

#include "expr.h"
#include "queryprc.h"
#include "stmt.h"
#include "ftindex.h"
#include "parser.h"
#include "blob.h"
#include "maps.h"
#include <math.h>
#include <stdio.h>

using namespace AfyKernel;

const byte Expr::compareCodeTab[] = {2,5,1,3,4,6};

static const byte cndAct[8][2] = {{0,1},{1,0},{0,2},{2,0},{2,1},{1,2},{2,3},{3,2}};

RC Expr::execute(Value& res,const Value *params,unsigned nParams) const
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION;
		if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		const Expr *exp=this; res.setError(); ValueV vv(params,nParams);
		RC rc=eval(&exp,1,res,NULL,0,&vv,1,ses);
		switch (rc) {
		default: return rc;
		case RC_TRUE: res.set(true); return RC_OK;
		case RC_FALSE: res.set(false); return RC_OK;
		}
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IExpr::execute()\n"); return RC_INTERNAL;}
}

bool Expr::condSatisfied(const Expr *const *exprs,ulong nExp,PINEx **vars,unsigned nVars,const ValueV *pars,unsigned nPars,MemAlloc *ma,bool fIgnore)
{
	try {
		Value res; 
		if (exprs==NULL || nExp==0 || exprs[0]==NULL) return true;
		switch (Expr::eval(exprs,nExp,res,vars,nVars,pars,nPars,ma,fIgnore)) {
		case RC_TRUE: return true;
		case RC_FALSE: return false;
		case RC_OK: if (res.type==VT_BOOL) return res.b;
		default: break;
		}
	} catch (...) {}
	return false;
}

RC Expr::eval(const Expr *const *exprs,ulong nExp,Value& result,PINEx **vars,ulong nVars,const ValueV *params,ulong nParams,MemAlloc *ma,bool fIgnore)
{
	if (nExp==1 && (exprs[0]->hdr.flags&EXPR_EXTN)!=0) {
		const void *itf=NULL;
		if (exprs[0]->hdr.lStack>=nExtns || (itf=extnTab[exprs[0]->hdr.lStack])==NULL) return RC_INVOP;
		// call external
		return RC_INTERNAL;
	}
	ulong idx=0; const Expr *exp; assert(exprs!=NULL && nExp>0 && exprs[0]!=NULL); Session *ses=NULL;
	const byte *codePtr=NULL,*codeEnd=NULL; ulong cntCatch=0,lStack=0; Value *stack=NULL,*top=NULL; VarD vds[4],*evds=NULL;
	for (;;) {
		if (codePtr>=codeEnd) {
			if (codePtr>codeEnd || top!=stack && top!=stack+1) return RC_INTERNAL;
			if (idx>=nExp) {if (top==stack) return RC_TRUE; result=top[-1]; return result.type!=VT_ERROR?RC_OK:RC_NOTFOUND;}
			if ((exp=exprs[idx++])==NULL || (params==NULL||nParams==0||params[0].nValues==0) && fIgnore && (exp->hdr.flags&EXPR_PARAMS)!=0) continue;
			if ((exp->hdr.flags&EXPR_NO_CODE)!=0) {
				const VarHdr *vh=(VarHdr*)(&exp->hdr+1); assert(exp->hdr.nVars==1);
				if (vh->var>=nVars || !vars[vh->var]->defined((PropertyID*)(vh+1),vh->nProps)) return RC_FALSE;
				continue;
			}
			codePtr=(const byte*)(&exp->hdr+1)+exp->hdr.lProps; codeEnd=(const byte*)&exp->hdr+exp->hdr.lExpr; 
			cntCatch=(exp->hdr.flags&EXPR_BOOL)!=0?1:0; if (top==stack+1) freeV(*--top);
			vds[0].fInit=vds[1].fInit=vds[2].fInit=vds[3].fInit=false;
			if (evds!=NULL) {
				//...
			}
			if (exp->hdr.lStack>lStack) {
				if ((stack=(Value*)alloca((exp->hdr.lStack-lStack)*sizeof(Value)))==NULL) return RC_NORESOURCES;
				top=stack; lStack=exp->hdr.lStack;
			}
		}
		assert(top>=stack && top<=stack+exp->hdr.lStack);
		TIMESTAMP ts; const Value *v; RC rc=RC_OK; VarD *vd; ElementID eid;
		byte op=*codePtr++; const bool ff=(op&0x80)!=0; int nops; unsigned fop; uint32_t u,vdx;
		switch (op&=0x7F) {
		case OP_CON:
			assert(top<stack+exp->hdr.lStack); top->flags=0;
			if ((rc=AfyKernel::deserialize(*top,codePtr,codeEnd,ma,true))!=RC_OK) break;
			assert(top->type!=VT_VARREF && top->type!=VT_CURRENT);
			top++; break;
		case OP_CONID:
			if ((u=*codePtr++)==0xFF) {u=codePtr[0]|codePtr[1]<<8; codePtr+=2;}
			if (!ff && vds[0].fInit) u=vds[0].props[u];
			else {
				const VarHdr *vh=(VarHdr*)(&exp->hdr+1),*vend=(VarHdr*)((byte*)vh+exp->hdr.lProps);
				for (vdx=ff?0xffff:0; vh<vend && vh->var!=vdx; vh=(VarHdr*)((byte*)(vh+1)+vh->nProps*sizeof(uint32_t)));
				u=((uint32_t*)(vh+1))[u]; if (!ff) {vds[0].props=(PropertyID*)(vh+1); vds[0].fInit=true; vds[0].fLoaded=false;}
			}
			if (ff) top->setIdentity(u); else top->setURIID(u&STORE_MAX_URIID);
			top++; break;
		case OP_PARAM:
			if ((u=*codePtr++)!=0xFF) vdx=u>>6,u&=0x3F; else {vdx=codePtr[0]; u=codePtr[1]; codePtr+=2;}
			if (vdx>=nParams || u>=params[vdx].nValues) rc=RC_NOTFOUND;
			else {
				v=&params[vdx].vals[u];
				if (v->type==VT_VARREF) {
					//u=v->refV.refN; if (v->length==0) goto var; propID=v->refV.id; eid=v->eid; goto prop;
				}
			}
			if (ff) top->set(unsigned(rc!=RC_OK?(rc=RC_OK,0u):v->type==VT_ARRAY?v->length:v->type==VT_COLLECTION?v->nav->count():1u)); 
			else if (rc==RC_OK) {*top=*v; top->flags=NO_HEAP;} else {top->setError(); if (cntCatch!=0) rc=RC_OK;}
			top++; break;
		case OP_VAR:
			if ((u=*codePtr++)<nVars && vars[u]->load()==RC_OK) {if (ff) top->set(1u); else top->set((PIN*)vars[u]);}
			else if (ff) top->set(0u); else {top->setError(); if (cntCatch==0) rc=RC_NOTFOUND;}
			top++; break;
		case OP_PROP: case OP_ELT: case OP_SETPROP:
			if ((u=*codePtr++)!=0xff) vd=&vds[vdx=u>>6],u&=0x3f;
			else {
				if ((vdx=codePtr[0])>=nVars) {/*...*/}
				if (vdx<4) vd=&vds[vdx]; else {/*???*/ vd=NULL;}
				u=codePtr[1]|codePtr[2]<<8; codePtr+=3;
			}
			if (!vd->fInit) {
				if (vdx>=nVars) {if (ff) (top++)->set(0u); else if (cntCatch!=0) (top++)->setError(); else rc=RC_NOTFOUND; break;}
				const VarHdr *vh=(VarHdr*)(&exp->hdr+1),*vend=(VarHdr*)((byte*)vh+exp->hdr.lProps);
				while (vh<vend && vh->var!=vdx) vh=(VarHdr*)((byte*)(vh+1)+vh->nProps*sizeof(uint32_t));
				vd->props=(PropertyID*)(vh+1); vd->fInit=true; vd->fLoaded=false;
				if ((exp->hdr.flags&EXPR_PRELOAD)!=0) {
					// ...alloc Value's
					//... load, if RC_NOTFOUND -> RC_FALSE or RC_NOTFOUND
					vd->fLoaded=true;
				}
			}
			if (op==OP_SETPROP) {
				top[-1].property=vd->props[u]&STORE_MAX_URIID; if (ff) top[-1].meta=*codePtr++;
			} else if (!vd->fLoaded) {
				if (op==OP_ELT) {afy_dec32(codePtr,eid); eid=afy_dec32zz(eid);} else eid=STORE_COLLECTION_ID;
				if ((rc=vars[vdx]->getValue(vd->props[u],*top,ff?LOAD_CARDINALITY|LOAD_SSV:LOAD_SSV,NULL,eid))!=RC_OK)
					{if (cntCatch!=0 && rc==RC_NOTFOUND) {top->setError(); rc=RC_OK;}}
			} else {
				v=&vd->vals[u]; assert(u<vd->nVals && vd->vals!=NULL); 
				if (op==OP_PROP) {*top=*v; top->flags=NO_HEAP;}
				else {
					afy_dec32(codePtr,eid); eid=afy_dec32zz(eid);
					if (v->type==VT_ARRAY || v->type==VT_COLLECTION) {
						// get elt
					} else if (eid==STORE_FIRST_ELEMENT || eid==STORE_LAST_ELEMENT) {
						*top=*v; top->flags=NO_HEAP;
					} else {
						// notfound
					}
				}
			}
			if (top->type==VT_EXPR) {
				Expr *expr=(Expr*)top->expr; const bool fFree=(top->flags&HEAP_TYPE_MASK)!=NO_HEAP;
				if ((rc=Expr::eval(&expr,1,*top,vars,nVars,params,nParams,ma))==RC_OK) {if (fFree) ma->free(expr);}
				else if (cntCatch!=0) {top->setError(); rc=RC_OK;}
			}
			top++; break;
		case OP_PATH:
			if ((rc=exp->path(top,codePtr,params[0],ma))!=RC_OK && rc!=RC_CORRUPTED && rc!=RC_NORESOURCES && cntCatch!=0)
				{(top++)->setError(); rc=RC_OK;}
			break;
		case OP_CURRENT:
			switch (*codePtr++) {
			default: rc=RC_CORRUPTED; break;
			case CVT_TIMESTAMP: getTimestamp(ts); top->setDateTime(ts); break;
			case CVT_USER: top->setIdentity(ses!=NULL || (ses=Session::getSession())!=NULL?ses->getIdentity():STORE_OWNER); break;
			case CVT_STORE: top->set((unsigned)StoreCtx::get()->storeID); break;
			}
			top++; break;
		case OP_CATCH:
			if (ff) cntCatch++; else {assert(cntCatch>0); cntCatch--;} break;
		case OP_JUMP:
			u=codePtr[0]|codePtr[1]<<8; codePtr+=ff?-(int)u:(int)u; break;
		case OP_COALESCE:
			if (top[-1].type==VT_ERROR) {freeV(*--top); codePtr+=2;} else codePtr+=codePtr[0]|codePtr[1]<<8;
			break;
		case OP_IN1:
			if (((u=*codePtr++)&CND_EXT)!=0) u|=*codePtr++<<8;
			rc=top[-2].type!=VT_ERROR&&top[-1].type!=VT_ERROR?calc(OP_EQ,top[-2],&top[-1],2,u,ma):(u&CND_NOT)!=0?RC_TRUE:RC_FALSE;
			if (unsigned(rc-RC_TRUE)<=unsigned(RC_FALSE-RC_TRUE)) {freeV(*--top); if (cndAct[u&CND_MASK][rc-RC_TRUE]!=2) freeV(*--top); goto bool_op;}
			break;
		case OP_IS_A:
			nops=ff?*codePtr++:2; if (((u=*codePtr++)&CND_EXT)!=0) u|=*codePtr++<<8;
			rc=top[-nops].type!=VT_ERROR && top[1-nops].type!=VT_ERROR ? calc(OP_IS_A,top[-nops],&top[1-nops],nops,u,ma):(u&CND_NOT)!=0?RC_TRUE:RC_FALSE;
			for (int i=nops; --i>=0;) freeV(*--top); goto bool_op;
		case OP_ISLOCAL:
			if (((u=*codePtr++)&CND_EXT)!=0) u|=*codePtr++<<8;
			switch (top[-1].type) {
			default: rc=(u&CND_NOT)!=0?RC_TRUE:RC_FALSE; freeV(*--top); goto bool_op;
			case VT_REF: rc=top[-1].pin->isLocal()?RC_TRUE:RC_FALSE; freeV(*--top); goto bool_op;
			case VT_REFID: rc=isRemote(top[-1].id)?RC_FALSE:RC_TRUE; freeV(*--top); goto bool_op;
			}
		case OP_EXISTS:
			rc=top[-1].type==VT_STMT?top[-1].stmt->exist(nParams>0?params[0].vals:(Value*)0,nParams>0?params[0].nValues:0):top[-1].type==VT_UINT&&top[-1].ui>0?RC_TRUE:RC_FALSE;
			freeV(*--top); if (((u=*codePtr++)&CND_EXT)!=0) u|=*codePtr++<<8; goto bool_op;
		case OP_EQ: case OP_NE: case OP_LT: case OP_LE: case OP_GT: case OP_GE:
		case OP_CONTAINS: case OP_BEGINS: case OP_ENDS: case OP_REGEX: case OP_IN:
			if (((u=*codePtr++)&CND_EXT)!=0) u|=*codePtr++<<8;
			rc=top[-2].type!=VT_ERROR&&top[-1].type!=VT_ERROR?calc((ExprOp)op,top[-2],&top[-1],2,u,ma):op==OP_NE||(u&CND_NOT)!=0?RC_TRUE:RC_FALSE;
			freeV(*--top); freeV(*--top);
		bool_op:
			if (unsigned(rc-RC_TRUE)<=unsigned(RC_FALSE-RC_TRUE)) {
				fop=cndAct[u&CND_MASK][rc-RC_TRUE];
				if ((u&CND_VBOOL)!=0) {
					top->set(rc==((u&CND_NOT)==0?RC_TRUE:RC_FALSE));
					switch (fop) {
					case 0: case 1: codePtr=codeEnd; continue;
					case 2: if ((u&CND_MASK)>=6) codePtr+=2; rc=RC_OK; break;
					case 3: codePtr+=codePtr[0]|codePtr[1]<<8; rc=RC_OK; break;
					}
				} else switch (fop) {
				case 0: assert(top==stack); if (idx<nExp) {codePtr=codeEnd; continue;} return RC_TRUE;
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
				|| (rc=(fop&_A)!=0?calcAgg((ExprOp)op,top[-nops],&top[1-nops],nops,u,ma):
					calc((ExprOp)op,top[-nops],&top[1-nops],nops,u,ma,vars,nVars))!=RC_OK && cntCatch!=0))
					{freeV(top[-nops]); top[-nops].setError(); rc=RC_OK;}
			for (int i=nops; --i>=1;) freeV(*--top);
			break;
		}
		if (rc!=RC_OK) {while (top>stack) freeV(*--top); return rc;}
	}
}

RC Expr::path(Value *&top,const byte *&codePtr,const ValueV& params,MemAlloc *ma) const
{
	const byte *pc=codePtr; const unsigned nPathSeg=*pc++; if (nPathSeg==0) return RC_CORRUPTED;
	PathSeg *path=new(ma) PathSeg[nPathSeg]; if (path==NULL) return RC_NORESOURCES;
	const byte *dscr=pc; pc+=nPathSeg; RC rc=RC_OK; --top;
	for (unsigned i=0; i<nPathSeg; i++) {
		PathSeg& sg=path[i]; byte u=*dscr++; Expr *filter=NULL;
		sg.eid=STORE_COLLECTION_ID; sg.filter=NULL; sg.cid=STORE_INVALID_CLASSID; sg.rmin=sg.rmax=1; sg.fLast=(u&1)!=0;
		switch (u&0xE0) {
		default: break;
		case 0x20: sg.rmin=0; break;
		case 0x40: sg.rmax=~0u; break;
		case 0x60: sg.rmin=0; sg.rmax=~0u; break;
		case 0x80: afy_dec32(pc,sg.rmin); afy_dec32(pc,sg.rmax); break;
		case 0xA0:
			afy_dec32(pc,sg.rmin);
			if (sg.rmin>=params.nValues) rc=RC_NOTFOUND;
			else if (params.vals[sg.rmin].type!=VT_UINT && params.vals[sg.rmin].type!=VT_INT) rc=RC_TYPE;
			else if (params.vals[sg.rmin].type==VT_INT && params.vals[sg.rmin].i<0) rc=RC_INVPARAM;
			else sg.rmin=sg.rmax=params.vals[sg.rmin].ui;
			break;
		case 0xC0:
			afy_dec32(pc,sg.rmin); afy_dec32(pc,sg.rmax);
			if (sg.rmin>=params.nValues||sg.rmax>=params.nValues) rc=RC_NOTFOUND;
			else if (params.vals[sg.rmin].type!=VT_UINT && params.vals[sg.rmin].type!=VT_INT || params.vals[sg.rmax].type!=VT_UINT && params.vals[sg.rmax].type!=VT_INT) rc=RC_TYPE;
			else if (params.vals[sg.rmin].type==VT_INT && params.vals[sg.rmin].i<0 || params.vals[sg.rmax].type==VT_INT && params.vals[sg.rmax].i<0 || params.vals[sg.rmin].ui>params.vals[sg.rmax].ui) rc=RC_INVPARAM;
			else {sg.rmin=params.vals[sg.rmin].ui; sg.rmax=params.vals[sg.rmax].ui;}
			break;
		}
		switch (u&0x18) {
		case 0: break;
		case 0x08: afy_dec32(pc,sg.eid); sg.eid=afy_dec32zz((uint32_t)sg.eid); break;
		case 0x10: if (top->type!=VT_INT && top->type!=VT_UINT) rc=RC_TYPE; else sg.eid=(top--)->ui; break;	// convert type?
		case 0x18:
//???		rc=deserialize(filter,pc,codeEnd,ses!=NULL?ses:(ses=Session::getSession()));
			if (rc==RC_OK) sg.filter=filter; //classID ??
			break;
		}
		if ((u&0x04)==0) {
			byte pref=*pc++;
			if (pref!=0xFF) {
				//...
			} else {
				//...
			}
		} else if (top->type!=VT_URIID) {
			// str -> URIID ???
			rc=RC_TYPE;
		} else sg.pid=(top--)->uid;
	}
	if (rc==RC_OK) {
		PathIt *pi=new(ma) PathIt(ma,path,nPathSeg,*top,true);		// ff -> fDFS
		if (pi==NULL) rc=RC_NORESOURCES; else (top++)->set(pi);
	}
	if (rc!=RC_OK) {
		// free filters ???
		ma->free(path);
	}
	codePtr=pc; return rc;
}

PathIt::~PathIt()
{
	if (path!=NULL) ma->free((void*)path);
	freeV(src); freeV(res);
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
			switch (src.type) {
			default:
				if (sidx!=0) return NULL;
				pv=&src; break;
			case VT_ARRAY:
				if (sidx>=src.length) return NULL;
				pv=&src.varray[sidx]; break;
			case VT_COLLECTION:
				if ((pv=src.nav->navigate(sidx==0?GO_FIRST:GO_NEXT))==NULL) return NULL;
				break;
			}
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
			pst->state=2; //printf("%*s(%d,%d):"_LX_FM"\n",(pst->idx-1+pst->rcnt-1)*2,"",pst->idx,pst->rcnt,res->id.pid);
//			if ((rc=getBody(*res))!=RC_OK || (res->hpin->hdr.descr&HOH_HIDDEN)!=0)
//				{res->cleanup(); if (rc==RC_OK || rc==RC_NOACCESS || rc==RC_REPEAT || rc==RC_DELETED) {pop(); continue;} else {state|=QST_EOF; return rc;}}
//			fOK=path[pst->idx-1].filter==NULL || ses->getStore()->queryMgr->condSatisfied((const Expr* const*)&path[pst->idx-1].filter,1,(const PINEx**)&res,1,params,nParams,ses);
			if (!fOK && !path[pst->idx-1].fLast) {AfyKernel::freeV(res); res.setError(); pop(); continue;}
			if (pst->rcnt<path[pst->idx-1].rmax||path[pst->idx-1].rmax==0xFFFF) {
//				if ((rc=ses->getStore()->queryMgr->loadV(pst->v[1],path[pst->idx-1].pid,*res,LOAD_SSV|LOAD_REF,ses,path[pst->idx-1].eid))==RC_OK) {if (pst->v[1].type!=VT_ERROR) pst->vidx=1;}
//				else if (rc!=RC_NOTFOUND) {freeV(res); res.setError(); return NULL;}
			}
			if (fOK && pst->rcnt>=path[pst->idx-1].rmin) {
				//pst->nSucc++;
				if (pst->idx<nPathSeg) {
					assert(pst->rcnt<=path[pst->idx-1].rmax||path[pst->idx-1].rmax==0xFFFF);
					for (;;) {
//						if ((rc=ses->getStore()->queryMgr->loadV(pst->v[0],path[pst->idx].pid,*res,LOAD_SSV|LOAD_REF,ses,path[pst->idx].eid))!=RC_OK && rc!=RC_NOTFOUND)
//							{freeV(res); res.setError(); return NULL;}
//						if (rc==RC_OK && pst->v[0].type!=VT_ERROR) {pst->vidx=0; break;}
						if (path[pst->idx].rmin!=0) break; if (pst->idx+1>=nPathSeg) return &res;
						unsigned s=pst->vidx; pst->vidx=0; //if ((rc=push(id))!=RC_OK) return NULL; pst->next->vidx=s;
					}
				} else if (path[pst->idx-1].filter!=NULL) {/*printf("->\n");*/ return &res;}
			}
			freeV(res); res.setError();
		case 2:
			if (pst->vidx>=2) {pop(); continue;}	// rmin==0 && pst->nSucc==0 -> goto next seg
			switch (pst->v[pst->vidx].type) {
			default: pst->vidx++; continue;		// rmin==0 -> goto next seg
//			case VT_REF: if (res!=NULL) *res=pst->v[pst->vidx].pin->getPID(); if ((rc=push())!=RC_OK) return rc; pst->next->vidx++; continue;
//			case VT_REFID: if (res!=NULL) *res=pst->v[pst->vidx].id; if ((rc=push())!=RC_OK) return rc; pst->next->vidx++; continue;
			case VT_STRUCT:
				//????
				continue;
			case VT_COLLECTION: case VT_ARRAY: pst->state=3; pst->cidx=0; break;
			}
		case 3:
			pv=pst->v[pst->vidx].type==VT_COLLECTION?pst->v[pst->vidx].nav->navigate(pst->cidx==0?GO_FIRST:GO_NEXT):pst->cidx<pst->v[pst->vidx].length?&pst->v[pst->vidx].varray[pst->cidx]:(const Value*)0;
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

unsigned long PathIt::count() const
{
	return ~0UL;
}

void PathIt::destroy()
{
	this->~PathIt(); free((void*)this,SES_HEAP);
}

RC Expr::calc(ExprOp op,Value& arg,const Value *moreArgs,int nargs,unsigned flags,MemAlloc *ma,PINEx **vars,ulong nVars)
{
	int c,i; RC rc=RC_OK; long start,lstr; const Value *arg2,*arg3,*pv; unsigned dtPart;
	Value *rng,*args,val,val2; uint32_t len,sht; byte *p; char *s; RefVID *rv; Session *ses;
	switch (op) {
	case OP_PLUS:
		if ((!isNumeric((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val,NO_FLT|NO_INT|NO_DAT1|NO_ITV1|NO_ITV2))==NULL) return RC_TYPE;
		if (moreArgs->type==VT_INTERVAL) {
			if (arg.type!=VT_DATETIME && arg.type!=VT_INTERVAL &&
				(rc=convV(arg,arg,VT_DATETIME,ma))!=RC_OK && (rc=convV(arg,arg,VT_INTERVAL,ma))!=RC_OK) return rc;
		}
		switch (arg.type) {
		default: assert(0);
		case VT_INT:		arg.i+=moreArgs->i; break;
		case VT_UINT:		arg.ui+=moreArgs->ui; break;
		case VT_INT64:		arg.i64+=moreArgs->i64; break;
		case VT_UINT64:		arg.ui64+=moreArgs->ui64; break;
		case VT_DATETIME:	
			if (moreArgs->type!=VT_INTERVAL) {
				if ((rc=convV(*moreArgs,val,VT_INTERVAL,ma))!=RC_OK) return rc;
				moreArgs=&val;
			}
			arg.ui64+=moreArgs->i64; break;
		case VT_INTERVAL:
			if (moreArgs->type!=VT_INTERVAL) {
				if ((rc=convV(*moreArgs,val,VT_INTERVAL,ma))!=RC_OK) return rc;
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
		if ((!isNumeric((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val,NO_FLT|NO_INT|NO_DAT1|NO_ITV1|NO_DAT2|NO_ITV2))==NULL) return RC_TYPE;
		if (moreArgs->type==VT_INTERVAL && arg.type!=VT_INTERVAL && (rc=convV(arg,arg,VT_INTERVAL,ma))!=RC_OK) return rc;
		switch (arg.type) {
		default: assert(0);
		case VT_INT:		arg.i-=moreArgs->i; break;
		case VT_UINT:		arg.ui-=moreArgs->ui; break;
		case VT_INT64:		arg.i64-=moreArgs->i64; break;
		case VT_UINT64:		arg.ui64-=moreArgs->ui64; break;
		case VT_DATETIME:	
			if (moreArgs->type!=VT_DATETIME && moreArgs->type!=VT_INTERVAL) {
				if ((rc=convV(*moreArgs,val,VT_DATETIME,ma))!=RC_OK && (rc=convV(*moreArgs,val,VT_INTERVAL,ma))!=RC_OK) return rc;
				moreArgs=&val;
			}
			if (moreArgs->type==VT_INTERVAL) arg.ui64-=moreArgs->i64; 
			else {assert(moreArgs->type==VT_DATETIME); arg.i64=arg.ui64-moreArgs->ui64; arg.type=VT_INTERVAL;}
			break;
		case VT_INTERVAL:
			if (moreArgs->type!=VT_INTERVAL) {
				if ((rc=convV(*moreArgs,val,VT_INTERVAL,ma))!=RC_OK) return rc;
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
		if ((!isNumeric((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val,NO_FLT|NO_INT))==NULL) return RC_TYPE;
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
		if ((!isNumeric((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val,NO_FLT|NO_INT))==NULL) return RC_TYPE;
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
		if ((!isNumeric((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val,NO_FLT|NO_INT))==NULL) return RC_TYPE;
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
		if (!isNumeric((ValueType)arg.type) && arg.type!=VT_INTERVAL && !numOpConv(arg,NO_FLT|NO_INT|NO_ITV1)) return RC_TYPE;
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
		if (!isNumeric((ValueType)arg.type) && arg.type!=VT_INTERVAL && !numOpConv(arg,NO_FLT|NO_INT|NO_ITV1)) return RC_TYPE;
		switch (arg.type) {
		default: assert(0);
		case VT_INT:		if ((uint32_t)arg.i==0x80000000) arg.type=VT_UINT; else if (arg.i<0) arg.i=-arg.i; break;
		case VT_UINT:		break;
		case VT_INT64:		if ((uint64_t)arg.i64==0x8000000000000000ULL) arg.type=VT_UINT64; else if (arg.i64<0LL) arg.i64=-arg.i64; break;
		case VT_UINT64:		break;
		case VT_FLOAT:		arg.f=fabs(arg.f); break;
		case VT_DOUBLE:		arg.d=fabs(arg.d); break;
		case VT_INTERVAL:	if ((uint64_t)arg.i64!=0x8000000000000000ULL && arg.i64<0LL) arg.i64=-arg.i64; break;
		}
		break;
	case OP_LN:
		if (arg.type==VT_FLOAT) {if (arg.f==0.f) return RC_DIV0; arg.f=logf(arg.f);}
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE,ma)!=RC_OK) return RC_TYPE;
		else if (arg.d==0.) return RC_DIV0; else arg.d=log(arg.d);
		// Udim ???
		break;
	case OP_EXP:
		if (arg.type==VT_FLOAT) arg.f=expf(arg.f);
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE,ma)!=RC_OK) return RC_TYPE;
		else arg.d=exp(arg.d);
		// Udim ???
		break;
	case OP_POW:
		// integer power???
		if (arg.type==VT_FLOAT && moreArgs->type==VT_FLOAT) arg.f=powf(arg.f,moreArgs->f);
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE,ma)!=RC_OK) return RC_TYPE;
		else if ((arg2=moreArgs)->type!=VT_DOUBLE && (arg2=&val,convV(*moreArgs,val,VT_DOUBLE,ma))!=RC_OK) return RC_TYPE;
		else arg.d=pow(arg.d,arg2->d);
		// Udim ???
		break;
	case OP_SQRT:
		if (arg.type==VT_FLOAT) {if (arg.f<0.f) return RC_INVPARAM; arg.f=sqrtf(arg.f);}
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE,ma)!=RC_OK) return RC_TYPE;
		else if (arg.d<0.) return RC_INVPARAM; else arg.d=sqrt(arg.d);
		// Udim ???
		break;
	case OP_FLOOR:
		if (isInteger((ValueType)arg.type)) break;
		if (arg.type==VT_FLOAT) arg.f=floorf(arg.f);
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE,ma)!=RC_OK) return RC_TYPE;
		else arg.d=::floor(arg.d);
		break;
	case OP_CEIL:
		if (isInteger((ValueType)arg.type)) break;
		if (arg.type==VT_FLOAT) arg.f=ceilf(arg.f);
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE,ma)!=RC_OK) return RC_TYPE;
		else arg.d=::ceil(arg.d);
		break;
	case OP_NOT:
		if (!isInteger((ValueType)arg.type) && !numOpConv(arg,NO_INT)) return RC_TYPE;
		switch (arg.type) {
		default: assert(0);
		case VT_INT:		arg.i=~arg.i; break;
		case VT_UINT:		arg.ui=~arg.ui; break;
		case VT_INT64:		arg.i64=~arg.i64; break;
		case VT_UINT64:		arg.ui64=~arg.ui64; break;
		}
		break;
	case OP_AND:
		if ((!isInteger((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val,NO_INT))==NULL) return RC_TYPE;
		switch (arg.type) {
		default: assert(0);
		case VT_INT:	arg.i&=moreArgs->i; break;
		case VT_UINT:	arg.ui&=moreArgs->ui; break;
		case VT_INT64:	arg.i64&=moreArgs->i64; break;
		case VT_UINT64:	arg.ui64&=moreArgs->ui64; break;
		}
		break;
	case OP_OR:
		if ((!isInteger((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val,NO_INT))==NULL) return RC_TYPE;
		switch (arg.type) {
		default: assert(0);
		case VT_INT:	arg.i|=moreArgs->i; break;
		case VT_UINT:	arg.ui|=moreArgs->ui; break;
		case VT_INT64:	arg.i64|=moreArgs->i64; break;
		case VT_UINT64:	arg.ui64|=moreArgs->ui64; break;
		}
		break;
	case OP_XOR:
		if ((!isInteger((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val,NO_INT))==NULL) return RC_TYPE;
		switch (arg.type) {
		default: assert(0);
		case VT_INT:	arg.i^=moreArgs->i; break;
		case VT_UINT:	arg.ui^=moreArgs->ui; break;
		case VT_INT64:	arg.i64^=moreArgs->i64; break;
		case VT_UINT64:	arg.ui64^=moreArgs->ui64; break;
		}
		break;
	case OP_LSHIFT:
		if (!isInteger((ValueType)arg.type) && !numOpConv(arg,NO_INT)) return RC_TYPE;
		if ((rc=getI(moreArgs[0],lstr))!=RC_OK) return rc;
		if (lstr<0) return RC_INVPARAM;
		switch (arg.type) {
		default: assert(0);
		case VT_INT:	arg.i<<=lstr; break;
		case VT_UINT:	arg.ui<<=lstr; break;
		case VT_INT64:	arg.i64<<=lstr; break;
		case VT_UINT64:	arg.ui64<<=lstr; break;
		}
		break;
	case OP_RSHIFT:
		if (!isInteger((ValueType)arg.type) && !numOpConv(arg,NO_INT)) return RC_TYPE;
		if ((rc=getI(moreArgs[0],lstr))!=RC_OK) return rc;
		if (lstr<0) return RC_INVPARAM;
		switch (arg.type) {
		default: assert(0);
		case VT_INT:	if ((flags&CND_UNS)==0) {arg.i>>=lstr; break;}
		case VT_UINT:	arg.ui>>=lstr; break;
		case VT_INT64:	if ((flags&CND_UNS)==0) {arg.i64>>=lstr; break;}
		case VT_UINT64:	arg.ui64>>=lstr; break;
		}
		break;
	case OP_MIN: case OP_MAX: 
		assert(nargs==2); c=cmp(arg,*moreArgs,flags);
		if (c==-1 && op==OP_MAX || c==1 && op==OP_MIN) {freeV(arg); if ((rc=copyV(*moreArgs,arg,ma))!=RC_OK) return rc;}
		break;
	case OP_LENGTH:
		len=arg.length;
//		switch (arg.type) {
//		default: break;
		// ...
//		}
		freeV(arg); arg.set(unsigned(len)); break;
	case OP_CONCAT:
		if (nargs==1) {
			if (arg.type==VT_ARRAY) {
				//...
			} else if (arg.type==VT_COLLECTION) {
				//...
			} else if (!isString((ValueType)arg.type)) rc=convV(arg,arg,VT_STRING,ma);
			return rc;
		}
		assert(nargs>=2&&nargs<255);
		if (!isString((ValueType)arg.type) && (rc=convV(arg,arg,VT_STRING,ma))!=RC_OK) return rc;
		for (i=1,len=arg.length,args=(Value*)moreArgs; i<nargs; i++) {
			Value *arg2=&args[i-1];
			if (arg2->type!=arg.type) {
				if (arg2->type==VT_ERROR) {freeV(arg); arg.setError(); return RC_OK;}
				if ((const Value*)args==moreArgs) {
					args = (Value*)alloca((nargs-1)*sizeof(Value));
					memcpy(args,moreArgs,(nargs-1)*sizeof(Value));
					for (int j=nargs-1; --j>=0;) args[j].flags=0;
				}
				if ((rc=convV(*arg2,*arg2,(ValueType)arg.type,ma))!=RC_OK) {	// ???
					for (int j=nargs-1; --j>=0;) freeV(args[j]);
					return rc;
				}
			}
			len+=arg2->length;
		}
		p=(byte*)ma->malloc(len+(arg.type==VT_BSTR?0:1));
		if (p==NULL) return RC_NORESOURCES; if (arg.type!=VT_BSTR) p[len]=0;
		memcpy(p,arg.bstr,arg.length); freeV(arg); arg.bstr=p; arg.flags=SES_HEAP;
		for (i=1,len=arg.length; i<nargs; i++) {
			const Value *arg2=&args[i-1]; memcpy(p+len,arg2->bstr,arg2->length);
			len+=arg2->length; if (args!=moreArgs) freeV(const_cast<Value&>(*arg2));
		}
		arg.length=len; break;
	case OP_SUBSTR:
		assert(nargs>=2&&nargs<=3);
		if (!isString((ValueType)arg.type) && (rc=convV(arg,arg,VT_STRING,ma))!=RC_OK || (rc=getI(moreArgs[0],lstr))!=RC_OK) return rc;
		if (nargs==3 && moreArgs[1].type==VT_ERROR) {freeV(arg); arg.setError(); return RC_OK;}
		if (nargs==2) start=0; else {start=lstr; if ((rc=getI(moreArgs[1],lstr))!=RC_OK) return rc;}
		if (lstr<0 || start<0 && -start>lstr) return RC_INVPARAM;
		if (lstr==0 || start==0 && arg.length>=(ulong)lstr) {arg.length=lstr; break;}
		p=(byte*)arg.bstr;
		if ((arg.flags&HEAP_TYPE_MASK)==NO_HEAP) {
			arg.bstr=(uint8_t*)ma->malloc(lstr+(arg.type!=VT_BSTR?1:0)); if (arg.bstr==NULL) return RC_NORESOURCES;
			arg.flags=arg.flags&~HEAP_TYPE_MASK|SES_HEAP;
		} else if ((ulong)lstr>arg.length) {
			arg.bstr=(uint8_t*)ma->realloc(p,lstr+(arg.type!=VT_BSTR?1:0)); if ((p=(byte*)arg.bstr)==NULL) return RC_NORESOURCES;
		}
		if (start<0 && -start<lstr) memmove((byte*)arg.bstr-start,p,min(uint32_t(lstr+start),arg.length));
		else if (start>=0 && (ulong)start<arg.length) memmove((byte*)arg.bstr,p+start,min(ulong(lstr),ulong(arg.length-start)));
		p=(byte*)arg.bstr; i=arg.type==VT_BSTR?0:' ';
		if (start>=(long)arg.length) memset(p,i,lstr);
		else {if (start<0) memset(p,i,-start); if (lstr>(long)arg.length-start) memset(p+arg.length-start,i,lstr-arg.length+start);}
		arg.length=lstr; if (arg.type!=VT_BSTR) ((char*)arg.str)[lstr]='\0'; break;
	case OP_REPLACE:
		if (moreArgs[1].type==VT_ERROR) {freeV(arg); arg.setError(); return RC_OK;}
		if ((arg2=strOpConv(arg,moreArgs,val))==NULL || (arg3=strOpConv(arg,moreArgs+1,val2))==NULL) return RC_TYPE;
		if (arg.length>0 && arg2->length>0) for (p=(byte*)arg.bstr,len=arg.length;;) {
			byte *q=(byte*)memchr(p,*arg2->bstr,len);
			if (q==NULL || (sht=len-ulong(q-p))<arg2->length) break; 
			if (memcmp(q,arg2->bstr,arg2->length)!=0) {if ((len=sht-1)<arg2->length) break; p=q+1; continue;}
			ulong l=arg.length+arg3->length-arg2->length,ll=ulong(q-(byte*)arg.bstr);
			if ((arg.flags&HEAP_TYPE_MASK)==NO_HEAP) {
				p=(byte*)ma->malloc(l); if (p==NULL) return RC_NORESOURCES;
				memcpy(p,arg.bstr,ll); memcpy(p+ll,arg3->bstr,arg3->length);
				if (arg.length>ll+arg2->length) memcpy(p+ll+arg3->length,arg.bstr+ll+arg2->length,arg.length-ll-arg2->length);
				arg.flags=arg.flags&~HEAP_TYPE_MASK|SES_HEAP; arg.bstr=p; p+=ll+arg3->length;
			} else if (arg3->length>arg2->length) {
				arg.bstr=p=(byte*)ma->realloc((byte*)arg.bstr,l); if (p==NULL) return RC_NORESOURCES;
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
		if ((rc=getI(moreArgs[0],lstr))!=RC_OK) return rc; if (lstr<0) return RC_INVPARAM;
		if (nargs>23 && moreArgs[1].type==VT_ERROR) {freeV(arg); arg.setError(); return RC_OK;}		//???????
		if (nargs==2) arg2=NULL; else if ((arg2=strOpConv(arg,moreArgs+1,val))==NULL) return RC_TYPE;
		if ((ulong)lstr>arg.length) {
			if ((arg.flags&HEAP_TYPE_MASK)==NO_HEAP) {
				p=(byte*)ma->malloc(lstr); if (p==NULL) return RC_NORESOURCES;
				memcpy(p,arg.bstr,arg.length); arg.bstr=(uint8_t*)p;
			} else {
				arg.bstr=(uint8_t*)ma->realloc((byte*)arg.bstr,lstr); 
				if (arg.bstr==NULL) return RC_NORESOURCES; p=(byte*)arg.bstr; 
			}
			p+=arg.length;
			if (arg2!=NULL && arg2->length>1) {
				for (ulong j=((ulong)lstr-arg.length)/arg2->length; j>0; --j) {memcpy(p,arg2->bstr,arg2->length); p+=arg2->length;}
				if ((len=((ulong)lstr-arg.length)%arg2->length)>0) memcpy(p,arg2->bstr,len);
			} else
				memset(p,arg2!=NULL&&arg2->length>0?*arg2->bstr:arg.type==VT_BSTR?0:' ',(ulong)lstr-arg.length);
			arg.length=lstr;
		}
		if (arg2==&val) freeV(val);
		break;
	case OP_TRIM:
		if ((arg2=strOpConv(arg,moreArgs,val))!=NULL) {s=(char*)arg2->str; lstr=arg2->length;} else return RC_TYPE;
		if (nargs<3) start=flags; else if ((rc=getI(moreArgs[1],start))!=RC_OK) return rc;
		if ((start&2)==0) {
			const char *const end=arg.str+arg.length,*p=arg.str;
			if (lstr==1) {for (const char ch=*s; p<end; p++) if (*p!=ch) break;}
			else for (; end-p>=lstr; p+=lstr) if (memcmp(p,s,lstr)!=0) break;
			if (p!=arg.str) {
				arg.length=uint32_t(end-p);
				if ((arg.flags&HEAP_TYPE_MASK)==NO_HEAP) arg.str=p; else memmove((char*)arg.str,p,arg.length);
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
		switch (arg.type) {
		default: break;
		case VT_ARRAY:
			for (len=arg.length,rc=RC_FALSE; len!=0; ) {
				rc=calc(op,const_cast<Value&>(arg.varray[--len]),moreArgs,2,flags,ma);
				if (rc==RC_TRUE) {if (op==OP_EQ) break;} else if (rc!=RC_FALSE || op!=OP_EQ) break;
			}
			return rc;
		case VT_COLLECTION:
			for (arg2=arg.nav->navigate(GO_FIRST),rc=RC_FALSE; arg2!=NULL; arg2=arg.nav->navigate(GO_NEXT)) {
				val=*arg2; val.flags=val.flags&~HEAP_TYPE_MASK|NO_HEAP;
				rc=calc(op,val,moreArgs,2,flags,ma); freeV(val);
				if (rc==RC_TRUE) {if (op==OP_EQ) break;} else if (rc!=RC_FALSE || op!=OP_EQ) break;
			}
			arg.nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID); return rc;
		}
		switch (moreArgs->type) {
		default: break;
		case VT_ARRAY:
			for (len=moreArgs->length; len!=0;) {
				c=cmp(arg,moreArgs->varray[--len],flags); if (op==OP_EQ) {if (c==0) return RC_TRUE;} else if ((rc=condRC(c,op))!=RC_TRUE) return rc;
			}
			return op!=OP_EQ?RC_TRUE:RC_FALSE;
		case VT_COLLECTION:
			for (arg2=moreArgs->nav->navigate(GO_FIRST); ;arg2=moreArgs->nav->navigate(GO_NEXT)) {
				if (arg2==NULL) {rc=op!=OP_EQ?RC_TRUE:RC_FALSE; break;}
				c=cmp(arg,*arg2,flags); if (op==OP_EQ) {if (c==0) {rc=RC_TRUE; break;}} else if ((rc=condRC(c,op))!=RC_TRUE) break;
			}
			moreArgs->nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID); return rc;
		case VT_STMT:
			return ((Stmt*)moreArgs->stmt)->cmp(arg,op,flags);
		}
		return condRC(cmp(arg,*moreArgs,flags),op);
	case OP_IS_A:
	case OP_IN:
		switch (arg.type) {
		default: break;
		case VT_ARRAY:
			for (len=arg.length,rc=RC_FALSE; rc==RC_FALSE && len!=0; )
				rc=calc(op,const_cast<Value&>(arg.varray[--len]),moreArgs,2,flags,ma);
			return rc;
		case VT_COLLECTION:
			for (arg2=arg.nav->navigate(GO_FIRST),rc=RC_FALSE; rc==RC_FALSE && arg2!=NULL; arg2=arg.nav->navigate(GO_NEXT))
				{val=*arg2; val.flags=val.flags&~HEAP_TYPE_MASK|NO_HEAP; rc=calc(op,val,moreArgs,2,flags,ma); freeV(val);}
			arg.nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID); return rc;
		}
		if (op==OP_IS_A) {
			if (moreArgs->type==VT_STMT) return ((Stmt*)moreArgs->stmt)->cmp(arg,op,flags);
			if (arg.type==VT_REFID) ses=Session::getSession(); else if (arg.type==VT_REF) ses=((PIN*)arg.pin)->getSes(); else return RC_FALSE;
			if (ses==NULL) return RC_NOSESSION; PINEx pex(ses),*ppe=&pex;
			if (arg.type==VT_REFID) {pex=arg.id; if ((rc=ses->getStore()->queryMgr->getBody(pex))!=RC_OK) return rc;}
			else if ((((PIN*)arg.pin)->getMode()&PIN_PINEX)!=0) ppe=(PINEx*)(PIN*)arg.pin; else pex=(PIN*)arg.pin;
			switch (moreArgs->type) {
			default: return RC_TYPE;
			case VT_URIID: return ses->getStore()->queryMgr->test(ppe,moreArgs->uid,ValueV(nargs>2?&moreArgs[1]:NULL,nargs-2),nargs<=2)?RC_TRUE:RC_FALSE;
			case VT_ARRAY:
				for (len=moreArgs->length,rc=RC_FALSE; len!=0;) {
					arg2=&moreArgs->varray[--len];
					if (arg2->type==VT_URIID &&
						ses->getStore()->queryMgr->test(ppe,arg2->uid,ValueV(nargs>2?&moreArgs[1]:NULL,nargs-2),nargs<=2)) return RC_TRUE;
				}
				return RC_FALSE;
			case VT_COLLECTION:
				for (arg2=moreArgs->nav->navigate(GO_FIRST),rc=RC_FALSE; arg2!=NULL; arg2=moreArgs->nav->navigate(GO_NEXT))
					if (arg2->type==VT_URIID &&
							ses->getStore()->queryMgr->test(ppe,arg2->uid,ValueV(nargs>2?&moreArgs[1]:NULL,nargs-2),nargs<=2)) return RC_TRUE;
				moreArgs->nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID); return rc;
			}
		} else switch (moreArgs->type) {
		default: return cmp(arg,*moreArgs,flags|CND_EQ)==0?RC_TRUE:RC_FALSE;
		case VT_RANGE:
			if ((c=moreArgs->range[0].type==VT_ANY?1:cmp(arg,moreArgs->range[0],(flags&CND_IN_LBND)!=0?flags|CND_NE:flags|CND_EQ))==-3) return RC_TYPE;
			if (c==-2 || (compareCodeTab[(flags&CND_IN_LBND)!=0?OP_GT-OP_EQ:OP_GE-OP_EQ]&1<<(c+1))==0) return RC_FALSE;
			if ((c=moreArgs->range[1].type==VT_ANY?-1:cmp(arg,moreArgs->range[1],(flags&CND_IN_RBND)!=0?flags|CND_NE:flags|CND_EQ))==-3) return RC_TYPE; 
			return c==-2 || (compareCodeTab[(flags&CND_IN_RBND)!=0?OP_LT-OP_EQ:OP_LE-OP_EQ]&1<<(c+1))==0?RC_FALSE:RC_TRUE;
		case VT_STMT:
			return ((Stmt*)moreArgs->stmt)->cmp(arg,op,flags);
		case VT_ARRAY:
			for (len=moreArgs->length; len!=0;) {
				arg2=&moreArgs->varray[--len];
				if (arg2->type!=VT_RANGE) {if (cmp(arg,*arg2,flags|CND_EQ)==0) return RC_TRUE;}
				else {
					c=cmp(arg,arg2->range[0],(flags&CND_IN_LBND)!=0?flags|CND_NE:flags|CND_EQ);
					if (c>=-1 && (compareCodeTab[OP_GE-OP_EQ]&1<<(c+1))!=0) {
						c=cmp(arg,arg2->range[1],(flags&CND_IN_RBND)!=0?flags|CND_NE:flags|CND_EQ);
						if (c>=-1 && (compareCodeTab[OP_LE-OP_EQ]&1<<(c+1))!=0) return RC_TRUE;
					}
				}
			}
			return RC_FALSE;
		case VT_COLLECTION:
			for (arg2=moreArgs->nav->navigate(GO_FIRST),rc=RC_FALSE; arg2!=NULL; arg2=moreArgs->nav->navigate(GO_NEXT)) {
				if (arg2->type!=VT_RANGE) {if (cmp(arg,*arg2,flags|CND_EQ)==0) {rc=RC_TRUE; break;}}
				else {
					c=cmp(arg,arg2->range[0],(flags&CND_IN_LBND)!=0?flags|CND_NE:flags|CND_EQ);
					if (c>=-1 && (compareCodeTab[OP_GE-OP_EQ]&1<<(c+1))!=0) {
						c=cmp(arg,arg2->range[1],(flags&CND_IN_RBND)!=0?flags|CND_NE:flags|CND_EQ);
						if (c>=-1 && (compareCodeTab[OP_LE-OP_EQ]&1<<(c+1))!=0) {rc=RC_TRUE; break;}
					}
				}
			}
			moreArgs->nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID); return rc;
		}
		break;
	case OP_CONTAINS:	// optimization (Boyer-Moore etc.)
	case OP_BEGINS:
	case OP_ENDS:
		rc=RC_FALSE;
	case OP_POSITION:
		switch (arg.type) {
		case VT_STREAM:
			// special implementation
		default: break;
		case VT_ARRAY:
			if (op==OP_POSITION) {
#if 1
				return RC_TYPE;
#else
				if (moreArgs->type!=VT_ARRAY && moreArgs->type!=VT_COLLECTION) {freeV(arg); arg.set(-1);}
				if (arg.length==1) {/* replace with arg.varray[0]*/}
				break;
#endif
			}
			for (len=arg.length; rc==RC_FALSE && len!=0; ) rc=calc(op,const_cast<Value&>(arg.varray[--len]),moreArgs,2,flags,ma);
			return rc;
		case VT_COLLECTION:
			if (op==OP_POSITION) {
#if 1
				return RC_TYPE;
#else
				if (moreArgs->type!=VT_ARRAY && moreArgs->type!=VT_COLLECTION) {freeV(arg); arg.set(-1);}
				if (arg.length==1) {/* replace with arg.varray[0]*/}
				break;
#endif
			}
			for (arg2=arg.nav->navigate(GO_FIRST); rc==RC_FALSE && arg2!=NULL; arg2=arg.nav->navigate(GO_NEXT)) {
				val=*arg2; val.flags=val.flags&~HEAP_TYPE_MASK|NO_HEAP;
				rc=calc(op,val,moreArgs,2,flags,ma); freeV(val);
			}
			arg.nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID); return rc;
		}
		switch (moreArgs->type) {
		default: break;
		case VT_ARRAY:
			if (op==OP_POSITION) {
#if 1
				return RC_TYPE;
#else
				//???
				break;
#endif
			}
			for (len=moreArgs->length; rc==RC_FALSE && len!=0; ) rc=calc(op,arg,&moreArgs->varray[len],2,flags,ma);
			return rc;
		case VT_COLLECTION:
			if (op==OP_POSITION) {
#if 1
				return RC_TYPE;
#else
				//???
				break;
#endif
			}
			for (arg2=moreArgs->nav->navigate(GO_FIRST); rc==RC_FALSE && arg2!=NULL; arg2=moreArgs->nav->navigate(GO_NEXT)) rc=calc(op,val,arg2,2,flags,ma);
			moreArgs->nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID); return rc;
		}
		if ((arg2=strOpConv(arg,moreArgs,val))==NULL) return RC_TYPE;
		if (arg.length>=arg2->length) switch (arg.type) {
		default: assert(0);
		case VT_STRING:
		case VT_URL:
			if (op!=OP_CONTAINS && op!=OP_POSITION) {
				const char *p=op==OP_BEGINS?arg.str:arg.str+arg.length-arg2->length;
				if (((flags&CND_NCASE)!=0?strncasecmp(p,arg2->str,arg2->length):strncmp(p,arg2->str,arg2->length))==0) rc=RC_TRUE;
			} else {
				const char *p=arg.str,*q=NULL; char ch=0; if ((flags&CND_NCASE)!=0) ch=toupper(*arg2->bstr);
				for (len=arg.length-arg2->length+1; ;len-=ulong(q-p)+1,p=q+1,q=NULL) {
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
				for (len=arg.length-arg2->length+1; ;len-=ulong(q-p)+1,p=q+1)
					if (len==0 || (q=(uint8_t*)memchr(p,*arg2->bstr,len))==NULL) {if (op==OP_POSITION) {freeV(arg); arg.set(-1);} break;}
					else if (!memcmp(q,arg2->bstr,arg2->length)) {if (op==OP_POSITION) {freeV(arg); arg.set((unsigned)(q-arg.bstr));} else rc=RC_TRUE; break;}
			}
			break;
		} else if (op==OP_POSITION) {freeV(arg); arg.set(-1);}
		if (arg2==&val) freeV(val);
		return rc;
	case OP_REGEX:
		if ((arg2=strOpConv(arg,moreArgs,val))==NULL) return RC_TYPE;
		switch (arg.type) {
		default: assert(0);
		case VT_STRING:
		case VT_URL:
			break;
		case VT_BSTR:
			break;
		}
		if (arg2==&val) freeV(val);
		break;
	case OP_COUNT:
		if (moreArgs->type!=VT_URIID) return RC_TYPE;
		switch (arg.type) {
		default: return RC_TYPE;
		case VT_REF:
			pv=arg.pin->getValue(moreArgs->uid);
			if (pv==NULL && op==OP_PATH) return RC_NOTFOUND;
			freeV(arg);
			if (op==OP_PATH) {if ((rc=copyV(*pv,arg,ma))!=RC_OK) return rc;}
			else arg.set(pv==NULL?0u:pv->type==VT_ARRAY?unsigned(pv->length):pv->type==VT_COLLECTION?~0u:1u);
			break;
		case VT_REFID:
			if ((ses=Session::getSession())==NULL) return RC_NOSESSION;
			if (ses->getStore()->queryMgr->loadValue(ses,arg.id,moreArgs->uid,STORE_COLLECTION_ID,arg,LOAD_CARDINALITY)!=RC_OK) {
				if (op==OP_COUNT) {freeV(arg); arg.set(0u);} else return RC_FALSE;
			}
			break;
		}
		break;
	case OP_UPPER:
	case OP_LOWER:
		if (arg.type==VT_STRING||arg.type==VT_URL||(rc=convV(arg,arg,VT_STRING,ma))==RC_OK) {
			s=forceCaseUTF8(arg.str,arg.length,len,ma,NULL,op==OP_UPPER); freeV(arg);
			if (s!=NULL) arg.set(s,len); else {arg.setError(); rc=RC_NORESOURCES;}
		}
		break;
	case OP_TONUM:
	case OP_TOINUM:
		switch (arg.type) {
		default: return RC_TYPE;
		case VT_STRING: return numOpConv(arg,op==OP_TOINUM?NO_INT:NO_INT|NO_FLT)?RC_OK:RC_TYPE;
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
		if ((rc=convV(arg,arg,flags,ma))!=RC_OK) return rc;
		break;
	case OP_RANGE:
		if (arg.type!=(arg2=moreArgs)->type && arg.type!=VT_ANY && arg2->type!=VT_ANY) {
			/* ??? */ return RC_TYPE;
		}
		if ((rng=new(ma) Value[2])==NULL) return RC_NORESOURCES;
		rng[0]=arg;
		if (arg2==&val) rng[1]=val;
		else if ((rc=copyV(*arg2,rng[1],ma))!=RC_OK) {free(rng,SES_HEAP); return rc;}
		rng[0].property=rng[1].property=STORE_INVALID_PROPID;
		rng[0].eid=rng[1].eid=STORE_COLLECTION_ID;
		arg.setRange(rng); arg.flags=ma->getAType(); break;
	case OP_ARRAY:
		if ((ses=Session::getSession())==NULL) return RC_NOSESSION;
		if ((rc=ExprTree::normalizeArray(&arg,nargs,val,ses,ses->getStore()))!=RC_OK) return rc;
		if (val.type==VT_EXPRTREE) {freeV(val); return RC_TYPE;}
		arg=val; break;
	case OP_STRUCT: case OP_PIN:
		if ((ses=Session::getSession())==NULL) return RC_NOSESSION;
		if (nargs==1) switch (arg.type) {
		case VT_REF:
		case VT_REFID:
		case VT_STRUCT:
		case VT_ARRAY:
		case VT_COLLECTION:
			//???
			break;
		}
		if ((rc=ExprTree::normalizeStruct(&arg,nargs,val,ses))!=RC_OK) return rc;
		if (val.type==VT_EXPRTREE) {freeV(val); return RC_TYPE;}
		assert(val.type==VT_STRUCT);
		if (op==OP_PIN) {
			PIN *pin=new(ses) PIN(ses,PIN::defPID,PageAddr::invAddr,0,(Value*)val.varray,val.length);
			if (pin!=NULL) {val.set(pin); val.flags=SES_HEAP;} else return RC_NORESOURCES;
		}
		arg=val; break;
	case OP_EDIT:
		assert(arg.type!=VT_STREAM);
		sht=ulong(moreArgs->edit.shift); len=moreArgs->edit.length;		// ???
		if ((arg2=strOpConv(arg,moreArgs,val))==NULL) return RC_TYPE;
		if (sht==uint32_t(~0ul)) sht=arg.length; rc=RC_OK;
		if (sht+len>arg.length) rc=RC_INVPARAM;
		else if (arg2->length<=len && (arg.flags&HEAP_TYPE_MASK)!=NO_HEAP) {
			p=(byte*)arg.bstr; if (arg2->length>0) memcpy(p+sht,arg2->bstr,arg2->length);
			if (len>arg2->length && sht+len<arg.length) memmove(p+sht+arg2->length,arg.bstr+sht+len,arg.length-sht-len);
			arg.length-=len-arg2->length;
		} else if ((p=(byte*)ma->malloc(arg.length-len+arg2->length+(arg.type==VT_BSTR?0:1)))==NULL)
			rc=RC_NORESOURCES;
		else {
			if (sht>0) memcpy(p,arg.bstr,sht); if (arg2->length>0) memcpy(p+sht,arg2->bstr,arg2->length);
			if (sht+len<arg.length) memcpy(p+sht+arg2->length,arg.bstr+sht+len,arg.length-sht-len);
			if ((arg.flags&HEAP_TYPE_MASK)!=NO_HEAP) free((byte*)arg.bstr,(HEAP_TYPE)(arg.flags&HEAP_TYPE_MASK));
			arg.length+=arg2->length-len; arg.flags=arg.flags&~HEAP_TYPE_MASK|SES_HEAP; arg.bstr=p;
		}
		if (rc==RC_OK && arg.type!=VT_BSTR) p[arg.length]=0;
		if (arg2==&val) freeV(val);
		return rc;
	case OP_EXTRACT:
		if (nargs==2) {
			if (moreArgs->type!=VT_INT && moreArgs->type!=VT_UINT) return RC_TYPE;
			flags=moreArgs->ui;
		}
		if (flags==EY_IDENTITY) {
			if (arg.type!=VT_REFID && (rc=convV(arg,arg,VT_REFID,ma))!=RC_OK) return rc;
			arg.setIdentity(arg.id.ident);
		} else {
			if (arg.type!=VT_DATETIME && (rc=convV(arg,arg,VT_DATETIME,ma))!=RC_OK
				|| (rc=getDTPart(arg.ui64,dtPart,flags))!=RC_OK) return rc;
			arg.set(dtPart);
		}
		break;
	case OP_DEREF:
		if ((rc=derefValue(arg,arg,Session::getSession()))!=RC_OK) return rc;
		break;
	case OP_REF:
		switch (arg.type) {
		default: if ((rc=convV(arg,arg,VT_REFID,ma))!=RC_OK) return rc;
		case VT_REFID: if (nargs==1) return RC_OK; break;
		case VT_REFIDPROP: case VT_REFIDELT: return nargs==1?RC_OK:RC_TYPE;
		}
		if ((arg2=moreArgs)->type!=VT_URIID) {
			if ((rc=convV(*moreArgs,val,VT_URIID,ma))!=RC_OK) return rc;
			arg2=&val;
		}
		if ((rv=new(SES_HEAP) RefVID)==NULL) return RC_NORESOURCES;
		rv->id=arg.id; rv->pid=arg2->uid; arg.set(*rv);
		if (nargs>=3) {
			if ((arg2=&moreArgs[1])->type!=VT_INT && arg2->type!=VT_UINT)
				{if ((rc=convV(*arg2,val,VT_UINT,ma))==RC_OK) arg2=&val; else return rc;}
			rv->eid=arg2->ui; arg.type=VT_REFIDELT;
		}
		break;
	case OP_CALL:
		if (arg.type==VT_STRING) {
			// try to compile
		}
		if ((ses=Session::getSession())==NULL) return RC_NOSESSION;
		{ValueV vv(moreArgs,nargs-1); if ((rc=ses->getStore()->queryMgr->eval(ses,&arg,arg,vars,nVars,&vv,1,ses,false))!=RC_OK) return rc;}
		break;
	case OP_MEMBERSHIP:
		if (arg.type==VT_REFID) {
			if ((ses=Session::getSession())==NULL) return RC_NOSESSION;
			ClassResult clr(ses,ses->getStore()); PINEx pex(ses,arg.id); 
			if ((rc=pex.getSes()->getStore()->queryMgr->getBody(pex))!=RC_OK) return rc;
			if ((rc=ses->getStore()->classMgr->classify(&pex,clr))!=RC_OK) return rc;
			if (clr.nClasses==0) arg.setError();
			else if ((args=new(ses) Value[clr.nClasses])==NULL) return RC_NORESOURCES;
			else {
				for (ulong i=0; i<clr.nClasses; i++) {args[i].setURIID(clr.classes[i]->cid); args[i].eid=i;}
				arg.set(args,clr.nClasses); arg.flags=SES_HEAP;
			}
		} else if (arg.type!=VT_REF) return RC_TYPE;
		else if ((((PIN*)arg.pin)->getMode()&(PIN_HIDDEN|PIN_NO_INDEX))!=0) {freeV(arg); arg.setError();}
		else {
			ses=((PIN*)arg.pin)->getSes(); ClassResult clr(ses,ses->getStore()); PINEx pex(ses),*ppe=&pex;
			if ((((PIN*)arg.pin)->getMode()&PIN_PINEX)!=0) ppe=(PINEx*)(PIN*)arg.pin; else pex=(PIN*)arg.pin;
			if ((rc=ses->getStore()->classMgr->classify(&pex,clr))!=RC_OK) return rc;
			freeV(arg); arg.setError();
			if (clr.nClasses!=0) {
				if ((args=new(ses) Value[clr.nClasses])==NULL) return RC_NORESOURCES;
				else {
					for (ulong i=0; i<clr.nClasses; i++) {args[i].setURIID(clr.classes[i]->cid); args[i].eid=i;}
					arg.set(args,clr.nClasses); arg.flags=SES_HEAP;
				}
			}
		}
		break;
	default:
		return RC_INTERNAL;
	}
	return RC_OK;
}

RC AggAcc::next(const Value& v)
{
	assert(v.type!=VT_ERROR);
	if (op==OP_COUNT) count++;
	else if (op==OP_HISTOGRAM) {
		assert(hist!=NULL); Value *pv=NULL; RC rc;
		switch (hist->add(v,&pv)) {
		case SLO_ERROR: return RC_NORESOURCES;
		case SLO_NOOP: break;
		case SLO_INSERT:
			if ((rc=copyV(v,*pv,ma))!=RC_OK) return rc;
			pv->property=0; pv->eid=1; break;
		default: assert(0);
		}
		count++; return RC_OK;
	} else if (sum.type==VT_ERROR) {
		sum=v; sum.flags=NO_HEAP;
		if (op==OP_SUM) {
			if (isNumeric((ValueType)sum.type) || sum.type==VT_INTERVAL || Expr::numOpConv(sum,NO_INT|NO_FLT|NO_ITV1)) count=1; else sum.type=VT_ERROR;
		} else if (op>=OP_AVG) {
			if (op==OP_AVG && sum.type==VT_DATETIME || sum.type==VT_DOUBLE || convV(sum,sum,VT_DOUBLE,ma)==RC_OK) count=1; else sum.type=VT_ERROR;
		} else {
			if (copyV(v,sum,ma)==RC_OK) count=1; else sum.type=VT_ERROR;
		}
	} else if (op==OP_MIN || op==OP_MAX) {
		int c=cmp(sum,v,flags); if (c==-1 && op==OP_MAX || c==1 && op==OP_MIN) {freeV(sum); RC rc=copyV(v,sum,ma); if (rc!=RC_OK) return rc;}
	} else {
		const Value *pv=&v; Value val;
		if (op!=OP_COUNT) try {
			if (pv->type!=sum.type && (pv=Expr::numOpConv(sum,pv,val,NO_INT|NO_FLT|NO_ITV1|NO_DAT1|NO_DAT2|NO_ITV2))==NULL) return RC_TYPE;
			switch (sum.type) {
			default: return RC_TYPE;
			case VT_INT: sum.i+=pv->i; assert(op==OP_SUM); break;
			case VT_UINT: sum.ui+=pv->ui; assert(op==OP_SUM); break;
			case VT_INT64: case VT_INTERVAL: sum.i64+=pv->i64; assert(op==OP_SUM); break;
			case VT_UINT64: case VT_DATETIME: sum.ui64+=pv->ui64; assert(op==OP_SUM||op==OP_AVG); break;
			case VT_FLOAT: sum.f+=pv->f; assert(op==OP_SUM); break;
			case VT_DOUBLE:	sum.d+=pv->d; if (op>=OP_VAR_POP) sum2+=pv->d*pv->d; break;
			}
		} catch (...) {return RC_TOOBIG;}
		count++;
	}
	return RC_OK;
}

RC AggAcc::result(Value& res)
{
	try {
		if (op==OP_COUNT) res.setU64(count);
		else if (count==0) res.setError();
		else switch (op) {
		case OP_HISTOGRAM:
			assert(hist!=NULL);
			{unsigned cnt=(unsigned)hist->getCount(); hist->start(); RC rc;
			Value *pv=new(ma) Value[cnt*3]; if (pv==NULL) return RC_NORESOURCES;
			for (unsigned i=0; i<cnt; i++) {
				const Value *vc=hist->next();
				if (vc==NULL) {freeV(pv,i,ma); return RC_CORRUPTED;}
				if ((rc=copyV(*(const Value*)vc,pv[cnt+i*2],ma))!=RC_OK) return rc;	// cleanup?
				pv[cnt+i*2+1].setU64(uint64_t(vc->property)<<32|vc->eid); pv[cnt+i*2+1].setPropID(PROP_SPEC_VALUE);
				pv[cnt+i*2].setPropID(PROP_SPEC_KEY); pv[i].setStruct(&pv[cnt+i*2],2); pv[i].eid=i;
			}
			res.set(pv,cnt); res.flags=ma->getAType(); hist->~Histogram(); ma->free(hist); hist=NULL;}	// cleanup!!!
			break;
		case OP_MIN: case OP_MAX: case OP_SUM: res=sum; sum.type=VT_ERROR; sum.flags=NO_HEAP; break;
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
	case VT_ARRAY:
		if (op==OP_COUNT && (flags&CND_DISTINCT)==0) count+=v.length;
		else for (i=0; i<v.length; i++) if ((flags&CND_DISTINCT)!=0) {
			//...
		} else if ((rc=next(v.varray[i]))!=RC_OK) return rc;
		break;
	case VT_COLLECTION:
		if (op==OP_COUNT && (flags&CND_DISTINCT)==0) count+=v.nav->count();
		else for (cv=v.nav->navigate(GO_FIRST); cv!=NULL; cv=v.nav->navigate(GO_NEXT)) if ((flags&CND_DISTINCT)!=0) {
			//...
		} else if ((rc=next(*cv))!=RC_OK) return rc;
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
		if ((flags&CND_DISTINCT)!=0) {
			//...
		} else if (op==OP_COUNT) count++; else if ((rc=next(v))!=RC_OK) return rc;
		break;
	}
	return RC_OK;
}

RC Expr::calcAgg(ExprOp op,Value& res,const Value *more,unsigned nargs,unsigned flags,MemAlloc *ma)
{
	Histogram *h=NULL;
	if (op==OP_HISTOGRAM) { 
		if ((h=new(ma) Histogram(*ma,flags))==NULL) return RC_NORESOURCES;
		flags&=~CND_DISTINCT;
	} else if ((flags&CND_DISTINCT)!=0) {
		//...
	}
	AggAcc aa(op,flags,ma,h); const Value *pv=&res; RC rc;
	for (unsigned idx=0; idx<nargs; pv=&more[idx++]) if ((rc=aa.process(*pv))!=RC_OK) return rc;
	if ((flags&CND_DISTINCT)!=0) {
		// sort
		// feed results to aa
	}
	freeV(res); return aa.result(res);
}

RC	Expr::getI(const Value& v,long& num)
{
	RC rc; Value val; const char *s;
	for (const Value *pv=&v;;) {
		HEAP_TYPE allc=(HEAP_TYPE)(pv->flags&HEAP_TYPE_MASK);
		bool fFree=pv==&val && allc!=NO_HEAP; 
		switch (pv->type) {
		default: return RC_TYPE;
		case VT_STRING: 
			if ((rc=strToNum(s=pv->str,pv->length,val))!=RC_OK) return rc;
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
			if ((rc=derefValue(*pv,val,Session::getSession()))!=RC_OK) return rc;
			pv=&val; continue;
		}
		if (fFree) freeV(val);
		return RC_OK;
	}
}

const Value *Expr::strOpConv(Value& arg,const Value *arg2,Value& buf)
{
	Session *ses=NULL;
	while (isRef((ValueType)arg.type)) {
		if (ses==NULL && (ses=Session::getSession())==NULL) return NULL;
		if (derefValue(arg,arg,ses)!=RC_OK) return NULL;
	}
	for (;isRef((ValueType)arg2->type);arg2=&buf) {
		if (ses==NULL && (ses=Session::getSession())==NULL) return NULL;
		if (derefValue(*arg2,buf,ses)!=RC_OK) return NULL;
	}
	ValueType vt1=(ValueType)arg.type,vt2=(ValueType)arg2->type;
	if (isString(vt1)) {
		if (isString(vt2)) {
			if (vt2==VT_BSTR) arg.type=VT_BSTR;
		} else {
			if (ses==NULL && (ses=Session::getSession())==NULL) return NULL;
			if (convV(*arg2,buf,vt1==VT_URL?VT_STRING:vt1,ses)!=RC_OK) return NULL;
			arg2=&buf;
		}
	} else if (isString(vt2)) {
		if (ses==NULL && (ses=Session::getSession())==NULL) return NULL;
		if (convV(arg,arg,vt2==VT_URL?VT_STRING:vt2,ses)!=RC_OK) return NULL;
	} else {
		if (ses==NULL && (ses=Session::getSession())==NULL) return NULL;
		if (convV(arg,arg,VT_STRING,ses)!=RC_OK || convV(*arg2,buf,VT_STRING,ses)!=RC_OK) return NULL;
		arg2=&buf;
	}
	return arg2;
}

const Value *Expr::numOpConv(Value& arg,const Value *arg2,Value& buf,unsigned flg)
{
	Session *ses=NULL;
	while (isRef((ValueType)arg.type)) {
		if (ses==NULL && (ses=Session::getSession())==NULL) return NULL;
		if (derefValue(arg,arg,ses)!=RC_OK) return NULL;
	}
	if (arg.type==VT_STRING) {
		if (strToNum(arg.str,arg.length,buf)!=RC_OK) return NULL;
		freeV(arg); arg=buf;
	}
	for (;isRef((ValueType)arg2->type);arg2=&buf) {
		if (ses==NULL && (ses=Session::getSession())==NULL) return NULL;
		if (derefValue(*arg2,buf,ses)!=RC_OK) return NULL;
	}
	if (arg2->type==VT_STRING) {
		const char *p=arg2->str;
		if (strToNum(p,arg2->length,buf)!=RC_OK) return NULL;
		if (arg2==&buf) free((void*)p,SES_HEAP); else arg2=&buf;
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

bool Expr::numOpConv(Value& arg,unsigned flg)
{
	Value val;
	for (;;) switch (arg.type) {
	default: return false;
	case VT_INT: case VT_UINT: case VT_INT64: case VT_UINT64: return true;
	case VT_FLOAT: if ((flg&NO_FLT)==0) {arg.i=(int32_t)arg.f; arg.type=VT_INT;} return true;
	case VT_DOUBLE: if ((flg&NO_FLT)==0) {arg.i64=(int64_t)arg.d; arg.type=VT_INT64;} return true;
	case VT_STRING:
		if (strToNum(arg.str,arg.length,val)!=RC_OK) return false;
		freeV(arg); arg=val; if ((flg&NO_FLT)==0 && arg.type>=VT_FLOAT) continue;
		return true;
	case VT_REF: case VT_REFID: case VT_REFPROP: case VT_REFELT: case VT_REFIDPROP: case VT_REFIDELT:
		if (derefValue(arg,arg,Session::getSession())==RC_OK) continue;
		return false;
	}
}
