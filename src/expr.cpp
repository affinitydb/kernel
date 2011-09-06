/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

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

using namespace MVStoreKernel;

static const bool jumpOp[8][2] = {{false,false},{false,false},{true,false},{false,true},{false,true},{true,false},{false,true},{true,false}};
static const byte cndAct[8][2] = {{0,1},{1,0},{0,2},{2,0},{2,1},{1,2},{2,3},{3,2}};
static const byte logOp[8][2] = {{4,2},{3,5},{6,2},{3,7},{4,7},{6,5},{6,7},{6,7}};
static const byte compareCodeTab[] = {2,5,1,3,4,6};

const ExprOp ExprTree::notOp[] = {OP_NE,OP_EQ,OP_GE,OP_GT,OP_LE,OP_LT};

RC Expr::execute(Value& res,const Value *params,unsigned nParams) const
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION;
		if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		const Expr *exp=this; res.setError();
		RC rc=eval(&exp,1,res,NULL,0,params,nParams,ses);
		switch (rc) {
		default: return rc;
		case RC_TRUE: res.set(true); return RC_OK;
		case RC_FALSE: res.set(false); return RC_OK;
		}
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in IExpr::execute()\n"); return RC_INTERNAL;}
}

RC Expr::eval(const Expr *const *exprs,ulong nExp,Value& result,const PINEx **vars,ulong nVars,const Value *params,ulong nParams,MemAlloc *ma,bool fIgnore)
{
	ulong idx=0; const Expr *exp; assert (exprs!=NULL && nExp>0 && exprs[0]!=NULL); Session *ses=NULL;
	const byte *codePtr=NULL,*codeEnd=NULL,*p; ulong cntCatch=0,lStack=0; Value *stack=NULL,*top=NULL;
	for (;;) {
		if (codePtr>=codeEnd) {
			if (codePtr>codeEnd || top!=stack && top!=stack+1) return RC_INTERNAL;
			if (idx>=nExp) {if (top==stack) return RC_TRUE; result=top[-1]; return result.type!=VT_ERROR?RC_OK:RC_NOTFOUND;}
			if ((exp=exprs[idx++])==NULL || (params==NULL||nParams==0) && fIgnore && (exp->hdr.flags&EXPR_PARAMS)!=0) continue;
			codePtr=(const byte*)(exp+1); codeEnd=(const byte*)&exp->hdr+exp->hdr.lExpr; cntCatch=(exp->hdr.flags&EXPR_BOOL)!=0?1:0; 
			if (top==stack+1) freeV(*--top);
			if (exp->hdr.lStack>lStack) {
				if ((stack=(Value*)alloca((exp->hdr.lStack-lStack)*sizeof(Value)))==NULL) return RC_NORESOURCES;
				top=stack; lStack=exp->hdr.lStack;
			}
		}
		assert(top>=stack && top<=stack+exp->hdr.lStack);
		TIMESTAMP ts; const PINEx *vi; const Value *v; RC rc=RC_OK; ElementID eid; Expr *filter; ClassID fcls;
		byte op=*codePtr++; const bool ff=(op&0x80)!=0; int nops; unsigned fop,rmin,rmax; uint32_t u,propID;
		switch (op&=0x7F) {
		case OP_CON:
			assert(top<stack+exp->hdr.lStack); top->flags=0;
			if ((rc=MVStoreKernel::deserialize(*top,codePtr,codeEnd,ma,true))!=RC_OK) break;
			assert(top->type!=VT_VARREF && top->type!=VT_PARAM && top->type!=VT_CURRENT);
			top++; break;
		case OP_PARAM:
			if ((u=*codePtr++)>=nParams) {if (ff) (top++)->set(0u); else if (cntCatch!=0) (top++)->setError(); else rc=RC_NOTFOUND; break;}
			v=&params[u]; 
			if (v->type==VT_VARREF) {u=v->refPath.refN; if (v->length==0) goto var; propID=v->refPath.id; eid=v->eid; goto prop;}
			if (ff) top->set(1u); else {*top=*v; top->flags=NO_HEAP;}
			top++; break;
		case OP_VAR: u=*codePtr++;
		var:
			if (u>=nVars) {if (ff) (top++)->set(0u); else if (cntCatch!=0) (top++)->setError(); else rc=RC_NOTFOUND; break;}
			if (ff) top->set(1u); else {
				PIN *pin=NULL; vi=vars[u];
				if (ses==NULL && (ses=Session::getSession())==NULL) {top->setError(); rc=cntCatch!=0?RC_OK:RC_NOSESSION;}
				else if ((vi->id.pid!=STORE_INVALID_PID || (rc=vi->unpack())==RC_OK) &&
					(rc=vi->ses->getStore()->queryMgr->loadPIN(vi->ses,vi->id,pin,0,(PINEx*)vi))==RC_OK)
						{top->set(pin); top->flags=SES_HEAP;} else {top->setError(); if (cntCatch!=0) rc=RC_OK;}
			}
			top++; break;
		case OP_PROP: u=*codePtr++; mv_dec32(codePtr,propID); eid=STORE_COLLECTION_ID;
		prop:
			if (u>=nVars) {if (ff) (top++)->set(0u); else if (cntCatch!=0) (top++)->setError(); else rc=RC_NOTFOUND; break;}
			if ((rc=vars[u]->getValue(propID,*top,ff?LOAD_CARDINALITY|LOAD_SSV:LOAD_SSV,NULL,eid))!=RC_OK) {if (cntCatch!=0) rc=RC_OK;}
			else if (top->type==VT_EXPR) {
				Expr *expr=(Expr*)top->expr; const bool fFree=(top->flags&HEAP_TYPE_MASK)!=NO_HEAP;
				if ((rc=Expr::eval(&expr,1,*top,vars,nVars,params,nParams,ma))==RC_OK) {if (fFree) ma->free(expr);}
				else if (cntCatch!=0) {top->setError(); rc=RC_OK;}
			}
			top++; break;
		case OP_ELT: u=*codePtr++; mv_dec32(codePtr,propID); mv_dec32(codePtr,eid); eid=mv_dec32zz(eid); goto prop;
		case OP_PATH:
			u=ff?*codePtr++:0; --top; eid=STORE_COLLECTION_ID; filter=NULL; fcls=STORE_INVALID_CLASSID; rmin=rmax=1;
			switch (u&0xE0) {
			default: break;
			case 0x20:	rmin=0; rmax=1; break;
			case 0x40:	rmin=1; rmax=~0u; break;
			case 0x60:	rmin=0; rmax=~0u; break;
			case 0x80: mv_dec32(codePtr,rmin); mv_dec32(codePtr,rmax); break;
			case 0xA0:
				mv_dec32(codePtr,rmin); // rmin=rmax=params[rmin];
				break;
			case 0xC0:
				mv_dec32(codePtr,rmin); mv_dec32(codePtr,rmax);
				// rmin=params[rmin]; rmax=params[rmax];
				break;
			}
			switch (u&0x18) {
			case 0: break;
			case 0x08: mv_dec32(codePtr,eid); eid=mv_dec32zz((uint32_t)eid); break;
			case 0x10: if (top->type!=VT_INT && top->type!=VT_UINT) rc=RC_TYPE; else eid=(top--)->ui; break;	// convert type?
			case 0x18:
				rc=deserialize(filter,codePtr,codeEnd,ses!=NULL?ses:(ses=Session::getSession()));
				//classID ??
				break;
			}
			if ((u&0x04)==0) {mv_dec32(codePtr,propID);}
			else if (top->type==VT_URIID) propID=(top--)->uid;
			else {
				// str -> URIID ???
				rc=RC_TYPE;
			}
			if ((rc!=RC_OK || (rc=path(top,propID,u,eid,filter,fcls,rmin,rmax))!=RC_OK) && cntCatch!=0) rc=RC_OK;
			if (filter!=NULL) filter->destroy();	// used in PathIt?
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
			p=codePtr; mv_dec32(p,u); codePtr+=u; break;
		case OP_COALESCE:
			if (top[-1].type==VT_ERROR) {freeV(*--top); mv_adv32(codePtr);}
			else {p=codePtr; mv_dec32(p,u); codePtr+=u;}
			break;
		case OP_IN1:
			if (((u=*codePtr++)&CND_EXT)!=0) u|=*codePtr++<<8;
			rc=top[-2].type!=VT_ERROR&&top[-1].type!=VT_ERROR?calc(OP_EQ,top[-2],&top[-1],2,u,ma):(u&CND_NOT)!=0?RC_TRUE:RC_FALSE;
			freeV(*--top); if (rc==RC_TRUE) {freeV(*--top); goto bool_op;}
			if (rc==RC_FALSE) {if ((u&CND_MASK)>=6) mv_adv32(codePtr); rc=RC_OK;}
			break;
		case OP_IS_A:
			nops=ff?*codePtr++:2; if (((u=*codePtr++)&CND_EXT)!=0) u|=*codePtr++<<8;
			rc=top[-nops].type!=VT_ERROR && top[1-nops].type!=VT_ERROR ? calc(OP_IS_A,top[-nops],&top[1-nops],nops,u,ma):RC_FALSE;
			for (int i=nops; --i>=0;) freeV(*--top); goto bool_op;
		case OP_ISLOCAL:
			if (((u=*codePtr++)&CND_EXT)!=0) u|=*codePtr++<<8;
			switch (top[-1].type) {
			default: rc=RC_FALSE; freeV(*--top); goto bool_op;
			case VT_REF: rc=top[-1].pin->isLocal()?RC_TRUE:RC_FALSE; freeV(*--top); goto bool_op;
			case VT_REFID: rc=isRemote(top[-1].id)?RC_FALSE:RC_TRUE; freeV(*--top); goto bool_op;
			}
		case OP_EXISTS:
			rc=top[-1].type==VT_STMT?top[-1].stmt->exist():top[-1].type==VT_UINT&&top[-1].ui>0?RC_TRUE:RC_FALSE;
			freeV(*--top); if (((u=*codePtr++)&CND_EXT)!=0) u|=*codePtr++<<8; goto bool_op;
		case OP_EQ: case OP_NE: case OP_LT: case OP_LE: case OP_GT: case OP_GE:
		case OP_CONTAINS: case OP_BEGINS: case OP_ENDS: case OP_REGEX: case OP_IN:
			if (((u=*codePtr++)&CND_EXT)!=0) u|=*codePtr++<<8;
			rc=top[-2].type!=VT_ERROR&&top[-1].type!=VT_ERROR?calc((ExprOp)op,top[-2],&top[-1],2,u,ma):op==OP_NE?RC_TRUE:RC_FALSE;
			freeV(*--top); freeV(*--top);
		bool_op:
			if (unsigned(rc-RC_TRUE)<=unsigned(RC_FALSE-RC_TRUE)) switch (cndAct[u&CND_MASK][rc-RC_TRUE]) {
			case 0: assert(top==stack); if (idx<nExp) {codePtr=codeEnd; continue;} return RC_TRUE;
			case 1: assert(top==stack); return RC_FALSE;
			case 2: if ((u&CND_MASK)>=6) mv_adv32(codePtr); rc=RC_OK; break;
			case 3: p=codePtr; mv_dec32(p,u); codePtr+=u; rc=RC_OK; break;
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

RC Expr::path(Value *v,PropertyID pid,unsigned flags,ElementID eid,Expr *filter,ClassID fcls,unsigned rmin,unsigned rmax)
{
	for (RC rc=RC_INTERNAL;;) {
		// (flags&0xE0)!=0 -> rmin/rmax
		// (flags&0x01)!=0 -> fRef
		// (flags&0x02)!=0 -> fDFS
		switch (v->type) {
		default: return RC_TYPE;
		case VT_REF:
			// getValue
			break;
		case VT_REFID:
			// get values
			break;
		case VT_ARRAY:
			// check refs, create PathIt
			break;
		case VT_COLLECTION:
			// create PathIt
			break;
		case VT_STRUCT:
			//???
			break;
		}
		return rc==RC_NOTFOUND&&rmin==0?RC_OK:rc;
	}
}

PathIt::~PathIt()
{
	//???
}

const Value *PathIt::navigate(GO_DIR dir,ElementID eid)
{
	//...
	return NULL;
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

RC Expr::calc(ExprOp op,Value& arg,const Value *moreArgs,int nargs,unsigned flags,MemAlloc *ma,const PINEx **vars,ulong nVars)
{
	int cmp,i; RC rc=RC_OK; long start,lstr; const Value *arg2,*arg3,*pv; unsigned dtPart;
	Value *rng,*args,val,val2; uint32_t len,sht; byte *p; char *s; RefVID *rv; Session *ses;
	switch (op) {
	case OP_PLUS:
		if (!isNumeric((ValueType)arg.type) || arg.type!=moreArgs->type) {
			if (arg.type==VT_DATETIME) {
				if (moreArgs->type!=VT_INTERVAL) {
					if ((rc=convV(*moreArgs,val,VT_INTERVAL))!=RC_OK) return rc;
					moreArgs=&val;
				}
			} else if (moreArgs->type==VT_INTERVAL) {
				if (arg.type!=VT_DATETIME && arg.type!=VT_INTERVAL &&
					(rc=convV(arg,arg,VT_DATETIME))!=RC_OK && (rc=convV(arg,arg,VT_INTERVAL))!=RC_OK) return rc;
			} else if (arg.type==VT_INTERVAL) {
				if (moreArgs->type!=VT_INTERVAL) {
					if ((rc=convV(*moreArgs,val,VT_INTERVAL))!=RC_OK) return rc;
					moreArgs=&val;
				}
			} else if ((moreArgs=numOpConv(arg,moreArgs,val))==NULL) return RC_TYPE;
		}
		switch (arg.type) {
		default: assert(0);
		case VT_INT:		arg.i+=moreArgs->i; break;
		case VT_UINT:		arg.ui+=moreArgs->ui; break;
		case VT_INT64:		arg.i64+=moreArgs->i64; break;
		case VT_UINT64:		arg.ui64+=moreArgs->ui64; break;
		case VT_DATETIME:	arg.ui64+=moreArgs->i64; break;
		case VT_INTERVAL:	arg.i64+=moreArgs->i64; break;	// interval + date -> date
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
		if (!isNumeric((ValueType)arg.type) || arg.type!=moreArgs->type) {
			if (arg.type==VT_DATETIME) {
				if (moreArgs->type!=VT_DATETIME && moreArgs->type!=VT_INTERVAL) {
					if ((rc=convV(*moreArgs,val,VT_DATETIME))!=RC_OK && 
						(rc=convV(*moreArgs,val,VT_INTERVAL))!=RC_OK) return rc;
					moreArgs=&val;
				}
			} else if (moreArgs->type==VT_INTERVAL) {
				if (arg.type!=VT_INTERVAL && (rc=convV(arg,arg,VT_INTERVAL))!=RC_OK) return rc;
			} else if (arg.type==VT_INTERVAL) {
				if (moreArgs->type!=VT_INTERVAL) {
					if ((rc=convV(*moreArgs,val,VT_INTERVAL))!=RC_OK) return rc;
					moreArgs=&val;
				}
			} else if ((moreArgs=numOpConv(arg,moreArgs,val))==NULL) return RC_TYPE;
		}
		switch (arg.type) {
		default: assert(0);
		case VT_INT:		arg.i-=moreArgs->i; break;
		case VT_UINT:		arg.ui-=moreArgs->ui; break;
		case VT_INT64:		arg.i64-=moreArgs->i64; break;
		case VT_UINT64:		arg.ui64-=moreArgs->ui64; break;
		case VT_DATETIME:	
			if (moreArgs->type==VT_INTERVAL) arg.ui64-=moreArgs->i64; 
			else {assert(moreArgs->type==VT_DATETIME); arg.i64=arg.ui64-moreArgs->ui64; arg.type=VT_INTERVAL;}
			break;
		case VT_INTERVAL:	arg.i64-=moreArgs->i64; break;
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
		if ((!isNumeric((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val))==NULL) return RC_TYPE;
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
		if ((!isNumeric((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val))==NULL) return RC_TYPE;
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
		if ((!isNumeric((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val))==NULL) return RC_TYPE;
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
		if (!isNumeric((ValueType)arg.type) && arg.type!=VT_INTERVAL && !numOpConv(arg)) return RC_TYPE;
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
		if (!isNumeric((ValueType)arg.type) && arg.type!=VT_INTERVAL && !numOpConv(arg)) return RC_TYPE;
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
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE)!=RC_OK) return RC_TYPE;
		else if (arg.d==0.) return RC_DIV0; else arg.d=log(arg.d);
		// Udim ???
		break;
	case OP_EXP:
		if (arg.type==VT_FLOAT) arg.f=expf(arg.f);
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE)!=RC_OK) return RC_TYPE;
		else arg.d=exp(arg.d);
		// Udim ???
		break;
	case OP_POW:
		// integer power???
		if (arg.type==VT_FLOAT && moreArgs->type==VT_FLOAT) arg.f=powf(arg.f,moreArgs->f);
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE)!=RC_OK) return RC_TYPE;
		else if ((arg2=moreArgs)->type!=VT_DOUBLE && (arg2=&val,convV(*moreArgs,val,VT_DOUBLE))!=RC_OK) return RC_TYPE;
		else arg.d=pow(arg.d,arg2->d);
		// Udim ???
		break;
	case OP_SQRT:
		if (arg.type==VT_FLOAT) {if (arg.f<0.f) return RC_INVPARAM; arg.f=sqrtf(arg.f);}
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE)!=RC_OK) return RC_TYPE;
		else if (arg.d<0.) return RC_INVPARAM; else arg.d=sqrt(arg.d);
		// Udim ???
		break;
	case OP_FLOOR:
		if (isInteger((ValueType)arg.type)) break;
		if (arg.type==VT_FLOAT) arg.f=floorf(arg.f);
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE)!=RC_OK) return RC_TYPE;
		else arg.d=::floor(arg.d);
		break;
	case OP_CEIL:
		if (isInteger((ValueType)arg.type)) break;
		if (arg.type==VT_FLOAT) arg.f=ceilf(arg.f);
		else if (arg.type!=VT_DOUBLE && convV(arg,arg,VT_DOUBLE)!=RC_OK) return RC_TYPE;
		else arg.d=::ceil(arg.d);
		break;
	case OP_NOT:
		if (!isInteger((ValueType)arg.type) && !numOpConv(arg,true)) return RC_TYPE;
		switch (arg.type) {
		default: assert(0);
		case VT_INT:		arg.i=~arg.i; break;
		case VT_UINT:		arg.ui=~arg.ui; break;
		case VT_INT64:		arg.i64=~arg.i64; break;
		case VT_UINT64:		arg.ui64=~arg.ui64; break;
		}
		break;
	case OP_AND:
		if ((!isInteger((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val,true))==NULL) return RC_TYPE;
		switch (arg.type) {
		default: assert(0);
		case VT_INT:	arg.i&=moreArgs->i; break;
		case VT_UINT:	arg.ui&=moreArgs->ui; break;
		case VT_INT64:	arg.i64&=moreArgs->i64; break;
		case VT_UINT64:	arg.ui64&=moreArgs->ui64; break;
		}
		break;
	case OP_OR:
		if ((!isInteger((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val,true))==NULL) return RC_TYPE;
		switch (arg.type) {
		default: assert(0);
		case VT_INT:	arg.i|=moreArgs->i; break;
		case VT_UINT:	arg.ui|=moreArgs->ui; break;
		case VT_INT64:	arg.i64|=moreArgs->i64; break;
		case VT_UINT64:	arg.ui64|=moreArgs->ui64; break;
		}
		break;
	case OP_XOR:
		if ((!isInteger((ValueType)arg.type) || arg.type!=moreArgs->type) && (moreArgs=numOpConv(arg,moreArgs,val,true))==NULL) return RC_TYPE;
		switch (arg.type) {
		default: assert(0);
		case VT_INT:	arg.i^=moreArgs->i; break;
		case VT_UINT:	arg.ui^=moreArgs->ui; break;
		case VT_INT64:	arg.i64^=moreArgs->i64; break;
		case VT_UINT64:	arg.ui64^=moreArgs->ui64; break;
		}
		break;
	case OP_LSHIFT:
		if (!isInteger((ValueType)arg.type) && !numOpConv(arg,true)) return RC_TYPE;
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
		if (!isInteger((ValueType)arg.type) && !numOpConv(arg,true)) return RC_TYPE;
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
		assert(nargs==2); cmp=Expr::cvcmp(arg,*moreArgs,flags);
		if (cmp==-1 && op==OP_MAX || cmp==1 && op==OP_MIN) {freeV(arg); if ((rc=copyV(*moreArgs,arg,ma))!=RC_OK) return rc;}
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
			} else if (!isString((ValueType)arg.type)) rc=convV(arg,arg,VT_STRING);
			return rc;
		}
		assert(nargs>=2&&nargs<255);
		if (!isString((ValueType)arg.type) && (rc=convV(arg,arg,VT_STRING))!=RC_OK) return rc;
		for (i=1,len=arg.length,args=(Value*)moreArgs; i<nargs; i++) {
			Value *arg2=&args[i-1];
			if (arg2->type!=arg.type) {
				if (arg2->type==VT_ERROR) {freeV(arg); arg.setError(); return RC_OK;}
				if ((const Value*)args==moreArgs) {
					args = (Value*)alloca((nargs-1)*sizeof(Value));
					memcpy(args,moreArgs,(nargs-1)*sizeof(Value));
					for (int j=nargs-1; --j>=0;) args[j].flags=0;
				}
				if ((rc=convV(*arg2,*arg2,(ValueType)arg.type))!=RC_OK) {	// ???
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
		if (!isString((ValueType)arg.type) && (rc=convV(arg,arg,VT_STRING))!=RC_OK || (rc=getI(moreArgs[0],lstr))!=RC_OK) return rc;
		if (nargs==3 && moreArgs[1].type==VT_ERROR) {freeV(arg); arg.setError(); return RC_OK;}
		if (nargs==2) start=0; else {start=lstr; if ((rc=getI(moreArgs[1],lstr))!=RC_OK) return rc;}
		if (lstr<0 || start<0 && -start>lstr) return RC_INVPARAM;
		if (lstr==0 || start==0 && arg.length>=(ulong)lstr) {arg.length=lstr; break;}
		p=(byte*)arg.bstr;
		if ((arg.flags&HEAP_TYPE_MASK)==NO_HEAP) {
			arg.bstr=(uint8_t*)ma->malloc(lstr); if (arg.bstr==NULL) return RC_NORESOURCES;
			arg.flags=arg.flags&~HEAP_TYPE_MASK|SES_HEAP;
		} else if ((ulong)lstr>arg.length) {
			arg.bstr=(uint8_t*)ma->realloc(p,lstr); if ((p=(byte*)arg.bstr)==NULL) return RC_NORESOURCES;
		}
		if (start<0 && -start<lstr) memmove((byte*)arg.bstr-start,p,min(uint32_t(lstr+start),arg.length));
		else if (start>=0 && (ulong)start<arg.length) memmove((byte*)arg.bstr,p+start,min(ulong(lstr),ulong(arg.length-start)));
		p=(byte*)arg.bstr; i=arg.type==VT_BSTR?0:' '; if (start<0) memset(p,i,-start);
		if (lstr>(long)arg.length-start) memset(p+arg.length-start,i,lstr-arg.length+start);
		arg.length=lstr; break;
	case OP_REPLACE:
		if (moreArgs[1].type==VT_ERROR) {freeV(arg); arg.setError(); return RC_OK;}
		if ((arg2=strOpConv(arg,moreArgs,val))==NULL || (arg3=strOpConv(arg,moreArgs+1,val2))==NULL) return RC_TYPE;
		assert(arg2->type==arg.type && arg3->type==arg.type);
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
				if ((arg.flags&HEAP_TYPE_MASK)==NO_HEAP) arg.str=p; else memcpy((char*)arg.str,p,arg.length);
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
		default: cmp=cvcmp(arg,*moreArgs,flags); break;
		case VT_ARRAY:
			for (len=moreArgs->length; len!=0; cmp=-2) {
				val=arg; val.flags=val.flags&~HEAP_TYPE_MASK|NO_HEAP; cmp=cvcmp(val,moreArgs->varray[--len],flags); freeV(val);
				if (op==OP_EQ) {if (cmp==0) break;} else if (cmp==-2 || cmp>=-1 && (compareCodeTab[op-OP_EQ]&1<<(cmp+1))==0) break;
			}
			break;
		case VT_COLLECTION:
			for (arg2=moreArgs->nav->navigate(GO_FIRST); arg2!=NULL; arg2=moreArgs->nav->navigate(GO_NEXT),cmp=-2) {
				val=arg; val.flags=val.flags&~HEAP_TYPE_MASK|NO_HEAP; cmp=cvcmp(val,*arg2,flags); freeV(val);
				if (op==OP_EQ) {if (cmp==0) break;} else if (cmp==-2 || cmp>=-1 && (compareCodeTab[op-OP_EQ]&1<<(cmp+1))==0) break;
			}
			moreArgs->nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID); break;
		}
		switch (cmp) {
		case -3: return RC_TYPE;
		case -2: return RC_FALSE;
		default: return (compareCodeTab[op-OP_EQ]&1<<(cmp+1))!=0?RC_TRUE:RC_FALSE;
		}
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
		if (op==OP_IS_A) switch (moreArgs->type) {
		default: return RC_TYPE;
		case VT_URIID:
			if (arg.type==VT_REFID) {
				if ((ses=Session::getSession())==NULL) return RC_NOSESSION; PINEx cb(ses,arg.id);
				return (rc=ses->getStore()->queryMgr->getBody(cb))!=RC_OK?rc:ses->getStore()->queryMgr->test(&cb,moreArgs->uid,nargs>2?&moreArgs[1]:NULL,nargs-2,nargs<=2)?RC_TRUE:RC_FALSE;
			} else if (arg.type==VT_REF) {
				PINEx pex((const PIN*)arg.pin); return arg.type==VT_REF&&StoreCtx::get()->queryMgr->test(&pex,moreArgs->uid,nargs>2?&moreArgs[1]:NULL,nargs-2,nargs<=2)?RC_TRUE:RC_FALSE;
			} else return RC_FALSE;
		case VT_ARRAY:
			for (len=moreArgs->length,rc=RC_FALSE; len!=0;) {
				arg2=&moreArgs->varray[--len];
				if (arg2->type==VT_URIID) {
					//...
				}
			}
			return RC_FALSE;
		case VT_COLLECTION:
			for (arg2=moreArgs->nav->navigate(GO_FIRST),rc=RC_FALSE; arg2!=NULL; arg2=moreArgs->nav->navigate(GO_NEXT)) {
				if (arg2->type==VT_URIID) {
					//...
					//  if ok -> break;
				}
			}
			moreArgs->nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID); return rc;
		} else switch (moreArgs->type) {
		default: return cvcmp(arg,*moreArgs,flags|CND_EQ)==0?RC_TRUE:RC_FALSE;
		case VT_RANGE:
			if ((cmp=moreArgs->range[0].type==VT_ANY?1:cvcmp(arg,moreArgs->range[0],(flags&CND_IN_LBND)!=0?flags|CND_NE:flags|CND_EQ))==-3) return RC_TYPE;
			if (cmp==-2 || (compareCodeTab[(flags&CND_IN_LBND)!=0?OP_GT-OP_EQ:OP_GE-OP_EQ]&1<<(cmp+1))==0) return RC_FALSE;
			if ((cmp=moreArgs->range[0].type==VT_ANY?-1:cvcmp(arg,moreArgs->range[1],(flags&CND_IN_RBND)!=0?flags|CND_NE:flags|CND_EQ))==-3) return RC_TYPE; 
			return cmp==-2 || (compareCodeTab[(flags&CND_IN_RBND)!=0?OP_LT-OP_EQ:OP_LE-OP_EQ]&1<<(cmp+1))==0?RC_FALSE:RC_TRUE;
		case VT_STMT:
			// VT_VARREF -> no load!!!
			if (arg.type==VT_REFID) {
				if ((ses=Session::getSession())==NULL) return RC_NOSESSION; PINEx cb(ses,arg.id);
				return (rc=ses->getStore()->queryMgr->getBody(cb))!=RC_OK?rc:((Stmt*)moreArgs->stmt)->checkConditions(&cb,NULL,0,ma)?RC_TRUE:RC_FALSE;
			}
			return arg.type==VT_REF&&((Stmt*)moreArgs->stmt)->isSatisfied(arg.pin)?RC_TRUE:RC_FALSE;
		case VT_ARRAY:
			for (len=moreArgs->length,rc=RC_FALSE; len!=0;) {
				val=arg; val.flags=val.flags&~HEAP_TYPE_MASK|NO_HEAP; arg2=&moreArgs->varray[--len]; bool f;
				if (arg2->type!=VT_RANGE) f=cvcmp(val,*arg2,flags|CND_EQ)==0;
				else {
					cmp=cvcmp(arg,arg2->range[0],(flags&CND_IN_LBND)!=0?flags|CND_NE:flags|CND_EQ);
					if (cmp<-1 || (compareCodeTab[OP_GE-OP_EQ]&1<<(cmp+1))==0) f=false;
					else {
						cmp=cvcmp(arg,arg2->range[1],(flags&CND_IN_RBND)!=0?flags|CND_NE:flags|CND_EQ);
						f=cmp>=-1 && (compareCodeTab[OP_LE-OP_EQ]&1<<(cmp+1))!=0;
					}
				}
				if ((val.flags&HEAP_TYPE_MASK)!=NO_HEAP) freeV(val); if (f) return RC_TRUE;
			}
			return RC_FALSE;
		case VT_COLLECTION:
			for (arg2=moreArgs->nav->navigate(GO_FIRST),rc=RC_FALSE; arg2!=NULL; arg2=moreArgs->nav->navigate(GO_NEXT)) {
				val=arg; val.flags=val.flags&~HEAP_TYPE_MASK|NO_HEAP; bool f;
				// CND_IS -> check class membership
				if (arg2->type!=VT_RANGE) f=cvcmp(val,*arg2,flags|CND_EQ)==0;
				else {
					cmp=cvcmp(arg,arg2->range[0],(flags&CND_IN_LBND)!=0?flags|CND_NE:flags|CND_EQ);
					if (cmp<-1 || (compareCodeTab[OP_GE-OP_EQ]&1<<(cmp+1))==0) f=false;
					else {
						cmp=cvcmp(arg,arg2->range[1],(flags&CND_IN_RBND)!=0?flags|CND_NE:flags|CND_EQ);
						f=cmp>=-1 && (compareCodeTab[OP_LE-OP_EQ]&1<<(cmp+1))!=0;
					}
				}
				if ((val.flags&HEAP_TYPE_MASK)!=NO_HEAP) freeV(val); if (f) {rc=RC_TRUE; break;}
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
		if (arg.type==VT_STRING||arg.type==VT_URL||(rc=convV(arg,arg,VT_STRING))==RC_OK) {
			s=forceCaseUTF8(arg.str,arg.length,len,ma,NULL,op==OP_UPPER); freeV(arg);
			if (s!=NULL) arg.set(s,len); else {arg.setError(); rc=RC_NORESOURCES;}
		}
		break;
	case OP_TONUM:
	case OP_TOINUM:
		switch (arg.type) {
		default: return RC_TYPE;
		case VT_STRING: return numOpConv(arg,op==OP_TOINUM)?RC_OK:RC_TYPE;
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
		if (flags<VT_STRING||flags>=VT_ALL) return RC_INVPARAM;
		if (arg.type!=moreArgs->ui) rc=convV(arg,arg,(ValueType)flags);
		break;
	case OP_RANGE:
		if (arg.type!=(arg2=moreArgs)->type && arg.type!=VT_ANY && arg2->type!=VT_ANY &&
			((arg2=cmpOpConv(arg,moreArgs,val))==NULL || arg2->type!=arg.type)) return RC_TYPE;
		if ((rng=new(ma) Value[2])==NULL) return RC_NORESOURCES;
		rng[0]=arg;
		if (arg2==&val) rng[1]=val;
		else if ((rc=copyV(*arg2,rng[1],ma))!=RC_OK) {free(rng,SES_HEAP); return rc;}
		rng[0].property=rng[1].property=STORE_INVALID_PROPID;
		rng[0].eid=rng[1].eid=STORE_COLLECTION_ID;
		arg.setRange(rng); arg.flags=ma->getAType(); break;
	case OP_ARRAY:
		//???
		break;
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
			if (arg.type!=VT_REFID && (rc=convV(arg,arg,VT_REFID))!=RC_OK) return rc;
			arg.setIdentity(arg.id.ident);
		} else {
			if (arg.type!=VT_DATETIME && (rc=convV(arg,arg,VT_DATETIME))!=RC_OK
				|| (rc=getDTPart(arg.ui64,dtPart,flags))!=RC_OK) return rc;
			arg.set(dtPart);
		}
		break;
	case OP_DEREF:
		if ((rc=derefValue(arg,arg,Session::getSession()))!=RC_OK) return rc;
		break;
	case OP_REF:
		switch (arg.type) {
		default: if ((rc=convV(arg,arg,VT_REFID))!=RC_OK) return rc;
		case VT_REFID: if (nargs==1) return RC_OK; break;
		case VT_REFIDPROP: case VT_REFIDELT: return nargs==1?RC_OK:RC_TYPE;
		}
		if ((arg2=moreArgs)->type!=VT_URIID) {
			if ((rc=convV(*moreArgs,val,VT_URIID))!=RC_OK) return rc;
			arg2=&val;
		}
		if ((rv=new(SES_HEAP) RefVID)==NULL) return RC_NORESOURCES;
		rv->id=arg.id; rv->pid=arg2->uid; arg.set(*rv);
		if (nargs>=3) {
			if ((arg2=&moreArgs[1])->type!=VT_INT && arg2->type!=VT_UINT)
				{if ((rc=convV(*arg2,val,VT_UINT))==RC_OK) arg2=&val; else return rc;}
			rv->eid=arg2->ui; arg.type=VT_REFIDELT;
		}
		break;
	case OP_CALL:
		if (arg.type==VT_STRING) {
			// try to compile
		}
		if ((ses=Session::getSession())==NULL) return RC_NOSESSION;
		if ((rc=ses->getStore()->queryMgr->eval(ses,&arg,arg,vars,nVars,moreArgs,nargs-1,ses,false))!=RC_OK) return rc;
		break;
	default:
		return RC_INTERNAL;
	}
	return RC_OK;
}

void AggAcc::next(const Value& v)
{
	assert(v.type!=VT_ERROR);
	if (op==OP_COUNT) count++;
	else if (sum.type==VT_ERROR) {
		sum=v; sum.flags=NO_HEAP;
		if (op==OP_SUM) {
			if (isNumeric((ValueType)sum.type) || sum.type==VT_INTERVAL && Expr::numOpConv(sum)) count=1; else sum.type=VT_ERROR;
		} else if (op>=OP_AVG) {
			if (sum.type==VT_DOUBLE || convV(sum,sum,VT_DOUBLE)==RC_OK) count=1; else sum.type=VT_ERROR;
		} else {
			if (copyV(v,sum,ma)==RC_OK) count=1; else sum.type=VT_ERROR;
		}
	} else if (op==OP_MIN || op==OP_MAX) {
		int cmp=Expr::cvcmp(sum,v,flags);
		if (cmp==-1 && op==OP_MAX || cmp==1 && op==OP_MIN) {freeV(sum); copyV(v,sum,ma);}
	} else {
		const Value *pv=&v; Value val;
		if (op!=OP_COUNT) try {
			if (pv->type!=sum.type && (pv=Expr::numOpConv(sum,pv,val))==NULL) return;
			switch (sum.type) {
			default: assert(0);
			case VT_INT: sum.i+=pv->i; assert(op==OP_SUM); break;
			case VT_UINT: sum.ui+=pv->ui; assert(op==OP_SUM); break;
			case VT_INT64: case VT_INTERVAL: sum.i64+=pv->i64; assert(op==OP_SUM); break;
			case VT_UINT64: case VT_DATETIME: sum.ui64+=pv->ui64; assert(op==OP_SUM); break;
			case VT_FLOAT: sum.f+=pv->f; assert(op==OP_SUM); break;
			case VT_DOUBLE:	sum.d+=pv->d; if (op>=OP_VAR_POP) sum2+=pv->d*pv->d; break;
			}
		} catch (...) {return;}
		count++;
	}
}

RC AggAcc::result(Value& res)
{
	try {
		switch (op) {
		case OP_COUNT: res.setU64(count); break;
		case OP_MIN: case OP_MAX: case OP_SUM: res=sum; sum.flags=NO_HEAP; break;
		case OP_AVG:
			if (count==0) res.setError(); else {assert(sum.type==VT_DOUBLE); res.set(sum.d/(double)count);}
			break;
		case OP_VAR_POP: case OP_VAR_SAMP: case OP_STDDEV_POP: case OP_STDDEV_SAMP:
			if (count==0 || count==1 && (op==OP_VAR_SAMP || op==OP_STDDEV_SAMP)) res.setError();
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
	} catch (...) {res.setError(); return RC_TOOBIG;}
	return RC_OK;
}

RC Expr::calcAgg(ExprOp op,Value& res,const Value *more,unsigned nargs,unsigned flags,MemAlloc *ma)
{
	AggAcc aa(op,flags,ma); const Value *pv=&res,*cv; uint32_t i;
	if ((flags&CND_DISTINCT)!=0) {
		//...
	}
	for (unsigned idx=0; idx<nargs; pv=&more[idx++]) switch (pv->type) {
	case VT_ERROR: break;
	case VT_ARRAY:
		if (op==OP_COUNT && (flags&CND_DISTINCT)==0) aa.count+=pv->length;
		else for (i=0; i<pv->length; i++) if ((flags&CND_DISTINCT)==0) aa.next(pv->varray[i]);
		else {
			//...
		}
		break;
	case VT_COLLECTION:
		if (op==OP_COUNT && (flags&CND_DISTINCT)==0) aa.count+=pv->nav->count();
		else {
			for (cv=pv->nav->navigate(GO_FIRST); cv!=NULL; cv=pv->nav->navigate(GO_NEXT))
				if ((flags&CND_DISTINCT)==0) aa.next(*cv);
				else {
					//...
				}
		}
		break;
	case VT_STMT:
		if (op==OP_COUNT && (flags&CND_DISTINCT)==0) {uint64_t cnt=0ULL;  if (pv->stmt->count(cnt)==RC_OK) aa.count+=cnt;}
		else {
			// cursor, etc.
		}
		break;
	default:
		if ((flags&CND_DISTINCT)!=0) {
			//...
		} else if (op==OP_COUNT) aa.count++; else aa.next(*pv);
		break;
	}
	if ((flags&CND_DISTINCT)!=0) {
		// sort
		// feed results to aa
	}
	freeV(res); return aa.result(res);
}

int Expr::cmp(const Value& arg,const Value& arg2,ulong u)
{
	assert(arg.type==arg2.type);
	ulong len; int cmp; QualifiedValue q1,q2;
	switch (arg.type) {
	default: break;
	case VT_STRING: case VT_URL: case VT_BSTR:
		if (arg.str==NULL||arg.length==0) return arg2.str==NULL||arg2.length==0?0:-1;
		if (arg2.str==NULL||arg2.length==0) return 1;
		len=arg.length<=arg2.length?arg.length:arg2.length;
		cmp=arg.type==VT_BSTR||(u&(CND_EQ|CND_NE))!=0&&(u&CND_NCASE)==0?memcmp(arg.bstr,arg2.bstr,len):
					(u&CND_NCASE)!=0?strncasecmp(arg.str,arg2.str,len):strncmp(arg.str,arg2.str,len);
		return cmp<0?-1:cmp>0?1:cmp3(arg.length,arg2.length);
	case VT_INT: return cmp3(arg.i,arg2.i);
	case VT_UINT: return cmp3(arg.ui,arg2.ui);
	case VT_INTERVAL:
	case VT_INT64: return cmp3(arg.i64,arg2.i64);
	case VT_DATETIME:
	case VT_UINT64: return cmp3(arg.ui64,arg2.ui64);
	case VT_FLOAT:
		if (arg.qval.units==arg2.qval.units) return cmp3(arg.f,arg2.f);
		q1.d=arg.f; q1.units=arg.qval.units; q2.d=arg2.f; q2.units=arg2.qval.units; 
		return !compatible(q1,q2)?-3:cmp3(q1.d,q2.d);
	case VT_DOUBLE:
		if (arg.qval.units==arg2.qval.units) return cmp3(arg.d,arg2.d);
		q1=arg.qval; q2=arg2.qval; return !compatible(q1,q2)?-3:cmp3(q1.d,q2.d);
	case VT_REF: return (u&CND_SORT)!=0?cmpPIDs(arg.pin->getPID(),arg2.pin->getPID()):arg.pin->getPID()==arg2.pin->getPID()?0:(u&CND_NE)!=0?-1:-2;
	case VT_REFPROP: return arg.ref.pin->getPID()==arg2.ref.pin->getPID()&&arg.ref.pid==arg.ref.pid?0:(u&CND_NE)!=0?-1:-2;								// CND_SORT
	case VT_REFELT: return arg.ref.pin->getPID()==arg2.ref.pin->getPID()&&arg.ref.pid==arg.ref.pid&&arg.ref.eid==arg2.ref.eid?0:(u&CND_NE)!=0?-1:-2;	// CND_SORT
	case VT_REFID: return (u&CND_SORT)!=0?cmpPIDs(arg.id,arg2.id):arg.id==arg2.id?0:(u&CND_NE)!=0?-1:-2;
	case VT_REFIDPROP: return arg.refId->id==arg2.refId->id&&arg.refId->pid==arg2.refId->pid?0:(u&CND_NE)!=0?-1:-2;										// CND_SORT
	case VT_REFIDELT:return arg.refId->id==arg2.refId->id&&arg.refId->pid==arg2.refId->pid&&arg.refId->eid==arg2.refId->eid?0:(u&CND_NE)!=0?-1:-2;		// CND_SORT
	case VT_BOOL: return arg.b==arg2.b?0:(u&CND_SORT)!=0?arg.b<arg2.b?-1:1:(u&CND_NE)!=0?-1:-2;
	case VT_URIID: return arg.uid==arg2.uid?0:(u&CND_SORT)!=0?arg.uid<arg2.uid?-1:1:(u&CND_NE)!=0?-1:-2;
	case VT_IDENTITY: return arg.iid==arg2.iid?0:(u&CND_SORT)!=0?arg.iid<arg2.iid?-1:1:(u&CND_NE)!=0?-1:-2;
	}
	return -3;
}

int Expr::cvcmp(Value& arg,const Value& arg2,ulong u)
{
	Value val; const Value *pv=&arg2;
	if (arg.type!=arg2.type && ((pv=cmpOpConv(arg,pv,val))==NULL || pv->type!=arg.type)) return -3;
	int ret=cmp(arg,*pv,u); if (pv==&val) freeV(val);
	return ret;
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
	ValueType vt1=(ValueType)arg.type,vt2=(ValueType)arg2->type;
	if (isString(vt1)) {
		if (vt1>=VT_URL) arg.type=vt1=(ValueType)(vt1-(VT_URL-VT_STRING));
		if (isString(vt2)) {
			if (vt2==VT_BSTR) arg.type=VT_BSTR; else if (vt2>=VT_URL) vt2=(ValueType)(vt2-(VT_URL-VT_STRING));
		} else {
			if (convV(*arg2,buf,vt1)!=RC_OK) return NULL;
			arg2=&buf;
		}
	} else if (isString(vt2)) {
		if (vt2>=VT_URL) vt2=(ValueType)(vt2-(VT_URL-VT_STRING));
		if (convV(arg,arg,vt2)!=RC_OK) return NULL;
	} else {
		if (convV(arg,arg,VT_STRING)!=RC_OK || convV(*arg2,buf,VT_STRING)!=RC_OK) return NULL;
		arg2=&buf;
	}
	return arg2;
}

enum CmpCvT {_N,_L,_R,_VL,_VR,_VB,_WL,_WR,_WB,_S=0x80,_LS=_L|_S,_RS=_R|_S};

static const CmpCvT cmpConvTab[VT_ALL][VT_ALL] = 
{
/*
ERR STR BST URL ENU INT UI  I64 U64 DEC FLT DBL BOO DT  ITV URI IDN REF RFI RFV RFIV RFE RFIE EXP QRY ARR COL SRU RNG STM NOW PRM VAR EXPT 
*/
{_N,_N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_ERROR
{_N,_N, _R, _L, _L, _RS,_RS,_RS,_RS,_L, _RS,_RS,_RS,_L, _L, _L, _L, _VR,_WR,_L, _L,  _L, _L,  _N, _N, _N, _N, _N, _N, _L, _L, _N, _N, _N,  }, //VT_STRING
{_N,_L, _N, _L, _L, _L, _L, _L, _L, _L, _L, _L, _L, _L, _L, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _L, _N, _N, _N,  }, //VT_BSTR
{_N,_R, _R, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VR,_WR,_L, _L,  _L, _L,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_URL
{_N,_R, _R, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_ENUM
{_N,_LS,_R, _N, _N, _N, _R, _R, _R, _N, _R, _R, _L, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_INT
{_N,_LS,_R, _N, _N, _L, _N, _R, _R, _N, _R, _R, _L, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_UINT
{_N,_LS,_R, _N, _N, _L, _L, _N, _R, _N, _R, _R, _L, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_INT64
{_N,_LS,_R, _N, _N, _L, _L, _L, _N, _N, _R, _R, _L, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_UINT64
{_N,_R, _R, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_DECIMAL	?????
{_N,_LS,_R, _N, _N, _L, _L, _L, _L, _N, _N, _R, _N, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_FLOAT
{_N,_LS,_R, _N, _N, _L, _L, _L, _L, _N, _L, _N, _N, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_DOUBLE
{_N,_R, _R, _N, _N, _R, _R, _R, _R, _N, _N, _N, _N, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_BOOL
{_N,_R, _R, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _L, _N, _N, _N,  }, //VT_DATETIME
{_N,_R, _R, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_INTERVAL
{_N,_R, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_URIID
{_N,_R, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_IDENTITY
{_N,_VL,_N, _VL,_VL,_VL,_VL,_VL,_VL,_VL,_VL,_VL,_VL,_VL,_VL,_VL,_VL,_N, _R, _VB,_VB, _VB,_VB, _VL,_VL,_VL,_VL,_VL,_VL,_VL,_VL,_VL,_VL,_VL, },//VT_REF
{_N,_WL,_WL,_WL,_WL,_WL,_WL,_WL,_WL,_WL,_WL,_WL,_WL,_WL,_WL,_WL,_WL,_L, _N, _WB,_WB, _WB,_WB, _WL,_WL,_WL,_WL,_WL,_WL,_WL,_WL,_WL,_WL,_WL, },//VT_REFID
{_N,_R, _N, _R, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VB,_WB,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_REFPROP
{_N,_R, _N, _R, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VB,_WB,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_REFIDPROP
{_N,_R, _N, _R, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VB,_WB,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_REFELT
{_N,_R, _N, _R, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VB,_WB,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_REFIDELT
{_N,_N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_EXPR
{_N,_N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_STMT
{_N,_N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_ARRAY
{_N,_N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_COLLECTION
{_N,_N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_STRUCT
{_N,_N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_RANGE
{_N,_R, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_STREAM
{_N,_R, _R, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _R, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_CURRENT		//???
{_N,_N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_PARAM
{_N,_N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_VARREF
{_N,_N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _VR,_WR,_N, _N,  _N, _N,  _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N,  }, //VT_EXPRTREE
};

const Value *Expr::cmpOpConv(Value& arg,const Value *arg2,Value& buf)
{
	for (;;) {
		byte cv=cmpConvTab[arg.type][arg2->type]; Session *ses;
		switch (cv&~_S) {
		default: return NULL;
		case _R:
			if (convV(arg,arg,(ValueType)arg2->type)==RC_OK) return arg2;
			if ((cv&_S)==0) return NULL;
			break;
		case _L:
			if (convV(*arg2,buf,(ValueType)arg.type)==RC_OK) return &buf;
			if ((cv&_S)==0) return NULL;
			break;
		case _VL: case _VB:
			if (((PIN*)arg.pin)->getPINValue(buf)!=RC_OK) return NULL;
			freeV(arg); arg=buf;
			if (cv==_VB) {
				//...
				return NULL;
			}
			continue;
		case _VR:
			if (((PIN*)arg2->pin)->getPINValue(buf)!=RC_OK) return NULL;
			arg2=&buf; continue;
		case _WL: case _WB:
			if ((ses=Session::getSession())==NULL) return NULL;
			if (ses->getStore()->queryMgr->getPINValue(arg.id,arg,0,ses)!=RC_OK) return NULL;
			if (cv==_WB) {
				//...
				return NULL;
			}
			continue;
		case _WR:
			if ((ses=Session::getSession())==NULL) return NULL;
			if (ses->getStore()->queryMgr->getPINValue(arg2->id,buf,0,ses)!=RC_OK) return NULL;
			arg2=&buf; continue;
		}
		assert((cv&_S)!=0);
		return convV(arg,arg,VT_STRING)==RC_OK&&convV(*arg2,buf,VT_STRING)==RC_OK?&buf:NULL;
	}
}

const Value *Expr::numOpConv(Value& arg,const Value *arg2,Value& buf,bool fInt)
{
	if (!isNumeric((ValueType)arg.type) && !numOpConv(arg)) return NULL;
	if (!isNumeric((ValueType)arg2->type)) {
		buf=*arg2; buf.flags=NO_HEAP;
		if (!numOpConv(buf)) return NULL;
		arg2=&buf;
	}
	if (arg.type!=arg2->type) {
		switch (cmpConvTab[arg.type][arg2->type]&~_S) {
		default: return NULL;
		case _R: if (convV(arg,arg,(ValueType)arg2->type)!=RC_OK) return NULL; break;
		case _L: if (convV(*arg2,buf,(ValueType)arg.type)!=RC_OK) return NULL; arg2=&buf; break;
		}
	}
	if (fInt && !isInteger((ValueType)arg.type)) {
		if (arg2!=&buf) {buf=*arg2; arg2=&buf;}
		switch (arg.type) {
		default: assert(0);
		case VT_FLOAT: arg.i=(int32_t)arg.f; buf.i=(int32_t)buf.f; arg.type=buf.type=VT_INT; break;
		case VT_DOUBLE: arg.i64=(int64_t)arg.d; buf.i64=(int64_t)buf.d; arg.type=buf.type=VT_INT64; break;
		}
	}
	return arg2;
}

bool Expr::numOpConv(Value& arg,bool fInt)
{
	Value val; RC rc;
	for (;;) switch (arg.type) {
	default: return false;
	case VT_INT: case VT_UINT: case VT_INT64: case VT_UINT64: return true;
	case VT_FLOAT: if (fInt) {arg.i=(int32_t)arg.f; arg.type=VT_INT;} return true;
	case VT_DOUBLE: if (fInt) {arg.i64=(int64_t)arg.d; arg.type=VT_INT64;} return true;
	case VT_STRING:
		if ((rc=strToNum(arg.str,arg.length,val))!=RC_OK) return false;
		freeV(arg); arg=val;
		if (fInt && (arg.type==VT_FLOAT||arg.type==VT_DOUBLE)) continue;
		return true;
	case VT_REF: case VT_REFID: case VT_REFPROP: case VT_REFELT: case VT_REFIDPROP: case VT_REFIDELT:
		if (derefValue(arg,arg,Session::getSession())==RC_OK) continue;
		return false;
	}
}

//---------------------------------------------------------------------------------------------------

RC Expr::compile(const ExprTree *tr,Expr *&expr,MemAlloc *ma,PropDNF **pd,size_t *pl)
{
	ExprCompileCtx ctx(ma,pd!=NULL && pl!=NULL); expr=NULL;
	RC rc=isBool(tr->op)?(ctx.flags|=EXPR_BOOL,ctx.compileCondition(tr,0,0)):ctx.compileNode(tr);
	if (rc==RC_OK) {
		size_t lExpr; byte *p=ctx.result(lExpr); assert(lExpr>sizeof(Expr)); 
		if (p!=NULL) expr=new(p) Expr(uint32_t(lExpr-sizeof(Expr)+sizeof(ExprHdr)),(uint16_t)ctx.xStack,(uint16_t)ctx.flags);
		if (ctx.fDNF) {*pd=ctx.dnf; ctx.dnf=NULL; *pl=ctx.ldnf;}
	}
	return rc;
}

ExprCompileCtx::ExprCompileCtx(MemAlloc *ma,bool fD) 
	: OutputBuf(ma,sizeof(Expr),256),labelCount(0),lStack(0),xStack(0),flags(0),labels(NULL),dnf(NULL),ldnf(0),fDNF(fD) {}

ExprCompileCtx::~ExprCompileCtx()
{
	for (ExprLbl *lb=labels,*lb2; lb!=NULL; lb=lb2) {lb2=lb->next; ma->free(lb);}
}

RC ExprCompileCtx::compileNode(const ExprTree *node,ulong flg)
{
	if (isBool(node->op)) {
		return RC_INTERNAL;		//compileCondition(node,???,0);
	}
	unsigned i; RC rc; byte *p; uint32_t lbl; Expr *filter;
	unsigned l=node->nops!=SInCtx::opDscr[node->op].nOps[0]?3:1; 
	byte f=(node->flags&CASE_INSENSITIVE_OP)!=0?CND_NCASE:0;
	switch ((byte)node->op) {
	case OP_CON:
		assert(node->nops==1); return compileValue(node->operands[0],flg|((node->flags&CARD_OP)!=0?CV_CARD:0));
	case OP_COALESCE:
		lbl=++labelCount;
		if ((p=alloc(1))==NULL) return RC_NORESOURCES; p[0]=OP_CATCH|0x80;
		for (i=0; i<unsigned(node->nops-1); i++) {
			if ((rc=compileValue(node->operands[i],flg))!=RC_OK) return rc;
			if ((p=alloc(2))==NULL) return RC_NORESOURCES;
			if ((labels=new(ma) ExprLbl(labels,lbl,uint32_t(cLen-1)))==NULL) return RC_NORESOURCES;
			p[0]=OP_COALESCE; lStack--;
		}
		if ((p=alloc(1))==NULL) return RC_NORESOURCES; p[0]=OP_CATCH;
		if ((rc=compileValue(node->operands[node->nops-1],flg))!=RC_OK) return rc;
		adjustRef(lbl); return RC_OK;
	case OP_CAST:
	case OP_EXTRACT:
		assert(node->nops==2);
		if ((node->operands[1].type==VT_INT||node->operands[1].type==VT_UINT) && node->operands[1].ui<=0xFF) {
			if ((rc=compileValue(node->operands[0],flg))!=RC_OK) return rc;
			if ((p=alloc(2))==NULL) return RC_NORESOURCES;
			p[0]=(byte)node->op|0x80; p[1]=(byte)node->operands[1].ui;
			assert(lStack>=1); return RC_OK;
		}
		break;
	case OP_TRIM:
		assert(node->nops==3);
		if ((node->operands[2].type==VT_INT||node->operands[2].type==VT_UINT) && node->operands[2].ui<=2) {
			if ((rc=compileValue(node->operands[0],flg))!=RC_OK || (rc=compileValue(node->operands[1],flg))!=RC_OK) return rc;
			if ((p=alloc(2))==NULL) return RC_NORESOURCES;
			p[0]=OP_TRIM|0x80; p[1]=(byte)node->operands[2].ui;
			assert(lStack>=2); --lStack; return RC_OK;
		}
		break;
	case OP_RSHIFT:
		if ((node->flags&UNSIGNED_OP)!=0) {f|=CND_UNS; l++;}
		break;
	case OP_PATH:
		if ((rc=compileValue(node->operands[0],flg))!=RC_OK) return rc;
		if (node->operands[1].type==VT_URIID) {f=0; l=mv_len32(node->operands[1].uid);}
		else {f=0x4; l=0; if ((rc=compileValue(node->operands[1],flg))!=RC_OK) return rc;}
		if (node->nops<4) f|=(node->flags&(QUEST_PATH_OP|PLUS_PATH_OP|STAR_PATH_OP))<<5;
		filter=NULL;
		if (node->nops>2) {
			switch (node->operands[2].type) {
			case VT_ERROR: break;
			case VT_INT: case VT_UINT:
				if (node->operands[2].ui!=STORE_COLLECTION_ID) {	f|=0x08; i=mv_enc32zz(node->operands[2].ui); l+=mv_len32(i);	}
				break;
			case VT_EXPRTREE:
				if (isBool(((ExprTree*)node->operands[2].exprt)->op)) {
					// classID ?
					if ((rc=Expr::compile((ExprTree*)node->operands[2].exprt,filter,ma))!=RC_OK) return rc;
					l+=(unsigned)filter->serSize(); f|=0x18; break;
				}
			default:
				if ((rc=compileValue(node->operands[2],flg))!=RC_OK) return rc;
				f|=0x10; break;
			}
			if (node->nops>3) {
				// const1,2, param1, param1,2
			}
		}
		if ((p=alloc((f!=0?2:1)+l))==NULL) return RC_NORESOURCES;
		if (f==0) *p++=OP_PATH; else {p[0]=OP_PATH|0x80; p[1]=f; p+=2;}
		switch (f&0xE0) {
		default: break;
		case 0x80: //mv_dec32(codePtr,rmin); mv_dec32(codePtr,rmax); break;
		case 0xA0:
			//mv_dec32(codePtr,rmin); // rmin=rmax=params[rmin];
			break;
		case 0xC0:
			//mv_dec32(codePtr,rmin); mv_dec32(codePtr,rmax);
			// rmin=params[rmin]; rmax=params[rmax];
			break;
		}
		switch (f&0x18) {
		default: break;
		case 0x08: l=mv_enc32zz(node->operands[2].i); mv_enc32(p,l); break;
		case 0x18: p=filter->serialize(p); break;		// classID??
		}
		if ((f&4)==0) mv_enc32(p,node->operands[1].uid);
		return RC_OK;
	default:
		if (node->op<OP_ALL && (SInCtx::opDscr[node->op].flags&_A)!=0 && (node->flags&DISTINCT_OP)!=0) {f|=CND_DISTINCT; if (l==1) l=2;}
		break;
	}
	for (i=0; i<node->nops; i++) if ((rc=compileValue(node->operands[i],flg))!=RC_OK) return rc;
	if ((p=alloc(l))==NULL) return RC_NORESOURCES;
	if (l==1) *p=(byte)node->op; else {p[0]=(byte)node->op|0x80; p[1]=f; if (l>2) p[2]=(byte)node->nops;}
	assert(lStack>=node->nops && node->nops>0);
	lStack-=node->nops-1;
	return RC_OK;
}

RC ExprCompileCtx::compileValue(const Value& v,ulong flg) {
	RC rc; size_t l; byte *p,fc=(flg&CV_CARD)!=0?0x80:0; Value val;
	switch (v.type) {
	case VT_EXPRTREE: return compileNode((const ExprTree*)v.exprt);
	case VT_PARAM:
		flags|=EXPR_PARAMS; 
		if ((p=alloc(2))==NULL) return RC_NORESOURCES;
		p[0]=OP_PARAM|fc; p[1]=v.refPath.refN; break;	// type???
	case VT_VARREF:
		if (v.length==0) {
			if ((p=alloc(2))==NULL) return RC_NORESOURCES; p[0]=OP_VAR|fc; p[1]=v.refPath.refN;
		} else if (fDNF && (flg&(CV_CARD|CV_NE))==0 && v.refPath.id!=PROP_SPEC_ANY && v.refPath.id!=PROP_SPEC_PINID 
			&& v.refPath.id!=PROP_SPEC_STAMP && (rc=PropDNF::andP(dnf,ldnf,&v.refPath.id,1,ma,false))!=RC_OK) return rc;
		else {
			byte op=OP_PROP; l=2+mv_len32(v.refPath.id); unsigned eid=0;
			if (v.eid!=STORE_COLLECTION_ID) {op=OP_ELT; eid=mv_enc32zz(v.eid); l+=mv_len32(eid);}
			if ((p=alloc(l))==NULL) return RC_NORESOURCES;
			p[0]=op|fc; p[1]=v.refPath.refN; p+=2; mv_enc32(p,v.refPath.id);
			if (op==OP_ELT) mv_enc32(p,eid);
		}
		break;
	case VT_CURRENT:
		if ((p=alloc(2))==NULL) return RC_NORESOURCES; 
		p[0]=OP_CURRENT; p[1]=(byte)v.ui; break;
	case VT_STREAM:
		if ((rc=streamToValue(v.stream.is,val,ma))!=RC_OK) return rc;
		if ((l=MVStoreKernel::serSize(val))==0) rc=RC_TYPE;
		else if ((p=alloc(l+1))==NULL) rc=RC_NORESOURCES;
		else {*p=OP_CON|fc; MVStoreKernel::serialize(val,p+1);}
		freeV(val); if (rc!=RC_OK) return rc;
		break;
	default: 
		if ((l=MVStoreKernel::serSize(v))==0) return RC_TYPE; if ((p=alloc(l+1))==NULL) return RC_NORESOURCES;
		*p=OP_CON|fc; MVStoreKernel::serialize(v,p+1); break;
	}
	if (++lStack>xStack) xStack=lStack; return RC_OK;
}

RC ExprCompileCtx::putCondCode(ExprOp op,ulong fa,uint32_t lbl,bool flbl,int nops) {
	const bool fJump=(fa&CND_MASK)>=6; size_t l=nops!=0?3:2;
	if (fa>0x7F) {fa|=CND_EXT; l++;} if (fJump) l+=flbl?1:mv_len32(lbl);
	byte *p=alloc(l); if (p==NULL) return RC_NORESOURCES;
	p[0]=op; if (nops!=0) {p[1]=(byte)nops; p++;}
	p[1]=byte(fa); p+=2; if ((fa&CND_EXT)!=0) *p++=byte(fa>>8);
	if (fJump) {
		if (!flbl) {mv_enc32(p,lbl);}
		else if ((labels=new(ma) ExprLbl(labels,lbl,uint32_t(cLen-1)))==NULL) return RC_NORESOURCES;
	}
	return RC_OK;
}

void ExprCompileCtx::adjustRef(uint32_t lbl) {
	bool fDel=true;
	for (ExprLbl *lb,**plb=&labels; (lb=*plb)!=NULL; ) {
		if (uint32_t(lb->label)!=lbl) {plb=&lb->next; fDel=false;}
		else {
			uint32_t l=uint32_t(cLen)-lb->addr/*,ll=mv_len32(l)*/;
			if (l>1) {
				//byte *p=alloc(ll-1);
				//move code, adjust jump addrs
			}
			byte *p=ptr+lb->addr; mv_enc32(p,l);
			if (!fDel) plb=&lb->next; else {*plb=lb->next; ma->free(lb);}
		}
	}
}

RC ExprCompileCtx::compileCondition(const ExprTree *node,ulong flags,uint32_t lbl) {
	RC rc=RC_OK; uint32_t lbl1; PropDNF *sdnf; size_t sldnf; ulong ff=0,i;
	ExprOp op=node->op; const Value *pv; bool fJump;
	switch (op) {
	default: assert(0); rc=RC_INTERNAL; break;
	case OP_LAND: case OP_LOR:
		assert(dnf==NULL && ldnf==0 && node->nops==2 && (node->flags&NOT_BOOLEAN_OP)==0);
		ff=op-OP_LAND; fJump=jumpOp[flags][ff]; lbl1=fJump?++labelCount:lbl;
		if ((rc=compileCondition((const ExprTree*)node->operands[0].exprt,logOp[flags][ff],lbl1))==RC_OK) {
			lStack=0; sdnf=dnf; sldnf=ldnf; dnf=NULL; ldnf=0;
			if ((rc=compileCondition((const ExprTree*)node->operands[1].exprt,flags,lbl))!=RC_OK) ma->free(sdnf);
			else {
				if (fJump) adjustRef(lbl1);
				if (fDNF) rc=op==OP_LAND?PropDNF::andP(dnf,ldnf,sdnf,sldnf,ma):PropDNF::orP(dnf,ldnf,sdnf,sldnf,ma);
			}
		}
		break;
	case OP_IN:
		pv=&node->operands[1];
		switch (pv->type) {
		case VT_ARRAY: if (pv->length==0) break;
		case VT_COLLECTION:
//			if ((node->flags&FOR_ALL_RIGHT_OP)!=0) flags|=CND_FORALL_R;			-- check!
//			if ((node->flags&EXISTS_RIGHT_OP)!=0) flags|=CND_EXISTS_R;
			if ((rc=compileValue(node->operands[0],ff))!=RC_OK) return rc;
			if ((node->flags&CASE_INSENSITIVE_OP)!=0) flags|=CND_NCASE;
			if ((node->flags&FOR_ALL_LEFT_OP)!=0) flags|=CND_FORALL_L;
			if ((node->flags&EXISTS_LEFT_OP)!=0) flags|=CND_EXISTS_L;
			if ((node->flags&NOT_BOOLEAN_OP)!=0) flags^=CND_NOT|1;
			if (pv->type==VT_ARRAY) for (i=0;;) {
				if ((rc=compileValue(pv->varray[i],ff))!=RC_OK) return rc;
				if (++i>=pv->length) break;
				if ((rc=putCondCode((ExprOp)OP_IN1,flags,lbl))!=RC_OK) return rc;
				assert(lStack>=1); --lStack;	 	// if no value required
			} else {
				const Value *cv=pv->nav->navigate(GO_FIRST); if (cv==NULL) break;
				for (;;) {
					if ((rc=compileValue(*cv,ff))!=RC_OK) return rc;
					if ((cv=pv->nav->navigate(GO_NEXT))==NULL) break;
					if ((rc=putCondCode((ExprOp)OP_IN1,flags,lbl))!=RC_OK) return rc;
					assert(lStack>=1); --lStack;	 	// if no value required
				}
			}
			if ((rc=putCondCode(ExprOp(OP_IN|0x80),flags,lbl))==RC_OK) {assert(lStack>=2); lStack-=2;}
			return rc;
		case VT_EXPRTREE:
			if (((ExprTree*)pv->exprt)->op==OP_ARRAY) {
				//???
				// return rc;
			}
		default: break;
		}
		if ((node->flags&EXCLUDE_LBOUND_OP)!=0) flags|=CND_IN_LBND;
		if ((node->flags&EXCLUDE_RBOUND_OP)!=0) flags|=CND_IN_RBND;
		goto bool_op;
	case OP_EXISTS:
		switch (node->operands[0].type) {
		case VT_STMT:
			if ((rc=compileValue(node->operands[0],0))==RC_OK) {
				if ((node->flags&NOT_BOOLEAN_OP)!=0) flags^=CND_NOT|1;
				if ((rc=putCondCode(ExprOp(OP_EXISTS|0x80),flags,lbl))==RC_OK) {assert(lStack>=1); lStack-=1;}
			}
			return rc;
		case VT_VARREF:
			if (fDNF) {
				const Value& v=node->operands[0];
				if (v.refPath.refN==0 && v.length!=0) {
					if (v.refPath.id!=PROP_SPEC_ANY && v.refPath.id!=PROP_SPEC_PINID && v.refPath.id!=PROP_SPEC_STAMP 
						&& (rc=PropDNF::andP(dnf,ldnf,&v.refPath.id,1,ma,(node->flags&NOT_BOOLEAN_OP)!=0))!=RC_OK) break;
					//if (v.length==1) break;
				}
			}
			break;
		}
		ff|=CV_CARD; goto bool_op;
	case OP_IS_A:
		if (node->nops<=2) goto bool_op;
		for (i=0; i<node->nops; i++) if ((rc=compileValue(node->operands[i],ff))!=RC_OK) return rc;
		if ((node->flags&NOT_BOOLEAN_OP)!=0) flags^=CND_NOT|1;
		if ((rc=putCondCode(ExprOp(OP_IS_A|0x80),flags,lbl,true,node->nops))==RC_OK) {assert(lStack>=node->nops); lStack-=node->nops;} 	// or -1 if value required
		break;
	case OP_NE: if ((node->flags&NULLS_NOT_INCLUDED_OP)==0) ff|=CV_NE; goto bool_op;
	case OP_BEGINS: case OP_ENDS: case OP_REGEX: case OP_ISLOCAL: case OP_CONTAINS:
	case OP_EQ: case OP_LT: case OP_LE: case OP_GT: case OP_GE:
	bool_op: 
		assert(node->nops<=2);
		if ((rc=compileValue(node->operands[0],ff))==RC_OK && (node->nops==1 || (rc=compileValue(node->operands[1],ff))==RC_OK)) {
			if ((node->flags&CASE_INSENSITIVE_OP)!=0) flags|=CND_NCASE;
			if ((node->flags&FOR_ALL_LEFT_OP)!=0) flags|=CND_FORALL_L;
			if ((node->flags&EXISTS_LEFT_OP)!=0) flags|=CND_EXISTS_L;
			if ((node->flags&FOR_ALL_RIGHT_OP)!=0) flags|=CND_FORALL_R;
			if ((node->flags&EXISTS_RIGHT_OP)!=0) flags|=CND_EXISTS_R;
			if ((node->flags&NOT_BOOLEAN_OP)!=0) flags^=CND_NOT|1;
			if ((rc=putCondCode(op,flags,lbl))==RC_OK) {assert(lStack>=node->nops); lStack-=node->nops;} 	// or -1 if value required
		}
		break;
	}
	return rc;
}

RC Expr::decompile(ExprTree*&res,Session *ses) const
{
	Value stack[256],*top=stack,*end=stack+256; RC rc=RC_OK;
	for (const byte *codePtr=(const byte*)(this+1),*codeEnd=(const byte*)&hdr+hdr.lExpr; codePtr<codeEnd; ) {
		byte op=*codePtr++; ulong idx; ExprTree *exp=NULL; uint32_t jsht,l,flg,rm,rx; bool ff=(op&0x80)!=0;
		switch (op&=0x7F) {
		case OP_CON:
			if (top>=end) {rc=RC_NORESOURCES; break;}
			if ((rc=MVStoreKernel::deserialize(*top,codePtr,codeEnd,ses,true))==RC_OK && ff)
				{if ((exp=new(1,ses) ExprTree((ExprOp)op,1,0,CARD_OP,top,ses))!=NULL) top->set(exp); else rc=RC_NORESOURCES;}
			top++; break;
		case OP_VAR: case OP_PROP:
			if (top>=end) {rc=RC_NORESOURCES; break;}
			idx=*codePtr++; if (op==OP_VAR) l=STORE_INVALID_PROPID; else mv_dec32(codePtr,l); top->setVarRef((byte)idx,l);
			if (ff) {if ((exp=new(1,ses) ExprTree((ExprOp)OP_CON,1,0,CARD_OP,top,ses))!=NULL) top->set(exp); else rc=RC_NORESOURCES;}
			top++; break;
		case OP_PARAM:
			if (top>=end) {rc=RC_NORESOURCES; break;}
			top->setParam(*codePtr++); if (ff) {if ((exp=new(1,ses) ExprTree((ExprOp)OP_CON,1,0,CARD_OP,top,ses))!=NULL) top->set(exp); else rc=RC_NORESOURCES;}
			top++; break;
		case OP_PATH:
			flg=ff?*codePtr++:0; l=(flg&0x80)!=0?4:(flg&0x18)!=0?3:2;
			if ((flg&0x80)!=0) {
				mv_dec32(codePtr,rm); if ((flg&0x20)!=0) rx=rm; else mv_dec32(codePtr,rx);
				if (top+1>=end) {rc=RC_NORESOURCES; break;}
				if ((flg&0x18)==0) {top->set((unsigned)STORE_COLLECTION_ID); top++;}
				top->setU64((uint64_t)rx<<32|rm); top++;
			}
			if ((flg&0x08)!=0) {
				Expr *filter=NULL;
				if (top>=end) {rc=RC_NORESOURCES; break;} if (l==4) top[0]=top[-1];
				if ((flg&0x10)==0) {mv_dec32(codePtr,rm); l=mv_dec32zz(rm); top[3-l].set((unsigned)rm);}
				else if ((rc=deserialize(filter,codePtr,codeEnd,ses))!=RC_OK) break;
				else {
					// classID
					rc=filter->decompile(exp,ses); filter->destroy();
					if (rc==RC_OK) top[3-l].set(exp); else break;
				}
				top++;
			}
			if ((flg&0x04)==0) {
				if (top>=end) {rc=RC_NORESOURCES; break;}
				if (l>2) memmove(&top[3-l],&top[2-l],(l-2)*sizeof(Value));
				mv_dec32(codePtr,rm); top[2-l].setURIID(rm); top++;
			}
			assert(top>=stack+l);
			if ((exp=new(l,ses) ExprTree(OP_PATH,(ushort)l,0,(flg>>5&0x06)|(flg&0x01),top-l,ses))==NULL) rc=RC_NORESOURCES;
			else {top-=l-1; top[-1].set(exp);}
			break;
		case OP_CURRENT:
			if (top>=end) {rc=RC_NORESOURCES; break;}
			switch (*codePtr++) {
			default: rc=RC_CORRUPTED; break;
			case CVT_TIMESTAMP: top->setNow(); break;
			case CVT_USER: top->setCUser(); break;
			case CVT_STORE: top->setCStore(); break;
			}
			top++; break;
		case OP_JUMP:
			//???
			mv_adv32(codePtr);
			break;
		case OP_CATCH:
			//???
			break;
		case OP_COALESCE:
			//???
			mv_adv32(codePtr);
			break;
		case OP_IN1:
			ff=true; if ((*codePtr++&CND_EXT)!=0) codePtr++;
		case OP_IN:
			if (ff) {
				assert(top>=stack+2);
				if (top[-2].type==VT_EXPRTREE && ((ExprTree*)top[-2].exprt)->op==OP_IN && top[-1].type!=VT_EXPRTREE) {
					Value &v=((ExprTree*)top[-2].exprt)->operands[1];
					if (v.type==VT_ARRAY) {
						Value *pv=(Value*)ses->realloc((void*)v.varray,(v.length+1)*sizeof(Value));
						if (pv!=NULL) {pv[v.length++]=*--top; v.varray=pv;} else {rc=RC_NORESOURCES; break;}
					} else {
						Value *pv=new(ses) Value[2]; if (pv==NULL) {rc=RC_NORESOURCES; break;}
						pv[0]=v; pv[1]=*--top; v.set(pv,2); v.flags=SES_HEAP;
					}
					if (op==OP_IN1) break; exp=(ExprTree*)(--top)->exprt; goto bool_op;
				}
			}
			assert(top>=stack+2);
			if ((exp=new(2,ses) ExprTree(OP_IN,2,0,0,top-2,ses))==NULL) {rc=RC_NORESOURCES; break;}
			if (op==OP_IN) {top-=2; goto bool_op;} else {--top; top[-1].set(exp); break;}
		case OP_ISLOCAL:
			assert(top>stack);
			if ((exp=new(1,ses) ExprTree((ExprOp)op,1,0,0,top-1,ses))==NULL) {rc=RC_NORESOURCES; break;}
			--top; goto bool_op;
		case OP_IS_A:
			l=ff?*codePtr++:2; assert(top>=stack+l);
			if ((exp=new(l,ses) ExprTree((ExprOp)op,(ushort)l,0,0,top-l,ses))==NULL) {rc=RC_NORESOURCES; break;}
			top-=l; goto bool_op;
		case OP_EXISTS:
			assert(top>=stack);
			if (top[-1].type==VT_EXPRTREE && (exp=(ExprTree*)top[-1].exprt)->op==OP_CON && (exp->flags&CARD_OP)!=0) exp->op=OP_EXISTS;
			else if ((exp=new(1,ses) ExprTree((ExprOp)op,1,0,0,top-1,ses))==NULL) {rc=RC_NORESOURCES; break;}
			--top; goto bool_op;
		case OP_EQ: case OP_NE: case OP_LT: case OP_LE: case OP_GT: case OP_GE:
		case OP_CONTAINS: case OP_BEGINS: case OP_ENDS: case OP_REGEX:
			assert(top>=stack+2);
			if ((exp=new(2,ses) ExprTree((ExprOp)op,2,0,0,top-2,ses))==NULL) {rc=RC_NORESOURCES; break;}
			top-=2;
		bool_op:
			if (((l=*codePtr++)&CND_EXT)!=0) {
				l|=*codePtr++<<8;
				if ((l&CND_EXISTS_L)!=0) exp->flags|=EXISTS_LEFT_OP;
				if ((l&CND_FORALL_L)!=0) exp->flags|=FOR_ALL_LEFT_OP;
				if ((l&CND_EXISTS_R)!=0) exp->flags|=EXISTS_RIGHT_OP;
				if ((l&CND_FORALL_R)!=0) exp->flags|=FOR_ALL_RIGHT_OP;
			}
			if ((l&CND_NCASE)!=0) exp->flags|=CASE_INSENSITIVE_OP;
			if (op==OP_IN) {
				if ((l&CND_IN_LBND)!=0) exp->flags|=EXCLUDE_LBOUND_OP;
				if ((l&CND_IN_RBND)!=0) exp->flags|=EXCLUDE_RBOUND_OP;
				//if (ff) ...
			}
			if ((l&CND_NOT)!=0) {exp->flags|=NOT_BOOLEAN_OP; l^=1;}
			if ((l&=CND_MASK)<6) jsht=hdr.lExpr;
			else {uint32_t jj=uint32_t(codePtr-(const byte*)(this+1)); mv_dec32(codePtr,jsht); jsht+=jj;}
			top->set(exp); top->meta=byte(l); top->eid=jsht; top++;
			while (top>=stack+2 && (logOp[l][idx=0]==top[-2].meta || logOp[l][idx=1]==top[-2].meta)
							&& (jumpOp[l][idx]?codePtr-(const byte*)(this+1):jsht)==top[-2].eid) {
				if ((exp=new(2,ses) ExprTree((ExprOp)(idx+OP_LAND),2,0,0,top-2,ses))==NULL) {rc=RC_NORESOURCES; break;}
				--top; top[-1].set(exp); top[-1].meta=(byte)l; top[-1].eid=jsht;
			}
			break;
		default:
			l=SInCtx::opDscr[op].nOps[0]; flg=0;
			if (ff) {
				if ((SInCtx::opDscr[op].flags&_I)!=0) {
					if (top<end) (top++)->set((unsigned)*codePtr++); else {rc=RC_NORESOURCES; break;}
				} else {
					flg=op==OP_RSHIFT&&(*codePtr&CND_UNS)!=0?UNSIGNED_OP:(*codePtr&CND_NCASE)!=0?CASE_INSENSITIVE_OP:0;
					if ((SInCtx::opDscr[op].flags&_A)!=0 && (*codePtr&CND_DISTINCT)!=0) flg|=DISTINCT_OP;
					codePtr++; if (l!=SInCtx::opDscr[op].nOps[1]) l=*codePtr++;
				}
			}
			assert(top>=stack+l);
			if ((exp=new(l,ses) ExprTree((ExprOp)op,(ushort)l,0,flg,top-l,ses))==NULL) rc=RC_NORESOURCES;
			else {top-=l-1; top[-1].set(exp);}
			break;
		}
		if (rc!=RC_OK) {while (top>stack) freeV(*--top); return rc;}
	}
	assert(top==stack+1);
	if ((rc=ExprTree::forceExpr(*stack,ses))==RC_OK) res=(ExprTree*)stack->exprt;
	return RC_OK;
}

RC Expr::deserialize(Expr *&exp,const byte *&buf,const byte *const ebuf,MemAlloc *ma)
{
	if (size_t(ebuf-buf)<=sizeof(ExprHdr)) return RC_CORRUPTED;
	ExprHdr hdr(0,0,0); memcpy(&hdr,buf,sizeof(ExprHdr));
	if (hdr.lExpr==0 || size_t(ebuf-buf)<hdr.lExpr) return RC_CORRUPTED;
	byte *p=(byte*)ma->malloc(hdr.lExpr-sizeof(ExprHdr)+sizeof(Expr)); if (p==NULL) return RC_NORESOURCES;
	memcpy(p+sizeof(Expr),(const byte*)buf+sizeof(ExprHdr),hdr.lExpr-sizeof(ExprHdr));
	exp=new(p) Expr(hdr); buf+=hdr.lExpr; return RC_OK;
}

Expr *Expr::clone(const Expr *exp,MemAlloc *ma)
{
	byte *p=(byte*)ma->malloc(exp->hdr.lExpr-sizeof(ExprHdr)+sizeof(Expr)); if (p==NULL) return NULL;
	memcpy(p+sizeof(Expr),&exp->hdr+1,exp->hdr.lExpr-sizeof(ExprHdr)); return new(p) Expr(exp->hdr);
}

IExpr *Expr::clone() const
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL||ses->getStore()->inShutdown()) return NULL;
		return clone(this,ses);
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IExpr::clone()\n");}
	return NULL;
}

void Expr::destroy()
{
	try {free(this,SES_HEAP);} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IExpr::destroy()\n");}
}

//----------------------------------------------------------------------------------------------------

RC ExprTree::node(Value& res,Session *ses,ExprOp op,unsigned nOperands,const Value *operands,ulong flags)
{
	if (op<OP_FIRST_EXPR || op>=OP_ALL || nOperands==0 || operands==NULL) return RC_INVPARAM;
	const OpDscr& od=SInCtx::opDscr[op]; if (nOperands<od.nOps[0] || nOperands>od.nOps[1]) return RC_INVPARAM;
	bool fFold=true,fNot=false; const Value *cv=operands; Value *pv; RC rc=RC_OK; 
	int idx; ushort vrefs=NO_VREFS; unsigned flgFold=0;
	for (unsigned i=0; i<nOperands; i++) {
		switch (cv[i].type) {
		case VT_EXPRTREE:
			if (cv[i].exprt==NULL) return RC_INVPARAM;
			if (((ExprTree*)cv[i].exprt)->op==OP_CON) {
				assert((flags&COPY_VALUES_OP)!=0);
				if (cv==operands) {
					Value *pv=(Value*)alloca(sizeof(Value)*nOperands); if (pv==NULL) return RC_NORESOURCES;
					memcpy(pv,operands,nOperands*sizeof(Value)); cv=pv;
				}
				if ((rc=copyV(((ExprTree*)cv[i].exprt)->operands[0],const_cast<Value&>(cv[i]),ses))!=RC_OK) return rc;
				((ExprTree*)operands[i].exprt)->destroy();
			} else if (isBool(((ExprTree*)cv[i].exprt)->op)&&op!=OP_LOR&&op!=OP_LAND&&op!=OP_LNOT&&(op!=OP_PATH||i!=2)) return RC_INVPARAM;	// also CASE/WHEN
			else {fFold=false; if ((cv[i].flags&HEAP_TYPE_MASK)==NO_HEAP) cv[i].flags=SES_HEAP;}
			break;
		case VT_VARREF: case VT_PARAM: case VT_CURRENT: fFold=false; break;
		}
		vRefs(vrefs,vRefs(cv[i]));
	}
	switch (op) {
	default: break;
	case OP_PLUS: case OP_MINUS:
		// associative folding
		break;
	case OP_MUL: case OP_DIV:
		// associative folding
		break;
	case OP_EQ: case OP_NE: case OP_LT: case OP_LE: case OP_GT: case OP_GE:
		if ((flags&NOT_BOOLEAN_OP)!=0) {op=ExprTree::notOp[op-OP_EQ]; flags&=~NOT_BOOLEAN_OP;}
	case OP_IN: case OP_CONTAINS: case OP_BEGINS: case OP_ENDS: case OP_REGEX: case OP_IS_A:
		if ((flags&(FOR_ALL_LEFT_OP|EXISTS_LEFT_OP))==(FOR_ALL_LEFT_OP|EXISTS_LEFT_OP) ||
			(flags&(FOR_ALL_RIGHT_OP|EXISTS_RIGHT_OP))==(FOR_ALL_RIGHT_OP|EXISTS_RIGHT_OP)) return RC_INVPARAM;
		if (fFold) {
			if (op==OP_IN) {
				if ((flags&EXCLUDE_LBOUND_OP)!=0) flgFold|=CND_IN_LBND;
				if ((flags&EXCLUDE_RBOUND_OP)!=0) flgFold|=CND_IN_RBND;
			}
			if ((flags&CASE_INSENSITIVE_OP)!=0) flgFold|=CND_NCASE;
			if ((flags&FOR_ALL_LEFT_OP)!=0) flgFold|=CND_FORALL_L;
			if ((flags&EXISTS_LEFT_OP)!=0) flgFold|=CND_EXISTS_L;
			if ((flags&FOR_ALL_RIGHT_OP)!=0) flgFold|=CND_FORALL_R;
			if ((flags&EXISTS_RIGHT_OP)!=0) flgFold|=CND_EXISTS_R;
			if ((flags&NOT_BOOLEAN_OP)!=0) flgFold|=CND_NOT;
		}
		break;
	case OP_MIN: case OP_MAX:
		if (fFold && (flags&CASE_INSENSITIVE_OP)!=0) flgFold|=CND_NCASE;
		break;
	case OP_RSHIFT:
		if (fFold && (flags&UNSIGNED_OP)!=0) flgFold|=CND_UNS;
		break;
	case OP_DEREF:
		switch (cv[0].type) {
		default: return RC_TYPE;
		case VT_PARAM: case VT_VARREF: case VT_EXPRTREE: case VT_REFID: case VT_REFIDPROP: case VT_REFIDELT: break;
		}
		fFold=false; break;
	case OP_REF:
		switch (cv[0].type) {
		default: return RC_TYPE;
		case VT_VARREF: case VT_PARAM:
			break;
		case VT_REFID: case VT_REFIDPROP: case VT_REFIDELT:
			if ((flags&COPY_VALUES_OP)==0) res=cv[0]; else rc=copyV(cv[0],res,ses);
			return rc;
		case VT_EXPRTREE:
			if (cv[0].exprt->getOp()==OP_PATH) {
				//???
			}
			break;
		}
		break;
	case OP_CALL:
		if (cv[0].type!=VT_VARREF && cv[0].type!=VT_PARAM && cv[0].type!=VT_STMT && cv[0].type!=VT_EXPR) return RC_TYPE;
		fFold=false; break;
	case OP_COUNT:
		if (nOperands==1 && cv[0].type==VT_ANY) {fFold=false; break;}
	case OP_EXISTS:
		if (cv[0].type!=VT_EXPRTREE) {
			if (cv[0].type!=VT_VARREF && cv[0].type!=VT_PARAM && (cv[0].type!=VT_STMT||op!=OP_EXISTS)) {
				if (op==OP_EXISTS) res.set(((flags&NOT_BOOLEAN_OP)!=0)==(cv[0].type==VT_ANY));
				else res.set(unsigned(cv[0].type==VT_ANY?0u:cv[0].type==VT_ARRAY?cv[0].length:cv[0].type==VT_COLLECTION?cv[0].nav->count():1u));
				freeV(*(Value*)&cv[0]); return RC_OK;
			}
			if (op==OP_COUNT) {op=(ExprOp)OP_CON; flags|=CARD_OP;}
		}
		fFold=false; break;
	case OP_PATH:
		if (cv[0].type!=VT_EXPRTREE && cv[0].type!=VT_VARREF && cv[0].type!=VT_PARAM && cv[0].type!=VT_REF && cv[0].type!=VT_REFID && cv[0].type!=VT_ARRAY 
			&& cv[0].type!=VT_COLLECTION || cv[1].type!=VT_EXPRTREE && cv[1].type!=VT_VARREF && cv[1].type!=VT_PARAM && cv[1].type!=VT_URIID) return RC_TYPE;
		if (nOperands>2) {
			//???
		}
		break;
	case OP_ISLOCAL:
		if (cv->type!=VT_EXPRTREE && cv->type!=VT_REF && cv->type!=VT_REFID && 
			cv->type!=VT_PARAM && cv->type!=VT_VARREF) return RC_TYPE;
		break;
	case OP_LOR:
		if ((flags&NOT_BOOLEAN_OP)==0 && cv[0].type==VT_EXPRTREE && cv[1].type==VT_EXPRTREE && ((ExprTree*)cv[1].exprt)->op==OP_EQ &&
			((ExprTree*)cv[1].exprt)->operands[1].type!=VT_EXPRTREE &&
			(((ExprTree*)cv[0].exprt)->op==OP_EQ || ((ExprTree*)cv[0].exprt)->op==OP_IN) &&
			((ExprTree*)cv[0].exprt)->operands[0]==((ExprTree*)cv[1].exprt)->operands[0]) switch (((ExprTree*)cv[0].exprt)->op) {
		case OP_IN:
			if (((ExprTree*)cv[0].exprt)->operands[1].type==VT_ARRAY) {
				Value &v=((ExprTree*)cv[0].exprt)->operands[1]; pv=(Value*)v.varray;
				if ((v.varray=pv=(Value*)ses->realloc(pv,(v.length+1)*sizeof(Value)))==NULL) return RC_NORESOURCES;
				pv[v.length++]=((ExprTree*)cv[1].exprt)->operands[1];
				((ExprTree*)cv[1].exprt)->operands[1].flags=NO_HEAP;
				freeV((Value&)cv[1]); res.set(cv[0].exprt); return RC_OK;
			}
			break;
		case OP_EQ:
			if (((ExprTree*)cv[0].exprt)->operands[1].type!=VT_EXPRTREE) {
				Value &v=((ExprTree*)cv[0].exprt)->operands[1];
				if ((pv=(Value*)ses->malloc(2*sizeof(Value)))!=NULL) {
					pv[0]=v; pv[1]=((ExprTree*)cv[1].exprt)->operands[1]; ((ExprTree*)cv[1].exprt)->operands[1].flags=NO_HEAP;
					v.type=VT_ARRAY; v.varray=pv; v.length=2; v.flags=v.flags&~HEAP_TYPE_MASK|SES_HEAP;
					freeV((Value&)cv[1]); ((ExprTree*)cv[0].exprt)->op=OP_IN; res.set(cv[0].exprt); return RC_OK;
				}
			}
			break;
		default: break;
		}
	case OP_LAND:
		idx=-1;
		if ((flags&NOT_BOOLEAN_OP)!=0) {op=op==OP_LAND?OP_LOR:OP_LAND; flags&=~NOT_BOOLEAN_OP; fNot=true;}
		if (cv[0].type==VT_BOOL) idx=cv[0].b==(op==OP_LAND?false:true)?0:1;
		else if (cv[0].type!=VT_EXPRTREE || !isBool(((ExprTree*)cv[0].exprt)->op)) return RC_TYPE;
		if (cv[1].type==VT_BOOL) {if (idx<0) idx=cv[1].b==(op==OP_LAND?false:true)?1:0;}
		else if (cv[1].type!=VT_EXPRTREE || !isBool(((ExprTree*)cv[1].exprt)->op)) return RC_TYPE;
		if (idx>=0) {res=cv[idx]; if ((flags&COPY_VALUES_OP)==0) freeV(*(Value*)&cv[idx^1]); return fNot?cvNot(res):RC_OK;}
		break;
	case OP_LNOT:
		if ((flags&NOT_BOOLEAN_OP)==0&&cv[0].type==VT_BOOL) res.set(!cv[0].b);
		else {
			res=cv[0];
			if ((flags&COPY_VALUES_OP)==0) cv[0].flags=NO_HEAP;
			else if (res.type!=VT_EXPRTREE && (rc=copyV(res,res,ses))!=RC_OK) return rc;
			if ((flags&NOT_BOOLEAN_OP)==0) rc=cvNot(res);
		}
		return rc;
	}
	if (fFold) {
		res=cv[0];
		if ((flags&COPY_VALUES_OP)==0) cv[0].flags=NO_HEAP;
		else if (res.type!=VT_EXPRTREE && (rc=copyV(res,res,ses))!=RC_OK) return rc;
		rc=(SInCtx::opDscr[op].flags&_A)==0?Expr::calc(op,res,&cv[1],nOperands,flgFold,ses):
			Expr::calcAgg(op,res,&cv[1],nOperands,((flags&DISTINCT_OP)!=0?CND_DISTINCT:0)|flgFold,ses);
		if (isBool(op) && (rc==RC_TRUE||rc==RC_FALSE)) {res.set(rc==RC_TRUE); rc=RC_OK;}
		if (rc==RC_OK && (flags&COPY_VALUES_OP)==0) for (unsigned i=1; i<nOperands; i++) freeV(*(Value*)&cv[i]);
	} else {
		ExprTree *pe=new(nOperands,ses) ExprTree(op,(ushort)nOperands,vrefs,flags,cv,ses);
		if (pe!=NULL) res.set(pe); else rc=RC_NORESOURCES;
	}
	return fNot&&rc==RC_OK?cvNot(res):rc;
}

RC ExprTree::cvNot(Value& v)
{
	ExprTree *ex; RC rc=RC_OK;
	switch (v.type) {
	default: return RC_TYPE;
	case VT_BOOL: v.b=!v.b; break;
	case VT_EXPRTREE:
		ex=(ExprTree*)v.exprt;
		switch (ex->op) {
		case OP_LAND:
			assert(ex->nops==2); ex->op=OP_LOR;
			if ((rc=cvNot(ex->operands[0]))==RC_OK) rc=cvNot(ex->operands[1]);
			break;
		case OP_LOR:
			assert(ex->nops==2); ex->op=OP_LAND;
			if ((rc=cvNot(ex->operands[0]))==RC_OK) rc=cvNot(ex->operands[1]);
			break;
		case OP_EQ: case OP_NE: case OP_LT: case OP_LE: case OP_GT: case OP_GE:
			ex->op=notOp[ex->op-OP_EQ]; break;
		case OP_IN: case OP_EXISTS: case OP_CONTAINS: case OP_BEGINS: case OP_ENDS: case OP_IS_A:
		case OP_REGEX: case OP_ISLOCAL: ex->flags^=NOT_BOOLEAN_OP; break;
		default: rc=RC_TYPE; break;
		}
	}
	return rc;
}

RC ExprTree::forceExpr(Value& v,Session *ses,bool fCopyV)
{
	if (v.type!=VT_EXPRTREE) {
		ExprTree *pe=new(1,ses) ExprTree((ExprOp)OP_CON,1,vRefs(v),fCopyV?COPY_VALUES_OP:0,&v,ses);
		if (pe==NULL) return RC_NORESOURCES; v.set(pe); v.flags=SES_HEAP;
	}
	return RC_OK;
}

RC ExprTree::normalizeArray(Value *vals,unsigned nvals,Value& res,MemAlloc *ma,StoreCtx *ctx)
{
	bool fConst=true,fNested=false; unsigned cnt=0;
	for (unsigned i=0; i<nvals; i++) {
		switch (vals[i].type) {
		default: cnt++; continue;
		case VT_ARRAY: cnt+=vals[i].length; fNested=true; continue;
		case VT_COLLECTION: cnt+=vals[i].nav->count(); fNested=true; continue;
		case VT_EXPRTREE: case VT_VARREF: case VT_PARAM: case VT_CURRENT: fConst=false; break;
		}
		break;
	}
	if (fConst) {
		Value *pv=new(ma) Value[cnt]; if (pv==NULL) return RC_NORESOURCES;
		ElementID prefix=ctx!=NULL?ctx->getPrefix():0; const Value *cv; RC rc;
		if (!fNested) {
			memcpy(pv,vals,nvals*sizeof(Value)); 
			for (unsigned i=0; i<cnt; i++) pv[i].eid=prefix+i;
		} else {
			for (unsigned i=0,j=0,k,l; i<nvals; i++) switch (vals[i].type) {
			default: pv[j]=vals[i]; pv[j].eid=prefix+j; j++; break;
			case VT_ARRAY:
				l=vals[i].length; assert(j+l<=cnt);
				memcpy(&pv[j],vals[i].varray,l*sizeof(Value));
				for (k=0; k<l; ++j,++k) pv[j].eid=prefix+j;
				ma->free((void*)vals[i].varray); break;
			case VT_COLLECTION:
				for (cv=vals[i].nav->navigate(GO_FIRST),k=j; cv!=NULL; cv=vals[i].nav->navigate(GO_NEXT),++j) {
					if ((rc=copyV(*cv,pv[j],ma))!=RC_OK) {while (j--!=k) freeV(pv[j]); ma->free(pv); return rc;}
					pv[j].eid=prefix+j;
				}
				vals[i].nav->destroy(); break;
			}
		}
		res.set(pv,cnt); res.flags=ma->getAType();
	} else {
//		ExprTree *pe=new(nvals,ma) ExprTree(OP_ARRAY,(ushort)nvals,NULL,0,vals,Session::getSession());	// vRefs???
//		if (pe!=NULL) res.set(pe); else return RC_NORESOURCES;
	}
	return RC_OK;
}



ExprTree::ExprTree(ExprOp o,ushort no,ushort vr,ulong f,const Value *ops,Session *s) 
	: ses(s),op(o),nops(no),vrefs(vr),flags((ushort)f)
{
	memcpy((Value*)operands,ops,no*sizeof(Value));
	if ((f&COPY_VALUES_OP)!=0) for (unsigned i=0; i<no; i++) {
		Value& oper=*const_cast<Value*>(&operands[i]); RC rc;
		if (oper.type==VT_EXPRTREE) {assert(((ExprTree*)ops[i].exprt)->op!=OP_CON); oper.flags=SES_HEAP;}
		else  {oper.flags=NO_HEAP; if ((rc=copyV(oper,oper,ses))!=RC_OK) throw rc;}
	}
}

ExprTree::~ExprTree()
{
	for (int i=nops; --i>=0;) freeV(operands[i]);
}

ExprOp ExprTree::getOp() const
{
	try {return op;} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IExprTree::getOp()\n");}
	return OP_ALL;
}

unsigned ExprTree::getNumberOfOperands() const
{
	try {return nops;} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IExprTree::getNumberOfOperands()\n");}
	return ~0u;
}

const Value& ExprTree::getOperand(unsigned idx) const
{
	try {assert(idx<nops); return operands[idx];} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IExprTree::getOperand()\n");}
	static Value v; v.setError(); return v;
}

unsigned ExprTree::getFlags() const
{
	try {return flags;} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IExprTree::getFlags()\n");}
	return ~0u;
}

bool ExprTree::operator==(const ExprTree& rhs) const
{
	if (op!=rhs.op || nops!=rhs.nops || flags!=rhs.flags) return false;
	for (int i=nops; --i>=0; ) if (operands[i]!=rhs.operands[i]) return false;
	return true;
}

void ExprTree::mergeVRefs(ushort& vr1,ushort vr2)
{
	assert(vr1!=MANY_VREFS && vr1!=NO_VREFS && vr2!=MANY_VREFS && vr2!=NO_VREFS);
	if (byte(vr2)!=byte(vr1) && byte(vr2)!=byte(vr1>>8)) {
		if (byte(vr1)!=byte(vr1>>8)) {vr1=MANY_VREFS; return;}
		vr1=byte(vr2)<<8|byte(vr1);
	}
	if (byte(vr2>>8)!=byte(vr1) && byte(vr2>>8)!=byte(vr1>>8)) {
		if (byte(vr1)!=byte(vr1>>8)) vr1=MANY_VREFS; else vr1=(vr2&0xFF00)|byte(vr1);
	}
}

void ExprTree::mapVRefs(byte from,byte to)
{
	for (ushort i=0; i<nops; i++) switch (operands[i].type) {
	default: break;
	case VT_VARREF:
		if (operands[i].refPath.refN==from) operands[i].refPath.refN=to;
		break;
	case VT_EXPRTREE:
		vrefs=((ExprTree*)operands[i].exprt)->vrefs;
		if (vrefs==MANY_VREFS || vrefs!=NO_VREFS && (byte(vrefs)==from || byte(vrefs>>8)==from))
			((ExprTree*)operands[i].exprt)->mapVRefs(from,to);
		break;
	}
}

void ExprTree::setFlags(unsigned flgs,unsigned mask)
{
	try {flags=flags&~mask|flgs&mask;}
	catch (...) {report(MSG_ERROR,"Exception in IExprTree::setFlags(...)\n");}
}

IExpr *ExprTree::compile()
{
	try {
		Session *ses=Session::getSession();
		if (ses==NULL||ses->getStore()->inShutdown()) return NULL;
		Expr *expr=NULL; Expr::compile(this,expr,ses); return expr;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IExprTree::compile()\n");}
	return NULL;
}

IExprTree *ExprTree::clone() const
{
	try {
		Session *ses=Session::getSession();
		if (ses==NULL||ses->getStore()->inShutdown()) return NULL;
		ExprTree *pe=new(nops,ses) ExprTree(op,nops,vrefs,flags|COPY_VALUES_OP,operands,ses);
		if (pe!=NULL) for (ulong i=0; i<ulong(nops); i++)
			if (pe->operands[i].type==VT_EXPRTREE) pe->operands[i].exprt=pe->operands[i].exprt->clone();
		return pe;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IExprTree::clone()\n");}
	return NULL;
}

void ExprTree::destroy()
{
	try {delete this;} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IExprTree::destory()\n");}
}

IExprTree *SessionX::expr(ExprOp op,unsigned nOperands,const Value *operands,unsigned flags)
{
	try {
		assert(ses==Session::getSession()); if (ses->getStore()->inShutdown()) return NULL;
		Value v; RC rc=ExprTree::node(v,ses,op,nOperands,operands,flags|COPY_VALUES_OP);
		if (rc==RC_OK && (rc=ExprTree::forceExpr(v,ses,true))!=RC_OK) freeV(v);
		return rc==RC_OK?v.exprt:(IExprTree*)0;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IStmt::expr()\n");}
	return NULL;
}
