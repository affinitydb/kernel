/**************************************************************************************

Copyright © 2004-2013 GoPivotal, Inc. All rights reserved.

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

using namespace AfyKernel;

static const bool jumpOp[8][2] = {{false,false},{false,false},{true,false},{false,true},{false,true},{true,false},{false,true},{true,false}};
static const byte logOp[8][2] = {{4,2},{3,5},{6,2},{3,7},{4,7},{6,5},{6,7},{6,7}};

const ExprOp ExprTree::notOp[] = {OP_NE,OP_EQ,OP_GE,OP_GT,OP_LE,OP_LT};

const static ExprHdr hdrInit(sizeof(ExprHdr),0,0,0,0,0);

RC Expr::compile(const ExprTree *tr,Expr *&expr,MemAlloc *ma,bool fCond,ValueV *aggs)
{
	ExprCompileCtx ctx(ma,aggs); expr=NULL; RC rc=RC_OK; if (fCond) if (isBool(tr->op)) ctx.pHdr->hdr.flags|=EXPR_BOOL; else return RC_INVOP;
	while ((rc=fCond?ctx.compileCondition(tr,0,0):ctx.compileNode(tr))==RC_OK && ctx.fCollectRefs) {
		for (ExprCompileCtx::ExprLbl *lb=ctx.labels,*lb2; lb!=NULL; lb=lb2) {lb2=lb->next; ma->free(lb);}
		ctx.labels=NULL; ctx.labelCount=0; ctx.lStack=ctx.pHdr->hdr.lStack=0; ctx.lCode=0; ctx.fCollectRefs=false;
	}
	return rc==RC_OK?ctx.result(expr):rc;
}

RC Expr::compileConds(const ExprTree *const *tr,unsigned ntr,Expr *&expr,MemAlloc *ma)
{
	ExprCompileCtx ctx(ma,NULL); expr=NULL; ctx.pHdr->hdr.flags|=EXPR_BOOL; RC rc=RC_OK; const ExprTree *et;
	if (ntr==0 || tr[0]==NULL) expr=NULL;
	else if (ntr>1 && !isBool(tr[0]->op)) rc=RC_INVOP;
	else for (;;) {
		rc=isBool(tr[0]->op)?(ctx.pHdr->hdr.flags|=EXPR_BOOL,ctx.compileCondition(tr[0],ntr>1?4:0,0)):ctx.compileNode(tr[0]);
		for (unsigned i=1; rc==RC_OK && i<ntr; i++) if ((et=tr[i])!=NULL) {
			if (!isBool(et->op)) rc=RC_INVOP;
			else {
				uint32_t lStack=ctx.lStack; ctx.lStack=0; 
				if ((rc=ctx.compileCondition(et,i+1<ntr?4:0,0))==RC_OK) ctx.lStack=max(ctx.lStack,lStack);
			}
		}
		if (rc!=RC_OK || !ctx.fCollectRefs) break;
		for (ExprCompileCtx::ExprLbl *lb=ctx.labels,*lb2; lb!=NULL; lb=lb2) {lb2=lb->next; ma->free(lb);}
		ctx.labels=NULL; ctx.labelCount=0; ctx.lStack=ctx.pHdr->hdr.lStack=0; ctx.lCode=0; ctx.fCollectRefs=false;
	}
	return rc==RC_OK?ctx.result(expr):rc;
}

RC Expr::create(uint16_t langID,const byte *body,uint32_t lBody,uint16_t flags,Expr *&exp,MemAlloc *ma)
{
	if ((exp=(Expr*)ma->malloc(sizeof(Expr)+lBody))==NULL) return RC_NORESOURCES;
	new((byte*)exp) Expr(ExprHdr(sizeof(ExprHdr)+lBody,langID,flags|EXPR_EXTN,0,0,0));
	memcpy(&exp->hdr+1,body,lBody); return RC_OK;
}

void Expr::getExtRefs(ushort var,const PropertyID *&pids,unsigned& nPids) const
{
	pids=NULL; nPids=0;
	if (var<=hdr.xVar) for (const VarHdr *vh=(VarHdr*)(&hdr+1),*vend=(VarHdr*)((byte*)(&hdr+1)+hdr.lProps); vh<vend; vh=(VarHdr*)((uint32_t*)(vh+1)+vh->nProps))
		if (vh->var==var) {pids=(const PropertyID*)(vh+1); nPids=vh->nProps; break;}
}

RC Expr::mergeProps(PropListP& plp,bool fForce,bool fFlags) const
{
	if (hdr.nVars==0) return RC_OK; RC rc=plp.checkVar(hdr.xVar);
	for (const VarHdr *vh=(VarHdr*)(&hdr+1),*vend=(VarHdr*)((byte*)(&hdr+1)+hdr.lProps); rc==RC_OK && vh<vend && (vh->var&0x8000)==0; vh=(VarHdr*)((uint32_t*)(vh+1)+vh->nProps))
		rc=plp.merge(vh->var,(const PropertyID *)(vh+1),vh->nProps,fForce,fFlags);
	return rc;
}

RC Expr::addPropRefs(Expr **pex,const PropertyID *props,unsigned nProps,MemAlloc *ma)
{
	Expr *pe=*pex;
	if (pe==NULL) {
		byte *p=(byte*)ma->malloc(sizeof(Expr)+sizeof(VarHdr)+nProps*sizeof(uint32_t)); if (p==NULL) return RC_NORESOURCES;
		pe=*pex=new(p) Expr(hdrInit); pe->hdr.lExpr=sizeof(ExprHdr)+(pe->hdr.lProps=uint16_t(sizeof(VarHdr)+nProps*sizeof(uint32_t)));
		pe->hdr.flags=EXPR_NO_CODE; pe->hdr.nVars=1; VarHdr *vh=(VarHdr*)(&pe->hdr+1); vh->var=0; vh->nProps=nProps; memcpy(vh+1,props,nProps*sizeof(uint32_t));
	} else {
		if ((pe=*pex=(Expr*)ma->realloc(pe,pe->hdr.lExpr+sizeof(Expr)-sizeof(ExprHdr)+nProps*sizeof(uint32_t)))==NULL) return RC_NORESOURCES;
		VarHdr *vh=(VarHdr*)(&pe->hdr+1); uint32_t *vp=(uint32_t*)(vh+1),*ins; unsigned np=vh->nProps; int c;
		for (unsigned i=0,n; i<nProps; i++) if (BIN<uint32_t,uint32_t,ExprPropCmp>::find(props[i],vp,np,&ins)==NULL) {
			if (ins>=(uint32_t*)(vh+1)+vh->nProps) n=nProps-i,c=1;
			else {
				for (n=1,c=1; i+n<nProps; n++) if ((c=cmp3((uint32_t)props[i+n]&STORE_MAX_URIID,*ins&STORE_MAX_URIID))>=0) break;
				vp=ins+n; np=vh->nProps-uint16_t(ins-(uint32_t*)(vh+1)); if (c==0) ++vp,--np; else c=1;
			}
			if ((byte*)ins<(byte*)&pe->hdr+pe->hdr.lExpr) memmove(ins+n,ins,pe->hdr.lExpr-((byte*)ins-(byte*)&pe->hdr));
			if (n==1) *ins=props[i]; else {memcpy(ins,&props[i],n*sizeof(uint32_t)); i+=n-c;}
			pe->hdr.lExpr+=n*sizeof(uint32_t); pe->hdr.lProps+=uint16_t(n*sizeof(uint32_t)); vh->nProps+=(uint16_t)n;
		}
	}
	return RC_OK;
}

ExprCompileCtx::ExprCompileCtx(MemAlloc *m,ValueV *ag) 
	: ma(m),aggs(ag),labelCount(0),lStack(0),pHdr(new(buf) Expr(hdrInit)),pCode(buf+sizeof(buf)/4),lCode(0),xlCode(sizeof(buf)*3/4),fCollectRefs(false),labels(NULL) {}

ExprCompileCtx::~ExprCompileCtx()
{
	for (ExprLbl *lb=labels,*lb2; lb!=NULL; lb=lb2) {lb2=lb->next; ma->free(lb);}
	if (pHdr!=NULL && (byte*)pHdr!=buf) ma->free(pHdr);
}

RC ExprCompileCtx::addExtRef(URIID id,uint16_t var,uint32_t flags,uint16_t& vidx,uint16_t& pidx)
{
	VarHdr *vh=(VarHdr*)(&pHdr->hdr+1),*vh0=vh; vidx=0;
	if (pHdr->hdr.lProps!=0) {
		for (const VarHdr *vend=(VarHdr*)((byte*)vh+pHdr->hdr.lProps); vh<vend; vh=(VarHdr*)((uint32_t*)(vh+1)+vh->nProps),++vidx) if (vh->var==var) {
			uint32_t *ins,*p=(uint32_t*)BIN<uint32_t,uint32_t,ExprPropCmp>::find(id,(const uint32_t*)(vh+1),vh->nProps,&ins);
			if (p!=NULL) {*p&=id|flags; pidx=uint16_t(p-(uint32_t*)(vh+1));}
			else {
				pidx=uint16_t(ins-(uint32_t*)(vh+1)); if (pidx!=vh->nProps) fCollectRefs=true;
				if ((byte*)vh0+pHdr->hdr.lProps+sizeof(uint32_t)>pCode) {
					if (!expandHdr(sizeof(uint32_t),vh)) return RC_NORESOURCES;
					ins=&((uint32_t*)(vh+1))[pidx]; vh0=(VarHdr*)(&pHdr->hdr+1);
				}
				if ((byte*)ins<(byte*)vh0+pHdr->hdr.lProps) memmove(ins+1,ins,pHdr->hdr.lProps-((byte*)ins-(byte*)vh0));
				*ins=id|flags; vh->nProps++; pHdr->hdr.lProps+=sizeof(uint32_t);
			}
			return RC_OK;
		}
	}
	if ((byte*)vh0+pHdr->hdr.lProps+sizeof(VarHdr)+sizeof(uint32_t)>pCode && !expandHdr(sizeof(VarHdr)+sizeof(uint32_t),vh)) return RC_NORESOURCES;
	vh->var=var; vh->nProps=1; *(uint32_t*)(vh+1)=id|flags; if (var<0x100 && var>pHdr->hdr.xVar) pHdr->hdr.xVar=uint8_t(var);
	pHdr->hdr.lProps+=sizeof(VarHdr)+sizeof(uint32_t); vidx=pHdr->hdr.nVars++; pidx=0;
	return RC_OK;
}

bool ExprCompileCtx::expandHdr(uint32_t l,VarHdr *&vh)
{
	const uint32_t lHdr=sizeof(Expr)+pHdr->hdr.lProps; const ptrdiff_t sht=(byte*)vh-(byte*)pHdr;
	if ((byte*)pHdr==buf) {
		xlCode=lCode+128;
		byte *p=(byte*)ma->malloc(lHdr+l+xlCode); if (p==NULL) return false; memcpy(p,pHdr,sizeof(Expr)+pHdr->hdr.lProps);
		if (lCode!=0) memcpy((byte*)(&((Expr*)p)->hdr+1)+pHdr->hdr.lProps+l,pCode,lCode); pHdr=(Expr*)p;
	} else {
		if (l>=xlCode-lCode && (pHdr=(Expr*)ma->realloc(pHdr,lHdr+(xlCode+=l+128)))==NULL) return false;
		memmove((byte*)(&pHdr->hdr+1)+pHdr->hdr.lProps+l,(byte*)(&pHdr->hdr+1)+pHdr->hdr.lProps,lCode); xlCode-=l;
	}
	pCode=(byte*)(&pHdr->hdr+1)+pHdr->hdr.lProps+l; vh=(VarHdr*)((byte*)pHdr+sht); return true;
}

bool ExprCompileCtx::expand(uint32_t l)
{
	const uint32_t lHdr=sizeof(Expr)+pHdr->hdr.lProps;
	if ((byte*)pHdr==buf) {
		xlCode=lCode+(lCode==0?max(l,(uint32_t)128):lCode<l?l:lCode);
		byte *p=(byte*)ma->malloc(lHdr+xlCode); if (p==NULL) return false; memcpy(p,pHdr,sizeof(Expr)+pHdr->hdr.lProps);
		memcpy((byte*)(&((Expr*)p)->hdr+1)+pHdr->hdr.lProps,pCode,lCode); pHdr=(Expr*)p;
	} else {
		xlCode+=l>xlCode?l:xlCode; if ((pHdr=(Expr*)ma->realloc(pHdr,lHdr+xlCode))==NULL) return false;
	}
	pCode=(byte*)(&pHdr->hdr+1)+pHdr->hdr.lProps; return true;
}

RC ExprCompileCtx::result(Expr *&res)
{
	if ((byte*)pHdr!=buf) {res=(Expr*)pHdr; pHdr=NULL;}
	else {
		res=(Expr*)ma->malloc(sizeof(Expr)+pHdr->hdr.lProps+lCode); if (res==NULL) return RC_NORESOURCES;
		memcpy(res,pHdr,sizeof(Expr)+pHdr->hdr.lProps); if (lCode!=0) memcpy((byte*)(&res->hdr+1)+pHdr->hdr.lProps,pCode,lCode);
	}
	if (lCode==0) res->hdr.flags|=EXPR_NO_CODE;
	else if (res->hdr.lProps!=0 && (res->hdr.flags&EXPR_PRELOAD)!=0) res->hdr.lStack+=(res->hdr.lProps-res->hdr.nVars*sizeof(VarHdr))/sizeof(uint32_t);
	res->hdr.lExpr=sizeof(ExprHdr)+res->hdr.lProps+lCode; return RC_OK;
}

RC ExprCompileCtx::compileNode(const ExprTree *node,unsigned flg)
{
	unsigned i; RC rc; byte *p; uint32_t lbl; const Value *pv; Value w;
	if (isBool(node->op)) return compileCondition(node,CND_VBOOL|4,0,flg);
	if (fCollectRefs) {
		if (node->op==OP_COUNT && node->operands[0].type==VT_VARREF) flg|=CV_CARD;
		for (i=0; i<node->nops; i++) if ((rc=compileValue(node->operands[i],flg))!=RC_OK) return rc;
		return RC_OK;
	}
	byte op=(byte)node->op,f=0; unsigned l=node->nops!=SInCtx::opDscr[op].nOps[0]?3:1; const ExprTree *nd; bool fvp;
	if ((node->flags&CASE_INSENSITIVE_OP)!=0) {f=CND_NCASE; if (l==1) l=2;}	// also COMP_PATTERN_OP -> CND_CMPLRX for OP_SIMILAR
	switch (op) {
	case OP_CON:
		assert(node->nops==1); return compileValue(node->operands[0],flg);
	case OP_COALESCE:
		lbl=++labelCount;
		if ((p=alloc(1))==NULL) return RC_NORESOURCES; p[0]=OP_CATCH|0x80;
		for (i=0; i<unsigned(node->nops-1); i++) {
			if ((rc=compileValue(node->operands[i],flg))!=RC_OK) return rc;
			if ((p=alloc(2))==NULL) return RC_NORESOURCES;
			if ((labels=new(ma) ExprLbl(labels,lbl,uint32_t(lCode-1)))==NULL) return RC_NORESOURCES;
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
			assert(fCollectRefs || lStack>=1); return RC_OK;
		}
		break;
	case OP_TRIM:
		assert(node->nops==3);
		if ((node->operands[2].type==VT_INT||node->operands[2].type==VT_UINT) && node->operands[2].ui<=2) {
			if ((rc=compileValue(node->operands[0],flg))!=RC_OK || (rc=compileValue(node->operands[1],flg))!=RC_OK) return rc;
			if ((p=alloc(2))==NULL) return RC_NORESOURCES;
			p[0]=OP_TRIM|0x80; p[1]=(byte)node->operands[2].ui;
			assert(fCollectRefs || lStack>=2); --lStack; return RC_OK;
		}
		break;
	case OP_RSHIFT:
		if ((node->flags&UNSIGNED_OP)!=0) {f|=CND_UNS; l++;}
		break;
	case OP_STRUCT: case OP_PIN: 
		flg|=CV_PROP; break;
	case OP_PATH:
		// fill f: path, rpath, DFS (exists,=, etc.)
		for (i=0,nd=node,fvp=false; nd->op==OP_PATH; nd=(const ExprTree*)pv->exprt) {
			if (++i>=256) return RC_TOOBIG;
			if (nd->operands[1].type!=VT_URIID && nd->operands[1].type!=VT_ARRAY && nd->operands[1].type!=VT_ANY || 
				nd->nops>=3 && nd->operands[2].type==VT_EXPRTREE && !isBool(((ExprTree*)nd->operands[2].exprt)->op) ||
				nd->nops>=4 && nd->operands[3].type!=VT_UINT) fvp=true;
			if ((pv=&nd->operands[0])->type!=VT_EXPRTREE) break;
		}
		if ((rc=compileValue(*pv,flg))!=RC_OK) return rc;
		l=lStack;
		if (fvp) for (nd=node; nd->op==OP_PATH; nd=(const ExprTree*)pv->exprt) {
			if (nd->nops>=4 && nd->operands[3].type!=VT_UINT && (rc=compileValue(nd->operands[3],0))!=RC_OK) return rc;
			if (nd->nops>=3 && nd->operands[2].type==VT_EXPRTREE && !isBool(((ExprTree*)nd->operands[2].exprt)->op) && (rc=compileValue(nd->operands[2],0))!=RC_OK) return rc;
			if (nd->operands[1].type!=VT_URIID && nd->operands[1].type!=VT_ARRAY && nd->operands[1].type!=VT_ANY && (rc=compileValue(nd->operands[1],0))!=RC_OK) return rc;
			if ((pv=&nd->operands[0])->type!=VT_EXPRTREE) break;
		}
		if ((p=alloc(f!=0?3:2))==NULL) return RC_NORESOURCES;
		p[0]=f!=0?OP_PATH|0x80:OP_PATH; p[1]=(byte)i; p+=2; if (f!=0) *p++=f;
		for (; node->op==OP_PATH; node=(const ExprTree*)pv->exprt) {
			f=(node->flags&FILTER_LAST_OP)!=0?0x01:0; uint32_t ll=1;
			if (node->operands[1].type!=VT_URIID && node->operands[1].type!=VT_ARRAY && node->operands[1].type!=VT_ANY) f|=0x02; else ll+=(uint32_t)serSize(node->operands[1]);
			if (node->nops<4) f|=(node->flags&(QUEST_PATH_OP|PLUS_PATH_OP|STAR_PATH_OP))<<5;
			Expr *filter=NULL;
			if (node->nops>=3) {
				switch (node->operands[2].type) {
				default: return RC_TYPE;
				case VT_ERROR: break;
				case VT_INT: case VT_UINT:
					if (node->operands[2].ui!=STORE_COLLECTION_ID) {f|=0x08; unsigned u=afy_enc32zz(node->operands[2].ui); ll+=afy_len32(u);}
					break;
				case VT_EXPRTREE:
					if (!isBool(((ExprTree*)node->operands[2].exprt)->op)) f|=0x10;
					else {
						// classID ?
						if ((rc=Expr::compile((ExprTree*)node->operands[2].exprt,filter,ma,true))!=RC_OK) return rc;
						ll+=(uint32_t)filter->serSize(); f|=0x18;
					}
					break;
				}
				if (node->nops>=4) {
					if (node->operands[3].type!=VT_UINT) f=f&~0xE0|0xA0; 
					else {f=f&~0xE0|0x80; ll+=afy_len16(uint16_t(node->operands[3].ui))+afy_len16(uint16_t(node->operands[3].ui>>16));}
				}
			}
			if ((p=alloc(ll))!=NULL) *p++=f; else return RC_NORESOURCES;
			if ((f&0x01)==0) p=serialize(node->operands[1],p);
			switch (f&0x18) {
			default: break;
			case 0x08: l=afy_enc32zz(node->operands[2].i); afy_enc32(p,l); break;
			case 0x18: p=filter->serialize(p); break;		// classID??
			}
			if ((f&0xE0)==0x80) {assert(node->nops>=4 && node->operands[3].type==VT_UINT); afy_enc16(p,uint16_t(node->operands[3].ui)); afy_enc16(p,uint16_t(node->operands[3].ui>>16));}
			if ((pv=&node->operands[0])->type!=VT_EXPRTREE) break;
		}
		lStack=l; return RC_OK;
	case OP_COUNT:
		if (node->operands[0].type!=VT_EXPRTREE && node->operands[0].type!=VT_ANY && (node->operands[0].type!=VT_VARREF||aggs==NULL)) {
			if (node->operands[0].type==VT_VARREF) pv=&node->operands[0];
			else {w.set(unsigned(node->operands[0].type==VT_ARRAY?node->operands[0].length:node->operands[0].type==VT_COLLECTION?node->operands[0].nav->count():1u)); pv=&w;}
			return compileValue(*pv,flg|CV_CARD);
		}
	default:
		if (node->nops==1 && node->op<OP_ALL && (SInCtx::opDscr[node->op].flags&_A)!=0) {
			if (aggs!=NULL) {
				if (aggs->nValues<0x3F) {
					if ((p=alloc(2))!=NULL) {p[0]=OP_PARAM; p[1]=byte(aggs->nValues|QV_AGGS<<6);} else return RC_NORESOURCES;
				} else if (aggs->nValues<=0xFF) {
					if ((p=alloc(4))!=NULL) {p[0]=OP_PARAM; p[1]=0xFF; p[2]=QV_AGGS; p[3]=byte(aggs->nValues);} else return RC_NORESOURCES;
				} else return RC_NORESOURCES;
				assert(aggs->vals==NULL||aggs->fFree); aggs->fFree=true;
				if ((aggs->vals=(Value*)ma->realloc((Value*)aggs->vals,(aggs->nValues+1)*sizeof(Value)))==NULL) return RC_NORESOURCES;
				Value &to=((Value*)aggs->vals)[aggs->nValues];
				if (node->operands[0].type==VT_EXPRTREE) {
					Expr *ag; if ((rc=Expr::compile((ExprTree*)node->operands[0].exprt,ag,ma,false))!=RC_OK) return rc; to.set(ag,1);
				} else if ((rc=copyV(node->operands[0],to,ma))!=RC_OK) return rc;
				to.op=node->op; //if ((node->flags&DISTINCT_OP)!=0) ->META_PROP_DISTINCT
				lStack++; return RC_OK;
			}
			if ((node->flags&DISTINCT_OP)!=0) {f|=CND_DISTINCT; if (l==1) l=2;}
		}
		break;
	}
	for (i=0; i<node->nops; i++) if ((rc=compileValue(node->operands[i],flg))!=RC_OK) return rc;
	if ((p=alloc(l))==NULL) return RC_NORESOURCES;
	if (l==1) *p=op; else {p[0]=op|0x80; p[1]=f; if (l>2) p[2]=(byte)node->nops;}
	assert(fCollectRefs || lStack>=node->nops && node->nops>0);
	lStack-=node->nops-1;
	return RC_OK;
}

RC ExprCompileCtx::compileValue(const Value& v,unsigned flg) {
	RC rc; uint32_t l; byte *p,fc=(flg&CV_CARD)!=0?0x80:0; Value val; uint16_t vdx,pdx;
	if (fCollectRefs) {
		switch (v.type) {
		case VT_EXPRTREE: return compileNode((const ExprTree*)v.exprt);
		case VT_VARREF: 
			switch (v.refV.flags&VAR_TYPE_MASK) {
			default: break;
			case VAR_SELF: vdx=(0xFFFE)-v.refV.refN; goto addref;
			case 0: vdx=v.refV.refN;
			addref: if ((flg&CV_NDATA)==0 && v.length!=0 && (rc=addExtRef(v.refV.id,vdx,((flg&CV_CARD)?PROP_ORD:0)|((flg&CV_OPT)!=0?PROP_OPTIONAL:0),vdx,pdx))!=RC_OK) return rc;
			}
			break;
		case VT_URIID: return addExtRef(v.uid,0xFFFE,PROP_OPTIONAL|PROP_NO_LOAD,vdx,pdx);
		case VT_IDENTITY: return addExtRef(v.iid,0xFFFF,PROP_OPTIONAL|PROP_NO_LOAD,vdx,pdx);
		case VT_REFIDPROP: case VT_REFIDELT: return addExtRef(v.refId->pid,0,PROP_OPTIONAL|PROP_NO_LOAD,vdx,pdx);	// Identity in PINID?
		case VT_REFID:
			// addExtRef IdentityID if not STORE_OWNER
			break;
		case VT_EXPR:
		case VT_STMT:
			//...
			break;
		case VT_ARRAY: case VT_STRUCT:
			//...
			break;
		case VT_COLLECTION:
			//...
			break;
		}
		return RC_OK;
	}
	switch (v.type) {
	case VT_EXPRTREE: return compileNode((const ExprTree*)v.exprt);
	case VT_VARREF:
		if ((pdx=v.refV.flags&VAR_TYPE_MASK)==0 || pdx==VAR_SELF) {
			const bool fSelf=pdx!=0;
			if (v.length==0 || v.refV.id==PROP_SPEC_SELF) {
				if ((p=alloc(2))==NULL) return RC_NORESOURCES; p[0]=(fSelf?OP_ENVV:OP_VAR)|fc; p[1]=v.refV.refN; pHdr->hdr.flags|=EXPR_SELF;
			} else {
				if ((flg&CV_NDATA)!=0) pdx=0;
				else if ((rc=addExtRef(v.refV.id,fSelf?(0xFFFE)-v.refV.refN:v.refV.refN,((flg&CV_CARD)?PROP_ORD:0)|((flg&CV_OPT)!=0?PROP_OPTIONAL:0),vdx,pdx))!=RC_OK) return rc;
				byte op=fSelf?OP_ENVV_PROP:OP_PROP; unsigned eid=0; const bool fExt=vdx>3||pdx>0x3F; l=fExt?5:2;
				if (v.eid!=STORE_COLLECTION_ID) {op=fSelf?OP_ENVV_ELT:OP_ELT; eid=afy_enc32zz(v.eid); l+=afy_len32(eid);}
				if ((p=alloc(l))==NULL) return RC_NORESOURCES; p[0]=op|fc;
				if (!fExt) {p[1]=byte(vdx<<6|pdx); p+=2;} else {p[1]=0xFF; p[2]=byte(vdx); p[3]=byte(pdx); p[4]=byte(pdx>>8); p+=5;}
				if (v.eid!=STORE_COLLECTION_ID) afy_enc32(p,eid);
			}
		} else switch (pdx) {
		case VAR_REXP:
			if ((p=alloc(3))!=NULL) {p[0]=OP_RXREF|fc; p[1]=byte(v.refV.id); p[2]=byte(v.refV.id>>8);} else return RC_NORESOURCES;
			break;
		case VAR_NAMED:
			//???
			//break;
		case VAR_PARAM:
			pHdr->hdr.flags|=EXPR_PARAMS;
		default:
			assert(v.refV.flags!=0xFFFF);
			if ((pdx=(pdx>>13)-1)<4 && v.refV.refN<0x3F) {
				if ((p=alloc(2))!=NULL) {p[0]=OP_PARAM|fc; p[1]=byte(v.refV.refN|pdx<<6);} else return RC_NORESOURCES;
			} else {
				if ((p=alloc(4))!=NULL) {p[0]=OP_PARAM|fc; p[1]=0xFF; p[2]=byte(pdx); p[3]=v.refV.refN;} else return RC_NORESOURCES;
			}
			break;
		}
		break;
	case VT_CURRENT:
		if ((p=alloc(2))==NULL) return RC_NORESOURCES; p[0]=OP_CURRENT; p[1]=(byte)v.ui; break;
	case VT_STREAM:
		if ((rc=streamToValue(v.stream.is,val,ma))!=RC_OK) return rc;
		if ((l=(uint32_t)AfyKernel::serSize(val))==0) rc=RC_TYPE;
		else if ((p=alloc(l+1))==NULL) rc=RC_NORESOURCES;
		else {*p=OP_CON|fc; AfyKernel::serialize(val,p+1);}
		freeV(val); if (rc!=RC_OK) return rc;
		break;
	case VT_URIID: case VT_IDENTITY:
		if ((rc=addExtRef(v.uid,v.type==VT_URIID?0:0xFFFF,v.type==VT_URIID?PROP_OPTIONAL|PROP_NO_LOAD:0,vdx,pdx))!=RC_OK) return rc;
		if ((p=alloc(pdx>=0xFF?4:2))==NULL) return RC_NORESOURCES; p[0]=v.type==VT_URIID?OP_CONID:OP_CONID|0x80;
		if (pdx<0xFF) p[1]=(byte)pdx; else {p[1]=0xFF; p[2]=byte(pdx); p[3]=byte(pdx>>8);}
		break;
	case VT_ARRAY: case VT_STRUCT:
		if (v.fcalc==0) goto const_data;
		for (l=0; l<v.length; l++) if ((rc=compileValue(v.varray[l],0))!=RC_OK) return rc;
		if (lStack+l>pHdr->hdr.lStack) pHdr->hdr.lStack=lStack+l;
		if ((p=alloc(1+afy_len32(v.length)))==NULL) return RC_NORESOURCES;
		*p++=v.type==VT_ARRAY?OP_ARRAY:OP_STRUCT; afy_enc32(p,v.length);
		break;
	case VT_COLLECTION:
		if (v.fcalc==0) goto const_data;
		//???
		break;
//	case VT_REFIDPROP: case VT_REFIDELT:
		// addExtRef
		// spec encoding
//	case VT_REFID:
		// addExtRef IdentityID if not STORE_OWNER
//		break;
	default:
	const_data:
		if ((l=(uint32_t)AfyKernel::serSize(v))==0) return RC_TYPE;
		if ((p=alloc(l+1))==NULL) return RC_NORESOURCES;
		*p=OP_CON|fc; AfyKernel::serialize(v,p+1); break;
	}
	if ((flg&CV_PROP)!=0) {
		if (v.property==STORE_INVALID_URIID) return RC_INTERNAL;
		if ((rc=addExtRef(v.property,0,PROP_OPTIONAL|PROP_NO_LOAD,vdx,pdx))!=RC_OK) return rc;
		fc=v.meta!=0?0x80:0; const bool fExt=vdx>3||pdx>0x3F; l=fExt?5:2;
		if ((p=alloc(l+(fc!=0?1:0)))==NULL) return RC_NORESOURCES; p[0]=OP_SETPROP|fc;
		if (!fExt) p[1]=byte(vdx<<6|pdx); else {p[1]=0xFF; p[2]=byte(vdx); p[3]=byte(pdx); p[4]=byte(pdx>>8);}
		if (fc!=0) p[l]=v.meta;
	}
	if (++lStack>pHdr->hdr.lStack) pHdr->hdr.lStack=lStack; return RC_OK;
}

RC ExprCompileCtx::putCondCode(ExprOp op,unsigned fa,uint32_t lbl,bool flbl,int nops) {
	if (!fCollectRefs) {
		const bool fJump=(fa&CND_MASK)>=6; uint32_t l=nops!=0?3:2;
		if (fa>0x7F) {fa|=CND_EXT; l++;} if (fJump) l+=2;
		byte *p=alloc(l); if (p==NULL) return RC_NORESOURCES;
		p[0]=op; if (nops!=0) {p[1]=(byte)nops; p++;}
		p[1]=byte(fa); p+=2; if ((fa&CND_EXT)!=0) *p++=byte(fa>>8);
		if (fJump) {
			if (!flbl) {p[0]=byte(lbl); p[1]=byte(lbl>>8);}
			else if ((labels=new(ma) ExprLbl(labels,lbl,lCode-2))==NULL) return RC_NORESOURCES;
		}
	}
	return RC_OK;
}

void ExprCompileCtx::adjustRef(uint32_t lbl) {
	bool fDel=true;
	for (ExprLbl *lb,**plb=&labels; (lb=*plb)!=NULL; ) {
		if (uint32_t(lb->label)!=lbl) {plb=&lb->next; fDel=false;}
		else {
			uint32_t l=uint32_t(lCode)-lb->addr;
			byte *p=pCode+lb->addr; p[0]=byte(l); p[1]=byte(l>>8);
			if (!fDel) plb=&lb->next; else {*plb=lb->next; ma->free(lb);}
		}
	}
}

RC ExprCompileCtx::compileCondition(const ExprTree *node,unsigned mode,uint32_t lbl,unsigned flg) {
	RC rc=RC_OK; uint32_t lbl1; unsigned i; ExprOp op=node->op; const Value *pv,*cv; bool fJump;
	switch (op) {
	default: assert(0); rc=RC_INTERNAL; break;
	case OP_LOR: pHdr->hdr.flags|=EXPR_DISJUNCTIVE; flg|=CV_OPT;
	case OP_LAND:
		assert(node->nops==2 && (node->flags&NOT_BOOLEAN_OP)==0);
		fJump=jumpOp[mode&7][op-OP_LAND]; lbl1=fJump?++labelCount:lbl;
		if ((rc=compileCondition((const ExprTree*)node->operands[0].exprt,logOp[mode&7][op-OP_LAND]|(mode&CND_VBOOL),lbl1,flg))==RC_OK) {
			uint32_t slStack=lStack; lStack=0; 
			if ((rc=compileCondition((const ExprTree*)node->operands[1].exprt,mode,lbl,flg))==RC_OK)
				{if (fJump) adjustRef(lbl1); lStack=max(lStack,slStack);}
		}
		break;
	case OP_IN:
		pv=&node->operands[1];
		switch (pv->type) {
		case VT_ARRAY: if (pv->length<2) break;
		case VT_COLLECTION:
			if ((rc=compileValue(node->operands[0],flg))!=RC_OK) return rc;
//			if ((node->flags&FOR_ALL_RIGHT_OP)!=0) mode|=CND_FORALL_R;			-- check!
//			if ((node->flags&EXISTS_RIGHT_OP)!=0) mode|=CND_EXISTS_R;
			if ((node->flags&CASE_INSENSITIVE_OP)!=0) mode|=CND_NCASE;
			if ((node->flags&FOR_ALL_LEFT_OP)!=0) mode|=CND_FORALL_L;
			if ((node->flags&EXISTS_LEFT_OP)!=0) mode|=CND_EXISTS_L;
			if ((node->flags&NOT_BOOLEAN_OP)!=0) mode^=CND_NOT|1;
			fJump=jumpOp[mode&7][1]; lbl1=fJump?++labelCount:lbl;
			for (cv=pv->type==VT_COLLECTION?pv->nav->navigate(GO_FIRST):&pv->varray[i=0]; cv!=NULL;) {
				if ((rc=compileValue(*cv,flg))!=RC_OK) return rc;
				if (pv->type==VT_COLLECTION) {if ((cv=pv->nav->navigate(GO_NEXT))==NULL) break;}
				else if (++i<pv->length) cv=&pv->varray[i]; else break;
				if ((rc=putCondCode((ExprOp)OP_IN1,logOp[mode&7][1],lbl1))!=RC_OK) return rc;
				assert(fCollectRefs || lStack>=1); if ((mode&CND_VBOOL)==0) --lStack;
			}
			if ((rc=putCondCode(ExprOp(OP_IN|0x80),mode,lbl))==RC_OK) {assert(fCollectRefs || lStack>=2); lStack-=((mode&CND_VBOOL)!=0?1:2);}
			if (fJump) adjustRef(lbl1); return rc;
		case VT_EXPRTREE:
			if (((ExprTree*)pv->exprt)->op==OP_ARRAY) {
				//???
				// return rc;
			}
		default: break;
		}
		if ((node->flags&EXCLUDE_LBOUND_OP)!=0) mode|=CND_IN_LBND;
		if ((node->flags&EXCLUDE_RBOUND_OP)!=0) mode|=CND_IN_RBND;
		goto bool_op;
	case OP_EXISTS:
		if (node->operands[0].type==VT_STMT) {
			if ((rc=compileValue(node->operands[0],0))==RC_OK) {
				if ((node->flags&NOT_BOOLEAN_OP)!=0) mode^=CND_NOT|1;
				if ((rc=putCondCode(ExprOp(OP_EXISTS|0x80),mode,lbl))==RC_OK) {assert(fCollectRefs || lStack>=1); if ((mode&CND_VBOOL)==0) lStack--;}
			}
			return rc;
		}
		flg|=(node->flags&NOT_BOOLEAN_OP)!=0?CV_CARD|CV_OPT:CV_CARD; goto bool_op;
	case OP_IS_A:
		if (node->nops<=2) goto bool_op;
		for (i=0; i<node->nops; i++) if ((rc=compileValue(node->operands[i],flg))!=RC_OK) return rc;
		if ((node->flags&NOT_BOOLEAN_OP)!=0) mode^=CND_NOT|1;
		if ((rc=putCondCode(ExprOp(OP_IS_A|0x80),mode,lbl,true,node->nops))==RC_OK) {assert(fCollectRefs || lStack>=node->nops); lStack-=node->nops-((mode&CND_VBOOL)!=0?1:0);}
		break;
	case OP_ISLOCAL: if (node->operands[0].type==VT_VARREF && node->operands[0].length==0) flg|=CV_NDATA; goto bool_op;
	case OP_NE: if ((node->flags&NULLS_NOT_INCLUDED_OP)==0) flg|=CV_OPT;
	case OP_BEGINS: case OP_ENDS: case OP_CONTAINS: case OP_EQ: case OP_LT: case OP_LE: case OP_GT: case OP_GE: case OP_SIMILAR:
	bool_op: 
		assert(node->nops<=2);
		if ((rc=compileValue(node->operands[0],flg))==RC_OK && (node->nops==1 || (rc=compileValue(node->operands[1],flg))==RC_OK)) {
			if ((node->flags&CASE_INSENSITIVE_OP)!=0) mode|=CND_NCASE;		// also COMP_PATTERN_OP -> CND_CMPLRX for OP_SIMILAR
			if ((node->flags&FOR_ALL_LEFT_OP)!=0) mode|=CND_FORALL_L;
			if ((node->flags&EXISTS_LEFT_OP)!=0) mode|=CND_EXISTS_L;
			if ((node->flags&FOR_ALL_RIGHT_OP)!=0) mode|=CND_FORALL_R;
			if ((node->flags&EXISTS_RIGHT_OP)!=0) mode|=CND_EXISTS_R;
			if ((node->flags&NOT_BOOLEAN_OP)!=0) mode^=CND_NOT|1;
			if ((rc=putCondCode(op,mode,lbl))==RC_OK) {assert(fCollectRefs || lStack>=node->nops); lStack-=node->nops-((mode&CND_VBOOL)!=0?1:0);}
		}
		break;
	}
	return rc;
}

RC Expr::decompile(ExprTree*&res,Session *ses) const
{
	if ((hdr.flags&EXPR_EXTN)!=0) {res=NULL; return RC_INVPARAM;}
	if ((hdr.flags&EXPR_NO_CODE)!=0) {
		//...
		assert(0);
	}
	Value stack[256],*top=stack,*end=stack+256; RC rc=RC_OK; const PropertyID *pids; unsigned nPids; ElementID eid;
	for (const byte *codePtr=(const byte*)(&hdr+1)+hdr.lProps,*codeEnd=(const byte*)&hdr+hdr.lExpr; codePtr<codeEnd; ) {
		byte op=*codePtr++; unsigned idx; ExprTree *exp=NULL,*exp0; uint32_t jsht,l,flg=0; bool ff=(op&0x80)!=0;
		switch (op&=0x7F) {
		case OP_CON:
			if (top>=end) {rc=RC_NORESOURCES; break;}
			if ((rc=AfyKernel::deserialize(*top,codePtr,codeEnd,ses,true))==RC_OK) top++;
			break;
		case OP_CONID:
			l=*codePtr++; if (l==0xFF) {l=codePtr[0]|codePtr[1]<<8; codePtr+=2;}
			getExtRefs(ff?0xFFFF:0,pids,nPids); if (l<nPids) l=pids[l]; else {rc=RC_CORRUPTED; break;}
			if (ff) top->setIdentity(l); else top->setURIID(l&STORE_MAX_URIID);
			top++; break;
		case OP_VAR: case OP_PROP: case OP_ELT: case OP_ENVV: case OP_ENVV_PROP: case OP_ENVV_ELT:
			if (top>=end) {rc=RC_NORESOURCES; break;}
		case OP_SETPROP:
			idx=*codePtr++; eid=STORE_COLLECTION_ID;
			if (op==OP_VAR||op==OP_ENVV) l=STORE_INVALID_URIID;
			else {
				if (idx!=0xFF) {l=idx&0x3F; idx>>=6;} else {idx=codePtr[0]; l=codePtr[1]|codePtr[2]<<8; codePtr+=3;}
				if (idx>=hdr.nVars) {rc=RC_CORRUPTED; break;} const VarHdr *vh=(VarHdr*)(&hdr+1);
				for (unsigned i=0; i<idx; i++) vh=(VarHdr*)((byte*)(vh+1)+vh->nProps*sizeof(uint32_t));
				idx=op<=OP_ENVV_PROP?(0xFFFE)-vh->var:vh->var;
				if (l<vh->nProps) l=((PropertyID*)(vh+1))[l]&STORE_MAX_URIID; else {rc=RC_CORRUPTED; break;}
				if (op==OP_ELT||op==OP_ENVV_ELT) {afy_dec32(codePtr,eid); eid=afy_dec32zz(eid);}
				else if (op==OP_SETPROP) {if (top>stack) {top[-1].property=l; if (ff) top[-1].meta=*codePtr++;} else rc=RC_CORRUPTED; break;}
			}
			top->setVarRef((byte)idx,l); top->eid=eid; if (op<=OP_ENVV) top->refV.flags=VAR_SELF;
			if (ff) {if ((exp=new(1,ses) ExprTree(OP_COUNT,1,0,0,top,ses))!=NULL) top->set(exp); else rc=RC_NORESOURCES;}
			top++; break;
		case OP_PARAM: case OP_RXREF:
			if (top>=end) {rc=RC_NORESOURCES; break;}
			if (op==OP_RXREF) {top->setParam(0); top->refV.flags=VAR_REXP; top->refV.id=codePtr[0]|codePtr[1]<<8; codePtr+=2; top->length=1;}
			else if ((idx=*codePtr++)!=0xFF) {top->setParam(idx&0x3F); top->refV.flags=(uint16_t(idx&0xC0)+0x40)<<7;}
			else {top->setParam(codePtr[1]); top->refV.flags=(uint16_t(codePtr[0])+1)<<13; codePtr+=2;}
			if (ff) {if ((exp=new(1,ses) ExprTree(OP_COUNT,1,0,0,top,ses))!=NULL) top->set(exp); else rc=RC_NORESOURCES;}
			top++; break;
		case OP_PATH:
			l=*codePtr++; if (ff) flg=*codePtr++;
			for (idx=0; idx<l; idx++) {
				byte f=*codePtr++; Value operands[4]; unsigned nops=2; operands[0].setError(); Expr *filter; ExprTree *et; uint16_t rm,rx;
				if ((f&0x02)!=0) {assert(top>=stack+1); operands[1]=*--top;}
				else if ((rc=AfyKernel::deserialize(operands[1],codePtr,codeEnd,ses,true))!=RC_OK) break;
				switch (f&0x18) {
				case 0: break;
				case 0x08: afy_dec32(codePtr,jsht); jsht=afy_dec32zz((uint32_t)jsht); operands[2].set((unsigned)jsht); nops=3; break;
				case 0x10: assert(top>=stack+1); operands[2]=*--top; nops=3; break;
				case 0x18: 
					if ((rc=deserialize(filter,codePtr,codeEnd,ses))==RC_OK) {		 //classID ??
						if ((rc=filter->decompile(et,ses))==RC_OK) {operands[2].set(et); nops=3;}
						filter->destroy();
					}
					break;
				}
				if (rc!=RC_OK) break;
				switch (f&0xE0) {
				default: break;
				case 0x80: afy_dec16(codePtr,rm); afy_dec16(codePtr,rx); operands[3].set((unsigned)rx<<16|rm); nops=4; break;
				case 0xA0: assert(top>=stack+1); operands[3]=*--top; nops=4; break;
				}
				ExprTree *ex=new(nops,ses) ExprTree(OP_PATH,nops,0,(flg>>5&0x06)|(flg&0x01),operands,ses);
				if (ex==NULL) rc=RC_NORESOURCES; else if (exp==NULL) exp=exp0=ex; else {exp->operands[0].set(ex); exp=ex;}
			}
			if (rc==RC_OK) {
				assert(exp!=NULL && exp->op==OP_PATH); assert(top>=stack+1);
				exp->operands[0]=top[-1]; top[-1].set(exp0);
			} else if (exp!=NULL) exp0->destroy();
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
			codePtr+=2; break;
		case OP_CATCH:
			//???
			break;
		case OP_COALESCE:
			//???
			codePtr+=2; break;
		case OP_IN1:
			ff=true; if (((l=*codePtr++)&CND_EXT)!=0) codePtr++; if ((l&CND_MASK)>=6) codePtr+=2;
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
					if (op==OP_IN) {exp=(ExprTree*)(--top)->exprt; goto bool_op;} else break;
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
			if (top[-1].type==VT_EXPRTREE && (exp=(ExprTree*)top[-1].exprt)->op==OP_COUNT) exp->op=OP_EXISTS;
			else if ((exp=new(1,ses) ExprTree(OP_EXISTS,1,0,0,top-1,ses))==NULL) {rc=RC_NORESOURCES; break;}
			--top; goto bool_op;
		case OP_EQ: case OP_NE: case OP_LT: case OP_LE: case OP_GT: case OP_GE: case OP_CONTAINS: case OP_BEGINS: case OP_ENDS: case OP_SIMILAR:
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
			if ((l&CND_NCASE)!=0) exp->flags|=CASE_INSENSITIVE_OP;		// also covers CND_CMPLRX -> COMP_PATTERN_OP
			if (op==OP_IN) {
				if ((l&CND_IN_LBND)!=0) exp->flags|=EXCLUDE_LBOUND_OP;
				if ((l&CND_IN_RBND)!=0) exp->flags|=EXCLUDE_RBOUND_OP;
				//if (ff) ...
			}
			if ((l&CND_NOT)!=0) {exp->flags|=NOT_BOOLEAN_OP; l^=1;}
			if ((l&=CND_MASK)<6) jsht=hdr.lExpr; else {jsht=(codePtr[0]|codePtr[1]<<8)+uint32_t(codePtr-(const byte*)(this+1)); codePtr+=2;}
			top->set(exp); top->meta=byte(l); top->eid=jsht; top++;
			while (top>=stack+2 && (logOp[l][idx=0]==top[-2].meta || logOp[l][idx=1]==top[-2].meta)
							&& (jumpOp[l][idx]?codePtr-(const byte*)(this+1):jsht)==top[-2].eid) {
				if ((exp=new(2,ses) ExprTree((ExprOp)(idx+OP_LAND),2,0,0,top-2,ses))==NULL) {rc=RC_NORESOURCES; break;}
				--top; top[-1].set(exp); top[-1].meta=(byte)l; top[-1].eid=jsht;
			}
			break;
		default:
			l=SInCtx::opDscr[op].nOps[0];
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
	ExprHdr hdr(0,0,0,0,0,0); memcpy(&hdr,buf,sizeof(ExprHdr));
	if (hdr.lExpr==0 || size_t(ebuf-buf)<hdr.lExpr) return RC_CORRUPTED;
	byte *p=(byte*)ma->malloc(hdr.lExpr-sizeof(ExprHdr)+sizeof(Expr)); if (p==NULL) return RC_NORESOURCES;
	memcpy(&((Expr*)p)->hdr+1,(const byte*)buf+sizeof(ExprHdr),hdr.lExpr-sizeof(ExprHdr));
	exp=new(p) Expr(hdr); buf+=hdr.lExpr; return RC_OK;
}

Expr *Expr::clone(const Expr *exp,MemAlloc *ma)
{
	byte *p=(byte*)ma->malloc(exp->hdr.lExpr-sizeof(ExprHdr)+sizeof(Expr)); if (p==NULL) return NULL;
	memcpy(&((Expr*)p)->hdr+1,&exp->hdr+1,exp->hdr.lExpr-sizeof(ExprHdr)); return new(p) Expr(exp->hdr);
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

RC Expr::getPropDNF(ushort var,PropDNF *&dnf,size_t& ldnf,MemAlloc *ma) const
{
	RC rc=RC_OK; ExprTree *et;
	if ((hdr.flags&EXPR_DISJUNCTIVE)==0) {
		const PropertyID *pids; unsigned nPids; PropDNF *pd; getExtRefs(var,pids,nPids);
		if (pids==NULL||nPids==0) {dnf=NULL; ldnf=0;}
		else if ((pd=(PropDNF*)ma->malloc(sizeof(PropDNF)+(nPids-1)*sizeof(PropertyID)))==NULL) return RC_NORESOURCES;
		else {
			pd->nIncl=pd->nExcl=0;
			for (unsigned i=0; i<nPids; i++) if ((pids[i]&(PROP_OPTIONAL|PROP_NO_LOAD))==0) pd->pids[pd->nIncl++]=pids[i]&STORE_MAX_URIID;
			if (pd->nIncl==0) {ma->free(pd); dnf=NULL; ldnf=0;} else {dnf=pd; ldnf=sizeof(PropDNF)+(pd->nIncl-1)*sizeof(PropertyID);}
		}
	} else if ((rc=decompile(et,Session::getSession()))==RC_OK) {
		rc=et->getPropDNF(var,dnf,ldnf,ma); et->destroy();
	}
	return rc;
}

RC ExprTree::getPropDNF(ushort var,PropDNF *&dnf,size_t& ldnf,MemAlloc *ma) const
{
	ushort i; RC rc; PropDNF *dnf2; size_t ldnf2;
	switch (op) {
	case OP_LAND: case OP_LOR:
		assert(operands[0].type==VT_EXPRTREE && operands[1].type==VT_EXPRTREE);
		if ((rc=((ExprTree*)operands[0].exprt)->getPropDNF(var,dnf,ldnf,ma))!=RC_OK) return rc;
		dnf2=NULL; ldnf2=0;
		if ((rc=((ExprTree*)operands[1].exprt)->getPropDNF(var,dnf2,ldnf2,ma))==RC_OK)
			rc=op==OP_LAND?PropDNF::andP(dnf,ldnf,dnf2,ldnf2,ma):PropDNF::orP(dnf,ldnf,dnf2,ldnf2,ma);
		break;
	case OP_NE: assert((flags&NOT_BOOLEAN_OP)==0); break;
	case OP_CON: break;
	case OP_EXISTS:
		if (operands[0].type==VT_VARREF && operands[0].length!=0 && operands[0].refV.refN==var &&
			operands[0].refV.id!=PROP_SPEC_ANY && operands[0].refV.id!=PROP_SPEC_PINID &&
					(rc=PropDNF::andP(dnf,ldnf,&operands[0].refV.id,1,ma,(flags&NOT_BOOLEAN_OP)!=0))!=RC_OK) return rc;
		break;
	default:
		for (i=0; i<nops; i++) switch (operands[i].type) {
		default: break;
		case VT_VARREF:
			if ((operands[i].refV.flags&VAR_TYPE_MASK)==0 && operands[i].length==1 && operands[i].refV.refN==var && operands[i].refV.id!=PROP_SPEC_ANY 
				&& operands[i].refV.id!=PROP_SPEC_PINID && (rc=PropDNF::andP(dnf,ldnf,&operands[i].refV.id,1,ma,false))!=RC_OK) return rc;
			break;
		case VT_EXPRTREE:
			if ((rc=((ExprTree*)operands[i].exprt)->getPropDNF(var,dnf,ldnf,ma))!=RC_OK) return rc;
			break;
		}
		break;
	}
	return RC_OK;
}

RC PropDNF::andP(PropDNF *&dnf,size_t& ldnf,PropDNF *rhs,size_t lrhs,MemAlloc *ma)
{
	RC rc=RC_OK;
	if (dnf==NULL || ldnf==0) {ma->free(dnf); dnf=rhs; ldnf=lrhs;}
	else if (rhs!=NULL && lrhs!=0) {
		if (rhs->isConjunctive(lrhs)) {
			if (rhs->nIncl!=0) rc=andP(dnf,ldnf,rhs->pids,rhs->nIncl,ma,false);
			if (rc==RC_OK && rhs->nExcl!=0) rc=andP(dnf,ldnf,rhs->pids+rhs->nIncl,rhs->nExcl,ma,true);
		} else if (dnf->isConjunctive(ldnf)) {
			if (dnf->nIncl!=0) rc=andP(rhs,lrhs,dnf->pids,dnf->nIncl,ma,false);
			if (rc==RC_OK && dnf->nExcl!=0) rc=andP(rhs,lrhs,dnf->pids+dnf->nIncl,dnf->nExcl,ma,true);
			PropDNF *tmp=dnf; dnf=rhs; ldnf=lrhs; rhs=tmp;
		} else {
			PropDNF *res=NULL,*tmp=NULL; size_t lres=0,ltmp=0;
			for (const PropDNF *p=rhs,*end=(const PropDNF*)((byte*)rhs+lrhs); p<end; p=(const PropDNF*)((byte*)(p+1)+int(p->nIncl+p->nExcl-1)*sizeof(PropertyID))) {
				if (res==NULL) {
					if ((res=(PropDNF*)ma->malloc(ldnf))!=NULL) memcpy(res,dnf,lres=ldnf); else {rc=RC_NORESOURCES; break;}
					if (p->nIncl!=0 && (rc=andP(res,lres,p->pids,p->nIncl,ma,false))!=RC_OK) break;
					if (p->nExcl!=0 && (rc=andP(res,lres,p->pids+p->nIncl,p->nExcl,ma,true))!=RC_OK) break;
				} else {
					if ((tmp=(PropDNF*)ma->malloc(ltmp=ldnf))!=NULL) memcpy(tmp,dnf,ldnf); else {rc=RC_NORESOURCES; break;}
					if (p->nIncl!=0 && (rc=andP(tmp,ltmp,p->pids,p->nIncl,ma,false))!=RC_OK) break;
					if (p->nExcl!=0 && (rc=andP(tmp,ltmp,p->pids+p->nIncl,p->nExcl,ma,true))!=RC_OK) break;
					if ((rc=orP(res,lres,tmp,ltmp,ma))!=RC_OK) break;
				}
			}
			ma->free(dnf); dnf=res; ldnf=lres;
		}
		ma->free(rhs); if (rc==RC_OK) normalize(dnf,ldnf,ma);
	}
	return rc;
}

RC PropDNF::orP(PropDNF *&dnf,size_t& ldnf,PropDNF *rhs,size_t lrhs,MemAlloc *ma)
{
	if (dnf!=NULL && ldnf!=0) {
		if (rhs==NULL || lrhs==0) {ma->free(dnf); dnf=NULL; ldnf=0;}
		else {
			if ((dnf=(PropDNF*)ma->realloc(dnf,ldnf+lrhs))==NULL) return RC_NORESOURCES;
			memcpy((byte*)dnf+ldnf,rhs,lrhs); ldnf+=lrhs; ma->free(rhs);
			normalize(dnf,ldnf,ma);
		}
	}
	return RC_OK;
}

RC PropDNF::andP(PropDNF *&dnf,size_t& ldnf,const PropertyID *pids,unsigned np,MemAlloc *ma,bool fNot)
{
	if (pids!=NULL && np!=0) {
		if (dnf==NULL || ldnf==0) {
			ma->free(dnf);
			if ((dnf=(PropDNF*)ma->malloc(sizeof(PropDNF)+int(np-1)*sizeof(PropertyID)))==NULL) return RC_NORESOURCES;
			if (fNot) {dnf->nExcl=(ushort)np; dnf->nIncl=0;} else {dnf->nIncl=(ushort)np; dnf->nExcl=0;}
			memcpy(dnf->pids,pids,np*sizeof(PropertyID)); ldnf=sizeof(PropDNF)+int(np-1)*sizeof(PropertyID);
		} else {
			bool fAdded=false;
			for (PropDNF *p=dnf,*end=(PropDNF*)((byte*)p+ldnf); p<end; p=(PropDNF*)((byte*)(p+1)+int(p->nIncl+p->nExcl-1)*sizeof(PropertyID))) {
				for (unsigned i=fNot?p->nIncl:0,j=0,n=fNot?p->nExcl:p->nIncl;;) {
					if (i>=n) {
						if (j<np) {
							unsigned l=(np-j)*sizeof(PropertyID); ptrdiff_t sht=(byte*)p-(byte*)dnf;
							if ((dnf=(PropDNF*)ma->realloc(dnf,ldnf+l))==NULL) return RC_NORESOURCES;
							p=(PropDNF*)((byte*)dnf+sht); end=(PropDNF*)((byte*)dnf+ldnf);
							if ((void*)&p->pids[i]!=(void*)end) memmove(&p->pids[i+np-j],&p->pids[i],(byte*)end-(byte*)&p->pids[i]);
							memcpy(&p->pids[i],&pids[j],l); ldnf+=l; end=(PropDNF*)((byte*)dnf+ldnf); fAdded=true;
							if (fNot) p->nExcl+=ushort(np-j); else p->nIncl+=ushort(np-j);
						}
						break;
					} else if (j>=np) break;
					else if (p->pids[i]<pids[j]) ++i;
					else if (p->pids[i]==pids[j]) ++i,++j;
					else {
						unsigned k=j; while (++j<np && p->pids[i]>pids[j]); unsigned m=j-k;
						unsigned l=m*sizeof(PropertyID); ptrdiff_t sht=(byte*)p-(byte*)dnf;
						if ((dnf=(PropDNF*)ma->realloc(dnf,ldnf+l))==NULL) return RC_NORESOURCES;
						p=(PropDNF*)((byte*)dnf+sht); end=(PropDNF*)((byte*)dnf+ldnf);
						memmove(&p->pids[i+m],&p->pids[i],(byte*)end-(byte*)&p->pids[i]);
						memcpy(&p->pids[i],&pids[k],l); ldnf+=l; end=(PropDNF*)((byte*)dnf+ldnf);
						i+=m; n+=m; fAdded=true; if (fNot) p->nExcl+=ushort(m); else p->nIncl+=ushort(m);
					}
				}
			}
			if (fAdded && (dnf->nIncl==0 || sizeof(PropDNF)+int(dnf->nIncl-1)*sizeof(PropertyID)!=ldnf)) normalize(dnf,ldnf,ma);
		}
	}
	return RC_OK;
}

RC PropDNF::orP(PropDNF *&dnf,size_t& ldnf,const PropertyID *pids,unsigned np,MemAlloc *ma,bool fNot)
{
	if (dnf!=NULL && ldnf!=0) {
		if (pids==NULL || np==0) {ma->free(dnf); dnf=NULL; ldnf=0;}
		else {
			if ((dnf=(PropDNF*)ma->realloc(dnf,ldnf+sizeof(PropDNF)+int(np-1)*sizeof(PropertyID)))==NULL) return RC_NORESOURCES;
			PropDNF *pd=(PropDNF*)((byte*)dnf+ldnf); ldnf+=sizeof(PropDNF)+int(np-1)*sizeof(PropertyID);
			memcpy(pd->pids,pids,np*sizeof(PropertyID)); if (fNot) pd->nExcl=(ushort)np; else pd->nIncl=(ushort)np;
			normalize(dnf,ldnf,ma);
		}
	}
	return RC_OK;
}

void PropDNF::normalize(PropDNF *&dnf,size_t& ldnf,MemAlloc *ma)
{
	//...
}

//----------------------------------------------------------------------------------------------------

RC ExprTree::node(Value& res,Session *ses,ExprOp op,unsigned nOperands,const Value *operands,unsigned flags)
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
			else {fFold=false; if ((cv[i].flags&HEAP_TYPE_MASK)<SES_HEAP) setHT(cv[i],SES_HEAP);}
			break;
		case VT_EXPR: case VT_STMT: case VT_ARRAY: case VT_STRUCT: case VT_REFIDPROP: case VT_REFIDELT: if (cv[i].fcalc==0) break;
		case VT_VARREF: case VT_CURRENT: fFold=false; break;
		}
		vRefs(vrefs,vRefs(cv[i]));
	}
	switch (op) {
	default: break;
	case OP_PLUS: case OP_MINUS:
		if (!fFold && isNumeric((ValueType)cv[1].type) && cv[0].type==VT_EXPRTREE && (((ExprTree*)cv[0].exprt)->op==OP_PLUS || ((ExprTree*)cv[0].exprt)->op==OP_MINUS) && isNumeric((ValueType)((ExprTree*)cv[0].exprt)->operands[1].type)
			&& Expr::calc(op==((ExprTree*)cv[0].exprt)->op?OP_PLUS:OP_MINUS,((ExprTree*)cv[0].exprt)->operands[1],&cv[1],2,0,ses)==RC_OK) {res=cv[0]; return RC_OK;}
		break;
	case OP_MUL:
		if (!fFold && isNumeric((ValueType)cv[1].type) && cv[0].type==VT_EXPRTREE && ((ExprTree*)cv[0].exprt)->op==OP_MUL && isNumeric((ValueType)((ExprTree*)cv[0].exprt)->operands[1].type)
			&& Expr::calc(OP_MUL,((ExprTree*)cv[0].exprt)->operands[1],&cv[1],2,0,ses)==RC_OK) {res=cv[0]; return RC_OK;}
		break;
	case OP_CONCAT:
		if (fFold || nOperands!=2) break;
		if (cv[0].type==VT_EXPRTREE && ((ExprTree*)cv[0].exprt)->op==OP_CONCAT) {
			if (isSimpleConst((ValueType)((ExprTree*)cv[0].exprt)->operands[((ExprTree*)cv[0].exprt)->nops-1].type) && isSimpleConst((ValueType)cv[1].type)
				&& Expr::calc(OP_CONCAT,((ExprTree*)cv[0].exprt)->operands[((ExprTree*)cv[0].exprt)->nops-1],&cv[1],2,0,ses)==RC_OK) {res=cv[0]; freeV(*(Value*)&cv[1]); return RC_OK;}
			if (((ExprTree*)cv[0].exprt)->nops<255) {
				ExprTree *et=(ExprTree*)ses->realloc(cv[0].exprt,sizeof(ExprTree)+sizeof(Value)*((ExprTree*)cv[0].exprt)->nops); if (et==NULL) {((Value*)cv)->exprt=NULL; return RC_NORESOURCES;}
				et->operands[et->nops++]=cv[1]; res=cv[0]; res.exprt=et; return RC_OK;
			}
		}
		break;
	case OP_EQ: case OP_NE: case OP_LT: case OP_LE: case OP_GT: case OP_GE:
		if ((flags&NOT_BOOLEAN_OP)!=0) {op=ExprTree::notOp[op-OP_EQ]; flags&=~NOT_BOOLEAN_OP;}
	case OP_IN: case OP_CONTAINS: case OP_BEGINS: case OP_ENDS: case OP_IS_A: case OP_SIMILAR:
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
		if (fFold && (flags&CASE_INSENSITIVE_OP)!=0) flgFold|=CND_NCASE; break;
	case OP_RSHIFT:
		if (fFold && (flags&UNSIGNED_OP)!=0) flgFold|=CND_UNS; break;
	case OP_CALL:
		if (cv[0].type!=VT_VARREF && cv[0].type!=VT_STMT && cv[0].type!=VT_EXPR) return RC_TYPE;
		fFold=false; break;
	case OP_COUNT:
		if (nOperands==1 && cv[0].type==VT_ANY) {fFold=false; break;}
	case OP_EXISTS:
		if (cv[0].type!=VT_EXPRTREE && cv[0].type!=VT_VARREF && (cv[0].type!=VT_STMT||op!=OP_EXISTS)) {
			if (op==OP_EXISTS) res.set(((flags&NOT_BOOLEAN_OP)!=0)==(cv[0].type==VT_ANY));
			else res.set(unsigned(cv[0].type==VT_ANY?0u:cv[0].type==VT_ARRAY?cv[0].length:cv[0].type==VT_COLLECTION?cv[0].nav->count():1u));
			freeV(*(Value*)&cv[0]); return RC_OK;
		}
		fFold=false; break;
	case OP_PATH:
		fFold=false;
		if (cv[0].type!=VT_EXPRTREE && cv[0].type!=VT_VARREF && cv[0].type!=VT_REF && cv[0].type!=VT_REFID && cv[0].type!=VT_ARRAY 
			&& cv[0].type!=VT_COLLECTION || cv[1].type!=VT_EXPRTREE && cv[1].type!=VT_VARREF && cv[1].type!=VT_URIID && cv[1].type!=VT_ANY && cv[1].type!=VT_ARRAY) return RC_TYPE;
		if (nOperands>2) {
			if (cv[2].type!=VT_INT && cv[2].type!=VT_UINT && (cv[2].type!=VT_EXPRTREE || !isBool(((ExprTree*)cv[2].exprt)->op))) return RC_TYPE;
			if (nOperands>3 && cv[3].type!=VT_UINT && cv[3].type!=VT_EXPRTREE) return RC_TYPE;
		}
		break;
	case OP_ISLOCAL:
		if (cv->type!=VT_EXPRTREE && cv->type!=VT_REF && cv->type!=VT_REFID && cv->type!=VT_VARREF) return RC_TYPE;
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
				pv[v.length++]=((ExprTree*)cv[1].exprt)->operands[1]; setHT(((ExprTree*)cv[1].exprt)->operands[1]);
				freeV((Value&)cv[1]); res.set(cv[0].exprt); return RC_OK;
			}
			break;
		case OP_EQ:
			if (((ExprTree*)cv[0].exprt)->operands[1].type!=VT_EXPRTREE) {
				Value &v=((ExprTree*)cv[0].exprt)->operands[1];
				if ((pv=(Value*)ses->malloc(2*sizeof(Value)))!=NULL) {
					pv[0]=v; pv[1]=((ExprTree*)cv[1].exprt)->operands[1]; setHT(((ExprTree*)cv[1].exprt)->operands[1]);
					v.type=VT_ARRAY; v.varray=pv; v.length=2; setHT(v,SES_HEAP);
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
			if ((flags&COPY_VALUES_OP)==0) setHT(cv[0]);
			else if (res.type!=VT_EXPRTREE && (rc=copyV(res,res,ses))!=RC_OK) return rc;
			if ((flags&NOT_BOOLEAN_OP)==0) rc=cvNot(res);
		}
		return rc;
	}
	if (fFold) {
		res=cv[0];
		if ((flags&COPY_VALUES_OP)==0) setHT(cv[0]);
		else if (res.type!=VT_EXPRTREE && (rc=copyV(res,res,ses))!=RC_OK) return rc;
		rc=(SInCtx::opDscr[op].flags&_A)==0?Expr::calc(op,res,&cv[1],nOperands,flgFold,ses):
			Expr::calcAgg(op,res,&cv[1],nOperands,((flags&DISTINCT_OP)!=0?CND_DISTINCT:0)|flgFold,ses);
		if (isBool(op) && (rc==RC_TRUE||rc==RC_FALSE)) {res.set(rc==RC_TRUE); rc=RC_OK;}
		if (rc==RC_OK && (flags&COPY_VALUES_OP)==0) for (unsigned i=1; i<nOperands; i++) freeV(*(Value*)&cv[i]);
	} else {
		ExprTree *pe=new(nOperands,ses) ExprTree(op,(ushort)nOperands,vrefs,flags,cv,ses);
		if (pe!=NULL) {res.set(pe); res.flags=SES_HEAP;} else rc=RC_NORESOURCES;
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
		case OP_SIMILAR: case OP_ISLOCAL: ex->flags^=NOT_BOOLEAN_OP; break;
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
	unsigned nExpr=0; unsigned cnt=nvals; RC rc;
	Value *pv=new(ma) Value[cnt]; if (pv==NULL) return RC_NORESOURCES;
	ElementID prefix=ctx!=NULL?ctx->getPrefix():0; const HEAP_TYPE ht=ma!=NULL?ma->getAType():NO_HEAP;
	memcpy(pv,vals,nvals*sizeof(Value));
	for (unsigned start=0;;) {
		unsigned extra=0,s0=~0u;
		for (unsigned i=start; i<cnt; i++) {
			switch (pv[i].type) {
			case VT_ARRAY: extra+=pv[i].length-1; if (s0==~0u) s0=i; if (pv[i].fcalc!=0) nExpr++; continue;
			case VT_COLLECTION: extra+=pv[i].nav->count()-1; if (s0==~0u) s0=i; continue;
			case VT_EXPRTREE: case VT_VARREF: case VT_CURRENT: nExpr++; break;
			case VT_STMT: case VT_EXPR: case VT_STRUCT: if (pv[i].fcalc!=0) nExpr++;
			default: pv[i].eid=prefix+i; break;
			}
			if (ma!=NULL && (pv[i].flags&HEAP_TYPE_MASK)!=ht && (rc=copyV(pv[i],pv[i],ma))!=RC_OK) return rc;	// cleanup
		}
		if (s0==~0u) break;
		if ((pv=(Value*)ma->realloc(pv,(cnt+extra)*sizeof(Value)))==NULL) return RC_NORESOURCES;
		for (unsigned i=start=s0; i<cnt; i++) if (pv[i].type==VT_ARRAY) {
			const Value *cv=pv[i].varray; unsigned l=pv[i].length;
			if (i+1!=cnt && l!=1) memmove(&pv[i+l],&pv[i+1],(cnt-i-1)*sizeof(Value));
			memcpy(&pv[i],cv,l*sizeof(Value)); cnt+=l-1; i+=l-1; if (ma!=NULL) ma->free((void*)cv);
		} else if (pv[i].type==VT_COLLECTION) {
			INav *nav=pv[i].nav; unsigned l=nav->count(),j=0;
			if (i+1!=cnt && l!=1) memmove(&pv[i+l],&pv[i+1],(cnt-i-1)*sizeof(Value));
			for (const Value *cv=nav->navigate(GO_FIRST); cv!=NULL; cv=nav->navigate(GO_NEXT),++j)
				if ((rc=copyV(*cv,pv[i+j],ma))!=RC_OK) return rc;	// cleanup
			cnt+=l-1; i+=l-1; nav->destroy();
		}
	}
	res.set(pv,cnt,nExpr!=0?1:0); res.flags=ma->getAType();
	return RC_OK;
}

static int __cdecl cmpValues(const void *v1, const void *v2)
{
	return cmp3(((const Value*)v1)->property,((const Value*)v2)->property);
}

RC ExprTree::normalizeStruct(Value *vals,unsigned nvals,Value &res,MemAlloc *ma)
{
	unsigned nExpr=0; RC rc; const HEAP_TYPE ht=ma!=NULL?ma->getAType():NO_HEAP;
	Value *pv=new(ma) Value[nvals]; if (pv==NULL) return RC_NORESOURCES;
	memcpy(pv,vals,nvals*sizeof(Value)); if (nvals>1) qsort(pv,nvals,sizeof(Value),cmpValues);
	for (unsigned i=0; i<nvals; i++) {
		if (i!=0 && pv[i-1].property==pv[i].property) {freeV(pv,nvals,ma); return RC_ALREADYEXISTS;}
		switch (pv[i].type) {
		default: break;
		case VT_STMT: case VT_EXPR: case VT_STRUCT: case VT_ARRAY: if (pv->fcalc==0) break;
		case VT_EXPRTREE: case VT_VARREF: case VT_CURRENT: nExpr++; break;
		}
		if (ma!=NULL && (pv[i].flags&HEAP_TYPE_MASK)!=ht && (rc=copyV(pv[i],pv[i],ma))!=RC_OK) return rc;	// cleanup
	}
	res.setStruct(pv,nvals,nExpr!=0?1:0); res.flags=ma->getAType();
	return RC_OK;
}

static int __cdecl cmpMapElts(const void *v1, const void *v2)
{
	return AfyKernel::cmp(((const MapElt*)v1)->key,((const MapElt*)v2)->key,CND_SORT,NULL);
}

RC ExprTree::normalizeMap(Value *vals,unsigned nvals,Value &res,MemAlloc *ma)
{
	if (nvals==0 || (nvals&1)!=0) return RC_INVPARAM;
	unsigned nExpr=0; RC rc; const HEAP_TYPE ht=ma!=NULL?ma->getAType():NO_HEAP;
	Value *pv=new(ma) Value[nvals]; if (pv==NULL) return RC_NORESOURCES;
	memcpy(pv,vals,nvals*sizeof(Value)); if (nvals>1) qsort(pv,nvals/2,sizeof(MapElt),cmpMapElts);
	for (unsigned i=0; i<nvals; i+=2) {
//		if (i!=0 && pv[i-2].property==pv[i].property) {freeV(pv,nvals,ma); return RC_ALREADYEXISTS;}
		switch (pv[i].type) {
		default: break;
		case VT_STMT: case VT_EXPR: case VT_STRUCT: case VT_ARRAY: if (pv->fcalc==0) break;
		case VT_EXPRTREE: case VT_VARREF: case VT_CURRENT: nExpr++; break;
		}
		if (ma!=NULL) {
			if ((pv[i].flags&HEAP_TYPE_MASK)!=ht && (rc=copyV(pv[i],pv[i],ma))!=RC_OK) return rc;	// cleanup
			if ((pv[i+1].flags&HEAP_TYPE_MASK)!=ht && (rc=copyV(pv[i+1],pv[i+1],ma))!=RC_OK) return rc;	// cleanup
		}
	}
	MemMap *mm=new(ma) MemMap((MapElt*)pv,nvals/2,ma);
	if (mm==NULL) {
		if (ma!=NULL) {freeV(pv,nvals,ma); ma->free(pv);}
		return RC_NORESOURCES;
	}
	res.set(mm); res.flags=ma->getAType(); return RC_OK;	// nExpr???
}

RC ExprTree::substitute(Value& v,const Value *params,unsigned nParams,MemAlloc *ma)
{
	if (v.type==VT_EXPRTREE) {
		ExprTree *et=(ExprTree*)v.exprt; RC rc;
		for (unsigned i=0; i<et->nops; i++) if ((rc=substitute(*(Value*)&et->operands[i],params,nParams,ma))!=RC_OK) return rc;
	} else if (v.type==VT_VARREF && (v.refV.flags&VAR_TYPE_MASK)==VAR_PARAM && v.refV.refN<nParams) {
		RC rc=copyV(params[v.refV.refN],v,ma); if (rc!=RC_OK) return rc;
	}
	return RC_OK;
}

ExprTree::ExprTree(ExprOp o,ushort no,ushort vr,unsigned f,const Value *ops,MemAlloc *m) 
	: ma(m),op(o),nops(no),vrefs(vr),flags((ushort)f)
{
	memcpy((Value*)operands,ops,no*sizeof(Value));
	if ((f&COPY_VALUES_OP)!=0) for (unsigned i=0; i<no; i++) {
		Value& oper=*const_cast<Value*>(&operands[i]); RC rc;
		if (oper.type==VT_EXPRTREE) {assert(((ExprTree*)ops[i].exprt)->op!=OP_CON); oper.flags=SES_HEAP;}
		else  {setHT(oper); if ((rc=copyV(oper,oper,m))!=RC_OK) throw rc;}
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
		if ((operands[i].refV.flags&VAR_TYPE_MASK)==0 && operands[i].refV.refN==from) operands[i].refV.refN=to;
		break;
	case VT_EXPRTREE:
		vrefs=((ExprTree*)operands[i].exprt)->vrefs;
		if (vrefs==MANY_VREFS || vrefs!=NO_VREFS && (byte(vrefs)==from || byte(vrefs>>8)==from))
			((ExprTree*)operands[i].exprt)->mapVRefs(from,to);
		break;
	}
}

static int __cdecl cmpProps(const void *v1, const void *v2)
{
	return cmp3(*(const PropertyID*)v1,*(const PropertyID*)v2);
}

RC ExprTree::toPathSeg(PathSeg& ps,MemAlloc *ma) const
{
	memset(&ps,0,sizeof(PathSeg)); ps.pid=PROP_SPEC_ANY; ps.eid=STORE_COLLECTION_ID; ps.cid=STORE_INVALID_CLASSID; ps.fLast=(flags&FILTER_LAST_OP)!=0;
	if (operands[1].type==VT_URIID && operands[1].uid!=PROP_SPEC_ANY) {ps.pid=operands[1].uid; ps.nPids=1;}
	else if (operands[1].type==VT_ARRAY) {
		if ((ps.pids=new(ma) PropertyID[operands[1].length])==NULL) return RC_NORESOURCES;
		for (unsigned i=ps.nPids=0; i<operands[1].length; i++)
			if (operands[1].varray[i].type==VT_URIID && (ps.pids[ps.nPids]=operands[1].varray[i].uid)!=PROP_SPEC_ANY) ps.nPids++;
		if (ps.nPids==0) {ma->free(ps.pids); ps.pid=PROP_SPEC_ANY;} else if (ps.nPids==1) {PropertyID pid=ps.pids[0]; ma->free(ps.pids); ps.pid=pid;}
		else qsort(ps.pids,ps.nPids,sizeof(PropertyID),cmpProps);
	} else if (operands[1].type==VT_ANY || operands[1].type==VT_URIID) {ps.pid=PROP_SPEC_ANY; ps.nPids=0;} else return RC_TYPE;
	switch (flags&(QUEST_PATH_OP|PLUS_PATH_OP|STAR_PATH_OP)) {
	default: ps.rmin=ps.rmax=1; break;
	case QUEST_PATH_OP: ps.rmin=0; ps.rmax=1; break;
	case PLUS_PATH_OP: ps.rmin=1; ps.rmax=uint16_t(~0u); break;
	case STAR_PATH_OP: ps.rmin=0; ps.rmax=uint16_t(~0u); break;
	}
	if (nops>2) {
		switch (operands[2].type) {
		case VT_ERROR: break;
		case VT_INT: case VT_UINT: ps.eid=operands[2].ui; break;
		case VT_EXPRTREE:
			if (isBool(((ExprTree*)operands[2].exprt)->op)) {
				// classID ?
				Expr *exp=NULL; RC rc;
				if ((rc=Expr::compile((ExprTree*)operands[2].exprt,exp,ma,true))!=RC_OK) return rc;
				ps.filter=exp; break;
			}
		default: return RC_TYPE;
		}
		if (nops>3) {
			if (operands[3].type!=VT_UINT) return RC_TYPE;
			ps.rmin=operands[3].ui&0xFFFF; ps.rmax=operands[3].ui>>16;
		}
	}
	return RC_OK;
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
		Expr *expr=NULL; Expr::compile(this,expr,ses,false); return expr;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IExprTree::compile()\n");}
	return NULL;
}

IExprTree *ExprTree::clone() const
{
	try {
		Session *ses=Session::getSession();
		if (ses==NULL||ses->getStore()->inShutdown()) return NULL;
		ExprTree *pe=new(nops,ses) ExprTree(op,nops,vrefs,flags|COPY_VALUES_OP,operands,ses);
		if (pe!=NULL) for (unsigned i=0; i<unsigned(nops); i++)
			if (pe->operands[i].type==VT_EXPRTREE) pe->operands[i].exprt=pe->operands[i].exprt->clone();
		return pe;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IExprTree::clone()\n");}
	return NULL;
}

void ExprTree::destroy()
{
	try {delete this;} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IExprTree::destory()\n");}
}

IExprTree *Session::expr(ExprOp op,unsigned nOperands,const Value *operands,unsigned flags)
{
	try {
		if (ctx->inShutdown()) return NULL;
		Value v; RC rc=ExprTree::node(v,this,op,nOperands,operands,flags|COPY_VALUES_OP);
		if (rc==RC_OK && (rc=ExprTree::forceExpr(v,this,true))!=RC_OK) freeV(v);
		return rc==RC_OK?v.exprt:(IExprTree*)0;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IStmt::expr()\n");}
	return NULL;
}

void **Expr::extnTab = NULL;
int	Expr::nExtns=0;

RC Expr::registerExtn(void *itf,uint16_t& langID)
{
	for (int i=0; i<nExtns; i++) if (extnTab[i]==itf) {langID=(uint16_t)i; return RC_OK;}
	if ((extnTab=(void**)::realloc(extnTab,(nExtns+1)*sizeof(void*)))==NULL) return RC_NORESOURCES;
	langID=(uint16_t)nExtns; extnTab[nExtns++]=itf; return RC_OK;
}

IExpr *Session::createExtExpr(uint16_t langID,const byte *body,uint32_t lBody,uint16_t flags)
{
	try {
		if (langID>=Expr::nExtns || body==NULL || lBody==0 || (flags&EXPR_EXTN)!=0 || ctx->inShutdown()) return NULL;
		Expr *exp=NULL; return Expr::create(langID,body,lBody,flags,exp,this)==RC_OK?exp:NULL;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::createExtExpr()\n");}
	return NULL;
}
