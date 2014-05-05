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

Written by Mark Venguerov 2014

**************************************************************************************/

#include "expr.h"
#include <math.h>

using namespace AfyKernel;

template<typename T> T round(T x)
{
   return (T)(x>=(T)0.0?floor(x+(T)0.5):ceil(x-(T)0.5));
}

RC Expr::laAddSub(ExprOp op,Value& lhs,const Value& rhs,unsigned flags,const EvalCtx& ctx)
{
	RC rc; assert(lhs.type==VT_ARRAY || rhs.type==VT_ARRAY);
	if (lhs.type!=rhs.type || lhs.fa.xdim!=rhs.fa.xdim || lhs.fa.ydim!=rhs.fa.ydim) return RC_TYPE;
	if (lhs.fa.type!=rhs.fa.type || !isNumeric((ValueType)lhs.fa.type)) {
		//numOpConv
		return RC_TYPE;	//tmp
	}
	// use fa.start!!!
	if ((lhs.flags&HEAP_TYPE_MASK)<SES_HEAP && (rc=copyV(lhs,lhs,ctx.ma))!=RC_OK) return rc;
	unsigned i,n=min(lhs.length,rhs.length); int sgn=op==OP_PLUS?1:-1;
	switch (lhs.fa.type) {
	default: assert(0);
	case VT_INT: for (i=0; i<n; i++) lhs.fa.i[i]+=rhs.fa.i[i]*sgn; break;
	case VT_UINT: for (i=0; i<n; i++) lhs.fa.ui[i]+=rhs.fa.ui[i]*sgn; break;
	case VT_INT64: for (i=0; i<n; i++) lhs.fa.i64[i]+=rhs.fa.i64[i]*sgn; break;
	case VT_UINT64: for (i=0; i<n; i++) lhs.fa.ui64[i]+=rhs.fa.ui64[i]*sgn; break;
	case VT_FLOAT: for (i=0; i<n; i++) lhs.fa.f[i]+=rhs.fa.f[i]*sgn; break;		// units???
	case VT_DOUBLE: for (i=0; i<n; i++) lhs.fa.d[i]+=rhs.fa.d[i]*sgn; break;	// units???
	}
	return RC_OK;
}

template<typename T> void mmul(T *res,const T *lhs,const T *rhs,unsigned x,unsigned y,unsigned n)
{
	for (unsigned i=0; i<y; i++) for (unsigned j=0; j<x; j++)
		{T &r=res[i*x+j]; for (unsigned k=0; k<n; k++) r+=lhs[i*n+k]*rhs[k*x+j];}
}

RC Expr::laMul(Value& lhs,const Value& rhs,unsigned flags,const EvalCtx& ctx)
{
	assert(lhs.type==VT_ARRAY || rhs.type==VT_ARRAY); Value r=rhs; RC rc;
	ValueType lty=ValueType(lhs.type==VT_ARRAY?lhs.fa.type:lhs.type),rty=ValueType(r.type==VT_ARRAY?r.fa.type:r.type);
	if (lty!=rty || !isNumeric(lty)) {
		//numOpConv
		return RC_TYPE;	//tmp
	}
	if (lhs.type!=VT_ARRAY) {r=lhs; if ((rc=copyV(rhs,lhs,ctx.ma))!=RC_OK) {freeV(r); return rc;}}
	if (r.type!=VT_ARRAY) {
		// multiplication of a vector or a matrix by a scalar
		if ((lhs.flags&HEAP_TYPE_MASK)<SES_HEAP && (rc=copyV(lhs,lhs,ctx.ma))!=RC_OK) return rc;
		unsigned i=0,n=lhs.length; assert(lhs.fa.type==r.type);
		switch (r.type) {
		default: assert(0);
		case VT_INT: for (; i<n; i++) lhs.fa.i[i]*=r.i; break;
		case VT_UINT: for (; i<n; i++) lhs.fa.ui[i]*=r.ui; break;
		case VT_INT64: for (; i<n; i++) lhs.fa.i64[i]*=r.i64; break;
		case VT_UINT64: for (; i<n; i++) lhs.fa.ui64[i]*=r.ui64; break;
		case VT_FLOAT: for (; i<n; i++) lhs.fa.f[i]*=r.f; break;		// check units
		case VT_DOUBLE: for (; i<n; i++) lhs.fa.d[i]*=r.d; break;	// check units
		}
	} else if (lhs.fa.ydim==1 && (r.fa.xdim==1||r.fa.ydim==1)) {
		// scalar product	// use fa.start!!!
		memset(&r,0,sizeof(Value)); r.eid=STORE_COLLECTION_ID; r.property=STORE_INVALID_URIID;
		r.length=typeInfo[r.type=lty].length; if (lty==VT_FLOAT||lty==VT_DOUBLE) r.qval.units=lhs.fa.units;
		unsigned i=0,n=min(lhs.length,rhs.length);
		switch (lty) {
		default: assert(0);
		case VT_INT: for (; i<n; i++) r.i+=lhs.fa.i[i]*rhs.fa.i[i]; break;
		case VT_UINT: for (; i<n; i++) r.ui+=lhs.fa.ui[i]*rhs.fa.ui[i]; break;
		case VT_INT64: for (; i<n; i++) r.i64+=lhs.fa.i64[i]*rhs.fa.i64[i]; break;
		case VT_UINT64: for (; i<n; i++) r.ui64+=lhs.fa.ui64[i]*rhs.fa.ui64[i]; break;
		case VT_FLOAT: for (; i<n; i++) r.f+=lhs.fa.f[i]*rhs.fa.f[i]; break;
		case VT_DOUBLE: for (; i<n; i++) r.d+=lhs.fa.d[i]*rhs.fa.d[i]; break;
		}
		freeV(lhs); lhs=r;
	} else if (lhs.fa.xdim==r.fa.ydim) {
		size_t l=typeInfo[lty].length*lhs.fa.ydim*rhs.fa.xdim;
		void *p=ctx.ma->malloc(l); if (p==NULL) return RC_NOMEM;
		memset(p,0,l); r.setArray(p,lhs.fa.ydim*rhs.fa.xdim,rhs.fa.xdim,lhs.fa.ydim,lty);
		switch (lty) {
		default: assert(0);
		case VT_INT: mmul(r.fa.i,lhs.fa.i,rhs.fa.i,rhs.fa.xdim,lhs.fa.ydim,lhs.fa.xdim); break;
		case VT_UINT: mmul(r.fa.ui,lhs.fa.ui,rhs.fa.ui,rhs.fa.xdim,lhs.fa.ydim,lhs.fa.xdim); break;
		case VT_INT64: mmul(r.fa.i64,lhs.fa.i64,rhs.fa.i64,rhs.fa.xdim,lhs.fa.ydim,lhs.fa.xdim); break;
		case VT_UINT64: mmul(r.fa.ui64,lhs.fa.ui64,rhs.fa.ui64,rhs.fa.xdim,lhs.fa.ydim,lhs.fa.xdim); break;
		case VT_FLOAT: mmul(r.fa.f,lhs.fa.f,rhs.fa.f,rhs.fa.xdim,lhs.fa.ydim,lhs.fa.xdim); break;
		case VT_DOUBLE: mmul(r.fa.d,lhs.fa.d,rhs.fa.d,rhs.fa.xdim,lhs.fa.ydim,lhs.fa.xdim); break;
		}
		freeV(lhs); lhs=r;
	} else return RC_TYPE;
	return RC_OK;
}

RC Expr::laDiv(Value& lhs,const Value& rhs,unsigned flags,const EvalCtx& ctx)
{
	assert(lhs.type==VT_ARRAY || rhs.type==VT_ARRAY); RC rc;
	if (rhs.type!=VT_ARRAY) {
		//...
		rc=RC_TYPE;
	} else {
		if (lhs.type!=VT_ARRAY || lhs.fa.xdim!=rhs.fa.xdim || lhs.fa.ydim!=1 || rhs.fa.xdim!=rhs.fa.ydim ||
			lhs.length<lhs.fa.xdim || !isNumeric((ValueType)lhs.fa.type) || !isNumeric((ValueType)rhs.fa.type)) return RC_TYPE;
		if (lhs.fa.type!=VT_DOUBLE) {if ((rc=laConvToDouble(lhs,ctx.ma))!=RC_OK) return rc;}
		else if ((lhs.flags&HEAP_TYPE_MASK)<SES_HEAP && (rc=copyV(lhs,lhs,ctx.ma))!=RC_OK) return rc;
		Value r=rhs; setHT(r); rc=RC_OK; double det;
		if (r.fa.type!=VT_DOUBLE) {if ((rc=laConvToDouble(r,ctx.ma))!=RC_OK) return rc;} else if ((rc=copyV(r,r,ctx.ma))!=RC_OK) return rc;
		bool fDel=lhs.fa.xdim>64; size_t l=lhs.fa.xdim*(sizeof(unsigned)+sizeof(double)); unsigned *piv=fDel?(unsigned*)ctx.ma->malloc(l):(unsigned*)alloca(l);
		if (piv==NULL) rc=RC_NOMEM;
		else {
			if ((rc=laLUdec(r,piv,det))==RC_OK && (rc=laLUslv(lhs.fa.d,r,(double*)(piv+lhs.fa.xdim),piv))==RC_OK)
				memcpy(lhs.fa.d,piv+lhs.fa.xdim,lhs.fa.xdim*sizeof(double));
			if (fDel) ctx.ma->free(piv);
		}
		freeV(r);
	}
	return rc;
}

RC Expr::laNorm(ExprOp op,Value& arg,const Value *moreArgs,int nargs,unsigned flags,const EvalCtx& ctx)
{
	assert(op==OP_NORM); if (arg.type!=VT_ARRAY) return RC_TYPE; RC rc; long ll; uint32_t i; int p;
	if (arg.type!=VT_ARRAY || !isNumeric((ValueType)arg.fa.type) || arg.fa.xdim!=1&&arg.fa.ydim!=1) return RC_TYPE;
	if (nargs==1) p=2; else if ((rc=getI(*moreArgs,ll,ctx))!=RC_OK) return rc; else if (ll<=0) return RC_INVPARAM; else p=(uint32_t)ll;
	if ((unsigned)p!=~0u) {
		double sum=0.;
		switch (arg.fa.type) {
		default: assert(0);
		case VT_INT: for (i=0; i<arg.length; i++) {double v=abs(arg.fa.i[i]); sum+=p==1?v:p==2?v*v:pow(v,p);} break;
		case VT_UINT: for (i=0; i<arg.length; i++) {double v=arg.fa.ui[i]; sum+=p==1?v:p==2?v*v:pow(v,p);} break;
		case VT_INT64: for (i=0; i<arg.length; i++) {double v=(double)abs(arg.fa.i64[i]); sum+=p==1?v:p==2?v*v:pow(v,p);} break;
		case VT_UINT64: for (i=0; i<arg.length; i++) {double v=(double)arg.fa.ui64[i]; sum+=p==1?v:p==2?v*v:pow(v,p);} break;
		case VT_FLOAT: for (i=0; i<arg.length; i++) {double v=fabs(arg.fa.f[i]); sum+=p==1?v:p==2?v*v:pow(v,p);} break;
		case VT_DOUBLE: for (i=0; i<arg.length; i++) {double v=fabs(arg.fa.d[i]); sum+=p==1?v:p==2?v*v:pow(v,p);} break;
		}
		freeV(arg); arg.set(p==1?sum:p==2?sqrt(sum):pow(sum,1./(double)p));
	} else if ((rc=calcAgg(OP_MAX,arg,NULL,1,flags,ctx))!=RC_OK) return rc;
	return RC_OK;
}

RC Expr::laTrace(ExprOp op,Value& arg,const Value *moreArgs,int nargs,unsigned flags,const EvalCtx& ctx)
{
	assert(op==OP_TRACE); if (arg.type!=VT_ARRAY) return RC_TYPE; uint32_t i; Value val;
	if (arg.fa.data==NULL||arg.fa.xdim!=arg.fa.ydim||!isNumeric((ValueType)arg.fa.type)) return RC_TYPE;
	memset(&val,0,sizeof(Value)); val.eid=STORE_COLLECTION_ID; val.property=STORE_INVALID_URIID;
	val.length=typeInfo[val.type=arg.fa.type].length; if (val.type==VT_FLOAT||val.type==VT_DOUBLE) val.qval.units=arg.fa.units;
	for (i=0; i<(uint32_t)arg.fa.ydim; i++) switch (val.type) {
	default: assert(0);
	case VT_INT: val.i+=arg.fa.i[i*(arg.fa.xdim+1)]; break;
	case VT_UINT: val.ui+=arg.fa.ui[i*(arg.fa.xdim+1)]; break;
	case VT_INT64: val.i64+=arg.fa.i64[i*(arg.fa.xdim+1)]; break;
	case VT_UINT64: val.ui64+=arg.fa.ui64[i*(arg.fa.xdim+1)]; break;
	case VT_FLOAT: val.f+=arg.fa.f[i*(arg.fa.xdim+1)]; break;
	case VT_DOUBLE: val.d+=arg.fa.d[i*(arg.fa.xdim+1)]; break;
	}
	freeV(arg); arg=val; return RC_OK;
}

RC Expr::laTrans(ExprOp op,Value& arg,const Value *moreArgs,int nargs,unsigned flags,const EvalCtx& ctx)
{
	assert(op==OP_TRANSPOSE); if (arg.type!=VT_ARRAY) return RC_TYPE; RC rc;
	if (arg.length!=uint32_t(arg.fa.xdim*arg.fa.ydim)) return RC_INVPARAM;	//???
	size_t l=arg.fa.type==VT_STRUCT?slength(arg):typeInfo[arg.fa.type].length;
	if ((arg.fa.xdim-1)*(arg.fa.ydim-1)==0) {uint16_t t=arg.fa.xdim; arg.fa.xdim=arg.fa.ydim; arg.fa.ydim=t;}		// if fa.start!=0 ???
	else if (arg.fa.xdim==arg.fa.ydim) {
		if ((arg.flags&HEAP_TYPE_MASK)<SES_HEAP && (rc=copyV(arg,arg,ctx.ma))!=RC_OK) return rc;
		for (unsigned i=0; i<(unsigned)arg.fa.xdim; i++) for (unsigned j=0; j<i; j++) {
			if (l==sizeof(uint32_t)) {uint32_t tmp=arg.fa.ui[i*arg.fa.xdim+j]; arg.fa.ui[i*arg.fa.xdim+j]=arg.fa.ui[j*arg.fa.xdim+i]; arg.fa.ui[j*arg.fa.xdim+i]=tmp;}
			else if (l==sizeof(uint64_t)) {uint64_t tmp=arg.fa.ui64[i*arg.fa.xdim+j]; arg.fa.ui64[i*arg.fa.xdim+j]=arg.fa.ui64[j*arg.fa.xdim+i]; arg.fa.ui64[j*arg.fa.xdim+i]=tmp;}
			else return RC_INTERNAL;	// tmp
		}
	} else {
		Value val=arg; if ((val.fa.data=ctx.ma->malloc(l*arg.length))==NULL) return RC_NOMEM;
		for (unsigned i=0; i<arg.fa.ydim; i++) for (unsigned j=0; j<arg.fa.xdim; j++) {
			if (l==sizeof(uint32_t)) val.fa.ui[j*arg.fa.ydim+i]=arg.fa.ui[i*arg.fa.xdim+j];
			else if (l==sizeof(uint64_t)) val.fa.ui64[j*arg.fa.ydim+i]=arg.fa.ui64[i*arg.fa.xdim+j];
			else return RC_INTERNAL;	// tmp
		}
		val.fa.ydim=arg.fa.xdim; val.fa.xdim=arg.fa.ydim; setHT(val,ctx.ma->getAType()); freeV(arg); arg=val;
	}
	return RC_OK;
}

RC Expr::laDet(ExprOp op,Value& arg,const Value *moreArgs,int nargs,unsigned flags,const EvalCtx& ctx)
{
	assert(op==OP_DET); RC rc; Value val; const ValueType ty=(ValueType)arg.fa.type; const unsigned n=arg.fa.xdim;
	if (arg.type!=VT_ARRAY || arg.fa.ydim!=n || arg.length!=n*n || !isNumeric(ty)) return RC_TYPE;
	memset(&val,0,sizeof(Value)); val.eid=STORE_COLLECTION_ID; val.property=STORE_INVALID_URIID; val.length=typeInfo[val.type=ty].length;
	if (n<=2) switch (ty) {
	default: assert(0);
	case VT_INT: val.i=n==1?arg.fa.i[0]:int32_t((int64_t)arg.fa.i[0]*arg.fa.i[3]-(int64_t)arg.fa.i[1]*arg.fa.i[2]); break;
	case VT_UINT: val.ui=n==1?arg.fa.ui[0]:uint32_t((uint64_t)arg.fa.ui[0]*arg.fa.ui[3]-(uint64_t)arg.fa.ui[1]*arg.fa.ui[2]); break;
	case VT_INT64: val.i64=n==1?arg.fa.i64[0]:arg.fa.i64[0]*arg.fa.i64[3]-arg.fa.i64[1]*arg.fa.i64[2]; break;
	case VT_UINT64: val.ui64=n==1?arg.fa.ui64[0]:arg.fa.ui64[0]*arg.fa.ui64[3]-arg.fa.ui64[1]*arg.fa.ui64[2]; break;
	case VT_FLOAT: val.f=n==1?arg.fa.f[0]:float(arg.fa.f[0]*arg.fa.f[3]-arg.fa.f[1]*arg.fa.f[2]); break;
	case VT_DOUBLE: val.d=n==1?arg.fa.d[0]:arg.fa.d[0]*arg.fa.d[3]-arg.fa.d[1]*arg.fa.d[2]; break;
	} else {
		if (ty!=VT_DOUBLE) {if ((rc=laConvToDouble(arg,ctx.ma))!=RC_OK) return rc;}
		else if ((arg.flags&HEAP_TYPE_MASK)<SES_HEAP && (rc=copyV(arg,arg,ctx.ma))!=RC_OK) return rc;
		bool fDel=n>64; unsigned *piv=fDel?new(ctx.ma) unsigned[n]:(unsigned*)alloca(n*sizeof(unsigned));
		if (piv==NULL) return RC_NOMEM; if (laLUdec(arg,piv,val.d)!=RC_OK) val.d=0.0; if (fDel) ctx.ma->free(piv);
		switch (ty) {
		default: assert(0);
		case VT_INT: val.i=(int)round(val.d); break;
		case VT_UINT: val.ui=(unsigned)round(val.d); break;
		case VT_INT64: val.i64=(int64_t)round(val.d); break;
		case VT_UINT64: val.ui64=(uint64_t)round(val.d); break;
		case VT_FLOAT: val.f=(float)val.d; break;
		case VT_DOUBLE: break;
		}
	}
	freeV(arg); arg=val; return RC_OK;
}

RC Expr::laInv(ExprOp op,Value& arg,const Value *moreArgs,int nargs,unsigned flags,const EvalCtx& ctx)
{
	assert(op==OP_INV); RC rc=RC_OK; Value val; double det; unsigned *piv=NULL; bool fDel; size_t l;
	if (arg.type!=VT_ARRAY || arg.fa.xdim!=arg.fa.ydim || arg.length!=arg.fa.xdim*arg.fa.ydim || !isNumeric((ValueType)arg.fa.type)) return RC_TYPE;
	if (arg.fa.type!=VT_DOUBLE) {if ((rc=laConvToDouble(arg,ctx.ma))!=RC_OK) return rc;}
	else if ((arg.flags&HEAP_TYPE_MASK)<SES_HEAP && (rc=copyV(arg,arg,ctx.ma))!=RC_OK) return rc;
	switch (arg.fa.xdim) {
	case 0: assert(0);
	case 1: 
		if (arg.fa.d[0]==0.) return RC_DIV0; arg.fa.d[0]=1./arg.fa.d[0]; break;		// check abs diff with precision
	case 2:
		if ((det=arg.fa.d[0]*arg.fa.d[3]-arg.fa.d[1]*arg.fa.d[2])==0.) rc=RC_DIV0; // check abs diff with precision
		else {
			val.d=arg.fa.d[0]; arg.fa.d[0]=arg.fa.d[3]/det; arg.fa.d[3]=val.d/det;
			arg.fa.d[1]=-arg.fa.d[1]/det; arg.fa.d[2]=-arg.fa.d[2]/det;
		}
		break;
	default:
		fDel=arg.fa.xdim>16; l=arg.fa.xdim*(sizeof(unsigned)+sizeof(double)+arg.fa.xdim*sizeof(double));
		if ((piv=fDel?(unsigned*)ctx.ma->malloc(l):(unsigned*)alloca(l))==NULL) return RC_NOMEM;
		if ((rc=laLUdec(arg,piv,det))==RC_OK) {
			double *b=(double*)(piv+arg.fa.xdim),*pd=b+arg.fa.xdim;
			for (unsigned i=0; i<(unsigned)arg.fa.xdim; i++) {
				memset(b,0,arg.fa.xdim*sizeof(double)); b[i]=1.0; if ((rc=laLUslv(b,arg,&pd[i*arg.fa.xdim],piv))!=RC_OK) break;
			}
			if (rc==RC_OK) for (unsigned i=0; i<arg.fa.ydim; i++) for (unsigned j=0; j<arg.fa.xdim; j++) arg.fa.d[i*arg.fa.xdim+j]=pd[j*arg.fa.xdim+i];
		}
		if (fDel) ctx.ma->free(piv); break;
	}
	return rc;
}

RC Expr::laRank(ExprOp op,Value& arg,const Value *moreArgs,int nargs,unsigned flags,const EvalCtx& ctx)
{
	assert(op==OP_RANK); if (arg.type!=VT_ARRAY) return RC_TYPE;
	//???
	return RC_INTERNAL;
}

RC Expr::laConvToDouble(Value& arg,MemAlloc *ma)
{
	assert(arg.type==VT_ARRAY && arg.fa.type!=VT_DOUBLE && ma!=NULL);
	if (arg.fa.data!=NULL) {
		if (arg.fa.type!=VT_INT64 && arg.fa.type!=VT_UINT64 || (arg.flags&HEAP_TYPE_MASK)<SES_HEAP) {
			double *pd=new(ma) double[arg.length]; if (pd==NULL) return RC_NOMEM;
			switch (arg.fa.type) {
			default: assert(0);
			case VT_INT: for (unsigned i=0; i<arg.length; i++) pd[i]=arg.fa.i[i]; break;
			case VT_UINT: for (unsigned i=0; i<arg.length; i++) pd[i]=arg.fa.ui[i]; break;
			case VT_INT64: case VT_INTERVAL: for (unsigned i=0; i<arg.length; i++) pd[i]=(double)arg.fa.i64[i]; break;
			case VT_UINT64: case VT_DATETIME: for (unsigned i=0; i<arg.length; i++) pd[i]=(double)arg.fa.ui64[i]; break;
			case VT_FLOAT: for (unsigned i=0; i<arg.length; i++) pd[i]=arg.fa.f[i]; break;
			}
			if ((arg.flags&HEAP_TYPE_MASK)>=SES_HEAP) free(arg.fa.data,(HEAP_TYPE)(arg.flags&HEAP_TYPE_MASK));
			arg.fa.d=pd; setHT(arg,ma->getAType());
		} else for (unsigned i=0; i<arg.length; i++) arg.fa.d[i]=arg.fa.type==VT_INT64?(double)arg.fa.i64[i]:(double)arg.fa.ui64[i];
	}
	arg.fa.type=VT_DOUBLE; return RC_OK;
}

RC Expr::laLUdec(Value &arg,unsigned *piv,double& det)
{
	assert(arg.type==VT_ARRAY && arg.fa.type==VT_DOUBLE && arg.fa.xdim==arg.fa.ydim && piv!=NULL);
	double *pd=arg.fa.d; const unsigned n=arg.fa.xdim; det=1.0; unsigned sign=0;
	for (unsigned i=0,j,k; i<n; pd+=n,i++) {
		double x=fabs(pd[i]),y,*pr,*px=NULL; piv[i]=i;
		for (j=i+1,pr=pd+n; j<n; pr+=n,j++) {y=fabs(pr[i]); if (x<y) {x=y; piv[i]=j; px=pr;}}
		if (piv[i]!=i) {assert(px!=NULL); sign^=1; for (j=0; j<n; j++) {x=px[j]; px[j]=pd[j]; pd[j]=x;}}
		if ((x=pd[i])!=0.0) det*=x; else return RC_DIV0;
		for (j=i+1,pr=pd+n; j<n; pr+=n,j++) pr[i]/=x;
		for (j=i+1,pr=pd+n; j<n; pr+=n,j++) for (k=i+1; k<n; k++) pr[k]-=pr[i]*pd[k];
	}
	if ((sign&1)!=0) det=-det;
	return RC_OK;
}

RC Expr::laLUslv(double *b,const Value &lu,double *res,const unsigned *piv)
{
	assert(lu.type==VT_ARRAY && lu.fa.type==VT_DOUBLE && lu.fa.xdim==lu.fa.ydim && b!=NULL && res!=NULL && piv!=NULL);
	const double *pd=lu.fa.d; const unsigned n=lu.fa.xdim;
	for (unsigned i=0,j; i<n; pd+=n,i++) {
		if (piv[i]!=i) {double tmp=b[i]; b[i]=b[piv[i]]; b[piv[i]]=tmp;}
		for (j=0,res[i]=b[i]; j<i; j++) res[i]-=res[j]*pd[j];
	}
	pd=&lu.fa.d[n*(n-1)];
	for (unsigned i=n; i--!=0; pd-=n) {
		if (piv[i]!=i) {double tmp=b[i]; b[i]=b[piv[i]]; b[piv[i]]=tmp;}
		for (unsigned j=i+1; j<n; j++) res[i]-=res[j]*pd[j];
		if (pd[i]!=0.0) res[i]/=pd[i]; else return RC_DIV0;
	}
	return RC_OK;
}
