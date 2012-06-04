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

/**
 * expression construction, compilation and evaluation classes
 * implementation of IExprTree and IExpr interfaces (see affinity.h)
 * aggregation operation data structures
 */
#ifndef _EXPR_H_
#define _EXPR_H_

#include "pinex.h"
#include "affinityimpl.h"

using namespace AfyDB;

namespace AfyKernel
{

/**
 * various constants used in expression p-code
 */
#define INITEXPRSIZE	128

#define	COPY_VALUES_OP	0x80000000

#define	NO_VREFS		0xFFFF
#define	MANY_VREFS		0xFFFE

#define	EXPR_EXTN			0x8000
#define	EXPR_PATH			0x4000
#define	EXPR_BOOL			0x2000
#define	EXPR_DISJUNCTIVE	0x1000
#define	EXPR_NO_CODE		0x0800
#define	EXPR_PRELOAD		0x0400
#define	EXPR_PARAMS			0x0200

#define	PROP_OPTIONAL	0x80000000
#define	PROP_NO_LOAD	0x40000000
#define	PROP_ORD		0x20000000

#define	OP_CON			0x7F
#define	OP_VAR			0x7E
#define	OP_PROP			0x7D
#define	OP_PARAM		0x7C
#define	OP_ELT			0x7B
#define	OP_JUMP			0x7A
#define	OP_CATCH		0x79
#define	OP_IN1			0x78
#define OP_CURRENT		0x77
#define	OP_CONID		0x76
#define	OP_SETPROP		0x75

#define	CND_SORT		0x80000000
#define	CND_EQ			0x40000000
#define	CND_NE			0x20000000

#define	CND_EXISTS_R	0x2000
#define	CND_FORALL_R	0x1000
#define	CND_EXISTS_L	0x0800
#define	CND_FORALL_L	0x0400
#define	CND_IN_LBND		0x0200
#define	CND_IN_RBND		0x0100
#define	CND_EXT			0x80
#define	CND_DISTINCT	0x20
#define	CND_UNS			0x20
#define	CND_VBOOL		0x20
#define	CND_NCASE		0x10
#define	CND_NOT			0x08
#define	CND_MASK		0x07

#define	NO_FLT			0x0001
#define	NO_INT			0x0002
#define	NO_DAT1			0x0004
#define	NO_ITV1			0x0008
#define	NO_DAT2			0x0010
#define	NO_ITV2			0x0020

/**
 * expression header
 * both in memory and in serialized compiled expressions
 */
struct ExprHdr
{
	uint32_t		lExpr;						/**< expression length */
	uint16_t		lStack;						/**< stack depth necessary for expression evaluation */
	uint16_t		flags;						/**< expression flags (see EXPR_XXX above ) */
	uint16_t		lProps;						/**< length of table of properties refered by this expression */
	uint8_t			nVars;						/**< number of variables refered by this expression */
	uint8_t			xVar;						/**< max varible ID refered by the expression */
	ExprHdr(uint32_t lE,uint16_t lS,uint16_t flg,uint16_t lP,uint8_t nV,uint8_t xV) : lExpr(lE),lStack(lS),flags(flg),lProps(lP),nVars(nV),xVar(xV) {}
	ExprHdr(const ExprHdr& hdr) : lExpr(hdr.lExpr),lStack(hdr.lStack),flags(hdr.flags),lProps(hdr.lProps),nVars(hdr.nVars),xVar(hdr.xVar) {}
};

/**
 * variable desriptor in property table in expression header
 */
struct VarHdr
{
	uint16_t		var;
	uint16_t		nProps;
};

class ExprPropCmp
{
public:
	__forceinline static int cmp(uint32_t x,uint32_t y) {return cmp3(x&STORE_MAX_URIID,y&STORE_MAX_URIID);}
};

class	ExprTree;
class	SOutCtx;
struct	PropListP;
struct	ValueV;

/**
 * compiled (p-code) expression, implements IExpr interface
 * expression in memory consists of expression header, followed by property table, folowed by p-code
 */
class Expr : public IExpr
{
	ExprHdr			hdr;
public:
	RC				execute(Value& res,const Value *params,unsigned nParams) const;
	char			*toString(unsigned mode=0,const QName *qNames=NULL,unsigned nQNames=0) const;
	IExpr			*clone() const;
	void			destroy();
	static	RC		eval(const Expr *const *exprs,ulong nExp,Value& result,PINEx **vars,ulong nVars,const ValueV *params,ulong nParams,MemAlloc *ma,bool fIgnore=false);
	static	RC		compile(const ExprTree *,Expr *&,MemAlloc *ma,bool fCond,ValueV *aggs=NULL);
	static	RC		compileConds(const ExprTree *const *,unsigned nExp,Expr *&,MemAlloc*);
	static	RC		create(uint16_t langID,const byte *body,uint32_t lBody,uint16_t flags,Expr *&,MemAlloc*);
	RC				decompile(ExprTree*&,Session *ses) const;
	static	RC		calc(ExprOp op,Value& arg,const Value *moreArgs,int nargs,unsigned flags,MemAlloc *ma,PINEx **vars=NULL,ulong nVars=0);
	static	RC		calcAgg(ExprOp op,Value& res,const Value *more,unsigned nargs,unsigned flags,MemAlloc *ma);
	ushort			getFlags() const {return hdr.flags;}
	size_t			serSize() const {return hdr.lExpr;}
	byte			*serialize(byte *buf) const {memcpy(buf,&hdr,hdr.lExpr); return buf+hdr.lExpr;}
	RC				render(int prty,SOutCtx&) const;
	void			getExtRefs(ushort var,const PropertyID *&pids,unsigned& nPids) const;
	RC				mergeProps(PropListP& plp,bool fForce=false,bool fFlags=false) const;
	RC				getPropDNF(ushort var,struct PropDNF *&dnf,size_t& ldnf,MemAlloc *ma) const;
	static	RC		addPropRefs(Expr **pex,const PropertyID *props,unsigned nProps,MemAlloc *ma);
	static	RC		deserialize(Expr*&,const byte *&buf,const byte *const ebuf,MemAlloc*);
	static	Expr	*clone(const Expr *exp,MemAlloc *ma);
	static	RC		getI(const Value& v,long& num);
	static	bool	condSatisfied(const class Expr *const *exprs,ulong nExp,PINEx **vars,unsigned nVars,const ValueV *pars,unsigned nPars,MemAlloc *ma,bool fIgnore=false);
	static	RC		condRC(int c,ExprOp op) {assert(op>=OP_EQ && op<=OP_GE); return c<-2?RC_TYPE:c==-2||(compareCodeTab[op-OP_EQ]&1<<(c+1))==0?RC_FALSE:RC_TRUE;}
	static	RC		registerExtn(void *itf,uint16_t& langID);
	static	void	**extnTab;
	static	int		nExtns;
private:
	Expr(const ExprHdr& h) : hdr(h) {}
	void			*operator new(size_t,byte *p) throw() {return p;}
	RC				path(Value *&top,const byte *&codePtr,const ValueV& params,MemAlloc *ma) const;
	static	const	Value *strOpConv(Value&,const Value*,Value&);
	static	const	Value *numOpConv(Value&,const Value*,Value&,unsigned flg);
	static	bool	numOpConv(Value&,unsigned flg);
	static	const	byte	compareCodeTab[];
	struct	VarD	{
		bool				fInit;
		bool				fLoaded;
		const	PropertyID	*props;
		Value				*vals;
		unsigned			nVals;
	};
	friend	class	ExprCompileCtx;
	friend	struct	AggAcc;
};

/**
 * expression tree node
 * implements IExprTree interface
 */
class ExprTree : public IExprTree
{
	MemAlloc *const	ma;
	ExprOp			op;
	ushort			nops;
	ushort			vrefs;
	ushort			flags;
	Value			operands[1];
private:
	void			*operator new(size_t s,unsigned nOps,MemAlloc *ma) throw() {return ma->malloc(s+int(nOps-1)*sizeof(Value));}
	void			operator delete(void *p) {if (p!=NULL) ((ExprTree*)p)->ma->free(p);}
	static	RC		render(const Value& v,int prty,SOutCtx&);
	static	RC		cvNot(Value&);
	const	static	ExprOp	notOp[];
public:
	ExprTree(ExprOp o,ushort no,ushort vr,ulong flags,const Value *ops,MemAlloc *ma);
	virtual			~ExprTree();
	ExprOp			getOp() const;
	unsigned		getNumberOfOperands() const;
	const	Value&	getOperand(unsigned idx) const;
	unsigned		getFlags() const;
	bool			operator==(const ExprTree& rhs) const;
	RC				render(int prty,SOutCtx&) const;
	RC				toPathSeg(PathSeg& ps,MemAlloc *ma) const;
	RC				getPropDNF(ushort var,struct PropDNF *&dnf,size_t& ldnf,MemAlloc *ma) const;
	void			setFlags(unsigned flags,unsigned mask);
	IExpr			*compile();
	char			*toString(unsigned mode=0,const QName *qNames=NULL,unsigned nQNames=0) const;
	IExprTree		*clone() const;
	void			destroy();
	static	RC		node(Value&,Session*,ExprOp,unsigned,const Value *,ulong);
	static	RC		forceExpr(Value&,Session *ses,bool fCopy=false);
	static	RC		normalizeArray(Value *vals,unsigned nvals,Value& res,MemAlloc *ma,StoreCtx *ctx);
	static	RC		normalizeStruct(Value *vals,unsigned nvals,Value& res,MemAlloc *ma);
	static	ushort	vRefs(const Value& v) {return v.type==VT_VARREF&&(v.refV.flags==0xFFFF||(v.refV.flags&VAR_TYPE_MASK)==0)?v.refV.refN<<8|v.refV.refN:v.type==VT_EXPRTREE?((ExprTree*)v.exprt)->vrefs:NO_VREFS;}
	static	void	vRefs(ushort& vr1,ushort vr2) {if (vr1==NO_VREFS||vr2==MANY_VREFS) vr1=vr2; else if (vr1!=vr2&&vr1!=MANY_VREFS&&vr2!=NO_VREFS) mergeVRefs(vr1,vr2);}
	static	void	mergeVRefs(ushort& vr1,ushort vr2);
	void			mapVRefs(byte from,byte to);
	friend	void	freeV0(Value&);
	friend	class	ExprCompileCtx;
	friend	class	QueryPrc;
	friend	class	SInCtx;
	friend	class	Stmt;
	friend	class	Expr;
};

/**
 * compilation control flags
 */
#define	CV_CARD		0x0001
#define	CV_REF		0x0002
#define	CV_OPT		0x0004
#define	CV_PROP		0x0008
#define	CV_NDATA	0x0010

/**
 * expression compilation context
 */
class ExprCompileCtx {
	MemAlloc	*const	ma;
	ValueV		*const	aggs;
	uint32_t	labelCount;
	uint16_t	lStack;
	byte		buf[256];
	Expr		*pHdr;
	byte		*pCode;
	uint32_t	lCode;
	uint32_t	xlCode;
	bool		fCollectRefs;
	struct ExprLbl {
		ExprLbl		*next; 
		int			label;
		uint32_t	addr;
		ExprLbl(ExprLbl *nxt,int l,uint32_t ad) : next(nxt),label(l),addr(ad) {}
	}		*labels;
	ExprCompileCtx(MemAlloc *,ValueV *);
	~ExprCompileCtx();
	RC		compileNode(const ExprTree *expr,ulong flg=0);
	RC		compileValue(const Value& v,ulong flg=0);
	RC		compileCondition(const ExprTree *node,ulong mode,uint32_t lbl,ulong flg=0);
	RC		addExtRef(uint32_t id,uint16_t var,uint32_t flags,uint16_t& idx);
	RC		putCondCode(ExprOp op,ulong fa,uint32_t lbl,bool flbl=true,int nops=0);
	void	adjustRef(uint32_t lbl);
	bool	expandHdr(uint32_t l,VarHdr *&vh);
	bool	expand(uint32_t l);
	RC		result(Expr *&);
	byte	*alloc(uint32_t l) {return lCode+l>xlCode && !expand(l)?NULL:(lCode+=l,pCode+lCode-l);}
	friend	RC	Expr::compile(const ExprTree *,Expr *&,MemAlloc*,bool fCond,ValueV*);
	friend	RC	Expr::compileConds(const ExprTree *const *,unsigned nExp,Expr *&,MemAlloc*);
};

class CmpValue
{
public:
	static SListOp compare(const Value& v1,Value& v2,ulong u) {
		int c=cmp(v1,v2,u); if (c<0) return SLO_LT; if (c>0) return SLO_GT; 
		if (++v2.eid==0) v2.property++; return SLO_NOOP;
	}
};

typedef SList<Value,CmpValue>	Histogram;

/**
 * aggregation operation data holder
 */
struct AggAcc
{
	uint64_t	count;
	Value		sum;
	double		sum2;
	Histogram	*hist;
	ExprOp		op;
	uint16_t	flags;
	MemAlloc	*const	ma;
	AggAcc() : count(0),sum2(0.),hist(NULL),op(OP_COUNT),flags(0),ma(NULL) {sum.setError();}
	AggAcc(ExprOp o,uint16_t f,MemAlloc *m,Histogram *h) : count(0),sum2(0.),hist(h),op(o),flags(f),ma(m) {sum.setError();}
	RC			next(const Value& v);
	RC			process(const Value& v);
	RC			result(Value& res);
	void		reset() {count=0; sum.setError(); sum2=0.; if (hist!=NULL) hist->clear();}
};

/**
 * path expression iterator
 * represents path expression as a collection
 */
class PathIt : public INav, public Path
{
	const	bool	fDFS;
	Value			src;
	Value			res;
	unsigned		sidx;
public:
	PathIt(MemAlloc *m,const PathSeg *ps,unsigned nP,const Value& v,bool fD) : Path(ma,ps,nP,false),fDFS(fD),sidx(0) {src=v; res.setError();}
	virtual	~PathIt();
	const	Value	*navigate(GO_DIR=GO_NEXT,ElementID=STORE_COLLECTION_ID);
	ElementID		getCurrentID();
	const	Value	*getCurrentValue();
	RC				getElementByID(ElementID,Value&);
	INav			*clone() const;
	unsigned long	count() const;
	void			destroy();
	friend	class	Expr;
};

};

#endif
