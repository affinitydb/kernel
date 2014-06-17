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

/**
 * expression construction, compilation and evaluation classes
 * implementation of IExprNode and IExpr interfaces (see affinity.h)
 * aggregation operation data structures
 */
#ifndef _EXPR_H_
#define _EXPR_H_

#include "session.h"

using namespace Afy;

namespace AfyKernel
{
class	ExprNode;
class	SOutCtx;
struct	PropListP;
struct	Values;
struct	RExpCtx;

/**
 * evaluation context type
 */
enum EvalCtxType
{
	ECT_QUERY, ECT_INSERT, ECT_DETECT, ECT_ACTION
};

/**
 * types of context variables (in Value.refV.flags)
 */
enum QCtxVT
{
	QV_PARAMS, QV_CTX, QV_CORRELATED, QV_AGGS, QV_GROUP, QV_REXP, QV_ALL
};

#define	VAR_CTX		((QV_CTX+1)<<13)
#define	VAR_CORR	((QV_CORRELATED+1)<<13)
#define	VAR_AGGS	((QV_AGGS+1)<<13)
#define	VAR_GROUP	((QV_GROUP+1)<<13)
#define	VAR_REXP	((QV_REXP+1)<<13)
#define	VAR_NAMED	((QV_ALL+1)<<13)

/**
 * expression/statement evaluation context
 */
struct EvalCtx
{
	MemAlloc	*const	ma;
	Session		*const	ses;
	PIN			**const	env;
	unsigned	const	nEnv;
	PIN			**const	vars;
	unsigned	const	nVars;
	const Values *const	params;
	unsigned	const	nParams;
	const EvalCtx *const stack;
	mutable	EvalCtxType	ect;
	mutable RExpCtx		*rctx;
	const		void	*modp;
	EvalCtx(Session *s,EvalCtxType e=ECT_QUERY,const EvalCtx *st=NULL) : ma(s),ses(s),env(NULL),nEnv(0),vars(NULL),nVars(0),params(NULL),nParams(0),stack(st),ect(e),rctx(NULL),modp(NULL) {}
	EvalCtx(const EvalCtx& e,EvalCtxType et=ECT_QUERY) : ma(e.ma),ses(e.ses),env(e.env),nEnv(e.nEnv),vars(e.vars),nVars(e.nVars),params(e.params),nParams(e.nParams),stack(e.stack),ect(et),rctx(NULL),modp(e.modp) {}
	EvalCtx(Session *s,PIN **en,unsigned ne,PIN **v,unsigned nV,const Values *par=NULL,unsigned nP=0,const EvalCtx *stk=NULL,MemAlloc *m=NULL,EvalCtxType e=ECT_QUERY,const void *mp=NULL)
		: ma(m!=NULL?m:(MemAlloc*)s),ses(s),env(en),nEnv(ne),vars(v),nVars(nV),params(par),nParams(nP),stack(stk),ect(e),rctx(NULL),modp(mp) {}
};

/**
 * various constants used in expression p-code
 */
#define INITEXPRSIZE	128

#define	COPY_VALUES_OP	0x80000000
#define	COMP_PATTERN_OP	CASE_INSENSITIVE_OP

#define	NO_VREFS		0xFFFF
#define	MANY_VREFS		0xFFFE

#define	EXPR_EXTN			0x8000
#define	EXPR_BOOL			0x4000
#define	EXPR_DISJUNCTIVE	0x2000
#define	EXPR_PARAMS			0x1000
#define	EXPR_SELF			0x0800
#define	EXPR_CONST			0x0400
#define EXPR_NO_VARS		0x0200
#define EXPR_MANY_VARS		0x0100
#define EXPR_SORTED			0x0080

#define	PROP_OPTIONAL		0x80000000

#define	STORE_VAR_ELEMENT	(STORE_ALL_ELEMENTS-1)
#define	STORE_VAR2_ELEMENT	(STORE_ALL_ELEMENTS-2)

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
#define	OP_ENVV			0x74
#define	OP_ENVV_PROP	0x73
#define	OP_ENVV_ELT		0x72
#define	OP_RXREF		0x71
#define	OP_NAMED_PROP	0x70
#define	OP_NAMED_ELT	0x6F
#define	OP_NAMED_PRM	0x6E
#define	OP_EXTCALL		0x6D
#define	OP_SUBX			0x6C

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
#define	CND_CMPLRX		0x10
#define	CND_NOT			0x08
#define	CND_MASK		0x07

#define	NO_FLT			0x0001
#define	NO_INT			0x0002
#define	NO_DAT1			0x0004
#define	NO_ITV1			0x0008
#define	NO_DAT2			0x0010
#define	NO_ITV2			0x0020

class ExprPropCmp
{
public:
	__forceinline static int cmp(uint32_t x,uint32_t y) {return cmp3(x&STORE_MAX_URIID,y&STORE_MAX_URIID);}
};

/**
 * expression header
 * both in memory and in serialized compiled expressions
 */
struct ExprHdr
{
	uint32_t		lExpr;						/**< expression length */
	uint16_t		nStack;						/**< stack depth necessary for expression evaluation */
	uint8_t			nSubx;						/**< number of common subexpressions (max 64) */
	uint8_t			var;						/**< referred variable */
	uint16_t		flags;						/**< expression flags (see EXPR_XXX above ) */
	uint16_t		nProps;						/**< number of properties referred by this expression */
	ExprHdr(uint32_t lE,uint16_t nS,uint8_t nSx,uint8_t v,uint16_t flg,uint16_t nP) : lExpr(lE),nStack(nS),nSubx(nSx),var(v),flags(flg),nProps(nP) {}
	ExprHdr(const ExprHdr& hdr) : lExpr(hdr.lExpr),nStack(hdr.nStack),nSubx(hdr.nSubx),var(hdr.var),flags(hdr.flags),nProps(hdr.nProps) {}
};

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
	RC				eval(Value& result,const EvalCtx& ctx) const;
	bool			condSatisfied(const EvalCtx& ctx) const;
	static	RC		compile(const ExprNode *,Expr *&,MemAlloc *ma,bool fCond,Values *aggs=NULL);
	static	RC		compileConds(const ExprNode *const *exprs,unsigned nExp,Expr *merge,Expr *&res,MemAlloc*ma);
	static	RC		create(uint16_t langID,const byte *body,uint32_t lBody,uint16_t flags,Expr *&,MemAlloc*);
	static	RC		substitute(Expr *&exp,const Value *pars,unsigned nPars,MemAlloc *ma);
	RC				decompile(ExprNode*&,MemAlloc *ses) const;
	static	RC		calc(ExprOp op,Value& arg,const Value *moreArgs,int nargs,unsigned flags,const EvalCtx& ctx);
	static	RC		calcAgg(ExprOp op,Value& res,const Value *more,int nargs,unsigned flags,const EvalCtx& ctx);
	static	RC		calcHash(ExprOp op,Value& res,const Value *more,int nargs,unsigned flags,const EvalCtx& ctx);
	static	RC		laNorm(ExprOp op,Value& arg,const Value *moreArgs,int nargs,unsigned flags,const EvalCtx& ctx);
	static	RC		laTrace(ExprOp op,Value& arg,const Value *moreArgs,int nargs,unsigned flags,const EvalCtx& ctx);
	static	RC		laTrans(ExprOp op,Value& arg,const Value *moreArgs,int nargs,unsigned flags,const EvalCtx& ctx);
	static	RC		laDet(ExprOp op,Value& arg,const Value *moreArgs,int nargs,unsigned flags,const EvalCtx& ctx);
	static	RC		laInv(ExprOp op,Value& arg,const Value *moreArgs,int nargs,unsigned flags,const EvalCtx& ctx);
	static	RC		laRank(ExprOp op,Value& arg,const Value *moreArgs,int nargs,unsigned flags,const EvalCtx& ctx);
	static	RC		laAddSub(ExprOp op,Value& lhs,const Value& rhs,unsigned flags,const EvalCtx& ctx);
	static	RC		laMul(Value& lhs,const Value& rhs,unsigned flags,const EvalCtx& ctx);
	static	RC		laDiv(Value& lhs,const Value& rhs,unsigned flags,const EvalCtx& ctx);
	ushort			getFlags() const {return hdr.flags;}
	size_t			serSize() const {return hdr.lExpr;}
	byte			*serialize(byte *buf) const {memcpy(buf,&hdr,hdr.lExpr); return buf+hdr.lExpr;}
	RC				render(int prty,SOutCtx&) const;
	void			getExtRefs(const PropertyID *&pids,unsigned& nPids) const {pids=hdr.nProps!=0?(const PropertyID*)(&hdr+1):NULL; nPids=hdr.nProps;}
	RC				mergeProps(PropListP& plp,bool fForce=false,bool fFlags=false) const;
	RC				getPropDNF(ushort var,struct PropDNF *&dnf,size_t& ldnf,MemAlloc *ma) const;
	static	RC		deserialize(Expr*&,const byte *&buf,const byte *const ebuf,MemAlloc*);
	static	Expr	*clone(const Expr *exp,MemAlloc *ma);
	static	RC		getI(const Value& v,long& num,const EvalCtx&);
	static	RC		condRC(int c,ExprOp op) {assert(op>=OP_EQ && op<=OP_GE); return c<-2?RC_TYPE:c==-2||(compareCodeTab[op-OP_EQ]&1<<(c+1))==0?RC_FALSE:RC_TRUE;}
	static	RC		registerExtn(void *itf,uint16_t& langID);
	static	void	**extnTab;
	static	int		nExtns;
private:
	Expr(const ExprHdr& h) : hdr(h) {}
	void			*operator new(size_t,byte *p) throw() {return p;}
	static	const	Value *strOpConv(Value&,const Value*,Value&,const EvalCtx&);
	static	const	Value *numOpConv(Value&,const Value*,Value&,unsigned flg,const EvalCtx&);
	static	bool	numOpConv(Value&,unsigned flg,const EvalCtx&);
	static	RC		expandStr(Value& v,uint32_t newl,MemAlloc *ma,bool fZero=false);
	static	RC		laConvToDouble(Value& arg,MemAlloc *ma);
	static	RC		laLUdec(Value &arg,unsigned *piv,double& det);
	static	RC		laLUslv(double *b,const Value &lu,double *res,const unsigned *piv);
	static	void	hashValue(const Value &v,unsigned flags,class SHA256& sha);
	static	const	byte	compareCodeTab[];
	friend	class	ExprCompileCtx;
	friend	struct	AggAcc;
};

/**
 * expression tree node
 * implements IExprNode interface
 */
class ExprNode : public IExprNode
{
	MemAlloc *const	ma;
	ExprOp			op;
	uint8_t			nops;
	short			refs;
	ushort			vrefs;
	ushort			flags;
	Value			operands[1];
private:
	void			*operator new(size_t s,unsigned nOps,MemAlloc *ma) throw() {return ma->malloc(s+int(nOps-1)*sizeof(Value));}
	void			operator delete(void *p) {if (p!=NULL) ((ExprNode*)p)->ma->free(p);}
	static	RC		render(const Value& v,int prty,SOutCtx&);
	static	RC		cvNot(Value&);
	const	static	ExprOp	notOp[];
public:
	ExprNode(ExprOp o,ushort no,ushort vr,unsigned flags,const Value *ops,MemAlloc *ma);
	virtual			~ExprNode();
	ExprOp			getOp() const;
	unsigned		getNumberOfOperands() const;
	const	Value&	getOperand(unsigned idx) const;
	unsigned		getFlags() const;
	bool			operator==(const ExprNode& rhs) const;
	RC				render(int prty,SOutCtx&) const;
	RC				toPathSeg(PathSeg& ps,MemAlloc *ma) const;
	RC				getPropDNF(ushort var,struct PropDNF *&dnf,size_t& ldnf,MemAlloc *ma) const;
	void			setFlags(unsigned flags,unsigned mask);
	IExpr			*compile();
	char			*toString(unsigned mode=0,const QName *qNames=NULL,unsigned nQNames=0) const;
	RC				substitute(const Value *pars,unsigned nPars,MemAlloc*);
	IExprNode		*clone() const;
	void			destroy();
	static	RC		node(Value&,Session*,ExprOp,unsigned,const Value *,unsigned);
	static	RC		forceExpr(Value&,MemAlloc *ma,bool fCopy=false);
	static	RC		normalizeCollection(Value *vals,unsigned nvals,Value& res,MemAlloc *ma,StoreCtx *ctx);
	static	RC		normalizeArray(Session *s,Value *vals,unsigned nvals,Value& res,MemAlloc *ma);
	static	RC		normalizeStruct(Session *s,Value *vals,unsigned nvals,Value& res,MemAlloc *ma,bool fPIN);
	static	RC		normalizeMap(Value *vals,unsigned nvals,Value& res,MemAlloc *ma);
	static	ushort	vRefs(const Value& v) {return v.type==VT_VARREF&&(v.refV.flags==0xFFFF||(v.refV.flags&VAR_TYPE_MASK)==0)?v.refV.refN<<8|v.refV.refN:v.type==VT_EXPRTREE?((ExprNode*)v.exprt)->vrefs:NO_VREFS;}
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
#define	CV_CALL		0x0020
#define	CV_CHNGD	0x0040

/**
 * expression compilation context
 */
class ExprCompileCtx {
	MemAlloc	*const	ma;
	Values		*const	aggs;
	uint32_t	labelCount;
	uint32_t	nStack;
	uint32_t	nCSE;
	byte		buf[256];
	Expr		*pHdr;
	byte		*pCode;
	uint32_t	lCode;
	uint32_t	xlCode;
	struct ExprLbl {
		ExprLbl		*next; 
		int			label;
		uint32_t	addr;
		ExprLbl(ExprLbl *nxt,int l,uint32_t ad) : next(nxt),label(l),addr(ad) {}
	}			*labels;
	ExprCompileCtx(MemAlloc *,Values *);
	~ExprCompileCtx();
	RC		compileNode(const ExprNode *expr,unsigned flg=0);
	RC		compileValue(const Value& v,unsigned flg=0);
	RC		compileCondition(const ExprNode *node,unsigned mode,uint32_t lbl,unsigned flg=0);
	RC		addUIDRef(uint32_t id,uint8_t var,uint32_t flags,uint16_t& idx);
	RC		putCondCode(ExprOp op,unsigned fa,uint32_t lbl,bool flbl=true,int nops=0);
	void	adjustRef(uint32_t lbl);
	bool	expand(uint32_t l);
	bool	expandHdr(uint32_t l);
	RC		result(Expr *&);
	byte	*alloc(uint32_t l) {return lCode+l>xlCode && !expand(l)?NULL:(lCode+=l,pCode+lCode-l);}
	friend	RC	Expr::compile(const ExprNode *,Expr *&,MemAlloc*,bool fCond,Values*);
	friend	RC	Expr::compileConds(const ExprNode *const *exprs,unsigned nExp,Expr *merge,Expr *&res,MemAlloc*ma);

};

/**
 * Regular expression matching result
 */
struct	RxSht {size_t sht,len;};

struct RExpCtx : public DynArray<RxSht>
{	
	char	*rxstr;
	bool	fFree;
	RExpCtx(MemAlloc *m,char *s=NULL,bool f=false) : DynArray<RxSht>(m),rxstr(s),fFree(f) {}
	~RExpCtx() {if (rxstr!=NULL && fFree) ma->free(rxstr);}
	void operator=(RExpCtx& rhs) {if (rxstr!=NULL && fFree) ma->free(rxstr); rxstr=rhs.rxstr; fFree=rhs.fFree; rhs.fFree=false; DynArray<RxSht>::operator=(rhs);}
};

class CmpValue
{
public:
	static SListOp compare(const Value& v1,Value& v2,unsigned u,MemAlloc& ma) {
		int c=cmp(v1,v2,u,&ma); if (c<0) return SLO_LT; if (c>0) return SLO_GT; 
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
	ValueC		sum;
	double		sum2;
	ElementID	eid;
	Histogram	*hist;
	ExprOp		op;
	uint16_t	flags;
	MemAlloc	*ma;
	const		EvalCtx		*const ctx;
	AggAcc() : count(0),sum2(0.),eid(0),hist(NULL),op(OP_COUNT),flags(0),ma(NULL),ctx(NULL) {}
	AggAcc(ExprOp o,uint16_t f,const EvalCtx *ct,Histogram *h,MemAlloc *m=NULL) : count(0),sum2(0.),hist(h),op(o),flags(f),ma(m!=NULL?m:ct->ma),ctx(ct) {}
	RC			next(const Value& v);
	RC			process(const Value& v);
	RC			result(Value& res,bool fRestore=false);
	void		reset() {count=0; sum.setEmpty(); sum2=0.; if (hist!=NULL) hist->clear();}
};

/**
 * Common code for PathOp and PathIt
 */
class Path
{
protected:
	struct PathState {
		PathState	*next;
		int			state;
		unsigned	idx;
		unsigned	rcnt;
		unsigned	vidx;
		unsigned	cidx;
		PID			id;
		Value		v[2];
	};
	MemAlloc		*const	ma;
	const	PathSeg	*const	path;
	const	unsigned		nPathSeg;
	const	bool			fCopied;
	bool					fThrough;
	PathState				*pst;
	PathState				*freePst;
protected:
	Path(MemAlloc *m,const PathSeg *ps,unsigned nP,bool fC) 
		: ma(m),path(ps),nPathSeg(nP),fCopied(fC),fThrough(true),pst(NULL),freePst(NULL)
			{for (unsigned i=0; i<nP; i++) if (ps[i].rmin!=0) {fThrough=false; break;}}
	~Path() {
		PathState *ps,*ps2;
		for (ps=pst; ps!=NULL; ps=ps2) {ps2=ps->next; freeV(ps->v[0]); freeV(ps->v[1]); ma->free(ps);}
		for (ps=freePst; ps!=NULL; ps=ps2) {ps2=ps->next; ma->free(ps);}
		if (fCopied) destroyPath((PathSeg*)path,nPathSeg,ma);
	}
	RC push(const PID& id) {
		PathState *ps;
		if (pst!=NULL && pst->vidx!=0) for (ps=pst; ps!=NULL && ps->idx==pst->idx; ps=ps->next) if (ps->id==id) {if (pst->state==2) pst->vidx++; return RC_OK;}
		if ((ps=freePst)!=NULL) freePst=ps->next; else if ((ps=new(ma) PathState)==NULL) return RC_NOMEM;
		if (pst==NULL) ps->idx=0,ps->rcnt=1; else if (pst->vidx==0) ps->idx=pst->idx+1,ps->rcnt=1; else ps->idx=pst->idx,ps->rcnt=pst->rcnt+1;
		ps->state=0; ps->vidx=2; ps->cidx=0; ps->id=id; ps->v[0].setEmpty(); ps->v[1].setEmpty(); ps->next=pst; pst=ps; return RC_OK;
	}
	void pop() {PathState *ps=pst; if (ps!=NULL) {pst=ps->next; freeV(ps->v[0]); freeV(ps->v[1]); ps->next=freePst; freePst=ps;}}
};

/**
 * path expression iterator
 * represents path expression as a collection
 */
class PathIt : public INav, public Path
{
	const	bool	fDFS;
	ValueC			src;
	ValueC			res;
	unsigned		sidx;
public:
	PathIt(MemAlloc *m,const PathSeg *ps,unsigned nP,const Value& v,bool fD) : Path(ma,ps,nP,false),fDFS(fD),src(v,m),sidx(0) {}
	virtual	~PathIt();
	const	Value	*navigate(GO_DIR=GO_NEXT,ElementID=STORE_COLLECTION_ID);
	ElementID		getCurrentID();
	const	Value	*getCurrentValue();
	RC				getElementByID(ElementID,Value&);
	INav			*clone() const;
	unsigned		count() const;
	void			destroy();
	friend	class	Expr;
};

/**
 * loadPIN/loadV flags
 */
#define	LOAD_CARDINALITY	0x8000
#define	LOAD_EXT_ADDR		0x4000
#define	LOAD_SSV			0x2000
#define	LOAD_REF			0x1000
#define	LOAD_COLLECTION		0x0800
#define	LOAD_CLIENT			0x0400
#define	LOAD_RAW			0x0200

/**
 * Internal MODE_* flags
 */
#define	MODE_COUNT			0x80000000
#define	MODE_MANY_PINS		0x40000000
#define	MODE_PREFIX_READ	0x40000000
#define	MODE_OLDLEN			0x20000000
#define	MODE_SAME_PROPS		0x20000000
#define	MODE_REFRESH		0x10000000

#define	MODE_DEVENT			0x00008000
#define	MODE_NODEL			0x00004000		
#define	MODE_NO_RINDEX		0x00002000
#define	MODE_COMPOUND		0x00002000
#define	MODE_CHECKBI		0x00002000
#define	MODE_CASCADE		0x00001000
#define	MODE_FSM			0x00001000
#define	MODE_PID			0x00000800
#define	MODE_INMEM			0x00000400

/**
 * types of SELECT lists
 */
enum SelectType
{
	SEL_CONST,			/**< select returns constant expressions */
	SEL_COUNT,			/**< SELECT COUNT(*) ... */
	SEL_PID,			/**< SELECT afy:pinID ... */
	SEL_FIRSTPID,		/**< SELECT FIRST afy:pinID ... */
	SEL_VALUE,			/**< SELECT expr, aggregation is used, one result is returned */
	SEL_VALUESET,		/**< SELECT expr, no aggregation */
	SEL_DERIVED,		/**< SELECT list of expressions, aggregation is used, one result is returned */
	SEL_DERIVEDSET,		/**< SELECT list of expressions, no aggregation */
	SEL_DERIVEDPINSET,	/**< SELECT @{...} */
	SEL_AUGMENTED,		/**< SELECT @,... */
	SEL_PIN,			/**< SELECT FIRST * ... */
	SEL_PINSET,			/**< SELECT * for non-join query */
	SEL_COMPOUND,		/**< SELECT * for join query */
	SEL_COMP_DERIVED	/**< SELECT list of expressions for join query */
};

/**
 * internal Value::flags flags (maximum 0x40)
 */
#define VF_PART				0x08
#define	VF_REF				0x10
#define	VF_STRING			0x20
#define	VF_SSV				0x40

#define	SORT_MASK			(ORD_DESC|ORD_NCASE|ORD_NULLS_BEFORE|ORD_NULLS_AFTER)

#define	ORDER_EXPR	0x8000

/**
 * internal descriptor for ordering segment
 */
struct OrderSegQ
{
	union {
		PropertyID	pid;
		Expr		*expr;
	};
	uint16_t		flags;
	uint8_t			var;
	uint8_t			aggop;
	uint32_t		lPref;
	RC				conv(const OrderSeg& sg,MemAlloc *ma);
};

/**
 * dynamic array of property IDs
 */
struct PropList
{
	PropertyID		*props;
	uint16_t		nProps;
	mutable	bool	fFree;
};

/**
 * property ID holder for multiple variables
 */
struct PropListP
{
	PropList	*pls;
	unsigned	nPls;
	MemAlloc	*const	ma;
	PropList	pbuf[16];
	PropListP(MemAlloc *m) : pls(pbuf),nPls(0),ma(m) {}
	~PropListP() {for (unsigned i=0; i<nPls; i++) if (pls[i].fFree) ma->free(pls[i].props); if (pls!=pbuf) ma->free(pls);}
	RC			merge(uint16_t var,const PropertyID *pid,unsigned nPids,bool fForce=false,bool fFlags=false);
	RC			operator+=(const PropListP &rhs);
	RC			checkVar(uint16_t var);
};

/**
 * dynamic array of values
 */
struct Values
{
	const Value	*vals;
	uint16_t	nValues;
	bool		fFree;
	Values() : vals(NULL),nValues(0),fFree(false) {}
	Values(const Values& vv) : vals(vv.vals),nValues(vv.nValues),fFree(false) {}
	Values(const Value *pv,unsigned nv,bool fF=false) : vals(pv),nValues((uint16_t)nv),fFree(fF) {}
};

// used in filter, sort, etc. operators
struct QueryWithParams
{
	class	Stmt	*qry;
	unsigned		nParams;
	Value			*params;
};

};

#endif
