/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _EXPR_H_
#define _EXPR_H_

#include "pinex.h"
#include "mvstoreimpl.h"

using namespace MVStore;

namespace MVStoreKernel
{

#define INITEXPRSIZE	128

#define	COPY_VALUES_OP	0x80000000
#define	CARD_OP			0x0001

#define	NO_VREFS		0xFFFF
#define	MANY_VREFS		0xFFFE

#define	EXPR_PARAMS		0x8000
#define	EXPR_PATH		0x4000
#define	EXPR_BOOL		0x2000

#define	OP_CON			0x7F
#define	OP_VAR			0x7E
#define	OP_PROP			0x7D
#define	OP_PARAM		0x7C
#define	OP_ELT			0x7B
#define	OP_JUMP			0x7A
#define	OP_CATCH		0x79
#define	OP_IN1			0x78
#define OP_CURRENT		0x77

#define	CND_SORT		0x80000000
#define	CND_EQ			0x40000000
#define	CND_NE			0x20000000
#define	CND_INSERT		0x10000000

#define	CND_EXISTS_R	0x0800
#define	CND_FORALL_R	0x0400
#define	CND_EXISTS_L	0x0200
#define	CND_FORALL_L	0x0100
#define	CND_EXT			0x80
#define	CND_IN_LBND		0x40
#define	CND_UNS			0x40
#define	CND_IN_RBND		0x20
#define	CND_DISTINCT	0x20
#define	CND_NCASE		0x10
#define	CND_NOT			0x08
#define	CND_MASK		0x07

struct ExprHdr
{
	uint32_t		lExpr;
	uint16_t		lStack;
	uint16_t		flags;
	ExprHdr(uint32_t lE,uint16_t lS,uint16_t flg) : lExpr(lE),lStack(lS),flags(flg) {}
	ExprHdr(const ExprHdr& hdr) : lExpr(hdr.lExpr),lStack(hdr.lStack),flags(hdr.flags) {}
};

class	ExprTree;
class	SOutCtx;

class Expr : public IExpr
{
	ExprHdr		hdr;
public:
	Expr() : hdr(0,0,0) {}
public:
	RC				execute(Value& res,const Value *params,unsigned nParams) const;
	char			*toString(unsigned mode=0,const QName *qNames=NULL,unsigned nQNames=0) const;
	IExpr			*clone() const;
	void			destroy();
	static	RC		eval(const Expr *const *exprs,ulong nExp,Value& result,const PINEx **vars,ulong nVars,
											const Value *params,ulong nParams,MemAlloc *ma,bool fIgnore=false);
	static	RC		compile(const ExprTree *,Expr *&,MemAlloc *ma,struct PropDNF **pd=NULL,size_t *pl=NULL);
	RC				decompile(ExprTree*&,Session *ses) const;
	static	RC		calc(ExprOp op,Value& arg,const Value *moreArgs,int nargs,unsigned flags,MemAlloc *ma,const PINEx **vars=NULL,ulong nVars=0);
	static	RC		calcAgg(ExprOp op,Value& res,const Value *more,unsigned nargs,unsigned flags,MemAlloc *ma);
	static	int		cmp(const Value&,const Value&,ulong);
	static	int		cvcmp(Value&,const Value&,ulong);
	ushort			getFlags() const {return hdr.flags;}
	size_t			serSize() const {return hdr.lExpr;}
	byte			*serialize(byte *buf) const {memcpy(buf,&hdr,hdr.lExpr); return buf+hdr.lExpr;}
	RC				render(int prty,SOutCtx&) const;
	static	RC		deserialize(Expr*&,const byte *&buf,const byte *const ebuf,MemAlloc*);
	static	Expr	*clone(const Expr *exp,MemAlloc *ma);
	static	RC		getI(const Value& v,long& num);
	static	RC		path(Value *v,PropertyID pid,unsigned flags,ElementID eid=STORE_COLLECTION_ID,Expr *filter=NULL,ClassID cls=STORE_INVALID_CLASSID,unsigned rmin=1,unsigned rmax=1);
private:
	Expr(uint32_t lE,ushort lS,ushort flg) : hdr(lE,lS,flg) {}
	Expr(const ExprHdr& h) : hdr(h) {}
	void			*operator new(size_t,byte *p) throw() {return p;}
	static	const	Value *strOpConv(Value&,const Value*,Value&);
	static	const	Value *cmpOpConv(Value&,const Value*,Value&);
	static	const	Value *numOpConv(Value&,const Value*,Value&,bool fInt=false);
	static	bool	numOpConv(Value&,bool fInt=false);
	friend	class	ExprCompileCtx;
	friend	struct	AggAcc;
};

class ExprTree : public IExprTree
{
	Session	*const	ses;
	ExprOp			op;
	ushort			nops;
	ushort			vrefs;
	ushort			flags;
	Value			operands[1];
private:
	void			*operator new(size_t s,unsigned nOps,Session *ses) throw() {return ses->malloc(s+int(nOps-1)*sizeof(Value));}
	void			operator delete(void *p) {if (p!=NULL) ((ExprTree*)p)->ses->free(p);}
	static	RC		render(const Value& v,int prty,SOutCtx&);
	static	RC		cvNot(Value&);
	const	static	ExprOp	notOp[];
public:
	ExprTree(ExprOp o,ushort no,ushort vr,ulong flags,const Value *ops,Session *s);
	virtual			~ExprTree();
	ExprOp			getOp() const;
	unsigned		getNumberOfOperands() const;
	const	Value&	getOperand(unsigned idx) const;
	unsigned		getFlags() const;
	bool			operator==(const ExprTree& rhs) const;
	bool			isEval() const {return true; /*???*/}
	RC				render(int prty,SOutCtx&) const;
	RC				toPathSeg(PathSeg& ps,MemAlloc *ma) const;
	void			setFlags(unsigned flags,unsigned mask);
	IExpr			*compile();
	char			*toString(unsigned mode=0,const QName *qNames=NULL,unsigned nQNames=0) const;
	IExprTree		*clone() const;
	void			destroy();
	static	RC		node(Value&,Session*,ExprOp,unsigned,const Value *,ulong);
	static	RC		forceExpr(Value&,Session *ses,bool fCopy=false);
	static	RC		normalizeArray(Value *vals,unsigned nvals,Value& res,MemAlloc *ma,StoreCtx *ctx);
	static	ushort	vRefs(const Value& v) {return v.type==VT_VARREF?v.refPath.refN<<8|v.refPath.refN:v.type==VT_EXPRTREE?((ExprTree*)v.exprt)->vrefs:NO_VREFS;}
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

#define	CV_CARD		0x0001
#define	CV_REF		0x0002
#define	CV_NE		0x0004

class ExprCompileCtx : public OutputBuf {
	uint32_t	labelCount;
	ulong		lStack;
	ulong		xStack;
	ulong		flags;
	struct ExprLbl {
		ExprLbl		*next; 
		int			label;
		uint32_t	addr;
		ExprLbl(ExprLbl *nxt,int l,uint32_t ad) : next(nxt),label(l),addr(ad) {}
	}		*labels;
	struct	PropDNF	*dnf;
	size_t			ldnf;
	const	bool	fDNF;
	ExprCompileCtx(MemAlloc *,bool fDNF);
	~ExprCompileCtx();
	RC		compileNode(const ExprTree *expr,ulong flg=0);
	RC		compileValue(const Value& v,ulong flg=0);
	RC		compileCondition(const ExprTree *node,ulong flags,uint32_t lbl);
	RC		putCondCode(ExprOp op,ulong fa,uint32_t lbl,bool flbl=true,int nops=0);
	void	adjustRef(uint32_t lbl);
	friend	RC	Expr::compile(const ExprTree *,Expr *&,MemAlloc*,struct PropDNF **,size_t *);
};

struct AggAcc
{
	uint64_t			count;
	Value				sum;
	double				sum2;
	ExprOp				op;
	uint16_t			flags;
	MemAlloc	*const	ma;
	AggAcc() : count(0),sum2(0.),op(OP_COUNT),flags(0),ma(NULL) {sum.setError();}
	AggAcc(ExprOp o,uint16_t f,MemAlloc *m) : count(0),sum2(0.),op(o),flags(f),ma(m) {sum.setError();}
	void		next(const Value& v);
	RC			result(Value& res);
};

class PathIt : public IntNav
{
	struct QElt	{
		QElt		*next;
		unsigned	depth;
		Value		src;
	};
	const	PropertyID	pid;
	const	ElementID	eid;
	Expr	*const		filter;
	const	ClassID		cls;
	const	unsigned	rmin,rmax;
	QElt				*next,*last;
	unsigned			depth;
	bool				fRef;
	bool				fDFS;
	unsigned			idx;
	Value				src;
public:
	PathIt(PropertyID pi,ElementID ei,Expr *flt,ClassID cl,unsigned rm,unsigned rx) 
		: pid(pi),eid(ei),filter(flt),cls(cl),rmin(rm),rmax(rx),next(NULL),last(NULL),depth(0),idx(0) {src.setError();}
	~PathIt();

	const	Value	*navigate(GO_DIR=GO_NEXT,ElementID=STORE_COLLECTION_ID);
	const	Value	*navigateNR(GO_DIR op,ElementID=STORE_COLLECTION_ID);
	ElementID		getCurrentID();
	const	Value	*getCurrentValue();
	RC				getElementByID(ElementID,Value&);
	INav			*clone() const;
	INav			*clone(MemAlloc *ma) const;
	unsigned long	count() const;
	void			release();
	void			destroy();
	friend	class	Expr;
};

};

#endif
