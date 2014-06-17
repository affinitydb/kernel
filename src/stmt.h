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
 * IStmt interface implementation
 */
#ifndef _STMT_H_
#define _STMT_H_

#include "expr.h"

using namespace Afy;

namespace AfyKernel
{

#define	CALC_PROP_NAME	"calculated"		/**< prefix for derived properties names */

/**
 * statement flags
 */
#define	QRY_PARAMS		0x80000000			/**< statement contains parameters references */
#define	QRY_ORDEXPR		0x40000000			/**< expressions are used in ORDER BY */
#define	QRY_IDXEXPR		0x20000000			/**< expressions used in index references */
#define	QRY_CPARAMS		0x10000000			/**< class/family references contain parameters */
#define	QRY_CALCWITH	0x08000000			/**< WITH contains calculated expressions */

class	SOutCtx;
class	SInCtx;
class	Cursor;
class	QueryOp;
struct	BuildCtx;

/**
 * index segment reference descriptor
 */
struct CondIdx
{
	CondIdx				*next;
	const	IndexSeg	ks;
	const	ushort		param;
	MemAlloc *const		ma;
	Expr				*expr;
	CondIdx(const IndexSeg& dscr,ushort parm,MemAlloc *m,Expr *exp=NULL) : next(NULL),ks(dscr),param(parm),ma(m),expr(exp) {}
	~CondIdx() {if (expr!=NULL) ma->free(expr);}
	CondIdx	*clone(MemAlloc *ma) const;
};

/*
 * equi-join element descriptor
 */
struct CondEJ
{
	CondEJ				*next;
	const	PropertyID	propID1;
	const	PropertyID	propID2;
	const	ushort		flags;
	CondEJ(PropertyID pid1,PropertyID pid2,ushort f) : propID1(pid1),propID2(pid2),flags(f) {}
};

/**
 * free-text serach descriptor
 */
struct CondFT
{
	CondFT			*next;
	const	char	*str;
	unsigned		flags;
	unsigned		nPids;
	PropertyID		pids[1];
	CondFT(CondFT *nxt,const char *s,unsigned f,const PropertyID *ps,unsigned nps) 
		: next(nxt),str(s),flags(f),nPids(nps) {if (ps!=NULL && nps>0) memcpy(pids,ps,nps*sizeof(PropertyID));}
	void	*operator new(size_t s,unsigned nps,MemAlloc *ma) throw() {return ma->malloc(s+(nps==0?0:nps-1)*sizeof(PropertyID));}
};

/**
 * simplified statement types for quick processing
 */
enum SimpleStmtType
{
	SST_GENERIC, SST_UPDATE_AUTO, SST_UPDATE_SELF, SST_UPDATE_NAMED, SST_UPDATE_CLASS, SST_SELECT_NAMED, SST_SELECT_CLASS, SST_SELECT_FULL, SST_ALL
};

/**
 * which part of query is being rendered
 * @see SOutCtx
 */
enum RenderPart
{
	RP_FROM, RP_WHERE, RP_MATCH
};

#define	QRY_SIMPLE	QRY_ALL_SETOP	/**< QRY_SIMPLE follows all other QRY_XXX defined in affinity.h */

#define	QVF_ALL			0x01
#define	QVF_DISTINCT	0x02
#define	QVF_RAW			0x04
#define	QVF_FIRST		0x08
#define	QVF_COND		0x10

/**
 * abstract class describing query variable
 */
class QVar
{
protected:
	QVar			*next;
	const	QVarID	id;
	const	byte	type;
	SelectType		stype;
	byte			qvf;
	MemAlloc *const	ma;
	char			*name;
	Values			*outs;
	unsigned		nOuts;
	Expr			*cond;
	PropertyID		*props;
	unsigned		nProps;
	Expr			*having;
	OrderSegQ		*groupBy;
	unsigned		nGroupBy;
	Values			aggrs;
	bool			fHasParent;
	QVar(QVarID i,byte ty,MemAlloc *m);
public:
	virtual			~QVar();
	virtual	RC		clone(MemAlloc *m,QVar*&) const = 0;
	virtual	RC		build(BuildCtx& qctx,QueryOp *&qop) const = 0;
	virtual	RC		render(RenderPart,SOutCtx&) const;
	virtual	RC		render(SOutCtx&) const;
	virtual	size_t	serSize() const;
	virtual	byte	*serialize(byte *buf) const = 0;
	virtual	const	QVar *getRefVar(unsigned refN) const;
	virtual	RC		mergeProps(PropListP& plp,bool fForce=false,bool fFlags=false) const;
	virtual	RC		substitute(const Value *pv,unsigned np,MemAlloc *ma);
	static	RC		deserialize(const byte *&buf,const byte *const ebuf,MemAlloc *ma,QVar*& res);
	void	operator delete(void *p) {if (p!=NULL) ((QVar*)p)->ma->free(p);}
	QVarID			getID() const {return id;}
	byte			getType() const {return type;}
	bool			isMulti() const {return type<QRY_ALL_SETOP;}
	RC				clone(QVar *cloned) const;
	byte			*serQV(byte *buf) const;
	RC				addReqProps(const PropertyID *props,unsigned nProps);
	friend	class	Stmt;
	friend	class	SimpleVar;
	friend	class	DataEventMgr;
	friend	class	DropDataEvent;
	friend	class	DataEventRegistry;
	friend	class	QueryPrc;
	friend	struct	BuildCtx;
	friend	class	Cursor;
	friend	class	SInCtx;
	friend	class	SOutCtx;
};

/**
 * simple query variable - no join, no set operation
 * can refer a list of data events or event families, individual PINs, collection, path expression, subquery
 */
class SimpleVar : public QVar
{
	SourceSpec		*srcs;
	unsigned		nSrcs;
	CondIdx			*condIdx;
	CondIdx			*lastCondIdx;
	unsigned		nCondIdx;
	ValueC			expr;
	CondFT			*condFT;
	bool			fOrProps;
	PathSeg			*path;
	unsigned		nPathSeg;
	SimpleVar(QVarID i,MemAlloc *m) : QVar(i,QRY_SIMPLE,m),srcs(NULL),nSrcs(0),condIdx(NULL),lastCondIdx(NULL),nCondIdx(0),
										condFT(NULL),fOrProps(false),path(NULL),nPathSeg(0) {}
	virtual			~SimpleVar();
	RC				clone(MemAlloc *m,QVar*&) const;
	RC				build(BuildCtx& qctx,QueryOp *&qop) const;
	RC				render(RenderPart,SOutCtx&) const;
	size_t			serSize() const;
	byte			*serialize(byte *buf) const;
	static	RC		deserialize(const byte *&buf,const byte *const ebuf,QVarID,MemAlloc *ma,QVar*& res);
	RC				mergeProps(PropListP& plp,bool fForce=false,bool fFlags=false) const;
	RC				substitute(const Value *pv,unsigned np,MemAlloc *ma);
	RC				getPropDNF(struct PropDNF *&dnf,size_t& ldnf,MemAlloc *ma) const;
	bool			checkXPropID(PropertyID xp) const;
public:
	friend	class	Stmt;
	friend	class	DataEvent;
	friend	class	DataEventMgr;
	friend	class	DropDataEvent;
	friend	class	DataEventRegistry;
	friend	class	SInCtx;
	friend	class	SOutCtx;
	friend	RC		QVar::render(SOutCtx&) const;
	friend	RC		QVar::deserialize(const byte *&buf,const byte *const ebuf,MemAlloc *ma,QVar*& res);
};

union QVarRef 
{
	QVar	*var;
	QVarID	varID;
};

/**
 * set operation (UNION, INTERSECT, EXCEPT) variable
 */
class SetOpVar : public QVar
{
	friend	class	Stmt;
	friend	class	SInCtx;
	friend	RC		QVar::render(SOutCtx&) const;
	friend	RC		QVar::deserialize(const byte *&buf,const byte *const ebuf,MemAlloc *ma,QVar*& res);
	const unsigned	nVars;
	QVarRef			vars[2];
	void			*operator new(size_t s,unsigned nv,MemAlloc *m) {return m->malloc(s+int(nv-2)*sizeof(QVarRef));}
	SetOpVar(unsigned nv,QVarID i,byte ty,MemAlloc *m) : QVar(i,ty,m),nVars(nv) {}
	RC				clone(MemAlloc *m,QVar*&) const;
	RC				build(BuildCtx& qctx,QueryOp *&qop) const;
	RC				render(RenderPart,SOutCtx&) const;
	RC				render(SOutCtx&) const;
	size_t			serSize() const;
	byte			*serialize(byte *buf) const;
	static	RC		deserialize(const byte *&buf,const byte *const ebuf,QVarID,byte,MemAlloc *ma,QVar*& res);
};

/**
 * join variable
 */
class JoinVar : public QVar
{
	friend	class	Stmt;
	friend	class	SInCtx;
	friend	RC		QVar::deserialize(const byte *&buf,const byte *const ebuf,MemAlloc *ma,QVar*& res);
	const unsigned	nVars;
	CondEJ			*condEJ;
	QVarRef			vars[2];
	void			*operator new(size_t s,unsigned nv,MemAlloc *m) {return m->malloc(s+int(nv-2)*sizeof(QVarRef));}
	JoinVar(unsigned nv,QVarID i,byte ty,MemAlloc *m) : QVar(i,ty,m),nVars(nv),condEJ(NULL) {stype=SEL_COMPOUND;}
	virtual			~JoinVar();
	RC				clone(MemAlloc *m,QVar*&) const;
	RC				build(BuildCtx& qctx,QueryOp *&qop) const;
	RC				render(RenderPart,SOutCtx&) const;
	size_t			serSize() const;
	byte			*serialize(byte *buf) const;
	static	RC		deserialize(const byte *&buf,const byte *const ebuf,QVarID,byte,MemAlloc *ma,QVar*& res);
	const	QVar	*getRefVar(unsigned refN) const;
	unsigned		getVarIdx(unsigned refN,unsigned& sht) const;
};

/**
 * new PIN descriptor for INSERT statements
 */
struct PINDscr : public Values
{
	uint64_t	tpid;
	PINDscr() : tpid(0) {}
	PINDscr(const Value *pv,unsigned nv,uint64_t tp=0ULL,bool fF=false) : Values(pv,nv,fFree),tpid(tp) {}
};

/**
 * IStmt implementation
 */
class Stmt : public IStmt
{
	const	STMT_OP	op;			/**< statement operation */
	MemAlloc *const	ma;			/**< memory where this statement is allocated */
	SimpleStmtType	sst;		/**< Stmt subtype for quick processing of simple statements */
	unsigned		mode;		/**< execution mode */
	TXI_LEVEL		txi;		/**< statement isolation level */
	QVar			*top;		/**< topmost variable */
	unsigned		nTop;		/**< number of tompmost variables */
	QVar			*vars;		/**< list of all variables */
	unsigned		nVars;		/**< total number of variables */
	OrderSegQ		*orderBy;	/**< ORDER BY segment descriptors */
	unsigned		nOrderBy;	/**< number of ORDER BY segments */
	Values			with;		/**< statement parameters values, specified in WITH */
	union {
		PINDscr	*pins;			/**< new PINs descriptors */
		Value	*vals;			/**< modifiers for update */
	};
	unsigned		nValues;	/**< number of pins to insert or number update modifiers */
	unsigned		nNested;	/**< number of nested PINs to be inserterd */
	IntoClass		*into;		/**< class membership constraints */
	uint32_t		nInto;		/**< number of class membership constraints */
	unsigned		pmode;		/**< PIN creation flags or index in @... for UPDATE */
	uint64_t		tpid;		/**< root PIN id for cross-references in graph insert */
	unsigned		refCnt;		/**< number of references to this statement from a client program and cursors */
public:
	Stmt(unsigned md,MemAlloc *m,STMT_OP sop=STMT_QUERY,TXI_LEVEL tx=TXI_DEFAULT) : op(sop),ma(m),sst(SST_GENERIC),mode(md),txi(tx),top(NULL),nTop(0),
					vars(NULL),nVars(0),orderBy(NULL),nOrderBy(0),nValues(0),nNested(0),into(NULL),nInto(0),pmode(0),tpid(STORE_INVALID_PID),refCnt(1) {vals=NULL;}
	virtual	~Stmt();
	void operator	delete(void *p) throw() {if (p!=NULL) ((Stmt*)p)->ma->free(p);}
	QVarID	addVariable(const SourceSpec *srcs=NULL,unsigned nSrcs=0,IExprNode *cond=NULL);
	QVarID	addVariable(const PID& pid,PropertyID propID,IExprNode *cond=NULL);
	QVarID	addVariable(IStmt *qry);
	QVarID	setOp(QVarID leftVar,QVarID rightVar,QUERY_SETOP);
	QVarID	setOp(const QVarID *vars,unsigned nVars,QUERY_SETOP);
	QVarID	join(QVarID leftVar,QVarID rightVar,IExprNode *cond=NULL,QUERY_SETOP=QRY_SEMIJOIN,PropertyID=STORE_INVALID_URIID);
	QVarID	join(const QVarID *vars,unsigned nVars,IExprNode *cond=NULL,QUERY_SETOP=QRY_SEMIJOIN,PropertyID=STORE_INVALID_URIID);
	RC		setName(QVarID var,const char *name);
	RC		setDistinct(QVarID var,DistinctType dt);
	RC		addOutput(QVarID var,const Value *dscr,unsigned nDscr);
	RC		addOutputNoCopy(QVarID var,Value *dscr,unsigned nDscr);
	RC		addCondition(QVarID var,IExprNode *cond);
	RC		addConditionFT(QVarID var,const char *str,unsigned flags=0,const PropertyID *pids=NULL,unsigned nPids=0);
	RC		setPIDs(QVarID var,const PID *pids,unsigned nPids);
	RC		setPath(QVarID var,const PathSeg *segs,unsigned nSegs);
	RC		setExpr(QVarID var,const Value& exp);
	RC		setPropCondition(QVarID var,const PropertyID *props,unsigned nProps,bool fOr=false);
	RC		setJoinProperties(QVarID var,const PropertyID *props,unsigned nProps);
	RC		setGroup(QVarID,const OrderSeg *order,unsigned nSegs,IExprNode *having=NULL);
	RC		setOrder(const OrderSeg *order,unsigned nSegs);
	RC		setValues(const Value *values,unsigned nValues,const IntoClass *into=NULL,unsigned nInto=0,uint64_t tid=0ULL);
	RC		setWith(const Value *params,unsigned nParams);
	STMT_OP	getOp() const;
	RC		execute(ICursor **result=NULL,const Value *params=NULL,unsigned nParams=0,unsigned nReturn=~0u,unsigned nSkip=0,unsigned mode=0,uint64_t *nProcessed=NULL,TXI_LEVEL=TXI_DEFAULT);
	RC		asyncexec(IStmtCallback *cb,const Value *params=NULL,unsigned nParams=0,unsigned nProcess=~0u,unsigned nSkip=0,unsigned mode=0,TXI_LEVEL=TXI_DEFAULT);
	RC		execute(IStreamOut*& result,const Value *params=NULL,unsigned nParams=0,unsigned nReturn=~0u,unsigned nSkip=0,unsigned mode=0,TXI_LEVEL=TXI_DEFAULT);
	RC		count(uint64_t& cnt,const Value *params=NULL,unsigned nParams=0,unsigned nAbort=~0u,unsigned mode=0,TXI_LEVEL=TXI_DEFAULT) const;
	RC		exist(const Value *params=NULL,unsigned nParams=0,unsigned mode=0,TXI_LEVEL=TXI_DEFAULT) const;
	RC		analyze(char *&plan,const Value *pars=NULL,unsigned nPars=0,unsigned md=0) const;

	bool	isSatisfied(const IPIN *,const Value *pars=NULL,unsigned nPars=0,unsigned mode=0) const;
	RC		cmp(const EvalCtx& ectx,const Value& v,ExprOp op,unsigned flags);

	char	*toString(unsigned mode=0,const QName *qNames=NULL,unsigned nQNames=0) const;
	IStmt	*clone(STMT_OP=STMT_OP_ALL) const;
	Stmt	*clone(STMT_OP sop,MemAlloc *ma) const;
	void	trace(const EvalCtx& ectx,const char *op,RC rc,unsigned cnt) const;
	void	destroy();

	bool	hasParams() const {return (mode&QRY_PARAMS)!=0;}
	bool	isClassOK() const {return top!=NULL && classOK(top);}
	bool	checkConditions(const EvalCtx& ectx,unsigned start=0,bool fIgnore=false) const;
	RC		setPath(QVarID var,const PathSeg *segs,unsigned nSegs,bool fCopy);
	QVar	*getTop() const {return top;}
	unsigned getMode() const {return mode;}
	void	setVarFlags(QVarID var,byte flg);
	void	checkParams(const Value& v,bool fRecurs=false);
	
	RC		setValuesNoCopy(PINDscr *values,unsigned nVals);
	RC		setValuesNoCopy(Value *values,unsigned nVals);
	RC		setWithNoCopy(Value *params,unsigned nParams);
	RC		getNested(PIN **ppins,PIN *pins,uint64_t& cnt,Session *ses,PIN *parent=NULL) const;
	static	RC	getNested(const Value *pv,unsigned nV,PIN **ppins,PIN *pins,uint64_t& cnt,Session *ses,PIN *parent);
	RC		insert(const EvalCtx& ectx,Value *ids,uint64_t& cnt) const;
	RC		substitute(const Value *params,unsigned nParams,MemAlloc *ma);
	void	ref() {refCnt++;}

	RC		render(SOutCtx&) const;
	size_t	serSize() const;
	byte	*serialize(byte *buf) const;
	static	RC		deserialize(Stmt*&,const byte *&,const byte *const ebuf,MemAlloc*);
protected:
	RC		connectVars();
	RC		processCondition(class ExprNode*,QVar *qv);
	RC		processHaving(class ExprNode*,QVar *qv);
	RC		processCond(class ExprNode*,QVar *qv,DynArray<const class ExprNode*> *exprs);
	QVar	*findVar(QVarID id) const {QVar *qv=vars; while (qv!=NULL&&qv->id!=id) qv=qv->next; return qv;}
	RC		render(const QVar *qv,SOutCtx& out) const;
	RC		copyValues(Value *vals,unsigned nVals,unsigned& pn,DynOArrayBuf<uint64_t,uint64_t>& tids,Session *ses=NULL);
	RC		countNestedNoCopy(Value *vals,unsigned nVals);
	RC		execute(const EvalCtx& ectx,Value *res,unsigned nReturn=~0u,unsigned nSkip=0,unsigned md=0,uint64_t *nProcessed=NULL,TXI_LEVEL txl=TXI_DEFAULT,Cursor **pResult=NULL);
	static	bool	classOK(const QVar *);
	friend	class	DataEvent;
	friend	class	DataEventMgr;
	friend	class	DataEventRegistry;
	friend	struct	StreamWindow;
	friend	class	ProcessStream;
	friend	class	ServiceCtx;
	friend	class	QueryPrc;
	friend	class	Cursor;
	friend	class	SimpleVar;
	friend	class	SInCtx;
};


};

#endif
