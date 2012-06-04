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
 * IStmt and ICursor interfaces implementation
 */
#ifndef _QUERY_H_
#define _QUERY_H_

#include "affinityimpl.h"
#include "session.h"
#include "idxtree.h"
#include "txmgr.h"
#include "pinex.h"

using namespace AfyDB;

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
#define	QRY_PATH		0x08000000			/**< query contains path expressions */

class SOutCtx;
class SInCtx;

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
 * types of SELECT lists
 */
enum SelectType
{
	SEL_CONST,			/**< select returns constant expressions */
	SEL_COUNT,			/**< SELECT COUNT(*) ... */
	SEL_VALUE,			/**< SELECT expr, aggregation is used, one result is returned */
	SEL_VALUESET,		/**< SELECT expr, no aggregation */
	SEL_DERIVED,		/**< SELECT list of expressions, aggregation is used, one result is returned */
	SEL_DERIVEDSET,		/**< SELECT list of expressions, no aggregation */
	SEL_AUGMENTED,		/**< SELECT @,... */
	SEL_PINSET,			/**< SELECT * for non-join query */
	SEL_COMPOUND,		/**< SELECT * for join query */
	SEL_COMP_DERIVED	/**< SELECT list of expressions for join query */
};

/**
 * extra parameters used in STMT_INSERT and STMT_UPDATE
 */
struct ExtraInfo
{
	Value			*values;	/**< values of a new PIN or modifiers for update */
	unsigned		nValues;	/**< number of values for insert or modifiers for update */
	unsigned		nNested;	/**< number of nested PINs to be inserterd */
	ClassSpec		*into;		/**< class membership constraints */
	unsigned		nInto;		/**< number of class membership constraints */
	unsigned		mode;		/**< PIN creation flags */
	uint64_t		id;			/**< root PIN id for cross-references in graph insert */
	ExtraInfo() : values(NULL),nValues(0),nNested(0),into(NULL),nInto(0),mode(0),id(~0ULL) {}
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
	DistinctType	dtype;
	MemAlloc *const	ma;
	char			*name;
	ValueV			*outs;
	unsigned		nOuts;
	union {
		Expr		*cond;
		Expr		**conds;
	};
	unsigned		nConds;
	Expr			*having;
	OrderSegQ		*groupBy;
	unsigned		nGroupBy;
	ValueV			aggrs;
	bool			fHasParent;
	QVar(QVarID i,byte ty,MemAlloc *m);
public:
	virtual			~QVar();
	virtual	RC		clone(MemAlloc *m,QVar*&,bool fClass) const = 0;
	virtual	RC		build(class QBuildCtx& qctx,class QueryOp *&qop) const = 0;
	virtual	RC		render(RenderPart,SOutCtx&) const;
	virtual	RC		render(SOutCtx&) const;
	virtual	size_t	serSize() const;
	virtual	byte	*serialize(byte *buf) const = 0;
	virtual	const	QVar *getRefVar(unsigned refN) const;
	virtual	RC		mergeProps(PropListP& plp,bool fForce=false,bool fFlags=false) const;
	static	RC		deserialize(const byte *&buf,const byte *const ebuf,MemAlloc *ma,QVar*& res);
	void	operator delete(void *p) {if (p!=NULL) ((QVar*)p)->ma->free(p);}
	QVarID			getID() const {return id;}
	byte			getType() const {return type;}
	bool			isMulti() const {return type<QRY_ALL_SETOP;}
	RC				clone(QVar *cloned,bool fClass=false) const;
	byte			*serQV(byte *buf) const;
	RC				addPropRefs(const PropertyID *props,unsigned nProps);
	friend	class	Stmt;
	friend	class	SimpleVar;
	friend	class	Classifier;
	friend	class	ClassDelTx;
	friend	class	ClassPropIndex;
	friend	class	QueryPrc;
	friend	class	QBuildCtx;
	friend	class	SInCtx;
	friend	class	SOutCtx;
};

/**
 * simple query variable - no join, no set operation
 * can refer a list of classes or class families, individual PINs, collection, path expression, subquery
 */
class SimpleVar : public QVar
{
	ClassSpec		*classes;
	ulong			nClasses;
	CondIdx			*condIdx;
	CondIdx			*lastCondIdx;
	unsigned		nCondIdx;
	PID				*pids;
	unsigned		nPids;
	Stmt			*subq;
	CondFT			*condFT;
	bool			fOrProps;
	PathSeg			*path;
	unsigned		nPathSeg;
	SimpleVar(QVarID i,MemAlloc *m) : QVar(i,QRY_SIMPLE,m),classes(NULL),nClasses(0),condIdx(NULL),lastCondIdx(NULL),nCondIdx(0),
									pids(NULL),nPids(0),subq(NULL),condFT(NULL),fOrProps(false),path(NULL),nPathSeg(0) {}
	virtual			~SimpleVar();
	RC				clone(MemAlloc *m,QVar*&,bool fClass) const;
	RC				build(class QBuildCtx& qctx,class QueryOp *&qop) const;
	RC				render(RenderPart,SOutCtx&) const;
	size_t			serSize() const;
	byte			*serialize(byte *buf) const;
	static	RC		deserialize(const byte *&buf,const byte *const ebuf,QVarID,MemAlloc *ma,QVar*& res);
	RC				mergeProps(PropListP& plp,bool fForce=false,bool fFlags=false) const;
	RC				getPropDNF(PropDNF *&dnf,size_t& ldnf,MemAlloc *ma) const;
	bool			checkXPropID(PropertyID xp) const;
public:
	friend	class	Stmt;
	friend	class	Class;
	friend	class	Classifier;
	friend	class	ClassDelTx;
	friend	class	ClassPropIndex;
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
	RC				clone(MemAlloc *m,QVar*&,bool fClass) const;
	RC				build(class QBuildCtx& qctx,class QueryOp *&qop) const;
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
	RC				clone(MemAlloc *m,QVar*&,bool fClass) const;
	RC				build(class QBuildCtx& qctx,class QueryOp *&qop) const;
	RC				render(RenderPart,SOutCtx&) const;
	size_t			serSize() const;
	byte			*serialize(byte *buf) const;
	static	RC		deserialize(const byte *&buf,const byte *const ebuf,QVarID,byte,MemAlloc *ma,QVar*& res);
	const	QVar	*getRefVar(unsigned refN) const;
};

/**
 * IStmt implementation
 */
class Stmt : public IStmt
{
	const	STMT_OP	op;
	ulong			mode;
	MemAlloc		*const	ma;
	QVar			*top;
	unsigned		nTop;
	QVar			*vars;
	unsigned		nVars;
	OrderSegQ		*orderBy;
	unsigned		nOrderBy;
	ExtraInfo		*ei;
public:
	Stmt(ulong md,MemAlloc *m,STMT_OP sop=STMT_QUERY) : op(sop),mode(md),ma(m),top(NULL),nTop(0),vars(NULL),nVars(0),orderBy(NULL),nOrderBy(0),ei(NULL) {}
	virtual	~Stmt();
	QVarID	addVariable(const ClassSpec *classes=NULL,unsigned nClasses=0,IExprTree *cond=NULL);
	QVarID	addVariable(const PID& pid,PropertyID propID,IExprTree *cond=NULL);
	QVarID	addVariable(IStmt *qry);
	QVarID	setOp(QVarID leftVar,QVarID rightVar,QUERY_SETOP);
	QVarID	setOp(const QVarID *vars,unsigned nVars,QUERY_SETOP);
	QVarID	join(QVarID leftVar,QVarID rightVar,IExprTree *cond=NULL,QUERY_SETOP=QRY_SEMIJOIN,PropertyID=STORE_INVALID_PROPID);
	QVarID	join(const QVarID *vars,unsigned nVars,IExprTree *cond=NULL,QUERY_SETOP=QRY_SEMIJOIN,PropertyID=STORE_INVALID_PROPID);
	RC		setName(QVarID var,const char *name);
	RC		setDistinct(QVarID var,DistinctType dt);
	RC		addOutput(QVarID var,const Value *dscr,unsigned nDscr);
	RC		addOutputNoCopy(QVarID var,Value *dscr,unsigned nDscr);
	RC		addCondition(QVarID var,IExprTree *cond);
	RC		addConditionFT(QVarID var,const char *str,unsigned flags=0,const PropertyID *pids=NULL,unsigned nPids=0);
	RC		setPIDs(QVarID var,const PID *pids,unsigned nPids);
	RC		setPath(QVarID var,const PathSeg *segs,unsigned nSegs);
	RC		setPropCondition(QVarID var,const PropertyID *props,unsigned nProps,bool fOr=false);
	RC		setJoinProperties(QVarID var,const PropertyID *props,unsigned nProps);
	RC		setGroup(QVarID,const OrderSeg *order,unsigned nSegs,IExprTree *having=NULL);
	RC		setOrder(const OrderSeg *order,unsigned nSegs);
	RC		setValues(const Value *values,unsigned nValues,const ClassSpec *into=NULL,unsigned nInto=0,unsigned mode=0);
	RC		setValuesNoCopy(Value *values,unsigned nValues,ClassSpec *into=NULL,unsigned nInto=0,unsigned mode=0);
	STMT_OP	getOp() const;
	RC		execute(ICursor **result=NULL,const Value *params=NULL,unsigned nParams=0,unsigned nReturn=~0u,unsigned nSkip=0,unsigned long mode=0,uint64_t *nProcessed=NULL,TXI_LEVEL=TXI_DEFAULT) const;
	RC		asyncexec(IStmtCallback *cb,const Value *params=NULL,unsigned nParams=0,unsigned nProcess=~0u,unsigned nSkip=0,unsigned long mode=0,TXI_LEVEL=TXI_DEFAULT) const;
	RC		execute(IStreamOut*& result,const Value *params=NULL,unsigned nParams=0,unsigned nReturn=~0u,unsigned nSkip=0,unsigned long mode=0,TXI_LEVEL=TXI_DEFAULT) const;
	RC		insert(Session *ses,PID& id,unsigned& cnt,const Value *pars=NULL,unsigned nPars=0) const;
	RC		count(uint64_t& cnt,const Value *params=NULL,unsigned nParams=0,unsigned long nAbort=~0u,unsigned long mode=0,TXI_LEVEL=TXI_DEFAULT) const;
	RC		exist(const Value *params=NULL,unsigned nParams=0,unsigned long mode=0,TXI_LEVEL=TXI_DEFAULT) const;
	RC		analyze(char *&plan,const Value *pars=NULL,unsigned nPars=0,unsigned long md=0) const;

	bool	isSatisfied(const IPIN *,const Value *pars=NULL,unsigned nPars=0,unsigned long mode=0) const;
	RC		cmp(const Value& v,ExprOp op,unsigned flags);

	char	*toString(unsigned mode=0,const QName *qNames=NULL,unsigned nQNames=0) const;
	IStmt	*clone(STMT_OP=STMT_OP_ALL) const;
	Stmt	*clone(STMT_OP sop,MemAlloc *ma,bool fClass=false) const;
	void	trace(Session *ses,const char *op,RC rc,ulong cnt,const Value *pars,unsigned nPars,const Value *mods=NULL,unsigned nMods=0) const;
	void	destroy();

	bool	hasParams() const {return (mode&QRY_PARAMS)!=0;}
	bool	isClassOK() const {return top!=NULL && classOK(top);}
	bool	checkConditions(PINEx *pin,const ValueV& pars,MemAlloc *ma,ulong start=0,bool fIgnore=false) const;
	RC		render(SOutCtx&) const;
	RC		setPath(QVarID var,const PathSeg *segs,unsigned nSegs,bool fCopy);
	QVar	*getTop() const {return top;}

	size_t	serSize() const;
	byte	*serialize(byte *buf) const;
	static	RC	deserialize(Stmt*&,const byte *&,const byte *const ebuf,MemAlloc*);
private:
	RC		connectVars();
	RC		processCondition(class ExprTree*,QVar *qv);
	RC		processHaving(class ExprTree*,QVar *qv);
	RC		processCond(class ExprTree*,QVar *qv,DynArray<const class ExprTree*> *exprs);
	QVar	*findVar(QVarID id) const {QVar *qv=vars; while (qv!=NULL&&qv->id!=id) qv=qv->next; return qv;}
	RC		render(const QVar *qv,SOutCtx& out) const;
	RC		getNested(PIN **ppins,PIN *pins,unsigned& cnt,Session *ses,PIN *parent=NULL) const;
	static	bool	classOK(const QVar *);
	friend class	Class;
	friend class	Classifier;
	friend	class	ClassDelTx;
	friend class	ClassPropIndex;
	friend class	QueryPrc;
	friend class	QBuildCtx;
	friend class	SimpleVar;
	friend class	SInCtx;
};

/**
 * ICursor implementation
 */
class Cursor : public ICursor
{
	friend	class		Stmt;
	QueryOp				*queryOp;
	Session	*const		ses;
	const	uint64_t	nReturn;
	const	Value		*values;
	const	unsigned	nValues;
	const	ulong		mode;
	const	SelectType	stype;
	const	STMT_OP		op;
	PINEx				**results;
	unsigned			nResults;
	PINEx				qr,*pqr;
	TXID				txid;
	TXCID				txcid;
	uint64_t			cnt;
	TxSP				tx;
	bool				fSnapshot;
	bool				fProc;
	bool				fAdvance;
	void	operator	delete(void *p) {if (p!=NULL) ((Cursor*)p)->ses->free(p);}
	RC					skip();
	RC					advance(bool fRet=true);
	void				getPID(PID &id) {qr.getID(id);}
	RC					extract(PIN *&,unsigned idx=0,bool fCopy=false);
public:
	Cursor(QueryOp *qop,uint64_t nRet,ulong md,const Value *vals,unsigned nV,Session *s,STMT_OP sop=STMT_QUERY,SelectType ste=SEL_PINSET,bool fSS=false)
		: queryOp(qop),ses(s),nReturn(nRet),values(vals),nValues(nV),mode(md),stype(ste),op(sop),results(NULL),nResults(0),qr(s),pqr(&qr),
		txid(INVALID_TXID),txcid(NO_TXCID),cnt(0),tx(s),fSnapshot(fSS),fProc(false),fAdvance(true) {}
	virtual				~Cursor();
	RC					next(Value&);
	RC					next(PID&);
	IPIN				*next();
	RC					rewind();
	uint64_t			getCount() const;
	void				destroy();

	RC					connect();
	SelectType			selectType() const {return stype;}
	Session				*getSession() const {return ses;}
	RC					next(const PINEx *&);
	friend	class		CursorNav;
};

/**
 * query result set representation as a collection
 */
class CursorNav : public INav
{
	Cursor	*const	curs;
	Value			v;
public:
	CursorNav(Cursor *cu) : curs(cu) {v.setError();}
	~CursorNav()	{freeV(v); if (curs!=NULL) curs->destroy();}
	const	Value	*navigate(GO_DIR=GO_NEXT,ElementID=STORE_COLLECTION_ID);
	ElementID		getCurrentID();
	const	Value	*getCurrentValue();
	RC				getElementByID(ElementID,Value&);
	INav			*clone() const;
	unsigned long	count() const;
	void			destroy();
};

};

#endif
