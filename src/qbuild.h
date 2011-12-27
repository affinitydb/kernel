/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _QBUILD_H_
#define _QBUILD_H_

#include "mvstore.h"
#include "types.h"
#include "mem.h"

using namespace MVStore;

namespace MVStoreKernel
{
/**
 * loadPIN/loadValues/loadV flags
 */
#define	LOAD_CARDINALITY	0x8000
#define	LOAD_EXT_ADDR		0x4000
#define	LOAD_SSV			0x2000
#define	LOAD_REF			0x1000
#define	LOAD_COLLECTION		0x0800
#define	LOAD_ENAV			0x0400

/**
 * Internal MODE_* flags
 */
#define	MODE_WITH_EVAL		0x80000000
#define	MODE_PREFIX_READ	0x40000000
#define	MODE_OLDLEN			0x20000000
#define	MODE_REFRESH		0x10000000
#define	MODE_COUNT			0x08000000

#define	MODE_CLASS			0x00008000
#define	MODE_NODEL			0x00004000
#define	MODE_NO_RINDEX		0x00002000
#define	MODE_CASCADE		0x00001000

#define	VF_SSV				0x04
#define	VF_PREFIX			0x08
#define	VF_REF				0x10
#define	VF_STRING			0x20

#define	SORT_MASK			(ORD_DESC|ORD_NCASE|ORD_NULLS_BEFORE|ORD_NULLS_AFTER)

struct	CondIdx;
struct	CondEJ;
struct	CondFT;
class	Session;
class	QVar;
class	Stmt;
class	Expr;

#define	ORDER_EXPR	0x8000

struct OrderSegQ
{
	union {
		PropertyID	pid;
		class Expr	*expr;
	};
	uint16_t		flags;
	uint8_t			var;
	uint8_t			aggop;
	uint32_t		lPref;
	RC				conv(const OrderSeg& sg,MemAlloc *ma);
};

struct PropList
{
	PropertyID		*props;
	uint16_t		nProps;
	mutable	bool	fFree;
};

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

struct ValueV
{
	const Value	*vals;
	uint16_t	nValues;
	bool		fFree;
	ValueV() : vals(NULL),nValues(0),fFree(false) {}
	ValueV(const ValueV& vv) : vals(vv.vals),nValues(vv.nValues),fFree(false) {}
	ValueV(const Value *pv,unsigned nv,bool fF=false) : vals(pv),nValues((uint16_t)nv),fFree(fF) {}
};

enum QCtxVT
{
	QV_PARAMS, QV_CORRELATED, QV_AGGS, QV_GROUP, QV_ALL
};

#define	VAR_CORR	((QV_CORRELATED+1)<<13)
#define	VAR_AGGS	((QV_AGGS+1)<<13)
#define	VAR_GROUP	((QV_GROUP+1)<<13)

class	QCtx
{
	int		refc;
	void	operator	delete(void *) {}
public:
	QCtx(Session *s) : refc(0),ses(s) {memset(vals,0,sizeof(vals));}
	Session	*const	ses;
	ValueV	vals[QV_ALL];
	void	ref() {refc++;}
	void	destroy();
	friend	class	QBuildCtx;
	friend	class	Stmt;
};

// Filter, sort, etc. operators
struct QueryWithParams
{
	Stmt		*qry;
	unsigned	nParams;
	Value		*params;
};

class QBuildCtx
{
	Session			*const	ses;
	QCtx			*const	qx;
	const Stmt				*stmt;
	ulong					nSkip;
	ulong					mode;
	ulong					flg;
	PropListP				propsReq;
	const OrderSegQ			*sortReq;
	unsigned				nSortReq;
	class	QueryOp			*src[256];
	ulong					nqs;
	QueryWithParams			condQs[256];
	ulong					ncqs;
public:
	QBuildCtx(Session *s,const ValueV& prs,const Stmt *st,ulong nsk,ulong f);
	~QBuildCtx();
	RC	process(QueryOp *&qop);
private:
	RC	sort(QueryOp *&qop,const OrderSegQ *os,unsigned no,PropListP *props=NULL,bool fTmp=false);
	RC	mergeN(QueryOp *&res,QueryOp **o,unsigned no,bool fOr);
	RC	merge2(QueryOp *&res,QueryOp **qs,const CondEJ *cej,QUERY_SETOP qo);
	RC	mergeFT(QueryOp *&res,const CondFT *cft);
	RC	nested(QueryOp *&res,QueryOp **qs,const Expr **conds,unsigned nConds);
	RC	filter(QueryOp *&qop,const Expr *const *c,unsigned nConds,const CondIdx *condIdx=NULL,unsigned ncq=0);
	RC	load(QueryOp *&qop,const PropListP& plp);
	RC	out(QueryOp *&qop,const QVar *qv);
	static	bool	checkSort(QueryOp *qop,const OrderSegQ *req,unsigned nReq,unsigned& nP);
	friend	class	SimpleVar;
	friend	class	SetOpVar;
	friend	class	JoinVar;
};

};

#endif
