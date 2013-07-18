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

/**
 * query builder data structures
 */
#ifndef _QBUILD_H_
#define _QBUILD_H_

#include "affinity.h"
#include "types.h"
#include "mem.h"

using namespace Afy;

namespace AfyKernel
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

#define	MODE_CLASS			0x00008000
#define	MODE_NODEL			0x00004000		
#define	MODE_NO_RINDEX		0x00002000
#define	MODE_COMPOUND		0x00002000
#define	MODE_CHECKBI		0x00002000
#define	MODE_CASCADE		0x00001000
#define	MODE_FSM			0x00001000

/**
 * internal Value::flags flags (maximum 0x40)
 */
#define	VF_PREFIX			0x08
#define	VF_REF				0x10
#define	VF_STRING			0x20
#define	VF_SSV				0x40

#define	SORT_MASK			(ORD_DESC|ORD_NCASE|ORD_NULLS_BEFORE|ORD_NULLS_AFTER)

struct	CondIdx;
struct	CondEJ;
struct	CondFT;
struct	EvalCtx;
class	Session;
class	QVar;
class	Stmt;
class	Expr;

#define	ORDER_EXPR	0x8000

/**
 * internal descriptor for ordering segment
 */
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
struct ValueV
{
	const Value	*vals;
	uint16_t	nValues;
	bool		fFree;
	ValueV() : vals(NULL),nValues(0),fFree(false) {}
	ValueV(const ValueV& vv) : vals(vv.vals),nValues(vv.nValues),fFree(false) {}
	ValueV(const Value *pv,unsigned nv,bool fF=false) : vals(pv),nValues((uint16_t)nv),fFree(fF) {}
};

/**
 * types of context variables (in Value.refV.flags)
 */
enum QCtxVT
{
	QV_PARAMS, QV_CORRELATED, QV_AGGS, QV_GROUP, QV_REXP, QV_SELF, QV_ALL
};

#define	VAR_CORR	((QV_CORRELATED+1)<<13)
#define	VAR_AGGS	((QV_AGGS+1)<<13)
#define	VAR_GROUP	((QV_GROUP+1)<<13)
#define	VAR_REXP	((QV_REXP+1)<<13)
#define	VAR_SELF	((QV_SELF+1)<<13)
#define	VAR_NAMED	((QV_ALL+1)<<13)

/**
 * query evaluation context
 * shared between query operators
 */
class	QCtx
{
	int		refc;
	void	operator	delete(void *) {}
public:
	QCtx(Session *s,EvalCtx *ect=NULL) : refc(0),ses(s),ectx(ect) {memset(vals,0,sizeof(vals));}
	Session	*const	ses;
	const	EvalCtx	*ectx;
	ValueV	vals[QV_ALL];
	void	ref() {refc++;}
	void	destroy();
	friend	class	QBuildCtx;
	friend	class	Stmt;
};

// used in filter, sort, etc. operators
struct QueryWithParams
{
	Stmt		*qry;
	unsigned	nParams;
	Value		*params;
};

/**
 * query builder context
 */
class QBuildCtx
{
	Session			*const	ses;
	QCtx			*const	qx;
	const Stmt				*stmt;
	unsigned				nSkip;
	unsigned				mode;
	unsigned				flg;
	PropListP				propsReq;
	const OrderSegQ			*sortReq;
	unsigned				nSortReq;
	class	QueryOp			*src[256];
	unsigned				nqs;
	QueryWithParams			condQs[256];
	unsigned				ncqs;
public:
	QBuildCtx(Session *s,EvalCtx *ectx,const Stmt *st,unsigned nsk,unsigned f);
	~QBuildCtx();
	RC	process(QueryOp *&qop);
private:
	RC	sort(QueryOp *&qop,const OrderSegQ *os,unsigned no,PropListP *props=NULL,bool fTmp=false);
	RC	mergeN(QueryOp *&res,QueryOp **o,unsigned no,QUERY_SETOP op);
	RC	merge2(QueryOp *&res,QueryOp **qs,const CondEJ *cej,QUERY_SETOP qo,const Expr *const *conds=NULL,unsigned nConds=0);
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
