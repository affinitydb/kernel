/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _QBUILD_H_
#define _QBUILD_H_

#include "mvstore.h"
#include "types.h"

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

struct	OrderSegQ;
struct	PropDNF;
struct	CondIdx;
struct	CondEJ;
struct	CondFT;
class	Session;
class	Stmt;
class	Expr;

// Filter, sort, etc. operators
struct QueryWithParams
{
	Stmt		*qry;
	unsigned	nParams;
	Value		*params;
};

class QueryCtx
{
	Session			*const	ses;
	const Value		*const	pars;
	const unsigned			nPars;
	const Stmt				*stmt;
	ulong					nSkip;
	ulong					mode;
	ulong					flg;
	const PropertyID		*propsReq;
	unsigned				nPropsReq;
	const OrderSegQ			*sortReq;
	unsigned				nSortReq;
	class	QueryOp			*src[256];
	ulong					nqs;
	QueryWithParams			condQs[256];
	ulong					ncqs;
public:
	QueryCtx(Session *s,const Value *prs,unsigned nprs,const Stmt *st,ulong nsk,ulong f);
	~QueryCtx();
	RC	process(QueryOp *&qop);
private:
	RC	sort(QueryOp *&qop,struct QODescr& dscr,const OrderSegQ *os,unsigned no);
	RC	mergeN(QueryOp *&res,QueryOp **o,unsigned no,bool fOr);
	RC	merge2(QueryOp *&res,QueryOp *qop1,PropertyID pid1,QueryOp *qop2,PropertyID pid2,
				QUERY_SETOP qo,const CondEJ *cej=NULL,const Expr *const *c=NULL,ulong nc=0);
	RC	mergeFT(QueryOp *&res,const CondFT *cft);
	RC	filter(QueryOp *&qop,const Expr *const *c,unsigned nConds,const PropDNF *condProps,size_t lProps,const CondIdx *condIdx=NULL,unsigned ncq=0);
	RC	group(QueryOp *&qop,const OrderSegQ *gb,unsigned nG,const Value *trs,unsigned nTrs,const Expr *const *c,unsigned nConds);
	friend	class	SimpleVar;
	friend	class	SetOpVar;
	friend	class	JoinVar;
};

};

#endif
