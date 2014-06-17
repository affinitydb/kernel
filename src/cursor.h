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
 * ICursor interface implementation
 */
#ifndef _CURSOR_H_
#define _CURSOR_H_

#include "expr.h"
#include "idxtree.h"
#include "txmgr.h"
#include "pinex.h"
#include "queryop.h"

using namespace Afy;

namespace AfyKernel
{

class Stmt;

class Cursor : public ICursor
{
	StackAlloc			alloc;
	EvalCtx				ectx;
	Values				vctx[QV_ALL];
	Stmt				*stmt;
	STMT_OP				op;
	SelectType			stype;
	QueryOp				*queryOp;
	uint64_t			nReturn;
	unsigned			mode;
	PINx				**results;
	unsigned			nResults;
	PINx				qr,*pqr;
	TXID				txid;
	TXCID				txcid;
	uint64_t			cnt;
	TxSP				tx;
	ValueC				retres;
	bool				fSnapshot;
	bool				fProc;
	bool				fAdvance;
private:
	RC					skip();
	RC					advance(bool fRet=true);
	void				getPID(PID &id) {qr.getID(id);}
	RC					extract(MemAlloc *ma,PIN *&,unsigned idx=0,bool fCopy=false);
	RC					connect();
public:
	Cursor(const EvalCtx &ec,EvalCtxType=ECT_QUERY);
	Cursor(Session *ses);
	virtual				~Cursor();
	RC					init(Stmt *st,uint64_t nRet,unsigned nSkip,unsigned md,bool fSS=false);
	RC					init(Value& v,uint64_t nRet,unsigned nSkip,unsigned md,bool fSS=false);
	RC					init(DataEventID dev=STORE_INVALID_URIID,unsigned md=0);

	RC					next(Value&);
	RC					next(PID&);
	IPIN				*next();
	RC					rewind();
	uint64_t			getCount() const;
	void				destroy();

	Session				*getSession() const {return ectx.ses;}
	unsigned			getNResults() const {return nResults;}
	EvalCtx&			getCtx() const {return (EvalCtx&)ectx;}
	SelectType			selectType() const {return stype;}
	RC					next(PINx *&,bool fExtract=false);
	RC					rewindInt();
	RC					count(uint64_t& cnt,unsigned nAbort,const OrderSegQ *os=NULL,unsigned nos=0);

	friend	struct		BuildCtx;
	friend	class		CursorNav;
	friend	class		SimpleVar;
	friend	class		SetOpVar;
	friend	class		JoinVar;
	friend	class		Stmt;
};

struct BuildCtx {
	Cursor&				cu;
	unsigned			flg;
	PropListP			propsReq;
	const OrderSegQ		*sortReq;
	unsigned			nSortReq;
	class	QueryOp		*src[256];
	unsigned			nqs;
	QueryWithParams		condQs[256];
	unsigned			ncqs;
public:
	BuildCtx(Cursor&);
	RC	sort(QueryOp *&qop,const OrderSegQ *os,unsigned no,PropListP *props=NULL,bool fTmp=false);
	RC	mergeN(QueryOp *&res,QueryOp **o,unsigned no,QUERY_SETOP op);
	RC	merge2(QueryOp *&res,QueryOp **qs,const CondEJ *cej,QUERY_SETOP op,const Expr *cond=NULL);
	RC	mergeFT(QueryOp *&res,const struct CondFT *cft);
	RC	nested(QueryOp *&res,QueryOp **qs,const Expr *cond);
	RC	filter(QueryOp *&qop,const Expr *c,const PropertyID *props=NULL,unsigned nProps=0,const CondIdx *condIdx=NULL,unsigned ncq=0);
	RC	load(QueryOp *&qop,const PropListP& plp);
	RC	out(QueryOp *&qop,const class QVar *qv);
	static	bool	checkSort(QueryOp *qop,const OrderSegQ *req,unsigned nReq,unsigned& nP);
};


/**
 * query result set representation as a collection
 */
class CursorNav : public INav
{
	MemAlloc	*const	ma;
	Cursor				*curs;
	const	Value		*cv;
	unsigned			idx;
	DynArray<Value,8>	vals;
	const	bool		fPID;
	bool				fColl;
public:
	CursorNav(MemAlloc *m,Cursor *cu,bool fP) : ma(m),curs(cu),cv(NULL),idx(0),vals(m),fPID(fP),fColl(false) {}
	~CursorNav();
	const	Value	*navigate(GO_DIR=GO_NEXT,ElementID=STORE_COLLECTION_ID);
	ElementID		getCurrentID();
	const	Value	*getCurrentValue();
	RC				getElementByID(ElementID,Value&);
	INav			*clone() const;
	unsigned		count() const;
	void			destroy();
	static	RC		create(Cursor *cr,Value& res,unsigned md,MemAlloc *ma);
private:
	static	RC		getNext(Cursor *curs,Value& v,MemAlloc *ma,bool fPID);
};

}

#endif
