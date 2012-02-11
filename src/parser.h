/**************************************************************************************

Copyright © 2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2010

**************************************************************************************/

#ifndef _PARSER_H_
#define _PARSER_H_

#include "mvstore.h"
#include "session.h"

using namespace MVStore;

class IMapDir;
struct StartupParameters;

namespace MVStoreKernel
{

enum SQType
{
	SQ_SQL, SQ_SPARQL
};

#undef	_F
#undef	_U
#undef	_C
#undef	_I
#undef	_A

#define	_F	0x01
#define	_U	0x02
#define	_C	0x04
#define	_I	0x08
#define	_A	0x10

struct OpDscr {
	ushort		flags;
	byte		match;
	byte		lstr;
	const char	*str;
	byte		prty[2];
	byte		nOps[2];
};

class QVar;

#define	SOM_VAR_NAMES	0x0001
#define	SOM_AND			0x0002
#define	SOM_WHERE		0x0004
#define	SOM_MATCH		0x0008
#define	SOM_STD_PREFIX	0x0010
#define	SOM_BASE_USED	0x0020

enum JRType
{
	JR_NO, JR_PROP, JR_EID
};

class SOutCtx
{
	Session	*const		ses;
	const	unsigned	mode;
	const	QVar		*cvar;
	const	QName		*const	qNames;
	const	unsigned	nQNames;
	unsigned			*usedQNames;
	unsigned			nUsedQNames;
	char				cbuf[100];
	ulong				flags;
	byte				*ptr;
	size_t				cLen;
	size_t				xLen;
public:
	SOutCtx(Session *s,unsigned md=0,const QName *qn=NULL,unsigned nQN=0) : ses(s),mode(md),cvar(NULL),
												qNames(qn),nQNames(nQN),usedQNames(NULL),nUsedQNames(0),flags(0),ptr(NULL),cLen(0),xLen(0) {}
	~SOutCtx() {if (usedQNames!=NULL) ses->free(usedQNames); if (ptr!=NULL) ses->free(ptr);}
	Session	*getSession() const {return ses;}
	RC		renderName(uint32_t id,const char *prefix=NULL,const QVar *qv=NULL,bool fQ=false,bool fEsc=true);
	RC		renderPID(const PID& id,bool fJSon=false);
	RC		renderValue(const Value& v,JRType jr=JR_NO);
	RC		renderVarName(const QVar *qv);
	RC		renderRep(unsigned rm,unsigned rx);
	RC		renderPath(const struct PathSeg& ps);
	char	*renderAll();
	RC		renderJSON(class Cursor *cr,uint64_t& cnt);
	size_t	getCLen() const {return cLen;}
	bool	append(const void *p,size_t l) {if (l==0) return true; byte *pp=alloc(l); return pp==NULL?false:(memcpy(pp,p,l),true);}
	bool	insert(const void *p,size_t l,size_t pos) {
		if (l==0) return true; size_t cl=cLen; byte *pp=alloc(l); if (pp==NULL) return false; 
		if (pos>=cl) memcpy(pp,p,l); else {memmove(ptr+pos+l,ptr+pos,cl-pos); memcpy(ptr+pos,p,l);} return true;
	}
	bool	fill(char c,int n) {if (n==0) return true; byte *pp=alloc(n); return pp==NULL?false:(memset(pp,c,n),true);}
	byte	*result(size_t& len) {byte *p=ptr; ptr=NULL; len=cLen; return cLen<xLen?(byte*)ses->realloc(p,cLen):p;}
	operator char*() {char *p=(char*)ptr; ptr=NULL; if (cLen+1!=xLen) p=(char*)ses->realloc(p,cLen+1); if (p!=NULL) p[cLen]=0; return p;}
	byte	*alloc(size_t l) {if (cLen+l>xLen && !expand(l)) return NULL; assert(ptr!=NULL); byte *p=ptr+cLen; cLen+=l; return p;}
	bool	expand(size_t l);
	friend	class	Stmt;
	friend	class	QVar;
	friend	class	JoinVar;
	friend	class	SimpleVar;
	friend	class	SInCtx;
};

#define	SIM_SIMPLE_NAME		0x0001
#define	SIM_NO_BASE			0x0002
#define	SIM_STD_OVR			0x0004
#define	SIM_INT_NUM			0x0008
#define	SIM_SELECT			0x0010
#define	SIM_DML_EXPR		0x0020
#define	SIM_HAVING			0x0040

enum Lexem {
	LX_BOE=OP_SET, LX_EOE=OP_ADD, LX_IDENT=OP_ADD_BEFORE, LX_CON=OP_MOVE,
	LX_KEYW=OP_MOVE_BEFORE, LX_LPR=OP_DELETE, LX_RPR=OP_EDIT, LX_COMMA=OP_RENAME,
		
	LX_COLON=OP_ALL, LX_LBR, LX_RBR, LX_LCBR, LX_RCBR, LX_PERIOD, LX_QUEST, LX_EXCL,
	LX_EXPR, LX_QUERY, LX_URSHIFT, LX_PREFIX, LX_IS, LX_BETWEEN, LX_BAND, LX_HASH,
	LX_SEMI, LX_CONCAT, LX_FILTER, LX_REPEAT, LX_PATHQ, LX_SELF, LX_SPROP,
		
	LX_NL, LX_SPACE, LX_DQUOTE, LX_QUOTE, LX_LT, LX_GT, LX_VL, LX_DOLLAR,
	LX_ERR, LX_UTF8, LX_DIGIT, LX_LETTER, LX_ULETTER, LX_XLETTER,
};

enum KW {
	KW_SELECT, KW_FROM, KW_WHERE, KW_ORDER, KW_BY, KW_GROUP, KW_HAVING, KW_JOIN, KW_ON,
	KW_LEFT, KW_OUTER, KW_RIGHT, KW_CROSS, KW_INNER, KW_USING, KW_BASE, KW_PREFIX,
	KW_UNION, KW_EXCEPT, KW_INTERSECT, KW_DISTINCT, KW_VALUES, KW_AS, KW_OP, KW_NULLS,
	KW_TRUE, KW_FALSE, KW_NULL, KW_ALL, KW_ANY, KW_SOME, KW_ASC, KW_DESC, KW_MATCH,
	KW_NOW, KW_CUSER, KW_CSTORE, KW_TIMESTAMP, KW_INTERVAL, KW_WITH,
	KW_INSERT, KW_DELETE, KW_UPDATE, KW_CREATE, KW_PURGE, KW_UNDELETE,
	KW_SET, KW_ADD, KW_MOVE, KW_RENAME, KW_EDIT, KW_CLASS, KW_START, KW_COMMIT,
	KW_ROLLBACK, KW_DROP
};

enum SynErr
{
	SY_INVCHR, SY_INVUTF8, SY_INVUTF8N, SY_MISQUO, SY_MISDQU, SY_INVHEX, SY_INVNUM,
	SY_MISPID, SY_INVURN, SY_MISNUM, SY_TOOBIG, SY_MISTYN, SY_MISRPR, SY_MISRBR, SY_MISRCBR,
	SY_INVTYN, SY_MISCOMEND, SY_INVPROP, SY_INVIDENT, SY_MISPROP, SY_MISIDENT, SY_MISPARN,
	SY_NOTSUP, SY_MISLPR, SY_MISCON, SY_MISMATCH, SY_FEWARG, SY_MANYARG, SY_EMPTY,
	SY_INVCOM, SY_MISCOM, SY_MISBIN, SY_MISSEL, SY_UNKVAR, SY_INVVAR, SY_INVCOND,
	SY_MISBY, SY_SYNTAX, SY_MISLGC, SY_MISAND, SY_INVTMS, SY_MISQNM, SY_MISQN2, SY_INVEXT,
	SY_UNBRPR, SY_UNBRBR, SY_UNBRCBR, SY_MISBOOL, SY_MISJOIN, SY_MISNAME, SY_MISAGNS,
	SY_MISEQ, SY_MISCLN, SY_UNKQPR, SY_INVGRP, SY_INVHAV,

	SY_ALL
};

typedef	byte TLx;

struct KWNode
{
	byte	mch,nch;
	byte	lx,kw;
	ushort	ql;
	KWNode	*nodes[1];
};

struct LexState
{
	const	char				*ptr;
	const	char				*lbeg;
	unsigned					lmb;
	unsigned					line;
};

#define	PRS_TOP					0x0001
#define	PRS_COPYV				0x0002
#define	PRS_PATH				0x0004

class SInCtx {
	Session				*const	ses;
	MemAlloc			*const	ma;
	const	HEAP_TYPE			mtype;
	const	char		*const	str;
	const	char		*const	end;
	const	URIID		*const	ids;
	const	unsigned			nids;
	const	SQType				sqt;
	const	char				*ptr;
	const	char				*lbeg;
	unsigned					lmb;
	unsigned					line;
	const	char				*errpos;
	unsigned					mode;
	char						*base;
	size_t						lBase;
	size_t						lBaseBuf;
	QName						*qNames;
	unsigned					nQNames;
	unsigned					lastQN;
	DynArray<Len_Str,6>			dnames;
	TLx							nextLex;
	Value						v;
public:
	SInCtx(Session *se,const char *s,size_t ls,const URIID *i=NULL,unsigned ni=0,SQType sq=SQ_SQL,MemAlloc *m=NULL)
		: ses(se),ma(m!=NULL?m:se),mtype(ma!=NULL?ma->getAType():SERVER_HEAP),str(s),end(s+ls),ids(i),nids(ni),sqt(sq),ptr(s),lbeg(s),lmb(0),line(1),errpos(NULL),
		mode(se==NULL?SIM_NO_BASE:(se->fStdOvr?SIM_STD_OVR:0)|(se->lURIBase==0?SIM_NO_BASE:0)),base(NULL),lBase(0),lBaseBuf(0),qNames(NULL),nQNames(0),lastQN(~0u),
		dnames(ma),nextLex(LX_ERR) {v.setError();}
	~SInCtx();
	Stmt	*parseStmt(bool fNested=false);
	RC		exec(const Value *params,unsigned nParams,char **result=NULL,uint64_t *nProcessed=NULL,unsigned nProcess=~0u,unsigned nSkip=0);
	QVarID	parseQuery(class Stmt*&,bool fNested=true);
	void	parseManage(IMapDir *,MVStoreCtx&,const StartupParameters *sp);
	void	parse(Value& res,const union QVarRef *vars=NULL,unsigned nVars=0,unsigned flags=0);
	class	ExprTree *parse(bool fCopy);
	void	checkEnd(bool fSemi=false) {switch (lex()) {case LX_RPR: throw SY_UNBRPR; case LX_RBR: throw SY_UNBRBR; case LX_RCBR: throw SY_UNBRCBR; case LX_SEMI: if (fSemi && lex()==LX_EOE) {case LX_EOE: return;} default: throw SY_SYNTAX;}}
	bool	checkDelimiter(char);
	void	getErrorInfo(RC rc,SynErr sy,CompilationError *ce) const {
		if (ce!=NULL) {ce->rc=rc; ce->line=line; ce->pos=unsigned((errpos==NULL?ptr:errpos)-lbeg-lmb); ce->msg=sy<SY_ALL?errorMsgs[sy]:(char*)0;}
	}
	static	void	getURIFlags(const char *uri,size_t l,struct URIInfo&);
	static	void	initKW();
	static	size_t	kwTabSize;
	static	const	OpDscr	opDscr[];
	static	const	TLx		charLex[256];
private:
	TLx		lex();
	bool	parseEID(TLx lx,ElementID& eid);
	void	saveLexState(LexState& s) const {s.ptr=ptr; s.lbeg=lbeg; s.lmb=lmb; s.line=line;}
	void	restoreLexState(const LexState& s) {ptr=s.ptr; lbeg=s.lbeg; lmb=s.lmb; line=s.line;}
	void	mapURI(const char *prefix=NULL,size_t lPrefix=0);
	void	mapIdentity();
	TLx		parsePrologue();
	QVarID	parseSelect(Stmt *res,bool fMod=false);
	QVarID	parseFrom(Stmt *stmt);
	QVarID	parseClasses(Stmt *stmt,bool fColon=false);
	TLx		parseOrderOrGroup(Stmt *stmt,QVarID var,Value *os=NULL,unsigned nO=0);
	ExprTree *parseCondition(const union QVarRef *vars,unsigned nVars);
	RC		splitWhere(Stmt *stmt,QVar *qv,ExprTree *pe);
	void	resolveVars(QVarRef *qv,Value &vv,Value *par=NULL);
	RC		replaceGroupExprs(Value& v,const OrderSeg *segs,unsigned nSegs);
	QVarID	findVar(const Value& v,const union QVarRef *vars,unsigned nVars) const;
	struct	FreeV	{static void free(Value& v) {freeV(v);}};
	template<typename T> struct	NoFree	{static void free(T) {}};
	template<typename T,typename fV,int size=256> struct Stack {
		T		stk[size];
		T		*ptr;
		Stack() : ptr(stk) {}
		~Stack() {while (ptr>stk) fV::free(*--ptr);}
		void	push(T v) {if (ptr==stk+size) throw RC_NORESOURCES; *ptr++=v;}
		T*		pop(int n) {return ptr>=stk+n?ptr-=n:(T*)0;}
		T		pop() {assert(ptr>stk); return *--ptr;}
		T*		top(int n) const {return ptr>=stk+n?ptr-n:(T*)0;}
		T		top() const {assert(ptr>stk); return ptr[-1];}
		bool	isEmpty() const {return ptr==stk;}
	};
	static	KWNode	*kwt['_'-'A'+1];
	static	const	char	*errorMsgs[SY_ALL];
	static	void	initKWEntry(const char *str,TLx lx,KW kw,ushort ql);
};

};

#endif
