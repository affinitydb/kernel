/**************************************************************************************

Copyright © 2010-2012 VMware, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,  WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.

Written by Mark Venguerov 2010-2012

**************************************************************************************/

/**
 * PathSQL parsing and rendering control structures
 * JSON rendering
 */
#ifndef _PARSER_H_
#define _PARSER_H_

#include "affinity.h"
#include "session.h"

using namespace AfyDB;

class IMapDir;
struct StartupParameters;

namespace AfyKernel
{

enum SQType
{
	SQ_PATHSQL, SQ_SPARQL		/**< type of query language (n.b. SPARQL is not implemented yet) */
};

#undef	_F
#undef	_U
#undef	_C
#undef	_I
#undef	_A

/**
 * OP_XXX code bit flags
 */
#define	_F	0x01			/**< is a function */
#define	_U	0x02			/**< is unary operation */
#define	_C	0x04			/**< requres balanced parantheses */
#define	_I	0x08			/**< can have constant second argument, special p-code encoding */
#define	_A	0x10			/**< aggregation function */

/**
 * operation descriptor
 */
struct OpDscr {
	ushort		flags;		/**< bit flags, see above */
	byte		match;		/**< matching paranthesis */
	byte		lstr;		/**< length of name string */
	const char	*str;		/**< name (for rendering) */
	byte		prty[2];	/**< shift-reduce priorities */
	byte		nOps[2];	/**< minimum and maximum number of operands */
};

class QVar;

/**
 * PathSQL rendering mode flags
 */
#define	SOM_VAR_NAMES	0x0001		/**< use variable names */
#define	SOM_AND			0x0002		/**< expressions combined with AND, insert '(...)' where necessary */
#define	SOM_WHERE		0x0004		/**< expression in WHERE clause */
#define	SOM_MATCH		0x0008		/**< expression in MATCH */
#define	SOM_STD_PREFIX	0x0010		/**< standard prefix is overwritten - don't use it */
#define	SOM_BASE_USED	0x0020		/**< BASE prefix used to compress IDs - add it to prologue */

/**
 * JSON Value rendering control
 * @see SOutCtx::renderValue()
 */
enum JRType
{
	JR_NO, JR_PROP, JR_EID
};

/**
 * PathSQL or JSON rendering context
 */
class SOutCtx
{
	Session	*const		ses;						/**< assosiated session */
	const	unsigned	mode;						/**< rendering mode */
	const	QVar		*cvar;						/**< current context for identifier rendering (required for joins) */
	const	QName		*const	qNames;				/**< table of used session-wide qnames */
	const	unsigned	nQNames;					/**< length of table of qnames */
	unsigned			*usedQNames;				/**< priority queue of used qnames */
	unsigned			nUsedQNames;				/**< length of qname priority queue */
	char				cbuf[100];					/**< working buffer for parts of strings */
	ulong				flags;						/**< current rendering mode, see SOM_XX flags above */
	byte				*ptr;						/**< main buffer */
	size_t				cLen;						/**< current output length */
	size_t				xLen;						/**< main buffer length */
public:
	SOutCtx(Session *s,unsigned md=0,const QName *qn=NULL,unsigned nQN=0) : ses(s),mode(md),cvar(NULL),
												qNames(qn),nQNames(nQN),usedQNames(NULL),nUsedQNames(0),flags(0),ptr(NULL),cLen(0),xLen(0) {}
	~SOutCtx() {if (usedQNames!=NULL) ses->free(usedQNames); if (ptr!=NULL) ses->free(ptr);}
	Session	*getSession() const {return ses;}																	
	RC		renderName(uint32_t id,const char *prefix=NULL,const QVar *qv=NULL,bool fQ=false,bool fEsc=true);	/**< render identifier depending on context */
	RC		renderPID(const PID& id,bool fJSon=false);															/**< render PIN ID */
	RC		renderValue(const Value& v,JRType jr=JR_NO);														/**< render Value as PathSQL or JSON */
	RC		renderVarName(const QVar *qv);																		/**< render variable name for PathSQL statement */
	RC		renderRep(unsigned rm,unsigned rx);																	/**< render path expression segment repeater '{...}' */
	RC		renderPath(const struct PathSeg& ps);																/**< render path expression */
	char	*renderAll();																						/**< append prologue if necessary */
	RC		renderJSON(class Cursor *cr,uint64_t& cnt);															/**< render JSON output from a cursor */
	
	// helper functions
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

/**
 * PathSQL parsing control flags
 */
#define	SIM_SIMPLE_NAME		0x0001			/**< parse simple names only (no qname) (e.g. in FROM ... AS name) */
#define	SIM_NO_BASE			0x0002			/**< don't use BASE */
#define	SIM_STD_OVR			0x0004			/**< standard prefix overwriten - don't use it */
#define	SIM_INT_NUM			0x0008			/**< force parsing integer number (don't treat '.' as a part of number) */
#define	SIM_SELECT			0x0010			/**< expression is part of SELECT list - special parsing for identifiers */
#define	SIM_DML_EXPR		0x0020			/**< expression in INSERT or UPDATE statements, can contain nested INSERTs */
#define	SIM_HAVING			0x0040			/**< expression in HAVING - aggregates with '*' are allowed */

/**
 * lexem enumeration
 * some OP_XXX are overwritten, extends past OP_ALL
 */
enum Lexem {
	LX_BOE=OP_SET, LX_EOE=OP_ADD, LX_IDENT=OP_ADD_BEFORE, LX_CON=OP_MOVE,
	LX_KEYW=OP_MOVE_BEFORE, LX_LPR=OP_DELETE, LX_RPR=OP_EDIT, LX_COMMA=OP_RENAME,
		
	LX_COLON=OP_ALL, LX_LBR, LX_RBR, LX_LCBR, LX_RCBR, LX_PERIOD, LX_QUEST, LX_EXCL,
	LX_EXPR, LX_QUERY, LX_URSHIFT, LX_PREFIX, LX_IS, LX_BETWEEN, LX_BAND, LX_HASH,
	LX_SEMI, LX_CONCAT, LX_FILTER, LX_REPEAT, LX_PATHQ, LX_SELF, LX_SPROP,
		
	LX_NL, LX_SPACE, LX_DQUOTE, LX_QUOTE, LX_LT, LX_GT, LX_VL, LX_DOLLAR,
	LX_ERR, LX_UTF8, LX_DIGIT, LX_LETTER, LX_ULETTER, LX_XLETTER,
};

/**
 * keyword enumeration
 * @see keyword table in parser.cpp
 */
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

/**
 * syntactic error enumeration
 */
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

/**
 * keyword trie node descriptor
 */
struct KWNode
{
	byte	mch,nch;
	byte	lx,kw;
	ushort	ql;
	KWNode	*nodes[1];
};

/**
 * lexer state descriptor for backtracking in lookahead
 */
struct LexState
{
	const	char				*ptr;
	const	char				*lbeg;
	unsigned					lmb;
	unsigned					line;
};

/**
 * expression parsing mode constants
 */
#define	PRS_TOP					0x0001
#define	PRS_COPYV				0x0002
#define	PRS_PATH				0x0004

/**
 * PathSQL expression of statement parsing context
 * uses top-down LL(1) (mostly) for statement parsing and priority-driven shift-reduce for expression parsing
 * special code introduces various contextual dependencies
 */
class SInCtx {
	Session				*const	ses;			/**< associated session */
	MemAlloc			*const	ma;				/**< memory allocator for expressions and statements */
	const	HEAP_TYPE			mtype;			/**< heap type of the above memory allocator */
	const	char		*const	str;			/**< PathSQL string to be parsed */
	const	char		*const	end;			/**< end of PathSQL string */
	const	URIID		*const	ids;			/**< table of URIIDs */
	const	unsigned			nids;			/**< length of URIID table */
	const	SQType				sqt;			/**< parser language */
	const	char				*ptr;			/**< current position in string */
	const	char				*lbeg;			/**< current line beginning for error reporting; lines are separated by '\n' */
	unsigned					lmb;			/**< multi-byte shift; used to calculate line position in characters rather than octets */
	unsigned					line;			/**< current line number for error reporting */
	const	char				*errpos;		/**< the lexem start where the error must be reported */
	unsigned					mode;			/**< parse mode, see SIM_XXX above */
	char						*base;			/**< BASE in prologue of this expression or statement */
	size_t						lBase;			/**< length of BASE string */
	size_t						lBaseBuf;		/**< length of buffer used to append BASE to identifiers */
	QName						*qNames;		/**< table of PREFIXes in prologue for this expression or statement */
	unsigned					nQNames;		/**< length of qname table */
	unsigned					lastQN;			/**< last QName used in QName table (checked first) */
	DynArray<Len_Str,6>			dnames;			/**< delayed mapping names for SELECT parsing */
	TLx							nextLex;		/**< look-ahead lexem */
	Value						v;				/**< value associated with current lexem (string, number, etc.) */
public:
	SInCtx(Session *se,const char *s,size_t ls,const URIID *i=NULL,unsigned ni=0,SQType sq=SQ_PATHSQL,MemAlloc *m=NULL)
		: ses(se),ma(m!=NULL?m:se),mtype(ma!=NULL?ma->getAType():SERVER_HEAP),str(s),end(s+ls),ids(i),nids(ni),sqt(sq),ptr(s),lbeg(s),lmb(0),line(1),errpos(NULL),
		mode(se==NULL?SIM_NO_BASE:(se->fStdOvr?SIM_STD_OVR:0)|(se->lURIBase==0?SIM_NO_BASE:0)),base(NULL),lBase(0),lBaseBuf(0),qNames(NULL),nQNames(0),lastQN(~0u),
		dnames(ma),nextLex(LX_ERR) {v.setError();}
	~SInCtx();
	Stmt	*parseStmt(bool fNested=false);															/**< parse PathSQL statement; can be called recursively for nested statements */
	RC		exec(const Value *params,unsigned nParams,char **result=NULL,uint64_t *nProcessed=NULL,unsigned nProcess=~0u,unsigned nSkip=0);	/**< parse PathSQL, execute and return result as JSON */
	QVarID	parseQuery(class Stmt*&,bool fNested=true);												/**< parse a query (i.e. not DML or DDL statement) */
	void	parseManage(IMapDir *,AfyDBCtx&,const StartupParameters *sp);							/**< parse and execute database management PathSQL */
	void	parse(Value& res,const union QVarRef *vars=NULL,unsigned nVars=0,unsigned flags=0);		/**< parse PathSQL expression */
	class	ExprTree *parse(bool fCopy);															/**< parse PathSQL expression, return as an expression tree */
	void	checkEnd(bool fSemi=false) {switch (lex()) {case LX_RPR: throw SY_UNBRPR; case LX_RBR: throw SY_UNBRBR; case LX_RCBR: throw SY_UNBRCBR; case LX_SEMI: if (fSemi && lex()==LX_EOE) {case LX_EOE: return;} default: throw SY_SYNTAX;}}	/**< check end of PathSQL string */
	bool	checkDelimiter(char);																	/**< check statement delimeter */
	void	getErrorInfo(RC rc,SynErr sy,CompilationError *ce) const {								/** fill CompilationError structure (see affinity.h) */
		if (ce!=NULL) {ce->rc=rc; ce->line=line; ce->pos=unsigned((errpos==NULL?ptr:errpos)-lbeg-lmb); ce->msg=sy<SY_ALL?errorMsgs[sy]:(char*)0;}
	}
	static	void	getURIFlags(const char *uri,size_t l,struct URIInfo&);							/**< get flags describing URI string (see map.h) */
	static	void	initKW();																		/**< initialize keyword trie */
	static	size_t	kwTabSize;																		/**< keyword table size */
	static	const	OpDscr	opDscr[];																/**< lexem and p-code operations descriptor table */
	static	const	TLx		charLex[256];															/**< octet to lexem map */
private:
	TLx		lex();																					/**< lexer */
	bool	parseEID(TLx lx,ElementID& eid);														/**< parse ':XXX' eid constant */
	void	saveLexState(LexState& s) const {s.ptr=ptr; s.lbeg=lbeg; s.lmb=lmb; s.line=line;}		/**< save lexer state for deep lookahead */
	void	restoreLexState(const LexState& s) {ptr=s.ptr; lbeg=s.lbeg; lmb=s.lmb; line=s.line;}	/**< restore lexer state */
	void	mapURI(const char *prefix=NULL,size_t lPrefix=0);										/**< map string to URIID, use prefixes, base, ect. */
	void	mapIdentity();																			/**< map identity string to IdentityID */
	TLx		parsePrologue();																		/**< parse prologue, i.e. BASE ... PREFIX ... PREFIX ... */
	QVarID	parseSelect(Stmt *res,bool fMod=false);													/**< parse SELECT statement */
	QVarID	parseFrom(Stmt *stmt);																	/**< parse FROM clause */
	QVarID	parseClasses(Stmt *stmt,bool fColon=false);												/**< parse classes variables in FROM clause */
	TLx		parseOrderOrGroup(Stmt *stmt,QVarID var,Value *os=NULL,unsigned nO=0);					/**< parse ORDER BY or GROUP BY clause */
	ExprTree *parseCondition(const union QVarRef *vars,unsigned nVars);								/**< parse condition expresion (either in WHERE or in ON or in HAVING) */
	RC		splitWhere(Stmt *stmt,QVar *qv,ExprTree *pe);											/**< down-propagate individual variable conditions in join, associate variables in JOIN on or WHERE conditions */
	void	resolveVars(QVarRef *qv,Value &vv,Value *par=NULL);										/**< associate SELECT identifiers with FROM variables */
	RC		replaceGroupExprs(Value& v,const OrderSeg *segs,unsigned nSegs);						/**< GROUP BY expressions are stored separately in variables */
	QVarID	findVar(const Value& v,const union QVarRef *vars,unsigned nVars) const;					/**< find variable by its name */
	struct	FreeV	{static void free(Value& v) {freeV(v);}};
	template<typename T> struct	NoFree	{static void free(T) {}};
	template<typename T,typename fV,int size=256> struct Stack {									/**< template for parser stacks */
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
	static	KWNode	*kwt['_'-'A'+1];																/**< keyword trie */
	static	const	char	*errorMsgs[SY_ALL];														/**< error messages */	
	static	void	initKWEntry(const char *str,TLx lx,KW kw,ushort ql);							/**< keyword trie initialization */
};

};

#endif
