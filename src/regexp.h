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
 * regular expression matching
 */
#ifndef _REGEXP_H_
#define _REGEXP_H_

#include "session.h"
#include "utils.h"
#include <ctype.h>

using namespace Afy;

namespace AfyKernel
{

#define BITS_PER_BYTE	8

#define	RXM_NCASE		0x0001
#define	RXM_MULTIL		0x0002
#define	RXM_MULTIM		0x0004

#define	RX_GROUP_MAX	0xFFFF	// maximum number of groups in pattern
#define RX_STRLEN_MAX	255		// maximum length of a sequential string

#define RXN_ANCHORED 0x01    // anchored at the front
#define RXN_SINGLE   0x02    // matches a single char
#define RXN_NONEMPTY 0x04    // does not match empty string
#define RXN_ISNEXT   0x08    // rx is next after at least one node
#define RXN_GOODNEXT 0x10    // rx->next is a tree-like edge in the graph
#define RXN_ISJOIN   0x20    // rx is a join point in the graph
#define RXN_REALLOK  0x40    // RX_STR owns tempPool space to realloc 
#define RXN_MINIMAL  0x80    // un-greedy matching for ? * + {} 

enum RxOp {
	RX_EMPTY      = 0,  // match rest of input against rest of r.e.
	RX_ALT        = 1,  // alternative subexpressions in child and next
	RX_BOL        = 2,  // beginning of input (or line if multiline)
	RX_EOL        = 3,  // end of input (or line if multiline)
	RX_WBND       = 4,  // match "" at word boundary
	RX_WNBND      = 5,  // match "" at word non-boundary
	RX_QNTF       = 6,  // quantified atom: atom{1,2}
	RX_STAR       = 7,  // zero or more occurrences of child
	RX_PLUS       = 8,  // one or more occurrences of child
	RX_OPT        = 9,  // optional subexpression in child
	RX_LPAREN     = 10, // left paren bytecode: child is num'th sub-regexp
	RX_RPAREN     = 11, // right paren bytecode
	RX_DOT        = 12, // stands for any character
	RX_CCLASS     = 13, // character class: [a-f]
	RX_DIGIT      = 14, // match a digit char: [0-9]
	RX_NONDIGIT   = 15, // match a non-digit char: [^0-9]
	RX_ALNUM      = 16, // match an alphanumeric char: [0-9a-z_A-Z]
	RX_NONALNUM   = 17, // match a non-alphanumeric char: [^0-9a-z_A-Z]
	RX_SPACE      = 18, // match a whitespace char
	RX_NONSPACE   = 19, // match a non-whitespace char
	RX_BACKREF    = 20, // back-reference (e.g., \1) to a parenthetical
	RX_STR        = 21, // match a string
	RX_CHR        = 22, // match a single char
	RX_JUMP       = 23, // for deoptimized closure loops
	RX_DOTSTAR    = 24, // optimize .* to use a single opcode
	RX_ANCHOR     = 25, // like .* but skips left context to unanchored r.e.
	RX_EOLONLY    = 26, // $ not preceded by any pattern
	RX_USTR       = 27, // flat Unicode string; len immediate counts chars
	RX_UCHR       = 28, // single Unicode char
	RX_UCLASS     = 29, // Unicode character class, vector of chars to match
	RX_NUCLASS    = 30, // negated Unicode character class
	RX_NCBACKREF  = 31, // case-independent RX_BACKREF
	RX_NCSTR      = 32, // case-independent RX_STR
	RX_NCCHR      = 33, // case-independent RX_CHR
	RX_NCUSTR     = 34, // case-independent RX_USTR
	RX_NCUCHR     = 35, // case-independent RX_UCHR
	RX_ANCHOR1    = 36, // first-char discriminating RX_ANCHOR
	RX_NCLASS     = 37, // negated 8-bit character class
	RX_DOTSTARMIN = 38, // ungreedy version of RX_DOTSTAR
	RX_LPARENNON  = 39, // non-capturing version of RX_LPAREN
	RX_RPARENNON  = 40, // non-capturing version of RX_RPAREN
	RX_ASSERT     = 41, // zero width positive lookahead assertion
	RX_ASSERT_NOT = 42, // zero width negative lookahead assertion
	RX_END
};

typedef ptrdiff_t	RxPtr;

struct RegExpHdr
{
	uint32_t		rx;
	uint32_t		lsrc;
	uint16_t		nGroups;
	uint16_t		flags;
};

struct RxNode {
	struct rng	{uint16_t rmin,rmax;};
	struct uccl	{uint16_t fnot,bmsize; uint32_t bitmap;};
	byte			op;         // packed op
	byte			flags;      // flags
	int32_t			next;       // next in concatenation order 
	int32_t			child;      // first operand
    union {
		int64_t		num;        // could be a number 
		byte		chr;        // or a character 
		rng			range;      // or a quantifier range 
		uccl		ucclass;	// or a class
	};
	RxNode	*getNext() const {return next!=0?(RxNode*)((byte*)this+next):0;}
	RxNode	*getChild() const {return child!=0?(RxNode*)((byte*)this+child):0;}
	byte	*getBitmap() const {return ucclass.bitmap!=0?(byte*)this+ucclass.bitmap:0;}
	byte	*getString() const {return (byte*)(this+1);}	
};

class RxCtx {
	MemAlloc	*const	ma;
	const byte	*const	beg;
	const byte	*const	end;
	const byte			*ptr;
	uint16_t			flags;
	uint16_t			nGroups;
	byte				*mem;
	size_t				left;
	size_t				lblock;
public:
	RxCtx(const byte* s,size_t ls,unsigned f,MemAlloc *m) : ma(m),beg(s),end(s+ls),ptr(s),flags(f),nGroups(0),mem(NULL),left(0),lblock(0) {}
	static	void	parse(Value& res,MemAlloc *ma,const byte *src,size_t lsrc,unsigned flags=0,const char *opts=NULL,size_t lopts=0);
private:
	RxPtr	parseRegExp();
	RxPtr	parseAltern();
	RxPtr	parseItem();
	RxPtr	parseQuantAtom();
	RxPtr	parseAtom();
	void	fixNext(RxPtr rx1,RxPtr rx2,RxPtr oldnext=0);
	void	buildBitmap(RxPtr rx,const byte *s,const byte *end);
	void	setNext(RxPtr rx,RxPtr next) {node(rx).next=next!=0?int32_t(next-rx):0;}
	void	setBitmap(RxPtr rx,byte *bmp) {node(rx).ucclass.bitmap=bmp!=NULL?uint32_t(bmp-mem-rx):0;}
	RxNode&	node(RxPtr rp) const {return *(RxNode*)(mem+rp);}
	RxPtr	newNode(RxOp o,RxPtr chld=NULL,byte flg=0) {
		RxNode *rx=(RxNode*)alloc(sizeof(RxNode)); rx->op=(byte)o; rx->flags=flg; rx->next=0; rx->child=chld!=0?int32_t(chld-((byte*)rx-mem)):0; return (byte*)rx-mem;
	}
	RxPtr	newNode(RxOp o,const byte *pc,unsigned l,byte flg) {
		RxNode *rx=(RxNode*)alloc(sizeof(RxNode)+(l>1?l+1&~1:l)); rx->op=(byte)o; rx->flags=flg; rx->next=0; rx->child=0; 
		if (l==1) {rx->chr=*pc; rx->flags|=RXN_SINGLE;} else {memcpy(rx+1,pc,l); rx->num=l;}
		return (byte*)rx-mem;
	}
	byte	*alloc(size_t s) {
		if (s>left) {
			size_t delta=lblock==0?0x100:lblock/2; if (delta+left<s) delta+=s;
			if ((mem=(byte*)ma->realloc(mem,lblock+delta,lblock))==NULL) throw RC_NORESOURCES;
			left+=delta; lblock+=delta;
		}
		byte *p=mem+lblock-left; left-=s; return p;
	}
};

struct RExpCtx;

class MatchCtx {
    const byte		*beg,*end;			// base address and limit
    size_t          start;              // offset from beg to start at
    ptrdiff_t       skipped;            // chars skipped anchoring this r.e.
    byte            flags;              // flags
    bool			ok;                 // indicates runtime error during matching
    bool			anchoring;          // true if multiline anchoring ^/$
	RExpCtx&		rctx;

	MatchCtx(const byte *s,size_t l,byte f,RExpCtx& rx,size_t st=0) : beg(s),end(s+l),start(st),skipped(0),flags(f),ok(true),anchoring(false),rctx(rx) {}
	bool matchChar(byte c, byte c2) {return c==c2 || (flags&RXM_NCASE)!=0 && (((c = toupper(c)) == (c2 = toupper(c2)))	|| (tolower(c) == tolower(c2)));}
	const byte *matchNodes(RxNode *rx,RxNode *stop,const byte *p);
	const byte *matchChild(RxNode *rx,unsigned childCount,unsigned maxChild,const byte *p);
	const byte *match(RxNode *rx, const byte *p);
	struct GMatchCtx {
		MatchCtx& ctx;
		RxNode *child;
		RxNode *next;
		RxNode *stop;
		unsigned childCount;
		unsigned maxChild;
		GMatchCtx(MatchCtx&ct,RxNode *rx,RxNode *stp,unsigned chCnt) : ctx(ct),child(rx->getChild()),next(rx->getNext()),stop(stp),childCount(chCnt),maxChild(rx->op==RX_QNTF ? rx->range.rmax : 0) {}
		const byte *gMatch(const byte *cp, const byte *previousChild);
	};
public:
	static RC match(const char *str,size_t lstr,const byte *rx,RExpCtx& rctx,unsigned flags=0);
};

class RegexService : public IService
{
	StoreCtx	*const	ctx;
	struct RegexProcessor : public Processor {
		const	byte	*const	re;
		Value					render;		
		const	unsigned		flags;
		RegexProcessor(byte *r,Value& rnd,unsigned f) : re(r),render(rnd),flags(f) {}
		RC invoke(IServiceCtx *ctx,const Value& inp,Value& out,unsigned& mode);
		void cleanup(IServiceCtx *,bool fDestroy);
	};
public:
	RegexService(StoreCtx *ct) : ctx(ct) {}
	RC create(IServiceCtx *ctx,uint32_t& dscr,IService::Processor *&ret);
};

};

#endif
