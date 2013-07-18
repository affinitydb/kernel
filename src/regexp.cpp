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
 * RegExp grammar
 *
 *  regexp:     altern                  A regular expression is one or more
 *              altern '|' regexp       alternatives separated by vertical bar.
 *
 *  altern:     item                    An alternative is one or more items,
 *              item altern             concatenated together.
 *
 *  item:       assertion               An item is either an assertion or
 *              quantatom               a quantified atom.
 *
 *  assertion:  '^'                     Assertions match beginning of string (or line if fMultiline is true).
 *              '$'                     End of string (or line if fMultiline is true).
 *              '\b'                    Word boundary (between \w and \W).
 *              '\B'                    Word non-boundary.
 *
 *  quantatom:  atom                    An unquantified atom.
 *              quantatom '{' n ',' m '}' Atom must occur between n and m times.
 *              quantatom '{' n ',' '}' Atom must occur at least n times.
 *              quantatom '{' n '}'     Atom must occur exactly n times.
 *              quantatom '*'           Zero or more times (same as {0,}).
 *              quantatom '+'           One or more times (same as {1,}).
 *              quantatom '?'           Zero or one time (same as {0,1}).
 *
 *              any of which can be optionally followed by '?' for ungreedy
 *
 *  atom:       '(' regexp ')'          A parenthesized regexp (what matched can be addressed using a backreference, see '\' n below).
 *              '.'                     Matches any char except '\n'.
 *              '[' classlist ']'       A character class.
 *              '[' '^' classlist ']'   A negated character class.
 *              '\f'                    Form Feed.
 *              '\n'                    Newline (Line Feed).
 *              '\r'                    Carriage Return.
 *              '\t'                    Horizontal Tab.
 *              '\v'                    Vertical Tab.
 *              '\d'                    A digit (same as [0-9]).
 *              '\D'                    A non-digit.
 *              '\w'                    A word character, [0-9a-z_A-Z].
 *              '\W'                    A non-word character.
 *              '\s'                    A whitespace character, [ \b\f\n\r\t\v].
 *              '\S'                    A non-whitespace character.
 *              '\' n                   A backreference to the nth (n decimal and positive) parenthesized expression.
 *              '\' octal               An octal escape sequence (octal must be two or three digits long, unless it is 0 for the null character).
 *              '\x' hex                A hex escape (hex must be two digits).
 *              '\c' ctrl               A control character, ctrl is a letter.
 *              '\' literalatomchar     Any character except one of the above that follow '\' in an atom.
 *              otheratomchar           Any character not first among the other atom right-hand sides.
 */

#include "regexp.h"
#include "expr.h"
#include "parser.h"
#include "qbuild.h"
#include "queryprc.h"
#include "service.h"

using namespace AfyKernel;

static const char escapes[] =
{
	'\b', 'b','\f', 'f','\n', 'n','\r', 'r','\t', 't','\v', 'v','"',  '"','\'', '\'','\\', '\\',0
};

static const char metachars[] =
{
    '|', '^', '$', '{', '*', '+', '?', '(', ')', '.', '[', '\\', '}', '\0'
};

static const char closurechars[] =
{
    '{', '*', '+', '?', '\0'	// balance} 
};

#define HexToDigit(a) ((a)>='a'?(a)-'a'+10:(a)>='A'?(a)-'A'+10:(a)-'0')

RxPtr RxCtx::parseRegExp()
{
	RxPtr rx=parseAltern(); if (rx==0) return 0;
	const byte *p=ptr;
    if (p<end && *p=='|') {
		RxPtr child=rx,rx1=rx=newNode(RX_ALT,child,node(child).flags&(RXN_ANCHORED|RXN_NONEMPTY));
		do {
			ptr=++p;			// (balance:
		    if (p<end && (*p=='|' || *p==')')) child=newNode(RX_EMPTY); else {child=parseAltern(); p=ptr;}
			if (child==0) return 0;
			RxPtr rx2=newNode(RX_ALT,child,node(child).flags&(RXN_ANCHORED|RXN_NONEMPTY)|RXN_ISNEXT);
		    setNext(rx1,rx2); node(rx1).flags|=RXN_GOODNEXT; rx1=rx2;
		} while (p<end && *p=='|');
    }
	return rx;
}

RxPtr RxCtx::parseAltern()
{
	uint32_t flags=0; byte c;
    RxPtr rx1,rx=rx1=parseItem(); if (rx==0) return 0;
	for (const byte *p=ptr; p<end && (c=*p)!='|' && c!=')'; p=ptr) {
		RxPtr rx2=parseItem(); if (rx2==0) return 0;
		fixNext(rx1,rx2); flags|=node(rx2).flags; rx1=rx2;
	}
    node(rx).flags|=flags&RXN_NONEMPTY;
	return rx;
}

RxPtr RxCtx::parseItem()
{
    const byte *p=ptr; RxOp op;

    if (p<end) switch (*p) {
    case '^':
		ptr=p+1; return newNode(RX_BOL,0,RXN_ANCHORED);

    case '$':
		ptr=p+1;
	    return newNode(p==beg || (p[-1]=='(' || p[-1]=='|') && (p-1==beg || p[-2]!='\\') ? RX_EOLONLY : RX_EOL);	//balance)

    case '\\':
		if (++p>=end) break;
		switch (*p) {
		case 'b': op=RX_WBND; break;
	    case 'B': op=RX_WNBND; break;
	    default: return parseQuantAtom();
		}
		ptr=p+1; return newNode(op,0,RXN_NONEMPTY);

    }
	return parseQuantAtom();
}

RxPtr RxCtx::parseQuantAtom()
{
	RxPtr rx=parseAtom(),rx2; if (rx==0) return 0;
	for (const byte *p=ptr;;) {
		uint32_t rmin,rmax;
		if (p>=end) {ptr=p; return rx;}
		switch (*p) {
		default: ptr=p; return rx;
	    case '{':
			if (++p>=end||!isdigit(*p)) throw SY_MISNUM;
			for (rmin=0; p<end && isdigit(*p); ++p) {
				rmin=10*rmin+(*p-'0');
		        if ((rmin>>16)!=0) throw SY_TOOBIG;
			}
		    if (p<end && *p==',') {
		        if (++p>=end) throw SY_MISRCBR;
				if (*p=='}') rmax=0xFFFF;
				else for (rmax=0; p<end && isdigit(*p); ++p) {
				    rmax=10 * rmax+*p-'0';
					if ((rmax>>16)!=0) throw SY_TOOBIG;
			    }
				if (rmax==0 || rmin>rmax) throw SY_INVNUM;
			} else if ((rmax=rmin)==0) throw SY_INVNUM;
		    if (p>=end || *p!='}') throw SY_MISRCBR;
			rx2=newNode(RX_QNTF,rx,rmin>0 && (node(rx).flags&RXN_NONEMPTY)!=0?RXN_NONEMPTY:0);
		    node(rx2).range.rmin=(uint16_t)rmin; node(rx2).range.rmax=(uint16_t)rmax; rx=rx2;
			break;
	    case '*':
		    rx=newNode(RX_STAR,rx); break;
		case '+':
			rx=newNode(RX_PLUS,rx,node(rx).flags&RXN_NONEMPTY); break;
		case '?':
			rx=newNode(RX_OPT,rx); break;
		}
        if (++p<end && *p=='?') {node(rx).flags|=RXN_MINIMAL; p++;}
    }
}

RxPtr RxCtx::parseAtom()
{
    const byte *p=ptr,*savep=p;
	uint32_t num,len; RxPtr rx,rx2; byte c; RxOp op;

    if (p==end || *p=='|') return newNode(RX_EMPTY);

	switch (c=*p) {
	case '(':
		num=-1; op=RX_END;
		if (p+2<end && p[1]=='?') switch (p[2]) {
		case ':' : op=RX_LPARENNON; break;
        case '=' : op=RX_ASSERT; break;
        case '!' : op=RX_ASSERT_NOT; break;
		}
		if (op==RX_END) {num=nGroups++; op=RX_LPAREN; ptr=p+1;} else ptr=p+3;
		if ((rx2=parseRegExp())==0) return 0;
		if ((p=ptr)>=end || *p!=')') throw SY_MISRPR;
		p++;
		rx=newNode(op,rx2,node(rx2).flags&(RXN_ANCHORED|RXN_NONEMPTY)); 
		node(rx).num=num;
		if (op==RX_LPAREN || op==RX_LPARENNON) {
			rx2=newNode((RxOp)(op+1));
			fixNext(rx,rx2); node(rx2).num=num;
		}
		break;

	case '.':
		p++; op=RX_DOT;
		if (p<end && *p=='*') {
			p++; op=RX_DOTSTAR;
		    if (p<end && *p=='?') {p++; op=RX_DOTSTARMIN;}
        }
		rx=newNode(op,0,op==RX_DOT?RXN_SINGLE|RXN_NONEMPTY:0);
		break;

	case '[':
		savep=++p; rx=newNode(RX_CCLASS,0,RXN_SINGLE|RXN_NONEMPTY);
		while ((c=*++p)!=']') {
		    if (p==end) throw SY_MISRBR;
			if (c=='\\' && p+1!=end) p++;
		}
		buildBitmap(rx,savep,p); ++p; break;

	case '\\':
        if (++p==end) throw SY_SYNTAX;
		switch (c=*p) {
		case 'f': case 'n': case 'r': case 't': case 'v':
			rx=newNode(RX_CHR); c=strchr(escapes,c)[-1]; break;

		case 'd': rx=newNode(RX_DIGIT); break;
		case 'D': rx=newNode(RX_NONDIGIT); break;
		case 'w': rx=newNode(RX_ALNUM); break;
		case 'W': rx=newNode(RX_NONALNUM); break;
		case 's': rx=newNode(RX_SPACE); break;
		case 'S': rx=newNode(RX_NONSPACE); break;
		case '0': rx=newNode(RX_CHR); c=0; break;
		case '1': case '2': case '3': case '4': 
		case '5': case '6': case '7': case '8': case '9':
            for (num=0; p<end && isdigit(*p); ++p)	num=10*num+*p-'0';
			rx=newNode(RX_BACKREF,0,RXN_NONEMPTY);
			node(rx).num=num-1; ptr=p; return rx;

		case 'x':
			if (p+3>=end || !isxdigit(p[1]) || !isxdigit(p[2])) num='x';
			else {num=(HexToDigit(p[1])<<4)+HexToDigit(p[2]); p+=2;}
			rx=newNode(RX_CHR);
			c=(byte)num;
			break;

		case 'c':
			if (++p>=end || !isalpha(*p)) {savep=--p; goto do_str;}
			c=toupper(*p)^0x40; rx=newNode(RX_CHR);
			break;

		case 'u':
			if (p+4<end && isxdigit(p[1]) && isxdigit(p[2]) &&
											isxdigit(p[3]) && isxdigit(p[4])) {
				c=(((((HexToDigit(p[1]) << 4)+HexToDigit(p[2])) << 4)
					 +HexToDigit(p[3])) << 4)+HexToDigit(p[4]);
				p+=4; rx=newNode(RX_CHR); break;
			}
		default:
			savep=p; goto do_str;
		}
		if (rx!=0) {node(rx).chr=c; node(rx).flags=RXN_SINGLE|RXN_NONEMPTY;}
		p++; break;

	default:
    do_str:
		do p++; while (p<end && !strchr(metachars,*p));
		len=(uint32_t)(p-savep);
		if (p!=end && len>1 && strchr(closurechars,c)) {p--; len--;}
		if (len>RX_STRLEN_MAX) {len=RX_STRLEN_MAX; p=savep+len;}
		rx=newNode(len==1?RX_CHR:RX_STR,savep,len,RXN_NONEMPTY);
		break;
	}
	ptr=p;
	return rx;
}

void RxCtx::buildBitmap(RxPtr rx,const byte *p0,const byte *end)
{
	uint32_t i,n,b,maxc=0; const byte *savep,*ptr=p0;
	while (ptr<end) {
		byte c=*ptr++,c2;
		if (c=='\\') {
			if (ptr+5 <= end && *ptr=='u' &&
						isxdigit(ptr[1]) && isxdigit(ptr[2]) &&
						isxdigit(ptr[3]) && isxdigit(ptr[4])) {
				c=(((((HexToDigit(ptr[1]) << 4)
					+ HexToDigit(ptr[2])) << 4)
				     +HexToDigit(ptr[3])) << 4)
				   +HexToDigit(ptr[4]);
				ptr += 5;
			} else {
	            if (maxc<255) maxc=255;
				continue;
		    }
		}
		if ((flags&RXM_NCASE)!=0) {
		    if ((c2=toupper(c)) > maxc) maxc=c2;
			if ((c2=tolower(c2)) > maxc) maxc=c2;
		}
		if (c > maxc) maxc=c;
    }
	ptr=p0;

	node(rx).ucclass.bmsize=maxc=(uint16_t)((size_t)(maxc+BITS_PER_BYTE)/BITS_PER_BYTE);
	byte *bitmap=alloc(maxc); setBitmap(rx,bitmap);

	byte fill=*ptr=='^'?(++ptr,0xff):0; memset(bitmap,fill,maxc);
	int nchars=maxc*BITS_PER_BYTE; node(rx).ucclass.fnot=fill;

#define MATCH_BIT(c) {i=(c)>>3; b=(c)&7; b=1<<b; if (fill) bitmap[i]&=~b; else bitmap[i]|=b;}
#define CHECK_RANGE() if (inrange) {MATCH_BIT(lastc); MATCH_BIT('-'); inrange=false;}

	int lastc=nchars; bool inrange=false;

	while (ptr<end) {
		int c=*ptr++;
		if (c=='\\') {
		    c=*ptr++;
			switch (c) {
			case 'b':
			case 'f':
			case 'n':
			case 'r':
			case 't':
			case 'v':
				c=strchr(escapes,(char)c)[-1];
				break;

			case 'd':
				CHECK_RANGE();
				for (c='0'; c<='9'; c++) MATCH_BIT(c);
				continue;

			case 'D':
				CHECK_RANGE();
				for (c=0; c<'0'; c++) MATCH_BIT(c);
				for (c='9'+1; c<nchars; c++) MATCH_BIT(c);
				continue;

			case 'w':
				CHECK_RANGE();
				for (c=0; c<nchars; c++)
					if (isalnum(c)||c=='_') MATCH_BIT(c);
				continue;

			case 'W':
				CHECK_RANGE();
				for (c=0; c<nchars; c++)
					if (!isalnum(c)&&c!='_') MATCH_BIT(c);
				continue;

			case 's':
				CHECK_RANGE();
				for (c=0; c<nchars; c++)
					if (isspace(c)) MATCH_BIT(c);		//?????
				continue;

			case 'S':
				CHECK_RANGE();
				for (c=0; c<nchars; c++)
					if (!isspace(c))	MATCH_BIT(c);
				continue;

#undef CHECK_RANGE

			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
				n=c-'0';
				savep=ptr-2;
				c=*ptr;
				if ('0' <= c && c <= '7') {
					ptr++;
					n=8 * n+c-'0';

					c=*ptr;
					if ('0' <= c && c <= '7') {
						ptr++;
						i=8 * n+c-'0';
						if (i <= 0377) n=i; else ptr--;
					}
				}
				c=n;
				break;

			case 'x':
				savep=ptr; c=*ptr++;
				if (isxdigit(c)) {
					n=HexToDigit(c); c=*ptr++;
					if (isxdigit(c)) {n<<=4; n+=HexToDigit(c);}
				} else {
					ptr=savep;	/* \xZZ is xZZ (Perl does \0ZZ!) */
					n='x';
				}
				c=n;
				break;

			case 'u':
				if (isxdigit(ptr[0]) && isxdigit(ptr[1]) &&
												isxdigit(ptr[2]) && isxdigit(ptr[3])) {
					n=(((((HexToDigit(ptr[0]) << 4)
						+ HexToDigit(ptr[1])) << 4)
						+ HexToDigit(ptr[2])) << 4)
						+ HexToDigit(ptr[3]);
					c=n;
					ptr += 4;
				}
				break;

			case 'c':
				c=*ptr++;
				c=toupper(c)^0x40;	// ctrl ???
				break;
			}
		}

		if (inrange) {
			if (lastc > c) throw SY_SYNTAX;	// Bad class range
			inrange=false;
		} else {
			lastc=c;
			if (*ptr=='-' && ptr+1<end && ptr[1]!=']') {ptr++; inrange=true; continue;}
		}

		for (; lastc<=c; lastc++) {
			MATCH_BIT(lastc);
			if ((flags&RXM_NCASE)!=0) {int foldc=toupper(lastc); MATCH_BIT(foldc); foldc=tolower(foldc); MATCH_BIT(foldc);}
		}
		lastc=c;
    }
}

void RxCtx::fixNext(RxPtr rx1,RxPtr rx2,RxPtr oldnext)
{
	const bool fNext=rx2!=0 && (node(rx2).flags&RXN_ISNEXT)==0;
	for (RxPtr next; (next=node(rx1).next)!=0 && (next+=rx1)!=oldnext; rx1=next) if (node(rx1).op==RX_ALT) {
	    RxPtr child=node(rx1).child+rx1,rx,rxn;
		if (node(child).op!=RX_JUMP) {
			for (rx=child; (rxn=node(rx).next)!=0; rx+=rxn) {assert(node(rx).op!=RX_ALT);}
			setNext(rx,newNode(RX_JUMP,0,RXN_ISNEXT)); node(rx).flags|=RXN_GOODNEXT;
			fixNext(child,rx2,oldnext);
		}
	}
	if (rx2!=0) node(rx2).flags|=(node(rx2).flags&RXN_ISNEXT)==0?RXN_ISNEXT:RXN_ISJOIN;
	setNext(rx1,rx2); if (fNext) node(rx1).flags|=RXN_GOODNEXT;

	switch (node(rx1).op) {
	case RX_ALT: case RX_QNTF: case RX_STAR: case RX_PLUS: case RX_OPT: case RX_LPAREN: case RX_LPARENNON:
		{RxPtr child=node(rx1).child; fixNext(child!=0?child+rx1:0,rx2,oldnext); break;}
	}
}

void RxCtx::parse(Value &res,MemAlloc *ma,const byte *src,size_t lsrc,unsigned flags,const char *opts,size_t lopts)
{
	assert(ma!=NULL && src!=NULL && lsrc!=0);
    if (opts!=NULL) while (lopts--!=0) switch (*opts++) {
    case 'g': flags|=RXM_MULTIM; break;
	case 'i': flags|=RXM_NCASE; break;
	case 'm': flags|=RXM_MULTIL; break;
	default: throw SY_SYNTAX;
    }
	RxCtx ctx(src,lsrc,flags,ma); res.setEmpty();
	try {
		byte *p=ctx.alloc(sizeof(RegExpHdr)+ceil(lsrc,sizeof(int32_t)));
		memcpy(p+sizeof(RegExpHdr),src,lsrc);
		RxPtr rx=ctx.parseRegExp(); assert(rx!=0); ctx.fixNext(rx,ctx.newNode(RX_END));
		RegExpHdr *rh=(RegExpHdr*)ctx.mem;
		rh->rx=(uint32_t)rx; rh->lsrc=(uint32_t)lsrc; rh->nGroups=ctx.nGroups; rh->flags=(uint16_t)flags;
		res.set(ctx.mem,uint32_t(ctx.lblock-ctx.left));
	} catch (...) {
		if (ctx.mem!=NULL) ma->free(ctx.mem); throw;
	}
}

const byte *MatchCtx::matchNodes(RxNode *rx,RxNode *stop,const byte *p)
{
	const byte *p2,*childMatch,*lastChild,*source;
	byte c; unsigned i,b,num; size_t length;

	while (rx!=stop && rx) {
		switch (rx->op) {
		case RX_EMPTY:
			break;
		case RX_ALT:
			if (rx->getNext()->op!=RX_ALT) {rx=rx->getChild(); continue;}
			childMatch=matchNodes(rx->getChild(),stop,p);
			if (childMatch!=NULL) return childMatch;
			break;
		case RX_QNTF:
			lastChild=NULL;
			for (num=0; num<rx->range.rmin; num++) {
				childMatch=matchNodes(rx->getChild(),rx->getNext(),p);
				if (childMatch==NULL) return NULL;
				lastChild=p; p=childMatch;
			}
			if (num==rx->range.rmax) break;
			if (!(rx->flags&RXN_MINIMAL))
				return GMatchCtx(*this,rx,stop,num).gMatch(p,lastChild);
			p=matchChild(rx,num,rx->range.rmax,p);
			if (p==NULL) return NULL;
			break;
		case RX_PLUS:
			childMatch=matchNodes(rx->getChild(),rx->getNext(),p);
			if (childMatch==NULL) return NULL;
			if (!(rx->flags&RXN_MINIMAL))
				return GMatchCtx(*this,rx,stop,1).gMatch(childMatch,p);
			p=matchChild(rx,1,0,childMatch);
			if (p==NULL) return NULL;
			break;
		case RX_STAR:
			if (!(rx->flags&RXN_MINIMAL))
				return GMatchCtx(*this,rx,stop,0).gMatch(p,NULL);
			p=matchChild(rx,0,0,p);
			if (p==NULL) return NULL;
			break;
		case RX_OPT:
			num=rctx;
			if (rx->flags & RXN_MINIMAL) {
				const byte *restMatch=matchNodes(rx->getNext(),stop,p);
				if (restMatch!=NULL) return restMatch;
			}
			childMatch=matchNodes(rx->getChild(),rx->getNext(),p);
			if (childMatch) {
				const byte *restMatch=matchNodes(rx->getNext(),stop,childMatch);
				if (restMatch) return restMatch;
				/* need to undo the result of running the child */
			}
			rctx.trunc(num); break;
		case RX_LPARENNON:
			rx=rx->getChild(); continue;
		case RX_RPARENNON:
			break;
		case RX_LPAREN:
			num=(unsigned)rx->num+1; rx=rx->getChild();
			{RxSht sht={size_t(p-beg),0}; if (rctx.set(num,sht)!=RC_OK) return NULL;}
			continue;
		case RX_RPAREN:
			num=(unsigned)rx->num+1; assert(num<rctx);
			{RxSht sht=rctx[num]; sht.len=size_t(p-beg)-sht.sht; if (rctx.set(num,sht)!=RC_OK) return NULL;}
			break;
		case RX_ASSERT:
			if (!matchNodes(rx->getChild(),rx->getNext(),p)) return NULL;
			break;
		case RX_ASSERT_NOT:
			if (matchNodes(rx->getChild(),rx->getNext(),p)) return NULL;
			break;
		case RX_BACKREF:
			num=(unsigned)rx->num+1;
			if (num>=rctx) {ok=false; return NULL;}
			if (p+rctx[num].len>end) return NULL;
			else {
				p2=beg+rctx[num].sht; size_t len=rctx[num].len;
				if ((flags&RXM_NCASE)==0) {if (memcmp(p,p2,len)) return NULL; else p+=len;}
				else while (--len>=0) {if (!matchChar(*p++,*p2++)) return NULL;}
			}
			break;
		case RX_CCLASS:
			if (p==end) return NULL;
			if (!rx->ucclass.bitmap) {ok=false; return NULL;}
			c=*p; b=c>>3;
			if (b>=rx->ucclass.bmsize) {
				if (rx->ucclass.fnot) p++; else return NULL;
			} else {
				if (rx->getBitmap()[b]&1<<(c&7)) p++; else return NULL;
			}
			break;
		case RX_DOT:
			if (p!=end && *p!='\n') p++; else return NULL;
			break;
		case RX_DOTSTARMIN:
			for (p2=p; p2<end; p2++) {
				const byte *cp3=matchNodes(rx->getNext(),stop,p2);
				if (cp3!=NULL) return cp3;
				if (*p2=='\n') return NULL;
			}
			return NULL;
		case RX_DOTSTAR:
			for (p2=p; p2<end; p2++) if (*p2=='\n') break;
			while (p2>=p) {
				const byte *cp3=matchNodes(rx->getNext(),stop,p2);
				if (cp3!=NULL) return cp3;
				p2--;
			}
			return NULL;
		case RX_WBND:
			if (!((p==beg || !isalnum(p[-1])&&p[-1]!='_')^(p>=end || !isalnum(*p) && *p!='_'))) return NULL;
			break;
		case RX_WNBND:
			if (!((p==beg || !isalnum(p[-1])&&p[-1]!='_')^(p<end && (isalnum(*p) || *p=='_')))) return NULL;
			break;
		case RX_EOLONLY:
		case RX_EOL:
			if (p!=end && ((flags&RXM_MULTIL)==0 || *p!='\n')) return NULL;
			break;
		case RX_BOL:
			if (p!=beg) {
				if (p<end && (flags&RXM_MULTIL)!=0 && p[-1]=='\n') break;
				return NULL;
			}
			break;
		case RX_DIGIT:
			if (p!=end && isdigit(*p)) p++; else return NULL;
			break;
		case RX_NONDIGIT:
			if (p!=end && !isdigit(*p)) p++; else return NULL;
			break;
		case RX_ALNUM:
			if (p!=end && (isalnum(*p)||*p=='_')) p++; else return NULL;
			break;
		case RX_NONALNUM:
			if (p!=end && !isalnum(*p) && *p!='_') p++; else return NULL;
			break;
		case RX_SPACE:
			if (p!=end && isspace(*p)) p++; else return NULL;	//???????
			break;
		case RX_NONSPACE:
			if (p!=end && !isspace(*p)) p++; else return NULL; //????????
			break;
		case RX_CHR:
			if (p!=end && matchChar(rx->chr,*p)) p++; else return NULL;
			break;
		case RX_STR:
			source=rx->getString(); length=(size_t)rx->num;
			if (p+length>end) return NULL;
			else if (!(flags&RXM_NCASE)) {if (memcmp(p,source,length)) return NULL; else p+=length;}
			else for (i=0; i<length; i++) if (!matchChar(*p++,*source++)) return NULL;
			break;
		case RX_JUMP:
			break;
		case RX_END:
			break;
		default :
			assert(0);
			break;
		}
		rx=rx->getNext();
	}
	return p;
}

const byte *MatchCtx::matchChild(RxNode *rx,unsigned childCount,unsigned maxChild,const byte *p)
{
	const byte *match=matchNodes(rx->getNext(),NULL,p); if (match!=NULL) return p;
	const byte *childMatch=matchNodes(rx->getChild(),rx->getNext(),p);
	if (childMatch==NULL) return NULL;
	if (childMatch==p) return childMatch;
	return matchChild(rx,childCount,maxChild,childMatch);
}

const byte *MatchCtx::GMatchCtx::gMatch(const byte *ptr,const byte *previousChild)
{
    unsigned num=ctx.rctx; 
	const byte *childMatch=ctx.matchNodes(child,next,ptr);
    if (childMatch==NULL) {
		if (ctx.rctx.trunc(num)!=RC_OK) return NULL;
		if (previousChild!=NULL) ctx.matchNodes(child,next,previousChild);
	    return ctx.matchNodes(next,stop,ptr);
	}
	if (childMatch==ptr) return childMatch;
	if (maxChild==0 || ++childCount<maxChild) {
		const byte *match=gMatch(childMatch,ptr);
		if (!ctx.ok) return NULL;
		if (match!=NULL) return match;
		--childCount;
		if (ctx.rctx.trunc(num)!=RC_OK) return NULL;
		ctx.matchNodes(child,next,ptr);
	}
	const byte *match=ctx.matchNodes(next,stop,childMatch);
	if (match!=NULL) return match;
	if (ctx.rctx.trunc(num)!=RC_OK) return NULL;
	if (previousChild!=NULL) ctx.matchNodes(child,next,previousChild);
	return ctx.matchNodes(next,stop,ptr);
}

const byte *MatchCtx::match(RxNode *rx,const byte *p)
{
	for (const byte *p2=p; p2<=end; p2++) {
		skipped=p2-p; rctx.trunc(0);
		const byte *p3=matchNodes(rx,NULL,p2);
		if (!ok) return NULL;
		if (p3!=NULL) return p3;
	}
	return NULL;
}

RC MatchCtx::match(const char *str,size_t lstr,const byte *rx,RExpCtx& rctx,unsigned flags)		// start
{
	const RegExpHdr *rh=(const RegExpHdr *)rx;
	MatchCtx ctx((const byte*)str,lstr,(byte)rh->flags|flags,rctx);
	RxSht sht0={0,lstr}; rctx.set(0,sht0);
	const byte *ptr=ctx.match((RxNode*)(rx+rh->rx),(byte*)str+ctx.start);
	return ptr!=NULL && ctx.ok?RC_TRUE:RC_FALSE;
	// matchlength, last matched...
}

RC RegexService::RegexProcessor::invoke(IServiceCtx *ctx,const Value& inp,Value& out,unsigned& mode)
{
	if (inp.type!=VT_STRING && inp.type!=VT_BSTR) return RC_INVPARAM;
	Session *ses=(Session*)ctx->getSession(); RExpCtx rctx(ses); // continue, flush?
	RC rc=MatchCtx::match(inp.str,inp.length,re,rctx,flags);
	if (rc==RC_TRUE) {EvalCtx ctx(ses); ctx.rctx=&rctx; rc=ses->getStore()->queryMgr->eval(&render,ctx,&out);} else if (rc==RC_FALSE) rc=RC_OK;
	return rc;
}

void RegexService::RegexProcessor::cleanup(IServiceCtx *ctx,bool fDestroy)
{
	if (fDestroy) {
		if (re!=NULL) ctx->free((byte*)re); freeV(render);
		this->~RegexProcessor(); ctx->free(this);
	} else {
		//???
	}
}

RC RegexService::create(IServiceCtx *ctx,uint32_t& dscr,Processor *&ret)
{
	if ((dscr&ISRV_PROC_MASK)!=ISRV_READ) return RC_INVOP;
	ISession *ses=ctx->getSession(); if (ses==NULL) return RC_NOSESSION;
	const Value *ptrn=ctx->getParameter(PROP_SPEC_PATTERN),*prot=ctx->getParameter(PROP_SPEC_PROTOTYPE); 
	if (ptrn==NULL || prot==NULL) return RC_INVPARAM;
	byte *re=NULL; unsigned flg=0;
	switch (ptrn->type) {
	default: return RC_INVPARAM;
//	case VT_STRING:
//		// compile
//		break;
	case VT_BSTR:
		if (ptrn->fcalc==0) return RC_INVPARAM;
		if ((re=(byte*)ctx->malloc(ptrn->length))==NULL) return RC_NORESOURCES;
		memcpy(re,ptrn->bstr,ptrn->length); break;
	}
	Value rnd; rnd.setError(); RC rc=RC_OK;
	switch (prot->type) {
	case VT_VARREF: if (prot->refV.refN==0 && prot->refV.flags==VAR_REXP) break;
	default: rc=RC_INVPARAM; break;
	case VT_STRUCT: case VT_REF: case VT_EXPR: case VT_ARRAY: 
		if (prot->fcalc==0) rc=RC_INVPARAM; break;
	}
	if (rc==RC_OK && (rc=copyV(*prot,rnd,(ServiceCtx*)ctx))==RC_OK && (ret=new(ctx) RegexProcessor(re,rnd,flg))==NULL) rc=RC_NORESOURCES;
	if (rc!=RC_OK) {ctx->free(re); freeV(rnd);}
	return rc;
}
