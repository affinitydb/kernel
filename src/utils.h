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
 * various data structures and helper functions
 */
#ifndef _UTILS_H_
#define _UTILS_H_

#include "afyutils.h"
#include "mem.h"

namespace AfyKernel
{

#ifndef LLONG_MAX
#define	LLONG_MAX	9223372036854775807LL
#endif
#ifndef ULLONG_MAX
#define	ULLONG_MAX	0xffffffffffffffffLL
#endif

/**
 * helpers for string tables and number conversions
 */
#define	S_L(a)	byte(sizeof(a)-1),a
#define	isD(a)	(unsigned((byte)(a)-'0')<=9u)
struct Len_Str {size_t l; const char *s;};

#define	TN_CHECK(a,b,c) (a)<(c/10)||(a)==(c/10)&&(b)<=(c%10)

/**
 * standard printf formats for TIMESTAMPS and intervals
 */
#define	DATETIMEFORMAT		"%04d-%02d-%02d %02d:%02d:%02d"
#define	INTERVALFORMAT		"%d:%02d:%02d"
#define FRACTIONALFORMAT	".%06d"

/**
 * case insensitive comparison for strings of ASCII letters
 * second string must be in upper case
 */
__forceinline bool cmpncase(const char *p1,const char *p2,size_t l)
{
	for (size_t i=0; i<l; ++i,++p1,++p2) if ((*p1&~0x20)!=*p2) return false;
	return true;
}

/**
 * int128 number compression and decompression macros
 */
#undef afy_rotl
#undef afy_rev
#ifdef WIN32
#define afy_rotl(u,n)	_lrotl(u,n)
#define afy_rev(u) _byteswap_ulong(u)
#elif defined(IA32) || defined(__x86_64__)
#define afy_rotl(u,n) ({register uint32_t ret; asm ("roll %1,%0":"=r"(ret):"I"(n),"0"(u):"cc"); ret;})
__forceinline uint32_t afy_rev(uint32_t u) {register uint32_t l=u; asm ("bswapl %0":"=r"(l):"0"(l)); return l;}
#elif defined(__arm__) && !defined(_ARMLES6)
#define afy_rotl(u, n) ({register uint32_t ret, i=32-n; asm (" mov %0, %2, ror %1":"=r"(ret):"r"(i),"0"(u):"cc"); ret;})
__forceinline uint32_t afy_rev(uint32_t u) {register uint32_t l=u; asm ("rev %0, %1":"=r"(l):"0"(l)); return l;}
#else
__forceinline uint32_t afy_rotl(uint32_t u,uint32_t n) {return u<<n|u>>(sizeof(uint32_t)*8-n);}
__forceinline uint32_t afy_rev(uint32_t u) {return afy_rotl(u,8)&0x00FF00FF|afy_rotl(u&0x00FF00FF,24);}
#endif

#define	afy_dec64(a,b)	{if (((b=*a++)&1<<7)!=0 && ((b=b&(1<<7)-1|*a++<<7)&1<<14)!=0 &&													\
						((b=b&(1<<14)-1|*a++<<14)&1<<21)!=0 && ((b=b&(1<<21)-1|*a++<<21)&1<<28)!=0 &&									\
						((b=b&(1<<28)-1|uint64_t(*a++)<<28)&1ULL<<35)!=0 && ((b=b&(1ULL<<35)-1|uint64_t(*a++)<<35)&1ULL<<42)!=0 &&		\
						((b=b&(1ULL<<42)-1|uint64_t(*a++)<<42)&1ULL<<49)!=0 && ((b=b&(1ULL<<49)-1|uint64_t(*a++)<<49)&1ULL<<56)!=0 &&	\
						((b=b&(1ULL<<56)-1|uint64_t(*a++)<<56)&1ULL<<63)!=0) b=b&(1ULL<<63)-1|uint64_t(*a++)<<63;}
#define afy_dec32(a,b)	{if (((b=*a++)&1<<7)!=0 && ((b=b&(1<<7)-1|(*a++)<<7)&1<<14)!=0 &&										\
						((b=b&(1<<14)-1|(*a++)<<14)&1<<21)!=0 && ((b=b&(1<<21)-1|(*a++)<<21)&1<<28)!=0)	b=b&(1<<28)-1|*a++<<28;}
#define afy_dec16(a,b)	{if (((b=*a++)&1<<7)!=0 && ((b=b&(1<<7)-1|(*a++)<<7)&1<<14)!=0) b=b&(1<<14)-1|(*a++&3)<<14;}
#define	afy_dec8(a,b)	{if (((b=*a++)&1<<7)!=0) b=b&(1<<7)-1|(*a++)<<7;}

#define	afy_dec64b(a,b,c)	{if (((b=*a++)&1<<7)!=0 && a<c && ((b=b&(1<<7)-1|*a++<<7)&1<<14)!=0 && a<c &&												\
							((b=b&(1<<14)-1|*a++<<14)&1<<21)!=0 && a<c && ((b=b&(1<<21)-1|*a++<<21)&1<<28)!=0 && a<c &&									\
							((b=b&(1<<28)-1|uint64_t(*a++)<<28)&1ULL<<35)!=0 && a<c && ((b=b&(1ULL<<35)-1|uint64_t(*a++)<<35)&1ULL<<42)!=0 && a<c &&	\
							((b=b&(1ULL<<42)-1|uint64_t(*a++)<<42)&1ULL<<49)!=0 && a<c && ((b=b&(1ULL<<49)-1|uint64_t(*a++)<<49)&1ULL<<56)!=0 && a<c &&	\
							((b=b&(1ULL<<56)-1|uint64_t(*a++)<<56)&1ULL<<63)!=0 && a<c) b=b&(1ULL<<63)-1|uint64_t(*a++)<<63;}
#define afy_dec32b(a,b,c)	{if (((b=*a++)&1<<7)!=0 && a<c && ((b=b&(1<<7)-1|(*a++)<<7)&1<<14)!=0 && a<c &&								\
							((b=b&(1<<14)-1|(*a++)<<14)&1<<21)!=0 && a<c && ((b=b&(1<<21)-1|(*a++)<<21)&1<<28)!=0 && a<c)	b=b&(1<<28)-1|*a++<<28;}
#define afy_dec16b(a,b,c)	{if (((b=*a++)&1<<7)!=0 && a<c && ((b=b&(1<<7)-1|(*a++)<<7)&1<<14)!=0 && a<c) b=b&(1<<14)-1|(*a++&3)<<14;}
#define	afy_dec8b(a,b,c)		{if (((b=*a++)&1<<7)!=0 && a<c) b=b&(1<<7)-1|(*a++)<<7;}

#define	CHECK_dec16(a,b,c)	{afy_dec16b(a,b,c); if (a==c && (a[-1]&0x80)!=0) return RC_CORRUPTED;}
#define	CHECK_dec32(a,b,c)	{afy_dec32b(a,b,c); if (a==c && (a[-1]&0x80)!=0) return RC_CORRUPTED;}
#define	CHECK_dec64(a,b,c)	{afy_dec64b(a,b,c); if (a==c && (a[-1]&0x80)!=0) return RC_CORRUPTED;}

#define	afy_enc64(a,b)	{if (b<1u<<7) *a++=byte(b);										\
						else {*a++=byte(b|0x80); if (b<1u<<14) *a++=byte(b>>7);			\
						else {*a++=byte(b>>7|0x80); if (b<1u<<21) *a++=byte(b>>14);		\
						else {*a++=byte(b>>14|0x80); if (b<1u<<28) *a++=byte(b>>21);	\
						else {*a++=byte(b>>21|0x80); if (b<1ULL<<35) *a++=byte(b>>28);	\
						else {*a++=byte(b>>28|0x80); if (b<1ULL<<42) *a++=byte(b>>35);	\
						else {*a++=byte(b>>35|0x80); if (b<1ULL<<49) *a++=byte(b>>42);	\
						else {*a++=byte(b>>42|0x80); if (b<1ULL<<56) *a++=byte(b>>49);	\
						else {*a++=byte(b>>49|0x80); if (b<1ULL<<63) *a++=byte(b>>56);	\
						else {*a++=byte(b>>56|0x80); *a++=byte(b>>63);}}}}}}}}}}
#define afy_enc32(a,b)	{if (b<1u<<7) *a++=byte(b);										\
						else {*a++=byte(b|0x80); if (b<1u<<14) *a++=byte(b>>7);			\
						else {*a++=byte(b>>7|0x80); if (b<1u<<21) *a++=byte(b>>14);		\
						else {*a++=byte(b>>14|0x80); if (b<1u<<28) *a++=byte(b>>21);	\
						else {*a++=byte(b>>21|0x80); *a++=byte(b>>28);}}}}}
#define afy_enc16(a,b)	{if (b<1u<<7) *a++=byte(b);										\
						else {*a++=byte(b|0x80); if (b<1u<<14) *a++=byte(b>>7);			\
						else {*a++=byte(b>>7|0x80); *a++=byte(b>>14&3);}}}
#define afy_enc8(a,b)	{if (((*a++=byte(b))&0x80)!=0) *a++=1;}

#define	afy_adv64(a)		{if ((*a++&0x80)!=0 && (*a++&0x80)!=0 && (*a++&0x80)!=0 && (*a++&0x80)!=0) && (*a++&0x80)!=0)\
							&& (*a++&0x80)!=0) && (*a++&0x80)!=0) && (*a++&0x80)!=0) && (*a++&0x80)!=0)) a++;}
#define	afy_adv32(a)		{if ((*a++&0x80)!=0 && (*a++&0x80)!=0 && (*a++&0x80)!=0 && (*a++&0x80)!=0) a++;}
#define	afy_adv16(a)		{if ((*a++&0x80)!=0 && (*a++&0x80)!=0) a++;}
#define	afy_adv8(a)		{if ((*a++&0x80)!=0) a++;}

#define	afy_len64(a)		(a<1u<<7?1:a<1u<<14?2:a<1u<<21?3:a<1u<<28?4:a<1ULL<<35?5:a<1ULL<<42?6:a<1ULL<<49?7:a<1ULL<<56?8:a<1ULL<<63?9:10)
#define	afy_len32(a)		(a<1u<<7?1:a<1u<<14?2:a<1u<<21?3:a<1u<<28?4:5)
#define	afy_len16(a)		(a<1u<<7?1:a<1u<<14?2:3)
#define	afy_len8(a)		(byte(a)<1u<<7?1:2)

#define afy_enc32r(a,b)	{if (b<1u<<7) *a++=byte(b);																							\
						else if (b<1u<<14) {a[0]=byte(b>>7); a[1]=byte(b|0x80); a+=2;}														\
						else if (b<1u<<21) {a[0]=byte(b>>14); a[1]=byte(b>>7|0x80); a[2]=byte(b|0x80); a+=3;}								\
						else if (b<1u<<28) {a[0]=byte(b>>21); a[1]=byte(b>>14|0x80); a[2]=byte(b>>7|0x80); a[3]=byte(b|0x80); a+=4;}		\
						else {a[0]=byte(b>>28); a[1]=byte(b>>21|0x80); a[2]=byte(b>>14|0x80); a[3]=byte(b>>7|0x80); a[4]=byte(b|0x80); a+=5;}}

#define afy_dec32r(a,b)	{if (((b=*--a)&1<<7)!=0 && ((b=b&(1<<7)-1|(*--a)<<7)&1<<14)!=0 &&				\
						((b=b&(1<<14)-1|(*--a)<<14)&1<<21)!=0 && ((b=b&(1<<21)-1|(*--a)<<21)&1<<28)!=0)	\
						b=b&(1<<28)-1|*--a<<28;}
#define	afy_adv32r(a)	{if ((*--a&0x80)!=0 && (*--a&0x80)!=0 && (*--a&0x80)!=0 && (*--a&0x80)!=0) --a;}

#define	afy_enc32zz(a)	((a)<<1^int32_t(a)>>31)
#define	afy_enc64zz(a)	((a)<<1^int64_t(a)>>63)
#define	afy_dec32zz(a)	int32_t(uint32_t(a)>>1^-int32_t((a)&1))
#define	afy_dec64zz(a)	int64_t(uint64_t(a)>>1^-int64_t((a)&1))

/**
 * UTF8 helper functions
 * conversions, type character, etc.
 */
class UTF8
{
	const static byte	slen[256];
	const static byte	sec[64][2];
	const static byte	mrk[7];
	const static ushort	upperrng[];
	const static unsigned	nupperrng;
	const static ushort	uppersgl[];
	const static unsigned	nuppersgl;
	const static ushort	lowerrng[];
	const static unsigned	nlowerrng;
	const static ushort	lowersgl[];
	const static unsigned	nlowersgl;
	const static ushort	otherrng[];
	const static unsigned	notherrng;
	const static ushort	othersgl[];
	const static unsigned	nothersgl;
	const static ushort	spacerng[];
	const static unsigned	nspacerng;
	static bool	test(wchar_t wch,const ushort *p,unsigned n,const ushort *sgl,unsigned nsgl) {
		const ushort *q; unsigned k;
		while (n>1) {k=n>>1; q=p+k*3; if (wch>=*q) {p=q; n-=k;} else n=k;}
		if (n>0 && wch>=p[0] && wch<=p[1]) return true;
		p=sgl; n=nsgl;
		while (n>1) {k=n>>1; q=p+k*2; if (wch>=*q) {p=q; n-=k;} else n=k;}
		return n>0 && wch==p[0];
	}
public:
	static int	len(byte ch) {return slen[ch];}
	static int	ulen(unsigned ch) {return ch>0x10FFFF || (ch&0xFFFE)==0xFFFE || ch>=0xD800 && ch<=0xDFFF ? 0 : ch<0x80 ? 1 : ch<0x800 ? 2 : ch<0x10000 ? 3 : 4;}
	static unsigned decode(unsigned ch,const byte *&s,size_t xl) {
		unsigned len=slen[(byte)ch]; if (len==1) return ch;
		if (len==0 || len>xl+1) return ~0u;
		byte c=*s++; unsigned i=ch&0x3F;
		if (c<sec[i][0] || c>sec[i][1]) return ~0u;
		for (ch&=0x3F>>--len;;) {
			ch = ch<<6|(c&0x3F);
			if (--len==0) return (ch&0xFFFE)!=0xFFFE?ch:~0u;
			if (((c=*s++)&0xC0)!=0x80) return ~0u;
		}
	}
	static int encode(byte *s,unsigned ch) {
		if (ch<0x80) {*s=(byte)ch; return 1;} int len=ulen(ch);
		for (int i=len; --i>=1; ch>>=6) s[i]=byte(ch&0x3F|0x80);
		s[0]=byte(ch|mrk[len]); return len;
	}
	static bool iswdigit(wchar_t wch) {return (unsigned)wch-'0'<=9u || (unsigned)wch-0xFF10<=9u;}
	static bool isrdigit(wchar_t wch) {return (unsigned)wch-0x2160<=0x1F;}
	static bool iswlower(wchar_t wch) {return test(wch,lowerrng,nlowerrng,lowersgl,nlowersgl);}
	static bool iswupper(wchar_t wch,unsigned& res) {
		const ushort *p=upperrng,*q; unsigned n=nupperrng,k;
		while (n>1) {k=n>>1; q=p+k*3; if (wch>=*q) {p=q; n-=k;} else n=k;}
		if (n>0 && wch>=p[0] && wch<=p[1]) {res=wchar_t(wch+p[2]-500); return true;}
		p=uppersgl; n=nuppersgl;
		while (n>1) {k=n>>1; q=p+k*2; if (wch>=*q) {p=q; n-=k;} else n=k;}
		if (n>0 && wch==p[0]) {res=wchar_t(wch+p[1]-500); return true;}
		return false;
	}
	static bool iswlalpha(wchar_t wch) {
		if (test(wch,lowerrng,nlowerrng,lowersgl,nlowersgl)) return true;
		const ushort *p=otherrng,*q; unsigned n=notherrng,k;
		while (n>1) {k=n>>1; q=p+k*2; if (wch>=*q) {p=q; n-=k;} else n=k;}
		if (n>0 && wch>=p[0] && wch<=p[1]) return true;
		p=othersgl; n=nothersgl;
		while (n>1) {k=n>>1; q=p+k; if (wch>=*q) {p=q; n-=k;} else n=k;}
		return n>0 && wch==p[0];
	}
	static bool iswalpha(wchar_t wch) {
		return iswlalpha(wch) || test(wch,upperrng,nupperrng,uppersgl,nuppersgl);
	}
	static bool iswalnum(wchar_t wch) {
		return iswdigit(wch) || iswlalpha(wch) || test(wch,upperrng,nupperrng,uppersgl,nuppersgl);
	}
	static bool iswspace(wchar_t wch) {
		const ushort *p=spacerng,*q; unsigned n=nspacerng,k;
		while (n>1) {k=n>>1; q=p+k*2; if (wch>=*q) {p=q; n-=k;} else n=k;}
		return n>0 && wch>=p[0] && wch<=p[1];
	}
	static wchar_t towlower(wchar_t wch) {
		const ushort *p=upperrng,*q; unsigned n=nupperrng,k;
		while (n>1) {k=n>>1; q=p+k*3; if (wch>=*q) {p=q; n-=k;} else n=k;}
		if (n>0 && wch>=p[0] && wch<=p[1]) return wchar_t(wch+p[2]-500);
		p=uppersgl; n=nuppersgl;
		while (n>1) {k=n>>1; q=p+k*2; if (wch>=*q) {p=q; n-=k;} else n=k;}
		return n>0 && wch==p[0] ? wchar_t(wch+p[1]-500) : wch;
	}
	static wchar_t towupper(wchar_t wch) {
		const ushort *p=lowerrng,*q; unsigned n=nlowerrng,k;
		while (n>1) {k=n>>1; q=p+k*3; if (wch>=*q) {p=q; n-=k;} else n=k;}
		if (n>0 && wch>=p[0] && wch<=p[1]) return wchar_t(wch+p[2]-500);
		p=lowersgl; n=nlowersgl;
		while (n>1) {k=n>>1; q=p+k*2; if (wch>=*q) {p=q; n-=k;} else n=k;}
		return n>0 && wch==p[0] ? wchar_t(wch+p[1]-500) : wch;
	}
	static int wdigit(unsigned ch) {return ch-0xFF10<=9u?ch-0xFF10:-1;}
};

/**
 * string reference key for hash tables
 */
class StrRefKey
{
	friend class StrKey;
	const char	*const str;
public:
	StrRefKey(const char *s) : str(s) {}
	operator uint32_t() const {uint32_t hash=0; for (const char *s=str; *s!='\0'; hash=hash<<1^*s++); return hash;}
	operator const char*() const {return str;}
	bool operator==(const char *key2) const {return strcmp(str,key2)==0;}
	bool operator!=(const char *key2) const {return strcmp(str,key2)!=0;}
};

/**
 * string key (copied, zero terminated) for hash tables
 */
class StrKey
{
	char	*str;
public:
	StrKey() {str=NULL;}
	StrKey(const char *s,bool fCopy=true) {str=fCopy?strdup(s,STORE_HEAP):(char*)s;}
	~StrKey() {free(str,STORE_HEAP);}
	operator uint32_t() const {uint32_t hash=0; for (const char *s=str; *s!='\0'; hash=hash<<1^*s++); return hash;}
	operator const char*() const {return str;}
	bool operator==(const char *key2) const {return strcmp(str,key2)==0;}
	bool operator!=(const char *key2) const {return strcmp(str,key2)!=0;}
	bool operator==(const StrRefKey& key2) const {return strcmp(str,key2.str)==0;}
	bool operator!=(const StrRefKey& key2) const {return strcmp(str,key2.str)!=0;}
	void operator=(const char *key) {if (str!=NULL) free(str,STORE_HEAP); str=strdup(key,STORE_HEAP);}
	void set(char *s) {if (str!=NULL) free(str,STORE_HEAP); str=s;}
	void reset() {str=NULL;}
};

/**
 * string with length
 */
struct StrLen
{
	const	char	*str;
	size_t			len;
	StrLen() : str(NULL),len(0) {}
	StrLen(const char *s) : str(s),len(strlen(s)) {}
	StrLen(const char *s,size_t l) : str(s),len(l) {}
public:
	operator const char*() const {return str;}
	operator uint32_t() const {uint32_t hash=uint32_t(len); for (unsigned i=0; i<len; i++) hash=hash<<1^str[i]; return hash;}
	bool operator==(const StrLen& key) const {return len==key.len && memcmp(str,key.str,len)==0;}
	bool operator!=(const StrLen& key) const {return len!=key.len || memcmp(str,key.str,len)!=0;}
	void operator=(const StrLen& key) {str=key.str; len=key.len;}
};

/**
 * skip list template
 */
enum SListOp {SLO_LT, SLO_GT, SLO_NOOP, SLO_INSERT, SLO_DELETE, SLO_ERROR};

template<typename T,class SL,int xHeight=16,unsigned factor=4> class SList
{
	struct Node {
		T		obj;
		Node	*ptrs[xHeight];
		Node()	{}
		Node(const T& o) : obj(o) {}
		void	*operator new(size_t s,int h,MemAlloc& sa) throw() {assert(h<=xHeight); return sa.malloc(s-int(xHeight-h)*sizeof(Node*));}
	};
	Node		node0;
	Node		*current;
	int			level;
	unsigned	count;
	unsigned	extra;
	MemAlloc&	alloc;
public:
	SList(MemAlloc& ma,unsigned ex=0) : current(&node0),level(0),count(0),extra(ex),alloc(ma) {for (int i=0; i<xHeight; i++) node0.ptrs[i]=NULL;}
	~SList() {}
	SListOp	add(const T& obj,T **ret=NULL) {
		Node *update[xHeight],*node=&node0,*prev=NULL,*nd; int i=level; if (ret!=NULL) *ret=NULL;
		if (--i>=0) for (;;) {
			switch ((nd=node->ptrs[i])!=NULL&&nd!=prev?SL::compare(obj,nd->obj,extra,alloc):SLO_LT) {
			default: assert(0);
			case SLO_ERROR: return SLO_ERROR;
			case SLO_INSERT: break;
			case SLO_NOOP: if (ret!=NULL) *ret=&nd->obj; return SLO_NOOP;
			case SLO_GT: node=nd; continue;
			case SLO_LT: prev=nd; update[i]=node; if (--i>=0) continue; else break;
			case SLO_DELETE:
				for (prev=nd;;) {
					node->ptrs[i]=prev->ptrs[i]; if (--i<0) break;
					while (node->ptrs[i]!=prev) {assert(node->ptrs[i]!=NULL); node=node->ptrs[i];}
				}
				assert(count!=0); count--; return SLO_DELETE;
			}
			break;
		}
		for (i=1; ;i++) if (i>=xHeight || rand()*factor>=RAND_MAX) {
			if (i>level) for (assert(i<=xHeight); level<i; level++) update[level]=&node0;
			if ((node=new(i,alloc) Node(obj))==NULL) return SLO_ERROR; count++;
			while (--i>=0) {node->ptrs[i]=update[i]->ptrs[i]; update[i]->ptrs[i]=node;}
			if (ret!=NULL) *ret=&node->obj; return SLO_INSERT;
		}
	}
	SListOp	addInt(T& obj) {
		Node *update[xHeight],*node=&node0,*prev=NULL,*nd; int i=level;
		if (--i>=0) for (;;) {
			switch ((nd=node->ptrs[i])!=NULL&&nd!=prev?SL::compare(obj,nd->obj,nd->ptrs[0]!=NULL?&nd->ptrs[0]->obj:NULL,node==&node0,alloc):SLO_LT) {
			default: assert(0);
			case SLO_ERROR: return SLO_ERROR;
			case SLO_NOOP: return SLO_NOOP;
			case SLO_GT: node=nd; continue;
			case SLO_LT: prev=nd; update[i]=node; if (--i>=0) continue; else break;
			case SLO_INSERT: while (i>=0) update[i--]=nd; break;
			}
			break;
		}
		for (i=1; ;i++) if (i>=xHeight || rand()*factor>=RAND_MAX) {
			if (i>level) for (assert(i<=xHeight); level<i; level++) update[level]=&node0;
			if ((node=new(i,alloc) Node(obj))==NULL) return SLO_ERROR; count++;
			while (--i>=0) {node->ptrs[i]=update[i]->ptrs[i]; update[i]->ptrs[i]=node;}
			return SLO_INSERT;
		}
	}
	bool	operator[](const T& obj) const {
		if (level==0) return false; const Node *node=&node0,*prev=NULL,*nd;
		for (int i=level-1;;) switch ((nd=node->ptrs[i])!=NULL && nd!=prev?SL::compare(obj,nd->obj,extra,alloc):SLO_LT) {
		default: return true;
		case SLO_GT: node=nd; continue;
		case SLO_LT: prev=nd; if (--i>=0) continue; return false;
		}
	}
	unsigned	getCount() const {return count;}
	size_t		nodeSize() const {return sizeof(Node);}
	unsigned	getExtra() const {return extra;}
	void		setExtra(unsigned ex) {extra=ex;}
	void		start() {current=&node0;}
	const T*	next() {return current->ptrs[0]!=NULL?&(current=current->ptrs[0])->obj:(T*)0;}
	void		*store(const void *ptr,size_t s) {void *buf=alloc.malloc(s); if (buf!=NULL) memcpy(buf,ptr,s); return buf;}
	void		clear() {alloc.release(); current=&node0; level=0; count=0; for (int i=0; i<xHeight; i++) node0.ptrs[i]=NULL;}
};

/**
 * priority queue template (based on skip list)
 */

template<typename T,typename P,int xHeight=8,unsigned factor=4> class PQueue
{
	struct Node {
		P		prty;
		T		*obj;
		Node	*ptrs[xHeight];
		Node()	: obj(NULL) {}
		Node(P pr,T *o) : prty(pr),obj(o) {}
	};
	Node					node0;
	RWLock					lock;
	int						level;
	volatile	unsigned	count;
	MemAlloc&				alloc;
	Node					*freeNodes;
public:
	PQueue(MemAlloc& ma) : level(0),count(0),alloc(ma),freeNodes(NULL) {for (int i=0; i<xHeight; i++) node0.ptrs[i]=NULL;}
	~PQueue() {}
	RC	push(T *obj,P prty) {
		Node *update[xHeight],*node=&node0,*prev=NULL; RWLockP lck(&lock,RW_X_LOCK); int i=level;
		if (--i>=0) for (;;) {
			if (node->ptrs[i]!=NULL&&node->ptrs[i]!=prev&&cmp3(prty,node->ptrs[i]->prty)>=0) node=node->ptrs[i];
			else {prev=(update[i]=node)->ptrs[i]; if (--i<0) break;}
		}
		for (i=1; ;i++) if (i>=xHeight || rand()*factor>=RAND_MAX) {
			if (i>level) for (assert(i<=xHeight); level<i; level++) update[level]=&node0;
			if (freeNodes!=NULL) {void *p=freeNodes; freeNodes=freeNodes->ptrs[0]; node=new(p) Node(prty,obj);}
			else if ((node=new(&alloc) Node(prty,obj))==NULL) return RC_NORESOURCES;
			while (--i>=0) {node->ptrs[i]=update[i]->ptrs[i]; update[i]->ptrs[i]=node;}
			count++; return node0.ptrs[0]==node?RC_TRUE:RC_OK;
		}
	}
	T	*pop(P *pPrty=NULL) {
		RWLockP lck(&lock,RW_X_LOCK);
		Node *node=node0.ptrs[0]; if (node==NULL) return NULL;
		T *ret=node->obj; if (pPrty!=NULL) *pPrty=node->prty;
		for (int i=level; --i>=0;) if (node0.ptrs[i]==node) node0.ptrs[i]=node->ptrs[i];
		node->ptrs[0]=freeNodes; freeNodes=node; assert(count!=0); count--;
		return ret;
	}
	T	*top(P *pPrty=NULL) {
		RWLockP lck(&lock,RW_S_LOCK);
		if (node0.ptrs[0]==NULL) return NULL;
		if (pPrty!=NULL) *pPrty=node0.ptrs[0]->prty;
		return node0.ptrs[0]->obj;
	}
	unsigned	getCount() const {return count;}
};

/**
 * compressed page set descriptor
 * (n.b. bitmaps are not implemented yet!)
 */
#define SZ_BMP (sizeof(uint32_t)*8)

class PageSet
{
	struct	PageSetChunk {
		PageID		from,to;
		uint32_t	*bmp;
		uint32_t	npg;
	};
	PageSetChunk		*chunks;
	unsigned				nChunks;
	unsigned				xChunks;
	unsigned				nPages;
	MemAlloc *const		ma;
public:
	PageSet(MemAlloc *m) : chunks(NULL),nChunks(0),xChunks(0),nPages(0),ma(m) {}
	~PageSet() {cleanup();}
	void	destroy() {cleanup(); ma->free((void*)this);}
	void	cleanup() {
		if (ma!=NULL) {
			for (unsigned i=0; i<nChunks; i++) if (chunks[i].bmp!=NULL) ma->free(chunks[i].bmp);
			ma->free(chunks); chunks=NULL; nChunks=0; xChunks=0; nPages=0;
		}
	}
	operator unsigned() const {return nPages;}
	bool	operator[](PageID pid) const {
		if (nPages!=0) for (unsigned n=nChunks,base=0; n>0;) {
			unsigned k=n>>1; const PageSetChunk &ch=chunks[base+k];
			if (ch.from>pid) n=k; else if (ch.to<pid) {base+=k+1; n-=k+1;}
			else return ch.bmp==NULL||(ch.bmp[pid/SZ_BMP-ch.from/SZ_BMP]&1<<pid%SZ_BMP)!=0;
		}
		return false;
	}
	RC		operator+=(PageID pid) {return add(pid,pid);}
	RC		add(PageID from,PageID to);
	RC		add(const PageID *pages,unsigned npg) {
		unsigned i=0,j=0;
		while (++i<npg) if (pages[i]!=pages[i-1]+1)
			{RC rc=add(pages[j],pages[i-1]); if (rc==RC_OK) j=i; else return rc;}
		return add(pages[j],pages[i-1]);
	}
	RC		add(const PageSet& rhs);
	RC		operator-=(PageID);
	RC		operator-=(const PageSet&);
	RC		remove(const PageID *,unsigned);
	PageID	pop();
	void	test() const;
	RC		operator+=(PageSet& rhs) {
		if (rhs.nPages!=0) {
			if (chunks!=NULL) return add(rhs);
			chunks=rhs.chunks; rhs.chunks=NULL; nChunks=rhs.nChunks; rhs.nChunks=0;
			xChunks=rhs.xChunks; rhs.xChunks=0; nPages=rhs.nPages; rhs.nPages=0; assert(ma==rhs.ma);
		}
		return RC_OK;
	}
	void	operator=(PageSet& rhs) {
		chunks=rhs.chunks; rhs.chunks=NULL; nChunks=rhs.nChunks; rhs.nChunks=0;
		xChunks=rhs.xChunks; rhs.xChunks=0; nPages=rhs.nPages; rhs.nPages=0; assert(ma==rhs.ma);
	}
	class it {
		const	PageSet			&set;
		const	PageSetChunk	*chunk;
		unsigned					idx,bidx,sht,start;
	public:
		it(const PageSet& ps) : set(ps),chunk(ps.nChunks>0?ps.chunks:(PageSetChunk*)0),idx(0),bidx(~0u),sht(0),start(0) {}
		const PageSet& getPageSet() const {return set;}
		PageID	operator++() {
			if (chunk!=NULL) {
				do if (chunk->bmp!=NULL) {
					if (bidx==~0u) start=chunk->from-(sht=chunk->from%SZ_BMP),bidx=0; 
					for (PageID pid; (pid=start+bidx*SZ_BMP+sht)<=chunk->to;)
						{uint32_t u=chunk->bmp[bidx]&1<<sht; if (++sht>=SZ_BMP) ++bidx,sht=0; if (u!=0) return pid;}
					bidx=~0u;
				} else if (chunk->from+sht<=chunk->to) return chunk->from+sht++; while ((sht=0,++chunk,++idx)<set.nChunks);
				chunk=NULL;
			}
			return INVALID_PAGEID;
		}
	};
};

template<typename T> class DefCmpM
{
public:
	__forceinline static int cmp(T x,T y) {return cmp3(x,y);}
	__forceinline static RC merge(T&,T&,MemAlloc*) {return RC_OK;}
};

/**
 * dynamic ordered array template; supports binary search & merge
 */
template<typename T,typename Key=T,class C=DefCmpM<Key>,unsigned initX=16,unsigned factor=1> class DynOArrayM : public DynOArray<T,Key,C,initX,factor>
{
	MemAlloc	*const mma;
public:
	DynOArrayM(MemAlloc *m) : DynOArray<T,Key,C,initX,factor>(m),mma(m) {}
	RC merge(DynOArrayM<T,Key,C,initX,factor>& src) {
		if (src.nTs==0) return RC_OK; if (this->nTs==0) {src.moveTo(*this); return RC_OK;}
		if (this->xTs-this->nTs<src.nTs) {
			if (src.xTs-src.nTs>=this->nTs) {T *tt=src.ts; unsigned tn=src.nTs,tx=src.xTs; src.ts=this->ts; src.nTs=this->nTs; src.xTs=this->xTs; this->ts=tt; this->nTs=tn; this->xTs=tx;}
			else {size_t old=this->xTs*sizeof(T); if ((this->ts=(T*)mma->realloc(this->ts,(this->xTs+=src.nTs)*sizeof(T),old))==NULL) return RC_NORESOURCES;}
		}
		T *p=this->ts,*from=src.ts,*const end=from+src.nTs; unsigned n=this->nTs; RC rc;
		do {
			T *ins,*pt=(T*)BIN<T,Key,C>::find((Key)*from,p,n,&ins);
			if (pt!=NULL) {
				if ((rc=C::merge(*pt,*from,mma))!=RC_OK || ++from>=end) {src.clear(); return rc;}
				p=pt+1; n=unsigned(&this->ts[this->nTs]-p);
			} else if ((n=unsigned(&this->ts[this->nTs]-ins))!=0) {
				Key k=(Key)*ins; unsigned nc=1; for (T* pf=from; ++pf<end && C::cmp(*pf,k)<0;) nc++;
				memmove(ins+nc,ins,n*sizeof(T)); memcpy(ins,from,nc*sizeof(T)); this->nTs+=nc;
				if ((from+=nc)>=end) {src.clear(); return RC_OK;} p+=nc;
			}
		} while (n!=0);
		assert(from<end); memcpy(&this->ts[this->nTs],from,(byte*)end-(byte*)from); this->nTs+=unsigned(end-from);
		src.clear(); return RC_OK;
	}
};

};

#endif
