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
 * various data structures and helper functions
 */
#ifndef _UTILS_H_
#define _UTILS_H_

#include "sync.h"
#include "mem.h"
#include <stddef.h>

namespace AfyDB {struct Value; struct DateTime;}

namespace AfyKernel
{

/**
 * Next Power of 2 (H. Warren, Hacker's Delight, 2003, p.48)
 */
__forceinline unsigned nextP2(unsigned x)
{
	x = x - 1;
	x = x | x >> 1;
	x = x | x >> 2;
	x = x | x >> 4;
	x = x | x >> 8;
	x = x | x >> 16;
	return x + 1;
}

/**
 * Number of 1-bits in 32-bit number (H. Warren, Hacker's Delight, 2003, p.66)
 */
__forceinline int pop(unsigned x)
{
	x = x - (x >> 1 & 0x55555555);
	x = (x & 0x33333333) + (x >> 2 & 0x33333333);
	x = x + (x>>4) & 0x0f0f0f0f; x += x>>8;
	return x+(x>>16)&0x3f;
}

/**
 * Number of 1-bits in 16-bit number
 */
__forceinline int pop16(unsigned short x)
{
	x = x - (x >> 1 & 0x5555);
	x = (x & 0x3333) + (x >> 2 & 0x3333);
	x = x + (x>>4) & 0x0f0f; 
	return x + (x>>8) & 0x1f;
}

/**
 * Number of 1-bits in 16-bit number
 */
__forceinline int pop8(unsigned char x)
{
	x = x - (x >> 1 & 0x55);
	x = (x & 0x33) + (x >> 2 & 0x33);
	return x + (x>>4) & 0x0f; 
}

/**
 * Number of leading zeros in 32-bit number (H. Warren, Hacker's Delight, 2003)
 */
__forceinline int nlz(unsigned x)
{
	x|=x>>1; x|=x>>2; x|=x>>4; x|=x>>8; return pop(~(x|x>>16));
}

/**
 * Number of trailing zeros in 32-bit number (H. Warren, Hacker's Delight, 2003)
 */
__forceinline int ntz(unsigned x)
{
	return pop(~x&x-1);
}

/**
 * 3-way comparison, returns -1,0,1 (H. Warren, Hacker's Delight, 2003)
 */
template<typename T> __forceinline int cmp3(T x,T y)
{
	return (x>y)-(x<y);
}

/**
 * sign function, returns -1,0,1 (H. Warren, Hacker's Delight, 2003)
 */
template<typename T> __forceinline int sign(T x)
{
	return (x>0)-(x<0);
}

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
 * various conversion helper functions
 */
extern	RC		strToNum(const char *str,size_t lstr,AfyDB::Value& res,const char **pend=NULL,bool fInt=false);
extern	RC		strToTimestamp(const char *str,size_t lstr,TIMESTAMP& res);
extern	RC		strToInterval(const char *str,size_t lstr,int64_t& res);
extern	RC		convDateTime(class Session *ses,TIMESTAMP dt,char *buf,int& l,bool fUTC=true);
extern	RC		convInterval(int64_t it,char *buf,int& l);
extern	RC		convDateTime(class Session *ses,TIMESTAMP dt,AfyDB::DateTime& dts,bool fUTC=true);
extern	RC		convDateTime(class Session *ses,const AfyDB::DateTime& dts,TIMESTAMP& dt,bool fUTC=true);
extern	RC		getDTPart(TIMESTAMP dt,unsigned& res,int part);

/**
 * int128 number compression and decompression macros
 */
#undef afy_rot
#undef afy_rev
#ifdef WIN32
#define afy_rot(u,n)	_lrotl(u,n)
#define afy_rev(u) _byteswap_ulong(u)
#elif defined(IA32) || defined(__x86_64__)
#define afy_rot(u,n) ({register uint32_t ret; asm ("roll %1,%0":"=r"(ret):"I"(n),"0"(u):"cc"); ret;})
__forceinline uint32_t afy_rev(uint32_t u) {register uint32_t l=u; asm ("bswapl %0":"=r"(l):"0"(l)); return l;}
#elif defined(__arm__)
#define afy_rot(u, n) ({register uint32_t ret, i=32-n; asm (" mov %0, %2, ror %1":"=r"(ret):"r"(i),"0"(u):"cc"); ret;})
__forceinline uint32_t afy_rev(uint32_t u) {register uint32_t l=u; asm ("rev %0, %1":"=r"(l):"0"(l)); return l;}
#else
__forceinline uint32_t afy_rot(uint32_t u,uint32_t n) {return u<<n|u>>(sizeof(uint32_t)*8-n);}
__forceinline uint32_t afy_rev(uint32_t u) {return afy_rot(u,8)&0x00FF00FF|afy_rot(u&0x00FF00FF,24);}
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

template<typename T> class DefCmp
{
public:
	__forceinline static int cmp(T x,T y) {return cmp3(x,y);}
	__forceinline static RC merge(T&,T&,MemAlloc*) {return RC_OK;}
};

/**
 * binary array search template
 */
template<typename T,typename Key=T,class C=DefCmp<Key>,typename N=unsigned> class BIN
{
public:
	__forceinline static const T *find(Key key,const T *arr,N nElts,T** ins=NULL) {
		N k=0; const T *r=NULL;
		if (arr!=NULL) while (nElts!=0) {
			k=nElts>>1; const T *q=&arr[k]; int c=C::cmp(*q,key);
			if (c==0) {r=q; break;} if (c>0) nElts=k; else {nElts-=++k; arr+=k; k=0;}
		}
		if (ins!=NULL) *ins=(T*)&arr[k]; return r;
	}
	__forceinline static const T *find(Key key,const T **arr,N nElts,const T*** ins=NULL) {
		N k=0; const T *r=NULL;
		if (arr!=NULL) while (nElts!=0) {
			k=nElts>>1; const T *q=arr[k]; int c=C::cmp(q,key); 
			if (c==0) {r=q; break;} if (c>0) nElts=k; else {nElts-=++k; arr+=k; k=0;}
		}
		if (ins!=NULL) *ins=(const T**)&arr[k]; return r;
	}
	__forceinline static RC insert(T *&arr,N& n,Key key,T newElt,MemAlloc *ma,N *xn=NULL) {
		T *ins=NULL; if (find(key,(const T*)arr,n,&ins)!=NULL) return RC_OK;
		if ((arr==NULL || xn==NULL || n>=*xn) && ma!=NULL) {
			ptrdiff_t sht=ins-arr;
			if ((arr=(T*)ma->realloc(arr,(xn==NULL?n+1:*xn+=(*xn==0?10:*xn/2))*sizeof(T)))==NULL) return RC_NORESOURCES;
			ins=arr+sht;
		}
		if (ins<&arr[n]) memmove(ins+1,ins,(byte*)&arr[n]-(byte*)ins); *ins=newElt; n++; return RC_OK;
	}
};

/**
 * CRC32 calculation
 */
class CRC32
{
	uint32_t	CRCTable[256];
public:
	CRC32();
	CRC		start() const {return CRC(0xFFFFFFFFul);}
	CRC		update(const byte *buf,size_t len,CRC sum) const {
		for (;len!=0; --len,++buf) sum = CRCTable[(sum^*buf)&0xff]^sum>>8;
		return sum;
	}
	CRC		finish(CRC sum) const {return ~sum;}
	CRC		checksum(const byte *buf,size_t len) const {return ~update(buf,len,CRC(~0ul));}
	static	CRC32	_CRC;
};

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
	const static ulong	nupperrng;
	const static ushort	uppersgl[];
	const static ulong	nuppersgl;
	const static ushort	lowerrng[];
	const static ulong	nlowerrng;
	const static ushort	lowersgl[];
	const static ulong	nlowersgl;
	const static ushort	otherrng[];
	const static ulong	notherrng;
	const static ushort	othersgl[];
	const static ulong	nothersgl;
	const static ushort	spacerng[];
	const static ulong	nspacerng;
	static bool	test(wchar_t wch,const ushort *p,ulong n,const ushort *sgl,ulong nsgl) {
		const ushort *q; ulong k;
		while (n>1) {k=n>>1; q=p+k*3; if (wch>=*q) {p=q; n-=k;} else n=k;}
		if (n>0 && wch>=p[0] && wch<=p[1]) return true;
		p=sgl; n=nsgl;
		while (n>1) {k=n>>1; q=p+k*2; if (wch>=*q) {p=q; n-=k;} else n=k;}
		return n>0 && wch==p[0];
	}
public:
	static int	len(byte ch) {return slen[ch];}
	static int	ulen(ulong ch) {return ch>0x10FFFF || (ch&0xFFFE)==0xFFFE || ch>=0xD800 && ch<=0xDFFF ? 0 : ch<0x80 ? 1 : ch<0x800 ? 2 : ch<0x10000 ? 3 : 4;}
	static ulong decode(ulong ch,const byte *&s,size_t xl) {
		ulong len=slen[(byte)ch]; if (len==1) return ch;
		if (len==0 || len>xl+1) return ~0u;
		byte c=*s++; ulong i=ch&0x3F;
		if (c<sec[i][0] || c>sec[i][1]) return ~0u;
		for (ch&=0x3F>>--len;;) {
			ch = ch<<6|(c&0x3F);
			if (--len==0) return (ch&0xFFFE)!=0xFFFE?ch:~0u;
			if (((c=*s++)&0xC0)!=0x80) return ~0u;
		}
	}
	static int encode(byte *s,ulong ch) {
		if (ch<0x80) {*s=(byte)ch; return 1;} int len=ulen(ch);
		for (int i=len; --i>=1; ch>>=6) s[i]=byte(ch&0x3F|0x80);
		s[0]=byte(ch|mrk[len]); return len;
	}
	static bool iswdigit(wchar_t wch) {return (ulong)wch-'0'<=9u || (ulong)wch-0xFF10<=9u;}
	static bool isrdigit(wchar_t wch) {return (ulong)wch-0x2160<=0x1F;}
	static bool iswlower(wchar_t wch) {return test(wch,lowerrng,nlowerrng,lowersgl,nlowersgl);}
	static bool iswupper(wchar_t wch,ulong& res) {
		const ushort *p=upperrng,*q; ulong n=nupperrng,k;
		while (n>1) {k=n>>1; q=p+k*3; if (wch>=*q) {p=q; n-=k;} else n=k;}
		if (n>0 && wch>=p[0] && wch<=p[1]) {res=wchar_t(wch+p[2]-500); return true;}
		p=uppersgl; n=nuppersgl;
		while (n>1) {k=n>>1; q=p+k*2; if (wch>=*q) {p=q; n-=k;} else n=k;}
		if (n>0 && wch==p[0]) {res=wchar_t(wch+p[1]-500); return true;}
		return false;
	}
	static bool iswlalpha(wchar_t wch) {
		if (test(wch,lowerrng,nlowerrng,lowersgl,nlowersgl)) return true;
		const ushort *p=otherrng,*q; ulong n=notherrng,k;
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
		const ushort *p=spacerng,*q; ulong n=nspacerng,k;
		while (n>1) {k=n>>1; q=p+k*2; if (wch>=*q) {p=q; n-=k;} else n=k;}
		return n>0 && wch>=p[0] && wch<=p[1];
	}
	static wchar_t towlower(wchar_t wch) {
		const ushort *p=upperrng,*q; ulong n=nupperrng,k;
		while (n>1) {k=n>>1; q=p+k*3; if (wch>=*q) {p=q; n-=k;} else n=k;}
		if (n>0 && wch>=p[0] && wch<=p[1]) return wchar_t(wch+p[2]-500);
		p=uppersgl; n=nuppersgl;
		while (n>1) {k=n>>1; q=p+k*2; if (wch>=*q) {p=q; n-=k;} else n=k;}
		return n>0 && wch==p[0] ? wchar_t(wch+p[1]-500) : wch;
	}
	static wchar_t towupper(wchar_t wch) {
		const ushort *p=lowerrng,*q; ulong n=nlowerrng,k;
		while (n>1) {k=n>>1; q=p+k*3; if (wch>=*q) {p=q; n-=k;} else n=k;}
		if (n>0 && wch>=p[0] && wch<=p[1]) return wchar_t(wch+p[2]-500);
		p=lowersgl; n=nlowersgl;
		while (n>1) {k=n>>1; q=p+k*2; if (wch>=*q) {p=q; n-=k;} else n=k;}
		return n>0 && wch==p[0] ? wchar_t(wch+p[1]-500) : wch;
	}
	static int wdigit(ulong ch) {return ch-0xFF10<=9u?ch-0xFF10:-1;}
};

/**
 * double-linked circular list
 * trades space for O(1) insertion and deletion operations
 */
struct DLList
{
	DLList	*prev;
	DLList	*next;
public:
	DLList() {prev=next=this;}
	void	insertFirst(DLList *elt) {elt->next=next; elt->prev=this; next=next->prev=elt;}
	void	insertLast(DLList *elt) {elt->next=this; elt->prev=prev; prev=prev->next=elt;}
	void	insertAfter(DLList *elt) {next=elt->next; prev=elt; elt->next=elt->next->prev=this;}
	void	insertBefore(DLList *elt) {next=elt; prev=elt->prev; elt->prev=elt->prev->next=this;}
	void	remove() {next->prev=prev; prev->next=next; prev=next=this;}
	bool	isInList() const {return next!=this;}
	void	reset() {prev=next=this;}
};

/**
 * hash-table chain element template
 */
template<class T> class HChain : public DLList
{
	T* const	obj;
	ulong		idx;
public:
	HChain() : obj(NULL),idx(~0u) {}
	HChain(T *t) : obj(t),idx(~0u) {}
	void	insertFirst(HChain<T> *elt) {assert(obj==NULL&&elt->obj!=NULL); DLList::insertFirst(elt);}
	void	insertLast(HChain<T> *elt) {assert(obj==NULL&&elt->obj!=NULL); DLList::insertLast(elt);}
	void	insertAfter(HChain<T> *elt) {assert(obj!=NULL); DLList::insertAfter(elt);}
	void	insertBefore(HChain<T> *elt) {assert(obj!=NULL); DLList::insertBefore(elt);}
	T*		getFirst() const {return next==this ? NULL : ((HChain<T>*)next)->obj;}
	T*		getLast() const {return prev==this ? NULL : ((HChain<T>*)prev)->obj;}
	T*		getNext() const {return ((HChain<T>*)next)->obj;}
	T*		getPrev() const {return ((HChain<T>*)prev)->obj;}
	T*		removeFirst() {HChain<T> *dl=(HChain<T>*)next; if (dl==this) return NULL; dl->remove(); return dl->obj;}
	T*		removeLast() {HChain<T> *dl=(HChain<T>*)prev; if (dl==this) return NULL; dl->remove(); return dl->obj;}
	ulong	getIndex() const {return idx;}
	void	setIndex(ulong i) {idx=i;}
	class it {
		HChain<T> *cur,*th; 
	public:
		it(HChain<T> *t) : cur(t),th(t) {}
		void reset(HChain<T> *t) {cur=th=t;}
		bool operator++() {return (cur=(HChain<T>*)cur->next)!=th;}
		bool operator--() {return (cur=(HChain<T>*)cur->prev)!=th;}
		operator HChain<T>*() const {return cur;}
		T* get() const {return cur->obj;}
	};
	class it_r {
		HChain<T> *cur,*th,*nxt; 
	public:
		it_r(HChain<T> *t) : cur(t),th(t),nxt((HChain<T>*)t->next) {}
		void reset(HChain<T> *t) {cur=th=t; nxt=t->next;}
		bool operator++() {return nxt==th?false:(nxt=(HChain<T>*)(cur=nxt)->next,true);}
		operator HChain<T>*() const {return cur;}
		T* get() const {return cur->obj;}
	};
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
	operator uint32_t() const {uint32_t hash=uint32_t(len); for (ulong i=0; i<len; i++) hash=hash<<1^str[i]; return hash;}
	bool operator==(const StrLen& key) const {return len==key.len && memcmp(str,key.str,len)==0;}
	bool operator!=(const StrLen& key) const {return len!=key.len || memcmp(str,key.str,len)!=0;}
	void operator=(const StrLen& key) {str=key.str; len=key.len;}
};

/**
 * simple hash table template
 */
template<class T,typename Key,HChain<T> T::*pList> class HashTab
{
	struct HashTabElt {
		HChain<T>		list;
		ulong			counter;
		HashTabElt()	{counter = 0;}
	};
	const	ulong		hashSize;
	const	ulong		keyMask;
	const	ulong		keyShift;
	MemAlloc			*const	ma;
	const	bool		fClear;
	HashTabElt			*hashTab;
	ulong				highwatermark;
public:
	HashTab(ulong size,MemAlloc *allc,bool fClr=true) : hashSize(nextP2(size)),keyMask(hashSize-1),keyShift(32-pop(keyMask)),ma(allc),fClear(fClr),highwatermark(0)
														{hashTab=new(allc) HashTabElt[hashSize];}
	~HashTab()	{if (fClear) {clear(); ma->free(hashTab);}}
	ulong		index(ulong hash) const {return hash*2654435769ul>>keyShift&keyMask;}
	HChain<T>	*start(ulong idx) const {assert(idx<hashSize); return &hashTab[idx].list;}
	void		insert(T *t) {insert(t, index(uint32_t(t->getKey())));}
	void		insert(T *t, ulong idx) {
		assert(idx<hashSize); HashTabElt *ht=&hashTab[idx];
		ht->list.insertFirst(&(t->*pList)); (t->*pList).setIndex(idx);
		if (++ht->counter>highwatermark) highwatermark=ht->counter;
	}
	void	remove(T *t,bool fDelete=false) {
		ulong idx=(t->*pList).getIndex(); (t->*pList).setIndex(~0u);
		if (idx<hashSize) --hashTab[idx].counter; 
		(t->*pList).remove(); if (fDelete) delete t;
	}
	void	remove(Key key,bool fDelete=false) {
		for (typename HChain<T>::it it(start(index(uint32_t(key)))); ++it;)
			{T *p = it.get(); assert(p!=NULL); if (p->getKey()==key) {remove(p,fDelete); break;}}
	}
	ulong	getHighwatermark() const {return highwatermark;}
	T*		find(Key key) const {
		for (typename HChain<T>::it it(start(index(uint32_t(key)))); ++it;)
			{T *p = it.get(); assert(p!=NULL); if (p->getKey()==key) return p;}
		return NULL;
	}
	T*		find(Key key,ulong idx) const {
		for (typename HChain<T>::it it(start(idx)); ++it;)
			{T *p = it.get(); assert(p!=NULL); if (p->getKey()==key) return p;}
		return NULL;
	}
	void clear() {
		T *t;
		for (ulong i=0; i<hashSize; i++)
			while ((t = hashTab[i].list.removeFirst())!=NULL) delete t;
	}
	friend class it;
	/**
	 * hash table iterator
	 */
	class it : public HChain<T>::it {
		const HashTab&		hashTab;
		ulong				idx;
	public:
		it(const HashTab& ht) : HChain<T>::it(ht.start(0)),hashTab(ht),idx(0) {}
		bool operator++() {
			for (;;) {
				if (HChain<T>::it::operator++()) return true;
				if (++idx>=hashTab.hashSize) return false;
				reset(hashTab.start(idx));
			}
		}
	};
	/**
	 * hash table search context
	 */
	class Find {
		const HashTab&	hashTab;
		const Key		key;
		const ulong		idx;
	public:
		Find(const HashTab& ht,Key k) : hashTab(ht),key(k),idx(ht.index(uint32_t(k))) {}
		T* find() const {
			for (typename HChain<T>::it it(hashTab.start(idx)); ++it;)
				{T *p = it.get(); assert(p!=NULL); if (p->getKey()==key) return p;}
			return NULL;
		}
		ulong getIdx() const {return idx;}
	};
};

/**
 * 'synchronised' hash table template (supports concurrent access)
 */
template<class T,typename Key,HChain<T> T::*pList,typename FKey=Key,typename FKeyArg=Key> class SyncHashTab
{
	struct HashTabElt {
		HChain<T>		list;
		RWSpin			lock;
		long			counter;
		HashTabElt() : counter(0) {}
	};
	HashTabElt			*hashTab;
	ulong				hashSize;
	int					keyShift;
	ulong				keyMask;
	long				highwatermark;
public:
	SyncHashTab(ulong size) {hashSize=nextP2(size); hashTab=new(STORE_HEAP) HashTabElt[hashSize]; keyMask=hashSize-1; keyShift=32-pop(keyMask); highwatermark=0;}
	~SyncHashTab() {free(hashTab,STORE_HEAP);}
	ulong index(ulong hash) const {return hash*2654435769ul>>keyShift&keyMask;}
	void insert(T *t) {insert(t, index(uint32_t(t->getKey())));}
	void insert(T *t, ulong idx) {
		assert(idx<hashSize); HashTabElt *ht=&hashTab[idx]; (t->*pList).setIndex(idx);
		ht->lock.lock(RW_X_LOCK); ht->list.insertFirst(&(t->*pList)); 
		if (++ht->counter>highwatermark) highwatermark=ht->counter;
		ht->lock.unlock();
	}
	void insertNoLock(T *t) {insertNoLock(t,index(uint32_t(t->getKey())));}
	void insertNoLock(T *t,ulong idx) {
		assert(idx<hashSize); HashTabElt *ht=&hashTab[idx];
		(t->*pList).setIndex(idx); ht->list.insertFirst(&(t->*pList));
		if (++ht->counter>highwatermark) highwatermark=ht->counter;
	}
	void remove(T *t,bool fDelete=false) {
		if ((t->*pList).isInList()) {
			ulong idx=(t->*pList).getIndex();
			if (idx<hashSize) {
				HashTabElt *ht=&hashTab[idx]; ht->lock.lock(RW_X_LOCK); 
				(t->*pList).setIndex(~0u); (t->*pList).remove(); 
				--ht->counter; ht->lock.unlock();
			}
		}
		if (fDelete) delete t;
	}
	void removeNoLock(T *t) {
		ulong idx=(t->*pList).getIndex();
		assert((t->*pList).isInList() && idx<hashSize);
		HashTabElt *ht=&hashTab[idx];
		(t->*pList).setIndex(~0u); (t->*pList).remove(); 
		--ht->counter; ht->lock.unlock();
	}
	void remove(Key key,bool fDelete=false) {
		HashTabElt *ht=&hashTab[index(uint32_t(key))]; ht->lock.lock(RW_U_LOCK);
		for (typename HChain<T>::it it(&ht->list); ++it;) {
			T *p=it.get(); assert(p!=NULL); 
			if (p->getKey()==key) {
				ht->lock.upgradelock(RW_X_LOCK); (p->*pList).remove(); 
				--ht->counter; ht->lock.unlock(); if (fDelete) delete p; 
				return;
			}
		}
		ht->lock.unlock(true);
	}
	ulong	getHighwatermark() const {return highwatermark;}
	T* find(Key key) const {return find(key,index(uint32_t(key)));}
	T* find(Key key,ulong idx) const {
		HashTabElt *ht=&hashTab[idx]; ht->lock.lock(RW_S_LOCK);
		typename HChain<T>::it it(&ht->list); T *ret=NULL;
		while (++it) {T *p=it.get(); assert(p!=NULL); if (p->getKey()==key) {ret=p; break;}}
		ht->lock.unlock(); return ret;
	}
	T* findLock(Key key) const {return findLock(key,index(uint32_t(key)));}
	T* findLock(Key key,ulong idx) const {
		HashTabElt *ht=&hashTab[idx]; ht->lock.lock(RW_S_LOCK);
		typename HChain<T>::it it(&ht->list);
		while (++it) {T *p=it.get(); assert(p!=NULL); if (p->getKey()==key) return p;}
		return NULL;
	}
	void lock(T* t,RW_LockType lt) {assert((t->*pList).getIndex()<hashSize); hashTab[(t->*pList).getIndex()].lock.lock(lt);}
	void unlock(Key key) {hashTab[index(uint32_t(key))].lock.unlock();}
	void unlock(T* t) {assert((t->*pList).getIndex()<hashSize); hashTab[(t->*pList).getIndex()].lock.unlock();}
	void clear() {
		for (ulong i=0; i<hashSize; i++) {
			HashTabElt *ht=&hashTab[i]; ht->lock.lock(RW_X_LOCK); T *t;
			while ((t=ht->list.removeFirst())!=NULL) delete t;
			ht->lock.unlock();
		}
	}
	friend class it;
	HChain<T>	*start(ulong idx) const {assert(idx<hashSize); return &hashTab[idx].list;}
	/**
	 * synchronised hash table iterator
	 */
	class it : public HChain<T>::it {
		const SyncHashTab&	hashTab;
		ulong				idx;
	public:
		it(const SyncHashTab& ht) : HChain<T>::it(ht.start(0)),hashTab(ht),idx(0) {}
		bool operator++() {
			for (;;) {
				if (HChain<T>::it::operator++()) return true;
				if (++idx>=hashTab.hashSize) return false;
				reset(hashTab.start(idx));
			}
		}
	};
	/**
	 * synchronized hash table search context
	 */
	class Find {
		SyncHashTab&		hashTab;
		const FKey			key;
		const ulong			idx;
		bool				fLocked;
	public:
		Find(SyncHashTab& ht,FKeyArg k) : hashTab(ht),key(k),idx(ht.index(uint32_t(key))),fLocked(false) {}
		~Find() {if (fLocked) unlock();}
		T* find() {
			assert(idx<hashTab.hashSize);
			HashTabElt *ht=&hashTab.hashTab[idx]; ht->lock.lock(RW_S_LOCK); fLocked=true;
			typename HChain<T>::it it(&ht->list); T *ret=NULL;
			while (++it) {T *p=it.get(); assert(p!=NULL); if (p->getKey()==key) {ret=p; break;}}
			ht->lock.unlock(); fLocked=false; return ret;
		}
		T* findLock(RW_LockType lt) {
			assert(idx<hashTab.hashSize);
			HashTabElt *ht=&hashTab.hashTab[idx]; ht->lock.lock(lt); fLocked=true; typename HChain<T>::it it(&ht->list);
			while (++it) {T *p=it.get(); assert(p!=NULL); if (p->getKey()==key) return p;}
			return NULL;
		}
		void remove(T *t) {if (!fLocked) hashTab.lock(t,RW_X_LOCK); hashTab.removeNoLock(t); fLocked=false;}
		void unlock() {assert(idx<hashTab.hashSize); hashTab.hashTab[idx].lock.unlock(); fLocked=false;}
		ulong getIdx() const {return idx;}
	};
};

/**
 * skip list template
 */
enum SListOp {SLO_LT, SLO_GT, SLO_NOOP, SLO_INSERT, SLO_DELETE, SLO_ERROR};

template<typename T,class SL,int xHeight=16,unsigned int factor=4> class SList
{
	struct Node {
		T		obj;
		Node	*ptrs[xHeight];
		Node()	{}
		Node(const T& o) : obj(o) {}
		void	*operator new(size_t s,int h,MemAlloc& sa) throw() {assert(h<=xHeight); return sa.memalign(sizeof(void*),s-int(xHeight-h)*sizeof(Node*));}
	};
	Node		node0;
	Node		*current;
	int			level;
	ulong		count;
	ulong		extra;
	MemAlloc&	alloc;
public:
	SList(MemAlloc& ma,ulong ex=0) : current(&node0),level(0),count(0),extra(ex),alloc(ma) {for (int i=0; i<xHeight; i++) node0.ptrs[i]=NULL;}
	~SList() {}
	SListOp	add(const T& obj,T **ret=NULL) {
		Node *update[xHeight],*node=&node0,*prev=NULL; int i=level; if (ret!=NULL) *ret=NULL;
		if (--i>=0) for (;;) {
			switch (node->ptrs[i]!=NULL&&node->ptrs[i]!=prev?SL::compare(obj,node->ptrs[i]->obj,extra):SLO_LT) {
			default: break;
			case SLO_NOOP: if (ret!=NULL) *ret=&node->ptrs[i]->obj; return SLO_NOOP;
			case SLO_GT: node=node->ptrs[i]; continue;
			case SLO_LT: prev=(update[i]=node)->ptrs[i]; if (--i>=0) continue; else break;
			case SLO_DELETE:
				for (prev=node->ptrs[i];;) {
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
	bool	operator[](const T& obj) const {
		if (level==0) return false; const Node *node=&node0,*prev=NULL;
		for (int i=level-1;;) switch (node->ptrs[i]!=NULL && node->ptrs[i]!=prev?SL::compare(obj,node->ptrs[i]->obj,extra):SLO_LT) {
		default: return true;
		case SLO_GT: node=node->ptrs[i]; continue;
		case SLO_LT: prev=node->ptrs[i]; if (--i>=0) continue; return false;
		}
	}
	ulong	getCount() const {return count;}
	size_t	nodeSize() const {return sizeof(Node);}
	void	setEtxra(ulong ex) {extra=ex;}
	void	start() {current=&node0;}
	const T* next() {return current->ptrs[0]!=NULL?&(current=current->ptrs[0])->obj:(T*)0;}
	void	*store(const void *ptr,size_t s) {void *buf=alloc.malloc(s); if (buf!=NULL) memcpy(buf,ptr,s); return buf;}
	void	clear() {alloc.release(); current=&node0; level=0; count=0; for (int i=0; i<xHeight; i++) node0.ptrs[i]=NULL;}
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
	ulong				nChunks;
	ulong				xChunks;
	ulong				nPages;
	MemAlloc *const		ma;
public:
	PageSet(MemAlloc *m) : chunks(NULL),nChunks(0),xChunks(0),nPages(0),ma(m) {}
	~PageSet() {cleanup();}
	void	destroy() {cleanup(); ma->free((void*)this);}
	void	cleanup() {
		if (ma!=NULL) {
			for (ulong i=0; i<nChunks; i++) if (chunks[i].bmp!=NULL) ma->free(chunks[i].bmp);
			ma->free(chunks); chunks=NULL; nChunks=0; xChunks=0; nPages=0;
		}
	}
	operator ulong() const {return nPages;}
	bool	operator[](PageID pid) const {
		if (nPages!=0) for (ulong n=nChunks,base=0; n>0;) {
			ulong k=n>>1; const PageSetChunk &ch=chunks[base+k];
			if (ch.from>pid) n=k; else if (ch.to<pid) {base+=k+1; n-=k+1;}
			else return ch.bmp==NULL||(ch.bmp[pid/SZ_BMP-ch.from/SZ_BMP]&1<<pid%SZ_BMP)!=0;
		}
		return false;
	}
	RC		operator+=(PageID pid) {return add(pid,pid);}
	RC		add(PageID from,PageID to);
	RC		add(const PageID *pages,ulong npg) {
		ulong i=0,j=0;
		while (++i<npg) if (pages[i]!=pages[i-1]+1)
			{RC rc=add(pages[j],pages[i-1]); if (rc==RC_OK) j=i; else return rc;}
		return add(pages[j],pages[i-1]);
	}
	RC		add(const PageSet& rhs);
	RC		operator-=(PageID);
	RC		operator-=(const PageSet&);
	RC		remove(const PageID *,ulong);
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
		ulong					idx,bidx,sht,start;
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

/**
 * dynamic unordered array template
 */
template<typename T,int initSize=10,unsigned factor=1> class DynArray
{
	MemAlloc	*const ma;
	T			tbuf[initSize];
	T			*ts;
	unsigned	nTs;
	unsigned	xTs;
public:
	DynArray(MemAlloc *m) : ma(m),ts(tbuf),nTs(0),xTs(initSize) {}
	~DynArray() {if (ts!=tbuf) ma->free(ts);}
	RC operator+=(T t) {return nTs>=xTs && !expand() ? RC_NORESOURCES : (ts[nTs++]=t,RC_OK);}
	RC operator-=(unsigned idx) {if (idx>=nTs) return RC_INVPARAM; if (idx+1<nTs) memmove(ts+idx,ts+idx+1,(nTs-idx-1)*sizeof(T)); --nTs; return RC_OK;}
	T* pop() {return nTs!=0?&ts[--nTs]:NULL;}
	T& add() {return nTs>=xTs && !expand() ? *(T*)0 : ts[nTs++];}
	T* get(uint32_t& n) {T *pt=NULL; if ((n=nTs)!=0) {if ((pt=ts)!=tbuf) ts=tbuf; else if ((pt=new(ma) T[nTs])!=NULL) memcpy(pt,ts,nTs*sizeof(T)); nTs=0;} return pt;}
	operator const T* () const {return ts;}
	operator unsigned () const {return nTs;}
	void clear() {if (ts!=tbuf) {ma->free(ts); ts=tbuf;} nTs=0; xTs=initSize;}
private:
	bool expand() {
		if (ts!=tbuf) ts=(T*)ma->realloc(ts,(xTs+=xTs/factor)*sizeof(T));
		else if ((ts=(T*)ma->malloc((xTs+=xTs/factor)*sizeof(T)))!=NULL) memcpy(ts,tbuf,sizeof(tbuf));
		return ts!=NULL;
	}
};

/**
 * dynamic ordered array template; supports binary search
 */
template<typename T,typename Key=T,class C=DefCmp<Key>,unsigned initX=16,unsigned factor=1> class DynOArray
{
	MemAlloc	*const ma;
	T			*ts;
	unsigned	nTs;
	unsigned	xTs;
public:
	DynOArray(MemAlloc *m) : ma(m),ts(NULL),nTs(0),xTs(0) {}
	~DynOArray() {if (ts!=NULL) ma->free(ts);}
	RC operator+=(T t) {return add(t);}
	RC add(T t,T **ret=NULL) {
		T *p,*ins=NULL;
		if ((p=(T*)BIN<T,Key,C>::find((Key)t,(const T*)ts,nTs,&ins))!=NULL) {if (ret!=NULL) *ret=p; return RC_FALSE;}
		if (nTs>=xTs) {ptrdiff_t sht=ins-ts; if ((ts=(T*)ma->realloc(ts,(xTs+=xTs==0?initX:xTs/factor)*sizeof(T)))==NULL) return RC_NORESOURCES; ins=ts+sht;}
		if (ins<&ts[nTs]) memmove(ins+1,ins,(byte*)&ts[nTs]-(byte*)ins); *ins=t; nTs++; if (ret!=NULL) *ret=ins; return RC_OK;
	}
	void moveTo(DynOArray<T,Key,C,initX,factor>& to) {if (to.ts!=NULL) to.ma->free(to.ts); to.ts=ts; to.nTs=nTs; to.xTs=xTs; ts=NULL; nTs=xTs=0;}
	RC merge(DynOArray<T,Key,C,initX,factor>& src) {
		if (src.nTs==0) return RC_OK; if (nTs==0) {src.moveTo(*this); return RC_OK;}
		if (xTs-nTs<src.nTs) {
			if (src.xTs-src.nTs>=nTs) {T *tt=src.ts; unsigned tn=src.nTs,tx=src.xTs; src.ts=ts; src.nTs=nTs; src.xTs=xTs; ts=tt; nTs=tn; xTs=tx;}
			else if ((ts=(T*)ma->realloc(ts,(xTs+=src.nTs)*sizeof(T)))==NULL) return RC_NORESOURCES;
		}
		T *p=ts,*from=src.ts,*const end=from+src.nTs; unsigned n=nTs; RC rc;
		do {
			T *ins,*pt=(T*)BIN<T,Key,C>::find((Key)*from,p,n,&ins);
			if (pt!=NULL) {
				if ((rc=C::merge(*pt,*from,ma))!=RC_OK || ++from>=end) {src.clear(); return rc;}
				p=pt+1; n=unsigned(&ts[nTs]-p);
			} else if ((n=unsigned(&ts[nTs]-ins))!=0) {
				Key k=(Key)*ins; unsigned nc=1; for (T* pf=from; ++pf<end && C::cmp(*pf,k)<0;) nc++;
				memmove(ins+nc,ins,n*sizeof(T)); memcpy(ins,from,nc*sizeof(T)); nTs+=nc;
				if ((from+=nc)>=end) {src.clear(); return RC_OK;} p+=nc;
			}
		} while (n!=0);
		assert(from<end); memcpy(&ts[nTs],from,(byte*)end-(byte*)from); nTs+=unsigned(end-from);
		src.clear(); return RC_OK;
	}
	T* get(uint32_t& n) {T *pt=NULL; if ((n=nTs)!=0) {pt=ts; ts=NULL; xTs=nTs=0;} return pt;}
	operator const T* () const {return ts;}
	operator unsigned () const {return nTs;}
	void clear() {if (ts!=NULL) {ma->free(ts); ts=NULL;} nTs=xTs=0;}
};

/**
 * dynamic ordered array template; supports binary search
 * contains pre-allocated buffer
 */
template<typename T,typename Key=T,class C=DefCmp<Key>,int initSize=16,unsigned factor=1> class DynOArrayBuf
{
	MemAlloc	*const ma;
	T			tbuf[initSize];
	T			*ts;
	unsigned	nTs;
	unsigned	xTs;
public:
	DynOArrayBuf(MemAlloc *m) : ma(m),ts(tbuf),nTs(0),xTs(initSize) {}
	~DynOArrayBuf() {if (ts!=tbuf) ma->free(ts);}
	RC operator+=(T t) {return add(t);}
	RC add(T t,T **ret=NULL) {
		T *p,*ins=NULL;
		if ((p=(T*)BIN<T,Key,C>::find((Key)t,(const T*)ts,nTs,&ins))!=NULL) {if (ret!=NULL) *ret=p; return RC_FALSE;}
		if (nTs>=xTs) {ptrdiff_t sht=ins-ts; if (!expand()) return RC_NORESOURCES; ins=ts+sht;}
		if (ins<&ts[nTs]) memmove(ins+1,ins,(byte*)&ts[nTs]-(byte*)ins); *ins=t; nTs++; if (ret!=NULL) *ret=ins; return RC_OK;
	}
	void moveTo(DynOArrayBuf<T,Key,C,initSize,factor>& to) {if (to.ts!=to.tbuf) to.ma->free(to.ts); if (ts==tbuf) memcpy(to.ts=to.tbuf,tbuf,sizeof(tbuf)); else to.ts=ts; to.nTs=nTs; to.xTs=xTs; ts=tbuf; nTs=0; xTs=initSize;}
	T* get(uint32_t& n) {T *pt=NULL; if ((n=nTs)!=0) {if ((pt=ts)!=tbuf) ts=tbuf; else if ((pt=new(ma) T[nTs])!=NULL) memcpy(pt,ts,nTs*sizeof(T));} return pt;}
	operator const T* () const {return ts;}
	operator unsigned () const {return nTs;}
	void clear() {if (ts!=tbuf) {ma->free(ts); ts=tbuf;} nTs=0; xTs=initSize;}
private:
	bool expand() {
		if (ts!=tbuf) ts=(T*)ma->realloc(ts,(xTs+=xTs/factor)*sizeof(T));
		else if ((ts=(T*)ma->malloc((xTs+=xTs/factor)*sizeof(T)))!=NULL) memcpy(ts,tbuf,sizeof(tbuf));
		return ts!=NULL;
	}
};

};

#endif
