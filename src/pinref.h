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

Written by Mark Venguerov 2008 - 2012

**************************************************************************************/

/**
 * compressed PIN reference format descriptor
 */

#ifndef _PINREF_H_
#define _PINREF_H_

#include "affinity.h"
#include "utils.h"

using namespace Afy;

namespace AfyKernel
{

#define	XPINREFSIZE	64

/**
 * field flags
 */
#define PR_PID2		0x0001
#define	PR_ADDR		0x0002
#define	PR_ADDR2	0x0004
#define	PR_U1		0x0008
#define	PR_U2		0x0010
#define	PR_COUNT	0x0020
#define	PR_FCOLL	0x0040
#define	PR_PREF32	0x0080
#define PR_PREF64	0x0100
#define	PR_SPECIAL	0x0200
#define	PR_HIDDEN	0x0400

/**
 * decompressed representation
 * used to assemble data for compression and to extract data after decomression
 */
struct PINRef
{
	const ushort	stID;
	uint16_t		def;
	PID				id;
	PID				id2;
	PageAddr		addr;
	PageAddr		addr2;
	uint32_t		u1;
	uint32_t		u2;
	uint32_t		count;
	uint64_t		prefix;

	PINRef(ushort storeID,const byte *p);
	PINRef(ushort storeID,const PID& pid) : stID(storeID),def(0),id(pid) {}
	PINRef(ushort storeID,const PID& pid,const PageAddr& ad) : stID(storeID),def(PR_ADDR),id(pid),addr(ad) {}
	byte				enc(byte *buf) const;
	RC					dec(const byte *p);
	static	byte		len(const byte *p) {return *p&0x3F;}
	static	bool		isDelete(const byte *p) {return (*p&0x40)!=0;}
	static	bool		isMoved(const byte *p) {byte l=*p; return (l&0x80)!=0 && (p[l-1&0x7F]&0x04)!=0;}
	static	bool		isColl(const byte *p) {byte l=*p; return (l&0x80)!=0 && (p[l-1&0x7F]&0x20)!=0;}
	static	bool		isMulti(const byte *p) {byte l=*p; return (l&0x80)!=0 && (p[l-1&0x7F]&0x80)!=0;}
	static	bool		isSpecial(const byte *p) {byte l=*p; return (l&~0x40)>0x82 && (p[l=l-1&0x7F]&0x40)!=0 && (p[l-1]&0x08)!=0;}
	static	bool		isHidden(const byte *p) {byte l=*p; return (l&~0x40)>0x82 && (p[l=l-1&0x7F]&0x40)!=0 && (p[l-1]&0x04)!=0;}
	static	byte		changeFColl(byte *p,bool fSet) {byte l=*p; if ((l&0x80)==0) {if (fSet) p[l++]=0x20,*p=l|0x80;} else {l&=0x7F; if (fSet) p[l-1]|=0x20; else if ((p[l-1]&=~0x20)==0) *p=--l;} return l;}
	static	void		setDelete(byte *p) {*p|=0x40;}
	static	bool		hasCount(const byte *p) {byte l=*p; return (l&0x80)!=0 && (p[l-1&0x7F]&0x10)!=0;}
	static	uint32_t	getCount(const byte *p) {uint32_t c=1; byte l=*p; if ((l&0x80)!=0 && (p[l=l-1&0x7F]&0x10)!=0) {p+=l; if ((*p&0x40)!=0) --p; afy_dec32r(p,c);} return c;}
	static	bool		getPrefix(const byte *p,uint64_t& pref) {byte l=*p++; pref=0; if ((l&0x80)==0||((l=p[l-1&0x7F])&3)==0) return false; if ((l&0x02)!=0) {afy_dec64(p,pref);} else {afy_dec32(p,*(uint32_t*)&pref);} return true;}
	static	RC			getPID(const byte *p,ushort stID,PID& id,PageAddr *paddr=NULL);
	static	RC			adjustCount(byte *p,uint32_t cnt,byte *buf,bool fDec=false);
	static	int			cmpPIDs(const byte *p1,const byte *p2);
};

/**
 * compressed reference representaion
 */
struct EncPINRef
{
	mutable uint16_t	flags;
	byte				buf[XPINREFSIZE];
	template<int align> byte trunc() const {return (byte)ceil(int(sizeof(EncPINRef)-XPINREFSIZE+PINRef::len(buf)),align);}
	int					cmp(const EncPINRef& rhs) const {return PINRef::cmpPIDs(buf,rhs.buf);}
};

__forceinline int cmpPIDs(const PID& x,const PID& y) {int c=cmp3(x.pid&0xFFFFFFFFFFFFULL,y.pid&0xFFFFFFFFFFFFULL); if (c==0) c=cmp3((uint16_t)(x.pid>>48),(uint16_t)(y.pid>>48)); return c==0?cmp3(x.ident,y.ident):c;}
extern int __cdecl	cmpPIDs(const void *p1,const void *p2);

};

#endif
