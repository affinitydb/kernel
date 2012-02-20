/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _PROPDNF_H_
#define _PROPDNF_H_

#include "affinity.h"

using namespace AfyDB;

namespace AfyKernel
{

struct PropDNF
{
	uint16_t		nExcl;
	uint16_t		nIncl;
	PropertyID		pids[1];
public:
	void			*operator new(size_t s,size_t l,MemAlloc *ma) throw() {return ma->malloc(l);}
	bool			isConjunctive(size_t l) const {return l==0 || sizeof(PropDNF)+int(nIncl+nExcl-1)*sizeof(PropertyID)==l;}
	static	RC		andP(PropDNF *&dnf,size_t& ldnf,PropDNF *rhs,size_t lrhs,MemAlloc *ma);
	static	RC		orP(PropDNF *&dnf,size_t& ldnf,PropDNF *rhs,size_t lrhs,MemAlloc *ma);
	static	RC		andP(PropDNF *&dnf,size_t& ldnf,const PropertyID *pids,ulong np,MemAlloc *ma,bool fNot=false);
	static	RC		orP(PropDNF *&dnf,size_t& ldnf,const PropertyID *pids,ulong np,MemAlloc *ma,bool fNot=false);
	static	void	normalize(PropDNF *&dnf,size_t& ldnf,MemAlloc *ma);
};

};

#endif
