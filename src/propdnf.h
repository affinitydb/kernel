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

Written by Mark Venguerov 2004-2012

**************************************************************************************/

/**
 * property lists in DNF form
 */
#ifndef _PROPDNF_H_
#define _PROPDNF_H_

#include "affinity.h"

using namespace Afy;

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
	static	RC		andP(PropDNF *&dnf,size_t& ldnf,const PropertyID *pids,unsigned np,MemAlloc *ma,bool fNot=false);
	static	RC		orP(PropDNF *&dnf,size_t& ldnf,const PropertyID *pids,unsigned np,MemAlloc *ma,bool fNot=false);
	static	void	normalize(PropDNF *&dnf,size_t& ldnf,MemAlloc *ma);
};

};

#endif
