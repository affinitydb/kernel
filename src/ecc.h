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

Written by Mark Venguerov 2013

**************************************************************************************/

/**
 * Elliptic curve cryptography
 */

#ifndef _ECC_H_
#define _ECC_H_

#include "utils.h"

namespace AfyKernel
{

struct MPN
{
	union {
		struct {
			uint16_t	fNeg	:1;
			uint16_t	fNegY	:1;
			uint16_t	nBits	:14;
			uint16_t	bits;
		};
		uint32_t		words[1];
	};
};

struct ECParams
{
	MPN		*A;
	MPN		*B;
	//...
};

class MPNCtx : public StackAlloc
{
public:
	MPNCtx(MemAlloc *s) : StackAlloc(s) {}
	RC	addsub(const MPN& lhs,const MPN& rhs,MPN *&res,uint16_t fSub=0);
	RC	_addsub(MPN& lhs,const MPN& rhs,bool fSub=false);
};

}

#endif
