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

#include "ecc.h"

using namespace AfyKernel;

RC	MPNCtx::addsub(const MPN& lhs,const MPN& rhs,MPN *&res,uint16_t fSub)
{
	assert(fSub<=1);
	if ((lhs.fNeg^rhs.fNeg^fSub)!=0) {
		//...
	} else if (lhs.nBits>=rhs.nBits) {
		//...
	} else {
		//...
	}
	return RC_OK;
}

RC	MPNCtx::_addsub(MPN& lhs,const MPN& rhs,bool fSub)
{
	assert(lhs.nBits>=rhs.nBits);
	if (fSub) {
		//...
	} else {
		//...
	}
	return RC_OK;
}

