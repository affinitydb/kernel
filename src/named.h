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

Written by Mark Venguerov 2013

**************************************************************************************/

/**
 * Named PINs: all meta-PINs and globally named PINs (anchors)
 */
#ifndef _NAMED_H_
#define _NAMED_H_

#include "idxtree.h"
#include "pinex.h"
#include "qmgr.h"

using namespace Afy;

namespace AfyKernel
{

class NamedMgr : public TreeGlobalRoot
{
	friend	class Classifier;
	StoreCtx	*const	ctx;
	const char	*storePrefix;
public:
	NamedMgr(StoreCtx *ct);
	RC				initStorePrefix();
	const	char	*getStorePrefix() const {return storePrefix;}
	bool			exists(URIID);
	RC				getNamed(URIID id,PINx& ,bool fUpdate=false);
	RC				update(URIID id,PINRef& pr,uint16_t meta,bool fInsert);
};

}

#endif
