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

#include "named.h"
#include "queryprc.h"
#include "maps.h"

using namespace AfyKernel;

static const IndexFormat classPINsFmt(KT_UINT,sizeof(uint64_t),KT_VARDATA);

NamedMgr::NamedMgr(StoreCtx *ct) : TreeGlobalRoot(MA_NAMEDPINS,classPINsFmt,ct,TF_WITHDEL),ctx(ct),storePrefix(NULL)
{
}

RC NamedMgr::initStorePrefix()
{
	//...
	return RC_OK;
}

bool NamedMgr::exists(URIID uid)
{
	SearchKey key((uint64_t)uid); size_t l; return find(key,NULL,l)==RC_OK;
}

RC NamedMgr::getNamed(URIID uid,PINx& cb,bool fUpdate)
{
	SearchKey key((uint64_t)uid); size_t l; byte buf[XPINREFSIZE]; RC rc;
	if ((rc=find(key,buf,l))!=RC_OK) return rc;
	PINRef pr(ctx->storeID,buf,l); cb=pr.id; if ((pr.def&PR_ADDR)!=0) cb=pr.addr;
	return ctx->queryMgr->getBody(cb,fUpdate?TVO_UPD:TVO_READ);
}

RC NamedMgr::update(URIID id,PINRef& pr,uint16_t meta,bool fInsert)
{
	SearchKey key((uint64_t)id); byte buf[XPINREFSIZE]; pr.u1=meta; pr.def|=PR_U1;
	return fInsert?insert(key,buf,pr.enc(buf)):TreeGlobalRoot::update(key,NULL,0,buf,pr.enc(buf));
}
