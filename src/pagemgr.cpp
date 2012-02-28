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

#include "pagemgr.h"
#include "session.h"

using namespace AfyKernel;

void PageMgr::initPage(byte *,size_t,PageID)
{
}

bool PageMgr::afterIO(class PBlock *,size_t,bool)
{
	return true;
}

bool PageMgr::beforeFlush(byte *,size_t,PageID)
{
	return true;
}

LSN PageMgr::getLSN(const byte *,size_t) const
{
	return LSN(0);
}

void PageMgr::setLSN(LSN,byte *,size_t)
{
}

RC PageMgr::update(PBlock *,size_t,ulong,const byte *,size_t,ulong,PBlock*)
{
	return RC_INTERNAL;
}

PageID PageMgr::multiPage(ulong,const byte*,size_t,bool& fMerge)
{
	fMerge=false; return INVALID_PAGEID;
}

RC PageMgr::undo(ulong info,const byte *rec,size_t lrec,PageID)
{
	report(MSG_ERROR,"Invalid LUNDO\n"); return RC_INTERNAL;
}

void PageMgr::unchain(PBlock *,PBlock *)
{
}

PGID PageMgr::getPGID() const
{
	return PGID_ALL;
}

void *PageMgr::operator new(size_t s,StoreCtx *ctx)
{
	void *p=ctx->malloc(s); if (p==NULL) throw RC_NORESOURCES; return p;
}
