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

#include "idxcache.h"
#include "session.h"
#include "pgtree.h"
#include "fio.h"

using namespace AfyKernel;

namespace AfyKernel
{

struct ExtFile
{
	StoreCtx	*const	ctx;
	FileID				fid;
	PageID				root;
	ExtFile(StoreCtx *ct) : ctx(ct),fid(INVALID_FILEID),root(INVALID_PAGEID) {}
	~ExtFile() {if (fid!=INVALID_FILEID) ctx->fileMgr->close(fid);}
	RC		init(SList<PID,PIDCmp>& sl) {
		assert(fid==INVALID_FILEID && root==INVALID_PAGEID);
		RC rc=ctx->fileMgr->open(fid,NULL,FIO_TEMP); 
		if (rc==RC_OK) {
			const PID *pd; sl.start();
			while ((pd=sl.next())!=NULL) {
				// copy from sl
			}
			if (rc==RC_OK) sl.clear();
		}
		return rc;
	}
	void	operator	delete(void *p) {free(p,SES_HEAP);}
};

}

PIDStore::PIDStore(Session *ses,unsigned lim)
: SubAlloc(ses),limit(lim),cache(*this),count(0),extFile(NULL)
{
}

PIDStore::~PIDStore()
{
	delete (ExtFile*)extFile;
}

bool PIDStore::operator[](PINx& cb) const
{
	if (extFile!=NULL) {
		// check ext memory
	}
	PID id; return cb.getID(id)==RC_OK && cache[id];
}

RC PIDStore::operator+=(PINx& cb)
{
	if (count+1>=limit) {
//		assert(extFile==NULL);
//		if ((extFile=new(SES_HEAP) ExtFile(ctx))==NULL) return RC_NORESOURCES;
//		RC rc=((ExtFile*)extFile)->init(cache); 
//		if (rc!=RC_OK) {delete (ExtFile*)extFile; extFile=NULL; return rc;}
	}
	if (extFile!=NULL) {
		// add to ext mem
	}
	PID id; RC rc=cb.getID(id); 
	if (rc==RC_OK && cache.add(id)!=SLO_ERROR) count++;
	return rc;
}

RC PIDStore::operator-=(PINx& cb)
{
	//...
	return RC_INTERNAL;
}

void PIDStore::clear()
{
	delete (ExtFile*)extFile; extFile=NULL; cache.clear(); count=0;
}
