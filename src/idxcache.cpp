/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "idxcache.h"
#include "session.h"
#include "pgtree.h"
#include "fio.h"

using namespace MVStoreKernel;

namespace MVStoreKernel
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

PIDStore::PIDStore(Session *ses,ulong lim)
: SubAlloc(ses),limit(lim),cache(*this),count(0),extFile(NULL)
{
}

PIDStore::~PIDStore()
{
	delete (ExtFile*)extFile;
}

bool PIDStore::operator[](PINEx& cb) const
{
	if (extFile!=NULL) {
		// check ext memory
	}
	PID id; return cb.getID(id)==RC_OK && cache[id];
}

RC PIDStore::operator+=(PINEx& cb)
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
	if (rc==RC_OK && (rc=cache.add(id))==RC_OK) count++;
	return rc;
}

RC PIDStore::operator-=(PINEx& cb)
{
	//...
	return RC_INTERNAL;
}

void PIDStore::clear()
{
	delete (ExtFile*)extFile; extFile=NULL; cache.clear(); count=0;
}
