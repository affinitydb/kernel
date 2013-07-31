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

#include "fsmgr.h"
#include "fio.h"
#include "buffer.h"
#include "txmgr.h"
#include "logmgr.h"
#include "startup.h"
#include "session.h"

using namespace AfyKernel;

FSMgr::FSMgr(StoreCtx *ct) : extentMapPage(ct),extentDirPage(ct),freeSpacePage(ct),ctx(ct)
{
	ctx->registerPageMgr(PGID_EXTENT,&extentMapPage);
	ctx->registerPageMgr(PGID_EXTDIR,&extentDirPage);
	ctx->registerPageMgr(PGID_FSPACE,&freeSpacePage);

	extentTable = NULL;
	lExtentTable = 0;
	nExtents = 0;
	slotsLeft = 0;
	dataFile = INVALID_FILEID;
}

FSMgr::~FSMgr()
{
//	if (extentTable!=NULL) {
//		for (int i=nExtents; --i>=0;) delete extentTable[i];
//		free(extentTable,STORE_HEAP);
//	}
}

void *FSMgr::operator new(size_t s,StoreCtx *ctx)
{
	void *p=ctx->malloc(s); if (p==NULL) throw RC_NORESOURCES; return p;
}

RC FSMgr::init(FileID fid)
{
	lock.lock(RW_X_LOCK); dataFile=fid;
	assert(ctx->theCB!=NULL && ctx->bufMgr!=NULL);
	if (ctx->theCB->nDirPages==0 || ctx->theCB->nDirPages>MAXDIRPAGES) {
		report(MSG_ERROR,"Invalid number of extent dir pages: %d\n",ctx->theCB->nDirPages);
		lock.unlock(); return RC_CORRUPTED;
	}
	size_t lPage=ctx->bufMgr->getPageSize(); PBlock *pb=NULL;
	lExtentTable = unsigned(ctx->theCB->nDirPages*lPage/sizeof(ExtentDirPage::DirSlot));
	if (lExtentTable<20) lExtentTable = 20;
	extentTable = (ExtentInfo**)ctx->malloc(lExtentTable*sizeof(ExtentInfo*));
	if (extentTable==NULL) {lock.unlock(); return RC_NORESOURCES;}
	if (ctx->theCB->nDirPages>2) ctx->bufMgr->prefetch(ctx->theCB->dirPages,ctx->theCB->nDirPages,&extentDirPage);
	for (unsigned i=0; i<ctx->theCB->nDirPages; i++) {
		pb=ctx->bufMgr->getPage((PageID)ctx->theCB->dirPages[i],&extentDirPage,0,pb);
		if (pb==NULL) report(MSG_ERROR,"Cannot read free page dir page %d(%08X)\n",i,ctx->theCB->dirPages[i]);
		else {
			PageHeader *dp = (PageHeader*)pb->getPageBuf();
			for (unsigned i=0; i<dp->nPages; i++) {
				ExtentDirPage::DirSlot *ds = &((ExtentDirPage::DirSlot*)(dp+1))[i];
				if (nExtents>=lExtentTable) {
					lExtentTable += lExtentTable/4;
					extentTable = (ExtentInfo**)ctx->realloc(extentTable,lExtentTable*sizeof(ExtentInfo*));
					if (extentTable==NULL) {pb->release(); lock.unlock(); return RC_NORESOURCES;}
				}
				ExtentInfo *ext = extentTable[nExtents++] = new(ctx) ExtentInfo;
				if (ext==NULL) {pb->release(); lock.unlock(); return RC_NORESOURCES;}
				ext->extentStart = ds->extentStart;
				ext->nPages = ds->nPages&~FREEPAGEFLAG;
				ext->dirPage = pb->getPageID();
				ext->pos = i;
				if ((ds->nPages&FREEPAGEFLAG)!=0) {
					extentList.insertLast(&ext->list);
					ext->nFreePages = ~0u;
					ext->maxContiguous = ~0u;
				}
			}
			slotsLeft = ExtentDirPage::maxDirSlots(lPage) - dp->nPages;
		}
	}
	if (pb!=NULL) pb->release();
	lock.unlock();
	return RC_OK;
}

RC FSMgr::create(FileID fid)
{
	assert(ctx->theCB!=NULL && ctx->bufMgr!=NULL);
	lock.lock(RW_X_LOCK);
	if (extentTable!=NULL) {lock.unlock(); return RC_OK;}
	dataFile = fid;
	ctx->theCB->nDirPages=0;
	lExtentTable = 20;
	extentTable = (ExtentInfo**)ctx->malloc(lExtentTable*sizeof(ExtentInfo*));
	lock.unlock();
	if (extentTable==NULL) return RC_NORESOURCES;
	ExtentInfo *ext=NULL; PBlock *pb=NULL;  
	RC rc = allocNewExtent(ext,pb);
	if (rc==RC_OK) pb->release();
	return rc;
}

void FSMgr::close()
{
	PageID dirPage=INVALID_PAGEID; 
	PBlock *dir=NULL; ExtentDirPage::DirSlot *ds=NULL; ExtentInfo *ext=NULL;
	lock.lock(RW_X_LOCK);
	for (unsigned i=0; i<nExtents; i++) if ((ext=extentTable[i])!=NULL && (ext->state&EXTMAP_MODIFIED)!=0) {
		if (ext->dirPage!=dirPage) {
			dirPage = ext->dirPage;
			dir = ctx->bufMgr->getPage(dirPage,&extentDirPage,PGCTL_XLOCK|QMGR_UFORCE,dir);
			ds = dir==NULL ? NULL : (ExtentDirPage::DirSlot*)(((PageHeader*)dir->getPageBuf())+1);
		}
		if (ds!=NULL) {
			if (ext->pos>=((PageHeader*)dir->getPageBuf())->nPages) {
				report(MSG_ERROR,"FSMgr::close: invalid pos: %d for extent %08X\n",ext->pos,ext->extentStart);
			} else {
				ds[ext->pos].extentStart = ext->extentStart;
				ds[ext->pos].nPages = ext->nPages|(ext->nFreePages!=0?FREEPAGEFLAG:0);
			}
		}
	}
	if (dir!=NULL) dir->release(QMGR_UFORCE);
	lock.unlock();
}

FSMgr::ExtentInfo *FSMgr::findExtent(PageID pid,bool fReserve)
{
	RWLockP lck(&lock,RW_S_LOCK);
	if (extentTable==NULL || nExtents==0) return NULL;
	ExtentInfo **tab = extentTable;
	for (unsigned n=nExtents; n>0; ) {
		unsigned k=n>>1; ExtentInfo *qq=tab[k];
		if (qq->extentStart>pid) n=k;
		else if (qq->extentStart+qq->nPages>=pid) return qq;
		else {tab+=k+1; n-=k+1;}
	}
	if (!fReserve) report(MSG_ERROR,"FSMsg::findExtent: invalid pid %08X\n",pid);
	return NULL;
}

PBlock *FSMgr::getExtentMapPage(ExtentInfo *ext,PBlock *pb)
{
	pb = ctx->bufMgr->getPage(ext->extentStart,&extentMapPage,PGCTL_XLOCK|QMGR_UFORCE,pb);
	if (pb!=NULL && (ext->state&EXTMAP_READ)==0) {
		TxPageHeader *emp = (TxPageHeader*)pb->getPageBuf();
		ext->nPages = emp->nPages;
		ext->nFreePages = 0;
		uint32_t *pBmp=extentMapPage.getBMP(emp); long n=(long)(emp->nPages+BITSPERELT-1)/BITSPERELT-1;
		uint32_t w=pBmp[n],mask=(1<<ext->nPages%BITSPERELT)-1;
		if ((w&mask)!=mask) {ext->nFreePages+=pop(~w&mask); ext->firstFree=n;}
		while (--n>=0) {
			if ((w=~pBmp[n])!=0) {ext->nFreePages+=pop(w); ext->firstFree=n;}
			// maxContiguous ???
		}
		ext->state|=EXTMAP_READ;
	}
	return pb;
}

PBlock *FSMgr::getNewPage(PageMgr *mgr)
{
	PageID pid; PBlock *allocPage=NULL;
	return allocPages(1,&pid,&allocPage)==RC_OK && pid!=INVALID_PAGEID ? ctx->bufMgr->newPage(pid,mgr,allocPage) : NULL;
}

RC FSMgr::allocPages(unsigned nPages,PageID *buf,PBlock **pAllocPage)
{
	Session *ses=Session::getSession(); unsigned nAlloc=0,ndw=0;
	if (!ses->inWriteTx() && pAllocPage==NULL) return RC_READTX;
	if (ses!=NULL && (unsigned)ses->tx.defFree!=0) while ((buf[nAlloc]=ses->tx.defFree.pop())!=INVALID_PAGEID)
		if (++nAlloc==nPages) {if (pAllocPage!=NULL) *pAllocPage=NULL; return RC_OK;}
	uint32_t *rec=(uint32_t*)alloca(nPages*2*sizeof(uint32_t)); if (rec==NULL) return RC_NORESOURCES;
	RWLockP lck(&txLock,RW_S_LOCK);
	for (PBlock *pb=NULL;;) {
		lock.lock(RW_S_LOCK); ExtentInfo *ext=extentList.getFirst(); lock.unlock();
		if (ext==NULL) {
			RC rc=allocNewExtent(ext,pb); if (rc!=RC_OK) return rc;
		} else if ((pb=getExtentMapPage(ext,pb))==NULL) {
			lock.lock(RW_X_LOCK); ext->list.remove(); ext->state|=EXTMAP_BAD; lock.unlock(); continue;
		} else if (ext->nFreePages==0) {
			lock.lock(RW_X_LOCK); ext->list.remove(); lock.unlock(); continue;
		}
		uint32_t nExtAlloc=0,sht=ext->firstFree*BITSPERELT;
		uint32_t *pBmp=extentMapPage.getBMP((TxPageHeader*)pb->getPageBuf());
		for (unsigned limit=(ext->nPages+BITSPERELT-1)/BITSPERELT; ext->firstFree<limit; ++ext->firstFree,sht+=BITSPERELT) {
			uint32_t bmp=pBmp[ext->firstFree],mask=ext->firstFree+1==limit?(1<<ext->nPages%BITSPERELT)-1:~0u;
			while ((bmp&mask)!=mask) {
				unsigned bitN=pop(bmp&~(bmp+1)); pBmp[ext->firstFree]=bmp|=1<<bitN;
				if (ndw>0 && rec[ndw-2]==ext->firstFree) rec[ndw-1]|=1<<bitN;
				else {rec[ndw++]=ext->firstFree; rec[ndw++]=1<<bitN;}
				bitN+=sht; buf[nAlloc]=bitN+ext->extentStart+1; ++nExtAlloc;
				if (++nAlloc==nPages || nExtAlloc>=ext->nFreePages) {
					RC rc = nExtAlloc==1 ? ctx->txMgr->update(pb,&extentMapPage,bitN) : 
										ctx->txMgr->update(pb,&extentMapPage,0,(byte*)rec,ndw*sizeof(uint32_t));
					assert(ext->nFreePages>=nExtAlloc && ext->list.isInList());
					ext->nFreePages-=nExtAlloc; ext->state|=EXTMAP_MODIFIED; ndw=0; if (nAlloc!=nPages) break;
					if (ext->nFreePages==0) {lock.lock(RW_X_LOCK); ext->list.remove(); lock.unlock();}
					if (pAllocPage!=NULL) *pAllocPage=pb; else pb->release(); return rc;
				}
			}
		}
		lock.lock(RW_X_LOCK); ext->list.remove(); lock.unlock();
	}
}

RC FSMgr::allocNewExtent(ExtentInfo*& ext,PBlock*& pb,bool fForce)
{
	unsigned nNewPages=ctx->theCB->nPagesPerExtent;
	size_t lPage=ctx->fileMgr->getPageSize(),pageSpace=ExtentMapPage::contentSize(lPage);
	ext=nExtents>0?extentTable[nExtents-1]:NULL;
	pb=ext!=NULL?getExtentMapPage(ext,pb):NULL;
	RWLockP lck(&lock,RW_X_LOCK); ExtentInfo *ext2;
	if (!fForce) while ((ext2=extentList.getFirst())!=NULL) {
		lck.set(NULL); pb=getExtentMapPage(ext2,pb); 
		if (pb!=NULL && ext2->nFreePages!=0) {ext=ext2; return RC_OK;}
		pb=getExtentMapPage(ext=extentTable[nExtents-1],pb);
		lck.set(&lock,RW_X_LOCK);
	}
	off64_t addr=ctx->fileMgr->getFileSize(dataFile);
	if (ctx->theCB->maxSize!=0) {
		uint64_t fullSize=addr;
		if (ctx->theCB->nDataFiles>1) fullSize+=uint64_t(ctx->theCB->nDataFiles-1)*MAXPAGESPERFILE*lPage;
		if (fullSize>=ctx->theCB->maxSize || fullSize+uint64_t(nNewPages)*lPage>ctx->theCB->maxSize &&
											(nNewPages=unsigned((ctx->theCB->maxSize-fullSize)/lPage))==0) 
			{if (pb!=NULL) pb->release(); return RC_QUOTA;}
	}
	RC rc; bool fNewFile=false;
	if (addr/lPage + nNewPages>MAXPAGESPERFILE) {
		FileID fid=dataFile+1; char buf[100];
		sprintf(buf,STOREPREFIX"%u"DATAFILESUFFIX,ctx->theCB->nDataFiles);
		if ((rc=ctx->fileMgr->open(fid,buf,FIO_CREATE|FIO_NEW))!=RC_OK) {
			report(MSG_ERROR,"FSMgr::allocNewExtent cannot create new file %s: %d\n",buf,rc);
			return rc;
		}
		dataFile=fid; ctx->theCB->nDataFiles++; fNewFile=true;
	}
	while ((rc=ctx->fileMgr->allocateExtent(dataFile,nNewPages,addr))!=RC_OK) {
		if (rc!=RC_FULL || (nNewPages>>=1)<=2) {
			report(MSG_ERROR,"FSMgr::allocNewExtent failed: %d\n",rc);
			return rc;
		}
	}
	assert(addr%lPage==0);
	PBlockP dir; bool fNewDirPage=false,fNewExtent=false;
	if (!fNewFile && nExtents>0 && pb!=NULL && ext->nPages+nNewPages<=pageSpace*8) {
		assert(ext==extentTable[nExtents-1] && pb->getPageID()==ext->extentStart);
		if (ext->nFreePages==0) ext->firstFree=ext->nPages/BITSPERELT;
		ext->maxContiguous = nNewPages;
		ext->nFreePages += nNewPages;
		ext->nPages += nNewPages;
		ext->state|=EXTMAP_MODIFIED;
		ext->list.remove();
	} else {
		if (slotsLeft==0) {
			if (ctx->theCB->nDirPages>=MAXDIRPAGES) {
				report(MSG_CRIT,"FSMgr: Too many directory pages\n"); return RC_NORESOURCES;
			}
			// can be in master file?
			dir.newPage(PageIDFromPageNum(dataFile,unsigned(addr/lPage)),&extentDirPage);
			addr+=lPage; nNewPages--; slotsLeft=ExtentDirPage::maxDirSlots(lPage); fNewDirPage=true;
		}
		PageID pid=PageIDFromPageNum(dataFile,unsigned(addr/lPage));
		if ((pb=ctx->bufMgr->newPage(pid,&extentMapPage,pb))==NULL) {
			report(MSG_CRIT,"FSMgr: Cannot allocate directory page\n"); return RC_NORESOURCES;
		}
		if (nExtents>=lExtentTable) {
			lExtentTable += lExtentTable/4;
			extentTable=(ExtentInfo**)ctx->realloc(extentTable,lExtentTable*sizeof(ExtentInfo*));
			if (extentTable==NULL) {pb->release(); pb=NULL; return RC_NORESOURCES;}
		}
		ext=extentTable[nExtents++]=new(ctx) ExtentInfo; fNewExtent=true;
		if (ext==NULL) {pb->release(); pb=NULL; return RC_NORESOURCES;}
		ext->extentStart=pid;
		ext->nPages=nNewPages - 1;
		ext->nFreePages=nNewPages - 1;
		ext->maxContiguous=nNewPages - 1;
		ext->firstFree=0;
		ext->state=EXTMAP_READ;
		ext->dirPage=!dir.isNull()?dir->getPageID():ctx->theCB->dirPages[ctx->theCB->nDirPages-1];
		assert(slotsLeft>0);
		ext->pos=ExtentDirPage::maxDirSlots(lPage) - slotsLeft--;
	}
	extentList.insertFirst(&ext->list);
	if (!fNewExtent) {pb=getExtentMapPage(ext,pb); assert(pb!=NULL);}
	TxPageHeader *emp=(TxPageHeader*)pb->getPageBuf();
	emp->nPages=ext->nPages; lck.set(NULL); pb->flushPage();
	if (dir.isNull()) {
		dir=ctx->bufMgr->getPage(ext->dirPage,&extentDirPage,PGCTL_XLOCK); dir.set(QMGR_UFORCE);
		if (dir.isNull()) {
			// panic!!!
			return RC_CORRUPTED;
		}
	}
	PageHeader *edh=(PageHeader*)dir->getPageBuf();
	ExtentDirPage::DirSlot *ds=(ExtentDirPage::DirSlot*)(edh+1);
	ds[ext->pos].extentStart=ext->extentStart;
	ds[ext->pos].nPages=ext->nPages|FREEPAGEFLAG;
	if ((ext->state&EXTMAP_MODIFIED)==0) edh->nPages++;
	for (int k=(int)nExtents-1; --k>=0;) {
		ExtentInfo *ei=extentTable[k];
		if (ei!=NULL) {
			if (ei->dirPage!=dir->getPageID()) break;
			if ((ei->state&EXTMAP_MODIFIED)!=0) {
				ds[ei->pos].extentStart=ei->extentStart;
				ds[ei->pos].nPages=ei->nPages|(ei->nFreePages!=0?FREEPAGEFLAG:0);
				ei->state&=~EXTMAP_MODIFIED;
			}
		}
	}
	dir->flushPage();
	if (rc==RC_OK && fNewDirPage) {
		ctx->theCB->dirPages[ctx->theCB->nDirPages++]=dir->getPageID();
		rc=ctx->theCB->update(ctx);
	}
	return rc;
}

RC FSMgr::freePage(PageID pid)
{
	Session *ses=Session::getSession();
	if (ses!=NULL && ses->getTxState()==TX_ACTIVE) {
		// check max size, if > -> return RC_NORESOURCES;
		assert(!ses->tx.defFree[pid]); return ses->tx.defFree+=pid;
	}
	if (ctx->bufMgr->exists(pid)) ctx->bufMgr->drop(pid);	// in case somebody ressurected it
	ExtentInfo *ext=findExtent(pid); PBlock *pb;
	if (ext==NULL || (pb=getExtentMapPage(ext,NULL))==NULL) return RC_CORRUPTED;
	uint32_t bitN=pid-ext->extentStart-1; RC rc;
	if ((rc=ctx->txMgr->update(pb,&extentMapPage,bitN|RESETBIT))!=RC_OK) return rc;
	++ext->nFreePages; ext->state|=EXTMAP_MODIFIED;
	if (!ext->list.isInList()) {
		lock.lock(RW_X_LOCK); if (!ext->list.isInList()) extentList.insertLast(&ext->list); lock.unlock();
	}
	if (bitN/BITSPERELT<ext->firstFree) ext->firstFree=bitN/BITSPERELT;
	pb->release(); ctx->logMgr->insert(NULL,LR_FLUSH,PGID_INDEX,pid); 
	return RC_OK;
}

RC FSMgr::freeTxPages(const PageSet& ps)
{
	ExtentInfo *ext=NULL; PBlock *pb=NULL; PageID pid; RC rc=RC_OK; txLock.lock(RW_X_LOCK);
	for (PageSet::it it(ps); (pid=++it)!=INVALID_PAGEID; ) {
		if (ext==NULL||pid<ext->extentStart||pid>ext->extentStart+ext->nPages)
			{if ((ext=findExtent(pid))==NULL || (pb=getExtentMapPage(ext,pb))==NULL) {rc=RC_CORRUPTED; txLock.unlock(); break;}}
		// more then one in update???
		uint32_t bitN=pid-ext->extentStart-1; 
		if ((rc=ctx->txMgr->update(pb,&extentMapPage,bitN|RESETBIT))!=RC_OK) {txLock.unlock(); break;}
		ctx->logMgr->insert(NULL,LR_FLUSH,PGID_INDEX,pid); 
		++ext->nFreePages; ext->state|=EXTMAP_MODIFIED; if (bitN/BITSPERELT<ext->firstFree) ext->firstFree=bitN/BITSPERELT;
		if (!ext->list.isInList()) {lock.lock(RW_X_LOCK); if (!ext->list.isInList()) extentList.insertLast(&ext->list); lock.unlock();}
	}
	if (pb!=NULL) pb->release();
	return rc;
}

bool FSMgr::isFreePage(PageID pid)
{
	ExtentInfo *ext=findExtent(pid,true); PBlock *pb; bool rc=true;
	if (ext!=NULL && (pb=getExtentMapPage(ext,NULL))!=NULL) {
		rc=extentMapPage.isFree((TxPageHeader*)pb->getPageBuf(),pid-ext->extentStart-1);
		pb->release();
	}
	return rc;
}

RC FSMgr::reservePage(PageID pid)
{
	if (Session::getSession()->inReadTx()) return RC_READTX;
	ExtentInfo *ext=findExtent(pid,true); PBlock *pb=NULL; RC rc=RC_OK;
	if (ext==NULL) for (;;) {
		rc=allocNewExtent(ext,pb,true); if (rc!=RC_OK) return rc;
		assert(ext!=NULL && pb!=NULL);
		if (pid==ext->extentStart) return RC_CORRUPTED;
		if (pid>ext->extentStart && pid<ext->extentStart+ext->nPages) break;
	} else if ((pb=getExtentMapPage(ext,NULL))==NULL) return RC_CORRUPTED;
	const TxPageHeader *ep=(const TxPageHeader*)pb->getPageBuf();
	if (extentMapPage.isFree(ep,pid-ext->extentStart-1)) {
		rc=ctx->txMgr->update(pb,&extentMapPage,pid-ext->extentStart-1); ext->state|=EXTMAP_MODIFIED;
		if (--ext->nFreePages==0) {lock.lock(RW_X_LOCK); ext->list.remove(); lock.unlock();}
	}
	pb->release();
	return rc;
}

PBlock *FSMgr::getNewPage(PageMgr *mgr,PageID pid)
{
	ExtentInfo *ext=findExtent(pid); PBlock *pb;
	if (ext==NULL || (pb=getExtentMapPage(ext,NULL))==NULL) return NULL;
	if (!extentMapPage.isFree((TxPageHeader*)pb->getPageBuf(),pid-ext->extentStart-1))
		{pb->release(); return NULL;}
	ctx->txMgr->update(pb,&extentMapPage,pid-ext->extentStart-1);	// if !=RC_OK ???
	return ctx->bufMgr->newPage(pid,mgr,pb);
}

//-------------------------------------------------------------------------------------------

ExtentMapPage::ExtentMapPage(StoreCtx *ctx) 
: TxPage(ctx),lExtHdr(EXT_HDR_SIZE)
{
}

RC ExtentMapPage::update(PBlock *pb,size_t len,unsigned info,const byte *rec,size_t lrec,unsigned flags,PBlock *)
{
	byte *frame=pb->getPageBuf(); TxPageHeader *emp=(TxPageHeader*)frame; 
	bool fReset=(info&RESETBIT)==((flags&TXMGR_UNDO)!=0?0:RESETBIT);
	FSMgr::ExtentInfo *ext=(flags&(TXMGR_UNDO|TXMGR_RECV))!=0?ctx->fsMgr->findExtent(emp->pageID+1):NULL;
	assert(ext==NULL||ext->extentStart==emp->pageID);
	if (rec==NULL || lrec==0) {
		unsigned bitN=info&~RESETBIT;
		if (bitN>MAXBITNUMBER || bitN>=emp->nPages) {report(MSG_ERROR,"FSMgr::update: invalid bit number %d\n",bitN); return RC_CORRUPTED;}
		uint32_t *pBmp=&getBMP(emp)[bitN/BITSPERELT],mask=1<<bitN%BITSPERELT;
		if (!fReset) {*pBmp|=mask; if (ext!=NULL) ext->nFreePages--;}
		else {*pBmp&=~mask; if (ext!=NULL) {ext->nFreePages++; if (bitN/BITSPERELT<ext->firstFree) ext->firstFree=bitN/BITSPERELT;}}
	} else {
		if ((lrec&(sizeof(uint32_t)*2-1))!=0) {	// check valid rec len
			// error msg
			return RC_CORRUPTED;
		}
		for (unsigned i=0; i*sizeof(uint32_t)<lrec; i+=2) {
			uint32_t idx=((uint32_t*)rec)[i],mask=((uint32_t*)rec)[i+1];
			if (idx*BITSPERELT>=MAXBITNUMBER) {
				// error msg
			} else {
				uint32_t *pBmp=&getBMP(emp)[idx];
				if (!fReset) {*pBmp|=mask; if (ext!=NULL) ext->nFreePages-=pop(mask);}
				else {*pBmp&=~mask; if (ext!=NULL) {ext->nFreePages+=pop(mask); if (idx<ext->firstFree) ext->firstFree=idx;}}
			}
		}
	}
	return RC_OK;
}

void ExtentMapPage::initPage(byte *frame,size_t len,PageID pid)
{
	memset(frame,0,len); TxPage::initPage(frame,len,pid);
}

bool ExtentMapPage::afterIO(PBlock *pb,size_t lPage,bool fLoad)
{
	if (!TxPage::afterIO(pb,lPage,fLoad)) return false;
	if (((TxPageHeader*)pb->getPageBuf())->nPages<=contentSize(lPage)*8) return true;
	report(MSG_ERROR,"Page %08X corrupt after read: incorrect nPages\n",pb->getPageID());
	return false;
}

bool ExtentMapPage::beforeFlush(byte *frame,size_t len,PageID pid)
{
	if (((TxPageHeader*)frame)->nPages>contentSize(len)*8) {
		report(MSG_ERROR,"Page %08X corrupt before write: incorrect nPages\n",pid);
		return false;
	}
	return TxPage::beforeFlush(frame,len,pid);
}

PGID ExtentMapPage::getPGID() const
{
	return PGID_EXTENT;
}

//--------------------------------------------------------------------------------------------------

bool ExtentDirPage::afterIO(PBlock *pb,size_t lPage,bool fLoad)
{
	if (!PageMgr::afterIO(pb,lPage,fLoad)) return false;
	if (((PageHeader*)pb->getPageBuf())->nPages>maxDirSlots(lPage)) {
		report(MSG_ERROR,"Page %08X corrupt after read: incorrect number of slots (%d)\n",pb->getPageID(),((PageHeader*)pb->getPageBuf())->nPages);
		return false;
	}
	return true;
}

bool ExtentDirPage::beforeFlush(byte *frame,size_t len,PageID pid)
{
	if (((PageHeader*)frame)->nPages>maxDirSlots(len)) {
		report(MSG_ERROR,"Page %08X corrupt before write: incorrect number of slots (%d)\n",pid,((PageHeader*)frame)->nPages);
		return false;
	}
	return PageMgr::beforeFlush(frame,len,pid);
}

PGID ExtentDirPage::getPGID() const
{
	return PGID_EXTDIR;
}

//-------------------------------------------------------------------------------------------------

RC FreeSpacePage::update(PBlock*,size_t len,unsigned info,const byte *rec,size_t lrec,unsigned flags,PBlock*)
{
	return RC_OK;
}

PGID FreeSpacePage::getPGID() const
{
	return PGID_FSPACE;
}
