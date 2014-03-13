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

#include "pgtree.h"
#include "buffer.h"
#include "txmgr.h"
#include "fsmgr.h"
#include "logmgr.h"
#include "pinref.h"
#include "startup.h"
#include "pin.h"
#include "expr.h"

using namespace Afy;
using namespace AfyKernel;

namespace AfyKernel
{

class TreeRQ : public Request, public TreeCtx {
public:
	HChain<TreeRQ>			list;
private:
	StoreCtx				*ctx;
	const	PageID			child;
	const	Tree::TreeOp	op;
	uint64_t				:0;
	SearchKey				key;
	void *operator new(size_t s,size_t l,StoreCtx *ctx) throw() {return ctx->malloc(s+l);}
	TreeRQ(StoreCtx *ct,const TreeCtx& tctx,Tree::TreeOp o,PageID chld) : TreeCtx(tctx),list(this),ctx(ct),child(chld),op(o),key(uint64_t(~0ULL)) {}
public:
	virtual		~TreeRQ();
	PageID		getKey() const {return child;}
	void		process() {
		if (tcon!=NULL && (tree=tcon->connect(thndl))==NULL || tree!=NULL && (tree->mode&TF_NOPOST)!=0) return;
		if (op!=Tree::TO_DELETE) {ctx->trpgMgr->addNewPage(*this,key,child); return;}
		assert((tree->mode&TF_WITHDEL)!=0); PBlock *pbl; unsigned pos;
		if (getParentPage(key,PTX_FORUPDATE|PTX_FORDELETE)!=RC_OK) return;
		TreePageMgr::TreePage *tp=(TreePageMgr::TreePage*)pb->getPageBuf();
		if (tp->isLeaf()) {
			assert(!tp->info.fmt.isFixedLenData() || tp->info.fmt.isKeyOnly());
			// find SubTree based on key
			// removeRootPage
			return;
		}
		if (tp->info.level==1 && tp->info.stamp!=stamps[PITREE_1STLEV]) return;	//????
		if (tp->info.nSearchKeys==0 || !tp->findKey(key,pos) || tp->getPageID(pos)!=child) return;
		PageID lpid=tp->getPageID(pos-1); assert(lpid!=INVALID_PAGEID);
		if (pb->isULocked()) pb->upgradeLock();
		if ((pbl=ctx->bufMgr->getPage(lpid,ctx->trpgMgr,PGCTL_XLOCK))==NULL) return;
		const TreePageMgr::TreePage *ltp=(const TreePageMgr::TreePage*)pbl->getPageBuf();
		if (ltp->info.sibling==child) {
			PBlock *pbr=ctx->bufMgr->getPage(child,ctx->trpgMgr,PGCTL_XLOCK);
			if (pbr!=NULL) {
				RC rc=ctx->trpgMgr->merge(pbl,pbr,pb,*tree,key,pos);
				if (rc==RC_OK) {
					if (tp->info.nSearchKeys==0) {
						// post
						if (tp->info.leftMost!=INVALID_PAGEID) {
						} else {
						}
					}
				} else if (rc!=RC_FALSE)
					report(MSG_ERROR,"TreeRQ: cannot merge pages %08X %08X, parent %08X (%d)\n",lpid,child,pb->getPageID(),rc);
			}
		}
		pbl->release();
	}
	void		destroy() {StoreCtx *ct=ctx; this->~TreeRQ(); ct->free(this);}
	static	RC	post(const TreeCtx& tctx,Tree::TreeOp op,PageID pid,const SearchKey *key,PBlock *pb=NULL,ushort idx=0);
};

class TreeRQTable : public SyncHashTab<TreeRQ,PageID,&TreeRQ::list>
{
public:
	TreeRQTable(unsigned hashSize,MemAlloc *ma) : SyncHashTab<TreeRQ,PageID,&TreeRQ::list>(hashSize,ma) {}
};

};

TreeMgr::TreeMgr(StoreCtx *ct,unsigned timeout) : ctx(ct)
{
	if ((ptrt=new(ct) TreeRQTable(DEFAULT_TREERQ_SIZE,ct))==NULL) throw RC_NORESOURCES;
	memset(factoryTable,0,sizeof(factoryTable));
}

TreeMgr::~TreeMgr()
{
	if (traverse!=0 && (ctx->mode&STARTUP_PRINT_STATS)!=0)
		report(MSG_INFO,"\tIndex access stats: %d/%d/%g/%d\n",(long)sideLink,(long)pageRead,double(pageRead)/double(traverse),(long)sibRead);
	//delete ptrt;
}

void *TreeMgr::operator new(size_t s,StoreCtx *ctx)
{
	void *p=ctx->malloc(s); if (p==NULL) throw RC_NORESOURCES; return p;
}

RC TreeMgr::registerFactory(TreeFactory &factory)
{
	byte id=factory.getID();
	if (id>=MAXFACTORIES) return RC_INVPARAM;
	factoryTable[id]=&factory;
	return RC_OK;
}

TreeRQ::~TreeRQ()
{
	if (ctx->treeMgr->ptrt!=NULL) ctx->treeMgr->ptrt->remove(this);
	if (tree!=NULL) tree->destroy();
}

RC TreeRQ::post(const TreeCtx& tctx,Tree::TreeOp op,PageID pid,const SearchKey *key,PBlock *pb,ushort idx) {
	if ((tctx.tree->mode&TF_NOPOST)!=0 || (tctx.tree->ctx->mode&STARTUP_RT)!=0) return RC_OK;
	StoreCtx *ctx=tctx.tree->getStoreCtx(); TreeMgr *mgr=ctx->treeMgr;
	if (mgr->ptrt!=NULL) {
		TreeRQ *req=mgr->ptrt->findLock(pid);
		if (req==NULL) mgr->ptrt->unlock(pid);
		else {
			bool f=req->op==op;			// check stack
			mgr->ptrt->unlock(req);
			if (f) return RC_OK;
		}
	}
	if (tctx.depth==0) {
		//???
		return RC_FALSE;
	}
	size_t lext=tctx.mainKey->isSet()?sizeof(SearchKey)+tctx.mainKey->extra():0,shift=0;
	TreeRQ *req=NULL; //size_t linfo=tctx.tree->getTreeInfoLen();
	if (pb!=NULL) {
		const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
		shift=ceil(tp->getKeyExtra(idx),sizeof(uint64_t));
		if ((req=new(shift+lext,ctx) TreeRQ(ctx,tctx,op,pid))==NULL) return RC_NORESOURCES;
		tp->getKey(idx,req->key); assert(req->key.type!=KT_BIN||req->key.v.ptr.l!=ushort(~0u));
	} else {
		assert(key->isSet()); shift=ceil(key->extra(),sizeof(uint64_t));
		if ((req=new(shift+lext,ctx) TreeRQ(ctx,tctx,op,pid))==NULL) return RC_NORESOURCES;
		req->key=*key; assert(req->key.type!=KT_BIN||req->key.v.ptr.l!=ushort(~0u));
	}
	if (lext!=0) {req->mainKey=(SearchKey*)((byte*)(req+1)+shift); *(SearchKey*)req->mainKey=*tctx.mainKey;}
	if (mgr->ptrt!=NULL) mgr->ptrt->insert(req); RequestQueue::postRequest(req,ctx); return RC_OK;
}

PBlock *Tree::getPage(PageID pid,unsigned stamp,TREE_NODETYPE type)
{
	lock(RW_S_LOCK); PBlock *pb=getStamp(type)==stamp?ctx->bufMgr->getPage(pid,ctx->trpgMgr):(PBlock*)0; unlock(); return pb;
}

RC TreeCtx::getParentPage(const SearchKey &key,unsigned flags)
{
	assert(depth>0 && ((flags&PTX_FORDELETE)==0||(tree->mode&TF_WITHDEL)!=0));
	if (!parent.isNull()) {
		parent.moveTo(pb); --depth;
		if ((flags&PTX_FORUPDATE)!=0 && !pb->isXLocked() && 
			(!pb->isULocked() || !pb.isSet(QMGR_UFORCE) || (flags&PTX_TRY)!=0 && !pb->tryupgrade())) return RC_NOACCESS;
		assert(mainKey==NULL || !((TreePageMgr::TreePage*)pb->getPageBuf())->isLeaf() || vp.len!=0 && vp.offset!=0);
		return RC_OK;
	}
	PageID pid=stack[--depth]; if (pb.isSet(PGCTL_NOREL)) pb=NULL; unsigned xlock=(flags&PTX_TRY)!=0?PGCTL_XLOCK|QMGR_TRY:PGCTL_ULOCK|QMGR_UFORCE;
	if ((tree->mode&TF_WITHDEL)!=0) {
		if (!tree->lock((flags&PTX_FORDELETE)!=0?RW_X_LOCK:RW_S_LOCK,(flags&PTX_TRY)!=0)) {pb.release(); return RC_FALSE;}
		TREE_NODETYPE ptty=depth>leaf?PITREE_INT2:depth==leaf?PITREE_LEAF:PITREE_INT;
		if (tree->getStamp(ptty)!=stamps[ptty]) {tree->unlock(); pb.release(); return RC_FALSE;}
		xlock|=PGCTL_COUPLE; if ((flags&PTX_FORDELETE)!=0 && ptty!=PITREE_LEAF) tree->advanceStamp(ptty);
	}
	// check latched!!!
	StoreCtx *ctx=tree->getStoreCtx();
	pb.getPage(pid,ctx->trpgMgr,(flags&PTX_TRY)!=0?PGCTL_XLOCK|QMGR_TRY:PGCTL_ULOCK);
	if ((tree->mode&TF_WITHDEL)!=0) tree->unlock();
	while (!pb.isNull()) {
		const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
		bool fLeaf=mainKey!=NULL && tp->isLeaf(); assert(!fLeaf || tp->info.fmt.isKeyOnly() || !tp->info.fmt.isFixedLenData());
		if (tp->checkSibling(fLeaf?*mainKey:key)) pb.getPage(tp->info.sibling,ctx->trpgMgr,xlock);
		else {
			if (fLeaf) {
				ushort l; const PagePtr *pv=NULL;
				if (!tp->findKey(*mainKey,index) || tp->findData(index,l,&pv)==NULL) {pb.release(); break;}
				assert(pv!=NULL); vp=*pv;
			}
			if ((xlock&PGCTL_ULOCK)!=0) pb.set(QMGR_UFORCE); break;
		}
		
	}
	return pb.isNull()?RC_NOTFOUND:RC_OK;
}

RC TreeCtx::getPreviousPage(bool fRead)
{
	const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage *)pb->getPageBuf();
	if (tp->info.nSearchKeys==0) return RC_NOTFOUND;
	SearchKey *key=(SearchKey*)alloca(tp->getKeyExtra(0)+sizeof(SearchKey));
	if (key==NULL) return RC_NORESOURCES; tp->getKey(0,*key);
	PageID prev=pb->getPageID(),pid; StoreCtx *ctx=tree->getStoreCtx();
	while (depth!=0 && getParentPage(*key,fRead?0:PTX_FORUPDATE)==RC_OK) {
		tp=(const TreePageMgr::TreePage *)pb->getPageBuf(); if (tp->info.leftMost==prev) break;
		unsigned pos; tp->findKey(*key,pos); assert(tp->info.level>0); pb.moveTo(parent); depth++;
		while (pos!=~0u && ((pid=tp->getPageID(--pos))!=prev || pos!=~0u && (pid=tp->getPageID(--pos))!=INVALID_PAGEID)) {
			while (pb.getPage(pid,ctx->trpgMgr,(fRead?0:PGCTL_XLOCK)|((tree->mode&TF_WITHDEL)!=0?PGCTL_COUPLE:0))!=NULL) {
				const TreePageMgr::TreePage *tp2=(const TreePageMgr::TreePage *)pb->getPageBuf();
				if (tp2->info.level==0) {if (tp2->info.nSearchKeys>0) return RC_OK; else break;}
				stack[depth++]=pid; pid=tp2->getPageID(tp->info.nSearchKeys-1);
			}
			if (pb.isNull()) break;
		}
		parent.moveTo(pb); depth--;
	}
	pb.release(); parent.release();
	if (leaf==~0u) {depth=0; return findPrevPage(key,!fRead);}
	//???
	return RC_NOTFOUND;
}

RC TreeCtx::findPage(const SearchKey *key)
{
	StoreCtx *ctx=tree->getStoreCtx(); getStamps(stamps);
	int level=-1; PageID pid=startPage(key,level); parent.moveTo(pb);
	unsigned xlock=(tree->mode&TF_WITHDEL)!=0?PGCTL_COUPLE:0,kpos=~0u; ++ctx->treeMgr->traverse;
	if (pid!=INVALID_PAGEID) while (pb.getPage(pid,ctx->trpgMgr,xlock)!=NULL) {
		const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
		++ctx->treeMgr->pageRead; if (tp->info.level>TREE_MAX_DEPTH) break;
		if (tp->info.nSearchKeys==0 && depth>0 && key!=NULL)
			{assert((tree->mode&TF_WITHDEL)!=0); TreeRQ::post(*this,Tree::TO_DELETE,pid,key);}
		if (key!=NULL && tp->checkSibling(*key)) {
			if (tp->info.nSearchKeys>0 && depth>0) TreeRQ::post(*this,Tree::TO_INSERT,tp->info.sibling,NULL,pb,ushort(~0u));
			pid=tp->info.sibling; ++ctx->treeMgr->sideLink; assert(pid!=INVALID_PAGEID);
		} else if (tp->info.nSearchKeys==0 && tp->info.level>0 && tp->info.leftMost==INVALID_PAGEID) break;
		else if (tp->info.level==0) {stamps[PITREE_LEAF]=tree->getStamp(PITREE_LEAF); return RC_OK;}
		else {
			assert(!tp->isLeaf() && depth<TREE_MAX_DEPTH); stack[depth++]=pid;
			if (tp->info.level==1) stamps[PITREE_1STLEV]=tp->info.stamp;
			if ((pid=key!=NULL?tp->getChild(*key,kpos,false):tp->info.leftMost)==INVALID_PAGEID) break;
		}
	}
	pb.release(); return RC_NOTFOUND;
}

RC TreeCtx::findPageForUpdate(const SearchKey *key,bool fIns)
{
	StoreCtx *ctx=tree->getStoreCtx(); ++ctx->treeMgr->traverse; getStamps(stamps); assert(key!=NULL);
	int level=-1; PageID pid=startPage(key,level,false); unsigned xlock=level==0||level==1?PGCTL_ULOCK:0; unsigned kpos=~0u;
	while (pid!=INVALID_PAGEID && pb.getPage(pid,ctx->trpgMgr,xlock)!=NULL) {
		++ctx->treeMgr->pageRead; xlock&=~PGCTL_COUPLE;
		const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
		if (tp->info.level>TREE_MAX_DEPTH) break;
		if (tp->info.nSearchKeys==0 && depth>0) TreeRQ::post(*this,Tree::TO_DELETE,pid,key,parent,ushort(kpos));
		if (tp->checkSibling(*key)) {
			if (tp->info.nSearchKeys>0 && depth>0) {
				//if (parent==NULL || !parent->isULocked()) 
				TreeRQ::post(*this,Tree::TO_INSERT,tp->info.sibling,NULL,pb,ushort(~0u));
				//else {ctx->trpgMgr->insert(parent,*key,(const byte*)&tp->info.sibling,sizeof(PageID),*this);}
			}
			if ((xlock&PGCTL_ULOCK)!=0) xlock|=QMGR_UFORCE; else if (tp->info.level==0) xlock|=PGCTL_ULOCK;
			pid=tp->info.sibling; if ((tree->mode&TF_WITHDEL)!=0) xlock|=PGCTL_COUPLE; ++ctx->treeMgr->sideLink;
		} else if (!fIns && tp->info.nSearchKeys==0 && (tp->info.level==0||tp->info.leftMost==INVALID_PAGEID)) break;
		else if (tp->info.level!=0) {
			pb.moveTo(parent); if (tp->info.level<=2) xlock|=PGCTL_ULOCK;
			assert(!tp->isLeaf() && depth<TREE_MAX_DEPTH); stack[depth++]=pid;
			if (tp->info.level==1) stamps[PITREE_1STLEV]=tp->info.stamp; pid=tp->getChild(*key,kpos,false);
		} else if ((xlock&PGCTL_ULOCK)==0) {assert(depth==0||depth==leaf+1); xlock|=PGCTL_ULOCK;}
		else {
			if (!parent.isSet(QMGR_UFORCE)) parent.release(); stamps[PITREE_LEAF]=tree->getStamp(PITREE_LEAF); return RC_OK;
		}
	}
	parent.release(); pb.release(); return RC_NOTFOUND;
}

RC TreeCtx::findPrevPage(const SearchKey *key0,bool fUpd)
{
	unsigned kpos=~0u,lkey=0; const SearchKey *key=key0;
	StoreCtx *ctx=tree->getStoreCtx(); ++ctx->treeMgr->traverse; getStamps(stamps); PageID next=INVALID_PAGEID;
	int level=-1; PageID pid=startPage(key,level,!fUpd,true); unsigned xlock=fUpd && (level==0||level==1)?PGCTL_ULOCK:0;
	while (pid!=INVALID_PAGEID && pb.getPage(pid,ctx->trpgMgr,xlock)!=NULL) {
		++ctx->treeMgr->pageRead; xlock&=~PGCTL_COUPLE;
		const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
		if (tp->info.level>TREE_MAX_DEPTH) break; 
		bool fFollow=tp->hasSibling()&&(key==NULL||tp->hasSibling()&&next!=tp->info.sibling&&tp->testKey(*key,ushort(~0u))>0);
		if (!fFollow && (tp->info.nSearchKeys==0 || key!=NULL&&tp->testKey(*key,0)<=0)) {
			if (depth>0) {
				if (kpos==~0u) break; next=pid;
				if (!parent.isNull()) {
					tp=(const TreePageMgr::TreePage*)parent->getPageBuf();
					if ((pid=tp->getPageID(--kpos))!=INVALID_PAGEID) {
						ushort l=sizeof(SearchKey)+tp->getKeyExtra(ushort(kpos+1));
						if (lkey<l) {key=(SearchKey*)(key==key0?malloc(l,SES_HEAP):realloc((void*)key,l,SES_HEAP)); lkey=l;}
						tp->getKey(ushort(kpos+1),*(SearchKey*)key); if ((xlock&PGCTL_ULOCK)!=0) xlock|=QMGR_UFORCE; continue;
					}
					assert(kpos==~0u);
				}
				pb.release();
				// ??? set key
				//if (getParentPage(*key,fUpd?PTX_FORUPDATE:0,ctx)!=RC_OK) ???
				if (pb.isNull()) {parent.release(); pid=prevStartPage(pid); continue;}
			} else if (tp->info.level==0 || tp->info.leftMost==INVALID_PAGEID) {
				pid=prevStartPage(pid); if ((xlock&PGCTL_ULOCK)!=0) xlock|=QMGR_UFORCE; continue;
			}
		}
		if (tp->info.nSearchKeys==0 && depth>0 && (!parent.isNull()||key0!=NULL))
			TreeRQ::post(*this,Tree::TO_DELETE,pid,key0,parent,ushort(kpos));
		if (fFollow) {
			if (tp->info.nSearchKeys>0 && depth>0) {
				//if (parent==NULL || !parent->isULocked()) 
					TreeRQ::post(*this,Tree::TO_INSERT,tp->info.sibling,NULL,pb,ushort(~0u));
				//else {ctx->trpgMgr->insert(parent,*key,(const byte*)&tp->info.sibling,sizeof(PageID),*this);}
			}
			if ((xlock&PGCTL_ULOCK)!=0) xlock|=QMGR_UFORCE; else if (fUpd && tp->info.level==0) xlock|=PGCTL_ULOCK;
			pid=tp->info.sibling; if ((tree->mode&TF_WITHDEL)!=0) xlock|=PGCTL_COUPLE; ++ctx->treeMgr->sideLink; continue;
		}
		if (!fUpd && tp->info.nSearchKeys==0 && (tp->info.level==0||tp->info.leftMost==INVALID_PAGEID)) break;
		if (tp->info.level!=0) {
			pb.moveTo(parent); next=INVALID_PAGEID; assert(!tp->isLeaf() && depth<TREE_MAX_DEPTH);		// if !fUpd???
			if ((xlock&PGCTL_ULOCK)!=0) xlock|=QMGR_UFORCE; else if (fUpd && tp->info.level<=2) xlock|=PGCTL_ULOCK;
			stack[depth++]=pid; if (tp->info.level==1) stamps[PITREE_1STLEV]=tp->info.stamp;
			pid=key!=NULL?tp->getChild(*key,kpos,true):tp->getPageID(kpos=tp->info.nSearchKeys-1);
		} else if (fUpd && (xlock&PGCTL_ULOCK)==0) {assert(depth==0||depth==leaf+1); xlock|=PGCTL_ULOCK;}
		else {
			if (key!=key0) free((SearchKey*)key,SES_HEAP); stamps[PITREE_LEAF]=tree->getStamp(PITREE_LEAF);	// ?????
			if (fUpd && !parent.isSet(QMGR_UFORCE)) parent.release(); return RC_OK;
		}
	}
	if (key!=key0) free((SearchKey*)key,SES_HEAP);
	parent.release(); pb.release(); return RC_NOTFOUND;
}

RC TreeCtx::postPageOp(const SearchKey& key,PageID pageID,bool fDel) const
{
	return fDel?TreeRQ::post(*this,Tree::TO_DELETE,pageID,&key):TreeRQ::post(*this,Tree::TO_INSERT,pageID,&key);
}

unsigned Tree::checkTree(StoreCtx *ctx,PageID root,CheckTreeReport& res,CheckTreeReport *sec)
{
	memset(&res,0,sizeof(CheckTreeReport));
	if (sec!=NULL) memset(sec,0,sizeof(CheckTreeReport));
	return root!=INVALID_PAGEID?ctx->trpgMgr->checkTree(root,INVALID_PAGEID,0,res,sec):0;
}

RC Tree::find(const SearchKey& key,void *buf,size_t &size)
{
	RC rc; TreeCtx tctx(*this); ushort lData;
	if ((rc=tctx.findPage(&key))==RC_OK) {
		const void *p=((const TreePageMgr::TreePage*)tctx.pb->getPageBuf())->getValue(key,lData);
		if (p!=NULL) {if (size+lData>0 && buf!=NULL) memcpy(buf,p,min(ushort(size),lData)); size=lData;} else rc=RC_NOTFOUND;
	}
	return rc;
}

RC Tree::findByPrefix(const SearchKey& key,uint32_t prefix,byte *buf,byte& l)
{
	RC rc; TreeCtx tctx(*this);
	if ((rc=tctx.findPage(&key))==RC_OK) rc=ctx->trpgMgr->findByPrefix(tctx,key,prefix,buf,l);
	return rc;
}

RC Tree::countValues(const SearchKey& key,uint64_t& nValues)
{
	TreeCtx tctx(*this); RC rc; nValues=0;
	return (rc=tctx.findPage(&key))==RC_OK?ctx->trpgMgr->count(tctx,key,nValues):rc;
}

RC Tree::insert(const SearchKey& key,const void *value,ushort lval,unsigned multi,bool fUnique)
{
	TreeCtx tctx(*this); RC rc=tctx.findPageForUpdate(&key,true);
	return rc==RC_OK?ctx->trpgMgr->insert(tctx,key,value,lval,multi,fUnique):rc;
}

RC Tree::insert(const SearchKey& key,SubTreeInit& st,Session *ses)
{
	RC rc=RC_OK; if (st.buf==NULL||(rc=st.flush(ses))!=RC_OK) return rc;
	TreeCtx tctx(*this); if ((rc=tctx.findPageForUpdate(&key,true))!=RC_OK) return rc;
	return ctx->trpgMgr->insert(tctx,key,st.buf,st.lbuf-st.freeSpaceLength,(unsigned)st.nKeys|(st.depth!=0?0xFFFF0000:0));
}

RC Tree::insert(IMultiKey& mk)
{
	TreeCtx tctx(*this,&mk); RC rc; const SearchKey *pkey; const void *value; ushort lval; unsigned multi;
	while ((rc=mk.nextKey(pkey,value,lval,multi))==RC_OK) {
		const TreePageMgr::TreePage *tp; int cmp=-1;
		if (tctx.pb.isNull() || (cmp=(tp=(TreePageMgr::TreePage*)tctx.pb->getPageBuf())->cmpKey(*pkey))!=0) {
			if (cmp>0) {
				assert(tp->hasSibling()); ++ctx->treeMgr->sibRead;
				if (tctx.pb.getPage(tp->info.sibling,ctx->trpgMgr,PGCTL_COUPLE|PGCTL_ULOCK)==NULL ||
					((TreePageMgr::TreePage*)tctx.pb->getPageBuf())->cmpKey(*pkey)>0) cmp=-1;
			}
			if (cmp<0 && (tctx.depth=0,rc=tctx.findPageForUpdate(pkey,true))!=RC_OK) break;
		}
		if ((rc=ctx->trpgMgr->insert(tctx,*pkey,value,lval,multi))!=RC_OK) break;
	}
	return rc==RC_EOF?RC_OK:rc;
}

RC Tree::insert(const void *val,ushort lval,uint32_t& id,PageID& pid)
{
	TreeCtx tctx(*this); SearchKey key(uint64_t(~0ULL)); pid=INVALID_PAGEID; id=~0u; assert((tctx.tree->mode&TF_WITHDEL)==0);
	RC rc=tctx.findPageForUpdate(&key,true); if (rc!=RC_OK) return rc;
	const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage *)tctx.pb->getPageBuf(); assert(tp->info.fmt.isSeq());
	key.v.u=id=uint32_t(tp->info.prefix+tp->info.nEntries); rc=ctx->trpgMgr->insert(tctx,key,val,lval);
	if (rc==RC_OK) pid=tctx.pb->getPageID(); return rc;
}

RC Tree::update(const SearchKey& key,const void *oldValue,ushort lOldValue,const void *newValue,ushort lNewValue)
{
	TreeCtx tctx(*this); RC rc=tctx.findPageForUpdate(&key); if (rc!=RC_OK) return rc;
	return ctx->trpgMgr->update(tctx,key,oldValue,lOldValue,newValue,lNewValue);
}

RC Tree::edit(const SearchKey& key,const void *newValue,ushort lNewValue,ushort lOld,ushort sht)
{
	byte *pOld=NULL; if (lOld!=0 && (pOld=(byte*)alloca(lOld))==NULL) return RC_NORESOURCES;
	TreeCtx tctx(*this); RC rc=tctx.findPageForUpdate(&key); if (rc!=RC_OK) return rc;
	const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)tctx.pb->getPageBuf(); const byte *pData; ushort lData;
	if (lOld>0 && (pData=(const byte*)tp->getValue(key,lData))!=NULL && sht+lOld<=lData) memcpy(pOld,pData+sht,lOld);
	return ctx->trpgMgr->edit(tctx,key,newValue,lNewValue,lOld,sht);
}

RC Tree::truncate(const SearchKey& key,uint64_t val,bool fCount)
{
	TreeCtx tctx(*this); RC rc=tctx.findPageForUpdate(&key); if (rc!=RC_OK) return rc;
	return rc==RC_OK?ctx->trpgMgr->truncate(tctx,key,val,fCount):rc;
}

RC Tree::remove(const SearchKey& key,const void *value,ushort lval,unsigned multi)
{
	TreeCtx tctx(*this); RC rc=tctx.findPageForUpdate(&key);
	return rc==RC_OK?ctx->trpgMgr->remove(tctx,key,value,lval,multi):rc;
}

RC Tree::drop(PageID pid,StoreCtx *ctx,TreeFreeData *dd)
{
	PageID stack[TREE_MAX_DEPTH]; int stkp=0; RC rc; StackAlloc pending(0);
	do {
		stack[stkp++]=pid; assert(pid!=INVALID_PAGEID);
		PBlockP pb(ctx->bufMgr->getPage(pid,ctx->trpgMgr),0); if (pb.isNull()) return RC_NOTFOUND;
		const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
		if (tp->info.level>=TREE_MAX_DEPTH) return RC_CORRUPTED;
		pid=tp->isLeaf()?INVALID_PAGEID:tp->info.leftMost;
	} while (pid!=INVALID_PAGEID);
	while (stkp>0) for (pid=stack[--stkp]; pid!=INVALID_PAGEID;) {
		PBlock *pb=ctx->bufMgr->getPage(pid,ctx->trpgMgr,PGCTL_XLOCK); if (pb==NULL) break;
		pid=((const TreePageMgr::TreePage*)pb->getPageBuf())->info.sibling;
		if (!pb->isDependent()) {
			if ((rc=ctx->trpgMgr->drop(pb,dd))!=RC_OK) {if (pb!=NULL) pb->release(); return rc;}
		} else {
			PageID *pp=pending.alloc0<PageID>(); if (pp!=NULL) *pp=pb->getPageID();
			pb->release(); if (pp==NULL) return RC_NORESOURCES;
		}
	}
	while (pending.pop(&pid)) {
		PBlock *pb=ctx->bufMgr->getPage(pid,ctx->trpgMgr,PGCTL_XLOCK); 
		if (pb!=NULL && (rc=ctx->trpgMgr->drop(pb,dd))!=RC_OK) {if (pb!=NULL) pb->release(); return rc;}
	}
	return RC_OK;
}

TreeConnect *Tree::persist(uint32_t&) const
{
	return NULL;
}

//---------------------------------------------------------------------------------------

TreeStdRoot::~TreeStdRoot()
{
}

PageID TreeStdRoot::startPage(const SearchKey*,int& level,bool fRead,bool fBefore)
{
	if (root==INVALID_PAGEID && !fRead) {
		PBlock *pb=ctx->fsMgr->getNewPage(ctx->trpgMgr);
		if (pb!=NULL) {
			if (ctx->trpgMgr->initPage(pb,indexFormat(),0,NULL,INVALID_PAGEID,INVALID_PAGEID)!=RC_OK) 
				pb->release();
			else 
				{root=pb->getPageID(); pb->release(); level=0;}
		}
	}
	level=root==INVALID_PAGEID?-1:(int)height;
	return root;
}

PageID TreeStdRoot::prevStartPage(PageID)
{
	return INVALID_PAGEID;
}

RC TreeStdRoot::addRootPage(const SearchKey& key,PageID& pageID,unsigned level)
{
	if (level<height) {
		// re-traverse from root etc.
		return RC_OK;
	}
	Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION; if (!ses->inWriteTx()) return RC_READTX;
	PageID newp; PBlock *pb; RC rc;
	if ((rc=ctx->fsMgr->allocPages(1,&newp))!=RC_OK) return rc;
	assert(newp!=INVALID_PAGEID);
	if ((pb=ctx->bufMgr->newPage(newp,ctx->trpgMgr,NULL,0,ses))==NULL) return RC_NORESOURCES;
	if ((rc=ctx->trpgMgr->initPage(pb,indexFormat(),ushort(level+1),&key,root,pageID))==RC_OK)
		{pageID=root=newp; height=level+1;}
	pb->release(); return rc;
}

RC TreeStdRoot::removeRootPage(PageID page,PageID leftmost,unsigned level)
{
	if (level!=height || root!=page || leftmost==INVALID_PAGEID) return RC_FALSE;
	root=leftmost; height--; return RC_OK;
}

unsigned TreeStdRoot::getStamp(TREE_NODETYPE nt) const
{
	return stamps[nt];
}

void TreeStdRoot::getStamps(unsigned stmps[TREE_NODETYPE_ALL]) const
{
	memcpy(stmps,stamps,sizeof(stamps));
}

void TreeStdRoot::advanceStamp(TREE_NODETYPE nt)
{
	stamps[nt]++;
}

TreeGlobalRoot::~TreeGlobalRoot()
{
}

TreeGlobalRoot::TreeGlobalRoot(unsigned idx,IndexFormat ifm,StoreCtx *ct,unsigned md) 
	: TreeStdRoot(ct->theCB->getRoot(idx),ct,md),index(idx),ifmt(ifm)
{
	ct->treeMgr->registerFactory(*this);
}


IndexFormat TreeGlobalRoot::indexFormat() const
{
	return ifmt;
}

PageID TreeGlobalRoot::startPage(const SearchKey *key,int& level,bool fRead,bool fBefore)
{
	if (root==INVALID_PAGEID) {
		RWLockP rw(&rootLock,RW_X_LOCK);
		if (root==INVALID_PAGEID) {
			if ((root=ctx->theCB->getRoot(index))==INVALID_PAGEID && !fRead) {
				MiniTx tx(Session::getSession(),0);
				if (TreeStdRoot::startPage(key,level,fRead,fBefore)==INVALID_PAGEID || setRoot(root,INVALID_PAGEID)!=RC_OK) 
					root=INVALID_PAGEID; else tx.ok();
			}
		}
	}
	level=root==INVALID_PAGEID?-1:(int)height;
	return root;
}

RC TreeGlobalRoot::addRootPage(const SearchKey& key,PageID& pageID,unsigned level)
{
	RWLockP rw(&rootLock,RW_X_LOCK); PageID oldRoot=ctx->theCB->getRoot(index);
	RC rc=TreeStdRoot::addRootPage(key,pageID,level);
	return rc==RC_OK && oldRoot!=root ? setRoot(root,oldRoot) : rc;
}

RC TreeGlobalRoot::removeRootPage(PageID page,PageID leftmost,unsigned level)
{
	RWLockP rw(&rootLock,RW_X_LOCK); PageID oldRoot=ctx->theCB->getRoot(index);
	RC rc=TreeStdRoot::removeRootPage(page,leftmost,level);
	return rc==RC_OK && oldRoot!=root ? setRoot(root,oldRoot) : rc;
}

RC TreeGlobalRoot::setRoot(PageID newRoot,PageID oldRoot)
{
	MapAnchorUpdate upd={oldRoot,newRoot}; Session *ses=Session::getSession();
	ctx->cbLSN=ctx->logMgr->insert(ses,LR_UPDATE,index<<PGID_SHIFT|PGID_MASTER,INVALID_PAGEID,NULL,&upd,sizeof(MapAnchorUpdate));
	return ctx->theCB->update(ctx,index,(byte*)&upd,sizeof(MapAnchorUpdate));
}

bool TreeGlobalRoot::lock(RW_LockType lty,bool fTry) const
{
	return fTry?rwlock.trylock(lty):(rwlock.lock(lty),true);
}

void TreeGlobalRoot::unlock() const
{
	rwlock.unlock();
}

void TreeGlobalRoot::destroy()
{
}

RC TreeGlobalRoot::dropTree()
{
	RWLockP rwl(&rwlock,RW_X_LOCK),rootl(&rootLock,RW_X_LOCK); RC rc=RC_OK; mode|=TF_NOPOST;
	if (root!=INVALID_PAGEID) {
		PageID old=root; 
		if ((rc=drop(root,ctx))==RC_OK && (rc=setRoot(INVALID_PAGEID,old))==RC_OK) root=INVALID_PAGEID;
	}
	return rc;
}

TreeFactory *TreeGlobalRoot::getFactory() const
{
	return (TreeFactory*)this;
}

byte TreeGlobalRoot::getID() const
{
	return (byte)index;
}

byte TreeGlobalRoot::getParamLength() const
{
	return 0;
}
	
void TreeGlobalRoot::getParams(byte *,const Tree&) const
{
}

RC TreeGlobalRoot::createTree(const byte *,byte,Tree *&tree)
{
	tree=this; return RC_OK;
}

//-------------------------------------------------------------------------------

const ushort SearchKey::extKeyLen[KT_ALL] = 
{
	sizeof(uint64_t),sizeof(int64_t),sizeof(float),sizeof(double),0,0,0
};

RC SearchKey::deserialize(const void *buf,size_t l,size_t *pl)
{
	const byte *p=(const byte *)buf; ushort ll=0,dl=1;
	if (l<1 || (type=(TREE_KT)*p)>KT_ALL) return RC_CORRUPTED;
	if (type<KT_BIN) {
		if (unsigned(ll=extKeyLen[type])+1>l) return RC_CORRUPTED;
		memcpy(&v,p+1,ll); loc=PLC_EMB;
	} else if (type<KT_ALL) {
		if (l<2) return RC_CORRUPTED; dl++;
		if (((ll=p[1])&0x80)!=0) {if (l<3) return RC_CORRUPTED; ll=ll&~0x80|p[2]<<7; dl++;}
		if (l<unsigned(ll+dl)) return RC_CORRUPTED;
		v.ptr.p=p+dl; v.ptr.l=ll; loc=PLC_SPTR;	// copy???
	}
	if (pl!=NULL) *pl=ll+dl; return RC_OK;
}

int IndexKeyV::cmp(const IndexKeyV& rhs,TREE_KT type) const
{
	int cmp;
	switch (type) {
	default: break;
	case KT_UINT: return cmp3(u,rhs.u);
	case KT_INT: return cmp3(i,rhs.i);
	case KT_FLOAT: return cmp3(f,rhs.f);
	case KT_DOUBLE: return cmp3(d,rhs.d);
	case KT_BIN: return (cmp=memcmp(ptr.p,rhs.ptr.p,min(ptr.l,rhs.ptr.l)))!=0?cmp:cmp3(ptr.l,rhs.ptr.l);
	case KT_REF: return PINRef::cmpPIDs((byte*)ptr.p,(byte*)rhs.ptr.p);
	case KT_VAR: return cmpMSeg((byte*)ptr.p,ptr.l,(byte*)rhs.ptr.p,rhs.ptr.l);
	}
	return -2;
}

namespace AfyKernel
{
enum KVT
{
	KVT_NULL,KVT_INT,KVT_UINT,KVT_INT64,KVT_UINT64,KVT_FLOAT,KVT_SUINT,KVT_INTV,KVT_URI,KVT_STR,KVT_BIN,KVT_URL,KVT_REF
};
const static ValueType vtFromKVT[] =
{
	VT_ANY,VT_INT,VT_UINT,VT_INT64,VT_UINT64,VT_DOUBLE,VT_DATETIME,VT_INTERVAL,VT_URIID,VT_STRING,VT_BSTR,VT_URL,VT_REFID
};
const static TREE_KT ktFromKVT[] =
{
	KT_ALL,KT_INT,KT_UINT,KT_INT,KT_UINT,KT_DOUBLE,KT_UINT,KT_INT,KT_UINT,KT_BIN,KT_BIN,KT_BIN,KT_REF
};
};

RC SearchKey::toKey(const Value **ppv,unsigned nv,const IndexSeg *kds,int idx,Session *ses,MemAlloc *ma)
{
	RC rc=RC_OK; byte *buf=NULL,*p; const bool fVar=nv>1 || kds[0].type==VT_ANY && kds[0].lPrefix==0 && (kds[0].flags&ORD_NCASE)==0;
	bool fDel=false; unsigned xbuf=512,lkey=0; if (ma==NULL) ma=ses;
	for (unsigned ii=0; ii<nv; ii++) {
		const IndexSeg& ks=kds[ii]; const Value *pv=ppv[ii]; Value w; byte kbuf[XPINREFSIZE];
		type=KT_ALL; loc=PLC_EMB;
		if (pv==NULL || pv->isEmpty()) {if (!fVar) return RC_TYPE;}
		else {
			if (pv->type==VT_RANGE) {
				if (idx==-1) {rc=RC_TYPE; break;} assert(idx==0||idx==1); pv=&pv->varray[idx]; 
				if (pv->type==VT_ANY) {if (!fVar && pv->varray[idx^1].type==VT_ANY) return RC_TYPE; pv=NULL;}
			} else switch (ks.op) {
			default: assert(0);
			case OP_EQ: case OP_BEGINS: case OP_IN: break;
			case OP_LT: case OP_LE: if (idx==0) pv=NULL; break;
			case OP_GT: case OP_GE: if (idx==1) pv=NULL; break;
			}
			if (pv!=NULL) {
				PID id; PropertyID pid=STORE_INVALID_URIID; ElementID eid=STORE_COLLECTION_ID;
				if (pv->type==VT_CURRENT) switch (pv->ui) {
				default: return RC_TYPE;
				case CVT_TIMESTAMP: {TIMESTAMP ts; getTimestamp(ts); w.setDateTime(ts); pv=&w;} break;
				case CVT_USER: w.setIdentity(ses->getIdentity()); pv=&w; break;
				case CVT_STORE: w.set((unsigned)ses->getStore()->storeID); pv=&w; break;
				}
				const ValueType kty=ks.type==VT_ANY&&(ks.lPrefix!=0||(ks.flags&ORD_NCASE)!=0)?VT_STRING:(ValueType)ks.type;
				if (kty==VT_ANY) {
					if (pv->type==VT_STRING && testStrNum(pv->str,pv->length,w)) pv=&w;
				} else if (pv->type!=kty && (!isString((ValueType)pv->type) || !isString(kty))) {
					if ((rc=convV(*pv,w,kty,ma,CV_NOTRUNC))==RC_OK) pv=&w; else break;
				}
				switch (pv->type) {
				default: rc=RC_TYPE; break;
				case VT_INT: v.i=pv->i; type=KT_INT; break;
				case VT_UINT: case VT_URIID: case VT_IDENTITY: v.u=pv->ui; type=KT_UINT; break;
				case VT_INT64: case VT_INTERVAL: v.i=pv->i64; type=KT_INT; break;
				case VT_UINT64: case VT_DATETIME: v.u=pv->ui64; type=KT_UINT; break;
				case VT_ENUM: v.u=(uint64_t)pv->enu.enumid<<32|pv->enu.eltid; type=KT_UINT; break;	//???
				case VT_FLOAT: v.f=pv->f; type=KT_FLOAT; break;		// units?
				case VT_DOUBLE: v.d=pv->d; type=KT_DOUBLE; break;	// units?
				case VT_BOOL: v.u=pv->b?1:0; type=KT_UINT; break;
				case VT_STREAM:
					//???
				//case VT_RESERVED2:
				case VT_STRING:
					if (pv->str!=NULL && (ks.flags&ORD_NCASE)!=0) {
						uint32_t len=0;
						if ((v.ptr.p=forceCaseUTF8(pv->str,pv->length,len,ma))==NULL) {rc=RC_NORESOURCES; break;}
						assert((len&0xFFFF0000)==0); v.ptr.l=ks.lPrefix!=0?min(ks.lPrefix,(ushort)len):(ushort)len;
						type=KT_BIN; loc=PLC_ALLC; break;
					}
				case VT_BSTR: case VT_URL:
					v.ptr.p=pv->bstr; v.ptr.l=ks.lPrefix!=0?min(ks.lPrefix,(ushort)pv->length):(ushort)pv->length;
					if (pv==&w) {loc=PLC_ALLC; w.type=VT_ERROR;} type=KT_BIN; break;
				case VT_REF: id=pv->pin->getPID(); goto encode;
				case VT_REFID: id=pv->id; goto encode;
				case VT_REFPROP: id=pv->ref.pin->getPID(); pid=pv->ref.pid; goto encode;
				case VT_REFIDPROP: id=pv->refId->id; pid=pv->refId->pid; goto encode;
				case VT_REFELT:	id=pv->ref.pin->getPID(); pid=pv->ref.pid; eid=pv->ref.eid; goto encode;
				case VT_REFIDELT: id=pv->refId->id; pid=pv->refId->pid; eid=pv->refId->eid;
				encode:
					{PINRef pr(ses->getStore()->storeID,id);
					if (pv->type>=VT_REFPROP) {pr.def|=PR_U1; pr.u1=pid; if (pv->type>=VT_REFELT) {pr.def|=PR_U2; pr.u2=eid;}}
					v.ptr.p=kbuf; v.ptr.l=pr.enc(kbuf); type=KT_REF;}
					break;
				}
				if (rc!=RC_OK) break;
			}
		}
		if (fVar) {
			uint32_t l0=1,l=0,j; const byte *pd=(byte*)&v; byte kt=(ks.flags&ORD_DESC)!=0?0xC0:0x80;
			switch (type) {
			default: assert(0);
			case KT_ALL:
				assert(pv==NULL||pv->isEmpty()); kt=idx==0||idx==-1&&(ks.flags&ORD_NULLS_AFTER)==0?0x80|KVT_NULL:0x90|KVT_NULL; break;
			case KT_INT:
				j=3-((int32_t)v.i==v.i)-((int16_t)v.i==v.i)-((int8_t)v.i==v.i); 
				kt|=j<<4|(pv->type==VT_INTERVAL?KVT_INTV:pv->type-VT_INT+KVT_INT); l=1<<j;
#ifdef _MSBF
				pd+=sizeof(int64_t)-l;
#endif
				break;
			case KT_UINT:
				if (pv->type<=VT_UINT64) j=3-((uint32_t)v.u==v.u)-((uint16_t)v.u==v.u)-((uint8_t)v.u==v.u),l=1<<j,kt|=j<<4|(pv->type-VT_UINT+KVT_UINT);
				else if (pv->type==VT_IDENTITY) j=2-((uint16_t)v.u==v.u),l=1<<j,kt|=j<<4|KVT_SUINT;
				else if (pv->type==VT_BOOL) kt|=KVT_SUINT; else l=sizeof(uint64_t),kt|=0x30|KVT_SUINT;
#ifdef _MSBF
				pd+=sizeof(uint64_t)-l;
#endif
				break;
			case KT_FLOAT: if (pv->qval.units==Un_NDIM) kt|=KVT_FLOAT,l=sizeof(float); else kt|=0x20|KVT_FLOAT,l=sizeof(float)+1; break;
			case KT_DOUBLE:  if (pv->qval.units==Un_NDIM) kt|=0x10|KVT_FLOAT,l=sizeof(double); else kt|=0x30|KVT_FLOAT,l=sizeof(double)+1; break;
			case KT_BIN:
				if ((l=v.ptr.l)<0x80 && (kt&0x40)==0 && pv->type==VT_STRING) kt=byte(l); else kt|=((l&0xFF00)!=0?(l0+=2,0x10):(l0+=1,0))|(pv->type-VT_STRING+KVT_STR);
				pd=(byte*)v.ptr.p; break;
			case KT_REF:
				pd=kbuf; l=v.ptr.l; l0++; kt|=KVT_REF; break;
			}
			if (lkey+l0+l>xbuf || buf==NULL && (buf=(byte*)alloca(xbuf))==NULL) {
				size_t old=xbuf; xbuf=max((unsigned)(lkey+l0+l),xbuf);
				if (buf!=NULL && !fDel) {
					if ((p=(byte*)ma->malloc(xbuf))==NULL) {rc=RC_NORESOURCES; break;}
					memcpy(p,buf,lkey); buf=p; fDel=true;
				} else if ((buf=(byte*)ma->realloc(buf,xbuf,old))==NULL) {rc=RC_NORESOURCES; break;}
			}
			buf[lkey]=kt; lkey+=l0; while (l0>1) {--l0; buf[lkey-l0]=byte(l>>8*(l0-1));}
			if ((kt&0xAF)==(0xA0|KVT_FLOAT)) {memcpy(buf+lkey,pd,l-1); buf[lkey+l-1]=(byte)pv->qval.units;} else if (l!=0) memcpy(buf+lkey,pd,l); 
			lkey+=l; if (loc==PLC_ALLC) ma->free((void*)v.ptr.p);
		}
		if (pv==&w) freeV(w);
	}
	if (rc!=RC_OK) {
		if (fDel) ma->free(buf); if (type>=KT_BIN && loc==PLC_ALLC) ma->free((void*)v.ptr.p); type=KT_ALL; loc=PLC_EMB;
	} else if (buf!=NULL) {
		if (fDel) v.ptr.p=xbuf>=lkey*2?(byte*)ma->realloc(buf,lkey,xbuf):buf;
		else if ((p=(byte*)ma->malloc(lkey))==NULL) {type=KT_ALL; loc=PLC_EMB; return RC_NORESOURCES;}
		else {v.ptr.p=p; memcpy(p,buf,lkey);}
		type=KT_VAR; loc=PLC_ALLC; v.ptr.l=uint16_t(lkey);
	} else if (type>=KT_BIN && type<KT_ALL && loc==PLC_EMB && v.ptr.p!=NULL) {
		const void *p=v.ptr.p;
		if ((v.ptr.p=ma->malloc(v.ptr.l))==NULL) rc=RC_NORESOURCES;
		else {memcpy((void*)v.ptr.p,p,v.ptr.l); loc=PLC_ALLC;}
	}
	return rc;
}

static int cmpSeg(const byte *&s1,ushort& l1,const byte *&s2,ushort& l2,byte mbeg=0)
{
	assert(l1!=0&&l2!=0);
	byte mod1=*s1++,ty1=mod1&0x0f,mod2=*s2++,ty2=mod2&0x0F; ushort ls1=0,ls2=0; --l1,--l2;
	union {uint64_t ui64; int64_t i64; float f; double d; QualifiedValue qv;} v1,v2; int cmp=0;

	if ((mod1&0x80)==0) {ty1=KVT_STR; ls1=mod1; mod1=0;}
	else if (ty1==KVT_FLOAT) ls1=((mod1&0x10)!=0?sizeof(double):sizeof(float))+((mod1&0x20)!=0?1:0);
	else if (ty1!=KVT_NULL) {ls1=1<<(mod1>>4&3); if (ty1>=KVT_STR) {if (l1<ls1) throw 1; l1-=ls1; for (unsigned j=ls1,i=ls1=0; i<j; i++) ls1=ls1<<8|*s1++;}} 
	if (l1<ls1 || ty1>KVT_REF) throw 2;

	if ((mod2&0x80)==0) {ty2=KVT_STR; ls2=mod2; mod2=0;}
	else if (ty2==KVT_FLOAT) ls2=((mod2&0x10)!=0?sizeof(double):sizeof(float))+((mod2&0x20)!=0?1:0);
	else if (ty2!=KVT_NULL) {ls2=1<<(mod2>>4&3); if (ty2>=KVT_STR) {if (l2<ls2) throw 3; l2-=ls2; for (unsigned j=ls2,i=ls2=0; i<j; i++) ls2=ls2<<8|*s2++;}} 
	if (l2<ls2 || ty2>KVT_REF) throw 4;

	if (ty1==KVT_NULL) cmp=ty2==KVT_NULL?cmp3(mod1&0x10,mod2&0x10):(mod1&0x10)!=0?1:-1; else if (ty2==KVT_NULL) cmp=(mod2&0x10)!=0?-1:1;
	else {
		if (ty1==ty2 && ls1==ls2 && ls1<=1) {if (ls1!=0) cmp=ktFromKVT[ty1]==KT_INT?cmp3(*(int8_t*)s1,*(int8_t*)s2):cmp3(*s1,*s2);}
		else {
			TREE_KT t1=ktFromKVT[ty1],t2=ktFromKVT[ty2];
			if (t1<KT_BIN) {
				if (ls1==1) {if (t1==KT_INT) v1.i64=*(int8_t*)s1; else v1.ui64=*(uint8_t*)s1;}
				else {
					memcpy(&v1,s1,ls1);
					if (ty1==KVT_FLOAT) {if ((mod1&0x10)==0) t1=KT_FLOAT;}
					else if (ls1<sizeof(uint64_t)) {if (t1==KT_INT) v1.i64=ls1==sizeof(int32_t)?*(int32_t*)&v1.i64:*(int16_t*)&v1.i64; else v1.ui64=ls1==sizeof(uint32_t)?*(uint32_t*)&v1.ui64:*(uint16_t*)&v1.ui64;}
				}
			}
			if (t2<KT_BIN) {
				if (ls2==1) {if (t2==KT_INT) v2.i64=*(int8_t*)s2; else v2.ui64=*(uint8_t*)s2;}
				else {
					memcpy(&v2,s2,ls2);
					if (ty2==KVT_FLOAT) {if ((mod2&0x10)==0) t2=KT_FLOAT;} 
					else if (ls2<sizeof(uint64_t)) {if (t2==KT_INT) v2.i64=ls2==sizeof(int32_t)?*(int32_t*)&v2.i64:*(int16_t*)&v2.i64; else v2.ui64=ls2==sizeof(uint32_t)?*(uint32_t*)&v2.ui64:*(uint16_t*)&v2.ui64;}
				}
			}
			if (t1!=t2) {
				if (ty1>=KVT_STR || ty2>=KVT_STR) cmp=cmp3(ty1,ty2);
				else if (ty1==KVT_FLOAT || ty2==KVT_FLOAT) {
					if (t1==KT_UINT) v1.d=(double)v1.ui64; else if (t1==KT_INT) v1.d=(double)v1.i64; else if ((mod1&0x10)==0) v1.d=v1.f;
					if (t2==KT_UINT) v2.d=(double)v2.ui64; else if (t2==KT_INT) v2.d=(double)v2.i64; else if ((mod2&0x10)==0) v2.d=v2.f;
					cmp=cmp3(v1.d,v2.d);
				} else if (t1==KT_INT) {
					if (v1.i64<0) cmp=-1; else cmp=cmp3((uint64_t)v1.i64,v2.ui64);
				} else {
					assert(t2==KT_INT); if (v2.i64<0) cmp=1; else cmp=cmp3(v1.ui64,(uint64_t)v2.i64);
				}
			} else switch (t1) {
			default: break;
			case KT_UINT: cmp=cmp3(v1.ui64,v2.ui64); break;
			case KT_INT: cmp=cmp3(v1.i64,v2.i64); break;
			case KT_FLOAT: cmp=cmp3(v1.f,v2.f); break;
			case KT_DOUBLE: cmp=cmp3(v1.d,v2.d); break;
			case KT_REF: cmp=PINRef::cmpPIDs(s1,s2); break;
			case KT_BIN: if ((cmp=memcmp(s1,s2,min(ls1,ls2)))==0) cmp=ls1<ls2?(mbeg&1)!=0?0:-1:ls1==ls2?0:(mbeg&2)!=0?0:1; break;
			}
		}
		if (((mod1|mod2)&0x40)!=0) cmp=-cmp;
	}
	s1+=ls1; l1-=ls1; s2+=ls2; l2-=ls2; return cmp;
}

int AfyKernel::cmpMSeg(const byte *s1,ushort l1,const byte *s2,ushort l2)
{
	try {
		do {int cmp=cmpSeg(s1,l1,s2,l2); if (cmp!=0) return cmp;} while (l1*l2!=0);
		if ((l1|l2)==0) return 0;
	} catch (int) {
		// report
	}
	return -2;
}

bool AfyKernel::isHyperRect(const byte *s1,ushort l1,const byte *s2,ushort l2)
{
	try {
		do {int cmp=cmpSeg(s1,l1,s2,l2); if (cmp!=0) return (l1|l2)!=0;} while (l1*l2!=0);
	} catch (int) {
		// report
	}
	return false;
}

bool AfyKernel::cmpBound(const byte *s1,ushort l1,const byte *s2,ushort l2,const IndexSeg *sg,unsigned nSegs,bool fStart)
{
	try {
		unsigned i=0;
		do {
			int cmp=cmpSeg(s1,l1,s2,l2,sg==0||(sg[i].flags&SCAN_PREFIX)==0?0:1); 
			if (fStart) {if (cmp<0) break; if (cmp>0 || cmp==0 && sg!=NULL && (sg[i].flags&SCAN_EXCLUDE_START)!=0) return false;} 
			else if (cmp>0) break; else if (cmp<0 || cmp==0 && sg!=NULL && (sg[i].flags&SCAN_EXCLUDE_END)!=0) return false;
		} while (l1*l2!=0 && (sg==NULL||++i<nSegs));
		return true;
	} catch (int) {
		// report
	}
	return false;
}

bool AfyKernel::checkHyperRect(const byte *s1,ushort l1,const byte *s2,ushort l2,const IndexSeg *sg,unsigned nSegs,bool fStart)
{
	try {
		unsigned i=0;
		do {
			int cmp=cmpSeg(s1,l1,s2,l2,sg==0||(sg[i].flags&SCAN_PREFIX)==0?0:fStart?1:2);
			if (cmp>0 || cmp==0 && sg!=NULL && (sg[i].flags&(fStart?SCAN_EXCLUDE_START:SCAN_EXCLUDE_END))!=0) return false;
		} while (l1*l2!=0 && (sg==NULL||++i<nSegs));
		return true;
	} catch (int) {
		// report
	}
	return false;
}

ushort AfyKernel::calcMSegPrefix(const byte *s1,ushort l1,const byte *s2,ushort l2)
{
//	for (ushort lPrefix=0;;) {
		//if (l1*l2==0 || *s1!=*s2) return lPrefix;
		//...
		return 0;
//	}
}

RC SearchKey::getValues(Value *vals,unsigned nv,const IndexSeg *kd,unsigned nFields,Session *ses,bool fFilter,MemAlloc *ma) const
{
	RC rc=RC_OK; RefVID r,*pr; void *p; if (ma==NULL) ma=ses;
	if (type==KT_VAR) {
		const byte *s=(byte*)getPtr2(); ushort l=v.ptr.l;
		for (unsigned i=0; i<nFields; i++,kd++) {
			if (l==0) return RC_CORRUPTED; Value *pv;
			if (fFilter) pv=(Value*)VBIN::find(kd->propID,vals,nv); else if (i>=nv) break; else pv=&vals[i];
			byte mod=*s++,ty=mod&0x0F; ushort ls=0; --l;
			if ((mod&0x80)==0) {ty=KVT_STR; ls=mod; mod=0;}
			else if (ty==KVT_FLOAT) ls=((mod&0x10)!=0?sizeof(double):sizeof(float))+((mod&0x20)!=0?1:0);
			else if (ty!=KVT_NULL) {ls=1<<(mod>>4&3); if (ty>=KVT_STR) {if (l<ls) return RC_CORRUPTED; for (unsigned j=ls,i=ls=0; i<j; i++) ls=ls<<8|*s++;}} 
			if (l<ls || ty>KVT_REF) return RC_CORRUPTED;
			if (pv!=NULL && (pv->property==kd->propID||pv->property==PROP_SPEC_ANY)) {
				pv->type=vtFromKVT[ty]; setHT(*pv); pv->meta=0; pv->eid=STORE_COLLECTION_ID; pv->op=OP_SET; pv->length=ls; pv->property=kd->propID;
				if (unsigned(ty-KVT_INT)<=unsigned(KVT_URI-KVT_INT)) memcpy(&pv->i,s,ls);
				switch (ty) {
				default: break;
				case KVT_INT: if (ls!=sizeof(int32_t)) pv->i=ls==sizeof(int8_t)?(int32_t)*(int8_t*)&pv->i:ls==sizeof(int16_t)?(int32_t)*(int16_t*)&pv->i:(int32_t)pv->i64; break;
				case KVT_UINT: case KVT_URI: if (ls!=sizeof(uint32_t)) pv->ui=ls==sizeof(uint8_t)?(uint32_t)*(uint8_t*)&pv->ui:ls==sizeof(uint16_t)?(uint32_t)*(uint16_t*)&pv->ui:(uint32_t)pv->ui64; break;
				case KVT_INT64: case KVT_INTV: if (ls!=sizeof(int64_t)) pv->i64=ls==sizeof(int8_t)?(int64_t)*(int8_t*)&pv->i:ls==sizeof(int16_t)?(int64_t)*(int16_t*)&pv->i:(int64_t)pv->i; break;
				case KVT_UINT64: if (ls!=sizeof(uint64_t)) pv->ui64=ls==sizeof(uint8_t)?(uint64_t)*(uint8_t*)&pv->ui:ls==sizeof(uint16_t)?(uint64_t)*(uint16_t*)&pv->ui:(uint64_t)pv->ui; break;
				case KVT_FLOAT: if ((mod&0x10)==0) pv->type=VT_FLOAT; /*if ((mod&0x20)!=0) pv->qval.units=...*/ break;
				case KVT_SUINT: if (ls==1) pv->type=VT_BOOL,pv->b=*(uint8_t*)&pv->ui!=0; else if (ls<sizeof(uint64_t)) {pv->type=VT_IDENTITY; if (ls<sizeof(uint32_t)) pv->iid=(IdentityID)*(uint16_t*)&pv->ui;} break;
				case KVT_STR: case KVT_BIN: case KVT_URL:
					if ((p=ma->malloc(ls+(pv->type==VT_BSTR?0:1)))==NULL) return RC_NORESOURCES;
					memcpy(p,s,ls); if (pv->type!=VT_BSTR) ((char*)p)[ls]='\0'; pv->str=(char*)p;
					pv->flags=ma->getAType(); break;
				case KVT_REF:
					r.pid=STORE_INVALID_URIID; r.eid=STORE_COLLECTION_ID; r.vid=STORE_CURRENT_VERSION;
					try {
						PINRef pr((ses!=NULL?ses->getStore():StoreCtx::get())->storeID,s);
						if ((kd->type==VT_REFIDELT||kd->type==VT_ANY) && (pr.def&PR_U2)!=0) {r.eid=pr.u2; pv->type=VT_REFIDELT;}
						if ((kd->type==VT_REFIDELT||kd->type==VT_REFIDPROP||kd->type==VT_ANY) && (pr.def&PR_U1)!=0) {r.pid=pr.u1; if (pv->type==VT_REFID) pv->type=VT_REFIDPROP;}
						r.id=pr.id;
					} catch (RC& rc2) {rc=rc2;}
					if (pv->type==VT_REFID) pv->id=r.id; 
					else if ((pr=(RefVID*)ma->malloc(sizeof(RefVID)))==NULL) return RC_NORESOURCES;
					else {*pr=r; pv->refId=pr; pv->flags=ma->getAType();}
					break;
				}
				if (kd->type!=VT_ANY && kd->type!=pv->type && pv->type!=VT_ANY) return RC_CORRUPTED; 
			}
			s+=ls; l-=ls;
		}
	} else {
		if (fFilter && nv>1) vals=(Value*)VBIN::find(kd->propID,vals,nv);
		else if (vals->property!=kd->propID) {if (vals->property==PROP_SPEC_ANY) vals->property=kd->propID; else vals=NULL;}
		if (vals!=NULL) {
			setHT(*vals); vals->meta=0; vals->eid=STORE_COLLECTION_ID; vals->op=OP_SET; vals->length=1; vals->type=kd->type; assert(kd->type!=VT_ANY);
			switch (type) {
			default: memcpy(&vals->i,&v,sizeof(v)); break;
			case KT_VAR: return RC_TYPE;
			case KT_REF:
				if (v.ptr.l==0) return RC_CORRUPTED;
				r.pid=STORE_INVALID_URIID; r.eid=STORE_COLLECTION_ID; r.vid=STORE_CURRENT_VERSION;
				try {
					PINRef pr(ses->getStore()->storeID,(byte*)getPtr2());
					if (kd->type==VT_REFIDELT && (pr.def&PR_U2)!=0) r.eid=pr.u2;
					if ((kd->type==VT_REFIDELT||kd->type==VT_REFIDPROP) && (pr.def&PR_U1)!=0) r.pid=pr.u1;
					r.id=pr.id;
				} catch (RC &rc) {return rc;}
				if (vals->type==VT_REFID) vals->id=r.id;
				else if ((pr=(RefVID*)ma->malloc(sizeof(RefVID)))==NULL) return RC_NORESOURCES;
				else {*pr=r; vals->refId=pr; vals->flags=ma->getAType();}
				break;
			case KT_BIN:
				switch (kd->type) {
				default: case VT_BSTR:
					if ((p=ma->malloc(v.ptr.l))==NULL) return RC_NORESOURCES;
					memcpy(p,getPtr2(),v.ptr.l); vals->bstr=(unsigned char*)p; vals->length=v.ptr.l; vals->flags=ma->getAType(); break;
				case VT_STRING: case VT_URL:
					if ((p=ma->malloc(v.ptr.l+1))==NULL) return RC_NORESOURCES;
					memcpy(p,getPtr2(),v.ptr.l); ((char*)p)[v.ptr.l]='\0'; vals->str=(char*)p; vals->length=v.ptr.l; vals->flags=ma->getAType(); break;	
				}
				break;
			}
		}
	}
	return rc;
}
