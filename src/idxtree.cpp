/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "pgtree.h"
#include "buffer.h"
#include "txmgr.h"
#include "fsmgr.h"
#include "logmgr.h"
#include "pinref.h"
#include "startup.h"
#include "mvstoreimpl.h"

using namespace MVStore;
using namespace MVStoreKernel;

namespace MVStoreKernel
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
	TreeRQ(StoreCtx *ct,const TreeCtx& tctx,Tree::TreeOp o,PageID chld) : TreeCtx(tctx),list(this),ctx(ct),child(chld),op(o),key((uint32_t)~0u) {}
public:
	virtual		~TreeRQ();
	PageID		getKey() const {return child;}
	void		process() {
		if (tcon!=NULL && (tree=tcon->connect(thndl))==NULL) return;
		if (op!=Tree::TO_DELETE) {ctx->trpgMgr->addNewPage(*this,key,child); return;}
		assert((mode&TF_WITHDEL)!=0); PBlock *pbl; ulong pos;
		if (getParentPage(key,PTX_FORUPDATE|PTX_FORDELETE)!=RC_OK) return;
		TreePageMgr::TreePage *tp=(TreePageMgr::TreePage*)pb->getPageBuf();
		if (tp->isLeaf()) {
			assert(!tp->info.fmt.isUnique() || tp->info.fmt.isKeyOnly());
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
	TreeRQTable(ulong hashSize) : SyncHashTab<TreeRQ,PageID,&TreeRQ::list>(hashSize) {}
};

};

TreeMgr::TreeMgr(StoreCtx *ct,ulong timeout) : ctx(ct)
{
	if ((ptrt=new(ct) TreeRQTable(DEFAULT_PITREERQ_SIZE))==NULL) throw RC_NORESOURCES;
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

PBlock *Tree::getPage(PageID pid,ulong stamp,TREE_NODETYPE type)
{
	lock(RW_S_LOCK); PBlock *pb=getStamp(type)==stamp?ctx->bufMgr->getPage(pid,ctx->trpgMgr):(PBlock*)0; unlock(); return pb;
}

RC TreeCtx::getParentPage(const SearchKey &key,ulong flags)
{
	assert(depth>0 && ((flags&PTX_FORDELETE)==0||(mode&TF_WITHDEL)!=0));
	if (!parent.isNull()) {
		pb.release(); pb=parent; parent=NULL; --depth;
		if ((flags&PTX_FORUPDATE)!=0 && !pb->isXLocked() && 
			(!pb->isULocked() || !pb.isSet(QMGR_UFORCE) || (flags&PTX_TRY)!=0 && !pb->tryupgrade())) return RC_NOACCESS;
		assert(mainKey==NULL || !((TreePageMgr::TreePage*)pb->getPageBuf())->isLeaf() || vp.len!=0 && vp.offset!=0);
		return RC_OK;
	}
	PageID pid=stack[--depth]; if (pb.isSet(PGCTL_NOREL)) pb=NULL;
	ulong xlock=(flags&PTX_TRY)!=0?PGCTL_XLOCK|QMGR_TRY:PGCTL_ULOCK|QMGR_UFORCE;
	if ((mode&TF_WITHDEL)!=0) {
		if (!tree->lock((flags&PTX_FORDELETE)!=0?RW_X_LOCK:RW_S_LOCK,(flags&PTX_TRY)!=0)) {pb.release(); return RC_FALSE;}
		TREE_NODETYPE ptty=depth>leaf?PITREE_INT2:depth==leaf?PITREE_LEAF:PITREE_INT;
		if (tree->getStamp(ptty)!=stamps[ptty]) {tree->unlock(); pb.release(); return RC_FALSE;}
		xlock|=PGCTL_COUPLE; if ((flags&PTX_FORDELETE)!=0 && ptty!=PITREE_LEAF) tree->advanceStamp(ptty);
	}
	// check latched!!!
	StoreCtx *ctx=tree->getStoreCtx();
	pb=ctx->bufMgr->getPage(pid,ctx->trpgMgr,((flags&PTX_TRY)!=0?PGCTL_XLOCK|QMGR_TRY:PGCTL_ULOCK)|pb.uforce(),pb);
	if ((mode&TF_WITHDEL)!=0) tree->unlock();
	while (!pb.isNull()) {
		const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
		bool fLeaf=mainKey!=NULL && tp->isLeaf(); assert(!fLeaf || tp->info.fmt.isKeyOnly() || !tp->info.fmt.isUnique());
		if (tp->checkSibling(fLeaf?*mainKey:key))
			pb=ctx->bufMgr->getPage(tp->info.sibling,ctx->trpgMgr,xlock,pb);
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
		ulong pos; tp->findKey(*key,pos); assert(tp->info.level>0); parent=pb; pb=NULL; depth++;
		while (pos!=~0ul && ((pid=tp->getPageID(--pos))!=prev || pos!=~0ul && (pid=tp->getPageID(--pos))!=INVALID_PAGEID)) {
			while ((pb=ctx->bufMgr->getPage(pid,ctx->trpgMgr,(fRead?0:PGCTL_XLOCK)|((mode&TF_WITHDEL)!=0?PGCTL_COUPLE:0)|pb.uforce(),pb))!=NULL) {
				const TreePageMgr::TreePage *tp2=(const TreePageMgr::TreePage *)pb->getPageBuf();
				if (tp2->info.level==0) {if (tp2->info.nSearchKeys>0) return RC_OK; else break;}
				stack[depth++]=pid; pid=tp2->getPageID(tp->info.nSearchKeys-1);
			}
			if (pb.isNull()) break;
		}
		pb.release(); pb=parent; parent=NULL; depth--;
	}
	pb.release(); parent.release();
	if (leaf==~0u) {depth=0; return findPrevPage(key,!fRead);}
	//???
	return RC_NOTFOUND;
}

RC TreeCtx::findPage(const SearchKey *key)
{
	StoreCtx *ctx=tree->getStoreCtx(); getStamps(stamps);
	int level=-1; PageID pid=startPage(key,level); PBlock *pb2;
	if (!parent.isSet(PGCTL_NOREL)) pb=parent; parent=NULL; 
	ulong xlock=(mode&TF_WITHDEL)!=0?PGCTL_COUPLE:0,kpos=~0ul; ++ctx->treeMgr->traverse;
	if (pid!=INVALID_PAGEID) for (Session *ses=Session::getSession();;) {
		if (ses!=NULL && (pb2=ses->getLatched(pid))!=NULL) {	pb.release(); pb=pb2; pb.set(PGCTL_NOREL);}
		else if ((pb=ctx->bufMgr->getPage(pid,ctx->trpgMgr,xlock,pb.isSet(PGCTL_NOREL)?(PBlock*)0:(PBlock*)pb))==NULL) break;
		const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
		++ctx->treeMgr->pageRead; if (tp->info.level>TREE_MAX_DEPTH) break;
		if (tp->info.nSearchKeys==0 && depth>0 && key!=NULL)
			{assert((mode&TF_WITHDEL)!=0); TreeRQ::post(*this,Tree::TO_DELETE,pid,key);}
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
	ulong xlock=pb.isSet(QMGR_UFORCE)?QMGR_UFORCE:0,kpos=~0ul;
	StoreCtx *ctx=tree->getStoreCtx(); ++ctx->treeMgr->traverse; getStamps(stamps); assert(key!=NULL);
	int level=-1; PageID pid=startPage(key,level,false); if (level==0||level==1) xlock|=PGCTL_ULOCK;
	while (pid!=INVALID_PAGEID && (pb=ctx->bufMgr->getPage(pid,ctx->trpgMgr,xlock,pb))!=NULL) {
		++ctx->treeMgr->pageRead; xlock&=~PGCTL_COUPLE; if ((xlock&PGCTL_ULOCK)!=0) pb.set(QMGR_UFORCE);
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
			pid=tp->info.sibling; if ((mode&TF_WITHDEL)!=0) xlock|=PGCTL_COUPLE; ++ctx->treeMgr->sideLink;
		} else if (!fIns && tp->info.nSearchKeys==0 && (tp->info.level==0||tp->info.leftMost==INVALID_PAGEID)) break;
		else if (tp->info.level!=0) {
			parent.release(); parent=pb; pb=NULL; if (tp->info.level<=2) xlock|=PGCTL_ULOCK;
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
	ulong xlock=pb.isSet(QMGR_UFORCE)?QMGR_UFORCE:0,kpos=~0ul,lkey=0; const SearchKey *key=key0; 
	StoreCtx *ctx=tree->getStoreCtx(); ++ctx->treeMgr->traverse; getStamps(stamps); PageID next=INVALID_PAGEID;
	int level=-1; PageID pid=startPage(key,level,!fUpd,true); if (fUpd && (level==0||level==1)) xlock|=PGCTL_ULOCK;
	while (pid!=INVALID_PAGEID && (pb=ctx->bufMgr->getPage(pid,ctx->trpgMgr,xlock,pb))!=NULL) {
		++ctx->treeMgr->pageRead; xlock&=~PGCTL_COUPLE; if ((xlock&PGCTL_ULOCK)!=0) pb.set(QMGR_UFORCE);
		const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
		if (tp->info.level>TREE_MAX_DEPTH) break; 
		bool fFollow=tp->hasSibling()&&(key==NULL||tp->hasSibling()&&next!=tp->info.sibling&&tp->testKey(*key,ushort(~0u))>0);
		if (!fFollow && (tp->info.nSearchKeys==0 || key!=NULL&&tp->testKey(*key,0)<=0)) {
			if (depth>0) {
				if (kpos==~0ul) break; next=pid;
				if (!parent.isNull()) {
					tp=(const TreePageMgr::TreePage*)parent->getPageBuf();
					if ((pid=tp->getPageID(--kpos))!=INVALID_PAGEID) {
						ushort l=sizeof(SearchKey)+tp->getKeyExtra(ushort(kpos+1));
						if (lkey<l) {key=(SearchKey*)(key==key0?malloc(l,SES_HEAP):realloc((void*)key,l,SES_HEAP)); lkey=l;}
						tp->getKey(ushort(kpos+1),*(SearchKey*)key); if ((xlock&PGCTL_ULOCK)!=0) xlock|=QMGR_UFORCE; continue;
					}
					assert(kpos==~0ul);
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
			pid=tp->info.sibling; if ((mode&TF_WITHDEL)!=0) xlock|=PGCTL_COUPLE; ++ctx->treeMgr->sideLink; continue;
		}
		if (!fUpd && tp->info.nSearchKeys==0 && (tp->info.level==0||tp->info.leftMost==INVALID_PAGEID)) break;
		if (tp->info.level!=0) {
			parent.release(); parent=pb; pb=NULL; next=INVALID_PAGEID; assert(!tp->isLeaf() && depth<TREE_MAX_DEPTH);		// if !fUpd???
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

ulong Tree::checkTree(StoreCtx *ctx,PageID root,CheckTreeReport& res,CheckTreeReport *sec)
{
	memset(&res,0,sizeof(CheckTreeReport));
	if (sec!=NULL) memset(sec,0,sizeof(CheckTreeReport));
	return root!=INVALID_PAGEID?ctx->trpgMgr->checkTree(root,INVALID_PAGEID,0,res,sec):0;
}

bool Tree::find(const SearchKey& key,void *buf,size_t &size)
{
	bool rc=false; TreeCtx tctx(*this);
	if (tctx.findPage(&key)==RC_OK) {
		ushort lData; assert(indexFormat().isUnique());
		const void *p=((const TreePageMgr::TreePage*)tctx.pb->getPageBuf())->getValue(key,lData);
		if (p!=NULL) {if (size+lData>0) memcpy(buf,p,min(ushort(size),lData)); size=lData; rc=true;}
	}
	return rc;
}

RC Tree::countValues(const SearchKey& key,uint64_t& nValues)
{
	TreeCtx tctx(*this); RC rc; nValues=0;
	return (rc=tctx.findPage(&key))==RC_OK?ctx->trpgMgr->count(tctx,key,nValues):rc;
}

RC Tree::insert(const SearchKey& key,const void *value,ushort lval,bool fMulti)
{
	TreeCtx tctx(*this); RC rc=tctx.findPageForUpdate(&key,true); PageID pid;
	return rc==RC_OK?insert(key,value,lval,fMulti,tctx,pid):rc;
}

RC Tree::insert(IMultiKey& mk)
{
	TreeCtx tctx(*this,&mk); RC rc; PageID pid; const SearchKey *pkey; const void *value; ushort lval; bool fMulti;
	while ((rc=mk.nextKey(pkey,value,lval,fMulti))==RC_OK) {
		const TreePageMgr::TreePage *tp; int cmp=-1;
		if (tctx.pb.isNull() || (cmp=(tp=(TreePageMgr::TreePage*)tctx.pb->getPageBuf())->cmpKey(*pkey))!=0) {
			if (cmp>0) {
				assert(tp->hasSibling()); ++ctx->treeMgr->sibRead;
				ulong xlock=tctx.pb.isSet(QMGR_UFORCE)?QMGR_UFORCE|PGCTL_COUPLE|PGCTL_ULOCK:PGCTL_COUPLE|PGCTL_ULOCK;
				if ((tctx.pb=ctx->bufMgr->getPage(tp->info.sibling,ctx->trpgMgr,xlock,tctx.pb))==NULL) cmp=-1;
				else {tctx.pb.set(QMGR_UFORCE); if (((TreePageMgr::TreePage*)tctx.pb->getPageBuf())->cmpKey(*pkey)>0) cmp=-1;}
			}
			if (cmp<0 && (tctx.depth=0,rc=tctx.findPageForUpdate(pkey,true))!=RC_OK) break;
		}
		if ((rc=insert(*pkey,value,lval,fMulti,tctx,pid))!=RC_OK) break;
	}
	return rc==RC_EOF?RC_OK:rc;
}

RC Tree::insert(const void *val,ushort lval,uint32_t& id,PageID& pid)
{
	TreeCtx tctx(*this); SearchKey key((uint32_t)~0u); pid=INVALID_PAGEID; id=~0u; assert((tctx.mode&TF_WITHDEL)==0);
	RC rc=tctx.findPageForUpdate(&key,true); if (rc!=RC_OK) return rc;
	const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage *)tctx.pb->getPageBuf(); assert(tp->info.fmt.isSeq());
	id=key.v.u32=uint32_t(tp->info.prefix>>32)+tp->info.nEntries; return insert(key,val,lval,false,tctx,pid);
}

RC Tree::insert(const SearchKey& key,const void *val,ushort lval,bool fMulti,TreeCtx& tctx,PageID& insPage)
{
	assert(!tctx.pb.isNull());
	RC rc=ctx->trpgMgr->insert(tctx,key,val,lval,fMulti);
	insPage=rc==RC_OK?tctx.pb->getPageID():INVALID_PAGEID; return rc;
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
	const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)tctx.pb->getPageBuf();
	const byte *pData; ushort lData;
	if (lOld>0 && tp->info.fmt.isUnique() && (pData=(const byte*)tp->getValue(key,lData))!=NULL 
		&& sht+lOld<=lData) memcpy(pOld,pData+sht,lOld);
	return ctx->trpgMgr->edit(tctx,key,newValue,lNewValue,lOld,sht);
}

RC Tree::remove(const SearchKey& key,const void *value,ushort lval,bool fMulti)
{
	TreeCtx tctx(*this); RC rc=tctx.findPageForUpdate(&key);
	return rc==RC_OK?ctx->trpgMgr->remove(tctx,key,value,lval,fMulti):rc;
}

RC Tree::drop(PageID pid,StoreCtx *ctx,TreeFreeData *dd)
{
	PageID stack[TREE_MAX_DEPTH]; int stkp=0; RC rc; SubAlloc pending(0);
	do {
		stack[stkp++]=pid; assert(pid!=INVALID_PAGEID);
		PBlockP pb(ctx->bufMgr->getPage(pid,ctx->trpgMgr)); if (pb.isNull()) return RC_NOTFOUND;
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

ulong Tree::getMode() const
{
	return TF_WITHDEL;
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
	level=-1;
	if (root==INVALID_PAGEID && !fRead) {
		PBlock *pb=ctx->fsMgr->getNewPage(ctx->trpgMgr);
		if (pb!=NULL) {
			if (ctx->trpgMgr->initPage(pb,indexFormat(),0,NULL,INVALID_PAGEID,INVALID_PAGEID)!=RC_OK) 
				pb->release();
			else 
				{root=pb->getPageID(); pb->release(); level=0;}
		}
	}
	return root;
}

PageID TreeStdRoot::prevStartPage(PageID)
{
	return INVALID_PAGEID;
}

RC TreeStdRoot::addRootPage(const SearchKey& key,PageID& pageID,ulong level)
{
	if (level<height) {
		// re-traverse from root etc.
		return RC_OK;
	}
	Session *ses=Session::getSession();
	if (ses==NULL) return RC_NOSESSION; if (!ses->inWriteTx()) return RC_READTX;
	PageID newp; PBlock *pb; RC rc;
	if ((rc=ctx->fsMgr->allocPages(1,&newp))!=RC_OK) return rc;
	assert(newp!=INVALID_PAGEID);
	if ((pb=ctx->bufMgr->newPage(newp,ctx->trpgMgr))==NULL) return RC_NORESOURCES;
	if ((rc=ctx->trpgMgr->initPage(pb,indexFormat(),ushort(level+1),&key,root,pageID))==RC_OK)
		{pageID=root=newp; height=level+1;}
	pb->release(); return rc;
}

RC TreeStdRoot::removeRootPage(PageID page,PageID leftmost,ulong level)
{
	if (level!=height || root!=page || leftmost==INVALID_PAGEID) return RC_FALSE;
	root=leftmost; height--; return RC_OK;
}

ulong TreeStdRoot::getStamp(TREE_NODETYPE nt) const
{
	return stamps[nt];
}

void TreeStdRoot::getStamps(ulong stmps[TREE_NODETYPE_ALL]) const
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

TreeGlobalRoot::TreeGlobalRoot(ulong idx,IndexFormat ifm,StoreCtx *ct,ulong md) 
	: TreeStdRoot(ct->theCB->getRoot(idx),ct),index(idx),ifmt(ifm),mode(md)
{
	ct->treeMgr->registerFactory(*this);
}


IndexFormat TreeGlobalRoot::indexFormat() const
{
	return ifmt;
}

ulong TreeGlobalRoot::getMode() const
{
	return mode;
}

PageID TreeGlobalRoot::startPage(const SearchKey *key,int& level,bool fRead,bool fBefore)
{
	level=-1;
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
	return root;
}

RC TreeGlobalRoot::addRootPage(const SearchKey& key,PageID& pageID,ulong level)
{
	RWLockP rw(&rootLock,RW_X_LOCK); PageID oldRoot=ctx->theCB->getRoot(index);
	RC rc=TreeStdRoot::addRootPage(key,pageID,level);
	return rc==RC_OK && oldRoot!=root ? setRoot(root,oldRoot) : rc;
}

RC TreeGlobalRoot::removeRootPage(PageID page,PageID leftmost,ulong level)
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
	RWLockP rwl(&rwlock,RW_X_LOCK),rootl(&rootLock,RW_X_LOCK); RC rc=RC_OK;
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
	sizeof(uint32_t),sizeof(uint64_t),sizeof(int32_t),sizeof(int64_t),sizeof(float),sizeof(double),0,0,0
};

RC SearchKey::deserialize(const void *buf,size_t l,size_t *pl)
{
	const byte *p=(const byte *)buf; ushort ll=0,dl=1;
	if (l<1 || (type=(TREE_KT)*p)>KT_ALL) return RC_CORRUPTED;
	if (type<KT_BIN) {
		if (ulong(ll=extKeyLen[type])+1>l) return RC_CORRUPTED;
		memcpy(&v,p+1,ll); loc=PLC_EMB;
	} else if (type<KT_ALL) {
		if (l<2) return RC_CORRUPTED; dl++;
		if (((ll=p[1])&0x80)!=0) {if (l<3) return RC_CORRUPTED; ll=ll&~0x80|p[2]<<7; dl++;}
		if (l<ulong(ll+dl)) return RC_CORRUPTED;
		v.ptr.p=p+dl; v.ptr.l=ll; loc=PLC_SPTR;	// copy???
	}
	if (pl!=NULL) *pl=ll+dl; return RC_OK;
}

int IndexKeyV::cmp(const IndexKeyV& rhs,TREE_KT type) const
{
	int cmp;
	switch (type) {
	default: break;
	case KT_UINT: return cmp3(u32,rhs.u32);
	case KT_UINT64: return cmp3(u64,rhs.u64);
	case KT_INT: return cmp3(i32,rhs.i32);
	case KT_INT64: return cmp3(i64,rhs.i64);
	case KT_FLOAT: return cmp3(f,rhs.f);
	case KT_DOUBLE: return cmp3(d,rhs.d);
	case KT_BIN: return (cmp=memcmp(ptr.p,rhs.ptr.p,min(ptr.l,rhs.ptr.l)))!=0?cmp:cmp3(ptr.l,rhs.ptr.l);
	case KT_REF: return PINRef::cmpPIDs((byte*)ptr.p,ptr.l,(byte*)rhs.ptr.p,rhs.ptr.l);
	case KT_MSEG: return cmpMSeg((byte*)ptr.p,ptr.l,(byte*)rhs.ptr.p,rhs.ptr.l);
	}
	return -2;
}

RC SearchKey::toKey(const Value **ppv,ulong nv,const IndexSeg *kds,int idx,Session *ses,MemAlloc *ma)
{
	RC rc=RC_OK; byte *buf=NULL,*p; bool fDel=false; ulong xbuf=512,lkey=0; if (ma==NULL) ma=ses;
	for (ulong ii=0; ii<nv; ii++) {
		const IndexSeg& ks=kds[ii]; const Value *pv=ppv[ii]; bool fExcl=false;
		if (pv!=NULL) {
			if (pv->type==VT_ERROR) pv=NULL;
			else if (pv->type==VT_RANGE) {
				if (idx==-1) {rc=RC_TYPE; break;} assert(idx==0||idx==1); 
				pv=&pv->varray[idx]; if (pv->type==VT_ANY) pv=NULL;
			} else switch (ks.op) {
			default: assert(0);
			case OP_EQ: case OP_BEGINS: case OP_IN: break;
			case OP_LT: fExcl=true;
			case OP_LE: if (idx==0) pv=NULL; break;
			case OP_GT: fExcl=true;
			case OP_GE: if (idx==1) pv=NULL; break;
			}
		}
		if (pv==NULL) {
			if (nv!=1) {
				if (buf==NULL && (buf=(byte*)alloca(xbuf))==NULL || lkey+1>=xbuf) {
					if (buf!=NULL && !fDel) {
						if ((p=(byte*)ma->malloc(xbuf*=2))==NULL) {rc=RC_NORESOURCES; break;}
						memcpy(p,buf,lkey); buf=p; fDel=true;
					} else if ((buf=(byte*)ma->realloc(buf,buf==NULL?xbuf:xbuf*=2))==NULL) {rc=RC_NORESOURCES; break;}
				}
				buf[lkey++]=idx==0||idx==-1&&(ks.flags&ORD_NULLS_AFTER)==0?0:byte(~KT_ALL);
			} else if ((pv=ppv[ii])==NULL || pv->type==VT_ERROR || pv->type==VT_RANGE && pv->varray[0].type==VT_ERROR && pv->varray[1].type==VT_ERROR)
				{rc=RC_TYPE; break;}		//??? KT_MSEG with 1 seg?
			else {type=KT_ALL; loc=PLC_EMB;}
		} else {
			loc=PLC_EMB;
			TIMESTAMP ts; Value w; byte kbuf[XPINREFSIZE]; PID id; bool fFree=false;
			PropertyID pid=STORE_INVALID_PROPID; ElementID eid=STORE_COLLECTION_ID;
			if (ks.type!=VT_ANY && pv->type!=ks.type) {
				if ((rc=convV(*pv,w,(ValueType)ks.type))!=RC_OK) break;
				pv=&w;
			}
			switch (pv->type) {
			default: rc=RC_TYPE; break;		// NULL segment in MSEG?
			case VT_STREAM:
				//???
			case VT_STRING: case VT_BSTR: case VT_URL:
				v.ptr.l=(ushort)pv->length;
				if (ks.lPrefix!=0 && v.ptr.l>ks.lPrefix) v.ptr.l=ks.lPrefix;	// convert to VT_STRING?
				v.ptr.p=pv->bstr; type=KT_BIN; break;
			//case VT_ENUM:
			case VT_INT: v.i32=pv->i; type=KT_INT; break;
			case VT_UINT: case VT_URIID: case VT_IDENTITY: v.u32=pv->ui; type=KT_UINT; break;
			case VT_INT64: case VT_INTERVAL: v.i64=pv->i64; type=KT_INT64; break;
			case VT_UINT64: case VT_DATETIME: v.u64=pv->ui64; type=KT_UINT64; break;
			//case VT_DECIMAL:
			case VT_FLOAT: v.f=pv->f; type=KT_FLOAT; break;
			case VT_DOUBLE: v.d=pv->d; type=KT_DOUBLE; break;
			case VT_BOOL: v.u32=pv->b?1:0; type=KT_UINT; break;
			case VT_REF: id=pv->pin->getPID(); goto encode;
			case VT_REFID: id=pv->id; goto encode;
			case VT_REFPROP:	id=pv->ref.pin->getPID(); pid=pv->ref.pid; goto encode;
			case VT_REFIDPROP: id=pv->refId->id; pid=pv->refId->pid; goto encode;
			case VT_REFELT:	id=pv->ref.pin->getPID(); pid=pv->ref.pid; eid=pv->ref.eid; goto encode;
			case VT_REFIDELT: id=pv->refId->id; pid=pv->refId->pid; eid=pv->refId->eid;
			encode:
				{PINRef pr(ses->getStore()->storeID,id);
				if (pv->type>=VT_REFPROP) {
					pr.def|=PR_U1; pr.u1=pid;
					if (pv->type>=VT_REFELT) {pr.def|=PR_U2; pr.u2=eid;}
				}
				v.ptr.p=kbuf; v.ptr.l=pr.enc(kbuf); type=KT_REF; break;}
			case VT_CURRENT:
				switch (pv->ui) {
				case CVT_TIMESTAMP: getTimestamp(ts); v.u64=ts; type=KT_UINT64; break;
				case CVT_USER: v.u32=ses->getIdentity(); type=KT_UINT; break;
				case CVT_STORE: v.u32=ses->getStore()->storeID; type=KT_UINT; break;
				}
				break;
			}
			if (rc!=RC_OK) break;
			if (nv>1) {
				bool fDesc=(ks.flags&ORD_DESC)!=0,fBeg; uint32_t l,l0=fDesc?2:1;
				if (type!=KT_BIN && type!=KT_REF) l=extKeyLen[type];
				else {
					l=v.ptr.l; fBeg=ks.op==OP_BEGINS&&idx>=0;
					if ((ks.flags&ORD_NCASE)!=0 && pv->type==VT_STRING) {
						if ((v.ptr.p=forceCaseUTF8((const char*)v.ptr.p,l,l,ma))==NULL) {rc=RC_NORESOURCES; break;}
						assert((l&0xFFFF0000)==0); fFree=true;
					}
					if (!fDesc && (fBeg || l>=byte(~KT_ALL) || type==KT_REF)) l0+=l<=0x1F?1:l<=0xFFF?2:3;
				}
				if (buf==NULL && (buf=(byte*)alloca(xbuf))==NULL || lkey+l0+l>=xbuf) {
					if (buf!=NULL && !fDel) {
						if ((p=(byte*)ma->malloc(xbuf*=2))==NULL) {rc=RC_NORESOURCES; break;}
						memcpy(p,buf,lkey); buf=p; fDel=true;
					} else if ((buf=(byte*)ma->realloc(buf,buf==NULL?xbuf:xbuf*=2))==NULL) {rc=RC_NORESOURCES; break;}
				}
				if (type==KT_BIN||type==KT_REF) {
					if (l0==1) buf[lkey++]=byte(l);
					else {
						buf[lkey++]=byte(~type); buf[lkey++]=byte((fDesc?0x80:0)|(fBeg?0x40:0)|(l<0x1F?l:l&0x1F|0x20));
						if (l0>2) {buf[lkey++]=byte(l>>5); if (l0>3) {buf[lkey-1]|=0x80; buf[lkey++]=byte(l>>12);}}
					}
					memcpy(buf+lkey,v.ptr.p,l); if (fFree) ma->free((void*)v.ptr.p);
				} else {
					assert(type!=KT_MSEG);
					if (!fDesc) buf[lkey++]=byte(~type);
					else {buf[lkey]=byte(~KT_MSEG); buf[lkey+1]=type; lkey+=2;}
					memcpy(buf+lkey,&v,l);
				}
				lkey+=l;
			} else if ((type==KT_BIN||type==KT_REF) && v.ptr.p!=NULL) {
				const void *p=v.ptr.p;
				if ((ks.flags&ORD_NCASE)!=0 && pv->type==VT_STRING) {
					uint32_t len=0;
					if ((v.ptr.p=forceCaseUTF8((const char*)p,v.ptr.l,len,ma))==NULL) {rc=RC_NORESOURCES; break;}
					assert((len&0xFFFF0000)==0); v.ptr.l=uint16_t(len);
				} else if ((v.ptr.p=ma->malloc(v.ptr.l))==NULL) {rc=RC_NORESOURCES; break;}
				else memcpy((void*)v.ptr.p,p,v.ptr.l);
				loc=PLC_ALLC;
			}
			if (pv==&w) freeV(w);
		}
	}
	if (rc!=RC_OK) {type=KT_ALL; loc=PLC_EMB; if (fDel) ma->free(buf);}
	else if (buf!=NULL) {
		if (fDel) v.ptr.p=xbuf>=lkey*2?(byte*)ma->realloc(buf,lkey):buf;
		else if ((p=(byte*)ma->malloc(lkey))==NULL) {type=KT_ALL; loc=PLC_EMB; return RC_NORESOURCES;}
		else {v.ptr.p=p; memcpy(p,buf,lkey);}
		type=KT_MSEG; loc=PLC_ALLC; v.ptr.l=uint16_t(lkey);
	}
	return rc;
}

__forceinline int decode(const byte *&s,ushort& l,byte& ty,byte& mod,void *buf,ushort& ls)
{
	if (l--==0) throw 0; mod=0; ls=0; if ((ty=*s++)==0) return -1;
	if (ty<byte(~KT_ALL)||ty==byte(~KT_BIN)||ty==byte(~KT_REF)) {
		if (ty<byte(~KT_ALL)) {ls=ty; ty=KT_BIN;}
		else if (l--==0) throw 1;
		else {
			mod=*s++; ls=mod&0x1F; ty=byte(~ty);
			if ((mod&0x20)!=0) {
				if (l--==0) throw 2;
				if (((ls|=*s++<<5)&0x1000)!=0) {if (l--!=0) ls=(ls&~0x1000)|*s++<<12; else throw 3;}
			}
		}
		if (l<ls) throw 4;
	} else if (ty==byte(~KT_ALL)) return 1;
	else {
		if (ty==byte(~KT_MSEG)) {if (l--!=0) {ty=*s++; mod=0x80; assert(ty<KT_ALL);} else throw 5;}
		else {assert(ty>byte(~KT_BIN)); ty=byte(~ty);}
		ls=SearchKey::extKeyLen[ty]; if (l<ls) throw 6; memcpy(buf,s,ls);
	}
	return 0;
}

int cmpSeg(const byte *&s1,ushort& l1,const byte *&s2,ushort& l2)
{
	byte ty1,mod1,ty2,mod2; ushort ls1,ls2;
	union {uint32_t ui; uint64_t ui64; int32_t i; int64_t i64; float f; double d;} v1,v2;
	int r1=decode(s1,l1,ty1,mod1,&v1,ls1),r2=decode(s2,l2,ty2,mod2,&v2,ls2),cmp=cmp3(r1,r2);
	if (cmp==0 && r1==0) {
		if (ty1!=ty2) {
			// convert
			throw -1000;
		}
		switch (ty1) {
		default: break;
		case KT_UINT: cmp=cmp3(v1.ui,v2.ui); break;
		case KT_UINT64: cmp=cmp3(v1.ui64,v2.ui64); break;
		case KT_INT: cmp=cmp3(v1.i,v2.i); break;
		case KT_INT64: cmp=cmp3(v1.i64,v2.i64); break;
		case KT_FLOAT: cmp=cmp3(v1.f,v2.f); break;
		case KT_DOUBLE: cmp=cmp3(v1.d,v2.d); break;
		case KT_REF: cmp=PINRef::cmpPIDs(s1,ls1,s2,ls2); break;
		case KT_BIN: if ((cmp=memcmp(s1,s2,min(ls1,ls2)))==0) cmp=ls1<ls2?(mod1&0x40)!=0?0:-1:ls1==ls2?0:(mod2&0x40)!=0?0:1; break;
		}
		if (((mod1|mod2)&0x80)!=0) cmp=-cmp;
	}
	s1+=ls1; l1-=ls1; s2+=ls2; l2-=ls2; return cmp;
}

int MVStoreKernel::cmpMSeg(const byte *s1,ushort l1,const byte *s2,ushort l2)
{
	try {
		do {int cmp=cmpSeg(s1,l1,s2,l2); if (cmp!=0) return cmp;} while (l1*l2!=0);
		if ((l1|l2)==0) return 0;
	} catch (int) {
		// report
	}
	return -2;
}

bool MVStoreKernel::isHyperRect(const byte *s1,ushort l1,const byte *s2,ushort l2)
{
	try {
		do {int cmp=cmpSeg(s1,l1,s2,l2); if (cmp!=0) return (l1|l2)!=0;} while (l1*l2!=0);
	} catch (int) {
		// report
	}
	return false;
}

bool MVStoreKernel::checkHyperRect(const byte *s1,ushort l1,const byte *s2,ushort l2)
{
	try {
		do {int cmp=cmpSeg(s1,l1,s2,l2); if (cmp>0) return false;} while (l1*l2!=0);
		return l1==0;
	} catch (int) {
		// report
	}
	return false;
}

ushort MVStoreKernel::calcMSegPrefix(const byte *s1,ushort l1,const byte *s2,ushort l2)
{
	for (ushort lPrefix=0,l,l0;;) {
		byte b1=*s1++,b2=*s2++; if (b1!=b2) return lPrefix;
		--l1,--l2; l0=1;
		if (b1==0||b1==byte(~KT_ALL)) l=0;
		else {
			if (b1<byte(~KT_ALL)||b1==byte(~KT_BIN)) {
				if (b1<byte(~KT_ALL)) l=b1;
				else {
					if (l1--==0||l2--==0) return lPrefix;
					if ((((b1=*s1++)^*s2++)&~0x40)!=0) return lPrefix;
					l=b1&0x1F; l0++;
					if ((b1&0x20)!=0) {
						if (l1--==0||l2--==0||(b1=*s1++)!=*s2++) return lPrefix;
						l|=b1<<5; l0++;
						if ((l&0x1000)!=0) {
							if (l1--==0||l2--==0||(b1=*s1++)!=*s2++) return lPrefix;
							l=(l&~0x1000)|b1<<12; l0++;
						}
					}
				}
			} else {
				assert(b1==byte(~KT_MSEG)||b1>byte(~KT_BIN));
				if (b1!=byte(~KT_MSEG)) b1=byte(~b1);
				else if (l1--==0||l2--==0||(b1=*s1++)!=*s2++) return lPrefix;
				l=SearchKey::extKeyLen[b1];
			}
			if (l1<l||l2<l||memcmp(s1,s2,l)) return lPrefix;
		}
		assert(l>=l && l2>=l); 
		s1+=l; l1-=l; s2+=l; l2-=l; lPrefix+=l+l0;
		if (l1*l2==0) return lPrefix;
	}
}

RC SearchKey::getValues(Value *vals,unsigned nv,const IndexSeg *kd,unsigned nFields,Session *ses,bool fFilter,MemAlloc *ma) const
{
	RC rc=RC_OK; RefVID r,*pr; void *p; if (ma==NULL) ma=ses;
	if (type==KT_MSEG) {
		const byte *s=(byte*)getPtr2(); unsigned l=v.ptr.l; Value *pv;
		for (unsigned i=0; i<nFields; i++,kd++) {
			if (l==0) return RC_CORRUPTED;
			byte b=*s++; --l; ushort ls=0;
			if (b!=0 && b!=byte(~KT_ALL)) {
				if (b<byte(~KT_ALL)||b==byte(~KT_BIN)||b==byte(~KT_REF)) {
					if (b<byte(~KT_ALL)) {ls=b; b=KT_BIN;}
					else if (l--==0) return RC_CORRUPTED; 
					else {
						byte b2=*s++; ls=b2&0x1F; b=byte(~b);
						if ((b2&0x20)!=0) {
							if (l--==0) return RC_CORRUPTED;
							if (((ls|=*s++<<5)&0x1000)!=0) {if (l--!=0) ls=(ls&~0x1000)|*s++<<12; else return RC_CORRUPTED;}
						}
					}
				} else {
					assert(b==byte(~KT_MSEG)||b>byte(~KT_BIN));
					if (b!=byte(~KT_MSEG)) b=byte(~b);
					else if (l--!=0) b=*s++; else return RC_CORRUPTED;
					ls=SearchKey::extKeyLen[b];
				}
				if (l<ls) return RC_CORRUPTED;
				if (fFilter) pv=(Value*)BIN<Value,PropertyID,ValCmp>::find(kd->propID,vals,nv); else if (i>=nv) break; else pv=&vals[i];
				if (pv!=NULL && pv->property==kd->propID) {
					pv->type=kd->type; pv->flags=NO_HEAP; pv->meta=0; pv->eid=STORE_COLLECTION_ID; pv->op=OP_SET; pv->length=ls;
					if (b<KT_BIN) memcpy(&pv->i,s,ls);
					else if (b==KT_REF) {
						if (ls==0) return RC_CORRUPTED;
						try {
							PINRef pr(ses->getStore()->storeID,s,ls);
							if (kd->type==VT_REFIDELT) r.eid=(pr.def&PR_U2)!=0?pr.u2:STORE_COLLECTION_ID;
							if (kd->type==VT_REFIDELT||kd->type==VT_REFIDPROP) r.eid=(pr.def&PR_U1)!=0?pr.u1:STORE_INVALID_PROPID;
							r.id=pr.id;
						} catch (RC &rc) {return rc;}
						if (kd->type==VT_REFID) pv->set(r.id);
						else if ((pr=(RefVID*)ma->malloc(sizeof(RefVID)))==NULL) return RC_NORESOURCES;
						else {r.vid=STORE_CURRENT_VERSION; *pr=r; pv->set(*pr); pv->flags=ma->getAType();}
						pv->property=kd->propID;
					} else if (kd->type==VT_STRING || kd->type==VT_URL) {
						if ((p=ma->malloc(ls+1))==NULL) return RC_NORESOURCES;
						memcpy(p,s,ls); ((char*)p)[ls]='\0'; pv->str=(char*)p; pv->flags=ma->getAType();
					} else {
						if ((p=ma->malloc(ls))==NULL) return RC_NORESOURCES;
						memcpy(p,s,ls); pv->bstr=(unsigned char*)p; pv->flags=ma->getAType();
					}
				}
			}
			assert(l>=ls); s+=ls; l-=ls;
		}
	} else {
		if (fFilter && nv>1) vals=(Value*)BIN<Value,PropertyID,ValCmp>::find(kd->propID,vals,nv);
		else if (vals->property!=kd->propID) {if (vals->property==PROP_SPEC_ANY) vals->property=kd->propID; else vals=NULL;}
		if (vals!=NULL) {
			vals->flags=NO_HEAP;
			switch (type) {
			case KT_INT: vals->set((int)v.i32); break;
			case KT_UINT: vals->set((unsigned int)v.u32); break;
			case KT_UINT64: if (kd->type==VT_DATETIME) vals->setDateTime(v.u64); else vals->setU64(v.u64); break;
			case KT_INT64: if (kd->type==VT_INTERVAL) vals->setInterval(v.i64); else vals->setI64(v.i64); break;
			case KT_FLOAT: vals->set(v.f); break; 
			case KT_DOUBLE: vals->set(v.d); break;
			case KT_MSEG: return RC_TYPE;
			case KT_REF:
				if (v.ptr.l==0) return RC_CORRUPTED;
				try {
					PINRef pr(ses->getStore()->storeID,(byte*)getPtr2(),v.ptr.l);
					if (kd->type==VT_REFIDELT) r.eid=(pr.def&PR_U2)!=0?pr.u2:STORE_COLLECTION_ID;
					if (kd->type==VT_REFIDELT||kd->type==VT_REFIDPROP) r.eid=(pr.def&PR_U1)!=0?pr.u1:STORE_INVALID_PROPID;
					r.id=pr.id;
				} catch (RC &rc) {return rc;}
				if (kd->type==VT_REFID) vals->set(r.id);
				else if ((pr=(RefVID*)ma->malloc(sizeof(RefVID)))==NULL) return RC_NORESOURCES;
				else {r.vid=STORE_CURRENT_VERSION; *pr=r; vals->set(*pr); vals->flags=ma->getAType();}
				break;
			case KT_BIN:
				switch (kd->type) {
				default: case VT_BSTR:
					if ((p=ma->malloc(v.ptr.l))==NULL) return RC_NORESOURCES;
					memcpy(p,getPtr2(),v.ptr.l); vals->set((unsigned char*)p,v.ptr.l); vals->flags=ma->getAType(); break;
				case VT_STRING: case VT_URL:
					if ((p=ma->malloc(v.ptr.l+1))==NULL) return RC_NORESOURCES;
					memcpy(p,getPtr2(),v.ptr.l); ((char*)p)[v.ptr.l]='\0'; vals->set((char*)p,v.ptr.l); vals->flags=ma->getAType(); break;	
				}
				break;
			default: return RC_CORRUPTED;
			}
			vals->property=kd->propID;
		}
	}
	return rc;
}
