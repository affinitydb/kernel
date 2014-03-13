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

Written by Mark Venguerov 2004-2012

**************************************************************************************/

/**
 * B-tree index page structure
 * index tree page manipulation routines
 */
#ifndef _PGTREE_H_
#define _PGTREE_H_

#include "pagemgr.h"
#include "idxtree.h"
#include "buffer.h"
#include "pinref.h"

namespace AfyKernel
{

#define	XSUBPAGES		20

/**
 * index tree operation format
 */
#define	TRO_SHIFT		4
#define	TRO_MASK		0x000F
#define	TRO_MANY		0x8000

/**
 * index tree operations
 */
enum TREE_OP
{
	TRO_INSERT,			/**< insert new key */
	TRO_UPDATE,			/**< update existing key (including insertion of new data for existing key) */
	TRO_DELETE,			/**< delete key or data element */
	TRO_EDIT,			/**< edit data */
	TRO_INIT,			/**< initilize page */
	TRO_SPLIT,			/**< split page */
	TRO_MERGE,			/**< merge 2 pages */
	TRO_SPAWN,			/**< spawn secondary sub-tree */
	TRO_ABSORB,			/**< absorb secondary sub-tree */
	TRO_ADDROOT,		/**< add new root page (increases tree height) */
	TRO_DELROOT,		/**< delete root page (decreases tree height) */
	TRO_REPLACE,		/**< replace data element with new one */
	TRO_DROP,			/**< drop page, e.g. when index is being deleted */
	TRO_MULTI,			/**< multielement operation, idx - type of operation */
	TRO_COUNTER,		/**< update for SubTreePage::counter */
	TRO_APPEND,			/**< append multiple keys with data to one page */
	TRO_ALL
};

/**
 * multiielement operations (see TRO_MULTI)
 */
enum MULTI_OP
{
	MO_INSERT, MO_DELETE, MO_INIT, MO_PAGEINIT
};

#define	SPAWN_THR			0.8		/**< spawn threshold */
#define	SPAWN_N_THR			4		/**< spawn number of keys threshold */

#define	PTX_NOPRNTREL		0x0001
#define	PTX_NOLEAFREL		0x0002
#define	PTX_RELEASED		0x0004

#define	PTX_FORUPDATE		0x8000
#define	PTX_FORDELETE		0x4000
#define	PTX_TRY				0x2000

#define	L_SHT				sizeof(uint16_t)

/**
 * index tree operation context
 */
struct TreeCtx
{
	Tree					*tree;
	PBlockP					pb;
	PBlockP					parent;
	unsigned				depth;
	unsigned				leaf;
	unsigned				index;
	unsigned				lvl2;
	const SearchKey			*mainKey;
	PagePtr					vp;
	PageID					newPage;
	IMultiKey				*moreKeys;
	TreeConnect				*tcon;
	uint32_t				thndl;
	unsigned				stamps[TREE_NODETYPE_ALL];
	PageID					stack[TREE_MAX_DEPTH];
	TreeCtx(Tree& tr,IMultiKey *mk=NULL) : tree(&tr),depth(0),leaf(~0u),index(0),lvl2(~0u),mainKey(NULL),newPage(INVALID_PAGEID),moreKeys(mk),tcon(NULL),thndl(0) {memset(stamps,0,sizeof(stamps));}
	TreeCtx(const TreeCtx &ctx) : tree(ctx.tree),depth(ctx.depth),leaf(ctx.leaf),index(ctx.index),lvl2(ctx.lvl2),mainKey(NULL),newPage(ctx.newPage),moreKeys(NULL),tcon(NULL),thndl(0)
				{if ((tcon=tree->persist(thndl))!=NULL) tree=NULL; memcpy(stamps,ctx.stamps,sizeof(stamps)); if (ctx.depth!=0) memcpy(stack,ctx.stack,ctx.depth*sizeof(PageID));}
	RC						findPage(const SearchKey *);
	RC						findPrevPage(const SearchKey *,bool fUpd=false);
	RC						findPageForUpdate(const SearchKey *,bool fIns=false);
	RC						getParentPage(const SearchKey&,unsigned);
	RC						getPreviousPage(bool fRead=true);
	PageID					startPage(const SearchKey*,int& level,bool=true,bool=false);
	PageID					prevStartPage(PageID pid);
	void					getStamps(unsigned stamps[TREE_NODETYPE_ALL]) const;
	RC						postPageOp(const SearchKey& key,PageID pageID,bool fDel=false) const;
};

/**
 * Subtree initialization with sorted data
 */
class SubTreeInit
{
protected:
	MemAlloc	*const ma;
	byte		*buf;
	uint16_t	lbuf;
	uint16_t	freeSpace;
	uint16_t	freeSpaceLength;
	uint16_t	nKeys;
	unsigned	depth;
	PBlock		*stack[TREE_MAX_DEPTH];
public:
	SubTreeInit(MemAlloc *m) : ma(m),buf(NULL),lbuf(0),freeSpace(0),freeSpaceLength(0),nKeys(0),depth(0) {}
	~SubTreeInit();
	RC			insert(Session *ses,const byte *key);
	RC			flush(Session *ses);
	friend	class Tree;
};

/**
 * free data callback interface
 * used when a leaf tree page is dropped
 */
class TreeFreeData
{
public:
	virtual	RC	freeData(const byte *data,StoreCtx*) = 0;
};

/**
 * tree page manager
 * extends TxPage
 * defines tree page format and manipulation routines
 */
class TreePageMgr : public TxPage
{
	const	size_t	xSize;
public:
	struct VarKey {
		PagePtr		ptr;
		PageID		pageID;
	};
	/**
	 * tree page header descriptor
	 */
	struct TreePageInfo {
		IndexFormat	fmt;
		uint16_t	nEntries;
		uint8_t		level;
		uint8_t		flags;
		uint16_t	freeSpace;
		uint16_t	freeSpaceLength;
		uint16_t	scatteredFreeSpace;
		uint16_t	lPrefix;
		uint64_t	prefix;
		PageID		sibling;
		PageID		leftMost;
		uint32_t	stamp;
		uint16_t	nSearchKeys;
		uint16_t	filler;

		void		initFree(size_t lPage) {freeSpace=uint16_t(lPage-FOOTERSIZE); freeSpaceLength=uint16_t(lPage-sizeof(TreePage)-FOOTERSIZE); scatteredFreeSpace=lPrefix=0;}
		uint16_t	keyLength() const {assert(!fmt.isKeyOnly()); return fmt.isSeq()?0:!fmt.isFixedLenKey()?sizeof(PagePtr):(uint16_t)ceil(fmt.keyLength()-lPrefix,sizeof(uint16_t));}
		uint16_t	calcEltSize() const {
			return fmt.isKeyOnly()?L_SHT:fmt.isSeq()?fmt.isFixedLenData()?fmt.dataLength():sizeof(PagePtr):
				!fmt.isFixedLenKey()?sizeof(PagePtr)+uint16_t(fmt.isFixedLenData()?ceil(fmt.dataLength(),sizeof(uint16_t)):sizeof(PagePtr)):fmt.isFixedLenData()?fmt.keyLength()-lPrefix:
				!fmt.isNumKey()?(uint16_t)ceil(fmt.keyLength()-lPrefix,sizeof(uint16_t))+sizeof(PagePtr):fmt.keyLength()==lPrefix?sizeof(PagePtr):
				(uint16_t)ceil(fmt.keyLength()-lPrefix+sizeof(PagePtr),fmt.keyLength()-lPrefix);
		}
		uint16_t	calcEltSize(uint16_t prefixSize) const {
			return fmt.isKeyOnly()?L_SHT:fmt.isSeq()?fmt.isFixedLenData()?fmt.dataLength():sizeof(PagePtr):
				!fmt.isFixedLenKey()?sizeof(PagePtr)+uint16_t(fmt.isFixedLenData()?ceil(fmt.dataLength(),sizeof(uint16_t)):sizeof(PagePtr)):fmt.isFixedLenData()?fmt.keyLength()-prefixSize:
				!fmt.isNumKey()?(uint16_t)ceil(fmt.keyLength()-prefixSize,sizeof(uint16_t))+sizeof(PagePtr):fmt.keyLength()==prefixSize?sizeof(PagePtr):
				(uint16_t)ceil(fmt.keyLength()-prefixSize+sizeof(PagePtr),fmt.keyLength()-prefixSize);
		}
		uint16_t	extraEltSize(uint16_t newPrefLen) const {
			assert(newPrefLen<lPrefix && lPrefix!=uint16_t(~0u));
			return !fmt.isFixedLenKey()||fmt.isFixedLenData()?lPrefix-newPrefLen:
				!fmt.isNumKey()?uint16_t(ceil(fmt.keyLength()-newPrefLen,sizeof(uint16_t))-ceil(fmt.keyLength()-lPrefix,sizeof(uint16_t))):
				uint16_t(ceil(fmt.keyLength()-newPrefLen+sizeof(PagePtr),fmt.keyLength()-newPrefLen)-ceil(fmt.keyLength()-lPrefix+sizeof(PagePtr),fmt.keyLength()-lPrefix));
		}
		uint16_t	calcVarSize(const SearchKey& key,uint16_t lData,uint16_t prefLen) const {
			assert(!fmt.isSeq()); return fmt.isFixedLenKey()?lData:fmt.isFixedLenData()?key.v.ptr.l-prefLen:key.v.ptr.l-prefLen+lData;
		}
	};
	/**
	 * tree page modification information
	 */
	struct TreePageModify {
		PagePtr		newData;
		PagePtr		oldData;
		uint16_t	newPrefSize;
	};
	/**
	 * tree page append information
	 */
	struct TreePageAppend {
		uint16_t	nKeys;
		uint16_t	newPrefixSize;
	};
	struct TreePageAppendElt {
		uint16_t	lkey;
		uint16_t	ldata;
		uint16_t	ndata;
		uint16_t	idx;
	};
	/**
	 * tree page initializatin information
	 */
	struct TreePageInit {
		IndexFormat		fmt;
		uint32_t		level;
		PageID			left;
		PageID			right;
	};
	/**
	 * tree page modification - multiple data elements
	 */
	struct TreePageMulti {
		IndexFormat		fmt;
		PageID			sibling;
		uint16_t		lData;
		uint16_t		nKeys	:15;
		uint16_t		fLastR	:1;
	};
	/**
	 * tree page split information
	 */
	struct TreePageSplit {
		PageID			newSibling;
		uint16_t		nEntriesLeft;
		uint16_t		oldPrefSize;
	};
	struct TreePageEdit {
		PagePtr		newData;
		PagePtr		oldData;
		uint32_t	shift;
	};
	struct TreePageSpawn {
		PageID		root;
		IndexFormat	fmt;
	};
	struct TreePageRoot {
		PageID		pageID;
		PagePtr		pkey;
	};
	struct SubTreePageKey {
		PageID		pageID;
		PagePtr		key;
	};
	struct SubTreePage {
		uint16_t	fSubPage;
		uint16_t	level;
		PageID		anchor;
		uint64_t	counter;
		PageID		leftMost;
		uint32_t	filler;
	};
	/**
	 * structure of index tree page
	 */
	struct TreePage {
		TxPageHeader	hdr;
		TreePageInfo	info;
		void store(const void *data,uint16_t len,PagePtr& ptr) {
			uint16_t l=len&~TRO_MANY; assert(info.freeSpaceLength>=l && !info.fmt.isKeyOnly()); 
			ptr.offset=info.freeSpace-=l; ptr.len=len; if (l!=0) {memcpy((byte*)this+info.freeSpace,data,l); info.freeSpaceLength-=l;}
		}
		uint16_t store(const void *data,uint16_t len) {
			assert(info.freeSpaceLength>=len && info.fmt.isKeyOnly() && len!=0); 
			info.freeSpace-=len; memcpy((byte*)this+info.freeSpace,data,len);
			info.freeSpaceLength-=len; return info.freeSpace-sizeof(TreePage);
		}
		template<typename T> bool findNumKey(T key,unsigned,unsigned& pos) const;
		template<typename T> bool findNumKeyVar(T key,unsigned,unsigned& pos) const;
		void		compact(bool fCheckOnly=false,unsigned extIdx=~0u,uint16_t lext=0);
		void		changePrefixSize(uint16_t prefLen);
		void		copyEntries(TreePage *,uint16_t prefLen,uint16_t start) const;
		uint16_t	calcPrefixSize(const SearchKey &key,unsigned idx,bool fExt=false,const SearchKey *pk=NULL) const;
		uint16_t	calcPrefixSize(unsigned start,unsigned end) const;
		void		storeKey(const void *key,void *ptr);
		RC			prefixFromKey(const void *key,uint16_t prefSize=uint16_t(~0u));
		void		moveMulti(const TreePage *src,const PagePtr& from,PagePtr& to);
		bool		hasSibling() const {return info.sibling!=INVALID_PAGEID;}
		bool		isLeaf() const {return info.level==0;}
		bool		checkPage(bool fWrite) const;
		PageID		getChild(const SearchKey& key,unsigned& pos,bool fBefore) const {
			assert(!isLeaf() && info.fmt.isFixedLenData() && info.fmt.dataLength()==sizeof(PageID));
			if (info.nSearchKeys==0) {pos=~0u; return info.leftMost;}
			if (!findKey(key,pos)||fBefore) --pos; return getPageID(pos);
		}
		PageID		getPageID(unsigned idx) const {
			assert(!isLeaf()&&(idx<info.nSearchKeys||idx==~0u&&info.leftMost!=INVALID_PAGEID)); 
			return idx==~0u?info.leftMost : !info.fmt.isFixedLenKey() ? ((VarKey*)(this+1))[idx].pageID :
										((PageID*)((byte*)this+info.freeSpace))[info.nSearchKeys-idx-1];
		}
		const void	*getValue(const SearchKey& key,uint16_t& lData) const {
			unsigned pos; const PagePtr *pp; const void *p; assert(isLeaf() && info.fmt.keyType()==key.type);
			return findKey(key,pos)&&(p=findData(pos,lData,&pp))!=NULL&&(pp==NULL||(pp->len&TRO_MANY)==0)?p:NULL;
		}
		uint32_t	getKeyU32(uint16_t idx) const;
		uint16_t	length(const PagePtr& ptr) const;
		byte		*findValue(const void *val,uint16_t lv,const PagePtr& vp,uint16_t *pins=NULL) const;
		uint16_t	calcSplitIdx(bool& fInsR,const SearchKey& key,unsigned idx,size_t lInsert,uint16_t prefixSize,bool fNewKey=false) const;
		bool		checkSibling(const SearchKey& key) const {return info.sibling!=INVALID_PAGEID && testKey(key,uint16_t(~0u))>=0;}
		int			testKey(const SearchKey& key,uint16_t idx,bool fPrefix=false) const;
		bool		testBound(const SearchKey& key,uint16_t idx,const IndexSeg *sg,unsigned nSegs,bool fStart,bool fPrefix=false) const;
		int			cmpKey(const SearchKey& key) const {return info.nSearchKeys>0&&testKey(key,0)<0?-1:info.sibling!=INVALID_PAGEID&&testKey(key,uint16_t(~0u))>=0?1:0;}
		int			cmpKeys(const byte *pv,unsigned idx,unsigned sidx) const;
		void		checkKeys() const;
		void		*findData(unsigned idx,uint16_t& lData,const PagePtr **pPtr=NULL) const;
		bool		findKey(const SearchKey& key,unsigned& pos) const;
		bool		findSubKey(const byte *pkey,uint16_t lkey,unsigned& pos) const;
		bool		findSubKey(const byte *tab,unsigned nElts,unsigned idx,unsigned& res) const;
		uint16_t	getSerKeySize(uint16_t idx) const {if (info.fmt.isNumKey()) return SearchKey::extKeyLen[info.fmt.keyType()]+1; uint16_t l=getKeySize(idx); return l+(l<128?2:3);}
		uint16_t	getKeyExtra(uint16_t idx) const {return info.fmt.isNumKey()?0:getKeySize(idx);}
		uint16_t	getKeySize(uint16_t idx) const;
		void		getKey(uint16_t idx,SearchKey& key) const;
		void		serializeKey(uint16_t idx,void *buf) const;
		bool		isSubTree(const PagePtr& dp) const {return (dp.len&TRO_MANY)!=0 && ((SubTreePage*)((byte*)this+dp.offset))->fSubPage==0xFFFF;}
		const SubTreePageKey *findExtKey(const void *key,size_t lkey,const SubTreePageKey *tpk,unsigned nKeys,uint16_t *poff=NULL) const;
		RC			insertKey(unsigned idx,const byte *key,const byte *data,uint16_t lData,uint16_t nData,uint16_t l);
		bool		insertValues(const byte *data,uint16_t ll,PagePtr *vp,uint16_t off,unsigned idx,uint16_t n=1,const uint16_t *shs=NULL);
		void		deleteValues(uint16_t ll,PagePtr *vp,uint16_t off,uint16_t n=1);
		RC			adjustCount(byte *ptr,PagePtr *vp,unsigned idx,const byte *p,size_t lp,bool fDec=false);
		RC			adjustKeyCount(unsigned idx,const byte *p,size_t l,bool fDec=false);
		const byte	*operator[](unsigned idx) const {return (const byte*)(this+1)+((uint16_t*)(this+1))[idx];}
		const byte	*getK(unsigned idx,uint16_t& l) const {byte *p=(byte*)(this+1)+((uint16_t*)(this+1))[idx]; l=PINRef::len(p); return p;}
		const byte	*getK(const void *pidx,uint16_t& l) const {byte *p=(byte*)(this+1)+*(uint16_t*)pidx; l=PINRef::len(p); return p;}
	};
	static	const byte	*getK(const void *p,unsigned idx,uint16_t& l) {p=(byte*)p+((uint16_t*)p)[idx]; l=PINRef::len((byte*)p); return (byte*)p;}
	static	const byte	*getK(const void *p,const void *pidx,uint16_t& l) {p=(byte*)p+*(uint16_t*)pidx; l=PINRef::len((byte*)p); return (byte*)p;}
	static	const byte	*findValue(const void *val,uint16_t lv,const byte *frame,const byte *ps,uint16_t ls,bool fPR,uint16_t *ins=NULL);

public:
	TreePageMgr(StoreCtx *ctx);
	void	initPage(byte *page,size_t lPage,PageID pid);
	bool	afterIO(PBlock *,size_t lPage,bool fLoad);
	bool	beforeFlush(byte *frame,size_t len,PageID pid);
	RC		update(PBlock *,size_t len,unsigned info,const byte *rec,size_t lrec,unsigned flags,PBlock *newp=NULL);
	PageID	multiPage(unsigned info,const byte *rec,size_t lrec,bool& fMerge);
	RC		undo(unsigned info,const byte *rec,size_t lrec,PageID);
	PGID	getPGID() const;

	RC		initPage(PBlock *page,IndexFormat ifmt,uint16_t level,const SearchKey* key,PageID pid0,PageID pid1);
	RC		count(TreeCtx& tctx,const SearchKey& key,uint64_t& nValues);
	RC		findByPrefix(TreeCtx& tctx,const SearchKey& key,uint32_t prefix,byte *buf,byte &l);
	RC		insert(TreeCtx& tctx,const SearchKey&,const void *value,uint16_t lValue,unsigned multi=0,bool fUnique=false);
	RC		update(TreeCtx& tctx,const SearchKey&,const void *oldValue,uint16_t lOldValue,const void *newValue,uint16_t lNewValue);
	RC		edit(TreeCtx& tctx,const SearchKey& key,const void *newValue,uint16_t lNewValue,uint16_t lOld,uint16_t sht);
	RC		truncate(TreeCtx& tctx,const SearchKey& key,uint64_t val,bool fCount);
	RC		remove(TreeCtx& tctx,const SearchKey&,const void *value,uint16_t lValue,unsigned multi=0);
	RC		merge(PBlock *left,PBlock *right,PBlock *par,Tree& tr,const SearchKey&,unsigned idx);
	RC		drop(PBlock *&pb,TreeFreeData*);

	size_t		contentSize() const {return xSize;}
	unsigned	checkTree(PageID pid,PageID sib,unsigned depth,CheckTreeReport& out,CheckTreeReport *sec,bool fLM=true);
	unsigned	enumPages(PageID root);

private:
	static		PageID	startSubPage(const TreePage *tp,const PagePtr& vp,const SearchKey *key,int& level,bool fRead,bool fBefore);
	static		PageID	prevStartSubPage(const TreePage *tp,const PagePtr& vp,PageID);
	void		addNewPage(TreeCtx& tctx,const SearchKey& key,PageID pid,bool fTry=false);
	uint16_t	packMulti(byte *buf,const void *pv,unsigned start,unsigned end,TreePage *tp=NULL);
	uint16_t	calcXSz(const byte *pv,unsigned from,unsigned to,const TreePage *tp=NULL);
	static	size_t	calcChunkSize(const byte *pv,unsigned start,unsigned end);
	RC			modSubTree(TreeCtx& tctx,const SearchKey& key,unsigned idx,const PagePtr *vp,const void *newV,uint16_t lNew,const void *oldV=NULL,uint16_t lOld=0,unsigned multi=0);
	RC			insertSubTree(TreeCtx &tctx,const void *value,IndexFormat ifmt,unsigned start,unsigned end,unsigned& pageCnt,PageID *pages,uint16_t *indcs,size_t xS,unsigned *pResidual=NULL);
	RC			split(TreeCtx& tctx,const SearchKey *key,unsigned& idx,uint16_t splitIdx,bool fInsR,PBlock **right=NULL);
	RC			spawn(TreeCtx& tctx,size_t lInsert,unsigned idx=~0u);
	friend	class	SubTreeInit;
	friend	struct	TreeCtx;
	friend	class	TreeRQ;
};

};

#endif
