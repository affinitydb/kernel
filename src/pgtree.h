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

/**
 * B-tree index page structure
 * index tree page manipulation routines
 */
#ifndef _PGTREE_H_
#define _PGTREE_H_

#include "pagemgr.h"
#include "idxtree.h"
#include "buffer.h"

namespace AfyKernel
{

#define	XSUBPAGES		20

/**
 * index tree operation format
 */
#define	TRO_SHIFT		4
#define	TRO_MASK		0x000F
#define	TRO_EXT_DATA	0x8000

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
	MO_INSERT, MO_DELETE, MO_INIT
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
	const	unsigned			mode;
	PBlockP					pb;
	PBlockP					parent;
	unsigned					depth;
	unsigned					leaf;
	unsigned					index;
	unsigned					lvl2;
	const SearchKey			*mainKey;
	PagePtr					vp;
	PageID					newPage;
	IMultiKey				*moreKeys;
	TreeConnect				*tcon;
	uint32_t				thndl;
	unsigned					stamps[TREE_NODETYPE_ALL];
	PageID					stack[TREE_MAX_DEPTH];
	TreeCtx(Tree& tr,IMultiKey *mk=NULL) : tree(&tr),mode(tr.getMode()),depth(0),leaf(~0u),index(0),lvl2(~0u),mainKey(NULL),newPage(INVALID_PAGEID),moreKeys(mk),tcon(NULL),thndl(0) {memset(stamps,0,sizeof(stamps));}
	TreeCtx(const TreeCtx &ctx) : tree(ctx.tree),mode(ctx.mode),depth(ctx.depth),leaf(ctx.leaf),index(ctx.index),lvl2(ctx.lvl2),mainKey(NULL),newPage(ctx.newPage),moreKeys(NULL),tcon(NULL),thndl(0)
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
		uint16_t	lUPrefix;

		void		initFree(size_t lPage) {freeSpace=ushort(lPage-FOOTERSIZE); freeSpaceLength=ushort(lPage-sizeof(TreePage)-FOOTERSIZE); scatteredFreeSpace=lPrefix=0;}
		ushort		keyLength() const {assert(!fmt.isKeyOnly()); return fmt.isSeq()?0:!fmt.isFixedLenKey()?sizeof(PagePtr):(ushort)ceil(fmt.keyLength()-lPrefix,sizeof(uint16_t));}
		ushort		calcEltSize() const {
			return fmt.isVarKeyOnly()?L_SHT:fmt.isSeq()?fmt.isFixedLenData()?fmt.dataLength():sizeof(PagePtr):
				!fmt.isFixedLenKey()?sizeof(PagePtr)+ushort(fmt.isFixedLenData()?ceil(fmt.dataLength(),sizeof(uint16_t)):sizeof(PagePtr)):fmt.isFixedLenData()?fmt.keyLength()-lPrefix:
				!fmt.isNumKey()?(ushort)ceil(fmt.keyLength()-lPrefix,sizeof(uint16_t))+sizeof(PagePtr):fmt.keyLength()==lPrefix?sizeof(PagePtr):
				(ushort)ceil(fmt.keyLength()-lPrefix+sizeof(PagePtr),fmt.keyLength()-lPrefix);
		}
		ushort		extraEltSize(ushort newPrefLen) const {
			assert(newPrefLen<lPrefix && lPrefix!=ushort(~0u));
			return !fmt.isFixedLenKey()||fmt.isFixedLenData()?lPrefix-newPrefLen:
				!fmt.isNumKey()?ushort(ceil(fmt.keyLength()-newPrefLen,sizeof(uint16_t))-ceil(fmt.keyLength()-lPrefix,sizeof(uint16_t))):
				ushort(ceil(fmt.keyLength()-newPrefLen+sizeof(PagePtr),fmt.keyLength()-newPrefLen)-ceil(fmt.keyLength()-lPrefix+sizeof(PagePtr),fmt.keyLength()-lPrefix));
		}
		ushort calcVarSize(const SearchKey& key,ushort lData,ushort prefLen) const {
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
		uint16_t	lkey	:15;
		uint16_t	fMulti	:1;
		uint16_t	ldata;
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
		uint16_t		lPrefix	:15;
		uint16_t		fLastR	:1;
		uint16_t		lData;
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
		uint64_t		level:4;
		uint64_t		counter:60;
		PageID			anchor;
		PageID			leftMost;
	};
	/**
	 * structure of index tree page
	 */
	struct TreePage {
		TxPageHeader	hdr;
		TreePageInfo	info;

		void		store(const void *data,ushort len,PagePtr& ptr) {
			ushort l=len&~TRO_EXT_DATA; assert(info.freeSpaceLength>=l); 
			ptr.offset=info.freeSpace-=l; ptr.len=len; if (l!=0) {memcpy((byte*)this+info.freeSpace,data,l); info.freeSpaceLength-=l;}
		}
		template<typename T> bool findNumKey(T key,unsigned,unsigned& pos) const;
		template<typename T> bool findNumKeyVar(T key,unsigned,unsigned& pos) const;
		void		compact(bool fCheckOnly=false,unsigned extIdx=~0u,ushort lext=0);
		void		changePrefixSize(ushort prefLen);
		void		copyEntries(TreePage *,ushort prefLen,ushort start) const;
		ushort		calcPrefixSize(const SearchKey &key,unsigned idx,bool fExt=false,const SearchKey *pk=NULL) const;
		ushort		calcPrefixSize(unsigned start,unsigned end) const;
		void		storeKey(const void *key,void *ptr);
		RC			prefixFromKey(const void *key,ushort prefSize=ushort(~0u));
		void		moveExtTree(const TreePage *src,const PagePtr& from,PagePtr& to);
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
		const void	*getValue(const SearchKey& key,ushort& lData) const {
			assert(isLeaf() && info.fmt.keyType()==key.type); unsigned pos;
			return info.fmt.isUnique()&&findKey(key,pos)?findData(pos,lData):NULL;
		}
		uint32_t	getKeyU32(ushort idx) const;
		ushort		length(const PagePtr& ptr) const;
		byte		*findValue(const void *val,ushort lv,const PagePtr& vp,ushort *pins=NULL) const;
		ushort		calcSplitIdx(bool& fInsR,const SearchKey& key,unsigned idx,size_t lInsert,ushort prefixSize,bool fNewKey=false) const;
		bool		checkSibling(const SearchKey& key) const {return info.sibling!=INVALID_PAGEID && testKey(key,ushort(~0u))>=0;}
		int			testKey(const SearchKey& key,ushort idx,bool fPrefix=false) const;
		bool		testBound(const SearchKey& key,ushort idx,const IndexSeg *sg,unsigned nSegs,bool fStart,bool fPrefix=false) const;
		int			cmpKey(const SearchKey& key) const {return info.nSearchKeys>0&&testKey(key,0)<0?-1:info.sibling!=INVALID_PAGEID&&testKey(key,ushort(~0u))>=0?1:0;}
		int			cmpKeys(const byte *pv,unsigned idx,unsigned sidx,ushort lElt,bool fVM) const;
		void		checkKeys() const;
		void		*findData(unsigned idx,ushort& lData,const PagePtr **pPtr=NULL) const;
		bool		findKey(const SearchKey& key,unsigned& pos) const;
		bool		findSubKey(const byte *pkey,ushort lkey,unsigned& pos) const;
		bool		findSubKey(const byte *pkey,ushort lkey,unsigned& pos,const byte *pref,ushort lpref) const;
		bool		findSubKey(const byte *tab,unsigned nElts,unsigned idx,unsigned& res,bool fVM,bool fPref=true) const;
		ushort		getSerKeySize(ushort idx) const {if (info.fmt.isNumKey()) return SearchKey::extKeyLen[info.fmt.keyType()]+1; ushort l=getKeySize(idx); return l+(l<128?2:3);}
		ushort		getKeyExtra(ushort idx) const {return info.fmt.isNumKey()?0:getKeySize(idx);}
		ushort		getKeySize(ushort idx) const;
		void		getKey(ushort idx,SearchKey& key) const;
		void		serializeKey(ushort idx,void *buf) const;
		static bool	isSubTree(const PagePtr& dp) {return (dp.len&TRO_EXT_DATA)!=0;}
		const SubTreePageKey *findExtKey(const void *key,size_t lkey,const SubTreePageKey *tpk,unsigned nKeys,ushort *poff=NULL) const;
		RC			insertKey(unsigned idx,const byte *key,const byte *data,ushort lData,ushort type,ushort l);
		bool		insertValues(const byte *data,ushort ll,PagePtr *vp,ushort off,unsigned idx,bool fVM,ushort n=1,const uint16_t *shs=NULL);
		void		deleteValues(ushort ll,PagePtr *vp,ushort off,bool fVM,ushort n=1);
		RC			adjustCount(byte *ptr,PagePtr *vp,unsigned idx,const byte *p,size_t lp,bool fDec=false);
		RC			adjustKeyCount(unsigned idx,const byte *p,size_t l,const byte *pref=NULL,size_t lpref=0,bool fDec=false);
		byte		*kbase() const {assert(info.fmt.isVarKeyOnly()); return (byte*)this+info.freeSpace;}
		//byte		*operator[](unsigned idx) const {assert(info.fmt.isVarKeyOnly()&&idx<(unsigned)info.nEntries); return (byte*)this+info.freeSpace-((ushort*)(this+1))[idx];}
		//byte		*operator[](const void *p) const {assert(info.fmt.isVarKeyOnly()); return (byte*)this+info.freeSpace-((ushort*)p)[0];}
		byte		*operator[](unsigned idx) const {assert(info.fmt.isVarKeyOnly()&&idx<(unsigned)info.nEntries); return (byte*)(this+1)+((ushort*)(this+1))[idx];}
		byte		*operator[](const void *p) const {assert(info.fmt.isVarKeyOnly()); return (byte*)(this+1)+((ushort*)p)[0];}
		ushort		lenPK(unsigned idx) const {assert(info.fmt.isVarKeyOnly()&&idx<(unsigned)info.nEntries); return ((ushort*)(this+1))[idx+1]-((ushort*)(this+1))[idx];}
		static	size_t	calcChunkSize(const byte *vals,ushort lVals,unsigned start,unsigned end,IndexFormat fmt,ushort& lpref,const TreePage *tp=NULL,bool fLast=false);
	};

public:
	TreePageMgr(StoreCtx *ctx);
	void	initPage(byte *page,size_t lPage,PageID pid);
	bool	afterIO(PBlock *,size_t lPage,bool fLoad);
	bool	beforeFlush(byte *frame,size_t len,PageID pid);
	RC		update(PBlock *,size_t len,unsigned info,const byte *rec,size_t lrec,unsigned flags,PBlock *newp=NULL);
	PageID	multiPage(unsigned info,const byte *rec,size_t lrec,bool& fMerge);
	RC		undo(unsigned info,const byte *rec,size_t lrec,PageID);
	PGID	getPGID() const;

	RC		initPage(PBlock *page,IndexFormat ifmt,ushort level,const SearchKey* key,PageID pid0,PageID pid1);
	RC		count(TreeCtx& tctx,const SearchKey& key,uint64_t& nValues);
	RC		findByPrefix(TreeCtx& tctx,const SearchKey& key,uint32_t prefix,byte *buf,byte &l);
	RC		insert(TreeCtx& tctx,const SearchKey&,const void *value,ushort lValue,bool=false);
	RC		update(TreeCtx& tctx,const SearchKey&,const void *oldValue,ushort lOldValue,const void *newValue,ushort lNewValue);
	RC		edit(TreeCtx& tctx,const SearchKey& key,const void *newValue,ushort lNewValue,ushort lOld,ushort sht);
	RC		remove(TreeCtx& tctx,const SearchKey&,const void *value,ushort lValue,bool=false);
	RC		merge(PBlock *left,PBlock *right,PBlock *par,Tree& tr,const SearchKey&,unsigned idx);
	RC		drop(PBlock *&pb,TreeFreeData*);

	size_t	contentSize() const {return xSize;}
	unsigned	checkTree(PageID pid,PageID sib,unsigned depth,CheckTreeReport& out,CheckTreeReport *sec,bool fLM=true);
	unsigned	enumPages(PageID root);

	static	ushort	nKeys(const void *p) {return *(ushort*)p/L_SHT-1;}
	static	ushort	lenK(const void *p,unsigned idx) {return ((ushort*)p)[idx+1]-((ushort*)p)[idx];}
	static	ushort	lenKK(const void *p,unsigned idx1,unsigned idx2) {assert(idx2>=idx1); return ((ushort*)p)[idx2]-((ushort*)p)[idx1];}
	static	ushort	lenA(const void *p) {return ((ushort*)p)[*(ushort*)p/L_SHT-1];}
	static	const byte *getK(const void *p,unsigned idx) {return (const byte*)p+((ushort*)p)[idx];}

private:
	static	PageID	startSubPage(const TreePage *tp,const PagePtr& vp,const SearchKey *key,int& level,bool fRead,bool fBefore);
	static	PageID	prevStartSubPage(const TreePage *tp,const PagePtr& vp,PageID);
	void	addNewPage(TreeCtx& tctx,const SearchKey& key,PageID pid,bool fTry=false);
	ushort	packMulti(byte *buf,const void *value,ushort lVal,unsigned start,unsigned end,ushort lpref,unsigned lElt,TreePage *tp=NULL);
	ushort	calcXSz(const byte *pv,unsigned from,unsigned to,const TreePage *tp=NULL);
	RC		modSubTree(TreeCtx& tctx,const SearchKey& key,unsigned idx,const PagePtr *vp,const void *newV,ushort lNew,const void *oldV=NULL,ushort lOld=0,bool fMulti=false);
	RC		insertSubTree(TreeCtx &tctx,const void *value,ushort lValue,IndexFormat ifmt,unsigned start,unsigned end,unsigned& pageCnt,PageID *pages,ushort *indcs,size_t xS,unsigned *pResidual=NULL);
	RC		split(TreeCtx& tctx,const SearchKey *key,unsigned& idx,ushort splitIdx,bool fInsR,PBlock **right=NULL);
	RC		spawn(TreeCtx& tctx,size_t lInsert,unsigned idx=~0u);
	friend	struct	TreeCtx;
	friend	class	TreeRQ;
};

};

#endif
