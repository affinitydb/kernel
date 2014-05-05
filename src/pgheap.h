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

Written by Mark Venguerov 2004-2014

**************************************************************************************/

/**
 * heap page and heap directory page structure description
 * heap page and heap directory page control structures
 */
#ifndef _PGHEAP_H_
#define _PGHEAP_H_

#include "pagemgr.h"
#include "utils.h"
#include "affinity.h"
#include "session.h"

#define SPACE_HASH_SIZE		512
#define	SPACE_TAB_SIZE		4096
#define	SPACE_PAGE_TRIES	8
#define	SPACE_OVERSHOOT		0x0100

#define	HP_ALIGN			2

#define	HPOP_MASK			0x000F
#define	HPOP_SHIFT			4

#define HOH_TYPEMASK		0x0003

#define	HOH_COMPOUND		0x0008		// PIN has compound properties (i.e. VT_COLLECTION, VT_MAP, VT_STRUCT)
#define	HOH_TEMP_ID			0x0010		// page slot to be re-used, when PIN is purged
#define	HOH_SSVS			0x0020		// PIN has SSVs(separately stored values)
#define	HOH_IMMUTABLE		0x0040		// PIN is immutable
#define	HOH_PARTS			0x0080		// PIN contains references to parts
#define	HOH_REPLICATED		0x0100		// PIN is or can be replicated
#define	HOH_NOTIFICATION	0x0200		// PIN modifications generate notifications
#define	HOH_MULTIPART		0x0400		// is not contiguous on page
#define	HOH_COMPACTREF		0x0800		// contains compressed refs to other PINs on this page
#define	HOH_NOREPLICATION	0x1000		// PIN is not replicated
#define	HOH_HIDDEN			0x2000		// hidden PIN
#define	HOH_FT				0x4000		// some properties are FT indexed
#define HOH_DELETED			0x8000		// PIN is soft-deleted

/**
 * Indexing flags
 */
#define	IX_OFT				0x01000000
#define	IX_NFT				0x02000000
#define	IX_RENAME			0x04000000
#define	IX_MASK				0x07000000

#define	META_PROP_LOCAL		0x04

using namespace Afy;

namespace AfyKernel
{

enum HeapObjType	{HO_PIN,HO_BLOB,HO_SSVALUE,HO_FORWARD,HO_ALL};
enum HPOP			{HPOP_INSERT,HPOP_EDIT,HPOP_DELETE,HPOP_PURGE,HPOP_PINOP,HPOP_MIGRATE,HPOP_ALL};
enum HeapDataFmt	{HDF_COMPACT,HDF_SHORT,HDF_NORMAL,HDF_LONG};

struct HType
{
	uint8_t		flags;
	uint8_t		type;

	ValueType	getType() const {return ValueType(type&0x3f);}
	HeapDataFmt	getFormat() const {return HeapDataFmt(type>>6);}
	bool		isCompact() const {return (type&0xC0)==(HDF_COMPACT<<6);}
	bool		isCompound() const {return (unsigned)((type&0x3f)-VT_COLLECTION)<=(unsigned)(VT_RANGE-VT_COLLECTION);}
	bool		isCollection() const {return (type&0x3f)==VT_COLLECTION;}
	bool		isArray() const {return (type&0x3f)==VT_ARRAY;}
	bool		isString() const {return (type&0x3f)==VT_STRING;}
	bool		isLOB() const {return (type&0x3f)==VT_STREAM;}
	bool		canBeSSV() const {byte ty=type&0x3f; return ty>=VT_STRING && ty<=VT_BSTR || ty==VT_STMT || ty==VT_EXPR;}
	bool		operator==(const HType& rhs) const {return type==rhs.type;}
	bool		operator!=(const HType& rhs) const {return type!=rhs.type;}
	void		setType(ValueType vt,HeapDataFmt fmt) {assert(((fmt&~3)|(vt&0xC0))==0); type=byte(fmt<<6|vt);}
	const	static	HType	compactRef;
};

struct HRefSSV
{
	PageID	pageID;
	PageIdx	idx;
	HType	type;
};

struct HLOB
{
	HRefSSV		ref;
	uint64_t	len;
};

class HeapPageMgr : public TxPage
{
protected:
	friend	class	QueryPrc;
	friend	class	PINx;
	friend	class	BigMgr;
	friend	class	Stmt;
	friend	class	Expr;
	friend	struct	SubTx;
	friend	class	StreamX;
	friend	struct	PropMod;
	friend	struct	PropInfo;
	friend	struct	ModCtx;
	friend	class	ClassPropIndex;
	friend	struct	ECB;
	friend	class	NavTxDelete;
	friend	class	Navigator;
	friend	class	Collection;
	friend	class	InitCollection;
	friend	struct	EMB;
	friend	class	MapTxDelete;
	friend	class	Map;
	friend	class	MMap;
	friend	class	InitMap;
	friend	class	PathOp;
	friend	class	FTIndexMgr;
	friend	class	FullScan;
	friend	class	Session;
	friend	class	TransOp;
#pragma pack(2)
	struct HeapObjHeader {
		uint16_t		descr;
		uint16_t		length;		// total length of all pieces on this page
		HeapObjType		getType() const {return (HeapObjType)(descr&HOH_TYPEMASK);}
		ushort			getLength() const {return (ushort)ceil((size_t)length,HP_ALIGN);}
	};
	struct HeapV {
		uint16_t		id[2];
		PageOff			offset;
		HType			type;
		uint32_t		getID() const {return (uint32_t)id[0]<<16|id[1];}
		void			setID(uint32_t ii) {id[0]=ushort(ii>>16); id[1]=ushort(ii);}
		PropertyID		getPropID() const {return PropertyID((uint32_t)id[0]<<16|id[1]);}
		class HVCmp {public: __forceinline static int cmp(const HeapV& hv,PropertyID pid) {return cmp3((uint32_t)hv.id[0]<<16|hv.id[1],pid);}};
	};
	struct HeapKey {
		uint16_t		key[2];
		uint32_t		getKey() const {return (uint32_t)key[0]<<16|key[1];}
		void			setKey(uint32_t k) {key[0]=uint16_t(k>>16); key[1]=uint16_t(k);}
	};
	struct HeapVV {
		uint16_t		cnt		:15;
		uint16_t		fUnord	:1;
		HeapV			start[1];
		ushort			length(bool fA) const {return (ushort)(sizeof(HeapVV)+(cnt-1)*sizeof(HeapV)+(fA?sizeof(HeapKey):0));}
		const	HeapV	*find(uint32_t id,HeapV **ins=NULL) const {return BIN<HeapV,PropertyID,HeapV::HVCmp>::find(id,start,cnt,ins);}
		const	HeapV	*findElt(uint32_t id) const {
			if (id==STORE_FIRST_ELEMENT) return start; else if (id==STORE_LAST_ELEMENT) return &start[cnt-1];
			if (fUnord==0) return BIN<HeapV,PropertyID,HeapV::HVCmp>::find(id,start,cnt);
			for (int i=cnt; --i>=0; ) if (start[i].getID()==id) return &start[i];
			return NULL;
		}
		HeapKey	*getHKey() const {return (HeapKey*)((HeapV*)(this+1)+cnt-1);}
	};
	struct HeapExtCollPage {
		uint32_t		key;
		PageID			page;
	};
	struct HeapExtCollection {
		uint32_t		nElements;
		uint32_t		keygen;
		PageID			anchor;
		PageID			leftmost;
		uint32_t		indexID;
		uint32_t		firstID;
		uint32_t		lastID;
		uint16_t		level;
		uint16_t		nPages;
		HeapExtCollPage	pages[1];
		HeapExtCollection(Session *ses) : nElements(0),keygen(ses->getStore()->getPrefix()),anchor(INVALID_PAGEID),leftmost(INVALID_PAGEID),indexID(~0u),firstID(0),lastID(0),level(0),nPages(0) {pages[0].key=0; pages[0].page=INVALID_PAGEID;}
	};
	struct HeapExtMapPage {		//????
		uint32_t		key;
		PageID			page;
	};
	struct HeapExtMap {
		uint32_t		nElements;
		PageID			anchor;
		PageID			leftmost;
		uint16_t		level;
		uint16_t		nPages;
		HeapExtMapPage	pages[1];
		HeapExtMap() : nElements(0),anchor(INVALID_PAGEID),leftmost(INVALID_PAGEID),level(0),nPages(0) {pages[0].key=0; pages[0].page=INVALID_PAGEID;}
	};
	struct HeapArray {
		uint16_t		xdim;
		uint16_t		ydim;
		uint16_t		start;
		uint8_t			type;
		uint8_t			flags;
		uint16_t		lelt;
		uint16_t		nelts;
		uint16_t		units;
		ushort			length() const {return sizeof(HeapArray)+xdim*ydim*lelt+(flags&6);}
		void			*data() const {return (byte*)(this+1)+((flags&0x38)>>3);}
		void			setAlign(uint8_t align) {flags=(flags&~0x38)|(align&6)<<3;}
	};
	struct HeapLOB {
		HeapObjHeader	hdr;
		byte			prev[PageAddrSize];
		byte			next[PageAddrSize];
	};
	struct TypedPtr {
		PagePtr			ptr;
		HType			type;
	};
	struct HeapPropMod {
		uint16_t		op;
		uint16_t		shift;
		uint32_t		propID;
		uint32_t		eltId;
		uint32_t		eltKey;
		TypedPtr		newData;
		TypedPtr		oldData;
	};
	struct HeapPINMod {
		uint32_t		nops;
		uint16_t		descr;
		uint16_t		odscr;
		HeapPropMod		ops[1];
	};
	struct HeapSetFlag {
		uint16_t		value;
		uint16_t		mask;
	};
	struct HeapModEdit {
		uint16_t		dscr;
		uint16_t		shift;
		PagePtr			oldPtr;
		PagePtr			newPtr;
	};
	struct HeapPage;
	struct HeapPIN {
		HeapObjHeader	hdr;
		uint16_t		nProps;
		uint16_t		fmtExtra	:2;
		uint16_t		meta		:14;
		ushort			headerLength() const {return sizeof(HeapPIN)+nProps*sizeof(HeapV)+refLength[fmtExtra];}
		HeapV			*getPropTab() const {return (HeapV*)(this+1);}
		const HeapV		*findProperty(uint32_t propID,HeapV **ins=NULL) const {return BIN<HeapV,PropertyID,HeapV::HVCmp>::find(propID,(HeapV*)(this+1),nProps,ins);}
		RC				serialize(byte *&buf,size_t& lrec,const HeapPage *hp,MemAlloc *ma,size_t len=0,bool fExpand=false) const;
		PageAddr		*getOrigLoc() const {return (PageAddr*)((byte*)(this+1)+nProps*sizeof(HeapV));}
		void			setOrigLoc(const PageAddr& addr) {assert(fmtExtra<=HDF_SHORT); memcpy((byte*)(this+1)+nProps*sizeof(HeapV),&addr,PageAddrSize); fmtExtra=HDF_SHORT;}
		bool			isMigrated() const {return fmtExtra==HDF_SHORT;}
		bool			hasRemoteID() const {return fmtExtra>=HDF_NORMAL;}
		byte			*getRemoteID(HeapDataFmt& fmt) const {fmt=(HeapDataFmt)fmtExtra; return (byte*)(this+1)+nProps*sizeof(HeapV);}
		bool			getAddr(PID& id) const;
		size_t			expLength(const byte *) const;
	};
	struct HeapPage {
		TxPageHeader	hdr;
		uint16_t		nSlots;
		uint16_t		freeSlots;
		PageOff			freeSpace;
		uint16_t		scatteredFreeSpace;

		size_t			totalFree() const {return hdr.length()-FOOTERSIZE-freeSpace-nSlots*sizeof(PageOff)+scatteredFreeSpace;}
		size_t			contFree() const {return hdr.length()-FOOTERSIZE-freeSpace-nSlots*sizeof(PageOff);}
		void			compact(bool fCheckOnly=false,PageIdx modified=INVALID_INDEX,ushort propIdx=ushort(~0u));
		void			moveObject(const HeapObjHeader *hobj,byte*,PageIdx,bool,ushort propIdx=ushort(~0u));
		bool			checkPage(bool fWrite) const;
		bool			checkIdx(PageIdx idx) const;
		bool			isFreeSlot(PageIdx idx) const {if (idx>=nSlots) return true; PageOff off=(*this)[idx]; return off==0||(off&1)!=0;}
		PageOff			getOffset(PageIdx idx) const {PageOff off=checkIdx(idx)?(*this)[idx]:0; return off;}
		HeapObjHeader	*getObject(PageOff off) const {return off==0||(off&1)!=0?NULL:(HeapObjHeader*)((char*)this+off);}
		PageOff&		operator[](PageIdx idx) const {return ((PageOff*)((byte*)this+hdr.length()-FOOTERSIZE))[-idx-1];}
		unsigned		countRefs(const HeapV *hprop) const;
		void			getRefs(const HeapV *hprop,PID *pids,unsigned xPids) const;
		void			getRef(PID& id,HeapDataFmt fmt,PageOff offs) const;
	};
#pragma pack()
	static const ushort	refLength[4];
	static	void	alignArray(byte *frame,HeapV *hprop);
	static	ushort	adjustCompound(byte *frame,HeapV *hprop,ushort sht,StoreCtx *ctx=NULL);
	static	ushort	moveCompound(HType vt,byte *dest,PageOff doff,const byte *src,PageOff soff);
	static	ushort	moveArray(HType vt,byte *dest,PageOff doff,const byte *src,PageOff soff,bool fAlign);
	static	ushort	moveData(byte *dest,const byte *src,HType vt) {assert(!vt.isCompound()); ushort size=dataLength(vt,src); memcpy(dest,src,size); return size;}
	static	ushort	collDescrSize(const HeapExtCollection *bc) {return bc==NULL?0:sizeof(HeapExtCollection)+ushort(bc->nPages>0?(bc->nPages-1)*sizeof(HeapExtCollPage):0);}
	static	HeapExtCollection *copyDescr(const HeapExtCollection *c,MemAlloc *ma) {size_t len=collDescrSize(c); byte *p=(byte*)ma->malloc(len); if (p!=NULL) memcpy(p,c,len); return (HeapExtCollection*)p;}

	class HeapSpace {
		struct HeapPageSpace {
			HChain<HeapPageSpace>	list;
			PageID					pageID;
			size_t					space;
			HeapPageSpace(PageID pid,size_t l) : list(this),pageID(pid),space(l) {} 
			PageID	getKey() const {return pageID;}
			void	*operator new(size_t s,StoreCtx *ctx) throw() {return ctx->malloc(s);}
			void	operator delete(void *p) {free(p,STORE_HEAP);}
		};
		HashTab<HeapPageSpace,PageID,&HeapPageSpace::list>	spaceTab;
		HeapPageSpace										**pageTab;
		unsigned											nPages;
		Mutex												lock;
	public:
		HeapSpace(MemAlloc *ma) : spaceTab(SPACE_HASH_SIZE,ma),pageTab(NULL),nPages(0) {}
		RC			set(StoreCtx *ctx,PageID,size_t,bool fAdd=true);
		RC			getPage(size_t size,class PBlock*& pb,HeapPageMgr *mgr);
		unsigned	find(const HeapPageSpace *hps) const {
			assert(hps!=NULL); unsigned i=0;
			if (pageTab!=NULL) for (unsigned n=nPages; n>0; ) {
				unsigned k=n>>1; const HeapPageSpace *qq=pageTab[i+k]; if (qq==hps) return i+k;
				if (qq->space<hps->space || qq->space==hps->space && qq>hps) n=k; else {i+=k+1; n-=k+1;}
			}
			return i;
		}
		friend	class	HeapPageMgr;
	} freeSpace;

public:
	static	size_t		contentSize(size_t lPage) {return lPage - sizeof(HeapPage) - FOOTERSIZE;}
	static	ushort		dataLength(HType vt,const byte *pData,const byte *frame=NULL,unsigned *idxMask=NULL);
	static	unsigned	getPrefix(const PID& id);
public:
	HeapPageMgr(StoreCtx*,PGID);
	virtual	~HeapPageMgr();
	void	initPage(byte *page,size_t lPage,PageID pid);
	bool	beforeFlush(byte *frame,size_t len,PageID pid);
	RC		update(class PBlock *,size_t,unsigned info,const byte *rec,size_t lrec,unsigned flags,class PBlock *newp=NULL);

	class	PBlock *getPartialPage(size_t size) {class PBlock *pb=NULL; freeSpace.getPage(size,pb,this); return pb;}
	void	reuse(PageID pid,size_t space,StoreCtx *ctx) {freeSpace.set(ctx,pid,space);}
	void	discardPage(PageID,Session *ses);
	void	initPartial();
	static	void	savePartial(HeapPageMgr *mgr1,HeapPageMgr *mgr2);
};

class PINPageMgr : public HeapPageMgr
{
public:
	PINPageMgr(StoreCtx *ctx) : HeapPageMgr(ctx,PGID_HEAP) {}
	bool	afterIO(class PBlock *,size_t lPage,bool fLoad);
	PGID	getPGID() const;
	class	PBlock *getNewPage(size_t size,size_t reserve,Session *ses);
	RC		addPagesToMap(const PageSet&,Session *ses,bool fClasses=false);
	void	reuse(class PBlock *,Session *ses,size_t reserve,bool fMod=false);
};

class SSVPageMgr : public HeapPageMgr
{
public:
	SSVPageMgr(StoreCtx *ctx) : HeapPageMgr(ctx,PGID_SSV) {}
	PGID	getPGID() const;
	class	PBlock *getNewPage(size_t size,Session *ses,bool& fNew);
	void	reuse(class PBlockP& ,Session *ses,bool fNew,bool fInsert=false);
};

enum {HDU_HPAGES, HDU_NEXT};

class HeapDirMgr : public TxPage
{
	struct HeapDirPage {
		TxPageHeader	hdr;
		PageID			next;
		uint32_t		nSlots;
	};
	struct HeapDirUpdate {
		uint32_t		nHeapPages;
		PageID			heapPages[1];
	};
	RWLock	dirLock;
	PageID	maxPage;
	PageIdx	maxIdx;
	static	size_t	contentSize(size_t lPage) {return lPage - sizeof(HeapDirPage) - FOOTERSIZE;}
public:
	HeapDirMgr(StoreCtx*);
	virtual	~HeapDirMgr();
	void	initPage(byte *page,size_t lPage,PageID pid);
	bool	afterIO(class PBlock *,size_t lPage,bool fLoad);
	bool	beforeFlush(byte *frame,size_t len,PageID pid);
	RC		update(class PBlock *,size_t len,unsigned info,const byte *rec,size_t lrec,unsigned flags,class PBlock *newp=NULL);
	PageID	multiPage(unsigned info,const byte *rec,size_t lrec,bool& fMerge);
	PGID	getPGID() const;

	RC		getReserved(uint32_t& nPages,PageID& nextDirPage,byte *&buf,size_t& lbuf,MemAlloc *ma);
//	void	getMax(PageID& pid,PageIdx& idx) {dirLock.lock(RW_S_LOCK); pid=maxPage; idx=maxIdx; dirLock.unlock();}
//	void	unlock() {dirLock.unlock(true);}
	void	lock(class PBlock*);

	friend	class PINPageMgr;
	friend	class QueryPrc;
	friend	class Classifier;
	friend	class FullScan;
};

};

#endif
