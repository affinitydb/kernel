/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _BLOB_H_
#define _BLOB_H_

#include "pgheap.h"
#include "pgtree.h"
#include "buffer.h"
#include "affinityimpl.h"

using namespace AfyDB;

namespace AfyKernel
{

class StreamX : public IStream, public ObjDealloc
{
protected:
	const	PageAddr	start;
	const	ValueType	type;
	MemAlloc *const		allc;
	StoreCtx *const		ctx;
	mutable	uint64_t	len;
			uint64_t	pos;
			PageAddr	current;
			size_t		shift;
public:
	StreamX() : start(), type(VT_BSTR), allc(NULL), ctx(NULL), len(0) {}
public:
	StreamX(const PageAddr& addr,uint64_t l,ValueType ty,MemAlloc *m);
	virtual				~StreamX();
	ValueType			dataType() const;
	uint64_t			length() const;
	const	PageAddr&	getAddr() const;
	size_t				read(void *buf,size_t maxLength);
	size_t				readChunk(uint64_t offset,void *buf,size_t l);
	IStream				*clone() const;
	RC					reset();
	void				destroy();
	void				destroyObj();
};

class StreamEdit : public IStream
{
	IStream	*const		stream;
	const	uint64_t	eshift;
	const	size_t		lOld;
	const	size_t		lNew;
	byte	*const		str;
	MemAlloc *const		allc;
	uint64_t			opos;
	uint64_t			epos;
public:
	StreamEdit() : stream(NULL), eshift(0), lOld(0), lNew(0), str(NULL), allc(NULL), opos(0), epos(0) {}
public:
	StreamEdit(IStream *sm,uint64_t sht,size_t lO,size_t lN,byte *s,MemAlloc *ma) 
		: stream(sm),eshift(sht),lOld(lO),lNew(lN),str(s),allc(ma),opos(0),epos(0) {}
	virtual				~StreamEdit();
	ValueType			dataType() const;
	virtual	uint64_t	length() const;
	size_t				read(void *buf,size_t maxLength);
	size_t				readChunk(uint64_t offset,void *buf,size_t l);
	virtual	IStream		*clone() const;
	RC					reset();
	void				destroy();
};

class StreamBuf : public IStream
{
	const	ValueType	type;
	const	byte		*buf;
	const	size_t		len;
			size_t		shift;
	MemAlloc *const		allc;
	const	bool		fFree;
public:
	StreamBuf(const byte *b,size_t l,ValueType ty,MemAlloc *ma,bool fF=true) 
						: type(ty),buf(b),len(l),shift(0),allc(ma),fFree(fF) {}
	ValueType			dataType() const;
	uint64_t			length() const;
	size_t				read(void *buf,size_t maxLength);
	size_t				readChunk(uint64_t offset,void *buf,size_t l);
	IStream				*clone() const;
	RC					reset();
	void				destroy();
};

class CollFactory : public TreeFactory
{
public:
	byte	getID() const;
	byte	getParamLength() const;
	void	getParams(byte *buf,const Tree& tr) const;
	RC		createTree(const byte *params,byte lparams,Tree *&tree);
	static CollFactory	factory;
};

struct ElementDataHdr {
	uint8_t			shift;
	uint8_t			flags;
	HType			type;
	// uint32_t [prev][next]
};

struct ECB : public TreeCtx
{
	StoreCtx								*const ctx;
	Tree									&tree;
	const	HeapPageMgr::HeapExtCollection	*coll;
	ElementID								eid;
	PageID									cPage;
	const	TreePageMgr::TreePage			*tp;
	ulong									pos;
	ushort									lData;
	const	ElementDataHdr					*hdr;
	ElementID								prevID;
	ElementID								nextID;
	ECB(StoreCtx *ct,Tree& tr,const HeapPageMgr::HeapExtCollection *bc) : TreeCtx(tr),ctx(ct),tree(tr),coll(bc),eid(STORE_COLLECTION_ID),
					cPage(INVALID_PAGEID),tp(NULL),pos(~0ul),lData(0),hdr(NULL),prevID(STORE_COLLECTION_ID),nextID(STORE_COLLECTION_ID) {}
	RC		get(GO_DIR whence,ElementID ei=STORE_COLLECTION_ID,bool fRead=false);
	RC		shift(ElementID ei,bool fPrev=false);
	RC		setLinks(ElementID prev,ElementID next,bool fForce=false);
	RC		unlink(ElementID& first,ElementID& last,bool fDelete=false);
	RC		prepare(ElementID newEid);
	void	release() {if (!pb.isNull()) {cPage=pb->getPageID(); pb.release();}}
};

class Collection : public Tree
{
	const	ulong					maxPages;
	MemAlloc						*const allc;
	HeapPageMgr::HeapExtCollection	*coll;
	ulong							stamp;
	bool							fMod;
	class CollFreeData : public TreeFreeData {public: RC freeData(const byte *data,StoreCtx *ctx);};
public:
	Collection(StoreCtx *ctx,ulong stamp,const HeapPageMgr::HeapExtCollection *c,MemAlloc *ma,ulong xP=0);
	virtual				~Collection();
	TreeFactory			*getFactory() const;
	IndexFormat			indexFormat() const;
	ulong				getMode() const;
	PageID				startPage(const SearchKey*,int& level,bool,bool=false);
	PageID				prevStartPage(PageID pid);
	RC					addRootPage(const SearchKey& key,PageID& pageID,ulong level);
	RC					removeRootPage(PageID page,PageID leftmost,ulong level);
	ulong				getStamp(TREE_NODETYPE) const;
	void				getStamps(ulong stamps[TREE_NODETYPE_ALL]) const;
	void				advanceStamp(TREE_NODETYPE);
	bool				lock(RW_LockType,bool fTry=false) const;
	void				unlock() const;
	void				destroy();

	bool				isModified() const {return fMod;}
	const HeapPageMgr::HeapExtCollection	*getDescriptor() const {return coll;}
	RC					modify(ExprOp op,const Value *pv,ElementID epos,ElementID eid,Session *ses);

	static	Collection	*create(const PageAddr& addr,PropertyID pid,StoreCtx *ctx,MemAlloc *ma,PBlock *pb=NULL);
	static	RC			persist(const Value& v,HeapPageMgr::HeapExtCollection& collection,Session *ses,bool fForce=true,bool fOld=false,ulong maxPages=0);
	static	RC			persistElement(Session *ses,const Value& v,ushort& lval,byte *&buf,size_t& lbuf,size_t threshold,
											ulong prev=STORE_COLLECTION_ID,ulong next=STORE_COLLECTION_ID,bool fOld=false);
	static	RC			purge(const HeapPageMgr::HeapExtCollection *hc,StoreCtx *ctx);

	const static IndexFormat collFormat;
};

class Navigator : public INav, public Tree, public ObjDealloc
{
	const	PageAddr	heapAddr;
	const	PropertyID	propID;
	const	ulong		mode;
	MemAlloc *const		allc;
	ECB					ecb;
	Value				curValue;
	ValueType			type;
public:
	Navigator(const PageAddr& addr,PropertyID pid,const HeapPageMgr::HeapExtCollection *coll,ulong md,MemAlloc *ma);
	virtual				~Navigator();
	const	Value		*navigate(GO_DIR=GO_NEXT,ElementID=STORE_COLLECTION_ID);
	ElementID			getCurrentID();
	const	Value		*getCurrentValue();
	RC					getElementByID(ElementID,Value&);
	INav				*clone() const;
	unsigned	long	count() const;	
	void				destroy();
	void				destroyObj();

	TreeFactory			*getFactory() const;
	IndexFormat			indexFormat() const;
	PageID				startPage(const SearchKey*,int& level,bool,bool=false);
	PageID				prevStartPage(PageID pid);
	RC					addRootPage(const SearchKey& key,PageID& pageID,ulong level);
	RC					removeRootPage(PageID page,PageID leftmost,ulong level);
	ulong				getStamp(TREE_NODETYPE) const;
	void				getStamps(ulong stamps[TREE_NODETYPE_ALL]) const;
	void				advanceStamp(TREE_NODETYPE);
	bool				lock(RW_LockType,bool fTry=false) const;
	void				unlock() const;
	TreeConnect			*persist(uint32_t& hndl) const;
	void				setType(ValueType ty) {type=ty;}
	
	const HeapPageMgr::HeapExtCollection	*getDescriptor() const {return ecb.coll;}
	static	RC			getPageAddr(const ElementDataHdr *edh,PageAddr&);
	friend	class		Collection;
};

#define	DEFAULT_BLOBREADTAB_SIZE	64
#define	DEFAULT_BLOBREAD_BLOCK		256

struct BlobRead
{
	HChain<BlobRead>	list;
	const PageAddr		start;
	ulong				nReaders;
	bool				fDelete;
	BlobRead(const PageAddr& addr) : list(this),start(addr),nReaders(1),fDelete(false) {}
	const PageAddr&		getKey() const {return start;}
};

typedef SyncHashTab<BlobRead,const PageAddr&,&BlobRead::list> BlobReadTab;

class BigMgr
{
	friend	class					StreamX;
	friend	class					Navigator;
	StoreCtx						*const ctx;
	BlobReadTab						blobReadTab;
	FreeQ<DEFAULT_BLOBREAD_BLOCK,Std_Alloc<STORE_HEAP> >	freeBlob;
public:
	BigMgr(StoreCtx *ct,ulong blobReadTabSize=DEFAULT_BLOBREADTAB_SIZE);
	void *operator new(size_t s,StoreCtx *ctx) {void *p=ctx->malloc(s); if (p==NULL) throw RC_NORESOURCES; return p;}
	bool canBePurged(const PageAddr& addr);
};

};

#endif
