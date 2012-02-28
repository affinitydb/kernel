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

/**
 * index B-tree control structures
 */
#ifndef _IDXTREE_H_
#define _IDXTREE_H_

#include "utils.h"
#include "affinity.h"

using namespace AfyDB;

namespace AfyKernel 
{

class Session;

/**
 * index key types
 */
enum TREE_KT
{
	KT_UINT,			/**< unsigned 64-bit integer */
	KT_INT,				/**< signed 64-bit integer */
	KT_FLOAT,			/**< single precision (32-bit) floating number */
	KT_DOUBLE,			/**< double precision (64-bit) floating number */
	KT_BIN,				/**< string (either of octets or of characters) */
	KT_REF,				/**< PIN reference (same as KT_BIN, but special ordering) */
	KT_VAR,				/**< variable type or multisegment */
	KT_ALL
};

/**
 * KT_VAR helper functions
 */
extern	int		cmpMSeg(const byte *s1,ushort l1,const byte *s2,ushort l2);
extern	bool	isHyperRect(const byte *s1,ushort l1,const byte *s2,ushort l2);
extern	bool	cmpBound(const byte *p1,ushort l1,const byte *p2,ushort l2,const IndexSeg *sg,unsigned nSegs,bool fStart);
extern	bool	checkHyperRect(const byte *s1,ushort l1,const byte *s2,ushort l2,const IndexSeg *sg,unsigned nSegs,bool fStart);
extern	ushort	calcMSegPrefix(const byte *s1,ushort l1,const byte *s2,ushort l2);

/**
 * union for different key types
 */
union IndexKeyV {
	struct {const void *p; uint16_t l;}	ptr;
	int64_t								i;
	uint64_t							u;
	float								f;
	double								d;
	int	cmp(const IndexKeyV& rhs,TREE_KT) const;
};

/**
 * search key descriptor (in-memory)
 */
struct SearchKey
{
	enum PIT_LOC {PLC_EMB, PLC_SPTR, PLC_ALLC};			/**< data allocation */
	IndexKeyV	v;
	TREE_KT		type;
	PIT_LOC		loc;
	SearchKey() {type=KT_ALL; loc=PLC_EMB;}
	SearchKey(const void *b,ushort l,bool fPR=false) {type=fPR?KT_REF:KT_BIN; loc=PLC_SPTR; v.ptr.p=b; v.ptr.l=l;}
	SearchKey(int64_t i) {type=KT_INT; loc=PLC_EMB; v.i=i;}
	SearchKey(uint64_t u) {type=KT_UINT; loc=PLC_EMB; v.u=u;}
	SearchKey(float ff) {type=KT_FLOAT; loc=PLC_EMB; v.f=ff;}
	SearchKey(double dd) {type=KT_DOUBLE; loc=PLC_EMB; v.d=dd;}
	SearchKey(const IndexKeyV& vv,TREE_KT ty,PIT_LOC lc) : v(vv),type(ty),loc(lc) {}
	bool	isSet() const {return this!=NULL && type<KT_ALL;}
	bool	isPrefNumKey() const {return type<=KT_INT;}
	bool	isNumKey() const {return type<=KT_DOUBLE;}
	ushort	extra() const {return type>=KT_BIN&&type<KT_ALL ? v.ptr.l : 0;}
	void	reset() {if (this!=NULL) {type=KT_ALL; loc=PLC_EMB;}}
	const	byte *getPtr2() const {return type<KT_BIN?(const byte*)&v:type>=KT_ALL?(byte*)0:v.ptr.p!=NULL?(const byte*)v.ptr.p:(const byte*)(this+1);}
	void	free(MemAlloc *ma) {if (loc==PLC_ALLC && type>=KT_BIN && type<KT_ALL) ma->free((void*)v.ptr.p); type=KT_ALL; loc=PLC_EMB;}
	int		cmp(const SearchKey& rhs) const {assert(type<KT_BIN||type>=KT_ALL||v.ptr.p!=NULL); return type==rhs.type?v.cmp(rhs.v,type):-2;}
	RC		toKey(const Value **pv,ulong nv,const IndexSeg *dscrs,int idx,Session *ses,MemAlloc *ma=NULL);
	RC		getValues(Value *pv,unsigned nv,const IndexSeg *dscrs,unsigned nFields,Session *ses,bool fFilter=false,MemAlloc *ma=NULL) const;
	static	ushort extLength(ushort l) {return l+(l<128?2:3);}
	ushort	extLength() const {return type<KT_BIN?extKeyLen[type]+1:type>=KT_ALL?1:v.ptr.l+(v.ptr.l<128?2:3);}
	void	serialize(void *p) const {if (type<KT_BIN) {*(byte*)p=type; memcpy((byte*)p+1,&v,extKeyLen[type]);} else if (type<KT_ALL) serialize(p,type,getPtr2(),v.ptr.l);}
	static	void serialize(void *buf,TREE_KT type,const void *p,ushort l) {byte *pb=(byte*)buf; *pb=byte(type); if (l<128) pb[1]=byte(l); else {*++pb=byte(l|0x80); pb[1]=byte(l>>7);} memcpy(pb+2,p,l);}
	static	ushort keyLen(const void *vp) {const byte *p=(const byte *)vp; return ushort(*p<KT_BIN||*p>=KT_ALL?0:p[1]&0x80?p[1]&~0x80|p[2]<<7:p[1]);}
	static	ushort keyLenP(const byte *&p) {if (*p<KT_BIN) return extKeyLen[*p++]; if (*p>=KT_ALL) {p++; return 0;} ushort l=p[1]; if ((l&0x80)!=0) {l=l&~0x80|p[2]<<7; p+=3;} else p+=2; return l;}
	void	operator=(const SearchKey& src) {if ((type=src.type)<KT_BIN) {v=src.v; loc=PLC_EMB;} else if (type<KT_ALL) {v.ptr.p=NULL; memcpy((byte*)(this+1),src.getPtr2(),v.ptr.l=src.v.ptr.l); loc=PLC_SPTR;}}	//???
	void	copy(const SearchKey& src) {v=src.v; type=src.type; loc=src.loc==PLC_ALLC?PLC_SPTR:src.loc;}
	RC		deserialize(const void *buf,size_t l,size_t* =NULL);
	static	const ushort extKeyLen[KT_ALL];
};

/**
 * index page format descriptor constants
 */
#define	KT_VARKEY		0x0FFF				/**< variable length key */
#define	KT_VARDATA		0x8000				/**< variable length data */
#define	KT_MULTIDATA(a)	((a)|0x8000)		/**< multiple data elements corresponding to 1 key */
#define	KT_VARMULTIDATA	0xFFFF				/**< multiple data elements of variable length */
#define	KT_VARMDPINREFS	0xFFFE				/**< multiple PIN references corresponding to one key */

/**
 * index page format descriptor
 * can combine any key format with any data format
 */
struct IndexFormat
{
	uint32_t	dscr;
	IndexFormat(TREE_KT type,ushort keyLen,ushort dataLen) : dscr(uint32_t(dataLen)<<16|(keyLen&0xFFF)<<4|type) {}
	bool		isValid(size_t lPage,ushort level) const;
	TREE_KT		keyType() const {return (TREE_KT)(dscr&0x0F);}
	bool		isSeq() const {return (dscr&0x0000FFFF)==KT_UINT;}
	bool		isPrefNumKey() const {return (dscr&0x0f)<=KT_INT;}
	bool		isNumKey() const {return (dscr&0x0f)<=KT_DOUBLE;}
	bool		isFixedLenKey() const {return (dscr&0x0000FF00)<0x0000FF00;}
	bool		isVarKeyOnly() const {return (dscr&0xFFFFFFE0)==0xFFE0;}
	bool		isRefKeyOnly() const {return dscr==(0xFFE0|KT_REF);}
	bool		isKeyOnly() const {return (dscr&0xFFFF0000)==0;}
	ushort		keyLength() const {return ushort((dscr&0x0000FFF0)>>4);}
	bool		isFixedLenData() const {return (dscr&0x80000000)==0;}
	bool		isUnique() const {return dscr<=0x8000FFFF;}
	bool		isVarMultiData() const {return dscr>=0xFFF00000;}
	bool		isPinRef() const {return (dscr>>16)==KT_VARMDPINREFS;}
	ushort		dataLength() const {return ushort(dscr>>16&0x7FFF);}
	ushort		makeSubKey() const {return ushort(dscr>>16&0x0FFF);}
	void		makeInternal() {dscr=(isSeq()?sizeof(uint64_t)<<4|KT_UINT:dscr&0x0000FFFF)|sizeof(PageID)<<16;}
	bool		operator==(const IndexFormat& rhs) const {return dscr==rhs.dscr;}
	bool		operator!=(const IndexFormat& rhs) const {return dscr!=rhs.dscr;}
};

/**
 * index scan flags
 */
#define	SCAN_EXACT			0x8000
#define	SCAN_BACKWARDS		0x4000
#define	SCAN_PREFIX			0x2000
#define	SCAN_EXCLUDE_START	0x1000
#define	SCAN_EXCLUDE_END	0x0800

/**
 * index scan callback interface
 */
class IKeyCallback
{
public:
	virtual	void		newKey() = 0;
};

/**
 * index scan interface
 */
class TreeScan
{
public:
	virtual	RC			nextKey(GO_DIR=GO_NEXT,const SearchKey *skip=NULL) = 0;
	virtual	const void	*nextValue(size_t& lData,GO_DIR=GO_NEXT,const byte *skip=NULL,size_t lsk=0) = 0;
	virtual	RC			skip(ulong& nSkip,bool fBack=false) = 0;
	virtual	const		SearchKey& getKey() = 0;
	virtual	bool		hasValues() = 0;
	virtual	void		release() = 0;
	virtual	RC			rewind() = 0;
	virtual	void		destroy() = 0;
};

#define	TREE_MAX_DEPTH		12				/**< maximum index B-tree depth, with typical page fanout sufficient to represent all pins in a store */
#define	TREE_N_HIST_BUCKETS	20				/**< used for gathering index tree statistics */

class	TreeFreeData;
class	TreeFactory;
class	StoreCtx;
class	PBlock;

/**
 * page position in B-tree
 */
enum TREE_NODETYPE
{
	PITREE_LEAF,			/**< leaf page of main tree */
	PITREE_INT,				/**< internal (non-leaf) page of main tree */
	PITREE_LEAF2,			/**< leaf page of secondary tree (tree of values correponding to the same key) */
	PITREE_INT2,			/**< internal page of secondary tree */
	PITREE_1STLEV,			/**< 1 level page (parent of leaf page) of main tree */
	TREE_NODETYPE_ALL
};

/**
 * tree statistics
 */
struct CheckTreeReport
{
	ulong	depth;
	ulong	total[TREE_MAX_DEPTH];
	ulong	missing[TREE_MAX_DEPTH];
	ulong	empty[TREE_MAX_DEPTH];
	ulong	histogram[TREE_N_HIST_BUCKETS];
};

/**
 * index mode flags
 */
#define	TF_WITHDEL		0x0001			/**< index allows deletions */
#define	TF_SPLITINTX	0x0002			/**< split operations don't require separate transactions */
#define	TF_NOPOST		0x0004			/**< no tree repair operations to be posted */

/**
 * multi-key insert interface
 */
class IMultiKey
{
public:
	virtual	RC	nextKey(const SearchKey *&nk,const void *&value,ushort& lValue,bool& fMulti) = 0;
};

/**
 * tree descriptor re-connect interface
 * used in asynchronous tree repair operations and in recovery
 */
class TreeConnect
{
public:
	virtual	class Tree	*connect(uint32_t handle) = 0;
};

/**
 * main index tree descriptor interface
 */
class Tree
{
public:
	// index tree interface
	virtual	TreeFactory *getFactory() const = 0;
	virtual	IndexFormat	indexFormat() const = 0;
	virtual	ulong		getMode() const;
	virtual	PageID		startPage(const SearchKey*,int& level,bool=true,bool=false) = 0;
	virtual	PageID		prevStartPage(PageID pid) = 0;
	virtual	RC			addRootPage(const SearchKey& key,PageID& pageID,ulong level) = 0;
	virtual	RC			removeRootPage(PageID page,PageID leftmost,ulong level) = 0;
	virtual	ulong		getStamp(TREE_NODETYPE) const = 0;
	virtual	void		getStamps(ulong stamps[TREE_NODETYPE_ALL]) const = 0;
	virtual	void		advanceStamp(TREE_NODETYPE) = 0;
	virtual	bool		lock(RW_LockType,bool fTry=false) const = 0;
	virtual	void		unlock() const = 0;
	virtual TreeConnect	*persist(uint32_t& hndl) const;
	virtual	void		destroy() = 0;
public:
	// index tree search and manipulation functions
	bool				find(const SearchKey& key,void *buf,size_t &size);
	RC					countValues(const SearchKey& key,uint64_t& nValues);
	RC					insert(IMultiKey& mk);
	RC					insert(const SearchKey& key,const void *value=NULL,ushort lValue=0,bool fMulti=false);
	RC					insert(const void *value,ushort lValue,uint32_t& id,PageID& pid);
	RC					update(const SearchKey& key,const void *oldValue,ushort lOldValue,const void *newValue,ushort lNewValue);
	RC					edit(const SearchKey& key,const void *newValue,ushort lNewValue,ushort lOld,ushort sht);
	RC					remove(const SearchKey& key,const void *value=NULL,ushort lValue=0,bool fMulti=false);
	TreeScan			*scan(Session *ses,const SearchKey *start,const SearchKey *finish=NULL,ulong flgs=0,const IndexSeg *sg=NULL,unsigned nSegs=0,IKeyCallback *kc=NULL);
	static	RC			drop(PageID,StoreCtx*,TreeFreeData* =NULL);
	static	ulong		checkTree(StoreCtx*,PageID root,CheckTreeReport& res,CheckTreeReport *sec=NULL);
	StoreCtx			*getStoreCtx() const {return ctx;}
protected:
	StoreCtx			*const ctx;
	Tree(StoreCtx *ct) : ctx(ct) {}
	enum	TreeOp		{TO_READ,TO_INSERT,TO_UPDATE,TO_DELETE,TO_EDIT};		/**< used in recovery */
	PBlock				*getPage(PageID pid,ulong stamp,TREE_NODETYPE type);
	RC					insert(const SearchKey& key,const void *val,ushort lval,bool fMulti,struct TreeCtx& rtr,PageID& pid);
	friend	class		TreePageMgr;
	friend	class		TreeScanImpl;
	friend	class		TreeRQ;
	friend	class		TreeInsertRQ;
	friend	class		TreeDeleteRQ;
	friend	struct		TreeCtx;
	friend	struct		ECB;
};

/**
 * tree factory interface
 * serialization/deserialization of index information used in undo and recovery
 */
class TreeFactory
{
public:
	virtual	byte	getID() const = 0;
	virtual	byte	getParamLength() const = 0;
	virtual	void	getParams(byte *buf,const Tree& tr) const = 0;
	virtual	RC		createTree(const byte *params,byte lparams,Tree *&tree) = 0;
};

/**
 * standard 1-root page index
 * implements Tree interface
 */
class TreeStdRoot : public Tree
{
protected:
	PageID			root;
	ulong			height;
	ulong			stamps[TREE_NODETYPE_ALL];
public:
	TreeStdRoot(PageID rt,StoreCtx *ct) : Tree(ct),root(rt),height(0) {memset(stamps,0,sizeof(stamps));}
	virtual			~TreeStdRoot();
	virtual	PageID	startPage(const SearchKey*,int& level,bool,bool=false);
	virtual	PageID	prevStartPage(PageID pid);
	virtual	RC		addRootPage(const SearchKey& key,PageID& pageID,ulong level);
	virtual	RC		removeRootPage(PageID page,PageID leftmost,ulong level);
	virtual	ulong	getStamp(TREE_NODETYPE) const;
	virtual	void	getStamps(ulong stamps[TREE_NODETYPE_ALL]) const;
	virtual	void	advanceStamp(TREE_NODETYPE);
};

/**
 * global index
 * root page is stored in store control record
 * access to this index is synchronized
 * extends TreeStdRoot
 */
class TreeGlobalRoot : public TreeStdRoot, public TreeFactory
{
	mutable	RWLock		rwlock;
	mutable	RWLock		rootLock;
	const	ulong		index;
	const	IndexFormat	ifmt;
	const	ulong		mode;
	RC					setRoot(PageID,PageID);
public:
	TreeGlobalRoot(ulong idx,IndexFormat ifm,StoreCtx *ct,ulong md=0);
	virtual				~TreeGlobalRoot();
	TreeFactory			*getFactory() const;
	IndexFormat			indexFormat() const;
	ulong				getMode() const;
	PageID				startPage(const SearchKey*,int& level,bool,bool=false);
	RC					addRootPage(const SearchKey& key,PageID& pageID,ulong level);
	RC					removeRootPage(PageID page,PageID leftmost,ulong level);
	bool				lock(RW_LockType,bool fTry=false) const;
	void				unlock() const;
	void				destroy();

	RC					dropTree();

	byte				getID() const;
	byte				getParamLength() const;
	void				getParams(byte *buf,const Tree& tr) const;
	RC					createTree(const byte *params,byte lparams,Tree *&tree);
};

#define	MAXFACTORIES			20				/**< length of table of index tree factories */
#define	DEFAULT_TREERQ_SIZE		256				/**< default size of posted tree repair request hash table */

/**
 * general B-tree index manager
 */
class TreeMgr
{
	class	StoreCtx	*const ctx;
	class	TreeRQTable	*ptrt;
	TreeFactory			*factoryTable[MAXFACTORIES];
	friend	class		TreeRQ;
public:
	SharedCounter		sideLink;
	SharedCounter		traverse;
	SharedCounter		pageRead;
	SharedCounter		sibRead;
public:
	TreeMgr(StoreCtx *ct,ulong timeout);
	virtual ~TreeMgr();
	void	*operator new(size_t s,StoreCtx *ctx);
	RC		registerFactory(TreeFactory& factory);
	TreeFactory	*getFactory(byte ID) const {return ID<MAXFACTORIES?factoryTable[ID]:NULL;}
	friend	struct	TreeCtx;
	friend	class	Tree;
};

};

#endif
