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

Written by Mark Venguerov 2010-2014

**************************************************************************************/

/**
 * memory allocation/deallocation control structures
 */
#ifndef _MEM_H_
#define _MEM_H_

#include "types.h"
#include "afysync.h"

namespace AfyKernel
{

/**
 * object resource release interface
 * used when bulk object deallocation is performed
 */
class ObjDealloc
{
	friend	class	StackAlloc;
	friend	class	Session;
	ObjDealloc		*next;
public:
	virtual	void	destroyObj() = 0;
};

/**
 * truncate mode
 * @see MemAlloc::truncate
 */
enum TruncMode
{
	TR_REL_ALL, TR_REL_ALLBUTONE, TR_REL_NONE
};

/**
 * standard memory control interface
 * can be implemented by session heap, store heap, server-wide heap
 * or special sub-allocators
 */
class MemAlloc : public IMemAlloc
{
public:
	virtual	void		*memalign(size_t align,size_t s) = 0;
	virtual	HEAP_TYPE	getAType() const = 0;
	virtual	void		addObj(ObjDealloc *od);
	virtual	void		truncate(TruncMode tm=TR_REL_ALLBUTONE,const void *mark=NULL);
};

class SharedAlloc : public MemAlloc
{
public:
	void *malloc(size_t pSize);
	void *memalign(size_t align,size_t s);
	void *realloc(void *pPtr, size_t pNewSize, size_t pOldSize=0);
	void free(void *pPtr);
	HEAP_TYPE getAType() const;
};

extern SharedAlloc sharedAlloc;

#define	NBLOCK_SHARED_QUEUES	10
#define	SES_MEM_BLOCK_QUEUES	6
#define	SES_MEM_BLOCK_MAX		32

class Session;

class BlockAlloc : public MemAlloc
{
	const	uint32_t	blockSize	:31;
	const	uint32_t	fSes		:1;
	uint32_t			left;
	BlockAlloc			*ext;
public:
	BlockAlloc(uint32_t	sz,bool fS) : blockSize(sz),fSes(fS?1:0),left(sz-sizeof(BlockAlloc)),ext(NULL) {assert((sz-1&sz)==0);}
	void *malloc(size_t pSize);
	void *memalign(size_t align,size_t s);
	void *realloc(void *pPtr, size_t pNewSize, size_t pOldSize=0);
	void free(void *pPtr);
	HEAP_TYPE getAType() const;
	static	BlockAlloc *allocBlock(size_t sz,Session *ses=NULL,bool fSes=false);
public:
	struct SharedBlockAlloc {
		LIFO	*queues[NBLOCK_SHARED_QUEUES];
		SharedBlockAlloc();
	};
};

#define	SA_DEFAULT_SIZE	0x200
#define	SA_MAX_SIZE		0x40000000		//1Gb

/**
 * memory sub-allocator
 * implements MemAlloc interface
 */
class StackAlloc : public MemAlloc
{
protected:
	struct SubExt {
		SubExt		*next;
		size_t		size;
	}				*extents;
	byte			*ptr;
	size_t			extentLeft;
	size_t			total;
	ObjDealloc		*chain;
	const	bool	fZero;
	MemAlloc* const	parent;
	byte			*expand(size_t s);
public:
	struct	SubMark {
		SubExt	*ext;
		byte	*end; 
		size_t	total;
	};
public:
	StackAlloc(MemAlloc *pr,size_t lbuf=SA_DEFAULT_SIZE,byte *buf=NULL,bool fZ=false) : extents(NULL),ptr(buf),extentLeft(lbuf),total(0),chain(NULL),fZero(fZ),parent(pr) {
		if (ptr==NULL && (extents=(SubExt*)(parent!=NULL?parent->malloc(lbuf+sizeof(SubExt)):AfyKernel::malloc(lbuf+sizeof(SubExt),SES_HEAP)))!=NULL) 
			{ptr=(byte*)(extents+1); extents->next=NULL; extents->size=lbuf+sizeof(SubExt); total=lbuf+sizeof(SubExt);}
		if (fZ && ptr!=NULL) memset(ptr,0,extentLeft);
	}
	~StackAlloc() {truncate(TR_REL_ALL);}
	void operator delete(void *p) {if (p!=NULL) ((StackAlloc*)p)->parent->free(p);}
	template<typename T> T *alloc(size_t s=0) {
		size_t align=ceil(ptr,DEFAULT_ALIGN)-ptr;
		if ((s+=align+sizeof(T))>extentLeft) {if (expand(s)!=NULL) {s-=align; align=0;} else return NULL;}
		byte *p=ptr+align; ptr+=s; extentLeft-=s; return (T*)p;
	}
	template<typename T> T *alloc0(size_t s=0) {
		if ((s+=sizeof(T))>extentLeft && expand(s)==NULL) return NULL;
		byte *p=ptr; ptr+=s; extentLeft-=s; return (T*)p;
	}
	template<typename T> bool pop(T *ret=NULL) {
		SubExt *se=extents; ptrdiff_t sht=ptr-(byte*)(se+1); 
		if (sht==0) return false; assert(sht%sizeof(T)==0 && chain==NULL);
		if (ret!=NULL) memcpy(ret,ptr-sizeof(T),sizeof(T));
		if (sht>ptrdiff_t(sizeof(T)) || se->next==NULL) {
			ptr-=sizeof(T); extentLeft+=sizeof(T);
		} else {
			extents=se->next; parent?parent->free(se):AfyKernel::free(se,SES_HEAP);
			extentLeft=(extents->size-sizeof(SubExt))%sizeof(T); ptr=(byte*)extents+extents->size-extentLeft;
		}
		return true;
	}
	void	*malloc(size_t s);
	void	*memalign(size_t,size_t);
	void	*realloc(void *p,size_t s,size_t old=0);
	void	free(void *p);
	HEAP_TYPE	getAType() const;
	char	*strdup(const char *s);
	void	addObj(ObjDealloc *od);
	void	compact();
	void	mark(SubMark& sm) {sm.ext=extents; sm.end=ptr; sm.total=total;}
	size_t	length(const SubMark& mrk);
	void	truncate(TruncMode tm=TR_REL_ALLBUTONE,const void *mark=NULL);
	size_t	getTotal() const {return total-extentLeft;}
	template<typename T> class it {
		SubExt			*ext;
		byte			*end;
		T*				ptr;
		bool			fMore;
	public:
		it(const StackAlloc& sa) : ext(sa.extents),end(sa.ptr),ptr(NULL),fMore(true) {}
		it(const SubMark& sm) : ext(sm.ext),end(sm.end),ptr(NULL),fMore(true) {}
		bool operator++() {
			if (fMore) {
				if (ptr==NULL) {if (ext==NULL||end==(byte*)(ext+1)&&(ext=ext->next)==NULL) fMore=false; else ptr=(T*)(ext+1);}
				else if (++ptr+1>(T*)end) {if ((ext=ext->next)==NULL) fMore=false; else {ptr=(T*)(ext+1); end=(byte*)ext+ext->size;}}
			}
			return fMore;
		}
		operator T*() const {return ptr;}
	};
};

};

#endif


