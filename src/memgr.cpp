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

#include "startup.h"
#include "pin.h"
#include "dlalloc.h"

using namespace	Afy;
using namespace AfyKernel;

namespace AfyKernel
{
	class SesMemAlloc : public MemAlloc
	{
		malloc_state	av;
	public:
		SesMemAlloc(size_t strt=MMAP_AS_MORECORE_SIZE) {memset(&av,0,sizeof(av)); av.block_size=strt;}		// round to page
		void *malloc(size_t pSize) {return afy_malloc(&av,pSize);}
		void *memalign(size_t align,size_t s) {return afy_memalign(&av,align,s);}
		void *realloc(void *pPtr, size_t pNewSize, size_t pOldSize=0) {return afy_realloc(&av,pPtr,pNewSize);}
		void free(void *pPtr) {afy_free(&av,pPtr);}
		virtual	HEAP_TYPE getAType() const {return SES_HEAP;}
		void truncate(TruncMode tm,const void *) {
			if (tm==TR_REL_ALL) {afy_release(&av); delete this;}
			else {
				//???
			}
		}
	};
	class StoreMemAlloc : public MemAlloc
	{
		Mutex			lock;
		malloc_state	av;
	public:
		StoreMemAlloc(size_t strt=MMAP_AS_MORECORE_SIZE) {memset(&av,0,sizeof(av)); av.block_size=strt;}		// round to page
		void *malloc(size_t pSize) {MutexP lck(&lock); return afy_malloc(&av,pSize);}
		void *memalign(size_t align,size_t s) {MutexP lck(&lock); return afy_memalign(&av,align,s);}
		void *realloc(void *pPtr, size_t pNewSize, size_t pOldSize=0) {MutexP lck(&lock); return afy_realloc(&av,pPtr,pNewSize);}
		void free(void *pPtr) {MutexP lck(&lock); afy_free(&av,pPtr);}
		virtual	HEAP_TYPE getAType() const {return STORE_HEAP;}
		void truncate(TruncMode tm,const void *) {
			if (tm==TR_REL_ALL) {afy_release(&av); delete this;}
			else {
				//???
			}
		}
	};


	void *SharedAlloc::malloc(size_t pSize) {return ::malloc(pSize);}
	void *SharedAlloc::memalign(size_t align,size_t s) {return NULL;} //???
	void *SharedAlloc::realloc(void *pPtr, size_t pNewSize, size_t pOldSize) {return ::realloc(pPtr,pNewSize);}
	void SharedAlloc::free(void *pPtr) {::free(pPtr);}
	HEAP_TYPE SharedAlloc::getAType() const {return NO_HEAP;}

	SharedAlloc sharedAlloc;
}

MemAlloc *AfyKernel::createMemAlloc(size_t startSize,bool fMulti)
{
	try {return fMulti?(MemAlloc*)new StoreMemAlloc(startSize):(MemAlloc*)new SesMemAlloc(startSize);} catch (...) {return NULL;}
}

void* AfyKernel::malloc(size_t s,HEAP_TYPE alloc)
{
	Session *ses; StoreCtx *ctx;
	switch (alloc) {
	default: assert(0);
	case SES_HEAP:
		if ((ses=Session::getSession())!=NULL) return ses->malloc(s);
	case STORE_HEAP: 
		if ((ctx=StoreCtx::get())!=NULL && ctx->mem!=NULL) return ctx->mem->malloc(s);
	case NO_HEAP:
		break;
	}
	try {return ::malloc(s);} catch(...) {return NULL;}
}

void* AfyKernel::memalign(size_t a,size_t s,HEAP_TYPE alloc)
{
	Session *ses; StoreCtx *ctx;
	switch (alloc) {
	default: assert(0);
	case SES_HEAP:
		if ((ses=Session::getSession())!=NULL) return ses->memalign(a,s);
	case STORE_HEAP: 
		if ((ctx=StoreCtx::get())!=NULL && ctx->mem!=NULL) return ctx->mem->memalign(a,s);
	case NO_HEAP:
		break;
	}
	try {
#ifdef WIN32
		return ::_aligned_malloc(s,a);
#elif !defined(__APPLE__)
		return ::memalign(a,s);
#else
		void * tmp; 
		return posix_memalign(&tmp,a,s) ? NULL : tmp ;
#endif
	} catch (...) {return NULL;}
}

void* AfyKernel::realloc(void *p,size_t s,HEAP_TYPE alloc)
{
	Session *ses; StoreCtx *ctx;
	switch (alloc) {
	default: assert(0);
	case SES_HEAP:
		if ((ses=Session::getSession())!=NULL) return ses->realloc(p,s);
	case STORE_HEAP: 
		if ((ctx=StoreCtx::get())!=NULL && ctx->mem!=NULL) return ctx->mem->realloc(p,s);
	case NO_HEAP:
		break;
	}
	try {return ::realloc(p,s);} catch (...) {return NULL;}
}

char* AfyKernel::strdup(const char *s,HEAP_TYPE alloc)
{
	if (s==NULL) return NULL;
	size_t l=strlen(s); char *p=(char*)malloc(l+1,alloc);
	if (p!=NULL) memcpy(p,s,l+1);
	return p;
}

char* AfyKernel::strdup(const char *s,MemAlloc *ma)
{
	if (s==NULL) return NULL;
	size_t l=strlen(s); char *p=(char*)ma->malloc(l+1);
	if (p!=NULL) memcpy(p,s,l+1);
	return p;
}

void AfyKernel::free(void *p,HEAP_TYPE alloc)
{
	Session *ses; StoreCtx *ctx;
	if (p!=NULL) switch (alloc) {
	case NO_HEAP: case PAGE_HEAP: assert(0);
	case SES_HEAP:
		if ((ses=Session::getSession())!=NULL) return ses->free(p);
	case STORE_HEAP: 
		if ((ctx=StoreCtx::get())!=NULL && ctx->mem!=NULL) return ctx->mem->free(p);
	default:
		::free(p);
	}
}

void MemAlloc::addObj(ObjDealloc *od)
{
}

void MemAlloc::truncate(TruncMode tm,const void *mark)
{
}

void *StackAlloc::malloc(size_t s)
{
	size_t align=ceil(ptr,DEFAULT_ALIGN)-ptr;
	if (s+align>extentLeft) {align=0; if (expand(s)==NULL) return NULL;}
	void *p=ptr+align; ptr+=s+align; extentLeft-=s+align; return p;
}

void *StackAlloc::memalign(size_t a,size_t s)
{
	do {
		byte *p=ceil(ptr,a); size_t sht=p-ptr;
		if (s+sht<=extentLeft) {ptr+=s+sht; extentLeft-=s+sht; return p;}
	} while (expand(s)!=NULL);
	return NULL;
}

void *StackAlloc::realloc(void *p,size_t s,size_t old)
{
	if (p==NULL) return malloc(s);
	if ((byte*)p<ptr && size_t(ptr-(byte*)p)==old) {
		if (old>=s) {ptr=(byte*)p+s; extentLeft+=old-s; return p;}
		if (s-old<=extentLeft) {ptr+=s-old; extentLeft-=s-old; return p;}
	}
	void *pp=malloc(s); if (pp!=NULL && old!=0) memcpy(pp,p,old); return pp;
}

void StackAlloc::free(void *p)
{
		// check end
}

HEAP_TYPE StackAlloc::getAType() const
{
	return NO_HEAP;
}

char *StackAlloc::strdup(const char *s)
{
	if (s==NULL) return NULL;
	size_t l=strlen(s); char *p=(char*)malloc(l+1);
	if (p!=NULL) memcpy(p,s,l+1);
	return p;
}

void StackAlloc::addObj(ObjDealloc *od)
{
	od->next=chain; chain=od;
}

byte *StackAlloc::expand(size_t s)
{
	extentLeft=extents==NULL?SA_DEFAULT_SIZE:extents->size>=SA_MAX_SIZE?extents->size:extents->size<<1; 
	if (extentLeft<s+sizeof(SubExt)) extentLeft=s+sizeof(SubExt);
	SubExt *se=(SubExt*)(parent?parent->malloc(extentLeft):AfyKernel::malloc(extentLeft,SES_HEAP));
	if (se==NULL) return NULL;
	se->next=extents; se->size=extentLeft; extents=se; total+=extentLeft;
	ptr=(byte*)(se+1); extentLeft-=sizeof(SubExt);
	if (fZero) memset(ptr,0,extentLeft);
	return ptr;
}

void StackAlloc::compact()
{
	if (extents!=NULL) {
		if (ptr==(byte*)(extents+1)) {
			SubExt *se=extents; extents=se->next; parent?parent->free(se):AfyKernel::free(se,SES_HEAP);
		} else {
			size_t s=ptr-(byte*)extents;
			extents=(SubExt*)(parent?parent->realloc(extents,s):AfyKernel::realloc(extents,s,SES_HEAP));
			assert(extents!=NULL); extents->size=s;
		}
		extentLeft=0;
	}
}

size_t StackAlloc::length(const SubMark& mrk)
{
	if (mrk.ext==extents) return ptr-mrk.end; 
	size_t l=ptr-(byte*)(extents+1); 
	for (SubExt *ext=extents->next; ext!=NULL; ext=ext->next) {
		if (mrk.ext!=ext) l+=ext->size-sizeof(SubExt);
		else {l+=ext->size-(mrk.end-(byte*)ext); break;}
	}
	return l;
}

void StackAlloc::truncate(TruncMode tm,const void *mrk)
{
	const SubMark *sm=(SubMark*)mrk;
	for (SubExt *next; extents!=NULL && (sm==NULL || extents!=sm->ext); extents=next) {
		if ((next=extents->next)==NULL && tm==TR_REL_ALLBUTONE) break;
//	for (ObjDealloc *od=chain,*od2; od!=NULL; od=od2) {od2=od->next; od->destroyObj();}		//????
		if (parent) parent->free(extents); else AfyKernel::free(extents,SES_HEAP);
	}
	if (extents==NULL) {ptr=NULL; extentLeft=total=0;}
	else if (sm!=NULL) {
		assert(sm->end>=(byte*)(extents+1) && sm->end<=(byte*)extents+extents->size);
		ptr=sm->end; extentLeft=((byte*)extents+extents->size)-ptr; total=sm->total;
	} else {
		ptr=(byte*)(extents+1); extentLeft=extents->size-sizeof(SubExt); total=extents->size;
	}
}

//------------------------------------------------------------------------------------------------------

static BlockAlloc::SharedBlockAlloc sharedBlocks;

BlockAlloc::SharedBlockAlloc::SharedBlockAlloc()
{
	for (unsigned i=0; i<NBLOCK_SHARED_QUEUES; i++) {
		if ((queues[i]=::new Pool(NULL,0x400>>i))==NULL) throw RC_NOMEM;	//???
	}
}

BlockAlloc *BlockAlloc::allocBlock(size_t s,Session *ses,bool fSes)
{
	void *p=NULL; uint32_t sz=max(nextP2((uint32_t)s),128u);
	if (fSes) {assert(ses!=NULL); p=ses->malloc(sz);}
	else {
		unsigned idx=pop(sz-1)-7;
		if (ses!=NULL && idx<SES_MEM_BLOCK_QUEUES && ses->memCache!=NULL && (p=ses->memCache[idx])!=NULL)
			ses->memCache[idx]=((SesMemCache*)p)->next; 
		else 
			p=idx<NBLOCK_SHARED_QUEUES?sharedBlocks.queues[idx]->alloc(sz):NULL;
	}
	return p!=NULL?new(p) BlockAlloc(sz,fSes):NULL;
}

void *BlockAlloc::malloc(size_t sz)
{
	byte *ptr=(byte*)this+blockSize-left; size_t align=ceil(ptr,DEFAULT_ALIGN)-ptr;
	if (sz+align<=left) {left-=uint32_t(sz+align); return ptr+align;}
	// extend ???
	return NULL;
}

void *BlockAlloc::memalign(size_t align,size_t s)
{
	return NULL; //???
}

void *BlockAlloc::realloc(void *p, size_t s, size_t old)
{
	if (p!=NULL) {
		byte *ptr=(byte*)this+blockSize-left;
		if ((byte*)p<ptr && size_t(ptr-(byte*)p)==old) {
			if (old>=s) {left+=uint32_t(old-s); return p;}
			if (s-old<=left) {left-=uint32_t(s-old); return p;}
		}
	}
	void *pp=malloc(s); if (pp!=NULL && old!=0) memcpy(pp,p,old); return pp;
}

void BlockAlloc::free(void *p)
{
	if (p==(byte*)this+sizeof(BlockAlloc)) {
		unsigned idx=pop(blockSize-1)-7; assert(idx<NBLOCK_SHARED_QUEUES); Session *ses;
		if ((fSes || idx<SES_MEM_BLOCK_QUEUES) && (ses=Session::getSession())!=NULL) {
			if (fSes) {ses->free(this); return;}
			if (ses->memCache!=NULL && (ses->memCache[idx]==NULL || ses->memCache[idx]->cnt<SES_MEM_BLOCK_MAX))
				{SesMemCache *sm=(SesMemCache*)p; sm->next=ses->memCache[idx]; sm->cnt=sm->next!=NULL?sm->next->cnt+1:1; return;}
		}
		if (!fSes) sharedBlocks.queues[idx]->dealloc(this);
	}
}

HEAP_TYPE BlockAlloc::getAType() const
{
	return NO_HEAP;
}
