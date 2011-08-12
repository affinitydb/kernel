/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "startup.h"
#include "mvstoreimpl.h"
#include "dlalloc.h"

using namespace	MVStore;
using namespace MVStoreKernel;

namespace MVStoreKernel
{
	class SesMemAlloc : public MemAlloc
	{
		malloc_state	av;
	public:
		SesMemAlloc(size_t strt=MMAP_AS_MORECORE_SIZE) {memset(&av,0,sizeof(av)); av.block_size=strt;}		// round to page
		void *malloc(size_t pSize) {return mv_malloc(&av,pSize);}
		void *memalign(size_t align,size_t s) {return mv_memalign(&av,align,s);}
		void *realloc(void *pPtr, size_t pNewSize) {return mv_realloc(&av,pPtr,pNewSize);}
		void free(void *pPtr) {mv_free(&av,pPtr);}
		virtual	HEAP_TYPE getAType() const {return SES_HEAP;}
		void release() {mv_release(&av); delete this;}
	};
	class StoreMemAlloc : public MemAlloc
	{
		Mutex			lock;
		malloc_state	av;
	public:
		StoreMemAlloc(size_t strt=MMAP_AS_MORECORE_SIZE) {memset(&av,0,sizeof(av)); av.block_size=strt;}		// round to page
		void *malloc(size_t pSize) {MutexP lck(&lock); return mv_malloc(&av,pSize);}
		void *memalign(size_t align,size_t s) {MutexP lck(&lock); return mv_memalign(&av,align,s);}
		void *realloc(void *pPtr, size_t pNewSize) {MutexP lck(&lock); return mv_realloc(&av,pPtr,pNewSize);}
		void free(void *pPtr) {MutexP lck(&lock); mv_free(&av,pPtr);}
		virtual	HEAP_TYPE getAType() const {return STORE_HEAP;}
		void release() {mv_release(&av); delete this;}
	};
}

MemAlloc *MVStoreKernel::createMemAlloc(size_t startSize,bool fMulti)
{
	return fMulti?(MemAlloc*)new StoreMemAlloc(startSize):(MemAlloc*)new SesMemAlloc(startSize);
}

void* MVStoreKernel::malloc(size_t s,HEAP_TYPE alloc)
{
	Session *ses; StoreCtx *ctx;
	switch (alloc) {
	case NO_HEAP: assert(0);
	case SES_HEAP:
		if ((ses=Session::getSession())!=NULL) return ses->malloc(s);
	case STORE_HEAP: 
		if ((ctx=StoreCtx::get())!=NULL && ctx->mem!=NULL) return ctx->mem->malloc(s);
	case SERVER_HEAP:
		break;
	}
	return ::malloc(s);
}

void* MVStoreKernel::memalign(size_t a,size_t s,HEAP_TYPE alloc)
{
	Session *ses; StoreCtx *ctx;
	switch (alloc) {
	case NO_HEAP: assert(0);
	case SES_HEAP:
		if ((ses=Session::getSession())!=NULL) return ses->memalign(a,s);
	case STORE_HEAP: 
		if ((ctx=StoreCtx::get())!=NULL && ctx->mem!=NULL) return ctx->mem->memalign(a,s);
	case SERVER_HEAP:
		break;
	}
#ifdef WIN32
	return ::_aligned_malloc(s,a);
#else
	return ::memalign(a,s);
#endif
}

void* MVStoreKernel::realloc(void *p,size_t s,HEAP_TYPE alloc)
{
	Session *ses; StoreCtx *ctx;
	switch (alloc) {
	case NO_HEAP: assert(0);
	case SES_HEAP:
		if ((ses=Session::getSession())!=NULL) return ses->realloc(p,s);
	case STORE_HEAP: 
		if ((ctx=StoreCtx::get())!=NULL && ctx->mem!=NULL) return ctx->mem->realloc(p,s);
	case SERVER_HEAP:
		break;
	}
	return ::realloc(p,s);
}

char* MVStoreKernel::strdup(const char *s,HEAP_TYPE alloc)
{
	if (s==NULL) return NULL;
	size_t l=strlen(s); char *p=(char*)malloc(l+1,alloc);
	if (p!=NULL) memcpy(p,s,l+1);
	return p;
}

char* MVStoreKernel::strdup(const char *s,MemAlloc *ma)
{
	if (s==NULL) return NULL;
	size_t l=strlen(s); char *p=(char*)ma->malloc(l+1);
	if (p!=NULL) memcpy(p,s,l+1);
	return p;
}

void MVStoreKernel::free(void *p,HEAP_TYPE alloc)
{
	Session *ses; StoreCtx *ctx;
	if (p!=NULL) switch (alloc) {
	case NO_HEAP: assert(0);
	case SES_HEAP:
		if ((ses=Session::getSession())!=NULL) return ses->free(p);
	case STORE_HEAP: 
		if ((ctx=StoreCtx::get())!=NULL && ctx->mem!=NULL) return ctx->mem->free(p);
	default:
		::free(p);
	}
}

byte *OutputBuf::alloc(size_t len)
{
	if (ptr==NULL) {
		xLen=lInit; assert(cLen==0);
		if (xLen<len+lHdr) xLen+=len+lHdr;
		if ((ptr=(byte*)ma->malloc(xLen))==NULL) return NULL;
		cLen+=lHdr;
	} else if (len>xLen-cLen) {
		if ((xLen+=xLen/2)-cLen<len) xLen=cLen+len+cLen/2;
		if ((ptr=(byte*)ma->realloc(ptr,xLen))==NULL) return NULL;
	}
	assert(ptr!=NULL); byte *p=ptr+cLen; cLen+=len; return p;
}

bool OutputBuf::insert(const void *p,size_t l,size_t offset)
{
	byte *pp;
	if (offset>cLen || (pp=alloc(l))==NULL) return false;
	if (offset<cLen) memmove(ptr+offset+l,ptr+offset,cLen-offset);
	memcpy(ptr+offset,p,l); return true;
}

void MemAlloc::addObj(ObjDealloc *od)
{
}

void *SubAlloc::malloc(size_t s)
{
	if (s>extentLeft && expand(s)==NULL) return NULL;
	void *p=ptr; ptr+=s; extentLeft-=s; return p;
}

void *SubAlloc::memalign(size_t a,size_t s)
{
	do {
		byte *p=ceil(ptr,a); size_t sht=p-ptr;
		if (s+sht<=extentLeft) {ptr+=s+sht; extentLeft-=s+sht; return p;}
	} while (expand(s)!=NULL);
	return NULL;
}

void *SubAlloc::realloc(void *p,size_t s)
{
	size_t os=0;
	if (p!=NULL) {			// assume last allocation of size ptr-p
		assert((byte*)p<ptr);
		os=size_t(ptr-(byte*)p);
		if (os>=s) {ptr=(byte*)p+s; extentLeft+=os-s; return p;}
		if (s-os<=extentLeft) {ptr+=s-os; extentLeft-=s-os; return p;}
	}
	void *pp=malloc(s); if (pp!=NULL && os!=0) memcpy(pp,p,os);
	return pp;
}

void SubAlloc::free(void *p)
{
		// check end
}

HEAP_TYPE SubAlloc::getAType() const
{
	return NO_HEAP;
}
	
void SubAlloc::release()
{
	for (ObjDealloc *od=chain,*od2; od!=NULL; od=od2) {od2=od->next; od->destroyObj();}
	for (SubExt *se=extents,*se2; se!=NULL; se=se2) {se2=se->next; parent!=NULL?parent->free(se):MVStoreKernel::free(se,SES_HEAP);} 
	chain=NULL; extents=NULL; ptr=NULL; extentLeft=0;
}

char *SubAlloc::strdup(const char *s)
{
	if (s==NULL) return NULL;
	size_t l=strlen(s); char *p=(char*)malloc(l+1);
	if (p!=NULL) memcpy(p,s,l+1);
	return p;
}

void SubAlloc::addObj(ObjDealloc *od)
{
	od->next=chain; chain=od;
}

byte *SubAlloc::expand(size_t s)
{
	extentLeft=extents!=NULL?extents->size<<1:0x1000; 
	if (extentLeft<s+sizeof(SubExt)) extentLeft=s+sizeof(SubExt);
	SubExt *se=(SubExt*)(parent?parent->malloc(extentLeft):MVStoreKernel::malloc(extentLeft,SES_HEAP));
	if (se==NULL) return NULL;
	se->next=extents; se->size=extentLeft; extents=se;
	ptr=(byte*)(se+1); extentLeft-=sizeof(SubExt);
	if (fZero) memset(ptr,0,extentLeft);
	return ptr;
}

void SubAlloc::compact()
{
	if (extents!=NULL) {
		if (ptr==(byte*)(extents+1)) {
			SubExt *se=extents; extents=se->next; parent?parent->free(se):MVStoreKernel::free(se,SES_HEAP);
		} else {
			size_t s=ptr-(byte*)extents;
			extents=(SubExt*)(parent?parent->realloc(extents,s):MVStoreKernel::realloc(extents,s,SES_HEAP));
			assert(extents!=NULL); extents->size=s;
		}
		extentLeft=0;
	}
}

size_t SubAlloc::length(const SubMark& mrk)
{
	if (mrk.ext==extents) return ptr-mrk.end; 
	size_t l=ptr-(byte*)(extents+1); 
	for (SubExt *ext=extents->next; ext!=NULL; ext=ext->next) {
		if (mrk.ext!=ext) l+=ext->size-sizeof(SubExt);
		else {l+=ext->size-(mrk.end-(byte*)ext); break;}
	}
	return l;
}

void SubAlloc::truncate(const SubMark& sm,size_t s)
{
	// ???
}
