/**************************************************************************************

Copyright © 2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2010

**************************************************************************************/
#ifndef _MEM_H_
#define _MEM_H_

#include "types.h"

namespace MVStoreKernel
{

class ObjDealloc
{
	friend	class	SubAlloc;
	friend	class	Session;
	ObjDealloc		*next;
public:
	virtual	void	destroyObj() = 0;
};

class MemAlloc
{
public:
	virtual void *malloc(size_t s) = 0;
	virtual	void *memalign(size_t align,size_t s) = 0;
	virtual	void *realloc(void *p,size_t s) = 0;
	virtual	void free(void *p) = 0;
	virtual	HEAP_TYPE getAType() const = 0;
	virtual	void addObj(ObjDealloc *od);
	virtual	void release() = 0;
};


#define	SA_DEFAULT_SIZE	0x200

class SubAlloc : public MemAlloc
{
protected:
	struct SubExt {
		SubExt		*next;
		size_t		size;
	}				*extents;
	byte			*ptr;
	size_t			extentLeft;
	ObjDealloc		*chain;
	const	bool	fZero;
	MemAlloc* const	parent;
	byte			*expand(size_t s);
public:
	SubAlloc(MemAlloc *pr,size_t lbuf=SA_DEFAULT_SIZE,byte *buf=NULL,bool fZ=false) : extents(NULL),ptr(buf),extentLeft(lbuf),chain(NULL),fZero(fZ),parent(pr) {
		if (ptr==NULL && (extents=(SubExt*)(parent!=NULL?parent->malloc(lbuf+sizeof(SubExt)):MVStoreKernel::malloc(lbuf+sizeof(SubExt),SES_HEAP)))!=NULL) 
			{ptr=(byte*)(extents+1); extents->next=NULL; extents->size=lbuf+sizeof(SubExt);}
		if (fZ && ptr!=NULL) memset(ptr,0,extentLeft);
	}
	~SubAlloc() {release();}
	void operator delete(void *p) {if (p!=NULL) ((SubAlloc*)p)->parent->free(p);}
	template<typename T> T *alloc(size_t s=0) {
		size_t align=ceil(ptr,DEFAULT_ALIGN)-ptr;
		if ((s+=align+sizeof(T))>extentLeft && expand(s)==NULL) return NULL;
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
			extents=se->next; parent?parent->free(se):MVStoreKernel::free(se,SES_HEAP);
			extentLeft=(extents->size-sizeof(SubExt))%sizeof(T); ptr=(byte*)extents+extents->size-extentLeft;
		}
		return true;
	}
	void	*malloc(size_t s);
	void	*memalign(size_t,size_t);
	void	*realloc(void *p,size_t s);
	void	free(void *p);
	HEAP_TYPE	getAType() const;
	void	release();
	char	*strdup(const char *s);
	void	addObj(ObjDealloc *od);
	void	compact();
	struct	SubMark {SubExt *ext; byte *end;};
	void	mark(SubMark& sm) {sm.ext=extents; sm.end=ptr;}
	size_t	length(const SubMark& mrk);
	void	truncate(const SubMark& sm,size_t s=0);
	void	*getBuffer(size_t& left) {if (extentLeft==0) expand(0); left=extentLeft; return ptr;}
	void	setLeft(size_t left) {assert(left<=extentLeft); ptr+=extentLeft-left; extentLeft=left;}
	template<typename T> class it {
		SubExt			*ext;
		byte			*end;
		T*				ptr;
		bool			fMore;
	public:
		it(const SubAlloc& sa) : ext(sa.extents),end(sa.ptr),ptr(NULL),fMore(true) {}
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


