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
 * definition of platform-dependent types, constants and functions
 */
#ifndef _TYPES_H_
#define _TYPES_H_

#include "affinity.h"

using namespace Afy;

#ifdef ANDROID
#define	POSIX
#endif

#include <new>

#ifdef WIN32
/**
 * Windows
 */
#include <stdint.h>
#include <malloc.h>
#include <stdio.h>
#ifndef _WIN32_WINNT
#define _WIN32_WINNT _WIN32_WINNT_VISTA
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#define _LX_FM				"%016I64X"
#define _LS_FM				"%I64X"
#define	_LD_FM				"%I64d"
#define	_LU_FM				"%I64u"
typedef unsigned char		byte;
typedef unsigned short		ushort;
typedef	unsigned __int64	off64_t;
extern "C" void				reportError(DWORD);
inline	size_t	getPageSize() {SYSTEM_INFO si; GetSystemInfo(&si); return si.dwPageSize;}
inline	size_t	getSectorSize() {
	DWORD sectorSize,sectorsPerCluster,numberOfFreeClusters,totalNumberOfClusters;
	GetDiskFreeSpace(NULL,&sectorsPerCluster,&sectorSize,&numberOfFreeClusters,&totalNumberOfClusters);
	return sectorSize;
}
inline	void	*allocAligned(size_t sz,size_t) {return VirtualAlloc(NULL,sz,MEM_COMMIT,PAGE_READWRITE);}
inline	void	freeAligned(void* p) {VirtualFree(p,0,MEM_RELEASE);}

#define	stricmp		_stricmp
#define	strnicmp	_strnicmp
#define	strcasecmp	_stricmp
#define	wcscasecmp	_wcsicmp
#define	strncasecmp	_strnicmp
#define	wcsncasecmp	_wcsnicmp
#define	strtoll		_strtoi64
#define	strtoull	_strtoui64
#define	wcstoll		_wcstoi64
#define	wcstoull	_wcstoui64
#undef	min
#undef	max

#if defined(_M_X64) || defined(_M_AMD64) || defined(IA64)
#define	DEFAULT_ALIGN	16
#else
#define	DEFAULT_ALIGN	8
#endif

#elif defined(POSIX) || defined(__APPLE__)
/**
 * Linux, OSX (POSIX), iOS or ARM
 */
#ifdef __APPLE__
#include <TargetConditionals.h>
#define _DARWIN_USE_64_BIT_INODE
#if TARGET_OS_IPHONE==1
#define	stat64	stat
#endif
#elif defined(ANDROID)
#include <malloc.h>
#include <asm/timex.h>
#include <sched.h>
#include <time64.h>
#else
#include <malloc.h>
#include <sys/timex.h>
#endif
#include <errno.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <wctype.h>
#include <pthread.h>
#define	__cdecl
#if defined(__x86_64__) && !defined(__APPLE__) // or 64-bit ARM (what is its #define?)
#define _LX_FM		"%016lX"
#define _LS_FM		"%lX"
#define _LD_FM		"%ld"
#define _LU_FM		"%lu"
#else
#define _LX_FM		"%016llX"
#define _LS_FM		"%llX"
#define _LD_FM		"%lld"
#define _LU_FM		"%llu"
#endif
typedef uint8_t		byte;
typedef uint16_t	ushort;
typedef	int			HANDLE;
#define	INVALID_HANDLE_VALUE	(-1)
extern	"C" void	reportError(int);
inline	size_t		getPageSize() {return (size_t)sysconf(_SC_PAGESIZE);}
inline	size_t		getSectorSize() {return 0x200;}		// ????
#ifndef __APPLE__
inline	void		*allocAligned(size_t sz,size_t alignment) {return memalign(alignment,sz);}
#else
/*Allocating /deallocating aligned memory...
  Check OSX man posix_memalign for details...  
*/
inline	void		*allocAligned(size_t sz,size_t alignment) {
void * tmp;
   return posix_memalign(&tmp, alignment, sz) ? NULL : tmp; 
}
#endif

inline	void		freeAligned(void *p) {free(p);}

#ifdef __APPLE__
#define _DARWIN_USE_64_BIT_INODE
#include <mach/mach.h>
#define open64 open
#define ftruncate64 ftruncate
#define pwrite64 pwrite
#define pread64 pread
#define aio_write64 aio_write 
#define aio_read64 aio_read
#define aiocb64 aiocb
#define fdatasync(x) fsync(x)
#define wcsncasecmp(x,y,z) wcsncmp(x,y,z)
typedef uint64_t	off64_t;   //??int:
#elif defined(ANDROID)
#define pthread_yield sched_yield
#define timegm timegm64
#define open64 open
#define ftruncate64 ftruncate
#define pwrite64 pwrite
#define pread64 pread
#define aiocb64 myaio
#endif

#ifdef __x86_64__
#define	DEFAULT_ALIGN	16
#else
#define	DEFAULT_ALIGN	8
#endif

#else

#error "Unknown platform"

#endif

#include <stdlib.h>
#include <ctype.h>

#ifndef _MSC_VER
template<typename T> struct __una__ {T __v;} __attribute__((packed));
template<typename T> inline T __una_get(const T& ptr) {return ((const __una__<T> *)(void*)&ptr)->__v;}
template<typename T> inline void __una_set(T *ptr,T val) {((__una__<T> *)(void*)ptr)->__v=val;}
template<typename T> inline void __una_set(T& ptr,T val) {((__una__<T> *)(void*)&ptr)->__v=val;}
#elif defined(_M_X64) || defined(_M_IA64)
template<typename T> __forceinline T __una_get(const T& ptr) {return *(const T __unaligned *)&ptr;}
template<typename T> __forceinline void __una_set(T *ptr,T val) {*(T __unaligned*)ptr=val;}
template<typename T> __forceinline void __una_set(T& ptr,T val) {*(T __unaligned*)&ptr=val;}
#else
template<typename T> __forceinline T __una_get(const T& ptr) {return ptr;}
template<typename T> __forceinline void __una_set(T *ptr,T val) {*(T*)ptr=val;}
template<typename T> __forceinline void __una_set(T& ptr,T val) {ptr=val;}
#endif
#define _u16(a)	__una_get(*(const ushort *)(a))
#define	_set_u16(a,b) __una_set((ushort*)(a),(ushort)(b))

namespace AfyKernel
{

/**
 * platform independent type definitions
 */
#define	INVALID_INDEX	0xFFFF
#define	INVALID_PAGEID	0xFFFFFFFFul
#define	INVALID_FILEID	0xFF

typedef	uint8_t		FileID;
typedef uint32_t	PageID;
typedef uint16_t	PageOff;
typedef uint16_t	PageIdx;

/**
 * alignment helper functions
 */
__forceinline size_t ceil(size_t l,size_t alignment) {assert((alignment&alignment-1)==0); return l + alignment - 1 & ~(alignment - 1);}
__forceinline size_t align(size_t sht,size_t alignment) {assert((alignment&alignment-1)==0); return (sht + alignment - 1 & ~(alignment - 1)) - sht;}
__forceinline size_t floor(size_t l,size_t alignment) {assert((alignment&alignment-1)==0); return l & ~(alignment - 1);}
template<class T> __forceinline T* ceil(T* p,size_t alignment) {assert((alignment&alignment-1)==0); return (T*)((size_t)p + alignment - 1 & ~(alignment - 1));}
template<class T> __forceinline T* floor(T* p,size_t alignment) {assert((alignment&alignment-1)==0); return (T*)((size_t)p & ~(alignment - 1));}

/**
 * heap page address
 * consists of page identifier and slot number
 */
struct PageAddr
{
	PageID		pageID;
	PageIdx		idx;
	bool		defined() const {return pageID!=INVALID_PAGEID && idx!=INVALID_INDEX;}
	bool		operator==(const PageAddr& rhs) const {return pageID==rhs.pageID && idx==rhs.idx;}
	bool		operator!=(const PageAddr& rhs) const {return pageID!=rhs.pageID || idx!=rhs.idx;}
	operator	uint32_t() const {return pageID^idx;}
	operator	uint64_t() const;
	bool		convert(uint64_t oid);
	static	const	PageAddr noAddr;
};

#define	PageAddrSize	(sizeof(PageID)+sizeof(PageIdx))

struct PagePtr 
{
	PageOff		offset;
	ushort		len;
};

#define	MAXPAGESPERFILE	0x00FFFFFFul		/**< maximum number of pages per one data file */

/**
 * types of allocated memory
 * stored in Value::flags
 */
enum HEAP_TYPE {NO_HEAP,PAGE_HEAP,SES_HEAP,STORE_HEAP,HEAP_TYPE_MASK=0x03};

/** 
 * memory allocation and deallocation helper functions
 */
extern	void*	malloc(size_t,HEAP_TYPE);
extern	void*	memalign(size_t,size_t,HEAP_TYPE);
extern	void*	realloc(void*,size_t,HEAP_TYPE);
extern	bool	check(const void*,size_t,HEAP_TYPE);
extern	char	*strdup(const char *s,HEAP_TYPE);
extern	char	*strdup(const char *s,class MemAlloc*);
extern	void	free(void*,HEAP_TYPE);

#define	UTF8_SUFFIX			".utf8"
#define	UTF8_DEFAULT_LOCALE	"en_US.utf8"

extern	char	*forceCaseUTF8(const char *str,size_t ilen,uint32_t& olen,class MemAlloc* =NULL,char *extBuf=NULL,bool fUpper=false);

extern void		initReport();
extern void		closeReport();

};

inline	void*	operator new(size_t s,AfyKernel::HEAP_TYPE allc) throw() {return AfyKernel::malloc(s,allc);}
inline	void*	operator new[](size_t s,AfyKernel::HEAP_TYPE allc) throw() {return AfyKernel::malloc(s,allc);}

#endif
