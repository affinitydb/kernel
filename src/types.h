/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _TYPES_H_
#define _TYPES_H_

#include "rc.h"

using namespace AfyRC;

#ifdef WIN32

#include <stdint.h>
#include <malloc.h>
#include <algorithm>
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0403
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#define _LX_FM				"%016I64X"
#define	_LD_FM				"%I64d"
#define	_LU_FM				"%I64u"
typedef unsigned char		byte;
typedef unsigned short		ushort;
typedef unsigned long		ulong;
typedef	unsigned __int64	off64_t;
typedef HANDLE				HTHREAD;
typedef	DWORD				THREADID;
#define	getThreadId()		GetCurrentThreadId()
extern "C" _declspec(dllexport) RC convCode(DWORD);
extern "C" void				reportError(DWORD);
inline	HTHREAD	getThread() {return GetCurrentThread();}
inline	size_t	getPageSize() {SYSTEM_INFO si; GetSystemInfo(&si); return si.dwPageSize;}
inline	size_t	getSectorSize() {
	DWORD sectorSize,sectorsPerCluster,numberOfFreeClusters,totalNumberOfClusters;
	GetDiskFreeSpace(NULL,&sectorsPerCluster,&sectorSize,&numberOfFreeClusters,&totalNumberOfClusters);
	return sectorSize;
}
inline	void	*allocAligned(size_t sz,size_t) {return VirtualAlloc(NULL,sz,MEM_COMMIT,PAGE_READWRITE);}
inline	void	freeAligned(void* p) {VirtualFree(p,0,MEM_RELEASE);}
inline	RC	createThread(LPTHREAD_START_ROUTINE pRoutine,LPVOID pParam,HANDLE& thread)
				{thread = ::CreateThread(NULL,0,pRoutine,pParam,0,NULL); return thread==NULL?convCode(GetLastError()):RC_OK;}
inline	int		getNProcessors() {SYSTEM_INFO si; GetSystemInfo(&si); return si.dwNumberOfProcessors;}

typedef	unsigned __int64	TIMESTAMP;
#define	TS_DELTA			TIMESTAMP(0)
inline	void	getTimestamp(TIMESTAMP& ts) {FILETIME ft; GetSystemTimeAsFileTime(&ft); ULARGE_INTEGER li={ft.dwLowDateTime,ft.dwHighDateTime}; ts=li.QuadPart/10+TS_DELTA;}

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

#ifdef __x86_64__
#define	DEFAULT_ALIGN	16
#else
#define	DEFAULT_ALIGN	8
#endif

#elif defined(POSIX) || defined(Darwin)

#include <errno.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>
#include <string.h>
#ifndef Darwin
#include <malloc.h>
#endif
#include <unistd.h>
#include <sys/time.h>
#ifndef Darwin
#include <sys/timex.h>
#endif
#include <algorithm>
#include <wctype.h>
#define	__cdecl
#define	__forceinline inline
#define _LX_FM		"%016LX"
#define _LD_FM		"%Ld"
#define _LU_FM		"%Lu"
#define W_LD_FM		L"%Ld"
#define W_LU_FM		L"%Lu"
typedef uint8_t		byte;
typedef uint16_t	ushort;
typedef	int			HANDLE;
//typedef uint32_t	ulong;
typedef pthread_t	HTHREAD;
typedef	pthread_t	THREADID;
#define	getThreadId()	pthread_self()
extern	"C" RC		convCode(int);
extern	"C" void	reportError(int);
inline	HTHREAD		getThread() {return pthread_self();}
inline	size_t		getPageSize() {return (size_t)sysconf(_SC_PAGESIZE);}
inline	size_t		getSectorSize() {return 0x200;}		// ????
#ifndef Darwin
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
inline	RC			createThread(void *(*pRoutine)(void*),void *pParam,pthread_t& thread)
									{return convCode(pthread_create(&thread,NULL,pRoutine,pParam));}
inline	int			getNProcessors() {return (int)sysconf(_SC_NPROCESSORS_ONLN);}

typedef	uint64_t	TIMESTAMP;
#define	TS_DELTA	TIMESTAMP(0x00295e9648864000ULL)

#ifndef Darwin
inline	void		getTimestamp(TIMESTAMP& ts) {timespec tsp; clock_gettime(CLOCK_REALTIME,&tsp); 
ts=uint64_t(tsp.tv_sec)*1000000+tsp.tv_nsec/1000+TS_DELTA;}
#else
inline	void		getTimestamp(TIMESTAMP& ts) {timeval tv; gettimeofday(&tv,NULL); ts=uint64_t(tv.tv_sec)*1000000+tv.tv_usec+TS_DELTA;}
#endif

#ifdef Darwin
#include <mach/mach.h>
#define wcsncasecmp(x,y,z) wcsncmp(x,y,z)
typedef unsigned long ulong;
typedef uint64_t	off64_t;   //??int:
#endif

#ifdef __x86_64__
#define	DEFAULT_ALIGN	16
#else
#define	DEFAULT_ALIGN	8
#endif

#else

#error "Unknown platform"

#endif

using namespace std;

#include <stdlib.h>
#include <assert.h>
#include <ctype.h>

#ifdef _MSC_VER
#pragma warning(disable:4291)
#pragma warning(disable:4251)
#pragma warning(disable:4355)
#pragma	warning(disable:4181)
#pragma	warning(disable:4512)
#pragma	warning(disable:4100)
#endif

#if defined(_M_IX86) || defined(IA32) || defined(__x86_64__) || defined(_M_X64) || defined(_M_IA64)
#define _u16(a)	(*(ushort*)(a))
#define	_set_u16(a,b) (*(ushort*)(a))=ushort(b)
#elif defined(_LSBF)
#define _u16(a)	(((byte*)(a))[1]<<8|((byte*)(a))[0])
#define	_set_u16(a,b) (((byte*)(a))[0]=(byte)(b),((byte*)(a))[1]=(byte)((b)>>8))
#else
#define _u16(a)	(((byte*)(a))[0]<<8|((byte*)(a))[1])
#define	_set_u16(a,b) (((byte*)(a))[1]=(byte)(b),((byte*)(a))[0]=(byte)((b)>>8))
#endif

namespace AfyKernel
{

typedef	uint32_t		CRC;
typedef uint64_t		TXID;
typedef	uint64_t		OID;

#define	INVALID_INDEX	0xFFFF
#define	INVALID_PAGEID	0xFFFFFFFFul
#define	INVALID_FILEID	0xFF

typedef	uint8_t		FileID;
typedef uint32_t	PageID;
typedef uint16_t	PageOff;
typedef uint16_t	PageIdx;

__forceinline size_t ceil(size_t l,size_t alignment) {assert((alignment&alignment-1)==0); return l + alignment - 1 & ~(alignment - 1);}
__forceinline size_t align(size_t sht,size_t alignment) {assert((alignment&alignment-1)==0); return (sht + alignment - 1 & ~(alignment - 1)) - sht;}
__forceinline size_t floor(size_t l,size_t alignment) {assert((alignment&alignment-1)==0); return l & ~(alignment - 1);}
template<class T> __forceinline T* ceil(T* p,size_t alignment) {assert((alignment&alignment-1)==0); return (T*)((size_t)p + alignment - 1 & ~(alignment - 1));}
template<class T> __forceinline T* floor(T* p,size_t alignment) {assert((alignment&alignment-1)==0); return (T*)((size_t)p & ~(alignment - 1));}

struct PageAddr
{
	PageID		pageID;
	PageIdx		idx;
	bool		defined() const {return pageID!=INVALID_PAGEID && idx!=INVALID_INDEX;}
	bool		operator==(const PageAddr& rhs) const {return pageID==rhs.pageID && idx==rhs.idx;}
	bool		operator!=(const PageAddr& rhs) const {return pageID!=rhs.pageID || idx!=rhs.idx;}
	operator	uint32_t() const {return pageID^idx;}
	operator	OID() const;
	bool		convert(OID oid);
	static	const	PageAddr invAddr;
};

#define	PageAddrSize	(sizeof(PageID)+sizeof(PageIdx))

struct PagePtr 
{
	PageOff		offset;
	ushort		len;
};

#define	MAXPAGESPERFILE	0x00FFFFFFul

enum HEAP_TYPE {NO_HEAP,SES_HEAP,STORE_HEAP,SERVER_HEAP,HEAP_TYPE_MASK=0x03};

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

extern void		report(MsgType,const char *str,...);
extern void		initReport();
extern void		closeReport();

};

inline	void*	operator new(size_t s,AfyKernel::HEAP_TYPE allc) throw() {return AfyKernel::malloc(s,allc);}
inline	void*	operator new[](size_t s,AfyKernel::HEAP_TYPE allc) throw() {return AfyKernel::malloc(s,allc);}

#endif
