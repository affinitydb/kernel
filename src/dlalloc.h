#ifndef _DLALLOC_H
#define _DLALLOC_H

/*
  This is a version (aka dlmalloc) of malloc/free/realloc written by
  Doug Lea and released to the public domain.  Use, modify, and
  redistribute this code without permission or acknowledgement in any
  way you wish.  Send questions, comments, complaints, performance
  data, etc to dl@cs.oswego.edu

* VERSION 2.7.2 Sat Aug 17 09:07:30 2002  Doug Lea  (dl at gee)

   Note: There may be an updated version of this malloc obtainable at
           ftp://gee.cs.oswego.edu/pub/misc/malloc.c
         Check before installing!

* Quickstart

  This library is all in one file to simplify the most common usage:
  ftp it, compile it (-O), and link it into another program. All
  of the compile-time options default to reasonable values for use on
  most unix platforms. Compile -DWIN32 for reasonable defaults on windows.
  You might later want to step through various compile-time and dynamic
  tuning options.

  For convenience, an include file for code using this malloc is at:
     ftp://gee.cs.oswego.edu/pub/misc/malloc-2.7.1.h
  You don't really need this .h file unless you call functions not
  defined in your system include files.  The .h file contains only the
  excerpts from this file needed for using this malloc on ANSI C/C++
  systems, so long as you haven't changed compile-time options about
  naming and tuning parameters.  If you do, then you can create your
  own malloc.h that does include all settings by cutting at the point
  indicated below.

* Why use this malloc?

  This is not the fastest, most space-conserving, most portable, or
  most tunable malloc ever written. However it is among the fastest
  while also being among the most space-conserving, portable and tunable.
  Consistent balance across these factors results in a good general-purpose
  allocator for malloc-intensive programs.

  The main properties of the algorithms are:
  * For large (>= 512 bytes) requests, it is a pure best-fit allocator,
    with ties normally decided via FIFO (i.e. least recently used).
  * For small (<= 64 bytes by default) requests, it is a caching
    allocator, that maintains pools of quickly recycled chunks.
  * In between, and for combinations of large and small requests, it does
    the best it can trying to meet both goals at once.
  * For very large requests (>= 128KB by default), it relies on system
    memory mapping facilities, if supported.

  For a longer but slightly out of date high-level description, see
     http://gee.cs.oswego.edu/dl/html/malloc.html

  You may already by default be using a C library containing a malloc
  that is  based on some version of this malloc (for example in
  linux). You might still want to use the one in this file in order to
  customize settings or to avoid overheads associated with library
  versions.

* Contents, described in more detail in "description of public routines" below.

  Standard (ANSI/SVID/...)  functions:
    malloc(size_t n);
    calloc(size_t n_elements, size_t element_size);
    free(Void_t* p);
    realloc(Void_t* p, size_t n);
    memalign(size_t alignment, size_t n);
    valloc(size_t n);
    mallinfo()
    mallopt(int parameter_number, int parameter_value)

  Additional functions:
    independent_calloc(size_t n_elements, size_t size, Void_t* chunks[]);
    independent_comalloc(size_t n_elements, size_t sizes[], Void_t* chunks[]);
    pvalloc(size_t n);
    cfree(Void_t* p);
    malloc_trim(size_t pad);
    malloc_usable_size(Void_t* p);
    malloc_stats();

* Vital statistics:

  Supported pointer representation:       4 or 8 bytes
  Supported size_t  representation:       4 or 8 bytes 
       Note that size_t is allowed to be 4 bytes even if pointers are 8.
       You can adjust this by defining INTERNAL_SIZE_T

  Alignment:                              2 * sizeof(size_t) (default)
       (i.e., 8 byte alignment with 4byte size_t). This suffices for
       nearly all current machines and C compilers. However, you can
       define MALLOC_ALIGNMENT to be wider than this if necessary.

  Minimum overhead per allocated chunk:   4 or 8 bytes
       Each malloced chunk has a hidden word of overhead holding size
       and status information.

  Minimum allocated size: 4-byte ptrs:  16 bytes    (including 4 overhead)
                          8-byte ptrs:  24/32 bytes (including, 4/8 overhead)

       When a chunk is freed, 12 (for 4byte ptrs) or 20 (for 8 byte
       ptrs but 4 byte size) or 24 (for 8/8) additional bytes are
       needed; 4 (8) for a trailing size field and 8 (16) bytes for
       free list pointers. Thus, the minimum allocatable size is
       16/24/32 bytes.

       Even a request for zero bytes (i.e., malloc(0)) returns a
       pointer to something of the minimum allocatable size.

       The maximum overhead wastage (i.e., number of extra bytes
       allocated than were requested in malloc) is less than or equal
       to the minimum size, except for requests >= mmap_threshold that
       are serviced via mmap(), where the worst case wastage is 2 *
       sizeof(size_t) bytes plus the remainder from a system page (the
       minimal mmap unit); typically 4096 or 8192 bytes.

  Maximum allocated size:  4-byte size_t: 2^32 minus about two pages 
                           8-byte size_t: 2^64 minus about two pages

       It is assumed that (possibly signed) size_t values suffice to
       represent chunk sizes. `Possibly signed' is due to the fact
       that `size_t' may be defined on a system as either a signed or
       an unsigned type. The ISO C standard says that it must be
       unsigned, but a few systems are known not to adhere to this.
       Additionally, even when size_t is unsigned, sbrk (which is by
       default used to obtain memory from system) accepts signed
       arguments, and may not be able to handle size_t-wide arguments
       with negative sign bit.  Generally, values that would
       appear as negative after accounting for overhead and alignment
       are supported only via mmap(), which does not have this
       limitation.

       Requests for sizes outside the allowed range will perform an optional
       failure action and then return null. (Requests may also
       also fail because a system is out of memory.)

  Thread-safety: NOT thread-safe unless USE_MALLOC_LOCK defined

       When USE_MALLOC_LOCK is defined, wrappers are created to
       surround every public call with either a pthread mutex or
       a win32 spinlock (depending on WIN32). This is not
       especially fast, and can be a major bottleneck.
       It is designed only to provide minimal protection
       in concurrent environments, and to provide a basis for
       extensions.  If you are using malloc in a concurrent program,
       you would be far better off obtaining ptmalloc, which is
       derived from a version of this malloc, and is well-tuned for
       concurrent programs. (See http://www.malloc.de) Note that
       even when USE_MALLOC_LOCK is defined, you can can guarantee
       full thread-safety only if no threads acquire memory through 
       direct calls to MORECORE or other system-level allocators.

  Compliance: I believe it is compliant with the 1997 Single Unix Specification
       (See http://www.opennc.org). Also SVID/XPG, ANSI C, and probably 
       others as well.

* Synopsis of compile-time options:

    People have reported using previous versions of this malloc on all
    versions of Unix, sometimes by tweaking some of the defines
    below. It has been tested most extensively on Solaris and
    Linux. It is also reported to work on WIN32 platforms.
    People also report using it in stand-alone embedded systems.

    The implementation is in straight, hand-tuned ANSI C.  It is not
    at all modular. (Sorry!)  It uses a lot of macros.  To be at all
    usable, this code should be compiled using an optimizing compiler
    (for example gcc -O3) that can simplify expressions and control
    paths. (FAQ: some macros import variables as arguments rather than
    declare locals because people reported that some debuggers
    otherwise get confused.)

    OPTION                     DEFAULT VALUE

    Compilation Environment options:

    __STD_C                    derived from C compiler defines
    WIN32                      NOT defined
    HAVE_MEMCPY                defined
    USE_MEMCPY                 1 if HAVE_MEMCPY is defined
    HAVE_MMAP                  defined as 1 
    MMAP_CLEARS                1
    HAVE_MREMAP                0 unless linux defined
    malloc_getpagesize         derived from system #includes, or 4096 if not
    HAVE_USR_INCLUDE_MALLOC_H  NOT defined
    LACKS_UNISTD_H             NOT defined unless WIN32
    LACKS_SYS_PARAM_H          NOT defined unless WIN32
    LACKS_SYS_MMAN_H           NOT defined unless WIN32
    LACKS_FCNTL_H              NOT defined

    Changing default word sizes:

    INTERNAL_SIZE_T            size_t
    MALLOC_ALIGNMENT           2 * sizeof(INTERNAL_SIZE_T)
    PTR_UINT                   unsigned long
    CHUNK_SIZE_T               unsigned long

    Configuration and functionality options:

    USE_DL_PREFIX              NOT defined
    USE_PUBLIC_MALLOC_WRAPPERS NOT defined
    USE_MALLOC_LOCK            NOT defined
    DEBUG                      NOT defined
    REALLOC_ZERO_BYTES_FREES   NOT defined
    MALLOC_FAILURE_ACTION      errno = ENOMEM, if __STD_C defined, else no-op
    TRIM_FASTBINS              0
    FIRST_SORTED_BIN_SIZE      512

    Options for customizing MORECORE:

    MORECORE                   sbrk
    MORECORE_CONTIGUOUS        1 
    MORECORE_CANNOT_TRIM       NOT defined
    MMAP_AS_MORECORE_SIZE      (1024 * 1024) 

    Tuning options that are also dynamically changeable via mallopt:

    DEFAULT_MXFAST             64
    DEFAULT_TRIM_THRESHOLD     256 * 1024
    DEFAULT_TOP_PAD            0
    DEFAULT_MMAP_THRESHOLD     256 * 1024
    DEFAULT_MMAP_MAX           65536

    There are several other #defined constants and macros that you
    probably don't want to touch unless you are extending or adapting malloc.
*/

typedef struct malloc_state *mstate;

/*
  WIN32 sets up defaults for MS environment and compilers.
  Otherwise defaults are for unix.
*/

/* #define WIN32 */

#ifdef WIN32

#define	DEBUG	_DEBUG

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <malloc.h>

/* Win32 doesn't supply or need the following headers */
#define LACKS_UNISTD_H
#define LACKS_SYS_PARAM_H
#define LACKS_SYS_MMAN_H

/* no sbrk, mmap only! */
#define MORECORE(a,b) ((void*)(-1))
#define MORECORE_CONTIGUOUS 0
#define MORECORE_FAILURE    ((void*)(-1))
//#define MORECORE_CANNOT_TRIM

/* Use the supplied emulation of mmap and munmap */
#define HAVE_MMAP 1
#define MUNMAP_FAILURE  (-1)
#define MMAP_CLEARS 1

/* These values don't really matter in windows mmap emulation */
#define MAP_PRIVATE 1
#define MAP_ANONYMOUS 2
#define PROT_READ 1
#define PROT_WRITE 2

/* Emulation functions defined at the end of this file */

static long getpagesize(void);
static long getregionsize(void);
static void *mmap(void *ptr, long size, long prot, long type, long handle, long arg);
static long munmap(void *ptr, long size);
static void vminfo (unsigned long*free, unsigned long*reserved, unsigned long*committed);
static int cpuinfo (int whole, unsigned long*kernel, unsigned long*user);

#endif

/*
  __STD_C should be nonzero if using ANSI-standard C compiler, a C++
  compiler, or a C compiler sufficiently close to ANSI to get away
  with it.
*/

#ifndef __STD_C
#if defined(__STDC__) || defined(_cplusplus) || defined(__cplusplus)
#define __STD_C     1
#else
#define __STD_C     0
#endif 
#endif /*__STD_C*/


/*
  Void_t* is the pointer type that malloc should say it returns
*/

#ifndef Void_t
#if (__STD_C || defined(WIN32))
#define Void_t      void
#else
#define Void_t      char
#endif
#endif /*Void_t*/

#if __STD_C
#include <stddef.h>   /* for size_t */
#else
#include <sys/types.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* define LACKS_UNISTD_H if your system does not have a <unistd.h>. */

/* #define  LACKS_UNISTD_H */

#ifndef LACKS_UNISTD_H
#include <unistd.h>
#endif

/* define LACKS_SYS_PARAM_H if your system does not have a <sys/param.h>. */

/* #define  LACKS_SYS_PARAM_H */


#include <stdio.h>    /* needed for malloc_stats */
#include <errno.h>    /* needed for optional MALLOC_FAILURE_ACTION */


/*
  Debugging:

  Because freed chunks may be overwritten with bookkeeping fields, this
  malloc will often die when freed memory is overwritten by user
  programs.  This can be very effective (albeit in an annoying way)
  in helping track down dangling pointers.

  If you compile with -DDEBUG, a number of assertion checks are
  enabled that will catch more memory errors. You probably won't be
  able to make much sense of the actual assertion errors, but they
  should help you locate incorrectly overwritten memory.  The
  checking is fairly extensive, and will slow down execution
  noticeably. Calling malloc_stats or mallinfo with DEBUG set will
  attempt to check every non-mmapped allocated and free chunk in the
  course of computing the summmaries. (By nature, mmapped regions
  cannot be checked very much automatically.)

  Setting DEBUG may also be helpful if you are trying to modify
  this code. The assertions in the check routines spell out in more
  detail the assumptions and invariants underlying the algorithms.

  Setting DEBUG does NOT provide an automated mechanism for checking
  that all accesses to malloced memory stay within their
  bounds. However, there are several add-ons and adaptations of this
  or other mallocs available that do this.
*/

#if DEBUG
#include <assert.h>
#else
#undef assert
#define assert(x) ((void)0)
#endif

/*
  The unsigned integer type used for comparing any two chunk sizes.
  This should be at least as wide as size_t, but should not be signed.
*/

#ifndef CHUNK_SIZE_T
#define CHUNK_SIZE_T unsigned long
#endif

/* 
  The unsigned integer type used to hold addresses when they are are
  manipulated as integers. Except that it is not defined on all
  systems, intptr_t would suffice.
*/
#ifndef PTR_UINT
#define PTR_UINT unsigned long
#endif


/*
  INTERNAL_SIZE_T is the word-size used for internal bookkeeping
  of chunk sizes.

  The default version is the same as size_t.

  While not strictly necessary, it is best to define this as an
  unsigned type, even if size_t is a signed type. This may avoid some
  artificial size limitations on some systems.

  On a 64-bit machine, you may be able to reduce malloc overhead by
  defining INTERNAL_SIZE_T to be a 32 bit `unsigned int' at the
  expense of not being able to handle more than 2^32 of malloced
  space. If this limitation is acceptable, you are encouraged to set
  this unless you are on a platform requiring 16byte alignments. In
  this case the alignment requirements turn out to negate any
  potential advantages of decreasing size_t word size.

  Implementors: Beware of the possible combinations of:
     - INTERNAL_SIZE_T might be signed or unsigned, might be 32 or 64 bits,
       and might be the same width as int or as long
     - size_t might have different width and signedness as INTERNAL_SIZE_T
     - int and long might be 32 or 64 bits, and might be the same width
  To deal with this, most comparisons and difference computations
  among INTERNAL_SIZE_Ts should cast them to CHUNK_SIZE_T, being
  aware of the fact that casting an unsigned int to a wider long does
  not sign-extend. (This also makes checking for negative numbers
  awkward.) Some of these casts result in harmless compiler warnings
  on some systems.
*/

#ifndef INTERNAL_SIZE_T
#define INTERNAL_SIZE_T size_t
#endif

/* The corresponding word size */
#define SIZE_SZ                (sizeof(INTERNAL_SIZE_T))



/*
  MALLOC_ALIGNMENT is the minimum alignment for malloc'ed chunks.
  It must be a power of two at least 2 * SIZE_SZ, even on machines
  for which smaller alignments would suffice. It may be defined as
  larger than this though. Note however that code and data structures
  are optimized for the case of 8-byte alignment.
*/


#ifndef MALLOC_ALIGNMENT
#define MALLOC_ALIGNMENT       (2 * SIZE_SZ)
#endif

/* The corresponding bit mask value */
#define MALLOC_ALIGN_MASK      (MALLOC_ALIGNMENT - 1)



/*
  REALLOC_ZERO_BYTES_FREES should be set if a call to
  realloc with zero bytes should be the same as a call to free.
  Some people think it should. Otherwise, since this malloc
  returns a unique pointer for malloc(0), so does realloc(p, 0).
*/

/*   #define REALLOC_ZERO_BYTES_FREES */

/*
  TRIM_FASTBINS controls whether free() of a very small chunk can
  immediately lead to trimming. Setting to true (1) can reduce memory
  footprint, but will almost always slow down programs that use a lot
  of small chunks.

  Define this only if you are willing to give up some speed to more
  aggressively reduce system-level memory footprint when releasing
  memory in programs that use many small chunks.  You can get
  essentially the same effect by setting MXFAST to 0, but this can
  lead to even greater slowdowns in programs using many small chunks.
  TRIM_FASTBINS is an in-between compile-time option, that disables
  only those chunks bordering topmost memory from being placed in
  fastbins.
*/

#ifndef TRIM_FASTBINS
#define TRIM_FASTBINS  0
#endif


/*
  USE_DL_PREFIX will prefix all public routines with the string 'dl'.
  This is necessary when you only want to use this malloc in one part 
  of a program, using your regular system malloc elsewhere.
*/

#define USE_DL_PREFIX


/*
  If USE_PUBLIC_MALLOC_WRAPPERS is defined, every public routine is
  actually a wrapper function that first calls MALLOC_PREACTION, then
  calls the internal routine, and follows it with
  MALLOC_POSTACTION. This is needed for locking, but you can also use
  this, without USE_MALLOC_LOCK, for purposes of interception,
  instrumentation, etc. It is a sad fact that using wrappers often
  noticeably degrades performance of malloc-intensive programs.
*/

#ifdef USE_MALLOC_LOCK
#define USE_PUBLIC_MALLOC_WRAPPERS
#else
/* #define USE_PUBLIC_MALLOC_WRAPPERS */
#endif


/* 
   Two-phase name translation.
   All of the actual routines are given mangled names.
   When wrappers are used, they become the public callable versions.
   When DL_PREFIX is used, the callable names are prefixed.
*/

#ifndef USE_PUBLIC_MALLOC_WRAPPERS
#define cALLOc      public_cALLOc
#define fREe        public_fREe
#define cFREe       public_cFREe
#define mALLOc      public_mALLOc
#define mEMALIGn    public_mEMALIGn
#define rEALLOc     public_rEALLOc
#define vALLOc      public_vALLOc
#define pVALLOc     public_pVALLOc
#define mALLINFo    public_mALLINFo
#define mALLOPt     public_mALLOPt
#define mTRIm       public_mTRIm
#define mSTATs      public_mSTATs
#define mUSABLe     public_mUSABLe
#define iCALLOc     public_iCALLOc
#define iCOMALLOc   public_iCOMALLOc
#define	rELEASe     public_rELEASe
#endif

#ifdef USE_DL_PREFIX
#define public_cALLOc    mv_calloc
#define public_fREe      mv_free
#define public_cFREe     mv_cfree
#define public_mALLOc    mv_malloc
#define public_mEMALIGn  mv_memalign
#define public_rEALLOc   mv_realloc
#define public_vALLOc    mv_valloc
#define public_pVALLOc   mv_pvalloc
#define public_mALLINFo  mv_mallinfo
#define public_mALLOPt   mv_mallopt
#define public_mTRIm     mv_malloc_trim
#define public_mSTATs    mv_malloc_stats
#define public_mUSABLe   mv_malloc_usable_size
#define public_iCALLOc   mv_independent_calloc
#define public_iCOMALLOc mv_independent_comalloc
#define	public_rELEASe   mv_release
#else /* USE_DL_PREFIX */
#define public_cALLOc    calloc
#define public_fREe      free
#define public_cFREe     cfree
#define public_mALLOc    malloc
#define public_mEMALIGn  memalign
#define public_rEALLOc   realloc
#define public_vALLOc    valloc
#define public_pVALLOc   pvalloc
#define public_mALLINFo  mallinfo
#define public_mALLOPt   mallopt
#define public_mTRIm     malloc_trim
#define public_mSTATs    malloc_stats
#define public_mUSABLe   malloc_usable_size
#define public_iCALLOc   independent_calloc
#define public_iCOMALLOc independent_comalloc
#define	public_rELEASe   release
#endif /* USE_DL_PREFIX */


/*
  HAVE_MEMCPY should be defined if you are not otherwise using
  ANSI STD C, but still have memcpy and memset in your C library
  and want to use them in calloc and realloc. Otherwise simple
  macro versions are defined below.

  USE_MEMCPY should be defined as 1 if you actually want to
  have memset and memcpy called. People report that the macro
  versions are faster than libc versions on some systems.
  
  Even if USE_MEMCPY is set to 1, loops to copy/clear small chunks
  (of <= 36 bytes) are manually unrolled in realloc and calloc.
*/

#define HAVE_MEMCPY

#ifndef USE_MEMCPY
#ifdef HAVE_MEMCPY
#define USE_MEMCPY 1
#else
#define USE_MEMCPY 0
#endif
#endif


#if (__STD_C || defined(HAVE_MEMCPY))

#ifdef WIN32
/* On Win32 memset and memcpy are already declared in windows.h */
#else
#if __STD_C
void* memset(void*, int, size_t);
void* memcpy(void*, const void*, size_t);
#else
Void_t* memset();
Void_t* memcpy();
#endif
#endif
#endif

/*
  MALLOC_FAILURE_ACTION is the action to take before "return 0" when
  malloc fails to be able to return memory, either because memory is
  exhausted or because of illegal arguments.
  
  By default, sets errno if running on STD_C platform, else does nothing.  
*/

#ifndef MALLOC_FAILURE_ACTION
#if __STD_C
#define MALLOC_FAILURE_ACTION \
   errno = ENOMEM;

#else
#define MALLOC_FAILURE_ACTION
#endif
#endif

/*
  MORECORE-related declarations. By default, rely on sbrk
*/


#ifdef LACKS_UNISTD_H
#if !defined(__FreeBSD__) && !defined(__OpenBSD__) && !defined(__NetBSD__)
#if __STD_C
extern Void_t*     sbrk(ptrdiff_t);
#else
extern Void_t*     sbrk();
#endif
#endif
#endif

/*
  MORECORE is the name of the routine to call to obtain more memory
  from the system.  See below for general guidance on writing
  alternative MORECORE functions, as well as a version for WIN32 and a
  sample version for pre-OSX macos.
*/

#ifndef MORECORE
#define MORECORE(a,b) ((void*)(-1))
#endif

/*
  MORECORE_FAILURE is the value returned upon failure of MORECORE
  as well as mmap. Since it cannot be an otherwise valid memory address,
  and must reflect values of standard sys calls, you probably ought not
  try to redefine it.
*/

#ifndef MORECORE_FAILURE
#define MORECORE_FAILURE (-1)
#endif

/*
  If MORECORE_CONTIGUOUS is true, take advantage of fact that
  consecutive calls to MORECORE with positive arguments always return
  contiguous increasing addresses.  This is true of unix sbrk.  Even
  if not defined, when regions happen to be contiguous, malloc will
  permit allocations spanning regions obtained from different
  calls. But defining this when applicable enables some stronger
  consistency checks and space efficiencies. 
*/

#ifndef MORECORE_CONTIGUOUS
#define MORECORE_CONTIGUOUS 0
#endif

/*
  Define MORECORE_CANNOT_TRIM if your version of MORECORE
  cannot release space back to the system when given negative
  arguments. This is generally necessary only if you are using
  a hand-crafted MORECORE function that cannot handle negative arguments.
*/

//#define MORECORE_CANNOT_TRIM


/*
  Define HAVE_MMAP as true to optionally make malloc() use mmap() to
  allocate very large blocks.  These will be returned to the
  operating system immediately after a free(). Also, if mmap
  is available, it is used as a backup strategy in cases where
  MORECORE fails to provide space from system.

  This malloc is best tuned to work with mmap for large requests.
  If you do not have mmap, operations involving very large chunks (1MB
  or so) may be slower than you'd like.
*/

#ifndef HAVE_MMAP
#define HAVE_MMAP 1
#endif

#if HAVE_MMAP
/* 
   Standard unix mmap using /dev/zero clears memory so calloc doesn't
   need to.
*/

#ifndef MMAP_CLEARS
#define MMAP_CLEARS 1
#endif

#else /* no mmap */
#ifndef MMAP_CLEARS
#define MMAP_CLEARS 0
#endif
#endif


/* 
   MMAP_AS_MORECORE_SIZE is the minimum mmap size argument to use if
   sbrk fails, and mmap is used as a backup (which is done only if
   HAVE_MMAP).  The value must be a multiple of page size.  This
   backup strategy generally applies only when systems have "holes" in
   address space, so sbrk cannot perform contiguous expansion, but
   there is still space available on system.  On systems for which
   this is known to be useful (i.e. most linux kernels), this occurs
   only when programs allocate huge amounts of memory.  Between this,
   and the fact that mmap regions tend to be limited, the size should
   be large, to avoid too many mmap calls and thus avoid running out
   of kernel resources.
*/

#ifndef MMAP_AS_MORECORE_SIZE
#define MMAP_AS_MORECORE_SIZE (1024 * 1024)
#endif

#ifndef MMAP_MAX_BLOCK_SIZE
#define MMAP_MAX_BLOCK_SIZE (64 * 1024 * 1024)
#endif

/*
  Define HAVE_MREMAP to make realloc() use mremap() to re-allocate
  large blocks.  This is currently only possible on Linux with
  kernel versions newer than 1.3.77.
*/

#ifndef HAVE_MREMAP
#ifdef linux
#define HAVE_MREMAP 1
#else
#define HAVE_MREMAP 0
#endif

#endif /* HAVE_MMAP */


/*
  The system page size. To the extent possible, this malloc manages
  memory from the system in page-size units.  Note that this value is
  cached during initialization into a field of malloc_state. So even
  if malloc_getpagesize is a function, it is only called once.

  The following mechanics for getpagesize were adapted from bsd/gnu
  getpagesize.h. If none of the system-probes here apply, a value of
  4096 is used, which should be OK: If they don't apply, then using
  the actual value probably doesn't impact performance.
*/


#ifndef malloc_getpagesize

#ifndef LACKS_UNISTD_H
#  include <unistd.h>
#endif

#  ifdef _SC_PAGESIZE         /* some SVR4 systems omit an underscore */
#    ifndef _SC_PAGE_SIZE
#      define _SC_PAGE_SIZE _SC_PAGESIZE
#    endif
#  endif

#  ifdef _SC_PAGE_SIZE
#    define malloc_getpagesize sysconf(_SC_PAGE_SIZE)
#  else
#    if defined(BSD) || defined(DGUX) || defined(HAVE_GETPAGESIZE)
       extern size_t getpagesize();
#      define malloc_getpagesize getpagesize()
#    else
#      ifdef WIN32 /* use supplied emulation of getpagesize */
#        define malloc_getpagesize getpagesize() 
#      else
#        ifndef LACKS_SYS_PARAM_H
#          include <sys/param.h>
#        endif
#        ifdef EXEC_PAGESIZE
#          define malloc_getpagesize EXEC_PAGESIZE
#        else
#          ifdef NBPG
#            ifndef CLSIZE
#              define malloc_getpagesize NBPG
#            else
#              define malloc_getpagesize (NBPG * CLSIZE)
#            endif
#          else
#            ifdef NBPC
#              define malloc_getpagesize NBPC
#            else
#              ifdef PAGESIZE
#                define malloc_getpagesize PAGESIZE
#              else /* just guess */
#                define malloc_getpagesize (4096) 
#              endif
#            endif
#          endif
#        endif
#      endif
#    endif
#  endif
#endif

/*
  This version of malloc supports the standard SVID/XPG mallinfo
  routine that returns a struct containing usage properties and
  statistics. It should work on any SVID/XPG compliant system that has
  a /usr/include/malloc.h defining struct mallinfo. (If you'd like to
  install such a thing yourself, cut out the preliminary declarations
  as described above and below and save them in a malloc.h file. But
  there's no compelling reason to bother to do this.)

  The main declaration needed is the mallinfo struct that is returned
  (by-copy) by mallinfo().  The SVID/XPG malloinfo struct contains a
  bunch of fields that are not even meaningful in this version of
  malloc.  These fields are are instead filled by mallinfo() with
  other numbers that might be of interest.

  HAVE_USR_INCLUDE_MALLOC_H should be set if you have a
  /usr/include/malloc.h file that includes a declaration of struct
  mallinfo.  If so, it is included; else an SVID2/XPG2 compliant
  version is declared below.  These must be precisely the same for
  mallinfo() to work.  The original SVID version of this struct,
  defined on most systems with mallinfo, declares all fields as
  ints. But some others define as unsigned long. If your system
  defines the fields using a type of different width than listed here,
  you must #include your system version and #define
  HAVE_USR_INCLUDE_MALLOC_H.
*/

#ifndef WIN32
#define HAVE_USR_INCLUDE_MALLOC_H
#endif

#ifdef HAVE_USR_INCLUDE_MALLOC_H
#include "/usr/include/malloc.h"
#else

/* SVID2/XPG mallinfo structure */

struct mallinfo {
  int arena;    /* non-mmapped space allocated from system */
  int ordblks;  /* number of free chunks */
  int smblks;   /* number of fastbin blocks */
  int hblks;    /* number of mmapped regions */
  int hblkhd;   /* space in mmapped regions */
  int usmblks;  /* maximum total allocated space */
  int fsmblks;  /* space available in freed fastbin blocks */
  int uordblks; /* total allocated space */
  int fordblks; /* total free space */
  int keepcost; /* top-most, releasable (via malloc_trim) space */
};

/*
  SVID/XPG defines four standard parameter numbers for mallopt,
  normally defined in malloc.h.  Only one of these (M_MXFAST) is used
  in this malloc. The others (M_NLBLKS, M_GRAIN, M_KEEP) don't apply,
  so setting them has no effect. But this malloc also supports other
  options in mallopt described below.
*/
#endif

/* ---------- description of public routines ------------ */

/*
  malloc(size_t n)
  Returns a pointer to a newly allocated chunk of at least n bytes, or null
  if no space is available. Additionally, on failure, errno is
  set to ENOMEM on ANSI C systems.

  If n is zero, malloc returns a minumum-sized chunk. (The minimum
  size is 16 bytes on most 32bit systems, and 24 or 32 bytes on 64bit
  systems.)  On most systems, size_t is an unsigned type, so calls
  with negative arguments are interpreted as requests for huge amounts
  of space, which will often fail. The maximum supported value of n
  differs across systems, but is in all cases less than the maximum
  representable value of a size_t.
*/
Void_t*  public_mALLOc(mstate, size_t);

/*
  free(Void_t* p)
  Releases the chunk of memory pointed to by p, that had been previously
  allocated using malloc or a related routine such as realloc.
  It has no effect if p is null. It can have arbitrary (i.e., bad!)
  effects if p has already been freed.

  Unless disabled (using mallopt), freeing very large spaces will
  when possible, automatically trigger operations that give
  back unused memory to the system, thus reducing program footprint.
*/
void     public_fREe(mstate, Void_t*);

/*
  calloc(size_t n_elements, size_t element_size);
  Returns a pointer to n_elements * element_size bytes, with all locations
  set to zero.
*/
Void_t*  public_cALLOc(mstate, size_t, size_t);

/*
  realloc(Void_t* p, size_t n)
  Returns a pointer to a chunk of size n that contains the same data
  as does chunk p up to the minimum of (n, p's size) bytes, or null
  if no space is available. 

  The returned pointer may or may not be the same as p. The algorithm
  prefers extending p when possible, otherwise it employs the
  equivalent of a malloc-copy-free sequence.

  If p is null, realloc is equivalent to malloc.  

  If space is not available, realloc returns null, errno is set (if on
  ANSI) and p is NOT freed.

  if n is for fewer bytes than already held by p, the newly unused
  space is lopped off and freed if possible.  Unless the #define
  REALLOC_ZERO_BYTES_FREES is set, realloc with a size argument of
  zero (re)allocates a minimum-sized chunk.

  Large chunks that were internally obtained via mmap will always
  be reallocated using malloc-copy-free sequences unless
  the system supports MREMAP (currently only linux).

  The old unix realloc convention of allowing the last-free'd chunk
  to be used as an argument to realloc is not supported.
*/
Void_t*  public_rEALLOc(mstate, Void_t*, size_t);

/*
  memalign(size_t alignment, size_t n);
  Returns a pointer to a newly allocated chunk of n bytes, aligned
  in accord with the alignment argument.

  The alignment argument should be a power of two. If the argument is
  not a power of two, the nearest greater power is used.
  8-byte alignment is guaranteed by normal malloc calls, so don't
  bother calling memalign with an argument of 8 or less.

  Overreliance on memalign is a sure way to fragment space.
*/
Void_t*  public_mEMALIGn(mstate, size_t, size_t);

/*
  valloc(size_t n);
  Equivalent to memalign(pagesize, n), where pagesize is the page
  size of the system. If the pagesize is unknown, 4096 is used.
*/
Void_t*  public_vALLOc(mstate, size_t);


/*
  mallopt(int parameter_number, int parameter_value)
  Sets tunable parameters The format is to provide a
  (parameter-number, parameter-value) pair.  mallopt then sets the
  corresponding parameter to the argument value if it can (i.e., so
  long as the value is meaningful), and returns 1 if successful else
  0.  SVID/XPG/ANSI defines four standard param numbers for mallopt,
  normally defined in malloc.h.  Only one of these (M_MXFAST) is used
  in this malloc. The others (M_NLBLKS, M_GRAIN, M_KEEP) don't apply,
  so setting them has no effect. But this malloc also supports four
  other options in mallopt. See below for details.  Briefly, supported
  parameters are as follows (listed defaults are for "typical"
  configurations).

  Symbol            param #   default    allowed param values
  M_MXFAST          1         64         0-80  (0 disables fastbins)
  M_TRIM_THRESHOLD -1         256*1024   any   (-1U disables trimming)
  M_TOP_PAD        -2         0          any  
  M_MMAP_THRESHOLD -3         256*1024   any   (or 0 if no MMAP support)
  M_MMAP_MAX       -4         65536      any   (0 disables use of mmap)
*/
int      public_mALLOPt(mstate, int, int);


/*
  mallinfo()
  Returns (by copy) a struct containing various summary statistics:

  arena:     current total non-mmapped bytes allocated from system 
  ordblks:   the number of free chunks 
  smblks:    the number of fastbin blocks (i.e., small chunks that
               have been freed but not use resused or consolidated)
  hblks:     current number of mmapped regions 
  hblkhd:    total bytes held in mmapped regions 
  usmblks:   the maximum total allocated space. This will be greater
                than current total if trimming has occurred.
  fsmblks:   total bytes held in fastbin blocks 
  uordblks:  current total allocated space (normal or mmapped)
  fordblks:  total free space 
  keepcost:  the maximum number of bytes that could ideally be released
               back to system via malloc_trim. ("ideally" means that
               it ignores page restrictions etc.)

  Because these fields are ints, but internal bookkeeping may
  be kept as longs, the reported values may wrap around zero and 
  thus be inaccurate.
*/
struct mallinfo public_mALLINFo(mstate);

/*
  independent_calloc(size_t n_elements, size_t element_size, Void_t* chunks[]);

  independent_calloc is similar to calloc, but instead of returning a
  single cleared space, it returns an array of pointers to n_elements
  independent elements that can hold contents of size elem_size, each
  of which starts out cleared, and can be independently freed,
  realloc'ed etc. The elements are guaranteed to be adjacently
  allocated (this is not guaranteed to occur with multiple callocs or
  mallocs), which may also improve cache locality in some
  applications.

  The "chunks" argument is optional (i.e., may be null, which is
  probably the most typical usage). If it is null, the returned array
  is itself dynamically allocated and should also be freed when it is
  no longer needed. Otherwise, the chunks array must be of at least
  n_elements in length. It is filled in with the pointers to the
  chunks.

  In either case, independent_calloc returns this pointer array, or
  null if the allocation failed.  If n_elements is zero and "chunks"
  is null, it returns a chunk representing an array with zero elements
  (which should be freed if not wanted).

  Each element must be individually freed when it is no longer
  needed. If you'd like to instead be able to free all at once, you
  should instead use regular calloc and assign pointers into this
  space to represent elements.  (In this case though, you cannot
  independently free elements.)
  
  independent_calloc simplifies and speeds up implementations of many
  kinds of pools.  It may also be useful when constructing large data
  structures that initially have a fixed number of fixed-sized nodes,
  but the number is not known at compile time, and some of the nodes
  may later need to be freed. For example:

  struct Node { int item; struct Node* next; };
  
  struct Node* build_list() {
    struct Node** pool;
    int n = read_number_of_nodes_needed();
    if (n <= 0) return 0;
    pool = (struct Node**)(independent_calloc(n, sizeof(struct Node), 0);
    if (pool == 0) die(); 
    // organize into a linked list... 
    struct Node* first = pool[0];
    for (i = 0; i < n-1; ++i) 
      pool[i]->next = pool[i+1];
    free(pool);     // Can now free the array (or not, if it is needed later)
    return first;
  }
*/
Void_t** public_iCALLOc(mstate, size_t, size_t, Void_t**);

/*
  independent_comalloc(size_t n_elements, size_t sizes[], Void_t* chunks[]);

  independent_comalloc allocates, all at once, a set of n_elements
  chunks with sizes indicated in the "sizes" array.    It returns
  an array of pointers to these elements, each of which can be
  independently freed, realloc'ed etc. The elements are guaranteed to
  be adjacently allocated (this is not guaranteed to occur with
  multiple callocs or mallocs), which may also improve cache locality
  in some applications.

  The "chunks" argument is optional (i.e., may be null). If it is null
  the returned array is itself dynamically allocated and should also
  be freed when it is no longer needed. Otherwise, the chunks array
  must be of at least n_elements in length. It is filled in with the
  pointers to the chunks.

  In either case, independent_comalloc returns this pointer array, or
  null if the allocation failed.  If n_elements is zero and chunks is
  null, it returns a chunk representing an array with zero elements
  (which should be freed if not wanted).
  
  Each element must be individually freed when it is no longer
  needed. If you'd like to instead be able to free all at once, you
  should instead use a single regular malloc, and assign pointers at
  particular offsets in the aggregate space. (In this case though, you 
  cannot independently free elements.)

  independent_comallac differs from independent_calloc in that each
  element may have a different size, and also that it does not
  automatically clear elements.

  independent_comalloc can be used to speed up allocation in cases
  where several structs or objects must always be allocated at the
  same time.  For example:

  struct Head { ... }
  struct Foot { ... }

  void send_message(char* msg) {
    int msglen = strlen(msg);
    size_t sizes[3] = { sizeof(struct Head), msglen, sizeof(struct Foot) };
    void* chunks[3];
    if (independent_comalloc(3, sizes, chunks) == 0)
      die();
    struct Head* head = (struct Head*)(chunks[0]);
    char*        body = (char*)(chunks[1]);
    struct Foot* foot = (struct Foot*)(chunks[2]);
    // ...
  }

  In general though, independent_comalloc is worth using only for
  larger values of n_elements. For small values, you probably won't
  detect enough difference from series of malloc calls to bother.

  Overuse of independent_comalloc can increase overall memory usage,
  since it cannot reuse existing noncontiguous small chunks that
  might be available for some of the elements.
*/
Void_t** public_iCOMALLOc(mstate, size_t, size_t*, Void_t**);


/*
  pvalloc(size_t n);
  Equivalent to valloc(minimum-page-that-holds(n)), that is,
  round up n to nearest pagesize.
 */
Void_t*  public_pVALLOc(mstate, size_t);

/*
  cfree(Void_t* p);
  Equivalent to free(p).

  cfree is needed/defined on some systems that pair it with calloc,
  for odd historical reasons (such as: cfree is used in example 
  code in the first edition of K&R).
*/
void     public_cFREe(mstate, Void_t*);

/*
  malloc_trim(size_t pad);

  If possible, gives memory back to the system (via negative
  arguments to sbrk) if there is unused memory at the `high' end of
  the malloc pool. You can call this after freeing large blocks of
  memory to potentially reduce the system-level memory requirements
  of a program. However, it cannot guarantee to reduce memory. Under
  some allocation patterns, some large free blocks of memory will be
  locked between two used chunks, so they cannot be given back to
  the system.
  
  The `pad' argument to malloc_trim represents the amount of free
  trailing space to leave untrimmed. If this argument is zero,
  only the minimum amount of memory to maintain internal data
  structures will be left (one page or less). Non-zero arguments
  can be supplied to maintain enough trailing space to service
  future expected allocations without having to re-obtain memory
  from the system.
  
  Malloc_trim returns 1 if it actually released any memory, else 0.
  On systems that do not support "negative sbrks", it will always
  rreturn 0.
*/
int      public_mTRIm(mstate, size_t);

/*
  malloc_usable_size(Void_t* p);

  Returns the number of bytes you can actually use in
  an allocated chunk, which may be more than you requested (although
  often not) due to alignment and minimum size constraints.
  You can use this many bytes without worrying about
  overwriting other allocated objects. This is not a particularly great
  programming practice. malloc_usable_size can be more useful in
  debugging and assertions, for example:

  p = malloc(n);
  assert(malloc_usable_size(p) >= 256);

*/
size_t   public_mUSABLe(Void_t*);

/*
  release();

  ...
*/

void     public_rELEASe(mstate);

/*
  malloc_stats();
  Prints on stderr the amount of space obtained from the system (both
  via sbrk and mmap), the maximum amount (which may be more than
  current if malloc_trim and/or munmap got called), and the current
  number of bytes allocated via malloc (or realloc, etc) but not yet
  freed. Note that this is the number of bytes allocated, not the
  number requested. It will be larger than the number requested
  because of alignment and bookkeeping overhead. Because it includes
  alignment wastage as being in use, this figure may be greater than
  zero even when no user-level chunks are allocated.

  The reported current and maximum system memory can be inaccurate if
  a program makes other calls to system memory allocation functions
  (normally sbrk) outside of malloc.

  malloc_stats prints only the most commonly interesting statistics.
  More information can be obtained by calling mallinfo.

*/
void     public_mSTATs(mstate);

/* mallopt tuning options */

/*
  M_MXFAST is the maximum request size used for "fastbins", special bins
  that hold returned chunks without consolidating their spaces. This
  enables future requests for chunks of the same size to be handled
  very quickly, but can increase fragmentation, and thus increase the
  overall memory footprint of a program.

  This malloc manages fastbins very conservatively yet still
  efficiently, so fragmentation is rarely a problem for values less
  than or equal to the default.  The maximum supported value of MXFAST
  is 80. You wouldn't want it any higher than this anyway.  Fastbins
  are designed especially for use with many small structs, objects or
  strings -- the default handles structs/objects/arrays with sizes up
  to 16 4byte fields, or small strings representing words, tokens,
  etc. Using fastbins for larger objects normally worsens
  fragmentation without improving speed.

  M_MXFAST is set in REQUEST size units. It is internally used in
  chunksize units, which adds padding and alignment.  You can reduce
  M_MXFAST to 0 to disable all use of fastbins.  This causes the malloc
  algorithm to be a closer approximation of fifo-best-fit in all cases,
  not just for larger requests, but will generally cause it to be
  slower.
*/


/* M_MXFAST is a standard SVID/XPG tuning option, usually listed in malloc.h */
#ifndef M_MXFAST
#define M_MXFAST            1    
#endif

#ifndef DEFAULT_MXFAST
#define DEFAULT_MXFAST     64
#endif


/*
  M_TRIM_THRESHOLD is the maximum amount of unused top-most memory
  to keep before releasing via malloc_trim in free().

  Automatic trimming is mainly useful in long-lived programs.
  Because trimming via sbrk can be slow on some systems, and can
  sometimes be wasteful (in cases where programs immediately
  afterward allocate more large chunks) the value should be high
  enough so that your overall system performance would improve by
  releasing this much memory.

  The trim threshold and the mmap control parameters (see below)
  can be traded off with one another. Trimming and mmapping are
  two different ways of releasing unused memory back to the
  system. Between these two, it is often possible to keep
  system-level demands of a long-lived program down to a bare
  minimum. For example, in one test suite of sessions measuring
  the XF86 X server on Linux, using a trim threshold of 128K and a
  mmap threshold of 192K led to near-minimal long term resource
  consumption.

  If you are using this malloc in a long-lived program, it should
  pay to experiment with these values.  As a rough guide, you
  might set to a value close to the average size of a process
  (program) running on your system.  Releasing this much memory
  would allow such a process to run in memory.  Generally, it's
  worth it to tune for trimming rather tham memory mapping when a
  program undergoes phases where several large chunks are
  allocated and released in ways that can reuse each other's
  storage, perhaps mixed with phases where there are no such
  chunks at all.  And in well-behaved long-lived programs,
  controlling release of large blocks via trimming versus mapping
  is usually faster.

  However, in most programs, these parameters serve mainly as
  protection against the system-level effects of carrying around
  massive amounts of unneeded memory. Since frequent calls to
  sbrk, mmap, and munmap otherwise degrade performance, the default
  parameters are set to relatively high values that serve only as
  safeguards.

  The trim value must be greater than page size to have any useful
  effect.  To disable trimming completely, you can set to 
  (unsigned long)(-1)

  Trim settings interact with fastbin (MXFAST) settings: Unless
  TRIM_FASTBINS is defined, automatic trimming never takes place upon
  freeing a chunk with size less than or equal to MXFAST. Trimming is
  instead delayed until subsequent freeing of larger chunks. However,
  you can still force an attempted trim by calling malloc_trim.

  Also, trimming is not generally possible in cases where
  the main arena is obtained via mmap.

  Note that the trick some people use of mallocing a huge space and
  then freeing it at program startup, in an attempt to reserve system
  memory, doesn't have the intended effect under automatic trimming,
  since that memory will immediately be returned to the system.
*/

#define M_TRIM_THRESHOLD       -1

#ifndef DEFAULT_TRIM_THRESHOLD
#define DEFAULT_TRIM_THRESHOLD (256 * 1024)
#endif

/*
  M_TOP_PAD is the amount of extra `padding' space to allocate or
  retain whenever sbrk is called. It is used in two ways internally:

  * When sbrk is called to extend the top of the arena to satisfy
  a new malloc request, this much padding is added to the sbrk
  request.

  * When malloc_trim is called automatically from free(),
  it is used as the `pad' argument.

  In both cases, the actual amount of padding is rounded
  so that the end of the arena is always a system page boundary.

  The main reason for using padding is to avoid calling sbrk so
  often. Having even a small pad greatly reduces the likelihood
  that nearly every malloc request during program start-up (or
  after trimming) will invoke sbrk, which needlessly wastes
  time.

  Automatic rounding-up to page-size units is normally sufficient
  to avoid measurable overhead, so the default is 0.  However, in
  systems where sbrk is relatively slow, it can pay to increase
  this value, at the expense of carrying around more memory than
  the program needs.
*/

#define M_TOP_PAD              -2

#ifndef DEFAULT_TOP_PAD
#define DEFAULT_TOP_PAD        (0)
#endif

/*
  M_MMAP_THRESHOLD is the request size threshold for using mmap()
  to service a request. Requests of at least this size that cannot
  be allocated using already-existing space will be serviced via mmap.
  (If enough normal freed space already exists it is used instead.)

  Using mmap segregates relatively large chunks of memory so that
  they can be individually obtained and released from the host
  system. A request serviced through mmap is never reused by any
  other request (at least not directly; the system may just so
  happen to remap successive requests to the same locations).

  Segregating space in this way has the benefits that:

   1. Mmapped space can ALWAYS be individually released back 
      to the system, which helps keep the system level memory 
      demands of a long-lived program low. 
   2. Mapped memory can never become `locked' between
      other chunks, as can happen with normally allocated chunks, which
      means that even trimming via malloc_trim would not release them.
   3. On some systems with "holes" in address spaces, mmap can obtain
      memory that sbrk cannot.

  However, it has the disadvantages that:

   1. The space cannot be reclaimed, consolidated, and then
      used to service later requests, as happens with normal chunks.
   2. It can lead to more wastage because of mmap page alignment
      requirements
   3. It causes malloc performance to be more dependent on host
      system memory management support routines which may vary in
      implementation quality and may impose arbitrary
      limitations. Generally, servicing a request via normal
      malloc steps is faster than going through a system's mmap.

  The advantages of mmap nearly always outweigh disadvantages for
  "large" chunks, but the value of "large" varies across systems.  The
  default is an empirically derived value that works well in most
  systems.
*/

#define M_MMAP_THRESHOLD      -3

#ifndef DEFAULT_MMAP_THRESHOLD
#define DEFAULT_MMAP_THRESHOLD (256 * 1024)
#endif

/*
  M_MMAP_MAX is the maximum number of requests to simultaneously
  service using mmap. This parameter exists because
. Some systems have a limited number of internal tables for
  use by mmap, and using more than a few of them may degrade
  performance.

  The default is set to a value that serves only as a safeguard.
  Setting to 0 disables use of mmap for servicing large requests.  If
  HAVE_MMAP is not set, the default value is 0, and attempts to set it
  to non-zero values in mallopt will fail.
*/

#define M_MMAP_MAX             -4

#ifndef DEFAULT_MMAP_MAX
#if HAVE_MMAP
#define DEFAULT_MMAP_MAX       (65536)
#else
#define DEFAULT_MMAP_MAX       (0)
#endif
#endif

#ifdef __cplusplus
};  /* end of extern "C" */
#endif

/* ------------- Optional versions of memcopy ---------------- */


#if USE_MEMCPY

/* 
  Note: memcpy is ONLY invoked with non-overlapping regions,
  so the (usually slower) memmove is not needed.
*/

#define MALLOC_COPY(dest, src, nbytes)  memcpy(dest, src, nbytes)
#define MALLOC_ZERO(dest, nbytes)       memset(dest, 0,   nbytes)

#else /* !USE_MEMCPY */

/* Use Duff's device for good zeroing/copying performance. */

#define MALLOC_ZERO(charp, nbytes)                                            \
do {                                                                          \
  INTERNAL_SIZE_T* mzp = (INTERNAL_SIZE_T*)(charp);                           \
  CHUNK_SIZE_T  mctmp = (nbytes)/sizeof(INTERNAL_SIZE_T);                     \
  long mcn;                                                                   \
  if (mctmp < 8) mcn = 0; else { mcn = (mctmp-1)/8; mctmp %= 8; }             \
  switch (mctmp) {                                                            \
    case 0: for(;;) { *mzp++ = 0;                                             \
    case 7:           *mzp++ = 0;                                             \
    case 6:           *mzp++ = 0;                                             \
    case 5:           *mzp++ = 0;                                             \
    case 4:           *mzp++ = 0;                                             \
    case 3:           *mzp++ = 0;                                             \
    case 2:           *mzp++ = 0;                                             \
    case 1:           *mzp++ = 0; if(mcn <= 0) break; mcn--; }                \
  }                                                                           \
} while(0)

#define MALLOC_COPY(dest,src,nbytes)                                          \
do {                                                                          \
  INTERNAL_SIZE_T* mcsrc = (INTERNAL_SIZE_T*) src;                            \
  INTERNAL_SIZE_T* mcdst = (INTERNAL_SIZE_T*) dest;                           \
  CHUNK_SIZE_T  mctmp = (nbytes)/sizeof(INTERNAL_SIZE_T);                     \
  long mcn;                                                                   \
  if (mctmp < 8) mcn = 0; else { mcn = (mctmp-1)/8; mctmp %= 8; }             \
  switch (mctmp) {                                                            \
    case 0: for(;;) { *mcdst++ = *mcsrc++;                                    \
    case 7:           *mcdst++ = *mcsrc++;                                    \
    case 6:           *mcdst++ = *mcsrc++;                                    \
    case 5:           *mcdst++ = *mcsrc++;                                    \
    case 4:           *mcdst++ = *mcsrc++;                                    \
    case 3:           *mcdst++ = *mcsrc++;                                    \
    case 2:           *mcdst++ = *mcsrc++;                                    \
    case 1:           *mcdst++ = *mcsrc++; if(mcn <= 0) break; mcn--; }       \
  }                                                                           \
} while(0)

#endif

/* ------------------ MMAP support ------------------  */


#if HAVE_MMAP

#ifndef LACKS_FCNTL_H
#include <fcntl.h>
#endif

#ifndef LACKS_SYS_MMAN_H
#include <sys/mman.h>
#endif

#if !defined(MAP_ANONYMOUS) && defined(MAP_ANON)
#define MAP_ANONYMOUS MAP_ANON
#endif

/* 
   Nearly all versions of mmap support MAP_ANONYMOUS, 
   so the following is unlikely to be needed, but is
   supplied just in case.
*/

#ifndef MAP_ANONYMOUS

static int dev_zero_fd = -1; /* Cached file descriptor for /dev/zero. */

#define MMAP(addr, size, prot, flags) ((dev_zero_fd < 0) ? \
 (dev_zero_fd = open("/dev/zero", O_RDWR), \
  mmap((addr), (size), (prot), (flags), dev_zero_fd, 0)) : \
   mmap((addr), (size), (prot), (flags), dev_zero_fd, 0))

#else

#define MMAP(addr, size, prot, flags) \
 (mmap((addr), (size), (prot), (flags)|MAP_ANONYMOUS, -1, 0))

#endif


#endif /* HAVE_MMAP */

/*
  ----------------------- Region representation ------------------------
*/

struct malloc_region {
	void				*prev;
	INTERNAL_SIZE_T		prev_size;
};

/*
  -----------------------  Chunk representations -----------------------
*/


/*
  This struct declaration is misleading (but accurate and necessary).
  It declares a "view" into memory allowing access to necessary
  fields at known offsets from a given base. See explanation below.
*/

struct malloc_chunk {

  INTERNAL_SIZE_T      prev_size;  /* Size of previous chunk (if free).  */
  INTERNAL_SIZE_T      size;       /* Size in bytes, including overhead. */

  struct malloc_chunk* fd;         /* double links -- used only if free. */
  struct malloc_chunk* bk;
};


typedef struct malloc_chunk* mchunkptr;

/*
   malloc_chunk details:

    (The following includes lightly edited explanations by Colin Plumb.)

    Chunks of memory are maintained using a `boundary tag' method as
    described in e.g., Knuth or Standish.  (See the paper by Paul
    Wilson ftp://ftp.cs.utexas.edu/pub/garbage/allocsrv.ps for a
    survey of such techniques.)  Sizes of free chunks are stored both
    in the front of each chunk and at the end.  This makes
    consolidating fragmented chunks into bigger chunks very fast.  The
    size fields also hold bits representing whether chunks are free or
    in use.

    An allocated chunk looks like this:


    chunk-> +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             Size of previous chunk, if allocated            | |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             Size of chunk, in bytes                         |P|
      mem-> +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             User data starts here...                          .
            .                                                               .
            .             (malloc_usable_space() bytes)                     .
            .                                                               |
nextchunk-> +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             Size of chunk                                     |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+


    Where "chunk" is the front of the chunk for the purpose of most of
    the malloc code, but "mem" is the pointer that is returned to the
    user.  "Nextchunk" is the beginning of the next contiguous chunk.

    Chunks always begin on even word boundries, so the mem portion
    (which is returned to the user) is also on an even word boundary, and
    thus at least double-word aligned.

    Free chunks are stored in circular doubly-linked lists, and look like this:

    chunk-> +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             Size of previous chunk                            |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    `head:' |             Size of chunk, in bytes                         |P|
      mem-> +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             Forward pointer to next chunk in list             |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             Back pointer to previous chunk in list            |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             Unused space (may be 0 bytes long)                .
            .                                                               .
            .                                                               |
nextchunk-> +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    `foot:' |             Size of chunk, in bytes                           |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

    The P (PREV_INUSE) bit, stored in the unused low-order bit of the
    chunk size (which is always a multiple of two words), is an in-use
    bit for the *previous* chunk.  If that bit is *clear*, then the
    word before the current chunk size contains the previous chunk
    size, and can be used to find the front of the previous chunk.
    The very first chunk allocated always has this bit set,
    preventing access to non-existent (or non-owned) memory. If
    prev_inuse is set for any given chunk, then you CANNOT determine
    the size of the previous chunk, and might even get a memory
    addressing fault when trying to do so.

    Note that the `foot' of the current chunk is actually represented
    as the prev_size of the NEXT chunk. This makes it easier to
    deal with alignments etc but can be very confusing when trying
    to extend or adapt this code.

    The two exceptions to all this are

     1. The special chunk `top' doesn't bother using the
        trailing size field since there is no next contiguous chunk
        that would have to index off it. After initialization, `top'
        is forced to always exist.  If it would become less than
        MINSIZE bytes long, it is replenished.

     2. Chunks allocated via mmap, which have the second-lowest-order
        bit (IS_MMAPPED) set in their size fields.  Because they are
        allocated one-by-one, each must contain its own trailing size field.

*/

/*
  ---------- Size and alignment checks and conversions ----------
*/

/* conversion from malloc headers to user pointers, and back */

#define chunk2mem(p)   ((Void_t*)((char*)(p) + 2*SIZE_SZ))
#define mem2chunk(mem) ((mchunkptr)((char*)(mem) - 2*SIZE_SZ))

/* The smallest possible chunk */
#define MIN_CHUNK_SIZE        (sizeof(struct malloc_chunk))

/* The smallest size we can malloc is an aligned minimal chunk */

#define MINSIZE  \
  (CHUNK_SIZE_T)(((MIN_CHUNK_SIZE+MALLOC_ALIGN_MASK) & ~MALLOC_ALIGN_MASK))

/* Check if m has acceptable alignment */

#define aligned_OK(m)  (((PTR_UINT)((m)) & (MALLOC_ALIGN_MASK)) == 0)

#define	LINK_SZ  (sizeof(Void_t*)+SIZE_SZ)


/* 
   Check if a request is so large that it would wrap around zero when
   padded and aligned. To simplify some other code, the bound is made
   low enough so that adding MINSIZE will also not wrap around sero.
*/

#define REQUEST_OUT_OF_RANGE(req)                                 \
  ((CHUNK_SIZE_T)(req) >=                                        \
   (CHUNK_SIZE_T)(INTERNAL_SIZE_T)(-2 * MINSIZE))    

/* pad request bytes into a usable size -- internal version */

#define request2size(req)                                         \
  (((req) + SIZE_SZ + MALLOC_ALIGN_MASK < MINSIZE)  ?             \
   MINSIZE :                                                      \
   ((req) + SIZE_SZ + MALLOC_ALIGN_MASK) & ~MALLOC_ALIGN_MASK)

/*  Same, except also perform argument check */

#define checked_request2size(req, sz)                             \
  if (REQUEST_OUT_OF_RANGE(req)) {                                \
    MALLOC_FAILURE_ACTION;                                        \
    return 0;                                                     \
  }                                                               \
  (sz) = request2size(req);                                              

/*
  --------------- Physical chunk operations ---------------
*/


/* size field is or'ed with PREV_INUSE when previous adjacent chunk in use */
#define PREV_INUSE 0x1

/* extract inuse bit of previous chunk */
#define prev_inuse(p)       ((p)->size & PREV_INUSE)


/* size field is or'ed with IS_MMAPPED if the chunk was obtained with mmap() */
#define IS_MMAPPED 0x2

/* check for mmap()'ed chunk */
#define chunk_is_mmapped(p) ((p)->size & IS_MMAPPED)

/* 
  Bits to mask off when extracting size 

  Note: IS_MMAPPED is intentionally not masked off from size field in
  macros for which mmapped chunks should never be seen. This should
  cause helpful core dumps to occur if it is tried by accident by
  people extending or adapting this malloc.
*/
#define SIZE_BITS (PREV_INUSE|IS_MMAPPED)

/* Get size, ignoring use bits */
#define chunksize(p)         ((p)->size & ~(SIZE_BITS))


/* Ptr to next physical malloc_chunk. */
#define next_chunk(p) ((mchunkptr)( ((char*)(p)) + ((p)->size & ~PREV_INUSE) ))

/* Ptr to previous physical malloc_chunk */
#define prev_chunk(p) ((mchunkptr)( ((char*)(p)) - ((p)->prev_size) ))

/* Treat space at ptr + offset as a chunk */
#define chunk_at_offset(p, s)  ((mchunkptr)(((char*)(p)) + (s)))

/* extract p's inuse bit */
#define inuse(p)\
((((mchunkptr)(((char*)(p))+((p)->size & ~PREV_INUSE)))->size) & PREV_INUSE)

/* set/clear chunk as being inuse without otherwise disturbing */
#define set_inuse(p)\
((mchunkptr)(((char*)(p)) + ((p)->size & ~PREV_INUSE)))->size |= PREV_INUSE

#define clear_inuse(p)\
((mchunkptr)(((char*)(p)) + ((p)->size & ~PREV_INUSE)))->size &= ~(PREV_INUSE)


/* check/set/clear inuse bits in known places */
#define inuse_bit_at_offset(p, s)\
 (((mchunkptr)(((char*)(p)) + (s)))->size & PREV_INUSE)

#define set_inuse_bit_at_offset(p, s)\
 (((mchunkptr)(((char*)(p)) + (s)))->size |= PREV_INUSE)

#define clear_inuse_bit_at_offset(p, s)\
 (((mchunkptr)(((char*)(p)) + (s)))->size &= ~(PREV_INUSE))


/* Set size at head, without disturbing its use bit */
#define set_head_size(p, s)  ((p)->size = (((p)->size & PREV_INUSE) | (s)))

/* Set size/use field */
#define set_head(p, s)       ((p)->size = (s))

/* Set size at footer (only when chunk is not in use) */
#define set_foot(p, s)       (((mchunkptr)((char*)(p) + (s)))->prev_size = (s))


/*
  -------------------- Internal data structures --------------------

   All internal state is held in an instance of malloc_state defined
   below. There are no other static variables, except in two optional
   cases: 
   * If USE_MALLOC_LOCK is defined, the mALLOC_MUTEx declared above. 
   * If HAVE_MMAP is true, but mmap doesn't support
     MAP_ANONYMOUS, a dummy file descriptor for mmap.

   Beware of lots of tricks that minimize the total bookkeeping space
   requirements. The result is a little over 1K bytes (for 4byte
   pointers and size_t.)
*/

/*
  Bins

    An array of bin headers for free chunks. Each bin is doubly
    linked.  The bins are approximately proportionally (log) spaced.
    There are a lot of these bins (128). This may look excessive, but
    works very well in practice.  Most bins hold sizes that are
    unusual as malloc request sizes, but are more usual for fragments
    and consolidated sets of chunks, which is what these bins hold, so
    they can be found quickly.  All procedures maintain the invariant
    that no consolidated chunk physically borders another one, so each
    chunk in a list is known to be preceeded and followed by either
    inuse chunks or the ends of memory.

    Chunks in bins are kept in size order, with ties going to the
    approximately least recently used chunk. Ordering isn't needed
    for the small bins, which all contain the same-sized chunks, but
    facilitates best-fit allocation for larger chunks. These lists
    are just sequential. Keeping them in order almost never requires
    enough traversal to warrant using fancier ordered data
    structures.  

    Chunks of the same size are linked with the most
    recently freed at the front, and allocations are taken from the
    back.  This results in LRU (FIFO) allocation order, which tends
    to give each chunk an equal opportunity to be consolidated with
    adjacent freed chunks, resulting in larger free chunks and less
    fragmentation.

    To simplify use in double-linked lists, each bin header acts
    as a malloc_chunk. This avoids special-casing for headers.
    But to conserve space and improve locality, we allocate
    only the fd/bk pointers of bins, and then use repositioning tricks
    to treat these as the fields of a malloc_chunk*.  
*/

typedef struct malloc_chunk* mbinptr;

/* addressing -- note that bin_at(0) does not exist */
#define bin_at(m, i) ((mbinptr)((char*)&((m)->bins[(i)<<1]) - (SIZE_SZ<<1)))

/* analog of ++bin */
#define next_bin(b)  ((mbinptr)((char*)(b) + (sizeof(mchunkptr)<<1)))

/* Reminders about list directionality within bins */
#define first(b)     ((b)->fd)
#define last(b)      ((b)->bk)

/* Take a chunk off a bin list */
#define unlink(P, BK, FD) {                                            \
  FD = P->fd;                                                          \
  BK = P->bk;                                                          \
  FD->bk = BK;                                                         \
  BK->fd = FD;                                                         \
}

/*
  Indexing

    Bins for sizes < 512 bytes contain chunks of all the same size, spaced
    8 bytes apart. Larger bins are approximately logarithmically spaced:

    64 bins of size       8
    32 bins of size      64
    16 bins of size     512
     8 bins of size    4096
     4 bins of size   32768
     2 bins of size  262144
     1 bin  of size what's left

    The bins top out around 1MB because we expect to service large
    requests via mmap.
*/

#define NBINS              96
#define NSMALLBINS         32
#define SMALLBIN_WIDTH      8
#define MIN_LARGE_SIZE    256

#define in_smallbin_range(sz)  \
  ((CHUNK_SIZE_T)(sz) < (CHUNK_SIZE_T)MIN_LARGE_SIZE)

#define smallbin_index(sz)     (((unsigned)(sz)) >> 3)

/*
  Compute index for size. We expect this to be inlined when
  compiled with optimization, else not, which works out well.
*/
inline int largebin_index(unsigned int sz) {
  unsigned int  x = sz >> SMALLBIN_WIDTH; 
  unsigned int m;            /* bit position of highest set bit of m */

  if (x >= 0x10000) return NBINS-1;

  /* On intel, use BSRL instruction to find highest bit */
#if defined(__GNUC__) && defined(i386)

  __asm__("bsrl %1,%0\n\t"
          : "=r" (m) 
          : "g"  (x));

#else
  {
    /*
      Based on branch-free nlz algorithm in chapter 5 of Henry
      S. Warren Jr's book "Hacker's Delight".
    */

    unsigned int n = ((x - 0x100) >> 16) & 8;
    x <<= n; 
    m = ((x - 0x1000) >> 16) & 4;
    n += m; 
    x <<= m; 
    m = ((x - 0x4000) >> 16) & 2;
    n += m; 
    x = (x << m) >> 14;
    m = 13 - n + (x & ~(x>>1));
  }
#endif

  /* Use next 2 bits to create finer-granularity bins */
  return NSMALLBINS + (m << 2) + ((sz >> (m + 6)) & 3);
}

#define bin_index(sz) \
 ((in_smallbin_range(sz)) ? smallbin_index(sz) : largebin_index(sz))

/*
  FIRST_SORTED_BIN_SIZE is the chunk size corresponding to the
  first bin that is maintained in sorted order. This must
  be the smallest size corresponding to a given bin.

  Normally, this should be MIN_LARGE_SIZE. But you can weaken
  best fit guarantees to sometimes speed up malloc by increasing value.
  Doing this means that malloc may choose a chunk that is 
  non-best-fitting by up to the width of the bin.

  Some useful cutoff values:
      512 - all bins sorted
     2560 - leaves bins <=     64 bytes wide unsorted  
    12288 - leaves bins <=    512 bytes wide unsorted
    65536 - leaves bins <=   4096 bytes wide unsorted
   262144 - leaves bins <=  32768 bytes wide unsorted
       -1 - no bins sorted (not recommended!)
*/

#define FIRST_SORTED_BIN_SIZE MIN_LARGE_SIZE 
/* #define FIRST_SORTED_BIN_SIZE 65536 */

/*
  Unsorted chunks

    All remainders from chunk splits, as well as all returned chunks,
    are first placed in the "unsorted" bin. They are then placed
    in regular bins after malloc gives them ONE chance to be used before
    binning. So, basically, the unsorted_chunks list acts as a queue,
    with chunks being placed on it in free (and malloc_consolidate),
    and taken off (to be either used or placed in bins) in malloc.
*/

/* The otherwise unindexable 1-bin is used to hold unsorted chunks. */
#define unsorted_chunks(M)          (bin_at(M, 1))

/*
  Top

    The top-most available chunk (i.e., the one bordering the end of
    available memory) is treated specially. It is never included in
    any bin, is used only if no other chunk is available, and is
    released back to the system if it is very large (see
    M_TRIM_THRESHOLD).  Because top initially
    points to its own bin with initial zero size, thus forcing
    extension on the first malloc request, we avoid having any special
    code in malloc to check whether it even exists yet. But we still
    need to do so when getting memory from system, so we make
    initial_top treat the bin as a legal but unusable chunk during the
    interval between initialization and the first call to
    sYSMALLOc. (This is somewhat delicate, since it relies on
    the 2 preceding words to be zero during this interval as well.)
*/

/* Conveniently, the unsorted bin can be used as dummy top on first call */
#define initial_top(M)              (unsorted_chunks(M))

/*
  Binmap

    To help compensate for the large number of bins, a one-level index
    structure is used for bin-by-bin searching.  `binmap' is a
    bitvector recording whether bins are definitely empty so they can
    be skipped over during during traversals.  The bits are NOT always
    cleared as soon as bins are empty, but instead only
    when they are noticed to be empty during traversal in malloc.
*/

/* Conservatively use 32 bits per map word, even if on 64bit system */
#define BINMAPSHIFT      5
#define BITSPERMAP       (1U << BINMAPSHIFT)
#define BINMAPSIZE       (NBINS / BITSPERMAP)

#define idx2block(i)     ((i) >> BINMAPSHIFT)
#define idx2bit(i)       ((1U << ((i) & ((1U << BINMAPSHIFT)-1))))

#define mark_bin(m,i)    ((m)->binmap[idx2block(i)] |=  idx2bit(i))
#define unmark_bin(m,i)  ((m)->binmap[idx2block(i)] &= ~(idx2bit(i)))
#define get_binmap(m,i)  ((m)->binmap[idx2block(i)] &   idx2bit(i))

/*
  Fastbins

    An array of lists holding recently freed small chunks.  Fastbins
    are not doubly linked.  It is faster to single-link them, and
    since chunks are never removed from the middles of these lists,
    double linking is not necessary. Also, unlike regular bins, they
    are not even processed in FIFO order (they use faster LIFO) since
    ordering doesn't much matter in the transient contexts in which
    fastbins are normally used.

    Chunks in fastbins keep their inuse bit set, so they cannot
    be consolidated with other free chunks. malloc_consolidate
    releases all chunks in fastbins and consolidates them with
    other free chunks. 
*/

typedef struct malloc_chunk* mfastbinptr;

/* offset 2 to use otherwise unindexable first 2 bins */
#define fastbin_index(sz)        ((((unsigned int)(sz)) >> 3) - 2)

/* The maximum fastbin request size we support */
#define MAX_FAST_SIZE     80

#define NFASTBINS  (fastbin_index(request2size(MAX_FAST_SIZE))+1)

/*
  FASTBIN_CONSOLIDATION_THRESHOLD is the size of a chunk in free()
  that triggers automatic consolidation of possibly-surrounding
  fastbin chunks. This is a heuristic, so the exact value should not
  matter too much. It is defined at half the default trim threshold as a
  compromise heuristic to only attempt consolidation if it is likely
  to lead to trimming. However, it is not dynamically tunable, since
  consolidation reduces fragmentation surrounding loarge chunks even 
  if trimming is not used.
*/

#define FASTBIN_CONSOLIDATION_THRESHOLD  \
  ((unsigned long)(DEFAULT_TRIM_THRESHOLD) >> 1)

/*
  Since the lowest 2 bits in max_fast don't matter in size comparisons, 
  they are used as flags.
*/

/*
  ANYCHUNKS_BIT held in max_fast indicates that there may be any
  freed chunks at all. It is set true when entering a chunk into any
  bin.
*/

#define ANYCHUNKS_BIT        (1U)

#define have_anychunks(M)     (((M)->max_fast &  ANYCHUNKS_BIT))
#define set_anychunks(M)      ((M)->max_fast |=  ANYCHUNKS_BIT)
#define clear_anychunks(M)    ((M)->max_fast &= ~ANYCHUNKS_BIT)

/*
  FASTCHUNKS_BIT held in max_fast indicates that there are probably
  some fastbin chunks. It is set true on entering a chunk into any
  fastbin, and cleared only in malloc_consolidate.
*/

#define FASTCHUNKS_BIT        (2U)

#define have_fastchunks(M)   (((M)->max_fast &  FASTCHUNKS_BIT))
#define set_fastchunks(M)    ((M)->max_fast |=  (FASTCHUNKS_BIT|ANYCHUNKS_BIT))
#define clear_fastchunks(M)  ((M)->max_fast &= ~(FASTCHUNKS_BIT))

/* 
   Set value of max_fast. 
   Use impossibly small value if 0.
*/

#define set_max_fast(M, s) \
  (M)->max_fast = (((s) == 0)? SMALLBIN_WIDTH: request2size(s)) | \
  ((M)->max_fast &  (FASTCHUNKS_BIT|ANYCHUNKS_BIT))

#define get_max_fast(M) \
  ((M)->max_fast & ~(FASTCHUNKS_BIT | ANYCHUNKS_BIT))


/*
  morecore_properties is a status word holding dynamically discovered
  or controlled properties of the morecore function
*/

#define MORECORE_CONTIGUOUS_BIT  (1U)

#define contiguous(M) \
        (((M)->morecore_properties &  MORECORE_CONTIGUOUS_BIT))
#define noncontiguous(M) \
        (((M)->morecore_properties &  MORECORE_CONTIGUOUS_BIT) == 0)
#define set_contiguous(M) \
        ((M)->morecore_properties |=  MORECORE_CONTIGUOUS_BIT)
#define set_noncontiguous(M) \
        ((M)->morecore_properties &= ~MORECORE_CONTIGUOUS_BIT)


/*
   ----------- Internal state representation and initialization -----------
*/

extern "C" 
{

struct malloc_state {

  /* The maximum chunk size to be eligible for fastbin */
  INTERNAL_SIZE_T  max_fast;   /* low 2 bits used as flags */

  /* Fastbins */
  mfastbinptr      fastbins[NFASTBINS];

  /* Base of the topmost chunk -- not otherwise kept in a bin */
  mchunkptr        top;

  /* The remainder from the most recent split of a small request */
  mchunkptr        last_remainder;

  /* Normal bins packed as described above */
  mchunkptr        bins[NBINS * 2];

  /* Bitmap of bins. Trailing zero map handles cases of largest binned size */
  unsigned int     binmap[BINMAPSIZE+1];

  /* Tunable parameters */
  CHUNK_SIZE_T     trim_threshold;
  INTERNAL_SIZE_T  top_pad;
  INTERNAL_SIZE_T  mmap_threshold;

  /* Memory map support */
  int              n_mmaps;
  int              n_mmaps_max;
  int              max_n_mmaps;

  /* Cache malloc_getpagesize */
  unsigned int     pagesize;    

  /* Track properties of MORECORE */
  unsigned int     morecore_properties;

  /* Statistics */
  INTERNAL_SIZE_T  mmapped_mem;
  INTERNAL_SIZE_T  sbrked_mem;
  INTERNAL_SIZE_T  max_sbrked_mem;
  INTERNAL_SIZE_T  max_mmapped_mem;
  INTERNAL_SIZE_T  max_total_mem;

  INTERNAL_SIZE_T  block_size;
  malloc_region    regions;

};

};

#endif
