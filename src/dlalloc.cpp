#include "dlalloc.h"

static void link_region(mstate,void *reg,size_t size);
static void unlink_region(mstate,void *reg,size_t size);

/*
  Initialize a malloc_state struct.

  This is called only from within malloc_consolidate, which needs
  be called in the same contexts anyway.  It is never called directly
  outside of malloc_consolidate because some optimizing compilers try
  to inline it at all call points, which turns out not to be an
  optimization at all. (Inlining it in malloc_consolidate is fine though.)
*/

static void malloc_init_state(mstate av)
{
  int     i;
  mbinptr bin;
  
  /* Establish circular links for normal bins */
  for (i = 1; i < NBINS; ++i) { 
    bin = bin_at(av,i);
    bin->fd = bin->bk = bin;
  }

  av->top_pad        = DEFAULT_TOP_PAD;
  av->n_mmaps_max    = DEFAULT_MMAP_MAX;
  av->mmap_threshold = DEFAULT_MMAP_THRESHOLD;
  av->trim_threshold = DEFAULT_TRIM_THRESHOLD;

#if MORECORE_CONTIGUOUS
  set_contiguous(av);
#else
  set_noncontiguous(av);
#endif


  set_max_fast(av, DEFAULT_MXFAST);

  av->top            = initial_top(av);
  av->pagesize       = malloc_getpagesize;
}

/* 
   Other internal utilities operating on mstates
*/

static Void_t*  sYSMALLOc(INTERNAL_SIZE_T, mstate);
static int      sYSTRIm(size_t, mstate);
static void     malloc_consolidate(mstate);
static Void_t** iALLOc(mstate,size_t, size_t*, int, Void_t**);

/*
  Debugging support

  These routines make a number of assertions about the states
  of data structures that should be true at all times. If any
  are not true, it's very likely that a user program has somehow
  trashed memory. (It's also possible that there is a coding error
  in malloc. In which case, please report it!)
*/

#if ! DEBUG

#define check_chunk(av,P)
#define check_free_chunk(av,P)
#define check_inuse_chunk(av,P)
#define check_remalloced_chunk(av,P,N)
#define check_malloced_chunk(av,P,N)
#define check_heap(av,P)
#define check_malloc_state(av)

#else
#define check_chunk(av,P)              do_check_chunk(av,P)
#define check_free_chunk(av,P)         do_check_free_chunk(av,P)
#define check_inuse_chunk(av,P)        do_check_inuse_chunk(av,P)
#define check_remalloced_chunk(av,P,N) do_check_remalloced_chunk(av,P,N)
#define check_malloced_chunk(av,P,N)   do_check_malloced_chunk(av,P,N)
#define check_heap(av,P)               do_check_heap(av,P)
#define check_malloc_state(av)         do_check_malloc_state(av)

/*
  Properties of all chunks
*/

static void do_check_chunk(mstate av, mchunkptr p)
{
  CHUNK_SIZE_T  sz = chunksize(p);
  /* min and max possible addresses assuming contiguous allocation */
  char* max_address = (char*)(av->top) + chunksize(av->top);
  char* min_address = max_address - av->sbrked_mem;

  if (!chunk_is_mmapped(p)) {
    
    /* Has legal address ... */
    if (p != av->top) {
      if (contiguous(av)) {
        assert(((char*)p) >= min_address);
        assert(((char*)p + sz) <= ((char*)(av->top)));
      }
    }
    else {
      /* top size is always at least MINSIZE */
      assert((CHUNK_SIZE_T)(sz) >= MINSIZE);
      /* top predecessor always marked inuse */
      assert(prev_inuse(p));
    }
      
  }
  else {
#if HAVE_MMAP
    /* address is outside main heap  */
    if (contiguous(av) && av->top != initial_top(av)) {
      assert(((char*)p) < min_address || ((char*)p) > max_address);
    }
    /* chunk is page-aligned */
    assert(((p->prev_size + sz) & (av->pagesize-1)) == 0);
    /* mem is aligned */
    assert(aligned_OK(chunk2mem(p)));
#else
    /* force an appropriate assert violation if debug set */
    assert(!chunk_is_mmapped(p));
#endif
  }
}

/*
  Properties of free chunks
*/

static void do_check_free_chunk(mstate av, mchunkptr p)
{
  INTERNAL_SIZE_T sz = p->size & ~PREV_INUSE;
  mchunkptr next = chunk_at_offset(p, sz);

  do_check_chunk(av,p);

  /* Chunk must claim to be free ... */
  assert(!inuse(p));
  assert (!chunk_is_mmapped(p));

  /* Unless a special marker, must have OK fields */
  if ((CHUNK_SIZE_T)(sz) >= MINSIZE)
  {
    assert((sz & MALLOC_ALIGN_MASK) == 0);
    assert(aligned_OK(chunk2mem(p)));
    /* ... matching footer field */
    assert(next->prev_size == sz);
    /* ... and is fully consolidated */
    assert(prev_inuse(p));
    assert (next == av->top || inuse(next));

    /* ... and has minimally sane links */
    assert(p->fd->bk == p);
    assert(p->bk->fd == p);
  }
  else /* markers are always of size SIZE_SZ */
    assert(sz == SIZE_SZ);
}

/*
  Properties of inuse chunks
*/

static void do_check_inuse_chunk(mstate av, mchunkptr p)
{
  mchunkptr next;
  do_check_chunk(av,p);

  if (chunk_is_mmapped(p))
    return; /* mmapped chunks have no next/prev */

  /* Check whether it claims to be in use ... */
  assert(inuse(p));

  next = next_chunk(p);

  /* ... and is surrounded by OK chunks.
    Since more things can be checked with free chunks than inuse ones,
    if an inuse chunk borders them and debug is on, it's worth doing them.
  */
  if (!prev_inuse(p))  {
    /* Note that we cannot even look at prev unless it is not inuse */
    mchunkptr prv = prev_chunk(p);
    assert(next_chunk(prv) == p);
    do_check_free_chunk(av,prv);
  }

  if (next == av->top) {
    assert(prev_inuse(next));
    assert(chunksize(next) >= MINSIZE);
  }
  else if (!inuse(next))
    do_check_free_chunk(av,next);
}

/*
  Properties of chunks recycled from fastbins
*/

static void do_check_remalloced_chunk(mstate av,mchunkptr p, INTERNAL_SIZE_T s)
{
  INTERNAL_SIZE_T sz = p->size & ~PREV_INUSE;

  do_check_inuse_chunk(av,p);

  /* Legal size ... */
  assert((sz & MALLOC_ALIGN_MASK) == 0);
  assert((CHUNK_SIZE_T)(sz) >= MINSIZE);
  /* ... and alignment */
  assert(aligned_OK(chunk2mem(p)));
  /* chunk is less than MINSIZE more than request */
  assert((long)(sz) - (long)(s) >= 0);
  assert((long)(sz) - (long)(s + MINSIZE) < 0);
}

/*
  Properties of nonrecycled chunks at the point they are malloced
*/

static void do_check_malloced_chunk(mstate av,mchunkptr p, INTERNAL_SIZE_T s)
{
  /* same as recycled case ... */
  do_check_remalloced_chunk(av, p, s);

  /*
    ... plus,  must obey implementation invariant that prev_inuse is
    always true of any allocated chunk; i.e., that each allocated
    chunk borders either a previously allocated and still in-use
    chunk, or the base of its memory arena. This is ensured
    by making all allocations from the the `lowest' part of any found
    chunk.  This does not necessarily hold however for chunks
    recycled via fastbins.
  */

  assert(prev_inuse(p));
}

static void do_check_heap(mstate av, void *p)
{
	for (const malloc_region *mr=&av->regions; ;mr=(malloc_region*)mr->prev)
		{assert(mr!=NULL); if (p>mr->prev && p<(char*)mr->prev+mr->prev_size) return;}
}


/*
  Properties of malloc_state.

  This may be useful for debugging malloc, as well as detecting user
  programmer errors that somehow write into malloc_state.

  If you are extending or experimenting with this malloc, you can
  probably figure out how to hack this routine to print out or
  display chunk addresses, sizes, bins, and other instrumentation.
*/

static void do_check_malloc_state(mstate av)
{
  int i;
  mchunkptr p;
  mchunkptr q;
  mbinptr b;
  unsigned int binbit;
  int empty;
  unsigned int idx;
  INTERNAL_SIZE_T size;
  CHUNK_SIZE_T  total = 0;
  int max_fast_bin;

  /* internal size_t must be no wider than pointer type */
  assert(sizeof(INTERNAL_SIZE_T) <= sizeof(char*));

  /* alignment is a power of 2 */
  assert((MALLOC_ALIGNMENT & (MALLOC_ALIGNMENT-1)) == 0);

  /* cannot run remaining checks until fully initialized */
  if (av->top == 0 || av->top == initial_top(av))
    return;

  /* pagesize is a power of 2 */
  assert((av->pagesize & (av->pagesize-1)) == 0);

  /* properties of fastbins */

  /* max_fast is in allowed range */
  assert(get_max_fast(av) <= request2size(MAX_FAST_SIZE));

  max_fast_bin = fastbin_index(av->max_fast);

  for (i = 0; (unsigned)i < NFASTBINS; ++i) {
    p = av->fastbins[i];

    /* all bins past max_fast are empty */
    if (i > max_fast_bin)
      assert(p == 0);

    while (p != 0) {
      /* each chunk claims to be inuse */
      do_check_inuse_chunk(av,p);
      total += chunksize(p);
      /* chunk belongs in this bin */
      assert(fastbin_index(chunksize(p)) == (unsigned)i);
      p = p->fd;
    }
  }

  if (total != 0)
    assert(have_fastchunks(av));
  else if (!have_fastchunks(av))
    assert(total == 0);

  /* check normal bins */
  for (i = 1; i < NBINS; ++i) {
    b = bin_at(av,i);

    /* binmap is accurate (except for bin 1 == unsorted_chunks) */
    if (i >= 2) {
      binbit = get_binmap(av,i);
      empty = last(b) == b;
      if (!binbit)
        assert(empty);
      else if (!empty)
        assert(binbit);
    }

    for (p = last(b); p != b; p = p->bk) {
      /* each chunk claims to be free */
      do_check_free_chunk(av,p);
      size = chunksize(p);
      total += size;
      if (i >= 2) {
        /* chunk belongs in bin */
        idx = bin_index(size);
        assert(idx == (unsigned)i);
        /* lists are sorted */
        if ((CHUNK_SIZE_T) size >= (CHUNK_SIZE_T)(FIRST_SORTED_BIN_SIZE)) {
          assert(p->bk == b || 
                 (CHUNK_SIZE_T)chunksize(p->bk) >= 
                 (CHUNK_SIZE_T)chunksize(p));
        }
      }
      /* chunk is followed by a legal chain of inuse chunks */
      for (q = next_chunk(p);
           (q != av->top && inuse(q) && 
             (CHUNK_SIZE_T)(chunksize(q)) >= MINSIZE);
           q = next_chunk(q))
        do_check_inuse_chunk(av,q);
    }
  }

  /* top chunk is OK */
  check_chunk(av,av->top);

  /* sanity checks for statistics */

  assert(total <= (CHUNK_SIZE_T)(av->max_total_mem));
  assert(av->n_mmaps >= 0);
  assert(av->n_mmaps <= av->max_n_mmaps);

  assert((CHUNK_SIZE_T)(av->sbrked_mem) <=
         (CHUNK_SIZE_T)(av->max_sbrked_mem));

  assert((CHUNK_SIZE_T)(av->mmapped_mem) <=
         (CHUNK_SIZE_T)(av->max_mmapped_mem));

  assert((CHUNK_SIZE_T)(av->max_total_mem) >=
         (CHUNK_SIZE_T)(av->mmapped_mem) + (CHUNK_SIZE_T)(av->sbrked_mem));
}
#endif


/* ----------- Routines dealing with system allocation -------------- */

/*
  sysmalloc handles malloc cases requiring more memory from the system.
  On entry, it is assumed that av->top does not have enough
  space to service request for nb bytes, thus requiring that av->top
  be extended or replaced.
*/

static Void_t* sYSMALLOc(INTERNAL_SIZE_T nb, mstate av)
{
  mchunkptr       old_top;        /* incoming value of av->top */
  INTERNAL_SIZE_T old_size;       /* its size */
  char*           old_end;        /* its end address */

  long            size;           /* arg to first MORECORE or mmap call */
  long            minSize;        /* minimal block size for this request */
  char*           brk;            /* return value from MORECORE */

  long            correction;     /* arg to 2nd MORECORE call */
  char*           snd_brk;        /* 2nd return val */

  INTERNAL_SIZE_T front_misalign; /* unusable bytes at front of new space */
  INTERNAL_SIZE_T end_misalign;   /* partial page left at end of new space */
  char*           aligned_brk;    /* aligned offset into brk */

  mchunkptr       p;              /* the allocated/returned chunk */
  mchunkptr       remainder;      /* remainder from allocation */
  CHUNK_SIZE_T    remainder_size; /* its size */

  CHUNK_SIZE_T    sum;            /* for updating stats */

  size_t          pagemask  = av->pagesize - 1;

  /*
    If there is space available in fastbins, consolidate and retry
    malloc from scratch rather than getting memory from system.  This
    can occur only if nb is in smallbin range so we didn't consolidate
    upon entry to malloc. It is much easier to handle this case here
    than in malloc proper.
  */

  if (have_fastchunks(av)) {
    assert(in_smallbin_range(nb));
    malloc_consolidate(av);
    return mALLOc(av, nb - MALLOC_ALIGN_MASK);
  }


#if HAVE_MMAP

  /*
    If have mmap, and the request size meets the mmap threshold, and
    the system supports mmap, and there are few enough currently
    allocated mmapped regions, try to directly map this request
    rather than expanding top.
  */

  if ((CHUNK_SIZE_T)(nb) >= (CHUNK_SIZE_T)(av->mmap_threshold) &&
      (av->n_mmaps < av->n_mmaps_max)) {

    char* mm;             /* return value from mmap call*/

    /*
      Round up size to nearest page.  For mmapped chunks, the overhead
      is one SIZE_SZ unit larger than for normal chunks, because there
      is no following chunk whose prev_size field could be used.
    */
    size = (nb + SIZE_SZ + LINK_SZ + MALLOC_ALIGN_MASK + pagemask) & ~pagemask;

    /* Don't try if size wraps around 0 */
    if ((CHUNK_SIZE_T)(size) > (CHUNK_SIZE_T)(nb)) {

      mm = (char*)(MMAP(0, size, PROT_READ|PROT_WRITE, MAP_PRIVATE));
      
      if (mm != (char*)(MORECORE_FAILURE)) {
        
        /*
          The offset to the start of the mmapped region is stored
          in the prev_size field of the chunk. This allows us to adjust
          returned start address to meet alignment requirements here 
          and in memalign(), and still be able to compute proper
          address argument for later munmap in free() and realloc().
        */
        
		link_region(av, mm, size);

        front_misalign = (LINK_SZ + (INTERNAL_SIZE_T)chunk2mem(mm)) & MALLOC_ALIGN_MASK;
		correction = LINK_SZ + (front_misalign > 0 ? MALLOC_ALIGNMENT - front_misalign : 0);
        p = (mchunkptr)(mm + correction);
        p->prev_size = correction;
        set_head(p, (size - correction) |IS_MMAPPED);
        
        /* update statistics */
        
        if (++av->n_mmaps > av->max_n_mmaps) 
          av->max_n_mmaps = av->n_mmaps;
        
        sum = av->mmapped_mem += size;
        if (sum > (CHUNK_SIZE_T)(av->max_mmapped_mem)) 
          av->max_mmapped_mem = sum;
        sum += av->sbrked_mem;
        if (sum > (CHUNK_SIZE_T)(av->max_total_mem)) 
          av->max_total_mem = sum;

        check_chunk(av,p);
        
        return chunk2mem(p);
      }
    }
  }
#endif

  /* Record incoming configuration of top */

  old_top  = av->top;
  old_size = chunksize(old_top);
  old_end  = (char*)(chunk_at_offset(old_top, old_size));

  brk = snd_brk = (char*)(MORECORE_FAILURE); 

  /* 
     If not the first time through, we require old_size to be
     at least MINSIZE and to have prev_inuse set.
  */

  assert((old_top == initial_top(av) && old_size == 0) || 
         ((CHUNK_SIZE_T) (old_size) >= MINSIZE &&
          prev_inuse(old_top)));

  /* Precondition: not enough current space to satisfy nb request */
  assert((CHUNK_SIZE_T)(old_size) < (CHUNK_SIZE_T)(nb + MINSIZE));

  /* Precondition: all fastbins are consolidated */
  assert(!have_fastchunks(av));


  /* Request enough space for nb + pad + overhead */

  size = nb + av->top_pad + MINSIZE + LINK_SZ;

  /*
    If contiguous, we can subtract out existing space that we hope to
    combine with new space. We add it back later only if
    we don't actually get contiguous space.
  */

  if (contiguous(av))
    size -= old_size;

  /*
    Round to a multiple of page size.
    If MORECORE is not contiguous, this ensures that we only call it
    with whole-page arguments.  And if MORECORE is contiguous and
    this is not first time through, this preserves page-alignment of
    previous calls. Otherwise, we correct to page-align below.
  */

  size = (size + pagemask) & ~pagemask;

  /*
    Don't try to call MORECORE if argument is so big as to appear
    negative. Note that since mmap takes size_t arg, it may succeed
    below even if we cannot call MORECORE.
  */

  if (size > 0) 
    brk = (char*)(MORECORE(av,size));

  /*
    If have mmap, try using it as a backup when MORECORE fails or
    cannot be used. This is worth doing on systems that have "holes" in
    address space, so sbrk cannot extend to give contiguous space, but
    space is available elsewhere.  Note that we ignore mmap max count
    and threshold limits, since the space will not be used as a
    segregated mmap region.
  */

#if HAVE_MMAP
  if (brk == (char*)(MORECORE_FAILURE)) {

    /* Cannot merge with old top, so add its size back in */
    if (contiguous(av))
      size = (size + old_size + pagemask) & ~pagemask;

	minSize = size;

    /* If we are relying on mmap as backup, then use larger units */
	if ((CHUNK_SIZE_T)(size) < (CHUNK_SIZE_T)(av->block_size)) {
		size = av->block_size;
		if (av->block_size < MMAP_MAX_BLOCK_SIZE) av->block_size *= 2;
	}

    /* Don't try if size wraps around 0 */
    if ((CHUNK_SIZE_T)(size) > (CHUNK_SIZE_T)(nb)) {

	  for (;;) {
        brk = (char*)(MMAP(0, size, PROT_READ|PROT_WRITE, MAP_PRIVATE));
      
        if (brk == (char*)(MORECORE_FAILURE)) {
		  if ((size/=2) >= minSize) {
		    av->block_size /= 2;
			continue;
		  }
		} else {

	      link_region(av, brk, size);
		  brk += LINK_SZ;
		  size -= LINK_SZ;
        
          /* We do not need, and cannot use, another sbrk call to find end */
          snd_brk = brk + size;
        
          /* 
           Record that we no longer have a contiguous sbrk region. 
           After the first time mmap is used as backup, we do not
           ever rely on contiguous space since this could incorrectly
           bridge regions.
          */
          set_noncontiguous(av);
		}
		break;
      }
    }
  }
#endif

  if (brk != (char*)(MORECORE_FAILURE)) {
    av->sbrked_mem += size;

    /*
      If MORECORE extends previous space, we can likewise extend top size.
    */
    
    if (brk == old_end && snd_brk == (char*)(MORECORE_FAILURE)) {
      set_head(old_top, (size + old_size) | PREV_INUSE);
    }

    /*
      Otherwise, make adjustments:
      
      * If the first time through or noncontiguous, we need to call sbrk
        just to find out where the end of memory lies.

      * We need to ensure that all returned chunks from malloc will meet
        MALLOC_ALIGNMENT

      * If there was an intervening foreign sbrk, we need to adjust sbrk
        request size to account for fact that we will not be able to
        combine new space with existing space in old_top.

      * Almost all systems internally allocate whole pages at a time, in
        which case we might as well use the whole last page of request.
        So we allocate enough more memory to hit a page boundary now,
        which in turn causes future contiguous calls to page-align.
    */
    
    else {
      front_misalign = 0;
      end_misalign = 0;
      correction = 0;
      aligned_brk = brk;

      /*
        If MORECORE returns an address lower than we have seen before,
        we know it isn't really contiguous.  This and some subsequent
        checks help cope with non-conforming MORECORE functions and
        the presence of "foreign" calls to MORECORE from outside of
        malloc or by other threads.  We cannot guarantee to detect
        these in all cases, but cope with the ones we do detect.
      */
      if (contiguous(av) && old_size != 0 && brk < old_end) {
        set_noncontiguous(av);
      }
      
      /* handle contiguous cases */
      if (contiguous(av)) { 

        /* 
           We can tolerate forward non-contiguities here (usually due
           to foreign calls) but treat them as part of our space for
           stats reporting.
        */
        if (old_size != 0) 
          av->sbrked_mem += brk - old_end;
        
        /* Guarantee alignment of first new chunk made from this space */

        front_misalign = (INTERNAL_SIZE_T)chunk2mem(brk) & MALLOC_ALIGN_MASK;
        if (front_misalign > 0) {

          /*
            Skip over some bytes to arrive at an aligned position.
            We don't need to specially mark these wasted front bytes.
            They will never be accessed anyway because
            prev_inuse of av->top (and any chunk created from its start)
            is always true after initialization.
          */

          correction = MALLOC_ALIGNMENT - front_misalign;
          aligned_brk += correction;
        }
        
        /*
          If this isn't adjacent to existing space, then we will not
          be able to merge with old_top space, so must add to 2nd request.
        */
        
        correction += old_size;
        
        /* Extend the end address to hit a page boundary */
        end_misalign = (INTERNAL_SIZE_T)(brk + size + correction);
        correction += ((end_misalign + pagemask) & ~pagemask) - end_misalign;
        
        assert(correction >= 0);
        snd_brk = (char*)(MORECORE(av,correction));
        
        if (snd_brk == (char*)(MORECORE_FAILURE)) {
          /*
            If can't allocate correction, try to at least find out current
            brk.  It might be enough to proceed without failing.
          */
          correction = 0;
          snd_brk = (char*)(MORECORE(av,0));
        }
        else if (snd_brk < brk) {
          /*
            If the second call gives noncontiguous space even though
            it says it won't, the only course of action is to ignore
            results of second call, and conservatively estimate where
            the first call left us. Also set noncontiguous, so this
            won't happen again, leaving at most one hole.
            
            Note that this check is intrinsically incomplete.  Because
            MORECORE is allowed to give more space than we ask for,
            there is no reliable way to detect a noncontiguity
            producing a forward gap for the second call.
          */
          snd_brk = brk + size;
          correction = 0;
          set_noncontiguous(av);
        }

      }
      
      /* handle non-contiguous cases */
      else { 
        /* MORECORE/mmap must correctly align */
        assert(aligned_OK(chunk2mem(brk)));
        
        /* Find out current end of memory */
        if (snd_brk == (char*)(MORECORE_FAILURE)) {
          snd_brk = (char*)(MORECORE(av,0));
          av->sbrked_mem += snd_brk - brk - size;
        }
      }
      
      /* Adjust top based on results of second sbrk */
      if (snd_brk != (char*)(MORECORE_FAILURE)) {
        av->top = (mchunkptr)aligned_brk;
        set_head(av->top, (snd_brk - aligned_brk + correction) | PREV_INUSE);
        av->sbrked_mem += correction;
     
        /*
          If not the first time through, we either have a
          gap due to foreign sbrk or a non-contiguous region.  Insert a
          double fencepost at old_top to prevent consolidation with space
          we don't own. These fenceposts are artificial chunks that are
          marked as inuse and are in any case too small to use.  We need
          two to make sizes and alignments work out.
        */
   
        if (old_size != 0) {
          /* 
             Shrink old_top to insert fenceposts, keeping size a
             multiple of MALLOC_ALIGNMENT. We know there is at least
             enough space in old_top to do this.
          */
          old_size = (old_size - 3*SIZE_SZ) & ~MALLOC_ALIGN_MASK;
          set_head(old_top, old_size | PREV_INUSE);
          
          /*
            Note that the following assignments completely overwrite
            old_top when old_size was previously MINSIZE.  This is
            intentional. We need the fencepost, even if old_top otherwise gets
            lost.
          */
          chunk_at_offset(old_top, old_size          )->size =
            SIZE_SZ|PREV_INUSE;

          chunk_at_offset(old_top, old_size + SIZE_SZ)->size =
            SIZE_SZ|PREV_INUSE;

          /* 
             If possible, release the rest, suppressing trimming.
          */
          if (old_size >= MINSIZE) {
            INTERNAL_SIZE_T tt = av->trim_threshold;
            av->trim_threshold = (INTERNAL_SIZE_T)(-1);
            fREe(av, chunk2mem(old_top));
            av->trim_threshold = tt;
          }
        }
      }
    }
    
    /* Update statistics */
    sum = av->sbrked_mem;
    if (sum > (CHUNK_SIZE_T)(av->max_sbrked_mem))
      av->max_sbrked_mem = sum;
    
    sum += av->mmapped_mem;
    if (sum > (CHUNK_SIZE_T)(av->max_total_mem))
      av->max_total_mem = sum;

    check_malloc_state(av);
    
    /* finally, do the allocation */

    p = av->top;
    size = chunksize(p);
    
    /* check that one of the above allocation paths succeeded */
    if ((CHUNK_SIZE_T)(size) >= (CHUNK_SIZE_T)(nb + MINSIZE)) {
      remainder_size = size - nb;
      remainder = chunk_at_offset(p, nb);
      av->top = remainder;
      set_head(p, nb | PREV_INUSE);
      set_head(remainder, remainder_size | PREV_INUSE);
      check_malloced_chunk(av, p, nb);
      return chunk2mem(p);
    }

  }

  /* catch all failure paths */
  MALLOC_FAILURE_ACTION;
  return 0;
}




/*
  sYSTRIm is an inverse of sorts to sYSMALLOc.  It gives memory back
  to the system (via negative arguments to sbrk) if there is unused
  memory at the `high' end of the malloc pool. It is called
  automatically by free() when top space exceeds the trim
  threshold. It is also called by the public malloc_trim routine.  It
  returns 1 if it actually released any memory, else 0.
*/

static int sYSTRIm(size_t pad, mstate av)
{
#if 0
  long  top_size;        /* Amount of top-most memory */
  long  extra;           /* Amount to release */
  long  released;        /* Amount actually released */
  char* current_brk;     /* address returned by pre-check sbrk call */
  char* new_brk;         /* address returned by post-check sbrk call */
  size_t pagesz;

  pagesz = av->pagesize;
  top_size = chunksize(av->top);
  
  /* Release in pagesize units, keeping at least one page */
  extra = ((top_size - pad - MINSIZE + (pagesz-1)) / pagesz - 1) * pagesz;
  
  if (extra > 0) {
    
    /*
      Only proceed if end of memory is where we last set it.
      This avoids problems if there were foreign sbrk calls.
    */
    current_brk = (char*)(MORECORE(av,0));
    if (current_brk == (char*)(av->top) + top_size) {
      
      /*
        Attempt to release memory. We ignore MORECORE return value,
        and instead call again to find out where new end of memory is.
        This avoids problems if first call releases less than we asked,
        of if failure somehow altered brk value. (We could still
        encounter problems if it altered brk in some very bad way,
        but the only thing we can do is adjust anyway, which will cause
        some downstream failure.)
      */
      
      MORECORE(av,-extra);
      new_brk = (char*)(MORECORE(av,0));
      
      if (new_brk != (char*)MORECORE_FAILURE) {
        released = (long)(current_brk - new_brk);
        
        if (released != 0) {
          /* Success. Adjust top. */
          av->sbrked_mem -= released;
          set_head(av->top, (top_size - released) | PREV_INUSE);
          check_malloc_state(av);
          return 1;
        }
      }
    }
  }
#endif
  return 0;
}

/*
  ------------------------------ malloc ------------------------------
*/

Void_t* mALLOc(mstate av, size_t bytes)
{
  INTERNAL_SIZE_T nb;               /* normalized request size */
  unsigned int    idx;              /* associated bin index */
  mbinptr         bin;              /* associated bin */
  mfastbinptr*    fb;               /* associated fastbin */

  mchunkptr       victim;           /* inspected/selected chunk */
  INTERNAL_SIZE_T size;             /* its size */
  int             victim_index;     /* its bin index */

  mchunkptr       remainder;        /* remainder from a split */
  CHUNK_SIZE_T    remainder_size;   /* its size */

  unsigned int    block;            /* bit map traverser */
  unsigned int    bit;              /* bit map traverser */
  unsigned int    map;              /* current word of binmap */

  mchunkptr       fwd;              /* misc temp for linking */
  mchunkptr       bck;              /* misc temp for linking */

  /*
    Convert request size to internal form by adding SIZE_SZ bytes
    overhead plus possibly more to obtain necessary alignment and/or
    to obtain a size of at least MINSIZE, the smallest allocatable
    size. Also, checked_request2size traps (returning 0) request sizes
    that are so large that they wrap around zero when padded and
    aligned.
  */

  checked_request2size(bytes, nb);

  /*
    Bypass search if no frees yet
   */
  if (!have_anychunks(av)) {
    if (av->max_fast == 0) /* initialization check */
      malloc_consolidate(av);
    goto use_top;
  }

  /*
    If the size qualifies as a fastbin, first check corresponding bin.
  */

  if ((CHUNK_SIZE_T)(nb) <= (CHUNK_SIZE_T)(av->max_fast)) { 
    fb = &(av->fastbins[(fastbin_index(nb))]);
    if ( (victim = *fb) != 0) {
      *fb = victim->fd;
      check_remalloced_chunk(av, victim, nb);
      return chunk2mem(victim);
    }
  }

  /*
    If a small request, check regular bin.  Since these "smallbins"
    hold one size each, no searching within bins is necessary.
    (For a large request, we need to wait until unsorted chunks are
    processed to find best fit. But for small ones, fits are exact
    anyway, so we can check now, which is faster.)
  */

  if (in_smallbin_range(nb)) {
    idx = smallbin_index(nb);
    bin = bin_at(av,idx);

    if ( (victim = last(bin)) != bin) {
      bck = victim->bk;
      set_inuse_bit_at_offset(victim, nb);
      bin->bk = bck;
      bck->fd = bin;
      
      check_malloced_chunk(av, victim, nb);
      return chunk2mem(victim);
    }
  }

  /* 
     If this is a large request, consolidate fastbins before continuing.
     While it might look excessive to kill all fastbins before
     even seeing if there is space available, this avoids
     fragmentation problems normally associated with fastbins.
     Also, in practice, programs tend to have runs of either small or
     large requests, but less often mixtures, so consolidation is not 
     invoked all that often in most programs. And the programs that
     it is called frequently in otherwise tend to fragment.
  */

  else {
    idx = largebin_index(nb);
    if (have_fastchunks(av)) 
      malloc_consolidate(av);
  }

  /*
    Process recently freed or remaindered chunks, taking one only if
    it is exact fit, or, if this a small request, the chunk is remainder from
    the most recent non-exact fit.  Place other traversed chunks in
    bins.  Note that this step is the only place in any routine where
    chunks are placed in bins.
  */
    
  while ( (victim = unsorted_chunks(av)->bk) != unsorted_chunks(av)) {
    bck = victim->bk;
    size = chunksize(victim);
    
    /* 
       If a small request, try to use last remainder if it is the
       only chunk in unsorted bin.  This helps promote locality for
       runs of consecutive small requests. This is the only
       exception to best-fit, and applies only when there is
       no exact fit for a small chunk.
    */
    
    if (in_smallbin_range(nb) && 
        bck == unsorted_chunks(av) &&
        victim == av->last_remainder &&
        (CHUNK_SIZE_T)(size) > (CHUNK_SIZE_T)(nb + MINSIZE)) {
      
      /* split and reattach remainder */
      remainder_size = size - nb;
      remainder = chunk_at_offset(victim, nb);
      unsorted_chunks(av)->bk = unsorted_chunks(av)->fd = remainder;
      av->last_remainder = remainder; 
      remainder->bk = remainder->fd = unsorted_chunks(av);
      
      set_head(victim, nb | PREV_INUSE);
      set_head(remainder, remainder_size | PREV_INUSE);
      set_foot(remainder, remainder_size);
      
      check_malloced_chunk(av, victim, nb);
      return chunk2mem(victim);
    }
    
    /* remove from unsorted list */
    unsorted_chunks(av)->bk = bck;
    bck->fd = unsorted_chunks(av);
    
    /* Take now instead of binning if exact fit */
    
    if (size == nb) {
      set_inuse_bit_at_offset(victim, size);
      check_malloced_chunk(av, victim, nb);
      return chunk2mem(victim);
    }
    
    /* place chunk in bin */
    
    if (in_smallbin_range(size)) {
      victim_index = smallbin_index(size);
      bck = bin_at(av, victim_index);
      fwd = bck->fd;
    }
    else {
      victim_index = largebin_index(size);
      bck = bin_at(av, victim_index);
      fwd = bck->fd;
      
      if (fwd != bck) {
        /* if smaller than smallest, place first */
        if ((CHUNK_SIZE_T)(size) < (CHUNK_SIZE_T)(bck->bk->size)) {
          fwd = bck;
          bck = bck->bk;
        }
        else if ((CHUNK_SIZE_T)(size) >= 
                 (CHUNK_SIZE_T)(FIRST_SORTED_BIN_SIZE)) {
          
          /* maintain large bins in sorted order */
          size |= PREV_INUSE; /* Or with inuse bit to speed comparisons */
          while ((CHUNK_SIZE_T)(size) < (CHUNK_SIZE_T)(fwd->size)) 
            fwd = fwd->fd;
          bck = fwd->bk;
        }
      }
    }
      
    mark_bin(av, victim_index);
    victim->bk = bck;
    victim->fd = fwd;
    fwd->bk = victim;
    bck->fd = victim;
  }
  
  /*
    If a large request, scan through the chunks of current bin to
    find one that fits.  (This will be the smallest that fits unless
    FIRST_SORTED_BIN_SIZE has been changed from default.)  This is
    the only step where an unbounded number of chunks might be
    scanned without doing anything useful with them. However the
    lists tend to be short.
  */
  
  if (!in_smallbin_range(nb)) {
    bin = bin_at(av, idx);
    
    for (victim = last(bin); victim != bin; victim = victim->bk) {
      size = chunksize(victim);
      
      if ((CHUNK_SIZE_T)(size) >= (CHUNK_SIZE_T)(nb)) {
        remainder_size = size - nb;
        unlink(victim, bck, fwd);
        
        /* Exhaust */
        if (remainder_size < MINSIZE)  {
          set_inuse_bit_at_offset(victim, size);
          check_malloced_chunk(av, victim, nb);
          return chunk2mem(victim);
        }
        /* Split */
        else {
          remainder = chunk_at_offset(victim, nb);
          unsorted_chunks(av)->bk = unsorted_chunks(av)->fd = remainder;
          remainder->bk = remainder->fd = unsorted_chunks(av);
          set_head(victim, nb | PREV_INUSE);
          set_head(remainder, remainder_size | PREV_INUSE);
          set_foot(remainder, remainder_size);
          check_malloced_chunk(av, victim, nb);
          return chunk2mem(victim);
        } 
      }
    }    
  }

  /*
    Search for a chunk by scanning bins, starting with next largest
    bin. This search is strictly by best-fit; i.e., the smallest
    (with ties going to approximately the least recently used) chunk
    that fits is selected.
    
    The bitmap avoids needing to check that most blocks are nonempty.
  */
    
  ++idx;
  bin = bin_at(av,idx);
  block = idx2block(idx);
  map = av->binmap[block];
  bit = idx2bit(idx);
  
  for (;;) {
    
    /* Skip rest of block if there are no more set bits in this block.  */
    if (bit > map || bit == 0) {
      do {
        if (++block >= BINMAPSIZE)  /* out of bins */
          goto use_top;
      } while ( (map = av->binmap[block]) == 0);
      
      bin = bin_at(av, (block << BINMAPSHIFT));
      bit = 1;
    }
    
    /* Advance to bin with set bit. There must be one. */
    while ((bit & map) == 0) {
      bin = next_bin(bin);
      bit <<= 1;
      assert(bit != 0);
    }
    
    /* Inspect the bin. It is likely to be non-empty */
    victim = last(bin);
    
    /*  If a false alarm (empty bin), clear the bit. */
    if (victim == bin) {
      av->binmap[block] = map &= ~bit; /* Write through */
      bin = next_bin(bin);
      bit <<= 1;
    }
    
    else {
      size = chunksize(victim);
      
      /*  We know the first chunk in this bin is big enough to use. */
      assert((CHUNK_SIZE_T)(size) >= (CHUNK_SIZE_T)(nb));
      
      remainder_size = size - nb;
      
      /* unlink */
      bck = victim->bk;
      bin->bk = bck;
      bck->fd = bin;
      
      /* Exhaust */
      if (remainder_size < MINSIZE) {
        set_inuse_bit_at_offset(victim, size);
        check_malloced_chunk(av, victim, nb);
        return chunk2mem(victim);
      }
      
      /* Split */
      else {
        remainder = chunk_at_offset(victim, nb);
        
        unsorted_chunks(av)->bk = unsorted_chunks(av)->fd = remainder;
        remainder->bk = remainder->fd = unsorted_chunks(av);
        /* advertise as last remainder */
        if (in_smallbin_range(nb)) 
          av->last_remainder = remainder; 
        
        set_head(victim, nb | PREV_INUSE);
        set_head(remainder, remainder_size | PREV_INUSE);
        set_foot(remainder, remainder_size);
        check_malloced_chunk(av, victim, nb);
        return chunk2mem(victim);
      }
    }
  }

  use_top:    
  /*
    If large enough, split off the chunk bordering the end of memory
    (held in av->top). Note that this is in accord with the best-fit
    search rule.  In effect, av->top is treated as larger (and thus
    less well fitting) than any other available chunk since it can
    be extended to be as large as necessary (up to system
    limitations).
    
    We require that av->top always exists (i.e., has size >=
    MINSIZE) after initialization, so if it would otherwise be
    exhuasted by current request, it is replenished. (The main
    reason for ensuring it exists is that we may need MINSIZE space
    to put in fenceposts in sysmalloc.)
  */
  
  victim = av->top;
  size = chunksize(victim);
  
  if ((CHUNK_SIZE_T)(size) >= (CHUNK_SIZE_T)(nb + MINSIZE)) {
    remainder_size = size - nb;
    remainder = chunk_at_offset(victim, nb);
    av->top = remainder;
    set_head(victim, nb | PREV_INUSE);
    set_head(remainder, remainder_size | PREV_INUSE);
    
    check_malloced_chunk(av, victim, nb);
    return chunk2mem(victim);
  }
  
  /* 
     If no space in top, relay to handle system-dependent cases 
  */
  return sYSMALLOc(nb, av);    
}

/*
  ------------------------------ free ------------------------------
*/

void fREe(mstate av, Void_t* mem)
{
  mchunkptr       p;           /* chunk corresponding to mem */
  INTERNAL_SIZE_T size;        /* its size */
  mfastbinptr*    fb;          /* associated fastbin */
  mchunkptr       nextchunk;   /* next contiguous chunk */
  INTERNAL_SIZE_T nextsize;    /* its size */
  int             nextinuse;   /* true if nextchunk is used */
  INTERNAL_SIZE_T prevsize;    /* size of previous contiguous chunk */
  mchunkptr       bck;         /* misc temp for linking */
  mchunkptr       fwd;         /* misc temp for linking */

  /* free(0) has no effect */
  if (mem != 0) {
    p = mem2chunk(mem);
    size = chunksize(p);

	check_heap(av,p);
    check_inuse_chunk(av,p);

    /*
      If eligible, place chunk on a fastbin so it can be found
      and used quickly in malloc.
    */

    if ((CHUNK_SIZE_T)(size) <= (CHUNK_SIZE_T)(av->max_fast)

#if TRIM_FASTBINS
        /* 
           If TRIM_FASTBINS set, don't place chunks
           bordering top into fastbins
        */
        && (chunk_at_offset(p, size) != av->top)
#endif
        ) {

      set_fastchunks(av);
      fb = &(av->fastbins[fastbin_index(size)]);
      p->fd = *fb;
      *fb = p;
    }

    /*
       Consolidate other non-mmapped chunks as they arrive.
    */

    else if (!chunk_is_mmapped(p)) {
      set_anychunks(av);

      nextchunk = chunk_at_offset(p, size);
      nextsize = chunksize(nextchunk);

      /* consolidate backward */
      if (!prev_inuse(p)) {
        prevsize = p->prev_size;
        size += prevsize;
        p = chunk_at_offset(p, -((long) prevsize));
        unlink(p, bck, fwd);
      }

      if (nextchunk != av->top) {
        /* get and clear inuse bit */
        nextinuse = inuse_bit_at_offset(nextchunk, nextsize);
        set_head(nextchunk, nextsize);

        /* consolidate forward */
        if (!nextinuse) {
          unlink(nextchunk, bck, fwd);
          size += nextsize;
        }

        /*
          Place the chunk in unsorted chunk list. Chunks are
          not placed into regular bins until after they have
          been given one chance to be used in malloc.
        */

        bck = unsorted_chunks(av);
        fwd = bck->fd;
        p->bk = bck;
        p->fd = fwd;
        bck->fd = p;
        fwd->bk = p;

        set_head(p, size | PREV_INUSE);
        set_foot(p, size);
        
        check_free_chunk(av,p);
      }

      /*
         If the chunk borders the current high end of memory,
         consolidate into top
      */

      else {
        size += nextsize;
        set_head(p, size | PREV_INUSE);
        av->top = p;
        check_chunk(av,p);
      }

      /*
        If freeing a large space, consolidate possibly-surrounding
        chunks. Then, if the total unused topmost memory exceeds trim
        threshold, ask malloc_trim to reduce top.

        Unless max_fast is 0, we don't know if there are fastbins
        bordering top, so we cannot tell for sure whether threshold
        has been reached unless fastbins are consolidated.  But we
        don't want to consolidate on each free.  As a compromise,
        consolidation is performed if FASTBIN_CONSOLIDATION_THRESHOLD
        is reached.
      */

      if ((CHUNK_SIZE_T)(size) >= FASTBIN_CONSOLIDATION_THRESHOLD) { 
        if (have_fastchunks(av)) 
          malloc_consolidate(av);

#ifndef MORECORE_CANNOT_TRIM        
        if ((CHUNK_SIZE_T)(chunksize(av->top)) >= 
            (CHUNK_SIZE_T)(av->trim_threshold))
          sYSTRIm(av->top_pad, av);
#endif
      }

    }
    /*
      If the chunk was allocated via mmap, release via munmap()
      Note that if HAVE_MMAP is false but chunk_is_mmapped is
      true, then user must have overwritten memory. There's nothing
      we can do to catch this error unless DEBUG is set, in which case
      check_inuse_chunk (above) will have triggered error.
    */

    else {
#if HAVE_MMAP
      int ret;
      INTERNAL_SIZE_T offset = p->prev_size;
      av->n_mmaps--;
      av->mmapped_mem -= (size + offset);
	  unlink_region(av, (char*)p - offset, size + offset);
      ret = munmap((char*)p - offset, size + offset);
      /* munmap returns non-zero on failure */
      assert(ret == 0);
#endif
    }
  }
}

/*
  ------------------------- malloc_consolidate -------------------------

  malloc_consolidate is a specialized version of free() that tears
  down chunks held in fastbins.  Free itself cannot be used for this
  purpose since, among other things, it might place chunks back onto
  fastbins.  So, instead, we need to use a minor variant of the same
  code.
  
  Also, because this routine needs to be called the first time through
  malloc anyway, it turns out to be the perfect place to trigger
  initialization code.
*/

static void malloc_consolidate(mstate av)
{
  mfastbinptr*    fb;                 /* current fastbin being consolidated */
  mfastbinptr*    maxfb;              /* last fastbin (for loop control) */
  mchunkptr       p;                  /* current chunk being consolidated */
  mchunkptr       nextp;              /* next chunk to consolidate */
  mchunkptr       unsorted_bin;       /* bin header */
  mchunkptr       first_unsorted;     /* chunk to link to */

  /* These have same use as in free() */
  mchunkptr       nextchunk;
  INTERNAL_SIZE_T size;
  INTERNAL_SIZE_T nextsize;
  INTERNAL_SIZE_T prevsize;
  int             nextinuse;
  mchunkptr       bck;
  mchunkptr       fwd;

  /*
    If max_fast is 0, we know that av hasn't
    yet been initialized, in which case do so below
  */

  if (av->max_fast != 0) {
    clear_fastchunks(av);

    unsorted_bin = unsorted_chunks(av);

    /*
      Remove each chunk from fast bin and consolidate it, placing it
      then in unsorted bin. Among other reasons for doing this,
      placing in unsorted bin avoids needing to calculate actual bins
      until malloc is sure that chunks aren't immediately going to be
      reused anyway.
    */
    
    maxfb = &(av->fastbins[fastbin_index(av->max_fast)]);
    fb = &(av->fastbins[0]);
    do {
      if ( (p = *fb) != 0) {
        *fb = 0;
        
        do {
          check_inuse_chunk(av,p);
          nextp = p->fd;
          
          /* Slightly streamlined version of consolidation code in free() */
          size = p->size & ~PREV_INUSE;
          nextchunk = chunk_at_offset(p, size);
          nextsize = chunksize(nextchunk);
          
          if (!prev_inuse(p)) {
            prevsize = p->prev_size;
            size += prevsize;
            p = chunk_at_offset(p, -((long) prevsize));
            unlink(p, bck, fwd);
          }
          
          if (nextchunk != av->top) {
            nextinuse = inuse_bit_at_offset(nextchunk, nextsize);
            set_head(nextchunk, nextsize);
            
            if (!nextinuse) {
              size += nextsize;
              unlink(nextchunk, bck, fwd);
            }
            
            first_unsorted = unsorted_bin->fd;
            unsorted_bin->fd = p;
            first_unsorted->bk = p;
            
            set_head(p, size | PREV_INUSE);
            p->bk = unsorted_bin;
            p->fd = first_unsorted;
            set_foot(p, size);
          }
          
          else {
            size += nextsize;
            set_head(p, size | PREV_INUSE);
            av->top = p;
          }
          
        } while ( (p = nextp) != 0);
        
      }
    } while (fb++ != maxfb);
  }
  else {
    malloc_init_state(av);
    check_malloc_state(av);
  }
}

/*
  ------------------------------ realloc ------------------------------
*/


Void_t* rEALLOc(mstate av, Void_t* oldmem, size_t bytes)
{
  INTERNAL_SIZE_T  nb;              /* padded request size */

  mchunkptr        oldp;            /* chunk corresponding to oldmem */
  INTERNAL_SIZE_T  oldsize;         /* its size */

  mchunkptr        newp;            /* chunk to return */
  INTERNAL_SIZE_T  newsize;         /* its size */
  Void_t*          newmem;          /* corresponding user mem */

  mchunkptr        next;            /* next contiguous chunk after oldp */

  mchunkptr        remainder;       /* extra space at end of newp */
  CHUNK_SIZE_T     remainder_size;  /* its size */

  mchunkptr        bck;             /* misc temp for linking */
  mchunkptr        fwd;             /* misc temp for linking */

  CHUNK_SIZE_T     copysize;        /* bytes to copy */
  unsigned int     ncopies;         /* INTERNAL_SIZE_T words to copy */
  INTERNAL_SIZE_T* s;               /* copy source */ 
  INTERNAL_SIZE_T* d;               /* copy destination */


#ifdef REALLOC_ZERO_BYTES_FREES
  if (bytes == 0) {
    fREe(oldmem);
    return 0;
  }
#endif

  /* realloc of null is supposed to be same as malloc */
  if (oldmem == 0) return mALLOc(av, bytes);

  checked_request2size(bytes, nb);

  oldp    = mem2chunk(oldmem);
  oldsize = chunksize(oldp);

  check_heap(av,oldp);
  check_inuse_chunk(av,oldp);

  if (!chunk_is_mmapped(oldp)) {

    if ((CHUNK_SIZE_T)(oldsize) >= (CHUNK_SIZE_T)(nb)) {
      /* already big enough; split below */
      newp = oldp;
      newsize = oldsize;
    }

    else {
      next = chunk_at_offset(oldp, oldsize);

      /* Try to expand forward into top */
      if (next == av->top &&
          (CHUNK_SIZE_T)(newsize = oldsize + chunksize(next)) >=
          (CHUNK_SIZE_T)(nb + MINSIZE)) {
        set_head_size(oldp, nb);
        av->top = chunk_at_offset(oldp, nb);
        set_head(av->top, (newsize - nb) | PREV_INUSE);
        return chunk2mem(oldp);
      }
      
      /* Try to expand forward into next chunk;  split off remainder below */
      else if (next != av->top && 
               !inuse(next) &&
               (CHUNK_SIZE_T)(newsize = oldsize + chunksize(next)) >=
               (CHUNK_SIZE_T)(nb)) {
        newp = oldp;
        unlink(next, bck, fwd);
      }

      /* allocate, copy, free */
      else {
        newmem = mALLOc(av, nb - MALLOC_ALIGN_MASK);
        if (newmem == 0)
          return 0; /* propagate failure */
      
        newp = mem2chunk(newmem);
        newsize = chunksize(newp);
        
        /*
          Avoid copy if newp is next chunk after oldp.
        */
        if (newp == next) {
          newsize += oldsize;
          newp = oldp;
        }
        else {
          /*
            Unroll copy of <= 36 bytes (72 if 8byte sizes)
            We know that contents have an odd number of
            INTERNAL_SIZE_T-sized words; minimally 3.
          */
          
          copysize = oldsize - SIZE_SZ;
          s = (INTERNAL_SIZE_T*)(oldmem);
          d = (INTERNAL_SIZE_T*)(newmem);
          ncopies = copysize / sizeof(INTERNAL_SIZE_T);
          assert(ncopies >= 3);
          
          if (ncopies > 9)
            MALLOC_COPY(d, s, copysize);
          
          else {
            *(d+0) = *(s+0);
            *(d+1) = *(s+1);
            *(d+2) = *(s+2);
            if (ncopies > 4) {
              *(d+3) = *(s+3);
              *(d+4) = *(s+4);
              if (ncopies > 6) {
                *(d+5) = *(s+5);
                *(d+6) = *(s+6);
                if (ncopies > 8) {
                  *(d+7) = *(s+7);
                  *(d+8) = *(s+8);
                }
              }
            }
          }
          
          fREe(av, oldmem);
          check_inuse_chunk(av,newp);
          return chunk2mem(newp);
        }
      }
    }

    /* If possible, free extra space in old or extended chunk */

    assert((CHUNK_SIZE_T)(newsize) >= (CHUNK_SIZE_T)(nb));

    remainder_size = newsize - nb;

    if (remainder_size < MINSIZE) { /* not enough extra to split off */
      set_head_size(newp, newsize);
      set_inuse_bit_at_offset(newp, newsize);
    }
    else { /* split remainder */
      remainder = chunk_at_offset(newp, nb);
      set_head_size(newp, nb);
      set_head(remainder, remainder_size | PREV_INUSE);
      /* Mark remainder as inuse so free() won't complain */
      set_inuse_bit_at_offset(remainder, remainder_size);
      fREe(av, chunk2mem(remainder)); 
    }

    check_inuse_chunk(av,newp);
    return chunk2mem(newp);
  }

  /*
    Handle mmap cases
  */

  else {
#if HAVE_MMAP

#if HAVE_MREMAP
    INTERNAL_SIZE_T offset = oldp->prev_size;
    size_t pagemask = av->pagesize - 1;
    char *cp;
    CHUNK_SIZE_T  sum;
    
    /* Note the extra SIZE_SZ overhead */
    newsize = (nb + offset + LINK_SZ + SIZE_SZ + pagemask) & ~pagemask;

    /* don't need to remap if still within same page */
    if (oldsize == newsize - offset) 
      return oldmem;

	unlink_region(av, (char*)oldp - offset, oldsize + offset);
    cp = (char*)mremap((char*)oldp - offset, oldsize + offset, newsize, 1);
    
    if (cp != (char*)MORECORE_FAILURE) {
	  link_region(av, cp, newsize);

      newp = (mchunkptr)(cp + offset);
      set_head(newp, (newsize - offset)|IS_MMAPPED);
      
      assert(aligned_OK(chunk2mem(newp)));
      assert((newp->prev_size == offset));
      
      /* update statistics */
      sum = av->mmapped_mem += newsize - oldsize;
      if (sum > (CHUNK_SIZE_T)(av->max_mmapped_mem)) 
        av->max_mmapped_mem = sum;
      sum += av->sbrked_mem;
      if (sum > (CHUNK_SIZE_T)(av->max_total_mem)) 
        av->max_total_mem = sum;
      
      return chunk2mem(newp);
    }
#endif

    /* Note the extra SIZE_SZ overhead. */
    if ((CHUNK_SIZE_T)(oldsize) >= (CHUNK_SIZE_T)(nb + SIZE_SZ)) 
      newmem = oldmem; /* do nothing */
    else {
      /* Must alloc, copy, free. */
      newmem = mALLOc(av,nb - MALLOC_ALIGN_MASK);
      if (newmem != 0) {
        MALLOC_COPY(newmem, oldmem, oldsize - 2*SIZE_SZ);
        fREe(av,oldmem);
      }
    }
    return newmem;

#else 
    /* If !HAVE_MMAP, but chunk_is_mmapped, user must have overwritten mem */
    check_malloc_state(av);
    MALLOC_FAILURE_ACTION;
    return 0;
#endif
  }
}

/*
  ------------------------------ memalign ------------------------------
*/

Void_t* mEMALIGn(mstate av, size_t alignment, size_t bytes)
{
  INTERNAL_SIZE_T nb;             /* padded  request size */
  char*           m;              /* memory returned by malloc call */
  mchunkptr       p;              /* corresponding chunk */
  char*           brk;            /* alignment point within p */
  mchunkptr       newp;           /* chunk to return */
  INTERNAL_SIZE_T newsize;        /* its size */
  INTERNAL_SIZE_T leadsize;       /* leading space before alignment point */
  mchunkptr       remainder;      /* spare room at end to split off */
  CHUNK_SIZE_T    remainder_size; /* its size */
  INTERNAL_SIZE_T size;

  /* If need less alignment than we give anyway, just relay to malloc */

  if (alignment <= MALLOC_ALIGNMENT) return mALLOc(av, bytes);

  /* Otherwise, ensure that it is at least a minimum chunk size */

  if (alignment <  MINSIZE) alignment = MINSIZE;

  /* Make sure alignment is power of 2 (in case MINSIZE is not).  */
  if ((alignment & (alignment - 1)) != 0) {
    size_t a = MALLOC_ALIGNMENT * 2;
    while ((CHUNK_SIZE_T)a < (CHUNK_SIZE_T)alignment) a <<= 1;
    alignment = a;
  }

  checked_request2size(bytes, nb);

  /*
    Strategy: find a spot within that chunk that meets the alignment
    request, and then possibly free the leading and trailing space.
  */


  /* Call malloc with worst case padding to hit alignment. */

  m  = (char*)(mALLOc(av, nb + alignment + MINSIZE));

  if (m == 0) return 0; /* propagate failure */

  p = mem2chunk(m);

  if ((((PTR_UINT)(m)) % alignment) != 0) { /* misaligned */

    /*
      Find an aligned spot inside chunk.  Since we need to give back
      leading space in a chunk of at least MINSIZE, if the first
      calculation places us at a spot with less than MINSIZE leader,
      we can move to the next aligned spot -- we've allocated enough
      total room so that this is always possible.
    */

    brk = (char*)mem2chunk((PTR_UINT)(((PTR_UINT)(m + alignment - 1)) &
                           -((signed long) alignment)));
    if ((CHUNK_SIZE_T)(brk - (char*)(p)) < MINSIZE)
      brk += alignment;

    newp = (mchunkptr)brk;
    leadsize = brk - (char*)(p);
    newsize = chunksize(p) - leadsize;

    /* For mmapped chunks, just adjust offset */
    if (chunk_is_mmapped(p)) {
      newp->prev_size = p->prev_size + leadsize;
      set_head(newp, newsize|IS_MMAPPED);
      return chunk2mem(newp);
    }

    /* Otherwise, give back leader, use the rest */
    set_head(newp, newsize | PREV_INUSE);
    set_inuse_bit_at_offset(newp, newsize);
    set_head_size(p, leadsize);
    fREe(av, chunk2mem(p));
    p = newp;

    assert (newsize >= nb &&
            (((PTR_UINT)(chunk2mem(p))) % alignment) == 0);
  }

  /* Also give back spare room at the end */
  if (!chunk_is_mmapped(p)) {
    size = chunksize(p);
    if ((CHUNK_SIZE_T)(size) > (CHUNK_SIZE_T)(nb + MINSIZE)) {
      remainder_size = size - nb;
      remainder = chunk_at_offset(p, nb);
      set_head(remainder, remainder_size | PREV_INUSE);
      set_head_size(p, nb);
      fREe(av, chunk2mem(remainder));
    }
  }

  check_inuse_chunk(av,p);
  return chunk2mem(p);
}

/*
  ------------------------------ calloc ------------------------------
*/

Void_t* cALLOc(mstate av, size_t n_elements, size_t elem_size)
{
  mchunkptr p;
  CHUNK_SIZE_T  clearsize;
  CHUNK_SIZE_T  nclears;
  INTERNAL_SIZE_T* d;

  Void_t* mem = mALLOc(av, n_elements * elem_size);

  if (mem != 0) {
    p = mem2chunk(mem);

    if (!chunk_is_mmapped(p))
    {  
      /*
        Unroll clear of <= 36 bytes (72 if 8byte sizes)
        We know that contents have an odd number of
        INTERNAL_SIZE_T-sized words; minimally 3.
      */

      d = (INTERNAL_SIZE_T*)mem;
      clearsize = chunksize(p) - SIZE_SZ;
      nclears = clearsize / sizeof(INTERNAL_SIZE_T);
      assert(nclears >= 3);

      if (nclears > 9)
        MALLOC_ZERO(d, clearsize);

      else {
        *(d+0) = 0;
        *(d+1) = 0;
        *(d+2) = 0;
        if (nclears > 4) {
          *(d+3) = 0;
          *(d+4) = 0;
          if (nclears > 6) {
            *(d+5) = 0;
            *(d+6) = 0;
            if (nclears > 8) {
              *(d+7) = 0;
              *(d+8) = 0;
            }
          }
        }
      }
    }
#if ! MMAP_CLEARS
    else
    {
      d = (INTERNAL_SIZE_T*)mem;
      /*
        Note the additional SIZE_SZ
      */
      clearsize = chunksize(p) - 2*SIZE_SZ;
      MALLOC_ZERO(d, clearsize);
    }
#endif
  }
  return mem;
}

/*
  ------------------------------ cfree ------------------------------
*/

void cFREe(mstate av, Void_t *mem)
{
  fREe(av, mem);
}

/*
  ------------------------- independent_calloc -------------------------
*/

Void_t** iCALLOc(mstate av,size_t n_elements, size_t elem_size, Void_t* chunks[])
{
  size_t sz = elem_size; /* serves as 1-element array */
  /* opts arg of 3 means all elements are same size, and should be cleared */
  return iALLOc(av, n_elements, &sz, 3, chunks);
}

/*
  ------------------------- independent_comalloc -------------------------
*/

Void_t** iCOMALLOc(mstate av, size_t n_elements, size_t sizes[], Void_t* chunks[])
{
  return iALLOc(av, n_elements, sizes, 0, chunks);
}


/*
  ------------------------------ ialloc ------------------------------
  ialloc provides common support for independent_X routines, handling all of
  the combinations that can result.

  The opts arg has:
    bit 0 set if all elements are same size (using sizes[0])
    bit 1 set if elements should be zeroed
*/


static Void_t** iALLOc(mstate av, size_t n_elements, size_t* sizes, int opts, Void_t* chunks[])
{
  INTERNAL_SIZE_T element_size;   /* chunksize of each element, if all same */
  INTERNAL_SIZE_T contents_size;  /* total size of elements */
  INTERNAL_SIZE_T array_size;     /* request size of pointer array */
  Void_t*         mem;            /* malloced aggregate space */
  mchunkptr       p;              /* corresponding chunk */
  INTERNAL_SIZE_T remainder_size; /* remaining bytes while splitting */
  Void_t**        marray;         /* either "chunks" or malloced ptr array */
  mchunkptr       array_chunk;    /* chunk for malloced ptr array */
  int             mmx;            /* to disable mmap */
  INTERNAL_SIZE_T size;           
  size_t          i;

  /* Ensure initialization */
  if (av->max_fast == 0) malloc_consolidate(av);

  /* compute array length, if needed */
  if (chunks != 0) {
    if (n_elements == 0)
      return chunks; /* nothing to do */
    marray = chunks;
    array_size = 0;
  }
  else {
    /* if empty req, must still return chunk representing empty array */
    if (n_elements == 0) 
      return (Void_t**) mALLOc(av, 0);
    marray = 0;
    array_size = request2size(n_elements * (sizeof(Void_t*)));
  }

  /* compute total element size */
  if (opts & 0x1) { /* all-same-size */
    element_size = request2size(*sizes);
    contents_size = n_elements * element_size;
  }
  else { /* add up all the sizes */
    element_size = 0;
    contents_size = 0;
    for (i = 0; i != n_elements; ++i) 
      contents_size += request2size(sizes[i]);     
  }

  /* subtract out alignment bytes from total to minimize overallocation */
  size = contents_size + array_size - MALLOC_ALIGN_MASK;
  
  /* 
     Allocate the aggregate chunk.
     But first disable mmap so malloc won't use it, since
     we would not be able to later free/realloc space internal
     to a segregated mmap region.
 */
  mmx = av->n_mmaps_max;   /* disable mmap */
  av->n_mmaps_max = 0;
  mem = mALLOc(av, size);
  av->n_mmaps_max = mmx;   /* reset mmap */
  if (mem == 0) 
    return 0;

  p = mem2chunk(mem);
  assert(!chunk_is_mmapped(p)); 
  remainder_size = chunksize(p);

  if (opts & 0x2) {       /* optionally clear the elements */
    MALLOC_ZERO(mem, remainder_size - SIZE_SZ - array_size);
  }

  /* If not provided, allocate the pointer array as final part of chunk */
  if (marray == 0) {
    array_chunk = chunk_at_offset(p, contents_size);
    marray = (Void_t**) (chunk2mem(array_chunk));
    set_head(array_chunk, (remainder_size - contents_size) | PREV_INUSE);
    remainder_size = contents_size;
  }

  /* split out elements */
  for (i = 0; ; ++i) {
    marray[i] = chunk2mem(p);
    if (i != n_elements-1) {
      if (element_size != 0) 
        size = element_size;
      else
        size = request2size(sizes[i]);          
      remainder_size -= size;
      set_head(p, size | PREV_INUSE);
      p = chunk_at_offset(p, size);
    }
    else { /* the final element absorbs any overallocation slop */
      set_head(p, remainder_size | PREV_INUSE);
      break;
    }
  }

#if DEBUG
  if (marray != chunks) {
    /* final element must have exactly exhausted chunk */
    if (element_size != 0) 
      assert(remainder_size == element_size);
    else
      assert(remainder_size == request2size(sizes[i]));
    check_inuse_chunk(av,mem2chunk(marray));
  }

  for (i = 0; i != n_elements; ++i)
    check_inuse_chunk(av,mem2chunk(marray[i]));
#endif

  return marray;
}


/*
  ------------------------------ valloc ------------------------------
*/

Void_t* vALLOc(mstate av, size_t bytes)
{
  /* Ensure initialization */
  if (av->max_fast == 0) malloc_consolidate(av);
  return mEMALIGn(av, av->pagesize, bytes);
}

/*
  ------------------------------ pvalloc ------------------------------
*/

Void_t* pVALLOc(mstate av, size_t bytes)
{
  size_t pagesz;

  /* Ensure initialization */
  if (av->max_fast == 0) malloc_consolidate(av);
  pagesz = av->pagesize;
  return mEMALIGn(av, pagesz, (bytes + pagesz - 1) & ~(pagesz - 1));
}
   

/*
  ------------------------------ malloc_trim ------------------------------
*/

int mTRIm(mstate av, size_t pad)
{
  /* Ensure initialization/consolidation */
  malloc_consolidate(av);

#ifndef MORECORE_CANNOT_TRIM        
  return sYSTRIm(pad, av);
#else
  return 0;
#endif
}


/*
  ------------------------- malloc_usable_size -------------------------
*/

size_t mUSABLe(Void_t* mem)
{
  mchunkptr p;
  if (mem != 0) {
    p = mem2chunk(mem);
    if (chunk_is_mmapped(p))
      return chunksize(p) - 2*SIZE_SZ;
    else if (inuse(p))
      return chunksize(p) - SIZE_SZ;
  }
  return 0;
}

/*
  ------------------------------ mallinfo ------------------------------
*/

struct mallinfo mALLINFo(mstate av)
{
  struct mallinfo mi;
  int i;
  mbinptr b;
  mchunkptr p;
  INTERNAL_SIZE_T avail;
  INTERNAL_SIZE_T fastavail;
  int nblocks;
  int nfastblocks;

  /* Ensure initialization */
  if (av->top == 0)  malloc_consolidate(av);

  check_malloc_state(av);

  /* Account for top */
  avail = chunksize(av->top);
  nblocks = 1;  /* top always exists */

  /* traverse fastbins */
  nfastblocks = 0;
  fastavail = 0;

  for (i = 0; (unsigned)i < NFASTBINS; ++i) {
    for (p = av->fastbins[i]; p != 0; p = p->fd) {
      ++nfastblocks;
      fastavail += chunksize(p);
    }
  }

  avail += fastavail;

  /* traverse regular bins */
  for (i = 1; i < NBINS; ++i) {
    b = bin_at(av, i);
    for (p = last(b); p != b; p = p->bk) {
      ++nblocks;
      avail += chunksize(p);
    }
  }

  mi.smblks = nfastblocks;
  mi.ordblks = nblocks;
  mi.fordblks = avail;
  mi.uordblks = av->sbrked_mem - avail;
  mi.arena = av->sbrked_mem;
  mi.hblks = av->n_mmaps;
  mi.hblkhd = av->mmapped_mem;
  mi.fsmblks = fastavail;
  mi.keepcost = chunksize(av->top);
  mi.usmblks = av->max_total_mem;
  return mi;
}

/*
  ------------------------------ malloc_stats ------------------------------
*/

void mSTATs(mstate av)
{
  struct mallinfo mi = mALLINFo(av);

#ifdef WIN32
  {
    CHUNK_SIZE_T  free, reserved, committed;
    vminfo (&free, &reserved, &committed);
    fprintf(stderr, "free bytes       = %10lu\n", 
            free);
    fprintf(stderr, "reserved bytes   = %10lu\n", 
            reserved);
    fprintf(stderr, "committed bytes  = %10lu\n", 
            committed);
  }
#endif


  fprintf(stderr, "max system bytes = %10lu\n",
          (CHUNK_SIZE_T)(mi.usmblks));
  fprintf(stderr, "system bytes     = %10lu\n",
          (CHUNK_SIZE_T)(mi.arena + mi.hblkhd));
  fprintf(stderr, "in use bytes     = %10lu\n",
          (CHUNK_SIZE_T)(mi.uordblks + mi.hblkhd));

#ifdef WIN32 
  {
    CHUNK_SIZE_T  kernel, user;
    if (cpuinfo (TRUE, &kernel, &user)) {
      fprintf(stderr, "kernel ms        = %10lu\n", 
              kernel);
      fprintf(stderr, "user ms          = %10lu\n", 
              user);
    }
  }
#endif
}


/*
  ------------------------------ mallopt ------------------------------
*/

int mALLOPt(mstate av, int param_number, int value)
{
  /* Ensure initialization/consolidation */
  malloc_consolidate(av);

  switch(param_number) {
  case M_MXFAST:
    if (value >= 0 && value <= MAX_FAST_SIZE) {
      set_max_fast(av, value);
      return 1;
    }
    else
      return 0;

  case M_TRIM_THRESHOLD:
    av->trim_threshold = value;
    return 1;

  case M_TOP_PAD:
    av->top_pad = value;
    return 1;

  case M_MMAP_THRESHOLD:
    av->mmap_threshold = value;
    return 1;

  case M_MMAP_MAX:
#if !HAVE_MMAP
    if (value != 0)
      return 0;
#endif
    av->n_mmaps_max = value;
    return 1;

  default:
    return 0;
  }
}

void rELEASe(mstate av)
{
	size_t sz=av->regions.prev_size;
	for (struct malloc_region *reg=(struct malloc_region*)av->regions.prev; reg!=NULL; ) {
		void *p=reg; size_t prev_sz=reg->prev_size; reg=(struct malloc_region*)reg->prev; munmap(p,sz); sz=prev_sz;
	}
}

static void link_region(mstate av,void *reg,size_t size)
{
	if (reg!=NULL) {
		*(struct malloc_region*)reg=av->regions;
		av->regions.prev=reg;
		av->regions.prev_size=size;
	}
}

static void unlink_region(mstate av,void *rg,size_t sz)
{
	for (struct malloc_region *reg=&av->regions; reg->prev!=NULL; reg=(struct malloc_region*)reg->prev)
		if (reg->prev==rg) {
			assert(reg->prev_size==sz);
			reg->prev=((struct malloc_region*)rg)->prev;
			reg->prev_size=((struct malloc_region*)rg)->prev_size;
			return;
		}
	// report not found!!!
}

#ifdef WIN32

#ifdef _DEBUG
/* #define TRACE */
#endif

/* getpagesize for windows */
static long getpagesize (void) {
    static long g_pagesize = 0;
    if (! g_pagesize) {
        SYSTEM_INFO system_info;
        GetSystemInfo (&system_info);
        g_pagesize = system_info.dwPageSize;
    }
    return g_pagesize;
}
static long getregionsize (void) {
    static long g_regionsize = 0;
    if (! g_regionsize) {
        SYSTEM_INFO system_info;
        GetSystemInfo (&system_info);
        g_regionsize = system_info.dwAllocationGranularity;
    }
    return g_regionsize;
}

/* mmap for windows */
static void *mmap (void *ptr, long size, long prot, long type, long handle, long arg) {
    static long g_pagesize;
    static long g_regionsize;
#ifdef TRACE
    printf ("mmap %d\n", size);
#endif
    /* First time initialization */
    if (! g_pagesize) 
        g_pagesize = getpagesize ();
    if (! g_regionsize) 
        g_regionsize = getregionsize ();
    /* Assert preconditions */
    assert ((unsigned) ptr % g_regionsize == 0);
    assert (size % g_pagesize == 0);
    /* Allocate this */
    ptr = VirtualAlloc (ptr, size,
					    MEM_RESERVE | MEM_COMMIT | MEM_TOP_DOWN, PAGE_READWRITE);
    if (! ptr) {
        ptr = (void *) MORECORE_FAILURE;
        goto mmap_exit;
    }
    /* Assert postconditions */
    assert ((unsigned) ptr % g_regionsize == 0);
#ifdef TRACE
    printf ("Commit %p %d\n", ptr, size);
#endif
mmap_exit:
    return ptr;
}

/* munmap for windows */
static long munmap (void *ptr, long size) {
    static long g_pagesize;
    static long g_regionsize;
    int rc = MUNMAP_FAILURE;
#ifdef TRACE
    printf ("munmap %p %d\n", ptr, size);
#endif
    /* First time initialization */
    if (! g_pagesize) 
        g_pagesize = getpagesize ();
    if (! g_regionsize) 
        g_regionsize = getregionsize ();
    /* Assert preconditions */
    assert ((unsigned) ptr % g_regionsize == 0);
    assert (size % g_pagesize == 0);
    /* Free this */
    if (! VirtualFree (ptr, 0, 
                       MEM_RELEASE))
        goto munmap_exit;
    rc = 0;
#ifdef TRACE
    printf ("Release %p %d\n", ptr, size);
#endif
munmap_exit:
    return rc;
}

static void vminfo (CHUNK_SIZE_T  *free, CHUNK_SIZE_T  *reserved, CHUNK_SIZE_T  *committed) {
    MEMORY_BASIC_INFORMATION memory_info;
    memory_info.BaseAddress = 0;
    *free = *reserved = *committed = 0;
    while (VirtualQuery (memory_info.BaseAddress, &memory_info, sizeof (memory_info))) {
        switch (memory_info.State) {
        case MEM_FREE:
            *free += memory_info.RegionSize;
            break;
        case MEM_RESERVE:
            *reserved += memory_info.RegionSize;
            break;
        case MEM_COMMIT:
            *committed += memory_info.RegionSize;
            break;
        }
        memory_info.BaseAddress = (char *) memory_info.BaseAddress + memory_info.RegionSize;
    }
}

static int cpuinfo (int whole, CHUNK_SIZE_T  *kernel, CHUNK_SIZE_T  *user) {
    if (whole) {
        __int64 creation64, exit64, kernel64, user64;
        int rc = GetProcessTimes (GetCurrentProcess (), 
                                  (FILETIME *) &creation64,  
                                  (FILETIME *) &exit64, 
                                  (FILETIME *) &kernel64, 
                                  (FILETIME *) &user64);
        if (! rc) {
            *kernel = 0;
            *user = 0;
            return FALSE;
        } 
        *kernel = (CHUNK_SIZE_T) (kernel64 / 10000);
        *user = (CHUNK_SIZE_T) (user64 / 10000);
        return TRUE;
    } else {
        __int64 creation64, exit64, kernel64, user64;
        int rc = GetThreadTimes (GetCurrentThread (), 
                                 (FILETIME *) &creation64,  
                                 (FILETIME *) &exit64, 
                                 (FILETIME *) &kernel64, 
                                 (FILETIME *) &user64);
        if (! rc) {
            *kernel = 0;
            *user = 0;
            return FALSE;
        } 
        *kernel = (CHUNK_SIZE_T) (kernel64 / 10000);
        *user = (CHUNK_SIZE_T) (user64 / 10000);
        return TRUE;
    }
}

#endif /* WIN32 */
