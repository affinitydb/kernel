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

Written by Mark Venguerov 2004-2013

**************************************************************************************/

#ifndef _AFYUTILS_H_
#define _AFYUTILS_H_

#include "afysync.h"
#include <stddef.h>

namespace Afy
{

/**
 * Next Power of 2 (H. Warren, Hacker's Delight, 2003, p.48)
 */
__forceinline unsigned nextP2(unsigned x)
{
	x = x - 1;
	x = x | x >> 1;
	x = x | x >> 2;
	x = x | x >> 4;
	x = x | x >> 8;
	x = x | x >> 16;
	return x + 1;
}

/**
 * Number of 1-bits in 32-bit number (H. Warren, Hacker's Delight, 2003, p.66)
 */
__forceinline int pop(unsigned x)
{
	x = x - (x >> 1 & 0x55555555);
	x = (x & 0x33333333) + (x >> 2 & 0x33333333);
	x = x + (x>>4) & 0x0f0f0f0f; x += x>>8;
	return x+(x>>16)&0x3f;
}

/**
 * Number of 1-bits in 16-bit number
 */
__forceinline int pop16(unsigned short x)
{
	x = x - (x >> 1 & 0x5555);
	x = (x & 0x3333) + (x >> 2 & 0x3333);
	x = x + (x>>4) & 0x0f0f; 
	return x + (x>>8) & 0x1f;
}

/**
 * Number of 1-bits in 16-bit number
 */
__forceinline int pop8(unsigned char x)
{
	x = x - (x >> 1 & 0x55);
	x = (x & 0x33) + (x >> 2 & 0x33);
	return x + (x>>4) & 0x0f; 
}

/**
 * Number of leading zeros in 32-bit number (H. Warren, Hacker's Delight, 2003)
 */
__forceinline int nlz(unsigned x)
{
	x|=x>>1; x|=x>>2; x|=x>>4; x|=x>>8; return pop(~(x|x>>16));
}

/**
 * Number of trailing zeros in 32-bit number (H. Warren, Hacker's Delight, 2003)
 */
__forceinline int ntz(unsigned x)
{
	return pop(~x&x-1);
}

/**
 * 3-way comparison, returns -1,0,1 (H. Warren, Hacker's Delight, 2003)
 */
template<typename T> __forceinline int cmp3(T x,T y)
{
	return (x>y)-(x<y);
}

/**
 * sign function, returns -1,0,1 (H. Warren, Hacker's Delight, 2003)
 */
template<typename T> __forceinline int sign(T x)
{
	return (x>0)-(x<0);
}

/**
 * min function: a<b?a:b
 */
#undef min
template<typename T> __forceinline T min(T x,T y)
{
	return x<y?x:y;
}

/**
 * max function: a>b?a:b
 */
#undef max
template<typename T> __forceinline T max(T x,T y)
{
	return x>y?x:y;
}

template<typename T> class DefCmp
{
public:
	__forceinline static int cmp(T x,T y) {return cmp3(x,y);}
};

class ValCmp
{
public:
	__forceinline static int cmp(const Value& v,PropertyID pid) {return cmp3(v.property,pid);}
};

/**
 * binary array search template
 */
template<typename T,typename Key=T,class C=DefCmp<Key>,typename N=unsigned> class BIN
{
public:
	__forceinline static const T *find(Key key,const T *arr,N nElts,T** ins=NULL) {
		N k=0; const T *r=NULL;
		if (arr!=NULL) while (nElts!=0) {
			k=nElts>>1; const T *q=&arr[k]; int c=C::cmp(*q,key);
			if (c==0) {r=q; break;} if (c>0) nElts=k; else {nElts-=++k; arr+=k; k=0;}
		}
		if (ins!=NULL) *ins=(T*)&arr[k]; return r;
	}
	__forceinline static const T *find(Key key,const T **arr,N nElts,const T*** ins=NULL) {
		N k=0; const T *r=NULL;
		if (arr!=NULL) while (nElts!=0) {
			k=nElts>>1; const T *q=arr[k]; int c=C::cmp(q,key); 
			if (c==0) {r=q; break;} if (c>0) nElts=k; else {nElts-=++k; arr+=k; k=0;}
		}
		if (ins!=NULL) *ins=(const T**)&arr[k]; return r;
	}
	__forceinline static RC insert(T *&arr,N& n,Key key,T newElt,IMemAlloc *ma,N *xn=NULL,T **pins=NULL) {
		T *ins=NULL; if (find(key,(const T*)arr,n,&ins)!=NULL) return RC_OK;
		if ((arr==NULL || xn==NULL || n>=*xn) && ma!=NULL) {
			ptrdiff_t sht=ins-arr; size_t old=(xn==NULL?n:*xn)*sizeof(T);
			if ((arr=(T*)ma->realloc(arr,(xn==NULL?n+1:*xn+=(*xn==0?10:*xn/2))*sizeof(T),old))==NULL) return RC_NORESOURCES;
			ins=arr+sht;
		}
		if (ins<&arr[n]) memmove(ins+1,ins,(uint8_t*)&arr[n]-(uint8_t*)ins); *ins=newElt; n++; 
		if (pins!=NULL) *pins=ins; return RC_OK;
	}
};

/**
 * quick sort template
 */
#define QS_STKSIZ	(8*sizeof(void*)-2)
#define QS_CUTOFF	8

template<typename T,typename N=unsigned> class QSort
{
public:
	__forceinline static void sort(T& t,N nElts) {
		struct {N lwr,upr;} bndstk[QS_STKSIZ]; int stkptr=0;
		for (N lwr=0,upr=nElts-1;;) {
			if (upr-lwr+1<=QS_CUTOFF) while (upr>lwr) {
				N max=lwr;
				for (N p=lwr+1; p<=upr; ++p) if (t.cmp(p,max)>0) max=p;
			    t.swap(max,upr); --upr;
			} else {
				N mdl=(upr+lwr)/2;
				if (t.cmp(lwr,mdl)>0) t.swap(lwr,mdl);
				if (t.cmp(lwr,upr)>0) t.swap(lwr,upr);
				if (t.cmp(mdl,upr)>0) t.swap(mdl,upr);
				N lwr2=lwr,upr2=upr;
				for (;;) {
					if (mdl>lwr2) do ++lwr2; while (lwr2<mdl && t.cmp(lwr2,mdl)<=0);
					if (mdl<=lwr2) do ++lwr2; while (lwr2<=upr && t.cmp(lwr2,mdl)<=0);	// ????
					do --upr2; while (upr2>mdl && t.cmp(upr2,mdl)>0);
					if (upr2<lwr2) break;
					t.swap(lwr2,upr2); if (mdl==upr2) mdl=lwr2;
				}
				++upr2;
				if (mdl<upr2) do --upr2; while (upr2>mdl && t.cmp(upr2,mdl)==0);
				if (mdl>=upr2) do --upr2; while (upr2>lwr && t.cmp(upr2,mdl)==0);
				if (upr2-lwr>=upr-lwr2) {
					if (lwr<upr2) {bndstk[stkptr].lwr=lwr; bndstk[stkptr].upr=upr2; ++stkptr;}
					if (lwr2<upr) {lwr=lwr2; continue;}
				} else {
					if (lwr2<upr) {bndstk[stkptr].lwr=lwr2; bndstk[stkptr].upr=upr; ++stkptr;}
					if (lwr<upr2) {upr=upr2; continue;}
				}
			}
			if (--stkptr<0) return; lwr=bndstk[stkptr].lwr; upr=bndstk[stkptr].upr;
		}
	}
};

template<typename T,class C=DefCmp<T>,typename N=unsigned> class QSortA
{
	T	*const	arr;
	const	N	nElts;
public:
	QSortA(T *t,N n) : arr(t),nElts(n) {}
	void sort() {QSort<QSortA,N>::sort(*this,nElts);}
	int cmp(N i,N j) const {return C::cmp(arr[i],arr[j]);}
	void swap(N i,N j) {T tmp=arr[i]; arr[i]=arr[j]; arr[j]=tmp;}
};

/**
 * dynamic unordered array template
 */
template<typename T,int initSize=10,unsigned factor=1> class DynArray
{
protected:
	IMemAlloc	*const ma;
	T			tbuf[initSize];
	T			*ts;
	unsigned	nTs;
	unsigned	xTs;
public:
	DynArray(IMemAlloc *m) : ma(m),ts(tbuf),nTs(0),xTs(initSize) {}
	~DynArray() {if (ts!=tbuf) ma->free(ts);}
	RC operator+=(T t) {return nTs>=xTs && !expand() ? RC_NORESOURCES : (ts[nTs++]=t,RC_OK);}
	RC operator-=(unsigned idx) {if (idx>=nTs) return RC_INVPARAM; if (idx+1<nTs) memmove(ts+idx,ts+idx+1,(nTs-idx-1)*sizeof(T)); --nTs; return RC_OK;}
	void operator=(DynArray<T,initSize,factor>& rhs) {clear(); if ((nTs=rhs.nTs)!=0) {if (rhs.ts==rhs.tbuf) memcpy(ts=tbuf,rhs.tbuf,nTs*sizeof(T)); else {ts=rhs.ts; rhs.ts=rhs.tbuf; rhs.nTs=0;}} xTs=rhs.xTs;}
	void sort() {if (nTs>1) qsort(ts,nTs,sizeof(T),T::compare);}
	T* pop() {return nTs!=0?&ts[--nTs]:NULL;}
	T& add() {return nTs>=xTs && !expand() ? *(T*)0 : ts[nTs++];}
	T* get(uint32_t& n) {T *pt=NULL; if ((n=nTs)!=0) {if ((pt=ts)!=tbuf) ts=tbuf; else if ((pt=new(ma) T[nTs])!=NULL) memcpy(pt,ts,nTs*sizeof(T)); nTs=0;} return pt;}
	operator const T* () const {return ts;}
	operator unsigned () const {return nTs;}
	void clear() {if (ts!=tbuf) {ma->free(ts); ts=tbuf;} nTs=0; xTs=initSize;}
	RC trunc(unsigned n) {if (n>nTs) return RC_INVPARAM; nTs=n; return RC_OK;}
	RC set(unsigned idx,T val) {while (idx>=xTs) if (!expand()) return RC_NORESOURCES; if (nTs<=idx) nTs=idx+1; ts[idx]=val; return RC_OK;}
	IMemAlloc *getmem() const {return ma;}
private:
	bool expand() {
		if (ts!=tbuf) {size_t old=xTs*sizeof(T); ts=(T*)ma->realloc(ts,(xTs+=xTs/factor)*sizeof(T),old);}
		else if ((ts=(T*)ma->malloc((xTs+=xTs/factor)*sizeof(T)))!=NULL) memcpy(ts,tbuf,sizeof(tbuf));
		return ts!=NULL;
	}
};

/**
 * dynamic ordered array template; supports binary search
 */
template<typename T,typename Key=T,class C=DefCmp<Key>,unsigned initX=16,unsigned factor=1> class DynOArray
{
protected:
	IMemAlloc	*const ma;
	T			*ts;
	unsigned	nTs;
	unsigned	xTs;
public:
	DynOArray(IMemAlloc *m) : ma(m),ts(NULL),nTs(0),xTs(0) {}
	~DynOArray() {if (ts!=NULL) ma->free(ts);}
	RC operator+=(T t) {return add(t);}
	RC operator-=(T t) {
		T *del=NULL; if (BIN<T,Key,C>::find((Key)t,(const T*)ts,nTs,&del)==NULL) return RC_FALSE;
		assert(del!=NULL); if (del<&ts[--nTs]) memmove(del,del+1,(uint8_t*)&ts[nTs]-(uint8_t*)del); return RC_OK;
	}
	RC add(T t,T **ret=NULL) {
		T *p,*ins=NULL;
		if ((p=(T*)BIN<T,Key,C>::find((Key)t,(const T*)ts,nTs,&ins))!=NULL) {if (ret!=NULL) *ret=p; return RC_FALSE;}
		if (nTs>=xTs) {ptrdiff_t sht=ins-ts; size_t old=xTs*sizeof(T); if ((ts=(T*)ma->realloc(ts,(xTs+=xTs==0?initX:xTs/factor)*sizeof(T),old))==NULL) return RC_NORESOURCES; ins=ts+sht;}
		if (ins<&ts[nTs]) memmove(ins+1,ins,(uint8_t*)&ts[nTs]-(uint8_t*)ins); *ins=t; nTs++; if (ret!=NULL) *ret=ins; return RC_OK;
	}
	RC remove(unsigned i) {if (i>=nTs) return RC_FALSE; if (i<--nTs) memmove(&ts[i],&ts[i+1],(nTs-i)*sizeof(T)); return RC_OK;}
	void moveTo(DynOArray<T,Key,C,initX,factor>& to) {if (to.ts!=NULL) to.ma->free(to.ts); to.ts=ts; to.nTs=nTs; to.xTs=xTs; ts=NULL; nTs=xTs=0;}
	const T* find(Key key) const {return BIN<T,Key,C>::find(key,(const T*)ts,nTs);}
	T* get(uint32_t& n) {T *pt=NULL; if ((n=nTs)!=0) {pt=ts; ts=NULL; xTs=nTs=0;} return pt;}
	operator const T* () const {return ts;}
	operator unsigned () const {return nTs;}
	void clear() {if (ts!=NULL) {ma->free(ts); ts=NULL;} nTs=xTs=0;}
};

/**
 * dynamic ordered array template; supports binary search
 * contains pre-allocated buffer
 */
template<typename T,typename Key=T,class C=DefCmp<Key>,int initSize=16,unsigned factor=1> class DynOArrayBuf
{
protected:
	IMemAlloc	*const ma;
	T			tbuf[initSize];
	T			*ts;
	unsigned	nTs;
	unsigned	xTs;
public:
	DynOArrayBuf(IMemAlloc *m) : ma(m),ts(tbuf),nTs(0),xTs(initSize) {}
	~DynOArrayBuf() {if (ts!=tbuf) ma->free(ts);}
	RC operator+=(T t) {return add(t);}
	RC operator-=(T t) {
		T *del=NULL; if (BIN<T,Key,C>::find((Key)t,(const T*)ts,nTs,&del)==NULL) return RC_FALSE;
		assert(del!=NULL); if (del<&ts[--nTs]) memmove(del,del+1,(uint8_t*)&ts[nTs]-(uint8_t*)del); return RC_OK;
	}
	RC add(T t,T **ret=NULL) {
		T *p,*ins=NULL;
		if ((p=(T*)BIN<T,Key,C>::find((Key)t,(const T*)ts,nTs,&ins))!=NULL) {if (ret!=NULL) *ret=p; return RC_FALSE;}
		if (nTs>=xTs) {ptrdiff_t sht=ins-ts; if (!expand()) return RC_NORESOURCES; ins=ts+sht;}
		if (ins<&ts[nTs]) memmove(ins+1,ins,(uint8_t*)&ts[nTs]-(uint8_t*)ins); *ins=t; nTs++; if (ret!=NULL) *ret=ins; return RC_OK;
	}
	RC remove(unsigned i) {if (i>=nTs) return RC_FALSE; if (i<--nTs) memmove(&ts[i],&ts[i+1],(nTs-i)*sizeof(T)); return RC_OK;}
	void moveTo(DynOArrayBuf<T,Key,C,initSize,factor>& to) {if (to.ts!=to.tbuf) to.ma->free(to.ts); if (ts==tbuf) memcpy(to.ts=to.tbuf,tbuf,sizeof(tbuf)); else to.ts=ts; to.nTs=nTs; to.xTs=xTs; ts=tbuf; nTs=0; xTs=initSize;}
	const T* find(Key key) const {return BIN<T,Key,C>::find(key,(const T*)ts,nTs);}
	T* get(uint32_t& n) {T *pt=NULL; if ((n=nTs)!=0) {if ((pt=ts)!=tbuf) ts=tbuf; else if ((pt=new(ma) T[nTs])!=NULL) memcpy(pt,ts,nTs*sizeof(T));} return pt;}
	operator const T* () const {return ts;}
	operator unsigned () const {return nTs;}
	void clear() {if (ts!=tbuf) {ma->free(ts); ts=tbuf;} nTs=0; xTs=initSize;}
private:
	bool expand() {
		if (ts!=tbuf) {size_t old=xTs*sizeof(T); ts=(T*)ma->realloc(ts,(xTs+=xTs/factor)*sizeof(T),old);}
		else if ((ts=(T*)ma->malloc((xTs+=xTs/factor)*sizeof(T)))!=NULL) memcpy(ts,tbuf,sizeof(tbuf));
		return ts!=NULL;
	}
};

/**
 * volatile dynamic ordered array template; supports binary search
 * contains pre-allocated buffer; for use in multithreaded environment
 */
template<typename T,typename Key=T,class C=DefCmp<Key>,int initSize=16,unsigned factor=1> class DynOArrayBufV
{
protected:
	IMemAlloc	*const ma;
	T			tbuf[initSize];
	T*			volatile	ts;
	volatile	unsigned	nTs;
	volatile	unsigned	xTs;
public:
	DynOArrayBufV(IMemAlloc *m) : ma(m),ts(tbuf),nTs(0),xTs(initSize) {}
	~DynOArrayBufV() {if (ts!=tbuf) ma->free(ts);}
	RC add(T t,unsigned *idx=NULL) {
		T *p,*ins=NULL;
		if ((p=(T*)BIN<T,Key,C>::find((Key)t,(const T*)ts,nTs,&ins))!=NULL) {if (idx!=NULL) *idx=unsigned(p-(T*)ts); return RC_FALSE;}
		if (nTs>=xTs) {ptrdiff_t sht=ins-(T*)ts; if (!expand()) return RC_NORESOURCES; ins=(T*)ts+sht;}
		if (ins<(T*)&ts[nTs]) memmove(ins+1,ins,(uint8_t*)&ts[nTs]-(uint8_t*)ins); *ins=t; nTs++; if (idx!=NULL) *idx=unsigned(ins-(T*)ts); return RC_OK;
	}
	RC remove(T t,unsigned *idx=NULL) {
		T *del=NULL; if (BIN<T,Key,C>::find((Key)t,(const T*)ts,nTs,&del)==NULL) return RC_FALSE; if (idx!=NULL) *idx=unsigned(del-(T*)ts);
		assert(del!=NULL); if (del<(T*)&ts[--nTs]) memmove(del,del+1,(uint8_t*)&ts[nTs]-(uint8_t*)del); return RC_OK;
	}
	RC remove(unsigned i) {if (i>=nTs) return RC_FALSE; if (i<--nTs) memmove((T*)&ts[i],(T*)&ts[i+1],(nTs-i)*sizeof(T)); return RC_OK;}
	const T* find(Key key) const {return BIN<T,Key,C>::find(key,(const T*)ts,nTs);}
	operator const T*() const {return (const T*)ts;}
	operator unsigned () const {return nTs;}
	void clear() {if (ts!=tbuf) {ma->free(ts); ts=tbuf;} nTs=0; xTs=initSize;}
private:
	bool expand() {
		if (ts!=tbuf) {size_t old=xTs*sizeof(T); ts=(T*)ma->realloc(ts,(xTs+=xTs/factor)*sizeof(T),old);}
		else if ((ts=(T*)ma->malloc((xTs+=xTs/factor)*sizeof(T)))!=NULL) memcpy(ts,tbuf,sizeof(tbuf));
		return ts!=NULL;
	}
};

/**
 * fast keyword parsing
 */
struct KWInit
{
	char		*kw;
	unsigned	val;
};

class AFY_EXP KWTrie
{
public:
	class	KWIt {
	public:
		virtual	RC		next(unsigned ch,unsigned& kw) = 0;
		virtual	void	destroy() = 0;
	};
	virtual	RC	createIt(ISession *ses,KWIt *&it) = 0;
	virtual	RC	addKeywords(const KWInit *initTab,unsigned nInit) = 0;
	virtual	RC	find(const char *str,size_t lstr,unsigned& code) = 0;
	static	RC	createTrie(const KWInit *initTab,unsigned nInit,KWTrie *&ret,bool fCaseSensitive=true);
};

/**
 * simple single-linked list with counter
 */
template<typename T,T *T::*pList> struct Queue
{
	T			*head;
	T			*tail;
	uint32_t	count;
	Queue() : head(NULL),tail(NULL),count(0) {}
	void		operator+=(T *t) {if (tail==NULL) head=t; else tail->*pList=t; tail=t; t->*pList=NULL; count++;}
	void		operator+=(Queue<T,pList>& q) {if (q.head!=NULL) {if (tail==NULL) head=q.head; else tail->*pList=q.head; tail=q.tail; count+=q.count;}}
	void		reset() {head=tail=NULL; count=0;}
};

/**
 * double-linked circular list
 * trades space for O(1) insertion and deletion operations
 */
struct DLList
{
	DLList	*prev;
	DLList	*next;
public:
	DLList() {prev=next=this;}
	void	insertFirst(DLList *elt) {elt->next=next; elt->prev=this; next=next->prev=elt;}
	void	insertLast(DLList *elt) {elt->next=this; elt->prev=prev; prev=prev->next=elt;}
	void	insertAfter(DLList *elt) {next=elt->next; prev=elt; elt->next=elt->next->prev=this;}
	void	insertBefore(DLList *elt) {next=elt; prev=elt->prev; elt->prev=elt->prev->next=this;}
	void	remove() {next->prev=prev; prev->next=next; prev=next=this;}
	bool	isInList() const {return next!=this;}
	void	reset() {prev=next=this;}
};

/**
 * hash-table chain element template
 */
template<class T> class HChain : public DLList
{
	T* const	obj;
	unsigned	idx;
public:
	HChain() : obj(NULL),idx(~0u) {}
	HChain(T *t) : obj(t),idx(~0u) {}
	void	insertFirst(HChain<T> *elt) {assert(obj==NULL&&elt->obj!=NULL); DLList::insertFirst(elt);}
	void	insertLast(HChain<T> *elt) {assert(obj==NULL&&elt->obj!=NULL); DLList::insertLast(elt);}
	void	insertAfter(HChain<T> *elt) {assert(obj!=NULL); DLList::insertAfter(elt);}
	void	insertBefore(HChain<T> *elt) {assert(obj!=NULL); DLList::insertBefore(elt);}
	T*		getFirst() const {return next==this ? NULL : ((HChain<T>*)next)->obj;}
	T*		getLast() const {return prev==this ? NULL : ((HChain<T>*)prev)->obj;}
	T*		getNext() const {return ((HChain<T>*)next)->obj;}
	T*		getPrev() const {return ((HChain<T>*)prev)->obj;}
	T*		removeFirst() {HChain<T> *dl=(HChain<T>*)next; if (dl==this) return NULL; dl->remove(); return dl->obj;}
	T*		removeLast() {HChain<T> *dl=(HChain<T>*)prev; if (dl==this) return NULL; dl->remove(); return dl->obj;}
	unsigned	getIndex() const {return idx;}
	void	setIndex(unsigned i) {idx=i;}
	class it {
		HChain<T> *cur,*th; 
	public:
		it(HChain<T> *t) : cur(t),th(t) {}
		void reset(HChain<T> *t) {cur=th=t;}
		bool operator++() {return (cur=(HChain<T>*)cur->next)!=th;}
		bool operator--() {return (cur=(HChain<T>*)cur->prev)!=th;}
		operator HChain<T>*() const {return cur;}
		T* get() const {return cur->obj;}
	};
	class it_r {
		HChain<T> *cur,*th,*nxt; 
	public:
		it_r(HChain<T> *t) : cur(t),th(t),nxt((HChain<T>*)t->next) {}
		void reset(HChain<T> *t) {cur=th=t; nxt=t->next;}
		bool operator++() {return nxt==th?false:(nxt=(HChain<T>*)(cur=nxt)->next,true);}
		operator HChain<T>*() const {return cur;}
		T* get() const {return cur->obj;}
	};
};

/**
 * simple hash table template
 */
template<class T,typename Key,HChain<T> T::*pList> class HashTab
{
	struct HashTabElt {
		HChain<T>		list;
		unsigned		counter;
		HashTabElt()	{counter = 0;}
	};
	const	unsigned	hashSize;
	const	unsigned	keyMask;
	const	unsigned	keyShift;
	IMemAlloc *const	ma;
	const	bool		fClear;
	HashTabElt			*hashTab;
	unsigned			highwatermark;
public:
	HashTab(unsigned size,IMemAlloc *allc,bool fClr=true) : hashSize(nextP2(size)),keyMask(hashSize-1),keyShift(32-pop(keyMask)),ma(allc),fClear(fClr),highwatermark(0)
														{hashTab=new(allc) HashTabElt[hashSize];}
	~HashTab()	{if (fClear) {clear(); ma->free(hashTab);}}
	unsigned		index(unsigned hash) const {return hash*2654435769ul>>keyShift&keyMask;}
	HChain<T>	*start(unsigned idx) const {assert(idx<hashSize); return &hashTab[idx].list;}
	void		insert(T *t) {insert(t, index(uint32_t(t->getKey())));}
	void		insert(T *t, unsigned idx) {
		assert(idx<hashSize); HashTabElt *ht=&hashTab[idx];
		ht->list.insertFirst(&(t->*pList)); (t->*pList).setIndex(idx);
		if (++ht->counter>highwatermark) highwatermark=ht->counter;
	}
	void	remove(T *t,bool fDelete=false) {
		unsigned idx=(t->*pList).getIndex(); (t->*pList).setIndex(~0u);
		if (idx<hashSize) --hashTab[idx].counter; 
		(t->*pList).remove(); if (fDelete) delete t;
	}
	void	remove(Key key,bool fDelete=false) {
		for (typename HChain<T>::it it(start(index(uint32_t(key)))); ++it;)
			{T *p = it.get(); assert(p!=NULL); if (p->getKey()==key) {remove(p,fDelete); break;}}
	}
	unsigned	getHighwatermark() const {return highwatermark;}
	T*		find(Key key) const {
		for (typename HChain<T>::it it(start(index(uint32_t(key)))); ++it;)
			{T *p = it.get(); assert(p!=NULL); if (p->getKey()==key) return p;}
		return NULL;
	}
	T*		find(Key key,unsigned idx) const {
		for (typename HChain<T>::it it(start(idx)); ++it;)
			{T *p = it.get(); assert(p!=NULL); if (p->getKey()==key) return p;}
		return NULL;
	}
	void clear(bool fDealloc=true) {
		T *t; highwatermark=0;
		for (unsigned i=0; i<hashSize; i++) {
			if (fDealloc) while ((t = hashTab[i].list.removeFirst())!=NULL) delete t; else hashTab[i].list.reset();
			hashTab[i].counter=0;
		}
	}
	friend class it;
	/**
	 * hash table iterator
	 */
	class it : public HChain<T>::it {
		const HashTab&		hashTab;
		unsigned				idx;
	public:
		it(const HashTab& ht) : HChain<T>::it(ht.start(0)),hashTab(ht),idx(0) {}
		bool operator++() {
			for (;;) {
				if (HChain<T>::it::operator++()) return true;
				if (++idx>=hashTab.hashSize) return false;
				HChain<T>::it::reset(hashTab.start(idx));
			}
		}
	};
	/**
	 * hash table search context
	 */
	class Find {
		const HashTab&	hashTab;
		const Key		key;
		const unsigned		idx;
	public:
		Find(const HashTab& ht,Key k) : hashTab(ht),key(k),idx(ht.index(uint32_t(k))) {}
		T* find() const {
			for (typename HChain<T>::it it(hashTab.start(idx)); ++it;)
				{T *p = it.get(); assert(p!=NULL); if (p->getKey()==key) return p;}
			return NULL;
		}
		unsigned getIdx() const {return idx;}
	};
};

/**
 * 'synchronised' hash table template (supports concurrent access)
 */
template<class T,typename Key,HChain<T> T::*pList,typename FKey=Key,typename FKeyArg=Key> class SyncHashTab
{
	struct HashTabElt {
		HChain<T>	list;
		RWSpin		lock;
		long		counter;
		HashTabElt() : counter(0) {}
	};
	IMemAlloc *const ma;
	HashTabElt		*hashTab;
	unsigned		hashSize;
	int				keyShift;
	unsigned		keyMask;
	long			highwatermark;
public:
	SyncHashTab(unsigned size,IMemAlloc *m) : ma(m) {hashSize=nextP2(size); hashTab=new(ma) HashTabElt[hashSize]; keyMask=hashSize-1; keyShift=32-pop(keyMask); highwatermark=0;}
	~SyncHashTab() {ma->free(hashTab);}
	unsigned index(unsigned hash) const {return hash*2654435769ul>>keyShift&keyMask;}
	void insert(T *t) {insert(t, index(uint32_t(t->getKey())));}
	void insert(T *t, unsigned idx) {
		assert(idx<hashSize); HashTabElt *ht=&hashTab[idx]; (t->*pList).setIndex(idx);
		ht->lock.lock(RW_X_LOCK); ht->list.insertFirst(&(t->*pList)); 
		if (++ht->counter>highwatermark) highwatermark=ht->counter;
		ht->lock.unlock();
	}
	void insertNoLock(T *t) {insertNoLock(t,index(uint32_t(t->getKey())));}
	void insertNoLock(T *t,unsigned idx) {
		assert(idx<hashSize); HashTabElt *ht=&hashTab[idx];
		(t->*pList).setIndex(idx); ht->list.insertFirst(&(t->*pList));
		if (++ht->counter>highwatermark) highwatermark=ht->counter;
	}
	void remove(T *t,bool fDelete=false) {
		if ((t->*pList).isInList()) {
			unsigned idx=(t->*pList).getIndex();
			if (idx<hashSize) {
				HashTabElt *ht=&hashTab[idx]; ht->lock.lock(RW_X_LOCK); 
				(t->*pList).setIndex(~0u); (t->*pList).remove(); 
				--ht->counter; ht->lock.unlock();
			}
		}
		if (fDelete) delete t;
	}
	void removeNoLock(T *t) {
		unsigned idx=(t->*pList).getIndex();
		assert((t->*pList).isInList() && idx<hashSize);
		HashTabElt *ht=&hashTab[idx];
		(t->*pList).setIndex(~0u); (t->*pList).remove(); 
		--ht->counter; ht->lock.unlock();
	}
	void remove(Key key,bool fDelete=false) {
		HashTabElt *ht=&hashTab[index(uint32_t(key))]; ht->lock.lock(RW_U_LOCK);
		for (typename HChain<T>::it it(&ht->list); ++it;) {
			T *p=it.get(); assert(p!=NULL); 
			if (p->getKey()==key) {
				ht->lock.upgradelock(RW_X_LOCK); (p->*pList).remove(); 
				--ht->counter; ht->lock.unlock(); if (fDelete) delete p; 
				return;
			}
		}
		ht->lock.unlock(true);
	}
	unsigned	getHighwatermark() const {return highwatermark;}
	T* find(Key key) const {return find(key,index(uint32_t(key)));}
	T* find(Key key,unsigned idx) const {
		HashTabElt *ht=&hashTab[idx]; ht->lock.lock(RW_S_LOCK);
		typename HChain<T>::it it(&ht->list); T *ret=NULL;
		while (++it) {T *p=it.get(); assert(p!=NULL); if (p->getKey()==key) {ret=p; break;}}
		ht->lock.unlock(); return ret;
	}
	T* findLock(Key key) const {return findLock(key,index(uint32_t(key)));}
	T* findLock(Key key,unsigned idx) const {
		HashTabElt *ht=&hashTab[idx]; ht->lock.lock(RW_S_LOCK);
		typename HChain<T>::it it(&ht->list);
		while (++it) {T *p=it.get(); assert(p!=NULL); if (p->getKey()==key) return p;}
		return NULL;
	}
	void lock(T* t,RW_LockType lt) {assert((t->*pList).getIndex()<hashSize); hashTab[(t->*pList).getIndex()].lock.lock(lt);}
	void unlock(Key key) {hashTab[index(uint32_t(key))].lock.unlock();}
	void unlock(T* t) {assert((t->*pList).getIndex()<hashSize); hashTab[(t->*pList).getIndex()].lock.unlock();}
	void clear(bool fDealloc=true) {
		for (unsigned i=0; i<hashSize; i++) {
			HashTabElt *ht=&hashTab[i]; ht->lock.lock(RW_X_LOCK); T *t;
			if (fDealloc) while ((t=ht->list.removeFirst())!=NULL) delete t; else ht->list.reset();
			hashTab[i].counter=0; ht->lock.unlock();
		}
		highwatermark=0;
	}
	friend class it;
	HChain<T>	*start(unsigned idx) const {assert(idx<hashSize); return &hashTab[idx].list;}
	/**
	 * synchronised hash table iterator
	 */
	class it : public HChain<T>::it {
		const SyncHashTab&	hashTab;
		unsigned				idx;
	public:
		it(const SyncHashTab& ht) : HChain<T>::it(ht.start(0)),hashTab(ht),idx(0) {}
		bool operator++() {
			for (;;) {
				if (HChain<T>::it::operator++()) return true;
				if (++idx>=hashTab.hashSize) return false;
				HChain<T>::it::reset(hashTab.start(idx));
			}
		}
	};
	/**
	 * synchronized hash table search context
	 */
	class Find {
		SyncHashTab&		hashTab;
		const FKey			key;
		const unsigned			idx;
		bool				fLocked;
	public:
		Find(SyncHashTab& ht,FKeyArg k) : hashTab(ht),key(k),idx(ht.index(uint32_t(key))),fLocked(false) {}
		~Find() {if (fLocked) unlock();}
		T* find() {
			assert(idx<hashTab.hashSize);
			HashTabElt *ht=&hashTab.hashTab[idx]; ht->lock.lock(RW_S_LOCK); fLocked=true;
			typename HChain<T>::it it(&ht->list); T *ret=NULL;
			while (++it) {T *p=it.get(); assert(p!=NULL); if (p->getKey()==key) {ret=p; break;}}
			ht->lock.unlock(); fLocked=false; return ret;
		}
		T* findLock(RW_LockType lt) {
			assert(idx<hashTab.hashSize);
			HashTabElt *ht=&hashTab.hashTab[idx]; ht->lock.lock(lt); fLocked=true; typename HChain<T>::it it(&ht->list);
			while (++it) {T *p=it.get(); assert(p!=NULL); if (p->getKey()==key) return p;}
			return NULL;
		}
		void remove(T *t) {if (!fLocked) hashTab.lock(t,RW_X_LOCK); hashTab.removeNoLock(t); fLocked=false;}
		void unlock() {assert(idx<hashTab.hashSize); hashTab.hashTab[idx].lock.unlock(); fLocked=false;}
		unsigned getIdx() const {return idx;}
	};
};

}

#endif
