/**************************************************************************************

Copyright © 2004-2013 GoPivotal, Inc. All rights reserved.

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

#include "pinref.h"
#include "pgtree.h"
#include "buffer.h"
#include "session.h"

using namespace Afy;
using namespace AfyKernel;

namespace AfyKernel
{

#define	SC_INIT		0x80000000
#define	SC_EOF		0x40000000
#define	SC_BOF		0x20000000
#define	SC_UNIQUE	0x10000000
#define	SC_SAVEPS	0x08000000
#define	SC_KEYSET	0x04000000
#define	SC_LASTKEY	0x02000000
#define	SC_FIRSTSP	0x01000000
#define	SC_LASTSP	0x00800000
#define	SC_FIXED	0x00400000
#define	SC_ADV		0x00200000
#define	SC_PINREF	0x00100000

#define	LBUF_EXTRA	24

class TreeScanImpl : public TreeScan, public TreeCtx, public LatchHolder
{
	Session			*const	ses;
	StoreCtx		*const	ctx;
	unsigned					state;
	const SearchKey	*const	start;
	const SearchKey	*const	finish;
	const IndexSeg	*const	segs;
	const unsigned			nSegs;
	IKeyCallback	*const	keycb;
	bool					fHyper;

	PageID					kPage;
	PageID					nPage;
	LSN						lsn;
	unsigned					stamp;
	SearchKey				*savedKey;
	size_t					lKeyBuf;

	PBlock					*subpg;
	PageID					sPage;
	PageID					anchor;
	unsigned					stamp2;

	const byte				*ps,*pe,*ptr;
	size_t					lElt;
	byte					*buf;
	size_t					lbuf;
	size_t					ldata;
	size_t					lpref;
	size_t					bufsht;

private:
	bool checkBounds(const TreePageMgr::TreePage* tp,bool fNext,bool fSibling=false) {
		const SearchKey *bound=fNext?finish:start; return bound!=NULL&&bound->isSet()?tp->testBound(*bound,ushort(fSibling?~0u:index),segs,nSegs,!fNext,(state&SCAN_PREFIX)!=0):true;
	}
	void saveKey() {
		assert((state&SCAN_EXACT)==0);
		const TreePageMgr::TreePage *tp=!pb.isNull()?(const TreePageMgr::TreePage*)pb->getPageBuf():(const TreePageMgr::TreePage*)0;
		size_t lk=tp!=NULL?tp->getKeySize(ushort(index)):0;
		if (savedKey==NULL) savedKey=(SearchKey*)ses->malloc(lKeyBuf=sizeof(SearchKey)+lk);
		else if (lKeyBuf<sizeof(SearchKey)+lk) savedKey=(SearchKey*)ses->realloc(savedKey,lKeyBuf=sizeof(SearchKey)+lk);
		if (savedKey!=NULL) if (tp!=NULL) tp->getKey(ushort(index),*savedKey); else new(savedKey) SearchKey;
	}
	void findSubPage(const byte *k,size_t lk,bool fF=true) {
		if (subpg!=NULL) {subpg->release(0,ses); subpg=NULL;}
		if ((state&SC_KEYSET)==0 || pb.isNull() && !restore()) return;
		RC rc=RC_OK; PBlockP spb(pb); pb.moveTo(parent); stack[leaf=depth++]=spb->getPageID();
		TreePageMgr::SubTreePage tpe; memcpy(&tpe,spb->getPageBuf()+vp.offset,sizeof(tpe)); lvl2=tpe.level;
		if ((state&SCAN_EXACT)!=0) mainKey=start; else {if (!savedKey->isSet()) saveKey(); mainKey=savedKey;}
		if (k!=NULL && lk!=0) {SearchKey key(k,(ushort)lk,(state&SC_PINREF)!=0); if ((rc=findPage(&key))==RC_OK) {subpg=pb; pb=NULL;}}
		else {assert(!fF); if ((rc=findPrevPage(NULL))==RC_OK) {subpg=pb; pb=NULL;} parent.release(ses);}
		assert(parent.isNull()); spb.moveTo(pb); depth=leaf; leaf=~0u; mainKey=NULL;
		while (subpg!=NULL) {
			const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)subpg->getPageBuf();
			if (tp->info.nSearchKeys>0) break;
			if (fF) {
				if (!tp->hasSibling()) {subpg->release(0,ses); subpg=NULL;}
				else subpg=ctx->bufMgr->getPage(tp->info.sibling,ctx->trpgMgr,PGCTL_COUPLE|QMGR_SCAN,subpg,ses);
			} else {
				// getPreviousPage
				subpg->release(0,ses); subpg=NULL;	//tmp
			}
		}
	}
	void getPrevSubPage() {
		if (subpg==NULL) {
			//??? restore subpg
		}
		assert(0&&"nextValue 8");
		if (subpg!=NULL) {subpg->release(0,ses); subpg=NULL;}
	}
	bool findValue(const byte *buf,const byte *ebuf,const byte *v,size_t lv,const byte *&res) {
		assert(ebuf>buf && (ebuf-buf)%lElt==0);
		for (unsigned n=(unsigned)((ebuf-buf)/lElt); n>0; ) {
			unsigned k=n>>1; const byte *p=buf+k*lElt,*q; size_t l; int cmp;
			if ((state&SC_FIXED)!=0) q=p,l=lElt; else if (lElt==1) q=ps+p[0],l=p[1]-p[0]; else q=ps+_u16(p),l=_u16(p+2)-_u16(p);
			if ((state&SC_PINREF)!=0) cmp=PINRef::cmpPIDs(q,(unsigned)l,v,(unsigned)lv); else if ((cmp=memcmp(q,v,min(l,lv)))==0) cmp=cmp3(l,lv);
			if (cmp==0) {res=p; return true;} if (cmp<0) {buf=p+lElt; n-=k+1;} else n=k;
		}
		res=buf; return false;
	}
	bool restore() {
		PageID pid=kPage; kPage=nPage=INVALID_PAGEID; state&=~SC_LASTKEY;
		if (pid!=INVALID_PAGEID && (pb=tree->getPage(pid,stamp,PITREE_LEAF))!=NULL) {
			const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
			if (tp->hdr.lsn==lsn || savedKey->isSet() && tp->findKey(*savedKey,index)) return true;
			pb.release(ses);
		}
		const SearchKey *key=(state&SCAN_EXACT)!=0?start:(const SearchKey*)savedKey;
		if (!key->isSet() || findPage(key)!=RC_OK) {index=0; state&=~SC_KEYSET; return false;}
		return ((const TreePageMgr::TreePage*)pb->getPageBuf())->findKey(*key,index);
	}
public:
	TreeScanImpl(Session *se,Tree& tr,const SearchKey *st,const SearchKey *fi,unsigned flgs,const IndexSeg *sg,unsigned nS,IKeyCallback *kcb)
		: TreeCtx(tr),LatchHolder(se),ses(se),ctx(ses->getStore()),state(flgs|SC_INIT),start(st),finish(fi),segs(sg),nSegs(nS),keycb(kcb),fHyper(false),
		kPage(INVALID_PAGEID),lsn(0),stamp(0),savedKey(NULL),lKeyBuf(0),subpg(NULL),sPage(INVALID_PAGEID),stamp2(0),ps(NULL),pe(NULL),ptr(NULL),
		lElt(0),buf(NULL),lbuf(0),ldata(0),lpref(0),bufsht(0)
	{
			IndexFormat ifmt=tr.indexFormat(); if (ifmt.isUnique()) state|=SC_UNIQUE;
			if (ifmt.isPinRef()) state|=SC_PINREF;
			else if (ifmt.isFixedLenData()||(state&SC_UNIQUE)==0&&!ifmt.isVarMultiData()) {state|=SC_FIXED; lElt=ifmt.dataLength();}
			assert(start==NULL || start->type==ifmt.keyType()); assert(start!=NULL || (state&SCAN_EXACT)==0);
			if ((state&SCAN_EXACT)==0 && sg!=NULL && nS>1 && (st!=NULL || fi!=NULL)) 
				fHyper=st==NULL || fi==NULL || st->type==KT_VAR && isHyperRect(st->getPtr2(),st->v.ptr.l,fi->getPtr2(),fi->v.ptr.l);
	}
	virtual ~TreeScanImpl() {
		pb.release(ses); parent.release(ses); if (subpg!=NULL) subpg->release(0,ses);
		if (savedKey!=NULL) ses->free(savedKey); if (buf!=NULL) ses->free(buf);
	}
	void operator delete(void *p) {if (p!=NULL) ((TreeScanImpl*)p)->ses->free(p);}
	RC nextKey(GO_DIR op=GO_NEXT,const SearchKey *skip=NULL) {
		bool fF=op==GO_NEXT||op==GO_FIRST,fAdv=false; int cmp; if ((state&SCAN_BACKWARDS)!=0) fF=!fF;
		if (skip!=NULL) {
			if (fF) {
				if (finish!=NULL && ((cmp=finish->cmp(*skip))<0 || cmp==0 && (state&SCAN_EXCLUDE_START)!=0)) return RC_EOF;
				if (start!=NULL && start->cmp(*skip)>=0) skip=NULL;
			} else {
				if (start!=NULL && ((cmp=start->cmp(*skip))>0 || cmp==0 && (state&SCAN_EXCLUDE_END)!=0)) return RC_EOF;
				if (finish!=NULL && finish->cmp(*skip)<=0) skip=NULL;
			}
		}
		for (;;) {
			if ((state&SC_INIT)==0) {
				ps=pe=ptr=NULL; lpref=ldata=bufsht=0; state&=~SC_SAVEPS; if (subpg!=NULL) {subpg->release(0,ses); subpg=NULL;}
				PageID npid=nPage; nPage=sPage=INVALID_PAGEID; bool fLast=(state&SC_LASTKEY)!=0; state&=~SC_LASTKEY;
				if (fF) {if ((state&(SC_BOF|SC_KEYSET))==SC_BOF && op==GO_NEXT) op=GO_FIRST;}
				else if ((state&(SC_EOF|SC_KEYSET))==SC_EOF && op==GO_PREVIOUS) op=GO_LAST;
				if (op==GO_NEXT || op==GO_PREVIOUS) {
					if ((state&(fF?SC_EOF|SC_KEYSET:SC_BOF|SC_KEYSET))!=SC_KEYSET) return RC_EOF;
					if (skip!=NULL) {
						if (!pb.isNull()) {
							const TreePageMgr::TreePage* tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
							cmp=tp->testKey(*skip,(ushort)index); assert(index<tp->info.nSearchKeys);
							if (fF && cmp>=0 || !fF && cmp<=0) skip=NULL;											// skip before(fF) or after(!fF) current key
							else if (tp->findKey(*skip,index) && index<(unsigned)tp->info.nSearchKeys) goto retkey;	// found skip
							else if (index>0 && index<(unsigned)tp->info.nEntries) skip=NULL;							// skip within page or before next page
							else if (index>=(unsigned)tp->info.nSearchKeys) index=tp->info.nSearchKeys-1;
						} else if (savedKey->isSet()) {
							cmp=savedKey->cmp(*skip); if (fF && cmp>=0 || !fF && cmp<=0) skip=NULL;
							// !!! else check first/last key on page: if inside -> restore page
						}
					}
					if (skip==NULL) {
						if (pb.isNull()) {
							if (fF && fLast) {
								if (npid==INVALID_PAGEID) {state=state&~SC_KEYSET|SC_EOF; return RC_EOF;}
								if ((pb=tree->getPage(npid,stamp,PITREE_LEAF))!=NULL) {
									const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
									if (tp->info.nSearchKeys>0) {
										index=0; savedKey->reset(); if (checkBounds(tp,true)) {state&=~SC_BOF; goto retkey;}
										state=state&~SC_KEYSET|SC_EOF; pb.release(ses); return RC_EOF;
									}
								}
							}
							if (pb.isNull() && !restore()) {
								savedKey->reset();
								if (pb.isNull()) {state=state&~SC_KEYSET|(fF?SC_EOF:SC_BOF); return RC_EOF;}
								const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
								if (fF && index<tp->info.nSearchKeys && checkBounds(tp,fF)) {state&=~SC_BOF; goto retkey;}
							}
						}
						const TreePageMgr::TreePage* tp=(const TreePageMgr::TreePage*)pb->getPageBuf(); savedKey->reset();
						if (fF) for (++index; index>=tp->info.nSearchKeys; index=0) {
							if (!tp->hasSibling()||!checkBounds(tp,true,true)) {pb.release(ses); state=state&~SC_KEYSET|SC_EOF; return RC_EOF;}
							if (pb.getPage(tp->info.sibling,ctx->trpgMgr,PGCTL_COUPLE|QMGR_SCAN,ses)==NULL) {state=state&~SC_KEYSET|SC_EOF; return RC_EOF;}
							tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
						} else for (; index--==0; index=tp->info.nSearchKeys) {
							if (getPreviousPage()!=RC_OK) {index=0; state=state&~SC_KEYSET|SC_BOF; return RC_EOF;}
							tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
						}
						if (checkBounds(tp,fF)) {state&=fF?~SC_BOF:~SC_EOF; goto retkey;}
						pb.release(ses); state=state&~SC_KEYSET|(fF?SC_EOF:SC_BOF); return RC_EOF;
					}
				} else if ((state&(SC_KEYSET|SC_BOF|SC_EOF))==(SC_BOF|SC_EOF)) return RC_EOF;
				else if (skip==NULL && (state&(op==((state&SCAN_BACKWARDS)!=0?GO_LAST:GO_FIRST)?SC_BOF:SC_EOF))!=0 && (state&SC_KEYSET)!=0) goto retkey;
				else if (!pb.isNull()) {
					const SearchKey *key=skip!=NULL?skip:fF?start:finish;
					if (key!=NULL) {
						const TreePageMgr::TreePage* tp=(const TreePageMgr::TreePage*)pb->getPageBuf(); savedKey->reset();
						if (tp->findKey(*key,index) && (key!=start || (state&SCAN_EXCLUDE_START)==0)
							&& (key!=finish || (state&SCAN_EXCLUDE_END)==0)) goto retkey;
						if (!fF) {if (index>0) {--index; if (checkBounds(tp,false)) goto retkey;}}
						else if (index+1<(unsigned)tp->info.nSearchKeys) {++index; if (checkBounds(tp,true)) goto retkey;}
					}
					pb.release(ses);
				}
			}	
			index=0; state=state&~(SC_INIT|SC_LASTKEY|SC_KEYSET)|(skip==NULL?SC_EOF|SC_BOF:fF?SC_EOF:SC_BOF); savedKey->reset();
			if (fF) {
				const SearchKey *key=skip!=NULL?skip:start; if (findPage(key)!=RC_OK) return RC_EOF;
				const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf(); //assert(key==NULL||tp->cmpKey(*key)==0);
				if (tp->info.nSearchKeys==0) fAdv=true;
				else if (key!=NULL) {
					if (!tp->findKey(*key,index)) {
						if (index>=tp->info.nSearchKeys) fAdv=true;
						else if (!checkBounds(tp,true)) {pb.release(ses); return RC_EOF;}
					} else if (skip==NULL && (state&SCAN_EXCLUDE_START)!=0 && ++index>=tp->info.nSearchKeys) fAdv=true;
				} else if (finish!=NULL && !checkBounds(tp,true)) {pb.release(ses); return RC_EOF;}
				if (fAdv) for (index=0;;) {
					if (!tp->hasSibling() || !checkBounds(tp,true,true)) {pb.release(ses); return RC_EOF;}
					if (pb.getPage(tp->info.sibling,ctx->trpgMgr,PGCTL_COUPLE|QMGR_SCAN,ses)==NULL) return RC_EOF;
					tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
					if (tp->info.nSearchKeys>0) {if (!checkBounds(tp,true)) {pb.release(ses); return RC_EOF;} else break;}
				}
				if (tp->hasSibling() && checkBounds(tp,true,true)) {
					//prefetch from parent
				}
				if ((state&SCAN_EXACT)==0) state&=~SC_EOF;
			} else {
				const SearchKey *key=skip!=NULL?skip:finish;
				if ((state&SCAN_PREFIX)!=0 && key==finish && finish->isSet()) {
					if (finish->type==KT_BIN) {
						size_t l=finish->v.ptr.l; SearchKey *pk;
						if ((pk=(SearchKey*)alloca(sizeof(SearchKey)+l))==NULL) return RC_NORESOURCES;
						byte *p=(byte*)(pk+1); memcpy(p,finish->getPtr2(),l); while (l>0 && p[l-1]++==0xFF) l--;
						if (l==0) key=NULL; else {key=pk; pk->type=KT_BIN; pk->loc=SearchKey::PLC_SPTR; pk->v.ptr.p=NULL; pk->v.ptr.l=(ushort)l;}
					} else if (finish->type==KT_VAR) {
						//????
					}
				}
				if ((key==NULL?findPrevPage(NULL):findPage(key))!=RC_OK) return RC_EOF;
				const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf(); assert(key==NULL||tp->cmpKey(*key)<=0);
				if (tp->info.nSearchKeys==0) {
					if (key==NULL || findPrevPage(key)!=RC_OK) {parent.release(ses); pb.release(ses); return RC_EOF;}
					tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
					if (tp->info.nSearchKeys==0) {parent.release(ses); pb.release(ses); return RC_EOF;}
				}
				if (key==NULL)
					index=tp->info.nSearchKeys-1;
				else if (!tp->findKey(*key,index)) {
					if (index==0) fAdv=true; else if (!(--index,checkBounds(tp,false))) {pb.release(ses); return RC_EOF;}
				} else if (skip==NULL && (state&(SCAN_PREFIX|SCAN_EXCLUDE_END))!=0) {
					if (index==0) fAdv=true; else index--;
				}
				if (fAdv) for (;;) {
					if (getPreviousPage()!=RC_OK) return RC_EOF;
					tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
					if (tp->info.nSearchKeys>0) {
						index=tp->info.nSearchKeys-1; if (checkBounds(tp,false)) break;
						pb.release(ses); return RC_EOF;
					}
				}
				if ((state&SCAN_EXACT)==0) state&=~SC_BOF;
			}
retkey:
			state|=SC_KEYSET;
			if (fHyper) {
				skip=NULL; saveKey(); if (savedKey==NULL) return RC_NORESOURCES;
				if (start!=NULL && !checkHyperRect(start->getPtr2(),start->v.ptr.l,savedKey->getPtr2(),savedKey->v.ptr.l,segs,nSegs,true) ||
					finish!=NULL && !checkHyperRect(savedKey->getPtr2(),savedKey->v.ptr.l,finish->getPtr2(),finish->v.ptr.l,segs,nSegs,false))
						{op=fF?GO_NEXT:GO_PREVIOUS; continue;}
				// exclude boundaries, skip to next
			}
			if (keycb!=NULL) keycb->newKey(); return RC_OK;
		}
	}
	const void *nextValue(size_t& lD,GO_DIR op,const byte *sk,size_t lsk) {
		for (bool fF=op==GO_NEXT||op==GO_FIRST;;sk=NULL) {
			const byte *q; size_t l; int cmp;
			if (ps==NULL) {
				if ((state&SC_KEYSET)==0 || (state&SC_ADV)!=0) {
					state&=~SC_ADV; if (nextKey(op)!=RC_OK) {lD=0; return NULL;}
				} else if (pb.isNull() && !restore() && (pb.isNull() || !checkBounds((const TreePageMgr::TreePage*)pb->getPageBuf(),fF))) {
					pb.release(ses); state=state&~SC_KEYSET|(fF?SC_EOF:SC_BOF); lD=0; return NULL;
				}
				ushort ld; lpref=0; const PagePtr *pvp; assert(!pb.isNull());
				const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
				if ((ps=(const byte*)tp->findData(index,ld,&pvp))==NULL) {state|=SC_ADV; continue;}
				state|=SC_SAVEPS|SC_FIRSTSP|SC_LASTSP; ldata=ld; pe=ps+ldata; memcpy(&vp,pvp,sizeof(PagePtr));
				if ((state&SC_UNIQUE)!=0) {lD=ld; ptr=pe; return ps;}
				if (!TreePageMgr::TreePage::isSubTree(vp)) {
					if ((state&SC_FIXED)==0) pe=ps+(vp.len<256?*ps-(lElt=1):_u16(ps)-(lElt=2));
					if (sk==NULL || lsk==0) ptr=fF?ps:pe;
					else if (findValue(ps,pe,sk,lsk,ptr)) {if (!fF) ptr+=lElt;}
					else if (fF && ptr>=pe || !fF && ptr<=ps) {ps=NULL; state|=SC_ADV;}
					continue;
				}
				TreePageMgr::SubTreePage ext; memcpy(&ext,(byte*)tp+vp.offset,sizeof(ext));
				ps=pe=ptr=NULL; anchor=ext.anchor; if ((state&SC_FIXED)==0) lElt=2;
				if (sk!=NULL && lsk>0) findSubPage(sk,lsk,fF);
				else if (fF) subpg=ctx->bufMgr->getPage(anchor,ctx->trpgMgr,QMGR_SCAN,NULL,ses);
				else findSubPage(NULL,0,false);
			} else switch (op) {
			default: assert(0);
			case GO_FIRST:
				if ((state&SC_BOF)==0) {ps=NULL; state|=SC_ADV; continue;}
				if ((state&SC_FIRSTSP)==0) {
					if (sk!=NULL&&lsk!=0) findSubPage(sk,lsk);
					else subpg=ctx->bufMgr->getPage(anchor,ctx->trpgMgr,PGCTL_COUPLE|QMGR_SCAN,subpg,ses);
					break;
				}
				ptr=ps; op=GO_NEXT;
			case GO_NEXT: 
				if (ptr<pe) {
					if (sk!=NULL && lsk!=0) {
						findValue(ptr,pe,sk,lsk,ptr);
						if (ptr>=pe) {
							if ((state&SC_LASTSP)!=0) {ps=NULL; state|=SC_ADV; continue;}
							findSubPage(sk,(ushort)lsk); break;
						}
					}
					if ((state&SC_FIXED)!=0) {q=ptr; lD=lElt-lpref; ptr+=lD;}
					else if (lElt==1) {q=ps+ptr[0]; lD=ptr[1]-ptr[0]; ++ptr;}
					else {q=ps+_u16(ptr); lD=_u16(ptr+2)-_u16(ptr); ptr+=2;}
					if (lpref==0) return q;
			add_pref:
					assert(buf!=NULL);
					if ((l=lD+lpref)+bufsht>lbuf) {
						bool fAdj=ps==buf;
						if ((buf=(byte*)ses->realloc(buf,lbuf=bufsht+l))==NULL) 
							{lD=ldata=bufsht=lpref=0; if (fAdj) ps=pe=ptr=NULL; return NULL;}
						if (fAdj) {pe=buf+(pe-ps); ptr=buf+(ptr-ps); ps=buf;}
					}
					memcpy(buf+bufsht+lpref,q,lD); lD=l; return buf+bufsht;
				}
				if ((state&SC_LASTSP)!=0) {ps=NULL; state|=SC_ADV; continue;}
				if (sk!=NULL && lsk>0) {
					q=pe-(l=lElt); if ((state&SC_FIXED)==0) {assert(lElt==2); l=_u16(q+2)-_u16(q); q=ps+_u16(q);}
					if ((state&SC_PINREF)!=0) cmp=PINRef::cmpPIDs(q,(unsigned)l,sk,(unsigned)lsk); else if ((cmp=memcmp(q,sk,min(l,lsk)))==0) cmp=cmp3(l,lsk);
					if (cmp<0) {findSubPage(sk,lsk); break;}
				}
				if (subpg!=NULL) {
					const TreePageMgr::TreePage *tp2=(const TreePageMgr::TreePage*)subpg->getPageBuf();
					assert(tp2->info.sibling!=INVALID_PAGEID);
					subpg=ctx->bufMgr->getPage(tp2->info.sibling,ctx->trpgMgr,PGCTL_COUPLE|QMGR_SCAN,subpg,ses);
				} else {
					PageID pid=sPage; sPage=INVALID_PAGEID; assert(pid!=INVALID_PAGEID);
					if ((subpg=tree->getPage(pid,stamp,PITREE_LEAF2))==NULL) {
		assert(0&&"nextValue 4");
						//sk=ps+_u16(pe) lk=_u16(pe+2)-_u16(pe)
						// if (lpref!=0) -> concatenate
						// findSubPage
					}
				}
				break;
			case GO_LAST:
				if ((state&SC_EOF)==0) {ps=NULL; state|=SC_ADV; continue;}
				if ((state&SC_LASTSP)==0) {findSubPage(sk,lsk,false); break;}
				ptr=pe; op=GO_PREVIOUS;
			case GO_PREVIOUS:
				if (ptr>ps) {
					if (sk!=NULL && lsk!=0) {
						if (findValue(ps,ptr,sk,lsk,ptr)) ptr+=lElt; 
						else if (ptr==ps) {
							if ((state&SC_FIRSTSP)!=0) {ps=NULL; state|=SC_ADV; continue;}
							findSubPage(sk,lsk,false); break;
						}
					}
					if ((state&SC_FIXED)!=0) {lD=lElt-lpref; q=ptr-=lD;}
					else if (lElt==1) {--ptr; q=ps+ptr[0]; lD=ptr[1]-ptr[0];}
					else {ptr-=2; q=ps+_u16(ptr); lD=_u16(ptr+2)-_u16(ptr);}
					if (lpref==0) return q; goto add_pref;
				}
				if ((state&SC_FIRSTSP)!=0) {ps=NULL; state|=SC_ADV; continue;}
				if (sk!=NULL && lsk>0) {
					if ((state&SC_FIXED)!=0) q=ps,l=lElt; else if (lElt==1) q=ps+ps[0],l=ps[1]-ps[0]; else q=ps+_u16(ps),l=_u16(ps+2)-_u16(ps);
					if ((state&SC_PINREF)!=0) cmp=PINRef::cmpPIDs(q,(unsigned)l,sk,(unsigned)lsk); else if ((cmp=memcmp(q,sk,min(l,lsk)))==0) cmp=cmp3(l,lsk);
					if (cmp>0) {
		assert(0&&"nextValue 7");
						// search
					}
				}
				getPrevSubPage(); break;
			}
			for (;;) {
				if (subpg==NULL) {state|=SC_ADV; ps=NULL; break;}
				const TreePageMgr::TreePage *tp2=(TreePageMgr::TreePage*)subpg->getPageBuf();
				if (tp2->hdr.pageID!=anchor) state&=~SC_FIRSTSP; else state|=SC_FIRSTSP;
				if (tp2->hasSibling()) state&=~SC_LASTSP; else state|=SC_LASTSP; state|=SC_SAVEPS;
				ps=(const byte*)(tp2+1); pe=ps+tp2->info.nSearchKeys*lElt; bufsht=0; sPage=INVALID_PAGEID;
				ldata=(state&SC_FIXED)==0?((ushort*)(tp2+1))[tp2->info.nEntries]:(lElt-tp2->info.lPrefix)*tp2->info.nEntries;
				if ((lpref=tp2->info.lPrefix)!=0) {
					const byte *pref=(state&SC_FIXED)==0?(byte*)tp2+((PagePtr*)&tp2->info.prefix)->offset:(byte*)&tp2->info.prefix;
					size_t l=lpref+LBUF_EXTRA;
					if (l>lbuf) buf=(byte*)ses->realloc(buf,lbuf=l);
					if (buf==NULL) {ldata=lpref=lbuf=0; ps=pe=ptr=NULL; return NULL;}
					memcpy(buf,pref,lpref);
				}
				if (sk==NULL || lsk==0) ptr=fF?ps:pe;
				else if (findValue(ps,pe,sk,lsk,ptr)) {if (!fF) ptr+=lElt;}
				else if (fF && ptr>=pe) {
					do {if (!tp2->hasSibling()) {subpg->release(0,ses); subpg=NULL; break;}}
					while ((subpg=ctx->bufMgr->getPage(tp2->info.sibling,ctx->trpgMgr,PGCTL_COUPLE|QMGR_SCAN,subpg,ses))!=NULL
									&& (tp2=(TreePageMgr::TreePage*)subpg->getPageBuf())->info.nSearchKeys==0);
					continue;
				} else if (!fF && ptr<=ps) {
					getPrevSubPage(); if (subpg==NULL) break; else continue;
				}
				// release 'page' ???
				if (fF && (state&(SC_FIRSTSP|SC_LASTSP))==SC_FIRSTSP) {
					//prefetch from parent
				}
				break;
			}
		}
	}
	RC skip(unsigned& nSkip,bool fBack) {
		if ((state&SC_INIT)==0) return RC_OTHER; if ((state&SC_UNIQUE)!=0) {--nSkip; return RC_EOF;} size_t lD;
		for (GO_DIR op=fBack?GO_LAST:GO_FIRST,op2=fBack?GO_PREVIOUS:GO_NEXT; nextValue(lD,op,NULL,0)!=NULL; op=op2) {
			unsigned n=(unsigned)((pe-ps)/lElt);
			if (n>=nSkip) {ptr=fBack?pe-nSkip*lElt:ps+nSkip*lElt; nSkip=0; return RC_OK;}
			ptr=fBack?ps:pe; nSkip-=n;
		}
		return RC_EOF;
	}
	void releaseLatches(PageID pid,PageMgr *mgr,bool fX) {
		if ((subpg!=NULL || !pb.isNull() || !parent.isNull()) && (pid==INVALID_PAGEID || fX || mgr==NULL || mgr->getPGID()!=PGID_INDEX)) release();
	}
	void checkNotHeld(PBlock *p) {
		// subpg ??? -> state
		assert(pb.isNull() || pb.isSet(PGCTL_INREL) || (PBlock*)pb!=p);
		assert(parent.isNull() || parent.isSet(PGCTL_INREL) || (PBlock*)parent!=p);
	}
	void release() {
		if ((state&SC_SAVEPS)!=0) {
			const byte *pref=NULL; lpref=0; state&=~SC_SAVEPS; assert(ps!=NULL);
			if (subpg!=NULL) {
				const TreePageMgr::TreePage* tp=(const TreePageMgr::TreePage*)subpg->getPageBuf();
				pref=(state&SC_FIXED)==0?(byte*)tp+((PagePtr*)&tp->info.prefix)->offset:(byte*)&tp->info.prefix;
				lpref=tp->info.lPrefix; sPage=tp->info.sibling; stamp2=tree->getStamp(PITREE_LEAF2);
			}
			size_t l=bufsht=ldata; if (lpref!=0) l+=lpref+LBUF_EXTRA;
			if (l>lbuf) buf=(byte*)ses->realloc(buf,lbuf=l);
			if (buf==NULL) {ldata=bufsht=lpref=lbuf=0; ps=pe=ptr=NULL;}
			else {memcpy(buf,ps,ldata); if (lpref!=0) memcpy(buf+ldata,pref,lpref); ptr=buf+(ptr-ps); pe=buf+(pe-ps); ps=buf;}
		}
		if (subpg!=NULL) {subpg->release(0,ses); subpg=NULL;}
		if (!pb.isNull()) {
			const TreePageMgr::TreePage* tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
			kPage=tp->hdr.pageID; nPage=tp->info.sibling; stamp=tree->getStamp(PITREE_LEAF); lsn=tp->hdr.lsn;
			if (index+1==tp->info.nSearchKeys) state|=SC_LASTKEY;
			//else check boundary for next
			if ((state&SCAN_EXACT)==0) saveKey(); pb.release(ses);
		}
		parent.release(ses); depth=0;
	}
	const SearchKey& getKey() {
		if ((state&SCAN_EXACT)!=0) return *start;
		assert((state&SC_INIT)==0);
		if (!savedKey->isSet()) saveKey();
		return *savedKey;
	}
	bool hasValues() {
		assert((state&SC_INIT)==0);
		if (ps!=NULL) return true;
		if (subpg!=NULL) {
			if (((const TreePageMgr::TreePage*)subpg->getPageBuf())->info.nSearchKeys!=0) return true;
			subpg->release(0,ses); subpg=NULL;
		}
		if ((state&SC_KEYSET)!=0 && (!pb.isNull() || restore())) {
			const TreePageMgr::TreePage* tp=(const TreePageMgr::TreePage*)pb->getPageBuf(); const PagePtr *pvp; ushort ld;
			if (tp->findData(index,ld,&pvp)!=NULL) {
				if ((state&SC_UNIQUE)!=0 || !TreePageMgr::TreePage::isSubTree(*pvp)) return true;
				TreePageMgr::SubTreePage ext; memcpy(&ext,(byte*)tp+pvp->offset,sizeof(ext));
				for (PageID pid=ext.anchor; pid!=INVALID_PAGEID && (subpg=ctx->bufMgr->getPage(pid,ctx->trpgMgr,PGCTL_COUPLE|QMGR_SCAN,subpg,ses))!=NULL;) {
					tp=(const TreePageMgr::TreePage*)subpg->getPageBuf();
					if (tp->isLeaf()) {
						for (;;) {
							if (tp->info.nSearchKeys>0) {subpg->release(0,ses); subpg=NULL; return true;}
							if (!tp->hasSibling()) {subpg->release(0,ses); subpg=NULL; break;}
							if ((subpg=ctx->bufMgr->getPage(tp->info.sibling,ctx->trpgMgr,PGCTL_COUPLE|QMGR_SCAN,subpg,ses))==NULL) break;
							tp=(const TreePageMgr::TreePage*)subpg->getPageBuf();
						}
						break;
					}
				}
			}
		}
		return false;
	}
	RC rewind() {
		ps=pe=ptr=NULL; if (subpg!=NULL) {subpg->release(0,ses); subpg=NULL;}
		pb.release(ses); state=state&~(SC_KEYSET|SC_ADV|SC_SAVEPS)|SC_BOF|SC_INIT;
		return RC_OK;
	}
	void destroy() {delete this;}
};

};

TreeScan *Tree::scan(Session *ses,const SearchKey *start,const SearchKey *finish,unsigned flags,const IndexSeg *sg,unsigned nSegs,IKeyCallback *kcb)
{
	return new(ses) TreeScanImpl(ses,*this,start,finish,flags,sg,nSegs,kcb);
}
