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
#define	SC_SAVEPS	0x10000000
#define	SC_KEYSET	0x08000000
#define	SC_LASTKEY	0x04000000
#define	SC_FIRSTSP	0x02000000
#define	SC_LASTSP	0x01000000
#define	SC_FIXED	0x00800000
#define	SC_ADV		0x00400000
#define	SC_PINREF	0x00200000
#define	SC_SINGLE	0x00100000

#define	LBUF_EXTRA	24

class TreeScanImpl : public TreeScan, public TreeCtx, public LatchHolder
{
	Session			*const	ses;
	StoreCtx		*const	ctx;
	unsigned				state;
	const SearchKey	*const	start;
	const SearchKey	*const	finish;
	const IndexSeg	*const	segs;
	const unsigned			nSegs;
	IKeyCallback	*const	keycb;
	bool					fHyper;
	const IndexFormat		ifmt;

	PageID					kPage;
	PageID					nPage;
	LSN						lsn;
	unsigned				stamp;
	SearchKey				*savedKey;
	size_t					lKeyBuf;

	PBlock					*subpg;
	PageID					sPage;
	PageID					anchor;
	unsigned				stamp2;

	const byte				*ps,*pe,*ptr,*frame;
	size_t					lElt;
	byte					*buf;
	size_t					lbuf;

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
	void findSubPage(const byte *k,size_t lk,bool fF) {
		if (fF && (k==NULL || lk==0)) subpg=ctx->bufMgr->getPage(anchor,ctx->trpgMgr,QMGR_SCAN,subpg,ses);
		else {
			if (subpg!=NULL) {subpg->release(0,ses); subpg=NULL;}
			if ((state&SC_KEYSET)!=0 && (!pb.isNull() || restore())) {
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
		}
		if (subpg!=NULL) {frame=(byte*)((TreePageMgr::TreePage*)subpg->getPageBuf()+1);}
	}
	void getPrevSubPage() {
		if (subpg==NULL) {
			//??? restore subpg
		}
		assert(0&&"getPrevSubPage");
		if (subpg!=NULL) {subpg->release(0,ses); subpg=NULL;}
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
		ifmt(tr.indexFormat()),kPage(INVALID_PAGEID),lsn(0),stamp(0),savedKey(NULL),lKeyBuf(0),subpg(NULL),sPage(INVALID_PAGEID),stamp2(0),
		ps(NULL),pe(NULL),ptr(NULL),frame(NULL),lElt(L_SHT),buf(NULL),lbuf(0)
	{
			if (ifmt.isFixedLenData()) {state|=SC_FIXED; lElt=ifmt.dataLength();} else if (ifmt.isPinRef()) state|=SC_PINREF;
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
				ps=pe=ptr=NULL; state&=~SC_SAVEPS; if (subpg!=NULL) {subpg->release(0,ses); subpg=NULL;}
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
			const byte *q; size_t l; int cmp; ushort ld,off;
			if (ps==NULL) {
				if ((state&SC_KEYSET)==0 || (state&SC_ADV)!=0) {
					state&=~SC_ADV; if (nextKey(op)!=RC_OK) {lD=0; return NULL;}
				} else if (pb.isNull() && !restore() && (pb.isNull() || !checkBounds((const TreePageMgr::TreePage*)pb->getPageBuf(),fF))) {
					pb.release(ses); state=state&~SC_KEYSET|(fF?SC_EOF:SC_BOF); lD=0; return NULL;
				}
				const PagePtr *pvp; assert(!pb.isNull());
				const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
				if ((ps=(const byte*)tp->findData(index,ld,&pvp))==NULL) {state|=SC_ADV; continue;}
				state|=SC_SAVEPS|SC_FIRSTSP|SC_LASTSP; pe=ps+ld; vp=*pvp; frame=(byte*)tp;
				if ((state&SC_FIXED)!=0 || (vp.len&TRO_MANY)==0) {lD=ld; ptr=fF?pe:ps; return ps;}
				if (!tp->isSubTree(vp)) {
					if (sk==NULL || lsk==0) ptr=fF?ps:pe;
					else if ((ptr=TreePageMgr::findValue(sk,(ushort)lsk,frame,ps,vp.len,(state&SC_PINREF)!=0,&off))!=NULL) {if (!fF) ptr+=L_SHT;}
					else {ptr=ps+off; if (fF && ptr>=pe || !fF && ptr<=ps) {ps=NULL; state|=SC_ADV;}}
					continue;
				}
				TreePageMgr::SubTreePage ext; memcpy(&ext,(byte*)tp+vp.offset,sizeof(ext));
				ps=pe=ptr=frame=NULL; anchor=ext.anchor; findSubPage(sk,lsk,fF);
			} else switch (op) {
			default: assert(0);
			case GO_FIRST: case GO_LAST:
				if ((state&(fF?SC_BOF:SC_EOF))==0) {ps=NULL; state|=SC_ADV; continue;}
				if ((state&(fF?SC_FIRSTSP:SC_LASTSP))==0) {findSubPage(sk,lsk,fF); break;}
				if (fF) {ptr=ps; op=GO_NEXT;} else {ptr=pe; op=GO_PREVIOUS;}
			case GO_NEXT: case GO_PREVIOUS:
				if (fF && ptr<pe || !fF && ptr>ps) {
					if ((vp.len&TRO_MANY)==0) {
						if (!fF) q=ptr=ps; else {q=ptr; ptr=pe;} ld=uint16_t(pe-ps);
					} else {
						if (sk!=NULL && lsk!=0) {
							const byte *p=TreePageMgr::findValue(sk,(ushort)lsk,frame,fF?ptr:ps,uint16_t(fF?pe-ptr:ptr-ps)|TRO_MANY,(state&SC_PINREF)!=0,&off);
							if (p==NULL) {if (fF) ptr+=off;} else if (!fF) ptr=p+L_SHT; else ptr=p;
							if (fF && ptr>=pe || !fF && off==0) {
								if ((state&(fF?SC_LASTSP:SC_FIRSTSP))!=0) {ps=NULL; state|=SC_ADV; continue;}
								findSubPage(sk,(ushort)lsk,fF); break;
							}
						}
						if (!fF) ptr-=L_SHT; q=TreePageMgr::getK(frame,ptr,ld); if (fF) ptr+=L_SHT;
					}
					lD=ld; return q;
				}
				if ((state&(fF?SC_LASTSP:SC_FIRSTSP))!=0) {ps=NULL; state|=SC_ADV; continue;}
				if (sk!=NULL && lsk>0) {
					q=fF?pe-lElt:ps; if ((state&SC_FIXED)==0) {q=TreePageMgr::getK(frame,q,ld); l=ld;} else l=lElt;
					if ((state&SC_PINREF)!=0) cmp=PINRef::cmpPIDs(q,sk); else if ((cmp=memcmp(q,sk,min(l,lsk)))==0) cmp=cmp3(l,lsk);
					if (fF && cmp<0) {findSubPage(sk,lsk,fF); break;}
					else if (!fF && cmp>0) {
		assert(0&&"nextValue 7");
						// search
					}
				}
				if (!fF) getPrevSubPage();
				else if (subpg!=NULL) {
					const TreePageMgr::TreePage *tp2=(const TreePageMgr::TreePage*)subpg->getPageBuf();
					assert(tp2->info.sibling!=INVALID_PAGEID);
					subpg=ctx->bufMgr->getPage(tp2->info.sibling,ctx->trpgMgr,PGCTL_COUPLE|QMGR_SCAN,subpg,ses);
				} else {
					PageID pid=sPage; sPage=INVALID_PAGEID; assert(pid!=INVALID_PAGEID);
					if ((subpg=tree->getPage(pid,stamp,PITREE_LEAF2))==NULL) {
		assert(0&&"nextValue 4");
						//sk=ps+_u16(pe) lk=_u16(pe+2)-_u16(pe)
						// findSubPage
					}
				}
				break;
			}
			for (;;) {
				if (subpg==NULL) {state|=SC_ADV; ps=NULL; break;}
				const TreePageMgr::TreePage *tp2=(TreePageMgr::TreePage*)subpg->getPageBuf();
				if (tp2->hdr.pageID!=anchor) state&=~SC_FIRSTSP; else state|=SC_FIRSTSP;
				if (tp2->hasSibling()) state&=~SC_LASTSP; else state|=SC_LASTSP; state|=SC_SAVEPS;
				frame=ps=(const byte*)(tp2+1); pe=ps+tp2->info.nSearchKeys*L_SHT; sPage=INVALID_PAGEID;
				if (sk==NULL || lsk==0) ptr=fF?ps:pe;
				else if ((ptr=TreePageMgr::findValue(sk,(ushort)lsk,frame,ps,ushort(pe-ps)|TRO_MANY,(state&SC_PINREF)!=0,&off))!=NULL) {if (!fF) ptr+=L_SHT;}
				else {
					ptr=ps+off;
					if (fF && ptr>=pe) {
						do {if (!tp2->hasSibling()) {subpg->release(0,ses); subpg=NULL; break;}}
						while ((subpg=ctx->bufMgr->getPage(tp2->info.sibling,ctx->trpgMgr,PGCTL_COUPLE|QMGR_SCAN,subpg,ses))!=NULL
										&& (tp2=(TreePageMgr::TreePage*)subpg->getPageBuf())->info.nSearchKeys==0);
						continue;
					}
					if (!fF && ptr<=ps) {getPrevSubPage(); if (subpg==NULL) break; else continue;}
					// release 'page' ???
					if (fF && (state&(SC_FIRSTSP|SC_LASTSP))==SC_FIRSTSP) {
						//prefetch from parent
					}
				}
				break;
			}
		}
	}
	RC skip(unsigned& nSkip,bool fBack) {
		if ((state&SC_INIT)==0) return RC_OTHER; if ((state&SC_FIXED)!=0) {--nSkip; return RC_EOF;} size_t lD;
		for (GO_DIR op=fBack?GO_LAST:GO_FIRST,op2=fBack?GO_PREVIOUS:GO_NEXT; nextValue(lD,op,NULL,0)!=NULL; op=op2) {
			unsigned n=(unsigned)((vp.len&TRO_MANY)!=0?(pe-ps)/L_SHT:1);
			if (n>=nSkip) {if ((vp.len&TRO_MANY)!=0) ptr=fBack?pe-nSkip*lElt:ps+nSkip*lElt; nSkip=0; return RC_OK;}
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
			const TreePageMgr::TreePage* tp=NULL; state&=~SC_SAVEPS; assert(ps!=NULL);
			size_t l=ses->getStore()->trpgMgr->contentSize();
			if (subpg!=NULL) {
				tp=(const TreePageMgr::TreePage*)subpg->getPageBuf();
				sPage=tp->info.sibling; stamp2=tree->getStamp(PITREE_LEAF2);
				l-=tp->info.freeSpaceLength;
			} else if ((state&SC_FIXED)==0 && (vp.len&TRO_MANY)!=0) {
				//l???
			} else l=pe-ps;
			if (l>lbuf) buf=(byte*)ses->realloc(buf,lbuf=l);
			if (buf==NULL) {lbuf=0; ps=pe=ptr=NULL;}
			else if (tp!=NULL) {
				memcpy(buf,ps,pe-ps); memcpy(buf+(pe-ps),(byte*)tp+tp->info.freeSpace,l-(pe-ps));
				frame=buf-(tp->info.freeSpace-sizeof(TreePageMgr::TreePage))+(pe-ps); ptr=buf+(ptr-ps); pe=buf+(pe-ps); ps=buf;
			} else if ((state&SC_FIXED)==0 && (vp.len&TRO_MANY)!=0) {
				ushort *p=(ushort*)buf; size_t left=lbuf,bsht=lbuf;
				for (const byte *pp=ps; pp<pe; pp+=L_SHT) {
					uint16_t ll; const byte *q=TreePageMgr::getK(frame,pp,ll);
					if (L_SHT+ll>left) {
						// realloc
					}
					*p++=ushort(bsht-=ll); memcpy(buf+bsht,q,ll); left-=L_SHT+ll;
				}
				frame=buf; ptr=buf+(ptr-ps); pe=buf+(pe-ps); ps=buf;
			} else {
				memcpy(buf,ps,l); frame=buf; ptr=buf+(ptr-ps); pe=buf+l;  ps=buf;
			}
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
				if ((state&SC_FIXED)!=0 || !tp->isSubTree(*pvp)) return true;
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
		ps=pe=ptr=frame=NULL; if (subpg!=NULL) {subpg->release(0,ses); subpg=NULL;}
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
