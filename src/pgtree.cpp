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

#include "pinref.h"
#include "pgtree.h"
#include "idxtree.h"
#include "buffer.h"
#include "txmgr.h"
#include "logmgr.h"
#include "fsmgr.h"

using namespace AfyKernel;

TreePageMgr::TreePageMgr(StoreCtx *ctx) : TxPage(ctx),xSize(ctx->bufMgr->getPageSize()-sizeof(TreePage)-FOOTERSIZE)
{
	ctx->registerPageMgr(PGID_INDEX,this);
}

RC TreePageMgr::update(PBlock *pb,size_t len,unsigned info,const byte *rec,size_t lrec,unsigned flags,PBlock *newp)
{
	if (rec==NULL || lrec==0) return RC_CORRUPTED;
	byte *frame=pb->getPageBuf(); TreePage *tp=(TreePage*)frame,*tp2=NULL;
	TREE_OP op=(TREE_OP)(info&TRO_MASK); unsigned idx=unsigned(info>>TRO_SHIFT&0xFFFF),idx1,idx2;
	if (tp->info.level>TREE_MAX_DEPTH || tp->info.nEntries!=tp->info.nSearchKeys+(tp->info.sibling!=INVALID_PAGEID?1:0))
		return RC_CORRUPTED;
	assert(newp==NULL || op==TRO_SPLIT || op==TRO_MERGE || op==TRO_SPAWN || op==TRO_ABSORB);
	static const unsigned infoSize[TRO_ALL] = {
		sizeof(TreePageModify),sizeof(TreePageModify),sizeof(TreePageModify),sizeof(TreePageEdit),
		sizeof(TreePageInit),sizeof(TreePageSplit),sizeof(TreePageSplit),
		sizeof(TreePageSpawn),sizeof(TreePageSpawn),sizeof(TreePageRoot),sizeof(TreePageRoot),
		0,0,sizeof(TreePageMulti),sizeof(int64_t),sizeof(TreePageAppend)
	};
	if (lrec<infoSize[op]) {
		report(MSG_ERROR,"Invalid lrec %d in TreePageMgr::update, op %d, page %08X\n",lrec,op,tp->hdr.pageID);
		return RC_CORRUPTED;
	}
	unsigned n; const byte *p1,*p2; uint16_t l1,l2,lkk; int icmp,delta; SearchKey sKey; RC rc;
	const TreePageModify *tpm=NULL; const TreePageEdit *tpe; const TreePageSplit *tps; const TreePageMulti *tpmi;
	const TreePageInit *tpi; const TreePageSpawn *tpa; const TreePageRoot *tpr; const SubTreePageKey *tpk;
	const PagePtr *oldData=NULL,*newData=NULL; uint16_t lElt,l,ll,off,newPrefSize=0; byte *ptr; PagePtr *vp,pp; SubTreePage tpExt;
	bool fFree,fSub=tp->info.fmt.isKeyOnly(),fSeq=tp->info.fmt.isSeq(),fPR;
	const TreePageAppend *tap; const TreePageAppendElt *tae;
	const static TREE_OP undoOP[TRO_ALL] = {
		TRO_DELETE,TRO_UPDATE,TRO_INSERT,TRO_EDIT,TRO_INIT,TRO_SPLIT,TRO_MERGE,TRO_ABSORB,
		TRO_SPAWN,TRO_DELROOT,TRO_ADDROOT,TRO_REPLACE,TRO_DROP,TRO_MULTI,TRO_COUNTER,TRO_APPEND
	};

	if (op<=TRO_DELETE) {
		tpm=(const TreePageModify*)rec; oldData=&tpm->oldData; newData=&tpm->newData;
		if (tp->info.fmt.keyType()!=*(byte*)(tpm+1)) return RC_CORRUPTED;
		newPrefSize=tpm->newPrefSize;
	}
	if ((flags&TXMGR_UNDO)!=0) {
		op=undoOP[op];
		if (op<=TRO_DELETE) {
			const PagePtr *tmp=newData; newData=oldData; oldData=tmp; newPrefSize=tp->info.lPrefix;
			if ((rc=sKey.deserialize(tpm+1,lrec-sizeof(TreePageModify)))!=RC_OK) return rc;
			if (tp->findKey(sKey,idx)==(op==TRO_INSERT)) {
				bool fCorrupted=true;
				if (!tp->info.fmt.isFixedLenData()) switch (op) {
				default: break;
				case TRO_INSERT: op=TRO_UPDATE; fCorrupted=false; break;
				case TRO_DELETE: op=TRO_UPDATE; fCorrupted=false; break;		//???
				case TRO_UPDATE: if (oldData->len==0) {op=TRO_INSERT; fCorrupted=false;} break;
				}
				if (fCorrupted) {report(MSG_ERROR,"UNDO in TreePageMgr::update: invalid key, op %d, page %08X\n",op,tp->hdr.pageID); return RC_CORRUPTED;}
			}
			if (!fSeq && op==TRO_INSERT && tp->info.nEntries!=0 && tp->info.lPrefix!=0 && (idx==0||idx==tp->info.nEntries))
				newPrefSize=tp->calcPrefixSize(sKey,idx==0?tp->info.nEntries-1:0);
		}
	}
#ifdef _DEBUG
	else if (op<=TRO_DELETE && (idx>tp->info.nEntries || op!=TRO_INSERT && idx>=tp->info.nSearchKeys)) return RC_CORRUPTED;
	else {
		// check key at idx equals op!=TRO_INSERT
	}
#endif

	switch (op) {
	default: return RC_CORRUPTED;
	case TRO_INSERT:
		l=newData->len; if (oldData->len!=0) return RC_CORRUPTED;
		if (!fSeq) {
			if (tp->info.nEntries==0 && !tp->info.fmt.isKeyOnly() && tp->info.fmt.keyType()<KT_REF) {
				if ((rc=tp->prefixFromKey(tpm+1))!=RC_OK) return rc;
			} else {
				if (newPrefSize<tp->info.lPrefix) {
					uint16_t extraSize=tp->info.extraEltSize(newPrefSize);
					if (extraSize==0) {
						tp->info.lPrefix=newPrefSize;
						if (tp->info.fmt.isPrefNumKey()) tp->info.prefix=newPrefSize==0?0:tp->info.prefix&~0ULL<<(sizeof(uint64_t)-newPrefSize)*8;
					} else if (unsigned(extraSize)*tp->info.nEntries>unsigned(tp->info.freeSpaceLength+tp->info.scatteredFreeSpace)) {
						report(MSG_ERROR,"TreePageMgr::update insert: insufficient space, newPrefSize=%d, page %08X\n",newPrefSize,tp->hdr.pageID);
						return RC_PAGEFULL;
					} else 
						tp->changePrefixSize(newPrefSize);
				} else if (newPrefSize>tp->info.lPrefix) return RC_CORRUPTED;
				if (!tp->info.fmt.isFixedLenKey()) {
					if (*(byte*)(tpm+1)<KT_BIN || (l1=SearchKey::keyLen(tpm+1))<tp->info.lPrefix) return RC_CORRUPTED;
					if (tp->info.fmt.isFixedLenData()) l=l1-tp->info.lPrefix; else l+=l1-tp->info.lPrefix;
				}
			}
		}
		if ((rc=tp->insertKey(idx,(byte*)(tpm+1),rec+newData->offset,newData->len,oldData->offset,l))!=RC_OK) return rc;
		break;
	case TRO_APPEND:
		if (fSeq) return RC_CORRUPTED; if ((flags&TXMGR_UNDO)!=0) return RC_OK;
		tap=(const TreePageAppend*)rec; tae=(const TreePageAppendElt*)(tap+1);
		if ((newPrefSize=tap->newPrefixSize)<tp->info.lPrefix) {
			uint16_t extraSize=tp->info.extraEltSize(newPrefSize);
			if (extraSize==0) {
				tp->info.lPrefix=newPrefSize;
				if (tp->info.fmt.isPrefNumKey()) tp->info.prefix=newPrefSize==0?0:tp->info.prefix&~0ULL<<(sizeof(uint64_t)-newPrefSize)*8;
			} else if (unsigned(extraSize)*tp->info.nEntries>unsigned(tp->info.freeSpaceLength+tp->info.scatteredFreeSpace)) {
				report(MSG_ERROR,"TreePageMgr::update append: insufficient space, newPrefSize=%d, page %08X\n",newPrefSize,tp->hdr.pageID);
				return RC_PAGEFULL;
			} else 
				tp->changePrefixSize(newPrefSize);
		} else if (newPrefSize>tp->info.lPrefix) {
			if (tp->info.nEntries!=0) return RC_CORRUPTED;
			if ((rc=tp->prefixFromKey(tae+1,newPrefSize))!=RC_OK) return rc;
		}
		for (n=0; n<(unsigned)tap->nKeys; n++) {
			byte *end=(byte*)(tae+1)+tae->lkey+(l=tae->ldata); if (end>rec+lrec) return RC_CORRUPTED;
			if (!tp->info.fmt.isFixedLenKey()) {
				if (*(byte*)(tae+1)<KT_BIN || (l1=SearchKey::keyLen(tae+1))<tp->info.lPrefix) return RC_CORRUPTED;
				if (tp->info.fmt.isFixedLenData()) l=l1-tp->info.lPrefix; else l+=l1-tp->info.lPrefix;
			}
			if ((rc=tp->insertKey(tae->idx,(byte*)(tae+1),(byte*)(tae+1)+tae->lkey,tae->ldata,tae->ndata,l))!=RC_OK) return rc;
			tae=(const TreePageAppendElt*)end;
		}
		break;
	case TRO_UPDATE:
		assert(tp->isLeaf() && idx<tp->info.nSearchKeys);
		if (tp->info.fmt.isKeyOnly()) {
			if (!tp->info.fmt.isRefKeyOnly() || oldData->len!=0 || newData->len!=0) return RC_CORRUPTED;
			if ((rc=tp->adjustKeyCount(idx,(byte*)(tpm+1)+2,((byte*)(tpm+1))[1]))!=RC_OK) return rc;
			break;
		}
		if ((ptr=(byte*)tp->findData(idx,l,(const PagePtr**)&vp))==NULL) {
			report(MSG_ERROR,"TreePageMgr::update UPDATE: data not found, idx=%d, page %08X\n",idx,tp->hdr.pageID);
			return RC_NOTFOUND;
		}
		// assert oldData -> same as ptr,l
		if (vp==NULL) {assert(tp->info.fmt.isFixedLenData()&&l==newData->len); memcpy(ptr,rec+newData->offset,l); break;}
	update_entry:
		assert(!tp->info.fmt.isFixedLenData() && vp!=NULL);
		if (tp->isSubTree(*vp)) {
			report(MSG_ERROR,"TreePageMgr::update UPDATE: secondary tree, idx=%d, page %08X\n",idx,tp->hdr.pageID);
			return RC_INTERNAL;
		}
		if (oldData->len!=0) {
			if (newData->len==0 && newData->offset!=0) {
				rec+=oldData->offset; lrec=oldData->len; n=newData->offset;
				for (unsigned idx2=0; idx2<n; ) {
					p1=getK(rec,idx2,l1); lkk=l1;
					if ((ptr=tp->findValue(p1,l1,*vp,&off))==NULL) return RC_NOTFOUND;
					if (tp->info.fmt.isPinRef()) {
						p2=(byte*)tp+vp->offset+off; p2=(byte*)tp+_u16(p2); l2=PINRef::len(p2);
						if (PINRef::getCount(p1)!=PINRef::getCount(p2)) {
							if ((rc=tp->adjustCount(ptr,vp,idx,p1,l1,true))!=RC_OK) return rc;
							idx2++; continue;
						}
					}
					uint16_t off0=off; unsigned idx0=idx2; int cmp;
					do {
						off+=L_SHT; if (++idx2>=n) break; p2=(byte*)tp+vp->offset+off;
						p1=getK(rec,idx2,l1); lkk+=l1; p2=(byte*)tp+_u16(p2); l2=PINRef::len(p2);
						if (!tp->info.fmt.isPinRef()) {if ((cmp=memcmp(p1,p2,min(l1,l2)))==0) cmp=cmp3(l1,l2);}
						else if ((cmp=PINRef::cmpPIDs(p1,p2))==0 && PINRef::getCount(p1)!=PINRef::getCount(p2)) {
							if ((rc=tp->adjustCount((byte*)tp+vp->offset+off,vp,idx,p1,l1,true))!=RC_OK) return rc;
							break;
						}
						if (cmp<0) return RC_NOTFOUND;
					} while (cmp==0);
					tp->deleteValues(lkk,vp,off0,uint16_t(idx2-idx0));
				}
			} else if ((ptr=tp->findValue(rec+oldData->offset,oldData->len,*vp,&off))==NULL) {
				report(MSG_ERROR,"TreePageMgr::update UPDATE: value not found, idx=%d, page %08X\n",idx,tp->hdr.pageID);
				return RC_NOTFOUND;
			} else {
				if (newData->len==oldData->len) {
					uint16_t off2;
					if (tp->findValue(rec+newData->offset,newData->len,*vp,&off2)!=NULL && off2==off) break;
					if (off2==off || off2==off+(tp->info.fmt.isFixedLenData()?oldData->len:L_SHT)) {
						if (tp->info.fmt.isFixedLenData()) memcpy(ptr,rec+newData->offset,newData->len);
						else if ((vp->len&TRO_MANY)==0) memcpy((byte*)tp+vp->offset,rec+newData->offset,newData->len);
						else memcpy((byte*)tp+_u16((byte*)tp+vp->offset+off),rec+newData->offset,newData->len);
						break;
					}
					// other variants with memmove
				}
				if (!tp->info.fmt.isPinRef()) tp->deleteValues(oldData->len,vp,off);
				else if ((rc=tp->adjustCount(ptr,vp,idx,rec+oldData->offset,oldData->len,true))!=RC_OK) return rc;
			}
		}
		if (newData->len!=0) {
			if (oldData->len!=0 || oldData->offset==0) {
				if ((ptr=tp->findValue(rec+newData->offset,newData->len,*vp,&off))!=NULL) {
					if ((rc=tp->adjustCount(ptr,vp,idx,rec+newData->offset,newData->len))!=RC_OK) return rc;
				} else if (!tp->insertValues(rec+newData->offset,newData->len,vp,off,idx))
					return RC_PAGEFULL;
			} else {
				rec+=newData->offset; lrec=newData->len; fPR=tp->info.fmt.isRefKeyOnly(); n=oldData->offset; assert((n&0x8000)==0);
				for (unsigned idx2=0; idx2<n; ) {
					p1=getK(rec,idx2,l1);
					if ((ptr=tp->findValue(p1,l1,*vp,&off))!=NULL) {
						if ((rc=tp->adjustCount(ptr,vp,idx,p1,l1))!=RC_OK) return rc;
						idx2++;
					} else {
						unsigned idx0=idx2; const byte *pd=(byte*)tp+vp->offset; lkk=l1;
						if (++idx2<n) {
							if (off>=(vp->len&~TRO_MANY)) for (;idx2<n;idx2++) {getK(rec,idx2,l2); lkk+=l2;}
							else {
								const byte *nxt=(byte*)tp+_u16(pd+off); const uint16_t lnxt=l2=PINRef::len(nxt); int cmp;
								do {
									p2=getK(rec,idx2,l2);
									if (fPR) cmp=PINRef::cmpPIDs(p2,nxt); else if ((cmp=memcmp(p2,nxt,min(lnxt,l2)))==0) cmp=cmp3(l2,lnxt);
								} while (cmp<0 && (lkk+=l2,++idx2)<n);
							}
						}
						if (!tp->insertValues(rec,lkk,vp,off,idx,uint16_t(idx2-idx0),(uint16_t*)rec+idx0)) return RC_PAGEFULL;
					}
				}
			}
			break;
		} else if ((vp->len&~TRO_MANY)!=0) break;
	case TRO_DELETE:
		lElt=tp->info.calcEltSize(); ptr=(byte*)(tp+1)+idx*lElt; assert(idx<tp->info.nSearchKeys && newData->len==0);
		if (fSeq) {
			if (!tp->info.fmt.isFixedLenData()) {tp->info.scatteredFreeSpace+=((PagePtr*)ptr)->len; ((PagePtr*)ptr)->len=0;}
			if (idx+1!=tp->info.nEntries || tp->hasSibling()) break;
		} else if (fSub) {
			if (!tp->info.fmt.isRefKeyOnly()) {
				uint16_t *p=(uint16_t*)(tp+1)+idx; tp->getK(p,l); if (idx+1<tp->info.nEntries) memmove(p,p+1,(tp->info.nEntries-idx-1)*L_SHT);
				if (*p==tp->info.freeSpace-sizeof(TreePage)) {tp->info.freeSpace+=l; tp->info.freeSpaceLength+=l;} else tp->info.scatteredFreeSpace+=l;
			} else if (oldData->len!=0 || newData->len!=0) return RC_CORRUPTED;
			else if ((rc=tp->adjustKeyCount(idx,(byte*)(tpm+1)+2,((byte*)(tpm+1))[1],true))!=RC_OK) return rc;
			else break;
		} else if (!tp->info.fmt.isFixedLenData() && oldData->len>0 && ((vp=(PagePtr*)(ptr+tp->info.keyLength()))->len&~TRO_MANY)!=0 && (newData->offset&0x8000)==0) goto update_entry;
		else {
			if (!tp->info.fmt.isFixedLenKey() || !tp->info.fmt.isFixedLenData()) {
				if (!tp->info.fmt.isFixedLenKey()) tp->info.scatteredFreeSpace+=((PagePtr*)ptr)->len;
				if (!tp->info.fmt.isFixedLenData()) tp->info.scatteredFreeSpace+=tp->length(*(PagePtr*)(ptr+tp->info.keyLength()));
			} else if ((l=tp->info.fmt.dataLength())!=0) {
				if (idx+1<tp->info.nSearchKeys) memmove((byte*)tp+tp->info.freeSpace+l,(byte*)tp+tp->info.freeSpace,(tp->info.nSearchKeys-idx-1)*l);
				tp->info.freeSpace+=l; tp->info.freeSpaceLength+=l;
			}
			if (idx+1<tp->info.nEntries) memmove(ptr,ptr+lElt,(tp->info.nEntries-idx-1)*lElt);
		}
		--tp->info.nSearchKeys; tp->info.freeSpaceLength+=lElt;
		if (--tp->info.nEntries==0) tp->info.initFree(tp->hdr.length());
		else if (!fSeq && (idx==0 || idx==tp->info.nEntries)) {
			uint16_t newPrefSize=tp->calcPrefixSize(0,tp->info.nEntries); assert(newPrefSize>=tp->info.lPrefix);
			if (newPrefSize>tp->info.lPrefix) tp->changePrefixSize(newPrefSize);
		}
		if ((flags&TXMGR_UNDO)==0 && !tp->isLeaf()) tp->info.stamp++;
		break;
	case TRO_EDIT:
		tpe=(const TreePageEdit*)rec; oldData=&tpe->oldData; newData=&tpe->newData;
		if (tp->info.fmt.isKeyOnly() || tp->info.fmt.keyType()!=*(byte*)(tpe+1)) {
			report(MSG_ERROR,"UNDO/RECV in TreePageMgr::update EDIT: invalid key type %d, page %08X\n",*(byte*)(tpe+1),tp->hdr.pageID);
			return RC_CORRUPTED;
		}
		if ((flags&TXMGR_UNDO)!=0) {
			if ((rc=sKey.deserialize(tpe+1,lrec-sizeof(TreePageEdit)))!=RC_OK) return rc;
			if (!tp->findKey(sKey,idx)) {report(MSG_ERROR,"UNDO in TreePageMgr EDIT: invalid key, page %08X\n",tp->hdr.pageID); return RC_NOTFOUND;}
			const PagePtr *tmp=newData; newData=oldData; oldData=tmp;
		}
		assert(tp->isLeaf() && idx<tp->info.nSearchKeys);
		if ((ptr=(byte*)tp->findData(idx,l,(const PagePtr**)&vp))==NULL) {
			report(MSG_ERROR,"TreePageMgr::update EDIT: data not found, idx=%d, page %08X\n",idx,tp->hdr.pageID);
			return RC_NOTFOUND;
		}
		if (vp!=NULL && (vp->len&TRO_MANY)!=0) return RC_CORRUPTED;
		if (tpe->shift+oldData->len>l) {
			report(MSG_ERROR,"TreePageMgr::update EDIT: invalid shift/len, idx=%d, page %08X\n",idx,tp->hdr.pageID);
			return RC_CORRUPTED;
		}
		if (tp->info.fmt.isFixedLenData() && newData->len!=oldData->len) {
			report(MSG_ERROR,"TreePageMgr::update EDIT: attempt to change length for FixedLenData, idx=%d, page %08X\n",idx,tp->hdr.pageID);
			return RC_CORRUPTED;
		}
		if (newData->len<=oldData->len) {
			if (newData->len>0) memcpy(ptr+tpe->shift,rec+newData->offset,newData->len);
			if (unsigned(tpe->shift+oldData->len)<l) memmove(ptr+tpe->shift+newData->len,ptr+tpe->shift+oldData->len,l-tpe->shift-oldData->len);
			tp->info.scatteredFreeSpace+=oldData->len-newData->len; if (vp!=NULL) vp->len-=oldData->len-newData->len;
			break;
		}
		lElt=newData->len-oldData->len;
		if (vp->offset==tp->info.freeSpace && lElt<=tp->info.freeSpaceLength) {
			tp->info.freeSpace-=lElt; tp->info.freeSpaceLength-=lElt;
			if (tpe->shift>0) memmove((byte*)tp+tp->info.freeSpace,ptr,tpe->shift);
			memcpy((byte*)tp+tp->info.freeSpace+tpe->shift,rec+newData->offset,newData->len);
			vp->offset=tp->info.freeSpace; vp->len+=lElt; break;
		}
		fFree=false;
		if (l+lElt<=tp->info.freeSpaceLength) tp->info.scatteredFreeSpace+=l;
		else {
			if (lElt>tp->info.freeSpaceLength+tp->info.scatteredFreeSpace) {
				report(MSG_ERROR,"TreePageMgr::update EDIT: insufficient space, requested %d, page %08X\n",lElt,tp->hdr.pageID);
				return RC_PAGEFULL;
			}
			byte *p=new(SES_HEAP) byte[l]; if (p==NULL) return RC_NOMEM;
			memcpy(p,ptr,l); fFree=true; ptr=p; vp->len=0; tp->info.scatteredFreeSpace+=l; 
			if (tp->info.fmt.isFixedLenData()) tp->compact(false,idx); else {vp->len=0; tp->compact(false);}
			tp->findData(idx,ll,(const PagePtr**)&vp); assert(unsigned(l+lElt)<=tp->info.freeSpaceLength);
		}
		vp->len=l+lElt; vp->offset=tp->info.freeSpace-=vp->len; tp->info.freeSpaceLength-=vp->len;
		if (tpe->shift>0) memmove((byte*)tp+tp->info.freeSpace,ptr,tpe->shift);
		memcpy((byte*)tp+tp->info.freeSpace+tpe->shift,rec+newData->offset,newData->len);
		if (tpe->shift+oldData->len<l) memmove((byte*)tp+tp->info.freeSpace+tpe->shift+newData->len,ptr+tpe->shift+oldData->len,l-tpe->shift-oldData->len);
		if (fFree) free(ptr,SES_HEAP);
		break;
	case TRO_INIT:
		if ((flags&TXMGR_UNDO)!=0) break;
		tpi=(TreePageInit*)rec; tp->info.level=byte(tpi->level); tp->info.flags=0; tp->info.fmt=tpi->fmt;
		if (tpi->level>0) {
			assert(tpi->left!=INVALID_PAGEID && tpi->right!=INVALID_PAGEID);
			if ((rc=tp->prefixFromKey(tpi+1))!=RC_OK) return rc;
			tp->storeKey(tpi+1,tp+1); tp->info.freeSpaceLength-=tp->info.calcEltSize();
			if (!tp->info.fmt.isFixedLenKey()) *(PageID*)((byte*)(tp+1)+sizeof(PagePtr))=tpi->right;
			else {
				tp->info.freeSpace-=sizeof(PageID); tp->info.freeSpaceLength-=sizeof(PageID);
				*(PageID*)((byte*)tp+tp->info.freeSpace)=tpi->right;
			}
			tp->info.leftMost=tpi->left; tp->info.nSearchKeys=tp->info.nEntries=1;
#ifdef _DEBUG
			if (!tp->info.fmt.isFixedLenKey()) tp->compact(true);
#endif
		}
		break;
	case TRO_DROP:
		if ((flags&TXMGR_UNDO)!=0) {
			if (lrec<sizeof(TreePageInfo)) {
				report(MSG_ERROR,"TreePageMgr::update undo DROP: invalid length %lu, page %08X\n",lrec,tp->hdr.pageID);
				return RC_CORRUPTED;
			}
			memcpy(&tp->info,rec,sizeof(TreePageInfo));
			ll=tp->info.nEntries*tp->info.calcEltSize(); memcpy(tp+1,rec+sizeof(TreePageInfo),ll);
			memcpy((byte*)tp+tp->info.freeSpace,rec+sizeof(TreePageInfo)+ll,lrec-sizeof(TreePageInfo)-ll);
		}
		break;
	case TRO_MERGE:
		if ((flags&TXMGR_RECV)!=0) {
			tps=(const TreePageSplit*)rec; p1=(const byte*)(tps+1); l=SearchKey::keyLenP(p1);
			if (lrec<sizeof(TreePageSplit)+l+sizeof(TreePageInfo)) {
				report(MSG_ERROR,"TreePageMgr::update MERGE: cannot recover old log format, page %08X\n",tp->hdr.pageID);
				return RC_INTERNAL;
			}
			if ((tp2=(TreePage*)alloca(tp->hdr.length()))==NULL) return RC_NOMEM;
			memcpy(tp2,tp,sizeof(TreePage)); tp2->hdr.pageID=tps->newSibling;
			memcpy(&tp2->info,p1+=l,sizeof(TreePageInfo)); ll=tp2->info.nEntries*tp2->info.calcEltSize(); memcpy(tp2+1,p1+sizeof(TreePageInfo),ll);
			memcpy((byte*)tp2+tp2->info.freeSpace,p1+sizeof(TreePageInfo)+ll,lrec-sizeof(TreePageSplit)-l-sizeof(TreePageInfo)-ll);
			ctx->bufMgr->drop(tps->newSibling); flags&=~TXMGR_RECV;
		}
		flags^=TXMGR_UNDO;
	case TRO_SPLIT:
		tps=(const TreePageSplit*)rec;
		if (tp2==NULL) {
			if (newp==NULL) {
				assert((flags&(TXMGR_RECV|TXMGR_UNDO))!=0);
				newp=ctx->logMgr->setNewPage((flags&TXMGR_UNDO)==0?ctx->bufMgr->newPage(tps->newSibling,this):
														ctx->bufMgr->getPage(tps->newSibling,this,PGCTL_XLOCK));
				if (newp==NULL) return RC_NOMEM;
			}
			tp2=(TreePage*)newp->getPageBuf(); assert(newp->getPageID()==tps->newSibling);
		}
		if ((flags&TXMGR_UNDO)==0) {
			if (op==TRO_MERGE) {
				if (tps->nEntriesLeft+1!=tp->info.nEntries) {
					report(MSG_ERROR,"TreePageMgr::update MERGE: nEntries not matching, page %08X\n",tp->hdr.pageID);
					return RC_CORRUPTED;
				}
			} else if (tps->nEntriesLeft>tp->info.nSearchKeys) {
				report(MSG_ERROR,"TreePageMgr::update SPLIT: nSearchKeys not matching (%d/%d), page %08X\n",
													tps->nEntriesLeft,tp->info.nSearchKeys,tp->hdr.pageID);
				return RC_CORRUPTED;
			}
			tp2->info.fmt=tp->info.fmt;
			tp2->info.level=tp->info.level;
			tp2->info.flags=tp->info.flags;
			tp2->info.sibling=tp->info.sibling;
			if (fSeq) {
				assert(!tp->hasSibling() && tps->nEntriesLeft==tp->info.nEntries);
				tp->info.sibling=tps->newSibling; tp2->info.prefix=tp->info.prefix+tp->info.nEntries;
			} else {
				if (tps->nEntriesLeft<tp->info.nEntries) tp->copyEntries(tp2,tp->calcPrefixSize(tps->nEntriesLeft,tp->info.nEntries),tps->nEntriesLeft);
				bool fFixedKey=tp->info.fmt.isFixedLenKey(),fFixedData=tp->info.fmt.isFixedLenData();
				lElt=tp->info.calcEltSize(); tp->info.freeSpaceLength+=lElt*(tp->info.nEntries-tps->nEntriesLeft);
				if (fFixedKey && fFixedData) {
					uint16_t lFreed=tp->info.fmt.dataLength()*uint16_t(tp->info.nSearchKeys-tps->nEntriesLeft);
					tp->info.freeSpace+=lFreed; tp->info.freeSpaceLength+=lFreed;
				} else {
					byte *p=(byte*)(tp+1)+tps->nEntriesLeft*lElt;
					for (uint16_t i=tps->nEntriesLeft; i<tp->info.nEntries; ++i,p+=lElt) {
						if (fSub) {tp->getK(p,l); tp->info.scatteredFreeSpace+=l;}
						else {
							if (!fFixedKey) tp->info.scatteredFreeSpace+=((PagePtr*)p)->len;
							if (!fFixedData && i<tp->info.nSearchKeys) 
								{vp=(PagePtr*)(p+tp->info.keyLength()); tp->info.scatteredFreeSpace+=tp->length(*vp);}
						}
					}
				}
				tp->info.nSearchKeys=tp->info.nEntries=tps->nEntriesLeft; tp->info.sibling=INVALID_PAGEID;
				if (!tp->info.fmt.isNumKey()||tp->info.fmt.isPrefNumKey()) {
					if ((rc=sKey.deserialize(tps+1,lrec-sizeof(TreePageSplit)))!=RC_OK) return rc;
					uint16_t prefixSize=tp->info.nEntries!=0?tp->calcPrefixSize(sKey,0,true):0;			// pass in TreePageSplit?
					if (prefixSize!=tp->info.lPrefix) {tp->changePrefixSize(prefixSize); lElt=tp->info.calcEltSize();}
				}
				if (tp->info.scatteredFreeSpace>0) tp->compact();
				tp->storeKey(tps+1,(byte*)(tp+1)+lElt*tp->info.nEntries);
				assert(tp->info.freeSpaceLength>=lElt); tp->info.freeSpaceLength-=lElt; 
				tp->info.nEntries++; tp->info.sibling=tps->newSibling;
#ifdef _DEBUG
				if (!tp->info.fmt.isFixedLenKey()||!tp->info.fmt.isFixedLenData()) {tp->compact(true); tp2->compact(true);}
				if (tp2->info.nEntries!=0 && sKey.deserialize(tps+1,lrec-sizeof(TreePageSplit))==RC_OK) {tp2->findKey(sKey,n); assert(n==0);}
#endif
			}
			if (tp->info.level>TREE_MAX_DEPTH) {
				report(MSG_ERROR,"Invalid level %d after split(1), page %08X\n",tp->info.level,tp->hdr.pageID);
				return RC_CORRUPTED;
			}
			if (tp2->info.level>TREE_MAX_DEPTH) {
				report(MSG_ERROR,"Invalid level %d after split(2), page %08X\n",tp2->info.level,tp->hdr.pageID);
				return RC_CORRUPTED;
			}
			if (!fSeq && tp2->info.nEntries!=tp2->info.nSearchKeys+(tp2->info.sibling!=INVALID_PAGEID?1:0)) {
				report(MSG_ERROR,"Invalid nEntries/nSearchKeys %d/%d after merge/split, page %08X\n",tp2->info.nEntries,tp2->info.nSearchKeys,tp2->hdr.pageID);
				return RC_CORRUPTED;
			}
			pb->setDependency(newp);
		} else {
			if (op==TRO_MERGE && tps->nEntriesLeft!=tp->info.nEntries) {
				report(MSG_ERROR,"TreePageMgr::update MERGE: nEntries not matching, page %08X\n",tp->hdr.pageID);
				return RC_CORRUPTED;
			}
			if (fSeq) {
				assert((flags&TXMGR_UNDO)!=0 && !tp2->hasSibling() && tps->nEntriesLeft==tp->info.nEntries);
				tp->info.sibling=tp2->info.sibling;
			} else {
				bool fVarKey=!tp->info.fmt.isFixedLenKey(); assert(tp->info.nEntries>0); lElt=tp->info.calcEltSize(); tp->info.freeSpaceLength+=lElt;
				if (fVarKey) tp->info.scatteredFreeSpace+=fSub?(tp->getK(tp->info.nEntries-1,l),l):((PagePtr*)((byte*)(tp+1)+(tp->info.nEntries-1)*lElt))->len;
				--tp->info.nEntries; tp->info.sibling=INVALID_PAGEID;
				if (tp->info.lPrefix!=tps->oldPrefSize) 
					{assert(tp->info.nEntries<=1||tp->info.lPrefix>tps->oldPrefSize); tp->changePrefixSize(tps->oldPrefSize); if (!fVarKey) lElt=tp->info.calcEltSize();}
				if (tp2->info.nEntries>0) {
					if (tp2->info.lPrefix!=tps->oldPrefSize) {assert(tp2->info.lPrefix>tps->oldPrefSize); tp2->changePrefixSize(tps->oldPrefSize);}
					if (tp->info.scatteredFreeSpace>0) tp->compact();
					if (unsigned(tp->info.freeSpaceLength+tp2->info.freeSpaceLength+tp2->info.scatteredFreeSpace)<xSize) {
						report(MSG_ERROR,"TreePageMgr::update MERGE: not enough space, page %08X\n",tp->hdr.pageID);
						return RC_PAGEFULL;
					}
					if (tp->info.nEntries==0) {
						if (fVarKey && tp->info.lPrefix!=0) {tp->info.freeSpaceLength+=tp->info.lPrefix; tp->info.lPrefix=0;}
						if (tp2->info.lPrefix!=0) {
							tp->info.lPrefix=tp2->info.lPrefix;
							if (tp->info.fmt.isFixedLenKey()) tp->info.prefix=tp2->info.prefix;
							else tp->store((byte*)tp2+((PagePtr*)&tp2->info.prefix)->offset,((PagePtr*)&tp2->info.prefix)->len,*(PagePtr*)&tp->info.prefix);
						}
					}
					l=lElt*tp2->info.nEntries; if (l>tp->info.freeSpaceLength) return RC_CORRUPTED; tp->info.freeSpaceLength-=l;
					if (fSub) for (uint16_t i=0; i<tp2->info.nEntries; ++i) {
						ptr=(byte*)tp2->getK(i,l); ((uint16_t*)(tp+1))[i+tp->info.nEntries]=tp->store(ptr,l);
					} else {
						bool fVarData=!tp->info.fmt.isFixedLenData(); if (!fVarKey || !fVarData) memcpy((byte*)(tp+1)+lElt*tp->info.nEntries,tp2+1,l);
						if (!fVarKey && !fVarData) {
							l=uint16_t(tp2->info.fmt.dataLength()*tp2->info.nSearchKeys);
							tp->info.freeSpace-=l; tp->info.freeSpaceLength-=l;
							memcpy((byte*)tp+tp->info.freeSpace,(byte*)tp2+tp->info.freeSpace,l);
						} else {
							ptr=(byte*)(tp+1)+tp->info.nEntries*lElt; byte *ptr2=(byte*)(tp2+1); unsigned sht=tp2->info.keyLength();
							for (uint16_t i=0; i<tp2->info.nEntries; ++i,ptr+=lElt,ptr2+=lElt) {
								if (fVarKey) tp->store((byte*)tp2+((PagePtr*)ptr2)->offset,((PagePtr*)ptr2)->len,*(PagePtr*)ptr); 
								if (fVarData && i<tp2->info.nSearchKeys) {
									tp->store((byte*)tp2+((PagePtr*)(ptr2+sht))->offset,((PagePtr*)(ptr2+sht))->len,*(PagePtr*)(ptr+sht));
									if ((((PagePtr*)(ptr2+sht))->len&TRO_MANY)!=0) tp->moveMulti(tp2,*(PagePtr*)(ptr2+sht),*(PagePtr*)(ptr+sht));
								}
							}
						}
					}
					tp->info.nEntries+=tp2->info.nEntries; tp->info.nSearchKeys+=tp2->info.nSearchKeys;
				} else if (tp->info.nEntries==0 && tp->info.lPrefix>0) {
					if (!tp->info.fmt.isFixedLenKey()) tp->info.scatteredFreeSpace+=tp->info.lPrefix; 
					tp->info.lPrefix=0;
				}
				tp->info.sibling=tp2->info.sibling;
#ifdef _DEBUG
				if (!tp->info.fmt.isFixedLenKey()||!tp->info.fmt.isFixedLenData()) tp->compact(true);
#endif
			}
			if (tp->info.level>TREE_MAX_DEPTH) {
				report(MSG_ERROR,"Invalid level %d after merge, page %08X\n",tp->info.level,tp->hdr.pageID);
				return RC_CORRUPTED;
			}
			// unset dependency???
			if (op==TRO_SPLIT && (flags&TXMGR_UNDO)!=0) {newp->release(PGCTL_DISCARD|QMGR_UFORCE); ctx->logMgr->setNewPage(NULL);}
		}
		if (!fSeq && tp->info.nEntries!=tp->info.nSearchKeys+(tp->info.sibling!=INVALID_PAGEID?1:0)) {
			report(MSG_ERROR,"Invalid nEntries/nSearchKeys %d/%d after merge/split, page %08X\n",tp->info.nEntries,tp->info.nSearchKeys,tp->hdr.pageID);
			return RC_CORRUPTED;
		}
		return RC_OK;
	case TRO_SPAWN:
		tpa=(const TreePageSpawn*)rec; assert(!tp->info.fmt.isFixedLenData() && !tp->info.fmt.isKeyOnly());
		if ((flags&TXMGR_UNDO)!=0) {
			if ((rc=sKey.deserialize(tpa+1,lrec))!=RC_OK) return rc;
			if (!tp->findKey(sKey,idx)) {report(MSG_ERROR,"UNDO in TreePageMgr SPAWN: invalid key, page %08X\n",tp->hdr.pageID); return RC_NOTFOUND;}
		}
		assert(idx<tp->info.nSearchKeys);
		if ((ptr=(byte*)tp->findData(idx,l,(const PagePtr**)&vp))==NULL) {
			report(MSG_ERROR,"TreePageMgr::update: invalid idx %d in SPAWN, page %08X\n",idx,tp->hdr.pageID);
			return RC_CORRUPTED;
		}
		assert(vp!=NULL);
		if (newp==NULL) {
			assert((flags&(TXMGR_RECV|TXMGR_UNDO))!=0);
			if ((newp=ctx->logMgr->setNewPage(ctx->bufMgr->newPage(tpa->root,this)))==NULL) return RC_NOMEM;
		}
		assert(newp->getPageID()==tpa->root);
		tp2=(TreePage*)newp->getPageBuf(); tp2->info.fmt=tpa->fmt;
		if (tp->isSubTree(*vp)) {
			l=vp->len&~TRO_MANY;
			assert(l>=sizeof(SubTreePage) && (l-sizeof(SubTreePage))%sizeof(SubTreePageKey)==0);
			memcpy(&tpExt,(byte*)tp+vp->offset,sizeof(SubTreePage));
			tp2->info.level=byte(++tpExt.level); tp2->info.leftMost=tpExt.leftMost; tp2->info.flags=0;
			tpExt.leftMost=newp->getPageID(); tp->info.scatteredFreeSpace+=l-sizeof(SubTreePage);
			n=tp2->info.nSearchKeys=(l-sizeof(SubTreePage))/sizeof(SubTreePageKey);
			SubTreePageKey tpk; memcpy(&tpk,(byte*)tp+vp->offset+sizeof(SubTreePage),sizeof(SubTreePageKey));
			const byte *p1=(byte*)tp+tpk.key.offset,*p0=p1; l=tpk.key.len;
			memcpy(&tpk,(byte*)tp+vp->offset+(vp->len&~TRO_MANY)-sizeof(SubTreePageKey),sizeof(SubTreePageKey));
			byte *p2=(byte*)tp+tpk.key.offset; if (tpk.key.len<l) l=tpk.key.len;
			//for (; tp2->info.lPrefix<l; ++tp2->info.lPrefix) if (*p1++!=*p2++) break;					// no prefix (see calcPrefixSize())
			if (tp2->info.lPrefix>0) tp2->store(p0,tp2->info.lPrefix,*(PagePtr*)&tp2->info.prefix);
			p1=(byte*)tp+vp->offset+sizeof(SubTreePage); p2=(byte*)(tp2+1);
			for (unsigned i=0; i<n; ++i,p1+=sizeof(SubTreePageKey)) {
				memcpy(&tpk,p1,sizeof(SubTreePageKey)); tp->info.scatteredFreeSpace+=tpk.key.len;
				assert(tp2->info.freeSpaceLength>=sizeof(PagePtr)+sizeof(PageID));
				tp2->store((byte*)tp+tpk.key.offset+tp2->info.lPrefix,tpk.key.len-tp2->info.lPrefix,*(PagePtr*)p2);
				*(PageID*)(p2+sizeof(PagePtr))=tpk.pageID; p2+=sizeof(PageID);
				tp2->info.freeSpaceLength-=sizeof(PagePtr)+sizeof(PageID); p2+=sizeof(PagePtr);
			}
		} else {
			tpExt.fSubPage=0xFFFF; tpExt.level=0; tpExt.anchor=tpExt.leftMost=tpa->root;
			if ((vp->len&TRO_MANY)==0) {
				tpExt.counter=tp2->info.nSearchKeys=1; tp2->info.freeSpaceLength-=vp->len+L_SHT;
				*(uint16_t*)(tp2+1)=tp2->store((byte*)tp+vp->offset,vp->len);
			} else {
				const byte *p1=(byte*)tp+vp->offset; uint16_t *p2=(uint16_t*)(tp2+1); l=vp->len&~TRO_MANY;
				tpExt.counter=tp2->info.nSearchKeys=l/L_SHT; tp2->info.freeSpaceLength-=l;
				for (uint16_t i=0; i<tp2->info.nSearchKeys; i++,p1+=L_SHT) {
					uint16_t sht=_u16(p1); assert(sht!=0);
					*p2++=tp2->store((byte*)tp+sht,PINRef::len((byte*)tp+sht));
				}
			}
			tp->info.scatteredFreeSpace+=tp->length(*vp); vp->len=0;
			if (tp->info.freeSpaceLength<sizeof(SubTreePage)) {tp->compact(); assert(tp->info.freeSpaceLength>=sizeof(SubTreePage));}
			vp->offset=tp->info.freeSpace-=sizeof(SubTreePage); tp->info.freeSpaceLength-=sizeof(SubTreePage);
		}
		tp2->info.nEntries=(uint16_t)tp2->info.nSearchKeys;
		memcpy((byte*)tp+vp->offset,&tpExt,sizeof(SubTreePage));
		vp->len=sizeof(SubTreePage)|TRO_MANY;
		if (tp->info.level>TREE_MAX_DEPTH) {
			report(MSG_ERROR,"Invalid level %d after spawn(1), page %08X\n",tp->info.level,tp->hdr.pageID);
			return RC_CORRUPTED;
		}
		if (tp2->info.level>TREE_MAX_DEPTH) {
			report(MSG_ERROR,"Invalid level %d after spawn(2), page %08X\n",tp2->info.level,tp->hdr.pageID);
			return RC_CORRUPTED;
		}
#ifdef _DEBUG
		tp->compact(true); if (!tp2->info.fmt.isFixedLenData()) tp2->compact(true);
#endif
		if (!fSeq && tp->info.nEntries!=tp->info.nSearchKeys+(tp->info.sibling!=INVALID_PAGEID?1:0)) {
			report(MSG_ERROR,"Invalid nEntries/nSearchKeys %d/%d after spawn, page %08X\n",tp->info.nEntries,tp->info.nSearchKeys,tp->hdr.pageID);
			return RC_CORRUPTED;
		}
		pb->setDependency(newp); return RC_OK;
	case TRO_ABSORB:
		tpa=(const TreePageSpawn*)rec; assert(!tp->info.fmt.isFixedLenData() && !tp->info.fmt.isKeyOnly());
		if ((flags&TXMGR_UNDO)!=0) {
			if ((rc=sKey.deserialize(tpa+1,lrec))!=RC_OK) return rc;
			if (!tp->findKey(sKey,idx)) {report(MSG_ERROR,"UNDO in TreePageMgr ABSORB: invalid key, page %08X\n",tp->hdr.pageID); return RC_NOTFOUND;}
		}
		assert(idx<tp->info.nSearchKeys);
		if ((ptr=(byte*)tp->findData(idx,l,(const PagePtr**)&vp))==NULL) {
			report(MSG_ERROR,"TreePageMgr::update: invalid idx %d in ABSORB, page %08X\n",idx,tp->hdr.pageID);
			return RC_CORRUPTED;
		}
		assert(vp!=NULL);
		if (!tp->isSubTree(*vp)) {
			// report
			break;
		}
		l=vp->len&~TRO_MANY; assert(l>=sizeof(SubTreePage));
		memcpy(&tpExt,(byte*)tp+vp->offset,sizeof(SubTreePage));
		if (l>sizeof(SubTreePage)||tpExt.leftMost!=tpa->root) {
			// report error
			return RC_CORRUPTED;
		}
		if (newp==NULL) {
			assert((flags&(TXMGR_RECV|TXMGR_UNDO))!=0);
			if ((newp=ctx->logMgr->setNewPage(ctx->bufMgr->newPage(tpa->root,this)/*,true*/))==NULL) return RC_NOMEM;
		}

		tp2=(TreePage*)newp->getPageBuf();
		if (tp2->info.level==0) {
//...................
		} else {
//...................
		}
		return RC_OK;
	case TRO_ADDROOT:
		tpr=(const TreePageRoot*)rec; assert(!tp->info.fmt.isFixedLenData() && !tp->info.fmt.isKeyOnly());
		if ((flags&TXMGR_UNDO)!=0) {
			if ((rc=sKey.deserialize(tpr+1,lrec))!=RC_OK) return rc;
			if (!tp->findKey(sKey,idx)) {report(MSG_ERROR,"UNDO in TreePageMgr ADDROOT: invalid key, page %08X\n",tp->hdr.pageID); return RC_NOTFOUND;}
		}
		assert(idx<tp->info.nSearchKeys);
		if ((ptr=(byte*)tp->findData(idx,l,(const PagePtr**)&vp))==NULL) {
			report(MSG_ERROR,"TreePageMgr::update: invalid idx %d in ADDROOT, page %08X\n",idx,tp->hdr.pageID);
			return RC_CORRUPTED;
		}
		assert(vp!=NULL && ptr==(byte*)tp+vp->offset);
		if (!tp->isSubTree(*vp)) {
			report(MSG_ERROR,"TreePageMgr::update: not a secondary tree in ADDROOT, idx %d, page %08X\n",idx,tp->hdr.pageID);
			break;
		}
		l=vp->len&~TRO_MANY;
		if ((tpk=tp->findExtKey(rec+tpr->pkey.offset,tpr->pkey.len,(SubTreePageKey*)((byte*)tp+vp->offset+sizeof(SubTreePage)),
															(l-sizeof(SubTreePage))/sizeof(SubTreePageKey),&off))!=NULL) {
			PageID pid; memcpy(&pid,&tpk->pageID,sizeof(PageID)); if (pid==tpr->pageID) break;
			report(MSG_ERROR,"TreePageMgr::update: key already exists in ADDROOT, idx %d, page %08X\n",idx,tp->hdr.pageID);
			return RC_ALREADYEXISTS;
		}
		if (unsigned(tp->info.freeSpaceLength+tp->info.scatteredFreeSpace)<tpr->pkey.len+sizeof(SubTreePageKey)) {
			report(MSG_ERROR,"TreePageMgr::update: not enough space in ADDROOT, requested: %d, available %d, idx %d, page %08X\n",
				int(tpr->pkey.len+sizeof(SubTreePageKey)),tp->info.freeSpaceLength+tp->info.scatteredFreeSpace,idx,tp->hdr.pageID);
			return RC_PAGEFULL;
		}
		if (vp->offset==tp->info.freeSpace && tp->info.freeSpaceLength>=sizeof(SubTreePageKey)) {
			tp->info.freeSpace-=sizeof(SubTreePageKey); tp->info.freeSpaceLength-=sizeof(SubTreePageKey);
			memmove((byte*)tp+tp->info.freeSpace,ptr,l); 
			ptr=(byte*)tp+(vp->offset=tp->info.freeSpace); vp->len+=sizeof(SubTreePageKey);
		} else if (tp->info.freeSpaceLength>=l+sizeof(SubTreePageKey)) {
			tp->info.freeSpace-=l+sizeof(SubTreePageKey); tp->info.freeSpaceLength-=l+sizeof(SubTreePageKey);
			memmove((byte*)tp+tp->info.freeSpace,ptr,l); tp->info.scatteredFreeSpace+=l; 
			ptr=(byte*)tp+(vp->offset=tp->info.freeSpace); vp->len+=sizeof(SubTreePageKey);
		} else {
			tp->compact(false,idx); ptr=(byte*)tp->findData(idx,ll,(const PagePtr**)&vp); assert(ptr!=NULL && vp!=NULL);
		}
		ptr+=off+=sizeof(SubTreePage); if (off<l) memmove(ptr+sizeof(SubTreePageKey),ptr,l-off);
		if (tp->info.freeSpaceLength<tpr->pkey.len) {
			memset(ptr,0,sizeof(SubTreePageKey)); tp->compact(); ptr=(byte*)tp->findData(idx,ll,(const PagePtr**)&vp)+off;
			assert((size_t)ptr>(size_t)off && vp!=NULL && tp->info.freeSpaceLength>=tpr->pkey.len);
		}
		tp->store(rec+tpr->pkey.offset,tpr->pkey.len,pp);
		__una_set(((SubTreePageKey*)ptr)->key,pp); __una_set(((SubTreePageKey*)ptr)->pageID,tpr->pageID);
		break;
	case TRO_DELROOT:
		tpr=(const TreePageRoot*)rec; assert(!tp->info.fmt.isFixedLenData() && !tp->info.fmt.isKeyOnly());
		if ((flags&TXMGR_UNDO)!=0) {
			if ((rc=sKey.deserialize(tpr+1,lrec))!=RC_OK) return rc;
			if (!tp->findKey(sKey,idx)) {report(MSG_ERROR,"UNDO in TreePageMgr DELROOT: invalid key, page %08X\n",tp->hdr.pageID); return RC_NOTFOUND;}
		}
		assert(idx<tp->info.nSearchKeys);
		if ((ptr=(byte*)tp->findData(idx,l,(const PagePtr**)&vp))==NULL) {
			report(MSG_ERROR,"TreePageMgr::update: invalid idx %d in DELROOT, page %08X\n",idx,tp->hdr.pageID);
			return RC_CORRUPTED;
		}
		assert(vp!=NULL);
		if (!tp->isSubTree(*vp)) {
			report(MSG_ERROR,"TreePageMgr::update: not a secondary tree in DELROOT, idx %d, page %08X\n",idx,tp->hdr.pageID);
			break;
		}
		l=(vp->len&~TRO_MANY)-sizeof(SubTreePage);
		if ((tpk=tp->findExtKey(rec+tpr->pkey.offset,tpr->pkey.len,(SubTreePageKey*)((byte*)tp+vp->offset+sizeof(SubTreePage)),
																					l/sizeof(SubTreePageKey),&off))!=NULL) {
			SubTreePageKey t; memcpy(&t,tpk,sizeof(SubTreePageKey));
			if (t.key.offset!=tp->info.freeSpace) tp->info.scatteredFreeSpace+=t.key.len;
			else {tp->info.freeSpace+=t.key.len; tp->info.freeSpaceLength+=t.key.len;}
			vp->len-=sizeof(SubTreePageKey); tp->info.scatteredFreeSpace+=sizeof(SubTreePageKey);
			if (off<l-sizeof(SubTreePageKey))
				{ptr+=sizeof(SubTreePage)+off; memmove(ptr,ptr+sizeof(SubTreePageKey),l-off-sizeof(SubTreePageKey));}
		}
		break;
	case TRO_MULTI:
		assert((flags&TXMGR_UNDO)==0); if (lrec<=sizeof(TreePageMulti)) return RC_CORRUPTED;
		tpmi=(TreePageMulti*)rec; rec+=sizeof(TreePageMulti);
		switch (idx) {
		default: return RC_CORRUPTED;
		case MO_INSERT:
			if (!tp->info.fmt.isKeyOnly()) return RC_CORRUPTED; 
			if (tp->info.sibling!=INVALID_PAGEID) {
				if (tp->info.nEntries!=tp->info.nSearchKeys+1) return RC_CORRUPTED;
				if (tpmi->fLastR!=0) {
					const byte *p=tp->getK(tp->info.nEntries-1,l);
					if (p==(byte*)tp+tp->info.freeSpace) {tp->info.freeSpace+=l; tp->info.freeSpaceLength+=l;} else tp->info.scatteredFreeSpace+=l;
					tp->info.nEntries--; tp->info.sibling=INVALID_PAGEID;
				}
			}
			idx1=idx2=0; l1=lElt=L_SHT; n=tpmi->nKeys;
			do {
				p1=getK(rec,idx1,l1);
				if (tp->findSubKey(p1,l1,idx2)) {
					if (!tp->info.fmt.isRefKeyOnly()) return RC_ALREADYEXISTS;
					if ((rc=tp->adjustKeyCount(idx2,p1,l1))!=RC_OK) return rc;
					++idx2; ++idx1;
				} else {
					idx=idx1;
					if (idx2>=tp->info.nSearchKeys) idx1=n; else tp->findSubKey(rec,n,idx2,idx1);
					uint16_t nIns=uint16_t(idx1-idx);
					if (nIns*L_SHT>tp->info.freeSpaceLength) return RC_PAGEFULL; tp->info.freeSpaceLength-=nIns*L_SHT;
					if (idx2<tp->info.nEntries) memmove((uint16_t*)(tp+1)+idx2+nIns,(uint16_t*)(tp+1)+idx2,(tp->info.nEntries-idx2)*L_SHT);
					for (; idx<idx1; idx++) {
						const byte *p=TreePageMgr::getK(rec,idx,l1); if (l1>tp->info.freeSpaceLength) return RC_PAGEFULL;	// undo?
						((uint16_t*)(tp+1))[idx2++]=tp->store(p,l1);
					}
					tp->info.nSearchKeys+=nIns; tp->info.nEntries+=nIns;
				}
			} while (idx1<n);
			if ((tp->info.sibling=tpmi->sibling)!=INVALID_PAGEID) tp->info.nSearchKeys=tp->info.nEntries-1;
			break;
		case MO_DELETE:
			if (!tp->info.fmt.isKeyOnly()) return RC_CORRUPTED;
			idx1=idx2=0; l1=l2=L_SHT; fPR=tp->info.fmt.isRefKeyOnly(); n=tpmi->nKeys;
			p1=getK(rec,idx1,l1);
			do {
				if (idx2>=tp->info.nSearchKeys || !tp->findSubKey(p1,l1,idx2)) return RC_NOTFOUND;
				bool fAdjcnt=false; idx=idx2;
				do {
					if (tp->info.fmt.isRefKeyOnly()) {
						//unsigned cnt2=PINRef::getCount(p1,l1,rec+tpmi->lData,tpmi->lPrefix);
						// if (cnt1<cnt2) return RC_NOTFOUND; if (cnt1>cnt2) {fAdjcnt=true; break;}
					}
					if (++idx1>=n || ++idx2>=tp->info.nSearchKeys) break;
					p1=getK(rec,idx1,l1); p2=tp->getK(idx2,l2);
				} while (fPR && PINRef::cmpPIDs(p1,p2)==0 || !fPR && memcmp(p1,p2,l1)==0);
				if (idx2>idx) {
					for (unsigned i=idx; i<idx2; i++)
						if (tp->getK(i,l2)==(byte*)tp+tp->info.freeSpace) {tp->info.freeSpace+=l2; tp->info.freeSpaceLength+=l2;} else tp->info.scatteredFreeSpace+=l2;
					if (idx2<tp->info.nEntries) memmove((uint16_t*)(tp+1)+idx,(uint16_t*)(tp+1)+idx2,(tp->info.nEntries-idx2)*L_SHT);
					l=uint16_t((idx2-idx)*L_SHT); tp->info.nEntries-=uint16_t(idx2-idx); tp->info.nSearchKeys-=uint16_t(idx2-idx); tp->info.freeSpaceLength+=l;
				}
				if (fAdjcnt) {
					//if ((rc=tp->adjustKeyCount(idx,(byte*)(tpm+1)+2,((byte*)(tpm+1))[1],NULL,0,true))!=RC_OK) return rc;
					idx1++; idx2++;
				}
			} while (idx1<n);
			break;
		case MO_INIT:
			tp->info.fmt=tpmi->fmt; tp->info.sibling=tpmi->sibling;
			if (tp->info.freeSpaceLength<tpmi->lData) {report(MSG_ERROR,"Not enough space in MULTIINIT, requested: %d\n",tpmi->lData); return RC_PAGEFULL;}
			tp->info.nEntries=tp->info.nSearchKeys=tpmi->nKeys; if (tp->info.sibling!=INVALID_PAGEID) tp->info.nSearchKeys--;
			if (!tp->info.fmt.isKeyOnly()) memcpy(tp+1,rec,tpmi->lData);	//????
			else {
				memcpy(tp+1,rec,tpmi->nKeys*L_SHT); l=tpmi->lData-tpmi->nKeys*L_SHT; tp->info.freeSpace-=l; memcpy((byte*)tp+tp->info.freeSpace,rec+tpmi->nKeys*L_SHT,l);
				uint16_t dl=tp->info.freeSpaceLength-tpmi->lData; if (dl!=0) for (uint16_t i=0; i<tpmi->nKeys; i++) ((uint16_t*)(tp+1))[i]+=dl;
			}
			tp->info.freeSpaceLength-=tpmi->lData; break;
		case MO_PAGEINIT:
			if ((flags&TXMGR_RECV)!=0) {
				tp->info.fmt=tpmi->fmt; tp->info.sibling=tpmi->sibling;
				if (tp->info.freeSpaceLength<tpmi->lData) {report(MSG_ERROR,"Not enough space in MULTIPAGEINIT, requested: %d\n",tpmi->lData); return RC_PAGEFULL;}
				tp->info.nEntries=tp->info.nSearchKeys=tpmi->nKeys; if (tp->info.sibling!=INVALID_PAGEID) tp->info.nSearchKeys--;
				memcpy(tp+1,rec,tpmi->lData); tp->info.freeSpaceLength-=tpmi->lData; tp->info.freeSpace-=tpmi->lData-tpmi->nKeys*L_SHT;
			}
			break;
		}
		break;
	case TRO_COUNTER:
		if (tp->info.fmt.isKeyOnly()) return RC_CORRUPTED;
		if ((flags&TXMGR_UNDO)!=0) {
			if ((rc=sKey.deserialize(rec+sizeof(int64_t),lrec-sizeof(int64_t)))!=RC_OK) return rc;
			if (!tp->findKey(sKey,idx)) {report(MSG_ERROR,"UNDO in TreePageMgr COUNTER: invalid key, page %08X\n",tp->hdr.pageID); return RC_NOTFOUND;}
		}
		if ((ptr=(byte*)tp->findData(idx,l,(const PagePtr**)&vp))==NULL) {
			report(MSG_ERROR,"TreePageMgr::update COUNTER: data not found, idx=%d, page %08X\n",idx,tp->hdr.pageID);
			return RC_NOTFOUND;
		}
		if (vp!=NULL && tp->isSubTree(*vp)) {
			memcpy(&tpExt,(byte*)tp+vp->offset,sizeof(SubTreePage)); int64_t d=__una_get(*(int64_t*)rec);
			if ((flags&TXMGR_UNDO)!=0) tpExt.counter=tpExt.counter-d; else tpExt.counter=tpExt.counter+d;
			memcpy((byte*)tp+vp->offset,&tpExt,sizeof(SubTreePage));
		}
		break;
	case TRO_REPLACE:
		if (!tp->info.fmt.isKeyOnly()) {
			report(MSG_ERROR,"TreePageMgr::update replace: incorrect page type, page %08X\n",tp->hdr.pageID);
			return RC_CORRUPTED;
		}
		p1=rec+sizeof(uint16_t); l1=*(uint16_t*)rec; if (lrec<l1+sizeof(uint16_t)) return RC_CORRUPTED;
		l2=uint16_t(lrec-l1-sizeof(uint16_t)); if (rec[lrec-1]==0xFF) {if (l2<rec[lrec-3]) return RC_CORRUPTED; l2-=rec[lrec-3];}
		if ((delta=l2-l1)>(int)tp->info.freeSpaceLength+tp->info.scatteredFreeSpace) {
			report(MSG_ERROR,"TreePageMgr::update replace: insufficient space, req=%d, page %08X\n",l2-l1,tp->hdr.pageID);
			return RC_PAGEFULL;
		}
		p2=p1+l1;
		if ((flags&(TXMGR_UNDO|TXMGR_RECV))!=0) {
			if ((flags&TXMGR_UNDO)!=0) {const byte *pp=p1; p1=p2; p2=pp; ll=l1; l1=l2; l2=ll; delta=-delta;}
			if (!tp->findSubKey(p1,l1,idx)) {
				report(MSG_ERROR,"TreePageMgr::update replace UNDO/REDO: key not found, page %08X\n",tp->hdr.pageID);
				return RC_NOTFOUND;
			}
		}
#ifdef _DEBUG
		else if (!tp->findSubKey(p1,l1,n)) {
			report(MSG_ERROR,"TreePageMgr::update replace: key not found or invalid, page %08X\n",tp->hdr.pageID);
			return RC_NOTFOUND;
		} else if (n!=idx) {
			report(MSG_ERROR,"TreePageMgr::update replace: invalid position %d != %d, page %08X\n",n,idx,tp->hdr.pageID);
			return RC_NOTFOUND;
		}
#endif
		icmp=tp->info.fmt.isRefKeyOnly()?PINRef::cmpPIDs(p2,p1):(icmp=memcmp(p2,p1,min(l2,l1)))==0?cmp3(l2,l1):icmp;
		if (icmp<0 && idx==0 || icmp>0 && idx+1>=tp->info.nSearchKeys) n=idx;
		else if (tp->findSubKey(p2,l2,n) && n!=idx) {
			report(MSG_ERROR,"TreePageMgr::update replace: key already exists, page %08X\n",ll,tp->hdr.pageID);
			return RC_ALREADYEXISTS;
		}
		lElt=L_SHT; off=((uint16_t*)(tp+1))[idx];
		if (n<idx) memmove((byte*)(tp+1)+(n+1)*lElt,(byte*)(tp+1)+n*lElt,(idx-n)*lElt);
		else if (n>idx+1) memmove((byte*)(tp+1)+idx*lElt,(byte*)(tp+1)+(idx+1)*lElt,(--n-idx)*lElt);
		if (delta<0) {
			delta=-delta;
			if (off!=tp->info.freeSpace-sizeof(TreePage)) {tp->info.scatteredFreeSpace+=uint16_t(delta); off+=sizeof(TreePage);}
			else {off=tp->info.freeSpace+=uint16_t(delta); tp->info.freeSpaceLength+=uint16_t(delta);}
		} else if (off+sizeof(TreePage)==tp->info.freeSpace && tp->info.freeSpaceLength>=delta) {off=tp->info.freeSpace-=uint16_t(delta); tp->info.freeSpaceLength-=uint16_t(delta);}
		else if (tp->info.freeSpaceLength>=l2) {off=tp->info.freeSpace-=l2; tp->info.freeSpaceLength-=l2; tp->info.scatteredFreeSpace+=l1;}
		else {((uint16_t*)(tp+1))[n]=0; tp->info.scatteredFreeSpace+=l1; tp->compact(false); assert(tp->info.freeSpaceLength>=l2); off=tp->info.freeSpace-=l2; tp->info.freeSpaceLength-=l2;}
		((uint16_t*)(tp+1))[n]=off-sizeof(TreePage); memcpy((byte*)tp+off,p2,l2); break;
	}
	if (tp->info.level>TREE_MAX_DEPTH || tp->info.nEntries!=tp->info.nSearchKeys+(tp->info.sibling!=INVALID_PAGEID?1:0)) {
		report(MSG_ERROR,"Invalid nEntries/nSearchKeys %d/%d after op %d, page %08X\n",tp->info.nEntries,tp->info.nSearchKeys,op,tp->hdr.pageID);
		return RC_CORRUPTED;
	}
#ifdef _DEBUG
	assert(!tp->info.fmt.isPrefNumKey()||tp->info.lPrefix==0||(tp->info.prefix&(1ULL<<(sizeof(uint64_t)-tp->info.lPrefix)*8)-1)==0);
	if (!tp->info.fmt.isFixedLenKey()||!tp->info.fmt.isFixedLenData()) tp->compact(true);
	tp->checkKeys();
#endif
	return RC_OK;
}

RC TreePageMgr::TreePage::insertKey(unsigned idx,const byte *key,const byte *data,uint16_t lData,uint16_t nData,uint16_t l)
{
	uint16_t lElt=info.calcEltSize(); l+=lElt;
	if (info.freeSpaceLength<l) {
		if (info.freeSpaceLength+info.scatteredFreeSpace<l) {
			report(MSG_ERROR,"TreePageMgr::insertKey: requested %d, available %d, page %08X\n",l,info.freeSpaceLength+info.scatteredFreeSpace,hdr.pageID);
			return RC_PAGEFULL;
		}
		compact(); assert(info.freeSpaceLength>=l);
	}
	byte *ptr=(byte*)(this+1)+idx*lElt;
	if (info.fmt.isSeq()) {
		assert(!hasSibling() && idx==info.nEntries); info.freeSpaceLength-=lElt;
		if (!info.fmt.isFixedLenData()) store(data,lData,*(PagePtr*)ptr); else {assert(lData==lElt); memcpy(ptr,data,lElt);}
	} else { 
		if (idx<info.nEntries) memmove(ptr+lElt,ptr,(info.nEntries-idx)*lElt);
		storeKey(key,ptr); info.freeSpaceLength-=lElt;
		if (!info.fmt.isKeyOnly()) {
			if (!info.fmt.isFixedLenData()) {
				store(data,lData,*(PagePtr*)(ptr+info.keyLength()));
				if (nData!=0) {
					PagePtr *vp=(PagePtr*)(ptr+info.keyLength());
					if ((nData&0x8000)==0) {
						if (nData*L_SHT>=lData) return RC_CORRUPTED;
						uint16_t *p=(uint16_t*)((byte*)this+vp->offset); vp->len=nData*L_SHT|TRO_MANY;
						for (uint16_t i=0; i<nData; i++,p++) {uint16_t sht=__una_get(*p); sht+=vp->offset; __una_set(*p,sht);}
					} else if (lData>sizeof(SubTreePage)) {
						vp->len=(sizeof(SubTreePage)+(nData&~0x8000)*sizeof(SubTreePageKey))|TRO_MANY;
						SubTreePageKey *pk=(SubTreePageKey*)((byte*)this+vp->offset+sizeof(SubTreePage));
						for	(int i=(nData&~0x8000); --i>=0; pk++)
							{uint16_t pp=_u16(&pk->key.offset); pp+=vp->offset; _set_u16(&pk->key.offset,pp);}
					} else if (lData==sizeof(SubTreePage)) vp->len|=TRO_MANY; else return RC_CORRUPTED;
				}
			} else if ((l=info.fmt.dataLength())!=0) {
				assert(lData==l);
				if (!info.fmt.isFixedLenKey()) memcpy(ptr+sizeof(PagePtr),data,l);
				else {
					assert(info.freeSpaceLength>=l);
					info.freeSpaceLength-=l; info.freeSpace-=l;
					ptr=(byte*)this+info.freeSpace+(info.nSearchKeys-idx)*l;
					if (idx<info.nSearchKeys) memmove((byte*)this+info.freeSpace,(byte*)this+info.freeSpace+l,ptr-(byte*)this-info.freeSpace);
					if (!isLeaf()) *(PageID*)ptr=*(PageID*)(data); else memcpy(ptr,data,l);
				}
			}
		}
	}
	info.nEntries++; info.nSearchKeys++;	
	return RC_OK;
}

bool TreePageMgr::TreePage::insertValues(const byte *p1,uint16_t ll,AfyKernel::PagePtr *vp,uint16_t off,unsigned idx,uint16_t n,const uint16_t *shs)
{
	byte *ptr=NULL; uint16_t n2=n*L_SHT; assert(n==1 || shs!=NULL); assert(!isSubTree(*vp));
	uint16_t l=ll,l2,len=vp->len&~TRO_MANY,oof=vp->offset; const bool fSingle=(vp->len&TRO_MANY)==0;
	if (len!=0 || n>1) {l+=n2; if (fSingle) l+=L_SHT;}
	if (l>info.freeSpaceLength+info.scatteredFreeSpace) {
		report(MSG_ERROR,"TreePageMgr insertValues: insufficient space, requested %d, page %08X\n",l,hdr.pageID); return false;
	}
	if (fSingle) {
		if (info.freeSpaceLength<l) {compact(false); findData(idx,l2,(const PagePtr**)&vp); assert(vp!=NULL && info.freeSpaceLength>=l); oof=vp->offset;}
		ptr=(byte*)this+(vp->offset=info.freeSpace-=l); info.freeSpaceLength-=l;
		if (len==0 && shs==NULL) {vp->len=ll; memcpy(ptr,p1,ll); return true;}
		if (off!=0) {assert(off==L_SHT); _set_u16(ptr,oof);} else _set_u16(ptr+n2,oof); vp->len=L_SHT|TRO_MANY; len=L_SHT;
	} else {
		if (oof==info.freeSpace && info.freeSpaceLength>=l) {
			memmove(ptr=(byte*)this+(vp->offset=info.freeSpace-=l),(byte*)this+oof,len); info.freeSpaceLength-=l;
		} else if (info.freeSpaceLength>=len+l) {
			info.freeSpaceLength-=len+l; info.scatteredFreeSpace+=len;
			memmove(ptr=(byte*)this+(vp->offset=info.freeSpace-=len+l),(byte*)this+oof,len);
		} else {
			compact(false,idx,l); ptr=(byte*)findData(idx,l2,(const PagePtr**)&vp);
			assert(ptr!=NULL && vp!=NULL && (vp->len&~TRO_MANY)>=l); oof=vp->offset; vp->len-=l;
		}
		if (off<len) memmove(ptr+off+n2,ptr+off,len-off);
	}
	ushort sht=vp->offset+len+n2; vp->len+=n2;
	if (shs==NULL) {memcpy(ptr+len+n2,p1,ll); _set_u16(ptr+off,sht);}
	else for (uint16_t j=0; j<n; j++) {
		const byte *pp=TreePageMgr::getK(p1,&shs[j],ll);
		_set_u16(ptr+off+j*L_SHT,sht); memcpy((byte*)this+sht,pp,ll); sht+=ll;
	}
	return true;
}

void TreePageMgr::TreePage::deleteValues(uint16_t l,PagePtr *vp,uint16_t off,uint16_t n)
{
	if (vp->len==l) {
		vp->len=0; if (vp->offset!=info.freeSpace) info.scatteredFreeSpace+=l; else {info.freeSpace+=l; info.freeSpaceLength+=l;}
	} else {
		uint16_t n2=n*L_SHT; byte *beg=(byte*)this+vp->offset+off,*ptr=beg+n2; assert((vp->len&TRO_MANY)!=0 && !isSubTree(*vp));
		// preload ptr+off,n2
		while ((ptr-=L_SHT)>=beg) {
			uint16_t ll; TreePageMgr::getK(this,ptr,ll);
			if (_u16(ptr)!=info.freeSpace) info.scatteredFreeSpace+=ll; else {info.freeSpace+=ll; info.freeSpaceLength+=ll;}
		}
		l=vp->len&~TRO_MANY; if ((vp->len-=n2)==TRO_MANY) vp->len=0; assert (n2+off<=l);
		if (vp->offset!=info.freeSpace) {
			if (n2+off<l) memmove(beg,beg+n2,l-off-n2); info.scatteredFreeSpace+=n2;
		} else {
			if (n2<l&&off!=0) memmove((byte*)this+vp->offset+n2,(byte*)this+vp->offset,off);
			vp->offset+=n2; info.freeSpace+=n2; info.freeSpaceLength+=n2;
		}
	}
}

RC TreePageMgr::TreePage::adjustCount(byte *ptr,PagePtr *vp,unsigned idx,const byte *p,size_t lp,bool fDec)
{
	if (!info.fmt.isPinRef()) {
		if (!fDec) return RC_ALREADYEXISTS;
		//deleteValues(
		return RC_OK;
	}
	uint16_t sht=(vp->len&TRO_MANY)!=0?_u16(ptr):vp->offset,l0=PINRef::len((byte*)this+sht),l; byte buf[XPINREFSIZE]; RC rc;
	switch (rc=PINRef::adjustCount((byte*)this+sht,PINRef::getCount(p),buf,fDec)) {
	default: return rc;
	case RC_TRUE:
		if ((l=PINRef::len(buf))<l0) {memcpy((byte*)this+sht,buf,l0); info.scatteredFreeSpace+=uint16_t(l0-l);}
		else {
			uint16_t dl=l-l0;
			if (dl>info.freeSpaceLength+info.scatteredFreeSpace) {
				report(MSG_ERROR,"TreePageMgr adjustCount: insufficient space, requested %d, page %08X\n",dl,hdr.pageID);
				return RC_PAGEFULL;
			}
			if (sht!=info.freeSpace || info.freeSpaceLength<dl) {
				dl=l;
				if (info.freeSpaceLength>=l) info.scatteredFreeSpace+=l0;
				else {
					if ((vp->len&TRO_MANY)!=0) _set_u16(ptr,0); else vp->len=0;
					uint16_t off=uint16_t(ptr-((byte*)this+vp->offset)); info.scatteredFreeSpace+=l0;
					compact(false); ptr=(byte*)findData(idx,l0,(const PagePtr**)&vp);
					assert(ptr!=NULL && vp!=NULL && info.freeSpaceLength>=l); ptr+=off;
				}
			}
			info.freeSpace-=dl; info.freeSpaceLength-=dl; memcpy((byte*)this+info.freeSpace,buf,l);
			if ((vp->len&TRO_MANY)!=0) _set_u16(ptr,info.freeSpace); else {vp->offset=info.freeSpace; vp->len=l;}
		}
#ifdef  _DEBUG
		compact(true);
#endif
		break;
	case RC_FALSE:
		assert(fDec);
		deleteValues((uint16_t)l0,vp,uint16_t(ptr-(byte*)this-vp->offset),true);
		break;
	}
	return RC_OK;
}

RC TreePageMgr::TreePage::adjustKeyCount(unsigned idx,const byte *p2,size_t l2,bool fDec)
{
	assert(info.fmt.isRefKeyOnly() && info.lPrefix==0);
	uint32_t cnt=PINRef::getCount(p2); byte buf[XPINREFSIZE]; RC rc;
	uint16_t l0,l; byte *p=(byte*)getK(idx,l0); uint16_t *pp=(uint16_t*)(this+1)+idx;
	switch (rc=PINRef::adjustCount(p,cnt,buf,fDec)) {
	default: return rc;
	case RC_TRUE:
		l=PINRef::len(buf); assert(fDec && l<=l0 || !fDec && l>=l0);
		if (l<=l0) {
			uint16_t dl=l0-l;
			if (*pp!=info.freeSpace-sizeof(TreePage)) info.scatteredFreeSpace+=dl; else {*pp=(info.freeSpace+=dl)-sizeof(TreePage); info.freeSpaceLength+=dl;}
		} else {
			uint16_t dl=l-l0;
			if (*pp==info.freeSpace-sizeof(TreePage) && info.freeSpaceLength>=dl) {*pp=info.freeSpace-=dl; info.freeSpaceLength-=dl;}
			else if (info.freeSpaceLength>=l) {*pp=info.freeSpace-=l; info.freeSpaceLength-=l; info.scatteredFreeSpace+=l0;}
			else {*pp=0; info.scatteredFreeSpace+=l0; compact(false); assert(info.freeSpaceLength>=l); *pp=info.freeSpace-=l; info.freeSpaceLength-=l;}
			*pp-=sizeof(TreePage);
		}
		memcpy((byte*)(this+1)+*pp,buf,l); break;
	case RC_FALSE:
		assert(fDec);
		if (*pp==info.freeSpace-sizeof(TreePage)) {info.freeSpace+=l0; info.freeSpaceLength+=l0;} else info.scatteredFreeSpace+=l0;
		if (idx<--info.nEntries) memmove(pp,pp+1,(info.nEntries-idx)*L_SHT);
		info.freeSpaceLength+=uint16_t(L_SHT); --info.nSearchKeys;
		break;
	}
	return RC_OK;
}

PageID TreePageMgr::multiPage(unsigned info,const byte *rec,size_t lrec,bool& fMerge)
{
	switch (info&TRO_MASK) {
	case TRO_SPLIT:
		fMerge=false;
		if (lrec>=sizeof(TreePageSplit)) return ((const TreePageSplit*)rec)->newSibling;
		break;
	case TRO_MERGE:
		fMerge=true;
		if (lrec>=sizeof(TreePageSplit)) return ((const TreePageSplit*)rec)->newSibling;
		break;
	case TRO_SPAWN:
		fMerge=false;
		if (lrec>=sizeof(TreePageSpawn)) return ((const TreePageSpawn*)rec)->root;
		break;
	case TRO_ABSORB:
		fMerge=true;
		if (lrec>=sizeof(TreePageSpawn)) return ((const TreePageSpawn*)rec)->root;
		break;
	}
	return INVALID_PAGEID;
}

RC TreePageMgr::undo(unsigned info,const byte *rec,size_t lrec,PageID pid)
{
	Tree *tree=NULL; RC rc=RC_OK; SearchKey key; byte *buf=NULL; size_t ll;
	TREE_OP op; uint16_t nl=0,ol=0,off=0; const byte *newV=NULL,*oldV=NULL;
	const TreePageMgr::TreePage *tp; unsigned idx; unsigned multi=0;
	if (pid!=INVALID_PAGEID) {
		op=(TREE_OP)(info&TRO_MASK); if (lrec<=2) return RC_CORRUPTED;
		if (rec[lrec-1]==0xFF) {
			TreeFactory *factory=ctx->treeMgr->getFactory(rec[lrec-2]); if (factory==NULL) return RC_CORRUPTED;
			size_t l=rec[lrec-4]<<8|rec[lrec-3]; const byte *p=rec+lrec-l; if ((rc=key.deserialize(p,l,&ll))!=RC_OK) return rc;
			if ((rc=factory->createTree(p+ll,(byte)(l-ll-4),tree))!=RC_OK) return rc;
			const TreePageModify *tpm; const TreePageMulti *tpmi;
			switch (op) {
			case TRO_MULTI:
				tpmi=(TreePageMulti*)rec; newV=rec+sizeof(TreePageMulti); nl=tpmi->lData; multi=tpmi->nKeys;
				op=TRO_INSERT;
				if ((info>>TRO_SHIFT&0xFFFF)==MO_DELETE) {op=TRO_DELETE; oldV=newV; ol=nl; newV=NULL; nl=0;}
				break;
			case TRO_REPLACE:
				op=TRO_UPDATE; oldV=rec+sizeof(uint16_t); ol=*(uint16_t*)rec;
				newV=oldV+ol; nl=uint16_t(lrec-ol-sizeof(uint16_t)-rec[lrec-3]);
				break;
			case TRO_UPDATE: op=TRO_INSERT;
			case TRO_INSERT: case TRO_DELETE:
				tpm=(const TreePageModify*)rec; if ((tpm->oldData.len|tpm->newData.len)!=0) return RC_CORRUPTED;
				newV=(byte*)(tpm+1); nl=SearchKey::keyLenP(newV); if (op==TRO_DELETE) {oldV=newV; ol=nl; newV=NULL; nl=0;}
				break;
			default:
				return RC_CORRUPTED;
			}
		} else {
			TreeFactory *factory=ctx->treeMgr->getFactory(rec[lrec-1]); if (factory==NULL) return RC_CORRUPTED;
			byte l=rec[lrec-2]; if ((rc=factory->createTree(rec+lrec-l,l-2,tree))!=RC_OK) return rc;
			switch (op) {
			case TRO_APPEND:
				{const TreePageAppend *tpa=(const TreePageAppend*)rec; const TreePageAppendElt *tae=(TreePageAppendElt*)(tpa+1); TreeCtx tctx(*tree);
				for (unsigned i=0; i<(unsigned)tpa->nKeys; ++i,tae=(const TreePageAppendElt*)((byte*)(tae+1)+tae->lkey+tae->ldata)) {
					key.deserialize(tae+1,tae->lkey); tp=!tctx.pb.isNull()?(const TreePage*)tctx.pb->getPageBuf():NULL; int cmp=-1;
					if (tp==NULL || (cmp=tp->cmpKey(key))!=0) {
						if (cmp>0) {
							assert(tp->hasSibling()); ++ctx->treeMgr->sibRead;
							if (tctx.pb.getPage(tp->info.sibling,ctx->trpgMgr,PGCTL_COUPLE|PGCTL_ULOCK)==NULL ||
								(tp=(TreePageMgr::TreePage*)tctx.pb->getPageBuf())->cmpKey(key)>0) cmp=-1;
						}
						if (cmp<0 && (tctx.depth=0,rc=tctx.findPageForUpdate(&key,true))!=RC_OK) {if (rc!=RC_NOTFOUND) break; rc=RC_OK; continue;}
					}
					if (tp->findKey(key,idx) && (rc=remove(tctx,key,(byte*)(tae+1)+tae->lkey,tae->ldata,tae->ndata))!=RC_OK) break;
				}
				tree->destroy(); return rc;
				}
			case TRO_COUNTER:
				key.deserialize(rec+sizeof(int64_t),lrec-l-sizeof(int64_t)); break;
			case TRO_EDIT:
				{const TreePageEdit *tpe=(const TreePageEdit*)rec; key.deserialize(tpe+1,lrec-l-sizeof(TreePageEdit));
				oldV=rec+tpe->oldData.offset; ol=tpe->oldData.len; nl=tpe->newData.len; off=(uint16_t)tpe->shift;
				}break;
			default:
				{const TreePageModify *tpm=(const TreePageModify*)rec; key.deserialize(tpm+1,lrec-l-sizeof(TreePageModify));
				newV=rec+tpm->newData.offset; if ((nl=tpm->newData.len)==0) op=TRO_DELETE;
				oldV=rec+tpm->oldData.offset; if ((ol=tpm->oldData.len)==0) op=TRO_INSERT;
				}break;
			}
		}
	} else {
		static const TREE_OP TRO_ops[] = {TRO_INSERT,TRO_UPDATE,TRO_DELETE,TRO_EDIT};
		byte l=byte(info>>16); if (l<Tree::TO_INSERT||l>Tree::TO_EDIT) return RC_CORRUPTED;
		op=TRO_ops[l-Tree::TO_INSERT]; l=byte(info>>8);
		TreeFactory *factory=ctx->treeMgr->getFactory((byte)info);
		if (factory==NULL || rec==NULL || lrec<=l) return RC_CORRUPTED;
		if ((rc=factory->createTree(rec,l,tree))!=RC_OK) return rc;
		if ((rc=key.deserialize(rec+l,lrec-l,&ll))!=RC_OK) return rc;
		rec+=l+ll; lrec-=l+ll;
		if (lrec<sizeof(uint16_t)*2) return RC_CORRUPTED;
		memcpy(&nl,rec,sizeof(uint16_t));
		memcpy(&off,rec+sizeof(uint16_t),sizeof(uint16_t));
		if (lrec<sizeof(uint16_t)*2+nl) return RC_CORRUPTED;
		newV=rec+sizeof(uint16_t)*2; oldV=newV+nl; 
		ol=uint16_t(lrec-sizeof(uint16_t)*2-nl);
	}
	TreeCtx tctx(*tree); assert(tree!=NULL);
	if ((rc=tctx.findPageForUpdate(&key,op==TRO_DELETE))!=RC_OK) rc=op==TRO_INSERT?RC_OK:rc;
	else switch (op) {
	default: rc=RC_CORRUPTED; break;
	case TRO_INSERT: rc=remove(tctx,key,newV,nl,multi); break;
	case TRO_DELETE: rc=insert(tctx,key,oldV,ol,multi); break;
	case TRO_UPDATE: rc=update(tctx,key,newV,nl,oldV,ol); break;
	case TRO_EDIT: rc=edit(tctx,key,oldV,ol,nl,off); break;
	case TRO_COUNTER:
		tp=(TreePage*)tctx.pb->getPageBuf();
		rc=tp->findKey(key,idx)?ctx->txMgr->update(tctx.pb,this,idx<<TRO_SHIFT|TRO_COUNTER,rec,lrec,TXMGR_UNDO):RC_NOTFOUND;
		break;
	}
	tree->destroy(); if (buf!=NULL) free(buf,SES_HEAP);
	return rc==RC_TRUE||rc==RC_FALSE ? RC_OK : rc;
}

RC TreePageMgr::initPage(PBlock *pb,IndexFormat ifmt,uint16_t level,const SearchKey* key,PageID pid0,PageID pid1)
{
	unsigned lrec=sizeof(TreePageInit)+(key!=NULL?key->extLength():0); 
	TreePageInit *tpi=(TreePageInit*)alloca(lrec); if (tpi==NULL) return RC_NOMEM;
	tpi->fmt=ifmt; tpi->level=level;
	if (key==NULL) {assert(level==0); tpi->left=tpi->right=INVALID_PAGEID;}
	else {
		assert(level>0 && level<=TREE_MAX_DEPTH && pid0!=INVALID_PAGEID && pid1!=INVALID_PAGEID);
		tpi->left=pid0; tpi->right=pid1; tpi->fmt.makeInternal(); key->serialize(tpi+1);
	}
	return ctx->txMgr->update(pb,this,TRO_INIT,(byte*)tpi,lrec);
}

uint16_t TreePageMgr::TreePage::calcSplitIdx(bool &fInsR,const SearchKey& key,unsigned idx,size_t lInsert,uint16_t prefixSize,bool fNewKey) const
{
	uint16_t splitIdx=(uint16_t)idx; fInsR=true; unsigned level=info.level;
	if (info.nSearchKeys<=1) fInsR=idx>0;
	else if (idx!=info.nEntries) {
		unsigned delta=~0u; uint16_t lElt=info.calcEltSize();
		unsigned d1=idx==0?info.lPrefix-prefixSize:0,d2=idx>=info.nSearchKeys?info.lPrefix-prefixSize:0;
		bool fFixedKey=info.fmt.isFixedLenKey(),fFixedData=info.fmt.isFixedLenData();
		if (fFixedKey && fFixedData) {
			for (uint16_t imin=0,imax=info.nSearchKeys,iprev=0xffff; imax!=imin;) {
				uint16_t i=(imax+imin)/2; if (i==iprev) break; else iprev=i;
				long d=long((i*2-info.nEntries)*(lElt+info.fmt.dataLength()))+
						d1*i-d2*(info.nEntries-i)+long(idx<=i?lInsert-prefixSize:prefixSize-lInsert);
				if (d<=0) {imin=i; if (unsigned(-d)<delta) {delta=-d; splitIdx=i;}} else {imax=i; if (unsigned(d)<delta) {delta=d; splitIdx=i;}}
			}
			fInsR=idx>=splitIdx;
		} else {
			const bool fSub=info.fmt.isKeyOnly(); uint16_t i=0,sht=fSub?0:info.keyLength();
			size_t total=hdr.length()-sizeof(TreePage)-FOOTERSIZE-info.freeSpaceLength-info.scatteredFreeSpace,l=0; // contentLength()?
			for (const byte *p=(const byte*)(this+1);;l+=lElt,p+=lElt) {
				long d=long(l*2-total)+d1*i-d2*(info.nEntries-i);
				if (idx<i) d+=long(lInsert-prefixSize); else if (idx>i||!fNewKey) d-=long(lInsert-prefixSize);
				else {
					long dd1=d+long(lInsert-prefixSize),dd2=d-long(lInsert-prefixSize);
					if (dd1>0&&(dd2>=0||-dd2<dd1)) d=dd2; else if (unsigned((d=dd1)<0?-dd1:dd1)<delta) fInsR=false;
				} 
				if (d<0) {if (unsigned(-d)<delta) {delta=-d; splitIdx=i;}} else {if (unsigned(d)<delta) splitIdx=i; break;}
				if (++i>info.nSearchKeys||!fNewKey&&i==info.nSearchKeys) break;
				if (fSub) {uint16_t ll; getK(p,ll); l+=ll;}
				else {
					if (!fFixedKey) l+=((PagePtr*)p)->len;
					if (!fFixedData && i<=info.nSearchKeys) l+=length(*(PagePtr*)(p+sht));
				}
			}
			if (idx<splitIdx) fInsR=false; else if (!fNewKey) fInsR=idx>=splitIdx;
		}
	} else if (!info.fmt.isSeq() || level>0) {
		size_t le=info.fmt.isSeq()?sizeof(uint32_t):info.calcEltSize();
		if (!info.fmt.isFixedLenKey()) le+=key.extra()-prefixSize;
		if (prefixSize<info.lPrefix) le+=info.extraEltSize(prefixSize)*info.nEntries;
		if (le>unsigned(info.freeSpaceLength+info.scatteredFreeSpace)) splitIdx--;	// more???
	}
	return splitIdx;
}

RC TreePageMgr::split(TreeCtx& tctx,const SearchKey *key,unsigned& idx,uint16_t splitIdx,bool fInsR,PBlock **pRight)
{
	{Session *ses=Session::getSession(); if (ses!=NULL) ses->setCodeTrace(0x10);}
	const TreePage *tp=(const TreePage*)tctx.pb->getPageBuf();
	bool fDKey=splitIdx==idx && fInsR; unsigned level=tp->info.level; 
	assert(tp->info.nSearchKeys!=0); assert(!fDKey || key!=NULL);

	if (level==0 && splitIdx>0 && !tp->info.fmt.isFixedLenKey() && !tp->info.fmt.isRefKeyOnly()) {
		uint16_t lkey=fDKey?key->v.ptr.l:tp->getKeyExtra(splitIdx); assert(key==NULL||key->type>=KT_BIN&&key->type<KT_ALL);
		uint16_t lTrunc=fDKey?tp->calcPrefixSize(*key,splitIdx-1,true):tp->calcPrefixSize(splitIdx-1,splitIdx+1);
		if (lTrunc!=0) {
			lTrunc++;
			if (splitIdx==idx && !fInsR && key!=NULL) {uint16_t lk=tp->calcPrefixSize(*key,splitIdx,true)+1; if (lk>lTrunc) lTrunc=lk;}
			assert(lTrunc<=lkey);
			if (lTrunc<lkey) {
				SearchKey *sk=(SearchKey*)alloca(sizeof(SearchKey)+lkey); if (sk==NULL) return RC_NOMEM;
				if (fDKey) *sk=*key; else {fDKey=true; tp->getKey(splitIdx,*sk);} sk->v.ptr.l=lTrunc; key=sk;
			}
		}
	}
	if (!fDKey) {
		SearchKey *sk=(SearchKey*)alloca(sizeof(SearchKey)+tp->getKeyExtra(splitIdx));
		if (sk==NULL) return RC_NOMEM; tp->getKey(splitIdx,*sk); key=sk;
	}
	unsigned lsib=sizeof(TreePageSplit)+key->extLength();

	TreePageSplit *tps=(TreePageSplit*)alloca(lsib); if (tps==NULL) return RC_NOMEM;
	tps->nEntriesLeft=splitIdx; tps->oldPrefSize=tp->info.lPrefix; key->serialize(tps+1);
	if (fInsR) {assert(idx>=splitIdx); idx-=splitIdx;}
	assert(idx<=unsigned(fInsR?tp->info.nEntries-splitIdx:splitIdx));

	MiniTx tx(Session::getSession(),(tctx.tree->mode&TF_SPLITINTX)!=0?MTX_SKIP:0); PBlock *pbn; RC rc;
	if ((rc=ctx->fsMgr->allocPages(1,&tps->newSibling))!=RC_OK) return rc;
	assert(tps->newSibling!=INVALID_PAGEID);
	if ((pbn=ctx->bufMgr->newPage(tps->newSibling,this))==NULL) return RC_NOMEM;
	tctx.pb->checkDepth();

	if ((rc=ctx->txMgr->update(tctx.pb,this,TRO_SPLIT,(byte*)tps,lsib,0,pbn))!=RC_OK)
		{pbn->release(PGCTL_DISCARD|QMGR_UFORCE); return rc;}

#ifdef TRACE_EMPTY_PAGES
	if (tctx.mainKey!=NULL && tctx.depth>tctx.leaf) {
		report(MSG_DEBUG,"Split to pages %X(%f) and %X(%f)\n",
			tctx.pb->getPageID(),double(xSize-((TreePage*)tctx.pb->getPageBuf())->info.freeSpaceLength)*100./xSize,
			pbn->getPageID(),double(xSize-((TreePage*)pbn->getPageBuf())->info.freeSpaceLength)*100./xSize);
	}
#endif
	if (pRight!=NULL) *pRight=pbn; else if (fInsR) {tctx.pb.release(); tctx.pb=pbn; tctx.pb.set(QMGR_UFORCE);} else pbn->release();

	PageID newSibling=tps->newSibling;
	if (tctx.depth!=0) {
		PBlockP spb(tctx.pb); addNewPage(tctx,*key,newSibling,(tctx.tree->mode&TF_SPLITINTX)==0);
		tctx.pb.moveTo(tctx.parent); spb.moveTo(tctx.pb); tctx.stack[++tctx.depth]=tctx.pb->getPageID();
		assert((PBlock*)tctx.parent!=(PBlock*)tctx.pb);
	} else if (tctx.tree->addRootPage(*key,newSibling,level)==RC_OK) {
		tctx.parent.release(); tctx.stack[0]=newSibling; tctx.stack[++tctx.depth]=tctx.pb->getPageID();
	}
	tctx.newPage=tps->newSibling; tx.ok(); return RC_OK;
}

void TreePageMgr::addNewPage(TreeCtx& tctx,const SearchKey& key,PageID pid,bool fTry)
{
	if (tctx.getParentPage(key,fTry?PTX_FORUPDATE|PTX_TRY:PTX_FORUPDATE)==RC_OK) {
		const TreePage *tp=(const TreePage*)tctx.pb->getPageBuf(); assert(tctx.pb->getPageID()!=pid);
		if (tctx.mainKey==NULL || tctx.leaf<tctx.depth) {
			if ((tp->info.level!=1 || tp->info.stamp==tctx.stamps[PITREE_1STLEV]) && insert(tctx,key,&pid,sizeof(PageID))==RC_OK) return;
		} else {
			assert(tctx.vp.len!=0 && tctx.vp.offset!=0 && tp->isLeaf() && tctx.leaf==tctx.depth);
			SubTreePage tpe; memcpy(&tpe,(byte*)tp+tctx.vp.offset,sizeof(SubTreePage));
			unsigned lk=key.v.ptr.l; uint16_t off; const PagePtr *pvp;
			if (tctx.lvl2!=tpe.level) {
				// re-traverse from root etc.
			} else if (!fTry && tp->findExtKey(key.getPtr2(),lk,(SubTreePageKey*)((byte*)tp+tctx.vp.offset+sizeof(SubTreePage)),
									((tctx.vp.len&~TRO_MANY)-sizeof(SubTreePage))/sizeof(SubTreePageKey),&off)!=NULL) return;
			else if (unsigned((tctx.vp.len&~TRO_MANY)+sizeof(SubTreePageKey)+lk)<=tp->hdr.length()*SPAWN_THR
							&&	sizeof(SubTreePageKey)+lk<=unsigned(tp->info.freeSpaceLength+tp->info.scatteredFreeSpace)) {
				unsigned lrec=sizeof(TreePageRoot)+tctx.mainKey->extLength()+lk; TreePageRoot *tpr=(TreePageRoot*)alloca(lrec); 
				if (tpr!=NULL) {
					tpr->pageID=pid; tpr->pkey.len=uint16_t(lk); tpr->pkey.offset=uint16_t(lrec-lk);
					tctx.mainKey->serialize(tpr+1); memcpy((byte*)tpr+tpr->pkey.offset,key.getPtr2(),lk);
					RC rc=ctx->txMgr->update(tctx.pb,this,tctx.index<<TRO_SHIFT|TRO_ADDROOT,(byte*)tpr,lrec);
					if (fTry && tp->findData(tctx.index,off,&pvp)!=NULL && pvp!=NULL) tctx.vp=__una_get(*pvp);
					if (rc==RC_OK) return;
				}
			} else {
				unsigned ls=sizeof(TreePageSpawn)+tctx.mainKey->extLength(); TreePageSpawn *tpm=(TreePageSpawn*)alloca(ls);
				if (tpm!=NULL) {
					IndexFormat ifmt(tp->info.fmt.isPinRef()?KT_REF:KT_BIN,KT_VARKEY,sizeof(PageID));
					tpm->fmt=ifmt; tctx.mainKey->serialize(tpm+1); PBlock *pbn; TxSP tx(Session::getSession());
					if (tx.start()==RC_OK && ctx->fsMgr->allocPages(1,&tpm->root)==RC_OK && (pbn=ctx->bufMgr->newPage(tpm->root,ctx->trpgMgr))!=NULL) {
						if (ctx->txMgr->update(tctx.pb,this,tctx.index<<TRO_SHIFT|TRO_SPAWN,(byte*)tpm,ls,0,pbn)!=RC_OK)
							pbn->release(PGCTL_DISCARD|QMGR_UFORCE);
						else {
							if (fTry && tp->findData(tctx.index,off,&pvp)!=NULL && pvp!=NULL) tctx.vp=__una_get(*pvp);
							assert(tctx.pb->isDependent()); tctx.pb.moveTo(tctx.parent); tctx.pb=pbn; tctx.pb.set(QMGR_UFORCE);
							tctx.stack[++tctx.depth]=pbn->getPageID(); tctx.lvl2++;
							if (insert(tctx,key,&pid,sizeof(PageID))==RC_OK) {tx.ok(); return;}
						}
					}
				}
			}
		}
	}
	if (fTry) {tctx.depth++; tctx.postPageOp(key,pid); tctx.depth--;}
}

RC TreePageMgr::spawn(TreeCtx& tctx,size_t lInsert,unsigned idx)
{
	{Session *ses=Session::getSession(); if (ses!=NULL) ses->setCodeTrace(0x20);}
	RC rc=RC_OK; tctx.parent.release(); const TreePage *tp=(const TreePage*)tctx.pb->getPageBuf(); 
	uint16_t lElt=tp->info.calcEltSize(),sht=tp->info.keyLength();
	if (idx==~0u) {
		unsigned xLen=0; const byte *p=(const byte*)(tp+1);
		for (unsigned i=0; i<tp->info.nSearchKeys; ++i,p+=lElt)
			{const PagePtr *pp=(const PagePtr*)(p+sht); if (!tp->isSubTree(*pp) && pp->len>xLen) {xLen=pp->len; idx=i;}}
		if (idx==~0u || lInsert>unsigned(tp->info.freeSpaceLength+tp->info.scatteredFreeSpace) &&
			lInsert-unsigned(tp->info.freeSpaceLength+tp->info.scatteredFreeSpace)>xLen-sizeof(SubTreePage)) return RC_TOOBIG;
	}
	unsigned ls=sizeof(TreePageSpawn)+tp->getSerKeySize(uint16_t(idx));
	TreePageSpawn *tpm=(TreePageSpawn*)alloca(ls); if (tpm==NULL) return RC_NOMEM;
	IndexFormat ifmt(tp->info.fmt.isPinRef()?KT_REF:KT_BIN,KT_VARKEY,0);
	tpm->fmt=ifmt; tp->serializeKey(uint16_t(idx),tpm+1); 
	MiniTx tx(Session::getSession(),0); PBlock *pbn;
	if ((rc=ctx->fsMgr->allocPages(1,&tpm->root))!=RC_OK) return rc;
	assert(tpm->root!=INVALID_PAGEID);
	if ((pbn=ctx->bufMgr->newPage(tpm->root,this))==NULL) return RC_NOMEM;
	tctx.pb->checkDepth();
	if ((rc=ctx->txMgr->update(tctx.pb,this,idx<<TRO_SHIFT|TRO_SPAWN,(byte*)tpm,ls,0,pbn))!=RC_OK)
		pbn->release(PGCTL_DISCARD|QMGR_UFORCE); else {pbn->release(); tx.ok();}
	return rc;
}

RC TreePageMgr::modSubTree(TreeCtx& tctx,const SearchKey& key,unsigned kidx,const PagePtr *vp,const void *newV,uint16_t lNew,const void *oldV,uint16_t lOld,unsigned multi)
{
	RC rc=RC_OK; PBlockP spb(tctx.pb),spr; if (tctx.moreKeys!=NULL) tctx.parent.moveTo(spr); tctx.pb.moveTo(tctx.parent);
	tctx.mainKey=&key; tctx.index=kidx; tctx.vp=__una_get(*vp); assert(((TreePage*)spb->getPageBuf())->isSubTree(tctx.vp));
	tctx.stack[tctx.leaf=tctx.depth++]=spb->getPageID(); 
	const IndexFormat ifmt=((TreePage*)spb->getPageBuf())->info.fmt; const bool fPR=ifmt.isPinRef();
	SubTreePage tpe; memcpy(&tpe,spb->getPageBuf()+tctx.vp.offset,sizeof(SubTreePage)); tctx.lvl2=tpe.level;
	const bool fIns=oldV==NULL; assert(oldV==NULL||newV==NULL||multi==0);
	const byte *pv=(const byte*)(fIns?newV:oldV);
	const unsigned start=multi>>16,end=multi==0?1:start+(multi&0xFFFF); assert(start!=0xFFFF);
	if (oldV==NULL||tpe.counter>=end-start) {
		unsigned lrec=sizeof(int64_t)+key.extLength();
		TreeFactory *tf=tctx.tree->getFactory(); uint16_t lFact=tf!=NULL?tf->getParamLength()+2:0;
		int64_t *rec=(int64_t*)alloca(lrec+lFact); if (rec==NULL) return RC_NOMEM;
		*rec=fIns?(int64_t)end-start:(int64_t)start-end; key.serialize(rec+1);
		if (tf!=NULL) {byte *p=(byte*)rec+lrec+lFact; tf->getParams(p-lFact,*tctx.tree); p[-2]=byte(lFact); p[-1]=tf->getID();}
		if ((rc=ctx->txMgr->update(tctx.parent,this,kidx<<TRO_SHIFT|TRO_COUNTER,(byte*)rec,lrec+lFact,tf!=NULL?LRC_LUNDO:0))!=RC_OK) return rc;
	}
	if (multi!=0) {
		unsigned from=start,to=end; uint16_t lk; const byte *pk=getK(pv,start,lk); SearchKey mkey(pk,lk,fPR);
		for (rc=tctx.findPageForUpdate(&mkey,fIns); rc==RC_OK; ) {
			const TreePage *tp=(const TreePage*)tctx.pb->getPageBuf(); PageID sib=tp->info.sibling;
			if (sib!=INVALID_PAGEID) {to=from; tp->findSubKey(pv,end,tp->info.nEntries-1,to); assert(from<to);}
			if (fIns) {
				unsigned pageCnt=0; PageID pages[XSUBPAGES]; uint16_t indcs[XSUBPAGES]; int cmp; bool fFits=false; size_t xSz=calcXSz(pv,from,to,tp);
				while (from<to && tp->info.nSearchKeys!=0) {
					size_t lChunk=calcChunkSize(pv,from,to); fFits=false;
					if (lChunk<unsigned(tp->info.freeSpaceLength)) {fFits=true; break;}
					if ((cmp=tp->cmpKeys(pv,from,tp->info.nSearchKeys-1))>0) break;
					unsigned sidx=0,psidx=0,idx=from,pidx=from; PBlock *right; uint16_t lf; const byte *pf=getK(pv,from,lf);
					if ((cmp=tp->cmpKeys(pv,from,0))<0) {
						tp->findSubKey(pv,to,0,idx); assert(idx>from && idx<=to);
						size_t sz=calcChunkSize(pv,from,idx); unsigned idx2=0;
						// lLast?
						if (sz>xSize) {
							//??? which key insret in split?
							if ((rc=split(tctx,NULL,idx2,0,false,&right))!=RC_OK ||
								(rc=insertSubTree(tctx,pv,ifmt,from,idx,pageCnt,pages,indcs,xSz,&from))!=RC_OK || from>=to) break;
							tctx.pb.release(); tctx.pb=right; tctx.pb.set(QMGR_UFORCE); tp=(const TreePage*)right->getPageBuf(); continue;
						}
					} else if (cmp>0) tp->findSubKey(pf,lf,sidx);
					SearchKey first(pf,lf,fPR),*skey=NULL;
					for (unsigned b2=0,e2=~0u;;) {
						size_t lChunk=calcChunkSize(pv,from,idx)+sidx*L_SHT,lK=0;
						for (unsigned j=0; j<sidx; j++) {uint16_t ll; tp->getK(j,ll); lChunk+=ll;}
						if (idx<to) {uint16_t ll; getK(pv,idx,ll); lK=ll+L_SHT;}
						if (sidx<tp->info.nEntries) {size_t lK2=PINRef::len((*tp)[sidx])+L_SHT; if (lK2>lK) lK=lK2;}
						if ((lChunk+=lK)>=xSz && lChunk<=xSize && double(lChunk-xSz)/xSz<0.05) break;
						if (psidx<sidx) {
							if (e2!=~0u) {
								if (lChunk>xSz) {if ((e2=sidx)==b2) break;} else if ((b2=sidx)+1>=e2) break;
								if ((sidx=(b2+e2)/2)==psidx) break;
							} else if (lChunk>xSz) {b2=psidx; e2=sidx; if ((sidx=(b2+e2)/2)==psidx) break;}
							else if ((psidx=sidx)>=tp->info.nSearchKeys) {b2=idx; e2=idx=to;}
							else {tp->findSubKey(pv,to,sidx,idx); assert(idx<=to);}
						} else {
							if (e2!=~0u) {
								if (lChunk>xSz) {if ((e2=idx)==b2) break;} else if ((b2=idx)+1>=e2) break;
								if ((idx=(b2+e2)/2)==pidx) break; assert(idx<=to);
							} else if (lChunk>xSz) {b2=pidx; e2=idx; if ((idx=(b2+e2)/2)==pidx) break;}
							else if ((pidx=idx)==to) {b2=sidx; e2=sidx=tp->info.nEntries;}
							else {uint16_t l2; const byte *p2=getK(pv,idx,l2); tp->findSubKey(p2,l2,sidx);}
						}
					}
					assert(idx<=to);
					if (uint16_t(sidx)>=tp->info.nSearchKeys) break;
					// here: sidx - where to split, idx - how many to insert
					if (idx<to && tp->cmpKeys(pv,idx,sidx)<0) {
						uint16_t ll; const byte *p=getK(pv,idx,ll); new(skey=&first) SearchKey(p,ll,fPR);
					} else
						while (idx>from && tp->cmpKeys(pv,idx-1,sidx)>0) idx--;
					if ((rc=split(tctx,skey,sidx,uint16_t(sidx),skey!=NULL,&right))!=RC_OK) break;
					if (from<idx) {
						if ((rc=insertSubTree(tctx,pv,ifmt,from,idx,pageCnt,pages,indcs,xSize))!=RC_OK) break;
						from=idx; assert(pageCnt==1);
					}
					tctx.pb.release(); tctx.pb=right; tctx.pb.set(QMGR_UFORCE); tp=(const TreePage*)right->getPageBuf();
				}
				if (rc!=RC_OK || from<to && (rc=insertSubTree(tctx,pv,ifmt,from,to,pageCnt,pages,indcs,fFits?xSize:xSz))!=RC_OK) break;
				assert(!fFits || pageCnt==1);
			} else {
				uint16_t sht=((uint16_t*)pv)[from],l1; getK(pv,from,l1);
				unsigned lChunk=(to-from+1)*L_SHT+((uint16_t*)pv)[to]-sht;		//??????????????????????????????????????????????????????
				TreeFactory *tf=tctx.tree->getFactory(); if (tf==NULL) {rc=RC_INTERNAL; break;}
				uint16_t lk=mkey.extLength(),lFact=lk+tf->getParamLength()+4; unsigned xbuf=lChunk+sizeof(TreePageMulti)+lFact; 
				byte *buf=(byte*)malloc(xbuf,SES_HEAP); if (buf==NULL) {rc=RC_NOMEM; break;}
				uint16_t lData=packMulti(buf,pv,from,to);
				((TreePageMulti*)buf)->fmt=tp->info.fmt; ((TreePageMulti*)buf)->sibling=sib;
				((TreePageMulti*)buf)->fLastR=0; ((TreePageMulti*)buf)->nKeys=to-from; ((TreePageMulti*)buf)->lData=lData;
				byte *p=(byte*)buf+sizeof(TreePageMulti)+lData+lFact; tf->getParams(p-lFact+lk,*tctx.tree);
				tctx.mainKey->serialize(p-lFact); p[-4]=byte(lFact>>8); p[-3]=byte(lFact); p[-2]=tf->getID(); p[-1]=0xFF;
				rc=ctx->txMgr->update(tctx.pb,this,MO_DELETE<<TRO_SHIFT|TRO_MULTI,buf,sizeof(TreePageMulti)+lData+lFact,LRC_LUNDO);
				free(buf,SES_HEAP); if (rc!=RC_OK) break;
			}
			if (to>=end) break;
			assert(sib!=INVALID_PAGEID);
			from=to; to=end; uint16_t lNext; const byte *next=getK(pv,from,lNext);
			if (!tctx.parent.isNull() && tctx.parent->getPageID()!=spb->getPageID()) tctx.parent.release();
			if (tctx.parent.isNull()) {tctx.parent=(PBlock*)spb; tctx.parent.set(PGCTL_NOREL);}
			++ctx->treeMgr->sibRead;
			if (tctx.pb.getPage(sib,this,PGCTL_COUPLE|PGCTL_ULOCK)!=NULL) {
				tp=(const TreePage*)tctx.pb->getPageBuf(); if (!tp->hasSibling()||tp->info.nEntries==0) continue; int cmp=0;
				if (!tp->info.fmt.isRefKeyOnly()) {
					uint16_t lk0; const byte *p=tp->getK(tp->info.nEntries-1,lk0);
					if (cmp==0 && ((cmp=memcmp(next,p,min(lk0,lNext)))<0 || cmp==0 && lNext<lk0)) continue;
				} else if (PINRef::cmpPIDs(next,(*tp)[tp->info.nEntries-1])<0) continue;
			}
			new(&mkey) SearchKey(next,lNext,fPR); tctx.depth=tctx.leaf+1; rc=tctx.findPageForUpdate(&mkey,fIns);
		}
	} else {
		if (oldV!=NULL && lOld!=0) {
			SearchKey okey((const byte*)oldV,lOld,fPR); const TreePage *tp; const PagePtr *pvp;
			if (tctx.findPageForUpdate(&okey,false)!=RC_OK || !(tp=(TreePage*)tctx.pb->getPageBuf())->findKey(okey,tctx.index)) rc=RC_NOTFOUND;
			else {
				tctx.parent.release(); bool fRepl=false; unsigned idx2; oldV=tp->getK(tctx.index,lOld);
				if (newV!=NULL && lNew!=0 && (lNew<=lOld || lNew-lOld<=tp->info.freeSpaceLength)) {
					if (tp->findSubKey((const byte*)newV,lNew,idx2)) {
						if (!ifmt.isPinRef() || PINRef::cmpPIDs((byte*)oldV,(byte*)newV)!=0) rc=RC_ALREADYEXISTS; else fRepl=true;
					} else if (idx2>0 && (!tp->hasSibling() || idx2<tp->info.nEntries)) fRepl=true;
					else if (idx2==0) {if (tpe.anchor==tctx.pb->getPageID()) fRepl=true;}
				}
				if (rc==RC_OK) {
					TreeFactory *tf=tctx.tree->getFactory(); uint16_t lk=key.extLength(),lFact=tf!=NULL?tf->getParamLength()+lk+4:0;
					if (fRepl) {
						uint16_t l=sizeof(uint16_t)+lOld+lNew; byte *buf=(byte*)alloca(l+lFact);
						if (buf==NULL) rc=RC_NOMEM;
						else {
							((uint16_t*)buf)[0]=lOld; memcpy(buf+sizeof(uint16_t),oldV,lOld); memcpy(buf+sizeof(uint16_t)+lOld,newV,lNew);
							if (tf!=NULL) {byte *p=buf+l+lFact; tf->getParams(p-lFact+lk,*tctx.tree); key.serialize(p-lFact); p[-4]=byte(lFact>>8); p[-3]=byte(lFact); p[-2]=tf->getID(); p[-1]=0xFF;}
							rc=ctx->txMgr->update(tctx.pb,this,tctx.index<<TRO_SHIFT|TRO_REPLACE,buf,l+lFact,tf!=NULL?LRC_LUNDO:0);
						}
						newV=NULL; lNew=0;
					} else {
						unsigned lrec=sizeof(TreePageModify)+SearchKey::extLength(lOld); TreePageModify *tpm=(TreePageModify*)alloca(lrec+lFact);
						if (tpm==NULL) rc=RC_NOMEM;
						else {
							memset(tpm,0,sizeof(TreePageModify)); tpm->newPrefSize=0; SearchKey::serialize(tpm+1,fPR?KT_REF:KT_BIN,oldV,lOld);
							if (tf!=NULL) {byte *p=(byte*)tpm+lrec+lFact; tf->getParams(p-lFact+lk,*tctx.tree); key.serialize(p-lFact); p[-4]=byte(lFact>>8); p[-3]=byte(lFact); p[-2]=tf->getID(); p[-1]=0xFF;}
							rc=ctx->txMgr->update(tctx.pb,this,tctx.index<<TRO_SHIFT|TRO_DELETE,(byte*)tpm,lrec+lFact,tf!=NULL?LRC_LUNDO:0);
							if (rc==RC_OK && newV!=NULL && lNew!=0) {
								tctx.parent.release(); tctx.parent=(PBlock*)spb; tctx.parent.set(PGCTL_NOREL|spb.uforce()); tctx.depth=tctx.leaf+1;
								if (!(tp=(TreePage*)spb->getPageBuf())->findKey(key,tctx.index) || tp->findData(tctx.index,lOld,&pvp)==NULL)
									rc=RC_CORRUPTED; else tctx.vp=__una_get(*pvp);
							}
						}
					}
				}
			}
		}
		if (rc==RC_OK && newV!=NULL && lNew!=0) {
			SearchKey key((const byte*)newV,lNew,fPR); assert(tctx.depth==tctx.leaf+1);
			if ((rc=tctx.findPageForUpdate(&key,true))==RC_OK) rc=insert(tctx,key,NULL,0);
		}
	}
	spr.moveTo(tctx.parent); spb.moveTo(tctx.pb); tctx.depth=tctx.leaf; tctx.leaf=~0u; tctx.mainKey=NULL;
	assert((PBlock*)tctx.parent!=(PBlock*)tctx.pb);
	return rc;
}

RC TreePageMgr::insertSubTree(TreeCtx &tctx,const void *value,IndexFormat ifmt,unsigned start,unsigned end,unsigned& pageCnt,PageID *pages,uint16_t *indcs,size_t xSz,unsigned *pRes)
{
	{Session *ses=Session::getSession(); if (ses!=NULL) ses->setCodeTrace(0x40);}
	RC rc=RC_OK; PBlockP pb; const TreePage *tp=NULL; PageID sibling=INVALID_PAGEID; pageCnt=0;
	size_t xbuf=0,lLast=0; unsigned nOldPages=0;
	if (!tctx.pb.isNull()) {
		tp=(const TreePage*)tctx.pb->getPageBuf(); pages[0]=tctx.pb->getPageID(); nOldPages=1;
		if ((sibling=tp->info.sibling)!=INVALID_PAGEID) lLast=tp->getKeyExtra(uint16_t(~0u))+L_SHT;
	}
	for (unsigned idx=start,i2; idx<end; idx=i2) {
		if (pageCnt>=XSUBPAGES) return RC_NOMEM;
		for (unsigned b2=idx+(pageCnt>=nOldPages?1:0),e2=i2=end; ;) {
			size_t dl=calcChunkSize((const byte*)value,idx,i2==end?i2:i2+1),lb=dl,xS=xSz;
			if (pageCnt!=0) {if (i2==end) lb=dl+=lLast;}
			else if (tp!=NULL) {xS=tp->info.freeSpaceLength; if (i2<end) xS+=lLast; if (xS>xSz) xS=xSz;}
			if (dl==xS || dl<xS && (b2=i2)+1>=e2) {
				if (lb+sizeof(TreePageMulti)+L_SHT>xbuf) xbuf=lb+sizeof(TreePageMulti)+L_SHT;
				if (i2==end && dl<xSz && pRes!=NULL) {*pRes=idx; assert(pageCnt!=0);}
				else {indcs[pageCnt]=(uint16_t)i2; pageCnt++;}
				break;
			}			
			if (dl<xS || (e2=i2)!=b2) i2=(e2+b2)/2;
			else if (tp==NULL || pageCnt>=nOldPages) return RC_PAGEFULL;
			else {
				i2=tp->info.nEntries-1; if ((rc=split(tctx,NULL,i2,tp->info.nEntries/2,true))!=RC_OK) return rc;
				tp=(const TreePage*)tctx.pb->getPageBuf(); pages[0]=tctx.pb->getPageID();
				b2=idx+(pageCnt>=nOldPages?1:0); e2=i2=end;
			}
		}
	}
	if (pageCnt<=nOldPages) {assert(lLast==0 || tp->cmpKeys((byte*)value,end-1,tp->info.nEntries-1)<=0); lLast=0;}
	TreeFactory *tf=tctx.tree->getFactory(); uint16_t lFact=tf!=NULL?tf->getParamLength()+2:0,lk=0; assert(tctx.mainKey!=NULL);
	if (tf!=NULL) lFact+=(lk=tctx.mainKey->extLength())+2; xbuf+=lFact;
	byte *buf=(byte*)malloc(xbuf,SES_HEAP); if (buf==NULL) return RC_NOMEM;
	TreePageMulti *tpm=(TreePageMulti*)buf; IndexFormat subfmt(ifmt.isPinRef()?KT_REF:KT_BIN,KT_VARKEY,0);
	if (pageCnt==nOldPages || (rc=ctx->fsMgr->allocPages(pageCnt-nOldPages,pages+nOldPages))==RC_OK) for (unsigned i=pageCnt; i--!=0; sibling=pages[i],lLast=0) {
		uint16_t strt=i==0?(uint16_t)start:indcs[i-1],end=indcs[i]+(i+1==pageCnt?0:1);
		uint16_t lData=packMulti(buf+sizeof(TreePageMulti),value,strt,end,lLast!=0?(TreePage*)tctx.pb->getPageBuf():(TreePage*)0);
		unsigned op=MO_INSERT<<TRO_SHIFT|TRO_MULTI; assert(sizeof(TreePageMulti)+lData+lFact<=xbuf); double prev=0.;
		tpm->fmt=subfmt; tpm->sibling=sibling; tpm->fLastR=0; tpm->lData=lData; tpm->nKeys=end-strt;
		if (i>=nOldPages) {op=MO_INIT<<TRO_SHIFT|TRO_MULTI; if (pb.newPage(pages[i],this)==NULL) {rc=RC_NOMEM; break;}}
		else {pb.release(); pb=(PBlock*)tctx.pb; pb.set(PGCTL_NOREL); tpm->fLastR=tp->hasSibling()&&pageCnt>nOldPages; prev=double(xSize-tp->info.freeSpaceLength)*100./xSize;}
		if (tf!=NULL) {
			byte *p=(byte*)buf+sizeof(TreePageMulti)+lData+lFact; tf->getParams(p-lFact+lk,*tctx.tree);
			tctx.mainKey->serialize(p-lFact); p[-4]=byte(lFact>>8); p[-3]=byte(lFact); p[-2]=tf->getID(); p[-1]=0xFF;
		}
		if ((rc=ctx->txMgr->update(pb,this,op,buf,sizeof(TreePageMulti)+lData+lFact,tf!=NULL?LRC_LUNDO:0))!=RC_OK) break;
#ifdef TRACE_EMPTY_PAGES
		report(MSG_DEBUG,"Inserted %d bytes into page %X(%f -> %f)\n",lData,pb->getPageID(),prev,double(xSize-((TreePage*)pb->getPageBuf())->info.freeSpaceLength)*100./xSize);
#endif
	}
	if (rc==RC_OK && pageCnt>nOldPages) {
#if 0
			PBlockP spb(tctx.pb); SearchKey key;
#ifdef _DEBUG
			unsigned depth=tctx.depth;
#endif
			for (unsigned i=0; i<pageCnt; i++) {
				// set key
				addNewPage(tctx,key,pages[i],true);
				tctx.parent.release(); tctx.parent=tctx.pb; tctx.pb=NULL; tctx.depth++;
			}
			if (!tctx.parent.isNull()) tctx.stack[tctx.depth-1]=tctx.parent->getPageID();
			tctx.pb=spb; assert(tctx.depth==depth);
#endif
	}
	pb.release(); free(buf,SES_HEAP); return rc;
}

int TreePageMgr::TreePage::cmpKeys(const byte *pv,unsigned idx,unsigned sidx) const
{
	uint16_t l1; const byte *p1=TreePageMgr::getK(pv,idx,l1); int cmp;
	if (info.fmt.isRefKeyOnly()) return PINRef::cmpPIDs(p1,(*this)[sidx]);
	uint16_t lk0; const byte *p=getK(sidx,lk0);
	return (cmp=memcmp(p1,p,min(lk0,l1)))==0?cmp3(l1,lk0):cmp;
}

size_t TreePageMgr::calcChunkSize(const byte *vals,unsigned start,unsigned end)
{
	size_t l=(end-start)*L_SHT;
	for (unsigned i=start; i<end; i++) l+=PINRef::len(vals+((uint16_t*)vals)[i]);
	return l;
}

uint16_t TreePageMgr::calcXSz(const byte *pv,unsigned from,unsigned to,const TreePage *tp) 
{
	size_t l=calcChunkSize(pv,from,to); if (tp!=NULL) l+=xSize-tp->info.freeSpaceLength;
	unsigned nPages=unsigned((l+xSize-1)/xSize); return uint16_t((l/=nPages)>xSize?xSize:l);
}

uint16_t TreePageMgr::packMulti(byte *buf,const void *value,unsigned start,unsigned end,TreePage *tp)
{
	uint16_t ll=uint16_t(end-start)*L_SHT,l; assert(start<end && (tp==NULL||tp->hasSibling()));
	for (unsigned i=start; i<end; i++)
		{const byte *pp=getK(value,i,l); ((uint16_t*)buf)[i-start]=ll; memcpy(buf+ll,pp,l); ll+=l;}
	if (tp!=NULL) {const byte *pp=tp->getK(tp->info.nEntries-1,l); memcpy(buf+ll,pp,l); ll+=l;}
	return ll;
}

PageID TreePageMgr::startSubPage(const TreePage *tp,const PagePtr& vp,const SearchKey *key,int& level,bool fRead,bool fBefore)
{
	uint16_t len=vp.len&~TRO_MANY;
	assert(tp->isSubTree(vp) && size_t(len)>=sizeof(SubTreePage) && (len-sizeof(SubTreePage))%sizeof(SubTreePageKey)==0);
	SubTreePage tpe; memcpy(&tpe,(byte*)tp+vp.offset,sizeof(SubTreePage)); level=tpe.level;
	if (len<=sizeof(SubTreePage) || key==NULL && !fBefore) return tpe.leftMost;
	const SubTreePageKey *tpk=NULL; PageID pid;
	if (key!=NULL) {
		assert(key->type==KT_BIN || key->type==KT_REF);
		tpk=tp->findExtKey(key->getPtr2(),key->v.ptr.l,(SubTreePageKey*)((byte*)tp+vp.offset+sizeof(SubTreePage)),(len-sizeof(SubTreePage))/sizeof(SubTreePageKey),&len);
		if (len==0 && (fBefore||tpk==NULL)) return tpe.leftMost; if (fBefore) tpk=NULL; else len+=sizeof(SubTreePage);
	}
	memcpy(&pid,tpk!=NULL?&tpk->pageID:&((SubTreePageKey*)((byte*)tp+vp.offset+len-sizeof(SubTreePageKey)))->pageID,sizeof(PageID));
	return pid;
}

PageID TreePageMgr::prevStartSubPage(const TreePage *tp,const PagePtr& vp,PageID pid)
{
	uint16_t len=vp.len&~TRO_MANY; if (len<=sizeof(SubTreePage)) return INVALID_PAGEID;
	SubTreePage tpe; memcpy(&tpe,(byte*)tp+vp.offset,sizeof(SubTreePage)); const byte *p=(byte*)tp+vp.offset+len;
	for (long i=(len-sizeof(SubTreePage))/sizeof(SubTreePageKey); --i>=0; p-=sizeof(SubTreePageKey)) {
		PageID pp; memcpy(&pp,&((SubTreePageKey*)p)->pageID,sizeof(PageID));
		if (pp==pid) return i==0?tpe.leftMost:(memcpy(&pp,&((SubTreePageKey*)p)[-1].pageID,sizeof(PageID)),pp);
	}
	return INVALID_PAGEID;
}

RC TreePageMgr::count(TreeCtx& tctx,const SearchKey& key,uint64_t& nValues)
{
	nValues=0; unsigned idx; uint16_t lData; const PagePtr *vp; assert(!tctx.pb.isNull());
	const TreePage *tp=(const TreePage*)tctx.pb->getPageBuf();
	if (tp->info.fmt.keyType()!=key.type||!tp->isLeaf()) return RC_INVPARAM;
	if (tp->info.fmt.isSeq()) {nValues=key.v.u<tp->info.prefix+tp->info.nEntries?1:0; return RC_OK;}
	if (!tp->findKey(key,idx)) return RC_NOTFOUND;
	if (tp->info.fmt.isKeyOnly()) return RC_OK;
	if (tp->info.fmt.isFixedLenData()) {nValues=1; return RC_OK;}
	if (tp->findData(idx,lData,&vp)==NULL||vp==NULL) return RC_CORRUPTED;
	if (!tp->isSubTree(*vp)) nValues=(vp->len&TRO_MANY)!=0?(vp->len&~TRO_MANY)/L_SHT:1;
	else if ((vp->len&~TRO_MANY)<sizeof(SubTreePage)) return RC_CORRUPTED;
	else {SubTreePage stp; memcpy(&stp,(byte*)tp+vp->offset,sizeof(SubTreePage)); nValues=stp.counter;}
	return RC_OK;
}

RC TreePageMgr::findByPrefix(TreeCtx& tctx,const SearchKey& key,uint32_t prefix,byte *buf,byte &l)
{
	unsigned idx; uint16_t lData; const PagePtr *vp; const byte *p0,*p; assert(!tctx.pb.isNull());
	const TreePage *tp=(const TreePage*)tctx.pb->getPageBuf();
	if (tp->info.fmt.keyType()!=key.type||!tp->isLeaf()||!tp->info.fmt.isPinRef()) return RC_INVPARAM;
	if (!tp->findKey(key,idx)) return RC_NOTFOUND;
	if (tp->findData(idx,lData,&vp)==NULL||vp==NULL) return RC_CORRUPTED;
	if (!tp->isSubTree(*vp)) p=p0=(const byte*)tp+vp->offset;
	else {
		if ((vp->len&~TRO_MANY)<sizeof(SubTreePage)) return RC_CORRUPTED;
		SubTreePage stp; memcpy(&stp,(byte*)tp+vp->offset,sizeof(SubTreePage));
		PID pd={0ULL,0}; PINRef pr(ctx->storeID,pd); pr.prefix=prefix; pr.def|=PR_PREF32; l=pr.enc(buf);
		PBlockP spb(tctx.pb); tctx.pb.moveTo(tctx.parent); tctx.mainKey=&key; tctx.index=idx;
		tctx.vp=__una_get(*vp); tctx.stack[tctx.leaf=tctx.depth++]=spb->getPageID(); tctx.lvl2=stp.level;
		SearchKey mkey(buf,l,true); RC rc=tctx.findPage(&mkey); if (rc!=RC_OK) return rc;
		p=p0=(const byte*)((const TreePage*)tctx.pb->getPageBuf()+1);
	}
	uint64_t pref; const byte *q; uint16_t ll,sht; int cmp;
	for (unsigned n=_u16(p)/L_SHT,lElt=L_SHT;;) {
		unsigned k=n>>1; q=p+k*L_SHT; sht=_u16(q); ll=_u16(q+L_SHT)-sht;
		if (!PINRef::getPrefix(p0+sht,pref)) break;
		if ((cmp=cmp3((uint32_t)pref,prefix))==0) {memcpy(buf,p0+sht,l=(byte)ll); return RC_OK;}
		if (cmp>0) {if ((n=k)==0) break;} else if ((n-=k+1)==0) break; else p=q+lElt;
	}
	return RC_NOTFOUND;
}

RC TreePageMgr::truncate(TreeCtx& tctx,const SearchKey& key,uint64_t val,bool fCount)
{
	unsigned idx; uint16_t lData; const PagePtr *vp; assert(!tctx.pb.isNull());
	const TreePage *tp=(const TreePage*)tctx.pb->getPageBuf();
	if (tp->info.fmt.keyType()!=key.type||!tp->isLeaf()) return RC_INVPARAM;
	if (tp->info.fmt.isSeq()) return RC_INTERNAL;
	if (tp->info.fmt.isKeyOnly() || tp->info.fmt.isFixedLenData() || !tp->findKey(key,idx)) return RC_OK;
	if (tp->findData(idx,lData,&vp)==NULL||vp==NULL) return RC_CORRUPTED;
	for (;;) {
		if (tp->isSubTree(*vp)) {
			if ((vp->len&~TRO_MANY)<sizeof(SubTreePage)) return RC_CORRUPTED;
			SubTreePage stp; memcpy(&stp,(byte*)tp+vp->offset,sizeof(SubTreePage));
//			nValues=stp.counter;
		} 
//		else if (tp->info.fmt.isFixedLenData()) nValues=vp->len/tp->info.fmt.dataLength();
//		else nValues=_u16((byte*)tp+vp->offset)/L_SHT;
		return RC_OK;
	}
}

RC TreePageMgr::insert(TreeCtx& tctx,const SearchKey& key0,const void *value,uint16_t lValue,unsigned multi,bool fUnique)
{
	RC rc=RC_OK; assert(!tctx.pb.isNull());
	const TreePage *tp=(const TreePage*)tctx.pb->getPageBuf(); const SearchKey *key=&key0;
	if (tp->info.fmt.keyType()!=key->type||multi!=0&&tp->info.fmt.isFixedLenData()) return RC_INVPARAM;
	size_t lrec=sizeof(TreePageModify)+key->extLength(),lInsert=0,lExtra=0,lKey=tp->info.calcEltSize();
	const void *pOld=NULL; const PagePtr *vp=NULL; TREE_OP op=TRO_INSERT;
	
	unsigned spaceLeft=tp->info.freeSpaceLength+tp->info.scatteredFreeSpace; 
	bool fVM=!tp->info.fmt.isFixedLenData(),fSubTree=(multi>>16)==0xFFFF; unsigned idx=0;
	uint16_t lOld,prefixSize=tp->info.lPrefix,n=uint16_t(multi&0xFFFF),start=fSubTree?0:uint16_t(multi>>16);

	if (!fSubTree && n==1) {n=multi=0; if (fVM) value=getK(value,start,lValue);}

	assert(!tp->info.fmt.isFixedLenData() || multi==0 && lValue==tp->info.fmt.dataLength());
	assert(!tp->hasSibling() || tp->testKey(key0,uint16_t(~0u))<0);
	assert((PBlock*)tctx.parent!=(PBlock*)tctx.pb);

	if (tp->info.fmt.isSeq()) {
		if (!tp->isLeaf() || tp->hasSibling() || key->v.u!=tp->info.prefix+tp->info.nEntries) {
			// report(...
			return RC_CORRUPTED;
		}
		idx=tp->info.nEntries; lInsert=lValue; if (!tp->info.fmt.isFixedLenData()) lInsert+=sizeof(PagePtr);
	} else if (tp->info.nSearchKeys==0 || !tp->findKey(*key,idx)) {
		assert((idx>0||tp->info.level==0||tp->info.leftMost!=INVALID_PAGEID) && idx<=tp->info.nSearchKeys&&(idx<tp->info.nEntries||!tp->hasSibling()));
		if (tp->info.nEntries==0) {prefixSize=tp->calcPrefixSize(*key,0); lKey=tp->info.calcEltSize(prefixSize);}
		else if (prefixSize!=0 && (idx==0 || idx==tp->info.nEntries) && (prefixSize=tp->calcPrefixSize(*key,idx==0?tp->info.nEntries-1:0))<tp->info.lPrefix)
			lExtra=tp->info.extraEltSize(prefixSize)*(tp->info.nEntries+1);
		lInsert=lKey+tp->info.calcVarSize(*key,lValue,prefixSize); if (!tp->info.fmt.isFixedLenKey()) lKey+=key->extra()-prefixSize;
		if (multi==0) {if (lInsert>xSize/3) return RC_TOOBIG;}
		else if (fSubTree) n|=0x8000;
		else if (lValue>=0x8000 || lInsert+lExtra>spaceLeft && lValue>xSize/6) {
			unsigned pageCnt=0,lkeys=0; PageID pages[XSUBPAGES]; uint16_t indcs[XSUBPAGES]; tctx.mainKey=key;
			PBlockP spb(tctx.pb); tctx.pb.moveTo(tctx.parent); tctx.stack[tctx.leaf=tctx.depth++]=spb->getPageID();
			rc=insertSubTree(tctx,value,tp->info.fmt,start,start+n,pageCnt,pages,indcs,calcXSz((byte*)value,start,start+n));
			tctx.parent.release(); spb.moveTo(tctx.pb); tctx.depth=tctx.leaf; tctx.leaf=~0u; tctx.mainKey=NULL;
			if (rc!=RC_OK) return rc; assert(pageCnt>0);
			if (pageCnt>1) for (unsigned i=1; i<pageCnt; i++) {uint16_t ll; getK(value,indcs[i-1],ll); lkeys+=ll;}
			unsigned lstp=sizeof(SubTreePage)+int(pageCnt-1)*sizeof(SubTreePageKey)+lkeys;
			SubTreePage *stp=(SubTreePage*)alloca(lstp); if (stp==NULL) return RC_NOMEM;
			stp->fSubPage=0xFFFF; stp->level=0; stp->counter=n; stp->leftMost=stp->anchor=pages[0]; unsigned shtkey=lkeys;
			for (unsigned i=pageCnt; --i>0;) {
				SubTreePageKey &spk=((SubTreePageKey*)(stp+1))[i-1]; spk.pageID=pages[i]; const byte *pkey; uint16_t lk;
				pkey=getK(value,indcs[i-1],lk); assert(shtkey>=lk);
				memcpy((byte*)(stp+1)+int(pageCnt-1)*sizeof(SubTreePageKey)+(shtkey-=lk),pkey,lk);
				spk.key.offset=uint16_t(sizeof(SubTreePage)+int(pageCnt-1)*sizeof(SubTreePageKey)+shtkey); spk.key.len=uint16_t(lk);
			}
			value=stp; lValue=uint16_t(lstp); n=(pageCnt-1)|0x8000; assert(shtkey==0);
			lInsert=tp->info.calcEltSize()+tp->info.calcVarSize(*key,lValue,prefixSize);
		}
		if (!tp->isLeaf()) lrec=ceil(lrec,sizeof(PageID));
		else if (tctx.mainKey==NULL && (n&0x8000)==0 && tctx.moreKeys!=NULL && lInsert+lExtra<spaceLeft) {
			const SearchKey *pkey; const void *val; uint16_t lVal; unsigned multi2; bool fPush=false;
			if ((pkey=(SearchKey*)alloca(sizeof(SearchKey)+key->extra()))!=NULL) {*(SearchKey*)pkey=key0; key=pkey;}
			if (pkey!=NULL && (rc=tctx.moreKeys->nextKey(pkey,val,lVal,multi2,true))==RC_OK) {
				if ((multi2>>16)!=0xFFFF && (multi2&0xFFFF)==1) {if (fVM) val=getK(val,uint16_t(multi2>>16),lVal); multi2=0;}
				unsigned idx2=idx; uint16_t prefSize2=prefixSize; size_t lE=lExtra; fPush=true;
				if (lVal<0x8000 && (idx2>=tp->info.nEntries || !tp->findKey(*pkey,idx2) && (!tp->hasSibling()||tp->testKey(*pkey,uint16_t(~0u))<0))) {
					if (tp->info.nEntries==0) {prefSize2=tp->calcPrefixSize(*pkey,0,false,key); lE=0;}
					else if (prefSize2!=0 && (idx2==0 || idx2==tp->info.nEntries) && (prefSize2=tp->calcPrefixSize(*pkey,idx2==0?tp->info.nEntries-1:0))<prefixSize)
						lE=tp->info.extraEltSize(prefSize2)*(tp->info.nEntries+2);
					size_t lInsert2=tp->info.calcEltSize(prefSize2),lKey2=lInsert2+(tp->info.fmt.isFixedLenKey()?0:pkey->extra()-prefSize2); lInsert2+=lInsert+tp->info.calcVarSize(*pkey,lVal,prefSize2);
					if (lInsert2+lE<=spaceLeft && (tp->info.nEntries==0 || idx2!=tp->info.nEntries || lInsert2+lKey2+lE<spaceLeft)) {
						TreeFactory *tf=tctx.tree->getFactory(); uint16_t lFact=tf!=NULL?tf->getParamLength()+2:0; unsigned nk=2;
						lrec=sizeof(TreePageAppend)+2*sizeof(TreePageAppendElt)+key->extLength()+lValue+pkey->extLength()+lVal+lFact; size_t lbuf=max(lrec,size_t(0x2000));
						TreePageAppend *tpa=(TreePageAppend*)alloca(lbuf); Session *ses=NULL;
						if (tpa==NULL && ((ses=Session::getSession())==NULL || (tpa=(TreePageAppend*)ses->malloc(lbuf))==NULL)) return RC_NOMEM;
						TreePageAppendElt *tae=(TreePageAppendElt*)(tpa+1);
						tae->ndata=(uint16_t)n; tae->lkey=key->extLength(); tae->ldata=lValue; tae->idx=(uint16_t)idx; key->serialize(tae+1); memcpy((byte*)(tae+1)+tae->lkey,value,lValue);
						tae=(TreePageAppendElt*)((byte*)(tae+1)+tae->lkey+tae->ldata); tae->ndata=(uint16_t)multi2; tae->idx=(uint16_t)idx2+1;
						tae->lkey=pkey->extLength(); tae->ldata=lVal; pkey->serialize(tae+1); memcpy((byte*)(tae+1)+tae->lkey,val,lVal);
						for (;(rc=tctx.moreKeys->nextKey(pkey,val,lVal,multi2,true))==RC_OK; nk++) {
							if ((multi2>>16)!=0xFFFF && (multi2&0xFFFF)==1) {if (fVM) val=getK(val,uint16_t(multi2>>16),lVal); multi2=0;}
							if (!(lVal<0x8000 && (idx2>=tp->info.nEntries || !tp->findKey(*pkey,idx2) && (!tp->hasSibling()||tp->testKey(*pkey,uint16_t(~0u))<0)))) break;
							uint16_t prefSize3=tp->info.nEntries==0?tp->calcPrefixSize(*pkey,0,false,key):prefSize2!=0 && (idx2==0 || idx2==tp->info.nEntries)?tp->calcPrefixSize(*pkey,idx2==0?tp->info.nEntries-1:0):prefSize2;
							if (prefSize3<prefSize2) lE=tp->info.nEntries!=0?tp->info.extraEltSize(prefSize3)*(tp->info.nEntries+nk+1):(prefSize2-prefSize3)*(nk+1);
							size_t lIns=tp->info.calcEltSize(prefSize3); lKey2=lIns+(tp->info.fmt.isFixedLenKey()?0:pkey->extra()-prefSize3); lInsert2+=lIns+tp->info.calcVarSize(*pkey,lVal,prefSize3);
							if (lInsert2+lE>spaceLeft || tp->info.nEntries!=0 && idx2==tp->info.nEntries && lInsert2+lKey2+lE>=spaceLeft) break;
							size_t nlrec=lrec+sizeof(TreePageAppendElt)+pkey->extLength()+lVal; prefSize2=prefSize3;
							if (nlrec>lbuf) {
								size_t nlbuf=max(nlrec,lbuf*2); ptrdiff_t sht=(byte*)tae-(byte*)tpa;
								if (ses==NULL) {
									void *p; if ((ses=Session::getSession())==NULL || (p=ses->malloc(nlbuf))==NULL) break;
									memcpy(p,tpa,lrec); tpa=(TreePageAppend*)p;
								} else if ((tpa=(TreePageAppend*)ses->realloc(tpa,nlbuf,lbuf))==NULL) return RC_NOMEM;
								lbuf=nlbuf; tae=(TreePageAppendElt*)((byte*)tpa+sht);
							}
							tae=(TreePageAppendElt*)((byte*)(tae+1)+tae->lkey+tae->ldata); tae->ndata=(uint16_t)multi2; tae->idx=(uint16_t)(idx2+nk);
							tae->lkey=pkey->extLength(); tae->ldata=lVal; pkey->serialize(tae+1); memcpy((byte*)(tae+1)+tae->lkey,val,lVal); lrec=nlrec;
						}
						if (rc==RC_OK || rc==RC_EOF) {
							tpa->newPrefixSize=prefSize2; tpa->nKeys=nk; if (rc==RC_OK) tctx.moreKeys->push_back();
							if (tf!=NULL) {byte *p=(byte*)tpa+lrec; tf->getParams(p-lFact,*tctx.tree); p[-2]=byte(lFact); p[-1]=tf->getID();}
							rc=ctx->txMgr->update(tctx.pb,this,idx<<TRO_SHIFT|TRO_APPEND,(byte*)tpa,lrec,tf!=NULL?LRC_LUNDO:0);
						}
						if (ses!=NULL) ses->free(tpa); return rc;
					}
				}
			}
			if (rc==RC_EOF) rc=RC_OK; else if (rc!=RC_OK) return rc; else if (fPush) tctx.moreKeys->push_back();
		}
	} else if (fUnique) return RC_ALREADYEXISTS; else if (fSubTree) return RC_INTERNAL;
	else if (!tp->isLeaf()) {assert(lValue==sizeof(PageID)); return tp->getPageID(idx)!=*(PageID*)value?RC_CORRUPTED:RC_FALSE;}
	else if (tp->info.fmt.isKeyOnly()) {
		if (!tp->info.fmt.isRefKeyOnly()) return RC_FALSE;
		op=TRO_UPDATE; lInsert=lKey; lKey+=key->extra()-prefixSize;
	} else if ((pOld=tp->findData(idx,lOld,&vp))==NULL) return RC_CORRUPTED;
	else if (!fVM) return lOld==lValue && !memcmp(pOld,value,lOld)?RC_FALSE:RC_ALREADYEXISTS;
	else if (tp->isSubTree(*vp)) return modSubTree(tctx,*key,idx,vp,value,lValue,NULL,0,multi);
	else {
		op=TRO_UPDATE; lInsert=lValue; if (!tp->info.fmt.isFixedLenKey()) lKey+=key->extra()-prefixSize;
		if (n==0) {
			if (!tp->info.fmt.isPinRef() && tp->findValue(value,lValue,*vp)!=NULL) return RC_FALSE;
			if (fVM) lInsert+=(vp->len&TRO_MANY)!=0?L_SHT:L_SHT*2;
		}
		const bool fLong=(vp->len&~TRO_MANY)+lInsert>=0x8000;
		if (fLong || tp->info.nSearchKeys<SPAWN_N_THR && lInsert+tp->info.calcEltSize()>unsigned(tp->info.freeSpaceLength+tp->info.scatteredFreeSpace)) {
			RC rc2=spawn(tctx,lInsert,fLong?idx:~0u);
			if (rc2!=RC_OK) {if (rc2!=RC_TOOBIG || fLong) return rc2;}
			else if ((pOld=tp->findData(idx,lOld,&vp))==NULL||vp==NULL) return RC_INTERNAL;
			else if (tp->isSubTree(*vp)) return modSubTree(tctx,*key,idx,vp,value,lValue,NULL,0,multi);
		}
	}

	if (lInsert+lExtra>spaceLeft || tp->info.nEntries!=0 && idx==tp->info.nEntries && lInsert+lExtra+lKey>=spaceLeft) {
		bool fSplit=true;
		if (tp->info.nSearchKeys==1 && !tp->info.fmt.isFixedLenData()) {
			const PagePtr *pp=(const PagePtr*)((byte*)(tp+1)+tp->info.keyLength());
			if (!tp->isSubTree(*pp)) {RC rc2=spawn(tctx,lInsert,0); if (rc2==RC_OK) fSplit=false; else if (rc2!=RC_TOOBIG) return rc2;}
		}
		if (fSplit) {
			bool fInsR; uint16_t splitIdx=tp->calcSplitIdx(fInsR,*key,idx,lInsert,prefixSize,op==TRO_INSERT);
			if ((rc=split(tctx,key,idx,splitIdx,fInsR))!=RC_OK) return rc;		// idx -> tctx.index ??
		}
		tp=(const TreePage*)tctx.pb->getPageBuf(); prefixSize=tp->info.lPrefix;
		if (op==TRO_INSERT && prefixSize!=0 && (idx==0 || idx==tp->info.nEntries)) prefixSize=tp->calcPrefixSize(*key,idx==0?tp->info.nEntries-1:0);
	}
	tctx.parent.release();

	TreeFactory *tf=tctx.tree->getFactory(); uint16_t lFact=tf!=NULL?tf->getParamLength()+2:0,lk=0;
	if (tctx.mainKey!=NULL && tf!=NULL) lFact+=(lk=tctx.mainKey->extLength())+2;
	TreePageModify *tpm=(TreePageModify*)alloca(lrec+lValue+lFact); if (tpm==NULL) return RC_NOMEM;
	key->serialize(tpm+1); tpm->newPrefSize=prefixSize<tp->info.lPrefix?prefixSize:tp->info.lPrefix;
	tpm->newData.offset=uint16_t(lrec); tpm->newData.len=lValue; tpm->oldData.len=0; tpm->oldData.offset=n;
	if (start==0) memcpy((byte*)tpm+tpm->newData.offset,value,lValue);
	else {
		// move one by one
	}
	assert(lrec+lValue<0x10000);
	if (tf!=NULL) {
		byte *p=(byte*)tpm+lrec+lValue+lFact; tf->getParams(p-lFact+lk,*tctx.tree);
		if (tctx.mainKey==NULL) {p[-2]=byte(lFact); p[-1]=tf->getID();}
		else {tctx.mainKey->serialize(p-lFact); p[-4]=byte(lFact>>8); p[-3]=byte(lFact); p[-2]=tf->getID(); p[-1]=0xFF;}
	}
	return ctx->txMgr->update(tctx.pb,this,idx<<TRO_SHIFT|op,(byte*)tpm,lrec+lValue+lFact,tf!=NULL?LRC_LUNDO:0);
}

RC TreePageMgr::update(TreeCtx& tctx,const SearchKey& key,const void *oldValue,uint16_t lOldValue,const void *newValue,uint16_t lNewValue)
{
	const TreePage *tp=(const TreePage*)tctx.pb->getPageBuf();
	if (tp->info.fmt.keyType()!=key.type||newValue==NULL||lNewValue==0||tp->info.fmt.isKeyOnly()) return RC_INVPARAM;

	uint16_t lOld,lExtra=0; const void *pOld; unsigned idx=0; const PagePtr *vp; RC rc=RC_OK; bool fExt=false;
	if (tp->info.nSearchKeys==0 || !tp->findKey(key,idx) || (pOld=tp->findData(idx,lOld,&vp))==NULL) return RC_NOTFOUND;
	if (!tp->info.fmt.isFixedLenData()) {
		assert(vp!=NULL);
		if (oldValue==NULL||lOldValue==0) {if ((vp->len&TRO_MANY)!=0) return RC_INVPARAM;}
		else if (tp->isSubTree(*vp)) fExt=true;
		else if (tp->findValue(oldValue,lOldValue,*vp)==NULL) return RC_NOTFOUND;
		else {pOld=oldValue; lOld=lOldValue;}
	}
	if (!fExt && lNewValue>lOld && lNewValue-lOld+lExtra>tp->info.freeSpaceLength+tp->info.scatteredFreeSpace) {
		if (tp->info.nSearchKeys>=2) {
			bool fInsR; uint16_t splitIdx=tp->calcSplitIdx(fInsR,key,idx,lNewValue-lOld,tp->info.lPrefix);
			if ((rc=split(tctx,&key,idx,splitIdx,fInsR))!=RC_OK) return rc;
			tp=(const TreePage*)tctx.pb->getPageBuf();
		} else if (tp->info.fmt.isFixedLenData()) return RC_TOOBIG;
		else if ((rc=spawn(tctx,lNewValue-lOld+lExtra))!=RC_OK) return rc;
		else if ((pOld=tp->findData(idx,lOld,&vp))==NULL||vp==NULL) return RC_CORRUPTED;
		else fExt=true;
	}
	if (fExt) return modSubTree(tctx,key,idx,vp,newValue,lNewValue,oldValue,lOldValue);
	unsigned lrec=sizeof(TreePageModify)+key.extLength(); tctx.parent.release(); assert(tctx.mainKey==NULL);
	TreeFactory *tf=tctx.tree->getFactory(); uint16_t lFact=tf!=NULL?tf->getParamLength()+2:0;
	TreePageModify *tpm=(TreePageModify*)alloca(lrec+lOld+lNewValue+lFact); if (tpm==NULL) return RC_NOMEM;
	key.serialize(tpm+1); tpm->newPrefSize=tp->info.lPrefix;
	tpm->oldData.len=lOld; tpm->oldData.offset=PageOff(lrec); memcpy((byte*)tpm+lrec,pOld,lOld);
	tpm->newData.len=lNewValue; tpm->newData.offset=PageOff(lrec+lOld); memcpy((byte*)tpm+lrec+lOld,newValue,lNewValue);
	if (tf!=NULL) {byte *p=(byte*)tpm+lrec+lOld+lNewValue+lFact; tf->getParams(p-lFact,*tctx.tree); p[-2]=byte(lFact); p[-1]=tf->getID();}
	return ctx->txMgr->update(tctx.pb,this,idx<<TRO_SHIFT|TRO_UPDATE,(byte*)tpm,lrec+lOld+lNewValue+lFact,tf!=NULL?LRC_LUNDO:0);
}

RC TreePageMgr::edit(TreeCtx& tctx,const SearchKey& key,const void *newValue,uint16_t lNewPart,uint16_t lOldPart,uint16_t sht)
{
	const TreePage *tp=(const TreePage*)tctx.pb->getPageBuf();
	if (tp->info.fmt.keyType()!=key.type||tp->info.fmt.isKeyOnly()) return RC_INVPARAM;

	unsigned idx; uint16_t lOld; const void *pOld; RC rc=RC_OK; const PagePtr *vp=NULL;
	if (tp->info.nSearchKeys==0 || !tp->findKey(key,idx) || (pOld=tp->findData(idx,lOld,&vp))==NULL) return RC_NOTFOUND;
	if (vp!=NULL&&(vp->len&TRO_MANY)!=0||sht+lOldPart>lOld||tp->info.fmt.isFixedLenData()&&lNewPart!=lOldPart) return RC_INVPARAM;
	if (lNewPart>lOldPart && lNewPart-lOldPart>tp->info.freeSpaceLength+tp->info.scatteredFreeSpace) {
		bool fInsR; uint16_t splitIdx=tp->calcSplitIdx(fInsR,key,idx,lNewPart-lOldPart,tp->info.lPrefix);
		if ((rc=split(tctx,&key,idx,splitIdx,fInsR))!=RC_OK) return rc;
		tp=(const TreePage*)tctx.pb->getPageBuf();
	}
	unsigned lrec=(unsigned)ceil(sizeof(TreePageEdit)+key.extLength(),sizeof(uint16_t)); tctx.parent.release();
	TreeFactory *tf=tctx.tree->getFactory(); uint16_t lFact=tf!=NULL?tf->getParamLength()+2:0;
	TreePageEdit *tpe=(TreePageEdit*)alloca(lrec+lOldPart+lNewPart+lFact); if (tpe==NULL) return RC_NOMEM;
	key.serialize(tpe+1); tpe->shift=sht; assert(tctx.mainKey==NULL);
	tpe->oldData.len=lOldPart; tpe->oldData.offset=PageOff(lrec); memcpy((byte*)tpe+lrec,(const byte*)pOld+sht,lOldPart);
	tpe->newData.len=lNewPart; tpe->newData.offset=PageOff(lrec+lOldPart); 
	if (newValue!=NULL) memcpy((byte*)tpe+lrec+lOldPart,newValue,lNewPart);
	if (tf!=NULL) {byte *p=(byte*)tpe+lrec+lOldPart+lNewPart+lFact; tf->getParams(p-lFact,*tctx.tree); p[-2]=byte(lFact); p[-1]=tf->getID();}
	return ctx->txMgr->update(tctx.pb,this,idx<<TRO_SHIFT|TRO_EDIT,(byte*)tpe,lrec+lOldPart+lNewPart+lFact,tf!=NULL?LRC_LUNDO:0);
}

RC TreePageMgr::remove(TreeCtx& tctx,const SearchKey& key,const void *value,uint16_t lValue,unsigned multi)
{
	const TreePage *tp=(const TreePage*)tctx.pb->getPageBuf();
	if (tp->info.fmt.keyType()!=key.type||multi!=0&&tp->info.fmt.isFixedLenData()) return RC_INVPARAM;

	unsigned op=TRO_DELETE; uint16_t lOld=0,n=multi!=0&&value!=NULL&&lValue>0?multi&0xFFFF:0;
	const void *pOld=NULL; unsigned idx=0; const PagePtr *vp=NULL;
	if (tp->info.nSearchKeys==0 || !tp->findKey(key,idx) ||
		!tp->info.fmt.isKeyOnly() && (pOld=tp->findData(idx,lOld,&vp))==NULL) return RC_NOTFOUND;
	
	bool fMerge=(tctx.tree->mode&TF_WITHDEL)!=0 && tp->info.nSearchKeys==1;
	if (!tp->info.fmt.isFixedLenData()) {
		assert(vp!=NULL);
		if (tp->isSubTree(*vp)) {
			assert(lOld>=sizeof(SubTreePage));
			if (value!=NULL && lValue!=0) return modSubTree(tctx,key,idx,vp,NULL,0,value,lValue,multi);
			SubTreePage tpe; memcpy(&tpe,(byte*)pOld,sizeof(SubTreePage)); RC rc; n=0x8000; 
			if (tpe.leftMost!=INVALID_PAGEID && (rc=Tree::drop(tpe.leftMost,ctx))!=RC_OK) return rc;
			if (lOld>sizeof(SubTreePage)) {n+=(lOld-sizeof(SubTreePage))/sizeof(SubTreePageKey); lOld=tp->length(*vp);}
		} else if (value!=NULL && lValue!=0) {
			if (tp->findValue(value,lValue,*vp)==NULL) {
				return RC_NOTFOUND;
			}
			// check if last! if not -> fMerge=false;
			op=TRO_UPDATE; pOld=value; lOld=lValue;
		} else if ((vp->len&TRO_MANY)!=0) {
			lOld=tp->length(*vp); n=(vp->len&~TRO_MANY)/L_SHT;
		}
	}
	if (!fMerge) tctx.parent.release();

	unsigned lrec=sizeof(TreePageModify)+key.extLength(); assert(tctx.mainKey==NULL);
	TreeFactory *tf=tctx.tree->getFactory(); uint16_t lFact=tf!=NULL?tf->getParamLength()+2:0;
	TreePageModify *tpm=(TreePageModify*)alloca(lrec+lOld+lFact); if (tpm==NULL) return RC_NOMEM;
	if (pOld!=NULL && lOld!=0) {
		if (vp==NULL || (vp->len&TRO_MANY)==0 || (n&~0x8000)==0) memcpy((byte*)tpm+lrec,pOld,lOld);
		else if (value!=NULL && lValue!=0) {
			assert((n&0x8000)==0);
			//...
		} else if ((n&0x8000)==0) {
			uint16_t sht=n*L_SHT,ll; byte *p=(byte*)pOld,*ptr=(byte*)tpm+lrec,*const p0=ptr,*const end=ptr+sht;
			for (; ptr<end; ptr+=L_SHT,p+=L_SHT) {const byte *q=getK(tp,p,ll); _set_u16(ptr,sht); memcpy(p0+sht,q,ll); sht+=ll;}
		} else {
			uint16_t sht=vp->len&~TRO_MANY; memcpy((byte*)tpm+lrec,pOld,sht); const byte *p=(byte*)tpm+lrec+sizeof(SubTreePage);
			for (unsigned i=0,j=n&~0x8000; i<j; i++,p+=sizeof(SubTreePageKey)) {
				PagePtr pp=__una_get(((SubTreePageKey*)p)->key); memcpy((byte*)tpm+lrec+sht,(byte*)tp+pp.offset,pp.len);
				_set_u16(&((SubTreePageKey*)p)->key.offset,sht); sht+=pp.len;
			}
		}
	}
	key.serialize(tpm+1); tpm->newPrefSize=tp->info.lPrefix;
	tpm->newData.len=0; tpm->newData.offset=n; tpm->oldData.len=lOld; tpm->oldData.offset=PageOff(lrec);
	if (tf!=NULL) {byte *p=(byte*)tpm+lrec+lOld+lFact; tf->getParams(p-lFact,*tctx.tree); p[-2]=byte(lFact); p[-1]=tf->getID();}
	RC rc=ctx->txMgr->update(tctx.pb,this,idx<<TRO_SHIFT|op,(byte*)tpm,lrec+lOld+lFact,tf!=NULL?LRC_LUNDO:0);
	if (rc==RC_OK && fMerge) {
		if (tctx.depth!=0) {
			PBlockP spb(tctx.pb); //bool fPost=true;
			if (tctx.getParentPage(key,PTX_FORUPDATE|PTX_FORDELETE|PTX_TRY)==RC_OK) {
//				fPost=!addNewPage(tctx,tps->newKey,tps->newSibling);
#if 0
				TreePageMgr::TreePage *tp=(TreePageMgr::TreePage*)tctx.pb->getPageBuf(); unsigned pos;
				if (tp->info.stamp!=tctx.stamps[PITREE_1STLEV] || tp->getChild(key,pos,false)!=pid) fPost=false;
				else {
					assert(tp->info.level>0);
					PageID lpid=pos==0||pos==~0u?tp->info.leftMost:tp->getPageID(pos-1);
					PBlock *left=ctx->bufMgr->getPage(lpid,ctx->trpgMgr,PGCTL_XLOCK),*right;
					const TreePageMgr::TreePage *ltp=(const TreePageMgr::TreePage*)left->getPageBuf();
					if (ltp->info.sibling==pid && (right=ctx->bufMgr->getPage(pid,ctx->trpgMgr,PGCTL_XLOCK))!=NULL &&
																(rc=ctx->trpgMgr->merge(left,right,tctx.pb,*this))==RC_OK) {
						fPost=false; advanceStamp(ltp->info.level==0?PITREE_LEAF:PITREE_INT);
						if (tp->info.nSearchKeys==0) {
							// post!!!
							if (tp->info.leftMost!=INVALID_PAGEID) {
								//...
							} else {
								//...
							}
						}
					}
					left->release();
				}
				if (!tctx.pb.isNull()) tctx.stack[tctx.depth]=tctx.pb->getPageID();
#endif
			}
			spb.moveTo(tctx.pb); tctx.depth++;
//			if (fPost) tctx.postPageOp(tps->newKey,tps->newSibling,true);
//		} else if ((rc2=tctx.tree.removeRootPage(tctx.pb->getPageID(),tp->info.leftMost,0))==RC_OK) {		//???
			// ???tctx.stack[0]=tps->newSibling; tctx.depth++;
		}
	}
	return rc;
}

RC TreePageMgr::merge(PBlock *left,PBlock *right,PBlock *par,Tree& tr,const SearchKey& key,unsigned idx)
{
	if (right->isDependent()) {right->release(QMGR_UFORCE); return RC_FALSE;}
	const TreePage *ltp=(const TreePage*)left->getPageBuf(),*rtp=(const TreePage*)right->getPageBuf(),*ptp=(const TreePage*)par->getPageBuf();
	assert(ltp->info.sibling==right->getPageID()&&ltp->info.nEntries==ltp->info.nSearchKeys+1);
	unsigned fSize=ltp->info.freeSpaceLength+ltp->info.scatteredFreeSpace+rtp->info.freeSpaceLength+rtp->info.scatteredFreeSpace;
	uint16_t lPrefix=0,lKey=key.extLength(); PBlockP rpp(right,QMGR_UFORCE);
	if (rtp->info.nEntries==0) lPrefix=ltp->info.nSearchKeys==1?ltp->calcPrefixSize(0,1):ltp->info.lPrefix;
	else if (ltp->info.nEntries>1 && ltp->info.lPrefix>0) {
		if (rtp->info.lPrefix>0) {
			uint16_t lkey=rtp->getKeySize(rtp->info.nEntries-1);
			SearchKey *key=(SearchKey*)alloca(sizeof(SearchKey)+lkey);
			if (key==NULL) return RC_NOMEM;
			rtp->getKey(rtp->info.nEntries-1,*key);
			lPrefix=ltp->calcPrefixSize(*key,0);
			if (lPrefix<rtp->info.lPrefix) {
				unsigned lext=rtp->info.extraEltSize(lPrefix)*rtp->info.nEntries;
				if (lext>fSize) return RC_FALSE; fSize-=lext;
			}
		}
		if (lPrefix<ltp->info.lPrefix) {
			unsigned lext=ltp->info.extraEltSize(lPrefix)*(ltp->info.nEntries-1);
			if (lext>fSize) return RC_FALSE; fSize-=lext;
		}
	} else if (rtp->info.lPrefix>0) {
		if (ltp->info.nEntries<=1) lPrefix=rtp->info.lPrefix;
		else {
			unsigned lext=rtp->info.extraEltSize(0)*rtp->info.nEntries;
			if (lext>fSize) return RC_FALSE; fSize-=lext;
		}
	}
	if (fSize<xSize) return RC_FALSE;
	size_t lrec=sizeof(TreePageSplit)+lKey+sizeof(TreePageInfo);
	if (rtp->info.nEntries>0) lrec+=xSize-(rtp->info.freeSpaceLength+rtp->info.scatteredFreeSpace);
	TreePageModify *tpm=(TreePageModify*)alloca(max(lrec,size_t(sizeof(TreePageModify)+lKey+sizeof(PageID))));
	if (tpm==NULL) return RC_NOMEM;
	MiniTx mtx(NULL,(tr.mode&TF_SPLITINTX)!=0?MTX_SKIP:0); RC rc=RC_OK;
	if (idx!=~0u) {
#ifdef _DEBUG
		unsigned idx2; assert(ptp->findKey(key,idx2) && idx==idx2);
#endif
		key.serialize(tpm+1); tpm->newPrefSize=ptp->info.lPrefix; tpm->newData.len=0; tpm->newData.offset=0;
		tpm->oldData.len=sizeof(PageID); tpm->oldData.offset=PageOff(sizeof(TreePageModify)+lKey);
		memcpy((byte*)tpm+tpm->oldData.offset,&rtp->hdr.pageID,sizeof(PageID));
		rc=ctx->txMgr->update(par,this,idx<<TRO_SHIFT|TRO_DELETE,(byte*)tpm,sizeof(TreePageModify)+lKey+sizeof(PageID));
	}
	if (rc==RC_OK) {
		TreePageSplit *tps=(TreePageSplit*)tpm;
		tps->nEntriesLeft=ltp->info.nEntries; tps->newSibling=rtp->hdr.pageID;
		tps->oldPrefSize=lPrefix; key.serialize(tps+1);
		byte *p=(byte*)(tps+1)+lKey; memcpy(p,&rtp->info,sizeof(TreePageInfo));
		if (rtp->info.nEntries>0) {
			if (rtp->info.scatteredFreeSpace!=0) ((TreePage*)rtp)->compact();
			unsigned ll=rtp->info.nEntries*rtp->info.calcEltSize();
			assert(lrec>=sizeof(TreePageSplit)+lKey+sizeof(TreePageInfo)+ll);
			memcpy(p+sizeof(TreePageInfo),rtp+1,ll);
			memcpy(p+sizeof(TreePageInfo)+ll,(byte*)rtp+rtp->info.freeSpace,lrec-sizeof(TreePageSplit)-lKey-sizeof(TreePageInfo)-ll);
		}
		rc=ctx->txMgr->update(left,this,TRO_MERGE,(byte*)tps,lrec,0,right); assert(!right->isDependent());
		if (rc==RC_OK) {PageID pid=rtp->hdr.pageID; rpp=NULL; right->release(PGCTL_DISCARD|QMGR_UFORCE); ctx->fsMgr->freePage(pid); mtx.ok();}
	}
	return rc;
}

RC TreePageMgr::drop(PBlock* &pb,TreeFreeData *dd)
{
	const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
	if ((!tp->info.fmt.isFixedLenData()||dd!=NULL) && tp->info.nSearchKeys>0) {
		uint16_t lElt=tp->info.calcEltSize(),sht=tp->info.keyLength();
		const byte *p=(const byte*)(tp+1); RC rc;
		for (uint16_t i=0; i<tp->info.nSearchKeys; ++i,p+=lElt)
			if (dd!=NULL) {if ((rc=dd->freeData(p+sht,ctx))!=RC_OK) return rc;}
			else {
				const PagePtr *vp=(const PagePtr*)(p+sht);
				if (tp->isSubTree(*vp)) {
					assert((vp->len&~TRO_MANY)>=sizeof(SubTreePage) && ((vp->len&~TRO_MANY)-sizeof(SubTreePage))%sizeof(SubTreePageKey)==0);
					SubTreePage tpe; memcpy(&tpe,(const byte*)tp+vp->offset,sizeof(SubTreePage));
					if (tpe.leftMost!=INVALID_PAGEID && (rc=Tree::drop(tpe.leftMost,ctx))!=RC_OK) return rc;
				}
			}
	}
	size_t lrec=xSize-tp->info.freeSpaceLength+sizeof(TreePageInfo);
	byte *rec=(byte*)alloca(lrec); if (rec==NULL) return RC_NOMEM;
	memcpy(rec,&tp->info,sizeof(TreePageInfo));
	if (tp->info.nEntries>0) {
		unsigned ll=tp->info.nEntries*tp->info.calcEltSize(); memcpy(rec+sizeof(TreePageInfo),tp+1,ll);
		memcpy(rec+sizeof(TreePageInfo)+ll,(byte*)tp+tp->info.freeSpace,lrec-sizeof(TreePageInfo)-ll);
	}
	PageID pid=pb->getPageID(); Session *ses=Session::getSession();
	ctx->logMgr->insert(ses,LR_DISCARD,TRO_DROP<<PGID_SHIFT|PGID_INDEX,pid,NULL,rec,lrec);
	pb->release(PGCTL_DISCARD|QMGR_UFORCE,ses); pb=NULL; return ctx->fsMgr->freePage(pid);
}

void TreePageMgr::initPage(byte *frame,size_t len,PageID pid)
{
	static const IndexFormat defaultFormat(KT_ALL,0,0);
	TxPage::initPage(frame,len,pid);
	TreePage *tp=(TreePage*)frame;
	tp->info.fmt			= defaultFormat;
	tp->info.nEntries		= 0;
	tp->info.level			= 0;
	tp->info.flags			= 0;
	tp->info.prefix			= 0;
	tp->info.sibling		= INVALID_PAGEID;
	tp->info.leftMost		= INVALID_PAGEID;
	tp->info.stamp			= 0;
	tp->info.nSearchKeys	= 0;
	tp->info.filler			= 0;
	tp->info.initFree(len);
}

bool TreePageMgr::afterIO(PBlock *pb,size_t lPage,bool fLoad)
{
	return TxPage::afterIO(pb,lPage,fLoad)?((TreePage*)pb->getPageBuf())->checkPage(false):false;
}

bool TreePageMgr::beforeFlush(byte *frame,size_t len,PageID pid)
{
	return ((TreePage*)frame)->checkPage(true)?TxPage::beforeFlush(frame,len,pid):false;
}

PGID TreePageMgr::getPGID() const
{
	return PGID_INDEX;
}

unsigned TreePageMgr::enumPages(PageID pid)
{
	unsigned cnt=0; PBlock *pb=NULL;
	while (pid!=INVALID_PAGEID && (pb=ctx->bufMgr->getPage(pid,this,0,pb))!=NULL) {
		const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
		pid=tp->isLeaf()?INVALID_PAGEID:tp->info.leftMost;
		for (;;tp=(const TreePageMgr::TreePage*)pb->getPageBuf()) {
			++cnt;
			if (tp->isLeaf() && !tp->info.fmt.isFixedLenData()) {
				uint16_t lElt=tp->info.calcEltSize(),sht=tp->info.keyLength(); const byte *p=(const byte*)(tp+1);
				for (uint16_t i=0; i<tp->info.nSearchKeys; ++i,p+=lElt) {
					const PagePtr *vp=(const PagePtr*)(p+sht);
					if (tp->isSubTree(*vp)) {
						assert((vp->len&~TRO_MANY)>=sizeof(SubTreePage) && ((vp->len&~TRO_MANY)-sizeof(SubTreePage))%sizeof(SubTreePageKey)==0);
						SubTreePage tpe; memcpy(&tpe,(const byte*)tp+vp->offset,sizeof(SubTreePage)); cnt+=enumPages(tpe.leftMost);
					}
				}
			}
			if (!tp->hasSibling() || (pb=ctx->bufMgr->getPage(tp->info.sibling,this,0,pb))==NULL) break;
		}
	}
	if (pb!=NULL) pb->release();
	return cnt;
}

unsigned TreePageMgr::checkTree(PageID pid,PageID sib,unsigned depth,CheckTreeReport& out,CheckTreeReport *sec,bool fLM)
{
	assert(depth<TREE_MAX_DEPTH); unsigned cnt=0; PBlock *pb=NULL; PageID pid2,sib2; uint16_t i;
	while ((pb=ctx->bufMgr->getPage(pid,ctx->trpgMgr,0,pb))!=NULL) {
		const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf(); 
		if (depth+1>out.depth) out.depth=depth+1; cnt++; out.total[depth]++;
		if (!tp->isLeaf()) {
			if (tp->info.nSearchKeys==0) {out.empty[depth]++; pid2=fLM?tp->info.leftMost:INVALID_PAGEID;}
			else for (pid2=fLM?tp->info.leftMost:tp->getPageID(0),i=fLM?0:1; i<tp->info.nSearchKeys; ++i,pid2=sib2,fLM=false)
				cnt+=checkTree(pid2,sib2=tp->getPageID(i),depth+1,out,sec,fLM);
			if (pid2!=INVALID_PAGEID) {
				sib2=INVALID_PAGEID; PBlock *pb2=NULL; const TreePageMgr::TreePage *tp2=tp;
				while (tp2->hasSibling() && (pb2=ctx->bufMgr->getPage(tp->info.sibling,ctx->trpgMgr,0,pb2))!=NULL)
					if ((tp2=(const TreePageMgr::TreePage*)pb2->getPageBuf())->info.nSearchKeys>0) {sib2=tp2->getPageID(0); break;}
				if (pb2!=NULL) pb2->release(); cnt+=checkTree(pid2,sib2,depth+1,out,sec,false);
			}
		} else {
			double prc=double(xSize-tp->info.freeSpaceLength-tp->info.scatteredFreeSpace)/xSize; 
			out.histogram[prc>=1.?TREE_N_HIST_BUCKETS-1:unsigned(prc*TREE_N_HIST_BUCKETS)]++;
			if (tp->info.nSearchKeys==0) out.empty[depth]++;
			else if (!tp->info.fmt.isFixedLenData()) {
				uint16_t lElt=tp->info.calcEltSize(),sht=tp->info.keyLength(); const byte *p=(const byte*)(tp+1);
				for (uint16_t i=0; i<tp->info.nSearchKeys; ++i,p+=lElt) {
					const PagePtr *vp=(const PagePtr*)(p+sht);
					if (tp->isSubTree(*vp)) {
						assert((vp->len&~TRO_MANY)>=sizeof(SubTreePage) && ((vp->len&~TRO_MANY)-sizeof(SubTreePage))%sizeof(SubTreePageKey)==0);
						SubTreePage tpe; memcpy(&tpe,(const byte*)tp+vp->offset,sizeof(SubTreePage));
						PageID pid2=tpe.leftMost,sib2; const byte *p1=(byte*)tp+vp->offset+sizeof(SubTreePage);
						unsigned n=((vp->len&~TRO_MANY)-sizeof(SubTreePage))/sizeof(SubTreePageKey);
						CheckTreeReport &rep=sec!=NULL?*sec:out;
						for (unsigned i=0; i<n; ++i,p1+=sizeof(SubTreePageKey),pid2=sib2) {
							SubTreePageKey tpk; memcpy(&tpk,p1,sizeof(SubTreePageKey));
							sib2=tpk.pageID; cnt+=checkTree(pid2,sib2,0,rep,NULL,i==0);
						}
						cnt+=checkTree(pid2,INVALID_PAGEID,0,rep,NULL,n==0);
					}
				}
			}
		}
		if (!tp->hasSibling() || tp->info.sibling==sib) {pb->release(); break;}
		out.missing[depth]++; pid=tp->info.sibling; fLM=false;
	}
	return cnt;
}

//----------------------------------------------------------------------------------------------

PageID TreeCtx::startPage(const SearchKey *key,int& level,bool fRead,bool fBefore)
{
	return mainKey!=NULL?TreePageMgr::startSubPage((const TreePageMgr::TreePage*)parent->getPageBuf(),vp,key,level,fRead,fBefore):
																						tree->startPage(key,level,fRead,fBefore);
}

PageID TreeCtx::prevStartPage(PageID pid)
{
	return mainKey!=NULL?TreePageMgr::prevStartSubPage((const TreePageMgr::TreePage*)parent->getPageBuf(),vp,pid):tree->prevStartPage(pid);
}

void TreeCtx::getStamps(unsigned stamps[TREE_NODETYPE_ALL]) const
{
	if (mainKey==NULL) tree->getStamps(stamps);
	else {stamps[PITREE_LEAF2]=tree->getStamp(PITREE_LEAF2); stamps[PITREE_INT2]=tree->getStamp(PITREE_INT2);}
}

//----------------------------------------------------------------------------------------------

bool TreePageMgr::TreePage::checkPage(bool fWrite) const
{
	const char *what;
#if 0
	unsigned maxFree=xSize;
	if (freeSpaceLength>maxFree) what="invalid freeSpaceLength";
	else if (scatteredFreeSpace>=maxFree-sizeof(PageOff)) what="invalid scatteredFreeSpace";
	else if (nSlots*sizeof(PageOff)>=maxFree) what="invalid nSlots";
	else if (freeSpace-sizeof(HeapPage)+freeSpaceLength!=maxFree-nSlots*sizeof(PageOff)) 
		what="invalid freeSpace+freeSpaceLength";
	else if (scatteredFreeSpace!=0 && scatteredFreeSpace>=freeSpace-sizeof(HeapPage)) 
		what="scatteredFreeSpace>=freeSpace";
#endif
	if (!info.fmt.isValid(hdr.length(),info.level)) what="invalid page type";
	else {
		// ???
		return true;
	}
	report(MSG_ERROR,"TreePage %08X corrupt %s: %s\n",hdr.pageID,fWrite?"before write":"after read",what);
	return false;
}

int TreePageMgr::TreePage::testKey(const SearchKey& key,uint16_t idx,bool fPrefix) const
{
	assert(info.fmt.keyType()==key.type && (idx==uint16_t(~0u)||idx<info.nEntries));
	if (info.fmt.isSeq()) return cmp3(key.v.u,info.prefix+(idx==uint16_t(~0u)?info.nEntries:idx));
	const byte *ptr=(const byte*)(this+1)+(idx==uint16_t(~0u)?info.nEntries-1:idx)*info.calcEltSize(); uint16_t lk=sizeof(uint64_t)-info.lPrefix,lsib; uint64_t u64=info.lPrefix!=0?info.prefix:0;
	switch (key.type) {
	case KT_UINT: assert(lk<=sizeof(uint64_t)); return cmp3(key.v.u,u64|(lk==sizeof(uint16_t)?*(uint16_t*)ptr:lk==sizeof(uint32_t)?*(uint32_t*)ptr:lk==sizeof(uint64_t)?*(uint64_t*)ptr:0));
	case KT_INT: assert(lk<=sizeof(uint64_t)); u64|=lk==sizeof(uint16_t)?*(uint16_t*)ptr:lk==sizeof(uint32_t)?*(uint32_t*)ptr:lk==sizeof(uint64_t)?*(uint64_t*)ptr:0; return cmp3(key.v.i,int64_t(u64));
	case KT_FLOAT: assert(info.lPrefix==0); return cmp3(key.v.f,*(float*)ptr);
	case KT_DOUBLE: assert(info.lPrefix==0); return cmp3(key.v.d,*(double*)ptr);
	default: break;
	}
	assert(!key.isNumKey());
	const byte *pkey=(const byte*)key.getPtr2(),*pref=NULL; lk=key.v.ptr.l; int res;
	if (info.lPrefix>0) {
		pref=info.fmt.isFixedLenKey()?(const byte*)&info.prefix:(const byte*)this+((PagePtr*)&info.prefix)->offset; assert(key.type!=KT_REF);
		if (key.type==KT_VAR) {
			//if ((res=cmpMSegPrefix(pkey,lkey,pref,info.lPrefix,&lsib))!=0) return res;
			//if (lsib!=0) {pkey+=lkey-lsib; lkey=lsib;} else return fPrefix?0:-1;
		} else {
			if ((res=memcmp(pkey,pref,min(lk,info.lPrefix)))!=0) return res;
			if (lk>info.lPrefix) {pkey+=info.lPrefix; lk-=info.lPrefix;} else return fPrefix?0:-1;
		}
	}
	if (info.fmt.isKeyOnly()) ptr=getK(ptr,lsib);
	else if (!info.fmt.isFixedLenKey()) {lsib=((PagePtr*)ptr)->len; ptr=(byte*)this+((PagePtr*)ptr)->offset;}
	else {assert(info.fmt.keyLength()>=info.lPrefix); lsib=info.fmt.keyLength()-info.lPrefix;}
	if (key.type==KT_VAR) return cmpMSeg(pkey,lk,ptr,lsib);
	if (key.type==KT_REF) return PINRef::cmpPIDs(pkey,ptr);
	res=(lk|lsib)==0?0:memcmp(pkey,ptr,min(lk,lsib));
	return res!=0||fPrefix&&lk<=lsib?res:cmp3(lk,lsib);
}

void TreePageMgr::TreePage::checkKeys() const
{
#ifdef _DEBUG
	if (info.fmt.isSeq()) return;
	uint16_t lElt=info.calcEltSize(); const byte *key1=(const byte *)(this+1),*key2=key1+lElt;
	int c; const byte *s1,*s2; uint16_t l1,l2;
	for (unsigned i=1; i<(unsigned)info.nEntries; ++i,key1+=lElt,key2+=lElt) {
		switch (info.fmt.keyType()) {
		default: assert(0);
		case KT_UINT:
			if (info.lPrefix==sizeof(uint32_t)+sizeof(uint16_t)) assert(*(uint16_t*)key1<*(uint16_t*)key2);
			else if (info.lPrefix==sizeof(uint32_t)) assert(*(uint32_t*)key1<*(uint32_t*)key2);
			else assert(*(uint64_t*)key1<*(uint64_t*)key2);
			break;
		case KT_INT:
			if (info.lPrefix==sizeof(int32_t)+sizeof(int16_t)) assert(*(int16_t*)key1<*(int16_t*)key2);
			else if (info.lPrefix==sizeof(int32_t)) assert(*(int32_t*)key1<*(int32_t*)key2);
			else assert(*(int64_t*)key1<*(int64_t*)key2);
			break;
		case KT_FLOAT: assert(*(float*)key1<*(float*)key2); break;
		case KT_DOUBLE: assert(*(double*)key1<*(double*)key2); break;
		case KT_BIN:
			if (info.fmt.isFixedLenKey()) {s1=key1; s2=key2; l1=l2=info.fmt.keyLength()-info.lPrefix;} else {s1=(byte*)this+((PagePtr*)key1)->offset; l1=((PagePtr*)key1)->len; s2=(byte*)this+((PagePtr*)key2)->offset; l2=((PagePtr*)key2)->len;}
			c=memcmp(s1,s2,min(l1,l2)); assert(c<0||c==0&&l1<l2); break;
		case KT_VAR: 
		case KT_REF:
			if (info.fmt.isKeyOnly()) {s1=getK(key1,l1); s2=getK(key2,l2);} else {s1=(byte*)this+((PagePtr*)key1)->offset; l1=((PagePtr*)key1)->len; s2=(byte*)this+((PagePtr*)key2)->offset; l2=((PagePtr*)key2)->len;}
			if (info.fmt.keyType()==KT_VAR) assert(cmpMSeg(s1,l1,s2,l2)<0); else assert(PINRef::cmpPIDs(s1,s2)<0);
		}
	}
#endif
}

bool TreePageMgr::TreePage::testBound(const SearchKey& key,uint16_t idx,const IndexSeg *sg,unsigned nSegs,bool fStart,bool fPrefix) const
{
	assert(info.fmt.keyType()==key.type && (idx==uint16_t(~0u)||idx<info.nEntries));
	if (info.fmt.keyType()==KT_VAR) {
		const PagePtr *pp=(const PagePtr*)((byte*)(this+1)+(idx==uint16_t(~0u)?info.nEntries-1:idx)*info.calcEltSize());
		return cmpBound((const byte *)key.getPtr2(),key.v.ptr.l,(byte*)this+pp->offset,pp->len,sg,nSegs,fStart);
	}
	int c=testKey(key,idx,fPrefix || sg!=NULL && (sg->flags&SCAN_PREFIX)!=0);
	return fStart?c<0||c==0&&(sg==NULL||(sg->flags&SCAN_EXCLUDE_START)==0):c>0||c==0&&(sg==NULL||(sg->flags&SCAN_EXCLUDE_END)==0);
}

template<typename T> bool TreePageMgr::TreePage::findNumKey(T sKey,unsigned nEnt,unsigned& pos) const {
	const T *keys=(const T *)(this+1);
	for (unsigned n=nEnt,base=0; n>0 ;) {
		unsigned k=n>>1; T key=keys[pos=base+k];
		if (key==sKey) return true; if (key>sKey) n=k; else {base+=k+1; n-=k+1; pos++;}
	}
	return false;
}

template<typename T> bool TreePageMgr::TreePage::findNumKeyVar(T sKey,unsigned nEnt,unsigned& pos) const {
	const static unsigned lElt=(sizeof(T)+1&~1)+sizeof(PagePtr)+sizeof(T)-1&~(sizeof(T)-1);
	for (unsigned n=nEnt,base=0; n>0 ;) {
		unsigned k=n>>1; T key=*(T*)((byte*)(this+1)+(pos=base+k)*lElt);
		if (key==sKey) return true; if (key>sKey) n=k; else {base+=k+1; n-=k+1; pos++;}
	}
	return false;
}

bool TreePageMgr::TreePage::findKey(const SearchKey& skey,unsigned& pos) const
{
	assert(info.fmt.keyType()==skey.type && (isLeaf() || info.fmt.isFixedLenData() && info.fmt.dataLength()==sizeof(PageID)));
	unsigned nEnt=info.nSearchKeys; 
	if (nEnt==0) pos=0;
	else if (!info.fmt.isFixedLenKey()) {
		const byte *pkey=(const byte*)skey.getPtr2(),*pref=NULL; int cmp;
		uint16_t lkey=skey.v.ptr.l;
		if (info.lPrefix>0) {
			pref=(byte*)this+((PagePtr*)&info.prefix)->offset; assert(skey.type!=KT_REF);
			if (skey.type==KT_VAR) {
//				cmp=cmpMSegPrefix(pkey,lkey,pref,info.lPrefix,&ll);
//				if (cmp<0) {pos=0; return false;} else if (cmp>0) {pos=nEnt; return false;}
//				else if (ll==0) {pos=0; return info.nEntries==1;}
//				pkey+=lkey-ll; lkey=ll;
			} else {
				cmp=memcmp(pkey,pref,min(lkey,info.lPrefix));
				if (cmp<0 || cmp==0 && lkey<info.lPrefix) {pos=0; return false;} else if (cmp>0) {pos=nEnt; return false;}
				pkey+=info.lPrefix; lkey-=info.lPrefix;
			}
		}
		const bool fSub=info.fmt.isKeyOnly();
		for (unsigned n=nEnt,base=0,lElt=info.calcEltSize(); n>0; ) {
			unsigned k=n>>1; const byte *pk; uint16_t lk; pos=base+k;
			if (fSub) pk=getK(base+k,lk); else {const PagePtr *pp=(PagePtr*)((byte*)(this+1)+lElt*(base+k)); pk=(byte*)this+pp->offset; lk=pp->len;}
			cmp=skey.type==KT_VAR?cmpMSeg(pkey,lkey,pk,lk):skey.type==KT_REF?PINRef::cmpPIDs(pkey,pk):
				(cmp=lkey==0||lk==0?0:memcmp(pkey,pk,min(lkey,lk)))==0?int(lkey)-int(lk):cmp;
			if (cmp==0) return true; else if (cmp<0) n=k; else {base+=k+1; n-=k+1; pos++;}
		}
	} else {
		bool fFixed=info.fmt.isFixedLenData(); uint16_t lkey; const void *pkey;
		if (info.fmt.isSeq()) {
			if (skey.v.u<info.prefix) pos=0;
			else if (skey.v.u>=info.prefix+info.nEntries) pos=info.nEntries;
			else {pos=(unsigned)(skey.v.u-info.prefix); return true;}
		} else switch (skey.type) {
		case KT_UINT:
			if (info.lPrefix!=0 && ((skey.v.u^info.prefix)&~0ULL<<(sizeof(uint64_t)-info.lPrefix)*8)!=0) pos=skey.v.u<info.prefix?0:nEnt;
			else switch (info.lPrefix) {
			default: assert(0);
			case 0: return fFixed?findNumKey(skey.v.u,nEnt,pos):findNumKeyVar(skey.v.u,nEnt,pos);
			case sizeof(uint32_t): return fFixed?findNumKey((uint32_t)skey.v.u,nEnt,pos):findNumKeyVar((uint32_t)skey.v.u,nEnt,pos);
			case sizeof(uint32_t)+sizeof(uint16_t): return fFixed?findNumKey((uint16_t)skey.v.u,nEnt,pos):findNumKeyVar((uint16_t)skey.v.u,nEnt,pos);
			case sizeof(uint64_t): assert(info.nEntries==1); pos=0; return true;
			}
			break;
		case KT_INT:
			if (info.lPrefix!=0 && ((skey.v.u^info.prefix)&~0ULL<<(sizeof(uint64_t)-info.lPrefix)*8)!=0) {
				// check neg in 0 prefix
				pos=skey.v.i<(int64_t)info.prefix?0:nEnt;
			} else switch (info.lPrefix) {
			default: assert(0);
			case 0: return fFixed?findNumKey(skey.v.i,nEnt,pos):findNumKeyVar(skey.v.i,nEnt,pos);
			case sizeof(uint32_t): return fFixed?findNumKey((int32_t)skey.v.i,nEnt,pos):findNumKeyVar((int32_t)skey.v.i,nEnt,pos);
			case sizeof(uint32_t)+sizeof(uint16_t): return fFixed?findNumKey((int16_t)skey.v.i,nEnt,pos):findNumKeyVar((int16_t)skey.v.i,nEnt,pos);
			case sizeof(uint64_t): assert(info.nEntries==1); pos=0; return true;
			}
			break;
		case KT_FLOAT: return fFixed?findNumKey(skey.v.f,nEnt,pos):findNumKeyVar(skey.v.f,nEnt,pos);
		case KT_DOUBLE: return fFixed?findNumKey(skey.v.d,nEnt,pos):findNumKeyVar(skey.v.d,nEnt,pos);
		default:
			lkey=info.fmt.keyLength(); pkey=skey.getPtr2(); assert(lkey==skey.v.ptr.l&&skey.type==KT_BIN);
			if (info.lPrefix>0) {
				assert(info.lPrefix<=lkey); int cmp=memcmp(pkey,&info.prefix,info.lPrefix);
				if (cmp<0) {pos=0; return false;} else if (cmp>0) {pos=nEnt; return false;}
				if ((lkey-=info.lPrefix)==0) {assert(nEnt==1); pos=0; return true;}
				pkey=(const byte*)pkey+info.lPrefix; 
			}
			for (unsigned n=nEnt,base=0,lElt=info.calcEltSize(); n>0; ) {
				unsigned k=n>>1; int cmp=memcmp(pkey,(const byte*)(this+1)+(pos=base+k)*lElt,lkey);
				if (cmp==0) return true; if (cmp<0) n=k; else {base+=k+1; n-=k+1; pos++;}
			}
			break;
		}
	}
	return false;
}

bool TreePageMgr::TreePage::findSubKey(const byte *pkey,uint16_t lkey,unsigned& pos) const
{
	if (info.nEntries==0) {pos=0; return false;}
	const bool fPR=info.fmt.isRefKeyOnly(); assert(info.fmt.isKeyOnly()); 
	const uint16_t *p0,*p=p0=(const uint16_t*)(this+1);
	for (unsigned n=info.nEntries;;) {
		unsigned k=n>>1; int cmp=0;
		if (fPR) cmp=PINRef::cmpPIDs((byte*)p0+((uint16_t*)p)[k],pkey); 
		else {uint16_t l; const byte *pk=TreePageMgr::getK(p,k,l); if ((cmp=memcmp(pk,pkey,min(l,lkey)))==0) cmp=cmp3(l,lkey);}
		if (cmp==0) {pos=uint16_t(p-p0)+k; return true;}
		if (cmp>0) {if ((n=k)==0) {pos=uint16_t(p-p0); return false;}}
		else if ((n-=k+1)==0) {pos=uint16_t(p-p0)+k+1; return false;} else p+=k+1;
	}
}

bool TreePageMgr::TreePage::findSubKey(const byte *tab,unsigned nElts,unsigned idx,unsigned& res) const
{
	assert(res<nElts && idx<info.nEntries && info.fmt.isKeyOnly());
	const bool fPR=info.fmt.isRefKeyOnly(); uint16_t lk0; const byte *p=getK(idx,lk0);
	for (unsigned base=res,n=nElts-base,k=n-1; n!=0; k=n>>1) {
		int cmp; uint16_t lk; const byte *pk=TreePageMgr::getK(tab,base+k,lk);
		if (!fPR) {
			uint16_t ll=min(lk,lk0); cmp=memcmp(pk,p,ll);
			if (cmp==0) {if (ll<lk0) cmp=-1; else if (lk>lk0) cmp=1; else {res=base+k; return true;}}
		} else if ((cmp=PINRef::cmpPIDs(pk,p))==0) {res=base+k; return true;}
		if (cmp<0) {res=base+=k+1; n-=k+1;} else n=k;
	}
	return false;
}

void *TreePageMgr::TreePage::findData(unsigned idx,uint16_t& lData,const PagePtr **pPtr) const
{
	assert(isLeaf()&&!info.fmt.isKeyOnly()); if (pPtr!=NULL) *pPtr=NULL;
	bool fFixedData=info.fmt.isFixedLenData(); if (fFixedData) lData=info.fmt.dataLength();
	
	if (info.fmt.isSeq()) {
		assert(idx<info.nEntries); 
		byte *pData=(byte*)(this+1)+(fFixedData?lData:sizeof(PagePtr))*idx;
		if (fFixedData) return pData; else if (pPtr!=NULL) *pPtr=(PagePtr*)pData;
		lData=((PagePtr*)pData)->len&~TRO_MANY; return (byte*)this+((PagePtr*)pData)->offset;
	}

	assert(idx<info.nSearchKeys); 
	if (fFixedData) return info.fmt.isFixedLenKey() ? (byte*)this + info.freeSpace + (info.nSearchKeys-idx-1)*lData :
				(byte*)(this+1) + idx*(sizeof(PagePtr)+(uint16_t)ceil(lData,sizeof(uint16_t))) + sizeof(PagePtr);

	const PagePtr *p=(const PagePtr*)((byte*)(this+1)+idx*info.calcEltSize()+info.keyLength()); 
	if (p->offset<sizeof(TreePage)) return NULL; if (pPtr!=NULL) *pPtr=(PagePtr*)p;
	lData=p->len&~TRO_MANY; return (byte*)this+p->offset;
}

byte *TreePageMgr::TreePage::findValue(const void *val,uint16_t lv,const PagePtr& vp,uint16_t *pins) const 
{
	assert(!info.fmt.isFixedLenData() && !isSubTree(vp));
	return (byte*)TreePageMgr::findValue(val,lv,(byte*)this,(byte*)this+vp.offset,vp.len,info.fmt.isPinRef(),pins);
}

const byte *TreePageMgr::findValue(const void *val,uint16_t lv,const byte *frame,const byte *ps,uint16_t ls,bool fPR,uint16_t *ins)
{
	if (ls==0) {if (ins!=NULL) *ins=0; return NULL;}
	if ((ls&TRO_MANY)!=0) {
		const byte *p=ps,*q; uint16_t l,sht; int cmp;
		for (unsigned n=(ls&~TRO_MANY)/L_SHT;;) {
			unsigned k=n>>1; q=p+k*L_SHT; sht=_u16(q);
			if (fPR) cmp=PINRef::cmpPIDs(frame+sht,(byte*)val); 
			else {l=PINRef::len(frame+sht); if ((cmp=memcmp(frame+sht,val,min(l,lv)))==0) cmp=cmp3(l,lv);}
			if (cmp==0) {if (ins!=NULL) *ins=uint16_t(q-ps); return q;}
			if (cmp>0) {if ((n=k)==0) {if (ins!=NULL) *ins=uint16_t(p-ps); return NULL;}}
			else if ((n-=k+1)==0) {if (ins!=NULL) *ins=uint16_t(q-ps)+L_SHT; return NULL;}
			else p=q+L_SHT;
		}
	} else {
		int cmp; if (ins!=NULL) *ins=0;
		if (fPR) cmp=PINRef::cmpPIDs(ps,(byte*)val); else if ((cmp=memcmp(ps,val,min(ls,lv)))==0) cmp=cmp3(ls,lv);
		if (cmp<0 && ins!=NULL) *ins=L_SHT; return cmp==0?(byte*)ps:NULL;
	}
}

const TreePageMgr::SubTreePageKey *TreePageMgr::TreePage::findExtKey(const void *key,size_t lkey,const SubTreePageKey *tpk,unsigned n,uint16_t *poff) const
{
	const byte *p=(const byte*)tpk; if (n==0) {if (poff!=NULL) *poff=0; return NULL;}
	for (const bool fPR=info.fmt.isPinRef();;) {
		unsigned k=n>>1; const byte *q=p+k*sizeof(SubTreePageKey);
		SubTreePageKey tkey; memcpy(&tkey,q,sizeof(SubTreePageKey)); int cmp;
		if (fPR) cmp=PINRef::cmpPIDs((byte*)this+tkey.key.offset,(byte*)key);
		else if ((cmp=memcmp((byte*)this+tkey.key.offset,key,min(tkey.key.len,uint16_t(lkey))))==0) cmp=cmp3(tkey.key.len,(uint16_t)lkey);
		if (cmp==0) {if (poff!=NULL) *poff=uint16_t(q-(const byte*)tpk); return (const SubTreePageKey*)q;}
		if (cmp>0) {if ((n=k)==0) {if (poff!=NULL) *poff=uint16_t(p-(const byte*)tpk); return NULL;}}
		else if ((n-=k+1)==0) {if (poff!=NULL) *poff=uint16_t(q-(const byte*)tpk+sizeof(SubTreePageKey)); return NULL;}
		else p=q+sizeof(SubTreePageKey);
	}
}

void TreePageMgr::TreePage::compact(bool fCheckOnly,unsigned extIdx,uint16_t lext)
{
	assert(!info.fmt.isFixedLenData() || !info.fmt.isFixedLenKey());
	{Session *ses=Session::getSession(); if (ses!=NULL) ses->setCodeTrace(0x80);}

	size_t lPage=hdr.length();
	TreePage *page=(TreePage*)alloca(lPage); memcpy(page,this,sizeof(TreePage));
	page->info.initFree(lPage); page->info.lPrefix=info.lPrefix;

#ifdef _DEBUG
	if (fCheckOnly) {
		if (info.nEntries==0) assert(info.lPrefix==0); else if (!info.fmt.isSeq()) assert(calcPrefixSize(0,info.nEntries)==info.lPrefix);
	}
#endif
	if (info.nEntries!=0) {
		bool fVarKey=!info.fmt.isFixedLenKey(),fVarData=!info.fmt.isFixedLenData();
		if (info.lPrefix>0 && fVarKey) page->store((byte*)this+((PagePtr*)&info.prefix)->offset,info.lPrefix,*(PagePtr*)&page->info.prefix);
		if (info.fmt.isKeyOnly()) {
			page->info.freeSpaceLength-=info.nEntries*L_SHT;
			for (uint16_t i=0; i<info.nEntries; ) {
				uint16_t beg=uint16_t(~0u),end,s=i;
				do {
					uint16_t sht=((uint16_t*)(this+1))[i],ll;
					if (sht!=0) {ll=PINRef::len((byte*)(this+1)+sht); if (beg==uint16_t(~0u)) end=(beg=sht)+ll; else if (sht==end) end+=ll; else if (sht+ll==beg) beg=sht; else break;}
				} while (++i<info.nEntries);
				int dl=(int)page->store((byte*)(this+1)+beg,end-beg)-(int)beg; assert(s<i);
				do {uint16_t sht=((uint16_t*)(this+1))[s]; ((uint16_t*)(page+1))[s]=sht!=0?uint16_t((int)sht+dl):0;} while (++s<i);
			}
#ifdef _DEBUG
			if (fCheckOnly) {
				const bool fPR=info.fmt.isRefKeyOnly();
				for (ushort i=0; i+1<info.nEntries; i++) {
					if (fPR) {assert(PINRef::cmpPIDs((*page)[i],(*page)[i+1])<0);}
					else {uint16_t l1,l2; const byte *p1=page->getK(i,l1),*p2=page->getK(i+1,l2); int cmp=memcmp(p1,p2,min(l1,l2)); assert(cmp<0 || cmp==0 && l1<=l2);}
				}
			}
#endif
		} else {
			uint16_t lElt=info.calcEltSize(),sht=info.keyLength();
			byte *from=(byte*)(this+1),*to=(byte*)(page+1); 
			uint16_t lMove=lElt*info.nEntries; assert(lMove<page->info.freeSpaceLength);
			memcpy(to,from,lMove); page->info.freeSpaceLength-=lMove; 
			unsigned nEnt=info.fmt.isSeq()?info.nEntries:info.nSearchKeys;
			for (unsigned idx=0; idx<nEnt; ++idx,from+=lElt,to+=lElt) {
				if (fVarKey) page->store((byte*)this+((PagePtr*)from)->offset,((PagePtr*)from)->len,*(PagePtr*)to);
				if (fVarData) {
					page->store((byte*)this+((PagePtr*)(from+sht))->offset,((PagePtr*)(from+sht))->len,*(PagePtr*)(to+sht));
					if (idx==extIdx) {
						if (isSubTree(*(PagePtr*)(from+sht))) lext=sizeof(SubTreePageKey);
						else if (info.fmt.isFixedLenData()) lext=info.fmt.dataLength();
						assert(page->info.freeSpaceLength>=lext); lMove=((PagePtr*)(to+sht))->len&~TRO_MANY;
						page->info.freeSpace-=lext; page->info.freeSpaceLength-=lext;
						memmove((byte*)page+page->info.freeSpace,(byte*)page+page->info.freeSpace+lext,lMove);
						((PagePtr*)(to+sht))->offset=page->info.freeSpace; ((PagePtr*)(to+sht))->len+=lext;
					}
					if ((((PagePtr*)(to+sht))->len&TRO_MANY)!=0) page->moveMulti(this,*(PagePtr*)(from+sht),*(PagePtr*)(to+sht));
				}
			}
			if (fVarKey && hasSibling()) page->store((byte*)this+((PagePtr*)from)->offset,((PagePtr*)from)->len,*(PagePtr*)to);
		}
		assert(page->info.freeSpaceLength+lext==info.freeSpaceLength+info.scatteredFreeSpace);
	}
	if (!fCheckOnly) memcpy(&info,&page->info,lPage-FOOTERSIZE-sizeof(TxPageHeader));
}

void TreePageMgr::TreePage::changePrefixSize(uint16_t prefLen)
{
	size_t lPage=hdr.length(); assert(prefLen!=info.lPrefix && !info.fmt.isSeq());
	TreePage *page=(TreePage*)alloca(lPage); copyEntries(page,prefLen,0);
	memcpy(&info,&page->info,page->info.nEntries!=0?lPage-FOOTERSIZE-sizeof(TxPageHeader):sizeof(TreePageInfo));
}

void TreePageMgr::TreePage::copyEntries(TreePage *page,uint16_t prefLen,uint16_t start) const
{
	{Session *ses=Session::getSession(); if (ses!=NULL) ses->setCodeTrace(0x100);}
	memcpy(&page->info,&info,sizeof(TreePageInfo)); assert(start<=info.nEntries);
	page->info.initFree(hdr.length()); page->info.lPrefix=prefLen;
	page->info.nEntries=info.nEntries-start; page->info.nSearchKeys=info.nSearchKeys<start?0:info.nSearchKeys-start;

	bool fVarKey=!info.fmt.isFixedLenKey();

	if (info.fmt.isSeq()) {assert(prefLen==0&&info.lPrefix==0); page->info.prefix=info.prefix+start;}
	if (page->info.nEntries==0) {page->info.lPrefix=0; return;}

	assert(prefLen<=info.lPrefix || info.nEntries>0);
	uint16_t lElt=info.calcEltSize(); const byte *startElt=(const byte*)(this+1)+start*lElt;
	if (fVarKey && !info.fmt.isKeyOnly()) {
		if (prefLen>info.lPrefix) page->store((byte*)this+((PagePtr*)startElt)->offset,prefLen-info.lPrefix,*(PagePtr*)&page->info.prefix);
		page->store((byte*)this+((PagePtr*)&info.prefix)->offset,min(info.lPrefix,prefLen),*(PagePtr*)&page->info.prefix);
		((PagePtr*)&page->info.prefix)->len=prefLen;
	} else if (info.fmt.isPrefNumKey()) {
		uint64_t v=info.lPrefix>0?info.prefix:0;
		switch (sizeof(uint64_t)-info.lPrefix) {
		default: assert(0);
		case 0: v=info.prefix; break;
		case sizeof(uint16_t): v|=*(uint16_t*)startElt; break;
		case sizeof(uint32_t): v|=*(uint32_t*)startElt; break;
		case sizeof(uint64_t): v=*(uint64_t*)startElt; break;
		}
		page->info.prefix=v&~0ULL<<(sizeof(uint64_t)-prefLen)*8;
	} else {
		assert(prefLen<=sizeof(page->info.prefix));
		if (prefLen>info.lPrefix) memcpy((byte*)&page->info.prefix+info.lPrefix,startElt,prefLen-info.lPrefix);
	}

	byte *pref=NULL; uint16_t delta=0;
	if (info.lPrefix<prefLen) delta=prefLen-info.lPrefix;
	else if (info.lPrefix>prefLen && !info.fmt.isPrefNumKey()) {
		assert(!info.fmt.isNumKey()); delta=info.lPrefix-prefLen;
		pref=fVarKey?(byte*)this+((PagePtr*)&info.prefix)->offset+prefLen:(byte*)&info.prefix+prefLen;
	}
	bool fVarData=!info.fmt.isFixedLenData();
	if (info.fmt.isKeyOnly()) {
		page->info.freeSpaceLength-=(info.nEntries-start)*L_SHT;
		for (uint16_t i=start; i<info.nEntries; ) {
			uint16_t beg=uint16_t(~0u),end,s=i;
			do {
				uint16_t sht=((uint16_t*)(this+1))[i],ll;
				if (sht!=0) {ll=PINRef::len((byte*)(this+1)+sht); if (beg==uint16_t(~0u)) end=(beg=sht)+ll; else if (sht==end) end+=ll; else if (sht+ll==beg) beg=sht; else break;}
			} while (++i<info.nEntries);
			int dl=(int)page->store((byte*)(this+1)+beg,end-beg)-(int)beg; assert(s<i);
			do {uint16_t sht=((uint16_t*)(this+1))[s]; ((uint16_t*)(page+1))[s-start]=sht!=0?uint16_t((int)sht+dl):0;} while (++s<i);
		}
	} else if (fVarKey) {
		const PagePtr *from=(PagePtr*)startElt; PagePtr *to=(PagePtr*)(page+1); 
		uint16_t lMove=(info.nEntries-start)*lElt; memcpy(to,from,lMove); page->info.freeSpaceLength-=lMove;
		for (uint16_t idx=start; idx<info.nEntries; idx++) {
			if (info.lPrefix<prefLen) {
				assert(from->len>=delta); page->store((byte*)this+from->offset+delta,from->len-delta,*to);
			} else {
				page->store((byte*)this+from->offset,from->len,*to); 
				if (info.lPrefix>prefLen) {page->store(pref,delta,*to); to->len=from->len+delta;}
			}
			if (fVarData && idx<info.nSearchKeys) {
				page->store((byte*)this+from[1].offset,from[1].len,to[1]);
				if ((to[1].len&TRO_MANY)!=0) page->moveMulti(this,from[1],to[1]);
			}
			from=(PagePtr*)((byte*)from+lElt); to=(PagePtr*)((byte*)to+lElt);
		}
	} else {
		if (!fVarData) {
			uint16_t lMove=(info.nEntries-start)*info.fmt.dataLength(); 
			assert(lMove<page->info.freeSpaceLength && info.scatteredFreeSpace==0);
			page->info.freeSpace-=lMove; page->info.freeSpaceLength-=lMove;
			memcpy((byte*)page+page->info.freeSpace,(byte*)this+info.freeSpace,lMove);
		}
		uint16_t lkey=info.fmt.keyLength(),lEltTo=page->info.calcEltSize(); assert(lkey>=info.lPrefix && lkey>=prefLen);
		uint16_t lkOld=lkey-info.lPrefix,lkNew=lkey-prefLen; uint64_t mask=0;
		if (info.lPrefix>prefLen && info.fmt.isPrefNumKey())		
			mask=info.prefix<<(sizeof(uint64_t)-lkNew)>>(sizeof(uint64_t)-lkNew+lkOld)<<lkOld;
		uint16_t sht=(uint16_t)ceil(lkOld,sizeof(short)),shtTo=(uint16_t)ceil(lkNew,sizeof(short));
		assert(lEltTo*(info.nEntries-start)<=page->info.freeSpaceLength);
		page->info.freeSpaceLength-=lEltTo*(info.nEntries-start);
		const byte *from=startElt; byte *to=(byte*)(page+1); 
		for (uint16_t idx=start; idx<info.nEntries; ++idx,from+=lElt,to+=lEltTo) {
			if (info.fmt.isPrefNumKey()) {
				uint64_t ukey=0;
				switch (lkOld) {
				case 0: assert(info.nEntries<=1); ukey=mask; break;
				case sizeof(uint16_t): ukey=*(uint16_t*)from|mask; break;
				case sizeof(uint32_t): ukey=*(uint32_t*)from|mask; break;
				case sizeof(uint64_t): ukey=*(uint64_t*)from|mask; break;
				default: assert(0);
				}
				switch (lkNew) {
				case 0: assert(page->info.nEntries<=1); break;
				case sizeof(uint16_t): *(uint16_t*)to=uint16_t(ukey); break;
				case sizeof(uint32_t): *(uint32_t*)to=uint32_t(ukey); break;
				case sizeof(uint64_t): *(uint64_t*)to=ukey; break;
				default: assert(0);
				}
			} else if (prefLen>info.lPrefix) memcpy(to,from+delta,lkNew);
			else {if (delta>0) memcpy(to,pref,delta); memcpy(to+delta,from,lkOld);}
			if (fVarData && idx<info.nSearchKeys) {
				page->store((byte*)this+((PagePtr*)(from+sht))->offset,((PagePtr*)(from+sht))->len,*((PagePtr*)(to+shtTo)));
				if ((((PagePtr*)(to+shtTo))->len&TRO_MANY)!=0) page->moveMulti(this,*(PagePtr*)(from+sht),*(PagePtr*)(to+shtTo));
			}
		}
	}
}

RC TreePageMgr::TreePage::prefixFromKey(const void *pk,uint16_t prefSize)
{
	const byte *key=(const byte *)pk;
	if (info.fmt.isPrefNumKey()) {
		memcpy(&info.prefix,key+1,sizeof(uint64_t)); info.lPrefix=min(prefSize,(uint16_t)sizeof(uint64_t));
		if (prefSize<sizeof(uint64_t)) info.prefix&=~0ULL<<(sizeof(uint64_t)-prefSize)*8;
	} else if (info.fmt.keyType()==KT_VAR) {
		//???
	} else if (!info.fmt.isNumKey() && info.fmt.keyType()!=KT_REF) {
		if (*key<KT_BIN) return RC_CORRUPTED; uint16_t lk=min(SearchKey::keyLenP(key),prefSize);
		if (info.freeSpaceLength<lk) {compact(); if (info.freeSpaceLength<lk) return RC_CORRUPTED;}
		if (!info.fmt.isFixedLenKey()) {store(key,lk,*(PagePtr*)&info.prefix); info.lPrefix=lk;}
		else memcpy(&info.prefix,key,info.lPrefix=min(lk,(uint16_t)sizeof(info.prefix)));
	}
	return RC_OK;
}

uint16_t TreePageMgr::TreePage::calcPrefixSize(const SearchKey &key,unsigned idx,bool fExt,const SearchKey *pk) const
{
	if (info.fmt.isPrefNumKey()) {
		uint64_t u=pk!=NULL?pk->v.u:key.v.u;
		if (info.nEntries!=0) {
			const byte *ptr=(const byte*)(this+1)+info.calcEltSize()*min(idx,unsigned(info.nEntries-1)); 
			switch (info.lPrefix) {
			default: assert(0);
			case sizeof(uint64_t): u=info.prefix; break;
			case sizeof(uint32_t)+sizeof(uint16_t): u=*(uint16_t*)ptr|info.prefix; break;
			case sizeof(uint32_t): u=*(uint32_t*)ptr|info.prefix; break;
			case 0: u=*(uint64_t*)ptr; break;
			}
		}
		u^=key.v.u;
		if (key.type==KT_INT) {
			//...
		}
		return uint32_t(u>>32)!=0?0:(uint32_t(u)&0xFFFF0000)!=0?sizeof(uint32_t):sizeof(uint32_t)+sizeof(uint16_t);
	}
	if (info.fmt.isNumKey()||info.fmt.keyType()>=KT_REF||info.fmt.isKeyOnly()) return 0;
	const byte *pkey=(const byte*)key.getPtr2(); uint16_t lCmp;
	if (info.nEntries==0) {
		if (pk==NULL) return 0;
		const byte *pkey2=pk->getPtr2(); lCmp=min(key.v.ptr.l,pk->v.ptr.l);
		for (uint16_t i=0; i<lCmp; i++) if (*pkey++!=*pkey2++) return i;
		return lCmp;
	}
	if (info.lPrefix>0) {
		const byte *p=info.fmt.isFixedLenKey()?(const byte*)&info.prefix:(const byte*)this+((PagePtr*)&info.prefix)->offset;
		uint16_t l=min(info.lPrefix,key.v.ptr.l); for (uint16_t i=0; i<l; i++) if (*pkey++!=*p++) {l=i; break;}
		if (!fExt||l<info.lPrefix) return l;
	}
	const byte *ptr=(const byte*)(this+1)+info.calcEltSize()*min(idx,unsigned(info.nEntries-1));
	if (info.fmt.isFixedLenKey()) {
		lCmp=info.fmt.keyLength()-info.lPrefix; assert(info.lPrefix<=sizeof(info.prefix));
		if (size_t(lCmp+info.lPrefix)>sizeof(info.prefix) && (lCmp=sizeof(info.prefix)-info.lPrefix)==0) return info.lPrefix;
	} else {
		lCmp=min(((PagePtr*)ptr)->len,uint16_t(key.v.ptr.l-info.lPrefix));
		ptr=(const byte*)this+((PagePtr*)ptr)->offset;
	}
	for (uint16_t i=0; i<lCmp; i++) if (*ptr++!=*pkey++) {lCmp=i; break;}
	return lCmp+info.lPrefix;
}

uint16_t TreePageMgr::TreePage::calcPrefixSize(unsigned start,unsigned end) const
{
	if (info.fmt.keyType()>=KT_REF||info.fmt.isKeyOnly()||info.fmt.isNumKey()&&!info.fmt.isPrefNumKey()) return 0;
	if (start+1>=end) {
		uint16_t l=getKeySize(uint16_t(start));
		return info.fmt.isFixedLenKey() && l>sizeof(info.prefix)?sizeof(info.prefix):l;
	}
	uint16_t lElt=info.calcEltSize(),lCmp; assert(start<end);
	const byte *p1=(byte*)(this+1)+start*lElt,*p2=(byte*)(this+1)+(end-1)*lElt;
	if (info.fmt.isPrefNumKey()) switch (info.lPrefix) {
	default: assert(0);
	case sizeof(uint64_t): //????
	case sizeof(uint32_t)+sizeof(uint16_t): return info.lPrefix;
	case sizeof(uint32_t):
		{uint32_t t=*(uint32_t*)p1^*(uint32_t*)p2; return (t&0xFFFF0000)!=0?info.lPrefix:info.lPrefix+sizeof(uint16_t);}
	case 0:
		assert(info.lPrefix==0);
		{uint64_t t=*(uint64_t*)p1^*(uint64_t*)p2; return uint32_t(t>>32)!=0?0:(uint32_t(t)&0xFFFF0000)!=0?sizeof(uint32_t):sizeof(uint32_t)+sizeof(uint16_t);}
	}
	if (info.fmt.isFixedLenKey()) {
		lCmp=info.fmt.keyLength()-info.lPrefix; assert(info.lPrefix<=sizeof(info.prefix));
		if (size_t(lCmp+info.lPrefix)>sizeof(info.prefix) && (lCmp=sizeof(info.prefix)-info.lPrefix)==0) return info.lPrefix;
	} else {
		lCmp=min(((PagePtr*)p1)->len,((PagePtr*)p2)->len);
		p1=(const byte*)this+((PagePtr*)p1)->offset; p2=(const byte*)this+((PagePtr*)p2)->offset;
	}
	for (uint16_t i=0; i<lCmp; ++p1,i++) if (*p1!=*p2++) {lCmp=i; break;}
	return lCmp+info.lPrefix;
}

uint16_t TreePageMgr::TreePage::getKeySize(uint16_t idx) const
{
	if (info.nEntries==0) return 0; if (idx==uint16_t(~0u)) idx=info.nEntries-1; assert(idx<info.nEntries);
	return info.fmt.isKeyOnly() ? PINRef::len((*this)[idx]) : info.fmt.isSeq() ? 0 : info.fmt.isFixedLenKey() ? info.fmt.keyLength() : 
		((PagePtr*)((byte*)(this+1)+idx*(sizeof(PagePtr)+(info.fmt.isFixedLenData()? (uint16_t)ceil(info.fmt.dataLength(),sizeof(uint16_t)):sizeof(PagePtr)))))->len+info.lPrefix;
}

void TreePageMgr::TreePage::getKey(uint16_t idx,SearchKey& key) const
{
	key.loc=SearchKey::PLC_EMB;
	if (info.fmt.isSeq()) {key.v.u=info.prefix+(idx==uint16_t(~0u)?info.nEntries:idx); key.type=KT_UINT; return;}
	if (idx==uint16_t(~0u)) {if (info.nEntries==0) {key.type=KT_ALL; return;} else idx=info.nEntries-1;} assert(idx<info.nEntries);
	const byte *p=(const byte*)(this+1)+idx*info.calcEltSize(); bool fFixed; uint16_t lkey;
	switch (key.type=info.fmt.keyType()) {
	case KT_UINT: case KT_INT:
		switch (info.lPrefix) {
		case 0: key.v.u=*(uint64_t*)p; break;
		case sizeof(uint32_t): key.v.u=*(uint32_t*)p|info.prefix; break;
		case sizeof(uint32_t)+sizeof(uint16_t): key.v.u=*(uint16_t*)p|info.prefix; break;
		case sizeof(uint64_t): key.v.u=info.prefix; break;
		}
		break;
	case KT_FLOAT: key.v.f=*(float*)p; break;;
	case KT_DOUBLE: key.v.d=*(double*)p; break;
	default:
		fFixed=info.fmt.isFixedLenKey(); assert(!info.fmt.isNumKey());
		if (info.lPrefix>0) memcpy((byte*)(&key+1),fFixed?(const byte*)&info.prefix:(const byte*)this+((PagePtr*)&info.prefix)->offset,info.lPrefix);
		if (fFixed) lkey=info.fmt.keyLength()-info.lPrefix;
		else if (info.fmt.isKeyOnly()) p=getK(p,lkey);
		else {lkey=((PagePtr*)p)->len; p=(const byte*)this+((PagePtr*)p)->offset;}
		memcpy((byte*)(&key+1)+info.lPrefix,p,lkey); key.v.ptr.p=NULL; key.v.ptr.l=info.lPrefix+lkey; key.loc=SearchKey::PLC_SPTR;
	}
}

void TreePageMgr::TreePage::serializeKey(uint16_t idx,void *buf) const
{
	byte *pb=(byte*)buf; uint64_t u64;
	if (info.fmt.isSeq()) {u64=info.prefix+(idx==uint16_t(~0u)?info.nEntries:idx); *pb=KT_UINT; memcpy(pb+1,&u64,sizeof(uint64_t)); return;}
	if (idx==uint16_t(~0u)) {if (info.nEntries==0) {*pb=KT_ALL; return;} else idx=info.nEntries-1;}
	assert(idx<info.nEntries); *pb=info.fmt.keyType();
	const byte *p=(const byte*)(this+1)+idx*info.calcEltSize();
	if (info.fmt.isPrefNumKey() && info.lPrefix!=0) {
		switch (info.lPrefix) {
		default: assert(0);
		case sizeof(uint64_t): u64=info.prefix; break;
		case sizeof(uint32_t)+sizeof(uint16_t): u64=*(uint16_t*)p|info.prefix; break;
		case sizeof(uint32_t): u64=*(uint32_t*)p|info.prefix; break;
		}
		memcpy(pb+1,&u64,sizeof(uint64_t)); return;
	}
	if (*pb<KT_BIN) {memcpy(pb+1,p,SearchKey::extKeyLen[*pb]); return;}
	const bool fFixed=info.fmt.isFixedLenKey(); uint16_t lkey; assert(!info.fmt.isNumKey()); 
	if (fFixed) lkey=info.fmt.keyLength()-info.lPrefix;
	else if (info.fmt.isKeyOnly()) p=getK(p,lkey);
	else {lkey=((PagePtr*)p)->len; p=(const byte*)this+((PagePtr*)p)->offset;}
	uint16_t l=lkey+info.lPrefix; if (l<128) pb[1]=byte(l); else {*++pb=byte(l|0x80); pb[1]=byte(l>>7);}
	if (info.lPrefix>0) memcpy(pb+2,fFixed?(const byte*)&info.prefix:(const byte*)this+((PagePtr*)&info.prefix)->offset,info.lPrefix);
	memcpy(pb+2+info.lPrefix,p,lkey);
}

uint32_t TreePageMgr::TreePage::getKeyU32(uint16_t idx) const {
	assert(idx<info.nEntries && info.fmt.keyType()==KT_UINT);
	const byte *p=(const byte*)(this+1)+idx*info.calcEltSize();
	switch (info.lPrefix) {
	default: assert(0); return ~0u;
	case sizeof(uint64_t): return (uint32_t)info.prefix;
	case sizeof(uint32_t)+sizeof(uint16_t): return (uint32_t)(*(uint16_t*)p|(uint32_t)info.prefix);
	case sizeof(uint32_t): return *(uint32_t*)p;
	case 0: return (uint32_t)*(uint64_t*)p;
	}
}

void TreePageMgr::TreePage::storeKey(const void *pk,void *ptr)
{
	const byte *key=(const byte*)pk; uint16_t lk,lkey; uint64_t u;
	switch (*key) {
	default:
		lk=SearchKey::keyLenP(key); lkey=lk-info.lPrefix; assert(lk>=info.lPrefix);
		if (info.fmt.isFixedLenKey()) memcpy(ptr,key+info.lPrefix,lkey);
		else {
			if (lkey>info.freeSpaceLength) {compact(); assert(lkey<=info.freeSpaceLength);}
			if (info.fmt.isKeyOnly()) *(uint16_t*)ptr=store(key,lkey);
			else if (lkey==0) ((PagePtr*)ptr)->len=((PagePtr*)ptr)->offset=0;
			else store(key+info.lPrefix,lkey,*(PagePtr*)ptr);
		}
		break;
	case KT_UINT: case KT_INT:
		memcpy(&u,key+1,sizeof(uint64_t));
		switch (info.lPrefix) {
		default: assert(0);
		case 0: *(uint64_t*)ptr=u; break;
		case sizeof(uint32_t): *(uint32_t*)ptr=uint32_t(u); break;
		case sizeof(uint32_t)+sizeof(uint16_t): *(uint16_t*)ptr=uint16_t(u); break;
		case sizeof(uint64_t): break;
		}
		break;
	case KT_FLOAT: memcpy(ptr,key+1,sizeof(float)); break;
	case KT_DOUBLE: memcpy(ptr,key+1,sizeof(double)); break;
	}
}

uint16_t TreePageMgr::TreePage::length(const PagePtr& ptr) const
{
	assert(!info.fmt.isKeyOnly()&&!info.fmt.isFixedLenData());
	if ((ptr.len&TRO_MANY)==0) return ptr.len;
	uint16_t l=ptr.len&~TRO_MANY;
	if (!isSubTree(ptr)) {
		assert(l%L_SHT==0);
		for (const uint16_t *p=(uint16_t*)((byte*)this+ptr.offset),*e=p+l/L_SHT; p<e; p++) l+=PINRef::len((byte*)this+_u16(p));
	} else {
		assert((l-sizeof(SubTreePage))%sizeof(SubTreePageKey)==0);
		const byte *p=(byte*)this+ptr.offset+sizeof(SubTreePage);
		for (int i=(l-sizeof(SubTreePage))/sizeof(SubTreePageKey); --i>=0; p+=sizeof(SubTreePageKey)) 
			l+=__una_get(((SubTreePageKey*)p)->key.len);
	}
	return l;
}

void TreePageMgr::TreePage::moveMulti(const TreePage *src,const PagePtr& from,PagePtr& to)
{
	uint16_t l=from.len&~TRO_MANY;
	if (src->isSubTree(from)) {
		assert((to.len&~TRO_MANY)>=l && (l-sizeof(SubTreePage))%sizeof(SubTreePageKey)==0);
		if (l>sizeof(SubTreePage)) {
			const byte *pf=(const byte*)src+from.offset+sizeof(SubTreePage);
			byte *pt=(byte*)this+to.offset+sizeof(SubTreePage); PagePtr ppf,ppt; 
			for (int i=(l-sizeof(SubTreePage))/sizeof(SubTreePageKey); --i>=0; pf+=sizeof(SubTreePageKey),pt+=sizeof(SubTreePageKey)) {
				ppf=__una_get(((SubTreePageKey*)pf)->key); store((const byte*)src+ppf.offset,ppf.len,ppt); __una_set(((SubTreePageKey*)pt)->key,ppt);
			}
		}
	} else if ((from.len&TRO_MANY)!=0) {
		assert(l%L_SHT==0 && !info.fmt.isFixedLenData());
		for (uint16_t i=0; i<l; ) {
			uint16_t beg=uint16_t(~0u),end,s=i;
			do {
				uint16_t sht=_u16((byte*)src+from.offset+i),ll;
				if (sht!=0) {ll=PINRef::len((byte*)src+sht); if (beg==uint16_t(~0u)) end=(beg=sht)+ll; else if (sht==end) end+=ll; else if (sht+ll==beg) beg=sht; else break;}
			} while ((i+=L_SHT)<l);
			PagePtr pp; store((byte*)src+beg,end-beg,pp); assert(s<i); int dl=(int)pp.offset-(int)beg;
			do {uint16_t sht=_u16((byte*)src+from.offset+s); if (sht!=0) _set_u16((byte*)this+to.offset+s,uint16_t((int)sht+dl));} while ((s+=L_SHT)<i);
		}
	}
}

bool IndexFormat::isValid(size_t lPage,uint16_t level) const
{
	if (isFixedLenKey()) {uint16_t lKey=keyLength(); if (lKey==0 && !isSeq() || lKey>lPage/4) return false;}
	if (level>0) {if (!isFixedLenData() || dataLength()!=sizeof(PageID)) return false;}
	else if (isFixedLenData()) {uint16_t lData=dataLength(); if (lData>lPage/4) return false;}
	// ...
	return true;
}

//-----------------------------------------------------------------------------------------------------------------------------------------------

SubTreeInit::~SubTreeInit()
{
	while (depth--!=0) stack[depth]->release(QMGR_UFORCE);
	if (buf!=NULL) ma->free(buf);
}

RC SubTreeInit::insert(Session *ses,const byte *key)
{
	uint16_t l=PINRef::len(key); PageID pg=INVALID_PAGEID,npg; StoreCtx *ctx=ses->getStore(); RC rc; PBlock *pb;
	if (depth==0) {
		if (buf==NULL) {
			assert(nKeys==0);
			freeSpace=freeSpaceLength=lbuf=uint16_t(ctx->bufMgr->getPageSize()/4);		// #define param
			if ((buf=(byte*)ma->malloc(lbuf))==NULL) return RC_NOMEM;
		}
		if (L_SHT+l<=freeSpaceLength) {
			((uint16_t*)buf)[nKeys++]=freeSpace-=l;
			memcpy(buf+freeSpace,key,l);
			freeSpaceLength-=L_SHT+l;
			return RC_OK;
		}
		if ((rc=ctx->fsMgr->allocPages(1,&pg))!=RC_OK) return rc;
		if ((stack[depth++]=pb=ctx->bufMgr->newPage(pg,ses->getStore()->trpgMgr,NULL,0,ses))==NULL) return RC_NOMEM;
		TreePageMgr::TreePage *tp=(TreePageMgr::TreePage*)pb->getPageBuf(); tp->info.fmt=IndexFormat(KT_REF,KT_VARKEY,0);
		memcpy(tp+1,buf,nKeys*L_SHT); tp->info.nEntries=tp->info.nSearchKeys=nKeys; tp->info.freeSpaceLength-=nKeys*L_SHT;
		ushort ld=lbuf-freeSpaceLength-nKeys*L_SHT; assert(ld<=tp->info.freeSpaceLength); 
		tp->info.freeSpace-=ld; tp->info.freeSpaceLength-=ld; memcpy((byte*)tp+tp->info.freeSpace,buf+freeSpace,ld);
		int dl=(int)tp->info.freeSpace-(int)freeSpace-sizeof(TreePageMgr::TreePage);
		if (dl!=0) for (unsigned i=0; i<nKeys; i++) ((uint16_t*)(tp+1))[i]+=(uint16_t)dl;
#ifdef _DEBUG
		tp->compact(true);
#endif
		TreePageMgr::SubTreePage *sp=(TreePageMgr::SubTreePage*)buf;
		sp->fSubPage=0xFFFF; sp->anchor=sp->leftMost=pg; sp->counter=nKeys; sp->level=0;
		freeSpace=lbuf; freeSpaceLength=lbuf-sizeof(TreePageMgr::SubTreePage); nKeys=0;
	}
	for (unsigned didx=depth-1;;didx--) {
		TreePageMgr::TreePage *tp=(TreePageMgr::TreePage*)stack[didx]->getPageBuf();
		ushort lElt=tp->info.fmt.isKeyOnly()?L_SHT:sizeof(PagePtr)+sizeof(PageID);
		if (lElt+l<=tp->info.freeSpaceLength) {				// leave extra space (%%)
			byte *ptr=(byte*)(tp+1)+tp->info.nEntries*lElt; assert(PINRef::len(key)==l);
			if (!tp->info.fmt.isKeyOnly()) {tp->store(key,l,*(PagePtr*)ptr); *(PageID*)(ptr+sizeof(PagePtr))=pg;}
			else {*(uint16_t*)ptr=tp->store(key,l); ((TreePageMgr::SubTreePage*)buf)->counter++;}
			tp->info.nEntries++; tp->info.nSearchKeys++; tp->info.freeSpaceLength-=lElt;
			assert(!tp->info.fmt.isKeyOnly() || PINRef::cmpPIDs((*tp)[tp->info.nEntries-2],(*tp)[tp->info.nEntries-1])<0);
			break;
		}
		if ((rc=ctx->fsMgr->allocPages(1,&npg))!=RC_OK) return rc;
		if ((pb=ctx->bufMgr->newPage(npg,ses->getStore()->trpgMgr,NULL,0,ses))==NULL) return RC_NOMEM;
		tp->info.sibling=npg; tp->info.nSearchKeys--;
		TreePageMgr::TreePage *tp2=(TreePageMgr::TreePage*)pb->getPageBuf(); tp2->info.fmt=tp->info.fmt; tp2->info.level=tp->info.level;
		if (tp->info.fmt.isKeyOnly()) {
			ushort ld; const byte *dkey=tp->getK(tp->info.nEntries-1,ld);
			dkey=(byte*)(tp2+1)+(((uint16_t*)(tp2+1))[0]=tp2->store(dkey,ld));
			((uint16_t*)(tp2+1))[1]=tp2->store(key,l); tp2->info.freeSpaceLength-=L_SHT*2;
			((TreePageMgr::SubTreePage*)buf)->counter++; key=dkey; l=ld; pg=npg;
		} else {
			TreePageMgr::VarKey *vk=&((TreePageMgr::VarKey*)(tp+1))[tp->info.nEntries-1],*vk2=(TreePageMgr::VarKey*)(tp2+1);
			tp2->store((byte*)(tp+1)+vk->ptr.offset,vk->ptr.len,vk2->ptr); vk2->pageID=vk->pageID; tp2->store(key,l,vk2[1].ptr); vk2[1].pageID=pg;
			tp2->info.freeSpaceLength-=sizeof(TreePageMgr::VarKey)*2; key=(byte*)(tp2+1)+vk2->ptr.offset; l=vk2->ptr.len; pg=npg;
		}
#ifdef _DEBUG
		tp->compact(true);
#endif
		tp2->info.nSearchKeys=tp2->info.nEntries=2;
		if (ctx->memory!=NULL) stack[didx]->resetNewPage();
		else {
			// rec, lrec
			ctx->logMgr->insert(ses,LR_CREATE,(MO_PAGEINIT<<TRO_SHIFT|TRO_MULTI)<<PGID_SHIFT|ctx->trpgMgr->getPGID(),stack[didx]->getPageID(),NULL,NULL,0,LRC_LUNDO,stack[didx]);
		}
		stack[didx]->release(QMGR_UFORCE); stack[didx]=pb;
		if (didx==0) {
			if (sizeof(TreePageMgr::SubTreePageKey)+l<=freeSpaceLength) {
				TreePageMgr::SubTreePageKey *pk=&((TreePageMgr::SubTreePageKey*)(buf+sizeof(TreePageMgr::SubTreePage)))[nKeys++];
				pk->key.offset=freeSpace-=l; pk->key.len=l; pk->pageID=pg; memcpy(buf+freeSpace,key,l);
				freeSpaceLength-=sizeof(TreePageMgr::SubTreePageKey)+l; break;
			}
			if (depth>=TREE_MAX_DEPTH/2-1) return RC_NOMEM;
			if ((rc=ctx->fsMgr->allocPages(1,&npg))!=RC_OK) return rc;
			if ((pb=ctx->bufMgr->newPage(npg,ses->getStore()->trpgMgr,NULL,0,ses))==NULL) return RC_NOMEM;
			memmove(&stack[1],&stack[0],depth*sizeof(PBlock*)); stack[0]=pb; depth++;
			TreePageMgr::TreePage *tp=(TreePageMgr::TreePage*)pb->getPageBuf();
			tp->info.fmt=IndexFormat(KT_REF,KT_VARKEY,sizeof(PageID)); tp->info.level=depth-1;
			TreePageMgr::VarKey *vk=(TreePageMgr::VarKey*)(tp+1);
			TreePageMgr::SubTreePage *sp=(TreePageMgr::SubTreePage*)buf;
			TreePageMgr::SubTreePageKey *pk=(TreePageMgr::SubTreePageKey*)(sp+1);
			tp->info.freeSpaceLength-=nKeys*sizeof(TreePageMgr::VarKey); tp->info.nEntries=tp->info.nSearchKeys=nKeys;
			for (unsigned i=0; i<nKeys; i++,pk++,vk++) {tp->store(buf+pk->key.offset,pk->key.len,vk->ptr); vk->pageID=pk->pageID;}
			tp->info.leftMost=sp->leftMost; sp->leftMost=npg; sp->level++;
			freeSpace=lbuf; freeSpaceLength=lbuf-sizeof(TreePageMgr::SubTreePage); nKeys=0; didx++;
#ifdef _DEBUG
			tp->compact(true);
#endif
		}
	}
	return RC_OK;
}

RC SubTreeInit::flush(Session *ses)
{
	RC rc=RC_OK;
	if (depth!=0) {
		StoreCtx *ctx=ses->getStore();
		for (unsigned i=depth; i--!=0; ) {
			PBlock *pb=stack[i];
			if (ctx->memory!=NULL) pb->resetNewPage();
			else {
				// rec, lrec
				ctx->logMgr->insert(ses,LR_CREATE,(MO_PAGEINIT<<TRO_SHIFT|TRO_MULTI)<<PGID_SHIFT|ctx->trpgMgr->getPGID(),pb->getPageID(),NULL,NULL,0,LRC_LUNDO,pb);
			}
#ifdef _DEBUG
			((TreePageMgr::TreePage*)pb->getPageBuf())->compact(true);
#endif
			pb->release(QMGR_UFORCE);
		}
	}
	if (freeSpaceLength!=0) {
		if (nKeys==0) {assert(freeSpaceLength+(depth==0?0:sizeof(TreePageMgr::SubTreePage))==lbuf);}
		else if (depth==0) {
			memmove(buf+nKeys*L_SHT,buf+freeSpace,lbuf-freeSpace); for (unsigned i=0; i<nKeys; i++) ((uint16_t*)buf)[i]-=freeSpaceLength;
		} else {
			memmove(buf+sizeof(TreePageMgr::SubTreePage)+nKeys*sizeof(TreePageMgr::SubTreePageKey),buf+freeSpace,lbuf-freeSpace);
			for (unsigned i=0; i<nKeys; i++) ((TreePageMgr::SubTreePageKey*)(buf+sizeof(TreePageMgr::SubTreePage)))[i].key.offset-=freeSpaceLength;
		}
	}
	return rc;
}
