/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "pinref.h"
#include "pgtree.h"
#include "idxtree.h"
#include "buffer.h"
#include "txmgr.h"
#include "logmgr.h"
#include "fsmgr.h"

using namespace MVStoreKernel;

TreePageMgr::TreePageMgr(StoreCtx *ctx) : TxPage(ctx),xSize(ctx->bufMgr->getPageSize()-sizeof(TreePage)-FOOTERSIZE)
{
	ctx->registerPageMgr(PGID_INDEX,this);
}

RC TreePageMgr::update(PBlock *pb,size_t len,ulong info,const byte *rec,size_t lrec,ulong flags,PBlock *newp)
{
	if (rec==NULL || lrec==0) return RC_CORRUPTED;
	byte *frame=pb->getPageBuf(); TreePage *tp=(TreePage*)frame,*tp2=NULL;
	TREE_OP op=(TREE_OP)(info&TRO_MASK); ulong idx=ulong(info>>TRO_SHIFT&0xFFFF),idx1,idx2;
	if (tp->info.level>TREE_MAX_DEPTH || tp->info.nEntries!=tp->info.nSearchKeys+(tp->info.sibling!=INVALID_PAGEID?1:0))
		return RC_CORRUPTED;
	assert(newp==NULL || op==TRO_SPLIT || op==TRO_MERGE || op==TRO_SPAWN || op==TRO_ABSORB);
	static const ulong infoSize[TRO_ALL] = {
		sizeof(TreePageModify),sizeof(TreePageModify),sizeof(TreePageModify),sizeof(TreePageEdit),
		sizeof(TreePageInit),sizeof(TreePageSplit),sizeof(TreePageSplit),
		sizeof(TreePageSpawn),sizeof(TreePageSpawn),sizeof(TreePageRoot),sizeof(TreePageRoot),
		0,0,sizeof(TreePageMulti),sizeof(TreePageMulti),sizeof(TreePageMulti)
	};
	if (lrec<infoSize[op]) {
		report(MSG_ERROR,"Invalid lrec %d in TreePageMgr::update, op %d, page %08X\n",lrec,op,tp->hdr.pageID);
		return RC_CORRUPTED;
	}
	ulong n; const byte *p1,*p2,*pref; ushort l1,l2,lpref; int icmp,delta; SearchKey sKey; RC rc;
	const TreePageModify *tpm=NULL; const TreePageEdit *tpe; const TreePageSplit *tps; const TreePageMulti *tpmi;
	const TreePageInit *tpi; const TreePageSpawn *tpa; const TreePageRoot *tpr; const SubTreePageKey *tpk;
	const PagePtr *oldData=NULL,*newData=NULL; ushort lElt,l,ll,off,newPrefSize=0; byte *ptr; PagePtr *vp,pp; SubTreePage tpExt;
	bool fFree,fVM=tp->info.fmt.isVarMultiData(),fVarSub=tp->info.fmt.isVarKeyOnly(),fSeq=tp->info.fmt.isSeq(),fPR;
	const static TREE_OP undoOP[TRO_ALL] = {
		TRO_DELETE,TRO_UPDATE,TRO_INSERT,TRO_EDIT,TRO_INIT,TRO_SPLIT,TRO_MERGE,TRO_ABSORB,
		TRO_SPAWN,TRO_DELROOT,TRO_ADDROOT,TRO_REPLACE,TRO_DROP,TRO_MULTIDEL,TRO_MULTIINS,TRO_MULTINIT
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
				if (!tp->info.fmt.isUnique()) switch (op) {
				default: break;
				case TRO_INSERT: op=TRO_UPDATE; fCorrupted=false; break;
				case TRO_DELETE: op=TRO_UPDATE; fCorrupted=false; break;		//???
				case TRO_UPDATE: if (oldData->len==0) {op=TRO_INSERT; fCorrupted=false;} break;
				}
				if (fCorrupted) {report(MSG_ERROR,"UNDO in TreePageMgr::update: invalid key, op %d, page %08X\n",op,tp->hdr.pageID); return RC_CORRUPTED;}
			}
			if (!fSeq && op==TRO_INSERT && tp->info.nEntries!=0 && tp->info.lPrefix!=0 && (idx==0||idx==tp->info.nEntries))
				newPrefSize=tp->calcPrefixSize(sKey,idx);
		}
	}
#ifdef _DEBUG
	else if (op<=TRO_DELETE && (idx>tp->info.nEntries || op!=TRO_INSERT && idx>=tp->info.nSearchKeys)) return RC_CORRUPTED;
	else {
		// check key at idx equals op!=TRO_INSERT
	}
#endif

	switch (op) {
	default: assert(0);
	case TRO_INSERT:
		l=newData->len; if (oldData->len!=0) return RC_CORRUPTED;
		if (!fSeq) {
			if (tp->info.nEntries==0) {
				if ((rc=tp->prefixFromKey(tpm+1))!=RC_OK) return rc;
			} else {
				if (newPrefSize<tp->info.lPrefix) {
					ushort extraSize=tp->info.extraEltSize(newPrefSize);
					if (extraSize==0) {
						tp->info.lPrefix=newPrefSize;
						if (tp->info.fmt.isPrefNumKey()) tp->info.prefix&=~0ULL<<(sizeof(uint64_t)-newPrefSize)*8;
					} else if (ulong(extraSize)*tp->info.nEntries>ulong(tp->info.freeSpaceLength+tp->info.scatteredFreeSpace)) {
						report(MSG_ERROR,"TreePageMgr::update insert: insufficient space, newPrefSize=%d, page %08X\n",newPrefSize,tp->hdr.pageID);
						return RC_PAGEFULL;
					} else 
						tp->changePrefixSize(newPrefSize);
				} else if (newPrefSize>tp->info.lPrefix) return RC_CORRUPTED;
				if (!tp->info.fmt.isFixedLenKey()) {
					if (*(byte*)(tpm+1)<KT_BIN || (l1=SearchKey::keyLen(tpm+1))<tp->info.lPrefix) return RC_CORRUPTED;
					if (tp->info.fmt.isFixedLenData()) l=l1-tp->info.lPrefix; else l+=l1-tp->info.lPrefix;
				}
				if (fVM && oldData->offset==0) l+=newData->len<254?2:2*L_SHT;
			}
		}
		l+=lElt=tp->info.calcEltSize(); 
		if (tp->info.freeSpaceLength<l) {
			if (tp->info.freeSpaceLength+tp->info.scatteredFreeSpace<l) {
				report(MSG_ERROR,"TreePageMgr::update insert: insufficient space, requested %d, page %08X\n",l,tp->hdr.pageID);
				return RC_PAGEFULL;
			}
			tp->compact(); assert(tp->info.freeSpaceLength>=l);
		}
		ptr=(byte*)(tp+1)+idx*lElt;
		if (fSeq) {
			assert(!tp->hasSibling() && idx==tp->info.nEntries); tp->info.freeSpaceLength-=lElt;
			if (!tp->info.fmt.isFixedLenData()) tp->store(rec+newData->offset,newData->len,*(PagePtr*)ptr);
			else {assert(newData->len==lElt); memcpy(ptr,rec+newData->offset,lElt);}
		} else if (fVarSub) {
			tp->storeKey((byte*)(tpm+1),ptr); tp->info.freeSpaceLength-=lElt;
		} else { 
			if (idx<tp->info.nEntries) memmove(ptr+lElt,ptr,(tp->info.nEntries-idx)*lElt);
			tp->storeKey((byte*)(tpm+1),ptr); tp->info.freeSpaceLength-=lElt;
			if (oldData->offset!=0 && !tp->info.fmt.isUnique()) {
				if (oldData->offset!=ushort(~0u) && (oldData->offset!=ushort(~1u) ||
					newData->len!=sizeof(SubTreePage) && newData->len<sizeof(SubTreePage)+sizeof(SubTreePageKey))) return RC_CORRUPTED;
				vp=(PagePtr*)(ptr+tp->info.keyLength()); tp->store(rec+newData->offset,newData->len,*vp);
				if (oldData->offset==ushort(~1u)) {
					if (newData->len>sizeof(SubTreePage)) {
						vp->len=((SubTreePageKey*)(rec+newData->offset+sizeof(SubTreePage)))->key.offset;
						SubTreePageKey *pk=(SubTreePageKey*)((byte*)tp+vp->offset+sizeof(SubTreePage));
						for (int i=(vp->len-sizeof(SubTreePage))/sizeof(SubTreePageKey); --i>=0; pk++)
							{memcpy(&pp,&pk->key,sizeof(PagePtr)); pp.offset+=vp->offset; memcpy(&pk->key,&pp,sizeof(PagePtr));}
					}
					vp->len|=TRO_EXT_DATA;
				}
			} else if (fVM) {
				byte *p=(byte*)alloca((l=newData->len)+4); if (p==NULL) return RC_NORESOURCES;
				if (newData->len<254) {p[0]=2; p[1]=byte(l+2); memcpy(p+2,rec+newData->offset,l); l+=2;}
				else {_set_u16(p,L_SHT*2); _set_u16(p+L_SHT,l+L_SHT*2); memcpy(p+L_SHT*2,rec+newData->offset,l); l+=L_SHT*2;}
				tp->store(p,l,*(PagePtr*)(ptr+tp->info.keyLength()));
			} else if (!tp->info.fmt.isFixedLenData()) 
				tp->store(rec+newData->offset,newData->len,*(PagePtr*)(ptr+tp->info.keyLength()));
			else if ((l=tp->info.fmt.dataLength())!=0) {
				assert(newData->len==l); 
				if (!tp->info.fmt.isFixedLenKey()) memcpy(ptr+sizeof(PagePtr),rec+newData->offset,l);
				else {
					assert(tp->info.freeSpaceLength>=l);
					tp->info.freeSpaceLength-=l; tp->info.freeSpace-=l;
					ptr=(byte*)tp+tp->info.freeSpace+(tp->info.nSearchKeys-idx)*l;
					if (idx<tp->info.nSearchKeys) memmove((byte*)tp+tp->info.freeSpace,(byte*)tp+tp->info.freeSpace+l,ptr-(byte*)tp-tp->info.freeSpace);
					if (!tp->isLeaf()) *(PageID*)ptr=*(PageID*)(rec+newData->offset); else memcpy(ptr,rec+newData->offset,l);
				}
			}
		}
		tp->info.nEntries++; tp->info.nSearchKeys++;
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
		if (tp->info.fmt.isUnique()) {
			assert(l==vp->len);
			tp->info.scatteredFreeSpace+=l;
			if (tp->info.freeSpaceLength<newData->len) {
				if (tp->info.freeSpaceLength+tp->info.scatteredFreeSpace<newData->len) {
					report(MSG_ERROR,"TreePageMgr::update UPDATE: insufficient space, requested %d, page %08X\n",newData->len,tp->hdr.pageID);
					return RC_PAGEFULL;
				}
				vp->len=0; tp->compact(); assert(tp->info.freeSpaceLength>=newData->len);
			}
			tp->store(rec+newData->offset,newData->len,*vp); break;
		}
	update_entry:
		if (TreePage::isSubTree(*vp)) {
			report(MSG_ERROR,"TreePageMgr::update UPDATE: secondary tree, idx=%d, page %08X\n",idx,tp->hdr.pageID);
			return RC_INTERNAL;
		}
		if (oldData->len!=0) {
			assert(fVM || oldData->len==tp->info.fmt.dataLength());
			if (!tp->info.fmt.isUnique() && newData->len==0 && newData->offset!=0) {
				if (newData->offset!=ushort(~0u)) return RC_CORRUPTED;
				rec+=oldData->offset; lrec=oldData->len;
				n=fVM?TreePage::nKeys(rec):ulong(lrec/(l1=tp->info.fmt.dataLength()));
				for (ulong idx2=0; idx2<n; ) {
					if (fVM) {p1=TreePage::getK(rec,idx2); l1=TreePage::lenK(rec,idx2);} else p1=rec+idx2*l1;
					if ((ptr=tp->findValue(p1,l1,*vp,&off))==NULL) return RC_NOTFOUND;
					if (tp->info.fmt.isPinRef()) {
						p2=(byte*)tp+vp->offset+off;
						if (vp->len<256) {l2=p2[1]-p2[0]; p2=(byte*)tp+vp->offset+p2[0];}
						else {l2=_u16(p2+L_SHT)-_u16(p2); p2=(byte*)tp+vp->offset+_u16(p2);}
						if (PINRef::getCount(p1,l1)!=PINRef::getCount(p2,l2)) {
							if ((rc=tp->adjustCount(ptr,vp,idx,p1,l1,true))!=RC_OK) return rc;
							idx2++; continue;
						}
					}
					ushort off0=off,sht=fVM?vp->len<256?1:L_SHT:l1; ulong idx0=idx2; int cmp;
					do {
						off+=sht; idx2++; p2=(byte*)tp+vp->offset+off;
						if (!fVM) cmp=memcmp(rec+idx2*l1,p2,l1);
						else {
							p1=TreePage::getK(rec,idx2); l1=TreePage::lenK(rec,idx2);
							if (vp->len<256) {l2=p2[1]-p2[0]; p2=(byte*)tp+vp->offset+p2[0];}
							else {l2=_u16(p2+L_SHT)-_u16(p2); p2=(byte*)tp+vp->offset+_u16(p2);}
							if (!tp->info.fmt.isPinRef()) {if ((cmp=memcmp(p1,p2,min(l1,l2)))==0) cmp=cmp3(l1,l2);}
							else if ((cmp=PINRef::cmpPIDs(p1,l1,p2,l2))==0 && PINRef::getCount(p1,l1)!=PINRef::getCount(p2,l2)) {
								if ((rc=tp->adjustCount((byte*)tp+vp->offset+off,vp,idx,p1,l1,true))!=RC_OK) return rc;
								break;
							}
						}
						if (cmp<0) return RC_NOTFOUND;
					} while (cmp==0);
					tp->deleteValues(fVM?TreePage::lenKK(rec,idx0,idx2):ushort(l1*(idx2-idx0)),vp,off0,fVM,ushort(idx2-idx0));
				}
			} else if ((ptr=tp->findValue(rec+oldData->offset,oldData->len,*vp,&off))==NULL) {
				report(MSG_ERROR,"TreePageMgr::update UPDATE: value not found, idx=%d, page %08X\n",idx,tp->hdr.pageID);
				return RC_NOTFOUND;
			} else {
				if (newData->len==oldData->len) {
					ushort off2;
					if (tp->findValue(rec+newData->offset,newData->len,*vp,&off2)!=NULL && off2==off) break;
					if (off2==off || off2==off+(!fVM?oldData->len:vp->len<256?1:L_SHT)) {
						if (!fVM) memcpy(ptr,rec+newData->offset,newData->len);
						else memcpy((byte*)tp+vp->offset+(vp->len>=256?_u16(ptr):ptr[0]),rec+newData->offset,newData->len);
						break;
					}
					// other variants with memmove
				}
				if (!tp->info.fmt.isPinRef()) tp->deleteValues(oldData->len,vp,off,fVM);
				else if ((rc=tp->adjustCount(ptr,vp,idx,rec+oldData->offset,oldData->len,true))!=RC_OK) return rc;
			}
		}
		if (newData->len!=0) {
			if (tp->info.fmt.isUnique() || oldData->len!=0 || oldData->offset==0) {
				if (!fVM && newData->len!=tp->info.fmt.dataLength()) return RC_CORRUPTED;
				if ((ptr=tp->findValue(rec+newData->offset,newData->len,*vp,&off))!=NULL) {
					if ((rc=tp->adjustCount(ptr,vp,idx,rec+newData->offset,newData->len))!=RC_OK) return rc;
				} else if (!tp->insertValues(rec+newData->offset,newData->len,vp,off,idx,fVM))
					return RC_PAGEFULL;
			} else if (oldData->offset!=ushort(~0u)) return RC_CORRUPTED;
			else {
				rec+=newData->offset; lrec=newData->len; fPR=tp->info.fmt.isRefKeyOnly();
				n=fVM?TreePage::nKeys(rec):ulong(lrec/(l1=tp->info.fmt.dataLength()));
				for (ulong idx2=0; idx2<n; ) {
					if (fVM) {p1=TreePage::getK(rec,idx2); l1=TreePage::lenK(rec,idx2);} else p1=rec+idx2*l1;
					if ((ptr=tp->findValue(p1,l1,*vp,&off))!=NULL) {
						if ((rc=tp->adjustCount(ptr,vp,idx,p1,l1))!=RC_OK) return rc;
						idx2++;
					} else {
						ulong idx0=idx2; const byte *pd=(byte*)tp+vp->offset;
						if (++idx2<n) {
							if (off>=(!fVM?vp->len:vp->len<256?pd[0]-1:_u16(pd)-L_SHT)) idx2=n;
							else {
								const byte *nxt=pd+(!fVM?off:vp->len<256?pd[off]:_u16(pd+off));
								const ushort lnxt=!fVM?l1:vp->len<256?pd[off+1]-pd[off]:_u16(pd+off+L_SHT)-_u16(pd+off); int cmp;
								do {
									if (fVM) {p2=TreePage::getK(rec,idx2); l2=TreePage::lenK(rec,idx2);} else p2=rec+idx2*(l2=l1);
									if (fPR) cmp=PINRef::cmpPIDs(p2,l2,nxt,lnxt); else if ((cmp=memcmp(p2,nxt,min(lnxt,l2)))==0) cmp=cmp3(l2,lnxt);
								} while (cmp<0 && ++idx2<n);
							}
						}
						if (!tp->insertValues(p1,fVM?TreePage::lenKK(rec,idx0,idx2):ushort(l1*(idx2-idx0)),
								vp,off,idx,fVM,ushort(idx2-idx0),fVM?(ushort*)rec+idx0:(ushort*)0)) return RC_PAGEFULL;
					}
				}
			}
			break;
		} else if (vp->len!=0) break;
	case TRO_DELETE:
		assert(idx<tp->info.nSearchKeys && newData->len==0);
		lElt=tp->info.calcEltSize(); ptr=(byte*)(tp+1)+idx*lElt;
		if (fSeq) {
			if (!tp->info.fmt.isFixedLenData()) {tp->info.scatteredFreeSpace+=((PagePtr*)ptr)->len; ((PagePtr*)ptr)->len=0;}
			if (idx+1!=tp->info.nEntries || tp->hasSibling()) break;
		} else if (fVarSub) {
			if (!tp->info.fmt.isRefKeyOnly()) {
				ushort *p=(ushort*)(tp+1)+idx; ulong sht=*p; l=TreePage::lenK(p,0); ll=((ushort*)(tp+1))[tp->info.nEntries];
				if (sht+l<ll) memmove((byte*)(tp+1)+sht,(byte*)(tp+1)+sht+l,ll-l-sht);
				memmove(p,p+1,ll-l-(idx+1)*L_SHT); p=(ushort*)(tp+1); tp->info.freeSpaceLength+=l;
				for (int j=tp->info.nEntries; --j>=0;) p[j]-=(ulong)j>=idx?l+L_SHT:L_SHT;
			} else if (oldData->len!=0 || newData->len!=0) return RC_CORRUPTED;
			else if ((rc=tp->adjustKeyCount(idx,(byte*)(tpm+1)+2,((byte*)(tpm+1))[1],NULL,0,true))!=RC_OK) return rc;
			else break;
		} else if (!tp->info.fmt.isUnique() && oldData->len>0 && (vp=(PagePtr*)(ptr+tp->info.keyLength()))->len!=0
																				&& newData->offset==0) goto update_entry;
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
			ushort newPrefSize=tp->calcPrefixSize(0,tp->info.nEntries); assert(newPrefSize>=tp->info.lPrefix);
			if (newPrefSize>tp->info.lPrefix) tp->changePrefixSize(newPrefSize);
		}
		if ((flags&TXMGR_UNDO)==0 && !tp->isLeaf()) tp->info.stamp++;
		break;
	case TRO_EDIT:
		tpe=(const TreePageEdit*)rec; oldData=&tpe->oldData; newData=&tpe->newData;
		if (!tp->info.fmt.isUnique() || tp->info.fmt.isKeyOnly() || tp->info.fmt.keyType()!=*(byte*)(tpe+1)) {
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
			if (ulong(tpe->shift+oldData->len)<l) memcpy(ptr+tpe->shift+newData->len,ptr+tpe->shift+oldData->len,l-tpe->shift-oldData->len);
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
			byte *p=new(SES_HEAP) byte[l]; if (p==NULL) return RC_NORESOURCES;
			memcpy(p,ptr,l); fFree=true; ptr=p; vp->len=0; tp->info.scatteredFreeSpace+=l; 
			if (!tp->info.fmt.isVarMultiData()) tp->compact(false,idx);
			else {
//.......................				//VM
			}
			tp->findData(idx,ll,(const PagePtr**)&vp); assert(vp!=NULL && ulong(l+lElt)<=tp->info.freeSpaceLength);
		}
		vp->len=l+lElt; vp->offset=tp->info.freeSpace-=vp->len; tp->info.freeSpaceLength-=vp->len;
		if (tpe->shift>0) memcpy((byte*)tp+tp->info.freeSpace,ptr,tpe->shift);
		memcpy((byte*)tp+tp->info.freeSpace+tpe->shift,rec+newData->offset,newData->len);
		if (tpe->shift+oldData->len<l) memcpy((byte*)tp+tp->info.freeSpace+tpe->shift+newData->len,ptr+tpe->shift+oldData->len,l-tpe->shift-oldData->len);
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
			if (tp->info.fmt.isVarKeyOnly()) memcpy(tp+1,rec+sizeof(TreePageInfo),lrec-sizeof(TreePageInfo));
			else {
				ll=tp->info.nEntries*tp->info.calcEltSize(); memcpy(tp+1,rec+sizeof(TreePageInfo),ll);
				memcpy((byte*)tp+tp->info.freeSpace,rec+sizeof(TreePageInfo)+ll,lrec-sizeof(TreePageInfo)-ll);
			}
		}
		break;
	case TRO_MERGE:
		if ((flags&TXMGR_RECV)!=0) {
			tps=(const TreePageSplit*)rec; p1=(const byte*)(tps+1); l=SearchKey::keyLenP(p1);
			if (lrec<sizeof(TreePageSplit)+l+sizeof(TreePageInfo)) {
				report(MSG_ERROR,"TreePageMgr::update MERGE: cannot recover old log format, page %08X\n",tp->hdr.pageID);
				return RC_INTERNAL;
			}
			if ((tp2=(TreePage*)alloca(tp->hdr.length()))==NULL) return RC_NORESOURCES;
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
				if (newp==NULL) return RC_NORESOURCES;
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
				if (tps->nEntriesLeft<tp->info.nEntries) {
					l=tp->calcPrefixSize(tps->nEntriesLeft,tp->info.nEntries);
					tp->copyEntries(tp2,l,tps->nEntriesLeft);
				}
				bool fFixedKey=tp->info.fmt.isFixedLenKey(),fFixedData=tp->info.fmt.isFixedLenData();
				lElt=tp->info.calcEltSize(); tp->info.freeSpaceLength+=lElt*(tp->info.nEntries-tps->nEntriesLeft);
				if (fFixedKey && fFixedData) {
					ushort lFreed=tp->info.fmt.dataLength()*ushort(tp->info.nSearchKeys-tps->nEntriesLeft);
					tp->info.freeSpace+=lFreed; tp->info.freeSpaceLength+=lFreed;
				} else if (fVarSub) {
					ushort *p=(ushort*)(tp+1)+tps->nEntriesLeft,*pe=(ushort*)(tp+1)+tp->info.nEntries;
					tp->info.freeSpaceLength+=pe[0]-p[0]; ushort delta=ushort((byte*)pe-(byte*)p);
					memmove(p+1,pe+1,p[0]-*(ushort*)(tp+1)); for (pe=(ushort*)(tp+1); pe<=p; *pe++-=delta);
				} else {
					byte *p=(byte*)(tp+1)+tps->nEntriesLeft*lElt;
					for (ushort i=tps->nEntriesLeft; i<tp->info.nSearchKeys; ++i,p+=lElt) {
						if (!fFixedKey) tp->info.scatteredFreeSpace+=((PagePtr*)p)->len;
						if (!fFixedData) {vp=(PagePtr*)(p+tp->info.keyLength()); tp->info.scatteredFreeSpace+=tp->length(*vp);}
					}
					if (!fFixedKey && tp->hasSibling()) tp->info.scatteredFreeSpace+=((PagePtr*)p)->len;
				}
				tp->info.nSearchKeys=tp->info.nEntries=tps->nEntriesLeft; tp->info.sibling=INVALID_PAGEID;
				if (!tp->info.fmt.isNumKey()||tp->info.fmt.isPrefNumKey()) {
					if ((rc=sKey.deserialize(tps+1,lrec-sizeof(TreePageSplit)))!=RC_OK) return rc;
					ushort prefixSize=tp->info.nEntries!=0?tp->calcPrefixSize(sKey,0,true):0;			// pass in TreePageSplit?
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
			//if (tp2->info.nEntries>0)		important for page creation in recovery
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
				bool fVarKey=!tp->info.fmt.isFixedLenKey(); assert(tp->info.nEntries>0);
				lElt=tp->info.calcEltSize(); tp->info.freeSpaceLength+=lElt;
				if (fVarKey) {
					if (!fVarSub) tp->info.scatteredFreeSpace+=((PagePtr*)((byte*)(tp+1)+(tp->info.nEntries-1)*lElt))->len;
					else {
						tp->info.freeSpaceLength+=l=tp->lenK(tp->info.nEntries-1);
						if (tp->info.nEntries==1) tp->info.freeSpaceLength+=L_SHT;
						else {
							memmove((ushort*)(tp+1)+tp->info.nEntries,(ushort*)(tp+1)+tp->info.nEntries+1,((ushort*)(tp+1))[tp->info.nEntries]-l-tp->info.nEntries*L_SHT);
							for (int i=tp->info.nEntries; --i>=0;) ((ushort*)(tp+1))[i]-=L_SHT;
						}
					}
				}
				--tp->info.nEntries; tp->info.sibling=INVALID_PAGEID;
				if (tp->info.lPrefix!=tps->oldPrefSize) 
					{assert(tp->info.nEntries<=1||tp->info.lPrefix>tps->oldPrefSize); tp->changePrefixSize(tps->oldPrefSize); if (!fVarKey) lElt=tp->info.calcEltSize();}
				if (tp2->info.nEntries>0) {
					if (tp2->info.lPrefix!=tps->oldPrefSize) {assert(tp2->info.lPrefix>tps->oldPrefSize); tp2->changePrefixSize(tps->oldPrefSize);}
					if (tp->info.scatteredFreeSpace>0) tp->compact();
					if (ulong(tp->info.freeSpaceLength+tp2->info.freeSpaceLength+tp2->info.scatteredFreeSpace)<xSize) {
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
					if (fVarSub) {
						if (tp->info.nEntries==0) {
							l=((ushort*)(tp2+1))[tp2->info.nEntries]; if (l>tp->info.freeSpaceLength) return RC_CORRUPTED;
							memcpy(tp+1,tp2+1,l); tp->info.freeSpaceLength-=l;
						} else {
							l=((ushort*)(tp2+1))[tp2->info.nEntries]-L_SHT; if (l>tp->info.freeSpaceLength) return RC_CORRUPTED;
							tp->info.freeSpaceLength-=l; l=((ushort*)(tp+1))[tp->info.nEntries];
							memmove((ushort*)(tp+1)+tp->info.nEntries+1+tp2->info.nEntries,(ushort*)(tp+1)+tp->info.nEntries+1,l-((ushort*)(tp+1))[0]);
							memcpy((ushort*)(tp+1)+tp->info.nEntries,tp2+1,(tp2->info.nEntries+1)*L_SHT);
							memcpy((byte*)(tp+1)+l+tp2->info.nEntries*L_SHT,(ushort*)(tp2+1)+tp2->info.nEntries+1,
											((ushort*)(tp2+1))[tp2->info.nEntries]-(tp2->info.nEntries+1)*L_SHT);
							for (int i=tp->info.nEntries; --i>=0; ) ((ushort*)(tp+1))[i]+=tp2->info.nEntries*L_SHT;
							for (int j=tp2->info.nEntries+1; --j>=0; ) ((ushort*)(tp+1))[j+tp->info.nEntries]+=l-L_SHT;
						}
					} else {
						l=lElt*tp2->info.nEntries; if (l>tp->info.freeSpaceLength) return RC_CORRUPTED;
						memcpy((byte*)(tp+1)+lElt*tp->info.nEntries,tp2+1,l); tp->info.freeSpaceLength-=l;
						bool fVarData=!tp->info.fmt.isFixedLenData();
						if (!fVarKey && !fVarData) {
							l=ushort(tp2->info.fmt.dataLength()*tp2->info.nSearchKeys);
							tp->info.freeSpace-=l; tp->info.freeSpaceLength-=l;
							memcpy((byte*)tp+tp->info.freeSpace,(byte*)tp2+tp->info.freeSpace,l);
						} else {
							ptr=(byte*)(tp+1)+tp->info.nEntries*lElt; ulong sht=tp2->info.keyLength();
							for (ushort i=0; i<tp2->info.nEntries; ++i,ptr+=lElt) {
								if (fVarKey) tp->store((byte*)tp2+((PagePtr*)ptr)->offset,((PagePtr*)ptr)->len,*(PagePtr*)ptr); 
								if (fVarData && i<tp2->info.nSearchKeys) {
									pp=*(PagePtr*)(ptr+sht); tp->store((byte*)tp2+pp.offset,pp.len,*(PagePtr*)(ptr+sht));
									if (TreePage::isSubTree(pp)) tp->moveExtTree(tp2,pp,*(PagePtr*)(ptr+sht));
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
			if (op==TRO_SPLIT && (flags&TXMGR_UNDO)!=0) {newp->release(PGCTL_DISCARD); ctx->logMgr->setNewPage(NULL);}
		}
		if (!fSeq && tp->info.nEntries!=tp->info.nSearchKeys+(tp->info.sibling!=INVALID_PAGEID?1:0)) {
			report(MSG_ERROR,"Invalid nEntries/nSearchKeys %d/%d after merge/split, page %08X\n",tp->info.nEntries,tp->info.nSearchKeys,tp->hdr.pageID);
			return RC_CORRUPTED;
		}
		return RC_OK;
	case TRO_SPAWN:
		tpa=(const TreePageSpawn*)rec; assert(!tp->info.fmt.isUnique() && !tp->info.fmt.isKeyOnly());
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
			newp=ctx->logMgr->setNewPage(ctx->bufMgr->newPage(tpa->root,this));
			if (newp==NULL) return RC_NORESOURCES;
		}
		assert(newp->getPageID()==tpa->root);
		tp2=(TreePage*)newp->getPageBuf(); tp2->info.fmt=tpa->fmt;
		if (TreePage::isSubTree(*vp)) {
			l=vp->len&~TRO_EXT_DATA;
			assert(l>=sizeof(SubTreePage) && (l-sizeof(SubTreePage))%sizeof(SubTreePageKey)==0);
			memcpy(&tpExt,(byte*)tp+vp->offset,sizeof(SubTreePage));
			tp2->info.level=byte(++tpExt.level); tp2->info.leftMost=tpExt.leftMost; tp2->info.flags=0;
			tpExt.leftMost=newp->getPageID(); tp->info.scatteredFreeSpace+=l-sizeof(SubTreePage);
			n=tp2->info.nSearchKeys=(l-sizeof(SubTreePage))/sizeof(SubTreePageKey);
			SubTreePageKey tpk; memcpy(&tpk,(byte*)tp+vp->offset+sizeof(SubTreePage),sizeof(SubTreePageKey));
			const byte *p1=(byte*)tp+tpk.key.offset,*p0=p1; l=tpk.key.len;
			memcpy(&tpk,(byte*)tp+vp->offset+(vp->len&~TRO_EXT_DATA)-sizeof(SubTreePageKey),sizeof(SubTreePageKey));
			byte *p2=(byte*)tp+tpk.key.offset; if (tpk.key.len<l) l=tpk.key.len;
			//for (; tp2->info.lPrefix<l; ++tp2->info.lPrefix) if (*p1++!=*p2++) break;					// no prefix (see calcPrefixSize())
			if (tp2->info.lPrefix>0) tp2->store(p0,tp2->info.lPrefix,*(PagePtr*)&tp2->info.prefix);
			p1=(byte*)tp+vp->offset+sizeof(SubTreePage); p2=(byte*)(tp2+1);
			lElt=fVM?sizeof(PagePtr):tp->info.fmt.dataLength()-tp2->info.lPrefix;
			for (ulong i=0; i<n; ++i,p1+=sizeof(SubTreePageKey)) {
				memcpy(&tpk,p1,sizeof(SubTreePageKey)); tp->info.scatteredFreeSpace+=tpk.key.len;
				assert(tp2->info.freeSpaceLength>=lElt+sizeof(PageID));
				if (fVM) {
					tp2->store((byte*)tp+tpk.key.offset+tp2->info.lPrefix,tpk.key.len-tp2->info.lPrefix,*(PagePtr*)p2);
					*(PageID*)(p2+sizeof(PagePtr))=tpk.pageID; p2+=sizeof(PageID);
				} else {
					memcpy(p2,(byte*)tp+tpk.key.offset+tp2->info.lPrefix,lElt);
					tp2->info.freeSpace-=sizeof(PageID); *(PageID*)((byte*)tp2+tp2->info.freeSpace)=tpk.pageID; 
				}
				tp2->info.freeSpaceLength-=lElt+sizeof(PageID); p2+=lElt;
			}
		} else {
			tpExt.level=0; tpExt.anchor=tpExt.leftMost=tpa->root;
			if (!fVM) {
				l=tp->info.fmt.dataLength(); byte *p1=(byte*)tp+vp->offset,*p2=p1+vp->len-l;
				for (; tp2->info.lPrefix<l && tp2->info.lPrefix<sizeof(tp2->info.prefix); ++tp2->info.lPrefix) if (*p1++!=*p2++) break;
				if (tp2->info.lPrefix>0) memcpy(&tp2->info.prefix,(byte*)tp+vp->offset,tp2->info.lPrefix);
				p1=(byte*)tp+vp->offset; p2=(byte*)(tp2+1);
				tpExt.counter=n=tp2->info.nSearchKeys=vp->len/l; tp2->info.freeSpaceLength-=ushort(n*(l-tp2->info.lPrefix));
				for (ulong i=0; i<n; ++i,p1+=l,p2+=l-tp2->info.lPrefix) memcpy(p2,p1+tp2->info.lPrefix,l-tp2->info.lPrefix);
			} else if (vp->len>=256) {
				const byte *p1=(byte*)tp+vp->offset,*p2,*p0; l=_u16(p1);
				p2=p1+l-4; ll=min(_u16(p2+2)-_u16(p2),_u16(p1+2)-l); p2=p1+_u16(p2); p0=p1+=l;
				//for (; tp2->info.lPrefix<ll; ++tp2->info.lPrefix) if (*p1++!=*p2++) break;			// no prefix (see calcPrefixSize())
				p1=p2=(byte*)tp+vp->offset; tpExt.counter=tp2->info.nSearchKeys=l/L_SHT-1;
				tp2->info.freeSpaceLength-=ushort(vp->len-tp2->info.nSearchKeys*tp2->info.lPrefix);
				if (tp2->info.lPrefix==0) memcpy((ushort*)(tp2+1),p1,vp->len);
				else {
					tp2->store(p0,tp2->info.lPrefix,*(PagePtr*)&tp2->info.prefix); 
					ushort *to=(ushort*)(tp2+1); memcpy(to+tp2->info.nSearchKeys+1,p1+l,vp->len-l);
					for (ulong i=0; i<tp2->info.nSearchKeys; ++i,p1+=L_SHT,++to) {
						ll=_u16(p1+L_SHT)-_u16(p1); *to=l; assert(ll>=tp2->info.lPrefix);
						memcpy((byte*)(tp2+1)+l,p2+_u16(p1)+tp2->info.lPrefix,ll-=tp2->info.lPrefix); l+=ll;
					}
					*to=l;
				}
			} else {
				//???
				assert(0 && "spawn vp->len<256");
			}
			tp->info.scatteredFreeSpace+=tp->length(*vp); vp->len=0;
			if (tp->info.freeSpaceLength<sizeof(SubTreePage)) {tp->compact(); assert(tp->info.freeSpaceLength>=sizeof(SubTreePage));}
			vp->offset=tp->info.freeSpace-=sizeof(SubTreePage); tp->info.freeSpaceLength-=sizeof(SubTreePage);
		}
		tp2->info.nEntries=(ushort)tp2->info.nSearchKeys;
		memcpy((byte*)tp+vp->offset,&tpExt,sizeof(SubTreePage));
		vp->len=sizeof(SubTreePage)|TRO_EXT_DATA;
		if (tp->info.level>TREE_MAX_DEPTH) {
			report(MSG_ERROR,"Invalid level %d after spawn(1), page %08X\n",tp->info.level,tp->hdr.pageID);
			return RC_CORRUPTED;
		}
		if (tp2->info.level>TREE_MAX_DEPTH) {
			report(MSG_ERROR,"Invalid level %d after spawn(2), page %08X\n",tp2->info.level,tp->hdr.pageID);
			return RC_CORRUPTED;
		}
#ifdef _DEBUG
		tp->compact(true); if (fVM) tp2->compact(true);
#endif
		if (!fSeq && tp->info.nEntries!=tp->info.nSearchKeys+(tp->info.sibling!=INVALID_PAGEID?1:0)) {
			report(MSG_ERROR,"Invalid nEntries/nSearchKeys %d/%d after spawn, page %08X\n",tp->info.nEntries,tp->info.nSearchKeys,tp->hdr.pageID);
			return RC_CORRUPTED;
		}
		pb->setDependency(newp); return RC_OK;
	case TRO_ABSORB:
		tpa=(const TreePageSpawn*)rec; assert(!tp->info.fmt.isUnique() && !tp->info.fmt.isKeyOnly());
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
		if (!TreePage::isSubTree(*vp)) {
			// report
			break;
		}
		l=vp->len&~TRO_EXT_DATA; assert(l>=sizeof(SubTreePage));
		memcpy(&tpExt,(byte*)tp+vp->offset,sizeof(SubTreePage));
		if (l>sizeof(SubTreePage)||tpExt.leftMost!=tpa->root) {
			// report error
			return RC_CORRUPTED;
		}
		if (newp==NULL) {
			assert((flags&(TXMGR_RECV|TXMGR_UNDO))!=0);
			newp=ctx->logMgr->setNewPage(ctx->bufMgr->newPage(tpa->root,this)/*,true*/);
			if (newp==NULL) return RC_NORESOURCES;
		}

		tp2=(TreePage*)newp->getPageBuf();
		if (tp2->info.level==0) {
//...................
		} else {
//...................
		}
		return RC_OK;
	case TRO_ADDROOT:
		tpr=(const TreePageRoot*)rec; assert(!tp->info.fmt.isUnique() && !tp->info.fmt.isKeyOnly());
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
		if (!TreePage::isSubTree(*vp)) {
			report(MSG_ERROR,"TreePageMgr::update: not a secondary tree in ADDROOT, idx %d, page %08X\n",idx,tp->hdr.pageID);
			break;
		}
		l=vp->len&~TRO_EXT_DATA;
		if ((tpk=tp->findExtKey(rec+tpr->pkey.offset,tpr->pkey.len,(SubTreePageKey*)((byte*)tp+vp->offset+sizeof(SubTreePage)),
															(l-sizeof(SubTreePage))/sizeof(SubTreePageKey),&off))!=NULL) {
			PageID pid; memcpy(&pid,&tpk->pageID,sizeof(PageID)); if (pid==tpr->pageID) break;
			report(MSG_ERROR,"TreePageMgr::update: key already exists in ADDROOT, idx %d, page %08X\n",idx,tp->hdr.pageID);
			return RC_ALREADYEXISTS;
		}
		if (ulong(tp->info.freeSpaceLength+tp->info.scatteredFreeSpace)<tpr->pkey.len+sizeof(SubTreePageKey)) {
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
		memcpy(&((SubTreePageKey*)ptr)->key,&pp,sizeof(PagePtr));
		memcpy(&((SubTreePageKey*)ptr)->pageID,&tpr->pageID,sizeof(PageID));
		break;
	case TRO_DELROOT:
		tpr=(const TreePageRoot*)rec; assert(!tp->info.fmt.isUnique() && !tp->info.fmt.isKeyOnly());
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
		if (!TreePage::isSubTree(*vp)) {
			report(MSG_ERROR,"TreePageMgr::update: not a secondary tree in DELROOT, idx %d, page %08X\n",idx,tp->hdr.pageID);
			break;
		}
		l=(vp->len&~TRO_EXT_DATA)-sizeof(SubTreePage);
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
	case TRO_MULTINIT:
		assert((flags&TXMGR_UNDO)==0); if (lrec<=sizeof(TreePageMulti)) return RC_CORRUPTED;
		tpmi=(TreePageMulti*)rec; rec+=sizeof(TreePageMulti); fVarSub=tpmi->fmt.isVarKeyOnly();
		tp->info.fmt=tpmi->fmt; tp->info.sibling=tpmi->sibling;
		if ((tp->info.lPrefix=tpmi->lPrefix)!=0) {
			if (lrec<=tpmi->lPrefix) return RC_CORRUPTED;
			if (fVarSub) tp->store(rec+tpmi->lData,tpmi->lPrefix,*(PagePtr*)&tp->info.prefix);
			else if (tpmi->lPrefix>sizeof(tp->info.prefix)) return RC_CORRUPTED;
			else memcpy(&tp->info.prefix,rec+tpmi->lData,tpmi->lPrefix);
		}
		if ((ulong)tp->info.freeSpaceLength<tpmi->lData) {report(MSG_ERROR,"Not enough space in MULTIINIT, requested: %d\n",tpmi->lData); return RC_PAGEFULL;}
		tp->info.nEntries=tp->info.nSearchKeys=fVarSub?TreePage::nKeys(rec):ushort(tpmi->lData/(tp->info.fmt.keyLength()-tp->info.lPrefix));
		memcpy(tp+1,rec,tpmi->lData); tp->info.freeSpaceLength-=tpmi->lData; if (tp->info.sibling!=INVALID_PAGEID) tp->info.nSearchKeys--;
		break;
	case TRO_MULTIINS:
		assert((flags&TXMGR_UNDO)==0);
		if (!tp->info.fmt.isKeyOnly() || lrec<=sizeof(TreePageMulti)) return RC_CORRUPTED; 
		tpmi=(TreePageMulti*)rec; rec+=sizeof(TreePageMulti);
		if (tp->info.sibling!=INVALID_PAGEID) {
			if (tp->info.nEntries!=tp->info.nSearchKeys+1) return RC_CORRUPTED;
			if (tpmi->fLastR!=0) {
				ushort *p=(ushort*)(tp+1)+tp->info.nEntries-1; l=(ll=p[1])-p[0];
				memmove(p+1,p+2,ll-l-tp->info.nEntries*L_SHT);
				tp->info.freeSpaceLength+=l+L_SHT; p=(ushort*)(tp+1);
				for (int j=tp->info.nEntries; --j>=0;) *p++-=L_SHT;
				tp->info.nEntries--; tp->info.sibling=INVALID_PAGEID;
			}
		}
		idx1=idx2=0; pref=NULL; lpref=0; l1=fVarSub?L_SHT:tp->info.fmt.keyLength()-tpmi->lPrefix;
		if (tp->info.lPrefix>tpmi->lPrefix) {
			if (tp->info.nEntries*(tp->info.lPrefix-tpmi->lPrefix)>tp->info.freeSpaceLength) return RC_PAGEFULL;	//???
			tp->changePrefixSize(tpmi->lPrefix);
		} else if (tp->info.lPrefix<tpmi->lPrefix) {
			if (tpmi->fLastR!=0 && tp->calcPrefixSize(0,tp->info.nEntries)>=tpmi->lPrefix) tp->changePrefixSize(tpmi->lPrefix);
			else {pref=rec+tpmi->lData+tp->info.lPrefix; lpref=tpmi->lPrefix-tp->info.lPrefix;}
		}
		n=fVarSub?TreePage::nKeys(rec):tpmi->lData/l1; lElt=tp->info.fmt.keyLength()-tp->info.lPrefix;
		do {
			if (fVarSub) {p1=TreePage::getK(rec,idx1); l1=TreePage::lenK(rec,idx1);} else p1=rec+l1*idx1;
			if (tp->findSubKey(p1,l1,idx2,pref,lpref)) {
				if (!tp->info.fmt.isRefKeyOnly()) return RC_ALREADYEXISTS;
				if ((rc=tp->adjustKeyCount(idx2,p1,l1,tpmi->lPrefix>0?rec+tpmi->lData:(byte*)0,tpmi->lPrefix))!=RC_OK) return rc;
				++idx2; ++idx1;
			} else {
				idx=idx1;
				if (idx2>=tp->info.nSearchKeys) idx1=n; else tp->findSubKey(rec,n,idx2,idx1,fVarSub,false);
				ushort nIns=ushort(idx1-idx);
				if (fVarSub) {
					l1=TreePage::lenKK(rec,idx,idx1); l2=nIns*L_SHT; ushort lIns=ushort(l1+l2+lpref*nIns);
					if (lIns<=tp->info.freeSpaceLength) tp->info.freeSpaceLength-=lIns; else return RC_PAGEFULL;
					ushort ll=((ushort*)(tp+1))[tp->info.nEntries],sht=((ushort*)(tp+1))[idx2],sh1=((ushort*)rec)[idx],k;
					if (sht<ll) memmove((byte*)(tp+1)+sht+l1,(byte*)(tp+1)+sht,ll-sht);
					memmove((ushort*)(tp+1)+idx2+nIns,(ushort*)(tp+1)+idx2,ll-idx2*L_SHT+(sht<ll?l1:0)); sht+=l2;
					for (k=0; k<idx2; k++) ((ushort*)(tp+1))[k]+=l2;
					for (k=0; k<nIns; k++) ((ushort*)(tp+1))[idx2+k]=((ushort*)rec)[idx+k]-sh1+sht;
					for (k=ushort(idx2); k<=tp->info.nEntries; k++) ((ushort*)(tp+1))[k+nIns]+=lIns;
					if (pref==NULL) memcpy((byte*)(tp+1)+sht,p1,l1);
					else for (k=0; k<nIns; k++) {
						ptr=(byte*)(tp+1)+(((ushort*)(tp+1))[idx2+k]+=ushort(lpref*k)); memcpy(ptr,pref,lpref);
						memcpy(ptr+lpref,TreePage::getK(rec,idx+k),TreePage::lenK(rec,idx+k));
					}
				} else {
					//...
				}
				tp->info.nSearchKeys+=nIns; tp->info.nEntries+=nIns;
			}
		} while (idx1<n);
		if ((tp->info.sibling=tpmi->sibling)!=INVALID_PAGEID) tp->info.nSearchKeys=tp->info.nEntries-1;
		break;
	case TRO_MULTIDEL:
		assert((flags&TXMGR_UNDO)==0);
		if (!tp->info.fmt.isKeyOnly() || lrec<=sizeof(TreePageMulti)) return RC_CORRUPTED;
		tpmi=(TreePageMulti*)rec; rec+=sizeof(TreePageMulti);
		if (tp->info.lPrefix>tpmi->lPrefix) return RC_NOTFOUND;
		idx1=idx2=0; pref=NULL; lpref=0; l1=fVarSub?L_SHT:tp->info.fmt.keyLength()-tpmi->lPrefix; fPR=tp->info.fmt.isRefKeyOnly();
		if (tp->info.lPrefix<tpmi->lPrefix) {pref=rec+tpmi->lData+tp->info.lPrefix; lpref=tpmi->lPrefix-tp->info.lPrefix;}
		n=fVarSub?TreePage::nKeys(rec):tpmi->lData/l1; l2=ushort(fVarSub?L_SHT:tp->info.fmt.keyLength()-tp->info.lPrefix);
		if (fVarSub) {p1=TreePage::getK(rec,idx1); l1=TreePage::lenK(rec,idx1);} else p1=rec+l1*idx1;
		do {
			if (idx2>=tp->info.nSearchKeys || !tp->findSubKey(p1,l1,idx2,pref,lpref)) return RC_NOTFOUND;
			if (!fVarSub) p2=(byte*)(tp+1)+l2*idx2; bool fAdjcnt=false; idx=idx2;
			do {
				if (tp->info.fmt.isRefKeyOnly()) {
					//ulong cnt2=PINRef::getCount(p1,l1,rec+tpmi->lData,tpmi->lPrefix);
					// if (cnt1<cnt2) return RC_NOTFOUND; if (cnt1>cnt2) {fAdjcnt=true; break;}
				}
				if (++idx1>=n || ++idx2>=tp->info.nSearchKeys) break;
				if (!fVarSub) {p1+=l1; p2+=l2;}
				else {p1=TreePage::getK(rec,idx1); l1=TreePage::lenK(rec,idx1); p2=(*tp)[idx2]; l2=tp->lenK(idx2);}
			} while (fPR && PINRef::cmpPIDs(p1,l1,p2,l2)==0 || !fPR && (pref==NULL || l2>=lpref && memcmp(pref,p2,lpref)==0) && l1+lpref==l2 && memcmp(p1,p2+lpref,l1)==0);
			if (idx2>idx) {
				if (fVarSub) {
					ushort *p=(ushort*)(tp+1)+idx; ulong sht=*p,nd=idx2-idx; l=p[nd]-p[0]; ll=((ushort*)(tp+1))[tp->info.nEntries];
					if (sht+l<ll) memmove((byte*)(tp+1)+sht,(byte*)(tp+1)+sht+l,ll-l-sht);
					memmove(p,p+nd,ll-l-(idx+1)*L_SHT); p=(ushort*)(tp+1);
					for (ulong j=tp->info.nEntries; --j>=idx;) p[j]-=ushort(l+L_SHT*nd);
					for (long k=(long)idx; --k>=0;) p[k]-=ushort(L_SHT*nd);
				} else {
					if (idx2<tp->info.nEntries) memcpy((byte*)(tp+1)+idx*l1,(byte*)(tp+1)+idx2*l1,(tp->info.nEntries-idx2)*l1);
					l=ushort(l1*(idx2-idx));
				}
				tp->info.nEntries-=ushort(idx2-idx); tp->info.nSearchKeys-=ushort(idx2-idx); tp->info.freeSpaceLength+=l;
			}
			if (fAdjcnt) {
				//if ((rc=tp->adjustKeyCount(idx,(byte*)(tpm+1)+2,((byte*)(tpm+1))[1],NULL,0,true))!=RC_OK) return rc;
				idx1++; idx2++;
			}
		} while (idx1<n);
		if (tp->info.nEntries==0) tp->info.freeSpaceLength+=L_SHT;
		if (tp->info.lPrefix>0) {
			if (tp->info.nEntries==0) {tp->info.freeSpaceLength+=tp->info.lPrefix; tp->info.lPrefix=0;}
			else {
				ushort newPrefSize=tp->calcPrefixSize(0,tp->info.nEntries); assert(newPrefSize>=tp->info.lPrefix);
				if (newPrefSize>tp->info.lPrefix) tp->changePrefixSize(newPrefSize);
			}
		}
		break;
	case TRO_REPLACE:
		if (!tp->info.fmt.isKeyOnly() || !fVarSub && lrec!=ulong(tp->info.fmt.keyLength()*2)) {
			report(MSG_ERROR,"TreePageMgr::update replace: incorrect page type, page %08X\n",tp->hdr.pageID);
			return RC_CORRUPTED;
		}
		if (!fVarSub) {p1=rec; l1=l2=tp->info.fmt.keyLength(); delta=0;}
		else {
			p1=rec+sizeof(uint16_t); l1=*(ushort*)rec; if (lrec<l1+sizeof(uint16_t)) return RC_CORRUPTED;
			l2=ushort(lrec-l1-sizeof(uint16_t)); if (rec[lrec-1]==0xFF) {if (l2<rec[lrec-3]) return RC_CORRUPTED; l2-=rec[lrec-3];}
			if ((delta=l2-l1)>(int)tp->info.freeSpaceLength) {
				report(MSG_ERROR,"TreePageMgr::update replace: insufficient space, req=%d, page %08X\n",l2-l1,tp->hdr.pageID);
				return RC_PAGEFULL;
			}
		}
		p2=p1+l1; icmp=0;
		if ((flags&(TXMGR_UNDO|TXMGR_RECV))!=0) {
			if ((flags&TXMGR_UNDO)!=0) {const byte *pp=p1; p1=p2; p2=pp; ll=l1; l1=l2; l2=ll;}
			if (!tp->findSubKey(p1,l1,idx)) {
				report(MSG_ERROR,"TreePageMgr::update replace: key not found, page %08X\n",l2-l1,tp->hdr.pageID);
				return RC_NOTFOUND;
			}
		}
		if (tp->info.lPrefix>0) {
			const byte *pref=fVarSub?(byte*)tp+((PagePtr*)&tp->info.prefix)->offset:(byte*)&tp->info.prefix;
			for (l=min(l2,tp->info.lPrefix),ll=0; ll<l && (icmp=*p2-*pref)==0; ++p2,++pref,++ll);
			if ((l1-=ll,l2-=ll)==0) icmp=-1;
			else if (ll<tp->info.lPrefix) {
				if ((tp->info.lPrefix-ll)*tp->info.nEntries>tp->info.freeSpaceLength) {
					report(MSG_ERROR,"TreePageMgr::update replace: insufficient space, newPrefSize=%d, page %08X\n",ll,tp->hdr.pageID);
					return RC_PAGEFULL;
				} 
				tp->changePrefixSize(ll); assert(icmp!=0);
			}
		}
		ptr=(byte*)(tp+1)+idx*(l=fVarSub?L_SHT:tp->info.fmt.keyLength()-tp->info.lPrefix);
		if (icmp==0) {
			assert(0&&"TRO_REPLACE 0");
#if 0
		int			cmp2(const byte *p2,ushort l2,const byte *ptr,ushort l,bool f) const
						{if (f) {l=((ushort*)ptr)[1]-((ushort*)ptr)[0]; ptr=(byte*)(this+1)+*(ushort*)ptr;} int i=memcmp(p2,ptr,min(l2,l)); return i!=0?i:cmp3(l2,l);}
		 || icmp==0 && (idx==0 || tp->cmp2(p2,l2,ptr-l,l,fVarSub)>0) 
						&& (idx+1==tp->info.nSearchKeys || tp->cmp2(p2,l2,ptr+l,l,fVarSub)<0)) {
#endif
		}
		if (icmp<0 && idx==0 || icmp>0 && idx+1==tp->info.nSearchKeys) {
			if (delta!=0) {
				assert(fVarSub);
				if (idx+1<tp->info.nEntries) {
					byte *p=(byte*)(tp+1)+((ushort*)ptr)[1];
					memmove(p+delta,p,((ushort*)(tp+1))[tp->info.nEntries]-((ushort*)ptr)[1]);
				}
				for (uint16_t *p=(ushort*)ptr; ++idx<=tp->info.nEntries; *++p+=(uint16_t)delta);
			}
			memcpy(fVarSub?(byte*)(tp+1)+*(ushort*)ptr:ptr,p2,l2);
		} else {
			if (icmp<0) n=0;
			else if (icmp>0) n=tp->info.nSearchKeys;
			else if (tp->findSubKey(p2,l2,n)) {
				report(MSG_ERROR,"TreePageMgr::update replace: key already exists, page %08X\n",ll,tp->hdr.pageID);
				return RC_ALREADYEXISTS;
			}
			assert(n!=idx && n!=idx+1);
			if (n<idx) {
				//...
			} else {
				//...
			}
			if (fVarSub) {
				assert(0&&"TRO_REPLACE 1");
				ulong sht=*(ushort*)ptr; l=TreePage::lenK(ptr,0); ll=((ushort*)(tp+1))[tp->info.nEntries];
				if (sht+l<ll) memmove((byte*)(tp+1)+sht,(byte*)(tp+1)+sht+l,ll-l-sht);
				memmove(ptr,ptr+L_SHT,ll-l-(idx+1)*L_SHT);
				idx=n>idx?n-1:n;
#if 0
				assert(lkey+L_SHT<=info.freeSpaceLength); ushort *p=(ushort*)(this+1);
				ulong sht=*(ushort*)ptr,lh=p[0],l=p[lh/L_SHT-1]; int off=int((ushort*)ptr-p);
				if (sht<l) memmove((byte*)p+sht+lkey,(byte*)p+sht,l-sht);
				memcpy((byte*)p+sht,(byte*)key.getPtr()+info.lPrefix,lkey);
				memmove((ushort*)ptr+1,ptr,l-off*L_SHT+lkey);
		
				for (int j=tp->info.nEntries; --j>=0;) p[j]-=(ulong)j>=idx?l+L_SHT:L_SHT;
				for (int j=lh/L_SHT+1; --j>=0; ) p[j]+=j>off?lkey+L_SHT:L_SHT;
#endif
			} else {
				assert(0&&"TRO_REPLACE 2");
				//???
			}
		}
		tp->info.freeSpaceLength-=delta; break;
	}
	if (tp->info.level>TREE_MAX_DEPTH || tp->info.nEntries!=tp->info.nSearchKeys+(tp->info.sibling!=INVALID_PAGEID?1:0)) {
		report(MSG_ERROR,"Invalid nEntries/nSearchKeys %d/%d after op %d, page %08X\n",tp->info.nEntries,tp->info.nSearchKeys,op,tp->hdr.pageID);
		return RC_CORRUPTED;
	}
#ifdef _DEBUG
	assert(!tp->info.fmt.isPrefNumKey()||tp->info.lPrefix==0||(tp->info.prefix&(1<<(sizeof(uint64_t)-tp->info.lPrefix)*8)-1)==0);
	if (!tp->info.fmt.isFixedLenKey()||!tp->info.fmt.isFixedLenData()) tp->compact(true);
#endif
	return RC_OK;
}

bool TreePageMgr::TreePage::insertValues(const byte *p1,ushort ll,MVStoreKernel::PagePtr *vp,ushort off,ulong idx,bool fVM,ushort n,const uint16_t *shs)
{
	ushort n2=n*L_SHT; ushort l=ll,l2; byte *ptr=NULL; assert(n==1 || shs!=NULL);
	if (fVM) l+=vp->len>=256?n2:vp->len==0?ll+n+1<256?n+1:n2+L_SHT:vp->len+n+ll<256?n:((byte*)this+vp->offset)[0]+n2;
	if (l>info.freeSpaceLength+info.scatteredFreeSpace) {
		report(MSG_ERROR,"TreePageMgr insertValues: insufficient space, requested %d, page %08X\n",l,hdr.pageID);
		return false;
	}
	if (vp->offset==info.freeSpace && info.freeSpaceLength>=l) {
		info.freeSpace-=l; info.freeSpaceLength-=l;
		memmove((byte*)this+info.freeSpace,(byte*)this+vp->offset,vp->len); 
		vp->offset=info.freeSpace; ptr=(byte*)this+info.freeSpace;
	} else if (info.freeSpaceLength>=vp->len+l) {
		info.freeSpace-=vp->len+l; info.freeSpaceLength-=vp->len+l;
		memmove((byte*)this+info.freeSpace,(byte*)this+vp->offset,vp->len);
		info.scatteredFreeSpace+=vp->len; vp->offset=info.freeSpace; ptr=(byte*)this+info.freeSpace;
	} else {
		compact(false,idx,l); ptr=(byte*)findData(idx,l2,(const PagePtr**)&vp);
		assert(ptr!=NULL && vp!=NULL && vp->len>=l); vp->len-=l;
	}
	if (!fVM) {
		assert(ptr==(byte*)this+vp->offset);
		if (off<vp->len) memmove(ptr+off+l,ptr+off,vp->len-off);
		memcpy(ptr+off,p1,l);
	} else if (vp->len==0) {
		assert(n==1);
		if (ll+2<256) {ptr[0]=2; ptr[1]=ll+2; memcpy(ptr+2,p1,ll);} else {_set_u16(ptr,L_SHT*2); _set_u16(ptr+L_SHT,ll+L_SHT*2); memcpy(ptr+L_SHT*2,p1,ll);}
	} else {
		ulong sht=vp->len<256?ptr[off]:_u16(ptr+off); int j;
		if (sht<vp->len) memmove(ptr+sht+ll,ptr+sht,vp->len-sht);
		memcpy(ptr+sht,p1,ll);
		if (vp->len>=256) {
			memmove(ptr+off+n2,ptr+off,vp->len-off+ll);
			for (ushort k=1; k<n; k++) {ushort sh=shs[k]-shs[0]+ushort(sht); _set_u16(ptr+off+k*L_SHT,sh);}
			for (j=_u16(ptr)+n2; (j-=L_SHT)>=off+n2; ) {sht=_u16(ptr+j)+ll+n2; _set_u16(ptr+j,sht);}
			for (;j>=0;j-=L_SHT) {sht=_u16(ptr+j)+n2; _set_u16(ptr+j,sht);}
		} else if (vp->len+n+ll<256) {
			memmove(ptr+off+n,ptr+off,vp->len-off+ll);
			for (ushort k=1; k<n; k++) ptr[off+k]=byte(shs[k]-shs[0]+sht);
			for (j=*ptr+n; --j>=off+n; ) ptr[j]+=ll+n; for (;j>=0;--j) ptr[j]+=n;
		} else {
			sht=*ptr; memmove(ptr+sht*L_SHT+n2,ptr+sht,vp->len-sht+ll); ulong k,m;
			for (j=sht; --j>=off;) {k=ptr[j]+sht+n2+ll; m=j*L_SHT+n2; _set_u16(ptr+m,k);}
			k=ptr[off]+sht+n2; m=off*L_SHT; _set_u16(ptr+m,k);
			if (n>1) for (ushort sh=shs[0],o=1; o<n; o++) {m=shs[o]-sh+k; _set_u16(ptr+off*L_SHT+o*L_SHT,m);}
			for (;j>=0;--j) {k=ptr[j]+sht+n2; m=j*L_SHT; _set_u16(ptr+m,k);}
		}
	}
	vp->len+=l; return true;
}

void TreePageMgr::TreePage::deleteValues(ushort l,PagePtr *vp,ushort off,bool fVM,ushort n)
{
	byte *ptr=(byte*)this+vp->offset;
	if (fVM) {
		ushort n2=n*L_SHT;
		if (vp->len<256 && *ptr==n+1) {assert(vp->len==l+n+1); info.scatteredFreeSpace+=vp->len; vp->len=0;}
		else if (vp->len>=256 && _u16(ptr)==n2+L_SHT) {assert(vp->len==l+n2+L_SHT); info.scatteredFreeSpace+=vp->len; vp->len=0;}
		else {
			ulong sht=vp->len>=256?_u16(ptr+off):ptr[off];
			if (sht+l<vp->len) memmove(ptr+sht,ptr+sht+l,vp->len-sht-l);
			if (vp->len<256) {
				int j=*ptr-1; l+=n; memmove(ptr+off,ptr+off+n,vp->len-off-l);
				while (--j>=0) ptr[j]-=j>=off?l:n;
			} else if (vp->len-l-n2<256) {		// better check?
				int k=_u16(ptr)/L_SHT-1; l+=n2+k;
				for (int j=0,m=0; j<k; ++j,m+=L_SHT) {if (m==off) m+=n2; ptr[j]=byte(_u16(ptr+m)-(m>=off?l:k+L_SHT)); assert(j==0 || ptr[j]>ptr[j-1]);}
				memmove(ptr+k,ptr+(k+1)*L_SHT,vp->len-k-l+n2);
			} else {
				int j=_u16(ptr)-L_SHT; l+=n2; memmove(ptr+off,ptr+off+n2,vp->len-off-l);
				while ((j-=L_SHT)>=0) {sht=_u16(ptr+j)-(j>=off?l:n2); assert(sht<=ulong(vp->len-l)); _set_u16(ptr+j,sht);}
			}
			vp->len-=l; info.scatteredFreeSpace+=l;
		}
	} else if ((vp->len-=l)!=0) {
		if (vp->len>off) memcpy(ptr,ptr+l,vp->len-off);
		info.scatteredFreeSpace+=l;
	} else if (vp->offset!=info.freeSpace) info.scatteredFreeSpace+=l;
	else {info.freeSpace+=l; info.freeSpaceLength+=l;}
}

RC TreePageMgr::TreePage::adjustCount(byte *ptr,PagePtr *vp,ulong idx,const byte *p,size_t lp,bool fDec)
{
	if (!info.fmt.isPinRef()) {
		if (!fDec) return RC_ALREADYEXISTS;
		//deleteValues(
		return RC_OK;
	}
	size_t sht,l,l0; byte buf[XPINREFSIZE]; RC rc; int j; ushort ll,off; int dl;
	if (vp->len<256) {sht=ptr[0]; l0=l=ptr[1]-sht;} else {sht=_u16(ptr); l0=l=_u16(ptr+L_SHT)-sht;}
	switch (rc=PINRef::adjustCount((byte*)this+vp->offset+sht,l,PINRef::getCount(p,lp),buf,fDec)) {
	default: return rc;
	case RC_TRUE:
		off=ushort(ptr-((byte*)this+vp->offset));
		dl=int(l-l0); assert(fDec && dl<0 || !fDec && dl>0);
		if (dl<0) info.scatteredFreeSpace+=ushort(-dl);
		else {
			int total=vp->len>=256||vp->len+dl<256?dl:dl+*((byte*)this+vp->offset)*(L_SHT-1);
			if (total>info.freeSpaceLength+info.scatteredFreeSpace) {
				report(MSG_ERROR,"TreePageMgr adjustCount: insufficient space, requested %d, page %08X\n",total,hdr.pageID);
				return RC_PAGEFULL;
			}
			if (vp->offset==info.freeSpace && info.freeSpaceLength>=total) {
				info.freeSpace-=ushort(total); info.freeSpaceLength-=ushort(total);
				memmove((byte*)this+info.freeSpace,(byte*)this+vp->offset,vp->len);
				vp->offset=info.freeSpace;
			} else if (info.freeSpaceLength>=vp->len+total) {
				info.freeSpace-=vp->len+ushort(total); info.freeSpaceLength-=vp->len+ushort(total);
				memmove((byte*)this+info.freeSpace,(byte*)this+vp->offset,vp->len);
				info.scatteredFreeSpace+=vp->len; vp->offset=info.freeSpace;
			} else {
				compact(false,idx,ushort(total)); ptr=(byte*)findData(idx,ll,(const PagePtr**)&vp);
				assert(ptr!=NULL && vp!=NULL && vp->len>=ushort(total)); vp->len-=ushort(total);
			}
		}
		ptr=(byte*)this+vp->offset;
		if (sht+l0<vp->len) memmove(ptr+sht+l,ptr+sht+l0,vp->len-sht-l0);
		memcpy(ptr+sht,buf,l);
		if (vp->len<256 && vp->len+dl<256)
			for (j=*ptr; --j>off; ) ptr[j]+=byte(dl);
		else if (vp->len>=256 && vp->len+dl>=256)
			for (j=_u16(ptr); (j-=L_SHT)>off; ) {ll=_u16(ptr+j)+dl; _set_u16(ptr+j,ll);}
		else if (vp->len>=256) {
			int k=nKeys((const byte*)this+vp->offset),dk=(k+1)*(L_SHT-1); byte *to=(byte*)this+vp->offset,*from=to;
			for (int j=0; j<=k; ++j,from+=L_SHT,++to) {ushort sh=_u16(from); *to=byte(sh-dk+(sh>sht?dl:0));}
			memcpy((byte*)this+vp->offset+k+1,(byte*)this+vp->offset+(k+1)*L_SHT,vp->len-(k+1)*L_SHT+dl);
			info.scatteredFreeSpace+=dk; dl-=dk;
		} else {
			byte *to=(byte*)this+vp->offset,*from=to; int k=*to,dk=k*(L_SHT-1);
			memmove(to+k*L_SHT,to+k,vp->len-k+dl); to+=L_SHT*k; from+=k;
			for  (int j=0; j<k; j++) {to-=L_SHT; ushort sh=*--from; sh+=dk+(sh>sht?dl:0); _set_u16(to,sh);}
			dl+=dk;
		}
		vp->len=ushort(vp->len+dl);
#ifdef  _DEBUG
		compact(true);
#endif
		break;
	case RC_FALSE:
		assert(fDec);
		deleteValues((ushort)l0,vp,ushort(ptr-(byte*)this-vp->offset),true);
		break;
	}
	return RC_OK;
}

RC TreePageMgr::TreePage::adjustKeyCount(ulong idx,const byte *p2,size_t l2,const byte *pref,size_t lpref,bool fDec)
{
	assert(info.fmt.isRefKeyOnly());
	uint32_t cnt=1; byte buf[XPINREFSIZE],buf2[XPINREFSIZE];
	if (pref==NULL || lpref==0) cnt=PINRef::getCount(p2,l2);
	else {
		if (lpref+l2>XPINREFSIZE) return RC_CORRUPTED;
		memcpy(buf,pref,lpref); memcpy(buf+lpref,p2,l2);
		cnt=PINRef::getCount(buf,lpref+l2);
	}
	byte *p=(*this)[idx]; size_t l=lenK(idx),l0=l; 
	if (info.lPrefix>0) {
		if (info.lPrefix+l>XPINREFSIZE) return RC_CORRUPTED;
		memcpy(buf,(byte*)this+((PagePtr*)&info.prefix)->offset,info.lPrefix);
		memcpy(buf+info.lPrefix,p,l); l0=l+=info.lPrefix; p=buf;
	}
	ushort *pp=(ushort*)(this+1)+idx,ll=((ushort*)(this+1))[info.nEntries];
	ulong sht=*pp; RC rc; int dl,j;
	switch (rc=PINRef::adjustCount(p,l,cnt,buf2,fDec)) {
	default: return rc;
	case RC_TRUE:
		if ((dl=int(l-l0))>0 && ulong(dl)>info.freeSpaceLength) return RC_PAGEFULL;
		l0-=info.lPrefix; l-=info.lPrefix;
		if (sht+l0<ll) memmove((byte*)(this+1)+sht+l,(byte*)(this+1)+sht+l0,ll-l0-sht);
		for (j=info.nEntries+1; --j>(int)idx;) ((ushort*)(this+1))[j]+=ushort(dl);
		memcpy((byte*)(this+1)+sht,buf2+info.lPrefix,l); info.freeSpaceLength-=ushort(dl);
		break;
	case RC_FALSE:
		assert(fDec); l0-=info.lPrefix;
		if (sht+l0<ll) memmove((byte*)(this+1)+sht,(byte*)(this+1)+sht+l0,ll-l0-sht);
		memmove(pp,pp+1,ll-l0-(idx+1)*L_SHT); pp=(ushort*)(this+1);
		for (j=info.nEntries; --j>=(int)idx;) pp[j]-=ushort(l0+L_SHT); for (;j>=0;--j) pp[j]-=L_SHT;
		info.freeSpaceLength+=ushort(l0+L_SHT); --info.nSearchKeys;
		if (--info.nEntries==0) info.freeSpaceLength+=L_SHT;
		if (info.lPrefix>0 && info.nEntries==0) {info.freeSpaceLength+=info.lPrefix; info.lPrefix=0;}
		else if (idx==0 || idx==info.nEntries) {
			ushort newPrefSize=calcPrefixSize(0,info.nEntries); assert(newPrefSize>=info.lPrefix);
			if (newPrefSize>info.lPrefix) changePrefixSize(newPrefSize);
		}
		break;
	}
	return RC_OK;
}

PageID TreePageMgr::multiPage(ulong info,const byte *rec,size_t lrec,bool& fMerge)
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

RC TreePageMgr::undo(ulong info,const byte *rec,size_t lrec,PageID pid)
{
	Tree *tree=NULL; RC rc=RC_OK; SearchKey key; byte *buf=NULL; size_t ll;
	TREE_OP op; ushort nl=0,ol=0,off=0; const byte *newV=NULL,*oldV=NULL; bool fMulti=false;
	if (pid!=INVALID_PAGEID) {
		op=(TREE_OP)(info&TRO_MASK); if (lrec<=2) return RC_CORRUPTED;
		if (rec[lrec-1]==0xFF) {
			TreeFactory *factory=ctx->treeMgr->getFactory(rec[lrec-2]); if (factory==NULL) return RC_CORRUPTED;
			size_t l=rec[lrec-4]<<8|rec[lrec-3]; const byte *p=rec+lrec-l; if ((rc=key.deserialize(p,l,&ll))!=RC_OK) return rc;
			if ((rc=factory->createTree(p+ll,(byte)(l-ll-4),tree))!=RC_OK) return rc;
			const TreePageModify *tpm; const TreePageMulti *tpmi;
			switch (op) {
			case TRO_MULTINIT:
			case TRO_MULTIINS:
			case TRO_MULTIDEL:
				tpmi=(TreePageMulti*)rec; newV=rec+sizeof(TreePageMulti); nl=tpmi->lData;
				if (tpmi->lPrefix!=0) {
					bool fVarSub=tpmi->fmt.isVarKeyOnly();
					ulong n=fVarSub?TreePage::nKeys(newV):nl/(tpmi->fmt.keyLength()-tpmi->lPrefix); if (n==0) return RC_CORRUPTED;
					if ((buf=(byte*)malloc(nl=ushort(tpmi->lData+n*tpmi->lPrefix),SES_HEAP))==NULL) return RC_NORESOURCES;
					const byte *from=newV,*pref=rec+sizeof(TreePageMulti)+tpmi->lData; byte *to=buf; newV=buf;
					if (!fVarSub) for (ulong i=0,lElt1=tpmi->fmt.keyLength(),lElt2=lElt1-tpmi->lPrefix; i<n; i++) {
						memcpy(to,pref,tpmi->lPrefix); memcpy(to+tpmi->lPrefix,from,lElt2); to+=lElt1; from+=lElt2;
					} else for (ulong i=0,sht=((ushort*)to)[0]=((ushort*)from)[0]; i<n; i++) {
						ushort l=TreePage::lenK(from,0); memcpy(buf+sht,pref,tpmi->lPrefix);
						memcpy(buf+sht+tpmi->lPrefix,rec+sizeof(TreePageMulti)+((ushort*)from)[0],l);
						from+=L_SHT; to+=L_SHT; *(ushort*)to=ushort(sht+=l+tpmi->lPrefix);
					}
				}
				op=TRO_INSERT; fMulti=true;
				if (op==TRO_MULTIDEL) {op=TRO_DELETE; oldV=newV; ol=nl; newV=NULL; nl=0;}
				break;
			case TRO_REPLACE:
				op=TRO_UPDATE; oldV=rec+sizeof(uint16_t); ol=*(uint16_t*)rec;
				newV=oldV+ol; nl=ushort(lrec-ol-sizeof(uint16_t)-rec[lrec-3]);
				break;
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
			if (op==TRO_EDIT) {
				const TreePageEdit *tpe=(const TreePageEdit*)rec; key.deserialize(tpe+1,lrec-l-sizeof(TreePageEdit));
				oldV=rec+tpe->oldData.offset; ol=tpe->oldData.len; nl=tpe->newData.len; off=(ushort)tpe->shift;
			} else {
				const TreePageModify *tpm=(const TreePageModify*)rec; key.deserialize(tpm+1,lrec-l-sizeof(TreePageModify));
				newV=rec+tpm->newData.offset; if ((nl=tpm->newData.len)==0) op=TRO_DELETE;
				oldV=rec+tpm->oldData.offset; if ((ol=tpm->oldData.len)==0) op=TRO_INSERT;
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
		if (lrec<sizeof(ushort)*2) return RC_CORRUPTED;
		memcpy(&nl,rec,sizeof(ushort));
		memcpy(&off,rec+sizeof(ushort),sizeof(ushort));
		if (lrec<sizeof(ushort)*2+nl) return RC_CORRUPTED;
		newV=rec+sizeof(ushort)*2; oldV=newV+nl; 
		ol=ushort(lrec-sizeof(ushort)*2-nl);
	}
	TreeCtx tctx(*tree); assert(tree!=NULL);
	if ((rc=tctx.findPageForUpdate(&key,op==TRO_DELETE))!=RC_OK) rc=op==TRO_INSERT?RC_OK:rc;
	else switch (op) {
	default: rc=RC_CORRUPTED; break;
	case TRO_INSERT: rc=remove(tctx,key,newV,nl,fMulti); break;
	case TRO_DELETE: rc=insert(tctx,key,oldV,ol,fMulti); break;
	case TRO_UPDATE: rc=update(tctx,key,newV,nl,oldV,ol); break;
	case TRO_EDIT: rc=edit(tctx,key,oldV,ol,nl,off); break;
	}
	tree->destroy(); if (buf!=NULL) free(buf,SES_HEAP);
	return rc==RC_TRUE||rc==RC_FALSE ? RC_OK : rc;
}

void TreePageMgr::unchain(PBlock *pb0,PBlock *pb) {
	if (pb==NULL) pb=pb0;
#if 0
	RC rc=RC_OK; PBlock *pb2; Session *ses=Session::getSession(); bool fRelease=false;
	while (pb0->isDependent()) {
		const TreePage *tp=(const TreePage*)pb->getPageBuf();
		if (tp->info.sibling==INVALID_PAGEID) {rc=RC_FALSE; break;}			// assert?
		if (ses!=NULL && (pb2=ses->getLatched(tp->info.sibling))!=NULL) {
			if (!pb2->isULocked() && !pb2->isXLocked()) {rc=RC_FALSE; break;}
			if (fRelease) pb->release(PGCTL_NOREG); pb=pb2; fRelease=false;
		} else if ((pb=ctx->bufMgr->getPage(tp->info.sibling,this,
			PGCTL_COUPLE|PGCTL_XLOCK|PGCTL_NOREG|QMGR_INMEM,fRelease?pb:(PBlock*)0))!=NULL) fRelease=true;
		else {
			rc=RC_FALSE; break;
		}
		if (pb->getDependent()==pb0) {
			if (pb->isDependent() && ((rc=unchain(pb))!=RC_OK||pb->isDependent())) break;
			if (pb->isULocked()) pb->upgradeLock(); pb->flushPage();
		}
	}
	if (pb!=NULL && fRelease) pb->release(PGCTL_NOREG);
#endif
}

RC TreePageMgr::initPage(PBlock *pb,IndexFormat ifmt,ushort level,const SearchKey* key,PageID pid0,PageID pid1)
{
	ulong lrec=sizeof(TreePageInit)+(key!=NULL?key->extLength():0); 
	TreePageInit *tpi=(TreePageInit*)alloca(lrec); if (tpi==NULL) return RC_NORESOURCES;
	tpi->fmt=ifmt; tpi->level=level;
	if (key==NULL) {assert(level==0); tpi->left=tpi->right=INVALID_PAGEID;}
	else {
		assert(level>0 && level<=TREE_MAX_DEPTH && pid0!=INVALID_PAGEID && pid1!=INVALID_PAGEID);
		tpi->left=pid0; tpi->right=pid1; tpi->fmt.makeInternal(); key->serialize(tpi+1);
	}
	return ctx->txMgr->update(pb,this,TRO_INIT,(byte*)tpi,lrec);
}

ushort TreePageMgr::TreePage::calcSplitIdx(bool &fInsR,const SearchKey& key,ulong idx,size_t lInsert,ushort prefixSize,bool fNewKey) const
{
	ushort splitIdx=(ushort)idx; fInsR=true; ulong level=info.level;
	if (info.nSearchKeys<=1) fInsR=idx>0;
	else if (idx!=info.nEntries) {
		ulong delta=~0ul; ushort lElt=info.calcEltSize();
		ulong d1=idx==0?info.lPrefix-prefixSize:0,d2=idx>=info.nSearchKeys?info.lPrefix-prefixSize:0;
		bool fFixedKey=info.fmt.isFixedLenKey(),fFixedData=info.fmt.isFixedLenData();
		if (fFixedKey && fFixedData) {
			for (ushort imin=0,imax=info.nSearchKeys,iprev=0xffff; imax!=imin;) {
				ushort i=(imax+imin)/2; if (i==iprev) break; else iprev=i;
				long d=long((i*2-info.nEntries)*(lElt+info.fmt.dataLength()))+
						d1*i-d2*(info.nEntries-i)+long(idx<=i?lInsert-prefixSize:prefixSize-lInsert);
				if (d<=0) {imin=i; if (ulong(-d)<delta) {delta=-d; splitIdx=i;}} else {imax=i; if (ulong(d)<delta) {delta=d; splitIdx=i;}}
			}
			fInsR=idx>=splitIdx;
		} else if (info.fmt.isVarKeyOnly()) {
			const ushort *pp=(const ushort*)(this+1); ushort total=pp[nKeys(pp)];
			for (ushort imin=0,imax=info.nSearchKeys,iprev=0xffff; imax!=imin;) {
				ushort i=(imax+imin)/2; if (i==iprev) break; else iprev=i;
				long d=(pp[i]-(info.nEntries-i)*L_SHT)*2-total+d1*i-d2*(info.nEntries-i)+long(idx<=i?lInsert-prefixSize:prefixSize-lInsert);
				if (d<=0) {imin=i; if (ulong(-d)<delta) {delta=-d; splitIdx=i;}} else {imax=i; if (ulong(d)<delta) {delta=d; splitIdx=i;}}
			}
			fInsR=idx>=splitIdx;
		} else {
			ushort i=0,sht=info.keyLength();
			size_t total=hdr.length()-sizeof(TreePage)-FOOTERSIZE-info.freeSpaceLength-info.scatteredFreeSpace,l=0; 
			for (const byte *p=(const byte*)(this+1);;l+=lElt,p+=lElt) {
				long d=long(l*2-total)+d1*i-d2*(info.nEntries-i);
				if (idx<i) d+=long(lInsert-prefixSize); else if (idx>i||!fNewKey) d-=long(lInsert-prefixSize);
				else {
					long dd1=d+long(lInsert-prefixSize),dd2=d-long(lInsert-prefixSize);
					if (dd1>0&&(dd2>=0||-dd2<dd1)) d=dd2; else if (ulong((d=dd1)<0?-dd1:dd1)<delta) fInsR=false;
				} 
				if (d<0) {if (ulong(-d)<delta) {delta=-d; splitIdx=i;}} else {if (ulong(d)<delta) splitIdx=i; break;}
				if (++i>info.nSearchKeys||!fNewKey&&i==info.nSearchKeys) break;
				if (!fFixedKey) l+=((PagePtr*)p)->len;
				if (!fFixedData && i<=info.nSearchKeys) l+=length(*(PagePtr*)(p+sht));
			}
			if (idx<splitIdx) fInsR=false; else if (!fNewKey) fInsR=idx>=splitIdx;
		}
	} else if (!info.fmt.isSeq() || level>0) {
		size_t le=info.fmt.isSeq()?sizeof(uint32_t):info.calcEltSize();
		if (!info.fmt.isFixedLenKey()) le+=key.extra()-prefixSize;
		if (prefixSize<info.lPrefix) le+=info.extraEltSize(prefixSize)*info.nEntries;
		if (le>ulong(info.freeSpaceLength+info.scatteredFreeSpace)) splitIdx--;	// more???
	}
	return splitIdx;
}

RC TreePageMgr::split(TreeCtx& tctx,const SearchKey *key,ulong& idx,ushort splitIdx,bool fInsR,PBlock **pRight)
{
	const TreePage *tp=(const TreePage*)tctx.pb->getPageBuf();
	bool fDKey=splitIdx==idx && fInsR; ulong level=tp->info.level; 
	assert(tp->info.nSearchKeys!=0); assert(!fDKey || key!=NULL);

	if (level==0 && splitIdx>0 && !tp->info.fmt.isFixedLenKey() && !tp->info.fmt.isRefKeyOnly()) {
		ushort lkey=fDKey?key->v.ptr.l:tp->getKeyExtra(splitIdx); assert(key==NULL||key->type>=KT_BIN&&key->type<KT_ALL);
		ushort lTrunc=fDKey?tp->calcPrefixSize(*key,splitIdx-1,true):tp->calcPrefixSize(splitIdx-1,splitIdx+1);
		if (lTrunc!=0) {
			lTrunc++;
			if (splitIdx==idx && !fInsR && key!=NULL) {ushort lk=tp->calcPrefixSize(*key,splitIdx,true)+1; if (lk>lTrunc) lTrunc=lk;}
			assert(lTrunc<=lkey);
			if (lTrunc<lkey) {
				SearchKey *sk=(SearchKey*)alloca(sizeof(SearchKey)+lkey); if (sk==NULL) return RC_NORESOURCES;
				if (fDKey) *sk=*key; else {fDKey=true; tp->getKey(splitIdx,*sk);} sk->v.ptr.l=lTrunc; key=sk;
			}
		}
	}
	if (!fDKey) {
		SearchKey *sk=(SearchKey*)alloca(sizeof(SearchKey)+tp->getKeyExtra(splitIdx));
		if (sk==NULL) return RC_NORESOURCES; tp->getKey(splitIdx,*sk); key=sk;
	}
	ulong lsib=sizeof(TreePageSplit)+key->extLength();

	TreePageSplit *tps=(TreePageSplit*)alloca(lsib); if (tps==NULL) return RC_NORESOURCES;
	tps->nEntriesLeft=splitIdx; tps->oldPrefSize=tp->info.lPrefix; key->serialize(tps+1);
	if (fInsR) {assert(idx>=splitIdx); idx-=splitIdx;}
	assert(idx<=ulong(fInsR?tp->info.nEntries-splitIdx:splitIdx));

	MiniTx tx(Session::getSession(),(tctx.mode&TF_SPLITINTX)!=0?MTX_SKIP:0); PBlock *pbn; RC rc;
	if ((rc=ctx->fsMgr->allocPages(1,&tps->newSibling))!=RC_OK) return rc;
	assert(tps->newSibling!=INVALID_PAGEID);
	if ((pbn=ctx->bufMgr->newPage(tps->newSibling,this))==NULL) return RC_NORESOURCES;

	if ((rc=ctx->txMgr->update(tctx.pb,this,TRO_SPLIT,(byte*)tps,lsib,0,pbn))!=RC_OK)
		{pbn->release(PGCTL_DISCARD); return rc;}

#ifdef TRACE_EMPTY_PAGES
	if (tctx.mainKey!=NULL && tctx.depth>tctx.leaf) {
		report(MSG_DEBUG,"Split to pages %X(%f) and %X(%f)\n",
			tctx.pb->getPageID(),double(xSize-((TreePage*)tctx.pb->getPageBuf())->info.freeSpaceLength)*100./xSize,
			pbn->getPageID(),double(xSize-((TreePage*)pbn->getPageBuf())->info.freeSpaceLength)*100./xSize);
	}
#endif
	if (pRight!=NULL) *pRight=pbn; else if (fInsR) {tctx.pb.release(); tctx.pb=pbn;} else pbn->release();

	PageID newSibling=tps->newSibling;
	if (tctx.depth!=0) {
		PBlockP spb(tctx.pb); addNewPage(tctx,*key,newSibling,(tctx.tree->getMode()&TF_SPLITINTX)==0);
		tctx.parent.release(); tctx.parent=tctx.pb; tctx.pb=spb; tctx.stack[++tctx.depth]=spb->getPageID();
	} else if (tctx.tree->addRootPage(*key,newSibling,level)==RC_OK) {
		tctx.parent.release(); tctx.stack[0]=newSibling; tctx.stack[++tctx.depth]=tctx.pb->getPageID();
	}
	tctx.newPage=tps->newSibling; tx.ok(); return RC_OK;
}

RC TreePageMgr::spawn(TreeCtx& tctx,size_t lInsert,ulong idx)
{
	RC rc=RC_OK; tctx.parent.release(); const TreePage *tp=(const TreePage*)tctx.pb->getPageBuf(); 
	ushort lElt=tp->info.calcEltSize(),sht=tp->info.keyLength();
	if (idx==~0u) {
		ulong xLen=0; const byte *p=(const byte*)(tp+1);
		for (ulong i=0; i<tp->info.nSearchKeys; ++i,p+=lElt)
			{const PagePtr *pp=(const PagePtr*)(p+sht); if (!TreePage::isSubTree(*pp) && pp->len>xLen) {xLen=pp->len; idx=i;}}
		if (idx==~0u || xLen<256 || lInsert>ulong(tp->info.freeSpaceLength+tp->info.scatteredFreeSpace) &&
			lInsert-ulong(tp->info.freeSpaceLength+tp->info.scatteredFreeSpace)>xLen-sizeof(SubTreePage)) return RC_TOOBIG;
	}
	ulong ls=sizeof(TreePageSpawn)+tp->getSerKeySize(ushort(idx));
	TreePageSpawn *tpm=(TreePageSpawn*)alloca(ls); if (tpm==NULL) return RC_NORESOURCES;
	IndexFormat ifmt(tp->info.fmt.isPinRef()?KT_REF:KT_BIN,tp->info.fmt.makeSubKey(),0);
	tpm->fmt=ifmt; tp->serializeKey(ushort(idx),tpm+1); 
	MiniTx tx(Session::getSession(),0); PBlock *pbn;
	if ((rc=ctx->fsMgr->allocPages(1,&tpm->root))!=RC_OK) return rc;
	assert(tpm->root!=INVALID_PAGEID);
	if ((pbn=ctx->bufMgr->newPage(tpm->root,this))==NULL) return RC_NORESOURCES;
	if ((rc=ctx->txMgr->update(tctx.pb,this,idx<<TRO_SHIFT|TRO_SPAWN,(byte*)tpm,ls,0,pbn))!=RC_OK)
		pbn->release(PGCTL_DISCARD); else {pbn->release(); tx.ok();}
	return rc;
}

RC TreePageMgr::modSubTree(TreeCtx& tctx,const SearchKey& key,ulong kidx,const PagePtr *vp,const void *newV,ushort lNew,const void *oldV,ushort lOld,bool fMulti)
{
	RC rc=RC_OK; PBlockP spb(tctx.pb); tctx.parent.release(); tctx.parent=tctx.pb; tctx.pb=NULL;
	tctx.mainKey=&key; tctx.index=kidx; memcpy(&tctx.vp,vp,sizeof(PagePtr)); assert(TreePage::isSubTree(tctx.vp));
	tctx.stack[tctx.leaf=tctx.depth++]=spb->getPageID(); 
	const IndexFormat ifmt=((TreePage*)spb->getPageBuf())->info.fmt; const bool fPR=ifmt.isPinRef();
	SubTreePage tpe; memcpy(&tpe,spb->getPageBuf()+tctx.vp.offset,sizeof(SubTreePage)); tctx.lvl2=tpe.level;
// tpe.counter!!!
	if (fMulti) {
		const bool fVM=ifmt.isVarMultiData(),fIns=oldV==NULL; assert(oldV==NULL||newV==NULL);
		const byte *pv=(const byte*)(fIns?newV:oldV); ushort lVal=fIns?lNew:lOld,lElt=fVM?L_SHT:ifmt.dataLength();
		const ulong end=fVM?TreePage::nKeys(pv):lVal/lElt; ulong from=0,to=end; 
		const byte *pk=fVM?TreePage::getK(pv,0):(const byte*)pv;
		ushort lk=fVM?TreePage::lenK(pv,0):lElt; SearchKey mkey(pk,lk,fPR);
		for (rc=tctx.findPageForUpdate(&mkey,fIns); rc==RC_OK; ) {
			const TreePage *tp=(const TreePage*)tctx.pb->getPageBuf(); PageID sib=tp->info.sibling;
			if (sib!=INVALID_PAGEID) {to=from; tp->findSubKey(pv,end,tp->info.nEntries-1,to,fVM); assert(from<to);}
			if (fIns) {
				ulong pageCnt=0; PageID pages[XSUBPAGES]; ushort indcs[XSUBPAGES]; int cmp; bool fFits=false; size_t xSz=calcXSz(pv,from,to,tp);
				while (from<to && tp->info.nSearchKeys!=0) {
					ushort lPref=0,lP; size_t lChunk=TreePage::calcChunkSize(pv,lVal,from,to,ifmt,lPref,tp); fFits=false;
					if (lChunk+tp->info.nEntries*(tp->info.lPrefix-lPref)<ulong(tp->info.freeSpaceLength)) {fFits=true; break;}
					if ((cmp=tp->cmpKeys(pv,from,tp->info.nSearchKeys-1,lElt,fVM))>0) break;
					ulong sidx=0,psidx=0,idx=from,pidx=from; PBlock *right;
					const byte *pf=fVM?TreePage::getK(pv,from):(const byte*)pv+lElt*from; ushort lf=fVM?TreePage::lenK(pv,from):lElt;
					if ((cmp=tp->cmpKeys(pv,from,0,lElt,fVM))<0) {
						tp->findSubKey(pv,to,0,idx,fVM); assert(idx>from && idx<=to);
						size_t sz=TreePage::calcChunkSize(pv,lVal,from,idx,ifmt,lPref); ulong idx2=0;
						// lLast?
						if (sz>xSize) {
							//??? which key insret in split?
							if ((rc=split(tctx,NULL,idx2,0,false,&right))!=RC_OK ||
								(rc=insertSubTree(tctx,pv,lVal,ifmt,from,idx,pageCnt,pages,indcs,xSz,&from))!=RC_OK || from>=to) break;
							tctx.pb.release(); tctx.pb=right; tp=(const TreePage*)right->getPageBuf(); continue;
						}
					} else if (cmp>0) tp->findSubKey(pf,lf,sidx);
					SearchKey first(pf,lf,fPR),*skey=NULL; lPref=tp->calcPrefixSize(first,0,true);
					for (ulong b2=0,e2=~0ul;;) {
						if ((lP=lPref)>0) {
							if (sidx>0 && (lP=tp->calcPrefixSize(first,sidx,true))>lPref) lP=lPref;	//???
							if (idx>from && lP!=0) {
								// idx+1???
								const byte *p2=fVM?TreePage::getK(pv,idx):(const byte*)pv+lElt*idx;
								ushort l2=fVM?TreePage::lenK(pv,idx):lElt; if (l2<lP) lP=l2;
								for (unsigned i=0; i<lP; i++) if (pf[i]!=p2[i]) {lP=i; break;}
							}
						}
						size_t lChunk=(fVM?TreePage::lenKK(pv,from,idx)+TreePage::lenKK((byte*)(tp+1),0,sidx)+sidx*tp->info.lPrefix+lP+L_SHT:0)+(idx-from+sidx)*(lElt-lP),lK=0;
						if (idx<to) lK=(fVM?TreePage::lenK(pv,idx)+L_SHT:lElt)-lP;
						if (sidx<tp->info.nEntries) {size_t lK2=(fVM?tp->lenK(sidx)+tp->info.lPrefix+L_SHT:lElt)-lP; if (lK2>lK) lK=lK2;}
						if ((lChunk+=lK)>=xSz && lChunk<=xSize && double(lChunk-xSz)/xSz<0.05) break;
						if (psidx<sidx) {
							if (e2!=~0ul) {
								if (lChunk>xSz) {if ((e2=sidx)==b2) break;} else if ((b2=sidx)+1>=e2) break;
								if ((sidx=(b2+e2)/2)==psidx) break;
							} else if (lChunk>xSz) {b2=psidx; e2=sidx; if ((sidx=(b2+e2)/2)==psidx) break;}
							else if ((psidx=sidx)>=tp->info.nSearchKeys) {b2=idx; e2=idx=to;}
							else {tp->findSubKey(pv,to,sidx,idx,fVM); assert(idx<=to);}
						} else {
							if (e2!=~0ul) {
								if (lChunk>xSz) {if ((e2=idx)==b2) break;} else if ((b2=idx)+1>=e2) break;
								if ((idx=(b2+e2)/2)==pidx) break; assert(idx<=to);
							} else if (lChunk>xSz) {b2=pidx; e2=idx; if ((idx=(b2+e2)/2)==pidx) break;}
							else if ((pidx=idx)==to) {b2=sidx; e2=sidx=tp->info.nEntries;}
							else {
								const byte *p2=fVM?TreePage::getK(pv,idx):(const byte*)pv+lElt*idx;
								ushort l2=fVM?TreePage::lenK(pv,idx):lElt; tp->findSubKey(p2,l2,sidx);
							}
						}
					}
					assert(idx<=to);
					if (ushort(sidx)>=tp->info.nSearchKeys) break;
					// here: sidx - where to split, idx - how many to insert
					if (idx<to && tp->cmpKeys(pv,idx,sidx,lElt,fVM)<0)
						new(skey=&first) SearchKey(fVM?TreePage::getK(pv,idx):(const byte*)pv+lElt*idx,fVM?TreePage::lenK(pv,idx):lElt,fPR);
					else 
						while (idx>from && tp->cmpKeys(pv,idx-1,sidx,lElt,fVM)>0) idx--;
					if ((rc=split(tctx,skey,sidx,ushort(sidx),skey!=NULL,&right))!=RC_OK) break;
					if (from<idx) {
						if ((rc=insertSubTree(tctx,pv,lVal,ifmt,from,idx,pageCnt,pages,indcs,xSize))!=RC_OK) break;
						from=idx; assert(pageCnt==1);
					}
					tctx.pb.release(); tctx.pb=right; tp=(const TreePage*)right->getPageBuf();
				}
				if (rc!=RC_OK || from<to && (rc=insertSubTree(tctx,pv,lVal,ifmt,from,to,pageCnt,pages,indcs,fFits?xSize:xSz))!=RC_OK) break;
				assert(!fFits || pageCnt==1);
			} else {
				ushort sht,l1;
				if (fVM) {sht=((ushort*)pv)[from]; l1=TreePage::lenK(pv,from);} else sht=ushort((l1=(ushort)lElt)*from);
				ulong lChunk=fVM?(to-from+1)*L_SHT+((ushort*)pv)[to]-sht:(to-from)*lElt;
				if (tp->info.lPrefix!=0) {
					// check prefix, if < tp->info.lPrefix -> RC_NOTFOUND
					lChunk-=(to-from-1)*tp->info.lPrefix;
				}
				TreeFactory *tf=tctx.tree->getFactory(); if (tf==NULL) {rc=RC_INTERNAL; break;}
				ushort lk=mkey.extLength(),lFact=lk+tf->getParamLength()+4; ulong xbuf=lChunk+sizeof(TreePageMulti)+lFact; 
				byte *buf=(byte*)malloc(xbuf,SES_HEAP); if (buf==NULL) {rc=RC_NORESOURCES; break;}
				ushort lData=packMulti(buf,pv,lVal,from,to,tp->info.lPrefix,fVM?~0u:lElt);
				((TreePageMulti*)buf)->fmt=tp->info.fmt; ((TreePageMulti*)buf)->sibling=sib;
				((TreePageMulti*)buf)->lPrefix=tp->info.lPrefix; ((TreePageMulti*)buf)->fLastR=0; ((TreePageMulti*)buf)->lData=lData;
				byte *p=(byte*)buf+sizeof(TreePageMulti)+lData+tp->info.lPrefix+lFact; tf->getParams(p-lFact+lk,*tctx.tree);
				tctx.mainKey->serialize(p-lFact); p[-4]=byte(lFact>>8); p[-3]=byte(lFact); p[-2]=tf->getID(); p[-1]=0xFF;
				rc=ctx->txMgr->update(tctx.pb,this,TRO_MULTIDEL,buf,sizeof(TreePageMulti)+lData+tp->info.lPrefix+lFact,LRC_LUNDO);
				free(buf,SES_HEAP); if (rc!=RC_OK) break;
			}
			if (to>=end) break;
			assert(sib!=INVALID_PAGEID);
			from=to; to=end; ushort lNext=fVM?TreePage::lenK(pv,from):lElt;
			const byte *next=fVM?TreePage::getK(pv,from):(const byte*)pv+lElt*from;
			if (!tctx.parent.isNull() && tctx.parent->getPageID()!=spb->getPageID()) tctx.parent.release();
			if (tctx.parent.isNull()) {tctx.parent=(PBlock*)spb; tctx.parent.set(PGCTL_NOREL);}
			tctx.pb=ctx->bufMgr->getPage(sib,this,PGCTL_COUPLE|PGCTL_ULOCK|(tctx.pb.isSet(QMGR_UFORCE)?QMGR_UFORCE:0),tctx.pb); ++ctx->treeMgr->sibRead;
			if (!tctx.pb.isNull()) {
				tp=(const TreePage*)tctx.pb->getPageBuf(); if (!tp->hasSibling()||tp->info.nEntries==0) continue; int cmp=0;
				if (!tp->info.fmt.isRefKeyOnly()) {
					const byte *pref=tp->info.lPrefix==0?(const byte*)0:fVM?(byte*)tp+((PagePtr*)&tp->info.prefix)->offset:(byte*)&tp->info.prefix;
					if (pref!=NULL && ((cmp=memcmp(next,pref,min(lNext,tp->info.lPrefix)))<0 || cmp==0 && lNext<tp->info.lPrefix)) continue;
					ushort lk0=fVM?L_SHT:lElt-tp->info.lPrefix; const byte *p=(const byte*)(tp+1)+(tp->info.nEntries-1)*lk0;
					if (fVM) {lk0=TreePage::lenK(p,0); p=(*tp)[p];}
					if (cmp==0 && ((cmp=memcmp(next+tp->info.lPrefix,p,min(lk0,ushort(lNext-tp->info.lPrefix))))<0 || cmp==0 && lNext-tp->info.lPrefix<lk0)) continue;
				} else if (PINRef::cmpPIDs(next,lNext,(*tp)[tp->info.nEntries-1],tp->lenK(tp->info.nEntries-1))<0) continue;
			}
			new(&mkey) SearchKey(next,lNext,fPR); tctx.depth=tctx.leaf+1; rc=tctx.findPageForUpdate(&mkey,fIns);
		}
	} else {
		if (oldV!=NULL && lOld!=0) {
			SearchKey okey((const byte*)oldV,lOld,fPR); const TreePage *tp; const PagePtr *pvp;
			if (tctx.findPageForUpdate(&okey,false)!=RC_OK || !(tp=(TreePage*)tctx.pb->getPageBuf())->findKey(okey,tctx.index)) 
				rc=RC_NOTFOUND;
			else {
				tctx.parent.release(); bool fRepl=false; ulong idx2;
				if (newV!=NULL && lNew!=0 && (lNew<=lOld || lNew-lOld<=tp->info.freeSpaceLength)) {
					if (tp->findSubKey((const byte*)newV,lNew,idx2)) {
						if (!ifmt.isPinRef() || PINRef::cmpPIDs((byte*)oldV,lOld,(byte*)newV,lNew)!=0) rc=RC_ALREADYEXISTS; //else fRepl=true;
					} else if (idx2>0 && (!tp->hasSibling() || idx2<tp->info.nEntries)) fRepl=true;
					else if (idx2==0) {if (tpe.anchor==tctx.pb->getPageID()) fRepl=true;}
					if (fRepl && tp->info.lPrefix>0 && (idx2==0 || idx2>=tp->info.nSearchKeys)) {
						// check prefix
					}
				}
				if (rc==RC_OK) {
					TreeFactory *tf=tctx.tree->getFactory(); ushort lk=key.extLength(),lFact=tf!=NULL?tf->getParamLength()+lk+4:0;
					if (fRepl) {
						ushort l=sizeof(uint16_t)+lOld+lNew; byte *buf=(byte*)alloca(l+lFact);
						if (buf==NULL) rc=RC_NORESOURCES;
						else {
							((uint16_t*)buf)[0]=lOld; memcpy(buf+sizeof(uint16_t),oldV,lOld); memcpy(buf+sizeof(uint16_t)+lOld,newV,lNew);
							if (tf!=NULL) {byte *p=buf+l+lFact; tf->getParams(p-lFact+lk,*tctx.tree); key.serialize(p-lFact); p[-4]=byte(lFact>>8); p[-3]=byte(lFact); p[-2]=tf->getID(); p[-1]=0xFF;}
							rc=ctx->txMgr->update(tctx.pb,this,tctx.index<<TRO_SHIFT|TRO_REPLACE,buf,l+lFact,tf!=NULL?LRC_LUNDO:0);
						}
						newV=NULL; lNew=0;
					} else {
						ulong lrec=sizeof(TreePageModify)+SearchKey::extLength(lOld); TreePageModify *tpm=(TreePageModify*)alloca(lrec+lFact);
						if (tpm==NULL) rc=RC_NORESOURCES;
						else {
							memset(tpm,0,sizeof(TreePageModify)); tpm->newPrefSize=tp->info.lPrefix; SearchKey::serialize(tpm+1,fPR?KT_REF:KT_BIN,oldV,lOld);
							if (tf!=NULL) {byte *p=(byte*)tpm+lrec+lFact; tf->getParams(p-lFact+lk,*tctx.tree); key.serialize(p-lFact); p[-4]=byte(lFact>>8); p[-3]=byte(lFact); p[-2]=tf->getID(); p[-1]=0xFF;}
							rc=ctx->txMgr->update(tctx.pb,this,tctx.index<<TRO_SHIFT|TRO_DELETE,(byte*)tpm,lrec+lFact,tf!=NULL?LRC_LUNDO:0);
							if (rc==RC_OK && newV!=NULL && lNew!=0) {
								tctx.parent.release(); tctx.parent=(PBlock*)spb; tctx.parent.set(PGCTL_NOREL|spb.uforce()); tctx.depth=tctx.leaf+1;
								if (!(tp=(TreePage*)spb->getPageBuf())->findKey(key,tctx.index) || tp->findData(tctx.index,lOld,&pvp)==NULL)
									rc=RC_CORRUPTED; else memcpy(&tctx.vp,pvp,sizeof(PagePtr));
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
	tctx.pb.release(); tctx.pb=spb; tctx.depth=tctx.leaf; tctx.leaf=~0u; tctx.mainKey=NULL; return rc;
}

RC TreePageMgr::insertSubTree(TreeCtx &tctx,const void *value,ushort lValue,IndexFormat ifmt,ulong start,ulong end,ulong& pageCnt,PageID *pages,ushort *indcs,size_t xSz,ulong *pRes)
{
	bool fVarSub=ifmt.isVarMultiData(); ulong lElt=fVarSub?L_SHT:ifmt.dataLength(); ushort lprefs[XSUBPAGES];
	RC rc=RC_OK; PBlockP pb; const TreePage *tp=NULL; PageID sibling=INVALID_PAGEID; pageCnt=0;
	size_t xbuf=0,lLast=0; ulong nOldPages=0; ushort lPref=0;
	if (!tctx.pb.isNull()) {
		tp=(const TreePage*)tctx.pb->getPageBuf(); pages[0]=tctx.pb->getPageID(); nOldPages=1;
		if ((sibling=tp->info.sibling)!=INVALID_PAGEID) lLast=tp->getKeyExtra(ushort(~0u))+L_SHT;
	}
	for (ulong idx=start,i2; idx<end; idx=i2) {
		if (pageCnt>=XSUBPAGES) return RC_NORESOURCES;
		for (ulong b2=idx+(pageCnt>=nOldPages?1:0),e2=i2=end; ;) {
			size_t dl=TreePage::calcChunkSize((const byte*)value,lValue,idx,i2==end?i2:i2+1,ifmt,lPref,
													pageCnt==0||i2==end&&lLast!=0?tp:0,lLast!=0&&pageCnt>=nOldPages),lb=dl,xS=xSz;
			if (pageCnt!=0) {if (i2==end) lb=dl+=lLast;}
			else if (tp!=NULL) {
				dl+=tp->info.nEntries*(tp->info.lPrefix-lPref); 
				xS=tp->info.freeSpaceLength; if (i2<end) xS+=lLast; if (xS>xSz) xS=xSz;
			}
			if (dl==xS || dl<xS && (b2=i2)+1>=e2) {
				if (lb+sizeof(TreePageMulti)+lPref+L_SHT>xbuf) xbuf=lb+sizeof(TreePageMulti)+lPref+L_SHT;
				if (i2==end && dl<xSz && pRes!=NULL) {*pRes=idx; assert(pageCnt!=0);}
				else {
					if (i2<end && lLast!=0 && pageCnt==0 && tp->info.nEntries>1) {
						SearchKey key(fVarSub?TreePage::getK(value,i2):(const byte*)value+lElt*i2,ushort(fVarSub?TreePage::lenK(value,i2):lElt));
						lPref=tp->calcPrefixSize(key,0);
					}
					indcs[pageCnt]=(ushort)i2; lprefs[pageCnt]=lPref; pageCnt++;
				}
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
	if (pageCnt<=nOldPages) {assert(lLast==0 || tp->cmpKeys((byte*)value,end-1,tp->info.nEntries-1,ushort(lElt),fVarSub)<=0); lLast=0;}
	TreeFactory *tf=tctx.tree->getFactory(); ushort lFact=tf!=NULL?tf->getParamLength()+2:0,lk=0; assert(tctx.mainKey!=NULL);
	if (tf!=NULL) lFact+=(lk=tctx.mainKey->extLength())+2; xbuf+=lFact;
	byte *buf=(byte*)malloc(xbuf,SES_HEAP); if (buf==NULL) return RC_NORESOURCES;
	TreePageMulti *tpm=(TreePageMulti*)buf; IndexFormat subfmt(ifmt.isPinRef()?KT_REF:KT_BIN,ifmt.makeSubKey(),0);
	if (pageCnt==nOldPages || (rc=ctx->fsMgr->allocPages(pageCnt-nOldPages,pages+nOldPages))==RC_OK) for (ulong i=pageCnt; i--!=0; sibling=pages[i],lLast=0) {
		ushort lData=packMulti(buf+sizeof(TreePageMulti),value,lValue,i==0?(ushort)start:indcs[i-1],indcs[i]+(i+1==pageCnt?0:1),lprefs[i],fVarSub?~0u:lElt,
																									lLast!=0?(TreePage*)tctx.pb->getPageBuf():(TreePage*)0);
		ulong op=TRO_MULTIINS; assert(sizeof(TreePageMulti)+lData+lprefs[i]+lFact<=xbuf); double prev=0.;
		tpm->fmt=subfmt; tpm->sibling=sibling; tpm->lPrefix=lprefs[i]; tpm->fLastR=0; tpm->lData=lData;
		if (i>=nOldPages) {op=TRO_MULTINIT; pb=ctx->bufMgr->newPage(pages[i],this,pb); if (pb.isNull()) {rc=RC_NORESOURCES; break;}}
		else {pb.release(); pb=(PBlock*)tctx.pb; pb.set(PGCTL_NOREL); tpm->fLastR=tp->hasSibling()&&pageCnt>nOldPages; prev=double(xSize-tp->info.freeSpaceLength)*100./xSize;}
		if (tf!=NULL) {
			byte *p=(byte*)buf+sizeof(TreePageMulti)+lData+lprefs[i]+lFact; tf->getParams(p-lFact+lk,*tctx.tree);
			tctx.mainKey->serialize(p-lFact); p[-4]=byte(lFact>>8); p[-3]=byte(lFact); p[-2]=tf->getID(); p[-1]=0xFF;
		}
		if ((rc=ctx->txMgr->update(pb,this,op,buf,sizeof(TreePageMulti)+lData+lprefs[i]+lFact,tf!=NULL?LRC_LUNDO:0))!=RC_OK) break;
#ifdef TRACE_EMPTY_PAGES
		report(MSG_DEBUG,"Inserted %d bytes into page %X(%f -> %f)\n",lData,pb->getPageID(),prev,double(xSize-((TreePage*)pb->getPageBuf())->info.freeSpaceLength)*100./xSize);
#endif
	}
	if (rc==RC_OK && pageCnt>nOldPages) {
#if 0
			PBlockP spb(tctx.pb); SearchKey key;
#ifdef _DEBUG
			ulong depth=tctx.depth;
#endif
			for (ulong i=0; i<pageCnt; i++) {
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

int TreePageMgr::TreePage::cmpKeys(const byte *pv,ulong idx,ulong sidx,ushort lElt,bool fVM) const
{
	const byte *p1=fVM?TreePage::getK(pv,idx):(const byte*)pv+lElt*idx; ushort l1=fVM?TreePage::lenK(pv,idx):lElt; int cmp;
	if (info.fmt.isRefKeyOnly()) return PINRef::cmpPIDs(p1,l1,(*this)[sidx],lenK(sidx));
	if (info.lPrefix!=0) {
		if ((cmp=memcmp(p1,fVM?(byte*)this+((PagePtr*)&info.prefix)->offset:(byte*)&info.prefix,min(l1,info.lPrefix)))!=0) return cmp;
		if (l1<=info.lPrefix) return -1; p1+=info.lPrefix; l1-=info.lPrefix;
	}
	ushort lk0=fVM?L_SHT:lElt-info.lPrefix; const byte *p=(const byte*)(this+1)+sidx*lk0; if (fVM) {lk0=TreePage::lenK(p,0); p=(*this)[p];}
	return (cmp=memcmp(p1,p,min(lk0,l1)))==0?cmp3(l1,lk0):cmp;
}

size_t TreePageMgr::TreePage::calcChunkSize(const byte *vals,ushort lVals,ulong start,ulong end,IndexFormat fmt,ushort& lpref,const TreePage *tp,bool fLast)
{
	lpref=0; const bool fVM=fmt.isVarMultiData(); const ushort lE=fmt.dataLength();
	assert(start<end && end<=ulong(fVM?nKeys(vals):lVals/lE));
	if (!fmt.isPinRef() && (tp==NULL || fLast || tp->info.lPrefix!=0)) {
		const byte *p1,*p2; ushort l1,l2,i,ll;
		if (!fVM) p1=vals+start*(l1=lE); else {p1=getK(vals,start); l1=lenK(vals,start);}
		if (start+1==end) lpref=l1;
		else {
			if (!fVM) p2=vals+(end-1)*(l2=lE); else {p2=getK(vals,end-1); l2=lenK(vals,end-1);}
			for (i=0,ll=min(l1,l2); i<ll; i++) if (p1[i]!=p2[i]) break;
			lpref=i;
		}
		if (tp!=NULL && lpref!=0) {
			ushort lmax=lpref; lpref=0;
			if ((l2=tp->info.lPrefix)!=0) {
				p2=fVM?(byte*)tp+((PagePtr*)&tp->info.prefix)->offset:(byte*)&tp->info.prefix;
				for (i=0,ll=min(lmax,l2); i<ll; i++) if (p1[i]!=p2[i]) break;
				lpref=i; p1+=lpref; l1-=lpref;
			}
			if (fLast && lpref==l2 && l1!=0) {
				p2=(const byte*)(tp+1)+(!fVM?lE-l2:L_SHT)*(tp->info.nEntries-1); 
				if (!fVM) l2=lE-l2; else {l2=lenK(p2,0); p2=(*tp)[p2];}
				for (i=0,ll=min(l1,l2); i<ll; i++) if (p1[i]!=p2[i]) break;
				if ((lpref+=i)>lmax) lpref=lmax;
			}
		}
	}
	return (fVM?(end-start+(tp==NULL||tp->info.nEntries==0?1:0))*L_SHT+lenKK(vals,start,end)+(tp==NULL?lpref:0):(end-start)*lE)-lpref*(end-start);
}

ushort TreePageMgr::packMulti(byte *buf,const void *value,ushort lVal,ulong start,ulong end,ushort lpref,ulong lElt,TreePage *tp)
{
	assert(start<end && (tp==NULL||tp->hasSibling()));
	if (tp==NULL && lpref==0 && start==0 && end==TreePage::nKeys(value)) {memcpy(buf,value,lVal); return lVal;}
	const byte *pref=NULL,*pp; ushort ll=0,l=0,d;
	if (lElt==~0u) {
		ll=ushort(end-start+1+(tp!=NULL?1:0))*L_SHT;
		if (lpref!=0) pref=TreePage::getK(value,start);
		for (ulong j=0;;ll+=l,j++) {
			((ushort*)(buf))[j]=ll; if (start+j>=end) break;
			memcpy(buf+ll,TreePage::getK(value,start+j)+lpref,l=TreePage::lenK(value,start+j)-lpref);
		}
	} else {
		l=ushort(lElt)-lpref;
		if (lpref!=0) {
			pref=(byte*)value+start*lElt; assert(lpref<=lElt && (tp==NULL||lpref==tp->info.lPrefix));
			for (; start<end; ll+=(ushort)l,start++) memcpy(buf+ll,(byte*)value+start*lElt+lpref,l);
		} else memcpy(buf,(byte*)value+lElt*start,ll=ushort(lElt*(end-start)));
	}
	if (tp!=NULL) {
		if (lElt==~0u) {assert(tp->info.nEntries!=0); l=tp->lenK(tp->info.nEntries-1); pp=(*tp)[tp->info.nEntries-1];} else pp=(byte*)(tp+1)+l*(tp->info.nEntries-1);
		if (lpref>=tp->info.lPrefix) memcpy(buf+ll,pp+(lpref-tp->info.lPrefix),l-=lpref-tp->info.lPrefix);
		else {d=tp->info.lPrefix-lpref; memcpy(buf+ll,(lElt==~0u?(byte*)tp+((PagePtr*)&tp->info.prefix)->offset:(byte*)&tp->info.prefix)+lpref,d); memcpy(buf+(ll+=d),pp,l);}
		ll+=l; if (lElt==~0u) ((ushort*)buf)[end-start+1]=ll;
	}
	if (lpref!=0) memcpy(buf+ll,pref,lpref);
	return ll;
}

ushort TreePageMgr::calcXSz(const byte *pv,ulong from,ulong to,const TreePage *tp) 
{
	size_t l=TreePage::lenKK(pv,from,to)+(to-from+1)*L_SHT;
	if (tp!=NULL) l+=xSize-tp->info.freeSpaceLength+tp->info.nEntries*tp->info.lPrefix;
	ulong nPages=ulong((l+xSize-1)/xSize); return ushort((l/=nPages)>xSize?xSize:l);
}

PageID TreePageMgr::startSubPage(const TreePage *tp,const PagePtr& vp,const SearchKey *key,int& level,bool fRead,bool fBefore)
{
	ushort len=vp.len&~TRO_EXT_DATA;
	assert(TreePage::isSubTree(vp) && size_t(len)>=sizeof(SubTreePage) && (len-sizeof(SubTreePage))%sizeof(SubTreePageKey)==0);
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
	ushort len=vp.len&~TRO_EXT_DATA; if (len<=sizeof(SubTreePage)) return INVALID_PAGEID;
	SubTreePage tpe; memcpy(&tpe,(byte*)tp+vp.offset,sizeof(SubTreePage)); const byte *p=(byte*)tp+vp.offset+len;
	for (long i=(len-sizeof(SubTreePage))/sizeof(SubTreePageKey); --i>=0; p-=sizeof(SubTreePageKey)) {
		PageID pp; memcpy(&pp,&((SubTreePageKey*)p)->pageID,sizeof(PageID));
		if (pp==pid) return i==0?tpe.leftMost:(memcpy(&pp,&((SubTreePageKey*)p)[-1].pageID,sizeof(PageID)),pp);
	}
	return INVALID_PAGEID;
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
			ulong lk=key.v.ptr.l; ushort off; const PagePtr *pvp;
			if (tctx.lvl2!=tpe.level) {
				// re-traverse from root etc.
			} else if (!fTry && tp->findExtKey(key.getPtr2(),lk,(SubTreePageKey*)((byte*)tp+tctx.vp.offset+sizeof(SubTreePage)),
									((tctx.vp.len&~TRO_EXT_DATA)-sizeof(SubTreePage))/sizeof(SubTreePageKey),&off)!=NULL) return;
			else if (ulong((tctx.vp.len&~TRO_EXT_DATA)+sizeof(SubTreePageKey)+lk)<=tp->hdr.length()*SPAWN_THR
							&&	sizeof(SubTreePageKey)+lk<=ulong(tp->info.freeSpaceLength+tp->info.scatteredFreeSpace)) {
				ulong lrec=sizeof(TreePageRoot)+tctx.mainKey->extLength()+lk; TreePageRoot *tpr=(TreePageRoot*)alloca(lrec); 
				if (tpr!=NULL) {
					tpr->pageID=pid; tpr->pkey.len=ushort(lk); tpr->pkey.offset=ushort(lrec-lk);
					tctx.mainKey->serialize(tpr+1); memcpy((byte*)tpr+tpr->pkey.offset,key.getPtr2(),lk);
					RC rc=ctx->txMgr->update(tctx.pb,this,tctx.index<<TRO_SHIFT|TRO_ADDROOT,(byte*)tpr,lrec);
					if (fTry && tp->findData(tctx.index,off,&pvp)!=NULL && pvp!=NULL) memcpy(&tctx.vp,pvp,sizeof(PagePtr));
					if (rc==RC_OK) return;
				}
			} else {
				ulong ls=sizeof(TreePageSpawn)+tctx.mainKey->extLength(); TreePageSpawn *tpm=(TreePageSpawn*)alloca(ls);
				if (tpm!=NULL) {
					IndexFormat ifmt(tp->info.fmt.isPinRef()?KT_REF:KT_BIN,tp->info.fmt.makeSubKey(),sizeof(PageID));
					tpm->fmt=ifmt; tctx.mainKey->serialize(tpm+1); PBlock *pbn; TxSP tx(Session::getSession());
					if (tx.start()==RC_OK && ctx->fsMgr->allocPages(1,&tpm->root)==RC_OK && (pbn=ctx->bufMgr->newPage(tpm->root,ctx->trpgMgr))!=NULL) {
						if (ctx->txMgr->update(tctx.pb,this,tctx.index<<TRO_SHIFT|TRO_SPAWN,(byte*)tpm,ls,0,pbn)!=RC_OK)
							pbn->release(PGCTL_DISCARD);
						else {
							if (fTry && tp->findData(tctx.index,off,&pvp)!=NULL && pvp!=NULL) memcpy(&tctx.vp,pvp,sizeof(PagePtr));
							assert(tctx.pb->isDependent()); tctx.parent=tctx.pb; tctx.pb=pbn;
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

RC TreePageMgr::count(TreeCtx& tctx,const SearchKey& key,uint64_t& nValues)
{
	nValues=0; ulong idx; ushort lData; const PagePtr *vp; assert(!tctx.pb.isNull());
	const TreePage *tp=(const TreePage*)tctx.pb->getPageBuf();
	if (tp->info.fmt.keyType()!=key.type||!tp->isLeaf()) return RC_INVPARAM;
	if (tp->info.fmt.isSeq()) {nValues=key.v.u<tp->info.prefix+tp->info.nEntries?1:0; return RC_OK;}
	if (!tp->findKey(key,idx)) return RC_NOTFOUND;
	if (tp->info.fmt.isKeyOnly()) return RC_OK;
	if (tp->info.fmt.isUnique()) {nValues=1; return RC_OK;}
	if (tp->findData(idx,lData,&vp)==NULL||vp==NULL) return RC_CORRUPTED;
	if (TreePage::isSubTree(*vp)) {
		if ((vp->len&~TRO_EXT_DATA)<sizeof(SubTreePage)) return RC_CORRUPTED;
		SubTreePage stp; memcpy(&stp,(byte*)tp+vp->offset,sizeof(SubTreePage));
		nValues=stp.counter;
	} else if (tp->info.fmt.isFixedLenData()) nValues=vp->len/tp->info.fmt.dataLength();
	else if (vp->len<256) nValues=*((byte*)tp+vp->offset)-1;
	else nValues=_u16((byte*)tp+vp->offset)/L_SHT-1;
	return RC_OK;
}

RC TreePageMgr::insert(TreeCtx& tctx,const SearchKey& key,const void *value,ushort lValue,bool fMulti)
{
	RC rc=RC_OK; assert(!tctx.pb.isNull());
	const TreePage *tp=(const TreePage*)tctx.pb->getPageBuf();
	if (tp->info.fmt.keyType()!=key.type||fMulti&&tp->info.fmt.isUnique()) return RC_INVPARAM;
	size_t lrec=sizeof(TreePageModify)+key.extLength(),lInsert=0,lExtra=0,lKey=tp->info.calcEltSize(); uint32_t nVals=1;
	const void *pOld=NULL; const PagePtr *vp=NULL; TREE_OP op=TRO_INSERT; bool fVM=tp->info.fmt.isVarMultiData();
	
	if (fMulti && (nVals=fVM?TreePage::nKeys(value):lValue/tp->info.fmt.dataLength())==1)
		{fMulti=false; if (fVM) {value=(byte*)value+L_SHT*2; lValue-=L_SHT*2;}}

	ushort lOld,prefixSize=tp->info.lPrefix,n=fMulti?ushort(~0u):0; ulong idx=0;

	assert(!tp->info.fmt.isFixedLenData() || fMulti || lValue==tp->info.fmt.dataLength());
	assert(!fMulti || !fVM || lValue==((ushort*)value)[nVals]);

	if (tp->info.fmt.isSeq()) {
		if (!tp->isLeaf() || tp->hasSibling() || key.v.u!=tp->info.prefix+tp->info.nEntries) {
			// report(...
			return RC_CORRUPTED;
		}
		idx=tp->info.nEntries; lInsert=lValue; if (!tp->info.fmt.isFixedLenData()) lInsert+=sizeof(PagePtr);
	} else if (!tp->findKey(key,idx)) {
		assert((idx>0||tp->info.level==0||tp->info.leftMost!=INVALID_PAGEID) && idx<=tp->info.nSearchKeys);
		if (prefixSize!=0 && tp->info.nEntries!=0 && (idx==0 || idx==tp->info.nEntries) && (prefixSize=tp->calcPrefixSize(key,idx))<tp->info.lPrefix)
			lExtra=tp->info.extraEltSize(prefixSize)*tp->info.nEntries+1;
		lInsert=lKey; if (!tp->info.fmt.isFixedLenKey()) lKey+=key.extra()-prefixSize;
		if (!fMulti) {
			lInsert+=tp->info.calcVarSize(key,lValue,prefixSize)+(!fVM?0:lValue<254?2:L_SHT*2);
			if (lInsert>xSize/3) return RC_TOOBIG;
		} else {
			if (fVM && lValue-(nVals+1)<256) lValue-=ushort(nVals+1);
			lInsert+=tp->info.calcVarSize(key,lValue,prefixSize);
			if (lValue>=0x8000 || lInsert+lExtra>ulong(tp->info.freeSpaceLength+tp->info.scatteredFreeSpace) && lValue>256) {
				ulong pageCnt=0,lkeys=0; PageID pages[XSUBPAGES]; ushort indcs[XSUBPAGES]; tctx.mainKey=&key;
				PBlockP spb(tctx.pb); tctx.parent.release(); tctx.parent=tctx.pb; 
				tctx.pb=NULL; tctx.stack[tctx.leaf=tctx.depth++]=spb->getPageID();
				rc=insertSubTree(tctx,value,lValue,tp->info.fmt,0,nVals,pageCnt,pages,indcs,calcXSz((byte*)value,0,nVals));
				tctx.pb.release(); tctx.pb=spb; tctx.parent.release();
				tctx.depth=tctx.leaf; tctx.leaf=~0u; tctx.mainKey=NULL;
				if (rc!=RC_OK) return rc; assert(pageCnt>0);
				if (pageCnt>1) {
					if (!fVM) lkeys=(pageCnt-1)*tp->info.fmt.dataLength();
					else for (ulong i=1; i<pageCnt; i++) lkeys+=TreePage::lenK(value,indcs[i-1]);
				}
				ulong lstp=sizeof(SubTreePage)+int(pageCnt-1)*sizeof(SubTreePageKey)+lkeys;
				SubTreePage *stp=(SubTreePage*)alloca(lstp); if (stp==NULL) return RC_NORESOURCES;
				stp->level=0; stp->counter=nVals; stp->leftMost=stp->anchor=pages[0]; ulong shtkey=lkeys;
				for (ulong i=pageCnt; --i>0;) {
					SubTreePageKey &spk=((SubTreePageKey*)(stp+1))[i-1]; spk.pageID=pages[i]; const byte *key; ulong lk;
					if (!fVM) key=(byte*)value+indcs[i-1]*(lk=tp->info.fmt.dataLength());
					else {key=TreePage::getK(value,indcs[i-1]); lk=TreePage::lenK(value,indcs[i-1]);}
					assert(shtkey>=lk); memcpy((byte*)(stp+1)+int(pageCnt-1)*sizeof(SubTreePageKey)+(shtkey-=lk),key,lk);
					spk.key.offset=ushort(sizeof(SubTreePage)+int(pageCnt-1)*sizeof(SubTreePageKey)+shtkey); spk.key.len=ushort(lk);
				}
				value=stp; lValue=ushort(lstp); n=ushort(~1u); fMulti=false; assert(shtkey==0);
				lInsert=tp->info.calcEltSize()+tp->info.calcVarSize(key,lValue,prefixSize);
			}
		}
		if (!tp->isLeaf()) lrec=ceil(lrec,sizeof(PageID));
	} else if (!tp->isLeaf()) {assert(lValue==sizeof(PageID)); return tp->getPageID(idx)!=*(PageID*)value?RC_CORRUPTED:RC_FALSE;}
	else if (tp->info.fmt.isKeyOnly()) {
		if (!tp->info.fmt.isRefKeyOnly()) return RC_FALSE;
		op=TRO_UPDATE; lInsert=lKey; lKey+=key.extra()-prefixSize;
	} else if ((pOld=tp->findData(idx,lOld,&vp))==NULL) return RC_CORRUPTED;
	else if (tp->info.fmt.isUnique()) return lOld==lValue && !memcmp(pOld,value,lOld)?RC_FALSE:RC_ALREADYEXISTS;
	else if (TreePage::isSubTree(*vp)) return modSubTree(tctx,key,idx,vp,value,lValue,NULL,0,fMulti);
	else {
		op=TRO_UPDATE; lInsert=lValue; if (!tp->info.fmt.isFixedLenKey()) lKey+=key.extra()-prefixSize;
		if (!fMulti) {
			if (!tp->info.fmt.isPinRef() && tp->findValue(value,lValue,*vp)!=NULL) return RC_FALSE;
			if (fVM) lInsert+=vp->len>=256?L_SHT:vp->len+1+lValue<256?1:((byte*)tp+vp->offset)[0]+L_SHT;
		} else if (fVM) {
			if (vp->len<256) if (vp->len+lValue-nVals>=256) lInsert+=((byte*)tp+vp->offset)[0]; else lInsert-=nVals;
		}
		if (vp->len+lInsert>=0x8000 || tp->info.nSearchKeys<SPAWN_N_THR && lInsert+tp->info.calcEltSize()>ulong(tp->info.freeSpaceLength+tp->info.scatteredFreeSpace)) {
			RC rc2=spawn(tctx,lInsert,vp->len+lInsert>=0x8000?idx:~0u);
			if (rc2!=RC_OK) {if (rc2!=RC_TOOBIG || vp->len+lInsert>=0x8000) return rc2;}
			else if ((pOld=tp->findData(idx,lOld,&vp))==NULL||vp==NULL) return RC_INTERNAL;
			else if (TreePage::isSubTree(*vp)) return modSubTree(tctx,key,idx,vp,value,lValue,NULL,0,fMulti);
		}
	}

	ulong spaceLeft=tp->info.freeSpaceLength+tp->info.scatteredFreeSpace;
	if (lInsert+lExtra>spaceLeft || idx==tp->info.nEntries && lInsert+lExtra+lKey>=spaceLeft) {
		bool fSplit=true;
		if (tp->info.nSearchKeys==1 && !tp->info.fmt.isUnique()) {
			const PagePtr *pp=(const PagePtr*)((byte*)(tp+1)+tp->info.keyLength());
			if (!TreePage::isSubTree(*pp) && pp->len>=256)
				{RC rc2=spawn(tctx,lInsert,0); if (rc2==RC_OK) fSplit=false; else if (rc2!=RC_TOOBIG) return rc2;}
		}
		if (fSplit) {
			bool fInsR; ushort splitIdx=tp->calcSplitIdx(fInsR,key,idx,lInsert,prefixSize,op==TRO_INSERT);
			if ((rc=split(tctx,&key,idx,splitIdx,fInsR))!=RC_OK) return rc;		// idx -> tctx.index ??
		}
		tp=(const TreePage*)tctx.pb->getPageBuf(); prefixSize=tp->info.lPrefix;
		if (op==TRO_INSERT && prefixSize!=0 && (idx==0 || idx==tp->info.nEntries)) prefixSize=tp->calcPrefixSize(key,idx);
	}
	tctx.parent.release();

	TreeFactory *tf=tctx.tree->getFactory(); ushort lFact=tf!=NULL?tf->getParamLength()+2:0,lk=0;
	if (tctx.mainKey!=NULL && tf!=NULL) lFact+=(lk=tctx.mainKey->extLength())+2;
	TreePageModify *tpm=(TreePageModify*)alloca(lrec+lValue+lFact); if (tpm==NULL) return RC_NORESOURCES;
	key.serialize(tpm+1); tpm->newPrefSize=prefixSize<tp->info.lPrefix?prefixSize:tp->info.lPrefix;
	tpm->newData.offset=ushort(lrec); tpm->newData.len=lValue; tpm->oldData.len=0; tpm->oldData.offset=n;
	byte *ptr=(byte*)tpm+tpm->newData.offset; assert(lrec+lValue<0x10000);
	if (!fMulti || lValue>=256 || op==TRO_UPDATE) memcpy(ptr,value,lValue);
	else {ulong j=nVals+1; for (ulong i=0; i<j; i++) *ptr++=byte(((ushort*)value)[i]-j); memcpy(ptr,(ushort*)value+j,lValue-j);}
	if (tf!=NULL) {
		byte *p=(byte*)tpm+lrec+lValue+lFact; tf->getParams(p-lFact+lk,*tctx.tree);
		if (tctx.mainKey==NULL) {p[-2]=byte(lFact); p[-1]=tf->getID();}
		else {tctx.mainKey->serialize(p-lFact); p[-4]=byte(lFact>>8); p[-3]=byte(lFact); p[-2]=tf->getID(); p[-1]=0xFF;}
	}
	return ctx->txMgr->update(tctx.pb,this,idx<<TRO_SHIFT|op,(byte*)tpm,lrec+lValue+lFact,tf!=NULL?LRC_LUNDO:0);
}

RC TreePageMgr::update(TreeCtx& tctx,const SearchKey& key,const void *oldValue,ushort lOldValue,
											const void *newValue,ushort lNewValue)
{
	const TreePage *tp=(const TreePage*)tctx.pb->getPageBuf();
	if (tp->info.fmt.keyType()!=key.type||newValue==NULL||lNewValue==0||tp->info.fmt.isKeyOnly()) return RC_INVPARAM;

	ushort lOld,lExtra=0; const void *pOld; ulong idx=0; const PagePtr *vp; RC rc=RC_OK; bool fExt=false;
	if (tp->info.nSearchKeys==0 || !tp->findKey(key,idx) || (pOld=tp->findData(idx,lOld,&vp))==NULL) return RC_NOTFOUND;
	if (!tp->info.fmt.isUnique()) {
		if (oldValue==NULL||lOldValue==0) return RC_INVPARAM;
		if (TreePage::isSubTree(*vp)) fExt=true;
		else {
			if (tp->findValue(oldValue,lOldValue,*vp)==NULL) return RC_NOTFOUND;
			if (tp->info.fmt.isVarMultiData() && vp->len<256 && lNewValue>lOldValue && vp->len+(lNewValue-lOldValue)>=256)
				lExtra=((byte*)tp+vp->offset)[0];
			pOld=oldValue; lOld=lOldValue;
		}
	}
	if (!fExt && lNewValue>lOld && lNewValue-lOld+lExtra>tp->info.freeSpaceLength+tp->info.scatteredFreeSpace) {
		if (tp->info.nSearchKeys>=2) {
			bool fInsR; ushort splitIdx=tp->calcSplitIdx(fInsR,key,idx,lNewValue-lOld,tp->info.lPrefix);
			if ((rc=split(tctx,&key,idx,splitIdx,fInsR))!=RC_OK) return rc;
			tp=(const TreePage*)tctx.pb->getPageBuf();
		} else if (tp->info.fmt.isUnique()) return RC_TOOBIG;
		else if ((rc=spawn(tctx,lNewValue-lOld+lExtra))!=RC_OK) return rc;
		else if ((pOld=tp->findData(idx,lOld,&vp))==NULL||vp==NULL) return RC_CORRUPTED;
		else fExt=true;
	}
	if (fExt) return modSubTree(tctx,key,idx,vp,newValue,lNewValue,oldValue,lOldValue);
	ulong lrec=sizeof(TreePageModify)+key.extLength(); tctx.parent.release(); assert(tctx.mainKey==NULL);
	TreeFactory *tf=tctx.tree->getFactory(); ushort lFact=tf!=NULL?tf->getParamLength()+2:0;
	TreePageModify *tpm=(TreePageModify*)alloca(lrec+lOld+lNewValue+lFact); if (tpm==NULL) return RC_NORESOURCES;
	key.serialize(tpm+1); tpm->newPrefSize=tp->info.lPrefix;
	tpm->oldData.len=lOld; tpm->oldData.offset=PageOff(lrec); memcpy((byte*)tpm+lrec,pOld,lOld);
	tpm->newData.len=lNewValue; tpm->newData.offset=PageOff(lrec+lOld); memcpy((byte*)tpm+lrec+lOld,newValue,lNewValue);
	if (tf!=NULL) {byte *p=(byte*)tpm+lrec+lOld+lNewValue+lFact; tf->getParams(p-lFact,*tctx.tree); p[-2]=byte(lFact); p[-1]=tf->getID();}
	return ctx->txMgr->update(tctx.pb,this,idx<<TRO_SHIFT|TRO_UPDATE,(byte*)tpm,lrec+lOld+lNewValue+lFact,tf!=NULL?LRC_LUNDO:0);
}

RC TreePageMgr::edit(TreeCtx& tctx,const SearchKey& key,const void *newValue,ushort lNewPart,ushort lOldPart,ushort sht)
{
	const TreePage *tp=(const TreePage*)tctx.pb->getPageBuf();
	if (tp->info.fmt.keyType()!=key.type||!tp->info.fmt.isUnique()||tp->info.fmt.isKeyOnly()) return RC_INVPARAM;

	ulong idx; ushort lOld; const void *pOld; RC rc=RC_OK;
	if (tp->info.nSearchKeys==0 || !tp->findKey(key,idx) || (pOld=tp->findData(idx,lOld))==NULL) return RC_NOTFOUND;
	if (sht+lOldPart>lOld||tp->info.fmt.isFixedLenData()&&lNewPart!=lOldPart) return RC_INVPARAM;
	if (lNewPart>lOldPart && lNewPart-lOldPart>tp->info.freeSpaceLength+tp->info.scatteredFreeSpace) {
		bool fInsR; ushort splitIdx=tp->calcSplitIdx(fInsR,key,idx,lNewPart-lOldPart,tp->info.lPrefix);
		if ((rc=split(tctx,&key,idx,splitIdx,fInsR))!=RC_OK) return rc;
		tp=(const TreePage*)tctx.pb->getPageBuf();
	}
	ulong lrec=(ulong)ceil(sizeof(TreePageEdit)+key.extLength(),sizeof(ushort)); tctx.parent.release();
	TreeFactory *tf=tctx.tree->getFactory(); ushort lFact=tf!=NULL?tf->getParamLength()+2:0;
	TreePageEdit *tpe=(TreePageEdit*)alloca(lrec+lOldPart+lNewPart+lFact); if (tpe==NULL) return RC_NORESOURCES;
	key.serialize(tpe+1); tpe->shift=sht; assert(tctx.mainKey==NULL);
	tpe->oldData.len=lOldPart; tpe->oldData.offset=PageOff(lrec); memcpy((byte*)tpe+lrec,(const byte*)pOld+sht,lOldPart);
	tpe->newData.len=lNewPart; tpe->newData.offset=PageOff(lrec+lOldPart); 
	if (newValue!=NULL) memcpy((byte*)tpe+lrec+lOldPart,newValue,lNewPart);
	if (tf!=NULL) {byte *p=(byte*)tpe+lrec+lOldPart+lNewPart+lFact; tf->getParams(p-lFact,*tctx.tree); p[-2]=byte(lFact); p[-1]=tf->getID();}
	return ctx->txMgr->update(tctx.pb,this,idx<<TRO_SHIFT|TRO_EDIT,(byte*)tpe,lrec+lOldPart+lNewPart+lFact,tf!=NULL?LRC_LUNDO:0);
}

RC TreePageMgr::remove(TreeCtx& tctx,const SearchKey& key,const void *value,ushort lValue,bool fMulti)
{
	const TreePage *tp=(const TreePage*)tctx.pb->getPageBuf();
	if (tp->info.fmt.keyType()!=key.type||fMulti&&tp->info.fmt.isUnique()) return RC_INVPARAM;

	ulong op=TRO_DELETE; ushort lOld=0,n=fMulti&&value!=NULL&&lValue>0?ushort(~0u):0;
	const void *pOld=NULL; ulong idx=0; const PagePtr *vp=NULL;
	if (tp->info.nSearchKeys==0 || !tp->findKey(key,idx) ||
		!tp->info.fmt.isKeyOnly() && (pOld=tp->findData(idx,lOld,&vp))==NULL) return RC_NOTFOUND;
	
	bool fMerge=(tctx.mode&TF_WITHDEL)!=0 && tp->info.nSearchKeys==1;
	if (!tp->info.fmt.isUnique()) {
		if (TreePage::isSubTree(*vp)) {
			assert(lOld>=sizeof(SubTreePage));
			if (value!=NULL && lValue!=0) return modSubTree(tctx,key,idx,vp,NULL,0,value,lValue,fMulti);
			SubTreePage tpe; memcpy(&tpe,(byte*)pOld,sizeof(SubTreePage)); RC rc;
			if (tpe.leftMost!=INVALID_PAGEID && (rc=Tree::drop(tpe.leftMost,ctx))!=RC_OK) return rc;
			n=ushort(~1u); fMerge=false;
			if (lOld>sizeof(SubTreePage)) {
				// calculate complete
			}
		} else if (value!=NULL && lValue!=0) {
			if (tp->findValue(value,lValue,*vp)==NULL) {
				return RC_NOTFOUND;
			}
			// check if last! if not -> fMerge=false;
			op=TRO_UPDATE; pOld=value; lOld=lValue;
		} else n=ushort(~0u);
	}
	if (!fMerge) tctx.parent.release();

	ulong lrec=sizeof(TreePageModify)+key.extLength(); assert(tctx.mainKey==NULL);
	TreeFactory *tf=tctx.tree->getFactory(); ushort lFact=tf!=NULL?tf->getParamLength()+2:0;
	TreePageModify *tpm=(TreePageModify*)alloca(lrec+lOld+lFact); if (tpm==NULL) return RC_NORESOURCES;
	if (pOld!=NULL && lOld!=0) {
		if (n!=ushort(~1u)||lOld==sizeof(SubTreePage)) memcpy((byte*)tpm+lrec,pOld,lOld);
		else {
			//...
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
				TreePageMgr::TreePage *tp=(TreePageMgr::TreePage*)tctx.pb->getPageBuf(); ulong pos;
				if (tp->info.stamp!=tctx.stamps[PITREE_1STLEV] || tp->getChild(key,pos,false)!=pid) fPost=false;
				else {
					assert(tp->info.level>0);
					PageID lpid=pos==0||pos==~0ul?tp->info.leftMost:tp->getPageID(pos-1);
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
			tctx.pb.release(); tctx.pb=spb; tctx.depth++;
//			if (fPost) tctx.postPageOp(tps->newKey,tps->newSibling,true);
//		} else if ((rc2=tctx.tree.removeRootPage(tctx.pb->getPageID(),tp->info.leftMost,0))==RC_OK) {		//???
			// ???tctx.stack[0]=tps->newSibling; tctx.depth++;
		}
	}
	return rc;
}

RC TreePageMgr::merge(PBlock *left,PBlock *right,PBlock *par,Tree& tr,const SearchKey& key,ulong idx)
{
	if (right->isDependent()) {unchain(right,NULL); right->release(); return RC_FALSE;}
	const TreePage *ltp=(const TreePage*)left->getPageBuf(),*rtp=(const TreePage*)right->getPageBuf(),*ptp=(const TreePage*)par->getPageBuf();
	assert(ltp->info.sibling==right->getPageID()&&ltp->info.nEntries==ltp->info.nSearchKeys+1);
	ulong fSize=ltp->info.freeSpaceLength+ltp->info.scatteredFreeSpace+rtp->info.freeSpaceLength+rtp->info.scatteredFreeSpace;
	ushort lPrefix=0,lKey=key.extLength(); PBlockP rpp(right);
	if (rtp->info.nEntries==0) lPrefix=ltp->info.nSearchKeys==1?ltp->calcPrefixSize(0,1):ltp->info.lPrefix;
	else if (ltp->info.nEntries>1 && ltp->info.lPrefix>0) {
		if (rtp->info.lPrefix>0) {
			ushort lkey=rtp->getKeySize(rtp->info.nEntries-1);
			SearchKey *key=(SearchKey*)alloca(sizeof(SearchKey)+lkey);
			if (key==NULL) return RC_NORESOURCES;
			rtp->getKey(rtp->info.nEntries-1,*key);
			lPrefix=ltp->calcPrefixSize(*key,0);
			if (lPrefix<rtp->info.lPrefix) {
				ulong lext=rtp->info.extraEltSize(lPrefix)*rtp->info.nEntries;
				if (lext>fSize) return RC_FALSE; fSize-=lext;
			}
		}
		if (lPrefix<ltp->info.lPrefix) {
			ulong lext=ltp->info.extraEltSize(lPrefix)*(ltp->info.nEntries-1);
			if (lext>fSize) return RC_FALSE; fSize-=lext;
		}
	} else if (rtp->info.lPrefix>0) {
		if (ltp->info.nEntries<=1) lPrefix=rtp->info.lPrefix;
		else {
			ulong lext=rtp->info.extraEltSize(0)*rtp->info.nEntries;
			if (lext>fSize) return RC_FALSE; fSize-=lext;
		}
	}
	if (fSize<xSize) return RC_FALSE;
	size_t lrec=sizeof(TreePageSplit)+lKey+sizeof(TreePageInfo);
	if (rtp->info.nEntries>0) lrec+=xSize-(rtp->info.freeSpaceLength+rtp->info.scatteredFreeSpace);
	TreePageModify *tpm=(TreePageModify*)alloca(max(lrec,size_t(sizeof(TreePageModify)+lKey+sizeof(PageID))));
	if (tpm==NULL) return RC_NORESOURCES;
	MiniTx mtx(NULL,(tr.getMode()&TF_SPLITINTX)!=0?MTX_SKIP:0); RC rc=RC_OK;
	if (idx!=~0u) {
#ifdef _DEBUG
		ulong idx2; assert(ptp->findKey(key,idx2) && idx==idx2);
#endif
		key.serialize(tpm+1); tpm->newPrefSize=ptp->info.lPrefix; tpm->newData.len=0; tpm->newData.offset=0;
		tpm->oldData.len=sizeof(PageID); tpm->oldData.offset=PageOff(sizeof(TreePageModify)+lKey);
		memcpy((byte*)tpm+tpm->oldData.offset,&rtp->hdr.pageID,sizeof(PageID)); RC rc=RC_OK;
		rc=ctx->txMgr->update(par,this,idx<<TRO_SHIFT|TRO_DELETE,(byte*)tpm,sizeof(TreePageModify)+lKey+sizeof(PageID));
	}
	if (rc==RC_OK) {
		TreePageSplit *tps=(TreePageSplit*)tpm;
		tps->nEntriesLeft=ltp->info.nEntries; tps->newSibling=rtp->hdr.pageID;
		tps->oldPrefSize=lPrefix; key.serialize(tps+1);
		byte *p=(byte*)(tps+1)+lKey; memcpy(p,&rtp->info,sizeof(TreePageInfo));
		if (rtp->info.nEntries>0) {
			if (rtp->info.scatteredFreeSpace!=0) ((TreePage*)rtp)->compact();
			ulong ll=rtp->info.nEntries*rtp->info.calcEltSize();
			assert(lrec>=sizeof(TreePageSplit)+lKey+sizeof(TreePageInfo)+ll);
			memcpy(p+sizeof(TreePageInfo),rtp+1,ll);
			memcpy(p+sizeof(TreePageInfo)+ll,(byte*)rtp+rtp->info.freeSpace,lrec-sizeof(TreePageSplit)-lKey-sizeof(TreePageInfo)-ll);
		}
		rc=ctx->txMgr->update(left,this,TRO_MERGE,(byte*)tps,lrec,0,right); assert(!right->isDependent());
		if (rc==RC_OK) {PageID pid=rtp->hdr.pageID; rpp=NULL; right->release(PGCTL_DISCARD); ctx->fsMgr->freePage(pid); mtx.ok();}
	}
	return rc;
}

RC TreePageMgr::drop(PBlock* &pb,TreeFreeData *dd)
{
	const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
	if ((!tp->info.fmt.isUnique()||dd!=NULL) && tp->info.nSearchKeys>0) {
		ushort lElt=tp->info.calcEltSize(),sht=tp->info.keyLength();
		const byte *p=(const byte*)(tp+1); RC rc;
		for (ushort i=0; i<tp->info.nSearchKeys; ++i,p+=lElt)
			if (dd!=NULL) {if ((rc=dd->freeData(p+sht,ctx))!=RC_OK) return rc;}
			else {
				const PagePtr *vp=(const PagePtr*)(p+sht);
				if (TreePage::isSubTree(*vp)) {
					assert((vp->len&~TRO_EXT_DATA)>=sizeof(SubTreePage) && ((vp->len&~TRO_EXT_DATA)-sizeof(SubTreePage))%sizeof(SubTreePageKey)==0);
					SubTreePage tpe; memcpy(&tpe,(const byte*)tp+vp->offset,sizeof(SubTreePage));
					if (tpe.leftMost!=INVALID_PAGEID && (rc=Tree::drop(tpe.leftMost,ctx))!=RC_OK) return rc;
				}
			}
	}
	size_t lrec=xSize-tp->info.freeSpaceLength+sizeof(TreePageInfo);
	byte *rec=(byte*)alloca(lrec); if (rec==NULL) return RC_NORESOURCES;
	memcpy(rec,&tp->info,sizeof(TreePageInfo));
	if (tp->info.nEntries>0) {
		if (tp->info.fmt.isVarKeyOnly()) memcpy(rec+sizeof(TreePageInfo),tp+1,((ushort*)(tp+1))[tp->info.nEntries]);
		else {
			ulong ll=tp->info.nEntries*tp->info.calcEltSize(); memcpy(rec+sizeof(TreePageInfo),tp+1,ll);
			memcpy(rec+sizeof(TreePageInfo)+ll,(byte*)tp+tp->info.freeSpace,lrec-sizeof(TreePageInfo)-ll);
		}
	}
	PageID pid=pb->getPageID();
	ctx->logMgr->insert(Session::getSession(),LR_DISCARD,TRO_DROP<<PGID_SHIFT|PGID_INDEX,pid,NULL,rec,lrec);
	pb->release(PGCTL_DISCARD); pb=NULL; return ctx->fsMgr->freePage(pid);
}

void TreePageMgr::initPage(byte *frame,size_t len,PageID pid)
{
	static const IndexFormat defaultFormat(KT_ALL,0,0);
	TxPage::initPage(frame,len,pid);
	TreePage *tp=(TreePage*)frame;
	tp->info.fmt				= defaultFormat;
	tp->info.nEntries		= 0;
	tp->info.level			= 0;
	tp->info.flags			= 0;
	tp->info.prefix			= 0;
	tp->info.sibling			= INVALID_PAGEID;
	tp->info.leftMost		= INVALID_PAGEID;
	tp->info.stamp			= 0;
	tp->info.nSearchKeys		= 0;
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

ulong TreePageMgr::enumPages(PageID pid)
{
	ulong cnt=0; PBlock *pb=NULL;
	while (pid!=INVALID_PAGEID && (pb=ctx->bufMgr->getPage(pid,this,0,pb))!=NULL) {
		const TreePageMgr::TreePage *tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
		pid=tp->isLeaf()?INVALID_PAGEID:tp->info.leftMost;
		for (;;tp=(const TreePageMgr::TreePage*)pb->getPageBuf()) {
			++cnt;
			if (tp->isLeaf() && !tp->info.fmt.isUnique()) {
				ushort lElt=tp->info.calcEltSize(),sht=tp->info.keyLength(); const byte *p=(const byte*)(tp+1);
				for (ushort i=0; i<tp->info.nSearchKeys; ++i,p+=lElt) {
					const PagePtr *vp=(const PagePtr*)(p+sht);
					if (TreePage::isSubTree(*vp)) {
						assert((vp->len&~TRO_EXT_DATA)>=sizeof(SubTreePage) && ((vp->len&~TRO_EXT_DATA)-sizeof(SubTreePage))%sizeof(SubTreePageKey)==0);
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

ulong TreePageMgr::checkTree(PageID pid,PageID sib,ulong depth,CheckTreeReport& out,CheckTreeReport *sec,bool fLM)
{
	assert(depth<TREE_MAX_DEPTH); ulong cnt=0; PBlock *pb=NULL; PageID pid2,sib2; ushort i;
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
			else if (!tp->info.fmt.isUnique()) {
				ushort lElt=tp->info.calcEltSize(),sht=tp->info.keyLength(); const byte *p=(const byte*)(tp+1);
				for (ushort i=0; i<tp->info.nSearchKeys; ++i,p+=lElt) {
					const PagePtr *vp=(const PagePtr*)(p+sht);
					if (TreePage::isSubTree(*vp)) {
						assert((vp->len&~TRO_EXT_DATA)>=sizeof(SubTreePage) && ((vp->len&~TRO_EXT_DATA)-sizeof(SubTreePage))%sizeof(SubTreePageKey)==0);
						SubTreePage tpe; memcpy(&tpe,(const byte*)tp+vp->offset,sizeof(SubTreePage));
						PageID pid2=tpe.leftMost,sib2; const byte *p1=(byte*)tp+vp->offset+sizeof(SubTreePage);
						ulong n=((vp->len&~TRO_EXT_DATA)-sizeof(SubTreePage))/sizeof(SubTreePageKey);
						CheckTreeReport &rep=sec!=NULL?*sec:out;
						for (ulong i=0; i<n; ++i,p1+=sizeof(SubTreePageKey),pid2=sib2) {
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

void TreeCtx::getStamps(ulong stamps[TREE_NODETYPE_ALL]) const
{
	if (mainKey==NULL) tree->getStamps(stamps);
	else {stamps[PITREE_LEAF2]=tree->getStamp(PITREE_LEAF2); stamps[PITREE_INT2]=tree->getStamp(PITREE_INT2);}
}

//----------------------------------------------------------------------------------------------

bool TreePageMgr::TreePage::checkPage(bool fWrite) const
{
	const char *what;
#if 0
	ulong maxFree=xSize;
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

int TreePageMgr::TreePage::testKey(const SearchKey& key,ushort idx,bool fPrefix) const
{
	assert(info.fmt.keyType()==key.type && (idx==ushort(~0u)||idx<info.nEntries));
	if (info.fmt.isSeq()) return cmp3(key.v.u,info.prefix+(idx==ushort(~0u)?info.nEntries:idx));
	const byte *ptr=(const byte*)(this+1)+(idx==ushort(~0u)?info.nEntries-1:idx)*info.calcEltSize(); ushort lk=sizeof(uint64_t)-info.lPrefix,lsib; uint64_t u64=info.lPrefix!=0?info.prefix:0;
	switch (key.type) {
	case KT_UINT: assert(lk<=sizeof(uint64_t)); return cmp3(key.v.u,u64|(lk==sizeof(uint8_t)?*(uint8_t*)ptr:lk==sizeof(uint16_t)?*(uint16_t*)ptr:lk==sizeof(uint32_t)?*(uint32_t*)ptr:lk==sizeof(uint64_t)?*(uint64_t*)ptr:0));
	case KT_INT: assert(lk<=sizeof(uint64_t)); u64|=lk==sizeof(uint8_t)?*(uint8_t*)ptr:lk==sizeof(uint16_t)?*(uint16_t*)ptr:lk==sizeof(uint32_t)?*(uint32_t*)ptr:lk==sizeof(uint64_t)?*(uint64_t*)ptr:0; return cmp3(key.v.i,int64_t(u64));
	case KT_FLOAT: assert(info.lPrefix==0); return cmp3(key.v.f,*(float*)ptr);
	case KT_DOUBLE: assert(info.lPrefix==0); return cmp3(key.v.d,*(double*)ptr);
	default: break;
	}
	assert(!key.isNumKey());
	const byte *pkey=(const byte*)key.getPtr2(),*pref=NULL; lk=key.v.ptr.l; int res;
	if (info.lPrefix>0) {
		pref=info.fmt.isFixedLenKey()?(const byte*)&info.prefix:(const byte*)this+((PagePtr*)&info.prefix)->offset;
		if (key.type==KT_VAR) {
			//if ((res=cmpMSegPrefix(pkey,lkey,pref,info.lPrefix,&lsib))!=0) return res;
			//if (lsib!=0) {pkey+=lkey-lsib; lkey=lsib;} else return fPrefix?0:-1;
		} else if (key.type==KT_REF) {
			//...
		} else {
			if ((res=memcmp(pkey,pref,min(lk,info.lPrefix)))!=0) return res;
			if (lk>info.lPrefix) {pkey+=info.lPrefix; lk-=info.lPrefix;} else return fPrefix?0:-1;
		}
	}
	if (info.fmt.isVarKeyOnly()) {lsib=lenK(ptr,0); ptr=(*this)[ptr];}
	else if (!info.fmt.isFixedLenKey()) {lsib=((PagePtr*)ptr)->len; ptr=(byte*)this+((PagePtr*)ptr)->offset;}
	else {assert(info.fmt.keyLength()>=info.lPrefix); lsib=info.fmt.keyLength()-info.lPrefix;}
	if (key.type==KT_VAR) return cmpMSeg(pkey,lk,ptr,lsib);
	if (key.type==KT_REF) return PINRef::cmpPIDs(pkey,lk,ptr,lsib);
	res=(lk|lsib)==0?0:memcmp(pkey,ptr,min(lk,lsib));
	return res!=0||fPrefix&&lk<=lsib?res:cmp3(lk,lsib);
}

bool TreePageMgr::TreePage::testBound(const SearchKey& key,ushort idx,const IndexSeg *sg,unsigned nSegs,bool fStart,bool fPrefix) const
{
	assert(info.fmt.keyType()==key.type && (idx==ushort(~0u)||idx<info.nEntries));
	if (info.fmt.keyType()==KT_VAR) {
		const PagePtr *pp=(const PagePtr*)((byte*)(this+1)+(idx==ushort(~0u)?info.nEntries-1:idx)*info.calcEltSize());
		return cmpBound((const byte *)key.getPtr2(),key.v.ptr.l,(byte*)this+pp->offset,pp->len,sg,nSegs,fStart);
	}
	int c=testKey(key,idx,fPrefix || sg!=NULL && (sg->flags&SCAN_PREFIX)!=0);
	return fStart?c<0||c==0&&(sg==NULL||(sg->flags&SCAN_EXCLUDE_START)==0):c>0||c==0&&(sg==NULL||(sg->flags&SCAN_EXCLUDE_END)==0);
}

template<typename T> bool TreePageMgr::TreePage::findNumKey(T sKey,ulong nEnt,ulong& pos) const {
	const T *keys=(const T *)(this+1);
	for (ulong n=nEnt,base=0; n>0 ;) {
		ulong k=n>>1; T key=keys[pos=base+k];
		if (key==sKey) return true; if (key>sKey) n=k; else {base+=k+1; n-=k+1; pos++;}
	}
	return false;
}

template<typename T> bool TreePageMgr::TreePage::findNumKeyVar(T sKey,ulong nEnt,ulong& pos) const {
	const static unsigned lElt=sizeof(Var<T>)+sizeof(T)-1&~(sizeof(T)-1);
	for (ulong n=nEnt,base=0; n>0 ;) {
		ulong k=n>>1; T key=((Var<T>*)((byte*)(this+1)+(pos=base+k)*lElt))->data;
		if (key==sKey) return true; if (key>sKey) n=k; else {base+=k+1; n-=k+1; pos++;}
	}
	return false;
}

bool TreePageMgr::TreePage::findKey(const SearchKey& skey,ulong& pos) const
{
	assert(info.fmt.keyType()==skey.type && (isLeaf() || info.fmt.isFixedLenData() && info.fmt.dataLength()==sizeof(PageID)));
	ulong nEnt=info.nSearchKeys; 
	if (nEnt==0) pos=0;
	else if (!info.fmt.isFixedLenKey()) {
		const byte *pkey=(const byte*)skey.getPtr2(),*pref=NULL; int cmp;
		ushort lkey=skey.v.ptr.l; const bool fPR=info.fmt.isRefKeyOnly();
		if (info.lPrefix>0) {
			pref=(byte*)this+((PagePtr*)&info.prefix)->offset;
			if (skey.type==KT_VAR) {
//				cmp=cmpMSegPrefix(pkey,lkey,pref,info.lPrefix,&ll);
//				if (cmp<0) {pos=0; return false;} else if (cmp>0) {pos=nEnt; return false;}
//				else if (ll==0) {pos=0; return info.nEntries==1;}
//				pkey+=lkey-ll; lkey=ll;
			} else if (skey.type==KT_REF) {
				//...
			} else {
				cmp=memcmp(pkey,pref,min(lkey,info.lPrefix));
				if (cmp<0 || cmp==0 && lkey<info.lPrefix) {pos=0; return false;} else if (cmp>0) {pos=nEnt; return false;}
				pkey+=info.lPrefix; lkey-=info.lPrefix;
			}
		}
		if (info.fmt.isVarKeyOnly()) {
			const ushort *p0,*p=p0=(const ushort*)(this+1); assert(nKeys(p)==info.nEntries);
			for (ulong n=nEnt;;) {
				ulong k=n>>1; const ushort *q=p+k; ushort l=q[1]-q[0]; const byte *pp=(byte*)p0+q[0];
				if (fPR) cmp=PINRef::cmpPIDs(pp,l,pkey,lkey); else if ((cmp=memcmp(pp,pkey,l<lkey?l:lkey))==0) cmp=cmp3(l,lkey);
				if (cmp==0) {pos=ushort(q-p0); return true;}
				if (cmp>0) {if ((n=k)==0) {pos=ushort(p-p0); break;}} else if ((n-=k+1)==0) {pos=ushort(q-p0+1); break;} else p=q+1;
			}
		} else for (ulong n=nEnt,base=0,lElt=info.calcEltSize(); n>0; ) {
			ulong k=n>>1; const PagePtr *vkey=(const PagePtr*)((byte*)(this+1)+lElt*(pos=base+k));
			cmp=skey.type==KT_VAR?cmpMSeg(pkey,lkey,(byte*)this+vkey->offset,vkey->len):
				skey.type==KT_REF?PINRef::cmpPIDs(pkey,lkey,(byte*)this+vkey->offset,vkey->len):
				(cmp=lkey==0||vkey->len==0?0:memcmp(pkey,(byte*)this+vkey->offset,min(lkey,vkey->len)))==0?int(lkey)-int(vkey->len):cmp;
			if (cmp==0) return true; else if (cmp<0) n=k; else {base+=k+1; n-=k+1; pos++;}
		}
	} else {
		bool fFixed=info.fmt.isFixedLenData(); ushort lkey; const void *pkey;
		if (info.fmt.isSeq()) {
			if (skey.v.u<info.prefix) pos=0;
			else if (skey.v.u>=info.prefix+info.nEntries) pos=info.nEntries;
			else {pos=(ulong)(skey.v.u-info.prefix); return true;}
		} else switch (skey.type) {
		case KT_UINT:
			if (((skey.v.u^info.prefix)&~0ULL<<(sizeof(uint64_t)-info.lPrefix)*8)!=0) pos=skey.v.u<info.prefix?0:nEnt;
			else switch (info.lPrefix) {
			default: assert(0);
			case 0: return fFixed?findNumKey(skey.v.u,nEnt,pos):findNumKeyVar(skey.v.u,nEnt,pos);
			case sizeof(uint32_t): return fFixed?findNumKey((uint32_t)skey.v.u,nEnt,pos):findNumKeyVar((uint32_t)skey.v.u,nEnt,pos);
			case sizeof(uint32_t)+sizeof(uint16_t): return fFixed?findNumKey((uint16_t)skey.v.u,nEnt,pos):findNumKeyVar((uint16_t)skey.v.u,nEnt,pos);
			case sizeof(uint32_t)+sizeof(uint16_t)+sizeof(uint8_t): return fFixed?findNumKey((uint8_t)skey.v.u,nEnt,pos):findNumKeyVar((uint8_t)skey.v.u,nEnt,pos);
			case sizeof(uint64_t): assert(info.nEntries==1); pos=0; return true;
			}
			break;
		case KT_INT:
			if (((skey.v.u^info.prefix)&~0ULL<<(sizeof(uint64_t)-info.lPrefix)*8)!=0) {
				// check neg in 0 prefix
				pos=skey.v.i<(int64_t)info.prefix?0:nEnt;
			} else switch (info.lPrefix) {
			default: assert(0);
			case 0: return fFixed?findNumKey(skey.v.i,nEnt,pos):findNumKeyVar(skey.v.i,nEnt,pos);
			case sizeof(uint32_t): return fFixed?findNumKey((int32_t)skey.v.i,nEnt,pos):findNumKeyVar((int32_t)skey.v.i,nEnt,pos);
			case sizeof(uint32_t)+sizeof(uint16_t): return fFixed?findNumKey((int16_t)skey.v.i,nEnt,pos):findNumKeyVar((int16_t)skey.v.i,nEnt,pos);
			case sizeof(uint32_t)+sizeof(uint16_t)+sizeof(uint8_t): return fFixed?findNumKey((int8_t)skey.v.i,nEnt,pos):findNumKeyVar((int8_t)skey.v.i,nEnt,pos);
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
			for (ulong n=nEnt,base=0,lElt=info.calcEltSize(); n>0; ) {
				ulong k=n>>1; int cmp=memcmp(pkey,(const byte*)(this+1)+(pos=base+k)*lElt,lkey);
				if (cmp==0) return true; if (cmp<0) n=k; else {base+=k+1; n-=k+1; pos++;}
			}
			break;
		}
	}
	return false;
}

bool TreePageMgr::TreePage::findSubKey(const byte *pkey,ushort lkey,ulong& pos) const
{
	if (info.nEntries==0) {pos=0; return false;} 
	const bool fPR=info.fmt.isRefKeyOnly(); assert(info.fmt.isKeyOnly()); 
	if (info.lPrefix>0) {
		if (fPR) {
			//...
		} else {
			const byte *pref=info.fmt.isVarKeyOnly()?(byte*)this+((PagePtr*)&info.prefix)->offset:(byte*)&info.prefix;
			ushort l=min(lkey,info.lPrefix); int cmp=memcmp(pkey,pref,l);
			if (cmp<0 || cmp==0 && lkey==l) {pos=0; return false;} else if (cmp>0) {pos=info.nSearchKeys; return false;}
			pkey+=l; lkey-=l;
		}
	}
	if (!fPR && info.fmt.isFixedLenKey()) for (ulong n=info.nEntries,base=0,lElt=info.fmt.keyLength()-info.lPrefix;;) {
		ulong k=n>>1; int cmp=memcmp(pkey,(const byte*)(this+1)+(pos=base+k)*lElt,lkey);
		if (cmp==0) return true; if (cmp<0) n=k; else {base+=k+1; n-=k+1; pos++;}
		if (n==0) return false;
	}
	const ushort *p0,*p=p0=(const ushort*)(this+1);
	for (ulong n=info.nEntries;;) {
		ulong k=n>>1; const ushort *q=p+k; ushort l=q[1]-q[0]; int cmp=0;
		if (fPR) cmp=PINRef::cmpPIDs((byte*)p0+q[0],l,pkey,lkey); else if ((cmp=memcmp((byte*)p0+q[0],pkey,min(l,lkey)))==0) cmp=cmp3(l,lkey);
		if (cmp==0) {pos=ushort(q-p0); return true;}
		if (cmp>0) {if ((n=k)==0) {pos=ushort(p-p0); return false;}}
		else if ((n-=k+1)==0) {pos=ushort(q-p0+1); return false;} else p=q+1;
	}
}

bool TreePageMgr::TreePage::findSubKey(const byte *pkey,ushort lkey,ulong& pos,const byte *pref,ushort lpref) const
{
	assert(info.fmt.isKeyOnly() && pos<=info.nSearchKeys);
	if (info.nEntries!=0) {
		if (info.fmt.isFixedLenKey()) {
			ulong lElt=info.fmt.keyLength()-info.lPrefix; assert(lElt>=lpref);
			for (ulong n=info.nEntries-pos,base=pos,k=0; n!=0; k=n>>1) {
				const byte *pp=(const byte*)(this+1)+(pos=base+k)*lElt; int cmp;
				if ((pref==NULL || (cmp=memcmp(pp,pref,lpref))==0) && (cmp=memcmp(pp+lpref,pkey,lkey-lpref))==0) return true;
				if (cmp>0) n=k; else {base+=k+1; n-=k+1; pos++;}
			}
		} else {
			const ushort *p0=(const ushort*)(this+1),*p=p0+pos; const bool fPR=info.fmt.isRefKeyOnly();
			for (ulong n=info.nEntries-pos,k=0;;k=n>>1) {
				const ushort *q=p+k; ushort l=q[1]-q[0]; const byte *pp=(byte*)p0+q[0]; int cmp=0;
				if (!fPR) {
					if (pref!=NULL && (cmp=memcmp(pp,pref,min(l,lpref)))==0) if (l<lpref) cmp=1; else {pp+=lpref; l-=lpref;}
					if (cmp==0 && (cmp=memcmp(pp,pkey,min(l,lkey)))==0 && (cmp=cmp3(l,lkey))==0) {pos=ushort(q-p0); return true;}
				} else if ((cmp=PINRef::cmpPIDs(pp,l,pkey,lkey))==0) {pos=ushort(q-p0); return true;}	// pref???
				if (cmp>0) {if ((n=k)==0) {pos=ushort(p-p0); break;}} else if ((n-=k+1)==0) {pos=ushort(q-p0+1); break;} else p=q+1;
			}
		}
	}
	return false;
}

bool TreePageMgr::TreePage::findSubKey(const byte *tab,ulong nElts,ulong idx,ulong& res,bool fVM,bool fPref) const
{
	const bool fPR=info.fmt.isRefKeyOnly(); assert(res<nElts && idx<info.nEntries && info.fmt.isKeyOnly());
	const ushort lElt=fVM?L_SHT:info.fmt.keyLength(); const ushort lpref=fPref?info.lPrefix:0;
	const byte *pref=lpref==0?(const byte*)0:fVM?(byte*)this+((PagePtr*)&info.prefix)->offset:(byte*)&info.prefix;
	ushort lk0=fVM?L_SHT:lElt-info.lPrefix; const byte *p=(const byte*)(this+1)+idx*lk0; if (fVM) {lk0=lenK(p,0); p=(*this)[p];}
	for (ulong base=res,n=nElts-base,k=n-1; n!=0; k=n>>1) {
		const byte *pk=tab+(base+k)*lElt; ushort lk=lElt; int cmp=0;
		if (fVM) {lk=lenK(pk,0); pk=tab+((ushort*)pk)[0];}
		if (!fPR) {
			if (pref!=NULL && (cmp=memcmp(pk,pref,min(lpref,lk)))==0 && lk<lpref) cmp=-1;
			if (cmp==0) {
				ushort ll=min(ushort(lk-lpref),lk0); cmp=memcmp(pk+lpref,p,ll);
				if (cmp==0) {if (ll<lk0) cmp=-1; else if (ushort(lk-lpref)>lk0) cmp=1; else {res=base+k; return true;}}
			}
		} else if ((cmp=PINRef::cmpPIDs(pk,lk,p,lk0))==0) {res=base+k; return true;}	// pref, lpref???
		if (cmp<0) {res=base+=k+1; n-=k+1;} else n=k;
	}
	return false;
}

void *TreePageMgr::TreePage::findData(ulong idx,ushort& lData,const PagePtr **pPtr) const
{
	assert(isLeaf()&&!info.fmt.isKeyOnly()); if (pPtr!=NULL) *pPtr=NULL;
	bool fFixedData=info.fmt.isFixedLenData(); if (fFixedData) lData=info.fmt.dataLength();
	
	if (info.fmt.isSeq()) {
		assert(idx<info.nEntries); 
		byte *pData=(byte*)(this+1)+(fFixedData?lData:sizeof(PagePtr))*idx;
		if (fFixedData) return pData; else if (pPtr!=NULL) *pPtr=(PagePtr*)pData;
		lData=((PagePtr*)pData)->len&~TRO_EXT_DATA; return (byte*)this+((PagePtr*)pData)->offset;
	}

	assert(idx<info.nSearchKeys); 
	if (fFixedData) return info.fmt.isFixedLenKey() ? (byte*)this + info.freeSpace + (info.nSearchKeys-idx-1)*lData :
				(byte*)(this+1) + idx*(sizeof(PagePtr)+(ushort)ceil(lData,sizeof(ushort))) + sizeof(PagePtr);

	const PagePtr *p=(const PagePtr*)((byte*)(this+1)+idx*info.calcEltSize()+info.keyLength()); 
	if (p->offset<sizeof(TreePage)) return NULL; if (pPtr!=NULL) *pPtr=(PagePtr*)p;
	lData=p->len&~TRO_EXT_DATA; return (byte*)this+p->offset;
}

byte *TreePageMgr::TreePage::findValue(const void *val,ushort lv,const PagePtr& vp,ushort *pins) const 
{
	assert(!info.fmt.isUnique() && !isSubTree(vp));
	if (vp.len==0) {if (pins!=NULL) *pins=0; return NULL;}
	if (!info.fmt.isVarMultiData()) {
		ushort l=info.fmt.dataLength(); assert(vp.len>=l && vp.len%l==0);
		const byte *p0,*p=p0=(const byte*)this+vp.offset;
		for (ulong n=vp.len/l;;) {
			ulong k=n>>1; const byte *q=p+k*l; int cmp=memcmp(q,val,l);
			if (cmp==0) {if (pins!=NULL) *pins=ushort(q-p0); return (byte*)q;}
			if (cmp>0) {if ((n=k)==0) {if (pins!=NULL) *pins=ushort(p-p0); return NULL;}}
			else if ((n-=k+1)==0) {if (pins!=NULL) *pins=ushort(q-p0+l); return NULL;}
			else p=q+l;
		}
	} else {
		const byte *p0,*p=p0=(const byte*)this+vp.offset;
		const bool fShort=vp.len<256,fPR=info.fmt.isPinRef(); const byte *q; ushort l,sht; int cmp;
		for (ulong n=fShort?*p-1:_u16(p)/L_SHT-1,lElt=fShort?1:L_SHT;;) {
			ulong k=n>>1; 
			if (fShort) {q=p+k; sht=q[0]; l=q[1]-sht;} else {q=p+k*L_SHT; sht=_u16(q); l=_u16(q+L_SHT)-sht;}
			if (fPR) cmp=PINRef::cmpPIDs(p0+sht,l,(byte*)val,lv); else if ((cmp=memcmp(p0+sht,val,min(l,lv)))==0) cmp=cmp3(l,lv);
			if (cmp==0) {if (pins!=NULL) *pins=ushort(q-p0); return (byte*)q;}
			if (cmp>0) {if ((n=k)==0) {if (pins!=NULL) *pins=ushort(p-p0); return NULL;}}
			else if ((n-=k+1)==0) {if (pins!=NULL) *pins=ushort(q-p0+lElt); return NULL;}
			else p=q+lElt;
		}
	}
}

const TreePageMgr::SubTreePageKey *TreePageMgr::TreePage::findExtKey(const void *key,size_t lkey,const SubTreePageKey *tpk,ulong n,ushort *poff) const
{
	const byte *p=(const byte*)tpk; if (n==0) {if (poff!=NULL) *poff=0; return NULL;}
	for (const bool fPR=info.fmt.isPinRef();;) {
		ulong k=n>>1; const byte *q=p+k*sizeof(SubTreePageKey);
		SubTreePageKey tkey; memcpy(&tkey,q,sizeof(SubTreePageKey)); int cmp;
		if (fPR) cmp=PINRef::cmpPIDs((byte*)this+tkey.key.offset,tkey.key.len,(byte*)key,(unsigned)lkey);
		else if ((cmp=memcmp((byte*)this+tkey.key.offset,key,min(tkey.key.len,ushort(lkey))))==0) cmp=cmp3(tkey.key.len,(ushort)lkey);
		if (cmp==0) {if (poff!=NULL) *poff=ushort(q-(const byte*)tpk); return (const SubTreePageKey*)q;}
		if (cmp>0) {if ((n=k)==0) {if (poff!=NULL) *poff=ushort(p-(const byte*)tpk); return NULL;}}
		else if ((n-=k+1)==0) {if (poff!=NULL) *poff=ushort(q-(const byte*)tpk+sizeof(SubTreePageKey)); return NULL;}
		else p=q+sizeof(SubTreePageKey);
	}
}

void TreePageMgr::TreePage::compact(bool fCheckOnly,ulong extIdx,ushort lext)
{
	assert(!info.fmt.isFixedLenData() || !info.fmt.isFixedLenKey());

	size_t lPage=hdr.length();
	TreePage *page=(TreePage*)alloca(lPage); memcpy(page,this,sizeof(TreePage));
	page->info.initFree(lPage); page->info.lPrefix=info.lPrefix;

#ifdef _DEBUG
	if (fCheckOnly) {if (info.nEntries==0) assert(info.lPrefix==0); else if (!info.fmt.isSeq()) assert(calcPrefixSize(0,info.nEntries)==info.lPrefix);}
#endif
	if (info.nEntries!=0) {
		bool fVarKey=!info.fmt.isFixedLenKey(), fVarData=!info.fmt.isFixedLenData();
		if (info.lPrefix>0 && fVarKey) page->store((byte*)this+((PagePtr*)&info.prefix)->offset,info.lPrefix,*(PagePtr*)&page->info.prefix);
		if (info.fmt.isVarKeyOnly()) {
			ushort l=((ushort*)(this+1))[info.nEntries];
			memcpy(page+1,this+1,l); page->info.freeSpaceLength-=l; assert(extIdx==~0ul && lext==0);
#ifdef _DEBUG
			if (fCheckOnly) {
				const bool fPR=info.fmt.isRefKeyOnly();
				const ushort *p=(const ushort*)(page+1); assert(nKeys(p)==info.nEntries);
				for (int i=info.nEntries-1; --i>=0; ) {
					ulong l1=lenK(p,i),l2=lenK(p,i+1);
					if (fPR) {assert(PINRef::cmpPIDs(getK(p,i),l1,getK(p,i+1),l2)<0);}
					else {int cmp=memcmp(getK(p,i),getK(p,i+1),min(l1,l2)); assert(cmp<0 || cmp==0 && l1<=l2);}
				}
			}
#endif
		} else {
			ushort lElt=info.calcEltSize(),sht=info.keyLength();
			byte *from=(byte*)(this+1),*to=(byte*)(page+1); 
			ushort lMove=lElt*info.nEntries; assert(lMove<page->info.freeSpaceLength);
			memcpy(to,from,lMove); page->info.freeSpaceLength-=lMove; 
			ulong nEnt=info.fmt.isSeq()?info.nEntries:info.nSearchKeys;
			for (ulong idx=0; idx<nEnt; ++idx,from+=lElt,to+=lElt) {
				if (fVarKey) page->store((byte*)this+((PagePtr*)from)->offset,((PagePtr*)from)->len,*(PagePtr*)to);
				if (fVarData) {
					page->store((byte*)this+((PagePtr*)(from+sht))->offset,((PagePtr*)(from+sht))->len,*(PagePtr*)(to+sht));
					if (!info.fmt.isUnique()) {
#ifdef _DEBUG
						if (fCheckOnly && info.fmt.isVarMultiData() && !isSubTree(*(PagePtr*)(from+sht))) {
							PagePtr *vp=(PagePtr*)(to+sht); const byte *ptr=(byte*)page+vp->offset;
							if (vp->len<256) {for (int i=*ptr; --i>=1;) assert(ptr[i-1]<ptr[i] && ptr[i]<=vp->len);}
							else for (int i=_u16(ptr); (i-=L_SHT)>=(int)L_SHT;) assert(_u16(ptr+i-L_SHT)<_u16(ptr+i) && _u16(ptr+i)<=vp->len);
						}
#endif
						if (idx==extIdx) {
							if (isSubTree(*(PagePtr*)(to+sht))) lext=sizeof(SubTreePageKey);
							else if (!info.fmt.isVarMultiData()) lext=info.fmt.dataLength();
							assert(page->info.freeSpaceLength>=lext); lMove=((PagePtr*)(to+sht))->len&~TRO_EXT_DATA;
							page->info.freeSpace-=lext; page->info.freeSpaceLength-=lext;
							memmove((byte*)page+page->info.freeSpace,(byte*)page+page->info.freeSpace+lext,lMove);
							((PagePtr*)(to+sht))->offset=page->info.freeSpace; ((PagePtr*)(to+sht))->len+=lext;
						}
						if (isSubTree(*(PagePtr*)(to+sht))) page->moveExtTree(this,*(PagePtr*)(from+sht),*(PagePtr*)(to+sht));
					}
				}
			}
			if (fVarKey && hasSibling()) page->store((byte*)this+((PagePtr*)from)->offset,((PagePtr*)from)->len,*(PagePtr*)to);
		}
		assert(page->info.freeSpaceLength+lext==info.freeSpaceLength+info.scatteredFreeSpace);
	}
	if (!fCheckOnly) memcpy(&info,&page->info,lPage-FOOTERSIZE-sizeof(TxPageHeader));
}

void TreePageMgr::TreePage::changePrefixSize(ushort prefLen)
{
	size_t lPage=hdr.length(); assert(prefLen!=info.lPrefix && !info.fmt.isSeq());
	TreePage *page=(TreePage*)alloca(lPage); copyEntries(page,prefLen,0);
	memcpy(&info,&page->info,page->info.nEntries!=0?lPage-FOOTERSIZE-sizeof(TxPageHeader):sizeof(TreePageInfo));
}

void TreePageMgr::TreePage::copyEntries(TreePage *page,ushort prefLen,ushort start) const
{
	memcpy(&page->info,&info,sizeof(TreePageInfo)); assert(start<=info.nEntries);
	page->info.initFree(hdr.length()); page->info.lPrefix=prefLen;
	page->info.nEntries=info.nEntries-start; page->info.nSearchKeys=info.nSearchKeys<start?0:info.nSearchKeys-start;

	bool fVarKey=!info.fmt.isFixedLenKey();

	if (info.fmt.isSeq()) {assert(prefLen==0&&info.lPrefix==0); page->info.prefix=info.prefix+start;}
	if (page->info.nEntries==0) {page->info.lPrefix=0; return;}

	assert(prefLen<=info.lPrefix || info.nEntries>0);
	ushort lElt=info.calcEltSize(); const byte *startElt=(const byte*)(this+1)+start*lElt;
	if (fVarKey) {
		if (prefLen>info.lPrefix) {
			if (info.fmt.isVarKeyOnly()) 
				page->store((byte*)(this+1)+*(ushort*)startElt,prefLen-info.lPrefix,*(PagePtr*)&page->info.prefix);
			else
				page->store((byte*)this+((PagePtr*)startElt)->offset,prefLen-info.lPrefix,*(PagePtr*)&page->info.prefix);
		}
		page->store((byte*)this+((PagePtr*)&info.prefix)->offset,min(info.lPrefix,prefLen),*(PagePtr*)&page->info.prefix);
		((PagePtr*)&page->info.prefix)->len=prefLen;
	} else if (info.fmt.isPrefNumKey()) {
		uint64_t v=info.lPrefix>0?info.prefix:0; ulong lk=info.fmt.keyLength();
		switch (lk-info.lPrefix) {
		case 0: v=info.prefix; break;
		case sizeof(uint8_t): v|=*(uint8_t*)startElt; break;
		case sizeof(uint16_t): v|=*(uint16_t*)startElt; break;
		case sizeof(uint32_t): v|=*(uint32_t*)startElt; break;
		case sizeof(uint64_t): v=*(uint64_t*)startElt; break;
		}
		page->info.prefix=v&~0ULL<<(lk-prefLen)*8;
	} else {
		assert(prefLen<=sizeof(page->info.prefix));
		if (prefLen>info.lPrefix) memcpy((byte*)&page->info.prefix+info.lPrefix,startElt,prefLen-info.lPrefix);
	}

	byte *pref=NULL; ushort delta=0;
	if (info.lPrefix<prefLen) delta=prefLen-info.lPrefix;
	else if (info.lPrefix>prefLen && !info.fmt.isPrefNumKey()) {
		assert(!info.fmt.isNumKey()); delta=info.lPrefix-prefLen;
		pref=fVarKey?(byte*)this+((PagePtr*)&info.prefix)->offset+prefLen:(byte*)&info.prefix+prefLen;
	}
	bool fVarData=!info.fmt.isFixedLenData();
	if (fVarKey) {
		if (info.fmt.isVarKeyOnly()) {
			const ushort *from=(ushort*)startElt; ushort *to=(ushort*)(page+1);
			ushort sht=(info.nEntries-start+1)*L_SHT; page->info.freeSpaceLength-=sht;
			for (ushort idx=start; idx<info.nEntries; ++idx,++from,++to) {
				ushort l=from[1]-from[0]; *to=sht;
				if (info.lPrefix<=prefLen) {assert(l>=delta); memcpy((byte*)(page+1)+sht,(byte*)(this+1)+from[0]+delta,l-=delta);}
				else {memcpy((byte*)(page+1)+sht,pref,delta); memcpy((byte*)(page+1)+sht+delta,(byte*)(this+1)+from[0],l); l+=delta;}
				assert(l<=page->info.freeSpaceLength); page->info.freeSpaceLength-=l; sht+=l;
			}
			*to=sht;
		} else {
			const PagePtr *from=(PagePtr*)startElt; PagePtr *to=(PagePtr*)(page+1); 
			ushort lMove=(info.nEntries-start)*lElt; memcpy(to,from,lMove); page->info.freeSpaceLength-=lMove;
			for (ushort idx=start; idx<info.nEntries; idx++) {
				if (info.lPrefix<prefLen) {
					assert(from->len>=delta); page->store((byte*)this+from->offset+delta,from->len-delta,*to);
				} else {
					page->store((byte*)this+from->offset,from->len,*to); 
					if (info.lPrefix>prefLen) {page->store(pref,delta,*to); to->len=from->len+delta;}
				}
				if (fVarData && idx<info.nSearchKeys) {
					page->store((byte*)this+from[1].offset,from[1].len,to[1]);
					if (isSubTree(to[1])) page->moveExtTree(this,from[1],to[1]);
				}
				from=(PagePtr*)((byte*)from+lElt); to=(PagePtr*)((byte*)to+lElt);
			}
		}
	} else {
		if (!fVarData) {
			ushort lMove=(info.nEntries-start)*info.fmt.dataLength(); 
			assert(lMove<page->info.freeSpaceLength && info.scatteredFreeSpace==0);
			page->info.freeSpace-=lMove; page->info.freeSpaceLength-=lMove;
			memcpy((byte*)page+page->info.freeSpace,(byte*)this+info.freeSpace,lMove);
		}
		ushort lkey=info.fmt.keyLength(),lEltTo=page->info.calcEltSize(); assert(lkey>=info.lPrefix && lkey>=prefLen);
		ushort lkOld=lkey-info.lPrefix,lkNew=lkey-prefLen; uint64_t mask=0;
		if (info.lPrefix>prefLen && info.fmt.isPrefNumKey())		
			mask=info.prefix<<(sizeof(uint64_t)-lkNew)>>(sizeof(uint64_t)-lkNew+lkOld)<<lkOld;
		ushort sht=(ushort)ceil(lkOld,sizeof(short)),shtTo=(ushort)ceil(lkNew,sizeof(short));
		assert(lEltTo*(info.nEntries-start)<=page->info.freeSpaceLength);
		page->info.freeSpaceLength-=lEltTo*(info.nEntries-start);
		const byte *from=startElt; byte *to=(byte*)(page+1); 
		for (ushort idx=start; idx<info.nEntries; ++idx,from+=lElt,to+=lEltTo) {
			if (info.fmt.isPrefNumKey()) {
				uint64_t ukey=0;
				switch (lkOld) {
				case 0: assert(info.nEntries<=1); ukey=mask; break;
				case sizeof(uint8_t): if (!fVarData) {ukey=*(uint8_t*)from|mask; break;}
				case sizeof(uint16_t): ukey=*(uint16_t*)from|mask; break;
				case sizeof(uint32_t): ukey=*(uint32_t*)from|mask; break;
				case sizeof(uint64_t): ukey=*(uint64_t*)from|mask; break;
				default: assert(0);
				}
				switch (lkNew) {
				case 0: assert(page->info.nEntries<=1); break;
				case sizeof(uint8_t): if (!fVarData) {*(uint8_t*)to=uint8_t(ukey); break;}
				case sizeof(uint16_t): *(uint16_t*)to=uint16_t(ukey); break;
				case sizeof(uint32_t): *(uint32_t*)to=uint32_t(ukey); break;
				case sizeof(uint64_t): *(uint64_t*)to=ukey; break;
				default: assert(0);
				}
			} else if (prefLen>info.lPrefix) memcpy(to,from+delta,lkNew);
			else {if (delta>0) memcpy(to,pref,delta); memcpy(to+delta,from,lkOld);}
			if (fVarData && idx<info.nSearchKeys) {
				page->store((byte*)this+((PagePtr*)(from+sht))->offset,((PagePtr*)(from+sht))->len,*((PagePtr*)(to+shtTo)));
				if (isSubTree(*((PagePtr*)(to+shtTo)))) page->moveExtTree(this,*(PagePtr*)(from+sht),*(PagePtr*)(to+shtTo));
			}
		}
	}
}

ushort TreePageMgr::TreePage::calcPrefixSize(const SearchKey &key,ulong idx,bool fExt) const
{
	idx=min(idx,ulong(info.nEntries-1)); assert(info.nEntries>0);
	if (info.fmt.isPrefNumKey()) {
		const byte *ptr=(const byte*)(this+1)+info.calcEltSize()*idx; uint64_t u=info.lPrefix!=0?info.prefix:0;
		switch (info.lPrefix) {
		default: assert(0);
		case sizeof(uint64_t): break;
		case sizeof(uint32_t)+sizeof(uint16_t)+sizeof(uint8_t): assert(info.fmt.isFixedLenData()); break;
		case sizeof(uint32_t)+sizeof(uint16_t): u|=*(uint16_t*)ptr; break;
		case sizeof(uint32_t): u|=*(uint32_t*)ptr; break;
		case 0: u=*(uint64_t*)ptr; break;
		}
		u^=key.v.u;
		if (key.type==KT_INT) {
			//...
		}
		return uint32_t(u>>32)!=0?0:(uint32_t(u)&0xFFFF0000)!=0?sizeof(uint32_t):!info.fmt.isFixedLenData()||(uint16_t(u)&0xFF00)!=0?sizeof(uint32_t)+sizeof(uint16_t):sizeof(uint32_t)+sizeof(uint16_t)+sizeof(uint8_t);
	} else if (!info.fmt.isNumKey()) {
		const byte *pkey=(const byte*)key.getPtr2(); ushort lCmp;
		if (info.fmt.isRefKeyOnly() || info.fmt.keyType()==KT_REF) {
			// ???
		} else if (info.fmt.keyType()==KT_VAR) {
			// ???
		} else {
			if (info.lPrefix>0) {
				const byte *p=info.fmt.isFixedLenKey()?(const byte*)&info.prefix:(const byte*)this+((PagePtr*)&info.prefix)->offset;
				ushort l=min(info.lPrefix,key.v.ptr.l); for (ushort i=0; i<l; i++) if (*pkey++!=*p++) {l=i; break;}
				if (!fExt||l<info.lPrefix) return l;
			}
			const byte *ptr=(const byte*)(this+1)+info.calcEltSize()*idx;
			if (info.fmt.isFixedLenKey()) {
				lCmp=info.fmt.keyLength()-info.lPrefix; assert(info.lPrefix<=sizeof(info.prefix));
				if (size_t(lCmp+info.lPrefix)>sizeof(info.prefix) && (lCmp=sizeof(info.prefix)-info.lPrefix)==0) return info.lPrefix;
			} else if (info.fmt.isVarKeyOnly()) {
				lCmp=min(int(lenK(ptr,0)),int(key.v.ptr.l-info.lPrefix)); ptr=(*this)[ptr];
			} else {
				lCmp=min(((PagePtr*)ptr)->len,ushort(key.v.ptr.l-info.lPrefix));
				ptr=(const byte*)this+((PagePtr*)ptr)->offset;
			}
			for (ushort i=0; i<lCmp; i++) if (*ptr++!=*pkey++) {lCmp=i; break;}
			return lCmp+info.lPrefix;
		}
	}
	return 0;
}

ushort TreePageMgr::TreePage::calcPrefixSize(ulong start,ulong end) const
{
	if (info.fmt.isNumKey()&&!info.fmt.isPrefNumKey()) return 0;
	if (info.fmt.isRefKeyOnly() || info.fmt.keyType()==KT_REF) {
		// ???
		return 0;
	}
	if (info.fmt.keyType()==KT_VAR) {
		// ???
		return 0;
	}
	if (start+1>=end) {
		ushort l=getKeySize(ushort(start));
		return info.fmt.isFixedLenKey() && l>sizeof(info.prefix)?sizeof(info.prefix):l;
	}
	ushort lElt=info.calcEltSize(),lCmp; assert(start<end);
	const byte *p1=(byte*)(this+1)+start*lElt,*p2=(byte*)(this+1)+(end-1)*lElt;
	if (info.fmt.isPrefNumKey()) switch (info.lPrefix) {
	default: assert(0);
	case sizeof(uint64_t): //????
	case sizeof(uint32_t)+sizeof(uint16_t)+sizeof(uint8_t): assert(info.fmt.isFixedLenData()); return info.lPrefix;
	case sizeof(uint32_t)+sizeof(uint16_t): return !info.fmt.isFixedLenData()||((*(uint16_t*)p1^*(uint16_t*)p2)&0xFF00)!=0?info.lPrefix:info.lPrefix+sizeof(uint8_t);
	case sizeof(uint32_t):
		{uint32_t t=*(uint32_t*)p1^*(uint32_t*)p2;
		return (t&0xFFFF0000)!=0?info.lPrefix:!info.fmt.isFixedLenData()||(t&0xFF00)!=0?info.lPrefix+sizeof(uint16_t):info.lPrefix+sizeof(uint16_t)+sizeof(uint8_t);}
	case 0:
		assert(info.lPrefix==0);
		{uint64_t t=*(uint64_t*)p1^*(uint64_t*)p2;
		return uint32_t(t>>32)!=0?0:(uint32_t(t)&0xFFFF0000)!=0?sizeof(uint32_t):
				!info.fmt.isFixedLenData()||(uint32_t(t)&0xFF00)!=0?sizeof(uint32_t)+sizeof(uint16_t):sizeof(uint32_t)+sizeof(uint16_t)+sizeof(uint8_t);}
	}
	if (info.fmt.isFixedLenKey()) {
		lCmp=info.fmt.keyLength()-info.lPrefix; assert(info.lPrefix<=sizeof(info.prefix));
		if (size_t(lCmp+info.lPrefix)>sizeof(info.prefix) && (lCmp=sizeof(info.prefix)-info.lPrefix)==0) return info.lPrefix;
	} else if (info.fmt.isVarKeyOnly()) {
		lCmp=min(lenK(p1,0),lenK(p2,0)); p1=(*this)[p1]; p2=(*this)[p2];
	} else {
		lCmp=min(((PagePtr*)p1)->len,((PagePtr*)p2)->len);
		p1=(const byte*)this+((PagePtr*)p1)->offset; p2=(const byte*)this+((PagePtr*)p2)->offset;
	}
	for (ushort i=0; i<lCmp; ++p1,i++) if (*p1!=*p2++) {lCmp=i; break;}
	return lCmp+info.lPrefix;
}

ushort TreePageMgr::TreePage::getKeySize(ushort idx) const
{
	if (info.nEntries==0) return 0; if (idx==ushort(~0u)) idx=info.nEntries-1; assert(idx<info.nEntries);
	return info.fmt.isVarKeyOnly() ? lenK(idx)+info.lPrefix : info.fmt.isSeq() ? 0 : info.fmt.isFixedLenKey() ? info.fmt.keyLength() : 
		((PagePtr*)((byte*)(this+1)+idx*(sizeof(PagePtr)+(info.fmt.isFixedLenData()? (ushort)ceil(info.fmt.dataLength(),sizeof(ushort)):sizeof(PagePtr)))))->len+info.lPrefix;
}

void TreePageMgr::TreePage::getKey(ushort idx,SearchKey& key) const
{
	key.loc=SearchKey::PLC_EMB;
	if (info.fmt.isSeq()) {key.v.u=info.prefix+(idx==ushort(~0u)?info.nEntries:idx); key.type=KT_UINT; return;}
	if (idx==ushort(~0u)) {if (info.nEntries==0) {key.type=KT_ALL; return;} else idx=info.nEntries-1;} assert(idx<info.nEntries);
	const byte *p=(const byte*)(this+1)+idx*info.calcEltSize(); bool fFixed; ushort lkey;
	switch (key.type=info.fmt.keyType()) {
	case KT_UINT: case KT_INT:
		switch (info.lPrefix) {
		case 0: key.v.u=*(uint64_t*)p; break;
		case sizeof(uint32_t): key.v.u=*(uint32_t*)p|info.prefix; break;
		case sizeof(uint32_t)+sizeof(uint16_t): key.v.u=*(uint16_t*)p|info.prefix; break;
		case sizeof(uint32_t)+sizeof(uint16_t)+sizeof(uint8_t): key.v.u=*(uint8_t*)p|info.prefix; break;
		case sizeof(uint64_t): key.v.u=info.prefix; break;
		}
		break;
	case KT_FLOAT: key.v.f=*(float*)p; break;;
	case KT_DOUBLE: key.v.d=*(double*)p; break;
	default:
		fFixed=info.fmt.isFixedLenKey(); assert(!info.fmt.isNumKey());
		if (info.lPrefix>0) memcpy((byte*)(&key+1),fFixed?(const byte*)&info.prefix:(const byte*)this+((PagePtr*)&info.prefix)->offset,info.lPrefix);
		if (fFixed) lkey=info.fmt.keyLength()-info.lPrefix;
		else if (info.fmt.isVarKeyOnly()) {lkey=lenK(p,0); p=(*this)[p];}
		else {lkey=((PagePtr*)p)->len; p=(const byte*)this+((PagePtr*)p)->offset;}
		memcpy((byte*)(&key+1)+info.lPrefix,p,lkey); key.v.ptr.p=NULL; key.v.ptr.l=info.lPrefix+lkey; key.loc=SearchKey::PLC_SPTR;
	}
}

void TreePageMgr::TreePage::serializeKey(ushort idx,void *buf) const
{
	byte *pb=(byte*)buf; uint64_t u64;
	if (info.fmt.isSeq()) {u64=info.prefix+(idx==ushort(~0u)?info.nEntries:idx); *pb=KT_UINT; memcpy(pb+1,&u64,sizeof(uint64_t)); return;}
	if (idx==ushort(~0u)) {if (info.nEntries==0) {*pb=KT_ALL; return;} else idx=info.nEntries-1;}
	assert(idx<info.nEntries); *pb=info.fmt.keyType();
	const byte *p=(const byte*)(this+1)+idx*info.calcEltSize();
	if (info.fmt.isPrefNumKey() && info.lPrefix!=0) {
		switch (info.lPrefix) {
		default: assert(0);
		case sizeof(uint64_t): u64=info.prefix; break;
		case sizeof(uint32_t)+sizeof(uint16_t)+sizeof(uint8_t): u64=*(uint8_t*)p|info.prefix; break;
		case sizeof(uint32_t)+sizeof(uint16_t): u64=*(uint16_t*)p|info.prefix; break;
		case sizeof(uint32_t): u64=*(uint32_t*)p|info.prefix; break;
		}
		memcpy(pb+1,&u64,sizeof(uint64_t)); return;
	}
	if (*pb<KT_BIN) {memcpy(pb+1,p,SearchKey::extKeyLen[*pb]); return;}
	const bool fFixed=info.fmt.isFixedLenKey(); ushort lkey; assert(!info.fmt.isNumKey()); 
	if (fFixed) lkey=info.fmt.keyLength()-info.lPrefix;
	else if (info.fmt.isVarKeyOnly()) {lkey=lenK(p,0); p=(*this)[p];}
	else {lkey=((PagePtr*)p)->len; p=(const byte*)this+((PagePtr*)p)->offset;}
	ushort l=lkey+info.lPrefix; if (l<128) pb[1]=byte(l); else {*++pb=byte(l|0x80); pb[1]=byte(l>>7);}
	if (info.lPrefix>0) memcpy(pb+2,fFixed?(const byte*)&info.prefix:(const byte*)this+((PagePtr*)&info.prefix)->offset,info.lPrefix);
	memcpy(pb+2+info.lPrefix,p,lkey);
}

uint32_t TreePageMgr::TreePage::getKeyU32(ushort idx) const {
	assert(idx<info.nEntries && info.fmt.keyType()==KT_UINT);
	const byte *p=(const byte*)(this+1)+idx*info.calcEltSize();
	switch (info.lPrefix) {
	default: assert(0); return ~0u;
	case sizeof(uint64_t): return (uint32_t)info.prefix;
	case sizeof(uint32_t)+sizeof(uint16_t)+sizeof(uint8_t): return (uint32_t)(*(uint8_t*)p|(uint32_t)info.prefix);
	case sizeof(uint32_t)+sizeof(uint16_t): return (uint32_t)(*(uint16_t*)p|(uint32_t)info.prefix);
	case sizeof(uint32_t): return *(uint32_t*)p;
	case 0: return (uint32_t)*(uint64_t*)p;
	}
}

RC TreePageMgr::TreePage::prefixFromKey(const void *pk)
{
	const byte *key=(const byte *)pk;
	if (info.fmt.isPrefNumKey()) memcpy(&info.prefix,key+1,info.lPrefix=sizeof(uint64_t));
	else if (!info.fmt.isNumKey()) {
		if (info.fmt.isRefKeyOnly() || info.fmt.keyType()==KT_REF) {
			//???
		} else if (info.fmt.keyType()==KT_VAR) {
			//???
		} else {
			if (*key<KT_BIN) return RC_CORRUPTED; ushort lk=SearchKey::keyLenP(key);
			if (info.freeSpaceLength<lk) {compact(); if (info.freeSpaceLength<lk) return RC_CORRUPTED;}
			if (!info.fmt.isFixedLenKey()) {store(key,lk,*(PagePtr*)&info.prefix); info.lPrefix=lk;}
			else memcpy(&info.prefix,key,info.lPrefix=min(lk,(ushort)sizeof(info.prefix)));
		}
	}
	return RC_OK;
}

void TreePageMgr::TreePage::storeKey(const void *pk,void *ptr)
{
	const byte *key=(const byte*)pk; ushort lk,lkey; uint64_t u;
	switch (*key) {
	default:
		lk=SearchKey::keyLenP(key); lkey=lk-info.lPrefix; assert(lk>=info.lPrefix);
		if (!info.fmt.isVarKeyOnly()) {
			if (info.fmt.isFixedLenKey()) memcpy(ptr,key+info.lPrefix,lkey);
			else if (lkey==0) ((PagePtr*)ptr)->len=((PagePtr*)ptr)->offset=0;
			else {
				if (lkey>info.freeSpaceLength) {compact(); assert(lkey<=info.freeSpaceLength);}
				store(key+info.lPrefix,lkey,*(PagePtr*)ptr);
			}
		} else if (info.nEntries==0) {
			((ushort*)ptr)[0]=2*L_SHT; ((ushort*)ptr)[1]=2*L_SHT+lkey;
			if (lkey!=0) memcpy((byte*)ptr+2*L_SHT,key+info.lPrefix,lkey);
			info.freeSpaceLength-=lkey+L_SHT; assert(ptr==this+1);
		} else {
			assert(lkey+L_SHT<=info.freeSpaceLength); ushort *p=(ushort*)(this+1);
			ulong sht=*(ushort*)ptr,lh=p[0],l=p[lh/L_SHT-1]; int off=int((ushort*)ptr-p);
			if (sht<l) memmove((byte*)p+sht+lkey,(byte*)p+sht,l-sht);
			memcpy((byte*)p+sht,key+info.lPrefix,lkey);
			memmove((ushort*)ptr+1,ptr,l-off*L_SHT+lkey); info.freeSpaceLength-=lkey;
			int j=lh/L_SHT+1; lkey+=L_SHT;
			while (--j>off) p[j]+=lkey; do p[j]+=L_SHT; while (--j>=0);
		}
		break;
	case KT_UINT: case KT_INT:
		memcpy(&u,key+1,sizeof(uint64_t));
		switch (info.lPrefix) {
		default: assert(0);
		case 0: *(uint64_t*)ptr=u; break;
		case sizeof(uint32_t): *(uint32_t*)ptr=uint32_t(u); break;
		case sizeof(uint32_t)+sizeof(uint16_t): *(uint16_t*)ptr=uint16_t(u); break;
		case sizeof(uint32_t)+sizeof(uint16_t)+sizeof(uint8_t): *(uint8_t*)ptr=uint8_t(u); break;
		case sizeof(uint64_t): break;
		}
		break;
	case KT_FLOAT: memcpy(ptr,key+1,sizeof(float)); break;
	case KT_DOUBLE: memcpy(ptr,key+1,sizeof(double)); break;
	}
}

ushort TreePageMgr::TreePage::length(const PagePtr& ptr) const
{
	assert(!info.fmt.isVarKeyOnly());
	if (info.fmt.isUnique()||!isSubTree(ptr)) return ptr.len;
	ushort l=ptr.len&~TRO_EXT_DATA; PagePtr pp;
	assert((l-sizeof(SubTreePage))%sizeof(SubTreePageKey)==0);
	const byte *p=(byte*)this+ptr.offset+sizeof(SubTreePage);
	for (int i=(l-sizeof(SubTreePage))/sizeof(SubTreePageKey); --i>=0; p+=sizeof(SubTreePageKey)) 
		{memcpy(&pp,&((SubTreePageKey*)p)->key,sizeof(PagePtr)); l+=pp.len;}
	return l;
}

void TreePageMgr::TreePage::moveExtTree(const TreePage *src,const PagePtr& from,PagePtr& to)
{
	ushort l=from.len&~TRO_EXT_DATA;
	assert(isSubTree(from) && to.len>=from.len && (l-sizeof(SubTreePage))%sizeof(SubTreePageKey)==0);
	if (l>sizeof(SubTreePage)) {
		const byte *pf=(const byte*)src+from.offset+sizeof(SubTreePage);
		byte *pt=(byte*)this+to.offset+sizeof(SubTreePage); PagePtr ppf,ppt; 
		for (int i=(l-sizeof(SubTreePage))/sizeof(SubTreePageKey); --i>=0; pf+=sizeof(SubTreePageKey),pt+=sizeof(SubTreePageKey)) {
			memcpy(&ppf,&((SubTreePageKey*)pf)->key,sizeof(PagePtr));
			store((const byte*)src+ppf.offset,ppf.len,ppt);
			memcpy(&((SubTreePageKey*)pt)->key,&ppt,sizeof(PagePtr));
		}
	}
}

bool IndexFormat::isValid(size_t lPage,ushort level) const
{
	if (isFixedLenKey()) {ushort lKey=keyLength(); if (lKey==0 && !isSeq() || lKey>lPage/4) return false;}
	if (level>0) {if (!isFixedLenData() || dataLength()!=sizeof(PageID)) return false;}
	else if (isFixedLenData()) {ushort lData=dataLength(); if (lData>lPage/4) return false;}
	// ...
	return true;
}
