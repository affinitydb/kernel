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

#include "pgheap.h"
#include "buffer.h"
#include "txmgr.h"
#include "logmgr.h"
#include "session.h"
#include "fsmgr.h"
#include "lock.h"

using namespace Afy;
using namespace AfyKernel;

const HType HType::compactRef={0,HDF_COMPACT<<6|VT_REFID};

#define	PINOP_ERROR(a)	{assert(rc==RC_OK); rc=(a); nop=hpi->nops-nop-1; flags^=TXMGR_UNDO; continue;}

HeapPageMgr::HeapPageMgr(StoreCtx *ctx,PGID pgid) : TxPage(ctx),freeSpace(ctx)
{
	ctx->registerPageMgr(pgid,this);
}

HeapPageMgr::~HeapPageMgr()
{
}

RC HeapPageMgr::update(PBlock *pb,size_t len,unsigned info,const byte *rec,size_t lrec,unsigned flags,PBlock*)
{
	byte *frame=pb->getPageBuf(); HeapPage *hp=(HeapPage*)frame;
#ifdef _DEBUG
	hp->compact(true);
	hp->checkPage(false);
#endif
	const ushort op0=(ushort)(info&HPOP_MASK); PageIdx idx=(PageIdx)(info>>HPOP_SHIFT);
	if ((rec==NULL || lrec==0) && op0!=HPOP_DELETE) return RC_CORRUPTED;
	if (op0>=HPOP_ALL) {
		report(MSG_ERROR,"Invalid HPOP %d in HeapPageMgr::update, page %08X\n",op0,hp->hdr.pageID);
		return RC_CORRUPTED;
	}
	const static HPOP undoOP[HPOP_ALL]={HPOP_PURGE,HPOP_EDIT,HPOP_DELETE,HPOP_INSERT,HPOP_PINOP,HPOP_MIGRATE,HPOP_SETFLAG};
	ushort op=(flags&TXMGR_UNDO)!=0?(ushort)undoOP[op0]:op0; const PageIdx *idxs=NULL; unsigned nObjs=1;
	size_t lr=ceil(lrec,HP_ALIGN); PageOff off; HeapObjHeader *hdr;
	switch (op) {
	case HPOP_MIGRATE:
		if ((flags&TXMGR_UNDO)==0) break;
		if ((hdr=hp->getObject(off=hp->getOffset(idx)))==NULL || hdr->getType()!=HO_FORWARD) return RC_CORRUPTED;
		if (off+sizeof(HeapObjHeader)+PageAddrSize!=hp->freeSpace) hp->scatteredFreeSpace+=sizeof(HeapObjHeader)+PageAddrSize; else hp->freeSpace=off;
		(*hp)[idx]=0; op=HPOP_INSERT; lrec-=PageAddrSize; lr=ceil(lrec,HP_ALIGN);
	case HPOP_INSERT: case HPOP_DELETE: case HPOP_PURGE:
		if ((idx&0x8000)!=0) {
			idxs=(PageIdx*)rec; nObjs=idx&~0x8000; rec+=nObjs*sizeof(PageIdx);
		} else if (op!=HPOP_DELETE && op0!=HPOP_MIGRATE && ((HeapObjHeader*)rec)->getLength()!=lr) {
			bool fCorrupted=true;
			if (((HeapObjHeader*)rec)->getType()==HO_PIN) {
				const byte *p=rec,*end=rec+lr;
				for (nObjs=0; p<end; nObjs++) p+=((HeapObjHeader*)p)->getLength();
				if (p==end) fCorrupted=false;
			}
			if (fCorrupted) {
				report(MSG_ERROR,"Corrupted image length in INSERT, header: %d, buffer: %d\n",((HeapObjHeader*)rec)->length,(int)lr);
				return RC_CORRUPTED;
			}
		}
		if (op==HPOP_INSERT) {
			PageIdx xSlot; 
			if (idxs==NULL) xSlot=idx+nObjs; else for (unsigned i=xSlot=0; i<nObjs; i++) if (idxs[i]>=xSlot) xSlot=idxs[i]+1;
			size_t l2=lr; if (xSlot>hp->nSlots) l2+=(xSlot-hp->nSlots)*sizeof(PageOff);
			if (l2>hp->contFree()) {
				hp->compact();
				if (l2>hp->contFree()) {
					report(MSG_ERROR,"Page %08X overflow in INSERT, requested: %d, available: %d\n",hp->hdr.pageID,l2,hp->contFree());
					return RC_PAGEFULL;
				}
			}
			memcpy(frame+hp->freeSpace,rec,lrec);
			if (xSlot>hp->nSlots) memset(&(*hp)[xSlot-1],0,(xSlot-hp->nSlots)*sizeof(PageOff));
			for (unsigned i=0; i<nObjs; i++) {
				PageIdx ii=idxs!=NULL?idxs[i]:idx++;
				if (ii<hp->nSlots && (off=(*hp)[ii])!=0) {
					if ((off&1)==0) {report(MSG_ERROR,"Slot %d not free in INSERT, page %08X\n",ii,hp->hdr.pageID); return RC_CORRUPTED;}
					if (hp->freeSlots==0) {report(MSG_ERROR,"freeSlots corrupted in INSERT, page %08X\n",hp->hdr.pageID); return RC_CORRUPTED;}
					if (((unsigned)hp->freeSlots>>1)==ii) hp->freeSlots=off==0xFFFF?0:off;
					else for (PageIdx j=hp->freeSlots>>1,k;;j=k>>1)
						if ((k=(*hp)[j])==0xFFFF) return RC_CORRUPTED; else if (((unsigned)k>>1)==ii) {(*hp)[j]=off; break;}
				}
				HeapObjHeader *hobj=(HeapObjHeader*)(frame+hp->freeSpace);
				if (hobj->getType()==HO_PIN) {
					HeapPIN *hpin=(HeapPIN*)hobj; HeapV *hprop=hpin->getPropTab();
					for (int i=hpin->nProps; --i>=0; hprop++) {
						if (hprop->type.isCompact()) {
							if (hprop->type==HType::compactRef) hpin->hdr.descr|=HOH_COMPACTREF;
						} else {
							hprop->offset+=hp->freeSpace;
							if (hprop->type.isCompound()) hpin->hdr.descr|=adjustCompound(frame,hprop,hprop->offset,ctx);
						}
					}
#ifdef _DEBUG
					hpin->hdr.descr|=HOH_MULTIPART; 
#endif
				}
				PageOff ll=hobj->getLength(); (*hp)[ii]=hp->freeSpace; hp->freeSpace+=ll;
			}
			if (xSlot>=hp->nSlots) hp->nSlots=xSlot;
		} else if (idx==0 && nObjs==hp->nSlots && (op0==HPOP_INSERT || op==HPOP_PURGE && hp->hdr.pgid==PGID_SSV)) {
			hp->freeSpace=sizeof(HeapPage); hp->scatteredFreeSpace=0; hp->freeSlots=hp->nSlots=0;
		} else for (unsigned i=0; i<nObjs; i++) {
			PageIdx ii=idxs!=NULL?idxs[i]:idx++;
			if ((hdr=hp->getObject(off=hp->getOffset(ii)))==NULL) {
				// report
				return RC_NOTFOUND;
			}
			if (op==HPOP_DELETE) {
				if ((flags&TXMGR_UNDO)!=0) hdr->descr&=~HOH_DELETED; else hdr->descr|=HOH_DELETED;
			} else {
				if ((hdr->descr&HOH_MULTIPART)!=0 || off+hdr->getLength()!=hp->freeSpace) hp->scatteredFreeSpace+=hdr->getLength(); else hp->freeSpace=off;
				if (op0!=HPOP_INSERT && (hdr->descr&HOH_TEMP_ID)==0 && hp->hdr.pgid!=PGID_SSV) (*hp)[ii]=0;
				else if (ii+1==hp->nSlots) hp->nSlots--;
				else if (hp->freeSlots==0) {hp->freeSlots=ii<<1|1; (*hp)[ii]=0xFFFF;}
				else if (ii>((unsigned)hp->freeSlots>>1)) {(*hp)[ii]=hp->freeSlots; hp->freeSlots=ii<<1|1;}
				else for (PageIdx j=hp->freeSlots>>1,k;;j=k>>1)
					if ((k=(*hp)[j])==0xFFFF || ii>((unsigned)k>>1)) {(*hp)[j]=ii<<1|1; (*hp)[ii]=k; break;}
			}
		}
#ifdef _DEBUG
		if (op!=HPOP_DELETE) {hp->compact(true); hp->checkPage(false);}
#endif
		return RC_OK;
	}

	if ((hdr=hp->getObject(off=hp->getOffset(idx)))==0) return RC_NOTFOUND;
	HeapObjType ht=hdr->getType(); const HeapModEdit *hed; const PagePtr *newPtr,*oldPtr;
	ushort l,nl,ol,newl; bool f; long delta; byte *pData;
	switch (op) {
	case HPOP_MIGRATE:
		assert(lrec>=PageAddrSize&&(flags&TXMGR_UNDO)==0);
		hp->scatteredFreeSpace+=hdr->getLength();
		if (hp->contFree()<sizeof(HeapObjHeader)+PageAddrSize) {
			if (hp->totalFree()<sizeof(HeapObjHeader)+PageAddrSize) {
				report(MSG_ERROR,"Page %08X overflow in MIGRATE, requested: %d, available: %d\n",hp->hdr.pageID,sizeof(HeapObjHeader)+PageAddrSize,hp->totalFree());
				hp->scatteredFreeSpace-=hdr->getLength(); return RC_PAGEFULL;
			}
			(*hp)[idx]=0; hp->compact(); assert(hp->contFree()>=sizeof(HeapObjHeader)+PageAddrSize);
		}
		(*hp)[idx]=hp->freeSpace; hdr=(HeapObjHeader*)(frame+hp->freeSpace); hdr->descr=HO_FORWARD; hdr->length=sizeof(HeapObjHeader)+PageAddrSize;
		hp->freeSpace+=sizeof(HeapObjHeader)+PageAddrSize; memcpy(hdr+1,rec+lrec-PageAddrSize,PageAddrSize); return RC_OK;
	case HPOP_SETFLAG:
		op=((HeapSetFlag*)rec)->value; if ((flags&TXMGR_UNDO)!=0) op^=((HeapSetFlag*)rec)->mask;
		hdr->descr=hdr->descr&~((HeapSetFlag*)rec)->mask|op; return RC_OK;
	case HPOP_EDIT:
		assert(lrec>=sizeof(HeapModEdit));
		if (ht!=HO_SSVALUE && ht!=HO_BLOB && ht!=HO_FORWARD) {
			report(MSG_ERROR,"Invalid object type %d in EDIT, slot: %d, page: %08X\n",ht,idx,hp->hdr.pageID);
			return RC_CORRUPTED;
		}
		hed=(const HeapModEdit*)rec; newPtr=&hed->newPtr; oldPtr=&hed->oldPtr;
		if ((flags&TXMGR_UNDO)!=0) {const PagePtr *tmp=newPtr; newPtr=oldPtr; oldPtr=tmp;}
		l=hdr->length-sizeof(HeapObjHeader); nl=newPtr->len; ol=oldPtr->len; delta=nl-ol;
		if (hed->shift+ol>l || -delta>l) {
			report(MSG_ERROR,"Invalid OP_EDIT: shift %d, length %d old length %d, slot: %d, page: %08X\n",
				hed->shift,ol,l,idx,hp->hdr.pageID);
			return RC_CORRUPTED;
		}
		f=off+hdr->getLength()==hp->freeSpace; newl=(ushort)ceil(l+delta+sizeof(HeapObjHeader),HP_ALIGN);
		if (!f && delta>0 && newl<=hp->contFree()) {
			HeapObjHeader *newh=(HeapObjHeader*)(frame+hp->freeSpace); byte *p=(byte*)(newh+1);
			newh->descr=hed->dscr==0?hdr->descr:(flags&TXMGR_UNDO)!=0?hed->dscr>>8&0xFF:hed->dscr&0xFF; 
			if (hed->shift>0) {memcpy(p,hdr+1,hed->shift); p+=hed->shift;}
			if (nl>0) {memcpy(p,rec+newPtr->offset,nl); p+=nl;}
			ushort ltail=l-hed->shift-ol; if (ltail>0) memcpy(p,(byte*)(hdr+1)+hed->shift+ol,ltail);
			(*hp)[idx]=hp->freeSpace; hp->freeSpace+=newl;
			hp->scatteredFreeSpace+=hdr->getLength(); newh->length=(ushort)(l+delta+sizeof(HeapObjHeader));
		} else {
			byte *p=pData=(byte*)(hdr+1); ushort ltail=l-hed->shift-ol;
			if (delta>0 && (!f || ceil((unsigned)delta,HP_ALIGN)>hp->contFree())) {
				if (newl>hp->totalFree()+hdr->getLength()) {
					report(MSG_ERROR,"Page %08X overflow in HOP_EDIT, requested: %d, available: %d\n",
						hp->hdr.pageID,newl-hdr->getLength(),hp->totalFree());
					return RC_PAGEFULL;
				}
				p=(byte*)alloca(l); if (p==NULL) return RC_NORESOURCES; memcpy(p,pData,l);
				ushort dscr=hdr->descr; (*hp)[idx]=0; hp->scatteredFreeSpace+=hdr->getLength();
				hp->compact(); assert(newl<=hp->contFree());
				(*hp)[idx]=hp->freeSpace; hdr=(HeapObjHeader*)(frame+hp->freeSpace);
				hdr->descr=dscr; hdr->length=sizeof(HeapObjHeader)+hed->shift+ltail; pData=(byte*)(hdr+1);
				hp->freeSpace+=hdr->getLength(); if (hed->shift>0) memcpy(pData,p,hed->shift); delta=nl;
			}
			if (ltail!=0 && (p!=pData || delta!=0)) memmove(pData+hed->shift+nl,p+hed->shift+ol,ltail);
			if (nl>0) memcpy(pData+hed->shift,rec+newPtr->offset,nl);
			if ((l=(ushort)ceil(unsigned(hdr->length+delta),HP_ALIGN))!=hdr->getLength()) {
				if (delta>0) {l-=hdr->getLength(); hp->freeSpace+=l;}
				else {l=hdr->getLength()-l; if (f) hp->freeSpace-=l; else hp->scatteredFreeSpace+=l;}
			}
			hdr->length+=ushort(delta); if (hed->dscr!=0) hdr->descr=(flags&TXMGR_UNDO)!=0?hed->dscr>>8&0xFF:hed->dscr&0xFF; 
		}
#ifdef _DEBUG
		hp->checkPage(false);
		hp->compact(true);
#endif
		return RC_OK;
	default: 
		report(MSG_ERROR,"Invalid operation %d, slot: %d, page: %08X\n",op0,idx,hp->hdr.pageID);
		return RC_CORRUPTED;
	case HPOP_PINOP:
		if (lrec<sizeof(HeapPINMod)) {
			report(MSG_ERROR,"Invalid lrec %d for HPOP_PINOP, slot: %d, page: %08X\n",lrec,idx,hp->hdr.pageID);
			return RC_CORRUPTED;
		}
		break;
	}

	if (ht!=HO_PIN) {
		report(MSG_ERROR,"PIN operation %d for a non-PIN object %d, slot: %d, page: %08X\n",op,ht,idx,hp->hdr.pageID);
		return RC_CORRUPTED;
	}
	if ((hdr->descr&HOH_DELETED)!=0) 
		{report(MSG_ERROR,"PIN deleted, slot: %d, page: %08X\n",idx,hp->hdr.pageID); return RC_NOTFOUND;}
	
	HeapPIN *hpin=(HeapPIN*)hdr; const HeapPINMod *hpi=(const HeapPINMod*)rec;
	rec+=sizeof(HeapPINMod)+int(hpi->nops-1)*sizeof(HeapPropMod); RC rc=RC_OK;
	if ((flags&TXMGR_UNDO)==0) hpin->hdr.descr|=hpi->descr;
	for (uint16_t nop=0; nop<hpi->nops; nop++) {
		const HeapPropMod *hpm=&hpi->ops[(flags&TXMGR_UNDO)!=0?hpi->nops-1-nop:nop]; op=hpm->op; ValueType ty;
		PageIdx hidx; HeapV *hins=NULL,*hins2=NULL; uint32_t key; HType *pType; ushort sht,x; HeapDataFmt fmt,newf; bool fNewProp=false;
		const TypedPtr *newPtr=&hpm->newData,*oldPtr=&hpm->oldData; PropertyID propID=hpm->propID,propID2; ElementID eid=hpm->eltId,eid2;
		if ((flags&TXMGR_UNDO)!=0) {
			const static ExprOp undoOP[OP_FIRST_EXPR]={OP_SET,OP_DELETE,OP_DELETE,OP_MOVE,OP_MOVE_BEFORE,OP_ADD,OP_EDIT,OP_RENAME};
			const TypedPtr *tmp=newPtr; newPtr=oldPtr; oldPtr=tmp; op=undoOP[op];
			switch (hpm->op) {
			case OP_RENAME: propID=hpm->eltKey; break;
			case OP_DELETE: if (eid!=STORE_COLLECTION_ID && (eid=hpm->eltKey)==STORE_FIRST_ELEMENT) op=OP_ADD_BEFORE; break;
			case OP_ADD: case OP_ADD_BEFORE: if (hpm->eltKey!=STORE_COLLECTION_ID) eid=hpm->eltKey; break;		// if array ???
			}
		}
		HeapV *hprop=(HeapV*)hpin->findProperty(propID,&hins); if (hins==NULL) hins=hprop; 
		hidx=hpin->nProps==0?0:PageIdx(hins-hpin->getPropTab());
		if (op==OP_ADD || op==OP_ADD_BEFORE) {
			if (hprop==NULL) {hprop=hins; fNewProp=true;}
			else if (eid==STORE_COLLECTION_ID) {
				report(MSG_ERROR,"Property %08X already exists, slot: %d, page: %08X\n",propID,idx,hp->hdr.pageID); 
				PINOP_ERROR(RC_ALREADYEXISTS);
			}
		} else if (hprop==NULL) {
			report(MSG_ERROR,"Property %08X not found, op: %d, slot: %d, page: %08X\n",propID,op,idx,hp->hdr.pageID); 
			PINOP_ERROR(RC_NOTFOUND);
		} else if (op!=OP_RENAME && op!=OP_MOVE && op!=OP_MOVE_BEFORE && op!=OP_EDIT && eid==STORE_COLLECTION_ID 
																	&& hprop->type.getType()!=oldPtr->type.getType()) {
			if (hprop->type.isCollection() && hprop->type.getFormat()!=HDF_LONG && ((HeapVV*)(frame+hprop->offset))->cnt==1) {
				if (hprop->type.getFormat()!=HDF_SHORT) eid=((HeapVV*)(frame+hprop->offset))->start->getID();
			} else {
				report(MSG_ERROR,"Property %08X value type mismatch, expected: %04X, actual: %04X, slot: %d, page: %08X\n",
												propID,oldPtr->type.getType(),hprop->type.getType(),idx,hp->hdr.pageID);
				PINOP_ERROR(RC_CORRUPTED);
			}
		}
		HeapVV *coll=NULL; HeapV *elt=NULL; HeapKey *hk;
		if (eid!=STORE_COLLECTION_ID && !fNewProp) {
			if (hprop->type.isCollection()) {
				if (hprop->type.getFormat()==HDF_LONG) {
					report(MSG_ERROR,"Attempt to modify big collection %08X, slot: %d, page: %08X\n",hprop->getID(),idx,hp->hdr.pageID);
					PINOP_ERROR(RC_CORRUPTED);
				}
				coll=(HeapVV*)(frame+hprop->offset);
				if (op==OP_DELETE && oldPtr->type.isCollection()) eid=((const HeapVV*)(rec+oldPtr->ptr.offset))->start->getID();
				elt=(HeapV*)coll->findElt(eid); if (elt==NULL) PINOP_ERROR(RC_NOTFOUND);
				if (op!=OP_ADD && op!=OP_DELETE && op!=OP_ADD_BEFORE && op!=OP_MOVE && op!=OP_MOVE_BEFORE && elt->type.getType()!=oldPtr->type.getType()) {
					report(MSG_ERROR,"Collection property %08X element %d type mismatch, expected: %04X, actual: %04X, slot: %d, page: %08X\n",
						propID,eid,oldPtr->type.getType(),elt->type.getType(),idx,hp->hdr.pageID);
					PINOP_ERROR(RC_CORRUPTED);
				}
			} else if (hpm->op==OP_MOVE || hpm->op==OP_MOVE_BEFORE) {
				report(MSG_ERROR,"Attempt to move a non-collection property %08X, slot: %d, page: %08X\n",propID,idx,hp->hdr.pageID);
				PINOP_ERROR(RC_NOTFOUND);
			} else if (hpm->op!=OP_ADD && hpm->op!=OP_ADD_BEFORE || eid!=STORE_LAST_ELEMENT && eid!=STORE_FIRST_ELEMENT) {
				key=ctx->getPrefix(); PID id;
				if (hpin->hasRemoteID() && (hprop->type.flags&META_PROP_LOCAL)==0 && hpin->getAddr(id)) key=getPrefix(id);
				if (op==OP_DELETE && (eid==key||(flags&TXMGR_UNDO)!=0)) eid=STORE_COLLECTION_ID;
				else {
					report(MSG_ERROR,"Attempt to modify a collection element from non-collection property %08X, slot: %d, page: %08X\n",
							propID,idx,hp->hdr.pageID);
					PINOP_ERROR(RC_NOTFOUND);
				}
			}
		}
		const byte *data=rec+newPtr->ptr.offset; bool fColl;
		ushort ol=(ushort)ceil((unsigned)oldPtr->ptr.len,HP_ALIGN),nl=(ushort)ceil((unsigned)newPtr->ptr.len,HP_ALIGN),extra,eidx=0;
		switch (op) {
		default: return RC_CORRUPTED;
		case OP_SET:
			if (newPtr->type==HType::compactRef) hpin->hdr.descr|=HOH_COMPACTREF; fColl=false;
			if (coll==NULL) {
				if (oldPtr->type==HType::compactRef) ol=refLength[hprop->type.getFormat()];
				if (hpm->eltKey!=STORE_COLLECTION_ID && !newPtr->type.isCollection()) {nl+=sizeof(HeapVV)+sizeof(HeapKey); fColl=true;}
				if (hprop->type.isCollection() && hprop->type.getFormat()==HDF_SHORT && !oldPtr->type.isCollection()) {
					ol+=sizeof(HeapVV)+sizeof(HeapKey); coll=(HeapVV*)(frame+hprop->offset);
#if _DEBUG
					assert(eid==STORE_COLLECTION_ID && oldPtr->type==coll->start->type);
					if (oldPtr->type.isCompact()) assert(coll->start[0].offset==oldPtr->ptr.offset);
					else assert(memcmp(frame+coll->start[0].offset,rec+oldPtr->ptr.offset,oldPtr->ptr.len)==0);
				} else {
					assert(eid==STORE_COLLECTION_ID && oldPtr->type==hprop->type);
					if (oldPtr->type.isCompact()) assert(hprop->offset==oldPtr->ptr.offset);
					else if (!hprop->type.isCompound()||hprop->type.getFormat()==HDF_LONG) 
						assert(memcmp(frame+hprop->offset,rec+oldPtr->ptr.offset,oldPtr->ptr.len)==0);
					else {
						// check compound
					}
#endif
				}
				elt=hprop;
			} else {
				if (oldPtr->type==HType::compactRef) ol=refLength[elt->type.getFormat()];
#if _DEBUG
				assert(elt!=NULL && oldPtr->type==elt->type);
				if (oldPtr->type.isCompact()) assert(elt->offset==oldPtr->ptr.offset);
				else assert(memcmp(frame+elt->offset,rec+oldPtr->ptr.offset,oldPtr->ptr.len)==0);
#endif
			}
			if (coll==NULL || !newPtr->type.isCollection()) {
				delta=nl-ol; f=false;
				if (!elt->type.isCollection()) {
					if (!elt->type.isCompact() && elt->offset+ol==hp->freeSpace && delta<=(long)hp->contFree()) {hp->freeSpace+=ushort(delta); f=true;}
					else if (delta<=0) {if (delta!=0) {hp->scatteredFreeSpace-=ushort(delta); hpin->hdr.descr|=HOH_MULTIPART;} f=true;}
				}
				if (!f && nl<=hp->contFree()) {hp->scatteredFreeSpace+=ol; elt->offset=hp->freeSpace; hp->freeSpace+=nl; hpin->hdr.descr|=HOH_MULTIPART; f=true;}
				if (f) {
					if (fColl) {
						coll=(HeapVV*)(frame+hprop->offset); coll->cnt=1; coll->fUnord=0; assert(elt==hprop);
						key=ctx->getPrefix(); if ((hpm->eltKey&CPREFIX_MASK)==key) key=hpm->eltKey+1;
						hk=(HeapKey*)(coll+1); hk->setKey(key); 	coll->start->type=newPtr->type;
						coll->start->setID(hpm->eltKey!=STORE_COLLECTION_ID?hpm->eltKey:hpm->eltId);
						assert(coll->start->getID()!=STORE_COLLECTION_ID);
						if (newPtr->type.isCompact()) coll->start->offset=newPtr->ptr.offset;
						else {memcpy(hk+1,data,newPtr->ptr.len); coll->start->offset=hprop->offset+sizeof(HeapVV)+sizeof(HeapKey);}
						hprop->type.setType(VT_ARRAY,HDF_SHORT);
					} else {
						elt->type=newPtr->type;
						if (newPtr->type.isCompact()) elt->offset=newPtr->ptr.offset;
						else {
							memcpy(frame+elt->offset,data,newPtr->ptr.len);
							if (newPtr->type.isCollection()) hpin->hdr.descr|=adjustCompound(frame,elt,elt->offset,ctx);
						}
					}
					hpin->hdr.length+=ushort(delta); break;
				}
			}
			if (coll!=NULL && --coll->cnt!=0) memmove(elt,elt+1,((byte*)&coll->start[coll->cnt]-(byte*)elt)+sizeof(HeapKey));
			else {
				fNewProp=true;
				if (hidx<--hpin->nProps||hpin->fmtExtra!=HDF_COMPACT) memmove(hprop,hprop+1,(hpin->nProps-hidx)*sizeof(HeapV)+refLength[hpin->fmtExtra]);
				if (coll!=NULL) {coll=NULL; elt=NULL; if (eid!=STORE_COLLECTION_ID) ol+=sizeof(HeapVV)+sizeof(HeapKey);}
			}
			hp->scatteredFreeSpace+=ol+=sizeof(HeapV); hpin->hdr.length-=ol; nl=(ushort)ceil((unsigned)newPtr->ptr.len,HP_ALIGN);
#ifdef _DEBUG
			hpin->hdr.descr|=HOH_MULTIPART; hp->compact(true);
#endif
		case OP_ADD:
		case OP_ADD_BEFORE:
			extra=sizeof(HeapV); if (newPtr->type==HType::compactRef) hpin->hdr.descr|=HOH_COMPACTREF;
			if (fNewProp) {ol=hpin->headerLength(); if (eid!=STORE_COLLECTION_ID) extra+=sizeof(HeapVV)+sizeof(HeapKey);}
			else if (coll==NULL) {ol=0; if (!newPtr->type.isCollection()) extra=sizeof(HeapVV)+sizeof(HeapV)+sizeof(HeapKey);}
			else {
				ol=coll->length(hprop->type.isCollection()); if (op==OP_ADD) elt++; eidx=(ushort)(elt-coll->start);
				if (newPtr->type.isCollection()) {nl-=sizeof(HeapVV)-sizeof(HeapV)+sizeof(HeapKey); extra=0;}
			}
			if (size_t(ol+nl+extra)>hp->contFree()) {
				if (size_t(nl+extra)>hp->totalFree()) {
					report(MSG_ERROR,"Page %08X overflow in %s, requested: %d, available: %d\n",hp->hdr.pageID,fNewProp?"ADDPROP":"ADDCELT",nl+extra,hp->totalFree());
					PINOP_ERROR(RC_PAGEFULL);
				}
				hp->compact(false,idx,fNewProp||coll==NULL?ushort(~0u):hidx);
				off=hp->getOffset(idx); assert(off!=0 && size_t(nl+extra)<=hp->contFree()); 
				hpin=(HeapPIN*)hp->getObject(off); hprop=&hpin->getPropTab()[hidx];
			} else if (ol!=0 && (fNewProp?off:hprop->offset)+ol!=hp->freeSpace) {
				memmove(frame+hp->freeSpace,frame+(fNewProp?off:hprop->offset),ol); 
				if (fNewProp) hprop=&(hpin=(HeapPIN*)(frame+((*hp)[idx]=off=hp->freeSpace)))->getPropTab()[hidx]; else hprop->offset=hp->freeSpace;
				hp->freeSpace+=ol; hp->scatteredFreeSpace+=ol;
			}
			if (fNewProp) {
				if (hpin->fmtExtra!=HDF_COMPACT||hidx<hpin->nProps) memmove(hprop+1,hprop,(hpin->nProps-hidx)*sizeof(HeapV)+refLength[hpin->fmtExtra]);
				hprop->setID(propID); hprop->type=newPtr->type; hprop->offset=hp->freeSpace+=sizeof(HeapV); hpin->nProps++;
				if (eid!=STORE_COLLECTION_ID) {
					coll=(HeapVV*)(frame+hprop->offset); coll->cnt=1; coll->fUnord=0; hp->freeSpace+=extra-sizeof(HeapV); 
					key=ctx->getPrefix(); if ((hpm->eltKey&CPREFIX_MASK)==key) key=hpm->eltKey+1;
					hk=(HeapKey*)(coll+1); hk->setKey(key);
					coll->start->type=newPtr->type; coll->start->setID(eid);
					if (newPtr->type.isCompact()) coll->start->offset=newPtr->ptr.offset;
					else {memcpy(hk+1,data,newPtr->ptr.len); coll->start->offset=hprop->offset+sizeof(HeapVV)+sizeof(HeapKey);}
					hprop->type.setType(VT_ARRAY,HDF_SHORT); hprop->type.flags=newPtr->type.flags;
				} else if (newPtr->type.isCompact()) {
					hprop->offset=newPtr->ptr.offset;
				} else {
					assert(nl!=0); memcpy(frame+hprop->offset,data,nl);
					if (hprop->type.isCollection()) hpin->hdr.descr|=adjustCompound(frame,hprop,hprop->offset,ctx);
				}
			} else {
				const ushort nElts=newPtr->type.isCollection()?((const HeapVV*)data)->cnt:1;
				if (coll!=0) {
					assert(hprop->offset+ol==hp->freeSpace);
					coll=(HeapVV*)(frame+hprop->offset); elt=coll->start+eidx;
					memmove(elt+nElts,elt,(coll->cnt-eidx)*sizeof(HeapV)+sizeof(HeapKey));
					coll->cnt+=nElts; hk=coll->getHKey(); key=hk->getKey(); 
				} else {
					coll=(HeapVV*)(frame+hp->freeSpace); coll->cnt=nElts+1; coll->fUnord=0; assert(!hprop->type.isCollection());
					elt=&coll->start[op==OP_ADD?0:nElts]; elt->type=hprop->type; elt->offset=hprop->offset; key=ctx->getPrefix(); 
					if (hpin->hasRemoteID()) {
						PID id; unsigned key2=(hprop->type.flags&META_PROP_LOCAL)==0&&hpin->getAddr(id)?getPrefix(id):key;
						elt->setID(hpm->eltKey==key2?++key2:key2);	if (((key^key2)&CPREFIX_MASK)==0) key=key2+1;	// collision with array elts (if array)?
					} else {
						elt->setID(hpm->eltKey==key?++key:key); ++key;												// collision with array elts (if array)?
					}
					hk=coll->getHKey(); hk->setKey(key); hprop->offset=hp->freeSpace; elt=&coll->start[op==OP_ADD?1:0];
				}
				if (newPtr->type.isCollection()) {
					const HeapVV *const ncoll=(const HeapVV *)data; uint32_t keyNew=ncoll->getHKey()->getKey();
					if (key<keyNew && ((key^keyNew)&CPREFIX_MASK)==0) hk->setKey(keyNew);
					const ushort ssht=ncoll->length(true),dsht=coll->length(hprop->type.isCollection()); coll->fUnord|=ncoll->fUnord;
					memcpy(elt,ncoll->start,nElts*sizeof(HeapV)); memcpy((byte*)coll+dsht,data+ssht,newPtr->ptr.len-ssht);
					const ushort sht=hprop->offset+dsht-ssht;
					for (unsigned i=0; i<nElts; ++i,++elt) {
						assert(elt->getID()!=STORE_COLLECTION_ID);
						if (!elt->type.isCompact()) elt->offset+=sht; else if (elt->type==HType::compactRef) hpin->hdr.descr|=HOH_COMPACTREF;
					}
					// coll->fUnord
				} else {
					if (newPtr->type.isCompact()) elt->offset=newPtr->ptr.offset;
					else {memcpy(frame+hp->freeSpace+extra,data,newPtr->ptr.len); elt->offset=hp->freeSpace+extra;}
					eid2=(flags&TXMGR_UNDO)!=0||hpm->eltKey==STORE_COLLECTION_ID?hpm->eltId:hpm->eltKey;
					elt->setID(eid2); elt->type=newPtr->type; assert(eid2!=STORE_COLLECTION_ID);
					if (key<=eid2 && ((key^eid2)&CPREFIX_MASK)==0) hk->setKey(eid2+1);
				}
				hprop->type.setType(VT_ARRAY,HDF_NORMAL); hp->freeSpace+=extra; if (op==OP_ADD_BEFORE) coll->fUnord=1;
			}
			hpin->hdr.descr|=HOH_MULTIPART; hp->freeSpace+=nl; hpin->hdr.length+=nl+extra;
			break;
		case OP_DELETE:
			if (oldPtr->type==HType::compactRef) ol=refLength[elt!=NULL?elt->type.getFormat():hprop->type.getFormat()];
#ifdef _DEBUG
			if (oldPtr->type.isCollection()) {
				if (!hprop->type.isCollection() || ((HeapVV*)(frame+hprop->offset))->cnt<((HeapVV*)(rec+oldPtr->ptr.offset))->cnt) return RC_CORRUPTED;
				// check all elts
			} else if (elt!=NULL && oldPtr->type.getType()!=elt->type.getType()) return RC_CORRUPTED;
			// else compare value like in OP_SET
#endif
			if (coll!=NULL) {
				fColl=oldPtr->type.isCollection(); assert(eid!=STORE_COLLECTION_ID && elt!=NULL && nl==0);
				const ushort nElts=fColl?((HeapVV*)(rec+oldPtr->ptr.offset))->cnt:1;
				if (nElts<coll->cnt) {
					for (ushort i=0,j=1;;) {
						ushort eidx=ushort(elt-coll->start);
						while (i+j<nElts && eidx+j<coll->cnt && ((HeapVV*)(rec+oldPtr->ptr.offset))->start[i+j].getID()==elt[j].getID()) j++;
						memmove(elt,elt+j,(coll->cnt-eidx-j)*sizeof(HeapV)+sizeof(HeapKey));
						coll->cnt-=j; if ((i+=j)>=nElts) break; assert(fColl); const HeapV *elt;
						if ((elt=coll->findElt(eid=((HeapVV*)(rec+oldPtr->ptr.offset))->start[i].getID()))==NULL) {
							report(MSG_ERROR,"Collection property %08X element %d not found, slot: %d, page: %08X\n",propID,eid,idx,hp->hdr.pageID);
							PINOP_ERROR(RC_NOTFOUND);
						}
					}
					if (!fColl) ol+=sizeof(HeapV); else ol-=sizeof(HeapVV)-sizeof(HeapV)+sizeof(HeapKey);
					hpin->hdr.length-=ol; hpin->hdr.descr|=HOH_MULTIPART; hp->scatteredFreeSpace+=ol;
					if (coll->cnt==1) {
						if (newPtr->ptr.offset==ushort(~0u)) hprop->type.setType(VT_ARRAY,HDF_SHORT);
						else if (newPtr->ptr.offset==ushort(~1u)) {
							hprop->type.type=coll->start->type.type; hprop->offset=coll->start->offset;
							hp->scatteredFreeSpace+=sizeof(HeapVV)+sizeof(HeapKey); hpin->hdr.length-=sizeof(HeapVV)+sizeof(HeapKey);
						}
					}
					break;
				}
				if (!fColl) ol+=sizeof(HeapVV)+sizeof(HeapKey);
			}
			hpin->nProps--; assert(nl==0);
			if (hprop->offset+ol!=hp->freeSpace || (hpin->hdr.descr&HOH_MULTIPART)!=0 && hprop->type.isCollection()) {
				hp->scatteredFreeSpace+=ol+sizeof(HeapV); hpin->hdr.descr|=HOH_MULTIPART;
			} else {
				hp->freeSpace-=ol;
				if ((byte*)(hprop+1)-frame==hp->freeSpace) hp->freeSpace-=sizeof(HeapV);
				else {hp->scatteredFreeSpace+=sizeof(HeapV); hpin->hdr.descr|=HOH_MULTIPART;}
			}
			if (hpin->fmtExtra!=HDF_COMPACT||hidx<hpin->nProps) memmove(hprop,hprop+1,(hpin->nProps-hidx)*sizeof(HeapV)+refLength[hpin->fmtExtra]);
			hpin->hdr.length-=ol+sizeof(HeapV); break;
		case OP_RENAME:
			propID2=(flags&TXMGR_UNDO)!=0?hpm->propID:hpm->eltKey;
			if (hpin->findProperty(propID2,&hins2)!=NULL) {
				report(MSG_ERROR,"Property %08X already exists in OP_RENAME of %08X, slot: %d, page: %08X\n",propID2,propID,idx,hp->hdr.pageID);
				PINOP_ERROR(RC_ALREADYEXISTS);
			}
			if (hins2!=hins && hins2!=hins+1) {
				HeapV save=*hprop; save.setID(propID2);
				if (hins2<hins) memmove(hins2+1,hins2,(byte*)hins-(byte*)hins2); else memmove(hins,hins+1,(byte*)--hins2-(byte*)hins);
				*hins2=save;
			} else hprop->setID(propID2); 
			break;
		case OP_EDIT:
			if (!isString(hprop->type.getType()) && (elt==NULL || !isString(elt->type.getType()))) {
				report(MSG_ERROR,"OP_EDIT for a non-string property %d, slot: %d, page: %08X\n",hprop->type.getType(),idx,hp->hdr.pageID);
				PINOP_ERROR(RC_TYPE);
			}
			delta=(nl=newPtr->ptr.len)-(ol=oldPtr->ptr.len);
			if (coll==NULL) {pType=&hprop->type; pData=frame+hprop->offset;} else {pType=&elt->type; pData=frame+elt->offset;}
			fmt=pType->getFormat(); l=fmt==HDF_COMPACT?0:fmt==HDF_SHORT?pData[0]:pData[0]<<8|pData[1];
			if (hpm->shift+oldPtr->ptr.len>l || -delta>l) {
				report(MSG_ERROR,"Invalid OP_EDIT: shift %d, length %d old length %d, slot: %d, page: %08X\n",
					hpm->shift,ol,l,idx,hp->hdr.pageID);
				PINOP_ERROR(RC_CORRUPTED);
			}
			sht=fmt==HDF_NORMAL?2:fmt==HDF_SHORT?1:0;
			newl=ushort(l+delta); newf=newl==0?HDF_COMPACT:newl<=0xFF?HDF_SHORT:HDF_NORMAL;
			x=newf==HDF_COMPACT?0:newf==HDF_SHORT?1:2;
			f=pData+ceil(l+sht,HP_ALIGN)==frame+hp->freeSpace; ty=pType->getType();
			if (!f && delta>0 && coll==NULL && ceil(x+newl,HP_ALIGN)<=hp->contFree()) {
				byte *p=frame+hp->freeSpace; assert(newf!=HDF_COMPACT);
				if (newf==HDF_NORMAL) *p++=byte(newl>>8); *p++=byte(newl);
				if (hpm->shift>0) {memmove(p,pData+sht,hpm->shift); p+=hpm->shift;}
				if (nl>0) {memcpy(p,data,nl); p+=nl;}
				ushort ltail=l-hpm->shift-ol; if (ltail>0) memmove(p,pData+sht+hpm->shift+ol,ltail);
				hprop->offset=hp->freeSpace; newl=(ushort)ceil(newl+x,HP_ALIGN); l=(ushort)ceil(l+sht,HP_ALIGN);
				hp->freeSpace+=newl; hp->scatteredFreeSpace+=l; hpin->hdr.descr|=HOH_MULTIPART; hpin->hdr.length+=newl-l;
			} else {
				byte *p=pData+sht; ushort len=l;
				if (delta>0 && (!f || pData+ceil(x+newl,HP_ALIGN)>frame+hp->freeSpace+hp->contFree())) {
					p=(byte*)alloca(len); if (p==NULL) PINOP_ERROR(RC_NORESOURCES);
					memcpy(p,pData+sht,len); pType->setType(ty,HDF_COMPACT);
					len=(ushort)ceil(len+sht,HP_ALIGN); hpin->hdr.length-=len; hp->scatteredFreeSpace+=len; hpin->hdr.descr|=HOH_MULTIPART;
					hp->compact(false,idx); off=hp->getOffset(idx); assert(off!=0);
					if (ceil(x+newl+(coll!=NULL?sizeof(HeapPageMgr::HeapV):0),HP_ALIGN)>hp->contFree()) {
						report(MSG_ERROR,"Page %08X overflow in OP_EDIT, requested: %d, available: %d\n",
							hp->hdr.pageID,ceil(newl+x+(coll!=NULL?sizeof(HeapPageMgr::HeapV):0),HP_ALIGN),hp->contFree());
						PINOP_ERROR(RC_PAGEFULL);	// check before compact?
					}
					hprop=&(hpin=(HeapPIN*)hp->getObject(off))->getPropTab()[hidx]; f=true;
					if (coll==NULL) {hprop->offset=hp->freeSpace; pData=frame+hp->freeSpace; pType=&hprop->type;}
					else {
						coll=(HeapVV*)(frame+hprop->offset); elt=(HeapV*)coll->findElt(eid); assert(elt!=NULL);
						elt->offset=hp->freeSpace; pData=frame+hp->freeSpace; pType=&elt->type; elt->type.flags=0;
					}
					if (hpm->shift>0) memcpy(pData+x,p,hpm->shift); len=sht=0;
				} else if (x>sht) {memmove(pData+x,p,l); p=pData+x;}
				else if (x<sht && hpm->shift>0 && newf!=HDF_COMPACT) memmove(pData+x,p,hpm->shift);
				if (newf!=HDF_COMPACT) {
					if (newf==HDF_NORMAL) *pData++=byte(newl>>8); *pData++=byte(newl);
					ushort ltail=l-hpm->shift-ol;
					if (ltail!=0&&(p!=pData||nl!=ol)) memmove(pData+hpm->shift+nl,p+hpm->shift+ol,ltail);
					if (nl>0) memcpy(pData+hpm->shift,data,nl);
				}
				delta=long(ceil(x+newl,HP_ALIGN)-ceil(sht+len,HP_ALIGN));
				if (delta>0) {
					assert(delta<=(long)hp->contFree() && f); 
					hp->freeSpace+=ushort(delta); hpin->hdr.length+=ushort(delta);
				} else if (delta<0) {
					len=(ushort)(-delta);
					if (f) hp->freeSpace-=len; else {hp->scatteredFreeSpace+=len; hpin->hdr.descr|=HOH_MULTIPART;}
					hpin->hdr.length-=len;
				}	
			}
			pType->setType(ty,newf); break;
		case OP_MOVE:
		case OP_MOVE_BEFORE:
			eid2=(flags&TXMGR_UNDO)!=0?*(ElementID*)&hpm->newData:hpm->eltKey;
			assert(eid!=STORE_COLLECTION_ID && eid2!=STORE_COLLECTION_ID && coll!=NULL);
			if (eid2!=eid) {
				HeapV *elt2=(HeapV*)coll->findElt(eid2); if (elt2==NULL) PINOP_ERROR(RC_NOTFOUND);
				if (elt!=(op==OP_MOVE?++elt2:elt2-1)) {
					HeapV save=*elt;
					if (elt2<elt) memmove(elt2+1,elt2,(byte*)elt-(byte*)elt2); else memmove(elt,elt+1,(byte*)--elt2-(byte*)elt);
					*elt2=save; coll->fUnord=1;
				}
			}
			break;
		}
#ifdef _DEBUG
		hp->checkPage(false);
		hp->compact(true);
#endif
	}
	return rc;
}

unsigned HeapPageMgr::getPrefix(const PID& id)
{
	assert(id.pid!=STORE_INVALID_PID&&id.ident!=STORE_INVALID_IDENTITY);
	return StoreCtx::genPrefix(ushort(id.pid>>48));
}

const HeapPageMgr::HeapTypeInfo HeapPageMgr::typeInfo[VT_ALL] =
{
	{0,					0},						//	VT_ERROR
	{sizeof(int32_t),	sizeof(int32_t)-1},		//	VT_INT
	{sizeof(uint32_t),	sizeof(uint32_t)-1},	//	VT_UINT
	{sizeof(int64_t),	sizeof(int64_t)-1},		//	VT_INT64
	{sizeof(uint64_t),	sizeof(uint64_t)-1},	//	VT_UINT64
	{sizeof(float),		sizeof(float)-1},		//	VT_FLOAT
	{sizeof(double),	sizeof(double)-1},		//	VT_DOUBLE
	{0,					0},						//	VT_BOOL
	{sizeof(uint64_t),	sizeof(uint64_t)-1},	//	VT_DATETIME
	{sizeof(int64_t),	sizeof(int64_t)-1},		//	VT_INTERVAL
	{sizeof(uint32_t),	sizeof(uint32_t)-1},	//	VT_URIID
	{sizeof(uint32_t),	sizeof(uint32_t)-1},	//	VT_IDENTITY
	{sizeof(uint32_t)*2,sizeof(uint32_t)*2},	//	VT_ENUM
	{0,					0},						//	VT_STRING
	{0,					0},						//	VT_BSTR
	{0,					0}, 					//	VT_URL
	{0,					sizeof(uint64_t)-1},	//	VT_REF
	{0,					sizeof(uint64_t)-1},	//	VT_REFID
	{0,					sizeof(uint64_t)-1},	//	VT_REFPROP
	{0,					sizeof(uint64_t)-1},	//	VT_REFIDPROP
	{0,					sizeof(uint64_t)-1},	//	VT_REFELT
	{0,					sizeof(uint64_t)-1},	//	VT_REFIDELT
	{0,					0},						//	VT_EXPR
	{0,					0},						//	VT_STMT
	{0,					0},						//	VT_ARRAY
	{0,					0},						//	VT_COLLECTION
	{0,					0},						//	VT_STRUCT
	{0,					0},						//	VT_MAP
	{0,					0},						//	VT_RANGE
	{sizeof(HRefSSV),	sizeof(uint32_t)-1},	//	VT_STREAM
	{0,					0},						//	VT_CURRENT
	{sizeof(RefV),		sizeof(RefV)},			//	VT_VARREF
	{0,					0},						//	VT_EXPRTREE
};

const ushort HeapPageMgr::refLength[4] = {0,PageAddrSize,sizeof(OID),sizeof(OID)+sizeof(IdentityID)};

ushort HeapPageMgr::dataLength(HType vt,const byte *pData,const byte *frame,unsigned *idxMask)
{
	HeapDataFmt fmt=vt.getFormat(); if (fmt==HDF_COMPACT) return 0;
	ValueType ty=vt.getType(); uint32_t l; ushort len; const byte *p;
	switch (ty) {
	default: break;
	case VT_STRING:
		if (idxMask!=NULL) *idxMask|=IX_OFT;
	case VT_BSTR: case VT_URL:
		return fmt==HDF_SHORT?pData[0]+1:(pData[0]<<8|pData[1])+2;
	case VT_FLOAT:
		return fmt==HDF_LONG?sizeof(float)+sizeof(uint16_t):sizeof(float);
	case VT_DOUBLE:
		return fmt==HDF_LONG?sizeof(double)+sizeof(uint16_t):sizeof(double);
	case VT_REFID:
		return refLength[fmt];
	case VT_REFIDPROP:
		return refLength[fmt]+sizeof(PropertyID);
	case VT_REFIDELT:
		return refLength[fmt]+sizeof(PropertyID)+sizeof(ElementID);
	case VT_EXPR:
		l=__una_get(*(uint32_t*)pData); assert(ushort(l)==l); return ushort(l);
	case VT_STMT:
		p=pData; afy_dec16(p,len); return len+ushort(p-pData);
	case VT_STREAM:
		if (fmt==HDF_LONG) {
			ty=((HRefSSV*)pData)->type.getType();
			if (idxMask!=NULL && ty==VT_STRING) *idxMask|=IX_OFT;
			return sizeof(HLOB);
		}
		if (idxMask!=NULL && ((HRefSSV*)pData)->type.isString()) *idxMask|=IX_OFT;
		return sizeof(HRefSSV);
	case VT_ARRAY:
	case VT_STRUCT:
	case VT_MAP:
		if (fmt==HDF_LONG) {
			len=ty==VT_MAP?0:ty==VT_STRUCT?sizeof(HRefSSV):collDescrSize((HeapExtCollection*)pData); if (idxMask!=NULL) *idxMask|=IX_OFT;	// VT_MAP?
		} else {
			const HeapVV *comp=(const HeapVV*)pData; len=0; assert(frame!=NULL);
			for (l=comp->cnt; l!=0; ) {
				const HeapV *elt=&comp->start[--l];
				if (!elt->type.isCompact()) len+=ushort(ceil(dataLength(elt->type,frame+elt->offset,frame,idxMask),HP_ALIGN)); 
			}
			len+=comp->length(ty==VT_ARRAY);
		}
		return len;
	}
	return ty<VT_ALL?typeInfo[ty].length:0;
}

ushort HeapPageMgr::moveCompound(HType vt,byte *dest,PageOff doff,const byte *src,PageOff soff)
{
	ushort ldata; const bool fA=vt.isCollection(); assert(vt.isCompound());
	if (vt.getFormat()==HDF_LONG)
		memcpy(dest+doff,src+soff,ldata=fA?collDescrSize((const HeapExtCollection*)(src+soff)):sizeof(HRefSSV));	// VT_MAP?
	else {
		const HeapVV *coll=(const HeapVV*)(src+soff); memcpy(dest+doff,src+soff,ldata=coll->length(fA));
		for (unsigned i=0,j=coll->cnt; i<j; i++) {
			const HeapV &se=coll->start[i];
			if (!se.type.isCompact()) {
				((HeapVV*)(dest+doff))->start[i].offset=doff+ldata;
				ldata+=se.type.isCompound()?moveCompound(se.type,dest,doff+ldata,src,se.offset) :
						(ushort)ceil(moveData(dest+doff+ldata,src+se.offset,se.type),HP_ALIGN);
			}
		}
	}
	return ldata;
}

ushort HeapPageMgr::adjustCompound(byte *frame,HeapV *hprop,ushort sht,StoreCtx *ctx)
{
	ushort flag=0; assert(hprop->type.isCompound());
	if (hprop->type.getFormat()!=HDF_LONG) {
		const unsigned prefix=ctx!=NULL?ctx->getPrefix():~0u; const bool fA=hprop->type.isCollection();
		HeapVV *comp=(HeapVV*)(frame+hprop->offset); HeapV *elt=comp->start; 
		HeapKey *hk=fA?comp->getHKey():NULL; uint32_t key=fA?hk->getKey():0;
		for (int i=comp->cnt; --i>=0; ++elt) {
			if (elt->type==HType::compactRef) flag=HOH_COMPACTREF;
			else if (!elt->type.isCompact()) {
				elt->offset+=sht; if (elt->type.isCompound()) flag|=adjustCompound(frame,elt,elt->offset,ctx);
			}
			if (fA) {const uint32_t eid=elt->getID(); if ((eid&CPREFIX_MASK)==prefix && eid>=key) key=eid+1;}
		}
		if (hk!=NULL) hk->setKey(key);
	}
	return flag;
}

void HeapPageMgr::initPage(byte *frame,size_t len,PageID pid)
{
	TxPage::initPage(frame,len,pid);
	HeapPage *hp = (HeapPage*)frame;
	hp->nSlots				= 0;
	hp->freeSlots			= 0;
	hp->scatteredFreeSpace	= 0;
	hp->freeSpace			= sizeof(HeapPage);
}

bool PINPageMgr::afterIO(PBlock *pb,size_t lPage,bool fLoad)
{
	if (!TxPage::afterIO(pb,lPage,fLoad) || !((HeapPage*)pb->getPageBuf())->checkPage(false)) return false;
	if (fLoad) pb->setVBlock(ctx->lockMgr->getVBlock(pb->getPageID()));
	return true;
}

bool HeapPageMgr::beforeFlush(byte *frame,size_t len,PageID pid)
{
	return ((HeapPage*)frame)->checkPage(true)?TxPage::beforeFlush(frame,len,pid):false;
}

PGID PINPageMgr::getPGID() const
{
	return PGID_HEAP;
}

PGID SSVPageMgr::getPGID() const
{
	return PGID_SSV;
}

//----------------------------------------------------------------------------------------------

bool HeapPageMgr::HeapPage::checkPage(bool fWrite) const
{
	size_t maxFree=contentSize(hdr.length()); const char *what;
	if (scatteredFreeSpace>maxFree-sizeof(PageOff))
		what="invalid scatteredFreeSpace";
	else if (nSlots*sizeof(PageOff)>=maxFree)
		what="invalid nSlots";
	else if (scatteredFreeSpace>freeSpace-sizeof(HeapPage)) 
		what="scatteredFreeSpace>=freeSpace";
	else {
		size_t allocated=0;
		for (PageIdx idx=0; idx<nSlots; idx++) {
			HeapObjHeader *hobj=getObject(getOffset(idx));
			if (hobj!=NULL) allocated+=hobj->getLength();
		}
//???	if (allocated!=maxFree - freeSpaceLength - scatteredFreeSpace - nSlots*sizeof(PageOff))
//			what = "invalid total allocated length";
//		else
			return true;
	}
	report(MSG_ERROR,"HeapPage %08X corrupt %s: %s\n",hdr.pageID,fWrite?"before write":"after read",what);
	return false;
}

void HeapPageMgr::HeapPage::compact(bool fCheckOnly,PageIdx modified,ushort propIdx)
{
	const size_t lPage=hdr.length(); HeapPage *page=(HeapPage*)alloca(lPage); if (page==NULL) return;
	memcpy(page,&hdr,sizeof(hdr)); page->nSlots=nSlots; page->freeSlots=freeSlots;
	page->scatteredFreeSpace=0; page->freeSpace=sizeof(HeapPage);
	const PageOff *from=(PageOff*)((byte*)this+lPage-FOOTERSIZE);
	PageOff *to=(PageOff*)((byte*)page+lPage-FOOTERSIZE); const HeapObjHeader *hobj;
	for (PageIdx i=0; i<nSlots; ++i) if ((hobj=getObject(*--to=*--from))!=NULL) {
#ifdef _DEBUG
		switch (hobj->getType()) {
		default: assert(0 && "Invalid object type in HeapPage::compact()");
		case HO_PIN: case HO_FORWARD: if (hdr.pgid!=PGID_HEAP) assert(0 && "PIN on SSV page"); break;
		case HO_BLOB: case HO_SSVALUE: if (hdr.pgid!=PGID_SSV) assert(0 && "SSVALUE on PIN page"); break;
		}
		assert((byte*)hobj<(byte*)this+freeSpace);
#endif
		if (i!=modified) page->moveObject(hobj,(byte*)this,i,false);
	}
	if (modified<nSlots && (hobj=getObject((*this)[modified]))!=NULL) page->moveObject(hobj,(byte*)this,modified,true,propIdx);
	assert(page->contFree()==totalFree());
	if (!fCheckOnly) memcpy(&hdr,page,lPage-FOOTERSIZE);
}

void HeapPageMgr::HeapPage::moveObject(const HeapPageMgr::HeapObjHeader *hobj,byte *frame,PageIdx idx,bool fModified,ushort propIdx)
{
	ushort len=hobj->getLength(); PageOff off=freeSpace;
	if ((hobj->descr&HOH_MULTIPART)==0 && !fModified) {
		assert(contFree()>=len); memcpy((byte*)this+off,hobj,len); ushort delta;
		if (hobj->getType()==HO_PIN && (delta=ushort(off-((byte*)hobj-frame)))!=0) {
			HeapV *hprop=((HeapPIN*)((byte*)this+off))->getPropTab();
			for (int i=((HeapPIN*)((byte*)this+off))->nProps; --i>=0; hprop++) if (!hprop->type.isCompact())
				{hprop->offset+=delta; if (hprop->type.isCompound()) adjustCompound((byte*)this,hprop,delta);}
		}
		freeSpace+=len;
	} else {
		ushort lhdr=((HeapPIN*)hobj)->headerLength(),ldata=0; assert(hobj->getType()==HO_PIN);		// or SSV struct?
		HeapPIN *hpin=(HeapPIN*)alloca(lhdr); memcpy(hpin,hobj,lhdr);
		if (!fModified||propIdx!=ushort(~0u)) {assert(lhdr<=contFree()); freeSpace+=lhdr;}
		HeapV *hprop=hpin->getPropTab(); ushort size;
		for (ushort i=0; i<hpin->nProps; ++i,++hprop) if (i!=propIdx && !hprop->type.isCompact()) {
			assert(hprop->offset<((HeapPage*)frame)->freeSpace);
			size=hprop->type.isCompound() ?
				moveCompound(hprop->type,(byte*)this,freeSpace,frame,hprop->offset) :
				(ushort)ceil(moveData((byte*)this+freeSpace,frame+hprop->offset,hprop->type),HP_ALIGN);
			assert(size<=contFree()); hprop->offset=freeSpace; ldata+=size; freeSpace+=size;
		}
		if (!fModified) hpin->hdr.descr&=~HOH_MULTIPART;
		else if (propIdx==ushort(~0u)) {assert(lhdr<=contFree()); hpin->hdr.descr|=HOH_MULTIPART; off=freeSpace; freeSpace+=lhdr;}
		else {
			hprop=&hpin->getPropTab()[propIdx]; assert(hprop->type.isCompound()&&hprop->type.getFormat()!=HDF_LONG);
			const HeapVV *coll=(const HeapVV*)(frame+hprop->offset); size=dataLength(hprop->type,(byte*)coll,frame); assert(size<=contFree());
			ushort clen=coll->length(hprop->type.isCollection()),sht=0; HeapVV *dcoll=(HeapVV*)((byte*)this+freeSpace+size-clen); memcpy(dcoll,coll,clen);
			for (unsigned i=0,j=coll->cnt; i<j; i++) if (!coll->start[i].type.isCompact())
				sht+=(ushort)ceil(moveData((byte*)this+(dcoll->start[i].offset=freeSpace+sht),frame+coll->start[i].offset,coll->start[i].type),HP_ALIGN);
			assert(sht+clen==size); assert(size<=contFree()); hprop->offset=freeSpace+sht; ldata+=size; freeSpace+=size; hpin->hdr.descr&=~HOH_MULTIPART;
		}
		memcpy((byte*)this+off,hpin,lhdr); assert(hpin->hdr.length==ldata+lhdr);
	}
	(*this)[idx]=off;
}

bool HeapPageMgr::HeapPage::checkIdx(PageIdx idx) const
{
	if (idx>=nSlots) {
		Session *ses=Session::getSession();
		if (ses==NULL || ses->getExtAddr().pageID!=hdr.pageID || ses->getExtAddr().idx!=idx)
			report(MSG_ERROR,"Invalid idx %u for page %08X (%u entries)\n",idx,hdr.pageID,nSlots);
		return false;
	}
	return true;
}

RC HeapPageMgr::HeapPIN::serialize(byte *&buf,size_t& lbuf,const HeapPageMgr::HeapPage *hp,MemAlloc *ma,size_t len,bool fExpand) const
{
	const bool fMultipart=fExpand||(hdr.descr&HOH_MULTIPART)!=0;
	ushort ldata=fMultipart?headerLength():hdr.length,off=ushort((const byte*)this-(const byte*)hp);
	size_t lrec=len!=0?len:hdr.getLength(); if (lrec<sizeof(HeapPIN)) return RC_CORRUPTED;
	if ((buf==NULL||lbuf<lrec) && (buf=(byte*)ma->realloc(buf,lrec,lbuf))==NULL) return RC_NORESOURCES;
	memcpy(buf,this,ldata); lbuf=lrec;
	if (off!=0 || fMultipart) {
		HeapV *hprop=((HeapPIN*)buf)->getPropTab();
		for (int i=nProps; --i>=0; hprop++) if (!hprop->type.isCompact()) {
			if (fMultipart) {
				off=hprop->offset; hprop->offset=ldata;
				if (!hprop->type.isCompound()) 
					ldata+=(ushort)ceil(moveData(buf+ldata,(const byte*)hp+off,hprop->type),HP_ALIGN);
				else if (!fExpand || hprop->type.getFormat()==HDF_LONG)
					ldata+=moveCompound(hprop->type,buf+ldata,0,(const byte*)hp,off);
				else {
					HeapVV *coll=(HeapVV*)(buf+ldata); ushort sht=((HeapVV*)((byte*)hp+off))->length(hprop->type.isCollection());
					memcpy(coll,(byte*)hp+off,sht); HeapV *elt=coll->start;
					for (int i=coll->cnt; --i>=0; ++elt) {
						if (!elt->type.isCompact()) {
							ushort sz=(ushort)ceil(moveData(buf+ldata+sht,(byte*)hp+elt->offset,elt->type),HP_ALIGN); elt->offset=sht; sht+=sz;
						} else if (elt->type.getType()==VT_REFID) {
							PageAddr addr={hp->hdr.pageID,elt->offset}; elt->offset=sht; elt->type.setType(VT_REFID,HDF_SHORT);
							memcpy(buf+ldata+sht,&addr,PageAddrSize); sht+=PageAddrSize; ((HeapPIN*)buf)->hdr.length+=PageAddrSize;
						}
					}
					ldata+=sht; assert(ldata<=lrec);
				}
			} else {
				if (hprop->type.isCompound() && hprop->type.getFormat()!=HDF_LONG) {
					HeapVV *coll=(HeapVV*)(buf+hprop->offset-off); HeapV *elt=coll->start;
					for (int i=coll->cnt; --i>=0; ++elt) if (!elt->type.isCompact()) elt->offset-=hprop->offset;
				}
				hprop->offset-=off;
			}
		} else if (fExpand && hprop->type.getType()==VT_REFID) {
			PageAddr addr={hp->hdr.pageID,hprop->offset}; ((HeapPIN*)buf)->hdr.length+=PageAddrSize;
			hprop->offset=ldata; hprop->type.setType(VT_REFID,HDF_SHORT);
			memcpy(buf+ldata,&addr,PageAddrSize); ldata+=PageAddrSize;
		}
		assert(((HeapPIN*)buf)->hdr.length==ldata);
		((HeapPIN*)buf)->hdr.descr&=~HOH_MULTIPART;
		if (fExpand) ((HeapPIN*)buf)->hdr.descr&=~HOH_COMPACTREF;
	}
	return RC_OK;
}

bool HeapPageMgr::HeapPIN::getAddr(PID& id) const
{
	id.ident=STORE_OWNER; PageAddr addr;
	HeapDataFmt fmt; const byte *p=getRemoteID(fmt); assert(p!=NULL);
	switch (fmt) {
	case HDF_COMPACT: return false;
	case HDF_SHORT: memcpy(&addr,p,PageAddrSize); id.pid=addr; break;
	case HDF_NORMAL: id.pid=__una_get(*(OID*)p); break;
	case HDF_LONG: id.pid=__una_get(*(OID*)p); id.ident=__una_get(*(IdentityID*)(p+sizeof(OID))); break;
	}
	return true;
}

size_t HeapPageMgr::HeapPIN::expLength(const byte *frame) const
{
	const HeapV *hprop=getPropTab(); size_t len=hdr.length;
	for (unsigned i=nProps; i!=0; --i,++hprop) switch (hprop->type.getType()) {
	default: break;
	case VT_REFID: if (hprop->type.isCompact()) len+=PageAddrSize; break;
	case VT_ARRAY: case VT_STRUCT: case VT_MAP:
		if (hprop->type.getFormat()!=HDF_LONG) {
			const HeapVV *coll=(const HeapVV*)(frame+hprop->offset); const HeapV *elt=coll->start;
			for (int i=coll->cnt; --i>=0; ++elt)
				if (elt->type.getFormat()==HDF_COMPACT && elt->type.getType()==VT_REFID) len+=PageAddrSize;
				//else if (VT_ARRAY in VT_STRUCT || VT_STRUCT in VT_ARRAY) -> ...
		}
		break;
	}
	return len;
}

unsigned HeapPageMgr::HeapPage::countRefs(const HeapPageMgr::HeapV *hprop) const
{
	unsigned cnt=0; assert(hprop!=NULL && hprop->type.isCompound() && hprop->type.getFormat()!=HDF_LONG);
	const HeapVV *coll=(const HeapVV*)((byte*)this+hprop->offset); const HeapV *elt=coll->start;
	for (int i=coll->cnt; --i>=0; ++elt) if (elt->type.getType()==VT_REFID) cnt++;
	return cnt;
}

void HeapPageMgr::HeapPage::getRefs(const HeapPageMgr::HeapV *hprop,PID *pids,unsigned xPids) const
{
	unsigned cnt=0; assert(hprop!=NULL && hprop->type.isCompound() && hprop->type.getFormat()!=HDF_LONG);
	const HeapVV *coll=(const HeapVV*)((byte*)this+hprop->offset); const HeapV *elt=coll->start;
	for (int i=coll->cnt; --i>=0 && cnt<xPids; ++elt) 
		if (elt->type.getType()==VT_REFID) getRef(pids[cnt++],elt->type.getFormat(),elt->offset);
}

void HeapPageMgr::HeapPage::getRef(PID& id,HeapDataFmt fmt,PageOff offs) const
{
	PageAddr addr;
	switch (fmt) {
	default: id.pid=STORE_INVALID_PID; id.ident=STORE_INVALID_IDENTITY; break;
	case HDF_LONG: memcpy(&id.pid,(byte*)this+offs,sizeof(OID)); memcpy(&id.ident,(byte*)this+offs+sizeof(OID),sizeof(IdentityID)); break;
	case HDF_NORMAL: memcpy(&id.pid,(byte*)this+offs,sizeof(OID)); id.ident=STORE_OWNER; break; 
	case HDF_SHORT: memcpy(&addr,(byte*)this+offs,PageAddrSize); id.pid=addr; id.ident=STORE_OWNER; break;
	case HDF_COMPACT: addr.pageID=hdr.pageID; addr.idx=(PageIdx)offs; id.pid=addr; id.ident=STORE_OWNER; break;
	}
}

//-----------------------------------------------------------------------------------------------

PBlock *PINPageMgr::getNewPage(size_t size,size_t reserve,Session *ses)
{
	if (ses==NULL||!ses->inWriteTx()) return NULL; StoreCtx *ctx=ses->getStore(); if (reserve>SPACE_OVERSHOOT) reserve-=SPACE_OVERSHOOT;
	if (ses->reuse.pinPages!=NULL && ses->reuse.nPINPages>0 && ses->reuse.pinPages[ses->reuse.nPINPages-1].space>=size+reserve)
		return ctx->bufMgr->getPage(ses->reuse.pinPages[--ses->reuse.nPINPages].pid,ctx->heapMgr,PGCTL_XLOCK|PGCTL_RLATCH,NULL,ses);
	PBlock *pb=NULL;
	if ((freeSpace.getPage(size,pb,this)!=RC_OK || pb==NULL) &&							// reserve?
		(pb=ctx->fsMgr->getNewPage(this))!=NULL && (ses->addToHeap(pb->getPageID(),false))!=RC_OK) {pb->release(QMGR_UFORCE,ses); pb=NULL;}
	return pb;
}

PBlock *SSVPageMgr::getNewPage(size_t size,Session *ses,bool& fNew)
{
	fNew=false; if (ses==NULL||!ses->inWriteTx()) return NULL;
	if (ses->reuse.ssvPages!=NULL && ses->reuse.nSSVPages>0 && ses->reuse.ssvPages[ses->reuse.nSSVPages-1].space>=size)
		{fNew=true; return ctx->bufMgr->getPage(ses->reuse.ssvPages[--ses->reuse.nSSVPages].pid,ctx->ssvMgr,PGCTL_XLOCK,NULL,ses);}
	StoreCtx *ctx=ses->getStore();
	size_t xSize=contentSize(ctx->bufMgr->getPageSize()); if (size>xSize) return NULL;
	if (size<xSize) {PBlock *pb=NULL; if (freeSpace.getPage(size,pb,this)==RC_OK && pb!=NULL) return pb;}
	fNew=true; return ctx->fsMgr->getNewPage(this);
}

RC PINPageMgr::addPagesToMap(const PageSet& ps,Session *ses,bool fCl)
{
	if (ses==NULL || ses->getTxState()!=TX_COMMITTING) return RC_INTERNAL;
	const HeapDirMgr::HeapDirPage *hd; PageID last; PBlockP pb; RWLockP lck; RC rc=RC_OK; bool fLocked=false;
	unsigned nSlots=0,xSlots=unsigned(HeapDirMgr::contentSize(ctx->bufMgr->getPageSize())/sizeof(PageID));
	const MapAnchor maFirst=fCl?MA_CLASSDIRFIRST:MA_HEAPDIRFIRST,maLast=fCl?MA_CLASSDIRLAST:MA_HEAPDIRLAST;
	if ((last=ctx->theCB->getRoot(maLast))==INVALID_PAGEID) {
		lck.set(&ctx->hdirMgr->dirLock,RW_X_LOCK); fLocked=true;
		if ((last=ctx->theCB->getRoot(maLast))!=INVALID_PAGEID) {lck.set(NULL); fLocked=false;}
	}
	if (last!=INVALID_PAGEID) {
		for (;;last=hd->next) {
			if (pb.getPage(last,ctx->hdirMgr,PGCTL_ULOCK|PGCTL_COUPLE,ses)==NULL) return RC_CORRUPTED;
			hd=(const HeapDirMgr::HeapDirPage*)pb->getPageBuf(); assert(hd->nSlots<=xSlots);
			if (hd->next==INVALID_PAGEID) break;
		}
		nSlots=xSlots-hd->nSlots; ctx->hdirMgr->lock(pb);
	}
	unsigned lrec=sizeof(HeapDirMgr::HeapDirUpdate)+((unsigned(ps)>xSlots?xSlots:unsigned(ps))-1)*sizeof(PageID);
	HeapDirMgr::HeapDirUpdate hu0,*hu=unsigned(ps)==1?&hu0:(HeapDirMgr::HeapDirUpdate*)ses->malloc(lrec); 
	if (hu==NULL) return RC_NORESOURCES;

	PageSet::it it(ps);
	if (nSlots>0) {
		if (nSlots>unsigned(ps)) nSlots=unsigned(ps); hu->nHeapPages=nSlots; assert(!pb.isNull());
		for (unsigned i=0; i<nSlots; i++) {hu->heapPages[i]=++it; assert(hu->heapPages[i]!=INVALID_PAGEID);}
		rc=ctx->txMgr->update(pb,ctx->hdirMgr,HDU_HPAGES,(byte*)hu,sizeof(HeapDirMgr::HeapDirUpdate)+int(nSlots-1)*sizeof(PageID));
	}
	if (rc==RC_OK && nSlots<unsigned(ps)) {
		unsigned nDirPages=(unsigned(ps)-nSlots+xSlots-1)/xSlots; PageID dpgs[6],*dPages=dpgs; PBlockP pb2;
		if (nDirPages>6 && (dPages=(PageID*)ses->malloc(nDirPages*sizeof(PageID)))==NULL) rc=RC_NORESOURCES;
		else {
			{MiniTx mtx(ses); MapAnchorUpdate anchorUpdate;
			if ((rc=ctx->fsMgr->allocPages(nDirPages,dPages))==RC_OK) {
				if (pb2.newPage(dPages[0],ctx->hdirMgr,0,ses)==NULL) rc=RC_NORESOURCES;
				else {
					if (!pb.isNull()) {
						pb->checkDepth(); rc=ctx->txMgr->update(pb,ctx->hdirMgr,HDU_NEXT,(byte*)&dPages[0],sizeof(PageID),0,pb2);
					} else if (nDirPages>1 || (pb2->checkDepth(),rc=ctx->txMgr->update(pb2,ctx->hdirMgr,HDU_NEXT,NULL,0))==RC_OK) {
						assert(ctx->theCB->getRoot(MA_HEAPDIRFIRST)==INVALID_PAGEID && fLocked);
						anchorUpdate.oldPageID=INVALID_PAGEID; anchorUpdate.newPageID=dPages[0];
						ctx->cbLSN=ctx->logMgr->insert(ses,LR_UPDATE,maFirst<<PGID_SHIFT|PGID_MASTER,INVALID_PAGEID,NULL,&anchorUpdate,sizeof(MapAnchorUpdate));
						rc=ctx->theCB->update(ctx,maFirst,(byte*)&anchorUpdate,sizeof(MapAnchorUpdate));
						if (rc==RC_OK) {
							// set maxPage, maxIdx, unlock
						}
					}
					if (rc==RC_OK) {
						for (unsigned i=1; i<nDirPages; i++) {
							PBlock *next=ctx->bufMgr->newPage(dPages[i],ctx->hdirMgr,NULL,0,ses);
							if (next==NULL) {rc=RC_NORESOURCES; break;}
							pb2->checkDepth(); rc=ctx->txMgr->update(pb2,ctx->hdirMgr,HDU_NEXT,(byte*)&dPages[i],sizeof(PageID),0,next);
							pb2.release(ses); pb2=next; if (rc!=RC_OK) break;
						}
						if (rc==RC_OK) {
							anchorUpdate.oldPageID=last; anchorUpdate.newPageID=dPages[nDirPages-1];
							ctx->cbLSN=ctx->logMgr->insert(ses,LR_UPDATE,maLast<<PGID_SHIFT|PGID_MASTER,INVALID_PAGEID,NULL,&anchorUpdate,sizeof(MapAnchorUpdate));
							rc=ctx->theCB->update(ctx,maLast,(byte*)&anchorUpdate,sizeof(MapAnchorUpdate));
						}
					}
				}
			}
			if (rc!=RC_OK) pb2.release(ses); else mtx.ok();
			}
			for (unsigned i=0; rc==RC_OK && i<nDirPages; i++) {
				if (i+1==nDirPages) pb2.moveTo(pb); else pb.getPage(dPages[i],ctx->hdirMgr,PGCTL_XLOCK,ses);
				if (pb.isNull()) rc=RC_NORESOURCES;
				else {
					unsigned n=unsigned(ps)-nSlots; assert(n>0); if (n>xSlots) n=xSlots; hu->nHeapPages=n; nSlots+=n;
					for (unsigned i=0; i<n; i++) {hu->heapPages[i]=++it; assert(hu->heapPages[i]!=INVALID_PAGEID);}
					rc=ctx->txMgr->update(pb,ctx->hdirMgr,HDU_HPAGES,(byte*)hu,sizeof(HeapDirMgr::HeapDirUpdate)+int(n-1)*sizeof(PageID));
				}
			}
			if (dPages!=dpgs) ses->free(dPages);
		}
	}
	if (rc==RC_OK) pb.release(ses);
	if (hu!=&hu0) ses->free(hu);
	return rc;
}

void PINPageMgr::reuse(PBlock *pb,Session *ses,size_t reserve,bool fMod)
{
	PageID pid=pb->getPageID(); assert(ses!=NULL);
	const HeapPage *hp=(const HeapPage*)pb->getPageBuf(); ushort spaceLeft=ushort(hp->totalFree()); 
	if (!ses->tx.testHeap(pid)) freeSpace.set(ses->getStore(),pid,spaceLeft,spaceLeft>reserve);
	else if (spaceLeft>reserve) {
		if (fMod && ses->reuse.nPINPages>0) {
			for (TxReuse::ReusePage *pg=&ses->reuse.pinPages[ses->reuse.nPINPages]; --pg>=ses->reuse.pinPages;) if (pg->pid==pid) {
				if ((pg==ses->reuse.pinPages || spaceLeft>=pg[-1].space) && (pg==&ses->reuse.pinPages[ses->reuse.nPINPages-1] || spaceLeft<=pg[1].space))
					{pg->space=spaceLeft; pg->nSlots=hp->nSlots; return;}
				if (pg<&ses->reuse.pinPages[--ses->reuse.nPINPages]) memmove(pg,pg+1,(byte*)&ses->reuse.pinPages[ses->reuse.nPINPages]-(byte*)pg);
				break;
			}
		}
#ifdef _DEBUG
		for (unsigned i=0; i<ses->reuse.nPINPages; i++) assert(ses->reuse.pinPages[i].pid!=pid);
#endif
		ses->reuse.pinPages=(TxReuse::ReusePage*)ses->realloc(ses->reuse.pinPages,(ses->reuse.nPINPages+1)*sizeof(TxReuse::ReusePage));
		if (ses->reuse.pinPages==NULL) {ses->reuse.nPINPages=0; return;}
		TxReuse::ReusePage *pg=ses->reuse.pinPages;
		for (unsigned n=ses->reuse.nPINPages,k; n>0;)
			if (pg[k=n>>1].space==spaceLeft || pg[k].space>spaceLeft && (n=k)==0 || pg[k].space<spaceLeft && (pg+=k+1,n-=k+1)==0) break;
		if (pg<&ses->reuse.pinPages[ses->reuse.nPINPages]) memmove(pg+1,pg,(byte*)&ses->reuse.pinPages[ses->reuse.nPINPages]-(byte*)pg);
		pg->pid=pid; pg->space=spaceLeft; pg->nSlots=hp->nSlots; ses->reuse.nPINPages++;
	}
}

void SSVPageMgr::reuse(PBlockP &pb,Session *ses,bool fNew,bool fInsert)
{
	PageID pid=pb->getPageID(); assert(ses!=NULL); StoreCtx *ctx=ses->getStore();
	const HeapPage *hp=(const HeapPage*)pb->getPageBuf(); size_t spaceLeft=hp->totalFree(); 
	size_t reserve=floor(size_t((contentSize(ctx->bufMgr->getPageSize())-sizeof(PageOff))*ctx->theCB->pctFree),HP_ALIGN);
	if (!fNew) {
		bool fDrop=fInsert && hp->nSlots==0; freeSpace.set(ctx,pid,spaceLeft,!fDrop && spaceLeft>=reserve);
		if (fDrop) {
			if (ses->reuse.nSSVPages>0) for (TxReuse::ReusePage *pg=&ses->reuse.ssvPages[ses->reuse.nSSVPages]; --pg>=ses->reuse.ssvPages;)
				if (pg->pid==pid) {
					if (pg<&ses->reuse.ssvPages[--ses->reuse.nSSVPages]) memmove(pg,pg+1,(byte*)&ses->reuse.ssvPages[ses->reuse.nSSVPages]-(byte*)pg);
					break;
				}
			pb.set(PGCTL_DISCARD); pb.release(ses); ctx->logMgr->insert(ses,LR_DISCARD,PGID_SSV,pid); ctx->fsMgr->freePage(pid);
		}
	} else if (spaceLeft>reserve) {
#ifdef _DEBUG
		for (unsigned i=0; i<ses->reuse.nSSVPages; i++) assert(ses->reuse.ssvPages[i].pid!=pid);
#endif
		ses->reuse.ssvPages=(TxReuse::ReusePage*)ses->realloc(ses->reuse.ssvPages,(ses->reuse.nSSVPages+1)*sizeof(TxReuse::ReusePage));
		if (ses->reuse.ssvPages==NULL) {ses->reuse.nSSVPages=0; return;}
		TxReuse::ReusePage *pg=ses->reuse.ssvPages;
		for (unsigned n=ses->reuse.nSSVPages;n>0;)
			{unsigned k=n>>1; if (pg[k].space==spaceLeft || pg[k].space>spaceLeft && (n=k)==0 || pg[k].space<spaceLeft && (pg+=k+1,n-=k+1)==0) break;}
		if (pg<&ses->reuse.ssvPages[ses->reuse.nSSVPages]) memmove(pg+1,pg,(byte*)&ses->reuse.ssvPages[ses->reuse.nSSVPages]-(byte*)pg);
		pg->pid=pid; pg->space=(ushort)spaceLeft; pg->nSlots=hp->nSlots; ses->reuse.nSSVPages++;
	}
}

void HeapPageMgr::discardPage(PageID pid,Session *ses)
{
	assert(ses!=NULL); freeSpace.set(ctx,pid,0,false);
}

RC HeapPageMgr::HeapSpace::set(StoreCtx *ctx,PageID pid,size_t size,bool fAdd)
{
	MutexP lck(&lock); HeapPageSpace *hps=spaceTab.find(pid); unsigned idx;
	if (hps!=NULL) {
		if (hps->space==size && fAdd) return RC_OK;
		if (pageTab!=NULL && (idx=find(hps))<nPages && pageTab[idx]==hps && idx<--nPages) 
			memmove(&pageTab[idx],&pageTab[idx+1],(nPages-idx)*sizeof(HeapPageSpace*));
	}
	if (!fAdd || nPages>=SPACE_TAB_SIZE && size<=pageTab[nPages-1]->space) 
		{if (hps!=NULL) spaceTab.remove(hps,true); return RC_OK;}
	if (hps!=NULL) hps->space=size;
	else if ((hps=new(ctx) HeapPageSpace(pid,size))==NULL) return RC_NORESOURCES;
	else {spaceTab.insert(hps); if (nPages>=SPACE_TAB_SIZE) spaceTab.remove(pageTab[--nPages],true);}
	if (pageTab!=NULL) idx=find(hps);
	else if ((pageTab=new(ctx) HeapPageSpace*[SPACE_TAB_SIZE])==NULL) return RC_NORESOURCES;
	else idx=0;
	assert(idx<=nPages && nPages<SPACE_TAB_SIZE);
	if (idx<nPages) memmove(&pageTab[idx+1],&pageTab[idx],(nPages-idx)*sizeof(HeapPageSpace*));
	pageTab[idx]=hps; nPages++;
	return RC_OK;
}

RC HeapPageMgr::HeapSpace::getPage(size_t size,PBlock*& pb,HeapPageMgr *mgr)
{
	MutexP lck(&lock);
	if (nPages==0 || pageTab==NULL || pageTab[0]->space<size) pb=NULL;
	else {
		unsigned midx=0;
		if (pageTab[nPages-1]->space>=size) midx=nPages;
		else for (unsigned n=nPages; n>0; ) {unsigned k=n>>1; if (pageTab[midx+k]->space<size) n=k; else {midx+=k+1; n-=k+1;}}
		for (int i=0; midx>0 && i<SPACE_PAGE_TRIES; i++) {
			unsigned idx=rand()%midx; HeapPageSpace *hps=pageTab[idx];
			pb=mgr->ctx->bufMgr->getPage(hps->pageID,mgr,PGCTL_ULOCK|QMGR_TRY|QMGR_UFORCE,pb);
			if (pb!=NULL) {
				const HeapPage *hp=(const HeapPage*)pb->getPageBuf();
				if (idx<--nPages) memmove(&pageTab[idx],&pageTab[idx+1],(nPages-idx)*sizeof(HeapPageSpace*));
				if ((hps->space=hp->totalFree())>=size) {spaceTab.remove(hps,true); return RC_OK;}
				idx=find(hps); assert(idx<=nPages && nPages<SPACE_TAB_SIZE);
				if (idx<nPages) memmove(&pageTab[idx+1],&pageTab[idx],(nPages-idx)*sizeof(HeapPageSpace*));
				pageTab[idx]=hps; nPages++; midx--;
			}
		}
		if (pb!=NULL) {pb->release(QMGR_UFORCE); pb=NULL;}
	}
	return RC_FALSE;
}

void HeapPageMgr::initPartial()
{
	if (ctx->theCB->nPartials>SPACE_TAB_SIZE) return;
	MutexP lck(&freeSpace.lock); PGID pgid=getPGID();
	for (unsigned i=0; i<ctx->theCB->nPartials; i++) {
		const PartialInfo &pi=ctx->theCB->partials[i];
		if (pi.pageType==pgid) {
			HeapSpace::HeapPageSpace *hps=new(ctx) HeapSpace::HeapPageSpace(pi.pageID,pi.spaceLeft); if (hps==NULL) break;
			if (freeSpace.nPages>=SPACE_TAB_SIZE) continue;
			if (freeSpace.pageTab==NULL && (freeSpace.pageTab=new(ctx) HeapSpace::HeapPageSpace*[SPACE_TAB_SIZE])==NULL) break;
			freeSpace.spaceTab.insert(hps); unsigned idx=freeSpace.find(hps);
			assert(idx<=freeSpace.nPages && freeSpace.nPages<SPACE_TAB_SIZE);
			if (idx<freeSpace.nPages) memmove(&freeSpace.pageTab[idx+1],&freeSpace.pageTab[idx],(freeSpace.nPages-idx)*sizeof(HeapSpace::HeapPageSpace*));
			freeSpace.pageTab[idx]=hps; freeSpace.nPages++;
		}
	}
}

void HeapPageMgr::savePartial(HeapPageMgr *mgr1,HeapPageMgr *mgr2)
{
	MutexP lck(&mgr1->freeSpace.lock); uint32_t cnt=0; uint16_t pt;
	for (unsigned i=0,j=0; cnt<uint32_t(sizeof(mgr1->ctx->theCB->partials)/sizeof(mgr1->ctx->theCB->partials[0])) && (i<mgr1->freeSpace.nPages||j<mgr2->freeSpace.nPages); cnt++) {
		const HeapSpace::HeapPageSpace *hps=i<mgr1->freeSpace.nPages && (j>=mgr2->freeSpace.nPages||mgr1->freeSpace.pageTab[i]->space>=mgr2->freeSpace.pageTab[j]->space)?
			(pt=mgr1->getPGID(),mgr1->freeSpace.pageTab[i++]):(pt=mgr2->getPGID(),mgr2->freeSpace.pageTab[j++]);
		PartialInfo &pi=mgr1->ctx->theCB->partials[cnt]; pi.pageID=hps->pageID; pi.spaceLeft=(uint16_t)hps->space; pi.pageType=pt;
	}
	mgr1->ctx->theCB->nPartials=cnt;
}

//---------------------------------------------------------------------------------------------------------------------------------------------------------

HeapDirMgr::HeapDirMgr(StoreCtx *ctx) : TxPage(ctx)
{
	ctx->registerPageMgr(PGID_HEAPDIR,this);
}

HeapDirMgr::~HeapDirMgr()
{
}

RC HeapDirMgr::update(PBlock *pb,size_t len,unsigned info,const byte *rec,size_t lrec,unsigned flags,PBlock *newp)
{
	byte *frame=pb->getPageBuf(); HeapDirPage *hp=(HeapDirPage*)frame,*hp2; const HeapDirUpdate *hd;
	switch (info&HPOP_MASK) {
	case HDU_NEXT:
		if (lrec==0) assert(newp==NULL);
		else if ((flags&TXMGR_UNDO)!=0) {
			assert(lrec==sizeof(PageID));
			PBlock *pb2=ctx->bufMgr->getPage(*(PageID*)rec,ctx->hdirMgr,PGCTL_XLOCK);
			if (pb2!=NULL) {
				if ((hp2=(HeapDirPage*)pb2->getPageBuf())!=NULL && hp2->nSlots==0 && hp2->next==INVALID_PAGEID) 
					{pb2->release(PGCTL_DISCARD|QMGR_UFORCE); hp->next=INVALID_PAGEID;} else pb2->release();
			}
		} else {
			assert(hp->next==INVALID_PAGEID&&lrec==sizeof(PageID));
			assert((flags&TXMGR_RECV)!=0 || newp!=NULL);
			PBlock *pb2=newp==NULL?ctx->logMgr->setNewPage(ctx->bufMgr->newPage(*(PageID*)rec,this)):newp;
			if (pb2==NULL) return RC_NORESOURCES; assert(pb2->getPageID()==*(PageID*)rec);
			hp->next=*(PageID*)rec; pb->setDependency(pb2);
		}
		break;
	case HDU_HPAGES:
		hd=(const HeapDirUpdate*)rec; assert(lrec==sizeof(HeapDirUpdate)+int(hd->nHeapPages-1)*sizeof(PageID));
		if ((flags&TXMGR_UNDO)!=0) {
			if (hd->nHeapPages>hp->nSlots) return RC_CORRUPTED; PageID *ids=(PageID*)(hp+1);
			for (unsigned i=0,j=hp->nSlots-hd->nHeapPages;;++ids,++i) 
				if (i>j) return RC_CORRUPTED;
				else if (*ids==hd->heapPages[0]) {
					for (unsigned j=1; j<hd->nHeapPages; j++) if (ids[j]!=hd->heapPages[j]) return RC_CORRUPTED;
					if ((hp->nSlots-=hd->nHeapPages)>i) memmove(&ids[0],&ids[hd->nHeapPages],(hp->nSlots-i)*sizeof(PageID));
					break;
				}
		} else {
			assert((hp->nSlots+hd->nHeapPages)*sizeof(PageID)<=contentSize(hp->hdr.length()));
			memcpy(&((PageID*)(hp+1))[hp->nSlots],hd->heapPages,hd->nHeapPages*sizeof(PageID));
			hp->nSlots+=hd->nHeapPages;
		}
		break;
	}
	return RC_OK;
}

PageID HeapDirMgr::multiPage(unsigned info,const byte *rec,size_t lrec,bool& fMerge)
{
	fMerge=false; return (info&HPOP_MASK)==HDU_NEXT && lrec>=sizeof(PageID) ? *(PageID*)rec : INVALID_PAGEID;
}

void HeapDirMgr::initPage(byte *frame,size_t len,PageID pid)
{
	TxPage::initPage(frame,len,pid);
	HeapDirPage *hp=(HeapDirPage*)frame;
	hp->nSlots=0; hp->next=INVALID_PAGEID;
}

bool HeapDirMgr::afterIO(PBlock *pb,size_t lPage,bool fLoad)
{
	return TxPage::afterIO(pb,lPage,fLoad);		// ???
}

bool HeapDirMgr::beforeFlush(byte *frame,size_t len,PageID pid)
{
	return TxPage::beforeFlush(frame,len,pid);		// ???
}

PGID HeapDirMgr::getPGID() const
{
	return PGID_HEAPDIR;
}

void HeapDirMgr::lock(PBlock *pb)
{
//	dirLock.lock(RW_X_LOCK);
//	???????if (pb!=NULL) {maxPage=pb->getPageID(); maxIdx=((HeapDirPage*)pb->getPageBuf())->nSlots;}
//	dirLock.downgradelock(RW_U_LOCK);
}
