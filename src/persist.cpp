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

#include "buffer.h"
#include "txmgr.h"
#include "fsmgr.h"
#include "queryprc.h"
#include "stmt.h"
#include "ftindex.h"
#include "startup.h"
#include "blob.h"
#include "expr.h"
#include "netmgr.h"
#include "classifier.h"
#include "service.h"
#include "maps.h"
#include "fsm.h"
#include "fio.h"

using namespace AfyKernel;

#define	START_BUF_SIZE	0x200

#define	PGF_NEW			0x0001
#define	PGF_FORCED		0x0002
#define	PGF_SLOTS		0x0004

#define	COMMIT_MASK			0xFFFF
#define	COMMIT_FTINDEX		0x8000
#define	COMMIT_REFERRED		0x4000
#define	COMMIT_REFERS		0x2000
#define	COMMIT_ALLOCATED	0x1000
#define	COMMIT_PREFIX		0x0800
#define	COMMIT_RESERVED		0x0400
#define	COMMIT_MIGRATE		0x0200
#define	COMMIT_FORCED		0x0100
#define	COMMIT_ISPART		0x0080
#define	COMMIT_PARTS		0x0040
#define	COMMIT_SSVS			0x0020
#define	COMMIT_INVOKED		0x0010

namespace AfyKernel
{
	struct AllocPage
	{
		AllocPage	*next;
		AllocPage	*next2;
		PageID		pid;
		size_t		spaceLeft;
		size_t		lPins;
		ushort		idx;
		ushort		flags;
		PIN			*pins;
		AllocPage(PageID p,size_t spc,ushort nsl,ushort f) : next(NULL),next2(NULL),pid(p),spaceLeft(spc),lPins(0),idx(nsl),flags(f),pins(NULL) {}
	};
};

RC QueryPrc::persistPINs(const EvalCtx& ectx,PIN *const *pins,unsigned nPins,unsigned mode,const AllocCtrl *actrl,size_t *pSize,const IntoClass *into,unsigned nInto)
{
	if (pins==NULL || nPins==0) return RC_OK; if (ectx.ses==NULL) return RC_NOSESSION;
	if (ctx->isServerLocked() || ectx.ses->inReadTx()) return RC_READTX;
	if (ectx.ses->getIdentity()!=STORE_OWNER && !ectx.ses->mayInsert()) return RC_NOACCESS;
	TxSP tx(ectx.ses); PBlockP pb; const HeapPageMgr::HeapPage *hp; RC rc=RC_OK; unsigned nMetaPINs=0,nNamedPINs=0,nIndexed=0;
	size_t totalSize=0,lmin=size_t(~0ULL),lmax=0; unsigned cnt=0,i,j; PIN *pin; byte *buf; TIMESTAMP ts=0ULL;
	const size_t xSize=HeapPageMgr::contentSize(ctx->bufMgr->getPageSize())-sizeof(PageOff);
	const size_t reserve=ceil(size_t(xSize*(actrl!=NULL&&actrl->pctPageFree>=0.f?actrl->pctPageFree:ctx->theCB->pctFree)),HP_ALIGN);
	const bool fNewPgOnly=ectx.ses->nTotalIns+nPins>INSERT_THRSH; const Value *pv;
	bool fForced=false,fUncommPINs=false; ElementID prefix=ctx->getPrefix(); PINMap pinMap((MemAlloc*)ectx.ses);
	byte cbuf[START_BUF_SIZE]; SubAlloc mem(ectx.ses,START_BUF_SIZE,cbuf,true); size_t threshold=xSize-reserve,xbuf=0;
	AllocPage *pages=NULL,*allocPages=NULL,*forcedPages=NULL; unsigned nNewPages=0; PIN **metaPINs=NULL; unsigned nInserted=0;
	for (i=0; i<nPins; i++) if ((pin=pins[i])!=NULL) {
		pin->mode&=~COMMIT_MASK; if (pin->addr.defined()) continue;
		if (((pin->mode|=mode&(PIN_NO_REPLICATION|PIN_NOTIFY|PIN_HIDDEN))&PIN_NO_REPLICATION)!=0) pin->mode&=~PIN_REPLICATED;
		if (pin->id.ident==STORE_INVALID_IDENTITY) {
			if (ectx.ses->isRestore()) {rc=RC_INVPARAM; goto finish;}
			if (pin->id.pid!=STORE_INVALID_PID) {PINMapElt pe={pin->id.pid,pin}; pinMap+=pe; pin->id.pid=STORE_INVALID_PID;}
		} else if (!isRemote(pin->id)) {pin->mode|=COMMIT_FORCED; fForced=true;}
		if ((pin->mode&PIN_HIDDEN)!=0) pin->mode=pin->mode&~(PIN_REPLICATED|PIN_NOTIFY)|PIN_NO_REPLICATION;
		size_t plen0=pin->length=sizeof(HeapPageMgr::HeapPIN)+(pin->id.pid==STORE_INVALID_PID||(pin->mode&COMMIT_FORCED)!=0?0:
													pin->id.ident==STORE_OWNER?sizeof(OID):sizeof(OID)+sizeof(IdentityID));
		size_t lBig=0,lOther=0; unsigned nBig=0; ElementID rPrefix=pin->getPrefix(ctx); PropertyID xPropID=STORE_INVALID_URIID;
		size_t len,l; const Value *cv; Value w; bool fComm=pin->findProperty(PROP_SPEC_SERVICE)!=NULL;
		uint64_t sp_bits[4]; sp_bits[0]=sp_bits[1]=sp_bits[2]=sp_bits[3]=0; unsigned nSpec=0;
		if ((pin->mode&PIN_TRANSIENT)!=0) sp_bits[0]|=1ULL<<PROP_SPEC_OBJID;
		for (j=0; j<pin->nProperties; j++) {
			pv=&pin->properties[j]; PropertyID pid=pv->property;
			assert(j==0 || pin->properties[j-1].property<pid); 
			switch (pv->type) {
			default: break;
			case VT_VARREF:
				if (pv->property==PROP_SPEC_SELF) {
					if (pv->length!=0 || pv->refV.flags!=VAR_SELF) {rc=RC_INVPARAM; goto finish;}
					if (ectx.env==NULL || ectx.nEnv==0 || ectx.env[0]==NULL) {rc=RC_NOTFOUND; goto finish;}
					Value *props=NULL,*tmp; unsigned np;
					if ((rc=copyV(ectx.env[0]->properties,ectx.env[0]->nProperties,props,ectx.ses))!=RC_OK) goto finish;
					tmp=pin->properties; np=pin->nProperties; pin->properties=props; pin->nProperties=ectx.env[0]->nProperties;
					for (unsigned i=0; i<np; i++) if (&tmp[i]!=pv) pin->modify(&tmp[i],STORE_LAST_ELEMENT,prefix,0,ectx.ses);
					pin->mode|=ectx.env[0]->mode&(PIN_NO_REPLICATION|PIN_NOTIFY|PIN_REPLICATED|PIN_HIDDEN|PIN_INMEM|PIN_TRANSIENT);
					if ((pin->mode&PIN_NO_FREE)==0) freeV(tmp,np,ectx.ses); 
					fComm=pin->findProperty(PROP_SPEC_SERVICE)!=NULL; pin->length=plen0; lBig=lOther=0; nBig=0; j=~0u; continue;
				}
			case VT_EXPR: case VT_STMT: case VT_ARRAY: case VT_COLLECTION: case VT_STRUCT: case VT_REF: case VT_REFIDPROP: case VT_REFIDELT: case VT_CURRENT:
				if (pv->fcalc==0 || pv->property==PROP_SPEC_PROTOTYPE) break;
			case VT_EXPRTREE:
				if ((rc=eval(pv,ectx,&w,!fComm||(pin->mode&PIN_TRANSIENT)==0?EV_PID|EV_COPY:EV_COPY))!=RC_OK) goto finish;
				if ((rc=pin->checkSet(pv,w))!=RC_OK) {freeV(w); goto finish;}
				break;
			}
			if (pid<=MAX_BUILTIN_URIID) {
				if (pid==PROP_SPEC_PARENT) pin->mode|=COMMIT_ISPART;
				sp_bits[pid/(sizeof(uint64_t)*8)]|=1ULL<<pid%(sizeof(uint64_t)*8); nSpec++;
			} else if (pid>STORE_MAX_URIID) {rc=RC_INVPARAM; goto finish;}
			if (pv->type==VT_ARRAY || pv->type==VT_COLLECTION || pv->type==VT_STRUCT) {
				INav *nav=NULL;
				if (pv->type!=VT_COLLECTION) cv=pv->varray;
				else if ((nav=pv->nav)==NULL || (cv=nav->navigate(GO_FIRST))==NULL) {
					if (j<--pin->nProperties) {
						if ((pin->mode&PIN_NO_FREE)==0) memmove((Value*)pv,pv+1,(pin->nProperties-j)*sizeof(Value));
						else {
							//????
						}
					}
					if (pin->nProperties==0) break; else {--j; continue;}
				}
				if (pv->type!=VT_STRUCT) pv->eid=STORE_COLLECTION_ID; 
				else if (pv->eid==STORE_COLLECTION_ID) pv->eid=rPrefix;
				else if (pv->eid!=prefix && pv->eid!=rPrefix) len=sizeof(HeapPageMgr::HeapVV)+sizeof(HeapPageMgr::HeapKey)+ceil(len,HP_ALIGN);
				if ((pv->meta&META_PROP_SSTORAGE)!=0) {pv->flags|=VF_SSV|VF_PREFIX; len=pv->type==VT_STRUCT?sizeof(HRefSSV):sizeof(HeapPageMgr::HeapExtCollection);}
				else len=sizeof(HeapPageMgr::HeapVV)-sizeof(HeapPageMgr::HeapV)+(pv->type==VT_STRUCT?0:sizeof(HeapPageMgr::HeapKey));
				unsigned k=0;
				do {
					if ((pv->type==VT_ARRAY||pv->type==VT_STRUCT) && pv->property!=PROP_SPEC_PROTOTYPE) switch (cv->type) {
					default: break;
					case VT_EXPR: case VT_STMT: case VT_ARRAY: case VT_COLLECTION: case VT_STRUCT: case VT_REF: case VT_REFIDPROP: case VT_REFIDELT: if (cv->fcalc==0) break;
					case VT_VARREF: case VT_EXPRTREE: case VT_CURRENT: if ((rc=eval(cv,ectx,(Value*)cv))!=RC_OK) goto finish;	// ect!!
					}
					if ((rc=calcLength(*cv,l,0,threshold,ectx.ses))!=RC_OK) goto finish;
					pv->flags|=cv->flags&(VF_PREFIX|VF_REF|VF_STRING); if (l>=bigThreshold) {lBig+=l; nBig++;} else lOther+=l;
					if ((pv->flags&VF_SSV)==0 && (len+=ceil(l,HP_ALIGN)+sizeof(HeapPageMgr::HeapV))>threshold) 
						{pv->flags|=VF_SSV|VF_PREFIX; len=pv->type==VT_STRUCT?sizeof(HRefSSV):sizeof(HeapPageMgr::HeapExtCollection);}
					if ((cv->meta&META_PROP_PART)!=0 && (cv->type==VT_REF || cv->type==VT_REFID)) pin->mode|=COMMIT_PARTS;
				} while ((cv=nav!=NULL?nav->navigate(GO_NEXT):++k<pv->length?&pv->varray[k]:(Value*)0)!=NULL);
			} else {
				if ((rc=calcLength(*pv,len,0,threshold,ectx.ses))!=RC_OK) goto finish;
				if ((pv->meta&META_PROP_PART)!=0 && (pv->type==VT_REF || pv->type==VT_REFID)) pin->mode|=COMMIT_PARTS;
				if ((pv->flags&VF_SSV)==0 && (pv->meta&META_PROP_SSTORAGE)!=0 && len>0) switch (pv->type) {
				case VT_STRING: case VT_BSTR: case VT_STREAM:
				case VT_EXPR: case VT_STMT: pv->flags|=VF_SSV; len=sizeof(HRefSSV); break;
				default: break;
				}
				if (pv->eid==STORE_COLLECTION_ID) pv->eid=rPrefix;
				else if (pv->eid!=prefix && pv->eid!=rPrefix) len=sizeof(HeapPageMgr::HeapVV)+sizeof(HeapPageMgr::HeapKey)+ceil(len,HP_ALIGN);
				if (len>=bigThreshold) {lBig+=len; nBig++;} else lOther+=len;
			}
			if ((pv->flags&VF_SSV)!=0) pin->mode|=COMMIT_SSVS;
			if ((pv->flags&VF_REF)!=0) pin->mode|=COMMIT_REFERS;
			if ((pv->flags&VF_PREFIX)!=0) pin->mode|=COMMIT_PREFIX;
			if ((pin->mode&PIN_HIDDEN)==0 && (pv->flags&VF_STRING)!=0 && (pv->meta&META_PROP_FTINDEX)!=0) pin->mode|=COMMIT_FTINDEX;
			pin->length+=ceil(len,HP_ALIGN)+sizeof(HeapPageMgr::HeapV);
			if (pid>MAX_BUILTIN_URIID && (xPropID==STORE_INVALID_URIID || pid>xPropID)) xPropID=pid;
		}
		if (nSpec!=0) for (j=0; j<sizeof(Classifier::specPINProps)/sizeof(Classifier::specPINProps[0]); j++)
			if ((Classifier::specPINProps[j].mask[0]&sp_bits[0])==Classifier::specPINProps[j].mask[0] &&
				(Classifier::specPINProps[j].mask[1]&sp_bits[1])==Classifier::specPINProps[j].mask[1] &&
				(Classifier::specPINProps[j].mask[2]&sp_bits[2])==Classifier::specPINProps[j].mask[2] &&
				(Classifier::specPINProps[j].mask[3]&sp_bits[3])==Classifier::specPINProps[j].mask[3]) pin->meta|=Classifier::specPINProps[j].meta;
		if ((pin->meta&PMT_FSMCTX)!=0 && (pin->mode&PIN_IMMUTABLE)!=0) {rc=RC_INVPARAM; goto finish;}
		if ((pin->mode&PIN_DELETED)==0) {
			if ((pin->meta&PMT_NAMED)!=0) nNamedPINs++; if ((pin->meta&(PMT_CLASS|PMT_TIMER|PMT_LISTENER|PMT_LOADER))!=0) nMetaPINs++;
			if ((pin->meta&PMT_CLASS)!=0 && (pv=pin->findProperty(PROP_SPEC_PREDICATE))!=NULL && (pv->meta&META_PROP_INDEXED)!=0) nIndexed++;
		}
		if ((pin->mode&PIN_TRANSIENT)!=0) {
			ClassResult clr(ectx.ses,ectx.ses->getStore());	if ((rc=tx.start(TXI_DEFAULT,TX_IATOMIC))!=RC_OK) goto finish;
			if ((rc=ctx->classMgr->classify(pin,clr))==RC_OK && (into==NULL || (rc=clr.checkConstraints(pin,into,nInto))==RC_OK))
				if (clr.nActions!=0 && (rc=clr.invokeActions(pin,CI_INSERT,&ectx))==RC_OK) pin->mode|=COMMIT_INVOKED;
			if (rc==RC_OK && (pin->mode&PIN_TRANSIENT)!=0) {
				if (rc==RC_OK && (pin->meta&PMT_LOADER)!=0) {
					const Value *pv=Value::find(PROP_SPEC_LOAD,pin->properties,pin->nProperties);
					assert(pv!=NULL && pv->type==VT_STRING); rc=ctx->fileMgr->loadExt(pv->str,pv->length,ectx.ses,pin->properties,pin->nProperties,true);
				}
				if (rc==RC_OK && (pin->meta&PMT_COMM)!=0) {
					ServiceCtx *sctx=NULL;
					rc=ectx.ses->prepare(sctx,PIN::defPID,pin->properties,pin->nProperties,ISRV_WRITE|ISRV_NOCACHE);
					if (rc==RC_OK && sctx!=NULL) {
						rc=sctx->invoke(pin->properties,pin->nProperties);
						sctx->destroy();	// cache?
					}
				}
				if (rc==RC_OK && (pin->meta&PMT_LISTENER)!=0) rc=ectx.ses->listen(pin->properties,pin->nProperties,pin->mode);
				if (rc==RC_OK) {
					if ((pin->meta&PMT_FSMCTX)!=0) rc=ctx->fsmMgr->process(ectx.ses,pin);
					else if (rc==RC_OK && (pin->meta&PMT_TIMER)!=0) {
						//???
					}
				}
			}
			if (rc!=RC_OK) goto finish; if ((pin->mode&PIN_TRANSIENT)!=0) continue;
		}
		l=pin->length; cnt++; totalSize+=l+sizeof(PageOff); fUncommPINs=true;
		if (pin->length>xSize || (pin->meta&PMT_CLASS)==0 && (double)lBig/nBig>lOther*SKEW_FACTOR) {
			CandidateSSVs cs((MemAlloc*)ectx.ses);
			if ((rc=findCandidateSSVs(cs,pin->properties,pin->nProperties,pin->length>xSize,ectx.ses,actrl))!=RC_OK) goto finish;
			if (cs!=0) {
				cs.sort();
				for (j=0; j<cs; j++) {
					const CandidateSSV &cd=cs[j]; assert(cd.length>sizeof(HRefSSV));
					if (pin->length>xSize || cd.length>=bigThreshold) {
						cd.pv->flags|=VF_SSV; pin->length-=cd.length-cd.dlen; pin->mode|=COMMIT_SSVS;
					} else if (cd.length<bigThreshold) break;
				}
			}
			if (pin->length>xSize) {rc=RC_TOOBIG; goto finish;}
		}
		if (pin->length<lmin) lmin=pin->length; if (pin->length>lmax) lmax=pin->length;
		if (xPropID!=STORE_INVALID_URIID) ctx->classMgr->setMaxPropID(xPropID);
	}

	if (!fUncommPINs || (rc=tx.start(TXI_DEFAULT,TX_IATOMIC))!=RC_OK) goto finish;
	if (nIndexed!=0 && ectx.ses->classLocked!=RW_NO_LOCK && ectx.ses->classLocked!=RW_X_LOCK) {rc=RC_DEADLOCK; goto finish;}	//???
	xbuf=lmax+PageAddrSize; ectx.ses->lockClass(nIndexed>0?RW_X_LOCK:RW_S_LOCK);
	if (nMetaPINs!=0 && (metaPINs=(PIN**)mem.malloc(nMetaPINs*sizeof(PIN*)))==NULL) {rc=RC_NORESOURCES; goto finish;}
		
	for (i=nMetaPINs=0; i<nPins; i++) if ((pin=pins[i])!=NULL && !pin->addr.defined() && (pin->mode&(PIN_TRANSIENT|PIN_INMEM))==0) {
		AllocPage *pg=NULL; assert((pin->mode&COMMIT_ALLOCATED)==0 && pin->length<=xSize);
		if ((pin->mode&COMMIT_FORCED)!=0) {
			if (!pin->addr.convert(pin->id.pid)) {rc=RC_INVPARAM; goto finish;}
			for (pg=forcedPages; pg!=NULL && pg->pid!=pin->addr.pageID; pg=pg->next2);
			if (pg==NULL) {
				if ((pg=new(mem.alloc<AllocPage>()) AllocPage(pin->addr.pageID,0,0,PGF_FORCED))==NULL) {rc=RC_NORESOURCES; goto finish;}
				pg->next=pages; pg->next2=forcedPages; forcedPages=pages=pg;
			}
			pin->mode|=COMMIT_RESERVED;
		} else {
			AllocPage **ppg=&allocPages;
			if ((pin->mode&(COMMIT_REFERS|COMMIT_REFERRED))!=0) {
				// affinity based allocation
			}
			if (pg==NULL) {
				for (;(pg=*ppg)!=NULL && pg->spaceLeft<pin->length+sizeof(PageOff)+reserve; ppg=&pg->next2);
				if (pg==NULL) {
					PageID pid=INVALID_PAGEID; size_t spc=xSize; ushort nSlots=0,flg=PGF_NEW;
					if (ectx.ses->reuse.pinPages!=NULL && ectx.ses->reuse.nPINPages>0 && ectx.ses->reuse.pinPages[ectx.ses->reuse.nPINPages-1].space>=pin->length+sizeof(PageOff)+reserve)
						{TxReuse::ReusePage &rp=ectx.ses->reuse.pinPages[ectx.ses->reuse.nPINPages-1]; pid=rp.pid; spc=rp.space; nSlots=rp.nSlots; --ectx.ses->reuse.nPINPages; flg=0;} else nNewPages++;
					if ((pg=new(mem.alloc<AllocPage>()) AllocPage(pid,spc,nSlots,flg))==NULL) {rc=RC_NORESOURCES; goto finish;}
					pg->next=pages; pg->next2=allocPages; allocPages=pages=pg; ppg=&allocPages;
				}
			}
			if ((pg->spaceLeft-=pin->length+sizeof(PageOff))<=reserve+lmin) *ppg=pg->next2;
			pin->addr.pageID=pg->pid; pin->addr.idx=pg->idx++;
		}
		if ((pg->lPins+=ceil(pin->length,HP_ALIGN))>xbuf) xbuf=pg->lPins;
		pin->sibling=pg->pins; pg->pins=pin; pin->mode|=COMMIT_ALLOCATED;
		if ((pin->meta&(PMT_CLASS|PMT_TIMER|PMT_LISTENER|PMT_LOADER))!=0 && (pin->mode&PIN_DELETED)==0) metaPINs[nMetaPINs++]=pin;
	}
	if (pages!=NULL && pages->next==NULL) ectx.ses->setAtomic();
	if (!fNewPgOnly && nNewPages>0) for (AllocPage **ppg=&pages,*pg; (pg=*ppg)!=0; ppg=&pg->next) if ((pg->flags&PGF_NEW)!=0) {
		pb=ctx->heapMgr->getPartialPage(xSize-pg->spaceLeft); assert(pg->pid==INVALID_PAGEID);
		if (!pb.isNull()) {
			if (pg!=pages) {*ppg=pg->next; pg->next=pages; pages=pg;}
			const HeapPageMgr::HeapPage *hp=(const HeapPageMgr::HeapPage *)pb->getPageBuf();
			pg->pid=pb->getPageID(); pg->flags&=~PGF_NEW; --nNewPages; pin=pg->pins;
			if ((pg->idx=hp->freeSlots)!=0) for (pg->flags|=PGF_SLOTS; pin!=NULL; pin=pin->sibling) {
				pin->addr.pageID=pg->pid; pin->addr.idx=pg->idx>>1;
				// add locks
				if ((pg->idx=(*hp)[pin->addr.idx])==0xFFFF) break;
			}
			for (pg->idx=hp->nSlots; pin!=NULL; pin=pin->sibling) {
				pin->addr.pageID=pg->pid; pin->addr.idx=pg->idx++;
				// add locks
			}
		}
		break;
	}
	if (nNewPages>0) {
		SubAlloc::SubMark mrk; mem.mark(mrk);
		PageID *pids=(PageID*)mem.malloc(nNewPages*sizeof(PageID)); rc=RC_NORESOURCES; unsigned i=0;
		if (pids==NULL || (rc=ctx->fsMgr->allocPages(nNewPages,pids))!=RC_OK || (rc=ectx.ses->addToHeap(pids,nNewPages,false))!=RC_OK) {
			for (i=0; i<nPins; i++) if ((pin=pins[i])!=NULL && (pin->mode&COMMIT_ALLOCATED)!=0)
				{pin->mode&=~(COMMIT_ALLOCATED|COMMIT_RESERVED|COMMIT_MIGRATE); pin->addr.pageID=INVALID_PAGEID; pin->addr.idx=INVALID_INDEX;}
			goto finish;
		}
		for (AllocPage *pg=pages; pg!=NULL && i<nNewPages; pg=pg->next) if ((pg->flags&PGF_NEW)!=0)
			{pg->pid=pids[i++]; for (pin=pg->pins; pin!=NULL; pin=pin->sibling) pin->addr.pageID=pg->pid;}
		mem.truncate(mrk);
	}

	if (pSize!=NULL) *pSize=totalSize;

	SubAlloc::SubMark mrk; mem.mark(mrk);
	if ((buf=(byte*)mem.malloc(xbuf))==NULL) {rc=RC_NORESOURCES; goto finish;}
	for (AllocPage *pg=pages; pg!=NULL; pg=pg->next) {
		assert(pg->pid!=INVALID_PAGEID);
		if (pb.isNull() || pb->getPageID()!=pg->pid) {
			if ((pg->flags&PGF_FORCED)==0) (pg->flags&PGF_NEW)==0 ? pb.getPage(pg->pid,ctx->heapMgr,PGCTL_XLOCK,ectx.ses):pb.newPage(pg->pid,ctx->heapMgr,0,ectx.ses);
			else if (pg->pid==ectx.ses->forcedPage || !ectx.ses->isRestore() && !ctx->fsMgr->isFreePage(pg->pid)) pb.getPage(pg->pid,ctx->heapMgr,PGCTL_XLOCK,ectx.ses);
			else if (!ectx.ses->isRestore() && (rc=ctx->fsMgr->reservePage(pg->pid))!=RC_OK) goto finish;
			else pb.newPage(pg->pid,ctx->heapMgr,0,ectx.ses);
			if (pb.isNull()) {rc=ectx.ses->isRestore()?RC_NOTFOUND:RC_NORESOURCES; break;}
		}
		hp=(HeapPageMgr::HeapPage *)pb->getPageBuf(); size_t lrec=0; PageIdx prevIdx=0,startIdx=0;
		for (pin=pg->pins; pin!=NULL; pin=pin->sibling) {
			assert(pin->addr.pageID==pb->getPageID() && (pin->mode&(COMMIT_ALLOCATED|PIN_TRANSIENT|PIN_INMEM))==COMMIT_ALLOCATED);
			if ((pin->meta&PMT_NAMED)!=0) {
				pv=pin->findProperty(PROP_SPEC_OBJID); assert(pv!=NULL && pv->type==VT_URIID);
				PID id=pin->id; if (id.pid==STORE_INVALID_PID) {id.pid=pin->addr; id.ident=STORE_OWNER;}
				PINRef pr(ctx->storeID,id,pin->addr); if ((rc=ctx->namedMgr->update(pv->uid,pr,pin->meta,true))!=RC_OK) goto finish;
			}
			// check other UNIQUE/IDEMPOTENT
			if ((pin->mode&COMMIT_FORCED)!=0) {
				if (!pin->addr.convert(pin->id.pid)) {rc=RC_INVPARAM; goto finish;}
				if (!hp->isFreeSlot(pin->addr.idx)) {rc=RC_ALREADYEXISTS; goto finish;}
			}
			if (lrec==0) startIdx=pin->addr.idx;
			else if (prevIdx+1!=pin->addr.idx) {
				if ((rc=ctx->txMgr->update(pb,ctx->heapMgr,(unsigned)startIdx<<HPOP_SHIFT|HPOP_INSERT,buf,lrec))!=RC_OK) break;
				lrec=0; startIdx=pin->addr.idx;
			}
			prevIdx=pin->addr.idx; byte *pbuf=buf+lrec;
			ushort lxtab=hp->nSlots>pin->addr.idx?0:(pin->addr.idx-hp->nSlots+1)*sizeof(PageOff);
			if (pin->length+lxtab>hp->totalFree() && ((rc=RC_PAGEFULL,pin->mode&COMMIT_FORCED)==0||(rc=makeRoom(pin,lxtab,pb,ectx.ses,reserve))!=RC_OK)) goto finish;

			HeapPageMgr::HeapPIN *hpin=(HeapPageMgr::HeapPIN*)pbuf; hpin->hdr.descr=HO_PIN; nInserted++;
			hpin->nProps=ushort(pin->nProperties); hpin->fmtExtra=HDF_COMPACT; hpin->meta=pin->meta;
			if ((mode&MODE_TEMP_ID)!=0) hpin->hdr.descr|=HOH_TEMP_ID;
			if ((pin->mode&PIN_HIDDEN)!=0) hpin->hdr.descr|=HOH_HIDDEN;
			else {
				if ((pin->mode&PIN_NOTIFY)!=0) hpin->hdr.descr|=HOH_NOTIFICATION;
				if ((pin->mode&COMMIT_FTINDEX)!=0) hpin->hdr.descr|=HOH_FT;
			}
			if ((pin->mode&PIN_NO_REPLICATION)!=0) hpin->hdr.descr|=HOH_NOREPLICATION;
			else if ((pin->mode&PIN_REPLICATED)!=0) hpin->hdr.descr|=HOH_REPLICATED;
			if ((pin->mode&PIN_IMMUTABLE)!=0) hpin->hdr.descr|=HOH_IMMUTABLE;
			if ((pin->mode&PIN_DELETED)!=0) hpin->hdr.descr|=HOH_DELETED;
			if ((pin->mode&COMMIT_PARTS)!=0) hpin->hdr.descr|=HOH_PARTS;
			if ((pin->mode&COMMIT_SSVS)!=0) hpin->hdr.descr|=HOH_SSVS;

			PageOff sht=hpin->headerLength();
			if (pin->id.pid!=STORE_INVALID_PID) {
				if ((pin->mode&COMMIT_FORCED)==0) {
					__una_set((OID*)(pbuf+sht),pin->id.pid); sht+=sizeof(OID);
					if (pin->id.ident==STORE_OWNER) hpin->fmtExtra=HDF_NORMAL;
					else {__una_set((IdentityID*)(pbuf+sht),pin->id.ident); hpin->fmtExtra=HDF_LONG; sht+=sizeof(IdentityID);}
				} else if ((pin->mode&COMMIT_MIGRATE)!=0 && !isRemote(pin->id)) {
					memcpy(pbuf+sht,&pin->addr,PageAddrSize); hpin->fmtExtra=HDF_SHORT; sht+=PageAddrSize;
				}
			}

			HeapPageMgr::HeapV *hprop=hpin->getPropTab(); ElementID rPrefix=pin->getPrefix(ctx);
			for (unsigned k=0; k<pin->nProperties; ++k,++hprop) {
				Value *pv=&pin->properties[k]; size_t l=pin->length; Value vv; hprop->offset=sht; ElementID keygen=prefix;
				if ((pv->flags&VF_REF)!=0 && (rc=resolveRef(*pv,pins,nPins,&pinMap))!=RC_OK) goto finish;
				if (pv->type!=VT_ARRAY && pv->type!=VT_COLLECTION && pv->eid!=rPrefix && pv->eid!=prefix)
					{vv.set((Value*)pv,1); vv.meta=pv->meta; vv.property=pv->property; pv=&vv;}
				if ((rc=persistValue(*pv,sht,hprop->type,hprop->offset,pbuf+sht,&l,pin->addr,(mode&MODE_FORCE_EIDS)==0?&keygen:NULL))!=RC_OK) goto finish;
				if (pv->eid==prefix && prefix!=rPrefix) hprop->type.flags|=META_PROP_LOCAL;
				if (pv==&vv) hprop->type.setType(VT_ARRAY,HDF_SHORT);
				hprop->type.flags=pv->meta; hprop->setID(pv->property); pin->length=l; sht=(PageOff)ceil(sht,HP_ALIGN);
			}
			hpin->hdr.length=ushort(sht); assert(unsigned(sht)==pin->length);
			if ((pin->mode&COMMIT_MIGRATE)!=0) {
				assert((pin->mode&COMMIT_RESERVED)!=0 && pin->length>sizeof(HeapPageMgr::HeapObjHeader)+PageAddrSize);
				PBlock *pb2=ctx->heapMgr->getNewPage(pin->length+sizeof(PageOff),threshold,ectx.ses); 
				if (pb2==NULL) {rc=RC_FULL; break;}
				const HeapPageMgr::HeapPage *hp2=(HeapPageMgr::HeapPage*)pb2->getPageBuf();
				PageAddr newAddr={pb2->getPageID(),hp2->nSlots}; assert(pin->length+sizeof(PageOff)<=hp2->totalFree());
				rc=ctx->txMgr->update(pb2,ctx->heapMgr,(unsigned)newAddr.idx<<HPOP_SHIFT|HPOP_INSERT,pbuf,(unsigned)pin->length);
				if (rc!=RC_OK) {pb2->release(QMGR_UFORCE,ectx.ses); break;}
				ctx->heapMgr->reuse(pb2,ectx.ses,reserve); pb2->release(QMGR_UFORCE,ectx.ses);
				if (isRemote(pin->id)) {pin->addr=newAddr; if ((rc=ctx->netMgr->insert(pin))!=RC_OK) break; else continue;}
				HeapPageMgr::HeapObjHeader *hobj=(HeapPageMgr::HeapObjHeader*)pbuf; hobj->descr=HO_FORWARD; 
				sht=hobj->length=sizeof(HeapPageMgr::HeapObjHeader)+PageAddrSize; memcpy(hobj+1,&newAddr,PageAddrSize);
			}
			if (pin->id.pid==STORE_INVALID_PID) {const_cast<PID&>(pin->id).pid=pin->addr; const_cast<PID&>(pin->id).ident=STORE_OWNER;}
			else if (isRemote(pin->id) && (mode&MODE_NO_RINDEX)==0 && (rc=ctx->netMgr->insert(pin))!=RC_OK) goto finish;
			lrec+=ceil(sht,HP_ALIGN); assert(lrec<=xbuf);
		}
		if (lrec!=0 && (rc=ctx->txMgr->update(pb,ctx->heapMgr,(unsigned)startIdx<<HPOP_SHIFT|HPOP_INSERT,buf,lrec))!=RC_OK) break;
	}
	mem.truncate(mrk);

finish:
	if (!pb.isNull()) {if (rc==RC_OK) {if (fForced) ectx.ses->forcedPage=pb->getPageID(); else ctx->heapMgr->reuse(pb,ectx.ses,reserve);} pb.release(ectx.ses);}

	ClassResult clr(ectx.ses,ectx.ses->getStore());
	for (i=0; i<nPins; i++) if ((pin=pins[i])!=NULL) {
		bool fProc = rc==RC_OK && (pin->mode&COMMIT_ALLOCATED)!=0; mem.mark(mrk);
		if ((pin->mode&COMMIT_PREFIX)!=0) for (j=0; j<pin->nProperties; j++) {
			Value *pv=&pin->properties[j],*pv2; const Value *cv; unsigned k; Value w; bool fP;
			if ((pv->flags&VF_PREFIX)!=0) switch (pv->type) {
			default: assert(0);
			case VT_STREAM:
				if (pv->stream.prefix==NULL) {
					if (fProc) {uint8_t ty=pv->stream.is->dataType(); freeV(*pv); pv->type=ty; pv->length=0; pv->bstr=(byte*)&pv->length;}
				} else if (fProc && (pv->flags&VF_SSV)==0) {
					pv->length=*(unsigned*)pv->stream.prefix; pv->type=pv->stream.is->dataType();
					memmove(pv->stream.prefix,(byte*)pv->stream.prefix+sizeof(uint32_t),pv->length);
					if ((pv->flags&HEAP_TYPE_MASK)>=SES_HEAP) pv->stream.is->destroy();
					pv->bstr=(byte*)pv->stream.prefix; setHT(*pv,SES_HEAP);
				} else {
					ectx.ses->free(pv->stream.prefix); pv->stream.prefix=NULL;
					if (fProc && loadValue(ectx.ses,pin->id,pv->property,STORE_COLLECTION_ID,w)==RC_OK) {freeV(*pv); *pv=w;}
				}
				break;
			case VT_ARRAY:
			case VT_STRUCT:			//??? recursion
				fP=fProc && (pv->flags&VF_SSV)==0;
				for (k=0; k<pv->length; k++) {if ((pv2=const_cast<Value*>(&pv->varray[k]))->type==VT_STREAM) {
					if (pv2->stream.prefix==NULL) {
						if (fP) {uint8_t ty=pv2->stream.is->dataType(); freeV(*pv2); pv2->type=ty; pv2->length=0; pv2->bstr=NULL;}
					} else if (fP && (pv2->flags&VF_SSV)==0) {
						pv2->length=*(unsigned*)pv2->stream.prefix; pv2->type=pv2->stream.is->dataType();
						memmove(pv2->stream.prefix,(byte*)pv2->stream.prefix+sizeof(uint32_t),pv2->length);
						if ((pv2->flags&HEAP_TYPE_MASK)>=SES_HEAP) pv2->stream.is->destroy(); 
						pv2->bstr=(byte*)pv2->stream.prefix; setHT(*pv2,SES_HEAP);
					} else {
						ectx.ses->free(pv2->stream.prefix); pv2->stream.prefix=NULL;
						if (fP && loadValue(ectx.ses,pin->id,pv->property,pv2->eid,w)==RC_OK) {freeV(*pv2); *pv2=w;}
					}
				}}
				if (fProc && (pv->flags&VF_SSV)!=0 && loadValue(ectx.ses,pin->id,pv->property,STORE_COLLECTION_ID,w,LOAD_ENAV)==RC_OK)
					{freeV(*pv); *pv=w;}
				break;
			case VT_COLLECTION:
				for (cv=pv->nav->navigate(GO_FIRST); cv!=NULL; cv=pv->nav->navigate(GO_NEXT))
					if (cv->type==VT_STREAM && cv->stream.prefix!=NULL) {ectx.ses->free(cv->stream.prefix); cv->stream.prefix=NULL;}
				if (fProc && loadValue(ectx.ses,pin->id,pv->property,STORE_COLLECTION_ID,w,LOAD_ENAV)==RC_OK) {freeV(*pv); *pv=w;}
				break;
			}
		}
		if (fProc) {
			if ((pin->mode&(PIN_TRANSIENT|PIN_DELETED))==0) {
				if ((rc=ctx->classMgr->classify(pin,clr))==RC_OK && into!=NULL) rc=clr.checkConstraints(pin,into,nInto);
				if (rc==RC_OK && clr.nClasses>0 && (rc=ctx->classMgr->index(ectx.ses,pin,clr,CI_INSERT))==RC_OK)
					if (clr.nActions!=0 && (pin->mode&COMMIT_INVOKED)==0 && (rc=clr.invokeActions(pin,CI_INSERT,&ectx))!=RC_OK) break;
				if (rc==RC_OK && (pin->mode&(PIN_DELETED|PIN_HIDDEN|COMMIT_FTINDEX))==COMMIT_FTINDEX) {
					const Value *doc=pin->findProperty(PROP_SPEC_DOCUMENT); SubAlloc sa(ectx.ses); FTList ftl(sa);
					ChangeInfo inf={pin->id,doc==NULL?PIN::defPID:doc->type==VT_REF?doc->pin->getPID():
							doc->type==VT_REFID?doc->id:PIN::defPID,NULL,NULL,STORE_INVALID_URIID,STORE_COLLECTION_ID};
					for (unsigned k=0; k<pin->nProperties; ++k) {
						const Value& pv=pin->properties[k]; 
						if ((pv.meta&META_PROP_FTINDEX)!=0) {	// VF_STRING???
							inf.propID=pv.property; inf.newV=&pv;
							rc=ctx->ftMgr->index(inf,&ftl,IX_NFT,/*(pv.meta&META_PROP_STOPWORDS)!=0?*/FTMODE_STOPWORDS,ectx.ses);
							if (rc!=RC_OK) break;
						}
					}
					if (rc==RC_OK) rc=ctx->ftMgr->process(ftl,inf.id,inf.docID);
				}
			}
			if (rc==RC_OK && (pin->mode&(PIN_DELETED|PIN_HIDDEN))==0) {
//				if (replication!=NULL && (pin->mode&(PIN_NO_REPLICATION|PIN_REPLICATED))==PIN_REPLICATED) rc=ectx.ses->replicate(pin);
				if (rc==RC_OK && ctx->queryMgr->notification!=NULL && ((pin->mode&PIN_NOTIFY)!=0||(clr.notif&CLASS_NOTIFY_NEW)!=0)) {
					IStoreNotification::NotificationEvent evt={pin->id,NULL,0,NULL,0,ectx.ses->isReplication()};
					if ((evt.events=(IStoreNotification::EventData*)mem.malloc((clr.nClasses+1)*sizeof(IStoreNotification::EventData)))!=NULL) {
						if ((clr.notif&CLASS_NOTIFY_NEW)!=0) for (unsigned i=0; i<clr.nClasses; i++) if ((clr.classes[i]->notifications&CLASS_NOTIFY_NEW)!=0) {
							IStoreNotification::EventData& ev=(IStoreNotification::EventData&)evt.events[evt.nEvents++];
							ev.type=IStoreNotification::NE_CLASS_INSTANCE_ADDED; ev.cid=clr.classes[i]->cid;
						}
						if ((pin->mode&PIN_NOTIFY)!=0) {
							IStoreNotification::EventData& ev=(IStoreNotification::EventData&)evt.events[evt.nEvents];
							ev.cid=STORE_INVALID_CLASSID; ev.type=IStoreNotification::NE_PIN_CREATED; evt.nEvents++;
						}
						if (evt.nEvents>0) {
							evt.nData=pin->nProperties;
							evt.data=(IStoreNotification::NotificationData*)mem.malloc(evt.nData*sizeof(IStoreNotification::NotificationData));
							if (evt.data!=NULL) for (unsigned i=0; i<evt.nData; i++) {
								const Value &pv=pin->properties[i]; 
								IStoreNotification::NotificationData& nd=const_cast<IStoreNotification::NotificationData&>(evt.data[i]);
								nd.propID=pv.property; nd.epos=STORE_COLLECTION_ID; nd.eid=pv.eid; nd.oldValue=NULL; nd.newValue=&pv;
							}
						}
						if (evt.nEvents>0) try {ctx->queryMgr->notification->notify(&evt,1,ectx.ses->getTXID());} catch (...) {}
					}
				}
			}
		}
		pin->mode&=~COMMIT_MASK; clr.nClasses=clr.nIndices=clr.notif=0; mem.truncate(mrk); if (rc!=RC_OK) pin->addr=PageAddr::invAddr;
	}
	if (rc==RC_OK && metaPINs!=NULL && nMetaPINs>0 && (rc=ctx->classMgr->classifyAll(metaPINs,nMetaPINs,ectx.ses))==RC_OK) for (unsigned i=0; i<nMetaPINs; i++) try {
		PIN *pin=metaPINs[i];
		if ((pin->meta&PMT_LOADER)!=0) {
			LoadService *ls=new(ectx.ses) LoadService(pin->properties,pin->nProperties,ectx.ses);
			if (ls!=NULL) ectx.ses->addOnCommit(ls); else {rc=RC_NORESOURCES; break;}
		}
		if ((pin->meta&PMT_LISTENER)!=0) {
			StartListener *sl=new(ectx.ses) StartListener(pin->properties,pin->nProperties,ectx.ses);
			if (sl!=NULL) ectx.ses->addOnCommit(sl); else {rc=RC_NORESOURCES; break;}
		}
		if ((pin->meta&PMT_TIMER)!=0) {
			uint32_t id; uint64_t itv; const Value *act=pin->findProperty(PROP_SPEC_ACTION),*pv;
			pv=pin->findProperty(PROP_SPEC_OBJID); assert(pv!=NULL&&pv->type==VT_URIID); id=pv->uid;
			pv=pin->findProperty(PROP_SPEC_INTERVAL); assert(pv!=NULL&&pv->type==VT_INTERVAL); itv=pv->i64;
			StartTimer *st=new(ectx.ses) StartTimer(id,itv,pin->id,act,ectx.ses);
			if (st!=NULL) ectx.ses->addOnCommit(st); else {rc=RC_NORESOURCES; break;}
		}
		if ((pin->meta&PMT_FSMCTX)!=0) {
			Value w; if ((pin->mode&PIN_INMEM)!=0) w.set(pin); else w.set(pin->id);
			StartFSM *sf=new(ectx.ses) StartFSM(w);
			if (sf!=NULL) ectx.ses->addOnCommit(sf); else {rc=RC_NORESOURCES; break;}
		}
	} catch (RC rc2) {rc=rc2;}
	if (rc==RC_OK) {tx.ok(); ectx.ses->nTotalIns+=nInserted; ectx.ses->tx.nInserted+=nInserted;}
	return rc;
}

RC QueryPrc::calcLength(const Value& v,size_t& res,unsigned mode,size_t threshold,MemAlloc *ma,PageID pageID,size_t *rlen)
{
	unsigned i; size_t l,len; PID id; ValueType ty; PageAddr addr; const Value *pv,*pv2; RC rc;
	v.flags&=HEAP_TYPE_MASK;
	switch (v.type) {
	default: return RC_TYPE;
	case VT_INT: res=v.i>=-32768&&v.i<=32767?0:sizeof(int32_t); break;
	case VT_UINT: res=v.ui<=0xFFFF?0:sizeof(uint32_t); break;
	case VT_INT64: case VT_INTERVAL: res=v.i64>=-32768&&v.i64<=32767?0:sizeof(int64_t); break;
	case VT_DATETIME: case VT_UINT64: res=v.ui64<=0xFFFF?0:sizeof(uint64_t); break;
	case VT_ENUM: res=sizeof(uint32_t)*2; break;
	case VT_FLOAT: res=sizeof(float); if (v.qval.units!=Un_NDIM) res+=sizeof(uint16_t); break;
	case VT_DOUBLE: res=sizeof(double); if (v.qval.units!=Un_NDIM) res+=sizeof(uint16_t); break;
	case VT_BOOL: res=0; break;
	case VT_URIID: res=v.uid<=0xFFFF?0:sizeof(URIID); break;
	case VT_IDENTITY: res=v.iid<=0xFFFF?0:sizeof(IdentityID); break;
	case VT_STRING: v.flags|=VF_STRING;
	case VT_BSTR: case VT_URL:
		if ((len=v.length==0?0:v.length+((v.length&~0xff)!=0?2:1))<=threshold) res=len;
		else {
			v.flags|=VF_SSV;
			res=v.length<=HeapPageMgr::contentSize(ctx->bufMgr->getPageSize())-sizeof(PageOff)-sizeof(HeapPageMgr::HeapObjHeader)?
					sizeof(HRefSSV):sizeof(HLOB);
		}
		break;
	case VT_REFID:
		if (v.id.pid==STORE_INVALID_PID) return RC_INVPARAM;
		if (v.id.ident==STORE_INVALID_IDENTITY) {res=PageAddrSize; v.flags|=VF_REF;}
		else if (isRemote(v.id)) res=v.id.ident==STORE_OWNER?sizeof(OID):sizeof(OID)+sizeof(IdentityID);
		else if (!addr.convert(v.id.pid)||addr.pageID!=pageID) res=PageAddrSize;
		else {if (rlen!=NULL) *rlen+=PageAddrSize; res=0;}
		break;
	case VT_REFIDPROP:
		if (v.refId->id.pid==STORE_INVALID_PID) return RC_INVPARAM;
		if (v.refId->id.ident==STORE_INVALID_IDENTITY) {res=PageAddrSize+sizeof(PropertyID); v.flags|=VF_REF;}
		else res=!isRemote(v.refId->id)?PageAddrSize+sizeof(PropertyID):
										v.refId->id.ident==STORE_OWNER?sizeof(OID)+sizeof(PropertyID):
										sizeof(OID)+sizeof(IdentityID)+sizeof(PropertyID);
		break;
	case VT_REFIDELT:
		if (v.refId->id.pid==STORE_INVALID_PID) return RC_INVPARAM;
		if (v.refId->id.ident==STORE_INVALID_IDENTITY) {res=PageAddrSize+sizeof(PropertyID)+sizeof(ElementID); v.flags|=VF_REF;}
		else res=!isRemote(v.refId->id)?PageAddrSize+sizeof(PropertyID)+sizeof(ElementID):
							v.refId->id.ident==STORE_OWNER?sizeof(OID)+sizeof(PropertyID)+sizeof(ElementID):
							sizeof(OID)+sizeof(IdentityID)+sizeof(PropertyID)+sizeof(ElementID);
		break;
	case VT_REF:
		id=((PIN*)v.pin)->id;
		if (!((PIN*)v.pin)->addr.defined() && (id.pid==STORE_INVALID_PID||id.ident==STORE_INVALID_IDENTITY)) v.flags|=VF_REF;
		if (id.ident==STORE_INVALID_IDENTITY) res=PageAddrSize;
		else if (isRemote(id)) res=id.ident==STORE_OWNER?sizeof(OID):sizeof(OID)+sizeof(IdentityID);
		else if (!addr.convert(id.pid)||addr.pageID!=pageID) res=PageAddrSize;
		else {if (rlen!=NULL) *rlen+=PageAddrSize; res=0;}
		break;
	case VT_REFPROP:
		id=((PIN*)v.ref.pin)->id; 
		if (!((PIN*)v.ref.pin)->addr.defined() && (id.pid==STORE_INVALID_PID||id.ident==STORE_INVALID_IDENTITY)) v.flags|=VF_REF;
		res=id.ident==STORE_INVALID_IDENTITY||!isRemote(id)?PageAddrSize+sizeof(PropertyID):
			id.ident==STORE_OWNER?sizeof(OID)+sizeof(PropertyID):sizeof(OID)+sizeof(IdentityID)+sizeof(PropertyID);
		break;
	case VT_REFELT:
		id=((PIN*)v.ref.pin)->id;
		if (!((PIN*)v.ref.pin)->addr.defined() && (id.pid==STORE_INVALID_PID||id.ident==STORE_INVALID_IDENTITY)) v.flags|=VF_REF;
		res=id.ident==STORE_INVALID_IDENTITY||!isRemote(id)?PageAddrSize+sizeof(PropertyID)+sizeof(ElementID):
							id.ident==STORE_OWNER?sizeof(OID)+sizeof(PropertyID)+sizeof(ElementID):
							sizeof(OID)+sizeof(IdentityID)+sizeof(PropertyID)+sizeof(ElementID);
		break;
	case VT_ARRAY:
	case VT_STRUCT:
		if (v.varray==NULL) return RC_INVPARAM;
		res=sizeof(HeapPageMgr::HeapVV)-sizeof(HeapPageMgr::HeapV); if (v.type==VT_ARRAY) res+=sizeof(HeapPageMgr::HeapKey);
		for (i=0; i<v.length; ++i) {
			if ((rc=calcLength(v.varray[i],l,mode,threshold,ma,pageID,rlen))!=RC_OK) return rc; v.flags|=v.varray[i].flags&(VF_STRING|VF_REF|VF_PREFIX);
			if ((res+=ceil(l,HP_ALIGN)+sizeof(HeapPageMgr::HeapV))>threshold) {v.flags|=VF_SSV; res=v.type==VT_STRUCT?sizeof(HRefSSV):sizeof(HeapPageMgr::HeapExtCollection); break;}
		}
		break;
	case VT_COLLECTION:
		if (v.nav==NULL) return RC_INVPARAM; if ((pv=v.nav->navigate(GO_FIRST))==NULL) return RC_EOF;
		res=sizeof(HeapPageMgr::HeapVV)-sizeof(HeapPageMgr::HeapV)+sizeof(HeapPageMgr::HeapKey);
		do {
			if ((rc=calcLength(*pv,l,mode,threshold,ma,pageID,rlen))!=RC_OK) return rc; v.flags|=pv->flags&(VF_STRING|VF_REF|VF_PREFIX);
			if ((res+=ceil(l,HP_ALIGN)+sizeof(HeapPageMgr::HeapV))>threshold) {v.flags|=VF_SSV; res=sizeof(HeapPageMgr::HeapExtCollection); break;}
		} while ((pv=v.nav->navigate(GO_NEXT))!=NULL);
		break;
	case VT_MAP:
		if (v.map==NULL) return RC_INVPARAM; res=sizeof(HeapPageMgr::HeapVV)-sizeof(HeapPageMgr::HeapV);
		if ((rc=v.map->getNext(pv,pv2,true))==RC_OK) do {
			if ((rc=calcLength(*pv,l,mode,threshold,ma,pageID,rlen))!=RC_OK) return rc; v.flags|=pv->flags&(VF_STRING|VF_REF|VF_PREFIX);
			if ((rc=calcLength(*pv2,len,mode,threshold,ma,pageID,rlen))!=RC_OK) return rc; v.flags|=pv2->flags&(VF_STRING|VF_REF|VF_PREFIX);
			if ((res+=ceil(l,HP_ALIGN)+ceil(len,HP_ALIGN)+sizeof(HeapPageMgr::HeapV)*2)>threshold) {v.flags|=VF_SSV; res=sizeof(HeapPageMgr::HeapExtCollection); break;}
		} while ((rc=v.map->getNext(pv,pv2))==RC_OK);
		if (rc!=RC_EOF) return rc;
		break;
	case VT_STREAM:
		if (v.stream.is==NULL) return RC_INVPARAM;
		if ((mode&MODE_PREFIX_READ)==0) {
			len=HeapPageMgr::contentSize(ctx->bufMgr->getPageSize())-sizeof(PageOff)-sizeof(HeapPageMgr::HeapObjHeader);
			if ((v.stream.prefix=ma->malloc(len+sizeof(uint32_t)*2))==NULL) return RC_NORESOURCES;
			ty=v.stream.is->dataType(); if (ty==VT_STRING) v.flags|=VF_STRING; v.flags|=VF_PREFIX;
			try {l=v.stream.is->read((byte*)v.stream.prefix+sizeof(uint32_t),len+sizeof(uint32_t));}
			catch (RC rc) {return rc;} catch (...) {return RC_INVPARAM;}
			if (l==0) {
				ma->free(v.stream.prefix); v.stream.prefix=NULL; res=0;
			} else {
				*(uint32_t*)v.stream.prefix=uint32_t(l);
				if (l>len) {v.flags|=VF_SSV; res=sizeof(HLOB);}
				else {
					v.stream.prefix=ma->realloc(v.stream.prefix,l+sizeof(uint32_t),len+sizeof(uint32_t)*2); assert(v.stream.prefix!=NULL);
					l+=l>0xff?2:1; res=l<=threshold?l:(v.flags|=VF_SSV,sizeof(HRefSSV));
				}
			}
		} else if (v.stream.prefix==NULL) res=0;
		else {
			l=*(unsigned*)v.stream.prefix;
			len=HeapPageMgr::contentSize(ctx->bufMgr->getPageSize())-sizeof(PageOff)-sizeof(HeapPageMgr::HeapObjHeader);
			res=(v.flags&VF_SSV)==0?l+(l>0xff?2:1):l>len?sizeof(HLOB):sizeof(HRefSSV);
		}
		break;
	case VT_EXPRTREE: return RC_INTERNAL;
	case VT_EXPR:
		if (v.expr==NULL) return RC_INVPARAM;
		len=((Expr*)v.expr)->serSize(); res=len<=threshold?len:(v.flags|=VF_SSV,
			len<=HeapPageMgr::contentSize(ctx->bufMgr->getPageSize())-sizeof(PageOff)-sizeof(HeapPageMgr::HeapObjHeader)?sizeof(HRefSSV):sizeof(HLOB));
		break;
	case VT_STMT:
		if (v.stmt==NULL) return RC_INVPARAM;
		len=((Stmt*)v.stmt)->serSize(); res=len<=threshold?afy_len16(len)+len:(v.flags|=VF_SSV,
			len<=HeapPageMgr::contentSize(ctx->bufMgr->getPageSize())-sizeof(PageOff)-sizeof(HeapPageMgr::HeapObjHeader)?sizeof(HRefSSV):sizeof(HLOB));
		break;
	// for prototype
	case VT_CURRENT: res=0; break;
	case VT_VARREF: res=sizeof(RefV); break;
	}
	return RC_OK;
}

RC QueryPrc::resolveRef(Value& v,PIN *const *pins,unsigned nPins,PINMap *map)
{
	unsigned k; const Value *pv,*pv2; RC rc;
	switch (v.type) {
	default: break;
	case VT_REFID:
		if (v.id.ident==STORE_INVALID_IDENTITY && v.id.pid!=STORE_INVALID_PID) {
			const PINMapElt *pe=map!=NULL?map->find(v.id.pid):NULL; if (pe==NULL) return RC_NOTFOUND;
			if ((v.id=pe->pin->id).pid==STORE_INVALID_PID || v.id.ident==STORE_INVALID_IDENTITY) 
				{assert(pe->pin->addr.defined()); v.id.pid=pe->pin->addr; v.id.ident=STORE_OWNER;}
		}
		break;
	case VT_REFIDPROP: case VT_REFIDELT:
		if (v.refId->id.ident==STORE_INVALID_IDENTITY && v.refId->id.pid!=STORE_INVALID_PID) {
			const PINMapElt *pe=map!=NULL?map->find(v.refId->id.pid):NULL; if (pe==NULL) return RC_NOTFOUND;
			if ((*(PID*)&v.refId->id=pe->pin->id).pid==STORE_INVALID_PID || v.refId->id.ident==STORE_INVALID_IDENTITY) 
				{assert(pe->pin->addr.defined()); ((PID*)&v.refId->id)->pid=pe->pin->addr; ((PID*)&v.refId->id)->ident=STORE_OWNER;}
		}
		break;
	case VT_REF:
		if (!v.pin->isPersistent()) {
			for (k=0;;k++) if (k>=nPins) return RC_NOTFOUND; 
			else if (pins[k]==v.pin) {pins[k]->mode|=COMMIT_REFERRED; break;}
		}
		break;
	case VT_REFPROP:
	case VT_REFELT:
		if (!v.ref.pin->isPersistent()) {
			for (k=0;;k++) if (k>=nPins) return RC_NOTFOUND; 
			else if (pins[k]==v.ref.pin) {pins[k]->mode|=COMMIT_REFERRED; break;}
		}
		break;
	case VT_ARRAY:
	case VT_STRUCT:		// recursion ???
		for (k=0; k<v.length; k++) 
			if ((v.varray[k].flags&VF_REF)!=0 && (rc=resolveRef(*(Value*)&v.varray[k],pins,nPins,map))!=RC_OK) return rc;
		break;
	case VT_COLLECTION:
		for (pv=v.nav->navigate(GO_FIRST); pv!=NULL; pv=v.nav->navigate(GO_NEXT)) {
			switch (pv->type) {
			case VT_REF: case VT_REFPROP: case VT_REFELT: case VT_STRUCT:
				if ((rc=resolveRef(*(Value*)pv,pins,nPins,map))!=RC_OK) return rc;
				break;
			case VT_REFID: case VT_REFIDPROP: case VT_REFIDELT:
				//???
				break;
			}
		}
		break;
	case VT_MAP:
		for (rc=v.map->getNext(pv,pv2,true); rc==RC_OK; rc=v.map->getNext(pv,pv2)) {
			switch (pv->type) {
			case VT_REF: case VT_REFPROP: case VT_REFELT: case VT_STRUCT:
				if ((rc=resolveRef(*(Value*)pv,pins,nPins,map))!=RC_OK) return rc;
				break;
			case VT_REFID: case VT_REFIDPROP: case VT_REFIDELT:
				//???
				break;
			}
			switch (pv2->type) {
			case VT_REF: case VT_REFPROP: case VT_REFELT: case VT_STRUCT:
				if ((rc=resolveRef(*(Value*)pv2,pins,nPins,map))!=RC_OK) return rc;
				break;
			case VT_REFID: case VT_REFIDPROP: case VT_REFIDELT:
				//???
				break;
			}
		}
		break;
	}
	return RC_OK;
}

RC QueryPrc::persistValue(const Value& v,ushort& sht,HType& vt,ushort& offs,byte *buf,size_t *plrec,const PageAddr& addr,ElementID *keygen)
{
	try {
		assert(ceil(buf,HP_ALIGN)==buf);
		PageOff len,l; PID id; PropertyID pid; ElementID eid; PageAddr rAddr;
		HeapPageMgr::HeapVV *hcol; HeapPageMgr::HeapV *helt;
		ValueType ty=(ValueType)v.type; unsigned i; size_t ll; RC rc; ElementID key;
		const byte *p; const Value *pv,*pv2; uint64_t len64; IStream *istr;
		switch (ty) {
		default: return RC_TYPE;
		case VT_INT:
			if (v.i>=-32768&&v.i<=32767) {offs=ushort(v.i); vt.setType(VT_INT,HDF_COMPACT); return RC_OK;}
			__una_set((int32_t*)buf,v.i); len=sizeof(int32_t); vt.setType(VT_INT,HDF_NORMAL); break;
		case VT_UINT: 
			if (v.ui<=0xFFFF) {offs=ushort(v.ui); vt.setType(VT_UINT,HDF_COMPACT); return RC_OK;}
			__una_set((uint32_t*)buf,v.ui); len=sizeof(uint32_t); vt.setType(VT_UINT,HDF_NORMAL); break;
		case VT_INT64: case VT_INTERVAL:
			if (v.i64>=-32768&&v.i64<=32767) {offs=ushort(v.i64); vt.setType(ty,HDF_COMPACT); return RC_OK;}
			__una_set((int64_t*)buf,v.i64); len=sizeof(int64_t); vt.setType(ty,HDF_NORMAL); break;
		case VT_UINT64: case VT_DATETIME:
			if (v.ui64<=0xFFFF) {offs=ushort(v.ui64); vt.setType(ty,HDF_COMPACT); return RC_OK;}
			__una_set((uint64_t*)buf,v.ui64); len=sizeof(uint64_t); vt.setType(ty,HDF_NORMAL); break;
		case VT_ENUM:
			__una_set((uint32_t*)buf,v.enu.enumid); __una_set(((uint32_t*)buf+1),v.enu.eltid); len=sizeof(uint32_t)*2; vt.setType(ty,HDF_NORMAL); break;
		case VT_FLOAT:
			__una_set((float*)buf,v.f); len=sizeof(float);
			if (v.qval.units==Un_NDIM) vt.setType(VT_FLOAT,HDF_NORMAL);
			else {__una_set((uint16_t*)(buf+sizeof(float)),v.qval.units); len+=sizeof(uint16_t); vt.setType(VT_FLOAT,HDF_LONG);}
			break;
		case VT_DOUBLE:
			__una_set((double*)buf,v.d); len=sizeof(double);
			if (v.qval.units==Un_NDIM) vt.setType(VT_DOUBLE,HDF_NORMAL);
			else {__una_set((uint16_t*)(buf+sizeof(double)),v.qval.units); len+=sizeof(uint16_t); vt.setType(VT_DOUBLE,HDF_LONG);}
			break;
		case VT_BOOL:
			vt.setType(VT_BOOL,HDF_COMPACT); offs=ushort(v.b?~0u:0); return RC_OK;
		case VT_URL:
			//if ((v.meta&META_PROP_URIREF)!=0 && ctx->uriMgr->URItoPID(v,id)==RC_OK) goto store_ref;
		case VT_BSTR: case VT_STRING:
			p=v.bstr; ll=v.length; istr=NULL; goto persist_str;
		case VT_STREAM:
			istr=v.stream.is; ty=istr->dataType();
			if (ty!=VT_STRING && ty!=VT_BSTR) return RC_TYPE;
			if (v.stream.prefix==NULL) {vt.setType(ty,HDF_COMPACT); offs=0; return RC_OK;}
			p=(const byte*)v.stream.prefix+sizeof(uint32_t); ll=*(uint32_t*)v.stream.prefix; 
		persist_str:
			if ((v.flags&VF_SSV)==0 || ll==0) {
				if (ll>0xFFFF) {vt.setType(VT_ERROR,HDF_COMPACT); offs=0; return RC_TOOBIG;}
				if (p==NULL || ll==0) {vt.setType(ty,HDF_COMPACT); offs=0; return RC_OK;}
				if ((len=(PageOff)ll)<=0xFF) {buf[0]=byte(len); vt.setType(ty,HDF_SHORT); l=1;}
				else {buf[0]=byte(len>>8); buf[1]=byte(len); vt.setType(ty,HDF_NORMAL); l=2;}
				memcpy(buf+l,p,len); len+=l;
			} else {
				rAddr=addr;
				if ((rc=persistData(istr,p,ll,rAddr,len64))!=RC_OK && rc!=RC_TRUE) return rc;
				HLOB hl={{rAddr.pageID,rAddr.idx,{0,0}},len64}; hl.ref.type.setType(ty,HDF_NORMAL);
				if (rc==RC_OK) {v.flags&=~VF_SSV; len=sizeof(HRefSSV); vt.setType(VT_STREAM,HDF_SHORT);} else {len=sizeof(HLOB); vt.setType(VT_STREAM,HDF_LONG);}
				memcpy(buf,&hl,len);
			}
			break;
		case VT_URIID:
			if (v.uid>STORE_MAX_URIID) return RC_INVPARAM;
			if (v.uid<=0xFFFF) {offs=ushort(v.uid); vt.setType(VT_URIID,HDF_COMPACT); return RC_OK;}
			__una_set((URIID*)buf,v.uid); len=sizeof(URIID); vt.setType(VT_URIID,HDF_NORMAL); break;
		case VT_IDENTITY:
			if (v.iid<=0xFFFF) {offs=ushort(v.iid); vt.setType(VT_IDENTITY,HDF_COMPACT); return RC_OK;}
			__una_set((IdentityID*)buf,v.iid); len=sizeof(IdentityID); vt.setType(VT_IDENTITY,HDF_NORMAL); break;
		case VT_REF: ty=VT_REFID; id=v.pin->getPID();
			if (id.pid==STORE_INVALID_PID || id.ident==STORE_INVALID_IDENTITY) 
				{assert(((PIN*)v.pin)->addr.defined()); id.pid=((PIN*)v.pin)->addr; id.ident=STORE_OWNER;}
			goto store_ref;
		case VT_REFID:
			id=v.id;
		store_ref:
			if (id.ident==STORE_INVALID_IDENTITY || id.pid==STORE_INVALID_PID) return RC_INVPARAM;
			if (!isRemote(id)) {
				if (!rAddr.convert(id.pid) || rAddr.pageID!=addr.pageID) {memcpy(buf,&rAddr,len=PageAddrSize); vt.setType(ty,HDF_SHORT);}
				else {vt.setType(ty,HDF_COMPACT); offs=rAddr.idx; if (plrec!=NULL) *plrec-=PageAddrSize; len=0;}
			} else {
				__una_set((OID*)buf,id.pid); len=sizeof(OID); 
				if (id.ident==STORE_OWNER) vt.setType(ty,HDF_NORMAL);
				else {__una_set((IdentityID*)(buf+sizeof(uint64_t)),id.ident); vt.setType(ty,HDF_LONG); len+=sizeof(IdentityID);}
			}
			break;
		case VT_REFPROP: ty=VT_REFIDPROP; goto check_refv;
		case VT_REFELT: eid=v.ref.eid; ty=VT_REFIDELT;
		check_refv: id=v.ref.pin->getPID(); pid=v.ref.pid;
			if (id.pid==STORE_INVALID_PID || id.ident==STORE_INVALID_IDENTITY) 
				{assert(((PIN*)v.ref.pin)->addr.defined()); id.pid=((PIN*)v.ref.pin)->addr; id.ident=STORE_OWNER;}
			goto store_refv;
		case VT_REFIDPROP: case VT_REFIDELT:
			id=v.refId->id; pid=v.refId->pid; eid=v.refId->eid;
		store_refv:
			if (id.ident==STORE_INVALID_IDENTITY || id.pid==STORE_INVALID_PID) return RC_INVPARAM;
			if (!isRemote(id)) {
				len=PageAddrSize; if (rAddr.convert(id.pid)) memcpy(buf,&rAddr,PageAddrSize); vt.setType(ty,HDF_SHORT);
			} else {
				__una_set((OID*)buf,id.pid); len=sizeof(OID);
				if (id.ident==STORE_OWNER) vt.setType(ty,HDF_NORMAL);
				else {__una_set((IdentityID*)(buf+sizeof(uint64_t)),id.ident); vt.setType(ty,HDF_LONG); len+=sizeof(IdentityID);}
			}
			__una_set((PropertyID*)(buf+len),pid); len+=sizeof(PropertyID);
			if (ty==VT_REFIDELT) {__una_set((ElementID*)(buf+len),eid); len+=sizeof(ElementID);}
			break;
		case VT_STRUCT:
			if (v.varray!=NULL && (v.flags&VF_SSV)!=0) {
				//???
				return RC_INTERNAL;
			}
		case VT_ARRAY:
			if (v.varray==NULL) {vt.setType(VT_ERROR,HDF_COMPACT); offs=0; return RC_INTERNAL;}
		case VT_COLLECTION:
			if ((v.flags&VF_SSV)==0) {
				hcol=(HeapPageMgr::HeapVV*)buf; hcol->cnt=(uint16_t)(ty!=VT_COLLECTION?v.length:v.nav->count()); hcol->fUnord=0; ElementID prevEID=0;
				if (hcol->cnt==0) {vt.setType(VT_ERROR,HDF_COMPACT); offs=0; return RC_INTERNAL;} key=keygen!=NULL?*keygen:ctx->getPrefix();
				len=sizeof(HeapPageMgr::HeapVV)+(hcol->cnt-1)*sizeof(HeapPageMgr::HeapV); if (v.type!=VT_STRUCT) len+=sizeof(HeapPageMgr::HeapKey);
				for (pv=ty!=VT_COLLECTION?v.varray:v.nav->navigate(GO_FIRST),i=0; pv!=NULL; ++i,pv=ty!=VT_COLLECTION?i>=v.length?(Value*)0:&v.varray[i]:v.nav->navigate(GO_NEXT)) {
					helt=&hcol->start[i]; helt->type.flags=pv->meta; helt->offset=len;
					if (v.type==VT_STRUCT) helt->setID(pv->property);
					else {
						if (keygen!=NULL || pv->eid==STORE_COLLECTION_ID) pv->eid=key++;
						else {
							assert(pv->eid!=STORE_FIRST_ELEMENT && pv->eid!=STORE_LAST_ELEMENT);
							if (pv->eid>=key&&((key^pv->eid)&CPREFIX_MASK)==0) key=pv->eid+1;
						}
						helt->setID(pv->eid); if (pv->eid<prevEID) hcol->fUnord=1; prevEID=pv->eid;
					}
					if ((rc=persistValue(*pv,len,helt->type,helt->offset,buf+len,plrec,addr))!=RC_OK) return rc;
					if (!helt->type.isCompact()) len=(PageOff)ceil(len,HP_ALIGN);
				}
				if (v.type==VT_STRUCT) vt.setType(VT_STRUCT,HDF_NORMAL); else {hcol->getHKey()->setKey(key); vt.setType(VT_ARRAY,HDF_NORMAL); if (keygen!=NULL) *keygen=key;}
			} else {
				Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION;
				HeapPageMgr::HeapExtCollection coll(ses); assert(v.type!=VT_STRUCT);
				if ((rc=Collection::persist(v,coll,ses,keygen==NULL))!=RC_OK) return rc;	// maxPages ???
				memcpy(buf,&coll,len=sizeof(HeapPageMgr::HeapExtCollection)); vt.setType(VT_ARRAY,HDF_LONG);
			}
			break;
		case VT_MAP:
			if ((v.flags&VF_SSV)==0) {
				hcol=(HeapPageMgr::HeapVV*)buf; hcol->cnt=(uint16_t)(v.map->count()*2); hcol->fUnord=0;
				if (hcol->cnt==0) {vt.setType(VT_ERROR,HDF_COMPACT); offs=0; return RC_INTERNAL;}
				len=sizeof(HeapPageMgr::HeapVV)+(hcol->cnt-1)*sizeof(HeapPageMgr::HeapV);
				for (rc=v.map->getNext(pv,pv2,true),i=0; rc==RC_OK; i+=2,rc=v.map->getNext(pv,pv2)) {
					helt=&hcol->start[i]; helt->type.flags=pv->meta; helt->offset=len; helt->setID(i/2);
					if ((rc=persistValue(*pv,len,helt->type,helt->offset,buf+len,plrec,addr))!=RC_OK) return rc;
					if (!helt->type.isCompact()) len=(PageOff)ceil(len,HP_ALIGN);
					++helt; helt->type.flags=pv2->meta; helt->offset=len; helt->setID(i/2+1);
					if ((rc=persistValue(*pv2,len,helt->type,helt->offset,buf+len,plrec,addr))!=RC_OK) return rc;
					if (!helt->type.isCompact()) len=(PageOff)ceil(len,HP_ALIGN);
				}
				vt.setType(VT_MAP,HDF_NORMAL);
			} else {
//				Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION;
//				HeapPageMgr::HeapExtCollection coll(ses); assert(v.type!=VT_STRUCT);
//				if ((rc=Collection::persist(v,coll,ses,keygen==NULL))!=RC_OK) return rc;	// maxPages ???
//				memcpy(buf,&coll,len=sizeof(HeapPageMgr::HeapExtCollection)); vt.setType(VT_ARRAY,HDF_LONG);
				return RC_INTERNAL;
			}
			break;
		case VT_EXPRTREE: return RC_INTERNAL;
		case VT_EXPR: case VT_STMT:
			ll=ty==VT_EXPR?((Expr*)v.expr)->serSize():((Stmt*)v.stmt)->serSize();
			if ((v.flags&VF_SSV)!=0) {
				byte *p=(byte*)malloc(ll,SES_HEAP); if (p==NULL) return RC_NORESOURCES;
				if (ty==VT_EXPR) ((Expr*)v.expr)->serialize(p); else ((Stmt*)v.stmt)->serialize(p);
				rAddr=addr; rc=persistData(NULL,p,ll,rAddr,len64);
				free(p,SES_HEAP); if (rc!=RC_OK && rc!=RC_TRUE) return rc;
				HLOB hl={{rAddr.pageID,rAddr.idx,{0,0}},len64}; hl.ref.type.setType(ty,HDF_NORMAL);
				if (rc==RC_OK) {v.flags&=~VF_SSV; len=sizeof(HRefSSV); vt.setType(VT_STREAM,HDF_SHORT);} else {len=sizeof(HLOB); vt.setType(VT_STREAM,HDF_LONG);}
				memcpy(buf,&hl,len);
			} else {
				assert(ll<0xFFFF); len=(PageOff)ll;
				if (ty==VT_EXPR) ((Expr*)v.expr)->serialize(buf);
				else {afy_enc16(buf,len); len+=afy_len16(len); ((Stmt*)v.stmt)->serialize(buf);}
				vt.setType(ty,HDF_NORMAL); 
			}
			break;
	// for prototype
		case VT_CURRENT: offs=ushort(v.ui); vt.setType(VT_CURRENT,HDF_COMPACT); return RC_OK;
		case VT_VARREF: vt.setType(VT_VARREF,HDF_NORMAL); memcpy(buf,&v.refV,sizeof(RefV)); len=sizeof(RefV); break;
		}
		sht+=len; return RC_OK;
	} catch (RC rc) {return rc;} 
	catch (...)  {return RC_INVPARAM;}
}

RC QueryPrc::persistData(IStream *stream,const byte *str,size_t lstr,PageAddr& addr,uint64_t& len64,const PageAddr *lastAddr,PBlockP *lastPB)
{
	len64=0; if (stream==NULL && str==NULL) return RC_INVPARAM;
	byte *buf; RC rc=RC_OK; Session *ses=Session::getSession();
	if (ses==NULL) return RC_NOSESSION; if (!ses->inWriteTx()) return RC_READTX;
	size_t xSize=HeapPageMgr::contentSize(ctx->bufMgr->getPageSize())-sizeof(PageOff);
	size_t l=ceil(lstr,HP_ALIGN)+sizeof(HeapPageMgr::HeapObjHeader); bool fNew=true;
	if (lastAddr==NULL && l<=xSize) {
		PBlockP pb(ctx->ssvMgr->getNewPage(l+sizeof(PageOff),ses,fNew),QMGR_UFORCE); if (pb.isNull()) return RC_FULL;
		const HeapPageMgr::HeapPage *hp=(const HeapPageMgr::HeapPage *)pb->getPageBuf();
		assert(hp->totalFree()>=l+sizeof(PageOff));
		if ((buf=(byte*)ses->malloc(l))==NULL) rc=RC_NORESOURCES;
		else {
			((HeapPageMgr::HeapObjHeader*)buf)->descr=HO_SSVALUE;
			((HeapPageMgr::HeapObjHeader*)buf)->length=ushort(lstr)+sizeof(HeapPageMgr::HeapObjHeader);
			try {
				memcpy(buf+sizeof(HeapPageMgr::HeapObjHeader),str,lstr);
				len64=lstr; addr.pageID=pb->getPageID(); addr.idx=hp->freeSlots!=0?hp->freeSlots>>1:hp->nSlots;
				rc=ctx->txMgr->update(pb,ctx->ssvMgr,(unsigned)addr.idx<<HPOP_SHIFT|HPOP_INSERT,buf,l);
			} catch (RC rc2) {rc=rc2;} catch (...) {rc=RC_INVPARAM;}
			ses->free(buf);
		}
		if (rc==RC_OK) ctx->ssvMgr->reuse(pb,ses,fNew);
		return rc;
	}
	PBlockP pb(ctx->fsMgr->getNewPage(ctx->ssvMgr),QMGR_UFORCE);
	if (pb.isNull()) rc=RC_FULL;
	else if ((buf=(byte*)ses->malloc(xSize))==NULL) rc=RC_NORESOURCES;
	else {
		const HeapPageMgr::HeapPage *hp=(const HeapPageMgr::HeapPage *)pb->getPageBuf();
		HeapPageMgr::HeapLOB *hl=(HeapPageMgr::HeapLOB*)buf; hl->hdr.descr=HO_BLOB; hl->hdr.length=ushort(xSize);
		memcpy(hl->prev,&addr,PageAddrSize); addr.pageID=pb->getPageID(); addr.idx=hp->nSlots;
		for (;;) {
			size_t left=xSize-sizeof(HeapPageMgr::HeapLOB); 
			byte *p=(byte*)(hl+1); PBlock *next;
			if (str!=NULL && lstr>0) try {
				if (left>=lstr) {memcpy(p,str,lstr); left-=lstr; p+=lstr; len64+=lstr; lstr=0; str=NULL;}
				else {memcpy(p,str,left); lstr-=left; str+=left; len64+=left; left=0;}
			} catch (RC rc2) {rc=rc2; break;} catch (...) {rc=RC_INVPARAM; break;} 
			if (left>0 && stream!=NULL) try {size_t l=stream->read(p,left); left-=l; len64+=l;}
			catch (RC rc2) {rc=rc2; break;} catch (...) {rc=RC_INVPARAM; break;}
			hl->hdr.length=ushort(xSize-left); size_t lpiece=ceil(hl->hdr.length,HP_ALIGN);
			PageIdx idx=hp->freeSlots!=0?hp->freeSlots>>1:hp->nSlots;
			if (left!=0) {next=NULL; memcpy(hl->next,lastAddr!=NULL?lastAddr:&PageAddr::invAddr,PageAddrSize);}
			else if ((next=ctx->ssvMgr->getNewPage(lpiece,ses,fNew))==NULL) {rc=RC_FULL; break;}
			else {PageAddr nxt={next->getPageID(),0}; memcpy(hl->next,&nxt,PageAddrSize);}
			if ((rc=ctx->txMgr->update(pb,ctx->ssvMgr,(unsigned)idx<<HPOP_SHIFT|HPOP_INSERT,buf,lpiece))!=RC_OK) 
				{if (next!=NULL) next->release(QMGR_UFORCE,ses); break;}
			if (next==NULL) {rc=RC_TRUE; break;}
			PageAddr prev={pb->getPageID(),idx}; pb.release(ses); 
			memcpy(hl->prev,&prev,PageAddrSize); pb=next;
			hp=(const HeapPageMgr::HeapPage *)pb->getPageBuf();
		}
		if (rc==RC_TRUE) {if (lastPB!=NULL) pb.moveTo(*lastPB); else ctx->ssvMgr->reuse(pb,ses,fNew);}
		ses->free(buf);
	}
	return rc;
}

RC QueryPrc::editData(Session *ses,PageAddr &startAddr,uint64_t& len,const Value& v,PBlockP *pbp,byte *pOld)
{
	assert(v.op==OP_EDIT && v.edit.shift<=len);
	if (ses==NULL) return RC_NOSESSION; if (!ses->inWriteTx()) return RC_READTX;
	PageAddr addr=startAddr,prevAddr=PageAddr::invAddr,nextAddr=PageAddr::invAddr;
	PageIdx idx=INVALID_INDEX; const byte *p=v.edit.bstr;
	size_t left=v.length,lpiece=0,lnew; uint64_t start,epos;
	bool fSSVLOB=false,fAddrChanged=false,fSetAddr=false,fNew=false; PBlockP pb; RC rc=RC_OK;
	byte *buf=NULL,abuf[sizeof(HeapPageMgr::HeapModEdit)+PageAddrSize*2]; size_t lbuf=0;
	if (v.edit.shift==~0ULL) epos=start=len; else epos=(start=v.edit.shift)+v.edit.length;
	for (uint64_t pos=0; pos<epos || pos==epos && left>0; pos+=lpiece) {
		if (addr.pageID==INVALID_PAGEID) {rc=RC_CORRUPTED; break;}
		if (pbp!=NULL && !pbp->isNull()) {
			if ((*pbp)->getPageID()!=addr.pageID) pbp->release(ses); else pbp->moveTo(pb);
		}
		if ((pb.isNull() || pb->getPageID()!=addr.pageID) && pb.getPage(addr.pageID,ctx->ssvMgr,PGCTL_XLOCK,ses)==NULL) {rc=RC_NOTFOUND; break;}
		const HeapPageMgr::HeapPage *hp=(const HeapPageMgr::HeapPage*)pb->getPageBuf();
		const HeapPageMgr::HeapObjHeader *hobj=hp->getObject(hp->getOffset(idx=addr.idx));
		if (hobj==NULL || (hobj->descr&HOH_DELETED)!=0) {rc=RC_NOTFOUND; break;}
		HeapObjType htype=hobj->getType(); ushort lhdr=0;
		if (htype==HO_BLOB) {
			memcpy(&addr,((HeapPageMgr::HeapLOB*)hobj)->next,PageAddrSize); lhdr=sizeof(HeapPageMgr::HeapLOB);
		} else if (htype==HO_SSVALUE) {
			addr.pageID=INVALID_PAGEID; addr.idx=INVALID_INDEX; lhdr=sizeof(HeapPageMgr::HeapObjHeader);
			len=hobj->length-lhdr; if (v.edit.shift==~0ULL) epos=start=len;
		} else {rc=RC_CORRUPTED; break;}
		lpiece=hobj->length-lhdr; if (pos+lpiece<start) continue;
		unsigned startOff=pos<start?unsigned(start-pos):0,lmod=unsigned(epos<pos+lpiece?epos-pos:lpiece)-startOff;
		if (lmod>0 && pOld) {memcpy(pOld,(byte*)hobj+hobj->length-lpiece+startOff,lmod); pOld+=lmod;}
		if (left==0 && lmod==lpiece) {
			assert(htype!=HO_SSVALUE);
			if (prevAddr.defined()) {fSetAddr=true; nextAddr=addr;}
			else {assert(addr.defined()); startAddr=addr; fAddrChanged=true;}
			unsigned l=hobj->length;
			if (l>lbuf && (buf=buf==NULL?(byte*)ses->malloc(lbuf=l):(byte*)ses->realloc(buf,lbuf=l))==NULL)
				{rc=RC_NORESOURCES; break;}
			memcpy(buf,hobj,l); 
			if ((rc=ctx->txMgr->update(pb,ctx->ssvMgr,(unsigned)idx<<HPOP_SHIFT|HPOP_PURGE,buf,l))!=RC_OK) break;
			ctx->ssvMgr->reuse(pb,ses,false,true); pb.release(ses); continue;
		}
		if (htype==HO_SSVALUE && left>lmod && left-lmod>hp->totalFree() &&
								(lnew=unsigned(lpiece+sizeof(HeapPageMgr::HeapObjHeader)+left-lmod+sizeof(PageOff)))<=
															HeapPageMgr::contentSize(ctx->bufMgr->getPageSize())) {
			PBlockP newPB(ctx->ssvMgr->getNewPage(lnew,ses,fNew),QMGR_UFORCE); if (newPB.isNull()) {rc=RC_FULL; break;}
			byte *ssv=(byte*)ses->malloc(lpiece+lhdr);
			if (ssv!=NULL) memcpy(ssv,hobj,lhdr+lpiece); else {rc=RC_NORESOURCES; break;}
			if ((rc=ctx->txMgr->update(pb,ctx->ssvMgr,(unsigned)idx<<HPOP_SHIFT|HPOP_PURGE,ssv,lhdr+lpiece))==RC_OK) {
				Value w,edit; w.set(ssv,uint32_t(lpiece+lhdr)); w.type=v.type; w.flags=SES_HEAP; edit=v; edit.edit.shift+=lhdr;
				if ((rc=Expr::calc(OP_EDIT,w,&edit,2,0,ses))==RC_OK) {
					((HeapPageMgr::HeapObjHeader*)w.bstr)->length=ushort(w.length);
					hp=(const HeapPageMgr::HeapPage*)newPB->getPageBuf(); idx=hp->nSlots;
					if ((rc=ctx->txMgr->update(newPB,ctx->ssvMgr,(unsigned)idx<<HPOP_SHIFT|HPOP_INSERT,w.bstr,w.length))==RC_OK) {
						startAddr.pageID=newPB->getPageID(); startAddr.idx=idx; fAddrChanged=true; 
						ctx->ssvMgr->reuse(newPB,ses,fNew,true); newPB.release(ses);
					}
				}
				ssv=(byte*)w.bstr;
			}
			ses->free(ssv); break;
		}
		if (lmod>0 || left>0 && epos<=pos+lpiece && left<=hp->totalFree()) {
			lnew=left<=lmod+hp->totalFree()?left:lmod;
			size_t l=sizeof(HeapPageMgr::HeapModEdit)+lmod+lnew;
			if (l>lbuf && (buf=buf==NULL?(byte*)ses->malloc(lbuf=l):(byte*)ses->realloc(buf,lbuf=l))==NULL)
				{rc=RC_NORESOURCES; break;}
			HeapPageMgr::HeapModEdit *he=(HeapPageMgr::HeapModEdit*)buf;
			he->shift=ushort(startOff+lhdr-sizeof(HeapPageMgr::HeapObjHeader)); he->dscr=0;
			he->newPtr.len=(ushort)lnew; he->newPtr.offset=sizeof(HeapPageMgr::HeapModEdit);
			he->oldPtr.len=(ushort)lmod; he->oldPtr.offset=he->newPtr.offset+ushort(lnew);
			if (lnew!=0) {memcpy(buf+he->newPtr.offset,p,lnew); p+=lnew; left-=lnew;}
			if (lmod!=0) memcpy(buf+he->oldPtr.offset,(byte*)hobj+hobj->length-lpiece+startOff,lmod);
			if ((rc=ctx->txMgr->update(pb,ctx->ssvMgr,(unsigned)idx<<HPOP_SHIFT|HPOP_EDIT,buf,l))!=RC_OK) break;
		}
		if (left>0 && epos<=pos+lpiece) {
			StreamBuf *psb=NULL; PageAddr newAddr={pb->getPageID(),idx}; byte *tail=NULL; unsigned ltail=0; uint64_t ll;
			if (htype==HO_SSVALUE) {
				if (hp->totalFree()<PageAddrSize*2) {
					byte *ssv=(byte*)ses->malloc(lnew=lhdr+lpiece+PageAddrSize*2); if (ssv==NULL) {rc=RC_NORESOURCES; break;}
					memcpy(ssv,hobj,lhdr+lpiece);
					if ((rc=ctx->txMgr->update(pb,ctx->ssvMgr,(unsigned)idx<<HPOP_SHIFT|HPOP_PURGE,ssv,lhdr+lpiece))==RC_OK) {
						ctx->ssvMgr->reuse(pb,ses,false); pb.release(ses); newAddr=PageAddr::invAddr;
						if ((rc=persistData(NULL,ssv+lhdr,lpiece,newAddr,ll,&PageAddr::invAddr,&pb))==RC_TRUE) rc=RC_OK;
					}
					ses->free(ssv); if (rc!=RC_OK) break;
					htype=HO_BLOB; startAddr=newAddr; 
				}
				fSSVLOB=true;
			}
			if (epos<pos+lpiece) {
				ltail=unsigned(pos+lpiece-epos); 
				if ((tail=(byte*)ses->malloc(ltail+sizeof(HeapPageMgr::HeapModEdit)))==NULL ||
					(psb=new(ses) StreamBuf(tail,ltail,(ValueType)v.type,ses))==NULL) {rc=RC_NORESOURCES; break;}
				memcpy(tail,(byte*)(hobj+1)+lpiece-ltail,ltail);
			} else if ((lnew=hp->totalFree())>(htype==HO_SSVALUE?PageAddrSize*2:0)) {
				if (lnew>=left) lnew=left; else if (htype==HO_SSVALUE) lnew-=PageAddrSize*2;
				size_t l=sizeof(HeapPageMgr::HeapModEdit)+lnew;
				if (l>lbuf && (buf=buf==NULL?(byte*)ses->malloc(lbuf=l):(byte*)ses->realloc(buf,lbuf=l))==NULL)
					{rc=RC_NORESOURCES; break;}
				HeapPageMgr::HeapModEdit *he=(HeapPageMgr::HeapModEdit*)buf;
				he->shift=ushort(lpiece+lhdr-sizeof(HeapPageMgr::HeapObjHeader)); he->dscr=0;
				he->newPtr.len=(ushort)lnew; he->newPtr.offset=sizeof(HeapPageMgr::HeapModEdit);
				he->oldPtr.len=0; he->oldPtr.offset=he->newPtr.offset+ushort(lnew);
				memcpy(buf+he->newPtr.offset,p,lnew); p+=lnew; left-=lnew;
				if ((rc=ctx->txMgr->update(pb,ctx->ssvMgr,(unsigned)idx<<HPOP_SHIFT|HPOP_EDIT,buf,l))!=RC_OK) break;
			}
			if (left>0 && ((rc=persistData(psb,p,left,newAddr,ll,&addr))==RC_OK || rc==RC_TRUE)) {
				HeapPageMgr::HeapModEdit *he=(HeapPageMgr::HeapModEdit*)abuf;
				if (htype==HO_SSVALUE) {
					he->newPtr.len=PageAddrSize*2; he->oldPtr.len=he->shift=0; he->dscr=HO_SSVALUE<<8|HO_BLOB;
					he->newPtr.offset=he->oldPtr.offset=sizeof(HeapPageMgr::HeapModEdit);
					memcpy(abuf+he->newPtr.offset,&PageAddr::invAddr,PageAddrSize);
					memcpy(abuf+he->newPtr.offset+PageAddrSize,&newAddr,PageAddrSize); 
				} else {
					he->newPtr.len=he->oldPtr.len=PageAddrSize; he->dscr=0;
					he->shift=ushort(offsetof(HeapPageMgr::HeapLOB,next)-sizeof(HeapPageMgr::HeapObjHeader));
					he->oldPtr.offset=(he->newPtr.offset=sizeof(HeapPageMgr::HeapModEdit))+PageAddrSize;
					memcpy(abuf+he->newPtr.offset,&newAddr,PageAddrSize); memcpy(abuf+he->oldPtr.offset,&addr,PageAddrSize);
				}
				rc=ctx->txMgr->update(pb,ctx->ssvMgr,(unsigned)idx<<HPOP_SHIFT|HPOP_EDIT,abuf,sizeof(abuf));
			}
			if (psb!=NULL) {
				if (rc==RC_OK) {
					memmove(tail+sizeof(HeapPageMgr::HeapModEdit),tail,ltail);
					HeapPageMgr::HeapModEdit *he=(HeapPageMgr::HeapModEdit*)tail;
					he->newPtr.offset=he->oldPtr.offset=sizeof(HeapPageMgr::HeapModEdit);
					he->newPtr.len=0; he->oldPtr.len=ushort(ltail);
					he->dscr=0; he->shift=ushort(lpiece-ltail+lhdr-sizeof(HeapPageMgr::HeapObjHeader)); 
					rc=ctx->txMgr->update(pb,ctx->ssvMgr,(unsigned)idx<<HPOP_SHIFT|HPOP_EDIT,tail,ltail+sizeof(HeapPageMgr::HeapModEdit));
				}
				psb->destroy();
			}
			break;
		}
		if (!fSetAddr) {prevAddr.pageID=hp->hdr.pageID; prevAddr.idx=idx;}
		else if (!nextAddr.defined()) {nextAddr.pageID=hp->hdr.pageID; nextAddr.idx=idx;}
	}
	if (fSetAddr && rc==RC_OK) {
		assert(prevAddr.defined());
		if (!pb.isNull()) ctx->ssvMgr->reuse(pb,ses,false,true);
		if (pb.getPage(prevAddr.pageID,ctx->ssvMgr,PGCTL_XLOCK,ses)==NULL) rc=RC_NOTFOUND;
		else {
			const HeapPageMgr::HeapPage *hp=(const HeapPageMgr::HeapPage*)pb->getPageBuf();
			const HeapPageMgr::HeapObjHeader *hobj=hp->getObject(hp->getOffset(prevAddr.idx));
			if (hobj==NULL || (hobj->descr&HOH_DELETED)!=0 || hobj->getType()!=HO_BLOB) rc=RC_CORRUPTED;
			else {
				HeapPageMgr::HeapModEdit *he=(HeapPageMgr::HeapModEdit*)abuf;
				he->shift=ushort(offsetof(HeapPageMgr::HeapLOB,next)-sizeof(HeapPageMgr::HeapObjHeader));
				he->newPtr.len=he->oldPtr.len=PageAddrSize; he->dscr=0;
				he->oldPtr.offset=(he->newPtr.offset=sizeof(HeapPageMgr::HeapModEdit))+PageAddrSize;
				memcpy(abuf+he->newPtr.offset,&nextAddr,PageAddrSize);
				memcpy(abuf+he->oldPtr.offset,((HeapPageMgr::HeapLOB*)hobj)->next,PageAddrSize);
				rc=ctx->txMgr->update(pb,ctx->ssvMgr,(unsigned)idx<<HPOP_SHIFT|HPOP_EDIT,abuf,sizeof(abuf));
			}
		}
	}
	ses->free(buf);
	if (rc==RC_OK) {ctx->ssvMgr->reuse(pb,ses,false,true); pb.release(ses); len+=v.length; len-=v.edit.length;}
	return rc!=RC_OK?rc:fSSVLOB?RC_TRUE:fAddrChanged?RC_FALSE:RC_OK;
}

RC QueryPrc::deleteData(const PageAddr& start,Session *ses,PBlockP *pbp)
{
	if (ses==NULL && (ses=Session::getSession())==NULL) return RC_NOSESSION;
	PBlockP pb; PageAddr addr=start; RC rc=RC_OK;
	do {
		if (pbp!=NULL && !pbp->isNull()) {
			if ((*pbp)->getPageID()!=addr.pageID) pbp->release(ses); else pbp->moveTo(pb);
		}
		if ((pb.isNull() || pb->getPageID()!=addr.pageID) && pb.getPage(addr.pageID,ctx->ssvMgr,PGCTL_XLOCK,ses)==NULL) {rc=RC_NOTFOUND; break;}
		const HeapPageMgr::HeapPage *hp=(const HeapPageMgr::HeapPage*)pb->getPageBuf(); PageIdx idx=addr.idx;
		const HeapPageMgr::HeapObjHeader *hobj=hp->getObject(hp->getOffset(idx));
		if (hobj==NULL || (hobj->descr&HOH_DELETED)!=0) {rc=RC_NOTFOUND; break;}
		if (hobj->getType()==HO_BLOB) memcpy(&addr,((HeapPageMgr::HeapLOB*)hobj)->next,PageAddrSize);
		else if (hobj->getType()==HO_SSVALUE) addr.pageID=INVALID_PAGEID;
		else {rc=addr==start?RC_NOTFOUND:RC_CORRUPTED; break;}
		byte *buf=(byte*)ses->malloc(hobj->length);
		if (buf==NULL) {rc=RC_NORESOURCES; break;}
		memcpy(buf,hobj,hobj->length);
		rc=ctx->txMgr->update(pb,ctx->ssvMgr,(unsigned)idx<<HPOP_SHIFT|HPOP_PURGE,buf,hobj->length);
		if (rc==RC_OK) ctx->ssvMgr->reuse(pb,ses,false,true);
		ses->free(buf);
	} while (rc==RC_OK && addr.pageID!=INVALID_PAGEID);
	return rc;
}

int __cdecl PIN::cmpLength(const void *v1, const void *v2)
{
	return cmp3((*(const PIN**)v2)->length,(*(const PIN**)v1)->length);
}

RC QueryPrc::makeRoom(PIN *pin,ushort lxtab,PBlock *pb,Session *ses,size_t reserve)
{
	PageIdx cidx=~0; assert((pin->mode&COMMIT_FORCED)!=0);
	const HeapPageMgr::HeapPage *hp=(const HeapPageMgr::HeapPage*)pb->getPageBuf();
	const HeapPageMgr::HeapPIN *cand=NULL; ushort lcand=sizeof(HeapPageMgr::HeapObjHeader)+PageAddrSize;
	for (int i=hp->nSlots; --i>=0; ) {
		HeapPageMgr::HeapObjHeader *hobj=hp->getObject(hp->getOffset(PageIdx(i)));
		// check affinity
		if (hobj!=NULL && hobj->getType()==HO_PIN && hobj->getLength()>lcand)
			{cand=(HeapPageMgr::HeapPIN*)hobj; lcand=hobj->getLength(); cidx=PageIdx(i);}
	}
	if (cand==NULL) {
		if (hp->totalFree()<sizeof(HeapPageMgr::HeapObjHeader)+PageAddrSize+lxtab) return RC_PAGEFULL;
		pin->mode|=COMMIT_MIGRATE; return RC_OK;
	}
	if (pin->length>=lcand && hp->totalFree()>=sizeof(HeapPageMgr::HeapObjHeader)+PageAddrSize+lxtab)
		{pin->mode|=COMMIT_MIGRATE; return RC_OK;} 		// and check affinity

	const bool fRemote=cand->hasRemoteID(),fMigrated=cand->isMigrated(); PID id; if (fRemote) cand->getAddr(id);
	byte *buf=NULL,*img=NULL,fbuf[sizeof(HeapPageMgr::HeapModEdit)+PageAddrSize*2]; size_t lr=0,limg=0;
	size_t expLen=(cand->hdr.descr&HOH_COMPACTREF)!=0?cand->expLength((const byte*)hp):cand->hdr.getLength(); 
	if (!fMigrated && !fRemote) expLen+=PageAddrSize;
	PBlockP newPB(ctx->heapMgr->getNewPage(expLen+sizeof(PageOff),reserve,ses),QMGR_UFORCE); if (newPB.isNull()) return RC_FULL;
	PageAddr newAddr={newPB->getPageID(),((HeapPageMgr::HeapPage*)newPB->getPageBuf())->nSlots},oldAddr={pb->getPageID(),cidx},origAddr;
	RC rc=cand->serialize(buf,lr,hp,ses,expLen,(cand->hdr.descr&HOH_COMPACTREF)!=0); if (rc!=RC_OK) return rc;
	if (fMigrated) {
		HeapPageMgr::HeapModEdit *hm=(HeapPageMgr::HeapModEdit*)fbuf; 
		hm->dscr=hm->shift=0; hm->oldPtr.len=hm->newPtr.len=PageAddrSize;
		hm->newPtr.offset=(hm->oldPtr.offset=sizeof(HeapPageMgr::HeapModEdit))+PageAddrSize;
		memcpy(fbuf+sizeof(HeapPageMgr::HeapModEdit),&oldAddr,PageAddrSize);
		memcpy(fbuf+sizeof(HeapPageMgr::HeapModEdit)+PageAddrSize,&newAddr,PageAddrSize);
		PageAddr *orig=cand->getOrigLoc(); assert(orig!=NULL); memcpy(&origAddr,orig,PageAddrSize);
	} else if (!fRemote) {
		byte *orig=(byte*)((HeapPageMgr::HeapPIN*)buf)->getOrigLoc();
		if (lr<PageAddrSize+(orig-buf)) return RC_CORRUPTED;
		memmove(orig+PageAddrSize,orig,lr-PageAddrSize-(orig-buf));
		((HeapPageMgr::HeapPIN*)buf)->hdr.length+=PageAddrSize;
		((HeapPageMgr::HeapPIN*)buf)->setOrigLoc(oldAddr);
		HeapPageMgr::HeapV *hprop=((HeapPageMgr::HeapPIN*)buf)->getPropTab();
		for (unsigned i=((HeapPageMgr::HeapPIN*)buf)->nProps; i!=0; ++hprop,--i)
			if (!hprop->type.isCompact()) hprop->offset+=PageAddrSize;
		img=buf; limg=lr;
	}
	if ((rc=ctx->txMgr->update(newPB,ctx->heapMgr,(unsigned)newAddr.idx<<HPOP_SHIFT|HPOP_INSERT,buf,lr))!=RC_OK) return rc;
	if (fRemote && (rc=ctx->netMgr->updateAddr(id,newAddr))!=RC_OK ||
		(rc=cand->serialize(img,limg,hp,ses,cand->hdr.getLength()+(fRemote?PageAddrSize:0)))!=RC_OK) return rc;
	if (!fRemote) memcpy(img+limg-PageAddrSize,&newAddr,PageAddrSize);
	rc=ctx->txMgr->update(pb,ctx->heapMgr,(unsigned)oldAddr.idx<<HPOP_SHIFT|(fRemote?HPOP_PURGE:HPOP_MIGRATE),img,limg);
	if (img!=buf) ses->free(img); if (rc!=RC_OK) return rc;
	ctx->heapMgr->reuse(newPB,ses,reserve);
	if (pin->length+lxtab>hp->totalFree()) pin->mode|=COMMIT_MIGRATE;
	return RC_OK;
}
