/**************************************************************************************

Copyright © 2004-2012 VMware, Inc. All rights reserved.

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
#include "maps.h"

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

namespace AfyKernel
{
	struct AllocPage {
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

RC QueryPrc::commitPINs(Session *ses,PIN *const *pins,unsigned nPins,unsigned mode,const ValueV& params,const AllocCtrl *actrl,size_t *pSize,const ClassSpec *into,unsigned nInto)
{
	if (pins==NULL || nPins==0) return RC_OK; if (ses==NULL) return RC_NOSESSION;
	if (ctx->isServerLocked() || ses->inReadTx()) return RC_READTX;
	if (ses->getIdentity()!=STORE_OWNER && !ses->mayInsert()) return RC_NOACCESS;
	unsigned nClassPINs=0; if (actrl==NULL) actrl=ses->allocCtrl;
	TxSP tx(ses); PBlockP pb; const HeapPageMgr::HeapPage *hp; RC rc=RC_OK;
	size_t totalSize=0,lmin=size_t(~0ULL),lmax=0; ulong cnt=0,j; unsigned i; PIN *pin; byte *buf;
	const size_t xSize=HeapPageMgr::contentSize(ctx->bufMgr->getPageSize())-sizeof(PageOff);
	const size_t reserve=ceil(size_t(xSize*(actrl!=NULL&&actrl->pctPageFree>=0.f?actrl->pctPageFree:ctx->theCB->pctFree)),HP_ALIGN);
	const bool fRepSes=ses->isReplication(),fNewPgOnly=ses->nTotalIns+nPins>INSERT_THRSH; 
	bool fForced=false,fUncommPINs=false; ElementID prefix=ctx->getPrefix(); 
	byte cbuf[START_BUF_SIZE]; SubAlloc mem(ses,START_BUF_SIZE,cbuf,true); size_t threshold=xSize-reserve,xbuf=0;
	AllocPage *pages=NULL,*allocPages=NULL,*forcedPages=NULL; ulong nNewPages=0; PIN **classPINs=NULL; ulong nInserted=0;
	for (i=0; i<nPins; i++) if ((pin=pins[i])!=NULL) {
		pin->mode&=~(COMMIT_MASK|PIN_CLASS); if (pin->addr.defined()) continue;
		if (((pin->mode|=mode&(PIN_NO_REPLICATION|PIN_NO_INDEX|PIN_NOTIFY))&PIN_NO_REPLICATION)!=0) pin->mode&=~PIN_REPLICATED;
		if (pin->id.pid==STORE_INVALID_PID) {if (ses->isRestore()) {rc=RC_INVPARAM; goto finish;}}
		else if (!isRemote(pin->id)) {pin->mode|=COMMIT_FORCED; fForced=true;}
		if ((pin->mode&PIN_HIDDEN)!=0) pin->mode=pin->mode&~(PIN_REPLICATED|PIN_NOTIFY)|(PIN_NO_INDEX|PIN_NO_REPLICATION);
		pin->length = sizeof(HeapPageMgr::HeapPIN) + (pin->id.pid==STORE_INVALID_PID||(pin->mode&COMMIT_FORCED)!=0?0:
												pin->id.ident==STORE_OWNER?sizeof(OID):sizeof(OID)+sizeof(IdentityID));
		size_t lBig=0,lOther=0; ulong nBig=0; ElementID rPrefix=pin->getPrefix(ctx); PropertyID xPropID=STORE_INVALID_PROPID;
		bool fCID=false; size_t len,l; const Value *cv; const Stmt *qry;
		for (j=0; j<pin->nProperties; j++) {
			Value *pv=&pin->properties[j]; PropertyID pid=pv->property;
			assert(j==0 || pin->properties[j-1].property<pid); 
			if (pid<=PROP_SPEC_LAST) switch (pid) {
			default: rc=RC_INVPARAM; goto finish;
			case PROP_SPEC_DOCUMENT:
				if (pv->type!=VT_REF && pv->type!=VT_REFID) {rc=RC_TYPE; goto finish;}
				break;
			case PROP_SPEC_PARENT:
				if (pv->type!=VT_REF && pv->type!=VT_REFID) {rc=RC_TYPE; goto finish;}
				//check mutual link
				pin->mode|=COMMIT_ISPART;
				break;
			case PROP_SPEC_VALUE:
				break;
			case PROP_SPEC_CREATED: 
			case PROP_SPEC_UPDATED:
				if (!fRepSes) {pv->setNow(); pv->property=pid;} else if (pv->type!=VT_DATETIME) {rc=RC_TYPE; goto finish;}
				break;
			case PROP_SPEC_CREATEDBY:
			case PROP_SPEC_UPDATEDBY:
				if (!fRepSes) {pv->setCUser(); pv->property=pid;} else if (pv->type!=VT_IDENTITY) {rc=RC_TYPE; goto finish;}
				break;
			case PROP_SPEC_ACL:
				break;
			case PROP_SPEC_URI:
				if (pv->type!=VT_STRING && pv->type!=VT_URL) {rc=RC_TYPE; goto finish;}
				if (pin->findProperty(PROP_SPEC_PREDICATE)!=NULL) {
					URI *uri=ctx->uriMgr->insert(pv->str); if (uri==NULL) {rc=RC_NORESOURCES; goto finish;}
					ClassID cid=uri->getID(); uri->release();
					if ((cv=pin->findProperty(PROP_SPEC_CLASSID))==NULL) {
						Value cidv; cidv.setURIID(cid); cidv.setPropID(PROP_SPEC_CLASSID); cidv.op=OP_ADD;
						if ((rc=pin->modify(&cidv,STORE_COLLECTION_ID,prefix,MODP_NCPY|MODP_EIDS,ses))!=RC_OK) goto finish;
						pv=&pin->properties[j];
					} else if (cv->type!=VT_URIID) {rc=RC_TYPE; goto finish;}
					else if (cv->uid!=cid) {rc=RC_INVPARAM; goto finish;}
				} else {
					// transalate external PIN uri
				}
				pv->meta|=META_PROP_NOFTINDEX; break;
			case PROP_SPEC_CLASSID:
				if (pv->type!=VT_URIID) {rc=RC_TYPE; goto finish;}
				if (pv->uid==STORE_INVALID_CLASSID ||
					pv->uid!=STORE_CLASS_OF_CLASSES && pv->uid<=PROP_SPEC_LAST) {rc=RC_INVPARAM; goto finish;}
				fCID=true; break;
			case PROP_SPEC_PREDICATE:
				if (!fCID) {rc=RC_INVPARAM; goto finish;}
				if (pv->type==VT_STRING) {
					if ((rc=convV(*pv,*pv,VT_STMT,ses))!=RC_OK) goto finish;
					pv->property=PROP_SPEC_PREDICATE;
				} else if (pv->type!=VT_STMT||pv->stmt==NULL) {rc=RC_TYPE; goto finish;}
				if ((qry=(Stmt*)pv->stmt)->op!=STMT_QUERY || qry->top==NULL || qry->top->getType()!=QRY_SIMPLE || (qry->mode&(QRY_CPARAMS|QRY_PATH))!=0)	// UNION/INTERSECT/EXCEPT ??? -> condSatisfied
					{rc=RC_INVPARAM; goto finish;}
				if ((pin->mode&(PIN_HIDDEN|PIN_DELETED))==0) {
					if ((cv=pin->findProperty(PROP_SPEC_CLASS_INFO))!=NULL) {
						if (cv->type!=VT_UINT && cv->type!=VT_INT) {rc=RC_TYPE; goto finish;}
						((Value*)cv)->ui&=CLASS_SDELETE|CLASS_VIEW|CLASS_CLUSTERED;
						if (qry->isClassOK()) ((Value*)cv)->ui|=CLASS_INDEXED; else ((Value*)cv)->ui&=~CLASS_INDEXED;
					} else if (!qry->isClassOK()) {
						Value civ; civ.set(0u); civ.setPropID(PROP_SPEC_CLASS_INFO); civ.op=OP_ADD;
						if ((rc=pin->modify(&civ,STORE_COLLECTION_ID,prefix,MODP_NCPY|MODP_EIDS,ses))!=RC_OK) goto finish;
						pv=&pin->properties[j]; assert(pv->property==PROP_SPEC_PREDICATE && pv->type==VT_STMT);
					}
					pin->mode|=PIN_CLASS; nClassPINs++;
				}
				break;
			case PROP_SPEC_CLASS_INFO:
				if ((pin->mode&PIN_CLASS)==0) {rc=RC_INVPARAM; goto finish;}
				break;
			} else if (pid>STORE_MAX_URIID) {rc=RC_INVPARAM; goto finish;}
			switch (pv->type) {
			default: break;
			case VT_EXPR: case VT_STMT: if ((pv->meta&META_PROP_EVAL)==0) break;
			case VT_VARREF: case VT_EXPRTREE:
				{PINEx pex(ses),*ppe=&pex; if ((pin->mode&PIN_PINEX)!=0) ppe=(PINEx*)pin; else pex=pin;
				if ((rc=eval(ses,pv,*pv,&ppe,1,&params,1,ses,true))!=RC_OK) goto finish;
				if (pv->type==VT_REF && pv->pin==(PIN*)ppe) pv->pin=pin;}	// VT_REFPROP, VT_REFELT?
				pv->property=pid; break;
			}
			if (pv->type==VT_ARRAY || pv->type==VT_COLLECTION) {
				INav *nav=NULL;
				if (pv->type==VT_ARRAY) cv=pv->varray;
				else if ((nav=pv->nav)==NULL || (cv=nav->navigate(GO_FIRST))==NULL) {
					if (j<--pin->nProperties) memmove(pv,pv+1,(pin->nProperties-j)*sizeof(Value));
					if (pin->nProperties==0) break; else {--j; continue;}
				}
				pv->eid=STORE_COLLECTION_ID; unsigned k=0;
				len=sizeof(HeapPageMgr::HeapVV)-sizeof(HeapPageMgr::HeapV)+sizeof(HeapPageMgr::HeapKey);
				do {
					if (pv->type==VT_ARRAY) switch (cv->type) {
					default: break;
					case VT_EXPR: case VT_STMT: if ((cv->meta&META_PROP_EVAL)==0) break;
					case VT_VARREF: if ((rc=eval(ses,cv,*(Value*)cv,NULL,0,&params,1,ses,true))!=RC_OK) goto finish; //pin->PINex
					}
					if ((rc=estimateLength(*cv,l,0,threshold,ses))!=RC_OK) goto finish;
					if (l>=bigThreshold) {lBig+=l; nBig++;} else lOther+=l;
					len+=ceil(l,HP_ALIGN)+sizeof(HeapPageMgr::HeapV); pv->flags|=cv->flags&(VF_PREFIX|VF_REF|VF_STRING);
					if ((cv->meta&META_PROP_PART)!=0 && (cv->type==VT_REF || cv->type==VT_REFID)) pin->mode|=COMMIT_PARTS;
				} while ((cv=nav!=NULL?nav->navigate(GO_NEXT):++k<pv->length?&pv->varray[k]:(Value*)0)!=NULL);
				if (len>threshold || (pv->meta&META_PROP_SSTORAGE)!=0) {pv->flags|=VF_SSV|VF_PREFIX; len=sizeof(HeapPageMgr::HeapExtCollection);}
			} else {
				if ((rc=estimateLength(*pv,len,0,threshold,ses))!=RC_OK) goto finish;
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
			if ((pv->flags&VF_PREFIX)!=0) pin->mode|=COMMIT_PREFIX;
			if ((pin->mode&PIN_NO_INDEX)==0 && (pv->flags&VF_STRING)!=0 && (pv->meta&META_PROP_NOFTINDEX)==0)
				pin->mode|=COMMIT_FTINDEX;
			if ((pv->flags&VF_REF)!=0) {
				if (!checkRef(*pv,pins,nPins)) {rc=RC_INVPARAM; goto finish;}
				pin->mode|=COMMIT_REFERS;
			}
			pin->length+=ceil(len,HP_ALIGN)+sizeof(HeapPageMgr::HeapV);
			if (pid>PROP_SPEC_LAST && (xPropID==STORE_INVALID_PROPID || pid>xPropID)) xPropID=pid;
		}
		l=pin->length; cnt++; totalSize+=l+sizeof(PageOff); fUncommPINs=true;
		if (pin->length>xSize || (pin->mode&PIN_CLASS)==0 && (double)lBig/nBig>lOther*SKEW_FACTOR) {
			CandidateSSVs cs; 
			if ((rc=findCandidateSSVs(cs,pin->properties,pin->nProperties,pin->length>xSize,ses,actrl))!=RC_OK) goto finish;
			if (cs.nCandidates>0) {
				if (cs.nCandidates>1) qsort(cs.candidates,cs.nCandidates,sizeof(CandidateSSV),cmpCandidateSSV);
				for (j=0; j<cs.nCandidates; j++) {
					CandidateSSV &cd=cs.candidates[j]; assert(cd.length>sizeof(HRefSSV));
					if (pin->length>xSize || cd.length>=bigThreshold) {
						cd.pv->flags|=VF_SSV; pin->length-=cd.length-cd.dlen; pin->mode|=COMMIT_SSVS;
					} else if (cd.length<bigThreshold) break;
				}
			}
			if (pin->length>xSize) {rc=RC_TOOBIG; goto finish;}
		}
		if (pin->length<lmin) lmin=pin->length; if (pin->length>lmax) lmax=pin->length;
		if (xPropID!=STORE_INVALID_PROPID) ctx->classMgr->setMaxPropID(xPropID);
	}

	if (!fUncommPINs || (rc=tx.start(TXI_DEFAULT,TX_IATOMIC))!=RC_OK) goto finish;
	if (nClassPINs>0 && ses->classLocked!=RW_NO_LOCK && ses->classLocked!=RW_X_LOCK) {rc=RC_DEADLOCK; goto finish;}	//???
	xbuf=lmax+PageAddrSize; ses->lockClass(nClassPINs>0?RW_X_LOCK:RW_S_LOCK);
	if (nClassPINs!=0 && (classPINs=(PIN**)mem.malloc(nClassPINs*sizeof(PIN*)))==NULL) {rc=RC_NORESOURCES; goto finish;}
		
	for (i=nClassPINs=0; i<nPins; i++) if ((pin=pins[i])!=NULL && !pin->addr.defined()) {
		AllocPage *pg=NULL; assert((pin->mode&COMMIT_ALLOCATED)==0 && pin->length<=xSize);
		if ((pin->mode&PIN_CLASS)!=0) {
			const Value *cv=pin->findProperty(PROP_SPEC_CLASSID); assert(cv!=NULL && cv->type==VT_URIID);
			Class *cls=ctx->classMgr->getClass(cv->uid);
			if (cls!=NULL) {cls->release(); rc=RC_ALREADYEXISTS; goto finish;}
			classPINs[nClassPINs++]=pin;
		}
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
					if (ses->reuse.pinPages!=NULL && ses->reuse.nPINPages>0 && ses->reuse.pinPages[ses->reuse.nPINPages-1].space>=pin->length+sizeof(PageOff)+reserve)
						{TxReuse::ReusePage &rp=ses->reuse.pinPages[ses->reuse.nPINPages-1]; pid=rp.pid; spc=rp.space; nSlots=rp.nSlots; --ses->reuse.nPINPages; flg=0;} else nNewPages++;
					if ((pg=new(mem.alloc<AllocPage>()) AllocPage(pid,spc,nSlots,flg))==NULL) {rc=RC_NORESOURCES; goto finish;}
					pg->next=pages; pg->next2=allocPages; allocPages=pages=pg; ppg=&allocPages;
				}
			}
			if ((pg->spaceLeft-=pin->length+sizeof(PageOff))<=reserve+lmin) *ppg=pg->next2;
			pin->addr.pageID=pg->pid; pin->addr.idx=pg->idx++;
		}
		if ((pg->lPins+=ceil(pin->length,HP_ALIGN))>xbuf) xbuf=pg->lPins;
		pin->sibling=pg->pins; pg->pins=pin; pin->mode|=COMMIT_ALLOCATED;
	}
	assert(pages!=NULL);
	if (pages->next==NULL) ses->setAtomic();
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
			for (pg->idx=hp->nSlots; pin!=0; pin=pin->sibling) {
				pin->addr.pageID=pg->pid; pin->addr.idx=pg->idx++;
				// add locks
			}
		}
		break;
	}
	if (nNewPages>0) {
		SubAlloc::SubMark mrk; mem.mark(mrk);
		PageID *pids=(PageID*)mem.malloc(nNewPages*sizeof(PageID)); rc=RC_NORESOURCES; i=0;
		if (pids==NULL || (rc=ctx->fsMgr->allocPages(nNewPages,pids))!=RC_OK || (rc=ses->addToHeap(pids,nNewPages,false))!=RC_OK) {
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
			if ((pg->flags&PGF_FORCED)==0) (pg->flags&PGF_NEW)==0 ? pb.getPage(pg->pid,ctx->heapMgr,PGCTL_XLOCK,ses):pb.newPage(pg->pid,ctx->heapMgr,0,ses);
			else if (pg->pid==ses->forcedPage || !ses->isRestore() && !ctx->fsMgr->isFreePage(pg->pid)) pb.getPage(pg->pid,ctx->heapMgr,PGCTL_XLOCK,ses);
			else if (!ses->isRestore() && (rc=ctx->fsMgr->reservePage(pg->pid))!=RC_OK) goto finish;
			else pb.newPage(pg->pid,ctx->heapMgr,0,ses);
			if (pb.isNull()) {rc=ses->isRestore()?RC_NOTFOUND:RC_NORESOURCES; break;}
		}
		hp=(HeapPageMgr::HeapPage *)pb->getPageBuf(); size_t lrec=0; PageIdx prevIdx=0,startIdx=0;
		for (pin=pg->pins; pin!=NULL; pin=pin->sibling) {
			assert(pin->addr.pageID==pb->getPageID() && (pin->mode&COMMIT_ALLOCATED)!=0);
			if ((pin->mode&COMMIT_FORCED)!=0) {
				if (!pin->addr.convert(pin->id.pid)) {rc=RC_INVPARAM; goto finish;}
				if (!hp->isFreeSlot(pin->addr.idx)) {rc=RC_ALREADYEXISTS; goto finish;}
			}
			if (lrec==0) startIdx=pin->addr.idx;
			else if (prevIdx+1!=pin->addr.idx) {
				if ((rc=ctx->txMgr->update(pb,ctx->heapMgr,(ulong)startIdx<<HPOP_SHIFT|HPOP_INSERT,buf,lrec))!=RC_OK) break;
				lrec=0; startIdx=pin->addr.idx;
			}
			prevIdx=pin->addr.idx; byte *pbuf=buf+lrec;
			ushort lxtab=hp->nSlots>pin->addr.idx?0:(pin->addr.idx-hp->nSlots+1)*sizeof(PageOff);
			if (pin->length+lxtab>hp->totalFree() && ((rc=RC_PAGEFULL,pin->mode&COMMIT_FORCED)==0||(rc=makeRoom(pin,lxtab,pb,ses,reserve))!=RC_OK)) goto finish;

			HeapPageMgr::HeapPIN *hpin=(HeapPageMgr::HeapPIN*)pbuf; hpin->hdr.descr=HO_PIN; nInserted++;
			hpin->nProps=ushort(pin->nProperties); hpin->lExtra=hpin->fmtExtra=0;
			if ((mode&MODE_TEMP_ID)!=0) hpin->hdr.descr|=HOH_TEMP_ID;
			if ((pin->mode&PIN_HIDDEN)!=0) hpin->hdr.descr|=HOH_NOREPLICATION|HOH_NOINDEX|HOH_HIDDEN;
			else {
				if ((pin->mode&PIN_NO_REPLICATION)!=0) hpin->hdr.descr|=HOH_NOREPLICATION;
				else if ((pin->mode&PIN_REPLICATED)!=0) hpin->hdr.descr|=HOH_REPLICATED;
				if ((pin->mode&PIN_NOTIFY)!=0) hpin->hdr.descr|=HOH_NOTIFICATION;
				if ((pin->mode&PIN_NO_INDEX)!=0) hpin->hdr.descr|=HOH_NOINDEX;
				if ((pin->mode&PIN_DELETED)!=0) hpin->hdr.descr|=HOH_DELETED;
				if ((pin->mode&PIN_CLASS)!=0) hpin->hdr.descr|=HOH_CLASS;
				if ((pin->mode&COMMIT_FTINDEX)!=0) hpin->hdr.descr|=HOH_FT;
				if ((pin->mode&COMMIT_ISPART)!=0) hpin->hdr.descr|=HOH_ISPART;
				if ((pin->mode&COMMIT_PARTS)!=0) hpin->hdr.descr|=HOH_PARTS;
				if ((pin->mode&COMMIT_SSVS)!=0) hpin->hdr.descr|=HOH_SSVS;
			}

			PageOff sht=hpin->headerLength();
			if (pin->id.pid!=STORE_INVALID_PID) {
				if ((pin->mode&COMMIT_FORCED)==0) {
					memcpy(pbuf+sht,&pin->id.pid,hpin->lExtra=sizeof(OID));
					if (pin->id.ident==STORE_OWNER) hpin->fmtExtra=HDF_NORMAL;
					else {memcpy(pbuf+sht+sizeof(OID),&pin->id.ident,sizeof(IdentityID)); hpin->fmtExtra=HDF_LONG; hpin->lExtra+=sizeof(IdentityID);}
					sht+=hpin->lExtra;
				} else if ((pin->mode&COMMIT_MIGRATE)!=0 && !isRemote(pin->id)) {
					memcpy(pbuf+sht,&pin->addr,PageAddrSize); hpin->fmtExtra=0x80; sht+=hpin->lExtra=PageAddrSize;
				}
			}

			HeapPageMgr::HeapV *hprop=hpin->getPropTab(); ElementID rPrefix=pin->getPrefix(ctx);
			for (ulong k=0; k<pin->nProperties; ++k,++hprop) {
				const Value *v=&pin->properties[k]; size_t l=pin->length; Value vv; hprop->offset=sht; ElementID keygen=prefix;
				if (v->type!=VT_ARRAY && v->type!=VT_COLLECTION && v->eid!=rPrefix && v->eid!=prefix)
					{vv.set((Value*)v,1); vv.meta=v->meta; vv.property=v->property; v=&vv;}
				if ((rc=persistValue(*v,sht,hprop->type,hprop->offset,pbuf+sht,&l,pin->addr,(mode&MODE_FORCE_EIDS)==0?&keygen:NULL))!=RC_OK) goto finish;
				if (v->eid==prefix && prefix!=rPrefix) hprop->type.flags|=META_PROP_LOCAL;
				if (v==&vv) hprop->type.setType(VT_ARRAY,HDF_SHORT);
				hprop->type.flags=v->meta; hprop->setID(v->property); pin->length=l; sht=(PageOff)ceil(sht,HP_ALIGN);
			}
			hpin->hdr.length=ushort(sht); assert(ulong(sht)==pin->length);
			if ((pin->mode&COMMIT_MIGRATE)!=0) {
				assert((pin->mode&COMMIT_RESERVED)!=0 && pin->length>sizeof(HeapPageMgr::HeapObjHeader)+PageAddrSize);
				PBlock *pb2=ctx->heapMgr->getNewPage(pin->length+sizeof(PageOff),threshold,ses); 
				if (pb2==NULL) {rc=RC_FULL; break;}
				const HeapPageMgr::HeapPage *hp2=(HeapPageMgr::HeapPage*)pb2->getPageBuf();
				PageAddr newAddr={pb2->getPageID(),hp2->nSlots}; assert(pin->length+sizeof(PageOff)<=hp2->totalFree());
				rc=ctx->txMgr->update(pb2,ctx->heapMgr,(ulong)newAddr.idx<<HPOP_SHIFT|HPOP_INSERT,pbuf,(ulong)pin->length);
				if (rc!=RC_OK) {pb2->release(QMGR_UFORCE,ses); break;}
				ctx->heapMgr->reuse(pb2,ses,reserve); pb2->release(QMGR_UFORCE,ses);
				if (isRemote(pin->id)) {pin->addr=newAddr; if ((rc=ctx->netMgr->insert(pin))!=RC_OK) break; else continue;}
				HeapPageMgr::HeapObjHeader *hobj=(HeapPageMgr::HeapObjHeader*)pbuf; hobj->descr=HO_FORWARD; 
				sht=hobj->length=sizeof(HeapPageMgr::HeapObjHeader)+PageAddrSize; memcpy(hobj+1,&newAddr,PageAddrSize);
			}
			if (pin->id.pid==STORE_INVALID_PID) {const_cast<PID&>(pin->id).pid=pin->addr; const_cast<PID&>(pin->id).ident=STORE_OWNER;}
			else if (isRemote(pin->id) && (mode&MODE_NO_RINDEX)==0 && (rc=ctx->netMgr->insert(pin))!=RC_OK) goto finish;
			lrec+=ceil(sht,HP_ALIGN); assert(lrec<=xbuf);
		}
		if (lrec!=0 && (rc=ctx->txMgr->update(pb,ctx->heapMgr,(ulong)startIdx<<HPOP_SHIFT|HPOP_INSERT,buf,lrec))!=RC_OK) break;
	}
	mem.truncate(mrk);

finish:
	if (!pb.isNull()) {if (rc==RC_OK) {if (fForced) ses->forcedPage=pb->getPageID(); else ctx->heapMgr->reuse(pb,ses,reserve);} pb.release(ses);}

	ClassResult clr(ses,ses->getStore());
	for (i=0; i<nPins; i++) if ((pin=pins[i])!=NULL) {
		bool fProc = rc==RC_OK && (pin->mode&COMMIT_ALLOCATED)!=0; mem.mark(mrk);
		if ((pin->mode&COMMIT_PREFIX)!=0) for (j=0; j<pin->nProperties; j++) {
			Value *pv=&pin->properties[j],*pv2; const Value *cv; ulong k; Value w; bool fP;
			if ((pv->flags&VF_PREFIX)!=0) switch (pv->type) {
			default: assert(0);
			case VT_STREAM:
				if (pv->stream.prefix==NULL) {
					if (fProc) {uint8_t ty=pv->stream.is->dataType(); freeV(*pv); pv->type=ty; pv->length=0; pv->bstr=(byte*)&pv->length;}
				} else if (fProc && (pv->flags&VF_SSV)==0) {
					pv->length=*(ulong*)pv->stream.prefix; pv->type=pv->stream.is->dataType();
					memmove(pv->stream.prefix,(byte*)pv->stream.prefix+sizeof(uint32_t),pv->length);
					if ((pv->flags&HEAP_TYPE_MASK)!=NO_HEAP) pv->stream.is->destroy();
					pv->bstr=(byte*)pv->stream.prefix; pv->flags=pv->flags&~HEAP_TYPE_MASK|SES_HEAP;
				} else {
					ses->free(pv->stream.prefix); pv->stream.prefix=NULL;
					if (fProc && loadValue(ses,pin->id,pv->property,STORE_COLLECTION_ID,w)==RC_OK) {freeV(*pv); *pv=w;}
				}
				break;
			case VT_ARRAY:
				fP=fProc && (pv->flags&VF_SSV)==0;
				for (k=0; k<pv->length; k++) {if ((pv2=const_cast<Value*>(&pv->varray[k]))->type==VT_STREAM) {
					if (pv2->stream.prefix==NULL) {
						if (fP) {uint8_t ty=pv2->stream.is->dataType(); freeV(*pv2); pv2->type=ty; pv2->length=0; pv2->bstr=NULL;}
					} else if (fP && (pv2->flags&VF_SSV)==0) {
						pv2->length=*(ulong*)pv2->stream.prefix; pv2->type=pv2->stream.is->dataType();
						memmove(pv2->stream.prefix,(byte*)pv2->stream.prefix+sizeof(uint32_t),pv2->length);
						if ((pv2->flags&HEAP_TYPE_MASK)!=NO_HEAP) pv2->stream.is->destroy(); 
						pv2->bstr=(byte*)pv2->stream.prefix; pv2->flags=pv2->flags&~HEAP_TYPE_MASK|SES_HEAP;
					} else {
						ses->free(pv2->stream.prefix); pv2->stream.prefix=NULL;
						if (fP && loadValue(ses,pin->id,pv->property,pv2->eid,w)==RC_OK) {freeV(*pv2); *pv2=w;}
					}
				}}
				if (fProc && (pv->flags&VF_SSV)!=0 && loadValue(ses,pin->id,pv->property,STORE_COLLECTION_ID,w,LOAD_ENAV)==RC_OK)
					{freeV(*pv); *pv=w;}
				break;
			case VT_COLLECTION:
				for (cv=pv->nav->navigate(GO_FIRST); cv!=NULL; cv=pv->nav->navigate(GO_NEXT))
					if (cv->type==VT_STREAM && cv->stream.prefix!=NULL) {ses->free(cv->stream.prefix); cv->stream.prefix=NULL;}
				if (fProc && loadValue(ses,pin->id,pv->property,STORE_COLLECTION_ID,w,LOAD_ENAV)==RC_OK) {freeV(*pv); *pv=w;}
				break;
			case VT_STRUCT:
				//???
				break;
			}
		}
		if (fProc && (pin->mode&PIN_HIDDEN)==0) {
			if ((pin->mode&PIN_NO_INDEX)==0) {
				PINEx pex(ses),*ppe=&pex; if ((pin->mode&PIN_PINEX)!=0) ppe=(PINEx*)pin; else pex=pin;
				if ((rc=ctx->classMgr->classify(ppe,clr))==RC_OK && clr.nClasses>0)
					rc=ctx->classMgr->index(ses,ppe,clr,(pin->mode&PIN_DELETED)!=0?CI_INSERTD:CI_INSERT);
				if (rc==RC_OK && into!=NULL) for (unsigned i=0; i<nInto; i++) {
					const ClassID cid=into[i].classID&STORE_MAX_URIID; bool fFound=false;
					for (unsigned j=0; j<clr.nClasses; j++) if (clr.classes[j]->cid==cid) {
						if ((into[i].classID&CLASS_UNIQUE)==0 && (clr.classes[j]->nIndexProps==0 || into[i].nParams==0)) fFound=true;
						else {
							// check params, uniquness
						}
						break;
					}
					if (!fFound) {rc=RC_CONSTRAINT; break;}
				}
				if (rc==RC_OK && (pin->mode&(PIN_DELETED|COMMIT_FTINDEX))==COMMIT_FTINDEX) {
					const Value *doc=pin->findProperty(PROP_SPEC_DOCUMENT); SubAlloc sa(ses); FTList ftl(sa);
					ChangeInfo inf={pin->id,doc==NULL?PIN::defPID:doc->type==VT_REF?doc->pin->getPID():
							doc->type==VT_REFID?doc->id:PIN::defPID,NULL,NULL,STORE_INVALID_PROPID,STORE_COLLECTION_ID};
					for (ulong k=0; k<pin->nProperties; ++k) {
						const Value& pv=pin->properties[k]; 
						if ((pv.meta&META_PROP_NOFTINDEX)==0) {
							inf.propID=pv.property; inf.newV=&pv;
							rc=ctx->ftMgr->index(inf,&ftl,IX_NFT,(pv.meta&META_PROP_STOPWORDS)!=0?FTMODE_STOPWORDS:0,ses);
							if (rc!=RC_OK) break;
						}
					}
					if (rc==RC_OK) rc=ctx->ftMgr->process(ftl,inf.id,inf.docID);
				}
			}
			if (rc==RC_OK && (pin->mode&PIN_DELETED)==0) {
				if (replication!=NULL && (pin->mode&(PIN_NO_REPLICATION|PIN_REPLICATED))==PIN_REPLICATED) rc=ses->replicate(pin);
				if (rc==RC_OK && ctx->queryMgr->notification!=NULL && ((pin->mode&PIN_NOTIFY)!=0||(clr.notif&CLASS_NOTIFY_NEW)!=0)) {
					IStoreNotification::NotificationEvent evt={pin->id,NULL,0,NULL,0,fRepSes};
					if ((evt.events=(IStoreNotification::EventData*)mem.malloc((clr.nClasses+1)*sizeof(IStoreNotification::EventData)))!=NULL) {
						if ((clr.notif&CLASS_NOTIFY_NEW)!=0) for (ulong i=0; i<clr.nClasses; i++) if ((clr.classes[i]->notifications&CLASS_NOTIFY_NEW)!=0) {
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
						if (evt.nEvents>0) try {ctx->queryMgr->notification->notify(&evt,1,ses->getTXID());} catch (...) {}
					}
				}
			}
		}
		pin->mode&=~COMMIT_MASK; pin->stamp=0; clr.nClasses=clr.nIndices=clr.notif=0;
		mem.truncate(mrk); if (rc!=RC_OK) pin->addr=PageAddr::invAddr;
	}
	if (rc==RC_OK && classPINs!=NULL && nClassPINs>0) rc=ctx->classMgr->classifyAll(classPINs,nClassPINs,ses);
	if (rc==RC_OK) {tx.ok(); ses->nTotalIns+=nInserted; ses->tx.nInserted+=nInserted;}
	return rc;
}

RC QueryPrc::estimateLength(const Value& v,size_t& res,ulong mode,size_t threshold,MemAlloc *ma,PageID pageID,size_t *rlen)
{
	ulong i; size_t l,len; PID id; ValueType ty; PageAddr addr; const Value *pv; Session *ses;
	v.flags&=HEAP_TYPE_MASK;
	switch (v.type) {
	default: return RC_TYPE;
	case VT_INT: res=v.i>=-32768&&v.i<=32767?0:sizeof(int32_t); break;
	case VT_UINT: res=v.ui<=0xFFFF?0:sizeof(uint32_t); break;
	case VT_INT64: case VT_INTERVAL: res=v.i64>=-32768&&v.i64<=32767?0:sizeof(int64_t); break;
	case VT_DATETIME: case VT_UINT64: res=v.ui64<=0xFFFF?0:sizeof(uint64_t); break;
	case VT_FLOAT: res=sizeof(float); if (v.qval.units!=Un_NDIM) res+=sizeof(uint16_t); break;
	case VT_DOUBLE: res=sizeof(double); if (v.qval.units!=Un_NDIM) res+=sizeof(uint16_t); break;
	case VT_RESERVED1: res=sizeof(uint32_t)*2; break;
	case VT_BOOL: res=0; break;
	case VT_URIID: res=v.uid<=0xFFFF?0:sizeof(URIID); break;
	case VT_IDENTITY: res=v.iid<=0xFFFF?0:sizeof(IdentityID); break;
	case VT_CURRENT: 
		switch (v.i) {
		default: res=0; break;
		case CVT_TIMESTAMP: res=sizeof(uint64_t); break;
		case CVT_USER: ses=Session::getSession(); res=ses!=NULL&&ses->getIdentity()>0xFFFF?sizeof(uint32_t):0; break;
		case CVT_STORE: res=0; break;
		}
		break;
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
		if (isRemote(v.id)) res=v.id.ident==STORE_OWNER?sizeof(OID):sizeof(OID)+sizeof(IdentityID);
		else if (!addr.convert(v.id.pid)||addr.pageID!=pageID) res=PageAddrSize;
		else {if (rlen!=NULL) *rlen+=PageAddrSize; res=0;}
		break;
	case VT_REFIDPROP:
		if (v.refId->id.pid==STORE_INVALID_PID) return RC_INVPARAM;
		res=!isRemote(v.refId->id)?PageAddrSize+sizeof(PropertyID):
										v.refId->id.ident==STORE_OWNER?sizeof(OID)+sizeof(PropertyID):
										sizeof(OID)+sizeof(IdentityID)+sizeof(PropertyID);
		break;
	case VT_REFIDELT:
		if (v.refId->id.pid==STORE_INVALID_PID) return RC_INVPARAM;
		res=!isRemote(v.refId->id)?PageAddrSize+sizeof(PropertyID)+sizeof(ElementID):
							v.refId->id.ident==STORE_OWNER?sizeof(OID)+sizeof(PropertyID)+sizeof(ElementID):
							sizeof(OID)+sizeof(IdentityID)+sizeof(PropertyID)+sizeof(ElementID);
		break;
	case VT_REF:
		id=((PIN*)v.pin)->id; if (!((PIN*)v.pin)->addr.defined()) v.flags|=VF_REF;
		if (id.pid==STORE_INVALID_PID) res=PageAddrSize;
		else if (isRemote(id)) res=id.ident==STORE_OWNER?sizeof(OID):sizeof(OID)+sizeof(IdentityID);
		else if (!addr.convert(id.pid)||addr.pageID!=pageID) res=PageAddrSize;
		else {if (rlen!=NULL) *rlen+=PageAddrSize; res=0;}
		break;
	case VT_REFPROP:
		id=((PIN*)v.ref.pin)->id; 
		if (!((PIN*)v.ref.pin)->addr.defined()) v.flags|=VF_REF; else if (id.pid==STORE_INVALID_PID) return RC_INVPARAM;
		res=id.pid==STORE_INVALID_PID||!isRemote(id)?PageAddrSize+sizeof(PropertyID):
			id.ident==STORE_OWNER?sizeof(OID)+sizeof(PropertyID):sizeof(OID)+sizeof(IdentityID)+sizeof(PropertyID);
		break;
	case VT_REFELT:
		id=((PIN*)v.ref.pin)->id; 
		if (!((PIN*)v.ref.pin)->addr.defined()) v.flags|=VF_REF; else if (id.pid==STORE_INVALID_PID) return RC_INVPARAM;
		res=id.pid==STORE_INVALID_PID||!isRemote(id)?PageAddrSize+sizeof(PropertyID)+sizeof(ElementID):
							id.ident==STORE_OWNER?sizeof(OID)+sizeof(PropertyID)+sizeof(ElementID):
							sizeof(OID)+sizeof(IdentityID)+sizeof(PropertyID)+sizeof(ElementID);
		break;
	case VT_ARRAY:
		if (v.varray==NULL) return RC_INVPARAM;
		len=sizeof(HeapPageMgr::HeapVV)-sizeof(HeapPageMgr::HeapV)+sizeof(HeapPageMgr::HeapKey);
		for (i=0; i<v.length; ++i) {
			RC rc=estimateLength(v.varray[i],l,mode,threshold,ma,pageID,rlen); if (rc!=RC_OK) return rc;
			len+=ceil(l,HP_ALIGN)+sizeof(HeapPageMgr::HeapV); v.flags|=v.varray[i].flags&(VF_STRING|VF_REF|VF_PREFIX);
		}
		res=len<=threshold?len:(v.flags|=VF_SSV,sizeof(HeapPageMgr::HeapExtCollection)); break;
	case VT_COLLECTION:
		if (v.nav==NULL) return RC_INVPARAM;
		len=sizeof(HeapPageMgr::HeapVV)-sizeof(HeapPageMgr::HeapV)+sizeof(HeapPageMgr::HeapKey);
		for (pv=v.nav->navigate(GO_FIRST),i=0; pv!=NULL; ++i,pv=v.nav->navigate(GO_NEXT)) {
			RC rc=estimateLength(*pv,l,mode,threshold,ma,pageID,rlen); if (rc!=RC_OK) return rc;
			len+=ceil(l,HP_ALIGN)+sizeof(HeapPageMgr::HeapV); v.flags|=pv->flags&(VF_STRING|VF_REF|VF_PREFIX);
		}
		res=len<=threshold?len:(v.flags|=VF_SSV,sizeof(HeapPageMgr::HeapExtCollection)); break;
	case VT_STRUCT:
		//???
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
					v.stream.prefix=ma->realloc(v.stream.prefix,l+sizeof(uint32_t)); assert(v.stream.prefix!=NULL);
					l+=l>0xff?2:1; res=l<=threshold?l:(v.flags|=VF_SSV,sizeof(HRefSSV));
				}
			}
		} else if (v.stream.prefix==NULL) res=0;
		else {
			l=*(ulong*)v.stream.prefix;
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
	}
	return RC_OK;
}

bool QueryPrc::checkRef(const Value& v,PIN *const *pins,unsigned nPins)
{
	ulong k,l; const Value *pv;
	switch (v.type) {
	default: return false;
	case VT_REF:
		if (!v.pin->isCommitted()) {
			for (k=0;;k++) if (k>=nPins) return false; 
			else if (pins[k]==v.pin) {pins[k]->mode|=COMMIT_REFERRED; break;}
		}
		break;
	case VT_REFPROP:
	case VT_REFELT:
		if (!v.ref.pin->isCommitted()) {
			for (k=0;;k++) if (k>=nPins) return false; 
			else if (pins[k]==v.ref.pin) {pins[k]->mode|=COMMIT_REFERRED; break;}
		}
		break;
	case VT_ARRAY:
		for (k=0; k<v.length; k++) {
			const Value &val=v.varray[k];
			switch (val.type) {
			case VT_REF:
				if (!val.pin->isCommitted()) {
					for (l=0;;l++) if (l>=nPins) return false; 
					else if (pins[l]==val.pin) {pins[l]->mode|=COMMIT_REFERRED; break;}
				}
				break;
			case VT_REFPROP:
			case VT_REFELT:
				if (!val.ref.pin->isCommitted()) {
					for (l=0;;l++) if (l>=nPins) return false; 
					else if (pins[l]==val.ref.pin) {pins[l]->mode|=COMMIT_REFERRED; break;}
				}
				break;
			default: if (val.type==VT_ERROR || val.type>=VT_ARRAY) return false;
			}
		}
		break;
	case VT_COLLECTION:
		for (pv=v.nav->navigate(GO_FIRST); pv!=NULL; pv=v.nav->navigate(GO_NEXT)) {
			switch (pv->type) {
			case VT_REF:
				if (!pv->pin->isCommitted()) {
					for (l=0;;l++) if (l>=nPins) return false; 
					else if (pins[l]==pv->pin) {pins[l]->mode|=COMMIT_REFERRED; break;}
				}
				break;
			case VT_REFPROP:
			case VT_REFELT:
				if (!pv->ref.pin->isCommitted()) {
					for (l=0;;l++) if (l>=nPins) return false; 
					else if (pins[l]==pv->ref.pin) {pins[l]->mode|=COMMIT_REFERRED; break;}
				}
				break;
			default: if (pv->type==VT_ERROR || pv->type>=VT_ARRAY) return false;
			}
		}
		break;
	case VT_STRUCT:
		//???
		break;
	}
	return true;
}

RC QueryPrc::persistValue(const Value& v,ushort& sht,HType& vt,ushort& offs,byte *buf,size_t *plrec,const PageAddr& addr,ElementID *keygen)
{
	assert(ceil(buf,HP_ALIGN)==buf);
	PageOff len,l; PID id; PropertyID pid; ElementID eid; IdentityID iid; PageAddr rAddr; Session *ses;
	HeapPageMgr::HeapVV *hcol; HeapPageMgr::HeapV *helt;
	ValueType ty=(ValueType)v.type; ulong i; size_t ll; HRefSSV *href; RC rc; ElementID key;
	const byte *p; TIMESTAMP ts; const Value *pv; uint64_t len64; IStream *istr;
	switch (ty) {
	default: return RC_TYPE;
	case VT_INT:
		if (v.i>=-32768&&v.i<=32767) {offs=ushort(v.i); vt.setType(VT_INT,HDF_COMPACT); return RC_OK;}
		memcpy(buf,&v.i,len=sizeof(int32_t)); vt.setType(VT_INT,HDF_NORMAL); break;
	case VT_UINT: 
		if (v.ui<=0xFFFF) {offs=ushort(v.ui); vt.setType(VT_UINT,HDF_COMPACT); return RC_OK;}
		memcpy(buf,&v.ui,len=sizeof(uint32_t)); vt.setType(VT_UINT,HDF_NORMAL); break;
	case VT_INT64: case VT_INTERVAL:
		if (v.i64>=-32768&&v.i64<=32767) {offs=ushort(v.i64); vt.setType(ty,HDF_COMPACT); return RC_OK;}
		memcpy(buf,&v.i64,len=sizeof(int64_t)); vt.setType(ty,HDF_NORMAL); break;
	case VT_UINT64: case VT_DATETIME:
		if (v.ui64<=0xFFFF) {offs=ushort(v.ui64); vt.setType(ty,HDF_COMPACT); return RC_OK;}
		memcpy(buf,&v.ui64,len=sizeof(uint64_t)); vt.setType(ty,HDF_NORMAL); break;
	case VT_FLOAT:
		memcpy(buf,&v.f,len=sizeof(float)); 
		if (v.qval.units==Un_NDIM) vt.setType(VT_FLOAT,HDF_NORMAL);
		else {memcpy(buf+sizeof(float),&v.qval.units,sizeof(uint16_t)); len+=sizeof(uint16_t); vt.setType(VT_FLOAT,HDF_LONG);}
		break;
	case VT_DOUBLE:
		memcpy(buf,&v.d,len=sizeof(double));
		if (v.qval.units==Un_NDIM) vt.setType(VT_DOUBLE,HDF_NORMAL);
		else {memcpy(buf+sizeof(double),&v.qval.units,sizeof(uint16_t)); len+=sizeof(uint16_t); vt.setType(VT_DOUBLE,HDF_LONG);}
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
			try {memcpy(buf+l,p,len); len+=l;} catch (RC rc) {return rc;} catch (...) {return RC_INVPARAM;}
		} else {
			rAddr=addr;
			if ((rc=persistData(istr,p,ll,rAddr,len64))!=RC_OK && rc!=RC_TRUE) return rc;
			href=(HRefSSV*)buf; href->pageID=rAddr.pageID; href->idx=rAddr.idx;
			href->type.setType(ty,HDF_NORMAL); href->type.flags=0;
			if (rc==RC_OK) {v.flags&=~VF_SSV; len=sizeof(HRefSSV); vt.setType(VT_STREAM,HDF_SHORT);}
			else {((HLOB*)href)->len=len64; len=sizeof(HLOB); vt.setType(VT_STREAM,HDF_LONG);}
		}
		break;
	case VT_URIID:
		if (v.uid<=0xFFFF) {offs=ushort(v.uid); vt.setType(VT_URIID,HDF_COMPACT); return RC_OK;}
		memcpy(buf,&v.uid,len=sizeof(URIID)); vt.setType(VT_URIID,HDF_NORMAL); break;
	case VT_IDENTITY:
		if (v.iid<=0xFFFF) {offs=ushort(v.iid); vt.setType(VT_IDENTITY,HDF_COMPACT); return RC_OK;}
		memcpy(buf,&v.iid,len=sizeof(IdentityID)); vt.setType(VT_IDENTITY,HDF_NORMAL); break;
	case VT_REF: ty=VT_REFID; id=v.pin->getPID();
		if (id.pid==STORE_INVALID_PID || id.ident==STORE_INVALID_IDENTITY) 
			{assert(((PIN*)v.pin)->addr.defined()); id.pid=((PIN*)v.pin)->addr; id.ident=STORE_OWNER;}
		goto store_ref;
	case VT_REFID: id=v.id;
	store_ref:
		if (!isRemote(id)) {
			if (!rAddr.convert(id.pid) || rAddr.pageID!=addr.pageID) {memcpy(buf,&rAddr,len=PageAddrSize); vt.setType(ty,HDF_SHORT);}
			else {vt.setType(ty,HDF_COMPACT); offs=rAddr.idx; if (plrec!=NULL) *plrec-=PageAddrSize; len=0;}
		} else {
			memcpy(buf,&id.pid,len=sizeof(uint64_t)); 
			if (id.ident==STORE_OWNER) vt.setType(ty,HDF_NORMAL);
			else {memcpy(buf+sizeof(uint64_t),&id.ident,sizeof(IdentityID)); vt.setType(ty,HDF_LONG); len+=sizeof(IdentityID);}
		}
		break;
	case VT_REFPROP: ty=VT_REFIDPROP; goto check_refv;
	case VT_REFELT: eid=v.ref.eid; ty=VT_REFIDELT;
	check_refv: id=v.ref.pin->getPID(); pid=v.ref.pid;
		if (id.pid==STORE_INVALID_PID || id.ident==STORE_INVALID_IDENTITY) 
			{assert(((PIN*)v.ref.pin)->addr.defined()); id.pid=((PIN*)v.ref.pin)->addr; id.ident=STORE_OWNER;}
		goto store_refv;
	case VT_REFIDPROP: case VT_REFIDELT: id=v.refId->id; pid=v.refId->pid; eid=v.refId->eid;
	store_refv:
		if (!isRemote(id)) {
			if (rAddr.convert(id.pid)) memcpy(buf,&rAddr,len=PageAddrSize); vt.setType(ty,HDF_SHORT);
		} else {
			memcpy(buf,&id.pid,len=sizeof(uint64_t)); 
			if (id.ident==STORE_OWNER) vt.setType(ty,HDF_NORMAL);
			else {memcpy(buf+sizeof(uint64_t),&id.ident,sizeof(IdentityID)); vt.setType(ty,HDF_LONG); len+=sizeof(IdentityID);}
		}
		memcpy(buf+len,&pid,sizeof(PropertyID)); len+=sizeof(PropertyID);
		if (ty==VT_REFIDELT) {memcpy(buf+len,&eid,sizeof(ElementID)); len+=sizeof(ElementID);}
		break;
	case VT_ARRAY:
		if (v.varray==NULL) {vt.setType(VT_ERROR,HDF_COMPACT); offs=0; return RC_INTERNAL;}
	case VT_COLLECTION:
		if ((v.flags&VF_SSV)==0) {
			hcol=(HeapPageMgr::HeapVV*)buf; hcol->cnt=(uint16_t)(ty==VT_ARRAY?v.length:v.nav->count()); hcol->fUnord=0;
			if (hcol->cnt==0) {vt.setType(VT_ERROR,HDF_COMPACT); offs=0; return RC_INTERNAL;} key=keygen!=NULL?*keygen:ctx->getPrefix();
			len=sizeof(HeapPageMgr::HeapVV)+(hcol->cnt-1)*sizeof(HeapPageMgr::HeapV)+sizeof(HeapPageMgr::HeapKey); ElementID prevEID=0;
			for (pv=ty==VT_ARRAY?v.varray:v.nav->navigate(GO_FIRST),i=0; pv!=NULL; ++i,pv=ty==VT_ARRAY?i>=v.length?(Value*)0:&v.varray[i]:v.nav->navigate(GO_NEXT)) {
				helt=&hcol->start[i]; helt->type.flags=pv->meta; helt->offset=len;
				if (keygen!=NULL || pv->eid==STORE_COLLECTION_ID) pv->eid=key++;
				else {
					assert(pv->eid!=STORE_FIRST_ELEMENT && pv->eid!=STORE_LAST_ELEMENT);
					if (pv->eid>=key&&((key^pv->eid)&CPREFIX_MASK)==0) key=pv->eid+1;
				}
				helt->setID(pv->eid);
				if ((rc=persistValue(*pv,len,helt->type,helt->offset,buf+len,plrec,addr))!=RC_OK) return rc;
				if (!helt->type.isCompact()) len=(PageOff)ceil(len,HP_ALIGN);
				if (pv->eid<prevEID) hcol->fUnord=1; prevEID=pv->eid;
			}
			hcol->getHKey()->setKey(key); vt.setType(VT_ARRAY,HDF_NORMAL); if (keygen!=NULL) *keygen=key;
		} else {
			if ((rc=Collection::persist(v,*(HeapPageMgr::HeapExtCollection*)buf,Session::getSession(),keygen==NULL))!=RC_OK) return rc;	// maxPages ???
			len=sizeof(HeapPageMgr::HeapExtCollection); vt.setType(VT_ARRAY,HDF_LONG);
		}
		break;
	case VT_STRUCT:
		//???
		break;
	case VT_CURRENT:
		switch (v.i) {
		default: break;
		case CVT_TIMESTAMP:
			getTimestamp(ts); memcpy(buf,&ts,sizeof(uint64_t)); 
			const_cast<Value&>(v).type=VT_DATETIME; const_cast<Value&>(v).ui64=ts;
			vt.setType(VT_DATETIME,HDF_NORMAL); len=sizeof(uint64_t); break;
		case CVT_USER:
			ses=Session::getSession(); iid=ses!=NULL?ses->getIdentity():STORE_OWNER;
			const_cast<Value&>(v).type=VT_IDENTITY; const_cast<Value&>(v).iid=iid;
			if (iid<=0xFFFF) {offs=ushort(iid); vt.setType(VT_IDENTITY,HDF_COMPACT); return RC_OK;}
			memcpy(buf,&iid,len=sizeof(IdentityID)); vt.setType(VT_IDENTITY,HDF_NORMAL); break;
		case CVT_STORE:
			const_cast<Value&>(v).type=VT_UINT; const_cast<Value&>(v).ui=StoreCtx::get()->storeID;
			offs=ushort(v.ui); vt.setType(VT_UINT,HDF_COMPACT); return RC_OK;
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
			href=(HRefSSV*)buf; href->pageID=rAddr.pageID; href->idx=rAddr.idx;
			href->type.setType(ty,HDF_NORMAL); href->type.flags=0;
			if (rc==RC_OK) {v.flags&=~VF_SSV; len=sizeof(HRefSSV); vt.setType(VT_STREAM,HDF_SHORT);}
			else {((HLOB*)href)->len=len64; len=sizeof(HLOB); vt.setType(VT_STREAM,HDF_LONG);}
		} else {
			assert(ll<0xFFFF); len=(PageOff)ll;
			if (ty==VT_EXPR) ((Expr*)v.expr)->serialize(buf);
			else {afy_enc16(buf,len); len+=afy_len16(len); ((Stmt*)v.stmt)->serialize(buf);}
			vt.setType(ty,HDF_NORMAL); 
		}
		break;
	case VT_RESERVED1:
		// ???
		return RC_INTERNAL;
	}
	sht+=len; return RC_OK;
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
				rc=ctx->txMgr->update(pb,ctx->ssvMgr,(ulong)addr.idx<<HPOP_SHIFT|HPOP_INSERT,buf,l);
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
			if ((rc=ctx->txMgr->update(pb,ctx->ssvMgr,(ulong)idx<<HPOP_SHIFT|HPOP_INSERT,buf,lpiece))!=RC_OK) 
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
		ulong startOff=pos<start?ulong(start-pos):0,lmod=ulong(epos<pos+lpiece?epos-pos:lpiece)-startOff;
		if (lmod>0 && pOld) {memcpy(pOld,(byte*)hobj+hobj->length-lpiece+startOff,lmod); pOld+=lmod;}
		if (left==0 && lmod==lpiece) {
			assert(htype!=HO_SSVALUE);
			if (prevAddr.defined()) {fSetAddr=true; nextAddr=addr;}
			else {assert(addr.defined()); startAddr=addr; fAddrChanged=true;}
			ulong l=hobj->length;
			if (l>lbuf && (buf=buf==NULL?(byte*)ses->malloc(lbuf=l):(byte*)ses->realloc(buf,lbuf=l))==NULL)
				{rc=RC_NORESOURCES; break;}
			memcpy(buf,hobj,l); 
			if ((rc=ctx->txMgr->update(pb,ctx->ssvMgr,(ulong)idx<<HPOP_SHIFT|HPOP_PURGE,buf,l))!=RC_OK) break;
			ctx->ssvMgr->reuse(pb,ses,false,true); pb.release(ses); continue;
		}
		if (htype==HO_SSVALUE && left>lmod && left-lmod>hp->totalFree() &&
								(lnew=ulong(lpiece+sizeof(HeapPageMgr::HeapObjHeader)+left-lmod+sizeof(PageOff)))<=
															HeapPageMgr::contentSize(ctx->bufMgr->getPageSize())) {
			PBlockP newPB(ctx->ssvMgr->getNewPage(lnew,ses,fNew),QMGR_UFORCE); if (newPB.isNull()) {rc=RC_FULL; break;}
			byte *ssv=(byte*)ses->malloc(lpiece+lhdr);
			if (ssv!=NULL) memcpy(ssv,hobj,lhdr+lpiece); else {rc=RC_NORESOURCES; break;}
			if ((rc=ctx->txMgr->update(pb,ctx->ssvMgr,(ulong)idx<<HPOP_SHIFT|HPOP_PURGE,ssv,lhdr+lpiece))==RC_OK) {
				Value w,edit; w.set(ssv,uint32_t(lpiece+lhdr)); w.type=v.type; w.flags=SES_HEAP; edit=v; edit.edit.shift+=lhdr;
				if ((rc=Expr::calc(OP_EDIT,w,&edit,2,0,ses))==RC_OK) {
					((HeapPageMgr::HeapObjHeader*)w.bstr)->length=ushort(w.length);
					hp=(const HeapPageMgr::HeapPage*)newPB->getPageBuf(); idx=hp->nSlots;
					if ((rc=ctx->txMgr->update(newPB,ctx->ssvMgr,(ulong)idx<<HPOP_SHIFT|HPOP_INSERT,w.bstr,w.length))==RC_OK) {
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
			if ((rc=ctx->txMgr->update(pb,ctx->ssvMgr,(ulong)idx<<HPOP_SHIFT|HPOP_EDIT,buf,l))!=RC_OK) break;
		}
		if (left>0 && epos<=pos+lpiece) {
			StreamBuf *psb=NULL; PageAddr newAddr={pb->getPageID(),idx}; byte *tail=NULL; ulong ltail=0; uint64_t ll;
			if (htype==HO_SSVALUE) {
				if (hp->totalFree()<PageAddrSize*2) {
					byte *ssv=(byte*)ses->malloc(lnew=lhdr+lpiece+PageAddrSize*2); if (ssv==NULL) {rc=RC_NORESOURCES; break;}
					memcpy(ssv,hobj,lhdr+lpiece);
					if ((rc=ctx->txMgr->update(pb,ctx->ssvMgr,(ulong)idx<<HPOP_SHIFT|HPOP_PURGE,ssv,lhdr+lpiece))==RC_OK) {
						ctx->ssvMgr->reuse(pb,ses,false); pb.release(ses); newAddr=PageAddr::invAddr;
						if ((rc=persistData(NULL,ssv+lhdr,lpiece,newAddr,ll,&PageAddr::invAddr,&pb))==RC_TRUE) rc=RC_OK;
					}
					ses->free(ssv); if (rc!=RC_OK) break;
					htype=HO_BLOB; startAddr=newAddr; 
				}
				fSSVLOB=true;
			}
			if (epos<pos+lpiece) {
				ltail=ulong(pos+lpiece-epos); 
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
				if ((rc=ctx->txMgr->update(pb,ctx->ssvMgr,(ulong)idx<<HPOP_SHIFT|HPOP_EDIT,buf,l))!=RC_OK) break;
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
				rc=ctx->txMgr->update(pb,ctx->ssvMgr,(ulong)idx<<HPOP_SHIFT|HPOP_EDIT,abuf,sizeof(abuf));
			}
			if (psb!=NULL) {
				if (rc==RC_OK) {
					memmove(tail+sizeof(HeapPageMgr::HeapModEdit),tail,ltail);
					HeapPageMgr::HeapModEdit *he=(HeapPageMgr::HeapModEdit*)tail;
					he->newPtr.offset=he->oldPtr.offset=sizeof(HeapPageMgr::HeapModEdit);
					he->newPtr.len=0; he->oldPtr.len=ushort(ltail);
					he->dscr=0; he->shift=ushort(lpiece-ltail+lhdr-sizeof(HeapPageMgr::HeapObjHeader)); 
					rc=ctx->txMgr->update(pb,ctx->ssvMgr,(ulong)idx<<HPOP_SHIFT|HPOP_EDIT,tail,ltail+sizeof(HeapPageMgr::HeapModEdit));
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
				rc=ctx->txMgr->update(pb,ctx->ssvMgr,(ulong)idx<<HPOP_SHIFT|HPOP_EDIT,abuf,sizeof(abuf));
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
		rc=ctx->txMgr->update(pb,ctx->ssvMgr,(ulong)idx<<HPOP_SHIFT|HPOP_PURGE,buf,hobj->length);
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
	size_t expLen=(cand->hdr.descr&HOH_COMPACTREF)!=0?cand->expLength((const byte*)hp):cand->hdr.getLength(); if (!fMigrated) expLen+=PageAddrSize;
	PBlockP newPB(ctx->heapMgr->getNewPage(expLen+sizeof(PageOff),reserve,ses),QMGR_UFORCE); if (newPB.isNull()) return RC_FULL;
	PageAddr newAddr={newPB->getPageID(),((HeapPageMgr::HeapPage*)newPB->getPageBuf())->nSlots},oldAddr={pb->getPageID(),cidx},origAddr;
	RC rc=cand->serialize(buf,lr,hp,ses,expLen,(cand->hdr.descr&HOH_COMPACTREF)!=0); if (rc!=RC_OK) return rc;
	if (fMigrated) {
		HeapPageMgr::HeapModEdit *hm=(HeapPageMgr::HeapModEdit*)fbuf; 
		hm->dscr=hm->shift=0; hm->oldPtr.len=hm->newPtr.len=PageAddrSize;
		hm->newPtr.offset=(hm->oldPtr.offset=sizeof(HeapPageMgr::HeapModEdit))+PageAddrSize;
		memcpy(fbuf+sizeof(HeapPageMgr::HeapModEdit),&oldAddr,PageAddrSize);
		memcpy(fbuf+sizeof(HeapPageMgr::HeapModEdit)+PageAddrSize,&newAddr,PageAddrSize);
		PageAddr *orig=cand->getOrigLoc(); assert(orig!=NULL);
		memcpy(&origAddr,orig,PageAddrSize);
	} else {
		byte *orig=(byte*)((HeapPageMgr::HeapPIN*)buf)->getOrigLoc();
		if (lr<PageAddrSize+(orig-buf)) return RC_CORRUPTED;
		memmove(orig+PageAddrSize,orig,lr-PageAddrSize-(orig-buf));
		((HeapPageMgr::HeapPIN*)buf)->hdr.length+=PageAddrSize;
		((HeapPageMgr::HeapPIN*)buf)->lExtra+=PageAddrSize;
		((HeapPageMgr::HeapPIN*)buf)->setOrigLoc(oldAddr);
		HeapPageMgr::HeapV *hprop=((HeapPageMgr::HeapPIN*)buf)->getPropTab();
		for (ulong i=((HeapPageMgr::HeapPIN*)buf)->nProps; i!=0; ++hprop,--i)
			if (!hprop->type.isCompact()) hprop->offset+=PageAddrSize;
		img=buf; limg=lr;
	}
	if ((rc=ctx->txMgr->update(newPB,ctx->heapMgr,(ulong)newAddr.idx<<HPOP_SHIFT|HPOP_INSERT,buf,lr))!=RC_OK) return rc;
	if ((rc=cand->serialize(img,limg,hp,ses,cand->hdr.getLength()+PageAddrSize))!=RC_OK) return rc;
	memcpy(img+limg-PageAddrSize,&newAddr,PageAddrSize);
	rc=ctx->txMgr->update(pb,ctx->heapMgr,(ulong)oldAddr.idx<<HPOP_SHIFT|HPOP_MIGRATE,img,limg);
	if (img!=buf) ses->free(img); if (rc!=RC_OK) return rc;
	if (fRemote) ctx->netMgr->updateAddr(id,newAddr);
	ctx->heapMgr->reuse(newPB,ses,reserve);
	if (pin->length+lxtab>hp->totalFree()) pin->mode|=COMMIT_MIGRATE;
	return RC_OK;
}
