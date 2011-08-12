/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "txmgr.h"
#include "lock.h"
#include "session.h"
#include "queryprc.h"
#include "startup.h"
#include "netmgr.h"
#include "classifier.h"
#include "ftindex.h"
#include "blob.h"
#include "maps.h"

using namespace MVStoreKernel;

RC QueryPrc::deletePINs(Session *ses,const PIN *const *pins,const PID *pids,unsigned nPins,unsigned mode,PINEx *pcb)
{
	if (pins==NULL&&pids==NULL || nPins==0) return RC_OK;
	if (ses==NULL) return RC_NOSESSION; if (ses->isRestore()) return RC_OTHER;
	if (ctx->isServerLocked()) return RC_READONLY; if (ses->inReadTx()) return RC_READTX;
	ClassResult clr(ses,ctx); if (pcb==NULL) ctx->classMgr->initClasses(ses);
	TxSP tx(ses); RC rc=tx.start(TXI_DEFAULT,nPins==1?TX_ATOMIC:0); if (rc!=RC_OK) return rc; 
	bool fRepSes=ses->isReplication(); PINEx cb(ses); if (pcb!=NULL && pcb->pb.isNull()) pcb=NULL;
	size_t threshold=ceil(size_t((HeapPageMgr::contentSize(ctx->bufMgr->getPageSize())-sizeof(PageOff))*(1.-(ses->allocCtrl!=NULL?ses->allocCtrl->pctPageFree:ctx->theCB->pctFree))),HP_ALIGN);
	Value *SSVs=NULL; unsigned nSSVs=0,xSSVs=0; PID *parts=NULL; unsigned nParts=0,xParts=0; Value *vals=NULL; unsigned nvals=0;
	const ulong flgs=(mode&(MODE_DELETED|MODE_PURGE|MODE_PURGE_IDS))!=0?GB_DELETED|GB_FORWARD:GB_FORWARD; const bool fPurge=(mode&(MODE_PURGE|MODE_PURGE_IDS))!=0; 
	const ulong hop=(mode&MODE_PURGE_IDS)!=0?HPOP_PURGE_IDS:fPurge?HPOP_PURGE:HPOP_DELETE; SubAlloc sft(ses);

	try {
		for (unsigned np=0; np<nPins; pcb=NULL,nParts=0) {
			PID pid=pids[np++]; cb=pid; if (pcb==NULL) pcb=&cb;
			if (isRemote(pid) && fPurge && (rc=ctx->netMgr->remove(pid,cb.addr))!=RC_OK) throw rc!=RC_FALSE?rc:RC_NOTFOUND;
			ushort pinDescr=0; ClassID cid=STORE_INVALID_CLASSID; unsigned nProps=0; const HeapPageMgr::HeapV *hprop;
			byte fwdbuf[sizeof(HeapPageMgr::HeapObjHeader)+PageAddrSize]; bool fDelFwd=false; PageAddr fwd=PageAddr::invAddr,deleted;
			FTList *ftl=NULL; ChangeInfo fti; ulong i; Value v; IStoreNotification::NotificationData *ndata=NULL;
			for (;;) {
				if (pcb->pb.isNull() && (rc=getBody(*pcb,TVO_UPD,flgs))!=RC_OK) throw rc;
				if (pcb->hpin->hdr.getType()==HO_FORWARD) {
					memcpy(fwdbuf,pcb->hpin,sizeof(fwdbuf)); if (!fwd.defined()) fwd=pcb->addr;
					if ((rc=ctx->txMgr->update(pcb->pb,ctx->heapMgr,(ulong)pcb->addr.idx<<HPOP_SHIFT|hop,fwdbuf,sizeof(fwdbuf)))!=RC_OK) throw rc;
					if (fPurge)	ctx->heapMgr->reuse(pcb->pb,ses,threshold,true); memcpy(&cb.addr,fwdbuf+sizeof(HeapPageMgr::HeapObjHeader),PageAddrSize);
					if (cb.addr.defined()) {pcb=&cb; cb.pb.release(); cb.mode|=PINEX_ADDRSET; continue;}
				} else if (pcb->hpin->isMigrated()) {
					memcpy(fwdbuf,pcb->hpin->getOrigLoc(),PageAddrSize);
					if (fwd!=*(PageAddr*)fwdbuf) {fwd=*(PageAddr*)fwdbuf; deleted=pcb->addr; fDelFwd=true;}
				}
				break;
			}
			if (pcb->hpin==NULL || pcb->hpin->hdr.getType()!=HO_PIN) break;
			if ((mode&MODE_CHECK_STAMP)!=0 && pins!=NULL && np<nPins && pcb->hpin->getStamp()!=pins[np]->stamp) throw RC_REPEAT;
			pinDescr=pcb->hpin->hdr.descr; if (!fPurge && (pinDescr&HOH_DELETED)!=0) break;
			if ((pinDescr&HOH_CLASS)!=0 && (hprop=pcb->hpin->findProperty(PROP_SPEC_CLASSID))!=NULL &&
				loadVH(v,hprop,*pcb,0,ses)==RC_OK && v.type==VT_URIID && (cid=v.uid)==STORE_CLASS_OF_CLASSES) throw RC_NOACCESS;
			if ((pinDescr&HOH_ISPART)!=0 && (mode&MODE_CASCADE)==0) throw RC_INVOP;
			nProps=pcb->hpin->nProps;
			if ((pinDescr&(HOH_HIDDEN|HOH_NOINDEX|HOH_DELETED))==0) {
				if ((rc=ctx->classMgr->classify(pcb,clr))!=RC_OK || clr.nClasses>0 &&
					(rc=ctx->classMgr->index(ses,pcb,clr,!fPurge?CI_SDELETE:(pinDescr&HOH_DELETED)==0?CI_DELETE:CI_PURGE))!=RC_OK) throw rc;
				if ((pinDescr&HOH_FT)!=0) {
					if ((ftl=new(sft.malloc(sizeof(FTList))) FTList(sft))==NULL) throw RC_NORESOURCES;
					fti.id=pid; fti.newV=NULL; fti.oldV=&v; fti.docID=PIN::defPID; fti.propID=STORE_INVALID_PROPID; fti.eid=STORE_COLLECTION_ID;
					if ((hprop=pcb->hpin->findProperty(PROP_SPEC_DOCUMENT))!=NULL && hprop->type.getType()==VT_REFID) {
						if ((rc=loadVH(v,hprop,*pcb,0,NULL))!=RC_OK) throw rc;
						fti.docID=v.id; assert(v.type==VT_REFID);
					}
				}
			}
			const bool fNotify=(pinDescr&HOH_HIDDEN)==0 && notification!=NULL && ((clr.notif&CLASS_NOTIFY_DELETE)!=0 || (pinDescr&(HOH_REPLICATED|HOH_NOTIFICATION))!=0);
			if (nProps!=0) {
				if (fNotify) {
					if ((vals=(Value*)ses->malloc((nvals=nProps)*(sizeof(Value)+sizeof(IStoreNotification::NotificationData))))==NULL) throw RC_NORESOURCES;
					ndata=(IStoreNotification::NotificationData*)(vals+nvals); memset(vals,0,nProps*sizeof(Value));
				}
				if (fNotify || ftl!=NULL || (pinDescr&HOH_PARTS)!=0 || fPurge && (pinDescr&HOH_SSVS)!=0) for (hprop=pcb->hpin->getPropTab(),i=0; i<nProps; ++hprop,++i) {
					const bool fFT=ftl!=NULL && (hprop->type.flags&META_PROP_NOFTINDEX)==0; Value *pv=&v;
					if (fNotify) {
						if ((rc=loadVH(vals[i],hprop,*pcb,0,ses))!=RC_OK) throw rc;
						ndata[i].propID=hprop->getID(); ndata[i].eid=vals[i].eid; ndata[i].epos=STORE_COLLECTION_ID;
						ndata[i].newValue=NULL; ndata[i].oldValue=pv=&vals[i];
					}
					switch (hprop->type.getType()) {
					default: break;
					case VT_STRING:
						if (fFT && hprop->type.getFormat()!=HDF_COMPACT) {
							fti.propID=hprop->getID(); fti.oldV=pv;
							if (!fNotify && (rc=loadV(v,hprop->type,hprop->offset,pcb->hp,0,NULL))!=RC_OK ||
								(rc=ctx->ftMgr->index(fti,ftl,IX_OFT,(hprop->type.flags&META_PROP_STOPWORDS)!=0?FTMODE_STOPWORDS:0,ses))!=RC_OK) throw rc;
						}
						break;
					case VT_REFID:
						if ((hprop->type.flags&META_PROP_PART)!=0) {
							if (!fNotify && (rc=loadV(v,hprop->type,hprop->offset,pcb->hp,0,NULL))!=RC_OK ||
								(rc=mv_bins<PID,unsigned>(parts,nParts,pv->id,ses,&xParts))!=RC_OK) throw rc;
						}
						break;
					case VT_STREAM:
						switch (hprop->type.getFormat()) {
						case HDF_COMPACT: break;
						case HDF_LONG:
			//				memcpy(&addr.pageID,&((HLOB*)pData)->ref.pageID,sizeof(PageID)); addr.idx=((HLOB*)pData)->ref.idx;
			//				memcpy(&l64,&((HLOB*)pData)->len,sizeof(uint64_t));
			//				if (ma==NULL && (ma=Session::getSession())==NULL && (ma=StoreCtx::get())==NULL) return RC_NOSESSION;
			//				if ((pstr=new(ma) StreamX(addr,l64,ty=((HLOB*)pData)->ref.type.getType(),ma))==NULL) return RC_NORESOURCES;
							break;
						default:
			//				v.flags|=VF_SSV; Session *ses=Session::getSession();
			//				if ((mode&LOAD_SSV)!=0 && (rc=loadSSVs(&v,1,mode,ses,ma!=NULL?ma:ses))!=RC_OK) return rc;
							break;
						}
						break;
					case VT_ARRAY:
						if (fFT || (hprop->type.flags&META_PROP_PART)!=0 || (pinDescr&HOH_SSVS)!=0 && hprop->type.getFormat()!=HDF_LONG) {
							if (!fNotify && (rc=loadVH(v,hprop,*pcb,0,ses))!=RC_OK) throw rc;
							for (unsigned j=0;;j++) {
								const Value *cv=NULL;
								if (pv->type==VT_ARRAY) {
									if (j<pv->length) cv=&pv->varray[j]; else break;
								} else if (pv->type==VT_COLLECTION) {
									if ((cv=((IntNav*)pv->nav)->navigateNR(j==0?GO_FIRST:GO_NEXT))==NULL) break;
								} else if (j==0) cv=pv; else break;
								if ((cv->flags&VF_SSV)!=0) {
									//...
								} else switch (cv->type) {
								case VT_STRING: 
									if (fFT) {
										fti.oldV=cv; fti.propID=hprop->getID();
										if ((rc=ctx->ftMgr->index(fti,ftl,IX_OFT,(hprop->type.flags&META_PROP_STOPWORDS)!=0?FTMODE_STOPWORDS:0,ses))!=RC_OK) throw rc;
									}
									break;
								case VT_REFID:
									if ((hprop->type.flags&META_PROP_PART)!=0 && (rc=mv_bins<PID,unsigned>(parts,nParts,cv->id,ses,&xParts))!=RC_OK) throw rc;
									break;
								}
							}
						}
						break;
					}
				}
			}
			byte *body=NULL; size_t lBody=0; 
			if (fPurge && (rc=pcb->hpin->serialize(body,lBody,pcb->hp,ses))!=RC_OK ||
				(rc=ctx->txMgr->update(pcb->pb,ctx->heapMgr,(ulong)pcb->addr.idx<<HPOP_SHIFT|hop,body,lBody))!=RC_OK) throw rc;
			if (fPurge) {ses->free(body); ctx->heapMgr->reuse(pcb->pb,ses,threshold,true);}
			pcb->pb.release(); pcb=NULL;
			if (nSSVs!=0 && (fNotify || ftl!=NULL)) {
				if ((rc=loadSSVs(SSVs,nSSVs,0,ses,ses))!=RC_OK) throw rc;
				if (ftl!=NULL) {
					//...
				}
				if (fNotify) {
					//...
				}
			}
			if ((pinDescr&(HOH_HIDDEN|HOH_NOINDEX|HOH_DELETED))==0) {
				if (cid!=STORE_INVALID_CLASSID && (rc=ctx->classMgr->remove(cid))!=RC_OK) throw rc;
				if (ftl!=NULL) {
					if ((rc=ctx->ftMgr->process(*ftl,pid,fti.docID))!=RC_OK) throw rc;
					sft.release(); ftl=NULL;
				}
			}
			if (fNotify) {
				IStoreNotification::NotificationEvent evt={pid,NULL,0,ndata,nProps,fRepSes};
				if ((evt.events=(IStoreNotification::EventData*)ses->malloc((clr.nClasses+1)*sizeof(IStoreNotification::EventData)))!=NULL) {
					evt.nEvents=0;
					if ((pinDescr&(HOH_REPLICATED|HOH_NOTIFICATION))!=0) {
						IStoreNotification::EventData& ev=(IStoreNotification::EventData&)evt.events[0];
						ev.cid=STORE_INVALID_CLASSID; ev.type=IStoreNotification::NE_PIN_DELETED; evt.nEvents++;
					}
					if ((clr.notif&CLASS_NOTIFY_DELETE)!=0) for (i=0; i<clr.nClasses; i++) if ((clr.classes[i]->notifications&CLASS_NOTIFY_DELETE)!=0) {
						IStoreNotification::EventData& ev=(IStoreNotification::EventData&)evt.events[evt.nEvents++];
						ev.type=IStoreNotification::NE_CLASS_INSTANCE_REMOVED; ev.cid=clr.classes[i]->cid;
					}
					if (evt.nEvents>0) {
						if ((pinDescr&HOH_NOTIFICATION)!=0 || clr.nClasses>0) try {ctx->queryMgr->notification->notify(&evt,1,ses->getTXID());} catch (...) {}
//						if ((pinDescr&HOH_REPLICATED)!=0 && !fRepSes) try {ctx->queryMgr->notification->replicationNotify(&evt,1,ses->getTXID());} catch (...) {}
					}
					if (evt.events!=NULL) ses->free((void*)evt.events);
				}
				if (vals!=NULL) {freeV(vals,nvals,ses); vals=NULL; nvals=0;}
			}
			if (nSSVs>0 && fPurge) {
#if 0
				for (ulong j=0; rc==RC_OK && j<nSSVs; j++) switch (refs[j].pv->type) {
				default: addr.pageID=refs[j].href.pageID; addr.idx=refs[j].href.idx; rc=deleteData(addr); break;
				case VT_STREAM: assert((refs[j].pv->flags&VF_SSV)==0); rc=((StreamX*)refs[j].pv->stream.is)->deleteData(ses); break;
				case VT_COLLECTION: if (refs[j].pv->nav!=NULL) rc=((Navigator*)refs[j].pv->nav)->deleteData(ses); break;
				}
#endif
				for (i=0; i<nSSVs; i++) freeV(SSVs[i]); nSSVs=0;
			}
			clr.nClasses=clr.nIndices=clr.notif=0;
			if (nParts!=0 && (rc=deletePINs(ses,NULL,parts,nParts,mode|MODE_CASCADE))!=RC_OK) throw rc;
			if (fDelFwd) do {
				cb.pb=ctx->bufMgr->getPage(fwd.pageID,ctx->heapMgr,PGCTL_XLOCK,cb.pb); if (cb.pb.isNull()) break;
				cb.hp=(const HeapPageMgr::HeapPage *)cb.pb->getPageBuf();
				const HeapPageMgr::HeapObjHeader *hdr=cb.hp->getObject(cb.hp->getOffset(fwd.idx));
				if (hdr==NULL || hdr->getType()!=HO_FORWARD) break; memcpy(fwdbuf,hdr,sizeof(fwdbuf));
				if ((rc=ctx->txMgr->update(cb.pb,ctx->heapMgr,(ulong)fwd.idx<<HPOP_SHIFT|hop,fwdbuf,sizeof(fwdbuf)))!=RC_OK) throw rc;
				if (fPurge)	ctx->heapMgr->reuse(cb.pb,ses,threshold,true);
				memcpy(&fwd,fwdbuf+sizeof(HeapPageMgr::HeapObjHeader),sizeof(PageAddr));
			} while (fwd!=deleted);
		}
		cb.pb.release(); tx.ok();
	} catch (RC rc2) {rc=rc2;}
	if (parts!=NULL) ses->free(parts);
	if (vals!=NULL) freeV(vals,nvals,ses);
	if (SSVs!=NULL) freeV(SSVs,nSSVs,ses);
	return rc;
}
