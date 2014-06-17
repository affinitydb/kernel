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

#include "txmgr.h"
#include "lock.h"
#include "session.h"
#include "queryprc.h"
#include "startup.h"
#include "netmgr.h"
#include "dataevent.h"
#include "ftindex.h"
#include "blob.h"
#include "maps.h"

using namespace AfyKernel;

RC QueryPrc::deletePINs(const EvalCtx& ectx,const PIN *const *pins,const PID *pids,unsigned nPins,unsigned mode,PINx *pcb)
{
	if (pins==NULL&&pids==NULL || nPins==0) return RC_OK; 
	Session *const ses=ectx.ses; if (ses==NULL) return RC_NOSESSION; if (ses->isRestore()) return RC_OTHER;
	if (ctx->isServerLocked()) return RC_READONLY; if (ses->inReadTx()) return RC_READTX;
	DetectedEvents clr(ses,ctx); TxSP tx(ses); RC rc=tx.start(TXI_DEFAULT,nPins==1?TX_ATOMIC:0); if (rc!=RC_OK) return rc; 
	bool fRepSes=ses->isReplication(); PINx cb(ses); if (pcb!=NULL && pcb->pb.isNull()) pcb=NULL;
	const size_t threshold=ceil(size_t((HeapPageMgr::contentSize(ctx->bufMgr->getPageSize())-sizeof(PageOff))*(1.-ctx->theCB->pctFree)),HP_ALIGN);
	DynOArray<PID> parts((MemAlloc*)ses); DynArray<Value> SSVs((MemAlloc*)ses); StackAlloc sft(ses); Value *vals=NULL; unsigned nvals=0;
	const unsigned flgs=(mode&(MODE_DELETED|MODE_PURGE))!=0?GB_DELETED|GB_FORWARD:GB_FORWARD; const bool fPurge=(mode&MODE_PURGE)!=0; 

	try {
		for (unsigned np=0; np<nPins; pcb=NULL,parts.clear()) {
			PID pid=pids[np++]; if (pcb==NULL) {cb=pid; cb=PageAddr::noAddr; pcb=&cb;}
			if (pcb->id.isPtr()) {
				//PIN *pin=(PIN*)pcb->id.pid;
				//????
				continue;
			}
			if (isRemote(pid) && fPurge && (rc=ctx->netMgr->remove(pid,pcb->addr))!=RC_OK) throw rc!=RC_FALSE?rc:RC_NOTFOUND;
			byte fwdbuf[sizeof(HeapPageMgr::HeapObjHeader)+PageAddrSize]; bool fDelFwd=false; PageAddr fwd=PageAddr::noAddr,deleted=PageAddr::noAddr;
			FTList *ftl=NULL; ChangeInfo fti; unsigned i; Value v; IStoreNotification::NotificationData *ndata=NULL;
			for (;;) {
				if (pcb->pb.isNull() && (rc=pcb->getBody(TVO_UPD,flgs))!=RC_OK) throw rc;
				if (pcb->hpin->hdr.getType()==HO_FORWARD) {
					memcpy(fwdbuf,pcb->hpin,sizeof(fwdbuf)); if (!fwd.defined()) fwd=pcb->addr;
					if ((rc=ctx->txMgr->update(pcb->pb,ctx->heapMgr,(unsigned)pcb->addr.idx<<HPOP_SHIFT|(fPurge?HPOP_PURGE:HPOP_DELETE),fwdbuf,sizeof(fwdbuf)))!=RC_OK) throw rc;
					if (fPurge)	ctx->heapMgr->reuse(pcb->pb,ses,threshold,true); memcpy(&cb.addr,fwdbuf+sizeof(HeapPageMgr::HeapObjHeader),PageAddrSize);
					if (cb.addr.defined()) {pcb=&cb; cb=pid; cb.pb.release(ses); cb.mode|=PINEX_ADDRSET; continue;}
				} else if (pcb->hpin->isMigrated()) {
					memcpy(fwdbuf,pcb->hpin->getOrigLoc(),PageAddrSize);
					if (fwd!=*(PageAddr*)fwdbuf) {fwd=*(PageAddr*)fwdbuf; deleted=pcb->addr; fDelFwd=true;}
				}
				break;
			}
			if (pcb->hpin==NULL || pcb->hpin->hdr.getType()!=HO_PIN) break;
			const HeapPageMgr::HeapPage *hp=(const HeapPageMgr::HeapPage*)pcb->pb->getPageBuf(); const HeapPageMgr::HeapV *hprop;
			ushort pinDescr=pcb->hpin->hdr.descr; unsigned nProps=pcb->hpin->nProps; DataEventID cid=STORE_INVALID_URIID;
			if ((pcb->hpin->meta&PMT_COMM)!=0) 	ses->removeServiceCtx(pid);
			if ((pcb->hpin->meta&PMT_DATAEVENT)!=0 && ((mode&MODE_DEVENT)==0 || (hprop=pcb->hpin->findProperty(PROP_SPEC_OBJID))!=NULL &&
				pcb->loadVH(v,hprop,0,ses)==RC_OK && v.type==VT_URIID && ((cid=v.uid)==CLASS_OF_DATAEVENTS||cid==CLASS_OF_PACKAGES||cid==CLASS_OF_NAMED))) throw RC_NOACCESS;
			if (!fPurge && (pinDescr&HOH_DELETED)!=0) break;
			if ((mode&MODE_CHECK_STAMP)!=0 && pins!=NULL && np<nPins) {
				// check stamp && pcb->hpin->getStamp()!=pins[np]->stamp) throw RC_REPEAT;
			}
			if ((pinDescr&(HOH_HIDDEN|HOH_DELETED))==0 && (pinDescr&HOH_FT)!=0) {
				if ((ftl=new(sft.malloc(sizeof(FTList))) FTList(sft))==NULL) throw RC_NOMEM;
				fti.id=pid; fti.newV=NULL; fti.oldV=&v; fti.docID=PIN::noPID; fti.propID=STORE_INVALID_URIID; fti.eid=STORE_COLLECTION_ID;
				if ((hprop=pcb->hpin->findProperty(PROP_SPEC_DOCUMENT))!=NULL && hprop->type.getType()==VT_REFID) {
					if ((rc=pcb->loadVH(v,hprop,0,NULL))!=RC_OK) throw rc;
					fti.docID=v.id; assert(v.type==VT_REFID);
				}
			}
			const bool fNotify=(pinDescr&HOH_HIDDEN)==0 && notification!=NULL && (clr.notif&CLASS_NOTIFY_DELETE)!=0; bool fSSV=false;
			if (nProps!=0) {
				if (fNotify) {
					if ((vals=(Value*)ses->malloc((nvals=nProps)*(sizeof(Value)+sizeof(IStoreNotification::NotificationData))))==NULL) throw RC_NOMEM;
					ndata=(IStoreNotification::NotificationData*)(vals+nvals); memset(vals,0,nProps*sizeof(Value));
				}
				if (fNotify || ftl!=NULL || (pinDescr&HOH_PARTS)!=0 || fPurge && (pinDescr&HOH_SSVS)!=0) for (hprop=pcb->hpin->getPropTab(),i=0; i<nProps; ++hprop,++i) {
					const bool fFT=ftl!=NULL && (hprop->type.flags&META_PROP_FTINDEX)!=0; Value *pv=&v; HeapDataFmt hfmt; PageAddr daddr;
					if (fNotify) {
						if ((rc=pcb->loadVH(vals[i],hprop,0,ses))!=RC_OK) throw rc;
						ndata[i].propID=hprop->getID(); ndata[i].eid=vals[i].eid; ndata[i].epos=STORE_COLLECTION_ID; ndata[i].newValue=NULL; ndata[i].oldValue=pv=&vals[i];
					}
					switch (hprop->type.getType()) {
					default: break;
					case VT_STRING:
						if (fFT && hprop->type.getFormat()!=HDF_COMPACT) {
							fti.propID=hprop->getID(); fti.oldV=pv;
							if (!fNotify && (rc=loadS(v,hprop->type,hprop->offset,hp,0,NULL))!=RC_OK ||
								(rc=ctx->ftMgr->index(fti,ftl,IX_OFT,/*(hprop->type.flags&META_PROP_STOPWORDS)!=0?*/FTMODE_STOPWORDS,ses))!=RC_OK) throw rc;
						}
						break;
					case VT_REFID:
						if ((hprop->type.flags&META_PROP_PART)!=0 && (!fNotify && (rc=loadS(v,hprop->type,hprop->offset,hp,0,NULL))!=RC_OK || (rc=parts+=pv->id)!=RC_OK)) throw rc;
						break;
					case VT_STREAM:
						if ((hfmt=hprop->type.getFormat())!=HDF_COMPACT) {
							const byte *pData=(const byte*)hp+hprop->offset;
							if (fPurge) {
								daddr.pageID=__una_get(((HRefSSV*)pData)->pageID); daddr.idx=((HRefSSV*)pData)->idx; 
								if ((rc=ses->queueForPurge(daddr))!=RC_OK) throw rc;
							}
							if (fNotify) fSSV=true;
							else if (fFT && ((HRefSSV*)pData)->type.getType()==VT_STRING) {
								if ((rc=loadS(v,hprop->type,hprop->offset,hp,0,NULL))!=RC_OK) throw rc;
								v.setPropID(hprop->getPropID()); fSSV=true; if ((rc=SSVs+=v)!=RC_OK) throw rc;
							}
						}
						break;
					case VT_COLLECTION:
						if (fPurge && hprop->type.getFormat()==HDF_LONG) {
							daddr.pageID=__una_get(((HeapPageMgr::HeapExtCollection*)((byte*)hp+hprop->offset))->anchor); daddr.idx=0;
							if ((rc=ses->queueForPurge(daddr,TXP_SSV,(byte*)hp+hprop->offset))!=RC_OK) throw rc;
						}
						if (fFT || (hprop->type.flags&META_PROP_PART)!=0 || fPurge && (pinDescr&HOH_SSVS)!=0 && hprop->type.getFormat()!=HDF_LONG) {
							if (!fNotify && (rc=pcb->loadVH(v,hprop,0,ses))!=RC_OK) throw rc;
							for (unsigned j=0;;j++) {
								const Value *cv=NULL;
								if (pv->type==VT_COLLECTION) {
									if (!pv->isNav()) {if (j<pv->length) cv=&pv->varray[j]; else break;}
									else if ((cv=pv->nav->navigate(j==0?GO_FIRST:GO_NEXT))==NULL) {pv->nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID); break;}
								} else if (j==0) cv=pv; else break;
								if ((cv->flags&VF_SSV)!=0) {
									const HRefSSV *href=(const HRefSSV *)&cv->id; assert(cv->type==VT_STREAM||isComposite((ValueType)cv->type));
									if (fPurge && (pv->type!=VT_COLLECTION || !pv->isNav()))		//??????????????????
										{daddr.pageID=href->pageID; daddr.idx=href->idx; if ((rc=ses->queueForPurge(daddr))!=RC_OK) throw rc;}
									if (fNotify) fSSV=true;
									else if (fFT && href->type.getType()==VT_STRING) {fSSV=true; v=*cv; v.setPropID(hprop->getPropID()); if ((rc=SSVs+=v)!=RC_OK) throw rc;}
								} else switch (cv->type) {
								case VT_STRING: 
									if (fFT) {
										fti.oldV=cv; fti.propID=hprop->getID();
										if ((rc=ctx->ftMgr->index(fti,ftl,IX_OFT,/*(hprop->type.flags&META_PROP_STOPWORDS)!=0?*/FTMODE_STOPWORDS,ses))!=RC_OK) throw rc;
									}
									break;
								case VT_REFID:
									if ((hprop->type.flags&META_PROP_PART)!=0 && (rc=parts+=cv->id)!=RC_OK) throw rc;
									break;
								}
							}
						}
						break;
					case VT_STRUCT:
						// PROP_SPEC_REF & META_PROP_PART
						// VT_STRING fields for fFT
						//???
						break;
					}
				}
			}

			if ((rc=fPurge?ses->queueForPurge(pcb->addr,TXP_PIN):
				ctx->txMgr->update(pcb->pb,ctx->heapMgr,(unsigned)pcb->addr.idx<<HPOP_SHIFT|(fPurge?HPOP_PURGE:HPOP_DELETE)))!=RC_OK) throw rc;

			if ((pinDescr&HOH_DELETED)==0) {
				if ((rc=ctx->classMgr->detect(pcb,clr,ses))!=RC_OK || 
					clr.ndevs>0 && (rc=ctx->classMgr->updateIndex(ses,pcb,clr,!fPurge?CI_SDELETE:(pinDescr&HOH_DELETED)==0?CI_DELETE:CI_PURGE))!=RC_OK ||
					clr.nActions!=0 && (rc=clr.publish(ses,pcb,CI_DELETE,&ectx))!=RC_OK) throw rc;
			}
			if (pcb==&cb) {pcb->pb.release(ses); pcb=NULL;}

			if (fSSV) {
				const Value *pv=vals; unsigned nv=nvals; if (!fNotify) {pv=&SSVs[0]; nvals=SSVs; assert(nvals!=0);}
				if ((rc=PINx::loadSSVs((Value*)pv,nv,0,ses,ses))==RC_OK && ftl!=NULL) {
					for (uint32_t i=0,j; rc==RC_OK && i<nv; i++) if ((pv[i].meta&META_PROP_FTINDEX)!=0) switch (pv[i].type) {
					case VT_STRING:
						fti.propID=pv[i].property; fti.oldV=&pv[i];
						rc=ctx->ftMgr->index(fti,ftl,IX_OFT,/*(pv[i].meta&META_PROP_STOPWORDS)!=0?*/FTMODE_STOPWORDS,ses); break;
					case VT_COLLECTION:
						if (!pv[i].isNav()) for (j=0; rc==RC_OK && j<pv[i].length; j++) if (pv[i].varray[j].type==VT_STRING) {
							fti.propID=pv[i].property; fti.oldV=&pv[i].varray[j]; 
							rc=ctx->ftMgr->index(fti,ftl,IX_OFT,/*(pv[i].meta&META_PROP_STOPWORDS)!=0?*/FTMODE_STOPWORDS,ses);
						}
						break;
					}
				}
				if (!fNotify) {for (unsigned i=0; i<nv; i++) freeV(*(Value*)&pv[i]); SSVs.clear();}
				if (rc!=RC_OK) throw rc;
			}
			if ((pinDescr&HOH_DELETED)==0) {
				if (cid!=STORE_INVALID_URIID) {DropDataEvent *cd=new(ses) DropDataEvent(cid); if (cd==NULL || ses->addOnCommit(cd)!=RC_OK) throw RC_NOMEM;}
				if (ftl!=NULL) {
					if ((rc=ctx->ftMgr->process(*ftl,pid,fti.docID))!=RC_OK) throw rc;
					sft.truncate(TR_REL_ALL); ftl=NULL;
				}
			}
//			if (replication!=NULL) {}
			if (fNotify) {
				IStoreNotification::NotificationEvent evt={pid,NULL,0,ndata,nProps,fRepSes};
				if ((evt.events=(IStoreNotification::EventData*)ses->malloc((clr.ndevs+1)*sizeof(IStoreNotification::EventData)))!=NULL) {
					evt.nEvents=0;
					if ((clr.notif&CLASS_NOTIFY_DELETE)!=0) for (i=0; i<clr.ndevs; i++) if ((clr.devs[i]->notifications&CLASS_NOTIFY_DELETE)!=0) {
						IStoreNotification::EventData& ev=(IStoreNotification::EventData&)evt.events[evt.nEvents++];
						ev.type=IStoreNotification::NE_CLASS_INSTANCE_REMOVED; ev.cid=clr.devs[i]->cid;
					}
					try {ctx->queryMgr->notification->notify(&evt,1,ses->getTXID());} catch (...) {}
					if (evt.events!=NULL) ses->free((void*)evt.events);
				}
				if (vals!=NULL) {freeV(vals,nvals,ses); vals=NULL; nvals=0;}
			}
			clr.ndevs=clr.nIndices=clr.notif=0;
			if ((unsigned)parts!=0 && (rc=deletePINs(ectx,NULL,&parts[0],parts,mode|MODE_CASCADE))!=RC_OK) throw rc;
			if (fDelFwd) do {
				cb=fwd;
				if (cb.pb.getPage(fwd.pageID,ctx->heapMgr,PGCTL_XLOCK|PGCTL_RLATCH,ses)==NULL) break;
				if (cb.fill()==NULL || cb.hpin->hdr.getType()!=HO_FORWARD) break; memcpy(fwdbuf,cb.hpin,sizeof(fwdbuf));
				if ((rc=ctx->txMgr->update(cb.pb,ctx->heapMgr,(unsigned)fwd.idx<<HPOP_SHIFT|(fPurge?HPOP_PURGE:HPOP_DELETE),fwdbuf,sizeof(fwdbuf)))!=RC_OK) throw rc;
				if (fPurge)	ctx->heapMgr->reuse(cb.pb,ses,threshold,true);
				memcpy(&fwd,fwdbuf+sizeof(HeapPageMgr::HeapObjHeader),PageAddrSize);
			} while (fwd!=deleted);
		}
		cb.pb.release(ses); tx.ok();
	} catch (RC rc2) {rc=rc2;}
	if (vals!=NULL) freeV(vals,nvals,ses);
	return rc;
}

RC QueryPrc::purge(PageID pageID,unsigned start,unsigned len,const uint32_t *bmp,PurgeType pt,Session *ses)
{
	if (start==0xFFFF) return Collection::purge((const HeapPageMgr::HeapExtCollection *)bmp,ctx);

	PageAddr addr={pageID,INVALID_INDEX}; RC rc=RC_OK; PBlockP pb;
	if (pb.getPage(pageID,pt==TXP_SSV?(PageMgr*)ctx->ssvMgr:(PageMgr*)ctx->heapMgr,PGCTL_XLOCK|PGCTL_RLATCH,ses)==NULL) return RC_NOTFOUND;
	const HeapPageMgr::HeapPage *hp=(const HeapPageMgr::HeapPage*)pb->getPageBuf();
	for (unsigned j=0; rc==RC_OK && j<len; j++) for (uint32_t w=bmp[j],c=0; w!=0; ++c,w>>=1) if ((w&1)!=0) {
		addr.idx=ushort((start+j)*sizeof(uint32_t)*8+c);
		const HeapPageMgr::HeapObjHeader *hobj=(const HeapPageMgr::HeapObjHeader*)hp->getObject(hp->getOffset(addr.idx));
		if (hobj==NULL) {
			// report error
		} else if (pt!=TXP_SSV) {
			byte *body=NULL; size_t lBody=0; if ((rc=((HeapPageMgr::HeapPIN*)hobj)->serialize(body,lBody,hp,ses))!=RC_OK) break;
			rc=ctx->txMgr->update(pb,ctx->heapMgr,(unsigned)addr.idx<<HPOP_SHIFT|HPOP_PURGE,body,lBody); ses->free(body); if (rc!=RC_OK) break;
		} else if ((hobj->getType()!=HO_BLOB || ctx->bigMgr->canBePurged(addr)) && (rc=deleteData(addr,ses,&pb))!=RC_OK) break;
	}
	if (rc==RC_OK && pt!=TXP_SSV) {
		const size_t threshold=ceil(size_t((HeapPageMgr::contentSize(ctx->bufMgr->getPageSize())-sizeof(PageOff))*(1.-ctx->theCB->pctFree)),HP_ALIGN);
		ctx->heapMgr->reuse(pb,ses,threshold,true);
	}
	return rc;
}
