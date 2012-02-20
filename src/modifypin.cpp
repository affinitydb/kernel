/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "buffer.h"
#include "txmgr.h"
#include "lock.h"
#include "fsmgr.h"
#include "queryprc.h"
#include "stmt.h"
#include "expr.h"
#include "ftindex.h"
#include "startup.h"
#include "blob.h"
#include "classifier.h"
#include "netmgr.h"
#include "maps.h"

using namespace AfyKernel;

namespace AfyKernel
{
#ifndef FORCE_SPLIT
	#define FORCE_SPLIT	false
#endif

#define	START_BUF_SIZE	0x200

#define MF_REPSES		0x00000001
#define	MF_MODDOC		0x00000002
#define	MF_PREFIX		0x00000004
#define	MF_NOTIFY		0x00000008
#define	MF_CNOTIFY		0x00000010
#define	MF_SSVS			0x00000020
#define	MF_BIGC			0x00000040
#define	MF_LASTUPD		0x00000080
#define	MF_UPDBY		0x00000100
#define	MF_DELSSV		0x00000200
#define	MF_REMOTE		0x00000400
#define	MF_LOCEID		0x00000800
#define	MF_MOVED		0x00001000
#define	MF_MIGRATE		0x00002000
#define	MF_REFRESH		0x00004000
#define	MF_ADDACL		0x00008000

struct ExtCollectionInfo
{
	ExtCollectionInfo	*next;
	Collection			*pc;
};

struct HRefInfo
{
	HRefSSV				href;
	HRefInfo			*next;
};

struct ModData : public SubAlloc
{
	PropInfo			**ppi;
	unsigned			npi;

	ModInfo				*list;
	ModInfo				*last;
	unsigned			nev;

	HRefInfo			*hRef;
	ExtCollectionInfo	*ext;
	ulong				flags;

	ModData(size_t s,byte *bf,unsigned np,Session *ses) : SubAlloc(ses,s,bf,true),ppi(alloc<PropInfo*>((np+1)*sizeof(PropInfo*))),npi(0),
		list(NULL),last(NULL),nev(0),hRef(NULL),ext(NULL),flags(0) {}
	~ModData() {for (ExtCollectionInfo *ei=ext; ei!=NULL; ei=ei->next) ei->pc->destroy();}
	ModInfo *findMod(PropInfo *pi,ElementID eid,const byte *hp,PageOff offset) {
		if (eid==STORE_FIRST_ELEMENT || eid==STORE_LAST_ELEMENT) {
			if ((pi->flags&PM_COLLECTION)==0) return eid==STORE_FIRST_ELEMENT?pi->first:pi->last;
			assert(hp!=NULL);
			if ((pi->flags&PM_BIGC)!=0) {
				const HeapPageMgr::HeapExtCollection *hc=(const HeapPageMgr::HeapExtCollection *)(hp+offset);
				eid=eid==STORE_FIRST_ELEMENT?hc->firstID:hc->lastID;
			} else {
				const HeapPageMgr::HeapV *elt=((HeapPageMgr::HeapVV *)(hp+offset))->findElt(eid);
				if (elt!=NULL) eid=elt->getID(); else return NULL;
			}
		}
		if (eid!=STORE_COLLECTION_ID) {for (ModInfo *mi=pi->first; mi!=NULL; mi=mi->pnext) if (mi->eltKey==eid) return mi;}
		else if ((pi->flags&(PM_COLLECTION|PM_NEWCOLL))==0 || (pi->flags&PM_SCOLL)!=0 || pi->last!=NULL && pi->last->eltKey==STORE_COLLECTION_ID) return pi->last;
		return NULL;
	}
	ModInfo *addMod(const Value *pv,unsigned n,PropInfo *pi,ExprOp op) {
		ModInfo *mi=alloc<ModInfo>(); if (mi==NULL||pv==NULL) return NULL;
		mi->pv=pv; mi->pvIdx=n; mi->pInfo=pi; mi->epos=pv->eid;
		if (op!=OP_RENAME && op!=OP_MOVE && op!=OP_MOVE_BEFORE) {
			if (pi->first==NULL) pi->first=pi->last=mi; else pi->last=pi->last->pnext=mi;
			pi->nElts+=pv->type==VT_ARRAY?pv->length:pv->type==VT_COLLECTION?pv->nav->count():1;
		}
		if (last==NULL) list=last=mi; else last=last->next=mi; 
		nev++; return mi;
	}
	RC addPropMod(PropertyID pid,const HeapPageMgr::HeapPIN *hpin,ModInfo *&mi,ulong prefix) {
		mi=NULL; const HeapPageMgr::HeapV *hprop=hpin->findProperty(pid); if (hprop==NULL) return RC_OK;
		PropInfo **pins=NULL,*pi=(PropInfo*)BIN<PropInfo,PropertyID,PropInfo::PropInfoCmp>::find(pid,(const PropInfo**)ppi,npi,(const PropInfo***)&pins); assert(pi==NULL);
		if ((pi=alloc<PropInfo>())==NULL) return RC_NORESOURCES;
		if (pins<&ppi[npi]) memmove(pins+1,pins,(byte*)&ppi[npi]-(byte*)pins); *pins=pi; npi++;
		pi->propID=pid; pi->hprop=hprop; pi->first=pi->last=NULL;
		pi->nElts=1; pi->maxKey=prefix; pi->delta=pi->nDelta=0; pi->pcol=NULL;
		pi->flags=(hprop->type.flags&META_PROP_NOFTINDEX)==0?PM_FTINDEXABLE|PM_OLDFTINDEX:0;
		if ((mi=alloc<ModInfo>())==NULL || (mi->pv=mi->newV=alloc<Value>())==NULL || (mi->oldV=alloc<Value>())==NULL) return RC_NORESOURCES;
		mi->pInfo=pi; mi->eid=mi->epos=STORE_COLLECTION_ID; mi->newV->setError(pid); mi->oldV->setError(pid);
		pi->first=pi->last=mi; if (last==NULL) list=last=mi; else last=last->next=mi;
		return RC_OK;
	}
	HRefSSV *addHRef(const HRefSSV *h) {HRefInfo *hr=alloc<HRefInfo>(); hr->href=*h; hr->next=hRef; hRef=hr; return &hr->href;}
};

};

RC QueryPrc::modifyPIN(Session *ses,const PID& id,const Value *v,unsigned nv,PINEx *pcb,const ValueV& params,PIN *pin,unsigned mode,const ElementID *eids,unsigned *pNFailed)
{
	if (pNFailed!=NULL) *pNFailed=~0u; if (v==NULL || nv==0) return RC_OK;
	if (id.pid==STORE_INVALID_PID || id.ident==STORE_INVALID_IDENTITY) return RC_NOTFOUND;

	if (ses==NULL) return RC_NOSESSION; if (ses->isRestore()) return RC_OTHER;
	if (ctx->isServerLocked()) return RC_READONLY; if (ses->inReadTx()) return RC_READTX;
	
	PINEx cb(ses,id); RC rc=RC_OK; TxSP tx(ses); if ((rc=tx.start(TXI_DEFAULT,TX_ATOMIC))!=RC_OK) return rc;

	if (pcb!=NULL && !pcb->pb.isNull()) cb=pcb->addr;
	else {
		pcb=&cb; if (pin!=NULL) cb.addr=pin->addr; 
		if ((rc=getBody(cb,TVO_UPD))!=RC_OK) {
			if (rc==RC_DELETED && pin!=NULL) pin->mode|=PIN_DELETED;
			return rc;
		}
		if ((mode&MODE_CHECK_STAMP)!=0 && pin!=NULL && pin->stamp!=cb.hpin->getStamp()) return RC_REPEAT;
	}

	ushort pinDescr=pcb->hpin->hdr.descr,newDescr=0; ulong k; const Value *pv; Value w,*opv;
	if ((mode&MODE_REFRESH)==0 && (pinDescr&HOH_REPLICATED)==0 && isRemote(id)) return RC_NOACCESS;
	PageAddr oldAddr=pcb->addr,origAddr=PageAddr::invAddr; if (pin!=NULL) pin->addr=pcb->addr;
	
	unsigned np=nv; if (np>20) for (unsigned i=np=1; i<nv; i++) if (v[i].property!=v[i-1].property) np++;
	byte mdb[START_BUF_SIZE]; ModData md(sizeof(mdb),mdb,np,ses); PBlockP pageSSV;

	if (isRemote(id)) md.flags|=MF_REMOTE; if (pcb->hpin->isMigrated()) md.flags|=MF_MOVED;
	if (ses->isReplication()) md.flags|=MF_REPSES; else if ((md.flags&MF_REMOTE)!=0 && (pinDescr&HOH_REPLICATED)!=0) md.flags|=MF_LOCEID;

	size_t xSize=HeapPageMgr::contentSize(ctx->bufMgr->getPageSize())-sizeof(PageOff);
	PID oldDoc=PIN::defPID,newDoc=PIN::defPID; ModInfo *mi; unsigned n; PropertyID xPropID=STORE_INVALID_PROPID;
	size_t reserve=ceil(size_t(xSize*(ses->allocCtrl!=NULL?ses->allocCtrl->pctPageFree:ctx->theCB->pctFree)),HP_ALIGN);
	ElementID prefix=ctx->getPrefix(),rprefix=(md.flags&MF_REMOTE)!=0?HeapPageMgr::getPrefix(id):prefix,lprefix=(md.flags&MF_LOCEID)!=0?prefix:rprefix;
	ClassID cid=STORE_INVALID_CLASSID; ClassResult clro(&md,ctx),clrn(&md,ctx),clru(&md,ctx); PropInfo *pi;
	if ((pinDescr&HOH_CLASS)!=0) {
		const HeapPageMgr::HeapV *hp=pcb->hpin->findProperty(PROP_SPEC_CLASSID);
		if (hp!=NULL && loadVH(w,hp,*pcb,0,&md)==RC_OK && w.type==VT_URIID) cid=w.uid;
	}
	for (n=0; n<nv; n++) {
		pv=&v[n]; if (pNFailed!=NULL) *pNFailed=n;
		const PropertyID propID=pv->property; ElementID eid=pv->eid,meid; ValueType ty; bool fNew=false;
		ulong flags=(pinDescr&HOH_NOINDEX)==0&&(pv->meta&META_PROP_NOFTINDEX)==0?PM_FTINDEXABLE:0;
		ExprOp op=(ExprOp)pv->op; if (op>OP_LAST_MODOP) return RC_INVPARAM;
		bool fAdd=op==OP_ADD||op==OP_ADD_BEFORE;
		switch (propID) {
		default: 
			if (propID<=PROP_SPEC_LAST||propID>STORE_MAX_URIID) return RC_INVPARAM;
			if (xPropID==STORE_INVALID_PROPID || propID>xPropID) xPropID=propID;
			break;
		case PROP_SPEC_CREATEDBY:
		case PROP_SPEC_CREATED:
			if ((md.flags&MF_REPSES)!=0) {
				if (pv->type!=(propID==PROP_SPEC_CREATED?VT_DATETIME:VT_IDENTITY)) return RC_TYPE;
				if (fAdd) {op=OP_SET; fAdd=false;} else if (op!=OP_SET && op!=OP_DELETE) return RC_INVPARAM;
			} else if (op!=OP_DELETE) return RC_INVPARAM;
			break;
		case PROP_SPEC_UPDATED:
		case PROP_SPEC_UPDATEDBY:
			if ((md.flags&MF_REPSES)!=0) {
				if (pv->type!=(propID==PROP_SPEC_UPDATED?VT_DATETIME:VT_IDENTITY)) return RC_TYPE;
				if (fAdd) {op=OP_SET; fAdd=false;} else if (op!=OP_SET && op!=OP_DELETE) return RC_INVPARAM;
			} else if (!fAdd) {if (op!=OP_DELETE) return RC_INVPARAM;}
			else if (pcb->hpin->findProperty(propID)!=NULL) return RC_INVPARAM;
			else {
				Value *pv2=md.alloc<Value>(); if (pv2==NULL) continue;
				if (propID==PROP_SPEC_UPDATED) pv2->setNow(); else pv2->setIdentity(ses->getIdentity());
				pv2->setPropID(propID); pv=pv2;
			}
			md.flags|=propID==PROP_SPEC_UPDATED?MF_LASTUPD:MF_UPDBY;
			break;
		case PROP_SPEC_DOCUMENT:
			if (fAdd || op==OP_SET) switch (pv->type) {
			case VT_REF: newDoc=pv->pin->getPID(); break;
			case VT_REFID: newDoc=pv->id; break;
			default: return RC_TYPE;
			} else if (op!=OP_DELETE && op!=OP_RENAME) return RC_INVPARAM;
			md.flags|=MF_MODDOC; break;
		case PROP_SPEC_PARENT:
			//check
			break;
		case PROP_SPEC_VALUE:
			break;
		case PROP_SPEC_ACL:
			if (cid!=STORE_INVALID_CLASSID && (fAdd || op==OP_SET)) md.flags|=MF_ADDACL;
			break;
		//case PROP_SPEC_PREDICATE:
			// rebuild class index
			// break;
		//case PROP_SPEC_CLASS_INFO:
			// check flags
			//break;
		}
		PropInfo **pins=NULL,*pi=(PropInfo*)BIN<PropInfo,PropertyID,PropInfo::PropInfoCmp>::find(propID,(const PropInfo**)md.ppi,md.npi,(const PropInfo***)&pins);
		if (pi==NULL) {
			if ((pi=md.alloc<PropInfo>())==NULL) return RC_NORESOURCES;
			if (pins<&md.ppi[md.npi]) memmove(pins+1,pins,(byte*)&md.ppi[md.npi]-(byte*)pins);
			*pins=pi; md.npi++;
			pi->propID=propID; pi->hprop=pcb->hpin->findProperty(propID); pi->first=pi->last=NULL;
			pi->nElts=pi->flags=0; pi->maxKey=prefix; pi->delta=pi->nDelta=0; pi->pcol=NULL; pi->single=STORE_COLLECTION_ID;
			if (pi->hprop==NULL) pi->flags=PM_NEWPROP|(flags&PM_FTINDEXABLE);
			else {
				if ((pi->hprop->type.flags&META_PROP_NOFTINDEX)==0) pi->flags|=PM_FTINDEXABLE|PM_OLDFTINDEX;
				if (!pi->hprop->type.isCollection()) pi->single=rprefix;
				else switch (pi->hprop->type.getFormat()) {
				case HDF_SHORT: pi->flags|=PM_COLLECTION|PM_SCOLL; pi->single=((HeapPageMgr::HeapVV*)(pcb->pb->getPageBuf()+pi->hprop->offset))->start[0].getID(); break;
				case HDF_LONG: pi->flags|=PM_COLLECTION|PM_BIGC; break;
				default: pi->flags|=PM_COLLECTION; break;
				}
			}
			if ((mode&MODE_FORCE_EIDS)==0) pi->flags|=PM_GENEIDS;
		}
		if (fAdd) {mi=NULL; fNew=true; if (eid==STORE_COLLECTION_ID) eid=STORE_LAST_ELEMENT;}
		else fNew=(mi=md.findMod(pi,eid,pcb->pb->getPageBuf(),pi->hprop!=NULL?pi->hprop->offset:0))==NULL;
		if (pi->hprop!=NULL && (pi->flags&PM_RESET)==0) {if ((pv->meta&META_PROP_IFNOTEXIST)!=0) continue;}
		else if (mi==NULL) {
			if ((pv->meta&META_PROP_IFEXIST)!=0) continue; if (op!=OP_SET && !fAdd) return RC_NOTFOUND;
			if (eid!=STORE_COLLECTION_ID && eid!=STORE_FIRST_ELEMENT && eid!=STORE_LAST_ELEMENT) return RC_INVPARAM;
		}
		switch (op) {
		default: break;
		case OP_RENAME:
			if (pv->type!=VT_URIID||eid!=STORE_COLLECTION_ID) return RC_TYPE;
			if (pi->hprop==NULL) {if ((pv->meta&META_PROP_IFEXIST)!=0) continue; else return RC_NOTFOUND;}
			if (pv->uid!=propID) {
				if (pcb->hpin->findProperty(pv->uid)!=NULL) return RC_ALREADYEXISTS;	// temp; later will merge
				if ((mi=md.addMod(pv,n,pi,op))==NULL) return RC_NORESOURCES;
				mi->eid=STORE_COLLECTION_ID; mi->flags|=PM_MOVE;
			}
			continue;
		case OP_MOVE:
		case OP_MOVE_BEFORE:
			if (pv->type!=VT_UINT) return RC_TYPE;
			if (pi->hprop==NULL||!pi->hprop->type.isCollection()||eid==STORE_COLLECTION_ID)
				{if ((pv->meta&META_PROP_IFEXIST)!=0) continue; else return RC_NOTFOUND;}
			if (eid!=pv->ui) {
				ElementID prev=STORE_FIRST_ELEMENT;
				if (pi->hprop->type.getFormat()==HDF_LONG) {
					Navigator nav(oldAddr,propID,(HeapPageMgr::HeapExtCollection*)(pcb->pb->getPageBuf()+pi->hprop->offset),0,&md);
					const Value *cv;
					if ((cv=nav.navigate(GO_FINDBYID,pv->ui))==NULL)
						{if ((pv->meta&META_PROP_IFEXIST)!=0) continue; else return RC_NOTFOUND;}
					ElementID ei=cv->eid;
					if ((cv=nav.navigate(GO_FINDBYID,eid))==NULL)
						{if ((pv->meta&META_PROP_IFEXIST)!=0) continue; else return RC_NOTFOUND;}
					if (cv->eid!=ei) md.flags|=MF_BIGC; else continue;
				} else {
					const HeapPageMgr::HeapV *e1,*e2;
					const HeapPageMgr::HeapVV *hcol=(const HeapPageMgr::HeapVV*)(pcb->pb->getPageBuf()+pi->hprop->offset);
					if ((e1=hcol->findElt(eid))==NULL || (e2=hcol->findElt(pv->ui))==NULL)
						{if ((pv->meta&META_PROP_IFEXIST)!=0) continue; else return RC_NOTFOUND;}
					if (e1==e2) continue; else if (e1!=hcol->start) prev=e1[-1].getID();
				}
				if ((mi=md.addMod(pv,n,pi,op))==NULL) return RC_NORESOURCES; 
				mi->eid=eid; mi->eltKey=prev; mi->flags|=flags|PM_MOVE;
			}
			continue;
		}
		switch (pv->type) {
		default: break;
		case VT_EXPR: case VT_STMT: if ((pv->meta&META_PROP_EVAL)==0) break;
		case VT_VARREF: case VT_EXPRTREE:
			if (op!=OP_SET && !fAdd && op<OP_FIRST_EXPR) return RC_INVPARAM;
			if (mi==NULL && (mi=md.addMod(pv,n,pi,op))==NULL) return RC_NORESOURCES;
			if (mi->newV!=NULL) {/*???*/freeV(*mi->newV);} else if ((mi->newV=md.alloc<Value>())==NULL) return RC_NORESOURCES; 	// ????????????????????????????
			if ((rc=eval(ses,pv,*mi->newV,&pcb,1,&params,1,&md,true))!=RC_OK && (rc!=RC_NOTFOUND || (pv->meta&META_PROP_IFEXIST)==0)) return rc;
			if (pcb->pb.isNull()) {pcb=&cb; if (cb.pb.getPage(cb.addr.pageID,ctx->heapMgr,PGCTL_XLOCK,ses)==NULL || cb.fill()==NULL) return RC_NOTFOUND;}
			if (rc==RC_NOTFOUND) {rc=RC_OK; mi->flags|=PM_INVALID; continue;}
			mi->newV->op=op; mi->newV->property=propID; mi->newV->meta=pv->meta; mi->eltKey=mi->eid=STORE_COLLECTION_ID;
			flags|=PM_CALCULATED; mi->pv=pv=mi->newV; break;
		}
		if (pv->type==VT_ARRAY || pv->type==VT_COLLECTION) {
			flags|=PM_ARRAY;
			if (op==OP_SET) {if (eid!=STORE_COLLECTION_ID) return RC_TYPE;} else if (!fAdd) return RC_INVPARAM;
			if ((mode&MODE_FORCE_EIDS)==0) {
				ElementID eid=prefix;
				if (fAdd && pi->hprop!=NULL && (eid=pi->maxKey)==prefix) {
					if (!pi->hprop->type.isCollection()) eid=prefix+1;
					else if (pi->hprop->type.getFormat()!=HDF_LONG)
						eid=((HeapPageMgr::HeapVV*)(pcb->pb->getPageBuf()+pi->hprop->offset))->getHKey()->getKey();
					else
						memcpy(&eid,&((HeapPageMgr::HeapExtCollection*)(pcb->pb->getPageBuf()+pi->hprop->offset))->keygen,sizeof(ElementID));
				}
				pi->maxKey=eid; if ((pi->flags&PM_COLLECTION)==0) pi->flags|=PM_NEWCOLL;
			} else if (pi->hprop!=NULL) {
				if (!pi->hprop->type.isCollection()) {
					// check it's not the same as will be assigned
				} else if (pi->hprop->type.getFormat()!=HDF_LONG) {
					const HeapPageMgr::HeapVV *hcol=(HeapPageMgr::HeapVV*)(pcb->pb->getPageBuf()+pi->hprop->offset);
					if (pv->type==VT_ARRAY) for (ulong i=0; i<pv->length; i++) {
						if (hcol->findElt(pv->varray[i].eid)!=NULL) return RC_ALREADYEXISTS;
					} else for (const Value *cv=pv->nav->navigate(GO_FIRST); cv!=NULL; cv=pv->nav->navigate(GO_NEXT)) {
						if (hcol->findElt(cv->eid)!=NULL) return RC_ALREADYEXISTS;
					}
				} else {
					Navigator nav(oldAddr,propID,(HeapPageMgr::HeapExtCollection*)(pcb->pb->getPageBuf()+pi->hprop->offset),0,&md);
					if (pv->type==VT_ARRAY) for (ulong i=0; i<pv->length; i++) {
						if (nav.navigate(GO_FINDBYID,pv->varray[i].eid)!=NULL) return RC_ALREADYEXISTS;
					} else for (const Value *cv=pv->nav->navigate(GO_FIRST); cv!=NULL; cv=pv->nav->navigate(GO_NEXT)) {
						if (nav.navigate(GO_FINDBYID,cv->eid)!=NULL) return RC_ALREADYEXISTS;
					}
				}
			}
		}
		if (fNew) {
			if (pi->hprop==NULL || (pi->flags&PM_RESET)!=0) {
				if (mi==NULL && (mi=md.addMod(pv,n,pi,op))==NULL) return RC_NORESOURCES;
				if ((pi->flags&PM_RESET)!=0 && pi->first->pv->op==OP_DELETE) {pi->first=mi; pi->nElts=1;}
				mi->eltKey=mi->eid=STORE_COLLECTION_ID; if ((md.flags&MF_LOCEID)!=0) flags|=PM_LOCAL;
				if ((flags&PM_ARRAY)==0) { 
					if (pi->nElts>1) {pi->flags&=~PM_SCOLL; mi->eid=eid; if (mi->epos==STORE_COLLECTION_ID) mi->epos=eid;}
					if (eids!=NULL && (mi->eltKey=eids[n])!=STORE_COLLECTION_ID) {
						if (pi->nElts>1) {
							if ((pi->flags&PM_NEWCOLL)==0) pi->flags|=PM_NEWCOLL; else pi->flags&=~PM_SCOLL;
						} else if (mi->eltKey==prefix) flags|=PM_LOCAL; else if (mi->eltKey!=lprefix) pi->flags|=PM_NEWCOLL|PM_SCOLL;
					} else {
						if (pi->nElts>1 && (pi->flags&PM_NEWCOLL)==0) {
							pi->flags|=PM_NEWCOLL; ModInfo *first=pi->first; assert(first!=mi);
							if ((first->flags&PM_ARRAY)==0 && first->eltKey==STORE_COLLECTION_ID)
								{first->eltKey=pi->maxKey++; if ((mode&MODE_NO_EID)==0) first->pv->eid=first->eltKey;}
						}
						mi->eltKey=pi->maxKey++;
					}
					if ((mode&MODE_NO_EID)==0) pv->eid=mi->eltKey;
				}
				if ((pinDescr&HOH_NOTIFICATION)!=0 && (pv->meta&META_PROP_NONOTIFICATION)==0) md.flags|=MF_NOTIFY;
				if ((pv->meta&META_PROP_SSTORAGE)!=0) flags|=PM_FORCESSV;
				if ((pv->meta&META_PROP_STOPWORDS)!=0) flags|=PM_SWORDS;
				mi->flags|=flags; pi->flags|=PM_NEWVALUES; continue;
			}
			if ((pi->flags&PM_BIGC)!=0) md.flags|=MF_BIGC;
			if ((pinDescr&HOH_NOTIFICATION)!=0 && (pi->hprop->type.flags&META_PROP_NONOTIFICATION)==0) md.flags|=MF_NOTIFY;
			if ((pi->hprop->type.flags&META_PROP_SSTORAGE)!=0) flags|=PM_FORCESSV;
			if ((pi->hprop->type.flags&META_PROP_STOPWORDS)!=0) flags|=PM_SWORDS;

			if ((meid=eid)==STORE_COLLECTION_ID) {
				assert(!fAdd);
				if ((pi->flags&PM_COLLECTION)!=0 && op!=OP_DELETE) {
					if ((pi->flags&PM_SCOLL)!=0 && (eids==NULL||eids[n]==STORE_COLLECTION_ID)) {
						const HeapPageMgr::HeapVV *hcol=(HeapPageMgr::HeapVV*)(pcb->pb->getPageBuf()+pi->hprop->offset);
						meid=eid=hcol->start->getID();
					} else if (op!=OP_SET) return RC_INVPARAM;
				}
			} else if ((pi->flags&PM_COLLECTION)==0) {
				if (eid==((pi->hprop->type.flags&META_PROP_LOCAL)!=0?prefix:rprefix)) 
					meid=fAdd?STORE_LAST_ELEMENT:STORE_COLLECTION_ID;
				else if (!fAdd || eid!=STORE_LAST_ELEMENT && eid!=STORE_FIRST_ELEMENT) 
					{if ((pv->meta&META_PROP_IFEXIST)!=0) continue; else return RC_NOTFOUND;}
			}

			if (fAdd) {
				if (propID==PROP_SPEC_DOCUMENT) return RC_ALREADYEXISTS;
				if (mi==NULL && (mi=md.addMod(pv,n,pi,op))==NULL) return RC_NORESOURCES;
				mi->eid=meid; mi->flags=flags; pi->flags|=PM_NEWVALUES;
				if ((pi->flags&PM_COLLECTION)==0) pi->flags|=PM_NEWCOLL;
				if (mi->epos==STORE_COLLECTION_ID) mi->epos=eid;
				if ((flags&PM_ARRAY)==0) {
					if (eids==NULL || (mi->eltKey=eids[n])==STORE_COLLECTION_ID) {
						if (pi->maxKey==prefix) {
							if ((pi->flags&PM_COLLECTION)==0) {
								if (pi->maxKey==((pi->hprop->type.flags&META_PROP_LOCAL)!=0?prefix:rprefix)) ++pi->maxKey;
							} else if (pi->hprop->type.getFormat()==HDF_LONG)
								memcpy(&pi->maxKey,&((HeapPageMgr::HeapExtCollection*)(pcb->pb->getPageBuf()+pi->hprop->offset))->keygen,sizeof(ElementID));
							else
								pi->maxKey=((HeapPageMgr::HeapVV*)(pcb->pb->getPageBuf()+pi->hprop->offset))->getHKey()->getKey();
						}
						mi->eltKey=pi->maxKey++;
					} else if ((pi->flags&(PM_COLLECTION|PM_RESET))==PM_COLLECTION) {
						if (pi->hprop->type.getFormat()==HDF_LONG) {
							Navigator nav(oldAddr,propID,(HeapPageMgr::HeapExtCollection*)(pcb->pb->getPageBuf()+pi->hprop->offset),0,&md);
							rc=nav.navigate(GO_FINDBYID,mi->eltKey)!=NULL?RC_ALREADYEXISTS:RC_OK;
						} else {
							const HeapPageMgr::HeapVV *hcol=(HeapPageMgr::HeapVV*)(pcb->pb->getPageBuf()+pi->hprop->offset);
							rc=hcol->findElt(mi->eltKey)!=NULL?RC_ALREADYEXISTS:RC_OK;
						}
						if (rc==RC_ALREADYEXISTS)
							{ModInfo *mi2=md.findMod(pi,mi->eltKey,pcb->pb->getPageBuf(),pi->hprop->offset); if (mi2==NULL || mi2->pv->op!=OP_DELETE) return rc;}
					} else if (mi->eltKey==((pi->hprop->type.flags&META_PROP_LOCAL)!=0?prefix:rprefix)) return RC_ALREADYEXISTS;
					if ((mode&MODE_NO_EID)==0) pv->eid=mi->eltKey;
				}
				continue;
			}
			if ((opv=md.alloc<Value>())==NULL) return RC_NORESOURCES;
			if ((rc=loadVH(*opv,pi->hprop,*pcb,0,&md,meid))!=RC_OK)
				{if (rc==RC_NOTFOUND && (pv->meta&META_PROP_IFEXIST)!=0) continue; else return RC_NOTFOUND;}
			if (propID==PROP_SPEC_DOCUMENT) {if (opv->type==VT_REFID) oldDoc=opv->id; else return RC_CORRUPTED;}
			if ((mi=md.addMod(pv,n,pi,op))==NULL) {freeV(*opv); return RC_NORESOURCES;}
			mi->oldV=opv; mi->eid=meid; mi->eltKey=opv->eid; mi->flags=flags; ty=(ValueType)opv->type;
			pi->flags|=PM_OLDVALUES; if (op!=OP_DELETE) pi->flags|=PM_NEWVALUES;
			if (eid==STORE_COLLECTION_ID) {
				if (op==OP_SET || op==OP_DELETE) {
					pi->flags=pi->flags&~(PM_COLLECTION|PM_NEWCOLL|PM_BIGC|PM_FTINDEXABLE)|PM_RESET|PM_OLDVALUES|(flags&PM_FTINDEXABLE);
					for (ModInfo *m=pi->first; m!=NULL && m!=mi; m=m->pnext) m->flags|=PM_INVALID|PM_PROCESSED;
					pi->first=pi->last=mi; mi->pnext=NULL;
					if (op==OP_SET && (flags&PM_ARRAY)==0 && eids!=NULL) {
						if ((mi->eltKey=eids[n])==STORE_COLLECTION_ID) mi->eltKey=opv->eid;
						else if (mi->eltKey!=lprefix) pi->flags|=PM_NEWCOLL|PM_SCOLL;
					}
					pi->first=mi;
				}
				if ((mode&MODE_NO_EID)==0) pv->eid=mi->eltKey;
			}
			if (ty==VT_ARRAY || ty==VT_COLLECTION) {
				if (op!=OP_SET && op!=OP_DELETE) return RC_TYPE;
				if (ty==VT_ARRAY && (opv->flags&VF_SSV)!=0)
					for (ulong i=0; i<opv->length; i++) if ((opv->varray[i].flags&VF_SSV)!=0) {
						HRefSSV *href=(HRefSSV*)&opv->varray[i].id; assert(opv->varray[i].type==VT_STREAM);
						if (md.addHRef(href)==NULL) return RC_NORESOURCES;
						if (pageSSV.isNull() || pageSSV->getPageID()!=href->pageID) {
							if (pageSSV.getPage(href->pageID,ctx->ssvMgr,PGCTL_ULOCK,ses)==NULL) return RC_CORRUPTED;
						}
						const HeapPageMgr::HeapPage *hp=(const HeapPageMgr::HeapPage *)pageSSV->getPageBuf(); 
						if ((rc=loadSSV(const_cast<Value&>(opv->varray[i]),href->type.getType(),
							hp->getObject(hp->getOffset(href->idx)),0,&md))!=RC_OK) return rc;
						opv->varray[i].flags&=~VF_SSV; md.flags|=MF_DELSSV;
					}
				continue;
			}
			if (ty==VT_STREAM) {
				mi->flags|=PM_SSV; md.flags|=MF_SSVS; 
				if ((opv->flags&VF_SSV)==0) ty=opv->stream.is->dataType();
				else if ((mi->href=md.addHRef((HRefSSV*)&opv->id))==NULL) return RC_NORESOURCES;
				else {
					ty=mi->href->type.getType();
					if (pageSSV.isNull() || pageSSV->getPageID()!=mi->href->pageID) {
						if (pageSSV.getPage(mi->href->pageID,ctx->ssvMgr,PGCTL_ULOCK,ses)==NULL) return RC_CORRUPTED;
					}
					const HeapPageMgr::HeapPage *hp=(const HeapPageMgr::HeapPage *)pageSSV->getPageBuf(); 
					if ((rc=loadSSV(*opv,ty,hp->getObject(hp->getOffset(mi->href->idx)),0,&md))!=RC_OK) return rc;
					opv->flags&=~VF_SSV;
				}
				if (op==OP_SET || op==OP_DELETE) continue; else if (op!=OP_EDIT) return RC_TYPE;
			}
			assert((opv->flags&VF_SSV)==0);
			if (op==OP_EDIT) {
				if (ty!=VT_STRING && ty!=VT_BSTR && ty!=VT_URL || ty!=pv->type) return RC_TYPE;
				uint64_t ls=opv->type!=VT_STREAM?opv->length:opv->stream.is->length();
				if (pv->edit.length==ls) {
					if (pv->edit.shift!=0) return RC_INVPARAM;
					if ((mi->newV=md.alloc<Value>())==NULL) return RC_NORESOURCES;
					mi->newV->bstr=pv->bstr; mi->newV->length=pv->length; mi->newV->type=pv->type;
					mi->newV->property=pv->property; mi->newV->eid=pv->eid; mi->newV->meta=pv->meta;
					mi->newV->op=OP_SET; mi->newV->flags=NO_HEAP; mi->pv=mi->newV;
					continue;
				}
				if (pv->edit.shift==~0ULL) {if (pv->edit.length!=0) return RC_INVPARAM;}
				else if (opv->type!=VT_STREAM) {if (pv->edit.shift+pv->edit.length>opv->length) return RC_INVPARAM;}
				else if (pv->edit.shift+pv->edit.length>opv->stream.is->length()) return RC_INVPARAM;
				else continue;
			} else if (op<OP_FIRST_EXPR) continue;
			mi->pv=opv;
		} else if (op==OP_SET||op==OP_DELETE) {
			if (mi->pv==mi->newV && mi->newV!=NULL && mi->newV!=pv) {freeV(*mi->newV); mi->newV=NULL;}
			if (eid==STORE_COLLECTION_ID) {
				pi->flags=pi->flags&~(PM_COLLECTION|PM_NEWCOLL|PM_BIGC|PM_FTINDEXABLE)|PM_RESET|(flags&PM_FTINDEXABLE);
				for (ModInfo *m=pi->first; m!=NULL && m!=mi; m=m->pnext) m->flags|=PM_INVALID|PM_PROCESSED;
				pi->first=mi; if (op==OP_DELETE && pi->hprop==NULL) mi->flags|=PM_INVALID|PM_PROCESSED;
			}
			mi->pv=pv; if (op!=OP_DELETE) pi->flags|=PM_NEWVALUES; continue;
		} else if (op==OP_EDIT) {
			if (mi->pv->op==OP_DELETE) {if ((pv->meta&META_PROP_IFEXIST)!=0) continue; else return RC_NOTFOUND;}
			if ((ty=(ValueType)mi->pv->type)==VT_STREAM) ty=mi->pv->stream.is->dataType();
			if (ty!=VT_STRING && ty!=VT_BSTR && ty!=VT_URL || ty!=pv->type) return RC_TYPE;
			if (mi->pv->op==OP_EDIT) {
				if (mi->oldV->type==VT_STREAM) {
					byte *p=(byte*)md.malloc(mi->pv->length); if (p==NULL) return RC_NORESOURCES;
					memcpy(p,mi->pv->edit.bstr,mi->pv->length);
					StreamEdit *estr=new(&md) StreamEdit(mi->oldV->stream.is->clone(),
										mi->pv->edit.shift,mi->pv->edit.length,mi->pv->length,p,&md);
					if (estr==NULL) return RC_NORESOURCES;
					if (mi->newV==NULL && (mi->newV=md.alloc<Value>())==NULL) {estr->destroy(); return RC_NORESOURCES;}
					mi->newV->set(estr); mi->newV->property=propID; 
					mi->newV->eid=mi->pv->eid; mi->newV->meta=mi->pv->meta;
				}
				mi->pv=mi->newV; assert(mi->newV!=NULL && mi->newV->op==OP_SET);
			}
			assert(mi->pv->op==OP_ADD||mi->pv->op==OP_ADD_BEFORE||mi->pv->op==OP_SET);
			if (mi->pv->type==VT_STREAM) {
				byte *p=(byte*)md.malloc(pv->length); if (p==NULL) return RC_NORESOURCES; memcpy(p,pv->bstr,pv->length);
				StreamEdit *ps=new(&md) StreamEdit(mi->pv->stream.is,pv->edit.shift,pv->edit.length,pv->length,p,&md);
				if (ps==NULL) return RC_NORESOURCES; mi->newV->stream.is=ps; mi->newV->flags=NO_HEAP; continue;
			}
		}
		assert(op>=OP_FIRST_EXPR||op==OP_EDIT);
		if (mi->pv->op==OP_DELETE) {if ((pv->meta&META_PROP_IFEXIST)!=0) continue; else return RC_NOTFOUND;}
		if (mi->newV==NULL) {
			if ((mi->newV=md.alloc<Value>())==NULL) return RC_NORESOURCES;
			*mi->newV=*mi->pv; if (mi->pv->type==VT_STREAM) mi->pv->stream.prefix=NULL;	// collection ???
			mi->newV->flags=mi->newV->flags&~HEAP_TYPE_MASK|NO_HEAP; mi->pv=op==OP_EDIT?pv:mi->newV;
		} else if (mi->pv!=mi->newV) {
			// OP_EDIT???
			return RC_INTERNAL;
		}
		rc=Expr::calc(op,*mi->newV,pv,2,0,ses); if (rc!=RC_OK) return rc;
		if (mi->oldV!=NULL) {mi->newV->eid=mi->oldV->eid; mi->newV->property=mi->oldV->property; mi->newV->meta=mi->oldV->meta;}
		mi->newV->op=OP_SET; pi->flags|=PM_NEWVALUES;
	}

	if (md.list==NULL) return RC_OK;

	if ((md.flags&(MF_REPSES|MF_UPDBY))==0) {
		if ((rc=md.addPropMod(PROP_SPEC_UPDATEDBY,pcb->hpin,mi,prefix))!=RC_OK) return rc;
		if (mi!=NULL) {
			if (loadVH(*mi->oldV,mi->pInfo->hprop,*pcb,0,&md)!=RC_OK||mi->oldV->type!=VT_IDENTITY
				||mi->oldV->iid==ses->getIdentity()) mi->flags=PM_PROCESSED|PM_INVALID;
			else {mi->newV->setIdentity(ses->getIdentity()); mi->newV->setPropID(PROP_SPEC_UPDATEDBY);}
		}
	}
	if ((md.flags&(MF_REPSES|MF_LASTUPD))==0) {
		if ((rc=md.addPropMod(PROP_SPEC_UPDATED,pcb->hpin,mi,prefix))!=RC_OK) return rc;
		if (mi!=NULL) {
			if (loadVH(*mi->oldV,mi->pInfo->hprop,*pcb,0,&md)!=RC_OK) mi->flags=PM_PROCESSED|PM_INVALID;
			else {mi->newV->setNow(); mi->newV->setPropID(PROP_SPEC_UPDATED);}
		}
	}

	ulong nMod=0,stamp=pcb->hpin->getStamp(),nEdits=0;
	long extraLen=0,xExtraLen=0; size_t lrec=0,expLen=0,newExtraLen=0,oldExtraLen=0;
	byte *buf=NULL,fbuf[sizeof(HeapPageMgr::HeapModEdit)+PageAddrSize*2]; size_t lbuf=0;
	IStoreNotification::NotificationEvent evt={id,NULL,0,NULL,0,(md.flags&MF_REPSES)!=0};
	bool fForceSplit = FORCE_SPLIT;

	if ((pinDescr&(HOH_HIDDEN|HOH_NOINDEX))==0) ctx->classMgr->classify(pcb,clro);

	xExtraLen=(long)((HeapPageMgr::HeapPage*)pcb->pb->getPageBuf())->totalFree();

	for (mi=md.list; mi!=NULL; mi=mi->next) if ((mi->flags&(PM_INVALID|PM_PROCESSED))==0) {
		if (pNFailed!=NULL && mi->pvIdx!=~0u) *pNFailed=mi->pvIdx; pv=mi->pv;
		ExprOp op=(ExprOp)pv->op; assert(mi->oldV==NULL||(mi->oldV->flags&VF_SSV)==0||mi->oldV->type==VT_ARRAY);
		if ((mi->flags&PM_MOVE)!=0 || (mi->pInfo->flags&PM_BIGC)!=0 && mi->eid!=STORE_COLLECTION_ID) {nMod++; continue;}
		size_t newLen,oldLen=0; ulong i; long delta=0; const Value *cv; bool fElt=false;
		if (mi->oldV!=NULL) {
			if (op!=OP_EDIT) {
				const byte *pData=pcb->pb->getPageBuf()+mi->pInfo->hprop->offset; HType ht=mi->pInfo->hprop->type;
				fElt=(mi->pInfo->flags&(PM_COLLECTION|PM_BIGC))==PM_COLLECTION; ElementID eid=mi->eid;
				if (fElt && eid==STORE_COLLECTION_ID) {if ((mi->pInfo->flags&PM_SCOLL)!=0) eid=STORE_FIRST_ELEMENT; else fElt=false;}
				if (fElt) {
					const HeapPageMgr::HeapV *elt=((const HeapPageMgr::HeapVV*)pData)->findElt(eid);
					assert(elt!=NULL); pData=pcb->pb->getPageBuf()+elt->offset; ht=elt->type;
				}
				oldLen=HeapPageMgr::dataLength(ht,pData,pcb->pb->getPageBuf(),(mi->pInfo->flags&PM_OLDFTINDEX)!=0?&mi->flags:(ulong*)0);
				if (ht==HType::compactRef) mi->flags|=PM_COMPACTREF;
			}
			if ((mi->pInfo->flags&PM_OLDFTINDEX)!=0) switch (mi->oldV->type) {
			case VT_STREAM: if (mi->oldV->stream.is->dataType()!=VT_STRING) break;
			case VT_STRING: mi->flags|=IX_OFT; break;
			case VT_ARRAY:
				for (i=0; i<mi->oldV->length; i++) switch (mi->oldV->varray[i].type) {
				case VT_STREAM: if (mi->oldV->varray[i].stream.is->dataType()!=VT_STRING) break;
				case VT_STRING: mi->flags|=IX_OFT; break;
				}
				break;
			case VT_COLLECTION:
				for (cv=mi->oldV->nav->navigate(GO_FIRST); cv!=NULL; cv=mi->oldV->nav->navigate(GO_NEXT))
					switch (cv->type) {
					case VT_STREAM: if (cv->stream.is->dataType()!=VT_STRING) break;
					case VT_STRING: mi->flags|=IX_OFT; break;
					}
				break;
			case VT_STRUCT:
				//???
				break;
			}
		}
		switch (op) {
		case OP_EDIT:
			if (mi->oldV->type==VT_STREAM) newLen=oldLen=sizeof(HLOB);
			else {
				if (mi->href!=NULL && mi->oldV->length+mi->pv->length-mi->pv->edit.length>xSize) delta+=sizeof(HLOB)-sizeof(HRefSSV);
				newLen=pv->length; oldLen=pv->edit.length;
			}
			break;
		case OP_DELETE:
			assert((mi->flags&PM_ARRAY)==0);	// delete entire collection???
			delta-=!fElt?sizeof(HeapPageMgr::HeapV):(mi->pInfo->flags&PM_SCOLL)==0?sizeof(HeapPageMgr::HeapV):
					sizeof(HeapPageMgr::HeapV)+sizeof(HeapPageMgr::HeapVV)+sizeof(HeapPageMgr::HeapV);
			newLen=0; mi->pInfo->nDelta--; break;
		default:
			if ((mi->pInfo->flags&PM_BIGC)!=0 && mi->pInfo->pcol!=NULL) newLen=HeapPageMgr::collDescrSize(mi->pInfo->pcol->getDescriptor());
			else if ((rc=estimateLength(*pv,newLen,0,xSize,&md,oldAddr.pageID,&newExtraLen))!=RC_OK) goto finish;
			if ((pv->flags&VF_REF)!=0) {rc=RC_INVPARAM; goto finish;}
			if ((pv->flags&VF_SSV)!=0) newDescr|=HOH_SSVS;
			if ((pv->flags&VF_PREFIX)!=0) md.flags|=MF_PREFIX;
			else if (pv->type==VT_STREAM) {
				uint8_t ty=pv->stream.is!=NULL?pv->stream.is->dataType():VT_BSTR;
				if (pv==mi->newV) freeV(*mi->newV);
				else if ((mi->newV=md.alloc<Value>())==NULL) {rc=RC_NORESOURCES; goto finish;}
				else *mi->newV=*pv; 
				mi->newV->flags=NO_HEAP; pv=mi->pv=mi->newV; mi->newV->type=ty;
				mi->newV->bstr=(byte*)&mi->newV->length; mi->newV->length=0;
			}
			if ((mi->pInfo->flags&PM_FTINDEXABLE)!=0 && (pv->flags&VF_STRING)!=0) {mi->flags|=IX_NFT; newDescr|=HOH_FT;}
			if ((mi->flags&PM_FORCESSV)!=0 && (pv->flags&VF_SSV)==0) switch (pv->type) {
			default: break;
			case VT_STRING: case VT_BSTR:
				if (pv->length!=0) {
					newLen=ceil(pv->length,HP_ALIGN)+sizeof(HeapPageMgr::HeapObjHeader)>xSize?sizeof(HLOB):sizeof(HRefSSV);
					pv->flags|=VF_SSV;
				}
				break;
			case VT_STREAM:
				if ((pv->flags&VF_PREFIX)!=0 && pv->stream.prefix!=NULL) {
					newLen=ceil(*(size_t*)pv->stream.prefix,HP_ALIGN)+sizeof(HeapPageMgr::HeapObjHeader)>=xSize?sizeof(HLOB):sizeof(HRefSSV);
					pv->flags|=VF_SSV;
				}
				break;
			case VT_EXPR: case VT_STMT:
				newLen=sizeof(HLOB); pv->flags|=VF_SSV; break;		// ??????????????????
			case VT_ARRAY: case VT_COLLECTION:
				newLen=sizeof(HeapPageMgr::HeapExtCollection); pv->flags|=VF_SSV; break;
			case VT_STRUCT:
				//???
				break;
			}
			if (mi->oldV==NULL) {
				mi->pInfo->nDelta+=(mi->flags&PM_ARRAY)==0?1:pv->type==VT_ARRAY?pv->length:pv->nav->count();
				delta+=(mi->pInfo->flags&(PM_BIGC|PM_NEWCOLL))==PM_NEWCOLL ? 
							mi->pInfo->first!=mi ? long(sizeof(HeapPageMgr::HeapV)):long(sizeof(HeapPageMgr::HeapVV)+sizeof(HeapPageMgr::HeapV)+sizeof(HeapPageMgr::HeapKey)):
						mi->eid==STORE_COLLECTION_ID?long(sizeof(HeapPageMgr::HeapV)):
							(mi->flags&PM_ARRAY)!=0?(mi->pInfo->flags&PM_COLLECTION)!=0?
							-long(sizeof(HeapPageMgr::HeapVV)-sizeof(HeapPageMgr::HeapV)+sizeof(HeapPageMgr::HeapKey)):long(sizeof(HeapPageMgr::HeapV)):
							(mi->pInfo->flags&PM_COLLECTION)!=0?long(sizeof(HeapPageMgr::HeapV)):
								long(sizeof(HeapPageMgr::HeapVV)+sizeof(HeapPageMgr::HeapV)+sizeof(HeapPageMgr::HeapKey));
				if ((mi->pInfo->flags&PM_BIGC)==0 && (pv->type==VT_ARRAY||pv->type==VT_COLLECTION) && 
					(pv->flags&VF_SSV)!=0 && mi->pInfo->hprop!=NULL) {mi->pInfo->flags|=PM_BIGC|PM_NEWCOLL; md.flags|=MF_BIGC;}
			} else if ((mi->flags&PM_ARRAY)!=0 && op==OP_SET && mi->eid!=STORE_COLLECTION_ID) 
				delta-=sizeof(HeapPageMgr::HeapVV)-sizeof(HeapPageMgr::HeapV)+sizeof(HeapPageMgr::HeapKey);
			else if ((mi->pInfo->flags&(PM_NEWCOLL|PM_SCOLL))==(PM_NEWCOLL|PM_SCOLL)) {
				delta+=long(sizeof(HeapPageMgr::HeapVV)+sizeof(HeapPageMgr::HeapKey));
			}
			break;
		}
		if (newLen<=oldLen) {if ((delta-=long(ceil(oldLen,HP_ALIGN)-ceil(newLen,HP_ALIGN)))>0) mi->flags|=PM_EXPAND;}
		else if ((delta+=long(ceil(newLen,HP_ALIGN)-ceil(oldLen,HP_ALIGN)))>0) mi->flags|=PM_EXPAND;
		mi->pInfo->delta+=delta; extraLen+=delta; lrec+=ceil(oldLen,HP_ALIGN)+ceil(newLen,HP_ALIGN); nMod++;
	}

	if (extraLen>xExtraLen || fForceSplit) {
		expLen=(pinDescr&HOH_COMPACTREF)!=0?pcb->hpin->expLength(pcb->pb->getPageBuf()):pcb->hpin->hdr.getLength();
		if ((md.flags&MF_MOVED)==0) expLen+=PageAddrSize;
		if (expLen+extraLen+newExtraLen<=xSize && (md.flags&(MF_MOVED|MF_REMOTE))!=0) md.flags|=MF_MIGRATE;
		else for (bool fForceOut=false;;) {
			const HeapPageMgr::HeapV *hprop=pcb->hpin->getPropTab(),*hend=hprop+pcb->hpin->nProps;
			PropInfo **ppi=md.ppi,**piend=ppi+md.npi; CandidateSSVs cs;
			while (hprop<hend || ppi<piend) {
				if (ppi<piend) {
					pi=*ppi;
					int cmp=hprop<hend?cmp3(pi->propID,hprop->getID()):-1;
					if (cmp<0) {
						for (mi=pi->first; mi!=NULL; mi=mi->pnext) if ((mi->flags&(PM_MOVE|PM_PROCESSED))==0 && (mi->pv->flags&VF_SSV)==0) {
							assert(mi->oldV==NULL && (pi->flags&PM_BIGC)==0);
							if ((pi->flags&PM_NEWCOLL)==0 || pi->nElts<ARRAY_THRESHOLD)
								(void)findCandidateSSVs(cs,mi->pv,1,true,&md,NULL,pi->propID,mi);
							else {
								size_t len=sizeof(HeapPageMgr::HeapVV),l; ModInfo *mi0=mi;
								do {
									if ((rc=estimateLength(*mi->pv,l,MODE_PREFIX_READ,xSize,&md,oldAddr.pageID))!=RC_OK) goto finish;
									if (mi->pv->type==VT_ARRAY||mi->pv->type==VT_COLLECTION) len-=sizeof(HeapPageMgr::HeapVV);
									else len+=sizeof(HeapPageMgr::HeapV);
									len+=ceil(l,HP_ALIGN);
								} while ((mi=mi->next)!=NULL);
								if (len>sizeof(HeapPageMgr::HeapExtCollection)) {
									if ((rc=cs.insert(mi0->pv,pi->propID,len,mi0,sizeof(HeapPageMgr::HeapExtCollection)))!=RC_OK) goto finish;
									pi->flags|=PM_BCCAND;
								}
								break;
							}
						}
						++ppi; continue;
					}
					if (cmp==0) {
						if ((pi->flags&PM_BIGC)==0) for (mi=pi->first; mi!=NULL; mi=mi->pnext) if (mi->pv->op!=OP_DELETE && (mi->flags&PM_MOVE)==0) {
							ulong nElts=!hprop->type.isCollection()||hprop->type.getFormat()==HDF_LONG?1:((HeapPageMgr::HeapVV *)(pcb->pb->getPageBuf()+hprop->offset))->cnt;
							if (mi->pv->op!=OP_SET && (nElts+pi->nDelta>=ARRAY_THRESHOLD || fForceOut)) {
								ulong len=HeapPageMgr::dataLength(hprop->type,pcb->pb->getPageBuf()+hprop->offset,pcb->pb->getPageBuf())+pi->delta;
								if (len>sizeof(HeapPageMgr::HeapExtCollection)) {
									if ((rc=cs.insert(mi->pv,pi->propID,len,mi,sizeof(HeapPageMgr::HeapExtCollection)))!=RC_OK) goto finish;
									pi->flags|=PM_BCCAND; break;
								}
							} else if ((mi->pv->flags&VF_SSV)==0 && (mi->pv->op!=OP_EDIT || mi->newV!=NULL))
								(void)findCandidateSSVs(cs,mi->pv->op==OP_EDIT?mi->newV:mi->pv,1,true,&md,NULL,pi->propID,mi);
						}
						++hprop; ++ppi; continue;
					}
				}
				ulong flags=0,l;
				if (hprop->type.isCollection()) {
					if (hprop->type.getFormat()==HDF_LONG ||
						((HeapPageMgr::HeapVV*)(pcb->pb->getPageBuf()+hprop->offset))->cnt<ARRAY_THRESHOLD && expLen+extraLen<xSize ||
						(l=HeapPageMgr::dataLength(hprop->type,pcb->pb->getPageBuf()+hprop->offset,pcb->pb->getPageBuf()))<=sizeof(HeapPageMgr::HeapExtCollection)) 
							{++hprop; continue;}
					flags=PM_COLLECTION|PM_BCCAND;
				} else if (hprop->type.getFormat()==HDF_COMPACT || !hprop->type.canBeSSV() || 
						hprop->type.getType()==VT_STMT || hprop->type.getType()==VT_EXPR ||	//tmp VT_EXPR, VT_STMT are excluded
						(l=HeapPageMgr::dataLength(hprop->type,pcb->pb->getPageBuf()+hprop->offset))<=sizeof(HRefSSV) ||
						l<STRING_THRESHOLD && expLen+extraLen<xSize) {++hprop; continue;}
				PropInfo *pi=md.alloc<PropInfo>(); ModInfo *cv=md.alloc<ModInfo>(); 
				if (pi==NULL||cv==NULL) {rc=RC_NORESOURCES; goto finish;}
				pi->first=pi->last=cv; pi->flags=flags; pi->hprop=hprop; pi->maxKey=prefix;
				pi->nElts=1; pi->pcol=NULL; cv->pInfo=pi;
				if ((rc=cs.insert(NULL,STORE_INVALID_PROPID,l,cv,(flags&PM_COLLECTION)!=0?
					sizeof(HeapPageMgr::HeapExtCollection):sizeof(HRefSSV)))!=RC_OK) goto finish;
				++hprop;
			}
			if (cs.nCandidates>0) {
				if (cs.nCandidates>1) qsort(cs.candidates,cs.nCandidates,sizeof(CandidateSSV),cmpCandidateSSV);
				bool fCheckMigrate=true;
				for (ulong j=0; j<cs.nCandidates && (extraLen>xExtraLen||fForceSplit); j++) {
					CandidateSSV &cd=cs.candidates[j]; assert(cd.length>cd.dlen);
					if (fCheckMigrate && expLen+extraLen+newExtraLen<=xSize) {
						if ((md.flags&(MF_MOVED|MF_REMOTE))!=0) break;
						long l=extraLen; ulong k=j; fCheckMigrate=false;
						for (; k<cs.nCandidates && l>xExtraLen; k++) l-=long(cd.length-cd.dlen);
						if (l>xExtraLen || k>3 && !fForceSplit) break;		// ???
					}
					if (cd.mi==NULL) cd.pv->flags|=VF_SSV;
					else {
						if ((cd.mi->pInfo->flags&PM_BCCAND)!=0) {
							if (cd.mi->pv==NULL) {
								if ((cd.mi->pv=cd.mi->newV=md.alloc<Value>())==NULL) {rc=RC_NORESOURCES; goto finish;}
								cd.mi->eid=cd.mi->eltKey=STORE_COLLECTION_ID; cd.mi->flags=PM_SPILL; cd.mi->pvIdx=~0u;
								cd.mi->newV->setError(cd.mi->pInfo->hprop->getID()); cd.mi->next=md.list; md.list=cd.mi; nMod++;
							}
							cd.mi->pInfo->flags|=PM_NEWCOLL|PM_BIGC; md.flags|=MF_BIGC;
						} else if (cd.mi->pv==NULL) {
							cd.mi->oldV=md.alloc<Value>(); cd.mi->pv=cd.mi->newV=md.alloc<Value>();
							if (cd.mi->oldV==NULL||cd.mi->newV==NULL) {rc=RC_NORESOURCES; goto finish;}
							if ((rc=loadVH(*cd.mi->oldV,cd.mi->pInfo->hprop,*pcb,0,&md))!=RC_OK) goto finish;	// LOAD_SSV???
							cd.mi->eid=cd.mi->eltKey=STORE_COLLECTION_ID; cd.mi->flags=PM_SPILL; cd.mi->pvIdx=~0u;
							*cd.mi->newV=*cd.mi->oldV; cd.mi->newV->flags=NO_HEAP|VF_SSV; cd.mi->next=md.list; md.list=cd.mi;
							nMod++; lrec+=ceil(cd.length,HP_ALIGN)+cd.dlen;
						} else {
							if (cd.mi->pv->op==OP_EDIT) {
								assert(cd.mi->newV!=NULL); 
								lrec+=ceil(cd.mi->newV->length-cd.mi->pv->length,HP_ALIGN); cd.mi->pv=cd.mi->newV;
							}
							cd.mi->pv->flags|=VF_SSV; if (cd.mi->oldV!=NULL) cd.mi->flags&=~PM_EXPAND;		// only OP_ADD?
						}
					}
					extraLen-=long(cd.length-cd.dlen); newDescr|=HOH_SSVS;
				}
			}
			if (extraLen>xExtraLen) {
				if (expLen+extraLen+newExtraLen>xSize) {
					if (!fForceOut) {fForceOut=true; continue;}
					rc=RC_TOOBIG; goto finish;
				}
				md.flags|=MF_MIGRATE;
			}
			break;
		}
	}

	if ((md.flags&(MF_BIGC|MF_SSVS))!=0 && !pcb->pb->isXLocked()) pcb->pb->upgradeLock();

	if ((md.flags&MF_BIGC)!=0) for (mi=md.list; mi!=NULL; mi=mi->next) if ((mi->pInfo->flags&PM_BIGC)!=0) {
		PropInfo *pInfo=mi->pInfo; assert((mi->flags&PM_PROCESSED)==0);
		if (pNFailed!=NULL && mi->pvIdx!=~0u) *pNFailed=mi->pvIdx;
		if (pInfo->pcol==NULL) {
			ExtCollectionInfo *ei=md.alloc<ExtCollectionInfo>(); if (ei==NULL) {rc=RC_NORESOURCES; goto finish;}
			if ((pInfo->flags&(PM_NEWCOLL|PM_COLLECTION|PM_BIGC))==(PM_COLLECTION|PM_BIGC))
				ei->pc=Collection::create(oldAddr,mi->pv->property,ctx,&md,pcb->pb);		// RLATCH ?? cache?
			else {
				HeapPageMgr::HeapExtCollection coll;
				if (pInfo->hprop==NULL) {
					assert((pInfo->flags&PM_NEWCOLL)!=0 && mi==pInfo->first);
					if ((rc=Collection::persist(*mi->pv,coll,ses,(pInfo->flags&PM_GENEIDS)==0))!=RC_OK) goto finish;
				} else {
					mi->flags|=PM_PUTOLD;
					if ((rc=loadVH(w,pInfo->hprop,*pcb,LOAD_SSV,&md))!=RC_OK || (rc=Collection::persist(w,coll,ses,true,true))!=RC_OK) goto finish;
					if (pInfo->hprop->type.getFormat()!=HDF_COMPACT)
						lrec+=ceil(HeapPageMgr::dataLength(pInfo->hprop->type,pcb->pb->getPageBuf()+pInfo->hprop->offset,pcb->pb->getPageBuf()),HP_ALIGN);
				}
				ei->pc=new(&md) Collection(ctx,pcb->hpin->getStamp(),&coll,&md);
			}
			if ((pInfo->pcol=ei->pc)==NULL) {rc=RC_NORESOURCES; goto finish;}
			ei->next=md.ext; md.ext=ei; 
			if (pInfo->hprop==NULL||(mi->flags&PM_SPILL)!=0) {lrec+=ceil(sizeof(HeapPageMgr::HeapExtCollection),HP_ALIGN); continue;}
		}
		if (((pv=mi->pv)->op==OP_SET||pv->op==OP_DELETE) && pv->eid==STORE_COLLECTION_ID) {
			assert(mi->oldV!=NULL && mi->oldV->type==VT_COLLECTION); PageAddr caddr;
			const HeapPageMgr::HeapExtCollection *hc=((Navigator*)mi->oldV->nav)->getDescriptor();
			memcpy(&caddr.pageID,&hc->anchor,sizeof(PageID)); caddr.idx=0;
			if ((rc=ses->queueForPurge(caddr,TXP_SSV,hc))!=RC_OK) goto finish;
			pInfo->pcol=NULL; mi->flags|=PM_BIGC;
		} else {
			if ((mi->flags&PM_ARRAY)==0) {
				if ((rc=pInfo->pcol->modify((ExprOp)pv->op,pv,mi->eid,mi->eltKey,ses))!=RC_OK) goto finish;
				if ((mi->pInfo->flags&PM_FTINDEXABLE)!=0) {
					if (pv->isFTIndexable()) {mi->flags|=IX_NFT; newDescr|=HOH_FT;}
					if (mi->oldV!=NULL && mi->oldV->isFTIndexable()) mi->flags|=IX_OFT;
				}
			} else if (pv->type==VT_ARRAY) {
				ExprOp op=(ExprOp)pv->op; assert(op==OP_ADD||op==OP_ADD_BEFORE);
				ElementID eid=mi->eid!=STORE_COLLECTION_ID?mi->eid:op==OP_ADD?STORE_LAST_ELEMENT:STORE_FIRST_ELEMENT;
				for (ulong k=0; k<pv->length; k++) {
					ElementID newEid=(mode&MODE_FORCE_EIDS)!=0?pv->varray[k].eid:mi->pInfo->maxKey++;
					if ((rc=pInfo->pcol->modify(op,&pv->varray[k],eid,newEid,ses))!=RC_OK) goto finish;
					if ((mi->pInfo->flags&PM_FTINDEXABLE)!=0 && pv->varray[k].isFTIndexable()) {mi->flags|=IX_NFT; newDescr|=HOH_FT;}
					if (op==OP_ADD) eid=newEid;
				}
			} else {
				ExprOp op=(ExprOp)pv->op; assert(pv->type==VT_COLLECTION && (op==OP_ADD||op==OP_ADD_BEFORE));
				ElementID eid=mi->eid!=STORE_COLLECTION_ID?mi->eid:op==OP_ADD?STORE_LAST_ELEMENT:STORE_FIRST_ELEMENT;
				for (const Value *cv=pv->nav->navigate(op==OP_ADD?GO_LAST:GO_FIRST);
					cv!=NULL; cv=pv->nav->navigate(op==OP_ADD?GO_PREVIOUS:GO_NEXT)) {
					ElementID newEid=(mode&MODE_FORCE_EIDS)!=0?cv->eid:mi->pInfo->maxKey++;
					if ((rc=pInfo->pcol->modify(op,cv,eid,newEid,ses))!=RC_OK) goto finish;
					if ((mi->pInfo->flags&PM_FTINDEXABLE)!=0 && cv->isFTIndexable()) {mi->flags|=IX_NFT; newDescr|=HOH_FT;}
					if (op==OP_ADD) eid=newEid;
				}
			}
			if ((pInfo->flags&PM_NEWCOLL)==0 || mi!=pInfo->first) {mi->flags|=PM_PROCESSED; nMod--;}	// lrec???
			else lrec+=ceil(HeapPageMgr::collDescrSize(pInfo->pcol->getDescriptor()),HP_ALIGN);
			if (pInfo->pcol->isModified() && (pInfo->first!=NULL && pInfo->first->flags&PM_PROCESSED)!=0) {
				pInfo->first->flags&=~PM_PROCESSED; nMod++; 
				lrec+=ceil(HeapPageMgr::collDescrSize(pInfo->pcol->getDescriptor()),HP_ALIGN);
				if ((pInfo->flags&PM_NEWCOLL)==0) {
					assert(pInfo->hprop!=NULL && pInfo->hprop->type.isCollection() && pInfo->hprop->type.getFormat()==HDF_LONG);
					const HeapPageMgr::HeapExtCollection *c=(const HeapPageMgr::HeapExtCollection *)(pcb->pb->getPageBuf()+pInfo->hprop->offset);
					lrec+=ceil(HeapPageMgr::collDescrSize(c),HP_ALIGN); pInfo->first->flags|=PM_BIGC;
				}
			}
		}
	}

	if ((md.flags&MF_SSVS)!=0) for (mi=md.list; mi!=NULL; mi=mi->next) if (mi->oldV!=NULL && (mi->flags&(PM_SSV|PM_PROCESSED))==PM_SSV) {
		pv=mi->pv; assert(pv->op==OP_SET||pv->op==OP_DELETE||pv->op==OP_EDIT);
		PageAddr saddr; uint64_t lstr; ValueType ty;
		switch (pv->op) {
		default: assert(0);
		case OP_DELETE:
			if (mi->oldV->type!=VT_STREAM) {if (mi->href!=NULL && mi->href->pageID!=INVALID_PAGEID) md.flags|=MF_DELSSV;}
			else if ((rc=ses->queueForPurge(((StreamX*)mi->oldV->stream.is)->getAddr()))!=RC_OK) goto finish;
			continue;
		case OP_SET:
			if (mi->oldV->type==VT_STREAM) {if ((rc=ses->queueForPurge(((StreamX*)mi->oldV->stream.is)->getAddr()))!=RC_OK) goto finish;}
#if 0
			else if ((pv->flags&VF_SSV)==0 || pv->type==VT_STREAM || pv->type!=mi->oldV->type /*and !inplace*/) fDelSSV=true;
			else {
				saddr.pageID=mi->href->pageID; saddr.idx=mi->href->idx; bool fDelete=false;
				const byte *data=pv->bstr; ulong lData=pv->length,lOld=mi->oldV->length;
				switch (pv->type) {
				default: assert(0);
				case VT_EXPR: assert(pv->op==OP_SET);
					data=new(ses) byte[lData=((Expr*)pv->expr)->length()];
					if (data==NULL) {rc=RC_NORESOURCES; goto finish;}
					lData=((Expr*)pv->expr)->serialize((byte*)data,lData); fDelete=true; break;
				case VT_STMT: assert(pv->op==OP_SET);
					data=new(ses) byte[lData=((Stmt*)pv->query)->length()];
					if (data==NULL) {rc=RC_NORESOURCES; goto finish;}
					if ((rc=((Stmt*)pv->query)->serialize((byte*)data,lData))!=RC_OK) goto finish; fDelete=true; break;
				case VT_STRING: case VT_URL: case VT_BSTR: break;
				}
				Value edit; edit.setEdit(data,lData,0,lOld); lstr=lOld; ty=(ValueType)pv->type;
				rc=editData(ses,saddr,lstr,edit,&pageSSV); if (fDelete) ses->free((byte*)data);
				break;
			}
#else
			else md.flags|=MF_DELSSV;
#endif
			continue;
		case OP_EDIT:
			if (mi->oldV->type==VT_STREAM) {
				assert(mi->href==NULL); byte *s=NULL;
				if (pv->edit.length!=0 && (s=(byte*)md.malloc(pv->edit.length))==NULL) {rc=RC_NORESOURCES; goto finish;}
				StreamX *str=(StreamX*)mi->oldV->stream.is; saddr=str->getAddr(); lstr=str->length(); ty=str->dataType(); 
				if ((rc=editData(ses,saddr,lstr,*pv,&pageSSV,s))==RC_OK || rc==RC_TRUE || rc==RC_FALSE) {
					str->destroy(); mi->oldV->stream.is=NULL;
					if ((str=new(&md) StreamX(saddr,lstr,ty,&md))==NULL) {rc=RC_NORESOURCES; goto finish;}
					if ((mi->oldV->stream.is=new(&md) StreamEdit(str,pv->edit.shift,pv->length,pv->edit.length,s,&md))==NULL)
						{str->destroy(); rc=RC_NORESOURCES; goto finish;}
					md.addObj((StreamX*)str);
					if (mi->newV==NULL) {
						if ((mi->newV=md.alloc<Value>())==NULL) {rc=RC_NORESOURCES; goto finish;}
						StreamX *pstr=new(&md) StreamX(saddr,lstr,ty,&md);
						if (pstr==NULL) {rc=RC_NORESOURCES; goto finish;}
						md.addObj(pstr); mi->newV->set(pstr); 
					}
					if (rc!=RC_OK || pv->edit.length!=pv->length) mi->flags|=PM_ESTREAM; else {mi->flags|=PM_PROCESSED; nMod--;}
					continue;
				}
			} else {
				assert(mi->href!=NULL); lstr=~0ULL;
				saddr.pageID=mi->href->pageID; saddr.idx=mi->href->idx; ty=mi->href->type.getType();
				if ((rc=editData(ses,saddr,lstr,*pv,&pageSSV))==RC_OK || rc==RC_TRUE || rc==RC_FALSE) mi->href->pageID=INVALID_PAGEID;
			}
			break;
		}
		switch (rc) {
		default: goto finish;
		case RC_OK: mi->flags|=PM_PROCESSED; nMod--; break;
		case RC_FALSE: case RC_TRUE:
			if (rc==RC_TRUE) {
				if (mi->newV!=NULL) freeV(*mi->newV);
				else if ((mi->newV=md.alloc<Value>())==NULL) {rc=RC_NORESOURCES; goto finish;}
				StreamX *pstr=new(&md) StreamX(saddr,lstr,ty,&md);
				if (pstr==NULL) {rc=RC_NORESOURCES; goto finish;}
				md.addObj(pstr); mi->newV->set(pstr); 
			} else {
				assert(mi->href!=NULL);
				HRefSSV *href=md.alloc<HRefSSV>(); if (href==NULL) {rc=RC_NORESOURCES; goto finish;}
				href->pageID=saddr.pageID; href->idx=saddr.idx; href->type=mi->href->type; mi->href=href;
			}
			mi->newV->property=mi->pv->property; mi->flags|=PM_ESTREAM; rc=RC_OK; break;
		}
	}
	pageSSV.release(ses);

	if ((md.flags&MF_DELSSV)!=0) for (HRefInfo *hi=md.hRef; hi!=NULL; hi=hi->next) if (hi->href.pageID!=INVALID_PAGEID) 
		{PageAddr addr={hi->href.pageID,hi->href.idx}; if ((rc=ses->queueForPurge(addr))!=RC_OK) goto finish;}

	if (nMod>0) {
		if ((md.flags&MF_MIGRATE)!=0) {
			size_t lr=lbuf;
			PBlockP newPB(ctx->heapMgr->getNewPage(expLen+sizeof(PageOff)+extraLen+newExtraLen,reserve,ses),QMGR_UFORCE);
			if (newPB.isNull()) {rc=RC_FULL; goto finish;}
			PageAddr newAddr={newPB->getPageID(),((HeapPageMgr::HeapPage*)newPB->getPageBuf())->nSlots};
			if ((rc=pcb->hpin->serialize(buf,lr,(HeapPageMgr::HeapPage*)pcb->pb->getPageBuf(),ses,expLen,(pinDescr&HOH_COMPACTREF)!=0))!=RC_OK) goto finish;
			if ((md.flags&MF_MOVED)!=0) {
				HeapPageMgr::HeapModEdit *hm=(HeapPageMgr::HeapModEdit*)fbuf; 
				hm->dscr=hm->shift=0; hm->oldPtr.len=hm->newPtr.len=PageAddrSize;
				hm->newPtr.offset=(hm->oldPtr.offset=sizeof(HeapPageMgr::HeapModEdit))+PageAddrSize;
				memcpy(fbuf+sizeof(HeapPageMgr::HeapModEdit),&oldAddr,PageAddrSize);
				memcpy(fbuf+sizeof(HeapPageMgr::HeapModEdit)+PageAddrSize,&newAddr,PageAddrSize);
				memcpy(&origAddr,pcb->hpin->getOrigLoc(),PageAddrSize);
			} else {
				byte *orig=(byte*)((HeapPageMgr::HeapPIN*)buf)->getOrigLoc();
				if (lr<PageAddrSize+(orig-buf)) {rc=RC_CORRUPTED; goto finish;}
				memmove(orig+PageAddrSize,orig,lr-PageAddrSize-(orig-buf));
				((HeapPageMgr::HeapPIN*)buf)->hdr.length+=PageAddrSize;
				((HeapPageMgr::HeapPIN*)buf)->lExtra+=PageAddrSize;
				((HeapPageMgr::HeapPIN*)buf)->setOrigLoc(oldAddr);
				HeapPageMgr::HeapV *hprop=((HeapPageMgr::HeapPIN*)buf)->getPropTab();
				for (ulong i=((HeapPageMgr::HeapPIN*)buf)->nProps; i!=0; ++hprop,--i)
					if (!hprop->type.isCompact()) hprop->offset+=PageAddrSize;
			}
			if ((rc=ctx->txMgr->update(newPB,ctx->heapMgr,(ulong)newAddr.idx<<HPOP_SHIFT|HPOP_INSERT,buf,lr))!=RC_OK) goto finish;
			byte *img=NULL; size_t limg=0; if (lr>lbuf) lbuf=lr; if ((md.flags&MF_MOVED)==0) {img=buf; limg=lr;}
			if ((rc=pcb->hpin->serialize(img,limg,(HeapPageMgr::HeapPage*)pcb->pb->getPageBuf(),&md,pcb->hpin->hdr.getLength()+PageAddrSize))!=RC_OK) goto finish;
			memcpy(img+limg-PageAddrSize,&newAddr,PageAddrSize);
			if ((rc=ctx->txMgr->update(pcb->pb,ctx->heapMgr,(ulong)oldAddr.idx<<HPOP_SHIFT|HPOP_MIGRATE,img,limg))!=RC_OK) goto finish;
			if (pcb==&cb) cb.pb.release(ses); else pcb=&cb;
			newPB.moveTo(cb.pb); cb=newAddr; cb.fill(); cb.properties=NULL; cb.nProperties=0; if (pin!=NULL) pin->addr=newAddr;
			for (mi=md.list; mi!=NULL; mi=mi->next) if ((mi->pInfo->flags&PM_PROCESSED)==0 && mi->pInfo->hprop!=NULL) {
				mi->pInfo->hprop=cb.hpin->findProperty(mi->pv->property);
				mi->pInfo->flags|=PM_PROCESSED; assert(mi->pInfo->hprop!=NULL);
				if ((mi->flags&PM_COMPACTREF)!=0) lrec+=PageAddrSize;
			}
			if ((md.flags&MF_REMOTE)!=0 && (rc=ctx->netMgr->updateAddr(id,newAddr))!=RC_OK) goto finish;
			if (cid!=STORE_INVALID_CLASSID) {
				Class *cls=ctx->classMgr->getClass(cid,RW_X_LOCK); if (cls==NULL) {rc=RC_NOTFOUND; goto finish;}
				rc=cls->setAddr(newAddr); cls->release(); if (rc!=RC_OK) goto finish;
			}
			lrec+=oldExtraLen+newExtraLen;
		}
		ulong lhdr=sizeof(HeapPageMgr::HeapPINMod)+int(nMod-1)*sizeof(HeapPageMgr::HeapPropMod); lrec+=lhdr;
		if ((buf==NULL||lbuf<lrec) && (buf=(byte*)ses->realloc(buf,lrec))==NULL) {rc=RC_NORESOURCES; goto finish;}
		HeapPageMgr::HeapPINMod *hpi=(HeapPageMgr::HeapPINMod*)buf; memset(buf,0,lhdr);
		hpi->nops=(uint16_t)nMod; hpi->stamp=stamp; hpi->descr=newDescr; ushort sht=0; ulong cnt=0;
		for (mi=md.list; mi!=NULL; mi=mi->next) if ((mi->flags&(PM_PROCESSED|PM_EXPAND))==0) {
			if (((pv=mi->pv)->op==OP_MOVE || pv->op==OP_MOVE_BEFORE) && (mi->pInfo->flags&PM_BIGC)==0) {
				HeapPageMgr::HeapPropMod *hpm=&hpi->ops[cnt++]; hpm->op=pv->op;
				hpm->propID=pv->property; hpm->eltId=mi->eid; hpm->eltKey=pv->ui;
				*(ElementID*)&hpm->newData=mi->eltKey; mi->flags|=PM_PROCESSED;
			} else if (pv->op!=OP_RENAME && (rc=putHeapMod(&hpi->ops[cnt++],mi,buf+lhdr,sht,*pcb))!=RC_OK) goto finish;
			assert(lhdr+sht<=lrec);
		}
		for (mi=md.list; mi!=NULL; mi=mi->next) if ((mi->flags&PM_PROCESSED)==0) {
			if ((pv=mi->pv)->op==OP_RENAME) {
				HeapPageMgr::HeapPropMod *hpm=&hpi->ops[cnt++]; hpm->op=OP_RENAME; 
				hpm->propID=pv->property; hpm->eltKey=pv->ui; hpm->eltId=STORE_COLLECTION_ID; mi->flags|=PM_PROCESSED;
			} else if ((rc=putHeapMod(&hpi->ops[cnt++],mi,buf+lhdr,sht,*pcb,(mi->flags&PM_LOCAL)!=0))!=RC_OK) goto finish;
			assert(lhdr+sht<=lrec);
		}
		assert(cnt==nMod);
		if ((rc=ctx->txMgr->update(pcb->pb,ctx->heapMgr,(ulong)pcb->addr.idx<<HPOP_SHIFT|HPOP_PINOP,buf,lhdr+sht))!=RC_OK) goto finish;
		if (pcb->fill()==NULL) {rc=RC_CORRUPTED; goto finish;}
	}

	if (xPropID!=STORE_INVALID_PROPID) ctx->classMgr->setMaxPropID(xPropID);
	if (pcb->hpin->nProps>0 && (pinDescr&(HOH_HIDDEN|HOH_NOINDEX))==0) rc=ctx->classMgr->classify(pcb,clrn);
	
	if ((pinDescr&HOH_HIDDEN)==0 && (pin!=NULL && pin->stamp==stamp || (md.flags&MF_NOTIFY)!=0 || ((pinDescr|newDescr)&HOH_FT)!=0 || clro.nClasses!=0 || clrn.nClasses!=0)) md.flags|=MF_REFRESH;

	if ((md.flags&MF_PREFIX)!=0) for (mi=md.list; mi!=NULL; mi=mi->next)
		if (mi->pv!=mi->newV && (mi->pv->flags&VF_PREFIX)!=0) switch (mi->pv->type) {
		default: assert(0);
		case VT_STREAM:
			if (mi->newV==NULL && (mi->newV=md.alloc<Value>())==NULL) {rc=RC_NORESOURCES; goto finish;}
			if ((md.flags&MF_REFRESH)!=0 && (rc=loadV(*mi->newV,mi->pv->property,*pcb,LOAD_SSV,&md,mi->eid!=STORE_COLLECTION_ID?mi->eltKey:STORE_COLLECTION_ID))==RC_OK) {
				if (mi->oldV==NULL) {mi->newV->op=OP_ADD; mi->newV->eid=mi->eid;}
				mi->pv=mi->newV;
			}
			break;
		case VT_ARRAY:
			pv=mi->pv;
			if ((md.flags&MF_REFRESH)!=0) {
				if (mi->newV==NULL && (mi->newV=md.alloc<Value>())==NULL) {rc=RC_NORESOURCES; goto finish;}
				mi->newV->set(new(&md) Value[pv->length],pv->length);
				if (mi->newV->varray==NULL) rc=RC_NORESOURCES;
				else {
					mi->newV->eid=mi->eid; mi->newV->property=pv->property; mi->newV->meta=pv->meta; mi->newV->op=pv->op;
					memset(const_cast<Value*>(mi->newV->varray),0,pv->length*sizeof(Value)); mi->pv=mi->newV;
				}
			}
			for (k=0; rc==RC_OK && k<pv->length; k++) {
				const Value &vv=pv->varray[k];
				if ((md.flags&MF_REFRESH)!=0) {
					assert(mi->pv==mi->newV && mi->pv->type==VT_ARRAY && mi->pv->length>k);
					rc=loadV(const_cast<Value&>(mi->pv->varray[k]),mi->pv->property,*pcb,LOAD_SSV,&md,vv.eid);
				}
			}
			break;
		case VT_COLLECTION:
			if ((md.flags&MF_REFRESH)!=0) {
				// ???
			}
			for (pv=mi->pv->nav->navigate(GO_FIRST); pv!=NULL; pv=mi->pv->nav->navigate(GO_NEXT)) {
				if ((md.flags&MF_REFRESH)!=0) {
					// ???
				}
			}
			break;
		case VT_STRUCT:
			//???
			break;
		}
	if (pin!=NULL) {
		if (pin->stamp!=stamp) {
			if (pin->properties!=NULL) for (unsigned j=0; j<pin->nProperties; j++) freeV(pin->properties[j]);
			size_t size = pcb->hpin->nProps*sizeof(Value);
			pin->properties = (Value*)(pin->properties==NULL ? ses->malloc(size) : ses->realloc(pin->properties,size));
			if (pin->properties!=NULL) {
				pin->nProperties=pcb->hpin->nProps; md.flags&=~MF_SSVS;
				const HeapPageMgr::HeapV *hprop=pcb->hpin->getPropTab();
				for (ulong i=0; i<pin->nProperties; ++i,++hprop) 
					if (loadVH(pin->properties[i],hprop,*pcb,LOAD_ENAV,ses)==RC_OK && (pin->properties[i].flags&VF_SSV)!=0)
						md.flags|=MF_SSVS;
				if ((md.flags&MF_SSVS)!=0) loadSSVs(pin->properties,pin->nProperties,pin->mode,ses,ses);
			}
		} else for (mi=md.list; mi!=NULL; mi=mi->next) if ((mi->flags&PM_INVALID)==0 && ((pv=mi->pv)->op!=OP_EDIT||(pv=mi->newV)!=NULL)) {
			if ((mi->flags&PM_SPILL)!=0) {
				if ((mi->pInfo->flags&PM_COLLECTION)!=0 && loadV(w,pv->property,*pcb,LOAD_SSV|LOAD_ENAV,ses)==RC_OK)
					{pin->modify(&w,STORE_COLLECTION_ID,STORE_COLLECTION_ID,MODP_EIDS|MODP_NEID,ses); freeV(w);}
			} else if ((mi->pInfo->flags&PM_BIGC)!=0 && mi->eid!=STORE_COLLECTION_ID) {
				Value *cv;
				if (mi->pInfo->first==mi && mi->pInfo->pcol!=NULL && 
					((mi->pInfo->flags&PM_NEWCOLL)!=0 || mi->pInfo->pcol->isModified()) && 
					(cv=(Value*)pin->findProperty(pv->property))!=NULL &&
					loadV(w,pv->property,*pcb,LOAD_ENAV,ses)==RC_OK) {freeV(*cv); *cv=w;}
			} else {
				ElementID eid=mi->eid,eltKey=pv->op==OP_MOVE||pv->op==OP_MOVE_BEFORE?pv->ui:mi->eltKey;
				if ((mi->flags&PM_CALCULATED)!=0 && pv->type==VT_COLLECTION && pv->op<=OP_ADD_BEFORE) {
					if (loadV(w,pv->property,*pcb,LOAD_SSV|LOAD_ENAV,ses)==RC_OK) {pv=&w; w.op=OP_SET; eid=eltKey=STORE_COLLECTION_ID;}
				} else if ((mi->pInfo->flags&PM_COLLECTION)!=0 && pv->op!=OP_RENAME && (pv->op!=OP_DELETE || mi->eid!=STORE_COLLECTION_ID)) {
					const Value *cv=pin->findProperty(pv->property);
					if (cv!=NULL && cv->type==VT_COLLECTION && loadV(w,pv->property,*pcb,LOAD_SSV|LOAD_ENAV,ses)==RC_OK)
						{pv=&w; w.op=OP_SET; eid=eltKey=STORE_COLLECTION_ID;}
				} else if (eltKey==STORE_COLLECTION_ID && (mi->flags&PM_ARRAY)==0) eltKey=lprefix;
				pin->modify(pv,eid,eltKey,MODP_NEID|MODP_EIDS,ses);
				if (pv==&w) freeV(w);
			}
		}
		pin->stamp=pcb->hpin->getStamp();
	}

	if (rc==RC_OK && (clrn.nClasses!=0 || clro.nClasses)) {
		for (ulong i=0,j=0; i<clrn.nClasses || j<clro.nClasses;) {
			if (i<clrn.nClasses && (j>=clro.nClasses || clrn.classes[i]->cid<clro.classes[j]->cid)) {
				if ((clrn.classes[i++]->notifications&CLASS_NOTIFY_JOIN)!=0) md.flags|=MF_CNOTIFY;
			} else if (j<clro.nClasses && (i>=clrn.nClasses || clrn.classes[i]->cid>clro.classes[j]->cid)) {
				if ((clro.classes[j++]->notifications&CLASS_NOTIFY_LEAVE)!=0) md.flags|=MF_CNOTIFY;
			} else {
				bool fCIndex=clrn.classes[i]->nIndexProps!=0; if (fCIndex) clru.nIndices++;
				if (fCIndex || (md.flags&MF_MIGRATE)!=0 || (clrn.classes[i]->notifications&CLASS_NOTIFY_CHANGE)!=0) {
					if (clru.classes==NULL && (clru.classes=(const ClassRef**)md.malloc(min(clro.nClasses,clrn.nClasses)*sizeof(ClassRef*)))==NULL)
						{rc=RC_NORESOURCES; goto finish;}
					clru.classes[clru.nClasses++]=clrn.classes[i];
					if ((clrn.classes[i]->notifications&CLASS_NOTIFY_CHANGE)!=0) md.flags|=MF_CNOTIFY;
				}
				if (i<--clrn.nClasses) memmove(&clrn.classes[i],&clrn.classes[i+1],(clrn.nClasses-i)*sizeof(ClassRefT*));
				if (j<--clro.nClasses) memmove(&clro.classes[j],&clro.classes[j+1],(clro.nClasses-j)*sizeof(ClassRefT*));
			}
		}
	}

	if (rc==RC_OK && ctx->queryMgr->notification!=NULL && (pinDescr&HOH_HIDDEN)==0 && (md.flags&(MF_NOTIFY|MF_CNOTIFY))!=0 
												&& (evt.data=new(ses) IStoreNotification::NotificationData[md.nev])!=NULL) {
		ulong cnt=0;
		for (mi=md.list; cnt<md.nev && mi!=NULL; mi=mi->next) if ((mi->flags&(PM_SPILL|PM_INVALID))==0) {
			IStoreNotification::NotificationData& nd=const_cast<IStoreNotification::NotificationData&>(evt.data[cnt++]);
			if ((mi->flags&PM_MOVE)!=0) {nd.eid=mi->eid; nd.epos=mi->pv->ui;} else {nd.eid=mi->eltKey; nd.epos=mi->epos;}
			nd.propID=mi->pv->property; nd.oldValue=mi->oldV;
			if (mi->pv!=mi->newV && mi->oldV==NULL) {
				if (mi->newV!=NULL) freeV(*mi->newV); else if ((mi->newV=md.alloc<Value>())==NULL) {rc=RC_NORESOURCES; break;}
				nd.newValue=mi->newV; *mi->newV=*mi->pv; mi->newV->flags=NO_HEAP; mi->newV->eid=nd.eid;
			} else if (mi->pv==NULL||mi->pv->op==OP_DELETE) nd.newValue=NULL;
			else if (mi->pv->op!=OP_EDIT) nd.newValue=mi->pv; else {nd.newValue=mi->newV; nEdits++;}
			evt.nData++;
		}
	}

	if (rc==RC_OK && clrn.nClasses!=0) rc=ctx->classMgr->index(ses,pcb,clrn,CI_INSERT,(const PropInfo**)md.ppi,md.npi);
	if (rc==RC_OK && clru.nClasses!=0 && ((md.flags&MF_MIGRATE)!=0||clru.nIndices>0)) rc=ctx->classMgr->index(ses,pcb,clru,CI_UPDATE,(const PropInfo**)md.ppi,md.npi,&oldAddr);
	if (rc==RC_OK && clro.nClasses!=0) {PageAddr saddr=pcb->addr; *pcb=oldAddr; rc=ctx->classMgr->index(ses,pcb,clro,CI_DELETE,(const PropInfo**)md.ppi,md.npi); *pcb=saddr;}

	if (pcb==&cb && !cb.pb.isNull()) {if (rc==RC_OK) ctx->heapMgr->reuse(cb.pb,ses,reserve,true); cb.pb.release(ses);}

	if (rc==RC_OK && (md.flags&(MF_MIGRATE|MF_MOVED))==(MF_MIGRATE|MF_MOVED) && nMod!=0) {
		assert(origAddr.defined());
		PBlock *oldPage=ctx->bufMgr->getPage(origAddr.pageID,ctx->heapMgr,pcb->pb.isNull()?PGCTL_XLOCK:QMGR_TRY|PGCTL_XLOCK,NULL,ses);
		if (oldPage==NULL) report(MSG_ERROR,"modifyPIN: cannot lock page %08X for migrate\n",origAddr.pageID);
		else {
			rc=ctx->txMgr->update(oldPage,ctx->heapMgr,(ulong)origAddr.idx<<HPOP_SHIFT|HPOP_EDIT,fbuf,sizeof(fbuf));
			oldPage->release(QMGR_UFORCE,ses);
		}
	}

	if (rc==RC_OK && ((pinDescr|newDescr)&(HOH_HIDDEN|HOH_NOINDEX|HOH_FT))==HOH_FT) {
		ChangeInfo inf={id,newDoc,NULL,NULL,STORE_INVALID_PROPID,STORE_COLLECTION_ID}; FTList ftl(md);
		for (mi=md.list; rc==RC_OK && mi!=NULL; mi=mi->next) if ((mi->flags&IX_MASK)!=0 && (mi->flags&(PM_INVALID|PM_SPILL))==0) {
			pv=mi->pv; inf.propID=pv->property; inf.eid=mi->eid==STORE_COLLECTION_ID?mi->eid:mi->eltKey;
			inf.oldV=mi->oldV; inf.newV=pv->op==OP_DELETE?NULL:pv->op==OP_EDIT?mi->newV:pv;
			rc=ctx->ftMgr->index(inf,&ftl,mi->flags&IX_MASK,(mi->flags&PM_SWORDS)!=0?FTMODE_STOPWORDS:0,ses);
			// check IX_RENAME
		}
		if (rc==RC_OK) rc=ctx->ftMgr->process(ftl,id,newDoc);
	}

	if (cid!=STORE_INVALID_CLASSID && (md.flags&MF_ADDACL)!=0) rc=ctx->classMgr->setFlags(cid,CLASS_ACL,CLASS_ACL);

	if (rc==RC_OK && (pinDescr&(HOH_HIDDEN|HOH_REPLICATED))==HOH_REPLICATED && (md.flags&MF_REPSES)==0) {		// and replication enabled
		// replication
	}

	if (rc==RC_OK && ctx->queryMgr->notification!=NULL && (pinDescr&HOH_HIDDEN)==0 && evt.data!=NULL) {
		if ((md.flags&MF_NOTIFY)!=0) {
			IStoreNotification::EventData *pev=(IStoreNotification::EventData*)ses->malloc(sizeof(IStoreNotification::EventData));
			pev->cid=STORE_INVALID_CLASSID; pev->type=IStoreNotification::NE_PIN_UPDATED; evt.events=pev; evt.nEvents++;
		}
		if (clro.classes!=NULL) for (ulong i=0; i<clro.nClasses; i++) if ((clro.classes[i]->notifications&CLASS_NOTIFY_LEAVE)!=0) {
			IStoreNotification::EventData *pev=evt.events==NULL?
				(IStoreNotification::EventData*)ses->malloc(sizeof(IStoreNotification::EventData)):
				(IStoreNotification::EventData*)ses->realloc((void*)evt.events,sizeof(IStoreNotification::EventData)*(evt.nEvents+1));
				evt.events=pev; pev+=evt.nEvents++; pev->cid=clro.classes[i]->cid; pev->type=IStoreNotification::NE_CLASS_INSTANCE_REMOVED; md.flags|=MF_NOTIFY;
		}
		if (clru.classes!=NULL) for (ulong i=0; i<clru.nClasses; i++) if ((clru.classes[i]->notifications&CLASS_NOTIFY_CHANGE)!=0) {
			IStoreNotification::EventData *pev=evt.events==NULL?
				(IStoreNotification::EventData*)ses->malloc(sizeof(IStoreNotification::EventData)):
				(IStoreNotification::EventData*)ses->realloc((void*)evt.events,sizeof(IStoreNotification::EventData)*(evt.nEvents+1));
			evt.events=pev; pev+=evt.nEvents++; pev->cid=clru.classes[i]->cid; pev->type=IStoreNotification::NE_CLASS_INSTANCE_CHANGED; md.flags|=MF_NOTIFY;
		}
		if (clrn.classes!=NULL) for (ulong i=0; i<clrn.nClasses; i++) if ((clrn.classes[i]->notifications&CLASS_NOTIFY_JOIN)!=0) {
			IStoreNotification::EventData *pev=evt.events==NULL?
				(IStoreNotification::EventData*)ses->malloc(sizeof(IStoreNotification::EventData)):
				(IStoreNotification::EventData*)ses->realloc((void*)evt.events,sizeof(IStoreNotification::EventData)*(evt.nEvents+1));
			evt.events=pev; pev+=evt.nEvents++; pev->cid=clrn.classes[i]->cid; pev->type=IStoreNotification::NE_CLASS_INSTANCE_ADDED; md.flags|=MF_NOTIFY;
		}
		uint64_t txid=ses!=NULL?ses->getTXID():INVALID_TXID;
		IStoreNotification::NotificationEvent *pevt=new(ses) IStoreNotification::NotificationEvent;
		if (pevt!=NULL) {
			*pevt=evt; IStoreNotification::NotificationData *pnd=(IStoreNotification::NotificationData*)pevt->data; ulong cnt;
			if (nEdits>0) for (mi=md.list,cnt=0; cnt<nEdits && mi!=NULL; mi=mi->next) if ((mi->flags&PM_SPILL)==0) {
				if (mi->pv!=NULL && mi->pv->op==OP_EDIT && pnd->newValue==mi->newV) {pnd->newValue=mi->pv; cnt++;}
				pnd++;
			}
			try {ctx->queryMgr->notification->notify(pevt,1,txid);} catch (...) {}
			ses->free(pevt);
		}
		ses->free((void*)evt.events);
	}
	ses->free((void*)evt.data);

finish:
	if (buf!=NULL) ses->free(buf);
	if (rc!=RC_OK) {pcb->pb.release(ses); if (pin!=NULL) pin->addr=oldAddr;} else tx.ok();
	return rc;
}

size_t QueryPrc::splitLength(const Value *pv)
{
	size_t len=0;
	if ((pv->flags&VF_SSV)==0) switch (pv->type) {
	default: break;
	case VT_STRING: case VT_BSTR: case VT_URL:
		len=pv->length; len+=len>0xff?2:1; break;
	case VT_STREAM:
		if (pv->stream.prefix!=NULL) {len=*(uint32_t*)pv->stream.prefix; len+=len>0xff?2:1;}
		break;
	case VT_EXPR:
		len=((Expr*)pv->expr)->serSize(); break;
	case VT_STMT:
		len=((Stmt*)pv->stmt)->serSize(); len+=afy_len16(len); break;
	}
	return len<=sizeof(HRefSSV)?0:ceil(len,HP_ALIGN);
}

RC QueryPrc::findCandidateSSVs(CandidateSSVs& cs,const Value *pv,ulong nv,bool fSplit,MemAlloc *ma,const AllocCtrl *act,PropertyID pid,ModInfo *mi)
{
	size_t length,dl; RC rc=RC_OK; const Value *cv;
	size_t xSize=HeapPageMgr::contentSize(ctx->bufMgr->getPageSize())-sizeof(PageOff);
	for (ulong i=0,j; rc==RC_OK && i<nv; ++pv,++i) if ((pv->flags&VF_SSV)==0) {
		switch (pv->type) {
		default: 
			length=splitLength(pv); dl=sizeof(HRefSSV);
			if (length!=0 && !fSplit && act!=NULL && length<act->ssvThreshold) continue;
			if (mi!=NULL && (mi->pInfo->flags&(PM_NEWCOLL|PM_SCOLL))==(PM_NEWCOLL|PM_SCOLL)) {
				dl+=long(sizeof(HeapPageMgr::HeapVV)+sizeof(HeapPageMgr::HeapKey));
				length+=long(sizeof(HeapPageMgr::HeapVV)+sizeof(HeapPageMgr::HeapKey));
			}
			break;	// sizeof(HeapLOB) ???
		case VT_ARRAY:
			if (pv->length<(act!=NULL&&act->arrayThreshold!=~0ul?act->arrayThreshold:ARRAY_THRESHOLD)) {
				if ((rc=findCandidateSSVs(cs,pv->varray,pv->length,fSplit,ma,act,pv->property))!=RC_OK) return rc;
				continue;
			}
			length=sizeof(HeapPageMgr::HeapVV)-sizeof(HeapPageMgr::HeapV)+sizeof(HeapPageMgr::HeapKey);
			for (j=0; j<pv->length; j++) {
				if ((rc=estimateLength(pv->varray[j],dl,MODE_PREFIX_READ,xSize,ma))!=RC_OK) return rc;
				length+=ceil(dl,HP_ALIGN)+sizeof(HeapPageMgr::HeapV);
			}
			dl=sizeof(HeapPageMgr::HeapExtCollection); break;
		case VT_COLLECTION:
			if (pv->nav->count()<(act!=NULL&&act->arrayThreshold!=~0ul?act->arrayThreshold:ARRAY_THRESHOLD)) {
				// ???
				continue;
			}
			length=sizeof(HeapPageMgr::HeapVV)-sizeof(HeapPageMgr::HeapV)+sizeof(HeapPageMgr::HeapKey);
			for (cv=pv->nav->navigate(GO_FIRST); cv!=NULL; cv=pv->nav->navigate(GO_NEXT)) {
				if ((rc=estimateLength(*cv,dl,MODE_PREFIX_READ,xSize,ma))!=RC_OK) return rc;
				length+=ceil(dl,HP_ALIGN)+sizeof(HeapPageMgr::HeapV);
			}
			dl=sizeof(HeapPageMgr::HeapExtCollection); break;
		case VT_STRUCT:
			length=sizeof(HeapPageMgr::HeapVV)-sizeof(HeapPageMgr::HeapV);
			//???
			break;
		}
		if (length>dl && (fSplit || length>bigThreshold)) rc=cs.insert(pv,pid==STORE_INVALID_PROPID?pv->property:pid,length,mi,dl);
	}
	return rc;
}

int __cdecl QueryPrc::cmpCandidateSSV(const void *v1, const void *v2)
{
	return cmp3(((const CandidateSSV*)v2)->length,((const CandidateSSV*)v1)->length);
}

RC QueryPrc::putHeapMod(HeapPageMgr::HeapPropMod *hpm,ModInfo *mi,byte *buf,ushort& sht,PINEx& cb,bool fLocal)
{
	RC rc; ushort osht; hpm->propID=mi->pv->property; mi->flags|=PM_PROCESSED;
	if ((mi->pInfo->flags&PM_BIGC)!=0) {
		hpm->op=mi->pInfo->hprop==NULL?OP_ADD:mi->pv->op==OP_DELETE&&mi->pv->eid==STORE_COLLECTION_ID?OP_DELETE:OP_SET;
		hpm->eltId=hpm->eltKey=STORE_COLLECTION_ID;
	} else {
		hpm->op=(mi->flags&PM_ESTREAM)!=0?OP_SET:mi->oldV!=NULL||mi->pv->op==OP_ADD_BEFORE?(ExprOp)mi->pv->op:OP_ADD;
		hpm->eltId=(mi->pInfo->flags&PM_NEWCOLL)!=0 && mi->pInfo->hprop==NULL && mi->pInfo->first==mi ? mi->eltKey : mi->eid;
		hpm->eltKey=hpm->op!=OP_SET||(mi->pInfo->flags&(PM_NEWCOLL|PM_SCOLL))==(PM_NEWCOLL|PM_SCOLL)?mi->eltKey:STORE_COLLECTION_ID;
		assert(hpm->op<OP_FIRST_EXPR);
	}
	if (hpm->op==OP_EDIT && (mi->flags&PM_ESTREAM)==0) {
		// special descriptors for OP_EDIT
		const Value *pv=mi->oldV; assert(pv!=NULL && (mi->pv->flags&VF_SSV)==0);
		uint32_t esht=mi->pv->edit.shift==~0ULL?pv->length:uint32_t(mi->pv->edit.shift);
		hpm->shift=ushort(esht); assert((esht&~0xFFFF)==0 && esht<=pv->length);
		hpm->newData.ptr.offset=sht; hpm->newData.ptr.len=ushort(mi->pv->length);
		hpm->newData.type.setType((ValueType)mi->pv->type,HDF_NORMAL);
		memcpy(buf+sht,mi->pv->bstr,hpm->newData.ptr.len); sht+=(ushort)ceil(hpm->newData.ptr.len,HP_ALIGN);
		hpm->oldData.ptr.offset=sht; hpm->oldData.ptr.len=ushort(mi->pv->edit.length);
		hpm->oldData.type.setType((ValueType)mi->oldV->type,HDF_NORMAL);
		memcpy(buf+sht,pv->bstr+esht,hpm->oldData.ptr.len); sht+=(ushort)ceil(hpm->oldData.ptr.len,HP_ALIGN);
		return RC_OK;
	}
	// new value descriptor, empty for deletes
	if ((mi->flags&PM_ESTREAM)!=0) {
		assert(mi->newV!=NULL);
		hpm->newData.ptr.offset=sht; hpm->newData.type.flags=mi->newV->meta; if (fLocal) hpm->newData.type.flags|=META_PROP_LOCAL;
		if (mi->newV->type==VT_STREAM) {
			StreamX *ps=(StreamX*)mi->newV->stream.is; HLOB *hl=(HLOB*)(buf+sht);
			const PageAddr &sa=ps->getAddr(); hl->ref.pageID=sa.pageID; hl->ref.idx=sa.idx;
			hl->ref.type.setType(ps->dataType(),HDF_NORMAL); hl->ref.type.flags=0; hl->len=ps->length();
			hpm->newData.type.setType(VT_STREAM,HDF_LONG); sht+=hpm->newData.ptr.len=sizeof(HLOB);
		} else {
			assert(mi->href!=NULL); memcpy(buf+sht,mi->href,sizeof(HRefSSV)); 
			hpm->newData.type.setType(VT_STREAM,HDF_SHORT); sht+=hpm->newData.ptr.len=sizeof(HRefSSV);
		}
	} else if ((mi->pInfo->flags&PM_BIGC)!=0 && mi->pInfo->pcol!=NULL && mi->pInfo->pcol->getDescriptor()!=NULL) {
		const HeapPageMgr::HeapExtCollection *coll=mi->pInfo->pcol->getDescriptor();
		ushort len=HeapPageMgr::collDescrSize(coll); hpm->newData.ptr.offset=sht; hpm->newData.ptr.len=len;
		hpm->newData.type.flags=mi->pv->meta; hpm->newData.type.setType(VT_ARRAY,HDF_LONG);
		memcpy(buf+sht,coll,len); sht+=(ushort)ceil(len,HP_ALIGN);
	} else if (hpm->op==OP_DELETE) hpm->newData.type.setType(VT_ERROR,HDF_COMPACT);
	else {
		osht=hpm->newData.ptr.offset=sht; hpm->newData.type.flags=mi->pv->meta; if (fLocal) hpm->newData.type.flags|=META_PROP_LOCAL;
		if ((rc=persistValue(*mi->pv,sht,hpm->newData.type,hpm->newData.ptr.offset,buf+sht,NULL,cb.addr,
														(mi->pInfo->flags&PM_GENEIDS)!=0?&mi->pInfo->maxKey:NULL))!=RC_OK) return rc;
		hpm->newData.ptr.len=sht-osht; sht=(ushort)ceil(sht,HP_ALIGN);
		if (cb.pb.isNull() && (cb.pb.getPage(cb.addr.pageID,ctx->heapMgr,PGCTL_XLOCK,cb.getSes())==NULL||cb.fill()==NULL)) return RC_NOTFOUND;
	}
	// old value descriptor, empty for inserts
	if (mi->oldV==NULL && (mi->flags&PM_PUTOLD)==0 && ((mi->pInfo->flags&(PM_BIGC|PM_NEWCOLL))!=PM_BIGC || (mi->flags&PM_BIGC)==0)) {
		hpm->oldData.type.setType(VT_ERROR,HDF_COMPACT); hpm->oldData.type.flags=0; hpm->oldData.ptr.len=0;
		hpm->oldData.ptr.offset=(mi->pInfo->flags&PM_SCOLL)!=0?ushort(~0u):mi->pInfo->hprop!=NULL&&(mi->pInfo->flags&PM_COLLECTION)==0?ushort(~1u):0;
	} else {
		const HeapPageMgr::HeapV *hprop=mi->pInfo->hprop,*elt=NULL; assert(hprop!=NULL);
		const byte *pData=cb.pb->getPageBuf()+hprop->offset; HType ht=hprop->type;
		bool fElt=(mi->pInfo->flags&(PM_COLLECTION|PM_BIGC))==PM_COLLECTION; ElementID eid=mi->eid;
		if (fElt && eid==STORE_COLLECTION_ID) {if ((mi->pInfo->flags&PM_SCOLL)!=0) eid=STORE_FIRST_ELEMENT; else fElt=false;}
		if (fElt) {
			elt=((const HeapPageMgr::HeapVV*)pData)->findElt(eid); assert(elt!=NULL); 
			if (hpm->op==OP_DELETE) hpm->eltKey=elt!=((const HeapPageMgr::HeapVV*)pData)->start?elt[-1].getID():STORE_FIRST_ELEMENT;
			pData=cb.pb->getPageBuf()+elt->offset; ht=elt->type;
		} else if (hpm->op!=OP_DELETE)
			hpm->newData.type.flags|=ht.flags&(META_PROP_PART|META_PROP_SSTORAGE|META_PROP_NONOTIFICATION|META_PROP_NOFTINDEX|META_PROP_LOCAL);
		hpm->oldData.type=ht;
		if (ht.isCompact()) {hpm->oldData.ptr.len=0; hpm->oldData.ptr.offset=elt!=NULL?elt->offset:hprop->offset;}
		else {
			hpm->oldData.ptr.len = fElt || !hprop->type.isCollection() ? HeapPageMgr::moveData(buf+sht,pData,ht) :
												HeapPageMgr::moveCollection(ht,buf+sht,0,cb.pb->getPageBuf(),hprop->offset);
			hpm->oldData.ptr.offset=sht; sht+=(ushort)ceil(hpm->oldData.ptr.len,HP_ALIGN);
		}
	}
	return RC_OK;
}

RC QueryPrc::rename(ChangeInfo& val,PropertyID pid,ulong flags,bool fSync)
{
	// ...
	return RC_OK;
}

RC QueryPrc::undeletePINs(Session *ses,const PID *pids,unsigned nPins)
{
	if (ses==NULL) return RC_NOSESSION; TxSP tx(ses); RC rc=tx.start(); if (rc!=RC_OK) return rc;
	for (unsigned i=0; i<nPins; i++) if ((rc=setFlag(ses,pids[i],NULL,HOH_DELETED,true))!=RC_OK) return rc;
	tx.ok(); return RC_OK;
}

RC QueryPrc::setFlag(Session *ses,const PID& id,PageAddr *addr,ushort flag,bool fReset)
{
	if (id.pid==STORE_INVALID_PID || id.ident==STORE_INVALID_IDENTITY) return RC_NOTFOUND;
	if (ses==NULL) return RC_NOSESSION; if (ses->isRestore()) return RC_OTHER;
	if (ctx->isServerLocked() || ses->inReadTx()) return RC_READTX;
	
	TxSP tx(ses); RC rc; if ((rc=tx.start(TXI_DEFAULT,TX_ATOMIC))!=RC_OK) return rc;

	PINEx cb(ses,id); if (addr!=NULL) cb.addr=*addr;
	if ((rc=getBody(cb,TVO_UPD,fReset&&(flag&HOH_DELETED)!=0?GB_DELETED:0))!=RC_OK) return rc;
	if (addr!=NULL) *addr=cb.addr;

	ushort dscr=cb.hpin->hdr.descr; if (((dscr&flag)!=0)!=fReset) return RC_OK;
	HeapPageMgr::HeapSetFlag hsf; hsf.mask=flag; hsf.value=fReset?0:flag;
	if ((rc=ctx->txMgr->update(cb.pb,ctx->heapMgr,(ulong)cb.addr.idx<<HPOP_SHIFT|HPOP_SETFLAG,(byte*)&hsf,sizeof(hsf)))==RC_OK) {
		if (!fReset && (flag&HOH_NOINDEX)!=0) {
			// remove from all indices
		} else if (fReset && (flag&HOH_DELETED)!=0) {
			cb.fill(); assert(cb.hpin!=NULL);
			if ((dscr&HOH_NOINDEX)==0) {
				ChangeInfo inf={id,PIN::defPID,NULL,NULL,STORE_INVALID_PROPID,STORE_COLLECTION_ID};
				const HeapPageMgr::HeapV *hprop=cb.hpin->findProperty(PROP_SPEC_DOCUMENT);
				if (hprop!=NULL) ((HeapPageMgr::HeapPage*)cb.pb->getPageBuf())->getRef(inf.docID,hprop->type.getFormat(),hprop->offset);
				hprop=cb.hpin->getPropTab(); Value v; SubAlloc sa(ses); FTList ftl(sa);
				for (ulong k=0; rc==RC_OK && k<cb.hpin->nProps; ++k,++hprop)
					if ((hprop->type.flags&META_PROP_NOFTINDEX)==0 && (rc=loadVH(v,hprop,cb,LOAD_SSV,NULL))==RC_OK) {
						inf.propID=v.property; inf.newV=&v; 
						rc=ctx->ftMgr->index(inf,&ftl,IX_NFT,(hprop->type.flags&META_PROP_STOPWORDS)!=0?FTMODE_STOPWORDS:0,ses);
						freeV(v);
					}
				if (rc==RC_OK) rc=ctx->ftMgr->process(ftl,id,inf.docID);
			}
			bool fNotify=(dscr&HOH_NOTIFICATION)!=0;
			IStoreNotification::NotificationEvent evt={id,NULL,0,NULL,0,ses->isReplication()};
			Value *vals=NULL; ClassResult clr(ses,ctx);
			if (cb.hpin->nProps>0 && (rc=ctx->classMgr->classify(&cb,clr))==RC_OK && clr.nClasses>0)
				rc=ctx->classMgr->index(ses,&cb,clr,CI_UDELETE);
			if (rc==RC_OK && (dscr&(HOH_REPLICATED|HOH_NOREPLICATION))==HOH_REPLICATED) {	// and replication enabled	HIDDEN?
				// replicate
			}
			if (rc==RC_OK && (fNotify||clr.classes!=NULL)) {
				evt.events=(IStoreNotification::EventData*)ses->malloc((clr.nClasses+1)*sizeof(IStoreNotification::EventData));
				if (evt.events!=NULL) {
					if (fNotify) {
						IStoreNotification::EventData& ev=(IStoreNotification::EventData&)evt.events[0];
						ev.cid=STORE_INVALID_CLASSID; ev.type=IStoreNotification::NE_PIN_CREATED; evt.nEvents=1;
					}
					if (clr.classes!=NULL) for (ulong i=0; i<clr.nClasses; i++) if ((clr.classes[i]->notifications&CLASS_NOTIFY_NEW)!=0) {
						IStoreNotification::EventData& ev=(IStoreNotification::EventData&)evt.events[evt.nEvents++];
						ev.type=IStoreNotification::NE_CLASS_INSTANCE_ADDED; ev.cid=clr.classes[i]->cid;
					}
				}
				if (ctx->queryMgr->notification!=NULL && evt.events!=NULL && evt.nEvents>0) {
					const HeapPageMgr::HeapV *hprop=cb.hpin->getPropTab(); evt.nData=cb.hpin->nProps; 
					if ((vals=new(ses) Value[evt.nData])!=NULL) memset(vals,0,sizeof(Value)*evt.nData);
					evt.data=new(ses) IStoreNotification::NotificationData[evt.nData];
					if (evt.data!=NULL && vals!=NULL) for (unsigned i=0; i<evt.nData; ++i,++hprop) {
						if ((rc=loadVH(vals[i],hprop,cb,LOAD_SSV,ses))!=RC_OK) break;
						IStoreNotification::NotificationData& nd=const_cast<IStoreNotification::NotificationData&>(evt.data[i]);
						nd.propID=vals[i].property; nd.epos=STORE_COLLECTION_ID; nd.eid=vals[i].eid; nd.oldValue=NULL; nd.newValue=&vals[i];
					}
				}
			}
			cb.pb.release(ses);
			if (rc==RC_OK && ctx->queryMgr->notification!=NULL && evt.events!=NULL && evt.nEvents>0 && evt.data!=NULL)
				try {ctx->queryMgr->notification->notify(&evt,1,ses->getTXID());} catch (...) {}
			if (vals!=NULL) {
				for (ulong i=0; i<evt.nData; i++) freeV(vals[i]);
				ses->free(vals);
			}
			if (evt.data!=NULL) ses->free((void*)evt.data);
			if (evt.events!=NULL) ses->free((void*)evt.events);
		}
    }
	if (rc==RC_OK) tx.ok(); return rc;
}

inline bool expand(Value *&props,ulong& xprops,Session *ses)
{
	props=props==NULL?(Value*)ses->malloc((xprops=20)*sizeof(Value)):(Value*)ses->realloc(props,(xprops+=xprops)*sizeof(Value));
	return props!=NULL;
}

RC QueryPrc::diffPIN(const PIN *pin,PINEx& cb,Value *&diffProps,ulong &nDiffProps,Session *ses)
{
	RC rc=RC_OK; ulong xDiffs=0; diffProps=NULL; nDiffProps=0; assert(ses!=NULL);
	if (cb.pb.isNull()) {assert(cb.addr.defined()); if ((rc=getBody(cb,TVO_UPD))!=RC_OK) return rc;}

	const Value *prop=pin->properties,*pend=prop+pin->nProperties;
	const HeapPageMgr::HeapV *hprop=cb.hpin->getPropTab(); 
	const HeapPageMgr::HeapV *hend=hprop+cb.hpin->nProps;
	for (;;) {
		if (prop<pend) {
			if (hprop>=hend || prop->property<hprop->getID()) {
				if (nDiffProps>=xDiffs && !expand(diffProps,xDiffs,ses)) {rc=RC_NORESOURCES; break;}
				Value &v=diffProps[nDiffProps++]; v=*prop++; v.op=OP_ADD; v.flags=NO_HEAP; continue;
			}
			if (hprop<hend && prop->property==hprop->getID()) {
				ValueType ty=hprop->type.getType(); bool fSet=true; Value v;
				if (ty==VT_ARRAY && (prop->type==VT_ARRAY||prop->type==VT_COLLECTION)) {		// spec case! 1 element array
					// check elements
				} else if (ty==prop->type && loadVH(v,hprop,cb,LOAD_SSV,NULL)==RC_OK)
					{fSet=!(v==*prop); freeV(v);}
				if (fSet) {
					if (nDiffProps>=xDiffs && !expand(diffProps,xDiffs,ses)) {rc=RC_NORESOURCES; break;}
					Value &v=diffProps[nDiffProps++]; v=*prop; v.op=OP_SET; v.flags=NO_HEAP;
				}
				++prop; ++hprop; continue;
			}
		}
		if (hprop>=hend) break;
		if (nDiffProps>=xDiffs && !expand(diffProps,xDiffs,ses)) {rc=RC_NORESOURCES; break;}
		diffProps[nDiffProps++].setDelete(hprop->getID()); ++hprop;
	}
	return rc;
}
