/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "blob.h"
#include "queryprc.h"
#include "txmgr.h"
#include "fsmgr.h"
#include "expr.h"

using namespace MVStoreKernel;

StreamX::StreamX(const PageAddr& addr,uint64_t l,ValueType ty,MemAlloc *ma) 
: start(addr),type(ty),allc(ma),ctx(StoreCtx::get()),len(l),pos(0),current(addr),shift(0)
{
	BlobReadTab::Find findBlob(ctx->bigMgr->blobReadTab,addr);
	BlobRead *blob=findBlob.findLock(RW_X_LOCK);
	if (blob!=NULL) 
		blob->nReaders++; 
	else if ((blob=new(ctx->bigMgr->freeBlob.alloc(sizeof(BlobRead))) BlobRead(addr))!=NULL)
		ctx->bigMgr->blobReadTab.insertNoLock(blob,findBlob.getIdx());
	else {
		// error msg
	}
	findBlob.unlock();
}

StreamX::~StreamX()
{
}

ValueType StreamX::dataType() const
{
	return type;
}

const PageAddr&	StreamX::getAddr() const 
{
	return start;
}

uint64_t StreamX::length() const
{
	PBlock *pb;
	if (len==uint64_t(~0ULL) && (pb=ctx->bufMgr->getPage(start.pageID,ctx->ssvMgr,0))!=NULL) {
		const HeapPageMgr::HeapPage *hp=(const HeapPageMgr::HeapPage*)pb->getPageBuf(); 
		const HeapPageMgr::HeapObjHeader *hdr=hp->getObject(hp->getOffset(start.idx));
		if (hdr!=NULL && hdr->getType()==HO_SSVALUE) len=hdr->length-sizeof(HeapPageMgr::HeapObjHeader);
		pb->release();
	}
	return len;
}

size_t StreamX::read(void *buf,size_t maxLength)
{
	PBlock *pb=NULL; size_t lData=0;
	do {
		if (pos>=len || (pb=ctx->bufMgr->getPage(current.pageID,ctx->ssvMgr,0,pb))==NULL) break;
		const HeapPageMgr::HeapPage *hp=(const HeapPageMgr::HeapPage*)pb->getPageBuf();
		const HeapPageMgr::HeapObjHeader *hdr=hp->getObject(hp->getOffset(current.idx));
		size_t lbuf=0; const byte *p=NULL; HeapObjType hty=HO_ALL;
		if (hdr!=NULL) switch (hty=hdr->getType()) {
		default: break;
		case HO_SSVALUE: p=(byte*)(hdr+1); lbuf=hdr->length-sizeof(HeapPageMgr::HeapObjHeader); break;
		case HO_BLOB: p=(byte*)hdr+sizeof(HeapPageMgr::HeapLOB); lbuf=hdr->length-sizeof(HeapPageMgr::HeapLOB); break;
		}
		if (p!=NULL && shift<lbuf) {
			size_t l=lbuf-shift; if (l>maxLength) l=maxLength;
			memcpy((byte*)buf+lData,p+shift,l);
			lData+=l; maxLength-=l; pos+=l; 
			if ((shift+=l)<lbuf) break;
		}
		if (hty==HO_BLOB) memcpy(&current,((const HeapPageMgr::HeapLOB*)hdr)->next,PageAddrSize); else current=PageAddr::invAddr;
		shift=0; if (current.pageID==INVALID_PAGEID||current.idx==INVALID_INDEX) {pos=len; break;}
	} while (maxLength>0);
	if (pb!=NULL) pb->release();
	return lData;
}

size_t StreamX::readChunk(uint64_t offset,void *buf,size_t maxLength)
{
	PBlock *pb=NULL; size_t lData=0; uint64_t sht=0; PageAddr addr=start;
	do {
		if ((pb=ctx->bufMgr->getPage(addr.pageID,ctx->ssvMgr,0,pb))==NULL) break;
		const HeapPageMgr::HeapPage *hp=(const HeapPageMgr::HeapPage*)pb->getPageBuf(); 
		const HeapPageMgr::HeapObjHeader *hdr=hp->getObject(hp->getOffset(addr.idx));
		size_t lbuf=0; const byte *p=NULL; addr=PageAddr::invAddr;
		if (hdr!=NULL) switch (hdr->getType()) {
		default: break;
		case HO_SSVALUE:
			p=(byte*)(hdr+1); lbuf=hdr->length-sizeof(HeapPageMgr::HeapObjHeader); break;
		case HO_BLOB:
			p=(byte*)hdr+sizeof(HeapPageMgr::HeapLOB); lbuf=hdr->length-sizeof(HeapPageMgr::HeapLOB); 
			memcpy(&addr,((HeapPageMgr::HeapLOB*)hdr)->next,PageAddrSize); break;
		}
		if (p!=NULL && sht+lbuf>offset) {
			size_t sh=sht>=offset?0:size_t(offset-sht); 
			size_t l=lbuf-sh; if (l>maxLength) l=maxLength;
			memcpy((byte*)buf+lData,p+sh,l); lData+=l; 
			if ((maxLength-=l)==0) break;
		}
		sht+=lbuf;
	} while (addr.pageID!=INVALID_PAGEID && addr.idx!=INVALID_INDEX);
	if (pb!=NULL) pb->release();
	return lData;
}

IStream	*StreamX::clone() const
{
	Session *ses=Session::getSession(); return new(ses) StreamX(start,len,type,ses);
}

RC StreamX::reset()
{
	current=start; shift=0; pos=0; return RC_OK;
}

void StreamX::destroy()
{
	BlobReadTab::Find findBlob(ctx->bigMgr->blobReadTab,start);
	BlobRead *blob=findBlob.findLock(RW_X_LOCK); bool fDelete=false;
	if (blob==NULL) {
		// error message
		findBlob.unlock();
	} else {
		assert(blob->nReaders>0);
		if (--blob->nReaders!=0) findBlob.unlock();
		else {fDelete=blob->fDelete; findBlob.remove(blob); ctx->bigMgr->freeBlob.dealloc(blob);}
	}
	if (fDelete) ctx->queryMgr->deleteData(start);
	MemAlloc *ma=allc; this->~StreamX(); if (ma!=NULL) ma->free((void*)this);
}

void StreamX::destroyObj()
{
	destroy();
}

namespace MVStoreKernel
{
	class StrTxDelete : public TxDelete
	{
		friend	class	StreamX;
		PageAddr	start;
		StoreCtx	*ctx;
		StrTxDelete(const PageAddr& st,StoreCtx *ct) : start(st),ctx(ct) {}
		RC deleteData() {
			BlobReadTab::Find findBlob(ctx->bigMgr->blobReadTab,start);
			BlobRead *blob = findBlob.findLock(RW_S_LOCK);
			if (blob!=NULL) blob->fDelete=true;
			findBlob.unlock();
			return blob==NULL?ctx->queryMgr->deleteData(start):RC_OK;
		}
		void release() {
			// ???
		}
	};
};

RC StreamX::deleteData(Session *ses)
{
	if (ses==NULL) return RC_NOSESSION; if (ses->inReadTx()) return RC_INTERNAL;
	StrTxDelete *td=new(ses) StrTxDelete(start,ctx); if (td==NULL) return RC_NORESOURCES;
	ses->addTxDelete(td); return RC_OK;
}

//---------------------------------------------------------------------------------------------------------

StreamEdit::~StreamEdit()
{
	if (allc!=NULL) {allc->free(str); if (stream!=NULL) stream->destroy();}
}

ValueType StreamEdit::dataType() const
{
	return stream->dataType();
}

uint64_t StreamEdit::length() const
{
	return stream->length()-lOld+lNew;
}

size_t StreamEdit::read(void *buf,size_t maxLength)
{
	try {
		size_t lData=0,l;
		if (epos<eshift) {
			l=epos+maxLength<=eshift?maxLength:size_t(eshift-epos); assert(epos==opos);
			epos+=lData=stream->read(buf,l); opos+=lData;
			if (lData<l||(maxLength-=lData)==0) return lData;
			buf=(byte*)buf+lData;
		}
		if (epos<eshift+lNew) {
			size_t sht=size_t(epos-eshift); l=size_t(lNew-sht); if (l>maxLength) l=maxLength;
			memcpy(buf,str+sht,l); lData+=l; epos+=l; if ((maxLength-=l)==0) return lData; 
			buf=(byte*)buf+l;
		}
		if (opos<eshift+lOld) {
			byte *w=(byte*)alloca(0x200); if (w==NULL) return lData;	// ???
			do {
				if ((l=size_t(eshift+lOld-opos))>0x200) l=0x200;
				size_t ll=stream->read(w,l); opos+=ll; if (ll<l) return lData;
			} while (opos<eshift+lOld);
		}
		l=stream->read(buf,maxLength); epos+=l; opos+=l;
		return lData+l;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"StreamEdit::read: exception reading provided stream\n");}
	return 0;
}

size_t StreamEdit::readChunk(uint64_t offset,void *buf,size_t maxLength)
{
	try {
		size_t lData=0;
		if (offset<eshift) {
			size_t l=offset+maxLength<=eshift?maxLength:size_t(eshift-offset);
			lData=stream->readChunk(offset,buf,l); if (lData<l||(maxLength-=lData)==0) return lData;
			buf=(byte*)buf+lData; offset+=lData;
		}
		if (offset<eshift+lNew) {
			size_t sht=size_t(offset-eshift),l=size_t(lNew-sht); if (l>maxLength) l=maxLength;
			memcpy(buf,str+sht,l); lData+=l; if ((maxLength-=l)==0) return lData; 
			buf=(byte*)buf+l; offset+=l;
		}
		return stream->readChunk(offset,buf,maxLength)+lData;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"StreamEdit::readChunk: exception reading provided stream\n");}
	return 0;
}

IStream *StreamEdit::clone() const
{
	Session *ses=Session::getSession(); if (ses==NULL) return NULL;
	byte *s=NULL; 
	if (str!=NULL) {
		if ((s=(byte*)ses->malloc(lNew))==NULL) return NULL;
		memcpy(s,str,lNew);
	}
	IStream *sm=stream->clone(); if (sm==NULL) {ses->free(s); return NULL;}
	StreamEdit *se=new(ses) StreamEdit(sm,eshift,lOld,lNew,s,ses);
	if (se==NULL) {ses->free(s); sm->destroy();}
	return se;
}

RC StreamEdit::reset()
{
	epos=opos=0; return stream->reset();
}

void StreamEdit::destroy()
{
	MemAlloc *ma=allc; this->~StreamEdit(); if (ma!=NULL) ma->free(this);
}

//---------------------------------------------------------------------------------------------------------

ValueType StreamBuf::dataType() const
{
	return type;
}

uint64_t StreamBuf::length() const
{
	return len;
}

size_t StreamBuf::read(void *bf,size_t maxLength)
{
	if (shift>=len) return 0;
	size_t l=len-shift; if (l>maxLength) l=maxLength;
	memcpy(bf,buf+shift,l); shift+=l;
	return l;
}

size_t StreamBuf::readChunk(uint64_t offset,void *bf,size_t length)
{
	if (offset>=uint64_t(len)) return 0;
	size_t l=len-size_t(offset); if (l>length) l=length;
	memcpy(bf,buf+size_t(offset),l);
	return l;
}

IStream	*StreamBuf::clone() const
{
	Session *ses=Session::getSession(); return ses!=NULL?new(ses) StreamBuf(buf,len,type,ses):(StreamBuf*)0;
}

RC StreamBuf::reset()
{
	shift=0; return RC_OK;
}

void StreamBuf::destroy()
{
	if (allc!=NULL) {if (fFree) allc->free((byte*)buf); allc->free((void*)this);}
}

//--------------------------------------------------------------------------------------------------------

RC ECB::get(GO_DIR whence,ElementID ei,bool fRead)
{
	RC rc=RC_OK;
	if (ei!=STORE_COLLECTION_ID && ei!=eid && (whence==GO_NEXT||whence==GO_PREVIOUS)
		&& (rc=get(GO_FINDBYID,ei,fRead))!=RC_OK) return rc;
	switch (whence) {
	case GO_NEXT:
		if (eid==STORE_COLLECTION_ID) return RC_INVPARAM; else if (eid==coll->lastID) return RC_NOTFOUND;
		if ((ei=nextID)!=STORE_COLLECTION_ID) pos=~0ul;
		else {
			if (pb.isNull()) {
				rc=RC_NOTFOUND;
				if (cPage!=INVALID_PAGEID) {
					// check deletes on heap page!!!
					pb=ctx->bufMgr->getPage(cPage,ctx->trpgMgr,fRead?0:PGCTL_XLOCK);
					cPage=INVALID_PAGEID; SearchKey key((uint64_t)eid);
					if (!pb.isNull() && (tp=(const TreePageMgr::TreePage*)pb->getPageBuf())->findKey(key,pos)) rc=RC_OK;
				}
				if (rc!=RC_OK && (rc=get(GO_FINDBYID,eid,fRead))!=RC_OK) return rc;
			}
			if (pos+1<tp->info.nSearchKeys) ++pos;
			else {
				do {
					if (tp->info.sibling==INVALID_PAGEID) return RC_NOTFOUND;
					pb=ctx->bufMgr->getPage(tp->info.sibling,ctx->trpgMgr,(fRead?PGCTL_COUPLE:PGCTL_XLOCK|PGCTL_COUPLE)|pb.uforce(),pb);
					if (pb.isNull()) return RC_NOTFOUND; tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
				} while (tp->info.nSearchKeys==0);
				pos=0;
			}
			ei=tp->getKeyU32(ushort(pos));
		}
		break;
	case GO_FIRST:
		if ((ei=coll->firstID)==STORE_COLLECTION_ID) return RC_NOTFOUND;
		pos=~0ul; cPage=INVALID_PAGEID; break;
	case GO_LAST:
		if ((ei=coll->lastID)==STORE_COLLECTION_ID) return RC_NOTFOUND;
		pos=~0ul; cPage=INVALID_PAGEID; break;
	case GO_PREVIOUS:
		if (eid==STORE_COLLECTION_ID) return RC_INVPARAM; else if (eid==coll->firstID) return RC_NOTFOUND;
		if ((ei=prevID)!=STORE_COLLECTION_ID) pos=~0ul;
		else {
			if (pb.isNull()) {
				rc=RC_NOTFOUND;
				if (cPage!=INVALID_PAGEID) {
					// check deletes on heap page!!!
					pb=ctx->bufMgr->getPage(cPage,ctx->trpgMgr,fRead?0:PGCTL_XLOCK);
					cPage=INVALID_PAGEID; SearchKey key((uint64_t)eid);
					if (!pb.isNull() && (tp=(const TreePageMgr::TreePage*)pb->getPageBuf())->findKey(key,pos)) rc=RC_OK;
				}
				if (rc!=RC_OK && (rc=get(GO_FINDBYID,eid,fRead))!=RC_OK) return rc;
			}
			if (pos>0) --pos;
			else if (tp->hdr.pageID==coll->anchor) return RC_NOTFOUND;
			else {
				do {
					if ((rc=getPreviousPage(fRead))!=RC_OK) return rc;
					tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
				} while (tp->info.nSearchKeys==0);
				pos=tp->info.nSearchKeys-1;
			}
			ei=tp->getKeyU32(ushort(pos));
		}
		break;
	case GO_FINDBYID:
		switch (ei) {
		case STORE_COLLECTION_ID: return RC_NOTFOUND;
		case STORE_FIRST_ELEMENT: if ((ei=coll->firstID)==STORE_COLLECTION_ID) return RC_NOTFOUND; break;
		case STORE_LAST_ELEMENT: if ((ei=coll->lastID)==STORE_COLLECTION_ID) return RC_NOTFOUND; break;
		}
		if (ei==eid && !pb.isNull()) return RC_OK;
		pos=~0ul; cPage=INVALID_PAGEID; break;
	}
	if (pb.isNull()) {
		if (cPage!=INVALID_PAGEID) {
			// check deletes on heap page!!!
			pb=ctx->bufMgr->getPage(cPage,ctx->trpgMgr,fRead?0:PGCTL_XLOCK); cPage=INVALID_PAGEID;
		}
		if (pb.isNull()) pos=~0ul;
	}
	if (pos==~0ul) {
		SearchKey key((uint64_t)ei);
		if (pb.isNull() || !tp->findKey(key,pos)) {
			pb.release(); parent.release(); depth=0; rc=fRead?findPage(&key):findPageForUpdate(&key);	// parent->findPage!
			if (rc!=RC_OK) return rc;
			if (!(tp=(const TreePageMgr::TreePage*)pb->getPageBuf())->findKey(key,pos)) return RC_NOTFOUND;
		}
	}
	if ((hdr=(const ElementDataHdr *)tp->findData(pos,lData))==NULL) return RC_NOTFOUND;
	prevID=(hdr->flags&1)!=0?((uint32_t*)(hdr+1))[0]:STORE_COLLECTION_ID;
	nextID=(hdr->flags&2)!=0?((uint32_t*)(hdr+1))[hdr->flags&1]:STORE_COLLECTION_ID;
	eid=ei; return RC_OK;
}

RC ECB::shift(ElementID ei,bool fPrev)
{
	assert(!pb.isNull() && eid!=(fPrev?coll->firstID:coll->lastID)); RC rc;
	if (ei!=STORE_COLLECTION_ID) return get(GO_FINDBYID,ei);
	if (fPrev) for (; pos--==0; pos=tp->info.nSearchKeys==0?0:tp->info.nSearchKeys-1) {
		if ((rc=getPreviousPage(false))!=RC_OK) return rc;
		tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
	} else for (; ++pos>=tp->info.nSearchKeys; pos=0) {
		if (tp->info.sibling==INVALID_PAGEID) return RC_NOTFOUND;
		pb=ctx->bufMgr->getPage(tp->info.sibling,ctx->trpgMgr,pb.uforce()|PGCTL_XLOCK|PGCTL_COUPLE,pb);
		if (!pb.isNull()) tp=(const TreePageMgr::TreePage*)pb->getPageBuf(); else return RC_NOTFOUND;
	}
	if ((hdr=(const ElementDataHdr *)tp->findData(pos,lData))==NULL) return RC_NOTFOUND;
	prevID=(hdr->flags&1)!=0?((uint32_t*)(hdr+1))[0]:STORE_COLLECTION_ID;
	nextID=(hdr->flags&2)!=0?((uint32_t*)(hdr+1))[hdr->flags&1]:STORE_COLLECTION_ID;
	eid=tp->getKeyU32(ushort(pos)); return RC_OK;
}

RC ECB::setLinks(ElementID prev,ElementID next,bool fForce)
{
	RC rc=RC_OK; if (eid==STORE_COLLECTION_ID) return RC_INVPARAM;
	if (!pb.isNull() || (rc=get(GO_FINDBYID,eid))==RC_OK) {
		byte buf[sizeof(ElementDataHdr)+sizeof(uint32_t)*2];
		ushort lOld=sizeof(ElementDataHdr)+hdr->shift,lNew=sizeof(ElementDataHdr); 
		memcpy(buf,hdr,sizeof(ElementDataHdr)); SearchKey k;
		((ElementDataHdr*)buf)->shift=0; ((ElementDataHdr*)buf)->flags&=~3;
		if (prev!=STORE_COLLECTION_ID && (fForce || pos==0 || tp->getKeyU32(ushort(pos-1))!=prev)) {
			*(uint32_t*)(buf+lNew)=prev; ((ElementDataHdr*)buf)->flags|=1;
			lNew+=sizeof(uint32_t); ((ElementDataHdr*)buf)->shift+=sizeof(uint32_t);
		}
		if (next!=STORE_COLLECTION_ID && (fForce || pos+1>=tp->info.nSearchKeys || tp->getKeyU32(ushort(pos+1))!=next)) {
			*(uint32_t*)(buf+lNew)=next; ((ElementDataHdr*)buf)->flags|=2;
			lNew+=sizeof(uint32_t); ((ElementDataHdr*)buf)->shift+=sizeof(uint32_t);
		}
		SearchKey key((uint64_t)eid);
		if ((rc=ctx->trpgMgr->edit(*this,key,buf,lNew,lOld,0))==RC_OK) {
			tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
			if (!tp->findKey(key,pos) || (hdr=(const ElementDataHdr *)tp->findData(pos,lData))==NULL) rc=RC_NOTFOUND;
			else {prevID=prev; nextID=next;}
		}
	}
	return rc;
}

RC ECB::unlink(ElementID& first,ElementID& last,bool fDelete)
{
	if (coll->firstID==eid && coll->lastID==eid) {first=last=STORE_COLLECTION_ID; return RC_OK;}
	if (fDelete && hdr->flags==0 && coll->firstID!=eid && coll->lastID!=eid) return RC_OK;
	ElementID ei=eid,prev=prevID,next=nextID; bool fSetPrev=false; RC rc=RC_OK;
	if (coll->firstID!=ei && (rc=get(GO_PREVIOUS))==RC_OK) {
		prev=eid; if (ei==coll->lastID) last=eid;
		if (next==STORE_COLLECTION_ID && ei!=coll->lastID) fSetPrev=true; else rc=setLinks(prevID,next);
	}
	if (rc==RC_OK && coll->lastID!=ei && (rc=get(GO_NEXT,ei))==RC_OK) {
		next=eid; if (ei==coll->firstID) first=eid;
		if (prevID!=STORE_COLLECTION_ID || ei!=coll->firstID) rc=setLinks(prev,nextID);
	}
	if (rc==RC_OK && fSetPrev && (rc=get(GO_FINDBYID,prev))==RC_OK)
		rc=setLinks(prevID,next);
	if (fDelete && rc==RC_OK && (eid!=ei || pb.isNull())) rc=get(GO_FINDBYID,ei);
	return rc==RC_NOTFOUND?RC_CORRUPTED:rc;
}

RC ECB::prepare(ElementID newEid)
{
	SearchKey key((uint64_t)newEid);
	if (tp->findKey(key,pos)) return RC_ALREADYEXISTS;
	if (pos==0 && tp->hdr.pageID!=coll->anchor || tp->checkSibling(key)) {
		pb.release(); RC rc=findPageForUpdate(&key,true); if (rc!=RC_OK) return rc;
		tp=(const TreePageMgr::TreePage*)pb->getPageBuf();
		if (tp->findKey(key,pos)) return RC_ALREADYEXISTS;
	}
	bool fBefore=pos==0;
	if (tp->info.nSearchKeys>0 &&
		(hdr=(const ElementDataHdr *)tp->findData(fBefore?0:pos-1,lData))!=NULL &&
		(hdr->flags&(fBefore?1:2))==0 && 
		(eid=tp->getKeyU32(ushort(fBefore?0:pos-1)))!=(fBefore?coll->firstID:coll->lastID)) {
		ElementID saved=eid; RC rc; if (!fBefore) --pos;
		prevID=(hdr->flags&1)!=0?((uint32_t*)(hdr+1))[0]:STORE_COLLECTION_ID;
		nextID=(hdr->flags&2)!=0?((uint32_t*)(hdr+1))[hdr->flags&1]:STORE_COLLECTION_ID;
		if ((rc=get(fBefore?GO_PREVIOUS:GO_NEXT))!=RC_OK ||
			(rc=setLinks(fBefore?prevID:saved,fBefore?saved:nextID,true))!=RC_OK) return rc;
		saved=eid;
		if ((rc=get(fBefore?GO_NEXT:GO_PREVIOUS))!=RC_OK ||
			(rc=setLinks(fBefore?saved:prevID,fBefore?nextID:saved,true))!=RC_OK) return rc;
		// check the page for insert is still ok
	}
	return RC_OK;
}

//--------------------------------------------------------------------------------------------------------

Navigator::Navigator(const PageAddr& addr,PropertyID pid,const HeapPageMgr::HeapExtCollection *c,ulong md,MemAlloc *ma)
	: Tree(StoreCtx::get()),heapAddr(addr),propID(pid),mode(md),allc(ma),ecb(ctx,*this,NULL),type(VT_ANY)
{
	curValue.type=VT_ERROR; curValue.length=0; curValue.flags=0; ulong len=HeapPageMgr::collDescrSize(c);
	if (len!=0) {
		byte *p=(byte*)(ma!=NULL?ma->malloc(len):malloc(len,SES_HEAP));
		if (p!=NULL) {memcpy(p,c,len); ecb.coll=(HeapPageMgr::HeapExtCollection*)p;}
	}
}

Navigator::~Navigator()
{
	freeV(curValue); allc!=NULL?allc->free((byte*)ecb.coll):free((byte*)ecb.coll,SES_HEAP);
}

const Value *Navigator::navigate(GO_DIR op,ElementID ei)
{
	Value w,*res=NULL; bool fRelease=(mode&LOAD_ENAV)!=0;
	if (op==GO_FINDBYID && ei==STORE_COLLECTION_ID) fRelease=true;
	else if (ecb.get(op,op==GO_FINDBYID?ei:STORE_COLLECTION_ID,true)==RC_OK && 
			ctx->queryMgr->loadS(w,ecb.hdr->type,ecb.hdr->type.isCompact()?*(ushort*)((byte*)(ecb.hdr+1)+ecb.hdr->shift):
			ushort((byte*)(ecb.hdr+1)-(byte*)ecb.tp)+ecb.hdr->shift,(const HeapPageMgr::HeapPage*)ecb.tp,mode|LOAD_SSV,allc)==RC_OK) {
		freeV(curValue); curValue=w; curValue.property=STORE_INVALID_PROPID; curValue.eid=ecb.eid; res=&curValue;
		if (type!=VT_ANY && curValue.type!=type && convV(curValue,curValue,type,allc)!=RC_OK) res=NULL;
	}
	if (fRelease) ecb.release();
	return res;
}

RC Navigator::getPageAddr(const ElementDataHdr *hdr,PageAddr& addr)
{
	if (!hdr->type.isLOB()||hdr->type.isCompact()) return RC_FALSE;
	const byte *pData=(const byte*)(hdr+1)+hdr->shift;
	if (hdr->type.getFormat()==HDF_LONG) {
		memcpy(&addr.pageID,&((HLOB*)pData)->ref.pageID,sizeof(PageID)); addr.idx=((HLOB*)pData)->ref.idx;
	} else {
		HRefSSV href; memcpy(&href,pData,sizeof(HRefSSV)); addr.pageID=href.pageID; addr.idx=href.idx;
	}
	return RC_OK;
}

ElementID Navigator::getCurrentID()
{
	return ecb.eid;
}

const Value *Navigator::getCurrentValue()
{
	return &curValue;
}

RC Navigator::getElementByID(ElementID eid,Value& v)
{
	ECB ecb2(ctx,*this,ecb.coll); RC rc=ecb2.get(GO_FINDBYID,eid,true); if (rc!=RC_OK) return rc;
	if ((rc=ctx->queryMgr->loadS(v,ecb2.hdr->type,ushort((byte*)ecb2.hdr-(byte*)ecb2.tp)+ecb2.hdr->shift+sizeof(ElementDataHdr),
		(const HeapPageMgr::HeapPage*)ecb2.tp,mode|LOAD_SSV,allc))==RC_OK) {v.property=STORE_INVALID_PROPID; v.eid=ecb2.eid;}
	else if (type!=VT_ANY && v.type!=type) rc=convV(v,v,type,allc);
	return rc;
}

INav *Navigator::clone() const
{
	MemAlloc *ma=allc!=NULL?allc:(MemAlloc*)Session::getSession(); return new(ma) Navigator(heapAddr,propID,ecb.coll,mode,ma);
}

unsigned long Navigator::count() const
{
	return ecb.coll!=NULL && ecb.coll->firstID!=STORE_COLLECTION_ID ? ecb.coll->nElements : 0;
}

void Navigator::destroy()
{
	MemAlloc *ma=allc; this->~Navigator(); if (ma!=NULL) ma->free((void*)this);
}

void Navigator::destroyObj()
{
	destroy();
}

namespace MVStoreKernel
{
	class NavTxDelete : public TxDelete, public TreeFreeData
	{
		friend	class	Navigator;
		StoreCtx	*const ctx;
		ulong		nPages;
		PageID		pages[1];
		void		*operator new(size_t s,ulong np) throw() {assert(np>0); return malloc(s+int(np-1)*sizeof(PageID),SES_HEAP);}
		NavTxDelete(StoreCtx *ct,const HeapPageMgr::HeapExtCollection *coll) : ctx(ct),nPages(coll->nPages+1)
			{pages[0]=coll->leftmost; for (ulong i=0; i<coll->nPages; i++) pages[i+1]=coll->pages[i].page;}
		RC deleteData() {RC rc=RC_OK; for (ulong i=0; rc==RC_OK && i<nPages; i++) rc=Tree::drop(pages[i],ctx,this); return rc;}
		RC freeData(const byte *data,StoreCtx *ctx) {
			const ElementDataHdr *hdr=(const ElementDataHdr*)data;
			if (!hdr->type.isLOB() || hdr->type.isCompact()) return RC_OK;
			const byte *pData=(const byte*)(hdr+1)+hdr->shift; PageAddr addr;
			if (hdr->type.getFormat()==HDF_LONG) {
				memcpy(&addr.pageID,&((HLOB*)pData)->ref.pageID,sizeof(PageID)); addr.idx=((HLOB*)pData)->ref.idx;
			} else {
				HRefSSV href; memcpy(&href,pData,sizeof(HRefSSV)); addr.pageID=href.pageID; addr.idx=href.idx;
			}
			return ctx->queryMgr->deleteData(addr);
		}
		void release() {
			// ???
		}
	};
};

RC Navigator::deleteData(Session *ses)
{
	if (ses==NULL) return RC_NOSESSION; if (ses->inReadTx()) return RC_INTERNAL;
	NavTxDelete *td=new(ecb.coll->nPages+1) NavTxDelete(ctx,ecb.coll);
	if (td==NULL) return RC_NORESOURCES;
	ses->addTxDelete(td); return RC_OK;
}

TreeFactory *Navigator::getFactory() const
{
	return NULL;
}

IndexFormat Navigator::indexFormat() const
{
	return Collection::collFormat;
}

PageID Navigator::startPage(const SearchKey *key,int& level,bool fRead,bool fBefore)
{
	PageID pid=ecb.coll->leftmost; assert(fRead); level=ecb.coll->level;
	if (ecb.coll->nPages>0) {
		if (key==NULL) {
			if (fBefore) pid=ecb.coll->pages[ecb.coll->nPages-1].page;
		} else if (key->v.u>=ecb.coll->pages[0].key) {
			const HeapPageMgr::HeapExtCollPage *cp=ecb.coll->pages,*pp; ulong n=ecb.coll->nPages;
			do {
				ulong k=n>>1; pp=&cp[k];
				if (pp->key==key->v.u) return !fBefore?pp->page:k>0?cp[k-1].page:ecb.coll->leftmost;
				if (pp->key>key->v.u) n=k; else {pid=pp->page; cp=pp+1; n-=k+1;}
			} while (n>0);
		}
	}
	return pid;
}

PageID Navigator::prevStartPage(PageID pid)
{
	if (pid!=ecb.coll->leftmost && ecb.coll->nPages>0) {
		if (pid==ecb.coll->pages[0].page) return ecb.coll->leftmost;
		for (ulong i=1; i<ecb.coll->nPages; i++) if (pid==ecb.coll->pages[i].page) return ecb.coll->pages[i-1].page;
	}
	return INVALID_PAGEID;
}

RC Navigator::addRootPage(const SearchKey& key,PageID& pageID,ulong level)
{
	assert(0); return RC_OK;
}

RC Navigator::removeRootPage(PageID pageID,PageID leftmost,ulong level)
{
	assert(0); return RC_OK;
}

ulong Navigator::getStamp(TREE_NODETYPE) const
{
	return 0;
}

void Navigator::getStamps(ulong stamps[TREE_NODETYPE_ALL]) const
{
	memset(stamps,0,TREE_NODETYPE_ALL*sizeof(ulong));
}

void Navigator::advanceStamp(TREE_NODETYPE)
{
	assert(0);
}

bool Navigator::lock(RW_LockType lt,bool fTry) const
{
	//...
	return true;
}

void Navigator::unlock() const
{
	//...
}

TreeConnect *Navigator::persist(uint32_t& hndl) const
{
#if 1
	return NULL;
#else
	assert(ecb.coll!=NULL);
	ulong len=HeapPageMgr::collDescrSize(ecb.coll); 
	byte *p=(byte*)malloc(len,STORE_HEAP); if (p==NULL) return NULL; 
	memcpy(p,ecb.coll,len); 
	return new(STORE_HEAP) Collection(StoreCtx::get(),0,(HeapPageMgr::HeapExtCollection*)p,0,STORE_HEAP);	// stamp???
#endif
}

//--------------------------------------------------------------------------------------------------------

const IndexFormat Collection::collFormat(KT_UINT,sizeof(uint64_t),KT_VARDATA); 

Collection::Collection(StoreCtx *ct,ulong stmp,const HeapPageMgr::HeapExtCollection *c,MemAlloc *ma,ulong xP)
: Tree(ct),maxPages(xP),allc(ma),coll(ma!=NULL?HeapPageMgr::copyDescr(c,ma):(HeapPageMgr::HeapExtCollection*)c),stamp(stmp),fMod(false)
{
}

Collection::~Collection()
{
	if (allc!=NULL) allc->free(coll);
}

TreeFactory *Collection::getFactory() const
{
	return NULL;	//&CollFactory::factory;	// untill fully implemented
}

IndexFormat Collection::indexFormat() const 
{
	return collFormat;
}

ulong Collection::getMode() const
{
	return TF_WITHDEL|TF_SPLITINTX;
}

Collection *Collection::create(const PageAddr& ad,PropertyID propID,StoreCtx *ctx,MemAlloc *ma,PBlock *pb)
{
	bool fRelease=false; const HeapPageMgr::HeapPIN *hpin=NULL; assert(ma!=NULL);
	for (PageAddr addr=ad;;) {
		if (pb==NULL || pb->getPageID()!=addr.pageID) {
			if ((pb=ctx->bufMgr->getPage(addr.pageID,ctx->heapMgr,0,fRelease?pb:NULL))==NULL) 
				return NULL;
			fRelease=true;
		}
		const HeapPageMgr::HeapPage *hp=(const HeapPageMgr::HeapPage*)pb->getPageBuf();
		hpin=(const HeapPageMgr::HeapPIN *)hp->getObject(hp->getOffset(addr.idx));
		if (hpin==NULL) {if (fRelease) pb->release(); return NULL;}
		if (hpin->hdr.getType()==HO_PIN) break;
		if (hpin->hdr.getType()!=HO_FORWARD) {if (fRelease) pb->release(); return NULL;}
		memcpy(&addr,((HeapPageMgr::HeapObjHeader*)hpin)+1,PageAddrSize);
	}
	const HeapPageMgr::HeapV *hprop=hpin->findProperty(propID);
	if (hprop==NULL || !hprop->type.isCollection() || hprop->type.getFormat()!=HDF_LONG)
		{if (fRelease) pb->release(); return NULL;}
	Collection *pc=new(ma) Collection(ctx,hpin->getStamp(),(const HeapPageMgr::HeapExtCollection*)(pb->getPageBuf()+hprop->offset),ma);
	if (fRelease) pb->release(); return pc!=NULL&&pc->coll!=NULL?pc:(delete pc,(Collection*)0);
}

RC Collection::modify(ExprOp op,const Value *pv,ElementID epos,ElementID eid,Session *ses)
{
	if (coll==NULL) return RC_NOTFOUND; if (ses==NULL) return RC_NOSESSION; if (!ses->inWriteTx()) return RC_READTX;
	ushort lval; byte *buf=NULL; size_t lbuf=0; size_t threshold=ctx->trpgMgr->contentSize()/8;
	ElementID first=coll->firstID,last=coll->lastID,prev,next; RC rc=RC_OK;
	if (first==STORE_COLLECTION_ID) {
		if (op!=OP_ADD && op!=OP_ADD_BEFORE && op!=OP_SET) return RC_NOTFOUND;
		if (eid==STORE_COLLECTION_ID || eid==STORE_LAST_ELEMENT || eid==STORE_FIRST_ELEMENT) return RC_INVPARAM;
		SearchKey key((uint64_t)eid); assert(coll->nElements==0);
		if ((rc=persistElement(ses,*pv,lval,buf,lbuf,threshold,STORE_COLLECTION_ID,STORE_COLLECTION_ID))==RC_OK
			&& (rc=insert(key,buf,lval))==RC_OK) first=last=eid;
	} else {
		ECB ecb(ctx,*this,coll);
		if ((rc=ecb.get(GO_FINDBYID,epos))==RC_NOTFOUND && (op==OP_ADD || op==OP_SET))
			{rc=ecb.get(GO_LAST); op=OP_ADD;}
		SearchKey key((uint64_t)ecb.eid),k2; epos=ecb.eid;
		if (rc==RC_OK) switch (op) {
		default: return RC_INVPARAM;
		case OP_SET:
			assert(ecb.hdr!=NULL);
			if ((rc=persistElement(ses,*pv,lval,buf,lbuf,threshold,ecb.prevID,ecb.nextID))==RC_OK)
				rc=ctx->trpgMgr->update(ecb,key,(byte*)ecb.hdr,ecb.lData,buf,lval);
			break;
		case OP_ADD:
			prev=STORE_COLLECTION_ID; next=ecb.nextID;
			if (eid==STORE_COLLECTION_ID || eid==STORE_LAST_ELEMENT || eid==STORE_FIRST_ELEMENT) return RC_INVPARAM;
			if (eid<epos || epos+1!=eid && (ecb.pos+1>=ecb.tp->info.nEntries || ecb.tp->getKeyU32(ushort(ecb.pos+1))<eid)) {
				prev=epos;
				if (next==STORE_COLLECTION_ID && epos!=coll->lastID) {
					if (ecb.pos+1<ecb.tp->info.nSearchKeys) next=ecb.tp->getKeyU32(ushort(ecb.pos+1));
					else if ((rc=ecb.get(GO_NEXT))!=RC_OK) break;
					else {next=ecb.eid; if ((rc=ecb.get(GO_PREVIOUS))!=RC_OK) break;}
				}
				if ((rc=ecb.setLinks(ecb.prevID,eid))==RC_OK) rc=ecb.prepare(eid);
			} else if (next!=STORE_COLLECTION_ID) rc=ecb.setLinks(ecb.prevID,STORE_COLLECTION_ID);
			if (rc==RC_OK && (rc=persistElement(ses,*pv,lval,buf,lbuf,threshold,prev,next))==RC_OK) {
				key.v.u=eid; rc=ctx->trpgMgr->insert(ecb,key,buf,lval); ecb.tp=(const TreePageMgr::TreePage*)ecb.pb->getPageBuf();
				if (rc==RC_OK && (next==STORE_COLLECTION_ID || (rc=ecb.get(GO_FINDBYID,next))==RC_OK
					&& (rc=ecb.setLinks(eid,ecb.nextID))==RC_OK) && epos==last) last=eid;
			}
			break;
		case OP_ADD_BEFORE:
			prev=ecb.prevID; next=STORE_COLLECTION_ID;
			if (eid==STORE_COLLECTION_ID || eid==STORE_LAST_ELEMENT || eid==STORE_FIRST_ELEMENT) return RC_INVPARAM;
			if (eid>epos || epos!=eid+1 && (ecb.pos==0 || ecb.tp->getKeyU32(ushort(ecb.pos-1))>eid)) {
				next=epos;
				if (prev==STORE_COLLECTION_ID && epos!=coll->firstID) {
					if (ecb.pos!=0) prev=ecb.tp->getKeyU32(ushort(ecb.pos-1));
					else if ((rc=ecb.get(GO_PREVIOUS))!=RC_OK) break;
					else {prev=ecb.eid; if ((rc=ecb.get(GO_NEXT))!=RC_OK) break;}
				}
				if ((rc=ecb.setLinks(eid,ecb.nextID))==RC_OK) rc=ecb.prepare(eid);
			} else if (prev!=STORE_COLLECTION_ID) rc=ecb.setLinks(STORE_COLLECTION_ID,ecb.nextID);
			if (rc==RC_OK && (rc=persistElement(ses,*pv,lval,buf,lbuf,threshold,prev,next))==RC_OK) {
				key.v.u=eid; rc=ctx->trpgMgr->insert(ecb,key,buf,lval); ecb.tp=(const TreePageMgr::TreePage*)ecb.pb->getPageBuf();
				if (rc==RC_OK && (prev==STORE_COLLECTION_ID || (rc=ecb.get(GO_FINDBYID,prev))==RC_OK
					&& (rc=ecb.setLinks(ecb.prevID,eid))==RC_OK) && epos==first) first=eid;
			}
			break;
		case OP_DELETE:
			assert(ecb.hdr!=NULL);
			if (ecb.hdr->type.isLOB() && !ecb.hdr->type.isCompact()) {
				const byte *pData=(const byte*)(ecb.hdr+1)+ecb.hdr->shift; PageAddr addr;
				if (ecb.hdr->type.getFormat()==HDF_LONG) {
					memcpy(&addr.pageID,&((HLOB*)pData)->ref.pageID,sizeof(PageID)); addr.idx=((HLOB*)pData)->ref.idx;
				} else {
					HRefSSV href; memcpy(&href,pData,sizeof(HRefSSV)); addr.pageID=href.pageID; addr.idx=href.idx;
				}
				if ((rc=ctx->queryMgr->deleteData(addr))!=RC_OK) break;
			}
			if ((rc=ecb.unlink(first,last,true))==RC_OK) {
				switch (rc=ctx->trpgMgr->remove(ecb,key,NULL,0)) {
				default: break;
				case RC_TRUE:
					// merge !!!
					rc=RC_OK;
				case RC_OK: coll->nElements--; fMod=true; break;
				}
			}
			break;
		case OP_EDIT:
			assert(ecb.hdr!=NULL);
			if (isString(ecb.hdr->type.getType())) {
				Value v;
				if ((rc=ctx->queryMgr->loadS(v,ecb.hdr->type,ecb.hdr->type.isCompact()?*(ushort*)((byte*)(ecb.hdr+1)+ecb.hdr->shift):
					ushort((byte*)(ecb.hdr+1)-(byte*)ecb.tp)+ecb.hdr->shift,(const HeapPageMgr::HeapPage*)ecb.tp,0,ses))!=RC_OK) break;
				if ((rc=Expr::calc(OP_EDIT,v,pv,2,0,ses))==RC_OK && (rc=persistElement(ses,v,lval,buf,lbuf,threshold,ecb.prevID,ecb.nextID))==RC_OK)
					rc=ctx->trpgMgr->update(ecb,key,(byte*)ecb.hdr,ecb.lData,buf,lval);
			} else if (ecb.hdr->type.isLOB()) {
				const byte *pData=(const byte*)(ecb.hdr+1)+ecb.hdr->shift; HLOB data;
				if (ecb.hdr->type.getFormat()==HDF_LONG) memcpy(&data,pData,sizeof(HLOB));
				else {memcpy(&data.ref,pData,sizeof(HRefSSV)); data.len=~0ULL;}
				if (!isString(data.ref.type.getType())) rc=RC_TYPE;
				else {
					PageAddr addr={data.ref.pageID,data.ref.idx};
					uint64_t oldl=data.len; ushort lmod;
					switch (rc=ctx->queryMgr->editData(ses,addr,data.len,*pv)) {
					default: break;
					case RC_OK:
						if (ecb.hdr->type.getFormat()==HDF_LONG && data.len!=oldl)
							rc=ctx->trpgMgr->edit(ecb,key,&data.len,sizeof(uint64_t),sizeof(uint64_t),
											sizeof(ElementDataHdr)+ecb.hdr->shift+sizeof(HRefSSV));
						break;
					case RC_TRUE:
						assert(data.len!=~0ULL && ecb.hdr->shift<=sizeof(uint32_t)*2 && ecb.hdr->type.getFormat()!=HDF_LONG);
						{byte obuf[sizeof(ElementDataHdr)+sizeof(uint32_t)*2+sizeof(HRefSSV)];
						byte nbuf[sizeof(ElementDataHdr)+sizeof(uint32_t)*2+sizeof(HLOB)];
						memcpy(obuf,ecb.hdr,sizeof(ElementDataHdr)+ecb.hdr->shift);
						memcpy(obuf+sizeof(ElementDataHdr)+ecb.hdr->shift,&data.ref,sizeof(HRefSSV));
						data.ref.pageID=addr.pageID; data.ref.idx=addr.idx;
						memcpy(nbuf,ecb.hdr,sizeof(ElementDataHdr)+ecb.hdr->shift);
						memcpy(nbuf+sizeof(ElementDataHdr)+ecb.hdr->shift,&data,sizeof(HLOB));
						((ElementDataHdr*)nbuf)->type.setType(ecb.hdr->type.getType(),HDF_LONG);
						rc=ctx->trpgMgr->update(ecb,key,obuf,sizeof(ElementDataHdr)+ecb.hdr->shift+sizeof(HRefSSV),
														nbuf,sizeof(ElementDataHdr)+ecb.hdr->shift+sizeof(HLOB));}
						break;
					case RC_FALSE:
						data.ref.pageID=addr.pageID; data.ref.idx=addr.idx;
						lmod=ecb.hdr->type.getFormat()!=HDF_LONG?sizeof(HRefSSV):sizeof(HLOB);
						rc=ctx->trpgMgr->edit(ecb,key,&data,lmod,lmod,sizeof(ElementDataHdr)+ecb.hdr->shift);
						break;
					}
				}
			} else return RC_TYPE;
			break;
		case OP_MOVE:
			assert(pv->type==VT_UINT);
			if (pv->ui==epos || pv->ui==ecb.prevID || pv->ui==STORE_LAST_ELEMENT && epos==coll->lastID ||
				pv->ui==STORE_FIRST_ELEMENT && (epos==coll->firstID || ecb.prevID==coll->firstID)) break;
			if ((rc=ecb.unlink(first,last))!=RC_OK || (rc=ecb.get(GO_FINDBYID,pv->ui))!=RC_OK) break;
			prev=ecb.eid; next=ecb.nextID; if ((rc=ecb.setLinks(ecb.prevID,epos))!=RC_OK) break;
			if (ecb.eid==coll->lastID || ecb.eid==last) {last=epos; assert(next==STORE_COLLECTION_ID);}
			else if ((rc=ecb.shift(next))!=RC_OK || (next=ecb.eid,rc=ecb.setLinks(epos,ecb.nextID))!=RC_OK) break;
			if (ecb.eid==epos && !ecb.pb.isNull() || (rc=ecb.get(GO_FINDBYID,epos))==RC_OK)
				rc=ecb.setLinks(prev,next);
			break;
		case OP_MOVE_BEFORE:
			assert(pv->type==VT_UINT);
			if (pv->ui==epos || pv->ui==ecb.nextID || pv->ui==STORE_FIRST_ELEMENT && epos==coll->firstID ||
				pv->ui==STORE_LAST_ELEMENT && (epos==coll->lastID || ecb.nextID==coll->lastID)) break;
			if ((rc=ecb.unlink(first,last))!=RC_OK || (rc=ecb.get(GO_FINDBYID,pv->ui))!=RC_OK) break;
			prev=ecb.prevID; next=ecb.eid; if ((rc=ecb.setLinks(epos,ecb.nextID))!=RC_OK) break;
			if (ecb.eid==coll->firstID || ecb.eid==first) {first=epos; assert(prev==STORE_COLLECTION_ID);}
			else if ((rc=ecb.shift(prev,true))!=RC_OK) break;
			else {prev=ecb.eid; if ((rc=ecb.setLinks(ecb.prevID,epos))!=RC_OK) break;}
			if (ecb.eid==epos && !ecb.pb.isNull() || (rc=ecb.get(GO_FINDBYID,epos))==RC_OK)
				rc=ecb.setLinks(prev,next);
			break;
		}
	}
	if (buf!=NULL) ses->free(buf);
	if (rc==RC_OK) {
		if (first!=coll->firstID || last!=coll->lastID) {coll->firstID=first; coll->lastID=last; fMod=true;}
		if (op==OP_ADD || op==OP_ADD_BEFORE) {
			coll->nElements++; fMod=true; 
			if (ctx->getPrefix()==(eid&CPREFIX_MASK) && eid>=coll->keygen) coll->keygen=eid+1;
		}
		// log !!!
	}
	return rc;
}

PageID Collection::startPage(const SearchKey *key,int& level,bool fRead,bool fBefore)
{
	if (coll==NULL) return INVALID_PAGEID;
	PageID pid=coll->leftmost; level=coll->level; assert(key==NULL || key->type==KT_UINT);
	if (pid==INVALID_PAGEID && !fRead) {
		PBlock *pb=ctx->fsMgr->getNewPage(ctx->trpgMgr);
		if (pb!=NULL) {
			if (ctx->trpgMgr->initPage(pb,Collection::collFormat,0,NULL,INVALID_PAGEID,INVALID_PAGEID)!=RC_OK) 
				pb->release();
			else 
				{pid=coll->leftmost=coll->anchor=pb->getPageID(); pb->release(); fMod=true;}
		}
	} else if (coll->nPages>0) {
		if (key==NULL) {if (fBefore) pid=coll->pages[coll->nPages-1].page;}
		else if (key->v.u>=coll->pages[0].key) {
			const HeapPageMgr::HeapExtCollPage *cp=coll->pages,*pp; ulong n=coll->nPages;
			do {
				ulong k=n>>1; pp=&cp[k]; 
				if (pp->key==key->v.u) return !fBefore?pp->page:k>0?cp[k-1].page:coll->leftmost;
				if (pp->key>key->v.u) n=k; else {pid=pp->page; cp=pp+1; n-=k+1;}
			} while (n>0);
		}
	}
	return pid;
}

PageID Collection::prevStartPage(PageID pid)
{
	if (pid!=coll->leftmost && coll->nPages>0) {
		if (pid==coll->pages[0].page) return coll->leftmost;
		for (ulong i=1; i<coll->nPages; i++) if (pid==coll->pages[i].page) return coll->pages[i-1].page;
	}
	return INVALID_PAGEID;
}

RC Collection::addRootPage(const SearchKey& key,PageID& pageID,ulong level) 
{
	if (coll==NULL) return RC_NOTFOUND;
	RC rc=RC_OK; PageID newRoot; PBlock *pb;
	if (coll->leftmost==INVALID_PAGEID) {coll->anchor=coll->leftmost=pageID; fMod=true;}
	else if (level<coll->level) {
		// re-traverse from root etc.
		rc=RC_INTERNAL;
	} else if (coll->nPages<maxPages && allc!=NULL) {
		if (coll->nPages!=0 && (coll=(HeapPageMgr::HeapExtCollection*)allc->realloc(coll,
			coll->nPages*sizeof(HeapPageMgr::HeapExtCollPage)+sizeof(HeapPageMgr::HeapExtCollection)))==NULL)
				rc=RC_NORESOURCES;
		else {
			ulong idx=0,n=coll->nPages;
			if (n>0) {
				for (;;) {
					ulong k=n>>1; const HeapPageMgr::HeapExtCollPage *cp=&coll->pages[idx+k]; 
					if (cp->key==key.v.u) {idx+=k; break;}
					if (cp->key>key.v.u) {if ((n=k)==0) break;} else {idx+=k+1; if ((n-=k+1)==0) break;}
				}
				if (idx<coll->nPages) memmove(&coll->pages[idx+1],&coll->pages[idx],(coll->nPages-idx)*sizeof(HeapPageMgr::HeapExtCollPage));
			}
			coll->pages[idx].key=(uint32_t)key.v.u; coll->pages[idx].page=pageID; coll->nPages++; fMod=true; rc=RC_FALSE;
		}
	} else if (!Session::getSession()->inWriteTx()) rc=RC_INTERNAL;
	else if ((rc=ctx->fsMgr->allocPages(1,&newRoot))==RC_OK) {
		if ((pb=ctx->bufMgr->newPage(newRoot,ctx->trpgMgr))==NULL) rc=RC_NORESOURCES;
		else {
			if (coll->nPages>0) {
				// ???
				rc=RC_INTERNAL;
			} else if ((rc=ctx->trpgMgr->initPage(pb,Collection::collFormat,ushort(coll->level+1),&key,coll->leftmost,pageID))==RC_OK)
				{pageID=coll->leftmost=newRoot; coll->nPages=0; coll->level++; fMod=true;}
			pb->release();
		}
	}
	return rc;
}

RC Collection::removeRootPage(PageID pageID,PageID leftmost,ulong level)
{
	//...
	return RC_OK;
}

ulong Collection::getStamp(TREE_NODETYPE) const
{
	return stamp;
}

void Collection::getStamps(ulong stamps[TREE_NODETYPE_ALL]) const
{
	for (int i=0; i<TREE_NODETYPE_ALL; i++) stamps[i]=stamp;
}

void Collection::advanceStamp(TREE_NODETYPE) 
{
	stamp++; fMod=true;		// ???
}

bool Collection::lock(RW_LockType,bool fTry) const 
{
	return true;
}

void Collection::unlock() const
{
}

void Collection::destroy()
{
	MemAlloc *ma=allc; this->~Collection(); if (ma!=NULL) ma->free(this);
}

namespace MVStoreKernel
{
class InitCollection : public Tree
{
	HeapPageMgr::HeapExtCollection&	coll;
	TreeCtx				tctx;
	const	ulong			maxPages;
	const	bool			fForce;
	const	bool			fOld;
	byte					*buf;
	size_t					lbuf;
	size_t					threshold;
	Session					*const ses;
public:
	InitCollection(HeapPageMgr::HeapExtCollection& cl,ulong mP,Session *ss,bool fF,bool fO)
		: Tree(ss->getStore()),coll(cl),tctx(*this),maxPages(mP),fForce(fF),fOld(fO),buf(NULL),lbuf(0),ses(ss)
			{threshold=ctx->trpgMgr->contentSize()/8;}
	virtual ~InitCollection() {ses->free(buf);}
	RC addRootPage(const SearchKey& key,PageID& pageID,ulong level) {
		assert(pageID!=INVALID_PAGEID && key.type==KT_UINT);
		RC rc=RC_OK; PageID newRoot;
		if (coll.nPages<maxPages) {
			HeapPageMgr::HeapExtCollPage& pg=coll.pages[coll.nPages++]; pg.key=(uint32_t)key.v.u; pg.page=pageID; rc=RC_TRUE;
		} else if (tctx.depth==TREE_MAX_DEPTH) rc=RC_NORESOURCES;
		else if ((rc=ctx->fsMgr->allocPages(1,&newRoot))==RC_OK) {
			PBlockP root(ctx->bufMgr->newPage(newRoot,ctx->trpgMgr));
			if (root.isNull()) rc=RC_FULL;
			else if (maxPages!=0) {
				// ???
				rc=RC_INTERNAL;
			} else
				rc=ctx->trpgMgr->initPage(root,Collection::collFormat,ushort(level+1),&key,coll.leftmost,pageID);
			if (rc==RC_OK) {pageID=coll.leftmost=newRoot; coll.nPages=0; coll.level++;}
		}
		return rc;
	}
	RC persist(const Value& v,ulong idx) {
		if (!fForce) v.eid=coll.keygen++;
		else if (ctx->getPrefix()==(v.eid&CPREFIX_MASK) && v.eid>=coll.keygen) coll.keygen=v.eid+1;
		if (idx==0) coll.firstID=v.eid; coll.lastID=v.eid; SearchKey key((uint64_t)v.eid); ushort lval;
		RC rc=Collection::persistElement(ses,v,lval,buf,lbuf,threshold,STORE_COLLECTION_ID,STORE_COLLECTION_ID,fOld);
		if (rc==RC_OK) {
			if (tctx.pb.isNull()) {
				assert(coll.leftmost=INVALID_PAGEID && tctx.depth==0);
				tctx.pb=ctx->fsMgr->getNewPage(ctx->trpgMgr); if (tctx.pb.isNull()) return RC_FULL;
				if ((rc=ctx->trpgMgr->initPage(tctx.pb,Collection::collFormat,0,NULL,INVALID_PAGEID,INVALID_PAGEID))!=RC_OK)
					{tctx.pb.release(); return rc;}
				coll.anchor=coll.leftmost=tctx.pb->getPageID();
			}
			if ((rc=ctx->trpgMgr->insert(tctx,key,buf,lval))==RC_OK) coll.nElements++;
		}
		return rc;
	}
	TreeFactory *getFactory() const {return NULL;}
	IndexFormat	indexFormat() const {return Collection::collFormat;}
	ulong		getMode() const {return TF_SPLITINTX;}
	PageID		startPage(const SearchKey*,int& level,bool,bool) {level=-1; return INVALID_PAGEID;}
	PageID		prevStartPage(PageID) {return INVALID_PAGEID;}
	RC			removeRootPage(PageID page,PageID leftmost,ulong level) {return RC_INTERNAL;}
	ulong		getStamp(TREE_NODETYPE) const {return 0;}
	void		getStamps(ulong stamps[TREE_NODETYPE_ALL]) const {}
	void		advanceStamp(TREE_NODETYPE) {}
	bool		lock(RW_LockType,bool fTry=false) const {return true;}
	void		unlock() const {}
	void		destroy() {}
	void		release() {tctx.pb.release();}
};
};

RC Collection::persistElement(Session *ses,const Value& v,ushort& lval,byte *&buf,size_t& lbuf,size_t threshold,ulong prev,ulong next,bool fOld)
{
	ushort sht=0,l; lval=sizeof(ElementDataHdr); byte flags=0; StoreCtx *ctx=ses->getStore();
	if (prev!=STORE_COLLECTION_ID) {lval+=sht=sizeof(uint32_t); flags|=1;}
	if (next!=STORE_COLLECTION_ID) {lval+=sizeof(uint32_t); sht+=sizeof(uint32_t); flags|=2;}
	if (v.type==VT_ARRAY || v.type==VT_COLLECTION) return RC_INTERNAL;
	size_t len; RC rc=RC_OK;
	if (fOld && v.type==VT_STREAM) l=(v.flags&VF_SSV)!=0?sizeof(HRefSSV):sizeof(HLOB);
	else if ((rc=ctx->queryMgr->estimateLength(v,len,MODE_PREFIX_READ,threshold,ses))!=RC_OK) return rc;
	else if ((l=(ushort)ceil(len,HP_ALIGN))==0) l=sizeof(ushort); else if (l>threshold) v.flags|=VF_SSV;
	if (buf==NULL||ulong(lval+l)>lbuf) {
		buf=buf==NULL?(byte*)ses->malloc(lbuf=lval+l):(byte*)ses->realloc(buf,lbuf=lval+l);
		if (buf==NULL) return RC_NORESOURCES;
	}
	ElementDataHdr *hdr=(ElementDataHdr*)buf; hdr->flags=flags; hdr->shift=byte(sht); hdr->type.flags=v.meta;
	if ((flags&1)!=0) ((uint32_t*)(hdr+1))[0]=prev; if ((flags&2)!=0) ((uint32_t*)(hdr+1))[flags&1]=next;
	if (fOld && v.type==VT_STREAM) {
		if ((v.flags&VF_SSV)!=0) {memcpy(buf+lval,&v.id,sizeof(HRefSSV)); hdr->type.setType(VT_STREAM,HDF_SHORT);}
		else {
			HLOB hl; hl.len=v.stream.is->length(); hl.ref.type.setType(v.stream.is->dataType(),HDF_NORMAL);
			hl.ref.pageID=((StreamX*)v.stream.is)->getAddr().pageID; hl.ref.idx=((StreamX*)v.stream.is)->getAddr().idx;
			memcpy(buf+lval,&hl,sizeof(HLOB)); hdr->type.setType(VT_STREAM,HDF_LONG);
		}
		lval+=l;
	} else if ((rc=ctx->queryMgr->persistValue(v,lval,hdr->type,*(ushort*)&buf[lval],buf+lval,NULL,PageAddr::invAddr))==RC_OK)
		{if (hdr->type.isCompact()) lval+=sizeof(ushort); else lval=(ushort)ceil(lval,HP_ALIGN);}
	return rc;
}

RC Collection::persist(const Value& v,HeapPageMgr::HeapExtCollection& collection,Session *ses,bool fForce,bool fOld,ulong maxPages)
{
	memset(&collection,0,sizeof(HeapPageMgr::HeapExtCollection));
	collection.anchor=collection.leftmost=collection.pages[0].page=INVALID_PAGEID;
	collection.keygen=ses->getStore()->getPrefix(); collection.indexID=~0u;
	InitCollection init(collection,maxPages,ses,fForce,fOld); 
	ulong k; const Value *cv; ElementID prev=0; RC rc=RC_OK;
	if (ses==NULL) rc=RC_NOSESSION; else if (!ses->inWriteTx()) rc=RC_READTX;
	else switch (v.type) {
	case VT_ARRAY:
		for (k=0; k<v.length; k++) {
			if (fForce) {if (v.varray[k].eid<prev) break; else prev=v.varray[k].eid;}
			if ((rc=init.persist(v.varray[k],k))!=RC_OK) break;
		}
		init.release(); 
		if (rc==RC_OK && k<v.length) for (Collection pcol(ses->getStore(),0,&collection,0,NO_HEAP); k<v.length; k++)
			{const Value *cv=&v.varray[k]; if ((rc=pcol.modify(OP_ADD,cv,prev,cv->eid,ses))!=RC_OK) break; prev=cv->eid;}
		break;
	case VT_COLLECTION:
		for (cv=v.nav->navigate(GO_FIRST),k=0; cv!=NULL; ++k,cv=v.nav->navigate(GO_NEXT))
			{if (fForce) {if (cv->eid<prev) break; else prev=cv->eid;} if ((rc=init.persist(*cv,k))!=RC_OK) break;}
		init.release(); 
		if (rc==RC_OK && cv!=NULL) for (Collection pcol(ses->getStore(),0,&collection,0,NO_HEAP); cv!=NULL; ++k,cv=v.nav->navigate(GO_NEXT))
			{if ((rc=pcol.modify(OP_ADD,cv,prev,cv->eid,ses))!=RC_OK) break; prev=cv->eid;}
		break;
	default:
		if ((rc=init.persist(v,0))==RC_OK) init.release(); 
		break;
	}
	return rc;
}

//----------------------------------------------------------------------------------------------------

CollFactory	CollFactory::factory;

byte CollFactory::getID() const
{
	return MA_HEAPDIRLAST;
}

byte CollFactory::getParamLength() const
{
	return 0;	// ???
}

void CollFactory::getParams(byte *buf,const Tree& tr) const
{
}

RC CollFactory::createTree(const byte *params,byte lparams,Tree *&tree)
{
	return RC_INTERNAL;
}

//-----------------------------------------------------------------------------------------------------

BigMgr::BigMgr(StoreCtx *ct,ulong blobReadTabSize) 
: ctx(ct),blobReadTab(blobReadTabSize)
{
	ct->treeMgr->registerFactory(CollFactory::factory);
}
