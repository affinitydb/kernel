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

#include "maps.h"
#include "session.h"
#include "ftindex.h"
#include "idxcache.h"
#include "classifier.h"
#include "queryprc.h"
#include "lock.h"
#include "stmt.h"
#include "parser.h"
#include "expr.h"
#include "blob.h"

#include <stdio.h>

using namespace AfyKernel;

FullScan::~FullScan()
{
	if (it!=NULL) {
		assert(stx!=NULL);
		if (&it->getPageSet()!=&stx->defHeap) ((PageSet*)&it->getPageSet())->destroy();
		qx->ses->free(it);
	}
}

PBlock *FullScan::init()
{
	PBlockP pb; heapPageID=INVALID_PAGEID; const HeapDirMgr::HeapDirPage *hd; state&=~QST_INIT;
	for (dirPageID=qx->ses->getStore()->theCB->getRoot(fClasses?MA_CLASSDIRFIRST:MA_HEAPDIRFIRST); dirPageID!=INVALID_PAGEID; ) {
		if (pb.getPage(dirPageID,qx->ses->getStore()->hdirMgr,QMGR_SCAN,qx->ses)==NULL) {dirPageID=INVALID_PAGEID; break;}
		hd=(const HeapDirMgr::HeapDirPage*)pb->getPageBuf();
		for (idx=0; idx<hd->nSlots; ) {
			heapPageID=((PageID*)(hd+1))[idx++]; slot=0;
			if (pb.getPage(heapPageID,qx->ses->getStore()->heapMgr,(qflags&QO_FORUPDATE)!=0?PGCTL_ULOCK|QMGR_SCAN|PGCTL_RLATCH:QMGR_SCAN|PGCTL_RLATCH,qx->ses)!=NULL)
				for (const HeapPageMgr::HeapPage *hp=(const HeapPageMgr::HeapPage*)pb->getPageBuf(); slot<hp->nSlots; slot++) {
					if (nSkip==0) {PBlock *ret=pb; pb=NULL; return ret;}
					const HeapPageMgr::HeapPIN *hpin=(const HeapPageMgr::HeapPIN *)hp->getObject(hp->getOffset(PageIdx(slot)));
					if (hpin==NULL) {
						// DELETED???
					} else if (hpin->hdr.getType()==HO_PIN && (hpin->hdr.descr&mask)==mask>>16) --nSkip;
				}
			if (pb.getPage(dirPageID,qx->ses->getStore()->hdirMgr,QMGR_SCAN,qx->ses)==NULL) {dirPageID=INVALID_PAGEID; break;}
			hd=(const HeapDirMgr::HeapDirPage*)pb->getPageBuf();
		}
		dirPageID=hd->next;
	}
	for (; stx!=NULL; stx=stx->next) if ((ulong)stx->defHeap!=0) {
		// copy if not class && topmost stx
		if ((it=new(qx->ses) PageSet::it(stx->defHeap))!=NULL) {
			while ((heapPageID=(++*it))!=INVALID_PAGEID) {
				if (pb.getPage(heapPageID,qx->ses->getStore()->heapMgr,(qflags&QO_FORUPDATE)!=0?PGCTL_ULOCK|QMGR_SCAN|PGCTL_RLATCH:QMGR_SCAN|PGCTL_RLATCH,qx->ses)!=NULL) {
					slot=0;
					for (const HeapPageMgr::HeapPage *hp=(const HeapPageMgr::HeapPage*)pb->getPageBuf(); slot<hp->nSlots; slot++) {
						if (nSkip==0) {PBlock *ret=pb; pb=NULL; return ret;}
						const HeapPageMgr::HeapPIN *hpin=(const HeapPageMgr::HeapPIN *)hp->getObject(hp->getOffset(PageIdx(slot)));
						if (hpin==NULL) {
							// DELETED???
						} else if (hpin->hdr.getType()==HO_PIN && (hpin->hdr.descr&mask)==mask>>16) --nSkip;
					}
				}
			}
			if (&it->getPageSet()!=&stx->defHeap) ((PageSet*)&it->getPageSet())->destroy();
			qx->ses->free(it); it=NULL;
		}
	}
	state|=QST_BOF|QST_EOF; return NULL;
}

RC FullScan::next(const PINEx *)
{
	PBlock *pb=NULL; if ((state&QST_EOF)!=0 || (state&QST_INIT)!=0 && (pb=init())==NULL) return RC_EOF;
	if (res!=NULL) {
		if (pb==NULL && !res->pb.isNull() && res->pb->getPageID()==heapPageID) {pb=res->pb; res->pb=NULL;}
		res->cleanup();  assert(res->ses==qx->ses);
	}
	RC rc; PBlockP pDir; 
	for (const ulong flags=(qflags&QO_FORUPDATE)!=0?PGCTL_ULOCK|QMGR_UFORCE|QMGR_SCAN|PGCTL_RLATCH:QMGR_SCAN|PGCTL_RLATCH;;) {
		if ((rc=qx->ses->testAbortQ())!=RC_OK) {if (pb!=NULL) pb->release((qflags&QO_FORUPDATE)!=0?QMGR_UFORCE:0,qx->ses); return rc;}
		if (heapPageID==INVALID_PAGEID) {
			if (dirPageID==INVALID_PAGEID) {
				if (it!=NULL) {
					if ((heapPageID=(++*it))!=INVALID_PAGEID) continue;
					if (&it->getPageSet()!=&stx->defHeap) ((PageSet*)&it->getPageSet())->destroy();
					qx->ses->free(it); it=NULL; stx=stx->next;
				}
				for (;;stx=stx->next) {
					if (stx==NULL) {state|=QST_EOF; return RC_EOF;}
					if ((ulong)stx->defHeap!=0) break;
				}
				// copy if not class && topmost stx
				if ((it=new(qx->ses) PageSet::it(stx->defHeap))!=NULL) continue;
				state|=QST_EOF; return RC_NORESOURCES;
			}
			if (pDir.isNull()||pDir->getPageID()!=dirPageID) pDir.getPage(dirPageID,qx->ses->getStore()->hdirMgr,QMGR_SCAN,qx->ses);
			if (pDir.isNull()) {dirPageID=INVALID_PAGEID; return RC_CORRUPTED;}
			const HeapDirMgr::HeapDirPage *hd=(const HeapDirMgr::HeapDirPage*)pDir->getPageBuf();
			if (idx>=hd->nSlots) {dirPageID=hd->next; idx=0; continue;}
			heapPageID=((PageID*)(hd+1))[idx++]; pDir.release(qx->ses); slot=0;
		}
		if (pb!=NULL && pb->getPageID()==heapPageID || (pb=qx->ses->getStore()->bufMgr->getPage(heapPageID,qx->ses->getStore()->heapMgr,flags,pb,qx->ses))!=NULL) {
			for (res->addr.pageID=pb->getPageID(); slot<((HeapPageMgr::HeapPage*)pb->getPageBuf())->nSlots; ) {
				res->addr.idx=(PageIdx)slot++; const HeapPageMgr::HeapPage *hp=(HeapPageMgr::HeapPage*)pb->getPageBuf();
				const HeapPageMgr::HeapPIN *hpin=(const HeapPageMgr::HeapPIN *)hp->getObject(hp->getOffset(PageIdx(res->addr.idx)));
				if (hpin==NULL) {
					res->pb=(PBlock*)pb;
					if (qx->ses->getStore()->lockMgr->getTVers(*res,(qflags&QO_FORUPDATE)!=0?TVO_UPD:TVO_READ)==RC_OK && res->tv!=NULL) {
						// check deleted in uncommitted tx
					}
					res->pb=NULL;
				} else if (hpin->hdr.getType()==HO_PIN) {
					if (!hpin->getAddr(const_cast<PID&>(res->id))) {const_cast<PID&>(res->id).pid=res->addr; const_cast<PID&>(res->id).ident=STORE_OWNER;}
					if (!qx->ses->inWriteTx()) {
						// check deleted before/inserted in uncommited
					}
					if ((hpin->hdr.descr&mask)==mask>>16) {res->pb=(PBlock*)pb; res->hpin=hpin; res->epr.flags|=PINEX_ADDRSET; return RC_OK;}
				}
			}
			pb->release((qflags&QO_FORUPDATE)!=0?QMGR_UFORCE:0,qx->ses); pb=NULL;
		}
		heapPageID=INVALID_PAGEID;
	}
}

RC FullScan::rewind()
{
	if (it!=NULL) {if (&it->getPageSet()!=&stx->defHeap) ((PageSet*)&it->getPageSet())->destroy(); qx->ses->free(it);}
	heapPageID=INVALID_PAGEID; dirPageID=qx->ses->getStore()->theCB->getRoot(fClasses?MA_CLASSDIRFIRST:MA_HEAPDIRFIRST); idx=0;
	stx=&qx->ses->tx; state=state&~QST_EOF|QST_BOF; return RC_OK;
}

void FullScan::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("fullscan\n",9);
}

//-----------------------------------------------------------------------------------------------

ClassScan::ClassScan(QCtx *s,Class *cls,ulong qflags) : QueryOp(s,qflags|QO_UNIQUE|QO_STREAM|QO_IDSORT),scan(NULL)
{
	new(&key) SearchKey((uint64_t)(cls->getID()|((qflags&QO_DELETED)!=0?SDEL_FLAG:0)));
	if (s->ses->getIdentity()!=STORE_OWNER && (cls->getFlags()&CLASS_ACL)!=0) {
		//
	}
}

ClassScan::~ClassScan()
{
	if (scan!=NULL) scan->destroy();
}

RC ClassScan::next(const PINEx *skip)
{
	RC rc; if (res!=NULL) {res->cleanup(); *res=PIN::defPID;}
	if ((state&QST_INIT)!=0) {
		state&=~QST_INIT; if ((scan=qx->ses->getStore()->classMgr->getClassMap().scan(qx->ses,&key,&key,SCAN_EXACT))==NULL) return RC_NORESOURCES;
		if (nSkip>0 && (rc=scan->skip(nSkip))!=RC_OK || (rc=qx->ses->testAbortQ())!=RC_OK) {state|=QST_EOF|QST_BOF; return rc;}
	} else if (scan==NULL) return RC_NORESOURCES;
	size_t lData; const byte *er; byte *sk=NULL,lsk=0; assert(scan!=NULL);
	if (skip!=NULL && (skip->epr.lref!=0 || skip->pack()==RC_OK)) {sk=skip->epr.buf; lsk=skip->epr.lref;}
	if ((state&QST_EOF)==0) while ((er=(const byte*)scan->nextValue(lData,GO_NEXT,sk,lsk))!=NULL) {
		if ((rc=qx->ses->testAbortQ())==RC_OK && res!=NULL) {memcpy(res->epr.buf,er,res->epr.lref=(byte)lData); if ((qflags&QO_CHECKED)!=0) res->epr.flags|=PINEX_ACL_CHKED;}
		return rc;
	}
	state|=QST_EOF;
	return RC_EOF;
}

RC ClassScan::rewind()
{
	RC rc=(state&QST_INIT)!=0?RC_OK:scan!=NULL?scan->rewind():
		(scan=qx->ses->getStore()->classMgr->getClassMap().scan(qx->ses,&key,&key,SCAN_EXACT))!=NULL?RC_OK:RC_NORESOURCES;
	if (rc==RC_OK) state=state&~QST_EOF|QST_BOF;
	return rc;
}

RC ClassScan::count(uint64_t& cnt,ulong nAbort)
{
#if 0
	return qx->ses->getStore()->classMgr->getClassMap().countValues(key,cnt);
#else
	if ((state&QST_INIT)!=0) {
		state&=~QST_INIT; if ((scan=qx->ses->getStore()->classMgr->getClassMap().scan(qx->ses,&key,&key,SCAN_EXACT))==NULL) return RC_NORESOURCES;
	} else if (scan==NULL) return RC_NORESOURCES;
	uint64_t c=0; size_t lData; RC rc;
	while (scan->nextValue(lData)!=NULL) if (++c>nAbort) return RC_TIMEOUT; else if ((rc=qx->ses->testAbortQ())!=RC_OK) return rc;
	cnt=c; scan->destroy(); scan=NULL; state=QST_INIT; return RC_OK;
#endif
}

void ClassScan::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("class: ",7);
	URI *uri=(URI*)qx->ses->getStore()->uriMgr->ObjMgr::find((uint32_t)key.v.u); char cbuf[20]; const char *s;
	if (uri!=NULL&&(s=uri->getName())!=NULL) buf.append(s,strlen(s));
	else {sprintf(cbuf,"%08X",(uint32_t)key.v.u); buf.append(cbuf,8);}
	if (uri!=NULL) uri->release(); buf.append("\n",1);
}

//-----------------------------------------------------------------------------------------------

IndexScan::IndexScan(QCtx *qc,ClassIndex& idx,ulong flg,ulong nr,ulong qf) 
: QueryOp(qc,qf|QO_STREAM|QO_UNIQUE|QO_REVERSIBLE),index(idx),classID(((Class&)idx).getID()),flags(flg),
	rangeIdx(0),scan(NULL),pids(NULL),nRanges(nr),vals(NULL)
{
	if (idx.getNSegs()==1) flags|=idx.getIndexSegs()->flags; if (nRanges==0) flags&=~SCAN_EXACT;
	sort=(OrderSegQ*)((byte*)(this+1)+nRanges*2*sizeof(SearchKey)); nSegs=index.nSegs; 
	props=&pl; nProps=1; pl.props=(PropertyID*)(sort+index.nSegs); pl.nProps=0; pl.fFree=false;
	for (unsigned i=0; i<index.nSegs; i++) {
		const IndexSeg& is=index.indexSegs[i]; OrderSegQ& os=*(OrderSegQ*)&sort[i]; 
		os.pid=is.propID; os.flags=uint8_t(is.flags)&~ORDER_EXPR; os.var=0; os.aggop=OP_SET; os.lPref=is.lPrefix;
		for (unsigned j=0; ;j++) 
			if (j>=pl.nProps) {pl.props[pl.nProps++]=is.propID; break;} else if (is.propID==pl.props[j]) break;
			else if (is.propID<pl.props[j]) {memmove(&pl.props[j+1],&pl.props[j],(pl.nProps-j)*sizeof(PropertyID)); pl.props[j]=is.propID; pl.nProps++; break;}
	}
	if (qc->ses->getIdentity()!=STORE_OWNER && (((Class&)idx).getFlags()&CLASS_ACL)!=0) {
		//
	}
}

IndexScan::~IndexScan()
{
	if (pids!=NULL) delete pids; if (scan!=NULL) scan->destroy(); if (vals!=NULL) freeV(vals,index.getNSegs(),qx->ses);
	for (ulong i=0; i<nRanges; i++) {((SearchKey*)(this+1))[i*2].free(qx->ses); ((SearchKey*)(this+1))[i*2+1].free(qx->ses);}
	index.release();
}

void IndexScan::initInfo()
{
	if ((flags&SCAN_PREFIX)==0 && nRanges==1 && ((SearchKey*)(this+1))[0].isSet() && ((SearchKey*)(this+1))[1].isSet() 
		&& ((SearchKey*)(this+1))[0].cmp(((SearchKey*)(this+1))[1])==0) {qflags=qflags&~QO_REVERSIBLE|QO_IDSORT|QO_UNIQUE; sort=NULL; nSegs=0;}
}

void IndexScan::reverse()
{
	flags|=SCAN_BACKWARDS;
}

RC IndexScan::setScan(ulong idx)
{
	if (index.fmt.keyType()==KT_ALL) return RC_EOF;
	if (nRanges==0) scan=index.scan(qx->ses,NULL,NULL,flags);
	else {
		rangeIdx=idx; assert(idx<nRanges); const SearchKey *key=&((SearchKey*)(this+1))[idx*2];
		scan=index.scan(qx->ses,key[0].isSet()?key:(const SearchKey*)0,key[1].isSet()?key+1:(const SearchKey*)0,flags,index.getIndexSegs(),index.getNSegs());
	}
	return scan!=NULL?RC_OK:RC_NORESOURCES;
}

RC IndexScan::init()
{
	RC rc=setScan(0);
	if (rc==RC_OK) while (nSkip>0 && (rc=scan->skip(nSkip))!=RC_OK) {
		scan->destroy(); scan=NULL;
		if (rc!=RC_EOF || nRanges==0 || ++rangeIdx>=nRanges || (rc=setScan(rangeIdx))!=RC_OK) {state|=QST_EOF; return rc;}
	}
	return rc==RC_OK?qx->ses->testAbortQ():rc;
}

RC IndexScan::next(const PINEx *skip)
{
	RC rc; size_t l; const byte *er; if (res!=NULL) res->cleanup();
	if ((state&QST_INIT)!=0) {
		state&=~QST_INIT; if ((rc=init())!=RC_OK) return rc;
	} else if (scan==NULL) return RC_EOF;
	if (skip!=NULL) {
		// create key from values in skip, if possible
	}
	while (scan!=NULL) {
		while ((er=(const byte*)scan->nextValue(l,GO_NEXT))!=NULL) {
			if ((rc=qx->ses->testAbortQ())!=RC_OK) return rc;
			// if (PINRef::hasU1() && PINRef::u1!=classID) continue;
			if ((qflags&QO_UNIQUE)!=0 && PINRef::isColl(er,l)) {
				PINEx pex(qx->ses); memcpy(pex.epr.buf,er,pex.epr.lref=(byte)l);
				if (pids!=NULL) {if ((*pids)[pex]) continue;}
				else if ((pids=new(qx->ses) PIDStore(qx->ses))==NULL) return RC_NORESOURCES;
				(*pids)+=pex;
			}
			if (res!=NULL) {
				*res=PIN::defPID; memcpy(res->epr.buf,er,res->epr.lref=(byte)l);
				if ((qflags&QO_CHECKED)!=0) res->epr.flags|=PINEX_ACL_CHKED;
			}
			return RC_OK;
		}
		if (nRanges==0 || rangeIdx+1>=nRanges) break;
		scan->destroy(); if ((rc=setScan(++rangeIdx))!=RC_OK) return rc;
	}
	return RC_EOF;
}

RC IndexScan::rewind()
{
	if ((state&QST_INIT)!=0) return RC_OK;
	if (pids!=NULL) {delete pids; pids=NULL;} RC rc;
	if (rangeIdx>0 || scan==NULL) {if (scan!=NULL) scan->destroy(); if ((rc=setScan(0))!=RC_OK) return rc;}
	if ((rc=scan->rewind())==RC_OK) state=state&~QST_EOF|QST_BOF;
	return rc;
}

RC IndexScan::count(uint64_t& cnt,ulong nAbort)
{
	uint64_t c=0; RC rc=RC_EOF; PINEx cb(qx->ses);
	if ((state&QST_INIT)!=0) {state&=~QST_INIT; if ((rc=init())!=RC_OK) return rc;}
	while (scan!=NULL) {
		size_t l; const byte *er; rc=RC_OK;
		while ((er=(const byte*)scan->nextValue(l))!=NULL) {
			if ((rc=qx->ses->testAbortQ())!=RC_OK) return rc;
			// if (PINRef::hasU1() && PINRef::u1!=classID) continue;
			if ((qflags&QO_UNIQUE)!=0 && PINRef::isColl(er,l)) {
				cb=PIN::defPID; memcpy(cb.epr.buf,er,cb.epr.lref=(byte)l);
				if (pids!=NULL) {if ((*pids)[cb]) continue;}
				else if ((pids=new(qx->ses) PIDStore(qx->ses))==NULL) return RC_NORESOURCES;
				(*pids)+=cb;
			}
			if (++c>=nAbort) return RC_TIMEOUT;
		}
		scan->destroy(); scan=NULL; if (nRanges==0 || ++rangeIdx>=nRanges) break;
		if ((rc=setScan(rangeIdx))!=RC_OK || (rc=qx->ses->testAbortQ())!=RC_OK) break;
	}
	cnt=c; return rc;
}

RC IndexScan::loadData(PINEx& qr,Value *pv,unsigned nv,ElementID eid,bool fSort,MemAlloc *ma)
{
	if (scan==NULL) return RC_NOTFOUND;
	if (pv==NULL || nv==0) {
		const IndexSeg *is=index.getIndexSegs(); nv=index.getNSegs();
		if ((pv=vals)==NULL) {
			if ((pv=vals=new(qx->ses) Value[nv])==NULL) return RC_NORESOURCES;
			for (unsigned i=0; i<nv; i++) vals[i].setError(is[i].propID);
		} else for (unsigned i=0; i<nv; i++) {freeV(vals[i]); vals[i].setError(is[i].propID);}
		res->setProps(pv,nv);
	}
	return scan->getKey().getValues(pv,nv,index.getIndexSegs(),index.getNSegs(),qx->ses,!fSort,ma);
}

void IndexScan::unique(bool f)
{
	if (f) qflags|=QO_UNIQUE; else {qflags&=~QO_UNIQUE; delete pids; pids=NULL;}
}

void IndexScan::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("index: ",7); buf.renderName(classID); 
	if ((flags&SCAN_EXCLUDE_START)!=0) buf.append("[<",2); else buf.append("[",1);
	for (ulong i=0; i<nRanges; i++) {
		if (i!=0) buf.append("],[",2);
		printKey(((SearchKey*)(this+1))[i*2],buf,"*",1);
		buf.append(",",1);
		printKey(((SearchKey*)(this+1))[i*2+1],buf,"*",1);
	}
	if ((flags&SCAN_EXCLUDE_END)!=0) buf.append(">]\n",3); else buf.append("]\n",2);
}

void IndexScan::printKey(const SearchKey& key,SOutCtx& buf,const char *def,size_t ldef) const
{
	if (!key.isSet()) buf.append(def,ldef);
	else {
		const ulong nFields=index.getNSegs(); Value *v=(Value*)alloca(nFields*sizeof(Value));
		if (v!=NULL) for (unsigned i=0; i<nFields; i++) v[i].setError(index.getIndexSegs()[i].propID);
		if (v==NULL || key.getValues(v,nFields,index.getIndexSegs(),nFields,qx->ses)!=RC_OK) buf.append("???",3);
		else for (ulong i=0; i<nFields; i++) {if (i!=0) buf.append(":",1); buf.renderValue(v[i]); freeV(v[i]);}
	}
}

//------------------------------------------------------------------------------------------------

ArrayScan::ArrayScan(QCtx *qc,const PID *pds,ulong nP,ulong qf) : QueryOp(qc,qf|QO_UNIQUE|QO_STREAM|QO_IDSORT),pids((PID*)(this+1)),nPids(0),idx(0)
{
	if (nP>0) {pids[0]=pds[0]; nPids++; for (ulong i=1; i<nP; i++) if (pds[i]!=pds[i-1]) pids[nPids++]=pds[i];}
}

ArrayScan::~ArrayScan()
{
}

RC ArrayScan::next(const PINEx *skip)
{
	if (res!=NULL) {res->cleanup(); res->epr.flags=PINEX_EXTPID;} if (pids==NULL) return RC_EOF;
	if ((state&QST_INIT)!=0) {
		state&=~QST_INIT;
		if (nSkip>0) {if (nSkip<nPids) idx=nSkip,nSkip=0; else {nSkip-=(idx=nPids); return RC_EOF;}}
	}
	if (idx>=nPids) return RC_EOF; if (res!=NULL) {*res=pids[idx++];}
	return RC_OK;
}

RC ArrayScan::rewind()
{
	idx=0; state=state&~QST_EOF|QST_BOF; return RC_OK;
}

RC ArrayScan::count(uint64_t& cnt,ulong)
{
	cnt=nPids; return RC_OK;
}

void ArrayScan::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("array\n",6);
}

//------------------------------------------------------------------------------------------------

FTScan::FTScan(QCtx *qc,const char *w,size_t lW,const PropertyID *pds,ulong nps,ulong qf,ulong f,bool fStp)
	: QueryOp(qc,qf|QO_STREAM),word((char*)&pids[nps],(ushort)lW),flags(f),fStop(fStp),nScans(0),nPids(nps)
{
	// QO_UNIQUE, QO_IDSORT?
	memcpy((char*)word.v.ptr.p,w,lW); ((char*)word.v.ptr.p)[lW]=0; current.setError();
	if (pds!=NULL && nps!=0) memcpy(pids,pds,nps*sizeof(PropertyID));
}

FTScan::~FTScan()
{
	for (ulong i=0; i<nScans; i++) if (scans[i].scan!=NULL) scans[i].scan->destroy();
}

RC FTScan::next(const PINEx *skip)
{
	if ((state&QST_EOF)!=0) {res->cleanup(); return RC_EOF;}
	RC rc=RC_OK; if (res!=NULL) {res->epr.lref=0; *res=PIN::defPID;}
	if ((state&QST_INIT)!=0) {
		state&=~QST_INIT;
		scans[0].scan=qx->ses->getStore()->ftMgr->getIndexFT().scan(qx->ses,&word,&word,fStop?SCAN_EXCLUDE_START|SCAN_PREFIX:SCAN_PREFIX);
		if (scans[0].scan==NULL) return RC_NORESOURCES; nScans=1; scans[0].state=QOS_ADV; if (nSkip>0 && (rc=initSkip())!=RC_OK) return rc;
	}
	size_t lData; const byte *er=NULL; byte *sk0=NULL; byte lsk0=0;
//	if (skip!=NULL && skip->type==VT_REFID)
//		{PINRef pr(qx->ses->getStore()->storeID,skip->id); pr.def|=PR_PID2; pr.id2=skip->id; lsk0=pr.enc(skbuf); sk0=skbuf;}

//	if (skip!=NULL && skip->type!=VT_REFID) skip=NULL; 
	current.setError(); rc=RC_EOF;
	for (ulong i=0,prev=~0u; i<nScans; i++) {
		FTScanS& qs=scans[i]; int cmp=0;
		if ((qs.state&QST_EOF)==0) {
			if ((qs.state&QOS_ADV)!=0) {
				if (res!=NULL) res->cleanup(); qs.state&=~QOS_ADV; byte *sk=sk0; size_t lsk=lsk0;
				while ((er=(const byte*)qs.scan->nextValue(lData,GO_NEXT,sk,lsk))!=NULL) {
					if ((rc=qx->ses->testAbortQ())!=RC_OK) return rc;
#if 0
					memcpy(res->ref,er,res->lref=(byte)lData);
#else
					try {
						PINRef pr(qx->ses->getStore()->storeID,er,lData);
						if (nPids>0) {
							bool fFound=false;
							if ((pr.def&PR_U1)!=0) for (ulong i=0; i<nPids; i++) if (pids[i]==pr.u1) {fFound=true; break;}
							if (!fFound) {sk=NULL; lsk=0; continue;}
						}
						if ((flags&QFT_RET_NO_PARTS)!=0 && (pr.def&PR_PID2)!=0 && pr.id2.pid!=STORE_INVALID_PID) {sk=NULL; lsk=0; continue;}
						if (res!=NULL) {
							if ((pr.def&PR_PID2)==0 || pr.id2.pid==STORE_INVALID_PID || pr.id2.ident==STORE_INVALID_IDENTITY || (flags&QFT_RET_NO_DOC)!=0)
								*res=pr.id;
							else {
								*res=pr.id2; //if ((flags&QFT_RET_PARTS)!=0) ret=pr.id;	// out of order!!!
							}
						}
						PID id1,id2; if (skip==NULL || res->getID(id1)==RC_OK && skip->getID(id2)==RC_OK && cmpPIDs(id1,id2)>=0) break;
					} catch (RC&) {continue;}		// report???
#endif
				}
				if (er==NULL) {
					rc=RC_EOF; qs.state|=QST_EOF;
					if (current.type==VT_REFID && res!=NULL) {res->cleanup(); *res=current.id; /*res->flags=PINEX_COLLECTION;*/}
				} else {
					rc=RC_OK; if (res!=NULL) res->getID(qs.id);		//???
					if (current.type==VT_REFID && (cmp=cmpPIDs(current.id,qs.id))<0) {
						res->cleanup(); *res=current.id; /*res->flags=PINEX_COLLECTION;*/
					} else {
						if (prev!=~0u && cmp!=0) scans[prev].state&=~QOS_ADV; 
						prev=i; current.set(qs.id); qs.state|=QOS_ADV;
					}
				}
			} else if (current.type!=VT_REFID || (cmp=cmpPIDs(qs.id,current.id))<0) {
				rc=RC_OK; res->cleanup(); *res=qs.id; current.set(qs.id); /*res->flags=PINEX_COLLECTION;*/
				if (prev!=~0u) scans[prev].state&=~QOS_ADV; prev=i; qs.state|=QOS_ADV;
			} else if (cmp==0) qs.state|=QOS_ADV;
		}
	}
	if (rc!=RC_OK) state|=QST_EOF;
	return rc;
}

RC FTScan::rewind()
{
	// ???
	state=state&~QST_EOF|QST_BOF;
	return RC_OK;
}

void FTScan::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("ft scan: ",9); buf.append((char*)word.v.ptr.p,word.v.ptr.l); buf.append("\n",1);
}

PhraseFlt::PhraseFlt(QCtx *qc,FTScan *const *fts,ulong ns,ulong qf) : QueryOp(qc,qf|QO_JOIN|QO_IDSORT|QO_UNIQUE),nScans(ns)
{
	current.setError(); for (ulong i=0; i<nScans; i++) {scans[i].scan=fts[i]; scans[i].state=QOS_ADV;}
}

PhraseFlt::~PhraseFlt()
{
	for (ulong i=0; i<nScans; i++) delete scans[i].scan;
}

RC PhraseFlt::next(const PINEx *skip)
{
	if ((state&QST_EOF)!=0) {res->cleanup(); return RC_EOF;}
	RC rc=RC_OK; if (res!=NULL) {res->cleanup(); *res=PIN::defPID;}
	if ((state&QST_INIT)!=0) {state&=~QST_INIT; if (nSkip>0 && (rc=initSkip())!=RC_OK) return rc;}
//		if ((state&QOS_FIRST)==0) {for (ulong i=0; i<nScans; i++) scans[i].state|=QOS_FIRST; state&=~(QOS_BEG|QOS_EOF);}
//	if (skip!=NULL && skip->type!=VT_REFID) skip=NULL; 
//	if (skip!=NULL && (current.type!=VT_REFID || cmpPIDs(current.id,skip->id)<=0)) current.set(skip->id);
//	skip=current.type==VT_REFID?&current:(Value*)0;
	for (ulong i=0,nOK=0; nOK<nScans; ) {
		FTScanS& qs=scans[i]; res->cleanup(); if ((qs.state&QST_EOF)!=0) {rc=RC_EOF; break;}
		if ((rc=qs.scan->next(skip))!=RC_OK) {qs.state|=QST_EOF; break;}
		// compare PropertyID -> save it
#if 0
		if (current.type!=VT_REFID) {current.set(res->id); /*skip=&current;*/ nOK=1;}
		else if (res->id==current.id) nOK++;
		else if (cmpPIDs(res->id,current.id)<0) continue;
		else {current.id=res->id; nOK=1;}
#endif
		i=(i+1)%nScans;
	}
	if (rc!=RC_OK) state|=QST_EOF;
	else {
		// readPIN
		// check phrase in propid
	}
	return rc;
}

RC PhraseFlt::rewind()
{
	// ???
	state=state&~QST_EOF|QST_BOF;
	return RC_OK;
}

void PhraseFlt::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("phrase: ",8); 
	for (ulong i=0; i<nScans; i++) {
		//const FTScan *ft=scans[i].scan;
		//...
	}
	buf.append("\n",1);
}
