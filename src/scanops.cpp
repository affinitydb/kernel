/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

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

using namespace MVStoreKernel;

FullScan::~FullScan()
{
}

PBlock *FullScan::init(bool& fMyPage,bool fLast)
{
	PBlockP pDir; heapPageID=INVALID_PAGEID; const HeapDirMgr::HeapDirPage *hd; state&=~QST_INIT; PBlock *pb=NULL; fMyPage=true;
	for (dirPageID=ses->getStore()->theCB->getRoot(fClasses?fLast?MA_CLASSDIRLAST:MA_CLASSDIRFIRST:fLast?MA_HEAPDIRLAST:MA_HEAPDIRFIRST); dirPageID!=INVALID_PAGEID; ) {
		pDir=ses->getStore()->bufMgr->getPage(dirPageID,ses->getStore()->hdirMgr,QMGR_SCAN,pDir); if (pDir.isNull()) {dirPageID=INVALID_PAGEID; break;}
		hd=(const HeapDirMgr::HeapDirPage*)pDir->getPageBuf();
		if (!fLast) {
			for (idx=0; idx<hd->nSlots; ) {
				heapPageID=((PageID*)(hd+1))[idx++]; slot=0;
				if ((pb=ses->getLatched(heapPageID))!=NULL) {fMyPage=false; pDir.release();}		// check ULocked/XLocked
				else {
					pb=ses->getStore()->bufMgr->getPage(heapPageID,ses->getStore()->heapMgr,(mode&QO_FORUPDATE)!=0?PGCTL_ULOCK|QMGR_SCAN:QMGR_SCAN,pDir);
					pDir=NULL; fMyPage=true;
				}
				if (pb!=NULL) for (const HeapPageMgr::HeapPage *hp=(const HeapPageMgr::HeapPage*)pb->getPageBuf(); slot<hp->nSlots; slot++) {
					if (nSkip==0) return pb;
					const HeapPageMgr::HeapPIN *hpin=(const HeapPageMgr::HeapPIN *)hp->getObject(hp->getOffset(PageIdx(slot)));
					if (hpin!=NULL && hpin->hdr.getType()==HO_PIN && (hpin->hdr.descr&mask)==mask>>16) --nSkip;
				}
				pDir=ses->getStore()->bufMgr->getPage(dirPageID,ses->getStore()->hdirMgr,(mode&QO_FORUPDATE)!=0?QMGR_UFORCE|QMGR_SCAN:QMGR_SCAN,fMyPage?pb:NULL); 
				if (pDir.isNull()) {dirPageID=INVALID_PAGEID; state|=QST_BOF|QST_EOF; return NULL;} else hd=(const HeapDirMgr::HeapDirPage*)pDir->getPageBuf();
			}
			dirPageID=hd->next;
		} else {
			//???
			break;
		}
	}
	if (fMyPage && pb!=NULL) pb->release((mode&QO_FORUPDATE)!=0?QMGR_UFORCE:0);
	state|=QST_BOF|QST_EOF; return NULL;
}

RC FullScan::next(const PINEx *)
{
	PBlock *pb=NULL; bool fMyPage=true; assert(res==NULL || res->ses==ses);
	if (res!=NULL && !res->pb.isNull() && res->pb->getPageID()==heapPageID) {pb=res->pb; fMyPage=!res->pb.isSet(PGCTL_NOREL); res->pb=NULL;}
	res->cleanup(); RC rc; PBlockP pDir;
	if (idx==~0u && (pb=init(fMyPage))==NULL) return RC_EOF;
	for (;;) {
		if ((rc=ses->testAbortQ())!=RC_OK) return rc;
		if (heapPageID==INVALID_PAGEID) {
			if (dirPageID==INVALID_PAGEID) return RC_EOF;
			if (pDir.isNull()||pDir->getPageID()!=dirPageID) pDir=ses->getStore()->bufMgr->getPage(dirPageID,ses->getStore()->hdirMgr,QMGR_SCAN,pDir);
			if (pDir.isNull()) {dirPageID=INVALID_PAGEID; return RC_CORRUPTED;}
			const HeapDirMgr::HeapDirPage *hd=(const HeapDirMgr::HeapDirPage*)pDir->getPageBuf();
			if (idx>=hd->nSlots) {dirPageID=hd->next; idx=0; continue;}
			heapPageID=((PageID*)(hd+1))[idx++]; pDir.release(); slot=0;
		}
		if (pb==NULL || pb->getPageID()!=heapPageID) {
			if (pb==NULL) {
				if ((pb=ses->getLatched(heapPageID))!=NULL) fMyPage=false;
				else fMyPage=(pb=ses->getStore()->bufMgr->getPage(heapPageID,ses->getStore()->heapMgr,(mode&QO_FORUPDATE)!=0?PGCTL_ULOCK|QMGR_UFORCE|QMGR_SCAN:QMGR_SCAN,pb))!=NULL;
			} else if ((mode&QO_FORUPDATE)!=0 && !pb->isULocked() && !pb->isXLocked()) pb=NULL;
		}
		if (pb!=NULL && slot<((const HeapPageMgr::HeapPage*)pb->getPageBuf())->nSlots) {
			res->hp=(const HeapPageMgr::HeapPage *)pb->getPageBuf(); res->addr.pageID=res->hp->hdr.pageID;
			while (slot<res->hp->nSlots) {
				res->hpin=(const HeapPageMgr::HeapPIN *)res->hp->getObject(res->hp->getOffset(PageIdx(slot))); res->addr.idx=(PageIdx)slot++;
				if (res->hpin==NULL) {
					res->pb=(PBlock*)pb;
					if (ses->getStore()->lockMgr->getTVers(*res,(mode&QO_FORUPDATE)!=0?TVO_UPD:TVO_READ)==RC_OK && res->tv!=NULL) {
						// check deleted in uncommitted tx
					}
					res->pb=NULL;
				} else if (res->hpin->hdr.getType()==HO_PIN) {
					if (!res->hpin->getAddr(const_cast<PID&>(res->id))) {const_cast<PID&>(res->id).pid=res->addr; const_cast<PID&>(res->id).ident=STORE_OWNER;}
					if (!ses->inWriteTx()) {
						// check deleted before/inserted in uncommited
					}
					if ((res->hpin->hdr.descr&mask)==mask>>16) {res->pb=(PBlock*)pb; if (!fMyPage) res->pb.set(PGCTL_NOREL); res->epr.flags|=PINEX_ADDRSET; return RC_OK;}
				}
			}
		}
		if (fMyPage && pb!=NULL) pb->release((mode&QO_FORUPDATE)!=0?QMGR_UFORCE:0); heapPageID=INVALID_PAGEID; pb=NULL;
	}
}

RC FullScan::rewind()
{
	idx=~0u;
	//???if (fMyPage && pb!=NULL) pb->release((mode&QO_FORUPDATE)!=0?QMGR_UFORCE:0);
	return RC_OK;
}

RC FullScan::release()
{
	if (res!=NULL && !res->pb.isNull()) {
		res->pb.release();
	}
	return RC_OK;
}

void FullScan::getOpDescr(QODescr& qop)
{
	qop.flags=QO_UNIQUE|QO_STREAM|QO_ALLPROPS;
}

void FullScan::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("fullscan\n",9);
}

//-----------------------------------------------------------------------------------------------

ClassScan::ClassScan(Session *s,Class *cls,ulong mode) : QueryOp(s,NULL,mode),scan(NULL)
{
	new(&key) SearchKey((uint64_t)(cls->getID()|((mode&QO_DELETED)!=0?SDEL_FLAG:0)));
	if (s->getIdentity()!=STORE_OWNER && (cls->getFlags()&CLASS_ACL)!=0) {
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
		state&=~QST_INIT; if ((scan=ses->getStore()->classMgr->getClassMap().scan(ses,&key,&key,SCAN_EXACT))==NULL) return RC_NORESOURCES;
		if (nSkip>0 && (rc=scan->skip(nSkip))!=RC_OK || (rc=ses->testAbortQ())!=RC_OK) {state|=QST_EOF|QST_BOF; return rc;}
	} else if (scan==NULL) return RC_NORESOURCES;
	size_t lData; const byte *er; byte *sk=NULL,lsk=0; assert(scan!=NULL);
	if (skip!=NULL && (skip->epr.lref!=0 || skip->unpack()==RC_OK)) {sk=skip->epr.buf; lsk=skip->epr.lref;}
	if ((state&QST_EOF)==0) while ((er=(const byte*)scan->nextValue(lData,GO_NEXT,sk,lsk))!=NULL) {
		if ((rc=ses->testAbortQ())==RC_OK && res!=NULL) {memcpy(res->epr.buf,er,res->epr.lref=(byte)lData); if ((mode&QO_CHECKED)!=0) res->epr.flags|=PINEX_ACL_CHKED;}
		return rc;
	}
	state|=QST_EOF;
	return RC_EOF;
}

RC ClassScan::rewind()
{
	return (state&QST_INIT)!=0?RC_OK:scan!=NULL?scan->rewind():
		(scan=ses->getStore()->classMgr->getClassMap().scan(ses,&key,&key,SCAN_EXACT))!=NULL?RC_OK:RC_NORESOURCES;
}

RC ClassScan::count(uint64_t& cnt,ulong nAbort)
{
#if 0
	return ses->getStore()->classMgr->getClassMap().countValues(key,cnt);
#else
	if ((state&QST_INIT)!=0) {
		state&=~QST_INIT; if ((scan=ses->getStore()->classMgr->getClassMap().scan(ses,&key,&key,SCAN_EXACT))==NULL) return RC_NORESOURCES;
	} else if (scan==NULL) return RC_NORESOURCES;
	uint64_t c=0; size_t lData; RC rc;
	while (scan->nextValue(lData)!=NULL) if (++c>nAbort) return RC_TIMEOUT; else if ((rc=ses->testAbortQ())!=RC_OK) return rc;
	cnt=c; scan->destroy(); scan=NULL; state=QST_INIT; return RC_OK;
#endif
}

RC ClassScan::release()
{
	if (scan!=NULL) scan->release(); return RC_OK;
}

void ClassScan::getOpDescr(QODescr& qop)
{
	qop.flags=QO_UNIQUE|QO_STREAM|QO_PIDSORT;
}

void ClassScan::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("class: ",7);
	URI *uri=(URI*)ses->getStore()->uriMgr->ObjMgr::find((uint32_t)key.v.u); char cbuf[20]; const char *s;
	if (uri!=NULL&&(s=uri->getName())!=NULL) buf.append(s,strlen(s));
	else {sprintf(cbuf,"%08X",(uint32_t)key.v.u); buf.append(cbuf,8);}
	if (uri!=NULL) uri->release(); buf.append("\n",1);
}

//-----------------------------------------------------------------------------------------------
IndexScan::IndexScan(Session *s,ClassIndex& idx,ulong flg,ulong nr,ulong mode) 
: QueryOp(s,NULL,mode|QO_UNIQUE),index(idx),classID(((Class&)idx).getID()),flags(flg),
	rangeIdx(0),scan(NULL),pids(NULL),segs(NULL),props(NULL),nProps(0),nRanges(nr),vals(NULL)
{
	if (idx.getNSegs()==1) flags|=idx.getIndexSegs()->flags; if (nRanges==0) flags&=~SCAN_EXACT;
	if (s->getIdentity()!=STORE_OWNER && (((Class&)idx).getFlags()&CLASS_ACL)!=0) {
		//
	}
}

IndexScan::~IndexScan()
{
	if (pids!=NULL) delete pids; if (scan!=NULL) scan->destroy(); if (vals!=NULL) freeV(vals,index.getNSegs(),ses);
	for (ulong i=0; i<nRanges; i++) {((SearchKey*)(this+1))[i*2].free(ses); ((SearchKey*)(this+1))[i*2+1].free(ses);}
	index.release();
}

RC IndexScan::setScan(ulong idx)
{
	if (index.fmt.keyType()==KT_ALL) return RC_EOF;
	if (nRanges==0) scan=index.scan(ses,NULL,NULL,flags);
	else {
		rangeIdx=idx; assert(idx<nRanges); const SearchKey *key=&((SearchKey*)(this+1))[idx*2];
		scan=index.scan(ses,key[0].isSet()?key:(const SearchKey*)0,key[1].isSet()?key+1:(const SearchKey*)0,flags,index.getIndexSegs(),index.getNSegs());
	}
	return scan!=NULL?RC_OK:RC_NORESOURCES;
}

RC IndexScan::init(bool fF)
{
	RC rc=setScan(fF?0:nRanges-1);
	if (rc==RC_OK) while (nSkip>0 && (rc=scan->skip(nSkip,!fF))!=RC_OK) {
		scan->destroy(); scan=NULL;
		if (rc!=RC_EOF || nRanges==0 || fF && ++rangeIdx>=nRanges || !fF && rangeIdx--==0
				|| (rc=setScan(rangeIdx))!=RC_OK) {state|=fF?QST_EOF:QST_BOF; return rc;}
	}
	return rc==RC_OK?ses->testAbortQ():rc;
}

RC IndexScan::next(const PINEx *skip)
{
	RC rc; size_t l; const byte *er;
	if (res!=NULL) {res->cleanup(); *res=PIN::defPID;}
	if ((state&QST_INIT)!=0) {
		state&=~QST_INIT; if ((rc=init())!=RC_OK) return rc;
	} else if (scan==NULL) return RC_EOF;
	if (skip!=NULL) {
		// create key from values in skip, if possible
	}
	while (scan!=NULL) {
		while ((er=(const byte*)scan->nextValue(l,GO_NEXT))!=NULL) {
			if ((rc=ses->testAbortQ())!=RC_OK) return rc;
			// if (PINRef::hasU1() && PINRef::u1!=classID) continue;
			if ((mode&QO_UNIQUE)!=0 && PINRef::isColl(er,l)) {
				PINEx pex(ses); memcpy(pex.epr.buf,er,pex.epr.lref=(byte)l);
				if (pids!=NULL) {if ((*pids)[pex]) continue;}
				else if ((pids=new(ses) PIDStore(ses))==NULL) return RC_NORESOURCES;
				(*pids)+=pex;
			}
			if (res!=NULL) {
				*res=PIN::defPID; memcpy(res->epr.buf,er,res->epr.lref=(byte)l);
				if ((mode&QO_CHECKED)!=0) res->epr.flags|=PINEX_ACL_CHKED;
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
	return scan->rewind();
}

RC IndexScan::count(uint64_t& cnt,ulong nAbort)
{
	uint64_t c=0; RC rc=RC_EOF; PINEx cb(ses);
	if ((state&QST_INIT)!=0) {state&=~QST_INIT; if ((rc=init())!=RC_OK) return rc;}
	while (scan!=NULL) {
		size_t l; const byte *er; rc=RC_OK;
		while ((er=(const byte*)scan->nextValue(l))!=NULL) {
			if ((rc=ses->testAbortQ())!=RC_OK) return rc;
			// if (PINRef::hasU1() && PINRef::u1!=classID) continue;
			if ((mode&QO_UNIQUE)!=0 && PINRef::isColl(er,l)) {
				const_cast<PID&>(cb.id)=PIN::defPID; memcpy(cb.epr.buf,er,cb.epr.lref=(byte)l);
				if (pids!=NULL) {if ((*pids)[cb]) continue;}
				else if ((pids=new(ses) PIDStore(ses))==NULL) return RC_NORESOURCES;
				(*pids)+=cb;
			}
			if (++c>=nAbort) return RC_TIMEOUT;
		}
		scan->destroy(); scan=NULL; if (nRanges==0 || ++rangeIdx>=nRanges) break;
		if ((rc=setScan(rangeIdx))!=RC_OK || (rc=ses->testAbortQ())!=RC_OK) break;
	}
	cnt=c; return rc;
}

RC IndexScan::loadData(PINEx& qr,Value *pv,unsigned nv,MemAlloc *ma,ElementID eid)
{
	if (scan==NULL) return RC_NOTFOUND;
	if (pv==NULL || nv==0) {
		const IndexSeg *is=index.getIndexSegs(); nv=index.getNSegs();
		if ((pv=vals)==NULL) {
			if ((pv=vals=new(ses) Value[nv])==NULL) return RC_NORESOURCES;
			for (unsigned i=0; i<nv; i++) vals[i].setError(is[i].propID);
		} else for (unsigned i=0; i<nv; i++) {freeV(vals[i]); vals[i].setError(is[i].propID);}
		res->props=pv; res->nProps=nv; res->epr.flags&=~PINEX_DESTROY;
	}
	return scan->getKey().getValues(pv,nv,index.getIndexSegs(),index.getNSegs(),ses,true,ma);
}

RC IndexScan::release()
{
	if (scan!=NULL) scan->release(); return RC_OK;
}

void IndexScan::unique(bool f)
{
	if (f) mode|=QO_UNIQUE; else {mode&=~QO_UNIQUE; delete pids; pids=NULL;}
}

void IndexScan::getOpDescr(QODescr& qop)
{
	if (segs==NULL) {
		segs=(OrderSegQ*)((byte*)(this+1)+nRanges*2*sizeof(SearchKey)); props=(PropertyID*)(segs+index.nSegs);
		for (unsigned i=0; i<index.nSegs; i++) {
			const IndexSeg& is=index.indexSegs[i]; OrderSegQ& os=segs[i]; os.pid=is.propID; os.flags=uint8_t(is.flags)&~ORDER_EXPR; os.var=0; os.aggop=OP_SET; os.lPref=is.lPrefix;
			for (unsigned j=0; ;j++) 
				if (j>=nProps) {props[nProps++]=is.propID; break;} else if (is.propID==props[j]) break;
				else if (is.propID<props[j]) {memmove(&props[j+1],&props[j],(nProps-j)*sizeof(PropertyID)); props[j]=is.propID; nProps++; break;}
		}
	}
	qop.flags=QO_STREAM|(mode&QO_UNIQUE)|QO_REVERSIBLE;
	qop.sort=segs; qop.nSegs=index.nSegs; qop.props=props; qop.nProps=nProps;
	if ((flags&SCAN_PREFIX)==0 && nRanges==1 && ((SearchKey*)(this+1))[0].isSet() && ((SearchKey*)(this+1))[1].isSet() 
		&& ((SearchKey*)(this+1))[0].cmp(((SearchKey*)(this+1))[1])==0) qop.flags|=QO_PIDSORT|QO_UNIQUE;
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
		if (v==NULL || key.getValues(v,nFields,index.getIndexSegs(),nFields,ses)!=RC_OK) buf.append("???",3);
		else for (ulong i=0; i<nFields; i++) {if (i!=0) buf.append(":",1); buf.renderValue(v[i]); freeV(v[i]);}
	}
}

//------------------------------------------------------------------------------------------------

ArrayScan::ArrayScan(Session *s,const PID *pds,ulong nP,ulong md) : QueryOp(s,NULL,md),pids((PID*)(this+1)),nPids(0),idx(0)
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
	idx=0; return RC_OK;
}

RC ArrayScan::count(uint64_t& cnt,ulong)
{
	cnt=nPids; return RC_OK;
}

void ArrayScan::getOpDescr(QODescr& qop)
{
	qop.flags=QO_UNIQUE|QO_STREAM|QO_PIDSORT;
}

void ArrayScan::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("array\n",6);
}

//------------------------------------------------------------------------------------------------

FTScan::FTScan(Session *s,const char *w,size_t lW,const PropertyID *pds,ulong nps,ulong md,ulong f,bool fStp)
	: QueryOp(s,NULL,md),word((char*)&pids[nps],(ushort)lW),flags(f),fStop(fStp),nScans(0),nPids(nps)
{
	memcpy((char*)word.v.ptr.p,w,lW); ((char*)word.v.ptr.p)[lW]=0; current.setError();
	if (pds!=NULL && nps!=0) memcpy(pids,pds,nps*sizeof(PropertyID));
}

FTScan::~FTScan()
{
	for (ulong i=0; i<nScans; i++) if (scans[i].scan!=NULL) scans[i].scan->destroy();
}

RC FTScan::next(const PINEx *skip)
{
	RC rc=RC_OK; if (res!=NULL) {res->epr.lref=0; *res=PIN::defPID;}
	if ((state&QST_INIT)!=0) {
		state&=~QST_INIT;
		scans[0].scan=ses->getStore()->ftMgr->getIndexFT().scan(ses,&word,&word,fStop?SCAN_EXCLUDE_START|SCAN_PREFIX:SCAN_PREFIX);
		if (scans[0].scan==NULL) return RC_NORESOURCES; nScans=1; scans[0].state=QOS_ADV; if (nSkip>0 && (rc=initSkip())!=RC_OK) return rc;
	}
	size_t lData; const byte *er=NULL; byte *sk0=NULL; byte lsk0=0;
//	if (skip!=NULL && skip->type==VT_REFID)
//		{PINRef pr(ses->getStore()->storeID,skip->id); pr.def|=PR_PID2; pr.id2=skip->id; lsk0=pr.enc(skbuf); sk0=skbuf;}

//	if ((state&QOS_FIRST)==0) {for (ulong i=0; i<nScans; i++) scans[i].state|=QOS_FIRST; state&=~(QOS_BEG|QOS_EOF);}
	if ((state&QOS_EOF)==0) state&=~QOS_FIRST; else {res->cleanup(); return RC_EOF;}
//	if (skip!=NULL && skip->type!=VT_REFID) skip=NULL; 
	current.setError(); rc=RC_EOF;
	for (ulong i=0,prev=~0u; i<nScans; i++) {
		FTScanS& qs=scans[i]; int cmp=0;
		if ((qs.state&QOS_EOF)==0) {
			if ((qs.state&QOS_ADV)!=0) {
				if (res!=NULL) res->cleanup(); qs.state&=~QOS_ADV; byte *sk=sk0; size_t lsk=lsk0;
				for (GO_DIR sop=(qs.state&QOS_FIRST)!=0?(qs.state&=~QOS_FIRST,GO_FIRST):GO_NEXT; (er=(const byte*)qs.scan->nextValue(lData,sop,sk,lsk))!=NULL; sop=GO_NEXT) {
					if ((rc=ses->testAbortQ())!=RC_OK) return rc;
#if 0
					memcpy(res->ref,er,res->lref=(byte)lData);
#else
					try {
						PINRef pr(ses->getStore()->storeID,er,lData);
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
						if (skip==NULL || cmpPIDs(res->id,skip->id)>=0) break;
					} catch (RC&) {continue;}		// report???
#endif
				}
				qs.scan->release();
				if (er==NULL) {
					rc=RC_EOF; qs.state|=QOS_EOF;
					if (current.type==VT_REFID && res!=NULL) {res->cleanup(); *res=current.id; /*res->flags=PINEX_COLLECTION;*/}
				} else {
					rc=RC_OK; if (res!=NULL) qs.id=res->id;		//???
					if (current.type==VT_REFID && (cmp=cmpPIDs(current.id,res->id))<0) {
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
	if (rc!=RC_OK) {state|=QOS_EOF; release();}
	return rc;
}

RC FTScan::rewind()
{
	// ???
	return RC_OK;
}

RC FTScan::release()
{
	for (ulong i=0; i<nScans; i++) if (scans[i].scan!=NULL) scans[i].scan->release();
	return RC_OK;
}

void FTScan::getOpDescr(QODescr& qop)
{
	qop.flags=/*QO_UNIQUE|*/QO_STREAM;
	//qop.flags|=QO_PIDSORT;
}

void FTScan::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("ft scan: ",9); buf.append((char*)word.v.ptr.p,word.v.ptr.l); buf.append("\n",1);
}

PhraseFlt::PhraseFlt(Session *ses,FTScan *const *fts,ulong ns,ulong md) : QueryOp(ses,NULL,md),nScans(ns)
{
	current.setError(); for (ulong i=0; i<nScans; i++) {scans[i].scan=fts[i]; scans[i].state=QOS_ADV;}
}

PhraseFlt::~PhraseFlt()
{
	for (ulong i=0; i<nScans; i++) delete scans[i].scan;
}

RC PhraseFlt::next(const PINEx *skip)
{
	RC rc=RC_OK; if (res!=NULL) {res->cleanup(); *res=PIN::defPID;}
	if ((state&QST_INIT)!=0) {state&=~QST_INIT; if (nSkip>0 && (rc=initSkip())!=RC_OK) return rc;}
//		if ((state&QOS_FIRST)==0) {for (ulong i=0; i<nScans; i++) scans[i].state|=QOS_FIRST; state&=~(QOS_BEG|QOS_EOF);}
	if ((state&QOS_EOF)==0) state&=~QOS_FIRST; else {res->cleanup(); return RC_EOF;}
//	if (skip!=NULL && skip->type!=VT_REFID) skip=NULL; 
//	if (skip!=NULL && (current.type!=VT_REFID || cmpPIDs(current.id,skip->id)<=0)) current.set(skip->id);
//	skip=current.type==VT_REFID?&current:(Value*)0;
	for (ulong i=0,nOK=0; nOK<nScans; ) {
		FTScanS& qs=scans[i]; res->cleanup(); if ((qs.state&QOS_EOF)!=0) {rc=RC_EOF; break;}
		if ((rc=qs.scan->next(skip))!=RC_OK) {qs.state|=QOS_EOF; qs.scan->release(); break;}
		// compare PropertyID -> save it
#if 0
		if (current.type!=VT_REFID) {current.set(res->id); /*skip=&current;*/ nOK=1;}
		else if (res->id==current.id) nOK++;
		else if (cmpPIDs(res->id,current.id)<0) continue;
		else {current.id=res->id; nOK=1;}
#endif
		qs.scan->release(); i=(i+1)%nScans;
	}
	if (rc!=RC_OK) {state|=QOS_EOF; release();}
	else {
		// readPIN
		// check phrase in propid
	}
	return rc;
}

RC PhraseFlt::rewind()
{
	// ???
	return RC_OK;
}

RC PhraseFlt::release()
{
	for (ulong i=0; i<nScans; i++) scans[i].scan->release();
	return RC_OK;
}

void PhraseFlt::getOpDescr(QODescr& scan)
{
	scan.flags=QO_JOIN|QO_PIDSORT|QO_UNIQUE|QO_DEGREE;
	for (ulong i=0; i<nScans; i++) {
		QODescr qop1; scans[i].scan->getOpDescr(qop1); scan.flags&=qop1.flags&QO_DEGREE;
		if (qop1.level+1>scan.level) scan.level=qop1.level+1;
	}
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
