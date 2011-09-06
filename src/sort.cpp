/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov and Andrew Skowronski 2004 - 2010

**************************************************************************************/

#include "lock.h"
#include "session.h"
#include "queryprc.h"
#include "parser.h"
#include "expr.h"
#include "blob.h"
#include "fio.h"

using namespace MVStoreKernel;

Sort::Sort(QueryOp *qop,const OrderSegQ *os,unsigned nsgs,ulong md,unsigned nP,const PropertyID *pids,unsigned nPids)
: QueryOp(qop->getSession(),qop,md),nPreSorted(nP),fRepeat(false),pinMem(ses),nAllPins(0),idx(0),pins(NULL),lPins(0),esFile(NULL),esRuns(NULL),esOutRun(NULL),nIns(0),curRun(~0u),
	peakSortMem(DEFAULT_QUERY_MEM),maxSortMem(DEFAULT_QUERY_MEM),memUsed(0),nValues(nPids),index((unsigned*)((byte*)(this+1)+int(nsgs-1)*sizeof(OrderSegQ)+nPids*sizeof(PropertyID))),
	nPropIDs(nPids),propIDs((PropertyID*)((byte*)(this+1)+int(nsgs-1)*sizeof(OrderSegQ))),nSegs(nsgs)
{
	assert(nSegs!=1 || os==NULL || (os->flags&ORDER_EXPR)!=0 || os->pid!=PROP_SPEC_PINID);
	if (os!=NULL) memcpy((OrderSegQ*)sortSegs,os,nsgs*sizeof(OrderSegQ));
	if (pids!=NULL && nPids>0) memcpy((PropertyID*)propIDs,pids,nPids*sizeof(PropertyID));
	if (nSegs==0) {qop->unique(false); mode|=QO_UNIQUE;}	// descendant?
	else for (unsigned i=0; i<nSegs; i++) if ((sortSegs[i].flags&ORDER_EXPR)!=0) {
		if ((md&QO_VCOPIED)!=0 && (sortSegs[i].expr=Expr::clone(sortSegs[i].expr,ses))==NULL) throw RC_NORESOURCES;
		index[i]=nValues++;
	} else if (sortSegs[i].pid==PROP_SPEC_ANY) index[i]=nValues++;
	else index[i]=unsigned(BIN<PropertyID>::find(sortSegs[i].pid,pids,nPids)-pids);
}

Sort::~Sort()
{
	esCleanup();
	if (nValues>nPropIDs && (mode&QO_VCOPIED)!=0) for (unsigned i=0; i<nSegs; i++)
		if ((sortSegs[i].flags&ORDER_EXPR)!=0) sortSegs[i].expr->destroy();
	if (pins!=NULL) ses->free(pins);
}

void Sort::connect(PINEx **results,unsigned nRes)
{
	if (results!=NULL && nRes!=0) res=results[0];
}

__forceinline const byte *getSortID(const EncPINRef **ep,byte& lp)
{
#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
	if ((((ptrdiff_t)*ep)&1)!=0) {lp=byte((ptrdiff_t)*ep)>>1; return (byte*)ep+1;}
#endif
	lp=(*ep)->lref; return (*ep)->buf;
}

__forceinline const Value *getValues(const EncPINRef *ep)
{
	return (const Value*)((byte*)ep+ep->trunc<8>());		// 8???
}

__forceinline EncPINRef *Sort::storeSortData(const PINEx& qr,SubAlloc& pinMem)
{
	EncPINRef *ep=(EncPINRef*)pinMem.memalign(sizeof(EncPINRef*),qr.epr.trunc<8>()+nValues*sizeof(Value));
	if (ep!=NULL) {ep->flags=qr.epr.flags; memcpy(ep->buf,qr.epr.buf,ep->lref=qr.epr.lref);}
	return ep;
}

__forceinline int cmpSort(const EncPINRef *ep1,const EncPINRef *ep2)
{
	byte l1,l2; const byte *p1=getSortID(&ep1,l1),*p2=getSortID(&ep2,l2); return PINRef::cmpPIDs(p1,l1,p2,l2);
}

int Sort::cmp(const EncPINRef *p1,const EncPINRef *p2) const
{
#if 0
	// compare two pins
	PIN pin1(ses,p1->id,PageAddr::invAddr,PIN_NO_FREE,p1->id.ptr,nPropIDs),pin2(ses,p2->id,PageAddr::invAddr,PIN_NO_FREE,p2->id.ptr,nPropIDs);
	const PIN *pp[2] = {&pin1,&pin2}; Value res; RC rc=Expr::eval(&sortExpr,1,res,pp,2,NULL,0,ses);
	switch (rc) {
	default: break;		// ???
	case RC_TRUE: return -1;
	case RC_FALSE: return 1;
	case RC_OK:
		switch (res.type) {
		case VT_INT: return res.i;
		case VT_INT64: return res.i64<0?-1:res.i64>0?1:0;
		case VT_FLOAT: return res.f<0.f?-1:res.f>0.f?1:0;
		case VT_DOUBLE: return res.d<0?-1:res.d>0.?1:0;
		default: break;
		}
		break;
	}
#endif
	if (nPreSorted<nSegs) {
		const Value *pv1=getValues(p1),*pv2=getValues(p2);
		for (ulong k=nPreSorted; k<nSegs; ++k) {
			const uint32_t u=sortSegs[k].flags; int ret=0;
			if (index[k]==~0u) ret=cmpSort(p1,p2);
			else {
				const Value *pp1=&pv1[index[k]],*pp2=&pv2[index[k]];
				if (pp1->type==VT_ERROR) {if (pp2->type!=VT_ERROR) return (u&ORD_NULLS_BEFORE)!=0?-1:1;}
				else if (pp2->type==VT_ERROR) return (u&ORD_NULLS_BEFORE)!=0?1:-1;
				else {
					if (pp1->type==pp2->type) ret=Expr::cmp(*pp1,*pp2,(u&ORD_NCASE)!=0?CND_NCASE|CND_SORT:CND_SORT);
					else {
						Value v=*pp1; v.flags=NO_HEAP;
						ret=Expr::cvcmp(v,*pp2,(u&ORD_NCASE)!=0?CND_NCASE|CND_SORT:CND_SORT);
						freeV(v);
					}
					if (ret!=0) return (u&ORD_DESC)!=0?-ret:ret;
				}
			}
		}
		if ((mode&QO_VUNIQUE)!=0) fRepeat=true;
	}
	int cmp=cmpSort(p1,p2); if (cmp==0 && (mode&QO_UNIQUE)!=0) fRepeat=true; return cmp;
}

#define QS_STKSIZ			(8*sizeof(void*)-2)
#define QS_CUTOFF			8

void Sort::quickSort(ulong nPins)
{
	struct {ulong lwr,upr;} bndstk[QS_STKSIZ]; int stkptr=0;
	for (ulong lwr=0,upr=nPins-1;;) {
		if (upr-lwr+1<=QS_CUTOFF) while (upr>lwr) {
			ulong max=lwr;
			for (ulong p=lwr+1; p<=upr; ++p) if (cmp(pins[p],pins[max])>0) max=p;
		    swap(max,upr); --upr;
		} else {
			ulong mdl=(upr+lwr)/2;
			if (cmp(pins[lwr],pins[mdl])>0) swap(lwr,mdl);
			if (cmp(pins[lwr],pins[upr])>0) swap(lwr,upr);
			if (cmp(pins[mdl],pins[upr])>0) swap(mdl,upr);
			ulong lwr2=lwr,upr2=upr;
			for (;;) {
				if (mdl>lwr2) do ++lwr2; while (lwr2<mdl && cmp(pins[lwr2],pins[mdl])<=0);
				if (mdl<=lwr2) do ++lwr2; while (lwr2<=upr && cmp(pins[lwr2],pins[mdl])<=0);	// ????
				do --upr2; while (upr2>mdl && cmp(pins[upr2],pins[mdl])>0);
				if (upr2<lwr2) break;
				swap(lwr2,upr2); if (mdl==upr2) mdl=lwr2;
			}
			++upr2;
			if (mdl<upr2) do --upr2; while (upr2>mdl && cmp(pins[upr2],pins[mdl])==0);
			if (mdl>=upr2) do --upr2; while (upr2>lwr && cmp(pins[upr2],pins[mdl])==0);
			if (upr2-lwr>=upr-lwr2) {
				if (lwr<upr2) {bndstk[stkptr].lwr=lwr; bndstk[stkptr].upr=upr2; ++stkptr;}
				if (lwr2<upr) {lwr=lwr2; continue;}
			} else {
				if (lwr2<upr) {bndstk[stkptr].lwr=lwr2; bndstk[stkptr].upr=upr; ++stkptr;}
				if (lwr<upr2) {upr=upr2; continue;}
			}
		}
		if (--stkptr<0) return;
		lwr=bndstk[stkptr].lwr; upr=bndstk[stkptr].upr;
	}
}

RC Sort::sort(ulong nAbort)
{
	PINEx qr(ses); queryOp->connect(&qr);
	RC rc=RC_OK,rc2; ulong nRunPins=0; nAllPins=0;
	assert(queryOp!=NULL && pins==NULL && esRuns==NULL);
	for (; rc==RC_OK && ((rc=queryOp->next())==RC_OK || rc==RC_EOF); qr.cleanup()) {
		if ((rc2=ses->testAbortQ())!=RC_OK) {rc=rc2; break;}
		if (rc==RC_OK) {
			if (qr.epr.lref==0 && (rc2=qr.pack())!=RC_OK) rc=rc2;
			else {
				if ((mode&QO_UNIQUE)!=0 && nRunPins!=0) {
					// check previous id continue;
				}
				if (nAllPins+nRunPins+1>nAbort) {rc=RC_TIMEOUT; break;}				// after repeating are deleted?
			}
		}
		if (rc==RC_EOF || memUsed>peakSortMem || (nRunPins+1)*(sizeof(EncPINRef*)+sizeof(EncPINRef)+nValues*sizeof(Value))>maxSortMem) {	//?????
			if (nRunPins>=2) {
				fRepeat=false; quickSort(nRunPins);
				if (fRepeat) {
					for (ulong i=0,j=1,cnt=nRunPins; j<cnt; j++) {
						if (cmpSort(pins[i],pins[j])==0 || (mode&QO_VUNIQUE)!=0 && (fRepeat=false,cmp(pins[i],pins[j]),fRepeat)) nRunPins--;
						else if (++i!=j) pins[i]=pins[j];
					}
					if (rc!=RC_EOF && memUsed<peakSortMem) continue;
				}
			}
			nAllPins+=nRunPins;
			if (rc==RC_EOF) {
				if (nAllPins>nRunPins && (rc=writeRun(nRunPins,memUsed))==RC_OK) rc=RC_EOF;
				break;
			}
			if ((rc=writeRun(nRunPins,memUsed))!=RC_OK) break;
			nRunPins=0; pinMem.release();		// better, reset() (without mem deallocation)
		}
		if (nRunPins>=lPins) {
			lPins=lPins==0?256:lPins*2; if (memUsed+(lPins-nRunPins)*sizeof(EncPINRef*)>peakSortMem) lPins=ulong((peakSortMem-memUsed)/sizeof(EncPINRef*))+nRunPins+1;
			if ((pins=(EncPINRef**)ses->realloc(pins,lPins*sizeof(EncPINRef*)))!=NULL) memUsed+=(lPins-nRunPins)*sizeof(byte*); else {rc=RC_NORESOURCES; break;}
		}
#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
		if (nValues==0 && qr.epr.lref<sizeof(EncPINRef*) && qr.epr.flags==0)
			{byte *p=(byte*)&pins[nRunPins++]; p[0]=qr.epr.lref<<1|1; memcpy(p+1,qr.epr.buf,qr.epr.lref); continue;}
#endif

		SubAlloc::SubMark mrk; pinMem.mark(mrk);
		EncPINRef *ep=storeSortData(qr,pinMem); if (ep==NULL) {rc=RC_NORESOURCES; break;}
		pins[nRunPins++]=ep; if (nValues==0) {memUsed+=pinMem.length(mrk); continue;}

		Value *vals=(Value*)getValues(ep); int nArrays=0; ulong iArr=~0u,i; 
		if (nPropIDs!=0) {
			for (i=0; i<nPropIDs; i++) vals[i].setError(propIDs[i]);
			if ((rc=queryOp->getData(qr,vals,nPropIDs,NULL,&pinMem,(mode&QO_TOPMOST)!=0?STORE_FIRST_ELEMENT:STORE_COLLECTION_ID))!=RC_OK) break;
		}
		for (i=0; i<nSegs; i++) if (index[i]!=~0u) {
			Value *pv=&vals[index[i]];
			if ((sortSegs[i].flags&ORDER_EXPR)!=0) {const PINEx *pp=&qr; rc=Expr::eval(&sortSegs[i].expr,1,*pv,&pp,1,NULL,0,ses);}
			else if (sortSegs[i].pid==PROP_SPEC_ANY) {pv->set(qr.degree); continue;}
			if (rc==RC_NORESOURCES) break; 
			if (rc!=RC_OK) {pv->setError(); rc=RC_OK;}
			else if (pv->type==VT_ARRAY || pv->type==VT_COLLECTION) {if (++nArrays==1) iArr=i;}
			else if (sortSegs[i].lPref!=0 && (pv->type==VT_STRING||pv->type==VT_BSTR||pv->type==VT_URL)) pv->length=min(pv->length,(uint32_t)sortSegs[i].lPref);	// convert to VT_STRING?
			if (pv->type==VT_ERROR && (sortSegs[i].flags&(ORD_NULLS_BEFORE|ORD_NULLS_AFTER))==0 && i==0)		//???
				{ep=NULL; pinMem.truncate(mrk); nRunPins--; break;}
		}
		if (rc==RC_OK && ep!=NULL) switch (nArrays) {
		case 0: memUsed+=pinMem.length(mrk); break;
		case 1:
			// changeFColl();
			{const Value &av=vals[index[iArr]],*cv; INav *nav=av.type==VT_COLLECTION?vals[index[iArr]].nav:(INav*)0;
			if (nav==NULL) cv=&av.varray[0]; else cv=nav->navigate(GO_FIRST);
			for (uint32_t i=0,nv=av.length; cv!=NULL; ep=NULL) {
				if (ep==NULL) {
					if (nRunPins>=lPins) {
						lPins=lPins==0?256:lPins*2; if (memUsed+(lPins-nRunPins)*sizeof(EncPINRef*)>peakSortMem) lPins=ulong((peakSortMem-memUsed)/sizeof(EncPINRef*))+nRunPins+1;
						if ((pins=(EncPINRef**)ses->realloc(pins,lPins*sizeof(EncPINRef*)))!=NULL) memUsed+=(lPins-nRunPins)*sizeof(byte*); else {rc=RC_NORESOURCES; break;}
					}
					if ((pins[nRunPins++]=ep=storeSortData(qr,pinMem))==NULL) {rc=RC_NORESOURCES; break;}
					Value *vv=(Value*)getValues(ep); memcpy(vv,vals,nValues*sizeof(Value)); vals=vv;
				}
				Value *pv=&vals[index[iArr]];
				if (nav==NULL) *pv=*cv; else {pinMem.mark(mrk); if ((rc=copyV(*cv,*pv,&pinMem))!=RC_OK) break; memUsed+=pinMem.length(mrk);}
				if (sortSegs[iArr].lPref!=0 && (pv->type==VT_STRING||pv->type==VT_BSTR||pv->type==VT_URL)) pv->length=min(pv->length,(uint32_t)sortSegs[iArr].lPref);	// convert to VT_STRING?
				if (memUsed>peakSortMem || (nRunPins+1)*(sizeof(EncPINRef*)+sizeof(EncPINRef)+nValues*sizeof(Value))>maxSortMem) {
					if (nRunPins>=2) quickSort(nRunPins); assert((mode&QO_UNIQUE)==0);
					nAllPins+=nRunPins; fRepeat=false; if ((rc=writeRun(nRunPins,memUsed))!=RC_OK) break;
					nRunPins=0; pinMem.release();		// better, reset() (without mem deallocation)
				}
				if (nav!=NULL) cv=nav->navigate(GO_NEXT); else if (++i<nv) ++cv; else break;
			}
			if (nav!=NULL) nav->destroy(); else {/*???*/}
			}
			break;
		default:
			// ???
			rc=RC_INTERNAL; break;
		}
	}
	delete queryOp; const_cast<QueryOp*&>(this->queryOp)=NULL;
	if (rc==RC_EOF && (nAllPins<=nRunPins||(rc=prepMerge())==RC_OK||rc==RC_EOF)) {rc=RC_OK; idx=0;} else {nAllPins=0; pinMem.release();}
	return rc;
}

namespace MVStoreKernel
{

typedef	uint32_t		RunID;
#define INVALID_RUN		RunID(~0u)
#define EXTENT_ALLOC	0x40
#define TESTES			0

struct ExtSortPageHdr 
{
	uint32_t	nItems;		//Number of pins
	uint32_t	bufEnd;		//End of buffer for backward scanning
	RunID run;				//Sanity check
};

class ExtSortFile 
{
	friend class OutRun;
	friend class InRun;
	struct ExtSortPage {
		PageID	next;	// next in run or empty list
		PageID	prev;	// previous in run or empty
	};
	struct RunInfo {
		PageID	head;
		PageID	pos;	  // current
		PageID	tail;  
		RunInfo() : head(INVALID_PAGEID),pos(INVALID_PAGEID),tail(INVALID_PAGEID) {}
	};
	Session		*const	ses;
	FileID		fid;
	ExtSortPage	*pages;
	ulong		nPages; // Currently allocated in file
	RunInfo		*runs;
	ulong		nRuns;
	ulong		xRuns;
	PageID		freeList;

public:
	ExtSortFile(Session *s) : ses(s),fid(INVALID_FILEID),pages(NULL),nPages(0),
						runs(NULL),nRuns(0),xRuns(0),freeList(INVALID_PAGEID) {}
	~ExtSortFile() {
		if (pages!=NULL) ses->free(pages);
		if (runs!=NULL) ses->free(runs);	
		if (fid!=INVALID_FILEID) ses->getStore()->fileMgr->close(fid);
	}

	RunID beginRun() {
		if (nRuns>=xRuns && (runs=(RunInfo*)ses->realloc(runs,sizeof(RunInfo)*(xRuns+=xRuns==0?16:xRuns/2)))==NULL) return INVALID_RUN;
		runs[nRuns].head=runs[nRuns].tail=runs[nRuns].pos=INVALID_PAGEID; return nRuns++;
	}

	RC writePage(RunID runid,byte *pageBuf) {
		if (runid>=nRuns) return RC_INVPARAM;
		if (fid==INVALID_FILEID) {
			RC rc=ses->getStore()->fileMgr->open(fid,NULL,FIO_TEMP);
			if (rc!=RC_OK) {report(MSG_ERROR,"Failure to create external sort file (%d)\n",rc); return rc;}
		}
		PageID pos=getFreePage(); if (pos==INVALID_PAGEID) return RC_NORESOURCES;
		assert(pages[pos].next==INVALID_PAGEID);

		// encrypt page!!!
		RC rc=ses->getStore()->fileMgr->io(FIO_WRITE,::PageIDFromPageNum(fid,pos),pageBuf,pageLen());  
		if (rc!=RC_OK) {
			report(MSG_ERROR,"Failure to write page %x to external sort file (%d)\n",pos,rc); releasePage(pos);
		} else {
			PageID oldTail=runs[runid].tail;
			if(oldTail==INVALID_PAGEID) {
				assert(runs[runid].head==INVALID_PAGEID);
				runs[runid].head=runs[runid].pos=runs[runid].tail=pos;
			} else {
				assert(pages[oldTail].next==INVALID_PAGEID);
				assert(runs[runid].head!=INVALID_PAGEID);
				pages[oldTail].next=pos; runs[runid].tail=pos;
			}
		}
		check();
		return rc;
	}
	RC readPage(RunID runid, byte* buf, bool bRelease=false) {
		if(buf==NULL) return RC_INVPARAM;
		if(runid>=nRuns || runs[runid].pos==INVALID_PAGEID) return RC_NOTFOUND;
		RC rc; PageID nextInRun=runs[runid].pos;
		if ((rc=ses->getStore()->fileMgr->io(FIO_READ,::PageIDFromPageNum(fid,nextInRun),buf,pageLen()))==RC_OK) {
			runs[runid].pos=pages[nextInRun].next;
			if (bRelease) {runs[runid].head=runs[runid].pos; releasePage(nextInRun);} // when no rewind needed
			// decrypt page!!!
		}
		check();
		return rc;
	}
	// Return run pages to empty list
	RC releaseRun(RunID runid) {	
		for (PageID pos=runs[runid].head,next; pos!=INVALID_PAGEID; pos=next) {next=pages[pos].next; releasePage(pos);}
		return RC_OK;
	}
	RC rewind(RunID runid) {
		if(runid>=nRuns) return RC_INVPARAM;
		runs[runid].pos=runs[runid].head;
		return RC_OK;
	}

	size_t pageLen() const {return ses->getStore()->fileMgr->getPageSize();}
	ulong runCount() const {return nRuns;}

	void check(bool bPrintStats=false) {
#ifdef _DEBUG
		if (fid==INVALID_FILEID) {
			assert(freeList==INVALID_PAGEID);
		} else {
			assert(nPages==(ulong)ses->getStore()->fileMgr->getFileSize(fid)/pageLen());
			assert(pages!=NULL);
			ulong cntFree=0; 
			for (PageID iter=freeList; iter!=INVALID_PAGEID; iter=pages[iter].next) cntFree++;
            assert(xRuns>=nRuns);
			ulong cnt=cntFree; ulong cntEmpty=0;
			for (ulong i=0; i<nRuns; i++) {
				PageID iter=runs[i].head;
				if (iter==INVALID_PAGEID) cntEmpty++;
				else for (;iter!=INVALID_PAGEID; iter=pages[iter].next) cnt++;
			}
			if(bPrintStats) report(MSG_DEBUG,"External sort: Alloc Pages 0x%x, Free Pages 0x%x Runs 0x%x Empty Runs 0x%x\n",nPages,cntFree,nRuns,cntEmpty);
			assert(nPages==cnt);
		}
#endif
	}
	void	operator	delete(void *p) {if (p!=NULL) ((ExtSortFile*)p)->ses->free(p);}

private:
	PageID getFreePage() {
		if (freeList==INVALID_PAGEID) growFile();
		if (freeList==INVALID_PAGEID) return INVALID_PAGEID;
		PageID free=freeList;
		freeList=pages[freeList].next;
		pages[free].next=INVALID_PAGEID;
		return free;
	}

	void releasePage(PageID c) {
		assert(c<nPages); assert(c!=INVALID_PAGEID);
		pages[c].next=freeList; freeList=c;
	}

	void growFile() {
		assert(freeList==INVALID_PAGEID); // Only grow if all full
		off64_t addr; RC rc;
		if ((rc=ses->getStore()->fileMgr->allocateExtent(fid,EXTENT_ALLOC,addr))!=RC_OK)
			{report(MSG_ERROR,"Cannot grow file for external sort (%d)\n",rc); return; }
		ulong curPageCnt=nPages; nPages+=EXTENT_ALLOC;
		pages=(ExtSortPage*)ses->realloc(pages,nPages*sizeof(ExtSortPage));
		for (ulong i=curPageCnt; i<nPages-1; i++) pages[i].next=i+1;
		pages[nPages-1].next=INVALID_PAGEID; freeList=curPageCnt;
	}
};

class OutRun
{
	// Assist in writing a sorted series of pins into ExtSortFile
	ExtSortFile		*esFile;
	byte			*page;     
	size_t			pagePos;
	ExtSortPageHdr	*hdr;
	unsigned		nValues;
	ulong			nRunPins;
public:
	OutRun(ExtSortFile *es,unsigned nVals) : esFile(es),page(NULL),hdr(NULL),nValues(nVals),nRunPins(0) {
		page=(byte*)es->ses->malloc(esFile->pageLen()); hdr=(ExtSortPageHdr*)page;
	}
	~OutRun() {
		if (page!=NULL) esFile->ses->free(page);
	}

	RC beginRun() {
		if (page==NULL) return RC_NORESOURCES;
		hdr->run=esFile->beginRun(); hdr->nItems=0;
		pagePos=sizeof(ExtSortPageHdr);
		nRunPins=0;
		return RC_OK;
	}

	RC add(const EncPINRef *ep) {
		// Get buffer to fill in
		const size_t lHdr=sizeof(uint16_t)+1+
#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
			((((ptrdiff_t)ep)&1)!=0?byte((ptrdiff_t)ep)>>1:ep->lref);
#else
			ep->lref;
#endif
		size_t pinlen=lHdr,pageLen=esFile->pageLen();
		if (nValues!=0) {
			const Value *pv=getValues(ep);
			for (ulong i=0; i<nValues; i++,pv++) pinlen+=MVStoreKernel::serSize(*pv);
		}
		size_t oldLen=mv_len32(pinlen); pinlen+=oldLen;
		size_t newLen=mv_len32(pinlen); pinlen+=newLen-oldLen;

		if (pinlen+pagePos>pageLen) {
			if (hdr->nItems==0) {report(MSG_ERROR,"Single sort key too large (%u) for external sort\n",pinlen); return RC_NORESOURCES;}
#if TESTES
			report(MSG_DEBUG,"Write page, run %x, %x pins\n",hdr->run,hdr->nItems);
#endif
			RC rc = esFile->writePage(hdr->run,page); if (rc!=RC_OK) return rc;
			// Start next page
			pagePos = sizeof(ExtSortPageHdr); hdr->nItems=0;
#ifdef _DEBUG
			memset(page+pagePos,0xCD,pageLen-pagePos);
#endif
		}

		// serialize pin
		byte *ps=page+pagePos; mv_enc32(ps,pinlen);
#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
		if ((((ptrdiff_t)ep)&1)!=0) {
			*(uint16_t*)ps=0; ps[sizeof(uint16_t)]=byte((ptrdiff_t)ep)>>1; memcpy(ps+sizeof(uint16_t)+1,(byte*)&ep+1,ps[sizeof(uint16_t)]);
		} else
#endif
		memcpy(ps,ep,lHdr); ps+=lHdr;

		// Values 
		if (nValues!=0) {
			const Value *pv=getValues(ep);
			for (ulong i=0; i<nValues; i++,pv++) ps=MVStoreKernel::serialize(*pv,ps);
		}

		hdr->nItems++; nRunPins++; pagePos=ulong(ps-page);
		return RC_OK;
	}

	RC term()
	{
		if (hdr->nItems>0) { 
			esFile->writePage(hdr->run,page); 
#if TESTES
			report(MSG_DEBUG,"Write last run page, run %x, %x pins, total %x\n",hdr->run,hdr->nItems,nRunPins);	
#endif
		}
		return RC_OK;
	}
	void	operator	delete(void *p) {if (p!=NULL) ((OutRun*)p)->esFile->ses->free(p);}
};

class InRun
{
	// Assist in reading a run from disk
	ExtSortFile			*esFile;
	RunID				runid;
	Sort				*sort;
	ulong				remItems; 
	const	byte		*page;     // Current page of run
	const	byte		*pos;		
	bool				bTemp;
	EncPINRef			*ep;
	byte				*buf;

public:
	// if new,delete created
	InRun() : esFile(NULL),runid(INVALID_RUN),sort(NULL),remItems(0),page(NULL),pos(NULL),bTemp(false),ep(NULL),buf(NULL) {}

	~InRun() {term(); if (buf!=NULL) sort->ses->free(buf);}

	RC init(ExtSortFile *es,Sort *s) {
		esFile=es; sort=s;
		buf=(byte*)s->ses->memalign(sizeof(EncPINRef*),sizeof(EncPINRef)+s->nValues*sizeof(Value));
#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
		assert((((ptrdiff_t)buf)&1)==0);
#endif
		return buf!=NULL&&(page=(byte*)s->ses->malloc(esFile->pageLen()))!=NULL?RC_OK:RC_NORESOURCES;
	}
	void setRun(RunID r, bool bTempRun) {
		// Can change run
		runid=r; remItems=0; pos=NULL;
		bTemp=bTempRun; 
		ep=NULL;
		next();
	}
	RC rewind() {
		assert(!bTemp);
		ep=NULL; remItems=0; pos=NULL;
		esFile->rewind(runid);
		return RC_OK;
	}
	void term() {
		if (ep!=NULL) {
#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
			if ((((ptrdiff_t)ep)&1)==0)
#endif
			{
				Value *pv=(Value*)getValues(ep);
				for (unsigned i=0; i<sort->nValues; i++) freeV(pv[i]);
			}
			ep=NULL;
		}
		if (page!=NULL) sort->ses->free((byte*)page);
	}

	bool next() {
		RC rc=RC_OK;
		if (remItems==0) {
			if (esFile->readPage(runid,(byte*)page,bTemp)==RC_OK) {
				pos=page;
				ExtSortPageHdr *pageHdr=(ExtSortPageHdr*)page;
				assert(pageHdr->run==runid);
				remItems=pageHdr->nItems;
				pos+=sizeof(ExtSortPageHdr);
#if TESTES
				report(MSG_DEBUG,"Read page, run %x, %x pins\n",runid,pageHdr->nItems);
#endif
			} else {
				if (ep!=NULL) {
#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
					if ((((ptrdiff_t)ep)&1)==0)
#endif
					{
						Value *pv=(Value*)getValues(ep);
						for (unsigned i=0; i<sort->nValues; i++) freeV(pv[i]);
					}
					ep=NULL;
				}
				pos=NULL; return false;
			}
		}
		assert(page!=NULL);
		assert((ulong)(pos-page)<esFile->pageLen());

		const byte *end=pos; size_t len; mv_dec32(pos,len); end+=len; assert((ulong)(end-page)<=esFile->pageLen());
		const EncPINRef *hdr=(const EncPINRef *)pos; pos+=sizeof(uint16_t)+1+hdr->lref; assert((ulong)(pos-page)<=esFile->pageLen());

#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
		if (sort->nValues==0 && hdr->lref<sizeof(EncPINRef*) && hdr->flags==0)		// flags alignment???
			{byte *p=(byte*)&ep; p[0]=hdr->lref<<1|1; memcpy(p+1,hdr->buf,hdr->lref);} else
#endif
		{
			ep=(EncPINRef*)buf; memcpy(ep,hdr,sizeof(uint16_t)+1+hdr->lref);
			Value *pv=(Value*)getValues(ep);
			for (unsigned i=0; i<sort->nValues; i++,pv++)
				if ((rc=MVStoreKernel::deserialize(*pv,pos,end,sort->ses,false))!=RC_OK) break;
		}
		pos=end; remItems--;
		return rc==RC_OK;
	}
	const EncPINRef *top() {return ep;} // current item in run
	void *operator new[](size_t s,Session *ses) throw() {return ses->malloc(s);}
	void operator delete[](void *p) {free(p,SES_HEAP);}
};

}

RC Sort::prepMerge() {
	RC rc; ulong r; ulong nTtlRuns=esFile->runCount(); assert(nTtlRuns>0);
	assert(esFile!=NULL && esRuns==NULL && esOutRun!=NULL);
	
	idx=0; curRun=~0u;
	ses->free(pins); pins=NULL; pinMem.release(); //Not needed for ext sort
	assert(maxSortMem%esFile->pageLen()==0);

	ulong maxMerge = (ulong)(maxSortMem/esFile->pageLen())-1;  assert(maxMerge>1);

	nIns=nTtlRuns>maxMerge?maxMerge:nTtlRuns;	
	if ((esRuns=new(ses) InRun[nIns])==NULL) return RC_NORESOURCES;
	for (r=0;r<nIns;r++) {rc=esRuns[r].init(esFile,this); if (rc!=RC_OK) return rc;}

	ulong firstLiveRun=0; ulong nActiveRuns=nTtlRuns;
	for (;;) {
		if (nActiveRuns>maxMerge) {
			//intermediate merges to reduce number of runs
			ulong npass = nActiveRuns/maxMerge; ulong extra = nActiveRuns%maxMerge;
			for (ulong p=0; p<npass; p++) {
#if TESTES
				report(MSG_DEBUG,"Pass %x merges to target %x\n",p,firstLiveRun+nActiveRuns+p+1);
#endif
				for (r=0;r<maxMerge;r++){
					ulong runid=firstLiveRun+p*maxMerge+r;
					esRuns[r].setRun(runid,true);
				}
				rc=esOutRun->beginRun(); if(rc!=RC_OK) return rc;
				for (;;) {
					const EncPINRef *minep=NULL; ulong champ=nIns;
					for (r=0; r<nIns; r++){
						const EncPINRef *cand=esRuns[r].top();
						if (minep==NULL || cand!=NULL && cmp(minep,cand)>0) {minep=cand; champ=r;}
					}
					if (minep==NULL) break; assert(champ<nIns);
					esOutRun->add(minep); esRuns[champ].next();
				}
				esOutRun->term();
			}					
			firstLiveRun+=npass*maxMerge;//prep next iter
			nActiveRuns=npass+extra;
		}
		else {
			if (nIns>nActiveRuns) nIns=nActiveRuns;
			for (r=0;r<nIns;r++) esRuns[r].setRun(firstLiveRun+r,false/*final*/);
			//final merge will be "on demand"
			break;
		}
		esFile->check(true);
	}
	delete esOutRun; esOutRun=NULL;
	return RC_OK;
}

RC Sort::esnext(PINEx& qr)
{
	const EncPINRef *ep=NULL; ulong r;
	if (curRun!=~0u) esRuns[curRun].next();
	for (r=0,curRun=~0u; r<nIns; r++) {
		const EncPINRef *cand=esRuns[r].top();
		if (cand!=NULL && (ep==NULL || cmp(ep,cand)>0)) {ep=cand; curRun=r;}
	}
	if (ep==NULL) return RC_EOF;					//All runs exhausted
#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
	if ((((ptrdiff_t)ep)&1)!=0) {
		qr.epr.flags=0; qr.epr.lref=byte((ptrdiff_t)ep)>>1; memcpy(qr.epr.buf,(byte*)&ep+1,qr.epr.lref);
	} else
#endif
	memcpy(&qr.epr,ep,ep->trunc<8>());
	return RC_OK;
}

RC Sort::writeRun(ulong nRunPins,size_t& mUsed)
{
	if (esFile==NULL) esFile=new(ses) ExtSortFile(ses);
	if (esOutRun==NULL) esOutRun=new(ses) OutRun(esFile,nValues);

	// dump sorted pins into run
	esOutRun->beginRun(); RC rc;
	for (ulong k=0; k<nRunPins; k++) if ((rc=esOutRun->add(pins[k]))!=RC_OK) return rc;
	esOutRun->term(); mUsed=esFile->pageLen();
	return RC_OK;
}

void Sort::esCleanup()
{
	if (esOutRun!=NULL) {delete esOutRun; esOutRun=NULL;}
	if (esRuns!=NULL) {delete[] esRuns; esRuns=NULL;}
	if (esFile!=NULL) {delete esFile; esFile=NULL;}
}

RC Sort::next(const PINEx *skip)
{
	res->cleanup(); *res=PIN::defPID; RC rc=RC_OK;
	if ((state&QST_INIT)!=0) {
		state&=~QST_INIT; assert(memUsed==0 && nAllPins==0 && pins==NULL && esRuns==NULL);
//		size_t minMem=ses->getStore()->fileMgr->getPageSize()*4;
//		peakSortMem=max(peak,minMem); maxSortMem=max(avg,minMem);
		if ((rc=sort())!=RC_OK||(rc=ses->testAbortQ())!=RC_OK) {state|=QST_EOF|QST_BOF; return rc;}
		state|=nAllPins<=1?QST_EOF|QST_BOF:QST_BOF; idx=0; if (nAllPins==0) return RC_EOF;
		if (nSkip!=0) {
			if (nSkip>=nAllPins) {nSkip-=(ulong)nAllPins; idx=nAllPins-1; state|=QST_EOF; return RC_EOF;}
			if (pins!=NULL) idx=nSkip;
			else {
				PINEx dm(ses); assert(esFile!=NULL);
				for(ulong i=0; i<nSkip; i++) {
					if ((rc=esnext(dm))!=RC_OK||(rc=ses->testAbortQ())!=RC_OK) return rc;
					dm.cleanup();
				}
			}
		}
	} else if (nAllPins==0) return RC_EOF;
	else {
		if ((state&QST_EOF)!=0) return RC_EOF;
		if (++idx+1<nAllPins && skip!=NULL) {
			if (esRuns!=NULL) {
				// ???
#if 0
			} else if (skip->type==VT_REFID) for (ulong n=nAllPins-idx; n!=0; ) {
				ulong k=n>>1; const SPID *sp=pins[k+idx]; int cmp=cmpSort(sp->id,skip->id);
				if (cmp==0) {idx+=k; break;} else if (cmp<0) {idx+=k+1; n-=k+1;} else n=k;
#endif
			} else {
				//???
			}
		}
		if (idx+1>=nAllPins) state|=QST_EOF;
	}
	if (pins!=NULL) {
		const EncPINRef *ep=pins[idx];
#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
		if ((((ptrdiff_t)ep)&1)!=0) {
			res->epr.flags=0; res->epr.lref=byte((ptrdiff_t)ep)>>1; memcpy(res->epr.buf,(byte*)&ep+1,res->epr.lref);
		} else
#endif
		 memcpy(&res->epr,ep,ep->trunc<8>());
	} else if (esRuns!=NULL) {
		if ((rc=esnext(*res))!=RC_OK) return rc;
	} else return RC_EOF;
	return ses->testAbortQ();
}

RC Sort::rewind()
{
	RC rc; idx=0; state|=QST_BOF; if (nAllPins>1) state&=~QST_EOF;
	if (esRuns!=NULL) for (ulong i=0; i<nIns; i++) if ((rc=esRuns[i].rewind())!=RC_OK) return rc;
	return RC_OK;
}

RC Sort::count(uint64_t& cnt,ulong nAbort)
{
	if (queryOp!=NULL) {
		QODescr qop; queryOp->getOpDescr(qop);
		if ((qop.flags&QO_UNIQUE)!=0 || (mode&QO_UNIQUE)==0) return queryOp->count(cnt,nAbort);
		if ((state&QST_INIT)!=0) {state&=~QST_INIT; RC rc=sort(nAbort); if (rc!=RC_OK) return rc;}
	}
	cnt=nAllPins; return RC_OK;
}

RC Sort::loadData(PINEx& qr,Value *pv,unsigned nv,MemAlloc *ma,ElementID eid)
{
	if (nPropIDs==0) return RC_NOTFOUND;
	RC rc=RC_OK; assert(pins!=NULL || curRun<nIns);
	const Value *vals=getValues(pins!=NULL?pins[idx]:esRuns[curRun].top());
	if (vals==NULL) rc=RC_NOTFOUND;
	else if (pv==NULL || nv==0) {qr.props=vals; qr.nProps=nPropIDs; qr.mode&=~PINEX_DESTROY;}
	else for (ulong i=0; rc==RC_OK && i<nv; i++) for (unsigned j=0; ;j++)	// BS::find???
		if (j>=nPropIDs) {pv[i].setError(pv[i].property); break;}
		else if (propIDs[j]==pv[i].property) {
			if (ma==NULL) {pv[i]=vals[j]; pv[i].flags=NO_HEAP;} else rc=copyV(vals[j],pv[i],ma);
			break;
		}
	return rc;
}

void Sort::unique(bool f)
{
	if (f) mode|=QO_UNIQUE; else {mode&=~QO_UNIQUE; if (queryOp!=NULL && nPropIDs!=0) queryOp->unique(f);}
}

void Sort::reorder(bool)
{
}

void Sort::getOpDescr(QODescr& qop)
{
	if (queryOp!=NULL) {queryOp->getOpDescr(qop); qop.flags&=~(QO_STREAM|QO_ALLPROPS); qop.level++;}
	qop.sort=sortSegs; qop.nSegs=nSegs; if (nPropIDs==0) qop.flags|=QO_PIDSORT|(mode&QO_UNIQUE);
}

void Sort::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("sort: ",6);
	if (nSegs==0) buf.renderName(PROP_SPEC_PINID);
	else for (unsigned i=0; i<nSegs; i++) {
		const OrderSegQ &os=sortSegs[i]; if (i!=0) buf.append(", ",2);
		if ((os.flags&ORDER_EXPR)!=0) os.expr->render(0,buf);
		else if (os.pid==PROP_SPEC_ANY) buf.append("#rank",5);
		else buf.renderName(os.pid);
		// flags!!!
	}
	buf.append("\n",1); if (queryOp!=NULL) queryOp->print(buf,level+1);
}
