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

Written by Mark Venguerov and Andrew Skowronski 2004-2014

**************************************************************************************/

#include "lock.h"
#include "session.h"
#include "queryprc.h"
#include "parser.h"
#include "expr.h"
#include "blob.h"
#include "fio.h"

using namespace AfyKernel;

Sort::Sort(QueryOp *qop,const OrderSegQ *os,unsigned nsgs,unsigned qf,unsigned nP,const PropList *pids,unsigned nPids)
: QueryOp(qop,qf),nPreSorted(nP),fRepeat(false),pinMem(ctx->ses),nAllPins(0),idx(0),pins(NULL),lPins(0),esFile(NULL),esRuns(NULL),esOutRun(NULL),nIns(0),curRun(~0u),
	peakSortMem(DEFAULT_QUERY_MEM),maxSortMem(DEFAULT_QUERY_MEM),memUsed(0),nValues(0),index((unsigned*)((byte*)(this+1)+int(nsgs-1)*sizeof(OrderSegQ)+nPids*sizeof(PropList))),
	nPls(nPids),pls((PropList*)((byte*)(this+1)+int(nsgs-1)*sizeof(OrderSegQ)))
{
	QueryOp::sort=sortSegs; nSegs=nsgs;
	assert(nsgs!=1 || os==NULL || (os->flags&ORDER_EXPR)!=0 || os->pid!=PROP_SPEC_PINID);
	if (os!=NULL) memcpy((OrderSegQ*)sortSegs,os,nsgs*sizeof(OrderSegQ));
	if (pids!=NULL && nPids>0) {
		memcpy((PropList*)pls,pids,nPids*sizeof(PropList));
		for (unsigned i=0; i<nPids; i++) nValues+=pls[i].nProps;
		props=pls; nProps=nPids;
	}
	if (nsgs==0) {qop->unique(false); qflags|=QO_UNIQUE|QO_IDSORT;}
	else for (unsigned i=0; i<nsgs; i++) if ((sortSegs[i].flags&ORDER_EXPR)!=0) index[i]=nValues++;
		else {
			if (sortSegs[i].var>nPids) throw RC_INVPARAM; const PropertyID *pi=pls[sortSegs[i].var].props;
			index[i]=unsigned(BIN<PropertyID>::find(sortSegs[i].pid,pi,pls[sortSegs[i].var].nProps)-pi);		// + shift for this var
		}
}

Sort::~Sort()
{
	esCleanup();
}

void Sort::connect(PINx **results,unsigned nRes)
{
	if (results!=NULL && nRes!=0) {res=results[0]; state|=QST_CONNECTED;}
}

__forceinline int cmpStoredIDs(const EncPINRef *ep1,const EncPINRef *ep2)
{
	byte l1,l2; const byte *p1,*p2;
#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
	if ((((ptrdiff_t)ep1)&1)!=0) l1=byte((ptrdiff_t)ep1)>>1,p1=(byte*)&ep1+1; else
#endif
	l1=PINRef::len(ep1->buf),p1=ep1->buf;
#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
	if ((((ptrdiff_t)ep2)&1)!=0) l2=byte((ptrdiff_t)ep2)>>1,p2=(byte*)&ep2+1; else
#endif
	l2=PINRef::len(ep2->buf),p2=ep2->buf;
	return l1*l2!=0?PINRef::cmpPIDs(p1,p2):0;
}

int Sort::cmp(const EncPINRef *p1,const EncPINRef *p2) const
{
#if 0
	// compare two pins
	PIN pin1(ctx->ses,p1->id,PageAddr::noAddr,PIN_NO_FREE,p1->id.ptr,nValues),pin2(ctx->ses,p2->id,PageAddr::noAddr,PIN_NO_FREE,p2->id.ptr,nValues);
	const PIN *pp[2] = {&pin1,&pin2}; Value res; RC rc=Expr::eval(&sortExpr,1,res,pp,2,NULL,0,ctx->ses);
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
	int ret=0;
	if (nSegs!=0) {
		if (nPreSorted>=nSegs) return 0;
		const Value *pv1=getStoredValues(p1),*pv2=getStoredValues(p2);
		for (unsigned k=nPreSorted; k<nSegs; ++k) {
			const uint32_t u=sortSegs[k].flags;
			if (index[k]==~0u) ret=cmpStoredIDs(p1,p2);
			else {
				const Value *pp1=&pv1[index[k]],*pp2=&pv2[index[k]];
				if (pp1->isEmpty()) {if (!pp2->isEmpty()) return (u&ORD_NULLS_BEFORE)!=0?-1:1;}
				else if (pp2->isEmpty()) return (u&ORD_NULLS_BEFORE)!=0?1:-1;
				else if ((ret=AfyKernel::cmp(*pp1,*pp2,(u&ORD_NCASE)!=0?CND_NCASE|CND_SORT:CND_SORT,ctx->ses))!=0) return (u&ORD_DESC)!=0?-ret:ret;
			}
		}
		if ((qflags&QO_VUNIQUE)!=0) fRepeat=true;
	}
	if ((ret=cmpStoredIDs(p1,p2))==0 && (qflags&QO_UNIQUE)!=0) fRepeat=true; 
	return ret;
}

namespace AfyKernel
{
	struct ArrayVal {ArrayVal *prev; INav *nav; Value *vals; unsigned length,idx,vidx; ushort lPref;};
};

RC Sort::sort(unsigned nAbort)
{
	assert(queryOp!=NULL && pins==NULL && esRuns==NULL);
	PINx qr(ctx->ses); RC rc=RC_OK,rc2; unsigned nRunPins=0; nAllPins=0; ArrayVal *freeAV=NULL;
	unsigned nqr=queryOp->getNOuts(); PINx **pqr=(PINx**)alloca(nqr*sizeof(PINx*)); if (pqr==NULL) return RC_NOMEM;
	for (unsigned i=1; i<nqr; i++) if ((pqr[i]=new(ctx->ma) PINx(ctx->ses))==NULL) return RC_NOMEM;
	pqr[0]=&qr; queryOp->connect(pqr,nqr);
	for (; ((rc=queryOp->next())==RC_OK || rc==RC_EOF); qr.cleanup()) {
		if ((rc2=ctx->ses->testAbortQ())!=RC_OK) {rc=rc2; break;}
		if (rc==RC_OK) {
			if (qr.epr.buf[0]==0 && qr.getPID().isPID() && (rc2=qr.pack())!=RC_OK) {rc=rc2; break;}
			if ((qflags&QO_UNIQUE)!=0 && nRunPins!=0) {
				// check previous id continue;
			}
			if (nAllPins+nRunPins+1>nAbort) {rc=RC_TIMEOUT; break;}				// after repeating are deleted?
		}
		if (rc==RC_EOF || ctx->ses->getStore()->fileMgr!=NULL && (memUsed>peakSortMem || (nRunPins+1)*(sizeof(EncPINRef*)+sizeof(EncPINRef)+nValues*sizeof(Value))>maxSortMem)) {	//?????
			if (nRunPins>=2) {
				fRepeat=false; QSort<Sort>::sort(*this,nRunPins);
				if (fRepeat) {
					for (unsigned i=0,j=1,cnt=nRunPins; j<cnt; j++) {
						if (cmpStoredIDs(pins[i],pins[j])==0 || (qflags&QO_VUNIQUE)!=0 && (fRepeat=false,cmp(i,j),fRepeat)) nRunPins--;
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
			nRunPins=0; pinMem.truncate(TR_REL_NONE);
		}
		if (nRunPins>=lPins) {
			lPins=lPins==0?256:lPins*2; if (memUsed+(lPins-nRunPins)*sizeof(EncPINRef*)>peakSortMem) lPins=unsigned((peakSortMem-memUsed)/sizeof(EncPINRef*))+nRunPins+1;
			if ((pins=(EncPINRef**)ctx->ma->realloc(pins,lPins*sizeof(EncPINRef*),nRunPins*sizeof(EncPINRef*)))!=NULL) memUsed+=(lPins-nRunPins)*sizeof(byte*); else {rc=RC_NOMEM; break;}
		}

#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
		if (nValues==0 && storeEPR(&pins[nRunPins],qr)) {nRunPins++; continue;}
#endif

		StackAlloc::SubMark mrk; pinMem.mark(mrk);
		EncPINRef *ep=storeValues(qr,nValues,pinMem); if (ep==NULL) {rc=RC_NOMEM; break;}
		pins[nRunPins++]=ep; if (nValues==0) {memUsed+=pinMem.length(mrk); continue;}

		Value *vals=(Value*)getStoredValues(ep);
		if (nPls!=0 && (rc=getData(pqr,nqr,pls,nPls,vals,&pinMem))!=RC_OK) break;
		bool fSkip=true; ArrayVal *avs=NULL;
		for (unsigned i=0; i<nSegs; i++) if (index[i]!=~0u) {
			Value *pv=&vals[index[i]]; ArrayVal *av; const Value *cv; INav *nav;
			if ((sortSegs[i].flags&ORDER_EXPR)!=0 && (rc=sortSegs[i].expr->eval(*pv,EvalCtx(ctx->ses,NULL,0,(PIN**)pqr,nqr)))!=RC_OK) 
				{if (rc==RC_NOMEM) break; pv->setError(); rc=RC_OK; continue;}
			switch (pv->type) {
			case VT_ERROR: if ((sortSegs[i].flags&(ORD_NULLS_BEFORE|ORD_NULLS_AFTER))==0) continue; break;
			case VT_COLLECTION:
				switch (sortSegs[i].aggop) {
				case OP_ALL: case OP_HISTOGRAM:
					if ((av=freeAV)!=NULL) freeAV=av->prev;
					else if ((av=pinMem.alloc<ArrayVal>())==NULL) {rc=RC_NOMEM; break;} //???
					av->idx=0; av->vidx=index[i]; av->lPref=sortSegs[i].lPref; av->prev=avs; avs=av;
					if (!pv->isNav()) {
						av->vals=(Value*)pv->varray; av->length=pv->length; av->nav=NULL; *pv=pv->varray[0];
					} else {
						av->nav=pv->nav; av->vals=NULL; av->length=0; cv=pv->nav->navigate(GO_FIRST);
						if ((rc=copyV(*cv,*pv,&pinMem))!=RC_OK) break;	//???
					}
					break;
				case OP_SET: case OP_ADD:
					nav=NULL; cv=!pv->isNav()?&pv->varray[sortSegs[i].aggop==OP_SET?0:pv->length-1]:(nav=pv->nav)->navigate(sortSegs[i].aggop==OP_SET?GO_FIRST:GO_LAST);
					rc=cv!=NULL?copyV(*cv,*pv,&pinMem):RC_CORRUPTED; if (nav!=NULL) nav->destroy(); 
					break;
				default:
					{EvalCtx ectx2(ctx->ses,NULL,0,NULL,0,NULL,0,ctx,&pinMem); AggAcc ag((ExprOp)sortSegs[i].aggop,0,&ectx2,NULL); unsigned j=0;	// prefix? flags?
					for (const Value *cv=pv->isNav()?pv->nav->navigate(GO_FIRST):pv->varray; cv!=NULL; cv=pv->isNav()?pv->nav->navigate(GO_NEXT):++j<pv->length?&pv->varray[j]:(const Value *)0) ag.next(*cv);
					freeV(*pv); if (ag.result(*pv)!=RC_OK) {pv->setError(); continue;}
					} break;
				}
				if (rc!=RC_OK || pv->type!=VT_STRING && pv->type!=VT_BSTR) break;
			case VT_STRING: case  VT_BSTR:
				if (sortSegs[i].lPref!=0) pv->length=min(pv->length,(uint32_t)sortSegs[i].lPref);	// convert to VT_STRING?
				break;
			}
			fSkip=false;
		}
		if (rc!=RC_OK) break;
		if (fSkip) {pinMem.truncate(TR_REL_NONE,&mrk); nRunPins--;}
		else if (avs==NULL) memUsed+=pinMem.length(mrk);
		else {
			// changeFColl();
			for (;;) {
				StackAlloc::SubMark mrk2; pinMem.mark(mrk2);
				if ((ep=storeValues(qr,nValues,pinMem))==NULL) {rc=RC_NOMEM; break;}
				Value *vv=(Value*)getStoredValues(ep); memcpy(vv,vals,nValues*sizeof(Value)); vals=vv; bool fNext=false; 
				for (ArrayVal *av=avs; !fNext && av!=NULL; av=av->prev) {
					Value *pv=&vals[av->vidx];
					if (av->vals!=NULL) {
						if (++av->idx>=av->length) av->idx=0; else fNext=true;
						*pv=av->vals[av->idx];
					} else {
						const Value *cv; assert(av->nav!=NULL);
						if ((cv=av->nav->navigate(GO_NEXT))!=NULL) fNext=true;
						else if (av->prev!=NULL) {cv=av->nav->navigate(GO_FIRST); assert(cv!=NULL);}
						if ((rc=copyV(*cv,*pv,&pinMem))!=RC_OK) {fNext=false; break;}
					}
					if (av->lPref!=0 && (pv->type==VT_STRING||pv->type==VT_BSTR)) pv->length=min(pv->length,(uint32_t)av->lPref);	// convert to VT_STRING?
				}
				if (!fNext) {pinMem.truncate(TR_REL_ALL,&mrk2); break;}
				if (nRunPins>=lPins) {
					lPins=lPins==0?256:lPins*2; if (memUsed+(lPins-nRunPins)*sizeof(EncPINRef*)>peakSortMem) lPins=unsigned((peakSortMem-memUsed)/sizeof(EncPINRef*))+nRunPins+1;
					if ((pins=(EncPINRef**)ctx->ma->realloc(pins,lPins*sizeof(EncPINRef*),nRunPins*sizeof(EncPINRef*)))!=NULL) memUsed+=(lPins-nRunPins)*sizeof(byte*); else {rc=RC_NOMEM; break;}
				}
				pins[nRunPins++]=ep;
				if (memUsed>peakSortMem || nRunPins*(sizeof(EncPINRef*)+sizeof(EncPINRef)+nValues*sizeof(Value))>maxSortMem) {
					if (nRunPins>=2) QSort<Sort>::sort(*this,nRunPins); assert((qflags&QO_UNIQUE)==0);
					nAllPins+=nRunPins; fRepeat=false; if ((rc=writeRun(nRunPins,memUsed))!=RC_OK) break;
					nRunPins=0; pinMem.truncate(TR_REL_NONE);		// vals ???????????
				}
			}
			while (avs!=NULL) {ArrayVal *av=avs->prev; if (avs->nav!=NULL) avs->nav->destroy(); avs->prev=freeAV; freeAV=avs; avs=av;}
			if (rc!=RC_OK) break;
		}
	}
	//queryOp->~QueryOp(); const_cast<QueryOp*&>(this->queryOp)=NULL;
	for (unsigned i=1; i<nqr; i++) pqr[i]->~PINx();
	if (rc==RC_EOF && (nAllPins<=nRunPins||(rc=prepMerge())==RC_OK||rc==RC_EOF)) {rc=RC_OK; idx=0;} else {nAllPins=0; pinMem.truncate(TR_REL_ALL);}
	return rc;
}

namespace AfyKernel
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
	FileMgr		*const	fileMgr;
	FileID		fid;
	ExtSortPage	*pages;
	unsigned	nPages; // Currently allocated in file
	RunInfo		*runs;
	unsigned	nRuns;
	unsigned	xRuns;
	PageID		freeList;

public:
	ExtSortFile(Session *s) : ses(s),fileMgr(s->getStore()->fileMgr),fid(INVALID_FILEID),pages(NULL),nPages(0),
						runs(NULL),nRuns(0),xRuns(0),freeList(INVALID_PAGEID) {}
	~ExtSortFile() {
		if (pages!=NULL) ses->free(pages); if (runs!=NULL) ses->free(runs);	
		if (fid!=INVALID_FILEID) {assert(fileMgr!=NULL); fileMgr->close(fid);}
	}

	RunID beginRun() {
		if (nRuns>=xRuns && (runs=(RunInfo*)ses->realloc(runs,sizeof(RunInfo)*(xRuns+=xRuns==0?16:xRuns/2)))==NULL) return INVALID_RUN;
		runs[nRuns].head=runs[nRuns].tail=runs[nRuns].pos=INVALID_PAGEID; return nRuns++;
	}

	RC writePage(RunID runid,byte *pageBuf) {
		if (runid>=nRuns) return RC_INVPARAM;
		if (fid==INVALID_FILEID) {
			assert(fileMgr!=NULL); RC rc=fileMgr->open(fid,NULL,FIO_TEMP);
			if (rc!=RC_OK) {report(MSG_ERROR,"Failure to create external sort file (%d)\n",rc); return rc;}
		}
		PageID pos=getFreePage(); if (pos==INVALID_PAGEID) return RC_NOMEM;
		assert(pages[pos].next==INVALID_PAGEID);

		// encrypt page!!!
		assert(fileMgr!=NULL);
		RC rc=fileMgr->io(FIO_WRITE,::PageIDFromPageNum(fid,pos),pageBuf,pageLen());  
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
		RC rc; PageID nextInRun=runs[runid].pos; assert(fileMgr!=NULL);
		if ((rc=fileMgr->io(FIO_READ,::PageIDFromPageNum(fid,nextInRun),buf,pageLen()))==RC_OK) {
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

	size_t pageLen() const {assert(fileMgr!=NULL); return fileMgr->getPageSize();}
	unsigned runCount() const {return nRuns;}

	void check(bool bPrintStats=false) {
#ifdef _DEBUG
		if (fid==INVALID_FILEID) {
			assert(freeList==INVALID_PAGEID);
		} else {
			assert(fileMgr!=NULL && nPages==(unsigned)fileMgr->getFileSize(fid)/pageLen());
			assert(pages!=NULL);
			unsigned cntFree=0; 
			for (PageID iter=freeList; iter!=INVALID_PAGEID; iter=pages[iter].next) cntFree++;
            assert(xRuns>=nRuns);
			unsigned cnt=cntFree; unsigned cntEmpty=0;
			for (unsigned i=0; i<nRuns; i++) {
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
		off64_t addr; RC rc; assert(fileMgr!=NULL);
		if ((rc=fileMgr->allocateExtent(fid,EXTENT_ALLOC,addr))!=RC_OK)
			{report(MSG_ERROR,"Cannot grow file for external sort (%d)\n",rc); return; }
		unsigned curPageCnt=nPages; nPages+=EXTENT_ALLOC;
		pages=(ExtSortPage*)ses->realloc(pages,nPages*sizeof(ExtSortPage));
		for (unsigned i=curPageCnt; i<nPages-1; i++) pages[i].next=i+1;
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
	unsigned			nRunPins;
public:
	OutRun(ExtSortFile *es,unsigned nVals) : esFile(es),page(NULL),hdr(NULL),nValues(nVals),nRunPins(0) {
		page=(byte*)es->ses->malloc(esFile->pageLen()); hdr=(ExtSortPageHdr*)page;
	}
	~OutRun() {
		if (page!=NULL) esFile->ses->free(page);
	}

	RC beginRun() {
		if (page==NULL) return RC_NOMEM;
		hdr->run=esFile->beginRun(); hdr->nItems=0;
		pagePos=sizeof(ExtSortPageHdr);
		nRunPins=0;
		return RC_OK;
	}

	RC add(const EncPINRef *ep) {
		// Get buffer to fill in
		const size_t lHdr=sizeof(uint16_t)+(
#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
			(((ptrdiff_t)ep)&1)!=0?byte((ptrdiff_t)ep)>>1:
#endif
			PINRef::len(ep->buf));
		size_t pinlen=lHdr,pageLen=esFile->pageLen();
		if (nValues!=0) {
			const Value *pv=getStoredValues(ep);
			for (unsigned i=0; i<nValues; i++,pv++) pinlen+=AfyKernel::serSize(*pv);
		}
		size_t oldLen=afy_len32(pinlen); pinlen+=oldLen;
		size_t newLen=afy_len32(pinlen); pinlen+=newLen-oldLen;

		if (pinlen+pagePos>pageLen) {
			if (hdr->nItems==0) {report(MSG_ERROR,"Single sort key too large (%u) for external sort\n",pinlen); return RC_NOMEM;}
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
		byte *ps=page+pagePos; afy_enc32(ps,pinlen);
#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
		if ((((ptrdiff_t)ep)&1)!=0) {
			*(uint16_t*)ps=0; memcpy(ps+sizeof(uint16_t),(byte*)&ep+1,byte((ptrdiff_t)ep)>>1);
		} else
#endif
		memcpy(ps,ep,lHdr); ps+=lHdr;

		// Values 
		if (nValues!=0) {
			const Value *pv=getStoredValues(ep);
			for (unsigned i=0; i<nValues; i++,pv++) ps=AfyKernel::serialize(*pv,ps);
		}

		hdr->nItems++; nRunPins++; pagePos=unsigned(ps-page);
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
	unsigned				remItems; 
	const	byte		*page;     // Current page of run
	const	byte		*pos;		
	bool				bTemp;
	EncPINRef			*ep;
	byte				*buf;

public:
	// if new,delete created
	InRun() : esFile(NULL),runid(INVALID_RUN),sort(NULL),remItems(0),page(NULL),pos(NULL),bTemp(false),ep(NULL),buf(NULL) {}

	~InRun() {term(); if (buf!=NULL) sort->ctx->ses->free(buf);}

	RC init(ExtSortFile *es,Sort *s) {
		esFile=es; sort=s; buf=(byte*)s->ctx->ses->malloc(sizeof(EncPINRef)+s->nValues*sizeof(Value));
#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
		assert((((ptrdiff_t)buf)&1)==0);
#endif
		return buf!=NULL&&(page=(byte*)s->ctx->ses->malloc(esFile->pageLen()))!=NULL?RC_OK:RC_NOMEM;
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
				Value *pv=(Value*)getStoredValues(ep);
				for (unsigned i=0; i<sort->nValues; i++) freeV(pv[i]);
			}
			ep=NULL;
		}
		if (page!=NULL) sort->ctx->ses->free((byte*)page);
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
						Value *pv=(Value*)getStoredValues(ep);
						for (unsigned i=0; i<sort->nValues; i++) freeV(pv[i]);
					}
					ep=NULL;
				}
				pos=NULL; return false;
			}
		}
		assert(page!=NULL);
		assert((unsigned)(pos-page)<esFile->pageLen());

		const byte *end=pos; size_t len; afy_dec32(pos,len); end+=len; assert((unsigned)(end-page)<=esFile->pageLen());
		const EncPINRef *hdr=(const EncPINRef *)pos; pos+=sizeof(uint16_t)+PINRef::len(hdr->buf); assert((unsigned)(pos-page)<=esFile->pageLen());

#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
		byte lref=PINRef::len(hdr->buf);
		if (sort->nValues==0 && lref<sizeof(EncPINRef*) && hdr->flags==0)		// flags alignment???
			{byte *p=(byte*)&ep; p[0]=lref<<1|1; memcpy(p+1,hdr->buf,lref);} else
#endif
		{
			ep=(EncPINRef*)buf; memcpy(ep,hdr,sizeof(uint16_t)+1+PINRef::len(hdr->buf));
			Value *pv=(Value*)getStoredValues(ep);
			for (unsigned i=0; i<sort->nValues; i++,pv++)
				if ((rc=AfyKernel::deserialize(*pv,pos,end,sort->ctx->ses,false))!=RC_OK) break;
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
	RC rc; unsigned r; unsigned nTtlRuns=esFile->runCount(); assert(nTtlRuns>0);
	assert(esFile!=NULL && esRuns==NULL && esOutRun!=NULL);
	
	idx=0; curRun=~0u; pins=NULL; pinMem.truncate(TR_REL_ALL); //Not needed for ext sort
	assert(maxSortMem%esFile->pageLen()==0);

	unsigned maxMerge = (unsigned)(maxSortMem/esFile->pageLen())-1;  assert(maxMerge>1);

	nIns=nTtlRuns>maxMerge?maxMerge:nTtlRuns;	
	if ((esRuns=new(ctx->ses) InRun[nIns])==NULL) return RC_NOMEM;
	for (r=0;r<nIns;r++) {rc=esRuns[r].init(esFile,this); if (rc!=RC_OK) return rc;}

	unsigned firstLiveRun=0; unsigned nActiveRuns=nTtlRuns;
	for (;;) {
		if (nActiveRuns>maxMerge) {
			//intermediate merges to reduce number of runs
			unsigned npass = nActiveRuns/maxMerge; unsigned extra = nActiveRuns%maxMerge;
			for (unsigned p=0; p<npass; p++) {
#if TESTES
				report(MSG_DEBUG,"Pass %x merges to target %x\n",p,firstLiveRun+nActiveRuns+p+1);
#endif
				for (r=0;r<maxMerge;r++){
					unsigned runid=firstLiveRun+p*maxMerge+r;
					esRuns[r].setRun(runid,true);
				}
				rc=esOutRun->beginRun(); if(rc!=RC_OK) return rc;
				for (;;) {
					const EncPINRef *minep=NULL; unsigned champ=nIns;
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

RC Sort::esnext(PINx& qr)
{
	const EncPINRef *ep=NULL; unsigned r;
	if (curRun!=~0u) esRuns[curRun].next();
	for (r=0,curRun=~0u; r<nIns; r++) {
		const EncPINRef *cand=esRuns[r].top();
		if (cand!=NULL && (ep==NULL || cmp(ep,cand)>0)) {ep=cand; curRun=r;}
	}
	if (ep==NULL) return RC_EOF;					//All runs exhausted
#if defined(__x86_64__) || defined(IA64) || defined(_M_X64) || defined(_M_IA64)
	if ((((ptrdiff_t)ep)&1)!=0) {
		qr.epr.flags=0; memcpy(qr.epr.buf,(byte*)&ep+1,byte((ptrdiff_t)ep)>>1);
	} else
#endif
	memcpy(&qr.epr,ep,ep->trunc<DEFAULT_ALIGN>());
	return RC_OK;
}

RC Sort::writeRun(unsigned nRunPins,size_t& mUsed)
{
	if (esFile==NULL) esFile=new(ctx->ses) ExtSortFile(ctx->ses);
	if (esOutRun==NULL) esOutRun=new(ctx->ses) OutRun(esFile,nValues);

	// dump sorted pins into run
	esOutRun->beginRun(); RC rc;
	for (unsigned k=0; k<nRunPins; k++) if ((rc=esOutRun->add(pins[k]))!=RC_OK) return rc;
	esOutRun->term(); mUsed=esFile->pageLen();
	return RC_OK;
}

void Sort::esCleanup()
{
	if (esOutRun!=NULL) {delete esOutRun; esOutRun=NULL;}
	if (esRuns!=NULL) {delete[] esRuns; esRuns=NULL;}
	if (esFile!=NULL) {delete esFile; esFile=NULL;}
}

RC Sort::init()
{
	RC rc; assert(memUsed==0 && nAllPins==0 && pins==NULL && esRuns==NULL);
//	size_t minMem=ctx->ses->getStore()->bufMgr->getPageSize()*4; peakSortMem=max(peak,minMem); maxSortMem=max(avg,minMem);
	if ((rc=sort())!=RC_OK||(rc=ctx->ses->testAbortQ())!=RC_OK) {state|=QST_EOF; return rc;}
	idx=0; if (nAllPins==0) return RC_EOF;
	if (nSkip!=0) {
		if (nSkip+1>=nAllPins) state|=QST_EOF;
		if (nSkip>=nAllPins) {nSkip-=(unsigned)nAllPins; idx=nAllPins-1; return RC_EOF;}
		if (pins!=NULL) idx=nSkip;
		else {
			PINx dm(ctx->ses); assert(esFile!=NULL);
			for(unsigned i=0; i<nSkip; i++) {
				if ((rc=esnext(dm))!=RC_OK||(rc=ctx->ses->testAbortQ())!=RC_OK) return rc;
				dm.cleanup();
			}
		}
		nSkip=0;
	}
	state|=nAllPins<=1?QST_EOF:0; return RC_OK;
}

RC Sort::advance(const PINx *skip)
{
	RC rc=RC_OK; if (res!=NULL) {res->cleanup(); *res=PIN::noPID;}
	assert(idx<nAllPins);
	if ((state&QST_BOF)!=0) state&=~QST_BOF; else idx++;
	if (idx+1>=nAllPins) state|=QST_EOF;
	else if (skip!=NULL) {
		if (esRuns!=NULL) {
			// ???
#if 0
		} else if (skip->type==VT_REFID) for (unsigned n=nAllPins-idx; n!=0; ) {
			unsigned k=n>>1; const SPID *sp=pins[k+idx]; int cmp=cmpSort(sp->id,skip->id);
			if (cmp==0) {idx+=k; break;} else if (cmp<0) {idx+=k+1; n-=k+1;} else n=k;
#endif
		} else {
			//???
		}
	}
	if (pins!=NULL) loadEPR(pins[idx],*res);
	else if (esRuns==NULL) return RC_EOF;
	else if ((rc=esnext(*res))!=RC_OK) return rc;
	if ((res->epr.flags&PINEX_DERIVED)!=0) *res=PIN::noPID;
	return ctx->ses->testAbortQ();
}

RC Sort::rewind()
{
	RC rc; idx=0;
	if (esRuns!=NULL) for (unsigned i=0; i<nIns; i++) if ((rc=esRuns[i].rewind())!=RC_OK) return rc;
	state|=QST_BOF; if (nAllPins!=0) state&=~QST_EOF; return RC_OK;
}

RC Sort::count(uint64_t& cnt,unsigned nAbort)
{
	if (queryOp!=NULL) {
		RC rc;
		if ((qflags&QO_VUNIQUE)!=0 || (qflags&QO_UNIQUE)!=0 && (queryOp->getQFlags()&QO_UNIQUE)==0) {
			if ((rc=sort(nAbort))!=RC_OK) {cnt=~0ULL; return rc;}
		} else {
			bool fCheck=false;
			for (unsigned i=0; i<nSegs && index[i]!=~0u; i++) if ((sortSegs[i].flags&(ORD_NULLS_BEFORE|ORD_NULLS_AFTER))==0) {fCheck=true; break;}
			if (!fCheck) return queryOp->count(cnt,nAbort);

			PINx qr(ctx->ses); RC rc=RC_OK,rc2; nAllPins=0; Value *vals=NULL; StackAlloc::SubMark mrk; pinMem.mark(mrk);
			unsigned nqr=queryOp->getNOuts(); PINx **pqr=(PINx**)alloca(nqr*sizeof(PINx*)); if (pqr==NULL) return RC_NOMEM;
			for (unsigned i=1; i<nqr; i++) if ((pqr[i]=new(ctx->ma) PINx(ctx->ses))==NULL) return RC_NOMEM;
			pqr[0]=&qr; queryOp->connect(pqr,nqr);
			for (; rc==RC_OK && (rc=queryOp->next())==RC_OK; qr.cleanup()) {
				if ((rc2=ctx->ses->testAbortQ())!=RC_OK) {rc=rc2; break;}
				if (qr.epr.buf[0]==0 && qr.getPID().isPID() && (rc2=qr.pack())!=RC_OK) {rc=rc2; break;}
				if (nAbort!=~0u && nAllPins+1>nAbort) {rc=RC_TIMEOUT; break;}
				if (vals==NULL) {
					EncPINRef *ep=storeValues(qr,nValues,pinMem); if (ep==NULL) {cnt=~0ULL; return RC_NOMEM;}
					vals=(Value*)getStoredValues(ep); pinMem.mark(mrk);
				}
				if (nPls!=0 && (rc=getData(pqr,nqr,pls,nPls,vals,&pinMem))!=RC_OK) break;
				bool fSkip=true; PIN *pp=&qr;
				for (unsigned i=0; fSkip && i<nSegs; i++) {
					assert(index[i]!=~0u); Value *pv=&vals[index[i]];
					if ((sortSegs[i].flags&ORDER_EXPR)!=0 && (rc=sortSegs[i].expr->eval(*pv,EvalCtx(ctx->ses,NULL,0,&pp,1)))!=RC_OK) 
						{if (rc==RC_NOMEM) {cnt=~0ULL; return rc;} else continue;}
					if (!pv->isEmpty()) fSkip=false;
				}
				if (!fSkip) nAllPins++; pinMem.truncate(TR_REL_ALL,&mrk);
			}
			for (unsigned i=1; i<nqr; i++) pqr[i]->~PINx(); pinMem.truncate(TR_REL_ALL);
		}
	}
	cnt=nAllPins; return RC_OK;
}

RC Sort::loadData(PINx& qr,Value *pv,unsigned nv,ElementID,bool fSort,MemAlloc *ma)
{
	if (nValues==0) return RC_NOTFOUND;
	RC rc=RC_OK; assert(pins!=NULL || curRun<nIns);
	const Value *vals=getStoredValues(pins!=NULL?pins[idx]:esRuns[curRun].top());
	if (vals==NULL) rc=RC_NOTFOUND;
	else if (pv==NULL || nv==0) qr.setProps(vals,nValues);
	else if (fSort) for (unsigned i=0,j=min((unsigned)nv,nSegs); i<j; i++) {if ((rc=copyV(vals[index[i]],pv[i],ctx->ses))!=RC_OK) break;}
	else if (nPls==0) rc=RC_NOTFOUND;
	else for (unsigned i=0; rc==RC_OK && i<nv; i++) for (unsigned j=0; ;j++)	// BS::find???
		if (j>=pls[0].nProps) {pv[i].setError(pv[i].property); break;}
		else if (pls[0].props[j]==pv[i].property) {
			if (ma==NULL) {pv[i]=vals[j]; setHT(pv[i]);} else rc=copyV(vals[j],pv[i],ma);
			break;
		}
	qr.setPartial();
	return rc;
}

const Value *Sort::getvalues() const
{
	return getStoredValues(pins!=NULL?pins[idx]:esRuns[curRun].top());
}

void Sort::unique(bool f)
{
	if (f) qflags|=QO_UNIQUE; else {qflags&=~QO_UNIQUE; if (queryOp!=NULL && nValues!=0) queryOp->unique(f);}
}

void Sort::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("sort: ",6);
	if (nSegs==0) buf.renderName(PROP_SPEC_PINID);
	else for (unsigned i=0; i<nSegs; i++) {
		const OrderSegQ &os=sortSegs[i]; if (i!=0) buf.append(", ",2);
		if ((os.flags&ORDER_EXPR)!=0) os.expr->render(0,buf);
		else buf.renderName(os.pid);
		// flags!!!
		// var
	}
	buf.append("\n",1); if (queryOp!=NULL) queryOp->print(buf,level+1);
}
