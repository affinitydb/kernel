/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "ftindex.h"
#include "queryprc.h"
#include "txmgr.h"
#include "blob.h"

using namespace MVStoreKernel;

static const IndexFormat ftIndexFmt(KT_BIN,KT_VARKEY,KT_VARMDPINREFS);

FTIndexMgr::FTIndexMgr(StoreCtx *ct) : ctx(ct),indexFT(MA_FTINDEX,ftIndexFmt,ct,TF_WITHDEL),localeTable(LOCALE_TABLE_SIZE,ct)
{
	FTLocaleInfo *loc = new(ct) FTLocaleInfo(ct,0,"'_",DEFAULT_MINSIZE,new(STORE_HEAP) PorterStemmer,defaultEnglishStopWords,defaultEnglishStopWordsLen());
	if (loc==NULL || loc->stemmer==NULL) throw RC_NORESOURCES;
	localeTable.insert(loc); defaultLocale=loc;
}

FTIndexMgr::~FTIndexMgr()
{
}

FTLocaleInfo::FTLocaleInfo(StoreCtx *ct,ulong ID,const char *nDelim,ulong ms,Stemmer *stm,const char *stopWords[],ulong nStopWords) 
	: list(this),localeID(ID),nonDelim(NULL),lNonDelim(0),stemmer(stm),minSize(ms),stopWordTable(nStopWords/4,ct)
{
	if (nDelim!=NULL) {nonDelim=strdup(nDelim,ct); lNonDelim=strlen(nDelim);}
	for (ulong i=0; i<nStopWords; i++) {StopWord *sw=new(ct) StopWord(StrLen(stopWords[i])); if (sw!=NULL) stopWordTable.insert(sw);}
}

RC FTIndexMgr::index(ChangeInfo& inf,FTList *ftl,ulong flags,ulong mode,MemAlloc *ma)
{
	const Value *newV=inf.newV,*oldV=inf.oldV; ElementID eid=inf.eid; const Value *v; ulong n; RC rc=RC_OK;
	if (oldV!=NULL && (flags&IX_OFT)!=0) {
		inf.newV=NULL;
		switch (oldV->type) {
		case VT_STREAM:
			assert((oldV->flags&VF_SSV)==0);
			if (oldV->stream.is->dataType()==VT_STRING && (newV==NULL||newV->type==VT_ARRAY||newV->type==VT_COLLECTION))
				{if ((rc=index(inf,ftl,mode,ma))!=RC_OK) return rc; inf.oldV=NULL;}
			break;
		case VT_STRING:
			if ((flags&IX_NFT)==0||newV==NULL||newV->type==VT_ARRAY||newV->type==VT_COLLECTION) 
				{if ((rc=index(inf,ftl,mode,ma))!=RC_OK) return rc; inf.oldV=NULL;}
			break;
		case VT_ARRAY:
			for (n=0; n<oldV->length; n++) {
				inf.oldV=v=&oldV->varray[n]; inf.eid=inf.oldV->eid;
				switch (v->type) {
				case VT_STREAM:
					assert((v->flags&VF_SSV)==0);
					if (v->stream.is->dataType()==VT_STRING && (rc=index(inf,ftl,mode,ma))!=RC_OK) return rc;
					break;
				case VT_STRING: if ((rc=index(inf,ftl,mode,ma))!=RC_OK) return rc; break;
				}
			}
			inf.oldV=NULL; break;
		case VT_COLLECTION:
			for (v=oldV->nav->navigate(GO_FIRST); rc==RC_OK && v!=NULL; v=oldV->nav->navigate(GO_NEXT)) {
				inf.oldV=v; inf.eid=v->eid;
				switch (v->type) {
				case VT_STRING: rc=index(inf,ftl,mode|FTMODE_SAVE_WORDS,ma); break;
				case VT_STREAM: 
					assert((v->flags&VF_SSV)==0);
					if (v->stream.is->dataType()==VT_STRING) rc=index(inf,ftl,mode,ma);
					break;
				}
			}
			oldV->nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID); if (rc!=RC_OK) return rc;
			inf.oldV=NULL; break;
		case VT_STRUCT:
			//???
			break;
		}
		inf.newV=newV; inf.eid=eid;
	}
	if (newV!=NULL && (flags&IX_NFT)!=0) {
		if ((flags&IX_OFT)==0) inf.oldV=NULL;
		switch (newV->type) {
		case VT_STREAM: if (newV->stream.is->dataType()==VT_STRING && (rc=index(inf,ftl,mode,ma))!=RC_OK) return rc; break;
		case VT_STRING: if ((rc=index(inf,ftl,mode,ma))!=RC_OK) return rc; break;
		case VT_ARRAY:
			for (n=0; n<newV->length; n++) {
				inf.newV=v=&newV->varray[n]; inf.eid=inf.newV->eid;
				switch (v->type) {
				case VT_STRING: if ((rc=index(inf,ftl,mode,ma))!=RC_OK) return rc; break;
				case VT_STREAM: if (v->stream.is->dataType()==VT_STRING && (rc=index(inf,ftl,mode,ma))!=RC_OK) return rc; break;
				}
			}
			break;
		case VT_COLLECTION:
			for (v=newV->nav->navigate(GO_FIRST); rc==RC_OK && v!=NULL; v=newV->nav->navigate(GO_NEXT)) {
				inf.newV=v; inf.eid=v->eid;
				switch (v->type) {
				case VT_STRING: rc=index(inf,ftl,mode|FTMODE_SAVE_WORDS,ma); break;
				case VT_STREAM: if (v->stream.is->dataType()==VT_STRING) rc=index(inf,ftl,mode,ma); break;
				}
			}
			newV->nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID); 
			if (rc!=RC_OK) return rc; break;
		case VT_STRUCT:
			//???
			break;
		}
		inf.oldV=oldV;
	}
	return RC_OK;
}

// Streams if any must be StreamX

RC FTIndexMgr::index(const ChangeInfo& ft,FTList *ftl,ulong mode,MemAlloc *ma)
{
	Tokenizer *oT=NULL,*nT=NULL; ValueType oType=VT_ERROR,nType=VT_ERROR;
	if (ft.oldV!=NULL) switch (ft.oldV->type) {
	case VT_STRING:
		if (ft.oldV->length>0) {oT=new(alloca(sizeof(StringTokenizer))) StringTokenizer(ft.oldV->str,ft.oldV->length,(mode&FTMODE_SAVE_WORDS)!=0); oType=VT_STRING;}
		break;
	case VT_STREAM:
		if (ft.oldV->stream.is!=NULL) {oT=(Tokenizer*)new(alloca(sizeof(StreamTokenizer))) StreamTokenizer(ft.oldV->stream.is); oType=VT_STREAM;}
		break;
	}
	if (ft.newV!=NULL) switch (ft.newV->type) {
	case VT_STRING:
		if (ft.newV->length>0) {nT=new(alloca(sizeof(StringTokenizer))) StringTokenizer(ft.newV->str,ft.newV->length,(mode&FTMODE_SAVE_WORDS)!=0); nType=VT_STRING;}
		break;
	case VT_STREAM:
		if (ft.newV->stream.is!=NULL) {nT=(Tokenizer*)new(alloca(sizeof(StreamTokenizer))) StreamTokenizer(ft.newV->stream.is); nType=VT_STREAM;}
		break;
	}

	ulong locID = 0;
	// getLocaleID

	char wbuf[FTBUFSIZE],wbuf2[FTBUFSIZE];
	if (oT==NULL) {if (nT==NULL) return RC_OK;}
	else if (nT!=NULL) {
		size_t lo,ln; const char *so,*sn; 
		const FTLocaleInfo *loc = locID==0 ? defaultLocale : localeTable.find(locID);
		do {so=oT->nextToken(lo,loc,wbuf,sizeof(wbuf)); sn=nT->nextToken(ln,loc,wbuf2,sizeof(wbuf2));}
		while (so!=NULL && sn!=NULL && lo==ln && !memcmp(so,sn,lo));
		if (so==NULL) {if (sn==NULL) return RC_OK; oT=NULL;} else oT->restore(so,lo);
		if (sn==NULL) nT=NULL; else nT->restore(sn,ln);
	}

	byte ext[XPINREFSIZE]; byte lext=0;
	if (ftl==NULL) {
		PINRef pr(ctx->storeID,ft.id); pr.def|=PR_U1; pr.u1=ft.propID;
		if (ft.docID.pid!=STORE_INVALID_PID) {pr.def|=PR_PID2; pr.id2=ft.docID;}
		lext=pr.enc(ext);
	}
	const char *pW; size_t lW; FTInfo fti; fti.count=1; RC rc=RC_OK;
	const FTLocaleInfo *loc = locID==0 ? defaultLocale : localeTable.find(locID);
	if (loc==NULL) loc=defaultLocale;
	if (oT!=NULL) while ((pW=oT->nextToken(lW,loc,wbuf,FTBUFSIZE))!=NULL) 
		if (lW>=loc->minSize && ((mode&FTMODE_STOPWORDS)==0 || !loc->isStopWord(StrLen(pW,lW)))) {
			lW=min(lW,size_t(MAX_WORD_SIZE));
			if (loc->stemmer!=NULL) pW=loc->stemmer->process(pW,lW,wbuf);
			if (ftl==NULL) {
				SearchKey key(pW,ushort(lW));
				if ((rc=indexFT.remove(key,ext,lext))!=RC_OK) {
					report(MSG_ERROR,"FT del failed, key: %.*s, prop: %d, eid:%08X, id: "_LX_FM", rc:%d\n",lW,pW,ft.propID,ft.eid,ft.id.pid,rc);
					if (rc!=RC_NOTFOUND) break;		//????????
				}
			} else if (/*(char*)pW!=wbuf && !oT->saveCopy() || */(pW=(const char*)ftl->store(pW,lW))!=NULL) {
				fti.op=OP_DELETE; fti.word=pW; fti.lw=lW; fti.propID=ft.propID; if ((rc=ftl->add(fti))!=RC_OK) break;
			}
		}
	if (nT!=NULL) while ((pW=nT->nextToken(lW,loc,wbuf,FTBUFSIZE))!=NULL)
		if (lW>=loc->minSize && ((mode&FTMODE_STOPWORDS)==0 || !loc->isStopWord(StrLen(pW,lW)))) {
			lW=min(lW,size_t(MAX_WORD_SIZE));
			if (loc->stemmer!=NULL) pW=loc->stemmer->process(pW,lW,wbuf);
			if (ftl==NULL) {
				SearchKey key(pW,ushort(lW));
				if ((rc=indexFT.insert(key,ext,lext))!=RC_OK) {
					report(MSG_ERROR,"FT ins failed, key: %.*s, prop: %d, eid:%08X, id: "_LX_FM", rc:%d\n",lW,pW,ft.propID,ft.eid,ft.id.pid,rc);
					break;
				}
			} else if (/*(char*)pW!=wbuf && !nT->saveCopy() ||*/ (pW=(const char*)ftl->store(pW,lW))!=NULL) {
				fti.op=OP_ADD; fti.word=pW; fti.lw=lW; fti.propID=ft.propID; if ((rc=ftl->add(fti))!=RC_OK) break;
			}
		}
	return rc;
}

RC FTIndexMgr::process(FTList& ftl,const PID& id,const PID& doc)
{
	const FTInfo *fti; RC rc=RC_OK; byte ext[XPINREFSIZE]; byte lext=0;
	PropertyID prev=STORE_INVALID_PROPID; long prevCnt=-1l;
	if (ftl.getCount()!=0) for (ftl.start(); (fti=ftl.next())!=NULL; ) {
		SearchKey key(fti->word,ushort(min(fti->lw,size_t(MAX_WORD_SIZE))));
		if (fti->propID!=prev || fti->count!=prevCnt) {
			PINRef pr(ctx->storeID,id); pr.def|=PR_U1; pr.u1=fti->propID;
			if (doc.pid!=STORE_INVALID_PID) {pr.def|=PR_PID2; pr.id2=doc;}
			if (fti->count!=1) {pr.def|=PR_COUNT; pr.count=fti->count;}
			prev=fti->propID; prevCnt=fti->count; lext=pr.enc(ext);
		}
		switch (fti->op) {
		default: assert(0);
		case OP_ADD: rc=indexFT.insert(key,ext,lext); break;
		case OP_DELETE: rc=indexFT.remove(key,ext,lext); break;
		}
		if (rc!=RC_OK) {
			report(MSG_ERROR,"FT %s failed, key: %.*s, id: "_LX_FM", rc:%d\n",fti->op==OP_ADD?"add":"del",fti->lw,fti->word,id.pid,rc);
			if (fti->op!=OP_DELETE || rc!=RC_NOTFOUND) break;
		}
	}
	return rc;
}

const FTLocaleInfo *FTIndexMgr::getLocale() const
{
	ulong locID=0;
	// getLocaleID
	const FTLocaleInfo *loc=locID==0 ? defaultLocale : localeTable.find(locID);
	return loc!=NULL?loc:defaultLocale;
}

RC FTIndexMgr::rebuildIndex(Session *ses)
{
	RC rc=RC_OK; MiniTx tx(ses,MTX_FLUSH|MTX_GLOB);
	if ((rc=indexFT.dropTree())==RC_OK) {
		PINEx qr(ses); ses->resetAbortQ();
		FullScan fs(ses,HOH_DELETED|HOH_HIDDEN); fs.connect(&qr); RWLockP lck(&lock,RW_X_LOCK);
		while ((rc=fs.next())==RC_OK) {
#if 0
			assert(!qr.pb.isNull() && qr.hpin!=NULL);
			if ((qr.hpin->hdr.descr&HOH_NOINDEX)!=0) continue;
			SubAlloc sa(ses); FTList ftl(sa); Value v; ctx->queryMgr->loadV(v,PROP_SPEC_DOCUMENT,qr,0,ses);
			ChangeInfo inf={qr.id,v.type==VT_REFID?v.id:PIN::defPID,NULL,&v,STORE_INVALID_PROPID,STORE_COLLECTION_ID};
			const HeapPageMgr::HeapV *hprop=qr.hpin->getPropTab();
			for (ulong k=0; k<qr.hpin->nProps; ++k,++hprop) if ((hprop->type.flags&META_PROP_NOFTINDEX)==0) {
				if ((rc=ctx->queryMgr->loadVTx(v,hprop,qr,LOAD_SSV,NULL))!=RC_OK) break;
				inf.propID=v.property;
				rc=ctx->ftMgr->index(inf,&ftl,IX_NFT,(hprop->type.flags&META_PROP_STOPWORDS)!=0?FTMODE_STOPWORDS:0,ses);
				freeV(v); if (rc!=RC_OK) break;
			}
			if (rc==RC_OK) rc=ctx->ftMgr->process(ftl,inf.id,inf.docID); else break;
#endif
		}
		if (rc==RC_EOF || rc==RC_OK) {tx.ok(); rc=RC_OK;}
	}
	return rc;
}

RC FTIndexMgr::listWords(Session *ses,const char *q,StringEnum *&sen)
{
	size_t l=0; char *s=NULL;
	if (q!=NULL && *q!='\0') {if ((s=(char*)malloc(l=strlen(q),SES_HEAP))!=NULL) memcpy(s,q,l); else return RC_NORESOURCES;}
	sen=new(ses) StringEnumFTScan(ses,s,l,getLocale()); return sen!=NULL?RC_OK:RC_NORESOURCES;
}

StringEnumFTScan::~StringEnumFTScan()
{
	if (scan!=NULL) scan->destroy(); ses->free(str);
}

const char *StringEnumFTScan::next()
{
	for (;;) {
		if (scan==NULL) {
			if (str==NULL) scan=ses->getStore()->ftMgr->getIndexFT().scan(ses,NULL);
			else {
				size_t l=0; const char *tok=stk.nextToken(l,loc,tbuf,sizeof(tbuf));
				if (tok==NULL||l==0) return NULL; new(&word) SearchKey(tok,(ushort)l);
				scan=ses->getStore()->ftMgr->getIndexFT().scan(ses,&word,&word,SCAN_PREFIX);
			}
			if (scan==NULL) return NULL;
		}
		RC rc=scan->nextKey(); scan->release();
		if (rc!=RC_EOF) {
			if (rc!=RC_OK) return NULL;
			const SearchKey& key=scan->getKey();
			if (!scan->hasValues() || key.type!=KT_BIN || key.v.ptr.l>MAX_WORD_SIZE) continue;
			memcpy(buf,key.getPtr2(),key.v.ptr.l); buf[key.v.ptr.l]=0; return buf;
		}
		scan->destroy(); scan=NULL;
	}
}

void StringEnumFTScan::destroy()
{
	delete this;
}

//-------------------------------------------------------------------------------------------

namespace MVStoreKernel
{
	enum TokenizerState {TK_NOWORD, TK_NUMBER, TK_LOWER, TK_WORD, TK_NSTATES, TK_STOP};
	enum TokenizerType {TK_OTHER, TK_DIGIT, TK_LC, TK_UC, TK_NCHARTYPES};
};

const char *StringTokenizer::nextToken(size_t& lToken,const FTLocaleInfo *loc,char *buf,size_t lBuf)
{
	try {
		TokenizerState state=TK_NOWORD; fSave=false; size_t l=0; int lch=0; assert(loc!=NULL);
		while (str<estr) {
			if (state==TK_NOWORD) prev=str;
			byte ch=*str++,ch2; ulong wch=0; TokenizerType ty;
			if ((lch=UTF8::len(ch))>1) {
				const byte *p=(byte*)str; 
				if ((wch=UTF8::decode(ch,p,ulong(estr-str)))==~0u) {
					ty=TK_OTHER; lch=1;
				} else {
					str=(const char*)p;
					if (UTF8::iswdigit(wchar_t(wch))) ty=TK_DIGIT;
					else if (UTF8::iswlalpha(wchar_t(wch))) ty=TK_LC;
					else if (UTF8::iswupper(wchar_t(wch),wch)) ty=TK_UC;
					else ty=TK_OTHER;
				}
			} else if (ch>='0' && ch<='9') ty=TK_DIGIT;
            else if (ch>='a' && ch<='z') ty=TK_LC;
			else if (ch>='A' && ch<='Z') {ty=TK_UC; ch+='a'-'A';}
            else ty=TK_OTHER;
			const static TokenizerState transition[TK_NSTATES][TK_NCHARTYPES] = {
			//	TK_OTHER	TK_DIGIT	TK_LC		TK_UC
				{TK_NOWORD,	TK_NUMBER,	TK_LOWER,	TK_WORD},	//TK_NOWORD
				{TK_STOP,	TK_NUMBER,	TK_STOP,	TK_STOP},	//TK_NUMBER		
				{TK_STOP,	TK_LOWER,	TK_LOWER,	TK_WORD},	//TK_LOWER		
				{TK_STOP,	TK_WORD,	TK_WORD,	TK_WORD},	//TK_WORD
			};
			TokenizerState ts=transition[state][ty];
			if (ts==TK_STOP) {
				if (state==TK_NUMBER || loc->lNonDelim==0 || str>=estr || memchr(loc->nonDelim,ch,loc->lNonDelim)==NULL) {str-=lch; break;}
				if (UTF8::len(ch2=*str)>1) {
					const byte *p=(byte*)str+1; ulong wch2=UTF8::decode(ch2,p,ulong(estr-str-1));
					if (wch2==~0u || !UTF8::iswalnum(wchar_t(wch2))) break;
				} else if ((ch2<'0' || ch2>'9') && (ch2<'a' || ch2>'z') && (ch2<'A' || ch2>'Z')) {str-=lch; break;}
				ts=state;
			}
			if (ts==TK_WORD) {
				if (state!=TK_WORD) {l=min(size_t(str-prev-lch),lBuf); if (l>0) memcpy(buf,prev,l);}
				if (lch==1) {if (l<lBuf) buf[l++]=ch;} else if (l+UTF8::ulen(wch)<=lBuf) l+=UTF8::encode((byte*)buf+l,wch);
			}
			state=ts; lch=0;
		}
		switch (state) {
		default: lToken=0; return NULL;
		case TK_NUMBER: case TK_LOWER: lToken=str-prev; return prev;
		case TK_WORD: assert(l>0); lToken=l; fSave=true; return buf;
		}
	} catch (...) {
		// something bad happened - just return NULL
	}
	return NULL;
}

void StringTokenizer::getData(const char *&s,size_t& ls,const char *&p,size_t& lp,IStream *&is) const
{
	is=NULL; p=NULL; lp=0; if (str>=estr) {s=NULL; ls=0;} else {s=str; ls=estr-str;}
}

bool StringTokenizer::saveCopy() const
{
	return fSave||fSave0;
}

void StringTokenizer::restore(const char *,size_t)
{
	if (prev!=NULL) str=prev;
}

const char *StreamTokenizer::nextToken(size_t& lToken,const FTLocaleInfo *loc,char *buf,size_t lBuf)
{
	try {
		assert(loc!=NULL && is!=NULL && is->dataType()==VT_STRING);
		if (prev!=NULL && lPrev>0) {const char *p=prev; lToken=lPrev; prev=NULL; lPrev=0; return p;}
		TokenizerState state=TK_NOWORD; size_t l=0,ll;
		while (str<estr || estr>=sbuf+sizeof(sbuf) && (estr=(str=sbuf)+is->read(sbuf,sizeof(sbuf)))!=sbuf) {
			byte ch=*str++,ch2; TokenizerType ty; int lch=UTF8::len(ch),lch2; ulong wch=0;
			if (lch>1) {
				if (str+lch>estr) {ll=estr-str; if (ll>0) memcpy(sbuf,str,ll); estr=(str=sbuf)+ll+is->read(sbuf+ll,sizeof(sbuf)-ll);}
				const byte *p=(byte*)str; 
				if ((wch=UTF8::decode(ch,p,ulong(estr-str)))==~0u) {
					ty=TK_OTHER; lch=1;
				} else {
					str=(const char*)p;
					if (UTF8::iswdigit(wchar_t(wch))) ty=TK_DIGIT;
					else if (UTF8::iswlalpha(wchar_t(wch))) ty=TK_LC;
					else if (UTF8::iswupper(wchar_t(wch),wch)) ty=TK_UC;
					else ty=TK_OTHER;
				}
			} else if (ch>='0' && ch<='9') ty=TK_DIGIT;
            else if (ch>='a' && ch<='z') ty=TK_LC;
			else if (ch>='A' && ch<='Z') {ty=TK_UC; ch+='a'-'A';}
            else ty=TK_OTHER;
			const static TokenizerState transition[TK_NSTATES][TK_NCHARTYPES] = {
			//	TK_OTHER	TK_DIGIT	TK_LC		TK_UC
				{TK_NOWORD,	TK_NUMBER,	TK_WORD,	TK_WORD},	//TK_NOWORD
				{TK_STOP,	TK_NUMBER,	TK_STOP,	TK_STOP},	//TK_NUMBER		
				{TK_STOP,	TK_WORD,	TK_WORD,	TK_WORD},	//TK_LOWER		
				{TK_STOP,	TK_WORD,	TK_WORD,	TK_WORD},	//TK_WORD
			};
			TokenizerState ts=transition[state][ty];
			if (ts==TK_STOP) {
				if (state==TK_NUMBER || loc->lNonDelim==0 || memchr(loc->nonDelim,ch,loc->lNonDelim)==NULL) {str-=lch; break;}
				if (str>=estr && (estr<sbuf+sizeof(sbuf) || (estr=(str=sbuf)+is->read(sbuf,sizeof(sbuf)))==sbuf)) break;
				if ((lch2=UTF8::len(ch2=*str))>1) {
					if (str+lch2>estr) {ll=estr-str; if (ll>0) memcpy(sbuf,str,ll); estr=(str=sbuf)+ll+is->read(sbuf+ll,sizeof(sbuf)-ll);}
					const byte *p=(byte*)str+1; ulong wch2=UTF8::decode(ch2,p,ulong(estr-str-1)); 
					if (wch2==~0u || !UTF8::iswalnum(wchar_t(wch2))) break;
				} else if ((ch2<'0' || ch2>'9') && (ch2<'a' || ch2>'z') && (ch2<'A' || ch2>'Z')) break;
				ts=state;
			}
			if (ts!=TK_NOWORD) {
				if (lch==1) {if (l<lBuf) buf[l++]=ch;} else if (l+UTF8::ulen(wch)<=lBuf) l+=UTF8::encode((byte*)buf+l,wch);
			}
			state=ts;
		}
		if (state==TK_NOWORD) {lToken=0; return NULL;}
		lToken=l; return buf;
	} catch (...) {
		// something bad happened - just return NULL
	}
	return NULL;
}

void StreamTokenizer::getData(const char *&s,size_t& ls,const char *&p,size_t& lp,IStream *&stream) const
{
	stream=is; p=prev; lp=lPrev; if (str>=estr) {s=NULL; ls=0;} else {s=str; ls=estr-str;}
}

bool StreamTokenizer::saveCopy() const
{
	return true;
}

void StreamTokenizer::restore(const char *s,size_t ls)
{
	prev=s; lPrev=ls;
}
