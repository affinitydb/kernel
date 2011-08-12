/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _FTINDEX_H_
#define	_FTINDEX_H_

#include "pinref.h"
#include "idxtree.h"
#include "pgheap.h"

using namespace std;

namespace MVStoreKernel
{

struct ChangeInfo;

#define	MAX_WORD_SIZE			256
#define	LOCALE_TABLE_SIZE		32
#define FTREFSIZE				(sizeof(uint64_t)*2+sizeof(IdentityID)*2+sizeof(uint32_t)*2)
#define	FT_QUEUE_LIMIT			1024
#define	DEFAULT_FTRESULT_SIZE	256
#define	DEFAULT_PHRASE_DEL		'"'
#define	DEFAULT_MINSIZE			2
#define	FTBUFSIZE				256
#define	FTSTRBUFSIZE			512

#define	FTMODE_STOPWORDS		0x0002
#define	FTMODE_SAVE_WORDS		0x0004

struct FTIndexKey
{
	PID				id;
	ulong			propID;
public:
	operator uint32_t() const {return uint32_t(id.pid>>32)^uint32_t(id.pid)^id.ident^propID;}
	bool operator==(const FTIndexKey& key2) const {return id==key2.id && propID==key2.propID;}
	bool operator!=(const FTIndexKey& key2) const {return id!=key2.id || propID!=key2.propID;}
	void operator=(const FTIndexKey& key) {id=key.id; propID=key.propID;}
};

struct StopWord
{
	HChain<StopWord>	list;
	StrLen				word;
	StopWord(const StrLen& wd) : list(this),word(wd) {}
	const StrLen&		getKey() const {return word;}
};

class Stemmer
{
public:
	virtual const char *process(const char *,size_t&,char*) = 0;
};

class PorterStemmer : public Stemmer
{
public:
	const char *process(const char *,size_t&,char*);
};

struct FTLocaleInfo
{
	HChain<FTLocaleInfo>	list;
	ulong					localeID;
	const char				*nonDelim;
	size_t					lNonDelim;
	Stemmer					*stemmer;
	ulong					minSize;
	HashTab<StopWord,const StrLen&,&StopWord::list> stopWordTable;
	// stemming alg/data

	FTLocaleInfo(StoreCtx *ct,ulong ID,const char *nDelim=NULL,ulong ms=DEFAULT_MINSIZE,Stemmer *stm=NULL,const char *stopWords[]=NULL,ulong nStopWords=0);
	bool	isStopWord(const StrLen& word) const {return stopWordTable.find(word)!=NULL;}
	ulong	getKey() const {return localeID;}
};

class Tokenizer
{
public:
	virtual	const char	*nextToken(size_t& lToken,const FTLocaleInfo *loc,char *buf,size_t lBuf) = 0;
	virtual	void		restore(const char *w,size_t lw) = 0;
	virtual	void		getData(const char *&s,size_t& ls,const char *&p,size_t& lp,IStream *&is) const = 0;
	virtual	bool		saveCopy() const = 0;
};

class StringTokenizer : public Tokenizer
{
	const	char			*str;
	const	char	*const	estr;
	const	char			*prev;
	const	bool			fSave0;
	bool					fSave;
public:
	StringTokenizer(const char *s,size_t l,bool fS) : str(s),estr(s+l),prev(NULL),fSave0(fS),fSave(false) {}
	const char	*nextToken(size_t& lToken,const FTLocaleInfo *loc,char *buf,size_t lBuf);
	void		restore(const char *w,size_t lw);
	void		getData(const char *&s,size_t& ls,const char *&p,size_t& lp,IStream *&is) const;
	bool		saveCopy() const;
	bool		isEnd() const {return str==estr;}
};

class StreamTokenizer : public Tokenizer
{
	IStream		*const	is;
	const	char		*str;
	const	char		*estr;
	char				sbuf[FTSTRBUFSIZE];
	const	char		*prev;
	size_t				lPrev;
public:
	StreamTokenizer(IStream *s,const char *st=NULL,size_t ls=0) : is(s),str(sbuf+sizeof(sbuf)),estr(sbuf+sizeof(sbuf)),prev(NULL),lPrev(0) {
		assert(is!=NULL&&is->dataType()==VT_STRING&&ls<=FTSTRBUFSIZE);
		if (st!=NULL && ls>0) memcpy((char*)(str=sbuf+sizeof(sbuf)-ls),st,ls);
	}
	const char	*nextToken(size_t& lToken,const FTLocaleInfo *loc,char *buf,size_t lBuf);
	void		restore(const char *w,size_t lw);
	void		getData(const char *&s,size_t& ls,const char *&p,size_t& lp,IStream *&is) const;
	bool		saveCopy() const;
};

struct FTInfo
{
	PropertyID			propID;
	const char			*word;
	size_t				lw;
	long				count;
	int					op;

	static SListOp compare(const FTInfo& left,FTInfo& right,ulong) {
		int cmp=memcmp(left.word,right.word,min(left.lw,right.lw));
		if (cmp<0 || cmp==0 && (left.lw<right.lw || left.lw==right.lw && left.propID<right.propID)) return SLO_LT;
		if (cmp>0 || cmp==0 && (left.lw>right.lw || left.lw==right.lw && left.propID>right.propID)) return SLO_GT;
		if (left.op==OP_ADD) {
			if (right.op!=OP_DELETE) {assert(right.op==OP_ADD); right.count+=left.count;}
			else if ((right.count-=left.count)<0) {right.count=-right.count; right.op=OP_ADD;}
			else if (right.count==0) return SLO_DELETE;
		} else {
			assert(left.op==OP_DELETE);
			if (right.op!=OP_ADD) {assert(right.op==OP_DELETE); right.count+=left.count;}
			else if ((right.count-=left.count)<0) {right.count=-right.count; right.op=OP_DELETE;}
			else if (right.count==0) return SLO_DELETE;
		}
		return SLO_NOOP;
	}
};

class Session;

class StringEnumFTScan : public StringEnum
{
	Session				*const ses;
	char				*const str;
	const FTLocaleInfo	*const loc;
	StringTokenizer		stk;
	class	TreeScan	*scan;
	SearchKey			word;
	char				buf[MAX_WORD_SIZE+1];
	char				tbuf[MAX_WORD_SIZE+1];
public:
	StringEnumFTScan(Session *se,char *s,size_t l,const FTLocaleInfo *lc) : ses(se),str(s),loc(lc),stk(s,l,false),scan(NULL) {}
	virtual				~StringEnumFTScan();
	const char			*next();
	void				destroy();
	void	operator	delete(void *p) {if (p!=NULL) ((StringEnumFTScan*)p)->ses->free(p);}
};

typedef SList<FTInfo,FTInfo>	FTList;

class FTIndexMgr
{
	StoreCtx	*const		ctx;		
	TreeGlobalRoot			indexFT;
	const	FTLocaleInfo	*defaultLocale;
	HashTab<FTLocaleInfo,ulong,&FTLocaleInfo::list> localeTable;
	RWLock						lock;
	RC				index(const ChangeInfo& ft,FTList *sl=NULL,ulong mode=0,MemAlloc *ma=NULL);
public:
					FTIndexMgr(class StoreCtx *ct);
	virtual			~FTIndexMgr();
	void			*operator new(size_t s,StoreCtx *ctx) {void *p=ctx->malloc(s); if (p==NULL) throw RC_NORESOURCES; return p;}
	Tree&			getIndexFT() {return indexFT;}
	RC				index(ChangeInfo& inf,FTList *sl,ulong flags,ulong mode,MemAlloc *ma);
	RC				process(FTList& ftl,const PID& id,const PID& doc);
	RC				rebuildIndex(Session *ses);
	RC				listWords(Session *ses,const char *q,StringEnum *&sen);
	const FTLocaleInfo	*getLocale() const;
};

extern const char	*defaultEnglishStopWords[];
extern ulong		defaultEnglishStopWordsLen();
extern const char	*stemPorter(const char *word,size_t& len,char *buf,size_t maxLen);

};

#endif
