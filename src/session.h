/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _SESSION_H_
#define _SESSION_H_

#include "utils.h"
#include "pagemgr.h"
#include "storecb.h"

namespace MVStore {class ISession; class ITrace; struct AllocCtrl; struct QName;};

namespace MVStoreKernel
{

#define	SESSION_START_MEM	0x20000
#define	STORE_START_MEM		0x4000
#define	STORE_NEW_MEM		0x20000

class StoreCtx : public MemAlloc
{
	friend	class				Session;
	static	Tls					storeTls;
			bool				fLocked;
public:
	class	BufMgr				*bufMgr;
	class	Classifier			*classMgr;
	class	CryptoMgr			*cryptoMgr;
	class	FileMgr				*fileMgr;
	class	FSMgr				*fsMgr;
	class	FTIndexMgr			*ftMgr;
	class	LockMgr				*lockMgr;
	class	LogMgr				*logMgr;
	class	URIMgr				*uriMgr;
	class	IdentityMgr			*identMgr;
	class	NetMgr				*netMgr;
	class	PINPageMgr			*heapMgr;
	class	HeapDirMgr			*hdirMgr;
	class	SSVPageMgr			*ssvMgr;
	class	TreePageMgr			*trpgMgr;
	class	TreeMgr				*treeMgr;
	class	QueryPrc			*queryMgr;
	class	TxMgr				*txMgr;
	class	BigMgr				*bigMgr;

	const	ulong				mode;
	long	volatile			sesCnt;

	ushort						storeID;
	uint32_t					bufSize;
	uint32_t					keyPrefix;

	PageMgr						*pageMgrTab[PGID_ALL];

	struct	StoreCB				*theCB;
	struct	StoreCB				*theCBEnc;
	LSN							cbLSN;
	RWLock						cbLock;

	byte						encKey0[ENC_KEY_SIZE];
	byte						HMACKey0[HMAC_KEY_SIZE];

	byte						encKey[ENC_KEY_SIZE];
	byte						HMACKey[HMAC_KEY_SIZE];
	bool						fEncrypted;
	
	volatile	long			state;
	MemAlloc					*mem;
	Mutex						memLock;
	StoreRef					*ref;

	SharedCounter				nSessions;
	static SharedCounter		nStores;

public:
	StoreCtx(ulong md) : fLocked(false),bufMgr(NULL),classMgr(NULL),cryptoMgr(NULL),fileMgr(NULL),
		fsMgr(NULL),ftMgr(NULL),lockMgr(NULL),logMgr(NULL),uriMgr(NULL),identMgr(NULL),netMgr(NULL),
		heapMgr(NULL),hdirMgr(NULL),ssvMgr(NULL),trpgMgr(NULL),treeMgr(NULL),queryMgr(NULL),txMgr(NULL),bigMgr(NULL),mode(md),sesCnt(0),
		storeID(0),bufSize(0),keyPrefix(0),theCB(NULL),theCBEnc(NULL),cbLSN(0),fEncrypted(false),state(0),mem(NULL),ref(NULL) {
			memset(pageMgrTab,0,sizeof(pageMgrTab));
			memset(encKey0,0,sizeof(encKey0)); memset(HMACKey0,0,sizeof(HMACKey0));
			memset(encKey,0,sizeof(encKey)); memset(HMACKey,0,sizeof(HMACKey));
			++nStores;
	}
	~StoreCtx();
	static	StoreCtx			*createCtx(ulong,bool fNew=false);
	static	StoreCtx			*get() {return (StoreCtx*)storeTls.get();}
	void						set() {storeTls.set(this);}
	bool						isServerLocked() const {return fLocked || theCB->state==SST_NO_SHUTDOWN;}
	const	byte				*getEncKey() const {return fEncrypted?encKey:NULL;}
	const	byte				*getHMACKey() const {return HMACKey;}
	uint32_t					getPrefix() const {return keyPrefix;}
	static	uint32_t			genPrefix(ushort storeID) {return uint32_t(byte(storeID>>8)^byte(storeID))<<24;}
	void						registerPageMgr(PGID pgid,PageMgr *pageMgr) {assert(pgid<PGID_ALL && pageMgrTab[pgid]==NULL); pageMgrTab[pgid]=pageMgr;}
	PageMgr						*getPageMgr(PGID) const;
	bool						isInit() const;
	LSN							getOldLSN() const;
	bool						inShutdown() const;
	bool						isReadOnly() const;
	unsigned					getState() const {return (unsigned)state;}
	void						setState(unsigned bits) {for (long st=state; (st&bits)!=bits && !cas(&state,st,long(st|bits)); st=state);}
	static ulong				getNStores() {return nStores;}
	ulong						getNSessions() const {return nSessions;}
	void*						malloc(size_t s);
	void*						memalign(size_t a,size_t s);
	void*						realloc(void *p,size_t s);
	void						free(void *p);
	HEAP_TYPE					getAType() const;
	void						release();
};

typedef	uint64_t		TXCID;

#define INVALID_TXID	TXID(0)
#define	NO_TXCID		TXCID(~0ULL)

enum TXState {TX_NOTRAN, TX_START, TX_ACTIVE, TX_ABORTING, TX_PREPARE, TX_COMMITTING, TX_ABORTED, TX_COMMITTED, TX_ALL};
enum LockType {LOCK_IS,LOCK_IX,LOCK_SHARED,LOCK_SIX,LOCK_UPDATE,LOCK_EXCLUSIVE,LOCK_ALL};

#define	TX_READONLY		0x80000000
#define	TX_WASINLIST	0x40000000
#define	TX_NONOTIFY		0x20000000
#define	TX_SYS			0x10000000
#define	TX_GSYS			0x08000000
#define	TX_ATOMIC		0x04000000
#define	TX_READLOCKS	0x02000000
#define	TX_UNCOMMITTED	0x01000000
#define	TX_IATOMIC		0x00800000

#define	S_REPLICATION	0x00000001
#define	S_INSERT		0x00000002
#define	S_RESTORE		0x00000004

#define	CPREFIX_MASK	0xFF000000

#define	MAX_SUBTX_ID	0x00FFFFFF

#define	MAXLATCHED		32		// required for cascated split in index trees

#define	INSERT_THRSH	16

class	ClassPropIndex;
class	PBlock;

struct GrantedLock;

struct LockReq
{
	class	Session		*next;
	struct	LockHdr		*lh;
	class	Session		*back;
	struct	GrantedLock	*gl;
	TIMESTAMP			stamp;
	HChain<class Session> wait; 
	SemData				sem;
	LockType			lt;
	RC					rc;
	LockReq(Session *ses) : next(NULL),lh(NULL),back(NULL),gl(NULL),stamp(0),wait(ses),lt(LOCK_SHARED),rc(RC_OK) {}
};

class TxGuard
{
	class Session	*const ses;
public:
	TxGuard(class Session *s) : ses(s) {}
	~TxGuard();
};

class TxDelete
{
	friend	struct	SubTx;
	friend	class	TxMgr;
	friend	class	Session;
	TxDelete		*next;
	virtual	RC		deleteData() = 0;
	virtual	void	release() = 0;
};

struct TxReuse
{
	struct ReusePage {
		PageID		pid;
		ushort		space;
		PageIdx		nSlots;
	};
	ReusePage		*pinPages;
	ulong			nPINPages;
	ReusePage		*ssvPages;
	ulong			nSSVPages;
	TxReuse() : pinPages(NULL),nPINPages(0),ssvPages(NULL),nSSVPages(0) {}
	~TxReuse() {cleanup();}
	void			operator=(TxReuse& rhs) {pinPages=rhs.pinPages; rhs.pinPages=NULL; nPINPages=rhs.nPINPages; rhs.nPINPages=0; ssvPages=rhs.ssvPages; rhs.ssvPages=NULL; nSSVPages=rhs.nSSVPages; rhs.nSSVPages=0;}
	void			cleanup() {if (pinPages!=NULL) {free(pinPages,SES_HEAP); pinPages=NULL;} if (ssvPages!=NULL) {free(ssvPages,SES_HEAP); ssvPages=NULL;} nPINPages=nSSVPages=0;}
};

struct TxData
{
	TXCID	txcid;
	TxData	*next;
	DLList	vList;
};

class	TxIndex;
struct	ClassDscr;

struct SubTx
{
	SubTx		*next;
	Session		*const ses;
	ulong		subTxID;
	LSN			lastLSN;
	ClassDscr	*txClass;
	TxIndex		*txIndex;
	TxDelete	*txDelete;
	PageSet		defHeap;
	PageSet		defClass;
	PageSet		defFree;
	ulong		nInserted;
	SubAlloc::SubMark rmark;
	SubTx(Session *s);
	~SubTx();
	void		addTxDelete(TxDelete *td) {if (td!=NULL) {td->next=txDelete; txDelete=td;}}
	RC			addToHeap(PageID pid,bool fC) {return fC?defClass+=pid:defHeap+=pid;}
	RC			addToHeap(const PageID *pids,ulong nPages,bool fC) {return fC?defClass.add(pids,nPages):defHeap.add(pids,nPages);}
	bool		testHeap(PageID pid) {for (SubTx *tx=this; tx!=NULL; tx=tx->next) if (tx->defHeap[pid]) return true; return false;}
	void		cleanup();
};

class ReleaseLatches
{
public: 
	virtual RC release() = 0;
};

class Session : public MemAlloc
{
	TXID			txid;
	TXCID			txcid;
	ulong			txState;
	ulong			sFlags;
	ulong			identity;
	HChain<Session>	list;

	LockReq			lockReq;
	GrantedLock		*heldLocks;
	PBlock			*latched[MAXLATCHED];
	int				nLatched;

	LSN				firstLSN;
	LSN				undoNextLSN;
	LSN				flushLSN;
	LSN				sesLSN;
	ulong			nLogRecs;

	PageAddr		extAddr;

	SubTx			tx;

	ulong			subTxCnt;
	class MiniTx	*mini;
	TxReuse			reuse;
	ulong			nTotalIns;
	PageID			xHeapPage;
	PageID			forcedPage;
	RW_LockType		classLocked;
	volatile bool	fAbort;
	ulong			txil;
	SubAlloc		*repl;
	ReleaseLatches	*rlatch;

	unsigned		itf;
	char			*URIBase;
	size_t			lURIBaseBuf;
	size_t			lURIBase;
	MVStore::QName	*qNames;
	unsigned		nQNames;
	bool			fStdOvr;

	StoreCtx		*ctx;
	MemAlloc		*const mem;
	MVStore::ITrace	*iTrace;
	unsigned		traceMode;

					Session(StoreCtx*,MemAlloc *ma);
					~Session();
	void			cleanup();
	RC				attach();
	RC				detach();
	void			set(StoreCtx *ctx);

	static	Tls		sessionTls;

public:
	TIMESTAMP		defExpiration;
	MVStore::AllocCtrl *allocCtrl;
	int64_t			tzShift;

public:
	TXState			getTxState() const {return (TXState)(txState&0xFFFF);}
	StoreCtx		*getStore() const {return ctx;}
	TXID			getTXID() const {return txid;}
	ulong			getIdentity() const {return identity;}
	const LSN&		getLastLSN() const {return tx.lastLSN;}
	unsigned		getItf() const {return itf;}
	void			setIdentity(ulong iid,bool fMayInsert) {identity=iid; if (fMayInsert) sFlags|=S_INSERT;}
	void			setReplication(bool fSet) {if (fSet) sFlags|=S_REPLICATION|S_INSERT; else sFlags&=~(S_REPLICATION|S_INSERT);}
	ulong			getIsolationLevel() const {return txil;}
	RC				setIsolationLevel(ulong tl) {return (txState&0xFFFF)!=TX_NOTRAN?RC_DEADLOCK:(txil=tl,RC_OK);}
	void			setRestore();
	bool			isReplication() const {return (sFlags&S_REPLICATION)!=0;}
	bool			isRestore() const {return (sFlags&S_RESTORE)!=0;}
	bool			mayInsert() const {return (sFlags&S_INSERT)!=0;}
	void			setExtAddr(const PageAddr& addr) {extAddr=addr;}
	const PageAddr&	getExtAddr() const {return extAddr;}
	void			setDefaultExpiration(TIMESTAMP ts) {defExpiration=ts;}
	TIMESTAMP		getDefaultExpiration() const {return defExpiration;}
	RC				setAllocCtrl(const MVStore::AllocCtrl *ac);
	bool			inWriteTx() const {return this==NULL?StoreCtx::get()->isInit():(txState&(TX_READONLY|0xFFFF))==TX_ACTIVE||(txState&(TX_READONLY|0xFFFF))==TX_ABORTING;}
	bool			inReadTx() const {return this!=NULL && (txState&(TX_READONLY|0xFFFF))==(TX_READONLY|TX_ACTIVE);}
	bool			isSysTx() const {return this!=NULL && (txState&TX_SYS)!=0;}
	void			abortTx() {if (this!=NULL) txState=txState&~0xFFFF|TX_ABORTING;}
	void			setAtomic() {if ((txState&TX_IATOMIC)!=0) txState|=TX_ATOMIC;}
	void			lockClass(RW_LockType=RW_S_LOCK);
	void			unlockClass();
	RC				setBase(const char *s,size_t l);
	RC				setPrefix(const char *qs,size_t lq,const char *str,size_t ls);

	void			resetAbortQ() {fAbort=false;}
	void			abortQ() {fAbort=true;}
	RC				testAbortQ() const {return fAbort?RC_TIMEOUT:RC_OK;}

	void			setTrace(MVStore::ITrace *trc) {iTrace=trc;}
	void			changeTraceMode(unsigned mask,bool fReset) {if (fReset) traceMode&=~mask; else traceMode|=mask;}
	unsigned		getTraceMode() const {return traceMode;}
	void			trace(long code,const char *msg,...);

	void*			malloc(size_t s);
	void*			memalign(size_t a,size_t s);
	void*			realloc(void *p,size_t s);
	void			free(void *p);
	HEAP_TYPE		getAType() const;
	void			release();

	PBlock			*getLatched(PageID pid) const;
	bool			hasLatched() const {return nLatched>0;}
	bool			isLatched(const PBlock *pb) const {for (int i=nLatched; --i>=0; ) if (latched[i]==pb) return true; return false;}
	void			regLatched(PBlock *pb) {assert(!isLatched(pb)&&nLatched<MAXLATCHED); latched[nLatched++]=pb;}
	void			unregLatched(PBlock *pb) {assert(isLatched(pb)); if (latched[--nLatched]!=pb) for (int i=nLatched; --i>=0;)
												if (latched[i]==pb) {do latched[i]=latched[i+1]; while (++i<nLatched); break;}}
	RC				releaseLatches() const {return rlatch!=NULL?rlatch->release():RC_OK;}

	RC				pushTx();
	RC				popTx(bool fCommit,bool fAll);
	void			addTxDelete(TxDelete *td) {tx.addTxDelete(td);}

	RC				addToHeap(PageID pid,bool fC) {return tx.addToHeap(pid,fC);}
	RC				addToHeap(const PageID *pids,ulong nPages,bool fC) {return tx.addToHeap(pids,nPages,fC);}

	RC				replicate(const class PIN *pin);

	static	bool	hasPrefix(const char *name,size_t lName);

	static	Session *getSession() {return (Session*)sessionTls.get();}

	static	Session	*createSession(StoreCtx *ctx);
	static	void	terminateSession();

	friend	class	TxMgr;
	friend	class	MiniTx;
	friend	class	TxSP;
	friend	class	TxGuard;
	friend	class	LogMgr;
	friend	class	LockMgr;
	friend	class	SessionX;
	friend	class	ThreadGroup;
	friend	class	BufMgr;
	friend	class	FSMgr;
	friend	class	RWLock;
	friend	class	HeapPageMgr;
	friend	class	PINPageMgr;
	friend	class	SSVPageMgr;
	friend	class	QueryPrc;
	friend	class	SInCtx;
	friend	class	SOutCtx;
	friend	class	Classifier;
	friend	class	Cursor;
	friend	class	Stmt;
};

};

inline void*	operator new(size_t s,MVStoreKernel::MemAlloc *ma) throw() {return ma->malloc(s);}
inline void*	operator new[](size_t s,MVStoreKernel::MemAlloc *ma) throw() {return ma->malloc(s);}

#endif
