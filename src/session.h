/**************************************************************************************

Copyright © 2004-2013 GoPivotal, Inc. All rights reserved.

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

/**
 * StoreCtx and Session class definitions
 */
#ifndef _SESSION_H_
#define _SESSION_H_

#include "utils.h"
#include "pagemgr.h"
#include "storecb.h"

using namespace Afy;

namespace AfyKernel
{

/**
 * class forward definitions
 */
class	Session;
class	PBlock;
class	ClassCreate;
class	ClassPropIndex;
class	ServiceCtx;
class	ServiceTab;
class	PIN;

/**
 * processor capabilities flags
 */

#define PRCF_CMPXCHG16B		0x00000001
#define PRCF_AESNI			0x00000002

struct ProcFlags
{
	uint32_t flg;
	ProcFlags();
	static ProcFlags pf;
};

/**
 * memory heap initialization connstants
 */
#define	SESSION_START_MEM	0x20000
#define	STORE_START_MEM		0x4000
#define	STORE_NEW_MEM		0x20000

#define	STORE_HANDLERTAB_SIZE	100

struct ServiceProvider
{
	const	uint32_t		id;
	HChain<ServiceProvider>	hash;
	IService	*const		handler;
	ServiceProvider			*stack;

	ServiceProvider(uint32_t ui,IService *hndlr,ServiceProvider *stk) : id(ui),hash(this),handler(hndlr),stack(stk) {}
	uint32_t	getKey()	const {return id;}
};

typedef SyncHashTab<ServiceProvider,uint32_t,&ServiceProvider::hash>	ServiceProviderTab;

struct ListenerNotificationHolder
{
	ListenerNotificationHolder	*next;
	IListenerNotification		*const	lnot;
	ListenerNotificationHolder(IListenerNotification *ln) : next(NULL),lnot(ln) {}
};

/**
 * store reference
 * indirect referencing with reference counting is used to prevent request crash during asynchronous store shutdown
 */
struct StoreRef
{
	class	StoreCtx&	ctx;
	volatile	long	cnt;
	SharedCounter		cntRef;
	StoreRef(class StoreCtx& ct) : ctx(ct),cnt(0) {++cntRef;}
};

/**
 * StoreCtx - main store state descriptor
 * contains references to various managers
 */
class StoreCtx : public MemAlloc, public IAffinity
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
	class	NamedMgr			*namedMgr;
	class	NetMgr				*netMgr;
	class	PINPageMgr			*heapMgr;
	class	HeapDirMgr			*hdirMgr;
	class	SSVPageMgr			*ssvMgr;
	class	TreePageMgr			*trpgMgr;
	class	TreeMgr				*treeMgr;
	class	QueryPrc			*queryMgr;
	class	TxMgr				*txMgr;
	class	BigMgr				*bigMgr;
	class	TimerQ				*tqMgr;
	class	FSMMgr				*fsmMgr;

	StrKey						directory;
	HChain<StoreCtx>			list;

	const	unsigned			mode;
	long	volatile			sesCnt;
	SharedCounter				nSessions;
	static	SharedCounter		nStores;


	ushort						storeID;
	uint32_t					bufSize;
	uint32_t					keyPrefix;

	IService					*defaultService;
	ServiceProviderTab			*serviceProviderTab;
	ListenerNotificationHolder	*lstNotif;

	PageMgr						*pageMgrTab[PGID_ALL];

	struct	StoreCB				*theCB;
	struct	StoreCB				*theCBEnc;
	LSN							cbLSN;
	RWLock						cbLock;

	byte						encKey0[ENC_KEY_SIZE];
	byte						HMACKey0[HMAC_KEY_SIZE];

	byte						encKey[ENC_KEY_SIZE];
	byte						pubKey[SHA_DIGEST_BYTES];
	byte						HMACKey[HMAC_KEY_SIZE];
	bool						fEncrypted;
	bool						fHMAC;
	
	volatile	long			state;
	MemAlloc					*mem;
	Mutex						memLock;
	StoreRef					*ref;
	
	QName						*qNames;
	unsigned					nQNames;


public:
	StoreCtx(unsigned md) : fLocked(false),bufMgr(NULL),classMgr(NULL),cryptoMgr(NULL),fileMgr(NULL),
		fsMgr(NULL),ftMgr(NULL),lockMgr(NULL),logMgr(NULL),uriMgr(NULL),identMgr(NULL),namedMgr(NULL),netMgr(NULL),heapMgr(NULL),
		hdirMgr(NULL),ssvMgr(NULL),trpgMgr(NULL),treeMgr(NULL),queryMgr(NULL),txMgr(NULL),bigMgr(NULL),tqMgr(NULL),fsmMgr(NULL),
		list(this),mode(md),sesCnt(0),storeID(0),bufSize(0),keyPrefix(0),defaultService(NULL),serviceProviderTab(NULL),lstNotif(NULL),
		theCB(NULL),theCBEnc(NULL),cbLSN(0),fEncrypted(false),fHMAC(false),state(0),mem(NULL),ref(NULL),qNames(NULL),nQNames(0) {
			memset(pageMgrTab,0,sizeof(pageMgrTab));
			memset(encKey0,0,sizeof(encKey0)); memset(HMACKey0,0,sizeof(HMACKey0));
			memset(encKey,0,sizeof(encKey)); memset(HMACKey,0,sizeof(HMACKey));
	}
	~StoreCtx();
	void						operator delete(void *p);
	const	StrKey&				getKey() const {return directory;}
	static	StoreCtx			*createCtx(unsigned,const char *dir,bool fNew=false);
	static	StoreCtx			*get() {return (StoreCtx*)storeTls.get();}
	void						set() {storeTls.set(this);}
	bool						isServerLocked() const {return fLocked || theCB->state==SST_NO_SHUTDOWN;}
	const	byte				*getEncKey() const {return fEncrypted?encKey:NULL;}
	const	byte				*getHMACKey() const {return HMACKey;}
	uint32_t					getPrefix() const {return keyPrefix;}
	static	uint32_t			genPrefix(ushort storeID) {return uint32_t(byte(storeID>>8)^byte(storeID))<<24;}
	char						*getDirString(const char *d,bool fRel=false);
	const	char				*getDirectory() const {return directory;}
	void						registerPageMgr(PGID pgid,PageMgr *pageMgr) {assert(pgid<PGID_ALL && pageMgrTab[pgid]==NULL); pageMgrTab[pgid]=pageMgr;}
	PageMgr						*getPageMgr(PGID) const;
	bool						isInit() const;
	LSN							getOldLSN() const;
	bool						inShutdown() const;
	bool						isReadOnly() const;
	RC							initBuiltinServices();
	void						setState(unsigned bits) {for (long st=state; (st&bits)!=bits && !cas(&state,st,long(st|bits)); st=state);}
	static unsigned				getNStores() {return nStores;}
	unsigned					getNSessions() const {return nSessions;}
	void*						malloc(size_t s);
	void*						memalign(size_t a,size_t s);
	void*						realloc(void *p,size_t s,size_t old=0);
	void						free(void *p);
	HEAP_TYPE					getAType() const;
	void						release();

	ISession					*startSession(const char *identityName=NULL,const char *password=NULL);
	unsigned					getState() const;
	size_t						getPublicKey(uint8_t *buf,size_t lbuf,bool fB64=false);
	RC							registerLangExtension(const char *langID,IStoreLang *ext,URIID *pID=NULL);
	RC							registerLangExtension(URIID uid,IStoreLang *ext);
	RC							registerService(const char *sname,IService *handler,URIID *puid=NULL,IListenerNotification *lnot=NULL);
	RC							registerService(URIID serviceID,IService *handler,IListenerNotification *lnot=NULL);
	RC							unregisterService(const char *sname,IService *handler=NULL);
	RC							unregisterService(URIID serviceID,IService *handler=NULL);
	void						shutdownServices();
	RC							registerSocket(IAfySocket *sock);
	void						unregisterSocket(IAfySocket *sock);
	RC							registerPrefix(const char *qs,size_t lq,const char *str,size_t ls);
	RC							initStaticService(const char *path,size_t l,Session *ses,const Value *pars,unsigned nPars,bool fNew);
	RC							shutdown();
};

typedef SyncHashTab<StoreCtx,const StrKey&,&StoreCtx::list> StoreTable;
extern	StoreTable	*volatile storeTable;

/**
 * transaftion and locking releated structures and constants
 */

typedef	uint64_t		TXCID;			/**< snapshot ID type */

#define INVALID_TXID	TXID(0)			/**< invalid transaction ID */
#define	NO_TXCID		TXCID(~0ULL)	/**< snapshot is not set */

/**
 * transaction state enumeration
 */
enum TXState {TX_NOTRAN, TX_START, TX_ACTIVE, TX_ABORTING, TX_PREPARE, TX_COMMITTING, TX_ABORTED, TX_COMMITTED, TX_ALL};

/**
 * lock types for transaction-level locks
 */
enum LockType {LOCK_IS,LOCK_IX,LOCK_SHARED,LOCK_SIX,LOCK_UPDATE,LOCK_EXCLUSIVE,LOCK_ALL};

/**
 * transaction flags
 */
#define	TX_READONLY		0x80000000		/**< read-only transaction */
#define	TX_WASINLIST	0x40000000		/**< transaction was in list of transaction when MiniTx was started (see txmgr.h) */
#define	TX_INTERNAL		0x20000000		/**< internal sub-transaction */
#define	TX_SYS			0x10000000		/**< system-level transaction */
#define	TX_GSYS			0x08000000		/**< global system-level transaction */
#define	TX_ATOMIC		0x04000000		/**< atomic trsansaction, e.g. one PIN change */
#define	TX_READLOCKS	0x02000000		/**< transaction uses read locks */
#define	TX_UNCOMMITTED	0x01000000		/**< uncommitted-read isolation level transaction */
#define	TX_IATOMIC		0x00800000		/**< index atomic trsancasction */

#define	S_REPLICATION	0x00000001		/**< session is a replication input session */
#define	S_INSERT		0x00000002		/**< inserts are allowed for this identity */
#define	S_RESTORE		0x00000004		/**< 'restore from logs' mode */

#define	CPREFIX_MASK	0xFF000000

#define	MAX_SUBTX_ID	0x00FFFFFF		/**< maximum number of subtransactions in a transaction */

#define	INITLATCHED		32				/**< initial size of session latched pages array */

#define	INSERT_THRSH	16				/**< max number of PINs inserted into 'old' pages in a transaction */

#define	MAX_SERV_CTX	32				/**<< max number of cached service stacks */

struct	GrantedLock;
struct	LockHdr;

/**
 * header for the list of locks acquired by this transaction
 */
struct LockReq
{
	Session			*next;
	LockHdr			*lh;
	Session			*back;
	GrantedLock		*gl;
	TIMESTAMP		stamp;
	HChain<Session>	wait; 
	SemData			sem;
	LockType		lt;
	RC				rc;
	LockReq(Session *ses) : next(NULL),lh(NULL),back(NULL),gl(NULL),stamp(0),wait(ses),lt(LOCK_SHARED),rc(RC_OK) {}
};

/**
 * automatic trsansaction rollback for crashed transactions
 */
class TxGuard
{
	Session	*const ses;
public:
	TxGuard(Session *s) : ses(s) {}
	~TxGuard();
};

/**
 * an action to be performed when transaction is committed
 */
struct OnCommit
{
	OnCommit			*next;
	virtual	ClassCreate	*getClass();
	virtual	RC			process(Session *s) = 0;
	virtual	void		destroy(Session *s) = 0;
};

/**
 * data purge type in transaction commit
 */
enum PurgeType
{
	TXP_PIN, TXP_SSV
};

/**
 * data purge operation descriptor
 * created at deletePINs(), executed on commit
 */
struct TxPurge
{
	PageID			pageID;
	uint32_t		range;
	uint32_t		*bmp;
	operator		PageID() const {return pageID;}
	RC				purge(Session *ses);
	class Cmp {
	public: 
		static int cmp(const TxPurge& lp,PageID pid) {return cmp3(lp.pageID,pid);}
		static RC merge(TxPurge& dst,TxPurge& src,MemAlloc *ma);
	};
};

typedef DynOArrayM<TxPurge,PageID,TxPurge::Cmp,16,2>	TxPurgeArr;

/**
 * transaction level descriptor of reusable pages, i.e. pages with enough room for inserts
 */
struct TxReuse
{
	struct ReusePage {
		PageID		pid;
		ushort		space;
		PageIdx		nSlots;
	};
	ReusePage		*pinPages;
	unsigned		nPINPages;
	ReusePage		*ssvPages;
	unsigned		nSSVPages;
	TxReuse() : pinPages(NULL),nPINPages(0),ssvPages(NULL),nSSVPages(0) {}
	~TxReuse() {cleanup();}
	void			operator=(TxReuse& rhs) {pinPages=rhs.pinPages; rhs.pinPages=NULL; nPINPages=rhs.nPINPages; rhs.nPINPages=0; ssvPages=rhs.ssvPages; rhs.ssvPages=NULL; nSSVPages=rhs.nSSVPages; rhs.nSSVPages=0;}
	void			cleanup() {if (pinPages!=NULL) {free(pinPages,SES_HEAP); pinPages=NULL;} if (ssvPages!=NULL) {free(ssvPages,SES_HEAP); ssvPages=NULL;} nPINPages=nSSVPages=0;}
};

class	TxIndex;

/**
 * subtransaction descriptor
 * active subtransactions are represented as a stack
 */
struct SubTx
{
	SubTx		*next;
	Session		*const ses;
	unsigned	subTxID;
	LSN			lastLSN;
	OnCommit	*onCommit;
	TxIndex		*txIndex;
	TxPurgeArr	txPurge;
	PageSet		defHeap;
	PageSet		defClass;
	PageSet		defFree;
	unsigned	nInserted;
	SubAlloc::SubMark rmark;
	SubTx(Session *s);
	~SubTx();
	RC			addToHeap(PageID pid,bool fC) {return fC?defClass+=pid:defHeap+=pid;}
	RC			addToHeap(const PageID *pids,unsigned nPages,bool fC) {return fC?defClass.add(pids,nPages):defHeap.add(pids,nPages);}
	bool		testHeap(PageID pid) {for (SubTx *tx=this; tx!=NULL; tx=tx->next) if (tx->defHeap[pid]) return true; return false;}
	RC			queueForPurge(const PageAddr& addr,PurgeType pt,const void *data);
	void		cleanup();
};

/**
 * latched page descriptor
 */
struct LatchedPage
{
	PBlock		*pb;			/**< page control block reference */
	uint16_t	cntX;			/**< counter of exclusive lock requests */
	uint16_t	cntS;			/**< counter of shared lock requests */
	class	Cmp	{public: static int cmp(const LatchedPage& lp,PageID pid);};
};

/**
 * main session context descriptor
 * implements ISession interface
 */
class Session : public MemAlloc, public ISession
{
	StoreCtx		*ctx;
	MemAlloc		*const mem;

	TXID			txid;
	TXCID			txcid;
	unsigned		txState;
	unsigned		sFlags;
	unsigned		identity;
	HChain<Session>	list;

	LockReq			lockReq;
	GrantedLock		*heldLocks;
	LatchedPage		*latched;
	unsigned		nLatched;
	unsigned		xLatched;
	DLList			latchHolderList;

	LSN				firstLSN;
	LSN				undoNextLSN;
	LSN				flushLSN;
	LSN				sesLSN;
	unsigned		nLogRecs;

	PageAddr		extAddr;

	SubTx			tx;

	unsigned		subTxCnt;
	class MiniTx	*mini;
	TxReuse			reuse;
	unsigned		nTotalIns;
	PageID			xHeapPage;
	PageID			forcedPage;
	RW_LockType		classLocked;
	volatile bool	fAbort;
	SubAlloc		*repl;
	unsigned		itf;

	ServiceTab		*serviceTab;
	ITrace			*iTrace;
	unsigned		traceMode;

	DLList			srvCtx;
	unsigned		nSrvCtx;
	unsigned		xSrvCtx;
	ServiceCtx		*active;

	static	Tls		sessionTls;

					Session(StoreCtx*,MemAlloc *ma);
					~Session();
	void			cleanup();
	RC				attach();
	RC				detach();
	void			set(StoreCtx *ctx);

	bool			login(const char *ident,const char *pwd);
	bool			checkAdmin() {return identity==STORE_OWNER;}

public:
	TIMESTAMP		defExpiration;
	int64_t			tzShift;

public:
	TXState			getTxState() const {return (TXState)(txState&0xFFFF);}
	StoreCtx		*getStore() const {return ctx;}
	TXID			getTXID() const {return txid;}
	unsigned		getSubTxID() const {return tx.subTxID;}
	unsigned		getIdentity() const {return identity;}
	const LSN&		getLastLSN() const {return tx.lastLSN;}
	unsigned		getItf() const {return itf;}
	void			setIdentity(unsigned iid,bool fMayInsert) {identity=iid; if (fMayInsert) sFlags|=S_INSERT;}
	void			setReplication(bool fSet) {if (fSet) sFlags|=S_REPLICATION|S_INSERT; else sFlags&=~(S_REPLICATION|S_INSERT);}
	void			setRestore();
	bool			isReplication() const {return (sFlags&S_REPLICATION)!=0;}
	bool			isRestore() const {return (sFlags&S_RESTORE)!=0;}
	bool			mayInsert() const {return (sFlags&S_INSERT)!=0;}
	void			setExtAddr(const PageAddr& addr) {extAddr=addr;}
	const PageAddr&	getExtAddr() const {return extAddr;}
	void			setDefaultExpiration(TIMESTAMP ts) {defExpiration=ts;}
	TIMESTAMP		getDefaultExpiration() const {return defExpiration;}

	void			resetAbortQ() {fAbort=false;}
	void			abortQ() {fAbort=true;}
	RC				testAbortQ() const {return fAbort?RC_TIMEOUT:RC_OK;}

	RC				listen(Value *vals,unsigned nVals,unsigned mode);
	RC				prepare(ServiceCtx *&srv,const PID& id,const Value *vals,unsigned nVals,unsigned flags);
	RC				createServiceCtx(const Value *vals,unsigned nVals,class IServiceCtx *&sctx,bool fWrite=false,class IListener *lctx=NULL);
	RC				stopListener(URIID lid,bool fSuspend=false);
	RC				resolve(IServiceCtx *sctx,URIID sid,AddrInfo& ai);
	void			removeServiceCtx(const PID& id);
	
	void			setTrace(ITrace *trc) {iTrace=trc;}
	void			changeTraceMode(unsigned mask,bool fReset) {if (fReset) traceMode&=~mask; else traceMode|=mask;}
	unsigned		getTraceMode() const {return traceMode;}
	void			trace(long code,const char *msg,...);

	void*			malloc(size_t s);
	void*			memalign(size_t a,size_t s);
	void*			realloc(void *p,size_t s,size_t old=0);
	void			free(void *p);
	HEAP_TYPE		getAType() const;
	void			release();

	RC				latch(PBlock *pb,unsigned mode);
	bool			relatch(PBlock *pb);
	bool			unlatch(PBlock *pb,unsigned mode);
	bool			hasLatched() const {return nLatched>0;}
	void			releaseLatches(PageID,PageMgr*,bool fX);
	RC				releaseAllLatches();

	bool			inWriteTx() const {return this==NULL?StoreCtx::get()->isInit():(txState&(TX_READONLY|0xFFFF))==TX_ACTIVE||(txState&(TX_READONLY|0xFFFF))==TX_ABORTING;}
	bool			inReadTx() const {return this!=NULL && (txState&(TX_READONLY|0xFFFF))==(TX_READONLY|TX_ACTIVE);}
	bool			isSysTx() const {return this!=NULL && (txState&TX_SYS)!=0;}
	void			abortTx() {if (this!=NULL) txState=txState&~0xFFFF|TX_ABORTING;}
	void			setAtomic() {if ((txState&TX_IATOMIC)!=0) txState|=TX_ATOMIC;}
	void			lockClass(RW_LockType=RW_S_LOCK);
	void			unlockClass();
	RC				pushTx();
	RC				popTx(bool fCommit,bool fAll);
	void			addOnCommit(OnCommit *oc) {assert(oc!=NULL); oc->next=tx.onCommit; tx.onCommit=oc;}

	RC				addToHeap(PageID pid,bool fC) {return tx.addToHeap(pid,fC);}
	RC				addToHeap(const PageID *pids,unsigned nPages,bool fC) {return tx.addToHeap(pids,nPages,fC);}
	RC				queueForPurge(const PageAddr& addr,PurgeType pt=TXP_SSV,const void *data=NULL) {return tx.queueForPurge(addr,pt,data);}

	static	bool	hasPrefix(const char *name,size_t lName);

	static	Session *getSession() {return (Session*)sessionTls.get();}

	static	Session	*createSession(StoreCtx *ctx);
	static	void	terminateSession();

	IAffinity		*getAffinity() const;
	ISession		*clone(const char* =NULL) const;
	RC				attachToCurrentThread();
	RC				detachFromCurrentThread();

	void			setInterfaceMode(unsigned);
	unsigned		getInterfaceMode() const;
	void			terminate();

	RC				mapURIs(unsigned nURIs,URIMap URIs[],const char *URIBase=NULL);
	RC				getURI(uint32_t,char *buf,size_t& lbuf,bool fFull=false);

	IdentityID		getIdentityID(const char *identity);
	RC				impersonate(const char *identity);
	IdentityID		storeIdentity(const char *ident,const char *pwd,bool fMayInsert=true,const unsigned char *cert=NULL,unsigned lcert=0);
	IdentityID		loadIdentity(const char *identity,const unsigned char *pwd,unsigned lPwd,bool fMayInsert=true,const unsigned char *cert=NULL,unsigned lcert=0);
	RC				setInsertPermission(IdentityID,bool fMayInsert=true);
	size_t			getStoreIdentityName(char buf[],size_t lbuf);
	size_t			getIdentityName(IdentityID,char buf[],size_t lbuf);
	size_t			getCertificate(IdentityID,unsigned char buf[],size_t lbuf);
	RC				changePassword(IdentityID,const char *oldPwd,const char *newPwd);
	RC				changeCertificate(IdentityID,const char *pwd,const unsigned char *cert,unsigned lcert);
	RC				changeStoreIdentity(const char *newIdentity);
	IdentityID		getCurrentIdentityID() const;

	unsigned		getStoreID(const PID&);
	unsigned		getLocalStoreID();

	IStmt			*createStmt(STMT_OP=STMT_QUERY,unsigned mode=0);
	IStmt			*createStmt(const char *queryStr,const URIID *ids=NULL,unsigned nids=0,CompilationError *ce=NULL);
	IExprTree		*expr(ExprOp op,unsigned nOperands,const Value *operands,unsigned flags=0);
	IExprTree		*createExprTree(const char *str,const URIID *ids=NULL,unsigned nids=0,CompilationError *ce=NULL);
	IExpr			*createExpr(const char *str,const URIID *ids=NULL,unsigned nids=0,CompilationError *ce=NULL);
	IExpr			*createExtExpr(uint16_t langID,const byte *body,uint32_t lBody,uint16_t flags);
	RC				getTypeName(ValueType type,char buf[],size_t lbuf);
	void			abortQuery();

	RC				execute(const char *str,size_t lstr,char **result=NULL,const URIID *ids=NULL,unsigned nids=0,
							const Value *params=NULL,unsigned nParams=0,CompilationError *ce=NULL,uint64_t *nProcessed=NULL,
							unsigned nProcess=~0u,unsigned nSkip=0);

	RC				createInputStream(IStreamIn *&in,IStreamIn *out=NULL,size_t lbuf=0);

	RC				getClassID(const char *className,ClassID& cid);
	RC				enableClassNotifications(ClassID,unsigned notifications);
	RC				rebuildIndices(const ClassID *cidx=NULL,unsigned nClasses=0);
	RC				rebuildIndexFT();
	RC				createIndexNav(ClassID,IndexNav *&nav);
	RC				listValues(ClassID cid,PropertyID pid,IndexNav *&ven);
	RC				listWords(const char *query,StringEnum *&sen);
	RC				getClassInfo(ClassID,IPIN*&);
	
	IPIN			*getPIN(const PID& id,unsigned=0);
	IPIN			*getPIN(const Value& id,unsigned=0);
	IPIN			*getPINByURI(const char *uri,unsigned mode=0);
	RC				getValues(Value *vals,unsigned nVals,const PID& id);
	RC				getValue(Value& res,const PID& id,PropertyID,ElementID=STORE_COLLECTION_ID);
	RC				getValue(Value& res,const PID& id);
	RC				getPINClasses(ClassID *&clss,unsigned& nclss,const PID& id);
	bool			isCached(const PID& id);
	IBatch			*createBatch();
	IPIN			*createPIN(Value *values=NULL,unsigned nValues=0,unsigned mode=0,const PID *original=NULL);
	RC				createPINAndCommit(PID& res,const Value values[],unsigned nValues,unsigned mode=0,const AllocCtrl* =NULL);
	RC				commitPINs(IPIN * const *newPins,unsigned nNew,unsigned mode=0,const AllocCtrl* =NULL,const Value *params=NULL,unsigned nParams=0,const IntoClass *into=NULL,unsigned nInto=0);
	RC				modifyPIN(const PID& id,const Value *values,unsigned nValues,unsigned mode=0,const ElementID *eids=NULL,unsigned *pNFailed=NULL,const Value *params=NULL,unsigned nParams=0);
	RC				deletePINs(IPIN **pins,unsigned nPins,unsigned mode=0);
	RC				deletePINs(const PID *pids,unsigned nPids,unsigned mode=0);
	RC				undeletePINs(const PID *pids,unsigned nPids);

	RC				startTransaction(TX_TYPE=TXT_READWRITE,TXI_LEVEL=TXI_DEFAULT);
	RC				commit(bool fAll);
	RC				rollback(bool fAll);

	RC				reservePage(uint32_t);

	RC				createMap(const MapElt *elts,unsigned nElts,IMap *&map,bool fCopy=true);
	RC				copyValue(const Value& src,Value& dest);
	RC				copyValues(const Value *src,unsigned nValues,Value *&dest);
	RC				convertValue(const Value& oldValue,Value& newValue,ValueType newType);
	RC				parseValue(const char *p,size_t l,Value& res,CompilationError *ce=NULL);
	RC				parseValues(const char *p,size_t l,Value *&res,unsigned& nValues,CompilationError *ce=NULL,char delimiter=',');
	int				compareValues(const Value& v1,const Value& v2,bool fNCase=false);
	void			freeValues(Value *vals,unsigned nVals);
	void			freeValue(Value& val);

	char			*convToJSON(const Value& v);

	void			setTimeZone(int64_t tz);
	RC				convDateTime(uint64_t ts,char *buf,size_t& l,bool fUTC=true) const;
	RC				convDateTime(uint64_t dt,DateTime& dts,bool fUTC=true) const;
	RC				convDateTime(const DateTime& dts,uint64_t& dt,bool fUTC=true) const;
	RC				strToTimestamp(const char *str,size_t lstr,TIMESTAMP& res) const;
	static	RC		strToNum(const char *str,size_t lstr,Value& res,const char **pend=NULL,bool fInt=false);
	static	RC		strToInterval(const char *str,size_t lstr,int64_t& res);
	static	RC		convInterval(int64_t it,char *buf,size_t& l);
	static	RC		getDTPart(TIMESTAMP dt,unsigned& res,int part);

	RC				normalize(const Value *&pv,uint32_t& nv,unsigned f,ElementID prefix,MemAlloc *ma=NULL);
	RC				checkBuiltinProp(Value &v,TIMESTAMP &ts,bool fInsert=false);
	RC				newPINFlags(unsigned& md,const PID *&orig);

	friend	class	TxMgr;
	friend	class	MiniTx;
	friend	class	TxSP;
	friend	class	TxGuard;
	friend	class	LogMgr;
	friend	class	LockMgr;
	friend	class	LatchHolder;
	friend	class	ServiceCtx;
	friend	class	ThreadGroup;
	friend	class	BufMgr;
	friend	class	FSMgr;
	friend	class	Afy::RWLock;
	friend	class	HeapPageMgr;
	friend	class	PINPageMgr;
	friend	class	SSVPageMgr;
	friend	class	QueryPrc;
	friend	class	SInCtx;
	friend	class	SOutCtx;
	friend	class	Classifier;
	friend	class	FullScan;
	friend	class	Cursor;
	friend	class	Stmt;
	friend	struct	ClassifierHdr;
	friend	class	IOCtl;
	friend	class	StoreCtx;
};

/**
 * latch holder descriptor
 * used for automatic unlatching of conflicting pages
 */
class LatchHolder : public DLList
{
public:
	LatchHolder(Session *ses) {assert(ses!=NULL); insertAfter(&ses->latchHolderList);}
	~LatchHolder()	{remove();}
	virtual	void	releaseLatches(PageID pid,PageMgr*,bool fX) = 0;
	virtual	void	checkNotHeld(PBlock*) = 0;
};

/**
 * convV flags
 */
#define	CV_NOTRUNC	0x0001
#define	CV_NODEREF	0x0002

/**
 * Value cmp flags
 */
#define	CND_SORT		0x80000000
#define	CND_EQ			0x40000000
#define	CND_NE			0x20000000

/**
 * Value helper functions
 */

extern	MemAlloc *createMemAlloc(size_t,bool fMulti);																						/**< new heaps for sessions and stores */
extern	RC		copyV(const Value *from,unsigned nv,Value *&to,MemAlloc *ma);																	/**< copy an array of Value structures */
extern	RC		copyV0(Value& to,MemAlloc *ma);																								/**< deep data copy in one Value */
__forceinline	RC	copyV(const Value& from,Value& to,MemAlloc *ma) {return (to=from).type>=VT_STRING && ma!=NULL?copyV0(to,ma):RC_OK;}		/**< inline: copy Value and check if deep data copy is necessary */
extern	bool	operator==(const Value& lhs, const Value& rhs);																				/**< check equality of 2 Value sturcts */
inline	bool	operator!=(const Value& lhs, const Value& rhs) {return !(lhs==rhs);}														/**< check non-equality of 2 Value structs */
extern	size_t	serSize(const Value& val,bool full=false);																					/**< calculate size of serialized Value */
extern	size_t	serSize(const PID &id);																										/**< calculate size of serialized PIN ID */
extern	byte	*serialize(const Value& val,byte *buf,bool full=false);																		/**< serialize Value to buf */
extern	byte	*serialize(const PID &id,byte *buf);																						/**< serialize PIN ID to buf */
extern	RC		deserialize(Value& val,const byte *&buf,const byte *const ebuf,MemAlloc*,bool,bool full=false);								/**< deserialize Value from buf */
extern	RC		deserialize(PID& id,const byte *&buf,const byte *const ebuf);																/**< deserialize PIN ID from buf */
extern	RC		streamToValue(IStream *str,Value& val,MemAlloc*);																			/**< load a stream to memory and return as a string in Value */
extern	int		cmpNoConv(const Value&,const Value&,unsigned u);																				/**< comparison of 2 Value structs of the same type */
extern	int		cmpConv(const Value&,const Value&,unsigned u,MemAlloc *ma);																	/**< comparison of 2 Value structs of different types with conversion */
extern	bool	testStrNum(const char *s,size_t l,Value& res);																				/**< test if a string is a representation of a number */
extern	RC		convV(const Value& src,Value& dst,ushort type,MemAlloc *ma,unsigned mode=0);												/**< convert Value to another type */
extern	RC		derefValue(const Value& src,Value& dst,Session *ses);																		/**< deference Value (VT_REFID, VT_REFIDPROP, etc.) */
extern	RC		convURL(const Value& src,Value& dst,HEAP_TYPE alloc);																		/**< convert URL */
extern	void	freeV(Value *v,unsigned nv,MemAlloc*);																							/**< free in array of Value structs */
extern	void	freeV0(Value& v);																											/**< free data in Value */
__forceinline	void freeV(Value& v) {if ((v.flags&HEAP_TYPE_MASK)>=SES_HEAP) freeV0(v); v.type=VT_ERROR; v.flags=NO_HEAP; v.fcalc=0;}		/**< inline check if data must be freed */
__forceinline	int	cmp(const Value& arg,const Value& arg2,unsigned u,MemAlloc *ma) {return arg.type==arg2.type?cmpNoConv(arg,arg2,u):cmpConv(arg,arg2,u,ma);}	/**< inline comparison of 2 Value structs */
__forceinline	void setHT(const Value& v,HEAP_TYPE ht=NO_HEAP) {v.flags=v.flags&~HEAP_TYPE_MASK|ht;}										/**< set mem allocation flag in Value structure */
extern	RC		copyPath(const PathSeg *src,unsigned nSegs,PathSeg *&dst,MemAlloc *ma);														/**< copy an array of PathSeg structures */
extern	void	destroyPath(PathSeg *path,unsigned nSegs,MemAlloc *ma);																		/**< destroy an array of PathSeg structures */

extern	RC		convUnits(QualifiedValue& q, Units u);																						/**< conversion between various compatible measurment units */
extern	bool	compatible(QualifiedValue&,QualifiedValue&);																				/**< check if measurment units are compatible */
extern	bool	compatibleMulDiv(Value&,uint16_t units,bool fDiv);																			/**< check if measument units can be mulitplied/divided */
extern	Units	getUnits(const char *suffix,size_t l);																						/**< convert unit suffix to Units enumeration constant */
extern	const char *getUnitName(Units u);																									/**< get measument unit suffix */
extern	const char *getLongUnitName(Units u);																								/**< get full name of unit */

class ValueC : public Value
{
public:
	ValueC() {setEmpty();}
	ValueC(const Value &v,MemAlloc *ma) {RC rc=copyV(v,*this,ma); if (rc!=RC_OK) throw rc;}
	~ValueC() {freeV(*this);}
};

typedef BIN<Value,PropertyID,ValCmp> VBIN;

class MapCmp
{
public:
	__forceinline static int cmp(const MapElt& vm,const Value& key) {return AfyKernel::cmp(vm.key,key,CND_SORT,NULL);}
};

typedef BIN<MapElt,const Value&,MapCmp> MBIN;

class QNameCmp
{
public:
	__forceinline static int cmp(const QName& lhs,const QName& rhs) {int c=memcmp(lhs.qpref,rhs.qpref,lhs.lq<rhs.lq?lhs.lq:rhs.lq); return c!=0?c:cmp3(lhs.lq,rhs.lq);}
};

};

inline void*	operator new(size_t s,AfyKernel::MemAlloc *ma) throw() {return ma->malloc(s);}
inline void*	operator new[](size_t s,AfyKernel::MemAlloc *ma) throw() {return ma->malloc(s);}

#endif
