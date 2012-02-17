/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _STARTUP_H_
#define _STARTUP_H_

#include "mvstore.h"

#define	MVSTOREPREFIX				"affinity"
#define MVSTOREDIR					"affinitydb"
#define	DATAFILESUFFIX				".db"
#define	LOGFILESUFFIX				".txlog"
#define	MASTERFILESUFFIX			".master"
#define	HOME_ENV					"AFFINITY_HOME"

#define	STORE_STD_URI_PREFIX		"http://www.affinitydb.org/builtin/"
#define	STORE_STD_QPREFIX			"afy:"

#define	DEFAULT_MAX_FILES			300
#define	DEFAULT_BLOCK_NUM			~0u
#define DEFAULT_PAGE_SIZE			0x8000		// in bytes
#define DEFAULT_EXTENT_SIZE			0x400		// in pages
#define	DEFAULT_ASYNC_TIMEOUT		30000
#define	DEFAULT_LOGSEG_SIZE			0x1000000	// 16Mb
#define	DEFAULT_LOGBUF_SIZE			0x40000		// 256Kb

#define	STARTUP_FORCE_NEW			0x0001
#define	STARTUP_ARCHIVE_LOGS		0x0002
#define	STARTUP_ROLLFORWARD			0x0004
#define	STARTUP_FORCE_OPEN			0x0008
#define	STARTUP_PRINT_STATS			0x0010
#define	STARTUP_SINGLE_SESSION		0x0020
#define	STARTUP_NO_RECOVERY			0x0040
#define	STARTUP_IN_MEMORY			0x0080
#define	STARTUP_REDUCED_DURABILITY	0x0100
#define	STARTUP_LOG_PREALLOC		0x0200
#define	STARTUP_TOUCH_FILE			0x0400

#define	STARTUP_MODE_DESKTOP		0x0000
#define	STARTUP_MODE_SERVER			0x8000
#define	STARTUP_MODE_CLOUD			0x4000

#define	SSTATE_OPEN					0x0001
#define	SSTATE_READONLY				0x0002
#define	SSTATE_NORESOURCES			0x0004
#define	SSTATE_IN_SHUTDOWN			0x0008
#define	SSTATE_MODIFIED				0x0010

#ifndef _EXP
#ifdef WIN32
#define _EXP _declspec(dllexport)
#else
#define _EXP
#endif
#endif

class IReport
{
public:
	virtual	void	report(void *ns,int level,const char *str,const char *file=0,int lineno=-1) = 0;
	virtual	void*	declareNamespace(char const *) = 0;
};

class IStoreNet
{
public:
	virtual	bool	isOnline() = 0;
	virtual	bool	getPIN(uint64_t pid,unsigned short storeId,MVStore::IdentityID iid,MVStore::IPIN *pin) = 0;
};

class IStoreIO;

class IStoreNotification
{
public:
	enum NotificationEventType {
		NE_PIN_CREATED, NE_PIN_UPDATED, NE_PIN_DELETED, NE_CLASS_INSTANCE_ADDED, NE_CLASS_INSTANCE_REMOVED, NE_CLASS_INSTANCE_CHANGED, NE_PIN_UNDELETE
	};
	enum TxEventType {
		NTX_START, NTX_SAVEPOINT, NTX_COMMIT, NTX_ABORT, NTX_COMMIT_SP, NTX_ABORT_SP
	};
	struct NotificationData {
		MVStore::PropertyID			propID;
		MVStore::ElementID			eid;
		MVStore::ElementID			epos;
		const	MVStore::Value		*oldValue;
		const	MVStore::Value		*newValue;
	};
	struct EventData {
		NotificationEventType		type;
		MVStore::ClassID			cid;
	};
	struct NotificationEvent {
		MVStore::PID				pin;
		const	EventData			*events;
		unsigned					nEvents;
		const	NotificationData	*data;
		unsigned					nData;
		bool						fReplication;
	};
	virtual	void	notify(NotificationEvent *events,unsigned nEvents,uint64_t txid) = 0;
	virtual	void	txNotify(TxEventType,uint64_t txid) = 0;
};

class ILockNotification
{
public:
	enum LockType {LT_SHARED, LT_UPDATE, LT_EXCLUSIVE};
	virtual	RC		beforeWait(MVStore::ISession *ses,const MVStore::PID& id,LockType lt) = 0;
	virtual	void	afterWait(MVStore::ISession *ses,const MVStore::PID& id,LockType lt,RC rc) = 0;
};

struct StartupParameters
{
	unsigned				mode;
	const char				*directory;
	unsigned				maxFiles;
	unsigned				nBuffers;
	unsigned				shutdownAsyncTimeout;
	IStoreNet				*network;
	IStoreNotification		*notification;
	const char				*password;
	const char				*logDirectory;
	IStoreIO				*io;
	ILockNotification		*lockNotification;
	size_t					logBufSize;
	StartupParameters(unsigned md=STARTUP_MODE_DESKTOP,const char *dir=NULL,unsigned xFiles=DEFAULT_MAX_FILES,unsigned nBuf=DEFAULT_BLOCK_NUM,
						unsigned asyncTimeout=DEFAULT_ASYNC_TIMEOUT,IStoreNet *net=NULL,IStoreNotification *notItf=NULL,
						const char *pwd=NULL,const char *logDir=NULL,IStoreIO *pio=NULL,ILockNotification *lno=NULL,size_t lbs=DEFAULT_LOGBUF_SIZE) 
		: mode(md),directory(dir),maxFiles(xFiles),nBuffers(nBuf),shutdownAsyncTimeout(asyncTimeout),
		network(net),notification(notItf),password(pwd),logDirectory(logDir),io(pio),lockNotification(lno),logBufSize(lbs) {}
};

struct StoreCreationParameters
{
	unsigned		nControlRecords;
	unsigned		pageSize;
	unsigned		fileExtentSize;
	const	char	*identity;
	unsigned short	storeId;
	const	char	*password;
	bool			fEncrypted;
	uint64_t		maxSize;
	float			pctFree;
	size_t			logSegSize;
	StoreCreationParameters(unsigned nCtl=0,unsigned lPage=DEFAULT_PAGE_SIZE,
		unsigned extSize=DEFAULT_EXTENT_SIZE,const char *ident=NULL,unsigned short stId=0,const char *pwd=NULL,
									bool fEnc=false,uint64_t xSize=0,float pctF=-1.f,size_t lss=DEFAULT_LOGSEG_SIZE)
		: nControlRecords(nCtl),pageSize(lPage),fileExtentSize(extSize),identity(ident),
		storeId(stId),password(pwd),fEncrypted(fEnc),maxSize(xSize),pctFree(pctF),logSegSize(lss) {}
};

class IMapDir
{
public:
	enum StoreOp {SO_CREATE, SO_OPEN, SO_DELETE, SO_MOVE, SO_LOG};
	virtual	RC	map(StoreOp op,const char *dir,size_t ldir,const char *&mdir,const char **pwd) = 0;
};

extern "C" _EXP RC			manageStores(const char *cmd,size_t lcmd,MVStoreCtx &store,IMapDir *id,const StartupParameters *sp=NULL,MVStore::CompilationError *ce=NULL);

extern "C" _EXP RC			openStore(const StartupParameters& params,MVStoreCtx &store);
extern "C" _EXP RC			createStore(const StoreCreationParameters& create,const StartupParameters& params,MVStoreCtx &store,MVStore::ISession **pLoad=NULL);
extern "C" _EXP RC			shutdownStore(MVStoreCtx store=NULL);
extern "C" _EXP	void		stopThreads();
extern "C" _EXP	RC			getStoreCreationParameters(StoreCreationParameters& params,MVStoreCtx store=NULL);
extern "C" _EXP	unsigned	getVersion();
extern "C" _EXP unsigned	getStoreState(MVStoreCtx=NULL);
extern "C" _EXP void		setReport(IReport *);
extern "C" _EXP	RC			loadLang(const char *path,uint16_t& langID);

enum StreamInType
{
	SITY_NORMAL, SITY_DUMPLOAD, SITY_REPLICATION
};

class IStreamInCallback
{
public:
	virtual	RC		send(const unsigned char *buf,size_t lbuf) = 0;
	virtual	void	close() = 0;
};

struct StreamInParameters
{
	IStreamInCallback	*cb;
	size_t				lBuffer;
	uint64_t			threshold;
	const char			*identity;
	size_t				lIdentity;
	const char			*pwd;
	size_t				lPwd;
};

extern "C" _EXP	RC			createServerInputStream(MVStoreCtx ctx,const StreamInParameters *params,MVStore::IStreamIn *&in,StreamInType=SITY_NORMAL);

#endif
