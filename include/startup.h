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

#ifndef _STARTUP_H_
#define _STARTUP_H_

#include "affinity.h"

#define	STOREPREFIX					"affinity"										/**< data file prefix */
#define	LOGPREFIX					"afy"											/**< log file prefix */
#define STOREDIR					"affinitydb"									/**< default store file directory */
#define	DATAFILESUFFIX				".db"											/**< data file extension */
#define	LOGFILESUFFIX				".txlog"										/**< log file extension */
#define	MASTERFILESUFFIX			".master"										/**< master record file extension */
#define	HOME_ENV					"AFFINITY_HOME"									/**< environment variable for affinity directory */

#define	STORE_STD_URI_PREFIX		"http://affinitydb.org/builtin/"				/**< URI prefix for built-in class and property names */
#define	STORE_STD_QPREFIX			"afy:"											/**< Qname prefix for built-in class and property names */

#define	DEFAULT_MAX_FILES			300												/**< maximum number of simultaneously open files */
#define	DEFAULT_BLOCK_NUM			~0u							
#define DEFAULT_PAGE_SIZE			0x8000											/**< default page size in bytes */
#define DEFAULT_EXTENT_SIZE			0x400											/**< default extent size in pages */
#define	DEFAULT_ASYNC_TIMEOUT		30000											/**< default timeout for asynchronous operations */
#define	DEFAULT_LOGSEG_SIZE			0x1000000										/**< log segment size in bytes (16Mb) */
#define	DEFAULT_LOGBUF_SIZE			0x40000											/**< log buffer size in bytes (256Kb) */

/**
 * startup flags
 * @see StartupParameters::mode
 */
#define	STARTUP_FORCE_NEW			0x0001											/**< force new store, will delete old one if exists */
#define	STARTUP_ARCHIVE_LOGS		0x0002											/**< don't delete logs until there are marked as archived */
#define	STARTUP_ROLLFORWARD			0x0004											/**< create a new store and roll it forward based on existing log files */
#define	STARTUP_FORCE_OPEN			0x0008											/**< force store open if even recovery fails (for salvaging information) */
#define	STARTUP_PRINT_STATS			0x0010											/**< print index statistics on store shutdown */
#define	STARTUP_SINGLE_SESSION		0x0020											/**< force single session for the store, synchronisation is turned off */
#define	STARTUP_NO_RECOVERY			0x0040											/**< force no-recovery mode: no logs are created */
#define	STARTUP_IN_MEMORY			0x0080											/**< pure 'in-memory' mode, no disk opeartions */
#define	STARTUP_REDUCED_DURABILITY	0x0100											/**< no log flush on transaction commit for improved performance */
#define	STARTUP_LOG_PREALLOC		0x0200											/**< pre-allocate log files */
#define	STARTUP_TOUCH_FILE			0x0400											/**< change file access date if even only read access */

#define	STARTUP_MODE_DESKTOP		0x0000											/**< database is running as a part of a desktop application */
#define	STARTUP_MODE_SERVER			0x8000											/**< database is opened on a server */
#define	STARTUP_MODE_CLOUD			0x4000											/**< in a cloud */

/**
 * current database state
 * @see getStoreState()
 */
#define	SSTATE_OPEN					0x0001											/**< database is loaded, no external access operations */
#define	SSTATE_READONLY				0x0002											/**< only read operations so far */
#define	SSTATE_NORESOURCES			0x0004											/**< critical NORESOURCE situation, e.g. no space for log files; only read access is allowed */
#define	SSTATE_IN_SHUTDOWN			0x0008											/**< database is being shutdown */
#define	SSTATE_MODIFIED				0x0010											/**< data was modified */

/**
 * error/debug information report interface; if not set platform-specific standard report channel is used (e.g. STDERR)
 */
class IReport
{
public:
	virtual	void	report(void *ns,int level,const char *str,const char *file=0,int lineno=-1) = 0;
	virtual	void*	declareNamespace(char const *) = 0;
};

/**
 * network interface for reading remote PINs
 */
class IStoreNet
{
public:
	virtual	bool	isOnline() = 0;
	virtual	bool	getPIN(uint64_t pid,unsigned short storeId,AfyDB::IdentityID iid,AfyDB::IPIN *pin) = 0;
};

class IStoreIO;

/**
 * PIN/class event notification interface
 */
class IStoreNotification
{
public:
	/**
	 * type of event
	 */
	enum NotificationEventType {
		NE_PIN_CREATED, NE_PIN_UPDATED, NE_PIN_DELETED, NE_CLASS_INSTANCE_ADDED, NE_CLASS_INSTANCE_REMOVED, NE_CLASS_INSTANCE_CHANGED, NE_PIN_UNDELETE
	};
	/**
	 * associated transaction state
	 */
	enum TxEventType {
		NTX_START, NTX_SAVEPOINT, NTX_COMMIT, NTX_ABORT, NTX_COMMIT_SP, NTX_ABORT_SP
	};
	/**
	 * event associated data
	 */
	struct NotificationData {
		AfyDB::PropertyID			propID;
		AfyDB::ElementID			eid;
		AfyDB::ElementID			epos;
		const	AfyDB::Value		*oldValue;
		const	AfyDB::Value		*newValue;
	};
	struct EventData {
		NotificationEventType		type;
		AfyDB::ClassID				cid;
	};
	/**
	 * event descriptor
	 */
	struct NotificationEvent {
		AfyDB::PID					pin;
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
	virtual	RC		beforeWait(AfyDB::ISession *ses,const AfyDB::PID& id,LockType lt) = 0;
	virtual	void	afterWait(AfyDB::ISession *ses,const AfyDB::PID& id,LockType lt,RC rc) = 0;
};

/**
 * startup parameters
 * @see openStore, createStore
 */
struct StartupParameters
{
	unsigned				mode;								/**< open flags */
	const char				*directory;							/**< db file directory */
	unsigned				maxFiles;							/**< maximum number of files which can be simultaneously open */
	unsigned				nBuffers;							/**< number of memory buffers for pages to allocate */
	unsigned				shutdownAsyncTimeout;				/**< shutdown wait timeout */
	IStoreNet				*network;							/**< network interface */
	IStoreNotification		*notification;						/**< event notification interface */
	const char				*password;							/**< store owner password */
	const char				*logDirectory;						/**< optional directory for log files */
	IStoreIO				*io;								/**< I/O interface, if not standard (e.g. S3) */
	ILockNotification		*lockNotification;
	size_t					logBufSize;							/**< size of log buffer in memory */
	StartupParameters(unsigned md=STARTUP_MODE_DESKTOP,const char *dir=NULL,unsigned xFiles=DEFAULT_MAX_FILES,unsigned nBuf=DEFAULT_BLOCK_NUM,
						unsigned asyncTimeout=DEFAULT_ASYNC_TIMEOUT,IStoreNet *net=NULL,IStoreNotification *notItf=NULL,
						const char *pwd=NULL,const char *logDir=NULL,IStoreIO *pio=NULL,ILockNotification *lno=NULL,size_t lbs=DEFAULT_LOGBUF_SIZE) 
		: mode(md),directory(dir),maxFiles(xFiles),nBuffers(nBuf),shutdownAsyncTimeout(asyncTimeout),
		network(net),notification(notItf),password(pwd),logDirectory(logDir),io(pio),lockNotification(lno),logBufSize(lbs) {}
};

/**
 * store creation parameters
 * @see createStore
 */
struct StoreCreationParameters
{
	unsigned		nControlRecords;							/**< number of duplicated control records; must be 0 at the moment */
	unsigned		pageSize;									/**< page size in bytes, aligned to 2**N, from 0x1000 to 0x10000 */
	unsigned		fileExtentSize;								/**< file extent size - number of pages to allocate when data file is extended */
	const	char	*identity;									/**< store owner identity */
	unsigned short	storeId;									/**< store ID for this identity; 0-65535, must be unique for this identity */
	const	char	*password;									/**< store owner password */
	bool			fEncrypted;									/**< if true, store is encrypted with AES-128 */
	uint64_t		maxSize;									/**< maximum store size in bytes; for quotas in multi-tenant environments */
	float			pctFree;									/**< percentage of free space on pages when new PINs are allocated */
	size_t			logSegSize;									/**< size of log segment */
	StoreCreationParameters(unsigned nCtl=0,unsigned lPage=DEFAULT_PAGE_SIZE,
		unsigned extSize=DEFAULT_EXTENT_SIZE,const char *ident=NULL,unsigned short stId=0,const char *pwd=NULL,
									bool fEnc=false,uint64_t xSize=0,float pctF=-1.f,size_t lss=DEFAULT_LOGSEG_SIZE)
		: nControlRecords(nCtl),pageSize(lPage),fileExtentSize(extSize),identity(ident),
		storeId(stId),password(pwd),fEncrypted(fEnc),maxSize(xSize),pctFree(pctF),logSegSize(lss) {}
};

/**
 * directory mapping interface
 * @see manageStores()
 */
class IMapDir
{
public:
	enum StoreOp {SO_CREATE, SO_OPEN, SO_DELETE, SO_MOVE, SO_LOG};
	virtual	RC	map(StoreOp op,const char *dir,size_t ldir,const char *&mdir,const char **pwd) = 0;
};

extern "C" AFY_EXP RC			manageStores(const char *cmd,size_t lcmd,AfyDBCtx &store,IMapDir *id,const StartupParameters *sp=NULL,AfyDB::CompilationError *ce=NULL);		/**< store management using PathSQL strings */

extern "C" AFY_EXP RC			openStore(const StartupParameters& params,AfyDBCtx &store);																						/**< opens an existing store, returns store context handle */
extern "C" AFY_EXP RC			createStore(const StoreCreationParameters& create,const StartupParameters& params,AfyDBCtx &store,AfyDB::ISession **pLoad=NULL);				/**< creates a new store, returns store context handle */
extern "C" AFY_EXP RC			shutdownStore(AfyDBCtx store=NULL);																												/**< shutdowns a store */
extern "C" AFY_EXP void			stopThreads();																																	/**< stops all threads started by Affinity kernel */
extern "C" AFY_EXP RC			getStoreCreationParameters(StoreCreationParameters& params,AfyDBCtx store=NULL);																/**< retrives parameters used to create this store */
extern "C" AFY_EXP unsigned		getVersion();																																	/**< get Affinity kernel version */
extern "C" AFY_EXP unsigned		getStoreState(AfyDBCtx=NULL);																													/**< get current store state asynchronously */
extern "C" AFY_EXP void			setReport(IReport *);																															/**< set (kernel-wide) error/debug info interface */
extern "C" AFY_EXP RC			loadLang(const char *path,uint16_t& langID);																									/**< load external langauge library */

/**
 * various types of protobuf streams
 */
enum StreamInType
{
	SITY_NORMAL, SITY_DUMPLOAD, SITY_REPLICATION
};

/**
 * input protobuf stream callback
 */
class IStreamInCallback
{
public:
	virtual	RC		send(const unsigned char *buf,size_t lbuf) = 0;
	virtual	void	close() = 0;
};

/**
 * parameters used to create input protobuf stream object
 */
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

extern "C" AFY_EXP	RC createServerInputStream(AfyDBCtx ctx,const StreamInParameters *params,AfyDB::IStreamIn *&in,StreamInType=SITY_NORMAL);		/**< create input protobuf stream object */

#endif
