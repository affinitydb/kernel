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

#ifndef _STARTUP_H_
#define _STARTUP_H_

#include "affinity.h"

using namespace Afy;

#define	STOREPREFIX					"affinity"										/**< data file prefix */
#define	LOGPREFIX					"afy"											/**< log file prefix */
#define STOREDIR					"affinity"										/**< default store file directory */
#define	DATAFILESUFFIX				".store"										/**< data file extension */
#define	LOGFILESUFFIX				".txlog"										/**< log file extension */
#define	MASTERFILESUFFIX			".master"										/**< master record file extension */
#define	HOME_ENV					"AFFINITY_HOME"									/**< environment variable for affinity directory */

#define	DEFAULT_MAX_FILES			300												/**< maximum number of simultaneously open files */
#define	DEFAULT_BLOCK_NUM			~0u							
#define DEFAULT_PAGE_SIZE			0x8000											/**< default page size in bytes */
#define DEFAULT_EXTENT_SIZE			0x400											/**< default extent size in pages */
#define	DEFAULT_ASYNC_TIMEOUT		30000											/**< default timeout for asynchronous operations */
#define	DEFAULT_LOGSEG_SIZE			0x1000000										/**< log segment size in bytes (16Mb) */
#define	DEFAULT_LOGBUF_SIZE			0x40000											/**< log buffer size in bytes (256Kb) */
#define	DEFAULT_MAX_SYNC_ACTION		16												/**< default maximum depth of synchronous actions evaluation stack */
#define	DEFAULT_MAX_ON_COMMIT		1024											/**< default maximum number of actions evaluated at transaction commit */
#define	DEFAULT_MAX_OBJ_SESSION		256												/**< default maximum number of objects per session */

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
#define STARTUP_NO_LOAD				0x0800											/**< don't load classes/timers/listeners until the first session is created */

#define	STARTUP_MODE_DESKTOP		0x0000											/**< database is running as a part of a desktop application */
#define	STARTUP_MODE_SERVER			0x8000											/**< database is opened on a server */
#define	STARTUP_MODE_CLOUD			0x4000											/**< in a cloud */

/**
 * store creation flags
 * @see StoreCreationParameters::mode
 */
#define	STORE_CREATE_ENCRYPTED		0x0001											/**< created store to be encrypted */
#define	STORE_CREATE_PAGE_INTEGRITY	0x0002											/**< check integrity of store pages by calculating HMAC */
#define	STORE_CREATE_NO_PREFIX		0x0004											/**< don't augment #names with a store specific prefix */

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
		PropertyID			propID;
		ElementID			eid;
		ElementID			epos;
		const	Value		*oldValue;
		const	Value		*newValue;
	};
	struct EventData {
		NotificationEventType		type;
		ClassID				cid;
	};
	/**
	 * event descriptor
	 */
	struct NotificationEvent {
		PID					pin;
		const	EventData			*events;
		unsigned					nEvents;
		const	NotificationData	*data;
		unsigned					nData;
		bool						fReplication;
	};
	virtual	void	notify(NotificationEvent *events,unsigned nEvents,uint64_t txid) = 0;
};

/**
 * startup parameters
 * @see openStore, createStore
 */
struct StartupParameters
{
	unsigned			mode;								/**< open flags */
	const char			*directory;							/**< db file directory */
	unsigned			maxFiles;							/**< maximum number of files which can be simultaneously open */
	unsigned			nBuffers;							/**< number of memory buffers for pages to allocate */
	unsigned			shutdownAsyncTimeout;				/**< shutdown wait timeout */
	IService			*service;							/**< default external service handler */
	IStoreNotification	*notification;						/**< event notification interface */
	const char			*password;							/**< store password */
	const char			*logDirectory;						/**< optional directory for log files */
	size_t				logBufSize;							/**< size of log buffer in memory */
	const char			*serviceDirectory;					/**< optional directory for service libraries */
	StartupParameters(unsigned md=STARTUP_MODE_DESKTOP,const char *dir=NULL,unsigned xFiles=DEFAULT_MAX_FILES,unsigned nBuf=DEFAULT_BLOCK_NUM,
						unsigned asyncTimeout=DEFAULT_ASYNC_TIMEOUT,IService *srv=NULL,IStoreNotification *notItf=NULL,
						const char *pwd=NULL,const char *logDir=NULL,size_t lbs=DEFAULT_LOGBUF_SIZE,const char *srvDir=NULL) 
		: mode(md),directory(dir),maxFiles(xFiles),nBuffers(nBuf),shutdownAsyncTimeout(asyncTimeout),
		service(srv),notification(notItf),password(pwd),logDirectory(logDir),logBufSize(lbs),serviceDirectory(srvDir) {}
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
	uint64_t		maxSize;									/**< maximum store size in bytes; for quotas in multi-tenant environments */
	float			pctFree;									/**< percentage of free space on pages when new PINs are allocated */
	size_t			logSegSize;									/**< size of log segment */
	unsigned		mode;										/**< store creation flags */
	unsigned		xSyncStack;									/**< maximum depth of synchronous action invocations */
	unsigned		xOnCommit;									/**< maximum number of OnCommit actions */
	unsigned		xObjSession;								/**< maximum number of objects per session */
#define	DEAFULT_MAX_OBJ_SESSION		256												
	StoreCreationParameters(unsigned nCtl=0,unsigned lPage=DEFAULT_PAGE_SIZE,
		unsigned extSize=DEFAULT_EXTENT_SIZE,const char *ident=NULL,unsigned short stId=0,const char *pwd=NULL,unsigned md=0,uint64_t xSize=0,
		float pctF=-1.f,size_t lss=DEFAULT_LOGSEG_SIZE,unsigned xSync=DEFAULT_MAX_SYNC_ACTION,unsigned xoc=DEFAULT_MAX_ON_COMMIT,unsigned xoses=DEFAULT_MAX_OBJ_SESSION)
		: nControlRecords(nCtl),pageSize(lPage),fileExtentSize(extSize),identity(ident),
		storeId(stId),password(pwd),maxSize(xSize),pctFree(pctF),logSegSize(lss),mode(md),xSyncStack(xSync),xOnCommit(xoc),xObjSession(xoses) {}
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

extern "C" AFY_EXP RC			manageStores(const char *cmd,size_t lcmd,IAffinity *&store,IMapDir *id,const StartupParameters *sp=NULL,CompilationError *ce=NULL);	/**< store management using PathSQL strings */
extern "C" AFY_EXP RC			openStore(const StartupParameters& params,IAffinity *&store);																		/**< opens an existing store, returns store context handle */
extern "C" AFY_EXP RC			createStore(const StoreCreationParameters& create,const StartupParameters& params,IAffinity *&store,ISession **pLoad=NULL);			/**< creates a new store, returns store context handle */
extern "C" AFY_EXP RC			getStoreCreationParameters(StoreCreationParameters& params,IAffinity *store=NULL);													/**< retrives parameters used to create this store */
extern "C" AFY_EXP unsigned		getVersion();																														/**< get Affinity kernel version */
extern "C" AFY_EXP void			setReport(IReport *);																												/**< set (kernel-wide) error/debug info interface */

#define	INIT_ENTRY_NAME			"initService"
#ifdef	AFFINITY_STATIC_LINK
#define	SERVICE_INIT(A)			A##_initService
#else
#define	SERVICE_INIT(A)			initService
#endif
extern "C" AFY_EXP	bool		initService(ISession *ses,const Value *pars,unsigned nPars,bool fNew);																/**< prototype of the service initializatio function; called when the service library is loaded by a store */

extern "C" AFY_EXP void			stopThreads();																														/**< stops all threads started by Affinity kernel */
extern "C" AFY_EXP RC			shutdownStore();																													/**< shutdown "current" store */ 

#endif
