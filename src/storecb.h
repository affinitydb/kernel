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
 * Store Control Block
 */
#ifndef _STORECB_H_
#define _STORECB_H_

#include "lsn.h"
#include "crypt.h"

#define	STORECBSIZE			0x1000		/**< size of control block in bytes */
#define	STORE_VERSION		200			/**< store file version */
#define	MAXMASTERFILES		2			/**< maximum number of master records */
#define	STORECBMAGIC		0x7B09FAD3	/**< magic number for control of corruption */
#define	DEFAULTHMACKEY		"yv345fw3098jfxmpe&&^%RBk(08)(*@!"	/**< default key used for HMAC calculation (for non-encrypted stores) */
#define	RESERVEDFILEIDS		12			/**< file descriptors reserved for log and temporary files */
#define	MAXDIRPAGES			32			/**< maximum number of free space directory pages */
#define	MAXPARTIALPAGES		64			/**< maximum number of partially filled pages recorded on shutdown */
#define	DEFAULTPCTFREE		0.15f		/**< default percentage of free space in heap pages */

struct StoreCreationParameters;

namespace AfyKernel
{

/**
 * store flags
 */
#define	STFLG_ENCRYPTED		0x0001
#define	STFLG_PAGEHMAC		0x0002
#define	STFLG_NO_PREFIX		0x0004

/**
 * store state enumeration
 */
enum StoreState 
{
	SST_INIT,							/**< store is created and being initialized */
	SST_SHUTDOWN_COMPLETE,				/**< after succesful shutdown, no recovery is required */
	SST_SHUTDOWN_IN_PROGRESS,			/**< shutdown is in progress */
	SST_IN_RECOVERY,					/**< during store recovery */
	SST_LOGGING,						/**< logging started - means there were write operations */
	SST_RESTORE,						/**< store being restored */
	SST_READ_ONLY,						/**< store is open, no write operations */
	SST_NO_SHUTDOWN						/**< critical error happened, no writes, no shutdown, next time open will recover the store */
};

/**
 * various anchor pages
 */
enum MapAnchor
{
	MA_URIID, MA_URI,					/**< URI map root pages */
	MA_IDENTID, MA_IDENTNAME,			/**< Identity map root pages */
	MA_RCACHE,							/**< remote PIN cache root page */
	MA_FTINDEX,							/**< Free-text index root page */
	MA_CLASSINDEX,						/**< class index root page */
	MA_NAMEDPINS,						/**< named PINs map root page */
	MA_PINEXTURI,						/**< PIN external URI map root page (not implemented yet) */
	MA_HEAPDIRFIRST, MA_HEAPDIRLAST,	/**< first and last pages in the directory of heap pages */
	MA_CLASSDIRFIRST, MA_CLASSDIRLAST,	/**< first and last pages in the directory of class PIN pages */
	MA_RESERVED1, MA_RESERVED2, MA_RESERVED3, MA_RESERVED4, MA_RESERVED5, MA_RESERVED6, MA_RESERVED7, MA_RESERVED8,		/**< reserved for future use */
	MA_ALL
};

/**
 * anchor page update structure
 */
struct MapAnchorUpdate
{
	PageID		oldPageID;
	PageID		newPageID;
};

/**
 * partially filled page info structure
 */
struct PartialInfo
{
	PageID		pageID;
	uint16_t	spaceLeft;
	uint16_t	pageType;
};

/**
 * store encryption information
 */
struct CryptInfo
{
	uint8_t			hmac[HMAC_SIZE];
	uint8_t			salt1[sizeof(uint32_t)+PWD_LSALT];
	uint8_t			salt2[sizeof(uint32_t)+PWD_LSALT];
	uint8_t			padding[8];
};

/**
 * Store Control Block
 */
struct StoreCB
{
	CryptInfo		hdr;						/**< encryption information */
	uint32_t		magic;						/**< magic number, see above */
	uint32_t		version;					/**< store version for backward compatibility */
	uint32_t		lPage;						/**< page length */
	uint32_t		nPagesPerExtent;			/**< number of pages in one extent */
	uint64_t		maxSize;					/**< maximum store size in bytes */
	uint32_t		logSegSize;					/**< log segment size */
	uint32_t		nMaster;					/**< number of master resords */
	float			pctFree;					/**< free space precentage for heap pages */
	uint16_t		storeID;					/**< store ID, 0 - 65535, set when store is created */
	uint16_t		flags;						/**< encryption, page HMAC and other flags */

	uint8_t			encKey[ENC_KEY_SIZE];		/**< encryption key, must be 64 bit aligned */
	uint8_t			HMACKey[HMAC_KEY_SIZE];		/**< HMAC calculation key */

	TIMESTAMP		timestamp;					/**< last store access timestamp */
	uint64_t		lastTXID;					/**< last transaction ID */
	LSN				checkpoint;					/**< last checkpoint LSN, recovery starts at this address */
	LSN				logEnd;						/**< current log end */
	uint64_t		nTotalPINs;					/**< total number of PINs in the store */
	uint32_t		nDataFiles;					/**< number of data files */
	uint32_t		state;						/**< current store state (see SST_XXX above) */
	uint32_t		nDirPages;					/**< number of directory pages */
	uint32_t		nPartials;					/**< number of recorded partially filled pages */
	uint32_t		xPropID;					/**< maximum property ID used */
	uint32_t		xOnCommit;					/**< maximum number of OnCommit actions */
	uint32_t		xSyncStack;					/**< maximum number of sync actions in stack */
	uint32_t		xSesObjects;				/**< maximum number of symultaneously accesable objects per session */
	PageID			mapRoots[MA_ALL];			/**< anchor pages (see MA_XXX above) */
	PageID			dirPages[MAXDIRPAGES];		/**< directory pages */
	PartialInfo		partials[MAXPARTIALPAGES];	/**< partially filled pages */

	PageID			getRoot(unsigned idx) const {return idx<MA_ALL?mapRoots[idx]:INVALID_PAGEID;}
	RC				update(class StoreCtx *ctx,unsigned info,const byte *rec,size_t lrec,bool fUndo=false);
	void			preload(class StoreCtx *ctx) const;

	static	RC		open(class StoreCtx *ctx,const char *fname,const char *pwd,bool fForce=false);
	static	RC		create(class StoreCtx *ctx,const char *fname,const StoreCreationParameters& cpar);
	static	RC		update(class StoreCtx *ctx,bool fSetLogEnd=true);
	static	RC		changePassword(class StoreCtx *ctx,const char *newPwd);
	static	void	close(class StoreCtx *ctx);
};

};

#endif
