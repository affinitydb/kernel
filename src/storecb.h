/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _STORECB_H_
#define _STORECB_H_

#include "lsn.h"
#include "crypt.h"

#define	STORECBSIZE			0x1000
#define	STORE_VERSION		200
#define	MAXMASTERFILES		2				// 2 master records maximum
#define	STORECBMAGIC		0x7B09FAD3
#define	DEFAULTHMACKEY		"yv345fw3098jfxmpe&&^%RBk(08)(*@!"
#define	RESERVEDFILEIDS		12
#define	MAXDIRPAGES			32
#define	MAXPARTIALPAGES		64
#define	DEFAULTPCTFREE		0.15f

struct StoreCreationParameters;

namespace AfyKernel
{

enum StoreState 
{
	SST_INIT, SST_SHUTDOWN_COMPLETE, SST_SHUTDOWN_IN_PROGRESS, SST_IN_RECOVERY, SST_LOGGING, SST_RESTORE, SST_READ_ONLY, SST_NO_SHUTDOWN
};

enum MapAnchor
{
	MA_URIID, MA_URI, MA_IDENTID, MA_IDENTNAME, MA_RCACHE, MA_FTINDEX, MA_CLASSINDEX, MA_CLASSPINS,
	MA_PINEXTURI, MA_HEAPDIRFIRST, MA_HEAPDIRLAST, MA_CLASSDIRFIRST, MA_CLASSDIRLAST, MA_RESERVED1, 
	MA_RESERVED2, MA_RESERVED3, MA_RESERVED4, MA_RESERVED5, MA_RESERVED6, MA_RESERVED7, MA_RESERVED8, MA_ALL
};

struct MapAnchorUpdate
{
	PageID		oldPageID;
	PageID		newPageID;
};

struct PartialInfo
{
	PageID		pageID;
	uint16_t	spaceLeft;
	uint16_t	pageType;
};

struct CryptInfo
{
	uint8_t			hmac[HMAC_SIZE];
	uint8_t			salt1[sizeof(uint32_t)+PWD_LSALT];
	uint8_t			salt2[sizeof(uint32_t)+PWD_LSALT];
	uint8_t			padding[4];
};

struct StoreCB
{
	CryptInfo		hdr;
	uint32_t		magic;
	uint32_t		version;
	uint32_t		lPage;
	uint32_t		nPagesPerExtent;
	uint64_t		maxSize;
	uint32_t		logSegSize;
	uint32_t		nMaster;
	float			pctFree;
	uint16_t		storeID;
	uint16_t		fIsEncrypted;
	uint32_t		filler;
	uint8_t			encKey[ENC_KEY_SIZE];
	uint8_t			HMACKey[HMAC_KEY_SIZE];

	TIMESTAMP		timestamp;
	TXID			lastTXID;
	LSN				checkpoint;
	LSN				logEnd;
	uint64_t		nTotalPINs;
	uint32_t		nDataFiles;
	uint32_t		state;
	uint32_t		nDirPages;
	uint32_t		nPartials;
	uint32_t		xPropID;
	PageID			mapRoots[MA_ALL];
	PageID			dirPages[MAXDIRPAGES];
	PartialInfo		partials[MAXPARTIALPAGES];

	PageID			getRoot(ulong idx) const {return idx<MA_ALL?mapRoots[idx]:INVALID_PAGEID;}
	RC				update(class StoreCtx *ctx,ulong info,const byte *rec,size_t lrec,bool fUndo=false);
	void			preload(class StoreCtx *ctx) const;

	static	RC		open(class StoreCtx *ctx,const char *fname,const char *pwd,bool fForce=false);
	static	RC		create(class StoreCtx *ctx,const char *fname,const StoreCreationParameters& cpar);
	static	RC		update(class StoreCtx *ctx,bool fSetLogEnd=true);
	static	RC		changePassword(class StoreCtx *ctx,const char *newPwd);
	static	void	close(class StoreCtx *ctx);
};

};

#endif
