/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _MAPS_H_
#define _MAPS_H_

#include "objmgr.h"
#include "crypt.h"
#include "mvstore.h"

#define	DEFAULT_URIHASH_SIZE	256
#define	DEFAULT_URINAME_SIZE	256
#define	DEFAULT_CACHED_URIS		4096

#define	DEFAULT_IDENTHASH_SIZE	128
#define	DEFAULT_IDENTNAME_SIZE	128
#define	DEFAULT_CACHED_IDENTS	256

using namespace MVStore;

namespace MVStoreKernel
{

#define	UID_ID		0x01
#define	UID_AID		0x02
#define	UID_IRI		0x04
#define	UID_DQU		0x08
#define	UID_SID		0x10

struct URIInfo
{
	byte			flags;
	byte			lSuffix;
};

class URI : public CachedObject
{
	URIInfo			info;
public:
	URI(ulong id,class URIMgr& mg) : CachedObject(id,*(NamedObjMgr*)&mg) {info.flags=info.lSuffix=0;}
	const	char	*getURI() const {return getName();}
	const	URIInfo	getInfo() const {return info;}
	RC				deserializeCachedObject(const void *buf,size_t size);
	RC				setAddr(const PageAddr& ad);
};

class URIMgr : public NamedObjMgr
{
public:
	URIMgr(class StoreCtx *ct,int hashSize=DEFAULT_URIHASH_SIZE,int nameHashSize=DEFAULT_URINAME_SIZE,unsigned xObj=DEFAULT_CACHED_URIS);
	CachedObject	*create();
	URI				*insert(const char *URI);
};

class Identity : public CachedObject
{
	struct IdentityInfo {
		uint32_t	lCert;
		PageID		pid;
		PageIdx		idx;
		uint8_t		fMayInsert;
		uint8_t		fPwd;
		uint8_t		pwd[PWD_ENC_SIZE];
	};
	byte			pwd[PWD_ENC_SIZE];
	PageAddr		certificate;
	size_t			lCert;
	bool			fPwd;
	bool			fMayInsert;
public:
	Identity(ulong id,const byte *p,const PageAddr& certAddr,size_t lc,class IdentityMgr& mg) 
		: CachedObject(id,*(NamedObjMgr*)&mg),certificate(certAddr),lCert(lc),fPwd(p!=NULL),fMayInsert(true)
			{if (fPwd) memcpy(pwd,p,PWD_ENC_SIZE);}
	const	byte	*getPwd() const {return fPwd?pwd:(const byte*)0;}
	byte			*getCertificate(Session *ses) const;
	size_t			getCertLength() const {return lCert;}
	bool			mayInsert() const {return fMayInsert;}
	RC				deserializeCachedObject(const void *buf,size_t size);
	friend	class	IdentityMgr;
};

class IdentityMgr : public NamedObjMgr
{
public:
	IdentityMgr(class StoreCtx *ct,int hashSize=DEFAULT_IDENTHASH_SIZE,int nameHashSize=DEFAULT_IDENTNAME_SIZE,unsigned xObj=DEFAULT_CACHED_IDENTS);
	CachedObject	*create();
	Identity		*insert(const char *ident,const byte *key,const byte *cert,size_t lcert,bool fMayInsert);
	RC				setInsertPermission(IdentityID,bool fMayInsert);
	RC				changePassword(IdentityID,const char *oldPwd,const char *newPwd);
	RC				changeCertificate(IdentityID,const char *pwd,const unsigned char *cert,unsigned lcert);
};

};

#endif
