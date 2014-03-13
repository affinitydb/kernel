/**************************************************************************************

Copyright © 2004-2014 GoPivotal, Inc. All rights reserved.

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
 * global maps of URI and Identities control structures
 */
#ifndef _MAPS_H_
#define _MAPS_H_

#include "objmgr.h"
#include "crypt.h"
#include "affinity.h"

#define	DEFAULT_URIHASH_SIZE	256
#define	DEFAULT_URINAME_SIZE	256
#define	DEFAULT_CACHED_URIS		4096

#define	DEFAULT_IDENTHASH_SIZE	128
#define	DEFAULT_IDENTNAME_SIZE	128
#define	DEFAULT_CACHED_IDENTS	256

using namespace Afy;

namespace AfyKernel
{

/**
 * URI flags
 */
#define	UID_ID		0x01		/**< URI is PathSQL identifier, no escaping is required */
#define	UID_AID		0x02		/**< URI contains only ASCII characters */
#define	UID_IRI		0x04		/**< URI contains characters other than letters and digits */
#define	UID_DQU		0x08		/**< URI contains double quotes */
#define	UID_SID		0x10		/**< URI contains escaped characters */

/**
 * URI structure info for PathSQL rendering
 */
struct URIInfo
{
	byte			flags;
	byte			lSuffix;
};

/**
 * URI descriptor for in-memory caching
 * extends CachedObject class
 */
class URI : public CachedObject
{
	URIInfo			info;
public:
	URI(unsigned id,class URIMgr& mg) : CachedObject(id,*(NamedObjMgr*)&mg) {info.flags=info.lSuffix=0;}
	const	StrLen	*getURI() const {return getName();}
	const	URIInfo	getInfo() const {return info;}
	RC				deserializeCachedObject(const void *buf,size_t size);
	RC				setAddr(const PageAddr& ad);
};

/**
 * URI cache and map manager
 * extends NamedObjMgr class
 */
class URIMgr : public NamedObjMgr
{
public:
	URIMgr(class StoreCtx *ct,int hashSize=DEFAULT_URIHASH_SIZE,int nameHashSize=DEFAULT_URINAME_SIZE,unsigned xObj=DEFAULT_CACHED_URIS);
	CachedObject	*create();
	URI				*insert(const char *URI,size_t l);
	char			*getURI(URIID uid,class Session *ses);
};

/**
 * Identity descriptor for in-memory caching
 * extends CachedObject class
 */
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
	Identity(unsigned id,const byte *p,const PageAddr& certAddr,size_t lc,class IdentityMgr& mg) 
		: CachedObject(id,*(NamedObjMgr*)&mg),certificate(certAddr),lCert(lc),fPwd(p!=NULL),fMayInsert(true)
			{if (fPwd) memcpy(pwd,p,PWD_ENC_SIZE);}
	const	byte	*getPwd() const {return fPwd?pwd:(const byte*)0;}
	byte			*getCertificate(Session *ses) const;
	size_t			getCertLength() const {return lCert;}
	bool			mayInsert() const {return fMayInsert;}
	RC				deserializeCachedObject(const void *buf,size_t size);
	friend	class	IdentityMgr;
};

/**
 * Identity cache and map manager
 * extends NamedObjMgr class
 */
class IdentityMgr : public NamedObjMgr
{
public:
	IdentityMgr(class StoreCtx *ct,int hashSize=DEFAULT_IDENTHASH_SIZE,int nameHashSize=DEFAULT_IDENTNAME_SIZE,unsigned xObj=DEFAULT_CACHED_IDENTS);
	CachedObject	*create();
	Identity		*insert(const char *ident,size_t l,const byte *key,const byte *cert,size_t lcert,bool fMayInsert);
	RC				setInsertPermission(IdentityID,bool fMayInsert);
	RC				changePassword(IdentityID,const char *oldPwd,const char *newPwd);
	RC				changeCertificate(IdentityID,const char *pwd,const unsigned char *cert,unsigned lcert);
};

};

#endif
