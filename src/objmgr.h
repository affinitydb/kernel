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

/**
 * cached resources generic control structures
 */
#ifndef _OBJMGR_H_
#define _OBJMGR_H_

#include "qmgr.h"
#include "idxtree.h"
#include "session.h"
#include <string.h>

#define	DEFAULT_MAX_OBJECTS	512

namespace AfyKernel
{

#define COBJ_NONAME	0x8000

/**
 * abstract cached object name descriptor
 */
class ObjName {
	friend	class			CachedObject;
	friend	class			NamedObjMgr;
	friend	class			ObjMgr;
	class	NamedObjMgr		&mgr;
	HChain<ObjName>			nameList;
	StrKey					name;
	ulong					ID;
	SharedCounter			count;
	RWLock					lock;
	bool					fBad;
	void					operator delete(void *p) {free(p,STORE_HEAP);}
	ObjName(const char *nm,ulong id,class NamedObjMgr& mg,bool fCopy=true) : mgr(mg),nameList(this),name(nm,fCopy),ID(id),fBad(false) {}
	void					destroy();
public:
	const	StrKey&	getKey() const {return name;}
};

/**
 * abstract (unnamed) cached object descriptor
 */
class CachedObject
{
	friend	class			ObjMgr;
	friend	class			NamedObjMgr;
	typedef	QElt<CachedObject,ulong,ulong> QE;
	QE						*qe;
	class	ObjName			*name;
	void					downgradeLock() {if (qe!=NULL) qe->downgradeLock(RW_S_LOCK);}
protected:
	ulong					ID;
	class	ObjMgr			&mgr;
	CachedObject(ulong id,ObjMgr& mg) : qe(NULL),name(NULL),ID(id),mgr(mg) {}
public:
	virtual					~CachedObject();
	ulong					getID() const {return ID;}
	const char*				getName() const {return name!=NULL?(const char*)name->name:(char*)0;}
	void					setKey(ulong id,void*);
	QE						*getQE() const {return qe;}
	void					setQE(QE *q) {qe=q;}
	bool					isDirty() const {return false;}
	RW_LockType				lockType(RW_LockType lt) const {return lt;}
	bool					save() const {return true;}
	RC						load(PageID,ulong);
	void					release();
	void					destroy();
	virtual	RC				deserializeCachedObject(const void *buf,size_t size) = 0;
	void					operator delete(void *p) {free(p,STORE_HEAP);}
	void					initNew() const {}
	static	CachedObject	*createNew(ulong id,void *mg);
	static	void			waitResource(void *mg);
	static	void			signal(void *mg);
};

typedef QMgr<CachedObject,ulong,ulong,PageID,STORE_HEAP> ObjHash;

/**
 * abstract unnamed cached object manager
 */
class ObjMgr : public ObjHash
{
	friend	class			CachedObject;
	friend	class			NamedObjMgr;
	static	const IndexFormat	objIndexFmt;
	virtual	bool			isNamed() const;
protected:
	StoreCtx				*const ctx;
	SharedCounter			nObj;
	int						xObj;
	TreeGlobalRoot			map;
	ObjMgr(StoreCtx *ct,MapAnchor ma,int hashSize,int xO=DEFAULT_MAX_OBJECTS);
	virtual					~ObjMgr();
	virtual	CachedObject	*create() = 0;
public:
	CachedObject			*find(ulong id) {CachedObject *obj; return get(obj,id,INVALID_PAGEID)==RC_OK?obj:(CachedObject*)0;}
	TreeScan				*scan(class Session *ses) {return map.scan(ses,NULL);}
	CachedObject			*insert(const void *,size_t);
	RC						modify(ulong id,const void *data,size_t lData,size_t sht=0);
	void *operator new(size_t s,StoreCtx *ctx) {void *p=ctx->malloc(s); if (p==NULL) throw RC_NORESOURCES; return p;}
};

/**
 * abstract named cached object manager
 * extands ObjMgr
 */
class NamedObjMgr : public ObjMgr
{
	friend	class				ObjName;
	friend	class				CachedObject;
	static	const	IndexFormat	nameIndexFmt;
	bool						isNamed() const;
	struct	NamedObjPtr {
		uint32_t	ID;
		PageID	pageID;		
	};
protected:
	typedef SyncHashTab<ObjName,const StrKey&,&ObjName::nameList,StrRefKey,const char*> NameHash;
	NameHash			nameHash;
	TreeGlobalRoot	nameMap;
	NamedObjMgr(StoreCtx *ct,MapAnchor ma,int hashSize,MapAnchor nma,int nameHashSize,int xO=DEFAULT_MAX_OBJECTS);
	virtual			~NamedObjMgr();
public:
	CachedObject	*find(const char *name);
	CachedObject	*insert(const char *name,const void *pv,size_t lv);
	bool			exists(const char *name);
	TreeScan		*scanNames(Session *ses) {return nameMap.scan(ses,NULL);}
	RC				modify(const CachedObject *co,const void *data,size_t lData,size_t sht=0);
	RC				rename(ulong id,const char *name);
};

};

#endif
