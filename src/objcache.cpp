/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "objmgr.h"
#include "pgtree.h"
#include "buffer.h"
#include "txmgr.h"
#include "affinity.h"

using namespace AfyDB;
using namespace AfyKernel;

CachedObject *CachedObject::createNew(ulong id,void *mg)
{
	CachedObject *obj=NULL; ObjMgr *mgr=(ObjMgr*)(ObjHash*)mg;
	if (mgr->nObj<mgr->xObj && (obj=mgr->create())!=NULL) {++mgr->nObj; obj->ID=id;}
	return obj;
}

void CachedObject::waitResource(void*)
{
	threadYield();
}

void CachedObject::signal(void*)
{
}

CachedObject::~CachedObject()
{
}

void CachedObject::setKey(ulong id,void*)
{
	if (name!=NULL) {name->destroy(); name=NULL;}
	ID=id;
}

void CachedObject::release()
{
	mgr.release(this);
}

void ObjName::destroy()
{
	mgr.nameHash.lock(this,RW_X_LOCK);
	nameList.remove(); mgr.nameHash.unlock(this);
	for (;;) {lock.lock(RW_X_LOCK); if (count==0) break; lock.unlock();}
	delete this;	// queue of free???
}

void CachedObject::destroy()
{
	if (name!=NULL) name->destroy();
	--mgr.nObj; delete this;
}

RC CachedObject::load(PageID pid,ulong flags)
{
	SearchKey key((uint64_t)ID); size_t size=0x4000,s0=size,lName; byte *buf=NULL; bool fFound=false;
	if (pid!=INVALID_PAGEID) {
		StoreCtx *ctx=StoreCtx::get(); PBlock *pb=ctx->bufMgr->getPage(pid,ctx->trpgMgr);
		if (pb!=NULL) {
			ushort lData=0;
			const byte *p=(const byte*)((TreePageMgr::TreePage*)pb->getPageBuf())->getValue(key,lData);
			if (p!=NULL) {
				if ((buf=(byte*)alloca(lData))==NULL) {pb->release(); return RC_NORESOURCES;}
				memcpy(buf,p,size=lData); fFound=true;
			}
			pb->release();
		}
	}
	if (!fFound) {
		if ((buf=(byte*)alloca(size))==NULL) return RC_NORESOURCES;
		if (!mgr.map.find(key,buf,size)) return RC_NOTFOUND;
		if (size>s0) {
			buf=(byte*)alloca(size-s0); if (buf==NULL) return RC_NORESOURCES;
			mgr.map.find(key,buf,size);
		}
	}
	if (mgr.isNamed()) {
		if ((lName=buf[0]<<8|buf[1])+2>size) return RC_CORRUPTED;
		if ((flags&COBJ_NONAME)==0) {
			char *str=(char*)malloc(lName+1,STORE_HEAP); 
			if (str==NULL) return RC_NORESOURCES;
			memcpy(str,buf+2,lName); str[lName]=0;
			NamedObjMgr::NameHash::Find findObj(((NamedObjMgr*)&mgr)->nameHash,str);
			ObjName *on=findObj.findLock(RW_X_LOCK);
			if (on==NULL) {
				if ((on=new(mgr.ctx) ObjName(str,ID,*(NamedObjMgr*)&mgr,false))==NULL) 
					{free(str,STORE_HEAP); return RC_NORESOURCES;}
				((NamedObjMgr*)&mgr)->nameHash.insertNoLock(on,findObj.getIdx());
			} else if (on->ID==~0ul) on->ID=ID; else if (on->ID!=ID) return RC_ALREADYEXISTS;	// ???
			name=on;
		}
		buf+=2+lName; size-=2+lName;
	}
	return deserializeCachedObject(buf,size);
}

const IndexFormat ObjMgr::objIndexFmt(KT_UINT,0,KT_VARDATA);

ObjMgr::ObjMgr(StoreCtx *ct,MapAnchor ma,int hashSize,int xO)
 : ObjHash(*new(ct) ObjHash::QueueCtrl<STORE_HEAP>(xO),hashSize),ctx(ct),xObj(xO),map(ma,objIndexFmt,ct)
{
	if (&ctrl==NULL) throw RC_NORESOURCES;
}

ObjMgr::~ObjMgr()
{
	if (&ctrl!=NULL) ctrl.~QueueCtrl();
}

bool ObjMgr::isNamed() const
{
	return false;
}

CachedObject *ObjMgr::insert(const void *data,size_t lData)
{
	CachedObject *obj=NULL; PageID pid; uint32_t id; MiniTx tx(Session::getSession());
	if (map.insert(data,(ushort)lData,id,pid)==RC_OK) {		// ulong -> ushort ???
		assert(id!=uint32_t(~0ul));
		if (get(obj,id,INVALID_PAGEID,QMGR_NOLOAD|RW_X_LOCK)==RC_OK)
			{if (obj->deserializeCachedObject(data,lData)!=RC_OK) {drop(obj); obj=NULL;} else tx.ok();}
	}
	return obj;
}

RC ObjMgr::modify(ulong id,const void *data,size_t lData,size_t sht)
{
	SearchKey key((uint64_t)id);
	return map.edit(key,data,(ushort)lData,(ushort)lData,(ushort)sht);
}

const IndexFormat NamedObjMgr::nameIndexFmt(KT_BIN,KT_VARKEY,sizeof(NamedObjPtr));

NamedObjMgr::NamedObjMgr(StoreCtx *ct,MapAnchor ma,int hashSize,MapAnchor nma,int nameHashSize,int xO)
	: ObjMgr(ct,ma,hashSize,xO),nameHash(nameHashSize),nameMap(nma,nameIndexFmt,ct)
{
}

NamedObjMgr::~NamedObjMgr()
{
}

bool NamedObjMgr::isNamed() const
{
	return true;
}

bool NamedObjMgr::exists(const char *name)
{
	if (nameHash.find(name)) return true;
	SearchKey key(name,(ushort)strlen(name)); NamedObjPtr objPtr; size_t size=sizeof(NamedObjPtr);
	return nameMap.find(key,&objPtr,size);
}

CachedObject *NamedObjMgr::find(const char *name) {
	if (name==NULL) return NULL; CachedObject *obj=NULL;
	NameHash::Find findObj(nameHash,name);
	ObjName *on=findObj.findLock(RW_X_LOCK);
	if (on!=NULL) {
		 ++on->count; RWLockP lck(&on->lock,RW_S_LOCK); findObj.unlock();
		if (!on->fBad) get(obj,on->ID,0); --on->count;
	} else if ((on=new(ctx) ObjName(name,~0ul,*this))!=NULL) {
		on->lock.lock(RW_X_LOCK); nameHash.insertNoLock(on,findObj.getIdx()); findObj.unlock();
		SearchKey key(name,(ushort)strlen(name)); NamedObjPtr objPtr; size_t size=sizeof(NamedObjPtr);
		if (nameMap.find(key,&objPtr,size)) {
			assert(size==sizeof(NamedObjPtr));
			get(obj,objPtr.ID,objPtr.pageID,RW_S_LOCK|COBJ_NONAME);
			on->ID=obj->ID; obj->name=on; on->lock.unlock();
		} else {
			on->fBad=true; on->lock.unlock();
			nameHash.lock(on,RW_X_LOCK); on->nameList.remove(); nameHash.unlock(on);
			for (;;) {on->lock.lock(RW_X_LOCK); if (on->count==0) break; on->lock.unlock();}
			delete on;	// queue of free???
		}
	}
	return obj;
}

CachedObject *NamedObjMgr::insert(const char *name,const void *data,size_t lData)
{
	if (name==NULL) return NULL; CachedObject *obj=NULL;
	NameHash::Find findObj(nameHash,name);
	ObjName *on=findObj.findLock(RW_X_LOCK);
	if (on!=NULL) {
		++on->count; RWLockP lck(&on->lock,RW_S_LOCK); findObj.unlock();
		if (!on->fBad) get(obj,on->ID,0); --on->count;
	} else if ((on=new(ctx) ObjName(name,~0ul,*this))!=NULL) {
		on->lock.lock(RW_X_LOCK); nameHash.insertNoLock(on,findObj.getIdx()); findObj.unlock();
		ushort lName=(ushort)strlen(name); SearchKey key(name,lName); 
		NamedObjPtr objPtr; size_t size=sizeof(NamedObjPtr); byte *buf;
		if (nameMap.find(key,&objPtr,size)) {
			assert(size==sizeof(NamedObjPtr)); on->ID=objPtr.ID;
			if (get(obj,objPtr.ID,objPtr.pageID,COBJ_NONAME|RW_S_LOCK)==RC_OK) obj->name=on;
		} else if ((buf=(byte*)alloca(lName+2+lData))!=NULL) {
			buf[0]=byte(lName>>8); buf[1]=byte(lName); memcpy(buf+2,name,lName);
			if (data!=NULL && lData!=0) memcpy(buf+2+lName,data,lData); 
			{MiniTx tx(Session::getSession());
			if (map.insert(buf,lName+2+(ushort)lData,objPtr.ID,objPtr.pageID)==RC_OK) {		// ulong -> ushort ???
				assert(objPtr.ID!=uint32_t(~0ul));
				if (nameMap.insert(key,&objPtr,sizeof(NamedObjPtr))==RC_OK &&
						get(obj,objPtr.ID,INVALID_PAGEID,QMGR_NOLOAD|RW_X_LOCK)==RC_OK) {
					if (obj->deserializeCachedObject(data,lData)!=RC_OK) {drop(obj); obj=NULL;}
					else {obj->name=on; on->ID=objPtr.ID; tx.ok();}
				}
			}}
		}
		on->lock.unlock();
	}
	return obj;
}

RC NamedObjMgr::modify(const CachedObject *obj,const void *data,size_t lData,size_t sht)
{
	assert(obj!=NULL); SearchKey key((uint64_t)obj->ID);
	return map.edit(key,data,(ushort)lData,(ushort)lData,(ushort)(2+strlen(obj->name->name)+sht));
}

RC NamedObjMgr::rename(ulong id,const char *name)
{
	if (name==NULL||id==~0ul) return RC_INVPARAM; 
	CachedObject *obj=NULL; NameHash::Find findObj(nameHash,name);
	if (findObj.findLock(RW_X_LOCK)!=NULL) return RC_ALREADYEXISTS;
	ObjName *on=new(ctx) ObjName(name,id,*this); if (on==NULL) return RC_NORESOURCES;
	RC rc=get(obj,id,INVALID_PAGEID,RW_X_LOCK); if (rc!=RC_OK) return rc;
	on->lock.lock(RW_X_LOCK); nameHash.insertNoLock(on,findObj.getIdx()); findObj.unlock();
	ushort lName=(ushort)strlen(name); SearchKey key(name,lName); 
	NamedObjPtr objPtr; size_t size=sizeof(NamedObjPtr);
	if (nameMap.find(key,&objPtr,size)) {
		assert(size==sizeof(NamedObjPtr)); on->ID=objPtr.ID;
		if (get(obj,objPtr.ID,objPtr.pageID,COBJ_NONAME|RW_S_LOCK,obj)==RC_OK) obj->name=on;
		rc=RC_ALREADYEXISTS;
	} else {
		ushort lOldName=(ushort)strlen(obj->name->name); size=sizeof(NamedObjPtr);
		SearchKey oldKey((const char*)obj->name->name,lOldName); byte *buf;
		if (!nameMap.find(oldKey,&objPtr,size)||objPtr.ID!=id) rc=RC_NOTFOUND;
		else if ((buf=(byte*)alloca(lName+2))==NULL) rc=RC_NORESOURCES;
		else {
			MiniTx tx(Session::getSession()); SearchKey kid((uint64_t)id);
			buf[0]=byte(lName>>8); buf[1]=byte(lName); memcpy(buf+2,name,lName);
			if ((rc=map.edit(kid,buf,lName+2,lOldName+2,0))==RC_OK && 		// ulong -> ushort ???
				(rc=nameMap.insert(key,&objPtr,sizeof(NamedObjPtr)))==RC_OK && (rc=nameMap.remove(oldKey))==RC_OK) {
					tx.ok(); obj->name->destroy(); obj->name=on; // drop old name from table
			}
		}
	}
	on->lock.unlock(); obj->release();
	return rc;
}
