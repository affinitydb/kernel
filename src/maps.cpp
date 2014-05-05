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

Written by Mark Venguerov 2004-2014

**************************************************************************************/

#include "maps.h"
#include "txmgr.h"
#include "queryprc.h"
#include "parser.h"

using namespace AfyKernel;

URIMgr::URIMgr(StoreCtx *ct,int hashSize,int nameHashSize,unsigned xObj) : NamedObjMgr(ct,MA_URIID,hashSize,MA_URI,nameHashSize,xObj)
{
}

CachedObject *URIMgr::create()
{
	return new(ctx) URI(STORE_INVALID_URIID,*this);
}

URI *URIMgr::insert(const char *uri,size_t ln)
{
	if (uri==NULL || *uri=='\0') return NULL;
	URIInfo info; SInCtx::getURIFlags(uri,ln,info);
	return (URI*)NamedObjMgr::insert(uri,ln,&info,sizeof(URIInfo));
}

char *URIMgr::getURI(URIID uid,Session *ses)
{
	if (uid>STORE_MAX_URIID) return NULL;
	URI *uri=(URI*)ObjMgr::find(uid); if (uri==NULL) return NULL;
	const StrLen *name=uri->getName(); char *p=NULL;
	if (name!=NULL && (p=(char*)ses->malloc(name->len+1))!=NULL) {memcpy(p,name->str,name->len); p[name->len]=0;}
	uri->release(); return p;
}

RC URI::deserializeCachedObject(const void *buf,size_t size)
{
	if (buf!=NULL && size>0) {
		if (size!=sizeof(URIInfo)) return RC_CORRUPTED;
		memcpy(&info,buf,sizeof(URIInfo));
	}
	return RC_OK; 
}

byte *Identity::getCertificate(Session *ses) const
{
	byte *cert=NULL; size_t l; assert(ses!=NULL);
	if (lCert>0 && certificate.defined()) ses->getStore()->queryMgr->loadData(certificate,cert,l,ses);		// cmp l & lCert?
	return cert;
}

IdentityMgr::IdentityMgr(StoreCtx *ct,int hashSize,int nameHashSize,unsigned xObj) : NamedObjMgr(ct,MA_IDENTID,hashSize,MA_IDENTNAME,nameHashSize,xObj)
{
}

CachedObject *IdentityMgr::create()
{
	PageAddr addr={INVALID_PAGEID,INVALID_INDEX}; return new(ctx) Identity(STORE_INVALID_IDENTITY,NULL,addr,0,*this);
}

Identity *IdentityMgr::insert(const char *ident,size_t l,const byte *pwd,const byte *cert,size_t lcert,bool fMayInsert)
{
	Identity::IdentityInfo ii={(uint32_t)lcert,INVALID_PAGEID,INVALID_INDEX,fMayInsert,pwd!=NULL,{0}};
	if (pwd!=NULL) memcpy(&ii.pwd,pwd,PWD_ENC_SIZE); MiniTx tx(Session::getSession());
	if (cert!=NULL && lcert!=0) {
		PageAddr addr; uint64_t ll; RC rc;
		if ((rc=ctx->queryMgr->persistData(NULL,cert,lcert,addr,ll))!=RC_OK && rc!=RC_TRUE) return NULL;
		ii.pid=addr.pageID; ii.idx=addr.idx;
	}
	Identity *id=(Identity*)NamedObjMgr::insert(ident,l,&ii,sizeof(ii));
	if (id!=NULL) tx.ok(); return id;
}

RC IdentityMgr::setInsertPermission(IdentityID iid,bool fMayInsert)
{
	Identity *ident=(Identity*)ObjMgr::find(iid); if (ident==NULL) return RC_NOTFOUND;
	MiniTx tx(Session::getSession());
	byte f=fMayInsert?1:0; RC rc=modify(ident,&f,1,offsetof(Identity::IdentityInfo,fMayInsert));
	if (rc==RC_OK) {ident->fMayInsert=fMayInsert; tx.ok();} ident->release(); return rc;
}

RC IdentityMgr::changePassword(IdentityID iid,const char *oldPwd,const char *newPwd)
{
	Identity *ident=(Identity*)ObjMgr::find(iid); if (ident==NULL) return RC_NOTFOUND;
	const byte *spwd=ident->getPwd(); if ((spwd==NULL)!=(oldPwd==NULL)) return RC_NOACCESS;
	if (spwd!=NULL) {PWD_ENCRYPT pwd_enc((const byte*)oldPwd,strlen(oldPwd),spwd); if (!pwd_enc.isOK()) return RC_NOACCESS;}
	byte buf[PWD_ENC_SIZE+1]; ushort l=sizeof(buf);
	if (newPwd==NULL||*newPwd=='\0') {buf[0]=0; l=1;}
	else {
		PWD_ENCRYPT epwd((byte*)newPwd,strlen(newPwd));
		buf[0]=1; memcpy(buf+1,epwd.encrypted(),PWD_ENC_SIZE);
	}
	MiniTx tx(Session::getSession()); RC rc=modify(ident,buf,l,offsetof(Identity::IdentityInfo,fPwd));
	if (rc==RC_OK) {ident->fPwd=newPwd!=NULL; if (ident->fPwd) memcpy(ident->pwd,buf+1,PWD_ENC_SIZE); tx.ok();}
	ident->release(); return rc;
}

RC IdentityMgr::changeCertificate(IdentityID iid,const char *pwd,const unsigned char *cert,unsigned lcert)
{
	Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION;
	Identity *ident=(Identity*)ObjMgr::find(iid); if (ident==NULL) return RC_NOTFOUND;
	const byte *spwd=ident->getPwd(); if ((spwd==NULL)!=(pwd==NULL)) {ident->release(); return RC_NOACCESS;}
	if (spwd!=NULL) {PWD_ENCRYPT pwd_enc((const byte*)pwd,strlen(pwd),spwd); if (!pwd_enc.isOK()) {ident->release(); return RC_NOACCESS;}}
	MiniTx tx(ses); RC rc=RC_OK; PageAddr addr; uint64_t ll;
	if (ident->certificate.defined()) rc=ctx->queryMgr->deleteData(ident->certificate,ses);
	if (rc==RC_OK) {
		if (cert==NULL || lcert==0) {addr.pageID=INVALID_PAGEID; addr.idx=INVALID_INDEX;}
		else if ((rc=ctx->queryMgr->persistData(NULL,cert,lcert,addr,ll))==RC_TRUE) rc=RC_OK;
		if (rc==RC_OK) {
			Identity::IdentityInfo ii; ii.lCert=lcert; ii.pid=addr.pageID; ii.idx=addr.idx;
			rc=modify(ident,&ii,sizeof(uint32_t)+sizeof(PageID)+sizeof(PageIdx),0);
			if (rc==RC_OK) {ident->lCert=lcert; ident->certificate=addr; tx.ok();}
		}
	}
	ident->release(); return rc;
}

RC Identity::deserializeCachedObject(const void *buf,size_t size)
{
	fPwd=fMayInsert=false; lCert=0; certificate.pageID=INVALID_PAGEID; certificate.idx=INVALID_INDEX;
	if (buf!=NULL) {
		if (size!=sizeof(Identity::IdentityInfo)) return RC_CORRUPTED;
		const Identity::IdentityInfo *pi=(Identity::IdentityInfo*)buf;
		if (pi->fPwd!=0) {memcpy(pwd,pi->pwd,PWD_ENC_SIZE); fPwd=true;}
		fMayInsert=pi->fMayInsert!=0; certificate.pageID=__una_get(pi->pid); 
		certificate.idx=__una_get(pi->idx); lCert=__una_get(pi->lCert); 
	}
	return RC_OK; 
}
