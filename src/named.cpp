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

Written by Mark Venguerov 2013

**************************************************************************************/

#include "named.h"
#include "queryprc.h"
#include "maps.h"
#include "fsm.h"
#include "service.h"

using namespace AfyKernel;

static const IndexFormat classPINsFmt(KT_UINT,sizeof(uint64_t),KT_VARDATA);

NamedMgr::NamedMgr(StoreCtx *ct)
	: TreeGlobalRoot(MA_NAMEDPINS,classPINsFmt,ct,TF_WITHDEL),ctx(ct),
	storePrefix(NULL),xPropID(ct->theCB->xPropID),fInit(false)
{
}

const BuiltinURI NamedMgr::builtinURIs[] = {
	{S_L("Classes"),		CLASS_OF_CLASSES},
	{S_L("Timers"),			CLASS_OF_TIMERS},
	{S_L("Listeners"),		CLASS_OF_LISTENERS},
	{S_L("Loaders"),		CLASS_OF_LOADERS},
	{S_L("Packages"),		CLASS_OF_PACKAGES},
	{S_L("NamedObjects"),	CLASS_OF_NAMED},
	{S_L("Enumerations"),	CLASS_OF_ENUMS},
	{S_L("Stores"),			CLASS_OF_STORES},
	{S_L("Services"),		CLASS_OF_SERVICES},
	{S_L("FSMs"),			CLASS_OF_FSMCTX},
	
	{S_L("pinID"),			PROP_SPEC_PINID},
	{S_L("document"),		PROP_SPEC_DOCUMENT},
	{S_L("parent"),			PROP_SPEC_PARENT},
	{S_L("value"),			PROP_SPEC_VALUE},
	{S_L("created"),		PROP_SPEC_CREATED},
	{S_L("createdBy"),		PROP_SPEC_CREATEDBY},
	{S_L("updated"),		PROP_SPEC_UPDATED},
	{S_L("updatedBy"),		PROP_SPEC_UPDATEDBY},
	{S_L("ACL"),			PROP_SPEC_ACL},
	{S_L("stamp"),			PROP_SPEC_STAMP},
	{S_L("objectID"),		PROP_SPEC_OBJID},
	{S_L("predicate"),		PROP_SPEC_PREDICATE},
	{S_L("count"),			PROP_SPEC_COUNT},
	{S_L("subclasses"),		PROP_SPEC_SUBCLASSES},
	{S_L("superclasses"),	PROP_SPEC_SUPERCLASSES},
	{S_L("indexInfo"),		PROP_SPEC_INDEX_INFO},
	{S_L("properties"),		PROP_SPEC_PROPERTIES},
	{S_L("onEnter"),		PROP_SPEC_ONENTER},
	{S_L("onUpdate"),		PROP_SPEC_ONUPDATE},
	{S_L("onLeave"),		PROP_SPEC_ONLEAVE},
	{S_L("namespace"),		PROP_SPEC_NAMESPACE},
	{S_L("ref"),			PROP_SPEC_REF},
	{S_L("service"),		PROP_SPEC_SERVICE},
	{S_L("version"),		PROP_SPEC_VERSION},
	{S_L("weight"),			PROP_SPEC_WEIGHT},
	{S_L("self"),			PROP_SPEC_SELF},
	{S_L("prototype"),		PROP_SPEC_PROTOTYPE},
	{S_L("window"),			PROP_SPEC_WINDOW},
	{S_L("timerInterval"),	PROP_SPEC_INTERVAL},
	{S_L("action"),			PROP_SPEC_ACTION},
	{S_L("address"),		PROP_SPEC_ADDRESS},
	{S_L("command"),		PROP_SPEC_COMMAND},
	{S_L("undo"),			PROP_SPEC_UNDO},
	{S_L("listen"),			PROP_SPEC_LISTEN},
	{S_L("condition"),		PROP_SPEC_CONDITION},
	{S_L("subpackage"),		PROP_SPEC_SUBPACKAGE},
	{S_L("enum"),			PROP_SPEC_ENUM},
	{S_L("bufferSize"),		PROP_SPEC_BUFSIZE},
	{S_L("pattern"),		PROP_SPEC_PATTERN},
	{S_L("exception"),		PROP_SPEC_EXCEPTION},
	{S_L("identity"),		PROP_SPEC_IDENTITY},
	{S_L("request"),		PROP_SPEC_REQUEST},
	{S_L("content"),		PROP_SPEC_CONTENT},
	{S_L("position"),		PROP_SPEC_POSITION},
	{S_L("load"),			PROP_SPEC_LOAD},
	{S_L("resolve"),		PROP_SPEC_RESOLVE},
	{S_L("transition"),		PROP_SPEC_TRANSITION},
	{S_L("state"),			PROP_SPEC_STATE},
	
	{S_L("encryption"),		SERVICE_ENCRYPTION},
	{S_L("serial"),			SERVICE_SERIAL},
	{S_L("bridge"),			SERVICE_BRIDGE},
	{S_L("affinity"),		SERVICE_AFFINITY},
	{S_L("regex"),			SERVICE_REGEX},
	{S_L("pathSQL"),		SERVICE_PATHSQL},
	{S_L("JSON"),			SERVICE_JSON},
	{S_L("protobuf"),		SERVICE_PROTOBUF},
	{S_L("sockets"),		SERVICE_SOCKETS},
	{S_L("IO"),				SERVICE_IO},
	{S_L("remoteRead"),		SERVICE_REMOTE},
	{S_L("replication"),	SERVICE_REPLICATION},
};

const SpecPINProps NamedMgr::specPINProps[9] = {
	{{1ULL<<PROP_SPEC_OBJID|1ULL<<PROP_SPEC_PREDICATE,0,0,0},								PMT_CLASS},
	{{1ULL<<PROP_SPEC_OBJID|1ULL<<PROP_SPEC_INTERVAL|1ULL<<PROP_SPEC_ACTION,0,0,0},			PMT_TIMER},
	{{1ULL<<PROP_SPEC_OBJID|1ULL<<PROP_SPEC_LISTEN,0,0,0},									PMT_LISTENER},
	{{1ULL<<PROP_SPEC_OBJID|1ULL<<PROP_SPEC_LOAD,0,0,0},									PMT_LOADER},
	{{1ULL<<PROP_SPEC_OBJID|PROP_SPEC_NAMESPACE,0,0,0},										PMT_PACKAGE},
	{{1ULL<<PROP_SPEC_SERVICE,0,0,0},														PMT_COMM},
	{{1ULL<<PROP_SPEC_OBJID|1ULL<<PROP_SPEC_ENUM,0,0,0},									PMT_ENUM},
	{{1ULL<<PROP_SPEC_OBJID,0,0,0},															PMT_NAMED},
	{{0,1ULL<<(PROP_SPEC_STATE-64),0,0},													PMT_FSMCTX},
};

const unsigned NamedMgr::classMeta[MAX_BUILTIN_CLASSID+1] = {PMT_CLASS,PMT_TIMER,PMT_LISTENER,PMT_LOADER,PMT_PACKAGE,PMT_NAMED,PMT_ENUM,0,0,PMT_FSMCTX};

const char *NamedMgr::getBuiltinName(URIID uid,size_t& lname)
{
	for (unsigned i=0; i<sizeof(builtinURIs)/sizeof(builtinURIs[0]); i++)
		if (builtinURIs[i].uid==uid) {lname=builtinURIs[i].lname; return builtinURIs[i].name;}
	lname=0; return 0;
}

uint16_t NamedMgr::getMeta(ClassID cid)
{
	return cid<=MAX_BUILTIN_CLASSID?classMeta[cid]:0;
}

URIID NamedMgr::getBuiltinURIID(const char *name,size_t lname,bool fSrv)
{
	for (unsigned i=0; i<sizeof(builtinURIs)/sizeof(builtinURIs[0]); i++) {
		if (!fSrv && builtinURIs[i].uid>=MIN_BUILTIN_SERVICE) break;
		if (builtinURIs[i].lname==lname && memcmp(name,builtinURIs[i].name,lname)==0)
		{if (!fSrv || builtinURIs[i].uid>=MIN_BUILTIN_SERVICE) return builtinURIs[i].uid; else break;}
	}
	return STORE_INVALID_URIID;
}

RC NamedMgr::createBuiltinObjects(Session *ses)
{
	if (ses==NULL) return RC_NOSESSION;
	TxSP tx(ses); RC rc=tx.start(); if (rc!=RC_OK) return rc;
	static char namebuf[sizeof(AFFINITY_STD_URI_PREFIX)+40] = {AFFINITY_STD_URI_PREFIX};
	uint32_t idx=0; xPropID=MAX_BUILTIN_URIID+1; URI *uri; URIID uid;
	for (unsigned i=0; i<sizeof(builtinURIs)/sizeof(builtinURIs[0]); i++,idx++) {
		for (; idx<builtinURIs[i].uid; idx++) {
			size_t l=sprintf(namebuf+sizeof(AFFINITY_STD_URI_PREFIX)-1,"reserved%u",(unsigned)idx);
			if ((uri=(URI*)ctx->uriMgr->insert(namebuf,sizeof(AFFINITY_STD_URI_PREFIX)-1+l))==NULL) return RC_NORESOURCES;
			uid=uri->getID(); uri->release(); if (uid!=idx) return RC_INTERNAL;
		}
		if (builtinURIs[i].name==NULL) return RC_INTERNAL;
		memcpy(namebuf+sizeof(AFFINITY_STD_URI_PREFIX)-1,builtinURIs[i].name,builtinURIs[i].lname);
		if ((uri=(URI*)ctx->uriMgr->insert(namebuf,sizeof(AFFINITY_STD_URI_PREFIX)-1+builtinURIs[i].lname))==NULL) return RC_NORESOURCES;
		uid=uri->getID(); uri->release(); if (uid!=idx) return RC_INTERNAL;
	}
	PIN *classPINs[MAX_BUILTIN_CLASSID+1]; unsigned nClasses=0;
	for (unsigned i=0; i<=MAX_BUILTIN_CLASSID; i++) {
		PropertyID props[20]; unsigned nProps=0;
		for (unsigned j=0; j<sizeof(specPINProps)/sizeof(specPINProps[0]); j++) if (specPINProps[j].meta==classMeta[i]) {
			const SpecPINProps& sp=specPINProps[j]; uint64_t u;
			for (j=0; j<4; j++) for (u=sp.mask[j],idx=0; u!=0; u>>=1,idx++) if ((u&1)!=0) props[nProps++]=j*sizeof(uint64_t)*8+idx;
			break;
		}
		if (nProps==0) continue; PIN *pin;
		if ((rc=ctx->classMgr->initClassPIN(ses,CLASS_OF_CLASSES+i,props,nProps,pin))!=RC_OK) break;
		classPINs[nClasses++]=pin;
	}
	fInit=true;
	if (rc==RC_OK && nClasses!=0 && (rc=ctx->queryMgr->persistPINs(EvalCtx(ses,ECT_INSERT),classPINs,nClasses,0))==RC_OK) tx.ok();
	for (unsigned i=0; i<nClasses; i++) classPINs[i]->destroy();
	return rc;
}

RC NamedMgr::restoreXPropID(Session *ses)
{
	if (ses==NULL) return RC_NOSESSION; assert(ctx->theCB!=NULL);
	if (ctx->theCB->mapRoots[MA_URIID]!=INVALID_PAGEID) {
		TreeScan *scan=ctx->uriMgr->scan(ses); if (scan==NULL) return RC_NORESOURCES;
		if (scan->nextKey(GO_LAST)==RC_OK) {const SearchKey &key=scan->getKey(); if (key.type==KT_UINT) xPropID=(long)key.v.u;}
	}
	return RC_OK;
}

RC NamedMgr::loadObjects(Session *ses,bool fSafe)
{
	RC rc=RC_OK;
	if (!fInit) {
		MutexP lck(&lock);
		if (!fInit) {
			PINx cb(ses),*pcb=&cb;
			{QCtx qc(ses); qc.ref(); ClassScan cs(&qc,CLASS_OF_LOADERS,QO_HIDDEN); cs.connect(&pcb);
			while ((rc=cs.next())==RC_OK && (rc=ctx->queryMgr->getBody(cb))==RC_OK && (rc=LoadService::loadLoader(cb))==RC_OK);}

			if (rc==RC_OK || rc==RC_EOF) {
				QCtx qc(ses); qc.ref(); ClassScan cs(&qc,CLASS_OF_CLASSES,QO_HIDDEN); cs.connect(&pcb);
				while ((rc=cs.next())==RC_OK && (rc=ctx->queryMgr->getBody(cb))==RC_OK && (rc=ClassCreate::loadClass(cb,fSafe))==RC_OK);
			}

			if (!fSafe) {
				if (rc==RC_OK || rc==RC_EOF) {
					QCtx qc(ses); qc.ref(); ClassScan cs(&qc,CLASS_OF_FSMCTX,QO_HIDDEN); cs.connect(&pcb);
					while ((rc=cs.next())==RC_OK && (rc=ctx->queryMgr->getBody(cb))==RC_OK && (rc=StartFSM::loadFSM(cb))==RC_OK);
				}
				if (rc==RC_OK || rc==RC_EOF) {
					QCtx qc(ses); qc.ref(); ClassScan cs(&qc,CLASS_OF_LISTENERS,QO_HIDDEN); cs.connect(&pcb);
					while ((rc=cs.next())==RC_OK && (rc=ctx->queryMgr->getBody(cb))==RC_OK && (rc=StartListener::loadListener(cb))==RC_OK);
				}
				if (rc==RC_OK || rc==RC_EOF) {
					QCtx qc(ses); qc.ref(); ClassScan cs(&qc,CLASS_OF_TIMERS,QO_HIDDEN); cs.connect(&pcb);
					while ((rc=cs.next())==RC_OK && (rc=ctx->queryMgr->getBody(cb))==RC_OK && (rc=ctx->tqMgr->loadTimer(cb))==RC_OK);
				}
			}
			fInit=true; if (rc==RC_EOF) rc=RC_OK;
		}
	}
	return rc;
}

void NamedMgr::setMaxPropID(PropertyID id)
{
	for (PropertyID xid=(PropertyID)xPropID; xid<id && !cas(&xPropID,(long)xid,(long)id); xid=(PropertyID)xPropID);
}

RC NamedMgr::initStorePrefix(Identity *ident)
{
	bool fRel=false; RC rc=RC_OK; if ((ctx->theCB->flags&STFLG_NO_PREFIX)!=0) return RC_OK;
	if (ident==NULL) {
		if ((ident=(Identity*)ctx->identMgr->ObjMgr::find(STORE_OWNER))==NULL) return RC_NOTFOUND;
		fRel=true;
	}
	const StrLen *idstr=ident->getName();
	if (idstr!=NULL) {
		storePrefix=new(ctx) char[idstr->len*3+sizeof(AFY_PROTOCOL)+20];
		if (storePrefix==NULL) rc=RC_NORESOURCES;
		else {
			memcpy((char*)storePrefix,AFY_PROTOCOL,lStorePrefix=sizeof(AFY_PROTOCOL)-1);
			memcpy((char*)storePrefix+lStorePrefix,idstr->str,idstr->len); lStorePrefix+=idstr->len; // check and convert to %xx
			lStorePrefix+=sprintf((char*)storePrefix+lStorePrefix,":%d/",ctx->storeID);
		}
	}
	if (fRel) ident->release();
	return rc;
}

bool NamedMgr::exists(URIID uid)
{
	SearchKey key((uint64_t)uid); size_t l=0; return find(key,NULL,l)==RC_OK;
}

RC NamedMgr::getNamedPID(URIID uid,PID& id)
{
	SearchKey key((uint64_t)uid); byte buf[XPINREFSIZE]; size_t l=sizeof(buf);
	RC rc=find(key,buf,l); if (rc==RC_OK) {PINRef pr(ctx->storeID,buf); id=pr.id;}
	return rc;
}

RC NamedMgr::getNamed(URIID uid,PINx& cb,bool fUpdate)
{
	SearchKey key((uint64_t)uid); byte buf[XPINREFSIZE]; size_t l=sizeof(buf);
	RC rc=find(key,buf,l); if (rc!=RC_OK) return rc;
	PINRef pr(ctx->storeID,buf); cb=pr.id; if ((pr.def&PR_ADDR)!=0) cb=pr.addr;
	return ctx->queryMgr->getBody(cb,fUpdate?TVO_UPD:TVO_READ);
}

RC NamedMgr::update(URIID id,PINRef& pr,uint16_t meta,bool fInsert)
{
	SearchKey key((uint64_t)id); byte buf[XPINREFSIZE]; pr.u1=meta; pr.def|=PR_U1;
	return fInsert?insert(key,buf,pr.enc(buf),0,true):TreeGlobalRoot::update(key,NULL,0,buf,pr.enc(buf));
}

const char *NamedMgr::getTraceName(URI *uri,size_t& l) const
{
	const StrLen *nm=uri!=NULL?uri->getName():NULL; if (nm==NULL) {l=3; return "???";}
	const char *p=nm->str; l=nm->len;
	if (l>lStorePrefix && memcmp(p,storePrefix,lStorePrefix)==0) {p+=lStorePrefix; l-=lStorePrefix;}
	return p;
}
