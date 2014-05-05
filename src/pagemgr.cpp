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

#include "pagemgr.h"
#include "session.h"
#include "buffer.h"

using namespace AfyKernel;

void PageMgr::initPage(byte *frame,size_t len,PageID pid)
{
	assert(((size_t)frame&0x07)==0 && (len&0x07)==0);		// 64-bit alignment
	PageHeader *pH	= (PageHeader*)frame;
	pH->pageID		= pid;
	pH->pglen		= ushort(len);
	pH->pgid		= (byte)getPGID();
	pH->version		= CURRENT_VERSION;
	pH->redirect	= INVALID_PAGEID;
	pH->nPages		= 0;
}

bool PageMgr::afterIO(PBlock *pb,size_t len,bool fLoad)
{
	byte *frame=pb->getPageBuf(); assert(((size_t)frame&0x07)==0 && (len&0x07)==0);		// 64-bit alignment

	TxPageHeader *pH=(TxPageHeader*)frame; const char *what; bool fCorrupted=false;
	if ((ctx->theCB->flags&STFLG_PAGEHMAC)!=0) {
		HMAC hmac(ctx->getHMACKey(),HMAC_KEY_SIZE); hmac.add(frame,len-FOOTERSIZE);
		fCorrupted=memcmp(frame+len-FOOTERSIZE,hmac.result(),FOOTERSIZE)!=0;
	}
	if (fCorrupted) {
		what="page not initialized";
		for (unsigned i=0; i<len; i++) if (frame[i]!=0) {what="incorrect checksum"; break;}
	} else {
		const byte *encKey=ctx->getEncKey();
		if (encKey!=NULL) {
			assert((len-IVSIZE-FOOTERSIZE)%AES_BLOCK_SIZE==0);
			AES aes(encKey,ENC_KEY_SIZE,true); aes.decrypt(frame+IVSIZE,len-IVSIZE-FOOTERSIZE,(uint32_t*)pH->IV);
		}
		if (pH->pageID!=pb->getPageID()) what="incorrect PageID";
		else if (pH->pglen!=ushort(len)) what="incorrect length";
		else if (pH->pgid!=getPGID()) what="incorrect PGID";
		else if (pH->version>CURRENT_VERSION) what="incorrect version";
		else return true;
	}

	Session *ses=Session::getSession();
	if (ses==NULL||ses->getExtAddr().pageID!=pb->getPageID())
		report(MSG_ERROR,"Cannot read page %08X: %s\n",pb->getPageID(),what);
	return false;
}

bool PageMgr::beforeFlush(byte *frame,size_t len,PageID pid)
{
	assert(((size_t)frame&0x07)==0 && (len&0x07)==0);		// 64-bit alignment

	TxPageHeader *pH=(TxPageHeader*)frame; const char *what;
	if (pH->pageID!=pid) what="incorrect PageID";
	else if (pH->pglen!=ushort(len)) what="incorrect length";
	else if (pH->pgid!=getPGID()) what="incorrect PGID";
	else if (pH->version>CURRENT_VERSION) what="incorrect version";
	else {
		const byte *encKey=ctx->getEncKey();
		if (encKey!=NULL) {
			assert((len-IVSIZE-FOOTERSIZE)%AES_BLOCK_SIZE==0);
			AES aes(encKey,ENC_KEY_SIZE,false); ctx->cryptoMgr->randomBytes(pH->IV,IVSIZE);
			aes.encrypt(frame+IVSIZE,len-IVSIZE-FOOTERSIZE,(uint32_t*)pH->IV);
		}
		if ((ctx->theCB->flags&STFLG_PAGEHMAC)!=0) {
			HMAC hmac(ctx->getHMACKey(),HMAC_KEY_SIZE); hmac.add(frame,len-FOOTERSIZE);
			memcpy(frame+len-FOOTERSIZE,hmac.result(),FOOTERSIZE);
		}
		return true;
	}
	report(MSG_ERROR,"Page %08X corrupt before write: %s\n",pid,what);
	return false;
}

LSN PageMgr::getLSN(const byte *,size_t) const
{
	return LSN(0);
}

void PageMgr::setLSN(LSN,byte *,size_t)
{
}

RC PageMgr::update(PBlock *,size_t,unsigned,const byte *,size_t,unsigned,PBlock*)
{
	return RC_INTERNAL;
}

PageID PageMgr::multiPage(unsigned,const byte*,size_t,bool& fMerge)
{
	fMerge=false; return INVALID_PAGEID;
}

RC PageMgr::undo(unsigned info,const byte *rec,size_t lrec,PageID)
{
	report(MSG_ERROR,"Invalid LUNDO\n"); return RC_INTERNAL;
}

PGID PageMgr::getPGID() const
{
	return PGID_ALL;
}

void *PageMgr::operator new(size_t s,StoreCtx *ctx)
{
	void *p=ctx->malloc(s); if (p==NULL) throw RC_NOMEM; return p;
}

//----------------------------------------------------------------------------------------------

void TxPage::initPage(byte *frame,size_t len,PageID pid)
{
	PageMgr::initPage(frame,len,pid);
	((TxPageHeader*)frame)->lsn=LSN(0);
}

LSN TxPage::getLSN(const byte *frame,size_t len) const
{
	assert(((size_t)frame&0x07)==0 && (len&0x07)==0);		// 64-bit alignment
	return ((TxPageHeader*)frame)->lsn;
}

void TxPage::setLSN(LSN lsn,byte *frame,size_t len)
{
	assert(((size_t)frame&0x07)==0 && (len&0x07)==0);		// 64-bit alignment
	((TxPageHeader*)frame)->lsn=lsn;
}
