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

#include "txpage.h"
#include "utils.h"
#include "session.h"
#include "buffer.h"

using namespace AfyKernel;

void TxPage::initPage(byte *frame,size_t len,PageID pid)
{
	assert(((size_t)frame&0x07)==0 && (len&0x07)==0);		// 64-bit alignment
	TxPageHeader *pH = (TxPageHeader*)frame;
	pH->pageID		= pid;
	pH->pglen		= ushort(len);
	pH->pgid		= (byte)getPGID();
	pH->version		= CURRENT_VERSION;
	pH->lsn			= LSN(0);
}

bool TxPage::afterIO(PBlock *pb,size_t len,bool fLoad)
{
	byte *frame=pb->getPageBuf(); assert(((size_t)frame&0x07)==0 && (len&0x07)==0);		// 64-bit alignment

	TxPageHeader *pH=(TxPageHeader*)frame; const char *what;
	HMAC hmac(ctx->getHMACKey(),HMAC_KEY_SIZE); hmac.add(frame,len-FOOTERSIZE);
	if (memcmp(frame+len-FOOTERSIZE,hmac.result(),FOOTERSIZE)!=0) {
		what="page not initialized";
		for (ulong i=0; i<len; i++) if (frame[i]!=0) {what="incorrect checksum"; break;}
	} else {
		const byte *encKey=ctx->getEncKey();
		if (encKey!=NULL) {
			assert((len-IVSIZE-FOOTERSIZE)%AES_BLOCK_SIZE==0);
			AES aes(encKey,ENC_KEY_SIZE); aes.decrypt(frame+IVSIZE,len-IVSIZE-FOOTERSIZE,(uint32_t*)pH->IV);
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

bool TxPage::beforeFlush(byte *frame,size_t len,PageID pid)
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
			AES aes(encKey,ENC_KEY_SIZE); ctx->cryptoMgr->randomBytes(pH->IV,IVSIZE);
			aes.encrypt(frame+IVSIZE,len-IVSIZE-FOOTERSIZE,(uint32_t*)pH->IV);
		}
		HMAC hmac(ctx->getHMACKey(),HMAC_KEY_SIZE); hmac.add(frame,len-FOOTERSIZE);
		memcpy(frame+len-FOOTERSIZE,hmac.result(),FOOTERSIZE);
		return true;
	}
	report(MSG_ERROR,"Page %08X corrupt before write: %s\n",pid,what);
	return false;
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
