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
 * transactional page header and TxPage interface
 * partially implements PageMgr interface
 */
#ifndef _TXPAGE_H_
#define _TXPAGE_H_

#include "pagemgr.h"

namespace AfyKernel
{

#define	CURRENT_VERSION	110

#define HMACSIZE	16			/**< size of HMAC field */
#define	IVSIZE		16			/**< initial random vector size (for page encryption) */

#define	FOOTERSIZE	HMACSIZE	/**< space reserved at the end of page */

/**
 * transactinal page header
 */
struct TxPageHeader	
{
	uint8_t		IV[IVSIZE];		/**< inital random vector (for page encryption) */
	LSN			lsn;			/**< LSN of last page modification operation */
	uint32_t	pageID;			/**< page ID */
	uint16_t	pglen;			/**< page length (same for all pages in the store) */
	uint8_t		pgid;			/**< PageMgr ID for this page (see pagemgr.h) */
	uint8_t		version;		/**< page format version (backward compatibility) */
	size_t		length() const {return ((size_t)pglen-1&0xFFFF)+1;}	/**< length 0x10000 is stored as 0, this function returns correct length */
};

/**
 * partial implementation of PageMgr interface for transactional pages
 */
class TxPage : public PageMgr
{
protected:
	class	StoreCtx	*const ctx;
public:
	TxPage(class StoreCtx *c) : ctx(c) {}
	virtual	void	initPage(byte *page,size_t lPage,PageID pid);
	virtual	bool	afterIO(class PBlock *,size_t lPage,bool fLoad);
	virtual	bool	beforeFlush(byte *frame,size_t len,PageID pid);
			LSN		getLSN(const byte *frame,size_t len) const;
			void	setLSN(LSN lsn,byte *frame,size_t len);
};

};

#endif
