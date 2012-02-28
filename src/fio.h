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

Written by Mark Venguerov and Andrew Skowronski 2004-2012

**************************************************************************************/

/**
 * platform independent file i/o control structures
 */
#ifndef _FIO_H_
#define _FIO_H_

#include "utils.h"
#include "storeio.h"

class IStoreIO;

namespace AfyKernel
{
class StoreCtx;

/**
 * async io control block
 * extends linux definition
 */
struct myaio
{
	FileID			aio_fildes;
	off64_t			aio_offset;
	void			*aio_buf; 
	size_t			aio_nbytes;
	int				aio_lio_opcode;
	void			(*aio_notify)(void *,RC);
	void			*aio_param;
	class PBlock	*aio_pb;
	RC				aio_rc;
	StoreCtx		*aio_ctx;
};

#define DEFAULT_SLOTS		100				/**< default number of simultaneously open file descriptors */

enum FIOType {FIO_READ, FIO_WRITE};

/**
 * file manager - platform independent part
 */
class FileMgr
{
	size_t				lPage;
	char				*dir;
	class StoreCtx		*const ctx;
	class IStoreIO		*pio;
	bool                bDefaultIO;

	static void asyncIOCompletionCallback(IStoreIO::iodesc *aiodesc);

public:
	FileMgr(class StoreCtx *ct,int maxOpenFiles,IStoreIO* =NULL);
	~FileMgr();
	void	*operator new(size_t s,StoreCtx *ctx);
	void	setDirectory(const char *dir);
	const	char *getDirectory() const {return dir;}
	size_t	getPageSize() const {return lPage;}
	void	setPageSize(size_t lP) {lPage = lP;}
	RC		open(FileID& fid,const char *fname,ulong flags=0);
	RC		close(FileID fid);
	void	closeAll(FileID start);
	RC		io(FIOType type,PageID pid,void *buf,size_t len,bool fSync=false);
	RC		listIO(int mode,int nent,myaio* const* pcbs,bool fSync=false);
	off64_t	getFileSize(FileID fid);
	RC		truncate(FileID fid,off64_t size);
	RC		allocateExtent(FileID fid,ulong nPages,off64_t& addr);
	void	deleteLogFiles(ulong maxFile,const char *lDir,bool fArchived=true);
	char	*getDirString(const char *d,bool fRel=false);
	static RC	moveStore(const char *from,const char *to,IStoreIO *pio=NULL);
	static RC	deleteStore(const char *path,IStoreIO *pio=NULL);
};

inline FileID FileIDFromPageID(PageID pid) {return (FileID)(pid>>24&0xFF);}
inline ulong  PageNumFromPageID(PageID pid) {return pid&0x00FFFFFF;}
inline PageID PageIDFromPageNum(FileID fid,ulong pageN) {assert((pageN&0xFF000000)==0); return (PageID)(fid<<24|pageN);}
inline off64_t PageIDToOffset(PageID pid,size_t lPage) {return off64_t(pid&0x00FFFFFF)*lPage;}

};

#endif
