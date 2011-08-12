/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov and Andrew Skowronski 2004 - 2010

**************************************************************************************/

#ifndef _FIO_H_
#define _FIO_H_

#include "utils.h"
#include "storeio.h"

class IStoreIO;

namespace MVStoreKernel
{
class StoreCtx;
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

#define DEFAULT_SLOTS		100

enum FIOType {FIO_READ, FIO_WRITE};

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
	bool	asyncIOEnabled() const;
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
