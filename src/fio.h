/**************************************************************************************

Copyright © 2004-2013 GoPivotal, Inc. All rights reserved.

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
 * file i/o control structures
 */
#ifndef _FIO_H_
#define _FIO_H_

#include "utils.h"
#include "startup.h"

/*
 * Flags to open()
 */
#define FIO_CREATE			0x0001		/**< Creates a new file. If the file exists, the function overwrites the file */
#define	FIO_REPLACE			0x0002		/**< Close any existing open file at the specified FileID */
#define	FIO_NEW				0x0004		/**< Fail if the file already exists */
#define	FIO_TEMP			0x0008		/**< Filename not specified.  To be deleted when closed */

#define FIO_MAX_PLUGIN_CHAIN	8		/**< Maximum possible chained i/o objects */
#define FIO_MAX_OPENFILES		100		/**< Maximum number of open files */

namespace AfyKernel
{

#ifdef ANDROID
enum LIOOP {LIO_READ, LIO_WRITE, LIO_NOP};
enum LIOMODE {LIO_WAIT, LIO_NOWAIT};
#endif

class StoreCtx;

/**
 * async io control block
 * extends linux definition
 */
#if defined(WIN32) || defined(ANDROID)
struct myaio
{
	HANDLE			aio_fildes;						/**< Inside listIO set to file handle, as returned by open method. */
	off64_t			aio_offset;						/**< Offset in bytes from beginning of file */
	void			*aio_buf;						/**< Buffer to read or write.  Normally needs to be aligned according to disk sector size (performance and O_DIRECT requirement). */
	size_t			aio_nbytes;						/**< Size of buffer */
	int				aio_lio_opcode;					/**< Action, from LIOOP enum */
#elif defined(__APPLE__)
#include <aio.h>
struct myaio : public aiocb
{
#else
#include <aio.h>
struct myaio : public aiocb64
{
#endif
	FileID			aio_fid;
	StoreCtx		*aio_ctx;
	void			(*aio_notify)(void *,RC,bool);
	class PBlock	*aio_pb;
	RC				aio_rc;							/**< Result of IO */
};

#define DEFAULT_SLOTS		100				/**< default number of simultaneously open file descriptors */

enum FIOType {FIO_READ, FIO_WRITE};

/**
 * file descriptor
 */
struct FileDesc
{
	char				*filePath;
	HANDLE				osFile;
	volatile off64_t	fileSize;
	volatile bool		fSize;
	bool				fTemp;
	
	void init() {
		filePath=NULL;		
		osFile=INVALID_HANDLE_VALUE;
		fileSize=0;
		fSize=false;
		fTemp=false;
	}
	bool isOpen() const {return osFile!=INVALID_HANDLE_VALUE;}

	void close() {
		if (osFile!=INVALID_HANDLE_VALUE) {
#ifdef WIN32
			::CloseHandle(osFile); 
#else
			::close(osFile);
#endif
			osFile=INVALID_HANDLE_VALUE; 
		}
		if (filePath!=NULL) {
#ifndef WIN32
			if (fTemp) unlink(filePath);
#endif
			free(filePath,STORE_HEAP); filePath=NULL;
		}
	}
};

/**
 * file i/o manager
 */
class GFileMgr
{
protected:
	StoreCtx		*const ctx;
	mutable RWLock	lock;
	FileDesc		*slotTab;
	int				xSlotTab;
	size_t			lPage;
	char			*loadDir;

public:
	GFileMgr(class StoreCtx *ct,int maxOpenFiles,const char *ldDir);
	~GFileMgr();
	void	*operator new(size_t s,StoreCtx *ctx);
	const	char *getDirectory() const;
	size_t	getPageSize() const {return lPage;}
	void	setPageSize(size_t lP) {lPage = lP;}

	RC		close(FileID fid);
	void	closeAll(FileID start);

	RC		io(FIOType type,PageID pid,void *buf,size_t len,bool fSync=false);
	off64_t	getFileSize(FileID fid);
	size_t	getFileName(FileID fid,char buf[],size_t lbuf) const;
	RC		growFile(FileID file, off64_t newsize);
	static RC deleteFile(const char *fname);
	static void	deleteLogFiles(unsigned maxFile,const char *lDir,bool fArchived=true);
	RC		loadExt(const char *fname,size_t l,class Session *ses,const Value *pars,unsigned nPars,bool fNew);

	RC			allocateExtent(FileID fid,unsigned nPages,off64_t& addr);
	static RC	moveStore(const char *from,const char *to);
	static RC	deleteStore(const char *path);
	static void asyncIOCallback(myaio *aio,bool fAsync=false);
	static void deleteLogFiles(const char *mask,unsigned maxFile,const char *lDir,bool fArchived);
};

inline FileID FileIDFromPageID(PageID pid) {return (FileID)(pid>>24&0xFF);}
inline unsigned  PageNumFromPageID(PageID pid) {return pid&0x00FFFFFF;}
inline PageID PageIDFromPageNum(FileID fid,unsigned pageN) {assert((pageN&0xFF000000)==0); return (PageID)(fid<<24|pageN);}
inline off64_t PageIDToOffset(PageID pid,size_t lPage) {return off64_t(pid&0x00FFFFFF)*lPage;}

};

#include "fiolinux.h"
#include "fioosx.h"
#include "fiowin.h"

#endif
