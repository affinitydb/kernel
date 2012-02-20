/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Andrew Skowronski and Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _STOREIO_H_
#define _STOREIO_H_

#include "startup.h"
#include "utils.h"

#ifdef WIN32
enum LIOOP {LIO_READ, LIO_WRITE, LIO_NOP};
enum LIOMODE {LIO_WAIT, LIO_NOWAIT};
#else
#include <fcntl.h>
#include <unistd.h>
#include <aio.h>
#include <signal.h>
#ifndef Darwin
#define	SIGPIAIO	(SIGRTMIN+6)
#define	SIGPISIO	(SIGRTMIN+7)
#else
#define	SIGPIAIO	(SIGUSR1)
#define	SIGPISIO	(SIGUSR2)
#endif
#endif

/* Flags to IStoreIO::open */
#define FIO_CREATE			0x0001  //!< Creates a new file. If the file exists, the function overwrites the file
#define	FIO_REPLACE			0x0002  //!< Close any existing open file at the specified FileID
#define	FIO_NEW				0x0004  //!< Fail if the file already exists
#define	FIO_TEMP			0x0008  //!< Filename not specified.  To be deleted when closed

#define FIO_MAX_PLUGIN_CHAIN 8 //!< Maximum possible chained i/o objects

#define FIO_MAX_OPENFILES 100 //!< Maximum number of open files

/*! \class IStoreIO
 \brief Interface which can be implemented in order to replace or supplement the native kernel i/o

 The AfyDB kernel uses this interface to open, read and change the contents of 
 store data files, log files. and temporary files.  Multiple files can be open at the 
 same time, and the implementation is responsible to track each file with a FD 
 identifier which remains unique and constant as long as the file is open.
 
 To provide an alternative implementation pass it in the StartupParameters structure.
 To profile or supplement the existing kernel implementation use getStoreIO

 \sa getStoreIO
*/
class IStoreIO
{
public:
	struct iodesc {
		AfyKernel::FileID	aio_fildes;		//!< In call to listIO set to file handle, as returned by open method.  Implementation may change to true file handle
		off64_t					aio_offset;		//!< Offset in bytes from beginning of file
		void					*aio_buf;	    //!< Buffer to read or write.  Normally needs to be aligned according to disk sector size (performance and O_DIRECT requirement).
		size_t					aio_nbytes;		//!< Size of buffer
		int						aio_lio_opcode; //!< Action, from LIOOP enum
		RC						aio_rc;			//!< Result of IO
		void					*aio_ptr[FIO_MAX_PLUGIN_CHAIN];	//!< Stack of context for use by IStoreIO asynchronous callbacks
		int						aio_ptrpos;		//!< Depth in aio_ptr
		bool                    aio_bFlush;     //!< Whether to make sure data was really written to disk before returning from synchronous call (in cases where OS caches I/O)
	} ;

	/*! Initialize the I/O system
	\param asyncIOCompletion Pointer to function to call when each async i/o operation is complete
	*/
	virtual void	init(void (*asyncIOCompletion)(iodesc*)) =0;

	/*! Generic mechanism for passing configuration information to driver.
	\param key a string that identifies the parameter.  a driver will support and publish some known keys
	\param value data of the value
	\param broadcast when false the parameter is meant specifically for this driver, otherwise it can be delegated further down the chain
	\returns RC_FALSE if the parameter was ignored
	*/
	virtual RC setParam(const char *key, const char *value, bool broadcast) =0;

	/*! Return the type name of the implementation, for example "ioprofiler", "fiowin"
	\returns pointer to name
	*/
	virtual const char * getType() const =0;

	/*! Opens or creates a file
	\param iofid Input/output variable that receives a unique identifier for this open file (do not assume this is a true OS file handle).  
			If set to INVALID_FILEID the function will assign an unused FD to the file.
			Use FIO_REPLACE and the FD of an existing file to close and replace it.
			Note: specifying an explicit FD has the risk of conflicting with any other file 
			that has already been opened and happened to have that handle.
	\param fname full path or file name (or empty in the case of temporary file)
	\param dir store directory, useful when fname does not specify a full path
	\param flags For example FIO_CREATE, FIO_TEMP
	*/
	virtual RC		open(AfyKernel::FileID& iofid,const char *fname,const char *dir,ulong flags) =0;

	/*! Returns the current size, in bytes, of the file
	\param fid Identifies an open file
	\returns size of file or 0 if invalid FileID
	*/
	virtual off64_t	getFileSize(AfyKernel::FileID fid) const =0;

	/*!Returns the full path to an open file
	\param fid Identifies an open file
	\param buf buffer to contain file path, of size lbuf.  Pass null to determine necessary string length.
	\param lbuf length of buffer
	\returns length of file name (excluding null termination)
	*/
	virtual size_t	getFileName(AfyKernel::FileID fid,char buf[],size_t lbuf) const =0;

	/*!Increase the size of an open file
	\param file Identifies an open file
	\param newsize in bytes
	\returns Will return error code if full disk space, or other error condition hit
	*/
	virtual RC      growFile(AfyKernel::FileID file, off64_t newsize)=0;

	/*!Close file (deleting temporary files)
	\param fid Which file to close
	*/
	virtual RC		close(AfyKernel::FileID fid) =0;

	/*!Close all open files
	\param start When zero all files are closed.  When > 0 then
	          only files with FileID larger than this value are closed.
	*/
	virtual void	closeAll(AfyKernel::FileID start) =0;


	/*! Perform one or more I/O reads or writes

	If mode is set to LIO_NOWAIT then the action is performed asynchronously 
	and the callback provided in IStoreIO::init method is called (potentially from a 
	different thread).

	Each iodesc structure describes the details of a requested i/o operation.
	The aio_rc member is filled in with the success state of each i/o operation.
	The function returns a failure code if any of the operations fails immediately
	but, in the case of asychronous calls, it is the aio_rc member passed to the i/o completion 
	callback which will  contain the specific failure information.  In the asynchronous case
	it is not safe to check the contents of pcbs after listIO has been called, as the memory
	will be deallocated in another thread.

	This API must support growing a file when write instructions span past the 
	current file end.  (That mechanism is used for log file allocation but 
	dat files strictly use the growFile mechanism.)
	Attempts to read beyond the end of file will fail (RC_EOF).

	\param mode LIO_WAIT or LIO_NOWAIT
	\param nent number of operations to perform
	\param pcbs pointer to array of pointers to i/o descriptions	
	\returns Returns error code if error condition was detected immediately (e.g. invalid arguments or synchronous I/O)
	*/
	virtual RC		listIO(int mode,int nent,iodesc* const* pcbs)=0;

	/*! Test whether the implementation supports asynchronous I/O
	*/

	/*!Remove a file.  It is expected that the file is not opened and correct permissions are available
	\param file to delete
	\returns RC_OK if successful
	*/
	virtual RC	deleteFile(const char *fname) =0;

	/*! Delete or archive log files.  The store will periodically discard log file that are not required for a crash recovery with this call.
	The log files will not be actively open at the time this is called.

	\param maxFile Maximum index of log file to remove. ~0ul means remove all logs.
	\param lDir Log Directory
	\param fArchived Whether to archive rather than delete (e.g. based on STARTUP_ARCHIVE_LOGS flag)
	*/
	virtual void deleteLogFiles(ulong maxFile,const char *lDir,bool fArchived) =0;

	/*! Release any resources.  Normally this will only be called at a point where all files will have been closed and all i/o complete.
	*/
	virtual void destroy()=0;


	virtual ~IStoreIO() {;}
} ;

/*!Function to retrieve an instance of the default Kernel file i/o implementation 
*/
extern "C"  _EXP IStoreIO *	getStoreIO();

#endif

