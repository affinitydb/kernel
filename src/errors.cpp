/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "types.h"
#include "startup.h"
#include <stdarg.h>
#include <stdio.h>

using namespace MVStoreKernel;

static IReport *iReport = NULL;
static void *reportNS = NULL;
static const int msgLevel[] = {100,4,3,2,1,0};

void setReport(IReport *ir)
{
	if ((iReport=ir)!=NULL && reportNS==NULL) reportNS=ir->declareNamespace("affinity");
}

#ifdef WIN32
RC convCode(DWORD dwError)
{
	RC rc;
	switch (dwError) {
	case ERROR_SUCCESS: rc = RC_OK; break;

	case ERROR_REM_NOT_LIST:
	case ERROR_BAD_NETPATH:
	case ERROR_DEV_NOT_EXIST:
	case ERROR_BAD_DEV_TYPE:
	case ERROR_BAD_NET_NAME:
	case ERROR_MOD_NOT_FOUND:
	case ERROR_PROC_NOT_FOUND:
	case ERROR_PATH_NOT_FOUND:
	case ERROR_FILE_NOT_FOUND: rc = RC_NOTFOUND; break;

	case ERROR_FILE_EXISTS:
	case ERROR_ALREADY_EXISTS: rc = RC_ALREADYEXISTS; break;

	case ERROR_INVALID_PARAMETER: rc = RC_INVPARAM; break;

	case ERROR_INVALID_BLOCK:
	case ERROR_BAD_ENVIRONMENT:
	case ERROR_INVALID_ADDRESS:
	case ERROR_INVALID_DATA:
	case ERROR_BAD_COMMAND:
	case ERROR_BAD_LENGTH:
	case ERROR_NOT_SUPPORTED:
	case ERROR_DIRECT_ACCESS_HANDLE:
	case ERROR_NEGATIVE_SEEK:
	case ERROR_EXCL_SEM_ALREADY_OWNED:
	case ERROR_SEM_IS_SET:
	case ERROR_SEM_OWNER_DIED:
	case ERROR_BUFFER_OVERFLOW:
	case ERROR_INVALID_NAME:
	case ERROR_CALL_NOT_IMPLEMENTED:
	case ERROR_INSUFFICIENT_BUFFER:
	case ERROR_BAD_THREADID_ADDR:
	case ERROR_BAD_ARGUMENTS:
	case ERROR_BAD_PATHNAME:
	case ERROR_FILENAME_EXCED_RANGE:
	case ERROR_DIRECTORY:
	case ERROR_INVALID_HANDLE: rc = RC_INTERNAL; break;

	case ERROR_NETWORK_BUSY:
	case ERROR_NOT_ENOUGH_MEMORY:
	case ERROR_TOO_MANY_OPEN_FILES:
	case ERROR_SHARING_BUFFER_EXCEEDED:
	case ERROR_REQ_NOT_ACCEP:
	case ERROR_CANNOT_MAKE:
	case ERROR_OUT_OF_STRUCTURES:
	case ERROR_NO_PROC_SLOTS:
	case ERROR_TOO_MANY_SEMAPHORES:
	case ERROR_TOO_MANY_SEM_REQUESTS:
	case ERROR_PATH_BUSY:
	case ERROR_TOO_MANY_TCBS:
	case ERROR_MAX_THRDS_REACHED:
	case ERROR_TOO_MANY_MODULES:
	case ERROR_TOO_MANY_POSTS:
	case ERROR_OUTOFMEMORY: rc = RC_NORESOURCES; break;

	case ERROR_BAD_UNIT:
	case ERROR_NOT_READY:
	case ERROR_SEEK:
	case ERROR_NOT_DOS_DISK:
	case ERROR_SECTOR_NOT_FOUND:
	case ERROR_WRITE_FAULT:
	case ERROR_READ_FAULT:
	case ERROR_GEN_FAILURE:
	case ERROR_SEEK_ON_DEVICE:
	case ERROR_ADAP_HDW_ERR:
	case ERROR_BAD_NET_RESP:
	case ERROR_UNEXP_NET_ERR:
	case ERROR_BAD_REM_ADAP:
	case ERROR_SHARING_PAUSED:
	case ERROR_NET_WRITE_FAULT:
	case ERROR_BROKEN_PIPE:
	case ERROR_INVALID_DRIVE: rc = RC_DEVICEERR; break;

	case ERROR_OPEN_FAILED:
	case ERROR_ACCESS_DENIED:
	case ERROR_NOT_SAME_DEVICE:
	case ERROR_NETWORK_ACCESS_DENIED:
	case ERROR_DRIVE_LOCKED:
	case ERROR_SHARING_VIOLATION:
	case ERROR_LOCK_VIOLATION:
	case ERROR_DELETE_PENDING:
	case ERROR_BUSY:
	case ERROR_LOCK_FAILED:
	case ERROR_WRITE_PROTECT: rc = RC_NOACCESS; break;

	case ERROR_OPERATION_ABORTED: rc = RC_CANCELED; break;

	case ERROR_BAD_FORMAT:
	case ERROR_CRC: rc = RC_DATAERROR; break;

	case ERROR_HANDLE_EOF: rc = RC_EOF; break;

	case ERROR_DISK_FULL:
	case ERROR_HANDLE_DISK_FULL: rc = RC_FULL; break;

	case WAIT_TIMEOUT: rc = RC_TIMEOUT; break;

	default: rc = RC_OTHER; break;
	}
	return rc;
}

static const char *msgType[] = {"PANIC", "ERROR", "WARNING", "NOTICE", "INFO", "DEBUG"};

void MVStoreKernel::report(MsgType type,const char *str,...)
{
	va_list args; va_start(args,str); char buffer[600];
	if (iReport!=NULL) {
		vsprintf(buffer,str,args);
		iReport->report(reportNS,msgLevel[type],buffer,__FILE__,__LINE__);
	} else {
#ifdef _DEBUG
		strcpy(buffer,msgType[type]); strcat(buffer,": ");
		vsprintf(buffer+strlen(buffer),str,args); OutputDebugString(buffer);
#endif
		fprintf(stderr,"%s: ",msgType[type]); vfprintf(stderr,str,args);
	}
	va_end(args);
#if	defined(_DEBUG) && defined(BREAK_ON_ERROR)
	if (type<=MSG_ERROR) DebugBreak();
#endif
}

void MVStoreKernel::initReport()
{
}

void MVStoreKernel::closeReport()
{
}

#else
#include <errno.h>
#ifndef Darwin
#include <error.h>
#endif
#include <syslog.h>

RC convCode(int err)
{
	RC rc;
	switch (err) {
	case 0: rc = RC_OK; break;
	case ENOSPC: rc = RC_FULL; break;
	case EAGAIN: rc = RC_REPEAT; break;
	case EINVAL:
	case EFAULT:
	case ERANGE:
	case EDOM:
	case ENOSYS:
	case EBADF: rc = RC_INTERNAL; break;
	case EMFILE:
	case ENFILE:
	case E2BIG:
	case ENOMEM:
	case ENOBUFS:
	case EFBIG: rc = RC_NORESOURCES; break;
	case EINTR:			//The open operation was interrupted by a signal. See Interrupted Primitives. 
	case ESPIPE:	//The file descriptor filedes is associated with a pipe or a FIFO and this device does not allow positioning of the file pointer. 
	case EIO: rc = RC_DEVICEERR; break;
	case EISDIR:
	case EROFS:
	case EPERM:
	case EBUSY:
	case EACCES: rc = RC_NOACCESS; break;
	case EEXIST: rc = RC_ALREADYEXISTS; break;
	case ENXIO:
	case ENOENT: rc = RC_NOTFOUND; break;
	case ETIMEDOUT: rc = RC_TIMEOUT; break;
	default: rc = RC_OTHER; break;
	}
	return rc;
}

static bool fSyslogOpen = false;
static int facility = LOG_USER;

void MVStoreKernel::report(MsgType type,const char *str,...)
{
	va_list va; va_start(va,str);
	if (iReport!=NULL) {
		char buffer[600]; vsprintf(buffer,str,va);
		iReport->report(reportNS,msgLevel[type],buffer,__FILE__,__LINE__);
	} else {
		int priority;
		switch (type) {
		case MSG_CRIT: priority = LOG_CRIT; break;
		case MSG_ERROR: priority = LOG_ERR; break;
		case MSG_WARNING: priority = LOG_WARNING; break;
		case MSG_NOTICE: priority = LOG_NOTICE; break;
		default:
		case MSG_INFO: priority = LOG_INFO; break;
		case MSG_DEBUG: priority = LOG_DEBUG; break; 
		}
		if (fSyslogOpen) vsyslog(LOG_MAKEPRI(facility,priority),str,va); else vfprintf(stderr,str,va);
	}
	va_end(va);
#ifdef _DEBUG
	if (type<=MSG_ERROR) {
		// raise();
	}
#endif
}

void MVStoreKernel::initReport()
{
	if (iReport==NULL) {
		facility = LOG_USER;
		setlogmask(LOG_UPTO(LOG_DEBUG));
		openlog("affinity",LOG_PERROR,facility);
		fSyslogOpen=true;
	}
}

void MVStoreKernel::closeReport()
{
	if (fSyslogOpen) {closelog(); fSyslogOpen=false;}
}

#endif
