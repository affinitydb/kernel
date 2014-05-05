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

#include "session.h"
#include "startup.h"
#include <stdarg.h>

using namespace AfyKernel;

static IReport *iReport = NULL;
static void *reportNS = NULL;
static const int msgLevel[] = {100,4,3,2,1,0};

void setReport(IReport *ir)
{
	if ((iReport=ir)!=NULL && reportNS==NULL) reportNS=ir->declareNamespace("affinity");
}

static const char *errorMsgs[] =
{
	"OK",
	"object not found",
	"object already exists",
	"internal error",
	"access is denied",
	"not possible to allocate resource",
	"disk is full",
	"i/o device error",
	"data check error",
	"EOF",
	"timeout",
	"REPEAT",
	"data is corrupted",
	"i/o operation was cancelled",
	"incompatible version",
	"TRUE",
	"FALSE",
	"invalid type or type conversion is impossible",
	"division by 0",
	"invalid parameter value",
	"attempt to modify data inside of a read-only transaction",
	"unspecified error",
	"deadlock",
	"store allocation quota exceeded",
	"the store is being shut - operation aborted",
	"pin was deleted",
	"result set closed after commit/rollback",
	"store in read-only state",
	"no session set for current thread",
	"invalid operation for this object",
	"syntax error",
	"object is too big",
	"no space on page for an object",
	"constraint violation"
};

size_t Afy::errorToString(RC rc,const CompilationError *err,char obuf[],size_t lbuf)
{
	try {
		char buf[256]; size_t l=0;
		if (rc!=RC_OK || err!=NULL && err->rc!=RC_OK) {
			if (err!=NULL && err->rc!=RC_OK) rc=err->rc;
			l=err!=NULL && err->rc==RC_SYNTAX && err->msg!=NULL ?
				sprintf(buf,"Syntax error: %s at %d, line %d",err->msg,err->pos,err->line) :
				sprintf(buf,"Error: %s(%d)",(size_t)rc<sizeof(errorMsgs)/sizeof(errorMsgs[0])?errorMsgs[rc]:"???",rc);
			if (l>lbuf-1) l=lbuf-1; memcpy(obuf,buf,l);
		}
		obuf[l]=0; return l;
	} catch (...) {report(MSG_ERROR,"Exception in ISession::errorToString()\n"); return 0;}
}

const char *AfyKernel::getErrMsg(RC rc)
{
	return (size_t)rc<sizeof(errorMsgs)/sizeof(errorMsgs[0])?errorMsgs[rc]:"???";
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

	case WSAENOTSOCK:
	case WSAEINVAL:
	case ERROR_INVALID_ADDRESS:
	case ERROR_INVALID_DATA:
	case ERROR_BAD_COMMAND:
	case ERROR_BAD_LENGTH:
	case ERROR_NEGATIVE_SEEK:
	case ERROR_BAD_ARGUMENTS:
	case ERROR_BAD_THREADID_ADDR:
	case ERROR_BAD_PATHNAME:
	case ERROR_FILENAME_EXCED_RANGE:
	case ERROR_DIRECTORY:
	case ERROR_INVALID_NAME:
	case ERROR_INVALID_PARAMETER: rc = RC_INVPARAM; break;

	case WSAEISCONN:
	case ERROR_INVALID_HANDLE:
	case ERROR_NOT_SUPPORTED:
	case ERROR_CALL_NOT_IMPLEMENTED: rc = RC_INVOP; break;

	case ERROR_BAD_ENVIRONMENT: rc = RC_CORRUPTED; break;

	case WSAEFAULT:
	case ERROR_INSUFFICIENT_BUFFER:
	case ERROR_BUFFER_OVERFLOW:
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
	case ERROR_OUTOFMEMORY: rc = RC_NOMEM; break;

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

	case WSAECONNREFUSED:
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

	case WSAECONNABORTED:
	case WSAECONNRESET:
	case WSAEINTR:
	case WSAESHUTDOWN:
	case ERROR_OPERATION_ABORTED: rc = RC_CANCELED; break;

	case ERROR_BAD_FORMAT:
	case ERROR_CRC: rc = RC_DATAERROR; break;

	case ERROR_HANDLE_EOF: rc = RC_EOF; break;

	case ERROR_DISK_FULL:
	case ERROR_HANDLE_DISK_FULL: rc = RC_FULL; break;

	case WSAETIMEDOUT:
	case WAIT_TIMEOUT: rc = RC_TIMEOUT; break;

	default: rc = RC_OTHER; break;
	}
	return rc;
}

static const char *msgType[] = {"PANIC", "ERROR", "WARNING", "NOTICE", "INFO", "DEBUG"};

void AfyRC::report(MsgType type,const char *str,...)
{
	try {
		va_list args; va_start(args,str); char buffer[600];
		if (iReport!=NULL) {
			vsprintf_s(buffer,sizeof(buffer),str,args);
			iReport->report(reportNS,msgLevel[type],buffer,__FILE__,__LINE__);
		} else {
			const char *msg=type<sizeof(msgType)/sizeof(msgType[0])?msgType[type]:"UNKNOWN";
#ifdef _DEBUG
			if (strlen(str)<400) {
				strcpy(buffer,msg); strcat(buffer,": "); size_t l=strlen(buffer);
				vsprintf_s(buffer+l,sizeof(buffer)-l,str,args); OutputDebugString(buffer);
			}
#endif
			fprintf(stderr,"%s: ",msg); vfprintf(stderr,str,args);
		}
		va_end(args);
#if	defined(_DEBUG) && defined(BREAK_ON_ERROR)
		if (type<=MSG_ERROR) DebugBreak();
#endif
	} catch (...) {
	}
}

void AfyKernel::initReport()
{
}

void AfyKernel::closeReport()
{
}

#else
#include <errno.h>
#if !defined(__APPLE__) && !defined(ANDROID)
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
	case ENOTSOCK:
	case EINVAL: rc = RC_INVPARAM; break;
	case EFAULT: rc = RC_INVPARAM; break;
	case ERANGE: rc = RC_TOOBIG; break;
	case EDOM: rc = RC_INVPARAM; break;
	case ENOSYS: rc = RC_INTERNAL; break;
	case EISCONN:
	case EBADF: rc = RC_INVOP; break;
	case EMFILE:
	case ENFILE:
	case E2BIG:
	case ENOMEM:
	case ENOBUFS:
	case EFBIG: rc = RC_NOMEM; break;
	case ECONNRESET:
	case ECONNABORTED:
	case ECANCELED:
	case ESHUTDOWN:
	case EINTR: rc = RC_CANCELED; break;
	case ESPIPE:	//The file descriptor filedes is associated with a pipe or a FIFO and this device does not allow positioning of the file pointer. 
	case EIO: rc = RC_DEVICEERR; break;
	case EISDIR:
	case EROFS:
	case EPERM:
	case EBUSY:
	case ECONNREFUSED:
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

#ifdef ANDROID
#define LOG_MAKEPRI(a,b)	(a|b)
#endif

void AfyRC::report(MsgType type,const char *str,...)
{
	try {
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
	} catch (...) {
	}
}

void AfyKernel::initReport()
{
	if (iReport==NULL) {
		facility = LOG_USER;
		setlogmask(LOG_UPTO(LOG_DEBUG));
		openlog("affinity",LOG_PERROR,facility);
		fSyslogOpen=true;
	}
}

void AfyKernel::closeReport()
{
	if (fSyslogOpen) {closelog(); fSyslogOpen=false;}
}

#endif
