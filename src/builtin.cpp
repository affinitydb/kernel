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

Written by Mark Venguerov 2012-2013

**************************************************************************************/
#ifdef WIN32
#define WIN32_LEAN_AND_MEAN
#define	FD_SETSIZE	1024
#endif

#include "afysock.h"
#include "qmgr.h"
#include "service.h"
#include "startup.h"

using namespace	Afy;
using namespace AfyKernel;

#define L_BIT		0x00000008

#define EREAD_BIT	0x80000000
#define EWRITE_BIT	0x40000000
#define CONNECT_BIT	0x20000000
#define READ_BIT	0x10000000
#define WRITE_BIT	0x08000000
#define	RWAIT_BIT	0x04000000
#define	WWAIT_BIT	0x02000000
#define	RESUME_BIT	0x01000000
#define	SERVER_BIT	0x00800000
#define	INIT_BIT	0x00400000

#ifdef WIN32
#define	SHANDLE				SOCKET
#define	INVALID_SHANDLE		INVALID_SOCKET
#define	SHUT_WR				SD_SEND
#define	SHUT_RD				SD_RECEIVE
#define	SHUT_RDWR			SD_BOTH
#define DEFAULT_PORT_NAME	"COM"
inline	bool				checkBlock(int err) {return err==WSAEWOULDBLOCK;}
#else
#include <termios.h>
#include <netinet/tcp.h>
#define	SHANDLE				int
#define	INVALID_SHANDLE		(-1)
#define	closesocket			close
#define	WSAGetLastError()	errno
#define DEFAULT_BAUDRATE	B115200
#define DEFAULT_PORT_NAME	"ttyS"
inline	bool				checkBlock(int err) {return err==EWOULDBLOCK||err==EAGAIN;}
#endif

#define BUF_SIZE			(16*1024)
#define MAX_WRITE_BUFFERS	16

namespace AfyKernel
{
	
inline RC getOSError()
{
#ifdef WIN32
	return convCode(GetLastError());
#else
	return convCode(errno);
#endif
}

inline int getSockErr(SOCKET s) {
#if 0
	int errcode=0; socklen_t errlen=sizeof(errcode);
	getsockopt(s,SOL_SOCKET,SO_ERROR,(char*)&errcode,&errlen);
	return errcode;
#else
	return WSAGetLastError();
#endif
}

inline RC nbio(SOCKET sock) {
#ifdef WIN32
	unsigned long mode=1; int res=ioctlsocket(sock,FIONBIO,&mode);
	if (res!=0) {report(MSG_ERROR,"FIONBIO failed(%d)\n",getSockErr(sock)); return RC_OTHER;}
#else
	int res=fcntl(sock,F_GETFL,0); res=fcntl(sock,F_SETFL,res|O_NONBLOCK);
	if (res<0) {int err=getSockErr(sock); report(MSG_ERROR,"O_NONBLOCK failed(%d)\n",err); return convCode(err);}
#endif
	return RC_OK;
}

struct AfySocketP {
	IAfySocket	*iob;
	AfySocketP() : iob(NULL) {}
	AfySocketP(IAfySocket *io) : iob(io) {}
	__forceinline static int cmp(const AfySocketP& ip,SHANDLE h) {return cmp3((SHANDLE)ip.iob->getSocket(),h);}
	__forceinline operator SHANDLE() const {return (SHANDLE)iob->getSocket();}
};

class IOBlock : public Afy::IAfySocket
{
	friend	class	IOCtl;
protected:
	StoreCtx	*const	ctx;
	SHANDLE				fd;
	Session				*ss;
public:
	IOBlock(StoreCtx *ct,SHANDLE h=INVALID_SHANDLE,Session *se=NULL) : ctx(ct),fd(h),ss(se) {}
	SOCKET		getSocket() const {return (SOCKET)fd;}
	IAffinity	*getAffinity() const {return ctx;}
	ISession	*getSession() const {return ss;}
	virtual	void destroy() = 0;
};

class IOCtl
{
	RWLock					lock;
	fd_set					R,W,E;
	volatile HTHREAD		thread;
	volatile THREADID		threadId;
	volatile bool			inSelect;
	volatile bool			fRemoved;
	volatile bool			fStop;
	DynOArrayBufV<AfySocketP,SHANDLE,AfySocketP,128> ioblocks;
	volatile int			idx;
#ifdef WIN32
	HANDLE					ioPort;
public:
	IOCtl() : thread((HTHREAD)0),threadId((THREADID)0),inSelect(false),fRemoved(false),fStop(false),ioblocks(&sharedAlloc),ioPort(NULL) {FD_ZERO(&R); FD_ZERO(&W); FD_ZERO(&E);}
#else
	SOCKET					spipe[2];
public:
	IOCtl() : thread((HTHREAD)0),inSelect(false),fRemoved(false),fStop(false),ioblocks(&sharedAlloc) {spipe[0]=spipe[1]=INVALID_SOCKET; FD_ZERO(&R); FD_ZERO(&W); FD_ZERO(&E);}
#endif
	RC	add(IAfySocket *iob,unsigned iobits) {
		if ((((StoreCtx*)iob->getAffinity())->mode&STARTUP_RT)!=0) return RC_INVPARAM;
		SHANDLE sh=(SHANDLE)iob->getSocket(); if (sh==INVALID_SHANDLE) return RC_INVPARAM;
		RWLockP lck(&lock,RW_X_LOCK); unsigned i=0; RC rc=ioblocks.add(AfySocketP(iob),&i);
		if (rc==RC_OK) {
			if ((iobits&R_BIT)!=0) FD_SET(sh,&R); else FD_CLR(sh,&R);
			if ((iobits&W_BIT)!=0) FD_SET(sh,&W); else FD_CLR(sh,&W);
			FD_SET(sh,&E); if (inSelect) interrupt(); else if ((int)i<=idx) idx++;
		}
		return rc==RC_OK?start():rc==RC_FALSE?RC_OK:rc;
	}
	void remove(IAfySocket *iob) {
		SHANDLE fd=(SHANDLE)iob->getSocket();
		if (fd!=INVALID_SHANDLE) {
			RWLockP lck(&lock,RW_X_LOCK); unsigned i=0;
			FD_CLR(fd,&R); FD_CLR(fd,&W); FD_CLR(fd,&E);
			if (ioblocks.remove(AfySocketP(iob),&i)==RC_OK && !inSelect && (int)i<idx) --idx;
			if (inSelect) fRemoved=true;
		}
	}
	void set(IAfySocket *iob,unsigned iobits) {
		RWLockP lck(&lock,RW_X_LOCK); SHANDLE fd=(SHANDLE)iob->getSocket();
		if ((iobits&R_BIT)!=0) FD_SET(fd,&R); else FD_CLR(fd,&R);
		if ((iobits&W_BIT)!=0) FD_SET(fd,&W); else FD_CLR(fd,&W);
		FD_SET(fd,&E); if (inSelect) interrupt();
	}
	void reset(IAfySocket *iob,unsigned iobits) {
		RWLockP lck(&lock,RW_X_LOCK); SHANDLE fd=(SHANDLE)iob->getSocket();
		if ((iobits&R_BIT)!=0) FD_CLR(fd,&R);
		if ((iobits&W_BIT)!=0) FD_CLR(fd,&W);
	}
	void threadProc() {
		if (!casP((void *volatile*)&thread,(void*)0,(void*)getThread())) return;
		threadId=getThreadId();
		
		Session *ioSes=Session::createSession(NULL); if (ioSes==NULL) return;
		ioSes->setIdentity(STORE_OWNER,true);
		
#ifndef WIN32
		if (pipe(spipe)>=0) {nbio(spipe[0]); nbio(spipe[1]); FD_SET(spipe[0],&R);} else report(MSG_WARNING,"Failed to create select pipe (%d)\n",errno); 
#endif
		lock.lock(RW_X_LOCK);
		for (fd_set r,w,e;;) {
			timeval timeout={1,0}; int nst=getNFDs(); r=R; w=W; e=E; inSelect=true; fRemoved=false; lock.unlock();
			int num=::select(nst,&r,&w,&e,&timeout); lock.lock(RW_X_LOCK); inSelect=false; if (fStop) break;
			if (num<0) {
				int err=WSAGetLastError();
#ifdef WIN32
				if (nst!=0 && err!=WAIT_IO_COMPLETION && err!=WAIT_TIMEOUT && (err!=WSAENOTSOCK || !fRemoved)) {
#else
				if (nst!=0 && err!=EAGAIN && err!=EINTR && (err!=ENOTSOCK || !fRemoved)) {
#endif
					report(MSG_ERROR,"Select error: %d\n",err);
				}
			} else {
				int n=0;
#ifndef WIN32
				if (FD_ISSET(spipe[0],&r)) {char buf[20]; while (read(spipe[0],buf,sizeof(buf))==sizeof(buf)); n++;}
#endif
				for (idx=(int)ioblocks; --idx>=0 && n<num; ) {
					IAfySocket *iob=ioblocks[idx].iob; if (iob==NULL) continue;
					SHANDLE fd=(SHANDLE)iob->getSocket(); assert(fd!=INVALID_SHANDLE);
					lock.unlock();

					unsigned iobits=0;
					if (FD_ISSET(fd,&r)!=0) iobits|=R_BIT;
					if (FD_ISSET(fd,&w)!=0) iobits|=W_BIT;
					if (FD_ISSET(fd,&e)!=0) iobits|=E_BIT;
					if (iobits!=0) {
						n++; Session *ses=(Session*)iob->getSession();
						if (ses==NULL) ioSes->set((StoreCtx*)iob->getAffinity());
						else {
							assert(ses->getStore()==iob->getAffinity());
							//iob->ses->attachThread();
							//iob->ctx->attach(); ???
						}
						try {iob->process(ses!=NULL?ses:ioSes,iobits);}
						catch (RC rc) {report(MSG_ERROR,"select process exception: %s\n",getErrMsg(rc));}
						catch (...) {report(MSG_ERROR,"Unknown select process exception\n");}
						if (ses==NULL) ioSes->set(NULL);
						else {
							//ses->detachThread();
							//ioSes->attachThread();
						}
					}
					lock.lock(RW_X_LOCK);
				}
			}
		}
		lock.unlock();
	}
	RC		start();
	void	interrupt();
	int		getNFDs() const;
	void	shutdown(StoreCtx *ctx) {
		lock.lock(RW_X_LOCK);
		for (uint32_t i=ioblocks; i--!=0;) {
			IAfySocket *iob=ioblocks[i].iob;
			if (iob!=NULL && iob->getAffinity()==ctx) {
				SOCKET fd=iob->getSocket(); 
				if (fd!=INVALID_SOCKET) {FD_CLR(fd,&R); FD_CLR(fd,&W); FD_CLR(fd,&E); ::closesocket(fd);}
				ioblocks.remove(i); if (inSelect) fRemoved=true;
			}
		}
		lock.unlock();
	}
	bool isIOThread() const {return getThreadId()==threadId;}
	void stopThread() {
		lock.lock(RW_X_LOCK); fStop=true; if (inSelect) interrupt(); lock.unlock();
		// wait for thread to terminate?
	}
};
	
#ifdef WIN32
static DWORD WINAPI _iothread(void *param) {((IOCtl*)param)->threadProc(); return 0;}
//static VOID NTAPI apc(ULONG_PTR) {
	// need to do something here
//}
void IOCtl::interrupt() {/*::QueueUserAPC(apc,thread,NULL);*/}
int IOCtl::getNFDs() const {return (int)ioblocks;}
#else
static void *_iothread(void *param) {((IOCtl*)param)->threadProc(); return NULL;}
void IOCtl::interrupt() {char c=0; write(spipe[1],&c,1);}
int IOCtl::getNFDs() const {int n=(int)ioblocks; return n!=0?ioblocks[n-1].iob->getSocket()+1:0;}
#endif
	
RC IOCtl::start()
{
	RC rc=RC_OK;
	if (thread==(HTHREAD)0) {HTHREAD h; while ((rc=createThread(_iothread,this,h))==RC_REPEAT);}
	return rc;
}

static IOCtl ioctl;

void stopSocketThreads()
{
	ioctl.stopThread();
}

RC StoreCtx::registerSocket(IAfySocket *s)
{
	return ioctl.add(s,R_BIT);
}

void StoreCtx::unregisterSocket(IAfySocket *s)
{
	ioctl.remove(s);
}

class IOProcessor : public IService::Processor, public IOBlock
{
	struct IOBuf {
		byte	*buf;
		size_t	lbuf;
		size_t	left;
	};
protected:
	const	unsigned	type;
	volatile unsigned	state;
	Mutex				lock;
	Event				r_wait;
	Event				w_wait;
	IOBuf				queue[MAX_WRITE_BUFFERS];
	volatile unsigned	first,nbufs;
	volatile RC			rrc,wrc;
	IServiceCtx			*sctx;
	bool				fSock;
	uint64_t			nwritten;
	uint64_t			nread;
public:
	IOProcessor(StoreCtx *ctx,unsigned ty,SHANDLE h=INVALID_SHANDLE)
		: IOBlock(ctx,h),type(ty),state(0),first(0),nbufs(0),rrc(RC_OK),wrc(RC_OK),sctx(NULL),fSock(true),nwritten(0),nread(0) {}
	RC invoke(IServiceCtx *ctx,const Value& inp,Value& out,unsigned& mode) {
		RC rc=RC_OK; const bool fIOT=ioctl.isIOThread(); int res,err; sctx=ctx;
		if ((state&CONNECT_BIT)!=0) {
			assert(fd==INVALID_SHANDLE);
			if ((rc=connect())!=RC_OK) {state|=EREAD_BIT|EWRITE_BIT; return rc;}
			state&=~CONNECT_BIT;
		}
		if ((mode&ISRV_SWITCH)!=0) swmode();
		if ((mode&ISRV_WRITE)!=0) {
			if ((type&(ISRV_WRITE|ISRV_REQUEST|ISRV_SERVER))==0 || !inp.isEmpty() && !isString((ValueType)inp.type)) return RC_INVPARAM;
			if (inp.isEmpty() || inp.bstr==NULL || inp.length==0) {int res=write(NULL,0); return res<0?convCode(getError()):RC_OK;}
			MutexP lck(&lock); 
			if ((state&EWRITE_BIT)!=0) rc=wrc;
			else for (uint32_t left=inp.length;;) {
				if (nbufs==0) {
					if ((res=write(inp.bstr+inp.length-left,left))>=0) {nwritten+=res; if ((left-=res)==0) break;}
					else if (!checkBlock(err=getError())) {state|=EWRITE_BIT; rc=wrc=convCode(err); break;}
				}
				if (nbufs<MAX_WRITE_BUFFERS) {
					IOBuf& iob=queue[(first+nbufs++)%MAX_WRITE_BUFFERS];
					iob.buf=(byte*)inp.bstr; iob.lbuf=inp.length; iob.left=left; mode|=ISRV_CONSUMED;
				}
				if ((state&(W_BIT|L_BIT))!=(W_BIT|L_BIT) && (rc=wait(fIOT||(mode&ISRV_WAIT)==0?W_BIT:W_BIT|WWAIT_BIT))!=RC_OK) break;
				if ((mode&(ISRV_CONSUMED|ISRV_WAIT))==ISRV_CONSUMED) break;
				if (fIOT) {mode|=ISRV_RESUME; state|=RESUME_BIT; break;}
				w_wait.wait(lock,0); if ((state&EWRITE_BIT)!=0) {rc=wrc; break;}
			}
		} else {
			if ((type&(ISRV_READ|ISRV_REQUEST|ISRV_SERVER))==0 || out.type!=VT_BSTR && out.bstr==NULL || out.length==0) return RC_INVPARAM;
			const uint32_t lbuf=out.length; out.length=0; if ((state&EREAD_BIT)!=0) return rrc;
			do {
				if ((res=read((byte*)out.str,lbuf))>0) {nread+=res; out.length=uint32_t(res); break;}
				if (res==0) {state|=EREAD_BIT; rc=rrc=RC_EOF; break;}
				if (!checkBlock(err=getError())) {state|=EREAD_BIT; rc=rrc=convCode(err); break;}
				if ((mode&ISRV_WAIT)==0) break;
				if (fIOT) {if ((rc=wait(R_BIT|RESUME_BIT))==RC_OK) mode|=ISRV_RESUME; break;}
				lock.lock(); if ((rc=wait(R_BIT|RWAIT_BIT))==RC_OK) r_wait.wait(lock,0); lock.unlock();
			} while (rc==RC_OK);
		}
		return rc;
	}
	virtual void process(ISession *,unsigned iobits) {
		MutexP lck(&lock); int res;
		if ((iobits&E_BIT)!=0) {
			rrc=wrc=convCode(getError()); if ((state&L_BIT)!=0) ioctl.remove(this);
			disconnect(); state=(state&~(R_BIT|W_BIT|E_BIT|L_BIT|READ_BIT|WRITE_BIT))|EREAD_BIT|EWRITE_BIT;
			report(MSG_ERROR,"IOProcessor::process() error: %s\n",getErrMsg(rrc));
		} else {
			if ((iobits&R_BIT)!=0 && (state&(R_BIT|RWAIT_BIT))==(R_BIT|RWAIT_BIT)) {
				if ((state&W_BIT)==0) {ioctl.remove(this); state&=~(R_BIT|W_BIT|E_BIT|L_BIT);} else {ioctl.reset(this,R_BIT); state&=~R_BIT;}
				state&=~RWAIT_BIT; r_wait.signal();
			}
			if ((iobits&W_BIT)!=0 && (state&W_BIT)!=0) {
				while (nbufs!=0) {
					if ((res=write(queue[first].buf+queue[first].lbuf-queue[first].left,queue[first].left))<0) {
						if (checkBlock(res=getError())) return; wrc=convCode(res); state|=EWRITE_BIT; break;
					} else if ((nwritten+=res,queue[first].left-=res)==0) {
						assert(sctx!=NULL); sctx->releaseBuffer(queue[first].buf,queue[first].lbuf);
						first=--nbufs!=0?(first+1)%MAX_WRITE_BUFFERS:0;
						if ((state&WWAIT_BIT)!=0) {state&=~WWAIT_BIT; w_wait.signal();}
					} else return;
				}
				if ((state&WWAIT_BIT)!=0) {state&=~WWAIT_BIT; w_wait.signal();}
				if ((state&R_BIT)==0) {ioctl.remove(this); state&=~(R_BIT|W_BIT|E_BIT|L_BIT);} else {ioctl.reset(this,W_BIT); state&=~W_BIT;}
			}
		}
	}
	RC wait(unsigned bits) {
#ifdef WIN32
		if (!fSock) {
			//???
		} else
#endif
		if ((state&L_BIT)==0) {state|=L_BIT|bits; return ioctl.add(this,state);} 
		bool fSet=((state^bits)&(R_BIT|W_BIT))!=0; state|=bits; if (fSet) ioctl.set(this,state);
		return RC_OK;
	}
	virtual	void cleanup(IServiceCtx *sctx) {if ((state&L_BIT)!=0) {ioctl.remove(this); state&=~(L_BIT|R_BIT|W_BIT|E_BIT);}}
	virtual	RC connect() {return RC_INVPARAM;}
	virtual	void swmode() {}
	virtual	int getError() const = 0;
	virtual	int	read(byte *buf,size_t lbuf) = 0;
	virtual	int	write(const byte *buf,size_t lbuf) = 0;
	virtual	void disconnect() = 0;
};

//----------------------------------------------------------------------------------------
	
class IO;

class Device
{
protected:
	typedef	QElt<Device,StrLen,const StrLen&> QE;
	StrLen				name;
	HANDLE				handle;
	QE					*qe;
	IO					&mgr;
public:
	Device(const StrLen& nm,IO& mg) : name(nm),handle(INVALID_HANDLE_VALUE),qe(NULL),mgr(mg) {}
	~Device();
	void				operator delete(void *p);
	const	StrLen&		getID() const {return name;}
	void				setKey(const StrLen& id,void*);
	QE					*getQE() const {return qe;}
	void				setQE(QE *q) {qe=q;}
	bool				isDirty() const {return false;}
	RW_LockType			lockType(RW_LockType lt) const {return lt;}
	bool				save() const {return true;}
	RC					load(const Value& info,unsigned);
	void				release();
	void				destroy();
	void				initNew() const {}
	static	Device		*createNew(const StrLen& id,void *mg);
	static	void		waitResource(void *mg) {threadYield();}
	static	void		signal(void *mg) {}
	virtual	RC			init() {return RC_OK;}
	virtual	bool		isSpecFD(const Value& v) const {return (v.type==VT_INT||v.type==VT_UINT)&&v.ui<3;}
	friend	class		IO;
	friend	class		SimpleIOProc;
	friend	class		SimpleIOListener;
};

class SerialDevice : public Device
{
#ifdef WIN32
	DCB				comSettings;
	COMMTIMEOUTS	commTimeouts;
#else
	struct termios	tio;
	struct termios	oldtio;
#endif
public:
	SerialDevice(const StrLen& nm,IO& mg) : Device(nm,mg) {}
	RC	init() {
#ifdef WIN32
		memset(&commTimeouts,0,sizeof(commTimeouts));
		commTimeouts.ReadIntervalTimeout			= MAXDWORD; 
		//commTimeouts.ReadTotalTimeoutConstant	= 100;
		//commTimeouts.WriteTotalTimeoutConstant	= 100;
		SetCommTimeouts(handle,&commTimeouts);
		GetCommState(handle, &comSettings);
		comSettings.BaudRate = 460800;
		comSettings.StopBits = ONESTOPBIT;
		comSettings.ByteSize = 8;
		comSettings.Parity   = NOPARITY;
		comSettings.fParity  = FALSE;    
		SetCommState(handle, &comSettings);
#else
		memset(&tio,0,sizeof(tio));
		if (tcgetattr(handle,&tio)<0) return getOSError();
		oldtio=tio;
		cfsetispeed(&tio, DEFAULT_BAUDRATE);
		cfsetospeed(&tio, DEFAULT_BAUDRATE);
		/* set raw input, 1 second timeout */
		tio.c_cflag = CS8 |CREAD | CLOCAL;
		tio.c_lflag=tio.c_lflag&~(ECHO|ECHOE|ISIG|ICANON);
		tio.c_oflag&=~OPOST;
		tio.c_cc[VMIN]=1;
		tio.c_cc[VTIME]=5;
		if (fcntl(handle, F_SETFL, 0)<0) return getOSError();
		if (tcflush(handle, TCIOFLUSH)<0) {
			//...
		}
		if (tcsetattr(handle, TCSANOW, &tio)<0) {
			//...
		}
#endif
		return RC_OK;
	}
	bool isSpecFD(const Value&) const {return false;}
};

class SimpleIOProc : public IOProcessor
{
	Device&				device;
	int64_t				pos;
	int					stype;
	ServiceCtx			*sctx;
public:
	SimpleIOProc(StoreCtx *ctx,unsigned ty,Device& dev,int64_t p=0,int sty=-1)
		: IOProcessor(ctx,ty,(SHANDLE)dev.handle),device(dev),pos(p),stype(sty),sctx(NULL) {fSock=false;}
	void setServiceCtx(IServiceCtx *srv) {sctx=(ServiceCtx*)srv;}
	void process(ISession *ses,unsigned iobits) {
		if ((iobits&(E_BIT|R_BIT))!=R_BIT || sctx==NULL) IOProcessor::process(ses,iobits);
		else {sctx->invoke(NULL,0); sctx->destroy(); sctx=NULL;}
	}
	int getError() const {
#ifdef WIN32
		return (int)GetLastError();
#else
		return errno;
#endif
	}
	int seek() {
		if (stype>=0) {
#ifdef WIN32
			LONG hpos=LONG(pos>>32);
			if (SetFilePointer(device.handle,(LONG)pos,&hpos,(DWORD)stype)==INVALID_SET_FILE_POINTER) return -1;
#elif defined(__APPLE__)
			if (::lseek(device.handle,pos,stype)==(off64_t)-1) return -1;
#else
			if (::lseek64(device.handle,pos,stype)==(off64_t)-1) return -1;
#endif
			stype=-1;
		}
		return 0;
	}
	int	read(byte *buf,size_t lbuf) {
		if (seek()<0) return -1;
#ifdef WIN32
		DWORD read; return ReadFile(device.handle,(byte*)buf,(DWORD)lbuf,&read,NULL)?(int)read:-1;
#else
		return ::read(device.handle,(byte*)buf,lbuf);
#endif
	}
	int	write(const byte *buf,size_t lbuf) {
		if (buf==NULL && stype<0) {
#ifdef WIN32
			::FlushFileBuffers(device.handle);
#else
			::fsync(device.handle);
#endif
			return (int)lbuf;
		}
		if (seek()<0) return -1;
		if (buf==NULL || lbuf==0) return 0;
#ifdef WIN32
		DWORD written;
		return WriteFile(device.handle,buf,(DWORD)lbuf,&written,NULL)?(int)written:-1;
#else
		return ::write(device.handle,buf,lbuf);
#endif
	}
	RC	 stop(bool);
	void cleanup(IServiceCtx *ctx,bool fD) {IOProcessor::cleanup(ctx); if (fD) device.release();}
	void disconnect();
	void destroy();
	friend class IO;
};

#ifdef WIN32
class SimpleIOListener : public IListener
#else
class SimpleIOListener : public IOBlock, public IListener
#endif
{
	const	URIID	id;
	IO				&mgr;
	Device			&device;
	Value			*vals;
	unsigned		nVals;
	unsigned		mode;
	SimpleIOProc	*prc;
#ifdef WIN32
	SimpleIOListener(URIID i,StoreCtx *ct,IO& mg,Device& dev,unsigned md) : id(i),mgr(mg),device(dev),vals(NULL),nVals(0),mode(md),prc(NULL) {}
#else
	SimpleIOListener(URIID i,StoreCtx *ct,IO& mg,Device& dev,unsigned md) : IOBlock(ct,(SHANDLE)dev.handle),id(i),mgr(mg),device(dev),vals(NULL),nVals(0),mode(md),prc(NULL) {}
#endif
	~SimpleIOListener();
	IService *getService() const;
	URIID getID() const {return id;}
	RC create(IServiceCtx *ctx,uint32_t& dscr,IService::Processor *&ret) {
		if (prc==NULL) return RC_INTERNAL;
		switch (dscr&ISRV_PROC_MASK) {
		default: ret=NULL; return RC_INVOP;
		case ISRV_ENDPOINT|ISRV_READ: dscr|=ISRV_ALLOCBUF;
		case ISRV_ENDPOINT|ISRV_WRITE: ret=prc; break;
		}
		return RC_OK;
	}
	RC	stop(bool fSuspend);
	void process(ISession*,unsigned iobits);
	void destroy();
	friend	class	IO;
};

typedef QMgr<Device,StrLen,const StrLen&,const Value&> IOHash;

class IO : public IService, public IOHash
{
protected:
	friend class	Device;
	StoreCtx		*const	ctx;
	unsigned		xCached;
	unsigned		nCached;
	RC	getDevice(const Value *pv,Device *&ioc) {
		if (pv==NULL) return RC_INVPARAM;
		StrLen name(NULL,0); ioc=NULL; RC rc=getName(pv,name);
		if (rc==RC_OK) {
			if ((rc=get(ioc,name,*pv))!=RC_OK || ioc==NULL) ctx->free((char*)name.str);
			else if (ioc->handle==INVALID_HANDLE_VALUE) {ioc->release(); rc=RC_NOTFOUND;}
		}
		return rc;
	}
public:
	IO(StoreCtx *ct) : IOHash(*new(ct) IOHash::QueueCtrl(100,(MemAlloc*)ct),10,ct),ctx(ct),xCached(100),nCached(0) {}
	RC create(IServiceCtx *sctx,uint32_t& dscr,Processor *&ret) {
		Device *ioc=NULL; assert(sctx->getServiceID()==SERVICE_SERIAL||sctx->getServiceID()==SERVICE_IO);
		RC rc=getDevice(sctx->getParameter(PROP_SPEC_ADDRESS),ioc); if (rc!=RC_OK || ioc==NULL) return rc;
		int64_t pos=0; int stype=-1; const Value *pv;
		if ((pv=sctx->getParameter(PROP_SPEC_POSITION))!=NULL) {
			switch (pv->type) {
			case VT_INT: pos=pv->i; break;
			case VT_UINT: pos=pv->ui; break;
			case VT_INT64: pos=pv->i64; break;
			case VT_UINT64: pos=pv->ui64; break;
			case VT_STRUCT:
				//???
			default: break;
			}
			stype=(pv->meta&META_PROP_SEEK_END)!=0?SEEK_END:(pv->meta&META_PROP_SEEK_CUR)!=0?SEEK_CUR:SEEK_SET;
		}
		switch (dscr&ISRV_PROC_MASK) {
		case ISRV_ENDPOINT|ISRV_READ:
			dscr|=ISRV_ALLOCBUF;
		case ISRV_ENDPOINT|ISRV_WRITE:
			if ((ret=new(sctx) SimpleIOProc(ctx,dscr&ISRV_PROC_MASK,*ioc,pos,stype))==NULL) rc=RC_NORESOURCES;
			break;
		case ISRV_ENDPOINT|ISRV_REQUEST:
			//???
		default:
			ioc->release(); rc=RC_INVOP; break;
		}
		return rc;
	}
	virtual RC getName(const Value *pv,StrLen& addr) {
		static const char *std_io[3] = {"STDIN","STDOUT","STDERR"};
		switch (pv->type) {
		default: return RC_TYPE;
		case VT_INT: case VT_UINT:
			if (pv->ui>=sizeof(std_io)/sizeof(std_io[0])) return RC_INVPARAM;
			addr.str=std_io[pv->ui]; addr.len=strlen(addr.str); break;
		case VT_STRING: case VT_URL:
			addr.str=pv->str; addr.len=pv->length; break;
		case VT_STRUCT:
			// find name field
			break;
		}
		return addr.str==NULL?RC_INVPARAM:(addr.str=strdup(addr.str,ctx))==NULL?RC_NORESOURCES:RC_OK;
	}
	virtual Device *createDevice(const StrLen& id);
	virtual RC listen(ISession *ses,URIID id,const Value *vals,unsigned nVals,const Value *srvParams,unsigned nSrvparams,unsigned mode,IListener *&ret) {
		Device *ioc=NULL; RC rc=getDevice(Value::find(PROP_SPEC_ADDRESS,vals,nVals),ioc); if (rc!=RC_OK || ioc==NULL) return rc;
		SimpleIOListener *ls=new(&sharedAlloc) SimpleIOListener(id,ctx,*this,*ioc,mode);
		if (ls==NULL) {ioc->release(); return RC_NORESOURCES;}
		rc=copyV(vals,ls->nVals=nVals,ls->vals,&sharedAlloc);
#ifdef WIN32
		//???
#else
		if (rc==RC_OK && (rc=ioctl.add(ls,R_BIT))==RC_OK) ret=ls; else ls->destroy();
#endif
		return rc;
	}
};

class Serial : public IO
{
public:
	Serial(StoreCtx *ct) : IO(ct) {}
	RC getName(const Value *pv,StrLen& addr) {
		if (pv->type!=VT_INT&&pv->type!=VT_UINT) return IO::getName(pv,addr);
		char buf[40]; addr.len=sprintf(buf,DEFAULT_PORT_NAME"%u",pv->ui);
		return (addr.str=strdup(buf,ctx))!=NULL?RC_OK:RC_NORESOURCES;
	}
	Device *createDevice(const StrLen& id);
};

};

void Device::setKey(const StrLen& id,void*)
{
#ifdef WIN32
	if (handle!=INVALID_HANDLE_VALUE) ::CloseHandle(handle);
#else
	if (handle!=INVALID_HANDLE_VALUE) ::close(handle);
#endif
	handle=INVALID_HANDLE_VALUE; if (name.str!=NULL) mgr.ctx->free((void*)name.str); name=id;
}

RC Device::load(const Value &addr,unsigned)
{
	assert(handle==INVALID_HANDLE_VALUE); unsigned flags=addr.meta; const Value *cmd; RC rc;
	if (isSpecFD(addr)) {
		if (addr.ui>2) return RC_INVPARAM; assert(addr.type==VT_INT||addr.type==VT_UINT);
#ifdef WIN32
		const static DWORD stdHndl[] = {STD_INPUT_HANDLE,STD_OUTPUT_HANDLE,STD_ERROR_HANDLE};
		if ((handle=::GetStdHandle(stdHndl[addr.ui]))==INVALID_HANDLE_VALUE) return convCode(GetLastError());
#else
		handle=(int)addr.i;
#endif
	} else {
#ifdef WIN32
		DWORD ff=((flags&(META_PROP_READ|META_PROP_WRITE))!=META_PROP_WRITE?GENERIC_READ:0)|((flags&META_PROP_WRITE)!=0?GENERIC_WRITE:0),disp=(flags&META_PROP_CREATE)==0?OPEN_EXISTING:OPEN_ALWAYS;
		if ((handle=::CreateFile(name.str,ff,FILE_SHARE_READ|FILE_SHARE_WRITE,NULL,disp,0,NULL))==INVALID_HANDLE_VALUE) return convCode(GetLastError());
#else
		int ff=(flags&(META_PROP_READ|META_PROP_WRITE))==(META_PROP_READ|META_PROP_WRITE)?O_RDWR:(flags&META_PROP_WRITE)!=0?O_WRONLY:O_RDONLY; if ((flags&META_PROP_CREATE)!=0) ff|=O_CREAT;
		if ((handle=::open64(name.str,ff|O_NOCTTY,S_IRUSR|S_IWUSR|S_IRGRP))==INVALID_HANDLE_VALUE) return convCode(errno);
#endif
	}
	if ((rc=init())!=RC_OK) {
#ifdef WIN32
		if (handle!=INVALID_HANDLE_VALUE) ::CloseHandle(handle);
#else
		if (handle!=INVALID_HANDLE_VALUE) ::close(handle);
#endif
		handle=INVALID_HANDLE_VALUE; return rc;
	}
	if (addr.type==VT_STRUCT && (cmd=VBIN::find(PROP_SPEC_COMMAND,addr.varray,addr.length))!=NULL) {
		switch (cmd->type) {
		default: break;
		case VT_STRUCT:
			//???
			break;
		}
	}
	return RC_OK;
}

void Device::release()
{
	mgr.release(this);
}

void Device::destroy() 
{
	IO *mg=&mgr; delete this; --mg->nCached;
}
Device::~Device()
{
	if (name.str!=NULL) mgr.ctx->free((void*)name.str);
}

void Device::operator delete(void *p)
{
	((Device*)p)->mgr.ctx->free(p);
}

Device *Device::createNew(const StrLen& id,void *mg)
{
	IO *si=(IO*)(IOHash*)mg; if (si->nCached>=si->xCached) return NULL;
	Device *ioc=si->createDevice(id); if (ioc!=NULL) si->nCached++;
	return ioc;
}

Device *IO::createDevice(const StrLen& id)
{
	return new(ctx) Device(id,*this);
}

Device *Serial::createDevice(const StrLen& id)
{
	return new(ctx) SerialDevice(id,*this);
}

RC SimpleIOProc::stop(bool fSuspend)
{
	//???
	return RC_OK;
}

void SimpleIOProc::disconnect()
{
	if (fd!=INVALID_SHANDLE) {
#ifdef WIN32
//		???
#else
//		ioctl.remove(this); ::close(fd); fd=INVALID_SHANDLE;
#endif
	}
}

void SimpleIOProc::destroy()
{
	this->~SimpleIOProc(); //mgr.processors.dealloc(this);
}

SimpleIOListener::~SimpleIOListener()
{
	freeV(vals,nVals,&sharedAlloc);
}

IService *SimpleIOListener::getService() const
{
	return &device.mgr;
}

RC SimpleIOListener::stop(bool fSuspend)
{
	if (!fSuspend) destroy(); return RC_OK;
}

void SimpleIOListener::process(ISession *ses,unsigned iobits)
{
	if ((iobits&E_BIT)!=0) {
		// report error
	} else if ((iobits&R_BIT)!=0) {
#if 0
		SocketProcessor *srv=new SocketProcessor(mgr.ctx,mgr,ISRV_SERVER,addr,asock);
		if (srv==NULL) {::closesocket(asock); report(MSG_ERROR,"SimpleIOListener: failed to allocate SocketProcessor\n"); break;}
		Session *ss=ses!=NULL?ses:Session::getSession(); if (ss==NULL) break; IServiceCtx *sctx=NULL;
		if ((rc=ss->createServiceCtx(vals,nVals,sctx,false,srv))!=RC_OK) {srv->destroy(); report(MSG_ERROR,"SimpleIOListener: failed in ISession::createServiceCtx() (%d)\n",rc); break;}
		srv->setServiceCtx(sctx);
		if (srv->wait(R_BIT)!=RC_OK) {srv->destroy(); report(MSG_ERROR,"SimpleIOListener: failed to add SocketProcessor (%d)\n",rc); break;}
		if ((mode&ISRV_TRANSIENT)!=0) {disconnect(); destroy(); break;}
#endif
	} else {
		report(MSG_WARNING,"Write state in SimpleIOListener\n");
	}
}

void SimpleIOListener::destroy()
{
	this->~SimpleIOListener(); sharedAlloc.free(this);
}

//-------------------------------------------------------------------------------------------------------------------------------------------------

namespace AfyKernel
{

#define	LSOCKET_ENUM		4
#define	BURST_ACCEPT		75
#define	DEFAULT_BUFFER_SIZE	(4*1024)

class Sockets;

struct SocketProcessor : public IOProcessor
{
	Sockets		&mgr;
	AddrInfo	addr;
	ServiceCtx	*sctx;
	SocketProcessor(StoreCtx *ct,Sockets& mg,unsigned ty,const AddrInfo& ad,SOCKET s=INVALID_SOCKET,ServiceCtx *sct=NULL)
		: IOProcessor(ct,ty,s),mgr(mg),sctx(sct) {memcpy(&addr,&ad,sizeof(AddrInfo)); if (s==INVALID_SOCKET) state|=CONNECT_BIT; if (sct!=NULL) state|=SERVER_BIT|INIT_BIT;}
	RC connect();
	void process(ISession *ses,unsigned iobits) {
		const bool fServer=(state&SERVER_BIT)!=0 && (state&(WWAIT_BIT|RWAIT_BIT))==0,fI=(state&(RESUME_BIT|INIT_BIT))!=0;
		if ((state&INIT_BIT)!=0) state&=~INIT_BIT; else IOProcessor::process(ses,iobits);
		if (fServer) {
			if (fI) {ioctl.remove(this); state&=~(R_BIT|W_BIT|E_BIT|L_BIT|RESUME_BIT);}
			if ((!fI || sctx->invoke(NULL,0)!=RC_REPEAT) && (state&L_BIT)==0) sctx->destroy();
		}
	}
	int	read(byte *buf,size_t lbuf) {return ::recv(fd,(char*)buf,(int)lbuf,0);}
	int write(const byte *buf,size_t lbuf) {return buf!=NULL&&lbuf!=0?::send(fd,(char*)buf,(int)lbuf,0):0;}
	void swmode() {shutdown(fd,SHUT_WR);}		// keep-alive ???
	int getError() const;
	void cleanup(IServiceCtx *ctx,bool fDestroying);
	void disconnect();
	void destroy();
};

struct SocketListener : public IOBlock, public IListener
{
	const	URIID	id;
	Sockets			&mgr;
	Value			*vals;
	unsigned		nVals;
	AddrInfo		addr;
	unsigned		mode;
	SOCKET			asock;
	sockaddr_storage sa; 
	socklen_t		lsa;
	SocketProcessor	*prc;
	SocketListener(URIID i,StoreCtx *ct,Sockets& mg,const AddrInfo &ai,SOCKET s,unsigned md) 
		: IOBlock(ct,s),id(i),mgr(mg),vals(NULL),nVals(0),mode(md),asock(INVALID_SOCKET),prc(NULL) {memcpy(&addr,&ai,sizeof(AddrInfo));}
	~SocketListener();
	void *operator new(size_t);
	IService *getService() const;
	URIID getID() const {return id;}
	RC create(IServiceCtx *ctx,uint32_t& dscr,IService::Processor *&ret);
	RC	stop(bool fSuspend);
	void process(ISession *ses,unsigned iobits);
	void disconnect();
	void destroy();
};

class Sockets : public IService
{
	friend	struct	SocketListener;
	friend	struct	SocketProcessor;
	StoreCtx		*const	ctx;

	static	FreeQ		listeners;
	static	ElementID	sockEnum[LSOCKET_ENUM];
	static	const char	*sockStr[LSOCKET_ENUM];
	static int getSocket(const AddrInfo& ai,SOCKET& sock,bool fListen=false) {
		int opt=1;
		if (ai.socktype!=SOCK_STREAM && ai.socktype!=SOCK_DGRAM) {
			report(MSG_ERROR,"unsupported socket type %d\n",ai.socktype); return -1;
		} else {
			sock=socket(ai.family,ai.socktype,ai.protocol);
			if (!isSOK(sock)) {report(MSG_ERROR,"socket failed with error: %ld\n",WSAGetLastError()); return -1;}
			if (ai.socktype==SOCK_STREAM && setsockopt(sock,IPPROTO_TCP,TCP_NODELAY,(char*)&opt,sizeof(opt))<0) report(MSG_WARNING,"setting TCP_NODELAY failed\n");
		}
		if (fListen) {
			if (ai.socktype==SOCK_DGRAM) {
				//if (fReuse) setsockopt(sock,SOL_SOCKET,SO_REUSEADDR,(char*)&opt,sizeof(opt));
				if (setsockopt(sock,SOL_SOCKET,SO_BROADCAST,(char*)&opt,sizeof(opt))!=0) {
				//...
				}
			}
			int res=::bind(sock,(sockaddr*)&ai.saddr,(int)ai.laddr);	
			if (res!=0) {report(MSG_ERROR,"bind failed with error: %d\n",WSAGetLastError()); ::closesocket(sock); return -1;}
			if (ai.socktype==SOCK_STREAM && ::listen(sock,SOMAXCONN)!=0) {report(MSG_ERROR,"listen failed with error: %d\n", WSAGetLastError()); ::closesocket(sock); return -1;}
			if (nbio(sock)!=RC_OK) {::closesocket(sock); return -1;}
		}
		return 0;
	}
public:
	Sockets(StoreCtx *ct) : ctx(ct) {}
	RC create(IServiceCtx *sctx,uint32_t& dscr,Processor *&ret) {
		ret=NULL; AddrInfo ai; RC rc;
		switch (dscr&ISRV_PROC_MASK) {
		default: return RC_INVOP;
		case ISRV_ENDPOINT|ISRV_READ: case ISRV_ENDPOINT|ISRV_REQUEST: dscr|=ISRV_ALLOCBUF; break;
		case ISRV_ENDPOINT|ISRV_WRITE: break;
		}
		const Value *addr=sctx->getParameter(PROP_SPEC_ADDRESS); ai.fUDP=(dscr&ISRV_ALT)!=0;
		if (addr==NULL) {
			if ((addr=sctx->getParameter(PROP_SPEC_RESOLVE))==NULL) return RC_NOTFOUND;
			if (addr->type!=VT_URIID) return RC_TYPE;
			if ((rc=((Session*)sctx->getSession())->resolve(sctx,addr->uid,ai))!=RC_OK) return rc;
		} else if ((rc=ai.resolve(addr,false))!=RC_OK) return rc;
		return (ret=new(sctx) SocketProcessor(ctx,*this,dscr&ISRV_PROC_MASK,ai))!=NULL?RC_OK:RC_NORESOURCES;
	}
	RC listen(ISession *ses,URIID id,const Value *vals,unsigned nVals,const Value *srvParams,unsigned nSrvparams,unsigned mode,IListener *&ret) {
		AddrInfo ai; SOCKET sock; RC rc;
		const Value *pv=Value::find(PROP_SPEC_SERVICE,srvParams,nSrvparams); if (pv==NULL) pv=Value::find(PROP_SPEC_SERVICE,vals,nVals);
		ai.fUDP=pv!=NULL && pv->type==VT_URIID && (pv->meta&META_PROP_ALT)!=0;
		if ((rc=ai.resolve(Value::find(PROP_SPEC_ADDRESS,vals,nVals),true))!=RC_OK) return rc;
		if (getSocket(ai,sock,true)!=0) return RC_OTHER;
		SocketListener *ls=new SocketListener(id,ctx,*this,ai,sock,mode);
		if (ls==NULL) {::closesocket(sock); return RC_NORESOURCES;}		
		rc=copyV(vals,ls->nVals=nVals,ls->vals,&sharedAlloc);
		if (rc==RC_OK && (rc=ioctl.add(ls,R_BIT))==RC_OK) ret=ls; else ls->destroy();
		return rc;
	}
	void shutdown() {ioctl.shutdown(ctx);}
};

};

RC AddrInfo::resolve(const Value *pv,bool fListener) {
	char buf[40],*p; memcpy(buf,"80",3); const char *psrv=buf,*paddr=NULL; 
	int flags=fListener?AI_PASSIVE|AI_NUMERICSERV:AI_NUMERICSERV;
	if (pv!=NULL) {
		switch (pv->type) {
		default: return RC_TYPE;
		case VT_INT: case VT_UINT:
			if (pv->ui==0||pv->ui>0xFFFF) return RC_INVPARAM;
			sprintf(buf,"%d",(uint16_t)pv->ui); break;
		case VT_STRING:
			if ((p=(char*)memchr(pv->str,':',pv->length))==NULL) paddr=pv->str;
			else if (p!=pv->str && p+1!=pv->str+pv->length && memchr(pv->str,'.',p-pv->str)!=NULL) {
				psrv=p+1; size_t l=p-pv->str;
				if (l<sizeof(buf)) {memcpy(buf,pv->str,l); buf[l]='\0'; paddr=buf;}
				else if ((p=(char*)alloca(l+1))!=NULL) {memcpy(p,pv->str,l); p[l]='\0'; paddr=p;}
				else return RC_NORESOURCES;
			} else {
				// assume IPv6 address
			}
			break;
		case VT_URL:
			//parse srv://[...]:port/...
			break;
		//case VT_STRUCT:
			//socket type
			// port
			// fReuse
			//	break;
		}
	}

	struct addrinfo hints,*result=NULL;
	memset(&hints,0,sizeof(hints));
	hints.ai_flags = flags;
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = fUDP?SOCK_DGRAM:SOCK_STREAM;
#ifndef ANDROID
	hints.ai_protocol = fUDP?IPPROTO_UDP:IPPROTO_TCP;
#endif

	int res = getaddrinfo(paddr,psrv,&hints,&result);
	if (res!=0) {report(MSG_ERROR,"getaddrinfo failed with error: %d\n",res); return convCode(res);}
	assert(result!=NULL);
	family=result->ai_family;
	socktype=result->ai_socktype;
	protocol=result->ai_protocol;
	laddr=(socklen_t)result->ai_addrlen;
	if (laddr>sizeof(sockaddr_storage)) {
		// error
	}
	memcpy(&saddr,result->ai_addr,laddr);
	freeaddrinfo(result);
	return RC_OK;
}

RC AddrInfo::getAddr(const sockaddr_storage *ss,socklen_t ls,Value& addr,IMemAlloc *ma,bool fPort)
{
	char buf[128],*p=buf; size_t l=0; addr.setEmpty();
	if (ss->ss_family==AF_INET6 && ls>=sizeof(sockaddr_in6)) {
		const sockaddr_in6 *sa=(sockaddr_in6*)ss; if (fPort) *p++='['; bool fComp=false;
		for (unsigned i=0; i<8; i++) {
			uint16_t seg=((uint16_t*)&sa->sin6_addr)[i];
			if (fComp || seg!=0) p+=sprintf(p,"%s%X",i!=0?":":"",seg);
			else for (fComp=true,*p++=':';;i++) if (i==7) {*p++=':'; break;} else if (((uint16_t*)&sa->sin6_addr)[i+1]!=0) break;
		}
		if (fPort) p+=sprintf(p,"]:%d",sa->sin6_port); l=p-buf;
	} else if (ss->ss_family==AF_INET && ls>=sizeof(sockaddr_in)) {
		const sockaddr_in *sa=(sockaddr_in*)ss;
		l=sprintf(buf,"%d.%d.%d.%d",*(uint32_t*)&sa->sin_addr>>24,*(uint32_t*)&sa->sin_addr>>16&0xFF,*(uint32_t*)&sa->sin_addr>>8&0xFF,*(uint32_t*)&sa->sin_addr&0xFF);
		if (fPort) l+=sprintf(buf+l,":%d",sa->sin_port);
	} else return RC_INVPARAM;
	if (l!=0) {
		if ((p=(char*)ma->malloc(l+1))==NULL) return RC_NORESOURCES;
		memcpy(p,buf,l); p[l]='\0'; addr.set(p,(uint32_t)l);
	}
	return RC_OK;
}

FreeQ Sockets::listeners;

SocketListener::~SocketListener()
{
	freeV(vals,nVals,&sharedAlloc);
}

void *SocketListener::operator new(size_t s)
{
	return Sockets::listeners.alloc(s);
}

IService *SocketListener::getService() const
{
	return &mgr;
}

RC SocketListener::stop(bool fSuspend)
{
	disconnect(); if (!fSuspend) destroy(); return RC_OK;
}


RC SocketListener::create(IServiceCtx *sctx,uint32_t& dscr,IService::Processor *&ret)
{
	switch (dscr&ISRV_PROC_MASK) {
	default: ret=NULL; return RC_INVOP;
	case ISRV_ENDPOINT|ISRV_READ: dscr|=ISRV_ALLOCBUF; break;
	case ISRV_ENDPOINT|ISRV_WRITE: if (prc!=NULL) {ret=prc; return RC_OK;} break;
	}
	RC rc=RC_OK; assert(asock!=INVALID_SOCKET); Value adr;
	if ((prc=new(sctx) SocketProcessor(mgr.ctx,mgr,ISRV_SERVER,addr,asock,(ServiceCtx*)sctx))==NULL) {::closesocket(asock); rc=RC_NORESOURCES;}
	else if (lsa!=0) {
		if ((rc=AddrInfo::getAddr(&sa,lsa,adr,sctx))==RC_OK) {adr.setPropID(PROP_SPEC_ADDRESS); adr.setOp(OP_ADD); rc=sctx->getCtxPIN()->modify(&adr,1);}	// cleanup adr!
		if (rc!=RC_OK) {prc->destroy(); prc=NULL; report(MSG_ERROR,"SocketListener: failed to set peer address (%d)\n",rc);}
	}
	asock=INVALID_SOCKET; lsa=0; ret=prc; return rc;
}

void SocketListener::process(ISession *ses,unsigned iobits)
{
	if ((iobits&E_BIT)!=0) {
		disconnect(); SOCKET sock;
		if (Sockets::getSocket(addr,sock,true)!=0) destroy();
		else {fd=sock; if (ioctl.add(this,R_BIT)!=RC_OK) {disconnect(); destroy();}}
	} else if ((iobits&R_BIT)!=0) for (unsigned nAcc=0; nAcc<BURST_ACCEPT; nAcc++) {
		if (addr.fUDP || addr.socktype==SOCK_DGRAM) {
			//???
		} else {
			RC rc; lsa=(socklen_t)sizeof(sa); asock=::accept(fd,(sockaddr*)&sa,&lsa); IServiceCtx *sctx;
			if (!isSOK(asock)) {
				int err=getSockErr(fd);
#ifdef WIN32
				if (err==WAIT_IO_COMPLETION) continue;
#endif
				if (!checkBlock(err)) report(MSG_ERROR,"SocketListener: accept error %d\n",err); break;
			} else if (nbio(asock)==RC_OK) {
				if ((rc=ses->createServiceCtx(vals,nVals,sctx,false,this))!=RC_OK) report(MSG_ERROR,"SocketListener: failed in ISession::createServiceCtx() (%d)\n",rc);
				else if ((rc=prc->wait(R_BIT))!=RC_OK) {sctx->destroy(); report(MSG_ERROR,"SocketListener: failed to add SocketProcessor (%d)\n",rc);}
				if ((mode&ISRV_TRANSIENT)!=0) {disconnect(); destroy(); break;}
			} else ::closesocket(asock);
			prc=NULL; asock=INVALID_SOCKET;
		}
	} else {
		report(MSG_WARNING,"Write state in SocketListener\n");
	}
}

void SocketListener::disconnect()
{
	if (fd!=INVALID_SHANDLE) {
		if (shutdown(fd,SHUT_RDWR)!=0) report(MSG_ERROR,"socket shutdown error %d\n",getSockErr(fd));
		ioctl.remove(this); ::closesocket(fd); fd=INVALID_SHANDLE;
	}
}

void SocketListener::destroy()
{
	this->~SocketListener(); mgr.listeners.dealloc(this);
}

RC SocketProcessor::connect()
{
	if (fd!=INVALID_SHANDLE) disconnect();
	SOCKET sock; if (Sockets::getSocket(addr,sock)!=0) return RC_OTHER;
	if (::connect(sock,(sockaddr*)&addr.saddr,(int)addr.laddr)!=0) {
		int err=WSAGetLastError(); RC rc=convCode(err);
		report(MSG_ERROR,"connect failed with error: %s(%d)\n",getErrMsg(rc),err);
		::closesocket(sock); return rc;
	}
	RC rc=nbio(sock); if (rc!=RC_OK) {::closesocket(sock); return rc;}
	fd=sock; return RC_OK;
}

void SocketProcessor::disconnect()
{
	if (fd!=INVALID_SHANDLE) {
		if (shutdown(fd,SHUT_RDWR)!=0) report(MSG_ERROR,"socket shutdown error %d\n",getSockErr(fd));
		if ((state&L_BIT)!=0) {ioctl.remove(this); state&=~(L_BIT|R_BIT|W_BIT|E_BIT);}
		::closesocket(fd); fd=INVALID_SHANDLE; state|=CONNECT_BIT;
	}
}

void SocketProcessor::cleanup(IServiceCtx *sctx,bool)
{
	IOProcessor::cleanup(sctx); state&=~(EREAD_BIT|EWRITE_BIT|RWAIT_BIT|WWAIT_BIT|RESUME_BIT);
	if (fd!=INVALID_SHANDLE) {::closesocket(fd); fd=INVALID_SHANDLE; state|=CONNECT_BIT;}
}

void SocketProcessor::destroy()
{
	IOProcessor::cleanup(sctx);
}

int SocketProcessor::getError() const
{
	return getSockErr(fd);
}

//---------------------------------------------------------------------------------------------------------------------

namespace AfyKernel
{

class StoreBridge : public IService
{
	StoreCtx	*const ctx;
public:
	StoreBridge(StoreCtx *ct) : ctx(ct) {}
	RC create(IServiceCtx *ctx,uint32_t& dscr,Processor *&ret) {
		return RC_INVOP;
	}
};

};

//---------------------------------------------------------------------------------------------------------------------

#include "protobuf.h"
#include "parser.h"
#include "regexp.h"
#include "crypt.h"

#ifdef AFFINITY_STATIC_LINK
extern "C" AFY_EXP	bool HTTP_initService(ISession *ses,const Value *pars,unsigned nPars,bool fNew);
extern "C" AFY_EXP	bool mDNS_initService(ISession *ses,const Value *pars,unsigned nPars,bool fNew);
extern "C" AFY_EXP	bool WEBAPP_initService(ISession *ses,const Value *pars,unsigned nPars,bool fNew);
extern "C" AFY_EXP	bool XML_initService(ISession *ses,const Value *pars,unsigned nPars,bool fNew);
extern "C" AFY_EXP	bool ZIGBEE_initService(ISession *ses,const Value *pars,unsigned nPars,bool fNew);
const static struct InitStaticService
{
	Len_Str	name;
	bool	(*initFunc)(ISession *ses,const Value *pars,unsigned nPars,bool fNew);
} initStaticServiceTab[] =
{
	{{S_L("http")},		HTTP_initService},
	{{S_L("mdns")},		mDNS_initService},
	{{S_L("webapp")},	WEBAPP_initService},
	{{S_L("xml")},		XML_initService},
	{{S_L("zigbee")},	ZIGBEE_initService}
};
RC StoreCtx::initStaticService(const char *name,size_t l,Session *ses,const Value *pars,unsigned nPars,bool fNew)
{
	for (unsigned i=0; i<unsigned(sizeof(initStaticServiceTab)/sizeof(initStaticServiceTab[0])); i++) {
		const InitStaticService& iss=initStaticServiceTab[i];
		if (iss.name.l==l && memcmp(iss.name.s,name,l)==0) return iss.initFunc(ses,pars,nPars,fNew)?RC_OK:RC_FALSE;
	}
	return RC_NOTFOUND;
}
#endif

RC StoreCtx::initBuiltinServices()
{
	RC rc; assert(serviceProviderTab==NULL && extFuncTab==NULL);
	if ((serviceProviderTab=new(this) ServiceProviderTab(STORE_HANDLERTAB_SIZE,(MemAlloc*)this))==NULL ||
		(extFuncTab=new(this) ExtFuncTab(STORE_EXTFUNCTAB_SIZE,(MemAlloc*)this))==NULL) return RC_NORESOURCES;
	if ((rc=registerService(SERVICE_IO,new(this) IO(this)))!=RC_OK) return rc;
	if ((rc=registerService(SERVICE_SOCKETS,new(this) Sockets(this)))!=RC_OK) return rc;
	if ((rc=registerService(SERVICE_PROTOBUF,new(this) ProtobufService(this)))!=RC_OK) return rc;
	if ((rc=registerService(SERVICE_JSON,new(this) JSONService(this)))!=RC_OK) return rc;
	if ((rc=registerService(SERVICE_PATHSQL,new(this) PathSQLService(this)))!=RC_OK) return rc;
	if ((rc=registerService(SERVICE_REGEX,new(this) RegexService(this)))!=RC_OK) return rc;
	if ((rc=registerService(SERVICE_BRIDGE,new(this) StoreBridge(this)))!=RC_OK) return rc;
	if ((rc=registerService(SERVICE_SERIAL,new(this) Serial(this)))!=RC_OK) return rc;
	if ((rc=registerService(SERVICE_ENCRYPTION,new(this) CryptService(this)))!=RC_OK) return rc;
	if ((rc=registerService(SERVICE_BRIDGE,new(this) StoreBridge(this)))!=RC_OK) return rc;

#ifdef WIN32
	WSADATA wsaData; int iResult=WSAStartup(MAKEWORD(2,2),&wsaData);
	if (iResult!=0) report(MSG_ERROR,"WSAStartup failed: %d\n", iResult);
#else
	signal(SIGPIPE,SIG_IGN);
	//???
#endif
	return RC_OK;
}
