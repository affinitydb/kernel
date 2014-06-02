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

Written by Mark Venguerov 2012-2014

**************************************************************************************/
#ifdef WIN32
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
#define READ_BIT	0x20000000
#define WRITE_BIT	0x10000000
#define	RWAIT_BIT	0x08000000
#define	WWAIT_BIT	0x04000000
#define	RESUME_BIT	0x02000000
#define	SERVER_BIT	0x01000000
#define	INIT_BIT	0x00800000
#define	CLOSE_BIT	0x00400000

#define	POLL_TIMEOUT	1000
#define	TCP_CONNECTION_TABLE_SIZE	32

#ifdef WIN32
#define	SHANDLE				SOCKET
#define	INVALID_SHANDLE		INVALID_SOCKET
#define	SHUT_WR				SD_SEND
#define	SHUT_RD				SD_RECEIVE
#define	SHUT_RDWR			SD_BOTH
#define DEFAULT_PORT_NAME	"COM"
inline	bool				checkBlock(int err) {return err==WSAEWOULDBLOCK;}
#define	pollfd				WSAPOLLFD
#define	poll				WSAPoll
#if defined(_WIN32_WINNT) && _WIN32_WINNT<_WIN32_WINNT_VISTA
#define SELECT_IMPL
#endif
#else
#include <poll.h>
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
	IAfySocket		*iob;
	SHANDLE			h;
	bool			fClose;
	AfySocketP() : iob(NULL),h(INVALID_SHANDLE),fClose(false) {}
	AfySocketP(SHANDLE sh) : iob(NULL),h(sh),fClose(false) {}
	AfySocketP(IAfySocket *io) : iob(io),h(io!=NULL?(SHANDLE)io->getSocket():INVALID_SHANDLE),fClose(false) {}
	__forceinline void operator=(const AfySocketP& rhs) {iob=rhs.iob; h=rhs.h; fClose=rhs.fClose;}
	__forceinline static int cmp(const AfySocketP& ip,SHANDLE h) {return cmp3((SHANDLE)ip,h);}
	__forceinline operator SHANDLE() const {return h;}
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

struct IOQueueElt
{
	IOQueueElt	*next;
	IAfySocket	*iob;
	unsigned	iobits;
};

static LIFO ioQueueBlocks(&sharedAlloc,0x20);

class IOCtl
{
	RWLock					lock;
	volatile HTHREAD		thread;
	volatile THREADID		threadId;
	volatile bool			fWaiting;
	volatile bool			fRemoved;
	volatile bool			fStop;
	SharedCounter			mark;
	RWLock					qlock;
	IOQueueElt				*queue;
	DynOArrayBufV<AfySocketP,SHANDLE,AfySocketP,128> ioblocks;
#ifdef WIN32
	HANDLE					ioPort;
#ifdef SELECT_IMPL
	fd_set					R,W,E;
public:
	IOCtl() : thread((HTHREAD)0),threadId((THREADID)0),fWaiting(false),fRemoved(false),fStop(false),ioblocks(&sharedAlloc),queue(NULL),ioPort(NULL) {FD_ZERO(&R); FD_ZERO(&W); FD_ZERO(&E);}
#else
	DynArray<pollfd,128>	fdfs;
public:
	IOCtl() : thread((HTHREAD)0),threadId((THREADID)0),fWaiting(false),fRemoved(false),fStop(false),queue(NULL),ioblocks(&sharedAlloc),ioPort(NULL),fdfs(&sharedAlloc) {}
#endif
#else
	DynArray<pollfd,128>	fdfs;
	struct PipeBlock : public Afy::IAfySocket {
		SOCKET		spipe[2];
		PipeBlock() {spipe[0]=spipe[1]=INVALID_SOCKET;}
		SOCKET		getSocket() const {return spipe[0];}
		IAffinity	*getAffinity() const {return NULL;}
		ISession	*getSession() const {return NULL;}
		void		process(ISession *ses,unsigned bits) {if ((bits&R_BIT)!=0) {char buf[20]; while (read(spipe[0],buf,sizeof(buf))==sizeof(buf));}}
	}						sp;
public:
	IOCtl() : thread((HTHREAD)0),fWaiting(false),fRemoved(false),fStop(false),queue(NULL),ioblocks(&sharedAlloc),fdfs(&sharedAlloc) {}
#endif
	RC	add(IAfySocket *iob,unsigned iobits) {
		IAffinity *aff=iob->getAffinity();
		if (aff!=NULL && (((StoreCtx*)aff)->mode&STARTUP_RT)!=0) return RC_INVPARAM;
		SHANDLE sh=(SHANDLE)iob->getSocket(); if (sh==INVALID_SHANDLE) return RC_INVPARAM;
		RWLockP lck(&qlock,RW_X_LOCK); 
		if (aff!=NULL && ((StoreCtx*)aff)->inShutdown()) return RC_SHUTDOWN;
		IOQueueElt *ie=(IOQueueElt*)ioQueueBlocks.alloc(sizeof(IOQueueElt));
		if (ie==NULL) return RC_NOMEM; ie->next=queue; ie->iob=iob; ie->iobits=iobits; queue=ie;
		return fWaiting?(interrupt(),RC_OK):start();
	}
	void remove(IAfySocket *iob,bool fClose) {
		SHANDLE fd=(SHANDLE)iob->getSocket();
		if (fd!=INVALID_SHANDLE) {
			RWLockP lck(&qlock,RW_X_LOCK);
			for (IOQueueElt **pie=&queue,*ie; (ie=*pie)!=NULL; pie=&ie->next)
				if (ie->iob==iob) {*pie=ie->next; ioQueueBlocks.dealloc(ie); if (fClose) ::closesocket(fd); return;}
			lck.set(NULL); lck.set(&lock,RW_X_LOCK); AfySocketP *sp=(AfySocketP*)ioblocks.find(fd);
			if (sp!=NULL && sp->iob==iob) {sp->iob=NULL; sp->fClose=fClose; fRemoved=true; if (fWaiting) interrupt();}
		}
	}
	void set(IAfySocket *iob,unsigned iobits,unsigned mask) {
		RWLockP lck(&lock,RW_X_LOCK); SHANDLE fd=(SHANDLE)iob->getSocket();
		unsigned idx=ioblocks.findIdx(fd);
		if (idx!=~0u && ioblocks[idx].iob==iob) {
#ifndef SELECT_IMPL
			pollfd &pfd=const_cast<pollfd&>(fdfs[idx]);
			if ((mask&R_BIT)!=0) if ((iobits&R_BIT)!=0) pfd.events|=POLLIN; else pfd.events&=~POLLIN;
			if ((mask&W_BIT)!=0) if ((iobits&W_BIT)!=0) pfd.events|=POLLOUT; else pfd.events&=~POLLOUT;
#else
			if ((mask&R_BIT)!=0) if ((iobits&R_BIT)!=0) FD_SET(fd,&R); else FD_CLR(fd,&R);
			if ((mask&W_BIT)!=0) if ((iobits&W_BIT)!=0) FD_SET(fd,&W); else FD_CLR(fd,&W);
#endif
			if (fWaiting) interrupt();
		}
	}
	void threadProc() {
		if (!casP((void *volatile*)&thread,(void*)0,(void*)getThread())) return;
		threadId=getThreadId();
		
		Session *ioSes=Session::createSession(NULL); if (ioSes==NULL) return;
		
#ifndef WIN32
		if (pipe(sp.spipe)<0) report(MSG_WARNING,"Failed to create poll interrupt pipe (%d)\n",errno);
		else {nbio(sp.spipe[0]); nbio(sp.spipe[1]); if (add(&sp,R_BIT)!=RC_OK) report(MSG_WARNING,"Failed to add poll interrupt pipe\n");}
#elif defined(SELECT_IMPL)
		fd_set r,w,e;
#endif
		lock.lock(RW_X_LOCK);
		for (;;++mark) {
			while (queue!=NULL) {
				IOQueueElt *ie=queue; queue=ie->next;
				unsigned i=0; SHANDLE sh=(SHANDLE)ie->iob->getSocket();
				RC rc=ioblocks.add(AfySocketP(ie->iob),&i);
				if (rc==RC_OK) {
#ifndef SELECT_IMPL
					pollfd pfd={sh,((ie->iobits&R_BIT)!=0?POLLIN:0)|((ie->iobits&W_BIT)!=0?POLLOUT:0),0};
					rc=fdfs.add(i,pfd);
#else
					if ((ie->iobits&R_BIT)!=0) FD_SET(sh,&R); else FD_CLR(sh,&R);
					if ((ie->iobits&W_BIT)!=0) FD_SET(sh,&W); else FD_CLR(sh,&W);
					FD_SET(sh,&E);
#endif
				}
				if (rc!=RC_OK) report(MSG_ERROR,"Failed to add socket %X to polling (%d)\n",sh,rc);
				ioQueueBlocks.dealloc(ie);
			}
			fWaiting=true; fRemoved=false; int nfds=(int)ioblocks,res;
#ifdef SELECT_IMPL
			timeval timeout={POLL_TIMEOUT/1000,0}; r=R; w=W; e=E; lock.unlock(); res=::select(nfds,&r,&w,&e,&timeout);
#else
			int timeout=POLL_TIMEOUT; lock.unlock(); res=nfds!=0?::poll(&fdfs[0],nfds,timeout):(threadSleep(timeout,true),0);
#endif
			lock.lock(RW_X_LOCK); fWaiting=false; if (fStop) break;
			if (res<0) {
				int err=WSAGetLastError();
#ifdef WIN32
				if (nfds!=0 && err!=WAIT_IO_COMPLETION && err!=WAIT_TIMEOUT) {
#else
				if (nfds!=0 && err!=EAGAIN && err!=EINTR) {
#endif
					report(MSG_ERROR,"Socket poll error: %d\n",err);
				}
			} else {
				int n=0; lock.unlock();
				for (unsigned idx=0; idx<ioblocks && n<res; idx++) {
					IAfySocket *iob=ioblocks[idx].iob; if (iob==NULL) continue; unsigned iobits=0;
#ifdef SELECT_IMPL
					SHANDLE fd=(SHANDLE)iob->getSocket(); assert(fd!=INVALID_SHANDLE);
					if (FD_ISSET(fd,&r)!=0) iobits|=R_BIT;
					if (FD_ISSET(fd,&w)!=0) iobits|=W_BIT;
					if (FD_ISSET(fd,&e)!=0) iobits|=E_BIT;
#else
					const pollfd &pfd=fdfs[idx];
					if ((pfd.revents&POLLIN)!=0) iobits|=R_BIT; if ((pfd.revents&POLLOUT)!=0) iobits|=W_BIT;
					if ((pfd.revents&(POLLERR|POLLNVAL))!=0||(pfd.revents&(POLLHUP|POLLIN))==POLLHUP) iobits|=E_BIT;	// delayed EOF if POLLIN
#endif
					if (iobits!=0) {
						Session *ses=(Session*)iob->getSession(); n++;
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
				}
				lock.lock(RW_X_LOCK);
			}
			if (fRemoved) for (uint32_t i=ioblocks; i--!=0;) if (ioblocks[i].iob==NULL) {
#ifdef SELECT_IMPL
				SHANDLE fd=ioblocks[i].h; FD_CLR(fd,&R); FD_CLR(fd,&W); FD_CLR(fd,&E);
#endif
				if (ioblocks[i].fClose) ::closesocket(ioblocks[i].h);
				ioblocks.remove(i);
#ifndef SELECT_IMPL
				fdfs-=i;
#endif
			}
		}
		lock.unlock();
	}
	RC		start();
	void	interrupt();
	void	shutdown(StoreCtx *ctx) {
		qlock.lock(RW_X_LOCK); assert(ctx->inShutdown());
		for (IOQueueElt **pie=&queue,*ie; (ie=*pie)!=NULL;)
			if (ie->iob->getAffinity()==ctx) {*pie=ie->next; ::closesocket((SHANDLE)ie->iob->getSocket()); ioQueueBlocks.dealloc(ie);} else pie=&ie->next;
		qlock.unlock();
		if (thread!=(HTHREAD)0) {
			lock.lock(RW_X_LOCK); long old=mark; bool fWait=false;
			for (uint32_t i=ioblocks; i--!=0;) {
				IAfySocket *iob=ioblocks[i].iob;
				if (iob!=NULL && iob->getAffinity()==ctx) {ioblocks[i].iob=NULL; ioblocks[i].fClose=true; fRemoved=true;}
			}
			if (fRemoved && fWaiting) {interrupt(); fWait=true;}
			lock.unlock(); while (fWait && mark==old) threadYield();
		}
	}
	bool isIOThread() const {return getThreadId()==threadId;}
	void stopThread() {
		lock.lock(RW_X_LOCK); fStop=true; if (fWaiting) interrupt(); lock.unlock();
		// wait for thread to terminate?
	}
};
	
#ifdef WIN32
static DWORD WINAPI _iothread(void *param) {((IOCtl*)param)->threadProc(); return 0;}
static VOID CALLBACK apc(ULONG_PTR) {}
void IOCtl::interrupt() {if (::QueueUserAPC(apc,thread,NULL)==0) report(MSG_ERROR,"QueueUserAPC failed with code %d\n",GetLastError());}
#else
static void *_iothread(void *param) {((IOCtl*)param)->threadProc(); return NULL;}
void IOCtl::interrupt() {char c=0; write(sp.spipe[1],&c,1);}
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

void StoreCtx::unregisterSocket(IAfySocket *s,bool fClose)
{
	ioctl.remove(s,fClose);
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
	unsigned			rtimeout;
	Mutex				lock;
	WaitEvent			r_wait;
	WaitEvent			w_wait;
	IOBuf				queue[MAX_WRITE_BUFFERS];
	volatile unsigned	first,nbufs;
	volatile RC			rrc,wrc;
	IServiceCtx			*sctx;
	bool				fSock;
	uint64_t			nwritten;
	uint64_t			nread;
public:
	IOProcessor(StoreCtx *ctx,unsigned ty,SHANDLE h=INVALID_SHANDLE,unsigned rt=DEFAULT_READ_TIMEOUT)
		: IOBlock(ctx,h),type(ty),state(0),rtimeout(rt),first(0),nbufs(0),rrc(RC_OK),wrc(RC_OK),sctx(NULL),fSock(true),nwritten(0),nread(0) {}
	RC invoke(IServiceCtx *ctx,const Value& inp,Value& out,unsigned& mode) {
		RC rc=RC_OK; const bool fIOT=ioctl.isIOThread(); int res,err; sctx=ctx;
		if ((mode&ISRV_WRITE)!=0) {
			if ((type&(ISRV_WRITE|ISRV_SERVER))==0 || !inp.isEmpty() && !isString((ValueType)inp.type)) return RC_INVPARAM;
			if (inp.isEmpty() || inp.bstr==NULL || inp.length==0) {int res=write(NULL,0); if (res<0) ctx->error(SET_COMM,convCode(getError())); return RC_OK;}	// return native error in info
			MutexP lck(&lock); 
			if ((state&EWRITE_BIT)!=0) ctx->error(SET_COMM,wrc);
			else for (uint32_t left=inp.length;;) {
				if (nbufs==0) {
					if ((res=write(inp.bstr+inp.length-left,left))>=0) {nwritten+=res; if ((left-=res)==0) break;}
					else if (!checkBlock(err=getError())) {state|=EWRITE_BIT; ctx->error(SET_COMM,wrc=convCode(err)); break;}
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
			if ((type&(ISRV_READ|ISRV_SERVER))==0 || out.type!=VT_BSTR || out.bstr==NULL || out.length==0) return RC_INVPARAM;
			const uint32_t lbuf=out.length; out.length=0; 
			if ((state&EREAD_BIT)!=0) {
				if (rrc!=RC_EOF) ctx->error(SET_COMM,rrc/*,???*/); else rc=RC_EOF;
			} else do {
				if ((res=read((byte*)out.str,lbuf))>0) {nread+=res; out.length=uint32_t(res); break;}
				if (res==0) {state|=EREAD_BIT; rc=rrc=RC_EOF; break;}
				if (!checkBlock(err=getError())) {state|=EREAD_BIT; ctx->error(SET_COMM,rrc=convCode(err)); break;}
				if ((mode&ISRV_WAIT)==0) break;
				if (fIOT) {if ((rc=wait(R_BIT|RESUME_BIT))==RC_OK) mode|=ISRV_RESUME; break;}
				lock.lock(); if ((rc=wait(R_BIT|RWAIT_BIT))==RC_OK) rc=r_wait.wait(lock,rtimeout); lock.unlock();
			} while (rc==RC_OK);
		}
		return rc;
	}
	virtual void process(ISession *,unsigned iobits) {
		MutexP lck(&lock); int res;
		if ((iobits&E_BIT)!=0) {
			rrc=wrc=convCode(getError()); if ((state&L_BIT)!=0) ioctl.remove(this,true); else ::closesocket(fd);
			state=(state&~(R_BIT|W_BIT|E_BIT|L_BIT|READ_BIT|WRITE_BIT))|EREAD_BIT|EWRITE_BIT; fd=INVALID_SOCKET;
			if ((state&RWAIT_BIT)!=0) {state&=~RWAIT_BIT; r_wait.signal();}
			if ((state&WWAIT_BIT)!=0) {state&=~WWAIT_BIT; w_wait.signal();}
		} else {
			if ((iobits&R_BIT)!=0 && (state&(R_BIT|RWAIT_BIT))==(R_BIT|RWAIT_BIT)) {
				if ((state&W_BIT)==0) {ioctl.remove(this,false); state&=~(R_BIT|W_BIT|E_BIT|L_BIT);} else {ioctl.set(this,0,R_BIT); state&=~R_BIT;}
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
				if ((state&R_BIT)==0) {ioctl.remove(this,false); state&=~(R_BIT|W_BIT|E_BIT|L_BIT);} else {ioctl.set(this,0,W_BIT); state&=~W_BIT;}
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
		if (((state^bits)&(R_BIT|W_BIT))!=0) ioctl.set(this,state|=bits,R_BIT|W_BIT);
		return RC_OK;
	}
	virtual	void cleanup(IServiceCtx *sctx) {
		if ((state&L_BIT)!=0) {ioctl.remove(this,false); state&=~(L_BIT|R_BIT|W_BIT|E_BIT);}
	}
	virtual	int getError() const = 0;
	virtual	int	read(byte *buf,size_t lbuf) = 0;
	virtual	int	write(const byte *buf,size_t lbuf) = 0;
	virtual	void disconnect(bool fKeepalive=false) = 0;
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
	void disconnect(bool);
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
		Device *ioc=NULL;
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
		case ISRV_ENDPOINT|ISRV_READ|ISRV_WRITE:
			//???
		case ISRV_ENDPOINT|ISRV_READ:
			dscr|=ISRV_ALLOCBUF;
		case ISRV_ENDPOINT|ISRV_WRITE:
			if ((ret=new(sctx) SimpleIOProc(ctx,dscr&ISRV_PROC_MASK,*ioc,pos,stype))==NULL) rc=RC_NOMEM;
			break;
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
		case VT_STRING:
			addr.str=pv->str; addr.len=pv->length; break;
		case VT_STRUCT:
			// find name field
			break;
		}
		return addr.str==NULL?RC_INVPARAM:(addr.str=strdup(addr.str,ctx))==NULL?RC_NOMEM:RC_OK;
	}
	virtual Device *createDevice(const StrLen& id);
	virtual RC listen(ISession *ses,URIID id,const Value *vals,unsigned nVals,const Value *srvParams,unsigned nSrvparams,unsigned mode,IListener *&ret) {
		Device *ioc=NULL; RC rc=getDevice(Value::find(PROP_SPEC_ADDRESS,vals,nVals),ioc); if (rc!=RC_OK || ioc==NULL) return rc;
		SimpleIOListener *ls=new(&sharedAlloc) SimpleIOListener(id,ctx,*this,*ioc,mode);
		if (ls==NULL) {ioc->release(); return RC_NOMEM;}
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
		return (addr.str=strdup(buf,ctx))!=NULL?RC_OK:RC_NOMEM;
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
	assert(handle==INVALID_HANDLE_VALUE); unsigned flags=addr.meta; RC rc;
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
		DWORD ff=((flags&(META_PROP_READ|META_PROP_WRITE))!=META_PROP_WRITE?GENERIC_READ:0)|((flags&META_PROP_WRITE)!=0?GENERIC_WRITE:0);
		DWORD disp=(flags&META_PROP_CREATE)==0?OPEN_EXISTING:OPEN_ALWAYS,flg=FILE_ATTRIBUTE_NORMAL; if ((flags&META_PROP_ASYNC)!=0) flg|=FILE_FLAG_OVERLAPPED;
		if ((handle=::CreateFile(name.str,ff,FILE_SHARE_READ|FILE_SHARE_WRITE,NULL,disp,flg,NULL))==INVALID_HANDLE_VALUE) return convCode(GetLastError());
#else
		int ff=(flags&(META_PROP_READ|META_PROP_WRITE))==(META_PROP_READ|META_PROP_WRITE)?O_RDWR:(flags&META_PROP_WRITE)!=0?O_WRONLY:O_RDONLY;
		if ((flags&META_PROP_CREATE)!=0) ff|=O_CREAT; if ((flags&META_PROP_ASYNC)!=0) ff|=O_NONBLOCK;
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

void SimpleIOProc::disconnect(bool)
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
	SockAddr	addr;
	ServiceCtx	*sctx;
	SocketProcessor(StoreCtx *ct,Sockets& mg,unsigned ty,const SockAddr& ad,SOCKET s=INVALID_SOCKET,ServiceCtx *sct=NULL,unsigned rt=DEFAULT_READ_TIMEOUT)
		: IOProcessor(ct,ty,s,rt),mgr(mg),addr(ad),sctx(sct) {if (s==INVALID_SOCKET) state|=CLOSE_BIT; else if (ad.socktype==SOCK_STREAM) state|=CLOSE_BIT; if (sct!=NULL) state|=SERVER_BIT|INIT_BIT;}
	RC connect(IServiceCtx *);
	void resetClose() {state&=~CLOSE_BIT;}
	void process(ISession *ses,unsigned iobits) {
		const bool fServer=(state&SERVER_BIT)!=0 && (state&(WWAIT_BIT|RWAIT_BIT))==0,fI=(state&(RESUME_BIT|INIT_BIT))!=0;
		if ((state&INIT_BIT)!=0) state&=~INIT_BIT; else IOProcessor::process(ses,iobits);
		if (fServer) {
			if (fI) {ioctl.remove(this,false); state&=~(R_BIT|W_BIT|E_BIT|L_BIT|RESUME_BIT);}
			if ((!fI || sctx->invoke(NULL,0)!=RC_REPEAT) && (state&L_BIT)==0) sctx->destroy();
		}
	}
	int	read(byte *buf,size_t lbuf) {return ::recv(fd,(char*)buf,(int)lbuf,0);}
	int write(const byte *buf,size_t lbuf) {return buf!=NULL&&lbuf!=0?::send(fd,(char*)buf,(int)lbuf,0):0;}
	int getError() const;
	void cleanup(IServiceCtx *ctx,bool fDestroying);
	void disconnect(bool);
	void destroy();
};

struct SocketListener : public IOBlock, public IListener
{
	const	URIID	id;
	Sockets			&mgr;
	Value			*vals;
	unsigned		nVals;
	SockAddr		addr;
	unsigned		mode;
	SOCKET			asock;
	sockaddr_storage sa; 
	socklen_t		lsa;
	SocketProcessor	*prc;
	SocketListener(URIID i,StoreCtx *ct,Sockets& mg,const SockAddr &ai,SOCKET s,unsigned md) 
		: IOBlock(ct,s),id(i),mgr(mg),vals(NULL),nVals(0),addr(ai),mode(md),asock(INVALID_SOCKET),prc(NULL) {}
	~SocketListener();
	void *operator new(size_t);
	IService *getService() const;
	URIID getID() const {return id;}
	RC create(IServiceCtx *ctx,uint32_t& dscr,IService::Processor *&ret);
	RC	stop(bool fSuspend);
	void process(ISession *ses,unsigned iobits);
	void disconnect(bool);
	void destroy();
};

struct TCPConnectionHolder : public SockAddr
{
	SOCKET						sock;
	HChain<TCPConnectionHolder>	list;
	TCPConnectionHolder(const SockAddr &sa,SOCKET s) : SockAddr(sa),sock(s),list(this) {assert(s!=INVALID_SOCKET);}
	const SockAddr& getKey() const {return *this;}
};

typedef SyncHashTab<TCPConnectionHolder,const IAddress&,&TCPConnectionHolder::list> TCPConnTable;

class Sockets : public IService
{
	friend	struct	SocketListener;
	friend	struct	SocketProcessor;
	StoreCtx		*const	ctx;

	static	TCPConnTable	*volatile TCPConns;
	static	LIFO			TCPCns;
	static	LIFO			listeners;
	static	ElementID		sockEnum[LSOCKET_ENUM];
	static	const char		*sockStr[LSOCKET_ENUM];
	static int getSocket(const SockAddr& ai,SOCKET& sock,bool fListen=false) {
		int opt=1;
		if (ai.socktype!=SOCK_STREAM && ai.socktype!=SOCK_DGRAM) {report(MSG_ERROR,"unsupported socket type %d\n",ai.socktype); return -1;}
		sock=socket(ai.saddr.ss_family,ai.socktype,ai.protocol);
		if (!isSOK(sock)) {report(MSG_ERROR,"socket failed with error: %ld\n",WSAGetLastError()); return -1;}
		if (ai.socktype==SOCK_STREAM && setsockopt(sock,IPPROTO_TCP,TCP_NODELAY,(char*)&opt,sizeof(opt))<0) report(MSG_WARNING,"setting TCP_NODELAY failed\n");
		if (fListen) {
			if (nbio(sock)!=RC_OK) {::closesocket(sock); return -1;}
			setsockopt(sock,SOL_SOCKET,SO_REUSEADDR,(char*)&opt,sizeof(opt));
			int res=::bind(sock,(sockaddr*)&ai.saddr,(int)ai.laddr);	
			if (res!=0) {report(MSG_ERROR,"bind failed with error: %d\n",WSAGetLastError()); ::closesocket(sock); return -1;}
			if (ai.socktype==SOCK_STREAM && ::listen(sock,SOMAXCONN)!=0) {report(MSG_ERROR,"listen failed with error: %d\n", WSAGetLastError()); ::closesocket(sock); return -1;}
		}
		return 0;
	}
public:
	Sockets(StoreCtx *ct) : ctx(ct) {}
	RC create(IServiceCtx *sctx,uint32_t& dscr,Processor *&ret) {
		ret=NULL;
		switch (dscr&ISRV_PROC_MASK) {
		default: return RC_INVOP;
		case ISRV_ENDPOINT|ISRV_READ: 
		case ISRV_ENDPOINT|ISRV_READ|ISRV_WRITE: dscr|=ISRV_ALLOCBUF;
		case ISRV_ENDPOINT|ISRV_WRITE: break;
		}
		return (ret=new(sctx) SocketProcessor(ctx,*this,dscr&ISRV_PROC_MASK,SockAddr()))!=NULL?RC_OK:RC_NOMEM;
	}
	RC listen(ISession *ses,URIID id,const Value *vals,unsigned nVals,const Value *srvParams,unsigned nSrvparams,unsigned mode,IListener *&ret) {
		SockAddr ai; SOCKET sock; RC rc;
		const Value *pv=Value::find(PROP_SPEC_SERVICE,srvParams,nSrvparams); if (pv==NULL) pv=Value::find(PROP_SPEC_SERVICE,vals,nVals);
#ifndef ANDROID
		if (pv!=NULL && pv->type==VT_URIID && (pv->meta&META_PROP_ALT)!=0) {ai.protocol=IPPROTO_UDP; ai.socktype=SOCK_DGRAM;}
#else
		if (pv!=NULL && pv->type==VT_URIID && (pv->meta&META_PROP_ALT)!=0) ai.socktype=SOCK_DGRAM;
#endif
		if ((rc=ai.resolve(Value::find(PROP_SPEC_ADDRESS,vals,nVals)))!=RC_OK) return rc;
		if (getSocket(ai,sock,true)!=0) return RC_OTHER;
		SocketListener *ls=new SocketListener(id,ctx,*this,ai,sock,mode);
		if (ls==NULL) {::closesocket(sock); return RC_NOMEM;}		
		rc=copyV(vals,ls->nVals=nVals,ls->vals,&sharedAlloc);
		if (rc==RC_OK && (rc=ioctl.add(ls,R_BIT))==RC_OK) ret=ls; else ls->destroy();
		return rc;
	}
	void shutdown() {ioctl.shutdown(ctx);}
};

};

RC SockAddr::resolve(const Value *pv,IServiceCtx *sctx) {
	char srv[10]; srv[0]=0; const char *paddr=NULL,*p,*q; size_t l,ll; 
	int flags=sctx==NULL?AI_PASSIVE|AI_NUMERICSERV:AI_NUMERICSERV;
	if (pv!=NULL) {
		switch (pv->type) {
		default: return RC_TYPE;
		case VT_INT: case VT_UINT:
			if (pv->ui==0||pv->ui>0xFFFF) return RC_INVPARAM;
			sprintf(srv,"%d",(uint16_t)pv->ui); break;
		case VT_STRING:
			for (q=pv->str,l=pv->length,p=NULL; l!=0 && *q==' ';++q) --l; if (l==0) break;
			if (*q>='0'&&*q<='9'&&memchr(q,'.',l)==NULL) {l=max(l,sizeof(srv)-1); memcpy(srv,q,l); srv[l]=0; break;}	// check all isdigit
			if (*q!='[' && (*q<'0'||*q>'9')&&(p=(char*)memchr(q,':',l))!=NULL && p+3<=q+l && p[1]=='/' && p[2]=='/') {l-=size_t(p+3-q); q=p+3; p=NULL;}
			paddr=q;
			if (*q=='[') {
				if ((p=(char*)memchr(q,']',l))!=NULL && ++p==q+l) p=NULL;
			} else if (p!=NULL || (p=(char*)memchr(q,':',l))!=NULL) {
				const char *s=p+1,*e=q+l; for (ll=0; ll+1<sizeof(srv) && s<e; ll++) srv[ll]=*s++; srv[ll]=0;
			} else p=(char*)memchr(q,'/',l);
			if (p!=NULL && p!=q) {
				l=size_t(p-q); if ((paddr=(char*)alloca(l+1))==NULL) return RC_NOMEM;
				memcpy((char*)paddr,q,l); ((char*)paddr)[l]=0;
			}
			break;
		//case VT_STRUCT:
			//socket type
			// port
			// fReuse
			//	break;
		}
	}
	
	int proto=-1; uint16_t port=0;
	if (sctx!=NULL) {
		((ServiceCtx*)sctx)->getSocketDefaults(proto,port);		//default from other services
		if (srv[0]=='\0' && port!=0) sprintf(srv,"%d",port);
	}

	struct addrinfo hints,*result=NULL;
	memset(&hints,0,sizeof(hints));
	hints.ai_flags = flags;
	hints.ai_family = (pv->meta&META_PROP_READ)!=0?AF_INET:AF_UNSPEC;
	hints.ai_socktype = socktype;
#ifndef ANDROID
	hints.ai_protocol = proto>=0?proto:0;
#endif

	int res = getaddrinfo(paddr,srv,&hints,&result);
	if (res!=0) {report(MSG_ERROR,"getaddrinfo failed with error: %d\n",res); return convCode(res);}
	assert(result!=NULL);
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

bool SockAddr::operator==(const IAddress& rhs) const
{
	const SockAddr &r=(const SockAddr&)rhs;
	if (protocol!=r.protocol || socktype!=r.socktype || laddr!=r.laddr || saddr.ss_family!=r.saddr.ss_family) return false;
	switch (saddr.ss_family) {
	case AF_INET:
		return ((sockaddr_in&)saddr).sin_port==((sockaddr_in&)r.saddr).sin_port && memcmp(&((sockaddr_in&)saddr).sin_addr,&((sockaddr_in&)r.saddr).sin_addr,sizeof(((sockaddr_in&)saddr).sin_addr))==0;
	case AF_INET6:
		return ((sockaddr_in6&)saddr).sin6_port==((sockaddr_in6&)r.saddr).sin6_port && memcmp(&((sockaddr_in6&)saddr).sin6_addr,&((sockaddr_in6&)r.saddr).sin6_addr,sizeof(((sockaddr_in6&)saddr).sin6_addr))==0;
	default:
		return memcmp(&saddr,&r.saddr,laddr)==0;
	}
}

int SockAddr::cmp(const IAddress &rhs) const
{
	const SockAddr &r=(const SockAddr&)rhs; int c;
	if ((c=cmp3(protocol,r.protocol))!=0||(c=cmp3(socktype,r.socktype))!=0||(c=cmp3(saddr.ss_family,r.saddr.ss_family))!=0) return c;
	switch (saddr.ss_family) {
	case AF_INET:
		return (c=cmp3(((sockaddr_in&)saddr).sin_port,((sockaddr_in&)r.saddr).sin_port))!=0?c:sign(memcmp(&((sockaddr_in&)saddr).sin_addr,&((sockaddr_in&)r.saddr).sin_addr,sizeof(((sockaddr_in&)saddr).sin_addr)));
	case AF_INET6:
		return (c=cmp3(((sockaddr_in6&)saddr).sin6_port,((sockaddr_in6&)r.saddr).sin6_port))!=0?c:sign(memcmp(&((sockaddr_in6&)saddr).sin6_addr,&((sockaddr_in6&)r.saddr).sin6_addr,sizeof(((sockaddr_in6&)saddr).sin6_addr)));
	default:
		return sign(memcmp(&saddr,&r.saddr,laddr));
	}
}

SockAddr::operator uint32_t() const
{
	const byte *p; size_t l; uint32_t hash=saddr.ss_family;
	switch (saddr.ss_family) {
	case AF_INET:
		hash=hash*31^((sockaddr_in&)saddr).sin_port; p=(byte*)&((sockaddr_in&)saddr).sin_addr; l=sizeof(&((sockaddr_in&)saddr).sin_addr); break;
	case AF_INET6:
		hash=hash*31^((sockaddr_in6&)saddr).sin6_port; p=(byte*)&((sockaddr_in6&)saddr).sin6_addr; l=sizeof(&((sockaddr_in6&)saddr).sin6_addr); break;
	default:
		p=(byte*)&saddr; l=laddr; break;
	}
	for (size_t i=0; i<l; i++) hash=hash<<1^p[i];
	return hash;
}

RC SockAddr::getAddr(const sockaddr_storage *ss,socklen_t ls,Value& addr,IMemAlloc *ma,bool fPort)
{
	char buf[128],*p=buf; size_t l=0; addr.setEmpty();
	if (ss->ss_family==AF_INET6 && ls>=sizeof(sockaddr_in6)) {
		const sockaddr_in6 *sa=(sockaddr_in6*)ss; if (fPort) *p++='['; bool fComp=false;
		for (int i=8; --i>=0; ) {
			uint16_t seg=((uint16_t*)&sa->sin6_addr)[i];
			if (fComp || seg!=0) p+=sprintf(p,"%s%X",i!=0?":":"",seg);
			else for (fComp=true,*p++=':';;i--) if (i==0) {*p++=':'; break;} else if (((uint16_t*)&sa->sin6_addr)[i-1]!=0) break;
		}
		if (fPort) p+=sprintf(p,"]:%d",sa->sin6_port); l=p-buf;
	} else if (ss->ss_family==AF_INET && ls>=sizeof(sockaddr_in)) {
		const sockaddr_in *sa=(sockaddr_in*)ss;
		l=sprintf(buf,"%d.%d.%d.%d",*(uint32_t*)&sa->sin_addr&0xFF,*(uint32_t*)&sa->sin_addr>>8&0xFF,*(uint32_t*)&sa->sin_addr>>16&0xFF,*(uint32_t*)&sa->sin_addr>>24);
		if (fPort) l+=sprintf(buf+l,":%d",sa->sin_port);
	} else return RC_INVPARAM;
	if (l!=0) {
		if ((p=(char*)ma->malloc(l+1))==NULL) return RC_NOMEM;
		memcpy(p,buf,l); p[l]='\0'; addr.set(p,(uint32_t)l);
	}
	return RC_OK;
}

TCPConnTable	*volatile	Sockets::TCPConns=NULL;
LIFO						Sockets::TCPCns;
LIFO						Sockets::listeners;

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
	disconnect(false); if (!fSuspend) destroy(); return RC_OK;
}


RC SocketListener::create(IServiceCtx *sctx,uint32_t& dscr,IService::Processor *&ret)
{
	switch (dscr&ISRV_PROC_MASK) {
	default: ret=NULL; return RC_INVOP;
	case ISRV_ENDPOINT|ISRV_READ: dscr|=ISRV_ALLOCBUF; break;
	case ISRV_ENDPOINT|ISRV_WRITE: if (prc!=NULL) {ret=prc; return RC_OK;} break;
	}
	RC rc=RC_OK; assert(asock!=INVALID_SOCKET); Value adr; unsigned rtimeout=DEFAULT_READ_TIMEOUT;
	const Value *timeout=sctx->getParameter(PROP_SPEC_INTERVAL);
	if (timeout!=NULL) {
		if (timeout->type==VT_INTERVAL) rtimeout=(unsigned)(timeout->i64/1000);
		else if (timeout->type==VT_UINT||timeout->type==VT_INT&&timeout->i>=0) rtimeout=timeout->ui;
	}
	if ((prc=new(sctx) SocketProcessor(mgr.ctx,mgr,ISRV_SERVER,addr,asock,(ServiceCtx*)sctx,rtimeout))==NULL) {if (asock!=fd) ::closesocket(asock); rc=RC_NOMEM;}
	else {
		if (asock==fd) prc->resetClose();
		if (lsa!=0) {
			if ((rc=SockAddr::getAddr(&sa,lsa,adr,sctx))==RC_OK) {adr.setPropID(PROP_SPEC_ADDRESS); adr.setOp(OP_ADD); rc=sctx->getCtxPIN()->modify(&adr,1);}	// cleanup adr!
			if (rc!=RC_OK) {prc->destroy(); prc=NULL; report(MSG_ERROR,"SocketListener: failed to set peer address (%d)\n",rc);}
		}
	}
	asock=INVALID_SOCKET; lsa=0; ret=prc; return rc;
}

void SocketListener::process(ISession *ses,unsigned iobits)
{
	if ((iobits&E_BIT)!=0) {
		disconnect(false); SOCKET sock;
		if (Sockets::getSocket(addr,sock,true)!=0) destroy();
		else {fd=sock; if (ioctl.add(this,R_BIT)!=RC_OK) {disconnect(false); destroy();}}
	} else if ((iobits&R_BIT)!=0) for (unsigned nAcc=0; nAcc<BURST_ACCEPT; nAcc++) {
		RC rc; IServiceCtx *sctx;
		if (addr.socktype==SOCK_DGRAM) asock=fd;
		else {
			lsa=(socklen_t)sizeof(sa); asock=::accept(fd,(sockaddr*)&sa,&lsa);
			if (!isSOK(asock)) {
				int err=getSockErr(fd);
#ifdef WIN32
				if (err==WAIT_IO_COMPLETION) continue;
#endif
				if (!checkBlock(err)) report(MSG_ERROR,"SocketListener: accept error %d\n",err); break;
			}
		}
		if ((rc=ses->createServiceCtx(vals,nVals,sctx,false,this))!=RC_OK) report(MSG_ERROR,"SocketListener: failed in ISession::createServiceCtx() (%d)\n",rc);
		else if ((rc=prc->wait(R_BIT))!=RC_OK) {sctx->destroy(); report(MSG_ERROR,"SocketListener: failed to add SocketProcessor (%d)\n",rc);}
		if ((mode&ISRV_TRANSIENT)!=0) {disconnect(false); destroy(); break;}
		prc=NULL; asock=INVALID_SOCKET; if (addr.socktype==SOCK_DGRAM) break;
	} else {
		report(MSG_WARNING,"Write state in SocketListener\n");
	}
}

void SocketListener::disconnect(bool)
{
	if (fd!=INVALID_SHANDLE) {ioctl.remove(this,true); fd=INVALID_SHANDLE;}
}

void SocketListener::destroy()
{
	this->~SocketListener(); mgr.listeners.dealloc(this);
}

RC SocketProcessor::connect(IServiceCtx *sctx)
{
	if (fd!=INVALID_SHANDLE) return RC_OK;
	const Value *ad=sctx->getParameter(PROP_SPEC_ADDRESS); RC rc; 
	if (ad==NULL) {
		if ((ad=sctx->getParameter(PROP_SPEC_RESOLVE))==NULL) return RC_NOTFOUND;
		if (ad->type==VT_URIID && (rc=((Session*)sctx->getSession())->resolve(sctx,ad->uid,addr))!=RC_OK) return rc;
	} else if ((rc=addr.resolve(ad,sctx))!=RC_OK) return rc;
	if (addr.socktype==SOCK_STREAM && mgr.TCPConns!=NULL) {
		TCPConnTable::Find tcpf(*mgr.TCPConns,addr); TCPConnectionHolder *conn=tcpf.findLock(RW_X_LOCK);
		if (conn!=NULL) {fd=conn->sock; tcpf.remove(conn); mgr.TCPCns.dealloc(conn); return RC_OK;}
	}
	SOCKET sock; if (Sockets::getSocket(addr,sock)!=0) return RC_OTHER;
	if (::connect(sock,(sockaddr*)&addr.saddr,(int)addr.laddr)!=0) {
		int err=WSAGetLastError(); RC rc=convCode(err);
		report(MSG_ERROR,"connect failed with error: %s(%d)\n",getErrMsg(rc),err);
		::closesocket(sock); return rc;
	}
	if ((rc=nbio(sock))!=RC_OK) {::closesocket(sock); return rc;}
	fd=sock; return RC_OK;
}

void SocketProcessor::disconnect(bool fKeepalive)
{
	if (fd!=INVALID_SHANDLE) {
		void *p;
		if (fKeepalive && addr.socktype==SOCK_STREAM && (p=mgr.TCPCns.alloc(sizeof(TCPConnectionHolder)))!=NULL) {
			if (mgr.TCPConns==NULL) {
				TCPConnTable *tct=new(&sharedAlloc) TCPConnTable(TCP_CONNECTION_TABLE_SIZE,&sharedAlloc);
				if (!casP(&mgr.TCPConns,(TCPConnTable*)0,tct)) {tct->~TCPConnTable(); sharedAlloc.free(tct);}
			}
			TCPConnectionHolder *tch=new(p) TCPConnectionHolder(addr,fd); mgr.TCPConns->insert(tch);
		} else if ((state&CLOSE_BIT)!=0) {
			if ((state&L_BIT)==0) ::closesocket(fd); else {ioctl.remove(this,true); state&=~(L_BIT|R_BIT|W_BIT|E_BIT);}
		}
		fd=INVALID_SHANDLE; state|=CLOSE_BIT;
	}
}

void SocketProcessor::cleanup(IServiceCtx *sctx,bool)
{
	IOProcessor::cleanup(sctx); state&=~(EREAD_BIT|EWRITE_BIT|RWAIT_BIT|WWAIT_BIT|RESUME_BIT);
	disconnect(sctx!=NULL?((ServiceCtx*)sctx)->isKeepalive():false);
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
extern "C" AFY_EXP	bool BLE_initService(ISession *ses,const Value *pars,unsigned nPars,bool fNew);
extern "C" AFY_EXP	bool MODBUS_initService(ISession *ses,const Value *pars,unsigned nPars,bool fNew);
extern "C" AFY_EXP	bool LOCALSENSOR_initService(ISession *ses,const Value *pars,unsigned nPars,bool fNew);
const static struct InitStaticService
{
	Len_Str	name;
	bool	(*initFunc)(ISession *ses,const Value *pars,unsigned nPars,bool fNew);
} initStaticServiceTab[] =
{
#if defined(STATIC_LINK_ALL_SERVICES) || defined(STATIC_LINK_HTTP_SERVICE)
	{{S_L("http")},			HTTP_initService},
#endif
#if defined(STATIC_LINK_ALL_SERVICES) || defined(STATIC_LINK_MDNS_SERVICE)
	{{S_L("mdns")},			mDNS_initService},
#endif
#if defined(STATIC_LINK_ALL_SERVICES) || defined(STATIC_LINK_WEBAPP_SERVICE)
	{{S_L("webapp")},		WEBAPP_initService},
#endif
#if defined(STATIC_LINK_ALL_SERVICES) || defined(STATIC_LINK_XML_SERVICE)
	{{S_L("xml")},			XML_initService},
#endif
#if defined(STATIC_LINK_ALL_SERVICES) || defined(STATIC_LINK_ZIGBEE_SERVICE)
	{{S_L("zigbee")},		ZIGBEE_initService},
#endif
#if defined(STATIC_LINK_ALL_SERVICES) || defined(STATIC_LINK_BLE_SERVICE)
	{{S_L("ble")},			BLE_initService},
#endif
#if defined(STATIC_LINK_ALL_SERVICES) || defined(STATIC_LINK_MODBUS_SERVICE)
	{{S_L("modbus")},		MODBUS_initService},
#endif
#if defined(STATIC_LINK_ALL_SERVICES) || defined(STATIC_LINK_LOCALSENSOR_SERVICE)
	{{S_L("localsensor")},	LOCALSENSOR_initService},
#endif
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
		(extFuncTab=new(this) ExtFuncTab(STORE_EXTFUNCTAB_SIZE,(MemAlloc*)this))==NULL) return RC_NOMEM;
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
