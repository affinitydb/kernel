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

Written by Mark Venguerov 2012-2013

**************************************************************************************/
#ifdef WIN32
#define WIN32_LEAN_AND_MEAN
#define	FD_SETSIZE	1024
#endif

#include "afysock.h"
#include "qmgr.h"
#include "service.h"

using namespace	Afy;
using namespace AfyKernel;

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
	
static RC getOSError()
{
#ifdef WIN32
	return convCode(GetLastError());
#else
	return convCode(errno);
#endif
}

#define L_BIT		0x0008

#define EREAD_BIT	0x8000
#define EWRITE_BIT	0x4000
#define EOF_BIT		0x2000
#define CONNECT_BIT	0x1000
#define READ_BIT	0x0800
#define WRITE_BIT	0x0400

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
	volatile bool			inSelect;
	DynOArrayBuf<AfySocketP,SHANDLE,AfySocketP,128>	ioblocks;
#ifdef WIN32
	HANDLE					ioPort;
public:
	IOCtl() : thread((HTHREAD)0),inSelect(false),ioblocks(&sharedAlloc),ioPort(NULL) {FD_ZERO(&R); FD_ZERO(&W); FD_ZERO(&E);}
#else
	SOCKET					spipe[2];
public:
	IOCtl() : thread((HTHREAD)0),inSelect(false),ioblocks(&sharedAlloc) {spipe[0]=spipe[1]=INVALID_SOCKET; FD_ZERO(&R); FD_ZERO(&W); FD_ZERO(&E);}
#endif
	RC	add(IAfySocket *iob,unsigned iobits) {
		SHANDLE sh=(SHANDLE)iob->getSocket(); if (sh==INVALID_SHANDLE) return RC_INVPARAM;
		RWLockP lck(&lock,RW_X_LOCK); RC rc=ioblocks+=AfySocketP(iob);
		if (rc==RC_OK) {
			if ((iobits&R_BIT)!=0) FD_SET(sh,&R); else FD_CLR(sh,&R);
			if ((iobits&W_BIT)!=0) FD_SET(sh,&W); else FD_CLR(sh,&W);
			FD_SET(sh,&E); if (inSelect) interrupt();
		}
		return rc==RC_OK?start():rc;
	}
	void remove(IAfySocket *iob) {
		SHANDLE fd=(SHANDLE)iob->getSocket();
		if (fd!=INVALID_SHANDLE) {
			RWLockP lck(&lock,RW_X_LOCK); ioblocks-=AfySocketP(iob);
			FD_CLR(fd,&R); FD_CLR(fd,&W); FD_CLR(fd,&E);
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
		
		Session *ioSes=Session::createSession(NULL); if (ioSes==NULL) return;
		ioSes->setIdentity(STORE_OWNER,true);
		
#ifndef WIN32
		if (pipe(spipe)<0) report(MSG_WARNING,"Failed to create select pipe (%d)\n",errno);
		else {FD_SET(spipe[0],&R); FD_SET(spipe[1],&W);}
#endif
		for (fd_set r,w,e;;) {
			timeval timeout={1,0};
			lock.lock(RW_X_LOCK); int nst=getNFDs(); r=R; w=W; e=E; inSelect=true; lock.unlock();
			int num=::select(nst,&r,&w,&e,&timeout);
			if (num<0) {
				int err=WSAGetLastError();
#ifdef WIN32
				if (err==WAIT_IO_COMPLETION) continue;
#else
				if (err==EAGAIN || err==EINTR) continue;
#endif
				break;	//???
			}
			lock.lock(RW_X_LOCK); inSelect=false; nst=(int)ioblocks; lock.unlock();
			for (int n=0,i=0; i<nst && n<num; i++) {
#ifndef WIN32
				if (i==spipe[0]) {if (FD_ISSET(i,&r)) {char b; read(spipe[0],&b,1);} continue;}
#endif
				lock.lock(RW_X_LOCK); IAfySocket *iob=ioblocks[i].iob; lock.unlock(); if (iob==NULL) continue;

				unsigned iobits=0; SHANDLE fd=(SHANDLE)iob->getSocket();
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
					iob->process(ses!=NULL?ses:ioSes,iobits);
					if (ses==NULL) ioSes->set(NULL);
					else {
						//ses->detachThread();
						//ioSes->attachThread();
					}
				}
			}
		}
	}
	RC		start();
	void	interrupt();
	int		getNFDs() const;
	void	shutdown(StoreCtx *ctx) {
		lock.lock(RW_X_LOCK);
		for (uint32_t i=0; i<ioblocks; i++) {
			IAfySocket *iob=ioblocks[i].iob;
			if (iob!=NULL && iob->getAffinity()==ctx) {
				SOCKET sock=iob->getSocket(); if (sock!=INVALID_SOCKET) ::closesocket(sock);
			}
		}
		lock.unlock();
	}
};
	
#ifdef WIN32
static DWORD WINAPI _iothread(void *param) {((IOCtl*)param)->threadProc(); return 0;}
static VOID NTAPI apc(ULONG_PTR) {
	// need to do something here
}
void IOCtl::interrupt() {::QueueUserAPC(apc,thread,NULL);}
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
	unsigned			state;
	Mutex				lock;
	Event				r_wait;
	Event				w_wait;
	int					refCnt;
	IOBuf				rbuf;
	IOBuf				queue[MAX_WRITE_BUFFERS];
	unsigned			first;
	unsigned			nbufs;
	RC					rrc,wrc;
	bool				fSock;
public:
	IOProcessor(StoreCtx *ctx,unsigned ty,SHANDLE h=INVALID_SHANDLE)
		: IOBlock(ctx,h),type(ty),state(0),refCnt(0),first(0),nbufs(0),rrc(RC_OK),wrc(RC_OK),fSock(true) {rbuf.buf=NULL; rbuf.lbuf=rbuf.left=0;}
	RC invoke(IServiceCtx *ctx,const Value& inp,Value& out,unsigned& mode) {
		RC rc=RC_OK;
		if ((state&CONNECT_BIT)!=0) {
			assert(fd==INVALID_SHANDLE); state&=~CONNECT_BIT;
			if ((rc=connect())!=RC_OK) {state|=EREAD_BIT|EWRITE_BIT; return rc;}
		}
		if ((mode&ISRV_SWITCH)!=0) swmode();
		if ((mode&ISRV_WRITE)!=0) {
			if ((type&(ISRV_WRITE|ISRV_REQUEST|ISRV_SERVER))==0 || !inp.isEmpty() && !isString((ValueType)inp.type)) return RC_INVPARAM;
			if (inp.isEmpty() || inp.bstr==NULL || inp.length==0) {int res=write(NULL,0); return res<0?convCode(getError()):RC_OK;}
			lock.lock(); if ((state&EWRITE_BIT)!=0) {lock.unlock(); return wrc;}
			for (;;) {
				size_t left=inp.length;
				if (nbufs==0) {
					lock.unlock(); int res=write(inp.bstr,(int)inp.length);
					if (res<0) {int err=getError(); if (!checkBlock(err)) {state|=EWRITE_BIT; return wrc=convCode(err);}}
					else if ((left-=res)==0) return RC_OK;
					lock.lock();
				}
				if (nbufs<MAX_WRITE_BUFFERS) {
					IOBuf& iob=queue[(first+nbufs++)%MAX_WRITE_BUFFERS];
					iob.buf=(byte*)inp.bstr; iob.lbuf=inp.length; iob.left=left;
					RC rc=wait(W_BIT); if (rc!=RC_OK) {lock.unlock(); return rc;}
					if ((mode&ISRV_WAIT)==0) {lock.unlock(); return RC_OK;}
				}
				w_wait.wait(lock,0); if ((state&EWRITE_BIT)!=0) {lock.unlock(); return wrc;}
			}
		} else {
			if ((type&(ISRV_READ|ISRV_REQUEST|ISRV_SERVER))==0 || out.type!=VT_BSTR && out.bstr==NULL || out.length==0) return RC_INVPARAM;
			if ((state&EREAD_BIT)!=0) return rrc; if ((state&EOF_BIT)!=0) return RC_EOF;
			int res=read((byte*)out.str,out.length); if (res==0) {state|=EOF_BIT; return RC_EOF;}
			if (res>0) out.length=uint32_t(res);
			else {
				int err=getError(); if (!checkBlock(err)) {state|=EREAD_BIT; return rrc=convCode(err);}
				if ((mode&ISRV_WAIT)==0) out.length=0;
				else {
					MutexP lck(&lock); rbuf.buf=(byte*)out.bstr; rbuf.left=rbuf.lbuf=out.length;
					RC rc=wait(R_BIT); if (rc!=RC_OK) return rc;
					for (;;) {
						r_wait.wait(lock,0); if ((state&EREAD_BIT)!=0) return rrc;
						if ((out.length=(uint32_t)(rbuf.lbuf-rbuf.left))!=0) break;
						if ((state&EOF_BIT)!=0) {out.length=0; return RC_EOF;}
					}
				}
			}
		}
		return RC_OK;
	}
	virtual void process(ISession *,unsigned iobits) {
		MutexP lck(&lock);
		if ((iobits&E_BIT)!=0) {
			rrc=wrc=convCode(getError()); if ((state&L_BIT)!=0) ioctl.remove(this);
			disconnect(); state=(state&~(R_BIT|W_BIT|E_BIT|L_BIT|READ_BIT|WRITE_BIT))|EREAD_BIT|EWRITE_BIT|EOF_BIT;
		} else {
			if ((iobits&R_BIT)!=0) {
				bool fRem=true,fSig=true;
				if (rbuf.buf!=NULL && rbuf.left!=0) {
					lck.set(NULL); int res=read(rbuf.buf+rbuf.lbuf-rbuf.left,rbuf.left); lck.set(&lock);
					if (res>0) {rbuf.left-=res; fRem=false;} else if (res==0) state|=EOF_BIT;
					else {int err=getError(); if (checkBlock(err)) fSig=false; else {state|=EREAD_BIT; rrc=convCode(err);}}
				}
				if (fRem && (state&L_BIT)!=0) {if ((state&W_BIT)==0) {ioctl.remove(this); state&=~(R_BIT|L_BIT);} else {ioctl.reset(this,R_BIT); state&=~R_BIT;}}
				if (fSig) r_wait.signal();
			}
			if ((iobits&W_BIT)!=0) {
				if (nbufs!=0) {
					lck.set(NULL);
					int res=write(queue[first].buf+queue[first].lbuf-queue[first].left,queue[first].left);
					if (res<0) {int err=getError(); if (checkBlock(err)) res=0; else {wrc=convCode(err); state|=EWRITE_BIT; return;}}
					lck.set(&lock);
					if ((queue[first].left-=res)!=0) return;
					//release queue[first].buf
					first=(first+1)%MAX_WRITE_BUFFERS; --nbufs;
					w_wait.signal(); if (nbufs!=0) return;
				}
				if ((state&L_BIT)!=0) {if ((state&R_BIT)==0) {ioctl.remove(this); state&=~(W_BIT|L_BIT);} else {ioctl.reset(this,W_BIT); state&=~W_BIT;}}
			}
		}
	}
	RC wait(unsigned bits) {
#ifdef WIN32
		if (!fSock) {
			//???
		} else
#endif
		if ((state&L_BIT)==0) {state|=L_BIT|bits; return ioctl.add(this,bits);} 
		if ((state&bits)!=bits) {state|=bits; ioctl.set(this,bits);}
		return RC_OK;
	}
	void incRef() {refCnt++;}
	virtual	void cleanup(IServiceCtx *sctx,bool fDestroy) {if ((state&L_BIT)!=0) ioctl.remove(this); state=CONNECT_BIT;}
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
	IO			&mgr;
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
	void cleanup(IServiceCtx *ctx,bool fDestroy) {IOProcessor::cleanup(ctx,fDestroy); if (fDestroy) {device.release(); ctx->free(this);}}
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
	IO				&mgr;
	Device			&device;
	Value			*vals;
	unsigned		nVals;
	unsigned		mode;
	SimpleIOProc	*prc;
#ifdef WIN32
	SimpleIOListener(StoreCtx *ct,IO& mg,Device& dev,unsigned md) : mgr(mg),device(dev),vals(NULL),nVals(0),mode(md),prc(NULL) {}
#else
	SimpleIOListener(StoreCtx *ct,IO& mg,Device& dev,unsigned md) : IOBlock(ct,(SHANDLE)dev.handle),mgr(mg),device(dev),vals(NULL),nVals(0),mode(md),prc(NULL) {}
#endif
	~SimpleIOListener();
	IService *getService() const;
	RC create(IServiceCtx *ctx,uint32_t& dscr,IService::Processor *&ret) {
		if (prc==NULL) return RC_INTERNAL;
		switch (dscr&ISRV_PROC_MASK) {
		default: ret=NULL; return RC_INVOP;
		case ISRV_ENDPOINT|ISRV_READ: dscr|=ISRV_ALLOCBUF;
		case ISRV_ENDPOINT|ISRV_WRITE: ret=prc; prc->incRef(); break;
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
	virtual RC listen(ISession *ses,const Value *vals,unsigned nVals,const Value *srvParams,unsigned nSrvparams,unsigned mode,IListener *&ret) {
		Device *ioc=NULL; RC rc=getDevice(Value::find(PROP_SPEC_ADDRESS,vals,nVals),ioc); if (rc!=RC_OK || ioc==NULL) return rc;
		SimpleIOListener *ls=new(&sharedAlloc) SimpleIOListener(ctx,*this,*ioc,mode);
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
//		ioctl.remove(this); ::close(fd); fd=INVALID_SHANDLE;
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
	SocketProcessor(StoreCtx *ct,Sockets& mg,unsigned ty,const AddrInfo& ad,SOCKET s=INVALID_SOCKET)
		: IOProcessor(ct,ty,s),mgr(mg),sctx(NULL) {memcpy(&addr,&ad,sizeof(AddrInfo)); if (s==INVALID_SOCKET) state|=CONNECT_BIT;}
	void *operator new(size_t);
	RC connect();
	void setServiceCtx(IServiceCtx *srv) {sctx=(ServiceCtx*)srv;}
	void process(ISession *ses,unsigned iobits) {
		if ((iobits&(E_BIT|R_BIT))!=R_BIT || sctx==NULL) IOProcessor::process(ses,iobits);
		else {ServiceCtx *sc=sctx; sctx=NULL; sc->invoke(NULL,0); sc->destroy();}
	}
	int	read(byte *buf,size_t lbuf) {return ::recv(fd,(char*)buf,(int)lbuf,0);}
	int write(const byte *buf,size_t lbuf) {return buf!=NULL&&lbuf!=0?::send(fd,(char*)buf,(int)lbuf,0):0;}
	void swmode() {shutdown(fd,SHUT_WR);}
	int getError() const;
	void cleanup(IServiceCtx *ctx,bool fDestroy);
	void disconnect();
	void destroy();
};

struct SocketListener : public IOBlock, public IListener
{
	Sockets			&mgr;
	Value			*vals;
	unsigned		nVals;
	AddrInfo		addr;
	unsigned		mode;
	SocketProcessor	*prc;
	SocketListener(StoreCtx *ct,Sockets& mg,const AddrInfo &ai,SOCKET s,unsigned md) : IOBlock(ct,s),mgr(mg),vals(NULL),nVals(0),mode(md),prc(NULL) {memcpy(&addr,&ai,sizeof(AddrInfo));}
	~SocketListener();
	void *operator new(size_t);
	IService *getService() const;
	RC create(IServiceCtx *ctx,uint32_t& dscr,IService::Processor *&ret) {
		if (prc==NULL) return RC_INTERNAL;
		switch (dscr&ISRV_PROC_MASK) {
		default: ret=NULL; return RC_INVOP;
		case ISRV_ENDPOINT|ISRV_READ: dscr|=ISRV_ALLOCBUF;
		case ISRV_ENDPOINT|ISRV_WRITE: ret=prc; prc->setServiceCtx(ctx); prc->incRef(); break;
		}
		return RC_OK;
	}
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

	static	FreeQ		processors;
	static	FreeQ		listeners;
	static	ElementID	sockEnum[LSOCKET_ENUM];
	static	const char	*sockStr[LSOCKET_ENUM];
	static int getSocket(const AddrInfo& ai,SOCKET& sock,bool fListen=false) {
		if (ai.socktype!=SOCK_STREAM && ai.socktype!=SOCK_DGRAM) {
			report(MSG_ERROR,"unsupported socket type %d\n",ai.socktype); return -1;
		} else {
			sock=socket(ai.family,ai.socktype,ai.protocol);
			if (!isSOK(sock)) {report(MSG_ERROR,"socket failed with error: %ld\n",WSAGetLastError()); return -1;}
		}
		if (fListen) {
			if (ai.socktype==SOCK_DGRAM) {
				int opt=1;
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
		return (ret=new SocketProcessor(ctx,*this,dscr&ISRV_PROC_MASK,ai))!=NULL?RC_OK:RC_NORESOURCES;
	}
	RC listen(ISession *ses,const Value *vals,unsigned nVals,const Value *srvParams,unsigned nSrvparams,unsigned mode,IListener *&ret) {
		AddrInfo ai; SOCKET sock; RC rc;
		const Value *pv=Value::find(PROP_SPEC_SERVICE,srvParams,nSrvparams); if (pv==NULL) pv=Value::find(PROP_SPEC_SERVICE,vals,nVals);
		ai.fUDP=pv!=NULL && pv->type==VT_URIID && (pv->meta&META_PROP_ALT)!=0;
		if ((rc=ai.resolve(Value::find(PROP_SPEC_ADDRESS,vals,nVals),true))!=RC_OK) return rc;
		if (getSocket(ai,sock,true)!=0) return RC_OTHER;
		SocketListener *ls=new SocketListener(ctx,*this,ai,sock,mode);
		if (ls==NULL) {::closesocket(sock); return RC_NORESOURCES;}		
		rc=copyV(vals,ls->nVals=nVals,ls->vals,&sharedAlloc);
		if (rc==RC_OK && (rc=ioctl.add(ls,R_BIT))==RC_OK) ret=ls; else ls->destroy();
		return rc;
	}
	static	RC	nbio(SOCKET sock) {
#ifdef WIN32
		unsigned long mode=1; int res=ioctlsocket(sock,FIONBIO,&mode);
		if (res!=0) {report(MSG_ERROR,"FIONBIO failed(%d)\n",getError(sock)); return RC_OTHER;}
#else
		int res=fcntl(sock,F_GETFL,0); res=fcntl(sock,F_SETFL,res|O_NONBLOCK);
		if (res<0) {int err=getError(sock); report(MSG_ERROR,"O_NONBLOCK failed(%d)\n",err); return convCode(err);}
#endif
		return RC_OK;

	}
	static	int getError(SOCKET s) {
#if 0
		int errcode=0; socklen_t errlen=sizeof(errcode);
		getsockopt(s,SOL_SOCKET,SO_ERROR,(char*)&errcode,&errlen);
		return errcode;
#else
		return WSAGetLastError();
#endif
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
		case VT_INT: case VT_UINT: sprintf(buf,"%d",(uint16_t)pv->ui); break;
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

FreeQ Sockets::processors;
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
			RC rc; socklen_t l=(socklen_t)addr.laddr; SOCKET asock=::accept(fd,(sockaddr*)&addr.saddr,&l); IServiceCtx *sctx;
			if (!isSOK(asock)) {
				int err=mgr.getError(fd);
#ifdef WIN32
				if (err==WAIT_IO_COMPLETION) continue;
#endif
				if (!checkBlock(err)) report(MSG_ERROR,"SocketListener: accept error %d\n",err); break;
			} else if (Sockets::nbio(asock)==RC_OK) {
				if ((prc=new SocketProcessor(mgr.ctx,mgr,ISRV_SERVER,addr,asock))==NULL) {::closesocket(asock); report(MSG_ERROR,"SocketListener: failed to allocate SocketProcessor\n"); break;}
				if ((rc=ses->createServiceCtx(vals,nVals,sctx,false,this))!=RC_OK) {prc->destroy(); prc=NULL; report(MSG_ERROR,"SocketListener: failed in ISession::createServiceCtx() (%d)\n",rc); break;}
				if (prc->wait(R_BIT)!=RC_OK) {prc->destroy(); prc=NULL; report(MSG_ERROR,"SocketListener: failed to add SocketProcessor (%d)\n",rc); break;} else prc=NULL;
				if ((mode&ISRV_TRANSIENT)!=0) {disconnect(); destroy(); break;}
			} else ::closesocket(asock);
		}
	} else {
		report(MSG_WARNING,"Write state in SocketListener\n");
	}
}

void SocketListener::disconnect()
{
	if (fd!=INVALID_SHANDLE) {
		if (shutdown(fd,SHUT_RDWR)!=0) report(MSG_ERROR,"socket shutdown error %d\n",Sockets::getError(fd));
		ioctl.remove(this); ::closesocket(fd); fd=INVALID_SHANDLE;
	}
}

void SocketListener::destroy()
{
	this->~SocketListener(); mgr.listeners.dealloc(this);
}

void *SocketProcessor::operator new(size_t s)
{
	return Sockets::processors.alloc(s);
}

RC SocketProcessor::connect()
{
	if (fd!=INVALID_SHANDLE) disconnect();
	SOCKET sock; if (Sockets::getSocket(addr,sock)!=0) return RC_OTHER;
	if (::connect(sock,(sockaddr*)&addr.saddr,(int)addr.laddr)!=0) {
		report(MSG_ERROR,"connect failed with error: %d\n", WSAGetLastError());
		::closesocket(sock); return RC_OTHER;
	}
	RC rc=Sockets::nbio(sock); if (rc!=RC_OK) {::closesocket(sock); return rc;}
	fd=sock; return RC_OK;
}

void SocketProcessor::disconnect()
{
	if (fd!=INVALID_SHANDLE) {
		if (shutdown(fd,SHUT_RDWR)!=0) report(MSG_ERROR,"socket shutdown error %d\n",Sockets::getError(fd));
		if ((state&L_BIT)!=0) {ioctl.remove(this); state&=~(L_BIT|R_BIT|W_BIT|E_BIT);}
		::closesocket(fd); fd=INVALID_SHANDLE;
	}
}

void SocketProcessor::cleanup(IServiceCtx *sctx,bool fDestroy)
{
	IOProcessor::cleanup(sctx,fDestroy);
	if (fd!=INVALID_SHANDLE) {::closesocket(fd); fd=INVALID_SHANDLE; state|=CONNECT_BIT;}
	if (fDestroy && --refCnt<=0) {this->~SocketProcessor(); mgr.processors.dealloc(this);}
}

void SocketProcessor::destroy()
{
	IOProcessor::cleanup(sctx,true);
}

int SocketProcessor::getError() const
{
	return Sockets::getError(fd);
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
	RC rc; assert(serviceProviderTab==NULL);
	if ((serviceProviderTab=new(this) ServiceProviderTab(STORE_HANDLERTAB_SIZE,(MemAlloc*)this))==NULL) return RC_NORESOURCES;
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
