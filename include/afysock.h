/**************************************************************************************

Copyright � 2004-2014 GoPivotal, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,  WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.

Written by Mark Venguerov 2013

**************************************************************************************/

#ifndef _AFYSOCK_H_
#define _AFYSOCK_H_

#ifdef WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef _WIN32_WINNT
#define _WIN32_WINNT _WIN32_WINNT_VISTA
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <dbt.h>
#pragma comment(lib, "Ws2_32.lib")
#define	isSOK(a)			((a)!=INVALID_SOCKET)
#define	sockaddr_storage	SOCKADDR_STORAGE
#else
#include <signal.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <netdb.h>
#define	isSOK(a)			((a)>=0)
#define	SOCKET				int
#define	INVALID_SOCKET		(-1)
#endif

#include "affinity.h"

namespace Afy
{

#define R_BIT		0x01
#define W_BIT		0x02
#define E_BIT		0x04

#ifndef DEFAULT_READ_TIMEOUT
#define DEFAULT_READ_TIMEOUT	120000	// in ms
#endif

struct AFY_EXP SockAddr : public IAddress
{
	sockaddr_storage	saddr;
	size_t				laddr;
	int					socktype;
#ifndef ANDROID
	int					protocol;
	SockAddr() : protocol(IPPROTO_TCP),socktype(SOCK_STREAM),laddr(0) {memset(&saddr,0,sizeof(saddr));}
#else
	SockAddr() : socktype(SOCK_STREAM),laddr(0) {memset(&saddr,0,sizeof(saddr));}
#endif
	RC					resolve(const Value *addr,IServiceCtx *sctx=NULL);		// sctx==NULL -> listener
	bool				operator==(const IAddress&) const;
	int					cmp(const IAddress&) const;
	operator			uint32_t() const;
	static	RC			getAddr(const sockaddr_storage *ss,socklen_t ls,Value& addr,IMemAlloc *ma,bool fPort=false);
};

class AFY_EXP IAfySocket
{
public:
	virtual	SOCKET		getSocket() const = 0;
	virtual	IAffinity	*getAffinity() const = 0;
	virtual	ISession	*getSession() const = 0;
	virtual	void		process(ISession *ses,unsigned bits) = 0;
};

}

#endif
