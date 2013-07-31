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
 * StoreCtx and Session class definitions
 */
#ifndef _SERVICE_H_
#define _SERVICE_H_

#include "session.h"
#include "utils.h"
#include "pin.h"

namespace AfyKernel
{

#define	ISRV_REVERSE		0x80000000
#define	ISRV_DELAYED		0x40000000

#define SRV_DEFAULT_BUFSIZE	0x4000
#define	SRV_DEFAULT_BATCH	0x400

enum CommStackType
{
	CST_READ, CST_WRITE, CST_SERVER, CST_REQUEST
};

/**
 * service stack call context
 * can contain several processing steps
 */
class ServiceCtx : public DLList, public IServiceCtx, public MemAlloc, public PIN
{
	struct BufCache {
		BufCache	*next;
		size_t		l;
	};
	struct ProcDscr {
		IService::Processor	*proc;
		URIID				sid;
		uint32_t			dscr;
		uint32_t			meta;
		size_t				lBuffer;
		size_t				lHeader;
		size_t				lTrailer;
		const	Value		*srvInfo;
		uint32_t			nSrvInfo;
		uint32_t			prevIdx;
		Value				inp;
		byte				*buf;
		uint32_t			lbuf;
		uint32_t			oleft;
	};
public:
	HChain<ServiceCtx>	list;
	PID					id;
	const	bool		fWrite;
	IListener			*lst;
	CommStackType		cst;
	unsigned			sstate;
	ProcDscr			*procs;
	unsigned			nProcs;
	unsigned			cur;
	unsigned			resume;
	unsigned			toggleIdx;
	unsigned			flushIdx;
	const	Value		*params;
	uint32_t			nParams;
	PINx				*pR;
	BufCache			*bufCache;
	uint64_t			msgLength;
	SubAlloc			bufAlloc;
	DynArray<PIN*,100>	pins;
	unsigned			xpins;
	class	Cursor		*crs;
	Value				action;
private:
	RC					build(const Value *vals,unsigned nVals);
	RC					getProcessor(ProcDscr *prc,const Value *vals,unsigned nVals);
	void				fill(ProcDscr *prc,IService *isrv,const Value *vals,unsigned nVals);
	ProcDscr			*flush(Value& inp) {
		ProcDscr *prc=&procs[cur=flushIdx]; assert(flushIdx<nProcs && (prc->dscr&(ISRV_FLUSH|ISRV_DELAYED))!=0);
		if ((prc->dscr&ISRV_FLUSH)!=0) {prc->dscr&=~ISRV_FLUSH; sstate|=ISRV_FLUSH; inp.setEmpty();}
		else {prc->dscr&=~ISRV_DELAYED; inp.set(prc->buf+prc->lHeader,(uint32_t)prc->lBuffer-prc->oleft); prc++; cur++;}
		for (;;) if (++flushIdx>=nProcs) {flushIdx=~0u; break;} else if ((procs[flushIdx].dscr&(ISRV_FLUSH|ISRV_DELAYED))!=0) break;
		return prc;
	}
	RC					server(ProcDscr *prc,Value& inp,Value& out);
	RC					commitPINs();
	void				cleanup(bool fDestroy);
public:
	ServiceCtx(Session *s,const Value *par,uint32_t nPar,bool fW,IListener *ls);
	~ServiceCtx();
	RC					invoke(const Value *vals,unsigned nVals,PINx *pRes) {pR=pRes; return invoke(vals,nVals);}
	RC					invoke(const Value *vals,unsigned nVals,Value *res=NULL);
	PIN					*createPIN(Value* values,unsigned nValues,unsigned md,const PID *orig);
	const PID&			getKey() const {return id;}
	ISession			*getSession() const;
	URIID				getServiceID() const;
	const	Value		*getParameter(URIID prop) const;
	void				getParameters(Value *vals,unsigned nVals) const;
	RC					expandBuffer(Value&,size_t extra=0);
	uint64_t			getMsgLength() const;
	void				setMsgLength(uint64_t lMsg);
	IPIN&				getCtxPIN();
	RC					getOSError() const;
	void				destroy();

	void				*malloc(size_t);
	void				*realloc(void *,size_t,size_t=0);
	void				free(void *);
	void				*memalign(size_t align,size_t s);
	HEAP_TYPE			getAType() const;
	void				release();

	friend	class		Session;
};

/**
 * listener start on tx commit
 */
class StartListener : public OnCommit
{
	Value	*props;
	unsigned nProps;
public:
	StartListener(const Value *p,unsigned n,MemAlloc *ma);
	RC		process(Session *ses);
	void	destroy(Session *ses);
	static	RC	loadListener(PINx& cb);
};

/**
 * load service on tx commit
 */
class LoadService : public OnCommit
{
	Value	*props;
	unsigned nProps;
public:
	LoadService(const Value *p,unsigned n,MemAlloc *ma);
	RC		process(Session *ses);
	void	destroy(Session *ses);
	static	RC	loadLoader(PINx& cb);
};

/**
 * table of cached service stacks
 */
class ServiceTab : public HashTab<ServiceCtx,const PID&,&ServiceCtx::list>
{
public:
	ServiceTab(unsigned size,MemAlloc *allc,bool fClr=true) : HashTab<ServiceCtx,const PID&,&ServiceCtx::list>(size,allc,fClr) {}
};

};

#endif
