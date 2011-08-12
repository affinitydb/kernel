/**************************************************************************************

Copyright © 2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2010

**************************************************************************************/

#include "session.h"
#include "pinref.h"
#include "utils.h"

using namespace MVStoreKernel;

PINRef::PINRef(ushort si,const byte *p,size_t l) : stID(si),count(1),def(0)
{
	const byte *end=p+l; PageAddr ad; id.ident=STORE_OWNER;
	mv_dec32(p,ad.pageID); if (p>=end) throw RC_CORRUPTED;
	mv_dec16(p,ad.idx);
	if (p<end) {
		byte dscr=*--end,dscr2=(dscr&0x40)!=0?*--end:0; if ((dscr&0x80)==0) throw RC_CORRUPTED;
		if ((dscr&0x10)!=0) {mv_dec32r(end,count); if (p>end) throw RC_CORRUPTED;}
		if ((dscr&0x01)!=0) {mv_dec16(p,si); if (p>end) throw RC_CORRUPTED;}
		if ((dscr&0x02)!=0) {mv_dec32(p,id.ident); if (p>end) throw RC_CORRUPTED;}
		if ((dscr&0x04)!=0) {def|=PR_ADDR; mv_dec32(p,addr.pageID); mv_dec16(p,addr.idx); if (p>end) throw RC_CORRUPTED;}
		if ((dscr&0x08)!=0) {def|=PR_U1; mv_dec32(p,u1); if (p>end) throw RC_CORRUPTED;}
		if ((dscr&0x40)!=0) {
			def|=PR_PID2; PageAddr ad2; ushort si2=si;	// what if u2 only?
			mv_dec32(p,ad2.pageID); mv_dec16(p,ad2.idx); if (p>end) throw RC_CORRUPTED;
			if ((dscr2&0x01)!=0) {if ((dscr2&0x02)==0) si2=stID; else {mv_dec16(p,si2); if (p>end) throw RC_CORRUPTED;}}
			id2.pid=(uint64_t(si2)<<32|ad2.pageID)<<16|ad2.idx;
			if ((dscr2&0x04)==0) id2.ident=id.ident;
			else if ((dscr2&0x08)==0) id2.ident=STORE_OWNER;
			else {mv_dec32(p,id2.ident); if (p>end) throw RC_CORRUPTED;}
			if ((dscr2&0x10)!=0) {def|=PR_ADDR2; mv_dec32(p,addr2.pageID); mv_dec16(p,addr2.idx); if (p>end) throw RC_CORRUPTED;}
			if ((dscr2&0x20)!=0) {def|=PR_U2; mv_dec32(p,u2); if (p>end) throw RC_CORRUPTED;}
		}
	} else if (p>end) throw RC_CORRUPTED;
	id.pid=(uint64_t(si)<<32|ad.pageID)<<16|ad.idx;
}

byte PINRef::enc(byte *p) const
{
	byte *const p0=p; byte dscr=0,dscr2=0; PageAddr ad; ushort si,si2;
	ad.pageID=uint32_t(id.pid>>16); ad.idx=PageIdx(id.pid); si=ushort(id.pid>>48);
	if (si!=stID) dscr|=0x01; if (id.ident!=STORE_OWNER) dscr|=0x02;
	mv_enc32(p,ad.pageID); mv_enc16(p,ad.idx);
	if ((dscr|def)!=0) {
		if ((dscr&0x01)!=0) mv_enc16(p,si);
		if ((dscr&0x02)!=0) mv_enc32(p,id.ident);
		if (def!=0) {
			if ((def&PR_ADDR)!=0 && addr.defined() && addr!=ad) {dscr|=0x04; mv_enc32(p,addr.pageID); mv_enc16(p,addr.idx);}
			if ((def&PR_U1)!=0 && u1!=~0u) {dscr|=0x08; mv_enc32(p,u1);}
			if ((def&PR_COUNT)!=0 && count!=1) dscr|=0x10;
			if ((def&PR_FCOLL)!=0) dscr|=0x20;
			if ((def&(PR_PID2|PR_U2))!=0) {
				dscr|=0x40;
				ad.pageID=uint32_t(id2.pid>>16); ad.idx=PageIdx(id2.pid); si2=ushort(id2.pid>>48);
				mv_enc32(p,ad.pageID); mv_enc16(p,ad.idx);
				if (si2!=si) {dscr2|=0x01; if (si2!=stID) {dscr2|=0x02; mv_enc16(p,si2);}}
				if (id2.ident!=id.ident) {dscr2|=0x04; if (id2.ident!=STORE_OWNER) {dscr2|=0x08; mv_enc32(p,id2.ident);}}
				if ((def&PR_ADDR2)!=0 && addr2.defined() && (addr2.pageID!=ad.pageID || addr2.idx!=ad.idx))
					{dscr2|=0x10; mv_enc32(p,addr2.pageID); mv_enc16(p,addr2.idx);}
				if ((def&PR_U2)!=0) {dscr2|=0x20; mv_enc32(p,u2);}
			}
			if ((dscr&0x10)!=0) mv_enc32r(p,count);
			if ((dscr&0x40)!=0) *p++=dscr2;
		}
		if (dscr!=0) *p++=dscr|0x80;
	}
	return byte(p-p0);
}

RC PINRef::getPID(const byte *p,size_t l,ushort si,PID& id,PageAddr *paddr)
{
	const byte *end=p+l; PageAddr ad,ad2; id.ident=STORE_OWNER;
	mv_dec32(p,ad.pageID); mv_dec16(p,ad.idx); ad2=ad;
	byte dscr=end[-1];
	if ((dscr&0x80)!=0) {
		if (p>=(end-=(dscr&0x40)!=0?2:1)) return RC_CORRUPTED;
		if ((dscr&0x01)!=0) mv_dec16(p,si);
		if ((dscr&0x02)!=0) {if (p>end) return RC_CORRUPTED; mv_dec32(p,id.ident);}
		if (paddr!=NULL && (dscr&0x04)!=0) {if (p>end) return RC_CORRUPTED; mv_dec32(p,ad2.pageID); mv_dec16(p,ad2.idx);}
	}
	id.pid=(uint64_t(si)<<32|ad.pageID)<<16|ad.idx;
	if (paddr!=NULL) *paddr=ad2;
	return RC_OK;
}

RC PINRef::adjustCount(byte *p,size_t& l,uint32_t cnt,byte *buf,bool fDec)
{
	byte *const p0=p; assert(l!=0); byte dscr=*(p+=l-1); RC rc=RC_CORRUPTED; uint32_t cnt0=1;
	if ((dscr&0x80)==0) {
		if (fDec) rc=cnt==1?RC_FALSE:RC_NOTFOUND;
		else {memcpy(buf,p0,l); p=buf+l; ++cnt; mv_enc32r(p,cnt); *p=0x90; l=ulong(p-buf+1); rc=RC_TRUE;}
	} else {
		const byte *const end=(dscr&0x40)!=0?--p:p;
		if ((dscr&0x10)!=0) mv_dec32r(p,cnt0);
		if (fDec && cnt0<=cnt) rc=cnt==cnt0?RC_FALSE:RC_NOTFOUND;
		else {
			if (fDec) cnt0-=cnt; else cnt0+=cnt;
			byte cntbuf[10],*pp=cntbuf; mv_enc32r(pp,cnt0);
			size_t lnew=pp-cntbuf,lold=end-p;
			if (lnew==lold) {memcpy(p,cntbuf,lnew); rc=RC_OK;}
			else {
				memcpy(buf,p0,p-p0); p=buf+(p-p0); memcpy(p,cntbuf,lnew); p+=lnew;
				if ((dscr&0x40)!=0) *p++=*end; *p=dscr|0x10; l=ulong(p-buf+1); rc=RC_TRUE;
			}
		}
	}
	if (rc==RC_NOTFOUND) report(MSG_ERROR,"Invalid descrease count, cnt=%u, decreased by %u\n",cnt0,cnt);
	return rc;
}

int PINRef::cmpPIDs(const byte *p1,unsigned l1,const byte *p2,unsigned l2)
{
	const byte *const e1=p1+l1,*const e2=p2+l2; int c;
	assert(p1!=NULL && l1!=0 && p2!=NULL && l2!=0);
	uint32_t u1,u2; mv_dec32(p1,u1); mv_dec32(p2,u2);
	if ((c=cmp3(u1,u2))!=0) return c; assert(p1<e1 && p2<e2);
	uint16_t x1,x2; mv_dec16(p1,x1); mv_dec16(p2,x2);
	if ((c=cmp3(x1,x2))!=0) return c; assert(p1<=e1 && p2<=e2);
	if ((e1[-1]&0x83)>0x80 || (e2[-1]&0x83)>0x80) {
		if (((e1[-1]|e2[-1])&0x01)!=0) {
			if ((e1[-1]&0x01)==0) x1=StoreCtx::get()->storeID; else mv_dec16(p1,x1);
			if ((e2[-1]&0x01)==0) x2=StoreCtx::get()->storeID; else mv_dec16(p2,x2);
			if ((c=cmp3(x1,x2))!=0) return c; assert(p1<e1 && p2<e2);
		}
		if (((e1[-1]|e2[-1])&0x02)!=0) {
			if ((e1[-1]&0x02)==0) u1=STORE_OWNER; else mv_dec32(p1,u1);
			if ((e2[-1]&0x02)==0) u2=STORE_OWNER; else mv_dec32(p2,u2);
			return cmp3(u1,u2);
		}
	}
	return 0;
}

int __cdecl MVStoreKernel::cmpPIDs(const void *p1,const void *p2)
{
	return cmpPIDs(*(PID*)p1,*(PID*)p2);
}
