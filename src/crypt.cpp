/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Adam Back and Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "crypt.h"
#include "session.h"
#include "utils.h"
#ifdef WIN32
#include <wincrypt.h>
#else
#include <fcntl.h>
#endif

using namespace MVStoreKernel;

const static uint32_t iii=1;
const bool fLEnd = *(byte*)&iii!=0;

#define F1(B,C,D)	((D)^((B)&((C)^(D))))
#define F2(B,C,D)	((B)^(C)^(D))
#define F3(B,C,D)	(((B)&((C)|(D)))|((C)&(D)))
#define F4(B,C,D)	((B)^(C)^(D))

#define K01_20		0x5A827999
#define K21_40		0x6ED9EBA1
#define K41_60		0x8F1BBCDC
#define K61_80		0xCA62C1D6

const uint32_t SHA1::IV[SHA1_DIGEST_WORDS] = {0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0};

__forceinline void	make_big_endian32(uint32_t *data,unsigned n) {if (fLEnd) for (;n>0;++data,--n) *data=mv_rev(*data);}
__forceinline void	restore_endian32(uint32_t *data,unsigned n) {if (fLEnd) for (;n>0;++data,--n) *data=mv_rev(*data);}
template<typename T> __forceinline void	axor(T *p1,const T *p2,size_t l) {for (;l!=0; --l) *p1++^=*p2++;}

void SHA1::transform()
{   
	uint32_t W[80];
    memcpy(W,M,SHA1_INPUT_BYTES); memset((byte*)W+SHA1_INPUT_BYTES,0,sizeof(W)-SHA1_INPUT_BYTES);
    for (unsigned t=16; t<80; t++) W[t]=mv_rot(W[t-16]^W[t-14]^W[t-8]^W[t-3],1);

#define ROUND(t,A,B,C,D,E,Func,K)	E+=mv_rot(A,5)+Func(B,C,D)+W[t]+K; B=mv_rot(B,30);
#define ROUND5(t,Func,K)			ROUND(t,A,B,C,D,E,Func,K); ROUND(t+1,E,A,B,C,D,Func,K); ROUND(t+2,D,E,A,B,C,Func,K );\
									ROUND(t+3,C,D,E,A,B,Func,K); ROUND(t+4,B,C,D,E,A,Func,K)
#define ROUND20(t,Func,K)			ROUND5(t,Func,K); ROUND5(t+5,Func,K); ROUND5(t+10,Func,K); ROUND5(t+15,Func,K)

    uint32_t A = H[0];
    uint32_t B = H[1];
    uint32_t C = H[2];
    uint32_t D = H[3];
    uint32_t E = H[4];

	ROUND20( 0,F1,K01_20);
    ROUND20(20,F2,K21_40);
    ROUND20(40,F3,K41_60);
    ROUND20(60,F4,K61_80);
  
    H[0] += A;
    H[1] += B;
    H[2] += C;
    H[3] += D;
    H[4] += E;
}

void SHA1::add(const byte* data, size_t data_len)
{
    unsigned mlen=(unsigned)((bits>>3)%SHA1_INPUT_BYTES);
    bits+=(uint64_t)data_len<<3;

    unsigned use=(unsigned)min((size_t)(SHA1_INPUT_BYTES - mlen),data_len);
    memcpy(M+mlen,data,use); mlen+=use;

    while (mlen==SHA1_INPUT_BYTES) {
		data_len-=use; data+=use;
		make_big_endian32((uint32_t*)M,SHA1_INPUT_WORDS);
		transform(); use=(unsigned)min((size_t)SHA1_INPUT_BYTES,data_len);
		memcpy(M,data,use); mlen=use;
    }
}

const byte *SHA1::result()
{
    unsigned mlen=(unsigned)((bits>>3)%SHA1_INPUT_BYTES),padding=SHA1_INPUT_BYTES-mlen;
    M[mlen++]=0x80;
    if (padding>=BIT_COUNT_BYTES) {
		memset(M+mlen,0x00,padding-BIT_COUNT_BYTES);
		make_big_endian32((uint32_t*)M,SHA1_INPUT_WORDS-BIT_COUNT_WORDS);
    } else {
		memset(M+mlen,0x00,SHA1_INPUT_BYTES-mlen);
		make_big_endian32((uint32_t*)M,SHA1_INPUT_WORDS);
		transform();
		memset(M,0x00,SHA1_INPUT_BYTES-BIT_COUNT_BYTES);
    }
    
	uint64_t temp = fLEnd?bits<<32|bits>>32:bits;
    memcpy(M+SHA1_INPUT_BYTES-BIT_COUNT_BYTES,&temp,BIT_COUNT_BYTES);
    transform();
    make_big_endian32(H,SHA1_DIGEST_WORDS);
	return (const byte*)H;
}

HMAC::HMAC(const byte *key,size_t lkey)
{
    byte ibuf[SHA1_INPUT_BYTES],obuf[SHA1_INPUT_BYTES];
    memset(ibuf,0x36,sizeof(ibuf)); memset(obuf,0x5c,sizeof(obuf));
	if (lkey<SHA1_INPUT_BYTES) {axor(ibuf,key,lkey); axor(obuf,key,lkey);}
	else {
		SHA1 tmp; tmp.add(key,lkey); 
		const uint32_t *key2=(uint32_t*)tmp.result();
		axor((uint32_t*)ibuf,key2,SHA1_DIGEST_WORDS); 
		axor((uint32_t*)obuf,key2,SHA1_DIGEST_WORDS);
    }
	in.add((byte*)ibuf,sizeof(ibuf)); out.add((byte*)obuf,sizeof(obuf));
}

PWD_PBKDF2::PWD_PBKDF2(const byte *key,size_t lkey,const byte *enc) : fOK(false)
{
	uint32_t count,i;
	if (enc!=NULL) {
		memcpy(pwd,enc,sizeof(uint32_t)+PWD_LSALT);
		count=fLEnd?mv_rev(*(uint32_t*)pwd):*(uint32_t*)pwd;
	} else {
		*(uint32_t*)pwd=count=PWD_COUNT; make_big_endian32((uint32_t*)pwd,1);
		StoreCtx::get()->cryptoMgr->randomBytes(pwd+sizeof(uint32_t),PWD_LSALT);
	}
	HMAC hmac(key,lkey),hsave(hmac); hmac.add(pwd+sizeof(uint32_t),PWD_LSALT);
	uint32_t i32=1; make_big_endian32(&i32,1); hmac.add((byte*)&i32,sizeof(i32));
	memcpy(pwd+sizeof(uint32_t)+PWD_LSALT,hmac.result(),SHA1_DIGEST_BYTES);
	byte Ui[SHA1_DIGEST_BYTES]; memset(Ui,0,sizeof(Ui));
	for (i=1; i<count; i++) {
		HMAC hm(hsave); hm.add(Ui,sizeof(Ui)); memcpy(Ui,hm.result(),SHA1_DIGEST_BYTES);
		axor((uint32_t*)(pwd+sizeof(uint32_t)+PWD_LSALT),(uint32_t*)Ui,SHA1_DIGEST_WORDS);
	}
	if (enc!=NULL) fOK=memcmp(enc,pwd,sizeof(pwd))==0;
}

//---------------------------------------------------------------------------------------------

const uint32_t AES::roundTab[10] =
{
	0x01000000,0x02000000,0x04000000,0x08000000,0x10000000,0x20000000,0x40000000,0x80000000,0x1B000000,0x36000000
};

uint32_t AES::encSTab[256];
uint32_t AES::decSTab[256];
uint32_t AES::encTab[4][256];
uint32_t AES::decTab[4][256];
uint32_t AES::keyTab[4][256];

AES::AESInit AES::init;

#define X(x) ((x)<<1^(((x)&0x80)!=0?0x1B:0x00))

AES::AESInit::AESInit()
{
	byte t1[256],t2[256],x,y; int i;
	for(i=0,x=1; i<256; ++i,x^=X(x)) t1[i]=x,t2[x]=i;

	encSTab[0x00]=0x63; decSTab[0x63] = 0x00;
	for(i=1; i<256; i++) {
		x=t1[255-t2[i]]; x^=y=x<<1|x>>7; x^=y=y<<1|y>>7; x^=y=y<<1|y>>7; x^=(y<<1|y>>7)^0x63;
		encSTab[i]=x; decSTab[x]=i;
	}
	for(i=0; i<256; i++) {
		uint32_t a=encSTab[i],b=X(a)&0xFF; a=encTab[0][i]=(a^b)^(a<<8)^(a<<16)^(b<<24);
		encTab[1][i]=mv_rot(a,24); encTab[2][i]=mv_rot(a,16); encTab[3][i]=mv_rot(a,8);
		if ((a=decSTab[i])==0) decTab[0][i]=decTab[1][i]=decTab[2][i]=decTab[3][i]=0;
		else {
			a=t2[a];
			a=decTab[0][i]=t1[(a+t2[0x0B])%0xFF]^uint32_t(t1[(a+t2[0x0D])%0xFF])<<8
				^uint32_t(t1[(a+t2[0x09])%0xFF])<<16^uint32_t(t1[(a+t2[0x0E])%0xFF])<<24;
			decTab[1][i]=mv_rot(a,24); decTab[2][i]=mv_rot(a,16); decTab[3][i]=mv_rot(a,8);
		}
    }
	for(i=0; i<256; i++) {
		uint32_t a=encSTab[i];
		keyTab[0][i]=decTab[0][a]; keyTab[1][i]=decTab[1][a]; keyTab[2][i]=decTab[2][a]; keyTab[3][i]=decTab[3][a];
	}
}

AES::AES(const byte *key,unsigned lkey)
{
	if (key==NULL) return;
	ulong i; uint32_t *penc=enc_key_sch; assert(lkey<=32);
	memcpy(penc,key,lkey); make_big_endian32(penc,lkey/sizeof(uint32_t));

	switch (lkey) {
	default: assert(0);
	case 16:
		for (i=0; i<10; ++i,penc+=4) {
			penc[4]=penc[0]^roundTab[i]^encSTab[penc[3]>>16&0xFF]<<24^encSTab[penc[3]>>8&0xFF]<<16
										^encSTab[penc[3]&0xFF]<<8^encSTab[penc[3]>>24&0xFF];
			penc[5]=penc[1]^penc[4]; penc[6]=penc[2]^penc[5]; penc[7]=penc[3]^penc[6];
		}
		Nr=10; break;
	case 24: 
		for (i=0; i<8; ++i,penc+=6) {
			penc[6]=penc[0]^roundTab[i]^encSTab[penc[5]>>16&0xFF]<<24^encSTab[penc[5]>>8&0xFF]<<16
										^encSTab[penc[5]&0xFF]<<8^encSTab[penc[5]>>24&0xFF];
			penc[7]=penc[1]^penc[6]; penc[8]=penc[2]^penc[7]; penc[9]=penc[3]^penc[8]; 
			penc[10]=penc[4]^penc[9]; penc[11]=penc[5]^penc[10];
		}
		Nr=12; break;
	case 32: 
		for (i=0; i<7; ++i,penc+=8) {
			penc[8]=penc[0]^roundTab[i]^encSTab[penc[7]>>16&0xFF]<<24^encSTab[penc[7]>>8&0xFF]<<16
										^encSTab[penc[7]&0xFF]<<8^encSTab[penc[7]>>24&0xFF];
			penc[9]=penc[1]^penc[8]; penc[10]=penc[2]^penc[9]; penc[11]=penc[3]^penc[10];
			penc[12]=penc[4]^encSTab[penc[11]>>24&0xFF]<<24^encSTab[penc[11]>>16&0xFF]<<16
										^encSTab[penc[11]>>8&0xFF]<<8^encSTab[penc[11]&0xFF];
			penc[13]=penc[5]^penc[12]; penc[14]=penc[6]^penc[13]; penc[15]=penc[7]^penc[14];
		}
		Nr=14; break;
	}
	uint32_t *pdec=dec_key_sch; memcpy(pdec,penc,4*sizeof(uint32_t));
	for(i=1,penc+=4; i<Nr; i++) {
		penc-=8; pdec+=4;
		pdec[0]=keyTab[0][*penc>>24&0xFF]^keyTab[1][*penc>>16&0xFF]^keyTab[2][*penc>>8&0xFF]^keyTab[3][*penc&0xFF]; penc++;
		pdec[1]=keyTab[0][*penc>>24&0xFF]^keyTab[1][*penc>>16&0xFF]^keyTab[2][*penc>>8&0xFF]^keyTab[3][*penc&0xFF]; penc++;
		pdec[2]=keyTab[0][*penc>>24&0xFF]^keyTab[1][*penc>>16&0xFF]^keyTab[2][*penc>>8&0xFF]^keyTab[3][*penc&0xFF]; penc++;
		pdec[3]=keyTab[0][*penc>>24&0xFF]^keyTab[1][*penc>>16&0xFF]^keyTab[2][*penc>>8&0xFF]^keyTab[3][*penc&0xFF]; penc++;
	}
	memcpy(pdec+4,penc-8,4*sizeof(uint32_t));
}

#define	ENC_ROUND(A,B,KEY)	KEY+=4;																				\
	A[0]=KEY[0]^encTab[0][B[0]>>24]^encTab[1][B[1]>>16&0xFF]^encTab[2][B[2]>>8&0xFF]^encTab[3][B[3]&0xFF];		\
	A[1]=KEY[1]^encTab[0][B[1]>>24]^encTab[1][B[2]>>16&0xFF]^encTab[2][B[3]>>8&0xFF]^encTab[3][B[0]&0xFF];		\
	A[2]=KEY[2]^encTab[0][B[2]>>24]^encTab[1][B[3]>>16&0xFF]^encTab[2][B[0]>>8&0xFF]^encTab[3][B[1]&0xFF];		\
	A[3]=KEY[3]^encTab[0][B[3]>>24]^encTab[1][B[0]>>16&0xFF]^encTab[2][B[1]>>8&0xFF]^encTab[3][B[2]&0xFF];
#define	ENC_LAST_ROUND(A,B,KEY)	KEY+=4;																			\
	A[0]=KEY[0]^encSTab[B[0]>>24]<<24^encSTab[B[1]>>16&0xFF]<<16^encSTab[B[2]>>8&0xFF]<<8^encSTab[B[3]&0xFF];	\
	A[1]=KEY[1]^encSTab[B[1]>>24]<<24^encSTab[B[2]>>16&0xFF]<<16^encSTab[B[3]>>8&0xFF]<<8^encSTab[B[0]&0xFF];	\
	A[2]=KEY[2]^encSTab[B[2]>>24]<<24^encSTab[B[3]>>16&0xFF]<<16^encSTab[B[0]>>8&0xFF]<<8^encSTab[B[1]&0xFF];	\
	A[3]=KEY[3]^encSTab[B[3]>>24]<<24^encSTab[B[0]>>16&0xFF]<<16^encSTab[B[1]>>8&0xFF]<<8^encSTab[B[2]&0xFF];
#define	ENC_ROUND2(A,B,KEY)	ENC_ROUND(A,B,KEY) ENC_ROUND(B,A,KEY)

void AES::encryptBlock(uint32_t buf[4])
{
	uint32_t tmp[4],*pk=enc_key_sch;
	make_big_endian32(buf,4); 
	axor(buf,pk,4);
	ENC_ROUND2(tmp,buf,pk);
	ENC_ROUND2(tmp,buf,pk);
	ENC_ROUND2(tmp,buf,pk);
	ENC_ROUND2(tmp,buf,pk);
	ENC_ROUND(tmp,buf,pk);
	if (Nr>10) {ENC_ROUND2(buf,tmp,pk); if (Nr>12) {ENC_ROUND2(buf,tmp,pk);}}
	ENC_LAST_ROUND(buf,tmp,pk);
	restore_endian32(buf,4);
}

#define	DEC_ROUND(A,B,KEY)	KEY+=4;																				\
	A[0]=KEY[0]^decTab[0][B[0]>>24]^decTab[1][B[3]>>16&0xFF]^decTab[2][B[2]>>8&0xFF]^decTab[3][B[1]&0xFF];		\
	A[1]=KEY[1]^decTab[0][B[1]>>24]^decTab[1][B[0]>>16&0xFF]^decTab[2][B[3]>>8&0xFF]^decTab[3][B[2]&0xFF];		\
	A[2]=KEY[2]^decTab[0][B[2]>>24]^decTab[1][B[1]>>16&0xFF]^decTab[2][B[0]>>8&0xFF]^decTab[3][B[3]&0xFF];		\
	A[3]=KEY[3]^decTab[0][B[3]>>24]^decTab[1][B[2]>>16&0xFF]^decTab[2][B[1]>>8&0xFF]^decTab[3][B[0]&0xFF];
#define	DEC_LAST_ROUND(A,B,KEY)	KEY+=4;																			\
	A[0]=KEY[0]^decSTab[B[0]>>24]<<24^decSTab[B[3]>>16&0xFF]<<16^decSTab[B[2]>>8&0xFF]<<8^decSTab[B[1]&0xFF];	\
	A[1]=KEY[1]^decSTab[B[1]>>24]<<24^decSTab[B[0]>>16&0xFF]<<16^decSTab[B[3]>>8&0xFF]<<8^decSTab[B[2]&0xFF];	\
	A[2]=KEY[2]^decSTab[B[2]>>24]<<24^decSTab[B[1]>>16&0xFF]<<16^decSTab[B[0]>>8&0xFF]<<8^decSTab[B[3]&0xFF];	\
	A[3]=KEY[3]^decSTab[B[3]>>24]<<24^decSTab[B[2]>>16&0xFF]<<16^decSTab[B[1]>>8&0xFF]<<8^decSTab[B[0]&0xFF];
#define	DEC_ROUND2(A,B,KEY)	DEC_ROUND(A,B,KEY) DEC_ROUND(B,A,KEY)

void AES::decryptBlock(uint32_t buf[4])
{
	uint32_t tmp[4],*pk=dec_key_sch;
	make_big_endian32(buf,4);
	axor(buf,pk,4);
	DEC_ROUND2(tmp,buf,pk);
	DEC_ROUND2(tmp,buf,pk);
	DEC_ROUND2(tmp,buf,pk);
	DEC_ROUND2(tmp,buf,pk);
	DEC_ROUND(tmp,buf,pk);
	if (Nr>10) {DEC_ROUND2(buf,tmp,pk); if (Nr>12) {DEC_ROUND2(buf,tmp,pk);}}
	DEC_LAST_ROUND(buf,tmp,pk);
	restore_endian32(buf,4);
}

void AES::encrypt(byte *buf,size_t lbuf,const uint32_t IV[4],AESMode mode)
{
	uint32_t *p=(uint32_t*)buf; const uint32_t *p2=IV;
	if (lbuf>0) switch (mode) {
	default: assert(0);
	case AES_CBC:
		assert(ceil(buf,sizeof(uint32_t))==buf && lbuf%AES_BLOCK_SIZE==0);
		do {
			p[0]^=p2[0]; p[1]^=p2[1]; p[2]^=p2[2]; p[3]^=p2[3];
			encryptBlock(p); p2=p; p+=4;
		} while ((lbuf-=AES_BLOCK_SIZE)!=0);
		break;
	case AES_CFB:
		//
		break;
	}
}

void AES::decrypt(byte *buf,size_t lbuf,const uint32_t IV[4],AESMode mode)
{
	uint32_t *p; const uint32_t *p2;
	if (lbuf>0) switch (mode) {
	default: assert(0);
	case AES_CBC:
		assert(ceil(buf,sizeof(uint32_t))==buf && lbuf%AES_BLOCK_SIZE==0);
		p=(uint32_t*)(buf+lbuf-AES_BLOCK_SIZE);
		do {
			decryptBlock(p); p2=(byte*)p==buf?IV:p-4;
			p[0]^=p2[0]; p[1]^=p2[1]; p[2]^=p2[2]; p[3]^=p2[3];
		} while ((p-=4)>=(uint32_t*)buf);
		break;
	case AES_CFB:
		//
		break;
	}
}

//--------------------------------------------------------------------------------------------------------------

CryptoMgr CryptoMgr::mgr;
	
CryptoMgr::~CryptoMgr()
{
	rndsht=sizeof(rndbuf);
#ifdef WIN32
	if (ctx!=NULL) {
	    HMODULE advapi=LoadLibrary(TEXT("ADVAPI32.DLL"));
		if (advapi!=NULL) {
			BOOL (WINAPI *release)(HANDLE,DWORD) = 
				(BOOL (WINAPI*)(HANDLE,DWORD))GetProcAddress(advapi,TEXT("CryptReleaseContext"));
			if (release!=NULL) release(ctx,0);
		}
	}
#else
	if (src>=0) close(src);
#endif
}

CryptoMgr *CryptoMgr::get()
{
#ifdef WIN32
	if (mgr.ctx==NULL) {
		RWLockP lck(&mgr.lock,RW_X_LOCK);
		if (mgr.ctx==NULL) {
		    HMODULE advapi=LoadLibrary(TEXT("ADVAPI32.DLL"));
			if (advapi!=NULL) {
				BOOL (WINAPI *acquire)(HANDLE *,LPCTSTR,LPCTSTR,DWORD,DWORD) = 
					(BOOL (WINAPI*)(HANDLE *,LPCTSTR,LPCTSTR,DWORD,DWORD))GetProcAddress(advapi,TEXT("CryptAcquireContextA"));
				mgr.gen=(BOOL (WINAPI*)(HANDLE,DWORD,BYTE*))GetProcAddress(advapi,TEXT("CryptGenRandom"));
				if (acquire!=NULL && mgr.gen!=NULL && acquire(&mgr.ctx,0,0,PROV_RSA_FULL,CRYPT_VERIFYCONTEXT)==FALSE) mgr.gen=NULL;
			}
		}
    }
	return mgr.gen!=NULL?&mgr:(CryptoMgr*)0;
#else
	if (mgr.src<0) {RWLockP lck(&mgr.lock,RW_X_LOCK); if (mgr.src<0) mgr.src=open( "/dev/urandom",O_RDONLY);}
	return mgr.src>=0?&mgr:(CryptoMgr*)0;
#endif
}

void CryptoMgr::fillBuf()
{
	assert(lock.isXLocked());
#ifdef WIN32
	if (gen!=NULL && ctx!=NULL && gen(ctx,sizeof(rndbuf),rndbuf)==TRUE) rndsht=0;
#else
	if (src>=0) {
		int l=read(src,rndbuf,sizeof(rndbuf));
		if (l>0) {rndsht=sizeof(rndbuf)-l; if (size_t(l)<sizeof(rndbuf)) memmove(rndbuf+rndsht,rndbuf,l);}
	}
#endif
}
