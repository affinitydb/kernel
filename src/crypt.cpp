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

Written by Adam Back and Mark Venguerov 2004-2013

**************************************************************************************/

#include "crypt.h"
#include "session.h"
#ifdef WIN32
#include <wincrypt.h>
#else
#include <fcntl.h>
#endif

using namespace AfyKernel;

//#define TEST_SHA
//#define TEST_AES

const static uint32_t iii=1;
const bool fLEnd = *(byte*)&iii!=0;

const static char b64et[] =
{	
	'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
    'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
    'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
    'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
    'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
    'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
    'w', 'x', 'y', 'z', '0', '1', '2', '3',
    '4', '5', '6', '7', '8', '9', '+', '/'
};

const static char b64dt[256] =
{
	-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
	-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
	-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,62,-1,-1,-1,63,
	52,53,54,55,56,57,58,59,60,61,-1,-1,-1, 0,-1,-1,
	-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,
	15,16,17,18,19,20,21,22,23,24,25,-1,-1,-1,-1,-1,
	-1,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
	41,42,43,44,45,46,47,48,49,50,51,-1,-1,-1,-1,-1,
	-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
	-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
	-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
	-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
	-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
	-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
	-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
	-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
};

RC AfyKernel::base64enc(const byte *buf,size_t lbuf,char *str,size_t& lstr)
{
	if (str==NULL || lstr==0 || buf==NULL || lbuf==0) return RC_INVPARAM;
	size_t l=((lbuf+2)/3)*4; if (l>lstr) return RC_NORESOURCES;
	const byte *bend3=buf+lbuf/3*3; lstr=l;
    while (buf<bend3) {
        uint32_t u=(uint32_t)buf[0]<<16|(uint32_t)buf[1]<<8|buf[2]; buf+=3;
		str[0]=b64et[(u>>18)&0x3F]; str[1]=b64et[(u>>12)&0x3F]; str[2]=b64et[(u>>6)&0x3F]; str[3]=b64et[u&0x3F]; str+=4;
    }
	switch (lbuf%3) {
	case 0: break;
	case 1: str[0]=b64et[(buf[0]>>2)&0x3F]; str[1]=b64et[(buf[0]<<4)&0x3F]; str[2]='='; str[3]='='; break;
	case 2: str[0]=b64et[(buf[0]>>2)&0x3F]; str[1]=b64et[(buf[0]<<4|buf[1]>>4)&0x3F]; str[2]=b64et[(buf[1]<<2)&0x3F]; str[3]='='; break;
	}
	return RC_OK;
}

RC AfyKernel::base64dec(const char *str,size_t lstr,byte *buf,size_t& lbuf)
{
	if (str==NULL || lstr==0 || lstr%4!=0 || buf==NULL || lbuf==0) return RC_INVPARAM;
	byte *bend=buf+lbuf,*buf0=buf;
	for (const char *end=str+lstr; str<end; str+=4,buf+=3) {
		if (buf+3>bend) return RC_NORESOURCES;
		int a0=b64dt[(byte)str[0]],a1=b64dt[(byte)str[1]],a2=b64dt[(byte)str[2]],a3=b64dt[(byte)str[3]];
		if ((a0|a1|a2|a3)<0) return RC_CORRUPTED;
		buf[0]=a0<<2|a1>>4; buf[1]=a1<<4|a2>>2; buf[2]=a2<<6|a3;
	}
	lbuf=buf-buf0; if (str[-1]=='=') {lbuf--; if (lstr>=2 && str[-2]=='=') lbuf--;}
	return RC_OK;
}

__forceinline void	make_big_endian32(uint32_t *data,unsigned n) {if (fLEnd) for (;n>0;++data,--n) *data=afy_rev(*data);}
__forceinline void	restore_endian32(uint32_t *data,unsigned n) {if (fLEnd) for (;n>0;++data,--n) *data=afy_rev(*data);}
template<typename T> __forceinline void	axor(T *p1,const T *p2,size_t l) {for (;l!=0; --l) *p1++^=*p2++;}

const uint32_t SHA256::IV[SHA_DIGEST_WORDS] =
{
	0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19
};
static const uint32_t K[64] =
{
	0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5,
	0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
	0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
	0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
	0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
	0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
	0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
	0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
	0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
	0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
	0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3,
	0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
	0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5,
	0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
	0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
	0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
};

#define Ch(x,y,z) (z^(x&(y^z)))
#define Maj(x,y,z) (y^((x^y)&(y^z)))

#define S0(x) (afy_rotl(x,30)^afy_rotl(x,19)^afy_rotl(x,10))
#define S1(x) (afy_rotl(x,26)^afy_rotl(x,21)^afy_rotl(x,7))
#define s0(x) (afy_rotl(x,25)^afy_rotl(x,14)^(x>>3))
#define s1(x) (afy_rotl(x,15)^afy_rotl(x,13)^(x>>10))

#define t0(i) T[(0-i)&7]
#define t1(i) T[(1-i)&7]
#define t2(i) T[(2-i)&7]
#define t3(i) T[(3-i)&7]
#define t4(i) T[(4-i)&7]
#define t5(i) T[(5-i)&7]
#define t6(i) T[(6-i)&7]
#define t7(i) T[(7-i)&7]

#define R0(i) t7(i)+=S1(t4(i))+Ch(t4(i),t5(i),t6(i))+K[i]+(W[i]=((uint32_t*)M)[i]); t3(i)+=t7(i); t7(i)+=S0(t0(i))+Maj(t0(i),t1(i),t2(i))
#define R(i) t7(i)+=S1(t4(i))+Ch(t4(i),t5(i),t6(i))+K[i+j]+(W[i&15]+=s1(W[(i-2)&15])+W[(i-7)&15]+s0(W[(i-15)&15])); t3(i)+=t7(i); t7(i)+=S0(t0(i))+Maj(t0(i),t1(i),t2(i))

void SHA256::transform()
{
	uint32_t W[16],T[8]; T[0]=H[0]; T[1]=H[1]; T[2]=H[2]; T[3]=H[3]; T[4]=H[4]; T[5]=H[5]; T[6]=H[6]; T[7]=H[7];
	R0( 0); R0( 1); R0( 2); R0( 3); R0( 4); R0( 5); R0( 6); R0( 7); R0( 8); R0( 9); R0(10); R0(11); R0(12); R0(13); R0(14); R0(15);
	for (unsigned j=16; j<64; j+=16) {
		R( 0); R( 1); R( 2); R( 3); R( 4); R( 5); R( 6); R( 7); R( 8); R( 9); R(10); R(11); R(12); R(13); R(14); R(15);
	}
    H[0]+=t0(0); H[1]+=t1(0); H[2]+=t2(0); H[3]+=t3(0); H[4]+=t4(0); H[5]+=t5(0); H[6]+=t6(0); H[7]+=t7(0);
}

void SHA256::add(const byte* data, size_t data_len)
{
    unsigned mlen=(unsigned)((bits>>3)%SHA_INPUT_BYTES);
    bits+=(uint64_t)data_len<<3;

    unsigned use=(unsigned)min((size_t)(SHA_INPUT_BYTES-mlen),data_len);
    memcpy(M+mlen,data,use); mlen+=use;

    while (mlen==SHA_INPUT_BYTES) {
		data_len-=use; data+=use;
		make_big_endian32((uint32_t*)M,SHA_INPUT_WORDS);
		transform(); use=(unsigned)min((size_t)SHA_INPUT_BYTES,data_len);
		memcpy(M,data,use); mlen=use;
    }
}

const byte *SHA256::result()
{
    unsigned mlen=(unsigned)((bits>>3)%SHA_INPUT_BYTES),padding=SHA_INPUT_BYTES-mlen; M[mlen++]=0x80;
    if (padding>BIT_COUNT_BYTES) {
		memset(M+mlen,0x00,padding-BIT_COUNT_BYTES);
		make_big_endian32((uint32_t*)M,SHA_INPUT_WORDS-BIT_COUNT_WORDS);
    } else {
		memset(M+mlen,0x00,SHA_INPUT_BYTES-mlen);
		make_big_endian32((uint32_t*)M,SHA_INPUT_WORDS);
		transform();
		memset(M,0x00,SHA_INPUT_BYTES-BIT_COUNT_BYTES);
    }
    
	uint64_t temp = fLEnd?bits<<32|bits>>32:bits;
    memcpy(M+SHA_INPUT_BYTES-BIT_COUNT_BYTES,&temp,BIT_COUNT_BYTES);
    transform(); make_big_endian32(H,SHA_DIGEST_WORDS);
	return (const byte*)H;
}

#ifdef TEST_SHA
static struct TestSHA
{
	TestSHA(const char *k,const char *test,unsigned rpt=1) {
		SHA256 sha; size_t l=strlen(k); while (rpt--!=0) sha.add((byte*)k,l);
		if (memcmp(sha.result(),test,SHA_DIGEST_BYTES)!=0) printf("SHA256 error: %s\n",k);
	}
}
	tst1("abc",																"\xba\x78\x16\xbf\x8f\x01\xcf\xea\x41\x41\x40\xde\x5d\xae\x22\x23\xb0\x03\x61\xa3\x96\x17\x7a\x9c\xb4\x10\xff\x61\xf2\x00\x15\xad"),
	tst2("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq",		"\x24\x8d\x6a\x61\xd2\x06\x38\xb8\xe5\xc0\x26\x93\x0c\x3e\x60\x39\xa3\x3c\xe4\x59\x64\xff\x21\x67\xf6\xec\xed\xd4\x19\xdb\x06\xc1"),
	tst3("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","\xcd\xc7\x6e\x5c\x99\x14\xfb\x92\x81\xa1\xc7\xe2\x84\xd7\x3e\x67\xf1\x80\x9a\x48\xa4\x97\x20\x0e\x04\x6d\x39\xcc\xc7\x11\x2c\xd0",15625);
#endif

HMAC::HMAC(const byte *key,size_t lkey)
{
    byte ibuf[SHA_INPUT_BYTES],obuf[SHA_INPUT_BYTES];
    memset(ibuf,0x36,sizeof(ibuf)); memset(obuf,0x5c,sizeof(obuf));
	if (lkey<SHA_INPUT_BYTES) {axor(ibuf,key,lkey); axor(obuf,key,lkey);}
	else {
		SHA256 tmp; tmp.add(key,lkey); 
		const uint32_t *key2=(uint32_t*)tmp.result();
		axor((uint32_t*)ibuf,key2,SHA_DIGEST_WORDS); 
		axor((uint32_t*)obuf,key2,SHA_DIGEST_WORDS);
    }
	in.add((byte*)ibuf,sizeof(ibuf)); out.add((byte*)obuf,sizeof(obuf));
}

PWD_PBKDF2::PWD_PBKDF2(const byte *key,size_t lkey,const byte *enc) : fOK(false)
{
	uint32_t count,i;
	if (enc!=NULL) {
		memcpy(pwd,enc,sizeof(uint32_t)+PWD_LSALT);
		count=fLEnd?afy_rev(*(uint32_t*)pwd):*(uint32_t*)pwd;
	} else {
		*(uint32_t*)pwd=count=PWD_COUNT; make_big_endian32((uint32_t*)pwd,1);
		StoreCtx::get()->cryptoMgr->randomBytes(pwd+sizeof(uint32_t),PWD_LSALT);
	}
	HMAC hmac(key,lkey),hsave(hmac); hmac.add(pwd+sizeof(uint32_t),PWD_LSALT);
	uint32_t i32=1; make_big_endian32(&i32,1); hmac.add((byte*)&i32,sizeof(i32));
	memcpy(pwd+sizeof(uint32_t)+PWD_LSALT,hmac.result(),SHA_DIGEST_BYTES);
	byte Ui[SHA_DIGEST_BYTES]; memset(Ui,0,sizeof(Ui));
	for (i=1; i<count; i++) {
		HMAC hm(hsave); hm.add(Ui,sizeof(Ui)); memcpy(Ui,hm.result(),SHA_DIGEST_BYTES);
		axor((uint32_t*)(pwd+sizeof(uint32_t)+PWD_LSALT),(uint32_t*)Ui,SHA_DIGEST_WORDS);
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
		encTab[1][i]=afy_rotl(a,24); encTab[2][i]=afy_rotl(a,16); encTab[3][i]=afy_rotl(a,8);
		if ((a=decSTab[i])==0) decTab[0][i]=decTab[1][i]=decTab[2][i]=decTab[3][i]=0;
		else {
			a=t2[a];
			a=decTab[0][i]=t1[(a+t2[0x0B])%0xFF]^uint32_t(t1[(a+t2[0x0D])%0xFF])<<8
				^uint32_t(t1[(a+t2[0x09])%0xFF])<<16^uint32_t(t1[(a+t2[0x0E])%0xFF])<<24;
			decTab[1][i]=afy_rotl(a,24); decTab[2][i]=afy_rotl(a,16); decTab[3][i]=afy_rotl(a,8);
		}
    }
	for(i=0; i<256; i++) {
		uint32_t a=encSTab[i];
		keyTab[0][i]=decTab[0][a]; keyTab[1][i]=decTab[1][a]; keyTab[2][i]=decTab[2][a]; keyTab[3][i]=decTab[3][a];
	}
}

AES::AES(const byte *key,unsigned lkey,bool fDec)
{
	if (key==NULL) return; Nr=lkey/4+6; assert(lkey==16 || lkey==24 || lkey==32);

	uint32_t *penc=(uint32_t*)enc_key_sch; const unsigned nw=lkey/sizeof(uint32_t); memcpy(penc,key,lkey);

#ifdef MM_AES_NI
	if ((ProcFlags::pf.flg&PRCF_AESNI)!=0) {
		static const uint32_t rcLE[] = {0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x1B, 0x36}; 
		const uint32_t *rc=rcLE,*end=(uint32_t*)&enc_key_sch[Nr+1];
		for (__m128i tmp=_mm_loadu_si128((__m128i *)(key+lkey-16));;penc+=nw) {
			penc[nw]=penc[0]^_mm_extract_epi32(_mm_aeskeygenassist_si128(tmp,0),3)^*(rc++);
			penc[nw+1]=penc[1]^penc[nw]; penc[nw+2]=penc[2]^penc[nw+1]; penc[nw+3]=penc[3]^penc[nw+2];
			if (penc+nw+4>=end) break;
			if (lkey==16) tmp=_mm_insert_epi32(tmp,penc[7],3);
			else if (lkey==24) {penc[10]=penc[4]^penc[9]; penc[11]=penc[5]^penc[10]; tmp=_mm_insert_epi32(tmp,penc[11],3);}
			else {
				tmp=_mm_insert_epi32(tmp,penc[11],3); penc[12]=penc[4]^_mm_extract_epi32(_mm_aeskeygenassist_si128(tmp,0),2);
				penc[13]=penc[5]^penc[12]; penc[14]=penc[6]^penc[13]; penc[15]=penc[7]^penc[14]; tmp=_mm_insert_epi32(tmp,penc[15],3);
			}
		}
	} else
#endif
	{
		make_big_endian32(penc,nw);
		switch (lkey) {
		default: assert(0);
		case 16:
			for (unsigned i=0; i<10; ++i,penc+=4) {
				penc[4]=penc[0]^roundTab[i]^encSTab[penc[3]>>16&0xFF]<<24^encSTab[penc[3]>>8&0xFF]<<16^encSTab[penc[3]&0xFF]<<8^encSTab[penc[3]>>24&0xFF];
				penc[5]=penc[1]^penc[4]; penc[6]=penc[2]^penc[5]; penc[7]=penc[3]^penc[6];
			}
			break;
		case 24: 
			for (unsigned i=0; i<8; ++i,penc+=6) {
				penc[6]=penc[0]^roundTab[i]^encSTab[penc[5]>>16&0xFF]<<24^encSTab[penc[5]>>8&0xFF]<<16^encSTab[penc[5]&0xFF]<<8^encSTab[penc[5]>>24&0xFF];
				penc[7]=penc[1]^penc[6]; penc[8]=penc[2]^penc[7]; penc[9]=penc[3]^penc[8]; penc[10]=penc[4]^penc[9]; penc[11]=penc[5]^penc[10];
			}
			break;
		case 32: 
			for (unsigned i=0; i<7; ++i,penc+=8) {
				penc[8]=penc[0]^roundTab[i]^encSTab[penc[7]>>16&0xFF]<<24^encSTab[penc[7]>>8&0xFF]<<16^encSTab[penc[7]&0xFF]<<8^encSTab[penc[7]>>24&0xFF];
				penc[9]=penc[1]^penc[8]; penc[10]=penc[2]^penc[9]; penc[11]=penc[3]^penc[10];
				penc[12]=penc[4]^encSTab[penc[11]>>24&0xFF]<<24^encSTab[penc[11]>>16&0xFF]<<16^encSTab[penc[11]>>8&0xFF]<<8^encSTab[penc[11]&0xFF];
				penc[13]=penc[5]^penc[12]; penc[14]=penc[6]^penc[13]; penc[15]=penc[7]^penc[14];
			}
			break;
		}
	}
	if (fDec) {
		uint32_t *pdec=(uint32_t*)dec_key_sch; unsigned i;
#ifdef MM_AES_NI
		if ((ProcFlags::pf.flg&PRCF_AESNI)!=0) {
			memcpy(dec_key_sch,enc_key_sch,(Nr+1)*4*sizeof(uint32_t)); unsigned j;
			__m128i tmp=*(__m128i*)pdec; *(__m128i*)pdec=*(__m128i*)(pdec+4*Nr); *(__m128i*)(pdec+4*Nr)=tmp;
			for (i=4,j=4*Nr-4; i<j; i+=4,j-=4) {tmp=_mm_aesimc_si128(*(__m128i*)(pdec+i)); *(__m128i*)(pdec+i)=_mm_aesimc_si128(*(__m128i*)(pdec+j)); *(__m128i*)(pdec+j)=tmp;}
			*(__m128i*)(pdec+i)=_mm_aesimc_si128(*(__m128i*)(pdec+i));
		} else
#endif
		{
			memcpy(dec_key_sch,penc,4*sizeof(uint32_t));
			for(i=1,penc+=4; i<Nr; i++) {
				penc-=8; pdec+=4;
				pdec[0]=keyTab[0][*penc>>24&0xFF]^keyTab[1][*penc>>16&0xFF]^keyTab[2][*penc>>8&0xFF]^keyTab[3][*penc&0xFF]; penc++;
				pdec[1]=keyTab[0][*penc>>24&0xFF]^keyTab[1][*penc>>16&0xFF]^keyTab[2][*penc>>8&0xFF]^keyTab[3][*penc&0xFF]; penc++;
				pdec[2]=keyTab[0][*penc>>24&0xFF]^keyTab[1][*penc>>16&0xFF]^keyTab[2][*penc>>8&0xFF]^keyTab[3][*penc&0xFF]; penc++;
				pdec[3]=keyTab[0][*penc>>24&0xFF]^keyTab[1][*penc>>16&0xFF]^keyTab[2][*penc>>8&0xFF]^keyTab[3][*penc&0xFF]; penc++;
			}
			memcpy(pdec+4,penc-8,4*sizeof(uint32_t));
		}
	}
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
	uint32_t tmp[4],*pk=(uint32_t*)enc_key_sch;
	make_big_endian32(buf,4); axor(buf,pk,4);
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
	uint32_t tmp[4],*pk=(uint32_t*)dec_key_sch;
	make_big_endian32(buf,4); axor(buf,pk,4);
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
#ifdef MM_AES_NI
			if ((ProcFlags::pf.flg&PRCF_AESNI)!=0) {
				__m128i b=_mm_loadu_si128((const __m128i*)p); b=_mm_xor_si128(b,_mm_loadu_si128((const __m128i *)p2)); b=_mm_xor_si128(b,((__m128i*)enc_key_sch)[0]);
				for (unsigned i=1; i+1<Nr; i+=2) {b=_mm_aesenc_si128(b,((__m128i*)enc_key_sch)[i]); b=_mm_aesenc_si128(b,((__m128i*)enc_key_sch)[i+1]);}
				b=_mm_aesenc_si128(b,((__m128i*)enc_key_sch)[Nr-1]); b=_mm_aesenclast_si128(b,((__m128i*)enc_key_sch)[Nr]); _mm_storeu_si128((__m128i *)p,b);
			} else
#endif
			{p[0]^=p2[0]; p[1]^=p2[1]; p[2]^=p2[2]; p[3]^=p2[3]; encryptBlock(p);}
			p2=p; p+=AES_BLOCK_SIZE/sizeof(uint32_t);
		} while ((lbuf-=AES_BLOCK_SIZE)!=0);
		break;
	case AES_CTR:
		//
		break;
	case AES_GCM:
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
			p2=(byte*)p==buf?IV:p-(AES_BLOCK_SIZE/sizeof(uint32_t));
#ifdef MM_AES_NI
			if ((ProcFlags::pf.flg&PRCF_AESNI)!=0) {
				__m128i b=_mm_loadu_si128((const __m128i*)p); b=_mm_xor_si128(b,((__m128i*)dec_key_sch)[0]);
				for (unsigned i=1; i+1<Nr; i+=2) {b=_mm_aesdec_si128(b,((__m128i*)dec_key_sch)[i]); b=_mm_aesdec_si128(b,((__m128i*)dec_key_sch)[i+1]);}
				b=_mm_aesdec_si128(b,((__m128i*)dec_key_sch)[Nr-1]); b=_mm_aesdeclast_si128(b,((__m128i*)dec_key_sch)[Nr]);
				b=_mm_xor_si128(b,_mm_loadu_si128((const __m128i *)p2)); _mm_storeu_si128((__m128i *)p,b);
			} else 
#endif
			{decryptBlock(p); p[0]^=p2[0]; p[1]^=p2[1]; p[2]^=p2[2]; p[3]^=p2[3];}
		} while ((p-=AES_BLOCK_SIZE/sizeof(uint32_t))>=(uint32_t*)buf);
		break;
	case AES_CTR:
		//
		break;
	case AES_GCM:
		//
		break;
	}
}

#ifdef TEST_AES
static struct TestAES
{
	TestAES() {
		static const uint32_t IV[4] = {0x03020100,0x07060504,0x0B0A0908,0x0F0E0D0C}; byte buf[4*16];
		static const char *key="\x60\x3d\xeb\x10\x15\xca\x71\xbe\x2b\x73\xae\xf0\x85\x7d\x77\x81\x1f\x35\x2c\x07\x3b\x61\x08\xd7\x2d\x98\x10\xa3\x09\x14\xdf\xf4";
		static const char *plain="\x6b\xc1\xbe\xe2\x2e\x40\x9f\x96\xe9\x3d\x7e\x11\x73\x93\x17\x2a\xae\x2d\x8a\x57\x1e\x03\xac\x9c\x9e\xb7\x6f\xac\x45\xaf\x8e\x51\x30\xc8\x1c\x46\xa3\x5c\xe4\x11\xe5\xfb\xc1\x19\x1a\x0a\x52\xef\xf6\x9f\x24\x45\xdf\x4f\x9b\x17\xad\x2b\x41\x7b\xe6\x6c\x37\x10";
		static const char *ciphr="\xf5\x8c\x4c\x04\xd6\xe5\xf1\xba\x77\x9e\xab\xfb\x5f\x7b\xfb\xd6\x9c\xfc\x4e\x96\x7e\xdb\x80\x8d\x67\x9f\x77\x7b\xc6\x70\x2c\x7d\x39\xf2\x33\x69\xa9\xd9\xba\xcf\xa5\x30\xe2\x63\x04\x23\x14\x61\xb2\xeb\x05\xe2\xc3\x9b\xe9\xfc\xda\x6c\x19\x07\x8c\x6a\x9d\x1b";
		AES aes((byte*)key,32,true); memcpy(buf,plain,4*16); aes.encrypt(buf,sizeof(buf),IV,AES::AES_CBC);
		if (memcmp(buf,ciphr,sizeof(buf))!=0) printf("AES encryption error\n");
		aes.decrypt(buf,sizeof(buf),IV,AES::AES_CBC);
		if (memcmp(buf,plain,sizeof(buf))!=0) printf("AES decryption error\n");
	}
} aes_tst1;
#endif

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

#define INTEL_DEF_PROV "Intel Hardware Cryptographic Service Provider"

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
				if (acquire!=NULL && mgr.gen!=NULL && acquire(&mgr.ctx,0,0,PROV_RSA_FULL,CRYPT_VERIFYCONTEXT)==FALSE
										&& acquire(&mgr.ctx,0,INTEL_DEF_PROV,PROV_INTEL_SEC, 0)==FALSE) mgr.gen=NULL;
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

RC CryptService::CryptProcessor::invoke(IServiceCtx *ctx,const Value& inp,Value& out,unsigned& mode)
{
	byte iv[IVSIZE];
	if ((mode&ISRV_WRITE)!=0) {
		if (inp.length<HMACSIZE+AES_BLOCK_SIZE) return RC_INVPARAM;
		byte *data=(byte*)inp.bstr+HMACSIZE; uint32_t lData=inp.length-HMACSIZE+AES_BLOCK_SIZE;
		uint32_t lpad=(ceil((uint32_t)lData,AES_BLOCK_SIZE)-(uint32_t)lData-1&AES_BLOCK_SIZE-1)+1;
		memset(data+lData,lpad,lpad); lData+=lpad;
		// generate IV
		encrypt(data,lData,(uint32_t*)iv); add(data,lData); memcpy((byte*)inp.bstr,result(),HMACSIZE);
		out=inp; out.length=HMACSIZE+lData;
	} else {
		if (inp.type!=VT_BSTR || inp.bstr==NULL || inp.length<HMACSIZE) return RC_CORRUPTED;	// maybe, just ask for more?
		byte *data=(byte*)inp.bstr+HMACSIZE; uint32_t lData=inp.length-HMACSIZE;
		add(data,lData); if (memcmp(inp.bstr,result(),HMACSIZE)!=0) return RC_CORRUPTED;
		// generate IV
		decrypt(data,lData,(uint32_t*)iv); lData-=data[lData-1];
		out=inp; out.bstr+=HMACSIZE; out.length=(uint32_t)lData;
	}
	memcpy(enc_key_sch,save_enc_key_sch,sizeof(enc_key_sch));
	memcpy(dec_key_sch,save_dec_key_sch,sizeof(dec_key_sch));
	memcpy(&in,&save_in,sizeof(SHA256));
	memcpy(&out,&save_out,sizeof(SHA256));
	mode|=ISRV_REFINP; return RC_OK;
}
		
void CryptService::CryptProcessor::cleanup(IServiceCtx *ctx,bool fDestroying)
{
	if (!fDestroying) {
		memcpy(enc_key_sch,save_enc_key_sch,sizeof(enc_key_sch)); memcpy(dec_key_sch,save_dec_key_sch,sizeof(dec_key_sch));
		memcpy(&in,&save_in,sizeof(SHA256)); memcpy(&out,&save_out,sizeof(SHA256));
	}
}

RC CryptService::create(IServiceCtx *ctx,uint32_t& dscr,IService::Processor *&ret)
{
	const byte *key=NULL; unsigned lkey=0; uint32_t d=dscr&ISRV_PROC_MASK;
	if (d!=ISRV_READ && d!=ISRV_WRITE) return RC_INVOP;
	// get key, lkey from ctx
	if ((ret=new(ctx) CryptProcessor(key,lkey,d==ISRV_READ))==NULL) return RC_NORESOURCES;
	if (d==ISRV_WRITE) dscr|=ISRV_ENVELOPE;
	return RC_OK;
}

void CryptService::getEnvelope(size_t& lHeader,size_t& lTrailer) const
{
	lHeader=HMACSIZE; lTrailer=AES_BLOCK_SIZE;
}
