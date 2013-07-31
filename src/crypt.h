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
 * Cryptography routines
 */
#ifndef _CRYPT_H_
#define _CRYPT_H_

#include "utils.h"

#ifndef __arm__
#ifndef __APPLE__
#include "wmmintrin.h"
#include "smmintrin.h"
#define MM_AES_NI
#endif
#endif

namespace AfyKernel
{

extern RC base64enc(const byte *buf,size_t lbuf,char *str,size_t& lstr);
extern RC base64dec(const char *str,size_t lstr,byte *buf,size_t& lbuf);

class CryptoMgr
{
#ifdef WIN32
	BOOL	(WINAPI* gen)(HANDLE,DWORD,BYTE*);
    HANDLE	ctx;
#else
	int		src;
#endif
	volatile	long	rndsht;
	RWLock				lock;
	byte				rndbuf[1024];
	void				fillBuf();
	static	CryptoMgr	mgr;
public:
	CryptoMgr() : rndsht(sizeof(rndbuf)) {
#ifdef WIN32
		ctx=NULL; gen=NULL;
#else
		src=-1;
#endif
	}
	~CryptoMgr();
	void	randomBytes(byte *buf,size_t lbuf) {
		while (lbuf>0) {
			long sht=rndsht; assert(sht<=(long)sizeof(rndbuf));
			if (sht<(long)sizeof(rndbuf)) lock.lock(RW_S_LOCK);
			else {lock.lock(RW_X_LOCK); if (rndsht==(long)sizeof(rndbuf)) fillBuf();}
			long l=min((long)lbuf,long(sizeof(rndbuf)-sht));
			if (l>0 && cas(&rndsht,sht,sht+l)) {memcpy(buf,rndbuf+sht,l); buf+=l; lbuf-=l;}
			lock.unlock();
		}
	}
	static	CryptoMgr	*get();
};

#define SHA_DIGEST_WORDS	8
#define SHA_DIGEST_BYTES	(SHA_DIGEST_WORDS*sizeof(uint32_t))
#define BIT_COUNT_WORDS		2
#define BIT_COUNT_BYTES		(BIT_COUNT_WORDS*sizeof(uint32_t))

#define SHA_INPUT_WORDS		16
#define SHA_INPUT_BYTES		(SHA_INPUT_WORDS*sizeof(uint32_t))

class SHA
{
	uint64_t	bits;
	uint32_t	H[SHA_DIGEST_WORDS];
    uint8_t		M[SHA_INPUT_BYTES];
	void		transform();
	static		const	uint32_t IV[SHA_DIGEST_WORDS];
public:
	SHA() : bits(0) {memcpy(H,IV,sizeof(H));}
	SHA(const SHA& s) {bits=s.bits; memcpy(H,s.H,sizeof(H)); memcpy(M,s.M,sizeof(M));}
	void		init() {bits=0; memcpy(H,IV,sizeof(H));}
	void		add(const byte *data,size_t len);
	const byte	*result();
};

#define HMACSIZE	16			/**< size of HMAC field for page encryption */
#define	IVSIZE		16			/**< initial random vector size for page encryption */

#define	HMAC_KEY_SIZE		SHA_DIGEST_BYTES
#define	HMAC_SIZE			SHA_DIGEST_BYTES

class HMAC
{
protected:
	SHA		in,out;
public:
	HMAC(const byte *key,size_t lkey);
	HMAC(const HMAC& hm) : in(hm.in),out(hm.out) {}
	void		add(const byte *data,size_t len) {in.add(data,len);}
	const byte	*result() {out.add(in.result(),SHA_DIGEST_BYTES); return out.result();}
};

#define	PWD_COUNT			32
#define	PWD_LSALT			32

#define	ENC_KEY_SIZE		32
#define	PWD_ENC_SIZE		(sizeof(uint32_t)+PWD_LSALT+ENC_KEY_SIZE)

#define	PWD_ENCRYPT			PWD_PBKDF2

class PWD_PBKDF2
{
	uint8_t		pwd[PWD_ENC_SIZE];
	bool		fOK;
public:
	PWD_PBKDF2(const byte *key,size_t lkey,const byte *enc=NULL);
	const uint8_t	*encrypted() const {return pwd;}
	bool			isOK() const {return fOK;}
};

#define AES_BLOCK_SIZE	(4*sizeof(uint32_t))

class AES
{
protected:
	static struct AESInit {AESInit();} init;
	static const uint32_t roundTab[10];
	static uint32_t	encSTab[256],decSTab[256],encTab[4][256],decTab[4][256],keyTab[4][256];
#ifdef MM_AES_NI
	__m128i		enc_key_sch[15];
	__m128i		dec_key_sch[15];
#else
	uint32_t	enc_key_sch[64];
	uint32_t	dec_key_sch[64];
#endif
	uint32_t	Nr;
public:
	enum AESMode {AES_CBC, AES_CTR, AES_GCM};
public:
	AES(const byte *key,unsigned lkey,bool fDec);
	void encryptBlock(uint32_t buf[4]);
	void decryptBlock(uint32_t buf[4]);
	void encrypt(byte *buf,size_t lbuf,const uint32_t IV[4],AESMode mode=AES_CBC);
	void decrypt(byte *buf,size_t lbuf,const uint32_t IV[4],AESMode mode=AES_CBC);
};

class MPN
{
	union {
		struct {
			uint16_t	fNeg	:1;
			uint16_t	fNegY	:1;
			uint16_t	nBits	:14;
		};
		uint32_t		words[1];
	};
public:
	MPN&	add(MPN& mpn1,MPN& mpn2);
	MPN&	sub(MPN& mpb1,MPN& mpn2);
	MPN&	mul(MPN& mpb1,MPN& mpn2,SubAlloc& ma);
	MPN&	div(MPN& mpb1,MPN& mpn2,SubAlloc& ma);
	MPN&	mod(MPN& mpb1,MPN& mpn2,SubAlloc& ma);
	MPN&	square(MPN& mpn,SubAlloc& ma);
	MPN&	sqrt(MPN& mpn,SubAlloc& ma);
	MPN&	random(unsigned bits,SubAlloc& ma);
	MPN&	random(const MPN& range,SubAlloc& ma);
private:
	MPN&	operator+=(const MPN&);
	MPN&	operator-=(const MPN&);
	MPN&	operator*=(const MPN&);
	MPN&	operator/=(const MPN&);
	MPN&	operator%=(const MPN&);
};

struct ECParams
{
	MPN		*A;
	MPN		*B;
	//...
};

class StoreCtx;

class CryptService : public IService
{
	StoreCtx	*const	ctx;
	struct CryptProcessor : public Processor, public AES, public HMAC {
		uint32_t	save_enc_key_sch[64];
		uint32_t	save_dec_key_sch[64];
		SHA			save_in,save_out;
		CryptProcessor(const byte *key,unsigned lkey,bool fDec) : AES(key,lkey,fDec),HMAC(key,lkey) {
			memcpy(save_enc_key_sch,enc_key_sch,sizeof(enc_key_sch)); memcpy(save_dec_key_sch,dec_key_sch,sizeof(dec_key_sch));
			memcpy(&save_in,&in,sizeof(SHA)); memcpy(&save_out,&out,sizeof(SHA));
		}
		RC invoke(IServiceCtx *ctx,const Value& inp,Value& out,unsigned& mode);
		void cleanup(IServiceCtx *,bool fDestroy);
	};
public:
	CryptService(StoreCtx *ct) : ctx(ct) {}
	RC create(IServiceCtx *ctx,uint32_t& dscr,IService::Processor *&ret);
	void getEnvelope(size_t& lHeader,size_t& lTrailer) const;
};

};

#endif
