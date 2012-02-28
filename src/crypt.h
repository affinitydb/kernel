/**************************************************************************************

Copyright © 2004-2012 VMware, Inc. All rights reserved.

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

#include "sync.h"

namespace AfyKernel
{

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

#define	PWD_ENCRYPT			PWD_PBKDF2

#define SHA1_INPUT_WORDS	16			
#define SHA1_DIGEST_WORDS	5
#define BIT_COUNT_WORDS		2
#define SHA1_INPUT_BYTES	(SHA1_INPUT_WORDS*sizeof(uint32_t))
#define SHA1_DIGEST_BYTES	(SHA1_DIGEST_WORDS*sizeof(uint32_t))
#define BIT_COUNT_BYTES		(BIT_COUNT_WORDS*sizeof(uint32_t))
#define	PWD_COUNT			32
#define	PWD_LSALT			32

#define	PWD_ENC_SIZE		(sizeof(uint32_t)+PWD_LSALT+SHA1_DIGEST_BYTES)
#define	HMAC_KEY_SIZE		SHA1_DIGEST_BYTES
#define	HMAC_SIZE			SHA1_DIGEST_BYTES
#define	ENC_KEY_SIZE		16

class SHA1
{
	uint64_t	bits;
	uint32_t	H[SHA1_DIGEST_WORDS];
    uint8_t		M[SHA1_INPUT_BYTES];

	static const uint32_t	IV[SHA1_DIGEST_WORDS];
	void		transform();
public:
	SHA1() : bits(0) {memcpy(H,IV,sizeof(H));}
	SHA1(const SHA1& s) {bits=s.bits; memcpy(H,s.H,sizeof(H)); memcpy(M,s.M,sizeof(M));}
	void		init() {bits=0; memcpy(H,IV,sizeof(H));}
	void		add(const byte *data,size_t len);
	const byte	*result();
};

class HMAC
{
	SHA1		in,out;
public:
	HMAC(const byte *key,size_t lkey);
	HMAC(const HMAC& hm) : in(hm.in),out(hm.out) {}
	void		add(const byte *data,size_t len) {in.add(data,len);}
	const byte	*result() {out.add(in.result(),SHA1_DIGEST_BYTES); return out.result();}
};

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
	static struct AESInit {AESInit();} init;
	static const uint32_t roundTab[10];
	static uint32_t	encSTab[256],decSTab[256],encTab[4][256],decTab[4][256],keyTab[4][256];
	uint32_t	enc_key_sch[64];
	uint32_t	dec_key_sch[64];
	uint32_t	Nr;
public:
	enum AESMode {AES_CBC, AES_CFB};
public:
	AES(const byte *key,unsigned lkey);
	void encryptBlock(uint32_t buf[4]);
	void decryptBlock(uint32_t buf[4]);
	void encrypt(byte *buf,size_t lbuf,const uint32_t IV[4],AESMode mode=AES_CBC);
	void decrypt(byte *buf,size_t lbuf,const uint32_t IV[4],AESMode mode=AES_CBC);
};

};

#endif
