/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include <limits.h>
#include <wchar.h>
#include <float.h>
#include <math.h>
#include "utils.h"
#include "session.h"
#include "mvstoreimpl.h"

using namespace	MVStore;
using namespace MVStoreKernel;

CRC32 CRC32::_CRC;

CRC32::CRC32()
{
     for (int n=0; n<256; n++) {
		ulong c = (ulong)n;
		for (int k=0; k<8; k++) c = c&1 ? 0xedb88320L ^ (c >> 1) : c >> 1;
		CRCTable[n] = c;
     }
}

const byte UTF8::slen[] = 
{
	1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,	// 0x00-0x0F
	1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,	// 0x10-0x1F 
	1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,	// 0x20-0x2F 
	1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,	// 0x30-0x3F 
	1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,	// 0x40-0x4F 
	1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,	// 0x50-0x5F 
	1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,	// 0x60-0x6F 
	1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,	// 0x70-0x7F 
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,	// 0x80-0x8F 
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,	// 0x90-0x9F 
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,	// 0xA0-0xAF 
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,	// 0xB0-0xBF 
	0,0,2,2,2,2,2,2,2,2,2,2,2,2,2,2,	// 0xC0-0xCF 
	2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,	// 0xD0-0xDF 
	3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,	// 0xE0-0xEF 
	4,4,4,4,4,0,0,0,0,0,0,0,0,0,0,0,	// 0xF0-0xFF 
};

const byte UTF8::sec[64][2] = 
{
	{ 0x00, 0x00 }, { 0x00, 0x00 }, { 0x80, 0xBF }, { 0x80, 0xBF },
	{ 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF },
	{ 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF },
	{ 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF },
	{ 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF },
	{ 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF },
	{ 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF }, 
	{ 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF }, 
	{ 0xA0, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF }, 
	{ 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF }, 
	{ 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF }, 
	{ 0x80, 0xBF }, { 0x80, 0x9F }, { 0x80, 0xBF }, { 0x80, 0xBF }, 
	{ 0x90, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF }, { 0x80, 0xBF }, 
	{ 0x80, 0x8F }, { 0x00, 0x00 }, { 0x00, 0x00 }, { 0x00, 0x00 }, 
	{ 0x00, 0x00 }, { 0x00, 0x00 }, { 0x00, 0x00 }, { 0x00, 0x00 }, 
	{ 0x00, 0x00 }, { 0x00, 0x00 }, { 0x00, 0x00 }, { 0x00, 0x00 }, 
};

const byte UTF8::mrk[] = {0x00, 0x00, 0xC0, 0xE0, 0xF0, 0xF8, 0xFC};

const ushort UTF8::upperrng[] =
{
	0x0041,	0x005a, 532,	/* A-Z a-z */
	0x00c0,	0x00d6, 532,	/* À-Ö à-ö */
	0x00d8,	0x00de, 532,	/* Ø-Þ ø-þ */
	0x0189,	0x018a, 705,	/* Ð-? ?-? */
	0x018e,	0x018f, 702,	/* ?-? ?-? */
	0x01b1,	0x01b2, 717,	/* ?-? ?-? */
	0x0388,	0x038a, 537,	/* ?-? ?-? */
	0x038e,	0x038f, 563,	/* ?-? ?-? */
	0x0391,	0x03a1, 532,	/* ?-? a-? */
	0x03a3,	0x03ab, 532,	/* S-? s-? */
	0x0401,	0x040c, 580,	/* ?-? ?-? */
	0x040e,	0x040f, 580,	/* ?-? ?-? */
	0x0410,	0x042f, 532,	/* ?-? ?-? */
	0x0531,	0x0556, 548,	/* ?-? ?-? */
	0x10a0,	0x10c5, 548,	/* ?-? ?-? */
	0x1f08,	0x1f0f, 492,	/* ?-? ?-? */
	0x1f18,	0x1f1d, 492,	/* ?-? ?-? */
	0x1f28,	0x1f2f, 492,	/* ?-? ?-? */
	0x1f38,	0x1f3f, 492,	/* ?-? ?-? */
	0x1f48,	0x1f4d, 492,	/* ?-? ?-? */
	0x1f68,	0x1f6f, 492,	/* ?-? ?-? */
	0x1f88,	0x1f8f, 492,	/* ?-? ?-? */
	0x1f98,	0x1f9f, 492,	/* ?-? ?-? */
	0x1fa8,	0x1faf, 492,	/* ?-? ?-? */
	0x1fb8,	0x1fb9, 492,	/* ?-? ?-? */
	0x1fba,	0x1fbb, 426,	/* ?-? ?-? */
	0x1fc8,	0x1fcb, 414,	/* ?-? ?-? */
	0x1fd8,	0x1fd9, 492,	/* ?-? ?-? */
	0x1fda,	0x1fdb, 400,	/* ?-? ?-? */
	0x1fe8,	0x1fe9, 492,	/* ?-? ?-? */
	0x1fea,	0x1feb, 388,	/* ?-? ?-? */
	0x1ff8,	0x1ff9, 372,	/* ?-? ?-? */
	0x1ffa,	0x1ffb, 374,	/* ?-? ?-? */
	0x2160,	0x216f, 516,	/* ?-? ?-? */
	0x24b6,	0x24cf, 526,	/* ?-? ?-? */
	0xff21,	0xff3a, 532,	/* A-Z a-z */
};

const ulong UTF8::nupperrng = sizeof(upperrng)/(sizeof(upperrng[0]*3));

const ushort UTF8::uppersgl[] =
{
	0x0100, 501,	/* A a */
	0x0102, 501,	/* A a */
	0x0104, 501,	/* A a */
	0x0106, 501,	/* C c */
	0x0108, 501,	/* C c */
	0x010a, 501,	/* C c */
	0x010c, 501,	/* C c */
	0x010e, 501,	/* D d */
	0x0110, 501,	/* Ð d */
	0x0112, 501,	/* E e */
	0x0114, 501,	/* E e */
	0x0116, 501,	/* E e */
	0x0118, 501,	/* E e */
	0x011a, 501,	/* E e */
	0x011c, 501,	/* G g */
	0x011e, 501,	/* G g */
	0x0120, 501,	/* G g */
	0x0122, 501,	/* G g */
	0x0124, 501,	/* H h */
	0x0126, 501,	/* H h */
	0x0128, 501,	/* I i */
	0x012a, 501,	/* I i */
	0x012c, 501,	/* I i */
	0x012e, 501,	/* I i */
	0x0130, 301,	/* I i */
	0x0132, 501,	/* ? ? */
	0x0134, 501,	/* J j */
	0x0136, 501,	/* K k */
	0x0139, 501,	/* L l */
	0x013b, 501,	/* L l */
	0x013d, 501,	/* L l */
	0x013f, 501,	/* ? ? */
	0x0141, 501,	/* L l */
	0x0143, 501,	/* N n */
	0x0145, 501,	/* N n */
	0x0147, 501,	/* N n */
	0x014a, 501,	/* ? ? */
	0x014c, 501,	/* O o */
	0x014e, 501,	/* O o */
	0x0150, 501,	/* O o */
	0x0152, 501,	/* Œ œ */
	0x0154, 501,	/* R r */
	0x0156, 501,	/* R r */
	0x0158, 501,	/* R r */
	0x015a, 501,	/* S s */
	0x015c, 501,	/* S s */
	0x015e, 501,	/* S s */
	0x0160, 501,	/* Š š */
	0x0162, 501,	/* T t */
	0x0164, 501,	/* T t */
	0x0166, 501,	/* T t */
	0x0168, 501,	/* U u */
	0x016a, 501,	/* U u */
	0x016c, 501,	/* U u */
	0x016e, 501,	/* U u */
	0x0170, 501,	/* U u */
	0x0172, 501,	/* U u */
	0x0174, 501,	/* W w */
	0x0176, 501,	/* Y y */
	0x0178, 379,	/* Ÿ ÿ */
	0x0179, 501,	/* Z z */
	0x017b, 501,	/* Z z */
	0x017d, 501,	/* Ž ž */
	0x0181, 710,	/* ? ? */
	0x0182, 501,	/* ? ? */
	0x0184, 501,	/* ? ? */
	0x0186, 706,	/* ? ? */
	0x0187, 501,	/* ? ? */
	0x018b, 501,	/* ? ? */
	0x0190, 703,	/* ? ? */
	0x0191, 501,	/* ƒ ƒ */
	0x0193, 705,	/* ? ? */
	0x0194, 707,	/* ? ? */
	0x0196, 711,	/* ? ? */
	0x0197, 709,	/* I ? */
	0x0198, 501,	/* ? ? */
	0x019c, 711,	/* ? ? */
	0x019d, 713,	/* ? ? */
	0x01a0, 501,	/* O o */
	0x01a2, 501,	/* ? ? */
	0x01a4, 501,	/* ? ? */
	0x01a7, 501,	/* ? ? */
	0x01a9, 718,	/* ? ? */
	0x01ac, 501,	/* ? ? */
	0x01ae, 718,	/* T ? */
	0x01af, 501,	/* U u */
	0x01b3, 501,	/* ? ? */
	0x01b5, 501,	/* ? z */
	0x01b7, 719,	/* ? ? */
	0x01b8, 501,	/* ? ? */
	0x01bc, 501,	/* ? ? */
	0x01c4, 502,	/* ? ? */
	0x01c5, 501,	/* ? ? */
	0x01c7, 502,	/* ? ? */
	0x01c8, 501,	/* ? ? */
	0x01ca, 502,	/* ? ? */
	0x01cb, 501,	/* ? ? */
	0x01cd, 501,	/* A a */
	0x01cf, 501,	/* I i */
	0x01d1, 501,	/* O o */
	0x01d3, 501,	/* U u */
	0x01d5, 501,	/* U u */
	0x01d7, 501,	/* U u */
	0x01d9, 501,	/* U u */
	0x01db, 501,	/* U u */
	0x01de, 501,	/* A a */
	0x01e0, 501,	/* ? ? */
	0x01e2, 501,	/* ? ? */
	0x01e4, 501,	/* G g */
	0x01e6, 501,	/* G g */
	0x01e8, 501,	/* K k */
	0x01ea, 501,	/* O o */
	0x01ec, 501,	/* O o */
	0x01ee, 501,	/* ? ? */
	0x01f1, 502,	/* ? ? */
	0x01f2, 501,	/* ? ? */
	0x01f4, 501,	/* ? ? */
	0x01fa, 501,	/* ? ? */
	0x01fc, 501,	/* ? ? */
	0x01fe, 501,	/* ? ? */
	0x0200, 501,	/* ? ? */
	0x0202, 501,	/* ? ? */
	0x0204, 501,	/* ? ? */
	0x0206, 501,	/* ? ? */
	0x0208, 501,	/* ? ? */
	0x020a, 501,	/* ? ? */
	0x020c, 501,	/* ? ? */
	0x020e, 501,	/* ? ? */
	0x0210, 501,	/* ? ? */
	0x0212, 501,	/* ? ? */
	0x0214, 501,	/* ? ? */
	0x0216, 501,	/* ? ? */
	0x0386, 538,	/* ? ? */
	0x038c, 564,	/* ? ? */
	0x03e2, 501,	/* ? ? */
	0x03e4, 501,	/* ? ? */
	0x03e6, 501,	/* ? ? */
	0x03e8, 501,	/* ? ? */
	0x03ea, 501,	/* ? ? */
	0x03ec, 501,	/* ? ? */
	0x03ee, 501,	/* ? ? */
	0x0460, 501,	/* ? ? */
	0x0462, 501,	/* ? ? */
	0x0464, 501,	/* ? ? */
	0x0466, 501,	/* ? ? */
	0x0468, 501,	/* ? ? */
	0x046a, 501,	/* ? ? */
	0x046c, 501,	/* ? ? */
	0x046e, 501,	/* ? ? */
	0x0470, 501,	/* ? ? */
	0x0472, 501,	/* ? ? */
	0x0474, 501,	/* ? ? */
	0x0476, 501,	/* ? ? */
	0x0478, 501,	/* ? ? */
	0x047a, 501,	/* ? ? */
	0x047c, 501,	/* ? ? */
	0x047e, 501,	/* ? ? */
	0x0480, 501,	/* ? ? */
	0x0490, 501,	/* ? ? */
	0x0492, 501,	/* ? ? */
	0x0494, 501,	/* ? ? */
	0x0496, 501,	/* ? ? */
	0x0498, 501,	/* ? ? */
	0x049a, 501,	/* ? ? */
	0x049c, 501,	/* ? ? */
	0x049e, 501,	/* ? ? */
	0x04a0, 501,	/* ? ? */
	0x04a2, 501,	/* ? ? */
	0x04a4, 501,	/* ? ? */
	0x04a6, 501,	/* ? ? */
	0x04a8, 501,	/* ? ? */
	0x04aa, 501,	/* ? ? */
	0x04ac, 501,	/* ? ? */
	0x04ae, 501,	/* ? ? */
	0x04b0, 501,	/* ? ? */
	0x04b2, 501,	/* ? ? */
	0x04b4, 501,	/* ? ? */
	0x04b6, 501,	/* ? ? */
	0x04b8, 501,	/* ? ? */
	0x04ba, 501,	/* ? h */
	0x04bc, 501,	/* ? ? */
	0x04be, 501,	/* ? ? */
	0x04c1, 501,	/* ? ? */
	0x04c3, 501,	/* ? ? */
	0x04c7, 501,	/* ? ? */
	0x04cb, 501,	/* ? ? */
	0x04d0, 501,	/* ? ? */
	0x04d2, 501,	/* ? ? */
	0x04d4, 501,	/* ? ? */
	0x04d6, 501,	/* ? ? */
	0x04d8, 501,	/* ? ? */
	0x04da, 501,	/* ? ? */
	0x04dc, 501,	/* ? ? */
	0x04de, 501,	/* ? ? */
	0x04e0, 501,	/* ? ? */
	0x04e2, 501,	/* ? ? */
	0x04e4, 501,	/* ? ? */
	0x04e6, 501,	/* ? ? */
	0x04e8, 501,	/* ? ? */
	0x04ea, 501,	/* ? ? */
	0x04ee, 501,	/* ? ? */
	0x04f0, 501,	/* ? ? */
	0x04f2, 501,	/* ? ? */
	0x04f4, 501,	/* ? ? */
	0x04f8, 501,	/* ? ? */
	0x1e00, 501,	/* ? ? */
	0x1e02, 501,	/* ? ? */
	0x1e04, 501,	/* ? ? */
	0x1e06, 501,	/* ? ? */
	0x1e08, 501,	/* ? ? */
	0x1e0a, 501,	/* ? ? */
	0x1e0c, 501,	/* ? ? */
	0x1e0e, 501,	/* ? ? */
	0x1e10, 501,	/* ? ? */
	0x1e12, 501,	/* ? ? */
	0x1e14, 501,	/* ? ? */
	0x1e16, 501,	/* ? ? */
	0x1e18, 501,	/* ? ? */
	0x1e1a, 501,	/* ? ? */
	0x1e1c, 501,	/* ? ? */
	0x1e1e, 501,	/* ? ? */
	0x1e20, 501,	/* ? ? */
	0x1e22, 501,	/* ? ? */
	0x1e24, 501,	/* ? ? */
	0x1e26, 501,	/* ? ? */
	0x1e28, 501,	/* ? ? */
	0x1e2a, 501,	/* ? ? */
	0x1e2c, 501,	/* ? ? */
	0x1e2e, 501,	/* ? ? */
	0x1e30, 501,	/* ? ? */
	0x1e32, 501,	/* ? ? */
	0x1e34, 501,	/* ? ? */
	0x1e36, 501,	/* ? ? */
	0x1e38, 501,	/* ? ? */
	0x1e3a, 501,	/* ? ? */
	0x1e3c, 501,	/* ? ? */
	0x1e3e, 501,	/* ? ? */
	0x1e40, 501,	/* ? ? */
	0x1e42, 501,	/* ? ? */
	0x1e44, 501,	/* ? ? */
	0x1e46, 501,	/* ? ? */
	0x1e48, 501,	/* ? ? */
	0x1e4a, 501,	/* ? ? */
	0x1e4c, 501,	/* ? ? */
	0x1e4e, 501,	/* ? ? */
	0x1e50, 501,	/* ? ? */
	0x1e52, 501,	/* ? ? */
	0x1e54, 501,	/* ? ? */
	0x1e56, 501,	/* ? ? */
	0x1e58, 501,	/* ? ? */
	0x1e5a, 501,	/* ? ? */
	0x1e5c, 501,	/* ? ? */
	0x1e5e, 501,	/* ? ? */
	0x1e60, 501,	/* ? ? */
	0x1e62, 501,	/* ? ? */
	0x1e64, 501,	/* ? ? */
	0x1e66, 501,	/* ? ? */
	0x1e68, 501,	/* ? ? */
	0x1e6a, 501,	/* ? ? */
	0x1e6c, 501,	/* ? ? */
	0x1e6e, 501,	/* ? ? */
	0x1e70, 501,	/* ? ? */
	0x1e72, 501,	/* ? ? */
	0x1e74, 501,	/* ? ? */
	0x1e76, 501,	/* ? ? */
	0x1e78, 501,	/* ? ? */
	0x1e7a, 501,	/* ? ? */
	0x1e7c, 501,	/* ? ? */
	0x1e7e, 501,	/* ? ? */
	0x1e80, 501,	/* ? ? */
	0x1e82, 501,	/* ? ? */
	0x1e84, 501,	/* ? ? */
	0x1e86, 501,	/* ? ? */
	0x1e88, 501,	/* ? ? */
	0x1e8a, 501,	/* ? ? */
	0x1e8c, 501,	/* ? ? */
	0x1e8e, 501,	/* ? ? */
	0x1e90, 501,	/* ? ? */
	0x1e92, 501,	/* ? ? */
	0x1e94, 501,	/* ? ? */
	0x1ea0, 501,	/* ? ? */
	0x1ea2, 501,	/* ? ? */
	0x1ea4, 501,	/* ? ? */
	0x1ea6, 501,	/* ? ? */
	0x1ea8, 501,	/* ? ? */
	0x1eaa, 501,	/* ? ? */
	0x1eac, 501,	/* ? ? */
	0x1eae, 501,	/* ? ? */
	0x1eb0, 501,	/* ? ? */
	0x1eb2, 501,	/* ? ? */
	0x1eb4, 501,	/* ? ? */
	0x1eb6, 501,	/* ? ? */
	0x1eb8, 501,	/* ? ? */
	0x1eba, 501,	/* ? ? */
	0x1ebc, 501,	/* ? ? */
	0x1ebe, 501,	/* ? ? */
	0x1ec0, 501,	/* ? ? */
	0x1ec2, 501,	/* ? ? */
	0x1ec4, 501,	/* ? ? */
	0x1ec6, 501,	/* ? ? */
	0x1ec8, 501,	/* ? ? */
	0x1eca, 501,	/* ? ? */
	0x1ecc, 501,	/* ? ? */
	0x1ece, 501,	/* ? ? */
	0x1ed0, 501,	/* ? ? */
	0x1ed2, 501,	/* ? ? */
	0x1ed4, 501,	/* ? ? */
	0x1ed6, 501,	/* ? ? */
	0x1ed8, 501,	/* ? ? */
	0x1eda, 501,	/* ? ? */
	0x1edc, 501,	/* ? ? */
	0x1ede, 501,	/* ? ? */
	0x1ee0, 501,	/* ? ? */
	0x1ee2, 501,	/* ? ? */
	0x1ee4, 501,	/* ? ? */
	0x1ee6, 501,	/* ? ? */
	0x1ee8, 501,	/* ? ? */
	0x1eea, 501,	/* ? ? */
	0x1eec, 501,	/* ? ? */
	0x1eee, 501,	/* ? ? */
	0x1ef0, 501,	/* ? ? */
	0x1ef2, 501,	/* ? ? */
	0x1ef4, 501,	/* ? ? */
	0x1ef6, 501,	/* ? ? */
	0x1ef8, 501,	/* ? ? */
	0x1f59, 492,	/* ? ? */
	0x1f5b, 492,	/* ? ? */
	0x1f5d, 492,	/* ? ? */
	0x1f5f, 492,	/* ? ? */
	0x1fbc, 491,	/* ? ? */
	0x1fcc, 491,	/* ? ? */
	0x1fec, 493,	/* ? ? */
	0x1ffc, 491,	/* ? ? */
};

const ulong UTF8::nuppersgl = sizeof(uppersgl)/(sizeof(uppersgl[0]*2));

const ushort UTF8::lowerrng[] =
{
	0x0061,	0x007a, 468,	/* a-z A-Z */
	0x00e0,	0x00f6, 468,	/* à-ö À-Ö */
	0x00f8,	0x00fe, 468,	/* ø-þ Ø-Þ */
	0x0256,	0x0257, 295,	/* ?-? Ð-? */
	0x0258,	0x0259, 298,	/* ?-? ?-? */
	0x028a,	0x028b, 283,	/* ?-? ?-? */
	0x03ad,	0x03af, 463,	/* ?-? ?-? */
	0x03b1,	0x03c1, 468,	/* a-? ?-? */
	0x03c3,	0x03cb, 468,	/* s-? S-? */
	0x03cd,	0x03ce, 437,	/* ?-? ?-? */
	0x0430,	0x044f, 468,	/* ?-? ?-? */
	0x0451,	0x045c, 420,	/* ?-? ?-? */
	0x045e,	0x045f, 420,	/* ?-? ?-? */
	0x0561,	0x0586, 452,	/* ?-? ?-? */
	0x1f00,	0x1f07, 508,	/* ?-? ?-? */
	0x1f10,	0x1f15, 508,	/* ?-? ?-? */
	0x1f20,	0x1f27, 508,	/* ?-? ?-? */
	0x1f30,	0x1f37, 508,	/* ?-? ?-? */
	0x1f40,	0x1f45, 508,	/* ?-? ?-? */
	0x1f60,	0x1f67, 508,	/* ?-? ?-? */
	0x1f70,	0x1f71, 574,	/* ?-? ?-? */
	0x1f72,	0x1f75, 586,	/* ?-? ?-? */
	0x1f76,	0x1f77, 600,	/* ?-? ?-? */
	0x1f78,	0x1f79, 628,	/* ?-? ?-? */
	0x1f7a,	0x1f7b, 612,	/* ?-? ?-? */
	0x1f7c,	0x1f7d, 626,	/* ?-? ?-? */
	0x1f80,	0x1f87, 508,	/* ?-? ?-? */
	0x1f90,	0x1f97, 508,	/* ?-? ?-? */
	0x1fa0,	0x1fa7, 508,	/* ?-? ?-? */
	0x1fb0,	0x1fb1, 508,	/* ?-? ?-? */
	0x1fd0,	0x1fd1, 508,	/* ?-? ?-? */
	0x1fe0,	0x1fe1, 508,	/* ?-? ?-? */
	0x2170,	0x217f, 484,	/* ?-? ?-? */
	0x24d0,	0x24e9, 474,	/* ?-? ?-? */
	0xff41,	0xff5a, 468,	/* a-z A-Z */
};

const ulong UTF8::nlowerrng = sizeof(lowerrng)/(sizeof(lowerrng[0]*3));

const ushort UTF8::lowersgl[] =
{
	0x00ff, 621,	/* ÿ Ÿ */
	0x0101, 499,	/* a A */
	0x0103, 499,	/* a A */
	0x0105, 499,	/* a A */
	0x0107, 499,	/* c C */
	0x0109, 499,	/* c C */
	0x010b, 499,	/* c C */
	0x010d, 499,	/* c C */
	0x010f, 499,	/* d D */
	0x0111, 499,	/* d Ð */
	0x0113, 499,	/* e E */
	0x0115, 499,	/* e E */
	0x0117, 499,	/* e E */
	0x0119, 499,	/* e E */
	0x011b, 499,	/* e E */
	0x011d, 499,	/* g G */
	0x011f, 499,	/* g G */
	0x0121, 499,	/* g G */
	0x0123, 499,	/* g G */
	0x0125, 499,	/* h H */
	0x0127, 499,	/* h H */
	0x0129, 499,	/* i I */
	0x012b, 499,	/* i I */
	0x012d, 499,	/* i I */
	0x012f, 499,	/* i I */
	0x0131, 268,	/* i I */
	0x0133, 499,	/* ? ? */
	0x0135, 499,	/* j J */
	0x0137, 499,	/* k K */
	0x013a, 499,	/* l L */
	0x013c, 499,	/* l L */
	0x013e, 499,	/* l L */
	0x0140, 499,	/* ? ? */
	0x0142, 499,	/* l L */
	0x0144, 499,	/* n N */
	0x0146, 499,	/* n N */
	0x0148, 499,	/* n N */
	0x014b, 499,	/* ? ? */
	0x014d, 499,	/* o O */
	0x014f, 499,	/* o O */
	0x0151, 499,	/* o O */
	0x0153, 499,	/* œ Œ */
	0x0155, 499,	/* r R */
	0x0157, 499,	/* r R */
	0x0159, 499,	/* r R */
	0x015b, 499,	/* s S */
	0x015d, 499,	/* s S */
	0x015f, 499,	/* s S */
	0x0161, 499,	/* š Š */
	0x0163, 499,	/* t T */
	0x0165, 499,	/* t T */
	0x0167, 499,	/* t T */
	0x0169, 499,	/* u U */
	0x016b, 499,	/* u U */
	0x016d, 499,	/* u U */
	0x016f, 499,	/* u U */
	0x0171, 499,	/* u U */
	0x0173, 499,	/* u U */
	0x0175, 499,	/* w W */
	0x0177, 499,	/* y Y */
	0x017a, 499,	/* z Z */
	0x017c, 499,	/* z Z */
	0x017e, 499,	/* ž Ž */
	0x017f, 200,	/* ? S */
	0x0183, 499,	/* ? ? */
	0x0185, 499,	/* ? ? */
	0x0188, 499,	/* ? ? */
	0x018c, 499,	/* ? ? */
	0x0192, 499,	/* ƒ ƒ */
	0x0199, 499,	/* ? ? */
	0x01a1, 499,	/* o O */
	0x01a3, 499,	/* ? ? */
	0x01a5, 499,	/* ? ? */
	0x01a8, 499,	/* ? ? */
	0x01ad, 499,	/* ? ? */
	0x01b0, 499,	/* u U */
	0x01b4, 499,	/* ? ? */
	0x01b6, 499,	/* z ? */
	0x01b9, 499,	/* ? ? */
	0x01bd, 499,	/* ? ? */
	0x01c5, 499,	/* ? ? */
	0x01c6, 498,	/* ? ? */
	0x01c8, 499,	/* ? ? */
	0x01c9, 498,	/* ? ? */
	0x01cb, 499,	/* ? ? */
	0x01cc, 498,	/* ? ? */
	0x01ce, 499,	/* a A */
	0x01d0, 499,	/* i I */
	0x01d2, 499,	/* o O */
	0x01d4, 499,	/* u U */
	0x01d6, 499,	/* u U */
	0x01d8, 499,	/* u U */
	0x01da, 499,	/* u U */
	0x01dc, 499,	/* u U */
	0x01df, 499,	/* a A */
	0x01e1, 499,	/* ? ? */
	0x01e3, 499,	/* ? ? */
	0x01e5, 499,	/* g G */
	0x01e7, 499,	/* g G */
	0x01e9, 499,	/* k K */
	0x01eb, 499,	/* o O */
	0x01ed, 499,	/* o O */
	0x01ef, 499,	/* ? ? */
	0x01f2, 499,	/* ? ? */
	0x01f3, 498,	/* ? ? */
	0x01f5, 499,	/* ? ? */
	0x01fb, 499,	/* ? ? */
	0x01fd, 499,	/* ? ? */
	0x01ff, 499,	/* ? ? */
	0x0201, 499,	/* ? ? */
	0x0203, 499,	/* ? ? */
	0x0205, 499,	/* ? ? */
	0x0207, 499,	/* ? ? */
	0x0209, 499,	/* ? ? */
	0x020b, 499,	/* ? ? */
	0x020d, 499,	/* ? ? */
	0x020f, 499,	/* ? ? */
	0x0211, 499,	/* ? ? */
	0x0213, 499,	/* ? ? */
	0x0215, 499,	/* ? ? */
	0x0217, 499,	/* ? ? */
	0x0253, 290,	/* ? ? */
	0x0254, 294,	/* ? ? */
	0x025b, 297,	/* ? ? */
	0x0260, 295,	/* ? ? */
	0x0263, 293,	/* ? ? */
	0x0268, 291,	/* ? I */
	0x0269, 289,	/* ? ? */
	0x026f, 289,	/* ? ? */
	0x0272, 287,	/* ? ? */
	0x0283, 282,	/* ? ? */
	0x0288, 282,	/* ? T */
	0x0292, 281,	/* ? ? */
	0x03ac, 462,	/* ? ? */
	0x03cc, 436,	/* ? ? */
	0x03d0, 438,	/* ? ? */
	0x03d1, 443,	/* ? T */
	0x03d5, 453,	/* ? F */
	0x03d6, 446,	/* ? ? */
	0x03e3, 499,	/* ? ? */
	0x03e5, 499,	/* ? ? */
	0x03e7, 499,	/* ? ? */
	0x03e9, 499,	/* ? ? */
	0x03eb, 499,	/* ? ? */
	0x03ed, 499,	/* ? ? */
	0x03ef, 499,	/* ? ? */
	0x03f0, 414,	/* ? ? */
	0x03f1, 420,	/* ? ? */
	0x0461, 499,	/* ? ? */
	0x0463, 499,	/* ? ? */
	0x0465, 499,	/* ? ? */
	0x0467, 499,	/* ? ? */
	0x0469, 499,	/* ? ? */
	0x046b, 499,	/* ? ? */
	0x046d, 499,	/* ? ? */
	0x046f, 499,	/* ? ? */
	0x0471, 499,	/* ? ? */
	0x0473, 499,	/* ? ? */
	0x0475, 499,	/* ? ? */
	0x0477, 499,	/* ? ? */
	0x0479, 499,	/* ? ? */
	0x047b, 499,	/* ? ? */
	0x047d, 499,	/* ? ? */
	0x047f, 499,	/* ? ? */
	0x0481, 499,	/* ? ? */
	0x0491, 499,	/* ? ? */
	0x0493, 499,	/* ? ? */
	0x0495, 499,	/* ? ? */
	0x0497, 499,	/* ? ? */
	0x0499, 499,	/* ? ? */
	0x049b, 499,	/* ? ? */
	0x049d, 499,	/* ? ? */
	0x049f, 499,	/* ? ? */
	0x04a1, 499,	/* ? ? */
	0x04a3, 499,	/* ? ? */
	0x04a5, 499,	/* ? ? */
	0x04a7, 499,	/* ? ? */
	0x04a9, 499,	/* ? ? */
	0x04ab, 499,	/* ? ? */
	0x04ad, 499,	/* ? ? */
	0x04af, 499,	/* ? ? */
	0x04b1, 499,	/* ? ? */
	0x04b3, 499,	/* ? ? */
	0x04b5, 499,	/* ? ? */
	0x04b7, 499,	/* ? ? */
	0x04b9, 499,	/* ? ? */
	0x04bb, 499,	/* h ? */
	0x04bd, 499,	/* ? ? */
	0x04bf, 499,	/* ? ? */
	0x04c2, 499,	/* ? ? */
	0x04c4, 499,	/* ? ? */
	0x04c8, 499,	/* ? ? */
	0x04cc, 499,	/* ? ? */
	0x04d1, 499,	/* ? ? */
	0x04d3, 499,	/* ? ? */
	0x04d5, 499,	/* ? ? */
	0x04d7, 499,	/* ? ? */
	0x04d9, 499,	/* ? ? */
	0x04db, 499,	/* ? ? */
	0x04dd, 499,	/* ? ? */
	0x04df, 499,	/* ? ? */
	0x04e1, 499,	/* ? ? */
	0x04e3, 499,	/* ? ? */
	0x04e5, 499,	/* ? ? */
	0x04e7, 499,	/* ? ? */
	0x04e9, 499,	/* ? ? */
	0x04eb, 499,	/* ? ? */
	0x04ef, 499,	/* ? ? */
	0x04f1, 499,	/* ? ? */
	0x04f3, 499,	/* ? ? */
	0x04f5, 499,	/* ? ? */
	0x04f9, 499,	/* ? ? */
	0x1e01, 499,	/* ? ? */
	0x1e03, 499,	/* ? ? */
	0x1e05, 499,	/* ? ? */
	0x1e07, 499,	/* ? ? */
	0x1e09, 499,	/* ? ? */
	0x1e0b, 499,	/* ? ? */
	0x1e0d, 499,	/* ? ? */
	0x1e0f, 499,	/* ? ? */
	0x1e11, 499,	/* ? ? */
	0x1e13, 499,	/* ? ? */
	0x1e15, 499,	/* ? ? */
	0x1e17, 499,	/* ? ? */
	0x1e19, 499,	/* ? ? */
	0x1e1b, 499,	/* ? ? */
	0x1e1d, 499,	/* ? ? */
	0x1e1f, 499,	/* ? ? */
	0x1e21, 499,	/* ? ? */
	0x1e23, 499,	/* ? ? */
	0x1e25, 499,	/* ? ? */
	0x1e27, 499,	/* ? ? */
	0x1e29, 499,	/* ? ? */
	0x1e2b, 499,	/* ? ? */
	0x1e2d, 499,	/* ? ? */
	0x1e2f, 499,	/* ? ? */
	0x1e31, 499,	/* ? ? */
	0x1e33, 499,	/* ? ? */
	0x1e35, 499,	/* ? ? */
	0x1e37, 499,	/* ? ? */
	0x1e39, 499,	/* ? ? */
	0x1e3b, 499,	/* ? ? */
	0x1e3d, 499,	/* ? ? */
	0x1e3f, 499,	/* ? ? */
	0x1e41, 499,	/* ? ? */
	0x1e43, 499,	/* ? ? */
	0x1e45, 499,	/* ? ? */
	0x1e47, 499,	/* ? ? */
	0x1e49, 499,	/* ? ? */
	0x1e4b, 499,	/* ? ? */
	0x1e4d, 499,	/* ? ? */
	0x1e4f, 499,	/* ? ? */
	0x1e51, 499,	/* ? ? */
	0x1e53, 499,	/* ? ? */
	0x1e55, 499,	/* ? ? */
	0x1e57, 499,	/* ? ? */
	0x1e59, 499,	/* ? ? */
	0x1e5b, 499,	/* ? ? */
	0x1e5d, 499,	/* ? ? */
	0x1e5f, 499,	/* ? ? */
	0x1e61, 499,	/* ? ? */
	0x1e63, 499,	/* ? ? */
	0x1e65, 499,	/* ? ? */
	0x1e67, 499,	/* ? ? */
	0x1e69, 499,	/* ? ? */
	0x1e6b, 499,	/* ? ? */
	0x1e6d, 499,	/* ? ? */
	0x1e6f, 499,	/* ? ? */
	0x1e71, 499,	/* ? ? */
	0x1e73, 499,	/* ? ? */
	0x1e75, 499,	/* ? ? */
	0x1e77, 499,	/* ? ? */
	0x1e79, 499,	/* ? ? */
	0x1e7b, 499,	/* ? ? */
	0x1e7d, 499,	/* ? ? */
	0x1e7f, 499,	/* ? ? */
	0x1e81, 499,	/* ? ? */
	0x1e83, 499,	/* ? ? */
	0x1e85, 499,	/* ? ? */
	0x1e87, 499,	/* ? ? */
	0x1e89, 499,	/* ? ? */
	0x1e8b, 499,	/* ? ? */
	0x1e8d, 499,	/* ? ? */
	0x1e8f, 499,	/* ? ? */
	0x1e91, 499,	/* ? ? */
	0x1e93, 499,	/* ? ? */
	0x1e95, 499,	/* ? ? */
	0x1ea1, 499,	/* ? ? */
	0x1ea3, 499,	/* ? ? */
	0x1ea5, 499,	/* ? ? */
	0x1ea7, 499,	/* ? ? */
	0x1ea9, 499,	/* ? ? */
	0x1eab, 499,	/* ? ? */
	0x1ead, 499,	/* ? ? */
	0x1eaf, 499,	/* ? ? */
	0x1eb1, 499,	/* ? ? */
	0x1eb3, 499,	/* ? ? */
	0x1eb5, 499,	/* ? ? */
	0x1eb7, 499,	/* ? ? */
	0x1eb9, 499,	/* ? ? */
	0x1ebb, 499,	/* ? ? */
	0x1ebd, 499,	/* ? ? */
	0x1ebf, 499,	/* ? ? */
	0x1ec1, 499,	/* ? ? */
	0x1ec3, 499,	/* ? ? */
	0x1ec5, 499,	/* ? ? */
	0x1ec7, 499,	/* ? ? */
	0x1ec9, 499,	/* ? ? */
	0x1ecb, 499,	/* ? ? */
	0x1ecd, 499,	/* ? ? */
	0x1ecf, 499,	/* ? ? */
	0x1ed1, 499,	/* ? ? */
	0x1ed3, 499,	/* ? ? */
	0x1ed5, 499,	/* ? ? */
	0x1ed7, 499,	/* ? ? */
	0x1ed9, 499,	/* ? ? */
	0x1edb, 499,	/* ? ? */
	0x1edd, 499,	/* ? ? */
	0x1edf, 499,	/* ? ? */
	0x1ee1, 499,	/* ? ? */
	0x1ee3, 499,	/* ? ? */
	0x1ee5, 499,	/* ? ? */
	0x1ee7, 499,	/* ? ? */
	0x1ee9, 499,	/* ? ? */
	0x1eeb, 499,	/* ? ? */
	0x1eed, 499,	/* ? ? */
	0x1eef, 499,	/* ? ? */
	0x1ef1, 499,	/* ? ? */
	0x1ef3, 499,	/* ? ? */
	0x1ef5, 499,	/* ? ? */
	0x1ef7, 499,	/* ? ? */
	0x1ef9, 499,	/* ? ? */
	0x1f51, 508,	/* ? ? */
	0x1f53, 508,	/* ? ? */
	0x1f55, 508,	/* ? ? */
	0x1f57, 508,	/* ? ? */
	0x1fb3, 509,	/* ? ? */
	0x1fc3, 509,	/* ? ? */
	0x1fe5, 507,	/* ? ? */
	0x1ff3, 509,	/* ? ? */
};

const ulong UTF8::nlowersgl = sizeof(lowersgl)/(sizeof(lowersgl[0]*2));

const ushort UTF8::otherrng[] = 
{
	0x00d8,	0x00f6,	/* Ø - ö */
	0x00f8,	0x01f5,	/* ø - ? */
	0x0250,	0x02a8,	/* ? - ? */
	0x038e,	0x03a1,	/* ? - ? */
	0x03a3,	0x03ce,	/* S - ? */
	0x03d0,	0x03d6,	/* ? - ? */
	0x03e2,	0x03f3,	/* ? - ? */
	0x0490,	0x04c4,	/* ? - ? */
	0x0561,	0x0587,	/* ? - ? */
	0x05d0,	0x05ea,	/* ? - ? */
	0x05f0,	0x05f2,	/* ? - ? */
	0x0621,	0x063a,	/* ? - ? */
	0x0640,	0x064a,	/* ? - ? */
	0x0671,	0x06b7,	/* ? - ? */
	0x06ba,	0x06be,	/* ? - ? */
	0x06c0,	0x06ce,	/* ? - ? */
	0x06d0,	0x06d3,	/* ? - ? */
	0x0905,	0x0939,	/* ? - ? */
	0x0958,	0x0961,	/* ? - ? */
	0x0985,	0x098c,	/* ? - ? */
	0x098f,	0x0990,	/* ? - ? */
	0x0993,	0x09a8,	/* ? - ? */
	0x09aa,	0x09b0,	/* ? - ? */
	0x09b6,	0x09b9,	/* ? - ? */
	0x09dc,	0x09dd,	/* ? - ? */
	0x09df,	0x09e1,	/* ? - ? */
	0x09f0,	0x09f1,	/* ? - ? */
	0x0a05,	0x0a0a,	/* ? - ? */
	0x0a0f,	0x0a10,	/* ? - ? */
	0x0a13,	0x0a28,	/* ? - ? */
	0x0a2a,	0x0a30,	/* ? - ? */
	0x0a32,	0x0a33,	/* ? - ? */
	0x0a35,	0x0a36,	/* ? - ? */
	0x0a38,	0x0a39,	/* ? - ? */
	0x0a59,	0x0a5c,	/* ? - ? */
	0x0a85,	0x0a8b,	/* ? - ? */
	0x0a8f,	0x0a91,	/* ? - ? */
	0x0a93,	0x0aa8,	/* ? - ? */
	0x0aaa,	0x0ab0,	/* ? - ? */
	0x0ab2,	0x0ab3,	/* ? - ? */
	0x0ab5,	0x0ab9,	/* ? - ? */
	0x0b05,	0x0b0c,	/* ? - ? */
	0x0b0f,	0x0b10,	/* ? - ? */
	0x0b13,	0x0b28,	/* ? - ? */
	0x0b2a,	0x0b30,	/* ? - ? */
	0x0b32,	0x0b33,	/* ? - ? */
	0x0b36,	0x0b39,	/* ? - ? */
	0x0b5c,	0x0b5d,	/* ? - ? */
	0x0b5f,	0x0b61,	/* ? - ? */
	0x0b85,	0x0b8a,	/* ? - ? */
	0x0b8e,	0x0b90,	/* ? - ? */
	0x0b92,	0x0b95,	/* ? - ? */
	0x0b99,	0x0b9a,	/* ? - ? */
	0x0b9e,	0x0b9f,	/* ? - ? */
	0x0ba3,	0x0ba4,	/* ? - ? */
	0x0ba8,	0x0baa,	/* ? - ? */
	0x0bae,	0x0bb5,	/* ? - ? */
	0x0bb7,	0x0bb9,	/* ? - ? */
	0x0c05,	0x0c0c,	/* ? - ? */
	0x0c0e,	0x0c10,	/* ? - ? */
	0x0c12,	0x0c28,	/* ? - ? */
	0x0c2a,	0x0c33,	/* ? - ? */
	0x0c35,	0x0c39,	/* ? - ? */
	0x0c60,	0x0c61,	/* ? - ? */
	0x0c85,	0x0c8c,	/* ? - ? */
	0x0c8e,	0x0c90,	/* ? - ? */
	0x0c92,	0x0ca8,	/* ? - ? */
	0x0caa,	0x0cb3,	/* ? - ? */
	0x0cb5,	0x0cb9,	/* ? - ? */
	0x0ce0,	0x0ce1,	/* ? - ? */
	0x0d05,	0x0d0c,	/* ? - ? */
	0x0d0e,	0x0d10,	/* ? - ? */
	0x0d12,	0x0d28,	/* ? - ? */
	0x0d2a,	0x0d39,	/* ? - ? */
	0x0d60,	0x0d61,	/* ? - ? */
	0x0e01,	0x0e30,	/* ? - ? */
	0x0e32,	0x0e33,	/* ? - ? */
	0x0e40,	0x0e46,	/* ? - ? */
	0x0e5a,	0x0e5b,	/* ? - ? */
	0x0e81,	0x0e82,	/* ? - ? */
	0x0e87,	0x0e88,	/* ? - ? */
	0x0e94,	0x0e97,	/* ? - ? */
	0x0e99,	0x0e9f,	/* ? - ? */
	0x0ea1,	0x0ea3,	/* ? - ? */
	0x0eaa,	0x0eab,	/* ? - ? */
	0x0ead,	0x0eae,	/* ? - ? */
	0x0eb2,	0x0eb3,	/* ? - ? */
	0x0ec0,	0x0ec4,	/* ? - ? */
	0x0edc,	0x0edd,	/* ? - ? */
	0x0f18,	0x0f19,	/* ? - ? */
	0x0f40,	0x0f47,	/* ? - ? */
	0x0f49,	0x0f69,	/* ? - ? */
	0x10d0,	0x10f6,	/* ? - ? */
	0x1100,	0x1159,	/* ? - ? */
	0x115f,	0x11a2,	/* ? - ? */
	0x11a8,	0x11f9,	/* ? - ? */
	0x1e00,	0x1e9b,	/* ? - ? */
	0x1f50,	0x1f57,	/* ? - ? */
	0x1f80,	0x1fb4,	/* ? - ? */
	0x1fb6,	0x1fbc,	/* ? - ? */
	0x1fc2,	0x1fc4,	/* ? - ? */
	0x1fc6,	0x1fcc,	/* ? - ? */
	0x1fd0,	0x1fd3,	/* ? - ? */
	0x1fd6,	0x1fdb,	/* ? - ? */
	0x1fe0,	0x1fec,	/* ? - ? */
	0x1ff2,	0x1ff4,	/* ? - ? */
	0x1ff6,	0x1ffc,	/* ? - ? */
	0x210a,	0x2113,	/* g - l */
	0x2115,	0x211d,	/* N - R */
	0x2120,	0x2122,	/* ? - ™ */
	0x212a,	0x2131,	/* K - F */
	0x2133,	0x2138,	/* M - ? */
	0x3041,	0x3094,	/* ? - ? */
	0x30a1,	0x30fa,	/* ? - ? */
	0x3105,	0x312c,	/* ? - ? */
	0x3131,	0x318e,	/* ? - ? */
	0x3192,	0x319f,	/* ? - ? */
	0x3260,	0x327b,	/* ? - ? */
	0x328a,	0x32b0,	/* ? - ? */
	0x32d0,	0x32fe,	/* ? - ? */
	0x3300,	0x3357,	/* ? - ? */
	0x3371,	0x3376,	/* ? - ? */
	0x337b,	0x3394,	/* ? - ? */
	0x3399,	0x339e,	/* ? - ? */
	0x33a9,	0x33ad,	/* ? - ? */
	0x33b0,	0x33c1,	/* ? - ? */
	0x33c3,	0x33c5,	/* ? - ? */
	0x33c7,	0x33d7,	/* ? - ? */
	0x33d9,	0x33dd,	/* ? - ? */
	0x4e00,	0x9fff,	/* ? - ? */
	0xac00,	0xd7a3,	/* ? - ? */
	0xf900,	0xfb06,	/* ? - ? */
	0xfb13,	0xfb17,	/* ? - ? */
	0xfb1f,	0xfb28,	/* ? - ? */
	0xfb2a,	0xfb36,	/* ? - ? */
	0xfb38,	0xfb3c,	/* ? - ? */
	0xfb40,	0xfb41,	/* ? - ? */
	0xfb43,	0xfb44,	/* ? - ? */
	0xfb46,	0xfbb1,	/* ? - ? */
	0xfbd3,	0xfd3d,	/* ? - ? */
	0xfd50,	0xfd8f,	/* ? - ? */
	0xfd92,	0xfdc7,	/* ? - ? */
	0xfdf0,	0xfdf9,	/* ? - ? */
	0xfe70,	0xfe72,	/* ? - ? */
	0xfe76,	0xfefc,	/* ? - ? */
	0xff66,	0xff6f,	/* ? - ? */
	0xff71,	0xff9d,	/* ? - ? */
	0xffa0,	0xffbe,	/* ? - ? */
	0xffc2,	0xffc7,	/* ? - ? */
	0xffca,	0xffcf,	/* ? - ? */
	0xffd2,	0xffd7,	/* ? - ? */
	0xffda,	0xffdc,	/* ? - ? */
};

const ulong UTF8::notherrng = sizeof(otherrng)/(sizeof(otherrng[0]*2));

const ushort UTF8::othersgl[] = 
{
	0x00aa,	/* ª */
	0x00b5,	/* µ */
	0x00ba,	/* º */
	0x03da,	/* ? */
	0x03dc,	/* ? */
	0x03de,	/* ? */
	0x03e0,	/* ? */
	0x06d5,	/* ? */
	0x09b2,	/* ? */
	0x0a5e,	/* ? */
	0x0a8d,	/* ? */
	0x0ae0,	/* ? */
	0x0b9c,	/* ? */
	0x0cde,	/* ? */
	0x0e4f,	/* ? */
	0x0e84,	/* ? */
	0x0e8a,	/* ? */
	0x0e8d,	/* ? */
	0x0ea5,	/* ? */
	0x0ea7,	/* ? */
	0x0eb0,	/* ? */
	0x0ebd,	/* ? */
	0x1fbe,	/* ? */
	0x207f,	/* n */
	0x20a8,	/* ? */
	0x2102,	/* C */
	0x2107,	/* E */
	0x2124,	/* Z */
	0x2126,	/* ? */
	0x2128,	/* Z */
	0xfb3e,	/* ? */
	0xfe74,	/* ? */
};

const ulong UTF8::nothersgl = sizeof(othersgl)/sizeof(othersgl[0]);

const ushort UTF8::spacerng[] =
{
	0x0009,	0x000a,	/* tab and newline */
	0x0020,	0x0020,	/* space */
	0x00a0,	0x00a0,	/*   */
	0x2000,	0x200b,	/*   - ? */
	0x2028,	0x2029,	/* -  */
	0x3000,	0x3000,	/*   */
	0xfeff,	0xfeff,	/* ? */
};

const ulong UTF8::nspacerng = sizeof(spacerng)/(sizeof(spacerng[0])*2);

char *MVStoreKernel::forceCaseUTF8(const char *str,size_t ilen,uint32_t& olen,MemAlloc *ma,char *extBuf,bool fUpper)
{
	const byte *in=(byte*)str,*end=in+(extBuf==NULL||ilen<olen?ilen:olen); byte ch;
	const byte mch=fUpper?'a':'A',nch='Z'-'A';
	while (in<end && byte((ch=*in)-mch)>nch && ch<0x80) in++;
	size_t l=in-(byte*)str; ulong wch; bool fAlc=false;
	if (extBuf==NULL) {
		if (l>=ilen && ma==NULL) return (char*)str;
		if ((fAlc=true,extBuf=(char*)ma->malloc(olen=uint32_t(ilen)))==NULL) return NULL;
	}
	if (l>0) memcpy(extBuf,str,l);
	while (in<end) {
		if (byte((ch=*in++)-mch)<=nch) extBuf[l++]=ch^0x20;
		else if (ch<0x80 || (wch=UTF8::decode(ch,in,ilen-(in-(byte*)str)))==~0u) extBuf[l++]=ch;
		else {
			int lch=UTF8::ulen(wch=fUpper?UTF8::towupper(wchar_t(wch)):UTF8::towlower(wchar_t(wch)));
			if (l+lch>olen && (!fAlc || (extBuf=(char*)ma->realloc(extBuf,olen+=(olen<12?6:olen/2)))==NULL)) break;
			l+=UTF8::encode((byte*)extBuf+l,wch);
		}
	}
	olen=uint32_t(l); return extBuf;
}

#define TN_NEG	0x0001
#define	TN_DOT	0x0002
#define	TN_EXP	0x0004
#define	TN_NEXP	0x0008
#define	TN_FLT	0x0010

RC MVStoreKernel::strToNum(const char *str,size_t lstr,Value& res,const char **pend,bool fInt)
{
	try {
		ulong flags=0; int exp=0,dpos=0; byte ch; const char *const end=str+lstr; Units units=Un_NDIM;
		for (;;++str) if (str>=end) return RC_SYNTAX; else if (!isspace((byte)*str)) break;	// iswspace??? -> skipSpace()
		if (*str=='+') ++str; else if (*str=='-') flags|=TN_NEG,++str;
		if (str>=end) return RC_SYNTAX;
		if (!isD(*str)) {const byte *bstr=(byte*)str+1; if (UTF8::wdigit(UTF8::decode(*str,bstr,end-str-1))==-1) return RC_SYNTAX;}
		res.type=VT_INT; res.flags=0; res.eid=STORE_COLLECTION_ID; res.i=0; if (pend!=NULL) *pend=end;
		while (str<end) switch (ch=*str++) {
		default:
			if (isD(ch)) ch-='0';
			else {
				const byte *bstr=(byte*)str; int i=UTF8::wdigit(UTF8::decode(ch,bstr,end-str));
				if (i!=-1) {str=(char*)bstr; ch=(byte)i;}
				else {
					if (!isalpha(ch)) --str;
					else {
						const char *beg=str-1; while (str<end && isalnum(*(byte*)str)) str++;
						if (fInt || (units=getUnits(beg,str-beg))==Un_NDIM) {
							if (beg+1!=str) return RC_SYNTAX;
							if (ch=='f' && !fInt) flags|=TN_FLT;
							else if (ch!='u' && ch!='U' || (flags&(TN_EXP|TN_DOT))!=0) return RC_SYNTAX;
							else switch (res.type) {
							case VT_INT: if ((flags&TN_NEG)!=0) return RC_SYNTAX; 
							case VT_UINT: if (ch=='U') {res.ui64=res.ui; res.type=VT_UINT64;} else res.type=VT_UINT; break;
							case VT_INT64: if ((flags&TN_NEG)!=0) return RC_SYNTAX; res.type=VT_UINT64; break;
							case VT_UINT64: break;
							default: return RC_INVPARAM;
							}
						} else switch (res.type) {
						default: return RC_INTERNAL;
						case VT_DOUBLE: break;
						case VT_INT: res.d=res.i; res.type=VT_DOUBLE; break;
						case VT_UINT: res.d=res.ui; res.type=VT_DOUBLE; break;
						case VT_INT64: res.d=double(res.i64); res.type=VT_DOUBLE; break;
						case VT_UINT64: res.d=double(res.ui64); res.type=VT_DOUBLE; break;
						}
					}
					if (pend!=NULL) {*pend=str; str=end;} else while (str<end) if (!isspace((byte)*str++)) return RC_SYNTAX;	// iswspace ???
					continue;
				}
			}
			if ((flags&TN_EXP)!=0) {
				if (TN_CHECK(exp,ch,DBL_MAX_10_EXP)) exp=exp*10+ch; else return RC_INVPARAM;
			} else switch (res.type) {
			case VT_INT:
				if (TN_CHECK(res.i,ch,INT_MAX)) {res.i=res.i*10+ch; break;}
				if ((flags&TN_NEG)!=0) {res.i64=res.i; res.type=VT_INT64; goto int64;}
				res.ui=res.i; res.type=VT_UINT;
			case VT_UINT:
				if (TN_CHECK(res.ui,ch,UINT_MAX)) {res.ui=res.ui*10+ch; break;}
				res.i64=res.ui; res.type=VT_INT64;
			case VT_INT64:
			int64:
				if (TN_CHECK(res.i64,ch,LLONG_MAX)) {res.i64=res.i64*10+ch; break;}
				if ((flags&TN_NEG)!=0) {res.d=double(res.i64); res.type=VT_DOUBLE; res.qval.units=Un_NDIM; goto dbl;}
				res.ui64=res.i64; res.type=VT_UINT64;
			case VT_UINT64:
				if (TN_CHECK(res.ui64,ch,ULLONG_MAX)) {res.ui64=res.ui64*10+ch; break;}
				if (fInt) return RC_TOOBIG;
				res.d=double(res.ui64); res.type=VT_DOUBLE; res.qval.units=Un_NDIM;
			case VT_DOUBLE:
			dbl:
				if ((flags&TN_FLT)!=0) return RC_SYNTAX;
				if ((flags&TN_DOT)!=0) {
					//if (dpos>=-???) 
						res.d+=ch*pow(10.,--dpos);	// underflow ???
				} else res.d=res.d*10.+ch;	// overflow ???
				break;
			}
			break;
		case 'e': case 'E':
			if (fInt||str>=end||(flags&(TN_FLT|TN_EXP))!=0) return RC_SYNTAX;
			if (*str=='+') ++str; else if (*str=='-') flags|=TN_NEXP,++str;
			if (str>=end||!isD(*str)) return RC_SYNTAX;
			flags|=TN_EXP; if ((flags&TN_DOT)!=0) break;
		case '.':
			if (fInt) {if (pend!=NULL) *pend=str-1; str=end; break;}
			if ((flags&TN_DOT)!=0) return RC_SYNTAX;
			dpos=0; flags|=TN_DOT;
			switch (res.type) {
			default: return RC_INTERNAL;
			case VT_DOUBLE: case VT_FLOAT: break;
			case VT_INT: res.d=res.i; break;
			case VT_UINT: res.d=res.ui; break;
			case VT_INT64: res.d=double(res.i64); break;
			case VT_UINT64: res.d=double(res.ui64); break;
			}
			res.type=VT_DOUBLE; res.qval.units=Un_NDIM; break;
		}
		if ((flags&TN_EXP)!=0) {
			assert(res.type==VT_DOUBLE);
			if ((flags&TN_NEXP)!=0) exp=-exp;
			res.d*=pow(10.,exp);		// overflow, underflow ???
		}
		if ((flags&TN_NEG)!=0) switch (res.type) {
		case VT_INT: res.i=-res.i; break;
		case VT_UINT: res.type=VT_INT64; res.i64=res.ui;
		case VT_INT64: res.i64=-res.i64; break;
		case VT_UINT64: res.ui64=-int64_t(res.ui64); break;
		case VT_DOUBLE: res.d=-res.d; break;
		default: assert(0);
		}
		if ((flags&TN_FLT)!=0 && res.type!=VT_FLOAT) try {
			switch (res.type) {
			default: return RC_INTERNAL;
			case VT_DOUBLE: res.f=float(res.d); break;
			case VT_INT: res.f=float(res.i); break;
			case VT_UINT: res.f=float(res.ui); break;
			case VT_INT64: res.f=float(res.i64); break;
			case VT_UINT64: res.f=float(res.ui64); break;
			}
			res.type=VT_FLOAT;
		} catch (...) {return RC_TOOBIG;}
		if (res.type==VT_DOUBLE || res.type==VT_FLOAT) res.qval.units=units;
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {return RC_SYNTAX;}		// ???
}

RC MVStoreKernel::strToTimestamp(const char *p,size_t l,TIMESTAMP& res)
{
	DateTime dt; memset(&dt,0,sizeof(dt));
	if (l<10 || !isD(p[0]) || !isD(p[1]) || !isD(p[2]) || !isD(p[3]) || p[4]!='-'
		|| !isD(p[5]) || !isD(p[6]) || p[7]!='-' || !isD(p[8]) || !isD(p[9])) return RC_SYNTAX;
	dt.year=(((p[0]-'0')*10+p[1]-'0')*10+p[2]-'0')*10+p[3]-'0';
	dt.month=(p[5]-'0')*10+p[6]-'0';
	dt.day=(p[8]-'0')*10+p[9]-'0';
	if (l>10) {
		const char *const end=p+l;
		for (p+=10; p<end && isspace((byte)*p); p++);	// iswspace???
		if (p<end) {
			if (end-p<8 || !isD(p[0]) || !isD(p[1]) || p[2]!=':' || !isD(p[3]) || !isD(p[4])
				|| p[5]!=':' || !isD(p[6]) || !isD(p[7])) return RC_SYNTAX;
			dt.hour=(p[0]-'0')*10+p[1]-'0';
			dt.minute=(p[3]-'0')*10+p[4]-'0';
			dt.second=(p[6]-'0')*10+p[7]-'0';
			if ((p+=8)<end) {
				if (end-p<2 || end-p>7 || p[0]!='.' || !isD(p[1])) return RC_SYNTAX;
				for (dt.microseconds=p[1]-'0',p+=2; p<end && isD(*p); p++) dt.microseconds=dt.microseconds*10+p[0]-'0';
				while (p<end && isspace((byte)*p)) p++;	// iswspace???
				if (p<end) return RC_SYNTAX;
			}
		}
	}
	return convDateTime(NULL,dt,res);
}

RC MVStoreKernel::strToInterval(const char *p,size_t l,int64_t& res)
{
	if (p==NULL || l==0) return RC_INVPARAM;
	const char *const end=p+l; int64_t v; int i; bool fNeg=false;
	if (*p=='-') {if (++p>=end) return RC_SYNTAX; fNeg=true;}
	if (!isD(*p)) return RC_SYNTAX;
	for (v=*p-'0'; ++p<end && isD(*p);)
		{byte ch=*p-'0'; if (TN_CHECK(v,ch,LLONG_MAX/3600000000LL)) v=v*10+ch; else return RC_INVPARAM;}
	v*=3600000000LL;
	if (p<end) {
		if (p+2>=end || p[0]!=':' || !isD(p[1]) || !isD(p[2])) return RC_SYNTAX;
		i=(p[1]-'0')*10+p[2]-'0'; if (i<60) v+=int64_t(i)*60000000,p+=3; else return RC_INVPARAM;
		if (p<end) {
			if (p+2>=end || p[0]!=':' || !isD(p[1]) || !isD(p[2])) return RC_SYNTAX;
			i=(p[1]-'0')*10+p[2]-'0'; if (i<60) v+=i*1000000,p+=3; else return RC_INVPARAM;
			if (p<end) {
				if (p+1>=end || p[0]!='.' || !isD(p[1])) return RC_SYNTAX;
				uint32_t mks=p[1]-'0',mul=100000; ++p;
				while (++p<end && isD(*p)) if (mul!=1) mks=mks*10+*p-'0',mul/=10;
				while (p<end && isspace((byte)*p)) p++;		// iswspace???
				if (p<end) return RC_SYNTAX;
				v+=mks*mul;
			}
		}
	}
	res=fNeg?-v:v; return RC_OK;
}

RC MVStoreKernel::convDateTime(Session *ses,TIMESTAMP ts,char *buf,int& l,bool fUTC)
{
	RC rc; DateTime dt;
	if ((rc=convDateTime(ses,ts,dt,fUTC))!=RC_OK) return rc;
	l=sprintf(buf,DATETIMEFORMAT,dt.year,dt.month,dt.day,dt.hour,dt.minute,dt.second);
	if (dt.microseconds!=0) l+=sprintf(buf+l,FRACTIONALFORMAT,dt.microseconds);
	return RC_OK;
}

RC MVStoreKernel::convInterval(int64_t it,char *buf,int& l)
{
	const bool fNeg=it<0; if (fNeg) it=-it;
	l=sprintf(buf,"%s"INTERVALFORMAT,fNeg?"-":"",(int)(it/3600000000LL),(int)(it/60000000)%60,(int)(it/1000000)%60);
	if (it%1000000!=0) l+=sprintf(buf+l,FRACTIONALFORMAT,(int)(it%1000000));
	return RC_OK;
}

RC MVStoreKernel::convDateTime(Session *ses,TIMESTAMP dt,DateTime& dts,bool fUTC)
{
	dt-=TS_DELTA; if (!fUTC && ses!=NULL) dt-=ses->tzShift;
	dts.microseconds	= (uint32_t)(dt%1000000ul);
#ifdef WIN32
	ULARGE_INTEGER ul; FILETIME ft; SYSTEMTIME st;
	ul.QuadPart=dt*10; ft.dwHighDateTime=ul.HighPart; ft.dwLowDateTime=ul.LowPart;
	if (FileTimeToSystemTime(&ft,&st)!=TRUE) return convCode(GetLastError());
	dts.second			= st.wSecond;
	dts.minute			= st.wMinute;
	dts.hour			= st.wHour;
	dts.day				= st.wDay;
	dts.dayOfWeek		= st.wDayOfWeek;
	dts.month			= st.wMonth;
	dts.year			= st.wYear;
#else
	tm tt; time_t t=(time_t)(dt/1000000ul);
	if (gmtime_r(&t,&tt)==NULL) return RC_INVPARAM;
	dts.second			= tt.tm_sec;
	dts.minute			= tt.tm_min;
	dts.hour			= tt.tm_hour;
	dts.day				= tt.tm_mday;
	dts.dayOfWeek		= tt.tm_wday;
	dts.month			= tt.tm_mon+1;
	dts.year			= tt.tm_year+1900;
#endif
	return RC_OK;
}

RC MVStoreKernel::convDateTime(Session *ses,const DateTime& dts,uint64_t& dt,bool fUTC)
{
#ifdef WIN32
	ULARGE_INTEGER ul; FILETIME ft; SYSTEMTIME st;
	st.wMilliseconds	= WORD(dts.microseconds/1000%1000);
	st.wSecond			= dts.second;
	st.wMinute			= dts.minute;
	st.wHour			= dts.hour;
	st.wDay				= dts.day;
	st.wDayOfWeek		= dts.dayOfWeek;
	st.wMonth			= dts.month;
	st.wYear			= dts.year;
	if (SystemTimeToFileTime(&st,&ft)!=TRUE) return convCode(GetLastError());
	ul.HighPart=ft.dwHighDateTime; ul.LowPart=ft.dwLowDateTime; 
	dt=ul.QuadPart/10+dts.microseconds%1000;
#else
	tm tt; memset(&tt,0,sizeof(tt));
	tt.tm_sec			= dts.second;
	tt.tm_min			= dts.minute;
	tt.tm_hour			= dts.hour;
	tt.tm_mday			= dts.day;
	tt.tm_wday			= dts.dayOfWeek;
	tt.tm_mon			= dts.month-1;
	tt.tm_year			= dts.year-1900;
	time_t t=timegm(&tt); if (t==(time_t)(-1)) return RC_INVPARAM;
	dt=uint64_t(t)*1000000ul+dts.microseconds;
#endif
	dt+=TS_DELTA; if (!fUTC && ses!=NULL) dt+=ses->tzShift;
	return RC_OK;
}

RC MVStoreKernel::getDTPart(TIMESTAMP dt,unsigned& res,int part)
{
	dt-=TS_DELTA; if (part==0) {res=dt%1000000; return RC_OK;}
#ifdef WIN32
	ULARGE_INTEGER ul; FILETIME ft; SYSTEMTIME st;
	ul.QuadPart=dt*10; ft.dwHighDateTime=ul.HighPart; ft.dwLowDateTime=ul.LowPart;
	if (FileTimeToSystemTime(&ft,&st)!=TRUE) return convCode(GetLastError());
	switch (part) {
	case 1: res=st.wSecond; break;
	case 2: res=st.wMinute; break;
	case 3: res=st.wHour; break;
	case 4: res=st.wDay; break;
	case 5: res=st.wDayOfWeek; break;
	case 6: res=st.wMonth; break;
	case 7: res=st.wYear; break;
	default: return RC_INTERNAL;
	}
#else
	tm tt; time_t t=(time_t)(dt/1000000ul);
	if (gmtime_r(&t,&tt)==NULL) return RC_INVPARAM;
	switch (part) {
	case 1: res=tt.tm_sec; break;
	case 2: res=tt.tm_min; break;
	case 3: res=tt.tm_hour; break;
	case 4: res=tt.tm_mday; break;
	case 5: res=tt.tm_wday; break;
	case 6: res=tt.tm_mon+1; break;
	case 7: res=tt.tm_year+1900; break;
	default: return RC_INTERNAL;
	}
#endif
	return RC_OK;
}

//----------------------------------------------------------------------------------------------------------------------------

RC PageSet::add(PageID from,PageID to)
{
	PageSetChunk *pc=chunks,*end=&chunks[nChunks]; ulong npg=to-from+1; assert(to>=from);
	if (pc!=NULL && nChunks>0) {
		if (pc[0].from>=from && end[-1].to<=to) {
			for (ulong i=0; i<nChunks; i++) if (chunks[i].bmp!=NULL) ma->free(chunks[i].bmp);
			nChunks=0; nPages=0;
		} else {
			for (ulong n=nChunks; n>0;) {
				ulong k=n>>1; PageSetChunk &ch=pc[k];
				if (ch.from>to) n=k; else if (ch.to<from) {pc=&ch+1; n-=k+1;} else {pc=&ch; break;}
			}
			if (pc<end && pc->from<=from && pc->to>=to) {
				if (pc->bmp!=NULL) {
					if (pc->from==from && pc->to==to) {
						assert(npg>=pc->npg); ma->free(pc->bmp); pc->bmp=NULL; nPages+=npg-pc->npg; pc->npg=npg;
					} else for (ulong idx0=from/SZ_BMP-pc->from/SZ_BMP,end=to/SZ_BMP-pc->from/SZ_BMP,idx=idx0; idx<=end; idx++) {
						ulong mask=idx==end?(1<<(to%SZ_BMP+1))-1:~0u; if (idx==idx0) mask&=~((1<<from%SZ_BMP)-1);
						ulong d=MVStoreKernel::pop((pc->bmp[idx]^mask)&mask); pc->bmp[idx]|=mask; pc->npg+=d; nPages+=d;
					}
				}
#ifdef _DEBUG
				test();
#endif
				return RC_OK;
			}
			PageSetChunk *prev=pc,*next=pc;
			while (--prev>=chunks && prev->from>=from);
			while (next<end && next->to<=to) next++;
			assert(prev>=chunks||next<end);
			if ((pc=prev+1)<next) {
				for (PageSetChunk *p=pc; p<next; p++) {assert(nPages>=p->npg); nPages-=p->npg; if (p->bmp!=NULL) ma->free(p->bmp);}
				if (next<end) memmove(pc,next,(byte*)end-(byte*)next); nChunks-=ulong(next-pc); next=pc;
			}
			assert(prev+1==next);
			if (prev>=chunks) {
				if (prev->to+1>=from) {
					assert(prev->to<to);
					if (prev->bmp!=NULL) {
						assert(0 && "PageSet::add 1");
						if (from<prev->to) {
							// merge overlapping
						}
						if ((to-prev->to)/8<=sizeof(PageSetChunk)) {
							// extend
						} else {
							// from=prev->to+1; npg=to-from+1; goto add_chunk;
						}
					}
					ulong d=to-prev->to; prev->npg+=d; nPages+=d; prev->to=to;
					if (next<end && prev->to+1>=next->from) {
						if (prev->bmp==NULL) {
							if (next->bmp==NULL) {
								ulong overlap=prev->to>=next->from?prev->to-next->from+1:0;
								prev->npg+=next->npg-overlap; nPages-=overlap; prev->to=next->to;
								if (next+1<end) memmove(next,next+1,(byte*)end-(byte*)(next+1));
								nChunks--;
							} else {
								assert(0 && "PageSet::add 2");
								// ???
							}
						} else if (next->bmp==NULL) {
							assert(0 && "PageSet::add 3");
							// ???
						} else {
							assert(0 && "PageSet::add 4");
							// ???
						}
					}
#ifdef _DEBUG
					test();
#endif
					return RC_OK;
				}
				if (next>=end || to+1<next->from) {
					// try to extend prev
				}
			}
			if (next<end) {
				if (to+1>=next->from) {
					assert(from<next->from);
					if (next->bmp!=NULL) {
						assert(0 && "PageSet::add 5");
						if (to>next->to) {
							// merge overlapping
						}
						if ((next->from-from)/8<=sizeof(PageSetChunk)) {
							// extend
						} else {
							// to=next->from-1; npg=to-from+1; goto add_chunk;
						}
					}
					ulong d=next->from-from; next->npg+=d; nPages+=d; next->from=from;
#ifdef _DEBUG
					test();
#endif
					return RC_OK;
				}
				// try extend next
			}
		}
	}
	if (nChunks>=xChunks) {
		ptrdiff_t sht=pc-chunks;
		chunks=(PageSetChunk*)(chunks==NULL?ma->malloc((xChunks=6)*sizeof(PageSetChunk)):ma->realloc(chunks,(xChunks*=2)*sizeof(PageSetChunk)));
		if (chunks!=NULL) pc=chunks+sht,end=chunks+nChunks; else {nChunks=xChunks=nPages=0; return RC_NORESOURCES;}
	}
	if (pc<end) memmove(pc+1,pc,(byte*)end-(byte*)pc);
	pc->from=from; pc->to=to; nPages+=pc->npg=npg; pc->bmp=NULL; nChunks++;
#ifdef _DEBUG
	test();
#endif
	return RC_OK;
}

RC PageSet::add(const PageSet& rhs)
{
	RC rc=RC_OK; assert(chunks!=NULL && rhs.chunks!=NULL);
	for (unsigned i=0; i<rhs.nChunks; i++) {
		const PageSetChunk& rp=rhs.chunks[i];
		if (rp.bmp!=NULL) {
			assert(0&&"not implemented");
			//...
		} else if ((rc=add(rp.from,rp.to))!=RC_OK) break;
	}
	return rc;
}

RC PageSet::operator-=(PageID pid)
{
	for (ulong n=nChunks,base=0; n>0;) {
		ulong k=n>>1; PageSetChunk &ch=chunks[base+k];
		if (ch.from>pid) n=k; else if (ch.to<pid) {base+=k+1; n-=k+1;}
		else {
			if (ch.to==ch.from) {
				if (ch.bmp!=NULL) ma->free(ch.bmp);
				if (base+k<--nChunks) memmove(&ch,&ch+1,(nChunks-base-k)*sizeof(PageSetChunk));
			} else if (ch.bmp==NULL) {
				if (ch.from==pid) {ch.from++; ch.npg--;} 
				else if (ch.to==pid) {ch.to--; ch.npg--;}
				else //if (ch.to/SZ_BMP-ch.from/SZ_BMP+1<sizeof(PageSetChunk)/sizeof(uint32_t)) {
					//if ((ch.bmp=(uint32_t*)malloc((ch.to/SZ_BMP-ch.from/SZ_BMP+1)*sizeof(uint32_t),alloc))==NULL) return RC_NORESOURCES;
					//for (ulong idx=0,end=ch.to/SZ_BMP-ch.from/SZ_BMP,idx0=pid/SZ_BMP-ch.from/SZ_BMP,u; idx<=end; idx++)
					//	{u=idx==end?(1<<(ch.to%SZ_BMP+1))-1:~0u; if (idx==0) u&=~((1<<ch.from%SZ_BMP)-1); if (idx==idx0) u&=~(1<<pid%SZ_BMP); ch.bmp[idx]=u;}
				//} else 
				{
					PageSetChunk *pc=&ch+1;
					if (nChunks>=xChunks) {
						ptrdiff_t sht=pc-chunks;
						chunks=(PageSetChunk*)(chunks==NULL?ma->malloc((xChunks=6)*sizeof(PageSetChunk)):ma->realloc(chunks,(xChunks*=2)*sizeof(PageSetChunk)));
						if (chunks!=NULL) pc=chunks+sht; else {nChunks=xChunks=nPages=0; return RC_NORESOURCES;}
					}
					if (pc<&chunks[nChunks]) memmove(pc+1,pc,(byte*)&chunks[nChunks]-(byte*)pc);
					pc->from=pid+1; pc->to=pc[-1].to; pc->npg=pc->to-pid; pc->bmp=NULL;
					pc[-1].to=pid-1; pc[-1].npg=pid-pc[-1].from; ++nChunks;
				}
			} else if (ch.to==pid || ch.from==pid) {
				assert(ch.npg>1);
				if (ch.npg==2) {ma->free(ch.bmp); ch.bmp=NULL; if (ch.to==pid) ch.to=ch.from; else ch.from=ch.to;}
				else if (ch.to==pid) {
					ulong idx=pid/SZ_BMP-ch.from/SZ_BMP,end=idx;
					for (ch.bmp[idx]&=~(1<<pid%SZ_BMP); ch.bmp[idx]==0; idx--) assert(idx>0);
					ch.to=ch.from-ch.from%SZ_BMP+idx*SZ_BMP+(SZ_BMP-1-nlz(ch.bmp[idx]));
					if (end-idx>10) ch.bmp=(uint32_t*)ma->realloc(ch.bmp,(idx+1)*sizeof(uint32_t));
				} else {
					ulong idx=0,end=ch.to/SZ_BMP-ch.from/SZ_BMP;
					for (ch.bmp[0]&=~(1<<pid%SZ_BMP); ch.bmp[idx]==0; idx++) assert(idx+1<=end);
					ch.from=ch.from-ch.from%SZ_BMP+idx*SZ_BMP+ntz(ch.bmp[idx]);
					if (idx>0) memmove(&ch.bmp[0],&ch.bmp[idx],(end-idx+1)*sizeof(uint32_t));
					if (idx>10) ch.bmp=(uint32_t*)ma->realloc(ch.bmp,(end-idx+1)*sizeof(uint32_t));
				}
			} else if ((ch.bmp[pid/SZ_BMP-ch.from/SZ_BMP]&1<<pid%SZ_BMP)!=0) {
				ch.bmp[pid/SZ_BMP-ch.from/SZ_BMP]&=~(1<<pid%SZ_BMP); ch.npg--; assert(ch.npg>=2);
			} else break;
			--nPages;
#ifdef _DEBUG
			test();
#endif
			return RC_OK;
		}
	}
	return RC_FALSE;
}

RC PageSet::operator-=(const PageSet& rhs)
{
	//...
	return RC_INTERNAL;
}

RC PageSet::remove(const PageID *pages,ulong npg)
{
	//...
	return RC_INTERNAL;
}

PageID PageSet::pop()
{
	PageID ret=INVALID_PAGEID;
	if (chunks!=NULL && nChunks!=0 && nPages!=0) {
		PageSetChunk *pc=&chunks[nChunks-1]; ret=pc->to; pc->npg--; nPages--;
		if (pc->bmp!=NULL) {
			if (pc->npg<=2) {ma->free(pc->bmp); pc->bmp=NULL; if (pc->npg==1) --nChunks; else pc->to=pc->from;}
			else {
				ulong idx=pc->to/SZ_BMP-pc->from/SZ_BMP,idx0=idx; pc->bmp[idx]&=~(1<<pc->to%SZ_BMP);
				while (pc->bmp[idx]==0) {assert(idx>0); idx--;}
				pc->to=pc->from-pc->from%SZ_BMP+idx*SZ_BMP+(SZ_BMP-1-nlz(pc->bmp[idx]));
				if (idx0-idx>10) pc->bmp=(uint32_t*)ma->realloc(pc->bmp,(idx+1)*sizeof(uint32_t));
			}
		} else if (pc->from==pc->to) --nChunks; else --pc->to;
#ifdef _DEBUG
		test();
#endif
	}
	return ret;
}

void PageSet::test() const
{
	ulong cnt=0;
	assert(nChunks<=xChunks); assert((nPages==0)==(nChunks==0)); 
	if (chunks!=NULL && nChunks>0) for (ulong i=0; i<nChunks; i++) {
		const PageSetChunk &ch=chunks[i]; cnt+=ch.npg;
		assert(ch.to>=ch.from);
		if (ch.bmp!=NULL) {
			ulong cnt2=0; assert(ch.to>ch.from);
			for (ulong i=0,j=ch.to/SZ_BMP-ch.from/SZ_BMP; i<=j; i++) cnt2+=MVStoreKernel::pop(ch.bmp[i]);
			assert(cnt2==ch.npg);
		} else assert(ch.npg==ch.to-ch.from+1);
		if (i!=0) assert(chunks[i-1].to<ch.from);
	}
	assert(cnt==nPages);
}
