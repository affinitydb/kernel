/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _LSN_H_
#define _LSN_H_

#include "types.h"

namespace MVStoreKernel
{

struct LSN
{
	uint64_t	lsn;

	LSN() {}
	LSN(uint64_t l) {lsn = l;}
	LSN(const LSN& rhs) {lsn = rhs.lsn;}
	LSN&	operator=(const LSN& rhs) {lsn=rhs.lsn; return *this;}
	bool	operator==(const LSN& rhs) const {return lsn==rhs.lsn;}
	bool	operator!=(const LSN& rhs) const {return lsn!=rhs.lsn;}
	bool	operator< (const LSN& rhs) const {return lsn< rhs.lsn;}
	bool	operator> (const LSN& rhs) const {return lsn> rhs.lsn;}
	bool	operator<=(const LSN& rhs) const {return lsn<=rhs.lsn;}
	bool	operator>=(const LSN& rhs) const {return lsn>=rhs.lsn;}
	LSN		operator+ (uint64_t shift) const {return LSN(lsn+shift);}
	LSN&	operator+=(uint64_t shift) {lsn+=shift; return *this;}
	LSN		operator- (uint64_t shift) const {return LSN(lsn-shift);}
	LSN&	operator-=(uint64_t shift) {lsn-=shift; return *this;}
	off64_t	operator- (const LSN& rhs) const {return lsn - rhs.lsn;}
	bool	isNull() const {return lsn==0;}
	LSN&	align() {lsn = lsn + sizeof(LSN) - 1 & ~(uint64_t(sizeof(LSN)) - 1); return *this;}
};

};

#endif
