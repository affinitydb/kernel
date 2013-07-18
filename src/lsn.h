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
 * Logical Sequential Number descriptor
 * (address of a log record in log)
 */
#ifndef _LSN_H_
#define _LSN_H_

#include "types.h"

namespace AfyKernel
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
