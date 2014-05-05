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

Written by Mark Venguerov 2004-2014

**************************************************************************************/

/**
 * PIN ID cache
 * implemented as in-memory SList
 */
#ifndef _IDXCACHE_H_
#define _IDXCACHE_H_

#include "pinex.h"

using namespace Afy;

namespace AfyKernel
{

#define	DEFAULT_LIMIT	(~0u)	//0x1000

class PIDCmp
{
public:
	static SListOp compare(const PID& left,const PID& right,unsigned,MemAlloc&)
		{return left==right?SLO_NOOP:left.ident<right.ident||left.ident==right.ident&&left.pid<right.pid?SLO_LT:SLO_GT;}
};

class PIDStore : public StackAlloc
{
	const	unsigned		limit;
	SList<PID,PIDCmp>	cache;
	unsigned				count;
	void				*extFile;
public:
	PIDStore(Session *ses,unsigned lim=DEFAULT_LIMIT);
	~PIDStore();
	void	operator	delete(void *p) {if (p!=NULL) ((PIDStore*)p)->parent->free(p);}
	bool	operator[](PINx&) const;
	RC		operator+=(PINx&);
	RC		operator-=(PINx&);
	void	clear();
};

}

#endif
