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
 * PIN ID cache
 * implemented as in-memory SList
 */
#ifndef _IDXCACHE_H_
#define _IDXCACHE_H_

#include "pinex.h"

using namespace AfyDB;

namespace AfyKernel
{

#define	DEFAULT_LIMIT	(~0u)	//0x1000

class PIDCmp
{
public:
	static SListOp compare(const PID& left,PID& right,ulong) 
		{return left==right?SLO_NOOP:left.ident<right.ident||left.ident==right.ident&&left.pid<right.pid?SLO_LT:SLO_GT;}
};

class PIDStore : public SubAlloc
{
	const	ulong		limit;
	SList<PID,PIDCmp>	cache;
	ulong				count;
	void				*extFile;
public:
	PIDStore(Session *ses,ulong lim=DEFAULT_LIMIT);
	~PIDStore();
	void	operator	delete(void *p) {if (p!=NULL) ((PIDStore*)p)->parent->free(p);}
	bool	operator[](PINEx&) const;
	RC		operator+=(PINEx&);
	RC		operator-=(PINEx&);
	void	clear();
};

}

#endif
