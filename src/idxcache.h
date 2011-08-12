/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _IDXCACHE_H_
#define _IDXCACHE_H_

#include "pinex.h"

using namespace MVStore;

namespace MVStoreKernel
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
