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

#ifndef _MODINFO_H_
#define _MODINFO_H_

#include "pgheap.h"

using namespace Afy;

namespace AfyKernel
{

/**
 * passed to FT indexer
 */
struct ChangeInfo
{
	PID				id;
	PID				docID;
	const Value		*oldV;
	const Value		*newV;
	PropertyID		propID;
	ElementID		eid;
};

/**
 * bit flags used in modifyPIN()
 */
#define	PM_PROCESSED	0x00000001
#define PM_COLLECTION	0x00000002
#define	PM_FTINDEXABLE	0x00000004
#define	PM_EXPAND		0x00000008
#define	PM_MODCOLL		0x00000010
#define	PM_SSV			0x00000020
#define	PM_ESTREAM		0x00000040
#define	PM_BIGC			0x00000080
#define	PM_FORCESSV		0x00000100
#define	PM_NEWCOLL		0x00000200
#define	PM_SPILL		0x00000400
#define	PM_BCCAND		0x00000800
#define	PM_MOVE			0x00001000
#define	PM_NEWPROP		0x00002000
#define	PM_PUTOLD		0x00008000
#define	PM_LOCAL		0x00010000
#define	PM_INVALID		0x00020000
#define	PM_COMPACTREF	0x00040000
#define	PM_RESET		0x00080000
#define	PM_SCOLL		0x00100000
#define	PM_OLDFTINDEX	0x00200000
#define	PM_NEWVALUES	0x00400000
#define	PM_OLDVALUES	0x00800000
#define	PM_CALCULATED	0x01000000
#define	PM_GENEIDS		0x02000000

/**
 * property information used in modifyPIN()
 */
struct PropInfo
{
	PropertyID					propID;
	const HeapPageMgr::HeapV	*hprop;
	struct	ModInfo				*first;
	struct	ModInfo				*last;
	unsigned					nElts;
	unsigned					flags;
	ElementID					maxKey;
	ElementID					single;
	long						delta;
	long						nDelta;
	class	Collection			*pcol;
	class PropInfoCmp {public: __forceinline static int	cmp(const PropInfo *pi,PropertyID pid) {return cmp3(pi->propID,pid);}};
	operator PropertyID() const {return propID;}
};

typedef DynOArrayPtr<PropInfo,PropertyID,PropInfo::PropInfoCmp,8> ModProps;

/**
 * individual modification information in modifyPIN()
 */
struct ModInfo
{
	ModInfo			*next;
	ModInfo			*pnext;
	const Value		*pv;
	unsigned		pvIdx;
	Value			*newV;
	Value			*oldV;
	unsigned		flags;
	ElementID		eid;
	ElementID		eltKey;
	ElementID		epos;
	PropInfo		*pInfo;
	HRefSSV			*href;
};

}

#endif
