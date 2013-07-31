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

Written by Mark Venguerov 2013

**************************************************************************************/

/**
 * Named PINs: all meta-PINs and globally named PINs (anchors)
 */
#ifndef _NAMED_H_
#define _NAMED_H_

#include "idxtree.h"
#include "pinex.h"
#include "qmgr.h"

using namespace Afy;

namespace AfyKernel
{

class Identity;
	
#define AFY_PROTOCOL	"affinity://"
	
struct SpecPINProps
{
	uint64_t	mask[4];
	unsigned	meta;
};
	
struct BuiltinURI
{
	size_t		lname;
	const char	*name;
	URIID		uid;
};

class NamedMgr : public TreeGlobalRoot
{
	friend	class Classifier;
	StoreCtx		*const	ctx;
	const char		*storePrefix;
	size_t			lStorePrefix;
	volatile long	xPropID;
	Mutex			lock;
	bool			fInit;
public:
	NamedMgr(StoreCtx *ct);
	RC					createBuiltinObjects(Session *ses);
	RC					loadObjects(Session *ses);
	RC					initStorePrefix(Identity *ident=NULL);
	RC					restoreXPropID(Session *ses);
	bool				isInit() const {return fInit;}
	const	char		*getStorePrefix(size_t& l) const {l=lStorePrefix; return storePrefix;}
	PropertyID			getXPropID() const {return (PropertyID)xPropID;}
	void				setMaxPropID(PropertyID id);
	bool				exists(URIID);
	RC					getNamedPID(URIID uid,PID& id);
	RC					getNamed(URIID id,PINx&,bool fUpdate=false);
	RC					update(URIID id,PINRef& pr,uint16_t meta,bool fInsert);
	static	const char	*getBuiltinName(URIID uid,size_t& lname);
	static	URIID		getBuiltinURIID(const char *name,size_t lname,bool fSrv);
	static	uint16_t	getMeta(ClassID cid);
	static	const		SpecPINProps specPINProps[9];
private:
	static	const		BuiltinURI	builtinURIs[];
	static	const		unsigned	classMeta[MAX_BUILTIN_CLASSID+1];
};

}

#endif
