/**************************************************************************************

Copyright � 2004-2014 GoPivotal, Inc. All rights reserved.

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

#include "expr.h"
#include <math.h>

using namespace AfyKernel;

RC Expr::linear(ExprOp op,Value& arg,const Value *moreArgs,int nargs,unsigned flags,const EvalCtx& ctx)
{
	assert(arg.type==VT_ARRAY || nargs==2 && moreArgs->type==VT_ARRAY);
	switch (op) {
	case OP_PLUS:
	case OP_MINUS:
	case OP_MUL:
	case OP_DIV:
	case OP_NORM:
	case OP_TRACE:
	case OP_INV:
	case OP_DET:
	case OP_RANK:
		//???
		return RC_INTERNAL;
	default: return RC_INVOP;
	}
	return RC_OK;
}
