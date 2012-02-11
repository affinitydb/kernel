/**************************************************************************************

Copyright © 2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2010

**************************************************************************************/

#include "queryprc.h"
#include "stmt.h"
#include "expr.h"
#include "parser.h"
#include "maps.h"
#include "classifier.h"
#include "startup.h"
#include "fio.h"

using namespace MVStoreKernel;

#define	SELECT_S			"SELECT"
#define	STAR_S				"*"
#define	FROM_S				"FROM"
#define	WHERE_S				"WHERE"
#define ORDER_S				"ORDER"
#define	BY_S				"BY"
#define	GROUP_S				"GROUP"
#define	HAVING_S			"HAVING"
#define	JOIN_S				"JOIN"
#define	ON_S				"ON"
#define	LEFT_S				"LEFT"
#define	OUTER_S				"OUTER"
#define	RIGHT_S				"RIGHT"
#define	FULL_S				"FULL"
#define	AND_S				"AND"
#define	OR_S				"OR"
#define	NOT_S				"NOT"
#define	UNION_S				"UNION"
#define	EXCEPT_S			"EXCEPT"
#define	INTERSECT_S			"INTERSECT"
#define	ALL_S				"ALL"
#define	ANY_S				"ANY"
#define	SOME_S				"SOME"
#define	MIN_S				"MIN"
#define	MAX_S				"MAX"
#define	ASC_S				"ASC"
#define	DISTINCT_S			"DISTINCT"
#define	VALUES_S			"VALUES"
#define	TRUE_S				"TRUE"
#define	FALSE_S				"FALSE"
#define	NULL_S				"NULL"
#define	AS_S				"AS"
#define	DESC_S				"DESC"
#define	CURRENT_TIMESTAMP_S	"CURRENT_TIMESTAMP"
#define	CURRENT_USER_S		"CURRENT_USER"
#define	CURRENT_STORE_S		"CURRENT_STORE"
#define	BETWEEN_S			"BETWEEN"
#define	COUNT_S				"COUNT"
#define	IS_S				"IS"
#define	TIMESTAMP_S			"TIMESTAMP"
#define	INTERVAL_S			"INTERVAL"
#define	AGAINST_S			"AGAINST"
#define	MATCH_S				"MATCH"
#define	UPPER_S				"UPPER"
#define	LOWER_S				"LOWER"
#define	NULLS_S				"NULLS"
#define	FIRST_S				"FIRST"
#define	LAST_S				"LAST"
#define	CROSS_S				"CROSS"
#define	INNER_S				"INNER"
#define	USING_S				"USING"
#define	WITH_S				"WITH"
#define	INTO_S				"INTO"

#define	INSERT_S			"INSERT"
#define	UPDATE_S			"UPDATE"
#define	DELETE_S			"DELETE"
#define	CREATE_S			"CREATE"
#define	DROP_S				"DROP"
#define	PURGE_S				"PURGE"
#define	UNDELETE_S			"UNDELETE"
#define	SET_S				"SET"
#define	ADD_S				"ADD"
#define	MOVE_S				"MOVE"
#define	RENAME_S			"RENAME"
#define	EDIT_S				"EDIT"
#define	START_S				"START"
#define	TRANSACTION_S		"TRANSACTION"
#define	READ_S				"READ"
#define	ONLY_S				"ONLY"
#define	WRITE_S				"WRITE"
#define	COMMIT_S			"COMMIT"
#define	ROLLBACK_S			"ROLLBACK"			
#define	ISOLATION_S			"ISOLATION"
#define	PART_S				"PART"

#define	CLASS_S				"CLASS"
#define	OPEN_S				"OPEN"
#define	STORE_S				"STORE"
#define	BEFORE_S			"BEFORE"
#define	AFTER_S				"AFTER"
#define	OPTIONS_S			"OPTIONS"
#define	CLOSE_S				"CLOSE"
#define	TO_S				"TO"

#define	BASE_S				"BASE"
#define PREFIX_S			"PREFIX"

#define CONSTRUCT_S			"CONSTRUCT"
#define	DESCRIBE_S			"DESCRIBE"
#define	ASK_S				"ASK"
#define	GRAPH_S				"GRAPH"
#define	FILTER_S			"FILTER"
#define	OPTIONAL_S			"OPTIONAL"
#define	REDUCED_S			"REDUCED"
#define	NAMED_S				"NAMED"
#define	BOUND_S				"BOUND"
#define	STR_S				"STR"
#define	LANG_S				"LANG"
#define	LANGMATCHES_S		"LANGMATCHES"
#define	DATATYPE_S			"DATATYPE"
#define ISURI_S				"ISURI"
#define ISIRI_S				"ISIRI"
#define ISLITERAL_S			"ISLITERAL"
#define	SAMETERM_S			"SAMETERM"
#define	A_S					"A"
 					
#define	URIID_PREFIX		"."
#define	IDENTITY_PREFIX		"!"
#define	PID_PREFIX			"@"
#define	PARAM_PREFIX		":"
#define	PATH_DELIM			"."
#define	QNAME_DELIM			":"

const OpDscr SInCtx::opDscr[] = 
{
	{0,		LX_EOE,		0,NULL,				{0,	  4},	{0,	0}	},		 //LX_BOE
	{0,		LX_ERR,		0,NULL,				{0,	  0},	{0,	0}	},		 //LX_EOE
	{0,		LX_ERR,		0,NULL,				{0,	  0},	{0,	0}	},		 //LX_IDENT
	{0,		LX_ERR,		0,NULL,				{0,	  0},	{0,	0}	},		 //LX_CON
	{0,		LX_ERR,		0,NULL,				{0,	  0},	{0,	0}	},		 //LX_KEYW
	{_U,	LX_RPR,		S_L("("),			{40,  2},	{1,	1}	},		 //LX_LPR
	{_C,	LX_ERR,		S_L(")"),			{2,	  0},	{0,	0}	},		 //LX_RPR
	{0,		LX_RPR,		S_L(","),			{3,	  2},	{0,	0}	},		 //LX_COMMA
	{0,		LX_ERR,		S_L("+"),			{12, 12},	{2,	2}	},		 //OP_PLUS
	{0,		LX_ERR,		S_L("-"),			{12, 12},	{2,	2}	},		 //OP_MINUS
	{0,		LX_ERR,		S_L("*"),			{15, 15},	{2,	2}	},		 //OP_MUL
	{0,		LX_ERR,		S_L("/"),			{15, 15},	{2,	2}	},		 //OP_DIV
	{0,		LX_ERR,		S_L("%"),			{15, 15},	{2,	2}	},		 //OP_MOD
	{_U,	LX_ERR,		S_L("-"),			{17, 16},	{1,	1}	},		 //OP_NEG
	{_U,	LX_ERR,		S_L("~"),			{17, 16},	{1,	1}	},		 //OP_NOT
	{0,		LX_ERR,		S_L("&"),			{10, 10},	{2,	2}	},		 //OP_AND
	{0,		LX_ERR,		S_L("|"),			{10, 10},	{2,	2}	},		 //OP_OR
	{0,		LX_ERR,		S_L("^"),			{10, 10},	{2,	2}	},		 //OP_XOR
	{0,		LX_ERR,		S_L("<<"),			{10, 10},	{2,	2}	},		 //OP_LSHIFT
	{0,		LX_ERR,		S_L(">>"),			{10, 10},	{2,	2}	},		 //OP_RSHIFT
	{_F|_A,	LX_RPR,		S_L(MIN_S),			{40,  2},	{1,255}	},		 //OP_MIN
	{_F|_A,	LX_RPR,		S_L(MAX_S),			{40,  2},	{1,255}	},		 //OP_MAX
	{_F,	LX_RPR,		S_L("ABS"),			{40,  2},	{1, 1}	},		 //OP_ABS
	{_F,	LX_RPR,		S_L("LN"),			{40,  2},	{1, 1}	},		 //OP_LN
	{_F,	LX_RPR,		S_L("EXP"),			{40,  2},	{1, 1}	},		 //OP_EXP
	{_F,	LX_RPR,		S_L("POWER"),		{40,  2},	{2, 2}	},		 //OP_POW
	{_F,	LX_RPR,		S_L("SQRT"),		{40,  2},	{1, 1}	},		 //OP_SQRT
	{_F,	LX_RPR,		S_L("FLOOR"),		{40,  2},	{1, 1}	},		 //OP_FLOOR
	{_F,	LX_RPR,		S_L("CEIL"),		{40,  2},	{1, 1}	},		 //OP_CEIL
	{0,		LX_ERR,		S_L("||"),			{11, 11},	{2,	2}	},		 //OP_CONCAT
	{_F,	LX_RPR,		S_L(LOWER_S),		{40,  2},	{1,	1}	},		 //OP_LOWER
	{_F,	LX_RPR,		S_L(UPPER_S),		{40,  2},	{1,	1}	},		 //OP_UPPER
	{_F,	LX_RPR,		S_L("TONUM"),		{40,  2},	{1,	1}	},		 //OP_TONUM
	{_F,	LX_RPR,		S_L("TOINUM"),		{40,  2},	{1,	1}	},		 //OP_TOINUM
	{_F|_I,	LX_RPR,		S_L("CAST"),		{40,  2},	{2,	2}	},		 //OP_CAST
	{0,		LX_RBR,		S_L(","),			{40,  2},	{2,	2}	},		 //OP_RANGE
	{0,		LX_RCBR,	S_L(","),			{40,  2},	{1,255}	},		 //OP_ARRAY
	{0,		LX_RCBR,	S_L(","),			{40,  2},	{1,255}	},		 //OP_STRUCT
	{0,		LX_RCBR,	S_L(","),			{40,  2},	{1,255}	},		 //OP_PIN
	{_F|_A,	LX_RPR,		S_L(COUNT_S),		{40,  2},	{1,	2}	},		 //OP_COUNT
	{_F,	LX_RPR,		S_L("LENGTH"),		{40,  2},	{1,	1}	},		 //OP_LENGTH
	{_F,	LX_RPR,		S_L("POSITION"),	{40,  2},	{2,	2}	},		 //OP_POSITION
	{_F,	LX_RPR,		S_L("SUBSTR"),		{40,  2},	{2,	3}	},		 //OP_SUBSTR
	{_F,	LX_RPR,		S_L("REPLACE"),		{40,  2},	{3,	3}	},		 //OP_REPLACE
	{_F,	LX_RPR,		S_L("PAD"),			{40,  2},	{2,	3}	},		 //OP_PAD
	{_F|_I,	LX_RPR,		S_L("TRIM"),		{40,  2},	{3,	3}	},		 //OP_TRIM
	{_F|_A,	LX_RPR,		S_L("SUM"),			{40,  2},	{1,255}	},		 //OP_SUM
	{_F|_A,	LX_RPR,		S_L("AVG"),			{40,  2},	{1,255}	},		 //OP_AVG
	{_F|_A,	LX_RPR,		S_L("VAR_POP"),		{40,  2},	{1,255}	},		 //OP_VAR_POP
	{_F|_A,	LX_RPR,		S_L("VAR_SAMP"),	{40,  2},	{1,255}	},		 //OP_VAR_SAMP
	{_F|_A,	LX_RPR,		S_L("STDDEV_POP"),	{40,  2},	{1,255}	},		 //OP_STDDEV_POP
	{_F|_A,	LX_RPR,		S_L("STDDEV_SAMP"),	{40,  2},	{1,255}	},		 //OP_STDDEV_SAMP
	{_F|_A,	LX_RPR,		S_L("HISTOGRAM"),	{40,  2},	{1,255}	},		 //OP_HISTOGRAM
	{_F,	LX_RPR,		S_L("COALESCE"),	{40,  2},	{2,255}	},		 //OP_COALESCE
	{_F,	LX_RPR,		S_L("MEMBERSHIP"),	{40,  2},	{1, 1}	},		 //OP_MEMBERSHIP
	{0,		LX_ERR,		S_L("."),			{32, 32},	{2,	4}	},		 //OP_PATH
	{0,		LX_ERR,		S_L("="),			{8,   8},	{2,	2}	},		 //OP_EQ
	{0,		LX_ERR,		S_L("<>"),			{8,   8},	{2,	2}	},		 //OP_NE
	{0,		LX_ERR,		S_L("<"),			{9,   9},	{2,	2}	},		 //OP_LT
	{0,		LX_ERR,		S_L("<="),			{9,   9},	{2,	2}	},		 //OP_LE
	{0,		LX_ERR,		S_L(">"),			{9,   9},	{2,	2}	},		 //OP_GT
	{0,		LX_ERR,		S_L(">="),			{9,   9},	{2,	2}	},		 //OP_GE
	{0,		LX_ERR,		S_L("IN"),			{9,   9},	{2,	2}	},		 //OP_IN
	{_F,	LX_RPR,		S_L("BEGINS"),		{40,  2},	{2,	2}	},		 //OP_BEGINS
	{_F,	LX_RPR,		S_L("CONTAINS"),	{40,  2},	{2,	2}	},		 //OP_CONTAINS
	{_F,	LX_RPR,		S_L("ENDS"),		{40,  2},	{2,	2}	},		 //OP_ENDS
	{_F,	LX_RPR,		S_L("REGEX"),		{40,  2},	{2,	2}	},		 //OP_REGEX
	{_F,	LX_RPR,		S_L("EXISTS"),		{40,  2},	{1,	1}	},		 //OP_EXISTS
	{_F,	LX_RPR,		S_L("ISLOCAL"),		{40,  2},	{1,	1}	},		 //OP_ISLOCAL
	{0,		LX_RPR,		0,NULL,				{9,   9},	{2,255}	},		 //OP_IS_A
	{0,		LX_ERR,		S_L(AND_S),			{6,   6},	{2,	2}	},		 //OP_LAND
	{0,		LX_ERR,		S_L(OR_S),			{5,   5},	{2,	2}	},		 //OP_LOR
	{_U,	LX_ERR,		S_L(NOT_S),			{7,	  7},	{1,	1}	},		 //OP_LNOT
	{_F|_I,	LX_RPR,		S_L("EXTRACT"),		{40,  2},	{2,	2}	},		 //OP_EXTRACT
	{_U,	LX_ERR,		S_L("*"),			{30, 29},	{1,	1}	},		 //OP_DEREF
	{_U,	LX_ERR,		S_L("&"),			{30, 29},	{1,	3}	},		 //OP_REF
	{0,		LX_RPR,		S_L("("),			{40,  2},	{1,255}	},		 //OP_CALL
	{0,		LX_ERR,		S_L(":"),			{20, 20},	{0,	0}	},		 //LX_COLON
	{_U,	LX_RBR,		S_L("["),			{36,  2},	{1,	1}	},		 //LX_LBR
	{_C,	LX_ERR,		S_L("]"),			{2,	  0},	{0,	0}	},		 //LX_RBR
	{_U,	LX_RCBR,	S_L("{"),			{36,  2},	{1,	1}	},		 //LX_LCBR
	{_C,	LX_ERR,		S_L("}"),			{2,	  0},	{0,	0}	},		 //LX_RCBR
	{0,		LX_ERR,		S_L("."),			{18, 18},	{0,	0}	},		 //LX_PERIOD
	{0,		LX_ERR,		S_L("?"),			{20, 20},	{0,	0}	},		 //LX_QUEST
	{0,		LX_ERR,		S_L("!"),			{20, 20},	{0,	0}	},		 //LX_EXCL
	{_F,	LX_RPR,		S_L("$"),			{40,  2},	{1,	1}	},		 //LX_EXPR
	{0,		LX_RCBR,	S_L("${"),			{0,   0},	{1,	1}	},		 //LX_QUERY
	{0,		LX_ERR,		S_L(">>>"),			{10, 10},	{2,	2}	},		 //LX_URSHIFT
	{0,		LX_ERR,		0,NULL,				{0,	  0},	{0,	0}	},		 //LX_PREFIX
	{0,		LX_ERR,		S_L(IS_S),			{9,   9},	{2,	2}	},		 //LX_IS
	{0,		LX_ERR,		S_L(BETWEEN_S),		{9,   9},	{2,	2}	},		 //LX_BETWEEN
	{0,		LX_ERR,		0,NULL,				{9,   9},	{2,	2}	},		 //LX_BAND
	{0,		LX_ERR,		0,NULL,				{0,	  0},	{0,	0}	},		 //LX_HASH
	{0,		LX_ERR,		0,NULL,				{0,	  0},	{0,	0}	},		 //LX_SEMI
	{_F|_A,	LX_RPR,		S_L("CONCAT"),		{40,  2},	{1,255}	},		 //LX_CONCAT
	{0,		LX_RBR,		S_L("["),			{36,  2},	{2,	2}	},		 //LX_FILTER
	{0,		LX_RCBR,	S_L("{"),			{36,  2},	{2,	3}	},		 //LX_REPEAT
	{0,		LX_ERR,		0,NULL,				{33, 32},	{2,	2}	},		 //LX_PATHQ
	{0,		LX_ERR,		0,NULL,				{0,	  0},	{0,	0}	},		 //LX_SELF
	{0,		LX_ERR,		0,NULL,				{40,  3},	{2,	2}	},		 //LX_SPROP
};

const static Len_Str typeName[VT_ALL] = 
{
	{S_L("")}, {S_L("INT")}, {S_L("UINT")}, {S_L("INT64")}, {S_L("UINT64")}, {S_L("DECIMAL")}, {S_L("FLOAT")}, {S_L("DOUBLE")},
	{S_L("BOOL")}, {S_L("DATETIME")}, {S_L("INTERVAL")}, {S_L("URIID")}, {S_L("IDENTITY")},
	{S_L("STRING")}, {S_L("BSTR")}, {S_L("URL")}, {S_L("ENUM")},
	{S_L("REF")}, {S_L("REFID")}, {S_L("REFVAL")}, {S_L("REFIDVAL")}, {S_L("REFELT")}, {S_L("REFIDELT")},
	{S_L("EXPR")}, {S_L("QUERY")}, {S_L("ARRAY")}, {S_L("COLLECTION")}, {S_L("STRUCT")}, {S_L("RANGE")},
	{S_L("STREAM")}, {S_L("CURRENT")}, {S_L("VARREF")}, {S_L("EXPRTREE")}
};

const static Len_Str extractWhat[] = 
{
	{S_L("FRACTIONAL")}, {S_L("SECOND")}, {S_L("MINUTE")}, {S_L("HOUR")}, {S_L("DAY")}, {S_L("WDAY")}, {S_L("MONTH")}, {S_L("YEAR")}, {S_L("IDENTITY")},
};

const static Len_Str stmtKWs[STMT_OP_ALL] =
{
	{S_L("")},{S_L(INSERT_S)},{S_L(UPDATE_S)},{S_L(DELETE_S)},{S_L(UNDELETE_S)},{S_L(START_S)},{S_L(COMMIT_S)},{S_L(ROLLBACK_S)},{S_L(SET_S)}
};

const static Len_Str opKWs[OP_FIRST_EXPR] =
{
	{S_L(" "SET_S" ")}, {S_L(" "ADD_S" ")}, {S_L(" "ADD_S" ")}, {S_L(" "MOVE_S" ")}, {S_L(" "MOVE_S" ")}, {S_L(" "DELETE_S" ")}, {S_L(" "EDIT_S" ")}, {S_L(" "RENAME_S" ")}
};

static RC renderElement(ElementID eid,SOutCtx& out,bool fBefore=false)
{
	if (eid!=STORE_COLLECTION_ID) {
		char *p,buf[30]; size_t l;
		switch (eid) {
		case STORE_FIRST_ELEMENT: p=":"FIRST_S; l=sizeof(FIRST_S); break;
		case STORE_LAST_ELEMENT: p=":"LAST_S; l=sizeof(LAST_S); break;
		case STORE_MIN_ELEMENT: p=":"MIN_S; l=sizeof(MIN_S); break;
		case STORE_MAX_ELEMENT: p=":"MAX_S; l=sizeof(MAX_S); break;
		case STORE_SUM_COLLECTION: p=":SUM"; l=sizeof(":SUM")-1; break;
		case STORE_AVG_COLLECTION: p=":AVG"; l=sizeof(":AVG")-1; break;
		case STORE_CONCAT_COLLECTION: p=":CONCAT"; l=sizeof(":CONCAT")-1; break;
		default: l=sprintf(buf,"%u",eid); p=buf; break;
		}
		if (!out.append("[",1) || fBefore && !out.append(BEFORE_S" ",sizeof(BEFORE_S)) ||
			!out.append(p,l) || !out.append("]",1))  return RC_NORESOURCES;
	}
	return RC_OK;
}

static RC renderOrder(const OrderSegQ *seg,unsigned nSegs,SOutCtx& out,bool fExprOnly=false)
{
	RC rc;
	for (unsigned i=0; i<nSegs; i++,seg++) {
		if (i!=0 && !out.append(",",1)) return RC_NORESOURCES;
		if ((seg->flags&ORDER_EXPR)!=0) {
			if ((rc=seg->expr->render(0,out))!=RC_OK) return rc;
		} else {
			if ((seg->flags&ORD_NCASE)!=0 && !out.append(UPPER_S"(",sizeof(UPPER_S))) return RC_NORESOURCES;
			if ((rc=out.renderName(seg->pid))!=RC_OK) return rc;
			if ((seg->flags&ORD_NCASE)!=0 && !out.append(")",1)) return RC_NORESOURCES;
		}
		if (!fExprOnly) {
			if ((seg->flags&ORD_DESC)!=0 && !out.append(" "DESC_S,sizeof(DESC_S))) return RC_NORESOURCES;
			if ((seg->flags&ORD_NULLS_BEFORE)!=0) {
				if (!out.append(" "NULLS_S" "FIRST_S,sizeof(NULLS_S)+sizeof(FIRST_S))) return RC_NORESOURCES;
			} else if ((seg->flags&ORD_NULLS_AFTER)!=0) {
				if (!out.append(" "NULLS_S" "LAST_S,sizeof(NULLS_S)+sizeof(LAST_S))) return RC_NORESOURCES;
			}
		}
	}
	return RC_OK;
}

RC Stmt::render(SOutCtx& out) const
{
	RC rc=RC_OK; unsigned i,prev=~0u; bool fFrom=top!=NULL;
	out.flags&=~(SOM_VAR_NAMES|SOM_WHERE|SOM_MATCH|SOM_AND);
	if (op<STMT_OP_ALL && op!=STMT_QUERY && 
		(!out.append(stmtKWs[op].s,stmtKWs[op].l) || !out.append(" ",1))) return RC_NORESOURCES;
	switch (op) {
	default: return RC_INVPARAM;
	case STMT_QUERY:
		if (top==NULL) return RC_INVPARAM;
		if ((rc=top->render(out))!=RC_OK) return rc;
		if (orderBy!=NULL && nOrderBy>0) {
			if (!out.append("\n"ORDER_S" "BY_S" ",sizeof(ORDER_S)+sizeof(BY_S)+1)) return RC_NORESOURCES;
			rc=renderOrder(orderBy,nOrderBy,out);
		}
		break;
	case STMT_START_TX:
		if (!out.append(TRANSACTION_S" ",sizeof(TRANSACTION_S))) return RC_NORESOURCES;
		// isolation level
		if ((mode&MODE_READONLY)!=0 && !out.append(READ_S" "ONLY_S,sizeof(READ_S)+sizeof(ONLY_S)-1)) return RC_NORESOURCES;
		break;
	case STMT_COMMIT:
	case STMT_ROLLBACK:
		if ((mode&MODE_ALL)!=0 && !out.append(ALL_S,sizeof(ALL_S)-1)) return RC_NORESOURCES;
		break;
	case STMT_ISOLATION:
		if (!out.append(ISOLATION_S" ",sizeof(ISOLATION_S))) return RC_NORESOURCES;
		// isolation level
		break;
	case STMT_INSERT:
		if (values!=NULL) for (i=0; i<nValues; i++) {
			const Value& v=values[i];
			if (v.property!=STORE_INVALID_PROPID) {
				if (i!=0 && !out.append(", ",2)) return RC_NORESOURCES;
				if ((rc=out.renderName(v.property))!=RC_OK) return rc;
				if (!out.append("=",1)) return RC_NORESOURCES;
				if ((rc=out.renderValue(v))!=RC_OK) return rc;
			}
		}
		if (fFrom) rc=top->render(out);
		break;
	case STMT_UPDATE:
		if (nVars==1 && top->getType()==QRY_SIMPLE)
			if ((rc=top->render(RP_FROM,out))==RC_OK) fFrom=false; else break;
		if (values!=NULL) for (i=0; i<nValues; i++) {
			const Value& v=values[i];
			if (v.property!=STORE_INVALID_PROPID && v.op<=OP_CONCAT) {
				unsigned op=v.op>=(uint8_t)OP_FIRST_EXPR?OP_SET:v.op==(uint8_t)OP_ADD_BEFORE?OP_ADD:v.op==(uint8_t)OP_MOVE_BEFORE?OP_MOVE:(ExprOp)v.op;
				if (op==prev) {if (!out.append(", ",2)) return RC_NORESOURCES;}
				else if (out.append(opKWs[op].s,opKWs[op].l)) prev=op; else return RC_NORESOURCES;
				if ((rc=out.renderName(v.property))!=RC_OK) return rc;
				if (v.eid!=STORE_COLLECTION_ID && op!=OP_RENAME && (rc=renderElement(v.eid,out,v.op==OP_ADD_BEFORE))!=RC_OK) return rc;
				if ((v.meta&META_PROP_IFEXIST)!=0) {if (!out.append("?",1)) return RC_NORESOURCES;}
				else if ((v.meta&META_PROP_IFNOTEXIST)!=0 && !out.append("!",1)) return RC_NORESOURCES;
				switch (op) {
				case OP_SET:
					if (v.op>=OP_FIRST_EXPR && !out.append(SInCtx::opDscr[v.op].str,SInCtx::opDscr[v.op].lstr)) return RC_NORESOURCES;
				case OP_ADD:
					if (!out.append("=",1)) return RC_NORESOURCES;
					if ((rc=out.renderValue(v))!=RC_OK) return rc;
				case OP_DELETE: break;
				case OP_MOVE: case OP_MOVE_BEFORE:
					if (v.op==OP_MOVE) {
						if (!out.append(" "AFTER_S" ",sizeof(AFTER_S)+1)) return RC_NORESOURCES;
					} else {
						if (!out.append(" "BEFORE_S" ",sizeof(BEFORE_S)+1)) return RC_NORESOURCES;
					}
					if ((rc=renderElement(v.ui,out))!=RC_OK) return rc;
					break;
				case OP_RENAME:
					if (!out.append("=",1)) return RC_NORESOURCES;
					if ((rc=out.renderName(v.ui))!=RC_OK) return rc;
					break;
				case OP_EDIT:
					//???
					break;
				}
			}
		}
	case STMT_DELETE: case STMT_UNDELETE:
		if (fFrom) {
			if (top->type!=QRY_SIMPLE || ((SimpleVar*)top)->classes!=NULL || ((SimpleVar*)top)->pids!=NULL || ((SimpleVar*)top)->path!=NULL) {
				if (!out.append("\n"FROM_S" ",sizeof(FROM_S)+1)) return RC_NORESOURCES;
				if ((rc=top->render(RP_FROM,out))!=RC_OK) break;
			} else {
				if (((SimpleVar*)top)->condIdx!=NULL) out.flags|=SOM_WHERE;
				if (((SimpleVar*)top)->condFT!=NULL) out.flags|=SOM_MATCH;
			}
		}
		if (top!=NULL) {
			if ((out.flags&SOM_WHERE)!=0 || top->nConds>0) {
				if (!out.append("\n"WHERE_S" ",sizeof(WHERE_S)+1)) return RC_NORESOURCES;
				out.flags&=~SOM_AND; if ((rc=top->render(RP_WHERE,out))!=RC_OK) break;
			}
			if ((out.flags&SOM_MATCH)!=0) rc=top->render(RP_MATCH,out);
		}
		break;
	}
	return rc;
}

RC QVar::render(SOutCtx& out) const
{
	RC rc; out.flags&=~(SOM_VAR_NAMES|SOM_WHERE|SOM_MATCH|SOM_AND);

	if (!out.append(SELECT_S" ",sizeof(SELECT_S))) return RC_NORESOURCES;
	if (dtype==DT_ALL) {
		if (!out.append(ALL_S" ",sizeof(ALL_S))) return RC_NORESOURCES;
	} else if (dtype!=DT_DEFAULT) {
		if (!out.append(DISTINCT_S" ",sizeof(DISTINCT_S))) return RC_NORESOURCES;
	}
	if (stype==SEL_COUNT) {
		if (!out.append(COUNT_S"("STAR_S")",sizeof(COUNT_S)+sizeof(STAR_S))) return RC_NORESOURCES;
	} else if (outs!=NULL && nOuts!=0) for (unsigned j=0; j<nOuts; j++) {
		const ValueV& o=outs[j]; rc=RC_OK; const QVar *save=out.cvar; out.cvar=this;
		if (nOuts>1) {
				//???
		}
		for (unsigned i=0; i<o.nValues; i++) {
			const Value &v=o.vals[i]; bool fAs=true; const char *p;
			if (i!=0 && !out.append(", ",2)) return RC_NORESOURCES;
			switch (v.type) {
			case VT_ANY: if (v.op==OP_COUNT) {if (!out.append(STAR_S,sizeof(STAR_S)-1)) rc=RC_NORESOURCES; break;}
			default: rc=out.renderValue(v); break;
			case VT_EXPR: rc=((Expr*)v.expr)->render(SInCtx::opDscr[LX_COMMA].prty[0],out); break;
			case VT_EXPRTREE: rc=((ExprTree*)v.exprt)->render(SInCtx::opDscr[LX_COMMA].prty[0],out); break;
			case VT_VARREF:
				if (v.refV.type!=VT_ANY && !out.append("CAST(",5)) return RC_NORESOURCES;
				if ((rc=out.renderValue(v))==RC_OK && v.refV.type!=VT_ANY) {
					if (!out.append(" "AS_S" ",sizeof(AS_S)+1)) return RC_NORESOURCES;
					if (v.refV.type<VT_ALL) {if (!out.append(typeName[v.refV.type].s,typeName[v.refV.type].l)) return RC_NORESOURCES;}
					else if ((v.refV.type&0xFF)!=VT_DOUBLE && (v.refV.type&0xFF)!=VT_FLOAT || (p=getUnitName((Units)byte(v.refV.type>>8)))==NULL) return RC_CORRUPTED;
					else if (!out.append(p,strlen(p))) return RC_NORESOURCES;
					if (!out.append(")",1)) rc=RC_NORESOURCES;
				}
				fAs=(v.property!=PROP_SPEC_VALUE||o.nValues>1)&&((v.refV.flags&VAR_TYPE_MASK)!=0||v.length==0||v.refV.id!=v.property);
				break;
			}
			if (rc==RC_OK && fAs && v.property!=STORE_INVALID_PROPID && !out.ses->getStore()->queryMgr->checkCalcPropID(v.property))
				{if (!out.append(" "AS_S" ",sizeof(AS_S)+1)) return RC_NORESOURCES; rc=out.renderName(v.property);}
			if (rc!=RC_OK) return rc;
		}
		if (nOuts>1) {
			//???
		}
		out.cvar=save;
	} else {
		// check dscr in children (if join)
		if (!out.append(STAR_S,sizeof(STAR_S)-1)) return RC_NORESOURCES;
	}
	if (type!=QRY_SIMPLE || ((SimpleVar*)this)->classes!=NULL || ((SimpleVar*)this)->pids!=NULL || ((SimpleVar*)this)->path!=NULL) {
		if (!out.append("\n"FROM_S" ",sizeof(FROM_S)+1)) return RC_NORESOURCES;
		if ((rc=render(RP_FROM,out))!=RC_OK) return rc;
	} else {
		if (((SimpleVar*)this)->condIdx!=NULL) out.flags|=SOM_WHERE;
		if (((SimpleVar*)this)->condFT!=NULL) out.flags|=SOM_MATCH;
	}
	if ((out.flags&SOM_WHERE)!=0 || nConds>0) {
		if (!out.append("\n"WHERE_S" ",sizeof(WHERE_S)+1)) return RC_NORESOURCES;
		out.flags&=~SOM_AND; if ((rc=render(RP_WHERE,out))!=RC_OK) return rc;
	}
	if ((out.flags&SOM_MATCH)!=0 && (rc=render(RP_MATCH,out))!=RC_OK) return rc;
	if (groupBy!=NULL && nGroupBy!=0) {
		if (!out.append("\n"GROUP_S" "BY_S" ",sizeof(GROUP_S)+sizeof(BY_S)+1)) return RC_NORESOURCES;
		if ((rc=renderOrder(groupBy,nGroupBy,out))!=RC_OK) return rc;
		if (having!=NULL) {
			if (!out.append("\n"HAVING_S" ",sizeof(HAVING_S)+1)) return RC_NORESOURCES;
			if ((rc=having->render(0,out))!=RC_OK) return rc;
		}
		if (!out.append("\n",1)) return RC_NORESOURCES;
	}
	return RC_OK;
}

RC SetOpVar::render(SOutCtx& out) const
{
	assert(type==QRY_UNION||type==QRY_INTERSECT||type==QRY_EXCEPT);
	for (unsigned i=0; i<nVars; ++i) {
		const QVarRef& vr=vars[i]; byte ty=vr.var->getType(); RC rc;
		const bool fPar=type==QRY_EXCEPT || ty!=type && (ty==QRY_UNION||ty==QRY_EXCEPT||ty==QRY_INTERSECT);
		if (i!=0) switch (type) {
		case QRY_UNION: if (!out.append(" "UNION_S" ",sizeof(UNION_S)+1)) return RC_NORESOURCES; break;
		case QRY_EXCEPT: if (!out.append(" "EXCEPT_S" ",sizeof(EXCEPT_S)+1)) return RC_NORESOURCES; break;
		case QRY_INTERSECT: if (!out.append(" "INTERSECT_S" ",sizeof(INTERSECT_S)+1)) return RC_NORESOURCES; break;
		}
		if (dtype==DT_ALL) {
			if (!out.append(ALL_S" ",sizeof(ALL_S))) return RC_NORESOURCES;
		} else if (dtype!=DT_DEFAULT) {
			if (!out.append(DISTINCT_S" ",sizeof(DISTINCT_S))) return RC_NORESOURCES;
		}
		if (fPar && !out.append("(",1)) return RC_NORESOURCES;
		if ((rc=vr.var->render(out))!=RC_OK) return rc;
		if (fPar && !out.append(")",1)) return RC_NORESOURCES;
	}
	return RC_OK;
}

RC QVar::render(RenderPart part,SOutCtx& out) const
{
	RC rc;
	switch (part) {
	default: break;
	case RP_FROM:
		if (nConds>0) out.flags|=SOM_WHERE;
		if ((out.flags&SOM_VAR_NAMES)!=0) {
			if (!out.append(" "AS_S" ",sizeof(AS_S)+1)) return RC_NORESOURCES;
			if ((rc=out.renderVarName(this))!=RC_OK) return rc;
		}
		break;
	case RP_WHERE:
		if (nConds>0) {
			Expr *const *pcnd=nConds==1?&cond:conds; const QVar *save=out.cvar; out.cvar=this;
			const int prty=(out.flags&SOM_AND)!=0||nConds>1?SInCtx::opDscr[OP_LAND].prty[0]:0;
			for (unsigned i=0; i<nConds; out.flags|=SOM_AND,i++) {
				if ((out.flags&SOM_AND)!=0 && !out.append("\n\t"AND_S" ",sizeof(AND_S)+2)) return RC_NORESOURCES;
				if ((rc=pcnd[i]->render(prty,out))!=RC_OK) return rc;
			}
			out.cvar=save;
		}
		break;
	}
	return RC_OK;
}

const QVar *QVar::getRefVar(unsigned refN) const
{
	return refN==0?this:(QVar*)0;
}

RC JoinVar::render(RenderPart part,SOutCtx& out) const
{
	RC rc; assert(type==QRY_SEMIJOIN||type==QRY_JOIN||type==QRY_LEFT_OUTER_JOIN||type==QRY_RIGHT_OUTER_JOIN||type==QRY_FULL_OUTER_JOIN);
	//if (part==RP_FROM && !out.append("(",1)) return RC_NORESOURCES;
	// other parts?
	out.flags|=SOM_VAR_NAMES;
	for (unsigned i=0; i<nVars; ++i) {
		const QVarRef& vr=vars[i];
		if (i!=0 && part==RP_FROM) {
			switch (type) {
			default: assert(0); return RC_INTERNAL;
			case QRY_JOIN: case QRY_SEMIJOIN: break;
			case QRY_LEFT_OUTER_JOIN: if (!out.append(" "LEFT_S" ",sizeof(LEFT_S)+1)) return RC_NORESOURCES; break;
			case QRY_RIGHT_OUTER_JOIN: if (!out.append(" "RIGHT_S" ",sizeof(RIGHT_S)+1)) return RC_NORESOURCES; break;
			case QRY_FULL_OUTER_JOIN: if (!out.append(" "FULL_S" "OUTER_S" ",sizeof(FULL_S)+sizeof(OUTER_S)+1)) return RC_NORESOURCES; break;
			}
			if (!out.append(" "JOIN_S" ",sizeof(JOIN_S)+1)) return RC_NORESOURCES;
		}
		if ((rc=vr.var->render(part,out))!=RC_OK) return rc;
	}
	if (part==RP_FROM && (condEJ!=NULL || nConds!=0)) {
		const QVar *save=out.cvar; out.cvar=this;
		if (!out.append(" "ON_S" ",sizeof(ON_S)+1)) return RC_NORESOURCES;
		for (CondEJ *ej=condEJ; ej!=NULL; ej=ej->next) {
			if (ej!=condEJ && !out.append(" "AND_S" ",sizeof(AND_S)+1)) return RC_NORESOURCES;
			Value v; v.setVarRef(0,ej->propID1);
			if ((rc=out.renderValue(v))!=RC_OK) return rc;
			if (!out.append(SInCtx::opDscr[OP_EQ].str,SInCtx::opDscr[OP_EQ].lstr)) return RC_NORESOURCES;
			v.setVarRef(1,ej->propID2);
			if ((rc=out.renderValue(v))!=RC_OK) return rc;
		}
		if (nConds!=0) {
			Expr *const *pcnd = nConds==1?&cond:conds; if (condEJ!=NULL) out.flags|=SOM_AND;
			const int prty=condEJ!=NULL||nConds>1?SInCtx::opDscr[OP_LAND].prty[0]:0;
			for (unsigned i=0; i<nConds; out.flags|=SOM_AND,i++) {
				if ((out.flags&SOM_AND)!=0 && !out.append(" "AND_S" ",sizeof(AND_S)+1)) return RC_NORESOURCES;
				if ((rc=pcnd[i]->render(prty,out))!=RC_OK) return rc;
			}
		}
		out.cvar=save;
	}
	//if (part==RP_FROM && level!=0 && !out.append(")",1)) return RC_NORESOURCES;
	return RC_OK;
}

const QVar *JoinVar::getRefVar(unsigned refN) const
{
	return refN<nVars?vars[refN].var->getRefVar(0):(QVar*)0;
}

RC SetOpVar::render(RenderPart,SOutCtx&) const
{
	return RC_OK;
}

RC SimpleVar::render(RenderPart part,SOutCtx& out) const
{
	RC rc; const QVar *save;
	switch (part) {
	default: break;
	case RP_FROM:
		if (condIdx!=NULL) out.flags|=SOM_WHERE;
		if (condFT!=NULL) out.flags|=SOM_MATCH;
		if (subq!=NULL) {
			if (!out.append("(",1)) return RC_NORESOURCES;
			ulong save=out.flags; out.flags&=SOM_STD_PREFIX|SOM_BASE_USED; RC rc;
			if ((rc=subq->render(out))!=RC_OK) return rc;
			if (!out.append(")",1)) return RC_NORESOURCES;
			out.flags=save|(out.flags&(SOM_STD_PREFIX|SOM_BASE_USED));
		} else if (classes!=NULL && nClasses!=0) {
			for (ulong i=0; i<nClasses; i++) {
				const ClassSpec &cs=classes[i];
				if (i>0 && !out.append(" & ",3)) return RC_NORESOURCES;
				if ((cs.classID&CLASS_PARAM_REF)!=0) {
					size_t l=sprintf(out.cbuf,PARAM_PREFIX"%u",cs.classID&~CLASS_PARAM_REF);
					if (!out.append(out.cbuf,l)) return RC_NORESOURCES;
				} else if ((rc=out.renderName(cs.classID))!=RC_OK) return rc;
				if (cs.params!=NULL && cs.nParams>0) {
					for (ulong j=0; j<cs.nParams; j++) {
						out.append(j==0?"(":",",1);
						if ((rc=out.renderValue(cs.params[j]))!=RC_OK) return rc;
					}
					out.append(")",1);
				}
			}
		}
		if (pids!=NULL && nPids>0) {
			if ((nPids>1 || classes!=NULL && nClasses!=0) && !out.append("{",1)) return RC_NORESOURCES;
			for (unsigned i=0; i<nPids; i++) {
				if (i!=0 && !out.append(",",1)) return RC_NORESOURCES;
				if ((rc=out.renderPID(pids[i]))!=RC_OK) return rc;
			}
			if ((nPids>1 || classes!=NULL && nClasses!=0) && !out.append("}",1)) return RC_NORESOURCES;
		} else if (classes==NULL && subq==NULL && !out.append(STAR_S,sizeof(STAR_S)-1)) return RC_NORESOURCES;
		if (path!=NULL) for (unsigned i=0; i<nPathSeg; i++) if ((rc=out.renderPath(path[i]))!=RC_OK) return rc;
		break;
	case RP_WHERE:
		save=out.cvar; out.cvar=this;
		for (CondIdx *ci=condIdx; ci!=NULL; ci=ci->next,out.flags|=SOM_AND) {
			assert(ci->ks.op>=OP_EQ && ci->ks.op<=OP_BEGINS);
			if ((out.flags&SOM_AND)!=0 && !out.append("\n\t"AND_S" ",sizeof(AND_S)+2)) return RC_NORESOURCES;
			if (ci->expr==NULL) {
				if ((rc=out.renderName(ci->ks.propID))!=RC_OK) return rc;
			} else {
				if ((ci->expr->render(SInCtx::opDscr[OP_EQ].prty[0],out))!=RC_OK) return rc;
			}
			if (ci->ks.op>=OP_ALL) return RC_CORRUPTED;
			if (!out.append(" ",1)) return RC_NORESOURCES;
			if (!out.append(SInCtx::opDscr[ci->ks.op].str,SInCtx::opDscr[ci->ks.op].lstr)) return RC_NORESOURCES;
			if (!out.append(" ",1)) return RC_NORESOURCES;
			Value v; v.setParam((byte)ci->param,(ValueType)ci->ks.type,ci->ks.flags);
			if ((rc=out.renderValue(v))!=RC_OK) return rc;
		}
		out.cvar=save; break;
	case RP_MATCH:
		if (condFT!=NULL) {
			if (!out.append("\n"MATCH_S,sizeof(MATCH_S))) return RC_NORESOURCES;
			if ((out.flags&SOM_VAR_NAMES)!=0 /*|| multiple properties */) {
				if (!out.append(" (",2)) return RC_NORESOURCES;
				if ((rc=out.renderVarName(this))!=RC_OK) return rc;		//???	+ properties?
				if (!out.append(")",1)) return RC_NORESOURCES;
			}
			if (!out.append(" "AGAINST_S" (",sizeof(AGAINST_S)+2)) return RC_NORESOURCES;
			for (CondFT *ft=condFT; ft!=NULL; ft=ft->next) if (ft->str!=NULL) {
				Value v; v.set(ft->str); if ((rc=out.renderValue(v))!=RC_OK) return rc;
				// ft-> flags ???
				if (ft->nPids>0) {
					// . A or (A, B, C)
				}
				if (!out.append(ft->next!=NULL?",":")",1)) return RC_NORESOURCES;
			}
		}
		break;
	}
	return QVar::render(part,out);
}

//------------------------------------------------------------------------------------------------------

RC ExprTree::render(const Value& v,int prty,SOutCtx& out)
{
	return v.type==VT_EXPRTREE?((ExprTree*)v.exprt)->render(prty,out):out.renderValue(v);
}

RC ExprTree::render(int prty,SOutCtx& out) const
{
	if (op==OP_CON) return render(operands[0],prty,out);
	RC rc; ExprOp o=op; if (o>=OP_ALL) return RC_CORRUPTED;
	const OpDscr *od=&SInCtx::opDscr[o]; bool f,fRange; ushort i; PathSeg pseg; const char *p;
	switch (o) {
	case OP_EXISTS:
		f=(flags&NOT_BOOLEAN_OP)!=0;
		if (nops==1 && operands[0].type==VT_STMT) {
			if (!out.append(f?" "NOT_S" EXISTS(":" EXISTS(",f?sizeof(NOT_S)+8:8)) return RC_NORESOURCES;		// any other predicate functions???
			if ((rc=((Stmt*)operands[0].stmt)->render(out))!=RC_OK) return rc;
			if (!out.append(")",1)) return RC_NORESOURCES;
		} else {
			int pr=SInCtx::opDscr[LX_IS].prty[0];
			if (prty>pr && !out.append("(",1)) return RC_NORESOURCES;
			if (nops>1) {
				//...
			} else if ((rc=render(operands[0],pr,out))!=RC_OK) return rc;
			if (!out.append(f?" IS NULL":" IS NOT NULL",f?8:12) || prty>pr && !out.append(")",1)) return RC_NORESOURCES;
		}
		break;
	case OP_IS_A:
		if (prty>od->prty[0] && !out.append("(",1)) return RC_NORESOURCES;
		if ((rc=render(operands[0],od->prty[0],out))!=RC_OK) return rc;
		f=(flags&NOT_BOOLEAN_OP)!=0;
		if (!out.append(f?" IS NOT A ":" IS A ",f?10:6)) return RC_NORESOURCES;
		if ((rc=operands[1].type==VT_URIID?out.renderName(operands[1].uid):out.renderValue(operands[1]))!=RC_OK) return rc;
		if (nops>2) {
			for (i=2,prty=SInCtx::opDscr[LX_COMMA].prty[0]; i<nops; i++) {
				if (!out.append(i==2?"(":",",1)) return RC_NORESOURCES;
				if ((rc=render(operands[i],prty,out))!=RC_OK) return rc;
			}
			if (!out.append(")",1)) return RC_NORESOURCES;
		}
		if (prty>od->prty[0] && !out.append(")",1)) return RC_NORESOURCES;
		break;
	case OP_IN:
		if (prty>od->prty[0] && !out.append("(",1)) return RC_NORESOURCES;
		f=(flags&NOT_BOOLEAN_OP)!=0; fRange=operands[1].type==VT_RANGE;
		if (fRange && operands[1].varray[0].type!=VT_ANY && operands[1].varray[1].type!=VT_ANY
										|| operands[1].type==VT_EXPRTREE && operands[1].exprt->getOp()==OP_RANGE) {
			if ((rc=render(operands[0],SInCtx::opDscr[LX_BETWEEN].prty[0],out))!=RC_OK) return rc;
			if (!out.append(f?" NOT "BETWEEN_S" ":" "BETWEEN_S" ",f?sizeof(BETWEEN_S)+5:sizeof(BETWEEN_S)+1)) return RC_NORESOURCES;
			if (operands[1].type==VT_EXPRTREE) {
				if ((rc=render(operands[1].exprt->getOperand(0),SInCtx::opDscr[LX_BAND].prty[0],out))!=RC_OK) return rc;
			} else if ((rc=out.renderValue(operands[1].varray[0]))!=RC_OK) return rc;
			if (!out.append(" "AND_S" ",sizeof(AND_S)+1)) return RC_NORESOURCES;
			if (operands[1].type==VT_EXPRTREE) {
				if ((rc=render(operands[1].exprt->getOperand(1),SInCtx::opDscr[LX_BAND].prty[0],out))!=RC_OK) return rc;
			} else if ((rc=out.renderValue(operands[1].varray[1]))!=RC_OK) return rc;
		} else {
			if ((rc=render(operands[0],od->prty[0],out))!=RC_OK) return rc;
			if (!out.append(f?" NOT IN ":" IN ",f?8:4) || !fRange && !out.append("(",1)) return RC_NORESOURCES;
			if (fRange) {
				if ((rc=out.renderValue(operands[1]))!=RC_OK) return rc;
			} else if (operands[1].type==VT_ARRAY) for (uint32_t i=0; i<operands[1].length; i++) {
				if (i!=0 && !out.append(",",1)) return RC_NORESOURCES;
				if ((rc=render(operands[1].varray[i],SInCtx::opDscr[LX_COMMA].prty[0],out))!=RC_OK) return rc;
			} else if (operands[1].type==VT_EXPRTREE && operands[1].exprt->getOp()==OP_ARRAY) {
				// linearize, if multiple levels of OP_ARRAY
				ExprTree *et=(ExprTree*)operands[1].exprt;
				for (unsigned i=0; i<et->nops; i++) {
					if (i!=0 && !out.append(",",1)) return RC_NORESOURCES;
					if ((rc=render(et->operands[i],SInCtx::opDscr[LX_COMMA].prty[0],out))!=RC_OK) return rc;
				}
			} else if (operands[1].type!=VT_STMT) return RC_CORRUPTED;
			else if ((rc=((Stmt*)operands[1].stmt)->render(out))!=RC_OK) return rc;
			if (!fRange && !out.append(")",1)) return RC_NORESOURCES;
		}
		if (prty>od->prty[0] && !out.append(")",1)) return RC_NORESOURCES;
		break;
	case OP_CAST:
		if (operands[1].type!=VT_INT && operands[1].type!=VT_UINT || operands[1].i<=VT_ERROR) return RC_CORRUPTED;
		if (!out.append("CAST(",5)) return RC_NORESOURCES;
		if ((rc=render(operands[0],prty,out))!=RC_OK) return rc;
		if (!out.append(" "AS_S" ",sizeof(AS_S)+1)) return RC_NORESOURCES;
		if (operands[1].i<VT_ALL) {if (!out.append(typeName[operands[1].i].s,typeName[operands[1].i].l)) return RC_NORESOURCES;}
		else if ((operands[1].i&0xFF)!=VT_DOUBLE && (operands[1].i&0xFF)!=VT_FLOAT || (p=getUnitName((Units)byte(operands[1].i>>8)))==NULL) return RC_CORRUPTED;
		else if (!out.append(p,strlen(p))) return RC_NORESOURCES;
		if (!out.append(")",1)) return RC_NORESOURCES;
		break;
	case OP_TRIM:
		if (!out.append("TRIM(",5)) return RC_NORESOURCES;
		if (operands[2].type!=VT_INT && operands[2].type!=VT_UINT || operands[2].ui>2) return RC_CORRUPTED;
		if (operands[2].ui==1 && !out.append("LEADING ",8)) return RC_NORESOURCES;
		else if (operands[2].ui==2 && !out.append("TRAILING ",9)) return RC_NORESOURCES;
		if ((rc=render(operands[1],prty,out))!=RC_OK) return rc;
		if (!out.append(" "FROM_S" ",sizeof(FROM_S)+1)) return RC_NORESOURCES;
		if ((rc=render(operands[0],prty,out))!=RC_OK) return rc;
		if (!out.append(")",1)) return RC_NORESOURCES;
		break;
	case OP_EXTRACT:
		if (!out.append(od->str,od->lstr) || !out.append("(",1)) return RC_NORESOURCES;
		if ((operands[1].type==VT_INT || operands[1].type==VT_UINT) && operands[1].ui<sizeof(extractWhat)/sizeof(extractWhat[0])) {
			if (!out.append(extractWhat[operands[1].ui].s,extractWhat[operands[1].ui].l)) return RC_NORESOURCES;
		} else if ((rc=render(operands[1],SInCtx::opDscr[LX_COMMA].prty[0],out))!=RC_OK) return rc;
		if (!out.append(" "FROM_S" ",sizeof(FROM_S)+1)) return RC_NORESOURCES;
		if ((rc=render(operands[0],SInCtx::opDscr[LX_COMMA].prty[0],out))!=RC_OK) return rc;
		if (!out.append(")",1)) return RC_NORESOURCES;
		break;
	case OP_CALL:
		if ((rc=render(operands[0],od->prty[0],out))!=RC_OK) return rc;
		if (!out.append("(",1)) return RC_NORESOURCES;
		for (i=1,prty=SInCtx::opDscr[LX_COMMA].prty[0]; i<nops; i++) {
			if (i>1 && !out.append(", ",2)) return RC_NORESOURCES;
			if ((rc=render(operands[i],prty,out))!=RC_OK) return rc;
		}
		if (!out.append(")",1)) return RC_NORESOURCES;
		break;
	case OP_PATH:
		if ((rc=render(operands[0],od->prty[0],out))!=RC_OK) return rc;
		if ((rc=toPathSeg(pseg,out.getSession()))!=RC_OK) return rc;
		rc=out.renderPath(pseg); if (pseg.filter!=NULL) pseg.filter->destroy();
		if (rc!=RC_OK) return rc;
		break;
	case OP_PIN: if (!out.append("@",1)) return RC_NORESOURCES;
	case OP_STRUCT:
		if (!out.append("{",1)) return RC_NORESOURCES;
		for (i=0; i<nops; i++) {
			if ((rc=out.renderName(operands[i].property))!=RC_OK) return rc;
			if (!out.append("=",1)) return RC_NORESOURCES; 
			if ((rc=out.renderValue(operands[i]))!=RC_OK) break;
			if (!out.append(i+1==nops?"}":",",1)) return RC_NORESOURCES;
		}
		break;
	case OP_CONCAT: if (nops==1) od=&SInCtx::opDscr[o=ExprOp(LX_CONCAT)];
	default:
		if (od->str==NULL) return RC_CORRUPTED;
		if ((od->flags&_F)!=0) {
			if (!out.append(od->str,od->lstr) || !out.append("(",1)) return RC_NORESOURCES;
			if ((od->flags&_A)!=0 && (flags&DISTINCT_OP)!=0 && !out.append(DISTINCT_S" ",sizeof(DISTINCT_S))) return RC_NORESOURCES;
			prty=SInCtx::opDscr[LX_COMMA].prty[0];
			for (ulong i=0; i<nops; i++) {
				if ((rc=render(operands[i],prty,out))!=RC_OK) return rc;
				if (!out.append(i+1==nops?")":",",1)) return RC_NORESOURCES;
			}
		} else {
			ulong idx=0;
			if (prty>od->prty[0] && !out.append("(",1)) return RC_NORESOURCES;
			if ((od->flags&_U)==0) {
				if (o==OP_RANGE) {if (!out.append("[",1)) return RC_NORESOURCES;}
				else if (o==OP_ARRAY) {if (!out.append("{",1)) return RC_NORESOURCES;}
				if ((rc=render(operands[idx++],od->prty[0],out))!=RC_OK) return rc;
				if (!out.append(" ",1)) return RC_NORESOURCES;
			} else if (nops!=1) return RC_CORRUPTED;
			// other flags!!!
			if (o==OP_RSHIFT) {if ((flags&UNSIGNED_OP)!=0) od=&SInCtx::opDscr[LX_URSHIFT];}
			else if ((flags&NOT_BOOLEAN_OP)!=0 && !out.append(NOT_S" ",sizeof(NOT_S))) return RC_NORESOURCES;
			do {
				if (!out.append(od->str,od->lstr) || !out.append(" ",1)) return RC_NORESOURCES;
				if ((rc=render(operands[idx],od->prty[0],out))!=RC_OK) return rc;
			} while (++idx<nops);
			if (o==OP_RANGE) {if (!out.append("]",1)) return RC_NORESOURCES;}
			else if (o==OP_ARRAY) {if (!out.append("}",1)) return RC_NORESOURCES;}
			if (prty>od->prty[0] && !out.append(")",1)) return RC_NORESOURCES;
		}
		break;
	}
	return RC_OK;
}

RC ExprTree::toPathSeg(PathSeg& ps,MemAlloc *ma) const
{
	if (operands[1].type==VT_URIID) ps.pid=operands[1].uid;
	else {
		ps.pid=STORE_INVALID_PROPID;
		//???
	}
	ps.eid=STORE_COLLECTION_ID; ps.filter=NULL; ps.cid=STORE_INVALID_CLASSID; ps.fLast=(flags&FILTER_LAST_OP)!=0;
	switch (flags&(QUEST_PATH_OP|PLUS_PATH_OP|STAR_PATH_OP)) {
	default: ps.rmin=ps.rmax=1; break;
	case QUEST_PATH_OP: ps.rmin=0; ps.rmax=1; break;
	case PLUS_PATH_OP: ps.rmin=1; ps.rmax=~0u; break;
	case STAR_PATH_OP: ps.rmin=0; ps.rmax=~0u; break;
	}
	if (nops>2) {
		switch (operands[2].type) {
		case VT_ERROR: break;
		case VT_INT: case VT_UINT: ps.eid=operands[2].ui; break;
		case VT_EXPRTREE:
			if (isBool(((ExprTree*)operands[2].exprt)->op)) {
				// classID ?
				Expr *exp=NULL; RC rc;
				if ((rc=Expr::compile((ExprTree*)operands[2].exprt,exp,ma,true))!=RC_OK) return rc;
				ps.filter=exp; break;
			}
		default:
			//??? error
			break;
		}
		if (nops>3) switch (operands[3].type) {
		case VT_UINT: ps.rmin=operands[3].ui&0xFFFF; ps.rmax=operands[3].ui>>16; break;
		case VT_VARREF: if ((operands[3].refV.flags&VAR_TYPE_MASK)==VAR_PARAM) ps.rmax=operands[3].refV.refN|0x8000; break;
		default:
			// error???
			break;
		}
	}
	return RC_OK;
}

RC Expr::render(int prty,SOutCtx& out) const
{
	RC rc=RC_OK;
	if ((hdr.flags&EXPR_NO_CODE)!=0) {
		const VarHdr *vh=(VarHdr*)(&hdr+1);
		if (vh->var==0) {
			const bool fPar=vh->nProps>1 && prty>SInCtx::opDscr[OP_LAND].prty[0];
			if (fPar && !out.append("(",1)) return RC_NORESOURCES;
			const char *exstr=SInCtx::opDscr[OP_EXISTS].str; size_t lExist=strlen(exstr);
			for	(unsigned i=0; i<vh->nProps; i++) if ((((uint32_t*)(vh+1))[i]&PROP_OPTIONAL)==0) {
				if (i!=0 && !out.append(" "AND_S" ",sizeof(AND_S)+1)) return RC_NORESOURCES;
				if (!out.append(exstr,lExist)||!out.append("(",1)) return RC_NORESOURCES;
				if ((rc=out.renderName(((uint32_t*)(vh+1))[i]&STORE_MAX_URIID,NULL))!=RC_OK) return rc;	// var?
				if (!out.append(")",1)) return RC_NORESOURCES;
			}
			if (fPar && !out.append(")",1)) return RC_NORESOURCES;
		}
	} else {
		ExprTree *et=NULL; rc=decompile(et,out.getSession());
		if (rc==RC_OK) {rc=et->render(prty,out); et->destroy();}
	}
	return rc;
}

bool SOutCtx::expand(size_t len)
{
	size_t d=xLen==0?256:xLen/2; if (d<len) d=len; return (ptr=(byte*)ses->realloc(ptr,xLen+=d))!=NULL;
}

char *SOutCtx::renderAll()
{
	if ((mode&TOS_PROLOGUE)==0 || usedQNames==NULL && (flags&SOM_STD_PREFIX)==0) return (char*)*this;
	size_t l=cLen; char *p=(char*)*this; cLen=xLen=0; unsigned skip=~0u,idx;
	if ((flags&SOM_BASE_USED)!=0) {
		if (!append(BASE_S" ",sizeof(BASE_S))) {ses->free(p); return NULL;}
		const QName *qn=NULL;
		for (unsigned i=0; i<nUsedQNames; i++) 
			if ((idx=usedQNames[i])==~0u || (qn=(idx&0x80000000)==0?qNames:ses->qNames)[idx&~0x80000000].qpref==NULL) {skip=i; break;}
		if (!append("'",1) || !(qn==NULL?append(ses->URIBase,ses->lURIBase):append(qn->str,qn->lstr)) || !append("'\n",2)) {ses->free(p); return NULL;}
	}
	for (unsigned i=0; i<nUsedQNames; i++) if (i!=skip) {
		idx=usedQNames[i]; assert(idx!=~0u); const QName *qn=&((idx&0x80000000)==0?qNames:ses->qNames)[idx&~0x80000000];
		if (!append(PREFIX_S" ",sizeof(PREFIX_S)) || !append(qn->qpref,qn->lq) || !append(QNAME_DELIM" '",sizeof(QNAME_DELIM)+1)
			|| !append(qn->str,qn->lstr) || !append("'\n",2)) {ses->free(p); return NULL;}
	}
	if ((p=(char*)ses->realloc(p,cLen+l+1))!=NULL) {memmove(p+cLen,p,l+1); memcpy(p,(char*)*this,cLen);}
	return p;
}

RC SOutCtx::renderName(uint32_t id,const char *prefix,const QVar *qv,bool fQ,bool fEsc) {
	URI *uri=NULL; RC rc=RC_OK;
	if ((flags&SOM_VAR_NAMES)!=0 && qv!=NULL) {
		if ((rc=renderVarName(qv))!=RC_OK) return rc;
		prefix=URIID_PREFIX;
	}
	if (prefix!=NULL && !append(prefix,1)) return RC_NORESOURCES;
	if (id<=PROP_SPEC_LAST && !ses->fStdOvr) {
		if ((mode&TOS_NO_QNAMES)==0) {
			if (append(STORE_STD_QPREFIX,sizeof(STORE_STD_QPREFIX)-1)) flags|=SOM_STD_PREFIX; else return RC_NORESOURCES;
		} else {
			if (!append(STORE_STD_URI_PREFIX,sizeof(STORE_STD_URI_PREFIX)-1)) return RC_NORESOURCES;
		}
		if (!append(Classifier::builtinURIs[id].name,Classifier::builtinURIs[id].lname)) rc=RC_NORESOURCES;
	} else if ((uri=(URI*)ses->getStore()->uriMgr->ObjMgr::find(id))==NULL) rc=RC_NOTFOUND;		// .09090900 ???
	else {
		const char *s=uri->getURI(); assert(s!=NULL); const URIInfo ui=uri->getInfo(); size_t l=strlen(s);
		if ((ui.flags&UID_ID)!=0) {if (!append(s,l)) rc=RC_NORESOURCES;}
		else {
			if ((mode&TOS_NO_QNAMES)==0 && (ui.flags&UID_IRI)!=0 && l>ui.lSuffix) {
				bool fQName=false; unsigned idx=~0u; size_t lpref=l-ui.lSuffix,lqp=0; const char *qp=NULL; const QName *qn;
				const static char *stdQPref=STORE_STD_QPREFIX,*stdPref=STORE_STD_URI_PREFIX;
				for (unsigned i=0; i<nUsedQNames; i++) {
					if ((idx=usedQNames[i])==~1u) {
						if (fQName=lpref==sizeof(STORE_STD_URI_PREFIX)-1 && memcmp(s,stdPref,lpref)==0) {qp=stdQPref; lqp=sizeof(STORE_STD_QPREFIX)-2;}
					} else if (idx==~0u) {
						assert(ses->URIBase!=NULL && ses->lURIBase!=0);
						fQName=lpref==ses->lURIBase && memcmp(s,ses->URIBase,lpref)==0;
					} else {
						qn=(idx&0x80000000)==0?&qNames[idx]:&ses->qNames[idx&~0x80000000];
						if (fQName=lpref==qn->lstr && memcmp(s,qn->str,lpref)==0) {qp=qn->qpref; lqp=qn->lq;}
					}
					if (fQName) {if (i!=0) memmove(usedQNames+1,usedQNames,i*sizeof(unsigned)); break;}
				}
				if (!fQName) {
					idx=~0u;
					if ((qn=qNames)!=NULL) for (unsigned i=0; i<nQNames; ++i,++qn)
						if (fQName=lpref==qn->lstr && memcmp(s,qn->str,lpref)==0) {qp=qn->qpref; lqp=qn->lq; idx=i; break;}
					if (!fQName && (qn=ses->qNames)!=NULL) for (unsigned i=0; i<ses->nQNames; ++i,++qn)
						if (fQName=lpref==qn->lstr && memcmp(s,qn->str,lpref)==0) {qp=qn->qpref; lqp=qn->lq; idx=i|0x80000000; break;}
					if (!fQName && !ses->fStdOvr && lpref==sizeof(STORE_STD_URI_PREFIX)-1 && !memcmp(s,stdPref,lpref))
						{fQName=true; qp=stdQPref; lqp=sizeof(STORE_STD_QPREFIX)-2; idx=~1u;}
					if (fQName || (fQName=ses->URIBase!=NULL && ses->lURIBase!=0 && lpref==ses->lURIBase && memcmp(s,ses->URIBase,lpref)==0)) {
						if ((usedQNames=(unsigned*)ses->realloc(usedQNames,(nUsedQNames+1)*sizeof(unsigned)))==NULL) return RC_NORESOURCES;
						if (nUsedQNames!=0) memmove(usedQNames+1,usedQNames,nUsedQNames*sizeof(unsigned)); nUsedQNames++;
					}
				}
				if (fQName) {
					usedQNames[0]=idx; s+=lpref; l=ui.lSuffix; fEsc=fEsc&&(ui.flags&UID_SID)==0;
					if (qp==NULL || lqp==0) flags|=SOM_BASE_USED; else if (!append(qp,lqp) || !append(QNAME_DELIM,1)) return RC_NORESOURCES;
				}
			}
			if (!fQ && fEsc && !append("\"",1)) return RC_NORESOURCES;
			if ((ui.flags&UID_DQU)!=0) do {
				const char *p=(char*)memchr(s,'"',l);
				if (p==NULL) {if (!append(s,l)) return RC_NORESOURCES; break;}
				if (p>s) {size_t ls=p-s; if (!append(s,ls)) return RC_NORESOURCES; s=p; l-=ls;}
				if (!append(fQ?"\\\"":"\"\"",2)) return RC_NORESOURCES;
			} while ((++s,--l)!=0);
			else if (!append(s,l)) return RC_NORESOURCES;
			if (!fQ && fEsc && !append("\"",1)) return RC_NORESOURCES;
		}
		uri->release();
	}
	return rc;
}

RC SOutCtx::renderPID(const PID& id,bool fJ) {
	size_t l=sprintf(cbuf,fJ?"{\"$ref\":\""_LX_FM"\"}":PID_PREFIX _LX_FM,id.pid);
	if (!append(cbuf,l))  return RC_NORESOURCES;
	if (!fJ && id.ident!=STORE_OWNER) {
		Identity *ident=(Identity*)ses->getStore()->identMgr->ObjMgr::find(id.ident); if (ident==NULL) return RC_NOTFOUND;
		const char *s=ident->getName(); bool f=append(IDENTITY_PREFIX,1) && append(s,strlen(s));
		ident->release(); if (!f) return RC_NORESOURCES;
	}
	return RC_OK;
}

RC SOutCtx::renderValue(const Value& v,JRType jr) {
	RC rc=RC_OK; unsigned long u; Identity *ident; const char *s; size_t l; const Value *cv; int ll; const QVar *qv; Value vv;
	if (jr!=JR_NO) {
		if (v.type>VT_DOUBLE && !isString((ValueType)v.type) && v.type!=VT_ARRAY && v.type!=VT_COLLECTION && v.type!=VT_STRUCT && v.type!=VT_REFID)
			{if ((rc=convV(v,vv,VT_STRING,ses,CV_NODEREF))==RC_OK) {vv.setPropID(v.property); rc=renderValue(vv,jr); freeV(vv);} return rc;}
		if (jr==JR_EID) {l=sprintf(cbuf,"\"%d\": ",v.eid); if (!append(cbuf,l)) return RC_NORESOURCES;}
		else if (!append("\"",1)) return RC_NORESOURCES;
		else if ((rc=renderName(v.property!=STORE_INVALID_PROPID?v.property:PROP_SPEC_VALUE,NULL,NULL,true))!=RC_OK) return rc;
		else if (!append("\": ",3)) return RC_NORESOURCES;
	}
	switch (v.type) {
	case VT_ANY: if (!append(NULL_S,sizeof(NULL_S)-1)) return RC_NORESOURCES; break;
	case VT_URL: if (jr==JR_NO && !append("U",1)) return RC_NORESOURCES;
	case VT_STRING:
		if (!append(jr!=JR_NO?"\"":"'",1)) return RC_NORESOURCES;
		if (v.length!=0) {
			s=v.str; l=v.length;
			if (jr!=JR_NO && (memchr(s,'\n',l)!=NULL||memchr(s,'\t',l)!=NULL)) for (size_t i=0; i<l; ++i,++s) {
				const char *p; size_t ll;
				if (*s=='\n') p="\\n",ll=2; else if (*s=='\n') p="\\t",ll=2; else if (*s=='"') p="\\\"",ll=2; else p=s,ll=1;
				if (!append(p,ll)) return RC_NORESOURCES;
			} else do {
				const char *p=(char*)memchr(s,jr!=JR_NO?'"':'\'',l);
				if (p==NULL) {if (!append(s,l)) return RC_NORESOURCES; break;}
				if (p>s) {size_t ls=p-s; if (!append(s,ls)) return RC_NORESOURCES; s=p; l-=ls;}
				if (!append(jr!=JR_NO?"\\\"":"''",2)) return RC_NORESOURCES;
			} while ((++s,--l)!=0);
		}
		if (!append(jr!=JR_NO?"\"":"'",1)) return RC_NORESOURCES;
		break;
	case VT_BSTR:
		if (jr!=JR_NO && !append("\"",1) || !append("X'",2)) return RC_NORESOURCES;
		for (u=0; u<v.length; u++) {sprintf(cbuf,"%02X",v.bstr[u]); if (!append(cbuf,2)) return RC_NORESOURCES;}
		if (!append("'",1) || jr!=JR_NO && !append("\"",1)) return RC_NORESOURCES;
		break;
	case VT_STREAM:
		//???
		break;
	case VT_INT: l=sprintf(cbuf,"%d",v.i); if (!append(cbuf,l)) return RC_NORESOURCES; break;
	case VT_UINT: l=sprintf(cbuf,"%u%c",v.ui,jr!=JR_NO?' ':'u'); if (!append(cbuf,l)) return RC_NORESOURCES; break;
	case VT_INT64: 
		l=sprintf(cbuf,_LD_FM,v.i64); if (!append(cbuf,l)) return RC_NORESOURCES; break;
	case VT_UINT64:
		l=sprintf(cbuf,_LU_FM"%c",v.ui64,jr!=JR_NO?' ':'U'); if (!append(cbuf,l)) return RC_NORESOURCES;
		break;
	case VT_DATETIME:
		if (!append(TIMESTAMP_S" '", sizeof(TIMESTAMP_S)+1)) return RC_NORESOURCES;
		if ((rc=convDateTime(ses,v.ui64,cbuf,ll))!=RC_OK) break;
		if (!append(cbuf,ll) || !append("'",1)) return RC_NORESOURCES;
		break;
	case VT_INTERVAL:
		if (!append(INTERVAL_S" '", sizeof(INTERVAL_S)+1)) return RC_NORESOURCES;
		if ((rc=convInterval(v.i64,cbuf,ll))!=RC_OK) break;
		if (!append(cbuf,ll) || !append("'",1)) return RC_NORESOURCES;
		break;
	case VT_BOOL: if (!append(v.b?TRUE_S:FALSE_S,v.b?sizeof(TRUE_S)-1:sizeof(FALSE_S)-1)) return RC_NORESOURCES; break;
	case VT_FLOAT: l=sprintf(cbuf,"%g%c",v.f,jr!=JR_NO?' ':'f'); if (!append(cbuf,l)) return RC_NORESOURCES; break;
	case VT_DOUBLE: l=sprintf(cbuf,"%g",v.d); if (!append(cbuf,l)) return RC_NORESOURCES; break;
	case VT_CURRENT:
		switch (v.i) {
		default:	//???
		case CVT_TIMESTAMP: if (!append(CURRENT_TIMESTAMP_S,sizeof(CURRENT_TIMESTAMP_S)-1)) return RC_NORESOURCES; break;
		case CVT_USER: if (!append(CURRENT_USER_S,sizeof(CURRENT_USER_S)-1)) return RC_NORESOURCES; break;
		case CVT_STORE: if (!append(CURRENT_STORE_S,sizeof(CURRENT_STORE_S)-1)) return RC_NORESOURCES; break;
		}
		break;
	case VT_ARRAY:
		if (!append("{",1)) return RC_NORESOURCES;
		for (u=0; u<v.length; u++) {
			if ((rc=renderValue(v.varray[u],jr!=JR_NO?JR_EID:JR_NO))!=RC_OK) break;
			if (!append(u+1==v.length?"}":",",1)) return RC_NORESOURCES;
		}
		break;
	case VT_COLLECTION:
		if (!append("{",1)) return RC_NORESOURCES;
		for (cv=v.nav->navigate(GO_FIRST),u=0; cv!=NULL; cv=v.nav->navigate(GO_NEXT),++u) {
			if (u!=0 && !append(",",1)) return RC_NORESOURCES;
			if ((rc=renderValue(*cv,jr!=JR_NO?JR_EID:JR_NO))!=RC_OK) break;
		}
		v.nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID);
		if (!append("}",1)) return RC_NORESOURCES;
		break;
	case VT_STRUCT:
		if (!append("{",1)) return RC_NORESOURCES;
		for (u=0; u<v.length; u++) {
			if (jr==JR_NO) {
				if ((rc=renderName(v.varray[u].property))!=RC_OK) return rc;
				if (!append("=",1)) return RC_NORESOURCES;
			}
			if ((rc=renderValue(v.varray[u],jr!=JR_NO?JR_PROP:JR_NO))!=RC_OK) break;
			if (!append(u+1==v.length?"}":",",1)) return RC_NORESOURCES;
		}
		break;
	case VT_RANGE:
		if (!append("[",1)) return RC_NORESOURCES;
		if (v.range[0].type==VT_ANY) {if (!append("*",1)) return RC_NORESOURCES;} else if ((rc=renderValue(v.range[0]))!=RC_OK) break;
		if (!append(",",1)) return RC_NORESOURCES;
		if (v.range[1].type==VT_ANY) {if (!append("*",1)) return RC_NORESOURCES;} else if ((rc=renderValue(v.range[1]))!=RC_OK) break;
		if (!append("]",1)) return RC_NORESOURCES;
		break;
	case VT_URIID:
		rc=renderName(v.uid,URIID_PREFIX); break;
	case VT_IDENTITY:
		ident=(Identity*)ses->getStore()->identMgr->ObjMgr::find(v.iid);
		if (ident==NULL) rc=RC_NOTFOUND;
		else {
			s=ident->getName(); assert(s!=NULL);
			rc=append(IDENTITY_PREFIX,1) && append(s,strlen(s))?RC_OK:RC_NORESOURCES;
			ident->release();
		}
		break;
	case VT_REFID:
		rc=renderPID(v.id,jr!=JR_NO); break;
	case VT_REFIDPROP:
	case VT_REFIDELT:
		if ((rc=renderPID(v.refId->id))!=RC_OK) break;
		if ((rc=renderName(v.refId->pid,URIID_PREFIX))!=RC_OK) break;
		if (v.type==VT_REFIDELT) rc=renderElement(v.refId->eid,*this);
		break;
	case VT_EXPRTREE:
		if (!append("$(",2) || (rc=((ExprTree*)v.exprt)->render(0,*this))==RC_OK && !append(")",1)) rc=RC_NORESOURCES;
		break;
	case VT_EXPR:
		if ((v.meta&META_PROP_EVAL)==0 && !append("$(",2) || (rc=((Expr*)v.expr)->render(0,*this))==RC_OK && (v.meta&META_PROP_EVAL)==0 && !append(")",1)) rc=RC_NORESOURCES;
		break;
	case VT_STMT:
		if ((v.meta&META_PROP_EVAL)==0 && !append("${",2) || (rc=((Stmt*)v.stmt)->render(*this))==RC_OK && (v.meta&META_PROP_EVAL)==0 && !append("}",1)) rc=RC_NORESOURCES;
		break;
	case VT_VARREF:
		switch (v.refV.flags&VAR_TYPE_MASK) {
		case 0:
			qv=NULL;
			if (v.length==0 && (cvar==NULL || cvar->getType()>=QRY_UNION)) return v.refV.refN!=0?RC_CORRUPTED:append("@",1)?RC_OK:RC_NORESOURCES;
			if (v.length==0 || (flags&SOM_VAR_NAMES)!=0) {
				if ((qv=cvar!=NULL?cvar->getRefVar(v.refV.refN):(QVar*)0)==NULL) return RC_CORRUPTED;
				if (v.length==0) return (rc=renderVarName(qv))!=RC_OK||append(".*",2)?rc:RC_NORESOURCES;
			}
			if (v.length!=0 && (rc=renderName(v.refV.id,NULL,qv))==RC_OK) rc=renderElement(v.eid,*this);
			break;
		case VAR_PARAM:
			l=sprintf(cbuf,PARAM_PREFIX"%d",v.refV.refN); if (!append(cbuf,l)) return RC_NORESOURCES;
			if (v.refV.type<VT_ALL && v.refV.type!=VT_ANY) {
				if (!append("(",1)) return RC_NORESOURCES;
				if (!append(typeName[v.refV.type].s,typeName[v.refV.type].l)) return RC_NORESOURCES;
				if ((v.refV.flags&ORD_DESC)!=0 && !append(","DESC_S,sizeof(DESC_S))) return RC_NORESOURCES;
				if ((v.refV.flags&(ORD_NULLS_BEFORE|ORD_NULLS_AFTER))!=0) {
					if (!append(","NULLS_S" ",sizeof(NULLS_S)+1)) return RC_NORESOURCES;
					if ((v.refV.flags&ORD_NULLS_BEFORE)!=0) {if (!append(FIRST_S,sizeof(FIRST_S)-1)) return RC_NORESOURCES;}
					else if (!append(LAST_S,sizeof(LAST_S)-1)) return RC_NORESOURCES;
				}
				if (!append(")",1)) return RC_NORESOURCES;
			}
			break;
		case VAR_AGGS: case VAR_GROUP:
			if (cvar!=NULL) {
				if ((v.refV.flags&VAR_TYPE_MASK)==VAR_AGGS) {
					if (cvar->aggrs.vals!=NULL && v.refV.refN<cvar->aggrs.nValues) {
						const Value &ag=cvar->aggrs.vals[v.refV.refN]; assert(ag.op<OP_ALL && (SInCtx::opDscr[ag.op].flags&_A)!=0);
						if (!append(SInCtx::opDscr[ag.op].str,SInCtx::opDscr[ag.op].lstr) || !append("(",1)) return RC_NORESOURCES;
						return (rc=renderValue(ag))!=RC_OK||append(")",1)?rc:RC_NORESOURCES;
					}
				} else if (cvar->groupBy!=NULL && v.refV.refN<cvar->nGroupBy) return renderOrder(&cvar->groupBy[v.refV.refN],1,*this,true);
			}
		default: rc=RC_NOTFOUND; break;
		}
		break;
	case VT_RESERVED1:
	case VT_RESERVED2:
		//???
		break;
	}
	return rc;
}

RC SOutCtx::renderPath(const PathSeg& ps)
{
	RC rc; if ((rc=renderName(ps.pid,PATH_DELIM))!=RC_OK) return rc;
	const bool fRep=ps.rmin!=1||ps.rmax!=1;
	if (fRep && ps.fLast && (rc=renderRep(ps.rmin,ps.rmax))!=RC_OK) return rc;
	if (ps.filter!=NULL /*|| ps.cls!=STORE_INVALID_CLASSID*/) {
		if (!append("[",1)) return RC_NORESOURCES;
		if ((rc=((Expr*)ps.filter)->render(0,*this))!=RC_OK) return rc;
		if (!append("]",1)) return RC_NORESOURCES;
	} else if ((rc=renderElement(ps.eid,*this))!=RC_OK) return rc;
	return fRep && !ps.fLast ? renderRep(ps.rmin,ps.rmax) : RC_OK;
}

RC SOutCtx::renderRep(unsigned rm,unsigned rx)
{
	if (rm==0 && rx==1) {if (!append("{?}",3)) return RC_NORESOURCES;}
	else if (rm==0 && rx==~0u) {if (!append("{*}",3)) return RC_NORESOURCES;}
	else if (rm==1 && rx==~0u) {if (!append("{+}",3)) return RC_NORESOURCES;}
	else {
		// params???
		char buf[30];
		if (rm==0) {if (!append("{*,",3)) return RC_NORESOURCES;}
		else {size_t l=sprintf(buf,"{%u,",rm); if (!append(buf,l)) return RC_NORESOURCES;}
		if (!append(",",1)) return RC_NORESOURCES;
		if (rx==~0u) {if (!append("*}",2)) return RC_NORESOURCES;}
		else {size_t l=sprintf(buf,"%u}",rx); if (!append(buf,l)) return RC_NORESOURCES;}
	}
	return RC_OK;
}

RC SOutCtx::renderVarName(const QVar *qv)
{
	if (qv->name!=NULL) return append(qv->name,strlen(qv->name))?RC_OK:RC_NORESOURCES;
	if (qv->type==QRY_SIMPLE && ((SimpleVar*)qv)->nClasses==1) {
		// use class name ???
	}
	char buf[10]; size_t l=sprintf(buf,"_%d",qv->id); 
	return append(buf,l)?RC_OK:RC_NORESOURCES;
}

RC SOutCtx::renderJSON(Cursor *cr,uint64_t& cnt)
{
	RC rc=RC_OK; cnt=0; size_t l=1; cbuf[0]='['; Value ret;
	while (rc==RC_OK && (rc=((Cursor*)cr)->next(ret))==RC_OK) {
		const Value *props; unsigned nProps; IPIN *next=NULL; bool fMany=false;
		switch (ret.type) {
		case VT_REF:
			if ((next=ret.pin->getSibling())!=NULL) {cbuf[l++]='['; fMany=true;}
			l+=sprintf(cbuf+l,"{\"id\":\""_LX_FM"\"",ret.pin->getPID().pid);
			props=ret.pin->getValueByIndex(0); nProps=ret.pin->getNumberOfProperties();
			if (props!=NULL && nProps!=0) {cbuf[l]=','; cbuf[l+1]=' '; l+=2;}
			goto render_props;
		case VT_STRUCT:
			cbuf[l++]='{'; props=ret.varray; nProps=ret.length;
		render_props:
			if (!append(cbuf,l)) rc=RC_NORESOURCES;
			else for (unsigned i=0; i<nProps; ++i,++props) if (props->property!=STORE_INVALID_PROPID) {
				if ((rc=renderValue(*props,JR_PROP))!=RC_OK) break;
				if (i+1<nProps && !append(", ",2)) {rc=RC_NORESOURCES; break;}
			}
			if (rc==RC_OK && !append("}",1)) rc=RC_NORESOURCES;
			else if (next!=NULL) {
				l=sprintf(cbuf,",{\"id\":\""_LX_FM"\"",next->getPID().pid);
				props=next->getValueByIndex(0); nProps=next->getNumberOfProperties();
				if (props!=NULL && nProps!=0) {cbuf[l]=','; cbuf[l+1]=' '; l+=2;}
				next=next->getSibling(); goto render_props;
			}
			break;
		default:
			cbuf[l++]='{'; 
			if (!append(cbuf,l) || (rc=renderValue(ret,JR_PROP))==RC_OK && !append("}",1)) rc=RC_NORESOURCES;
			break;
		}
		if (fMany && !append("]",1)) rc=RC_NORESOURCES;
		freeV(ret); cnt++; cbuf[0]=','; cbuf[1]='\n'; l=2;
	}
	if (rc==RC_EOF) rc=RC_OK;
	return rc!=RC_OK||cnt==0||append("]",1)?rc:RC_NORESOURCES;
}

//----------------------------------------------------------------------------------------------------------------

const TLx SInCtx::charLex[256] = 
{
	LX_EOE,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_SPACE,LX_SPACE,LX_NL,LX_ERR,LX_SPACE,LX_SPACE,LX_ERR,LX_ERR,											// 0x00 - 0x0F
	LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,												// 0x10 - 0x1F
	LX_SPACE,LX_EXCL,LX_DQUOTE,LX_HASH,LX_DOLLAR,OP_MOD,OP_AND,LX_QUOTE,LX_LPR,LX_RPR,OP_MUL,OP_PLUS,LX_COMMA,OP_MINUS,LX_PERIOD,OP_DIV,							// 0x20 = 0x2F
	LX_DIGIT,LX_DIGIT,LX_DIGIT,LX_DIGIT,LX_DIGIT,LX_DIGIT,LX_DIGIT,LX_DIGIT,LX_DIGIT,LX_DIGIT,LX_COLON,LX_SEMI,LX_LT,OP_EQ,LX_GT,LX_QUEST,							// 0x30 - 0x3F
	LX_SELF,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,	// 0x40 - 0x4F
	LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_ULETTER,LX_LETTER,LX_LETTER,LX_XLETTER,LX_LETTER,LX_LETTER,LX_LBR,LX_ERR,LX_RBR,OP_XOR,LX_LETTER,			// 0x50 - 0x5F
	LX_ERR,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,	// 0x60 - 0x6F
	LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LCBR,LX_VL,LX_RCBR,OP_NOT,LX_ERR,				// 0x70 - 0x7F
	LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,												// 0x80 - 0x8F
	LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,												// 0x90 - 0x9F
	LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,												// 0xA0 - 0xAF
	LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,												// 0xB0 - 0xBF
	LX_ERR,LX_ERR,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,									// 0xC0 - 0xCF
	LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,								// 0xD0 - 0xDF
	LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,								// 0xE0 - 0xEF
	LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_UTF8,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,											// 0xF0 - 0xFF
};

bool MVStoreKernel::testStrNum(const char *s,size_t l,Value& res)
{
	while (l!=0 && SInCtx::charLex[(byte)*s]==LX_SPACE) --l,++s;
	if (l==0) return false;
	TLx lx=SInCtx::charLex[(byte)*s];
	if (lx==LX_DIGIT && l>5) {
		// try date/interval
	}
	return (lx==LX_DIGIT || l>1 && (lx==OP_PLUS || lx==OP_MINUS)) && strToNum(s,l,res)==RC_OK;
}

KWNode *SInCtx::kwt['_'-'A'+1] = {NULL};
size_t SInCtx::kwTabSize=sizeof(kwt);

void SInCtx::initKW()
{
	const static struct KWTab {
		const char	*s;
		KW			kw;
		ushort		qlang;
	} kwTab[] = {
		{SELECT_S,				KW_SELECT,				1<<SQ_SQL|1<<SQ_SPARQL},
		{FROM_S,				KW_FROM,				1<<SQ_SQL|1<<SQ_SPARQL},
		{WHERE_S,				KW_WHERE,				1<<SQ_SQL|1<<SQ_SPARQL},
		{ORDER_S,				KW_ORDER,				1<<SQ_SQL|1<<SQ_SPARQL},
		{BY_S,					KW_BY,					1<<SQ_SQL|1<<SQ_SPARQL},
		{GROUP_S,				KW_GROUP,				1<<SQ_SQL},
		{HAVING_S,				KW_HAVING,				1<<SQ_SQL},
		{JOIN_S,				KW_JOIN,				1<<SQ_SQL},
		{ON_S,					KW_ON,					1<<SQ_SQL},
		{LEFT_S,				KW_LEFT,				1<<SQ_SQL},
		{OUTER_S,				KW_OUTER,				1<<SQ_SQL},
		{RIGHT_S,				KW_RIGHT,				1<<SQ_SQL},
		{CROSS_S,				KW_CROSS,				1<<SQ_SQL},
		{INNER_S,				KW_INNER,				1<<SQ_SQL},
		{USING_S,				KW_USING,				1<<SQ_SQL},
		{BASE_S,				KW_BASE,				1<<SQ_SQL|1<<SQ_SPARQL},
		{PREFIX_S,				KW_PREFIX,				1<<SQ_SQL|1<<SQ_SPARQL},
		{UNION_S,				KW_UNION,				1<<SQ_SQL|1<<SQ_SPARQL},
		{EXCEPT_S,				KW_EXCEPT,				1<<SQ_SQL},
		{INTERSECT_S,			KW_INTERSECT,			1<<SQ_SQL},
		{DISTINCT_S,			KW_DISTINCT,			1<<SQ_SQL|1<<SQ_SPARQL},
		{VALUES_S,				KW_VALUES,				1<<SQ_SQL},
		{AS_S,					KW_AS,					1<<SQ_SQL},
		{NULLS_S,				KW_NULLS,				1<<SQ_SQL},
		{TRUE_S,				KW_TRUE,				1<<SQ_SQL|1<<SQ_SPARQL},
		{FALSE_S,				KW_FALSE,				1<<SQ_SQL|1<<SQ_SPARQL},
		{NULL_S,				KW_NULL,				1<<SQ_SQL},
		{ALL_S,					KW_ALL,					1<<SQ_SQL},
		{ANY_S,					KW_ANY,					1<<SQ_SQL},
		{SOME_S,				KW_SOME,				1<<SQ_SQL},
		{ASC_S,					KW_ASC,					1<<SQ_SQL},
		{DESC_S,				KW_DESC,				1<<SQ_SQL},
		{MATCH_S,				KW_MATCH,				1<<SQ_SQL|1<<SQ_SPARQL},
		{CURRENT_TIMESTAMP_S,	KW_NOW,					1<<SQ_SQL},
		{CURRENT_USER_S,		KW_CUSER,				1<<SQ_SQL},
		{CURRENT_STORE_S,		KW_CSTORE,				1<<SQ_SQL},
		{TIMESTAMP_S,			KW_TIMESTAMP,			1<<SQ_SQL},
		{INTERVAL_S,			KW_INTERVAL,			1<<SQ_SQL},
		{WITH_S,				KW_WITH,				1<<SQ_SQL},
		{INSERT_S,				KW_INSERT,				1<<SQ_SQL},
		{DELETE_S,				KW_DELETE,				1<<SQ_SQL},
		{UPDATE_S,				KW_UPDATE,				1<<SQ_SQL},
		{CREATE_S,				KW_CREATE,				1<<SQ_SQL},
		{PURGE_S,				KW_PURGE,				1<<SQ_SQL},
		{UNDELETE_S,			KW_UNDELETE,			1<<SQ_SQL},
		{SET_S,					KW_SET,					1<<SQ_SQL},
		{ADD_S,					KW_ADD,					1<<SQ_SQL},
		{MOVE_S,				KW_MOVE,				1<<SQ_SQL},
		{RENAME_S,				KW_RENAME,				1<<SQ_SQL},
		{EDIT_S,				KW_EDIT,				1<<SQ_SQL},
		{CLASS_S,				KW_CLASS,				1<<SQ_SQL},
		{START_S,				KW_START,				1<<SQ_SQL},
		{COMMIT_S,				KW_COMMIT,				1<<SQ_SQL},
		{ROLLBACK_S,			KW_ROLLBACK,			1<<SQ_SQL},
		{DROP_S,				KW_DROP,				1<<SQ_SQL},
	};
	static volatile long fInit=-1;
	while (fInit!=0) {
		if (!cas(&fInit,-1L,1L)) threadYield();
		else {
			for (unsigned i=0; i<sizeof(kwTab)/sizeof(kwTab[0]); i++)
				initKWEntry(kwTab[i].s,LX_KEYW,kwTab[i].kw,kwTab[i].qlang);
			for (unsigned i=0; i<sizeof(opDscr)/sizeof(opDscr[0]); i++) {
				const OpDscr& od=opDscr[i];
				if (od.str!=NULL && byte(*od.str-'A')<=byte('Z'-'A'))
					initKWEntry(od.str,(TLx)i,KW_OP,1<<SQ_SQL|1<<SQ_SPARQL);
			}
			fInit=0;
		}
	}
}

void SInCtx::initKWEntry(const char *str,TLx lx,KW kw,ushort ql)
{
	assert(byte(*str-'A')<=byte('_'-'A'));
	KWNode **ppn=&kwt[*str++-'A'];
	for (;;) {
		KWNode *node=*ppn; byte ch;
		if (node==NULL) {
			*ppn=node=(KWNode*)::malloc(sizeof(KWNode)); if (node==NULL) throw RC_NORESOURCES;
			node->lx=LX_ERR; node->mch=0; node->nch=0; node->nodes[0]=NULL; kwTabSize+=sizeof(KWNode);
		}
		if ((ch=*str++)=='\0') {assert(node->lx==LX_ERR); node->lx=lx; node->kw=kw; node->ql=ql; return;}
		assert(byte(ch-'A')<=byte('_'-'A'));
		if (node->nch==0) {node->mch=ch; node->nch=1;}
		else if (byte(ch-node->mch)>=node->nch) {
			byte delta=ch<node->mch?node->mch-ch:ch-(node->mch+node->nch-1);
			*ppn=node=(KWNode*)::realloc(node,sizeof(KWNode)+(node->nch+delta-1)*sizeof(KWNode*));
			if (node==NULL) throw RC_NORESOURCES; kwTabSize+=sizeof(KWNode*)*delta;
			if (ch>node->mch) memset(node->nodes+node->nch,0,delta*sizeof(KWNode*));
			else {
				memmove(node->nodes+delta,node->nodes,node->nch*sizeof(KWNode*));
				memset(node->nodes,0,delta*sizeof(KWNode*)); node->mch=ch;
			}
			node->nch+=delta;
		}
		ppn=&node->nodes[ch-node->mch];
	}
}

const char *SInCtx::errorMsgs[SY_ALL] = {
	"invalid character", "invalid UTF8 character", "invalid UTF8 number", "missing '''", "missing '\"'", "missing hexadecimal digit",
	"invalid number", "missing PID", "invalid URIID number", "missing number", "number too big", "missing type name",
	"missing ')'", "missing ']'", "missing '}'", "invalid type name", "missing '*/'", "invalid property name", "invalid identity name",
	"missing property name", "missing identity name", "missing parameter number", "not supported yet", "missing '('",
	"missing constant or identifier", "mismatch", "too few arguments", "too many arguments", "empty expression", "illegal ','",
	"missing ','", "missing binary operation", "missing 'SELECT'", "unknown type of variable", "invalid variable",
	"invalid condition", "missing 'BY'", "syntax error", "missing 'TRUE', 'FALSE' or 'NULL'", "missing 'AND'",
	"invalid timestamp or interval constant", "missing prefix name", "missing second part of qname",
	"invalid constant name in EXTRACT(...)", "unbalanced ')'", "unbalanced ']'", "unbalanced '}'",
	"condition must be a Boolean expression", "missing 'JOIN'", "missing variable name", "missing 'AGAINST'",
	"missing '='", "missing class name", "unknown qname prefix", "invalid SELECT list for GROUP BY", "invalid HAVING expression",
};

SInCtx::~SInCtx()
{
	if ((v.flags&HEAP_TYPE_MASK)!=NO_HEAP) freeV(v);
	if (qNames!=NULL) ma->free(qNames);
	if (base!=NULL) ma->free(base);
}

TLx SInCtx::lex()
{
	TLx lx;
	if ((lx=nextLex)!=LX_ERR) {if (lx!=LX_EOE) nextLex=LX_ERR; return lx;}
	if ((v.flags&HEAP_TYPE_MASK)!=NO_HEAP) {freeV(v); v.setError();}
	const char *beg,*cbeg; size_t ls; ulong wch; const byte *bptr;
	KWNode *kwn; byte ch,ch2,*buf; RC rc; unsigned u; PID pid; bool fStd;
	while ((errpos=ptr)<end) switch (lx=charLex[(byte)*ptr++]) {
	case LX_EOE: --ptr;
	default: return lx;
	case LX_NL: lbeg=ptr; lmb=0; ++line; continue;
	case LX_SPACE: continue;
	case LX_UTF8:
		bptr=(const byte *)ptr; beg=ptr-1; lmb+=UTF8::len(*beg)-1;
		if ((wch=UTF8::decode(ptr[-1],bptr,end-ptr))==~0u) throw SY_INVUTF8;
		ptr=(const char*)bptr;
		if (UTF8::iswalpha((wchar_t)wch)) goto ident;
		else if (UTF8::iswdigit((wchar_t)wch)) {
			if (strToNum(beg,end-beg,v,&ptr)==RC_OK) return LX_CON;
			throw SY_INVUTF8N;
		} else if (UTF8::iswspace((wchar_t)wch)) continue;
		throw SY_INVCHR;
	case LX_XLETTER:
		if (ptr<end && *ptr=='\'') {
			if ((buf=(byte*)ma->malloc((end-ptr)/2))==NULL) throw RC_NORESOURCES;
			for (ls=0,++ptr;;ls++) {
				if (ptr>=end) {errpos=ptr; throw SY_MISQUO;}
				if ((ch=*ptr++)=='\'') break;
				if (!isxdigit(ch)||ptr>=end||!isxdigit(ch2=*ptr++)) {errpos=ptr-1; throw SY_INVHEX;}
				buf[ls]=(ch<='9'?ch-'0':ch<='F'?ch-'A'+10:ch-'a'+10)<<4
						|(ch2<='9'?ch2-'0':ch2<='F'?ch2-'A'+10:ch2-'a'+10);
			}
			v.set((byte*)buf,(uint32_t)ls); v.flags=mtype; return LX_CON;
		}
	case LX_LETTER:
	letter:
		beg=ptr-1; 
		for (kwn=kwt[(*beg&~0x20)-'A']; kwn!=NULL;) {
			if (ptr<end) {
				if ((ch=byte((*ptr&~0x20)-kwn->mch))<kwn->nch) {kwn=kwn->nodes[ch]; ptr++; continue;} 
				if ((lx=charLex[(byte)*ptr])>=LX_DIGIT) {ptr++; break;}
				if (lx==LX_UTF8) {
					bptr=(const byte *)ptr+1;
					if ((wch=UTF8::decode((byte)*ptr,bptr,end-ptr-1))==~0u) {errpos=ptr; throw SY_INVUTF8;}
					if (UTF8::iswalnum((wchar_t)wch)) {lmb+=unsigned((const char*)bptr-ptr-1); ptr=(const char*)bptr; break;}
				}
			}
			if (kwn->lx==LX_ERR||(kwn->ql&1<<sqt)==0) break;		// check ':' ???
			switch (v.op=kwn->kw) {
			default: return kwn->lx;
			case KW_TRUE: v.set(true); break;
			case KW_FALSE: v.set(false); break;
			case KW_NOW: v.setNow(); break;
			case KW_CUSER: v.setCUser(); break;
			case KW_CSTORE: v.setCStore(); break;
			case KW_TIMESTAMP: case KW_INTERVAL:
				if (lex()!=LX_CON || v.type!=VT_STRING || 
					convV(v,v,kwn->kw==KW_TIMESTAMP?VT_DATETIME:VT_INTERVAL,ses)!=RC_OK) throw SY_INVTMS;
				break;
			}
			return LX_CON;
		}
	ident:
		while (ptr<end && (lx=charLex[(byte)*ptr])>=LX_UTF8) {
			if (lx>=LX_DIGIT) ptr++;
			else {
				bptr=(const byte *)ptr+1;
				if ((wch=UTF8::decode((byte)*ptr,bptr,end-ptr-1))==~0u) {errpos=ptr; throw SY_INVUTF8;}
				if (UTF8::iswalnum((wchar_t)wch)) {lmb+=unsigned((const char*)bptr-ptr-1); ptr=(const char*)bptr;} else break;
			}
		}
		v.set(beg,uint32_t(ptr-beg)); if (ptr>=end || *ptr!=':') return LX_IDENT; if ((mode&SIM_SIMPLE_NAME)!=0) return LX_ERR;
		if ((cbeg=++ptr)>=end || (lx=charLex[(byte)*ptr])<LX_UTF8 && lx!=LX_DQUOTE || lx==LX_DIGIT) return LX_PREFIX;
		if (lx==LX_UTF8) {
			bptr=(const byte *)ptr+1;
			if ((wch=UTF8::decode((byte)*ptr,bptr,end-ptr-1))==~0u) {errpos=ptr; throw SY_INVUTF8;}
			if (UTF8::iswalpha((wchar_t)wch)) {lmb+=unsigned((const char*)bptr-ptr-1); ptr=(const char*)bptr;} else return LX_PREFIX;
		}
		fStd=(mode&SIM_STD_OVR)==0 && cbeg-beg==sizeof(STORE_STD_QPREFIX)-1 && memcmp(beg,STORE_STD_QPREFIX,sizeof(STORE_STD_QPREFIX)-1)==0;
		if ((lx=lex())!=LX_IDENT) throw SY_MISQNM;
		if (fStd) for (u=0; u<=PROP_SPEC_MAX; u++)
			if (Classifier::builtinURIs[u].lname==(size_t)v.length && memcmp(v.str,Classifier::builtinURIs[u].name,v.length)==0)
				{v.setURIID(URIID(u)); return LX_IDENT;}
		mapURI(beg,cbeg-beg-1);
		return LX_IDENT;
	case LX_DIGIT:
		if ((rc=strToNum(ptr-1,end-ptr+1,v,&ptr,(mode&SIM_INT_NUM)!=0))!=RC_OK) throw SY_INVNUM;
		if (ptr<end && (lx=charLex[(byte)*ptr])>=LX_UTF8) {
			if (lx!=LX_UTF8) throw SY_INVNUM; bptr=(const byte *)ptr+1;
			if ((wch=UTF8::decode((byte)*ptr,bptr,end-ptr-1))!=~0u && UTF8::iswalnum((wchar_t)wch)) throw SY_INVNUM;
		}
		return LX_CON;
	case LX_ULETTER:
		if (ptr>=end || *ptr!='\'') {beg=ptr-1; goto letter;}
		ptr++;
	case LX_DQUOTE:
	case LX_QUOTE:
		for (cbeg=beg=ptr,ls=0,buf=NULL,ch=ptr[-1]; ptr<end; ) if (*ptr++==ch) {
			if (ptr>=end || *ptr!=ch) {
				size_t ll=ptr-cbeg-1; 
				if (buf!=NULL) {if (ll!=0) memcpy(buf+ls,cbeg,ll); buf[ls+ll]='\0';}
				v.set(beg,uint32_t(ls+ll)); if (buf!=NULL) v.flags=mtype;
				if (lx==LX_DQUOTE) return LX_IDENT;
				if (lx==LX_ULETTER) v.type=VT_URL;
				return LX_CON;
			}
			if (buf==NULL) {
				if ((buf=(byte*)ma->malloc(end-beg))==NULL) throw RC_NORESOURCES;
				memcpy(buf,beg,ls=ptr-beg); beg=(const char*)buf;
			} else if (cbeg<ptr) {memcpy(buf+ls,cbeg,ptr-cbeg); ls+=ptr-cbeg;}
			cbeg=++ptr;
		}
		errpos=ptr; throw lx==LX_DQUOTE?SY_MISDQU:SY_MISQUO;
	case LX_SELF:
		if (ptr>=end) return LX_SELF;
		if (!isxdigit(*(byte*)ptr)) return charLex[(byte)*ptr]==LX_LCBR?OP_PIN:LX_SELF;
		pid.pid=0; pid.ident=STORE_OWNER; beg=ptr;
		do {ch=*ptr++; pid.pid=pid.pid<<4|(ch<='9'?ch-'0':ch<='F'?ch-'A'+10:ch-'a'+10);}
		while (ptr<end && isxdigit(*(byte*)ptr));
		if (ptr-beg>16) throw SY_TOOBIG;
		if (ptr<end && *ptr=='!') {
			if ((++ptr,lex())!=LX_IDENT || v.type!=VT_STRING) throw SY_MISIDENT;
			mapIdentity(); assert(v.type==VT_IDENTITY); pid.ident=v.iid;
		}
		v.set(pid); return LX_CON;
	case LX_DOLLAR:
		if (ptr<end) {
			switch (charLex[(byte)*ptr]) {
			default: break;
			case LX_LPR: return LX_EXPR;
			case LX_LCBR: ++ptr; return LX_QUERY;
			case LX_DIGIT:
				if (ids==NULL || nids==0 || (u=*ptr-'0')>=nids) throw SY_INVURN;
				while (++ptr<end && isD(ch=(byte)*ptr)) if (TN_CHECK(u,ch,nids-1)) u=u*10+ch-'0'; else throw SY_INVURN;
				v.setURIID(ids[u]); return LX_IDENT;
			}
		}
		errpos=ptr; throw SY_MISNUM;
	case LX_LT:
		return ptr>=end?OP_LT:*ptr=='>'?(++ptr,OP_NE):*ptr=='='?(++ptr,OP_LE):*ptr=='<'?(++ptr,OP_LSHIFT):OP_LT;
	case LX_GT:
		return ptr>=end?(TLx)OP_GT:*ptr=='='?(++ptr,(TLx)OP_GE):*ptr!='>'?(TLx)OP_GT:++ptr<end&&*ptr=='>'?(++ptr,(TLx)LX_URSHIFT):(TLx)OP_RSHIFT;
	case LX_VL:
		return ptr<end&&*ptr=='|'?(++ptr,OP_CONCAT):OP_OR;
	case OP_MINUS:
		if (sqt==SQ_SQL && ptr<end && *ptr=='-') {while (++ptr<end && *ptr!='\n'); continue;}
		return OP_MINUS;
	case OP_DIV:
		if (sqt==SQ_SQL && ptr<end && *ptr=='*') {
			for (unsigned level=1;;) {
				if (++ptr>=end || *ptr=='\0') throw SY_MISCOMEND;
				if (*ptr=='\n') {line++; lbeg=ptr+1; lmb=0;}
				else if (ptr+1<end) {
					if (ptr[0]=='*' && ptr[1]=='/') {ptr+=2; if (--level==0) break;}
					else if (ptr[0]=='/' && ptr[1]=='*') {ptr+=2; ++level;}
				}
			}
			continue;
		}
		return lx;
	case LX_HASH:
		if (sqt==SQ_SPARQL) {while (ptr<end && *ptr!='\n') ptr++; continue;}
		return lx;
	case LX_ERR: throw SY_INVCHR;
	}
	return LX_EOE;
}

bool SInCtx::checkDelimiter(char c)
{
	const TLx del=charLex[(byte)(c==0?',':c)];
	TLx lx=lex(); if (lx!=del) {nextLex=lx; return false;}
	return true;
}

void SInCtx::getURIFlags(const char *ptr,size_t l,URIInfo& info)
{
	info.flags=info.lSuffix=0;
	if (ptr!=NULL && l!=0) {
		info.flags=UID_ID|UID_AID|UID_IRI;
		const char *const p0=ptr,*const end=ptr+l; const byte *bptr; ulong wch;
		for (;;) {
			if (ptr==end) return;
			switch (charLex[(byte)*ptr++]) {
			default: break;
			case LX_UTF8:
				info.flags&=~UID_AID; bptr=(const byte *)ptr; wch=UTF8::decode(ptr[-1],bptr,end-ptr);
				if (wch==~0u || !UTF8::iswalpha((wchar_t)wch) && (ptr-1==p0 || !UTF8::iswdigit((wchar_t)wch))) break;
				ptr=(const char*)bptr; continue;
			case LX_XLETTER: case LX_ULETTER: case LX_LETTER: continue;
			case LX_DIGIT: if (ptr-1!=p0) continue; else break;
			}
			--ptr; info.flags=UID_IRI; break;
		}
		const char *suffix=NULL;
		do switch (charLex[(byte)*ptr++]) {
		default: info.flags&=UID_DQU; suffix=NULL; break;
		case LX_XLETTER: case LX_ULETTER: case LX_LETTER: case LX_DIGIT: break;
		case LX_DQUOTE: info.flags=UID_DQU; suffix=NULL; break;
		case LX_SELF: case LX_DOLLAR: case LX_COLON: case LX_PERIOD: case LX_EXCL:
		case OP_MINUS: case OP_PLUS: case OP_NOT: case OP_MOD: case OP_XOR: case OP_AND:
			info.flags&=~UID_SID; break;
		case LX_UTF8:
			bptr=(const byte *)ptr; wch=UTF8::decode(ptr[-1],bptr,end-ptr);
			if (wch==~0u || !UTF8::iswalnum((wchar_t)wch)) {info.flags&=UID_DQU; suffix=NULL;}
			break;
		case OP_DIV: case LX_HASH:
			suffix=NULL; info.flags&=~UID_SID;
			if (ptr<end) switch (charLex[(byte)*ptr]){
			default: break;
			case LX_UTF8:
				bptr=(const byte *)ptr+1; wch=UTF8::decode(*ptr,bptr,end-ptr-1);
				if (wch==~0u || !UTF8::iswalpha((wchar_t)wch)) break;
			case LX_XLETTER: case LX_ULETTER: case LX_LETTER:
				suffix=ptr; info.flags|=UID_SID; break;
			}
			break;
		} while (ptr<end);
		if (suffix!=NULL && end-suffix<256) info.lSuffix=byte(end-suffix);
	}
}

void SInCtx::mapIdentity()
{
	RC rc;
	if (ses==NULL) throw RC_NOSESSION;
	if (v.type!=VT_STRING) throw SY_MISIDENT;
	if (v.flags!=mtype && (rc=copyV(v,v,ma))!=RC_OK) throw rc;
	const char *str=v.str;
	Identity *ident=(Identity*)ses->getStore()->uriMgr->insert(str); if (ident==NULL) throw RC_NORESOURCES;
	v.setIdentity(ident->getID()); ident->release(); ma->free((char*)str);
}

QVarID SInCtx::findVar(const Value &v,const QVarRef *vars,unsigned nVars) const
{
	QVarID classVar=INVALID_QVAR_ID; bool fMany=false; assert(ses!=NULL);
	if (vars!=NULL) for (unsigned i=0; i<nVars; i++) {
		QVar *qv=vars[i].var; URI *uri;
		if (v.type==VT_STRING && qv->name!=NULL && strlen(qv->name)==v.length && !strncasecmp(qv->name,v.str,v.length)) return qv->id;
		if (qv->type==QRY_SIMPLE && ((SimpleVar*)qv)->nClasses==1 && !fMany) switch (v.type) {
		case VT_URIID:
			if (((SimpleVar*)qv)->classes[0].classID==v.uid)
				{if (classVar==INVALID_QVAR_ID) classVar=qv->id; else {classVar=INVALID_QVAR_ID; fMany=true;}}
			break;
		case VT_STRING:
			if ((uri=(URI*)ses->getStore()->uriMgr->ObjMgr::find(((SimpleVar*)qv)->classes[0].classID))!=NULL) {
				const char *cname=uri->getName();
				if (cname!=NULL && strlen(cname)==v.length && !strncasecmp(cname,v.str,v.length))
					{if (classVar==INVALID_QVAR_ID) classVar=qv->id; else {classVar=INVALID_QVAR_ID; fMany=true;}}
				uri->release();
			}
			break;
		} else if (qv->type<=QRY_FULL_OUTER_JOIN) {
			QVarID var=findVar(v,((JoinVar*)qv)->vars,((JoinVar*)qv)->nVars);
			if (var!=INVALID_QVAR_ID) return var;
		} else if (qv->type<QRY_ALL_SETOP) {
			QVarID var=findVar(v,((SetOpVar*)qv)->vars,((SetOpVar*)qv)->nVars);
			if (var!=INVALID_QVAR_ID) return var;
		}
	}
	return classVar;
}

TLx SInCtx::parsePrologue()
{
	TLx lx=lex();
	if (lx==LX_KEYW) {
		if (v.op==KW_BASE) {
			assert(base==NULL);
			if (lex()!=LX_CON || v.type!=VT_STRING) throw SY_MISCON;
			if (v.length==0) mode|=SIM_NO_BASE;
			else if (ses==NULL || ses->URIBase==NULL || ses->lURIBase!=v.length&&ses->lURIBase!=v.length+1 ||
									strncasecmp(ses->URIBase,v.str,v.length)!=0 || 
									ses->lURIBase==v.length+1 && ses->URIBase[v.length]!='/') {
				if ((base=new(ma) char[lBaseBuf=v.length+100])==NULL) throw RC_NORESOURCES;
				memcpy(base,v.str,lBase=v.length); mode&=~SIM_NO_BASE;
				if (base[lBase-1]!='/' && base[lBase-1]!='#') base[lBase++]='/';
			}
			lx=lex();
		}
		for (QName qn; lx==LX_KEYW && v.op==KW_PREFIX; lx=lex()) {
			if (lex()!=LX_PREFIX) throw SY_MISQNM;
			if (v.length==sizeof(STORE_STD_QPREFIX)-1 && memcmp(v.str,STORE_STD_QPREFIX,sizeof(STORE_STD_QPREFIX)-1)==0) mode|=SIM_STD_OVR;
			qn.qpref=v.str; qn.lq=v.length; RC rc;
			if (lex()!=LX_CON || v.type!=VT_STRING) throw SY_MISCON;
			qn.str=v.str; qn.lstr=v.length; qn.fDel=qn.lstr!=0&&(qn.str[qn.lstr-1]=='/'||qn.str[qn.lstr-1]=='#'||qn.str[qn.lstr-1]=='?');
			if ((rc=BIN<QName,const QName&,QNameCmp>::insert(qNames,nQNames,qn,qn,ma))!=RC_OK) throw rc;
		}
	}
	return lx;
}

void SInCtx::mapURI(const char *prefix,size_t lPrefix)
{
	if (ses!=NULL && v.type==VT_STRING) {
		char *str=(char*)v.str; bool fCopy=v.flags!=mtype||str[v.length]!='\0'; bool fAddDel=false;
		if (prefix!=NULL && lPrefix!=0) {
			const QName *pq=NULL; QName qn={prefix,lPrefix,NULL,0,false};
			if (lastQN>=nQNames || (pq=&qNames[lastQN])->lq!=lPrefix || memcmp(prefix,pq->qpref,lPrefix)!=0)
				pq=BIN<QName,const QName&,QNameCmp>::find(qn,qNames,nQNames);
			if (pq==NULL && (pq=BIN<QName,const QName&,QNameCmp>::find(qn,ses->qNames,ses->nQNames))==NULL) throw SY_UNKQPR;
			prefix=pq->str; lPrefix=pq->lstr; fAddDel=!pq->fDel; lastQN=unsigned((QName*)pq-qNames); fCopy=true;
		} else if ((mode&SIM_NO_BASE)==0 && !Session::hasPrefix(str,v.length)) {
			if (lBase!=0) {
				if (lBase+v.length+1>lBaseBuf && (base=(char*)
					ma->realloc(base,lBaseBuf=lBase+v.length+1))==NULL) throw RC_NORESOURCES;
				memcpy(base+lBase,str,v.length); base[lBase+v.length]=0; str=base;
			} else {
				assert(ses!=NULL && ses->lURIBase!=0);
				if (ses->lURIBase+v.length+1>ses->lURIBaseBuf && (ses->URIBase=(char*)
					ses->realloc(ses->URIBase,ses->lURIBaseBuf=ses->lURIBase+v.length+1))==NULL) throw RC_NORESOURCES;
				memcpy(ses->URIBase+ses->lURIBase,str,v.length); ses->URIBase[ses->lURIBase+v.length]=0; str=ses->URIBase;
			}
			fCopy=false;
		}
		if (fCopy) {
			if (lBase+lPrefix+v.length+2>lBaseBuf && (base=(char*)
				ma->realloc(base,lBaseBuf=lBase+lPrefix+v.length+1))==NULL) throw RC_NORESOURCES;
			if (prefix!=NULL) {memcpy(base+lBase,prefix,lPrefix); if (fAddDel) base[lBase+lPrefix++]='/';}
			memcpy(base+lBase+lPrefix,str,v.length); base[lBase+lPrefix+v.length]=0; str=base+lBase;
		}
		URI *uri=(URI*)ses->getStore()->uriMgr->insert(str); if (uri==NULL) throw RC_NORESOURCES;
		v.setURIID(uri->getID()); uri->release();
	} else if (v.type!=VT_URIID||prefix!=NULL) throw SY_INVPROP;
}

struct OpF
{
	TLx lx; byte flag; 
	OpF() : lx(LX_BOE),flag(0) {} 
	OpF(TLx l,byte f) : lx(l),flag(f) {}
};

void SInCtx::parse(Value& res,const QVarRef *vars,unsigned nVars,unsigned pflags)
{
	RC rc; unsigned i,nvals,flags; byte opf=0,est=0; OpF *po; assert(ses!=NULL);
	Stack<OpF,NoFree<OpF> > ops; Stack<Value,FreeV> oprs; Value vv,*vals; LexState lst;
	Expr *exp; Stmt *stmt; QVarID var; RefVID *rv; ops.push(OpF(LX_BOE,0));
	if ((pflags&PRS_PATH)!=0) {ops.push(OpF(OP_PATH,0)); vv.setVarRef(0); oprs.push(vv);}
	for (TLx lx=(pflags&PRS_TOP)==0?lex():parsePrologue(),lx2;;lx=lex()) {
		const OpDscr *od=&opDscr[lx]; opf=0; assert(lx<sizeof(opDscr)/sizeof(opDscr[0]));
		switch (lx) {
		case LX_IDENT:
			if (ops.top().lx==OP_PATH) {if (v.type!=VT_URIID) mapURI();}
			else if ((mode&SIM_SELECT)!=0 && v.type==VT_STRING && (v.flags&HEAP_TYPE_MASK)==NO_HEAP) {
				for (i=0;;i++)
					if (i>=dnames) {Len_Str &ls=dnames.add(); ls.s=v.str; ls.l=v.length; break;}
					else if (dnames[i].l==v.length && !strncasecmp(dnames[i].s,v.str,v.length)) break;
					v.setVarRef((byte)i); v.refV.flags=0xFFFF; 
			} else if (vars!=NULL && (var=findVar(v,vars,nVars))!=INVALID_QVAR_ID) {freeV(v); v.setVarRef(var);} 
			else if (nVars<=1) {mapURI(); assert(v.type==VT_URIID); URIID uid=v.uid; v.setVarRef(0,uid);}
			else throw SY_MISNAME;
		case LX_CON:
			if (est!=0) goto error_no_op; oprs.push(v); v.setError(); est=1; continue;
		case LX_SELF: 
			if (est!=0) goto error_no_op; v.setVarRef(0); oprs.push(v); v.setError(); est=1; continue;
		case LX_PREFIX: throw SY_MISQN2;
		case LX_QUERY:
			stmt=parseStmt(true);
			if (lex()!=LX_RCBR) {if (stmt!=NULL) stmt->destroy(); throw SY_MISRCBR;}
			v.set(stmt); oprs.push(v); v.setError(); est=1; continue;
		case LX_PERIOD:
			if ((lx=lex())==LX_IDENT) {
				mapURI(); assert(v.type==VT_URIID);
				if (est==0) {oprs.push(v); est=1; v.setError(); continue;}
			} else if (est==0) throw SY_MISPROP;
			nextLex=lx; od=&opDscr[lx=OP_PATH]; est=0; break;
		case LX_EXCL:
			if (est!=0) goto error_no_op;
			if (lex()!=LX_IDENT || v.type!=VT_STRING) throw SY_MISIDENT;
			mapIdentity(); assert(v.type==VT_IDENTITY); oprs.push(v); v.setError(); est=1; continue;
		case LX_COLON:
			if (est!=0) goto error_no_op;
			if ((lx=lex())!=LX_CON || v.type!=VT_INT) {
				if (ops.top().lx!=LX_FILTER) throw SY_SYNTAX;
				switch (lx) {
				case OP_MIN: vv.set((unsigned)STORE_MIN_ELEMENT); break;
				case OP_MAX: vv.set((unsigned)STORE_MAX_ELEMENT); break;
				case OP_SUM: vv.set((unsigned)STORE_SUM_COLLECTION); break;
				case OP_AVG: vv.set((unsigned)STORE_AVG_COLLECTION); break;
				case OP_CONCAT: vv.set((unsigned)STORE_CONCAT_COLLECTION); break;
				case LX_IDENT:
					if (v.type==VT_STRING) {
						if (v.length==sizeof(FIRST_S)-1 && cmpncase(v.str,FIRST_S,sizeof(FIRST_S)-1)) {vv.set((unsigned)STORE_FIRST_ELEMENT); break;}
						if (v.length==sizeof(LAST_S)-1 && cmpncase(v.str,LAST_S,sizeof(LAST_S)-1)) {vv.set((unsigned)STORE_LAST_ELEMENT); break;}
					}
				default: throw SY_SYNTAX;
				}
				if ((nextLex=lex())!=LX_RBR) throw SY_MISRBR;
			} else if (v.i<0) throw SY_INVNUM;
			else if (v.i>255) throw SY_TOOBIG;
			else {
				vv.setParam((byte)v.i);
				if ((lx=lex())!=LX_LPR) nextLex=lx;
				else if (lex()!=LX_IDENT || v.type!=VT_STRING) throw SY_MISTYN;
				else {
					for (unsigned i=1; ; i++)
						if (i>=sizeof(typeName)/sizeof(typeName[0])) throw SY_INVTYN;
						else if (typeName[i].l==v.length && cmpncase(v.str,typeName[i].s,v.length)) {vv.refV.type=(byte)i; break;}
					if ((lx=lex())==LX_COMMA) {
						if ((lx=lex())==LX_RPR) throw SY_SYNTAX;
						if (lx==LX_KEYW && (v.op==KW_DESC||v.op==KW_ASC)) {
							if (v.op==KW_DESC) vv.refV.flags|=ORD_DESC; 
							if ((lx=lex())!=LX_RPR) {if (lx!=LX_COMMA) throw SY_MISCOM; if ((lx=lex())==LX_RPR) throw SY_SYNTAX;}
						}
						if (lx!=LX_RPR) {
							if (lx!=LX_KEYW || v.op!=KW_NULLS || lex()!=LX_IDENT || v.type!=VT_STRING) throw SY_SYNTAX;
							if (v.length==sizeof(FIRST_S)-1 && cmpncase(v.str,FIRST_S,sizeof(FIRST_S)-1)) vv.refV.flags|=ORD_NULLS_BEFORE;
							else if (v.length==sizeof(LAST_S)-1 && cmpncase(v.str,LAST_S,sizeof(LAST_S)-1)) vv.refV.flags|=ORD_NULLS_AFTER;
							else throw SY_SYNTAX;
							lx=lex();
						}
					}
					if (lx!=LX_RPR) throw SY_MISRPR;
				}
			}
			oprs.push(vv); est=1; continue;
		case LX_LBR:
			od=&opDscr[lx=est==0?(TLx)OP_RANGE:(est=0,(TLx)LX_FILTER)]; break;
		case LX_LCBR:
			if (est!=0) {od=&opDscr[lx=LX_REPEAT]; est=0;} break;
		case LX_QUEST:
			if (est!=0 || ops.top().lx!=LX_REPEAT) throw SY_SYNTAX;
			if (lex()!=LX_RCBR) throw SY_MISRCBR;
			ops.pop();
			if ((po=ops.top(1))->lx==LX_PATHQ) po=ops.top(2); else po->flag|=FILTER_LAST_OP;
			assert(po->lx==OP_PATH); po->flag|=QUEST_PATH_OP; est=1; continue;
		case LX_IS:
			if (est==0) throw SY_MISCON;
			if ((lx2=lex())==OP_LNOT) {opf=NOT_BOOLEAN_OP; lx2=lex();}
			if ((lx2!=LX_CON || v.type!=VT_BOOL) && (lx2!=LX_KEYW || v.op!=KW_NULL)) {
				if (lx2!=LX_IDENT || v.length!=1 || (*v.bstr&~0x20)!='A') throw SY_MISLGC;
				if ((lx2=lex())==LX_IDENT) mapURI();
				else if (lx2==LX_COLON) {
					if (lex()!=LX_CON || v.type!=VT_INT) throw SY_MISNUM;
					if (v.i<0) throw SY_INVNUM; if (v.i>255) throw SY_TOOBIG;
					vv.setParam((byte)v.i); v=vv; lx2=LX_CON;
				} else throw SY_MISCLN;
				od=&opDscr[lx=OP_IS_A];
			}
			nextLex=lx2; est=0; break;
		case OP_PLUS:
			if (est!=0) {est=0; break;}
			if (ops.top().lx==LX_REPEAT && (nextLex=lex())==LX_RCBR) {
				ops.pop(); 
				if ((po=ops.top(1))->lx==LX_PATHQ) po=ops.top(2); else po->flag|=FILTER_LAST_OP;
				assert(po->lx==OP_PATH); po->flag|=PLUS_PATH_OP; nextLex=LX_ERR; est=1;
			}
			continue;
		case OP_MINUS:
			if (est!=0) est=0; else od=&opDscr[lx=OP_NEG]; break;
		case OP_MUL:
			if (est!=0) est=0;
			else switch (nextLex=lex()) {
			case LX_RPR:
				if (ops.top().lx!=OP_COUNT || (mode&(SIM_SELECT|SIM_HAVING))==0) throw SY_MISCON;
				vv.setError(); oprs.push(vv); est=1; continue;
			case LX_RBR:
				if (ops.top().lx!=LX_COMMA && ops.top(2)->lx!=OP_RANGE) throw SY_MISCON;
				vv.setError(); oprs.push(vv); est=1; continue;
			case LX_COMMA:
				if (ops.top().lx!=OP_RANGE) throw SY_MISCON;
				vv.setError(); oprs.push(vv); est=1; continue;
			case LX_RCBR:
				if (ops.top().lx!=LX_REPEAT) throw SY_MISCON;
				ops.pop(); if ((po=ops.top(1))->lx==LX_PATHQ) po=ops.top(2); else po->flag|=FILTER_LAST_OP;
				assert(po->lx==OP_PATH); po->flag|=STAR_PATH_OP;
				nextLex=LX_ERR; est=1; continue;
			default: od=&opDscr[lx=OP_DEREF]; break;
			}
			break;
		case OP_AND:
			if (est!=0) est=0; else od=&opDscr[lx=OP_REF]; break;
		case OP_LNOT:
			if (est==0) break;
			if ((lx=lex())!=LX_BETWEEN && lx!=OP_IN) goto error_no_op;
			od=&opDscr[lx]; est=0; opf=NOT_BOOLEAN_OP; break;
		case LX_LPR:
			if (est!=0) {lx=OP_CALL; est=0;}
			else if ((mode&SIM_DML_EXPR)!=0 && ((po=ops.top(1))->lx==LX_BOE||po->lx==LX_SPROP||po->lx==LX_LCBR||po->lx==OP_PIN||po->lx==LX_COMMA&&po->flag!=0) && (nextLex=lex())==LX_KEYW && (v.op==KW_INSERT||v.op==KW_SELECT)) {
				stmt=parseStmt(true); assert(stmt!=NULL); if ((lx=lex())!=LX_RPR) {stmt->destroy(); throw SY_MISRPR;}
				if ((po->lx==LX_LCBR||po->lx==LX_COMMA||po->lx==OP_PIN) && po->flag!=VT_STRUCT && stmt->getOp()==STMT_QUERY && (stmt->getTop()==NULL ||
					(i=stmt->getTop()->stype)!=SEL_COUNT && i!=SEL_VALUE && i!=SEL_CONST && i!=SEL_DERIVED)) {stmt->destroy(); throw SY_SYNTAX;}	// SY_INVNST
				vv.set(stmt); vv.meta|=META_PROP_EVAL; oprs.push(vv); est=1; continue;
			}
			break;
		case OP_EQ:
			if (est!=0) est=0; else throw SY_MISCON;
			if (((po=ops.top(1))->lx==LX_LCBR && po->flag==0 || po->lx==OP_PIN || po->lx==LX_COMMA && po->flag==VT_STRUCT) && oprs.top().type==VT_VARREF)
				{po->flag=VT_STRUCT; od=&opDscr[lx=LX_SPROP];}
			break;
		case LX_KEYW:
			switch (v.op) {
			case KW_FROM:
				if (est!=0 && ((lx2=ops.top().lx)==OP_TRIM || lx2==OP_EXTRACT)) od=&opDscr[lx=LX_COMMA]; // || lx2==LX_SUBSTRING
				break;
			//case KW_FOR:
				// LX_SUBSTRING
			case KW_AS:
				if (ops.top().lx==OP_CAST) {
					mode|=SIM_SIMPLE_NAME;
					if ((lx2=lex())!=LX_IDENT || v.type!=VT_STRING) throw SY_MISTYN;
					mode&=~SIM_SIMPLE_NAME;
					for (i=1; ;i++)
						if (i>=sizeof(typeName)/sizeof(typeName[0])) {
							Units u=getUnits(v.str,v.length); if (u==Un_NDIM) throw SY_INVTYN;
							if ((lx2=lex())!=LX_PERIOD) {nextLex=lx2; i=u<<8|VT_DOUBLE;}
							else if (lex()!=LX_IDENT || v.type!=VT_STRING || v.length!=1 || *v.str!='f') throw SY_SYNTAX;
							else i=u<<8|VT_FLOAT; break;
						} else if (typeName[i].l==v.length && cmpncase(v.str,typeName[i].s,v.length)) break;
					v.set(i); ops.push(OpF(LX_COMMA,0)); oprs.push(v); if ((lx=lex())!=LX_RPR) throw SY_MISRPR;
					od=&opDscr[lx];
				}
				break;
			case KW_ALL:
			case KW_DISTINCT:
				if (est!=0 || (lx2=ops.top().lx)>=OP_ALL || (opDscr[lx2].flags&_A)==0) throw SY_SYNTAX;
				if (v.op==KW_DISTINCT) ops.top(1)->flag=DISTINCT_OP;
				if ((nextLex=lex())==LX_KEYW && (v.op==KW_DISTINCT || v.op==KW_ALL)) throw SY_SYNTAX;
				continue;
			}
		default:
			if ((od->flags&(_U|_F))!=0) {
				if (est!=0) goto error_no_op;
				if ((od->flags&_F)!=0 && lex()!=LX_LPR) throw SY_MISLPR;
			} else if (est==0) {
				if (ops.top().lx!=OP_CALL || lx!=LX_RPR) throw SY_MISCON;
			} else if ((od->flags&_C)==0) est=0;
			break;
		}
		
		for (const byte prty=od->prty[0];;) {
			const OpDscr *od2=&opDscr[ops.top().lx]; if (prty>od2->prty[1]) break;
			OpF op=ops.pop(); nvals=od2->nOps[0]; flags=op.flag|((pflags&PRS_COPYV)!=0?COPY_VALUES_OP:0);
			switch (op.lx) {
			case LX_BOE:
				nextLex=lx; vals=oprs.pop(1); assert(vals!=NULL&&oprs.isEmpty()); res=*vals; return;
			case LX_COMMA:
				for (nvals=2; (op=ops.pop()).lx==LX_COMMA; nvals++);
				if (op.lx==LX_LPR || op.lx==LX_LCBR || op.lx==OP_PIN) {
			case LX_LCBR: case OP_PIN:
					if (op.lx==LX_LPR) {if (lx!=LX_RPR) throw SY_MISRPR;} else if (lx!=LX_RCBR) throw SY_MISRCBR;
					vals=oprs.pop(nvals); assert(vals!=NULL);
					if ((rc=op.lx==LX_LCBR&&op.flag==VT_STRUCT||op.lx==OP_PIN?ExprTree::normalizeStruct(vals,nvals,vv,ma):
						ExprTree::normalizeArray(vals,nvals,vv,ma,ses!=NULL?ses->getStore():StoreCtx::get()))!=RC_OK) throw rc;
					oprs.push(vv); goto next_lx;
				} else if (op.lx==OP_IS_A || op.lx==OP_CALL) nvals++;
				else if (op.lx==LX_REPEAT) {nvals++; goto lx_repeat;}
				od2=&opDscr[op.lx];
				if (op.lx==OP_TRIM || op.lx==OP_EXTRACT) {
					if (nvals>2) throw SY_MANYARG;
					vals=oprs.top(2); vv=vals[0]; vals[0]=vals[1]; vals[1]=vv;
					if (op.lx==OP_TRIM) {v.set(op.flag); oprs.push(v); nvals=3;}
				}
				break;
			case OP_TRIM:
				v.set(" ",1); oprs.push(v); v.set(op.flag); oprs.push(v); nvals=3; break;
			case OP_CALL:
				if (est!=0) nvals++; else est=1; break;
			case LX_CONCAT: op.lx=OP_CONCAT; break;
			case LX_LPR:
				if (lx!=LX_RPR) throw SY_MISRPR; goto next_lx;
			case LX_URSHIFT:
				flags|=UNSIGNED_OP; op.lx=OP_RSHIFT; break;
			case LX_BETWEEN:
				if (lx!=OP_LAND) throw SY_MISAND;
				ops.push(OpF(LX_BETWEEN,op.flag)); ops.push(OpF(LX_BAND,0)); goto next_lx;
			case LX_BAND:
				op=ops.pop(); vals=oprs.pop(2); assert(op.lx==LX_BETWEEN && vals!=NULL);
				if (vals[0].type!=VT_EXPRTREE && vals[1].type!=VT_EXPRTREE) {
					Value *pv=new(ma) Value[2]; if (pv==NULL) throw RC_NORESOURCES;
					pv[0]=vals[0]; pv[1]=vals[1]; // reconcile types
					vv.setRange(pv); vv.flags=mtype;
				} else if ((rc=ExprTree::node(vv,ses,OP_RANGE,2,vals,flags))!=RC_OK) throw rc;	// fcopy? ses???
				oprs.push(vv); od2=&opDscr[op.lx=OP_IN]; flags=(flags&COPY_VALUES_OP)|op.flag; break;
			case LX_FILTER:
				if (lx!=LX_RBR) throw SY_MISRBR; vals=oprs.top(2); assert(vals!=NULL);
				if (vals[1].type==VT_INT||vals[1].type==VT_UINT) {
					if (vals[0].type==VT_REFIDPROP) {((RefVID*)vals[0].refId)->eid=vals[1].ui; vals[0].type=VT_REFIDELT; oprs.pop(); goto next_lx;}
					if (vals[0].type==VT_VARREF && (vals[0].refV.flags&VAR_TYPE_MASK)==0 && (lx2=ops.top().lx)!=OP_PATH&&lx2!=LX_PATHQ) {vals[0].eid=vals[1].ui; oprs.pop(); goto next_lx;}
				}
				ops.push(OpF(LX_PATHQ,0)); goto next_lx;
			case LX_REPEAT:
			lx_repeat:
				if (lx!=LX_RCBR) throw SY_MISRCBR; if (nvals>3) throw SY_MANYARG;
				if (nvals<3) i=~0u;
				else {
					vals=oprs.top(2);
					switch (vals[0].type) {
					case VT_INT64: case VT_UINT64: case VT_FLOAT: case VT_DOUBLE: throw SY_INVNUM;
					case VT_INT: if (vals[0].i<0) throw SY_INVNUM;
					case VT_UINT: i=vals[0].ui>0x7FFFF?0xFFFF:vals[0].ui; break;
					case VT_VARREF: if ((vals[0].refV.flags&VAR_TYPE_MASK)==VAR_PARAM) {i=0x8000|vals[0].refV.refN; break;}
					default: throw SY_MISNUM;
					}
					vals[0]=vals[1]; oprs.pop();
				}
				vals=oprs.top(1);
				switch (vals[0].type) {
				case VT_INT64: case VT_UINT64: case VT_FLOAT: case VT_DOUBLE: throw SY_INVNUM;
				case VT_INT: if (vals[0].i<0) throw SY_INVNUM; vals[0].type=VT_UINT;
				case VT_UINT:
					if (vals[0].ui>0x7FFFF) vals[0].ui=0xFFFF; else if (vals[0].ui==0||i<0x8000&&i>vals[0].ui) throw SY_INVNUM;
					vals[0].ui=vals[0].ui<<16|(i!=~0u?i:vals[0].ui); break;
				case VT_VARREF: if ((vals[0].refV.flags&VAR_TYPE_MASK)==VAR_PARAM) {vals[0].ui=(0x8000|vals[0].refV.refN)<<16|(i!=~0u?i:0x8000|vals[0].refV.refN); vals[0].type=VT_UINT; break;}
				default: throw SY_MISNUM;
				}
				ops.push(OpF(LX_PATHQ,1)); goto next_lx;
			case LX_PATHQ:
				nvals=3;
				if ((op=ops.pop()).lx==LX_PATHQ) {
					byte ff=0;
					if (op.flag!=0) {vals=oprs.top(2); vv=vals[0]; vals[0]=vals[1]; vals[1]=vv; ff=FILTER_LAST_OP;}
					nvals++; op=ops.pop(); op.flag|=ff;
				} else if ((flags&~COPY_VALUES_OP)!=0) {
					vv.set((unsigned)STORE_COLLECTION_ID); oprs.push(vv); vals=oprs.top(2); vv=vals[0]; vals[0]=vals[1]; vals[1]=vv; nvals++; 
				} else if ((pflags&PRS_PATH)==0) {
					vals=oprs.top(3); assert(vals!=NULL);
					if (vals[1].type==VT_URIID && (vals[2].type==VT_INT||vals[2].type==VT_UINT)) switch (vals[0].type) {
					case VT_VARREF: 
						if ((vals[0].refV.flags&VAR_TYPE_MASK)!=0||vals[0].length!=0) break;
						vals[0].refV.id=vals[1].uid; vals[0].length=1; vals[0].eid=vals[2].ui; oprs.pop(2); continue;
					case VT_REFID:
						if ((rv=new(ma) RefVID)==NULL) throw RC_NORESOURCES;
						rv->id=vals[0].id; rv->pid=vals[1].uid; rv->eid=vals[2].ui;
						rv->vid=STORE_CURRENT_VERSION; vals->set(*rv); oprs.pop(2); continue;
					}
				}
				assert(op.lx==OP_PATH); flags=flags&COPY_VALUES_OP|op.flag; od2=&opDscr[OP_PATH]; break;
			case OP_PATH:
				if ((pflags&PRS_PATH)==0) {
					vals=oprs.top(2); assert(vals!=NULL);
					if (vals[1].type==VT_URIID) switch (vals[0].type) {
					case VT_VARREF:
						if ((vals[0].refV.flags&VAR_TYPE_MASK)!=0||vals[0].length!=0) break;
						vals[0].refV.id=vals[1].uid; vals[0].length=1; oprs.pop(); continue;
					case VT_REFID:
						if ((rv=new(ma) RefVID)==NULL) throw RC_NORESOURCES;
						rv->id=vals[0].id; rv->pid=vals[1].uid; rv->eid=STORE_COLLECTION_ID;
						rv->vid=STORE_CURRENT_VERSION; vals->set(*rv); oprs.pop(); continue;
					}
				}
				break;
			case LX_SPROP:
				if (lx!=LX_COMMA && lx!=LX_RCBR) throw SY_MISRCBR;
				vals=oprs.top(2); assert(vals->type==VT_VARREF && vals->length!=0);
				vals[1].property=vals[0].refV.id; vals[0]=vals[1]; oprs.pop(); continue;
			}
			if (od2->match!=LX_ERR && od2->match!=lx) switch (od2->match) {
			case LX_RPR: throw SY_MISRPR;
			case LX_RBR: throw SY_MISRBR;
			case LX_RCBR: throw SY_MISRCBR;
			default: throw SY_MISMATCH;
			}
			if (nvals<od2->nOps[0]) throw SY_FEWARG;
			if (nvals>od2->nOps[1]) throw SY_MANYARG;
			if ((vals=oprs.top(nvals))==NULL) throw SY_EMPTY;
			if (op.lx==LX_EXPR) {
				if ((rc=ExprTree::forceExpr(*vals,ses))!=RC_OK || (rc=Expr::compile((ExprTree*)vals->exprt,exp,ma,false))!=RC_OK) throw rc;		// ses ???
				vals->exprt->destroy(); vals->set(exp); est=1; goto next_lx;
			}
			if ((rc=ExprTree::node(vv,ses,(ExprOp)op.lx,nvals,vals,flags))!=RC_OK) throw rc;	// fcopy? ses???
			oprs.pop(nvals); oprs.push(vv);
			if (od2->match!=LX_ERR) {assert((op.lx==OP_CALL||(od->flags&_C)!=0)&&est==1); goto next_lx;}
		}
		switch (lx) {
		default:
			if (isBool((ExprOp)lx)) {
				if ((lx2=lex())==LX_KEYW && (v.op==KW_ALL || v.op==KW_SOME || v.op==KW_ANY)) {
					opf|=v.op==KW_SOME?EXISTS_RIGHT_OP:FOR_ALL_RIGHT_OP; if ((lx2=lex())!=LX_LPR) throw SY_MISLPR;
				}
				if (lx2!=LX_LPR) nextLex=lx2; else {ops.push(OpF(lx,opf)); opf=0; lx=lx2; goto check_select;}
			}
			ops.push(OpF(lx,opf)); break;
		case LX_COMMA:
			switch ((po=ops.top(1))->lx) {
			default: if (opDscr[po->lx].match==LX_ERR) throw SY_INVCOM; break;
			case LX_LPR: if (ops.top(2)->lx!=OP_IN) throw SY_SYNTAX; break;
			case OP_PIN: opf=VT_STRUCT; break;
			case LX_LCBR: if (po->flag==0) {opf=VT_ARRAY; break;}
			case LX_COMMA: opf=po->flag; break;
			}
			ops.push(OpF(LX_COMMA,opf)); break;
		case LX_IS:
			flags=(pflags&PRS_COPYV)!=0?COPY_VALUES_OP:0;
			if ((lx2=lex())==LX_CON) {
				assert(v.type==VT_BOOL); if (opf!=0) v.b=!v.b; oprs.push(v); nvals=2; lx=OP_EQ;
			} else {
				assert(lx2==LX_KEYW && v.op==KW_NULL);
				flags|=opf^NOT_BOOLEAN_OP; nvals=1; lx=OP_EXISTS;
			}
			if ((rc=ExprTree::node(vv,ses,(ExprOp)lx,nvals,oprs.pop(nvals),flags))!=RC_OK) throw rc;	// fcopy? ses???
			oprs.push(vv); est=1; break;
		case OP_IS_A:
			lx2=lex(); assert(lx2==LX_IDENT&&v.type==VT_URIID||lx2==LX_CON&&v.type==VT_VARREF&&(v.refV.flags&VAR_TYPE_MASK)==VAR_PARAM);
			oprs.push(v); flags=opf|((pflags&PRS_COPYV)!=0?COPY_VALUES_OP:0);
			if ((lx2=lex())==LX_LPR) {ops.push(OpF(OP_IS_A,opf)); ops.push(OpF(LX_COMMA,0)); est=0;}
			else if ((rc=ExprTree::node(vv,ses,OP_IS_A,2,oprs.pop(2),flags))!=RC_OK) throw rc;	// fcopy? ses???
			else {oprs.push(vv); est=1; nextLex=lx2;}
			break;
		case OP_TRIM:
			opf=0;
			if ((lx2=lex())!=LX_IDENT || v.type!=VT_STRING) nextLex=lx2;
			else if (v.length==7 && cmpncase(v.str,"LEADING",7)) opf=1;
			else if (v.length==8 && cmpncase(v.str,"TRAILING",8)) opf=2;
			else if (v.length!=4 || !cmpncase(v.str,"BOTH",4)) nextLex=lx2;
			if ((nextLex=lex())==LX_KEYW && v.op==KW_FROM) {vv.set(" ",1); oprs.push(vv); est=1;}
			ops.push(OpF(lx,opf)); break;
		case OP_EXTRACT:
			mode|=SIM_SIMPLE_NAME;
			if ((nextLex=lex())!=LX_IDENT || v.type!=VT_STRING) throw SY_INVEXT;
			mode&=~SIM_SIMPLE_NAME;
			for (i=0; ;i++) 
				if (i>=sizeof(extractWhat)/sizeof(extractWhat[0])) throw SY_INVEXT;
				else if (extractWhat[i].l==v.length && cmpncase(v.str,extractWhat[i].s,v.length))
					{freeV(v); v.set(i); nextLex=LX_CON; break;}
			ops.push(OpF(lx,opf)); break;
		case OP_IN: 
			if ((lx=lex())==LX_KEYW && (v.op==KW_ALL||v.op==KW_SOME||v.op==KW_ANY)) {opf|=v.op==KW_SOME?EXISTS_RIGHT_OP:FOR_ALL_RIGHT_OP; lx=lex();}
			ops.push(OpF(OP_IN,opf)); opf=0; if (lx!=LX_LPR) {nextLex=lx; break;}
		case OP_EXISTS:
		check_select:
			ops.push(OpF(lx,opf)); saveLexState(lst);
			for (i=0; (lx=lex())==LX_LPR; i++) ops.push(OpF(LX_LPR,0));
			if (lx!=LX_KEYW || v.op!=KW_SELECT) nextLex=lx;
			else {
				ops.pop(i); restoreLexState(lst);
				try {stmt=NULL; parseQuery(stmt);} catch (...) {if (stmt!=NULL) stmt->destroy(); throw;}
				if (lex()!=LX_RPR) {if (stmt!=NULL) stmt->destroy(); throw SY_MISRPR;}
				v.set(stmt); oprs.push(v); v.setError(); est=1; nextLex=LX_RPR;
			}
			break;
		case LX_FILTER: case LX_REPEAT:
			if ((lx2=ops.top().lx)!=OP_PATH) {
				if (lx2==LX_PATHQ) {
					if (ops.top(2)->lx!=OP_PATH || ops.top().flag!=(lx==LX_FILTER?1:0)) throw SY_SYNTAX;
				} else if (lx==LX_FILTER && oprs.top().type==VT_VARREF) {
					// check unbound
				} else throw SY_SYNTAX;
			}
			ops.push(OpF(lx,opf)); break;
		}
next_lx:;
	}

error_no_op:
	throw !ops.isEmpty() && (ops.top().lx==LX_COMMA || (opDscr[ops.top().lx].flags&_F)!=0) ? SY_MISCOM : SY_MISBIN;
}

ExprTree *SInCtx::parse(bool fCopy)
{
	Value res; parse(res,NULL,0,fCopy?PRS_TOP|PRS_COPYV:PRS_TOP); RC rc;
	if ((rc=ExprTree::forceExpr(res,ses,fCopy))!=RC_OK) throw rc;
	return (ExprTree*)res.exprt;
}

struct OpD
{
	QUERY_SETOP op; DistinctType dt; 
	OpD() : op(QRY_UNION),dt(DT_DEFAULT) {} 
	OpD(QUERY_SETOP so,DistinctType d) : op(so),dt(d) {}
	bool operator==(const OpD& rhs) const {return op==rhs.op && dt==rhs.dt;}
};

QVarID SInCtx::parseQuery(Stmt*& res,bool fNested)
{
	assert(ses!=NULL);
	if (res==NULL && (res=new(ma) Stmt(0,ses))==NULL) throw RC_NORESOURCES;	// md???
	Stack<OpD,NoFree<OpD> > ops; Stack<QVarID,NoFree<byte> > vars;
	for (TLx lx=lex();;lx=lex()) {
		if (lx==LX_KEYW && v.op==KW_SELECT) vars.push(parseSelect(res));
		else if (lx!=LX_LPR) throw SY_MISSEL;
		else {vars.push(parseQuery(res)); if (lex()!=LX_RPR) throw SY_MISRPR;}
		QUERY_SETOP type=QRY_ALL_SETOP; DistinctType dt=DT_DEFAULT;
		if ((lx=lex())==LX_KEYW) switch (v.op) {
		case KW_UNION: type=QRY_UNION; break;
		case KW_EXCEPT: type=QRY_EXCEPT; break;
		case KW_INTERSECT: type=QRY_INTERSECT; break;
		}
		if (type!=QRY_ALL_SETOP && (lx=lex())==LX_KEYW) {
			if (v.op==KW_DISTINCT) dt=DT_DISTINCT; else if (v.op==KW_ALL) dt=DT_ALL; else nextLex=lx;
		} else if (lx!=LX_EOE) nextLex=lx;
		if (type!=QRY_INTERSECT) for (OpD op;;) {
			if (ops.isEmpty()) {
				if (type!=QRY_ALL_SETOP) break;
				if (!fNested) {
					if ((lx=lex())==LX_KEYW && v.op==KW_ORDER) lx=parseOrderOrGroup(res,INVALID_QVAR_ID);
					if (lx!=LX_EOE) nextLex=lx;
				}
				return vars.top();
			}
			if ((op=ops.top()).op==type && op.op!=QRY_EXCEPT) break;
			QVarID var; unsigned nv;
			for (nv=2,ops.pop(); !ops.isEmpty() && ops.top()==op; ops.pop()) nv++;
			if ((var=res->setOp(vars.pop(nv),nv,op.op))==INVALID_QVAR_ID) throw RC_NORESOURCES;
			if (op.dt!=DT_DEFAULT) res->setDistinct(var,op.dt);
			vars.push(var);
		}
		ops.push(OpD(type,dt)); res->top=NULL;
	}
}

QVarID SInCtx::parseSelect(Stmt* stmt,bool fMod)
{
	RC rc=RC_OK; TLx lx=lex(); assert(stmt!=NULL);
	QVarID var=INVALID_QVAR_ID; DistinctType dt=DT_DEFAULT; DynArray<Value> outs(ma); QVarRef qr; qr.var=NULL;
	if (!fMod) {
		if (lx==LX_KEYW) {if (v.op==KW_ALL) {dt=DT_ALL; lx=lex();} else if (v.op==KW_DISTINCT) {dt=DT_DISTINCT; lx=lex();}}
		if (lx==OP_MUL) {
			lx=lex();	// check next keyword -> 
		} else if (lx!=LX_KEYW) {
			nextLex=lx; const bool fSel=(mode&SIM_SELECT)!=0; mode|=SIM_SELECT;
			// multiple pins???
			do {
				Value w; parse(w); w.property=STORE_INVALID_PROPID;										// cleanup?
				if ((w.flags&HEAP_TYPE_MASK)!=ma->getAType() && (rc=copyV(w,w,ma))!=RC_OK) break;
				if (w.type==VT_VARREF) w.meta|=META_PROP_EVAL;
				if ((lx=lex())==LX_KEYW && v.op==KW_AS) {
					if (lex()!=LX_IDENT || v.type!=VT_STRING && v.type!=VT_URIID) throw SY_MISPROP;		// cleanup?
					mapURI(); w.property=v.uid;
				} else if (lx==LX_IDENT && (v.type==VT_STRING || v.type==VT_URIID)) {
					mapURI(); w.property=v.uid;
				} else nextLex=lx;
				if ((rc=outs+=w)!=RC_OK) break;
			} while ((lx=lex())==LX_COMMA);
			if (rc!=RC_OK) {for (unsigned i=0; i<outs; i++) freeV(*(Value*)&outs[i]); throw rc;}
			if (!fSel) mode&=~SIM_SELECT;
		}
	}
	if (lx==LX_KEYW && v.op==KW_FROM) {
		if (fMod && stmt->top!=NULL) throw SY_SYNTAX;
		var=parseFrom(stmt); lx=lex(); assert(var!=INVALID_QVAR_ID);
	} else if (stmt->top!=NULL) var=stmt->top->id;
	else if ((var=stmt->addVariable())==INVALID_QVAR_ID) throw RC_NORESOURCES;
	if (dt!=DT_DEFAULT) stmt->setDistinct(var,dt);
	if (lx==LX_KEYW && v.op==KW_WHERE) {
		qr.var=stmt->findVar(var); assert(qr.var!=NULL);
		ExprTree *pe=qr.var->type<=QRY_FULL_OUTER_JOIN?parseCondition(((JoinVar*)qr.var)->vars,((JoinVar*)qr.var)->nVars):parseCondition(&qr,1);
		if (pe!=NULL) {rc=splitWhere(stmt,qr.var,pe); pe->destroy(); if (rc!=RC_OK) throw rc;}
		lx=lex();
	}
	for (; lx==LX_KEYW && v.op==KW_MATCH; lx=lex()) {
		QVarID mvar=INVALID_QVAR_ID; PropertyID pid=STORE_INVALID_PROPID; QVar *qv;
		if ((lx=lex())==LX_LPR) {
			//QVarRef qr; qr.var=stmt->findVar(var); assert(qr.var!=NULL);
			//ExprTree *pe=qr.var->type<=QRY_OUTERJOIN?parseCondition(((JoinVar*)qr.var)->vars,((JoinVar*)qr.var)->nVars):parseCondition(&qr,1);
			// mvar=findVar(v,vars,nVars);
			//if ((lx=lex())!=LX_PERIOD) nextLex=lx;
			//else if (lex()!=LX_IDENT) throw SY_MISPROP;
			//else {mapURI(); assert(v.type==VT_URIID); pid=v.uid;}
			if (lex()==LX_RPR) lx=lex(); else throw SY_MISRPR;
		} else if ((qv=stmt->findVar(mvar=var))==NULL || qv->type!=QRY_SIMPLE) throw SY_MISLPR;
		if (lx!=LX_IDENT || v.type!=VT_STRING || v.length!=sizeof(AGAINST_S)-1 || !cmpncase(v.str,AGAINST_S,v.length))
			throw SY_MISAGNS;
		if (lex()!=LX_LPR) throw SY_MISLPR; 
		do {
			if (lex()!=LX_CON || v.type!=VT_STRING) throw SY_MISCON;
			char *str=new(ses) char[v.length+1]; if (str==NULL) throw RC_NORESOURCES;
			memcpy(str,v.str,v.length); str[v.length]='\0';
			// flags, pin ids ???
			if ((rc=stmt->addConditionFT(mvar,str,0,pid!=STORE_INVALID_PROPID?&pid:(PropertyID*)0,pid!=STORE_INVALID_PROPID?1:0))!=RC_OK) throw rc;
		} while ((lx=lex())==LX_COMMA);
		if (lx!=LX_RPR) throw SY_MISRPR;
	}
	uint32_t nOuts=0; Value *os=outs.get(nOuts);
	if (nOuts!=0) {
		if (qr.var==NULL) {qr.var=stmt->findVar(var); assert(qr.var!=NULL);}
		for (unsigned i=0; i<nOuts; i++) try {resolveVars(&qr,os[i]);} catch (...) {freeV(os,nOuts,ma); throw;}
	}
	if (!fMod && lx==LX_KEYW && v.op==KW_GROUP) try {lx=parseOrderOrGroup(stmt,var,os,nOuts);} catch (...) {if (os!=NULL) freeV(os,nOuts,ma); throw;}
	if (nOuts!=0 && (rc=stmt->addOutputNoCopy(var,os,nOuts))!=RC_OK) {freeV(os,nOuts,ma); throw rc;}
	if (lx!=LX_EOE) nextLex=lx;
	return var;
}

QVarID SInCtx::parseFrom(Stmt *stmt)
{
	TLx lx; RC rc; int jkw=-1; assert(stmt!=NULL); PathSeg *pseg=NULL; unsigned nSegs=0;
	for (QVarID var=INVALID_QVAR_ID,prev=INVALID_QVAR_ID;;) {
		bool fAs=true;
		switch (lx=lex()) {
		case LX_LPR:
			if ((lx=lex())!=LX_KEYW) {nextLex=lx; var=parseFrom(stmt); fAs=false;}
			else if (v.op!=KW_SELECT) throw SY_MISSEL;
			else {Stmt *subq=NULL; parseQuery(subq); var=stmt->addVariable(subq);}
			if (lex()!=LX_RPR) throw SY_MISRPR;
			break;
		case OP_MUL:
			var=stmt->addVariable(); break;
		case LX_LCBR:
			{DynArray<PID> pids(ma);
			do {
				if (lex()!=LX_CON || v.type!=VT_REFID) throw SY_MISPID;
				if ((rc=pids+=v.id)!=RC_OK) throw rc;
			} while ((lx=lex())==LX_COMMA);
			if (lx!=LX_RCBR) throw SY_MISRCBR;
			if ((var=stmt->addVariable())!=INVALID_QVAR_ID && (rc=stmt->setPIDs(var,pids,pids))!=RC_OK) throw rc;
			} break;
		case LX_CON:
			if (v.type!=VT_REFID) throw SY_UNKVAR;
			if ((var=stmt->addVariable())!=INVALID_QVAR_ID && (rc=stmt->setPIDs(var,&v.id,1))!=RC_OK) throw rc;
			break;
		case LX_IDENT: case LX_COLON:
			var=parseClasses(stmt,lx==LX_COLON); break;
		default: throw SY_UNKVAR;
		}
		if (var==INVALID_QVAR_ID) throw SY_INVVAR;
		if (fAs) {
			if ((lx=lex())==LX_PERIOD) {
				Value v; parse(v,NULL,0,PRS_PATH); unsigned nS=0; const Value *pv; PathSeg *ps;
				for (pv=&v; pv->type==VT_EXPRTREE && pv->exprt->getOp()==OP_PATH; pv=&pv->exprt->getOperand(0)) nS++;
				if (nS==0) {freeV(v); throw SY_SYNTAX;}	// SY_INVPTH
				if (nS>nSegs && (pseg=(PathSeg*)alloca((nSegs=nS)*sizeof(PathSeg)))==NULL) {freeV(v); throw RC_NORESOURCES;}
				for (pv=&v,ps=&pseg[nS-1]; pv->type==VT_EXPRTREE && pv->exprt->getOp()==OP_PATH; pv=&pv->exprt->getOperand(0),--ps) {
					assert(ps>=pseg && pv->type==VT_EXPRTREE && pv->exprt->getOp()==OP_PATH);
					ExprTree *et=(ExprTree*)pv->exprt; assert(ps!=pseg || et->operands[0].type==VT_VARREF && et->operands[0].length==0);
					if (et->operands[1].type!=VT_URIID) {freeV(v); throw SY_SYNTAX;}
					if ((rc=et->toPathSeg(*ps,ses))!=RC_OK) {freeV(v); throw rc;}
				}
				rc=stmt->setPath(var,pseg,nS,false); freeV(v); if (rc!=RC_OK) throw rc; lx=lex();
			}
			if (lx!=LX_KEYW || v.op!=KW_AS) nextLex=lx;
			else {
				mode|=SIM_SIMPLE_NAME;
				if (lex()!=LX_IDENT || v.type!=VT_STRING) throw SY_MISNAME;
				mode&=~SIM_SIMPLE_NAME; size_t l=min(v.length,uint32_t(255));
				QVar *qv=stmt->findVar(var); assert(qv!=NULL && qv->name==NULL);
				if ((qv->name=new(ses) char[l+1])==NULL) throw RC_NORESOURCES;
				memcpy(qv->name,v.str,l); qv->name[l]=0;
			}
		}
		if (jkw!=-1) {
			assert(prev!=INVALID_QVAR_ID);
			ExprTree *pe=NULL; DynArray<PropertyID> props(ma);
			if (jkw!=KW_CROSS) {
				if ((lx=lex())==LX_KEYW && v.op==KW_ON) {
					QVarRef vars[2]; vars[0].var=stmt->findVar(prev); vars[1].var=stmt->findVar(var);
					assert(vars[0].var!=NULL && vars[1].var!=NULL); pe=parseCondition(vars,2);
				} else if (lx!=LX_KEYW || v.op!=KW_USING) nextLex=lx;
				else if (lex()!=LX_LPR) throw SY_MISLPR;
				else {
					do {
						if (lex()!=LX_IDENT) throw SY_MISPROP;
						mapURI(); assert(v.type==VT_URIID);
						if ((rc=props+=v.uid)!=RC_OK) throw rc;
					} while ((lx=lex())==LX_COMMA);
					if (lx!=LX_RPR) throw SY_MISRPR;
				}
			}
			var=stmt->join(prev,var,pe,jkw==KW_LEFT?QRY_LEFT_OUTER_JOIN:jkw==KW_RIGHT?QRY_RIGHT_OUTER_JOIN:jkw==KW_OUTER?QRY_FULL_OUTER_JOIN:QRY_JOIN);		// ???????
			if (pe!=NULL) pe->destroy(); if (var==INVALID_QVAR_ID) throw SY_UNKVAR;	// memory???
			if ((unsigned)props!=0 && (rc=stmt->setJoinProperties(var,props,props))!=RC_OK) throw rc;
		}
		if ((lx=lex())==LX_KEYW) switch (jkw=v.op) {
		default: break;
		case KW_LEFT: case KW_RIGHT:
			if ((lx=lex())!=LX_KEYW || v.op!=KW_OUTER) nextLex=lx;
		case KW_OUTER: case KW_INNER: case KW_CROSS:
			if ((lx=lex())!=LX_KEYW || v.op!=KW_JOIN) throw SY_MISJOIN;
		case KW_JOIN: prev=var; var=INVALID_QVAR_ID; continue;
		} else if (lx==LX_COMMA) {
			jkw=KW_CROSS; prev=var; var=INVALID_QVAR_ID; continue;
		}
		if (lx!=LX_EOE) nextLex=lx;
		return var;
	}
}

TLx SInCtx::parseOrderOrGroup(Stmt *stmt,QVarID var,Value *os,unsigned nO)
{
	if (lex()!=LX_KEYW || v.op!=KW_BY) throw SY_MISBY; assert(stmt!=NULL);
	DynArray<OrderSeg> segs(ses); TLx lx; RC rc=RC_OK;
	do {
		Value w; parse(w,NULL,0,PRS_COPYV);	// vars from Transform?
		OrderSeg &sg=segs.add(); sg.expr=NULL; sg.pid=STORE_INVALID_PROPID; sg.flags=0; sg.var=0; sg.lPrefix=0;
		if (w.type==VT_URIID) sg.pid=w.uid;
		else if (w.type==VT_VARREF && w.length==1) {sg.pid=w.refV.id; sg.var=w.refV.refN;}
		else if ((rc=ExprTree::forceExpr(w,ses,true))!=RC_OK) throw rc;
		else sg.expr=(ExprTree*)w.exprt;
		if ((lx=lex())==LX_KEYW && (v.op==KW_DESC||v.op==KW_ASC)) {if (v.op==KW_DESC) sg.flags|=ORD_DESC; lx=lex();}
		if (lx==LX_KEYW && v.op==KW_NULLS) {
			if (lex()!=LX_IDENT || v.type!=VT_STRING) throw SY_SYNTAX;
			if (v.length==sizeof(FIRST_S)-1 && cmpncase(v.str,FIRST_S,sizeof(FIRST_S)-1)) sg.flags|=ORD_NULLS_BEFORE;
			else if (v.length==sizeof(LAST_S)-1 && cmpncase(v.str,LAST_S,sizeof(LAST_S)-1)) sg.flags|=ORD_NULLS_AFTER;
			else throw SY_SYNTAX;
			lx=lex();
		}
	} while (lx==LX_COMMA);
	if (segs!=0) {
		if (var==INVALID_QVAR_ID) rc=stmt->setOrder(segs,segs);
		else {
			ExprTree *having=NULL;
			if (lx==LX_KEYW && v.op==KW_HAVING) {
				const bool fHave=(mode&SIM_HAVING)!=0; mode|=SIM_HAVING;
				having=parseCondition(NULL,0); lx=lex(); if (!fHave) mode&=~SIM_HAVING;
				Value w; w.set(having); rc=replaceGroupExprs(w,segs,segs);
				assert(w.type==VT_EXPRTREE); having=(ExprTree*)w.exprt;
			}
			if (rc==RC_OK && os!=NULL) for (unsigned i=0; i<nO; i++) if ((rc=replaceGroupExprs(os[i],segs,segs))!=RC_OK) break;		// check all converted
			if (rc==RC_OK) rc=stmt->setGroup(var,segs,segs,having); if (having!=NULL) having->destroy();
		}
		for (unsigned i=0; i<segs; i++) if (segs[i].expr!=NULL) segs[i].expr->destroy();
	}
	if (rc!=RC_OK) throw rc;
	return lx;
}

QVarID SInCtx::parseClasses(Stmt *stmt,bool fColon)
{
	DynArray<ClassSpec> css(ma); unsigned i; RC rc=RC_OK; SynErr sy=SY_ALL;
	TLx lx; QVarID var=INVALID_QVAR_ID;
	for (;;) {
		if (!fColon) {mapURI(); assert(v.type==VT_URIID);}
		else {
			mode|=SIM_INT_NUM;
			if (lex()!=LX_CON || v.type!=VT_INT) {sy=SY_MISNUM; break;}
			else if (v.i<0) {sy=SY_INVNUM; break;} else if (v.i>255) {sy=SY_TOOBIG; break;}
			else {v.uid=(URIID)(v.i|CLASS_PARAM_REF);}
			mode&=~SIM_INT_NUM;
		}
		ClassSpec &cs=css.add(); if (&cs==NULL) {rc=RC_NORESOURCES; break;}
		cs.classID=v.uid; cs.params=NULL; cs.nParams=0; fColon=false;
		if ((lx=lex())==LX_LPR) {
			unsigned xP=0;
			do {
				Value w;
				switch (lx=lex()) {
				case OP_MUL: w.setError(); break;
				case LX_KEYW: if (v.op==KW_NULL) {w.setError(); w.meta=META_PROP_EVAL; break;}
				default: nextLex=lx; parse(w); break;
				}
				if (w.type==VT_EXPRTREE || w.type==VT_VARREF && (w.refV.flags&VAR_TYPE_MASK)!=VAR_PARAM) {sy=SY_MISCON; break;}
				if (cs.nParams>=xP) {
					cs.params=(Value*)ses->realloc((Value*)cs.params,(xP+=xP==0?16:xP)*sizeof(Value));
					if (cs.params==NULL) {rc=RC_NORESOURCES; break;}
				}
				if (w.flags==SES_HEAP) {*(Value*)&cs.params[cs.nParams]=w; cs.nParams++;}
				else {if ((rc=copyV(w,*(Value*)&cs.params[cs.nParams],ses))==RC_OK) cs.nParams++; freeV(w);}
			} while (rc==RC_OK && (lx=lex())==LX_COMMA);
			if (rc!=RC_OK || sy!=SY_ALL) break; if (lx!=LX_RPR) {sy=SY_MISRPR; break;}
			lx=lex();
		}
		if (lx!=OP_AND) {nextLex=lx; break;}
		if ((lx=lex())!=LX_IDENT) {if (lx==LX_COLON) fColon=true; else {sy=SY_UNKVAR; break;}}
	}
	if (rc==RC_OK && sy==SY_ALL) var=stmt->addVariable(css,css);		// NoCopy?
	for (i=css; i--!=0; ) if (css[i].params!=NULL) freeV((Value*)css[i].params,css[i].nParams,ses);
	if (sy!=SY_ALL) throw sy; if (rc!=RC_OK) throw rc;
	if ((lx=lex())!=LX_LCBR) nextLex=lx;
	else {
		DynArray<PID> pids(ma);
		do {
			if (lex()!=LX_CON || v.type!=VT_REFID) throw SY_MISPID;
			if ((rc=pids+=v.id)!=RC_OK) throw rc;
		} while ((lx=lex())==LX_COMMA);
		if (lx!=LX_RCBR) throw SY_MISRCBR;
		if ((rc=stmt->setPIDs(var,pids,pids))!=RC_OK) throw rc;
	}
	return var;
}

ExprTree *SInCtx::parseCondition(const QVarRef *vars,unsigned nVars)
{
	Value w; parse(w,vars,nVars); RC rc;
	if (w.type!=VT_EXPRTREE) {
		if (w.type!=VT_BOOL) {freeV(w); throw SY_MISBOOL;}
		if (w.b) return NULL;
		if ((rc=ExprTree::forceExpr(w,ses))!=RC_OK) throw rc;
	} else if (!isBool(w.exprt->getOp())) {freeV(w); throw SY_MISBOOL;}
	return (ExprTree*)w.exprt;
}

RC SInCtx::splitWhere(Stmt *stmt,QVar *qv,ExprTree *pe)
{
	assert(stmt!=NULL && qv!=NULL && pe!=NULL); RC rc; byte id=INVALID_QVAR_ID;
	switch (pe->vrefs) {
	case NO_VREFS:
		if (qv->type>=QRY_ALL_SETOP) id=qv->id;
		else {
			// find first suitable
		}
		return stmt->addCondition(id,pe);
	default:
		if (byte(pe->vrefs)==byte(pe->vrefs>>8)) {if ((id=byte(pe->vrefs))!=0) pe->mapVRefs(id,0); return stmt->addCondition(id,pe);}
	case MANY_VREFS: break;
	}
	if (pe->op==OP_LAND) {
		assert(pe->operands[0].type==VT_EXPRTREE && pe->operands[1].type==VT_EXPRTREE);
		return (rc=splitWhere(stmt,qv,(ExprTree*)pe->operands[0].exprt))==RC_OK?
			splitWhere(stmt,qv,(ExprTree*)pe->operands[1].exprt):rc;
	}
	if (qv->type<=QRY_FULL_OUTER_JOIN) {
		JoinVar *jv=(JoinVar*)qv; 
		if (pe->vrefs!=MANY_VREFS) {
			byte to1=0xFF,to2=0xFF; unsigned i;
			for (i=0; i<jv->nVars; i++) if (jv->vars[i].var->id==byte(pe->vrefs>>8)) {to1=i; break;}
			if (to1!=0xFF) for (i=0; i<jv->nVars; i++) if (jv->vars[i].var->id==byte(pe->vrefs)) {to2=i; break;}
			if (to2!=0xFF) {pe->mapVRefs(byte(pe->vrefs>>8),to1); pe->mapVRefs(byte(pe->vrefs),to2); return stmt->addCondition(qv->id,pe);}
			for (i=0; i<jv->nVars; i++) if (jv->vars[i].var->type<=QRY_FULL_OUTER_JOIN && splitWhere(stmt,jv->vars[i].var,pe)==RC_OK) return RC_OK;
		}
		//???
	}
	return RC_NOTFOUND;
}

void SInCtx::resolveVars(QVarRef *qv,Value& vv,Value *par)
{
	if (vv.type==VT_EXPRTREE) {
		ExprTree *pe=(ExprTree*)vv.exprt;
		for (unsigned i=0; i<pe->nops; i++) {
			Value *pv=&pe->operands[i];
			switch (pv->type) {
			default: break;
			case VT_EXPRTREE:
				if (((ExprTree*)pv->exprt)->vrefs!=NO_VREFS) resolveVars(qv,*pv); break;
			case VT_VARREF:
				resolveVars(qv,*pv,i==0 && pe->op==OP_PATH && pe->operands[1].type==VT_URIID?&vv:(Value*)0); break;
			}
			if (vv.type!=VT_EXPRTREE) break;
		}
	} else if (vv.type==VT_VARREF) {
		if (vv.refV.flags==0xFFFF) {
			vv.refV.flags=0; Len_Str *ls=(Len_Str*)&dnames[vv.refV.refN]; assert(vv.refV.refN<dnames);
			if (ls->s!=NULL) {
				Value save=v; v.set(ls->s,(uint32_t)ls->l); QVarID var=findVar(v,qv,1);
				if (var!=INVALID_QVAR_ID) {ls->s=NULL; ls->l=var|0x80000000;} else {mapURI(); ls->s=NULL; ls->l=v.uid;}	// check there is only one var in stmt
				 v=save;
			}
			if ((ls->l&0x80000000)!=0) vv.refV.refN=byte(ls->l&~0x80000000);
			else if (qv->var->type<QRY_UNION) throw SY_MISNAME;
			else if (vv.length==0) {vv.refV.refN=0; vv.refV.id=(URIID)ls->l; vv.length=1;} 
			else {
				// path
				throw RC_INTERNAL;
			}				
		} else if (vv.length==1) {
			Value save=v; v.setURIID(vv.refV.id); QVarID var=findVar(v,qv,1); v=save;
			if (var==INVALID_QVAR_ID) {if (qv->var->type<QRY_UNION) throw SY_MISNAME;}
			else if (par==NULL) {vv.length=0; vv.refV.refN=var;}
			else {ExprTree *pe=(ExprTree*)par->exprt;	par->setVarRef(var,pe->operands[1].uid); if (pe->nops>=3) par->eid=pe->operands[2].ui; ma->free(pe);}
		}
	}
}

RC SInCtx::replaceGroupExprs(Value& v,const OrderSeg *segs,unsigned nSegs)
{
	ExprTree *et; RC rc;
	if (v.type==VT_VARREF) {
		if (v.length==1 && (v.refV.flags&VAR_TYPE_MASK)==0) for (unsigned i=0; i<nSegs && i<256; i++) 
			if (segs[i].expr==NULL && segs[i].var==v.refV.refN && segs[i].pid==v.refV.id) {
				if (v.property==PROP_SPEC_ANY) v.property=v.refV.id;
				v.length=0; v.refV.refN=byte(i); v.refV.type=VT_ANY; v.refV.flags|=VAR_GROUP; return RC_OK;
			}
		// not found -> ???
	} else if (v.type==VT_EXPRTREE && ((et=(ExprTree*)v.exprt)->op>=OP_ALL || (SInCtx::opDscr[et->op].flags&_A)==0)) {
		if (!isBool(et->op)) for (unsigned i=0; i<nSegs && i<256; i++) if (segs[i].expr!=NULL && *(ExprTree*)segs[i].expr==*et) 
			{et->destroy(); v.type=VT_VARREF; v.length=0; v.refV.refN=byte(i); v.refV.type=VT_ANY; v.refV.flags=VAR_GROUP; return RC_OK;}
		for (unsigned i=0; i<et->nops; i++) if ((rc=replaceGroupExprs(et->operands[i],segs,nSegs))!=RC_OK) return rc;
	}
	return RC_OK;
}

bool SInCtx::parseEID(TLx lx,ElementID& eid)
{
	if (lx!=LX_COLON) {
		if (lx==LX_CON && (v.type==VT_INT || v.type==VT_UINT)) {eid=v.ui; return true;}
	} else {
		if ((lx=lex())==LX_IDENT && v.type==VT_STRING) {
			if (v.length==sizeof(FIRST_S)-1 && cmpncase(v.str,FIRST_S,sizeof(FIRST_S)-1)) {eid=STORE_FIRST_ELEMENT; return true;}
			if (v.length==sizeof(LAST_S)-1 && cmpncase(v.str,LAST_S,sizeof(LAST_S)-1)) {eid=STORE_LAST_ELEMENT; return true;}
		}
		nextLex=lx;
	}
	return false;
}

inline bool checkProp(URIID uid,bool fNew)
{
	if (uid<PROP_SPEC_LAST) switch (uid) {
	case PROP_SPEC_PINID: case PROP_SPEC_STAMP: case PROP_SPEC_INDEX_INFO: case PROP_SPEC_PROPERTIES:
	case PROP_SPEC_NINSTANCES: case PROP_SPEC_NDINSTANCES: case PROP_SPEC_CLASS_INFO: case PROP_SPEC_PARENT: return false;
	case PROP_SPEC_CREATED: case PROP_SPEC_CREATEDBY: case PROP_SPEC_CLASSID: if (!fNew) return false; break;
	default: break;
	}
	return true;
}

Stmt *SInCtx::parseStmt(bool fNested)
{
	Stmt *stmt=NULL; TLx lx; uint8_t op; unsigned md,msave=mode&SIM_DML_EXPR; DynArray<Value,40> props(ma); 
	SynErr sy=SY_ALL; RC rc=RC_OK; Value w; ElementID eid,eid2; QVarID var; Class *cls; Value vals[3];
	switch (lx=fNested?lex():parsePrologue()) {
	default: throw SY_SYNTAX;
	case LX_EOE: break;
	case LX_LPR: v.op=KW_SELECT;
	case LX_KEYW:
		md=0;
		switch (op=v.op) {
		case KW_SELECT:
			nextLex=lx; stmt=NULL; parseQuery(stmt,false); break;
		case KW_INSERT:
			mode|=SIM_DML_EXPR;
			if ((lx=lex())==LX_IDENT && msave!=0 && v.type==VT_STRING && v.length==sizeof(PART_S)-1 && cmpncase(v.str,PART_S,sizeof(PART_S)-1))
				{md=MODE_PART; lx=lex();}
			if (lx==LX_KEYW && v.op==KW_AS) {
				// read id
				lx=lex();
			}
			if (lx==LX_IDENT && v.type==VT_STRING && v.length==sizeof(INTO_S)-1 && cmpncase(v.str,INTO_S,sizeof(INTO_S)-1)) {
				//read classes with & and UNIQUE if family
				lx=lex();
			}
			if (lx==LX_IDENT && v.type==VT_STRING && v.length==sizeof(OPTIONS_S)-1 && cmpncase(v.str,OPTIONS_S,sizeof(OPTIONS_S)-1)) {
				if (lex()!=LX_LPR) throw SY_MISLPR;
				//...
				lx=lex();
			}
			if (lx==LX_KEYW && (v.op==KW_SELECT || v.op==KW_FROM || v.op==KW_WHERE || v.op==KW_MATCH)) nextLex=lx;
			else if (lx==LX_LPR) {
				do {
					if (lex()!=LX_IDENT) throw SY_MISPROP;
					mapURI(); assert(v.type==VT_URIID);
					if (!checkProp(v.uid,true)) throw SY_INVPROP;
					v.property=v.uid; if ((rc=props+=v)!=RC_OK) throw rc;
				} while ((lx=lex())==LX_COMMA);
				if (lx!=LX_RPR) throw SY_MISRPR;
				if (lex()!=LX_KEYW || v.op!=KW_VALUES) throw SY_SYNTAX;
				if (lex()!=LX_LPR) throw SY_MISLPR; unsigned i=0;
				do {
					if (i>=props) {sy=SY_MANYARG; break;}
					if ((lx=lex())==LX_KEYW && v.op==KW_NULL) props-=i;
					else {
						nextLex=lx; parse(w); Value *pv=(Value*)&props[i++];
						if (w.type==VT_VARREF) w.meta|=META_PROP_EVAL;
						w.property=pv->property; rc=copyV(w,*pv,ma); freeV(w);
					}
				} while (rc==RC_OK && (lx=lex())==LX_COMMA);
				if (rc==RC_OK && sy==SY_ALL) {if (lx!=LX_RPR) sy=SY_MISRPR; else if (i<props) sy=SY_FEWARG;}
			} else for (;;lx=lex()) {
				if (lx!=LX_IDENT) {if (props!=0) sy=SY_MISPROP; else if (lx!=LX_EOE) {if (lx==LX_SEMI) nextLex=lx; else sy=SY_MISPROP;} break;}
				mapURI(); assert(v.type==VT_URIID); URIID uid=v.uid;
				if (!checkProp(uid,true)) {sy=SY_INVPROP; break;}
				if (lex()!=OP_EQ) {sy=SY_MISEQ; break;}
				if ((lx=lex())!=LX_KEYW || v.op!=KW_NULL) {
					nextLex=lx; parse(w);
					if ((w.flags&HEAP_TYPE_MASK)!=ma->getAType() && (rc=copyV(w,w,ma))!=RC_OK) break;
					if (w.type==VT_VARREF) w.meta|=META_PROP_EVAL;
					w.property=uid; if ((rc=props+=w)!=RC_OK) break;
				}
				if ((lx=lex())!=LX_COMMA) {nextLex=lx; break;}
			}
			mode=mode&~SIM_DML_EXPR|msave;
			if (rc==RC_OK && sy==SY_ALL) {
				uint32_t nVals=0; Value *vals=props.get(nVals);
				if (nVals!=0) vals=PIN::normalize(vals,nVals,PIN_NO_FREE,ses!=NULL?ses->getStore()->getPrefix():0,ma);
				rc=(stmt=new(ma) Stmt(md,ma,STMT_INSERT))==NULL?RC_NORESOURCES:nVals==0?RC_OK:stmt->setValuesNoCopy(vals,nVals);
				if (rc!=RC_OK) freeV(vals,nVals,ma);
				else if ((lx=lex())!=LX_KEYW || v.op!=KW_SELECT && v.op!=KW_FROM && v.op!=KW_WHERE && v.op!=KW_MATCH) nextLex=lx;
				else try {if (v.op!=KW_SELECT) nextLex=lx; parseSelect(stmt,v.op!=KW_SELECT);} catch (...) {stmt->destroy(); throw;}
			} else for (unsigned i=0; i<props; i++) freeV(*(Value*)&props[i]);
			break;
		case KW_UPDATE:
			if ((stmt=new(ma) Stmt(0,ma,STMT_UPDATE))==NULL) throw RC_NORESOURCES;
			if ((lx=lex())==LX_CON && v.type==VT_REFID) {
				rc=(var=stmt->addVariable())!=INVALID_QVAR_ID?stmt->setPIDs(var,&v.id,1):RC_NORESOURCES; lx=lex();
			} else if (lx==LX_LCBR) {
				DynArray<PID,10> pids(ma);
				//...
				lx=lex();
			} else if (lx==LX_IDENT||lx==LX_COLON) {
				try {var=parseClasses(stmt,lx==LX_COLON);} catch (...) {stmt->destroy(); throw;} lx=lex();
			} else if (lx==OP_MUL) {
				if ((var=stmt->addVariable())==INVALID_QVAR_ID) rc=RC_NORESOURCES; lx=lex();
			}
			mode|=SIM_DML_EXPR;
			while (rc==RC_OK && sy==SY_ALL && lx==LX_KEYW) {
				switch (v.op) {
				case KW_SET: op=OP_SET; break;
				case KW_ADD: op=OP_ADD; break;
				case KW_DELETE: op=OP_DELETE; break;
				case KW_RENAME: op=OP_RENAME; break;
				case KW_EDIT: op=OP_EDIT; break;
				case KW_MOVE: op=OP_MOVE; break;
				default: op=0xFF; if (props==0) sy=SY_SYNTAX; break;
				}
				if (op==0xFF) break;
				do {
					if (lex()!=LX_IDENT) {sy=SY_MISPROP; break;}
					mapURI(); assert(v.type==VT_URIID);
					URIID uid=v.uid; if (!checkProp(v.uid,false)) {sy=SY_INVPROP; break;}
					w.setError(); eid=STORE_COLLECTION_ID; byte eop=op; byte meta=0;
					if ((lx=lex())==LX_LBR) {
						if (op==OP_RENAME) {sy=SY_SYNTAX; break;}
						if ((lx=lex())==LX_IDENT && v.type==VT_STRING && v.length==sizeof(BEFORE_S)-1 && cmpncase(v.str,BEFORE_S,sizeof(BEFORE_S)-1))
							{if (op==OP_ADD) {eop=OP_ADD_BEFORE; lx=lex();} else {sy=SY_SYNTAX; break;}}
						if (!parseEID(lx,eid)) {sy=SY_MISCON; break;}
						if (lex()!=LX_RBR) {sy=SY_MISRBR; break;} lx=lex();
					}
					if (lx==LX_QUEST) {meta|=META_PROP_IFEXIST; lx=lex();} else if (lx==LX_EXCL) {meta|=META_PROP_IFNOTEXIST; lx=lex();}
					switch (op) {
					case OP_EDIT: //???
					case OP_DELETE:
						nextLex=lx; break;
					case OP_RENAME: 
						if (lx!=OP_EQ) sy=SY_MISEQ; else if (lex()!=LX_IDENT) sy=SY_MISPROP; else {mapURI(); w=v;}
						break;
					case OP_MOVE:
						if (eid==STORE_COLLECTION_ID) {sy=SY_SYNTAX; break;}
						if (lx==LX_IDENT && v.type==VT_STRING && v.length==sizeof(BEFORE_S)-1 && cmpncase(v.str,BEFORE_S,sizeof(BEFORE_S)-1))
							eop=OP_MOVE_BEFORE;
						else if (lx!=LX_IDENT || v.type!=VT_STRING || v.length!=sizeof(AFTER_S)-1 || !cmpncase(v.str,AFTER_S,sizeof(AFTER_S)-1))
							{sy=SY_SYNTAX; break;}
						if (!parseEID(lx=lex(),eid2)) sy=SY_MISCON; else w.set((unsigned)eid2); 
						break;
					case OP_ADD: case OP_ADD_BEFORE:
						if (lx!=OP_EQ) {sy=SY_MISEQ; break;}
					case OP_SET:
						if (lx!=OP_EQ && ((eop=lx)<OP_FIRST_EXPR || lx>OP_CONCAT || lex()!=OP_EQ)) {sy=SY_MISEQ; break;}
						if ((lx=lex())==LX_KEYW && v.op==KW_NULL && op==OP_SET) eop=OP_DELETE;
						else {
							nextLex=lx; parse(w); if ((w.flags&HEAP_TYPE_MASK)!=ma->getAType()) rc=copyV(w,w,ma);
							if (w.type==VT_EXPRTREE) {
								ExprTree *et=(ExprTree*)w.exprt;
								if (eop==OP_SET && et->op>=OP_FIRST_EXPR && et->op<=OP_CONCAT && et->nops==2) {
									const Value& lhs=et->operands[0];
									if (lhs.type==VT_VARREF && lhs.length==1 && lhs.refV.refN==0 && lhs.refV.id==uid)
										{eop=et->op; w=et->operands[1]; et->operands[1].flags=NO_HEAP; et->destroy();}
								}
							}
						}
						break;
					}
					w.property=uid; w.op=eop; w.eid=eid; w.meta|=meta;
				} while (sy==SY_ALL && rc==RC_OK && (rc=props+=w)==RC_OK && (lx=lex())==LX_COMMA);
			}
			mode=mode&~SIM_DML_EXPR|msave;
			if (rc==RC_OK && sy==SY_ALL) {
				nextLex=lx; uint32_t nVals=0; Value *vals=props.get(nVals);
				if (nVals==0) sy=SY_SYNTAX;
				else if ((rc=stmt->setValuesNoCopy(vals,nVals))!=RC_OK) freeV(vals,nVals,ma);
				if (rc==RC_OK && lx==LX_KEYW && (v.op==KW_FROM && stmt->top==NULL || v.op==KW_WHERE || v.op==KW_MATCH))
					try {parseSelect(stmt,true);} catch (...) {stmt->destroy(); throw;}
			} else for (unsigned i=0; i<props; i++) freeV(*(Value*)&props[i]);
			break;
		case KW_PURGE:
			md=MODE_PURGE;
		case KW_DELETE: case KW_UNDELETE:
			if ((stmt=new(ma) Stmt(md,ma,op==KW_UNDELETE?STMT_UNDELETE:STMT_DELETE))==NULL) throw RC_NORESOURCES;
			else {
				bool fLCBR=false;
				if ((lx=lex())==LX_LCBR) {fLCBR=true; if ((lx=lex())!=LX_CON || v.type!=VT_REFID) {stmt->destroy(); throw SY_MISPID;}}
				if (lx==LX_CON && v.type==VT_REFID) {
					DynArray<PID,10> pins(ma); if ((rc=pins+=v.id)!=RC_OK) break;
					while ((lx=lex())==LX_COMMA) {
						if (lex()!=LX_CON || v.type!=VT_REFID) {sy=SY_MISPID; break;}
						if ((rc=pins+=v.id)!=RC_OK) break;
					}
					if (!fLCBR) nextLex=lx; else if (lx!=LX_RCBR) sy=SY_MISRCBR;
					if (rc==RC_OK && sy==SY_ALL) rc=(var=stmt->addVariable())!=INVALID_QVAR_ID?stmt->setPIDs(var,pins,pins):RC_NORESOURCES;
				} else if (lx==LX_KEYW && (v.op==KW_FROM || v.op==KW_WHERE || v.op==KW_MATCH)) {
					nextLex=lx; try {parseSelect(stmt,true);} catch (...) {stmt->destroy(); throw;}
				} else sy=SY_SYNTAX;
			}
			break;
		case KW_CREATE:
			if (lex()!=LX_KEYW || v.op!=KW_CLASS) throw SY_SYNTAX; if (lex()!=LX_IDENT) throw SY_MISNAME;
			mapURI(); vals[0].setURIID(v.uid); vals[0].setPropID(PROP_SPEC_CLASSID); vals[1].setError(); vals[2].setError();
			if ((lx=lex())==LX_IDENT && v.type==VT_STRING && v.length==sizeof(OPTIONS_S)-1 && cmpncase(v.str,OPTIONS_S,sizeof(OPTIONS_S)-1)) {
				if (lex()!=LX_LPR) {freeV(vals[0]); throw SY_MISLPR;}
				unsigned flags=0;
				do {
					if (lex()==LX_IDENT && v.type==VT_STRING) {
						if (v.length==sizeof("VIEW")-1 && cmpncase(v.str,"VIEW",sizeof("VIEW")-1))
							{if ((flags&CLASS_VIEW)==0) {flags|=CLASS_VIEW; continue;}}
						if (v.length==sizeof("SOFT_DELETE")-1 && cmpncase(v.str,"SOFT_DELETE",sizeof("SOFT_DELETE")-1))
							{if ((flags&CLASS_SDELETE)==0) {flags|=CLASS_SDELETE; continue;}}
						if (v.length==sizeof("CLUSTERED")-1 && cmpncase(v.str,"CLUSTERED",sizeof("CLUSTERED")-1))
							{if ((flags&CLASS_CLUSTERED)==0) {flags|=CLASS_CLUSTERED; continue;}}
					}
					freeV(vals[0]); throw SY_SYNTAX;
				} while ((lx=lex())==LX_COMMA);
				if (lx!=LX_RPR) {freeV(vals[0]); throw SY_MISRPR;}
				lx=lex(); vals[2].set(flags); vals[2].setPropID(PROP_SPEC_CLASS_INFO);
			}
			if (lx!=LX_KEYW || v.op!=KW_AS) {freeV(vals[0]); throw SY_SYNTAX;}
			try {stmt=NULL; parseQuery(stmt);} catch (...) {if (stmt!=NULL) stmt->destroy(); freeV(vals[0]); throw;}
			vals[1].set(stmt); vals[1].setPropID(PROP_SPEC_PREDICATE); vals[1].flags=mtype;
			if ((stmt=new(ma) Stmt(MODE_CLASS,ma,STMT_INSERT))==NULL) rc=RC_NORESOURCES;
			else if ((lx=lex())!=LX_KEYW || v.op!=KW_SET) {nextLex=lx; rc=stmt->setValues(vals,vals[2].type==VT_ANY?2:3);}
			else {
				props+=vals[0]; props+=vals[1]; if (vals[2].type!=VT_ANY) props+=vals[2]; vals[0].setError(); vals[1].setError();
				for (;;) {
					if ((lx=lex())!=LX_IDENT) {if (props!=0) sy=SY_MISPROP; else if (lx!=LX_EOE) {if (lx==LX_SEMI) nextLex=lx; else sy=SY_MISPROP;} break;}
					mapURI(); assert(v.type==VT_URIID); URIID uid=v.uid;
					if (lex()!=OP_EQ) {sy=SY_MISEQ; break;}
					if ((lx=lex())!=LX_KEYW || v.op!=KW_NULL) {
						nextLex=lx; parse(w);
						if ((w.flags&HEAP_TYPE_MASK)!=ma->getAType() && (rc=copyV(w,w,ma))!=RC_OK) break;
						w.property=uid; if ((rc=props+=w)!=RC_OK) break;
					}
					if ((lx=lex())!=LX_COMMA) {nextLex=lx; break;}
				}
				if (rc==RC_OK && sy==SY_ALL) {
					uint32_t nVals=0; Value *pv=props.get(nVals);
					if (nVals!=0) pv=PIN::normalize(pv,nVals,PIN_NO_FREE,ses!=NULL?ses->getStore()->getPrefix():0,ma);
					if ((rc=stmt->setValuesNoCopy(pv,nVals))!=RC_OK) freeV(pv,nVals,ma);
				} else for (unsigned i=0; i<props; i++) freeV(*(Value*)&props[i]);
			}
			freeV(vals[0]); freeV(vals[1]); if (rc!=RC_OK) throw rc;
			break;
		case KW_DROP:
			if (lex()!=LX_KEYW || v.op!=KW_CLASS) throw SY_SYNTAX;
			if (lex()!=LX_IDENT) throw SY_MISNAME;
			mapURI();
			if ((cls=ses->getStore()->classMgr->getClass(v.uid))==NULL) rc=RC_NOTFOUND;
			else {
				const PID id=cls->getPID(); cls->release();
				if ((stmt=new(ma) Stmt(MODE_PURGE|MODE_CLASS,ma,STMT_DELETE))==NULL) throw RC_NORESOURCES;
				rc=(var=stmt->addVariable())!=INVALID_QVAR_ID?stmt->setPIDs(var,&id,1):RC_NORESOURCES;
			}
			break;
		case KW_START:
			if (lex()!=LX_IDENT || v.length!=sizeof(TRANSACTION_S)-1 || !cmpncase(v.str,TRANSACTION_S,sizeof(TRANSACTION_S)-1)) throw SY_SYNTAX;	//SY_MISTRX
			//
			if ((lx=lex())!=LX_IDENT || v.length!=sizeof(READ_S)-1 || !cmpncase(v.str,READ_S,sizeof(READ_S)-1)) nextLex=lx;
			else if ((lx=lex())==LX_IDENT && v.length==sizeof(ONLY_S)-1 || cmpncase(v.str,ONLY_S,sizeof(ONLY_S)-1)) md=MODE_READONLY;
			else if (lx!=LX_IDENT || v.length!=sizeof(WRITE_S)-1 || !cmpncase(v.str,WRITE_S,sizeof(WRITE_S)-1)) throw SY_SYNTAX;
			if ((stmt=new(ma) Stmt(md,ma,STMT_START_TX))==NULL) throw RC_NORESOURCES;
			break;
		case KW_COMMIT: case KW_ROLLBACK:
			if ((lx=lex())!=LX_KEYW || v.op!=KW_ALL) nextLex=lx; else md=MODE_ALL;
			if ((stmt=new(ma) Stmt(md,ma,op==KW_COMMIT?STMT_COMMIT:STMT_ROLLBACK))==NULL) throw RC_NORESOURCES;
			break;
		case KW_SET:
			if ((lx=lex())==LX_KEYW && (v.op==KW_BASE || v.op==KW_PREFIX)) {
				Value vals[2];
				if (v.op==KW_BASE) vals[0].setError();
				else if (lex()!=LX_PREFIX) throw SY_MISQNM;
				else vals[0]=v;
				if (lex()!=LX_CON || v.type!=VT_STRING) throw SY_MISQUO;
				vals[1]=v;
				if ((stmt=new(ma) Stmt(md,ma,STMT_PREFIX))==NULL) throw RC_NORESOURCES;
				if ((rc=stmt->setValues(vals,2))!=RC_OK) throw rc;
			} else if (lx==LX_IDENT && v.length==sizeof(ISOLATION_S)-1 && cmpncase(v.str,ISOLATION_S,sizeof(ISOLATION_S)-1)) {
				// read level
				if ((stmt=new(ma) Stmt(md,ma,STMT_ISOLATION))==NULL) throw RC_NORESOURCES;
			} else throw SY_SYNTAX;
			break;
		}
		break;
	}
	if (sy!=SY_ALL || rc!=RC_OK) {
		if (stmt!=NULL) stmt->destroy();
		if (sy!=SY_ALL) throw sy; else throw rc;
	}
	return stmt;
}

RC SInCtx::exec(const Value *params,unsigned nParams,char **result,uint64_t *nProcessed,unsigned nProcess,unsigned nSkip)
{
	SOutCtx out(ses); assert(ses!=NULL); unsigned fBrkIns=false; uint64_t cnt=0;
	for (TLx lx=lex();;lx=lex()) {
		if (lx==LX_EOE) break; if (lx!=LX_LPR && lx!=LX_KEYW) throw SY_SYNTAX;
		nextLex=lx; uint64_t c; SynErr sy=SY_ALL; RC rc=RC_OK; Stmt *stmt; ICursor *ir=NULL;
		if ((stmt=parseStmt())!=NULL) {
			if (cnt!=0 && stmt->getOp()<STMT_START_TX) {
				if (!fBrkIns) {fBrkIns=true; if (!out.insert("[",1,0)) throw RC_NORESOURCES;}
				if (!out.append(",\n",2)) throw RC_NORESOURCES;
			}
			if ((rc=stmt->execute(result!=NULL?&ir:NULL,params,nParams,nProcess,nSkip,0/*mode???*/))==RC_OK && ir!=NULL)
				{if ((rc=out.renderJSON((Cursor*)ir,c))==RC_EOF) rc=RC_OK; cnt+=c; ir->destroy();}
			stmt->destroy();
		}
		if (nProcessed!=NULL) *nProcessed=cnt;
		if (sy!=SY_ALL) throw sy; if (rc!=RC_OK) throw rc;
		if (lex()!=LX_SEMI) break;
	}
	if (cnt!=0 && !out.append(fBrkIns?"]\n":"\n",fBrkIns?2:1)) throw RC_NORESOURCES;
	return result==NULL||(*result=(char*)out)!=NULL?RC_OK:RC_NORESOURCES;
}

void SInCtx::parseManage(IMapDir *id,MVStoreCtx& ctx,const StartupParameters *sp)
{
	struct StrCopy {
		char *s; 
		StrCopy() : s(NULL) {} 
		~StrCopy() {::free(s);} 
		operator const char*() const {return s;} 
		void set(const char *str,size_t l) {if ((s=(char*)::malloc(l+1))==NULL) throw RC_NORESOURCES; memcpy(s,str,l); s[l]='\0';}
	};
	for (TLx lx=lex(); lx!=LX_EOE; lx=lex()) {
		byte op=KW_SELECT; RC rc=RC_OK; const char *mapped=NULL,*pwd=NULL,*dir2; StrCopy dirc,pwdc,identc,dir2c;
		if (lx==LX_KEYW) {
			if (v.op==KW_CREATE || v.op==KW_DELETE || v.op==KW_DROP || v.op==KW_MOVE) op=v.op; else throw RC_SYNTAX;
		} else if (lx==LX_IDENT || v.type==VT_STRING) {
			if (v.length==sizeof(CLOSE_S)-1 && cmpncase(v.str,CLOSE_S,sizeof(CLOSE_S)-1)) op=KW_SET;
			else if (v.length!=sizeof(OPEN_S)-1 || !cmpncase(v.str,OPEN_S,sizeof(OPEN_S)-1)) throw SY_SYNTAX;
		} else throw SY_SYNTAX;
		if (lex()!=LX_IDENT || v.type!=VT_STRING || v.length!=sizeof(STORE_S)-1 || !cmpncase(v.str,STORE_S,sizeof(STORE_S)-1)) throw SY_SYNTAX;
		const char *dir=NULL; size_t ldir=0;
		if (op==KW_MOVE) {
			if ((lx=lex())!=LX_KEYW || v.op!=KW_FROM) nextLex=lx;
			else if (lex()!=LX_CON || v.type!=VT_STRING) throw SY_MISCON;
			else {dir=v.str; ldir=v.length;}
		} else if ((lx=lex())!=OP_IN) nextLex=lx;
		else if (lex()!=LX_CON || v.type!=VT_STRING) throw SY_MISCON;
		else {dir=v.str; ldir=v.length;}
		if (id!=NULL && (rc=id->map(op==KW_CREATE?IMapDir::SO_CREATE:op==KW_MOVE?IMapDir::SO_MOVE:
			op==KW_DROP||op==KW_DELETE?IMapDir::SO_DELETE:IMapDir::SO_OPEN,dir,ldir,mapped,&pwd))!=RC_OK) throw rc;
		if (mapped==NULL && dir!=NULL && ldir!=0) {dirc.set(dir,ldir); mapped=dirc;}
		switch (op) {
		case KW_CREATE: case KW_SELECT:
			{StoreCreationParameters cparams; StartupParameters params(STARTUP_MODE_SERVER);
			if (sp!=NULL) params=*sp; params.directory=mapped; if (pwd!=NULL) cparams.password=params.password=pwd;
			if ((lx=lex())!=LX_IDENT || v.type!=VT_STRING || v.length!=sizeof(OPTIONS_S)-1 || !cmpncase(v.str,OPTIONS_S,sizeof(OPTIONS_S)-1)) nextLex=lx;
			else {
				if (lex()!=LX_LPR) throw SY_MISLPR;
				do {
					if (lex()!=LX_IDENT || v.type!=VT_STRING) throw SY_SYNTAX;		// SY_MISPNM	- parameter name
					Value vv=v; if (lex()!=OP_EQ) throw SY_MISEQ; if (lex()!=LX_CON) throw SY_MISCON;
					if (vv.length==sizeof("NBUFFERS")-1 && cmpncase(vv.str,"NBUFFERS",vv.length)) {
						if (v.type==VT_INT || v.type==VT_UINT) params.nBuffers=v.ui>=20?v.ui:20; else throw SY_MISNUM;
					} else if (vv.length==sizeof("LOGBUFSIZE")-1 && cmpncase(vv.str,"LOGBUFSIZE",vv.length)) {
						if (v.type==VT_INT || v.type==VT_UINT) params.logBufSize=v.ui; else throw SY_MISNUM;
					} else if (vv.length==sizeof("MAXFILES")-1 && cmpncase(vv.str,"MAXFILES",vv.length)) {
						if (v.type==VT_INT || v.type==VT_UINT) params.maxFiles=v.ui>=20?v.ui:20; else throw SY_MISNUM;
					} else if (vv.length==sizeof("SHUTDOWNASYNCTIMEOUT")-1 && cmpncase(vv.str,"SHUTDOWNASYNCTIMEOUT",vv.length)) {
						if (v.type==VT_INT || v.type==VT_UINT) params.shutdownAsyncTimeout=v.ui; else throw SY_MISNUM;
					} else if (pwd==NULL && vv.length==sizeof("PASSWORD")-1 && cmpncase(vv.str,"PASSWORD",vv.length)) {
						if (v.type!=VT_STRING) throw SY_MISQUO;
						pwdc.set(v.str,v.length); params.password=cparams.password=pwdc;
					} else if (vv.length==sizeof("LOGDIRECTORY")-1 && cmpncase(vv.str,"LOGDIRECTORY",vv.length)) {
						if (v.type!=VT_STRING) throw SY_MISQUO; //???
						if (id!=NULL && (rc=id->map(IMapDir::SO_LOG,v.str,v.length,params.logDirectory,NULL))!=RC_OK) throw rc;
						if (params.logDirectory==NULL) {dir2c.set(v.str,v.length); params.logDirectory=dir2c;}
					} else if (op!=KW_CREATE) throw SY_SYNTAX;
					else if (vv.length==sizeof("PAGESIZE")-1 && cmpncase(vv.str,"PAGESIZE",vv.length)) {
						if (v.type==VT_INT || v.type==VT_UINT) cparams.pageSize=v.ui; else throw SY_MISNUM;
					} else if (vv.length==sizeof("FILEEXTENTSIZE")-1 && cmpncase(vv.str,"FILEEXTENTSIZE",vv.length)) {
						if (v.type==VT_INT || v.type==VT_UINT) cparams.fileExtentSize=v.ui; else throw SY_MISNUM;
					} else if (vv.length==sizeof("OWNER")-1 && cmpncase(vv.str,"OWNER",vv.length)) {
						if (v.type!=VT_STRING) throw SY_MISQUO;	//???
						identc.set(v.str,v.length); cparams.identity=identc;
					} else if (vv.length==sizeof("STOREID")-1 && cmpncase(vv.str,"STOREID",vv.length)) {
						if (v.type==VT_INT || v.type==VT_UINT) cparams.storeId=(uint16_t)v.ui; else throw SY_MISNUM;
					} else if (vv.length==sizeof("ENCRYPTED")-1 && cmpncase(vv.str,"ENCRYPTED",vv.length)) {
						if (v.type==VT_BOOL) cparams.fEncrypted=v.b; else throw SY_MISLGC;
					} else if (vv.length==sizeof("MAXSIZE")-1 && cmpncase(vv.str,"MAXSIZE",vv.length)) {
						if (v.type==VT_INT || v.type==VT_UINT) cparams.maxSize=v.ui;
						else if (v.type==VT_INT64 || v.type==VT_UINT64) cparams.maxSize=v.ui64;
						else throw SY_MISNUM;
					} else if (vv.length==sizeof("LOGSEGSIZE")-1 && cmpncase(vv.str,"LOGSEGSIZE",vv.length)) {
						if (v.type==VT_INT || v.type==VT_UINT) cparams.logSegSize=v.ui; else throw SY_MISNUM;
					} else if (vv.length==sizeof("PCTFREE")-1 && cmpncase(vv.str,"PCTFREE",vv.length)) {
						if (v.type==VT_FLOAT) cparams.pctFree=v.f; else if (v.type==VT_DOUBLE) cparams.pctFree=(float)v.d; else throw SY_SYNTAX;
					} else throw SY_SYNTAX;
				} while ((lx=lex())==LX_COMMA);
				if (lx!=LX_RPR) throw SY_MISRPR;
			}
			rc=op==KW_CREATE?createStore(cparams,params,ctx):openStore(params,ctx);}
			break;
		case KW_SET:
			// check opened -> close
			ctx=NULL;
			break;
		case KW_MOVE:
			if (lex()!=LX_IDENT || v.type!=VT_STRING || v.length!=sizeof(TO_S)-1 || !cmpncase(v.str,TO_S,v.length)) throw SY_SYNTAX;
			if (lex()!=LX_CON || v.type!=VT_STRING) throw SY_MISQUO; dir2=NULL;	//???
			if (id!=NULL && (rc=id->map(IMapDir::SO_CREATE,v.str,v.length,dir2,NULL))!=RC_OK) throw rc;
			if (dir2==NULL) {dir2c.set(v.str,v.length); dir2=dir2c;}
			rc=FileMgr::moveStore(mapped,dir2);
			break;
		case KW_DROP: case KW_DELETE:
			// check opened -> close
			ctx=NULL; rc=FileMgr::deleteStore(mapped);
			break;
		}
		if (rc!=RC_OK) throw rc;
		if ((lx=lex())!=LX_SEMI && lx!=LX_EOE) throw RC_SYNTAX;
	}
}

//---------------------------------------------------------------------------------------------------

IStmt *SessionX::createStmt(STMT_OP st_op,unsigned mode)
{
	try {
		assert(ses==Session::getSession()); if (ses->getStore()->inShutdown()) return NULL;
		return new(ses) Stmt(ulong(ses->itf)|mode,ses,st_op);
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::createStmt()\n");}
	return NULL;
}

IStmt *SessionX::createStmt(const char *qs,const URIID *ids,unsigned nids,CompilationError *ce) 
{
	try {
		assert(ses==Session::getSession());
		if (ce!=NULL) {memset(ce,0,sizeof(CompilationError)); ce->msg="";}
		if (qs==NULL || ses->getStore()->inShutdown()) return NULL;
		SInCtx in(ses,qs,strlen(qs),ids,nids,(ses->itf&ITF_SPARQL)!=0?SQ_SPARQL:SQ_SQL);
		try {Stmt *st=in.parseStmt(); in.checkEnd(true); return st;}
		catch (SynErr sy) {in.getErrorInfo(RC_SYNTAX,sy,ce);}
		catch (RC rc) {in.getErrorInfo(rc,SY_ALL,ce);}
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::createStmt(const char *,...)\n");}
	return NULL;
}

IExpr *SessionX::createExpr(const char *s,const URIID *ids,unsigned nids,CompilationError *ce)
{
	try {
		assert(ses==Session::getSession()); 
		if (ce!=NULL) {memset(ce,0,sizeof(CompilationError)); ce->msg="";}
		if (s==NULL||ses->getStore()->inShutdown()) return NULL;
		SInCtx in(ses,s,strlen(s),ids,nids); Expr *pe=NULL;
		try {ExprTree *et=in.parse(false); in.checkEnd(); Expr::compile(et,pe,ses,false); et->destroy(); return pe;}
		catch (SynErr sy) {in.getErrorInfo(RC_SYNTAX,sy,ce);}
		catch (RC rc) {in.getErrorInfo(rc,SY_ALL,ce);}
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::toExpr()\n");}
	return NULL;
}

IExprTree *SessionX::createExprTree(const char *s,const URIID *ids,unsigned nids,CompilationError *ce)
{
	try {
		assert(ses==Session::getSession()); 
		if (ce!=NULL) {memset(ce,0,sizeof(CompilationError)); ce->msg="";}
		if (s==NULL||ses->getStore()->inShutdown()) return NULL;
		SInCtx in(ses,s,strlen(s),ids,nids);
		try {ExprTree *et=in.parse(true); in.checkEnd(); return et;} 
		catch (SynErr sy) {in.getErrorInfo(RC_SYNTAX,sy,ce);}
		catch (RC rc) {in.getErrorInfo(rc,SY_ALL,ce);}
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::toExprTree()\n");}
	return NULL;
}

RC SessionX::parseValue(const char *s,size_t l,Value& res,CompilationError *ce)
{
	try {
		assert(ses==Session::getSession()); 
		if (ce!=NULL) {memset(ce,0,sizeof(CompilationError)); ce->msg="";}
		if (s==NULL) return RC_INVPARAM; if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		SInCtx in(ses,s,l,NULL,0);
		try {in.parse(res,NULL,0,PRS_COPYV); in.checkEnd();}
		catch (SynErr sy) {in.getErrorInfo(RC_SYNTAX,sy,ce); return RC_SYNTAX;}
		catch (RC rc) {in.getErrorInfo(rc,SY_ALL,ce); return rc;}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::parseValue()\n"); return RC_INTERNAL;}
}

RC SessionX::parseValues(const char *s,size_t l,Value *&res,unsigned& nValues,CompilationError *ce,char delimiter)
{
	try {
		assert(ses==Session::getSession()); res=NULL; nValues=0;
		if (ce!=NULL) {memset(ce,0,sizeof(CompilationError)); ce->msg="";}
		if (s==NULL) return RC_INVPARAM; if (ses->getStore()->inShutdown()) return RC_SHUTDOWN;
		SInCtx in(ses,s,l,NULL,0); Value *pv=NULL; unsigned nvals=0,xvals=0; RC rc=RC_OK;
		try {
			do {
				if (nvals>=xvals && (pv=(Value*)ses->realloc(pv,(xvals+=xvals==0?8:xvals/2)*sizeof(Value)))==NULL)
					{nvals=0; throw RC_NORESOURCES;}
				in.parse(pv[nvals],NULL,0,PRS_COPYV); nvals++;
			} while (in.checkDelimiter(delimiter));
			in.checkEnd(); if ((nValues=nvals)!=0) res=pv; return RC_OK;
		} catch (SynErr sy) {in.getErrorInfo(RC_SYNTAX,sy,ce); rc=RC_SYNTAX;}
		catch (RC rc2) {in.getErrorInfo(rc,SY_ALL,ce); rc=rc2;}
		freeV(pv,nvals,ses); return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::parseValues()\n"); return RC_INTERNAL;}
}

RC SessionX::getTypeName(ValueType type,char buf[],size_t lbuf)
{
	try {
		assert(ses==Session::getSession()); 
		if (type>=VT_ALL || buf==NULL || lbuf==0) return RC_INVPARAM;
		strncpy(buf,typeName[type].s,lbuf-1)[lbuf-1]=0; return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::getTypeName()\n"); return RC_INTERNAL;}
}

char *Stmt::toString(unsigned mode,const QName *qNames,unsigned nQNames) const
{
	try {
		Session *ses=Session::getSession();
		if (ses==NULL||ses->getStore()->inShutdown()) return NULL;
		SOutCtx out(ses,mode|(ses->getItf()&ITF_SPARQL),qNames,nQNames);
		return render(out)==RC_OK?out.renderAll():(char*)0;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in Stmt::toString()\n");}
	return NULL;
}

char *Expr::toString(unsigned mode,const QName *qNames,unsigned nQNames) const
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return NULL;
		if (ses==NULL||ses->getStore()->inShutdown()) return NULL;
		SOutCtx out(ses,mode,qNames,nQNames); return render(0,out)==RC_OK?out.renderAll():(char*)0;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IExpr::toString()\n");}
	return NULL;
}

char *ExprTree::toString(unsigned mode,const QName *qNames,unsigned nQNames) const
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return NULL;
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return NULL;
		SOutCtx out(ses,mode,qNames,nQNames); return render(0,out)==RC_OK?out.renderAll():(char*)0;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IExprTree::toString()\n");}
	return NULL;
}
