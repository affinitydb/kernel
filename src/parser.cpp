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

Written by Mark Venguerov 2010-2014

**************************************************************************************/

#include "queryprc.h"
#include "stmt.h"
#include "expr.h"
#include "parser.h"
#include "maps.h"
#include "classifier.h"
#include "startup.h"
#include "fio.h"
#include "regexp.h"
#include "named.h"

using namespace AfyKernel;

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
#define	OFF_S				"OFF"
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
#define	UNIQUE_S			"UNIQUE"
#define	CASE_S				"CASE"
#define WHEN_S				"WHEN"
#define	THEN_S				"THEN"
#define	ELSE_S				"ELSE"
#define	END_S				"END"

#define	INSERT_S			"INSERT"
#define	UPDATE_S			"UPDATE"
#define	DELETE_S			"DELETE"
#define	CREATE_S			"CREATE"
#define	DROP_S				"DROP"
#define	PURGE_S				"PURGE"
#define	UNDELETE_S			"UNDELETE"
#define	BASE_S				"BASE"
#define PREFIX_S			"PREFIX"
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
#define	INLINE_S			"INLINE"
#define	TIMER_S				"TIMER"
#define	SELF_S				"SELF"
#define	RAW_S				"RAW"
#define	RULE_S				"RULE"
#define	LISTENER_S			"LISTENER"
#define	LOADER_S			"LOADER"
#define	IDEMPOTENT_S		"IDEMPOTENT"
#define	EVENT_S				"EVENT"
#define HANDLER_S			"HANDLER"
#define FSM_S				"FSM"
#define	CTX_S				"CTX"
#define	OLD_S				"OLD"
#define	AUTO_S				"AUTO"
#define	OPTIONAL_S			"OPTIONAL"
#define	PACKAGE_S			"PACKAGE"
#define	ACTION_S			"ACTION"
#define	CONDITION_S			"CONDITION"
#define	ENUMERATION_S		"ENUMERATION"
#define	CLASS_S				"CLASS"
#define	OPEN_S				"OPEN"
#define	STORE_S				"STORE"
#define	BEFORE_S			"BEFORE"
#define	AFTER_S				"AFTER"
#define	OPTIONS_S			"OPTIONS"
#define	CLOSE_S				"CLOSE"
#define	TO_S				"TO"
#define	TRACE_S				"TRACE"
#define MEMORY_S			"MEMORY"
#define	INSTANCE_S			"INSTANCE"
#define	OF_S				"OF"
#define	TOPIC_S				"TOPIC"
#define	INSERTED_S			"INSERTED"
#define	UPDATED_S			"UPDATED"
#define	DELETED_S			"DELETED"
#define	EXPIRES_S			"EXPIRES"
 					
#define	URIID_PREFIX		"."
#define	IDENTITY_PREFIX		"!"
#define	PID_PREFIX			"@"
#define	PARAM_PREFIX		":"
#define	PATH_DELIM			"."
#define	QNAME_DELIM			":"

const OpDscr SInCtx::opDscr[] = 
{
	{0,		LX_EOE,		0,NULL,				NULL,			{0,	  4},	{0,	0}	},		 //LX_BOE
	{0,		LX_ERR,		0,NULL,				NULL,			{0,	  0},	{0,	0}	},		 //LX_EOE
	{0,		LX_ERR,		0,NULL,				NULL,			{0,	  0},	{0,	0}	},		 //LX_IDENT
	{0,		LX_ERR,		0,NULL,				NULL,			{0,	  0},	{0,	0}	},		 //LX_CON
	{0,		LX_ERR,		0,NULL,				NULL,			{0,	  0},	{0,	0}	},		 //LX_KEYW
	{_U,	LX_RPR,		S_L("("),			NULL,			{40,  2},	{1,	1}	},		 //LX_LPR
	{_C,	LX_ERR,		S_L(")"),			NULL,			{2,	  0},	{0,	0}	},		 //LX_RPR
	{0,		LX_RPR,		S_L(","),			NULL,			{3,	  2},	{0,	0}	},		 //LX_COMMA
	{0,		LX_ERR,		S_L("+"),			Expr::calc,		{12, 12},	{2,	2}	},		 //OP_PLUS
	{0,		LX_ERR,		S_L("-"),			Expr::calc,		{12, 12},	{2,	2}	},		 //OP_MINUS
	{0,		LX_ERR,		S_L("*"),			Expr::calc,		{15, 15},	{2,	2}	},		 //OP_MUL
	{0,		LX_ERR,		S_L("/"),			Expr::calc,		{15, 15},	{2,	2}	},		 //OP_DIV
	{0,		LX_ERR,		S_L("%"),			Expr::calc,		{15, 15},	{2,	2}	},		 //OP_MOD
	{_U,	LX_ERR,		S_L("-"),			Expr::calc,		{17, 16},	{1,	1}	},		 //OP_NEG
	{_U,	LX_ERR,		S_L("~"),			Expr::calc,		{17, 16},	{1,	1}	},		 //OP_NOT
	{0,		LX_ERR,		S_L("&"),			Expr::calc,		{10, 10},	{2,	2}	},		 //OP_AND
	{0,		LX_ERR,		S_L("|"),			Expr::calc,		{10, 10},	{2,	2}	},		 //OP_OR
	{0,		LX_ERR,		S_L("^"),			Expr::calc,		{10, 10},	{2,	2}	},		 //OP_XOR
	{0,		LX_ERR,		S_L("<<"),			Expr::calc,		{10, 10},	{2,	2}	},		 //OP_LSHIFT
	{0,		LX_ERR,		S_L(">>"),			Expr::calc,		{10, 10},	{2,	2}	},		 //OP_RSHIFT
	{_F,	LX_RPR,		S_L("SETBIT"),		Expr::calc,		{40,  2},	{2,	2}	},		 //OP_SETBIT
	{_F,	LX_RPR,		S_L("RESETBIT"),	Expr::calc,		{40,  2},	{2,	2}	},		 //OP_RESETBIT
	{_F|_A,	LX_RPR,		S_L(MIN_S),			Expr::calcAgg,	{40,  2},	{1,255}	},		 //OP_MIN
	{_F|_A,	LX_RPR,		S_L(MAX_S),			Expr::calcAgg,	{40,  2},	{1,255}	},		 //OP_MAX
	{_F|_A,	LX_RPR,		S_L("ARGMIN"),		Expr::calcAgg,	{40,  2},	{1,255}	},		 //OP_ARGMIN
	{_F|_A,	LX_RPR,		S_L("ARGMAX"),		Expr::calcAgg,	{40,  2},	{1,255}	},		 //OP_ARGMAX
	{_F,	LX_RPR,		S_L("ABS"),			Expr::calc,		{40,  2},	{1, 1}	},		 //OP_ABS
	{_F,	LX_RPR,		S_L("LN"),			Expr::calc,		{40,  2},	{1, 1}	},		 //OP_LN
	{_F,	LX_RPR,		S_L("EXP"),			Expr::calc,		{40,  2},	{1, 1}	},		 //OP_EXP
	{_F,	LX_RPR,		S_L("POWER"),		Expr::calc,		{40,  2},	{2, 2}	},		 //OP_POW
	{_F,	LX_RPR,		S_L("SQRT"),		Expr::calc,		{40,  2},	{1, 1}	},		 //OP_SQRT
	{_F,	LX_RPR,		S_L("SIN"),			Expr::calc,		{40,  2},	{1, 1}	},		 //OP_SIN
	{_F,	LX_RPR,		S_L("COS"),			Expr::calc,		{40,  2},	{1, 1}	},		 //OP_COS
	{_F,	LX_RPR,		S_L("TAN"),			Expr::calc,		{40,  2},	{1, 1}	},		 //OP_TAN
	{_F,	LX_RPR,		S_L("ASIN"),		Expr::calc,		{40,  2},	{1, 1}	},		 //OP_ASIN
	{_F,	LX_RPR,		S_L("ACOS"),		Expr::calc,		{40,  2},	{1, 1}	},		 //OP_ACOS
	{_F,	LX_RPR,		S_L("ATAN"),		Expr::calc,		{40,  2},	{1, 2}	},		 //OP_ATAN
	{_F,	LX_RPR,		S_L("NORM"),		Expr::laNorm,	{40,  2},	{1, 2}	},		 //OP_NORM
	{_F,	LX_RPR,		S_L("TRACE"),		Expr::laTrace,	{40,  2},	{1, 1}	},		 //OP_TRACE
	{_F,	LX_RPR,		S_L("INV"),			Expr::laInv,	{40,  2},	{1, 1}	},		 //OP_INV
	{_F,	LX_RPR,		S_L("DET"),			Expr::laDet,	{40,  2},	{1, 1}	},		 //OP_DET
	{_F,	LX_RPR,		S_L("RANK"),		Expr::laRank,	{40,  2},	{1, 1}	},		 //OP_RANK
	{_F,	LX_RPR,		S_L("TRANSPOSE"),	Expr::laTrans,	{40,  2},	{1, 1}	},		 //OP_TRANSPOSE
	{_F,	LX_RPR,		S_L("FLOOR"),		Expr::calc,		{40,  2},	{1, 1}	},		 //OP_FLOOR
	{_F,	LX_RPR,		S_L("CEIL"),		Expr::calc,		{40,  2},	{1, 1}	},		 //OP_CEIL
	{_F,	LX_RPR,		S_L("CONCAT"),		Expr::calc,		{40,  2},	{1,255}	},		 //OP_CONCAT
	{_F,	LX_RPR,		S_L(LOWER_S),		Expr::calc,		{40,  2},	{1,	1}	},		 //OP_LOWER
	{_F,	LX_RPR,		S_L(UPPER_S),		Expr::calc,		{40,  2},	{1,	1}	},		 //OP_UPPER
	{_F,	LX_RPR,		S_L("TONUM"),		Expr::calc,		{40,  2},	{1,	1}	},		 //OP_TONUM
	{_F,	LX_RPR,		S_L("TOINUM"),		Expr::calc,		{40,  2},	{1,	1}	},		 //OP_TOINUM
	{_F|_I,	LX_RPR,		S_L("CAST"),		Expr::calc,		{40,  2},	{2,	2}	},		 //OP_CAST
	{_U,	LX_RBR,		S_L("@["),			Expr::calc,		{40,  2},	{2,	2}	},		 //OP_RANGE
	{_U,	LX_RCBR,	S_L("{"),			Expr::calc,		{40,  2},	{1,255}	},		 //OP_COLLECTION
	{_U,	LX_RCBR,	S_L("{"),			Expr::calc,		{40,  2},	{1,255}	},		 //OP_STRUCT
	{_U,	LX_RCBR,	S_L("@{"),			Expr::calc,		{40,  2},	{1,255}	},		 //OP_PIN
	{_U,	LX_RBR,		S_L("["),			Expr::calc,		{40,  2},	{1,255}	},		 //OP_ARRAY
	{0,		LX_ERR,		S_L("["),			Expr::calc,		{32, 32},	{2,	3}	},		 //OP_ELEMENT
	{_F|_A,	LX_RPR,		S_L(COUNT_S),		Expr::calcAgg,	{40,  2},	{1,	2}	},		 //OP_COUNT
	{_F,	LX_RPR,		S_L("LENGTH"),		Expr::calc,		{40,  2},	{1,	1}	},		 //OP_LENGTH
	{_F,	LX_RPR,		S_L("POSITION"),	Expr::calc,		{40,  2},	{2,	2}	},		 //OP_POSITION
	{_F,	LX_RPR,		S_L("SUBSTR"),		Expr::calc,		{40,  2},	{2,	3}	},		 //OP_SUBSTR
	{_F,	LX_RPR,		S_L("REPLACE"),		Expr::calc,		{40,  2},	{3,	3}	},		 //OP_REPLACE
	{_F,	LX_RPR,		S_L("PAD"),			Expr::calc,		{40,  2},	{2,	3}	},		 //OP_PAD
	{_F|_I,	LX_RPR,		S_L("TRIM"),		Expr::calc,		{40,  2},	{3,	3}	},		 //OP_TRIM
	{_F|_A,	LX_RPR,		S_L("SUM"),			Expr::calcAgg,	{40,  2},	{1,255}	},		 //OP_SUM
	{_F|_A,	LX_RPR,		S_L("AVG"),			Expr::calcAgg,	{40,  2},	{1,255}	},		 //OP_AVG
	{_F|_A,	LX_RPR,		S_L("VAR_POP"),		Expr::calcAgg,	{40,  2},	{1,255}	},		 //OP_VAR_POP
	{_F|_A,	LX_RPR,		S_L("VAR_SAMP"),	Expr::calcAgg,	{40,  2},	{1,255}	},		 //OP_VAR_SAMP
	{_F|_A,	LX_RPR,		S_L("STDDEV_POP"),	Expr::calcAgg,	{40,  2},	{1,255}	},		 //OP_STDDEV_POP
	{_F|_A,	LX_RPR,		S_L("STDDEV_SAMP"),	Expr::calcAgg,	{40,  2},	{1,255}	},		 //OP_STDDEV_SAMP
	{_F|_A,	LX_RPR,		S_L("HISTOGRAM"),	Expr::calcAgg,	{40,  2},	{1,255}	},		 //OP_HISTOGRAM
	{_F,	LX_RPR,		S_L("COALESCE"),	Expr::calc,		{40,  2},	{2,255}	},		 //OP_COALESCE
	{_F,	LX_RPR,		S_L("MEMBERSHIP"),	Expr::calc,		{40,  2},	{1, 1}	},		 //OP_MEMBERSHIP
	{0,		LX_ERR,		S_L("."),			Expr::calc,		{32, 32},	{2,	4}	},		 //OP_PATH
	{0,		LX_ERR,		S_L("="),			Expr::calc,		{8,   8},	{2,	2}	},		 //OP_EQ
	{0,		LX_ERR,		S_L("<>"),			Expr::calc,		{8,   8},	{2,	2}	},		 //OP_NE
	{0,		LX_ERR,		S_L("<"),			Expr::calc,		{9,   9},	{2,	2}	},		 //OP_LT
	{0,		LX_ERR,		S_L("<="),			Expr::calc,		{9,   9},	{2,	2}	},		 //OP_LE
	{0,		LX_ERR,		S_L(">"),			Expr::calc,		{9,   9},	{2,	2}	},		 //OP_GT
	{0,		LX_ERR,		S_L(">="),			Expr::calc,		{9,   9},	{2,	2}	},		 //OP_GE
	{0,		LX_ERR,		S_L("IN"),			Expr::calc,		{9,   9},	{2,	2}	},		 //OP_IN
	{_F,	LX_RPR,		S_L("BEGINS"),		Expr::calc,		{40,  2},	{2,	2}	},		 //OP_BEGINS
	{_F,	LX_RPR,		S_L("CONTAINS"),	Expr::calc,		{40,  2},	{2,	2}	},		 //OP_CONTAINS
	{_F,	LX_RPR,		S_L("ENDS"),		Expr::calc,		{40,  2},	{2,	2}	},		 //OP_ENDS
	{_F,	LX_RPR,		S_L("TESTBIT"),		Expr::calc,		{40,  2},	{2,	2}	},		 //OP_TESTBIT
	{0,		LX_ERR,		S_L("SIMILAR"),		Expr::calc,		{9,   9},	{2,	2}	},		 //OP_SIMILAR
	{_F,	LX_RPR,		S_L("EXISTS"),		Expr::calc,		{40,  2},	{1,	1}	},		 //OP_EXISTS
	{_F,	LX_RPR,		S_L("ISLOCAL"),		Expr::calc,		{40,  2},	{1,	1}	},		 //OP_ISLOCAL
	{0,		LX_RPR,		0,NULL,				Expr::calc,		{9,   9},	{2,255}	},		 //OP_IS_A
	{0,		LX_ERR,		S_L(AND_S),			Expr::calc,		{6,   6},	{2,	2}	},		 //OP_LAND
	{0,		LX_ERR,		S_L(OR_S),			Expr::calc,		{5,   5},	{2,	2}	},		 //OP_LOR
	{_U,	LX_ERR,		S_L(NOT_S),			Expr::calc,		{7,	  7},	{1,	1}	},		 //OP_LNOT
	{_F|_I,	LX_RPR,		S_L("EXTRACT"),		Expr::calc,		{40,  2},	{2,	2}	},		 //OP_EXTRACT
	{_F,	LX_RPR,		S_L("BITFIELD"),	Expr::calc,		{40,  2},	{3,	3}	},		 //OP_BITFIELD
	{0,		LX_RPR,		S_L("("),			Expr::calc,		{31,  2},	{1,255}	},		 //OP_CALL
	{0,		LX_ERR,		0,NULL,				Expr::calc,		{0,	  0},	{3,255}	},		 //OP_CASE
	{0,		LX_ERR,		S_L(":"),			Expr::calc,		{20, 20},	{0,	0}	},		 //LX_COLON
	{_U,	LX_RBR,		S_L("["),			NULL,			{36,  2},	{1,	1}	},		 //LX_LBR
	{_C,	LX_ERR,		S_L("]"),			NULL,			{2,	  0},	{0,	0}	},		 //LX_RBR
	{_U,	LX_RCBR,	S_L("{"),			NULL,			{36,  2},	{1,	1}	},		 //LX_LCBR
	{_C,	LX_ERR,		S_L("}"),			NULL,			{2,	  0},	{0,	0}	},		 //LX_RCBR
	{0,		LX_ERR,		S_L("."),			NULL,			{18, 18},	{0,	0}	},		 //LX_PERIOD
	{0,		LX_ERR,		S_L("?"),			NULL,			{20, 20},	{0,	0}	},		 //LX_QUEST
	{0,		LX_ERR,		S_L("!"),			NULL,			{20, 20},	{0,	0}	},		 //LX_EXCL
	{_F,	LX_RPR,		S_L("$"),			NULL,			{40,  2},	{1,	1}	},		 //LX_EXPR
	{0,		LX_RCBR,	S_L("${"),			NULL,			{0,   0},	{1,	1}	},		 //LX_STMT
	{0,		LX_ERR,		S_L(">>>"),			NULL,			{10, 10},	{2,	2}	},		 //LX_URSHIFT
	{0,		LX_ERR,		0,NULL,				NULL,			{0,	  0},	{0,	0}	},		 //LX_PREFIX
	{0,		LX_ERR,		S_L(IS_S),			NULL,			{9,   9},	{2,	2}	},		 //LX_IS
	{0,		LX_ERR,		S_L(BETWEEN_S),		NULL,			{9,   9},	{2,	2}	},		 //LX_BETWEEN
	{0,		LX_ERR,		0,NULL,				NULL,			{9,   9},	{2,	2}	},		 //LX_BAND
	{0,		LX_ERR,		0,NULL,				NULL,			{0,	  0},	{0,	0}	},		 //LX_HASH
	{0,		LX_ERR,		0,NULL,				NULL,			{0,	  0},	{0,	0}	},		 //LX_SEMI
	{0,		LX_ERR,		S_L("||"),			NULL,			{11, 11},	{2,	2}	},		 //LX_CONCAT
	{0,		LX_RBR,		S_L("["),			NULL,			{36,  2},	{2,	2}	},		 //LX_FILTER
	{0,		LX_RCBR,	S_L("{"),			NULL,			{36,  2},	{2,	3}	},		 //LX_REPEAT
	{0,		LX_ERR,		0,NULL,				NULL,			{33, 32},	{2,	2}	},		 //LX_PATHQ
	{0,		LX_ERR,		0,NULL,				NULL,			{0,	  0},	{0,	0}	},		 //LX_SELF
	{0,		LX_ERR,		0,NULL,				NULL,			{40,  3},	{2,	2}	},		 //LX_SPROP
	{0,		LX_ERR,		0,NULL,				NULL,			{0,	  0},	{0,	0}	},		 //LX_TPID
	{0,		LX_ERR,		0,NULL,				NULL,			{0,	  0},	{0,	0}	},		 //LX_RXREF
	{_U,	LX_ERR,		S_L("&"),			NULL,			{30, 29},	{1,	1}	},		 //LX_REF
	{0,		LX_ERR,		S_L("->"),			NULL,			{3,	  2},	{0,	0}	},		 //LX_ARROW
};

const static Len_Str typeName[VT_ALL] = 
{
	{S_L("")}, {S_L("INT")}, {S_L("UINT")}, {S_L("INT64")}, {S_L("UINT64")}, {S_L("FLOAT")}, {S_L("DOUBLE")},
	{S_L("BOOL")}, {S_L("DATETIME")}, {S_L("INTERVAL")}, {S_L("URIID")}, {S_L("IDENTITY")}, {S_L("ENUM")}, {S_L("STRING")}, {S_L("BSTR")},
	{S_L("REF")}, {S_L("REFID")}, {S_L("REFVAL")}, {S_L("REFIDVAL")}, {S_L("REFELT")}, {S_L("REFIDELT")},
	{S_L("EXPR")}, {S_L("STMT")}, {S_L("COLLECTION")}, {S_L("STRUCT")}, {S_L("MAP")}, {S_L("ARRAY")},
	{S_L("RANGE")}, {S_L("STREAM")}, {S_L("CURRENT")}, {S_L("VARREF")}, {S_L("EXPRTREE")}
};

const static Len_Str extractWhat[] = 
{
	{S_L("FRACTIONAL")}, {S_L("SECOND")}, {S_L("MINUTE")}, {S_L("HOUR")}, {S_L("DAY")}, {S_L("WDAY")}, {S_L("MONTH")}, {S_L("YEAR")}, {S_L("IDENTITY")}, {S_L("STOREID")},
};

const static Len_Str stmtKWs[STMT_OP_ALL] =
{
	{S_L("")},{S_L(INSERT_S)},{S_L(UPDATE_S)},{S_L(DELETE_S)},{S_L(UNDELETE_S)},{S_L(START_S)},{S_L(COMMIT_S)},{S_L(ROLLBACK_S)}
};

const static Len_Str opKWs[OP_FIRST_EXPR] =
{
	{S_L(" "SET_S" ")}, {S_L(" "ADD_S" ")}, {S_L(" "ADD_S" ")}, {S_L(" "MOVE_S" ")}, {S_L(" "MOVE_S" ")}, {S_L(" "DELETE_S" ")}, {S_L(" "EDIT_S" ")}, {S_L(" "RENAME_S" ")}
};

const static Len_Str atNames[] =
{
	{S_L(SELF_S)}, {S_L(CTX_S)}, {S_L(OLD_S)}, {S_L(EVENT_S)}, {S_L(AUTO_S)}
};

const static struct
{
	Len_Str		ls;
	uint32_t	flg;
} PINoptions[] =
{
	{{S_L("TRANSIENT")},		PIN_TRANSIENT},
	{{S_L("IMMUTABLE")},		PIN_IMMUTABLE},
	{{S_L("PERSISTENT")},		PIN_PERSISTENT},
	{{S_L("NO_REPLICATION")},	PIN_NO_REPLICATION},
	{{S_L("NOTIFY")},			PIN_NOTIFY},
	{{S_L("HIDDEN")},			PIN_HIDDEN},
};

const static struct
{
	Len_Str		ls;
	uint32_t	flg;
} traceOptions[] =
{
	{{S_L("QUERIES")},			TRACE_SESSION_QUERIES},
	{{S_L("ACTIONS")},			TRACE_ACTIONS},
	{{S_L("TIMERS")},			TRACE_TIMERS},
	{{S_L("COMMUNICATIONS")},	TRACE_COMMS},
	{{S_L("FSMS")},				TRACE_FSMS},
};

const static struct
{
	Len_Str		ls;
	uint32_t	flg;
	URIID		prop;
} metaProps[] =
{
	{{S_L("SEEK_CUR")},			META_PROP_SEEK_CUR,			PROP_SPEC_POSITION},
	{{S_L("OPTIONAL")},			META_PROP_OPTIONAL,			PROP_SPEC_LOAD},
	{{S_L("ENTER")},			META_PROP_ENTER,			PROP_SPEC_ACTION},
	{{S_L("READ_PERM")},		META_PROP_READ,				PROP_SPEC_ADDRESS},
	{{S_L("COMPATIBLE")},		META_PROP_READ,				PROP_SPEC_ADDRESS},
	{{S_L("SEEK_END")},			META_PROP_SEEK_END,			PROP_SPEC_POSITION},
	{{S_L("UPDATE")},			META_PROP_UPDATE,			PROP_SPEC_ACTION},
	{{S_L("WRITE_PERM")},		META_PROP_WRITE,			PROP_SPEC_ANY},
	{{S_L("LEAVE")},			META_PROP_LEAVE,			PROP_SPEC_ACTION},
	{{S_L("CREATE_PERM")},		META_PROP_CREATE,			PROP_SPEC_ADDRESS},
	{{S_L("INDEXED")},			META_PROP_INDEXED,			PROP_SPEC_PREDICATE},
	{{S_L("SYNC")},				META_PROP_SYNC,				PROP_SPEC_ANY},
	{{S_L("KEEPALIVE")},		META_PROP_KEEPALIVE,		PROP_SPEC_ADDRESS},
	{{S_L("ASYNC")},			META_PROP_ASYNC,			PROP_SPEC_ANY},
	{{S_L("INMEM")},			META_PROP_INMEM,			PROP_SPEC_ANY},
	{{S_L("ALT")},				META_PROP_ALT,				PROP_SPEC_SERVICE},
	{{S_L("FT_INDEX")},			META_PROP_FTINDEX,			PROP_SPEC_ANY},
	{{S_L("SEPARATE_STORAGE")},	META_PROP_SSTORAGE,			PROP_SPEC_ANY},
};

enum CRT_STMT
{
	CRT_CLASS, CRT_EVENT, CRT_LISTENER, CRT_ENUM, CRT_TIMER, CRT_LOADER, CRT_PACKAGE, CRT_CONDITION, CRT_ACTION, CRT_HANDLER, CRT_FSM
};

const static struct
{
	Len_Str		ls;
	CRT_STMT	sty;
	uint16_t	meta;
	uint8_t		flags;
} createStmt[] = 
{
	{{S_L(CLASS_S)},			CRT_CLASS,			PMT_CLASS,		META_PROP_INDEXED},
	{{S_L(EVENT_S)},			CRT_EVENT,			PMT_CLASS,		0},
	{{S_L(LISTENER_S)},			CRT_LISTENER,		PMT_LISTENER,	0},
	{{S_L(ENUMERATION_S)},		CRT_ENUM,			PMT_ENUM,		0},
	{{S_L(TIMER_S)},			CRT_TIMER,			PMT_TIMER,		0},
	{{S_L(LOADER_S)},			CRT_LOADER,			PMT_LOADER,		0},
	{{S_L(PACKAGE_S)},			CRT_PACKAGE,		PMT_PACKAGE,	0},
	{{S_L(CONDITION_S)},		CRT_CONDITION,		0,				0},
	{{S_L(ACTION_S)},			CRT_ACTION,			0,				0},
	{{S_L(HANDLER_S)},			CRT_HANDLER,		0,				0},
	{{S_L(FSM_S)},				CRT_FSM,			0,				0},
};


static RC renderElement(ElementID eid,SOutCtx& out,bool fBefore=false)
{
	if (eid!=STORE_COLLECTION_ID && eid!=STORE_VAR_ELEMENT) {
		const char *p; char buf[30]; size_t l;
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
			!out.append(p,l) || !out.append("]",1))  return RC_NOMEM;
	}
	return RC_OK;
}

static RC renderOrder(const OrderSegQ *seg,unsigned nSegs,SOutCtx& out,bool fExprOnly=false)
{
	RC rc;
	for (unsigned i=0; i<nSegs; i++,seg++) {
		if (i!=0 && !out.append(",",1)) return RC_NOMEM;
		if ((seg->flags&ORDER_EXPR)!=0) {
			if ((rc=seg->expr->render(0,out))!=RC_OK) return rc;
		} else {
			if ((seg->flags&ORD_NCASE)!=0 && !out.append(UPPER_S"(",sizeof(UPPER_S))) return RC_NOMEM;
			if ((rc=out.renderName(seg->pid))!=RC_OK) return rc;
			if ((seg->flags&ORD_NCASE)!=0 && !out.append(")",1)) return RC_NOMEM;
		}
		if (!fExprOnly) {
			if ((seg->flags&ORD_DESC)!=0 && !out.append(" "DESC_S,sizeof(DESC_S))) return RC_NOMEM;
			if ((seg->flags&ORD_NULLS_BEFORE)!=0) {
				if (!out.append(" "NULLS_S" "FIRST_S,sizeof(NULLS_S)+sizeof(FIRST_S))) return RC_NOMEM;
			} else if ((seg->flags&ORD_NULLS_AFTER)!=0) {
				if (!out.append(" "NULLS_S" "LAST_S,sizeof(NULLS_S)+sizeof(LAST_S))) return RC_NOMEM;
			}
		}
	}
	return RC_OK;
}

RC SOutCtx::renderProperty(const Value &v,unsigned flags)
{
	if (v.property!=STORE_INVALID_URIID) {
		RC rc; if ((flags&2)!=0 && !append(", ",2)) return RC_NOMEM;
		if ((rc=renderName(v.property))!=RC_OK) return rc;
		if (v.meta!=0) {
			bool fMeta=false;
			for (unsigned k=0; k<sizeof(metaProps)/sizeof(metaProps[0]); k++) 
				if ((v.meta&metaProps[k].flg)!=0 && (metaProps[k].prop==PROP_SPEC_ANY || metaProps[k].prop==v.property)) {
					if (metaProps[k].flg==META_PROP_FTINDEX && v.type!=VT_STRING && v.type!=VT_COLLECTION && v.type!=VT_STRUCT && v.type!=VT_MAP) continue;
					if (metaProps[k].flg==META_PROP_SSTORAGE && v.op==OP_RSHIFT && (flags&8)!=0) continue;
					if (!append(fMeta?",":"(",1)) return RC_NOMEM; fMeta=true;
					if (!append(metaProps[k].ls.s,metaProps[k].ls.l)) return RC_NOMEM;
				}
			if (fMeta && !append(") ",2)) return RC_NOMEM;
		}
		if ((flags&4)!=0 && v.eid!=STORE_COLLECTION_ID && v.op!=OP_RENAME && (rc=renderElement(v.eid,*this,v.op==OP_ADD_BEFORE))!=RC_OK) return rc;
		if ((flags&1)!=0 && (v.property!=PROP_SPEC_SELF || v.type!=VT_VARREF || v.length!=0 || v.refV.flags!=VAR_CTX)) {
			if ((flags&8)!=0 && v.op>=OP_FIRST_EXPR) {
				byte op=v.op==OP_RSHIFT&&(v.meta&META_PROP_UNSIGNED)!=0?LX_URSHIFT:v.op;
				if (!append(SInCtx::opDscr[op].str,SInCtx::opDscr[op].lstr)) return RC_NOMEM;
			}
			if (!append("=",1)) return RC_NOMEM; if ((rc=renderValue(v))!=RC_OK) return rc;
		}
	}
	return RC_OK;
}

const char *SOutCtx::getTypeName(ValueType ty)
{
	return ty<VT_ALL?typeName[ty].s:"???";
}

RC Stmt::render(SOutCtx& out) const
{
	out.flags&=~(SOM_VAR_NAMES|SOM_WHERE|SOM_MATCH|SOM_AND); RC rc=RC_OK;
	if (with.vals!=NULL && with.nValues!=0) {
		if (!out.append(WITH_S " ",sizeof(WITH_S))) return RC_NOMEM;
		for (unsigned i=0; i<with.nValues; i++)
			if ((rc=out.renderProperty(with.vals[i],i!=0?3:1))!=RC_OK) return rc;
		if (!out.append(" ",1)) return RC_NOMEM;
	}
	if (op<STMT_OP_ALL && op!=STMT_QUERY && 
		(!out.append(stmtKWs[op].s,stmtKWs[op].l) || !out.append(" ",1))) return RC_NOMEM;
	unsigned prev=~0u; bool fFrom=top!=NULL,fOpt;
	switch (op) {
	default: return RC_INVPARAM;
	case STMT_QUERY:
		if (top==NULL) return RC_INVPARAM;
		if ((rc=top->render(out))!=RC_OK) return rc;
		if (orderBy!=NULL && nOrderBy>0) {
			if (!out.append(" "ORDER_S" "BY_S" ",sizeof(ORDER_S)+sizeof(BY_S)+1)) return RC_NOMEM;
			rc=renderOrder(orderBy,nOrderBy,out);
		}
		break;
	case STMT_START_TX:
		if (!out.append(TRANSACTION_S" ",sizeof(TRANSACTION_S))) return RC_NOMEM;
		// isolation level
		if ((mode&MODE_READONLY)!=0 && !out.append(READ_S" "ONLY_S,sizeof(READ_S)+sizeof(ONLY_S)-1)) return RC_NOMEM;
		break;
	case STMT_COMMIT:
	case STMT_ROLLBACK:
		if ((mode&MODE_ALL)!=0 && !out.append(ALL_S,sizeof(ALL_S)-1)) return RC_NOMEM;
		break;
	case STMT_INSERT:
		if ((mode&MODE_PART)!=0 && out.append(PART_S" ",sizeof(PART_S))) return RC_NOMEM;
		if (tpid!=STORE_INVALID_PID && (mode&MODE_MANY_PINS)==0) {
			size_t l=sprintf(out.cbuf,PID_PREFIX ":" _LX_FM " ",tpid);
			if (!out.append(out.cbuf,l)) return RC_NOMEM;
		}
		if (into!=NULL && nInto!=0) {
			if (!out.append(INTO_S" ",sizeof(INTO_S))) return RC_NOMEM;
			for (unsigned i=0; i<nInto; i++) {
				const IntoClass &cs=into[i];
				if (i>0 && !out.append(" & ",3)) return RC_NOMEM;
				if ((rc=out.renderName(cs.cid))!=RC_OK) return rc;
				if ((cs.flags&IC_UNIQUE)!=0) {
					if (!out.append(" " UNIQUE_S,sizeof(UNIQUE_S))) return RC_NOMEM;
				} else if ((cs.flags&IC_IDEMPOTENT)!=0) {
					if (!out.append(" " IDEMPOTENT_S,sizeof(IDEMPOTENT_S))) return RC_NOMEM;
				}
			}
		}
		fOpt=false;
		if (pmode!=0) for (unsigned i=0; i<sizeof(PINoptions)/sizeof(PINoptions[0]); i++) if ((pmode&PINoptions[i].flg)!=0) {
			if (fOpt) {if (!out.append(",",1)) return RC_NOMEM;}
			else {fOpt=true; if (!out.append(OPTIONS_S"(",sizeof(OPTIONS_S))) return RC_NOMEM;}
			if (!out.append(PINoptions[i].ls.s,PINoptions[i].ls.l)) return RC_NOMEM;
		}
		if (fOpt && !out.append(") ",2)) return RC_NOMEM;
		if (vals!=NULL && nValues!=0) {
			if ((mode&MODE_MANY_PINS)!=0) {
				if ((mode&MODE_SAME_PROPS)!=0 && nValues>1) {
					if (!out.append("(",1)) return RC_NOMEM;
					for (unsigned i=0; i<pins[0].nValues; i++)
						if ((rc=out.renderProperty(pins[0].vals[i],i!=0?2:0))!=RC_OK) return rc;
					if (!out.append(") " VALUES_S " (",sizeof(VALUES_S)+3)) return RC_NOMEM;
					for (unsigned j=0; j<nValues; j++) {
						if (j!=0 && !out.append("),  (",5)) return RC_NOMEM;
						for (unsigned i=0; i<pins[j].nValues; i++) {
							if (i!=0 && !out.append(", ",2)) return RC_NOMEM;
							if ((rc=out.renderValue(pins[j].vals[i]))!=RC_OK) return rc;
						}
					}
					if (!out.append(")",1)) return RC_NOMEM;
				} else {
					for (unsigned j=0; j<nValues; j++) {
						if (j!=0 && !out.append("}, ",3)) return RC_NOMEM;
						if (pins[j].tpid!=STORE_INVALID_PID) {
							size_t l=sprintf(out.cbuf,PID_PREFIX ":" _LX_FM " ",pins[j].tpid);
							if (!out.append(out.cbuf,l)) return RC_NOMEM;
						}
						if (nValues>1 && !out.append("@{",2)) return RC_NOMEM;
						for (unsigned i=0; i<pins[j].nValues; i++)
							if ((rc=out.renderProperty(pins[j].vals[i],i!=0?3:1))!=RC_OK) return rc;
					}
					if (nValues>1 && !out.append("}",1)) return RC_NOMEM;
				}
			} else {
				for (unsigned i=0; i<nValues; i++) if ((rc=out.renderProperty(vals[i],i!=0?3:1))!=RC_OK) return rc;
			}
		}
		if (rc==RC_OK && top!=NULL) rc=top->render(out);
		break;
	case STMT_UPDATE:
		if ((mode&MODE_RAW)!=0 && !out.append(RAW_S" ",sizeof(RAW_S))) return RC_NOMEM;
		if (nVars==0) {
			if (pmode>=sizeof(atNames)/sizeof(atNames[0])) return RC_CORRUPTED;
			if (!out.append("@",1) || !out.append(atNames[pmode].s,atNames[pmode].l)) return RC_NOMEM;
		} else if (nVars==1 && top->getType()==QRY_SIMPLE) {
			if ((rc=top->render(RP_FROM,out))==RC_OK) fFrom=false; else return rc;
		}
		if (vals!=NULL) for (unsigned i=0; i<nValues; i++) {
			const Value& v=vals[i];
			if (v.property!=STORE_INVALID_URIID && v.op<=OP_CONCAT) {
				unsigned op=v.op>=(uint8_t)OP_FIRST_EXPR?OP_SET:v.op==(uint8_t)OP_ADD_BEFORE?OP_ADD:v.op==(uint8_t)OP_MOVE_BEFORE?OP_MOVE:(ExprOp)v.op;
				if (op==prev) {if (!out.append(", ",2)) return RC_NOMEM;}
				else if (out.append(opKWs[op].s,opKWs[op].l)) prev=op; else return RC_NOMEM;
				if (op==OP_EDIT && isString((ValueType)v.type)) {
					// SUBSTR(prop,....)='str'
				} else if ((rc=out.renderProperty(v,op==OP_SET?13:op==OP_ADD?5:4))!=RC_OK) return rc;
				else switch (op) {
				case OP_SET:
				case OP_ADD:
				case OP_DELETE: break;
				case OP_MOVE: case OP_MOVE_BEFORE:
					if (v.op==OP_MOVE) {
						if (!out.append(" "AFTER_S" ",sizeof(AFTER_S)+1)) return RC_NOMEM;
					} else {
						if (!out.append(" "BEFORE_S" ",sizeof(BEFORE_S)+1)) return RC_NOMEM;
					}
					if ((rc=renderElement(v.ui,out))!=RC_OK) return rc;
					break;
				case OP_RENAME:
					if (!out.append("=",1)) return RC_NOMEM;
					if ((rc=out.renderName(v.ui))!=RC_OK) return rc;
					break;
				case OP_EDIT:
					if (!out.append("{",1)) return RC_NOMEM;
					for (uint64_t m=v.type==VT_UINT?v.bedt.mask:v.bedt64.mask,b=v.type==VT_UINT?v.bedt.bits:v.bedt64.bits,j=0; m!=0; m>>=1,b>>=1,j++) if ((m&1)!=0) {
						if ((b&1)==0 && !out.append(NOT_S " ",sizeof(NOT_S))) return RC_NOMEM;
						char buf[32]; size_t ll=sprintf(buf,_LD_FM,j); if (!out.append(buf,ll)) return RC_NOMEM;
					}
					if (!out.append("}",1)) return RC_NOMEM;
					break;
				}
			}
		}
		if (rc==RC_OK && into!=NULL && nInto!=0) {
			//????
		}
		if (rc!=RC_OK || top==NULL) break;
	case STMT_DELETE: case STMT_UNDELETE:
		if (fFrom) {
			if (top->type!=QRY_SIMPLE || ((SimpleVar*)top)->classes!=NULL || !((SimpleVar*)top)->expr.isEmpty() || ((SimpleVar*)top)->path!=NULL) {
				if (!out.append(" " FROM_S " ",sizeof(FROM_S)+1)) return RC_NOMEM;
				if ((rc=top->render(RP_FROM,out))!=RC_OK) break;
			} else {
				if (((SimpleVar*)top)->condIdx!=NULL) out.flags|=SOM_WHERE;
				if (((SimpleVar*)top)->condFT!=NULL) out.flags|=SOM_MATCH;
			}
		}
		if (top!=NULL) {
			if ((out.flags&SOM_WHERE)!=0 || top->cond!=NULL || top->props!=NULL&&top->nProps!=0) {
				if (!out.append(" " WHERE_S " ",sizeof(WHERE_S)+1)) return RC_NOMEM;
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

	if (!out.append(SELECT_S" ",sizeof(SELECT_S))) return RC_NOMEM;
	if ((qvf&QVF_ALL)!=0) {
		if (!out.append(ALL_S" ",sizeof(ALL_S))) return RC_NOMEM;
	} else if ((qvf&QVF_DISTINCT)!=0) {
		if (!out.append(DISTINCT_S" ",sizeof(DISTINCT_S))) return RC_NOMEM;
	} else if ((qvf&QVF_FIRST)!=0) {
		if (!out.append(FIRST_S" ",sizeof(FIRST_S))) return RC_NOMEM;
	}
	if ((qvf&QVF_RAW)!=0 && !out.append(RAW_S" ",sizeof(RAW_S))) return RC_NOMEM;
	if (stype==SEL_COUNT) {
		if (!out.append(COUNT_S"("STAR_S")",sizeof(COUNT_S)+sizeof(STAR_S))) return RC_NOMEM;
	} else if (outs!=NULL && nOuts!=0) for (unsigned j=0; j<nOuts; j++) {
		const Values& o=outs[j]; rc=RC_OK; const QVar *save=out.cvar; out.cvar=this;
		if (nOuts>1 && !out.append("@{",2)) return RC_NOMEM;
		for (unsigned i=0; i<o.nValues; i++) {
			const Value &v=o.vals[i]; bool fAs=true; const char *p;
			if (i!=0 && !out.append(", ",2)) return RC_NOMEM;
			switch (v.type) {
			case VT_ANY: if (v.op==OP_COUNT) {if (!out.append(STAR_S,sizeof(STAR_S)-1)) rc=RC_NOMEM; break;}
			default: rc=out.renderValue(v); break;
			case VT_EXPR: rc=((Expr*)v.expr)->render(SInCtx::opDscr[LX_COMMA].prty[0],out); break;
			case VT_EXPRTREE: rc=((ExprNode*)v.exprt)->render(SInCtx::opDscr[LX_COMMA].prty[0],out); break;
			case VT_VARREF:
				if (v.refV.type!=VT_ANY && !out.append("CAST(",5)) return RC_NOMEM;
				if ((rc=out.renderValue(v))==RC_OK && v.refV.type!=VT_ANY) {
					if (!out.append(" "AS_S" ",sizeof(AS_S)+1)) return RC_NOMEM;
					if (v.refV.type<VT_ALL) {if (!out.append(typeName[v.refV.type].s,typeName[v.refV.type].l)) return RC_NOMEM;}
					else if ((v.refV.type&0xFF)!=VT_DOUBLE && (v.refV.type&0xFF)!=VT_FLOAT || (p=getUnitName((Units)byte(v.refV.type>>8)))==NULL) return RC_CORRUPTED;
					else if (!out.append(p,strlen(p))) return RC_NOMEM;
					if (!out.append(")",1)) rc=RC_NOMEM;
				}
				fAs=(v.property!=PROP_SPEC_VALUE||o.nValues>1)&&((v.refV.flags&VAR_TYPE_MASK)!=0||v.length==0||v.refV.id!=v.property);
				break;
			}
			if (rc==RC_OK && fAs && v.property!=STORE_INVALID_URIID && !out.ses->getStore()->queryMgr->checkCalcPropID(v.property))
				{if (!out.append(" "AS_S" ",sizeof(AS_S)+1)) return RC_NOMEM; rc=out.renderName(v.property);}
			if (rc!=RC_OK) return rc;
		}
		if (nOuts>1 && !out.append("},",j+1<nOuts?2:1)) return RC_NOMEM;
		out.cvar=save;
	} else {
		// check dscr in children (if join)
		if (!out.append(STAR_S,sizeof(STAR_S)-1)) return RC_NOMEM;
	}
	if (type!=QRY_SIMPLE || ((SimpleVar*)this)->classes!=NULL || !((SimpleVar*)this)->expr.isEmpty() || ((SimpleVar*)this)->path!=NULL) {
		if (!out.append(" " FROM_S " ",sizeof(FROM_S)+1)) return RC_NOMEM;
		if ((rc=render(RP_FROM,out))!=RC_OK) return rc;
	} else {
		if (((SimpleVar*)this)->condIdx!=NULL) out.flags|=SOM_WHERE;
		if (((SimpleVar*)this)->condFT!=NULL) out.flags|=SOM_MATCH;
	}
	if ((out.flags&SOM_WHERE)!=0 || cond!=NULL || props!=NULL && nProps!=0) {
		if (!out.append(" " WHERE_S " ",sizeof(WHERE_S)+1)) return RC_NOMEM;
		out.flags&=~SOM_AND; if ((rc=render(RP_WHERE,out))!=RC_OK) return rc;
	}
	if ((out.flags&SOM_MATCH)!=0 && (rc=render(RP_MATCH,out))!=RC_OK) return rc;
	if (groupBy!=NULL && nGroupBy!=0) {
		if (!out.append(" " GROUP_S " " BY_S " ",sizeof(GROUP_S)+sizeof(BY_S)+1)) return RC_NOMEM;
		if ((rc=renderOrder(groupBy,nGroupBy,out))!=RC_OK) return rc;
		if (having!=NULL) {
			if (!out.append(" " HAVING_S " ",sizeof(HAVING_S)+1)) return RC_NOMEM;
			if ((rc=having->render(0,out))!=RC_OK) return rc;
		}
		if (!out.append(" ",1)) return RC_NOMEM;
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
		case QRY_UNION: if (!out.append(" "UNION_S" ",sizeof(UNION_S)+1)) return RC_NOMEM; break;
		case QRY_EXCEPT: if (!out.append(" "EXCEPT_S" ",sizeof(EXCEPT_S)+1)) return RC_NOMEM; break;
		case QRY_INTERSECT: if (!out.append(" "INTERSECT_S" ",sizeof(INTERSECT_S)+1)) return RC_NOMEM; break;
		}
		if ((qvf&QVF_ALL)!=0) {
			if (!out.append(ALL_S" ",sizeof(ALL_S))) return RC_NOMEM;
		} else if ((qvf&QVF_DISTINCT)!=0) {
			if (!out.append(DISTINCT_S" ",sizeof(DISTINCT_S))) return RC_NOMEM;
		}
		if (fPar && !out.append("(",1)) return RC_NOMEM;
		if ((rc=vr.var->render(out))!=RC_OK) return rc;
		if (fPar && !out.append(")",1)) return RC_NOMEM;
	}
	return RC_OK;
}

RC QVar::render(RenderPart part,SOutCtx& out) const
{
	RC rc;
	switch (part) {
	default: break;
	case RP_FROM:
		if (cond!=NULL||props!=NULL&&nProps!=0) out.flags|=SOM_WHERE;
		if ((out.flags&SOM_VAR_NAMES)!=0) {
			if (!out.append(" "AS_S" ",sizeof(AS_S)+1)) return RC_NOMEM;
			if ((rc=out.renderVarName(this))!=RC_OK) return rc;
		}
		break;
	case RP_WHERE:
		if (props!=NULL && nProps!=0) {
			const char *exstr=SInCtx::opDscr[OP_EXISTS].str; size_t lExist=strlen(exstr);
			for	(unsigned i=0; i<nProps; i++,out.flags|=SOM_AND) {
				if ((out.flags&SOM_AND)!=0 && !out.append(" "AND_S" ",sizeof(AND_S)+1)) return RC_NOMEM;
				if (!out.append(exstr,lExist)||!out.append("(",1)) return RC_NOMEM;
				if ((rc=out.renderName(props[i]&STORE_MAX_URIID,NULL))!=RC_OK) return rc;
				if (!out.append(")",1)) return RC_NOMEM;
			}
		}
		if (cond!=NULL) {
			const QVar *save=out.cvar; out.cvar=this;
			const int prty=(out.flags&SOM_AND)!=0?SInCtx::opDscr[OP_LAND].prty[0]:0;
			if ((out.flags&SOM_AND)!=0 && !out.append(" " AND_S " ",sizeof(AND_S)+1)) return RC_NOMEM;
			if ((rc=cond->render(prty,out))!=RC_OK) return rc; out.cvar=save; out.flags|=SOM_AND;
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
	//if (part==RP_FROM && !out.append("(",1)) return RC_NOMEM;
	// other parts?
	out.flags|=SOM_VAR_NAMES;
	for (unsigned i=0; i<nVars; ++i) {
		const QVarRef& vr=vars[i];
		if (i!=0 && part==RP_FROM) {
			switch (type) {
			default: assert(0); return RC_INTERNAL;
			case QRY_JOIN: case QRY_SEMIJOIN: break;
			case QRY_LEFT_OUTER_JOIN: if (!out.append(" "LEFT_S" ",sizeof(LEFT_S)+1)) return RC_NOMEM; break;
			case QRY_RIGHT_OUTER_JOIN: if (!out.append(" "RIGHT_S" ",sizeof(RIGHT_S)+1)) return RC_NOMEM; break;
			case QRY_FULL_OUTER_JOIN: if (!out.append(" "FULL_S" "OUTER_S" ",sizeof(FULL_S)+sizeof(OUTER_S)+1)) return RC_NOMEM; break;
			}
			if (!out.append(" "JOIN_S" ",sizeof(JOIN_S)+1)) return RC_NOMEM;
		}
		if ((rc=vr.var->render(part,out))!=RC_OK) return rc;
	}
	if (part==RP_FROM && (condEJ!=NULL || cond!=NULL)) {
		const QVar *save=out.cvar; out.cvar=this;
		if (!out.append(" "ON_S" ",sizeof(ON_S)+1)) return RC_NOMEM;
		for (CondEJ *ej=condEJ; ej!=NULL; ej=ej->next) {
			if (ej!=condEJ && !out.append(" "AND_S" ",sizeof(AND_S)+1)) return RC_NOMEM;
			Value v; v.setVarRef(0,ej->propID1);
			if ((rc=out.renderValue(v))!=RC_OK) return rc;
			if (!out.append(SInCtx::opDscr[OP_EQ].str,SInCtx::opDscr[OP_EQ].lstr)) return RC_NOMEM;
			v.setVarRef(1,ej->propID2);
			if ((rc=out.renderValue(v))!=RC_OK) return rc;
		}
		if (cond!=NULL) {
			if (condEJ!=NULL) out.flags|=SOM_AND;
			const int prty=condEJ!=NULL?SInCtx::opDscr[OP_LAND].prty[0]:0;
			if ((out.flags&SOM_AND)!=0 && !out.append(" "AND_S" ",sizeof(AND_S)+1)) return RC_NOMEM;
			if ((rc=cond->render(prty,out))!=RC_OK) return rc; out.flags|=SOM_AND;
		}
		out.cvar=save;
	}
	//if (part==RP_FROM && level!=0 && !out.append(")",1)) return RC_NOMEM;
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
	RC rc; const QVar *save; unsigned i;
	switch (part) {
	default: break;
	case RP_FROM:
		if (condIdx!=NULL) out.flags|=SOM_WHERE;
		if (condFT!=NULL) out.flags|=SOM_MATCH;
		switch (expr.type) {
		case VT_REF:
			//???
			break;
		case VT_COLLECTION: case VT_REFID: case VT_STRUCT: case VT_EXPR: case VT_STMT: case VT_VARREF:
			if ((rc=out.renderValue(expr))!=RC_OK) return rc;
			break;
		}
		if (classes!=NULL && nClasses!=0) for (i=0; i<nClasses; i++) {
			const SourceSpec &cs=classes[i];
			if (i>0 && !out.append(" & ",3)) return RC_NOMEM;
			if ((rc=out.renderName(cs.objectID))!=RC_OK) return rc;
			if (cs.params!=NULL && cs.nParams>0) {
				for (unsigned j=0; j<cs.nParams; j++) {
					out.append(j==0?"(":",",1);
					if ((rc=out.renderValue(cs.params[j]))!=RC_OK) return rc;
				}
				out.append(")",1);
			}
		} else if (expr.isEmpty() && !out.append(STAR_S,sizeof(STAR_S)-1)) return RC_NOMEM;
		if (path!=NULL) for (unsigned i=0; i<nPathSeg; i++) if ((rc=out.renderPath(path[i]))!=RC_OK) return rc;
		break;
	case RP_WHERE:
		save=out.cvar; out.cvar=this;
		for (CondIdx *ci=condIdx; ci!=NULL; ci=ci->next,out.flags|=SOM_AND) {
			assert(ci->ks.op>=OP_EQ && ci->ks.op<=OP_BEGINS);
			if ((out.flags&SOM_AND)!=0 && !out.append(" " AND_S " ",sizeof(AND_S)+1)) return RC_NOMEM;
			if (ci->expr==NULL) {
				if ((rc=out.renderName(ci->ks.propID))!=RC_OK) return rc;
			} else {
				if ((ci->expr->render(SInCtx::opDscr[OP_EQ].prty[0],out))!=RC_OK) return rc;
			}
			if (ci->ks.op>=OP_ALL) return RC_CORRUPTED;
			if (!out.append(" ",1)) return RC_NOMEM;
			if (!out.append(SInCtx::opDscr[ci->ks.op].str,SInCtx::opDscr[ci->ks.op].lstr)) return RC_NOMEM;
			if (!out.append(" ",1)) return RC_NOMEM;
			Value v; v.setParam((byte)ci->param,(ValueType)ci->ks.type,ci->ks.flags);
			if ((rc=out.renderValue(v))!=RC_OK) return rc;
		}
		out.cvar=save; break;
	case RP_MATCH:
		if (condFT!=NULL) {
			if (!out.append(" " MATCH_S,sizeof(MATCH_S))) return RC_NOMEM;
			if ((out.flags&SOM_VAR_NAMES)!=0 /*|| multiple properties */) {
				if (!out.append(" (",2)) return RC_NOMEM;
				if ((rc=out.renderVarName(this))!=RC_OK) return rc;		//???	+ properties?
				if (!out.append(")",1)) return RC_NOMEM;
			}
			if (!out.append(" "AGAINST_S" (",sizeof(AGAINST_S)+2)) return RC_NOMEM;
			for (CondFT *ft=condFT; ft!=NULL; ft=ft->next) if (ft->str!=NULL) {
				Value v; v.set(ft->str); if ((rc=out.renderValue(v))!=RC_OK) return rc;
				// ft-> flags ???
				if (ft->nPids>0) {
					// . A or (A, B, C)
				}
				if (!out.append(ft->next!=NULL?",":")",1)) return RC_NOMEM;
			}
		}
		break;
	}
	return QVar::render(part,out);
}

RC PIN::render(SOutCtx&,bool fInsert) const
{
	//...
	return RC_OK;
}

//------------------------------------------------------------------------------------------------------

RC ExprNode::render(const Value& v,int prty,SOutCtx& out)
{
	return v.type==VT_EXPRTREE?((ExprNode*)v.exprt)->render(prty,out):out.renderValue(v);
}

RC ExprNode::render(int prty,SOutCtx& out) const
{
	if (op==OP_CON) return render(operands[0],prty,out);
	RC rc; ExprOp o=op; if (o>=OP_ALL) return RC_CORRUPTED;
	const OpDscr *od=&SInCtx::opDscr[o]; bool f,fRange; ushort i; const char *p;
	switch (o) {
	case OP_EXISTS:
		f=(flags&NOT_BOOLEAN_OP)!=0;
		if (nops==1 && operands[0].type==VT_STMT) {
			unsigned save; out.saveFlags(save);
			if (!out.append(f?" "NOT_S" EXISTS(":" EXISTS(",f?sizeof(NOT_S)+8:8)) return RC_NOMEM;		// any other predicate functions???
			if ((rc=((Stmt*)operands[0].stmt)->render(out))!=RC_OK) return rc;
			if (!out.append(")",1)) return RC_NOMEM;
			out.restoreFlags(save);
		} else {
			int pr=SInCtx::opDscr[LX_IS].prty[0];
			if (prty>pr && !out.append("(",1)) return RC_NOMEM;
			if (nops>1) {
				//...
			} else if ((rc=render(operands[0],pr,out))!=RC_OK) return rc;
			if (!out.append(f?" IS NULL":" IS NOT NULL",f?8:12) || prty>pr && !out.append(")",1)) return RC_NOMEM;
		}
		break;
	case OP_IS_A:
		if (prty>od->prty[0] && !out.append("(",1)) return RC_NOMEM;
		if ((rc=render(operands[0],od->prty[0],out))!=RC_OK) return rc;
		f=(flags&NOT_BOOLEAN_OP)!=0;
		if (!out.append(f?" IS NOT A ":" IS A ",f?10:6)) return RC_NOMEM;
		if ((rc=operands[1].type==VT_URIID?out.renderName(operands[1].uid):out.renderValue(operands[1]))!=RC_OK) return rc;
		if (nops>2) {
			for (i=2,prty=SInCtx::opDscr[LX_COMMA].prty[0]; i<nops; i++) {
				if (!out.append(i==2?"(":",",1)) return RC_NOMEM;
				if ((rc=render(operands[i],prty,out))!=RC_OK) return rc;
			}
			if (!out.append(")",1)) return RC_NOMEM;
		}
		if (prty>od->prty[0] && !out.append(")",1)) return RC_NOMEM;
		break;
	case OP_IN:
		if (prty>od->prty[0] && !out.append("(",1)) return RC_NOMEM;
		f=(flags&NOT_BOOLEAN_OP)!=0; fRange=operands[1].type==VT_RANGE;
		if (fRange && operands[1].varray[0].type!=VT_ANY && operands[1].varray[1].type!=VT_ANY
										|| operands[1].type==VT_EXPRTREE && operands[1].exprt->getOp()==OP_RANGE) {
			if ((rc=render(operands[0],SInCtx::opDscr[LX_BETWEEN].prty[0],out))!=RC_OK) return rc;
			if (!out.append(f?" NOT "BETWEEN_S" ":" "BETWEEN_S" ",f?sizeof(BETWEEN_S)+5:sizeof(BETWEEN_S)+1)) return RC_NOMEM;
			if (operands[1].type==VT_EXPRTREE) {
				if ((rc=render(operands[1].exprt->getOperand(0),SInCtx::opDscr[LX_BAND].prty[0],out))!=RC_OK) return rc;
			} else if ((rc=out.renderValue(operands[1].varray[0]))!=RC_OK) return rc;
			if (!out.append(" "AND_S" ",sizeof(AND_S)+1)) return RC_NOMEM;
			if (operands[1].type==VT_EXPRTREE) {
				if ((rc=render(operands[1].exprt->getOperand(1),SInCtx::opDscr[LX_BAND].prty[0],out))!=RC_OK) return rc;
			} else if ((rc=out.renderValue(operands[1].varray[1]))!=RC_OK) return rc;
		} else {
			if ((rc=render(operands[0],od->prty[0],out))!=RC_OK) return rc;
			if (!out.append(f?" NOT IN ":" IN ",f?8:4) || !fRange && !out.append("(",1)) return RC_NOMEM;
			if (fRange) {
				if ((rc=out.renderValue(operands[1]))!=RC_OK) return rc;
			} else if (operands[1].type==VT_COLLECTION && !operands[1].isNav()) for (uint32_t i=0; i<operands[1].length; i++) {
				if (i!=0 && !out.append(",",1)) return RC_NOMEM;
				if ((rc=render(operands[1].varray[i],SInCtx::opDscr[LX_COMMA].prty[0],out))!=RC_OK) return rc;
			} else if (operands[1].type==VT_EXPRTREE && operands[1].exprt->getOp()==OP_COLLECTION) {
				ExprNode *et=(ExprNode*)operands[1].exprt;
				for (unsigned i=0; i<et->nops; i++) {
					if (i!=0 && !out.append(",",1)) return RC_NOMEM;
					if ((rc=render(et->operands[i],SInCtx::opDscr[LX_COMMA].prty[0],out))!=RC_OK) return rc;
				}
			} else if ((rc=render(operands[1],od->prty[0],out))!=RC_OK) return rc;
			if (!fRange && !out.append(")",1)) return RC_NOMEM;
		}
		if (prty>od->prty[0] && !out.append(")",1)) return RC_NOMEM;
		break;
	case OP_CAST:
		if (operands[1].type!=VT_INT && operands[1].type!=VT_UINT || operands[1].i<=VT_ERROR) return RC_CORRUPTED;
		if (!out.append("CAST(",5)) return RC_NOMEM;
		if ((rc=render(operands[0],prty,out))!=RC_OK) return rc;
		if (!out.append(" "AS_S" ",sizeof(AS_S)+1)) return RC_NOMEM;
		if (operands[1].i<VT_ALL) {if (!out.append(typeName[operands[1].i].s,typeName[operands[1].i].l)) return RC_NOMEM;}
		else if ((operands[1].i&0xFF)!=VT_DOUBLE && (operands[1].i&0xFF)!=VT_FLOAT || (p=getUnitName((Units)byte(operands[1].i>>8)))==NULL) return RC_CORRUPTED;
		else if (!out.append(p,strlen(p))) return RC_NOMEM;
		if (!out.append(")",1)) return RC_NOMEM;
		break;
	case OP_TRIM:
		if (!out.append("TRIM(",5)) return RC_NOMEM;
		if (operands[2].type!=VT_INT && operands[2].type!=VT_UINT || operands[2].ui>2) return RC_CORRUPTED;
		if (operands[2].ui==1 && !out.append("LEADING ",8)) return RC_NOMEM;
		else if (operands[2].ui==2 && !out.append("TRAILING ",9)) return RC_NOMEM;
		if ((rc=render(operands[1],prty,out))!=RC_OK) return rc;
		if (!out.append(" "FROM_S" ",sizeof(FROM_S)+1)) return RC_NOMEM;
		if ((rc=render(operands[0],prty,out))!=RC_OK) return rc;
		if (!out.append(")",1)) return RC_NOMEM;
		break;
	case OP_EXTRACT:
		if (!out.append("EXTRACT(",8)) return RC_NOMEM;
		if ((operands[1].type==VT_INT || operands[1].type==VT_UINT) && operands[1].ui<sizeof(extractWhat)/sizeof(extractWhat[0])) {
			if (!out.append(extractWhat[operands[1].ui].s,extractWhat[operands[1].ui].l)) return RC_NOMEM;
		} else if ((rc=render(operands[1],SInCtx::opDscr[LX_COMMA].prty[0],out))!=RC_OK) return rc;
		if (!out.append(" "FROM_S" ",sizeof(FROM_S)+1)) return RC_NOMEM;
		if ((rc=render(operands[0],SInCtx::opDscr[LX_COMMA].prty[0],out))!=RC_OK) return rc;
		if (!out.append(")",1)) return RC_NOMEM;
		break;
	case OP_CALL:
		if ((rc=render(operands[0],od->prty[0],out))!=RC_OK) return rc;
		if (!out.append("(",1)) return RC_NOMEM;
		for (i=1,prty=SInCtx::opDscr[LX_COMMA].prty[0]; i<nops; i++) {
			if (i>1 && !out.append(", ",2)) return RC_NOMEM;
			if ((rc=render(operands[i],prty,out))!=RC_OK) return rc;
		}
		if (!out.append(")",1)) return RC_NOMEM;
		break;
	case OP_ELEMENT:
		if ((rc=render(operands[0],od->prty[0],out))!=RC_OK) return rc;
		if (!out.append("[",1)) return RC_NOMEM;
		if ((rc=render(operands[1],SInCtx::opDscr[LX_LBR].prty[1],out))!=RC_OK) return rc;
		if (!out.append("]",1)) return RC_NOMEM;
		break;
	case OP_PATH:
		if ((rc=render(operands[0],od->prty[0],out))!=RC_OK) return rc;
		switch (operands[1].type) {
		case VT_ANY: if (!out.append(PATH_DELIM"*",sizeof(PATH_DELIM))) return RC_NOMEM; break;
		case VT_URIID: if ((rc=out.renderName(operands[1].uid,PATH_DELIM))!=RC_OK) return rc; break;
		case VT_COLLECTION:
			if (!operands[1].isNav() && operands[1].fcalc==0) {
				if (!out.append(PATH_DELIM"{",sizeof(PATH_DELIM))) return RC_NOMEM;
				for (unsigned i=0; i<operands[1].length; i++) {
					if (i!=0 && !out.append(",",1)) return RC_NOMEM;
					if (operands[1].varray[i].type==VT_URIID && (rc=out.renderName(operands[1].varray[i].uid,PATH_DELIM))!=RC_OK) return rc;
				}
				if (!out.append("}",1)) return RC_NOMEM; break;
			}
		default:
			if (!out.append(PATH_DELIM,sizeof(PATH_DELIM)-1)) return RC_NOMEM;
			if ((rc=render(operands[1],100,out))!=RC_OK) return rc;
			break;
		}
		f=nops>=4 && (operands[3].type!=VT_UINT || operands[3].ui!=0x00010001);
		if (f && (flags&FILTER_LAST_OP)!=0 && (rc=out.renderRep(operands[3]))!=RC_OK) return rc;
		if (nops>=3) switch (operands[2].type) {
		case VT_INT: case VT_UINT:
			if ((rc=renderElement(operands[2].ui,out))!=RC_OK) return rc; break;
		case VT_EXPRTREE:
		case VT_EXPR:
			if (!out.append("[",1)) return RC_NOMEM;
			if ((rc=operands[2].type==VT_EXPR?((Expr*)operands[2].expr)->render(0,out):((ExprNode*)operands[2].exprt)->render(0,out))!=RC_OK) return rc;
			if (!out.append("]",1)) return RC_NOMEM;
			break;
		}
		if (f && (flags&FILTER_LAST_OP)==0 && (rc=out.renderRep(operands[3]))!=RC_OK) return rc;
		break;
	case OP_ARRAY:
		if (!out.append("[",1)) return RC_NOMEM;
		//???
		if (!out.append("]",1)) return RC_NOMEM;
		break;
	case OP_PIN: if (!out.append("@",1)) return RC_NOMEM;
	case OP_STRUCT:
		if (!out.append("{",1)) return RC_NOMEM;
		for (i=0; i<nops; i++) if ((rc=out.renderProperty(operands[i],i==0?1:3))!=RC_OK) return rc;
		if (!out.append("}",1)) return RC_NOMEM;
		break;
	case OP_CONCAT: if (nops==2) od=&SInCtx::opDscr[o=ExprOp(LX_CONCAT)];
	default:
		if (od->str==NULL) return RC_CORRUPTED;
		if ((od->flags&_F)!=0) {
			if (!out.append(od->str,od->lstr) || !out.append("(",1)) return RC_NOMEM;
			if ((od->flags&_A)!=0 && (flags&DISTINCT_OP)!=0 && !out.append(DISTINCT_S" ",sizeof(DISTINCT_S))) return RC_NOMEM;
			prty=SInCtx::opDscr[LX_COMMA].prty[0];
			for (unsigned i=0; i<nops; i++) {
				if ((rc=render(operands[i],prty,out))!=RC_OK) return rc;
				if (!out.append(i+1==nops?")":",",1)) return RC_NOMEM;
			}
		} else {
			unsigned idx=0;
			if (prty>od->prty[0] && !out.append("(",1)) return RC_NOMEM;
			if ((od->flags&_U)==0 || o==OP_RANGE || o==OP_COLLECTION) {
				if (o==OP_RANGE) {if (!out.append("@[",2)) return RC_NOMEM;}
				else if (o==OP_COLLECTION) {if (!out.append("{",1)) return RC_NOMEM;}
				if ((rc=render(operands[idx++],od->prty[0],out))!=RC_OK) return rc;
				if (!out.append(" ",1)) return RC_NOMEM;
			} else if (nops!=1) return RC_CORRUPTED;
			// other flags!!!
			if (o==OP_RSHIFT) {if ((flags&UNSIGNED_OP)!=0) od=&SInCtx::opDscr[LX_URSHIFT];}
			else if ((flags&NOT_BOOLEAN_OP)!=0 && !out.append(NOT_S" ",sizeof(NOT_S))) return RC_NOMEM;
			do {
				if (!out.append(od->str,od->lstr) || !out.append(" ",1)) return RC_NOMEM;
				if (op==OP_SIMILAR) {
					if (!out.append(TO_S" ",sizeof(TO_S))) return RC_NOMEM;
					if ((flags&COMP_PATTERN_OP)==0) {
						assert(idx==1 && operands[1].type==VT_BSTR && operands[1].length>=sizeof(RegExpHdr));
						if (!out.append("/",1)) return RC_NOMEM;
						const RegExpHdr *rh=(RegExpHdr*)operands[1].bstr;
						if (!out.append((char*)(rh+1),rh->lsrc) || !out.append("/",1)) return RC_NOMEM;
						if ((rh->flags&RXM_MULTIM)!=0 && !out.append("g",1) ||
							(rh->flags&RXM_NCASE)!=0 && !out.append("i",1) ||
							(rh->flags&RXM_MULTIL)!=0 && !out.append("m",1)) return RC_NOMEM;
						break;
					}
				}
				if ((rc=render(operands[idx],od->prty[0],out))!=RC_OK) return rc;
			} while (++idx<nops);
			if (o==OP_RANGE) {if (!out.append("]",1)) return RC_NOMEM;}
			else if (o==OP_COLLECTION) {if (!out.append("}",1)) return RC_NOMEM;}
			if (prty>od->prty[0] && !out.append(")",1)) return RC_NOMEM;
		}
		break;
	}
	return RC_OK;
}

RC Expr::render(int prty,SOutCtx& out) const
{
	ExprNode *et=NULL; RC rc=decompile(et,out.getSession());
	if (rc==RC_OK) {rc=et->render(prty,out); et->destroy();}
	return rc;
}

bool SOutCtx::expand(size_t len)
{
	size_t d=xLen==0?256:xLen/2; if (d<len) d=len; return (ptr=(byte*)ses->realloc(ptr,xLen+=d))!=NULL;
}

char *SOutCtx::renderAll()
{
	if ((mode&TOS_PROLOGUE)==0 || usedQNames==NULL && (flags&(SOM_STD_PREFIX|SOM_SRV_PREFIX))==0) return (char*)*this;
	size_t l=cLen; char *p=(char*)*this; cLen=xLen=0; unsigned skip=~0u,idx;
	if ((flags&SOM_BASE_USED)!=0) {
		if (!append(BASE_S" ",sizeof(BASE_S))) {ses->free(p); return NULL;}
		for (unsigned i=0; i<nUsedQNames; i++) 
			if ((idx=usedQNames[i])==~0u || qNames[idx].qpref==NULL) {skip=i; break;}
		if (!append("'",1) || !append(qNames[idx].str,qNames[idx].lstr) || !append("' ",2)) {ses->free(p); return NULL;}
	}
	for (unsigned i=0; i<nUsedQNames; i++) if (i!=skip) {
		idx=usedQNames[i]; assert(idx!=~0u);
		if (!append(PREFIX_S" ",sizeof(PREFIX_S)) || !append(qNames[idx].qpref,qNames[idx].lq) || !append(QNAME_DELIM" '",sizeof(QNAME_DELIM)+1)
			|| !append(qNames[idx].str,qNames[idx].lstr) || !append("' ",2)) {ses->free(p); return NULL;}
	}
	if ((p=(char*)ses->realloc(p,cLen+l+1))!=NULL) {memmove(p+cLen,p,l+1); memcpy(p,(char*)*this,cLen);}
	return p;
}

RC SOutCtx::renderName(uint32_t id,const char *prefix,const QVar *qv,bool fQ,bool fEsc) {
	URI *uri=NULL; RC rc=RC_OK;
	if (id==PROP_SPEC_SELF) return append("@",1)?RC_OK:RC_NOMEM;
	if ((flags&SOM_VAR_NAMES)!=0 && qv!=NULL) {
		if ((rc=renderVarName(qv))!=RC_OK) return rc;
		prefix=URIID_PREFIX;
	}
	if (prefix!=NULL && !append(prefix,1)) return RC_NOMEM;
	const char *bname; size_t lbname;
	if (id<=MAX_BUILTIN_URIID && (bname=NamedMgr::getBuiltinName(id,lbname))!=NULL) {
		const char *p; size_t l;
		if (id>=MIN_BUILTIN_SERVICE) {
			if ((mode&TOS_NO_QNAMES)==0) {p=AFFINITY_SRV_QPREFIX; l=sizeof(AFFINITY_SRV_QPREFIX)-1; flags|=SOM_SRV_PREFIX;} else {p=AFFINITY_SERVICE_PREFIX; l=sizeof(AFFINITY_SERVICE_PREFIX)-1;}
		} else {
			if ((mode&TOS_NO_QNAMES)==0) {p=AFFINITY_STD_QPREFIX; l=sizeof(AFFINITY_STD_QPREFIX)-1; flags|=SOM_STD_PREFIX;} else {p=AFFINITY_STD_URI_PREFIX; l=sizeof(AFFINITY_STD_URI_PREFIX)-1;}
		}
		if (!append(p,l) || !append(bname,lbname)) rc=RC_NOMEM;
	} else if ((uri=(URI*)ses->getStore()->uriMgr->ObjMgr::find(id))==NULL) rc=RC_NOTFOUND;		// .09090900 ???
	else {
		const StrLen *ss=uri->getURI(); assert(ss!=NULL);
		const char *s=ss->str; size_t l=ss->len; const URIInfo ui=uri->getInfo();
		if ((ui.flags&UID_ID)!=0) {if (!append(s,l)) rc=RC_NOMEM;}
		else {
			if ((mode&TOS_NO_QNAMES)==0 && (ui.flags&UID_IRI)!=0 && l>ui.lSuffix) {
				bool fQName=false; unsigned idx=~0u; size_t lpref=l-ui.lSuffix,lqp=0; const char *qp=NULL; const QName *qn;
				const static char *stdQPref=AFFINITY_STD_QPREFIX,*stdPref=AFFINITY_STD_URI_PREFIX;
				const static char *srvQPref=AFFINITY_SRV_QPREFIX,*srvPref=AFFINITY_SERVICE_PREFIX;
				for (unsigned i=0; i<nUsedQNames; i++) {
					if ((idx=usedQNames[i])==~0u) {
						if (fQName=lpref==sizeof(AFFINITY_STD_URI_PREFIX)-1 && memcmp(s,stdPref,lpref)==0) {qp=stdQPref; lqp=sizeof(AFFINITY_STD_QPREFIX)-2;}
					} else {
						qn=&qNames[idx]; if (fQName=lpref==qn->lstr && memcmp(s,qn->str,lpref)==0) {qp=qn->qpref; lqp=qn->lq;}
					}
					if (fQName) {if (i!=0) memmove(usedQNames+1,usedQNames,i*sizeof(unsigned)); break;}
				}
				if (!fQName) {
					idx=~0u;
					if ((qn=qNames)!=NULL) for (unsigned i=0; i<nQNames; ++i,++qn)
						if (fQName=lpref==qn->lstr && memcmp(s,qn->str,lpref)==0) {qp=qn->qpref; lqp=qn->lq; idx=i; break;}
					if (!fQName && lpref==sizeof(AFFINITY_STD_URI_PREFIX)-1 && !memcmp(s,stdPref,lpref))
						{fQName=true; qp=stdQPref; lqp=sizeof(AFFINITY_STD_QPREFIX)-2; idx=~0u;}
					else if (!fQName && lpref==sizeof(AFFINITY_SERVICE_PREFIX)-1 && !memcmp(s,srvPref,lpref))
						{fQName=true; qp=srvQPref; lqp=sizeof(AFFINITY_SRV_QPREFIX)-2; idx=~0u;}
					if (fQName) {
						if ((usedQNames=(unsigned*)ses->realloc(usedQNames,(nUsedQNames+1)*sizeof(unsigned)))==NULL) return RC_NOMEM;
						if (nUsedQNames!=0) memmove(usedQNames+1,usedQNames,nUsedQNames*sizeof(unsigned)); nUsedQNames++;
					}
				}
				if (fQName) {
					usedQNames[0]=idx; s+=lpref; l=ui.lSuffix; fEsc=fEsc&&(ui.flags&UID_SID)==0;
					if (qp==NULL || lqp==0) flags|=SOM_BASE_USED; else if (!append(qp,lqp) || !append(QNAME_DELIM,1)) return RC_NOMEM;
				}
			}
			if (!fQ && fEsc && !append("\"",1)) return RC_NOMEM;
			if ((ui.flags&UID_DQU)!=0) do {
				const char *p=(char*)memchr(s,'"',l);
				if (p==NULL) {if (!append(s,l)) return RC_NOMEM; break;}
				if (p>s) {size_t ls=p-s; if (!append(s,ls)) return RC_NOMEM; s=p; l-=ls;}
				if (!append(fQ?"\\\"":"\"\"",2)) return RC_NOMEM;
			} while ((++s,--l)!=0);
			else if (!append(s,l)) return RC_NOMEM;
			if (!fQ && fEsc && !append("\"",1)) return RC_NOMEM;
		}
		uri->release();
	}
	return rc;
}

RC SOutCtx::renderPID(const PID& id,bool fJ) {
	size_t l=sprintf(cbuf,fJ?"{\"$ref%s\":\"" _LX_FM "\"}":PID_PREFIX "%s" _LX_FM,id.isTPID()?":":"",id.pid);
	if (!append(cbuf,l))  return RC_NOMEM;
	if (!fJ && id.ident!=STORE_OWNER && id.ident!=STORE_INVALID_IDENTITY && id.ident!=STORE_INMEM_IDENTITY) {
		Identity *ident=(Identity*)ses->getStore()->identMgr->ObjMgr::find(id.ident); if (ident==NULL) return RC_NOTFOUND;
		const StrLen *s=ident->getName(); bool f=append(IDENTITY_PREFIX,1) && append(s->str,s->len);
		ident->release(); if (!f) return RC_NOMEM;
	}
	return RC_OK;
}

RC SOutCtx::renderValue(const Value& v,JRType jr) {
	RC rc=RC_OK; unsigned u,sflags; Identity *ident; const char *s; size_t l; const Value *cv,*cv2; const QVar *qv; Value vv; bool ff; char *ss;
	if (jr!=JR_NO) {
		if (v.type>VT_DOUBLE && !isString((ValueType)v.type) && v.type!=VT_COLLECTION && v.type!=VT_STRUCT && v.type!=VT_MAP && v.type!=VT_REFID && v.type!=VT_URIID)
			{if ((rc=convV(v,vv,VT_STRING,ses,CV_NODEREF))==RC_OK) {vv.setPropID(v.property); vv.eid=v.eid; rc=renderValue(vv,jr); freeV(vv);} return rc;}
		if (jr==JR_EID) {l=sprintf(cbuf,"\"%d\": ",v.eid); if (!append(cbuf,l)) return RC_NOMEM;}
		else if (jr==JR_PROP) {
			if (!append("\"",1)) return RC_NOMEM;
			if ((rc=renderName(v.property!=STORE_INVALID_URIID?v.property:PROP_SPEC_VALUE,NULL,NULL,true))!=RC_OK) return rc;
			if (!append("\": ",3)) return RC_NOMEM;
		}
	}
	switch (v.type) {
	case VT_ANY: if (!append(NULL_S,sizeof(NULL_S)-1)) return RC_NOMEM; break;
	case VT_STRING:
		if (!append(jr!=JR_NO?"\"":"'",1)) return RC_NOMEM;
		if (v.length!=0) {
			s=v.str; l=v.length;
			if (jr!=JR_NO && (memchr(s,'\n',l)!=NULL||memchr(s,'\t',l)!=NULL)) for (size_t i=0; i<l; ++i,++s) {
				const char *p; size_t ll;
				if (*s=='\n') p="\\n",ll=2; else if (*s=='\n') p="\\t",ll=2; else if (*s=='"') p="\\\"",ll=2; else p=s,ll=1;
				if (!append(p,ll)) return RC_NOMEM;
			} else do {
				const char *p=(char*)memchr(s,jr!=JR_NO?'"':'\'',l);
				if (p==NULL) {if (!append(s,l)) return RC_NOMEM; break;}
				if (p>s) {size_t ls=p-s; if (!append(s,ls)) return RC_NOMEM; s=p; l-=ls;}
				if (!append(jr!=JR_NO?"\\\"":"''",2)) return RC_NOMEM;
			} while ((++s,--l)!=0);
		}
		if (!append(jr!=JR_NO?"\"":"'",1)) return RC_NOMEM;
		break;
	case VT_BSTR:		// special delimeter?
		if (jr!=JR_NO && !append("\"",1) || !append("X'",2)) return RC_NOMEM;
		for (u=0; u<v.length; u++) {sprintf(cbuf,"%02X",v.bstr[u]); if (!append(cbuf,2)) return RC_NOMEM;}
		if (!append("'",1) || jr!=JR_NO && !append("\"",1)) return RC_NOMEM;
		break;
	case VT_STREAM:
		//???
		break;
	case VT_INT: l=sprintf(cbuf,"%d",v.i); if (!append(cbuf,l)) return RC_NOMEM; break;
	case VT_UINT: l=sprintf(cbuf,"%u%c",v.ui,jr!=JR_NO?' ':'u'); if (!append(cbuf,l)) return RC_NOMEM; break;
	case VT_INT64: 
		l=sprintf(cbuf,_LD_FM,v.i64); if (!append(cbuf,l)) return RC_NOMEM; break;
	case VT_UINT64:
		l=sprintf(cbuf,_LU_FM"%c",v.ui64,jr!=JR_NO?' ':'U'); if (!append(cbuf,l)) return RC_NOMEM;
		break;
	case VT_ENUM:
		if (jr!=JR_NO && !append("\"",1)) return RC_NOMEM;
		if ((rc=renderName(v.enu.enumid,NULL,NULL,jr!=JR_NO))!=RC_OK) return rc; l=256;
		if (!append("#",1) || (ss=(char*)alloca(l))==NULL) return RC_NOMEM;
		if ((rc=ses->getStore()->classMgr->findEnumStr(ses,v.enu.enumid,v.enu.eltid,ss,l))!=RC_OK) return rc;
		if (!append(ss,l) || jr!=JR_NO && !append("\"",1)) return RC_NOMEM;
		break;
	case VT_DATETIME:
		if (!append(TIMESTAMP_S" '", sizeof(TIMESTAMP_S)+1)) return RC_NOMEM;
		if ((rc=ses->convDateTime(v.ts,cbuf,l))!=RC_OK) break;
		if (!append(cbuf,l) || !append("'",1)) return RC_NOMEM;
		break;
	case VT_INTERVAL:
		if (!append(INTERVAL_S" '", sizeof(INTERVAL_S)+1)) return RC_NOMEM;
		if ((rc=Session::convInterval(v.i64,cbuf,l))!=RC_OK) break;
		if (!append(cbuf,l) || !append("'",1)) return RC_NOMEM;
		break;
	case VT_BOOL: if (!append(v.b?TRUE_S:FALSE_S,v.b?sizeof(TRUE_S)-1:sizeof(FALSE_S)-1)) return RC_NOMEM; break;
	case VT_FLOAT: l=sprintf(cbuf,"%g%c",v.f,jr!=JR_NO?' ':'f'); if (!append(cbuf,l)) return RC_NOMEM; break;
	case VT_DOUBLE: l=sprintf(cbuf,"%g",v.d); if (!append(cbuf,l)) return RC_NOMEM; break;
	case VT_CURRENT:
		switch (v.i) {
		default:	//???
		case CVT_TIMESTAMP: if (!append(CURRENT_TIMESTAMP_S,sizeof(CURRENT_TIMESTAMP_S)-1)) return RC_NOMEM; break;
		case CVT_USER: if (!append(CURRENT_USER_S,sizeof(CURRENT_USER_S)-1)) return RC_NOMEM; break;
		case CVT_STORE: if (!append(CURRENT_STORE_S,sizeof(CURRENT_STORE_S)-1)) return RC_NOMEM; break;
		}
		break;
	case VT_COLLECTION:
		if (!append("{",1)) return RC_NOMEM;
		if (v.isNav()) {
			for (cv=v.nav->navigate(GO_FIRST),u=0; cv!=NULL; cv=v.nav->navigate(GO_NEXT),++u) {
				if (u!=0 && !append(",",1)) return RC_NOMEM;
				if ((rc=renderValue(*cv,jr!=JR_NO?JR_EID:JR_NO))!=RC_OK) break;
			}
			v.nav->navigate(GO_FINDBYID,STORE_COLLECTION_ID);
			if (!append("}",1)) return RC_NOMEM;
		} else for (u=0; u<v.length; u++) {
			if ((rc=renderValue(v.varray[u],jr!=JR_NO?JR_EID:JR_NO))!=RC_OK) break;
			if (!append(u+1==v.length?"}":",",1)) return RC_NOMEM;
		}
		break;
	case VT_STRUCT:
		if (!append("{",1)) return RC_NOMEM;
		for (u=0; u<v.length; u++) {
			if (jr==JR_NO) {if ((rc=renderProperty(v.varray[u],u==0?1:3))!=RC_OK) return rc;}
			else if (u!=0 && !append(",",1)) return RC_NOMEM;
			else if ((rc=renderValue(v.varray[u],JR_PROP))!=RC_OK) break;
		}
		if (!append("}",1)) return RC_NOMEM;
		break;
	case VT_MAP:
		if (!append("{",1)) return RC_NOMEM;
		for (rc=v.map->getNext(cv,cv2,IMAP_FIRST),u=0; rc==RC_OK; rc=v.map->getNext(cv,cv2),++u) {
			if (u!=0 && !append(",",1)) return RC_NOMEM;
			if ((rc=renderValue(*cv,jr!=JR_NO?JR_VAL:JR_NO))!=RC_OK) break;
			if (!append(jr!=JR_NO?": ":"->",2)) return RC_NOMEM;
			if ((rc=renderValue(*cv2,jr!=JR_NO?JR_VAL:JR_NO))!=RC_OK) break;
		}
		if (!append("}",1)) return RC_NOMEM;
		if (rc==RC_EOF) rc=RC_OK; break;
	case VT_ARRAY:
		if (!append("[",1)) return RC_NOMEM;
		for (u=0; u<(unsigned)v.fa.ydim; u++) {
			if (v.fa.ydim!=1 && !append("[",1)) return RC_NOMEM;
			for (unsigned i=0; i<(unsigned)v.fa.xdim; i++) {
				if (v.fa.type==VT_STRUCT) {
					//???
				} else {
					switch (v.fa.type) {
					case VT_INT: vv.set((int)v.fa.i[u*v.fa.xdim+i]); break;
					case VT_UINT: vv.set((unsigned)v.fa.ui[u*v.fa.xdim+i]); break;
					case VT_INT64: case VT_INTERVAL: vv.setI64(v.fa.i64[u*v.fa.xdim+i]); break;
					case VT_UINT64: case VT_DATETIME: vv.setU64((unsigned)v.fa.ui64[u*v.fa.xdim+i]); break;
					case VT_FLOAT: vv.set(v.fa.f[u*v.fa.xdim+i],(Units)v.fa.units); break;
					case VT_DOUBLE: vv.set(v.fa.d[u*v.fa.xdim+i],(Units)v.fa.units); break;
					case VT_REFID: vv.set(v.fa.id[u*v.fa.xdim+i]); break;
					}
					if ((rc=renderValue(vv,jr))!=RC_OK) return rc;
				}
				if (i+1<v.fa.xdim && !append(",",1)) return RC_NOMEM;
			}
			if (v.fa.ydim!=1 && !append("],",u+1<v.fa.ydim?2:1)) return RC_NOMEM;
		}
		if (!append("]",1)) return RC_NOMEM;
		break;
	case VT_RANGE:
		if (!append("@[",2)) return RC_NOMEM;
		if (v.range[0].type==VT_ANY) {if (!append("*",1)) return RC_NOMEM;} else if ((rc=renderValue(v.range[0]))!=RC_OK) break;
		if (!append(",",1)) return RC_NOMEM;
		if (v.range[1].type==VT_ANY) {if (!append("*",1)) return RC_NOMEM;} else if ((rc=renderValue(v.range[1]))!=RC_OK) break;
		if (!append("]",1)) return RC_NOMEM;
		break;
	case VT_URIID:
		if (jr!=JR_NO && !append("\"",1) ||
			(rc=renderName(v.uid,URIID_PREFIX,NULL,jr!=JR_NO))==RC_OK && jr!=JR_NO && !append("\"",1)) rc=RC_NOMEM;
		break;
	case VT_IDENTITY:
		ident=(Identity*)ses->getStore()->identMgr->ObjMgr::find(v.iid);
		if (ident==NULL) rc=RC_NOTFOUND;
		else {
			const StrLen *s=ident->getName(); assert(s!=NULL);
			rc=append(IDENTITY_PREFIX,1) && append(s->str,s->len)?RC_OK:RC_NOMEM;
			ident->release();
		}
		break;
	case VT_REF:
		if (!append("@{",2)) {rc=RC_NOMEM; break;} sflags=1;
		if (((PIN*)v.pin)->id.isPID()) {
			Value w; w.set(((PIN*)v.pin)->id); w.setPropID(PROP_SPEC_PINID);
			if ((rc=renderProperty(w,1))!=RC_OK) return rc; sflags|=2;
		}
		for (u=0; u<((PIN*)v.pin)->nProperties; ++u,sflags|=2) if ((rc=renderProperty(((PIN*)v.pin)->properties[u],sflags))!=RC_OK) return rc;
		if (!append("}",1)) rc=RC_NOMEM;
		break;
	case VT_REFID:
		rc=renderPID(v.id,jr!=JR_NO); break;
	case VT_REFIDPROP:
	case VT_REFIDELT:
		if (jr==JR_NO && v.fcalc==0 && !append("&",1)) {rc=RC_NOMEM; break;}
		if ((rc=renderPID(v.refId->id))!=RC_OK) break;
		if ((rc=renderName(v.refId->pid,URIID_PREFIX))!=RC_OK) break;
		if (v.type==VT_REFIDELT) rc=renderElement(v.refId->eid,*this);
		break;
	case VT_EXPRTREE:
		if (!append("$(",2) || (rc=((ExprNode*)v.exprt)->render(0,*this))==RC_OK && !append(")",1)) rc=RC_NOMEM;
		break;
	case VT_EXPR:
		if (v.fcalc==0 && !append("$(",2) || (rc=((Expr*)v.expr)->render(0,*this))==RC_OK && v.fcalc==0 && !append(")",1)) rc=RC_NOMEM;
		break;
	case VT_STMT:
		saveFlags(sflags);
		if (!(v.fcalc==0?append("${",2):append("(",1)) || (rc=((Stmt*)v.stmt)->render(*this))==RC_OK && !append(v.fcalc==0?"}":")",1)) rc=RC_NOMEM;
		restoreFlags(sflags); break;
	case VT_VARREF:
		switch (v.refV.flags&VAR_TYPE_MASK) {
		case 0:
			qv=NULL; ff=v.length==0||v.refV.id==PROP_SPEC_SELF;
			if (ff && (cvar==NULL || cvar->getType()>=QRY_UNION)) return v.refV.refN!=0?RC_CORRUPTED:append("@",1)?RC_OK:RC_NOMEM;
			if (ff || (flags&SOM_VAR_NAMES)!=0) {
				if ((qv=cvar!=NULL?cvar->getRefVar(v.refV.refN):(QVar*)0)==NULL) return RC_CORRUPTED;
				if (ff) return (rc=renderVarName(qv))!=RC_OK||append(".@",2)?rc:RC_NOMEM;
			}
			if ((rc=renderName(v.refV.id,NULL,qv))==RC_OK) rc=renderElement(v.eid,*this);
			break;
		case VAR_PARAM:
			l=sprintf(cbuf,PARAM_PREFIX"%d",v.refV.refN); if (!append(cbuf,l)) return RC_NOMEM;
			if (v.refV.type<VT_ALL && v.refV.type!=VT_ANY) {
				if (!append("(",1)) return RC_NOMEM;
				if (!append(typeName[v.refV.type].s,typeName[v.refV.type].l)) return RC_NOMEM;
				if ((v.refV.flags&ORD_DESC)!=0 && !append(","DESC_S,sizeof(DESC_S))) return RC_NOMEM;
				if ((v.refV.flags&(ORD_NULLS_BEFORE|ORD_NULLS_AFTER))!=0) {
					if (!append(","NULLS_S" ",sizeof(NULLS_S)+1)) return RC_NOMEM;
					if ((v.refV.flags&ORD_NULLS_BEFORE)!=0) {if (!append(FIRST_S,sizeof(FIRST_S)-1)) return RC_NOMEM;}
					else if (!append(LAST_S,sizeof(LAST_S)-1)) return RC_NOMEM;
				}
				if (!append(")",1)) return RC_NOMEM;
			}
			break;
		case VAR_AGGS:
			if (cvar!=NULL && cvar->aggrs.vals!=NULL && v.refV.refN<cvar->aggrs.nValues) {
				const Value &ag=cvar->aggrs.vals[v.refV.refN]; assert(ag.op<OP_ALL && (SInCtx::opDscr[ag.op].flags&_A)!=0);
				if (!append(SInCtx::opDscr[ag.op].str,SInCtx::opDscr[ag.op].lstr) || !append("(",1)) return RC_NOMEM;
				return (rc=renderValue(ag))!=RC_OK||append(")",1)?rc:RC_NOMEM;
			}
			break;
		case VAR_GROUP:
			if (cvar!=NULL && cvar->groupBy!=NULL && v.refV.refN<cvar->nGroupBy) return renderOrder(&cvar->groupBy[v.refV.refN],1,*this,true);
			break;
		case VAR_REXP:
			if (v.length!=0) {size_t l=sprintf(cbuf,"/%d",v.refV.id); if (!append(cbuf,l)) return RC_NOMEM;}
			break;
		case VAR_CTX:
			if ((size_t)v.refV.refN>=sizeof(atNames)/sizeof(atNames[0])) return RC_CORRUPTED;
			if (!append("@",1) || !append(atNames[v.refV.refN].s,atNames[v.refV.refN].l)) return RC_NOMEM;
			if (v.length!=0 && v.refV.id!=PROP_SPEC_SELF && (rc=renderName(v.refV.id,URIID_PREFIX))==RC_OK) rc=renderElement(v.eid,*this);
			break;
		case VAR_NAMED:
			if (!append("#",1)) return RC_NOMEM; if ((rc=renderName(v.property))!=RC_OK) return rc;
			if (v.length!=0 && v.refV.id!=PROP_SPEC_SELF && (rc=renderName(v.refV.id,URIID_PREFIX))==RC_OK)
				rc=renderElement(v.eid,*this);
			break;
		default: rc=RC_NOTFOUND; break;
		}
		break;
	}
	return rc;
}

RC SOutCtx::renderPath(const PathSeg& ps)
{
	RC rc; unsigned i;
	switch (ps.nPids) {
	case 0: if (!append(PATH_DELIM"{*}",sizeof(PATH_DELIM)+2)) return RC_NOMEM; break;
	case 1: if ((rc=renderName(ps.pid,PATH_DELIM))!=RC_OK) return rc; break;
	default:
		for (i=0; i<ps.nPids; i++) if ((rc=renderName(ps.pids[i],i==0?PATH_DELIM"{":","))!=RC_OK) return rc;
		if (!append("}",1)) return RC_NOMEM; break;
	}
	const bool fRep=ps.rmin!=1||ps.rmax!=1; Value rep; if (fRep) rep.set((unsigned)ps.rmax<<16|ps.rmin);
	if (fRep && ps.fLast && (rc=renderRep(rep))!=RC_OK) return rc;
	if (ps.filter!=NULL || ps.cid!=STORE_INVALID_URIID) {
		if (!append("[",1)) return RC_NOMEM;
		if (ps.cid!=STORE_INVALID_URIID) {
			//"@ IS A ...(params)"
			if (ps.filter!=NULL && !append(" AND ",5)) return RC_NOMEM;
		}
		if (ps.filter!=NULL && (rc=((Expr*)ps.filter)->render(0,*this))!=RC_OK) return rc;
		if (!append("]",1)) return RC_NOMEM;
	} else if (!ps.eid.isEmpty()) {
		if (!append("[",1)) return RC_NOMEM;
		if ((rc=renderValue(ps.eid))!=RC_OK) return rc;
		if (!append("]",1)) return RC_NOMEM;
	} else if ((rc=renderElement(ps.eid.eid,*this))!=RC_OK) return rc;
	return fRep && !ps.fLast ? renderRep(rep) : RC_OK;
}

RC SOutCtx::renderRep(const Value& rep)
{
	RC rc;
	if (rep.type==VT_EXPRTREE) {
		if (!append("{",1)) return RC_NOMEM;
		if (((ExprNode*)rep.exprt)->getOp()==OP_RANGE) {
			if ((rc=renderValue(((ExprNode*)rep.exprt)->getOperand(0)))!=RC_OK) return rc;
			if (!append(",",1)) return RC_NOMEM;
			if ((rc=renderValue(((ExprNode*)rep.exprt)->getOperand(1)))!=RC_OK) return rc;
		} else {
			if ((rc=renderValue(rep))!=RC_OK) return rc;
		}
		if (!append("}",1)) return RC_NOMEM;
	} else if (rep.type!=VT_UINT) return RC_TYPE;
	else if (rep.ui==0x00010000) {if (!append("{?}",3)) return RC_NOMEM;}
	else if (rep.ui==0xFFFF0000) {if (!append("{*}",3)) return RC_NOMEM;}
	else if (rep.ui==0xFFFF0001) {if (!append("{+}",3)) return RC_NOMEM;}
	else {
		char buf[30];
		if ((rep.ui&0xFFFF)==0) {if (!append("{*,",3)) return RC_NOMEM;}
		else {size_t l=sprintf(buf,"{%u,",rep.ui&0xFFFF); if (!append(buf,l)) return RC_NOMEM;}
		if ((rep.ui&0xFFFF0000)==0xFFFF0000) {if (!append("*}",2)) return RC_NOMEM;}
		else {size_t l=sprintf(buf,"%u}",rep.ui>>16&0xFFFF); if (!append(buf,l)) return RC_NOMEM;}
	}
	return RC_OK;
}

RC SOutCtx::renderVarName(const QVar *qv)
{
	if (qv->name!=NULL) return append(qv->name,strlen(qv->name))?RC_OK:RC_NOMEM;
	if (qv->type==QRY_SIMPLE && ((SimpleVar*)qv)->nClasses==1) {
		// use class name ???
	}
	char buf[10]; size_t l=sprintf(buf,"_%d",qv->id); 
	return append(buf,l)?RC_OK:RC_NOMEM;
}

RC SOutCtx::renderJSON(Cursor *cr,uint64_t& cnt)
{
	RC rc=RC_OK; cnt=0; size_t l=1; cbuf[0]='['; 
	Value ret; const unsigned nRes=cr->getNResults();
	while (rc==RC_OK && (rc=((Cursor*)cr)->next(ret))==RC_OK) {
		if (ret.type==VT_REF || nRes>1 && ret.type==VT_COLLECTION && !ret.isNav()) {
			const Value *pvs=&ret;
			if (nRes>1) {cbuf[l++]='['; if (ret.type==VT_COLLECTION) pvs=ret.varray;}
			for (unsigned i=0; rc==RC_OK && i<nRes; ++i,++pvs) {
				PIN *pin=(PIN*)pvs->pin; assert(pvs->type==VT_REF);
				const Value *props=pin->getValueByIndex(0); unsigned nProps=pin->getNumberOfProperties(); PID id=pin->getPID();
				if (!id.isPID()) cbuf[l++]='{';
				else {
					l+=sprintf(cbuf+l,"{\"id\":\"" _LX_FM "\"",id.pid);
					if (props!=NULL && nProps!=0) {cbuf[l]=','; cbuf[l+1]=' '; l+=2;}
				}
				if (l!=0 && !append(cbuf,l)) {rc=RC_NOMEM; break;}
				for (unsigned i=0; i<nProps; ++i,++props) if (props->property!=STORE_INVALID_URIID) {
					if ((rc=renderValue(*props,JR_PROP))!=RC_OK) break;
					if (i+1<nProps && !append(", ",2)) {rc=RC_NOMEM; break;}
				}
				if (rc==RC_OK && !append("}",1)) rc=RC_NOMEM;
				cbuf[0]=','; cbuf[1]=' '; l=2;
			}
			if (rc==RC_OK && nRes>1 && !append("]",1)) rc=RC_NOMEM;
		} else {
			cbuf[l++]='{';
			if (l!=0 && !append(cbuf,l) || (rc=renderValue(ret,JR_PROP))==RC_OK && !append("}",1)) rc=RC_NOMEM;
		}
		cnt++; cbuf[0]=','; cbuf[1]='\n'; l=2;
	}
	if (rc==RC_EOF) rc=RC_OK;
	return rc!=RC_OK||cnt==0||append("]",1)?rc:RC_NOMEM;
}

char *Session::convToJSON(const Value& v)
{
	SOutCtx out(this);
	if (v.type==VT_REF) {
		PID id=v.pin->getPID(); char cbuf[30];
		const Value *props=v.pin->getValueByIndex(0); unsigned nProps=v.pin->getNumberOfProperties();
		size_t l=sprintf(cbuf,"{\"id\":\"" _LX_FM "\"",id.pid); if (props!=NULL && nProps!=0) {cbuf[l]=','; cbuf[l+1]=' '; l+=2;}
		if (l!=0 && !out.append(cbuf,l)) return NULL;
		for (unsigned i=0; i<nProps; ++i,++props) if (props->property!=STORE_INVALID_URIID) {
			if (out.renderValue(*props,JR_PROP)!=RC_OK) return NULL;
			if (i+1<nProps && !out.append(", ",2)) return NULL;
		}
		if (!out.append("}",1)) return NULL;
	} else if (out.renderValue(v,JR_PROP)!=RC_OK) return NULL;
	return (char*)out;
}

//----------------------------------------------------------------------------------------------------------------

const TLx SInCtx::charLex[256] = 
{
	LX_EOE,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_SPACE,LX_SPACE,LX_NL,LX_ERR,LX_SPACE,LX_SPACE,LX_ERR,LX_ERR,											// 0x00 - 0x0F
	LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,LX_ERR,												// 0x10 - 0x1F
	LX_SPACE,LX_EXCL,LX_DQUOTE,LX_HASH,LX_DOLLAR,OP_MOD,OP_AND,LX_QUOTE,LX_LPR,LX_RPR,OP_MUL,OP_PLUS,LX_COMMA,OP_MINUS,LX_PERIOD,OP_DIV,							// 0x20 = 0x2F
	LX_DIGIT,LX_DIGIT,LX_DIGIT,LX_DIGIT,LX_DIGIT,LX_DIGIT,LX_DIGIT,LX_DIGIT,LX_DIGIT,LX_DIGIT,LX_COLON,LX_SEMI,LX_LT,OP_EQ,LX_GT,LX_QUEST,							// 0x30 - 0x3F
	LX_SELF,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,	// 0x40 - 0x4F
	LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_LETTER,LX_XLETTER,LX_LETTER,LX_LETTER,LX_LBR,LX_BSLASH,LX_RBR,OP_XOR,LX_LETTER,		// 0x50 - 0x5F
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

bool AfyKernel::testStrNum(const char *s,size_t l,Value& res)
{
	while (l!=0 && SInCtx::charLex[(byte)*s]==LX_SPACE) --l,++s;
	if (l==0) return false;
	TLx lx=SInCtx::charLex[(byte)*s];
	if (lx==LX_DIGIT && l>5) {
		// try date/interval
	}
	return (lx==LX_DIGIT || l>1 && (lx==OP_PLUS || lx==OP_MINUS)) && Session::strToNum(s,l,res)==RC_OK;
}

KWTrieImpl SInCtx::kwt(false);

void SInCtx::initKW()
{
	const static KWInit kwTab[] = 
	{
		{SELECT_S,				KW_SELECT},
		{FROM_S,				KW_FROM},
		{WHERE_S,				KW_WHERE},
		{ORDER_S,				KW_ORDER},
		{BY_S,					KW_BY},
		{GROUP_S,				KW_GROUP},
		{HAVING_S,				KW_HAVING},
		{JOIN_S,				KW_JOIN},
		{ON_S,					KW_ON},
		{LEFT_S,				KW_LEFT},
		{OUTER_S,				KW_OUTER},
		{RIGHT_S,				KW_RIGHT},
		{CROSS_S,				KW_CROSS},
		{INNER_S,				KW_INNER},
		{USING_S,				KW_USING},
		{BASE_S,				KW_BASE},
		{PREFIX_S,				KW_PREFIX},
		{UNION_S,				KW_UNION},
		{EXCEPT_S,				KW_EXCEPT},
		{INTERSECT_S,			KW_INTERSECT},
		{DISTINCT_S,			KW_DISTINCT},
		{VALUES_S,				KW_VALUES},
		{AS_S,					KW_AS},
		{NULLS_S,				KW_NULLS},
		{TRUE_S,				KW_TRUE},
		{FALSE_S,				KW_FALSE},
		{NULL_S,				KW_NULL},
		{ALL_S,					KW_ALL},
		{ANY_S,					KW_ANY},
		{SOME_S,				KW_SOME},
		{ASC_S,					KW_ASC},
		{DESC_S,				KW_DESC},
		{MATCH_S,				KW_MATCH},
		{CURRENT_TIMESTAMP_S,	KW_NOW},
		{CURRENT_USER_S,		KW_CUSER},
		{CURRENT_STORE_S,		KW_CSTORE},
		{TIMESTAMP_S,			KW_TIMESTAMP},
		{INTERVAL_S,			KW_INTERVAL},
		{WITH_S,				KW_WITH},
		{INSERT_S,				KW_INSERT},
		{DELETE_S,				KW_DELETE},
		{UPDATE_S,				KW_UPDATE},
		{CREATE_S,				KW_CREATE},
		{PURGE_S,				KW_PURGE},
		{UNDELETE_S,			KW_UNDELETE},
		{SET_S,					KW_SET},
		{ADD_S,					KW_ADD},
		{MOVE_S,				KW_MOVE},
		{RENAME_S,				KW_RENAME},
		{EDIT_S,				KW_EDIT},
		{START_S,				KW_START},
		{COMMIT_S,				KW_COMMIT},
		{ROLLBACK_S,			KW_ROLLBACK},
		{DROP_S,				KW_DROP},
		{CASE_S,				KW_CASE},
		{WHEN_S,				KW_WHEN},
		{THEN_S,				KW_THEN},
		{ELSE_S,				KW_ELSE},
		{END_S,					KW_END},
		{RULE_S,				KW_RULE},
	};
	static volatile long fInit=-1;
	while (fInit!=0) {
		if (!cas(&fInit,-1L,1L)) threadYield();
		else {
			kwt.addKeywords(kwTab,sizeof(kwTab)/sizeof(kwTab[0]));
			for (unsigned i=0; i<sizeof(opDscr)/sizeof(opDscr[0]); i++) {
				const OpDscr& od=opDscr[i];
				if (od.str!=NULL && byte(*od.str-'A')<=byte('Z'-'A')) kwt.add(od.str,KW_OP,(TLx)i);
			}
			fInit=0;
		}
	}
}

void KWTrieImpl::add(const char *str,unsigned kw,TLx lx)
{
	assert(byte(*str-'A')<=byte('Z'-'A') || byte(*str-'a')<=byte('z'-'a') || *str=='_'); byte ch;
	for (KWNode **ppn=&kwt[*str++-'A'],*node;;ppn=&node->nodes[ch-node->mch]) {
		if ((node=*ppn)==NULL) {
			*ppn=node=(KWNode*)::malloc(sizeof(KWNode)); if (node==NULL) throw RC_NOMEM;
			node->kw=~0u; node->lx=LX_ERR; node->mch=0; node->nch=0; node->nodes[0]=NULL; kwTabSize+=sizeof(KWNode);
		}
		if ((ch=*str++)=='\0') {assert(node->lx==LX_ERR); node->lx=lx; node->kw=kw; return;}
		if (!fCase && byte(ch-'a')<=byte('z'-'a')) ch^=0x20;
		if (node->nch==0) {node->mch=ch; node->nch=1;}
		else if (byte(ch-node->mch)>=node->nch) {
			byte delta=ch<node->mch?node->mch-ch:ch-(node->mch+node->nch-1);
			*ppn=node=(KWNode*)::realloc(node,sizeof(KWNode)+(node->nch+delta-1)*sizeof(KWNode*));
			if (node==NULL) throw RC_NOMEM; kwTabSize+=sizeof(KWNode*)*delta;
			if (ch>node->mch) memset(node->nodes+node->nch,0,delta*sizeof(KWNode*));
			else {
				memmove(node->nodes+delta,node->nodes,node->nch*sizeof(KWNode*));
				memset(node->nodes,0,delta*sizeof(KWNode*)); node->mch=ch;
			}
			node->nch+=delta;
		}
	}
}

RC KWTrieImpl::addKeywords(const KWInit *initTab,unsigned nInit)
{
	try {
		for (unsigned i=0; i<nInit; i++) add(initTab[i].kw,initTab[i].val);
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {return RC_INTERNAL;}
}

RC KWTrieImpl::createIt(ISession *ses,KWTrie::KWIt *&it)
{
	try {
		it=NULL; void *p=ses->malloc(sizeof(KWItImpl)); if (p==NULL) return RC_NOMEM;
		it=new(p) KWItImpl(*this,ses); return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {return RC_INTERNAL;}
}

RC KWTrieImpl::find(const char *str,size_t lstr,unsigned& code)
{
	try {
		code=~0u; if (str==NULL || lstr==0) return RC_INVPARAM;
		byte ch=*str++; if (!fCase && byte(ch-'a')<=byte('z'-'a')) ch^=0x20;
		if (byte(ch-'A')>byte('Z'-'A') && byte(ch-'a')>byte('z'-'a') && ch!='_') return RC_NOTFOUND;
		const KWNode *node=kwt[ch-'A'];
		for (size_t i=1; node!=NULL && i<lstr; i++) {
			ch=*str++; if (!fCase && byte(ch-'a')<=byte('z'-'a')) ch^=0x20;
			node=(ch=byte(ch-node->mch))<node->nch?node->nodes[ch]:NULL;
		}
		return node!=NULL && (code=node->kw)!=~0u?RC_OK:RC_NOTFOUND;
	} catch (RC rc) {return rc;} catch (...) {return RC_INTERNAL;}
}

RC KWTrie::createTrie(const KWInit *initTab,unsigned nInit,KWTrie *&ret,bool fCaseSensitive)
{
	try {
		ret=NULL;
		KWTrieImpl *kwt=new(&sharedAlloc) KWTrieImpl(fCaseSensitive); if (kwt==NULL) return RC_NOMEM;
		if (initTab!=NULL && nInit!=0) kwt->addKeywords(initTab,nInit); ret=kwt; return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {return RC_INTERNAL;}
}

RC KWTrieImpl::KWItImpl::next(unsigned ch,unsigned& kw)
{
	if (!trie.fCase && unsigned(ch-'a')<=unsigned('z'-'a')) ch^=0x20;
	if (node!=NULL) node=(ch-=node->mch)<unsigned(node->nch)?node->nodes[ch]:NULL;
	else if (unsigned(ch-'A')<=unsigned('Z'-'A') || unsigned(ch-'a')<=unsigned('z'-'a') || ch=='_') node=trie.kwt[ch-'A']; else return RC_NOTFOUND;
	kw=node!=NULL?node->kw:~0u; return node!=NULL?RC_OK:RC_NOTFOUND;
}

void KWTrieImpl::KWItImpl::destroy()
{
	ses->free(this);
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
	"missing '/'", "missing identificator", "missing '{'", "missing ':'", "missing '->'", "missing 'WHEN'", "missing 'AS'", "missing 'OF'"
};

SInCtx::~SInCtx()
{
	freeV(v);
	if (qNames!=NULL) ma->free(qNames);
	if (base!=NULL) ma->free(base);
}

TLx SInCtx::lex()
{
	TLx lx;
	if ((lx=nextLex)!=LX_ERR) {if (lx!=LX_EOE) nextLex=LX_ERR; return lx;}
	if ((v.flags&HEAP_TYPE_MASK)>=SES_HEAP) {freeV(v); v.setError();}
	const char *beg,*cbeg; size_t ls; unsigned wch; const byte *bptr; URIID uid;
	KWTrieImpl::KWNode *kwn; byte ch,ch2,*buf; RC rc; unsigned u; PID pid; bool fStd,fSrv;
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
		if (UTF8::iswdigit((wchar_t)wch)) {
			if (Session::strToNum(beg,end-beg,v,&ptr)==RC_OK) return LX_CON;
			throw SY_INVUTF8N;
		} else if (UTF8::iswspace((wchar_t)wch)) continue;
		throw SY_INVCHR;
	case LX_XLETTER:
		if (ptr<end && *ptr=='\'') {
			if ((buf=(byte*)ma->malloc((end-ptr)/2))==NULL) throw RC_NOMEM;
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
		beg=ptr-1; 
		if ((mode&SIM_SIMPLE_NAME)==0) for (kwn=kwt.kwt[(*beg&~0x20)-'A']; kwn!=NULL;) {
			if (ptr<end) {
				if ((ch=byte((*ptr&~0x20)-kwn->mch))<kwn->nch) {kwn=kwn->nodes[ch]; ptr++; continue;} 
				if ((lx=charLex[(byte)*ptr])>=LX_DIGIT) {ptr++; break;}
				if (lx==LX_UTF8) {
					bptr=(const byte *)ptr+1;
					if ((wch=UTF8::decode((byte)*ptr,bptr,end-ptr-1))==~0u) {errpos=ptr; throw SY_INVUTF8;}
					if (UTF8::iswalnum((wchar_t)wch)) {lmb+=unsigned((const char*)bptr-ptr-1); ptr=(const char*)bptr; break;}
				}
			}
			if (kwn->lx==LX_ERR) break;		// check ':' ???
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
		v.set(beg,uint32_t(ptr-beg));
		if (ptr<end && (mode&SIM_SIMPLE_NAME)==0) {
			if (*ptr==':') {
				if ((cbeg=++ptr)>=end || (lx=charLex[(byte)*ptr])<LX_UTF8 && lx!=LX_DQUOTE || lx==LX_DIGIT) return LX_PREFIX;
				if (lx==LX_UTF8) {
					bptr=(const byte *)ptr+1;
					if ((wch=UTF8::decode((byte)*ptr,bptr,end-ptr-1))==~0u) {errpos=ptr; throw SY_INVUTF8;}
					if (UTF8::iswalpha((wchar_t)wch)) {lmb+=unsigned((const char*)bptr-ptr-1); ptr=(const char*)bptr;} else return LX_PREFIX;
				}
				fStd=cbeg-beg==sizeof(AFFINITY_STD_QPREFIX)-1 && memcmp(beg,AFFINITY_STD_QPREFIX,sizeof(AFFINITY_STD_QPREFIX)-1)==0;
				fSrv=!fStd && cbeg-beg==sizeof(AFFINITY_SRV_QPREFIX)-1 && memcmp(beg,AFFINITY_SRV_QPREFIX,sizeof(AFFINITY_SRV_QPREFIX)-1)==0;
				mode|=SIM_SIMPLE_NAME; lx=lex(); mode&=~SIM_SIMPLE_NAME; if (lx!=LX_IDENT) throw SY_MISQNM;
				if ((fStd || fSrv) && (uid=NamedMgr::getBuiltinURIID(v.str,v.length,fSrv))!=STORE_INVALID_URIID) v.setURIID(uid);
				else mapURI(false,beg,cbeg-beg-1,fSrv);
			}
			if (ptr<end && *ptr=='#') {
				if (++ptr>=end || (lx=charLex[(byte)*ptr])<LX_UTF8 && lx!=LX_DQUOTE || lx==LX_DIGIT) throw SY_MISIDN;
				if (v.type==VT_STRING) mapURI(true); assert(v.type==VT_URIID); uid=v.uid; ElementID ei=0;
				mode|=SIM_SIMPLE_NAME; lx=lex(); mode&=~SIM_SIMPLE_NAME;
				if (lx==LX_IDENT) {
					assert(v.type==VT_STRING);
					if ((rc=ses->getStore()->classMgr->findEnumVal(ses,uid,v.str,v.length,ei))!=RC_OK) throw rc;
				} else if (lx==LX_CON && (v.type==VT_INT || v.type==VT_UINT)) ei=v.ui; else throw SY_MISIDN;
				v.setEnum(uid,ei); return LX_CON;
			}
		}
		return LX_IDENT;
	case LX_DIGIT:
		if ((rc=Session::strToNum(ptr-1,end-ptr+1,v,&ptr,(mode&SIM_INT_NUM)!=0))!=RC_OK) throw SY_INVNUM;
		if (ptr<end && (lx=charLex[(byte)*ptr])>=LX_UTF8) {
			if (lx!=LX_UTF8) throw SY_INVNUM; bptr=(const byte *)ptr+1;
			if ((wch=UTF8::decode((byte)*ptr,bptr,end-ptr-1))!=~0u && UTF8::iswalnum((wchar_t)wch)) throw SY_INVNUM;
		}
		return LX_CON;
	case LX_DQUOTE:
	case LX_QUOTE:
		for (cbeg=beg=ptr,ls=0,buf=NULL,ch=ptr[-1]; ptr<end; ) if ((ch2=*ptr++)==ch || ch2=='\\') {
			if (ptr>=end || *ptr!=ch2) {
				if (ch2=='\\') {
					if (ptr>=end) break;
					const static char escChr[8]="0nfrtvb",escChr2[8]="\0\n\f\r\t\v\b";
					char *pp=(char*)memchr(escChr,(byte)*ptr,sizeof(escChr)-1); if (pp==NULL) throw SY_INVCHR;
					ch2=escChr2[pp-escChr];
				} else {
					size_t ll=ptr-cbeg-1; 
					if (buf!=NULL) {if (ll!=0) memcpy(buf+ls,cbeg,ll); buf[ls+ll]='\0';}
					v.set(beg,uint32_t(ls+ll)); if (buf!=NULL) v.flags=mtype;
					if (lx==LX_DQUOTE) return LX_IDENT;
					return LX_CON;
				}
			}
			if (buf==NULL) {
				if ((buf=(byte*)ma->malloc(end-beg))==NULL) throw RC_NOMEM;
				memcpy(buf,beg,ls=ptr-beg); beg=(const char*)buf;
			} else if (cbeg<ptr) {memcpy(buf+ls,cbeg,ptr-cbeg); ls+=ptr-cbeg;}
			buf[ls-1]=ch2; cbeg=++ptr;
		}
		errpos=ptr; throw lx==LX_DQUOTE?SY_MISDQU:SY_MISQUO;
	case LX_SELF:
		if (ptr>=end) {v.op=0; return LX_SELF;}
		switch (charLex[(byte)*ptr]) {
		default: v.op=0; return LX_SELF;
		case LX_LCBR: ++ptr; return OP_PIN;
		case LX_LBR: ++ptr; return OP_RANGE;
		case LX_COLON:
			if (++ptr>=end || !isxdigit(*(byte*)ptr)) throw SY_MISCON;
			lx=LX_TPID; pid.ident=STORE_INVALID_IDENTITY; break;
		case LX_DIGIT: lx=LX_CON; pid.ident=STORE_OWNER; break;
		case LX_LETTER: case LX_XLETTER:
			for (beg=ptr; ptr<end; ptr++) if (charLex[(byte)*ptr]<LX_DIGIT) break;
			if (ptr>beg) {
				size_t l=ptr-beg;
				for (unsigned i=0; i<sizeof(atNames)/sizeof(atNames[0]); i++)
					if (atNames[i].l==l && cmpncase(beg,atNames[i].s,l)) {v.op=i; return LX_SELF;}
				ptr=beg;
			}
			if (!isxdigit(*(byte*)ptr)) {v.op=0; return LX_SELF;}
			lx=LX_CON; pid.ident=STORE_OWNER; break;
		}
		pid.pid=0; beg=ptr;
		do {ch=*ptr++; pid.pid=pid.pid<<4|(ch<='9'?ch-'0':ch<='F'?ch-'A'+10:ch-'a'+10);}
		while (ptr<end && isxdigit(*(byte*)ptr));
		if (ptr-beg>16) throw SY_TOOBIG;
		if (lx==LX_CON && ptr<end && *ptr=='!') {
			if ((++ptr,lex())!=LX_IDENT || v.type!=VT_STRING) throw SY_MISIDENT;
			mapIdentity(); assert(v.type==VT_IDENTITY); pid.ident=v.iid;
		}
		v.set(pid); return lx;
	case LX_DOLLAR:
		if (ptr<end) {
			switch (charLex[(byte)*ptr]) {
			default: break;
			case LX_LPR: return LX_EXPR;
			case LX_LCBR: ++ptr; return LX_STMT;
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
		return ptr<end&&*ptr=='|'?(++ptr,LX_CONCAT):(Lexem)OP_OR;
	case OP_MINUS:
		if (ptr<end) {
			if (*ptr=='>') {++ptr; return LX_ARROW;}
			if (*ptr=='-') {while (++ptr<end && *ptr!='\n'); continue;}
		}
		return OP_MINUS;
	case OP_DIV:
		if (ptr<end && *ptr=='*') {
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
		if (ptr>=end || (lx=charLex[(byte)*ptr])<LX_UTF8 && lx!=LX_DQUOTE || lx==LX_DIGIT) throw SY_MISIDN;
		if (lex()!=LX_IDENT) throw SY_MISIDN; mapURI(true); v.meta=META_PROP_OBJECT; return LX_IDENT;
	case LX_ERR: case LX_BSLASH:
		throw SY_INVCHR;
	}
	return LX_EOE;
}

void SInCtx::parseRegExp()
{
	const char *beg=ptr,*eend,*opts=NULL;
	for (TLx lx;;) {
		if (ptr>=end) throw SY_MISSLASH;
		switch (lx=charLex[(byte)*ptr++]) {
		case OP_DIV:
			if ((eend=ptr-1)==beg) throw SY_MISCON;
			if (ptr<end && (lx=charLex[(byte)*ptr])==LX_LETTER || lx==LX_XLETTER)
				{opts=ptr; while (++ptr<end && (lx=charLex[(byte)*ptr])==LX_LETTER || lx==LX_XLETTER);}
			RxCtx::parse(v,ma,(byte*)beg,eend-beg,0,opts,opts!=NULL?ptr-opts:0); return;
		case LX_BSLASH:
			if (ptr>=end) throw SY_MISSLASH;
			if (charLex[(byte)*ptr++]==LX_UTF8) {
				const byte *bptr=(const byte *)ptr; lmb+=UTF8::len(bptr[-1])-1;
				if (UTF8::decode(ptr[-1],bptr,end-ptr)==~0u) throw SY_INVUTF8;
				ptr=(const char*)bptr;
			}
			break;
		case LX_NL: throw SY_MISSLASH;
		case LX_ERR: throw SY_INVCHR;
		default: break;
		}
	}
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
		const char *const p0=ptr,*const end=ptr+l; const byte *bptr; unsigned wch;
		for (;;) {
			if (ptr==end) return;
			switch (charLex[(byte)*ptr++]) {
			default: break;
			case LX_UTF8:
				info.flags&=~UID_AID; bptr=(const byte *)ptr; wch=UTF8::decode(ptr[-1],bptr,end-ptr);
				if (wch==~0u || !UTF8::iswalpha((wchar_t)wch) && (ptr-1==p0 || !UTF8::iswdigit((wchar_t)wch))) break;
				ptr=(const char*)bptr; continue;
			case LX_XLETTER: case LX_LETTER: continue;
			case LX_DIGIT: if (ptr-1!=p0) continue; else break;
			}
			--ptr; info.flags=UID_IRI; break;
		}
		const char *suffix=NULL;
		do switch (charLex[(byte)*ptr++]) {
		default: info.flags&=UID_DQU; suffix=NULL; break;
		case LX_XLETTER: case LX_LETTER: case LX_DIGIT: break;
		case LX_DQUOTE: info.flags=UID_DQU; suffix=NULL; break;
		case LX_SELF: case LX_DOLLAR: case LX_COLON: case LX_PERIOD: case LX_EXCL:
		case OP_MINUS: case OP_PLUS: case OP_NOT: case OP_MOD: case OP_XOR: case OP_AND: case LX_ARROW:
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
			case LX_XLETTER: case LX_LETTER:
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
	Identity *ident=(Identity*)ses->getStore()->uriMgr->insert(str,v.length);
	if (ident==NULL) throw RC_NOMEM;
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
			if (((SimpleVar*)qv)->classes[0].objectID==v.uid)
				{if (classVar==INVALID_QVAR_ID) classVar=qv->id; else {classVar=INVALID_QVAR_ID; fMany=true;}}
			break;
		case VT_STRING:
			if ((uri=(URI*)ses->getStore()->uriMgr->ObjMgr::find(((SimpleVar*)qv)->classes[0].objectID))!=NULL) {
				const StrLen *cname=uri->getName();
				if (cname!=NULL && cname->len==v.length && !strncasecmp(cname->str,v.str,v.length))
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

void SInCtx::mapURI(bool fOID,const char *prefix,size_t lPrefix,bool fSrv)
{
	if (ses!=NULL && v.type==VT_STRING) {
		char *str=(char*)v.str; size_t ls=v.length; bool fCopy=v.flags!=mtype||str[v.length]!='\0'; bool fAddDel=false;
		if (fSrv) {prefix=AFFINITY_SERVICE_PREFIX; lPrefix=sizeof(AFFINITY_SERVICE_PREFIX)-1; fCopy=true;}
		else if (prefix!=NULL && lPrefix!=0) {
			const QName *pq=NULL; QName qn={prefix,lPrefix,NULL,0,false};
			if (lastQN>=nQNames || (pq=&qNames[lastQN])->lq!=lPrefix || memcmp(prefix,pq->qpref,lPrefix)!=0)
				pq=BIN<QName,const QName&,QNameCmp>::find(qn,qNames,nQNames);
			if (pq==NULL && (pq=BIN<QName,const QName&,QNameCmp>::find(qn,ses->getStore()->qNames,ses->getStore()->nQNames))==NULL) throw SY_UNKQPR;
			prefix=pq->str; lPrefix=pq->lstr; fAddDel=!pq->fDel; lastQN=unsigned((QName*)pq-qNames); fCopy=true;
		} else if (!Session::hasPrefix(str,v.length)) {
			fCopy=false;
			if (lBase!=0) {
				if (lBase+v.length+1>lBaseBuf && (base=(char*)
					ma->realloc(base,lBaseBuf=lBase+v.length+1))==NULL) throw RC_NOMEM;
				memcpy(base+lBase,str,v.length); str=base; ls+=lBase;
			} else if (fOID && (prefix=ses->getStore()->namedMgr->getStorePrefix(lPrefix))!=NULL)
				fCopy=true;
		}
		if (fCopy) {
			if (lBase+lPrefix+v.length+2>lBaseBuf && (base=(char*)
				ma->realloc(base,lBaseBuf=lBase+lPrefix+v.length+2))==NULL) throw RC_NOMEM;
			if (prefix!=NULL) {memcpy(base+lBase,prefix,lPrefix); if (fAddDel) base[lBase+lPrefix++]='/';}
			memcpy(base+lBase+lPrefix,str,v.length); str=base+lBase; ls+=lPrefix;
		}
		URI *uri=(URI*)ses->getStore()->uriMgr->insert(str,ls); if (uri==NULL) throw RC_NOMEM;
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
	Stack<OpF,NoFree<OpF> > ops; Stack<Value,FreeV> oprs; Value vv,*vals;
	Expr *exp; Stmt *stmt; QVarID var; RefVID *rv; Units unt; URIID uid; ops.push(OpF(LX_BOE,0));
	if ((pflags&PRS_PATH)!=0) {ops.push(OpF(OP_PATH,0)); vv.setVarRef(vars!=NULL?vars->var->id:0); oprs.push(vv);}
	TLx lx=lex(),lx2;
	for (;;lx=lex()) {
		const OpDscr *od=&opDscr[lx]; opf=0; assert(lx<sizeof(opDscr)/sizeof(opDscr[0]));
		switch (lx) {
		case LX_IDENT:
			if (est!=0) goto error_no_op;
			if ((v.meta&META_PROP_OBJECT)!=0) {
				assert(v.type==VT_URIID); if (ops.top().lx==OP_PATH) throw SY_SYNTAX;
				if ((nextLex=lex())!=LX_LPR && nextLex!=LX_PERIOD) throw SY_SYNTAX;
				uid=v.uid; v.setVarRef(0); v.setPropID(uid); v.refV.flags=VAR_NAMED;
				oprs.push(v); est=1; continue;
			}
			if (ops.top().lx!=OP_PATH) {
				vv=v; v.setError(); nextLex=lex(); Value save=v; v=vv;
				if ((nextLex==OP_EQ || nextLex==LX_LPR) && ((po=ops.top(1))->lx==LX_LCBR || po->lx==OP_PIN)) {
					mapURI(); uid=v.uid; uint8_t meta=0;
					if (nextLex==LX_LPR) {
						try {parseMeta(uid,meta);} catch (RC) {throw;}	// if not meta?
						if (lex()!=OP_EQ) throw SY_MISEQ;
					}
					vv.setURIID(uid); vv.meta=meta; oprs.push(vv);
					po->flag=VT_STRUCT; nextLex=LX_SPROP; est=1; continue;
				}
				if (nextLex==LX_PERIOD) {
					if ((mode&SIM_SELECT)!=0 && v.type==VT_STRING && (v.flags&HEAP_TYPE_MASK)<SES_HEAP) {
						for (i=0;;i++)
							if (i>=dnames) {Len_Str &ls=dnames.add(); ls.s=v.str; ls.l=v.length; break;}
							else if (dnames[i].l==v.length && !strncasecmp(dnames[i].s,v.str,v.length)) break;
						v.setVarRef((byte)i); v.refV.flags=0xFFFF;
					} else if (vars!=NULL && (var=findVar(v,vars,nVars))!=INVALID_QVAR_ID) {
						freeV(v); v.setVarRef(var);
					} else {
						if (v.type!=VT_URIID) mapURI(); uid=v.uid; v.setVarRef(vars!=NULL?vars->var->id:0,uid);
					}
				} else if (v.type==VT_STRING && nextLex==LX_KEYW && save.op==KW_FROM && ops.top().lx==OP_EXTRACT) {
					for (i=0; ;i++) 
						if (i>=sizeof(extractWhat)/sizeof(extractWhat[0])) throw SY_INVEXT;
						else if (extractWhat[i].l==v.length && cmpncase(v.str,extractWhat[i].s,v.length)) {freeV(v); v.set(i); break;}
				} else if (nVars<=1) {
					mapURI(); assert(v.type==VT_URIID); uid=v.uid; v.setVarRef(vars!=NULL?vars->var->id:0,uid);
				} else {
					freeV(save); throw SY_MISNAME;
				}
				oprs.push(v); v=save; est=1; continue;
			}
			if (v.type!=VT_URIID) mapURI();
		case LX_CON:
			if (est==0) {oprs.push(v); v.setError(); est=1;}
			else if (v.type!=VT_STRING || (vals=oprs.top(1))->type!=VT_STRING) goto error_no_op;
			else if (v.length!=0) {
				char *p;
				if ((vals->flags&HEAP_TYPE_MASK)==ma->getAType()) {
					p=(char*)ma->realloc((char*)vals->str,vals->length+v.length+1);
				} else {
					p=new(ma) char[vals->length+v.length+1]; if (p!=NULL) memcpy(p,vals->str,vals->length);
				}
				if (p==NULL) throw RC_NOMEM;
				memcpy(p+vals->length,v.str,v.length); p[vals->length+v.length]='\0';
				vals->str=p; vals->length+=v.length; setHT(*vals,ma->getAType());
			}
			continue;
		case LX_SELF: 
			if (est!=0) goto error_no_op; 
			if (ops.top().lx!=OP_PATH) {vv.setVarRef(v.op); vv.refV.flags=VAR_CTX;}
			else if (v.op==0) vv.setURIID(PROP_SPEC_SELF); else throw SY_SYNTAX;
			if ((nextLex=lex())==LX_LCBR || nextLex==LX_LBR || vv.type==VT_VARREF && vv.refV.refN==2 && nextLex!=LX_PERIOD) throw SY_SYNTAX;
			oprs.push(vv); est=1; continue;
		case LX_TPID:
			if (est!=0) goto error_no_op;
			if ((mode&SIM_INSERT)==0||(po=ops.top(1))->lx!=LX_BOE&&po->lx!=LX_SPROP&&po->lx!=LX_LCBR&&po->lx!=LX_COMMA) throw SY_SYNTAX;
			oprs.push(v); v.setError(); est=1; continue;
		case LX_PREFIX: throw SY_MISQN2;
		case LX_STMT:
			if (est!=0) goto error_no_op;
			stmt=parseStmt();
			if (lex()!=LX_RCBR) {if (stmt!=NULL) stmt->destroy(); throw SY_MISRCBR;}
			v.set(stmt); oprs.push(v); v.setError(); est=1; continue;
		case LX_PERIOD:
			if ((lx=lex())==LX_IDENT) {
				mapURI(); assert(v.type==VT_URIID);
				if (est==0) {oprs.push(v); est=1; v.setError(); continue;}
			} else if (est==0 || lx!=LX_LPR && lx!=LX_LCBR && lx!=OP_MUL) throw SY_MISPROP;
			nextLex=lx; od=&opDscr[lx=OP_PATH]; est=0; break;
		case LX_EXCL:
			if (est!=0) goto error_no_op;
			if (lex()!=LX_IDENT || v.type!=VT_STRING) throw SY_MISIDENT;
			mapIdentity(); assert(v.type==VT_IDENTITY); oprs.push(v); v.setError(); est=1; continue;
		case LX_COLON:
			if (est!=0) goto error_no_op;
			mode|=SIM_INT_NUM; lx=lex(); mode&=~SIM_INT_NUM;
			if (lx==LX_CON && v.type==VT_INT) {
				if (v.i<0) throw SY_INVNUM; if (v.i>255) throw SY_TOOBIG;
				vv.setParam((byte)v.i);
				if ((lx=lex())!=LX_LPR) nextLex=lx;
				else if (lex()!=LX_IDENT || v.type!=VT_STRING) throw SY_MISTYN;
				else {
					for (unsigned i=1; ; i++)
						if (i>=sizeof(typeName)/sizeof(typeName[0])) throw SY_INVTYN;
						else if (typeName[i].l==v.length && cmpncase(v.str,typeName[i].s,v.length)) {vv.refV.type=(byte)i; break;}
					if ((lx=lex())==LX_COMMA) {
						if ((lx=lex())==LX_RPR) throw SY_UNBRPR;
						if (lx==LX_KEYW && (v.op==KW_DESC||v.op==KW_ASC)) {
							if (v.op==KW_DESC) vv.refV.flags|=ORD_DESC; 
							if ((lx=lex())!=LX_RPR) {if (lx!=LX_COMMA) throw SY_MISCOM; if ((lx=lex())==LX_RPR) throw SY_UNBRPR;}
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
			} else {
				// add here named params, e.g. :afy:request
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
			}
			oprs.push(vv); est=1; continue;
		case LX_LBR:
			if (est!=0) {est=0; od=&opDscr[lx=(TLx)LX_FILTER];}
			else if ((po=ops.top(1))->lx==LX_LBR || po->lx==LX_COMMA&&po->flag==(VT_ARRAY|0x80)) throw SY_SYNTAX;
			else if (po->lx!=OP_ARRAY && (po->lx!=LX_COMMA || po->flag!=VT_ARRAY)) od=&opDscr[lx=(TLx)OP_ARRAY];
			break;
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
				if ((lx2=lex())==LX_IDENT) mapURI(true);
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
			else if (ops.top().lx==OP_PATH) {vv.setError(); oprs.push(vv); est=1; continue;}
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
				if (ops.top().lx==LX_REPEAT) {
					ops.pop(); if ((po=ops.top(1))->lx==LX_PATHQ) po=ops.top(2); else po->flag|=FILTER_LAST_OP;
					assert(po->lx==OP_PATH); po->flag|=STAR_PATH_OP; nextLex=LX_ERR; est=1; continue;
				}
			default: throw SY_MISCON; 
			}
			break;
		case OP_DIV:
			if (est!=0) {est=0; break;}
			if ((po=ops.top(1))->lx==OP_SIMILAR || po->lx==LX_BOE && (mode&SIM_DML_EXPR)!=0) {po->flag=0; parseRegExp();}
			else if (ptr>=end || charLex[(byte)*ptr]!=LX_DIGIT || lex()!=LX_CON || v.type!=VT_INT && v.type!=VT_UINT) throw SY_MISNUM;
			else {i=v.ui; v.setVarRef(vars!=NULL?vars->var->id:0,i); v.refV.flags=VAR_REXP;}
			oprs.push(v); v.setError(); est=1; continue;
		case OP_SIMILAR:
			if (lex()!=LX_IDENT || v.type!=VT_STRING || v.length!=sizeof(TO_S)-1 || !cmpncase(v.str,TO_S,sizeof(TO_S)-1)) throw SY_SYNTAX;
			if (est==0) throw SY_MISCON;
			opf=COMP_PATTERN_OP; est=0; break;
		case OP_AND:
			if (est!=0) est=0;
			else if ((nextLex=lex())==LX_CON && v.type==VT_REFID) od=&opDscr[lx=LX_REF];
			else throw SY_SYNTAX;
			break;
		case OP_LNOT:
			if (est==0) break;
			if ((lx=lex())!=LX_BETWEEN && lx!=OP_IN) goto error_no_op;
			od=&opDscr[lx]; est=0; opf=NOT_BOOLEAN_OP; break;
		case LX_LPR:
			if (est!=0) {od=&opDscr[lx=OP_CALL]; est=0;}
			else if ((mode&SIM_DML_EXPR)!=0 && ((po=ops.top(1))->lx==LX_BOE||po->lx==LX_SPROP||po->lx==LX_LCBR||po->lx==OP_PIN||po->lx==LX_COMMA&&po->flag!=0) && 
															(nextLex=lex())==LX_KEYW && (v.op==KW_INSERT||v.op==KW_SELECT||v.op==KW_CREATE||v.op==KW_UPDATE)) {
				stmt=parseStmt(); assert(stmt!=NULL); if ((lx=lex())!=LX_RPR) {stmt->destroy(); throw SY_MISRPR;}
				if ((po->lx==LX_LCBR||po->lx==LX_COMMA||po->lx==OP_PIN) && po->flag!=VT_STRUCT && po->flag!=VT_MAP && stmt->getOp()==STMT_QUERY && (stmt->getTop()==NULL ||
					(i=stmt->getTop()->stype)!=SEL_COUNT && i!=SEL_VALUE && i!=SEL_CONST && i!=SEL_DERIVED)) {stmt->destroy(); throw SY_SYNTAX;}	// SY_INVNST
				vv.set(stmt,1); oprs.push(vv); est=1; continue;
			}
			break;
		case LX_ARROW:
			if (est!=0) est=0; else throw SY_MISCON;
			if ((po=ops.top(1))->lx==LX_LCBR && po->flag==0 || po->lx==LX_COMMA && po->flag==VT_MAP) {po->flag=VT_MAP; od=&opDscr[lx=LX_COMMA];}
			break;
		case LX_KEYW:
			switch (v.op) {
			case KW_SELECT:
				if ((lx2=ops.top().lx)==LX_LPR || (size_t)lx2<sizeof(SInCtx::opDscr)/sizeof(SInCtx::opDscr[0]) && (SInCtx::opDscr[lx2].flags&_F)!=0) {
					unsigned npar=0,npar0=0; for (; lx2==LX_LPR; npar0=npar) lx2=ops.top(++npar+1)->lx;
					nextLex=lx; stmt=NULL; parseQuery(stmt,true,&npar); if (npar<npar0) ops.pop(npar0-npar);
					if ((nextLex=lex())!=LX_RPR) {if (stmt!=NULL) stmt->destroy(); throw SY_MISRPR;}
					v.set(stmt,1); oprs.push(v); v.setError(); est=1; continue;
				}
				if (est==0) throw SY_MISLPR;
				break;
			case KW_FROM:
				if (est!=0 && ((lx2=ops.top().lx)==OP_TRIM || lx2==OP_EXTRACT)) od=&opDscr[lx=LX_COMMA]; // || lx2==LX_SUBSTRING
				break;
			//case KW_FOR:
				// LX_SUBSTRING
			case KW_AS:
				if (ops.top().lx==OP_CAST) {
					mode|=SIM_SIMPLE_NAME;
					if ((lx2=lex())!=LX_IDENT || v.type!=VT_STRING) {
						if (lx2!=OP_IN) throw SY_MISTYN; unt=Un_in; goto units;
					} else for (i=1; ;i++)
						if (i>=sizeof(typeName)/sizeof(typeName[0])) {
							unt=getUnits(v.str,v.length); if (unt==Un_NDIM) throw SY_INVTYN;
						units:
							if ((lx2=lex())!=LX_PERIOD) {nextLex=lx2; i=unt<<8|VT_DOUBLE;}
							else if (lex()!=LX_IDENT || v.type!=VT_STRING || v.length!=1 || *v.str!='f') throw SY_SYNTAX;
							else i=unt<<8|VT_FLOAT; break;
						} else if (typeName[i].l==v.length && cmpncase(v.str,typeName[i].s,v.length)) break;
					mode&=~SIM_SIMPLE_NAME;
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
			case KW_CASE:
				if (est==0) {parseCase(); oprs.push(v); est=1; continue;}
				break;
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
				nextLex=lx; vals=oprs.pop(1); assert(vals!=NULL&&oprs.isEmpty()); 
				res=*vals; res.op=OP_SET; res.meta=0; return;
			case LX_COMMA:
				for (nvals=2; (op=ops.pop()).lx==LX_COMMA; nvals++);
				if (op.lx==LX_LPR || op.lx==LX_LCBR || op.lx==OP_PIN || op.lx==OP_ARRAY || op.lx==LX_LBR) {
			case LX_LCBR: case OP_PIN: case OP_ARRAY: case LX_LBR:
					if (op.lx==LX_LPR) {if (lx!=LX_RPR) throw SY_MISRPR;} 
					else if (op.lx==OP_ARRAY||op.lx==LX_LBR) {if (lx!=LX_RBR) throw SY_MISRPR;}
					else if (lx!=LX_RCBR) throw SY_MISRCBR;
					vals=oprs.pop(nvals); assert(vals!=NULL);
					if ((rc=op.lx==LX_LCBR&&op.flag==VT_MAP?ExprNode::normalizeMap(vals,nvals,vv,ma):
						op.lx==OP_ARRAY||op.lx==LX_LBR?ExprNode::normalizeArray(ses,vals,nvals,vv,ma):
						op.lx==LX_LCBR&&op.flag==VT_STRUCT||op.lx==OP_PIN?ExprNode::normalizeStruct(ses,vals,nvals,vv,ma,op.lx==OP_PIN):
						ExprNode::normalizeCollection(vals,nvals,vv,ma,ses!=NULL?ses->getStore():StoreCtx::get()))!=RC_OK) throw rc;
					oprs.push(vv); goto next_lx;
				} else if (op.lx==OP_IS_A || op.lx==OP_CALL) nvals++;
				else if (op.lx==LX_FILTER) {if (++nvals>3) throw SY_MANYARG; goto lx_filter;}
				else if (op.lx==LX_REPEAT) {nvals++; goto lx_repeat;}
				od2=&opDscr[op.lx];
				if (nvals==2 && (op.lx==OP_TRIM || op.lx==OP_EXTRACT)) {
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
					Value *pv=new(ma) Value[2]; if (pv==NULL) throw RC_NOMEM;
					pv[0]=vals[0]; pv[1]=vals[1]; // reconcile types
					vv.setRange(pv); vv.flags=mtype;
				} else if ((rc=ExprNode::node(vv,ses,OP_RANGE,2,vals,flags))!=RC_OK) throw rc;	// fcopy? ses???
				oprs.push(vv); od2=&opDscr[op.lx=OP_IN]; flags=(flags&COPY_VALUES_OP)|op.flag; break;
			case LX_FILTER:
			lx_filter:
				if (lx!=LX_RBR) throw SY_MISRBR;
				if (nvals==2) {
					vals=oprs.top(nvals); assert(vals!=NULL); 
					if (vals[1].type==VT_INT||vals[1].type==VT_UINT) {
						if (vals[0].type==VT_REFIDPROP) {((RefVID*)vals[0].refId)->eid=vals[1].ui; vals[0].type=VT_REFIDELT; oprs.pop(); goto next_lx;}
						if (vals[0].type==VT_VARREF && (vals[0].refV.flags&VAR_TYPE_MASK)==0 && (lx2=ops.top().lx)!=OP_PATH&&lx2!=LX_PATHQ) {vals[0].eid=vals[1].ui; oprs.pop(); goto next_lx;}
					}
				}
				ops.push(OpF(LX_PATHQ,byte(nvals-1))); goto next_lx;
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
					case VT_VARREF: case VT_CURRENT: case VT_EXPRTREE:
						// OP_RANGE
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
				case VT_VARREF: case VT_CURRENT: case VT_EXPRTREE:
					if (i!=~0u) {
						// OP_RANGE
					}
					break;
				default: throw SY_MISNUM;
				}
				ops.push(OpF(LX_PATHQ,0)); goto next_lx;
			case LX_PATHQ:
				nvals=3;
				if ((op=ops.pop()).lx==LX_PATHQ) {
					byte ff=0;
					if (op.flag==0) {vals=oprs.top(2); vv=vals[0]; vals[0]=vals[1]; vals[1]=vv; ff=FILTER_LAST_OP;} else if (op.flag==2) throw SY_SYNTAX;
					nvals++; op=ops.pop(); op.flag|=ff;
				} else if ((flags&3)==0) {
					vv.set((unsigned)STORE_COLLECTION_ID); oprs.push(vv); vals=oprs.top(2); vv=vals[0]; vals[0]=vals[1]; vals[1]=vv; nvals++; 
				} else if ((pflags&PRS_PATH)==0) {
					if ((flags&3)==2) {
						vals=oprs.top(4); assert(vals!=NULL);
						if (vals[0].type==VT_VARREF&&vals[0].length==0&&(vals[0].refV.flags&VAR_TYPE_MASK)==0&&vals[1].type==VT_URIID) {
							vals[0].refV.id=vals[1].uid; vals[0].length=1; vals[1]=vals[2]; vals[2]=vals[3]; oprs.pop(); op.lx=OP_ELEMENT; op.flag=0;
						} else {
							//a.b.c[1,3]	???
							throw SY_SYNTAX; //tmp
						}
					} else {
						vals=oprs.top(3); assert(vals!=NULL);
						if (lx!=OP_PATH && vals[1].type==VT_URIID) switch (vals[0].type) {
						case VT_VARREF:
							if ((i=vals[0].refV.flags&VAR_TYPE_MASK)!=0&&i!=VAR_CTX&&i!=VAR_NAMED||vals[0].length!=0) break;
							vals[0].refV.id=vals[1].uid; vals[0].length=1;
							if (vals[2].type==VT_INT||vals[2].type==VT_UINT) {vals[0].eid=vals[2].ui; oprs.pop(2); continue;}
							vals[1]=vals[2]; op.lx=OP_ELEMENT; oprs.pop(1); nvals=2; break;
						case VT_REFID:
							if ((rv=new(ma) RefVID)==NULL) throw RC_NOMEM;
							rv->id=vals[0].id; rv->pid=vals[1].uid; rv->eid=vals[2].ui; rv->vid=STORE_CURRENT_VERSION;
							vals->set(*rv); vals->fcalc=1; oprs.pop(2); continue;
						}
					}
				}
				assert(op.lx==OP_PATH||op.lx==OP_ELEMENT); flags=flags&COPY_VALUES_OP|op.flag; od2=&opDscr[op.lx]; break;
			case OP_PATH:
				if ((pflags&PRS_PATH)==0) {
					vals=oprs.top(2); assert(vals!=NULL);
					if (lx!=OP_PATH && vals[1].type==VT_URIID) switch (vals[0].type) {
					case VT_VARREF:
						if ((i=vals[0].refV.flags&VAR_TYPE_MASK)!=0&&i!=VAR_CTX&&i!=VAR_NAMED||vals[0].length!=0) break;
						vals[0].refV.id=vals[1].uid; vals[0].length=1; vals[0].eid=STORE_COLLECTION_ID; oprs.pop(); continue;
					case VT_REFID:
						if ((rv=new(ma) RefVID)==NULL) throw RC_NOMEM;
						rv->id=vals[0].id; rv->pid=vals[1].uid; rv->eid=STORE_COLLECTION_ID; rv->vid=STORE_CURRENT_VERSION;
						vals->set(*rv); vals->fcalc=1; oprs.pop(); continue;
					}
				}
				break;
			case LX_SPROP:
				if (lx!=LX_COMMA && lx!=LX_RCBR) throw SY_MISRCBR; vals=oprs.top(2); assert(vals->type==VT_URIID);
				vals[1].property=vals->uid; vals[1].meta=vals->meta; vals[0]=vals[1]; oprs.pop(); continue;
			case LX_REF:
				vals=oprs.top(1); 
				if (vals->type!=VT_REFID && vals->type!=VT_REFIDPROP && vals->type!=VT_REFIDELT) throw SY_SYNTAX;
				vals->fcalc=0; continue;
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
				if ((rc=ExprNode::forceExpr(*vals,ses))!=RC_OK || (rc=Expr::compile((ExprNode*)vals->exprt,exp,ma,false))!=RC_OK) throw rc;		// ses ???
				vals->exprt->destroy(); vals->set(exp); est=1; goto next_lx;
			}
			if ((rc=ExprNode::node(vv,ses,(ExprOp)op.lx,nvals,vals,flags))!=RC_OK) throw rc;	// fcopy? ses???
			oprs.pop(nvals); oprs.push(vv);
			if (od2->match!=LX_ERR) {assert((op.lx==OP_CALL||(od->flags&_C)!=0)&&est==1); goto next_lx;}
		}
		switch (lx) {
		default:
			if (isBool((ExprOp)lx)) {
				if ((lx2=lex())==LX_KEYW && (v.op==KW_ALL || v.op==KW_SOME || v.op==KW_ANY)) {
					opf|=v.op==KW_SOME?EXISTS_RIGHT_OP:FOR_ALL_RIGHT_OP; if ((lx2=lex())!=LX_LPR) throw SY_MISLPR;
				}
				nextLex=lx2;
			}
			ops.push(OpF(lx,opf)); break;
		case LX_COMMA:
			switch ((po=ops.top(1))->lx) {
			default: if (opDscr[po->lx].match==LX_ERR) throw SY_INVCOM; break;
			case LX_LPR: if (ops.top(2)->lx!=OP_IN) throw SY_SYNTAX; break;
			case OP_PIN: opf=VT_STRUCT; break;
			case OP_ARRAY: opf=VT_ARRAY; break;
			case LX_LBR: opf=VT_ARRAY|0x80; break;
			case LX_LCBR: if (po->flag==0) {opf=VT_COLLECTION; break;}
			case LX_COMMA: opf=po->flag; break;
			}
			ops.push(OpF(LX_COMMA,opf));
			if (opf==VT_STRUCT) {
				if (lex()!=LX_IDENT) throw SY_MISPROP;
				mapURI(); uid=v.uid; uint8_t meta=0;
				if ((lx=lex())==LX_LPR) {nextLex=LX_LPR; parseMeta(uid,meta); lx=lex();}
				if (lx!=OP_EQ) throw SY_MISEQ;
				vv.setURIID(uid); vv.meta=meta; oprs.push(vv); est=1; nextLex=LX_SPROP;
			}
			break;
		case LX_IS:
			flags=(pflags&PRS_COPYV)!=0?COPY_VALUES_OP:0;
			if ((lx2=lex())==LX_CON) {
				assert(v.type==VT_BOOL); if (opf!=0) v.b=!v.b; oprs.push(v); nvals=2; lx=OP_EQ;
			} else {
				assert(lx2==LX_KEYW && v.op==KW_NULL);
				flags|=opf^NOT_BOOLEAN_OP; nvals=1; lx=OP_EXISTS;
			}
			if ((rc=ExprNode::node(vv,ses,(ExprOp)lx,nvals,oprs.pop(nvals),flags))!=RC_OK) throw rc;	// fcopy? ses???
			oprs.push(vv); est=1; break;
		case OP_IS_A:
			lx2=lex(); assert(lx2==LX_IDENT&&v.type==VT_URIID||lx2==LX_CON&&v.type==VT_VARREF&&(v.refV.flags&VAR_TYPE_MASK)==VAR_PARAM);
			oprs.push(v); flags=opf|((pflags&PRS_COPYV)!=0?COPY_VALUES_OP:0);
			if ((lx2=lex())==LX_LPR) {ops.push(OpF(OP_IS_A,opf)); ops.push(OpF(LX_COMMA,0)); est=0;}
			else if ((rc=ExprNode::node(vv,ses,OP_IS_A,2,oprs.pop(2),flags))!=RC_OK) throw rc;	// fcopy? ses???
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
		case OP_IN: 
			if ((lx=lex())==LX_KEYW && (v.op==KW_ALL||v.op==KW_SOME||v.op==KW_ANY)) {opf|=v.op==KW_SOME?EXISTS_RIGHT_OP:FOR_ALL_RIGHT_OP;} else nextLex=lx;
			ops.push(OpF(OP_IN,opf)); opf=0; break;
		case LX_FILTER: case LX_REPEAT:
			if ((lx2=ops.top().lx)!=OP_PATH) {
				if (lx2==LX_PATHQ) {
					if (ops.top(2)->lx!=OP_PATH || lx==LX_FILTER&&ops.top().flag!=0 || lx==LX_REPEAT&&ops.top().flag==0) throw SY_SYNTAX;
				} else if (lx==LX_FILTER && oprs.top().type==VT_VARREF) {
					Value *pv=oprs.top(1); assert(pv->length!=0 && pv->refV.flags!=0xFFFF);
					pv->length=0; vv.setURIID(pv->refV.id); oprs.push(vv); ops.push(OpF(OP_PATH,0));
				} else throw SY_SYNTAX;
			}
			ops.push(OpF(lx,opf)); break;
		}
next_lx:;
	}

error_no_op:
	throw !ops.isEmpty() && ((lx=ops.top().lx)==LX_COMMA || (opDscr[lx].flags&_F)!=0 || lx==LX_LCBR || lx==LX_FILTER) ? SY_MISCOM : SY_MISBIN;
}

void SInCtx::parseCase()
{
	TLx lx; bool fSimple=false;
	if ((lx=lex())!=LX_KEYW || v.op!=KW_WHEN) {
		nextLex=lx; fSimple=true;
		//parse();
		lx=lex();
	}
	if (lx!=LX_KEYW || v.op!=KW_WHEN) throw SY_SYNTAX;	// SY_MISWHEN
	do {
		if (fSimple) {
			// parse comparison and rhs
		} else {
			// parse() condition
		}
		if (lex()!=LX_KEYW || v.op!=KW_THEN) throw SY_SYNTAX;	// SY_MISTHEN
		// parse result
		// add
	} while ((lx=lex())==LX_KEYW && v.op==KW_WHEN);
	if (lx==LX_KEYW && v.op==KW_ELSE) {
		//...
		lx=lex();
	}
	if (lx!=LX_KEYW || v.op!=KW_END) throw SY_SYNTAX;	// SY_MISEND
}

ExprNode *SInCtx::parse(bool fCopy)
{
	Value res; parse(res,NULL,0,fCopy?PRS_COPYV:0); RC rc;
	if ((rc=ExprNode::forceExpr(res,ses,fCopy))!=RC_OK) throw rc;
	return (ExprNode*)res.exprt;
}

struct OpD
{
	QUERY_SETOP op; DistinctType dt; 
	OpD() : op(QRY_UNION),dt(DT_DEFAULT) {} 
	OpD(QUERY_SETOP so,DistinctType d) : op(so),dt(d) {}
	bool operator==(const OpD& rhs) const {return op==rhs.op && dt==rhs.dt;}
};

QVarID SInCtx::parseQuery(Stmt*& res,bool fNested,unsigned *pnp)
{
	assert(ses!=NULL);
	if (res==NULL && (res=new(ma) Stmt(0,ses,STMT_QUERY,txi))==NULL) throw RC_NOMEM;	// md???
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
			if ((var=res->setOp(vars.pop(nv),nv,op.op))==INVALID_QVAR_ID) throw RC_NOMEM;
			if (op.dt!=DT_DEFAULT) res->setDistinct(var,op.dt);
			vars.push(var);
		}
		ops.push(OpD(type,dt)); res->top=NULL;
	}
}

QVarID SInCtx::parseSelect(Stmt* stmt,bool fMod)
{
	RC rc=RC_OK; TLx lx=lex(); DynArray<Value> outs(ma); assert(stmt!=NULL);
	QVarID var=INVALID_QVAR_ID; byte qvf=0; QVarRef qr; qr.var=NULL;
	if (!fMod) {
		for (;;lx=lex()) {
			byte ff=0;
			if (lx==LX_KEYW) {if (v.op==KW_ALL) ff=QVF_ALL; else if (v.op==KW_DISTINCT) ff=QVF_DISTINCT;}
			else if (lx==LX_IDENT && v.type==VT_STRING) {
				if (v.length==sizeof(RAW_S)-1 && cmpncase(v.str,RAW_S,v.length)) ff=QVF_RAW;
				else if (v.length==sizeof(FIRST_S)-1 && cmpncase(v.str,FIRST_S,v.length)) ff=QVF_FIRST;
			}
			if (ff==0) break; if ((qvf&ff)!=0) throw SY_SYNTAX; qvf|=ff;
		}
		if (lx==OP_MUL) {
			if ((lx=lex())==LX_KEYW && v.op==KW_AS) throw SY_SYNTAX;	// check next keyword -> 
		} else if (lx!=LX_KEYW) {
			nextLex=lx; const bool fSel=(mode&SIM_SELECT)!=0; mode|=SIM_SELECT;
			// multiple pins???
			do {
				Value w; parse(w); w.property=STORE_INVALID_URIID;										// cleanup?
				if ((w.flags&HEAP_TYPE_MASK)!=ma->getAType() && (rc=copyV(w,w,ma))!=RC_OK) break;
				if ((lx=lex())==LX_KEYW && v.op==KW_AS) {
					if (lex()!=LX_IDENT || v.type!=VT_STRING && v.type!=VT_URIID) throw SY_MISPROP;		// cleanup?
					mapURI(); w.property=v.uid;
				} else if (lx==LX_IDENT && (v.type==VT_STRING || v.type==VT_URIID)) {
					mapURI(); w.property=v.uid;
				} else nextLex=lx;
				if ((rc=outs+=w)!=RC_OK) break;
			} while ((lx=lex())==LX_COMMA);
			if (rc!=RC_OK) {for (unsigned i=0; i<outs; i++) freeV(outs[i]); throw rc;}
			if (!fSel) mode&=~SIM_SELECT;
		}
	}
	if (lx==LX_KEYW && v.op==KW_FROM) {
		if (fMod && stmt->top!=NULL) throw SY_SYNTAX;
		var=parseFrom(stmt); lx=lex(); assert(var!=INVALID_QVAR_ID);
	} else if (stmt->top!=NULL) var=stmt->top->id;
	else if ((var=stmt->addVariable())==INVALID_QVAR_ID) throw RC_NOMEM;
	if (qvf!=0) stmt->setVarFlags(var,qvf);
	while (lx==LX_KEYW && v.op==KW_WHERE) {
		qr.var=stmt->findVar(var); assert(qr.var!=NULL);
		ExprNode *pe=qr.var->type<=QRY_FULL_OUTER_JOIN?parseCondition(((JoinVar*)qr.var)->vars,((JoinVar*)qr.var)->nVars):parseCondition(&qr,1);
		if (pe!=NULL) {rc=splitWhere(stmt,qr.var,pe); pe->destroy(); if (rc!=RC_OK) throw rc;}
		lx=lex();
	}
	for (; lx==LX_KEYW && v.op==KW_MATCH; lx=lex()) {
		QVarID mvar=INVALID_QVAR_ID; PropertyID pid=STORE_INVALID_URIID; QVar *qv;
		if ((lx=lex())==LX_LPR) {
			//QVarRef qr; qr.var=stmt->findVar(var); assert(qr.var!=NULL);
			//ExprNode *pe=qr.var->type<=QRY_OUTERJOIN?parseCondition(((JoinVar*)qr.var)->vars,((JoinVar*)qr.var)->nVars):parseCondition(&qr,1);
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
			char *str=new(ses) char[v.length+1]; if (str==NULL) throw RC_NOMEM;
			memcpy(str,v.str,v.length); str[v.length]='\0';
			// flags, pin ids ???
			if ((rc=stmt->addConditionFT(mvar,str,0,pid!=STORE_INVALID_URIID?&pid:(PropertyID*)0,pid!=STORE_INVALID_URIID?1:0))!=RC_OK) throw rc;
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
	TLx lx; RC rc; int jkw=-1; assert(stmt!=NULL);
	for (QVarID var=INVALID_QVAR_ID,prev=INVALID_QVAR_ID;;) {
		bool fAs=true; if ((var=parseSource(stmt,lex(),&fAs))==INVALID_QVAR_ID) throw SY_INVVAR;
		if (fAs) {
			if ((lx=lex())!=LX_KEYW || v.op!=KW_AS) nextLex=lx;
			else {
				mode|=SIM_SIMPLE_NAME;
				if (lex()!=LX_IDENT || v.type!=VT_STRING) throw SY_MISNAME;
				mode&=~SIM_SIMPLE_NAME; size_t l=min(v.length,uint32_t(255));
				QVar *qv=stmt->findVar(var); assert(qv!=NULL && qv->name==NULL);
				if ((qv->name=new(ses) char[l+1])==NULL) throw RC_NOMEM;
				memcpy(qv->name,v.str,l); qv->name[l]=0;
			}
		}
		if (jkw!=-1) {
			assert(prev!=INVALID_QVAR_ID);
			ExprNode *pe=NULL; DynArray<PropertyID> props(ma);
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
			if ((unsigned)props!=0 && (rc=stmt->setJoinProperties(var,&props[0],props))!=RC_OK) throw rc;
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
	DynArray<OrderSeg> segs((MemAlloc*)ses); TLx lx; RC rc=RC_OK;
	do {
		Value w; parse(w,NULL,0,PRS_COPYV);	// vars from Transform?
		OrderSeg &sg=segs.add(); sg.expr=NULL; sg.pid=STORE_INVALID_URIID; sg.flags=0; sg.var=0; sg.lPrefix=0;
		if (w.type==VT_URIID) sg.pid=w.uid;
		else if (w.type==VT_VARREF && w.length==1) {sg.pid=w.refV.id; sg.var=w.refV.refN;}
		else if ((rc=ExprNode::forceExpr(w,ses,true))!=RC_OK) throw rc;
		else sg.expr=(ExprNode*)w.exprt;
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
		if (var==INVALID_QVAR_ID) rc=stmt->setOrder(&segs[0],segs);
		else {
			ExprNode *having=NULL;
			if (lx==LX_KEYW && v.op==KW_HAVING) {
				const bool fHave=(mode&SIM_HAVING)!=0; mode|=SIM_HAVING;
				having=parseCondition(NULL,0); lx=lex(); if (!fHave) mode&=~SIM_HAVING;
				Value w; w.set(having); rc=replaceGroupExprs(w,&segs[0],segs);
				assert(w.type==VT_EXPRTREE); having=(ExprNode*)w.exprt;
			}
			if (rc==RC_OK && os!=NULL) for (unsigned i=0; i<nO; i++) if ((rc=replaceGroupExprs(os[i],&segs[0],segs))!=RC_OK) break;		// check all converted
			if (rc==RC_OK) rc=stmt->setGroup(var,&segs[0],segs,having); if (having!=NULL) having->destroy();
		}
		for (unsigned i=0; i<segs; i++) if (segs[i].expr!=NULL) segs[i].expr->destroy();
	}
	if (rc!=RC_OK) throw rc;
	return lx;
}

QVarID SInCtx::parseSource(Stmt *stmt,TLx lx,bool *pF,bool fSelect)
{
	DynArray<SourceSpec> css(ma); RC rc=RC_OK; QVarID var=INVALID_QVAR_ID;
	Value w,path; path.setError(); const Value *pv; unsigned i;
	switch (lx) {
	case LX_IDENT:
		if (v.type==VT_URIID && (v.meta&META_PROP_OBJECT)!=0) {
			w.set(PIN::noPID);
			if ((rc=ses->getStore()->namedMgr->getNamedPID(v.uid,w.id))!=RC_OK ||
				(var=stmt->addVariable())!=INVALID_QVAR_ID && (rc=stmt->setExpr(var,w))!=RC_OK) throw rc;
		} else {
			parseClasses(css); 
			if ((var=stmt->addVariable(&css[0],css))==INVALID_QVAR_ID) break;		// NoCopy?
			for (i=css; i--!=0; ) if (css[i].params!=NULL) freeV((Value*)css[i].params,css[i].nParams,ses);
			if ((lx=lex())!=LX_LCBR) nextLex=lx;
			else {
				// parse + setExpr()
				DynArray<PID> pids(ma);
				do {
					if (lex()!=LX_CON || v.type!=VT_REFID) throw SY_MISPID;
					if ((rc=pids+=v.id)!=RC_OK) throw rc;
				} while ((lx=lex())==LX_COMMA);
				if (lx!=LX_RCBR) throw SY_MISRCBR;
				if ((rc=stmt->setPIDs(var,&pids[0],pids))!=RC_OK) throw rc;
			}
		}
		if ((lx=lex())!=LX_PERIOD) nextLex=lx;
		else {parse(path,NULL,0,PRS_PATH); if (path.type!=VT_EXPRTREE || ((ExprNode*)path.exprt)->op!=OP_PATH) throw SY_SYNTAX;}
		break;
	case OP_MUL:
		return stmt->addVariable();
	case LX_LPR:
		if (fSelect) {
			if ((lx=lex())!=LX_KEYW) {nextLex=lx; var=parseFrom(stmt); *pF=false;}
			else if (v.op!=KW_SELECT) throw SY_MISSEL;
			else {Stmt *subq=NULL; parseQuery(subq); var=stmt->addVariable(subq);}
			if (lex()!=LX_RPR) throw SY_MISRPR;
			return var;
		}
	default:
		nextLex=lx; parse(w);
		if (w.type==VT_EXPRTREE && ((ExprNode*)w.exprt)->op==OP_PATH) {
			for (pv=&((ExprNode*)w.exprt)->operands[0]; pv->type==VT_EXPRTREE && ((ExprNode*)pv->exprt)->op==OP_PATH; pv=&((ExprNode*)pv->exprt)->operands[0]);
			path=w; w=*pv; ((Value*)pv)->setVarRef(0);	// varID???????
		}
		switch (w.type) {
		default: freeV(w); throw RC_TYPE;
		case VT_REFIDPROP: case VT_REFIDELT: w.fcalc=1;
		case VT_REFID: case VT_STMT: case VT_EXPR: break;
		case VT_COLLECTION:
			// check PIDs, sort, remove duplicates
			break;
		case VT_EXPRTREE:
			// compile
			break;
		case VT_VARREF:
			if (!fSelect && path.isEmpty() && w.length==0 && (w.refV.flags&VAR_TYPE_MASK)==VAR_CTX) {
				stmt->pmode=w.refV.refN; *pF=true; assert(w.refV.refN<sizeof(atNames)/sizeof(atNames[0]));
			} else {
				//???
			}
			break;
		case VT_RANGE: break;
		}
		if ((fSelect || !*pF) && (var=stmt->addVariable())!=INVALID_QVAR_ID && (rc=stmt->setExpr(var,w))!=RC_OK) {freeV(w); throw rc;}
		break;
	}
	if (path.type==VT_EXPRTREE) {
		unsigned nS=0; PathSeg *ps,*ps0; assert(((ExprNode*)path.exprt)->op==OP_PATH);
		for (pv=&path; pv->type==VT_EXPRTREE && pv->exprt->getOp()==OP_PATH; pv=&pv->exprt->getOperand(0)) nS++;
		if ((ps0=ps=new(ses) PathSeg[nS])==NULL) {freeV(v); throw RC_NOMEM;}
		for (pv=&path,ps=&ps0[nS-1]; pv->type==VT_EXPRTREE && pv->exprt->getOp()==OP_PATH; pv=&pv->exprt->getOperand(0),--ps) {
			assert(ps>=ps0 && pv->type==VT_EXPRTREE && pv->exprt->getOp()==OP_PATH);
			ExprNode *et=(ExprNode*)pv->exprt; assert(ps!=ps0 || et->operands[0].type==VT_VARREF && et->operands[0].length==0);
			if (et->operands[1].type!=VT_URIID && et->operands[1].type!=VT_ANY && et->operands[1].type!=VT_COLLECTION) {freeV(v); ses->free(ps0); throw SY_SYNTAX;}
			if ((rc=et->toPathSeg(*ps,ses))!=RC_OK) {freeV(v); ses->free(ps0); throw rc;}
		}
		rc=stmt->setPath(var,ps0,nS,false); freeV(path); if (rc!=RC_OK) {ses->free(ps0); throw rc;}
	}
	return var;
}

void SInCtx::parseClasses(DynArray<SourceSpec> &css)
{
	unsigned i; RC rc=RC_OK; SynErr sy=SY_ALL; TLx lx;
	for (;;) {
		mapURI(true); assert(v.type==VT_URIID);
		SourceSpec &cs=css.add(); if (&cs==NULL) {rc=RC_NOMEM; break;}
		cs.objectID=v.uid; cs.params=NULL; cs.nParams=0;
		if ((lx=lex())==LX_LPR) {
			unsigned xP=0;
			do {
				Value w;
				switch (lx=lex()) {
				case OP_MUL: w.setError(); w.fcalc=1; break;
				case LX_KEYW: if (v.op==KW_NULL) {w.setError(); break;}
				default: nextLex=lx; parse(w); break;
				}
				if (w.type==VT_EXPRTREE || w.type==VT_VARREF && (w.refV.flags&VAR_TYPE_MASK)!=VAR_PARAM) {sy=SY_MISCON; break;}
				if (cs.nParams>=xP) {
					cs.params=(Value*)ses->realloc((Value*)cs.params,(xP+=xP==0?16:xP)*sizeof(Value));
					if (cs.params==NULL) {rc=RC_NOMEM; break;}
				}
				if (w.flags==SES_HEAP) {*(Value*)&cs.params[cs.nParams]=w; cs.nParams++;}
				else {if ((rc=copyV(w,*(Value*)&cs.params[cs.nParams],ses))==RC_OK) cs.nParams++; freeV(w);}
			} while (rc==RC_OK && (lx=lex())==LX_COMMA);
			if (rc!=RC_OK || sy!=SY_ALL) break; if (lx!=LX_RPR) {sy=SY_MISRPR; break;}
			lx=lex();
		} else if (lx==LX_LBR) {
			// condition like in path expr seg
		}
		if (lx!=OP_AND) {nextLex=lx; break;}
		if ((lx=lex())!=LX_IDENT) {sy=SY_UNKVAR; break;}
	}
	if (rc!=RC_OK || sy!=SY_ALL) {
		for (i=css; i--!=0; ) if (css[i].params!=NULL) freeV((Value*)css[i].params,css[i].nParams,ses);
		if (sy!=SY_ALL) throw sy; if (rc!=RC_OK) throw rc;
	}
}

ExprNode *SInCtx::parseCondition(const QVarRef *vars,unsigned nVars)
{
	Value w; parse(w,vars,nVars); RC rc;
	if (w.type!=VT_EXPRTREE) {
		if (w.type!=VT_BOOL) {freeV(w); throw SY_MISBOOL;}
		if (w.b) return NULL;
		if ((rc=ExprNode::forceExpr(w,ses))!=RC_OK) throw rc;
	} else if (!isBool(w.exprt->getOp())) {freeV(w); throw SY_MISBOOL;}
	return (ExprNode*)w.exprt;
}

RC SInCtx::splitWhere(Stmt *stmt,QVar *qv,ExprNode *pe)
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
		return (rc=splitWhere(stmt,qv,(ExprNode*)pe->operands[0].exprt))==RC_OK?
			splitWhere(stmt,qv,(ExprNode*)pe->operands[1].exprt):rc;
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
		ExprNode *pe=(ExprNode*)vv.exprt;
		for (unsigned i=0; i<pe->nops; i++) {
			Value *pv=&pe->operands[i];
			switch (pv->type) {
			default: break;
			case VT_EXPRTREE:
				if (((ExprNode*)pv->exprt)->vrefs!=NO_VREFS) resolveVars(qv,*pv); break;
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
			if ((ls->l&0x80000000)!=0) {
				vv.refV.refN=byte(ls->l&~0x80000000);
				if (par!=NULL) {
					ExprNode *pe=(ExprNode*)par->exprt; assert(par->type==VT_EXPRTREE && pe->op==OP_PATH);
					if (pe->nops==2 || pe->nops==3 && (pe->operands[2].type==VT_INT||pe->operands[2].type==VT_UINT)) {
						par->type=VT_VARREF; par->refV=vv.refV; par->eid=pe->nops==2?STORE_COLLECTION_ID:pe->operands[2].ui;
						par->length=(par->refV.id=pe->operands[1].uid)==PROP_SPEC_SELF?0:1; par->fcalc=1; pe->destroy();
					}
				}
			} else if (qv->var->type<QRY_UNION||par!=NULL&&((ExprNode*)par->exprt)->operands[1].uid==PROP_SPEC_SELF) throw SY_MISNAME;
			else if (vv.length==0) {vv.refV.refN=0; vv.refV.id=(URIID)ls->l; vv.length=1;} 
			else throw RC_INTERNAL;			
		} else if (vv.length==1) {
			Value save=v; v.setURIID(vv.refV.id); QVarID var=findVar(v,qv,1); v=save;
			if (var==INVALID_QVAR_ID) {if (qv->var->type<QRY_UNION||par!=NULL&&((ExprNode*)par->exprt)->operands[1].uid==PROP_SPEC_SELF) throw SY_MISNAME;}
			else if (par==NULL) {vv.length=0; vv.refV.refN=var;}
			else {
				ExprNode *pe=(ExprNode*)par->exprt; par->setVarRef(var,pe->operands[1].uid==PROP_SPEC_SELF?STORE_INVALID_URIID:pe->operands[1].uid); 
				if (pe->nops>=3) par->eid=pe->operands[2].ui; pe->destroy();
			}
		} else if (qv->var->type<QRY_UNION) throw SY_MISNAME;
	}
}

RC SInCtx::replaceGroupExprs(Value& v,const OrderSeg *segs,unsigned nSegs)
{
	ExprNode *et; RC rc;
	if (v.type==VT_VARREF) {
		if (v.length==1 && (v.refV.flags&VAR_TYPE_MASK)==0) for (unsigned i=0; i<nSegs && i<256; i++) 
			if (segs[i].expr==NULL && segs[i].var==v.refV.refN && segs[i].pid==v.refV.id) {
				if (v.property==PROP_SPEC_ANY) v.property=v.refV.id;
				v.length=0; v.refV.refN=byte(i); v.refV.type=VT_ANY; v.refV.flags|=VAR_GROUP; return RC_OK;
			}
		// not found -> ???
	} else if (v.type==VT_EXPRTREE && ((et=(ExprNode*)v.exprt)->op>=OP_ALL || (SInCtx::opDscr[et->op].flags&_A)==0)) {
		if (!isBool(et->op)) for (unsigned i=0; i<nSegs && i<256; i++) if (segs[i].expr!=NULL && *(ExprNode*)segs[i].expr==*et) 
			{et->destroy(); v.setVarRef(byte(i)); v.refV.flags=VAR_GROUP; return RC_OK;}
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

void SInCtx::parseMeta(PropertyID pid,uint8_t& meta)
{
	meta=0; TLx lx;
	if ((lx=lex())!=LX_LPR) nextLex=lx;
	else {
		do {
			if (lex()!=LX_IDENT || v.type!=VT_STRING) throw SY_MISCON;
			for (unsigned i=0; ;i++)
				if (i>=sizeof(metaProps)/sizeof(metaProps[0])) throw SY_SYNTAX;
				else if (v.length==metaProps[i].ls.l && cmpncase(v.str,metaProps[i].ls.s,v.length) && (metaProps[i].prop==PROP_SPEC_ANY || metaProps[i].prop==pid)) 
					{meta|=(uint8_t)metaProps[i].flg; break;}
		} while ((lx=lex())==LX_COMMA);
		if (lx!=LX_RPR) throw SY_MISRPR;
	}
}

inline bool checkProp(URIID uid,bool fNew)
{
	if (uid<MAX_BUILTIN_URIID) switch (uid) {
	case PROP_SPEC_PINID: case PROP_SPEC_INDEX_INFO: case PROP_SPEC_PROPERTIES: case PROP_SPEC_COUNT: case PROP_SPEC_PARENT: return false;
	case PROP_SPEC_CREATED: case PROP_SPEC_CREATEDBY: case PROP_SPEC_OBJID: case PROP_SPEC_STAMP: if (!fNew) return false; break;
	default: break;
	}
	return true;
}

void SInCtx::parseInsert(Stmt *&stmt,DynArray<Value,10>& with)
{
	SynErr sy=SY_ALL; RC rc=RC_OK; Value w; bool fFreeTIDs=false; TLx lx;
	DynArray<Value,40> vals(ma); DynArray<IntoClass> classes(ma); DynArray<PINDscr,100> *pins=NULL;
	unsigned msave=mode&(SIM_DML_EXPR|SIM_INSERT); mode|=SIM_DML_EXPR|SIM_INSERT;
	try {
		if ((stmt=new(ma) Stmt(0,ma,STMT_INSERT,txi))==NULL) throw RC_NOMEM;
		if (with!=0) {uint32_t nWith=0; Value *pw=with.get(nWith); stmt->with=Values(pw,nWith,true);}
		if ((lx=lex())==LX_IDENT && msave!=0 && v.type==VT_STRING && v.length==sizeof(PART_S)-1 && cmpncase(v.str,PART_S,sizeof(PART_S)-1))
			{stmt->mode|=MODE_PART; lx=lex();}
		if (lx==LX_IDENT && v.type==VT_STRING && v.length==sizeof(INTO_S)-1 && cmpncase(v.str,INTO_S,sizeof(INTO_S)-1)) {
			do {
				if (lex()!=LX_IDENT) throw SY_MISIDN; mapURI(true); IntoClass ic={v.uid,0};
				if ((lx=lex())==LX_IDENT && v.type==VT_STRING) {
					if (v.length==sizeof(UNIQUE_S)-1 && cmpncase(v.str,UNIQUE_S,sizeof(UNIQUE_S)-1)) {ic.flags|=IC_UNIQUE; lx=lex();}
					else if (v.length==sizeof(IDEMPOTENT_S)-1 && cmpncase(v.str,IDEMPOTENT_S,sizeof(IDEMPOTENT_S)-1)) {ic.flags|=IC_IDEMPOTENT; lx=lex();}
				}
				if ((rc=classes+=ic)!=RC_OK) throw rc;
			} while (lx==LX_COMMA);
			stmt->into=classes.get(stmt->nInto);
		}
		if (lx==LX_IDENT && v.type==VT_STRING && v.length==sizeof(OPTIONS_S)-1 && cmpncase(v.str,OPTIONS_S,sizeof(OPTIONS_S)-1)) {
			if (lex()!=LX_LPR) throw SY_MISLPR;
			do {
				if (lex()!=LX_IDENT || v.type!=VT_STRING) throw SY_MISCON;
				for (unsigned i=0; ;i++)
					if (i>=sizeof(PINoptions)/sizeof(PINoptions[0])) throw SY_SYNTAX;
					else if (v.length==PINoptions[i].ls.l && cmpncase(v.str,PINoptions[i].ls.s,v.length)) {stmt->pmode|=PINoptions[i].flg; break;}
			} while ((lx=lex())==LX_COMMA);
			if (lx!=LX_RPR) throw SY_MISRPR;
			lx=lex();
		}
		if (lx==LX_TPID) {
			if (tids==NULL) {
				if ((tids=new(alloca(sizeof(DynOArrayBuf<uint64_t,uint64_t>))) DynOArrayBuf<uint64_t,uint64_t>((MemAlloc*)ses))==NULL) throw RC_NOMEM;
				fFreeTIDs=true;
			}
			stmt->tpid=v.id.pid; if ((rc=tids->add(v.id.pid))!=RC_OK) throw rc==RC_FALSE?RC_ALREADYEXISTS:rc;
			lx=lex();
		}
		if (lx==LX_KEYW && v.op==KW_SELECT) nextLex=lx;
		else if (lx==LX_LPR) {
			DynArray<Value,40> props(ma);
			do {
				if (lex()!=LX_IDENT) throw SY_MISPROP;
				mapURI(); assert(v.type==VT_URIID);
				if (!checkProp(v.uid,true)) throw SY_INVPROP;
				w.setError(v.uid); parseMeta(v.uid,w.meta);
				if ((rc=props+=w)!=RC_OK) throw rc;
			} while ((lx=lex())==LX_COMMA);
			if (lx!=LX_RPR) throw SY_MISRPR;
			if (lex()!=LX_KEYW || v.op!=KW_VALUES) throw SY_SYNTAX;
			for (;;) {
				if (lex()!=LX_LPR) throw SY_MISLPR; unsigned i=0;
				do {
					if (i>=props) {sy=SY_MANYARG; break;}
					if ((lx=lex())!=LX_KEYW || v.op!=KW_NULL) {
						nextLex=lx; parse(w); w.setPropID(props[i].property); w.setMeta(props[i].meta);
						if ((w.flags&HEAP_TYPE_MASK)==ma->getAType() || (rc=copyV(w,w,ma))==RC_OK) rc=vals+=w;
					}
					i++;
				} while (rc==RC_OK && (lx=lex())==LX_COMMA);
				if (rc!=RC_OK || sy!=SY_ALL) break;
				if (lx!=LX_RPR) {sy=SY_MISRPR; break;} else if (i<props) {sy=SY_FEWARG; break;}
				if ((lx=lex())!=LX_COMMA) {nextLex=lx; if (pins==NULL) break;}
				if (pins==NULL && (pins=new(ma) DynArray<PINDscr,100>(ma))==NULL) {rc=RC_NOMEM; break;}
				uint32_t nVals=0; const Value *pv=vals.get(nVals); stmt->mode|=MODE_SAME_PROPS;
				if ((rc=ses->normalize(pv,nVals,0,ses!=NULL?ses->getStore()->getPrefix():0,ma,true))!=RC_OK) break;
				PINDscr vv(pv,nVals,STORE_INVALID_PID,true); if ((rc=(*pins)+=vv)!=RC_OK || lx!=LX_COMMA) break;
			}
		} else {
			uint64_t tpid=STORE_INVALID_PID; const bool fMany=lx==OP_PIN;
			if (fMany) {tpid=stmt->tpid; stmt->tpid=STORE_INVALID_PID; lx=lex();}
			for (;;lx=lex()) {
				for (bool fSelf=false;;lx=lex()) {
					if (lx==LX_SELF) {
						if (!fSelf) fSelf=true; else {rc=RC_ALREADYEXISTS; break;}
						w.setVarRef(0); w.refV.flags=VAR_CTX; w.setPropID(PROP_SPEC_SELF);
						if ((rc=vals+=w)!=RC_OK) break;
					} else if (lx==LX_IDENT) {
						mapURI(); assert(v.type==VT_URIID); URIID uid=v.uid; uint8_t meta=0;
						if (!checkProp(uid,true)) {sy=SY_INVPROP; break;}
						parseMeta(uid,meta); if (lex()!=OP_EQ) {sy=SY_MISEQ; break;}
						if ((lx=lex())!=LX_KEYW || v.op!=KW_NULL) {
							nextLex=lx; parse(w);
							if ((w.flags&HEAP_TYPE_MASK)!=ma->getAType() && (rc=copyV(w,w,ma))!=RC_OK) break;
							w.setPropID(uid); w.meta|=meta; if ((rc=vals+=w)!=RC_OK) break;
						}
					} else if (vals!=0) {sy=SY_MISPROP; break;}
					else if (lx==LX_SEMI) {nextLex=lx; break;}
					else if (lx!=LX_EOE) {sy=SY_MISPROP; break;}
					else break;
					if ((lx=lex())!=LX_COMMA) {nextLex=lx; break;}
				}
				if (!fMany || rc!=RC_OK || sy!=SY_ALL) break;
				if ((lx=lex())!=LX_RCBR) {sy=SY_MISRCBR; break;}
				if (pins==NULL && (pins=new(ma) DynArray<PINDscr,100>(ma))==NULL) {rc=RC_NOMEM; break;}
				uint32_t nVals=0; const Value *pv=vals.get(nVals);
				if ((rc=ses->normalize(pv,nVals,0,ses!=NULL?ses->getStore()->getPrefix():0,ma,true))!=RC_OK) break;
				PINDscr vv(pv,nVals,tpid,true); if ((rc=(*pins)+=vv)!=RC_OK) break;
				if ((lx=lex())!=LX_COMMA) {nextLex=lx; break;}
				if ((lx=lex())==LX_TPID) {
					if (tids==NULL) {
						if ((tids=new(alloca(sizeof(DynOArrayBuf<uint64_t,uint64_t>))) DynOArrayBuf<uint64_t,uint64_t>((MemAlloc*)ses))==NULL) throw RC_NOMEM;
						fFreeTIDs=true;
					}
					if ((rc=tids->add(v.id.pid))!=RC_OK) {if (rc==RC_FALSE) rc=RC_ALREADYEXISTS; break;}
					tpid=v.id.pid; lx=lex();
				}
				if (lx!=OP_PIN) {sy=SY_SYNTAX; break;}
			}
		}
		mode=mode&~(SIM_DML_EXPR|SIM_INSERT)|msave;
		if (rc!=RC_OK) throw rc; if (sy!=SY_ALL) throw sy;
		if (pins!=NULL) {
			uint32_t nPins=0; PINDscr *pds=pins->get(nPins);
			if (pds!=NULL && (rc=stmt->setValuesNoCopy(pds,nPins))!=RC_OK) {
				for (unsigned i=0; i<nPins; i++) freeV(*(Value**)&pds[i].vals,pds[i].nValues,ma);
				ma->free(pds); throw rc;
			}
		} else {
			uint32_t nVals=0; const Value *pv=vals.get(nVals);
			if (nVals!=0) {
				if ((rc=ses->normalize(pv,nVals,0,ses!=NULL?ses->getStore()->getPrefix():0,ma,true))!=RC_OK ||
					(rc=stmt->setValuesNoCopy((Value*)pv,nVals))!=RC_OK) {freeV((Value*)pv,nVals,ma); throw rc;}
			}
		}
		if ((lx=lex())!=LX_KEYW || v.op!=KW_SELECT) nextLex=lx;
		else if (pins!=NULL) throw SY_SYNTAX; else parseSelect(stmt,false);
		if (fFreeTIDs && tids!=NULL) {tids->~DynOArrayBuf<uint64_t,uint64_t>(); tids=NULL;}
	} catch (...) {
		if (stmt!=NULL) stmt->destroy();
		for (unsigned i=0; i<vals; i++) freeV(vals[i]);
		for (unsigned i=0; i<with; i++) freeV(with[i]);
		if (pins!=NULL) {
			for (unsigned i=0; i<*pins; i++) freeV(*(Value**)&(*pins)[i].vals,(*pins)[i].nValues,ma);
			pins->~DynArray<PINDscr,100>(); ma->free(pins);
		}
		if (fFreeTIDs && tids!=NULL) {tids->~DynOArrayBuf<uint64_t,uint64_t>(); tids=NULL;}
		mode=mode&~(SIM_DML_EXPR|SIM_INSERT)|msave;
		throw;
	}
}

void SInCtx::parseUpdate(Stmt *&stmt,DynArray<Value,10>& with)
{
	uint8_t op; bool fOnePin=false; unsigned md=0,msave=mode&(SIM_DML_EXPR|SIM_INSERT),nBI;
	SynErr sy=SY_ALL; RC rc=RC_OK; Value w; ElementID eid,eid2; QVarID var; DynArray<Value,40> vals(ma); TLx lx;

	if ((lx=lex())==LX_IDENT && v.type==VT_STRING && v.length==sizeof(RAW_S)-1 && cmpncase(v.str,RAW_S,sizeof(RAW_S)-1)) {md=MODE_RAW; lx=lex();}
	if (lx==LX_KEYW) throw SY_SYNTAX;
	if ((stmt=new(ma) Stmt(md,ma,STMT_UPDATE,txi))==NULL) throw RC_NOMEM;
	if (with!=0) {uint32_t nWith=0; Value *pw=with.get(nWith); stmt->with=Values(pw,nWith,true);}
	try {var=parseSource(stmt,lx,&fOnePin,false);} catch (...) {stmt->destroy(); throw;} 
	mode|=SIM_DML_EXPR; lx=lex(); nBI=0;
	while (rc==RC_OK && sy==SY_ALL && lx==LX_KEYW) {
		switch (v.op) {
		case KW_SET: op=OP_SET; break;
		case KW_ADD: op=OP_ADD; break;
		case KW_DELETE: op=OP_DELETE; break;
		case KW_RENAME: op=OP_RENAME; break;
		case KW_EDIT: op=OP_EDIT; break;
		case KW_MOVE: op=OP_MOVE; break;
		default: op=0xFF; break;
		}
		if (op==0xFF) break;
		do {
			bool fSubstr=false;
			if ((lx=lex())!=LX_IDENT) {
				if (op!=OP_EDIT || lx!=OP_SUBSTR) {sy=SY_MISPROP; break;}
				if (lex()!=LX_LPR) {sy=SY_MISLPR; break;}
				if (lex()!=LX_IDENT) {sy=SY_MISPROP; break;}
				fSubstr=true;
			}
			mapURI(); assert(v.type==VT_URIID);
			URIID uid=v.uid; if (!checkProp(v.uid,false)) {sy=SY_INVPROP; break;}
			if (uid<=MAX_BUILTIN_URIID) nBI++;
			w.setError(); eid=STORE_COLLECTION_ID; byte eop=op; byte meta=0;
			if ((lx=lex())==LX_LBR) {
				if (op==OP_RENAME) {sy=SY_SYNTAX; break;}
				if ((lx=lex())==LX_IDENT && v.type==VT_STRING && v.length==sizeof(BEFORE_S)-1 && cmpncase(v.str,BEFORE_S,sizeof(BEFORE_S)-1))
					{if (op==OP_ADD) {eop=OP_ADD_BEFORE; lx=lex();} else {sy=SY_SYNTAX; break;}}
				if (!parseEID(lx,eid)) {sy=SY_MISCON; break;}
				if (lex()!=LX_RBR) {sy=SY_MISRBR; break;} lx=lex();
			}
			if (lx==LX_LPR && (op==OP_SET || op==OP_ADD || op==OP_ADD_BEFORE)) {nextLex=lx; parseMeta(uid,meta); lx=lex();}
			switch (op) {
			case OP_EDIT:
				if (fSubstr) {
					StrEdit se; se.bstr=NULL; se.shift=0; se.length=0;
					if (lx!=LX_COMMA) sy=SY_MISCOM;
					else if ((lx=lex())==OP_MUL) {
						se.shift=~0ULL; if (lex()!=LX_RPR) sy=SY_MISRPR;
					} else if (lx==LX_CON && isInteger((ValueType)v.type)) {
						switch (v.type) {
						case VT_INT: se.shift=v.i; break;
						case VT_UINT: se.shift=v.ui; break;
						case VT_INT64: case VT_UINT64: se.shift=v.ui64; break;
						}
						if ((lx=lex())==LX_RPR) {
							nextLex=lx; se.length=(uint32_t)se.shift; se.shift=0;
						} else if (lx==LX_COMMA) {
							if (lex()!=LX_CON || v.type!=VT_INT && v.type!=VT_UINT) sy=SY_MISCON;
							else {se.length=v.ui; if (lex()!=LX_RPR) sy=SY_MISRPR;}
						} else sy=SY_MISCOM;
					} else sy=SY_MISCON;
					if (sy==SY_ALL) {
						if (lex()!=OP_EQ) sy=SY_MISEQ;
						else if (lex()!=LX_CON || !isString((ValueType)v.type)) sy=SY_MISCON;
						else if ((v.flags&HEAP_TYPE_MASK)==ma->getAType() || (rc=copyV(v,v,ma))==RC_OK)
							{se.str=v.str; w.edit=se; w.type=v.type; w.length=v.length; w.flags=v.flags; setHT(v);}
					}
				} else if (lx==OP_EQ) {
					if (lex()!=LX_LCBR) {sy=SY_MISLCBR; break;}
					uint64_t bits=0,mask=0;
					do {
						bool fNot=false;
						if ((lx=lex())==OP_LNOT) {fNot=true; lx=lex();}
						if (lx==LX_CON) {
							if (isInteger((ValueType)v.type)) {
								mask|=v.type==VT_INT||v.type==VT_UINT?v.ui:v.ui64;
								if (!fNot) bits|=v.type==VT_INT||v.type==VT_UINT?v.ui:v.ui64;
							} else if (v.type==VT_ENUM) {
								mask|=v.enu.eltid; if (!fNot) bits|=v.enu.eltid;
							} else {rc=RC_TYPE; break;}
						} else if (uid==PROP_SPEC_SELF && lx==LX_IDENT && v.type==VT_STRING) {
							for (unsigned k=0; ;k++)
								if (k>=sizeof(PINoptions)/sizeof(PINoptions[0])) {sy=SY_SYNTAX; break;}
								else if (v.length==PINoptions[k].ls.l && cmpncase(v.str,PINoptions[k].ls.s,v.length))
									{mask|=PINoptions[k].flg; if (!fNot) bits|=PINoptions[k].flg; break;}
							if (sy!=SY_ALL) break;
						} else {sy=SY_MISCON; break;}
					} while ((lx=lex())==LX_COMMA);
					if (sy==SY_ALL && rc==RC_OK) {
						if (lx!=LX_RCBR) sy=SY_MISRCBR;
						else if ((mask&0xFFFFFFFF00000000ULL)!=0) w.setEdit(bits,mask);
						else w.setEdit((uint32_t)bits,(uint32_t)mask);
					}
				} else sy=SY_MISEQ;
				break;
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
				if (lx!=OP_EQ && ((eop=lx)<OP_FIRST_EXPR || lx>OP_RSHIFT && (eop=OP_CONCAT,lx)!=LX_CONCAT && (eop=OP_RSHIFT,meta|=META_PROP_UNSIGNED,lx)!=LX_URSHIFT || lex()!=OP_EQ)) {sy=SY_MISEQ; break;}
				if ((lx=lex())==LX_KEYW && v.op==KW_NULL && eop==OP_SET) eop=OP_DELETE;
				else {
					nextLex=lx; parse(w); if ((w.flags&HEAP_TYPE_MASK)!=ma->getAType()) rc=copyV(w,w,ma);
					if (w.type==VT_EXPRTREE) {
						ExprNode *et=(ExprNode*)w.exprt;
						if (eop==OP_SET && et->op>=OP_FIRST_EXPR && et->op<=OP_CONCAT && et->nops==2) {
							const Value& lhs=et->operands[0];
							if (lhs.type==VT_VARREF && lhs.length==1 && lhs.refV.refN==0 && lhs.refV.id==uid)
								{eop=et->op; w=et->operands[1]; setHT(et->operands[1]); et->destroy();}
						}
					}
					if (eop==OP_CONCAT && isString((ValueType)w.type)) {eop=OP_EDIT; w.edit.shift=~0ULL; w.edit.length=0;}
				}
				break;
			}
			w.property=uid; w.op=eop; w.eid=eid; w.meta|=meta;
		} while (sy==SY_ALL && rc==RC_OK && (rc=vals+=w)==RC_OK && (lx=lex())==LX_COMMA);
	}
	mode=mode&~SIM_DML_EXPR|msave;
	if (rc==RC_OK && sy==SY_ALL) {
		nextLex=lx; uint32_t nVals=0; Value *pv=vals.get(nVals); TIMESTAMP ts=0ULL;
		if (pv!=NULL) {
			if (nBI!=0) for (uint32_t i=0; i<nVals; i++) if (pv[i].property<=MAX_BUILTIN_URIID) {
				if (pv[i].op<=OP_ADD_BEFORE && (rc=ses->checkBuiltinProp(pv[i],ts))!=RC_OK) {freeV(pv,nVals,ma); stmt->destroy(); throw rc;}
				if (--nBI==0) break;
			}
			if (nVals==0 || (rc=stmt->setValuesNoCopy(pv,nVals))!=RC_OK) freeV(pv,nVals,ma);
		}
		if (rc==RC_OK && lx==LX_KEYW && (v.op==KW_WHERE || v.op==KW_MATCH)) try {
			if (fOnePin && stmt->nVars==0) {
				if ((var=stmt->addVariable())==INVALID_QVAR_ID) throw RC_NOMEM;
				w.setVarRef((byte)stmt->pmode); w.refV.flags=VAR_CTX; if ((rc=stmt->setExpr(var,w))!=RC_OK) throw rc;
			}
			parseSelect(stmt,true);
		} catch (...) {stmt->destroy(); throw;}
	} else {
		for (unsigned i=0; i<vals; i++) freeV(vals[i]);
		for (unsigned i=0; i<with; i++) freeV(with[i]);
	}
}

void SInCtx::parseDelete(Stmt *&stmt,KW kw,DynArray<Value,10>& with)
{
	unsigned md=kw==KW_PURGE?MODE_PURGE:0; TLx lx; RC rc; QVarID var;
	if ((stmt=new(ma) Stmt(md,ma,kw==KW_UNDELETE?STMT_UNDELETE:STMT_DELETE,txi))==NULL) throw RC_NOMEM;
	try {
		if (with!=0) {uint32_t nWith=0; Value *pw=with.get(nWith); stmt->with=Values(pw,nWith,true);}
		bool fLCBR=false;
		if ((lx=lex())==LX_LCBR) {fLCBR=true; if ((lx=lex())!=LX_CON || v.type!=VT_REFID) {stmt->destroy(); throw SY_MISPID;}}
		if (lx==LX_CON && v.type==VT_REFID) {
			DynArray<PID,10> pins(ma); if ((rc=pins+=v.id)!=RC_OK) throw rc;
			while ((lx=lex())==LX_COMMA) {
				if (lex()!=LX_CON || v.type!=VT_REFID) throw SY_MISPID;
				if ((rc=pins+=v.id)!=RC_OK) throw rc;
			}
			if (!fLCBR) nextLex=lx; else if (lx!=LX_RCBR) throw SY_MISRCBR;
			if ((var=stmt->addVariable())==INVALID_QVAR_ID) throw RC_NOMEM;
			if ((rc=stmt->setPIDs(var,&pins[0],pins))!=RC_OK) throw rc;
		} else if (lx==LX_KEYW && (v.op==KW_FROM || v.op==KW_WHERE || v.op==KW_MATCH)) {
			nextLex=lx; try {parseSelect(stmt,true);} catch (...) {stmt->destroy(); throw;}
		} else throw SY_SYNTAX;
	} catch (...) {
		stmt->destroy(); stmt=NULL; throw;
	}
}

void SInCtx::parseCreate(Stmt *&stmt,DynArray<Value,10>& with)
{
	SynErr sy=SY_ALL; RC rc; Value w; DynArray<Value,40> vals(ma); TLx lx;
	unsigned md=0,msave=mode&(SIM_DML_EXPR|SIM_INSERT); mode|=SIM_DML_EXPR|SIM_INSERT;
	try {
		uint8_t flags=0; CRT_STMT sty; Expr *exp;
		if ((lx=lex())!=LX_IDENT || v.type!=VT_STRING) throw SY_SYNTAX;
		if (v.length==sizeof(OPTIONAL_S)-1 && cmpncase(v.str,OPTIONAL_S,sizeof(OPTIONAL_S)-1)) {
			if (lex()!=LX_IDENT || v.type!=VT_STRING || v.length!=sizeof(LOADER_S)-1 || !cmpncase(v.str,LOADER_S,sizeof(LOADER_S)-1)) throw SY_SYNTAX;
			sty=CRT_LOADER; flags=META_PROP_OPTIONAL;
		} else for (unsigned i=0; ;i++) {
			if (i>=sizeof(createStmt)/sizeof(createStmt[0])) throw SY_SYNTAX;
			if (v.length==createStmt[i].ls.l && cmpncase(v.str,createStmt[i].ls.s,v.length))
				{sty=createStmt[i].sty; flags=createStmt[i].flags; break;}
		}
		if (sty==CRT_FSM) {parseFSM(stmt); mode=mode&~(SIM_DML_EXPR|SIM_INSERT)|msave; return;}
		if (sty==CRT_EVENT) if ((lx=lex())==LX_IDENT && v.type==VT_STRING && v.length==sizeof(HANDLER_S)-1 &&
			cmpncase(v.str,HANDLER_S,sizeof(HANDLER_S)-1)) sty=CRT_HANDLER; else nextLex=lx;
		if (lex()!=LX_IDENT) throw SY_MISIDN; mapURI(true); v.setPropID(PROP_SPEC_OBJID); v.setOp(OP_SET); vals+=v;
		switch (sty) {
		default: break;
		case CRT_CLASS: case CRT_EVENT: md|=MODE_CLASS; break;
		case CRT_TIMER:
			if (lex()!=LX_CON) throw SY_MISCON;
			else if (v.type!=VT_INTERVAL) throw RC_TYPE;
			else if (v.i64==0) throw RC_INVPARAM;
			else {v.setPropID(PROP_SPEC_INTERVAL); v.setOp(OP_SET); vals+=v;}
			break;
		case CRT_LISTENER:
			if ((lx=lex())!=LX_KEYW || v.op!=KW_ON) nextLex=lx;
			else {
				parse(w); if ((w.flags&HEAP_TYPE_MASK)!=ma->getAType() && (rc=copyV(w,w,ma))!=RC_OK) break;
				w.setPropID(PROP_SPEC_ADDRESS); if ((rc=vals+=w)!=RC_OK) throw rc;
			}
			break;
		}
		if (lex()!=LX_KEYW || v.op!=KW_AS) throw SY_MISAS;
		switch (sty) {
		case CRT_CLASS: case CRT_EVENT:
			parseQuery(stmt); w.set(stmt); w.setPropID(PROP_SPEC_PREDICATE); break;
		case CRT_LISTENER:
			if ((lx=lex())==LX_CON) {
				if (v.type==VT_STRING) mapURI(); else if (v.type!=VT_URIID) throw RC_TYPE; w=v;
			} else if (lx==LX_LCBR) {
				nextLex=lx; parse(w); if (w.type!=VT_COLLECTION && w.type!=VT_STRUCT) {freeV(w); throw RC_TYPE;}
				if ((w.flags&HEAP_TYPE_MASK)!=ma->getAType() && (rc=copyV(w,w,ma))!=RC_OK) throw rc;
			} else throw SY_MISLCBR;
			w.setPropID(PROP_SPEC_LISTEN); break;
		case CRT_LOADER:
			if ((lx=lex())!=LX_CON) throw SY_MISCON; if (v.type!=VT_STRING) throw RC_TYPE; w=v;
			if ((w.flags&HEAP_TYPE_MASK)!=ma->getAType() && (rc=copyV(w,w,ma))!=RC_OK) throw rc;
			w.setPropID(PROP_SPEC_LOAD); break;
		case CRT_PACKAGE:
			//???
			break;
		case CRT_CONDITION:
			parse(w);
			if (w.type!=VT_EXPRTREE || !isBool(w.exprt->getOp())) {freeV(w); throw SY_MISBOOL;}
			rc=Expr::compile((ExprNode*)w.exprt,exp,ma,true); freeV(w); if (rc!=RC_OK) throw rc;
			w.set(exp); w.setPropID(PROP_SPEC_CONDITION); break;
		case CRT_ACTION:
		case CRT_TIMER:
		case CRT_ENUM:
			if ((nextLex=lex())!=LX_LCBR) {
				if (sty==CRT_ENUM || (stmt=parseStmt())==NULL) throw SY_SYNTAX;
				if (stmt->op==STMT_QUERY) throw RC_INVOP; w.set(stmt);
			} else {
				parse(w); if ((w.flags&HEAP_TYPE_MASK)!=ma->getAType() && (rc=copyV(w,w,ma))!=RC_OK) throw rc;
				if (sty==CRT_ENUM) {
					if (w.type!=VT_COLLECTION) throw RC_TYPE;
					for (unsigned i=0; i<w.length; i++) if (w.varray[i].type!=VT_STRING) throw RC_TYPE;
				} else if (w.type==VT_COLLECTION && w.length==1) {Value ww=w.varray[0]; ma->free((void*)w.varray); w=ww;}
			}
			w.setPropID(sty==CRT_ENUM?PROP_SPEC_ENUM:PROP_SPEC_ACTION); break;
		case CRT_HANDLER:
			parseEvents(w); w.setPropID(PROP_SPEC_EVENT); break;
		default: assert(0);
		}
		setHT(w,mtype); w.setMeta(flags); w.setOp(OP_SET); if ((rc=vals+=w)!=RC_OK) throw rc; stmt=NULL;
		if ((lx=lex())!=LX_KEYW || v.op!=KW_SET) nextLex=lx;
		else for (;;) {
			if ((lx=lex())!=LX_IDENT) {if (vals!=0) sy=SY_MISPROP; else if (lx!=LX_EOE) {if (lx==LX_SEMI) nextLex=lx; else sy=SY_MISPROP;} break;}
			mapURI(); assert(v.type==VT_URIID); URIID uid=v.uid; uint8_t meta=0; parseMeta(uid,meta);
			if (lex()!=OP_EQ) {sy=SY_MISEQ; break;}
			if ((lx=lex())!=LX_KEYW || v.op!=KW_NULL) {
				nextLex=lx; parse(w);
				if ((w.flags&HEAP_TYPE_MASK)!=ma->getAType() && (rc=copyV(w,w,ma))!=RC_OK) break;
				w.setPropID(uid); w.setMeta(meta); if ((rc=vals+=w)!=RC_OK) break;
			}
			if ((lx=lex())!=LX_COMMA) {nextLex=lx; break;}
		}
		mode=mode&~(SIM_DML_EXPR|SIM_INSERT)|msave; if (rc!=RC_OK) throw rc; if (sy!=SY_ALL) throw sy;
		uint32_t nVals=0; const Value *pv=vals.get(nVals);
		if (nVals!=0 && (rc=ses->normalize(pv,nVals,0,ses!=NULL?ses->getStore()->getPrefix():0,ma,true))!=RC_OK) {freeV((Value*)pv,nVals,ma); throw rc;}
		if ((rc=(stmt=new(ma) Stmt(md,ma,STMT_INSERT,txi))==NULL?RC_NOMEM:stmt->setValuesNoCopy((Value*)pv,nVals))==RC_OK && with!=0)
			{uint32_t nWith=0; Value *pw=with.get(nWith); stmt->with=Values(pw,nWith,true);}
		if (rc!=RC_OK) {freeV((Value*)pv,nVals,ma); throw rc;}
	} catch (...) {
		if (stmt!=NULL) {stmt->destroy(); stmt=NULL;}
		for (unsigned i=0; i<vals; i++) freeV(vals[i]);
		for (unsigned i=0; i<with; i++) freeV(with[i]);
		mode=mode&~(SIM_DML_EXPR|SIM_INSERT)|msave;
		throw;
	}
}

void SInCtx::parseEvents(Value &res,FSMTable *ft,unsigned *id)
{
	TLx lx; RC rc; Value w,vals[4]; unsigned nvals=0; DynArray<Value> events(ma);
	if (lex()!=LX_LCBR) throw SY_MISLCBR;
	try {
		do {
			EventSpec evs; evs.evType=EvNone; evs.nQEvents=0xFF; evs.flags=0; bool fOID=false;
			if ((lx=lex())==LX_KEYW && v.op==KW_WHEN) switch (lex()) {
			default: throw SY_SYNTAX;
			case LX_IDENT:
				evs.evType=EvProp;
				if (v.type==VT_STRING) {
					if (v.length==sizeof(INSTANCE_S)-1 && cmpncase(v.str,INSTANCE_S,sizeof(INSTANCE_S)-1)) {
						if (lex()!=LX_IDENT || v.type!=VT_STRING || v.length!=sizeof(OF_S)-1 || !cmpncase(v.str,OF_S,sizeof(OF_S)-1)) throw SY_MISOF;
						evs.evType=EvClass; fOID=true;
					} else if (v.length==sizeof(LISTENER_S)-1 && cmpncase(v.str,LISTENER_S,sizeof(LISTENER_S)-1)) {evs.evType=EvExt; fOID=true;}
					else if (v.length==sizeof(TOPIC_S)-1 && cmpncase(v.str,TOPIC_S,sizeof(TOPIC_S)-1)) evs.evType=EvSubs;		// topic ID ? special URI prefix ?
					if (evs.evType!=EvProp && lex()!=LX_IDENT) throw SY_MISIDN;
				}
				mapURI(fOID); evs.uid=v.uid; if ((lx=lex())==LX_IS) lx=lex();
				for (EvType evMask;;) {
					if (lx!=LX_IDENT || v.type!=VT_STRING) throw SY_SYNTAX;
					if (v.length==sizeof(INSERTED_S)-1 && cmpncase(v.str,INSERTED_S,sizeof(INSERTED_S)-1)) evMask=EvNew;
					else if (v.length==sizeof(UPDATED_S)-1 && cmpncase(v.str,UPDATED_S,sizeof(UPDATED_S)-1)) evMask=EvUpdate;
					else if (v.length==sizeof(DELETED_S)-1 && cmpncase(v.str,DELETED_S,sizeof(DELETED_S)-1)) evMask=EvLeave;
					else throw SY_SYNTAX;
					if ((evs.evType&evMask)==0) evs.evType=EvType(evs.evType|evMask); else throw SY_SYNTAX;
					if ((lx=lex())!=OP_OR) {nextLex=lx; break;}
				}
				break;
			case LX_CON:
				if (v.type!=VT_INTERVAL) throw RC_TYPE; if (v.i64<=0) throw RC_INVPARAM;
				evs.evType=EvTime; evs.itv=uint32_t(v.i64>=0x80000000LL?v.i64/1000|0x80000000:v.i64);
				if (lex()!=LX_IDENT || v.type!=VT_STRING || v.length!=sizeof(EXPIRES_S)-1 || !cmpncase(v.str,EXPIRES_S,sizeof(EXPIRES_S)-1)) throw SY_SYNTAX;
				break;
			} else if (ft==NULL) throw SY_MISWHEN;
			vals[0].setU64(evs.u64); vals[0].setPropID(PROP_SPEC_EVENT); nvals++;
			if (evs.evType!=EvNone && (lx=lex())!=OP_LAND) nextLex=lx;
			else {
				parse(w); Expr *exp;
				if (w.type!=VT_EXPRTREE || !isBool(w.exprt->getOp())) {freeV(w); throw SY_MISBOOL;}
				rc=Expr::compile((ExprNode*)w.exprt,exp,ma,true); freeV(w); if (rc!=RC_OK) throw rc;
				w.set(exp); w.setPropID(PROP_SPEC_CONDITION); vals[nvals++]=w;
			}
			if (lex()!=LX_ARROW) throw SY_MISARROW;
			mode|=SIM_SIMPLE_NAME; lx=lex(); mode&=~SIM_SIMPLE_NAME;
			if (lx==LX_LCBR || lx==LX_STMT) {
				nextLex=lx; parse(w);
				if (w.type!=VT_STMT) {
					if (w.type!=VT_COLLECTION) {freeV(w); throw RC_TYPE;}
					assert(!w.isNav() && (w.flags&HEAP_TYPE_MASK)==ma->getAType());
					for (unsigned i=0; i<w.length; i++) if (w.varray[i].type!=VT_STMT) {freeV(w); throw RC_TYPE;}
				}
				w.fcalc=0; w.setPropID(PROP_SPEC_ACTION); vals[nvals++]=w; lx=lex();
			} else {nextLex=lx; lx=LX_ARROW;}
			if (lx==LX_ARROW) {
				mode|=SIM_SIMPLE_NAME; lx=lex(); mode&=~SIM_SIMPLE_NAME;
				if (lx!=LX_IDENT || v.type!=VT_STRING) {evs.flags|=EV_EXIT; vals[0].ui64=evs.u64;}
				else if (ft==NULL) throw SY_SYNTAX;
				else {
					FSMTable::Find fft(*ft,StrLen(v.str,v.length)); FSMState *st=fft.find();
					if (st==NULL) {
						assert(id!=NULL);
						if ((st=new(ma) FSMState(v.str,v.length,(++*id)|0x80000000,ma))==NULL) throw RC_NOMEM;
						ft->insert(st,fft.getIdx());
					}
					PID id={st->id&~0x80000000,STORE_INVALID_IDENTITY};
					vals[nvals].set(id); vals[nvals++].setPropID(PROP_SPEC_REF); lx=lex();
				}
			}
			Value *pv=new(ma) Value[nvals]; assert(nvals!=0); if (pv==NULL) throw RC_NOMEM;
			memcpy(pv,vals,nvals*sizeof(Value)); vals[0].setStruct(pv,nvals); setHT(vals[0],ma->getAType());
			if ((rc=events+=vals[0])==RC_OK) nvals=0; else {nvals=1; throw rc;}
		} while (lx==LX_COMMA);
		if (lx!=LX_RCBR) throw SY_MISRCBR;
		uint32_t nev=0; Value *pv=events.get(nev); assert(pv!=NULL);
		if (nev==1) {res=*pv; ma->free(pv);} else {res.set(pv,nev); setHT(res,ma->getAType());}
	} catch (...) {
		for (unsigned i=0; i<nvals; i++) freeV(vals[i]);
		for (unsigned i=0; i<events; i++) freeV(events[i]);
		throw;
	}
}

void SInCtx::parseFSM(Stmt *&stmt)
{
	TLx lx; RC rc; FSMTable stateTable(16,ma); unsigned id=0;
	DynArray<PINDscr,100> pins(ma); DynArray<Value,10> vals(ma);
	if (lex()!=LX_IDENT) throw SY_MISIDN; mapURI(true); v.setPropID(PROP_SPEC_OBJID); if ((rc=vals+=v)!=RC_OK) throw rc;
	if (lex()!=LX_KEYW || v.op!=KW_AS) throw SY_MISAS; if (lex()!=LX_LCBR) throw SY_MISLCBR;
	try {
		do {
			mode|=SIM_SIMPLE_NAME; if (lex()!=LX_IDENT || v.type!=VT_STRING) throw SY_MISIDN; mode&=~SIM_SIMPLE_NAME;
			FSMTable::Find fft(stateTable,StrLen(v.str,v.length)); FSMState *st=fft.find();
			if (st==NULL) {
				if ((st=new(ma) FSMState(v.str,v.length,++id,ma))==NULL) throw RC_NOMEM;
				stateTable.insert(st,fft.getIdx());
			} else if ((st->id&0x80000000)!=0) st->id&=~0x80000000; else throw RC_ALREADYEXISTS;
			if (lex()!=LX_COLON) throw SY_MISCOLON; if (lex()!=LX_LCBR) throw SY_MISLCBR;
			do {
				if (lex()!=LX_IDENT) throw SY_MISPROP;
				mapURI(); assert(v.type==VT_URIID); URIID uid=v.uid; uint8_t meta=0;
				if (!checkProp(uid,true)) throw SY_INVPROP;
				parseMeta(uid,meta); if (lex()!=OP_EQ) throw SY_MISEQ;
				if ((lx=lex())!=LX_KEYW || v.op!=KW_NULL) {
					nextLex=lx; Value w;
					if (uid==PROP_SPEC_TRANSITION) parseEvents(w,&stateTable,&id); else parse(w); w.setPropID(uid); w.meta|=meta;
					if ((w.flags&HEAP_TYPE_MASK)!=ma->getAType() && (rc=copyV(w,w,ma))!=RC_OK || (rc=vals+=w)!=RC_OK) {freeV(w); throw rc;}
				}
			} while ((lx=lex())==LX_COMMA);
			uint32_t nVals=0; const Value *pv=vals.get(nVals);
			if ((rc=ses->normalize(pv,nVals,0,ses!=NULL?ses->getStore()->getPrefix():0,ma,true))!=RC_OK) {freeV((Value*)pv,nVals,ma); throw rc;}
			PINDscr vv(pv,nVals,st->id,true); if ((rc=pins+=vv)!=RC_OK) {freeV((Value*)pv,nVals,ma); throw rc;}
			if (lx!=LX_RCBR) throw SY_MISRCBR;
		} while ((lx=lex())==LX_COMMA);
		if (lx!=LX_RCBR) throw SY_MISRCBR;
		for (FSMTable::it sit(stateTable); ++sit; ) {FSMState *st=sit.get(); if ((st->id&0x80000000)!=0) throw RC_NOTFOUND;}
		if ((stmt=new(ma) Stmt(0,ma,STMT_INSERT,txi))==NULL) throw RC_NOMEM;
		uint32_t nPins=0; PINDscr *pds=pins.get(nPins);
		if (pds!=NULL && (rc=stmt->setValuesNoCopy(pds,nPins))!=RC_OK) {
			for (unsigned i=0; i<nPins; i++) freeV(*(Value**)&pds[i].vals,pds[i].nValues,ma);
			ma->free(pds); throw rc;
		}
	} catch (...) {
		for (unsigned i=0; i<vals; i++) freeV(vals[i]);
		for (unsigned i=0; i<pins; i++) freeV(*(Value**)&pins[i].vals,pins[i].nValues,ma);
		throw;
	}
}

void SInCtx::parseStart(Stmt *&stmt)
{
	TLx lx; unsigned md=0;
	if ((lx=lex())==LX_IDENT && v.type==VT_STRING && v.length==sizeof(TRANSACTION_S)-1 && cmpncase(v.str,TRANSACTION_S,sizeof(TRANSACTION_S)-1)) {
		//
		if ((lx=lex())!=LX_IDENT || v.type!=VT_STRING || v.length!=sizeof(READ_S)-1 || !cmpncase(v.str,READ_S,sizeof(READ_S)-1)) nextLex=lx;
		else if ((lx=lex())==LX_IDENT && v.type==VT_STRING && v.length==sizeof(ONLY_S)-1 || cmpncase(v.str,ONLY_S,sizeof(ONLY_S)-1)) md=MODE_READONLY;
		else if (lx!=LX_IDENT || v.type!=VT_STRING || v.length!=sizeof(WRITE_S)-1 || !cmpncase(v.str,WRITE_S,sizeof(WRITE_S)-1)) throw SY_SYNTAX;
		if ((stmt=new(ma) Stmt(md,ma,STMT_START_TX))==NULL) throw RC_NOMEM;
	} else if (lx==LX_IDENT && v.type==VT_URIID && (v.meta&META_PROP_OBJECT)!=0) {
		// START #myFSM(afy:content=...,param1=...) -> INSERT afy:state=#myFSM,...
	}
}

void SInCtx::parseDrop(Stmt *&stmt)
{
	TLx lx; RC rc; QVarID var;
	if ((lx=lex())!=LX_KEYW) {
		if (lx!=LX_IDENT || v.type!=VT_STRING) throw SY_SYNTAX;
		if (v.length==sizeof(CLASS_S)-1 && cmpncase(v.str,CLASS_S,sizeof(CLASS_S)-1) || v.length==sizeof(EVENT_S)-1 && cmpncase(v.str,EVENT_S,sizeof(EVENT_S)-1)) {
			if (lex()!=LX_IDENT) throw SY_MISIDN;
			mapURI(true); Class *cls;
			if ((cls=ses->getStore()->classMgr->getClass(v.uid))==NULL) rc=RC_NOTFOUND;
			else {
				const PID id=cls->getPID(); cls->release();
				if ((stmt=new(ma) Stmt(MODE_PURGE|MODE_CLASS,ma,STMT_DELETE,txi))==NULL) throw RC_NOMEM;
				rc=(var=stmt->addVariable())!=INVALID_QVAR_ID?stmt->setPIDs(var,&id,1):RC_NOMEM;
			}
		} else if (v.length==sizeof(TIMER_S)-1 && cmpncase(v.str,TIMER_S,sizeof(TIMER_S)-1)) {
			//...
		} else if (v.length==sizeof(LISTENER_S)-1 && cmpncase(v.str,LISTENER_S,sizeof(LISTENER_S)-1)) {
			//...
		} else throw SY_SYNTAX;
	} else switch (v.op) {
	default: throw SY_SYNTAX;
	case KW_USING:
		//???? MODE_PURGE
		break;
	}
}

void SInCtx::parseSet(Stmt *&stmt)
{
	TLx lx; RC rc;
	if ((lx=lex())==LX_KEYW) {
		if (v.op==KW_BASE) {
			if (lex()!=LX_CON || v.type!=VT_STRING) throw SY_MISQUO;
			if (v.length!=0) {
				size_t l=v.length+100;
				if ((base=(char*)ma->realloc(base,l,lBaseBuf))==NULL) throw RC_NOMEM;
				memcpy(base,v.str,lBase=v.length); lBaseBuf=l;
				if (base[lBase-1]!='/' && base[lBase-1]!='#') base[lBase++]='/';
			}
		} else if (v.op==KW_PREFIX) {
			if (lex()!=LX_PREFIX) throw SY_MISQNM; QName qn;
			if (v.length==sizeof(AFFINITY_STD_QPREFIX)-1 && memcmp(v.str,AFFINITY_STD_QPREFIX,sizeof(AFFINITY_STD_QPREFIX)-1)==0 ||
				v.length==sizeof(AFFINITY_SRV_QPREFIX)-1 && memcmp(v.str,AFFINITY_SRV_QPREFIX,sizeof(AFFINITY_SRV_QPREFIX)-1)==0) throw RC_ALREADYEXISTS;
			qn.qpref=v.str; qn.lq=v.length;
			if (lex()!=LX_CON || v.type!=VT_STRING) throw SY_MISQUO;
			if (v.length!=0) {
				qn.str=v.str; qn.lstr=v.length; qn.fDel=qn.lstr!=0&&(qn.str[qn.lstr-1]=='/'||qn.str[qn.lstr-1]=='#'||qn.str[qn.lstr-1]=='?');
				if ((rc=BIN<QName,const QName&,QNameCmp>::insert(qNames,nQNames,qn,qn,ma))!=RC_OK) throw rc;
			}
		} else throw SY_SYNTAX;
	} else if (lx==OP_TRACE) {
		unsigned trc=0; bool fReset=false,fAll=false; if ((lx=lex())==LX_KEYW && v.op==KW_ALL) fAll=true; else nextLex=lx;
		do {
			if (lex()!=LX_IDENT || v.type!=VT_STRING) throw SY_SYNTAX;
			for (unsigned i=0; ;i++)
				if (i>=sizeof(traceOptions)/sizeof(traceOptions[0])) throw SY_SYNTAX;
				else if (v.length==traceOptions[i].ls.l && cmpncase(v.str,traceOptions[i].ls.s,v.length)) {trc|=traceOptions[i].flg; break;}
		} while ((lx=lex())==LX_COMMA);
		if (lx!=LX_IDENT || v.type!=VT_STRING) nextLex=lx;
		else if (v.length==sizeof(OFF_S)-1 && cmpncase(v.str,OFF_S,sizeof(OFF_S)-1)) fReset=true; 
		else if (v.length!=sizeof(ON_S)-1 || !cmpncase(v.str,ON_S,sizeof(ON_S)-1)) nextLex=lx;
		ses->changeTraceMode(trc,fReset); if (fAll) ses->getStore()->changeTraceMode(trc,fReset);
	} else if (lx==LX_IDENT && v.type==VT_STRING && v.length==sizeof(ISOLATION_S)-1 && cmpncase(v.str,ISOLATION_S,sizeof(ISOLATION_S)-1)) {
		// read level, set this->txi
	} else throw SY_SYNTAX;
}

void SInCtx::parseRule(Stmt *&stmt)
{
	TLx lx; RC rc; Value w; unsigned md=0; DynArray<Value> *params=NULL;
	DynArray<Value,40> vals(ma); DynArray<Value,10> args(ma);

	if ((lx=lex())==LX_PREFIX) nextLex=LX_COLON; else if (lx!=LX_IDENT) throw SY_MISIDN;
	mapURI(true); if (ses->getStore()->namedMgr->exists(v.uid)) throw RC_ALREADYEXISTS;
	v.setPropID(PROP_SPEC_OBJID); v.setOp(OP_SET); vals+=v;
	if (lex()!=LX_COLON) throw SY_MISCOLON;
	try {
		if ((stmt=new(ma) Stmt(md,ma,STMT_QUERY,txi))==NULL) throw RC_NOMEM;
		QVar *qv=stmt->findVar(stmt->addVariable()); if (qv==NULL) throw RC_NOMEM;
		do {
			if (lex()!=LX_IDENT) throw SY_MISIDN; mapURI(true); PINx cb(ses); const Value *pv;
			if ((rc=ses->getStore()->namedMgr->getNamed(v.uid,cb))!=RC_OK || (rc=cb.getV(PROP_SPEC_CONDITION,pv))!=RC_OK) throw rc;
			if (pv->type!=VT_EXPR /*|| (((Expr*)pv->expr)->getFlags()&EXPR_BOOL)==0*/) throw RC_TYPE;
			ExprNode *cnd=NULL; if ((rc=((Expr*)pv->expr)->decompile(cnd,ses))!=RC_OK) throw rc;
			if ((lx=lex())==LX_LPR) {
				do {
					parse(w);
					if ((w.flags&HEAP_TYPE_MASK)!=ma->getAType() && (rc=copyV(w,w,ma))!=RC_OK) {freeV(w); throw rc;}
					if ((rc=args+=w)!=RC_OK) break;
				} while ((lx=lex())==LX_COMMA);
				if (lx!=LX_RPR) {cnd->destroy(); throw SY_MISRPR;}
				uint32_t np; Value *pm=args.get(np);
				if ((((Expr*)pv->expr)->getFlags()&EXPR_PARAMS)!=0) rc=cnd->substitute(pm,np,ma);
				freeV(pm,np,ma); if (rc!=RC_OK) {cnd->destroy(); throw rc;} lx=lex();
			}
			if ((rc=stmt->processCondition(cnd,qv))!=RC_OK) {cnd->destroy(); throw rc;}
		} while (lx==OP_LAND);
		if (lx!=LX_ARROW) throw SY_SYNTAX;
		w.set(stmt); w.setPropID(PROP_SPEC_PREDICATE); setHT(w,mtype); if ((rc=vals+=w)!=RC_OK) throw rc; stmt=NULL;
		do {
			if (lex()!=LX_IDENT) throw SY_MISIDN; mapURI(true); PINx cb(ses); const Value *pv; Value act;
			if ((rc=ses->getStore()->namedMgr->getNamed(v.uid,cb))!=RC_OK || (rc=cb.getV(PROP_SPEC_ACTION,pv))!=RC_OK) throw rc;
			if (pv->type!=VT_STMT && pv->type!=VT_COLLECTION) throw RC_TYPE;
			if ((rc=copyV(*pv,act,ma))!=RC_OK) throw rc;
			if ((lx=lex())==LX_LPR) {
				if (params==NULL) params=new(ma) DynArray<Value>(ma);
				do {
					parse(w);
					if ((w.flags&HEAP_TYPE_MASK)!=ma->getAType() && (rc=copyV(w,w,ma))!=RC_OK) {freeV(w); throw rc;}
					if ((rc=(*params)+=w)!=RC_OK) break;
				} while ((lx=lex())==LX_COMMA);
				if (lx!=LX_RPR) throw SY_MISRPR;
				uint32_t nParams; const Value *pars=params->get(nParams);
				rc=substitute(act,pars,nParams,ma); freeV((Value*)pars,nParams,ma);
				if (rc!=RC_OK) {freeV(act); throw rc;} lx=lex();
			}
			if ((rc=args+=act)!=RC_OK) throw rc;
		} while (lx==LX_COMMA);
		nextLex=lx;
		uint32_t nActs=0; Value *acts=(Value*)args.get(nActs); assert(acts!=NULL && nActs!=0);
		if (nActs==1) {w=acts[0]; ma->free(acts);}
		else if ((rc=ExprNode::normalizeCollection(acts,nActs,w,ma,ses->getStore()))!=RC_OK) {freeV(acts,nActs,ma); throw rc;}
		w.setPropID(PROP_SPEC_ACTION); w.setMeta(META_PROP_ENTER); if ((rc=vals+=w)!=RC_OK) throw rc;
		uint32_t nVals=0; const Value *pv=vals.get(nVals); assert(pv!=NULL && nVals!=0);
		if ((rc=ses->normalize(pv,nVals,0,ses!=NULL?ses->getStore()->getPrefix():0,ma,true))!=RC_OK ||
			(rc=(stmt=new(ma) Stmt(md,ma,STMT_INSERT,txi))==NULL?RC_NOMEM:stmt->setValuesNoCopy((Value*)pv,nVals))!=RC_OK)
				{freeV((Value*)pv,nVals,ma); throw rc;}
	} catch (...) {
		if (stmt!=NULL) stmt->destroy();
		if (params!=NULL) {for (unsigned i=0; i<*params; i++) freeV(*(Value*)&(*params)[i]); delete params;}
		for (unsigned i=0; i<args; i++) freeV(args[i]);
		for (unsigned i=0; i<vals; i++) freeV(vals[i]);
		throw;
	}
}

Stmt *SInCtx::parseStmt()
{
	Stmt *stmt=NULL; uint8_t op; unsigned md,msave=mode&(SIM_DML_EXPR|SIM_INSERT);
	SynErr sy=SY_ALL; RC rc=RC_OK; Value w; DynArray<Value,10> with(ma);
	for (TLx lx;;) {
		switch (lx=lex()) {
		default: throw SY_SYNTAX;
		case LX_EOE: break;
		case LX_LPR: v.op=KW_SELECT;
		case LX_KEYW:
			md=0;
			switch (op=v.op) {
			case KW_SELECT:
				nextLex=lx; parseQuery(stmt,false); 
				if (with!=0) {uint32_t nWith=0; Value *pw=with.get(nWith); stmt->with=Values(pw,nWith,true);}
				break;
			case KW_INSERT:
				parseInsert(stmt,with); break;
			case KW_UPDATE:
				parseUpdate(stmt,with); break;
			case KW_PURGE: case KW_DELETE: case KW_UNDELETE:
				parseDelete(stmt,(KW)op,with); break;
			case KW_CREATE:
				parseCreate(stmt,with); break;
			case KW_DROP:
				parseDrop(stmt); break;
			case KW_START:
				parseStart(stmt); break;
			case KW_COMMIT: case KW_ROLLBACK:
				if ((lx=lex())!=LX_KEYW || v.op!=KW_ALL) nextLex=lx; else md=MODE_ALL;
				if ((stmt=new(ma) Stmt(md,ma,op==KW_COMMIT?STMT_COMMIT:STMT_ROLLBACK))==NULL) throw RC_NOMEM;
				break;
			case KW_SET:
				parseSet(stmt);
				if ((lx=lex())==LX_SEMI) continue;
				nextLex=lx; break;
			case KW_WITH:
				assert(with==0);
				for (mode&=~(SIM_DML_EXPR|SIM_INSERT);;) {
					if ((lx=lex())!=LX_IDENT) {if (with!=0) sy=SY_MISPROP; else if (lx!=LX_EOE) {if (lx==LX_SEMI) nextLex=lx; else sy=SY_MISPROP;} break;}
					mapURI(); assert(v.type==VT_URIID); URIID uid=v.uid; uint8_t meta=0; parseMeta(uid,meta);
					if (lex()!=OP_EQ) {sy=SY_MISEQ; break;}
					if ((lx=lex())!=LX_KEYW || v.op!=KW_NULL) {
						nextLex=lx; parse(w);
						if ((w.flags&HEAP_TYPE_MASK)!=ma->getAType() && (rc=copyV(w,w,ma))!=RC_OK) break;
						w.setPropID(uid); w.meta=meta; if ((rc=with+=w)!=RC_OK) break;
					}
					if ((lx=lex())!=LX_COMMA) {nextLex=lx; break;}
				}
				mode=mode&~(SIM_DML_EXPR|SIM_INSERT)|msave; if (rc!=RC_OK) throw rc; if (sy!=SY_ALL) throw sy;
				if ((nextLex=lex())!=LX_KEYW || v.op!=KW_SELECT && v.op!=KW_INSERT && v.op!=KW_UPDATE && v.op!=KW_CREATE && v.op!=KW_DELETE && v.op!=KW_PURGE && v.op!=KW_UNDELETE) throw SY_SYNTAX;
				continue;
			case KW_USING:
				//???
				throw RC_INTERNAL;
				if ((lx=lex())==LX_SEMI) continue;
				nextLex=lx; break;
			case KW_RULE:
				parseRule(stmt); break;
			}
			break;
		}
		if (sy!=SY_ALL || rc!=RC_OK) {
			if (stmt!=NULL) stmt->destroy();
			if (sy!=SY_ALL) throw sy; else throw rc;
		}
		return stmt;
	}
}

RC SInCtx::exec(const Value *params,unsigned nParams,char **result,uint64_t *nProcessed,unsigned nProcess,unsigned nSkip)
{
	SOutCtx out(ses); assert(ses!=NULL); unsigned fBrkIns=false; uint64_t cnt=0,c=0; 
	Values vv(params,nParams); EvalCtx ectx(ses,NULL,0,NULL,0,&vv,1,NULL,NULL,ECT_INSERT);
	for (TLx lx=lex();;lx=lex()) {
		if (lx==LX_EOE) break; if (lx!=LX_LPR && lx!=LX_KEYW && lx!=LX_IDENT) throw SY_SYNTAX;
		nextLex=lx; SynErr sy=SY_ALL; RC rc=RC_OK; Stmt *stmt; Cursor *cr=NULL;
		if ((stmt=parseStmt())!=NULL) {
			if ((nextLex=lex())!=LX_SEMI && nextLex!=LX_EOE) throw SY_SYNTAX;
			if (c!=0 && stmt->getOp()<STMT_START_TX) {
				if (!fBrkIns) {fBrkIns=true; if (!out.insert("[",1,0)) throw RC_NOMEM;}
				if (!out.append(",\n",2)) throw RC_NOMEM;
			}
			if ((rc=stmt->execute(ectx,NULL,nProcess,nSkip,0/*mode???*/,NULL,TXI_DEFAULT,result!=NULL?&cr:NULL))==RC_OK && cr!=NULL)
				{if ((rc=out.renderJSON(cr,c))==RC_EOF) rc=RC_OK; cnt+=c; cr->destroy();}
			stmt->destroy();
		}
		if (nProcessed!=NULL) *nProcessed=cnt;
		if (sy!=SY_ALL) throw sy; if (rc!=RC_OK) throw rc;
		if (lex()!=LX_SEMI) break;
	}
	if (cnt!=0 && !out.append(fBrkIns?"]\n":"\n",fBrkIns?2:1)) throw RC_NOMEM;
	return result==NULL||(*result=(char*)out)!=NULL?RC_OK:RC_NOMEM;
}

void SInCtx::parseManage(IAffinity *&ctx,const StartupParameters *sp)
{
	struct StrCopy {
		char *s; 
		StrCopy() : s(NULL) {} 
		~StrCopy() {::free(s);} 
		operator const char*() const {return s;} 
		void set(const char *str,size_t l) {if ((s=(char*)::malloc(l+1))==NULL) throw RC_NOMEM; memcpy(s,str,l); s[l]='\0';}
	};
	for (TLx lx=lex(); lx!=LX_EOE; lx=lex()) {
		byte op=KW_SELECT; RC rc=RC_OK; StrCopy dirc,pwdc,identc,dir2c,dir3c; uint64_t memSize=0;
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
		else if ((lx=lex())==LX_IDENT && v.type==VT_STRING && v.length==sizeof(MEMORY_S)-1 && cmpncase(v.str,MEMORY_S,sizeof(MEMORY_S)-1)) {
			if ((lx=lex())!=LX_LPR) {nextLex=lx; memSize=DEFAULT_INMEM_SIZE;}
			else if (lex()!=LX_CON) throw SY_MISCON;
			else {
				switch (v.type) {
				default: throw RC_TYPE;
				case VT_INT: if (v.i<0) throw RC_INVPARAM;
				case VT_UINT: memSize=v.ui; break;
				case VT_INT64: if (v.i64<0) throw RC_INVPARAM;
				case VT_UINT64: memSize=v.ui64; break;
				}
				if (memSize<0x100000) throw RC_INVPARAM;
				if (lex()!=LX_RPR) throw SY_MISRPR;
			}
		} else if (lx==LX_CON && v.type==VT_STRING) {dir=v.str; ldir=v.length;} else throw SY_MISCON;
		if (dir!=NULL && ldir!=0) {dirc.set(dir,ldir); dir=dirc;} else if (sp!=NULL) dir=sp->directory;
		switch (op) {
		case KW_CREATE: case KW_SELECT:
			{StoreCreationParameters cparams; StartupParameters params(STARTUP_MODE_SERVER); if (sp!=NULL) params=*sp; params.directory=dir;
			if (memSize!=0) {if ((params.memory=allocAligned((size_t)memSize,getPageSize()))==NULL) throw RC_NOMEM; params.lMemory=memSize;}
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
					} else if (vv.length==sizeof("PASSWORD")-1 && cmpncase(vv.str,"PASSWORD",vv.length)) {
						if (v.type!=VT_STRING) throw SY_MISQUO;
						pwdc.set(v.str,v.length); params.password=cparams.password=pwdc;
					} else if (vv.length==sizeof("LOGDIRECTORY")-1 && cmpncase(vv.str,"LOGDIRECTORY",vv.length)) {
						if (v.type!=VT_STRING) throw SY_MISQUO; //???
						dir2c.set(v.str,v.length); params.logDirectory=dir2c;
					} else if (vv.length==sizeof("SERVICEDIRECTORY")-1 && cmpncase(vv.str,"SERVICEDIRECTORY",vv.length)) {
						if (v.type!=VT_STRING) throw SY_MISQUO; //???
						dir3c.set(v.str,v.length); params.serviceDirectory=dir3c;
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
						if (v.type!=VT_BOOL) throw SY_MISLGC;if (v.b) cparams.mode|=STORE_CREATE_ENCRYPTED;
					} else if (vv.length==sizeof("PAGEINTEGRITY")-1 && cmpncase(vv.str,"PAGEINTEGRITY",vv.length)) {
						if (v.type!=VT_BOOL) throw SY_MISLGC;if (v.b) cparams.mode|=STORE_CREATE_PAGE_INTEGRITY;
					} else if (vv.length==sizeof("MAXSIZE")-1 && cmpncase(vv.str,"MAXSIZE",vv.length)) {
						if (v.type==VT_INT || v.type==VT_UINT) cparams.maxSize=v.ui;
						else if (v.type==VT_INT64 || v.type==VT_UINT64) cparams.maxSize=v.ui64;
						else throw SY_MISNUM;
					} else if (vv.length==sizeof("LOGSEGSIZE")-1 && cmpncase(vv.str,"LOGSEGSIZE",vv.length)) {
						if (v.type==VT_INT || v.type==VT_UINT) cparams.logSegSize=v.ui; else throw SY_MISNUM;
					} else if (vv.length==sizeof("PCTFREE")-1 && cmpncase(vv.str,"PCTFREE",vv.length)) {
						if (v.type==VT_FLOAT) cparams.pctFree=v.f; else if (v.type==VT_DOUBLE) cparams.pctFree=(float)v.d; else throw SY_SYNTAX;
					} else if (vv.length==sizeof("MAXSYNCACTIONS")-1 && cmpncase(vv.str,"MAXSYNCACTIONS",vv.length)) {
						if (v.type==VT_INT || v.type==VT_UINT) cparams.xSyncStack=v.ui; else throw SY_MISNUM;
					} else if (vv.length==sizeof("MAXONCOMMIT")-1 && cmpncase(vv.str,"MAXONCOMMIT",vv.length)) {
						if (v.type==VT_INT || v.type==VT_UINT) cparams.xOnCommit=v.ui; else throw SY_MISNUM;
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
			if (lex()!=LX_CON || v.type!=VT_STRING) throw SY_MISQUO;
			dir2c.set(v.str,v.length); rc=FileMgr::moveStore(dir,dir2c);
			break;
		case KW_DROP: case KW_DELETE:
			// check opened -> close
			ctx=NULL; rc=FileMgr::deleteStore(dir);
			break;
		}
		if (rc!=RC_OK) throw rc;
		if ((lx=lex())!=LX_SEMI && lx!=LX_EOE) throw RC_SYNTAX;
	}
}

//---------------------------------------------------------------------------------------------------

IStmt *Session::createStmt(STMT_OP st_op,unsigned mode)
{
	try {return !ctx->inShutdown()?new(this) Stmt(unsigned(itf)|mode,this,st_op):NULL;}
	catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::createStmt()\n");}
	return NULL;
}

IStmt *Session::createStmt(const char *qs,const URIID *ids,unsigned nids,CompilationError *ce) 
{
	try {
		if (ce!=NULL) {memset(ce,0,sizeof(CompilationError)); ce->msg="";}
		if (qs==NULL || ctx->inShutdown()) return NULL;
		SInCtx in(this,qs,strlen(qs),ids,nids);
		try {Stmt *st=in.parseStmt(); in.checkEnd(true); return st;}
		catch (SynErr sy) {in.getErrorInfo(RC_SYNTAX,sy,ce);}
		catch (RC rc) {in.getErrorInfo(rc,SY_ALL,ce);}
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::createStmt(const char *,...)\n");}
	return NULL;
}

IExpr *Session::createExpr(const char *s,const URIID *ids,unsigned nids,CompilationError *ce)
{
	try {
		if (ce!=NULL) {memset(ce,0,sizeof(CompilationError)); ce->msg="";}
		if (s==NULL||ctx->inShutdown()) return NULL;
		SInCtx in(this,s,strlen(s),ids,nids); Expr *pe=NULL;
		try {ExprNode *et=in.parse(false); in.checkEnd(); Expr::compile(et,pe,this,false); et->destroy(); return pe;}
		catch (SynErr sy) {in.getErrorInfo(RC_SYNTAX,sy,ce);}
		catch (RC rc) {in.getErrorInfo(rc,SY_ALL,ce);}
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::toExpr()\n");}
	return NULL;
}

IExprNode *Session::createExprTree(const char *s,const URIID *ids,unsigned nids,CompilationError *ce)
{
	try {
		if (ce!=NULL) {memset(ce,0,sizeof(CompilationError)); ce->msg="";}
		if (s==NULL||ctx->inShutdown()) return NULL;
		SInCtx in(this,s,strlen(s),ids,nids);
		try {ExprNode *et=in.parse(true); in.checkEnd(); return et;} 
		catch (SynErr sy) {in.getErrorInfo(RC_SYNTAX,sy,ce);}
		catch (RC rc) {in.getErrorInfo(rc,SY_ALL,ce);}
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in ISession::toExprTree()\n");}
	return NULL;
}

RC Session::parseValue(const char *s,size_t l,Value& res,CompilationError *ce)
{
	try {
		if (ce!=NULL) {memset(ce,0,sizeof(CompilationError)); ce->msg="";}
		if (s==NULL) return RC_INVPARAM; if (ctx->inShutdown()) return RC_SHUTDOWN;
		SInCtx in(this,s,l,NULL,0);
		try {in.parse(res,NULL,0,PRS_COPYV); in.checkEnd();}
		catch (SynErr sy) {in.getErrorInfo(RC_SYNTAX,sy,ce); return RC_SYNTAX;}
		catch (RC rc) {in.getErrorInfo(rc,SY_ALL,ce); return rc;}
		return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::parseValue()\n"); return RC_INTERNAL;}
}

RC Session::parseValues(const char *s,size_t l,Value *&res,unsigned& nValues,CompilationError *ce,char delimiter)
{
	try {
		res=NULL; nValues=0;
		if (ce!=NULL) {memset(ce,0,sizeof(CompilationError)); ce->msg="";}
		if (s==NULL) return RC_INVPARAM; if (ctx->inShutdown()) return RC_SHUTDOWN;
		SInCtx in(this,s,l,NULL,0); Value *pv=NULL; unsigned nvals=0,xvals=0; RC rc=RC_OK;
		try {
			do {
				if (nvals>=xvals && (pv=(Value*)realloc(pv,(xvals+=xvals==0?8:xvals/2)*sizeof(Value)))==NULL)
					{nvals=0; throw RC_NOMEM;}
				in.parse(pv[nvals],NULL,0,PRS_COPYV); nvals++;
			} while (in.checkDelimiter(delimiter));
			in.checkEnd(); if ((nValues=nvals)!=0) res=pv; return RC_OK;
		} catch (SynErr sy) {in.getErrorInfo(RC_SYNTAX,sy,ce); rc=RC_SYNTAX;}
		catch (RC rc2) {in.getErrorInfo(rc,SY_ALL,ce); rc=rc2;}
		freeV(pv,nvals,this); return rc;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::parseValues()\n"); return RC_INTERNAL;}
}

RC Session::getTypeName(ValueType type,char buf[],size_t lbuf)
{
	try {
		if (type>=VT_ALL || buf==NULL || lbuf==0) return RC_INVPARAM;
		strncpy(buf,typeName[type].s,lbuf-1)[lbuf-1]=0; return RC_OK;
	} catch (RC rc) {return rc;} catch (...) {report(MSG_ERROR,"Exception in ISession::getTypeName()\n"); return RC_INTERNAL;}
}

char *Stmt::toString(unsigned mode,const QName *qNames,unsigned nQNames) const
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL||ses->getStore()->inShutdown()) return NULL;
		SOutCtx out(ses,mode,qNames,nQNames); return render(out)==RC_OK?out.renderAll():(char*)0;
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

char *ExprNode::toString(unsigned mode,const QName *qNames,unsigned nQNames) const
{
	try {
		Session *ses=Session::getSession(); if (ses==NULL) return NULL;
		StoreCtx *ctx=ses->getStore(); if (ctx->inShutdown()) return NULL;
		SOutCtx out(ses,mode,qNames,nQNames); return render(0,out)==RC_OK?out.renderAll():(char*)0;
	} catch (RC) {} catch (...) {report(MSG_ERROR,"Exception in IExprNode::toString()\n");}
	return NULL;
}

RC PathSQLParser::invoke(IServiceCtx *ctx,const Value& inp,Value& out,unsigned& mode)
{
	if ((mode&(ISRV_MOREOUT|ISRV_WRITE))!=0) return RC_INVPARAM;
	if (inp.isEmpty() || isString((ValueType)inp.type) && inp.length==0) {
		const Value *pv=ctx->getCtxPIN()->getValue(PROP_SPEC_REQUEST);
		if (pv==NULL || !isString((ValueType)pv->type) || pv->length==0) return RC_OK;
		init(pv->str,pv->length);
	} else if (inp.type!=VT_STRING && inp.type!=VT_BSTR) return RC_TYPE;
	else init(inp.str,inp.length);
	try {
		Stmt *st=parseStmt(); checkEnd(true); if (st!=NULL) out.set(st,1); mode|=ISRV_EOM;
	} catch (SynErr/* sy*/) {
		/*getErrorInfo(RC_SYNTAX,sy,ce);*/ 
		if (ptr==end) mode|=ISRV_NEEDMORE|ISRV_APPEND; else return RC_SYNTAX;
	} catch (RC rc) {
		/*getErrorInfo(rc,SY_ALL,ce);*/ return rc;
	}
	return RC_OK;
}

RC PathSQLRenderer::invoke(IServiceCtx *ctx,const Value& inp,Value& out,unsigned& mode)
{
	RC rc=RC_OK;
	if (out.type!=VT_BSTR || out.length==0 || (mode&(ISRV_READ|ISRV_MOREOUT))!=0) return RC_INVPARAM;
	if (!inp.isEmpty()) switch (inp.type) {
	case VT_STMT: rc=((Stmt*)inp.stmt)->render(*this); break;
	case VT_EXPR: rc=((Expr*)inp.expr)->render(0,*this); break;
	case VT_REF: // as INSERT ???
	default: rc=renderValue(inp); break;
	} else if ((mode&ISRV_EOM)==0) {out.setEmpty(); return RC_OK;}
	if (rc==RC_OK) {
		uint32_t left=out.length; out.length=0;
		if (cLen!=0) {
			assert(sht<cLen); uint32_t l=min(uint32_t(cLen-sht),left); out.length=l;
			memcpy((char*)out.str,ptr+sht,l); left-=l; if ((sht+=l)==cLen) sht=cLen=0;
		}
		if (cLen!=0) mode=mode&~ISRV_EOM|ISRV_NEEDFLUSH; out.type=VT_STRING;
	}
	return rc;
}

void PathSQLRenderer::cleanup(IServiceCtx *sctx,bool fDestroying)
{
	if (ptr!=NULL) {ses->free(ptr); ptr=NULL;} sht=cLen=xLen=0;
}

RC PathSQLService::create(IServiceCtx *ctx,uint32_t& dscr,Processor *&ret)
{
	Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION;
	switch (dscr&ISRV_PROC_MASK) {
	default: ret=NULL; return RC_INVOP;
	case ISRV_READ:
		if ((ret=new(ctx) PathSQLParser(ctx,ses))==NULL) return RC_NOMEM;
		break;
	case ISRV_WRITE:
		if ((ret=new(ctx) PathSQLRenderer(ses))==NULL) return RC_NOMEM;
		dscr|=ISRV_ALLOCBUF; break;
	}
	return RC_OK;
}

RC JSONOut::outPIN(IServiceCtx *ctx,PIN *pin)
{
	PID id=pin->getPID(); RC rc;
	const Value *props=pin->getValueByIndex(0); unsigned nProps=pin->getNumberOfProperties();
	if (!id.isPID()) cbuf[lc++]='{';
	else {
		lc+=sprintf(cbuf+lc,"{\"id\":\"" _LX_FM "\"",id.pid);
		if (props!=NULL && nProps!=0) {cbuf[lc]=','; cbuf[lc+1]=' '; lc+=2;}
	}
	if (lc!=0) {if (append(cbuf,lc)) lc=0; else return RC_NOMEM;}
	for (unsigned i=0; i<nProps; ++i,++props) if (props->property!=STORE_INVALID_URIID) {
		if ((rc=renderValue(*props,JR_PROP))!=RC_OK) return rc;
		if (i+1<nProps && !append(", ",2)) return RC_NOMEM;
	}
	return append("}",1)?RC_OK:RC_NOMEM;
}

RC JSONOut::invoke(IServiceCtx *ctx,const Value& inp,Value& out,unsigned& mode)
{
	RC rc=RC_OK;
	if (out.type!=VT_BSTR || out.bstr==NULL || out.length==0 || (mode&(ISRV_READ|ISRV_MOREOUT))!=0) return RC_INVPARAM;
	if (ptr==NULL) {
		Value v; v.set(MIME_JSON,sizeof(MIME_JSON)-1); v.setPropID(PROP_SPEC_CONTENTTYPE); 
		RC rc=ctx->getCtxPIN()->modify(&v,1); if (rc!=RC_OK) return rc;
	}
	if (!inp.isEmpty()) switch (inp.type) {
	case VT_REF:
		if ((rc=outPIN(ctx,(PIN*)inp.pin))==RC_OK) {cbuf[0]=','; cbuf[1]='\n'; lc=2;}
		break;
	case VT_COLLECTION:
		if (inp.isNav()) {
			for (const Value *cv=inp.nav->navigate(GO_FIRST); cv!=NULL; cv=inp.nav->navigate(GO_NEXT)) {
				rc=cv->type==VT_REF?outPIN(ctx,(PIN*)cv->pin):lc==0||append(cbuf,lc)?(lc=0,renderValue(*cv,JR_VAL)):RC_NOMEM;
				if (rc==RC_OK) {cbuf[0]=','; cbuf[1]='\n'; lc=2;} else break;
			}
			break;
		}
	default:
		if ((rc=lc==0||append(cbuf,lc)?(lc=0,renderValue(inp,JR_VAL)):RC_NOMEM)==RC_OK) {cbuf[0]=','; cbuf[1]='\n'; lc=2;}
		break;
	} else if ((mode&ISRV_EOM)==0) {out.setEmpty(); return RC_OK;}
	if (rc==RC_OK) {
		uint32_t left=out.length; out.length=0;
		if (cLen!=0) {
			assert(sht<cLen); uint32_t l=min(uint32_t(cLen-sht),left); out.length=l;
			memcpy((char*)out.str,ptr+sht,l); left-=l; if ((sht+=l)==cLen) sht=cLen=0;
		}
		if ((mode&ISRV_EOM)==0) {if (out.length!=0) mode|=ISRV_NEEDFLUSH;}
		else if (left<2) {mode=mode&~ISRV_EOM|ISRV_NEEDFLUSH;}
		else {((char*)out.str)[out.length++]=']'; ((char*)out.str)[out.length++]='\n';}
		out.type=VT_STRING;
	}
	return rc;
}

void JSONOut::cleanup(IServiceCtx *sctx,bool fDestroying)
{
	cbuf[0]='['; lc=1; if (ptr!=NULL) {ses->free(ptr); ptr=NULL;} sht=cLen=xLen=0;
}

RC JSONService::create(IServiceCtx *ctx,uint32_t& dscr,Processor *&ret)
{
	if ((dscr&ISRV_PROC_MASK)!=ISRV_WRITE) {ret=NULL; return RC_INVOP;}
	Session *ses=Session::getSession(); if (ses==NULL) return RC_NOSESSION;
	dscr|=ISRV_ALLOCBUF; return (ret=new(ctx) JSONOut(ses))!=NULL?RC_OK:RC_NOMEM;
}
