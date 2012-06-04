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

#ifndef _AFFINITY_H_
#define _AFFINITY_H_

#undef AFY_EXP
#ifndef _MSC_VER
#define AFY_EXP
#else
#define AFY_EXP	_declspec(dllexport)
#endif
#include <stdint.h>
#include <stddef.h>
#include <stdarg.h>
#include <string.h>

#include "rc.h"
#include "units.h"

using namespace AfyRC;

namespace AfyKernel {class StoreCtx;}

typedef AfyKernel::StoreCtx *AfyDBCtx;

/**
 * Namespace to encapsulate the public interface of Affinity.
 * Overview:
 *  ISession represents a client's connection to the store.
 *  IPIN represents a PIN, i.e. a node of information.
 *  Through its ISession, a client can create, query, modify and delete PINs, control transactions, set session parameters, etc.
 *  Through IPIN, one can create, query, modify and delete properties or clone the PIN optionally overwriting properties.
 */
namespace AfyDB 
{
	typedef	uint32_t					IdentityID;	/**< Identity ID - used to represent the identity in this store */
	typedef	uint32_t					URIID;		/**< URI ID - used to represent a URI in this store */
	typedef	URIID						ClassID;	/**< Class ID - used to represent the class URI in this store */
	typedef	URIID						PropertyID;	/**< Property ID - same as URIID */
	typedef	uint32_t					ElementID;	/**< Immutable collection element ID */
	typedef	uint32_t					VersionID;	/**< Version number */

	#define	STORE_INVALID_PID			0ULL
	#define	STORE_INVALID_PROPID		(~AfyDB::PropertyID(0))	
	#define	STORE_INVALID_IDENTITY		(~AfyDB::IdentityID(0))	
	#define	STORE_INVALID_CLASSID		(~AfyDB::ClassID(0))	
	#define	STORE_CURRENT_VERSION		(~AfyDB::VersionID(0))	
	#define	STORE_MAX_URIID				AfyDB::URIID(0x1FFFFFFF)		/**< Maximum value for URIID */
	#define	STORE_OWNER					AfyDB::IdentityID(0)			/**< The owner of the store always has IdentityID equals to 0 */


	/**
	 * special property and class IDs
	 */
	#define	STORE_CLASS_OF_CLASSES		AfyDB::ClassID(0)				/**< Fixed ID of the class of all classes in the store */
	#define	PROP_SPEC_PINID				AfyDB::PropertyID(1)			/**< PIN id as PIN's property, immutable */
	#define	PROP_SPEC_DOCUMENT			AfyDB::PropertyID(2)			/**< document PIN this PIN is a part of */
	#define	PROP_SPEC_PARENT			AfyDB::PropertyID(3)			/**< parent (whole) PIN */
	#define	PROP_SPEC_VALUE				AfyDB::PropertyID(4)			/**< value of the PIN (can be an expression using other properties */
	#define	PROP_SPEC_CREATED			AfyDB::PropertyID(5)			/**< PIN creation timestamp, immutable */
	#define	PROP_SPEC_CREATEDBY			AfyDB::PropertyID(6)			/**< identity created the PIN, immutable */
	#define	PROP_SPEC_UPDATED			AfyDB::PropertyID(7)			/**< timestamp of the latest PIN modification, updated automatically */
	#define	PROP_SPEC_UPDATEDBY			AfyDB::PropertyID(8)			/**< identity of the latest PIN modification, updated automatically */
	#define	PROP_SPEC_ACL				AfyDB::PropertyID(9)			/**< ACL - can be a collection and contain references to other PINs */
	#define	PROP_SPEC_URI				AfyDB::PropertyID(10)			/**< External (RDF) URI of a pin, class or relation */
	#define	PROP_SPEC_STAMP				AfyDB::PropertyID(11)			/**< stamp (unsigned integer) of the latest modification, updated automatically */
	#define	PROP_SPEC_CLASSID			AfyDB::PropertyID(12)			/**< classID of the class represented by this pin (VT_CLASSID) */
	#define PROP_SPEC_PREDICATE			AfyDB::PropertyID(13)			/**< predicate of a class or relations (VT_STMT) */
	#define	PROP_SPEC_NINSTANCES		AfyDB::PropertyID(14)			/**< number of instances of a given class currently in the store */
	#define	PROP_SPEC_NDINSTANCES		AfyDB::PropertyID(15)			/**< number of soft-deleted instances of a given class currently in the store */
	#define	PROP_SPEC_SUBCLASSES		AfyDB::PropertyID(16)			/**< collection of classes which are specializations of this class */
	#define	PROP_SPEC_SUPERCLASSES		AfyDB::PropertyID(17)			/**< collection of classes which are abstractions of this class */
	#define PROP_SPEC_CLASS_INFO		AfyDB::PropertyID(18)			/**< bit flags describing the class (VT_UINT) */
	#define PROP_SPEC_INDEX_INFO		AfyDB::PropertyID(19)			/**< Family index information */
	#define PROP_SPEC_PROPERTIES		AfyDB::PropertyID(20)			/**< Properties refered by the class */
	#define	PROP_SPEC_JOIN_TRIGGER		AfyDB::PropertyID(21)			/**< Triggers associated with class events */
	#define	PROP_SPEC_UPDATE_TRIGGER	AfyDB::PropertyID(22)			/**< Triggers associated with class events */
	#define	PROP_SPEC_LEAVE_TRIGGER		AfyDB::PropertyID(23)			/**< Triggers associated with class events */
	#define	PROP_SPEC_REFID				AfyDB::PropertyID(24)			/**< Future implementation */
	#define	PROP_SPEC_KEY				AfyDB::PropertyID(25)			/**< Future implementation */
	#define	PROP_SPEC_VERSION			AfyDB::PropertyID(26)			/**< Chain of versions */
	#define	PROP_SPEC_WEIGHT			AfyDB::PropertyID(27)			/**< Future implementation */
	#define	PROP_SPEC_SELF				AfyDB::PropertyID(28)			/**< pseudo-property: the PIN itself */
	#define	PROP_SPEC_PROTOTYPE			AfyDB::PropertyID(29)			/**< JavaScript-like inheritance */
	#define	PROP_SPEC_WINDOW			AfyDB::PropertyID(30)			/**< Stream windowing control (size of class-related window) */

	#define	PROP_SPEC_MAX				PROP_SPEC_WINDOW
	
	#define	PROP_SPEC_ANY				(~AfyDB::PropertyID(0))			/**< used in queries, matches any property */

	/**
	 * special collection element IDs
	 * @see Value::eid
	 */
	#define	STORE_COLLECTION_ID			(~AfyDB::ElementID(0))		/**< Singleton value or operation applied to the whole collection */
	#define	STORE_LAST_ELEMENT			(~AfyDB::ElementID(1))		/**< Last element of a collection */
	#define	STORE_FIRST_ELEMENT			(~AfyDB::ElementID(2))		/**< First element of a collection */
	#define	STORE_SUM_COLLECTION		(~AfyDB::ElementID(3))		/**< IStmt: replace collection with the sum of its elements */
	#define	STORE_AVG_COLLECTION		(~AfyDB::ElementID(4))		/**< IStmt: replace collection with the average of its elements */
	#define	STORE_CONCAT_COLLECTION		(~AfyDB::ElementID(5))		/**< IStmt: replace collection with the concatenation of its elements */
	#define	STORE_MIN_ELEMENT			(~AfyDB::ElementID(6))		/**< IStmt: replace collection with the minimum of its elements */
	#define	STORE_MAX_ELEMENT			(~AfyDB::ElementID(7))		/**< IStmt: replace collection with the maximum of its elements */
	#define	STORE_SOME_ELEMENT			(~AfyDB::ElementID(8))		/**< IStmt: existential quantifier for the collection */
	#define	STORE_ALL_ELEMENTS			(~AfyDB::ElementID(9))		/**< IStmt: 'for all' quantifier */

	/**
	 * PIN create/commit/modify/delete, IStmt execute mode flags
	 */
	#define MODE_NO_EID					0x00010000	/**< in modify() - don't update "eid" field of Value structure */
	#define	MODE_FOR_UPDATE				0x00020000	/**< in IStmt::execute(): lock pins for update while reading them */
	#define	MODE_NEW_COMMIT				0x00040000	/**< used in PIN::clone() to immediately commit a cloned pin */
	#define	MODE_PURGE					0x00040000	/**< in deletePINs(): purge pins rather than just delete */
	#define	MODE_PRESERVE_INSERT_ORDER	0x00040000	/**< in commitPINs() - preserve insert order when allocating pins on pages */
	#define	MODE_ALL					0x00040000	/**< for STMT_COMMIT, STMT_ROLLBACK - commit/rollback all nested transactions */
	#define	MODE_READONLY				0x00040000	/**< for STMT_START_TX - start r/o transaction */
	#define	MODE_COPY_VALUES			0x00080000	/**< used in createUncommittedPIN() and IStmt::execute() to copy Values (parameters, query expressions, etc.) passed rather than assume ownership */
	#define	MODE_FORCE_EIDS				0x00100000	/**< used only in replication */
	#define	MODE_PART					0x00100000	/**< in nested insert: create a part */
	#define	MODE_TEMP_ID				0x00200000	/**< in commitPINs(): create a pin with a temporary (reusable, non-unique) id */
	#define	MODE_CHECK_STAMP			0x00200000	/**< forces stamp check before modification; if stamp changed the op is aborted and RC_REPEAT is returned */
	#define	MODE_HOLD_RESULT			0x00200000	/**< for IStmt::execute(): don't close result set on transaction commit/rollback */
	#define	MODE_ALL_WORDS				0x00400000	/**< all words must be present in FT condition */
	#define	MODE_DELETED				0x00800000	/**< for query: return only (soft)deleted pins */
	#define	MODE_SSV_AS_STREAM			0x01000000	/**< for getPIN(), IStmt::execute() - return SSVs as streams instead of strings */
	#define	MODE_FORCED_SSV_AS_STREAM	0x02000000	/**< for getPIN(), IStmt::execute(): return only META_PROP_SSTORAGE SSVs as streams instead of strings */
	#define	MODE_VERBOSE				0x04000000	/**< in IStmt::execute(): print evaluation plan */

	/**
	 * PIN creation flags
	 * @see ISession::commitPINs(), ISession::createPIN, ISession::createUncommittedPIN()
	 */
	#define	PIN_NO_REPLICATION			0x40000000	/**< marks a pin as one which shouldn't be replicated (only when the pin is committed) */
	#define	PIN_NO_INDEX				0x20000000	/**< special pin - no indexing */
	#define	PIN_NOTIFY					0x10000000	/**< pin generates notifications */
	#define	PIN_REPLICATED				0x08000000	/**< pin is replicated to another store or is a replica */
	#define	PIN_HIDDEN					0x04000000	/**< special pin - totally hidden from search/replication - accessible only through its PID */

	/**
	 * meta-properties
	 * @see Value::meta
	 */
	#define	META_PROP_PART				0x80		/**< property is a reference to a PIN which is a part of this one */
	#define	META_PROP_NOFTINDEX			0x80		/**< this property is not to be FT indexed/searched */
	#define	META_PROP_EVAL				0x80		/**< eval IExpr/IStmt in commitPINs/modifyPIN */
	#define	META_PROP_SSTORAGE			0x40		/**< store this property separately from the pin body (for string, stream and collection property types) */
	#define	META_PROP_RACCESS			0x40		/**< a stream should be optimized for random access */
	#define	META_PROP_NONOTIFICATION	0x20		/**< this property doesn't generate notifications when modified */
	#define	META_PROP_INHERIT			0x10		/**< inherited property of a class (equiv. to a method if VT_EXPR or to a static member) */
	#define	META_PROP_IFEXIST			0x08		/**< in modify/transform - apply modification if property exists, otherwise - ignore */
	#define META_PROP_IFNOTEXIST		0x04		/**< in modify/transform - apply modification (OP_SET or OP_ADD) if property doesn't exist, otherwise - ignore */
	#define	META_PROP_DERIVED			0x04		/**< this property was calculated from other properties in Transform rather than stored with the pin */
	#define	META_PROP_STOPWORDS			0x02		/**< filter stop words in ft indexing */

	/**
	 * ACL permissions
	 */
	#define	ACL_READ					0x0001
	#define	ACL_WRITE					0x0002

	/**
	 * Class flags
	 * @returned in PROP_SPEC_CLASS_INFO property of a class pin
	 */
	#define	CLASS_SDELETE				0x0001		/**< Class supports soft-delete optimization */
	#define	CLASS_VIEW					0x0002		/**< Class membership is not to be indexed */
	#define	CLASS_CLUSTERED				0x0004		/**< PINs belonging to class are clustered for more efficient sequential scan */
	#define	CLASS_INDEXED				0x0008		/**< Class membership is indexed (set by the kernel) */

	/**
	 * class-related notification flags
	 * @see ISession::setNotification()
	 */
	#define CLASS_NOTIFY_JOIN			0x0001		/**< a pin becomes a member of the class */
	#define CLASS_NOTIFY_LEAVE			0x0002		/**< a pin stops to be a member of the class */
	#define	CLASS_NOTIFY_CHANGE			0x0004		/**< a member of the class was changed */
	#define	CLASS_NOTIFY_DELETE			0x0008		/**< a pin - a member of the class - was deleted */
	#define	CLASS_NOTIFY_NEW			0x0010		/**< a new pin was created as a member of the class */

	/**
	 * types of transactions
	 */
	enum TX_TYPE
	{
		TXT_READONLY, TXT_READWRITE, TXT_MODIFYCLASS
	};

	/**
	 * transaction isolation levels
	 */
	enum TXI_LEVEL
	{
		TXI_DEFAULT,
		TXI_READ_UNCOMMITTED,
		TXI_READ_COMMITTED,
		TXI_REPEATABLE_READ,
		TXI_SERIALIZABLE
	};

	/**
	 * toString() functions control flags
	 * @see IStmt::toString(), IExpr::toString(), IExprTree::toString()
	 */
	#define	TOS_PROLOGUE				0x40000000
	#define	TOS_NO_QNAMES				0x80000000

	/**
	 * interface control flags
	 * @see ISession::setInterfaceMode()
	 */
	#define	ITF_DEFAULT_REPLICATION		0x0001		/**< by default all new pins marked as replicated */
	#define	ITF_REPLICATION				0x0002		/**< replication session */
	#define	ITF_CATCHUP					0x0004		/**< catchup session */
	#define	ITF_SPARQL					0x0008		/**< default style for text statement representation (if the flag is not set - SQL) */

	/**
	 * Trace control flags
	 */
	#define	TRACE_SESSION_QUERIES		0x0001		/**< trace all queries executed within this session */
	#define TRACE_STORE_BUFFERS			0x0002		/**< not implemented yet */
	#define TRACE_STORE_IO				0x0004		/**< not implemented yet */
	#define	TRACE_EXEC_PLAN				0x0008		/**< output execution plan for all queries in this session */

	/**
	 * External timestamp (VT_DATETIME) representation
	 * @see ISession::convDateTime()
	 */
	struct DateTime
	{
		uint16_t	year;
		uint16_t	month;
		uint16_t	dayOfWeek;
		uint16_t	day;
		uint16_t	hour;
		uint16_t	minute;
		uint16_t	second;
		uint32_t	microseconds;
	};

	/**
	 * Enumeration of subtypes for VT_CURRENT
	 * @see Value
	 */
	enum CurrentValue
	{
		CVT_TIMESTAMP, CVT_USER, CVT_STORE
	};

	/**
	 * OP_EXTRACT modes
	 */
	enum ExtractType
	{
		EY_FRACTIONAL, EY_SECOND, EY_MINUTE, EY_HOUR, EY_DAY, EY_WDAY, EY_MONTH, EY_YEAR,
		EY_IDENTITY,
	};

	/**
	 * Supported value types
	 * @see Value::type
	 */
	enum ValueType 
	{
		VT_ERROR,VT_ANY=VT_ERROR,		/**< used to report errors, non-existant properties; VT_ANY designates no conversion in transforms */

		VT_INT,							/**< 32-bit signed integer */
		VT_UINT,						/**< 32-bit unsigned integer */
		VT_INT64,						/**< 64-bit signed integer */
		VT_UINT64,						/**< 64-bit unsigned integer */
		VT_RESERVED1,					/**< reserved for future use */
		VT_FLOAT,						/**< 32-bit floating number */
		VT_DOUBLE,						/**< 64-bit floating number */

		VT_BOOL,						/**< boolean value (true, false) */

		VT_DATETIME,					/**< timestamp stored as a 64-bit number of microseconds since fixed date */
		VT_INTERVAL,					/**< time interval stored as a 64-bit signed number of microseconds */

		VT_URIID,						/**< URIID */
		VT_IDENTITY,					/**< IdentityID */

		VT_STRING,						/**< zero-ended string UTF-8*/
		VT_BSTR,						/**< binary string */
		VT_URL,							/**< URL string with special interpretation, UTF-8 */
		VT_RESERVED2,					/**< reserved for future use */

		VT_REF,							/**< a reference to another IPIN */
		VT_REFID,						/**< a reference to another PIN by its PID */
		VT_REFPROP,						/**< a reference to a value (property of this or another PIN) by IPIN & PropertyID */
		VT_REFIDPROP,					/**< a reference to a value (property of this or another PIN) by its PID/PropertyID */
		VT_REFELT,						/**< a reference to a collection element by its IPIN/PropertyID/ElementID */
		VT_REFIDELT,					/**< a reference to a collection element by its PID/PropertyID/ElementID */

		VT_EXPR,						/**< IExpr: stored expression */
		VT_STMT,						/**< IStmt: stored query object */

		VT_ARRAY,						/**< a collection property, i.e. a multi-valued property */
		VT_COLLECTION,					/**< collection iterator interface for big collections */
		VT_STRUCT,						/**< composite value */
		VT_RANGE,						/**< range of values for OP_IN, equivalent to VT_ARRAY with length = 2 */
		VT_STREAM,						/**< IStream interface */
		VT_CURRENT,						/**< current moment in time, current user, etc. */

		VT_VARREF,						/**< statement: var ref  (cannot be persisted as a property value) */
		VT_EXPRTREE,					/**< sub-expression reference for statement conditions (cannot be persisted as a property value) */

		VT_ALL
	};

	inline	bool	isRef(ValueType vt) {return uint8_t(vt-VT_REF)<=uint8_t(VT_REFIDELT-VT_REF);}
	inline	bool	isInteger(ValueType vt) {return uint8_t(vt-VT_INT)<=uint8_t(VT_UINT64-VT_INT);}
	inline	bool	isNumeric(ValueType vt) {return uint8_t(vt-VT_INT)<=uint8_t(VT_DOUBLE-VT_INT);}
	inline	bool	isString(ValueType vt) {return uint8_t(vt-VT_STRING)<=uint8_t(VT_URL-VT_STRING);}
	inline	bool	isCollection(ValueType vt) {return uint8_t(vt-VT_ARRAY)<=uint8_t(VT_COLLECTION-VT_ARRAY);}
	inline	bool	isComposite(ValueType vt) {return uint8_t(vt-VT_ARRAY)<=uint8_t(VT_STRUCT-VT_ARRAY);}

	/**
	 * Supported operations for expressions and property modifications
	 * @see Value::op
	 */
	enum ExprOp
	{
		OP_SET,						/**< used to add a new property or to set a new value for old property */
		OP_ADD,						/**< adds a new collection element for existing property (doesn't have to be a collection) */
		OP_ADD_BEFORE,				/**< add a new collection element before existing element (OP_ADD inserts after) */
		OP_MOVE,					/**< move a collection element to new position */
		OP_MOVE_BEFORE,				/**< move a collection element to new position (before specific element) */			
		OP_DELETE,					/**< deletes the whole property (non-collection), the whole collection (type==VT_ARRAY), or a collection element (type==VT_UINT, Value::ui==element index) */
		OP_EDIT,					/**< string editing operation; requires VT_STRING/VT_BSTR/VT_URL property */
		OP_RENAME,					/**< rename a property, i.e. replace one PropertyID with another one */
		OP_FIRST_EXPR,				/**< start of op codes which can be used in expressions */
		OP_PLUS=OP_FIRST_EXPR,		/**< binary '+' or '+=' */
		OP_MINUS,					/**< binary '-' or '-=' */
		OP_MUL,						/**< '*' or '*=' */
		OP_DIV,						/**< '/' or '/=' */
		OP_MOD,						/**< '%' or '%=' */
		OP_NEG,						/**< unary '-'*/
		OP_NOT,						/**< bitwise not, '~' */
		OP_AND,						/**< bitwise '&' or '&=' */
		OP_OR,						/**< bitwise '|' or '|=' */
		OP_XOR,						/**< bitwise '^' or '^=' */
		OP_LSHIFT,					/**< bitwise '<<' or '<<=' */
		OP_RSHIFT,					/**< bitwise '>>' or '>>=', can be unsigned if UNSIGNED_OP (see below) flag is set */
		OP_MIN,						/**< minimum of 2 or more values, minimum of collection */
		OP_MAX,						/**< maximum of 2 or more values, maximum of collection */
		OP_ABS,						/**< absolute value */
		OP_LN,						/**< natural logarithm */
		OP_EXP,						/**< exponent */
		OP_POW,						/**< power */
		OP_SQRT,					/**< square root */
		OP_FLOOR,					/**< floor */
		OP_CEIL,					/**< ceil */
		OP_CONCAT,					/**< concatenate strings */
		OP_LOWER,					/**< convert UTF-8 string to lower case */
		OP_UPPER,					/**< convert UTF-8 string to upper case */
		OP_TONUM,					/**< convert to number */
		OP_TOINUM,					/**< convert to integer number */
		OP_CAST,					/**< convert to arbitrary type, convert measurment units */
		OP_LAST_MODOP=OP_CAST,
		OP_RANGE,					/**< VT_RANGE constructor, returns a range defined by its 2 arguments */
		OP_ARRAY,					/**< VT_ARRAY constructor, returns a collection consisting of arguments passed */
		OP_STRUCT,					/**< VT_STRUCT constructor, returns a structure consisting of passed values, Value::property must be set */
		OP_PIN,						/**< same as OP_STRUCT, but returns a PIN (VT_REF) */
		OP_COUNT,					/**< number of elements in a collection, or number of PINs in the result set of a query or in a group */
		OP_LENGTH,					/**< string length in bytes or number elements in a collection */
		OP_POSITION,				/**< position of a substring in a string */
		OP_SUBSTR,					/**< extract substring from a string */
		OP_REPLACE,					/**< replace substring in a string with another string */
		OP_PAD,						/**< pad a string with a given character or a string of characters */
		OP_TRIM,					/**< trim leading, trailing characters, or both */
		OP_SUM,						/**< aggregate: sum of collection values, sum of values in a group */
		OP_AVG,						/**< aggregate: average of collection values, average of values in a group */
		OP_VAR_POP,					/**< aggregate: variance of collection values, variance of values in a group */
		OP_VAR_SAMP,				/**< aggregate: variance of collection values, variance of values in a group */
		OP_STDDEV_POP,				/**< aggregate: standard deviation of collection values, standard deviation of values in a group */
		OP_STDDEV_SAMP,				/**< aggregate: standard deviation of collection values, standard deviation of values in a group */
		OP_HISTOGRAM,				/**< aggregate: histogram of collection values, histogram of values in a group */
		OP_COALESCE,				/**< not implemented yet */
		OP_MEMBERSHIP,				/**< returns a collection of ClassIDs of classes a pin is a member of */
		OP_PATH,					/**< segment of a path expression */
		OP_FIRST_BOOLEAN,			/**< start of Boolean predicates */
		OP_EQ=OP_FIRST_BOOLEAN,		/**< equal */
		OP_NE,						/**< not equal */
		OP_LT,						/**< less than */
		OP_LE,						/**< less or equal than */
		OP_GT,						/**< greater than */
		OP_GE,						/**< greater or equal than */
		OP_IN,						/**< value is in a collection or result set, value is in range, pin is in result set, etc. */
		OP_BEGINS,					/**< string begins with the prefix */
		OP_CONTAINS,				/**< string contains substring */
		OP_ENDS,					/**< string ends with the suffix */
		OP_REGEX,					/**< regular expression match - not implemented yet */
		OP_EXISTS,					/**< property exists in PIN, i.e. IS NOT NULL */
		OP_ISLOCAL,					/**< PIN is local, i.e. not replicated, not read-only remote cached */
		OP_IS_A,					/**< PIN is a member of a class or one in a collection of classes */
		OP_LAND,					/**< logical AND */
		OP_LOR,						/**< logical OR */
		OP_LNOT,					/**< logical NOT */
		OP_LAST_BOOLEAN=OP_LNOT,

		OP_EXTRACT,					/**< extract part of a date, extract identity from a PIN ID */
		OP_DEREF,					/**< not implemented yet */
		OP_REF,						/**< not implemented yet */
		OP_CALL,					/**< invoke a function of code block with parameters */

		OP_ALL
	};

	inline	bool	isBool(ExprOp op) {return unsigned(op-OP_FIRST_BOOLEAN)<=unsigned(OP_LAST_BOOLEAN-OP_FIRST_BOOLEAN);}

	/**
	 * operation modifiers (bit flags) passed to IExprTree::expr(...)
	 */
	#define	UNSIGNED_OP				0x0001	// unsigned op (for right shift)
	#define	CASE_INSENSITIVE_OP		0x0001	// case insensitive comparison of strings
	#define	FILTER_LAST_OP			0x0001	// filter only last segment in OP_PATH repeating segment
	#define	NOT_BOOLEAN_OP			0x0002	// logical NOT of a boolean operation (e.g. NOT CONTAINS, DOESN'T EXIST)
	#define	DISTINCT_OP				0x0002	// DICTINCT modifier for aggregate functions
	#define	QUEST_PATH_OP			0x0002	// OP_PATH: a.b{?}
	#define	EXCLUDE_LBOUND_OP		0x0004	// exclude lower bound in 'in [a,b]'
	#define	PLUS_PATH_OP			0x0004	// OP_PATH: a.b{+}
	#define	STAR_PATH_OP			0x0006	// OP_PATH: a.b{*}
	#define	EXCLUDE_RBOUND_OP		0x0008	// exclude upper bound in 'in [a,b]'
	#define	FOR_ALL_LEFT_OP			0x0010	// quantification of the left argument of a comparison
	#define	EXISTS_LEFT_OP			0x0020	// quantification of the left argument of a comparison
	#define	FOR_ALL_RIGHT_OP		0x0040	// quantification of the right argument of a comparison
	#define	EXISTS_RIGHT_OP			0x0080	// quantification of the right argument of a comparison
	#define	NULLS_NOT_INCLUDED_OP	0x0100	// missing property doesn't satisfy OP_NE (or !OP_EQ) condition

	struct	Value;
	class	IPIN;
	class	IStoreInspector;

	/**
	 * PIN ID representation: 64-bit integer + 32-bit representing identity
	 */
	struct PID
	{
		uint64_t	pid;
		IdentityID	ident;
		operator	uint32_t() const {return uint32_t(pid>>32)^uint32_t(pid)^ident;}
		bool		operator==(const PID& rhs) const {return pid==rhs.pid && ident==rhs.ident;}
		bool		operator!=(const PID& rhs) const {return pid!=rhs.pid || ident!=rhs.ident;}
		void		setStoreID(uint16_t sid) {pid=(pid&0x0000FFFFFFFFFFFFULL)|(uint64_t(sid)<<48);}
	};

	/**
	 * PIN ID augmented by version ID (for future implementation of versioning)
	 */
	struct VPID
	{
		uint64_t	pid;
		IdentityID	ident;
		VersionID	vid;
	};

	/**
	 * reference to a PIN data element (property or an element of a collection property) + version
	 * @see VT_REFPROP and VT_REFELT in Value
	 */
	struct RefP
	{
		IPIN		*pin;
		PropertyID	pid;
		ElementID	eid;
		VersionID	vid;
	};

	/**
	 * reference to a PIN data element (PID, property or an element of a collection property) + version
	 * @see VT_REDIDPROP and VT_REFIDELT in Value
	 */
	struct RefVID
	{
		PID			id;
		PropertyID	pid;
		ElementID	eid;
		VersionID	vid;
	};

	/**
	 * generic stream interface for input and output parameters
	 * @see VT_STREAM in Value
	 */
	class IStream
	{
	public:
		virtual ValueType	dataType() const = 0;
		virtual	uint64_t	length() const = 0;
		virtual size_t		read(void *buf,size_t maxLength) = 0;
		virtual	size_t		readChunk(uint64_t offset,void *buf,size_t length) = 0;
		virtual	IStream		*clone() const = 0;
		virtual	RC			reset() = 0;
		virtual void		destroy() = 0;
	};
	struct	IStreamRef {
		IStream			*is;
		mutable	void	*prefix;
	};

	#define	VAR_TYPE_MASK	0xE000		// mask for RefV::flags field
	#define	VAR_PARAM		0x2000		// external parameter reference; other bits reserved for internal use

	/**
	 * VT_VARREF representation
	 */
	struct RefV
	{
		unsigned	char	refN;
		unsigned	short	type;
		unsigned	short	flags;
		PropertyID			id;
	};

	/**
	 * collection navigation operations
	 */
	enum GO_DIR
	{
		GO_FIRST,GO_LAST,GO_NEXT,GO_PREVIOUS,GO_FINDBYID
	};

	/**
	 * collection interface for in and out parameters
	 * @see VT_COLLECTION in Value
	 */
	class INav
	{
	public:
		virtual	const	Value			*navigate(GO_DIR=GO_NEXT,ElementID=STORE_COLLECTION_ID) = 0;
		virtual			ElementID		getCurrentID() = 0;
		virtual	const	Value			*getCurrentValue() = 0;
		virtual			RC				getElementByID(ElementID,Value&) = 0;
		virtual	INav					*clone() const = 0;
		virtual	unsigned long			count() const = 0;
		virtual			void			destroy() = 0;
	};

	struct IndexSeg
	{
		uint32_t	propID;
		uint16_t	flags;
		uint16_t	lPrefix;
		uint8_t		type;
		uint8_t		op;
	};

	/**
	 * OP_EDIT data
	 */
	struct StrEdit
	{
		union {
			const	char		*str;
			const	uint8_t		*bstr;
		};
		uint32_t				length;				/**< shift+length must be <= the length of the string being edited */
		uint64_t				shift;				/**< shift==~0ULL means 'end of string'; in this case length must be 0 */
	};
		
	/**
	 * floating value with measurement unit information
	 * @see VT_DOUBLE and VT_FLOAT in Value
	 */
	struct QualifiedValue {
		double			d;
		uint16_t		units;						/**< value form Units enumeration, see units.h */
	};
	
	/**
	 * main unit of information exchange interface; can contain data of various types, property ID, element ID, metaproperties, operation, etc.
	 */
	struct Value
	{
					PropertyID	property;
		mutable		ElementID	eid;
					uint32_t	length;
					uint8_t		type;
					uint8_t		op;
					uint8_t		meta;
		mutable		uint8_t		flags;				// for internal use only
		union {
			const	char		*str;
			const	uint8_t		*bstr;
					int32_t		i;
					uint32_t	ui;
					int64_t		i64;
					uint64_t	ui64;
					float		f;
					double		d;
					PID			id;
					VPID		vpid;
					IPIN		*pin;
			const	RefVID		*refId;
					RefP		ref;
			const	Value		*varray;
					INav		*nav;
					Value		*range;
					bool		b;
					URIID		uid;
					IdentityID	iid;
					IStreamRef	stream;
			class	IStmt		*stmt;
			class	IExpr		*expr;
			class	IExprTree	*exprt;
					RefV		refV;
					StrEdit		edit;
					IndexSeg	iseg;
			QualifiedValue		qval;
		};
	public:
		void	set(const char *s) {type=VT_STRING; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=s==NULL?0:(uint32_t)strlen(s); str=s; meta=0;}
		void	set(const char *s,uint32_t nChars) {type=VT_STRING; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=nChars; str=s; meta=0;}
		void	set(const unsigned char *bs,uint32_t l) {type=VT_BSTR; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=l; bstr=bs; meta=0;}
		void	setURL(const char *s) {type=VT_URL; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=s==NULL?0:(uint32_t)strlen(s); str=s; meta=0;}
		void	setURL(const char *s,uint32_t nChars) {type=VT_URL; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=nChars; str=s; meta=0;}
		void	set(int ii) {type=VT_INT; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(int32_t); i=ii; meta=0;}
		void	set(unsigned int u) {type=VT_UINT; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(uint32_t); ui=u; meta=0;}
		void	setI64(int64_t ii) {type=VT_INT64; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(int64_t); i64=ii; meta=0;}
		void	setU64(uint64_t u) {type=VT_UINT64; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(uint64_t); ui64=u; meta=0;}
		void	set(float fl,Units pu=Un_NDIM) {type=VT_FLOAT; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(float); f=fl; meta=0; qval.units=(uint16_t)pu;}
		void	set(double dd,Units pu=Un_NDIM) {type=VT_DOUBLE; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(double); d=dd; meta=0; qval.units=(uint16_t)pu;}
		void	set(IPIN *ip) {type=VT_REF; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(IPIN*); pin=ip; meta=0;}
		void	set(const PID& pi) {type=VT_REFID; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(PID); id=pi; meta=0;}
		void	set(const RefVID& re) {type=uint8_t(re.eid==STORE_COLLECTION_ID?VT_REFIDPROP:VT_REFIDELT); property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=1; refId=&re; meta=0;}
		void	set(const RefP& re) {type=uint8_t(re.eid==STORE_COLLECTION_ID?VT_REFPROP:VT_REFELT); property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=1; ref=re; meta=0;}
		void	set(Value *coll,uint32_t nValues) {type=VT_ARRAY; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=nValues; varray=coll; meta=0;}
		void	set(INav *nv) {type=VT_COLLECTION; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=1; nav=nv; meta=0;}
		void	setStruct(Value *coll,uint32_t nValues) {type=VT_STRUCT; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=nValues; varray=coll; meta=0;}
		void	set(bool bl) {type=VT_BOOL; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(bool); b=bl; meta=0;}
		void	setDateTime(uint64_t datetime) {type=VT_DATETIME; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(uint64_t); ui64=datetime; meta=0;}
		void	setInterval(int64_t intvl) {type=VT_INTERVAL; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(int64_t); i64=intvl; meta=0;}
		void	set(IStream *strm) {type=VT_STREAM; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=1; stream.is=strm; stream.prefix=NULL; meta=0;}
		void	set(IStmt *stm) {type=VT_STMT; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=1; stmt=stm; meta=0;}
		void	set(IExprTree *ex) {type=VT_EXPRTREE; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=1; exprt=ex; meta=0;}
		void	set(IExpr *ex) {type=VT_EXPR; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=1; expr=ex; meta=0;}
		void	setParam(unsigned char pn,ValueType ty=VT_ANY,unsigned short f=0) {type=VT_VARREF; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=0; refV.refN=pn; refV.type=(unsigned char)ty; refV.flags=(f&~VAR_TYPE_MASK)|VAR_PARAM; refV.id=STORE_INVALID_PROPID; meta=0;}
		void	setVarRef(unsigned char vn,PropertyID id=STORE_INVALID_PROPID,ValueType ty=VT_ANY,unsigned short f=0) {type=VT_VARREF; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=id!=STORE_INVALID_PROPID?1:0; refV.refN=vn; refV.type=(unsigned char)ty; refV.flags=f&~VAR_TYPE_MASK; refV.id=id; meta=0;}
		void	setRange(Value *rng) {type=VT_RANGE; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=2; range=rng; meta=0;}
		void	setURIID(URIID uri) {type=VT_URIID; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(uint32_t); uid=uri; meta=0;}
		void	setIdentity(IdentityID ii) {type=VT_IDENTITY; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(uint32_t); iid=ii; meta=0;}
		void	setNow() {type=VT_CURRENT; i=CVT_TIMESTAMP; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=0; meta=0;}
		void	setCUser() {type=VT_CURRENT; i=CVT_USER; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=0; meta=0;}
		void	setCStore() {type=VT_CURRENT; i=CVT_STORE; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=0; meta=0;}
		void	setEdit(const char *s,uint64_t sht,uint32_t l) {type=VT_STRING; property=STORE_INVALID_PROPID; flags=0; op=OP_EDIT; eid=STORE_COLLECTION_ID; length=s==NULL?0:(uint32_t)strlen(s); edit.str=s; edit.shift=sht; edit.length=l; meta=0;}
		void	setEdit(const char *s,uint32_t nChars,uint64_t sht,uint32_t l) {type=VT_STRING; property=STORE_INVALID_PROPID; flags=0; op=OP_EDIT; eid=STORE_COLLECTION_ID; length=nChars; edit.str=s; edit.shift=sht; edit.length=l; meta=0;}
		void	setEdit(const unsigned char *bs,uint32_t l,uint64_t sht,uint32_t ll) {type=VT_BSTR; property=STORE_INVALID_PROPID; flags=0; op=OP_EDIT; eid=STORE_COLLECTION_ID; length=l; edit.bstr=bs; edit.shift=sht; edit.length=ll; meta=0;}
		void	setPart(const PID& id) {set(id); meta|=META_PROP_PART;}
		void	setPart(IPIN *pin) {set(pin); meta|=META_PROP_PART;} 
		void	setRename(PropertyID from,PropertyID to) {type=VT_URIID; property=from; flags=0; op=OP_RENAME; eid=STORE_COLLECTION_ID; length=sizeof(PropertyID); uid=to; meta=0;}
		void	setDelete(PropertyID p,ElementID ei=STORE_COLLECTION_ID) {type=VT_ANY; property=p; flags=0; op=OP_DELETE; eid=ei; length=0; meta=0;}
		void	setError(PropertyID pid=STORE_INVALID_PROPID) {type=VT_ERROR; property=pid; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=0; meta=0;}

		void		setOp(ExprOp o) {op=uint8_t(o);}
		void		setMeta(uint8_t mt) {meta=mt;}
		void		setPropID(PropertyID p) {property=p;}
		PropertyID	getPropID() const {return property;}
		bool		isFTIndexable() const {return type==VT_STRING || (type==VT_STREAM && stream.is->dataType()==VT_STRING);}
	};

	/**
	 * The main interface to manipulate PINs.
	 * Allows to enumerate all properties of a PIN, as well as to
	 * create, query, modify and delete properties.
	 * @see ISession
	 */
	
	class AFY_EXP IPIN
	{
	public:
		virtual	const PID&	getPID() const  = 0;					/**< returns PIN ID */
		virtual	bool		isLocal() const = 0;					/**< check the PIN is local, i.e. neither replicated, nor remoted cached */
		virtual	bool		isCommitted() const = 0;				/**< PIN is committed to persistent storage, see ISession::commitPINs() */
		virtual	bool		isReadonly() const = 0;					/**< read-only remote cached PIN */
		virtual	bool		canNotify() const = 0;					/**< PIN generates notification messages when modified */
		virtual	bool		isReplicated() const = 0;				/**< PIN is replicated from another store */
		virtual	bool		canBeReplicated() const = 0;			/**< PIN can be replicated to another store */
		virtual	bool		isHidden() const = 0;					/**< hidden PIN - not returned in query results, not indexed, etc. Accessible through it's PIN ID only */
		virtual	bool		isDeleted() const = 0;					/**< PIN is soft-deleted */
		virtual	bool		isClass() const = 0;					/**< PIN represents a class */
		virtual	bool		isDerived() const = 0;					/**< PIN is not stored in DB, rather calculated from other PINs; PIN ID in this case is invalid */
		virtual	bool		isProjected() const = 0;				/**< PIN is a projection of a real PIN */

		virtual	uint32_t	getNumberOfProperties() const = 0;		/**< number of PIN properties */
		virtual	const Value	*getValueByIndex(unsigned idx) const = 0;	/**< get property value by index [0..getNumberOfProperties()-1] */
		virtual	const Value	*getValue(PropertyID pid) const = 0;	/**< get property value by property ID */
		virtual	IPIN		*getSibling() const = 0;				/**< get PIN's sibling (for results of join queries */

		virtual	RC			getPINValue(Value& res) const = 0;		/**< get PIN 'value', based on PROP_SPEC_VALUE */
		virtual uint32_t	getStamp() const = 0;					/**< get PIN stamp */
		virtual	char		*getURI() const = 0;					/**< get PIN URI based on PROP_SPEC_URI */

		virtual	bool		testClassMembership(ClassID,const Value *params=NULL,unsigned nParams=0) const = 0;		/**< test if the PIN is a member of class */
		virtual	bool		defined(const PropertyID *pids,unsigned nProps) const = 0;	/**< test if properties exists in the PIN */
		virtual	RC			isMemberOf(ClassID *&clss,unsigned& nclss) = 0;				/**< returns an array of ClassIDs the PIN is a member of */

		virtual	RC			refresh(bool fNet=true) = 0;			/**< refresh PIN, i.e. re-read properties from persistent storage */
		virtual	IPIN		*clone(const Value *overwriteValues=NULL,unsigned nOverwriteValues=0,unsigned mode=0) = 0;	/**< clone PIN (optionally modifying some properties) */
		virtual	IPIN		*project(const PropertyID *properties,unsigned nProperties,const PropertyID *newProps=NULL,unsigned mode=0) = 0;	/**< project PIN */
		virtual	RC			modify(const Value *values,unsigned nValues,unsigned mode=0,	/**< modify PIN, can be applied to uncommitted PINs */
									const ElementID *eids=NULL,unsigned *pNFailed=NULL) = 0;

		virtual	RC			setExpiration(uint32_t) = 0;			/**< set expiration time for cached remote PINs */
		virtual	RC			setNotification(bool fReset=false) = 0;	/**< switch notification generation on or off for this PIN */
		virtual	RC			setReplicated() = 0;					/**< set 'replicatable' status */
		virtual	RC			setNoIndex() = 0;						/**< exclude from indexes */
		virtual	RC			deletePIN() = 0;						/**< soft-delete PIN from persistent stoarge */
		virtual	RC			undelete() = 0;							/**< undelete soft-deleted PIN */

		virtual	void		destroy() = 0;							/**< destroy this IPIN (doesn't delete it from DB) */
	};

	/**
	 * QName declaration structure; fDel for internal use only
	 */
	struct AFY_EXP QName
	{
		const	char	*qpref;
		size_t			lq;
		const	char	*str;
		size_t			lstr;
		bool			fDel;
	};

	/**
	 * compilation error information: type of error, position, etc.
	 * @see createXXX() functions
	 */
	struct AFY_EXP CompilationError
	{
		RC				rc;
		const	char	*msg;
		unsigned		line;
		unsigned		pos;
	};

	/**
	 * Expression tree node interface
	 * @see ISession::expr()
	 */
	class AFY_EXP IExprTree 
	{
	public:
		virtual ExprOp			getOp() const = 0;
		virtual	unsigned		getNumberOfOperands() const = 0;
		virtual	const	Value&	getOperand(unsigned idx) const = 0;
		virtual	unsigned		getFlags() const = 0;
		virtual	void			setFlags(unsigned flags,unsigned mask) = 0;
		virtual	IExpr			*compile() = 0;
		virtual	char			*toString(unsigned mode=0,const QName *qNames=NULL,unsigned nQNames=0) const = 0;
		virtual IExprTree		*clone() const = 0;
		virtual	void			destroy() = 0;
	};
	
	/**
	 * Compiled expression interface
	 * @see IExprTree::compile()
	 */
	class AFY_EXP IExpr 
	{
	public:
		virtual	RC			execute(Value& res,const Value *params=NULL,unsigned nParams=0) const = 0;
		virtual	char		*toString(unsigned mode=0,const QName *qNames=NULL,unsigned nQNames=0) const = 0;
		virtual IExpr		*clone() const = 0;
		virtual	void		destroy() = 0;
	};

	/**
	 * Query results iterator interface
	 */
	class AFY_EXP ICursor
	{
	public:
		virtual	RC			next(Value &ret) = 0;
		virtual	RC			next(PID&) = 0;
		virtual	IPIN		*next() = 0;					// obsolete
		virtual	RC			rewind() = 0;
		virtual	uint64_t	getCount() const = 0;
		virtual	void		destroy() = 0;
	};

	/**
	 * Output protobuf stream interface
	 */
	class AFY_EXP IStreamOut
	{
	public:
		virtual	RC		next(unsigned char *buf,size_t& lBuf) = 0;
		virtual	void	destroy() = 0;
	};
	
	/**
	 * Input protobuf stream interface
	 */
	class AFY_EXP IStreamIn
	{
	public:
		virtual	RC		next(const unsigned char *buf,size_t lBuf) = 0;
		virtual	void	destroy() = 0;
	};

	/**
	 * Free-text search flags
	 */
	#define	QFT_FILTER_SW		0x0001
	#define	QFT_RET_NO_DOC		0x0002
	#define	QFT_RET_PARTS		0x0004
	#define	QFT_RET_NO_PARTS	0x0008

	/**
	 * Query variable types
	 */
	enum QUERY_SETOP
	{
		QRY_SEMIJOIN, QRY_JOIN, QRY_LEFT_OUTER_JOIN, QRY_RIGHT_OUTER_JOIN, QRY_FULL_OUTER_JOIN, QRY_UNION, QRY_EXCEPT, QRY_INTERSECT, QRY_ALL_SETOP
	};

	#define	CLASS_PARAM_REF		0x80000000		/**< classID field in ClassSpec specifies parameter number containing actual ClassID */
	#define	CLASS_UNIQUE		0x40000000		/**< for commitPINs(): check uniqueness of data in this family before insert */

	/**
	 * Class specification for query variables
	 */
	struct AFY_EXP ClassSpec
	{
		ClassID			classID;			/**< class ID */
		unsigned		nParams;			/**< number of parameters for family reference */
		const	Value	*params;			/**< parameter values for family reference */
	};

	/**
	 * Path segment specification for query expressions and variables
	 */
	struct AFY_EXP PathSeg
	{
		PropertyID		pid;				/**< property ID */
		ElementID		eid;				/**< collection element ID, may be STORE_FIRST_ELEMENT or STORE_LAST_ELEMENT; if is equal to STORE_COLLECTION_ID the whole collection is used */
		IExpr			*filter;			/**< filter expression; applied to PINs refered by this segment collection or single reference */
		ClassID			cid;				/**< class the above PINs must be members of */
		unsigned		rmin;				/**< minimum number of repetitions of this segment in valid path */
		unsigned		rmax;				/**< maximum number of repetitions of this segment in valid path */
		bool			fLast;				/**< filtering must be applied only to the last segment */
	};

	/**
	 * Ordering modifier flags
	 */
	#define	ORD_DESC			0x01		/**< descending order */
	#define	ORD_NCASE			0x02		/**< case insensitive comparison of VT_STRING data */
	#define	ORD_NULLS_BEFORE	0x04		/**< NULLs are sorted before non-null data */
	#define	ORD_NULLS_AFTER		0x08		/**< NULLS are sorted after non-null data */

	/**
	 * Ordering segment specification
	 */
	struct AFY_EXP OrderSeg
	{
		IExprTree	*expr;					/**< ordering expression, must be NULL is pid is set */
		PropertyID	pid;					/**< ordering property, must be STORE_INVALID_PROPID if expr is not NULL */
		uint8_t		flags;					/**< bit flags (see ORD_XXX above) */
		uint8_t		var;					/**< in joins, variable number */
		uint16_t	lPrefix;				/**< prefix length to be used for comparisons of VT_STRING data */
	};

	/**
	 * Query variable identifier
	 */
	typedef	unsigned	char	QVarID;
	#define	INVALID_QVAR_ID		0xFF

	/**
	 * Type of statement
	 */
	enum STMT_OP
	{
		STMT_QUERY, STMT_INSERT, STMT_UPDATE, STMT_DELETE, STMT_UNDELETE, STMT_START_TX, STMT_COMMIT, STMT_ROLLBACK, STMT_ISOLATION, STMT_PREFIX, STMT_OP_ALL
	};
	
	/**
	 * DISTINCT type
	 */
	enum DistinctType
	{
		DT_DEFAULT, DT_ALL, DT_DISTINCT
	};

	/**
	 * Statement execution callback for aynchronous query execution
	 * @see IStmt::asyncexec()
	 */
	class AFY_EXP IStmtCallback
	{
	public:
		virtual	void	result(ICursor *res,RC rc) = 0;
	};

	/**
	 * Main query, DML and DDL statement interface
	 */
	class AFY_EXP IStmt
	{
	public:
		virtual QVarID	addVariable(const ClassSpec *classes=NULL,unsigned nClasses=0,IExprTree *cond=NULL) = 0;		/**< add varaible either for full scan (no ClassSpec specified) or for intersection of given calsses/families with optional condition */
		virtual QVarID	addVariable(const PID& pid,PropertyID propID,IExprTree *cond=NULL) = 0;							/**< add collection scanning variable */
		virtual QVarID	addVariable(IStmt *qry) = 0;																	/**< add sub-query variable (e.g. nested SELECT in FROM */
		virtual	QVarID	setOp(QVarID leftVar,QVarID rightVar,QUERY_SETOP) = 0;											/**< add set operation (QRY_UNION, QRY_INTERSECT, QRY_EXCEPT) variable for 2 sub-variables*/
		virtual	QVarID	setOp(const QVarID *vars,unsigned nVars,QUERY_SETOP) = 0;										/**< add set operation (QRY_UNION, QRY_INTERSECT, QRY_EXCEPT) variable for multiple sub-variables*/
		virtual	QVarID	join(QVarID leftVar,QVarID rightVar,IExprTree *cond=NULL,QUERY_SETOP=QRY_SEMIJOIN,PropertyID=STORE_INVALID_PROPID) = 0;		/**< set join variable for 2 sub-variables with optional condition */
		virtual	QVarID	join(const QVarID *vars,unsigned nVars,IExprTree *cond=NULL,QUERY_SETOP=QRY_SEMIJOIN,PropertyID=STORE_INVALID_PROPID) = 0;	/**< set join variable for multiple sub-variables with optional condition */
		virtual	RC		setName(QVarID var,const char *name) = 0;														/**< set variable name string to be used in query rendering (i.e. FROM ... AS name ) */
		virtual	RC		setDistinct(QVarID var,DistinctType) = 0;														/**< set DISTINCT mode for a variable */
		virtual	RC		addOutput(QVarID var,const Value *dscr,unsigned nDscr) = 0;										/**< add output descriptior */
		virtual RC		addCondition(QVarID var,IExprTree *cond) = 0;													/**< add a condition (part of WHERE clause); multiple conditions are AND-ed */
		virtual	RC		addConditionFT(QVarID var,const char *str,unsigned flags=0,const PropertyID *pids=NULL,unsigned nPids=0) = 0;	/**< add free-text search string and optional property filter for a simple variable */
		virtual	RC		setPIDs(QVarID var,const PID *pids,unsigned nPids) = 0;											/**< set PIN ID array filter for a simple (not-join, not-set) variable */
		virtual	RC		setPath(QVarID var,const PathSeg *segs,unsigned nSegs) = 0;										/**< set path expression for a simple variable */
		virtual	RC		setPropCondition(QVarID var,const PropertyID *props,unsigned nProps,bool fOr=false) = 0;		/**< set property existence condition, i.e. EXISTS(prop1) AND EXISTS(prop2) ... */
		virtual	RC		setJoinProperties(QVarID var,const PropertyID *props,unsigned nProps) = 0;						/**< set properties for natural join or USING(...) */
		virtual	RC		setGroup(QVarID,const OrderSeg *order,unsigned nSegs,IExprTree *having=NULL) = 0;				/**< set GROUP BY clause */
		virtual	RC		setOrder(const OrderSeg *order,unsigned nSegs) = 0;												/**< set statement-wide result ordering */
		virtual	RC		setValues(const Value *values,unsigned nValues,const ClassSpec *into=NULL,unsigned nInto=0,unsigned mode=0) =  0;		/**< set Value structures, class filter and PIN flags for STMT_INSERT and STMT_UPDATE statements */

		virtual	STMT_OP	getOp() const = 0;																										/**< get statement type */

		virtual	RC		execute(ICursor **result=NULL,const Value *params=NULL,unsigned nParams=0,unsigned nProcess=~0u,unsigned nSkip=0,		/**< execute statement, return result set iterator or number of affected PINs */
										unsigned long mode=0,uint64_t *nProcessed=NULL,TXI_LEVEL=TXI_DEFAULT) const = 0;
		virtual	RC		asyncexec(IStmtCallback *cb,const Value *params=NULL,unsigned nParams=0,unsigned nProcess=~0u,unsigned nSkip=0,			/**< execute statement asynchronously */
										unsigned long mode=0,TXI_LEVEL=TXI_DEFAULT) const = 0;
		virtual	RC		execute(IStreamOut*& result,const Value *params=NULL,unsigned nParams=0,unsigned nProcess=~0u,unsigned nSkip=0,			/**< execute statement, return results as a protobuf stream */
										unsigned long mode=0,TXI_LEVEL=TXI_DEFAULT) const = 0;
		virtual	RC		count(uint64_t& cnt,const Value *params=NULL,unsigned nParams=0,unsigned long nAbort=~0u,								/**< count statement results */
										unsigned long mode=0,TXI_LEVEL=TXI_DEFAULT) const = 0;
		virtual	RC		exist(const Value *params=NULL,unsigned nParams=0,unsigned long mode=0,TXI_LEVEL=TXI_DEFAULT) const = 0;				/**< check if any PINs satfisfy query conditions */
		virtual	RC		analyze(char *&plan,const Value *pars=NULL,unsigned nPars=0,unsigned long md=0) const = 0;								/**< return execution plan for the statement */

		virtual	bool	isSatisfied(const IPIN *pin,const Value *pars=NULL,unsigned nPars=0,unsigned long mode=0) const = 0;					/**< check if a given PIN satisfies query conditions */
		virtual	char	*toString(unsigned mode=0,const QName *qNames=NULL,unsigned nQNames=0) const = 0;										/**< convert statement to PathSQL string */

		virtual	IStmt	*clone(STMT_OP=STMT_OP_ALL) const = 0;															/**< clone IStmt object */
		virtual	void	destroy() = 0;																					/**< destroy IStmt object */
	};

	/**
	 * Full identification of properties.
	 * @see ISession::mapURIs()
	 */
	struct URIMap
	{
		const		char	*URI;
		URIID				uid;
	};

	/**
	 * PIN page allocation control structure
	 */
	struct AllocCtrl
	{
		unsigned long	arrayThreshold;
		size_t			ssvThreshold;
		float			pctPageFree;
	};

	/**
	 * Index navigator
	 */
	class IndexNav
	{
	public:
		virtual	RC		next(PID& id,GO_DIR=GO_NEXT) = 0;
		virtual	RC		position(const Value *pv,unsigned nValues) = 0;
		virtual	RC		getValue(const Value *&pv,unsigned& nValues) = 0;
		virtual	const Value	*next() = 0;
		virtual	unsigned nValues() = 0;
		virtual	void	destroy() = 0;
	};

	/**
	 * Free-text index scanning interface
	 */
	class StringEnum
	{
	public:
		virtual	const char	*next() = 0;
		virtual	void		destroy() = 0;
	};

	/**
	 * Query execution trace interface
	 */
	class ITrace
	{
	public:
		virtual	void	trace(long code,const char *msg,va_list) = 0;
	};

	/**
	 * The session encapsulates a client's connection to the store.
	 * Through it, the client can create, query, modify and delete PINs,
	 * in a transactioned manner.  This is also where the client
	 * can map PropIDs to full property information (e.g. names).
	 * Note:
	 *  A command executed outside a transaction is implicitly
	 *  executed in its own transaction.
	 * @see IPIN
	 */
	class AFY_EXP ISession
	{
		void	operator	delete(void*) {}		/**< ISession delete is forbidden */
	public:
		static	ISession	*startSession(AfyDBCtx,const char *identityName=NULL,const char *password=NULL);	/**< start new session, optional login */
		virtual	ISession	*clone(const char * =NULL) const = 0;												/**< clone existing session */
		virtual	RC			attachToCurrentThread() = 0;														/**< in server: attach ISession to current worker thread (ISession can be accessed concurrently only from one thread */
		virtual	RC			detachFromCurrentThread() = 0;														/**< in server: detach ISession from current worker thread */

		virtual	void		setInterfaceMode(unsigned flags) = 0;												/**< set interface mode, see ITF_XXX flags above */
		virtual	unsigned	getInterfaceMode() const = 0;														/**< get current interface mode */
		virtual	RC			setURIBase(const char *URIBase) = 0;												/**< set session-wide BASE for URIs; appended to all 'simple' property and class names */
		virtual	RC			addURIPrefix(const char *name,const char *URIprefix) = 0;							/**< add session-wide PREFIX for QNames */
		virtual	void		setDefaultExpiration(uint64_t defExp) = 0;											/**< set default expiration time for cached remore PINs */
		virtual	void		changeTraceMode(unsigned mask,bool fReset=false) = 0;								/**< change query execution trace mode, see TRACE_XXX flags above  */
		virtual	void		setTrace(ITrace *) = 0;																/**< set query execution trace interface */
		virtual	void		terminate() = 0;																	/**< terminate session; drops the session */

		virtual	RC			mapURIs(unsigned nURIs,URIMap URIs[]) = 0;											/**< maps URI strings to their internal URIID values used throughout the interface */
		virtual	RC			getURI(uint32_t,char buf[],size_t& lbuf) = 0;										/**< get URI string by its URIID value */

		virtual	IdentityID	getCurrentIdentityID() const = 0;													/**< get the identity the current session is running with */
		virtual	IdentityID	getIdentityID(const char *identity) = 0;											/**< get ID for an identity; the identity must be stored by calling storeIdentity() */
		virtual	RC			impersonate(const char *identity) = 0;												/**< this session impresonates given identity; can be called only for OWNER sessions */
		virtual	IdentityID	storeIdentity(const char *identity,const char *pwd,bool fMayInsert=true,const unsigned char *cert=NULL,unsigned lcert=0) = 0;	/**< store identity information, including password, insert permission, optional certificate */
		virtual	IdentityID	loadIdentity(const char *identity,const unsigned char *pwd,unsigned lPwd,bool fMayInsert=true,const unsigned char *cert=NULL,unsigned lcert=0) = 0; /**< load identity information */
		virtual	RC			setInsertPermission(IdentityID,bool fMayInsert=true) = 0;							/**< set insert premission for existing identity */
		virtual	size_t		getStoreIdentityName(char buf[],size_t lbuf) = 0;									/**< get identity name of the store owner */
		virtual	size_t		getIdentityName(IdentityID,char buf[],size_t lbuf) = 0;								/**< get identity name for a stored identity */
		virtual	size_t		getCertificate(IdentityID,unsigned char buf[],size_t lbuf) = 0;						/**< get identity certificate, if any */
		virtual	RC			changePassword(IdentityID,const char *oldPwd,const char *newPwd) = 0;				/**< change identity password */
		virtual	RC			changeCertificate(IdentityID,const char *pwd,const unsigned char *cert,unsigned lcert) = 0;	/**< change identity certificate */
		virtual	RC			changeStoreIdentity(const char *newIdentity) = 0;									/**< change identity of the store owner */

		virtual	unsigned	getStoreID(const PID& ) = 0;														/**< get ID (number from 0 t0 65535) of store the PIN with this ID was created in */
		virtual	unsigned	getLocalStoreID() = 0;																/**< get ID of this store */

		virtual	IExprTree	*expr(ExprOp op,unsigned nOperands,const Value *operands,unsigned flags=0) = 0;		/**< create expression tree node */

		virtual	IStmt		*createStmt(STMT_OP=STMT_QUERY,unsigned mode=0) = 0;								/**< create IStmt object */
		virtual	IStmt		*createStmt(const char *queryStr,const URIID *ids=NULL,unsigned nids=0,CompilationError *ce=NULL) = 0;	/**< create IStmt from PathSQL string */
		virtual	IExprTree	*createExprTree(const char *str,const URIID *ids=NULL,unsigned nids=0,CompilationError *ce=NULL) = 0;	/**< create expression tree from PathSQL string */
		virtual	IExpr		*createExpr(const char *str,const URIID *ids=NULL,unsigned nids=0,CompilationError *ce=NULL) = 0;		/**< create compiled expression from PathSQL string */
		virtual	IExpr		*createExtExpr(uint16_t langID,const unsigned char *body,uint32_t lBody,uint16_t flags) = 0;			/**< create 'external expression' for loadable language libraries */
		virtual	RC			getTypeName(ValueType type,char buf[],size_t lbuf) = 0;
		virtual	void		abortQuery() = 0;

		virtual	RC			execute(const char *str,size_t lstr,char **result=NULL,const URIID *ids=NULL,unsigned nids=0,			/**< execute query specified by PathSQL string; return result in JSON form */
									const Value *params=NULL,unsigned nParams=0,CompilationError *ce=NULL,uint64_t *nProcessed=NULL,
									unsigned nProcess=~0u,unsigned nSkip=0) = 0;

		virtual	RC			createInputStream(IStreamIn *&in,IStreamIn *out=NULL,size_t lbuf=0) = 0;			/**< create protobuf input stream interface */

		virtual	RC			getClassID(const char *className,ClassID& cid) = 0;									/**< get ClassID for gived class URI; equivalent to mapURIs() but check class existence */
		virtual	RC			enableClassNotifications(ClassID,unsigned notifications) = 0;						/**< enables notifications for a given class, see CLASS_NOTIFY_XXX above */
		virtual	RC			rebuildIndices(const ClassID *cidx=NULL,unsigned nClasses=0) = 0;					/**< rebuild all DB indices (except free-text index) */
		virtual	RC			rebuildIndexFT() = 0;																/**< rebuild free-text index */
		virtual	RC			createIndexNav(ClassID,IndexNav *&nav) = 0;											/**< create IndexNav object */
		virtual	RC			listValues(ClassID cid,PropertyID pid,IndexNav *&ven) = 0;							/**< list all stored values for a given class family */
		virtual	RC			listWords(const char *query,StringEnum *&sen) = 0;									/**< list all words in FT index matching given prefix or list of words */
		virtual	RC			getClassInfo(ClassID,IPIN*&) = 0;													/**< get class information */

		virtual	IPIN		*getPIN(const PID& id,unsigned mode=0) = 0;											/**< retrieve a PIN by its ID */
		virtual	IPIN		*getPIN(const Value& ref,unsigned mode=0) = 0;										/**< retrive a PIN by its reference in a Value */
		virtual	IPIN		*getPINByURI(const char *uri,unsigned mode=0) = 0;									/**< retrieve a PIN by its URI (not implemented yet) */
		virtual	RC			getValues(Value *vals,unsigned nVals,const PID& id) = 0;							/**< get PIN property values by its ID */
		virtual	RC			getValue(Value& res,const PID& id,PropertyID,ElementID=STORE_COLLECTION_ID) = 0;	/**< get property value for a value reference */
		virtual	RC			getValue(Value& res,const PID& id) = 0;												/**< get PIN value based on its PROP_SPEC_VALUE */
		virtual	RC			getPINClasses(ClassID *&clss,unsigned& nclss,const PID& id) = 0;					/**< array of ClassIDs the PIN is a member of */
		virtual	bool		isCached(const PID& id) = 0;														/**< check if a PIN is in remote PIN cache by its ID */
		virtual	RC			createPIN(PID& res,const Value values[],unsigned nValues,unsigned mode=0,const AllocCtrl* =NULL) = 0;		/**< create and commit PIN */
		virtual	IPIN		*createUncommittedPIN(Value* values=0,unsigned nValues=0,unsigned mode=0,const PID *original=NULL) = 0;		/**< create uncommitted PIN, to be used in commitPINs() */
		virtual	RC			commitPINs(IPIN *const *pins,unsigned nPins,unsigned mode=0,const AllocCtrl* =NULL,const Value *params=NULL,unsigned nParams=0,const ClassSpec *into=NULL,unsigned nInto=0) = 0;	/**< commit to persistent memory a set of uncommitted PINs; assignes PIN IDs */
		virtual	RC			modifyPIN(const PID& id,const Value *values,unsigned nValues,unsigned mode=0,const ElementID *eids=NULL,unsigned *pNFailed=NULL,const Value *params=NULL,unsigned nParams=0) = 0;	/**< modify committed or uncommitted PIN */
		virtual	RC			deletePINs(IPIN **pins,unsigned nPins,unsigned mode=0) = 0;							/**< (soft-)delete or purge committed PINs from persistent memory */
		virtual	RC			deletePINs(const PID *pids,unsigned nPids,unsigned mode=0) = 0;						/**< (soft-)delete or purge committed PINs from persistent memory by their IDs */
		virtual	RC			undeletePINs(const PID *pids,unsigned nPids) = 0;									/**< undelete soft-deleted PINs */
		virtual	RC			setPINAllocationParameters(const AllocCtrl *ac) = 0;								/**< set session-wide page allocation parameters for new PINs */

		virtual	RC			setIsolationLevel(TXI_LEVEL) = 0;													/**< set session-wide default isolation level */
		virtual	RC			startTransaction(TX_TYPE=TXT_READWRITE,TXI_LEVEL=TXI_DEFAULT) = 0;					/**< start transaction, READ-ONLY or READ_WRITE */
		virtual	RC			commit(bool fAll=false) = 0;														/**< commit transaction */
		virtual	RC			rollback(bool fAll=false) = 0;														/**< rollback (abort) transaction */

		virtual	RC			reservePage(uint32_t) = 0;															/**< used in dump/load entire store */

		virtual	RC			copyValue(const Value& src,Value& dest) = 0;										/**< copy Value to session memeory */
		virtual	RC			convertValue(const Value& oldValue,Value& newValue,ValueType newType) = 0;			/**< convert Value type */
		virtual	RC			parseValue(const char *p,size_t l,Value& res,CompilationError *ce=NULL) = 0;		/**< parse a string to Value */
		virtual	RC			parseValues(const char *p,size_t l,Value *&res,unsigned& nValues,CompilationError *ce=NULL,char delimiter=',') = 0;	/**< parse a string containing multiple values in PathSQL format */
		virtual	int			compareValues(const Value& v1,const Value& v2,bool fNCase=false) = 0;				/**< compare 2 Values (can be of different types) */
		virtual	void		freeValues(Value *vals,unsigned nVals) = 0;											/**< free an array of Value structures allocated in session memory */
		virtual	void		freeValue(Value& val) = 0;															/**< free a Value structure with data allocation in session memory */

		virtual	void		setTimeZone(int64_t tzShift) = 0;													/**< set session time zone */
		virtual	RC			convDateTime(uint64_t dt,DateTime& dts,bool fUTC=true) const = 0;					/**< convert timestamp from internal representation to DateTiem structure */
		virtual	RC			convDateTime(const DateTime& dts,uint64_t& dt,bool fUTC=true) const = 0;			/**< convert tiemstamp from DateTime to internal representation */

		virtual	RC			setStopWordTable(const char **words,uint32_t nWords,PropertyID pid=STORE_INVALID_PROPID,	/**< set optional session-wide table of stop-words for FT indexing */
															bool fOverwriteDefault=false,bool fSessionOnly=false) = 0;

		virtual	void		*alloc(size_t) = 0;																	/**< allocate a block in session memory */
		virtual	void		*realloc(void *,size_t) = 0;														/**< re-allocate a block in session memory */
		virtual	void		free(void *) = 0;																	/**< free block of session memory */
	};

	/**
	 * Single-threaded server interface
	 */
	class AFY_EXP IConnection
	{
	public:
		static	IConnection	*create(AfyDBCtx,const char *identityName=NULL,const char *password=NULL);
		virtual	RC			setURIBase(const char *URIBase) = 0;
		virtual	RC			addURIPrefix(const char *name,const char *URIprefix) = 0;
		virtual	void		close() = 0;

		virtual	RC			process(const unsigned char *buf,size_t lBuf) = 0;
		virtual	RC			read(unsigned char *buf,size_t lBuf,size_t& lRead) = 0;

		virtual	RC			setIsolationLevel(TXI_LEVEL) = 0;
		virtual	RC			startTransaction(TX_TYPE=TXT_READWRITE,TXI_LEVEL=TXI_DEFAULT) = 0;
		virtual	RC			commit(bool fAll=false) = 0;
		virtual	RC			rollback(bool fAll=false) = 0;

	};
}

#endif
