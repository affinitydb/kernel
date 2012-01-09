/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _STORE_H_
#define _STORE_H_

#undef MV_EXP
#ifndef _MSC_VER
#define MV_EXP
#else
#define MV_EXP	_declspec(dllexport)
#endif
#include <stdint.h>
#include <stddef.h>
#include <stdarg.h>
#include <string.h>

#include "rc.h"
#include "units.h"

using namespace MVStoreRC;

namespace MVStoreKernel {class StoreCtx;}

typedef MVStoreKernel::StoreCtx *MVStoreCtx;

/**
 * Namespace to encapsulate the public interface of mvstore.
 * Overview:
 *  ISession represents a client's connection to the store.
 *  IPIN represents a PIN, i.e. a node of information.
 *  Through its ISession, a client can create, query, modify and delete PINs, control transactions, set session parameters, etc.
 *  Through IPIN, one can create, query, modify and delete properties or clone the PIN optionally overwriting properties.
 */
namespace MVStore 
{
	typedef	uint32_t					IdentityID;	/**< Identity ID - used to represent the identity in this store */
	typedef	uint32_t					URIID;		/**< URI ID - used to represent a URI in this store */
	typedef	URIID						ClassID;	/**< Class ID - used to represent the class URI in this store */
	typedef	URIID						PropertyID;	/**< Property ID - same as URIID */
	typedef	uint32_t					ElementID;	/**< Immutable collection element ID */
	typedef	uint32_t					VersionID;	/**< Version number */

	#define	STORE_INVALID_PID			0ULL
	#define	STORE_INVALID_PROPID		(~MVStore::PropertyID(0))	
	#define	STORE_INVALID_IDENTITY		(~MVStore::IdentityID(0))	
	#define	STORE_INVALID_CLASSID		(~MVStore::ClassID(0))	
	#define	STORE_CURRENT_VERSION		(~MVStore::VersionID(0))	
	#define	STORE_MAX_URIID				MVStore::URIID(0x1FFFFFFF)		/**< Maximum value for URIID */

	/**
	 * special identity ID
	 */
	#define	STORE_OWNER					MVStore::IdentityID(0)			/**< The owner of the store always has IdentityID equals to 0 */


	/**
	 * special property and class IDs
	 */
	#define	STORE_CLASS_OF_CLASSES		MVStore::ClassID(0)				/**< Fixed ID of the class of all classes in the store */
	#define	PROP_SPEC_PINID				MVStore::PropertyID(1)			/**< PIN id as PIN's property, immutable */
	#define	PROP_SPEC_DOCUMENT			MVStore::PropertyID(2)			/**< document PIN this PIN is a part of */
	#define	PROP_SPEC_PARENT			MVStore::PropertyID(3)			/**< parent (whole) PIN */
	#define	PROP_SPEC_VALUE				MVStore::PropertyID(4)			/**< value of the PIN (can be an expression using other properties */
	#define	PROP_SPEC_CREATED			MVStore::PropertyID(5)			/**< PIN creation timestamp, immutable */
	#define	PROP_SPEC_CREATEDBY			MVStore::PropertyID(6)			/**< identity created the PIN, immutable */
	#define	PROP_SPEC_UPDATED			MVStore::PropertyID(7)			/**< timestamp of the latest PIN modification, updated automatically */
	#define	PROP_SPEC_UPDATEDBY			MVStore::PropertyID(8)			/**< identity of the latest PIN modification, updated automatically */
	#define	PROP_SPEC_ACL				MVStore::PropertyID(9)			/**< ACL - can be a collection and contain references to other PINs */
	#define	PROP_SPEC_URI				MVStore::PropertyID(10)			/**< External (RDF) URI of a pin, class or relation */
	#define	PROP_SPEC_STAMP				MVStore::PropertyID(11)			/**< stamp (unsigned integer) of the latest modification, updated automatically */
	#define	PROP_SPEC_CLASSID			MVStore::PropertyID(12)			/**< classID of the class represented by this pin (VT_CLASSID) */
	#define PROP_SPEC_PREDICATE			MVStore::PropertyID(13)			/**< predicate of a class or relations (VT_STMT) */
	#define	PROP_SPEC_NINSTANCES		MVStore::PropertyID(14)			/**< number of instances of a given class currently in the store */
	#define	PROP_SPEC_NDINSTANCES		MVStore::PropertyID(15)			/**< number of soft-deleted instances of a given class currently in the store */
	#define	PROP_SPEC_SUBCLASSES		MVStore::PropertyID(16)			/**< collection of classes which are specializations of this class */
	#define	PROP_SPEC_SUPERCLASSES		MVStore::PropertyID(17)			/**< collection of classes which are abstractions of this class */
	#define PROP_SPEC_CLASS_INFO		MVStore::PropertyID(18)			/**< bit flags describing the class (VT_UINT) */
	#define PROP_SPEC_INDEX_INFO		MVStore::PropertyID(19)			/**< Family index information */
	#define PROP_SPEC_PROPERTIES		MVStore::PropertyID(20)			/**< Properties refered by the class */
	#define	PROP_SPEC_JOIN_TRIGGER		MVStore::PropertyID(21)			/**< Triggers associated with class events */
	#define	PROP_SPEC_UPDATE_TRIGGER	MVStore::PropertyID(22)			/**< Triggers associated with class events */
	#define	PROP_SPEC_LEAVE_TRIGGER		MVStore::PropertyID(23)			/**< Triggers associated with class events */
	#define	PROP_SPEC_REFID				MVStore::PropertyID(24)			/**< Future implementation */
	#define	PROP_SPEC_KEY				MVStore::PropertyID(25)			/**< Future implementation */
	#define	PROP_SPEC_VERSION			MVStore::PropertyID(26)			/**< Chain of versions */
	#define	PROP_SPEC_WEIGHT			MVStore::PropertyID(27)			/**< Future implementation */
	#define	PROP_SPEC_PROTOTYPE			MVStore::PropertyID(28)			/**< JavaScript-like inheritance */
	#define	PROP_SPEC_WINDOW			MVStore::PropertyID(29)			/**< Stream windowing control (size of class-related window) */

	#define	PROP_SPEC_MAX				PROP_SPEC_WINDOW
	
	#define	PROP_SPEC_ANY				(~MVStore::PropertyID(0))		/**< used in queries, matches any property */

	/**
	 * special collection element IDs
	 */
	#define	STORE_COLLECTION_ID			(~MVStore::ElementID(0))		/**< Singleton value or operation applied to the whole collection */
	#define	STORE_LAST_ELEMENT			(~MVStore::ElementID(1))		/**< Last element of a collection */
	#define	STORE_FIRST_ELEMENT			(~MVStore::ElementID(2))		/**< First element of a collection */
	#define	STORE_SUM_COLLECTION		(~MVStore::ElementID(3))		/**< IStmt: replace collection with the sum of its elements */
	#define	STORE_AVG_COLLECTION		(~MVStore::ElementID(4))		/**< IStmt: replace collection with the average of its elements */
	#define	STORE_CONCAT_COLLECTION		(~MVStore::ElementID(5))		/**< IStmt: replace collection with the concatenation of its elements */
	#define	STORE_MIN_ELEMENT			(~MVStore::ElementID(6))		/**< IStmt: replace collection with the minimum of its elements */
	#define	STORE_MAX_ELEMENT			(~MVStore::ElementID(7))		/**< IStmt: replace collection with the maximum of its elements */
	#define	STORE_SOME_ELEMENT			(~MVStore::ElementID(8))		/**< IStmt: existential quantifier for the collection */
	#define	STORE_ALL_ELEMENTS			(~MVStore::ElementID(9))		/**< IStmt: 'for all' quantifier */

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
	#define	MODE_PURGE_IDS				0x00100000	/**< in deletePINs(): purge pins and reuse their slots on pages (so their IDs are not unique) */
	#define	MODE_PART					0x00100000	/**< in nested insert: create a part */
	#define	MODE_CHECK_STAMP			0x00200000	/**< forces stamp check before modification; if stamp changed the op is aborted and RC_REPEAT is returned */
	#define	MODE_HOLD_RESULT			0x00200000	/**< for IStmt::execute(): don't close result set on transaction commit/rollback */
	#define	MODE_ALL_WORDS				0x00400000	/**< all words must be present in FT condition */
	#define	MODE_DELETED				0x00800000	/**< for query: return only (soft)deleted pins */
	#define	MODE_SSV_AS_STREAM			0x01000000	/**< for getPIN(), IStmt::execute() - return SSVs as streams instead of strings */
	#define	MODE_FORCED_SSV_AS_STREAM	0x02000000	/**< for getPIN(), IStmt::execute(): return only META_PROP_SSTORAGE SSVs as streams instead of strings */
	#define	MODE_VERBOSE				0x04000000	/**< in IStmt::execute(): print evaluation plan */

	/**
	 * PIN creation flags
	 */
	#define	PIN_NO_REPLICATION			0x40000000	/**< marks a pin as one which shouldn't be replicated (only when the pin is committed) */
	#define	PIN_NO_INDEX				0x20000000	/**< special pin - no indexing */
	#define	PIN_NOTIFY					0x10000000	/**< pin generates notifications */
	#define	PIN_REPLICATED				0x08000000	/**< pin is replicated to another store or is a replica */
	#define	PIN_HIDDEN					0x04000000	/**< special pin - totally hidden from search/replication - accessible only through its PID */

	/**
	 * meta-properties
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
	#define	CLASS_UNIQUE				0x0010		/**< Unique family */

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
	enum ExtractType
	{
		EY_FRACTIONAL, EY_SECOND, EY_MINUTE, EY_HOUR, EY_DAY, EY_WDAY, EY_MONTH, EY_YEAR,
		EY_IDENTITY,
	};

	/**
	 * Supported value types
	 * @see Value
	 */
	enum ValueType 
	{
		VT_ERROR,VT_ANY=VT_ERROR,		/**< used to report errors, non-existant properties; VT_ANY designates no conversion in transforms */

		VT_INT,							/**< 32-bit signed integer */
		VT_UINT,						/**< 32-bit unsigned integer */
		VT_INT64,						/**< 64-bit signed integer */
		VT_UINT64,						/**< 64-bit unsigned integer */
		VT_DECIMAL,						/**< decimal number (for some SQL databases compatibility) */
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
		VT_ENUM,						/**< an enumeration value 'by value' i.e. 32-bit unsigned integer */

		VT_REF,							/**< a reference to another IPIN */
		VT_REFID,						/**< a reference to another PIN by its PID */
		VT_REFPROP,						/**< a reference to a value (property of this or another PIN) by IPIN & PropertyID */
		VT_REFIDPROP,					/**< a reference to a value (property of this or another PIN) by its PID/PropertyID */
		VT_REFELT,						/**< a reference to a collection element by its IPIN/PropertyID/ElementID */
		VT_REFIDELT,					/**< a reference to a collection element by its PID/PropertyID/ElementID */

		VT_EXPR,						/**< stored expression */
		VT_STMT,						/**< IStmt object */

		VT_ARRAY,						/**< a collection property, i.e. a property composed of sub-properties */
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
		OP_PLUS=OP_FIRST_EXPR,
		OP_MINUS,
		OP_MUL,
		OP_DIV,
		OP_MOD,
		OP_NEG,
		OP_NOT,
		OP_AND,
		OP_OR,
		OP_XOR,
		OP_LSHIFT,
		OP_RSHIFT,
		OP_MIN,
		OP_MAX,
		OP_ABS,
		OP_LN,
		OP_EXP,
		OP_POW,
		OP_SQRT,
		OP_FLOOR,
		OP_CEIL,
		OP_CONCAT,
		OP_LOWER,
		OP_UPPER,
		OP_TONUM,
		OP_TOINUM,
		OP_CAST,
		OP_LAST_MODOP=OP_CAST,
		OP_RANGE,
		OP_ARRAY,
		OP_COUNT,
		OP_LENGTH,
		OP_POSITION,
		OP_SUBSTR,
		OP_REPLACE,
		OP_PAD,
		OP_TRIM,
		OP_SUM,
		OP_AVG,
		OP_VAR_POP,
		OP_VAR_SAMP,
		OP_STDDEV_POP,
		OP_STDDEV_SAMP,
		OP_HISTOGRAM,
		OP_COALESCE,
		OP_PATH,
		OP_FIRST_BOOLEAN,
		OP_EQ=OP_FIRST_BOOLEAN,
		OP_NE,
		OP_LT,
		OP_LE,
		OP_GT,
		OP_GE,
		OP_IN,
		OP_BEGINS,
		OP_CONTAINS,
		OP_ENDS,
		OP_REGEX,
		OP_EXISTS,
		OP_ISLOCAL,
		OP_IS_A,
		OP_LAND,
		OP_LOR,
		OP_LNOT,
		OP_LAST_BOOLEAN=OP_LNOT,

		OP_EXTRACT,
		OP_DEREF,
		OP_REF,
		OP_CALL,

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

	struct PID
	{
		uint64_t	pid;
		IdentityID	ident;
		operator	uint32_t() const {return uint32_t(pid>>32)^uint32_t(pid)^ident;}
		bool		operator==(const PID& rhs) const {return pid==rhs.pid && ident==rhs.ident;}
		bool		operator!=(const PID& rhs) const {return pid!=rhs.pid || ident!=rhs.ident;}
		void		setStoreID(uint16_t sid) {pid=(pid&0x0000FFFFFFFFFFFFULL)|(uint64_t(sid)<<48);}
	};

	struct VPID
	{
		uint64_t	pid;
		IdentityID	ident;
		VersionID	vid;
	};

	struct RefP
	{
		IPIN		*pin;
		PropertyID	pid;
		ElementID	eid;
		VersionID	vid;
	};

	struct RefVID
	{
		PID			id;
		PropertyID	pid;
		ElementID	eid;
		VersionID	vid;
	};

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

	struct RefV
	{
		unsigned	char	refN;
		unsigned	char	type;
		unsigned	short	flags;
		PropertyID			id;
	};

	enum GO_DIR
	{
		GO_FIRST,GO_LAST,GO_NEXT,GO_PREVIOUS,GO_FINDBYID
	};

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
	struct StrEdit
	{
		union {
			const	char		*str;
			const	uint8_t		*bstr;
		};
		uint32_t				length;				// shift+length must be <= the length of the string being edited
		uint64_t				shift;				// shift==~0ULL means 'end of string'; in this case length must be 0
	};
	struct QualifiedValue {
		double			d;
		uint16_t		units;
	};

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
		void	setParam(unsigned char pn,ValueType ty=VT_ANY,unsigned short f=0) {type=VT_VARREF; property=STORE_INVALID_PROPID; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=0; refV.refN=pn; refV.type=(unsigned char)ty; refV.flags=f&~VAR_TYPE_MASK|VAR_PARAM; refV.id=STORE_INVALID_PROPID; meta=0;}
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
	
	class MV_EXP IPIN
	{
	public:
		virtual	const PID&	getPID() const  = 0;
		virtual	bool		isLocal() const = 0;
		virtual	bool		isCommitted() const = 0;
		virtual	bool		isReadonly() const = 0;
		virtual	bool		canNotify() const = 0;
		virtual	bool		isReplicated() const = 0;
		virtual	bool		canBeReplicated() const = 0;
		virtual	bool		isHidden() const = 0;
		virtual	bool		isDeleted() const = 0;
		virtual	bool		isClass() const = 0;
		virtual	bool		isDerived() const = 0;
		virtual	bool		isProjected() const = 0;

		virtual	uint32_t	getNumberOfProperties() const = 0;
		virtual	const Value	*getValueByIndex(unsigned idx) const = 0;
		virtual	const Value	*getValue(PropertyID pid) const = 0;

		virtual	RC			getPINValue(Value& res) const = 0;
		virtual uint32_t	getStamp() const = 0;
		virtual	char		*getURI() const = 0;

		virtual	bool		testClassMembership(ClassID,const Value *params=NULL,unsigned nParams=0) const = 0;
		virtual	bool		defined(const PropertyID *pids,unsigned nProps) const = 0;

		virtual	RC			refresh(bool fNet=true) = 0;
		virtual	IPIN		*clone(const Value *overwriteValues=NULL,unsigned nOverwriteValues=0,unsigned mode=0) = 0;
		virtual	IPIN		*project(const PropertyID *properties,unsigned nProperties,const PropertyID *newProps=NULL,unsigned mode=0) = 0;
		virtual	RC			modify(const Value *values,unsigned nValues,unsigned mode=0,
									const ElementID *eids=NULL,unsigned *pNFailed=NULL) = 0;

		virtual	RC			setExpiration(uint32_t) = 0;
		virtual	RC			setNotification(bool fReset=false) = 0;
		virtual	RC			setReplicated() = 0;
		virtual	RC			setNoIndex() = 0;
		virtual	RC			deletePIN() = 0;
		virtual	RC			undelete() = 0;

		virtual	void		destroy() = 0;
	};
	
	struct QName
	{
		const	char	*qpref;
		size_t			lq;
		const	char	*str;
		size_t			lstr;
		bool			fDel;
	};

	struct MV_EXP CompilationError
	{
		RC				rc;
		const	char	*msg;
		unsigned		line;
		unsigned		pos;
	};

	class MV_EXP IExprTree 
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

	class MV_EXP IExpr 
	{
	public:
		virtual	RC		execute(Value& res,const Value *params=NULL,unsigned nParams=0) const = 0;
		virtual	char	*toString(unsigned mode=0,const QName *qNames=NULL,unsigned nQNames=0) const = 0;
		virtual IExpr	*clone() const = 0;
		virtual	void	destroy() = 0;
	};

	class MV_EXP ICursor
	{
	public:
		virtual	IPIN		*next() = 0;
		virtual	RC			next(PID&) = 0;
		virtual	RC			next(IPIN *pins[],unsigned nPins,unsigned& nRet) = 0;
		virtual	RC			rewind() = 0;
		virtual	uint64_t	getCount() const = 0;
		virtual	void		destroy() = 0;
	};

	class MV_EXP IStreamOut
	{
	public:
		virtual	RC		next(unsigned char *buf,size_t& lBuf) = 0;
		virtual	void	destroy() = 0;
	};
	
	class MV_EXP IStreamIn
	{
	public:
		virtual	RC		next(const unsigned char *buf,size_t lBuf) = 0;
		virtual	void	destroy() = 0;
	};

	#define	QFT_FILTER_SW		0x0001
	#define	QFT_RET_NO_DOC		0x0002
	#define	QFT_RET_PARTS		0x0004
	#define	QFT_RET_NO_PARTS	0x0008

	enum QUERY_SETOP
	{
		QRY_JOIN, QRY_LEFTJOIN, QRY_RIGHTJOIN, QRY_OUTERJOIN, QRY_UNION, QRY_EXCEPT, QRY_INTERSECT, QRY_ALL_SETOP
	};

	#define	CLASS_PARAM_REF		0x80000000

	struct	ClassSpec
	{
		ClassID			classID;
		unsigned		nParams;
		const	Value	*params;
	};

	struct PathSeg
	{
		PropertyID		pid;
		ElementID		eid;
		IExpr			*filter;
		ClassID			cls;
		unsigned		rmin;
		unsigned		rmax;
		bool			fLast;
	};

	#define	ORD_DESC			0x01
	#define	ORD_NCASE			0x02
	#define	ORD_NULLS_BEFORE	0x04
	#define	ORD_NULLS_AFTER		0x08

	struct	OrderSeg
	{
		IExprTree	*expr;
		PropertyID	pid;
		uint8_t		flags;
		uint8_t		var;
		uint16_t	lPrefix;
	};

	typedef	unsigned	char	QVarID;
	#define	INVALID_QVAR_ID		0xFF

	enum STMT_OP
	{
		STMT_QUERY, STMT_INSERT, STMT_UPDATE, STMT_DELETE, STMT_UNDELETE, STMT_START_TX, STMT_COMMIT, STMT_ROLLBACK, STMT_ISOLATION, STMT_PREFIX, STMT_OP_ALL
	};
	
	enum DistinctType
	{
		DT_DEFAULT, DT_ALL, DT_DISTINCT, DT_DISTINCT_VALUES
	};

	class IStmtCallback
	{
	public:
		virtual	void	result(ICursor *res,RC rc) = 0;
	};

	class MV_EXP IStmt
	{
	public:
		virtual QVarID	addVariable(const ClassSpec *classes=NULL,unsigned nClasses=0,IExprTree *cond=NULL) = 0;
		virtual QVarID	addVariable(const PID& pid,PropertyID propID,IExprTree *cond=NULL) = 0;
		virtual QVarID	addVariable(IStmt *qry) = 0;
		virtual	QVarID	setOp(QVarID leftVar,QVarID rightVar,QUERY_SETOP) = 0;
		virtual	QVarID	setOp(const QVarID *vars,unsigned nVars,QUERY_SETOP) = 0;
		virtual	QVarID	join(QVarID leftVar,QVarID rightVar,IExprTree *cond=NULL,QUERY_SETOP=QRY_JOIN,PropertyID=STORE_INVALID_PROPID) = 0;
		virtual	QVarID	join(const QVarID *vars,unsigned nVars,IExprTree *cond=NULL,QUERY_SETOP=QRY_JOIN,PropertyID=STORE_INVALID_PROPID) = 0;
		virtual	RC		setName(QVarID var,const char *name) = 0;
		virtual	RC		setDistinct(QVarID var,DistinctType) = 0;
		virtual	RC		addOutput(QVarID var,const Value *dscr,unsigned nDscr) = 0;
		virtual RC		addCondition(QVarID var,IExprTree *cond) = 0;
		virtual	RC		addConditionFT(QVarID var,const char *str,unsigned flags=0,const PropertyID *pids=NULL,unsigned nPids=0) = 0;
		virtual	RC		setPIDs(QVarID var,const PID *pids,unsigned nPids) = 0;
		virtual	RC		setPath(QVarID var,const PathSeg *segs,unsigned nSegs) = 0;
		virtual	RC		setPropCondition(QVarID var,const PropertyID *props,unsigned nProps,bool fOr=false) = 0;
		virtual	RC		setJoinProperties(QVarID var,const PropertyID *props,unsigned nProps) = 0;
		virtual	RC		setGroup(QVarID,const OrderSeg *order,unsigned nSegs,IExprTree *having=NULL) = 0;
		virtual	RC		setOrder(const OrderSeg *order,unsigned nSegs) = 0;
		virtual	RC		setValues(const Value *values,unsigned nValues) =  0;

		virtual	STMT_OP	getOp() const = 0;

		virtual	RC		execute(ICursor **result=NULL,const Value *params=NULL,unsigned nParams=0,unsigned nProcess=~0u,unsigned nSkip=0,
										unsigned long mode=0,uint64_t *nProcessed=NULL,TXI_LEVEL=TXI_DEFAULT) const = 0;
		virtual	RC		asyncexec(IStmtCallback *cb,const Value *params=NULL,unsigned nParams=0,unsigned nProcess=~0u,unsigned nSkip=0,
										unsigned long mode=0,TXI_LEVEL=TXI_DEFAULT) const = 0;
		virtual	RC		execute(IStreamOut*& result,const Value *params=NULL,unsigned nParams=0,unsigned nProcess=~0u,unsigned nSkip=0,
										unsigned long mode=0,TXI_LEVEL=TXI_DEFAULT) const = 0;
		virtual	RC		count(uint64_t& cnt,const Value *params=NULL,unsigned nParams=0,unsigned long nAbort=~0u,
										unsigned long mode=0,TXI_LEVEL=TXI_DEFAULT) const = 0;
		virtual	RC		exist(const Value *params=NULL,unsigned nParams=0,unsigned long mode=0,TXI_LEVEL=TXI_DEFAULT) const = 0;
		virtual	RC		analyze(char *&plan,const Value *pars=NULL,unsigned nPars=0,unsigned long md=0) const = 0;

		virtual	bool	isSatisfied(const IPIN *pin,const Value *pars=NULL,unsigned nPars=0,unsigned long mode=0) const = 0;
		virtual	bool	isSatisfied(const IPIN *const *pins,unsigned nPins,const Value *pars=NULL,unsigned nPars=0,unsigned long mode=0) const = 0;
		virtual	char	*toString(unsigned mode=0,const QName *qNames=NULL,unsigned nQNames=0) const = 0;

		virtual	IStmt	*clone(STMT_OP=STMT_OP_ALL) const = 0;
		virtual	void	destroy() = 0;
	};

	/**
	 * Full identification of properties.
	 * @see ISession
	 */
	struct URIMap
	{
		const		char	*URI;
		URIID				uid;
	};

	struct AllocCtrl
	{
		unsigned long	arrayThreshold;
		size_t			ssvThreshold;
		float			pctPageFree;
	};

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

	class StringEnum
	{
	public:
		virtual	const char	*next() = 0;
		virtual	void		destroy() = 0;
	};

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
	class MV_EXP ISession
	{
		void	operator	delete(void*) {}
	public:
		static	ISession	*startSession(MVStoreCtx,const char *identityName=NULL,const char *password=NULL);
		virtual	ISession	*clone(const char * =NULL) const = 0;
		virtual	RC			attachToCurrentThread() = 0;
		virtual	RC			detachFromCurrentThread() = 0;

		virtual	void		setInterfaceMode(unsigned flags) = 0;
		virtual	unsigned	getInterfaceMode() const = 0;
		virtual	RC			setURIBase(const char *URIBase) = 0;
		virtual	RC			addURIPrefix(const char *name,const char *URIprefix) = 0;
		virtual	void		setDefaultExpiration(uint64_t defExp) = 0;
		virtual	void		changeTraceMode(unsigned mask,bool fReset=false) = 0;
		virtual	void		setTrace(ITrace *) = 0;
		virtual	void		terminate() = 0;

		virtual	RC			mapURIs(unsigned nURIs,URIMap URIs[]) = 0;
		virtual	RC			getURI(uint32_t,char buf[],size_t& lbuf) = 0;

		virtual	IdentityID	getCurrentIdentityID() const = 0;
		virtual	IdentityID	getIdentityID(const char *identity) = 0;
		virtual	RC			impersonate(const char *identity) = 0;
		virtual	IdentityID	storeIdentity(const char *identity,const char *pwd,bool fMayInsert=true,const unsigned char *cert=NULL,unsigned lcert=0) = 0;
		virtual	IdentityID	loadIdentity(const char *identity,const unsigned char *pwd,unsigned lPwd,bool fMayInsert=true,const unsigned char *cert=NULL,unsigned lcert=0) = 0;
		virtual	RC			setInsertPermission(IdentityID,bool fMayInsert=true) = 0;	
		virtual	size_t		getStoreIdentityName(char buf[],size_t lbuf) = 0;
		virtual	size_t		getIdentityName(IdentityID,char buf[],size_t lbuf) = 0;
		virtual	size_t		getCertificate(IdentityID,unsigned char buf[],size_t lbuf) = 0;
		virtual	RC			changePassword(IdentityID,const char *oldPwd,const char *newPwd) = 0;
		virtual	RC			changeCertificate(IdentityID,const char *pwd,const unsigned char *cert,unsigned lcert) = 0;
		virtual	RC			changeStoreIdentity(const char *newIdentity) = 0;

		virtual	unsigned	getStoreID(const PID& ) = 0;
		virtual	unsigned	getLocalStoreID() = 0;

		virtual	IExprTree	*expr(ExprOp op,unsigned nOperands,const Value *operands,unsigned flags=0) = 0;

		virtual	IStmt		*createStmt(STMT_OP=STMT_QUERY,unsigned mode=0) = 0;
		virtual	IStmt		*createStmt(const char *queryStr,const URIID *ids=NULL,unsigned nids=0,CompilationError *ce=NULL) = 0;
		virtual	IExprTree	*createExprTree(const char *str,const URIID *ids=NULL,unsigned nids=0,CompilationError *ce=NULL) = 0;
		virtual	IExpr		*createExpr(const char *str,const URIID *ids=NULL,unsigned nids=0,CompilationError *ce=NULL) = 0;
		virtual	IExpr		*createExtExpr(uint16_t langID,const unsigned char *body,uint32_t lBody,uint16_t flags) = 0;
		virtual	RC			getTypeName(ValueType type,char buf[],size_t lbuf) = 0;
		virtual	void		abortQuery() = 0;

		virtual	RC			execute(const char *str,size_t lstr,char **result=NULL,const URIID *ids=NULL,unsigned nids=0,
									const Value *params=NULL,unsigned nParams=0,CompilationError *ce=NULL,uint64_t *nProcessed=NULL,
									unsigned nProcess=~0u,unsigned nSkip=0) = 0;

		virtual	RC			createInputStream(IStreamIn *&in,IStreamIn *out=NULL,size_t lbuf=0) = 0;

		virtual	RC			getClassID(const char *className,ClassID& cid) = 0;
		virtual	RC			enableClassNotifications(ClassID,unsigned notifications) = 0;
		virtual	RC			rebuildIndices(const ClassID *cidx=NULL,unsigned nClasses=0) = 0;
		virtual	RC			rebuildIndexFT() = 0;
		virtual	RC			createIndexNav(ClassID,IndexNav *&nav) = 0;
		virtual	RC			listValues(ClassID cid,PropertyID pid,IndexNav *&ven) = 0;
		virtual	RC			listWords(const char *query,StringEnum *&sen) = 0;
		virtual	RC			getClassInfo(ClassID,IPIN*&) = 0;

		virtual	IPIN		*getPIN(const PID& id,unsigned mode=0) = 0;
		virtual	IPIN		*getPIN(const Value& ref,unsigned mode=0) = 0;
		virtual	IPIN		*getPINByURI(const char *uri,unsigned mode=0) = 0;
		virtual	RC			getValues(Value *vals,unsigned nVals,const PID& id) = 0;
		virtual	RC			getValue(Value& res,const PID& id,PropertyID,ElementID=STORE_COLLECTION_ID) = 0;
		virtual	RC			getValue(Value& res,const PID& id) = 0;
		virtual	bool		isCached(const PID& id) = 0;
		virtual	RC			createPIN(PID& res,const Value values[],unsigned nValues,unsigned mode=0,const AllocCtrl* =NULL) = 0;
		virtual	IPIN		*createUncommittedPIN(Value* values=0,unsigned nValues=0,unsigned mode=0,const PID *original=NULL) = 0;
		virtual	RC			commitPINs(IPIN *const *pins,unsigned nPins,unsigned mode=0,const AllocCtrl* =NULL,const Value *params=NULL,unsigned nParams=0) = 0;
		virtual	RC			modifyPIN(const PID& id,const Value *values,unsigned nValues,unsigned mode=0,const ElementID *eids=NULL,unsigned *pNFailed=NULL,const Value *params=NULL,unsigned nParams=0) = 0;
		virtual	RC			deletePINs(IPIN **pins,unsigned nPins,unsigned mode=0) = 0;
		virtual	RC			deletePINs(const PID *pids,unsigned nPids,unsigned mode=0) = 0;
		virtual	RC			undeletePINs(const PID *pids,unsigned nPids) = 0;
		virtual	RC			setPINAllocationParameters(const AllocCtrl *ac) = 0;

		virtual	RC			setIsolationLevel(TXI_LEVEL) = 0;
		virtual	RC			startTransaction(TX_TYPE=TXT_READWRITE,TXI_LEVEL=TXI_DEFAULT) = 0;
		virtual	RC			commit(bool fAll=false) = 0;
		virtual	RC			rollback(bool fAll=false) = 0;

		virtual	RC			reservePage(uint32_t) = 0;

		virtual	RC			copyValue(const Value& src,Value& dest) = 0;
		virtual	RC			convertValue(const Value& oldValue,Value& newValue,ValueType newType) = 0;
		virtual	RC			parseValue(const char *p,size_t l,Value& res,CompilationError *ce=NULL) = 0;
		virtual	RC			parseValues(const char *p,size_t l,Value *&res,unsigned& nValues,CompilationError *ce=NULL,char delimiter=',') = 0;
		virtual	int			compareValues(const Value& v1,const Value& v2,bool fNCase=false) = 0;
		virtual	void		freeValues(Value *vals,unsigned nVals) = 0;
		virtual	void		freeValue(Value& val) = 0;

		virtual	void		setTimeZone(int64_t tzShift) = 0;
		virtual	RC			convDateTime(uint64_t dt,DateTime& dts,bool fUTC=true) const = 0;
		virtual	RC			convDateTime(const DateTime& dts,uint64_t& dt,bool fUTC=true) const = 0;

		virtual	RC			setStopWordTable(const char **words,uint32_t nWords,PropertyID pid=STORE_INVALID_PROPID,
															bool fOverwriteDefault=false,bool fSessionOnly=false) = 0;

		virtual	void		*alloc(size_t) = 0;
		virtual	void		*realloc(void *,size_t) = 0;
		virtual	void		free(void *) = 0;
	};

	class MV_EXP IConnection
	{
	public:
		static	IConnection	*create(MVStoreCtx,const char *identityName=NULL,const char *password=NULL);
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
