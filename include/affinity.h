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

#ifndef _AFFINITY_H_
#define _AFFINITY_H_

#include <stdint.h>
#include <stddef.h>
#include <stdarg.h>
#include <string.h>
#include <assert.h>

#include "rc.h"
#include "units.h"

using namespace AfyRC;

/**
 * Namespace to encapsulate the public interface of Affinity.
 * Overview:
 *  ISession represents a client's connection to the store.
 *  IPIN represents a PIN, i.e. a node of information.
 *  Through its ISession, a client can create, query, modify and delete PINs, control transactions, set session parameters, etc.
 *  Through IPIN, one can create, query, modify and delete properties or clone the PIN optionally overwriting properties.
 */
namespace Afy 
{
	typedef	uint32_t					IdentityID;						/**< Identity ID - represents an identity in this store */
	typedef	uint32_t					URIID;							/**< URI ID - represents a URI in this store */
	typedef	URIID						DataEventID;					/**< Data event ID - represents the URI of a specific data event in this store */
	typedef	URIID						PropertyID;						/**< Property ID - same as URIID */
	typedef	uint32_t					ElementID;						/**< Immutable collection element ID */
	typedef	uint32_t					VersionID;						/**< Version number */
	typedef	uint64_t					TIMESTAMP;						/**< Timestamp in miscroseconds since start date */

	#define	STORE_INVALID_PID			0ULL
	#define	STORE_INVALID_URIID			(~Afy::PropertyID(0))	
	#define	STORE_INVALID_IDENTITY		(~Afy::IdentityID(0))	
	#define	STORE_INMEM_IDENTITY		(~Afy::IdentityID(1))
	#define	STORE_INVALID_CLASSID		STORE_INVALID_URIID
	#define	STORE_CURRENT_VERSION		(~Afy::VersionID(0))	
	#define	STORE_MAX_URIID				Afy::URIID(0x1FFFFFFF)				/**< Maximum value for URIID */
	#define	STORE_OWNER					Afy::IdentityID(0)					/**< The owner of the store always has IdentityID equals to 0 */

	#define	AFFINITY_URI_PREFIX			"http://affinityng.org/"			/**< URI prefix for Affinity URIs */
	#define	AFFINITY_STD_URI_PREFIX		"http://affinityng.org/builtin/"	/**< URI prefix for built-in Affinity URIs */
	#define	AFFINITY_STD_QPREFIX		"afy:"								/**< Qname prefix for built-in URIs */
	#define	AFFINITY_SERVICE_PREFIX		"http://affinityng.org/service/"	/**< URI prefix for service URIs */
	#define	AFFINITY_SRV_QPREFIX		"srv:"								/**< Qname prefix for services, both built-in and external */
	#define	AFFINITY_STORE_QPREFIX		"store:"							/**< Qname prefix for #names, added automatically */

	/**
	 * special property and class IDs
	 */
	#define	CLASS_OF_CLASSES			Afy::URIID(0)				/**< Fixed ID of the class of all classes and data events */
	#define	CLASS_OF_TIMERS				Afy::URIID(1)				/**< Class of all timers */
	#define	CLASS_OF_LISTENERS			Afy::URIID(2)				/**< Class of all listeners */
	#define	CLASS_OF_LOADERS			Afy::URIID(3)				/**< Class of all loaders (i.e. PINs describing external loadable service libraries) */
	#define	CLASS_OF_PACKAGES			Afy::URIID(4)				/**< Class of all installed packages */
	#define	CLASS_OF_NAMED				Afy::URIID(5)				/**< Class of all globally named PINs (i.e. having PROP_SPEC_OBJID) */
	#define	CLASS_OF_ENUMS				Afy::URIID(6)				/**< Class of enumerations */
	#define	CLASS_OF_STORES				Afy::URIID(7)				/**< Class of known stores */
	#define	CLASS_OF_SERVICES			Afy::URIID(8)				/**< Class of registered services */
	#define	CLASS_OF_FSMCTX				Afy::URIID(9)				/**< Class of all persistent FSM contexts */
	#define	CLASS_OF_FSMS				Afy::URIID(10)				/**< Class of FSMs */
	#define	CLASS_OF_HANDLERS			Afy::URIID(11)				/**< Class of event handlers */
	
	#define	PROP_SPEC_PINID				Afy::PropertyID(17)			/**< PIN id as PIN's property, immutable */
	#define	PROP_SPEC_DOCUMENT			Afy::PropertyID(18)			/**< document PIN this PIN is a part of */
	#define	PROP_SPEC_PARENT			Afy::PropertyID(19)			/**< parent (whole) PIN */
	#define	PROP_SPEC_VALUE				Afy::PropertyID(20)			/**< value of the PIN (can be an expression using other properties */
	#define	PROP_SPEC_CREATED			Afy::PropertyID(21)			/**< PIN creation timestamp, immutable */
	#define	PROP_SPEC_CREATEDBY			Afy::PropertyID(22)			/**< identity created the PIN, immutable */
	#define	PROP_SPEC_UPDATED			Afy::PropertyID(23)			/**< timestamp of the latest PIN modification, updated automatically */
	#define	PROP_SPEC_UPDATEDBY			Afy::PropertyID(24)			/**< identity of the latest PIN modification, updated automatically */
	#define	PROP_SPEC_ACL				Afy::PropertyID(25)			/**< ACL - can be a collection and contain references to other PINs */
	#define	PROP_SPEC_STAMP				Afy::PropertyID(26)			/**< stamp (unsigned integer) of the latest modification, updated automatically */
	#define	PROP_SPEC_OBJID				Afy::PropertyID(27)			/**< objectID of the class represented by this pin (VT_CLASSID) */
	#define PROP_SPEC_PREDICATE			Afy::PropertyID(28)			/**< predicate of a class or relations (VT_STMT) */
	#define	PROP_SPEC_COUNT				Afy::PropertyID(29)			/**< number of instances of a given class currently in the store */
	#define	PROP_SPEC_SPECIALIZATION	Afy::PropertyID(30)			/**< collection of data events which are specializations of this data event */
	#define	PROP_SPEC_ABSTRACTION		Afy::PropertyID(31)			/**< collection of data events which are abstractions of this data event */
	#define PROP_SPEC_INDEX_INFO		Afy::PropertyID(32)			/**< family index information */
	#define PROP_SPEC_PROPERTIES		Afy::PropertyID(33)			/**< properties refered by the class */
	#define	PROP_SPEC_ONENTER			Afy::PropertyID(34)			/**< PIN entering a class action; flow of control entering a state action */
	#define	PROP_SPEC_ONUPDATE			Afy::PropertyID(35)			/**< action on PIN belonging to a class is modified */
	#define	PROP_SPEC_ONLEAVE			Afy::PropertyID(36)			/**< PIN leaving a class action; flow of control leaving a state action */
	#define	PROP_SPEC_WINDOW			Afy::PropertyID(37)			/**< stream windowing control (size of class-related window) */
	#define	PROP_SPEC_TRANSITION		Afy::PropertyID(38)			/**< in a workflow: transition(s) to other states */
	#define	PROP_SPEC_EVENT				Afy::PropertyID(39)			/**< event descriptor */
	#define	PROP_SPEC_CONDITION			Afy::PropertyID(40)			/**< condition descriptor in a package; transition condition in a FSM */
	#define	PROP_SPEC_ACTION			Afy::PropertyID(41)			/**< timer or listener action; set of actions in a package; transition action */
	#define	PROP_SPEC_REF				Afy::PropertyID(42)			/**< reference field in VT_STRUCT */
	#define	PROP_SPEC_STATE				Afy::PropertyID(43)			/**< FSM instance state */
	#define	PROP_SPEC_INTERVAL			Afy::PropertyID(44)			/**< timer interval or i/o timeout*/
	#define PROP_SPEC_LOAD				Afy::PropertyID(45)			/**< the name of a library to load */
	#define	PROP_SPEC_SERVICE			Afy::PropertyID(46)			/**< service ID or communication stack descriptor */
	#define	PROP_SPEC_LISTEN			Afy::PropertyID(47)			/**< listener service stack */
	#define	PROP_SPEC_ADDRESS			Afy::PropertyID(48)			/**< address for communication PINs (can be reference to other PIN(s)) */
	#define	PROP_SPEC_RESOLVE			Afy::PropertyID(49)			/**< service ID to resolve the address */
	#define	PROP_SPEC_POSITION			Afy::PropertyID(50)			/**< positioning before i/o (seek) */
	#define	PROP_SPEC_REQUEST			Afy::PropertyID(51)			/**< communication request */
	#define	PROP_SPEC_CONTENT			Afy::PropertyID(52)			/**< content of i/o operation, either to be written or read */
	#define	PROP_SPEC_BUFSIZE			Afy::PropertyID(53)			/**< buffer size for buffering services */
	#define	PROP_SPEC_EXCEPTION			Afy::PropertyID(54)			/**< exception code */
	#define	PROP_SPEC_VERSION			Afy::PropertyID(55)			/**< chain of versions */
	#define	PROP_SPEC_WEIGHT			Afy::PropertyID(56)			/**< future implementation */
	#define	PROP_SPEC_SELF				Afy::PropertyID(57)			/**< pseudo-property: the PIN itself */
	#define	PROP_SPEC_PATTERN			Afy::PropertyID(58)			/**< pattern for Regex service */
	#define	PROP_SPEC_PROTOTYPE			Afy::PropertyID(59)			/**< JavaScript-like inheritance or regex prototype*/
	#define	PROP_SPEC_UNDO				Afy::PropertyID(60)			/**< UNDO operation for communication PINs */
	#define	PROP_SPEC_NAMESPACE			Afy::PropertyID(61)			/**< package namespace prefix */
	#define	PROP_SPEC_SUBPACKAGE		Afy::PropertyID(62)			/**< sub-package(s) of this package */
	#define	PROP_SPEC_ENUM				Afy::PropertyID(63)			/**< enumeration string collection */
	#define	PROP_SPEC_IDENTITY			Afy::PropertyID(64)			/**< identity for access to remote store or encryption/decryption in communications */
	#define	PROP_SPEC_CONTENTTYPE		Afy::PropertyID(65)			/**< content type in communication services */
	#define	PROP_SPEC_CONTENTLENGTH		Afy::PropertyID(66)			/**< buffered content length in communication services */
	#define	PROP_SPEC_ACCEPT			Afy::PropertyID(67)			/**< acceptable encodings for communications */
	#define	PROP_SPEC_TOKEN				Afy::PropertyID(68)			/**< token to match request/response in communications */

	#define	SERVICE_ENCRYPTION			Afy::URIID(244)				/**< content encryption */
	#define	SERVICE_SERIAL				Afy::URIID(245)				/**< serial port communication */
	#define	SERVICE_BRIDGE				Afy::URIID(246)				/**< bridge between two stores */
	#define	SERVICE_AFFINITY			Afy::URIID(247)				/**< affinity query processor as a service */
	#define	SERVICE_REGEX				Afy::URIID(248)				/**< transformations using regular expressions */
	#define	SERVICE_PATHSQL				Afy::URIID(249)				/**< pathSQL parsing/rendering */
	#define	SERVICE_JSON				Afy::URIID(250)				/**< JSON encoding service */
	#define	SERVICE_PROTOBUF			Afy::URIID(251)				/**< protobuf encoding/decoding service */
	#define	SERVICE_SOCKETS				Afy::URIID(252)				/**< sockets service */
	#define	SERVICE_IO					Afy::URIID(253)				/**< OS i/o service */
	#define	SERVICE_REMOTE				Afy::URIID(254)				/**< remote content reading service (not built-in) */
	#define	SERVICE_REPLICATION			Afy::URIID(255)				/**< replication service (not built-in) */

	#define	MIN_BUILTIN_SERVICE			Afy::URIID(216)
	#define	MAX_BUILTIN_URIID			Afy::URIID(255)
	#define	MAX_BUILTIN_CLASSID			CLASS_OF_HANDLERS
	#define	MAX_BUILTIN_PROPID			PROP_SPEC_STATE
	
	#define	PROP_SPEC_ANY				(~Afy::PropertyID(0))			/**< used in queries, matches any property */

	/**
	 * special collection element IDs
	 * @see Value::eid
	 */
	#define	STORE_COLLECTION_ID			(~Afy::ElementID(0))		/**< Singleton value or operation applied to the whole collection */
	#define	STORE_LAST_ELEMENT			(~Afy::ElementID(1))		/**< Last element of a collection */
	#define	STORE_FIRST_ELEMENT			(~Afy::ElementID(2))		/**< First element of a collection */
	#define	STORE_SUM_COLLECTION		(~Afy::ElementID(3))		/**< IStmt: replace collection with the sum of its elements */
	#define	STORE_AVG_COLLECTION		(~Afy::ElementID(4))		/**< IStmt: replace collection with the average of its elements */
	#define	STORE_CONCAT_COLLECTION		(~Afy::ElementID(5))		/**< IStmt: replace collection with the concatenation of its elements */
	#define	STORE_MIN_ELEMENT			(~Afy::ElementID(6))		/**< IStmt: replace collection with the minimum of its elements */
	#define	STORE_MAX_ELEMENT			(~Afy::ElementID(7))		/**< IStmt: replace collection with the maximum of its elements */
	#define	STORE_SOME_ELEMENT			(~Afy::ElementID(8))		/**< IStmt: existential quantifier for the collection */
	#define	STORE_ALL_ELEMENTS			(~Afy::ElementID(9))		/**< IStmt: 'for all' quantifier */

	/**
	 * PIN create/commit/modify/delete, IStmt execute mode flags
	 */
	#define MODE_NORET					0x00010000	/**< in ServiceCtx::createPIN() - the PIN is not returned by the service, rather referred by another PIN */
	#define MODE_NO_EID					0x00010000	/**< in modify() - don't update "eid" field of Value structure */
	#define	MODE_FOR_UPDATE				0x00020000	/**< in IStmt::execute(): lock pins for update while reading them */
	#define	MODE_PERSISTENT				0x00040000	/**< used in PIN::clone(), ISession::createPIN() to immediately commit new pin */
	#define	MODE_PURGE					0x00040000	/**< in deletePINs(): purge pins rather than just delete */
	#define	MODE_ALL					0x00040000	/**< for STMT_COMMIT, STMT_ROLLBACK - commit/rollback all nested transactions or persist all transient pins */
	#define	MODE_READONLY				0x00040000	/**< for STMT_START_TX - start r/o transaction */
	#define	MODE_COPY_VALUES			0x00080000	/**< used in createPIN() and IStmt::execute() to copy Values (parameters, query expressions, etc.) passed rather than assume ownership */
	#define	MODE_FORCE_EIDS				0x00100000	/**< used only in replication */
	#define	MODE_PART					0x00100000	/**< in nested insert: create a part */
	#define	MODE_TEMP_ID				0x00200000	/**< when a PIN is persisted: create a pin with a temporary (reusable, non-unique) id */
	#define	MODE_CHECK_STAMP			0x00200000	/**< forces stamp check before modification; if stamp changed the op is aborted and RC_REPEAT is returned */
	#define	MODE_HOLD_RESULT			0x00200000	/**< for IStmt::execute(): don't close result set on transaction commit/rollback */
	#define	MODE_ALL_WORDS				0x00400000	/**< all words must be present in FT condition */
	#define	MODE_SAME					0x00400000	/**< IBatch::createPIN(): the structure of the pin is the same as previous */
	#define	MODE_DELETED				0x00800000	/**< for query: return only (soft)deleted pins */
	#define	MODE_RAW					0x01000000	/**< SELECT/UPDATE: return PIN properties as they are stored, no calculation/communication */

	/**
	 * PIN creation flags (starts with 0x40000000 for compatibility with protobuf)
	 */
	#define	PIN_NO_REPLICATION			0x40000000	/**< marks a PIN as one which shouldn't be replicated (set only when the PIN is committed) */
	#define	PIN_NOTIFY					0x20000000	/**< PIN generates notifications when committed, modified or deleted */
	#define	PIN_REPLICATED				0x10000000	/**< PIN is replicated to another store or is a replica */
	#define	PIN_HIDDEN					0x08000000	/**< special PIN - totally hidden from search/replication - accessible only through its PID */
	#define	PIN_PERSISTENT				0x04000000	/**< PIN is committed to persistent storage */
	#define	PIN_TRANSIENT				0x02000000	/**< PIN used as a message, not indexed, not persisted */
	#define	PIN_IMMUTABLE				0x01000000	/**< immutable PIN (e.g. external event) */
	#define	PIN_DELETED					0x00800000	/**< PIN has been soft-deleted */

	/**
	 * PIN metatype flags
	 * @see PIN::getMetaType()
	 */
	#define	PMT_NAMED					0x0001		/**< PIN with afy:objectID */
	#define	PMT_DATAEVENT				0x0002		/**< data event PIN */
	#define	PMT_TIMER					0x0004		/**< timer PIN */
	#define	PMT_LISTENER				0x0008		/**< socket listener PIN */
	#define	PMT_PACKAGE					0x0010		/**< package PIN */
	#define	PMT_LOADER					0x0020		/**< service descriptor for automatic load */
	#define	PMT_ENUM					0x0040		/**< enumeration PIN */
	#define	PMT_COMM					0x0080		/**< communication PIN */
	#define	PMT_FSMCTX					0x0100		/**< FSM invocation context */
	#define	PMT_FSM						0x0200		/**< FSM start node */
	#define	PMT_HANDLER					0x0400		/**< event handler */

	/**
	 * meta-properties
	 * @see Value::meta
	 */
	#define	META_PROP_PART				0x80		/**< property is a reference to a PIN which is a part of this one */
	#define	META_PROP_FTINDEX			0x80		/**< this property (VT_STRING) to be FT indexed */
	#define	META_PROP_SSTORAGE			0x40		/**< store this property separately from the pin body (for string, stream and compound property types) */
	#define	META_PROP_UNSIGNED			0x40		/**< unsigned right shift (>>>=) for UPDATE */
	#define	META_PROP_INMEM				0x20		/**< property is not persisted */
	#define	META_PROP_INDEXED			0x10		/**< PROP_SPEC_PREDICATE: a class with indexed membership */
	#define	META_PROP_SYNC				0x10		/**< PROP_SPEC_SERVICE: force blocking read/write operations, PROP_SPEC_STATE, PROP_SPEC_ACTION: synchronous execution */
	#define	META_PROP_ASYNC				0x08		/**< PROP_SPEC_SERVICE: force non-blocking read/write operations, PROP_SPEC_STATE, PROP_SPEC_ACTION: asynchronous execution */
	#define	META_PROP_KEEPALIVE			0x08		/**< PROP_SPEC_ADDRESS: cache connection/device handle */
	#define	META_PROP_CREATE			0x04		/**< PROP_SPEC_ADDRESS: CREATE flag when openning communication channel */
	#define	META_PROP_LEAVE				0x04		/**< PROP_SPEC_ACTION:  call on leaving class */
	#define	META_PROP_WRITE				0x02		/**< write premission in ACLs and device communication */
	#define	META_PROP_UPDATE			0x02		/**< PROP_SPEC_ACTION: call on update in a class */
	#define	META_PROP_SEEK_END			0x02		/**< PROP_SPEC_POSITION: seek from the end */
	#define	META_PROP_READ				0x01		/**< read premission in ACLs and device communication, force to IPv4 in sockets */
	#define	META_PROP_ENTER				0x01		/**< PROP_SPEC_ACTION: call on entering a class */
	#define	META_PROP_OPTIONAL			0x01		/**< PROP_SPEC_LOAD: don't fail if cannot load */
	#define	META_PROP_SEEK_CUR			0x01		/**< PROP_SPEC_POSITION: seek from current position */
	#define	META_PROP_ALT				0x01		/**< PROP_SPEC_SERVICE: alternative service format (e.g. UDP for srv:sockets) */

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
	 * @see IStmt::toString(), IExpr::toString(), IExprNode::toString()
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

	/**
	 * Trace control flags
	 */
	#define	TRACE_SESSION_QUERIES		0x0001		/**< trace all queries executed within this session */
	#define TRACE_STORE_BUFFERS			0x0002		/**< not implemented yet */
	#define TRACE_STORE_IO				0x0004		/**< not implemented yet */
	#define	TRACE_EXEC_PLAN				0x0008		/**< output execution plan for all queries in this session */
	#define	TRACE_ACTIONS				0x0010		/**< trace class action invocation */
	#define	TRACE_TIMERS				0x0020		/**< trace timer events */
	#define	TRACE_COMMS					0x0040		/**< trace communication stacks */
	#define	TRACE_FSMS					0x0080		/**< trace FSM state transitions */

	/**
	 * memory allocator interface
	 */
	class IMemAlloc
	{
	public:
		virtual	void		*malloc(size_t) = 0;																/**< allocate a memory block */
		virtual	void		*realloc(void *,size_t,size_t=0) = 0;												/**< increase or decrease block size */
		virtual	void		free(void *) = 0;																	/**< free block */
	};

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
		EY_IDENTITY, EY_STOREID
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
		VT_FLOAT,						/**< 32-bit floating number */
		VT_DOUBLE,						/**< 64-bit floating number */

		VT_BOOL,						/**< boolean value (true, false) */

		VT_DATETIME,					/**< timestamp stored as a 64-bit number of microseconds since fixed date */
		VT_INTERVAL,					/**< time interval stored as a 64-bit signed number of microseconds */

		VT_URIID,						/**< URIID */
		VT_IDENTITY,					/**< IdentityID */

		VT_ENUM,						/**< enumeration element */

		VT_STRING,						/**< zero-ended string UTF-8*/
		VT_BSTR,						/**< binary string */

		VT_REF,							/**< a reference to another IPIN */
		VT_REFID,						/**< a reference to another PIN by its PID */
		VT_REFPROP,						/**< a reference to a value (property of this or another PIN) by IPIN & PropertyID */
		VT_REFIDPROP,					/**< a reference to a value (property of this or another PIN) by its PID/PropertyID */
		VT_REFELT,						/**< a reference to a collection element by its IPIN/PropertyID/ElementID */
		VT_REFIDELT,					/**< a reference to a collection element by its PID/PropertyID/ElementID */

		VT_EXPR,						/**< IExpr: stored expression */
		VT_STMT,						/**< IStmt: stored query object */

		VT_COLLECTION,					/**< collection property */
		VT_STRUCT,						/**< composite value */
		VT_MAP,							/**< reserved for future use */
		VT_RANGE,						/**< range of values for OP_IN, equivalent to VT_COLLECTION with length = 2 */
		VT_ARRAY,						/**< an array (see FixedArray) */
		VT_STREAM,						/**< IStream interface */
		VT_CURRENT,						/**< current moment in time, current user, etc. */

		VT_VARREF,						/**< statement: var ref  (cannot be persisted as a property value) */
		VT_EXPRTREE,					/**< sub-expression reference for statement conditions (cannot be persisted as a property value) */

		VT_ALL
	};

	inline	bool	isRef(ValueType vt) {return uint8_t(vt-VT_REF)<=uint8_t(VT_REFIDELT-VT_REF);}
	inline	bool	isInteger(ValueType vt) {return uint8_t(vt-VT_INT)<=uint8_t(VT_UINT64-VT_INT);}
	inline	bool	isNumeric(ValueType vt) {return uint8_t(vt-VT_INT)<=uint8_t(VT_DOUBLE-VT_INT);}
	inline	bool	isString(ValueType vt) {return uint8_t(vt-VT_STRING)<=uint8_t(VT_BSTR-VT_STRING);}
	inline	bool	isCollection(ValueType vt) {return vt==VT_COLLECTION;}
	inline	bool	isComposite(ValueType vt) {return uint8_t(vt-VT_COLLECTION)<=uint8_t(VT_ARRAY-VT_COLLECTION);}
	inline	bool	isSimpleConst(ValueType vt) {return uint8_t(vt-VT_INT)<=uint8_t(VT_BSTR-VT_INT);}

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
		OP_DELETE,					/**< deletes the whole property or an element of a compound property */
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
		OP_SETBIT,					/**< set bit in a binary string by bit number */
		OP_RESETBIT,				/**< reset bit in a binary string by bit number */
		OP_MIN,						/**< minimum of 2 or more values, minimum of collection, array */
		OP_MAX,						/**< maximum of 2 or more values, maximum of collection, array */
		OP_ARGMIN,					/**< the index of the argument or array element, or the reference to collection elemnet, which is minimal (minimizes an expression) */
		OP_ARGMAX,					/**< the index of the argument or array element, or the reference to collection elemnet, which is maximal (maximizes an expression) */
		OP_ABS,						/**< absolute value */
		OP_LN,						/**< natural logarithm */
		OP_EXP,						/**< exponent */
		OP_POW,						/**< power */
		OP_SQRT,					/**< square root */
		OP_SIN,						/**< sine */
		OP_COS,						/**< cosine */
		OP_TAN,						/**< tangent */
		OP_ASIN,					/**< arcsine */
		OP_ACOS,					/**< arccosine */
		OP_ATAN,					/**< arctangent */
		OP_NORM,					/**< vector norm for 1-dim VT_ARRAY */
		OP_TRACE,					/**< matrix trace for 2-dim VT_ARRAY */
		OP_INV,						/**< matrix inverse for 2-dim VT_ARRAY */
		OP_DET,						/**< matrix determinant for 2-dim VT_ARRAY */
		OP_RANK,					/**< matrix rank for 2-dim VT_ARRAY */
		OP_TRANSPOSE,				/**< transpose of a matrix */
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
		OP_COLLECTION,				/**< VT_COLLECTION constructor, returns a collection consisting of arguments passed */
		OP_STRUCT,					/**< VT_STRUCT constructor, returns a structure consisting of passed values, Value::property must be set */
		OP_PIN,						/**< same as OP_STRUCT, but returns a PIN (VT_REF) */
		OP_ARRAY,					/**< VT_ARRAY constructor */
		OP_ELEMENT,					/**< extract collection, array, map element or structure field by calculated index */
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
		OP_DATAEVENTS,				/**< returns a collection of DataEventIDs for this pin */
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
		OP_TESTBIT,					/**< test bit in a binary string */
		OP_SIMILAR,					/**< regular expression match */
		OP_EXISTS,					/**< property exists in PIN, i.e. IS NOT NULL */
		OP_ISCHANGED,				/**< property has been changed */
		OP_ISLOCAL,					/**< PIN is local, i.e. not replicated, not read-only remote cached */
		OP_IS_A,					/**< PIN is a member of a data event or one in a collection of data events */
		OP_LAND,					/**< logical AND */
		OP_LOR,						/**< logical OR */
		OP_LNOT,					/**< logical NOT */
		OP_LAST_BOOLEAN=OP_LNOT,

		OP_EXTRACT,					/**< extract part of a date, extract identity from a PIN ID */
		OP_BITFIELD,				/**< extract a bit field from a number or a binary string */
		OP_CALL,					/**< call a function of code block with parameters */
		OP_HASH,					/**< hash (SHA256) of a pin, a value, a collection */
		OP_CASE,					/**< CASE ... WHEN ... THEN ... END construct */

		OP_ALL
	};

	inline	bool	isBool(ExprOp op) {return unsigned(op-OP_FIRST_BOOLEAN)<=unsigned(OP_LAST_BOOLEAN-OP_FIRST_BOOLEAN);}

	/**
	 * operation modifiers (bit flags) passed to IExprNode::expr(...)
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
		bool		isPID() const {return pid!=STORE_INVALID_PID && ident!=STORE_INVALID_IDENTITY && ident!=STORE_INMEM_IDENTITY;}
		bool		isEmpty() const {return pid==STORE_INVALID_PID;}
		bool		isTPID() const {return pid!=STORE_INVALID_PID && ident==STORE_INVALID_IDENTITY;}
		bool		isPtr() const {return pid!=0ULL && ident==STORE_INMEM_IDENTITY;}
		void		setEmpty() {pid=STORE_INVALID_PID; ident=STORE_INVALID_IDENTITY;}
		void		setTPID(uint64_t t) {pid=t; ident=STORE_INVALID_IDENTITY;}
		void		setPtr(void *p) {pid=(uint64_t)p; ident=STORE_INMEM_IDENTITY;}
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
		PropertyID			id;
		unsigned	short	flags;
		unsigned	short	type;
		unsigned	char	refN;
		unsigned	char	filler[3];
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
		virtual			INav			*clone() const = 0;
		virtual			unsigned		count() const = 0;
		virtual			void			destroy() = 0;
	};

	/**
	 * map interface for in and out parameters
	 * @see VT_MAP in Value
	 */

	#define	IMAP_FIRST		0x0001		/**< return first (last if IMAP_REVERSE is set) element of the map */
	#define	IMAP_REVERSE	0x0002		/**< return elements in reverse order */

	class IMap
	{
	public:
		virtual	const	Value			*find(const Value &key) = 0;
		virtual			RC				getNext(const Value *&key,const Value *&val,unsigned mode=0) = 0;
		virtual			IMap			*clone() const = 0;
		virtual			unsigned		count() const = 0;
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
	 * floating value with measurement unit information
	 * @see VT_DOUBLE and VT_FLOAT in Value
	 */
	struct QualifiedValue
	{
		double			d;
		uint16_t		units;						/**< value form Units enumeration, see units.h */
	};

	/**
	 * representation of an element of enumeration
	 * @see VT_ENUM in Value
	 */
	struct VEnum
	{
		URIID			enumid;
		ElementID		eltid;
	};

	/**
	 * representation of a fixed size array
	 * @see VT_ARRAY in Value
	 */
	struct FixedArray
	{
		union {
			int32_t		*i;
			uint32_t	*ui;
			int64_t		*i64;
			uint64_t	*ui64;
			TIMESTAMP	*ts;
			float		*f;
			double		*d;
			PID			*id;
			void		*data;
		};
		uint16_t		xdim;			// vector size or matrix X-dimension
		uint16_t		ydim;			// matrix Y-dimension, 0 for vectors
		uint16_t		start;			// index of the first element for circular 1-dimensional arrays, set by the kernel
		uint16_t		units;			// (optional) units for VT_DOUBLE,VT_FLOAT
		uint8_t			type;			// data type of element arrays (VT_INT-VT_DOUBLE,VT_DATETIME,VT_REFID,VT_STRUCT)
		mutable uint8_t	flags;			// internal use
	};
	struct FixedStruct
	{
		struct StructDscr
		{
			PropertyID		property;
			uint16_t		length;
			uint8_t			type;
			uint8_t			flags;
		};
		uint32_t		nDscr;
		StructDscr		dscr[1];
		// nDscr StructDscr field descriptors followed by data elements
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
					uint8_t		fcalc	:1;
		mutable		uint8_t		flags	:7;
		union {
			const	char		*str;
			const	uint8_t		*bstr;
					int32_t		i;
					uint32_t	ui;
					int64_t		i64;
					uint64_t	ui64;
					TIMESTAMP	ts;
					VEnum		enu;
					float		f;
					double		d;
					PID			id;
					VPID		vpid;
					IPIN		*pin;
			const	RefVID		*refId;
					RefP		ref;
			const	Value		*varray;
					INav		*nav;
					IMap		*map;
					Value		*range;
					bool		b;
					URIID		uid;
					IdentityID	iid;
					IStreamRef	stream;
			class	IStmt		*stmt;
			class	IExpr		*expr;
			class	IExprNode	*exprt;
					RefV		refV;
					IndexSeg	iseg;
			QualifiedValue		qval;
			FixedArray			fa;
		};
	public:
		void	set(const char *s) {type=VT_STRING; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=s==NULL?0:(uint32_t)strlen(s); str=s; meta=0;}
		void	set(const char *s,uint32_t nChars) {type=VT_STRING; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=nChars; str=s; meta=0;}
		void	set(const unsigned char *bs,uint32_t l) {type=VT_BSTR; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=l; bstr=bs; meta=0;}
		void	set(int ii) {type=VT_INT; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(int32_t); i=ii; meta=0;}
		void	set(unsigned int u) {type=VT_UINT; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(uint32_t); ui=u; meta=0;}
		void	setI64(int64_t ii) {type=VT_INT64; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(int64_t); i64=ii; meta=0;}
		void	setU64(uint64_t u) {type=VT_UINT64; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(uint64_t); ui64=u; meta=0;}
		void	setEnum(URIID en,ElementID ei) {type=VT_ENUM; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(VEnum); enu.enumid=en; enu.eltid=ei; meta=0;}
		void	set(float fl,Units pu=Un_NDIM) {type=VT_FLOAT; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(float); f=fl; meta=0; qval.units=(uint16_t)pu;}
		void	set(double dd,Units pu=Un_NDIM) {type=VT_DOUBLE; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(double); d=dd; meta=0; qval.units=(uint16_t)pu;}
		void	set(IPIN *ip) {type=VT_REF; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(IPIN*); pin=ip; meta=0;}
		void	set(const PID& pi) {type=VT_REFID; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(PID); id=pi; meta=0;}
		void	set(const RefVID& re) {type=uint8_t(re.eid==STORE_COLLECTION_ID?VT_REFIDPROP:VT_REFIDELT); property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=1; refId=&re; meta=0;}
		void	set(const RefP& re) {type=uint8_t(re.eid==STORE_COLLECTION_ID?VT_REFPROP:VT_REFELT); property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=1; ref=re; meta=0;}
		void	setArray(void *arr,uint32_t nValues,uint16_t xdim,uint16_t ydim=1,ValueType vt=VT_DOUBLE,Units pu=Un_NDIM) {type=VT_ARRAY; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=nValues; fa.data=arr; fa.xdim=xdim; fa.ydim=ydim; fa.start=0; fa.units=pu; fa.type=vt; fa.flags=0; meta=0;}
		void	set(Value *coll,uint32_t nValues,uint8_t f=0) {type=VT_COLLECTION; property=STORE_INVALID_URIID; fcalc=f; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=nValues; varray=coll; meta=0;}
		void	set(INav *nv,uint8_t f=0) {type=VT_COLLECTION; property=STORE_INVALID_URIID; fcalc=f; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=~0u; nav=nv; meta=0;}
		void	setStruct(Value *coll,uint32_t nValues,uint8_t f=0) {type=VT_STRUCT; property=STORE_INVALID_URIID; fcalc=f; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=nValues; varray=coll; meta=0;}
		void	set(IMap *mp) {type=VT_MAP; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=1; map=mp; meta=0;}
		void	set(bool bl) {type=VT_BOOL; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(bool); b=bl; meta=0;}
		void	setDateTime(TIMESTAMP datetime) {type=VT_DATETIME; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(uint64_t); ts=datetime; meta=0;}
		void	setInterval(int64_t intvl) {type=VT_INTERVAL; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(int64_t); i64=intvl; meta=0;}
		void	set(IStream *strm,bool fPB=false) {type=VT_STREAM; property=STORE_INVALID_URIID; fcalc=fPB?1:0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=1; stream.is=strm; stream.prefix=NULL; meta=0;}
		void	set(IStmt *stm,uint8_t f=0) {type=VT_STMT; property=STORE_INVALID_URIID; fcalc=f; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=1; stmt=stm; meta=0;}
		void	set(IExprNode *ex) {type=VT_EXPRTREE; property=STORE_INVALID_URIID; fcalc=1; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=1; exprt=ex; meta=0;}
		void	set(IExpr *ex,uint8_t f=0) {type=VT_EXPR; property=STORE_INVALID_URIID; fcalc=f; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=1; expr=ex; meta=0;}
		void	setParam(unsigned char pn,ValueType ty=VT_ANY,unsigned short f=0) {type=VT_VARREF; property=STORE_INVALID_URIID; fcalc=1; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=0; refV.refN=pn; refV.type=(unsigned char)ty; refV.flags=(f&~VAR_TYPE_MASK)|VAR_PARAM; refV.id=STORE_INVALID_URIID; meta=0;}
		void	setVarRef(unsigned char vn,PropertyID id=STORE_INVALID_URIID,ValueType ty=VT_ANY,unsigned short f=0) {type=VT_VARREF; property=STORE_INVALID_URIID; fcalc=1; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=id!=STORE_INVALID_URIID?1:0; refV.refN=vn; refV.type=(unsigned char)ty; refV.flags=f&~VAR_TYPE_MASK; refV.id=id; meta=0;}
		void	setRange(Value *rng,uint8_t f=0) {type=VT_RANGE; property=STORE_INVALID_URIID; fcalc=f; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=2; range=rng; meta=0;}
		void	setURIID(URIID uri) {type=VT_URIID; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(uint32_t); uid=uri; meta=0;}
		void	setIdentity(IdentityID ii) {type=VT_IDENTITY; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=sizeof(uint32_t); iid=ii; meta=0;}
		void	setNow() {type=VT_CURRENT; i=CVT_TIMESTAMP; property=STORE_INVALID_URIID; fcalc=1; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=0; meta=0;}
		void	setCUser() {type=VT_CURRENT; i=CVT_USER; property=STORE_INVALID_URIID; fcalc=1; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=0; meta=0;}
		void	setCStore() {type=VT_CURRENT; i=CVT_STORE; property=STORE_INVALID_URIID; fcalc=1; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=0; meta=0;}
		void	setRename(PropertyID from,PropertyID to) {type=VT_URIID; property=from; fcalc=0; flags=0; op=OP_RENAME; eid=STORE_COLLECTION_ID; length=sizeof(PropertyID); uid=to; meta=0;}
		void	setDelete(PropertyID p,ElementID ei=STORE_COLLECTION_ID) {type=VT_ANY; property=p; fcalc=0; flags=0; op=OP_DELETE; eid=ei; length=0; meta=0;}
		void	setError(PropertyID pid=STORE_INVALID_URIID) {type=VT_ERROR; property=pid; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=0; meta=0;}
		void	setEmpty() {type=VT_ERROR; property=STORE_INVALID_URIID; fcalc=0; flags=0; op=OP_SET; eid=STORE_COLLECTION_ID; length=0; meta=0;}

		void		setOp(ExprOp o) {op=uint8_t(o);}
		void		setMeta(uint8_t mt) {meta=mt;}
		void		setPropID(PropertyID p) {property=p;}
		PropertyID	getPropID() const {return property;}
		operator	PropertyID() const {return property;}
		bool		isFTIndexable() const {return type==VT_STRING || (type==VT_STREAM && stream.is->dataType()==VT_STRING);}
		bool		isEmpty() const {return type==VT_ERROR;}
		bool		isNav() const {assert(type==VT_COLLECTION); return length==~0u;}
		unsigned	count() const {return type==VT_ANY?0u:type!=VT_COLLECTION?1u:length!=~0u?length:nav->count();}
		static	const Value *find(PropertyID pid,const Value *vals,unsigned nv) {
			if (vals!=NULL) while (nv!=0) {
				unsigned k=nv>>1; const Value *q=&vals[k]; 
				if (q->property<pid) {nv-=++k; vals+=k;} else if (q->property>pid) nv=k; else return q;
			}
			return NULL;
		}
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
		virtual	const PID&	getPID() const  = 0;						/**< returns PIN ID */
		virtual	bool		isLocal() const = 0;						/**< check the PIN is local, i.e. neither replicated, nor remoted cached */
		virtual	unsigned	getFlags() const = 0;						/**< retireve PIN flags (PIN_*) */
		virtual	unsigned	getMetaType() const = 0;					/**< retrieve PIN metatype as a set of flags (@see PMT_* flags) */
		virtual	uint32_t	getNumberOfProperties() const = 0;			/**< number of PIN properties */
		virtual	const Value	*getValueByIndex(unsigned idx) const = 0;	/**< get property value by index [0..getNumberOfProperties()-1] */
		virtual	const Value	*getValue(PropertyID pid) const = 0;		/**< get property value by property ID */

		virtual	RC			getPINValue(Value& res) const = 0;			/**< get PIN 'value', based on PROP_SPEC_VALUE */

		virtual	bool		testDataEvent(DataEventID,const Value *params=NULL,unsigned nParams=0) const = 0;										/**< test if the PIN would trigger a data event */
		virtual	bool		defined(const PropertyID *pids,unsigned nProps) const = 0;																/**< test if properties exists in the PIN */
		virtual	RC			isMemberOf(DataEventID *&devs,unsigned& ndevs) = 0;																		/**< returns an array of DataEventIDs for this PIN */

		virtual	RC			refresh(bool fNet=true) = 0;																							/**< refresh PIN, i.e. re-read properties from persistent storage */
		virtual	IPIN		*clone(const Value *overwriteValues=NULL,unsigned nOverwriteValues=0,unsigned mode=0) = 0;								/**< clone PIN (optionally modifying some properties) */
		virtual	IPIN		*project(const PropertyID *properties,unsigned nProperties,const PropertyID *newProps=NULL,unsigned mode=0) = 0;		/**< project PIN */
		virtual	RC			modify(const Value *values,unsigned nValues,unsigned mode=0,const ElementID *eids=NULL,unsigned *pNFailed=NULL) = 0;	/**< modify PIN, can be applied to uncommitted PINs */

		virtual	RC			setExpiration(uint32_t) = 0;				/**< set expiration time for cached remote PINs */
		virtual	RC			setNotification(bool fReset=false) = 0;		/**< switch notification generation on or off for this PIN */
		virtual	RC			setReplicated() = 0;						/**< set 'replicatable' status */
		virtual	RC			hide() = 0;									/**< exclude from indexes and hide */
		virtual	RC			deletePIN() = 0;							/**< soft-delete PIN from persistent stoarge */
		virtual	RC			undelete() = 0;								/**< undelete soft-deleted PIN */

		virtual	IMemAlloc	*getAlloc() const = 0;						/**< get memory allocator of this PIN */
		virtual	RC			allocSubPIN(unsigned nProps,IPIN *&pin,Value *&values,unsigned mode=0) const = 0;	/**< allocate space for a sub-PIN in the same heap */

		virtual	void		destroy() = 0;								/**< destroy this IPIN (doesn't delete it from DB) */
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
	
	extern "C" AFY_EXP size_t	errorToString(RC rc,const CompilationError *err,char buf[],size_t lbuf);		/**< convert error information to string */

	/**
	 * Expression tree node interface
	 * @see ISession::expr()
	 */
	class AFY_EXP IExprNode 
	{
	public:
		virtual ExprOp			getOp() const = 0;
		virtual	unsigned		getNumberOfOperands() const = 0;
		virtual	const	Value&	getOperand(unsigned idx) const = 0;
		virtual	unsigned		getFlags() const = 0;
		virtual	void			setFlags(unsigned flags,unsigned mask) = 0;
		virtual	IExpr			*compile() = 0;
		virtual	char			*toString(unsigned mode=0,const QName *qNames=NULL,unsigned nQNames=0) const = 0;
		virtual IExprNode		*clone() const = 0;
		virtual	void			destroy() = 0;
	};
	
	/**
	 * Compiled expression interface
	 * @see IExprNode::compile()
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
	
	/**
	 * INTO data events descriptor for INSERT
	 */
	#define	IC_UNIQUE		0x0001			/**< uniqueness constraint */
	#define	IC_IDEMPOTENT	0x0002			/**< idempotent insert */
	
	struct	IntoClass
	{
		DataEventID		cid;
		unsigned	flags;
	};

	/**
	 * Class specification for query variables
	 */
	struct AFY_EXP SourceSpec
	{
		URIID			objectID;			/**< named object PIN ID (class, template, comm, etc.) */
		unsigned		nParams;			/**< number of parameters for family reference */
		const	Value	*params;			/**< parameter values for family reference */
	};

	/**
	 * Path segment specification for query expressions and variables
	 */
	struct AFY_EXP PathSeg
	{
		unsigned		nPids;				/**< number of properties in pids array */
		union {
			PropertyID		pid;			/**< property ID */
			PropertyID		*pids;			/**< property IDs (if more than 1) */
		};
		Value			eid;				/**< collection/structure/map element index, if isEmpty() and eid is equal to STORE_COLLECTION_ID the whole collection/structure/map is used */
		IExpr			*filter;			/**< filter expression; applied to PINs refered by this segment collection or single reference */
		DataEventID			cid;				/**< class the above PINs must be members of */
		Value			*params;			/**< family parameter values */
		uint16_t		nParams;			/**< number of family parameters */
		uint16_t		rmin;				/**< minimum number of repetitions of this segment in valid path */
		uint16_t		rmax;				/**< maximum number of repetitions of this segment in valid path (0xFFFF - unlimited) */
		bool			fLast;				/**< if repeating, filtering must be applied only to the last segment */
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
		IExprNode	*expr;					/**< ordering expression, must be NULL is pid is set */
		PropertyID	pid;					/**< ordering property, must be STORE_INVALID_URIID if expr is not NULL */
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
		STMT_QUERY, STMT_INSERT, STMT_UPDATE, STMT_DELETE, STMT_UNDELETE, STMT_START_TX, STMT_COMMIT, STMT_ROLLBACK, STMT_OP_ALL
	};
	
	/**
	 * DISTINCT type
	 */
	enum DistinctType
	{
		DT_DEFAULT, DT_ALL, DT_DISTINCT, DT_FIRST
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
		virtual QVarID	addVariable(const SourceSpec *srcs=NULL,unsigned nSrcs=0,IExprNode *cond=NULL) = 0;				/**< add varaible either for full scan (no SourceSpec specified) or for intersection of given calsses/families with optional condition */
		virtual QVarID	addVariable(const PID& pid,PropertyID propID,IExprNode *cond=NULL) = 0;							/**< add collection scanning variable */
		virtual QVarID	addVariable(IStmt *qry) = 0;																	/**< add sub-query variable (e.g. nested SELECT in FROM */
		virtual	QVarID	setOp(QVarID leftVar,QVarID rightVar,QUERY_SETOP) = 0;											/**< add set operation (QRY_UNION, QRY_INTERSECT, QRY_EXCEPT) variable for 2 sub-variables*/
		virtual	QVarID	setOp(const QVarID *vars,unsigned nVars,QUERY_SETOP) = 0;										/**< add set operation (QRY_UNION, QRY_INTERSECT, QRY_EXCEPT) variable for multiple sub-variables*/
		virtual	QVarID	join(QVarID leftVar,QVarID rightVar,IExprNode *cond=NULL,QUERY_SETOP=QRY_SEMIJOIN,PropertyID=STORE_INVALID_URIID) = 0;		/**< set join variable for 2 sub-variables with optional condition */
		virtual	QVarID	join(const QVarID *vars,unsigned nVars,IExprNode *cond=NULL,QUERY_SETOP=QRY_SEMIJOIN,PropertyID=STORE_INVALID_URIID) = 0;	/**< set join variable for multiple sub-variables with optional condition */
		virtual	RC		setName(QVarID var,const char *name) = 0;														/**< set variable name string to be used in query rendering (i.e. FROM ... AS name ) */
		virtual	RC		setDistinct(QVarID var,DistinctType) = 0;														/**< set DISTINCT mode for a variable */
		virtual	RC		addOutput(QVarID var,const Value *dscr,unsigned nDscr) = 0;										/**< add output descriptior */
		virtual RC		addCondition(QVarID var,IExprNode *cond) = 0;													/**< add a condition (part of WHERE clause); multiple conditions are AND-ed */
		virtual	RC		addConditionFT(QVarID var,const char *str,unsigned flags=0,const PropertyID *pids=NULL,unsigned nPids=0) = 0;	/**< add free-text search string and optional property filter for a simple variable */
		virtual	RC		setPIDs(QVarID var,const PID *pids,unsigned nPids) = 0;											/**< set PIN ID array filter for a simple (not-join, not-set) variable */
		virtual	RC		setPath(QVarID var,const PathSeg *segs,unsigned nSegs) = 0;										/**< set path expression for a simple variable */
		virtual	RC		setExpr(QVarID var,const Value& exp) = 0;														/**< set a generic expression for a simple variable */
		virtual	RC		setPropCondition(QVarID var,const PropertyID *props,unsigned nProps,bool fOr=false) = 0;		/**< set property existence condition, i.e. EXISTS(prop1) AND EXISTS(prop2) ... */
		virtual	RC		setJoinProperties(QVarID var,const PropertyID *props,unsigned nProps) = 0;						/**< set properties for natural join or USING(...) */
		virtual	RC		setGroup(QVarID,const OrderSeg *order,unsigned nSegs,IExprNode *having=NULL) = 0;				/**< set GROUP BY clause */
		virtual	RC		setOrder(const OrderSeg *order,unsigned nSegs) = 0;												/**< set statement-wide result ordering */
		virtual	RC		setValues(const Value *values,unsigned nValues,const IntoClass *into=NULL,unsigned nInto=0,uint64_t tid=0ULL) =  0;		/**< set Value structures, class filter and PIN flags for STMT_INSERT and STMT_UPDATE statements */
		virtual	RC		setWith(const Value *params,unsigned nParams) =  0;												/**< set WITH parameters for the statement evaluation */

		virtual	STMT_OP	getOp() const = 0;																										/**< get statement type */

		virtual	RC		execute(ICursor **result=NULL,const Value *params=NULL,unsigned nParams=0,unsigned nProcess=~0u,unsigned nSkip=0,		/**< execute statement, return result set iterator or number of affected PINs */
										unsigned mode=0,uint64_t *nProcessed=NULL,TXI_LEVEL=TXI_DEFAULT) const = 0;
		virtual	RC		asyncexec(IStmtCallback *cb,const Value *params=NULL,unsigned nParams=0,unsigned nProcess=~0u,unsigned nSkip=0,			/**< execute statement asynchronously */
										unsigned mode=0,TXI_LEVEL=TXI_DEFAULT) const = 0;
		virtual	RC		execute(IStreamOut*& result,const Value *params=NULL,unsigned nParams=0,unsigned nProcess=~0u,unsigned nSkip=0,			/**< execute statement, return results as a protobuf stream */
										unsigned mode=0,TXI_LEVEL=TXI_DEFAULT) const = 0;
		virtual	RC		count(uint64_t& cnt,const Value *params=NULL,unsigned nParams=0,unsigned nAbort=~0u,									/**< count statement results */
										unsigned mode=0,TXI_LEVEL=TXI_DEFAULT) const = 0;
		virtual	RC		exist(const Value *params=NULL,unsigned nParams=0,unsigned mode=0,TXI_LEVEL=TXI_DEFAULT) const = 0;						/**< check if any PINs satfisfy query conditions */
		virtual	RC		analyze(char *&plan,const Value *pars=NULL,unsigned nPars=0,unsigned md=0) const = 0;									/**< return execution plan for the statement */

		virtual	bool	isSatisfied(const IPIN *pin,const Value *pars=NULL,unsigned nPars=0,unsigned mode=0) const = 0;							/**< check if a given PIN satisfies query conditions */
		virtual	char	*toString(unsigned mode=0,const QName *qNames=NULL,unsigned nQNames=0) const = 0;										/**< convert statement to PathSQL string */

		virtual	IStmt	*clone(STMT_OP=STMT_OP_ALL) const = 0;															/**< clone IStmt object */
		virtual	void	destroy() = 0;																					/**< destroy IStmt object */
	};
	
	/**
	 * Event types in event specifications
	 * @see EventSpec
	 */
	enum EvType
	{
		EvNone,								/**< no-event (for use in FSMs) */
		EvClass,							/**< class event */
		EvProp,								/**< property modification */
		EvTime,								/**< interval expiration */
		EvExt,								/**< external I/O events reported by various listeners */
		EvSubs,								/**< events related to generic subscription topics, e.g. in publish/subscribe protocols */
		EvMask			=0x0FFF,			/**< basic event type mask */
		EvLeave			=0x2000,			/**< bit: instance or property is removed */
		EvUpdate		=0x4000,			/**< bit: instance or property is modified */
		EvNew			=0x8000,			/**< bit: instance or property is added */
	};
	 
	/**
	 * bits for EventSpec::flags
	 */
	#define	EV_LAST_ELEMENT		0x01		/**< with EvProp - last element of collection */
	#define	EV_FIRST_ELEMENT	0x02		/**< with EvProp - first element of collection */
	#define	EV_DROP_FIRST_EVENT	0x04		/**< when event queue overflows - drop first rather than last element */
	#define	EV_EXIT				0x08		/**< exit event handler/FSM */
	 
	/**
	 * Event specification
	 * fits into uint64_t and stored as VT_UINT64 in PROP_SPEC_EVENT
	 */
	union EventSpec
	{
		struct {
			uint16_t		evType;
			uint8_t			nQEvents;
			uint8_t			flags;
			union {
				URIID		uid;
				uint32_t	itv;
			};
		};
		uint64_t			u64;
	};

	struct ActionDescriptor
	{
		unsigned	nActions;
		bool		fOnce;
		IStmt		*actions[1];
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
		unsigned	arrayThreshold;
		size_t		ssvThreshold;
		float		pctPageFree;
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
	 * map element declaration 
	 * @see ISession::createMap()
	 */
	struct MapElt
	{
		Value	key;
		Value	val;
	};

	/**
	 * trace interface
	 */
	class ITrace
	{
	public:
		virtual	void	trace(long code,const char *msg,va_list) = 0;
	};
	
	/**
	 * PIN batch insert interface
	 */
	class IBatch : public IMemAlloc
	{
	public:
		virtual	unsigned	getNumberOfPINs() const = 0;															/**< get number of PINs in the batch */
		virtual	size_t		getSize() const = 0;																	/**< get current batch size in memory */
		virtual	Value		*createValues(uint32_t nValues) = 0;													/**< Value array creation helper */
		virtual	RC			createPIN(Value *values,unsigned nValues,unsigned mode=0,const PID *original=NULL) = 0; /**< create a PIN in the batch */
		virtual	RC			addRef(unsigned from,const Value& to,bool fAdd=true) = 0;								/**< add a reference from 'from' PIN to 'to' PIN (PIN id, PIN * or index in the batch) */
		virtual	RC			clone(IPIN *pin,const Value *values=NULL,unsigned nValues=0,unsigned mode=0) = 0;		/**< clone a pin into this batch */
		virtual	RC			process(bool fDestroy=true,unsigned mode=0,const AllocCtrl* =NULL,const IntoClass *into=NULL,unsigned nInto=0) = 0;	/**< insert all batched PINs (and optionally destroy the batch) */
		virtual	RC			getPIDs(PID *pids,unsigned& nPIDs,unsigned start=0) const = 0;							/**< return PIN ids after call to process */
		virtual	RC			getPID(unsigned idx,PID& pid) const = 0;												/**< return PIN id after call to process (by PIN index) */
		virtual	const Value *getProperty(unsigned idx,URIID pid) const = 0;											/**< return PIN property (by PIN index) */
		virtual	ElementID	getEIDBase() const = 0;																	/**< return base number used to generate element ids in collections */
		virtual	void		destroy() = 0;																			/**< destroy the batch */
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
	class AFY_EXP ISession : public IMemAlloc
	{
	public:
		virtual	class IAffinity	*getAffinity() const = 0;														/**< retrieve store context */
		virtual	void		freeMemory() = 0;																	/**< free ISession memory; all objects are discarded */							
		virtual	ISession	*clone(const char * =NULL) const = 0;												/**< clone existing session */
		virtual	RC			attachToCurrentThread() = 0;														/**< in server: attach ISession to current worker thread (ISession can be accessed concurrently only from one thread */
		virtual	RC			detachFromCurrentThread() = 0;														/**< in server: detach ISession from current worker thread */

		virtual	void		setInterfaceMode(unsigned flags) = 0;												/**< set interface mode, see ITF_XXX flags above */
		virtual	unsigned	getInterfaceMode() const = 0;														/**< get current interface mode */
		virtual	void		setDefaultExpiration(uint64_t defExp) = 0;											/**< set default expiration time for cached remore PINs */
		virtual	void		changeTraceMode(unsigned mask,bool fReset=false) = 0;								/**< change trace mode, see TRACE_XXX flags above  */
		virtual	void		setTrace(ITrace *) = 0;																/**< set query execution trace interface */
		virtual	void		terminate() = 0;																	/**< terminate session; drops the session */

		virtual	RC			mapURIs(unsigned nURIs,URIMap URIs[],const char *URIBase=NULL,bool fObj=false) = 0;	/**< maps URI strings to their internal URIID values used throughout the interface */
		virtual	RC			getURI(URIID,char *buf,size_t& lbuf,bool fFull=false) = 0;							/**< get URI string by its URIID value; if buf==NULL return URI string length */

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

		virtual	IExprNode	*expr(ExprOp op,unsigned nOperands,const Value *operands,unsigned flags=0) = 0;		/**< create expression tree node */

		virtual	IStmt		*createStmt(STMT_OP=STMT_QUERY,unsigned mode=0) = 0;								/**< create IStmt object */
		virtual	IStmt		*createStmt(const char *queryStr,const URIID *ids=NULL,unsigned nids=0,CompilationError *ce=NULL) = 0;	/**< create IStmt from PathSQL string */
		virtual	IExprNode	*createExprTree(const char *str,const URIID *ids=NULL,unsigned nids=0,CompilationError *ce=NULL) = 0;	/**< create expression tree from PathSQL string */
		virtual	IExpr		*createExpr(const char *str,const URIID *ids=NULL,unsigned nids=0,CompilationError *ce=NULL) = 0;		/**< create compiled expression from PathSQL string */
		virtual	IExpr		*createExtExpr(uint16_t langID,const unsigned char *body,uint32_t lBody,uint16_t flags) = 0;			/**< create 'external expression' for loadable language libraries */
		virtual	RC			getTypeName(ValueType type,char buf[],size_t lbuf) = 0;
		virtual	void		abortQuery() = 0;

		virtual	RC			execute(const char *str,size_t lstr,char **result=NULL,const URIID *ids=NULL,unsigned nids=0,			/**< execute query specified by PathSQL string; return result in JSON form */
									const Value *params=NULL,unsigned nParams=0,CompilationError *ce=NULL,uint64_t *nProcessed=NULL,
									unsigned nProcess=~0u,unsigned nSkip=0,const char *importBase=NULL) = 0;

		virtual	RC			createInputStream(IStreamIn *&in,IStreamIn *out=NULL,size_t lbuf=0) = 0;			/**< create protobuf input stream interface */
		virtual	RC			createServiceCtx(const Value *vals,unsigned nVals,class IServiceCtx *&sctx,bool fWrite=false,class IListener *lctx=NULL) = 0; /**< create IServiceCtx from provided parameters */

		virtual	RC			getDataEventID(const char *className,DataEventID& cid) = 0;									/**< get DataEventID for gived class URI; equivalent to mapURIs() but check class existence */
		virtual	RC			enableClassNotifications(DataEventID,unsigned notifications) = 0;						/**< enables notifications for a given class, see CLASS_NOTIFY_XXX above */
		virtual	RC			rebuildIndices(const DataEventID *cidx=NULL,unsigned ndevs=0) = 0;					/**< rebuild all DB indices (except free-text index) */
		virtual	RC			rebuildIndexFT() = 0;																/**< rebuild free-text index */
		virtual	RC			createIndexNav(DataEventID,IndexNav *&nav) = 0;											/**< create IndexNav object */
		virtual	RC			listValues(DataEventID cid,PropertyID pid,IndexNav *&ven) = 0;							/**< list all stored values for a given class family */
		virtual	RC			listWords(const char *query,StringEnum *&sen) = 0;									/**< list all words in FT index matching given prefix or list of words */
		virtual	RC			getDataEventInfo(DataEventID,IPIN*&) = 0;													/**< get class information */

		virtual	RC			allocPIN(size_t maxSize,unsigned nProps,IPIN *&pin,Value *&values,unsigned mode=0) = 0;	/**< fast allocate space for a PIN which can be passed between sessions, up to 64K (for use in services mainly) */
		virtual	RC			inject(IPIN *pin) = 0;																/**< inject a new PIN into the system triggering various events */
		virtual	RC			createEventHandler(const EventSpec evdesc[],unsigned nDesc,RC (*callback)(/*???*/)) = 0; /**< create an event handler with a callback function (normally used by services) */
		virtual	RC			createEventHandler(const EventSpec evdesc[],unsigned nDesc,ActionDescriptor[]) = 0; /**< create an event handler defined by an array of ActionDescrition structures */

		virtual	IPIN		*getPIN(const PID& id,unsigned mode=0) = 0;											/**< retrieve a PIN by its ID */
		virtual	IPIN		*getPIN(const Value& ref,unsigned mode=0) = 0;										/**< retrive a PIN by its reference in a Value */
		virtual	RC			getValue(Value& res,const PID& id,PropertyID,ElementID=STORE_COLLECTION_ID) = 0;	/**< get property value for a value reference */
		virtual	RC			getPINEvents(DataEventID *&devs,unsigned& ndevs,const PID& id) = 0;					/**< array of DataEventIDs for the PIN */
		virtual	bool		isCached(const PID& id) = 0;														/**< check if a PIN is in remote PIN cache by its ID */
		virtual	IBatch		*createBatch() = 0;																	/**< create PIN batch insert interface */
		virtual	RC			createPIN(Value *values,unsigned nValues,IPIN **result,unsigned mode=0,const PID *original=NULL) = 0; /**< create a PIN */
		virtual	RC			modifyPIN(const PID& id,const Value *values,unsigned nValues,unsigned mode=0,const ElementID *eids=NULL,unsigned *pNFailed=NULL,const Value *params=NULL,unsigned nParams=0) = 0;	/**< modify committed or uncommitted PIN */
		virtual	RC			deletePINs(IPIN **pins,unsigned nPins,unsigned mode=0) = 0;							/**< (soft-)delete or purge committed PINs from persistent memory */
		virtual	RC			deletePINs(const PID *pids,unsigned nPids,unsigned mode=0) = 0;						/**< (soft-)delete or purge committed PINs from persistent memory by their IDs */
		virtual	RC			undeletePINs(const PID *pids,unsigned nPids) = 0;									/**< undelete soft-deleted PINs */

		virtual	RC			startTransaction(TX_TYPE=TXT_READWRITE,TXI_LEVEL=TXI_DEFAULT) = 0;					/**< start transaction, READ-ONLY or READ_WRITE */
		virtual	RC			commit(bool fAll=false) = 0;														/**< commit transaction */
		virtual	RC			rollback(bool fAll=false) = 0;														/**< rollback (abort) transaction */
		virtual	void		setLimits(unsigned xSyncStack,unsigned xOnCommit) = 0;								/**< set transaction guard limits */

		virtual	RC			reservePages(const uint32_t *pages,unsigned nPages) = 0;							/**< used in dump/load entire store */

		virtual	RC			createMap(const MapElt *elts,unsigned nElts,IMap *&map,bool fCopy=true) = 0;		/**< create a map (VT_MAP type) */
		virtual	RC			copyValue(const Value& src,Value& dest) = 0;										/**< copy Value to session memory */
		virtual	RC			copyValues(const Value *src,unsigned nValues,Value *&dest) = 0;						/**< copy an array of Value structures to session memory */
		virtual	RC			convertValue(const Value& oldValue,Value& newValue,ValueType newType) = 0;			/**< convert Value type */
		virtual	RC			parseValue(const char *p,size_t l,Value& res,CompilationError *ce=NULL) = 0;		/**< parse a string to Value */
		virtual	RC			parseValues(const char *p,size_t l,Value *&res,unsigned& nValues,CompilationError *ce=NULL,char delimiter=',') = 0;	/**< parse a string containing multiple values in PathSQL format */
		virtual	int			compareValues(const Value& v1,const Value& v2,bool fNCase=false) = 0;				/**< compare 2 Values (can be of different types) */
		virtual	void		freeValues(Value *vals,unsigned nVals) = 0;											/**< free an array of Value structures allocated in session memory */
		virtual	void		freeValue(Value& val) = 0;															/**< free a Value structure with data allocation in session memory */

		virtual	char		*convToJSON(const Value& v) = 0;													/**< convert a value to JSON representation */

		virtual	void		setTimeZone(int64_t tzShift) = 0;													/**< set session time zone */
		virtual	RC			convDateTime(TIMESTAMP dt,DateTime& dts,bool fUTC=true) const = 0;					/**< convert timestamp from internal representation to DateTiem structure */
		virtual	RC			convDateTime(const DateTime& dts,TIMESTAMP& dt,bool fUTC=true) const = 0;			/**< convert tiemstamp from DateTime to internal representation */

		virtual	uint64_t	getCodeTrace() = 0;																	/**< used for performance Affinity tracing */
	};

	/**
	 * Service interface flags
	 */
	#define	ISRV_ENDPOINT		0x00000001		/**< end-point service (can be in the middle of the stack) */
	#define	ISRV_READ			0x00000002		/**< read stack service */
	#define	ISRV_WRITE			0x00000004		/**< write stack service */
	#define	ISRV_SERVER			0x00000008		/**< server service: processes request and returns result to be passed back to caller */
	#define	ISRV_REQUEST		0x00000010		/**< request parsing or rendering */
	#define	ISRV_RESPONSE		0x00000020		/**< response parsing or rendering */

	#define	ISRV_ALLOCBUF		0x00000100		/**< output buffer to be allocated before call to invoke() */
	#define	ISRV_NOCACHE		0x00000200		/**< service processor cannot be cached between calls */
	#define	ISRV_ENVELOPE		0x00000400		/**< service augments a buffer with a header and/or trailer */
	#define	ISRV_TRANSIENT		0x00000800		/**< transient listener: one event only */
	#define	ISRV_ERROR			0x00001000		/**< service can report an error to the client */
	
	/**
	 * IService::Processor::invoke() 'mode' parameter flags
	 */
	#define	ISRV_NEEDMORE		0x00010000		/**< need more input to finish output */
	#define	ISRV_APPEND			0x00020000		/**< append data to previous buffer */
	#define	ISRV_NEEDFLUSH		0x00040000		/**< a service has output which needs to be flushed */
	#define	ISRV_REFINP			0x00080000		/**< output refers to input (buffer or Value) */
	#define	ISRV_MOREOUT		0x00100000		/**< continue processing input/partially processed input */
	#define	ISRV_KEEPINP		0x00200000		/**< keep input; call with the same input again */
	#define	ISRV_CONSUMED		0x00400000		/**< buffer is stored (usually write endpoint); don't use it anymore */
	#define	ISRV_WAIT			0x00800000		/**< endpoint: wait for read operation to return data or for write operation to finish */
	#define	ISRV_EOM			0x01000000		/**< end-of-message marker encountered on read or needs to be added on write */
	#define	ISRV_FLUSH			0x02000000		/**< flush marker encountered in a stream */
	#define	ISRV_SKIP			0x04000000		/**< skip this service when processing more data */

	#define	ISRV_PROC_MASK		(ISRV_ENDPOINT|ISRV_READ|ISRV_WRITE)
	#define	ISRV_PROC_MODE		(ISRV_PROC_MASK|ISRV_REQUEST|ISRV_RESPONSE|ISRV_ALLOCBUF|ISRV_NOCACHE|ISRV_ENVELOPE|ISRV_TRANSIENT|ISRV_ERROR|ISRV_WAIT)

	/**
	 * types of service errors
	 */
	enum ServiceErrorType
	{
		SET_COMM,				/**< communication error reported by endpoint, e.g. connection reset, access denied etc. */
		SET_FORM,				/**< malformed request or response */
		SET_AUTH,				/**< authentication error, e.g. incorrectly signed message */
		SET_MISMATCH,			/**< mismatched request-response token */
		SET_REMOTE,				/**< well-formed response containing remote error information, e.g. HTTP 3XX-5XX codes */
		SET_LOCAL				/**< incorrect data passed by the local program, e.g. missing property, incorrect property type or value */
	};

	/**
	 * result allocator interface for structured output
	 * used by services to allocate memory for results returned from invoke()
	 */
	class IResAlloc : public IMemAlloc
	{
	public:
		virtual	Value		*createValues(uint32_t nValues) = 0;														/**< Value array creation helper */
		virtual	RC			createPIN(Value& result,Value *values,unsigned nValues,const PID *id=NULL,unsigned mode=0) = 0;	/**< create a result PIN */
	};

	/**
	 * Service stack invocation context
	 */
	class AFY_EXP IServiceCtx : public IMemAlloc
	{
	public:
		virtual	RC				invoke(const Value *vals,unsigned nVals,Value *res=NULL) = 0;	/**< invoke services in this context */
		virtual	void			error(ServiceErrorType etype,RC rc,const Value *info=NULL) = 0;	/**< service calls to report an error */
		virtual	ISession		*getSession() const = 0;										/**< get current session */
		virtual	const	Value	*getParameter(URIID prop) const = 0;							/**< get parameter of this service invokation */
		virtual	void			getParameters(Value *vals,unsigned nVals) const = 0;			/**< get parameters of the stack invocation */
		virtual	IResAlloc		*getResAlloc() = 0;												/**< return allocator interface for structured results */
		virtual	RC				expandBuffer(Value&,size_t extra=0) = 0;						/**< expand output buffer */
		virtual	void			releaseBuffer(void *buf,size_t lbuf) = 0;						/**< return 'consumed' buffer for reuse */
		virtual	void			setReadMode(bool fWait) = 0;									/**< set endpoint read mode: wait/nowait */
		virtual	void			setKeepalive(bool fSet=true) = 0;								/**< set keep-alive mode for connection-based network protocols */
		virtual	URIID			getEndpointID(bool fOut=false) const = 0;						/**< returns endpoint URIID for this context (if any); fOut=true is used for ISRV_SERVER contexts, when output endpoint is different */
		virtual	IPIN			*getCtxPIN() = 0;												/**< get context PIN */
		virtual	RC				getOSError() const = 0;											/**< get OS specific error and convert to RC code */
		virtual	void			destroy() = 0;													/**< destroy this service context after use */
	};

	/**
	 * Abstract address encapsulation interface
	 */
	class AFY_EXP IAddress
	{
	public:
		virtual	bool operator==(const IAddress&) const	= 0;		/**< equality comparison */
		virtual	int	cmp(const IAddress&) const = 0;					/**< -1,0,+1 for ordered structures */
		virtual	operator uint32_t() const = 0;						/**< hash function for hash tables */
	};

	/**
	 * Service interface
	 */
	class AFY_EXP IService
	{
	public:
		class AFY_EXP Processor {
		public:
			virtual	RC		connect(IServiceCtx *ctx);
			virtual	RC		invoke(IServiceCtx *ctx,const Value& inp,Value& out,unsigned& mode) = 0;
			virtual	void	cleanup(IServiceCtx *ctx,bool fDestroying);
		};
		virtual	RC			create(IServiceCtx *ctx,uint32_t& dscr,Processor *&ret);
		virtual	RC			listen(ISession *ses,URIID id,const Value *params,unsigned nParams,const Value *srvParams,unsigned nSrvparams,unsigned mode,class IListener *&ret);
		virtual	RC			resolve(ISession *ses,const Value *vals,unsigned nVals,IAddress& res);
		virtual	size_t		getBufSize() const;
		virtual	void		getEnvelope(size_t& lHeader,size_t& lTrailer) const;
		virtual	void		getSocketDefaults(int& protocol,uint16_t& port) const;
		virtual	void		shutdown();
	};
	
	/**
	 * Listener interface
	 */
	class AFY_EXP IListener
	{
	public:
		virtual	IService	*getService() const = 0;
		virtual	URIID		getID() const = 0;
		virtual	RC			create(IServiceCtx *ctx,uint32_t& dscr,IService::Processor *&ret) = 0;
		virtual	RC			stop(bool fSuspend=false) = 0;
	};

	/**
	 * Listener initialization notification
	 */
	class AFY_EXP IListenerNotification
	{
	public:
		virtual	RC	onListener(ISession *ses,URIID sid,const Value *vals,unsigned nVals,const Value *srvInfo=NULL,unsigned nSrvInfo=0,bool fStop=false) = 0;
	};

	/**
	 * external language interpreter interface
	 */
	class IStoreLang
	{
	public:
	};

	/**
	 * current store state
	 * @see IAffinity::getState()
	 */
	#define	SSTATE_OPEN					0x0001											/**< store is loaded, no external access operations */
	#define	SSTATE_READONLY				0x0002											/**< only read operations so far */
	#define	SSTATE_NORESOURCES			0x0004											/**< critical NORESOURCE situation, e.g. no space for log files; only read access is allowed */
	#define	SSTATE_IN_SHUTDOWN			0x0008											/**< store is being shutdown */
	#define	SSTATE_MODIFIED				0x0010											/**< data was modified */

	class IAfySocket;

	class AFY_EXP IAffinity : public IMemAlloc
	{
	public:
		virtual	ISession	*startSession(const char *identityName=NULL,const char *password=NULL,size_t initMem=0) = 0;				/**< start new session, optional login, optional stack memory allocation */
		virtual	unsigned	getState() const = 0;																						/**< get current store state asynchronously */
		virtual	size_t		getPublicKey(uint8_t *buf,size_t lbuf,bool fB64=false) = 0;													/**< get store public key */
		virtual	uint64_t	getOccupiedMemory() const = 0;																				/**< for inmem store: return currently used memory */
		virtual	void		changeTraceMode(unsigned mask,bool fReset=false) = 0;														/**< change trace mode, see TRACE_XXX flags above  */
		virtual	RC			registerLangExtension(const char *langID,IStoreLang *ext,URIID *pID=NULL) = 0;								/**< register external langauge interpreter */
		virtual	RC			registerService(const char *sname,IService *handler,URIID *puid=NULL,IListenerNotification *lnot=NULL) = 0;	/**< register a handler for external actions by name */
		virtual	RC			registerService(URIID serviceID,IService *handler,IListenerNotification *lnot=NULL) = 0;					/**< register a handler for external actions */
		virtual	RC			unregisterService(const char *sname,IService *handler=NULL) = 0;											/**< remove a handler for external actions, if handler==NULL - all handlers for this service */
		virtual	RC			unregisterService(URIID serviceID,IService *handler=NULL) = 0;												/**< remove a handler for external actions, if handler==NULL - all handlers for this service */
		virtual	RC			registerSocket(IAfySocket *sock) = 0;																		/**< registering socket callback interface (see afysock.h for details */
		virtual	void		unregisterSocket(IAfySocket *sock,bool fClose) = 0;															/**< unregistering socket callback interface (see afysock.h for details */
		virtual	RC			registerPrefix(const char *qs,size_t lq,const char *str,size_t ls) = 0;										/**< for service libraries - to register their qname prefixes */
		virtual	RC			registerFunction(const char *fname,RC (*func)(Value& arg,const Value *moreArgs,unsigned nargs,unsigned mode,ISession *ses),URIID serviceID) = 0;	/**< register external function by name */
		virtual	RC			registerFunction(URIID funcID,RC (*func)(Value& arg,const Value *moreArgs,unsigned nargs,unsigned mode,ISession *ses),URIID serviceID) = 0;			/**< register external function */
		virtual	RC			registerOperator(ExprOp op,RC (*func)(Value& arg,const Value *moreArgs,unsigned nargs,unsigned mode,ISession *ses),
											URIID serviceID,int nargs=-1,unsigned ntypes=0,const ValueType **argTypes=NULL) = 0;		/**< register external operator */
		virtual	RC			shutdown() = 0;																								/**< shutdowns store instance */
	};
};

#ifdef _MSC_VER
#pragma warning(disable:4291)
#pragma warning(disable:4251)
#pragma warning(disable:4355)
#pragma	warning(disable:4181)
#pragma	warning(disable:4512)
#pragma	warning(disable:4100)
#else
#define __forceinline	inline
#endif

__forceinline void*	operator new(size_t s,Afy::IMemAlloc *ma) throw() {return ma->malloc(s);}
__forceinline void*	operator new[](size_t s,Afy::IMemAlloc *ma) throw() {return ma->malloc(s);}

#endif
