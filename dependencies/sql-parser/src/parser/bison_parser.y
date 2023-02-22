%{
/**
 * bison_parser.y
 * defines bison_parser.h
 * outputs bison_parser.c
 *
 * Grammar File Spec: http://dinosaur.compilertools.net/bison/bison_6.html
 *
 */
/*********************************
 ** Section 1: C Declarations
 *********************************/

#include "bison_parser.h"
#include "flex_lexer.h"

#include <stdio.h>
#include <string.h>

using namespace hsql;

int yyerror(YYLTYPE* llocp, SQLParserResult* result, yyscan_t scanner, const char *msg) {
	result->setIsValid(false);
	result->setErrorDetails(strdup(msg), llocp->first_line, llocp->first_column);
	return 0;
}

%}
/*********************************
 ** Section 2: Bison Parser Declarations
 *********************************/


// Specify code that is included in the generated .h and .c files
%code requires {
// %code requires block

#include "sql/statements.h"
#include "SQLParserResult.h"
#include "parser_typedef.h"

// Auto update column and line number
#define YY_USER_ACTION \
		yylloc->first_line = yylloc->last_line; \
		yylloc->first_column = yylloc->last_column; \
		for(int i = 0; yytext[i] != '\0'; i++) { \
			yylloc->total_column++; \
			yylloc->string_length++; \
				if(yytext[i] == '\n') { \
						yylloc->last_line++; \
						yylloc->last_column = 0; \
				} \
				else { \
						yylloc->last_column++; \
				} \
		}
}

// Define the names of the created files (defined in Makefile)
// %output  "bison_parser.cpp"
// %defines "bison_parser.h"

// Tell bison to create a reentrant parser
%define api.pure full

// Prefix the parser
%define api.prefix {hsql_}
%define api.token.prefix {SQL_}

%define parse.error verbose
%locations

%initial-action {
	// Initialize
	@$.first_column = 0;
	@$.last_column = 0;
	@$.first_line = 0;
	@$.last_line = 0;
	@$.total_column = 0;
	@$.string_length = 0;
};


// Define additional parameters for yylex (http://www.gnu.org/software/bison/manual/html_node/Pure-Calling.html)
%lex-param   { yyscan_t scanner }

// Define additional parameters for yyparse
%parse-param { hsql::SQLParserResult* result }
%parse-param { yyscan_t scanner }


/*********************************
 ** Define all data-types (http://www.gnu.org/software/bison/manual/html_node/Union-Decl.html)
 *********************************/
%union {
	double fval;
	int64_t ival;
	char* sval;
	uintmax_t uval;
	bool bval;
	char* token_str;

	hsql::SQLStatement* statement;
	hsql::SelectStatement* 	select_stmt;
	hsql::ImportStatement* 	import_stmt;
	hsql::CreateStatement* 	create_stmt;
	hsql::InsertStatement* 	insert_stmt;
	hsql::DeleteStatement* 	delete_stmt;
	hsql::UpdateStatement* 	update_stmt;
	hsql::DropStatement*   	drop_stmt;
	hsql::PrepareStatement* prep_stmt;
	hsql::ExecuteStatement* exec_stmt;
	hsql::ShowStatement*    show_stmt;

	hsql::TableName table_name;
	hsql::TableRef* table;
	hsql::Expr* expr;
	hsql::OrderDescription* order;
	hsql::OrderType order_type;
	hsql::WithDescription* with_description_t;
	hsql::DatetimeField datetime_field;
	hsql::LimitDescription* limit;
	hsql::ColumnDefinition* column_t;
	hsql::ColumnType column_type_t;
	hsql::GroupByDescription* group_t;
	hsql::UpdateClause* update_t;
	hsql::Alias* alias_t;

	std::vector<hsql::SQLStatement*>* stmt_vec;

	std::vector<char*>* str_vec;
	std::vector<hsql::TableRef*>* table_vec;
	std::vector<hsql::ColumnDefinition*>* column_vec;
	std::vector<hsql::UpdateClause*>* update_vec;
	std::vector<hsql::Expr*>* expr_vec;
	std::vector<hsql::OrderDescription*>* order_vec;
	std::vector<hsql::WithDescription*>* with_description_vec;
}


/*********************************
 ** Destructor symbols
 *********************************/
%destructor { } <fval> <ival> <uval> <bval> <order_type> <datetime_field> <column_type_t>
%destructor { free( ($$.name) ); free( ($$.schema) ); } <table_name>
%destructor { free( ($$) ); } <sval>
%destructor {
	if (($$) != nullptr) {
		for (auto ptr : *($$)) {
			delete ptr;
		}
	}
	delete ($$);
} <str_vec> <table_vec> <column_vec> <update_vec> <expr_vec> <order_vec> <stmt_vec>
%destructor { delete ($$); } <*>


/*********************************
 ** Token Definition
 *********************************/
%token <sval> IDENTIFIER STRING
%token <fval> FLOATVAL
%token <ival> INTVAL

/* SQL Keywords */
%token	ACCESSIBLE
%token	ACTION
%token	ADD
%token	ADDDATE
%token	ADMIN
%token	AFTER
%token	AGAINST
%token	AGGREGATE
%token	ALGORITHM
%token	ALL
%token	ALTER
%token	ALWAYS
%token	ANALYZE
%token	AND
%token	ANY
%token	ARRAY
%token	AS
%token	ASC
%token	ASCII
%token	ASENSITIVE
%token	AT
%token	ATOMIC
%token	AUTHORS
%token	AUTO
%token	AUTO_INCREMENT
%token	AUTOEXTEND_SIZE
%token	AVG
%token	AVG_ROW_LENGTH
%token	BACKUP
%token	BEFORE
%token	BEGIN
%token	BETWEEN
%token	BIGINT
%token	BINARY
%token	BINLOG
%token	BIT
%token	BIT_AND
%token	BIT_OR
%token	BIT_XOR
%token	BLOB
%token	BLOCK
%token	BODY
%token	BOOL
%token	BOOLEAN
%token	BOTH
%token	BTREE
%token	BY
%token	BYTE
%token	CACHE
%token	CALL
%token	CASCADE
%token	CASCADED
%token	CASE
%token	CAST
%token	CATALOG_NAME
%token	CHAIN
%token	CHANGE
%token	CHANGED
%token	CHAR
%token	CHARACTER
%token	CHARSET
%token	CHECK
%token	CHECKPOINT
%token	CHECKSUM
%token	CIPHER
%token	CLASS_ORIGIN
%token	CLIENT
%token	CLOB
%token	CLOSE
%token	COALESCE
%token	CODE
%token	COLLATE
%token	COLLATION
%token	COLUMN
%token	COLUMN_ADD
%token	COLUMN_CHECK
%token	COLUMN_CREATE
%token	COLUMN_DELETE
%token	COLUMN_GET
%token	COLUMN_NAME
%token	COLUMNS
%token	COMMENT
%token	COMMIT
%token	COMMITTED
%token	COMPACT
%token	COMPLETION
%token	COMPRESSED
%token	CONCURRENT
%token	CONDITION
%token	CONNECTION
%token	CONSISTENT
%token	CONSTRAINT
%token	CONSTRAINT_CATALOG
%token	CONSTRAINT_NAME
%token	CONSTRAINT_SCHEMA
%token	CONTAINS
%token	CONTEXT
%token	CONTINUE
%token	CONTRIBUTORS
%token	CONTROL
%token	CONVERT
%token	COUNT
%token	CPU
%token	CREATE
%token	CROSS
%token	CSV
%token	CUBE
%token	CUME_DIST
%token	CURDATE
%token	CURRENT
%token	CURRENT_DATE
%token	CURRENT_POS
%token	CURRENT_ROLE
%token	CURRENT_TIME
%token	CURRENT_TIMESTAMP
%token	CURRENT_USER
%token	CURSOR
%token	CURSOR_NAME
%token	CURTIME
%token	CYCLE
%token	DATA
%token	DATABASE
%token	DATABASES
%token	DATAFILE
%token	DATE
%token	DATE_ADD
%token	DATE_FORMAT
%token	DATE_SUB
%token	DATETIME
%token	DAY
%token	DAY_HOUR
%token	DAY_MICROSECOND
%token	DAY_MINUTE
%token	DAY_SECOND
%token	DEALLOCATE
%token	DEC
%token	DECIMAL
%token	DECLARE
%token	DECODE
%token	DEFAULT
%token	DEFINER
%token	DELAY_KEY_WRITE
%token	DELAYED
%token	DELETE
%token	DELETE_DOMAIN_ID
%token	DELTA
%token	DENSE_RANK
%token	DES_KEY_FILE
%token	DESC
%token	DESCRIBE
%token	DETERMINISTIC
%token	DIAGNOSTICS
%token	DIRECT
%token	DIRECTORY
%token	DISABLE
%token	DISCARD
%token	DISK
%token	DISTINCT
%token	DISTINCTROW
%token	DIV
%token	DO
%token	DO_DOMAIN_IDS
%token	DOUBLE
%token	DROP
%token	DUAL
%token	DUMPFILE
%token	DUPLICATE
%token	DYNAMIC
%token	EACH
%token	ELSE
%token	ELSEIF
%token	ELSIF
%token	ENABLE
%token	ENCLOSED
%token	END
%token	ENDS
%token	ENGINE
%token	ENGINES
%token	ENUM
%token	ERROR
%token	ERRORS
%token	ESCAPE
%token	ESCAPED
%token	EVENT
%token	EVENTS
%token	EVERY
%token	EXAMINED
%token	EXCEPT
%token	EXCEPTION
%token	EXCHANGE
%token	EXCLUDE
%token	EXECUTE
%token	EXISTS
%token	EXIT
%token	EXPANSION
%token	EXPLAIN
%token	EXPORT
%token	EXTENDED
%token	EXTENT_SIZE
%token	EXTRACT
%token	FAST
%token	FAULTS
%token	FETCH
%token	FIELDS
%token	FILE
%token	FIRST
%token	FIRST_VALUE
%token	FIXED
%token	FLOAT
%token	FLOAT4
%token	FLOAT8
%token	FLUSH
%token	FOLLOWING
%token	FOLLOWS
%token	FOR
%token	FORCE
%token	FOREIGN
%token	FORMAT
%token	FOUND
%token	FROM
%token	FULL
%token	FULLTEXT
%token	FUNCTION
%token	GENERAL
%token	GENERATED
%token	GEOMETRY
%token	GEOMETRYCOLLECTION
%token	GET
%token	GET_FORMAT
%token	GLOBAL
%token	GOTO
%token	GRANT
%token	GRANTS
%token	GROUP
%token	GROUP_CONCAT
%token	HANDLER
%token	HARD
%token	HASH
%token	HAVING
%token	HELP
%token	HIGH_PRIORITY
%token	HINT
%token	HISTORY
%token	HOST
%token	HOSTS
%token	HOUR
%token	HOUR_MICROSECOND
%token	HOUR_MINUTE
%token	HOUR_SECOND
%token	ID
%token	IDENTIFIED
%token	IF
%token	IGNORE
%token	IGNORE_DOMAIN_IDS
%token	IGNORE_SERVER_IDS
%token	ILIKE
%token	IMMEDIATE
%token	IMPORT
%token	IN
%token	INCREMENT
%token	INDEX
%token	INDEXES
%token	INFILE
%token	INITIAL_SIZE
%token	INNER
%token	INOUT
%token	INSENSITIVE
%token	INSERT
%token	INSERT_METHOD
%token	INSTALL
%token	INT
%token	INT1
%token	INT2
%token	INT3
%token	INT4
%token	INT8
%token	INTEGER
%token	INTERSECT
%token	INTERVAL
%token	INTO
%token	INVISIBLE
%token	INVOKER
%token	IO
%token	IO_THREAD
%token	IPC
%token	IS
%token	ISNULL
%token	ISOLATION
%token	ISOPEN
%token	ISSUER
%token	ITERATE
%token	JOIN
%token	JSON
%token	KEY
%token	KEY_BLOCK_SIZE
%token	KEYS
%token	KILL
%token	LAG
%token	LANGUAGE
%token	LAST
%token	LAST_VALUE
%token	LASTVAL
%token	LEAD
%token	LEADING
%token	LEAVE
%token	LEAVES
%token	LEFT
%token	LESS
%token	LEVEL
%token	LIKE
%token	LIMIT
%token	LINEAR
%token	LINES
%token	LINESTRING
%token	LIST
%token	LOAD
%token	LOCAL
%token	LOCALTIME
%token	LOCALTIMESTAMP
%token	LOCK
%token	LOCKS
%token	LOGFILE
%token	LOGS
%token	LONG
%token	LONGBLOB
%token	LONGTEXT
%token	LOOP
%token	LOW_PRIORITY
%token	MASTER
%token	MASTER_CONNECT_RETRY
%token	MASTER_DELAY
%token	MASTER_GTID_POS
%token	MASTER_HEARTBEAT_PERIOD
%token	MASTER_HOST
%token	MASTER_LOG_FILE
%token	MASTER_LOG_POS
%token	MASTER_PASSWORD
%token	MASTER_PORT
%token	MASTER_SERVER_ID
%token	MASTER_SSL
%token	MASTER_SSL_CA
%token	MASTER_SSL_CAPATH
%token	MASTER_SSL_CERT
%token	MASTER_SSL_CIPHER
%token	MASTER_SSL_CRL
%token	MASTER_SSL_CRLPATH
%token	MASTER_SSL_KEY
%token	MASTER_SSL_VERIFY_SERVER_CERT
%token	MASTER_USE_GTID
%token	MASTER_USER
%token	MATCH
%token	MAX
%token	MAX_CONNECTIONS_PER_HOUR
%token	MAX_QUERIES_PER_HOUR
%token	MAX_ROWS
%token	MAX_SIZE
%token	MAX_STATEMENT_TIME
%token	MAX_UPDATES_PER_HOUR
%token	MAX_USER_CONNECTIONS
%token	MAXVALUE
%token	MEDIAN
%token	MEDIUM
%token	MEDIUMBLOB
%token	MEDIUMINT
%token	MEDIUMTEXT
%token	MEMORY
%token	MERGE
%token	MESSAGE_TEXT
%token	MICROSECOND
%token	MID
%token	MIDDLEINT
%token	MIGRATE
%token	MIN
%token	MIN_ROWS
%token	MINUS
%token	MINUTE
%token	MINUTE_MICROSECOND
%token	MINUTE_SECOND
%token	MINVALUE
%token	MOD
%token	MODE
%token	MODIFIES
%token	MODIFY
%token	MONTH
%token	MULTILINESTRING
%token	MULTIPOINT
%token	MULTIPOLYGON
%token	MUTEX
%token	MYSQL
%token	MYSQL_ERRNO
%token	NAME
%token	NAMES
%token	NAME_CONST
%token	NATIONAL
%token	NATURAL
%token	NCHAR
%token	NEW
%token	NEXT
%token	NEXTVAL
%token	NO
%token	NO_WAIT
%token	NO_WRITE_TO_BINLOG
%token	NOCACHE
%token	NOCYCLE
%token	NODEGROUP
%token	NOMAXVALUE
%token	NOMINVALUE
%token	NONE
%token	NOT
%token	NOTFOUND
%token	NOW
%token	NOWAIT
%token	NTH_VALUE
%token	NTILE
%token	NULL
%token	NUMBER
%token	NUMERIC
%token	NVARCHAR
%token	OF
%token	OFF
%token	OFFSET
%token	OLD_PASSWORD
%token	ON
%token	ONE
%token	ONLINE
%token	ONLY
%token	OPEN
%token	OPTIMIZE
%token	OPTION
%token	OPTIONALLY
%token	OPTIONS
%token	OR
%token	ORDER
%token	OTHERS
%token	OUT
%token	OUTER
%token	OUTFILE
%token	OVER
%token	OWNER
%token	PACK_KEYS
%token	PACKAGE
%token	PAGE
%token	PAGE_CHECKSUM
%token	PARAMETERS
%token	PARSE_VCOL_EXPR
%token	PARSER
%token	PARTIAL
%token	PARTITION
%token	PARTITIONING
%token	PARTITIONS
%token	PASSWORD
%token	PERCENT_RANK
%token	PERCENTILE_CONT
%token	PERCENTILE_DISC
%token	PERIOD
%token	PERSISTENT
%token	PHASE
%token	PLAN
%token	PLUGIN
%token	PLUGINS
%token	POINT
%token	POLYGON
%token	PORT
%token	POSITION
%token	PRECEDES
%token	PRECEDING
%token	PRECISION
%token	PREPARE
%token	PRESERVE
%token	PREV
%token	PREVIOUS
%token	PRIMARY
%token	PRIVILEGES
%token	PROCEDURE
%token	PROCESS
%token	PROCESSLIST
%token	PROFILE
%token	PROFILES
%token	PROXY
%token	PURGE
%token	QUARTER
%token	QUERY
%token	QUICK
%token	RAISE
%token	RANGE
%token	RANK
%token	RAW
%token	READ
%token	READ_ONLY
%token	READ_WRITE
%token	READS
%token	REAL
%token	REBUILD
%token	RECOVER
%token	RECURSIVE
%token	REDO_BUFFER_SIZE
%token	REDOFILE
%token	REDUNDANT
%token	REF_SYSTEM_ID
%token	REFERENCES
%token	REGEXP
%token	RELAY
%token	RELAY_LOG_FILE
%token	RELAY_LOG_POS
%token	RELAY_THREAD
%token	RELAYLOG
%token	RELEASE
%token	RELOAD
%token	REMOVE
%token	RENAME
%token	REORGANIZE
%token	REPAIR
%token	REPEAT
%token	REPEATABLE
%token	REPLACE
%token	REPLICATION
%token	REQUIRE
%token	RESET
%token	RESIGNAL
%token	RESTART
%token	RESTORE
%token	RESTRICT
%token	RESUME
%token	RETURN
%token	RETURNED_SQLSTATE
%token	RETURNING
%token	RETURNS
%token	REUSE
%token	REVERSE
%token	REVOKE
%token	RIGHT
%token	RLIKE
%token	ROLE
%token	ROLLBACK
%token	ROLLUP
%token	ROUTINE
%token	ROW
%token	ROW_COUNT
%token	ROW_FORMAT
%token	ROW_NUMBER
%token	ROWCOUNT
%token	ROWS
%token	ROWTYPE
%token	RTREE
%token	SAVEPOINT
%token	SCHEDULE
%token	SCHEMA
%token	SCHEMA_NAME
%token	SCHEMAS
%token	SECOND
%token	SECOND_MICROSECOND
%token	SECURITY
%token	SELECT
%token	SENSITIVE
%token	SEPARATOR
%token	SEQUENCE
%token	SERIAL
%token	SERIALIZABLE
%token	SERVER
%token	SESSION
%token	SESSION_USER
%token	SET
%token	SETVAL
%token	SHARE
%token	SHOW
%token	SHUTDOWN
%token	SIGNAL
%token	SIGNED
%token	SIMPLE
%token	SLAVE
%token	SLAVE_POS
%token	SLAVES
%token	SLOW
%token	SMALLINT
%token	SNAPSHOT
%token	SOCKET
%token	SOFT
%token	SOME
%token	SONAME
%token	SORTED
%token	SOUNDS
%token	SOURCE
%token	SPATIAL
%token	SPECIFIC
%token	SQL
%token	SQL_BIG_RESULT
%token	SQL_BUFFER_RESULT
%token	SQL_CACHE
%token	SQL_CALC_FOUND_ROWS
%token	SQL_NO_CACHE
%token	SQL_SMALL_RESULT
%token	SQL_THREAD
%token	SQL_TSI_DAY
%token	SQL_TSI_HOUR
%token	SQL_TSI_MINUTE
%token	SQL_TSI_MONTH
%token	SQL_TSI_QUARTER
%token	SQL_TSI_SECOND
%token	SQL_TSI_WEEK
%token	SQL_TSI_YEAR
%token	SQLEXCEPTION
%token	SQLSTATE
%token	SQLWARNING
%token	SSL
%token	START
%token	STARTING
%token	STARTS
%token	STATEMENT
%token	STATS_AUTO_RECALC
%token	STATS_PERSISTENT
%token	STATS_SAMPLE_PAGES
%token	STATUS
%token	STD
%token	STDDEV
%token	STDDEV_POP
%token	STDDEV_SAMP
%token	STOP
%token	STORAGE
%token	STORED
%token	STRAIGHT_JOIN
%token	SUBCLASS_ORIGIN
%token	SUBDATE
%token	SUBJECT
%token	SUBPARTITION
%token	SUBPARTITIONS
%token	SUBSTR
%token	SUBSTRING
%token	SUM
%token	SUPER
%token	SUSPEND
%token	SWAPS
%token	SWITCHES
%token	SYSDATE
%token	SYSTEM
%token	SYSTEM_TIME
%token	SYSTEM_USER
%token	TABLE
%token	TABLE_CHECKSUM
%token	TABLE_NAME
%token	TABLES
%token	TABLESPACE
%token	TBL
%token	TEMPORARY
%token	TEMPTABLE
%token	TERMINATED
%token	TEXT
%token	THAN
%token	THEN
%token	TIES
%token	TIME
%token	TIMESTAMP
%token	TIMESTAMPADD
%token	TIMESTAMPDIFF
%token	TINYBLOB
%token	TINYINT
%token	TINYTEXT
%token	TO
%token	TOP
%token	TRAILING
%token	TRANSACTION
%token	TRANSACTIONAL
%token	TRIGGER
%token	TRIGGERS
%token	TRIM
%token	TRIM_ORACLE
%token	TRUNCATE
%token	TYPE
%token	TYPES
%token	UNBOUNDED
%token	UNCOMMITTED
%token	UNDEFINED
%token	UNDO
%token	UNDO_BUFFER_SIZE
%token	UNDOFILE
%token	UNICODE
%token	UNINSTALL
%token	UNION
%token	UNIQUE
%token	UNKNOWN
%token	UNLOAD
%token	UNLOCK
%token	UNSIGNED
%token	UNTIL
%token	UPDATE
%token	UPGRADE
%token	USAGE
%token	USE
%token	USE_FRM
%token	USER
%token	USER_RESOURCES
%token	USING
%token	UTC_DATE
%token	UTC_TIME
%token	UTC_TIMESTAMP
%token	VALUE
%token	VALUES
%token	VAR_POP
%token	VAR_SAMP
%token	VARBINARY
%token	VARCHAR
%token	VARCHAR2
%token	VARCHARACTER
%token	VARIABLES
%token	VARIANCE
%token	VARYING
%token	VERSIONING
%token	VIA
%token	VIEW
%token	VIRTUAL
%token	WAIT
%token	WARNINGS
%token	WEEK
%token	WEIGHT_STRING
%token	WHEN
%token	WHERE
%token	WHILE
%token	WINDOW
%token	WITH
%token	WITHIN
%token	WITHOUT
%token	WORK
%token	WRAPPER
%token	WRITE
%token	X509
%token	XA
%token	XML
%token	XOR
%token	YEAR
%token	YEAR_MONTH
%token	ZEROFILL
%token	FALSE
%token	TRUE

%token	AND_AND
%token	SHIFT_LEFT
%token	SHIFT_RIGHT
%token	EQUAL
%token	VARASSIGN

%token _utf8mb4


/*********************************
 ** Non-Terminal types (http://www.gnu.org/software/bison/manual/html_node/Type-Decl.html)
 *********************************/
%type <stmt_vec>	    statement_list
%type <statement> 	    statement preparable_statement
%type <exec_stmt>	    execute_statement
%type <prep_stmt>	    prepare_statement
%type <select_stmt>     select_statement select_with_paren select_no_paren select_clause select_paren_or_clause
%type <import_stmt>     import_statement
%type <create_stmt>     create_statement
%type <insert_stmt>     insert_statement
%type <delete_stmt>     delete_statement truncate_statement
%type <update_stmt>     update_statement
%type <drop_stmt>	    drop_statement
%type <show_stmt>	    show_statement
%type <table_name>      table_name
%type <sval> 		    file_path prepare_target_query
%type <bval> 		    opt_not_exists opt_exists opt_distinct opt_column_nullable
%type <uval>		    import_file_type opt_join_type
%type <table> 		    opt_from_clause from_clause table_ref table_ref_atomic table_ref_name nonjoin_table_ref_atomic
%type <table>		    join_clause table_ref_name_no_alias
%type <expr> 		    expr operand scalar_expr unary_expr binary_expr logic_expr exists_expr extract_expr
%type <expr>		    function_expr between_expr expr_alias param_expr
%type <expr> 		    column_name literal int_literal num_literal string_literal bool_literal mysql_name_const_literal
%type <expr> 		    comp_expr opt_where join_condition opt_having case_expr case_list in_expr hint
%type <expr> 		    array_expr array_index null_literal
%type <limit>		    opt_limit opt_top
%type <order>		    order_desc
%type <order_type>	    opt_order_type
%type <datetime_field>	datetime_field
%type <column_t>	    column_def
%type <column_type_t>   column_type
%type <update_t>	    update_clause
%type <group_t>		    opt_group
%type <alias_t>		    opt_table_alias table_alias opt_alias alias
%type <with_description_t>  with_description

%type <str_vec>			ident_commalist opt_column_list
%type <expr_vec> 		expr_list select_list opt_literal_list literal_list hint_list opt_hints
%type <table_vec> 		table_ref_commalist
%type <order_vec>		opt_order order_list
%type <with_description_vec> 	opt_with_clause with_clause with_description_list
%type <update_vec>		update_clause_commalist
%type <column_vec>		column_def_commalist

%type <sval>            x_compat_ident

/******************************
 ** Token Precedence and Associativity
 ** Precedence: lowest to highest
 ******************************/
%left		OR
%left		AND
%right		NOT
%nonassoc	'=' EQUALS NOTEQUALS LIKE ILIKE
%nonassoc	'<' '>' LESS GREATER LESSEQ GREATEREQ

%nonassoc	NOTNULL
%nonassoc	ISNULL
%nonassoc	IS				/* sets precedence for IS NULL, etc */
%left		'+' '-'
%left		'*' '/' '%'
%left		'^'
%left		CONCAT

/* Unary Operators */
%right  UMINUS
%left		'[' ']'
%left		'(' ')'
%left		'.'
%left   JOIN
%%
/*********************************
 ** Section 3: Grammar Definition
 *********************************/

// Defines our general input.
input:
		statement_list opt_semicolon {
			for (SQLStatement* stmt : *$1) {
				// Transfers ownership of the statement.
				result->addStatement(stmt);
			}

			unsigned param_id = 0;
			for (void* param : yyloc.param_list) {
				if (param != nullptr) {
					Expr* expr = (Expr*) param;
					expr->ival = param_id;
					result->addParameter(expr);
					++param_id;
				}
			}
			delete $1;
		}
	;


statement_list:
		statement {
			$1->stringLength = yylloc.string_length;
			yylloc.string_length = 0;
			$$ = new std::vector<SQLStatement*>();
			$$->push_back($1);
		}
	|	statement_list ';' statement {
			$3->stringLength = yylloc.string_length;
			yylloc.string_length = 0;
			$1->push_back($3);
			$$ = $1;
		}
	;

statement:
		prepare_statement opt_hints {
			$$ = $1;
			$$->hints = $2;
		}
	|	preparable_statement opt_hints {
			$$ = $1;
			$$->hints = $2;
		}
	|	show_statement {
			$$ = $1;
		}
	;


preparable_statement:
		select_statement { $$ = $1; }
	|	import_statement { $$ = $1; }
	|	create_statement { $$ = $1; }
	|	insert_statement { $$ = $1; }
	|	delete_statement { $$ = $1; }
	|	truncate_statement { $$ = $1; }
	|	update_statement { $$ = $1; }
	|	drop_statement { $$ = $1; }
	|	execute_statement { $$ = $1; }
	;


/******************************
 * Hints
 ******************************/

opt_hints:
    WITH HINT '(' hint_list ')' { $$ = $4; }
  | /* empty */ { $$ = nullptr; }
  ;


hint_list:
	  hint { $$ = new std::vector<Expr*>(); $$->push_back($1); }
	| hint_list ',' hint { $1->push_back($3); $$ = $1; }
	;

hint:
		IDENTIFIER {
			$$ = Expr::make(kExprHint);
			$$->name = $1;
		}
	| IDENTIFIER '(' literal_list ')' {
			$$ = Expr::make(kExprHint);
			$$->name = $1;
			$$->exprList = $3;
		}
	;


/******************************
 * Prepared Statement
 ******************************/
prepare_statement:
		PREPARE IDENTIFIER FROM prepare_target_query {
			$$ = new PrepareStatement();
			$$->name = $2;
			$$->query = $4;
		}
	;

prepare_target_query: STRING

execute_statement:
		EXECUTE IDENTIFIER {
			$$ = new ExecuteStatement();
			$$->name = $2;
		}
	|	EXECUTE IDENTIFIER '(' opt_literal_list ')' {
			$$ = new ExecuteStatement();
			$$->name = $2;
			$$->parameters = $4;
		}
	;


/******************************
 * Import Statement
 ******************************/
import_statement:
		IMPORT FROM import_file_type FILE file_path INTO table_name {
			$$ = new ImportStatement((ImportType) $3);
			$$->filePath = $5;
			$$->pos = $7.pos;
			$$->schema = $7.schema;
			$$->tableName = $7.name;
		}
	;

import_file_type:
		CSV { $$ = kImportCSV; }
	;

file_path:
		string_literal { $$ = strdup($1->name); delete $1; }
	;


/******************************
 * Show Statement
 * SHOW TABLES;
 ******************************/

show_statement:
		SHOW TABLES {
			$$ = new ShowStatement(kShowTables);
		}
	|	SHOW COLUMNS table_name {
			$$ = new ShowStatement(kShowColumns);
			$$->pos = $3.pos;
			$$->schema = $3.schema;
			$$->name = $3.name;
		}
	;


/******************************
 * Create Statement
 * CREATE TABLE students (name TEXT, student_number INTEGER, city TEXT, grade DOUBLE)
 * CREATE TABLE students FROM TBL FILE 'test/students.tbl'
 ******************************/
create_statement:
		CREATE TABLE opt_not_exists table_name FROM TBL FILE file_path {
			$$ = new CreateStatement(kCreateTableFromTbl);
			$$->ifNotExists = $3;
			$$->pos = $4.pos;
			$$->schema = $4.schema;
			$$->tableName = $4.name;
			$$->filePath = $8;
		}
	|	CREATE TABLE opt_not_exists table_name '(' column_def_commalist ')' {
			$$ = new CreateStatement(kCreateTable);
			$$->ifNotExists = $3;
			$$->pos = $4.pos;
			$$->schema = $4.schema;
			$$->tableName = $4.name;
			$$->columns = $6;
		}
	|	CREATE TABLE opt_not_exists table_name AS select_statement {
			$$ = new CreateStatement(kCreateTable);
			$$->ifNotExists = $3;
			$$->pos = $4.pos;
			$$->schema = $4.schema;
			$$->tableName = $4.name;
			$$->select = $6;
		}
	|	CREATE VIEW opt_not_exists table_name opt_column_list AS select_statement {
			$$ = new CreateStatement(kCreateView);
			$$->ifNotExists = $3;
			$$->pos = $4.pos;
			$$->schema = $4.schema;
			$$->tableName = $4.name;
			$$->viewColumns = $5;
			$$->select = $7;
		}
	;

opt_not_exists:
		IF NOT EXISTS { $$ = true; }
	|	/* empty */ { $$ = false; }
	;

column_def_commalist:
		column_def { $$ = new std::vector<ColumnDefinition*>(); $$->push_back($1); }
	|	column_def_commalist ',' column_def { $1->push_back($3); $$ = $1; }
	;

column_def:
		x_compat_ident column_type opt_column_nullable {
			$$ = new ColumnDefinition($1, $2, $3);
		}
	;

column_type:
		INT { $$ = ColumnType{DataType::INT}; }
	|	INTEGER { $$ = ColumnType{DataType::INT}; }
	|	LONG { $$ = ColumnType{DataType::LONG}; }
	|	FLOAT { $$ = ColumnType{DataType::FLOAT}; }
	|	DOUBLE { $$ = ColumnType{DataType::DOUBLE}; }
	|	VARCHAR '(' INTVAL ')' { $$ = ColumnType{DataType::VARCHAR, $3}; }
	|	CHAR '(' INTVAL ')' { $$ = ColumnType{DataType::CHAR, $3}; }
	|	TEXT { $$ = ColumnType{DataType::TEXT}; }
	;

opt_column_nullable:
		NULL { $$ = true; }
	|	NOT NULL { $$ = false; }
	|	/* empty */ { $$ = false; }
	;

/******************************
 * Drop Statement
 * DROP TABLE students;
 * DEALLOCATE PREPARE stmt;
 ******************************/

drop_statement:
		DROP TABLE opt_exists table_name {
			$$ = new DropStatement(kDropTable);
			$$->ifExists = $3;
			$$->pos = $4.pos;
			$$->schema = $4.schema;
			$$->name = $4.name;
		}
	|	DROP VIEW opt_exists table_name {
			$$ = new DropStatement(kDropView);
			$$->ifExists = $3;
			$$->pos = $4.pos;
			$$->schema = $4.schema;
			$$->name = $4.name;
		}
	|	DEALLOCATE PREPARE IDENTIFIER {
			$$ = new DropStatement(kDropPreparedStatement);
			$$->ifExists = false;
			$$->name = $3;
		}
	;

opt_exists:
		IF EXISTS   { $$ = true; }
	|	/* empty */ { $$ = false; }
	;

/******************************
 * Delete Statement / Truncate statement
 * DELETE FROM students WHERE grade > 3.0
 * DELETE FROM students <=> TRUNCATE students
 ******************************/
delete_statement:
		DELETE FROM table_name opt_where {
			$$ = new DeleteStatement();
			$$->pos = $3.pos;
			$$->schema = $3.schema;
			$$->tableName = $3.name;
			$$->expr = $4;
		}
	;

truncate_statement:
		TRUNCATE table_name {
			$$ = new DeleteStatement();
			$$->pos = $2.pos;
			$$->schema = $2.schema;
			$$->tableName = $2.name;
		}
	;

/******************************
 * Insert Statement
 * INSERT INTO students VALUES ('Max', 1112233, 'Musterhausen', 2.3)
 * INSERT INTO employees SELECT * FROM stundents
 ******************************/
insert_statement:
		INSERT INTO table_name opt_column_list VALUES '(' expr_list ')' {
			$$ = new InsertStatement(kInsertValues);
			$$->pos = $3.pos;
			$$->schema = $3.schema;
			$$->tableName = $3.name;
			$$->columns = $4;
			$$->values = $7;
		}
	|	INSERT table_name opt_column_list VALUES '(' expr_list ')' {
			$$ = new InsertStatement(kInsertValues);
			$$->pos = $2.pos;
			$$->schema = $2.schema;
			$$->tableName = $2.name;
			$$->columns = $3;
			$$->values = $6;
		}
	|	INSERT INTO table_name opt_column_list select_no_paren {
			$$ = new InsertStatement(kInsertSelect);
			$$->pos = $3.pos;
			$$->schema = $3.schema;
			$$->tableName = $3.name;
			$$->columns = $4;
			$$->select = $5;
		}
	|	INSERT table_name opt_column_list select_no_paren {
			$$ = new InsertStatement(kInsertSelect);
			$$->pos = $2.pos;
			$$->schema = $2.schema;
			$$->tableName = $2.name;
			$$->columns = $3;
			$$->select = $4;
		}
	;


opt_column_list:
		'(' ident_commalist ')' { $$ = $2; }
	|	/* empty */ { $$ = nullptr; }
	;


/******************************
 * Update Statement
 * UPDATE students SET grade = 1.3, name='Felix Fürstenberg' WHERE name = 'Max Mustermann';
 ******************************/

update_statement:
	UPDATE table_ref_name_no_alias SET update_clause_commalist opt_where {
		$$ = new UpdateStatement();
		$$->table = $2;
		$$->updates = $4;
		$$->where = $5;
	}
	;

update_clause_commalist:
		update_clause { $$ = new std::vector<UpdateClause*>(); $$->push_back($1); }
	|	update_clause_commalist ',' update_clause { $1->push_back($3); $$ = $1; }
	;

update_clause:
		x_compat_ident '=' expr {
			$$ = new UpdateClause();
			$$->column = $1;
			$$->value = $3;
		}
	|	x_compat_ident VARASSIGN expr {
			$$ = new UpdateClause();
			$$->column = $1;
			$$->value  = $3;
		}
	;

/******************************
 * Select Statement
 ******************************/

select_statement:
		opt_with_clause select_with_paren {
			$$ = $2;
			$$->withDescriptions = $1;
		}
	|	opt_with_clause select_no_paren {
			$$ = $2;
			$$->withDescriptions = $1;
		}
	|	opt_with_clause select_with_paren set_operator select_paren_or_clause opt_order opt_limit {
			// TODO: allow multiple unions (through linked list)
			// TODO: capture type of set_operator
			// TODO: might overwrite order and limit of first select here
			$$ = $2;
			$$->withDescriptions = $1;
			$$->unionSelect = $4;
			$$->order = $5;

			// Limit could have been set by TOP.
			if ($6 != nullptr) {
				delete $$->limit;
				$$->limit = $6;
			}
		}
	;

select_with_paren:
		'(' select_no_paren ')' { $$ = $2; }
	|	'(' select_with_paren ')' { $$ = $2; }
	;

select_paren_or_clause:
		select_with_paren
	|	select_clause
	;

select_no_paren:
		select_clause opt_order opt_limit {
			$$ = $1;
			$$->order = $2;

			// Limit could have been set by TOP.
			if ($3 != nullptr) {
				delete $$->limit;
				$$->limit = $3;
			}
		}
	|	select_clause set_operator select_paren_or_clause opt_order opt_limit {
			// TODO: allow multiple unions (through linked list)
			// TODO: capture type of set_operator
			// TODO: might overwrite order and limit of first select here
			$$ = $1;
			$$->unionSelect = $3;
			$$->order = $4;

			// Limit could have been set by TOP.
			if ($5 != nullptr) {
				delete $$->limit;
				$$->limit = $5;
			}
		}
	;

set_operator:
		set_type opt_all
	;

set_type:
		UNION
	|	INTERSECT
	|	EXCEPT
	;

opt_all:
		ALL
	|	/* empty */
	;

select_clause:
		SELECT opt_top opt_distinct select_list opt_from_clause opt_where opt_group {
			$$ = new SelectStatement();
			$$->limit = $2;
			$$->selectDistinct = $3;
			$$->selectList = $4;
			$$->fromTable = $5;
			$$->whereClause = $6;
			$$->groupBy = $7;
		}
	;

opt_distinct:
		DISTINCT { $$ = true; }
	|	/* empty */ { $$ = false; }
	;

select_list:
		expr_list
	;

opt_from_clause:
        from_clause  { $$ = $1; }
    |   /* empty */  { $$ = nullptr; }

from_clause:
		FROM table_ref { $$ = $2; }
	;


opt_where:
		WHERE expr { $$ = $2; }
	|	/* empty */ { $$ = nullptr; }
	;

opt_group:
		GROUP BY expr_list opt_having {
			$$ = new GroupByDescription();
			$$->columns = $3;
			$$->having = $4;
		}
	|	/* empty */ { $$ = nullptr; }
	;

opt_having:
		HAVING expr { $$ = $2; }
	|	/* empty */ { $$ = nullptr; }

opt_order:
		ORDER BY order_list { $$ = $3; }
	|	/* empty */ { $$ = nullptr; }
	;

order_list:
		order_desc { $$ = new std::vector<OrderDescription*>(); $$->push_back($1); }
	|	order_list ',' order_desc { $1->push_back($3); $$ = $1; }
	;

order_desc:
		expr opt_order_type { $$ = new OrderDescription($2, $1); }
	;

opt_order_type:
		ASC { $$ = kOrderAsc; }
	|	DESC { $$ = kOrderDesc; }
	|	/* empty */ { $$ = kOrderAsc; }
	;

// TODO: TOP and LIMIT can take more than just int literals.

opt_top:
		TOP int_literal { $$ = new LimitDescription($2, nullptr); }
	|	/* empty */ { $$ = nullptr; }
	;

opt_limit:
		LIMIT expr { $$ = new LimitDescription($2, nullptr); }
	|	OFFSET expr { $$ = new LimitDescription(nullptr, $2); }
	|	LIMIT expr OFFSET expr { $$ = new LimitDescription($2, $4); }
	|	LIMIT ALL { $$ = new LimitDescription(nullptr, nullptr); }
	|	LIMIT ALL OFFSET expr { $$ = new LimitDescription(nullptr, $4); }
	|	/* empty */ { $$ = nullptr; }
	;

/******************************
 * Expressions
 ******************************/
expr_list:
		expr_alias { $$ = new std::vector<Expr*>(); $$->push_back($1); }
	|	expr_list ',' expr_alias { $1->push_back($3); $$ = $1; }
	;

opt_literal_list:
		literal_list { $$ = $1; }
	|	/* empty */ { $$ = nullptr; }
	;

literal_list:
		literal { $$ = new std::vector<Expr*>(); $$->push_back($1); }
	|	literal_list ',' literal { $1->push_back($3); $$ = $1; }
	;

expr_alias:
		expr opt_alias {
			$$ = $1;
			if ($2) {
				$$->alias = strdup($2->name);
				delete $2;
			}
		}
	;

expr:
		operand
	|	between_expr
	|	logic_expr
	|	exists_expr
	|	in_expr
	;

operand:
		'(' expr ')' { $$ = $2; }
	|	array_index
	|	scalar_expr
	|	unary_expr
	|	binary_expr
	|	case_expr
	|	function_expr
	|	extract_expr
	|	array_expr
	|	'(' select_no_paren ')' { $$ = Expr::makeSelect($2); }
	;

scalar_expr:
		column_name
	|	literal
	|       '@' x_compat_ident { std::string var($2); var = "__ULTRAVERSE_SQLVAR__" + var; $$ = Expr::makeLiteral(strdup(var.c_str())); }
	;

unary_expr:
		'-' operand { $$ = Expr::makeOpUnary(kOpUnaryMinus, $2); }
	|	NOT operand { $$ = Expr::makeOpUnary(kOpNot, $2); }
	|	operand ISNULL { $$ = Expr::makeOpUnary(kOpIsNull, $1); }
	|	operand IS NULL { $$ = Expr::makeOpUnary(kOpIsNull, $1); }
	|	operand IS NOT NULL { $$ = Expr::makeOpUnary(kOpNot, Expr::makeOpUnary(kOpIsNull, $1)); }
	;

binary_expr:
		comp_expr
	|	operand '-' operand			{ $$ = Expr::makeOpBinary($1, kOpMinus, $3); }
	|	operand '+' operand			{ $$ = Expr::makeOpBinary($1, kOpPlus, $3); }
	|	operand '/' operand			{ $$ = Expr::makeOpBinary($1, kOpSlash, $3); }
	|	operand '*' operand			{ $$ = Expr::makeOpBinary($1, kOpAsterisk, $3); }
	|	operand '%' operand			{ $$ = Expr::makeOpBinary($1, kOpPercentage, $3); }
	|	operand '^' operand			{ $$ = Expr::makeOpBinary($1, kOpCaret, $3); }
	|	operand LIKE operand		{ $$ = Expr::makeOpBinary($1, kOpLike, $3); }
	|	operand NOT LIKE operand	{ $$ = Expr::makeOpBinary($1, kOpNotLike, $4); }
	|	operand ILIKE operand		{ $$ = Expr::makeOpBinary($1, kOpILike, $3); }
	|	operand CONCAT operand	{ $$ = Expr::makeOpBinary($1, kOpConcat, $3); }
	;

logic_expr:
		expr AND expr	{ $$ = Expr::makeOpBinary($1, kOpAnd, $3); }
	|	expr OR expr	{ $$ = Expr::makeOpBinary($1, kOpOr, $3); }
	;

in_expr:
		operand IN '(' expr_list ')'			{ $$ = Expr::makeInOperator($1, $4); }
	|	operand NOT IN '(' expr_list ')'		{ $$ = Expr::makeOpUnary(kOpNot, Expr::makeInOperator($1, $5)); }
	|	operand IN '(' select_no_paren ')'		{ $$ = Expr::makeInOperator($1, $4); }
	|	operand NOT IN '(' select_no_paren ')'	{ $$ = Expr::makeOpUnary(kOpNot, Expr::makeInOperator($1, $5)); }
	;

// CASE grammar based on: flex & bison by John Levine
// https://www.safaribooksonline.com/library/view/flex-bison/9780596805418/ch04.html#id352665
case_expr:
		CASE expr case_list END         	{ $$ = Expr::makeCase($2, $3, nullptr); }
	|	CASE expr case_list ELSE expr END	{ $$ = Expr::makeCase($2, $3, $5); }
	|	CASE case_list END			        { $$ = Expr::makeCase(nullptr, $2, nullptr); }
	|	CASE case_list ELSE expr END		{ $$ = Expr::makeCase(nullptr, $2, $4); }
	;

case_list:
		WHEN expr THEN expr              { $$ = Expr::makeCaseList(Expr::makeCaseListElement($2, $4)); }
	|	case_list WHEN expr THEN expr    { $$ = Expr::caseListAppend($1, Expr::makeCaseListElement($3, $5)); }
	;

exists_expr:
		EXISTS '(' select_no_paren ')' { $$ = Expr::makeExists($3); }
	|	NOT EXISTS '(' select_no_paren ')' { $$ = Expr::makeOpUnary(kOpNot, Expr::makeExists($4)); }
	;

comp_expr:
		operand '=' operand			{ $$ = Expr::makeOpBinary($1, kOpEquals, $3); }
	|	operand EQUALS operand			{ $$ = Expr::makeOpBinary($1, kOpEquals, $3); }
	|	operand NOTEQUALS operand	{ $$ = Expr::makeOpBinary($1, kOpNotEquals, $3); }
	|	operand '<' operand			{ $$ = Expr::makeOpBinary($1, kOpLess, $3); }
	|	operand '>' operand			{ $$ = Expr::makeOpBinary($1, kOpGreater, $3); }
	|	operand LESSEQ operand		{ $$ = Expr::makeOpBinary($1, kOpLessEq, $3); }
	|	operand GREATEREQ operand	{ $$ = Expr::makeOpBinary($1, kOpGreaterEq, $3); }
	;

function_expr:
               x_compat_ident '(' ')' { $$ = Expr::makeFunctionRef($1, new std::vector<Expr*>(), false); }
       |       x_compat_ident '(' opt_distinct expr_list ')' { $$ = Expr::makeFunctionRef($1, $4, $3); }
       ;


extract_expr:
         EXTRACT '(' datetime_field FROM expr ')'    { $$ = Expr::makeExtract($3, $5); }
    ;

datetime_field:
        SECOND { $$ = kDatetimeSecond; }
    |   MINUTE { $$ = kDatetimeMinute; }
    |   HOUR { $$ = kDatetimeHour; }
    |   DAY { $$ = kDatetimeDay; }
    |   MONTH { $$ = kDatetimeMonth; }
    |   YEAR { $$ = kDatetimeYear; }

array_expr:
	  	ARRAY '[' expr_list ']' { $$ = Expr::makeArray($3); }
	;

array_index:
	   	operand '[' int_literal ']' { $$ = Expr::makeArrayIndex($1, $3->ival); }
	;

between_expr:
		operand BETWEEN operand AND operand { $$ = Expr::makeBetween($1, $3, $5); }
	;

column_name:
		x_compat_ident { $$ = Expr::makeColumnRef($1); }
	|	IDENTIFIER '.' x_compat_ident { $$ = Expr::makeColumnRef($1, $3); }
	|	'*' { $$ = Expr::makeStar(); }
	|	IDENTIFIER '.' '*' { $$ = Expr::makeStar($1); }
	;

literal:
		mysql_name_const_literal
	|	string_literal
	|	bool_literal
	|	num_literal
	|	null_literal
	|	param_expr
	;

mysql_name_const_literal:
		NAME_CONST '(' string_literal ',' literal ')' { $$ = $5; }
	;

string_literal:
		STRING { $$ = Expr::makeLiteral($1); }
	|       _utf8mb4 STRING { $$ = Expr::makeLiteral($2); }
	|       _utf8mb4 STRING COLLATE STRING { $$ = Expr::makeLiteral($2); }
	|   	x_compat_ident { $$ = Expr::makeLiteral($1); }
	;

bool_literal:
		TRUE { $$ = Expr::makeLiteral(true); }
	|	FALSE { $$ = Expr::makeLiteral(false); }
	;

num_literal:
		FLOATVAL { $$ = Expr::makeLiteral($1); }
	|	int_literal
	;

int_literal:
		INTVAL { $$ = Expr::makeLiteral($1); }
	;

null_literal:
	    	NULL { $$ = Expr::makeNullLiteral(); }
	;

param_expr:
		'?' {
			$$ = Expr::makeParameter(yylloc.total_column);
			$$->ival2 = yyloc.param_list.size();
			yyloc.param_list.push_back($$);
		}
	;


/******************************
 * Table
 ******************************/
table_ref:
		table_ref_atomic
	|	table_ref_commalist ',' table_ref_atomic {
			$1->push_back($3);
			auto tbl = new TableRef(kTableCrossProduct);
			tbl->list = $1;
			$$ = tbl;
		}
	;


table_ref_atomic:
		nonjoin_table_ref_atomic
	|	join_clause
	;

nonjoin_table_ref_atomic:
		table_ref_name
	|	'(' select_statement ')' opt_table_alias {
			auto tbl = new TableRef(kTableSelect);
			tbl->select = $2;
			tbl->alias = $4;
			$$ = tbl;
		}
	;

table_ref_commalist:
		table_ref_atomic { $$ = new std::vector<TableRef*>(); $$->push_back($1); }
	|	table_ref_commalist ',' table_ref_atomic { $1->push_back($3); $$ = $1; }
	;


table_ref_name:
		table_name opt_table_alias {
			auto tbl = new TableRef(kTableName);
			tbl->pos = $1.pos;
			tbl->schema = $1.schema;
			tbl->name = $1.name;
			tbl->alias = $2;
			$$ = tbl;
		}
	;


table_ref_name_no_alias:
		table_name {
			$$ = new TableRef(kTableName);
			$$->pos = $1.pos;
			$$->schema = $1.schema;
			$$->name = $1.name;
		}
	;


table_name:
		x_compat_ident {
			$$.pos = yylloc.total_column;
			$$.schema = nullptr;
			$$.name = $1;
		}
	|	x_compat_ident '.' x_compat_ident {
			$$.pos = yylloc.total_column;
			$$.schema = $1;
			$$.name = $3;
		}
	;


table_alias:
		alias
	|	AS x_compat_ident '(' ident_commalist ')' { $$ = new Alias($2, $4); }
	;


opt_table_alias:
		table_alias
	|	/* empty */ { $$ = nullptr; }


alias:
		AS x_compat_ident { $$ = new Alias($2); }
	|	x_compat_ident { $$ = new Alias($1); }
	;


opt_alias:
		alias
	|	/* empty */ { $$ = nullptr; }


/******************************
 * With Descriptions
 ******************************/

opt_with_clause:
		with_clause
	| 	/* empty */ { $$ = nullptr; }
	;

with_clause:
		WITH with_description_list { $$ = $2; }
	;

with_description_list:
		with_description {
			$$ = new std::vector<WithDescription*>();
			$$->push_back($1);
		}
	|	with_description_list ',' with_description {
			$1->push_back($3);
                        $$ = $1;
		}
	;

with_description:
		IDENTIFIER AS select_with_paren {
			$$ = new WithDescription();
			$$->alias = $1;
			$$->select = $3;
		}
	;


/******************************
 * Join Statements
 ******************************/

join_clause:
		table_ref_atomic NATURAL JOIN nonjoin_table_ref_atomic
		{
			$$ = new TableRef(kTableJoin);
			$$->join = new JoinDefinition();
			$$->join->type = kJoinNatural;
			$$->join->left = $1;
			$$->join->right = $4;
		}
	|	table_ref_atomic opt_join_type JOIN table_ref_atomic ON join_condition
		{
			$$ = new TableRef(kTableJoin);
			$$->join = new JoinDefinition();
			$$->join->type = (JoinType) $2;
			$$->join->left = $1;
			$$->join->right = $4;
			$$->join->condition = $6;
		}
	|
		table_ref_atomic opt_join_type JOIN table_ref_atomic USING '(' column_name ')'
		{
			$$ = new TableRef(kTableJoin);
			$$->join = new JoinDefinition();
			$$->join->type = (JoinType) $2;
			$$->join->left = $1;
			$$->join->right = $4;
			auto left_col = Expr::makeColumnRef(strdup($7->name));
			if ($7->alias != nullptr) left_col->alias = strdup($7->alias);
			if ($1->getName() != nullptr) left_col->table = strdup($1->getName());
			auto right_col = Expr::makeColumnRef(strdup($7->name));
			if ($7->alias != nullptr) right_col->alias = strdup($7->alias);
			if ($4->getName() != nullptr) right_col->table = strdup($4->getName());
			$$->join->condition = Expr::makeOpBinary(left_col, kOpEquals, right_col);
			delete $7;
		}
	;

opt_join_type:
		INNER		{ $$ = kJoinInner; }
	|	LEFT OUTER	{ $$ = kJoinLeft; }
	|	LEFT		{ $$ = kJoinLeft; }
	|	RIGHT OUTER	{ $$ = kJoinRight; }
	|	RIGHT		{ $$ = kJoinRight; }
	|	FULL OUTER	{ $$ = kJoinFull; }
	|	OUTER		{ $$ = kJoinFull; }
	|	FULL		{ $$ = kJoinFull; }
	|	CROSS		{ $$ = kJoinCross; }
	|	/* empty, default */	{ $$ = kJoinInner; }
	;


join_condition:
		expr
		;


/******************************
 * Misc
 ******************************/

opt_semicolon:
		';'
	|	/* empty */
	;


ident_commalist:
		x_compat_ident { $$ = new std::vector<char*>(); $$->push_back($1); }
	|	ident_commalist ',' x_compat_ident { $1->push_back($3); $$ = $1; }
	;

x_compat_ident:
        IDENTIFIER
    |  error { if (yylval.token_str != nullptr) { yyclearin; $$ = strdup(yylval.token_str); } }
    ;

%%
/*********************************
 ** Section 4: Additional C code
 *********************************/

/* empty */
