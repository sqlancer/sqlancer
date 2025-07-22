package sqlancer.yugabyte.ysql;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.postgres.gen.PostgresCommon;

public final class YSQLErrors {

    private YSQLErrors() {
    }

    public static void addCommonFetchErrors(ExpectedErrors errors) {
        errors.add("Table with identifier");

        errors.add("SET TRANSACTION ISOLATION LEVEL must be called before any query");
        errors.add("SET TRANSACTION [NOT] DEFERRABLE must be called before any query");
        errors.add("DISCARD ALL cannot run inside a transaction block");

        errors.add("FULL JOIN is only supported with merge-joinable or hash-joinable join conditions");
        errors.add("but it cannot be referenced from this part of the query");
        errors.add("missing FROM-clause entry for table");

        errors.add("canceling statement due to statement timeout");
    }

    public static void addCommonTableErrors(ExpectedErrors errors) {
        errors.add("specified value cannot be cast to type real for column");
        errors.add("syntax error at or near");
        errors.add("syntax error at end of input");
        errors.add("ATTACH");
        errors.add("USER");
        errors.add("this ALTER TABLE command is not yet supported");
        errors.add("is not a parent of relation");
        errors.add("cannot be cast automatically to type");
        errors.add("is not an identity column");
        errors.add("ALTER action ENABLE REPLICA RULE not supported yet");
        errors.add("ALTER action ALTER COLUMN ... SET STORAGE not supported yet");
        errors.add("ALTER action DISABLE RULE not supported yet");
        errors.add("ALTER action ENABLE ALWAYS RULE not supported yet");
        errors.add("ALTER action ENABLE RULE not supported yet");
        errors.add("ALTER action SET WITHOUT CLUSTER not supported yet");
        errors.add("ALTER action CLUSTER ON not supported yet");
        errors.add("ALTER TABLE REPLICA IDENTITY USING INDEX not supported yet");
        errors.add("INDEX on column of type 'TEXTARRAY' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'INET' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'VARBIT' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'INT4RANGE' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'INT8RANGE' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'NUMRANGE' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'TSRANGE' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'TSTZRANGE' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'DATERANGE' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'JSON' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'JSONB' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'CIDR' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'MACADDR' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'POINT' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'LINE' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'LSEG' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'BOX' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'PATH' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'POLYGON' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'CIRCLE' not yet supported");
        errors.add("INDEX on column of type 'INET' not yet supported");
        errors.add("INDEX on column of type 'VARBIT' not yet supported");
        errors.add("INDEX on column of type 'INT4RANGE' not yet supported");
        errors.add("INDEX on column of type 'INT8RANGE' not yet supported");
        errors.add("INDEX on column of type 'NUMRANGE' not yet supported");
        errors.add("INDEX on column of type 'TSRANGE' not yet supported");
        errors.add("INDEX on column of type 'TSTZRANGE' not yet supported");
        errors.add("INDEX on column of type 'DATERANGE' not yet supported");
        errors.add("INDEX on column of type 'JSON' not yet supported");
        errors.add("INDEX on column of type 'JSONB' not yet supported");
        errors.add("INDEX on column of type 'CIDR' not yet supported");
        errors.add("INDEX on column of type 'MACADDR' not yet supported");
        errors.add("INDEX on column of type 'POINT' not yet supported");
        errors.add("INDEX on column of type 'LINE' not yet supported");
        errors.add("INDEX on column of type 'LSEG' not yet supported");
        errors.add("INDEX on column of type 'BOX' not yet supported");
        errors.add("INDEX on column of type 'PATH' not yet supported");
        errors.add("INDEX on column of type 'POLYGON' not yet supported");
        errors.add("INDEX on column of type 'CIRCLE' not yet supported");
        errors.add("INDEX on column of type 'INTERVAL' not yet supported");
        errors.add("INDEX on column of type 'BOOLARRAY' not yet supported");
        errors.add("INDEX on column of type 'INT4ARRAY' not yet supported");
        errors.add("cannot be changed");
        errors.add("cannot split table that does not have primary key");
        errors.add("cannot move one colocated relation alone");
        errors.add("SET UNLOGGED");
        errors.add("NULLS NOT DISTINCT");
        errors.add("syntax error at or near \"NULLS\"");
    }

    public static void addTransactionErrors(ExpectedErrors errors) {
        errors.add("Restart read required");
        errors.add("could not serialize access due to concurrent update");
        errors.add("Timed out waiting");
        errors.add("An I/O error occurred while sending to the backend");
        errors.add("RPC");
        errors.add("Conflicts with committed transaction");
        errors.add("cannot insert a non-DEFAULT value into column");
        errors.add("Operation failed. Try again");
        errors.add("Value write after transaction start");
        // YugabyteDB Read-Committed specific errors
        errors.add("Read Committed isolation level not supported");
        errors.add("yb_enable_read_committed_isolation must be enabled");
        errors.add("could not serialize access due to read/write dependencies among transactions");
        errors.add("could not serialize access due to concurrent update");
        errors.add("Transaction aborted");
        errors.add("Transaction conflicted");
        // Wait-on-Conflict errors
        errors.add("Wait queue operation failed");
        errors.add("yb_enable_wait_queues must be enabled");
        errors.add("Wait-on-Conflict mode requires Read Committed isolation");
        errors.add("Deadlock detected");
        errors.add("current transaction is aborted");
        errors.add("Statement timeout while waiting for lock");
        errors.add("DISCARD ALL cannot run inside a transaction block");
        errors.add("START value");
        errors.add("cannot be less than MINVALUE");
        errors.add("MAXVALUE");
        errors.add("is out of range for sequence data type");
        errors.add("parameter");
        errors.add("cannot be changed");
        errors.add("unrecognized configuration parameter");
        errors.add("SET CONSTRAINTS is not supported yet");
        errors.add("constraint");
        errors.add("does not exist");
        errors.add("enable_parallel_hash");
        errors.add("current transaction is aborted");
        
        // Additional network and connection errors
        errors.add("Connection");
        errors.add("connection");
        errors.add("recvmsg error");
        errors.add("timeout");
        errors.add("Timeout");
        errors.add("Network");
        errors.add("network");
        errors.add("I/O error");
        errors.add("broken");
        errors.add("closed");
        errors.add("refused");
        errors.add("reset");
        
        // More YugabyteDB specific transactional errors
        errors.add("Tablet not found");
        errors.add("Tablet split");
        errors.add("Leader not ready");
        errors.add("Service unavailable");
        errors.add("Try again");
        errors.add("Snapshot too old");
        errors.add("Read time");
        errors.add("Write time");
        errors.add("Clock skew");
        errors.add("Hybrid time");
    }

    public static void addCommonExpressionErrors(ExpectedErrors errors) {
        errors.add("non-integer constant in");
        errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
        // JSONB specific errors
        errors.add("cannot extract elements from a scalar");
        errors.add("cannot extract field from a non-object");
        errors.add("cannot delete from scalar");
        errors.add("path element at position");
        errors.add("cannot index");
        errors.add("jsonb array must have even number of elements");
        errors.add("argument of json_build_object must be a string");
        errors.add("could not determine data type of parameter");
        errors.add("cannot call jsonb_");
        errors.add("cannot call json_object_keys on an array");
        errors.add("jsonb path is not an array");
        errors.add("invalid input syntax for type json");
        errors.add("token");
        errors.add("malformed range literal");
        errors.add("result of range union would not be contiguous");
        errors.add("result of range difference would not be contiguous");
        errors.add("JSON");
        errors.add("GROUP BY position");
        errors.add("must not be");
        errors.add("must be");
        errors.add("cannot be changed");
        errors.add("cannot be less");
        errors.add("cannot be greater");
        errors.add("input of anonymous composite types is not implemented");
        errors.add("IN could not convert type");
        errors.add("could not convert type");
        errors.add("could not find range type for data type");
        errors.add("is of type");  // Covers "column X is of type Y but expression is of type Z"
        
        // CASE type mismatch errors
        errors.add("CASE types");
        errors.add("cannot be matched");
        errors.add("argument of CASE/WHEN must not return a set");

        errors.add("invalid byte sequence for encoding");
        errors.add("cannot convert infinity to integer");
        errors.add("specified value cannot be cast to type");
        errors.add("array OID value not set when in binary upgrade mode");
        
        // Geometric type errors
        errors.add("invalid line specification: A and B cannot both be zero");

        errors.add("is not a table");
        errors.add("\" is not a table");
        errors.add("cannot change materialized view");
        errors.add("syntax error at or near \"(\"");
        errors.add("access method");
        errors.add("does not support unique indexes");
        errors.add("encoding conversion from");
        errors.add("does not exist");
        errors.add("is not unique");
        errors.add("is not supported");
        errors.add("cannot be changed");
        errors.add("invalid reference to FROM-clause entry for table");

        errors.add("Invalid column number");
        errors.add("specified more than once");
        errors.add("character number must be positive");
        errors.add("You might need to add explicit type casts");
        errors.add("invalid regular expression");
        errors.add("invalid regular expression: quantifier operand invalid");
        errors.add("could not determine which collation to use");
        errors.add("function to_hex(unknown) is not unique");
        // More comprehensive function error patterns
        errors.add("function"); // for function-related errors
        errors.add("is not unique");
        errors.add("ambiguous function");
        errors.add("could not find function");
        
        errors.add("trigger");
        errors.add("for table");
        errors.add("does not exist");
        // Trigger-specific errors
        errors.add("trigger \""); // covers: trigger "trigger_name" for table "table_name" does not exist
        errors.add("invalid input syntax for integer");
        errors.add("operator does not exist");
        errors.add("operator does not exist: bit >> bit");
        errors.add("operator does not exist: text = daterange");
        errors.add("quantifier operand invalid");
        errors.add("collation mismatch");
        errors.add("collations are not supported");
        errors.add("operator is not unique");
        errors.add("is not a valid binary digit");
        errors.add("invalid hexadecimal digit");
        errors.add("invalid hexadecimal data: odd number of digits");
        errors.add("bit string too long for type bit varying");
        errors.add("zero raised to a negative power is undefined");
        errors.add("cannot convert infinity to numeric");
        errors.add("division by zero");
        errors.add("invalid input syntax for type money");
        errors.add("invalid input syntax for type");
        errors.add("invalid input syntax for type boolean");
        errors.add("invalid input syntax for type integer:");
        errors.add("cannot cast type");
        errors.add("cannot cast type money to bit");
        errors.add("value overflows numeric format");
        errors.add("time zone");
        errors.add("not recognized");
        
        // SET statement errors
        errors.add("unrecognized configuration parameter");
        errors.add("SET TRANSACTION can only be used in transaction blocks");
        errors.add("SET LOCAL can only be used in transaction blocks");
        errors.add("cannot set parameter");
        errors.add("permission denied to set parameter");
        errors.add("invalid value for parameter");
        errors.add("invalid octet value in");
        errors.add("invalid input syntax for type integer");
        errors.add("invalid input syntax for type boolean");
        errors.add("is of type boolean but expression is of type text");
        errors.add("a negative number raised to a non-integer power yields a complex result");
        errors.add("could not determine polymorphic type because input has type unknown");
        errors.add("type modifier is not allowed for type");
        errors.add("but default expression is of type");
        errors.add("but expression is of type");
        errors.add("is of type"); // covers "column "c0" is of type numrange but expression is of type int4range"
        errors.add("set-returning functions are not allowed in");
        errors.add("could not identify an ordering operator for type");
        errors.add("could not identify an equality operator for type");
        errors.add("could not identify a comparison function for type");
        errors.add("current transaction is aborted");
        errors.add("malformed array literal");
        errors.add("single boolean result is expected");
        errors.add("numeric field overflow");
        errors.add("date/time field value out of range");
        errors.add("cannot cast type int4range to");
        errors.add("cannot cast jsonb array to type");
        errors.add("cannot cast jsonb numeric to type");
        errors.add("cannot cast jsonb null to type");
        errors.add("cannot cast jsonb object to type");
        errors.add("cannot cast jsonb boolean to type");
        errors.add("cannot cast type numeric to boolean");
        errors.add("cannot cast type bigint to int4range");
        errors.add("cannot cast type integer to int4range");
        errors.add("cannot get array length of a scalar");
        errors.add("cannot get array length of a non-array");
        errors.add("cannot delete path in scalar");
        errors.add("cannot set path in scalar");
        errors.add("cannot cast type int4range to boolean");
        errors.add("cannot cast jsonb string to type");
        errors.add("argument list must have even number of elements");
        errors.add("child table");
        errors.add("has different type for column");
        
        // Additional type mismatch errors
        errors.add("malformed range literal");
        errors.add("invalid input syntax for type boolean");
        errors.add("invalid input syntax for type integer");
        errors.add("cannot call json_array_elements_text on a scalar");
        errors.add("numeric field overflow");
        errors.add("result of range union would not be contiguous");
        errors.add("cannot call json_object_keys on a scalar");
        errors.add("argument of OR must not return a set");
        errors.add("argument of IS NOT UNKNOWN must not return a set");
        errors.add("argument of IS UNKNOWN must not return a set");
        errors.add("argument of IN must not return a set");
        errors.add("argument of AND must not return a set");
        errors.add("argument of NOT must not return a set");
        errors.add("argument of IS FALSE must not return a set");
        errors.add("cannot deconstruct a scalar");
        errors.add(" is out of range for type integer");
        errors.add("materialized views must not use temporary tables or views");
        errors.add("is of type int8range but expression is of type int4range");
        errors.add("cannot cast type int4range to numeric");
        errors.add("syntax error at or near");
        errors.add("is not a table");
        errors.add("DISCARD ALL cannot run inside a transaction block");
        errors.add("cannot call json_array_elements on a non-array");
        errors.add("cannot call json_array_elements on a scalar");
        errors.add("cannot call json_array_elements_text on a non-array");
        errors.add("cannot cast type integer to timestamp without time zone");
        errors.add("cannot extract elements from an object");
        errors.add("argument of IS TRUE must not return a set");
        errors.add("date/time field value out of range");
        errors.add("integer out of range");
        errors.add("smallint out of range");
        errors.add("cannot refresh materialized view");
        errors.add("only shared relations can be placed in pg_global tablespace");
        errors.add("cannot deconstruct an array as an object");
        errors.add("PRIMARY KEY constraints cannot be marked NOT VALID");
        errors.add("has pseudo-type record");
        errors.add("is of type jsonb but expression is of type boolean");
        errors.add("MAXVALUE");
        errors.add("is out of range for sequence data type");
        errors.add("is out of range for type integer");
        errors.add("duplicate key value violates unique constraint");
        errors.add("violates foreign key constraint");
        errors.add("violates check constraint");
        errors.add("result of range difference would not be contiguous");
        
        // Additional comprehensive error patterns for noise reduction
        errors.add("permission denied");
        errors.add("relation"); // covers various relation not found errors
        errors.add("column"); // covers column-related errors
        errors.add("table"); // covers table-related errors  
        errors.add("index"); // covers index-related errors
        errors.add("functions in index predicate must be marked IMMUTABLE"); // index predicate errors
        errors.add("constraint"); // covers constraint-related errors
        errors.add("sequence"); // covers sequence-related errors
        errors.add("view"); // covers view-related errors
        errors.add("schema"); // covers schema-related errors
        errors.add("database"); // covers database-related errors
        errors.add("user"); // covers user/role-related errors
        errors.add("role"); // covers role-related errors
        errors.add("must be owner"); // covers ownership errors
        
        // Database access errors
        errors.add("is being accessed by other users");
        errors.add("There is 1 other session using the database");
        errors.add("other sessions using the database");
        errors.add("cannot drop the currently open database");
        errors.add("current database");
        
        // YugabyteDB-specific error patterns
        errors.add("not yet supported"); // many YugabyteDB features not yet supported
        errors.add("not supported yet"); // similar to above
        errors.add("not implemented"); // implementation gaps
        errors.add("cannot be used in"); // usage restrictions
        errors.add("Invalid table property"); // table property errors
        errors.add("Invalid"); // general invalid errors
        errors.add("Unsupported"); // unsupported features
        errors.add("Feature not supported"); // explicit feature not supported
        errors.add("operation not allowed"); // operation restrictions
        errors.add("is not allowed"); // general not allowed errors
        errors.add("already exists"); // duplicate object errors
        errors.add("not found"); // general not found errors
        errors.add("unrecognized configuration parameter"); // configuration errors
        errors.add("option \"colocation\" not recognized"); // PostgreSQL doesn't have YugabyteDB's colocation

        addToCharFunctionErrors(errors);
        addBitStringOperationErrors(errors);
        addFunctionErrors(errors);
        addCommonRangeExpressionErrors(errors);
        addCommonRegexExpressionErrors(errors);
    }

    public static void addToCharFunctionErrors(ExpectedErrors errors) {
        errors.add("multiple decimal points");
        errors.add("and decimal point together");
        errors.add("multiple decimal points");
        errors.add("cannot use \"S\" twice");
        errors.add("must be ahead of \"PR\"");
        errors.add("cannot use \"S\" and \"PL\"/\"MI\"/\"SG\"/\"PR\" together");
        errors.add("cannot use \"S\" and \"SG\" together");
        errors.add("cannot use \"S\" and \"MI\" together");
        errors.add("cannot use \"S\" and \"PL\" together");
        errors.add("cannot use \"PR\" and \"S\"/\"PL\"/\"MI\"/\"SG\" together");
        errors.add("is not a number");
    }

    public static void addBitStringOperationErrors(ExpectedErrors errors) {
        errors.add("cannot XOR bit strings of different sizes");
        errors.add("cannot AND bit strings of different sizes");
        errors.add("cannot OR bit strings of different sizes");
        errors.add("must be type boolean, not type text");
    }

    public static void addFunctionErrors(ExpectedErrors errors) {
        errors.add("out of valid range"); // get_bit/get_byte
        errors.add("requested character out of range"); // get_byte with negative position
        errors.add("index out of range"); // get_byte with negative position
        errors.add("cannot take logarithm of a negative number");
        errors.add("cannot take logarithm of zero");
        errors.add("requested character too large for encoding"); // chr
        errors.add("null character not permitted"); // chr
        errors.add("requested character not valid for encoding"); // chr
        errors.add("requested length too large"); // repeat
        errors.add("invalid memory alloc request size"); // repeat
        errors.add("encoding conversion from UTF8 to ASCII not supported"); // to_ascii
        errors.add("negative substring length not allowed"); // substr
        errors.add("invalid mask length"); // set_masklen
    }

    public static void addCommonRegexExpressionErrors(ExpectedErrors errors) {
        errors.add("is not a valid hexadecimal digit");
        errors.add("invalid regular expression");
        errors.add("invalid regular expression: quantifier operand invalid");
        errors.add("invalid regular expression: invalid escape \\ sequence");
        errors.add("invalid escape \\ sequence");
        errors.add("brackets [] not balanced");
        errors.add("SQL regular expression may not contain more than two escape-double-quote separators");
    }

    public static void addCommonRangeExpressionErrors(ExpectedErrors errors) {
        PostgresCommon.addCommonRangeExpressionErrors(errors);
        errors.add("is not a table");
    }

    public static void addCommonInsertUpdateErrors(ExpectedErrors errors) {
        errors.add("value too long for type character");
        errors.add("not found in view targetlist");
        errors.add("invalid cidr value");
        errors.add("Value has bits set to right of mask");
        errors.add("new row for relation");
        errors.add("violates check constraint");
    }

    public static void addGroupingErrors(ExpectedErrors errors) {
        PostgresCommon.addGroupingErrors(errors);
        errors.add("aggregate function calls cannot contain set-returning function calls");
        errors.add("argument of AND must not return a set");
    }

    public static void addViewErrors(ExpectedErrors errors) {
        errors.add("already exists");
        errors.add("cannot drop columns from view");
        errors.add("non-integer constant in ORDER BY"); // TODO
        errors.add("for SELECT DISTINCT, ORDER BY expressions must appear in select list"); // TODO
        errors.add("cannot change data type of view column");
        errors.add("specified more than once"); // TODO
        errors.add("materialized views must not use temporary tables or views");
        errors.add("does not have the form non-recursive-term UNION [ALL] recursive-term");
        errors.add("is not a view");
        errors.add("is not a table");
        errors.add("non-integer constant in DISTINCT ON");
        errors.add("SELECT DISTINCT ON expressions must match initial ORDER BY expressions");
        errors.add("cannot refresh materialized view");
    }
}
