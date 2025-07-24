package sqlancer.yugabyte.ysql;

import sqlancer.common.query.ExpectedErrors;

public final class YSQLErrors {

    private YSQLErrors() {
    }

    public static void addCommonFetchErrors(ExpectedErrors errors) {
        errors.add("Table with identifier");

        errors.add("SET TRANSACTION ISOLATION LEVEL must be called before any query");

        errors.add("FULL JOIN is only supported with merge-joinable or hash-joinable join conditions");
        errors.add("but it cannot be referenced from this part of the query");
        errors.add("missing FROM-clause entry for table");
        errors.add("set-returning functions are not allowed in JOIN conditions");
        errors.add("set-returning functions are not allowed in WHERE");
        errors.add("set-returning functions are not allowed in partition key expressions");
        errors.add("set-returning functions are not allowed in VALUES");
        errors.add("set-returning functions are not allowed in RETURNING");
        errors.add("set-returning functions are not allowed in HAVING");
        errors.add("argument of IN must not return a set");
        errors.add("argument of NOT must not return a set");
        errors.add("argument of AND must not return a set");
        errors.add("argument of OR must not return a set");
        errors.add("set-returning functions are not allowed in CASE");
        errors.add("set-returning functions are not allowed in expressions");

        errors.add("canceling statement due to statement timeout");
    }

    public static void addCommonTableErrors(ExpectedErrors errors) {
        errors.add("specified value cannot be cast to type real for column");
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
        errors.add("INDEX on column of type 'TEXTARRAY' not yet supported");
        errors.add("cannot be changed");
        errors.add("cannot split table that does not have primary key");
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
        errors.add("no partition of relation");
        // YugabyteDB Read-Committed specific errors
        errors.add("Read Committed isolation level not supported");
        errors.add("yb_enable_read_committed_isolation must be enabled");
        errors.add("could not serialize access due to read/write dependencies among transactions");
        errors.add("could not serialize access due to concurrent update");
        errors.add("Transaction aborted");
        errors.add("Transaction conflicted");
        errors.add("current transaction is aborted, commands ignored until end of transaction block");
        // Wait-on-Conflict errors
        errors.add("Wait queue operation failed");
        errors.add("yb_enable_wait_queues must be enabled");
        errors.add("Wait-on-Conflict mode requires Read Committed isolation");
        errors.add("Deadlock detected");
        errors.add("Statement timeout while waiting for lock");
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
        errors.add("argument list must have even number of elements");
        errors.add("could not determine data type of parameter");
        errors.add("cannot call jsonb_");
        errors.add("jsonb path is not an array");
        errors.add("invalid input syntax for type json");
        errors.add("token");
        errors.add("JSON");
        errors.add("GROUP BY position");
        errors.add("must not be");
        errors.add("must be");
        errors.add("cannot be changed");
        errors.add("cannot be less");
        errors.add("cannot be greater");

        errors.add("invalid byte sequence for encoding");
        errors.add("cannot convert infinity to integer");
        errors.add("specified value cannot be cast to type");
        errors.add("array OID value not set when in binary upgrade mode");

        errors.add("is not a table");
        errors.add("cannot change materialized view");
        errors.add("syntax error at or near \"(\"");
        errors.add("syntax error at or near \"WITH\"");
        errors.add("syntax error at or near \"RENAME\"");
        errors.add("syntax error at or near \",\"");
        errors.add("syntax error at or near \"ATTACH\"");
        errors.add("syntax error at or near \"SCHEMA\"");
        errors.add("syntax error at or near \"USER\"");
        errors.add("syntax error at or near \"ALL\"");
        errors.add("syntax error at or near");
        errors.add("encoding conversion from");
        errors.add("does not exist");
        errors.add("is not unique");
        errors.add("is not supported");
        errors.add("This statement not supported yet");
        errors.add("not supported yet");
        errors.add("cannot be changed");
        errors.add("invalid reference to FROM-clause entry for table");

        errors.add("Invalid column number");
        errors.add("specified more than once");
        errors.add("character number must be positive");
        errors.add("You might need to add explicit type casts");
        errors.add("invalid regular expression");
        errors.add("could not determine which collation to use");
        errors.add("invalid input syntax for integer");
        errors.add("operator does not exist");
        errors.add("quantifier operand invalid");
        errors.add("collation mismatch");
        errors.add("collations are not supported");
        errors.add("operator is not unique");
        errors.add("is not a valid binary digit");
        errors.add("invalid hexadecimal digit");
        errors.add("invalid hexadecimal data: odd number of digits");
        errors.add("zero raised to a negative power is undefined");
        errors.add("cannot convert infinity to numeric");
        errors.add("division by zero");
        errors.add("invalid input syntax for type money");
        errors.add("invalid input syntax for type");
        errors.add("time zone");
        errors.add("not recognized");
        errors.add("cannot cast type");
        errors.add("cannot cast jsonb array to type");
        errors.add("cannot cast jsonb object to type boolean");
        errors.add("cannot cast jsonb object to type integer");
        errors.add("cannot cast jsonb string to type integer");
        errors.add("cannot cast jsonb string to type boolean");
        errors.add("CASE types");
        errors.add("value overflows numeric format");
        errors.add("is out of range for type");
        errors.add("numeric field overflow");
        errors.add("is of type boolean but expression is of type text");
        errors.add("but default expression is of type");
        errors.add("but expression is of type");
        errors.add("but default expression is of type");
        errors.add("is of type numrange but default expression is of type int4range");
        errors.add("is of type int8range but default expression is of type int4range");
        errors.add("CASE types text and bytea cannot be matched");
        errors.add("a negative number raised to a non-integer power yields a complex result");
        errors.add("could not determine polymorphic type because input has type unknown");
        errors.add("input of anonymous composite types is not implemented");
        
        // Ordering errors for types that don't have natural ordering
        errors.add("could not identify an ordering operator for type circle");
        errors.add("could not identify an ordering operator for type");
        errors.add("could not identify an equality operator for type json");
        errors.add("could not identify an equality operator for type point");
        errors.add("could not identify an equality operator for type circle");
        errors.add("could not identify an equality operator for type box");
        errors.add("could not identify an equality operator for type polygon");
        errors.add("could not identify an equality operator for type lseg");
        errors.add("could not identify an equality operator for type line");
        errors.add("could not identify an equality operator for type path");
        errors.add("cannot cast jsonb numeric to type boolean");
        errors.add("cannot set path in scalar");
        errors.add("cannot delete path in scalar");
        errors.add("aggregate function calls cannot contain set-returning function calls");
        errors.add("single boolean result is expected");
        errors.add("cannot deconstruct a scalar");

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
        errors.add("malformed array literal");
    }

    public static void addCommonRangeExpressionErrors(ExpectedErrors errors) {
        errors.add("range lower bound must be less than or equal to range upper bound");
        errors.add("result of range difference would not be contiguous");
        errors.add("out of range");
        errors.add("malformed range literal");
        errors.add("result of range union would not be contiguous");
    }

    public static void addCommonInsertUpdateErrors(ExpectedErrors errors) {
        errors.add("value too long for type character");
        errors.add("not found in view targetlist");
    }

    public static void addGroupingErrors(ExpectedErrors errors) {
        errors.add("non-integer constant in GROUP BY"); // TODO
        errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
        errors.add("is not in select list");
        errors.add("aggregate functions are not allowed in GROUP BY");
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
        errors.add("non-integer constant in DISTINCT ON");
        errors.add("SELECT DISTINCT ON expressions must match initial ORDER BY expressions");
    }
}
