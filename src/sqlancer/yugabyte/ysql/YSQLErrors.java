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
        // Syntax errors
        errors.add("syntax error");
        errors.add("ATTACH");
        errors.add("USER");
        
        // ALTER TABLE errors
        errors.add("this ALTER TABLE command is not yet supported");
        errors.add("ALTER action");
        errors.add("ALTER TABLE REPLICA IDENTITY USING INDEX not supported yet");
        
        // Type support errors
        errors.add("PRIMARY KEY containing column of type");
        errors.add("INDEX on column of type");
        errors.add("not yet supported");
        errors.add("is not supported");
        
        // Table and relation errors
        errors.add("is not a parent of relation");
        errors.add("is not an identity column");
        errors.add("is not a table");
        errors.add("cannot change materialized view");
        errors.add("cannot split table that does not have primary key");
        errors.add("cannot move one colocated relation alone");
        errors.add("materialized views must not use temporary tables or views");
        errors.add("cannot refresh materialized view");
        errors.add("only shared relations can be placed in pg_global tablespace");
        
        // Constraint errors
        errors.add("PRIMARY KEY constraints cannot be marked NOT VALID");
        errors.add("violates check constraint");
        errors.add("violates foreign key constraint");
        errors.add("duplicate key value violates unique constraint");
        
        // Other table errors
        errors.add("SET UNLOGGED");
        errors.add("NULLS NOT DISTINCT");
        errors.add("has pseudo-type record");
        errors.add("access method");
        errors.add("encoding conversion from");
        errors.add("specified more than once");
        errors.add("Invalid column number");
        
        // Global expected errors
        errors.add("Catalog Version Mismatch");
        errors.add("Restart read required");
        errors.add("could not serialize access");
        errors.add("Timed out");
        errors.add("RPC");
        errors.add("I/O error");
        errors.add("Operation failed");
        errors.add("Transaction");
        errors.add("Deadlock detected");
        errors.add("current transaction is aborted");
        errors.add("cannot insert a non-DEFAULT value into column");
        errors.add("Value write after transaction start");
        
        // Read-Committed and Wait-on-Conflict errors
        errors.add("Read Committed isolation level not supported");
        errors.add("yb_enable_read_committed_isolation must be enabled");
        errors.add("yb_enable_wait_queues must be enabled");
        errors.add("Wait-on-Conflict mode requires Read Committed isolation");
        errors.add("Wait queue operation failed");
        errors.add("Statement timeout while waiting for lock");
        
        // Sequence errors
        errors.add("START value");
        errors.add("MINVALUE");
        errors.add("MAXVALUE");
        errors.add("is out of range for sequence data type");
        
        // Configuration and parameter errors
        errors.add("parameter");
        errors.add("cannot be changed");
        errors.add("unrecognized configuration parameter");
        errors.add("SET CONSTRAINTS is not supported yet");
        errors.add("SET TRANSACTION can only be used in transaction blocks");
        errors.add("SET LOCAL can only be used in transaction blocks");
        errors.add("cannot set parameter");
        errors.add("permission denied to set parameter");
        errors.add("invalid value for parameter");
        
        // Connection and network errors
        errors.add("Connection");
        errors.add("connection");
        errors.add("Network");
        errors.add("network");
        errors.add("timeout");
        errors.add("broken");
        errors.add("closed");
        errors.add("refused");
        errors.add("reset");
        errors.add("recvmsg error");
        
        // YugabyteDB specific errors
        errors.add("Tablet");
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
        // Basic expression errors
        errors.add("non-integer constant in");
        errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
        errors.add("GROUP BY position");
        errors.add("argument of");
        errors.add("single boolean result is expected");
        
        // JSONB errors
        errors.add("cannot extract");
        errors.add("cannot delete");
        errors.add("cannot index");
        errors.add("cannot call json");
        errors.add("cannot get array length");
        errors.add("cannot set path");
        errors.add("cannot deconstruct");
        errors.add("jsonb");
        errors.add("JSON");
        errors.add("path element at position");
        errors.add("argument list must have even number of elements");
        
        // Type and casting errors
        errors.add("invalid input syntax for type");
        errors.add("invalid byte sequence for encoding");
        errors.add("cannot cast type");
        errors.add("cannot convert");
        errors.add("could not convert type");
        errors.add("could not determine");
        errors.add("specified value cannot be cast to type");
        errors.add("is of type");
        errors.add("but expression is of type");
        errors.add("but default expression is of type");
        errors.add("type modifier is not allowed for type");
        errors.add("array OID value not set when in binary upgrade mode");
        errors.add("input of anonymous composite types is not implemented");
        
        // Range type errors
        errors.add("malformed range literal");
        errors.add("result of range union would not be contiguous");
        errors.add("result of range difference would not be contiguous");
        errors.add("could not find range type for data type");
        errors.add("invalid line specification");
        
        // Numeric errors
        errors.add("numeric field overflow");
        errors.add("integer out of range");
        errors.add("smallint out of range");
        errors.add("division by zero");
        errors.add("zero raised to a negative power is undefined");
        errors.add("a negative number raised to a non-integer power yields a complex result");
        errors.add("value overflows numeric format");
        
        // Date/time errors
        errors.add("date/time field value out of range");
        errors.add("time zone");
        
        // Other expression errors
        errors.add("CASE types");
        errors.add("malformed array literal");
        errors.add("invalid reference to FROM-clause entry");
        errors.add("character number must be positive");
        errors.add("You might need to add explicit type casts");
        errors.add("set-returning functions are not allowed in");
        errors.add("could not identify");
        errors.add("child table");
        errors.add("has different type for column");
    }

    public static void addCommonRegressionErrors(ExpectedErrors errors) {
        errors.add("invalid regular expression");
        errors.add("quantifier operand invalid");
        errors.add("invalid escape");
        errors.add("collation");
        errors.add("is not unique");
        errors.add("ambiguous");
    }

    public static void addGroupingErrors(ExpectedErrors errors) {
        PostgresCommon.addGroupingErrors(errors);
        YSQLErrors.addCommonExpressionErrors(errors);
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        errors.add("cannot insert");
        errors.add("functions in index predicate must be marked IMMUTABLE");
    }
    
    public static void addCommonInsertUpdateErrors(ExpectedErrors errors) {
        addInsertErrors(errors);
        addCommonTableErrors(errors);
        addCommonExpressionErrors(errors);
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        PostgresCommon.addCommonExpressionErrors(errors);
        YSQLErrors.addCommonExpressionErrors(errors);
    }

    public static void addTransactionErrors(ExpectedErrors errors) {
        errors.add("there is no unique or exclusion constraint matching the ON CONFLICT specification");
        YSQLErrors.addCommonTableErrors(errors);
    }

    public static void addFunctionErrors(ExpectedErrors errors) {
        errors.add("does not exist");
        errors.add("operator does not exist");
        errors.add("function");
        errors.add("trigger");
        errors.add("constraint");
        errors.add("permission denied");
        
        // Binary/hex errors
        errors.add("is not a valid binary digit");
        errors.add("invalid hexadecimal");
        errors.add("bit string too long");
        
        // Set-returning function errors
        errors.add("must not return a set");
    }

    public static void addViewErrors(ExpectedErrors errors) {
        errors.add("relation");
        errors.add("table");
        errors.add("view");
        errors.add("column");
        errors.add("index");
        errors.add("sequence");
        errors.add("schema");
        errors.add("database");
        errors.add("user");
        errors.add("role");
    }

    public static void addLoadExtensionError(ExpectedErrors errors) {
        errors.add("extension");
        errors.add("library");
        errors.add("could not open extension control file");
        errors.add("This statement not supported yet");
        errors.add("Load");
        errors.add("permission denied");
    }
}