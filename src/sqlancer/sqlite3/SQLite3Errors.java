package sqlancer.sqlite3;

import java.util.Arrays;

import sqlancer.common.query.ExpectedErrors;

public final class SQLite3Errors {

    private SQLite3Errors() {
    }

    public static void addDeleteErrors(ExpectedErrors errors) {
        // DELETE trigger for a view/table to which colomns were added or deleted
        errors.add("columns but");
        // trigger with on conflict clause
        errors.add("ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint");
    }

    public static void addExpectedExpressionErrors(ExpectedErrors errors) {
        errors.add("[SQLITE_BUSY]  The database file is locked");
        errors.add("FTS expression tree is too large");
        errors.add("String or BLOB exceeds size limit");
        errors.add("[SQLITE_ERROR] SQL error or missing database (integer overflow)");
        errors.add("second argument to likelihood() must be a constant between 0.0 and 1.0");
        errors.add("ORDER BY term out of range");
        errors.add("GROUP BY term out of range");
        errors.add("not authorized"); // load_extension
        errors.add("aggregate functions are not allowed in the GROUP BY clause");
        errors.add("parser stack overflow");

        // nested query
        errors.add("misuse of aggregate");
        errors.add("second argument to nth_value must be a positive integer");
        errors.add("parser stack overflow");

        // window functions
        errors.add("RANGE with offset PRECEDING/FOLLOWING requires one ORDER BY expression");
        errors.add("frame starting offset must be a non-negative integer");
        errors.add("frame starting offset must be a non-negative number");
        errors.add("unsupported frame specification");
        errors.add("frame ending offset must be a non-negative integer");
        errors.add("frame ending offset must be a non-negative number");
        errors.add("argument of ntile must be a positive integer");

        errors.add("malformed JSON");
        errors.add("JSON cannot hold BLOB values");
        errors.add("JSON path error");
        errors.add("json_insert() needs an odd number of arguments");
        errors.add("json_object() labels must be TEXT");
        errors.add("json_object() requires an even number of arguments");

        // fts5 functions
        errors.add("unable to use function highlight in the requested context");
        errors.add("no such cursor");

        // INDEXED BY
        errors.add("no query solution");
        errors.add("no such index");

        // UNION/INTERSECT ...
        errors.add("ORDER BY term does not match any column in the result set");
        errors.add("ORDER BY clause should come after");
        errors.add("LIMIT clause should come after");

    }

    public static void addMatchQueryErrors(ExpectedErrors errors) {
        errors.add("unable to use function MATCH in the requested context");
        errors.add("malformed MATCH expression");
        errors.add("fts5: syntax error near");
        errors.add("no such column"); // vt0.c0 MATCH '-799256540'
        errors.add("unknown special query"); // vt0.c1 MATCH '*YD)LC3^cG|'
        errors.add("fts5: column queries are not supported"); // vt0.c0 MATCH '2016456922'
        errors.add("fts5: phrase queries are not supported");
        errors.add("unterminated string");
    }

    public static void addTableManipulationErrors(ExpectedErrors errors) {
        errors.add("unsupported frame specification");
        errors.add("non-deterministic functions prohibited in CHECK constraints");
        errors.addAll(Arrays.asList("subqueries prohibited in CHECK constraints",
                "generated columns cannot be part of the PRIMARY KEY", "must have at least one non-generated column"));
    }

    public static void addQueryErrors(ExpectedErrors errors) {
        errors.add("ON clause references tables to its right");
    }

    public static void addInsertNowErrors(ExpectedErrors errors) {
        errors.add("non-deterministic use of strftime()");
        errors.add("non-deterministic use of time()");
        errors.add("non-deterministic use of datetime()");
        errors.add("non-deterministic use of julianday()");
        errors.add("non-deterministic use of date()");
    }

    public static void addInsertUpdateErrors(ExpectedErrors errors) {
        errors.add("String or BLOB exceeds size limit");
        errors.add("[SQLITE_CONSTRAINT_CHECK]");
        errors.add("[SQLITE_CONSTRAINT_PRIMARYKEY]");
        errors.add("[SQLITE_CONSTRAINT]");
        errors.add("[SQLITE_CONSTRAINT_NOTNULL]");
        errors.add("[SQLITE_CONSTRAINT_UNIQUE]");
        errors.add("cannot INSERT into generated column"); // TODO: filter out generated columns
        errors.add("A table in the database is locked"); // https://www.sqlite.org/src/tktview?name=56a74875be
        errors.add("The database file is locked");
        errors.add("too many levels of trigger recursion");
        errors.add("cannot UPDATE generated column");
        errors.add("[SQLITE_ERROR] SQL error or missing database (no such table:");
        errors.add("[SQLITE_ERROR] SQL error or missing database (foreign key mismatch");
        errors.add("no such column"); // trigger
    }

}
