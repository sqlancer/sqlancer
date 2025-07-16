package reducer;

import java.util.*;
import sqlancer.ReducerContext;

public class TestSimpleReducer {

    public static void main(String[] args) throws Exception {
        System.out.println("=== SimpleReducer Test Suite ===");

        runTest("Exception Reduction", TestSimpleReducer::testException);
        runTest("NoREC Oracle Reduction", TestSimpleReducer::testNoRECOracle);
        runTest("TLP WHERE Oracle Reduction", TestSimpleReducer::testTLPWhereOracle);

        System.out.println("\n=== All tests completed successfully ===");
    }

    private static void runTest(String testName, Runnable test) {
        System.out.println("\n--- " + testName + " ---");
        test.run();
    }

    private static void testException() {
        try {
            ReducerContext context = createExceptionContext();
            executeTest(context);
        } catch (Exception e) {
            System.err.println("Exception test failed: " + e.getMessage());
        }
    }

    private static void testNoRECOracle() {
        try {
            ReducerContext context = createNoRECOracleContext();
            executeTest(context);
        } catch (Exception e) {
            System.err.println("NoREC Oracle test failed: " + e.getMessage());
        }
    }

    private static void testTLPWhereOracle() {
        try {
            ReducerContext context = createTLPWhereOracleContext();
            executeTest(context);
        } catch (Exception e) {
            System.err.println("TLP WHERE Oracle test failed: " + e.getMessage());
        }
    }

    private static void executeTest(ReducerContext context) throws Exception {
        SimpleReducer reducer = new SimpleReducer(context);
        List<String> result = reducer.reduce();

        System.out.println("Final reduced statements:");
        for (String sql : result) {
            System.out.println("  " + sql);
        }
    }

    // sqlite3 version: 3.28.0
    // https://www.sqlite.org/src/tktview?name=771fe61761
    private static ReducerContext createExceptionContext() {
        List<String> sqlStatements = Arrays.asList("PRAGMA cache_size = 50000;", "PRAGMA temp_store=MEMORY;",
                "PRAGMA synchronous=off;", "PRAGMA encoding = 'UTF-16be';",
                "CREATE VIRTUAL TABLE vt0 USING fts4(c0 UNINDEXED, prefix=426, order=DESC);",
                "CREATE TABLE t1 (c0 REAL, c1 INTEGER UNIQUE ON CONFLICT ROLLBACK NOT NULL, c2 TEXT);",
                "INSERT OR IGNORE INTO t1(c2) VALUES (0.29185622418114365);",
                "INSERT OR IGNORE INTO t1(c1, c2) VALUES (0.053727314532117876, 0X4311a985);",
                "INSERT OR REPLACE INTO t1(c0, c1) VALUES (0.24051690203533227, x'');", "ROLLBACK TRANSACTION;",
                "PRAGMA auto_vacuum = FULL;", "BEGIN IMMEDIATE TRANSACTION;", "ANALYZE;",
                "UPDATE OR FAIL t1 SET (c2)=(NULL);", "INSERT OR ABORT INTO t1(c1) VALUES (0.24051690203533227);",
                "INSERT INTO vt0(vt0, rank) VALUES('rank', 'bm25(10.0, 5.0)');",
                "INSERT INTO vt0(vt0) VALUES('optimize');",
                "INSERT OR IGNORE INTO vt0(c0) VALUES (0.7581753911019898);",
                "INSERT OR ABORT INTO vt0 VALUES (0.5674108139086818);",
                "INSERT INTO vt0(vt0) VALUES('integrity-check');");
        String errorMessage = "[SQLITE_CORRUPT]  The database disk image is malformed (database disk image is malformed)";

        ReducerContext context = new ReducerContext(ReducerContext.ErrorType.EXCEPTION,
                "sqlancer.sqlite3.SQLite3Provider", "sqlite3", "test_exception_db", sqlStatements);
        context.setErrorMessage(errorMessage);

        return context;
    }

    // sqlite3 version: 3.28.0
    // https://www.sqlite.org/src/tktview?name=a7debbe0ad
    private static ReducerContext createNoRECOracleContext() {
        List<String> sqlStatements = Arrays.asList("CREATE TABLE t0 (c0 INTEGER, c1 TEXT, c2 REAL);",
                "INSERT INTO t0(c0) VALUES('');",
                "CREATE VIEW v2(c0, c1) AS SELECT 'B' COLLATE NOCASE, 'a' FROM t0 ORDER BY t0.c0;",
                "CREATE TABLE t1(x INT);", "CREATE TABLE t2(y TEXT);", "CREATE VIEW v1 AS SELECT * FROM t1;",
                "INSERT INTO t1(x) VALUES (1), (2), (3);", "INSERT INTO t2(y) VALUES ('a'), ('b');",
                "SELECT * FROM t1;", "SELECT COUNT(*) FROM t2;",
                "CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN SELECT 'triggered'; END;", "SELECT ABS(-42);",
                "SELECT UPPER('test');", "BEGIN TRANSACTION;", "ROLLBACK;", "SELECT (SELECT MAX(x) FROM t1) AS max_x;",
                "CREATE INDEX idx_x ON t1(x);", "DROP INDEX idx_x;", "CREATE TEMP TABLE temp_tbl(z INT);",
                "INSERT INTO temp_tbl(z) VALUES (10), (20);", "DROP TABLE temp_tbl;");
        Map<String, String> reproducerData = new HashMap<>();
        reproducerData.put("optimizedQuery",
                "SELECT SUM(count) FROM (SELECT v2.c1 BETWEEN v2.c0 AND v2.c1 as count FROM v2);");
        reproducerData.put("unoptimizedQuery",
                "SELECT SUM(CASE WHEN v2.c1 COLLATE BINARY < v2.c0 COLLATE BINARY THEN 1 WHEN v2.c1 COLLATE BINARY > v2.c1 COLLATE BINARY THEN 1 ELSE 0 END) FROM v2;");
        reproducerData.put("shouldUseAggregate", "true");
        Map<String, Set<String>> expectedErrorsMap = new HashMap<>();
        Set<String> commonErrors = new HashSet<>();
        commonErrors.add("no such table");
        expectedErrorsMap.put("SELECT SUM(count) FROM (SELECT v2.c1 BETWEEN v2.c0 AND v2.c1 as count FROM v2);",
                commonErrors);

        ReducerContext context = new ReducerContext(ReducerContext.ErrorType.ORACLE, "sqlancer.sqlite3.SQLite3Provider",
                "sqlite3", "test_norec_db", sqlStatements);
        context.setOracleType(ReducerContext.OracleType.NOREC);
        context.setReproducerData(reproducerData);
        context.setExpectedErrorsMap(expectedErrorsMap);

        return context;
    }

    // duckdb version: 0.2.9
    // https://github.com/cwida/duckdb/issues/590
    private static ReducerContext createTLPWhereOracleContext() {
        List<String> sqlStatements = Arrays.asList("CREATE TABLE t1(c1 VARCHAR);",
                "INSERT INTO t1(c1) VALUES (DATE '2000-01-02');", "CREATE TABLE t0(c0 VARCHAR);",
                "INSERT INTO t0(c0) VALUES (DATE '2000-01-02');", "INSERT INTO t0(c0) VALUES (DATE '2000-01-03');",
                "INSERT INTO t0(c0) VALUES (DATE '2000-01-04');");
        Map<String, String> reproducerData = new HashMap<>();
        reproducerData.put("originalQuery", "SELECT * FROM t0;");
        reproducerData.put("firstQuery", "SELECT * FROM t0 WHERE (DATE '2000-01-01' < t0.c0);");
        reproducerData.put("secondQuery", "SELECT * FROM t0 WHERE (NOT(DATE '2000-01-01' < t0.c0));");
        reproducerData.put("thirdQuery", "SELECT * FROM t0 WHERE ((DATE '2000-01-01' < t0.c0) IS NULL);");
        reproducerData.put("orderBy", "false");

        ReducerContext context = new ReducerContext(ReducerContext.ErrorType.ORACLE, "sqlancer.duckdb.DuckDBProvider",
                "duckdb", "test_tlp_db", sqlStatements);
        context.setOracleType(ReducerContext.OracleType.TLP_WHERE);
        context.setReproducerData(reproducerData);

        return context;
    }
}