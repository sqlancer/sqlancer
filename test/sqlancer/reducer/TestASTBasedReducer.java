package sqlancer.reducer;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.junit.jupiter.api.Test;
import sqlancer.common.query.Query;

import java.util.Arrays;
import java.util.List;

public class TestASTBasedReducer {
    @Test
    void testLongStatement() throws Exception {
        TestEnvironment env = TestEnvironment.getASTBasedReducerEnv();

        String[] queriesStr = {
                "SELECT DISTINCT * FROM v0 WHERE ((v0.rowid || ( (v0.c + v0.d) < 200 && v0.c >= 100) || 114514)OR(((v0.c0)||(1529686005)))) UNION SELECT DISTINCT * FROM v0 WHERE (NOT ((v0.rowid)OR(((v0.c0)||(1529686005))))) UNION SELECT DISTINCT * FROM v0 WHERE ((((v0.rowid)OR(((v0.c0)||(1529686005))))) IS NULL)" };
        env.setInitialStatementsFromStrings(List.of(queriesStr));
        env.setBugInducingCondition(statements -> {
            String queriesString = TestEnvironment.getQueriesString(statements);
            try {
                CCJSqlParserUtil.parse(queriesString);
            } catch (JSQLParserException e) {
                return false;
            }
            return queriesString.contains("&&");
        });
        env.runReduce();
        List<Query<?>> reducedResult = env.getReducedStatements();
        System.out.println(Arrays.toString(queriesStr));
        System.out.println(reducedResult);
    }

    @Test
    void testReducingMultipleTokensToOne() throws Exception {
        TestEnvironment env = TestEnvironment.getASTBasedReducerEnv();

        String[] queriesStr = {
                "SELECT DISTINCT row_id, c FROM v0 WHERE ((v0.rowid || (v0.c < 200 && v0.c >= 100) || 114514)OR(((v0.c0)||(1529686005)))) UNION SELECT DISTINCT * FROM v0 WHERE (NOT ((v0.rowid)OR(((v0.c0)||(1529686005))))) UNION SELECT DISTINCT * FROM v0 WHERE ((((v0.rowid)OR(((v0.c0)||(1529686005))))) IS NULL)" };
        env.setInitialStatementsFromStrings(List.of(queriesStr));
        env.setBugInducingCondition(statements -> {
            String queriesString = TestEnvironment.getQueriesString(statements);
            try {
                CCJSqlParserUtil.parse(queriesString);
            } catch (JSQLParserException e) {
                return false;
            }
            return queriesString.contains("||");
        });
        env.runReduce();
        List<Query<?>> reducedResult = env.getReducedStatements();
        System.out.println(Arrays.toString(queriesStr));
        System.out.println(reducedResult);
    }

    @Test
    void testMultipleStatements() throws Exception {
        TestEnvironment env = TestEnvironment.getASTBasedReducerEnv();

        String[] queriesStrs = {
                "SELECT DISTINCT row_id, c FROM v0 WHERE ((v0.rowid || (v0.c < 200 && v0.c >= 100) || 114514)OR(((v0.c0)||(1529686005)))) UNION SELECT DISTINCT * FROM v0 WHERE (NOT ((v0.rowid)OR(((v0.c0)||(1529686005))))) UNION SELECT DISTINCT * FROM v0 WHERE ((((v0.rowid)OR(((v0.c0)||(1529686005))))) IS NULL)",
                "SELECT DISTINCT row_id, c FROM v0 WHERE ((v0.rowid || (v0.c < 200 && v0.c >= 100) || 114514)OR(((v0.c0)||(1529686005)))) UNION SELECT DISTINCT * FROM v0 WHERE (NOT ((v0.rowid)OR(((v0.c0)||(1529686005))))) UNION SELECT DISTINCT * FROM v0 WHERE ((((v0.rowid)OR(((v0.c0)||(1529686005))))) IS NULL)" };
        env.setInitialStatementsFromStrings(List.of(queriesStrs));
        env.setBugInducingCondition(statements -> {
            String queriesString = TestEnvironment.getQueriesString(statements);
            try {
                for (Query<?> s : statements) {
                    CCJSqlParserUtil.parse(s.getQueryString());
                }
            } catch (JSQLParserException e) {
                return false;
            }

            return queriesString.toUpperCase().contains("UNION");
        });
        env.runReduce();
        List<Query<?>> reducedResult = env.getReducedStatements();
        System.out.println(Arrays.toString(queriesStrs));
        System.out.println(reducedResult);
    }

    @Test
    void testJoin() throws Exception {
        TestEnvironment env = TestEnvironment.getASTBasedReducerEnv();

        String[] queriesStrs = { "SELECT * FROM t0, t1, t2, t3, t4 Where t2.val = t1.val" };
        env.setInitialStatementsFromStrings(List.of(queriesStrs));
        env.setBugInducingCondition(statements -> {
            try {
                for (Query<?> s : statements) {
                    CCJSqlParserUtil.parse(s.getQueryString());
                }
            } catch (JSQLParserException e) {
                return false;
            }

            String queriesString = TestEnvironment.getQueriesString(statements);
            return queriesString.contains("t1") && queriesString.contains("WHERE");
        });
        env.runReduce();
        List<Query<?>> reducedResult = env.getReducedStatements();
        System.out.println(Arrays.toString(queriesStrs));
        System.out.println(reducedResult);
    }

    @Test
    void testComplicated() throws Exception {
        TestEnvironment env = TestEnvironment.getASTBasedReducerEnv();

        String[] queriesStrs = {
                "SELECT STRING_AGG(v0.c2) FROM t0, v0 WHERE (CASE true WHEN (ABS(true) BETWEEN (v0.c0 LIKE NULL ESCAPE v0.c2) AND (DATE '1970-01-23' NOT IN (v0.c2))) THEN (0.07914839711718646 NOT BETWEEN '' AND ((v0.c0)OR(v0.c2))) WHEN v0.c1 THEN ((v0.c1)-(v0.c0)) WHEN t0.c1 THEN (TIMESTAMP '1969-12-29 20:22:33' IN (PI(), v0.c2, (v0.c1 BETWEEN '' AND v0.rowid))) WHEN v0.c1 THEN TIMESTAMP '1969-12-16 17:24:43' WHEN ((((v0.c1)-(t0.c0)))||(t0.c0)) THEN true ELSE ((0.279978719843174)/(((v0.c1)>(DATE '1969-12-19')))) END ) GROUP BY ((DATE '1970-01-24') IS NULL), t0.c1, (CASE (v0.c1 LIKE ((0.9833120083624495)SIMILAR TO(t0.rowid)) ESCAPE CEIL(TIMESTAMP '1970-01-11 16:38:26')) WHEN t0.rowid THEN 0.27742217994251717 ELSE ((v0.c0) IS NOT NULL) END );" };
        env.setInitialStatementsFromStrings(List.of(queriesStrs));
        env.setBugInducingCondition(statements -> {
            String queriesString = TestEnvironment.getQueriesString(statements);
            try {
                CCJSqlParserUtil.parse(queriesString);
            } catch (JSQLParserException e) {
                return false;
            }
            return queriesString.toUpperCase().contains("CASE");
        });
        env.runReduce();
        List<Query<?>> reducedResult = env.getReducedStatements();
        System.out.println(Arrays.toString(queriesStrs));
        System.out.println(reducedResult);
    }

    @Test
    void testConstantVar() throws Exception {
        TestEnvironment env = TestEnvironment.getASTBasedReducerEnv();

        String[] queriesStrs = {
                "SELECT * FROM t0 GROUP BY ( (CASE (t0.rowid) WHEN t0.rowid THEN 27742217994251717 ELSE ((v0.c0) IS NOT NULL) END) )" };
        env.setInitialStatementsFromStrings(List.of(queriesStrs));
        env.setBugInducingCondition(statements -> {
            try {
                for (Query<?> s : statements) {
                    CCJSqlParserUtil.parse(s.getQueryString());
                }
            } catch (JSQLParserException e) {
                return false;
            }

            String queriesString = TestEnvironment.getQueriesString(statements);
            return queriesString.toUpperCase().contains("CASE");
        });
        env.runReduce();
        List<Query<?>> reducedResult = env.getReducedStatements();
        System.out.println(Arrays.toString(queriesStrs));
        System.out.println(reducedResult);
    }

    @Test
    void testSubSelects() throws Exception {
        TestEnvironment env = TestEnvironment.getASTBasedReducerEnv();

        String[] queriesStrs = {
                // "SELECT AVG(sum_column1) FROM (SELECT SUM(column1) AS sum_column1 FROM t1 GROUP BY column1 LIMIT 32
                // OFFSET 128) AS t1;",
                "WITH cte1 AS (SELECT a, b FROM table1), cte2 AS (SELECT c, d FROM table2) SELECT b, d FROM cte1 JOIN cte2 WHERE cte1.a = cte2.c" };
        env.setInitialStatementsFromStrings(List.of(queriesStrs));
        env.setBugInducingCondition(statements -> {
            String queriesString = TestEnvironment.getQueriesString(statements);
            try {
                for (Query<?> s : statements) {
                    CCJSqlParserUtil.parse(s.getQueryString());
                }
            } catch (JSQLParserException e) {
                return false;
            }
            return queriesString.contains("cte2");
        });
        env.runReduce();
        List<Query<?>> reducedResult = env.getReducedStatements();
        System.out.println(Arrays.toString(queriesStrs));
        System.out.println(reducedResult);
    }

    @Test
    void testInsert() throws Exception {
        TestEnvironment env = TestEnvironment.getASTBasedReducerEnv();

        String[] queriesStrs = {
                "INSERT INTO t1(c2, c0) VALUES (1508438260, 2929), (1508438260, TIMESTAMP '1969-12-26 01:57:21'), (0.5347171705591047, 398662142);" };
        env.setInitialStatementsFromStrings(List.of(queriesStrs));
        env.setBugInducingCondition(statements -> {
            String queriesString = TestEnvironment.getQueriesString(statements);
            try {
                CCJSqlParserUtil.parse(queriesString);
            } catch (JSQLParserException e) {
                return false;
            }
            return queriesString.toUpperCase().contains("0.5347171705591047");
        });
        env.runReduce();
        List<Query<?>> reducedResult = env.getReducedStatements();
        System.out.println(Arrays.toString(queriesStrs));
        System.out.println(reducedResult);
    }

}
