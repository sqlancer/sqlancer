package sqlancer.reducer;

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
                "SELECT DISTINCT row_id, c FROM v0 WHERE ((v0.rowid || (v0.c < 200 && v0.c >= 100) || 114514)OR(((v0.c0)||(1529686005)))) UNION SELECT DISTINCT * FROM v0 WHERE (NOT ((v0.rowid)OR(((v0.c0)||(1529686005))))) UNION SELECT DISTINCT * FROM v0 WHERE ((((v0.rowid)OR(((v0.c0)||(1529686005))))) IS NULL)",
                "SELECT * FROM table_3;" };
        env.setInitialStatementsFromStrings(List.of(queriesStrs));
        env.setBugInducingCondition(statements -> {
            String queriesString = TestEnvironment.getQueriesString(statements);
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
            String queriesString = TestEnvironment.getQueriesString(statements);
            return queriesString.contains("t1") && queriesString.contains("WHERE");
        });
        env.runReduce();
        List<Query<?>> reducedResult = env.getReducedStatements();
        System.out.println(Arrays.toString(queriesStrs));
        System.out.println(reducedResult);
    }

    @Test
    void testCase() throws Exception {
        TestEnvironment env = TestEnvironment.getASTBasedReducerEnv();

        String[] queriesStrs = {
                "SELECT STRING_AGG(v0.c2) FROM t0, v0 WHERE (CASE true WHEN (ABS(true) BETWEEN (v0.c0 LIKE NULL ESCAPE v0.c2) AND (DATE '1970-01-23' NOT IN (v0.c2))) THEN (0.07914839711718646 NOT BETWEEN '' AND ((v0.c0)OR(v0.c2))) WHEN v0.c1 THEN ((v0.c1)-(v0.c0)) WHEN t0.c1 THEN (TIMESTAMP '1969-12-29 20:22:33' IN (PI(), v0.c2, (v0.c1 BETWEEN '' AND v0.rowid))) WHEN v0.c1 THEN TIMESTAMP '1969-12-16 17:24:43' WHEN ((((v0.c1)-(t0.c0)))||(t0.c0)) THEN true ELSE ((0.279978719843174)/(((v0.c1)>(DATE '1969-12-19')))) END ) GROUP BY ((DATE '1970-01-24') IS NULL), t0.c1, (CASE (v0.c1 LIKE ((0.9833120083624495)SIMILAR TO(t0.rowid)) ESCAPE CEIL(TIMESTAMP '1970-01-11 16:38:26')) WHEN t0.rowid THEN 0.27742217994251717 ELSE ((v0.c0) IS NOT NULL) END );" };
        env.setInitialStatementsFromStrings(List.of(queriesStrs));
        env.setBugInducingCondition(statements -> {
            String queriesString = TestEnvironment.getQueriesString(statements);
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
                "SELECT STRING_AGG(v0.c2) FROM t0 GROUP BY ( (CASE (t0.rowid) WHEN t0.rowid THEN 0.27742217994251717 ELSE ((v0.c0) IS NOT NULL) END) )" };
        env.setInitialStatementsFromStrings(List.of(queriesStrs));
        env.setBugInducingCondition(statements -> {
            String queriesString = TestEnvironment.getQueriesString(statements);
            return queriesString.toUpperCase().contains("CASE");
        });
        env.runReduce();
        List<Query<?>> reducedResult = env.getReducedStatements();
        System.out.println(Arrays.toString(queriesStrs));
        System.out.println(reducedResult);
    }

    @Test
    void testFunction() throws Exception {
        TestEnvironment env = TestEnvironment.getASTBasedReducerEnv();

        String[] queriesStrs = {
                "SELECT DATE '1970-01-11', false, t1.c1, t1.c1, (t1.c1 NOT IN (((('' LIKE t1.c2 ESCAPE t1.c1)) IS NOT NULL))) FROM t1 WHERE t1.c0 GROUP BY (((('Zlb)' IN (t1.c0)) LIKE t1.c1 ESCAPE (0.6419925594156123 BETWEEN t1.c1 AND ')-'))) ::BOOL) HAVING ((LAST_DAY(1630554083))&(AVG((CASE t1.c0 WHEN t1.c2 THEN DATE '1970-01-09' ELSE '' END )))) LIMIT 714775291;" };
        env.setInitialStatementsFromStrings(List.of(queriesStrs));
        env.setBugInducingCondition(statements -> {
            String queriesString = TestEnvironment.getQueriesString(statements);
            return queriesString.contains("AVG");
        });
        env.runReduce();
        List<Query<?>> reducedResult = env.getReducedStatements();
        System.out.println(Arrays.toString(queriesStrs));
        System.out.println(reducedResult);
    }
}
