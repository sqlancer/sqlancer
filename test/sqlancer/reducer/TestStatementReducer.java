package sqlancer.reducer;

import org.junit.jupiter.api.Test;
import sqlancer.common.query.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestStatementReducer {

    @Test
    void testSimple() throws Exception {
        TestEnvironment env = new TestEnvironment();

        String[] queriesStr = { "CREATE TABLE FAKE_TABLE;", "SELECT * FROM FAKE_TABLE;", "EXIT", };
        env.setInitialStatementsFromStrings(List.of(queriesStr));
        env.setBugInducingCondition(statements -> {
            String queriesString = TestEnvironment.getQueriesString(statements);
            return queriesString.contains("SELECT");
        });
        env.runReduce();
        List<Query<?>> reducedResult = env.getReducedStatements();
        assertEquals(1, reducedResult.size());
        assertEquals("SELECT * FROM FAKE_TABLE;", reducedResult.get(0).toString());

    }

    @Test
    void testDeltaDebugging() throws Exception {
        TestEnvironment env = new TestEnvironment();
        List<String> fakeStatements = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            String statement = "Statement_" + i + ";";
            fakeStatements.add(statement);
        }

        env.setInitialStatementsFromStrings(fakeStatements);
        env.setBugInducingCondition(statements -> {
            String queries = TestEnvironment.getQueriesString(statements);
            return queries.contains("Statement_29;");
        });

        env.runReduce();
        List<Query<?>> reducedQueries = env.getReducedStatements();
        String queriesString = TestEnvironment.getQueriesString(reducedQueries);
        assertEquals(queriesString, "Statement_29;");
    }

    @Test
    void testDeltaDebuggingWithStatementsCombination() throws Exception {
        TestEnvironment env = new TestEnvironment();
        List<String> fakeStatements = new ArrayList<>();

        String pattern = "(.*\\n)*(Statement_2;)\\n(.*\\n)*(Statement_318);\\n(.*\\n)*(Statement_990;)(.*\\n)*.*";
        for (int i = 0; i < 1000; i++) {
            String statement = "Statement_" + i + ";";
            fakeStatements.add(statement);
        }

        env.setInitialStatementsFromStrings(fakeStatements);
        env.setBugInducingCondition(queryList -> {
            String queries = TestEnvironment.getQueriesString(queryList);
            return Pattern.matches(pattern, queries);
        });

        env.runReduce();
        List<Query<?>> reducedQueries = env.getReducedStatements();
        String queriesString = TestEnvironment.getQueriesString(reducedQueries);
        assertEquals(queriesString, "Statement_2;\nStatement_318;\nStatement_990;");
    }

}
