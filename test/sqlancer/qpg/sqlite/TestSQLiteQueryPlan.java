package sqlancer.qpg.sqlite;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import sqlancer.Main;
import sqlancer.MainOptions;
import sqlancer.SQLConnection;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Options;
import sqlancer.sqlite3.SQLite3Provider;

public class TestSQLiteQueryPlan {

    @Test
    void testSQLiteQueryPlan() throws Exception {
        String databaseName = "sqlite";
        SQLite3Provider provider = new SQLite3Provider();
        SQLite3GlobalState state = provider.getGlobalStateClass().getDeclaredConstructor().newInstance();
        SQLite3Options sqlite3Option = provider.getOptionClass().getDeclaredConstructor().newInstance();
        state.setDbmsSpecificOptions(sqlite3Option);
        state.setDatabaseName(databaseName);
        SQLConnection con = provider.createDatabase(state);
        state.setConnection(con);
        MainOptions options = new MainOptions();
        state.setMainOptions(options);
        Main.StateLogger logger = new Main.StateLogger(databaseName, provider, options);
        state.setStateLogger(logger);

        SQLQueryAdapter q = new SQLQueryAdapter("CREATE TABLE t1(a INT, b INT);", true);
        q.execute(state);
        q = new SQLQueryAdapter("CREATE TABLE t2(c INT);", true);
        q.execute(state);
        String queryPlan = provider.getQueryPlan("SELECT * FROM t1 RIGHT JOIN t2 ON a<>0;", state);

        assertEquals("SCAN t1;SCAN t2;RIGHT-JOIN t2;SCAN t2;", queryPlan);
    }

}
