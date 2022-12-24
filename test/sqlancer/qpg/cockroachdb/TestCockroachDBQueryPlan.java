package sqlancer.qpg.cockroachdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;
import sqlancer.MainOptions;
import sqlancer.SQLConnection;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.cockroachdb.CockroachDBOptions;
import sqlancer.cockroachdb.CockroachDBProvider;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;

public class TestCockroachDBQueryPlan {

    @Test
    void testCockroachDBQueryPlan() throws Exception {
        String cockroachDB = System.getenv("COCKROACHDB_AVAILABLE");
        boolean cockroachDBIsAvailable = cockroachDB != null && cockroachDB.equalsIgnoreCase("true");
        assumeTrue(cockroachDBIsAvailable);

        String databaseName = "cockroachdb";
        CockroachDBProvider provider = new CockroachDBProvider();
        CockroachDBGlobalState state = provider.getGlobalStateClass().getDeclaredConstructor().newInstance();
        CockroachDBOptions cockroachdbOption = provider.getOptionClass().getDeclaredConstructor().newInstance();
        state.setDbmsSpecificOptions(cockroachdbOption);
        state.setDatabaseName(databaseName);
        MainOptions options = new MainOptions();
        state.setMainOptions(options);
        state.setState(provider.getStateToReproduce(databaseName));
        SQLConnection con = provider.createDatabase(state);
        state.setConnection(con);
        Main.StateLogger logger = new Main.StateLogger(databaseName, provider, options);
        state.setStateLogger(logger);

        SQLQueryAdapter q = new SQLQueryAdapter("CREATE TABLE t1(a INT, b INT);", true);
        q.execute(state);
        q = new SQLQueryAdapter("CREATE TABLE t2(c INT);", true);
        q.execute(state);
        String queryPlan = provider.getQueryPlan("SELECT * FROM t1 RIGHT JOIN t2 ON a<>0;", state);

        assertEquals("left-join (cross);scan t2;select;scan t1;filters;filters (true);", queryPlan);
    }

}
