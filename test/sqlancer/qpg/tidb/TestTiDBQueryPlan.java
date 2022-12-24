package sqlancer.qpg.tidb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;
import sqlancer.MainOptions;
import sqlancer.SQLConnection;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBOptions;
import sqlancer.tidb.TiDBProvider;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;

public class TestTiDBQueryPlan {

    @Test
    void testTiDBQueryPlan() throws Exception {
        String tiDB = System.getenv("TIDB_AVAILABLE");
        boolean tiDBIsAvailable = tiDB != null && tiDB.equalsIgnoreCase("true");
        assumeTrue(tiDBIsAvailable);

        String databaseName = "tidb";
        TiDBProvider provider = new TiDBProvider();
        TiDBGlobalState state = provider.getGlobalStateClass().getDeclaredConstructor().newInstance();
        TiDBOptions TiDBOption = provider.getOptionClass().getDeclaredConstructor().newInstance();
        state.setDbmsSpecificOptions(TiDBOption);
        state.setDatabaseName(databaseName);
        MainOptions options = new MainOptions();
        state.setMainOptions(options);
        Main.StateLogger logger = new Main.StateLogger(databaseName, provider, options);
        state.setStateLogger(logger);
        state.setState(provider.getStateToReproduce(databaseName));
        SQLConnection con = provider.createDatabase(state);
        state.setConnection(con);

        SQLQueryAdapter q = new SQLQueryAdapter("CREATE TABLE t1(a INT, b INT);", true);
        q.execute(state);
        q = new SQLQueryAdapter("CREATE TABLE t2(c INT);", true);
        q.execute(state);
        String queryPlan = provider.getQueryPlan("SELECT * FROM t1 RIGHT JOIN t2 ON a<>0;", state);

        assertEquals(
                "HashJoin_7;TableReader_10(Build);Selection_9;TableFullScan_8;TableReader_12(Probe);TableFullScan_11;",
                queryPlan);
    }

}
