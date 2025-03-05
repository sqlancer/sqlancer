package sqlancer.qpg.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

import sqlancer.Main;
import sqlancer.MainOptions;
import sqlancer.SQLConnection;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresOptions;
import sqlancer.postgres.PostgresProvider;

public class TestPostgresQueryPlan {


    @Test
    void testPostgresQueryPlan() throws Exception {
        String databaseName = "postgres";
        PostgresProvider provider = new PostgresProvider();
        PostgresGlobalState state = provider.getGlobalStateClass().getDeclaredConstructor().newInstance();
        PostgresOptions postgresOption = provider.getOptionClass().getDeclaredConstructor().newInstance();
        state.setDbmsSpecificOptions(postgresOption);
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
        assertEquals("NestedLoopLeftJoin;->SeqScanont2;->Materialize;->SeqScanont1;Filter:;", queryPlan);
    }

}
