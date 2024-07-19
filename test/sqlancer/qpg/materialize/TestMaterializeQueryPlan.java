package sqlancer.qpg.materialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;
import sqlancer.MainOptions;
import sqlancer.SQLConnection;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.materialize.MaterializeOptions;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeProvider;

public class TestMaterializeQueryPlan {

    @Test
    void testMaterializeQueryPlan() throws Exception {
        String materialize = System.getenv("MATERIALIZE_AVAILABLE");
        boolean materializeIsAvailable = materialize != null && materialize.equalsIgnoreCase("true");
        assumeTrue(materializeIsAvailable);

        String databaseName = "queryplan";
        MaterializeProvider provider = new MaterializeProvider();
        MaterializeGlobalState state = provider.getGlobalStateClass().getDeclaredConstructor().newInstance();
        MaterializeOptions materializeOption = provider.getOptionClass().getDeclaredConstructor().newInstance();
        state.setDbmsSpecificOptions(materializeOption);
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

        assertEquals(
                "Return // { arity: 3 };Union // { arity: 3 };Get l0 // { arity: 3 };Project (#2{c}, #3, #0) // { arity: 3 };Union // { arity: 1 };Negate // { arity: 1 };Project (#2) // { arity: 1 };ReadStorage queryplan.public.t2 // { arity: 1 };ReadStorage queryplan.public.t2 // { arity: 1 };With;ReadStorage queryplan.public.t1 // { arity: 2 };ReadStorage queryplan.public.t2 // { arity: 1 };;Source queryplan.public.t1;Source queryplan.public.t2;;Target cluster: quickstart;",
                queryPlan);
    }

}
