package sqlancer.yugabyte.ycql.test;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ycql.YCQLProvider;
import sqlancer.yugabyte.ycql.YCQLToStringVisitor;
import sqlancer.yugabyte.ycql.gen.YCQLRandomQuerySynthesizer;

public class YCQLFuzzer implements TestOracle<YCQLProvider.YCQLGlobalState> {
    private final YCQLProvider.YCQLGlobalState globalState;
    private final List<Query> testQueries;
    private final ExpectedErrors errors = new ExpectedErrors();

    public YCQLFuzzer(YCQLProvider.YCQLGlobalState globalState) {
        this.globalState = globalState;

        errors.add("Query timed out after PT2S");
        errors.add("Datatype Mismatch");
        errors.add("Invalid CQL Statement");
        errors.add("Invalid SQL Statement");
        errors.add("Invalid Arguments");
        errors.add("Invalid Function Call");

        testQueries = new ArrayList<>();

        testQueries.add(new SelectQuery());
        testQueries.add(new ActionQuery(YCQLProvider.Action.UPDATE));
        testQueries.add(new ActionQuery(YCQLProvider.Action.DELETE));
        testQueries.add(new ActionQuery(YCQLProvider.Action.INSERT));
    }

    @Override
    public void check() throws Exception {
        Query s = testQueries.get(globalState.getRandomly().getInteger(0, testQueries.size()));
        globalState.executeStatement(s.getQuery(globalState, errors));
        globalState.getManager().incrementSelectQueryCount();
    }

    private static class Query {
        public SQLQueryAdapter getQuery(YCQLProvider.YCQLGlobalState state, ExpectedErrors errors) throws Exception {
            throw new IllegalAccessException("Should be implemented");
        };
    }

    private static class ActionQuery extends Query {
        private final YCQLProvider.Action action;

        ActionQuery(YCQLProvider.Action action) {
            this.action = action;
        }

        @Override
        public SQLQueryAdapter getQuery(YCQLProvider.YCQLGlobalState state, ExpectedErrors errors) throws Exception {
            return action.getQuery(state);
        }
    }

    private static class SelectQuery extends Query {

        @Override
        public SQLQueryAdapter getQuery(YCQLProvider.YCQLGlobalState state, ExpectedErrors errors) throws Exception {
            return new SQLQueryAdapter(
                    YCQLToStringVisitor.asString(
                            YCQLRandomQuerySynthesizer.generateSelect(state, Randomly.smallNumber() + 1)) + ";",
                    errors);
        }
    }
}
