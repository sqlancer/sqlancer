package sqlancer.yugabyte.ysql.oracle;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLProvider;
import sqlancer.yugabyte.ysql.YSQLVisitor;
import sqlancer.yugabyte.ysql.gen.YSQLRandomQueryGenerator;

public class YSQLFuzzer implements TestOracle<YSQLGlobalState> {
    private final YSQLGlobalState globalState;
    private final List<Query> testQueries;
    private final ExpectedErrors errors = new ExpectedErrors();

    public YSQLFuzzer(YSQLGlobalState globalState) {
        this.globalState = globalState;

        YSQLErrors.addCommonExpressionErrors(errors);
        YSQLErrors.addCommonFetchErrors(errors);
        YSQLErrors.addGroupingErrors(errors);
        YSQLErrors.addViewErrors(errors);

        // remove timeout error from scope
        errors.add("canceling statement due to statement timeout");

        // exclude nemesis exceptions
        errors.add("terminating connection due to administrator command");
        errors.add("Java heap space");
        errors.add("Connection refused");
        errors.add("Connection to");

        testQueries = new ArrayList<>();

        testQueries.add(new SelectQuery());
        testQueries.add(new ActionQuery(YSQLProvider.Action.UPDATE));
        testQueries.add(new ActionQuery(YSQLProvider.Action.DELETE));
        testQueries.add(new ActionQuery(YSQLProvider.Action.INSERT));
    }

    @Override
    public void check() throws Exception {
        Query s = testQueries.get(globalState.getRandomly().getInteger(0, testQueries.size()));
        globalState.executeStatement(s.getQuery(globalState, errors));
        globalState.getManager().incrementSelectQueryCount();
    }

    private static class Query {
        public SQLQueryAdapter getQuery(YSQLGlobalState state, ExpectedErrors errors) throws Exception {
            throw new IllegalAccessException("Should be implemented");
        };
    }

    private static class ActionQuery extends Query {
        private final YSQLProvider.Action action;

        ActionQuery(YSQLProvider.Action action) {
            this.action = action;
        }

        @Override
        public SQLQueryAdapter getQuery(YSQLGlobalState state, ExpectedErrors errors) throws Exception {
            return action.getQuery(state);
        }
    }

    private static class SelectQuery extends Query {

        @Override
        public SQLQueryAdapter getQuery(YSQLGlobalState state, ExpectedErrors errors) throws Exception {
            return new SQLQueryAdapter(
                    YSQLVisitor.asString(YSQLRandomQueryGenerator.createRandomQuery(Randomly.smallNumber() + 1, state))
                            + ";",
                    errors);
        }
    }
}
