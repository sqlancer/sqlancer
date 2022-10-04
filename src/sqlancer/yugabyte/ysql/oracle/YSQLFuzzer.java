package sqlancer.yugabyte.ysql.oracle;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLProvider;
import sqlancer.yugabyte.ysql.YSQLVisitor;
import sqlancer.yugabyte.ysql.gen.YSQLCommon;
import sqlancer.yugabyte.ysql.gen.YSQLRandomQueryGenerator;

public class YSQLFuzzer implements TestOracle {
    private final YSQLGlobalState globalState;
    private final List<Query> testQueries;
    private final ExpectedErrors errors = new ExpectedErrors();

    public YSQLFuzzer(YSQLGlobalState globalState) {
        this.globalState = globalState;

        YSQLCommon.addCommonExpressionErrors(errors);
        YSQLCommon.addCommonFetchErrors(errors);
        YSQLCommon.addGroupingErrors(errors);
        YSQLCommon.addViewErrors(errors);

        // remove timeout error from scope
        errors.add("canceling statement due to statement timeout");

        // exclude nemesis exceptions
        errors.add("terminating connection due to administrator command");
        errors.add("Java heap space");
        errors.add("Connection refused");
        errors.add("Connection to");

        // get config from -Dconfig.file="path/to/fuzzer.conf"
        testQueries = new ArrayList<>();
        try {
            Config config = ConfigFactory.load();
            ArrayList<Object> queriesList = (ArrayList<Object>) config.getList("queries").unwrapped();
            for (Object configValue : queriesList) {
                String type = ((String) ((Map<?, ?>) configValue).get("type")).toUpperCase(Locale.ROOT);
                Integer weight = (Integer) ((Map<?, ?>) configValue).get("weight");

                Query query = type.equalsIgnoreCase("SELECT") ? new SelectQuery()
                        : new ActionQuery(YSQLProvider.Action.valueOf(type));

                for (int i = 0; i < weight; i++) {
                    testQueries.add(query);
                }
            }
        } catch (Exception e) {
            // do nothing
        } finally {
            if (testQueries.isEmpty()) {
                System.out.println("No configuration found. Using just random select statements");
                testQueries.add(new SelectQuery());
                testQueries.add(new ActionQuery(YSQLProvider.Action.UPDATE));
                testQueries.add(new ActionQuery(YSQLProvider.Action.DELETE));
                testQueries.add(new ActionQuery(YSQLProvider.Action.INSERT));
            }
        }
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
