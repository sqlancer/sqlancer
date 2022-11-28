package sqlancer.yugabyte.ysql.oracle;

import static sqlancer.yugabyte.ysql.YSQLProvider.DDL_LOCK;

import java.util.Arrays;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.MainOptions;
import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLProvider;
import sqlancer.yugabyte.ysql.gen.YSQLTableGenerator;

public class YSQLCatalog implements TestOracle<YSQLGlobalState> {
    protected final YSQLGlobalState state;

    protected final ExpectedErrors errors = new ExpectedErrors();
    protected final Main.StateLogger logger;
    protected final MainOptions options;
    protected final SQLConnection con;

    private final List<YSQLProvider.Action> dmlActions = Arrays.asList(YSQLProvider.Action.INSERT,
            YSQLProvider.Action.UPDATE, YSQLProvider.Action.DELETE);
    private final List<YSQLProvider.Action> catalogActions = Arrays.asList(YSQLProvider.Action.CREATE_VIEW,
            YSQLProvider.Action.CREATE_SEQUENCE, YSQLProvider.Action.ALTER_TABLE, YSQLProvider.Action.SET_CONSTRAINTS,
            YSQLProvider.Action.DISCARD, YSQLProvider.Action.DROP_INDEX, YSQLProvider.Action.COMMENT_ON,
            YSQLProvider.Action.RESET_ROLE, YSQLProvider.Action.RESET);
    private final List<YSQLProvider.Action> diskActions = Arrays.asList(YSQLProvider.Action.TRUNCATE,
            YSQLProvider.Action.VACUUM);

    public YSQLCatalog(YSQLGlobalState globalState) {
        this.state = globalState;
        this.con = state.getConnection();
        this.logger = state.getLogger();
        this.options = state.getOptions();
        YSQLErrors.addCommonExpressionErrors(errors);
        YSQLErrors.addCommonFetchErrors(errors);
    }

    private YSQLProvider.Action getRandomAction(List<YSQLProvider.Action> actions) {
        return actions.get(state.getRandomly().getInteger(0, actions.size()));
    }

    protected void createTables(YSQLGlobalState globalState, int numTables) throws Exception {
        synchronized (DDL_LOCK) {
            while (globalState.getSchema().getDatabaseTables().size() < numTables) {
                // TODO concurrent DDLs may produce a lot of noise in test logs so its disabled right now
                // added timeout to avoid possible catalog collisions
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new AssertionError();
                }

                try {
                    String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
                    SQLQueryAdapter createTable = YSQLTableGenerator.generate(tableName, true, globalState);
                    globalState.executeStatement(createTable);
                    globalState.getManager().incrementSelectQueryCount();
                    globalState.executeStatement(new SQLQueryAdapter("COMMIT", true));
                } catch (IgnoreMeException e) {
                    // do nothing
                }
            }
        }
    }

    @Override
    public void check() throws Exception {
        // create table or evaluate catalog test
        int seed = state.getRandomly().getInteger(1, 100);
        if (seed > 95) {
            createTables(state, 1);
        } else {
            YSQLProvider.Action randomAction;

            if (seed > 40) {
                randomAction = getRandomAction(dmlActions);
            } else if (seed > 10) {
                randomAction = getRandomAction(catalogActions);
            } else {
                randomAction = getRandomAction(diskActions);
            }

            state.executeStatement(randomAction.getQuery(state));
        }
        state.getManager().incrementSelectQueryCount();
    }
}
