package sqlancer.duckdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import sqlancer.AbstractAction;
import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.QueryAdapter;
import sqlancer.common.query.QueryProvider;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.gen.DuckDBDeleteGenerator;
import sqlancer.duckdb.gen.DuckDBIndexGenerator;
import sqlancer.duckdb.gen.DuckDBInsertGenerator;
import sqlancer.duckdb.gen.DuckDBRandomQuerySynthesizer;
import sqlancer.duckdb.gen.DuckDBTableGenerator;
import sqlancer.duckdb.gen.DuckDBUpdateGenerator;
import sqlancer.duckdb.gen.DuckDBViewGenerator;

public class DuckDBProvider extends SQLProviderAdapter<DuckDBGlobalState, DuckDBOptions> {

    public DuckDBProvider() {
        super(DuckDBGlobalState.class, DuckDBOptions.class);
    }

    public enum Action implements AbstractAction<DuckDBGlobalState> {

        INSERT(DuckDBInsertGenerator::getQuery), //
        CREATE_INDEX(DuckDBIndexGenerator::getQuery), //
        VACUUM((g) -> new QueryAdapter("VACUUM;")), //
        ANALYZE((g) -> new QueryAdapter("ANALYZE;")), //
        DELETE(DuckDBDeleteGenerator::generate), //
        UPDATE(DuckDBUpdateGenerator::getQuery), //
        CREATE_VIEW(DuckDBViewGenerator::generate), //
        EXPLAIN((g) -> {
            ExpectedErrors errors = new ExpectedErrors();
            DuckDBErrors.addExpressionErrors(errors);
            DuckDBErrors.addGroupByErrors(errors);
            return new QueryAdapter(
                    "EXPLAIN " + DuckDBToStringVisitor
                            .asString(DuckDBRandomQuerySynthesizer.generateSelect(g, Randomly.smallNumber() + 1)),
                    errors);
        });

        private final QueryProvider<DuckDBGlobalState> queryProvider;

        Action(QueryProvider<DuckDBGlobalState> queryProvider) {
            this.queryProvider = queryProvider;
        }

        @Override
        public Query getQuery(DuckDBGlobalState state) throws SQLException {
            return queryProvider.getQuery(state);
        }
    }

    private static int mapActions(DuckDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case CREATE_INDEX:
            if (!globalState.getDmbsSpecificOptions().testIndexes) {
                return 0;
            }
            // fall through
        case UPDATE:
            return r.getInteger(0, globalState.getDmbsSpecificOptions().maxNumUpdates + 1);
        case VACUUM: // seems to be ignored
        case ANALYZE: // seems to be ignored
        case EXPLAIN:
            return r.getInteger(0, 2);
        case DELETE:
            return r.getInteger(0, globalState.getDmbsSpecificOptions().maxNumDeletes + 1);
        case CREATE_VIEW:
            return r.getInteger(0, globalState.getDmbsSpecificOptions().maxNumViews + 1);
        default:
            throw new AssertionError(a);
        }
    }

    public static class DuckDBGlobalState extends GlobalState<DuckDBOptions, DuckDBSchema> {

        @Override
        protected DuckDBSchema readSchema() throws SQLException {
            return DuckDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    @Override
    public void generateDatabase(DuckDBGlobalState globalState) throws SQLException {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success = false;
            do {
                Query qt = new DuckDBTableGenerator().getQuery(globalState);
                success = globalState.executeStatement(qt);
            } while (!success);
        }
        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException(); // TODO
        }
        StatementExecutor<DuckDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                DuckDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    public Connection createDatabase(DuckDBGlobalState globalState) throws SQLException {
        String url = "jdbc:duckdb:";
        return DriverManager.getConnection(url, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword());
    }

    @Override
    public String getDBMSName() {
        return "duckdb";
    }

}
