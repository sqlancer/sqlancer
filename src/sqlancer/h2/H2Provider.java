package sqlancer.h2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.AbstractAction;
import sqlancer.ComparatorHelper;
import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.ProviderAdapter;
import sqlancer.Randomly;
import sqlancer.StatementExecutor;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.QueryAdapter;
import sqlancer.common.query.QueryProvider;
import sqlancer.h2.H2Provider.H2GlobalState;
import sqlancer.h2.H2Schema.H2Column;
import sqlancer.h2.H2Schema.H2Tables;

public class H2Provider extends ProviderAdapter<H2GlobalState, H2Options> {

    public H2Provider() {
        super(H2GlobalState.class, H2Options.class);
    }

    public enum Action implements AbstractAction<H2GlobalState> {

        INSERT(H2InsertGenerator::getQuery), //
        INDEX(H2IndexGenerator::getQuery), //
        ANALYZE((g) -> new QueryAdapter("ANALYZE"));

        private final QueryProvider<H2GlobalState> queryProvider;

        Action(QueryProvider<H2GlobalState> queryProvider) {
            this.queryProvider = queryProvider;
        }

        @Override
        public Query getQuery(H2GlobalState state) throws SQLException {
            return queryProvider.getQuery(state);
        }
    }

    private static int mapActions(H2GlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case ANALYZE:
            return r.getInteger(0, 5);
        case INDEX:
            return r.getInteger(0, 5);
        default:
            throw new AssertionError(a);
        }
    }

    public static class H2GlobalState extends GlobalState<H2Options, H2Schema> {

        @Override
        protected void updateSchema() throws SQLException {
            setSchema(H2Schema.fromConnection(getConnection(), getDatabaseName()));
        }

    }

    @Override
    public void generateDatabase(H2GlobalState globalState) throws SQLException {
        boolean success = false;
        for (int i = 0; i < Randomly.fromOptions(1, 2, 3); i++) {
            do {
                Query qt = new H2TableGenerator().getQuery(globalState);
                success = globalState.executeStatement(qt);
            } while (!success);
        }
        StatementExecutor<H2GlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                H2Provider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    public Connection createDatabase(H2GlobalState globalState) throws SQLException {
        String connectionString = "jdbc:h2:~/" + globalState.getDatabaseName() + ";DB_CLOSE_ON_EXIT=FALSE";
        Connection connection = DriverManager.getConnection(connectionString, "sa", "");
        connection.createStatement().execute("DROP ALL OBJECTS DELETE FILES");
        connection.close();
        connection = DriverManager.getConnection(connectionString, "sa", "");
        return connection;
    }

    @Override
    public String getDBMSName() {
        return "h2";
    }

    public static class H2TLPWhereOracle implements TestOracle {

        private final H2GlobalState globalState;

        public H2TLPWhereOracle(H2GlobalState globalState) {
            this.globalState = globalState;
        }

        @Override
        public void check() throws SQLException {
            H2Tables tables = globalState.getSchema().getRandomTableNonEmptyTables();
            String tablesString = tables.tableNamesAsString();
            List<H2Column> columns = tables.getColumns();
            String predicate = H2ToStringVisitor
                    .asString(new H2ExpressionGenerator(globalState).setColumns(columns).generateExpression());
            String original = "SELECT * FROM " + tablesString;
            ExpectedErrors errors = new ExpectedErrors();
            H2Errors.addExpressionErrors(errors);
            List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(original, errors, globalState);

            String nonNegated = "SELECT * FROM " + tablesString + " WHERE " + predicate;
            String negated = "SELECT * FROM " + tablesString + " WHERE NOT " + predicate;
            String isNull = "SELECT * FROM " + tablesString + " WHERE " + predicate + " IS NULL";
            List<String> combinedString = new ArrayList<>();

            List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(nonNegated, negated, isNull,
                    combinedString, true, globalState, errors);
            ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, original, combinedString,
                    globalState);
        }

    }

}
