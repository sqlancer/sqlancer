package sqlancer.databend;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.gen.DatabendDeleteGenerator;
import sqlancer.databend.gen.DatabendInsertGenerator;
import sqlancer.databend.gen.DatabendRandomQuerySynthesizer;
import sqlancer.databend.gen.DatabendTableGenerator;
import sqlancer.databend.gen.DatabendViewGenerator;

@AutoService(DatabaseProvider.class)
public class DatabendProvider extends SQLProviderAdapter<DatabendGlobalState, DatabendOptions> {

    public DatabendProvider() {
        super(DatabendGlobalState.class, DatabendOptions.class);
    }

    public enum Action implements AbstractAction<DatabendGlobalState> {

        INSERT(DatabendInsertGenerator::getQuery), DELETE(DatabendDeleteGenerator::generate),
        // TODO 等待databend实现update
        // UPDATE(DatabendUpdateGenerator::getQuery), //
        CREATE_VIEW(DatabendViewGenerator::generate), EXPLAIN((g) -> {
            ExpectedErrors errors = new ExpectedErrors();
            DatabendErrors.addExpressionErrors(errors);
            DatabendErrors.addGroupByErrors(errors);
            return new SQLQueryAdapter(
                    "EXPLAIN " + DatabendToStringVisitor
                            .asString(DatabendRandomQuerySynthesizer.generateSelect(g, Randomly.smallNumber() + 1)),
                    errors);
        });

        private final SQLQueryProvider<DatabendGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<DatabendGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(DatabendGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapActions(DatabendGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case EXPLAIN:
            return r.getInteger(0, 2);
        // TODO 等待databend实现update && delete
        // case UPDATE:
        // return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumUpdates + 1);
        case DELETE:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumDeletes + 1);
        case CREATE_VIEW:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumViews + 1);
        default:
            throw new AssertionError(a);
        }
    }

    public static class DatabendGlobalState extends SQLGlobalState<DatabendOptions, DatabendSchema> {

        @Override
        protected DatabendSchema readSchema() throws SQLException {
            return DatabendSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    @Override
    public void generateDatabase(DatabendGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(3, 4); i++) {
            boolean success;
            do {
                SQLQueryAdapter qt = new DatabendTableGenerator().getQuery(globalState);
                success = globalState.executeStatement(qt);
            } while (!success);
        }
        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException(); // TODO
        }
        StatementExecutor<DatabendGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                DatabendProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements(); // 增删改一些数据（按权重随机选取算法）
    }

    @Override
    public SQLConnection createDatabase(DatabendGlobalState globalState) throws SQLException {
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = DatabendOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = DatabendOptions.DEFAULT_PORT;
        }
        String databaseName = globalState.getDatabaseName();
        String url = String.format("jdbc:mysql://%s:%d?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true",
                host, port);
        Connection con = DriverManager.getConnection(url, username, password);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
            globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
            s.execute("CREATE DATABASE " + databaseName);
            globalState.getState().logStatement("CREATE DATABASE " + databaseName);
            s.execute("USE " + databaseName);
            globalState.getState().logStatement("USE " + databaseName);
        }

        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "databend"; // 用于DatabendOptions
    }

}
