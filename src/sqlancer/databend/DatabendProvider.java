package sqlancer.databend;

import java.io.File;
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
import sqlancer.databend.gen.DatabendInsertGenerator;
import sqlancer.databend.gen.DatabendRandomQuerySynthesizer;
import sqlancer.databend.gen.DatabendTableGenerator;

@AutoService(DatabaseProvider.class)
public class DatabendProvider extends SQLProviderAdapter<DatabendGlobalState, DatabendOptions> {

    public DatabendProvider() {
        super(DatabendGlobalState.class, DatabendOptions.class);
    }

    public enum Action implements AbstractAction<DatabendGlobalState> {

        INSERT(DatabendInsertGenerator::getQuery), //
        // TODO 等待databend实现update && delete
        // DELETE(DatabendDeleteGenerator::generate), //
        // UPDATE(DatabendUpdateGenerator::getQuery), //

        // CREATE_VIEW(DatabendViewGenerator::generate), //TODO 等待databend的create view语法 更加贴近mysql
        EXPLAIN((g) -> {
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
        // case DELETE:
        // return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumDeletes + 1);
        // case CREATE_VIEW:
        // return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumViews + 1);
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
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
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
        se.executeStatements(); // 在已有的表格中插入数据，原先是增删改一些数据，除了insert和explan我都去掉了
    }

    public void tryDeleteFile(String fname) {
        try {
            File f = new File(fname);
            f.delete();
        } catch (Exception e) {
        }
    }

    public void tryDeleteDatabase(String dbpath) {
        if (dbpath.equals("") || dbpath.equals(":memory:")) {
            return;
        }
        tryDeleteFile(dbpath);
        tryDeleteFile(dbpath + ".wal");
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
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + databaseName);
            globalState.getState().logStatement("CREATE DATABASE " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + databaseName);
            globalState.getState().logStatement("USE " + databaseName);
        }

        // try (Statement s = con.createStatement()) {
        // s.execute("set enable_planner_v2 = 0;");
        // globalState.getState().logStatement("set enable_planner_v2 = 0;");
        // }

        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "databend"; // 用于DatabendOptions
    }

}
