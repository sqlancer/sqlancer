package sqlancer.doris;

import com.google.auto.service.AutoService;
import sqlancer.*;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.gen.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@AutoService(DatabaseProvider.class)
public class DorisProvider extends SQLProviderAdapter<DorisGlobalState, DorisOptions> {

    public DorisProvider() {
        super(DorisGlobalState.class, DorisOptions.class);
    }

    public enum Action implements AbstractAction<DorisGlobalState> {
        INSERT(DorisInsertGenerator::getQuery), //
        DELETE(DorisDeleteGenerator::generate), //
        UPDATE(DorisUpdateGenerator::getQuery), //
        REHASH((g) -> new SQLQueryAdapter("REHASH;"));

        private final SQLQueryProvider<DorisGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<DorisGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(DorisGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapActions(DorisGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case DELETE:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumDeletes + 1);
        case UPDATE:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumUpdates + 1);
        case REHASH:
            return r.getInteger(0, 2);
        default:
            throw new AssertionError(a);
        }
    }

    public static class DorisGlobalState extends SQLGlobalState<DorisOptions, DorisSchema> {

        @Override
        protected DorisSchema readSchema() throws SQLException {
            return DorisSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    @Override
    public void generateDatabase(DorisGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success = false;
            do {
                SQLQueryAdapter qt = new DorisTableGenerator().getQuery(globalState);
                if (qt != null) success = globalState.executeStatement(qt);
            } while (!success);
        }
        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException(); // TODO
        }
        StatementExecutor<DorisGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                DorisProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(DorisGlobalState globalState) throws SQLException {
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        if (password.equals("\"\"")) password = "";
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = DorisOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = DorisOptions.DEFAULT_PORT;
        }
        String databaseName = globalState.getDatabaseName();
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
        globalState.getState().logStatement("CREATE DATABASE " + databaseName);
        globalState.getState().logStatement("USE " + databaseName);
        String url = String.format("jdbc:mysql://%s:%d?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true",
                host, port);
        Connection con = DriverManager.getConnection(url, username, password);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + databaseName);
        }
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "doris";
    }

}
