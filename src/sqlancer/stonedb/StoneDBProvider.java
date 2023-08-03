package sqlancer.stonedb;

import static sqlancer.stonedb.StoneDBBugs.bugNotReported2;

import java.sql.Connection;
import java.sql.DriverManager;
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
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.stonedb.gen.StoneDBIndexCreateGenerator;
import sqlancer.stonedb.gen.StoneDBIndexDropGenerator;
import sqlancer.stonedb.gen.StoneDBTableAlterGenerator;
import sqlancer.stonedb.gen.StoneDBTableCreateGenerator;
import sqlancer.stonedb.gen.StoneDBTableDeleteGenerator;
import sqlancer.stonedb.gen.StoneDBTableInsertGenerator;
import sqlancer.stonedb.gen.StoneDBTableUpdateGenerator;

@AutoService(DatabaseProvider.class)
public class StoneDBProvider extends SQLProviderAdapter<StoneDBProvider.StoneDBGlobalState, StoneDBOptions> {

    public StoneDBProvider() {
        super(StoneDBGlobalState.class, StoneDBOptions.class);
    }

    public static class StoneDBGlobalState extends SQLGlobalState<StoneDBOptions, StoneDBSchema> {
        @Override
        protected StoneDBSchema readSchema() throws Exception {
            return StoneDBSchema.fromConnection(getConnection(), getDatabaseName());
        }
    }

    enum Action implements AbstractAction<StoneDBGlobalState> {
        TABLES_SHOW((g) -> new SQLQueryAdapter("SHOW TABLES")), //
        TABLE_ALTER(StoneDBTableAlterGenerator::generate), //
        TABLE_CREATE((g) -> {
            String tableName = DBMSCommon.createTableName(g.getSchema().getDatabaseTables().size());
            return StoneDBTableCreateGenerator.generate(g, tableName);
        }), //
        TABLE_DELETE(StoneDBTableDeleteGenerator::generate), //
        TABLE_UPDATE(StoneDBTableUpdateGenerator::generate), //
        INDEX_CREATE(StoneDBIndexCreateGenerator::generate), //
        INDEX_DROP(StoneDBIndexDropGenerator::generate), //
        TABLE_INSERT(StoneDBTableInsertGenerator::generate); //

        private final SQLQueryProvider<StoneDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<StoneDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public Query<?> getQuery(StoneDBGlobalState globalState) throws Exception {
            return sqlQueryProvider.getQuery(globalState);
        }
    }

    private static int mapActions(StoneDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case TABLES_SHOW:
            return r.getInteger(0, 1);
        case TABLE_ALTER:
            return r.getInteger(0, 5);
        case TABLE_CREATE:
            return r.getInteger(0, 1);
        case TABLE_DELETE:
            if (bugNotReported2) {
                return 0;
            }
            return r.getInteger(0, 10);
        case TABLE_INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case TABLE_UPDATE:
            return r.getInteger(0, 1);
        case INDEX_CREATE:
            return r.getInteger(0, 1);
        case INDEX_DROP:
            return r.getInteger(0, 1);
        default:
            throw new AssertionError(a);
        }
    }

    @Override
    public void generateDatabase(StoneDBGlobalState globalState) throws Exception {
        while (globalState.getSchema().getDatabaseTables().size() < Randomly.smallNumber() + 1) {
            String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
            SQLQueryAdapter createTable = StoneDBTableCreateGenerator.generate(globalState, tableName);
            globalState.executeStatement(createTable);
        }
        StatementExecutor<StoneDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                StoneDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(StoneDBGlobalState globalState) throws Exception {
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = StoneDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = StoneDBOptions.DEFAULT_PORT;
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
            s.execute("CREATE DATABASE " + databaseName);
            s.execute("USE " + databaseName);
        }
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "stonedb";
    }
}
