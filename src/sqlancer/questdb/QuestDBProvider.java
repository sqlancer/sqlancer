package sqlancer.questdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;
import sqlancer.questdb.gen.QuestDBDeleteGenerator;
import sqlancer.questdb.gen.QuestDBIndexGenerator;
import sqlancer.questdb.gen.QuestDBInsertGenerator;
import sqlancer.questdb.gen.QuestDBTableGenerator;

@AutoService(DatabaseProvider.class)
public class QuestDBProvider extends SQLProviderAdapter<QuestDBGlobalState, QuestDBOptions> {
    public QuestDBProvider() {
        super(QuestDBGlobalState.class, QuestDBOptions.class);
    }

    public enum Action implements AbstractAction<QuestDBGlobalState> {
        INSERT(QuestDBInsertGenerator::getQuery), //
        CREATE_INDEX(QuestDBIndexGenerator::getQuery), //
        DELETE(QuestDBDeleteGenerator::generate); //
        // TODO: maybe implement these later
        // UPDATE(QuestDBUpdateGenerator::getQuery), //
        // CREATE_VIEW(QuestDBViewGenerator::generate), //

        private final SQLQueryProvider<QuestDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<QuestDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(QuestDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    public static class QuestDBGlobalState extends SQLGlobalState<QuestDBOptions, QuestDBSchema> {

        @Override
        protected QuestDBSchema readSchema() throws SQLException {
            return QuestDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    @Override
    public void generateDatabase(QuestDBGlobalState globalState) throws Exception {
        // TODO: should follow duckdb or cockrachdb? what's the difference between generate and create db?
    }

    @Override
    public SQLConnection createDatabase(QuestDBGlobalState globalState) throws Exception {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = QuestDBOptions.DEFAULT_HOST;
        }
        if (port == sqlancer.MainOptions.NO_SET_PORT) {
            port = QuestDBOptions.DEFAULT_PORT;
        }
        // TODO(anxing): maybe not hardcode here...
        String databaseName = "qdb";
        String tableName = "test";
        String url = String.format("jdbc:postgresql://%s:%d/%s", host, port, databaseName);
        // use QuestDB default username & password for Postgres JDBC
        Properties properties = new Properties();
        properties.setProperty("user", globalState.getDbmsSpecificOptions().getUserName());
        properties.setProperty("password", globalState.getDbmsSpecificOptions().getPassword());
        properties.setProperty("sslmode", "disable");

        Connection con = DriverManager.getConnection(url, properties);
        // QuestDB cannot create or drop `DATABASE`, can only create or drop `TABLE`
        globalState.getState().logStatement("DROP TABLE IF EXISTS " + tableName + " CASCADE");
        SQLQueryAdapter createTableCommand = new QuestDBTableGenerator().getQuery(globalState);
        globalState.getState().logStatement(createTableCommand);

        try (Statement s = con.createStatement()) {
            s.execute("DROP TABLE IF EXISTS " + tableName);
        }
        try (Statement s = con.createStatement()) {
            s.execute(createTableCommand.getQueryString());
        }
        con.close();
        con = DriverManager.getConnection(url, properties);
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "questdb";
    }

}
