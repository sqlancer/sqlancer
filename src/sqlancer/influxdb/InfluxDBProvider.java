package sqlancer.influxdb;

import com.google.auto.service.AutoService;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import sqlancer.*;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.influxdb.gen.InfluxDBCreateDatabaseGenerator;
import sqlancer.influxdb.gen.InfluxDBWritePointGenerator;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

@AutoService(DatabaseProvider.class)
public class InfluxDBProvider extends SQLProviderAdapter<InfluxDBProvider.InfluxDBGlobalState, InfluxDBOptions> {

    public InfluxDBProvider() {
        super(InfluxDBGlobalState.class, InfluxDBOptions.class);
    }

    public enum Action implements AbstractAction<InfluxDBGlobalState> {
        WRITE_POINT(sqlancer.influxdb.gen.InfluxDBInsertGenerator::getQuery),
        CREATE_DATABASE(sqlancer.influxdb.gen.InfluxDBInsertGenerator::getQuery),
        InfluxDBWritePointGenerator();

        private final SQLQueryProvider<InfluxDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<InfluxDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(InfluxDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapActions(InfluxDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
            case WRITE_POINT:
                return r.getInteger(0, globalState.getOptions().getMaxNumberWrites());
            case CREATE_DATABASE:
                return r.getInteger(0, 2);
            default:
                throw new AssertionError("Unknown action: " + a);
        }
    }

    public static class InfluxDBGlobalState extends SQLGlobalState<InfluxDBOptions, InfluxDBSchema> {
        @Override
        protected InfluxDBSchema readSchema() throws SQLException {
            return InfluxDBSchema.fromConnection(getConnection(), getDatabaseName());
        }
    }

    @Override
    public void generateDatabase(InfluxDBGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success;
            do {
                SQLQueryAdapter qt = new sqlancer.influxdb.gen.InfluxDBInsertGenerator().getQuery(globalState, null);
                success = globalState.executeStatement(qt);
            } while (!success);
        }
        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException();
        }
        StatementExecutor<InfluxDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                InfluxDBProvider::mapActions, (q) -> {
            if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                throw new IgnoreMeException();
            }
        });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(InfluxDBGlobalState globalState) throws Exception {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = InfluxDBOptions.DEFAULT_HOST;
        }
        if (port == sqlancer.MainOptions.NO_SET_PORT) {
            port = InfluxDBOptions.DEFAULT_PORT;
        }

        String databaseName = "influxdb_sqlancer_test";
        String url = String.format("http://%s:%d", host, port);

        // Create InfluxDB Connection
        InfluxDB influxDB = InfluxDBFactory.connect(url);
        globalState.setConnection(new SQLConnection(influxDB));

        // Create database if it does not exist
        String query = String.format("CREATE DATABASE %s", databaseName);
        QueryResult result = influxDB.query(new Query(query));
        globalState.getState().logStatement(query);

        return new SQLConnection(influxDB);
    }

    @Override
    public String getDBMSName() {
        return "influxdb";
    }
}