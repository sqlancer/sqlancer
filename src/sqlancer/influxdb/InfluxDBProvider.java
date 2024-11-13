package sqlancer.influxdb;

import com.google.auto.service.AutoService;
import sqlancer.*;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.influxdb.InfluxDBProvider.InfluxDBGlobalState;

import java.sql.SQLException;
import java.util.Properties;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

@AutoService(DatabaseProvider.class)
public class InfluxDBProvider extends SQLProviderAdapter<InfluxDBGlobalState, InfluxDBOptions> {

    public InfluxDBProvider() {
        super(InfluxDBGlobalState.class, InfluxDBOptions.class);
    }

    public enum Action implements AbstractAction<InfluxDBGlobalState> {
        WRITE_POINT(influxdb.gen.InfluxDBWritePointGenerator::getQuery),
        CREATE_DATABASE(influxdb.gen.InfluxDBCreateDatabaseGenerator::getQuery);

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
                return 1;  // Typically, a create database action is called once.
            default:
                throw new AssertionError("Unknown action: " + a);
        }
    }

    public static class InfluxDBGlobalState extends SQLGlobalState<InfluxDBOptions, InfluxDBSchema> {
        @Override
        protected InfluxDBSchema readSchema() throws SQLException {
            // Implement method to read InfluxDB schema
            // This might involve querying the database for measurement names, field keys, etc.
            return InfluxDBSchema.fromConnection(getConnection(), getDatabaseName());
        }
    }

    @Override
    public void generateDatabase(InfluxDBGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success;
            do {
                SQLQueryAdapter qt = new influxdb.gen.InfluxDBWritePointGenerator().getQuery(globalState);
                success = globalState.executeStatement(qt);
            } while (!success);
        }
        if (globalState.getSchema().getDatabaseMeasurements().isEmpty()) {
            throw new IgnoreMeException();
        }
        StatementExecutor<InfluxDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                InfluxDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseMeasurements().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(InfluxDBGlobalState globalState) throws Exception {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        String databaseName = globalState.getDatabaseName();

        String url = String.format("http://%s:%d", host, port);
        Properties properties = new Properties();
        properties.setProperty("user", globalState.getDbmsSpecificOptions().getUserName());
        properties.setProperty("password", globalState.getDbmsSpecificOptions().getPassword());

        InfluxDB influxDB = InfluxDBFactory.connect(url, properties.getProperty("user"), properties.getProperty("password"));

        // Create database if not exists
        String createDatabaseQuery = String.format("CREATE DATABASE \"%s\"", databaseName);
        influxDB.query(new Query(createDatabaseQuery));

        return new SQLConnection(influxDB);
    }

    @Override
    public String getDBMSName() {
        return "influxdb";
    }
}