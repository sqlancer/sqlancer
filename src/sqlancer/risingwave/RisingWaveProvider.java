package sqlancer.risingwave;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import com.google.auto.service.AutoService;

import sqlancer.DatabaseProvider;
import sqlancer.MainOptions;
import sqlancer.Reproducer;
import sqlancer.SQLConnection;
import sqlancer.SQLProviderAdapter;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.risingwave.RisingWaveSchema.RisingWaveTable;
import sqlancer.risingwave.gen.RisingWaveTableGenerator;

@AutoService(DatabaseProvider.class)
public class RisingWaveProvider extends SQLProviderAdapter<RisingWaveGlobalState, RisingWaveOptions> {
    public RisingWaveProvider() {
        super(RisingWaveGlobalState.class, RisingWaveOptions.class);
    }

    @Override
    public String getDBMSName() {
        return "risingwave";
    }

    public static Connection connect(RisingWaveGlobalState state, String database) throws SQLException {
        RisingWaveOptions rw = state.getDbmsSpecificOptions();
        rw.validate();
        MainOptions options = state.getOptions();
        String host = options.getHost() == null ? "localhost" : options.getHost();
        if (host.contains(":") && !host.startsWith("[")) {
            host = "[" + host + "]";
        }
        int port = options.getPort() == MainOptions.NO_SET_PORT ? 4566 : options.getPort();
        Properties properties = new Properties();
        properties.setProperty("user", options.getUserName());
        properties.setProperty("password", options.getPassword());
        properties.setProperty("connectTimeout", Integer.toString(rw.connectTimeoutSeconds));
        properties.setProperty("socketTimeout", Integer.toString(rw.statementTimeoutSeconds + 5));
        properties.setProperty("cancelSignalTimeout", Integer.toString(rw.connectTimeoutSeconds));
        Connection connection = DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/" + database,
                properties);
        try (Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(rw.statementTimeoutSeconds);
            statement.execute("SET statement_timeout = " + rw.statementTimeoutSeconds * 1000);
        } catch (SQLException failure) {
            try {
                connection.close();
            } catch (SQLException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
        return connection;
    }

    @Override
    public SQLConnection createDatabase(RisingWaveGlobalState state) throws SQLException {
        RisingWaveOptions options = state.getDbmsSpecificOptions();
        options.validate();
        String database = "rw_sqlancer_" + UUID.randomUUID().toString().replace("-", "");
        boolean created = false;
        try {
            try (Connection admin = connect(state, options.entryDatabase);
                    Statement statement = admin.createStatement()) {
                statement.setQueryTimeout(options.statementTimeoutSeconds);
                statement.execute("CREATE DATABASE " + database);
                created = true;
            }
            state.getState().logStatement("CREATE DATABASE " + database);
            state.getState().logStatement("-- Physical RisingWave database: " + database);
            state.getState().logStatement("SET statement_timeout = " + options.statementTimeoutSeconds * 1000);
            state.setDatabaseName(database);
            return new RisingWaveConnection(openConnection(state, database), database, options,
                    () -> dropOwned(state, database));
        } catch (SQLException | RuntimeException failure) {
            if (created) {
                try {
                    dropOwned(state, database);
                } catch (SQLException cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
            throw failure;
        }
    }

    protected Connection openConnection(RisingWaveGlobalState state, String database) throws SQLException {
        return connect(state, database);
    }

    private static void dropOwned(RisingWaveGlobalState state, String database) throws SQLException {
        // Only drop the exact database whose creation succeeded in this invocation.
        try (Connection admin = connect(state, state.getDbmsSpecificOptions().entryDatabase);
                Statement statement = admin.createStatement()) {
            statement.setQueryTimeout(state.getDbmsSpecificOptions().statementTimeoutSeconds);
            statement.execute("DROP DATABASE " + RisingWaveToStringVisitor.identifier(database));
        }
    }

    @Override
    protected void checkViewsAreValid(RisingWaveGlobalState state) {
        RisingWaveConnection connection = (RisingWaveConnection) state.getConnection();
        try {
            for (RisingWaveTable view : state.getSchema().getViews()) {
                connection.resetLastError();
                boolean valid = state.executeStatement(new SQLQueryAdapter(
                        "SELECT 1 FROM " + RisingWaveToStringVisitor.identifier(view.getName()) + " LIMIT 1",
                        RisingWaveErrors.expressionErrors()));
                if (!valid) {
                    // Unlike the shared best-effort validation, retain unexpected errors and incomplete reads.
                    if (connection.getLastError() != RisingWaveErrors.Category.NUMERIC_OVERFLOW) {
                        throw new AssertionError("Incomplete RisingWave view validation: " + view.getName());
                    }
                    String kind = view.getKind() == RisingWaveSchema.RelationKind.MATERIALIZED_VIEW
                            ? "MATERIALIZED VIEW" : "VIEW";
                    state.executeStatement(new SQLQueryAdapter(
                            "DROP " + kind + " " + RisingWaveToStringVisitor.identifier(view.getName()), true));
                }
            }
        } catch (Exception failure) {
            throw new AssertionError("RisingWave view validation failed", failure);
        }
    }

    @Override
    public Reproducer<RisingWaveGlobalState> generateAndTestDatabase(RisingWaveGlobalState state) throws Exception {
        RisingWaveConnection connection = (RisingWaveConnection) state.getConnection();
        connection.beginRun();
        try {
            Reproducer<RisingWaveGlobalState> result = super.generateAndTestDatabase(state);
            connection.finish(result != null);
            return result;
        } catch (Exception | AssertionError failure) {
            connection.finish(true);
            throw failure;
        }
    }

    @Override
    public void generateDatabase(RisingWaveGlobalState state) throws Exception {
        try {
            List<RisingWaveTable> tables = RisingWaveTableGenerator.generate(state.getRandomly(),
                    state.getDbmsSpecificOptions());
            for (RisingWaveTable table : tables) {
                state.executeStatement(new SQLQueryAdapter(RisingWaveTableGenerator.create(table), true));
            }
            state.updateSchema();
            sqlancer.risingwave.gen.RisingWaveSetupGenerator.execute(state, tables);
            state.executeStatement(new SQLQueryAdapter("FLUSH"));
            state.updateSchema();
        } catch (Exception | AssertionError failure) {
            if (state.getConnection() instanceof RisingWaveConnection) {
                ((RisingWaveConnection) state.getConnection()).finish(true);
            }
            throw failure;
        }
    }
}
