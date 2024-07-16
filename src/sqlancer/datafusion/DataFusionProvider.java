package sqlancer.datafusion;

import static sqlancer.datafusion.DataFusionUtil.DataFusionLogger.DataFusionLogType.DML;
import static sqlancer.datafusion.DataFusionUtil.dfAssert;
import static sqlancer.datafusion.DataFusionUtil.displayTables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;

import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionSchema.DataFusionTable;
import sqlancer.datafusion.DataFusionUtil.DataFusionInstanceID;
import sqlancer.datafusion.DataFusionUtil.DataFusionLogger;
import sqlancer.datafusion.gen.DataFusionInsertGenerator;
import sqlancer.datafusion.gen.DataFusionTableGenerator;

@AutoService(DatabaseProvider.class)
public class DataFusionProvider extends SQLProviderAdapter<DataFusionGlobalState, DataFusionOptions> {

    public DataFusionProvider() {
        super(DataFusionGlobalState.class, DataFusionOptions.class);
    }

    @Override
    public void generateDatabase(DataFusionGlobalState globalState) throws Exception {
        int tableCount = Randomly.fromOptions(1, 2, 3, 4, 5, 6, 7);
        for (int i = 0; i < tableCount; i++) {
            SQLQueryAdapter queryCreateRandomTable = new DataFusionTableGenerator().getQuery(globalState);
            queryCreateRandomTable.execute(globalState);
            globalState.updateSchema();
            globalState.dfLogger.appendToLog(DML, queryCreateRandomTable.toString() + "\n");
        }

        // Now only `INSERT` DML is supported
        // If more DMLs are added later, should use`StatementExecutor` instead
        // (see DuckDB's implementation for reference)

        globalState.updateSchema();
        List<DataFusionTable> allTables = globalState.getSchema().getDatabaseTables();
        List<String> allTablesName = allTables.stream().map(t -> t.getName()).collect(Collectors.toList());
        if (allTablesName.isEmpty()) {
            dfAssert(false, "Generate Database failed.");
        }

        // Randomly insert some data into existing tables
        for (DataFusionTable table : allTables) {
            int nInsertQuery = globalState.getRandomly().getInteger(0, globalState.getOptions().getMaxNumberInserts());

            for (int i = 0; i < nInsertQuery; i++) {
                SQLQueryAdapter insertQuery = null;
                try {
                    insertQuery = DataFusionInsertGenerator.getQuery(globalState, table);
                } catch (IgnoreMeException e) {
                    // Only for special case: table has 0 column
                    continue;
                }

                insertQuery.execute(globalState);
                globalState.dfLogger.appendToLog(DML, insertQuery.toString() + "\n");
            }
        }

        // TODO(datafusion) add `DataFUsionLogType.STATE` for this whole db state log
        if (globalState.getDbmsSpecificOptions().showDebugInfo) {
            System.out.println(displayTables(globalState, allTablesName));
        }
    }

    @Override
    public SQLConnection createDatabase(DataFusionGlobalState globalState) throws SQLException {
        if (globalState.getDbmsSpecificOptions().showDebugInfo) {
            System.out.println("A new database get created!\n");
        }
        Properties props = new Properties();
        props.setProperty("UseEncryption", "false");
        // must set 'user' and 'password' to trigger server 'do_handshake()'
        props.setProperty("user", "foo");
        props.setProperty("password", "bar");
        props.setProperty("create", globalState.getDatabaseName()); // Hack: use this property to let DataFusion server
        // clear the current context
        String url = "jdbc:arrow-flight-sql://127.0.0.1:50051";
        Connection connection = DriverManager.getConnection(url, props);

        return new SQLConnection(connection);
    }

    @Override
    public String getDBMSName() {
        return "datafusion";
    }

    // If run SQLancer with multiple thread
    // Each thread's instance will have its own `DataFusionGlobalState`
    // It will store global states including:
    // JDBC connection to DataFusion server
    // Logger for this thread
    public static class DataFusionGlobalState extends SQLGlobalState<DataFusionOptions, DataFusionSchema> {
        public DataFusionLogger dfLogger;
        DataFusionInstanceID id;

        public DataFusionGlobalState() {
            // HACK: test will only run in spawned thread, not main thread
            // this way redundant logger files won't be created
            if (Thread.currentThread().getName().equals("main")) {
                return;
            }

            id = new DataFusionInstanceID(Thread.currentThread().getName());
            try {
                dfLogger = new DataFusionLogger(this, id);
            } catch (Exception e) {
                throw new IgnoreMeException();
            }
        }

        @Override
        protected DataFusionSchema readSchema() throws SQLException {
            return DataFusionSchema.fromConnection(getConnection(), getDatabaseName());
        }
    }
}
