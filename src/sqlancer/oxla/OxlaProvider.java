package sqlancer.oxla;

import com.google.auto.service.AutoService;
import sqlancer.*;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.oxla.gen.OxlaCreateTableGenerator;
import sqlancer.oxla.gen.OxlaDropTableGenerator;
import sqlancer.oxla.gen.OxlaInsertIntoGenerator;
import sqlancer.oxla.schema.OxlaTable;

import java.sql.DriverManager;

// EXISTS
// IN
@AutoService(DatabaseProvider.class)
public class OxlaProvider extends SQLProviderAdapter<OxlaGlobalState, OxlaOptions> {
    protected String entryURL;
    protected String username;
    protected String password;
    protected String host;
    protected Integer port;

    public OxlaProvider() {
        super(OxlaGlobalState.class, OxlaOptions.class);
    }

    protected OxlaProvider(Class<OxlaGlobalState> globalClass, Class<OxlaOptions> optionClass) {
        super(globalClass, optionClass);
    }

    @Override
    public void generateDatabase(OxlaGlobalState globalState) throws Exception {
        // Read functions
        SQLQueryAdapter query = new SQLQueryAdapter("SELECT proname, provolatile FROM pg_proc;");
        SQLancerResultSet rs = query.executeAndGet(globalState);
        while (rs.next()) {
            String functionName = rs.getString(1);
            Character functionType = rs.getString(2).charAt(0);
            globalState.functionAndTypes.put(functionName, functionType);
        }

        // Prepare tables
        ensureValidDatabaseState(globalState);
    }

    @Override
    public SQLConnection createDatabase(OxlaGlobalState globalState) throws Exception {
        MainOptions genericOptions = globalState.getOptions();

        username = genericOptions.getUserName();
        password = genericOptions.getPassword();
        host = genericOptions.getHost();
        port = genericOptions.getPort();
        entryURL = globalState.getDbmsSpecificOptions().connectionURL;

        if (entryURL.startsWith("jdbc:")) {
            entryURL = entryURL.substring(5);
        }
        return new SQLConnection(DriverManager.getConnection("jdbc:" + entryURL, username, password));
    }

    @Override
    public String getDBMSName() {
        return "oxla";
    }

    private synchronized void ensureValidDatabaseState(OxlaGlobalState globalState) throws Exception {
        // 1. Delete random tables until we're not over the specified limit...
        final OxlaOptions options = globalState.getDbmsSpecificOptions();
        int presentTablesCount = globalState.getSchema().getDatabaseTables().size();
        if (presentTablesCount > options.maxTableCount) {
            OxlaDropTableGenerator dropTableGenerator = new OxlaDropTableGenerator(globalState);
            while (presentTablesCount > options.maxTableCount) {
                if (globalState.executeStatement(dropTableGenerator.getQuery(0))) {
                    presentTablesCount--;
                }
            }
        }

        // 2. ...but if we're under, then generate them until the upper limit is reached...
        if (presentTablesCount < options.minTableCount) {
            OxlaCreateTableGenerator createTableGenerator = new OxlaCreateTableGenerator(globalState );
            while (presentTablesCount < options.minTableCount) {
                if (globalState.executeStatement(createTableGenerator.getQuery(0))) {
                    presentTablesCount++;
                }
            }
        }

        // 3. ... while making sure that each table has sufficient number of rows.
        OxlaInsertIntoGenerator insertIntoGenerator = new OxlaInsertIntoGenerator(globalState);
        for (OxlaTable table : globalState.getSchema().getDatabaseTables()) {
            final SQLQueryAdapter rowCountQuery = new SQLQueryAdapter(String.format("SELECT COUNT(*) FROM %s", table.getName()));
            try (SQLancerResultSet rowCountResult = globalState.executeStatementAndGet(rowCountQuery)) {
                rowCountResult.next();
                int rowCount = rowCountResult.getInt(1);
                assert !rowCountResult.next();
                if (rowCount < options.minRowCount) {
                    globalState.executeStatement(insertIntoGenerator.getQueryForTable(table));
                }
            } catch (Exception e) {
                throw new AssertionError("[OxlaFuzzer] failed to insert rows to a table '" + table.getName() + "', because: " + e);
            }
        }

        // FIXME: What about cases where we've run an UPDATE? Should we just drop all rows and repopulate?
    }
}
