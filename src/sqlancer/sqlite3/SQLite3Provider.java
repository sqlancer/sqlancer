package sqlancer.sqlite3;

import java.io.File;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.sqlite3.SQLite3Options.SQLite3OracleFactory;
import sqlancer.sqlite3.gen.SQLite3AnalyzeGenerator;
import sqlancer.sqlite3.gen.SQLite3CreateVirtualRtreeTabelGenerator;
import sqlancer.sqlite3.gen.SQLite3ExplainGenerator;
import sqlancer.sqlite3.gen.SQLite3PragmaGenerator;
import sqlancer.sqlite3.gen.SQLite3ReindexGenerator;
import sqlancer.sqlite3.gen.SQLite3TransactionGenerator;
import sqlancer.sqlite3.gen.SQLite3VacuumGenerator;
import sqlancer.sqlite3.gen.SQLite3VirtualFTSTableCommandGenerator;
import sqlancer.sqlite3.gen.ddl.SQLite3AlterTable;
import sqlancer.sqlite3.gen.ddl.SQLite3CreateTriggerGenerator;
import sqlancer.sqlite3.gen.ddl.SQLite3CreateVirtualFTSTableGenerator;
import sqlancer.sqlite3.gen.ddl.SQLite3DropIndexGenerator;
import sqlancer.sqlite3.gen.ddl.SQLite3DropTableGenerator;
import sqlancer.sqlite3.gen.ddl.SQLite3IndexGenerator;
import sqlancer.sqlite3.gen.ddl.SQLite3TableGenerator;
import sqlancer.sqlite3.gen.ddl.SQLite3ViewGenerator;
import sqlancer.sqlite3.gen.dml.SQLite3DeleteGenerator;
import sqlancer.sqlite3.gen.dml.SQLite3InsertGenerator;
import sqlancer.sqlite3.gen.dml.SQLite3StatTableGenerator;
import sqlancer.sqlite3.gen.dml.SQLite3UpdateGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;

@AutoService(DatabaseProvider.class)
public class SQLite3Provider extends SQLProviderAdapter<SQLite3GlobalState, SQLite3Options> {

    public static boolean allowFloatingPointFp = true;
    public static boolean mustKnowResult;

    // PRAGMAS to achieve good performance
    private static final List<String> DEFAULT_PRAGMAS = Arrays.asList("PRAGMA cache_size = 50000;",
            "PRAGMA temp_store=MEMORY;", "PRAGMA synchronous=off;");

    public SQLite3Provider() {
        super(SQLite3GlobalState.class, SQLite3Options.class);
    }

    public enum Action implements AbstractAction<SQLite3GlobalState> {
        PRAGMA(SQLite3PragmaGenerator::insertPragma), // 0
        CREATE_INDEX(SQLite3IndexGenerator::insertIndex), // 1
        CREATE_VIEW(SQLite3ViewGenerator::generate), // 2
        CREATE_TRIGGER(SQLite3CreateTriggerGenerator::create), // 3
        CREATE_TABLE(SQLite3TableGenerator::createRandomTableStatement), // 4
        CREATE_VIRTUALTABLE(SQLite3CreateVirtualFTSTableGenerator::createRandomTableStatement), // 5
        CREATE_RTREETABLE(SQLite3CreateVirtualRtreeTabelGenerator::createRandomTableStatement), // 6
        INSERT(SQLite3InsertGenerator::insertRow), // 7
        DELETE(SQLite3DeleteGenerator::deleteContent), // 8
        ALTER(SQLite3AlterTable::alterTable), // 9
        UPDATE(SQLite3UpdateGenerator::updateRow), // 10
        DROP_INDEX(SQLite3DropIndexGenerator::dropIndex), // 11
        DROP_TABLE(SQLite3DropTableGenerator::dropTable), // 12
        DROP_VIEW(SQLite3ViewGenerator::dropView), // 13
        VACUUM(SQLite3VacuumGenerator::executeVacuum), // 14
        REINDEX(SQLite3ReindexGenerator::executeReindex), // 15
        ANALYZE(SQLite3AnalyzeGenerator::generateAnalyze), // 16
        EXPLAIN(SQLite3ExplainGenerator::explain), // 17
        CHECK_RTREE_TABLE((g) -> {
            SQLite3Table table = g.getSchema().getRandomTableOrBailout(t -> t.getName().startsWith("r"));
            String format = String.format("SELECT rtreecheck('%s');", table.getName());
            return new SQLQueryAdapter(format, ExpectedErrors.from("The database file is locked"));
        }), // 18
        VIRTUAL_TABLE_ACTION(SQLite3VirtualFTSTableCommandGenerator::create), // 19
        MANIPULATE_STAT_TABLE(SQLite3StatTableGenerator::getQuery), // 20
        TRANSACTION_START(SQLite3TransactionGenerator::generateBeginTransaction) {
            @Override
            public boolean canBeRetried() {
                return false;
            }

        }, // 21
        ROLLBACK_TRANSACTION(SQLite3TransactionGenerator::generateRollbackTransaction) {
            @Override
            public boolean canBeRetried() {
                return false;
            }
        }, // 22
        COMMIT(SQLite3TransactionGenerator::generateCommit) {
            @Override
            public boolean canBeRetried() {
                return false;
            }
        }; // 23

        private final SQLQueryProvider<SQLite3GlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<SQLite3GlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(SQLite3GlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private enum TableType {
        NORMAL, FTS, RTREE
    }

    private static int mapActions(SQLite3GlobalState globalState, Action a) {
        int nrPerformed = 0;
        Randomly r = globalState.getRandomly();
        switch (a) {
        case CREATE_VIEW:
            nrPerformed = r.getInteger(0, 2);
            break;
        case DELETE:
        case DROP_VIEW:
        case DROP_INDEX:
            nrPerformed = r.getInteger(0, 0);
            break;
        case ALTER:
            nrPerformed = r.getInteger(0, 0);
            break;
        case EXPLAIN:
        case CREATE_TRIGGER:
        case DROP_TABLE:
            nrPerformed = r.getInteger(0, 0);
            break;
        case VACUUM:
        case CHECK_RTREE_TABLE:
            nrPerformed = r.getInteger(0, 3);
            break;
        case INSERT:
            nrPerformed = r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
            break;
        case MANIPULATE_STAT_TABLE:
            nrPerformed = r.getInteger(0, 5);
            break;
        case CREATE_INDEX:
            nrPerformed = r.getInteger(0, 5);
            break;
        case VIRTUAL_TABLE_ACTION:
        case UPDATE:
            nrPerformed = r.getInteger(0, 30);
            break;
        case PRAGMA:
            nrPerformed = r.getInteger(0, 20);
            break;
        case CREATE_TABLE:
        case CREATE_VIRTUALTABLE:
        case CREATE_RTREETABLE:
            nrPerformed = 0;
            break;
        case TRANSACTION_START:
        case REINDEX:
        case ANALYZE:
        case ROLLBACK_TRANSACTION:
        case COMMIT:
        default:
            nrPerformed = r.getInteger(1, 10);
            break;
        }
        return nrPerformed;
    }

    @Override
    public void generateDatabase(SQLite3GlobalState globalState) throws Exception {
        Randomly r = new Randomly(SQLite3SpecialStringGenerator::generate);
        globalState.setRandomly(r);
        if (globalState.getDbmsSpecificOptions().generateDatabase) {

            addSensiblePragmaDefaults(globalState);
            int nrTablesToCreate = 1;
            if (Randomly.getBoolean()) {
                nrTablesToCreate++;
            }
            while (Randomly.getBooleanWithSmallProbability()) {
                nrTablesToCreate++;
            }
            int i = 0;

            do {
                SQLQueryAdapter tableQuery = getTableQuery(globalState, i++);
                globalState.executeStatement(tableQuery);
            } while (globalState.getSchema().getDatabaseTables().size() < nrTablesToCreate);
            assert globalState.getSchema().getTables().getTables().size() == nrTablesToCreate;
            checkTablesForGeneratedColumnLoops(globalState);
            if (globalState.getDbmsSpecificOptions().testDBStats && Randomly.getBooleanWithSmallProbability()) {
                SQLQueryAdapter tableQuery = new SQLQueryAdapter(
                        "CREATE VIRTUAL TABLE IF NOT EXISTS stat USING dbstat(main)");
                globalState.executeStatement(tableQuery);
            }
            StatementExecutor<SQLite3GlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                    SQLite3Provider::mapActions, (q) -> {
                        if (q.couldAffectSchema() && globalState.getSchema().getDatabaseTables().isEmpty()) {
                            throw new IgnoreMeException();
                        }
                    });
            se.executeStatements();

            SQLQueryAdapter query = SQLite3TransactionGenerator.generateCommit(globalState);
            globalState.executeStatement(query);

            // also do an abort for DEFERRABLE INITIALLY DEFERRED
            query = SQLite3TransactionGenerator.generateRollbackTransaction(globalState);
            globalState.executeStatement(query);
        }
    }

    private void checkTablesForGeneratedColumnLoops(SQLite3GlobalState globalState) throws Exception {
        for (SQLite3Table table : globalState.getSchema().getDatabaseTables()) {
            SQLQueryAdapter q = new SQLQueryAdapter("SELECT * FROM " + table.getName(),
                    ExpectedErrors.from("needs an odd number of arguments", " requires an even number of arguments",
                            "generated column loop", "integer overflow", "malformed JSON",
                            "JSON cannot hold BLOB values", "JSON path error", "labels must be TEXT",
                            "table does not support scanning"));
            if (!q.execute(globalState)) {
                throw new IgnoreMeException();
            }
        }
    }

    private SQLQueryAdapter getTableQuery(SQLite3GlobalState globalState, int i) throws AssertionError {
        SQLQueryAdapter tableQuery;
        List<TableType> options = new ArrayList<>(Arrays.asList(TableType.values()));
        if (!globalState.getDbmsSpecificOptions().testFts) {
            options.remove(TableType.FTS);
        }
        if (!globalState.getDbmsSpecificOptions().testRtree) {
            options.remove(TableType.RTREE);
        }
        switch (Randomly.fromList(options)) {
        case NORMAL:
            String tableName = DBMSCommon.createTableName(i);
            tableQuery = SQLite3TableGenerator.createTableStatement(tableName, globalState);
            break;
        case FTS:
            String ftsTableName = "v" + DBMSCommon.createTableName(i);
            tableQuery = SQLite3CreateVirtualFTSTableGenerator.createTableStatement(ftsTableName,
                    globalState.getRandomly());
            break;
        case RTREE:
            String rTreeTableName = "rt" + i;
            tableQuery = SQLite3CreateVirtualRtreeTabelGenerator.createTableStatement(rTreeTableName, globalState);
            break;
        default:
            throw new AssertionError();
        }
        return tableQuery;
    }

    private void addSensiblePragmaDefaults(SQLite3GlobalState globalState) throws Exception {
        List<String> pragmasToExecute = new ArrayList<>();
        if (!Randomly.getBooleanWithSmallProbability()) {
            pragmasToExecute.addAll(DEFAULT_PRAGMAS);
        }
        if (Randomly.getBoolean() && globalState.getDbmsSpecificOptions().oracles != SQLite3OracleFactory.PQS) {
            // the PQS implementation currently assumes the default behavior of LIKE
            pragmasToExecute.add("PRAGMA case_sensitive_like=ON;");
        }
        if (Randomly.getBoolean() && globalState.getDbmsSpecificOptions().oracles != SQLite3OracleFactory.PQS) {
            // the encoding has an influence how binary strings are cast
            pragmasToExecute.add(String.format("PRAGMA encoding = '%s';",
                    Randomly.fromOptions("UTF-8", "UTF-16", "UTF-16le", "UTF-16be")));
        }
        for (String s : pragmasToExecute) {
            globalState.executeStatement(new SQLQueryAdapter(s));
        }
    }

    @Override
    public SQLConnection createDatabase(SQLite3GlobalState globalState) throws SQLException {
        File dir = new File("." + File.separator + "databases");
        if (!dir.exists()) {
            dir.mkdir();
        }
        File dataBase = new File(dir, globalState.getDatabaseName() + ".db");
        if (dataBase.exists() && ((SQLite3GlobalState) globalState).getDbmsSpecificOptions().deleteIfExists) {
            dataBase.delete();
        }
        String url = "jdbc:sqlite:" + dataBase.getAbsolutePath();
        return new SQLConnection(DriverManager.getConnection(url));
    }

    @Override
    public String getDBMSName() {
        return "sqlite3";
    }

    @Override
    public String getQueryPlan(String selectStr, SQLite3GlobalState globalState) throws Exception {
        String queryPlan = "";
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(selectStr);
            try {
                globalState.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // Set up the expected errors for NoREC oracle.
        ExpectedErrors errors = new ExpectedErrors();
        SQLite3Errors.addExpectedExpressionErrors(errors);
        SQLite3Errors.addMatchQueryErrors(errors);
        SQLite3Errors.addQueryErrors(errors);
        SQLite3Errors.addInsertUpdateErrors(errors);

        SQLQueryAdapter q = new SQLQueryAdapter(SQLite3ExplainGenerator.explain(selectStr), errors);
        try (SQLancerResultSet rs = q.executeAndGet(globalState)) {
            if (rs != null) {
                while (rs.next()) {
                    queryPlan += rs.getString(4) + ";";
                }
            }
        } catch (SQLException | AssertionError e) {
            queryPlan = "";
        }
        return queryPlan;
    }

    @Override
    protected double[] initializeWeightedAverageReward() {
        return new double[Action.values().length];
    }

    @Override
    protected void executeMutator(int index, SQLite3GlobalState globalState) throws Exception {
        SQLQueryAdapter queryMutateTable = Action.values()[index].getQuery(globalState);
        globalState.executeStatement(queryMutateTable);

    }

    @Override
    protected boolean addRowsToAllTables(SQLite3GlobalState globalState) throws Exception {
        List<SQLite3Table> tablesNoRow = globalState.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getNrRows(globalState) == 0).collect(Collectors.toList());
        for (SQLite3Table table : tablesNoRow) {
            SQLQueryAdapter queryAddRows = SQLite3InsertGenerator.insertRow(globalState, table);
            globalState.executeStatement(queryAddRows);
        }

        return true;
    }
}
