package sqlancer.sqlite3;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.Main.QueryManager;
import sqlancer.Main.StateLogger;
import sqlancer.MainOptions;
import sqlancer.ProviderAdapter;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.QueryProvider;
import sqlancer.Randomly;
import sqlancer.StateToReproduce;
import sqlancer.StateToReproduce.SQLite3StateToReproduce;
import sqlancer.TestOracle;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;
import sqlancer.sqlite3.gen.SQLite3AnalyzeGenerator;
import sqlancer.sqlite3.gen.SQLite3Common;
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
import sqlancer.sqlite3.gen.dml.SQLite3UpdateGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table.TableKind;

public class SQLite3Provider extends ProviderAdapter<SQLite3GlobalState, SQLite3Options> {

    public static boolean allowFloatingPointFp = true;
    public static boolean mustKnowResult;

    private SQLite3StateToReproduce state;
    private String databaseName;

    private SQLite3GlobalState globalState;

    // PRAGMAS to achieve good performance
    private static final List<String> DEFAULT_PRAGMAS = Arrays.asList("PRAGMA cache_size = 50000;",
            "PRAGMA temp_store=MEMORY;", "PRAGMA synchronous=off;");

    public SQLite3Provider() {
        super(SQLite3GlobalState.class, SQLite3Options.class);
    }

    public enum Action {
        PRAGMA(SQLite3PragmaGenerator::insertPragma), //
        INDEX(SQLite3IndexGenerator::insertIndex), //
        INSERT(SQLite3InsertGenerator::insertRow), //
        VACUUM(SQLite3VacuumGenerator::executeVacuum), //
        REINDEX(SQLite3ReindexGenerator::executeReindex), //
        ANALYZE(SQLite3AnalyzeGenerator::generateAnalyze), //
        DELETE(SQLite3DeleteGenerator::deleteContent), //
        TRANSACTION_START(SQLite3TransactionGenerator::generateBeginTransaction), //
        ALTER(SQLite3AlterTable::alterTable), //
        DROP_INDEX(SQLite3DropIndexGenerator::dropIndex), //
        UPDATE(SQLite3UpdateGenerator::updateRow), //
        ROLLBACK_TRANSACTION(SQLite3TransactionGenerator::generateRollbackTransaction), //
        COMMIT(SQLite3TransactionGenerator::generateCommit), //
        DROP_TABLE(SQLite3DropTableGenerator::dropTable), //
        DROP_VIEW(SQLite3ViewGenerator::dropView), //
        EXPLAIN(SQLite3ExplainGenerator::explain), //
        CHECK_RTREE_TABLE((g) -> {
            SQLite3Table table = g.getSchema().getRandomTableOrBailout(t -> t.getName().startsWith("r"));
            String format = String.format("SELECT rtreecheck('%s');", table.getName());
            return new QueryAdapter(format);
        }), //
        VIRTUAL_TABLE_ACTION(SQLite3VirtualFTSTableCommandGenerator::create), //
        CREATE_VIEW(SQLite3ViewGenerator::generate), //
        CREATE_TRIGGER(SQLite3CreateTriggerGenerator::create), //
        MANIPULATE_STAT_TABLE((g) -> {
            List<SQLite3Column> columns = new ArrayList<>();
            SQLite3Table t = new SQLite3Table("sqlite_stat1", columns, TableKind.MAIN, false, 1, false, false, false);
            if (Randomly.getBoolean()) {
                return SQLite3DeleteGenerator.deleteContent(g, t);
            } else {
                StringBuilder sb = new StringBuilder();
                sb.append("INSERT OR IGNORE INTO sqlite_stat1");
                String indexName;
                try (Statement stat = g.getConnection().createStatement()) {
                    try (ResultSet rs = stat.executeQuery(
                            "SELECT name FROM sqlite_master WHERE type='index' ORDER BY RANDOM() LIMIT 1;")) {
                        if (rs.isClosed()) {
                            throw new IgnoreMeException();
                        }
                        indexName = rs.getString("name");
                    }
                }
                sb.append(" VALUES");
                sb.append("('");
                sb.append(g.getSchema().getRandomTable().getName());
                sb.append("', ");
                sb.append("'");
                if (Randomly.getBoolean()) {
                    sb.append(indexName);
                } else {
                    sb.append(g.getSchema().getRandomTable().getName());
                }
                sb.append("'");
                sb.append(", '");
                for (int i = 0; i < Randomly.smallNumber(); i++) {
                    if (i != 0) {
                        sb.append(" ");
                    }
                    if (Randomly.getBoolean()) {
                        sb.append(g.getRandomly().getInteger());
                    } else {
                        sb.append(Randomly.smallNumber());
                    }
                }
                if (Randomly.getBoolean()) {
                    sb.append(" sz=");
                    sb.append(g.getRandomly().getInteger());
                }
                if (Randomly.getBoolean()) {
                    sb.append(" unordered");
                }
                if (Randomly.getBoolean()) {
                    sb.append(" noskipscan");
                }
                sb.append("')");
                return new QueryAdapter(sb.toString(), Arrays.asList("no such table"));
            }
        });

        private final QueryProvider<SQLite3GlobalState> queryProvider;

        Action(QueryProvider<SQLite3GlobalState> queryProvider) {
            this.queryProvider = queryProvider;
        }

        public Query getQuery(SQLite3GlobalState state) throws SQLException {
            return queryProvider.getQuery(state);
        }
    }

    public static class SQLite3GlobalState extends GlobalState<SQLite3Options> {

        private SQLite3Schema schema;
        private SQLite3Options sqliteOptions;

        public SQLite3Schema getSchema() {
            return schema;
        }

        public void setSchema(SQLite3Schema schema) {
            this.schema = schema;
        }

        public void setSqliteOptions(SQLite3Options sqliteOptions) {
            this.sqliteOptions = sqliteOptions;
        }

        public SQLite3Options getSqliteOptions() {
            return sqliteOptions;
        }

    }

    private enum TableType {
        NORMAL, FTS, RTREE
    }

    @Override
    public void generateAndTestDatabase(SQLite3GlobalState globalState) throws SQLException {
        this.globalState = globalState;
        SQLite3Options sqliteOptions = globalState.getDmbsSpecificOptions();
        QueryManager manager = globalState.getManager();
        MainOptions options = globalState.getOptions();
        this.databaseName = globalState.getDatabaseName();
        Randomly r = new Randomly(SQLite3SpecialStringGenerator::generate);
        globalState.setSqliteOptions(sqliteOptions);
        globalState.setRandomly(r);
        StateLogger logger = globalState.getLogger();
        this.state = (SQLite3StateToReproduce) globalState.getState();
        globalState.setState(state);
        if (globalState.getDmbsSpecificOptions().generateDatabase) {

            addSensiblePragmaDefaults(globalState);
            int nrTablesToCreate = 1;
            if (Randomly.getBoolean()) {
                nrTablesToCreate++;
            }
            while (Randomly.getBooleanWithSmallProbability()) {
                nrTablesToCreate++;
            }
            int i = 0;

            globalState.setSchema(SQLite3Schema.fromConnection(globalState));
            do {
                Query tableQuery = getTableQuery(r, i++);
                executeStatement(globalState, manager, tableQuery);
                globalState.setSchema(SQLite3Schema.fromConnection(globalState));
            } while (globalState.getSchema().getDatabaseTables().size() != nrTablesToCreate);
            assert globalState.getSchema().getTables().getTables().size() == nrTablesToCreate;
            checkTablesForGeneratedColumnLoops(globalState);
            if (globalState.getDmbsSpecificOptions().testDBStats && Randomly.getBooleanWithSmallProbability()) {
                QueryAdapter tableQuery = new QueryAdapter(
                        "CREATE VIRTUAL TABLE IF NOT EXISTS stat USING dbstat(main)");
                executeStatement(globalState, manager, tableQuery);
                globalState.setSchema(SQLite3Schema.fromConnection(globalState));
            }
            int[] nrRemaining = new int[Action.values().length];
            List<Action> actions = new ArrayList<>();
            int total = 0;
            for (i = 0; i < Action.values().length; i++) {
                Action action = Action.values()[i];
                int nrPerformed = 0;
                switch (action) {
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
                    nrPerformed = r.getInteger(0, options.getMaxNumberInserts());
                    break;
                case MANIPULATE_STAT_TABLE:
                    nrPerformed = r.getInteger(0, 5);
                    break;
                case INDEX:
                    nrPerformed = r.getInteger(0, 5);
                    break;
                case VIRTUAL_TABLE_ACTION:
                case UPDATE:
                    nrPerformed = r.getInteger(0, 30);
                    break;
                case PRAGMA:
                    nrPerformed = r.getInteger(0, 20);
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
                if (nrPerformed != 0) {
                    actions.add(action);
                }
                nrRemaining[action.ordinal()] = nrPerformed;
                total += nrPerformed;
            }

            if (options.logEachSelect()) {
                logger.writeCurrent(state);
            }

            while (total != 0) {
                Action nextAction = null;
                int selection = r.getInteger(0, total);
                int previousRange = 0;
                for (i = 0; i < nrRemaining.length; i++) {
                    if (previousRange <= selection && selection < previousRange + nrRemaining[i]) {
                        nextAction = Action.values()[i];
                        break;
                    } else {
                        previousRange += nrRemaining[i];
                    }
                }
                assert nextAction != null;
                assert nrRemaining[nextAction.ordinal()] > 0;
                nrRemaining[nextAction.ordinal()]--;
                Query query = null;
                try {
                    query = nextAction.getQuery(globalState);
                    if (options.logEachSelect()) {
                        logger.writeCurrent(query.getQueryString());
                    }
                    executeStatement(globalState, manager, query);
                } catch (IgnoreMeException e) {

                }
                if (query != null && query.couldAffectSchema()) {
                    globalState.setSchema(SQLite3Schema.fromConnection(globalState));
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                }
                total--;
            }
            Query query = SQLite3TransactionGenerator.generateCommit(globalState);
            executeStatement(globalState, manager, query);

            // also do an abort for DEFERRABLE INITIALLY DEFERRED
            query = SQLite3TransactionGenerator.generateRollbackTransaction(globalState);
            executeStatement(globalState, manager, query);
            globalState.setSchema(SQLite3Schema.fromConnection(globalState));
            manager.incrementCreateDatabase();
        } else {
            globalState.setSchema(SQLite3Schema.fromConnection(globalState));
        }
        TestOracle oracle = globalState.getSqliteOptions().oracle.create(globalState);
        if (oracle.onlyWorksForNonEmptyTables()) {
            for (SQLite3Table table : globalState.getSchema().getDatabaseTables()) {
                int nrRows = SQLite3Schema.getNrRows(globalState, table.getName());
                if (nrRows == 0) {
                    throw new IgnoreMeException();
                }
            }
        }
        for (int i = 0; i < options.getNrQueries(); i++) {
            try {
                oracle.check();
                manager.incrementSelectQueryCount();
            } catch (IgnoreMeException e) {

            }
        }
        try {
            if (options.logEachSelect()) {
                logger.getCurrentFileWriter().close();
                logger.currentFileWriter = null;
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if (globalState.getDmbsSpecificOptions().exitAfterFirstDatabase) {
            System.exit(0);
        }
    }

    private void executeStatement(SQLite3GlobalState globalState, QueryManager manager, Query tableQuery)
            throws SQLException {
        manager.execute(tableQuery);
        if (globalState.getDmbsSpecificOptions().printStatements) {
            String s = tableQuery.getQueryString();
            if (!s.endsWith(";")) {
                s = s + ";";
            }
            System.out.println(s);
        }
    }

    private void checkTablesForGeneratedColumnLoops(SQLite3GlobalState globalState) throws SQLException {
        for (SQLite3Table table : globalState.getSchema().getDatabaseTables()) {
            Query q = new QueryAdapter("SELECT * FROM " + table.getName(),
                    Arrays.asList("needs an odd number of arguments", " requires an even number of arguments",
                            "generated column loop", "integer overflow", "malformed JSON",
                            "JSON cannot hold BLOB values", "JSON path error", "labels must be TEXT"));
            if (!q.execute(globalState)) {
                throw new IgnoreMeException();
            }
        }
    }

    private Query getTableQuery(Randomly r, int i) throws AssertionError {
        Query tableQuery;
        List<TableType> options = new ArrayList<>(Arrays.asList(TableType.values()));
        if (!globalState.getSqliteOptions().testFts) {
            options.remove(TableType.FTS);
        }
        if (!globalState.getSqliteOptions().testRtree) {
            options.remove(TableType.RTREE);
        }
        switch (Randomly.fromList(options)) {
        case NORMAL:
            String tableName = SQLite3Common.createTableName(i);
            tableQuery = SQLite3TableGenerator.createTableStatement(tableName, globalState);
            break;
        case FTS:
            String ftsTableName = "v" + SQLite3Common.createTableName(i);
            tableQuery = SQLite3CreateVirtualFTSTableGenerator.createTableStatement(ftsTableName, r);
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

    private void addSensiblePragmaDefaults(SQLite3GlobalState globalState) throws SQLException {
        List<String> pragmasToExecute = new ArrayList<>();
        if (!Randomly.getBooleanWithSmallProbability()) {
            pragmasToExecute.addAll(DEFAULT_PRAGMAS);
        }
        if (Randomly.getBoolean() && !mustKnowResult) {
            pragmasToExecute.add("PRAGMA case_sensitive_like=ON;");
        }
        if (Randomly.getBoolean()) {
            pragmasToExecute.add(String.format("PRAGMA encoding = '%s';",
                    Randomly.fromOptions("UTF-8", "UTF-16", "UTF-16le", "UTF-16be")));
        }
        for (String s : pragmasToExecute) {
            if (globalState.getDmbsSpecificOptions().printStatements) {
                System.out.println(s);
            }
            Query q = new QueryAdapter(s);
            state.statements.add(q);
            q.execute(globalState);
        }
    }

    @Override
    public Connection createDatabase(SQLite3GlobalState globalState) throws SQLException {
        File dir = new File("." + File.separator + "databases");
        if (!dir.exists()) {
            dir.mkdir();
        }
        File dataBase = new File(dir, globalState.getDatabaseName() + ".db");
        if (dataBase.exists() && ((SQLite3GlobalState) globalState).getDmbsSpecificOptions().deleteIfExists) {
            dataBase.delete();
        }
        String url = "jdbc:sqlite:" + dataBase.getAbsolutePath();
        return DriverManager.getConnection(url);
    }

    @Override
    public String getDBMSName() {
        return "sqlite3";
    }

    @Override
    public String toString() {
        return String.format("SQLite3Provider [database: %s]", databaseName);
    }

    @Override
    public void printDatabaseSpecificState(FileWriter writer, StateToReproduce state) {
    }

    @Override
    public StateToReproduce getStateToReproduce(String databaseName) {
        return new SQLite3StateToReproduce(databaseName);
    }

}
