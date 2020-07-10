package sqlancer.mariadb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.Main.QueryManager;
import sqlancer.MainOptions;
import sqlancer.ProviderAdapter;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.gen.MariaDBIndexGenerator;
import sqlancer.mariadb.gen.MariaDBInsertGenerator;
import sqlancer.mariadb.gen.MariaDBSetGenerator;
import sqlancer.mariadb.gen.MariaDBTableAdminCommandGenerator;
import sqlancer.mariadb.gen.MariaDBTableGenerator;
import sqlancer.mariadb.gen.MariaDBTruncateGenerator;
import sqlancer.mariadb.gen.MariaDBUpdateGenerator;
import sqlancer.mariadb.oracle.MariaDBNoRECOracle;
import sqlancer.sqlite3.gen.SQLite3Common;

public class MariaDBProvider extends ProviderAdapter<MariaDBGlobalState, MariaDBOptions> {

    public static final int MAX_EXPRESSION_DEPTH = 3;
    private final Randomly r = new Randomly();
    private String databaseName;

    public MariaDBProvider() {
        super(MariaDBGlobalState.class, MariaDBOptions.class);
    }

    enum Action {
        ANALYZE_TABLE, //
        CHECKSUM, //
        CHECK_TABLE, //
        CREATE_INDEX, //
        INSERT, //
        OPTIMIZE, //
        REPAIR_TABLE, //
        SET, //
        TRUNCATE, //
        UPDATE, //
    }

    @Override
    public void generateAndTestDatabase(MariaDBGlobalState globalState) throws SQLException {
        this.databaseName = globalState.getDatabaseName();
        MainOptions options = globalState.getOptions();
        QueryManager manager = globalState.getManager();

        while (globalState.getSchema().getDatabaseTables().size() < Randomly.smallNumber() + 1) {
            String tableName = SQLite3Common.createTableName(globalState.getSchema().getDatabaseTables().size());
            Query createTable = MariaDBTableGenerator.generate(tableName, r, globalState.getSchema());
            globalState.executeStatement(createTable);
        }

        int[] nrRemaining = new int[Action.values().length];
        List<Action> actions = new ArrayList<>();
        int total = 0;
        for (int i = 0; i < Action.values().length; i++) {
            Action action = Action.values()[i];
            int nrPerformed = 0;
            switch (action) {
            case CHECKSUM:
            case CHECK_TABLE:
            case TRUNCATE:
            case REPAIR_TABLE:
            case OPTIMIZE:
            case ANALYZE_TABLE:
            case UPDATE:
            case CREATE_INDEX:
                nrPerformed = r.getInteger(0, 2);
                break;
            case SET:
                nrPerformed = 20;
                break;
            case INSERT:
                nrPerformed = r.getInteger(0, options.getMaxNumberInserts());
                break;
            default:
                throw new AssertionError(action);
            }
            if (nrPerformed != 0) {
                actions.add(action);
            }
            nrRemaining[action.ordinal()] = nrPerformed;
            total += nrPerformed;
        }
        while (total != 0) {
            Action nextAction = null;
            int selection = r.getInteger(0, total);
            int previousRange = 0;
            for (int i = 0; i < nrRemaining.length; i++) {
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
            Query query;
            try {
                switch (nextAction) {
                case CHECKSUM:
                    query = MariaDBTableAdminCommandGenerator.checksumTable(globalState.getSchema());
                    break;
                case CHECK_TABLE:
                    query = MariaDBTableAdminCommandGenerator.checkTable(globalState.getSchema());
                    break;
                case TRUNCATE:
                    query = MariaDBTruncateGenerator.truncate(globalState.getSchema());
                    break;
                case REPAIR_TABLE:
                    query = MariaDBTableAdminCommandGenerator.repairTable(globalState.getSchema());
                    break;
                case INSERT:
                    query = MariaDBInsertGenerator.insert(globalState.getSchema(), r);
                    break;
                case OPTIMIZE:
                    query = MariaDBTableAdminCommandGenerator.optimizeTable(globalState.getSchema());
                    break;
                case ANALYZE_TABLE:
                    query = MariaDBTableAdminCommandGenerator.analyzeTable(globalState.getSchema());
                    break;
                case UPDATE:
                    query = MariaDBUpdateGenerator.update(globalState.getSchema(), r);
                    break;
                case CREATE_INDEX:
                    query = MariaDBIndexGenerator.generate(globalState.getSchema());
                    break;
                case SET:
                    query = MariaDBSetGenerator.set(r, options);
                    break;
                default:
                    throw new AssertionError(nextAction);
                }
            } catch (IgnoreMeException e) {
                total--;
                continue;
            }
            try {
                globalState.executeStatement(query);
            } catch (Throwable t) {
                System.err.println(query.getQueryString());
                throw t;
            }
            total--;
        }
        //
        MariaDBNoRECOracle queryGenerator = new MariaDBNoRECOracle(globalState);
        for (int i = 0; i < options.getNrQueries(); i++) {
            try {
                queryGenerator.generateAndCheck();
            } catch (IgnoreMeException e) {

            }
            manager.incrementSelectQueryCount();
        }

    }

    public static class MariaDBGlobalState extends GlobalState<MariaDBOptions> {

        private MariaDBSchema schema;

        public MariaDBSchema getSchema() {
            if (schema == null) {
                try {
                    updateSchema();
                } catch (SQLException e) {
                    throw new AssertionError(e);
                }
            }
            return schema;
        }

        @Override
        protected void updateSchema() throws SQLException {
            this.schema = MariaDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    @Override
    public Connection createDatabase(MariaDBGlobalState globalState) throws SQLException {
        globalState.getState().statements
                .add(new QueryAdapter("DROP DATABASE IF EXISTS " + globalState.getDatabaseName()));
        globalState.getState().statements.add(new QueryAdapter("CREATE DATABASE " + globalState.getDatabaseName()));
        globalState.getState().statements.add(new QueryAdapter("USE " + globalState.getDatabaseName()));
        // /?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true
        String url = "jdbc:mariadb://localhost:3306";
        Connection con = DriverManager.getConnection(url, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword());
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + globalState.getDatabaseName());
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + globalState.getDatabaseName());
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + globalState.getDatabaseName());
        }
        return con;
    }

    @Override
    public String getDBMSName() {
        return "mariadb";
    }

    @Override
    public String toString() {
        return String.format("MariaDBProvider [database: %s]", databaseName);
    }

}
