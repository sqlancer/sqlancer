package sqlancer;

import java.sql.Connection;

import sqlancer.Main.QueryManager;
import sqlancer.Main.StateLogger;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;

/**
 * Represents a global state that is valid for a testing session on a given database.
 *
 * @param <O>
 *            the option parameter
 * @param <S>
 *            the schema parameter
 */
public abstract class GlobalState<O extends DBMSSpecificOptions<?>, S extends AbstractSchema<?>> {

    private Connection con;
    private Randomly r;
    private MainOptions options;
    private O dmbsSpecificOptions;
    private S schema;
    private StateLogger logger;
    private StateToReproduce state;
    private QueryManager manager;
    private String databaseName;

    public void setConnection(Connection con) {
        this.con = con;
    }

    @SuppressWarnings("unchecked")
    public void setDmbsSpecificOptions(Object dmbsSpecificOptions) {
        this.dmbsSpecificOptions = (O) dmbsSpecificOptions;
    }

    public O getDmbsSpecificOptions() {
        return dmbsSpecificOptions;
    }

    public Connection getConnection() {
        return con;
    }

    public void setRandomly(Randomly r) {
        this.r = r;
    }

    public Randomly getRandomly() {
        return r;
    }

    public MainOptions getOptions() {
        return options;
    }

    public void setMainOptions(MainOptions options) {
        this.options = options;
    }

    public void setStateLogger(StateLogger logger) {
        this.logger = logger;
    }

    public StateLogger getLogger() {
        return logger;
    }

    public void setState(StateToReproduce state) {
        this.state = state;
    }

    public StateToReproduce getState() {
        return state;
    }

    public QueryManager getManager() {
        return manager;
    }

    public void setManager(QueryManager manager) {
        this.manager = manager;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    private ExecutionTimer executePrologue(Query q) throws Exception {
        boolean logExecutionTime = getOptions().logExecutionTime();
        ExecutionTimer timer = null;
        if (logExecutionTime) {
            timer = new ExecutionTimer().start();
        }
        if (getOptions().printAllStatements()) {
            System.out.println(q.getQueryString());
        }
        if (getOptions().logEachSelect()) {
            if (logExecutionTime) {
                getLogger().writeCurrentNoLineBreak(q.getQueryString());
            } else {
                getLogger().writeCurrent(q.getQueryString());
            }
        }
        return timer;
    }

    private void executeEpilogue(Query q, boolean success, ExecutionTimer timer) throws Exception {
        boolean logExecutionTime = getOptions().logExecutionTime();
        if (success && getOptions().printSucceedingStatements()) {
            System.out.println(q.getQueryString());
        }
        if (logExecutionTime) {
            getLogger().writeCurrent(" -- " + timer.end().asString());
        }
        if (q.couldAffectSchema()) {
            updateSchema();
        }
    }

    public boolean executeStatement(Query q, String... fills) throws Exception {
        ExecutionTimer timer = executePrologue(q);
        boolean success = manager.execute(q, fills);
        executeEpilogue(q, success, timer);
        return success;
    }

    public SQLancerResultSet executeStatementAndGet(Query q, String... fills) throws Exception {
        ExecutionTimer timer = executePrologue(q);
        SQLancerResultSet result = manager.executeAndGet(q, fills);
        boolean success = result != null;
        if (success) {
            result.registerEpilogue(() -> {
                try {
                    executeEpilogue(q, success, timer);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
        }
        return result;
    }

    public S getSchema() {
        if (schema == null) {
            try {
                updateSchema();
            } catch (Exception e) {
                throw new AssertionError();
            }
        }
        return schema;
    }

    protected void setSchema(S schema) {
        this.schema = schema;
    }

    public void updateSchema() throws Exception {
        setSchema(readSchema());
        for (AbstractTable<?, ?> table : schema.getDatabaseTables()) {
            table.recomputeCount();
        }
    }

    protected abstract S readSchema() throws Exception;

}
