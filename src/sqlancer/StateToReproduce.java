package sqlancer;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.clickhouse.ast.ClickHouseConstant;
import sqlancer.clickhouse.ast.ClickHouseExpression;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.ast.PostgresConstant;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.sqlite3.ast.SQLite3Constant;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;

public class StateToReproduce {

    private final List<Query> statements = new ArrayList<>();

    private final String databaseName;

    public String databaseVersion;

    protected long seedValue;

    public String values;

    String exception;

    public String queryTargetedTablesString;

    public String queryTargetedColumnsString;

    public OracleRunReproductionState localState;

    public StateToReproduce(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getException() {
        return exception;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getDatabaseVersion() {
        return databaseVersion;
    }

    /**
     * Logs the statement string without executing the corresponding statement.
     *
     * @param queryString
     *            the query string to be logged
     */
    public void logStatement(String queryString) {
        if (queryString == null) {
            throw new IllegalArgumentException();
        }
        logStatement(new QueryAdapter(queryString));
    }

    /**
     * Logs the statement without executing it.
     *
     * @param query
     *            the query to be logged
     */
    public void logStatement(Query query) {
        if (query == null) {
            throw new IllegalArgumentException();
        }
        statements.add(query);
    }

    public List<Query> getStatements() {
        return Collections.unmodifiableList(statements);
    }

    public long getSeedValue() {
        return seedValue;
    }

    /**
     * Returns a local state in which a test oracle can save useful information about a single run. If the local state
     * is closed without indicating access to it, the local statements will be added to the global state.
     *
     * @return
     */
    public OracleRunReproductionState getLocalState() {
        return localState;
    }

    public static class MySQLStateToReproduce extends StateToReproduce {

        public Map<MySQLColumn, MySQLConstant> randomRowValues;

        public MySQLExpression whereClause;

        public String queryThatSelectsRow;

        public MySQLStateToReproduce(String databaseName) {
            super(databaseName);
        }

        public Map<MySQLColumn, MySQLConstant> getRandomRowValues() {
            return randomRowValues;
        }

        public MySQLExpression getWhereClause() {
            return whereClause;
        }

    }

    public static class SQLite3StateToReproduce extends StateToReproduce {
        public Map<SQLite3Column, SQLite3Constant> randomRowValues;

        public SQLite3Expression whereClause;

        public SQLite3StateToReproduce(String databaseName) {
            super(databaseName);
        }

        public Map<SQLite3Column, SQLite3Constant> getRandomRowValues() {
            return randomRowValues;
        }

        public SQLite3Expression getWhereClause() {
            return whereClause;
        }

    }

    public static class PostgresStateToReproduce extends StateToReproduce {

        public Map<PostgresColumn, PostgresConstant> randomRowValues;

        public PostgresExpression whereClause;

        public String queryThatSelectsRow;

        public PostgresStateToReproduce(String databaseName) {
            super(databaseName);
        }

        public Map<PostgresColumn, PostgresConstant> getRandomRowValues() {
            return randomRowValues;
        }

        public PostgresExpression getWhereClause() {
            return whereClause;
        }

    }

    public static class ClickHouseStateToReproduce extends StateToReproduce {

        public Map<ClickHouseSchema.ClickHouseColumn, ClickHouseConstant> randomRowValues;

        public ClickHouseExpression whereClause;

        public String queryThatSelectsRow;

        public ClickHouseStateToReproduce(String databaseName) {
            super(databaseName);
        }

        public Map<ClickHouseSchema.ClickHouseColumn, ClickHouseConstant> getRandomRowValues() {
            return randomRowValues;
        }

        public ClickHouseExpression getWhereClause() {
            return whereClause;
        }

    }

    /**
     * State information that is logged if the test oracle finds a bug or if an exception is thrown.
     */
    public class OracleRunReproductionState implements Closeable {

        private final List<Query> statements = new ArrayList<>();

        public boolean success;

        public OracleRunReproductionState() {
            StateToReproduce.this.localState = this;
        }

        public void executedWithoutError() {
            this.success = true;
        }

        public void log(Query q) {
            statements.add(q);
        }

        public void log(String s) {
            statements.add(new QueryAdapter(s));
        }

        @Override
        public void close() {
            if (!success) {
                StateToReproduce.this.statements.addAll(statements);
            }

        }

    }

    public OracleRunReproductionState createLocalState() {
        return new OracleRunReproductionState();
    }

}
