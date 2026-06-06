package sqlancer.common.oracle;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import sqlancer.Main;
import sqlancer.MainOptions;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryError;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractTables;

/*
 * In DBMSs, SELECT, UPDATE and DELETE queries utilize predicates (i.e., WHERE clauses) to specify which rows to retrieve, update or delete, respectively.
 * If they use the same predicate φ, they should access the same rows in a database.
 * Ideally, DBMSs can adopt the same implementations for predicate evaluation in SELECT, UPDATE and DELETE queries.
 * However, a DBMS usually adopts different implementations for predicate evaluation in SELECT, UPDATE and DELETE queries due to various optimization choices.
 * Inconsistent implementations for predicate evaluation among these queries can cause SELECT, UPDATE and DELETE queries with the same predicate φ to access different rows.
 *
 *
 * Inspired by this key observation, we propose Differential Query Execution(DQE), a novel and general approach to detect logic bugs in SELECT, UPDATE and DELETE queries.
 * DQE solves the test oracle problem by executing SELECT, UPDATE and DELETE queries with the same predicate φ, and observing inconsistencies among their execution results.
 * For example, if a row that is updated by an UPDATE query with a predicate φ does not appear in the query result of a SELECT query with the same predicate φ, a logic bug is detected in the target DBMS.
 * The key challenge of DQE is to automatically obtain the accessed rows for a given SELECT, UPDATE or DELETE query.
 * To address this challenge, we append two extra columns to each table in a database, to uniquely identify each row and track whether a row has been modified, respectively.
 * We further rewrite SELECT and UPDATE queries to identify their accessed rows.
 *
 * more information see [DQE paper](https://ieeexplore.ieee.org/document/10172736)
 */

public abstract class DQEBase<S extends SQLGlobalState<?, ?>> {

    public static final String COLUMN_ROWID = "rowId";
    public static final String COLUMN_UPDATED = "updated";

    protected final S state;
    protected final ExpectedErrors selectExpectedErrors = new ExpectedErrors();
    protected final ExpectedErrors updateExpectedErrors = new ExpectedErrors();
    protected final ExpectedErrors deleteExpectedErrors = new ExpectedErrors();

    protected final Main.StateLogger logger;
    protected final MainOptions options;
    protected final SQLConnection con;

    public DQEBase(S state) {
        this.state = state;
        this.con = state.getConnection();
        this.logger = state.getLogger();
        this.options = state.getOptions();
    }

    public abstract String generateSelectStatement(AbstractTables<?, ?> tables, String tableName,
            String whereClauseStr);

    public abstract String generateUpdateStatement(AbstractTables<?, ?> tables, String tableName,
            String whereClauseStr);

    public abstract String generateDeleteStatement(String tableName, String whereClauseStr);

    // Add auxiliary columns to the database A abstract method, subclasses need to implement it.
    public abstract void addAuxiliaryColumns(AbstractRelationalTable<?, ?, ?> table) throws SQLException;

    public void dropAuxiliaryColumns(AbstractRelationalTable<?, ?, ?> table) throws SQLException {
        String tableName = table.getName();
        String dropColumnRowId = String.format("ALTER TABLE %s DROP COLUMN %s", tableName, COLUMN_ROWID);
        new SQLQueryAdapter(dropColumnRowId).execute(state);
        String dropColumnUpdated = String.format("ALTER TABLE %s DROP COLUMN %s", tableName, COLUMN_UPDATED);
        new SQLQueryAdapter(dropColumnUpdated).execute(state);
    }

    // This interface is to record Error code
    public interface UpdateErrorCodes {

    }

    public interface ErrorCodeStrategy {
        Set<Integer> getUpdateSpecificErrorCodes();

        Set<Integer> getDeleteSpecificErrorCodes();

    }

    /**
     * The core idea of DQE is that the SELECT, UPDATE and DELETE queries with the same predicate φ should access the
     * same rows. If these queries access different rows, DQE reveals a potential logic bug in the target DBMS.
     */
    public static class SQLQueryResult {

        private final Map<AbstractRelationalTable<?, ?, ?>, Set<String>> accessedRows; // Table name with respect rows
        private final List<SQLQueryError> queryErrors;

        public SQLQueryResult(Map<AbstractRelationalTable<?, ?, ?>, Set<String>> accessedRows,
                List<SQLQueryError> queryErrors) {
            this.accessedRows = accessedRows;
            this.queryErrors = queryErrors;
        }

        public Map<AbstractRelationalTable<?, ?, ?>, Set<String>> getAccessedRows() {
            return accessedRows;
        }

        public List<SQLQueryError> getQueryErrors() {
            return queryErrors;
        }

        public boolean hasEmptyErrors() {
            return queryErrors.isEmpty();
        }

        public boolean hasSameErrors(SQLQueryResult that) {
            if (queryErrors.size() != that.getQueryErrors().size()) {
                return false;
            } else {
                for (int i = 0; i < queryErrors.size(); i++) {
                    if (!queryErrors.get(i).equals(that.getQueryErrors().get(i))) {
                        return false;
                    }
                }
            }
            return true;
        }

        public boolean hasAccessedRows() {
            if (accessedRows.isEmpty()) {
                return false;
            }
            for (Set<String> accessedRow : accessedRows.values()) {
                if (!accessedRow.isEmpty()) {
                    return true;
                }
            }
            return false;
        }

        public boolean hasSameAccessedRows(SQLQueryResult that) {
            return accessedRows.equals(that.getAccessedRows());
        }

    }
}
