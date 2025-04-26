package sqlancer.common.oracle;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import sqlancer.MainOptions;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryError;
import sqlancer.common.schema.AbstractRelationalTable;

public abstract class DQEBase<S extends SQLGlobalState<?, ?>> {

    public static String COLUMN_ROWID = "rowId";
    public static String COLUMN_UPDATED = "updated";

    protected final S state;
    protected final ExpectedErrors selectExpectedErrors = new ExpectedErrors();
    protected final ExpectedErrors updateExpectedErrors = new ExpectedErrors();
    protected final ExpectedErrors deleteExpectedErrors = new ExpectedErrors();

    // protected final Main.StateLogger logger;
    protected final MainOptions options;
    protected final SQLConnection con;

    public DQEBase(S state) {
        this.state = state;
        this.con = state.getConnection();
        // this.logger = state.getLogger();
        this.options = state.getOptions();
    }

    public abstract void addAuxiliaryColumns(AbstractRelationalTable<?, ?, ?> table) throws SQLException;

    public void dropAuxiliaryColumns(AbstractRelationalTable<?, ?, ?> table) throws SQLException {
        String tableName = table.getName();
        String dropColumnRowId = String.format("ALTER TABLE %s DROP COLUMN %s", tableName, COLUMN_ROWID);
        new SQLQueryAdapter(dropColumnRowId).execute(state);
        String dropColumnUpdated = String.format("ALTER TABLE %s DROP COLUMN %s", tableName, COLUMN_UPDATED);
        new SQLQueryAdapter(dropColumnUpdated).execute(state);
    }

    public static class SQLQueryResult {

        private final Map<AbstractRelationalTable<?, ?, ?>, Set<String>> accessedRows; //Table name with respect rows
        private final List<SQLQueryError> queryErrors;

        public SQLQueryResult(Map<AbstractRelationalTable<?, ?, ?>, Set<String>> accessedRows, List<SQLQueryError> queryErrors) {
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

        public boolean hasErrors() {
            return !hasEmptyErrors();
        }

        public boolean hasSameErrors(SQLQueryResult that) {
            if (queryErrors.size() != that.getQueryErrors().size()) {
                return false;
            } else {
                Collections.sort(queryErrors);
                Collections.sort(that.getQueryErrors());
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

