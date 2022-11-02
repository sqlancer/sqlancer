package sqlancer.sqlite3.gen.ddl;

import java.sql.SQLException;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Provider;
import sqlancer.sqlite3.SQLite3ToStringVisitor;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.gen.SQLite3Common;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;

// see https://www.sqlite.org/lang_createindex.html
public class SQLite3IndexGenerator {

    private final ExpectedErrors errors = new ExpectedErrors();
    private final SQLite3GlobalState globalState;

    public static SQLQueryAdapter insertIndex(SQLite3GlobalState globalState) throws SQLException {
        return new SQLite3IndexGenerator(globalState).create();
    }

    public SQLite3IndexGenerator(SQLite3GlobalState globalState) throws SQLException {
        this.globalState = globalState;
    }

    private SQLQueryAdapter create() throws SQLException {
        SQLite3Table t = globalState.getSchema()
                .getRandomTableOrBailout(tab -> !tab.isView() && !tab.isVirtual() && !tab.isReadOnly());
        String q = createIndex(t, t.getColumns());
        errors.add("no such collation sequence: UINT");
        errors.add("[SQLITE_ERROR] SQL error or missing database (parser stack overflow)");
        errors.add("subqueries prohibited in index expressions");
        errors.add("subqueries prohibited in partial index WHERE clauses");
        errors.add("non-deterministic use of time() in an index");
        errors.add("non-deterministic use of strftime() in an index");
        errors.add("non-deterministic use of julianday() in an index");
        errors.add("non-deterministic use of date() in an index");
        errors.add("non-deterministic use of datetime() in an index");
        errors.add("The database file is locked");
        SQLite3Errors.addExpectedExpressionErrors(errors);
        if (!SQLite3Provider.mustKnowResult) {
            // can only happen when PRAGMA case_sensitive_like=ON;
            errors.add("non-deterministic functions prohibited");
        }

        /*
         * Strings in single quotes are sometimes interpreted as column names. Since we found an issue with double
         * quotes, they can no longer be used (see https://sqlite.org/src/info/9b78184b). Single quotes are interpreted
         * as column names in certain contexts (see
         * https://www.mail-archive.com/sqlite-users@mailinglists.sqlite.org/msg115014.html).
         */
        errors.add("[SQLITE_ERROR] SQL error or missing database (no such column:");
        return new SQLQueryAdapter(q, errors, true);
    }

    private String createIndex(SQLite3Table t, List<SQLite3Column> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE");
        if (Randomly.getBoolean()) {
            errors.add("UNIQUE constraint failed ");
            sb.append(" UNIQUE");
        }
        sb.append(" INDEX");
        if (Randomly.getBoolean()) {
            sb.append(" IF NOT EXISTS");
        } else {
            errors.add("already exists");
        }
        sb.append(" ");
        sb.append(SQLite3Common.getFreeIndexName(globalState.getSchema()));
        sb.append(" ON ");
        sb.append(t.getName());
        sb.append("(");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(",");
            }
            SQLite3Expression expr = new SQLite3ExpressionGenerator(globalState).setColumns(columns).deterministicOnly()
                    .generateExpression();
            SQLite3ToStringVisitor visitor = new SQLite3ToStringVisitor();
            visitor.fullyQualifiedNames = false;
            visitor.visit(expr);
            sb.append(visitor.get());
            if (Randomly.getBoolean()) {
                sb.append(SQLite3Common.getRandomCollate());
            }
            appendPotentialOrdering(sb);
        }
        sb.append(")");
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            SQLite3Expression expr = new SQLite3ExpressionGenerator(globalState).setColumns(columns).deterministicOnly()
                    .generateExpression();
            SQLite3ToStringVisitor visitor = new SQLite3ToStringVisitor();
            visitor.fullyQualifiedNames = false;
            visitor.visit(expr);
            sb.append(visitor.get());
        }
        return sb.toString();
    }

    /*
     * Appends ASC, DESC, or nothing.
     */
    private void appendPotentialOrdering(StringBuilder sb) {
        if (Randomly.getBoolean()) {
            if (Randomly.getBoolean()) {
                sb.append(" ASC");
            } else {
                sb.append(" DESC");
            }
        }
    }

}
