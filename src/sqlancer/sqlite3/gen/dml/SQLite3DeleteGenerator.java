package sqlancer.sqlite3.gen.dml;

import java.util.Arrays;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractDeleteGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;

public final class SQLite3DeleteGenerator extends AbstractDeleteGenerator {

    private final SQLite3GlobalState globalState;
    private final SQLite3Table table;

    private SQLite3DeleteGenerator(SQLite3GlobalState globalState, SQLite3Table table) {
        this.globalState = globalState;
        this.table = table;
        this.canAffectSchema = true;
    }

    public static SQLQueryAdapter deleteContent(SQLite3GlobalState globalState) {
        SQLite3Table table = globalState.getSchema().getRandomTable(t -> !t.isView() && !t.isReadOnly());
        return deleteContent(globalState, table);
    }

    public static SQLQueryAdapter deleteContent(SQLite3GlobalState globalState, SQLite3Table table) {
        return new SQLite3DeleteGenerator(globalState, table).getStatement();
    }

    @Override
    public void buildStatement() {
        appendDeleteFromTable(table.getName());
        if (Randomly.getBoolean()) {
            appendWhereClause(SQLite3Visitor.asString(
                    new SQLite3ExpressionGenerator(globalState).setColumns(table.getColumns()).generateExpression()));
        }
        SQLite3Errors.addExpectedExpressionErrors(errors);
        errors.addAll(Arrays.asList("[SQLITE_ERROR] SQL error or missing database (foreign key mismatch",
                "[SQLITE_CONSTRAINT]  Abort due to constraint violation ",
                "[SQLITE_ERROR] SQL error or missing database (parser stack overflow)",
                "[SQLITE_ERROR] SQL error or missing database (no such table:", "no such column",
                "too many levels of trigger recursion", "cannot UPDATE generated column",
                "cannot INSERT into generated column", "A table in the database is locked",
                "load_extension() prohibited in triggers and views", "The database file is locked"));
        SQLite3Errors.addDeleteErrors(errors);
    }

}
