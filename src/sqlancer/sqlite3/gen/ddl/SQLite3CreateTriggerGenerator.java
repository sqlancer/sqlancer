package sqlancer.sqlite3.gen.ddl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.gen.dml.SQLite3DeleteGenerator;
import sqlancer.sqlite3.gen.dml.SQLite3InsertGenerator;
import sqlancer.sqlite3.gen.dml.SQLite3UpdateGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;

public final class SQLite3CreateTriggerGenerator {

    private SQLite3CreateTriggerGenerator() {
    }

    private enum OnAction {
        INSERT, DELETE, UPDATE
    }

    private enum TriggerAction {
        INSERT, DELETE, UPDATE, RAISE
    }

    public static SQLQueryAdapter create(SQLite3GlobalState globalState) throws SQLException {
        SQLite3Schema s = globalState.getSchema();
        StringBuilder sb = new StringBuilder();
        SQLite3Table table = s.getRandomTableOrBailout(t -> !t.isVirtual());
        sb.append("CREATE");
        if (table.isTemp()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("TEMP", "TEMPORARY"));
        }
        sb.append(" TRIGGER");
        sb.append(" IF NOT EXISTS ");
        sb.append("tr");
        sb.append(Randomly.smallNumber());
        sb.append(" ");
        if (table.isView()) {
            sb.append("INSTEAD OF");
        } else {
            sb.append(Randomly.fromOptions("BEFORE", "AFTER"));
        }
        sb.append(" ");

        OnAction randomAction = Randomly.fromOptions(OnAction.values());
        switch (randomAction) {
        case INSERT:
            sb.append("INSERT ON ");
            break;
        case DELETE:
            sb.append("DELETE ON ");
            break;
        case UPDATE:
            sb.append("UPDATE ");
            if (Randomly.getBoolean()) {
                sb.append("OF ");
                for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
                    if (i != 0) {
                        sb.append(", ");
                    }
                    sb.append(table.getRandomColumn().getName());
                }
                sb.append(" ");
            }
            sb.append("ON ");
            break;
        default:
            throw new AssertionError();
        }
        appendTableNameAndWhen(globalState, sb, table);

        SQLite3Table randomActionTable = s.getRandomTableNoViewOrBailout();
        sb.append(" BEGIN ");
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            switch (Randomly.fromOptions(TriggerAction.values())) {
            case DELETE:
                sb.append(SQLite3DeleteGenerator.deleteContent(globalState, randomActionTable));
                break;
            case INSERT:
                sb.append(getQueryString(s, globalState));
                break;
            case UPDATE:
                sb.append(SQLite3UpdateGenerator.updateRow(globalState, randomActionTable));
                break;
            case RAISE:
                sb.append("SELECT RAISE(");
                if (Randomly.getBoolean()) {
                    sb.append("IGNORE");
                } else {
                    sb.append(Randomly.fromOptions("ROLLBACK", "ABORT", "FAIL"));
                    sb.append(", 'asdf'");
                }
                sb.append(")");
                break;
            default:
                throw new AssertionError();
            }
            sb.append(";");
        }
        sb.append("END");

        return new SQLQueryAdapter(sb.toString(),
                ExpectedErrors.from("parser stack overflow", "unsupported frame specification"));
    }

    private static void appendTableNameAndWhen(SQLite3GlobalState globalState, StringBuilder sb, SQLite3Table table) {
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" FOR EACH ROW ");
        }
        if (Randomly.getBoolean()) {
            sb.append(" WHEN ");
            sb.append(SQLite3Visitor.asString(
                    new SQLite3ExpressionGenerator(globalState).setColumns(table.getColumns()).generateExpression()));
        }
    }

    private static String getQueryString(SQLite3Schema s, SQLite3GlobalState globalState) throws SQLException {
        String q;
        do {
            q = SQLite3InsertGenerator.insertRow(globalState, getTableNotEqualsTo(s, s.getRandomTableNoViewOrBailout()))
                    .getQueryString();
        } while (q.contains("DEFAULT VALUES"));
        return q;
    }

    private static SQLite3Table getTableNotEqualsTo(SQLite3Schema s, SQLite3Table table) {
        List<SQLite3Table> tables = new ArrayList<>(s.getDatabaseTablesWithoutViews());
        tables.remove(table);
        if (tables.isEmpty()) {
            throw new IgnoreMeException();
        }
        return Randomly.fromList(tables);
    }

}
