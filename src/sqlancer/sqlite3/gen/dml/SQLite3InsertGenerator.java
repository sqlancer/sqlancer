package sqlancer.sqlite3.gen.dml;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3ToStringVisitor;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Constant;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;

public class SQLite3InsertGenerator {

    private final Randomly r;
    private final ExpectedErrors errors;
    private final SQLite3GlobalState globalState;

    public SQLite3InsertGenerator(SQLite3GlobalState globalState, Randomly r) {
        this.globalState = globalState;
        this.r = r;
        errors = new ExpectedErrors();
    }

    public static SQLQueryAdapter insertRow(SQLite3GlobalState globalState) throws SQLException {
        SQLite3Table randomTable = globalState.getSchema().getRandomTableOrBailout(t -> !t.isView() && !t.isReadOnly());
        return insertRow(globalState, randomTable);
    }

    public static SQLQueryAdapter insertRow(SQLite3GlobalState globalState, SQLite3Table randomTable) {
        SQLite3InsertGenerator generator = new SQLite3InsertGenerator(globalState, globalState.getRandomly());
        String query = generator.insertRow(randomTable);
        return new SQLQueryAdapter(query, generator.errors, true);
    }

    private String insertRow(SQLite3Table table) {
        SQLite3Errors.addInsertUpdateErrors(errors);
        errors.add("[SQLITE_FULL]");
        // // TODO: also check if the table is really missing (caused by a DROP TABLE)
        errors.add("ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint"); // trigger
        errors.add("values were supplied"); // trigger
        errors.add("Data type mismatch (datatype mismatch)"); // trigger

        errors.add("load_extension() prohibited in triggers and views");
        SQLite3Errors.addInsertNowErrors(errors);
        SQLite3Errors.addExpectedExpressionErrors(errors);
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT ");
        if (Randomly.getBoolean()) {
            sb.append("OR IGNORE "); // TODO: try to generate REPLACE
        } else {
            String fromOptions = Randomly.fromOptions("OR REPLACE ", "OR ABORT ", "OR FAIL ", "OR ROLLBACK ");
            sb.append(fromOptions);
        }
        boolean defaultValues = false;
        sb.append("INTO ");
        sb.append(table.getName());
        List<SQLite3Column> cols = table.getRandomNonEmptyColumnSubset();
        if (cols.size() != table.getColumns().size() || Randomly.getBoolean()) {
            sb.append("(");
            appendColumnNames(cols, sb);
            sb.append(")");
        } else {
            // If the column-name list after table-name is omitted then the number of values
            // inserted into each row must be the same as the number of columns in the
            // table.
            cols = table.getColumns(); // get them again in sorted order
            assert cols.size() == table.getColumns().size();
        }
        sb.append(" VALUES ");
        int nrRows = 1 + Randomly.smallNumber();
        appendNrValues(sb, cols, nrRows);
        boolean columnsInConflictClause = Randomly.getBoolean();
        if (!defaultValues && Randomly.getBooleanWithSmallProbability() && !table.isVirtual()) {
            sb.append(" ON CONFLICT");
            if (columnsInConflictClause) {
                sb.append("(");
                sb.append(table.getRandomNonEmptyColumnSubset().stream().map(c -> c.getName())
                        .collect(Collectors.joining(", ")));
                sb.append(")");
                errors.add("ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint");
            }
            sb.append(" DO ");
            if (Randomly.getBoolean() || !columnsInConflictClause) {
                sb.append("NOTHING");
            } else {
                sb.append("UPDATE SET ");
                List<SQLite3Column> columns = table.getRandomNonEmptyColumnSubset();
                for (int i = 0; i < columns.size(); i++) {
                    if (i != 0) {
                        sb.append(", ");
                    }
                    sb.append(columns.get(i).getName());
                    sb.append("=");
                    if (Randomly.getBoolean()) {
                        sb.append(
                                SQLite3Visitor.asString(SQLite3ExpressionGenerator.getRandomLiteralValue(globalState)));
                    } else {
                        if (Randomly.getBoolean()) {
                            sb.append("excluded.");
                        }
                        sb.append(table.getRandomColumn().getName());
                    }

                }
                errors.add("Abort due to constraint violation");
                errors.add("Data type mismatch (datatype mismatch)");
                if (Randomly.getBoolean()) {
                    sb.append(" WHERE ");
                    sb.append(SQLite3Visitor.asString(new SQLite3ExpressionGenerator(globalState)
                            .setColumns(table.getColumns()).generateExpression()));
                }
            }
        }
        return sb.toString();
    }

    private void appendNrValues(StringBuilder sb, List<SQLite3Column> columns, int nrValues) {
        for (int i = 0; i < nrValues; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append("(");
            appendValue(sb, columns);
            sb.append(")");
        }
    }

    private void appendValue(StringBuilder sb, List<SQLite3Column> columns) {
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            SQLite3Expression literal;
            if (columns.get(i).isIntegerPrimaryKey()) {
                literal = SQLite3Constant.createIntConstant(r.getInteger(0, 1000));
            } else {
                if (Randomly.getBooleanWithSmallProbability()) {
                    literal = new SQLite3ExpressionGenerator(globalState).generateExpression();
                } else {
                    literal = SQLite3ExpressionGenerator.getRandomLiteralValue(globalState);
                }
            }
            SQLite3ToStringVisitor visitor = new SQLite3ToStringVisitor();
            visitor.visit(literal);
            sb.append(visitor.get());
        }
    }

    private static List<SQLite3Column> appendColumnNames(List<SQLite3Column> columns, StringBuilder sb) {
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
        }
        return columns;
    }

}
