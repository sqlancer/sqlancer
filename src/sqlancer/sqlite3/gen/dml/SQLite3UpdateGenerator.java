package sqlancer.sqlite3.gen.dml;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractUpdateGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Constant;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;

public class SQLite3UpdateGenerator extends AbstractUpdateGenerator<SQLite3Column> {

    private final SQLite3GlobalState globalState;
    private final Randomly r;

    public SQLite3UpdateGenerator(SQLite3GlobalState globalState, Randomly r) {
        this.globalState = globalState;
        this.r = r;
    }

    public static SQLQueryAdapter updateRow(SQLite3GlobalState globalState) {
        SQLite3Table randomTableNoViewOrBailout = globalState.getSchema()
                .getRandomTableOrBailout(t -> !t.isView() && !t.isReadOnly());
        return updateRow(globalState, randomTableNoViewOrBailout);
    }

    public static SQLQueryAdapter updateRow(SQLite3GlobalState globalState, SQLite3Table table) {
        SQLite3UpdateGenerator generator = new SQLite3UpdateGenerator(globalState, globalState.getRandomly());
        return generator.generate(table);
    }

    private SQLQueryAdapter generate(SQLite3Table table) {
        List<SQLite3Column> columnsToUpdate = Randomly.nonEmptySubsetPotentialDuplicates(table.getColumns());
        sb.append("UPDATE ");
        if (Randomly.getBoolean()) {
            sb.append("OR IGNORE ");
        } else {
            if (Randomly.getBoolean()) {
                String fromOptions = Randomly.fromOptions("OR ROLLBACK", "OR ABORT", "OR REPLACE", "OR FAIL");
                sb.append(fromOptions);
                sb.append(" ");
            }
            errors.add("[SQLITE_CONSTRAINT]");
        }
        // TODO Beginning in SQLite version 3.15.0 (2016-10-14), an assignment in the
        // SET clause can be a parenthesized list of column names on the left and a row
        // value of the same size on the right.

        sb.append(table.getName());
        sb.append(" SET ");
        if (Randomly.getBoolean()) {
            sb.append("(");
            sb.append(columnsToUpdate.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
            sb.append(")");
            sb.append("=");
            sb.append("(");
            for (int i = 0; i < columnsToUpdate.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                updateValue(columnsToUpdate.get(i));
            }
            sb.append(")");
            // row values
        } else {
            updateColumns(columnsToUpdate);
        }

        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            String whereClause = SQLite3Visitor.asString(
                    new SQLite3ExpressionGenerator(globalState).setColumns(table.getColumns()).generateExpression());
            sb.append(whereClause);
        }

        // ORDER BY and LIMIT are only supported by enabling a compile-time option
        // List<Expression> expressions = QueryGenerator.generateOrderBy(table.getColumns());
        // if (!expressions.isEmpty()) {
        // sb.append(" ORDER BY ");
        // sb.append(expressions.stream().map(e -> SQLite3Visitor.asString(e)).collect(Collectors.joining(", ")));
        // }

        SQLite3Errors.addInsertUpdateErrors(errors);

        errors.add("[SQLITE_ERROR] SQL error or missing database (parser stack overflow)");
        errors.add(
                "[SQLITE_ERROR] SQL error or missing database (second argument to likelihood() must be a constant between 0.0 and 1.0)");
        // for views
        errors.add("ORDER BY term out of range");
        errors.add("unknown function: json_type");

        SQLite3Errors.addInsertNowErrors(errors);
        SQLite3Errors.addExpectedExpressionErrors(errors);
        SQLite3Errors.addDeleteErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true /* column could have an ON UPDATE clause */);

    }

    @Override
    protected void updateValue(SQLite3Column column) {
        if (column.isIntegerPrimaryKey()) {
            sb.append(SQLite3Visitor.asString(SQLite3Constant.createIntConstant(r.getInteger())));
        } else {
            sb.append(SQLite3Visitor.asString(SQLite3ExpressionGenerator.getRandomLiteralValue(globalState)));
        }
    }

}
