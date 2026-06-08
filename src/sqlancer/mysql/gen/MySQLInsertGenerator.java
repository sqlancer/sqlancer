package sqlancer.mysql.gen;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.MySQLErrors;
import sqlancer.mysql.MySQLBugs;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLDataType;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLConstant;

public class MySQLInsertGenerator {

    private final MySQLTable table;
    private final StringBuilder sb = new StringBuilder();
    private final ExpectedErrors errors = new ExpectedErrors();
    private final MySQLGlobalState globalState;

    public MySQLInsertGenerator(MySQLGlobalState globalState, MySQLTable table) {
        this.globalState = globalState;
        this.table = table;
    }

    public static SQLQueryAdapter insertRow(MySQLGlobalState globalState) throws SQLException {
        MySQLTable table = globalState.getSchema().getRandomTable();
        return insertRow(globalState, table);
    }

    public static SQLQueryAdapter insertRow(MySQLGlobalState globalState, MySQLTable table) throws SQLException {
        if (Randomly.getBoolean()) {
            return new MySQLInsertGenerator(globalState, table).generateInsert();
        } else {
            return new MySQLInsertGenerator(globalState, table).generateReplace();
        }
    }

    private SQLQueryAdapter generateReplace() {
        sb.append("REPLACE");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("LOW_PRIORITY", "DELAYED"));
        }
        return generateInto();

    }

    private SQLQueryAdapter generateInsert() {
        sb.append("INSERT");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("LOW_PRIORITY", "DELAYED", "HIGH_PRIORITY"));
        }
        if (Randomly.getBoolean()) {
            sb.append(" IGNORE");
        }
        return generateInto();
    }

    private SQLQueryAdapter generateInto() {
        sb.append(" INTO ");
        sb.append(table.getName());
        List<MySQLColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append("(");
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(") ");
        sb.append("VALUES");
        MySQLExpressionGenerator gen = new MySQLExpressionGenerator(globalState);
        int nrRows;
        if (Randomly.getBoolean()) {
            nrRows = 1;
        } else {
            nrRows = 1 + Randomly.smallNumber();
        }
        for (int row = 0; row < nrRows; row++) {
            if (row != 0) {
                sb.append(", ");
            }
            sb.append("(");
            for (int c = 0; c < columns.size(); c++) {
                if (c != 0) {
                    sb.append(", ");
                }
                MySQLExpression constExpr;
                // Bug workaround: for integer columns, reject numeric values that round to 1. Regenerate until valid.
                if (MySQLBugs.bug120711 && columns.get(c).getType() == MySQLDataType.INT) {
                    while (true) {
                        constExpr = gen.generateConstant();
                        boolean reject = false;
                        if (constExpr instanceof MySQLConstant.MySQLIntConstant) {
                            long value = ((MySQLConstant.MySQLIntConstant) constExpr).getInt();
                            reject = value == 1;
                        } else if (constExpr instanceof MySQLConstant.MySQLDoubleConstant) {
                            double value = ((MySQLConstant.MySQLDoubleConstant) constExpr).getDouble();
                            reject = value >= 0.5 && value < 1.5;
                        } else if (constExpr instanceof MySQLConstant.MySQLTextConstant) { // reject strings, which may be implicitly cast to 1
                            reject = true;
                        }
                        if (!reject) {
                            break;
                        }
                    }
                // Bug workaround: for decimal columns, reject values that round to 0. Regenerate until valid.
                } else if (MySQLBugs.bug120710 && columns.get(c).getType() == MySQLDataType.DECIMAL) {
                    while (true) {
                        constExpr = gen.generateConstant();
                        boolean reject = false;
                        if (constExpr instanceof MySQLConstant.MySQLIntConstant) {
                            long value = ((MySQLConstant.MySQLIntConstant) constExpr).getInt();
                            reject = value == 0;
                        } else if (constExpr instanceof MySQLConstant.MySQLDoubleConstant) {
                            double value = ((MySQLConstant.MySQLDoubleConstant) constExpr).getDouble();
                            reject = value >= -0.5 && value < 0.5;
                        } else if (constExpr instanceof MySQLConstant.MySQLTextConstant) { // reject strings, which may be implicitly cast to 0
                            reject = true;
                        }
                        if (!reject) {
                            break;
                        }
                    }
                } else {
                    constExpr = gen.generateConstant();
                }
                sb.append(MySQLVisitor.asString(constExpr));
            }
            sb.append(")");
        }
        MySQLErrors.addInsertUpdateErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
