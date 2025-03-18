package sqlancer.mysql.gen;

import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.MySQLErrors;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLVisitor;

public class MySQLDeleteGenerator {

    private final StringBuilder sb = new StringBuilder();
    private final MySQLGlobalState globalState;

    public MySQLDeleteGenerator(MySQLGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter delete(MySQLGlobalState globalState) {
        return new MySQLDeleteGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        MySQLTable randomTable = globalState.getSchema().getRandomTable();
        MySQLExpressionGenerator gen = new MySQLExpressionGenerator(globalState).setColumns(randomTable.getColumns());
        ExpectedErrors errors = new ExpectedErrors();
        sb.append("DELETE");
        if (Randomly.getBoolean()) {
            sb.append(" LOW_PRIORITY");
        }
        if (Randomly.getBoolean()) {
            sb.append(" QUICK");
        }
        if (Randomly.getBoolean()) {
            sb.append(" IGNORE");
        }
        // TODO: support partitions
        sb.append(" FROM ");
        sb.append(randomTable.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(MySQLVisitor.asString(gen.generateExpression()));
            MySQLErrors.addExpressionErrors(errors);
        }
        errors.addAll(Arrays.asList("doesn't have this option",
                "Truncated incorrect DOUBLE value" /*
                                                    * ignore as a workaround for https://bugs.mysql.com/bug.php?id=95997
                                                    */, "Truncated incorrect INTEGER value",
                "Truncated incorrect DECIMAL value", "Data truncated for functional index"));
        // TODO: support ORDER BY
        if (Randomly.getBoolean() && !randomTable.getColumns().isEmpty()) {
            sb.append(" ORDER BY ");

            List<MySQLColumn> columns = randomTable.getColumns();
            List<MySQLColumn> selectedColumns = Randomly.nonEmptySubset(columns);

            boolean firstColumn = true;
            for (MySQLColumn column : selectedColumns) {
                if (!firstColumn) {
                    sb.append(", ");
                }
                firstColumn = false;

                sb.append(column.getName());

                if (Randomly.getBoolean()) {
                    if (Randomly.getBoolean()) {
                        sb.append(" ASC");
                    } else {
                        sb.append(" DESC");
                    }
                }
            }
        }

        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
