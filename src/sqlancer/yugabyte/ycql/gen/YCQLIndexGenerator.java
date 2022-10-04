package sqlancer.yugabyte.ycql.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ycql.YCQLProvider.YCQLGlobalState;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLColumn;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLTable;
import sqlancer.yugabyte.ycql.YCQLToStringVisitor;
import sqlancer.yugabyte.ycql.ast.YCQLExpression;

public final class YCQLIndexGenerator {

    private YCQLIndexGenerator() {
    }

    public static SQLQueryAdapter getQuery(YCQLGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (Randomly.getBoolean()) {
            errors.add("Cant create unique index, table contains duplicate data on indexed column(s)");
            sb.append("UNIQUE ");
        }
        sb.append("INDEX ");
        sb.append(Randomly.fromOptions("i0", "i1", "i2", "i3", "i4"));
        sb.append(" ON ");
        YCQLTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        sb.append("(");
        List<YCQLColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
        }
        sb.append(")");
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            Node<YCQLExpression> expr = new YCQLExpressionGenerator(globalState).setColumns(table.getColumns())
                    .generateExpression();
            sb.append(YCQLToStringVisitor.asString(expr));
        }
        errors.add("Query timed out after PT2S");
        errors.add("Invalid SQL Statement");
        errors.add("Invalid CQL Statement");
        errors.add(
                "Invalid Table Definition. Transactions cannot be enabled in an index of a table without transactions enabled.");
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
