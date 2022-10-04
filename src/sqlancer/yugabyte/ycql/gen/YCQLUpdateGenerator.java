package sqlancer.yugabyte.ycql.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ycql.YCQLErrors;
import sqlancer.yugabyte.ycql.YCQLProvider.YCQLGlobalState;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLColumn;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLTable;
import sqlancer.yugabyte.ycql.YCQLToStringVisitor;
import sqlancer.yugabyte.ycql.ast.YCQLExpression;

public final class YCQLUpdateGenerator {

    private YCQLUpdateGenerator() {
    }

    public static SQLQueryAdapter getQuery(YCQLGlobalState globalState) {
        StringBuilder sb = new StringBuilder("UPDATE ");
        ExpectedErrors errors = new ExpectedErrors();
        YCQLTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        YCQLExpressionGenerator gen = new YCQLExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append(" SET ");
        List<YCQLColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append("=");
            Node<YCQLExpression> expr;
            if (Randomly.getBooleanWithSmallProbability()) {
                expr = gen.generateExpression();
                YCQLErrors.addExpressionErrors(errors);
            } else {
                expr = gen.generateConstant();
            }
            sb.append(YCQLToStringVisitor.asString(expr));
        }

        errors.add("Invalid Arguments");
        errors.add("Invalid CQL Statement");
        errors.add("Invalid SQL Statement");
        errors.add("Datatype Mismatch");
        errors.add("Null Argument for Primary Key");
        errors.add("Missing Argument for Primary Key");

        YCQLErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
