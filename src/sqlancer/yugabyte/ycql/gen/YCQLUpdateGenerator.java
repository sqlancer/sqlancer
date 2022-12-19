package sqlancer.yugabyte.ycql.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.AbstractUpdateGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ycql.YCQLErrors;
import sqlancer.yugabyte.ycql.YCQLProvider.YCQLGlobalState;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLColumn;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLTable;
import sqlancer.yugabyte.ycql.YCQLToStringVisitor;
import sqlancer.yugabyte.ycql.ast.YCQLExpression;

public final class YCQLUpdateGenerator extends AbstractUpdateGenerator<YCQLColumn> {

    private final YCQLGlobalState globalState;
    private YCQLExpressionGenerator gen;

    private YCQLUpdateGenerator(YCQLGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(YCQLGlobalState globalState) {
        return new YCQLUpdateGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        YCQLTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<YCQLColumn> columns = table.getRandomNonEmptyColumnSubset();
        gen = new YCQLExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append("UPDATE ");
        sb.append(table.getName());
        sb.append(" SET ");
        updateColumns(columns);
        errors.add("Invalid Arguments");
        errors.add("Invalid CQL Statement");
        errors.add("Invalid SQL Statement");
        errors.add("Datatype Mismatch");
        errors.add("Null Argument for Primary Key");
        errors.add("Missing Argument for Primary Key");

        YCQLErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void updateValue(YCQLColumn column) {
        Node<YCQLExpression> expr;
        if (Randomly.getBooleanWithSmallProbability()) {
            expr = gen.generateExpression();
            YCQLErrors.addExpressionErrors(errors);
        } else {
            expr = gen.generateConstant();
        }
        sb.append(YCQLToStringVisitor.asString(expr));
    }

}
