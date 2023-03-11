package sqlancer.doris.gen;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.AbstractUpdateGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.doris.ast.DorisExpression;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisTable;
import sqlancer.doris.DorisToStringVisitor;

import java.util.List;

public final class DorisUpdateGenerator extends AbstractUpdateGenerator<DorisColumn> {

    private final DorisGlobalState globalState;
    private DorisExpressionGenerator gen;

    private DorisUpdateGenerator(DorisGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(DorisGlobalState globalState) {
        return new DorisUpdateGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        DorisTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<DorisColumn> columns = table.getRandomNonEmptyColumnSubset();
        gen = new DorisExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append("UPDATE ");
        sb.append(table.getName());
        sb.append(" SET ");
        updateColumns(columns);
        DorisErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void updateValue(DorisColumn column) {
        Node<DorisExpression> expr;
        if (Randomly.getBooleanWithSmallProbability()) {
            expr = gen.generateExpression();
            DorisErrors.addExpressionErrors(errors);
        } else {
            expr = gen.generateConstant(globalState, column.getType().getPrimitiveDataType(), column.isNullable());
        }
        sb.append(DorisToStringVisitor.asString(expr));
    }

}
