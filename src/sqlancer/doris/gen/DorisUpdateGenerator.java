package sqlancer.doris.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractUpdateGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisTable;
import sqlancer.doris.ast.DorisExpression;
import sqlancer.doris.visitor.DorisExprToNode;
import sqlancer.doris.visitor.DorisToStringVisitor;

public final class DorisUpdateGenerator extends AbstractUpdateGenerator<DorisColumn> {

    private final DorisGlobalState globalState;
    private DorisNewExpressionGenerator gen;

    private DorisUpdateGenerator(DorisGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(DorisGlobalState globalState) {
        return new DorisUpdateGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        DorisTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<DorisColumn> columns = table.getRandomNonEmptyColumnSubset();
        gen = new DorisNewExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append("UPDATE ");
        sb.append(table.getName());
        sb.append(" SET ");
        updateColumns(columns);
        sb.append(" WHERE ");
        sb.append(DorisToStringVisitor.asString(gen.generateExpression(DorisSchema.DorisDataType.BOOLEAN)));
        DorisErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void updateValue(DorisColumn column) {
        if (Randomly.getBooleanWithSmallProbability()) {
            DorisExpression expr = gen.generateExpression(column.getType().getPrimitiveDataType());
            sb.append(DorisToStringVisitor.asString(DorisExprToNode.cast(expr)));
        } else {
            DorisExpression expr = gen.generateConstant(column.getType().getPrimitiveDataType(), column.isNullable());
            sb.append(DorisToStringVisitor.asString(expr));
        }

    }

}
