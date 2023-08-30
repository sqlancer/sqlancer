package sqlancer.presto.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.AbstractUpdateGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.presto.PrestoErrors;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema.PrestoColumn;
import sqlancer.presto.PrestoSchema.PrestoTable;
import sqlancer.presto.PrestoToStringVisitor;
import sqlancer.presto.ast.PrestoExpression;

public final class PrestoUpdateGenerator extends AbstractUpdateGenerator<PrestoColumn> {

    private final PrestoGlobalState globalState;
    private PrestoTypedExpressionGenerator gen;

    private PrestoUpdateGenerator(PrestoGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(PrestoGlobalState globalState) {
        return new PrestoUpdateGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        PrestoTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<PrestoColumn> columns = table.getRandomNonEmptyColumnSubset();
        gen = new PrestoTypedExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append("UPDATE ");
        sb.append(table.getName());
        sb.append(" SET ");
        updateColumns(columns);
        PrestoErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, false, false);
    }

    @Override
    protected void updateValue(PrestoColumn column) {
        Node<PrestoExpression> expr;
        if (Randomly.getBooleanWithSmallProbability()) {
            expr = gen.generateExpression(column.getType());
            PrestoErrors.addExpressionErrors(errors);
        } else {
            expr = gen.generateConstant(column.getType());
        }
        sb.append(PrestoToStringVisitor.asString(expr));
    }

}
