package sqlancer.hsqldb.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractUpdateGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hsqldb.HSQLDBErrors;
import sqlancer.hsqldb.HSQLDBProvider;
import sqlancer.hsqldb.HSQLDBSchema;
import sqlancer.hsqldb.HSQLDBSchema.HSQLDBColumn;
import sqlancer.hsqldb.HSQLDBSchema.HSQLDBCompositeDataType;
import sqlancer.hsqldb.HSQLDBSchema.HSQLDBDataType;
import sqlancer.hsqldb.HSQLDBToStringVisitor;
import sqlancer.hsqldb.ast.HSQLDBExpression;

public final class HSQLDBUpdateGenerator extends AbstractUpdateGenerator<HSQLDBColumn> {

    private final HSQLDBProvider.HSQLDBGlobalState globalState;
    private HSQLDBExpressionGenerator gen;

    private HSQLDBUpdateGenerator(HSQLDBProvider.HSQLDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(HSQLDBProvider.HSQLDBGlobalState globalState) {
        return new HSQLDBUpdateGenerator(globalState).getStatement();
    }

    @Override
    public void buildStatement() {
        HSQLDBSchema.HSQLDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<HSQLDBSchema.HSQLDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        gen = new HSQLDBExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append("UPDATE ");
        sb.append(table.getName());
        sb.append(" SET ");
        updateColumns(columns);
        if (Randomly.getBooleanWithSmallProbability()) {
            appendWhereClause(HSQLDBToStringVisitor.asString(
                    gen.generateExpression(HSQLDBCompositeDataType.getRandomWithType(HSQLDBDataType.BOOLEAN))));
            errors.add("data type of expression is not boolean");
            HSQLDBErrors.addExpressionErrors(errors);
        }
    }

    @Override
    protected void updateValue(HSQLDBColumn column) {
        HSQLDBExpression expr;
        expr = gen.generateConstant(column.getType());
        sb.append(HSQLDBToStringVisitor.asString(expr));
    }

}
