package sqlancer.hsqldb.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hsqldb.HSQLDBProvider;
import sqlancer.hsqldb.HSQLDBSchema;
import sqlancer.hsqldb.HSQLDBToStringVisitor;
import sqlancer.hsqldb.ast.HSQLDBExpression;

public final class HSQLDBUpdateGenerator {

    private static final ExpectedErrors EXPECTED_ERRORS = new ExpectedErrors();
    private final HSQLDBProvider.HSQLDBGlobalState globalState;

    private HSQLDBUpdateGenerator(HSQLDBProvider.HSQLDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(HSQLDBProvider.HSQLDBGlobalState globalState) {
        return new HSQLDBUpdateGenerator(globalState).getQuery();
    }

    private SQLQueryAdapter getQuery() {
        StringBuilder sb = new StringBuilder("UPDATE ");
        HSQLDBSchema.HSQLDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        HSQLDBExpressionGenerator gen = new HSQLDBExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append(" SET ");
        List<HSQLDBSchema.HSQLDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append("=");
            Node<HSQLDBExpression> expr;
            if (Randomly.getBooleanWithSmallProbability()) {
                sb.append(" WHERE ");
                expr = gen.generateExpression(columns.get(i).getType());
                // HSQLDBErrors.addExpressionErrors(errors);
            } else {
                expr = gen.generateConstant(columns.get(i).getType());
            }
            sb.append(HSQLDBToStringVisitor.asString(expr));
        }
        return new SQLQueryAdapter(sb.toString(), EXPECTED_ERRORS);
    }

}
