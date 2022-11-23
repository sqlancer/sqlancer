package sqlancer.hsqldb.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hsqldb.HSQLDBErrors;
import sqlancer.hsqldb.HSQLDBProvider.HSQLDBGlobalState;
import sqlancer.hsqldb.HSQLDBSchema;

import java.util.List;

public final class HSQLDBUpdateGenerator {

    private HSQLDBUpdateGenerator() {
    }

    public static SQLQueryAdapter getQuery(HSQLDBGlobalState globalState) {
        StringBuilder sb = new StringBuilder("UPDATE ");
        ExpectedErrors errors = new ExpectedErrors();
        HSQLDBSchema.HSQLDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        //HSQLDBExpressionGenerator gen = new HSQLDBExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append(" SET ");
        List<HSQLDBSchema.HSQLDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append("=");
//            Node<HSQLDBExpression> expr;
//            if (Randomly.getBooleanWithSmallProbability()) {
//                //expr = gen.generateExpression();
//                HSQLDBErrors.addExpressionErrors(errors);
//            } else {
//                //expr = gen.generateConstant();
//            }
        }
        HSQLDBErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
