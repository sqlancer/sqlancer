package sqlancer.questdb.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.questdb.QuestDBErrors;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;
import sqlancer.questdb.QuestDBSchema.QuestDBColumn;
import sqlancer.questdb.QuestDBSchema.QuestDBTable;
import sqlancer.questdb.QuestDBToStringVisitor;

public final class QuestDBUpdateGenerator {

    private QuestDBUpdateGenerator() {
    }

    public static SQLQueryAdapter getQuery(QuestDBGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        ExpectedErrors errors = new ExpectedErrors();
        
        QuestDBTable table = globalState.getSchema().getRandomTable();
        List<QuestDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        
        sb.append("UPDATE ");
        sb.append(table.getName());
        sb.append(" SET ");
        
        // Generate SET clauses
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append(" = ");
            sb.append(QuestDBToStringVisitor.asString(
                new QuestDBExpressionGenerator(globalState).generateConstant()));
        }
        
        // Add WHERE clause (optional)
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(QuestDBToStringVisitor.asString(
                new QuestDBExpressionGenerator(globalState).setColumns(table.getColumns()).generateExpression()));
        }
        
        QuestDBErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
