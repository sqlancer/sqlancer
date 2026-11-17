package sqlancer.questdb.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.questdb.QuestDBErrors;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;
import sqlancer.questdb.QuestDBSchema.QuestDBColumn;
import sqlancer.questdb.QuestDBSchema.QuestDBTable;
import sqlancer.questdb.QuestDBToStringVisitor;
import sqlancer.questdb.ast.QuestDBSelect;
import sqlancer.questdb.ast.QuestDBTableReference;
import sqlancer.questdb.ast.QuestDBExpression;
import sqlancer.questdb.ast.QuestDBColumnReference;

public final class QuestDBViewGenerator {
    private QuestDBViewGenerator() {
    }

    public static SQLQueryAdapter generate(QuestDBGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        ExpectedErrors errors = new ExpectedErrors();
        
        String viewName = "v" + globalState.getSchema().getDatabaseTables().size();
        QuestDBTable table = globalState.getSchema().getRandomTable();
        
        sb.append("CREATE VIEW ");
        sb.append(viewName);
        sb.append(" AS ");
        
        // Generate SELECT statement for the view
        QuestDBSelect select = new QuestDBSelect();
        List<QuestDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        List<QuestDBExpression> fetchColumns = new ArrayList<>();
        
        for (QuestDBColumn column : columns) {
            fetchColumns.add(new QuestDBColumnReference(column));
        }
        select.setFetchColumns(fetchColumns);
        
        List<QuestDBExpression> tableList = new ArrayList<>();
        tableList.add(new QuestDBTableReference(table));
        select.setFromList(tableList);
        
        // Add WHERE clause (optional)
        if (Randomly.getBoolean()) {
            select.setWhereClause(new QuestDBExpressionGenerator(globalState)
                .setColumns(table.getColumns()).generateExpression());
        }
        
        sb.append(QuestDBToStringVisitor.asString(select));
        
        QuestDBErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }
}
