package sqlancer.questdb.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;
import sqlancer.questdb.QuestDBSchema.QuestDBColumn;
import sqlancer.questdb.QuestDBSchema.QuestDBCompositeDataType;

public class QuestDBTableGenerator {

    public SQLQueryAdapter getQuery(QuestDBGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        String tableName = "test"; // globalState.getSchema().getFreeTableName();
        sb.append("CREATE TABLE ");
        sb.append(tableName);
        sb.append("(");
        List<QuestDBColumn> columns = getNewColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append(" ");
            sb.append(columns.get(i).getType());
        }
        sb.append(");");
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private static List<QuestDBColumn> getNewColumns() {
        List<QuestDBColumn> columns = new ArrayList<>();
        for (int i = 0; i < sqlancer.Randomly.smallNumber() + 1; i++) {
            String columnName = String.format("c%d", i);
            QuestDBCompositeDataType columnType = QuestDBCompositeDataType.getRandomWithoutNull();
            columns.add(new QuestDBColumn(columnName, columnType, false));
        }
        return columns;
    }
}
