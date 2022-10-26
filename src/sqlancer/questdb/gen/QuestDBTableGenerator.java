package sqlancer.questdb.gen;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.questdb.QuestDBBugs;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;
import sqlancer.questdb.QuestDBSchema.QuestDBColumn;
import sqlancer.questdb.QuestDBSchema.QuestDBCompositeDataType;

public class QuestDBTableGenerator {

    public SQLQueryAdapter getQuery(QuestDBGlobalState globalState, @Nullable String tableName) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        String name = tableName;
        if (tableName == null) {
            name = globalState.getSchema().getFreeTableName();
        }
        sb.append("CREATE TABLE ");
        if (Randomly.getBoolean()) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(name);
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
        sb.append(")");
        // test index at Create Table
        if (Randomly.getBooleanWithRatherLowProbability()) {
            errors.add("cannot create index");
            // QuestDB does not support index for non-SYMBOL typed columns
            errors.add("Index flag is only supported for SYMBOL");
            sb.append(",");
            String index = String.format(" INDEX (%s)", Randomly.fromList(columns).getName());
            sb.append(index);
        }
        if (QuestDBBugs.bug2689) {
            errors.add("Invalid metadata");
        }
        sb.append(";");
        errors.add("table already exists");
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private static List<QuestDBColumn> getNewColumns() {
        List<QuestDBColumn> columns = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String columnName = String.format("c%d", i);
            QuestDBCompositeDataType columnType = QuestDBCompositeDataType.getRandomWithoutNull();
            columns.add(new QuestDBColumn(columnName, columnType, false));
        }
        return columns;
    }
}
