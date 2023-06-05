package sqlancer.questdb.gen;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.questdb.QuestDBDataType;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;

import static sqlancer.questdb.QuestDBSchema.QuestDBColumn;

public class QuestDBTableGenerator {
    private static List<QuestDBColumn> getNewColumns() {
        List<QuestDBColumn> columns = new ArrayList<>();
        boolean hasDesignatedTimestamp = false;
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            QuestDBDataType columnType = QuestDBDataType.getNonNullRandom();
            boolean isIndexed = columnType == QuestDBDataType.SYMBOL && Randomly.getBoolean();
            boolean isDesignated = false;
            if (columnType == QuestDBDataType.TIMESTAMP && !hasDesignatedTimestamp) {
                hasDesignatedTimestamp = true;
                isDesignated = true;
            }
            columns.add(new QuestDBColumn("c" + i, columnType, isIndexed, isDesignated));
        }
        return columns;
    }

    public SQLQueryAdapter getQuery(QuestDBGlobalState globalState, @Nullable String tableName) {
        String name = tableName;
        if (tableName == null) {
            name = globalState.getSchema().getFreeTableName();
        }
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        if (Randomly.getBoolean()) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append('\'').append(name).append("' (");
        List<QuestDBColumn> columns = getNewColumns();
        QuestDBColumn designated = null;
        for (int i = 0, n = columns.size(); i < n; i++) {
            QuestDBColumn column = columns.get(i);
            sb.append(column.getName());
            sb.append(' ');
            sb.append(column.getType());
            if (column.isIndexed()) {
                sb.append(" INDEX CAPACITY 256");
            }
            if (column.isDesignated()) {
                designated = column;
            }
            if (i + 1 < n) {
                sb.append(", ");
            }
        }
        sb.append(')');
        if (designated != null) {
            PartitionBy partitionBy = Randomly.fromOptions(PartitionBy.values());
            sb.append(" TIMESTAMP(")
                    .append(designated.getName())
                    .append(") PARTITION BY ")
                    .append(partitionBy);
            if (Randomly.getBoolean() && partitionBy != PartitionBy.NONE) {
                sb.append(" WAL");
            } else {
                sb.append(" BYPASS WAL");
            }
        }
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("table already exists");
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    public enum PartitionBy {
        NONE, HOUR, DAY, WEEK, MONTH, YEAR
    }
}
