package sqlancer.questdb;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.Query;
import java.util.List;

public class QuestDBQueryGenerator {

    // Generate a simple SELECT query for QuestDB
    public Query generateSelect(String tableName, List<String> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        for (int i = 0; i < columns.size(); i++) {
            sb.append(columns.get(i));
            if (i < columns.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(" FROM ").append(tableName);
        sb.append(" LIMIT 10;"); // simple query
        return new SQLQueryAdapter(sb.toString());
    }
}