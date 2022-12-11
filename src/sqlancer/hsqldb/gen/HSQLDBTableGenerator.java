package sqlancer.hsqldb.gen;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hsqldb.HSQLDBProvider;
import sqlancer.hsqldb.HSQLDBSchema;

public class HSQLDBTableGenerator {

    public SQLQueryAdapter getQuery(HSQLDBProvider.HSQLDBGlobalState globalState, @Nullable String tableName) {
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
        List<HSQLDBSchema.HSQLDBColumn> columns = getNewColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append(" ");
            sb.append(columns.get(i).getType().getType().name());
            if (columns.get(i).getType().getSize() > 0) {
                // Cannot specify size for non composite data types
                sb.append("(");
                sb.append(columns.get(i).getType().getSize());
                sb.append(")");
            }
        }
        sb.append(")");
        sb.append(";");
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private static List<HSQLDBSchema.HSQLDBColumn> getNewColumns() {
        List<HSQLDBSchema.HSQLDBColumn> columns = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String columnName = String.format("c%d", i);
            HSQLDBSchema.HSQLDBCompositeDataType columnType = HSQLDBSchema.HSQLDBCompositeDataType
                    .getRandomWithoutNull();
            columns.add(new HSQLDBSchema.HSQLDBColumn(columnName, null, columnType));
        }
        return columns;
    }
}
