package sqlancer.yugabyte.ycql.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.yugabyte.ycql.YCQLProvider.YCQLGlobalState;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLColumn;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLCompositeDataType;

public class YCQLTableGenerator {

    public SQLQueryAdapter getQuery(YCQLGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        String tableName = globalState.getSchema().getFreeTableName();
        sb.append("CREATE TABLE ");
        if (Randomly.getBoolean()) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(tableName);
        sb.append("(");
        List<YCQLColumn> columns = getNewColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append(" ");
            sb.append(columns.get(i).getType());
            // todo PK, STATIC
        }
        errors.add("Query timed out after PT2S");
        errors.add("Invalid type for index");
        List<YCQLColumn> primaryKeyColumns = Randomly.nonEmptySubset(columns);
        sb.append(", PRIMARY KEY(");
        sb.append(primaryKeyColumns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
        sb.append(")");
        sb.append(")");
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private static List<YCQLColumn> getNewColumns() {
        List<YCQLColumn> columns = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String columnName = String.format("c%d", i);
            YCQLCompositeDataType columnType = YCQLCompositeDataType.getRandom();
            columns.add(new YCQLColumn(columnName, columnType, false, false));
        }
        return columns;
    }

}
