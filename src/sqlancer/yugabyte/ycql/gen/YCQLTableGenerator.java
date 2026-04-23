package sqlancer.yugabyte.ycql.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractTableGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.yugabyte.ycql.YCQLProvider.YCQLGlobalState;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLColumn;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLCompositeDataType;

public class YCQLTableGenerator extends AbstractTableGenerator<YCQLColumn> {

    private YCQLGlobalState globalState;

    public YCQLTableGenerator() {
        this.canAffectSchema = true;
    }

    public SQLQueryAdapter getQuery(YCQLGlobalState globalState) {
        this.globalState = globalState;
        return getStatement();
    }

    @Override
    public void buildStatement() {
        String tableName = globalState.getSchema().getFreeTableName();
        appendCreateTable(tableName, Randomly.getBoolean());
        List<YCQLColumn> columns = getNewColumns();
        sb.append("(");
        appendColumnDefinitionList(columns);
        errors.add("Query timed out after PT2S");
        errors.add("Invalid type for index");
        List<YCQLColumn> primaryKeyColumns = Randomly.nonEmptySubset(columns);
        sb.append(", PRIMARY KEY(");
        sb.append(primaryKeyColumns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
        sb.append(")");
        sb.append(")");
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
