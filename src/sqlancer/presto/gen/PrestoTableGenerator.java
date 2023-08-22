package sqlancer.presto.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema.PrestoColumn;
import sqlancer.presto.PrestoSchema.PrestoCompositeDataType;

public class PrestoTableGenerator {

    private static List<PrestoColumn> getNewColumns() {
        List<PrestoColumn> columns = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String columnName = String.format("c%d", i);
            PrestoCompositeDataType columnType = PrestoCompositeDataType.getRandomWithoutNull();
            columns.add(new PrestoColumn(columnName, columnType, false, false));
        }
        return columns;
    }

    public SQLQueryAdapter getQuery(PrestoGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        String tableName = globalState.getSchema().getFreeTableName();
        sb.append("CREATE TABLE ");
        String catalog = globalState.getDbmsSpecificOptions().catalog;
        String schema = globalState.getDatabaseName();

        sb.append(catalog).append(".");
        sb.append(schema).append(".");

        sb.append(tableName);
        sb.append("(");
        List<PrestoColumn> columns = getNewColumns();
        // TypedExpressionGenerator<Node<PrestoExpression>, PrestoColumn, PrestoCompositeDataType>
        // typedExpressionGenerator = new PrestoTypedExpressionGenerator(globalState).setColumns(columns);
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            PrestoColumn column = columns.get(i);
            sb.append(column.getName());
            sb.append(" ");
            sb.append(column.getType());
            // if (globalState.getDbmsSpecificOptions().testIndexes && Randomly.getBooleanWithRatherLowProbability()) {
            // sb.append(" UNIQUE");
            // }
            // if (globalState.getDbmsSpecificOptions().testNotNullConstraints
            // && Randomly.getBooleanWithRatherLowProbability()) {
            // sb.append(" NOT NULL");
            // }
        }
        // if (globalState.getDbmsSpecificOptions().testIndexes && Randomly.getBoolean()) {
        // errors.add("Invalid type for index");
        // List<PrestoColumn> primaryKeyColumns = Randomly.nonEmptySubset(columns);
        // sb.append(", PRIMARY KEY(");
        // sb.append(primaryKeyColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        // sb.append(")");
        // }
        sb.append(")");

        return new SQLQueryAdapter(sb.toString(), errors, true, false);
    }

}
