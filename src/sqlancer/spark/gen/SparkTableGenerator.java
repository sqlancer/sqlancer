package sqlancer.spark.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.gen.AbstractTableGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.spark.SparkErrors;
import sqlancer.spark.SparkGlobalState;
import sqlancer.spark.SparkSchema;
import sqlancer.spark.SparkSchema.SparkColumn;
import sqlancer.spark.SparkSchema.SparkDataType;
import sqlancer.spark.SparkSchema.SparkTable;
import sqlancer.spark.SparkToStringVisitor;

public class SparkTableGenerator extends AbstractTableGenerator<SparkColumn> {

    private enum ColumnConstraints {
        NOT_NULL, DEFAULT
        // PRIMARY KEY and UNIQUE are often not supported in standard Spark file sources
        // (Parquet/ORC)
        // without specific catalogs (like Delta/Iceberg), so we limit to constraints
        // Spark SQL widely accepts.
    }

    private final SparkGlobalState globalState;
    private final String tableName;
    private final SparkExpressionGenerator gen;
    private final SparkTable table;
    private final List<SparkColumn> columnsToBeAdded = new ArrayList<>();

    public SparkTableGenerator(SparkGlobalState globalState, String tableName) {
        this.tableName = tableName;
        this.globalState = globalState;
        this.table = new SparkTable(tableName, columnsToBeAdded, false);
        this.gen = new SparkExpressionGenerator(globalState).setColumns(columnsToBeAdded);
        this.canAffectSchema = true;
        this.canonicalizeString = false;
    }

    public static SQLQueryAdapter generate(SparkGlobalState globalState, String tableName) {
        return new SparkTableGenerator(globalState, tableName).getStatement();
    }

    @Override
    public void buildStatement() {
        int columnCount = Randomly.smallNumber() + 1;
        for (int i = 0; i < columnCount; i++) {
            String columnName = DBMSCommon.createColumnName(i);
            SparkDataType type = SparkSchema.SparkDataType.getRandomType();
            columnsToBeAdded.add(new SparkColumn(columnName, table, type));
        }
        appendCreateTable(globalState.getDatabaseName() + "." + tableName);
        sb.append(" ");
        appendColumnDefinitions(columnsToBeAdded);
        sb.append(" USING PARQUET");

        // TODO: implement PARTITION BY clause
        // TODO: implement CLUSTERED BY clauses
        // TODO: implement ROW FORMAT and STORED AS clauses
        // TODO: randomly add some predefined TABLEPROPERTIES

        SparkErrors.addExpressionErrors(errors);
    }

    @Override
    protected void appendColumnDefinition(SparkColumn column) {
        sb.append(column.getName());
        sb.append(" ");
        sb.append(column.getType());
        appendColumnConstraint();
    }

    private void appendColumnConstraint() {
        if (Randomly.getBoolean()) {
            return;
        }

        ColumnConstraints constraint = Randomly.fromOptions(ColumnConstraints.values());
        switch (constraint) {
        case NOT_NULL:
            sb.append(" NOT NULL");
            break;
        case DEFAULT:
            sb.append(" DEFAULT ");
            sb.append(SparkToStringVisitor.asString(gen.generateConstant()));
            sb.append(" ");
            break;
        default:
            throw new AssertionError(constraint);
        }
    }
}
