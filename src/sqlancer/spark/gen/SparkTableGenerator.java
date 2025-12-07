package sqlancer.spark.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.spark.SparkErrors;
import sqlancer.spark.SparkGlobalState;
import sqlancer.spark.SparkSchema;
import sqlancer.spark.SparkSchema.SparkColumn;
import sqlancer.spark.SparkSchema.SparkDataType;
import sqlancer.spark.SparkSchema.SparkTable;
import sqlancer.spark.SparkToStringVisitor;

public class SparkTableGenerator {

    private enum ColumnConstraints {
        NOT_NULL, DEFAULT
        // PRIMARY KEY and UNIQUE are often not supported in standard Spark file sources (Parquet/ORC)
        // without specific catalogs (like Delta/Iceberg), so we limit to constraints Spark SQL widely accepts.
    }

    private final SparkGlobalState globalState;
    private final String tableName;
    private final StringBuilder sb = new StringBuilder();
    private final SparkExpressionGenerator gen;
    private final SparkTable table;
    private final List<SparkColumn> columnsToBeAdded = new ArrayList<>();

    public SparkTableGenerator(SparkGlobalState globalState, String tableName) {
        this.tableName = tableName;
        this.globalState = globalState;
        this.table = new SparkTable(tableName, columnsToBeAdded, false);
        this.gen = new SparkExpressionGenerator(globalState).setColumns(columnsToBeAdded);
    }

    public static SQLQueryAdapter generate(SparkGlobalState globalState, String tableName) {
        SparkTableGenerator generator = new SparkTableGenerator(globalState, tableName);
        return generator.create();
    }

    private SQLQueryAdapter create() {
        ExpectedErrors errors = new ExpectedErrors();

        sb.append("CREATE TABLE ");
        sb.append(globalState.getDatabaseName());
        sb.append(".");
        sb.append(tableName);
        sb.append(" (");
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            appendColumn(i);
        }
        sb.append(")");
        sb.append(" USING PARQUET");

        // TODO: implement PARTITION BY clause
        // TODO: implement CLUSTERED BY clauses
        // TODO: implement ROW FORMAT and STORED AS clauses
        // TODO: randomly add some predefined TABLEPROPERTIES

        SparkErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true, false);
    }

    private void appendColumn(int columnId) {
        String columnName = DBMSCommon.createColumnName(columnId);
        sb.append(columnName);
        sb.append(" ");
        SparkDataType randType = SparkSchema.SparkDataType.getRandomType();
        sb.append(randType);
        columnsToBeAdded.add(new SparkColumn(columnName, table, randType));
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