package sqlancer.clickhouse.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.clickhouse.ClickHouseErrors;
import sqlancer.clickhouse.ClickHouseProvider;
import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.clickhouse.ClickHouseToStringVisitor;
import sqlancer.clickhouse.ast.ClickHouseExpression;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;

public class ClickHouseTableGenerator {

    private enum ClickHouseEngine {
        // TinyLog, StripeLog,
        Log, Memory, MergeTree
    }

    private final StringBuilder sb = new StringBuilder();
    private final String tableName;
    private int columnId;
    private final List<String> columnNames = new ArrayList<>();
    private final List<ClickHouseSchema.ClickHouseColumn> columns = new ArrayList<>();
    private final ClickHouseProvider.ClickHouseGlobalState globalState;

    public ClickHouseTableGenerator(String tableName, ClickHouseProvider.ClickHouseGlobalState globalState) {
        this.tableName = tableName;
        this.globalState = globalState;
    }

    public static SQLQueryAdapter createTableStatement(String tableName,
            ClickHouseProvider.ClickHouseGlobalState globalState) {
        ClickHouseTableGenerator chTableGenerator = new ClickHouseTableGenerator(tableName, globalState);
        chTableGenerator.start();
        ExpectedErrors errors = new ExpectedErrors();
        ClickHouseErrors.addExpectedExpressionErrors(errors);
        return new SQLQueryAdapter(chTableGenerator.sb.toString(), errors, true);
    }

    public void start() {
        ClickHouseEngine engine = Randomly.fromOptions(ClickHouseEngine.values());
        ClickHouseExpressionGenerator gen = new ClickHouseExpressionGenerator(globalState).allowAggregates(false);
        sb.append("CREATE ");
        sb.append("TABLE ");
        if (Randomly.getBoolean()) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(this.globalState.getDatabaseName());
        sb.append(".");
        sb.append(this.tableName);
        sb.append(" (");
        int nrColumns = 1 + Randomly.smallNumber();
        for (int i = 0; i < nrColumns; i++) {
            columns.add(ClickHouseSchema.ClickHouseColumn.createDummy(ClickHouseCommon.createColumnName(i), null));
        }
        for (int i = 0; i < nrColumns; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            String columnName = ClickHouseCommon.createColumnName(columnId);
            ClickHouseColumnBuilder columnBuilder = new ClickHouseColumnBuilder();
            sb.append(columnBuilder.createColumn(columnName, globalState, columns));
            columnNames.add(columnName);
            columnId++;
        }
        if (Randomly.getBooleanWithSmallProbability()) {
            for (int i = 0; i < Randomly.smallNumber(); i++) {
                addColumnsConstraint(gen);
            }
        }
        sb.append(") ENGINE = ");
        sb.append(engine);
        sb.append("(");
        sb.append(") ");
        if (engine == ClickHouseEngine.MergeTree) {
            // First, determine eligible columns for primary key
            List<String> eligibleColumns = columnNames.stream()
                    .filter(colName -> columns.stream().filter(col -> col.getName().equals(colName))
                            .anyMatch(col -> !col.isAlias() && !col.isMaterialized()))
                    .collect(Collectors.toList());

            // Generate primary key if we have eligible columns
            final List<String> pkColumns;
            if (!eligibleColumns.isEmpty() && Randomly.getBooleanWithSmallProbability()) {
                pkColumns = Randomly.nonEmptySubset(eligibleColumns);
            } else {
                pkColumns = new ArrayList<>();
            }

            if (Randomly.getBoolean()) {
                sb.append(" PARTITION BY ");
                ClickHouseExpression partitionExpr = gen.generateExpressionWithColumns(
                        columns.stream().map(c -> c.asColumnReference(null)).collect(Collectors.toList()), 3);
                sb.append(ClickHouseToStringVisitor.asString(partitionExpr));
            }

            // Always generate ORDER BY
            sb.append(" ORDER BY ");
            if (pkColumns.size() > 0) {
                // If we have primary key columns, use them as prefix for ORDER BY
                sb.append("(");
                sb.append(String.join(", ", pkColumns));
                // Optionally add more columns to ORDER BY
                if (Randomly.getBoolean()) {
                    List<String> additionalOrderColumns = eligibleColumns.stream()
                            .filter(col -> !pkColumns.contains(col)).collect(Collectors.toList());
                    if (!additionalOrderColumns.isEmpty()) {
                        sb.append(", ");
                        List<String> selectedAdditionalColumns = Randomly.nonEmptySubset(additionalOrderColumns);
                        sb.append(String.join(", ", selectedAdditionalColumns));
                    }
                }
                sb.append(")");
            } else {
                // No primary key, use tuple() or random columns
                if (Randomly.getBoolean()) {
                    sb.append("tuple()");
                } else {
                    sb.append("(");
                    List<String> selectedColumns = Randomly.nonEmptySubset(eligibleColumns);
                    sb.append(String.join(", ", selectedColumns));
                    sb.append(")");
                }
            }

            // Add PRIMARY KEY if we genrated one
            if (pkColumns.size() > 0) {
                sb.append(" PRIMARY KEY (");
                sb.append(String.join(", ", pkColumns));
                sb.append(")");
            }

            if (Randomly.getBoolean()) {
                sb.append(" SAMPLE BY ");
                ClickHouseExpression sampleExpr = gen.generateExpressionWithColumns(
                        columns.stream().map(c -> c.asColumnReference(null)).collect(Collectors.toList()), 3);
                sb.append(ClickHouseToStringVisitor.asString(sampleExpr));
            }
            // Suppress index sanity checks https://github.com/sqlancer/sqlancer/issues/788
            sb.append(" SETTINGS allow_suspicious_indices=1");
        }
    }

    private void addColumnsConstraint(ClickHouseExpressionGenerator gen) {
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            sb.append(",");
            sb.append(" CONSTRAINT ");
            sb.append(ClickHouseCommon.createConstraintName(i));
            sb.append(" CHECK ");
            ClickHouseExpression expr = gen.generateExpressionWithColumns(
                    columns.stream().map(c -> c.asColumnReference(null)).collect(Collectors.toList()), 2);
            sb.append(ClickHouseToStringVisitor.asString(expr));
        }
    }
}
