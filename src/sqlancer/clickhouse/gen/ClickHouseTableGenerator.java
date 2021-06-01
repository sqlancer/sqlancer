package sqlancer.clickhouse.gen;

import java.util.ArrayList;
import java.util.List;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
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
        ClickHouseErrors.addTableManipulationErrors(errors);
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
            columns.add(ClickHouseSchema.ClickHouseColumn.createDummy(ClickHouseCommon.createColumnName(i)));
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
            if (Randomly.getBoolean()) {
                sb.append(" ORDER BY ");
                ClickHouseExpression expr = gen
                        .generateExpression(ClickHouseSchema.ClickHouseLancerDataType.getRandom());
                sb.append(ClickHouseToStringVisitor.asString(expr));
            } else {
                sb.append(" ORDER BY tuple() ");
            }

            if (Randomly.getBoolean()) {
                sb.append(" PARTITION BY ");
                ClickHouseExpression expr = gen
                        .generateExpression(ClickHouseSchema.ClickHouseLancerDataType.getRandom());
                sb.append(ClickHouseToStringVisitor.asString(expr));
            }
            if (Randomly.getBoolean()) {
                sb.append(" SAMPLE BY ");
                ClickHouseExpression expr = gen
                        .generateExpression(ClickHouseSchema.ClickHouseLancerDataType.getRandom());
                sb.append(ClickHouseToStringVisitor.asString(expr));
            }
            // TODO: PRIMARY KEY
        }

    }

    private void addColumnsConstraint(ClickHouseExpressionGenerator gen) {
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            sb.append(",");
            sb.append(" CONSTRAINT ");
            sb.append(ClickHouseCommon.createConstraintName(i));
            sb.append(" CHECK ");
            ClickHouseExpression expr = gen
                    .generateExpression(new ClickHouseSchema.ClickHouseLancerDataType(ClickHouseDataType.UInt8));
            sb.append(ClickHouseToStringVisitor.asString(expr));
        }
    }
}
