package sqlancer.doris.gen;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisCompositeDataType;
import sqlancer.doris.visitor.DorisExprToNode;
import sqlancer.doris.visitor.DorisToStringVisitor;

public class DorisTableGenerator {

    // private final ExpectedErrors errors = new ExpectedErrors();

    public static SQLQueryAdapter createRandomTableStatement(DorisGlobalState globalState) throws SQLException {
        if (globalState.getSchema().getDatabaseTables().size() > globalState.getDbmsSpecificOptions().maxNumTables) {
            throw new IgnoreMeException();
        }
        return new DorisTableGenerator().getQuery(globalState);
    }

    public SQLQueryAdapter getQuery(DorisGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        String tableName = globalState.getSchema().getFreeTableName();
        DorisSchema.DorisTableDataModel dataModel = DorisSchema.DorisTableDataModel.getRandom();
        sb.append("CREATE TABLE ");
        sb.append(tableName);
        sb.append("(");
        List<DorisColumn> columns = getNewColumns(globalState);
        Collections.sort(columns);
        if (columns.isEmpty() || !columns.get(0).isKey()) {
            return null; // ensure table has at least one key column
        }
        sb.append(columns.stream().map(DorisColumn::toString).collect(Collectors.joining(", ")));
        sb.append(")");

        List<DorisColumn> keysColumn = columns.stream().filter(DorisColumn::isKey).collect(Collectors.toList());
        if (globalState.getDbmsSpecificOptions().testDataModel && Randomly.getBoolean() && !keysColumn.isEmpty()) {
            sb.append(" " + dataModel).append(" KEY(");
            sb.append(keysColumn.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
            sb.append(")");
        }
        sb.append(generateDistributionStr(globalState, dataModel, keysColumn));
        sb.append(" PROPERTIES (\"replication_num\" = \"1\")"); // now only consider this one parameter
        DorisErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    public static String generateDistributionStr(DorisGlobalState globalState,
            DorisSchema.DorisTableDataModel dataModel, List<DorisColumn> keysColumn) {
        // DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]
        // DISTRIBUTED BY RANDOM [BUCKETS num]
        StringBuilder sb = new StringBuilder();
        sb.append(" DISTRIBUTED BY");
        if (dataModel == DorisSchema.DorisTableDataModel.UNIQUE || Randomly.getBoolean()) {
            sb.append(" HASH (");
            sb.append(Randomly.nonEmptySubset(keysColumn).stream().map(DorisColumn::getName)
                    .collect(Collectors.joining(", ")));
            sb.append(")");
        } else {
            sb.append(" RANDOM");
        }
        if (Randomly.getBoolean()) {
            sb.append(" BUCKETS ").append(globalState.getRandomly().getInteger(1, 32));
        }
        return sb.toString();
    }

    private static List<DorisColumn> getNewColumns(DorisGlobalState globalState) {
        List<DorisColumn> columns = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String columnName = String.format("c%d", i);
            DorisCompositeDataType columnType = DorisCompositeDataType.getRandomWithoutNull(globalState);
            columnType.initColumnArgs(); // set decimalAndVarchar

            boolean iskey = columnType.canBeKey() && Randomly.getBoolean();
            boolean isNullable = Randomly.getBoolean();
            if (!globalState.getDbmsSpecificOptions().testNotNullConstraints) {
                isNullable = true;
            }
            // boolean isHllOrBitmap = (columnType.getPrimitiveDataType() == DorisSchema.DorisDataType.HLL)
            // || (columnType.getPrimitiveDataType() == DorisSchema.DorisDataType.BITMAP);
            boolean isHllOrBitmap = false;
            DorisSchema.DorisColumnAggrType aggrType = DorisSchema.DorisColumnAggrType.NULL;
            if (globalState.getDbmsSpecificOptions().testColumnAggr && (isHllOrBitmap || !iskey)) {
                aggrType = DorisSchema.DorisColumnAggrType.getRandom(columnType);
            }

            boolean hasDefaultValue = globalState.getDbmsSpecificOptions().testDefaultValues && Randomly.getBoolean()
                    && !isHllOrBitmap;
            String defaultValue = "";
            if (hasDefaultValue) {
                defaultValue = DorisToStringVisitor
                        .asString(DorisExprToNode.cast(new DorisNewExpressionGenerator(globalState)
                                .generateConstant(columnType.getPrimitiveDataType(), isNullable)));
            }
            columns.add(new DorisColumn(columnName, columnType, iskey, isNullable, aggrType, hasDefaultValue,
                    defaultValue));
        }
        return columns;
    }

}
