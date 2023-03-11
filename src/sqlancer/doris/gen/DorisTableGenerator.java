package sqlancer.doris.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.doris.DorisSchema;
import sqlancer.doris.DorisToStringVisitor;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisCompositeDataType;

import java.util.*;
import java.util.stream.Collectors;

public class DorisTableGenerator {

    public SQLQueryAdapter getQuery(DorisGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        String tableName = globalState.getSchema().getFreeTableName();
        DorisSchema.DorisTableDataModel dataModel = DorisSchema.DorisTableDataModel.getRandom();
        sb.append("CREATE TABLE ");
        sb.append(tableName);
        sb.append("(");
        List<DorisColumn> columns = getNewColumns(globalState, dataModel);
        Collections.sort(columns);
        if (columns.isEmpty() || !columns.get(0).isKey()) return null; // ensure table has at least one key column

        sb.append(columns.stream().map(DorisColumn::toString).collect(Collectors.joining(", ")));
        sb.append(")");

        List<DorisColumn> keysColumn = columns.stream().filter(DorisColumn::isKey).collect(Collectors.toList());
        if (globalState.getDbmsSpecificOptions().testDataModel && Randomly.getBoolean() && !keysColumn.isEmpty()) {
            sb.append(" " + dataModel).append(" KEY(");
            sb.append(keysColumn.stream().map(c->c.getName()).collect(Collectors.joining(", ")));
            sb.append(")");
        }
        sb.append(generateDistributionStr(globalState, dataModel, keysColumn));
        sb.append(" PROPERTIES (\"replication_num\" = \"1\")"); // now only consider this one parameter
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    public static String generateDistributionStr(DorisGlobalState globalState, DorisSchema.DorisTableDataModel dataModel, List<DorisColumn> keysColumn) {
        // DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]
        // DISTRIBUTED BY RANDOM [BUCKETS num]
        StringBuilder sb = new StringBuilder();
        sb.append(" DISTRIBUTED BY");
        if (dataModel == DorisSchema.DorisTableDataModel.UNIQUE || Randomly.getBoolean()) {
            sb.append(" HASH (");
            sb.append(Randomly.nonEmptySubset(keysColumn).stream().map(DorisColumn::getName).collect(Collectors.joining(", ")));
            sb.append(")");
        } else {
            sb.append(" RANDOM");
        }
        if (Randomly.getBoolean()) {
            sb.append(" BUCKETS ").append(globalState.getRandomly().getInteger(1, 32));
        }
        return sb.toString();
    }

    private static List<DorisColumn> getNewColumns(DorisGlobalState globalState, DorisSchema.DorisTableDataModel tableModel) {
        List<DorisColumn> columns = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String columnName = String.format("c%d", i);
            DorisCompositeDataType columnType = DorisCompositeDataType.getRandomWithoutNull();

            boolean iskey = columnType.canBeKey() && Randomly.getBoolean();
            boolean isNullable = Randomly.getBoolean();
            boolean isHllOrBitmap = (columnType.getPrimitiveDataType() == DorisSchema.DorisDataType.HLL)
                    || (columnType.getPrimitiveDataType() == DorisSchema.DorisDataType.BITMAP);
            DorisSchema.DorisColumnAggrType aggrType = DorisSchema.DorisColumnAggrType.NULL;
            if (globalState.getDbmsSpecificOptions().testColumnAggr) {
                if (isHllOrBitmap || !iskey) {
                    aggrType = DorisSchema.DorisColumnAggrType.getRandom(columnType);
                }
            }

            boolean hasDefaultValue = globalState.getDbmsSpecificOptions().testDefaultValues && Randomly.getBoolean() && !isHllOrBitmap;
            String defaultValue = "";
            if (hasDefaultValue) {
                defaultValue = DorisToStringVisitor.asString(new DorisExpressionGenerator(globalState).generateConstant(globalState, columnType.getPrimitiveDataType(), isNullable));
                if (!defaultValue.equals("NULL") && !defaultValue.equals("CURRENT_TIMESTAMP")) {
                    defaultValue = "\"" + defaultValue + "\"";
                }
            }
            columns.add(new DorisColumn(columnName, columnType, iskey, isNullable, aggrType, hasDefaultValue, defaultValue));
        }
        return columns;
    }

}
