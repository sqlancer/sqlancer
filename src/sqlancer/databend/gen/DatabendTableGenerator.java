package sqlancer.databend.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.databend.DatabendExprToNode;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.DatabendSchema.DatabendCompositeDataType;
import sqlancer.databend.DatabendSchema.DatabendDataType;
import sqlancer.databend.DatabendToStringVisitor;
import sqlancer.databend.ast.DatabendExpression;

public class DatabendTableGenerator {

    public SQLQueryAdapter getQuery(DatabendGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        String tableName = globalState.getSchema().getFreeTableName();
        sb.append("CREATE TABLE ");
        sb.append(tableName);
        sb.append("(");
        List<DatabendColumn> columns = getNewColumns();
        TypedExpressionGenerator<DatabendExpression, DatabendColumn, DatabendDataType> gen = new DatabendNewExpressionGenerator(
                globalState).setColumns(columns);
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append(" ");
            sb.append(columns.get(i).getType());

            if (globalState.getDbmsSpecificOptions().testNotNullConstraints
                    && Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" NOT NULL");
            } else {
                sb.append(" NULL"); // Databend 默认字段为非空，这个将它默认设置为允许空
            }

            if (Randomly.getBoolean() && globalState.getDbmsSpecificOptions().testDefaultValues) {
                sb.append(" DEFAULT(");
                sb.append(DatabendToStringVisitor.asString(// 常量类型于字段类型等同
                        DatabendExprToNode
                                .cast(gen.generateConstant(columns.get(i).getType().getPrimitiveDataType()))));
                sb.append(")");
            }
        }

        sb.append(")");
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private static List<DatabendColumn> getNewColumns() {
        List<DatabendColumn> columns = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            DatabendCompositeDataType columnType = DatabendCompositeDataType.getRandomWithoutNull();
            String columnName = String.format("c%d%s", i, columnType.getPrimitiveDataType().toString());
            columns.add(new DatabendColumn(columnName, columnType, false, false));
        }
        return columns;
    }

}
