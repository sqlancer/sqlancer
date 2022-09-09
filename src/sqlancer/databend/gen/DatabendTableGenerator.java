package sqlancer.databend.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
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
        TypedExpressionGenerator<Node<DatabendExpression>, DatabendColumn, DatabendDataType> gen = new DatabendNewExpressionGenerator(
                globalState).setColumns(columns);
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append(" ");
            sb.append(columns.get(i).getType());
            // if (globalState.getDbmsSpecificOptions().testCollate && Randomly.getBooleanWithRatherLowProbability()
            // && columns.get(i).getType().getPrimitiveDataType() == DatabendDataType.VARCHAR) {
            // sb.append(" COLLATE ");
            // sb.append(getRandomCollate());
            // }
            // if (globalState.getDbmsSpecificOptions().testIndexes && Randomly.getBooleanWithRatherLowProbability()) {
            // sb.append(" UNIQUE");
            // }
            if (globalState.getDbmsSpecificOptions().testNotNullConstraints
                    && Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" NOT NULL");
            } else {
                sb.append(" NULL"); // Databend 默认字段为非空，这个将它默认设置为允许空
            }
            // if (globalState.getDbmsSpecificOptions().testCheckConstraints //databend 无check约束
            // && Randomly.getBooleanWithRatherLowProbability()) {
            // sb.append(" CHECK(");
            // sb.append(DatabendToStringVisitor.asString(gen.generateExpression()));
            // DatabendErrors.addExpressionErrors(errors);
            // sb.append(")");
            // }
            if (Randomly.getBoolean() && globalState.getDbmsSpecificOptions().testDefaultValues) {
                sb.append(" DEFAULT(");
                sb.append(DatabendToStringVisitor.asString(// 常量类型于字段类型等同
                        gen.generateConstant(columns.get(i).getType().getPrimitiveDataType())));
                sb.append(")");
            }
        }
        // databend并没有索引
        // if (globalState.getDbmsSpecificOptions().testIndexes && Randomly.getBoolean()) {
        // errors.add("Invalid type for index");
        // List<DatabendColumn> primaryKeyColumns = Randomly.nonEmptySubset(columns);
        // sb.append(", PRIMARY KEY(");
        // sb.append(primaryKeyColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        // sb.append(")");
        // }
        sb.append(")");
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    public static String getRandomCollate() {
        return Randomly.fromOptions("NOCASE", "NOACCENT", "NOACCENT.NOCASE", "C", "POSIX");
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
