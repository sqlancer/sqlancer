package sqlancer.databend.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractTableGenerator;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.databend.DatabendErrors;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.DatabendSchema.DatabendCompositeDataType;
import sqlancer.databend.DatabendSchema.DatabendDataType;
import sqlancer.databend.DatabendToStringVisitor;
import sqlancer.databend.ast.DatabendExpression;

public class DatabendTableGenerator extends AbstractTableGenerator<DatabendColumn> {

    private DatabendGlobalState globalState;
    private TypedExpressionGenerator<DatabendExpression, DatabendColumn, DatabendDataType> gen;

    public DatabendTableGenerator() {
        this.canAffectSchema = true;
    }

    public SQLQueryAdapter getQuery(DatabendGlobalState globalState) {
        this.globalState = globalState;
        return getStatement();
    }

    @Override
    public void buildStatement() {
        DatabendErrors.addExpressionErrors(errors);
        String tableName = globalState.getSchema().getFreeTableName();
        appendCreateTable(tableName);
        List<DatabendColumn> columns = getNewColumns();
        gen = new DatabendNewExpressionGenerator(globalState).setColumns(columns);
        appendColumnDefinitions(columns);
    }

    @Override
    protected void appendColumnDefinition(DatabendColumn column) {
        sb.append(column.getName());
        sb.append(" ");
        sb.append(column.getType());

        if (globalState.getDbmsSpecificOptions().testNotNullConstraints
                && Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(" NOT NULL");
        } else {
            sb.append(" NULL"); // Databend 默认字段为非空，这个将它默认设置为允许空
        }

        if (Randomly.getBoolean() && globalState.getDbmsSpecificOptions().testDefaultValues) {
            sb.append(" DEFAULT(");
            sb.append(DatabendToStringVisitor.asString(// 常量类型于字段类型等同
                    gen.generateConstant(column.getType().getPrimitiveDataType())));
            sb.append(")");
        }
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
