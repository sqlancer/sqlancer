package sqlancer.databend.gen;

import java.util.List;

import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.databend.DatabendErrors;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.DatabendSchema.DatabendTable;
import sqlancer.databend.DatabendToStringVisitor;

public class DatabendInsertGenerator extends AbstractInsertGenerator<DatabendColumn> {

    private final DatabendGlobalState globalState;

    public DatabendInsertGenerator(DatabendGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(DatabendGlobalState globalState) {
        return new DatabendInsertGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        DatabendTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<DatabendColumn> columns = table.getRandomNonEmptyColumnSubset();
        buildInsertInto(table.getName(), columns);
        DatabendErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void insertValue(DatabendColumn column) {
        // TODO: 等Databend实现DEFAULT关键字,暂时注入普通的value
        // if (Randomly.getBooleanWithRatherLowProbability()) {
        // sb.append("DEFAULT");
        // } else {
        // sb.append(DatabendToStringVisitor.asString(new DatabendExpressionGenerator(globalState).generateConstant()));
        // }
        String value = DatabendToStringVisitor.asString(new DatabendNewExpressionGenerator(globalState)
                .generateConstant(column.getType().getPrimitiveDataType(), column.isNullable())); // 生成一个与column相同的常量类型
        sb.append(value);

    }

}
