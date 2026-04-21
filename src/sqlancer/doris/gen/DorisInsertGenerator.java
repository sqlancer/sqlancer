package sqlancer.doris.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisTable;
import sqlancer.doris.visitor.DorisToStringVisitor;

public class DorisInsertGenerator extends AbstractInsertGenerator<DorisColumn> {

    private final DorisGlobalState globalState;

    public DorisInsertGenerator(DorisGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(DorisGlobalState globalState) {
        return new DorisInsertGenerator(globalState).getStatement();
    }

    @Override
    public void buildStatement() {
        DorisTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<DorisColumn> columns = table.getRandomNonEmptyColumnSubset();
        buildInsertInto(table.getName(), columns);
        DorisErrors.addInsertErrors(errors);
    }

    @Override
    protected void insertValue(DorisColumn column) {
        if (column.hasDefaultValue() && Randomly.getBooleanWithRatherLowProbability()) {
            sb.append("DEFAULT");
        } else {
            String value = DorisToStringVisitor.asString(new DorisNewExpressionGenerator(globalState)
                    .generateConstant(column.getType().getPrimitiveDataType(), column.isNullable())); // 生成一个与column相同的常量类型
            sb.append(value);
        }
    }

}
