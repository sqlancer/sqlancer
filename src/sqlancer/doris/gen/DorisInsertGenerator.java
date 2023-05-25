package sqlancer.doris.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisTable;
import sqlancer.doris.visitor.DorisExprToNode;
import sqlancer.doris.visitor.DorisToStringVisitor;

public class DorisInsertGenerator extends AbstractInsertGenerator<DorisColumn> {

    private final DorisGlobalState globalState;
    private final ExpectedErrors errors = new ExpectedErrors();

    public DorisInsertGenerator(DorisGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(DorisGlobalState globalState) {
        return new DorisInsertGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        sb.append("INSERT INTO ");
        DorisTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<DorisColumn> columns = table.getRandomNonEmptyInsertColumns();
        sb.append(table.getName());
        sb.append(" (");
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(")");
        sb.append(" VALUES ");
        insertColumns(columns);
        DorisErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void insertValue(DorisColumn column) {
        if (column.hasDefaultValue() && Randomly.getBooleanWithRatherLowProbability()) {
            sb.append("DEFAULT");
        } else {
            String value = DorisToStringVisitor
                    .asString(DorisExprToNode.cast(new DorisNewExpressionGenerator(globalState)
                            .generateConstant(column.getType().getPrimitiveDataType(), column.isNullable()))); // 生成一个与column相同的常量类型
            sb.append(value);
        }
    }

}
