package sqlancer.doris.gen;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisTable;
import sqlancer.doris.DorisToStringVisitor;

import java.util.List;
import java.util.stream.Collectors;

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
        List<DorisColumn> columns = table.getRandomNonEmptyColumnSubset();
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
        // TODO: select a more meaningful value
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append("DEFAULT");
        } else {
            sb.append(DorisToStringVisitor.asString(new DorisExpressionGenerator(globalState).generateConstant(globalState, column.getType().getPrimitiveDataType(), column.isNullable())));
        }
    }

}
