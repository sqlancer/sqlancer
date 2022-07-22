package sqlancer.databend.gen;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.databend.DatabendErrors;
import sqlancer.databend.DatabendToStringVisitor;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.DatabendSchema.DatabendTable;

import java.util.List;
import java.util.stream.Collectors;

public class DatabendInsertGenerator extends AbstractInsertGenerator<DatabendColumn> {

    private final DatabendGlobalState globalState;
    private final ExpectedErrors errors = new ExpectedErrors();

    public DatabendInsertGenerator(DatabendGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(DatabendGlobalState globalState) {
        return new DatabendInsertGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        sb.append("INSERT INTO ");
        DatabendTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<DatabendColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append(table.getName());
        sb.append("(");
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(")");
        sb.append(" VALUES ");
        insertColumns(columns);
        DatabendErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void insertValue(DatabendColumn tiDBColumn) {
        // TODO: 等Databend实现NULL 和 DEFAULT ,暂时注入普通的value
//        if (Randomly.getBooleanWithRatherLowProbability()) {
//            sb.append("DEFAULT");
//        } else {
//            sb.append(DatabendToStringVisitor.asString(new DatabendExpressionGenerator(globalState).generateConstant()));
//        }

        String value = DatabendToStringVisitor.asString(new DatabendExpressionGenerator(globalState).generateConstant());
        sb.append(value);

    }

}
