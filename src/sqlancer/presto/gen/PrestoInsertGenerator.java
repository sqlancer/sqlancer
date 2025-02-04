package sqlancer.presto.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.presto.PrestoErrors;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema.PrestoColumn;
import sqlancer.presto.PrestoSchema.PrestoTable;
import sqlancer.presto.PrestoToStringVisitor;
import sqlancer.presto.ast.PrestoExpression;

public class PrestoInsertGenerator extends AbstractInsertGenerator<PrestoColumn> {

    private final PrestoGlobalState globalState;
    private final ExpectedErrors errors = new ExpectedErrors();

    public PrestoInsertGenerator(PrestoGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(PrestoGlobalState globalState) {
        return new PrestoInsertGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        sb.append("INSERT INTO ");
        PrestoTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<PrestoColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append(table.getName());
        sb.append("(");
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(")");
        sb.append(" VALUES ");
        insertColumns(columns);
        PrestoErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, false, false);
    }

    @Override
    protected void insertValue(PrestoColumn prestoColumn) {
        Node<PrestoExpression> constant = new PrestoTypedExpressionGenerator(globalState)
                .generateInsertConstant(prestoColumn.getType());
        sb.append(PrestoToStringVisitor.asString(constant));

    }

}
