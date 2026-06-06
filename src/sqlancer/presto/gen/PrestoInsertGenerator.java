package sqlancer.presto.gen;

import java.util.List;

import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.presto.PrestoErrors;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema.PrestoColumn;
import sqlancer.presto.PrestoSchema.PrestoTable;
import sqlancer.presto.PrestoToStringVisitor;
import sqlancer.presto.ast.PrestoExpression;

public class PrestoInsertGenerator extends AbstractInsertGenerator<PrestoColumn> {

    private final PrestoGlobalState globalState;

    public PrestoInsertGenerator(PrestoGlobalState globalState) {
        this.globalState = globalState;
        this.canonicalizeString = false;
    }

    public static SQLQueryAdapter getQuery(PrestoGlobalState globalState) {
        return new PrestoInsertGenerator(globalState).getStatement();
    }

    @Override
    public void buildStatement() {
        PrestoTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<PrestoColumn> columns = table.getRandomNonEmptyColumnSubset();
        buildInsertInto(table.getName(), columns);
        PrestoErrors.addInsertErrors(errors);
    }

    @Override
    protected void insertValue(PrestoColumn prestoColumn) {
        PrestoExpression constant = new PrestoTypedExpressionGenerator(globalState)
                .generateInsertConstant(prestoColumn.getType());
        sb.append(PrestoToStringVisitor.asString(constant));

    }

}
