package sqlancer.hsqldb.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hsqldb.HSQLDBProvider;
import sqlancer.hsqldb.HSQLDBSchema;
import sqlancer.hsqldb.HSQLDBToStringVisitor;
import sqlancer.hsqldb.ast.HSQLDBExpression;

public class HSQLDBInsertGenerator extends AbstractInsertGenerator<HSQLDBSchema.HSQLDBColumn> {

    private final HSQLDBProvider.HSQLDBGlobalState globalState;
    private final ExpectedErrors errors = new ExpectedErrors();

    public HSQLDBInsertGenerator(HSQLDBProvider.HSQLDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(HSQLDBProvider.HSQLDBGlobalState globalState) {
        return new HSQLDBInsertGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        sb.append("INSERT INTO ");
        HSQLDBSchema.HSQLDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<HSQLDBSchema.HSQLDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append(table.getName());
        sb.append("(");
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(")");
        sb.append(" VALUES ");
        insertColumns(columns);
        // HSQLDBErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void insertValue(HSQLDBSchema.HSQLDBColumn column) {
        Node<HSQLDBExpression> expression = new HSQLDBExpressionGenerator(globalState)
                .generateConstant(column.getType());
        String s = HSQLDBToStringVisitor.asString(expression);
        sb.append(s);
    }

}
