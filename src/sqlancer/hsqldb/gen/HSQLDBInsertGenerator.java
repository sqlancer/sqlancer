package sqlancer.hsqldb.gen;

import java.util.List;

import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hsqldb.HSQLDBProvider;
import sqlancer.hsqldb.HSQLDBSchema;
import sqlancer.hsqldb.HSQLDBToStringVisitor;
import sqlancer.hsqldb.ast.HSQLDBExpression;

public class HSQLDBInsertGenerator extends AbstractInsertGenerator<HSQLDBSchema.HSQLDBColumn> {

    private final HSQLDBProvider.HSQLDBGlobalState globalState;

    public HSQLDBInsertGenerator(HSQLDBProvider.HSQLDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(HSQLDBProvider.HSQLDBGlobalState globalState) {
        return new HSQLDBInsertGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        HSQLDBSchema.HSQLDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<HSQLDBSchema.HSQLDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        buildInsertInto(table.getName(), columns);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void insertValue(HSQLDBSchema.HSQLDBColumn column) {
        HSQLDBExpression expression = new HSQLDBExpressionGenerator(globalState).generateConstant(column.getType());
        String s = HSQLDBToStringVisitor.asString(expression);
        sb.append(s);
    }

}
