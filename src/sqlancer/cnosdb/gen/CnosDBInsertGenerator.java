package sqlancer.cnosdb.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBGlobalState;
import sqlancer.cnosdb.CnosDBSchema.CnosDBColumn;
import sqlancer.cnosdb.CnosDBSchema.CnosDBTable;
import sqlancer.cnosdb.CnosDBVisitor;
import sqlancer.cnosdb.ast.CnosDBExpression;
import sqlancer.cnosdb.query.CnosDBOtherQuery;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.schema.AbstractTableColumn;

public final class CnosDBInsertGenerator {

    private final CnosDBGlobalState globalState;

    public CnosDBInsertGenerator(CnosDBGlobalState globalState) {
        this.globalState = globalState;
    }   

    public static CnosDBOtherQuery getQuery(CnosDBGlobalState globalState){
        return new CnosDBInsertGenerator(globalState).generate();
    }

    private CnosDBOtherQuery generate() {
        CnosDBTable table = globalState.getSchema().getRandomTable();
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("Column time cannot be null.");
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT ");
        sb.append(table.getName());
        List<CnosDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append("(");
        sb.append(columns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
        sb.append(")");
        sb.append(" VALUES");

        int n = Randomly.smallNumber() + 1;
        for (int i = 0; i < n; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            insertRow(sb, columns);
        }

        // error
        return new CnosDBOtherQuery(sb.toString(), errors);
    }

    private void insertRow(StringBuilder sb, List<CnosDBColumn> columns) {
        CnosDBExpressionGenerator gen = new CnosDBExpressionGenerator(globalState);
        sb.append("(");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            CnosDBExpression generateConstant = gen.generateConstant(columns.get(i).getType());
            sb.append(CnosDBVisitor.asString(generateConstant));
        }
        sb.append(")");
    }

}
