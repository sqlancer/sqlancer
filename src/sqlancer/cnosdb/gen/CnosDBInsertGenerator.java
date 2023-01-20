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

    private CnosDBInsertGenerator() {
    }

    public static CnosDBOtherQuery insert(CnosDBGlobalState globalState) {
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
            insertRow(globalState, sb, columns);
        }

        // error
        return new CnosDBOtherQuery(sb.toString(), errors);
    }

    private static void insertRow(CnosDBGlobalState globalState, StringBuilder sb, List<CnosDBColumn> columns) {
        sb.append("(");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            CnosDBExpression generateConstant = CnosDBExpressionGenerator.generateConstant(globalState.getRandomly(),
                    columns.get(i).getType());
            sb.append(CnosDBVisitor.asString(generateConstant));
        }
        sb.append(")");
    }

}
