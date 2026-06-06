package sqlancer.yugabyte.ycql.gen;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractIndexGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ycql.YCQLProvider.YCQLGlobalState;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLColumn;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLTable;
import sqlancer.yugabyte.ycql.YCQLToStringVisitor;
import sqlancer.yugabyte.ycql.ast.YCQLExpression;

public class YCQLIndexGenerator extends AbstractIndexGenerator<YCQLColumn> {

    private final YCQLGlobalState globalState;

    public YCQLIndexGenerator(YCQLGlobalState globalState) {
        this.globalState = globalState;
        this.canAffectSchema = true;
    }

    public static SQLQueryAdapter getQuery(YCQLGlobalState globalState) {
        return new YCQLIndexGenerator(globalState).getStatement();
    }

    @Override
    public void buildStatement() {
        boolean unique = Randomly.getBoolean();
        if (unique) {
            errors.add("Cant create unique index, table contains duplicate data on indexed column(s)");
        }
        appendCreateIndex(unique);
        sb.append(Randomly.fromOptions("i0", "i1", "i2", "i3", "i4"));
        sb.append(" ON ");
        YCQLTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        appendIndexColumnList(table.getRandomNonEmptyColumnSubset(), false);
        if (Randomly.getBoolean()) {
            YCQLExpression expr = new YCQLExpressionGenerator(globalState).setColumns(table.getColumns())
                    .generateExpression();
            appendWhereClause(YCQLToStringVisitor.asString(expr));
        }
        errors.add("Query timed out after PT2S");
        errors.add("Invalid SQL Statement");
        errors.add("Invalid CQL Statement");
        errors.add(
                "Invalid Table Definition. Transactions cannot be enabled in an index of a table without transactions enabled.");
    }

}
