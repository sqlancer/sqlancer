package sqlancer.presto.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractIndexGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.PrestoSchema.PrestoColumn;
import sqlancer.presto.PrestoSchema.PrestoTable;
import sqlancer.presto.PrestoToStringVisitor;
import sqlancer.presto.ast.PrestoExpression;

public class PrestoIndexGenerator extends AbstractIndexGenerator<PrestoColumn> {

    private final PrestoGlobalState globalState;

    public PrestoIndexGenerator(PrestoGlobalState globalState) {
        this.globalState = globalState;
        this.canAffectSchema = true;
        this.canonicalizeString = false;
    }

    public static SQLQueryAdapter getQuery(PrestoGlobalState globalState) {
        return new PrestoIndexGenerator(globalState).getStatement();
    }

    @Override
    public void buildStatement() {
        boolean unique = Randomly.getBoolean();
        if (unique) {
            errors.add("Cant create unique index, table contains duplicate data on indexed column(s)");
        }
        appendCreateIndex(unique);
        sb.append(Randomly.fromOptions("i0", "i1", "i2", "i3", "i4")); // cannot query this information
        sb.append(" ON ");
        PrestoTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        sb.append("(");
        List<PrestoColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append(" ");
            if (Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(Randomly.fromOptions("ASC", "DESC"));
            }
        }
        sb.append(")");
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            PrestoExpression expr = new PrestoTypedExpressionGenerator(globalState).setColumns(table.getColumns())
                    .generateExpression(PrestoSchema.PrestoCompositeDataType.getRandomWithoutNull());
            sb.append(PrestoToStringVisitor.asString(expr));
        }
        errors.add("already exists!");
    }

}
