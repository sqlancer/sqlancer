package sqlancer.duckdb.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractIndexGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;

public class DuckDBIndexGenerator extends AbstractIndexGenerator<DuckDBColumn> {

    private final DuckDBGlobalState globalState;

    public DuckDBIndexGenerator(DuckDBGlobalState globalState) {
        this.globalState = globalState;
        this.canAffectSchema = true;
    }

    public static SQLQueryAdapter getQuery(DuckDBGlobalState globalState) {
        return new DuckDBIndexGenerator(globalState).getStatement();
    }

    @Override
    public void buildStatement() {
        boolean unique = Randomly.getBoolean();
        if (unique) {
            errors.add("Data contains duplicates on indexed column(s)");
        }
        appendCreateIndex(unique);
        sb.append(globalState.getSchema().getFreeIndexName());
        sb.append(" ON ");
        DuckDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        sb.append("(");
        List<DuckDBColumn> columns = table.getRandomNonEmptyColumnSubset();
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
        if (globalState.getDbmsSpecificOptions().testRowid) {
            errors.add("cannot create an index on the rowid");
        }
    }

}
