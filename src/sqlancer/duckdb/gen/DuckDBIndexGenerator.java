package sqlancer.duckdb.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;

public final class DuckDBIndexGenerator {

    private DuckDBIndexGenerator() {
    }

    public static SQLQueryAdapter getQuery(DuckDBGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (Randomly.getBoolean()) {
            errors.add("Data contains duplicates on indexed column(s)");
            sb.append("UNIQUE ");
        }
        sb.append("INDEX ");
        sb.append(Randomly.fromOptions("i0", "i1", "i2", "i3", "i4")); // cannot query this information
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
        errors.add("already exists!");
        if (globalState.getDbmsSpecificOptions().testRowid) {
            errors.add("cannot create an index on the rowid");
        }
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
