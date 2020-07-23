package sqlancer.duckdb.gen;

import java.util.HashSet;
import java.util.Set;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBToStringVisitor;

public final class DuckDBViewGenerator {

    private DuckDBViewGenerator() {
    }

    public static Query generate(DuckDBGlobalState globalState) {
        int nrColumns = Randomly.smallNumber() + 1;
        StringBuilder sb = new StringBuilder("CREATE ");
        sb.append("VIEW ");
        sb.append(globalState.getSchema().getFreeViewName());
        sb.append("(");
        for (int i = 0; i < nrColumns; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append("c");
            sb.append(i);
        }
        sb.append(") AS ");
        sb.append(DuckDBToStringVisitor.asString(DuckDBRandomQuerySynthesizer.generateSelect(globalState, nrColumns)));
        Set<String> errors = new HashSet<>();
        DuckDBErrors.addExpressionErrors(errors);
        DuckDBErrors.addGroupByErrors(errors);
        return new QueryAdapter(sb.toString(), errors, true);
    }

}
