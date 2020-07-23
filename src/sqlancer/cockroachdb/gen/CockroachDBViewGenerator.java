package sqlancer.cockroachdb.gen;

import java.util.HashSet;
import java.util.Set;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;

public final class CockroachDBViewGenerator {

    private CockroachDBViewGenerator() {
    }

    public static Query generate(CockroachDBGlobalState globalState) {
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
        sb.append(CockroachDBRandomQuerySynthesizer.generate(globalState, nrColumns).getQueryString());
        Set<String> errors = new HashSet<>();
        CockroachDBErrors.addExpressionErrors(errors);
        CockroachDBErrors.addTransactionErrors(errors);
        errors.add("value type unknown cannot be used for table columns");
        errors.add("already exists");
        return new QueryAdapter(sb.toString(), errors, true);
    }

}
