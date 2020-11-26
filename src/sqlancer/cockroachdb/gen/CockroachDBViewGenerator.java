package sqlancer.cockroachdb.gen;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;

public final class CockroachDBViewGenerator {

    private CockroachDBViewGenerator() {
    }

    public static SQLQueryAdapter generate(CockroachDBGlobalState globalState) {
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
        ExpectedErrors errors = new ExpectedErrors();
        CockroachDBErrors.addExpressionErrors(errors);
        CockroachDBErrors.addTransactionErrors(errors);
        errors.add("value type unknown cannot be used for table columns");
        errors.add("already exists");
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
