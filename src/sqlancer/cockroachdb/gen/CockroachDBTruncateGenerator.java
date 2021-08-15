package sqlancer.cockroachdb.gen;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;

public final class CockroachDBTruncateGenerator {

    private CockroachDBTruncateGenerator() {
    }

    // https://www.cockroachlabs.com/docs/v19.2/truncate.html
    public static SQLQueryAdapter truncate(CockroachDBGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("is referenced by foreign key");

        // https://github.com/cockroachdb/cockroach/issues/47030
        errors.add("unexpected value: <nil>");
        StringBuilder sb = new StringBuilder();
        sb.append("TRUNCATE");
        if (Randomly.getBoolean()) {
            sb.append(" TABLE");
        }
        sb.append(" ");
        if (Randomly.getBooleanWithRatherLowProbability()) {
            for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(globalState.getSchema().getRandomTable(t -> !t.isView()).getName());
            }
        } else {
            sb.append(globalState.getSchema().getRandomTable(t -> !t.isView()).getName());
        }
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("CASCADE", "RESTRICT"));
        }
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
