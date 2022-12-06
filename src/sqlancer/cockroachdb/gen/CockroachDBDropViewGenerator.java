package sqlancer.cockroachdb.gen;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;

public final class CockroachDBDropViewGenerator {

    private CockroachDBDropViewGenerator() {
    }

    public static SQLQueryAdapter drop(CockroachDBGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("is referenced by foreign key");

        StringBuilder sb = new StringBuilder();
        sb.append("DROP");
        if (Randomly.getBoolean()) {
            sb.append(" MATERIALIZED");
        }
        sb.append(" VIEW");
        sb.append(" ");
        if (Randomly.getBooleanWithRatherLowProbability()) {
            for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(globalState.getSchema().getRandomTable(t -> t.isView()).getName());
            }
        } else {
            sb.append(globalState.getSchema().getRandomTable(t -> t.isView()).getName());
        }
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("CASCADE", "RESTRICT"));
        }
        return new SQLQueryAdapter(sb.toString(), true);
    }

}
