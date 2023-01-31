package sqlancer.cockroachdb.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;

public final class CockroachDBDropTableGenerator {

    private CockroachDBDropTableGenerator() {
    }

    public static SQLQueryAdapter drop(CockroachDBGlobalState globalState) {
        if (globalState.getSchema().getTables(t -> !t.isView()).size() <= 1) {
            throw new IgnoreMeException();
        }

        ExpectedErrors errors = new ExpectedErrors();
        errors.add("is referenced by foreign key");

        StringBuilder sb = new StringBuilder();
        sb.append("DROP");
        sb.append(" TABLE");
        sb.append(" ");
        sb.append(globalState.getSchema().getRandomTable(t -> !t.isView()).getName());

        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("CASCADE", "RESTRICT"));
        }
        return new SQLQueryAdapter(sb.toString(), true);
    }

}
