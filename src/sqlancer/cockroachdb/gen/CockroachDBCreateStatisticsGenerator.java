package sqlancer.cockroachdb.gen;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;

public final class CockroachDBCreateStatisticsGenerator {

    private CockroachDBCreateStatisticsGenerator() {
    }

    public static SQLQueryAdapter create(CockroachDBGlobalState globalState) {
        CockroachDBTable randomTable = globalState.getSchema().getRandomTable(t -> !t.isView());
        StringBuilder sb = new StringBuilder("CREATE STATISTICS s");
        sb.append(Randomly.smallNumber());
        if (Randomly.getBoolean()) {
            sb.append(" ON ");
            sb.append(randomTable.getRandomColumn().getName());
        }
        sb.append(" FROM ");
        sb.append(randomTable.getName());

        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("overflow during Encode")); // https://github.com/cockroachdb/cockroach/issues/84078
    }

}
