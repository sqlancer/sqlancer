package sqlancer.cockroachdb.gen;

import java.util.Arrays;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;

public final class CockroachDBCreateStatisticsGenerator {

    private CockroachDBCreateStatisticsGenerator() {
    }

    public static Query create(CockroachDBGlobalState globalState) {
        CockroachDBTable randomTable = globalState.getSchema().getRandomTable(t -> !t.isView());
        StringBuilder sb = new StringBuilder("CREATE STATISTICS s");
        sb.append(Randomly.smallNumber());
        if (Randomly.getBoolean()) {
            sb.append(" ON ");
            sb.append(randomTable.getRandomColumn().getName());
        }
        sb.append(" FROM ");
        sb.append(randomTable.getName());

        Query q = new QueryAdapter(sb.toString(),
                Arrays.asList("current transaction is aborted, commands ignored until end of transaction block",
                        "ERROR: unable to encode table key: *tree.DArray" /*
                                                                           * https://github.com/cockroachdb/cockroach/
                                                                           * issues/46964
                                                                           */, "overflow during Encode"));
        return q;
    }

}
