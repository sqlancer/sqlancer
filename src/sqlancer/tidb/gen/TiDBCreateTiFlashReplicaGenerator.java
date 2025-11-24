package sqlancer.tidb.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema;

public final class TiDBCreateTiFlashReplicaGenerator {
    private TiDBCreateTiFlashReplicaGenerator() {
    }

    public static SQLQueryAdapter getQuery(TiDBGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        TiDBSchema.TiDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        StringBuilder sb = new StringBuilder("ALTER TABLE ");
        sb.append(table.getName());
        sb.append(" SET TIFLASH REPLICA 1;");
        return new SQLQueryAdapter(sb.toString(), errors);
    }
}
