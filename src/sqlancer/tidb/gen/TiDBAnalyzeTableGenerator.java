package sqlancer.tidb.gen;

import java.sql.SQLException;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBTable;

public final class TiDBAnalyzeTableGenerator {

    private TiDBAnalyzeTableGenerator() {
    }

    public static SQLQueryAdapter getQuery(TiDBGlobalState globalState) throws SQLException {
        ExpectedErrors errors = new ExpectedErrors();
        TiDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        boolean analyzeIndex = !table.getIndexes().isEmpty() && Randomly.getBoolean();
        StringBuilder sb = new StringBuilder("ANALYZE ");
        if (analyzeIndex && Randomly.getBoolean()) {
            sb.append("INCREMENTAL ");
        }
        sb.append("TABLE ");
        sb.append(table.getName());
        if (analyzeIndex) {
            sb.append(" INDEX ");
            sb.append(table.getRandomIndex().getIndexName());
        }
        if (Randomly.getBoolean()) {
            sb.append(" WITH ");
            sb.append(Randomly.getNotCachedInteger(1, 1024));
            sb.append(" BUCKETS");
        }
        errors.add("Fast analyze hasn't reached General Availability and only support analyze version 1 currently");
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
