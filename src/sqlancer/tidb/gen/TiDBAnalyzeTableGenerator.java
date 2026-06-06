package sqlancer.tidb.gen;

import java.sql.SQLException;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.TableIndex;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBTable;

public final class TiDBAnalyzeTableGenerator {

    private TiDBAnalyzeTableGenerator() {
    }

    public static SQLQueryAdapter getQuery(TiDBGlobalState globalState) throws SQLException {
        ExpectedErrors errors = ExpectedErrors.newErrors().with(TiDBErrors.getExpressionErrors()).build();
        TiDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<TableIndex> indexes = table.getIndexes();
        indexes.removeIf(index -> index.getIndexName().contains("PRIMARY"));
        boolean analyzeIndex = !indexes.isEmpty() && Randomly.getBoolean();
        StringBuilder sb = new StringBuilder("ANALYZE TABLE ");
        sb.append(table.getName());
        if (analyzeIndex) {
            sb.append(" INDEX ");
            sb.append(Randomly.fromList(indexes).getIndexName());
        }
        if (!analyzeIndex && Randomly.getBoolean()) {
            sb.append(" ALL COLUMNS");
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
