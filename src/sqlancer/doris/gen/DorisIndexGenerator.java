package sqlancer.doris.gen;

import java.sql.SQLException;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisTable;

public final class DorisIndexGenerator {

    private DorisIndexGenerator() {
    }

    public static SQLQueryAdapter getQuery(DorisGlobalState globalState) throws SQLException {
        if (globalState.getSchema().getIndexCount() > globalState.getDbmsSpecificOptions().maxNumIndexes) {
            throw new IgnoreMeException();
        }
        ExpectedErrors errors = new ExpectedErrors();

        DorisTable randomTable = globalState.getSchema().getRandomTable(t -> !t.isView());
        String indexName = globalState.getSchema().getFreeIndexName();
        StringBuilder sb = new StringBuilder("CREATE ");
        sb.append("INDEX ");
        if (Randomly.getBoolean()) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(indexName);
        sb.append(" ON ");
        sb.append(randomTable.getName());
        sb.append("(");
        int nr = 1; // Doris Only support CREATE_INDEX on single column and index type is BITMAP;
        List<DorisColumn> subset = Randomly.extractNrRandomColumns(randomTable.getColumns(), nr);
        sb.append(subset.get(0).getName());
        sb.append(") ");
        if (Randomly.getBoolean()) {
            sb.append("USING BITMAP ");
        }
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
