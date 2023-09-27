package sqlancer.tidb.gen;

import java.sql.SQLException;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBTable;

public final class TiDBIndexGenerator {

    private TiDBIndexGenerator() {
    }

    public static SQLQueryAdapter getQuery(TiDBGlobalState globalState) throws SQLException {
        if (globalState.getSchema().getIndexCount() > globalState.getDbmsSpecificOptions().maxNumIndexes) {
            throw new IgnoreMeException();
        }
        ExpectedErrors errors = new ExpectedErrors();

        TiDBTable randomTable = globalState.getSchema().getRandomTable(t -> !t.isView());
        String indexName = globalState.getSchema().getFreeIndexName();
        StringBuilder sb = new StringBuilder("CREATE ");
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append("UNIQUE ");
            errors.add("Duplicate for key");
            errors.add("Duplicate entry ");
            errors.add("A UNIQUE INDEX must include all columns in the table's partitioning function");
        }
        sb.append("INDEX ");
        sb.append(indexName);
        sb.append(" ON ");
        sb.append(randomTable.getName());
        sb.append("(");
        int nr = Math.min(Randomly.smallNumber() + 1, randomTable.getColumns().size());
        List<TiDBColumn> subset = Randomly.extractNrRandomColumns(randomTable.getColumns(), nr);
        for (int i = 0; i < subset.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(subset.get(i).getName());
            if (!randomTable.isView()) {
                // TODO: otherwise: Incorrect prefix key; the used key part isn't a string, the
                // used length is longer than the key part, or the storage engine doesn't
                // support unique prefix keys
                TiDBTableGenerator.appendSpecifiers(sb, subset.get(i).getType().getPrimitiveDataType());
            }
            if (Randomly.getBoolean()) {
                sb.append(" ");
                sb.append(Randomly.fromOptions("ASC", "DESC"));
            }
        }
        sb.append(")");
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(" KEY_BLOCK_SIZE ");
            sb.append(Randomly.getPositiveOrZeroNonCachedInteger());
        }
        errors.add("Cannot decode index value, because"); // invalid value for generated column
        errors.add("index already exist");
        errors.add("Data truncation");
        errors.add("key was too long");
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
