package sqlancer.cockroachdb.gen;

import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBBugs;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.common.query.SQLQueryAdapter;

// https://www.cockroachlabs.com/docs/stable/create-index.html
public class CockroachDBIndexGenerator extends CockroachDBGenerator {

    public CockroachDBIndexGenerator(CockroachDBGlobalState globalState) {
        super(globalState);
    }

    public static SQLQueryAdapter create(CockroachDBGlobalState s) {
        if (s.getSchema().getIndexCount() >= s.getDbmsSpecificOptions().maxNumIndexes) {
            throw new IgnoreMeException();
        }
        return new CockroachDBIndexGenerator(s).getQuery();
    }

    @Override
    public void buildStatement() {
        // TODO inverted index
        errors.add("is part of the primary index and therefore implicit in all indexes");
        errors.add("already contains column");
        errors.add("violates unique constraint");
        errors.add("schema change statement cannot follow a statement that has written in the same transaction");
        errors.add("and thus is not indexable"); // array types are not indexable
        errors.add("the following columns are not indexable due to their type"); // array types are not indexable
        errors.add("cannot determine type of empty array. Consider annotating with the desired type");
        errors.add("incompatible IF expression"); // TODO: investigate; seems to be a bug
        if (CockroachDBBugs.bug84154) {
            errors.add("overflow during Encode");
            errors.add("of type interval");
        }
        CockroachDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append("CREATE ");
        if (Randomly.getBoolean()) {
            sb.append("UNIQUE ");
        }
        sb.append("INDEX ON ");
        sb.append(table.getName());
        List<CockroachDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        addColumns(sb, columns, true);
        boolean hashSharded = globalState.getDbmsSpecificOptions().testHashIndexes
                && Randomly.getBooleanWithSmallProbability();
        if (hashSharded) {
            sb.append(" USING HASH WITH BUCKET_COUNT=");
            sb.append(Randomly.getNotCachedInteger(2, 2048));
            errors.add("null value in column");
            errors.add("cannot create a sharded index on a computed column");
        }
        if (Randomly.getBoolean()) {
            List<CockroachDBColumn> columns2 = table.getRandomNonEmptyColumnSubset();
            columns2.removeAll(columns);
            if (!columns2.isEmpty()) {
                sb.append(" ");
                sb.append(Randomly.fromOptions("STORING", "COVERING"));
                sb.append(" ");
                addColumns(sb, columns2, false);
            }
        }
    }

}
