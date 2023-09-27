package sqlancer.stonedb.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema;

public class StoneDBIndexDropGenerator {
    // which table to drop index
    StoneDBSchema.StoneDBTable table;
    private final StringBuilder sb = new StringBuilder();
    ExpectedErrors errors = new ExpectedErrors();

    public StoneDBIndexDropGenerator(StoneDBGlobalState globalState) {
        table = globalState.getSchema().getRandomTable();
    }

    public static SQLQueryAdapter generate(StoneDBGlobalState globalState) {
        return new StoneDBIndexDropGenerator(globalState).getQuery();
    }

    private SQLQueryAdapter getQuery() {
        if (!table.hasIndexes()) {
            return null;
        }
        sb.append("DROP INDEX ");
        sb.append(table.getRandomIndex().getIndexName());
        sb.append(" ON ");
        sb.append(table.getName());
        appendAlgoOrLockOption();
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private void appendAlgoOrLockOption() {
        if (Randomly.getBoolean()) {
            return;
        }
        if (Randomly.getBoolean()) {
            sb.append(Randomly.fromOptions(" ALGORITHM ", " ALGORITHM = "));
            sb.append(Randomly.fromOptions("DEFAULT", "INPLACE", "COPY"));
        } else {
            sb.append(Randomly.fromOptions(" LOCK ", " LOCK = "));
            sb.append(Randomly.fromOptions("DEFAULT", "NONE", "SHARED", "EXCLUSIVE"));
        }
    }
}
