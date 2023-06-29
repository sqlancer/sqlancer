package sqlancer.stonedb.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema.StoneDBTable;

public class StoneDBIndexGenerator {
    private final StoneDBGlobalState globalState;
    // which table to add index
    StoneDBTable table;
    private final StringBuilder sb = new StringBuilder();
    ExpectedErrors errors = new ExpectedErrors();

    public StoneDBIndexGenerator(StoneDBGlobalState globalState) {
        this.globalState = globalState;
        table = globalState.getSchema().getRandomTable();
    }

    public static SQLQueryAdapter generate(StoneDBGlobalState globalState) {
        return new StoneDBIndexGenerator(globalState).getQuery();
    }

    private SQLQueryAdapter getQuery() {
        sb.append("CREATE ");
        sb.append(Randomly.fromOptions("UNIQUE", "FULLTEXT", "SPATIAL"));
        sb.append(" INDEX ");
        sb.append(globalState.getSchema().getFreeIndexName());
        appendIndexType();
        sb.append(" ON ");
        sb.append(table.getName());
        appendKeyPart();
        appendIndexOption();
        appendAlgoOrLockOption();
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private void appendIndexType() {
        if (Randomly.getBoolean()) {
            return;
        }
        sb.append(" USING ");
        sb.append(Randomly.fromOptions("BTREE", "HASH"));
    }

    private void appendKeyPart() {
        sb.append("(");
        sb.append(table.getRandomColumn().getName());
        if (Randomly.getBoolean()) {
            sb.append(Randomly.fromOptions("ASC", "DESC"));
        }
        sb.append(")");
    }

    private void appendIndexOption() {
        if (Randomly.getBoolean()) {
            return;
        }
        sb.append(Randomly.fromOptions(" VISIBLE", " INVISIBLE"));
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
