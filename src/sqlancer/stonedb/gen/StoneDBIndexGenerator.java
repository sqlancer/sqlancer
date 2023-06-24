package sqlancer.stonedb.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema.StoneDBTable;

public class StoneDBIndexGenerator {
    private final StoneDBGlobalState globalState;
    private final StringBuilder sb = new StringBuilder();
    ExpectedErrors errors = new ExpectedErrors();

    public StoneDBIndexGenerator(StoneDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter generate(StoneDBGlobalState globalState) {
        return new StoneDBIndexGenerator(globalState).getQuery();
    }

    private SQLQueryAdapter getQuery() {
        sb.append("CREATE ");
        sb.append(Randomly.fromOptions("UNIQUE", "FULLTEXT", "SPATIAL"));
        sb.append(" INDEX");
        sb.append(globalState.getSchema().getFreeIndexName());
        appendIndexType();
        sb.append(" ON ");
        StoneDBTable table = globalState.getSchema().getRandomTable();
        sb.append(table.getName());
        appendKeyPart(table);
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

    private void appendKeyPart(StoneDBTable table) {
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
