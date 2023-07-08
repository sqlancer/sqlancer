package sqlancer.stonedb.gen;

import sqlancer.Randomly;
import sqlancer.Randomly.StringGenerationStrategy;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema.StoneDBColumn;
import sqlancer.stonedb.StoneDBSchema.StoneDBTable;

public class StoneDBIndexCreateGenerator {
    private final StoneDBGlobalState globalState;
    private final Randomly r;
    // which table to add index
    StoneDBTable table;
    private final StringBuilder sb = new StringBuilder();
    ExpectedErrors errors = new ExpectedErrors();

    public StoneDBIndexCreateGenerator(StoneDBGlobalState globalState) {
        this.globalState = globalState;
        r = globalState.getRandomly();
        table = globalState.getSchema().getRandomTable();
    }

    public static SQLQueryAdapter generate(StoneDBGlobalState globalState) {
        return new StoneDBIndexCreateGenerator(globalState).getQuery();
    }

    private SQLQueryAdapter getQuery() {
        sb.append("CREATE ");
        sb.append(Randomly.fromOptions("UNIQUE", "FULLTEXT", "SPATIAL"));
        sb.append(" INDEX ");
        sb.append(globalState.getSchema().getFreeIndexName());
        if (Randomly.getBoolean()) {
            appendIndexType();
        }
        sb.append(" ON ");
        sb.append(table.getName());
        appendKeyParts();
        appendIndexOption();
        appendAlgoOrLockOption();
        addExpectedErrors();
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private void addExpectedErrors() {
        // java.sql.SQLException: Tianmu engine does not support unique index.
        errors.add("Tianmu engine does not support unique index");
        // java.sql.SQLException: Tianmu engine does not support fulltext index.
        errors.add("Tianmu engine does not support fulltext index");
        // java.sql.SQLException: The used table type doesn't support SPATIAL indexes
        errors.add("The used table type doesn't support SPATIAL indexes");
        // java.sql.SQLException: ALGORITHM=INPLACE is not supported for this operation. Try ALGORITHM=COPY.
        errors.add("ALGORITHM=INPLACE is not supported for this operation. Try ALGORITHM=COPY.");
        // java.sql.SQLSyntaxErrorException: Key column 'c0' doesn't exist in table
        errors.add("doesn't exist in table");
        // java.sql.SQLSyntaxErrorException: A SPATIAL index may only contain a geometrical type column
        errors.add("A SPATIAL index may only contain a geometrical type column");
    }

    private void appendIndexType() {
        // The index_type clause cannot be used for FULLTEXT INDEX or SPATIAL INDEX specifications.
        if (sb.toString().contains("FULLTEXT") || sb.toString().contains("SPATIAL")) {
            return;
        }
        if (Randomly.getBoolean()) {
            return;
        }
        sb.append(" USING ");
        sb.append(Randomly.fromOptions("BTREE", "HASH"));
    }

    private void appendKeyParts() {
        int numberOfKeyParts = Randomly.fromOptions(1, 1, 1, 1, table.getColumns().size());
        for (int i = 0; i < numberOfKeyParts; i++) {
            appendKeyPart();
        }
    }

    private void appendKeyPart() {
        sb.append("(");
        StoneDBColumn randomColumn = table.getRandomColumn();
        sb.append(randomColumn.getName());
        if (Randomly.getBoolean()) {
            sb.append(" (").append(Randomly.smallNumber()).append(")");
        }
        if (Randomly.getBoolean()) {
            sb.append(Randomly.fromOptions(" ASC", " DESC"));
        }
        sb.append(")");
    }

    private void appendIndexOption() {
        if (Randomly.getBoolean()) {
            return;
        }
        if (Randomly.getBoolean()) {
            sb.append(Randomly.fromOptions("KEY_BLOCK_SIZE ", "KEY_BLOCK_SIZE = "));
            sb.append(r.getInteger(1, Math.max(1, Randomly.smallNumber())));
            sb.append(" ");
        }
        if (Randomly.getBoolean()) {
            StringGenerationStrategy strategy = Randomly.StringGenerationStrategy.ALPHANUMERIC;
            sb.append(String.format("COMMENT '%s' ", strategy.getString(r)));
        }
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
