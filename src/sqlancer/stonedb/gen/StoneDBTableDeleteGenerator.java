package sqlancer.stonedb.gen;

import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.stonedb.StoneDBBugs;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema.StoneDBTable;
import sqlancer.stonedb.StoneDBToStringVisitor;

public final class StoneDBTableDeleteGenerator {
    private final StoneDBGlobalState globalState;
    private final StringBuilder sb = new StringBuilder();
    ExpectedErrors errors = new ExpectedErrors();
    Randomly r;

    private StoneDBTableDeleteGenerator(StoneDBGlobalState globalState) {
        this.globalState = globalState;
        r = globalState.getRandomly();
    }

    public static SQLQueryAdapter generate(StoneDBGlobalState globalState) {
        return new StoneDBTableDeleteGenerator(globalState).getQuery();
    }

    public SQLQueryAdapter getQuery() {
        StoneDBTable randomTable = globalState.getSchema().getRandomTable();
        sb.append("DELETE");
        if (Randomly.getBoolean()) {
            sb.append(" LOW_PRIORITY");
        }
        if (Randomly.getBoolean()) {
            sb.append(" QUICK");
        }
        if (Randomly.getBoolean()) {
            sb.append(" IGNORE");
        }
        sb.append(" FROM ");
        sb.append(randomTable.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(StoneDBToStringVisitor.asString(new StoneDBExpressionGenerator(globalState)
                    .setColumns(randomTable.getColumns()).generateExpression()));
        }
        if (!StoneDBBugs.bug1933 && Randomly.getBoolean()) {
            sb.append(" ORDER BY ");
            sb.append(String.join(", ", Randomly.fromOptions(
                    randomTable.getColumns().stream().map(AbstractTableColumn::getName).collect(Collectors.toList())))
                    .replace('[', '(').replace(']', ')'));
        }
        if (Randomly.getBoolean()) {
            sb.append(" LIMIT ");
            sb.append(r.getInteger(0, (int) randomTable.getNrRows(globalState)));
        }
        addExpectedErrors();
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private void addExpectedErrors() {
        // java.sql.SQLException: Incorrect string value: '\xBC\xE7\xC9\x91\x05R...' for column 'c1' at row 1
        errors.add("Incorrect string value: ");
    }
}
