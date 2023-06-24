package sqlancer.stonedb.gen;

import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema.StoneDBTable;
import sqlancer.stonedb.StoneDBToStringVisitor;

public final class StoneDBDeleteGenerator {
    private final StoneDBGlobalState globalState;
    private final StringBuilder sb = new StringBuilder();
    ExpectedErrors errors = new ExpectedErrors();

    private StoneDBDeleteGenerator(StoneDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter generate(StoneDBGlobalState globalState) {
        return new StoneDBDeleteGenerator(globalState).getQuery();
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
            sb.append(" AS ");
            sb.append(globalState.getSchema().getRandomTable().getName());
        }
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(StoneDBToStringVisitor.asString(new StoneDBExpressionGenerator(globalState)
                    .setColumns(randomTable.getColumns()).generateExpression()));
        }
        if (Randomly.getBoolean()) {
            sb.append(" ORDER BY ");
            sb.append(Randomly.fromOptions(
                    randomTable.getColumns().stream().map(AbstractTableColumn::getName).collect(Collectors.toList())));
        }
        if (Randomly.getBoolean()) {
            sb.append(" LIMIT ");
            sb.append(new Randomly().getInteger());
        }
        return new SQLQueryAdapter(sb.toString(), errors);
    }
}
