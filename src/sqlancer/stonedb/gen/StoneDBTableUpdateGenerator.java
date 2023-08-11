package sqlancer.stonedb.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema;
import sqlancer.stonedb.StoneDBSchema.StoneDBTable;
import sqlancer.stonedb.StoneDBToStringVisitor;

public class StoneDBTableUpdateGenerator {
    private final StoneDBGlobalState globalState;
    // which table to insert into
    private final StoneDBTable table;
    private final StringBuilder sb = new StringBuilder();
    ExpectedErrors errors = new ExpectedErrors();

    public StoneDBTableUpdateGenerator(StoneDBGlobalState globalState) {
        this.globalState = globalState;
        table = globalState.getSchema().getRandomTable();
    }

    public static SQLQueryAdapter generate(StoneDBGlobalState globalState) {
        return new StoneDBTableUpdateGenerator(globalState).getQuery();
    }

    public SQLQueryAdapter getQuery() {
        sb.append("UPDATE");
        sb.append(Randomly.fromOptions(" ", " LOW_PRIORITY ", " LOW_PRIORITY IGNORE ", " IGNORE "));
        sb.append(table.getName());
        sb.append("SET ");
        appendAssignmentList();
        if (Randomly.getBoolean()) {
            appendWhereCondition();
        }
        if (Randomly.getBoolean()) {
            appendOrderBy();
        }
        if (Randomly.getBoolean()) {
            appendLimit();
        }
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    public void appendAssignmentList() {
        for (int i = 0; i < new Randomly().getInteger(1, table.getColumns().size()); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(table.getColumns().get(i));
            sb.append(" = ");
            sb.append(Randomly.fromOptions(
                    StoneDBToStringVisitor.asString(StoneDBSchema.StoneDBDataType.getRandomValue(
                            table.getColumns().get(i).getType().getPrimitiveDataType(), globalState.getRandomly())),
                    "DEFAULT"));
        }
    }

    private void appendWhereCondition() {
    }

    private void appendOrderBy() {
        sb.append(" ORDER BY ");
        sb.append(table.getRandomColumn().getName());
    }

    private void appendLimit() {
        sb.append(" LIMIT ");
        sb.append(new Randomly().getInteger(0, (int) table.getNrRows(globalState)));
    }
}
