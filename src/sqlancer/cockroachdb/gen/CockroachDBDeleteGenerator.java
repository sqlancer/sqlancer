package sqlancer.cockroachdb.gen;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.common.gen.AbstractDeleteGenerator;
import sqlancer.common.query.SQLQueryAdapter;

public final class CockroachDBDeleteGenerator extends AbstractDeleteGenerator {

    private final CockroachDBGlobalState globalState;

    private CockroachDBDeleteGenerator(CockroachDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter delete(CockroachDBGlobalState globalState) {
        return new CockroachDBDeleteGenerator(globalState).getStatement();
    }

    @Override
    public void buildStatement() {
        CockroachDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append("DELETE FROM ");
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            CockroachDBErrors.addExpressionErrors(errors);
            sb.append(CockroachDBVisitor.asString(new CockroachDBExpressionGenerator(globalState)
                    .setColumns(table.getColumns()).generateExpression(CockroachDBDataType.BOOL.get())));
        } else {
            errors.add("rejected: DELETE without WHERE clause (sql_safe_updates = true)");
        }
        errors.add("foreign key violation");
        CockroachDBErrors.addTransactionErrors(errors);
    }

}
