package sqlancer.voltdb.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.voltdb.VoltDBErrors;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.VoltDBSchema.VoltDBTable;
import sqlancer.voltdb.VoltDBToStringVisitor;

/** DELETE from one table, almost always with a WHERE clause, so that a database keeps some of its rows. */
public final class VoltDBDeleteGenerator {

    private VoltDBDeleteGenerator() {
    }

    public static SQLQueryAdapter getQuery(VoltDBGlobalState globalState) {
        return getQuery(globalState, globalState.getSchema().getRandomTable());
    }

    public static SQLQueryAdapter getQuery(VoltDBGlobalState globalState, VoltDBTable table) {
        StringBuilder sql = new StringBuilder("DELETE FROM ").append(table.getName());
        if (!Randomly.getBooleanWithSmallProbability()) {
            VoltDBExpressionGenerator gen = new VoltDBExpressionGenerator(globalState).setColumns(table.getColumns());
            sql.append(" WHERE ").append(VoltDBToStringVisitor.asString(gen.generateBooleanExpression()));
        }
        ExpectedErrors errors = ExpectedErrors.newErrors().build();
        VoltDBErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sql.toString(), errors);
    }

}
