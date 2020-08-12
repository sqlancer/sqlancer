package sqlancer.tidb.gen;

import java.sql.SQLException;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.QueryAdapter;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.visitor.TiDBVisitor;

public final class TiDBUpdateGenerator {

    private TiDBUpdateGenerator() {
    }

    public static Query getQuery(TiDBGlobalState globalState) throws SQLException {
        ExpectedErrors errors = new ExpectedErrors();
        TiDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        TiDBExpressionGenerator gen = new TiDBExpressionGenerator(globalState).setColumns(table.getColumns());
        StringBuilder sb = new StringBuilder("UPDATE ");
        sb.append(table.getName());
        sb.append(" SET ");
        List<TiDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append("=");
            if (Randomly.getBoolean()) {
                sb.append(gen.generateConstant());
            } else {
                sb.append(TiDBVisitor.asString(gen.generateExpression()));
                TiDBErrors.addExpressionErrors(errors);
            }
        }
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            TiDBErrors.addExpressionErrors(errors);
            sb.append(TiDBVisitor.asString(gen.generateExpression()));
            errors.add("Data Too Long"); // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/43
        }
        TiDBErrors.addInsertErrors(errors);

        return new QueryAdapter(sb.toString(), errors);
    }

}
