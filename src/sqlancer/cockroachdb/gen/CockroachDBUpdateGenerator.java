package sqlancer.cockroachdb.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;

public final class CockroachDBUpdateGenerator {

    private CockroachDBUpdateGenerator() {
    }

    public static SQLQueryAdapter gen(CockroachDBGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        CockroachDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<CockroachDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(globalState).setColumns(columns);
        StringBuilder sb = new StringBuilder("UPDATE ");
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append("@{FORCE_INDEX=");
            sb.append(Randomly.fromList(table.getIndexes()).getIndexName());
            sb.append("}");
        }
        sb.append(" SET ");
        int i = 0;
        for (CockroachDBColumn c : columns) {
            if (i++ != 0) {
                sb.append(", ");
            }
            sb.append(c.getName());
            sb.append("=");
            sb.append(CockroachDBVisitor.asString(gen.generateExpression(c.getType())));
        }
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(CockroachDBVisitor.asString(gen.generateExpression(CockroachDBDataType.BOOL.get())));
        }
        errors.add("violates unique constraint");
        errors.add("violates not-null constraint");
        errors.add("violates foreign key constraint");
        errors.add("UPDATE without WHERE clause (sql_safe_updates = true)");
        errors.add("numeric constant out of int64 range");
        errors.add("failed to satisfy CHECK constraint");
        errors.add("cannot write directly to computed column");
        CockroachDBErrors.addExpressionErrors(errors);
        CockroachDBErrors.addTransactionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
