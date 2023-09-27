package sqlancer.cockroachdb.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.common.gen.AbstractUpdateGenerator;
import sqlancer.common.query.SQLQueryAdapter;

public final class CockroachDBUpdateGenerator extends AbstractUpdateGenerator<CockroachDBColumn> {

    private final CockroachDBGlobalState globalState;
    private CockroachDBExpressionGenerator gen;

    private CockroachDBUpdateGenerator(CockroachDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter gen(CockroachDBGlobalState globalState) {
        return new CockroachDBUpdateGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        CockroachDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<CockroachDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        gen = new CockroachDBExpressionGenerator(globalState).setColumns(columns);
        sb.append("UPDATE ");
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append("@{FORCE_INDEX=");
            sb.append(Randomly.fromList(table.getIndexes()).getIndexName());
            sb.append("}");
        }
        sb.append(" SET ");
        updateColumns(columns);
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

    @Override
    protected void updateValue(CockroachDBColumn column) {
        sb.append(CockroachDBVisitor.asString(gen.generateExpression(column.getType())));
    }

}
