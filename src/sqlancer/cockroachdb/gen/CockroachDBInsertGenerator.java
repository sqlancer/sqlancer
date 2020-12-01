package sqlancer.cockroachdb.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;

public final class CockroachDBInsertGenerator {

    private CockroachDBInsertGenerator() {
    }

    public static SQLQueryAdapter insert(CockroachDBGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();

        CockroachDBErrors.addExpressionErrors(errors); // e.g., caused by computed columns
        errors.add("violates not-null constraint");
        errors.add("violates unique constraint");
        errors.add("primary key column");
        errors.add("cannot write directly to computed column"); // TODO: do not select generated columns

        errors.add("failed to satisfy CHECK constraint");

        errors.add("violates foreign key constraint");
        errors.add("foreign key violation");
        errors.add("multi-part foreign key");
        StringBuilder sb = new StringBuilder();
        CockroachDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        boolean isUpsert = Randomly.getBoolean();
        if (!isUpsert) {
            sb.append("INSERT INTO ");
        } else {
            sb.append("UPSERT INTO ");
            errors.add("UPSERT or INSERT...ON CONFLICT command cannot affect row a second time");
        }
        sb.append(table.getName());
        sb.append(" ");
        CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(globalState);
        if (Randomly.getBooleanWithSmallProbability()) {
            sb.append("DEFAULT VALUES");
        } else {
            List<CockroachDBColumn> columns = table.getRandomNonEmptyColumnSubset();
            sb.append("(");
            sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
            sb.append(")");
            sb.append(" VALUES");
            for (int j = 0; j < Randomly.smallNumber() + 1; j++) {
                if (j != 0) {
                    sb.append(", ");
                }
                sb.append("(");
                int i = 0;
                for (CockroachDBColumn c : columns) {
                    if (i++ != 0) {
                        sb.append(", ");
                    }
                    sb.append(CockroachDBVisitor.asString(gen.generateConstant(c.getType())));
                }
                sb.append(")");
            }
        }
        if (Randomly.getBoolean() && !isUpsert) {
            sb.append(" ON CONFLICT (");
            sb.append(table.getRandomNonEmptyColumnSubset().stream().map(c -> c.getName())
                    .collect(Collectors.joining(", ")));
            sb.append(")");
            // WHERE clause not yet implemented, see
            // https://github.com/cockroachdb/cockroach/issues/32557
            sb.append(" DO ");
            if (Randomly.getBoolean()) {
                sb.append(" NOTHING ");
            } else {
                // TODO: also support excluded. (see
                // https://www.cockroachlabs.com/docs/stable/insert.html)
                sb.append(" UPDATE SET ");
                List<CockroachDBColumn> columns = table.getRandomNonEmptyColumnSubset();
                int i = 0;
                for (CockroachDBColumn c : columns) {
                    if (i++ != 0) {
                        sb.append(", ");
                    }
                    sb.append(c.getName());
                    sb.append(" = ");
                    sb.append(CockroachDBVisitor.asString(gen.generateConstant(c.getType())));
                }
                errors.add("UPSERT or INSERT...ON CONFLICT command cannot affect row a second time");
            }
            errors.add("there is no unique or exclusion constraint matching the ON CONFLICT specification");
        }
        CockroachDBErrors.addTransactionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
