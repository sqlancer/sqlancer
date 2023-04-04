package sqlancer.materialize.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeSchema.MaterializeTable;

public final class MaterializeIndexGenerator {

    private MaterializeIndexGenerator() {
    }

    public enum IndexType {
        BTREE, HASH, GIST, GIN
    }

    public static SQLQueryAdapter generate(MaterializeGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE");
        sb.append(" INDEX ");
        MaterializeTable randomTable = globalState.getSchema().getRandomTable(t -> !t.isView()); // TODO: materialized
                                                                                                 // views
        sb.append(MaterializeCommon.getFreeIndexName(globalState.getSchema()));
        sb.append(" ON ");
        sb.append(randomTable.getName());
        IndexType method;
        method = IndexType.BTREE;

        sb.append("(");
        if (method == IndexType.HASH) {
            sb.append(randomTable.getRandomColumn().getName());
        } else {
            for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(randomTable.getRandomColumn().getName());
                if (Randomly.getBoolean()) {
                    sb.append(" ");
                    sb.append(Randomly.fromOptions("ASC", "DESC"));
                }
                if (Randomly.getBooleanWithRatherLowProbability()) {
                    sb.append(" NULLS ");
                    sb.append(Randomly.fromOptions("FIRST", "LAST"));
                }
            }
        }

        sb.append(")");
        errors.add("already contains data"); // CONCURRENT INDEX failed
        errors.add("You might need to add explicit type casts");
        errors.add(" collations are not supported");
        errors.add("because it has pending trigger events");
        errors.add("could not determine which collation to use for index expression");
        errors.add("could not determine which collation to use for string comparison");
        errors.add("is duplicated");
        errors.add("access method \"gin\" does not support unique indexes");
        errors.add("access method \"hash\" does not support unique indexes");
        errors.add("already exists");
        errors.add("could not create unique index");
        errors.add("has no default operator class");
        errors.add("does not support");
        errors.add("does not support casting");
        errors.add("unsupported UNIQUE constraint with partition key definition");
        errors.add("insufficient columns in UNIQUE constraint definition");
        errors.add("invalid input syntax for");
        errors.add("must be type ");
        errors.add("integer out of range");
        errors.add("division by zero");
        errors.add("out of range");
        errors.add("functions in index predicate must be marked IMMUTABLE");
        errors.add("functions in index expression must be marked IMMUTABLE");
        errors.add("result of range difference would not be contiguous");
        errors.add("which is part of the partition key");
        MaterializeCommon.addCommonExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }
}
