package sqlancer.cockroachdb.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.common.gen.AbstractGenerator;

public abstract class CockroachDBGenerator extends AbstractGenerator {

    protected final CockroachDBGlobalState globalState;

    public CockroachDBGenerator(CockroachDBGlobalState globalState) {
        this.globalState = globalState;
    }

    static void addColumns(StringBuilder sb, List<CockroachDBColumn> columns, boolean allowOrdering) {
        sb.append("(");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            if (allowOrdering && Randomly.getBoolean()) {
                sb.append(" ");
                sb.append(Randomly.fromOptions("ASC", "DESC"));
            }
        }
        sb.append(")");
    }

    void generateInterleave() {
        // TODO make this more likely to succeed
        CockroachDBTable parentTable = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<CockroachDBColumn> parentColumns = parentTable.getRandomNonEmptyColumnSubset();
        sb.append(" INTERLEAVE IN PARENT ");
        sb.append(parentTable.getName());
        sb.append("(");
        sb.append(parentColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(")");
        errors.add("must refer to a prefix of the primary key column names being interleaved");
        errors.add("must refer to a prefix of the index column names being interleaved");
        errors.add("must match the parent's primary index");
        errors.add("must match type and sort direction of the parent's primary index");
        errors.add("must be a prefix of the index columns being interleaved");
        errors.add("must be a prefix of the primary key columns being interleaved");
    }

}
