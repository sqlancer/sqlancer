package sqlancer.materialize.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeSchema.MaterializeTable;

public final class MaterializeClusterGenerator {

    private MaterializeClusterGenerator() {
    }

    public static SQLQueryAdapter create(MaterializeGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("there is no previously clustered index for table");
        errors.add("cannot cluster a partitioned table");
        errors.add("access method does not support clustering");
        StringBuilder sb = new StringBuilder("CLUSTER ");
        if (Randomly.getBoolean()) {
            MaterializeTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
            sb.append(table.getName());
            if (Randomly.getBoolean() && !table.getIndexes().isEmpty()) {
                sb.append(" USING ");
                sb.append(table.getRandomIndex().getIndexName());
                errors.add("cannot cluster on partial index");
            }
        }
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
