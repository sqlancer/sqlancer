package sqlancer.doris.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.doris.DorisProvider.DorisGlobalState;

public final class DorisDropTableGenerator {

    private DorisDropTableGenerator() {
    }

    public static SQLQueryAdapter dropTable(DorisGlobalState globalState) {
        if (globalState.getSchema().getTables(t -> !t.isView()).size() <= 1) {
            throw new IgnoreMeException();
        }
        StringBuilder sb = new StringBuilder("DROP TABLE ");
        if (Randomly.getBoolean()) {
            sb.append("IF EXISTS ");
        }
        sb.append(globalState.getSchema().getRandomTableOrBailout(t -> !t.isView()).getName());
        if (Randomly.getBoolean()) {
            sb.append(" FORCE ");
        }
        return new SQLQueryAdapter(sb.toString(), null, true);
    }

}
