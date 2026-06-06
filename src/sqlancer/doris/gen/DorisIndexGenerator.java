package sqlancer.doris.gen;

import java.sql.SQLException;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.gen.AbstractIndexGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisTable;

public class DorisIndexGenerator extends AbstractIndexGenerator<DorisColumn> {

    private final DorisGlobalState globalState;

    public DorisIndexGenerator(DorisGlobalState globalState) {
        this.globalState = globalState;
        this.canAffectSchema = true;
    }

    public static SQLQueryAdapter getQuery(DorisGlobalState globalState) throws SQLException {
        if (globalState.getSchema().getIndexCount() > globalState.getDbmsSpecificOptions().maxNumIndexes) {
            throw new IgnoreMeException();
        }
        return new DorisIndexGenerator(globalState).getStatement();
    }

    @Override
    public void buildStatement() {
        DorisTable randomTable = globalState.getSchema().getRandomTable(t -> !t.isView());
        appendCreateIndex(false);
        if (Randomly.getBoolean()) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(globalState.getSchema().getFreeIndexName());
        sb.append(" ON ");
        sb.append(randomTable.getName());
        // Doris only supports CREATE INDEX on a single column; index type is BITMAP
        List<DorisColumn> subset = Randomly.extractNrRandomColumns(randomTable.getColumns(), 1);
        appendIndexColumnList(subset, false);
        sb.append(" ");
        if (Randomly.getBoolean()) {
            sb.append("USING BITMAP ");
        }
    }

}
