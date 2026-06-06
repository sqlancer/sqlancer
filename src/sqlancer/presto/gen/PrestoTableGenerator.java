package sqlancer.presto.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractTableGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema.PrestoColumn;
import sqlancer.presto.PrestoSchema.PrestoCompositeDataType;

public class PrestoTableGenerator extends AbstractTableGenerator<PrestoColumn> {

    private PrestoGlobalState globalState;

    public PrestoTableGenerator() {
        this.canAffectSchema = true;
        this.canonicalizeString = false;
    }

    public SQLQueryAdapter getQuery(PrestoGlobalState globalState) {
        this.globalState = globalState;
        return getStatement();
    }

    @Override
    public void buildStatement() {
        String catalog = globalState.getDbmsSpecificOptions().catalog;
        String schema = globalState.getDatabaseName();
        String tableName = globalState.getSchema().getFreeTableName();
        String qualifiedName = catalog + "." + schema + "." + tableName;
        appendCreateTable(qualifiedName);
        appendColumnDefinitions(getNewColumns());
    }

    private static List<PrestoColumn> getNewColumns() {
        List<PrestoColumn> columns = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String columnName = String.format("c%d", i);
            PrestoCompositeDataType columnType = PrestoCompositeDataType.getRandomWithoutNull();
            columns.add(new PrestoColumn(columnName, columnType, false, false));
        }
        return columns;
    }

}
