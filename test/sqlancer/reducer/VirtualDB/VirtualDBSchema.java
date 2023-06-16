package sqlancer.reducer.VirtualDB;

import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.TableIndex;
import sqlancer.reducer.VirtualDB.VirtualDBSchema.VirtualDBTable;

import java.util.List;

public class VirtualDBSchema extends AbstractSchema<VirtualDBGlobalState, VirtualDBTable> {

    public VirtualDBSchema(List<VirtualDBTable> databaseTables) {
        super(databaseTables);
    }

    public static class VirtualDBTable extends AbstractTable<VirtualDBColumn, VirtualDBIndex, VirtualDBGlobalState> {
        protected VirtualDBTable(String name, List<VirtualDBColumn> columns, List<VirtualDBIndex> indexes,
                boolean isView) {
            super(name, columns, indexes, isView);
        }

        @Override
        public long getNrRows(VirtualDBGlobalState globalState) {
            return 0;
        }
    }

    public static class VirtualDBIndex extends TableIndex {

        protected VirtualDBIndex(String indexName) {
            super(indexName);
        }
    }

    public static class VirtualDBDataType {

    }

    public static class VirtualDBColumn extends AbstractTableColumn<VirtualDBTable, VirtualDBDataType> {

        public VirtualDBColumn(String name, VirtualDBTable table, VirtualDBDataType type) {
            super(name, table, type);
        }
    }
}
