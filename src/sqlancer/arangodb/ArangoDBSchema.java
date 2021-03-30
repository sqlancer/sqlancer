package sqlancer.arangodb;

import java.util.Collections;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;

public class ArangoDBSchema extends AbstractSchema<ArangoDBProvider.ArangoDBGlobalState, ArangoDBSchema.ArangoDBTable> {

    public enum ArangoDBDataType {
        INTEGER, DOUBLE, STRING, BOOLEAN;

        public static ArangoDBDataType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public static class ArangoDBColumn extends AbstractTableColumn<ArangoDBTable, ArangoDBDataType> {

        private final boolean isId;
        private final boolean isNullable;

        public ArangoDBColumn(String name, ArangoDBDataType type, boolean isId, boolean isNullable) {
            super(name, null, type);
            this.isId = isId;
            this.isNullable = isNullable;
        }

        public boolean isId() {
            return isId;
        }

        public boolean isNullable() {
            return isNullable;
        }
    }

    public ArangoDBSchema(List<ArangoDBTable> databaseTables) {
        super(databaseTables);
    }

    public static class ArangoDBTables extends AbstractTables<ArangoDBTable, ArangoDBColumn> {

        public ArangoDBTables(List<ArangoDBTable> tables) {
            super(tables);
        }
    }

    public static class ArangoDBTable
            extends AbstractTable<ArangoDBColumn, TableIndex, ArangoDBProvider.ArangoDBGlobalState> {

        public ArangoDBTable(String name, List<ArangoDBColumn> columns, boolean isView) {
            super(name, columns, Collections.emptyList(), isView);
        }

        @Override
        public long getNrRows(ArangoDBProvider.ArangoDBGlobalState globalState) {
            throw new UnsupportedOperationException();
        }
    }

    public ArangoDBTables getRandomTableNonEmptyTables() {
        return new ArangoDBTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }
}
