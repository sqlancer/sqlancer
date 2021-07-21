package sqlancer.mongodb;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.BsonType;

import com.mongodb.client.MongoDatabase;

import sqlancer.Randomly;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.mongodb.MongoDBProvider.MongoDBGlobalState;

public class MongoDBSchema extends AbstractSchema<MongoDBGlobalState, MongoDBSchema.MongoDBTable> {

    public enum MongoDBDataType {
        INTEGER(BsonType.INT32), STRING(BsonType.STRING), BOOLEAN(BsonType.BOOLEAN), DOUBLE(BsonType.DOUBLE),
        DATE_TIME(BsonType.DATE_TIME), TIMESTAMP(BsonType.TIMESTAMP);

        private final BsonType bsonType;

        MongoDBDataType(BsonType type) {
            this.bsonType = type;
        }

        public BsonType getBsonType() {
            return bsonType;
        }

        public static MongoDBDataType getRandom(MongoDBGlobalState state) {
            Set<MongoDBDataType> valueSet = new HashSet<>(Arrays.asList(values()));
            if (state.getDbmsSpecificOptions().nullSafety) {
                valueSet.remove(STRING);
            }
            MongoDBDataType[] configuredValues = new MongoDBDataType[valueSet.size()];
            return Randomly.fromOptions(valueSet.toArray(configuredValues));
        }
    }

    public static class MongoDBColumn extends AbstractTableColumn<MongoDBTable, MongoDBDataType> {

        private final boolean isId;
        private final boolean isNullable;

        public MongoDBColumn(String name, MongoDBDataType type, boolean isId, boolean isNullable) {
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

    public static class MongoDBTables extends AbstractTables<MongoDBTable, MongoDBColumn> {

        public MongoDBTables(List<MongoDBTable> tables) {
            super(tables);
        }
    }

    public MongoDBSchema(List<MongoDBTable> databaseTables) {
        super(databaseTables);
    }

    public static class MongoDBTable extends AbstractTable<MongoDBColumn, TableIndex, MongoDBGlobalState> {
        public MongoDBTable(String name, List<MongoDBColumn> columns, boolean isView) {
            super(name, columns, Collections.emptyList(), isView);
        }

        @Override
        public long getNrRows(MongoDBGlobalState globalState) {
            throw new UnsupportedOperationException();
        }
    }

    public static MongoDBSchema fromConnection(MongoDatabase connection, String databaseName) {
        throw new UnsupportedOperationException();
    }

    public MongoDBTables getRandomTableNonEmptyTables() {
        return new MongoDBTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }
}
