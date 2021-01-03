package sqlancer.mongodb;

import java.util.Collections;
import java.util.List;

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

    public enum MongoDBDataType implements HasBsonType {
        INTEGER {
            @Override
            public BsonType getBsonType() {
                return BsonType.INT32;
            }
        },
        STRING {
            @Override
            public BsonType getBsonType() {
                return BsonType.STRING;
            }
        },
        BOOLEAN {
            @Override
            public BsonType getBsonType() {
                return BsonType.BOOLEAN;
            }
        },
        DOUBLE {
            @Override
            public BsonType getBsonType() {
                return BsonType.DOUBLE;
            }
        },
        DATE_TIME {
            @Override
            public BsonType getBsonType() {
                return BsonType.DATE_TIME;
            }
        },
        TIMESTAMP {
            @Override
            public BsonType getBsonType() {
                return BsonType.TIMESTAMP;
            }
        };

        public static MongoDBDataType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public interface HasBsonType {
        BsonType getBsonType();
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
