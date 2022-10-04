package sqlancer.yugabyte.ysql;

import java.util.Optional;

import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public final class YSQLCompoundDataType {

    private final YSQLDataType dataType;
    private final YSQLCompoundDataType elemType;
    private final Integer size;

    private YSQLCompoundDataType(YSQLDataType dataType, YSQLCompoundDataType elemType, Integer size) {
        this.dataType = dataType;
        this.elemType = elemType;
        this.size = size;
    }

    public static YSQLCompoundDataType create(YSQLDataType type, int size) {
        return new YSQLCompoundDataType(type, null, size);
    }

    public static YSQLCompoundDataType create(YSQLDataType type) {
        return new YSQLCompoundDataType(type, null, null);
    }

    public YSQLDataType getDataType() {
        return dataType;
    }

    public YSQLCompoundDataType getElemType() {
        if (elemType == null) {
            throw new AssertionError();
        }
        return elemType;
    }

    public Optional<Integer> getSize() {
        if (size == null) {
            return Optional.empty();
        } else {
            return Optional.of(size);
        }
    }
}
