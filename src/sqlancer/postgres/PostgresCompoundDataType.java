package sqlancer.postgres;

import java.util.Optional;

import sqlancer.common.schema.AbstractCompoundDataType;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public final class PostgresCompoundDataType implements AbstractCompoundDataType<PostgresDataType> {

    private final PostgresDataType dataType;
    private final PostgresCompoundDataType elemType;
    private final Integer size;

    private PostgresCompoundDataType(PostgresDataType dataType, PostgresCompoundDataType elemType, Integer size) {
        this.dataType = dataType;
        this.elemType = elemType;
        this.size = size;
    }

    @Override
    public PostgresDataType getDataType() {
        return dataType;
    }

    public PostgresCompoundDataType getElemType() {
        if (elemType == null) {
            throw new AssertionError();
        }
        return elemType;
    }

    @Override
    public Optional<Integer> getSize() {
        if (size == null) {
            return Optional.empty();
        } else {
            return Optional.of(size);
        }
    }

    @Override
    public AbstractCompoundDataType<PostgresDataType> getElementType() {
        if (elemType == null) {
            throw new AssertionError();
        }
        return elemType;
    }

    public static PostgresCompoundDataType create(PostgresDataType type, int size) {
        return new PostgresCompoundDataType(type, null, size);
    }

    public static PostgresCompoundDataType create(PostgresDataType type) {
        return new PostgresCompoundDataType(type, null, null);
    }
}
