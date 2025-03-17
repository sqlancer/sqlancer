package sqlancer.materialize;

import java.util.Optional;

import sqlancer.common.schema.AbstractCompoundDataType;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;

public final class MaterializeCompoundDataType implements AbstractCompoundDataType<MaterializeDataType> {

    private final MaterializeDataType dataType;
    private final MaterializeCompoundDataType elemType;
    private final Integer size;

    private MaterializeCompoundDataType(MaterializeDataType dataType, MaterializeCompoundDataType elemType,
            Integer size) {
        this.dataType = dataType;
        this.elemType = elemType;
        this.size = size;
    }

    @Override
    public MaterializeDataType getDataType() {
        return dataType;
    }

    public MaterializeCompoundDataType getElemType() {
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
    public AbstractCompoundDataType<MaterializeDataType> getElementType() {
        return elemType;
    }

    public static MaterializeCompoundDataType create(MaterializeDataType type, int size) {
        return new MaterializeCompoundDataType(type, null, size);
    }

    public static MaterializeCompoundDataType create(MaterializeDataType type) {
        return new MaterializeCompoundDataType(type, null, null);
    }
}
