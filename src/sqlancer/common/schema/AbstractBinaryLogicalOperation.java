package sqlancer.common.schema;

public interface AbstractBinaryLogicalOperation<T> {
    T getLeft();

    String getTextRepresentation();

    T getRight();
}
