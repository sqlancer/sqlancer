package sqlancer.common.schema;

public interface AbstractCastOperation<E, D> {
    E getExpression();

    AbstractCompoundDataType<D> getCompoundType();
}
