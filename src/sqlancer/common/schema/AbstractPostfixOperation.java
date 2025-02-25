package sqlancer.common.schema;

public interface AbstractPostfixOperation<T> {
    T getExpression();

    Enum<?> getOperator();

    boolean isNegated();

}
