package sqlancer.common.visitor;

public interface UnaryOperation<T> {

    enum OperatorKind {
        PREFIX, POSTFIX
    }

    T getExpression();

    String getOperatorRepresentation();

    default boolean omitBracketsWhenPrinting() {
        return false;
    }

    OperatorKind getOperatorKind();

}
