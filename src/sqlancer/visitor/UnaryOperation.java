package sqlancer.visitor;

public interface UnaryOperation<T> {

	static enum OperatorKind {
		PREFIX, POSTFIX
	}

	T getExpression();

	String getOperatorRepresentation();

	default boolean omitBracketsWhenPrinting() {
		return false;
	}

	OperatorKind getOperatorKind();

}
