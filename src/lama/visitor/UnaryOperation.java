package lama.visitor;

public interface UnaryOperation<T>  {
	
	public static enum OperatorKind {
		PREFIX, POSTFIX
	}
	
	public T getExpression();

	public String getOperatorRepresentation();
	
	public default boolean omitBracketsWhenPrinting() {
		return false;
	}

	public OperatorKind getOperatorKind();
	
}
