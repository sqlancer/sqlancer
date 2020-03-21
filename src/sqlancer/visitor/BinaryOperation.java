package sqlancer.visitor;

public interface BinaryOperation<T> {
	
	public abstract T getLeft();

	public abstract T getRight();

	public abstract String getOperatorRepresentation();
}
