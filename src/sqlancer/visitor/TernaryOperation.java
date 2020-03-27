package sqlancer.visitor;

public interface TernaryOperation<T> {

	public abstract T getLeft();
	
	public abstract T getMiddle();

	public abstract T getRight();

}
