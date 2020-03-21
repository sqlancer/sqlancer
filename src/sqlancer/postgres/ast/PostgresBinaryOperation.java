package sqlancer.postgres.ast;

public abstract class PostgresBinaryOperation extends PostgresExpression {

	public abstract PostgresExpression getLeft();
	
	public abstract PostgresExpression getRight();
	
	public abstract String getOperatorTextRepresentation();
	
}
