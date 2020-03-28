package sqlancer.postgres.ast;

public abstract class PostgresBinaryOperation implements PostgresExpression {

	public abstract PostgresExpression getLeft();
	
	public abstract PostgresExpression getRight();
	
	public abstract String getOperatorTextRepresentation();
	
}
