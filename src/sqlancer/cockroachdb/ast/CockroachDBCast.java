package sqlancer.cockroachdb.ast;

import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBCompositeDataType;
import sqlancer.visitor.UnaryOperation;

public class CockroachDBCast extends CockroachDBExpression implements UnaryOperation<CockroachDBExpression> {
	
	private final CockroachDBExpression expr;
	private final CockroachDBCompositeDataType type;

	public CockroachDBCast(CockroachDBExpression expr, CockroachDBCompositeDataType type) {
		this.expr = expr;
		this.type = type;
	}

	@Override
	public CockroachDBExpression getExpression() {
		return expr;
	}

	@Override
	public String getOperatorRepresentation() {
		return "::" + type.toString();
	}

	@Override
	public OperatorKind getOperatorKind() {
		return OperatorKind.POSTFIX;
	}

}
