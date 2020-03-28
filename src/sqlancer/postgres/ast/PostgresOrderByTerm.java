package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresOrderByTerm implements PostgresExpression {

	private final PostgresOrder order;
	private final PostgresExpression expr;
	private ForClause forClause;

	public enum ForClause {
		UPDATE("UPDATE"), NO_KEY_UPDATE("NO KEY UPDATE"), SHARE("SHARE"), KEY_SHARE("KEY SHARE");

		private final String textRepresentation;

		private ForClause(String textRepresentation) {
			this.textRepresentation = textRepresentation;
		}

		public String getTextRepresentation() {
			return textRepresentation;
		}

		public static ForClause getRandom() {
			return Randomly.fromOptions(values());
		}
	}

	public enum PostgresOrder {
		ASC, DESC;

		public static PostgresOrder getRandomOrder() {
			return Randomly.fromOptions(PostgresOrder.values());
		}
	}

	public PostgresOrderByTerm(PostgresExpression expr, PostgresOrder order, ForClause forClause) {
		this.expr = expr;
		this.order = order;
		this.forClause = forClause;
	}

	public PostgresOrder getOrder() {
		return order;
	}

	public PostgresExpression getExpr() {
		return expr;
	}

	@Override
	public PostgresConstant getExpectedValue() {
		throw new AssertionError(this);
	}

	@Override
	public PostgresDataType getExpressionType() {
		return null;
	}

	public String getForClause() {
		if (forClause == null) {
			return null;
		}
		return forClause.getTextRepresentation();
	}

}
