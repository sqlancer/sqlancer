package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresBinaryRangeOperation extends PostgresBinaryOperation {

	private String op;
	private PostgresExpression left;
	private PostgresExpression right;
	
	public enum PostgresBinaryRangeOperator {
		UNION("*"), INTERSECTION("*"), DIFFERENCE("-");
		
		
		private final String textRepresentation;

		private PostgresBinaryRangeOperator(String textRepresentation) {
			this.textRepresentation = textRepresentation;
		}

		public String getTextRepresentation() {
			return textRepresentation;
		}
		
		public static PostgresBinaryRangeOperator getRandom() {
			return Randomly.fromOptions(values());
		}
		
	}

	public enum PostgresBinaryRangeComparisonOperator {
		CONTAINS_RANGE_OR_ELEMENT("@>"), RANGE_OR_ELEMENT_IS_CONTAINED("<@"), OVERLAP("&&"), STRICT_LEFT_OF("<<"),
		STRICT_RIGHT_OF(">>"), NOT_RIGHT_OF("&<"), NOT_LEFT_OF(">&"), ADJACENT("-|-");

		private final String textRepresentation;

		private PostgresBinaryRangeComparisonOperator(String textRepresentation) {
			this.textRepresentation = textRepresentation;
		}

		public String getTextRepresentation() {
			return textRepresentation;
		}

		public static PostgresBinaryRangeComparisonOperator getRandom() {
			return Randomly.fromOptions(values());
		}
	}
	
	public PostgresBinaryRangeOperation(PostgresBinaryRangeComparisonOperator op, PostgresExpression left, PostgresExpression right) {
		this.op = op.getTextRepresentation();
		this.left = left;
		this.right = right;
	}
	
	public PostgresBinaryRangeOperation(PostgresBinaryRangeOperator op, PostgresExpression left, PostgresExpression right) {
		this.op = op.getTextRepresentation();
		this.left = left;
		this.right = right;
	}

	@Override
	public PostgresDataType getExpressionType() {
		return PostgresDataType.BOOLEAN;
	}

	@Override
	public PostgresConstant getExpectedValue() {
		return null;
	}
	
	public PostgresExpression getLeft() {
		return left;
	}
	
	public String getOpAsText() {
		return op;
	}
	
	public PostgresExpression getRight() {
		return right;
	}

	@Override
	public String getOperatorTextRepresentation() {
		return op;
	}

}
