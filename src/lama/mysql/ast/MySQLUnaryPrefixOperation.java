package lama.mysql.ast;

import lama.IgnoreMeException;
import lama.Randomly;

public class MySQLUnaryPrefixOperation extends MySQLExpression {

	private final MySQLExpression expr;
	private MySQLUnaryPrefixOperator op;
	private final String operatorTextRepresentation;

	public enum MySQLUnaryPrefixOperator {
		NOT("!", "NOT") {
			@Override
			public MySQLConstant applyNotNull(MySQLConstant expr) {
				return MySQLConstant.createIntConstant(expr.asBooleanNotNull() ? 0 : 1);
			}
		},
		PLUS("+") {
			@Override
			public MySQLConstant applyNotNull(MySQLConstant expr) {
				return expr;
			}
		},
		MINUS("-") {
			@Override
			public MySQLConstant applyNotNull(MySQLConstant expr) {
				if (expr.isString()) {
					// TODO: implement floating points
					throw new IgnoreMeException();
				} else if (expr.isInt()) {
					if (!expr.isSigned()) {
						// TODO 
						throw new IgnoreMeException();
					}
					return MySQLConstant.createIntConstant(-expr.getInt());
				} else {
					throw new AssertionError(expr);
				}
			}
		};

		private String[] textRepresentations;

		private MySQLUnaryPrefixOperator(String... textRepresentations) {
			this.textRepresentations = textRepresentations;
		}

		public abstract MySQLConstant applyNotNull(MySQLConstant expr);

		public static MySQLUnaryPrefixOperator getRandom() {
			return Randomly.fromOptions(values());
		}
	}

	public MySQLUnaryPrefixOperation(MySQLExpression expr, MySQLUnaryPrefixOperator op) {
		this.expr = expr;
		this.op = op;
		this.operatorTextRepresentation = Randomly.fromOptions(op.textRepresentations);
	}

	public MySQLExpression getExpression() {
		return expr;
	}

	public String getOperatorTextRepresentation() {
		return operatorTextRepresentation;
	}

	@Override
	public MySQLConstant getExpectedValue() {
		MySQLConstant subExprVal = expr.getExpectedValue();
		if (subExprVal.isNull()) {
			return MySQLConstant.createNullConstant();
		} else {
			return op.applyNotNull(subExprVal);
		}
	}

}
