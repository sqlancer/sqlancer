package sqlancer.cockroachdb.ast;

import sqlancer.Randomly;
import sqlancer.visitor.BinaryOperation;

public class CockroachDBRegexOperation extends CockroachDBExpression implements BinaryOperation<CockroachDBExpression> {

	public enum CockroachDBRegexOperator {
		LIKE("LIKE"), //
		NOT_LIKE("NOT LIKE"), //
		ILIKE("ILIKE"),
		NOT_ILIKE("NOT ILIKE"),
		SIMILAR("SIMILAR TO"), //
		NOT_SIMILAR("NOT SIMILAR TO"),
		MATCH_CASE_SENSITIVE("~"), //
		NOT_MATCH_CASE_SENSITIVE("!~"), //
		MATCH_CASE_INSENSITIVE("~*"), //
		NOT_MATCH_CASE_INSENSITIVE("!~*");

		private String textRepr;

		private CockroachDBRegexOperator(String textRepr) {
			this.textRepr = textRepr;
		}

		public static CockroachDBRegexOperator getRandom() {
			return Randomly.fromOptions(values());
		}

	}

	private final CockroachDBExpression left;
	private final CockroachDBExpression right;
	private final CockroachDBRegexOperator op;

	public CockroachDBRegexOperation(CockroachDBExpression left, CockroachDBExpression right,
			CockroachDBRegexOperator op) {
		this.left = left;
		this.right = right;
		this.op = op;
	}

	@Override
	public CockroachDBExpression getLeft() {
		return left;
	}

	@Override
	public CockroachDBExpression getRight() {
		return right;
	}

	@Override
	public String getOperatorRepresentation() {
		return op.textRepr;
	}

}
