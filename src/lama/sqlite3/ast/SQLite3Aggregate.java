package lama.sqlite3.ast;

import lama.IgnoreMeException;
import lama.Randomly;
import lama.sqlite3.gen.SQLite3Cast;
import lama.sqlite3.schema.SQLite3DataType;
import lama.sqlite3.schema.SQLite3Schema.Column.CollateSequence;

public class SQLite3Aggregate extends SQLite3Expression {

	private SQLite3AggregateFunction func;
	private SQLite3Expression expr;

	public enum SQLite3AggregateFunction {
		MIN, MAX, AVG () {
			@Override
			public SQLite3Constant apply(SQLite3Constant exprVal) {
				return SQLite3Cast.castToReal(exprVal);
			}
			
		}, SUM () {
			@Override
			public SQLite3Constant apply(SQLite3Constant exprVal) {
				return SQLite3Cast.castToReal(exprVal);
			}
			
		},
//		GROUP_CONCAT() {
//			@Override
//			public SQLite3Constant apply(SQLite3Constant exprVal) {
//				SQLite3Constant castToText = SQLite3Cast.castToText(exprVal);
//				if (castToText == null) {
//					throw new IgnoreMeException();
//				}
//				return castToText;
//			}
//		}
//		, COUNT() {
//			@Override
//			public SQLite3Constant apply(SQLite3Constant exprVal) {
//				return SQLite3Constant.createIntConstant(1);
//			}
//		}
		; // MIN , SUM;

		public SQLite3Constant apply(SQLite3Constant exprVal) {
			return exprVal;
		}
		
		public static SQLite3AggregateFunction getRandom() {
			return Randomly.fromOptions(values());
		}
		
		public static SQLite3AggregateFunction getRandom(SQLite3DataType type) {
			return Randomly.fromOptions(values());
		}
		
	}
	
	public SQLite3Aggregate(SQLite3Expression expr, SQLite3AggregateFunction func) {
		this.expr = expr;
		this.func = func;
	}
	
	public SQLite3AggregateFunction getFunc() {
		return func;
	}
	
	public SQLite3Expression getExpr() {
		return expr;
	}

	@Override
	public CollateSequence getExplicitCollateSequence() {
		return expr.getExplicitCollateSequence();
	}
	
	@Override
	public SQLite3Constant getExpectedValue() {
		return func.apply(expr.getExpectedValue());
	}
	
}
