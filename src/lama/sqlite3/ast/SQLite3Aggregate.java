package lama.sqlite3.ast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import lama.IgnoreMeException;
import lama.Randomly;
import lama.sqlite3.SQLite3Provider;
import lama.sqlite3.schema.SQLite3DataType;
import lama.sqlite3.schema.SQLite3Schema.Column.CollateSequence;

/**
 * @see https://www.sqlite.org/lang_aggfunc.html
 */
public class SQLite3Aggregate extends SQLite3Expression {

	private SQLite3AggregateFunction func;
	private List<SQLite3Expression> expr;

	public enum SQLite3AggregateFunction {
		AVG() {
			@Override
			public SQLite3Constant apply(SQLite3Constant exprVal) {
				return SQLite3Cast.castToReal(exprVal);
			}

		},
		COUNT() {
			@Override
			public SQLite3Constant apply(SQLite3Constant exprVal) {
				int count;
				if (exprVal.isNull()) {
					count = 0;
				} else {
					count = 1;
				}
				return SQLite3Constant.createIntConstant(count);
			}
		},
		COUNT_ALL() {
			@Override
			public SQLite3Constant apply(SQLite3Constant exprVal) {
				return SQLite3Constant.createIntConstant(1);
			}
		},
		GROUP_CONCAT() {
			@Override
			public SQLite3Constant apply(SQLite3Constant exprVal) {
				SQLite3Constant castToText = SQLite3Cast.castToText(exprVal);
				if (castToText == null && SQLite3Provider.MUST_KNOW_RESULT) {
					throw new IgnoreMeException();
				}
				return castToText;
			}
		},
		MAX {
			@Override
			public SQLite3Constant apply(SQLite3Constant exprVal) {
				return exprVal;
			}
		},
		MIN {
			@Override
			public SQLite3Constant apply(SQLite3Constant exprVal) {
				return exprVal;
			}
		},
		SUM() {
			@Override
			public SQLite3Constant apply(SQLite3Constant exprVal) {
				return SQLite3Cast.castToReal(exprVal);
			}

		},
		// zipfile needs to be loaded as an extension
//		ZIPFILE() {
//
//			@Override
//			public SQLite3Constant apply(SQLite3Constant exprVal) {
//				if (SQLite3Provider.MUST_KNOW_RESULT) {
//					throw new AssertionError();
//				}
//				return null;
//			}
//			
//		},
		TOTAL() {
			@Override
			public SQLite3Constant apply(SQLite3Constant exprVal) {
				if (exprVal.isNull()) {
					return SQLite3Constant.createRealConstant(0);
				} else {
					return SQLite3Cast.castToReal(exprVal);
				}
			}

		};

		public abstract SQLite3Constant apply(SQLite3Constant exprVal);

		public static SQLite3AggregateFunction getRandom() {
			List<SQLite3AggregateFunction> functions = new ArrayList<>(Arrays.asList(values()));
			if (SQLite3Provider.MUST_KNOW_RESULT) {
				functions.remove(SQLite3AggregateFunction.SUM);
				functions.remove(SQLite3AggregateFunction.TOTAL);
				functions.remove(SQLite3AggregateFunction.GROUP_CONCAT);
			}
			return Randomly.fromOptions(values());
		}

		public static SQLite3AggregateFunction getRandom(SQLite3DataType type) {
			return Randomly.fromOptions(values());
		}

	}

	public SQLite3Aggregate(List<SQLite3Expression> expr, SQLite3AggregateFunction func) {
		this.expr = expr;
		this.func = func;
	}

	public SQLite3AggregateFunction getFunc() {
		return func;
	}

	public List<SQLite3Expression> getExpr() {
		return expr;
	}

	@Override
	public CollateSequence getExplicitCollateSequence() {
		return null;
//		return expr.getExplicitCollateSequence();
	}

	@Override
	public SQLite3Constant getExpectedValue() {
		assert !SQLite3Provider.MUST_KNOW_RESULT;
		return null;
//		return func.apply(expr.getExpectedValue());
	}

}
