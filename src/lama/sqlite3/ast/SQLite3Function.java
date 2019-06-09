package lama.sqlite3.ast;

import lama.Randomly;
import lama.sqlite3.gen.SQLite3Cast;
import lama.sqlite3.gen.SQLite3ExpressionGenerator;
import lama.sqlite3.schema.SQLite3DataType;
import lama.sqlite3.schema.SQLite3Schema.Column.CollateSequence;

public class SQLite3Function extends SQLite3Expression {

	private final ComputableFunction func;
	private final SQLite3Expression[] args;

	public SQLite3Function(ComputableFunction func, SQLite3Expression[] args) {
		assert args.length == func.getNrArgs();
		this.func = func;
		this.args = args;
	}

	public enum ComputableFunction {

		ABS(1) {
			@Override
			public SQLite3Constant apply(SQLite3Constant... args) {
				SQLite3Constant castValue = SQLite3Cast.castToInt(args[0]);
				if (castValue.isNull()) {
					return castValue;
				} else if (castValue.getDataType() == SQLite3DataType.INT) {
					long absVal = Math.abs(castValue.asInt());
					return SQLite3Constant.createIntConstant(absVal);
				} else {
					assert castValue.getDataType() == SQLite3DataType.REAL;
					double absVal = Math.abs(castValue.asDouble());
					return SQLite3Constant.createRealConstant(absVal);
				}
			}
		},

		LOWER(1) {
			@Override
			public SQLite3Constant apply(SQLite3Constant... args) {
				if (args[0].getDataType() == SQLite3DataType.TEXT) {
					String text = args[0].asString().toLowerCase();
					return SQLite3Constant.createTextConstant(text);
				} else {
					return args[0];
				}
			}
		},
		LIKELY(1) {
			@Override
			public SQLite3Constant apply(SQLite3Constant... args) {
				return args[0];
			}
		},
		IFNULL(2) {
			@Override
			public SQLite3Constant apply(SQLite3Constant... args) {
				for (SQLite3Expression arg : args) {
					if (!arg.getExpectedValue().isNull()) {
						return arg.getExpectedValue();
					}
				}
				return SQLite3Constant.createNullConstant();
			}
		},

		UPPER(1) {

			@Override
			public SQLite3Constant apply(SQLite3Constant... args) {
				if (args[0].getDataType() == SQLite3DataType.TEXT) {
					String text = args[0].asString().toUpperCase();
					return SQLite3Constant.createTextConstant(text);
				} else {
					return args[0];
				}
			}

		},
		TYPEOF(1) {

			@Override
			public SQLite3Constant apply(SQLite3Constant... args) {
				switch (args[0].getDataType()) {
				case BINARY:
					return SQLite3Constant.createTextConstant("blob");
				case INT:
					return SQLite3Constant.createTextConstant("integer");
				case NULL:
					return SQLite3Constant.createTextConstant("null");
				case REAL:
					return SQLite3Constant.createTextConstant("real");
				case TEXT:
					return SQLite3Constant.createTextConstant("text");
				default:
					throw new AssertionError(args[0]);
				}
			}

		},
		UNLIKELY(1) {
			@Override
			public SQLite3Constant apply(SQLite3Constant... args) {
				return args[0];
			}
		};

		private ComputableFunction(int nrArgs) {
			this.nrArgs = nrArgs;
		}

		final int nrArgs;

		public int getNrArgs() {
			return nrArgs;
		}

		public abstract SQLite3Constant apply(SQLite3Constant... args);

		public static ComputableFunction getRandomFunction() {
			return Randomly.fromOptions(ComputableFunction.values());
		}

	}

	@Override
	public CollateSequence getExplicitCollateSequence() {
		return null;
	}

	public SQLite3Expression[] getArgs() {
		return args;
	}

	public ComputableFunction getFunc() {
		return func;
	}

}
