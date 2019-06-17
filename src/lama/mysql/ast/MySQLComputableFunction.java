package lama.mysql.ast;

import lama.Randomly;

public class MySQLComputableFunction extends MySQLExpression {

	private final MySQLFunction func;
	private final MySQLExpression[] args;

	public MySQLComputableFunction(MySQLFunction func, MySQLExpression... args) {
		assert args.length == func.getNrArgs();
		this.func = func;
		this.args = args;
	}

	public MySQLFunction getFunction() {
		return func;
	}

	public MySQLExpression[] getArguments() {
		return args;
	}

	public enum MySQLFunction {
		COALESCE(2, "COALESCE") {

			@Override
			public MySQLConstant apply(MySQLConstant... args) {
				for (MySQLConstant arg : args) {
					if (!arg.isNull()) {
						return arg;
					}
				}
				return MySQLConstant.createNullConstant();
			}

			@Override
			public boolean isVariadic() {
				return true;
			}

		},
		/**
		 * @see https://dev.mysql.com/doc/refman/8.0/en/control-flow-functions.html#function_if
		 */
		IF(3, "IF") {

			@Override
			public MySQLConstant apply(MySQLConstant... args) {
				MySQLConstant cond = args[0];
				MySQLConstant left = args[1];
				MySQLConstant right = args[2];
				if (cond.isNull() || !cond.asBooleanNotNull()) {
					return right;
				} else {
					return left;
				}
			}

		},
		/**
		 * @see https://dev.mysql.com/doc/refman/8.0/en/control-flow-functions.html#function_ifnull
		 */
		IFNULL(2, "IFNULL") {

			@Override
			public MySQLConstant apply(MySQLConstant... args) {
				if (args[0].isNull()) {
					return args[1];
				} else {
					return args[0];
				}
			}

		};

		private String functionName;
		final int nrArgs;

		private MySQLFunction(int nrArgs, String functionName) {
			this.nrArgs = nrArgs;
			this.functionName = functionName;
		}

		/**
		 * Gets the number of arguments if the function is non-variadic. If the function
		 * is variadic, the minimum number of arguments is returned.
		 */
		public int getNrArgs() {
			return nrArgs;
		}

		public abstract MySQLConstant apply(MySQLConstant... args);

		public static MySQLFunction getRandomFunction() {
			return Randomly.fromOptions(values());
		}

		@Override
		public String toString() {
			return functionName;
		}

		public boolean isVariadic() {
			return false;
		}

		public String getName() {
			return functionName;
		}
	}

	@Override
	public MySQLConstant getExpectedValue() {
		MySQLConstant[] constants = new MySQLConstant[args.length];
		for (int i = 0; i < constants.length; i++) {
			constants[i] = args[i].getExpectedValue();
		}
		return func.apply(constants);
	}

}
