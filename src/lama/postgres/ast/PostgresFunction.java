package lama.postgres.ast;

import lama.Randomly;
import lama.postgres.PostgresSchema.PostgresDataType;

public class PostgresFunction extends PostgresExpression {

	private final String func;
	private final PostgresExpression[] args;
	private final PostgresDataType returnType;
	private PostgresFunctionWithResult functionWithKnownResult;

	public PostgresFunction(PostgresFunctionWithResult func, PostgresDataType returnType, PostgresExpression... args) {
		functionWithKnownResult = func;
		this.func = func.getName();
		this.returnType = returnType;
		this.args = args;
	}
	
	public PostgresFunction(PostgresFunctionWithUnknownResult f, PostgresDataType returnType, PostgresExpression...args) {
		this.func = f.getName();
		this.returnType = returnType;
		this.args = args;
	}

	public String getFunctionName() {
		return func;
	}

	public PostgresExpression[] getArguments() {
		return args;
	}

	public enum PostgresFunctionWithResult {
		ABS(1, "abs") {

			@Override
			public PostgresConstant apply(PostgresConstant[] evaluatedArgs, PostgresExpression[] args) {
				if (evaluatedArgs[0].isNull()) {
					return PostgresConstant.createNullConstant();
				} else {
					return PostgresConstant
							.createIntConstant(Math.abs(evaluatedArgs[0].cast(PostgresDataType.INT).asInt()));
				}
			}

			@Override
			public boolean supportsReturnType(PostgresDataType type) {
				return type == PostgresDataType.INT;
			}

			@Override
			public PostgresDataType[] getInputTypesForReturnType(PostgresDataType returnType, int nrArguments) {
				return new PostgresDataType[] { returnType };
			}

		},
		LOWER(1, "lower") {

			@Override
			public PostgresConstant apply(PostgresConstant[] evaluatedArgs, PostgresExpression[] args) {
				if (evaluatedArgs[0].isNull()) {
					return PostgresConstant.createNullConstant();
				} else {
					String text = evaluatedArgs[0].asString();
					return PostgresConstant.createTextConstant(text.toLowerCase());
				}
			}

			@Override
			public boolean supportsReturnType(PostgresDataType type) {
				return type == PostgresDataType.TEXT;
			}

			@Override
			public PostgresDataType[] getInputTypesForReturnType(PostgresDataType returnType, int nrArguments) {
				return new PostgresDataType[] { PostgresDataType.TEXT };
			}

		},
		LENGTH(1, "length") {
			@Override
			public PostgresConstant apply(PostgresConstant[] evaluatedArgs, PostgresExpression[] args) {
				if (evaluatedArgs[0].isNull()) {
					return PostgresConstant.createNullConstant();
				}
				String text = evaluatedArgs[0].asString();
				return PostgresConstant.createIntConstant(text.length());
			}

			@Override
			public boolean supportsReturnType(PostgresDataType type) {
				return type == PostgresDataType.INT;
			}

			@Override
			public PostgresDataType[] getInputTypesForReturnType(PostgresDataType returnType, int nrArguments) {
				return new PostgresDataType[] { PostgresDataType.TEXT };
			}
		},
		UPPER(1, "upper") {

			@Override
			public PostgresConstant apply(PostgresConstant[] evaluatedArgs, PostgresExpression[] args) {
				if (evaluatedArgs[0].isNull()) {
					return PostgresConstant.createNullConstant();
				} else {
					String text = evaluatedArgs[0].asString();
					return PostgresConstant.createTextConstant(text.toUpperCase());
				}
			}

			@Override
			public boolean supportsReturnType(PostgresDataType type) {
				return type == PostgresDataType.TEXT;
			}

			@Override
			public PostgresDataType[] getInputTypesForReturnType(PostgresDataType returnType, int nrArguments) {
				return new PostgresDataType[] { PostgresDataType.TEXT };
			}

		},
//		NULL_IF(2, "nullif") {
//
//			@Override
//			public PostgresConstant apply(PostgresConstant[] evaluatedArgs, PostgresExpression[] args) {
//				PostgresConstant equals = evaluatedArgs[0].isEquals(evaluatedArgs[1]);
//				if (equals.isBoolean() && equals.asBoolean()) {
//					return PostgresConstant.createNullConstant();
//				} else {
//					// TODO: SELECT (nullif('1', FALSE)); yields '1', but should yield TRUE
//					return evaluatedArgs[0];
//				}
//			}
//
//			@Override
//			public boolean supportsReturnType(PostgresDataType type) {
//				return true;
//			}
//
//			@Override
//			public PostgresDataType[] getInputTypesForReturnType(PostgresDataType returnType, int nrArguments) {
//				return getType(nrArguments, returnType);
//			}
//
//			@Override
//			public boolean checkArguments(PostgresExpression[] constants) {
//				for (PostgresExpression e : constants) {
//					if (!(e instanceof PostgresNullConstant)) {
//						return true;
//					}
//				}
//				return false;
//			}
//
//		},
		NUM_NONNULLS(1, "num_nonnulls") {
			@Override
			public PostgresConstant apply(PostgresConstant[] args, PostgresExpression[] origArgs) {
				int nr = 0;
				for (PostgresConstant c : args) {
					if (!c.isNull()) {
						nr++;
					}
				}
				return PostgresConstant.createIntConstant(nr);
			}

			@Override
			public PostgresDataType[] getInputTypesForReturnType(PostgresDataType returnType, int nrArguments) {
				return getRandomTypes(nrArguments);
			}

			@Override
			public boolean supportsReturnType(PostgresDataType type) {
				return type == PostgresDataType.INT;
			}

			@Override
			public boolean isVariadic() {
				return true;
			}

		},
		NUM_NULLS(1, "num_nulls") {
			@Override
			public PostgresConstant apply(PostgresConstant[] args, PostgresExpression[] origArgs) {
				int nr = 0;
				for (PostgresConstant c : args) {
					if (c.isNull()) {
						nr++;
					}
				}
				return PostgresConstant.createIntConstant(nr);
			}

			@Override
			public PostgresDataType[] getInputTypesForReturnType(PostgresDataType returnType, int nrArguments) {
				return getRandomTypes(nrArguments);
			}

			@Override
			public boolean supportsReturnType(PostgresDataType type) {
				return type == PostgresDataType.INT;
			}

			@Override
			public boolean isVariadic() {
				return true;
			}

		},
//		TO_HEX(1, "to_hex") {
//
//			@Override
//			public PostgresConstant apply(PostgresConstant[] evaluatedArgs, PostgresExpression[] args) {
//				throw new IgnoreMeException();
//				// TODO: needs to take into account the column types
//				if (evaluatedArgs[0].isNull()) {
//					return PostgresConstant.createNullConstant();
//				} else {
//					long val = evaluatedArgs[0].cast(PostgresDataType.INT).asInt();
//					String hexString;
//					if ((int) val == val) {
//						hexString = Integer.toHexString((int) val);
//					} else {
//						hexString = Long.toHexString(val);
//					}
//					if (hexString.startsWith("0")) {
//						hexString = " " + hexString.substring(1);
//					}
//					return PostgresConstant.createTextConstant(hexString);
//				}
//			}
//
//			@Override
//			public boolean supportsReturnType(PostgresDataType type) {
//				return type == PostgresDataType.TEXT;
//			}
//
//			@Override
//			public PostgresDataType[] getInputTypesForReturnType(PostgresDataType returnType, int nrArguments) {
//				return new PostgresDataType[] { PostgresDataType.INT};
//			}
//			
//		},;

		;
		public PostgresDataType[] getRandomTypes(int nr) {
			PostgresDataType[] types = new PostgresDataType[nr];
			for (int i = 0; i < types.length; i++) {
				types[i] = PostgresDataType.getRandomType();
			}
			return types;
		}

		public PostgresDataType[] getType(int nr, PostgresDataType type) {
			PostgresDataType[] types = new PostgresDataType[nr];
			for (int i = 0; i < types.length; i++) {
				types[i] = type;
			}
			return types;
		}

		private String functionName;
		final int nrArgs;
		private final boolean variadic;

		private PostgresFunctionWithResult(int nrArgs, String functionName) {
			this.nrArgs = nrArgs;
			this.functionName = functionName;
			this.variadic = false;
		}

		private PostgresFunctionWithResult(int nrArgs, String functionName, boolean variadic) {
			this.nrArgs = nrArgs;
			this.functionName = functionName;
			this.variadic = variadic;
		}

		/**
		 * Gets the number of arguments if the function is non-variadic. If the function
		 * is variadic, the minimum number of arguments is returned.
		 */
		public int getNrArgs() {
			return nrArgs;
		}

		public abstract PostgresConstant apply(PostgresConstant[] evaluatedArgs, PostgresExpression[] args);

		public static PostgresFunctionWithResult getRandomFunction() {
			return Randomly.fromOptions(values());
		}

		@Override
		public String toString() {
			return functionName;
		}

		public boolean isVariadic() {
			return variadic;
		}

		public String getName() {
			return functionName;
		}

		public abstract boolean supportsReturnType(PostgresDataType type);

		public abstract PostgresDataType[] getInputTypesForReturnType(PostgresDataType returnType, int nrArguments);

		public boolean checkArguments(PostgresExpression[] constants) {
			return true;
		}

	}

	@Override
	public PostgresConstant getExpectedValue() {
		assert functionWithKnownResult != null;
		PostgresConstant[] constants = new PostgresConstant[args.length];
		for (int i = 0; i < constants.length; i++) {
			constants[i] = args[i].getExpectedValue();
		}
		return functionWithKnownResult.apply(constants, args);
	}

	@Override
	public PostgresDataType getExpressionType() {
		return returnType;
	}

}
