package sqlancer.postgres.ast;

import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresFunction implements PostgresExpression {

    private final String func;
    private final PostgresExpression[] args;
    private final PostgresDataType returnType;
    private PostgresFunctionWithResult functionWithKnownResult;

    public PostgresFunction(PostgresFunctionWithResult func, PostgresDataType returnType, PostgresExpression... args) {
        functionWithKnownResult = func;
        this.func = func.getName();
        this.returnType = returnType;
        this.args = args.clone();
    }

    public PostgresFunction(PostgresFunctionWithUnknownResult f, PostgresDataType returnType,
            PostgresExpression... args) {
        this.func = f.getName();
        this.returnType = returnType;
        this.args = args.clone();
    }

    public String getFunctionName() {
        return func;
    }

    public PostgresExpression[] getArguments() {
        return args.clone();
    }

    public enum PostgresFunctionWithResult {
        ABS(1, "abs") {

            @Override
            public PostgresConstant apply(PostgresConstant[] evaluatedArgs, PostgresExpression... args) {
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
            public PostgresConstant apply(PostgresConstant[] evaluatedArgs, PostgresExpression... args) {
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
            public PostgresConstant apply(PostgresConstant[] evaluatedArgs, PostgresExpression... args) {
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
            public PostgresConstant apply(PostgresConstant[] evaluatedArgs, PostgresExpression... args) {
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
        // NULL_IF(2, "nullif") {
        //
        // @Override
        // public PostgresConstant apply(PostgresConstant[] evaluatedArgs, PostgresExpression[] args) {
        // PostgresConstant equals = evaluatedArgs[0].isEquals(evaluatedArgs[1]);
        // if (equals.isBoolean() && equals.asBoolean()) {
        // return PostgresConstant.createNullConstant();
        // } else {
        // // TODO: SELECT (nullif('1', FALSE)); yields '1', but should yield TRUE
        // return evaluatedArgs[0];
        // }
        // }
        //
        // @Override
        // public boolean supportsReturnType(PostgresDataType type) {
        // return true;
        // }
        //
        // @Override
        // public PostgresDataType[] getInputTypesForReturnType(PostgresDataType returnType, int nrArguments) {
        // return getType(nrArguments, returnType);
        // }
        //
        // @Override
        // public boolean checkArguments(PostgresExpression[] constants) {
        // for (PostgresExpression e : constants) {
        // if (!(e instanceof PostgresNullConstant)) {
        // return true;
        // }
        // }
        // return false;
        // }
        //
        // },
        NUM_NONNULLS(1, "num_nonnulls") {
            @Override
            public PostgresConstant apply(PostgresConstant[] args, PostgresExpression... origArgs) {
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
            public PostgresConstant apply(PostgresConstant[] args, PostgresExpression... origArgs) {
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

        };

        private String functionName;
        final int nrArgs;
        private final boolean variadic;

        public PostgresDataType[] getRandomTypes(int nr) {
            PostgresDataType[] types = new PostgresDataType[nr];
            for (int i = 0; i < types.length; i++) {
                types[i] = PostgresDataType.getRandomType();
            }
            return types;
        }

        PostgresFunctionWithResult(int nrArgs, String functionName) {
            this.nrArgs = nrArgs;
            this.functionName = functionName;
            this.variadic = false;
        }

        /**
         * Gets the number of arguments if the function is non-variadic. If the function is variadic, the minimum number
         * of arguments is returned.
         *
         * @return the number of arguments
         */
        public int getNrArgs() {
            return nrArgs;
        }

        public abstract PostgresConstant apply(PostgresConstant[] evaluatedArgs, PostgresExpression... args);

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

        public boolean checkArguments(PostgresExpression... constants) {
            return true;
        }

    }

    @Override
    public PostgresConstant getExpectedValue() {
        if (functionWithKnownResult == null) {
            return null;
        }
        PostgresConstant[] constants = new PostgresConstant[args.length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = args[i].getExpectedValue();
            if (constants[i] == null) {
                return null;
            }
        }
        return functionWithKnownResult.apply(constants, args);
    }

    @Override
    public PostgresDataType getExpressionType() {
        return returnType;
    }

}
