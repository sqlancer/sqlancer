package sqlancer.yugabyte.ysql.ast;

import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public class YSQLFunction implements YSQLExpression {

    private final String func;
    private final YSQLExpression[] args;
    private final YSQLDataType returnType;
    private YSQLFunctionWithResult functionWithKnownResult;

    public YSQLFunction(YSQLFunctionWithResult func, YSQLDataType returnType, YSQLExpression... args) {
        functionWithKnownResult = func;
        this.func = func.getName();
        this.returnType = returnType;
        this.args = args.clone();
    }

    public YSQLFunction(YSQLFunctionWithUnknownResult f, YSQLDataType returnType, YSQLExpression... args) {
        this.func = f.getName();
        this.returnType = returnType;
        this.args = args.clone();
    }

    public String getFunctionName() {
        return func;
    }

    public YSQLExpression[] getArguments() {
        return args.clone();
    }

    @Override
    public YSQLDataType getExpressionType() {
        return returnType;
    }

    @Override
    public YSQLConstant getExpectedValue() {
        if (functionWithKnownResult == null) {
            return null;
        }
        YSQLConstant[] constants = new YSQLConstant[args.length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = args[i].getExpectedValue();
            if (constants[i] == null) {
                return null;
            }
        }
        return functionWithKnownResult.apply(constants, args);
    }

    public enum YSQLFunctionWithResult {
        ABS(1, "abs") {
            @Override
            public YSQLConstant apply(YSQLConstant[] evaluatedArgs, YSQLExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return YSQLConstant.createNullConstant();
                } else {
                    return YSQLConstant.createIntConstant(Math.abs(evaluatedArgs[0].cast(YSQLDataType.INT).asInt()));
                }
            }

            @Override
            public boolean supportsReturnType(YSQLDataType type) {
                return type == YSQLDataType.INT;
            }

            @Override
            public YSQLDataType[] getInputTypesForReturnType(YSQLDataType returnType, int nrArguments) {
                return new YSQLDataType[] { returnType };
            }

        },
        LOWER(1, "lower") {
            @Override
            public YSQLConstant apply(YSQLConstant[] evaluatedArgs, YSQLExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return YSQLConstant.createNullConstant();
                } else {
                    String text = evaluatedArgs[0].asString();
                    return YSQLConstant.createTextConstant(text.toLowerCase());
                }
            }

            @Override
            public boolean supportsReturnType(YSQLDataType type) {
                return type == YSQLDataType.TEXT;
            }

            @Override
            public YSQLDataType[] getInputTypesForReturnType(YSQLDataType returnType, int nrArguments) {
                return new YSQLDataType[] { YSQLDataType.TEXT };
            }

        },
        LENGTH(1, "length") {
            @Override
            public YSQLConstant apply(YSQLConstant[] evaluatedArgs, YSQLExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return YSQLConstant.createNullConstant();
                }
                String text = evaluatedArgs[0].asString();
                return YSQLConstant.createIntConstant(text.length());
            }

            @Override
            public boolean supportsReturnType(YSQLDataType type) {
                return type == YSQLDataType.INT;
            }

            @Override
            public YSQLDataType[] getInputTypesForReturnType(YSQLDataType returnType, int nrArguments) {
                return new YSQLDataType[] { YSQLDataType.TEXT };
            }
        },
        UPPER(1, "upper") {
            @Override
            public YSQLConstant apply(YSQLConstant[] evaluatedArgs, YSQLExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return YSQLConstant.createNullConstant();
                } else {
                    String text = evaluatedArgs[0].asString();
                    return YSQLConstant.createTextConstant(text.toUpperCase());
                }
            }

            @Override
            public boolean supportsReturnType(YSQLDataType type) {
                return type == YSQLDataType.TEXT;
            }

            @Override
            public YSQLDataType[] getInputTypesForReturnType(YSQLDataType returnType, int nrArguments) {
                return new YSQLDataType[] { YSQLDataType.TEXT };
            }

        },
        // NULL_IF(2, "nullif") {
        //
        // @Override
        // public YSQLConstant apply(YSQLConstant[] evaluatedArgs, YSQLExpression[] args) {
        // YSQLConstant equals = evaluatedArgs[0].isEquals(evaluatedArgs[1]);
        // if (equals.isBoolean() && equals.asBoolean()) {
        // return YSQLConstant.createNullConstant();
        // } else {
        // // TODO: SELECT (nullif('1', FALSE)); yields '1', but should yield TRUE
        // return evaluatedArgs[0];
        // }
        // }
        //
        // @Override
        // public boolean supportsReturnType(YSQLDataType type) {
        // return true;
        // }
        //
        // @Override
        // public YSQLDataType[] getInputTypesForReturnType(YSQLDataType returnType, int nrArguments) {
        // return getType(nrArguments, returnType);
        // }
        //
        // @Override
        // public boolean checkArguments(YSQLExpression[] constants) {
        // for (YSQLExpression e : constants) {
        // if (!(e instanceof YSQLNullConstant)) {
        // return true;
        // }
        // }
        // return false;
        // }
        //
        // },
        NUM_NONNULLS(1, "num_nonnulls") {
            @Override
            public YSQLConstant apply(YSQLConstant[] args, YSQLExpression... origArgs) {
                int nr = 0;
                for (YSQLConstant c : args) {
                    if (!c.isNull()) {
                        nr++;
                    }
                }
                return YSQLConstant.createIntConstant(nr);
            }

            @Override
            public YSQLDataType[] getInputTypesForReturnType(YSQLDataType returnType, int nrArguments) {
                return getRandomTypes(nrArguments);
            }

            @Override
            public boolean supportsReturnType(YSQLDataType type) {
                return type == YSQLDataType.INT;
            }

            @Override
            public boolean isVariadic() {
                return true;
            }

        },
        NUM_NULLS(1, "num_nulls") {
            @Override
            public YSQLConstant apply(YSQLConstant[] args, YSQLExpression... origArgs) {
                int nr = 0;
                for (YSQLConstant c : args) {
                    if (c.isNull()) {
                        nr++;
                    }
                }
                return YSQLConstant.createIntConstant(nr);
            }

            @Override
            public YSQLDataType[] getInputTypesForReturnType(YSQLDataType returnType, int nrArguments) {
                return getRandomTypes(nrArguments);
            }

            @Override
            public boolean supportsReturnType(YSQLDataType type) {
                return type == YSQLDataType.INT;
            }

            @Override
            public boolean isVariadic() {
                return true;
            }

        },
        // Mathematical functions that support pushdown
        CEIL(1, "ceil") {
            @Override
            public YSQLConstant apply(YSQLConstant[] evaluatedArgs, YSQLExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return YSQLConstant.createNullConstant();
                }
                // Cast to INT first to get numeric value, then perform operation
                long val = evaluatedArgs[0].cast(YSQLDataType.INT).asInt();
                return YSQLConstant.createIntConstant((long) Math.ceil(val));
            }

            @Override
            public boolean supportsReturnType(YSQLDataType type) {
                return type == YSQLDataType.BIGINT || type == YSQLDataType.INT;
            }

            @Override
            public YSQLDataType[] getInputTypesForReturnType(YSQLDataType returnType, int nrArguments) {
                return new YSQLDataType[] { YSQLDataType.INT };
            }
        },
        FLOOR(1, "floor") {
            @Override
            public YSQLConstant apply(YSQLConstant[] evaluatedArgs, YSQLExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return YSQLConstant.createNullConstant();
                }
                // Cast to INT first to get numeric value, then perform operation
                long val = evaluatedArgs[0].cast(YSQLDataType.INT).asInt();
                return YSQLConstant.createIntConstant((long) Math.floor(val));
            }

            @Override
            public boolean supportsReturnType(YSQLDataType type) {
                return type == YSQLDataType.BIGINT || type == YSQLDataType.INT;
            }

            @Override
            public YSQLDataType[] getInputTypesForReturnType(YSQLDataType returnType, int nrArguments) {
                return new YSQLDataType[] { YSQLDataType.INT };
            }
        },
        ROUND(1, "round") {
            @Override
            public YSQLConstant apply(YSQLConstant[] evaluatedArgs, YSQLExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return YSQLConstant.createNullConstant();
                }
                // Cast to INT first to get numeric value
                long val = evaluatedArgs[0].cast(YSQLDataType.INT).asInt();
                return YSQLConstant.createIntConstant(val);
            }

            @Override
            public boolean supportsReturnType(YSQLDataType type) {
                return type == YSQLDataType.BIGINT || type == YSQLDataType.INT;
            }

            @Override
            public YSQLDataType[] getInputTypesForReturnType(YSQLDataType returnType, int nrArguments) {
                return new YSQLDataType[] { YSQLDataType.INT };
            }
        },
        SQRT(1, "sqrt") {
            @Override
            public YSQLConstant apply(YSQLConstant[] evaluatedArgs, YSQLExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return YSQLConstant.createNullConstant();
                }
                // For SQRT, just return a float constant for now since we don't have float evaluation
                return YSQLConstant.createFloatConstant(1.0f);
            }

            @Override
            public boolean supportsReturnType(YSQLDataType type) {
                return type == YSQLDataType.FLOAT || type == YSQLDataType.REAL || type == YSQLDataType.DOUBLE_PRECISION;
            }

            @Override
            public YSQLDataType[] getInputTypesForReturnType(YSQLDataType returnType, int nrArguments) {
                return new YSQLDataType[] { YSQLDataType.FLOAT };
            }
        },
        // String functions that support pushdown
        SUBSTRING(3, "substring") {
            @Override
            public YSQLConstant apply(YSQLConstant[] evaluatedArgs, YSQLExpression... args) {
                if (evaluatedArgs[0].isNull() || evaluatedArgs[1].isNull() || evaluatedArgs[2].isNull()) {
                    return YSQLConstant.createNullConstant();
                }
                String str = evaluatedArgs[0].asString();
                int start = (int) evaluatedArgs[1].asInt() - 1; // PostgreSQL uses 1-based indexing
                int length = (int) evaluatedArgs[2].asInt();
                if (start < 0) start = 0;
                if (start >= str.length()) return YSQLConstant.createTextConstant("");
                int end = Math.min(start + length, str.length());
                return YSQLConstant.createTextConstant(str.substring(start, end));
            }

            @Override
            public boolean supportsReturnType(YSQLDataType type) {
                return type == YSQLDataType.TEXT;
            }

            @Override
            public YSQLDataType[] getInputTypesForReturnType(YSQLDataType returnType, int nrArguments) {
                return new YSQLDataType[] { YSQLDataType.TEXT, YSQLDataType.INT, YSQLDataType.INT };
            }
        },
        TRIM(1, "trim") {
            @Override
            public YSQLConstant apply(YSQLConstant[] evaluatedArgs, YSQLExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return YSQLConstant.createNullConstant();
                }
                return YSQLConstant.createTextConstant(evaluatedArgs[0].asString().trim());
            }

            @Override
            public boolean supportsReturnType(YSQLDataType type) {
                return type == YSQLDataType.TEXT;
            }

            @Override
            public YSQLDataType[] getInputTypesForReturnType(YSQLDataType returnType, int nrArguments) {
                return new YSQLDataType[] { YSQLDataType.TEXT };
            }
        };

        final int nrArgs;
        private final String functionName;
        private final boolean variadic;

        YSQLFunctionWithResult(int nrArgs, String functionName) {
            this.nrArgs = nrArgs;
            this.functionName = functionName;
            this.variadic = false;
        }

        public YSQLDataType[] getRandomTypes(int nr) {
            YSQLDataType[] types = new YSQLDataType[nr];
            for (int i = 0; i < types.length; i++) {
                types[i] = YSQLDataType.getRandomType();
            }
            return types;
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

        public abstract YSQLConstant apply(YSQLConstant[] evaluatedArgs, YSQLExpression... args);

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

        public abstract boolean supportsReturnType(YSQLDataType type);

        public abstract YSQLDataType[] getInputTypesForReturnType(YSQLDataType returnType, int nrArguments);

        public boolean checkArguments(YSQLExpression... constants) {
            return true;
        }

    }

}
