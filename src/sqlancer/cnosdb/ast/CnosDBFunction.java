package sqlancer.cnosdb.ast;

import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;

public class CnosDBFunction implements CnosDBExpression {

    private final String func;
    private final CnosDBExpression[] args;
    private final CnosDBDataType returnType;
    private CnosDBFunctionWithResult functionWithKnownResult;

    public CnosDBFunction(CnosDBFunctionWithResult func, CnosDBDataType returnType, CnosDBExpression... args) {
        functionWithKnownResult = func;
        this.func = func.getName();
        this.returnType = returnType;
        this.args = args.clone();
    }

    public CnosDBFunction(CnosDBFunctionWithUnknownResult f, CnosDBDataType returnType, CnosDBExpression... args) {
        this.func = f.getName();
        this.returnType = returnType;
        this.args = args.clone();
    }

    public String getFunctionName() {
        return func;
    }

    public CnosDBExpression[] getArguments() {
        return args.clone();
    }

    public enum CnosDBFunctionWithResult {
        ABS(1, "abs") {
            @Override
            public CnosDBConstant apply(CnosDBConstant[] evaluatedArgs, CnosDBExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return CnosDBConstant.createNullConstant();
                } else {
                    return CnosDBConstant
                            .createDoubleConstant(Math.abs(evaluatedArgs[0].cast(CnosDBDataType.DOUBLE).asDouble()));
                }
            }

            @Override
            public boolean supportsReturnType(CnosDBDataType type) {
                return type == CnosDBDataType.DOUBLE;
            }

            @Override
            public CnosDBDataType[] getInputTypesForReturnType(CnosDBDataType returnType, int nrArguments) {
                return new CnosDBDataType[] { returnType };
            }

        },
        LOWER(1, "lower") {
            @Override
            public CnosDBConstant apply(CnosDBConstant[] evaluatedArgs, CnosDBExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return CnosDBConstant.createNullConstant();
                } else {
                    String text = evaluatedArgs[0].asString();
                    return CnosDBConstant.createStringConstant(text.toLowerCase());
                }
            }

            @Override
            public boolean supportsReturnType(CnosDBDataType type) {
                return type == CnosDBDataType.STRING;
            }

            @Override
            public CnosDBDataType[] getInputTypesForReturnType(CnosDBDataType returnType, int nrArguments) {
                return new CnosDBDataType[] { CnosDBDataType.STRING };
            }

        },
        LENGTH(1, "length") {
            @Override
            public CnosDBConstant apply(CnosDBConstant[] evaluatedArgs, CnosDBExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return CnosDBConstant.createNullConstant();
                }
                String text = evaluatedArgs[0].asString();
                return CnosDBConstant.createIntConstant(text.length());
            }

            @Override
            public boolean supportsReturnType(CnosDBDataType type) {
                return type == CnosDBDataType.INT;
            }

            @Override
            public CnosDBDataType[] getInputTypesForReturnType(CnosDBDataType returnType, int nrArguments) {
                return new CnosDBDataType[] { CnosDBDataType.STRING };
            }
        },
        UPPER(1, "upper") {
            @Override
            public CnosDBConstant apply(CnosDBConstant[] evaluatedArgs, CnosDBExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return CnosDBConstant.createNullConstant();
                } else {
                    String text = evaluatedArgs[0].asString();
                    return CnosDBConstant.createStringConstant(text.toUpperCase());
                }
            }

            @Override
            public boolean supportsReturnType(CnosDBDataType type) {
                return type == CnosDBDataType.STRING;
            }

            @Override
            public CnosDBDataType[] getInputTypesForReturnType(CnosDBDataType returnType, int nrArguments) {
                return new CnosDBDataType[] { CnosDBDataType.STRING };
            }

        },
        // NULL_IF(2, "nullif") {
        //
        // @Override
        // public CnosDBConstant apply(CnosDBConstant[] evaluatedArgs, CnosDBExpression[] args) {
        // CnosDBConstant equals = evaluatedArgs[0].isEquals(evaluatedArgs[1]);
        // if (equals.isBoolean() && equals.asBoolean()) {
        // return CnosDBConstant.createNullConstant();
        // } else {
        // // TODO: SELECT (nullif('1', FALSE)); yields '1', but should yield TRUE
        // return evaluatedArgs[0];
        // }
        // }
        //
        // @Override
        // public boolean supportsReturnType(CnosDBDataType type) {
        // return true;
        // }
        //
        // @Override
        // public CnosDBDataType[] getInputTypesForReturnType(CnosDBDataType returnType, int nrArguments) {
        // return getType(nrArguments, returnType);
        // }
        //
        // @Override
        // public boolean checkArguments(CnosDBExpression[] constants) {
        // for (CnosDBExpression e : constants) {
        // if (!(e instanceof CnosDBNullConstant)) {
        // return true;
        // }
        // }
        // return false;
        // }
        //
        // },
        // NUM_NONNULLS(1, "num_nonnulls") {
        // @Override
        // public CnosDBConstant apply(CnosDBConstant[] args, CnosDBExpression... origArgs) {
        // int nr = 0;
        // for (CnosDBConstant c : args) {
        // if (!c.isNull()) {
        // nr++;
        // }
        // }
        // return CnosDBConstant.createIntConstant(nr);
        // }
        //
        // @Override
        // public CnosDBDataType[] getInputTypesForReturnType(CnosDBDataType returnType, int nrArguments) {
        // return getRandomTypes(nrArguments);
        // }
        //
        // @Override
        // public boolean supportsReturnType(CnosDBDataType type) {
        // return type == CnosDBDataType.INT;
        // }
        //
        // @Override
        // public boolean isVariadic() {
        // return true;
        // }
        //
        // },
        // NUM_NULLS(1, "num_nulls") {
        // @Override
        // public CnosDBConstant apply(CnosDBConstant[] args, CnosDBExpression... origArgs) {
        // int nr = 0;
        // for (CnosDBConstant c : args) {
        // if (c.isNull()) {
        // nr++;
        // }
        // }
        // return CnosDBConstant.createIntConstant(nr);
        // }
        //
        // @Override
        // public CnosDBDataType[] getInputTypesForReturnType(CnosDBDataType returnType, int nrArguments) {
        // return getRandomTypes(nrArguments);
        // }
        //
        // @Override
        // public boolean supportsReturnType(CnosDBDataType type) {
        // return type == CnosDBDataType.INT;
        // }
        //
        // @Override
        // public boolean isVariadic() {
        // return true;
        // }
        //
        // }
        ;

        private String functionName;
        final int nrArgs;
        private final boolean variadic;

        CnosDBFunctionWithResult(int nrArgs, String functionName) {
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

        public abstract CnosDBConstant apply(CnosDBConstant[] evaluatedArgs, CnosDBExpression... args);

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

        public abstract boolean supportsReturnType(CnosDBDataType type);

        public abstract CnosDBDataType[] getInputTypesForReturnType(CnosDBDataType returnType, int nrArguments);

        public boolean checkArguments(CnosDBExpression... constants) {
            return true;
        }

    }

    @Override
    public CnosDBConstant getExpectedValue() {
        if (functionWithKnownResult == null) {
            return null;
        }
        CnosDBConstant[] constants = new CnosDBConstant[args.length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = args[i].getExpectedValue();
            if (constants[i] == null) {
                return null;
            }
        }
        return functionWithKnownResult.apply(constants, args);
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return returnType;
    }

}
