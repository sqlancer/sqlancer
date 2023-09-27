package sqlancer.materialize.ast;

import sqlancer.materialize.MaterializeSchema.MaterializeDataType;

public class MaterializeFunction implements MaterializeExpression {

    private final String func;
    private final MaterializeExpression[] args;
    private final MaterializeDataType returnType;
    private MaterializeFunctionWithResult functionWithKnownResult;

    public MaterializeFunction(MaterializeFunctionWithResult func, MaterializeDataType returnType,
            MaterializeExpression... args) {
        functionWithKnownResult = func;
        this.func = func.getName();
        this.returnType = returnType;
        this.args = args.clone();
    }

    public MaterializeFunction(MaterializeFunctionWithUnknownResult f, MaterializeDataType returnType,
            MaterializeExpression... args) {
        this.func = f.getName();
        this.returnType = returnType;
        this.args = args.clone();
    }

    public String getFunctionName() {
        return func;
    }

    public MaterializeExpression[] getArguments() {
        return args.clone();
    }

    public enum MaterializeFunctionWithResult {
        ABS(1, "abs") {

            @Override
            public MaterializeConstant apply(MaterializeConstant[] evaluatedArgs, MaterializeExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return MaterializeConstant.createNullConstant();
                } else {
                    return MaterializeConstant
                            .createIntConstant(Math.abs(evaluatedArgs[0].cast(MaterializeDataType.INT).asInt()));
                }
            }

            @Override
            public boolean supportsReturnType(MaterializeDataType type) {
                return type == MaterializeDataType.INT;
            }

            @Override
            public MaterializeDataType[] getInputTypesForReturnType(MaterializeDataType returnType, int nrArguments) {
                return new MaterializeDataType[] { returnType };
            }

        },
        LOWER(1, "lower") {

            @Override
            public MaterializeConstant apply(MaterializeConstant[] evaluatedArgs, MaterializeExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return MaterializeConstant.createNullConstant();
                } else {
                    String text = evaluatedArgs[0].asString();
                    return MaterializeConstant.createTextConstant(text.toLowerCase());
                }
            }

            @Override
            public boolean supportsReturnType(MaterializeDataType type) {
                return type == MaterializeDataType.TEXT;
            }

            @Override
            public MaterializeDataType[] getInputTypesForReturnType(MaterializeDataType returnType, int nrArguments) {
                return new MaterializeDataType[] { MaterializeDataType.TEXT };
            }

        },
        LENGTH(1, "length") {
            @Override
            public MaterializeConstant apply(MaterializeConstant[] evaluatedArgs, MaterializeExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return MaterializeConstant.createNullConstant();
                }
                String text = evaluatedArgs[0].asString();
                return MaterializeConstant.createIntConstant(text.length());
            }

            @Override
            public boolean supportsReturnType(MaterializeDataType type) {
                return type == MaterializeDataType.INT;
            }

            @Override
            public MaterializeDataType[] getInputTypesForReturnType(MaterializeDataType returnType, int nrArguments) {
                return new MaterializeDataType[] { MaterializeDataType.TEXT };
            }
        },
        UPPER(1, "upper") {

            @Override
            public MaterializeConstant apply(MaterializeConstant[] evaluatedArgs, MaterializeExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return MaterializeConstant.createNullConstant();
                } else {
                    String text = evaluatedArgs[0].asString();
                    return MaterializeConstant.createTextConstant(text.toUpperCase());
                }
            }

            @Override
            public boolean supportsReturnType(MaterializeDataType type) {
                return type == MaterializeDataType.TEXT;
            }

            @Override
            public MaterializeDataType[] getInputTypesForReturnType(MaterializeDataType returnType, int nrArguments) {
                return new MaterializeDataType[] { MaterializeDataType.TEXT };
            }

        },
        NUM_NONNULLS(1, "num_nonnulls") {
            @Override
            public MaterializeConstant apply(MaterializeConstant[] args, MaterializeExpression... origArgs) {
                int nr = 0;
                for (MaterializeConstant c : args) {
                    if (!c.isNull()) {
                        nr++;
                    }
                }
                return MaterializeConstant.createIntConstant(nr);
            }

            @Override
            public MaterializeDataType[] getInputTypesForReturnType(MaterializeDataType returnType, int nrArguments) {
                return getRandomTypes(nrArguments);
            }

            @Override
            public boolean supportsReturnType(MaterializeDataType type) {
                return type == MaterializeDataType.INT;
            }

            @Override
            public boolean isVariadic() {
                return true;
            }

        },
        NUM_NULLS(1, "num_nulls") {
            @Override
            public MaterializeConstant apply(MaterializeConstant[] args, MaterializeExpression... origArgs) {
                int nr = 0;
                for (MaterializeConstant c : args) {
                    if (c.isNull()) {
                        nr++;
                    }
                }
                return MaterializeConstant.createIntConstant(nr);
            }

            @Override
            public MaterializeDataType[] getInputTypesForReturnType(MaterializeDataType returnType, int nrArguments) {
                return getRandomTypes(nrArguments);
            }

            @Override
            public boolean supportsReturnType(MaterializeDataType type) {
                return type == MaterializeDataType.INT;
            }

            @Override
            public boolean isVariadic() {
                return true;
            }

        };

        private String functionName;
        final int nrArgs;
        private final boolean variadic;

        public MaterializeDataType[] getRandomTypes(int nr) {
            MaterializeDataType[] types = new MaterializeDataType[nr];
            for (int i = 0; i < types.length; i++) {
                types[i] = MaterializeDataType.getRandomType();
            }
            return types;
        }

        MaterializeFunctionWithResult(int nrArgs, String functionName) {
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

        public abstract MaterializeConstant apply(MaterializeConstant[] evaluatedArgs, MaterializeExpression... args);

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

        public abstract boolean supportsReturnType(MaterializeDataType type);

        public abstract MaterializeDataType[] getInputTypesForReturnType(MaterializeDataType returnType,
                int nrArguments);

        public boolean checkArguments(MaterializeExpression... constants) {
            return true;
        }

    }

    @Override
    public MaterializeConstant getExpectedValue() {
        if (functionWithKnownResult == null) {
            return null;
        }
        MaterializeConstant[] constants = new MaterializeConstant[args.length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = args[i].getExpectedValue();
            if (constants[i] == null) {
                return null;
            }
        }
        return functionWithKnownResult.apply(constants, args);
    }

    @Override
    public MaterializeDataType getExpressionType() {
        return returnType;
    }

}
