package sqlancer.presto.ast;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.PrestoSchema.PrestoCompositeDataType;
import sqlancer.presto.PrestoSchema.PrestoDataType;
import sqlancer.presto.gen.PrestoTypedExpressionGenerator;

public enum PrestoDefaultFunction implements PrestoFunction {

    // Conditional functions
    IF_TRUE("if", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoSchema.PrestoCompositeDataType returnType) {
            return true;
        }

        @Override
        public PrestoSchema.PrestoDataType[] getArgumentTypes(PrestoSchema.PrestoCompositeDataType returnType) {
            return new PrestoSchema.PrestoDataType[] { PrestoSchema.PrestoDataType.BOOLEAN,
                    returnType.getPrimitiveDataType() };
        }
    },

    IF_TRUE_FALSE("if", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoSchema.PrestoCompositeDataType returnType) {
            return true;
        }

        @Override
        public PrestoSchema.PrestoDataType[] getArgumentTypes(PrestoSchema.PrestoCompositeDataType returnType) {
            return new PrestoSchema.PrestoDataType[] { PrestoSchema.PrestoDataType.BOOLEAN,
                    returnType.getPrimitiveDataType(), returnType.getPrimitiveDataType() };
        }
    },

    NULLIF("nullif", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoSchema.PrestoCompositeDataType returnType) {
            return true;
        }

        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] { returnType.getPrimitiveDataType(), returnType.getPrimitiveDataType() };
        }
    },

    COALESCE("coalesce", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoSchema.PrestoCompositeDataType returnType) {
            return true;
        }

        @Override
        public int getNumberOfArguments() {
            return UNLIMITED_NO_OF_ARGUMENTS;
        }

        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            ArrayList<PrestoDataType> prestoDataTypes = new ArrayList<>();
            long no = Randomly.getNotCachedInteger(2, 10);
            for (int i = 0; i < no; i++) {
                prestoDataTypes.add(returnType.getPrimitiveDataType());
            }
            return prestoDataTypes.toArray(new PrestoDataType[0]);
        }

        @Override
        public List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                PrestoDataType[] argumentTypes, PrestoCompositeDataType returnType) {
            return super.getArgumentsForReturnType(gen, depth, argumentTypes, returnType);
        }
    },

    // comparison

    // Returns the largest of the provided values. → [same as input]
    GREATEST("greatest", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoSchema.PrestoCompositeDataType returnType) {
            return PrestoDataType.getOrderableTypes().contains(returnType.getPrimitiveDataType());
        }

        @Override
        public int getNumberOfArguments() {
            return UNLIMITED_NO_OF_ARGUMENTS;
        }

        @Override
        public PrestoSchema.PrestoDataType[] getArgumentTypes(PrestoSchema.PrestoCompositeDataType returnType) {
            return new PrestoSchema.PrestoDataType[] { returnType.getPrimitiveDataType() };
        }
    },
    // Returns the smallest of the provided values. → [same as input]
    LEAST("least", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoSchema.PrestoCompositeDataType returnType) {
            return PrestoDataType.getOrderableTypes().contains(returnType.getPrimitiveDataType());
        }

        @Override
        public int getNumberOfArguments() {
            return UNLIMITED_NO_OF_ARGUMENTS;
        }

        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            ArrayList<PrestoDataType> prestoDataTypes = new ArrayList<>();
            long no = Randomly.getNotCachedInteger(2, 10);
            for (int i = 0; i < no; i++) {
                prestoDataTypes.add(returnType.getPrimitiveDataType());
            }
            return prestoDataTypes.toArray(new PrestoDataType[0]);
        }
    };

    private static final int UNLIMITED_NO_OF_ARGUMENTS = -1;
    private final PrestoDataType returnType;
    private final PrestoDataType[] argumentTypes;
    private final String functionName;

    PrestoDefaultFunction(String functionName, PrestoDataType returnType) {
        this.functionName = functionName;
        this.returnType = returnType;
        this.argumentTypes = new PrestoDataType[0];
    }

    PrestoDefaultFunction(PrestoDataType returnType) {
        this.returnType = returnType;
        this.argumentTypes = new PrestoDataType[0];
        this.functionName = toString();
    }

    PrestoDefaultFunction(PrestoDataType returnType, PrestoDataType... argumentTypes) {
        this.returnType = returnType;
        this.argumentTypes = argumentTypes.clone();
        this.functionName = toString();
    }

    PrestoDefaultFunction(String functionName, PrestoDataType returnType, PrestoDataType... argumentTypes) {
        this.functionName = functionName;
        this.returnType = returnType;
        this.argumentTypes = argumentTypes.clone();
    }

    public static List<PrestoDefaultFunction> getFunctionsCompatibleWith(PrestoCompositeDataType returnType) {
        return Stream.of(values()).filter(f -> f.isCompatibleWithReturnType(returnType)).collect(Collectors.toList());
    }

    @Override
    public String getFunctionName() {
        return functionName;
    }

    @Override
    public int getNumberOfArguments() {
        return argumentTypes == null ? 0 : argumentTypes.length;
    }

    @Override
    public boolean isCompatibleWithReturnType(PrestoCompositeDataType returnType) {
        return this.returnType == returnType.getPrimitiveDataType();
    }

    @Override
    public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
        return argumentTypes.clone();
    }

    @Override
    public List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
            PrestoDataType[] argumentTypes, PrestoCompositeDataType returnType) {
        List<Node<PrestoExpression>> arguments = new ArrayList<>();

        // This is a workaround based on the assumption that array types should refer to the same element type.
        PrestoCompositeDataType savedArrayType = null;
        if (returnType.getPrimitiveDataType() == PrestoDataType.ARRAY) {
            savedArrayType = returnType;
        }

        if (getNumberOfArguments() == UNLIMITED_NO_OF_ARGUMENTS) {
            PrestoDataType dataType = getArgumentTypes(returnType)[0];
            // TODO: consider upper
            long no = Randomly.getNotCachedInteger(2, 10);
            for (int i = 0; i < no; i++) {
                PrestoCompositeDataType type;

                if (dataType == PrestoDataType.ARRAY) {
                    if (savedArrayType == null) {
                        savedArrayType = dataType.get();
                    }
                    type = savedArrayType;
                } else {
                    type = PrestoCompositeDataType.fromDataType(dataType);
                }
                arguments.add(gen.generateExpression(type, depth + 1));
            }
        } else {
            for (PrestoDataType arg : argumentTypes) {
                PrestoCompositeDataType type;
                if (arg == PrestoDataType.ARRAY) {
                    if (savedArrayType == null) {
                        savedArrayType = arg.get();
                    }
                    type = savedArrayType;
                } else {
                    type = PrestoCompositeDataType.fromDataType(arg);
                }
                arguments.add(gen.generateExpression(type, depth + 1));

            }
        }
        return arguments;
    }

    @Override
    public String toString() {
        if (functionName != null) {
            return functionName;
        }
        return super.toString();
    }

}
