package sqlancer.presto.ast;

import java.util.ArrayList;

import sqlancer.Randomly;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.PrestoSchema.PrestoCompositeDataType;
import sqlancer.presto.PrestoSchema.PrestoDataType;

public enum PrestoComparisonFunction implements PrestoFunction {

    // comparison

    // Returns the largest of the provided values.
    // → [same as input]
    GREATEST("greatest", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoSchema.PrestoCompositeDataType returnType) {
            return PrestoDataType.getOrderableTypes().contains(returnType.getPrimitiveDataType());
        }

        @Override
        public int getNumberOfArguments() {
            return -1;
        }

        @Override
        public PrestoSchema.PrestoDataType[] getArgumentTypes(PrestoSchema.PrestoCompositeDataType returnType) {
            return new PrestoSchema.PrestoDataType[] { returnType.getPrimitiveDataType() };
        }
    },
    // Returns the smallest of the provided values.
    // → [same as input]#
    LEAST("least", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoSchema.PrestoCompositeDataType returnType) {
            return PrestoDataType.getOrderableTypes().contains(returnType.getPrimitiveDataType());
        }

        @Override
        public int getNumberOfArguments() {
            return -1;
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

    private final PrestoDataType returnType;
    private final PrestoDataType[] argumentTypes;
    private final String functionName;

    PrestoComparisonFunction(String functionName, PrestoDataType returnType, PrestoDataType... argumentTypes) {
        this.functionName = functionName;
        this.returnType = returnType;
        this.argumentTypes = argumentTypes.clone();
    }

    @Override
    public String getFunctionName() {
        return functionName;
    }

    @Override
    public boolean isCompatibleWithReturnType(PrestoCompositeDataType returnType) {
        return this.returnType == returnType.getPrimitiveDataType();
    }

    @Override
    public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
        return argumentTypes.clone();
    }

}
