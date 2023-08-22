package sqlancer.presto.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.presto.PrestoSchema.PrestoCompositeDataType;
import sqlancer.presto.PrestoSchema.PrestoDataType;

public enum PrestoConditionalFunction implements PrestoFunction {

    // Conditional functions
    IF_TRUE("if", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoCompositeDataType returnType) {
            return true;
        }

        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] { PrestoDataType.BOOLEAN, returnType.getPrimitiveDataType() };
        }
    },

    IF_TRUE_FALSE("if", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoCompositeDataType returnType) {
            return true;
        }

        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] { PrestoDataType.BOOLEAN, returnType.getPrimitiveDataType(),
                    returnType.getPrimitiveDataType() };
        }
    },

    NULLIF("nullif", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoCompositeDataType returnType) {
            return true;
        }

        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[] { returnType.getPrimitiveDataType(), returnType.getPrimitiveDataType() };
        }
    },

    COALESCE("coalesce", null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoCompositeDataType returnType) {
            return true;
        }

        @Override
        public int getNumberOfArguments() {
            return -1;
        }

        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            List<PrestoDataType> prestoDataTypes = new ArrayList<>();
            long no = Randomly.getNotCachedInteger(2, 10);
            for (int i = 0; i < no; i++) {
                prestoDataTypes.add(returnType.getPrimitiveDataType());
            }
            return prestoDataTypes.toArray(new PrestoDataType[0]);
        }
    };

    private final PrestoDataType returnType;
    private final String functionName;

    PrestoConditionalFunction(String functionName, PrestoDataType returnType) {
        this.functionName = functionName;
        this.returnType = returnType;
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
    public int getNumberOfArguments() {
        return getArgumentTypes(PrestoCompositeDataType.fromDataType(returnType)).length;
    }

}
