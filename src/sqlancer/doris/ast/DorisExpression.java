package sqlancer.doris.ast;

import sqlancer.doris.DorisSchema;

public interface DorisExpression {
    default DorisSchema.DorisDataType getExpectedType() {
        return null;
    }

    default DorisConstant getExpectedValue() {
        return null;
    }
}
