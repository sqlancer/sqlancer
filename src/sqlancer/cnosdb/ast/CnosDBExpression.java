package sqlancer.cnosdb.ast;

import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;

public interface CnosDBExpression {

    default CnosDBDataType getExpressionType() {
        return null;
    }

    default CnosDBConstant getExpectedValue() {
        return null;
    }
}
