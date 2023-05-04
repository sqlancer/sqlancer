package sqlancer.materialize.ast;

import sqlancer.materialize.MaterializeSchema.MaterializeDataType;

public interface MaterializeExpression {

    default MaterializeDataType getExpressionType() {
        return null;
    }

    default MaterializeConstant getExpectedValue() {
        return null;
    }
}
