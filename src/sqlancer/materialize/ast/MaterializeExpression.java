package sqlancer.materialize.ast;

import sqlancer.common.ast.newast.Expression;
import sqlancer.materialize.MaterializeSchema.MaterializeColumn;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;

public interface MaterializeExpression extends Expression<MaterializeColumn> {

    default MaterializeDataType getExpressionType() {
        return null;
    }

    default MaterializeConstant getExpectedValue() {
        return null;
    }
}
