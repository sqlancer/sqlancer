package sqlancer.cnosdb.ast;

import sqlancer.cnosdb.CnosDBSchema.CnosDBColumn;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.common.ast.newast.Expression;

public interface CnosDBExpression extends Expression<CnosDBColumn> {

    default CnosDBDataType getExpressionType() {
        return null;
    }

    default CnosDBConstant getExpectedValue() {
        throw new AssertionError("Not impl");
    }
}
