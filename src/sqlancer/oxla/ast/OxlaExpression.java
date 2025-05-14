package sqlancer.oxla.ast;

import sqlancer.common.ast.newast.Expression;
import sqlancer.oxla.schema.OxlaColumn;

public interface OxlaExpression extends Expression<OxlaColumn> {
    default OxlaConstant getExpectedValue() {
        return null;
    }
}
