package sqlancer.yugabyte.ycql.ast;

import sqlancer.common.ast.newast.Expression;
import sqlancer.common.schema.AbstractTableColumn;

public interface YCQLExpression extends Expression<AbstractTableColumn<?,?>> {

    default YCQLConstant getExpectedValue() {
        return null;
    }
}
