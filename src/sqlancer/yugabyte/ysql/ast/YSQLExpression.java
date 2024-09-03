package sqlancer.yugabyte.ysql.ast;

import sqlancer.common.ast.newast.Expression;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public interface YSQLExpression extends Expression<YSQLColumn> {

    default YSQLDataType getExpressionType() {
        return null;
    }

    default YSQLConstant getExpectedValue() {
        return null;
    }
}
