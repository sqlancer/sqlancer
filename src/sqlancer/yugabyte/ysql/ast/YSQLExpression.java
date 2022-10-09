package sqlancer.yugabyte.ysql.ast;

import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public interface YSQLExpression {

    default YSQLDataType getExpressionType() {
        return null;
    }

    default YSQLConstant getExpectedValue() {
        return null;
    }
}
