package sqlancer.mysql.ast;

import sqlancer.common.ast.newast.Expression;
import sqlancer.mysql.MySQLSchema.MySQLColumn;

public interface MySQLExpression extends Expression<MySQLColumn> {

    default MySQLConstant getExpectedValue() {
        throw new AssertionError("PQS not supported for this operator");
    }

}
