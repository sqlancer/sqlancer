package sqlancer.mysql.ast;

public interface MySQLExpression {

    default MySQLConstant getExpectedValue() {
        throw new AssertionError("PQS not supported for this operator");
    }

}
