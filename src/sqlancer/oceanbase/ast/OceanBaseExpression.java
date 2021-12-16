package sqlancer.oceanbase.ast;

public interface OceanBaseExpression {

    default OceanBaseConstant getExpectedValue() {
        throw new AssertionError("PQS not supported for this operator");
    }

}
