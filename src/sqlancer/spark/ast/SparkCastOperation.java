package sqlancer.spark.ast;

import sqlancer.spark.SparkSchema.SparkDataType;

public class SparkCastOperation implements SparkExpression {

    private final SparkExpression expression;
    private final SparkDataType type;

    public SparkCastOperation(SparkExpression expression, SparkDataType type) {
        if (expression == null) {
            throw new AssertionError();
        }
        this.expression = expression;
        this.type = type;
    }

    public SparkExpression getExpression() {
        return expression;
    }

    public SparkDataType getType() {
        return type;
    }
}