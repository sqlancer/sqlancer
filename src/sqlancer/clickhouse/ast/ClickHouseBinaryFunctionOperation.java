package sqlancer.clickhouse.ast;

import sqlancer.Randomly;

public class ClickHouseBinaryFunctionOperation extends ClickHouseExpression {

    public enum ClickHouseBinaryFunctionOperator {
        INT_DIV("intDiv"), GCD("gcd"), LCM("lcm"), MAX2("max2"), MIN2("min2"), POW("pow");

        String textRepresentation;

        ClickHouseBinaryFunctionOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static ClickHouseBinaryFunctionOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

    private final ClickHouseBinaryFunctionOperator operation;
    private final ClickHouseExpression left;
    private final ClickHouseExpression right;

    public ClickHouseBinaryFunctionOperation(ClickHouseExpression left, ClickHouseExpression right,
            ClickHouseBinaryFunctionOperator operation) {
        this.left = left;
        this.right = right;
        this.operation = operation;
    }

    public ClickHouseBinaryFunctionOperator getOperator() {
        return operation;
    }

    public ClickHouseExpression getLeft() {
        return left;
    }

    public ClickHouseExpression getRight() {
        return right;
    }

    public String getOperatorRepresentation() {
        return operation.getTextRepresentation();
    }

    public static ClickHouseBinaryFunctionOperation create(ClickHouseExpression left, ClickHouseExpression right,
            ClickHouseBinaryFunctionOperator op) {
        return new ClickHouseBinaryFunctionOperation(left, right, op);
    }

}
