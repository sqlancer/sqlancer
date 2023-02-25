package sqlancer.clickhouse.ast;

import sqlancer.Randomly;
import sqlancer.common.visitor.BinaryOperation;

public class ClickHouseBinaryArithmeticOperation extends ClickHouseExpression
        implements BinaryOperation<ClickHouseExpression> {

    public enum ClickHouseBinaryArithmeticOperator {
        ADD("+"), //
        MINUS("-"), //
        MULT("*"), //
        DIV("/"), //
        MODULO("%"); //

        String textRepresentation;

        ClickHouseBinaryArithmeticOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static ClickHouseBinaryArithmeticOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

    private final ClickHouseBinaryArithmeticOperation.ClickHouseBinaryArithmeticOperator operation;
    private final ClickHouseExpression left;
    private final ClickHouseExpression right;

    public ClickHouseBinaryArithmeticOperation(ClickHouseExpression left, ClickHouseExpression right,
            ClickHouseBinaryArithmeticOperation.ClickHouseBinaryArithmeticOperator operation) {
        this.left = left;
        this.right = right;
        this.operation = operation;
    }

    public ClickHouseBinaryArithmeticOperation.ClickHouseBinaryArithmeticOperator getOperator() {
        return operation;
    }

    @Override
    public ClickHouseExpression getLeft() {
        return left;
    }

    @Override
    public ClickHouseExpression getRight() {
        return right;
    }

    @Override
    public String getOperatorRepresentation() {
        return operation.getTextRepresentation();
    }

    public static ClickHouseBinaryArithmeticOperation create(ClickHouseExpression left, ClickHouseExpression right,
            ClickHouseBinaryArithmeticOperation.ClickHouseBinaryArithmeticOperator op) {
        return new ClickHouseBinaryArithmeticOperation(left, right, op);
    }
}
