package sqlancer.postgres.ast;

import java.util.function.BinaryOperator;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.ast.PostgresBinaryArithmeticOperation.PostgresBinaryOperator;

public class PostgresBinaryArithmeticOperation extends BinaryOperatorNode<PostgresExpression, PostgresBinaryOperator>
        implements PostgresExpression {

    public enum PostgresBinaryOperator implements Operator {

        ADDITION("+") {
            @Override
            public PostgresConstant apply(PostgresConstant left, PostgresConstant right) {
                if (left.isReal() && right.isReal()) {
                    return applyRealOperation(left, right, (l, r) -> l + r);
                } else if (left.isFloat() && right.isFloat()) {
                    return applyFloatOperation(left, right, (l, r) -> l + r);
                } else {
                    return applyIntOperation(left, right, (l, r) -> l + r);
                }
            }

        },
        SUBTRACTION("-") {
            @Override
            public PostgresConstant apply(PostgresConstant left, PostgresConstant right) {
                if (left.isReal() && right.isReal()) {
                    return applyRealOperation(left, right, (l, r) -> l - r);
                } else if (left.isFloat() && right.isFloat()) {
                    return applyFloatOperation(left, right, (l, r) -> l - r);
                } else {
                    return applyIntOperation(left, right, (l, r) -> l - r);
                }
            }
        },
        MULTIPLICATION("*") {
            @Override
            public PostgresConstant apply(PostgresConstant left, PostgresConstant right) {
                if (left.isReal() && right.isReal()) {
                    return applyRealOperation(left, right, (l, r) -> l * r);
                } else if (left.isFloat() && right.isFloat()) {
                    return applyFloatOperation(left, right, (l, r) -> l * r);
                } else {
                    return applyIntOperation(left, right, (l, r) -> l * r);
                }
            }
        },
        DIVISION("/") {

            @Override
            public PostgresConstant apply(PostgresConstant left, PostgresConstant right) {
                if (left.isReal() && right.isReal()) {
                    return applyRealOperation(left, right, (l, r) -> r == 0 ? -1 : l / r);
                } else if (left.isFloat() && right.isFloat()) {
                    return applyFloatOperation(left, right, (l, r) -> r == 0 ? -1 : l / r);
                } else {
                    return applyIntOperation(left, right, (l, r) -> r == 0 ? -1 : l / r);
                }
            }

        },
        MODULO("%") {
            @Override
            public PostgresConstant apply(PostgresConstant left, PostgresConstant right) {
                return applyIntOperation(left, right, (l, r) -> r == 0 ? -1 : l % r);

            }
        },
        EXPONENTIATION("^") {
            @Override
            public PostgresConstant apply(PostgresConstant left, PostgresConstant right) {
                return null;
            }
        };

        private String textRepresentation;

        private static PostgresConstant applyIntOperation(PostgresConstant left, PostgresConstant right,
                BinaryOperator<Long> op) {
            if (left.isNull() || right.isNull()) {
                return PostgresConstant.createNullConstant();
            } else {
                long leftVal = left.cast(PostgresDataType.INT).asInt();
                long rightVal = right.cast(PostgresDataType.INT).asInt();
                long value = op.apply(leftVal, rightVal);
                return PostgresConstant.createIntConstant(value);
            }
        }

        private static PostgresConstant applyFloatOperation(PostgresConstant left, PostgresConstant right,
                BinaryOperator<Float> op) {
            if (left.isNull() || right.isNull()) {
                return PostgresConstant.createNullConstant();
            } else {
                float leftVal = left.cast(PostgresDataType.FLOAT).asFloat();
                float rightVal = right.cast(PostgresDataType.FLOAT).asFloat();
                float value = op.apply(leftVal, rightVal);
                return PostgresConstant.createFloatConstant(value);
            }
        }

        private static PostgresConstant applyRealOperation(PostgresConstant left, PostgresConstant right,
                BinaryOperator<Double> op) {
            if (left.isNull() || right.isNull()) {
                return PostgresConstant.createNullConstant();
            } else {
                double leftVal = left.cast(PostgresDataType.REAL).asReal();
                double rightVal = right.cast(PostgresDataType.REAL).asReal();
                double value = op.apply(leftVal, rightVal);
                return PostgresConstant.createDoubleConstant(value);
            }
        }

        PostgresBinaryOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract PostgresConstant apply(PostgresConstant left, PostgresConstant right);

        public static PostgresBinaryOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public PostgresBinaryArithmeticOperation(PostgresExpression left, PostgresExpression right,
            PostgresBinaryOperator op) {
        super(left, right, op);
    }

    @Override
    public PostgresConstant getExpectedValue() {
        PostgresConstant leftExpected = getLeft().getExpectedValue();
        PostgresConstant rightExpected = getRight().getExpectedValue();
        if (leftExpected == null || rightExpected == null) {
            return null;
        }
        return getOp().apply(leftExpected, rightExpected);
    }

    @Override
    public PostgresDataType getExpressionType() {
        return PostgresDataType.INT;
    }

}
