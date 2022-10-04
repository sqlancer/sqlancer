package sqlancer.yugabyte.ysql.ast;

import java.util.function.BinaryOperator;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.ast.YSQLBinaryArithmeticOperation.YSQLBinaryOperator;

public class YSQLBinaryArithmeticOperation extends BinaryOperatorNode<YSQLExpression, YSQLBinaryOperator>
        implements YSQLExpression {

    public YSQLBinaryArithmeticOperation(YSQLExpression left, YSQLExpression right, YSQLBinaryOperator op) {
        super(left, right, op);
    }

    @Override
    public YSQLDataType getExpressionType() {
        return YSQLDataType.INT;
    }

    @Override
    public YSQLConstant getExpectedValue() {
        YSQLConstant leftExpected = getLeft().getExpectedValue();
        YSQLConstant rightExpected = getRight().getExpectedValue();
        if (leftExpected == null || rightExpected == null) {
            return null;
        }
        return getOp().apply(leftExpected, rightExpected);
    }

    public enum YSQLBinaryOperator implements Operator {

        ADDITION("+") {
            @Override
            public YSQLConstant apply(YSQLConstant left, YSQLConstant right) {
                return applyBitOperation(left, right, Long::sum);
            }

        },
        SUBTRACTION("-") {
            @Override
            public YSQLConstant apply(YSQLConstant left, YSQLConstant right) {
                return applyBitOperation(left, right, (l, r) -> l - r);
            }
        },
        MULTIPLICATION("*") {
            @Override
            public YSQLConstant apply(YSQLConstant left, YSQLConstant right) {
                return applyBitOperation(left, right, (l, r) -> l * r);
            }
        },
        DIVISION("/") {
            @Override
            public YSQLConstant apply(YSQLConstant left, YSQLConstant right) {
                return applyBitOperation(left, right, (l, r) -> r == 0 ? -1 : l / r);

            }

        },
        MODULO("%") {
            @Override
            public YSQLConstant apply(YSQLConstant left, YSQLConstant right) {
                return applyBitOperation(left, right, (l, r) -> r == 0 ? -1 : l % r);

            }
        },
        EXPONENTIATION("^") {
            @Override
            public YSQLConstant apply(YSQLConstant left, YSQLConstant right) {
                return null;
            }
        };

        private final String textRepresentation;

        YSQLBinaryOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        private static YSQLConstant applyBitOperation(YSQLConstant left, YSQLConstant right, BinaryOperator<Long> op) {
            if (left.isNull() || right.isNull()) {
                return YSQLConstant.createNullConstant();
            } else {
                long leftVal = left.cast(YSQLDataType.INT).asInt();
                long rightVal = right.cast(YSQLDataType.INT).asInt();
                long value = op.apply(leftVal, rightVal);
                return YSQLConstant.createIntConstant(value);
            }
        }

        public static YSQLBinaryOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract YSQLConstant apply(YSQLConstant left, YSQLConstant right);

    }

}
