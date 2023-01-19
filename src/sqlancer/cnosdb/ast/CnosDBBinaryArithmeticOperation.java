package sqlancer.cnosdb.ast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BinaryOperator;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.ast.CnosDBBinaryArithmeticOperation.CnosDBBinaryOperator;
import sqlancer.common.ast.BinaryOperatorNode;

public class CnosDBBinaryArithmeticOperation extends BinaryOperatorNode<CnosDBExpression, CnosDBBinaryOperator>
        implements CnosDBExpression {

    public CnosDBBinaryArithmeticOperation(CnosDBExpression left, CnosDBExpression right, CnosDBBinaryOperator op) {
        super(left, right, op);
    }

    @Override
    public CnosDBConstant getExpectedValue() {
        CnosDBConstant leftExpected = getLeft().getExpectedValue();
        CnosDBConstant rightExpected = getRight().getExpectedValue();
        if (leftExpected == null || rightExpected == null) {
            return null;
        }
        return getOp().apply(leftExpected, rightExpected);
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return CnosDBDataType.INT;
    }

    public enum CnosDBBinaryOperator implements BinaryOperatorNode.Operator {

        ADDITION("+") {
            @Override
            public CnosDBConstant apply(CnosDBConstant left, CnosDBConstant right) {
                return applyBitOperation(left, right, (l, r) -> l + r);
            }

        },
        SUBTRACTION("-") {
            @Override
            public CnosDBConstant apply(CnosDBConstant left, CnosDBConstant right) {
                return applyBitOperation(left, right, (l, r) -> l - r);
            }
        },
        MULTIPLICATION("*") {
            @Override
            public CnosDBConstant apply(CnosDBConstant left, CnosDBConstant right) {
                return applyBitOperation(left, right, (l, r) -> l * r);
            }
        },
        DIVISION("/") {
            @Override
            public CnosDBConstant apply(CnosDBConstant left, CnosDBConstant right) {
                return applyBitOperation(left, right, (l, r) -> r == 0 ? -1 : l / r);

            }

        },
        MODULO("%") {
            @Override
            public CnosDBConstant apply(CnosDBConstant left, CnosDBConstant right) {
                return applyBitOperation(left, right, (l, r) -> r == 0 ? -1 : l % r);

            }
        },
        EXPONENTIATION("^") {
            @Override
            public CnosDBConstant apply(CnosDBConstant left, CnosDBConstant right) {
                return null;
            }
        };

        private final String textRepresentation;

        CnosDBBinaryOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        private static CnosDBConstant applyBitOperation(CnosDBConstant left, CnosDBConstant right,
                BinaryOperator<Long> op) {
            if (left.isNull() || right.isNull()) {
                return CnosDBConstant.createNullConstant();
            } else {
                long leftVal = left.cast(CnosDBDataType.INT).asInt();
                long rightVal = right.cast(CnosDBDataType.INT).asInt();
                long value = op.apply(leftVal, rightVal);
                return CnosDBConstant.createIntConstant(value);
            }
        }

        public static CnosDBBinaryOperator getRandom(CnosDBDataType dataType) {
            List<CnosDBBinaryOperator> ops = new ArrayList<>(Arrays.asList(values()));
            switch (dataType) {
            case DOUBLE:
            case UINT:
            case STRING:
                ops.remove(MODULO);
                ops.remove(EXPONENTIATION);
                ops.remove(EXPONENTIATION);
                ops.remove(MODULO);
                break;
            default:
                break;
            }

            return Randomly.fromList(ops);
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract CnosDBConstant apply(CnosDBConstant left, CnosDBConstant right);

    }

}
