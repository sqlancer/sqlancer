package sqlancer.materialize.ast;

import java.util.function.BinaryOperator;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.ast.MaterializeBinaryArithmeticOperation.MaterializeBinaryOperator;

public class MaterializeBinaryArithmeticOperation
        extends BinaryOperatorNode<MaterializeExpression, MaterializeBinaryOperator> implements MaterializeExpression {

    public enum MaterializeBinaryOperator implements Operator {

        ADDITION("+") {
            @Override
            public MaterializeConstant apply(MaterializeConstant left, MaterializeConstant right) {
                return applyBitOperation(left, right, (l, r) -> l + r);
            }

        },
        SUBTRACTION("-") {
            @Override
            public MaterializeConstant apply(MaterializeConstant left, MaterializeConstant right) {
                return applyBitOperation(left, right, (l, r) -> l - r);
            }
        },
        MULTIPLICATION("*") {
            @Override
            public MaterializeConstant apply(MaterializeConstant left, MaterializeConstant right) {
                return applyBitOperation(left, right, (l, r) -> l * r);
            }
        },
        DIVISION("/") {

            @Override
            public MaterializeConstant apply(MaterializeConstant left, MaterializeConstant right) {
                return applyBitOperation(left, right, (l, r) -> r == 0 ? -1 : l / r);

            }

        },
        MODULO("%") {
            @Override
            public MaterializeConstant apply(MaterializeConstant left, MaterializeConstant right) {
                return applyBitOperation(left, right, (l, r) -> r == 0 ? -1 : l % r);

            }
        };

        private String textRepresentation;

        private static MaterializeConstant applyBitOperation(MaterializeConstant left, MaterializeConstant right,
                BinaryOperator<Long> op) {
            if (left.isNull() || right.isNull()) {
                return MaterializeConstant.createNullConstant();
            } else {
                long leftVal = left.cast(MaterializeDataType.INT).asInt();
                long rightVal = right.cast(MaterializeDataType.INT).asInt();
                long value = op.apply(leftVal, rightVal);
                return MaterializeConstant.createIntConstant(value);
            }
        }

        MaterializeBinaryOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract MaterializeConstant apply(MaterializeConstant left, MaterializeConstant right);

        public static MaterializeBinaryOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public MaterializeBinaryArithmeticOperation(MaterializeExpression left, MaterializeExpression right,
            MaterializeBinaryOperator op) {
        super(left, right, op);
    }

    @Override
    public MaterializeConstant getExpectedValue() {
        MaterializeConstant leftExpected = getLeft().getExpectedValue();
        MaterializeConstant rightExpected = getRight().getExpectedValue();
        if (leftExpected == null || rightExpected == null) {
            return null;
        }
        return getOp().apply(leftExpected, rightExpected);
    }

    @Override
    public MaterializeDataType getExpressionType() {
        return MaterializeDataType.INT;
    }

}
