package sqlancer.materialize.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.ast.MaterializeBinaryComparisonOperation.MaterializeBinaryComparisonOperator;

public class MaterializeBinaryComparisonOperation
        extends BinaryOperatorNode<MaterializeExpression, MaterializeBinaryComparisonOperator>
        implements MaterializeExpression {

    public enum MaterializeBinaryComparisonOperator implements Operator {
        EQUALS("=") {
            @Override
            public MaterializeConstant getExpectedValue(MaterializeConstant leftVal, MaterializeConstant rightVal) {
                return leftVal.isEquals(rightVal);
            }
        },
        NOT_EQUALS("!=") {
            @Override
            public MaterializeConstant getExpectedValue(MaterializeConstant leftVal, MaterializeConstant rightVal) {
                MaterializeConstant isEquals = leftVal.isEquals(rightVal);
                if (isEquals.isBoolean()) {
                    return MaterializeConstant.createBooleanConstant(!isEquals.asBoolean());
                }
                return isEquals;
            }
        },
        LESS("<") {

            @Override
            public MaterializeConstant getExpectedValue(MaterializeConstant leftVal, MaterializeConstant rightVal) {
                return leftVal.isLessThan(rightVal);
            }
        },
        LESS_EQUALS("<=") {

            @Override
            public MaterializeConstant getExpectedValue(MaterializeConstant leftVal, MaterializeConstant rightVal) {
                MaterializeConstant lessThan = leftVal.isLessThan(rightVal);
                if (lessThan.isBoolean() && !lessThan.asBoolean()) {
                    return leftVal.isEquals(rightVal);
                } else {
                    return lessThan;
                }
            }
        },
        GREATER(">") {
            @Override
            public MaterializeConstant getExpectedValue(MaterializeConstant leftVal, MaterializeConstant rightVal) {
                MaterializeConstant equals = leftVal.isEquals(rightVal);
                if (equals.isBoolean() && equals.asBoolean()) {
                    return MaterializeConstant.createFalse();
                } else {
                    MaterializeConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return MaterializeConstant.createNullConstant();
                    }
                    return MaterializePrefixOperation.PrefixOperator.NOT.getExpectedValue(applyLess);
                }
            }
        },
        GREATER_EQUALS(">=") {

            @Override
            public MaterializeConstant getExpectedValue(MaterializeConstant leftVal, MaterializeConstant rightVal) {
                MaterializeConstant equals = leftVal.isEquals(rightVal);
                if (equals.isBoolean() && equals.asBoolean()) {
                    return MaterializeConstant.createTrue();
                } else {
                    MaterializeConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return MaterializeConstant.createNullConstant();
                    }
                    return MaterializePrefixOperation.PrefixOperator.NOT.getExpectedValue(applyLess);
                }
            }

        };

        private final String textRepresentation;

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

        MaterializeBinaryComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public abstract MaterializeConstant getExpectedValue(MaterializeConstant leftVal, MaterializeConstant rightVal);

        public static MaterializeBinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(MaterializeBinaryComparisonOperator.values());
        }

    }

    public MaterializeBinaryComparisonOperation(MaterializeExpression left, MaterializeExpression right,
            MaterializeBinaryComparisonOperator op) {
        super(left, right, op);
    }

    @Override
    public MaterializeConstant getExpectedValue() {
        MaterializeConstant leftExpectedValue = getLeft().getExpectedValue();
        MaterializeConstant rightExpectedValue = getRight().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        return getOp().getExpectedValue(leftExpectedValue, rightExpectedValue);
    }

    @Override
    public MaterializeDataType getExpressionType() {
        return MaterializeDataType.BOOLEAN;
    }

}
