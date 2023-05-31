package sqlancer.materialize.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.ast.MaterializeBinaryLogicalOperation.BinaryLogicalOperator;

public class MaterializeBinaryLogicalOperation extends BinaryOperatorNode<MaterializeExpression, BinaryLogicalOperator>
        implements MaterializeExpression {

    public enum BinaryLogicalOperator implements Operator {
        AND {
            @Override
            public MaterializeConstant apply(MaterializeConstant left, MaterializeConstant right) {
                MaterializeConstant leftBool = left.cast(MaterializeDataType.BOOLEAN);
                MaterializeConstant rightBool = right.cast(MaterializeDataType.BOOLEAN);
                if (leftBool.isNull()) {
                    if (rightBool.isNull()) {
                        return MaterializeConstant.createNullConstant();
                    } else {
                        if (rightBool.asBoolean()) {
                            return MaterializeConstant.createNullConstant();
                        } else {
                            return MaterializeConstant.createFalse();
                        }
                    }
                } else if (!leftBool.asBoolean()) {
                    return MaterializeConstant.createFalse();
                }
                assert leftBool.asBoolean();
                if (rightBool.isNull()) {
                    return MaterializeConstant.createNullConstant();
                } else {
                    return MaterializeConstant.createBooleanConstant(rightBool.isBoolean() && rightBool.asBoolean());
                }
            }
        },
        OR {
            @Override
            public MaterializeConstant apply(MaterializeConstant left, MaterializeConstant right) {
                MaterializeConstant leftBool = left.cast(MaterializeDataType.BOOLEAN);
                MaterializeConstant rightBool = right.cast(MaterializeDataType.BOOLEAN);
                if (leftBool.isBoolean() && leftBool.asBoolean()) {
                    return MaterializeConstant.createTrue();
                }
                if (rightBool.isBoolean() && rightBool.asBoolean()) {
                    return MaterializeConstant.createTrue();
                }
                if (leftBool.isNull() || rightBool.isNull()) {
                    return MaterializeConstant.createNullConstant();
                }
                return MaterializeConstant.createFalse();
            }
        };

        public abstract MaterializeConstant apply(MaterializeConstant left, MaterializeConstant right);

        public static BinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

    public MaterializeBinaryLogicalOperation(MaterializeExpression left, MaterializeExpression right,
            BinaryLogicalOperator op) {
        super(left, right, op);
    }

    @Override
    public MaterializeDataType getExpressionType() {
        return MaterializeDataType.BOOLEAN;
    }

    @Override
    public MaterializeConstant getExpectedValue() {
        MaterializeConstant leftExpectedValue = getLeft().getExpectedValue();
        MaterializeConstant rightExpectedValue = getRight().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        return getOp().apply(leftExpectedValue, rightExpectedValue);
    }

}
