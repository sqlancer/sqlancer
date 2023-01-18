package sqlancer.cnosdb.ast;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.ast.CnosDBBinaryLogicalOperation.BinaryLogicalOperator;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;

public class CnosDBBinaryLogicalOperation extends BinaryOperatorNode<CnosDBExpression, BinaryLogicalOperator>
        implements CnosDBExpression {

    public enum BinaryLogicalOperator implements Operator {
        AND {
            @Override
            public CnosDBConstant apply(CnosDBConstant left, CnosDBConstant right) {
                CnosDBConstant leftBool = left.cast(CnosDBDataType.BOOLEAN);
                CnosDBConstant rightBool = right.cast(CnosDBDataType.BOOLEAN);
                if (leftBool.isNull()) {
                    if (rightBool.isNull()) {
                        return CnosDBConstant.createNullConstant();
                    } else {
                        if (rightBool.asBoolean()) {
                            return CnosDBConstant.createNullConstant();
                        } else {
                            return CnosDBConstant.createFalse();
                        }
                    }
                } else if (!leftBool.asBoolean()) {
                    return CnosDBConstant.createFalse();
                }
                assert leftBool.asBoolean();
                if (rightBool.isNull()) {
                    return CnosDBConstant.createNullConstant();
                } else {
                    return CnosDBConstant.createBooleanConstant(rightBool.isBoolean() && rightBool.asBoolean());
                }
            }
        },
        OR {
            @Override
            public CnosDBConstant apply(CnosDBConstant left, CnosDBConstant right) {
                CnosDBConstant leftBool = left.cast(CnosDBDataType.BOOLEAN);
                CnosDBConstant rightBool = right.cast(CnosDBDataType.BOOLEAN);
                if (leftBool.isBoolean() && leftBool.asBoolean()) {
                    return CnosDBConstant.createTrue();
                }
                if (rightBool.isBoolean() && rightBool.asBoolean()) {
                    return CnosDBConstant.createTrue();
                }
                if (leftBool.isNull() || rightBool.isNull()) {
                    return CnosDBConstant.createNullConstant();
                }
                return CnosDBConstant.createFalse();
            }
        };

        public abstract CnosDBConstant apply(CnosDBConstant left, CnosDBConstant right);

        public static BinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

    public CnosDBBinaryLogicalOperation(CnosDBExpression left, CnosDBExpression right, BinaryLogicalOperator op) {
        super(left, right, op);
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return CnosDBDataType.BOOLEAN;
    }

    @Override
    public CnosDBConstant getExpectedValue() {
        CnosDBConstant leftExpectedValue = getLeft().getExpectedValue();
        CnosDBConstant rightExpectedValue = getRight().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        return getOp().apply(leftExpectedValue, rightExpectedValue);
    }

}
