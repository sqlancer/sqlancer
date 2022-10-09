package sqlancer.yugabyte.ysql.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.ast.YSQLBinaryLogicalOperation.BinaryLogicalOperator;

public class YSQLBinaryLogicalOperation extends BinaryOperatorNode<YSQLExpression, BinaryLogicalOperator>
        implements YSQLExpression {

    public YSQLBinaryLogicalOperation(YSQLExpression left, YSQLExpression right, BinaryLogicalOperator op) {
        super(left, right, op);
    }

    @Override
    public YSQLDataType getExpressionType() {
        return YSQLDataType.BOOLEAN;
    }

    @Override
    public YSQLConstant getExpectedValue() {
        YSQLConstant leftExpectedValue = getLeft().getExpectedValue();
        YSQLConstant rightExpectedValue = getRight().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        return getOp().apply(leftExpectedValue, rightExpectedValue);
    }

    public enum BinaryLogicalOperator implements Operator {
        AND {
            @Override
            public YSQLConstant apply(YSQLConstant left, YSQLConstant right) {
                YSQLConstant leftBool = left.cast(YSQLDataType.BOOLEAN);
                YSQLConstant rightBool = right.cast(YSQLDataType.BOOLEAN);
                if (leftBool.isNull()) {
                    if (rightBool.isNull()) {
                        return YSQLConstant.createNullConstant();
                    } else {
                        if (rightBool.asBoolean()) {
                            return YSQLConstant.createNullConstant();
                        } else {
                            return YSQLConstant.createFalse();
                        }
                    }
                } else if (!leftBool.asBoolean()) {
                    return YSQLConstant.createFalse();
                }
                assert leftBool.asBoolean();
                if (rightBool.isNull()) {
                    return YSQLConstant.createNullConstant();
                } else {
                    return YSQLConstant.createBooleanConstant(rightBool.isBoolean() && rightBool.asBoolean());
                }
            }
        },
        OR {
            @Override
            public YSQLConstant apply(YSQLConstant left, YSQLConstant right) {
                YSQLConstant leftBool = left.cast(YSQLDataType.BOOLEAN);
                YSQLConstant rightBool = right.cast(YSQLDataType.BOOLEAN);
                if (leftBool.isBoolean() && leftBool.asBoolean()) {
                    return YSQLConstant.createTrue();
                }
                if (rightBool.isBoolean() && rightBool.asBoolean()) {
                    return YSQLConstant.createTrue();
                }
                if (leftBool.isNull() || rightBool.isNull()) {
                    return YSQLConstant.createNullConstant();
                }
                return YSQLConstant.createFalse();
            }
        };

        public static BinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        public abstract YSQLConstant apply(YSQLConstant left, YSQLConstant right);

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

}
