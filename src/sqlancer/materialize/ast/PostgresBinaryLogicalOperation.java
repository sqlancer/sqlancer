package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.ast.PostgresBinaryLogicalOperation.BinaryLogicalOperator;

public class PostgresBinaryLogicalOperation extends BinaryOperatorNode<PostgresExpression, BinaryLogicalOperator>
        implements PostgresExpression {

    public enum BinaryLogicalOperator implements Operator {
        AND {
            @Override
            public PostgresConstant apply(PostgresConstant left, PostgresConstant right) {
                PostgresConstant leftBool = left.cast(PostgresDataType.BOOLEAN);
                PostgresConstant rightBool = right.cast(PostgresDataType.BOOLEAN);
                if (leftBool.isNull()) {
                    if (rightBool.isNull()) {
                        return PostgresConstant.createNullConstant();
                    } else {
                        if (rightBool.asBoolean()) {
                            return PostgresConstant.createNullConstant();
                        } else {
                            return PostgresConstant.createFalse();
                        }
                    }
                } else if (!leftBool.asBoolean()) {
                    return PostgresConstant.createFalse();
                }
                assert leftBool.asBoolean();
                if (rightBool.isNull()) {
                    return PostgresConstant.createNullConstant();
                } else {
                    return PostgresConstant.createBooleanConstant(rightBool.isBoolean() && rightBool.asBoolean());
                }
            }
        },
        OR {
            @Override
            public PostgresConstant apply(PostgresConstant left, PostgresConstant right) {
                PostgresConstant leftBool = left.cast(PostgresDataType.BOOLEAN);
                PostgresConstant rightBool = right.cast(PostgresDataType.BOOLEAN);
                if (leftBool.isBoolean() && leftBool.asBoolean()) {
                    return PostgresConstant.createTrue();
                }
                if (rightBool.isBoolean() && rightBool.asBoolean()) {
                    return PostgresConstant.createTrue();
                }
                if (leftBool.isNull() || rightBool.isNull()) {
                    return PostgresConstant.createNullConstant();
                }
                return PostgresConstant.createFalse();
            }
        };

        public abstract PostgresConstant apply(PostgresConstant left, PostgresConstant right);

        public static BinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

    public PostgresBinaryLogicalOperation(PostgresExpression left, PostgresExpression right, BinaryLogicalOperator op) {
        super(left, right, op);
    }

    @Override
    public PostgresDataType getExpressionType() {
        return PostgresDataType.BOOLEAN;
    }

    @Override
    public PostgresConstant getExpectedValue() {
        PostgresConstant leftExpectedValue = getLeft().getExpectedValue();
        PostgresConstant rightExpectedValue = getRight().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        return getOp().apply(leftExpectedValue, rightExpectedValue);
    }

}
