package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.ast.BinaryOperatorNode;
import sqlancer.ast.BinaryOperatorNode.Operator;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.ast.PostgresBinaryLogicalOperation.BinaryLogicalOperator;

public class PostgresBinaryLogicalOperation extends BinaryOperatorNode<PostgresExpression, BinaryLogicalOperator>
        implements PostgresExpression {

    public enum BinaryLogicalOperator implements Operator {
        AND {
            @Override
            public PostgresConstant apply(PostgresConstant left, PostgresConstant right) {
                left = left.cast(PostgresDataType.BOOLEAN);
                right = right.cast(PostgresDataType.BOOLEAN);
                if (left.isNull()) {
                    if (right.isNull()) {
                        return PostgresConstant.createNullConstant();
                    } else {
                        if (right.asBoolean()) {
                            return PostgresConstant.createNullConstant();
                        } else {
                            return PostgresConstant.createFalse();
                        }
                    }
                } else if (!left.asBoolean()) {
                    return PostgresConstant.createFalse();
                }
                assert left.asBoolean();
                if (right.isNull()) {
                    return PostgresConstant.createNullConstant();
                } else {
                    return PostgresConstant.createBooleanConstant(right.isBoolean() && right.asBoolean());
                }
            }
        },
        OR {
            @Override
            public PostgresConstant apply(PostgresConstant left, PostgresConstant right) {
                left = left.cast(PostgresDataType.BOOLEAN);
                right = right.cast(PostgresDataType.BOOLEAN);
                if (left.isBoolean() && left.asBoolean()) {
                    return PostgresConstant.createTrue();
                }
                if (right.isBoolean() && right.asBoolean()) {
                    return PostgresConstant.createTrue();
                }
                if (left.isNull() || right.isNull()) {
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
        return getOp().apply(getLeft().getExpectedValue(), getRight().getExpectedValue());
    }

}
