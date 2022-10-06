package sqlancer.yugabyte.ysql.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.ast.YSQLBinaryComparisonOperation.YSQLBinaryComparisonOperator;

public class YSQLBinaryComparisonOperation extends BinaryOperatorNode<YSQLExpression, YSQLBinaryComparisonOperator>
        implements YSQLExpression {

    public YSQLBinaryComparisonOperation(YSQLExpression left, YSQLExpression right, YSQLBinaryComparisonOperator op) {
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
        return getOp().getExpectedValue(leftExpectedValue, rightExpectedValue);
    }

    public enum YSQLBinaryComparisonOperator implements Operator {
        EQUALS("=") {
            @Override
            public YSQLConstant getExpectedValue(YSQLConstant leftVal, YSQLConstant rightVal) {
                return leftVal.isEquals(rightVal);
            }
        },
        IS_DISTINCT("IS DISTINCT FROM") {
            @Override
            public YSQLConstant getExpectedValue(YSQLConstant leftVal, YSQLConstant rightVal) {
                return YSQLConstant
                        .createBooleanConstant(!IS_NOT_DISTINCT.getExpectedValue(leftVal, rightVal).asBoolean());
            }
        },
        IS_NOT_DISTINCT("IS NOT DISTINCT FROM") {
            @Override
            public YSQLConstant getExpectedValue(YSQLConstant leftVal, YSQLConstant rightVal) {
                if (leftVal.isNull()) {
                    return YSQLConstant.createBooleanConstant(rightVal.isNull());
                } else if (rightVal.isNull()) {
                    return YSQLConstant.createFalse();
                } else {
                    return leftVal.isEquals(rightVal);
                }
            }
        },
        NOT_EQUALS("!=") {
            @Override
            public YSQLConstant getExpectedValue(YSQLConstant leftVal, YSQLConstant rightVal) {
                YSQLConstant isEquals = leftVal.isEquals(rightVal);
                if (isEquals.isBoolean()) {
                    return YSQLConstant.createBooleanConstant(!isEquals.asBoolean());
                }
                return isEquals;
            }
        },
        LESS("<") {
            @Override
            public YSQLConstant getExpectedValue(YSQLConstant leftVal, YSQLConstant rightVal) {
                return leftVal.isLessThan(rightVal);
            }
        },
        LESS_EQUALS("<=") {
            @Override
            public YSQLConstant getExpectedValue(YSQLConstant leftVal, YSQLConstant rightVal) {
                YSQLConstant lessThan = leftVal.isLessThan(rightVal);
                if (lessThan.isBoolean() && !lessThan.asBoolean()) {
                    return leftVal.isEquals(rightVal);
                } else {
                    return lessThan;
                }
            }
        },
        GREATER(">") {
            @Override
            public YSQLConstant getExpectedValue(YSQLConstant leftVal, YSQLConstant rightVal) {
                YSQLConstant equals = leftVal.isEquals(rightVal);
                if (equals.isBoolean() && equals.asBoolean()) {
                    return YSQLConstant.createFalse();
                } else {
                    YSQLConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return YSQLConstant.createNullConstant();
                    }
                    return YSQLPrefixOperation.PrefixOperator.NOT.getExpectedValue(applyLess);
                }
            }
        },
        GREATER_EQUALS(">=") {
            @Override
            public YSQLConstant getExpectedValue(YSQLConstant leftVal, YSQLConstant rightVal) {
                YSQLConstant equals = leftVal.isEquals(rightVal);
                if (equals.isBoolean() && equals.asBoolean()) {
                    return YSQLConstant.createTrue();
                } else {
                    YSQLConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return YSQLConstant.createNullConstant();
                    }
                    return YSQLPrefixOperation.PrefixOperator.NOT.getExpectedValue(applyLess);
                }
            }

        };

        private final String textRepresentation;

        YSQLBinaryComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static YSQLBinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(YSQLBinaryComparisonOperator.values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract YSQLConstant getExpectedValue(YSQLConstant leftVal, YSQLConstant rightVal);

    }

}
