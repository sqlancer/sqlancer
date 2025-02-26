package sqlancer.sqlite3.ast;

import java.util.Optional;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.visitor.BinaryOperation;
import sqlancer.sqlite3.SQLite3Provider;
import sqlancer.sqlite3.schema.SQLite3DataType;
import sqlancer.sqlite3.schema.SQLite3Schema;

public class SQLite3BinaryOperation implements SQLite3Expression, BinaryOperation<SQLite3Expression> {

    public enum BinaryOperator {
        CONCATENATE("||") {
            @Override
            public SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
                    return null;
                }
                if (!SQLite3Provider.allowFloatingPointFp && (left.getDataType() == SQLite3DataType.REAL
                        || right.getDataType() == SQLite3DataType.REAL)) {
                    throw new IgnoreMeException();
                }
                if (left.getExpectedValue().isNull() || right.getExpectedValue().isNull()) {
                    return SQLite3Constant.createNullConstant();
                }
                SQLite3Constant leftText = SQLite3Cast.castToText(left);
                SQLite3Constant rightText = SQLite3Cast.castToText(right);
                if (leftText == null || rightText == null) {
                    return null;
                }
                return SQLite3Constant.createTextConstant(leftText.asString() + rightText.asString());
            }
        },
        MULTIPLY("*") {
            @Override
            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                return null;
            }

        },
        DIVIDE("/") {

            @Override
            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                return null;
            }

        }, // division by zero results in zero
        REMAINDER("%") {
            @Override
            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                return null;
            }

        },

        PLUS("+") {

            @Override
            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                return null;
            }
        },

        MINUS("-") {

            @Override
            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                return null;
            }

        },
        SHIFT_LEFT("<<") {

            @Override
            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                return applyIntOperation(left, right, (leftResult, rightResult) -> {
                    if (rightResult >= 0) {
                        if (rightResult >= Long.SIZE) {
                            return 0L;
                        }
                        return leftResult << rightResult;
                    } else {
                        if (rightResult == Long.MIN_VALUE) {
                            return leftResult >= 0 ? 0L : -1L;
                        }
                        return SHIFT_RIGHT.apply(left, SQLite3Constant.createIntConstant(-rightResult)).asInt();
                    }

                });
            }

        },
        SHIFT_RIGHT(">>") {

            @Override
            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                return applyIntOperation(left, right, (leftResult, rightResult) -> {
                    if (rightResult >= 0) {
                        if (rightResult >= Long.SIZE) {
                            return leftResult >= 0 ? 0L : -1L;
                        }
                        return leftResult >> rightResult;
                    } else {
                        if (rightResult == Long.MIN_VALUE) {
                            return 0L;
                        }
                        return SHIFT_LEFT.apply(left, SQLite3Constant.createIntConstant(-rightResult)).asInt();
                    }

                });
            }

        },
        ARITHMETIC_AND("&") {

            @Override
            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                return applyIntOperation(left, right, (a, b) -> a & b);
            }

        },
        ARITHMETIC_OR("|") {

            @Override
            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                return applyIntOperation(left, right, (a, b) -> a | b);
            }

        },
        AND("AND") {

            @Override
            public SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {

                if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
                    return null;
                } else {
                    Optional<Boolean> leftBoolVal = SQLite3Cast.isTrue(left.getExpectedValue());
                    Optional<Boolean> rightBoolVal = SQLite3Cast.isTrue(right.getExpectedValue());
                    if (leftBoolVal.isPresent() && !leftBoolVal.get()) {
                        return SQLite3Constant.createFalse();
                    } else if (rightBoolVal.isPresent() && !rightBoolVal.get()) {
                        return SQLite3Constant.createFalse();
                    } else if (!rightBoolVal.isPresent() || !leftBoolVal.isPresent()) {
                        return SQLite3Constant.createNullConstant();
                    } else {
                        return SQLite3Constant.createTrue();
                    }
                }
            }

        },
        OR("OR") {

            @Override
            public SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
                if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
                    return null;
                } else {
                    Optional<Boolean> leftBoolVal = SQLite3Cast.isTrue(left.getExpectedValue());
                    Optional<Boolean> rightBoolVal = SQLite3Cast.isTrue(right.getExpectedValue());
                    if (leftBoolVal.isPresent() && leftBoolVal.get()) {
                        return SQLite3Constant.createTrue();
                    } else if (rightBoolVal.isPresent() && rightBoolVal.get()) {
                        return SQLite3Constant.createTrue();
                    } else if (!rightBoolVal.isPresent() || !leftBoolVal.isPresent()) {
                        return SQLite3Constant.createNullConstant();
                    } else {
                        return SQLite3Constant.createFalse();
                    }
                }
            }
        };

        private final String[] textRepresentation;

        BinaryOperator(String... textRepresentation) {
            this.textRepresentation = textRepresentation.clone();
        }

        public static BinaryOperator getRandomOperator() {
            return Randomly.fromOptions(values());
        }

        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentation);
        }

        public SQLite3Constant applyOperand(SQLite3Constant left, SQLite3TypeAffinity leftAffinity,
                SQLite3Constant right, SQLite3TypeAffinity rightAffinity) {
            return apply(left, right);
        }

        public SQLite3Constant applyIntOperation(SQLite3Constant left, SQLite3Constant right,
                java.util.function.BinaryOperator<Long> func) {
            if (left.isNull() || right.isNull()) {
                return SQLite3Constant.createNullConstant();
            }
            SQLite3Constant leftInt = SQLite3Cast.castToInt(left);
            SQLite3Constant rightInt = SQLite3Cast.castToInt(right);
            long result = func.apply(leftInt.asInt(), rightInt.asInt());
            return SQLite3Constant.createIntConstant(result);
        }

        SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
            return null;
        }

    }

    private final SQLite3BinaryOperation.BinaryOperator operation;
    private final SQLite3Expression left;
    private final SQLite3Expression right;

    @Override
    public SQLite3Schema.SQLite3Column.SQLite3CollateSequence getExplicitCollateSequence() {
        if (left.getExplicitCollateSequence() != null) {
            return left.getExplicitCollateSequence();
        } else {
            return right.getExplicitCollateSequence();
        }
    }

    public SQLite3BinaryOperation(SQLite3Expression left, SQLite3Expression right, BinaryOperator operation) {
        this.left = left;
        this.right = right;
        this.operation = operation;
    }

    public BinaryOperator getOperator() {
        return operation;
    }

    @Override
    public SQLite3Expression getLeft() {
        return left;
    }

    @Override
    public SQLite3Expression getRight() {
        return right;
    }

    @Override
    public SQLite3Constant getExpectedValue() {
        if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
            return null;
        }
        SQLite3Constant result = operation.applyOperand(left.getExpectedValue(), left.getAffinity(),
                right.getExpectedValue(), right.getAffinity());
        if (result != null && result.isReal()) {
            SQLite3Cast.checkDoubleIsInsideDangerousRange(result.asDouble());
        }
        return result;
    }

    public static SQLite3BinaryOperation create(SQLite3Expression leftVal, SQLite3Expression rightVal,
            SQLite3BinaryOperation.BinaryOperator op) {
        return new SQLite3BinaryOperation(leftVal, rightVal, op);
    }

    @Override
    public String getOperatorRepresentation() {
        return Randomly.fromOptions(operation.textRepresentation);
    }

}
