package sqlancer.sqlite3.ast;

import static sqlancer.sqlite3.ast.SQLite3AffinityHelper.applyAffinities;

import sqlancer.LikeImplementationHelper;
import sqlancer.Randomly;
import sqlancer.common.visitor.BinaryOperation;
import sqlancer.sqlite3.ast.SQLite3AffinityHelper.ConstantTuple;
import sqlancer.sqlite3.schema.SQLite3DataType;
import sqlancer.sqlite3.schema.SQLite3Schema;

public class SQLite3BinaryComparisonOperation implements SQLite3Expression, BinaryOperation<SQLite3Expression> {

    private final BinaryComparisonOperator operation;
    private final SQLite3Expression left;
    private final SQLite3Expression right;

    public SQLite3BinaryComparisonOperation(SQLite3Expression left, SQLite3Expression right,
            BinaryComparisonOperator operation) {
        this.left = left;
        this.right = right;
        this.operation = operation;
    }

    public BinaryComparisonOperator getOperator() {
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
        SQLite3Constant leftExpected = left.getExpectedValue();
        SQLite3Constant rightExpected = right.getExpectedValue();
        if (leftExpected == null || rightExpected == null) {
            return null;
        }
        SQLite3TypeAffinity leftAffinity = left.getAffinity();
        SQLite3TypeAffinity rightAffinity = right.getAffinity();
        return operation.applyOperand(leftExpected, leftAffinity, rightExpected, rightAffinity, left, right,
                operation.shouldApplyAffinity());
    }

    public static SQLite3BinaryComparisonOperation create(SQLite3Expression leftVal, SQLite3Expression rightVal,
            BinaryComparisonOperator op) {
        return new SQLite3BinaryComparisonOperation(leftVal, rightVal, op);
    }

    public enum BinaryComparisonOperator {
        SMALLER("<") {
            @Override
            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right,
                    SQLite3Schema.SQLite3Column.SQLite3CollateSequence collate) {
                return left.applyLess(right, collate);
            }

        },
        SMALLER_EQUALS("<=") {

            @Override
            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right,
                    SQLite3Schema.SQLite3Column.SQLite3CollateSequence collate) {
                SQLite3Constant lessThan = left.applyLess(right, collate);
                if (lessThan == null) {
                    return null;
                }
                if (lessThan.getDataType() == SQLite3DataType.INT && lessThan.asInt() == 0) {
                    return left.applyEquals(right, collate);
                } else {
                    return lessThan;
                }
            }

        },
        GREATER(">") {
            @Override
            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right,
                    SQLite3Schema.SQLite3Column.SQLite3CollateSequence collate) {
                SQLite3Constant equals = left.applyEquals(right, collate);
                if (equals == null) {
                    return null;
                }
                if (equals.getDataType() == SQLite3DataType.INT && equals.asInt() == 1) {
                    return SQLite3Constant.createFalse();
                } else {
                    SQLite3Constant applyLess = left.applyLess(right, collate);
                    if (applyLess == null) {
                        return null;
                    }
                    return SQLite3UnaryOperation.UnaryOperator.NOT.apply(applyLess);
                }
            }

        },
        GREATER_EQUALS(">=") {

            @Override
            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right,
                    SQLite3Schema.SQLite3Column.SQLite3CollateSequence collate) {
                SQLite3Constant equals = left.applyEquals(right, collate);
                if (equals == null) {
                    return null;
                }
                if (equals.getDataType() == SQLite3DataType.INT && equals.asInt() == 1) {
                    return SQLite3Constant.createTrue();
                } else {
                    SQLite3Constant applyLess = left.applyLess(right, collate);
                    if (applyLess == null) {
                        return null;
                    }
                    return SQLite3UnaryOperation.UnaryOperator.NOT.apply(applyLess);
                }
            }

        },
        EQUALS("=", "==") {
            @Override
            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right,
                    SQLite3Schema.SQLite3Column.SQLite3CollateSequence collate) {
                return left.applyEquals(right, collate);
            }

        },
        NOT_EQUALS("!=", "<>") {
            @Override
            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right,
                    SQLite3Schema.SQLite3Column.SQLite3CollateSequence collate) {
                if (left == null || right == null) {
                    return null;
                }
                if (left.isNull() || right.isNull()) {
                    return SQLite3Constant.createNullConstant();
                } else {
                    SQLite3Constant applyEquals = left.applyEquals(right, collate);
                    if (applyEquals == null) {
                        return null;
                    }
                    boolean equals = applyEquals.asInt() == 1;
                    return SQLite3Constant.createBoolean(!equals);
                }
            }

        },
        IS("IS") {
            @Override
            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right,
                    SQLite3Schema.SQLite3Column.SQLite3CollateSequence collate) {
                if (left == null || right == null) {
                    return null;
                } else if (left.isNull()) {
                    return SQLite3Constant.createBoolean(right.isNull());
                } else if (right.isNull()) {
                    return SQLite3Constant.createFalse();
                } else {
                    return left.applyEquals(right, collate);
                }
            }

        },
        IS_NOT("IS NOT") {
            @Override
            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right,
                    SQLite3Schema.SQLite3Column.SQLite3CollateSequence collate) {
                if (left == null || right == null) {
                    return null;
                } else if (left.isNull()) {
                    return SQLite3Constant.createBoolean(!right.isNull());
                } else if (right.isNull()) {
                    return SQLite3Constant.createTrue();
                } else {
                    SQLite3Constant applyEquals = left.applyEquals(right, collate);
                    if (applyEquals == null) {
                        return null;
                    }
                    boolean equals = applyEquals.asInt() == 1;
                    return SQLite3Constant.createBoolean(!equals);
                }
            }

        },
        LIKE("LIKE") {
            @Override
            public boolean shouldApplyAffinity() {
                return false;
            }

            @Override
            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right,
                    SQLite3Schema.SQLite3Column.SQLite3CollateSequence collate) {
                if (left == null || right == null) {
                    return null;
                }
                if (left.isNull() || right.isNull()) {
                    return SQLite3Constant.createNullConstant();
                }
                SQLite3Constant leftStr = SQLite3Cast.castToText(left);
                SQLite3Constant rightStr = SQLite3Cast.castToText(right);
                if (leftStr == null || rightStr == null) {
                    return null;
                }
                boolean val = LikeImplementationHelper.match(leftStr.asString(), rightStr.asString(), 0, 0, false);
                return SQLite3Constant.createBoolean(val);
            }

        },
        GLOB("GLOB") {

            @Override
            public boolean shouldApplyAffinity() {
                return false;
            }

            @Override
            SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right,
                    SQLite3Schema.SQLite3Column.SQLite3CollateSequence collate) {
                if (left == null || right == null) {
                    return null;
                }
                if (left.isNull() || right.isNull()) {
                    return SQLite3Constant.createNullConstant();
                }
                SQLite3Constant leftStr = SQLite3Cast.castToText(left);
                SQLite3Constant rightStr = SQLite3Cast.castToText(right);
                if (leftStr == null || rightStr == null) {
                    return null;
                }
                boolean val = match(leftStr.asString(), rightStr.asString(), 0, 0);
                return SQLite3Constant.createBoolean(val);
            }

            private boolean match(String str, String regex, int regexPosition, int strPosition) {
                if (strPosition == str.length() && regexPosition == regex.length()) {
                    return true;
                }
                if (regexPosition >= regex.length()) {
                    return false;
                }
                char cur = regex.charAt(regexPosition);
                if (strPosition >= str.length()) {
                    if (cur == '*') {
                        return match(str, regex, regexPosition + 1, strPosition);
                    } else {
                        return false;
                    }
                }
                switch (cur) {
                case '[':
                    int endingBrackets = regexPosition;
                    do {
                        endingBrackets++;
                        if (endingBrackets >= regex.length()) {
                            return false;
                        }
                    } while (regex.charAt(endingBrackets) != ']');
                    StringBuilder patternInBrackets = new StringBuilder(
                            regex.substring(regexPosition + 1, endingBrackets));
                    boolean inverted;
                    if (patternInBrackets.toString().startsWith("^")) {
                        if (patternInBrackets.length() > 1) {
                            inverted = true;
                            patternInBrackets = new StringBuilder(patternInBrackets.substring(1));
                        } else {
                            return false;
                        }
                    } else {
                        inverted = false;
                    }
                    int currentSearchIndex = 0;
                    boolean found = false;
                    do {
                        int minusPosition = patternInBrackets.toString().indexOf('-', currentSearchIndex);
                        boolean minusAtBoundaries = minusPosition == 0
                                || minusPosition == patternInBrackets.length() - 1;
                        if (minusPosition == -1 || minusAtBoundaries) {
                            break;
                        }
                        found = true;
                        StringBuilder expandedPattern = new StringBuilder();
                        for (char start = patternInBrackets.charAt(minusPosition - 1); start < patternInBrackets
                                .charAt(minusPosition + 1); start += 1) {
                            expandedPattern.append(start);
                        }
                        patternInBrackets.replace(minusPosition, minusPosition + 1, expandedPattern.toString());
                        currentSearchIndex = minusPosition + expandedPattern.length();
                    } while (found);

                    if (patternInBrackets.length() > 0) {
                        char textChar = str.charAt(strPosition);
                        boolean contains = patternInBrackets.toString().contains(Character.toString(textChar));
                        if (contains && !inverted || !contains && inverted) {
                            return match(str, regex, endingBrackets + 1, strPosition + 1);
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }

                case '*':
                    // match
                    boolean foundMatch = match(str, regex, regexPosition, strPosition + 1);
                    if (!foundMatch) {
                        return match(str, regex, regexPosition + 1, strPosition);
                    } else {
                        return true;
                    }
                case '?':
                    return match(str, regex, regexPosition + 1, strPosition + 1);
                default:
                    if (cur == str.charAt(strPosition)) {
                        return match(str, regex, regexPosition + 1, strPosition + 1);
                    } else {
                        return false;
                    }
                }
            }

        };

        private final String[] textRepresentation;

        SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right,
                SQLite3Schema.SQLite3Column.SQLite3CollateSequence collate) {
            return null;
        }

        public boolean shouldApplyAffinity() {
            return true;
        }

        BinaryComparisonOperator(String... textRepresentation) {
            this.textRepresentation = textRepresentation.clone();
        }

        public static BinaryComparisonOperator getRandomOperator() {
            return Randomly.fromOptions(values());
        }

        public static BinaryComparisonOperator getRandomRowValueOperator() {
            return Randomly.fromOptions(SMALLER, SMALLER_EQUALS, GREATER, GREATER_EQUALS, EQUALS, NOT_EQUALS);
        }

        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentation);
        }

        public SQLite3Constant applyOperand(SQLite3Constant leftBeforeAffinity, SQLite3TypeAffinity leftAffinity,
                SQLite3Constant rightBeforeAffinity, SQLite3TypeAffinity rightAffinity, SQLite3Expression origLeft,
                SQLite3Expression origRight, boolean applyAffinity) {

            SQLite3Constant left;
            SQLite3Constant right;
            if (applyAffinity) {
                ConstantTuple vals = applyAffinities(leftAffinity, rightAffinity, leftBeforeAffinity,
                        rightBeforeAffinity);
                left = vals.left;
                right = vals.right;
            } else {
                left = leftBeforeAffinity;
                right = rightBeforeAffinity;
            }

            // If either operand has an explicit collating function assignment using the
            // postfix COLLATE operator, then the explicit collating function is used for
            // comparison, with precedence to the collating function of the left operand.
            SQLite3Schema.SQLite3Column.SQLite3CollateSequence seq = origLeft.getExplicitCollateSequence();
            if (seq == null) {
                seq = origRight.getExplicitCollateSequence();
            }
            // If either operand is a column, then the collating function of that column is
            // used with precedence to the left operand. For the purposes of the previous
            // sentence, a column name preceded by one or more unary "+" operators is still
            // considered a column name.
            if (seq == null) {
                seq = origLeft.getImplicitCollateSequence();
            }
            if (seq == null) {
                seq = origRight.getImplicitCollateSequence();
            }
            // Otherwise, the BINARY collating function is used for comparison.
            if (seq == null) {
                seq = SQLite3Schema.SQLite3Column.SQLite3CollateSequence.BINARY;
            }
            return apply(left, right, seq);
        }

    }

    @Override
    public SQLite3Schema.SQLite3Column.SQLite3CollateSequence getExplicitCollateSequence() {
        if (left.getExplicitCollateSequence() != null) {
            return left.getExplicitCollateSequence();
        } else {
            return right.getExplicitCollateSequence();
        }
    }

    @Override
    public String getOperatorRepresentation() {
        return operation.getTextRepresentation();
    }

}
