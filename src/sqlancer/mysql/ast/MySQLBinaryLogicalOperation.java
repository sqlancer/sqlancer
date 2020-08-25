package sqlancer.mysql.ast;

import sqlancer.Randomly;

public class MySQLBinaryLogicalOperation implements MySQLExpression {

    private final MySQLExpression left;
    private final MySQLExpression right;
    private final MySQLBinaryLogicalOperator op;
    private final String textRepresentation;

    public enum MySQLBinaryLogicalOperator {
        AND("AND", "&&") {
            @Override
            public MySQLConstant apply(MySQLConstant left, MySQLConstant right) {
                if (left.isNull() && right.isNull()) {
                    return MySQLConstant.createNullConstant();
                } else if (left.isNull()) {
                    if (right.asBooleanNotNull()) {
                        return MySQLConstant.createNullConstant();
                    } else {
                        return MySQLConstant.createFalse();
                    }
                } else if (right.isNull()) {
                    if (left.asBooleanNotNull()) {
                        return MySQLConstant.createNullConstant();
                    } else {
                        return MySQLConstant.createFalse();
                    }
                } else {
                    return MySQLConstant.createBoolean(left.asBooleanNotNull() && right.asBooleanNotNull());
                }
            }
        },
        OR("OR", "||") {
            @Override
            public MySQLConstant apply(MySQLConstant left, MySQLConstant right) {
                if (!left.isNull() && left.asBooleanNotNull()) {
                    return MySQLConstant.createTrue();
                } else if (!right.isNull() && right.asBooleanNotNull()) {
                    return MySQLConstant.createTrue();
                } else if (left.isNull() || right.isNull()) {
                    return MySQLConstant.createNullConstant();
                } else {
                    return MySQLConstant.createFalse();
                }
            }
        },
        XOR("XOR") {
            @Override
            public MySQLConstant apply(MySQLConstant left, MySQLConstant right) {
                if (left.isNull() || right.isNull()) {
                    return MySQLConstant.createNullConstant();
                }
                boolean xorVal = left.asBooleanNotNull() ^ right.asBooleanNotNull();
                return MySQLConstant.createBoolean(xorVal);
            }
        };

        private final String[] textRepresentations;

        MySQLBinaryLogicalOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        public abstract MySQLConstant apply(MySQLConstant left, MySQLConstant right);

        public static MySQLBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public MySQLBinaryLogicalOperation(MySQLExpression left, MySQLExpression right, MySQLBinaryLogicalOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
        this.textRepresentation = op.getTextRepresentation();
    }

    public MySQLExpression getLeft() {
        return left;
    }

    public MySQLBinaryLogicalOperator getOp() {
        return op;
    }

    public MySQLExpression getRight() {
        return right;
    }

    public String getTextRepresentation() {
        return textRepresentation;
    }

    @Override
    public MySQLConstant getExpectedValue() {
        MySQLConstant leftExpected = left.getExpectedValue();
        MySQLConstant rightExpected = right.getExpectedValue();
        if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
            return null;
        }
        return op.apply(leftExpected, rightExpected);
    }

}
