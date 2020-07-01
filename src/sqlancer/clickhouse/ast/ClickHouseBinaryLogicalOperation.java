package sqlancer.clickhouse.ast;

import sqlancer.Randomly;

public class ClickHouseBinaryLogicalOperation extends ClickHouseExpression {

    private final ClickHouseExpression left;
    private final ClickHouseExpression right;
    private final ClickHouseBinaryLogicalOperator op;
    private final String textRepresentation;

    public enum ClickHouseBinaryLogicalOperator {
        AND("AND", "and") {
            @Override
            public ClickHouseConstant apply(ClickHouseConstant left, ClickHouseConstant right) {
                if (left.isNull() && right.isNull()) {
                    return ClickHouseConstant.createNullConstant();
                } else if (left.isNull()) {
                    if (right.asBooleanNotNull()) {
                        return ClickHouseConstant.createNullConstant();
                    } else {
                        return ClickHouseConstant.createFalse();
                    }
                } else if (right.isNull()) {
                    if (left.asBooleanNotNull()) {
                        return ClickHouseConstant.createNullConstant();
                    } else {
                        return ClickHouseConstant.createFalse();
                    }
                } else {
                    return left.asBooleanNotNull() && right.asBooleanNotNull() ? ClickHouseConstant.createTrue()
                            : ClickHouseConstant.createFalse();
                }
            }
        },
        OR("OR", "or") {
            @Override
            public ClickHouseConstant apply(ClickHouseConstant left, ClickHouseConstant right) {
                if (!left.isNull() && left.asBooleanNotNull()) {
                    return ClickHouseConstant.createTrue();
                } else if (!right.isNull() && right.asBooleanNotNull()) {
                    return ClickHouseConstant.createTrue();
                } else if (left.isNull() || right.isNull()) {
                    return ClickHouseConstant.createNullConstant();
                } else {
                    return ClickHouseConstant.createFalse();
                }
            }
        };

        private final String[] textRepresentations;

        ClickHouseBinaryLogicalOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        public abstract ClickHouseConstant apply(ClickHouseConstant left, ClickHouseConstant right);

        public static ClickHouseBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public ClickHouseBinaryLogicalOperation(ClickHouseExpression left, ClickHouseExpression right,
            ClickHouseBinaryLogicalOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
        this.textRepresentation = op.getTextRepresentation();
    }

    public ClickHouseExpression getLeft() {
        return left;
    }

    public ClickHouseBinaryLogicalOperator getOp() {
        return op;
    }

    public ClickHouseExpression getRight() {
        return right;
    }

    public String getTextRepresentation() {
        return textRepresentation;
    }

    @Override
    public ClickHouseConstant getExpectedValue() {
        ClickHouseConstant leftExpected = left.getExpectedValue();
        ClickHouseConstant rightExpected = right.getExpectedValue();

        return op.apply(leftExpected, rightExpected);
    }

}
