package sqlancer.oceanbase.ast;

import sqlancer.Randomly;

public class OceanBaseBinaryLogicalOperation implements OceanBaseExpression {

    private final OceanBaseExpression left;
    private final OceanBaseExpression right;
    private final OceanBaseBinaryLogicalOperator op;
    private final String textRepresentation;

    public enum OceanBaseBinaryLogicalOperator {
        AND("AND", "&&") {
            @Override
            public OceanBaseConstant apply(OceanBaseConstant left, OceanBaseConstant right) {
                if (left.isNull() && right.isNull()) {
                    return OceanBaseConstant.createNullConstant();
                } else if (left.isNull()) {
                    if (right.asBooleanNotNull()) {
                        return OceanBaseConstant.createNullConstant();
                    } else {
                        return OceanBaseConstant.createFalse();
                    }
                } else if (right.isNull()) {
                    if (left.asBooleanNotNull()) {
                        return OceanBaseConstant.createNullConstant();
                    } else {
                        return OceanBaseConstant.createFalse();
                    }
                } else {
                    return OceanBaseConstant.createBoolean(left.asBooleanNotNull() && right.asBooleanNotNull());
                }
            }
        },
        OR("OR", "||") {
            @Override
            public OceanBaseConstant apply(OceanBaseConstant left, OceanBaseConstant right) {
                if (!left.isNull() && left.asBooleanNotNull()) {
                    return OceanBaseConstant.createTrue();
                } else if (!right.isNull() && right.asBooleanNotNull()) {
                    return OceanBaseConstant.createTrue();
                } else if (left.isNull() || right.isNull()) {
                    return OceanBaseConstant.createNullConstant();
                } else {
                    return OceanBaseConstant.createFalse();
                }
            }
        },
        XOR("XOR") {
            @Override
            public OceanBaseConstant apply(OceanBaseConstant left, OceanBaseConstant right) {
                if (left.isNull() || right.isNull()) {
                    return OceanBaseConstant.createNullConstant();
                }
                boolean xorVal = left.asBooleanNotNull() ^ right.asBooleanNotNull();
                return OceanBaseConstant.createBoolean(xorVal);
            }
        };

        private final String[] textRepresentations;

        OceanBaseBinaryLogicalOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        public abstract OceanBaseConstant apply(OceanBaseConstant left, OceanBaseConstant right);

        public static OceanBaseBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public OceanBaseBinaryLogicalOperation(OceanBaseExpression left, OceanBaseExpression right,
            OceanBaseBinaryLogicalOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
        this.textRepresentation = op.getTextRepresentation();
    }

    public OceanBaseExpression getLeft() {
        return left;
    }

    public OceanBaseBinaryLogicalOperator getOp() {
        return op;
    }

    public OceanBaseExpression getRight() {
        return right;
    }

    public String getTextRepresentation() {
        return textRepresentation;
    }

    @Override
    public OceanBaseConstant getExpectedValue() {
        OceanBaseConstant leftExpected = left.getExpectedValue();
        OceanBaseConstant rightExpected = right.getExpectedValue();
        if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
            return null;
        }
        return op.apply(leftExpected, rightExpected);
    }

}
