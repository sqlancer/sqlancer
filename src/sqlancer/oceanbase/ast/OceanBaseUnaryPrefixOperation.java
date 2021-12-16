package sqlancer.oceanbase.ast;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.UnaryOperatorNode;
import sqlancer.oceanbase.ast.OceanBaseUnaryPrefixOperation.OceanBaseUnaryPrefixOperator;

public class OceanBaseUnaryPrefixOperation extends UnaryOperatorNode<OceanBaseExpression, OceanBaseUnaryPrefixOperator>
        implements OceanBaseExpression {

    public enum OceanBaseUnaryPrefixOperator implements Operator {
        NOT("!", "NOT") {
            @Override
            public OceanBaseConstant applyNotNull(OceanBaseConstant expr) {
                return OceanBaseConstant.createIntConstant(expr.asBooleanNotNull() ? 0 : 1);
            }
        },
        PLUS("+") {
            @Override
            public OceanBaseConstant applyNotNull(OceanBaseConstant expr) {
                return expr;
            }
        },
        MINUS("-") {
            @Override
            public OceanBaseConstant applyNotNull(OceanBaseConstant expr) {
                if (expr.isString()) {
                    throw new IgnoreMeException();
                } else if (expr.isInt()) {
                    if (!expr.isSigned()) {
                        throw new IgnoreMeException();
                    }
                    return OceanBaseConstant.createIntConstant(-expr.getInt());
                } else if (expr.isDouble()) {
                    return OceanBaseConstant.createDoubleConstant(-expr.getDouble());
                } else {
                    throw new AssertionError(expr);
                }
            }
        };

        private String[] textRepresentations;

        OceanBaseUnaryPrefixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract OceanBaseConstant applyNotNull(OceanBaseConstant expr);

        public static OceanBaseUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }
    }

    public OceanBaseUnaryPrefixOperation(OceanBaseExpression expr, OceanBaseUnaryPrefixOperator op) {
        super(expr, op);
    }

    @Override
    public OceanBaseConstant getExpectedValue() {
        OceanBaseConstant subExprVal = expr.getExpectedValue();
        if (op == OceanBaseUnaryPrefixOperator.PLUS) {
            if (subExprVal.isNull() && subExprVal.getType() == null) {
                return OceanBaseConstant.createNullConstant();
            } else {
                return subExprVal;
            }
        }
        if (subExprVal.isNull()) {
            return OceanBaseConstant.createNullConstant();
        } else {
            return op.applyNotNull(subExprVal);
        }
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }

    public OceanBaseExpression getExpr() {
        return expr;
    }

    public OceanBaseUnaryPrefixOperator getOp() {
        return op;
    }
}
