package sqlancer.clickhouse.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.visitor.UnaryOperation;

public class ClickHouseUnaryPostfixOperation extends ClickHouseExpression
        implements UnaryOperation<ClickHouseExpression> {

    private final ClickHouseExpression expression;
    private final ClickHouseUnaryPostfixOperator operator;
    private boolean negate;

    public enum ClickHouseUnaryPostfixOperator implements BinaryOperatorNode.Operator {
        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

        private String s;

        ClickHouseUnaryPostfixOperator(String s) {
            this.s = s;
        }

        public static ClickHouseUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return s;
        }
    }

    public ClickHouseUnaryPostfixOperation(ClickHouseExpression expr, ClickHouseUnaryPostfixOperator op,
            boolean negate) {
        this.expression = expr;
        this.operator = op;
        this.setNegate(negate);
    }

    @Override
    public ClickHouseExpression getExpression() {
        return expression;
    }

    @Override
    public String getOperatorRepresentation() {
        return operator.getTextRepresentation();
    }

    @Override
    public boolean omitBracketsWhenPrinting() {
        return false;
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.POSTFIX;
    }

    public ClickHouseUnaryPostfixOperator getOperator() {
        return operator;
    }

    public boolean isNegated() {
        return negate;
    }

    public void setNegate(boolean negate) {
        this.negate = negate;
    }

    @Override
    public ClickHouseConstant getExpectedValue() {
        boolean val;
        ClickHouseConstant expectedValue = expression.getExpectedValue();
        switch (operator) {
        case IS_NULL:
            val = expectedValue.isNull();
            break;
        case IS_NOT_NULL:
            val = !expectedValue.isNull();
            break;
        default:
            throw new AssertionError(operator);
        }
        if (negate) {
            val = !val;
        }
        return ClickHouseConstant.createInt32Constant(val ? 1 : 0);
    }

}
