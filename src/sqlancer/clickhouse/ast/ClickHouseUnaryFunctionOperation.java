package sqlancer.clickhouse.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.visitor.UnaryOperation;

public class ClickHouseUnaryFunctionOperation extends ClickHouseExpression
        implements UnaryOperation<ClickHouseExpression> {
    private final ClickHouseUnaryFunctionOperation.ClickHouseUnaryFunctionOperator operator;
    private final ClickHouseExpression expression;

    public ClickHouseUnaryFunctionOperation(ClickHouseExpression expression, ClickHouseUnaryFunctionOperator operator) {
        this.operator = operator;
        this.expression = expression;
    }

    public enum ClickHouseUnaryFunctionOperator implements Operator {
        EXP("exp"), SQRT("sqrt"), ERF("erf"), SIN("sin"), COS("cos"), TAN("tan"), SIGN("sign"), RADIANS("radians"),
        LOG("log"), ABS("abs");

        private String textRepresentation;

        ClickHouseUnaryFunctionOperator(String text) {
            this.textRepresentation = text;
        }

        public static ClickHouseUnaryFunctionOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }
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
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }

}
