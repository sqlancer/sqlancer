package sqlancer.simple.expression;

import java.util.List;

import sqlancer.simple.Expression;
import sqlancer.simple.Signal;
import sqlancer.simple.SignalResponse;

public class NotBetween implements Expression {
    Expression expr;
    Expression min;
    Expression max;

    public NotBetween(Expression expr, Expression min, Expression max) {
        this.expr = expr;
        this.min = min;
        this.max = max;
    }

    @Override
    public String parse() {
        return expr.parse() + " BETWEEN " + min.parse() + " AND " + max.parse();
    }

    public static class Op implements Operation {

        static final List<Signal> signals = List.of(Signal.EXPRESSION, Signal.EXPRESSION, Signal.EXPRESSION);

        @Override
        public List<Signal> getRequestSignals() {
            return signals;
        }

        @Override
        public Expression create(List<SignalResponse> innerExpressions) {
            assert innerExpressions.size() == getRequestSignals()
                    .size() : "Between Operation has 3 operands, but received " + innerExpressions.size();

            Expression expr = ((SignalResponse.ExpressionResponse) innerExpressions.get(0)).getExpression();
            Expression min = ((SignalResponse.ExpressionResponse) innerExpressions.get(1)).getExpression();
            Expression max = ((SignalResponse.ExpressionResponse) innerExpressions.get(2)).getExpression();

            return new NotBetween(expr, min, max);
        }
    }
}
