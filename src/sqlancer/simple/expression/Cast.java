package sqlancer.simple.expression;

import java.util.List;

import sqlancer.simple.Expression;
import sqlancer.simple.Signal;
import sqlancer.simple.SignalResponse;

public class Cast implements Expression {
    Expression inner;
    String type;

    public Cast(Expression inner, String type) {
        this.inner = inner;
        this.type = type;
    }

    @Override
    public String parse() {
        return "CAST " + inner.parse() + " AS " + type;
    }

    public static class Op implements Operation {

        static final List<Signal> signals = List.of(Signal.EXPRESSION, Signal.TYPE);

        @Override
        public List<Signal> getRequestSignals() {
            return signals;
        }

        @Override
        public Expression create(List<SignalResponse> innerExpressions) {
            assert innerExpressions.size() == getRequestSignals().size() : "Cast has 2 operands, but received "
                    + innerExpressions.size();

            Expression expr = ((SignalResponse.ExpressionResponse) innerExpressions.get(0)).getExpression();
            String type = ((SignalResponse.StringResponse) innerExpressions.get(1)).getString();

            return new Cast(expr, type);
        }
    }
}
