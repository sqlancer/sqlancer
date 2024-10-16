package sqlancer.simple.expression;

import java.util.List;

import sqlancer.simple.Expression;
import sqlancer.simple.Signal;
import sqlancer.simple.SignalResponse;

public class Constant implements Expression {
    String value;

    public Constant(String value) {
        this.value = value;
    }

    @Override
    public String parse() {
        return value;
    }

    public static class Op implements Operation {

        static final List<Signal> signals = List.of(Signal.CONSTANT_VALUE);

        @Override
        public List<Signal> getRequestSignals() {
            return signals;
        }

        @Override
        public Expression create(List<SignalResponse> innerExpressions) {
            assert innerExpressions.size() == getRequestSignals()
                    .size() : "Constant operation has 1 operand, but received " + innerExpressions.size();

            return ((SignalResponse.ExpressionResponse) innerExpressions.get(0)).getExpression();
        }
    }

}
