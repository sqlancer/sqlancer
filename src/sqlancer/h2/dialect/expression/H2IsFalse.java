package sqlancer.h2.dialect.expression;

import sqlancer.Randomly;
import sqlancer.simple.expression.Expression;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class H2IsFalse implements Expression {
    Expression expr;
    boolean isNegated;

    public H2IsFalse(Generator gen) {
        expr = gen.generateResponse(Signal.EXPRESSION);
        isNegated = Randomly.getBoolean();
    }

    @Override
    public String print() {
        String notText = isNegated ? "NOT" : "";
        return "(" + expr.print() + ") IS " + notText + " FALSE";
    }
}
