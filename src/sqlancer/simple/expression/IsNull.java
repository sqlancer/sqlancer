package sqlancer.simple.expression;

import sqlancer.Randomly;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class IsNull implements Expression {

    Expression expr;
    boolean isNegated;

    public IsNull(Expression expr, boolean isNegated) {
        this.expr = expr;
        this.isNegated = isNegated;
    }

    public IsNull(Generator gen) {
        expr = gen.generateResponse(Signal.EXPRESSION);
        isNegated = Randomly.getBoolean();
    }

    @Override
    public String print() {
        String notText = isNegated ? "NOT" : "";
        return "(" + expr.print() + ") IS " + notText + " NULL";
    }
}
