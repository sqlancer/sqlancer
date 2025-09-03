package sqlancer.simple.expression;

import sqlancer.Randomly;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class Between implements Expression {
    Expression expr;
    Expression min;
    Expression max;
    boolean isNegated;

    public Between(Expression expr, Expression min, Expression max, boolean isNegated) {
        this.expr = expr;
        this.min = min;
        this.max = max;
        this.isNegated = isNegated;
    }

    public Between(Generator gen) {
        expr = gen.generateResponse(Signal.EXPRESSION);
        min = gen.generateResponse(Signal.EXPRESSION);
        max = gen.generateResponse(Signal.EXPRESSION);
        isNegated = Randomly.getBoolean();
    }

    @Override
    public String print() {
        String notText = isNegated ? "NOT" : "";
        return "(" + expr.print() + ") " + notText + " BETWEEN (" + min.print() + ") AND (" + max.print() + ")";
    }

}
