package sqlancer.simple.expression;

import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class Not implements Expression {

    Expression expr;

    public Not(Expression expr) {
        this.expr = expr;
    }

    public Not(Generator gen) {
        expr = gen.generateResponse(Signal.EXPRESSION);
    }

    @Override
    public String print() {
        return "NOT (" + expr.print() + ")";
    }
}
