package sqlancer.simple.expression;

import sqlancer.Randomly;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class SimilarTo implements Expression {
    Expression left;
    Expression right;
    boolean isNegated;

    public SimilarTo(Generator gen) {
        this.left = gen.generateResponse(Signal.EXPRESSION);
        this.right = gen.generateResponse(Signal.EXPRESSION);
        this.isNegated = Randomly.getBoolean();
    }

    @Override
    public String print() {
        String notText = isNegated ? "NOT" : "";
        return "(" + this.left.print() + ") " + notText + " SIMILAR TO (" + this.right.print() + ")";
    }

}
