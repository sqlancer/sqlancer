package sqlancer.h2.dialect.expression;

import sqlancer.Randomly;
import sqlancer.simple.expression.Expression;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class H2IsDistinctFrom implements Expression {
    Expression left;
    Expression right;
    boolean isNegated;

    public H2IsDistinctFrom(Generator gen) {
        left = gen.generateResponse(Signal.EXPRESSION);
        right = gen.generateResponse(Signal.EXPRESSION);
        isNegated = Randomly.getBoolean();
    }

    @Override
    public String print() {
        String notText = isNegated ? "NOT" : "";
        return "(" + left.print() + ") IS " + notText + " DISTINCT FROM (" + right.print() + ")";
    }

}
