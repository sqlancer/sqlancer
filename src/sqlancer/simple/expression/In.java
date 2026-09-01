package sqlancer.simple.expression;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class In implements Expression {
    Expression left;
    List<Expression> right;
    boolean isNegated;

    public In(Generator gen) {
        this.left = gen.generateResponse(Signal.EXPRESSION);
        this.right = gen.generateResponse(Signal.EXPRESSION_LIST);
        this.isNegated = Randomly.getBoolean();
    }

    @Override
    public String print() {
        String notText = isNegated ? "NOT" : "";
        String listText = right.stream().map(Expression::print).collect(Collectors.joining(", "));

        return "(" + left.print() + ") " + notText + " IN (" + listText + ")";
    }
}
