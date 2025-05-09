package sqlancer.simple.expression;

import sqlancer.Randomly;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class Collate implements Expression {
    public static String[] operators = { "NOCASE", "NOACCENT", "NOACCENT.NOCASE", "C", "POSIX" };
    Expression expression;
    String operator;

    public Collate(Generator gen) {
        this.expression = gen.generateResponse(Signal.EXPRESSION);
        this.operator = Randomly.fromOptions(operators);
    }

    @Override
    public String print() {
        return "(" + expression.print() + ") COLLATE " + operator;
    }
}
