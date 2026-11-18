package sqlancer.simple.expression;

import sqlancer.Randomly;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class PrefixSign implements Expression {
    public static String[] operators = { "+", "-" };
    Expression expr;
    String operator;

    public PrefixSign(Generator gen) {
        expr = gen.generateResponse(Signal.EXPRESSION);
        operator = Randomly.fromOptions(operators);
    }

    @Override
    public String print() {
        return operator + "(" + expr.print() + ")";
    }
}
