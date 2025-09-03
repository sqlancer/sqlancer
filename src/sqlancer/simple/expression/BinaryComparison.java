package sqlancer.simple.expression;

import sqlancer.Randomly;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class BinaryComparison implements Expression {
    static String[] operators = { "=", "+", ">", ">=", "<", "<=", "!=", "~", "!~" };

    Expression left;
    Expression right;
    String operator;

    public BinaryComparison(Generator gen) {
        this.left = gen.generateResponse(Signal.EXPRESSION);
        this.right = gen.generateResponse(Signal.EXPRESSION);
        this.operator = Randomly.fromOptions(operators);
    }

    @Override
    public String print() {
        return "(" + this.left.print() + ") " + operator + " (" + this.right.print() + ")";
    }
}
