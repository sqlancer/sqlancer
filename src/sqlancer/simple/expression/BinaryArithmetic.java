package sqlancer.simple.expression;

import sqlancer.Randomly;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class BinaryArithmetic implements Expression {
    static String[] operators = { "AND", "OR" };
    Expression left;
    Expression right;
    String operator;

    public BinaryArithmetic(Generator gen) {
        this.left = gen.generateResponse(Signal.EXPRESSION);
        this.right = gen.generateResponse(Signal.EXPRESSION);
        this.operator = Randomly.fromOptions(operators);
    }

    @Override
    public String print() {
        return "(" + this.left.print() + ") " + operator + " (" + this.right.print() + ")";
    }
}
