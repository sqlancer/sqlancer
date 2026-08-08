package sqlancer.sqlite3.dialect.expression;

import sqlancer.Randomly;
import sqlancer.simple.expression.Expression;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class SQLite3Collate implements Expression {
    public static String[] operators = { "NOCASE", "BINARY", "RTRIM" };
    Expression expression;
    String operator;

    public SQLite3Collate(Generator gen) {
        this.expression = gen.generateResponse(Signal.EXPRESSION);
        this.operator = Randomly.fromOptions(operators);
    }

    @Override
    public String print() {
        return "(" + expression.print() + ") COLLATE " + operator;
    }
}
