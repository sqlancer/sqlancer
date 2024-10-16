package sqlancer.simple.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class Function implements Expression {

    static Func[] functions = { Func.create("ACOS", 1), Func.create("ASIN", 1), Func.create("ATAN", 1),
            Func.create("COS", 1), Func.create("SIN", 1), Func.create("TAN", 1), Func.create("COT", 1),
            Func.create("ATAN2", 1), Func.create("ABS", 1), Func.create("CEIL", 1), Func.create("CEILING", 1),
            Func.create("FLOOR", 1), Func.create("LOG", 1), Func.create("LOG10", 1), Func.create("LOG2", 1),
            Func.create("LN", 1), Func.create("PI", 0), Func.create("SQRT", 1), Func.create("POWER", 1),
            Func.create("CBRT", 1), Func.create("ROUND", 2), Func.create("SIGN", 1), Func.create("DEGREES", 1),
            Func.create("RADIANS", 1), Func.create("MOD", 2), Func.create("XOR", 2), Func.create("LENGTH", 1),
            Func.create("LOWER", 1), Func.create("UPPER", 1), Func.create("SUBSTRING", 3), Func.create("REVERSE", 1),
            Func.create("CONCAT", -1), Func.create("CONCAT_WS", -1), Func.create("CONTAINS", 2),
            Func.create("PREFIX", 2), Func.create("SUFFIX", 2), Func.create("INSTR", 2), Func.create("PRINTF", -1),
            Func.create("REGEXP_MATCHES", 2), Func.create("REGEXP_REPLACE", 3), Func.create("STRIP_ACCENTS", 1),
            Func.create("DATE_PART", 2), Func.create("AGE", 2), Func.create("COALESCE", 3), Func.create("NULLIF", 2),
            Func.create("LTRIM", 1), Func.create("RTRIM", 1), Func.create("REPLACE", 3), Func.create("UNICODE", 1),
            Func.create("BIT_COUNT", 1), Func.create("BIT_LENGTH", 1), Func.create("LAST_DAY", 1),
            Func.create("MONTHNAME", 1), Func.create("DAYNAME", 1), Func.create("YEARWEEK", 1),
            Func.create("DAYOFMONTH", 1), Func.create("WEEKDAY", 1), Func.create("WEEKOFYEAR", 1),
            Func.create("IFNULL", 2), Func.create("IF", 3) };

    String functionName;
    List<Expression> expressions;

    public Function(Generator gen) {
        Func func = Randomly.fromOptions(functions);
        functionName = func.name;
        int argCount = func.argCount;
        if (argCount == -1) {
            argCount = Randomly.smallNumber() + 1;
        }
        expressions = new ArrayList<>();
        for (int i = 0; i < argCount; i++) {
            expressions.add(gen.generateResponse(Signal.EXPRESSION));
        }
    }

    @Override
    public String print() {
        String expressionsText = expressions.stream().map(Expression::print).collect(Collectors.joining(", "));

        return functionName + "(" + expressionsText + ")";
    }

    static class Func {
        String name;
        int argCount;

        Func(String name, int argCount) {
            this.name = name;
            this.argCount = argCount;
        }

        static Func create(String name, int argCount) {
            return new Func(name, argCount);
        }
    }
}
