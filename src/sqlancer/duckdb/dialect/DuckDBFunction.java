package sqlancer.duckdb.dialect;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.simple.expression.Expression;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class DuckDBFunction implements Expression {

    static Func[] functions = { new Func("ACOS", 1), new Func("ASIN", 1), new Func("ATAN", 1), new Func("COS", 1),
            new Func("SIN", 1), new Func("TAN", 1), new Func("COT", 1), new Func("ATAN2", 1), new Func("ABS", 1),
            new Func("CEIL", 1), new Func("CEILING", 1), new Func("FLOOR", 1), new Func("LOG", 1), new Func("LOG10", 1),
            new Func("LOG2", 1), new Func("LN", 1), new Func("PI", 0), new Func("SQRT", 1), new Func("POWER", 1),
            new Func("CBRT", 1), new Func("ROUND", 2), new Func("SIGN", 1), new Func("DEGREES", 1),
            new Func("RADIANS", 1), new Func("MOD", 2), new Func("XOR", 2), new Func("LENGTH", 1), new Func("LOWER", 1),
            new Func("UPPER", 1), new Func("SUBSTRING", 3), new Func("REVERSE", 1), new Func("CONCAT", 1, true),
            new Func("CONCAT_WS", 1, true), new Func("CONTAINS", 2), new Func("PREFIX", 2), new Func("SUFFIX", 2),
            new Func("INSTR", 2), new Func("PRINTF", 1, true), new Func("REGEXP_MATCHES", 2),
            new Func("REGEXP_REPLACE", 3), new Func("STRIP_ACCENTS", 1), new Func("DATE_PART", 2), new Func("AGE", 2),
            new Func("COALESCE", 3), new Func("NULLIF", 2), new Func("LTRIM", 1), new Func("RTRIM", 1),
            new Func("REPLACE", 3), new Func("UNICODE", 1), new Func("BIT_COUNT", 1), new Func("BIT_LENGTH", 1),
            new Func("LAST_DAY", 1), new Func("MONTHNAME", 1), new Func("DAYNAME", 1), new Func("YEARWEEK", 1),
            new Func("DAYOFMONTH", 1), new Func("WEEKDAY", 1), new Func("WEEKOFYEAR", 1), new Func("IFNULL", 2),
            new Func("IF", 3) };

    String functionName;
    List<Expression> expressions;

    public DuckDBFunction(Generator gen) {
        Func func = Randomly.fromOptions(functions);
        functionName = func.name;
        int argCount = func.isVariadic ? Randomly.smallNumber() + func.argCount : func.argCount;
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
        boolean isVariadic;

        Func(String name, int argCount, boolean isVariadic) {
            this.name = name;
            this.argCount = argCount;
            this.isVariadic = isVariadic;
        }

        Func(String name, int argCount) {
            this.name = name;
            this.argCount = argCount;
            this.isVariadic = false;
        }

    }
}
