package sqlancer.h2.dialect.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.simple.expression.Expression;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class H2Function implements Expression {
    static Func[] functions = { new Func("ABS", 1), new Func("ACOS", 1), new Func("ASIN", 1), new Func("ATAN", 1),
            new Func("COS", 1), new Func("COSH", 1), new Func("COT", 1), new Func("SIN", 1), new Func("SINH", 1),
            new Func("TAN", 1), new Func("TANH", 1), new Func("ATAN2", 2), new Func("BITAND", 2), new Func("BITGET", 2),
            new Func("BITNOT", 1), new Func("BITOR", 2), new Func("BITXOR", 2), new Func("LSHIFT", 2),
            new Func("RSHIFT", 2), new Func("MOD", 2), new Func("CEILING", 1), new Func("DEGREES", 1),
            new Func("EXP", 1), new Func("FLOOR", 1), new Func("LN", 1), new Func("LOG", 2), new Func("LOG10", 1),
            new Func("ORA_HASH", 1), new Func("RADIANS", 1), new Func("SQRT", 1), new Func("PI", 0),
            new Func("POWER", 2), new Func("ROUND", 2), new Func("ROUNDMAGIC", 1), new Func("SIGN", 1),
            new Func("TRUNCATE", 2), new Func("COMPRESS", 1), new Func("ZERO", 0), new Func("ASCII", 1),
            new Func("BIT_LENGTH", 1), new Func("LENGTH", 1), new Func("OCTET_LENGTH", 1), new Func("CHAR", 1),
            new Func("CONCAT", 2, true), new Func("CONCAT_WS", 3, true), new Func("DIFFERENCE", 2),
            new Func("HEXTORAW", 1), new Func("RAWTOHEX", 1), new Func("INSTR", 3), new Func("INSERT", 4),
            new Func("LOWER", 1), new Func("UPPER", 1), new Func("LEFT", 2), new Func("RIGHT", 2),
            new Func("LOCATE", 3), new Func("POSITION", 2), new Func("LTRIM", 1), new Func("RTRIM", 1),
            new Func("TRIM", 1), new Func("REGEXP_REPLACE", 3), new Func("REGEXP_LIKE", 2), new Func("REPLACE", 3),
            new Func("SOUNDEX", 1), new Func("STRINGDECODE", 1), new Func("STRINGENCODE", 1),
            new Func("STRINGTOUTF8", 1), new Func("SUBSTRING", 2), new Func("UTF8TOSTRING", 1),
            new Func("QUOTE_IDENT", 1), new Func("XMLATTR", 2), new Func("XMLNODE", 1), new Func("XMLCOMMENT", 1),
            new Func("XMLCDATA", 1), new Func("XMLSTARTDOC", 0), new Func("XMLTEXT", 1), new Func("TRANSLATE", 3),
            new Func("CASEWHEN", 3), new Func("COALESCE", 1, true), new Func("CURRENT_SCHEMA", 0),
            new Func("CURRENT_CATALOG", 0), new Func("DATABASE_PATH", 0), new Func("DECODE", 3, true),
            new Func("GREATEST", 2, true), new Func("IFNULL", 2), new Func("LEAST", 2, true), new Func("LOCK_MODE", 0),
            new Func("LOCK_TIMEOUT", 0), new Func("NULLIF", 2), new Func("NVL2", 3), new Func("READONLY", 0),
            new Func("SESSION_ID", 0), new Func("TRUNCATE_VALUE", 3), new Func("USER", 0)
            // TODO: time and date function
            // TODO: array functions
            // TODO JSON functions
    };

    String functionName;
    List<Expression> expressions;

    public H2Function(Generator gen) {
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
        String expressionsText = expressions.stream().map(e -> "(" + e.print() + ")").collect(Collectors.joining(", "));

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
