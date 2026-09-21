package sqlancer.sqlite3.dialect.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.simple.expression.Expression;
import sqlancer.simple.gen.Generator;
import sqlancer.simple.gen.Signal;

public class SQLite3Function implements Expression {

    static Func[] functions = { new Func("ABS", 1), new Func("CHANGES", 0), new Func("CHAR", 1, true),
            new Func("COALESCE", 2, true), new Func("GLOB", 2), new Func("HEX", 1), new Func("IFNULL", 2),
            new Func("INSTR", 2), new Func("LAST_INSERT_ROWID", 0), new Func("LENGTH", 1), new Func("LIKE", 2),
            new Func("LIKE", 3), new Func("LIKELIHOOD", 2), new Func("LIKELY", 1), new Func("load_extension", 1),
            new Func("load_extension", 2), new Func("LOWER", 1), new Func("LTRIM", 1), new Func("LTRIM", 2),
            new Func("MAX", 2, true), new Func("MIN", 2, true), new Func("NULLIF", 2), new Func("PRINTF", 1, true),
            new Func("QUOTE", 1), new Func("ROUND", 2), new Func("RTRIM", 1), new Func("soundex", 1),
            new Func("SQLITE_COMPILEOPTION_GET", 1), new Func("SQLITE_COMPILEOPTION_USED", 1),
            new Func("SQLITE_SOURCE_ID", 0), new Func("SQLITE_VERSION", 0), new Func("SUBSTR", 2),
            new Func("TOTAL_CHANGES", 0), new Func("TRIM", 1), new Func("TYPEOF", 1), new Func("UNICODE", 1),
            new Func("UNLIKELY", 1), new Func("UPPER", 1), new Func("DATE", 3, true), new Func("TIME", 3, true),
            new Func("DATETIME", 3, true), new Func("JULIANDAY", 3, true), new Func("STRFTIME", 3, true),
            new Func("json", 1), new Func("json_array", 2, true), new Func("json_array_length", 1),
            new Func("json_array_length", 2), new Func("json_extract", 2, true), new Func("json_insert", 3, true),
            new Func("json_object", 2, true), new Func("json_patch", 2), new Func("json_remove", 2, true),
            new Func("json_type", 1), new Func("json_valid", 1), new Func("json_quote", 1), new Func("rtreenode", 2),
            new Func("highlight", 4)

    };

    String functionName;
    List<Expression> expressions;

    public SQLite3Function(Generator gen) {
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
