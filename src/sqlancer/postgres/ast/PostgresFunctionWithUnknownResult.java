package sqlancer.postgres.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

public enum PostgresFunctionWithUnknownResult {

    ABBREV("abbrev", PostgresDataType.TEXT, PostgresDataType.INET),
    BROADCAST("broadcast", PostgresDataType.INET, PostgresDataType.INET),
    FAMILY("family", PostgresDataType.INT, PostgresDataType.INET),
    HOSTMASK("hostmask", PostgresDataType.INET, PostgresDataType.INET),
    MASKLEN("masklen", PostgresDataType.INT, PostgresDataType.INET),
    NETMASK("netmask", PostgresDataType.INET, PostgresDataType.INET),
    SET_MASKLEN("set_masklen", PostgresDataType.INET, PostgresDataType.INET, PostgresDataType.INT),
    TEXT("text", PostgresDataType.TEXT, PostgresDataType.INET),
    INET_SAME_FAMILY("inet_same_family", PostgresDataType.BOOLEAN, PostgresDataType.INET, PostgresDataType.INET),

    // https://www.postgresql.org/docs/devel/functions-admin.html#FUNCTIONS-ADMIN-SIGNAL-TABLE
    // PG_RELOAD_CONF("pg_reload_conf", PostgresDataType.BOOLEAN), // too much output
    // PG_ROTATE_LOGFILE("pg_rotate_logfile", PostgresDataType.BOOLEAN), prints warning

    // https://www.postgresql.org/docs/devel/functions-info.html#FUNCTIONS-INFO-SESSION-TABLE
    CURRENT_DATABASE("current_database", PostgresDataType.TEXT), // name
    // CURRENT_QUERY("current_query", PostgresDataType.TEXT), // can generate false positives
    CURRENT_SCHEMA("current_schema", PostgresDataType.TEXT), // name
    // CURRENT_SCHEMAS("current_schemas", PostgresDataType.TEXT, PostgresDataType.BOOLEAN),
    INET_CLIENT_PORT("inet_client_port", PostgresDataType.INT),
    // INET_SERVER_PORT("inet_server_port", PostgresDataType.INT),
    PG_BACKEND_PID("pg_backend_pid", PostgresDataType.INT),
    PG_CURRENT_LOGFILE("pg_current_logfile", PostgresDataType.TEXT),
    PG_IS_OTHER_TEMP_SCHEMA("pg_is_other_temp_schema", PostgresDataType.BOOLEAN),
    PG_JIT_AVAILABLE("pg_jit_available", PostgresDataType.BOOLEAN),
    PG_NOTIFICATION_QUEUE_USAGE("pg_notification_queue_usage", PostgresDataType.REAL),
    PG_TRIGGER_DEPTH("pg_trigger_depth", PostgresDataType.INT), VERSION("version", PostgresDataType.TEXT),

    //
    TO_CHAR("to_char", PostgresDataType.TEXT, PostgresDataType.TEXT, PostgresDataType.TEXT) {
        @Override
        public PostgresExpression[] getArguments(PostgresDataType returnType, PostgresExpressionGenerator gen,
                int depth) {
            PostgresExpression[] args = super.getArguments(returnType, gen, depth);
            args[0] = gen.generateExpression(PostgresDataType.getRandomType());
            return args;
        }
    },

    // String functions
    ASCII("ascii", PostgresDataType.INT, PostgresDataType.TEXT),
    BTRIM("btrim", PostgresDataType.TEXT, PostgresDataType.TEXT, PostgresDataType.TEXT),
    CHR("chr", PostgresDataType.TEXT, PostgresDataType.INT),
    CONVERT_FROM("convert_from", PostgresDataType.TEXT, PostgresDataType.TEXT, PostgresDataType.TEXT) {
        @Override
        public PostgresExpression[] getArguments(PostgresDataType returnType, PostgresExpressionGenerator gen,
                int depth) {
            PostgresExpression[] args = super.getArguments(returnType, gen, depth);
            args[1] = PostgresConstant.createTextConstant(Randomly.fromOptions("UTF8", "LATIN1"));
            return args;
        }
    },
    // concat
    // segfault
    // BIT_LENGTH("bit_length", PostgresDataType.INT, PostgresDataType.TEXT),
    INITCAP("initcap", PostgresDataType.TEXT, PostgresDataType.TEXT),
    LEFT("left", PostgresDataType.TEXT, PostgresDataType.INT, PostgresDataType.TEXT),
    LOWER("lower", PostgresDataType.TEXT, PostgresDataType.TEXT),
    MD5("md5", PostgresDataType.TEXT, PostgresDataType.TEXT),
    UPPER("upper", PostgresDataType.TEXT, PostgresDataType.TEXT),
    // PG_CLIENT_ENCODING("pg_client_encoding", PostgresDataType.TEXT),
    QUOTE_LITERAL("quote_literal", PostgresDataType.TEXT, PostgresDataType.TEXT),
    QUOTE_IDENT("quote_ident", PostgresDataType.TEXT, PostgresDataType.TEXT),
    REGEX_REPLACE("regex_replace", PostgresDataType.TEXT, PostgresDataType.TEXT, PostgresDataType.TEXT),
    // REPEAT("repeat", PostgresDataType.TEXT, PostgresDataType.TEXT,
    // PostgresDataType.INT),
    REPLACE("replace", PostgresDataType.TEXT, PostgresDataType.TEXT, PostgresDataType.TEXT),
    REVERSE("reverse", PostgresDataType.TEXT, PostgresDataType.TEXT),
    RIGHT("right", PostgresDataType.TEXT, PostgresDataType.TEXT, PostgresDataType.INT),
    RPAD("rpad", PostgresDataType.TEXT, PostgresDataType.INT, PostgresDataType.TEXT),
    RTRIM("rtrim", PostgresDataType.TEXT, PostgresDataType.TEXT),
    SPLIT_PART("split_part", PostgresDataType.TEXT, PostgresDataType.TEXT, PostgresDataType.INT),
    STRPOS("strpos", PostgresDataType.INT, PostgresDataType.TEXT, PostgresDataType.TEXT),
    SUBSTR("substr", PostgresDataType.TEXT, PostgresDataType.TEXT, PostgresDataType.INT, PostgresDataType.INT),
    TO_ASCII("to_ascii", PostgresDataType.TEXT, PostgresDataType.TEXT),
    TO_HEX("to_hex", PostgresDataType.INT, PostgresDataType.TEXT),
    TRANSLATE("translate", PostgresDataType.TEXT, PostgresDataType.TEXT, PostgresDataType.TEXT, PostgresDataType.TEXT),
    // mathematical functions
    // https://www.postgresql.org/docs/9.5/functions-math.html
    ABS("abs", PostgresDataType.REAL, PostgresDataType.REAL),
    CBRT("cbrt", PostgresDataType.REAL, PostgresDataType.REAL), CEILING("ceiling", PostgresDataType.REAL), //
    DEGREES("degrees", PostgresDataType.REAL), EXP("exp", PostgresDataType.REAL), LN("ln", PostgresDataType.REAL),
    LOG("log", PostgresDataType.REAL), LOG2("log", PostgresDataType.REAL, PostgresDataType.REAL),
    PI("pi", PostgresDataType.REAL), POWER("power", PostgresDataType.REAL, PostgresDataType.REAL),
    TRUNC("trunc", PostgresDataType.REAL, PostgresDataType.INT),
    TRUNC2("trunc", PostgresDataType.REAL, PostgresDataType.INT, PostgresDataType.REAL),
    FLOOR("floor", PostgresDataType.REAL),

    // trigonometric functions - complete
    // https://www.postgresql.org/docs/12/functions-math.html#FUNCTIONS-MATH-TRIG-TABLE
    ACOS("acos", PostgresDataType.REAL), //
    ACOSD("acosd", PostgresDataType.REAL), //
    ASIN("asin", PostgresDataType.REAL), //
    ASIND("asind", PostgresDataType.REAL), //
    ATAN("atan", PostgresDataType.REAL), //
    ATAND("atand", PostgresDataType.REAL), //
    ATAN2("atan2", PostgresDataType.REAL, PostgresDataType.REAL), //
    ATAN2D("atan2d", PostgresDataType.REAL, PostgresDataType.REAL), //
    COS("cos", PostgresDataType.REAL), //
    COSD("cosd", PostgresDataType.REAL), //
    COT("cot", PostgresDataType.REAL), //
    COTD("cotd", PostgresDataType.REAL), //
    SIN("sin", PostgresDataType.REAL), //
    SIND("sind", PostgresDataType.REAL), //
    TAN("tan", PostgresDataType.REAL), //
    TAND("tand", PostgresDataType.REAL), //

    // hyperbolic functions - complete
    // https://www.postgresql.org/docs/12/functions-math.html#FUNCTIONS-MATH-HYP-TABLE
    SINH("sinh", PostgresDataType.REAL), //
    COSH("cosh", PostgresDataType.REAL), //
    TANH("tanh", PostgresDataType.REAL), //
    ASINH("asinh", PostgresDataType.REAL), //
    ACOSH("acosh", PostgresDataType.REAL), //
    ATANH("atanh", PostgresDataType.REAL), //

    // https://www.postgresql.org/docs/devel/functions-binarystring.html
    GET_BIT("get_bit", PostgresDataType.INT, PostgresDataType.TEXT, PostgresDataType.INT),
    GET_BYTE("get_byte", PostgresDataType.INT, PostgresDataType.TEXT, PostgresDataType.INT),

    // range functions
    // https://www.postgresql.org/docs/devel/functions-range.html#RANGE-FUNCTIONS-TABLE
    RANGE_LOWER("lower", PostgresDataType.INT, PostgresDataType.RANGE), //
    RANGE_UPPER("upper", PostgresDataType.INT, PostgresDataType.RANGE), //
    RANGE_ISEMPTY("isempty", PostgresDataType.BOOLEAN, PostgresDataType.RANGE), //
    RANGE_LOWER_INC("lower_inc", PostgresDataType.BOOLEAN, PostgresDataType.RANGE), //
    RANGE_UPPER_INC("upper_inc", PostgresDataType.BOOLEAN, PostgresDataType.RANGE), //
    RANGE_LOWER_INF("lower_inf", PostgresDataType.BOOLEAN, PostgresDataType.RANGE), //
    RANGE_UPPER_INF("upper_inf", PostgresDataType.BOOLEAN, PostgresDataType.RANGE), //
    RANGE_MERGE("range_merge", PostgresDataType.RANGE, PostgresDataType.RANGE, PostgresDataType.RANGE), //

    // https://www.postgresql.org/docs/devel/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE
    GET_COLUMN_SIZE("get_column_size", PostgresDataType.INT, PostgresDataType.TEXT);
    // PG_DATABASE_SIZE("pg_database_size", PostgresDataType.INT, PostgresDataType.INT);
    // PG_SIZE_BYTES("pg_size_bytes", PostgresDataType.INT, PostgresDataType.TEXT);

    private String functionName;
    private PostgresDataType returnType;
    private PostgresDataType[] argTypes;

    PostgresFunctionWithUnknownResult(String functionName, PostgresDataType returnType, PostgresDataType... indexType) {
        this.functionName = functionName;
        this.returnType = returnType;
        this.argTypes = indexType.clone();

    }

    public boolean isCompatibleWithReturnType(PostgresDataType t) {
        return t == returnType;
    }

    public PostgresExpression[] getArguments(PostgresDataType returnType, PostgresExpressionGenerator gen, int depth) {
        PostgresExpression[] args = new PostgresExpression[argTypes.length];
        for (int i = 0; i < args.length; i++) {
            args[i] = gen.generateExpression(depth, argTypes[i]);
        }
        return args;

    }

    public String getName() {
        return functionName;
    }

    public static List<PostgresFunctionWithUnknownResult> getSupportedFunctions(PostgresDataType type) {
        List<PostgresFunctionWithUnknownResult> functions = new ArrayList<>();
        for (PostgresFunctionWithUnknownResult func : values()) {
            if (func.isCompatibleWithReturnType(type)) {
                functions.add(func);
            }
        }
        return functions;
    }

}
