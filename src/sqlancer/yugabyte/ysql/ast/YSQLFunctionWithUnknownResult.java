package sqlancer.yugabyte.ysql.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.gen.YSQLExpressionGenerator;

public enum YSQLFunctionWithUnknownResult {

    ABBREV("abbrev", YSQLDataType.TEXT, YSQLDataType.INET),
    BROADCAST("broadcast", YSQLDataType.INET, YSQLDataType.INET), FAMILY("family", YSQLDataType.INT, YSQLDataType.INET),
    HOSTMASK("hostmask", YSQLDataType.INET, YSQLDataType.INET), MASKLEN("masklen", YSQLDataType.INT, YSQLDataType.INET),
    NETMASK("netmask", YSQLDataType.INET, YSQLDataType.INET),
    SET_MASKLEN("set_masklen", YSQLDataType.INET, YSQLDataType.INET, YSQLDataType.INT),
    TEXT("text", YSQLDataType.TEXT, YSQLDataType.INET),
    INET_SAME_FAMILY("inet_same_family", YSQLDataType.BOOLEAN, YSQLDataType.INET, YSQLDataType.INET),

    // https://www.postgres.org/docs/devel/functions-admin.html#FUNCTIONS-ADMIN-SIGNAL-TABLE
    // PG_RELOAD_CONF("pg_reload_conf", YSQLDataType.BOOLEAN), // too much output
    // PG_ROTATE_LOGFILE("pg_rotate_logfile", YSQLDataType.BOOLEAN), prints warning

    // https://www.postgresql.org/docs/devel/functions-info.html#FUNCTIONS-INFO-SESSION-TABLE
    CURRENT_DATABASE("current_database", YSQLDataType.TEXT), // name
    // CURRENT_QUERY("current_query", YSQLDataType.TEXT), // can generate false positives
    CURRENT_SCHEMA("current_schema", YSQLDataType.TEXT), // name
    // CURRENT_SCHEMAS("current_schemas", YSQLDataType.TEXT, YSQLDataType.BOOLEAN),
    INET_CLIENT_PORT("inet_client_port", YSQLDataType.INT), INET_SERVER_PORT("inet_server_port", YSQLDataType.INT),
    PG_BACKEND_PID("pg_backend_pid", YSQLDataType.INT), PG_CURRENT_LOGFILE("pg_current_logfile", YSQLDataType.TEXT),
    // PG_IS_OTHER_TEMP_SCHEMA("pg_is_other_temp_schema", YSQLDataType.BOOLEAN),
    // PG_JIT_AVAILABLE("pg_is_other_temp_schema", YSQLDataType.BOOLEAN),
    PG_NOTIFICATION_QUEUE_USAGE("pg_notification_queue_usage", YSQLDataType.REAL),
    PG_TRIGGER_DEPTH("pg_trigger_depth", YSQLDataType.INT), VERSION("version", YSQLDataType.TEXT),

    //
    TO_CHAR("to_char", YSQLDataType.TEXT, YSQLDataType.BYTEA, YSQLDataType.TEXT) {
        @Override
        public YSQLExpression[] getArguments(YSQLDataType returnType, YSQLExpressionGenerator gen, int depth) {
            YSQLExpression[] args = super.getArguments(returnType, gen, depth);
            args[0] = gen.generateExpression(YSQLDataType.getRandomType());
            return args;
        }
    },

    // String functions
    ASCII("ascii", YSQLDataType.INT, YSQLDataType.TEXT),
    BTRIM("btrim", YSQLDataType.TEXT, YSQLDataType.TEXT, YSQLDataType.TEXT),
    CHR("chr", YSQLDataType.TEXT, YSQLDataType.INT),
    CONVERT_FROM("convert_from", YSQLDataType.TEXT, YSQLDataType.TEXT, YSQLDataType.TEXT) {
        @Override
        public YSQLExpression[] getArguments(YSQLDataType returnType, YSQLExpressionGenerator gen, int depth) {
            YSQLExpression[] args = super.getArguments(returnType, gen, depth);
            args[1] = YSQLConstant.createTextConstant("UTF8");
            return args;
        }
    },
    // concat
    // segfault
    BIT_LENGTH("bit_length", YSQLDataType.INT, YSQLDataType.BYTEA),
    INITCAP("initcap", YSQLDataType.TEXT, YSQLDataType.TEXT),
    LEFT("left", YSQLDataType.TEXT, YSQLDataType.INT, YSQLDataType.TEXT),
    LOWER("lower", YSQLDataType.TEXT, YSQLDataType.TEXT), MD5("md5", YSQLDataType.TEXT, YSQLDataType.TEXT),
    UPPER("upper", YSQLDataType.TEXT, YSQLDataType.TEXT),
    // PG_CLIENT_ENCODING("pg_client_encoding", YSQLDataType.TEXT),
    QUOTE_LITERAL("quote_literal", YSQLDataType.TEXT, YSQLDataType.TEXT),
    QUOTE_IDENT("quote_ident", YSQLDataType.TEXT, YSQLDataType.TEXT),
    REGEX_REPLACE("regexp_replace", YSQLDataType.TEXT, YSQLDataType.TEXT, YSQLDataType.TEXT, YSQLDataType.TEXT),
    // todo mute repeat function because it may provide OOMs
    // REPEAT("repeat", YSQLDataType.TEXT, YSQLDataType.TEXT, YSQLDataType.INT),
    REPLACE("replace", YSQLDataType.TEXT, YSQLDataType.TEXT, YSQLDataType.TEXT, YSQLDataType.TEXT),
    REVERSE("reverse", YSQLDataType.TEXT, YSQLDataType.TEXT),
    RIGHT("right", YSQLDataType.TEXT, YSQLDataType.TEXT, YSQLDataType.INT),
    RPAD("rpad", YSQLDataType.TEXT, YSQLDataType.INT, YSQLDataType.TEXT),
    RTRIM("rtrim", YSQLDataType.TEXT, YSQLDataType.TEXT),
    SPLIT_PART("split_part", YSQLDataType.TEXT, YSQLDataType.TEXT, YSQLDataType.INT),
    STRPOS("strpos", YSQLDataType.INT, YSQLDataType.TEXT, YSQLDataType.TEXT),
    SUBSTR("substr", YSQLDataType.TEXT, YSQLDataType.TEXT, YSQLDataType.INT, YSQLDataType.INT),
    TO_ASCII("to_ascii", YSQLDataType.TEXT, YSQLDataType.TEXT), TO_HEX("to_hex", YSQLDataType.INT, YSQLDataType.TEXT),
    TRANSLATE("translate", YSQLDataType.TEXT, YSQLDataType.TEXT, YSQLDataType.TEXT, YSQLDataType.TEXT),
    // mathematical functions
    // https://www.postgresql.org/docs/9.5/functions-math.html
    ABS("abs", YSQLDataType.REAL, YSQLDataType.REAL), CBRT("cbrt", YSQLDataType.REAL, YSQLDataType.REAL),
    CEILING("ceiling", YSQLDataType.REAL), //
    DEGREES("degrees", YSQLDataType.REAL), EXP("exp", YSQLDataType.REAL), LN("ln", YSQLDataType.REAL),
    LOG("log", YSQLDataType.REAL), LOG2("log", YSQLDataType.REAL, YSQLDataType.REAL), PI("pi", YSQLDataType.REAL),
    POWER("power", YSQLDataType.REAL, YSQLDataType.REAL), TRUNC("trunc", YSQLDataType.REAL, YSQLDataType.INT),
    TRUNC2("trunc", YSQLDataType.REAL, YSQLDataType.INT, YSQLDataType.REAL), FLOOR("floor", YSQLDataType.REAL),

    // trigonometric functions - complete
    // https://www.postgresql.org/docs/12/functions-math.html#FUNCTIONS-MATH-TRIG-TABLE
    ACOS("acos", YSQLDataType.REAL), //
    ACOSD("acosd", YSQLDataType.REAL), //
    ASIN("asin", YSQLDataType.REAL), //
    ASIND("asind", YSQLDataType.REAL), //
    ATAN("atan", YSQLDataType.REAL), //
    ATAND("atand", YSQLDataType.REAL), //
    ATAN2("atan2", YSQLDataType.REAL, YSQLDataType.REAL), //
    ATAN2D("atan2d", YSQLDataType.REAL, YSQLDataType.REAL), //
    COS("cos", YSQLDataType.REAL), //
    COSD("cosd", YSQLDataType.REAL), //
    COT("cot", YSQLDataType.REAL), //
    COTD("cotd", YSQLDataType.REAL), //
    SIN("sin", YSQLDataType.REAL), //
    SIND("sind", YSQLDataType.REAL), //
    TAN("tan", YSQLDataType.REAL), //
    TAND("tand", YSQLDataType.REAL), //

    // hyperbolic functions - complete
    // https://www.postgresql.org/docs/12/functions-math.html#FUNCTIONS-MATH-HYP-TABLE
    SINH("sinh", YSQLDataType.REAL), //
    COSH("cosh", YSQLDataType.REAL), //
    TANH("tanh", YSQLDataType.REAL), //
    ASINH("asinh", YSQLDataType.REAL), //
    ACOSH("acosh", YSQLDataType.REAL), //
    ATANH("atanh", YSQLDataType.REAL), //

    // https://www.postgresql.org/docs/devel/functions-binarystring.html
    GET_BIT("get_bit", YSQLDataType.INT, YSQLDataType.TEXT, YSQLDataType.INT),
    GET_BYTE("get_byte", YSQLDataType.INT, YSQLDataType.TEXT, YSQLDataType.INT),

    // range functions
    // https://www.postgresql.org/docs/devel/functions-range.html#RANGE-FUNCTIONS-TABLE
    RANGE_LOWER("lower", YSQLDataType.INT, YSQLDataType.RANGE), //
    RANGE_UPPER("upper", YSQLDataType.INT, YSQLDataType.RANGE), //
    RANGE_ISEMPTY("isempty", YSQLDataType.BOOLEAN, YSQLDataType.RANGE), //
    RANGE_LOWER_INC("lower_inc", YSQLDataType.BOOLEAN, YSQLDataType.RANGE), //
    RANGE_UPPER_INC("upper_inc", YSQLDataType.BOOLEAN, YSQLDataType.RANGE), //
    RANGE_LOWER_INF("lower_inf", YSQLDataType.BOOLEAN, YSQLDataType.RANGE), //
    RANGE_UPPER_INF("upper_inf", YSQLDataType.BOOLEAN, YSQLDataType.RANGE), //
    RANGE_MERGE("range_merge", YSQLDataType.RANGE, YSQLDataType.RANGE, YSQLDataType.RANGE), //

    // https://www.postgresql.org/docs/devel/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE
    GET_COLUMN_SIZE("get_column_size", YSQLDataType.INT, YSQLDataType.TEXT);
    // PG_DATABASE_SIZE("pg_database_size", YSQLDataType.INT, YSQLDataType.INT);
    // PG_SIZE_BYTES("pg_size_bytes", YSQLDataType.INT, YSQLDataType.TEXT);

    private final String functionName;
    private final YSQLDataType returnType;
    private final YSQLDataType[] argTypes;

    YSQLFunctionWithUnknownResult(String functionName, YSQLDataType returnType, YSQLDataType... indexType) {
        this.functionName = functionName;
        this.returnType = returnType;
        this.argTypes = indexType.clone();
    }

    public static List<YSQLFunctionWithUnknownResult> getSupportedFunctions(YSQLDataType type) {
        List<YSQLFunctionWithUnknownResult> functions = new ArrayList<>();
        for (YSQLFunctionWithUnknownResult func : values()) {
            if (func.isCompatibleWithReturnType(type)) {
                functions.add(func);
            }
        }
        return functions;
    }

    public boolean isCompatibleWithReturnType(YSQLDataType t) {
        return t == returnType;
    }

    public YSQLExpression[] getArguments(YSQLDataType returnType, YSQLExpressionGenerator gen, int depth) {
        YSQLExpression[] args = new YSQLExpression[argTypes.length];
        for (int i = 0; i < args.length; i++) {
            args[i] = gen.generateExpression(depth, argTypes[i]);
        }
        return args;

    }

    public String getName() {
        return functionName;
    }

}
