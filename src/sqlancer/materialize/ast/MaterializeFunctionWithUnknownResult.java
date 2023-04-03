package sqlancer.materialize.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.gen.MaterializeExpressionGenerator;

public enum MaterializeFunctionWithUnknownResult {

    CURRENT_DATABASE("current_database", MaterializeDataType.TEXT), // name
    CURRENT_SCHEMA("current_schema", MaterializeDataType.TEXT), // name
    PG_BACKEND_PID("pg_backend_pid", MaterializeDataType.INT),
    PG_CURRENT_LOGFILE("pg_current_logfile", MaterializeDataType.TEXT),
    PG_IS_OTHER_TEMP_SCHEMA("pg_is_other_temp_schema", MaterializeDataType.BOOLEAN),
    PG_JIT_AVAILABLE("pg_jit_available", MaterializeDataType.BOOLEAN),
    PG_NOTIFICATION_QUEUE_USAGE("pg_notification_queue_usage", MaterializeDataType.REAL),
    PG_TRIGGER_DEPTH("pg_trigger_depth", MaterializeDataType.INT), VERSION("version", MaterializeDataType.TEXT),

    //
    TO_CHAR("to_char", MaterializeDataType.TEXT, MaterializeDataType.TEXT, MaterializeDataType.TEXT) {
        @Override
        public MaterializeExpression[] getArguments(MaterializeDataType returnType, MaterializeExpressionGenerator gen,
                int depth) {
            MaterializeExpression[] args = super.getArguments(returnType, gen, depth);
            args[0] = gen.generateExpression(MaterializeDataType.getRandomType());
            return args;
        }
    },

    // String functions
    ASCII("ascii", MaterializeDataType.INT, MaterializeDataType.TEXT),
    BTRIM("btrim", MaterializeDataType.TEXT, MaterializeDataType.TEXT, MaterializeDataType.TEXT),
    CHR("chr", MaterializeDataType.TEXT, MaterializeDataType.INT),
    CONVERT_FROM("convert_from", MaterializeDataType.TEXT, MaterializeDataType.TEXT, MaterializeDataType.TEXT) {
        @Override
        public MaterializeExpression[] getArguments(MaterializeDataType returnType, MaterializeExpressionGenerator gen,
                int depth) {
            MaterializeExpression[] args = super.getArguments(returnType, gen, depth);
            args[1] = MaterializeConstant.createTextConstant(Randomly.fromOptions("UTF8", "LATIN1"));
            return args;
        }
    },
    INITCAP("initcap", MaterializeDataType.TEXT, MaterializeDataType.TEXT),
    LEFT("left", MaterializeDataType.TEXT, MaterializeDataType.INT, MaterializeDataType.TEXT),
    LOWER("lower", MaterializeDataType.TEXT, MaterializeDataType.TEXT),
    MD5("md5", MaterializeDataType.TEXT, MaterializeDataType.TEXT),
    UPPER("upper", MaterializeDataType.TEXT, MaterializeDataType.TEXT),
    QUOTE_LITERAL("quote_literal", MaterializeDataType.TEXT, MaterializeDataType.TEXT),
    QUOTE_IDENT("quote_ident", MaterializeDataType.TEXT, MaterializeDataType.TEXT),
    REGEX_REPLACE("regex_replace", MaterializeDataType.TEXT, MaterializeDataType.TEXT, MaterializeDataType.TEXT),
    REPLACE("replace", MaterializeDataType.TEXT, MaterializeDataType.TEXT, MaterializeDataType.TEXT),
    REVERSE("reverse", MaterializeDataType.TEXT, MaterializeDataType.TEXT),
    RIGHT("right", MaterializeDataType.TEXT, MaterializeDataType.TEXT, MaterializeDataType.INT),
    RPAD("rpad", MaterializeDataType.TEXT, MaterializeDataType.INT, MaterializeDataType.TEXT),
    RTRIM("rtrim", MaterializeDataType.TEXT, MaterializeDataType.TEXT),
    SPLIT_PART("split_part", MaterializeDataType.TEXT, MaterializeDataType.TEXT, MaterializeDataType.INT),
    STRPOS("strpos", MaterializeDataType.INT, MaterializeDataType.TEXT, MaterializeDataType.TEXT),
    SUBSTR("substr", MaterializeDataType.TEXT, MaterializeDataType.TEXT, MaterializeDataType.INT,
            MaterializeDataType.INT),
    TO_ASCII("to_ascii", MaterializeDataType.TEXT, MaterializeDataType.TEXT),
    TO_HEX("to_hex", MaterializeDataType.INT, MaterializeDataType.TEXT),
    TRANSLATE("translate", MaterializeDataType.TEXT, MaterializeDataType.TEXT, MaterializeDataType.TEXT,
            MaterializeDataType.TEXT),
    // mathematical functions
    ABS("abs", MaterializeDataType.REAL, MaterializeDataType.REAL),
    CBRT("cbrt", MaterializeDataType.REAL, MaterializeDataType.REAL), CEILING("ceiling", MaterializeDataType.REAL), //
    DEGREES("degrees", MaterializeDataType.REAL), EXP("exp", MaterializeDataType.REAL),
    LN("ln", MaterializeDataType.REAL), LOG("log", MaterializeDataType.REAL),
    LOG2("log", MaterializeDataType.REAL, MaterializeDataType.REAL), PI("pi", MaterializeDataType.REAL),
    POWER("power", MaterializeDataType.REAL, MaterializeDataType.REAL),
    TRUNC("trunc", MaterializeDataType.REAL, MaterializeDataType.INT),
    TRUNC2("trunc", MaterializeDataType.REAL, MaterializeDataType.INT, MaterializeDataType.REAL),
    FLOOR("floor", MaterializeDataType.REAL),

    // trigonometric functions - complete
    ACOS("acos", MaterializeDataType.REAL), //
    ACOSD("acosd", MaterializeDataType.REAL), //
    ASIN("asin", MaterializeDataType.REAL), //
    ASIND("asind", MaterializeDataType.REAL), //
    ATAN("atan", MaterializeDataType.REAL), //
    ATAND("atand", MaterializeDataType.REAL), //
    ATAN2("atan2", MaterializeDataType.REAL, MaterializeDataType.REAL), //
    ATAN2D("atan2d", MaterializeDataType.REAL, MaterializeDataType.REAL), //
    COS("cos", MaterializeDataType.REAL), //
    COSD("cosd", MaterializeDataType.REAL), //
    COT("cot", MaterializeDataType.REAL), //
    COTD("cotd", MaterializeDataType.REAL), //
    SIN("sin", MaterializeDataType.REAL), //
    SIND("sind", MaterializeDataType.REAL), //
    TAN("tan", MaterializeDataType.REAL), //
    TAND("tand", MaterializeDataType.REAL), //

    // hyperbolic functions - complete
    SINH("sinh", MaterializeDataType.REAL), //
    COSH("cosh", MaterializeDataType.REAL), //
    TANH("tanh", MaterializeDataType.REAL), //
    ASINH("asinh", MaterializeDataType.REAL), //
    ACOSH("acosh", MaterializeDataType.REAL), //
    ATANH("atanh", MaterializeDataType.REAL), //

    GET_BIT("get_bit", MaterializeDataType.INT, MaterializeDataType.TEXT, MaterializeDataType.INT),
    GET_BYTE("get_byte", MaterializeDataType.INT, MaterializeDataType.TEXT, MaterializeDataType.INT),

    GET_COLUMN_SIZE("get_column_size", MaterializeDataType.INT, MaterializeDataType.TEXT);

    private String functionName;
    private MaterializeDataType returnType;
    private MaterializeDataType[] argTypes;

    MaterializeFunctionWithUnknownResult(String functionName, MaterializeDataType returnType,
            MaterializeDataType... indexType) {
        this.functionName = functionName;
        this.returnType = returnType;
        this.argTypes = indexType.clone();

    }

    public boolean isCompatibleWithReturnType(MaterializeDataType t) {
        return t == returnType;
    }

    public MaterializeExpression[] getArguments(MaterializeDataType returnType, MaterializeExpressionGenerator gen,
            int depth) {
        MaterializeExpression[] args = new MaterializeExpression[argTypes.length];
        for (int i = 0; i < args.length; i++) {
            args[i] = gen.generateExpression(depth, argTypes[i]);
        }
        return args;

    }

    public String getName() {
        return functionName;
    }

    public static List<MaterializeFunctionWithUnknownResult> getSupportedFunctions(MaterializeDataType type) {
        List<MaterializeFunctionWithUnknownResult> functions = new ArrayList<>();
        for (MaterializeFunctionWithUnknownResult func : values()) {
            if (func.isCompatibleWithReturnType(type)) {
                functions.add(func);
            }
        }
        return functions;
    }

}
