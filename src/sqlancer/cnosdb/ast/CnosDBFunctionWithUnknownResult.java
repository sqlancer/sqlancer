package sqlancer.cnosdb.ast;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import sqlancer.cnosdb.CnosDBBugs;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.gen.CnosDBExpressionGenerator;

public enum CnosDBFunctionWithUnknownResult {

    // String functions
    ASCII("ascii", CnosDBDataType.INT, CnosDBDataType.STRING),
    BTRIM("btrim", CnosDBDataType.STRING, CnosDBDataType.STRING, CnosDBDataType.STRING),
    CHAR_LENGTH("char_length", CnosDBDataType.INT, CnosDBDataType.STRING),
    CHARACTER_LENGTH("character_length", CnosDBDataType.INT, CnosDBDataType.STRING),
    CONCAT("concat", CnosDBDataType.STRING, CnosDBDataType.STRING, CnosDBDataType.STRING),
    CONCAT_WS("concat_ws", CnosDBDataType.STRING, CnosDBDataType.STRING, CnosDBDataType.STRING),
    CHR("chr", CnosDBDataType.STRING, CnosDBDataType.INT),
    BIT_LENGTH("bit_length", CnosDBDataType.INT, CnosDBDataType.STRING),
    INITCAP("initcap", CnosDBDataType.STRING, CnosDBDataType.STRING),

    LEFT("left", CnosDBDataType.STRING, CnosDBDataType.STRING, CnosDBDataType.INT),
    LENGTH("length", CnosDBDataType.UINT, CnosDBDataType.STRING),
    LOWER("lower", CnosDBDataType.STRING, CnosDBDataType.STRING),
    LPAD3("lpad", CnosDBDataType.STRING, CnosDBDataType.STRING, CnosDBDataType.INT, CnosDBDataType.STRING),
    LPAD2("lpad", CnosDBDataType.STRING, CnosDBDataType.STRING, CnosDBDataType.INT),
    RPAD3("rpad", CnosDBDataType.STRING, CnosDBDataType.STRING, CnosDBDataType.INT, CnosDBDataType.STRING),
    RPAD2("rpad", CnosDBDataType.STRING, CnosDBDataType.STRING, CnosDBDataType.INT),
    LTRIM("ltrim", CnosDBDataType.STRING, CnosDBDataType.STRING, CnosDBDataType.STRING),
    OCTET_LENGTH("octet_length", CnosDBDataType.INT, CnosDBDataType.STRING),
    // REPEAT("repeat", CnosDBDataType.STRING, CnosDBDataType.STRING, CnosDBDataType.INT),
    REPLACE("replace", CnosDBDataType.STRING, CnosDBDataType.STRING, CnosDBDataType.STRING, CnosDBDataType.STRING),
    REVERSE("reverse", CnosDBDataType.STRING, CnosDBDataType.STRING),
    RIGHT("right", CnosDBDataType.STRING, CnosDBDataType.STRING, CnosDBDataType.INT),
    RTRIM("rtrim", CnosDBDataType.STRING, CnosDBDataType.STRING),
    SPLIT_PART("split_part", CnosDBDataType.STRING, CnosDBDataType.STRING, CnosDBDataType.STRING, CnosDBDataType.INT),
    STARTS_WITH("starts_with", CnosDBDataType.BOOLEAN, CnosDBDataType.STRING, CnosDBDataType.STRING),
    STRPOS("strpos", CnosDBDataType.INT, CnosDBDataType.STRING, CnosDBDataType.STRING),
    SUBSTR("substr", CnosDBDataType.STRING, CnosDBDataType.STRING, CnosDBDataType.INT, CnosDBDataType.INT),
    TRANSLATE("translate", CnosDBDataType.STRING, CnosDBDataType.STRING, CnosDBDataType.STRING, CnosDBDataType.STRING),

    UPPER("upper", CnosDBDataType.STRING, CnosDBDataType.STRING),

    MD5("md5", CnosDBDataType.STRING, CnosDBDataType.STRING),

    // mathematical functions
    ABS("abs", CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE),
    CEIL("ceil", CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE),
    EXP("exp", CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE), LN("ln", CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE),
    LOG2("log2", CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE),
    LOG10("log10", CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE),
    POWER("power", CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE),
    ROUND("round", CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE),
    TRUNC("trunc", CnosDBDataType.DOUBLE, CnosDBDataType.INT),
    FLOOR("floor", CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE),
    SIGNUM("signum", CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE),
    ACOS("acos", CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE),
    ASIN("asin", CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE),
    ATAN2("atan2", CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE),
    COS("cos", CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE), SIN("sin", CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE),
    SQRT("sqrt", CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE),
    TAN("tan", CnosDBDataType.DOUBLE, CnosDBDataType.DOUBLE),
    DATE_PART("date_part", CnosDBDataType.INT, CnosDBDataType.STRING, CnosDBDataType.TIMESTAMP),
    TO_TIMESTAMP("to_timestamp", CnosDBDataType.TIMESTAMP, CnosDBDataType.INT),
    TO_TIMESTAMP_MILLIS("to_timestamp_millis", CnosDBDataType.TIMESTAMP, CnosDBDataType.INT),
    TO_TIMESTAMP_MICROS("to_timestamp_micros", CnosDBDataType.TIMESTAMP, CnosDBDataType.INT),
    TO_TIMESTAMP_SECONDS("to_timestamp_seconds", CnosDBDataType.TIMESTAMP, CnosDBDataType.INT),
    DATA_TRUNC("date_trunc", CnosDBDataType.TIMESTAMP, CnosDBDataType.STRING, CnosDBDataType.TIMESTAMP);

    private final String functionName;
    private final CnosDBDataType returnType;
    private final CnosDBDataType[] argTypes;

    CnosDBFunctionWithUnknownResult(String functionName, CnosDBDataType returnType, CnosDBDataType... indexType) {
        this.functionName = functionName;
        this.returnType = returnType;
        this.argTypes = indexType.clone();

    }

    public static List<CnosDBFunctionWithUnknownResult> getSupportedFunctions(CnosDBDataType type) {
        List<CnosDBFunctionWithUnknownResult> res = Stream.of(values())
                .filter(function -> function.isCompatibleWithReturnType(type)).collect(Collectors.toList());
        if (CnosDBBugs.bug3547) {
            res.removeAll(
                    List.of(DATA_TRUNC, TO_TIMESTAMP, TO_TIMESTAMP_MICROS, TO_TIMESTAMP_MILLIS, TO_TIMESTAMP_SECONDS));
        }
        return res;
    }

    public boolean isCompatibleWithReturnType(CnosDBDataType t) {
        return t == returnType;
    }

    public CnosDBExpression[] getArguments(CnosDBDataType ignore, CnosDBExpressionGenerator gen, int depth) {
        CnosDBExpression[] args = new CnosDBExpression[argTypes.length];
        for (int i = 0; i < args.length; i++) {
            args[i] = gen.generateExpression(depth, argTypes[i]);
        }
        return args;
    }

    public String getName() {
        return functionName;
    }

}
