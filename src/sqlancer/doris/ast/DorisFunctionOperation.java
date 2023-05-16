package sqlancer.doris.ast;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.doris.DorisSchema.DorisDataType;
import sqlancer.doris.gen.DorisNewExpressionGenerator;

public class DorisFunctionOperation implements Node<DorisExpression>, DorisExpression {

    private DorisFunction function;
    private List<DorisExpression> args;

    // https://doris.apache.org/zh-CN/docs/dev/summary/basic-summary
    public enum DorisFunction {

        // Array functions, https://doris.apache.org/docs/dev/sql-manual/sql-functions/array-functions/array
        // Skip now

        // Date functions, https://doris.apache.org/docs/dev/sql-manual/sql-functions/date-time-functions/convert_tz/
        CONVERT_TZ(false, DorisDataType.DATETIME, DorisDataType.DATETIME, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        CURDATE(false, DorisDataType.DATE), CURRENT_DATE(false, DorisDataType.DATE),
        CURTIME(false, DorisDataType.VARCHAR), CURRENT_TIME(false, DorisDataType.VARCHAR),
        CURRENT_TIMESTAMP(false, DorisDataType.DATETIME), LOCALTIME(false, DorisDataType.DATETIME),
        LOCALTIMESTAMP(false, DorisDataType.DATETIME), NOW(false, DorisDataType.DATETIME),
        YEAR(false, DorisDataType.INT, DorisDataType.DATETIME),
        QUARTER(false, DorisDataType.INT, DorisDataType.DATETIME),
        MONTH(false, DorisDataType.INT, DorisDataType.DATETIME), DAY(false, DorisDataType.INT, DorisDataType.DATETIME),
        DAYOFYEAR(false, DorisDataType.INT, DorisDataType.DATETIME),
        DAYOFMONTH(false, DorisDataType.INT, DorisDataType.DATETIME),
        DAYOFWEEK(false, DorisDataType.INT, DorisDataType.DATETIME), WEEK(false, DorisDataType.INT, DorisDataType.DATE),
        WEEKDAY(false, DorisDataType.INT, DorisDataType.DATE),
        WEEKOFYEAR(false, DorisDataType.INT, DorisDataType.DATETIME),
        YEARWEEK(false, DorisDataType.INT, DorisDataType.DATE),
        DAYNAME(false, DorisDataType.VARCHAR, DorisDataType.DATETIME),
        MONTHNAME(false, DorisDataType.VARCHAR, DorisDataType.DATETIME),
        HOUR(false, DorisDataType.INT, DorisDataType.DATETIME),
        MINUTE(false, DorisDataType.INT, DorisDataType.DATETIME),
        SECOND(false, DorisDataType.INT, DorisDataType.DATETIME),
        FROM_DAYS(false, DorisDataType.DATE, DorisDataType.INT),
        LAST_DAYS(false, DorisDataType.DATE, DorisDataType.DATETIME),
        TO_MONDAY(false, DorisDataType.DATE, DorisDataType.DATETIME),
        FROM_UNIXTIME(false, DorisDataType.DATETIME, DorisDataType.INT),
        UNIX_TIMESTAMP(false, DorisDataType.INT, DorisDataType.DATETIME), UTC_TIMESTAMP(false, DorisDataType.DATETIME),
        TO_DATE(false, DorisDataType.DATE, DorisDataType.DATETIME),
        TO_DAYS(false, DorisDataType.INT, DorisDataType.DATETIME),
        TIME_TO_SEC(false, DorisDataType.INT, DorisDataType.DATETIME),
        // EXTRACT(1), // select extract(year from '2022-09-22 17:01:30') as year, currently not considered
        MAKEDATE(false, DorisDataType.DATE, DorisDataType.INT, DorisDataType.INT),
        STR_TO_DATE(false, DorisDataType.DATETIME, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        TIME_ROUND(false, DorisDataType.DATETIME, DorisDataType.DATETIME),
        TIME_DIFF(false, DorisDataType.VARCHAR, DorisDataType.DATETIME, DorisDataType.DATETIME),
        TIMESTAMPADD(false, DorisDataType.DATETIME, DorisDataType.VARCHAR, DorisDataType.INT, DorisDataType.DATETIME),
        TIMESTAMPDIFF(false, DorisDataType.VARCHAR, DorisDataType.DATETIME, DorisDataType.DATETIME),
        DATE_ADD(false, DorisDataType.INT, DorisDataType.DATETIME, DorisDataType.VARCHAR),
        DATE_SUB(false, DorisDataType.DATETIME, DorisDataType.DATETIME, DorisDataType.VARCHAR),
        DATE_TRUNC(false, DorisDataType.DATETIME, DorisDataType.DATETIME, DorisDataType.VARCHAR),
        DATE_FORMAT(false, DorisDataType.VARCHAR, DorisDataType.DATETIME, DorisDataType.VARCHAR),
        DATEDIFF(false, DorisDataType.DATETIME, DorisDataType.DATETIME, DorisDataType.DATETIME),
        // MICROSECONDS_ADD(false),
        MINUTES_ADD(false, DorisDataType.DATETIME, DorisDataType.DATETIME, DorisDataType.INT),
        MINUTES_DIFF(false, DorisDataType.INT, DorisDataType.DATETIME, DorisDataType.DATETIME),
        MINUTES_SUB(false, DorisDataType.DATETIME, DorisDataType.DATETIME, DorisDataType.INT),
        SECONDS_ADD(false, DorisDataType.DATETIME, DorisDataType.DATETIME, DorisDataType.INT),
        SECONDS_DIFF(false, DorisDataType.INT, DorisDataType.DATETIME, DorisDataType.DATETIME),
        SECONDS_SUB(false, DorisDataType.DATETIME, DorisDataType.DATETIME, DorisDataType.INT),
        HOURS_ADD(false, DorisDataType.DATETIME, DorisDataType.DATETIME, DorisDataType.INT),
        HOURS_DIFF(false, DorisDataType.INT, DorisDataType.DATETIME, DorisDataType.DATETIME),
        HOURS_SUB(false, DorisDataType.DATETIME, DorisDataType.DATETIME, DorisDataType.INT),
        DAYS_ADD(false, DorisDataType.DATETIME, DorisDataType.DATETIME, DorisDataType.INT),
        DAYS_DIFF(false, DorisDataType.INT, DorisDataType.DATETIME, DorisDataType.DATETIME),
        DAYS_SUB(false, DorisDataType.DATETIME, DorisDataType.DATETIME, DorisDataType.INT),
        WEEKS_ADD(false, DorisDataType.DATETIME, DorisDataType.DATETIME, DorisDataType.INT),
        WEEKS_DIFF(false, DorisDataType.INT, DorisDataType.DATETIME, DorisDataType.DATETIME),
        WEEKS_SUB(false, DorisDataType.DATETIME, DorisDataType.DATETIME, DorisDataType.INT),
        MONTHS_ADD(false, DorisDataType.DATETIME, DorisDataType.DATETIME, DorisDataType.INT),
        MONTHS_DIFF(false, DorisDataType.INT, DorisDataType.DATETIME, DorisDataType.DATETIME),
        MONTHS_SUB(false, DorisDataType.DATETIME, DorisDataType.DATETIME, DorisDataType.INT),
        YEARS_ADD(false, DorisDataType.DATETIME, DorisDataType.DATETIME, DorisDataType.INT),
        YEARS_DIFF(false, DorisDataType.INT, DorisDataType.DATETIME, DorisDataType.DATETIME),
        YEARS_SUB(false, DorisDataType.DATETIME, DorisDataType.DATETIME, DorisDataType.INT),

        // GIS functions, https://doris.apache.org/docs/dev/sql-manual/sql-functions/spatial-functions/st_x
        // Skip now

        // String functions, https://doris.apache.org/docs/dev/sql-manual/sql-functions/string-functions/to_base64
        TO_BASE64(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        FROM_BASE64(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        ASCII(false, DorisDataType.INT, DorisDataType.VARCHAR), LENGTH(false, DorisDataType.INT, DorisDataType.VARCHAR),
        BIT_LENGTH(false, DorisDataType.INT, DorisDataType.VARCHAR),
        CHAR_LENGTH(false, DorisDataType.INT, DorisDataType.VARCHAR),
        LPAD(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.INT, DorisDataType.VARCHAR),
        RPAD(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.INT, DorisDataType.VARCHAR),
        LOWER(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        LCASE(false, DorisDataType.INT, DorisDataType.VARCHAR),
        UPPER(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        UCASE(false, DorisDataType.INT, DorisDataType.VARCHAR),
        INITCAP(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        REPEAT(false, DorisDataType.VARCHAR, DorisDataType.INT),
        REVERSE(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        CHAR(true, DorisDataType.VARCHAR, DorisDataType.INT),
        CONCAT(true, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        CONCAT_WS(true, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        SUBSTR(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.INT, DorisDataType.INT),
        SUBSTRING(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.INT),
        SUB_REPLACE(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.INT),
        APPEND_TRAILING_CHAR_IF_ABSENT(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        ENDS_WITH(false, DorisDataType.BOOLEAN, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        STARTS_WITH(false, DorisDataType.BOOLEAN, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        TRIM(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        LTRIM(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        RTRIM(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        NULL_OR_EMPTY(false, DorisDataType.BOOLEAN, DorisDataType.VARCHAR),
        NOT_NULL_OR_EMPTY(false, DorisDataType.BOOLEAN, DorisDataType.VARCHAR),
        HEX(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        UNHEX(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        ELT(true, DorisDataType.VARCHAR, DorisDataType.INT, DorisDataType.VARCHAR),
        INSTR(false, DorisDataType.INT, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        LOCATE(false, DorisDataType.INT, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        // FIELD(1, true),
        FIND_IN_SET(false, DorisDataType.INT, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        REPLACE(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        LEFT(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.INT),
        RIGHT(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.INT),
        STRLEFT(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.INT),
        STRRIGHT(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.INT),
        SPLIT_PART(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.INT),
        // SPLIT_BY_STRING(2),
        SUBSTRING_INDEX(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.INT),
        MONEY_FORMAT(false, DorisDataType.VARCHAR, DorisDataType.DECIMAL),
        PARSE_URL(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        CONVERT_TO(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        EXTRACT_URL_PARAMETER(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        UUID(false, DorisDataType.VARCHAR), SPACE(false, DorisDataType.VARCHAR, DorisDataType.INT),
        // SLEEP(1),
        ESQUERY(false, DorisDataType.BOOLEAN, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        MASK(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        MASK_FIRST_N(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        MASK_LAST_N(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        // MULTI_SEARCH_ALL_POSITIONS(2),
        // MULTI_MATCH_ANY(2),

        // BITMAP functions, https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/bitmap-functions/to_bitmap
        // skip now

        // Bitwise functions, https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/bitwise-functions/bitand
        BITAND(false, DorisDataType.INT, DorisDataType.INT, DorisDataType.INT),
        BITOR(false, DorisDataType.INT, DorisDataType.INT, DorisDataType.INT),
        BITXOR(false, DorisDataType.INT, DorisDataType.INT, DorisDataType.INT),
        BITNOT(false, DorisDataType.INT, DorisDataType.INT),

        // condition funtions
        // case(),
        COALESCE(true, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        IF(false, DorisDataType.VARCHAR, DorisDataType.BOOLEAN, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        IFNULL(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        NVL(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.VARCHAR),
        NULLIF(false, DorisDataType.VARCHAR, DorisDataType.VARCHAR, DorisDataType.VARCHAR),

        // JSON Functions, https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/json-functions/jsonb_parse
        // skip now

        // Hash functions,
        // https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/hash-functions/murmur_hash3_32
        MURMUR_HASH3_32(true, DorisDataType.INT, DorisDataType.VARCHAR),
        MURMUR_HASH3_64(true, DorisDataType.INT, DorisDataType.VARCHAR),

        // HLL functions, https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/hll-functions/hll_cardinality
        // skip now

        // Math functions, https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/math-functions/conv
        CONV(false, DorisDataType.VARCHAR, DorisDataType.INT, DorisDataType.INT, DorisDataType.INT),
        BIN(false, DorisDataType.VARCHAR, DorisDataType.INT), SIN(false, DorisDataType.FLOAT, DorisDataType.FLOAT),
        COS(false, DorisDataType.FLOAT, DorisDataType.FLOAT), TAN(false, DorisDataType.FLOAT, DorisDataType.FLOAT),
        ASIN(false, DorisDataType.FLOAT, DorisDataType.FLOAT), ACOS(false, DorisDataType.FLOAT, DorisDataType.FLOAT),
        ATAN(false, DorisDataType.FLOAT, DorisDataType.FLOAT), E(false, DorisDataType.FLOAT),
        PI(false, DorisDataType.FLOAT), EXP(false, DorisDataType.FLOAT, DorisDataType.FLOAT),
        LOG(false, DorisDataType.FLOAT, DorisDataType.FLOAT, DorisDataType.FLOAT),
        LOG2(false, DorisDataType.FLOAT, DorisDataType.FLOAT), LN(false, DorisDataType.FLOAT, DorisDataType.FLOAT),
        LOG10(false, DorisDataType.FLOAT, DorisDataType.FLOAT), CEIL(false, DorisDataType.FLOAT, DorisDataType.FLOAT),
        FLOOR(false, DorisDataType.FLOAT, DorisDataType.FLOAT),
        PMOD(false, DorisDataType.FLOAT, DorisDataType.FLOAT, DorisDataType.FLOAT),
        ROUND(false, DorisDataType.INT, DorisDataType.FLOAT),
        ROUND_BANKERS(false, DorisDataType.FLOAT, DorisDataType.FLOAT, DorisDataType.INT),
        TRUNCATE(false, DorisDataType.FLOAT, DorisDataType.FLOAT, DorisDataType.INT),
        ABS(false, DorisDataType.FLOAT, DorisDataType.FLOAT), SQRT(false, DorisDataType.FLOAT, DorisDataType.FLOAT),
        CBRT(false, DorisDataType.FLOAT, DorisDataType.FLOAT),
        POW(false, DorisDataType.FLOAT, DorisDataType.FLOAT, DorisDataType.FLOAT),
        DEGREES(false, DorisDataType.FLOAT, DorisDataType.FLOAT),
        RADIANS(false, DorisDataType.FLOAT, DorisDataType.FLOAT), SIGN(false, DorisDataType.INT, DorisDataType.FLOAT),
        POSTIVE(false, DorisDataType.FLOAT, DorisDataType.FLOAT),
        NEGATIVE(false, DorisDataType.FLOAT, DorisDataType.FLOAT),
        GREATEST(true, DorisDataType.FLOAT, DorisDataType.FLOAT), LEAST(true, DorisDataType.FLOAT, DorisDataType.FLOAT),
        RANDOM(false, DorisDataType.FLOAT), MOD(false, DorisDataType.FLOAT, DorisDataType.FLOAT, DorisDataType.FLOAT);

        // encrypt-digest-functions,
        // https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/encrypt-digest-functions/aes
        // skip now

        private boolean isVariadic; // If isVALid is true, then treat the last argumentTypes as an infinite type
        private DorisDataType returnType;
        private DorisDataType[] argumentTypes;
        private String functionName;

        DorisFunction(String functionName, boolean isVariadic, DorisDataType returnType,
                DorisDataType... argumentTypes) {
            this.functionName = functionName;
            this.isVariadic = isVariadic;
            this.returnType = returnType;
            this.argumentTypes = argumentTypes.clone();
        }

        DorisFunction(boolean isVariadic, DorisDataType returnType, DorisDataType... argumentTypes) {
            this.functionName = toString();
            this.isVariadic = isVariadic;
            this.returnType = returnType;
            this.argumentTypes = argumentTypes.clone();
        }

        DorisFunction(boolean isVariadic, DorisDataType returnType) {
            this.functionName = toString();
            this.isVariadic = isVariadic;
            this.returnType = returnType;
            this.argumentTypes = null;
        }

        public String getFunctionName() {
            return functionName;
        }

        public static DorisFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public boolean isVariadic() {
            return isVariadic;
        }

        public boolean isCompatibleWithReturnType(DorisDataType returnType) {
            return this.returnType == returnType;
        }

        public DorisDataType[] getArgumentTypes() {
            if (argumentTypes == null) {
                return null;
            }
            return argumentTypes.clone();
        }

        public DorisFunctionOperation getCall(DorisDataType returnType, DorisNewExpressionGenerator gen, int depth) {
            List<DorisExpression> arguments = new ArrayList<>();
            if (getArgumentTypes() != null) {
                Stream.of(getArgumentTypes()).forEach(arg -> arguments.add(gen.generateExpression(arg, depth + 1)));
            }
            return new DorisFunctionOperation(this, arguments);
        }

        public static List<DorisFunction> getFunctionsCompatibleWith(DorisDataType returnType) {
            return Stream.of(values()).filter(f -> f.isCompatibleWithReturnType(returnType))
                    .collect(Collectors.toList());
        }

    }

    public DorisFunctionOperation(DorisFunction function, List<DorisExpression> args) {
        this.function = function;
        this.args = args;
    }

    public List<DorisExpression> getArgs() {
        return args;
    }

    public DorisFunction getFunction() {
        return function;
    }

}
