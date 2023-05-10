package sqlancer.doris.ast;

import java.util.List;

import sqlancer.Randomly;

public class DorisFunctionOperation implements DorisExpression {

    private DorisFunction function;
    private List<DorisExpression> args;

    // https://doris.apache.org/zh-CN/docs/dev/summary/basic-summary
    public enum DorisFunction {

        // Array functions, https://doris.apache.org/docs/dev/sql-manual/sql-functions/array-functions/array
        ARRAY(1, true), ARRAY_MAX(1), ARRAY_MIN(1),
        // ARRAY_MAP(2), // use lambda expression, currently not considered
        // ARRAY_FILTER(2), // use lambda expression, currently not considered
        ARRAY_AVG(1), ARRAY_SUM(1), ARRAY_SIZE(1), SIZE(1), CARDINALITY(1), ARRAY_REMOVE(2), ARRAY_SLICE(3),
        ARRAY_SORT(1), ARRAY_REVERSE_SORT(1), ARRAY_POSITION(2), ARRAY_CONTAINS(2), ARRAY_EXCEPT(2), ARRAY_PRODUCT(1),
        ARRAY_INTERSECT(2), ARRAY_RANGE(0) {
            @Override
            public int getNrArgs() {
                return Randomly.fromOptions(1, 2, 3);
            }
        },
        ARRAY_DISTINCT(1), ARRAY_DIFFERENCE(1), ARRAY_UNION(2), ARRAY_JOIN(0) {
            @Override
            public int getNrArgs() {
                return Randomly.fromOptions(2, 3);
            }
        },
        ARRAY_WITH_CONSTANT(2), ARRAY_ENUMERATE(1), ARRAY_ENUMERATE_UNIQ(1), ARRAY_POPBACK(1), ARRAY_POPFRONT(1),
        ARRAY_PUSHFRONT(2), ARRAY_COMPACT(1), ARRAY_CONCAT(1), ARRAY_ZIP(1, true),
        // ARRAY_EXISTS(2), // use lambda expression, currently not considered
        ARRAYS_OVERLAP(2), COUNTEQUAL(2), ELEMENT_AT(2),

        // Date functions, https://doris.apache.org/docs/dev/sql-manual/sql-functions/date-time-functions/convert_tz/
        CONVERT_TZ(3), CURDATE(0), CURRENT_DATE(0), CURTIME(0), CURRENT_TIME(0), CURRENT_TIMESTAMP(0), LOCALTIME(0),
        LOCALTIMESTAMP(0), NOW(0), YEAR(1), QUARTER(1), MONTH(1), DAY(1), DAYOFYEAR(1), DAYOFMONTH(1), DAYOFWEEK(1),
        WEEK(0) {
            @Override
            public int getNrArgs() {
                return Randomly.fromOptions(1, 2);
            }
        },
        WEEKDAY(1), WEEKOFYEAR(1), YEARWEEK(0) {
            @Override
            public int getNrArgs() {
                return Randomly.fromOptions(1, 2);
            }
        },
        DAYNAME(1), MONTHNAME(1), HOUR(1), MINUTE(1), SECOND(1), FROM_DAYS(1), LAST_DAYS(1), TO_MONDAY(1),
        FROM_UNIXTIME(0) {
            @Override
            public int getNrArgs() {
                return Randomly.fromOptions(1, 2);
            }
        },
        UNIX_TIMESTAMP(0) {
            @Override
            public int getNrArgs() {
                return Randomly.fromOptions(0, 1);
            }
        },
        UTC_TIMESTAMP(0), TO_DATE(1), TO_DAYS(1),
        // EXTRACT(1), // select extract(year from '2022-09-22 17:01:30') as year, currently not considered
        MAKEDATE(2), STR_TO_DATE(2), TIME_ROUND(0) {
            @Override
            public int getNrArgs() {
                return Randomly.fromOptions(1, 2, 3);
            }
        },
        TIME_DIFF(2), TIMESTAMPADD(3), TIMESTAMPDIFF(3), DATE_ADD(2), DATE_SUB(2), DATE_TRUNC(2), DATE_FORMAT(2),
        DATEDIFF(2), MICROSECONDS_ADD(2), MINUTES_ADD(2), MINUTES_DIFF(2), MINUTES_SUB(2), SECONDS_ADD(2),
        SECONDS_DIFF(2), SECONDS_SUB(2), HOURS_ADD(2), HOURS_DIFF(2), HOURS_SUB(2), DAYS_ADD(2), DAYS_DIFF(2),
        DAYS_SUB(2), WEEKS_ADD(2), WEEKS_DIFF(2), WEEKS_SUB(2), MONTHS_ADD(2), MONTHS_DIFF(2), MONTHS_SUB(2),
        YEARS_ADD(2), YEARS_DIFF(2), YEARS_SUB(2),

        // GIS functions, https://doris.apache.org/docs/dev/sql-manual/sql-functions/spatial-functions/st_x
        ST_X(1), ST_Y(1), ST_CIRCLE(3), ST_DISTANCE_SPEHERE(4), ST_POINT(2), ST_POLYGON(1), ST_POLYFROMTEXT(1),
        ST_POLYGONFROMTEXT(1), ST_ASTEXT(1), ST_ASWKT(1), ST_CONTAINS(2), ST_GEOMETRYFROMTEXT(1), ST_GEOMFROMTEXT(1),
        ST_LINEFROMTEXT(1), ST_LINESTRINGFROMTEXT(1),

        // String functions, https://doris.apache.org/docs/dev/sql-manual/sql-functions/string-functions/to_base64
        TO_BASE64(1), FROM_BASE64(1), ASCII(1), LENGTH(1), BIT_LENGTH(1), CHAR_LENGTH(1), LPAD(3), RPAD(3), LOWER(1),
        LCASE(1), UPPER(1), UCASE(1), INITCAP(1), REPEAT(2), REVERSE(1), CONCAT(1, true), CONCAT_WS(2, true), SUBSTR(3),
        SUBSTRING(0) {
            @Override
            public int getNrArgs() {
                return Randomly.fromOptions(2, 3);
            }
        },
        SUB_REPLACE(0) {
            @Override
            public int getNrArgs() {
                return Randomly.fromOptions(3, 4);
            }
        },
        APPEND_TRAILING_CHAR_IF_ABSENT(2), ENDS_WITH(2), STARTS_WITH(2), TRIM(0) {
            @Override
            public int getNrArgs() {
                return Randomly.fromOptions(1, 2);
            }
        },
        LTRIM(0) {
            @Override
            public int getNrArgs() {
                return Randomly.fromOptions(1, 2);
            }
        },
        RTRIM(0) {
            @Override
            public int getNrArgs() {
                return Randomly.fromOptions(1, 2);
            }
        },
        NULL_OR_EMPTY(1), NOT_NULL_OR_EMPTY(1), HEX(1), UNHEX(1), ELT(1, true), INSTR(2), LOCATE(0) {
            @Override
            public int getNrArgs() {
                return Randomly.fromOptions(2, 3);
            }
        },
        FIELD(1, true), FIND_IN_SET(2), REPLACE(3), LEFT(2), RIGHT(2), STRLEFT(2), STRRIGHT(2), SPLIT_PART(3),
        SPLIT_BY_STRING(2), SUBSTRING_INDEX(3), MONEY_FORMAT(1), PARSE_URL(2), CONVERT_TO(2), EXTRACT_URL_PARAMETER(2),
        UUID(0), SPACE(1),
        // SLEEP(1),
        ESQUERY(2), MASK(0) {
            @Override
            public int getNrArgs() {
                return Randomly.fromOptions(1, 2, 3, 4);
            }
        },
        MASK_FIRST_N(0) {
            @Override
            public int getNrArgs() {
                return Randomly.fromOptions(1, 2);
            }
        },
        MASK_LAST_N(0) {
            @Override
            public int getNrArgs() {
                return Randomly.fromOptions(1, 2);
            }
        },
        MULTI_SEARCH_ALL_POSITIONS(2), MULTI_MATCH_ANY(2),

        // BITMAP functions, https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/bitmap-functions/to_bitmap
        TO_BITMAP(1), BITMAP_HASH(1), BITMAP_FROM_STRING(1), BITMAP_TO_STRING(1), BITMAP_TO_ARRAY(1),
        BITMAP_FROM_ARRAY(1), BITMAP_EMPTY(0), BITMAP_OR(2, true), BITMAP_AND(2), BITMAP_UNION(1), BITMAP_XOR(2, true),
        BITMAP_NOT(2), BITMAP_AND_NOT(2), BITMAP_SUBSET_LIMIT(3), BITMAP_SUBSET_IN_RANGE(3), SUB_BITMAP(3),
        BITMAP_COUNT(1), BITMAP_AND_COUNT(2, true), BITMAP_AND_NOT_COUNT(2), ORTHOGONAL_BITMAP_UNION_COUNT(2, true),
        BITMAP_XOR_COUNT(2, true), BITMAP_OR_COUNT(2, true), BITMAP_CONTAINS(2), BITMAP_HAS_ALL(2), BITMAP_HAS_ANY(2),
        BITMAP_MAX(1), BITMAP_MIN(1), INTERSECT_COUNT(2, true), BITMAP_INTERSECT(1), ORTHOGONAL_BITMAP_INTERSECT(3),
        ORTHOGONAL_BITMAP_INTERSECT_COUNT(3), ORTHOGONAL_BITMAP_EXPR_CALCULATE(3),
        ORTHOGONAL_BITMAP_EXPR_CALCULATE_COUNT(3), BITMAP_HASH64(1),

        // Bitwise functions, https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/bitwise-functions/bitand
        BITAND(2), BITOR(2), BITXOR(2), BITNOT(1),

        // condition funtions
        // case(),
        COALESCE(3, true), IF(3), IFNULL(2), NVL(2), NULLIF(2),

        // JSON Functions, https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/json-functions/jsonb_parse
        JSONB_PARSE(1), JSONB_PARSE_ERROR_TO_NULL(1), JSONB_PARSE_ERROR_TO_VALUE(2), JSONB_EXTRACT(2),
        JSONB_EXTRACT_ISNULL(2), JSONB_EXTRACT_BOOL(2), JSONB_EXTRACT_INT(2), JSONB_EXTRACT_BIGINT(2),
        JSONB_EXTRACT_DOUBLE(2), JSONB_EXTRACT_STRING(2), JSONB_EXISTS_PATH(2), JSONB_TYPE(2), GET_JSON_DOUBLE(2),
        GET_JSON_INT(2), GET_JSON_STRING(2), JSON_ARRAY(0, true), JSON_OBJECT(0, true), JSON_QUOTE(1), JSON_UNQUOTE(1),
        JSON_VALID(1), JSON_EXTRACT(2, true),

        // Hash functions,
        // https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/hash-functions/murmur_hash3_32
        MURMUR_HASH3_32(0, true), MURMUR_HASH3_64(0, true),

        // HLL functions, https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/hll-functions/hll_cardinality
        HLL_CARDINALITY(1), HLL_EMPTY(1), HLL_HASH(1),

        // Math functions, https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/math-functions/conv
        CONV(3), BIN(1), SIN(1), COS(1), TAN(1), ASIN(1), ACOS(1), ATAN(1), E(0), PI(0), EXP(1), LOG(2), LOG2(1), LN(1),
        LOG10(1), CEIL(1), FLOOR(1), PMOD(2), ROUND(0) {
            @Override
            public int getNrArgs() {
                return Randomly.fromOptions(1, 2);
            }
        },
        ROUND_BANKERS(0) {
            @Override
            public int getNrArgs() {
                return Randomly.fromOptions(1, 2);
            }
        },
        TRUNCATE(2), ABS(1), SQRT(1), CBRT(1), POW(2), DEGREES(1), RADIANS(1), SIGN(1), POSTIVE(1), NEGATIVE(1),
        GREATEST(1, true), LEAST(1, true), RANDOM(0), MOD(2);

        // /encrypt-digest-functions,
        // https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/encrypt-digest-functions/aes

        private int nrArgs;
        private boolean isVariadic;

        DorisFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        DorisFunction(int nrArgs, boolean isVariadic) {
            this.nrArgs = nrArgs;
            this.isVariadic = true;
        }

        public static DorisFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs + (isVariadic() ? Randomly.smallNumber() : 0);
        }

        public boolean isVariadic() {
            return isVariadic;
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
