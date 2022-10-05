package sqlancer.tidb.ast;

import java.util.List;

import sqlancer.Randomly;

public class TiDBFunctionCall implements TiDBExpression {

    private TiDBFunction function;
    private List<TiDBExpression> args;

    // https://pingcap.github.io/docs/stable/reference/sql/functions-and-operators/numeric-functions-and-operators/
    public enum TiDBFunction {

        POW(2), //
        POWER(2), //
        EXP(1), //
        SQRT(1), //
        LN(1), //
        LOG(1), //
        LOG2(1), //
        LOG10(1), //
        PI(0), //
        TAN(1), //
        COT(1), //
        SIN(1), //
        COS(1), //
        ATAN(1), //
        ATAN2(2), //
        ACOS(1), //
        RADIANS(1), //
        DEGREES(1), //
        MOD(2), //
        ABS(1), //
        CEIL(1), //
        CEILING(1), //
        FLOOR(1), //
        ROUND(1),
        // RAND(1),
        SIGN(1), //
        // CONV()
        // TRUNCATE(),
        CRC32(1), //

        // https://pingcap.github.io/docs/stable/reference/sql/functions-and-operators/bit-functions-and-operators/
        BIT_COUNT(1),

        // https://pingcap.github.io/docs/stable/reference/sql/functions-and-operators/information-functions/
        CONNECTION_ID(0), //
        CURRENT_USER(0), //
        DATABASE(0), //
        // FOUND_ROWS(0), <-- non-deterministic
        // LAST_INSERT_ID(0), <-- non-deterministic
        // ROW_COUNT(0), <-- non-deterministic
        SCHEMA(0), //
        SESSION_USER(0), //
        SYSTEM_USER(0), //
        USER(0), //
        VERSION(0),

        TIDB_VERSION(0), //

        IF(3), //
        IFNULL(2), //
        NULLIF(2),

        // string functions
        ASCII(1), //
        BIN(1), //
        BIT_LENGTH(1), //
        CHAR(1), //
        CHAR_LENGTH(1), //
        CHARACTER_LENGTH(1), //
        CONCAT(1, true), //
        CONCAT_WS(2, true), //
        ELT(2, true), //
        EXPORT_SET(0) {
            @Override
            public int getNrArgs() {
                return Randomly.fromOptions(3, 4, 5);
            }
        },
        // [...]
        FIELD(2, true), //
        FIND_IN_SET(2), //
        FORMAT(2), //
        FROM_BASE64(1), //
        HEX(1), //
        INSERT(4), //
        INSTR(2), //

        // [...]
        REPLACE(3), //
        REVERSE(1), //
        RIGHT(2), //
        // RPAD TODO
        RTRIM(1), //
        SPACE(1), // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/6
        STRCMP(2), //
        SUBSTRING(2), // TODO: support other versions
        SUBSTRING_INDEX(3), //
        TO_BASE64(1), //
        TRIM(1), //
        UCASE(1), //
        UNHEX(1), //
        UPPER(1), //

        COALESCE(1, true), //

        // https://pingcap.github.io/docs/stable/reference/sql/functions-and-operators/miscellaneous-functions/
        INET_ATON(1), //
        INET_NTOA(1), //
        INET6_ATON(1), //
        INET6_NTOA(1), //
        IS_IPV4(1), //
        IS_IPV4_COMPAT(1), //
        IS_IPV4_MAPPED(1), //
        IS_IPV6(1),
        // NAME_CONST(2),

        DATE_FORMAT(2), //
        // ANY_VALUE(1),
        DEFAULT(-1);

        private int nrArgs;
        private boolean isVariadic;

        TiDBFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        TiDBFunction(int nrArgs, boolean isVariadic) {
            this.nrArgs = nrArgs;
            this.isVariadic = true;
        }

        public static TiDBFunction getRandom() {
            while (true) {
                TiDBFunction func = Randomly.fromOptions(values());
                if (func.getNrArgs() != -1) {
                    // special functions that need to be created manually (e.g., DEFAULT)
                    return func;
                }
            }
        }

        public int getNrArgs() {
            return nrArgs + (isVariadic() ? Randomly.smallNumber() : 0);
        }

        public boolean isVariadic() {
            return isVariadic;
        }

    }

    public TiDBFunctionCall(TiDBFunction function, List<TiDBExpression> args) {
        this.function = function;
        this.args = args;
    }

    public List<TiDBExpression> getArgs() {
        return args;
    }

    public TiDBFunction getFunction() {
        return function;
    }

}
