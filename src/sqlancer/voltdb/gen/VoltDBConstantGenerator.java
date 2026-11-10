package sqlancer.voltdb.gen;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import sqlancer.Randomly;
import sqlancer.voltdb.VoltDBSchema.VoltDBCompositeDataType;

/**
 * Literals for each VoltDB column type, always inside the type's valid range.
 *
 * <p>
 * Intentional VoltDB differences the generator works around:
 * <ul>
 * <li>Each integer type's minimum value is VoltDB's NULL for that type, so the valid range starts one above it (for
 * example TINYINT is -127 to 127). Some paths reject the minimum and others silently store NULL (ENG-22880), so the
 * minimum is never generated.</li>
 * <li>FLOAT's NULL is -Double.MAX_VALUE, and infinite values are not accepted, so neither is generated. Nor are
 * subnormal values (ENG-30551).</li>
 * <li>DECIMAL is fixed at precision 38, scale 12: at most 26 digits before the point.</li>
 * <li>DATE and TIMESTAMP accept years 1583 to 9999 only.</li>
 * </ul>
 */
public final class VoltDBConstantGenerator {

    private static final long MIN_TIMESTAMP_MICROS = LocalDate.of(1583, 1, 1).atStartOfDay()
            .toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
    private static final long MAX_TIMESTAMP_MICROS = LocalDate.of(10000, 1, 1).atStartOfDay()
            .toEpochSecond(ZoneOffset.UTC) * 1_000_000L - 1;
    private static final long MIN_EPOCH_DAY = LocalDate.of(1583, 1, 1).toEpochDay();
    private static final long MAX_EPOCH_DAY = LocalDate.of(9999, 12, 31).toEpochDay();
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS");
    private static final BigInteger MAX_DECIMAL_UNSCALED = BigInteger.TEN.pow(38).subtract(BigInteger.ONE);
    private static final int DECIMAL_SCALE = 12;

    private VoltDBConstantGenerator() {
    }

    // A literal of the column's type, or NULL with a small probability when the column is nullable.
    public static String generate(Randomly r, VoltDBCompositeDataType type, boolean nullable) {
        if (nullable && Randomly.getBooleanWithRatherLowProbability()) {
            return "NULL";
        }
        return generate(r, type);
    }

    public static String generate(Randomly r, VoltDBCompositeDataType type) {
        switch (type.getType()) {
        case TINYINT:
            return Long.toString(integer(r, Byte.MAX_VALUE));
        case SMALLINT:
            return Long.toString(integer(r, Short.MAX_VALUE));
        case INTEGER:
            return Long.toString(integer(r, Integer.MAX_VALUE));
        case BIGINT:
            return Long.toString(integer(r, Long.MAX_VALUE));
        case FLOAT:
            return floatLiteral(r);
        case DECIMAL:
            return decimal(r).toPlainString();
        case VARCHAR:
            return varchar(r, type.getSize());
        case VARBINARY:
            return varbinary(r, type.getSize());
        case TIMESTAMP:
            return "'" + timestamp(r) + "'";
        case DATE:
            return "'" + LocalDate.ofEpochDay(r.getLong(MIN_EPOCH_DAY, MAX_EPOCH_DAY + 1)) + "'";
        case BOOLEAN:
            return Randomly.getBoolean() ? "TRUE" : "FALSE";
        default:
            throw new AssertionError(type);
        }
    }

    // A value in [-max, max]: SQLancer's interesting values, clamped, or a uniform one.
    private static long integer(Randomly r, long max) {
        long v = r.getInteger();
        if (v >= -max && v <= max) {
            return v;
        }
        if (v == Long.MIN_VALUE || v < -max) {
            return Randomly.getBoolean() ? -max : r.getLong(-max, max);
        }
        return Randomly.getBoolean() ? max : r.getLong(-max, max);
    }

    private static String floatLiteral(Randomly r) {
        double d;
        do {
            d = r.getFiniteDouble();
            // ENG-30551: a subnormal FLOAT constant in a statement that is not parameterized makes its plan fail to
            // load, and re-running it after DDL crashes the node; such values are never generated
        } while (d <= -Double.MAX_VALUE || d != 0 && Math.abs(d) < Double.MIN_NORMAL);
        String s = Double.toString(d);
        // without an exponent VoltDB reads the literal as a DECIMAL, which holds only 12 digits after the point
        return s.contains("E") ? s : s + "E0";
    }

    private static BigDecimal decimal(Randomly r) {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return new BigDecimal(Randomly.fromOptions(MAX_DECIMAL_UNSCALED, MAX_DECIMAL_UNSCALED.negate()),
                    DECIMAL_SCALE);
        }
        BigDecimal d = new BigDecimal(r.getInteger()).add(BigDecimal.valueOf(r.getInteger(0, 1_000_000), 6));
        BigDecimal bound = new BigDecimal(MAX_DECIMAL_UNSCALED, DECIMAL_SCALE);
        if (d.abs().compareTo(bound) > 0) {
            return BigDecimal.ZERO;
        }
        return d;
    }

    private static String varchar(Randomly r, int size) {
        String s = r.getString();
        if (s.codePointCount(0, s.length()) > size) {
            s = s.substring(0, s.offsetByCodePoints(0, size));
        }
        if (s.equals("\\N")) {
            // ENG-30552: the literal '\N' is stored and read as NULL
            s = "N";
        }
        return quote(s);
    }

    // A VARCHAR literal. ENG-30562: VoltDB reads a backslash before a quote as escaping it, so after a literal ending
    // in
    // one it strips /*...*/ and -- from the statement's later literals; a backslash there is written as a slash.
    static String quote(String s) {
        return "'" + s.replaceAll("\\\\(?='|$)", "/").replace("'", "''") + "'";
    }

    private static String varbinary(Randomly r, int size) {
        byte[] bytes = r.getBytes();
        int n = Math.min(bytes.length, size);
        StringBuilder sb = new StringBuilder("x'");
        for (int i = 0; i < n; i++) {
            sb.append(String.format("%02X", bytes[i]));
        }
        return sb.append("'").toString();
    }

    private static String timestamp(Randomly r) {
        long micros = r.getLong(MIN_TIMESTAMP_MICROS, MAX_TIMESTAMP_MICROS + 1);
        long seconds = Math.floorDiv(micros, 1_000_000L);
        int nanos = (int) Math.floorMod(micros, 1_000_000L) * 1000;
        return LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC).format(TIMESTAMP_FORMAT);
    }

}
