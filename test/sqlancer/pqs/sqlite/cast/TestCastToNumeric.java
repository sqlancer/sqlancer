package sqlancer.pqs.sqlite.cast;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Cast;
import sqlancer.sqlite3.ast.SQLite3Constant;
import sqlancer.sqlite3.schema.SQLite3DataType;

class TestCastToNumeric {

    @Test
    void testLong() {
        long numbers[] = new long[] { 0, 1, 123, Long.MAX_VALUE, Long.MIN_VALUE };
        for (long number : numbers) {
            assertEquals(castLongConstant(number), number);
        }
    }

    class StringTestTriple {
        String value;
        SQLite3DataType type;
        Object expectedCastValue;

        public StringTestTriple(String value, SQLite3DataType type, Object expectedCastValue) {
            this.value = value;
            this.type = type;
            this.expectedCastValue = expectedCastValue;
        }
    }

    @Test
    void testString() {
        List<StringTestTriple> triples = new ArrayList<>();
        triples.add(new StringTestTriple("-3.0", SQLite3DataType.INT, -3L));
        triples.add(new StringTestTriple("8.2250617031974513E18", SQLite3DataType.REAL, 8.2250617031974513E18));

        triples.add(new StringTestTriple("-2277224522334683278", SQLite3DataType.INT, -2277224522334683278L));

        triples.add(new StringTestTriple("123a", SQLite3DataType.INT, 123L));
        triples.add(new StringTestTriple("", SQLite3DataType.INT, 0L));
        triples.add(new StringTestTriple("a", SQLite3DataType.INT, 0L));
        triples.add(new StringTestTriple("3", SQLite3DataType.INT, 3L));
        triples.add(new StringTestTriple("-3", SQLite3DataType.INT, -3L));
        triples.add(new StringTestTriple("0.0", SQLite3DataType.INT, 0L));
        triples.add(new StringTestTriple("+0", SQLite3DataType.INT, 0L));
        triples.add(new StringTestTriple("+9", SQLite3DataType.INT, 9L));
        triples.add(new StringTestTriple("++9", SQLite3DataType.INT, 0L));
        triples.add(new StringTestTriple("+-9", SQLite3DataType.INT, 0L));
        triples.add(new StringTestTriple("-1748799336", SQLite3DataType.INT, -1748799336L));
        triples.add(new StringTestTriple("-0", SQLite3DataType.INT, 0L));

        triples.add(new StringTestTriple("4E ", SQLite3DataType.INT, 4L));
        triples.add(new StringTestTriple("3.0e+5", SQLite3DataType.INT, 300000L));
        triples.add(new StringTestTriple("-3.2", SQLite3DataType.REAL, -3.2d));
        triples.add(new StringTestTriple("10e9", SQLite3DataType.INT, 10000000000L));
        // triples.add(new StringTestTriple("-0.0", SQLite3DataType.REAL, 0.0d));
        triples.add(new StringTestTriple("9223372036854775807", SQLite3DataType.INT, 9223372036854775807L));
        triples.add(new StringTestTriple("4337561223119921152", SQLite3DataType.INT, 4337561223119921152L));
        triples.add(new StringTestTriple("7839344951195291815", SQLite3DataType.INT, 7839344951195291815L));

        // infinities
        triples.add(new StringTestTriple("-Infinity", SQLite3DataType.INT, 0L)); //
        triples.add(new StringTestTriple("Infinity", SQLite3DataType.INT, 0L)); //
        triples.add(new StringTestTriple("Inf", SQLite3DataType.INT, 0L)); //
        triples.add(new StringTestTriple("-Inf", SQLite3DataType.INT, 0L)); //
        triples.add(new StringTestTriple("NaN", SQLite3DataType.INT, 0L)); //
        triples.add(new StringTestTriple("1e500", SQLite3DataType.REAL, Double.POSITIVE_INFINITY)); //
        triples.add(new StringTestTriple("-1e500", SQLite3DataType.REAL, Double.NEGATIVE_INFINITY)); //

        for (StringTestTriple triple : triples) {
            SQLite3Constant castVal = SQLite3Cast.castToNumeric(SQLite3Constant.createTextConstant(triple.value));
            assertEquals(triple.expectedCastValue, castVal.getValue(), triple.value.toString());
        }
    }

    @Test
    void testBinary() {
        List<StringTestTriple> triples = new ArrayList<>();
        triples.add(new StringTestTriple("112B3980", SQLite3DataType.INT, 0L)); // +9�
        triples.add(new StringTestTriple("0936", SQLite3DataType.INT, 6L)); // 6
        triples.add(new StringTestTriple("0C36", SQLite3DataType.INT, 6L)); // 6
        triples.add(new StringTestTriple("0a36", SQLite3DataType.INT, 6L)); // 6
        triples.add(new StringTestTriple("0b36", SQLite3DataType.INT, 6L)); // 6
        triples.add(new StringTestTriple("0c36", SQLite3DataType.INT, 6L)); // 6
        triples.add(new StringTestTriple("0d36", SQLite3DataType.INT, 6L)); // 6
        triples.add(new StringTestTriple("0e36", SQLite3DataType.INT, 0L)); // 6
        triples.add(new StringTestTriple("1a347C", SQLite3DataType.INT, 0L)); //
        triples.add(new StringTestTriple("1b347C", SQLite3DataType.INT, 0L)); //
        triples.add(new StringTestTriple("1C32", SQLite3DataType.INT, 0L)); // FS2
        triples.add(new StringTestTriple("1D32", SQLite3DataType.INT, 0L)); // GS2
        triples.add(new StringTestTriple("1e32", SQLite3DataType.INT, 0L)); // RS2
        triples.add(new StringTestTriple("1f32", SQLite3DataType.INT, 0L)); // RS2
        triples.add(new StringTestTriple("2032", SQLite3DataType.INT, 2L)); // RS2
        triples.add(new StringTestTriple("09013454", SQLite3DataType.INT, 0L)); // RS2
        triples.add(new StringTestTriple("2016347C", SQLite3DataType.INT, 0L)); //
        triples.add(new StringTestTriple("2017347C", SQLite3DataType.INT, 0L)); //
        triples.add(new StringTestTriple("2018347C", SQLite3DataType.INT, 0L)); //
        triples.add(new StringTestTriple("2019347C", SQLite3DataType.INT, 0L)); //

        for (StringTestTriple triple : triples) {
            SQLite3Constant castVal = SQLite3Cast.castToNumeric(
                    SQLite3Constant.createBinaryConstant(SQLite3Visitor.hexStringToByteArray(triple.value)));
            assertEquals(triple.expectedCastValue, castVal.getValue(), triple.value.toString());
        }
    }

    private long castLongConstant(long constant) {
        return SQLite3Cast.castToNumeric(SQLite3Constant.createIntConstant(constant)).asInt();
    }

}
