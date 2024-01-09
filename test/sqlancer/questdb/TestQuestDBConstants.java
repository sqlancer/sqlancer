package sqlancer.questdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sqlancer.questdb.ast.QuestDBConstants;

import java.time.Instant;

import static sqlancer.questdb.ast.QuestDBConstants.*;

public class TestQuestDBConstants {
    @Test
    public void testBoolean() {
        QuestDBBooleanConstant c = QuestDBConstants.createQuestDBConstant(QuestDBDataType.BOOLEAN, false);
        Assertions.assertFalse(c.getValue());
        Assertions.assertFalse(c.isNull());
        Assertions.assertEquals("false", c.toString());
        Assertions.assertEquals(QuestDBDataType.BOOLEAN, c.getType());
    }

    @Test
    public void testBooleanNull() {
        QuestDBConstant<?> c = QuestDBConstants.getQuestDBNullConstant(QuestDBDataType.BOOLEAN);
        Assertions.assertFalse((boolean) c.getValue());
        Assertions.assertTrue(c.isNull());
        Assertions.assertEquals("false", c.toString());
        Assertions.assertEquals(QuestDBDataType.NULL, c.getType());
    }

    @Test
    public void testByte() {
        QuestDBByteConstant c = QuestDBConstants.createQuestDBConstant(QuestDBDataType.BYTE, (byte) -127);
        Assertions.assertEquals((byte) -127, c.getValue());
        Assertions.assertFalse(c.isNull());
        Assertions.assertEquals("-127", c.toString());
        Assertions.assertEquals(QuestDBDataType.BYTE, c.getType());
    }

    @Test
    public void testByteNull() {
        QuestDBConstant<?> c = QuestDBConstants.getQuestDBNullConstant(QuestDBDataType.BYTE);
        Assertions.assertEquals((byte) 0, c.getValue());
        Assertions.assertTrue(c.isNull());
        Assertions.assertEquals("", c.toString());
        Assertions.assertEquals(QuestDBDataType.NULL, c.getType());
    }

    @Test
    public void testShort() {
        QuestDBShortConstant c = QuestDBConstants.createQuestDBConstant(QuestDBDataType.SHORT, (short) -127);
        Assertions.assertEquals((short) -127, c.getValue());
        Assertions.assertFalse(c.isNull());
        Assertions.assertEquals("-127", c.toString());
        Assertions.assertEquals(QuestDBDataType.SHORT, c.getType());
    }

    @Test
    public void testShortNull() {
        QuestDBConstant<?> c = QuestDBConstants.getQuestDBNullConstant(QuestDBDataType.SHORT);
        Assertions.assertEquals((short) 0, c.getValue());
        Assertions.assertTrue(c.isNull());
        Assertions.assertEquals("", c.toString());
        Assertions.assertEquals(QuestDBDataType.NULL, c.getType());
    }

    @Test
    public void testChar() {
        QuestDBCharConstant c = QuestDBConstants.createQuestDBConstant(QuestDBDataType.CHAR, 'V');
        Assertions.assertEquals('V', c.getValue());
        Assertions.assertFalse(c.isNull());
        Assertions.assertEquals("V", c.toString());
        Assertions.assertEquals(QuestDBDataType.CHAR, c.getType());
    }

    @Test
    public void testCharNull() {
        QuestDBConstant<?> c = QuestDBConstants.getQuestDBNullConstant(QuestDBDataType.CHAR);
        Assertions.assertEquals((char) 0, c.getValue());
        Assertions.assertTrue(c.isNull());
        Assertions.assertEquals("", c.toString());
        Assertions.assertEquals(QuestDBDataType.NULL, c.getType());
    }

    @Test
    public void testInt() {
        QuestDBIntConstant c = QuestDBConstants.createQuestDBConstant(QuestDBDataType.INT, -127);
        Assertions.assertEquals(-127, c.getValue());
        Assertions.assertFalse(c.isNull());
        Assertions.assertEquals("-127", c.toString());
        Assertions.assertEquals(QuestDBDataType.INT, c.getType());
    }

    @Test
    public void testIntNull() {
        QuestDBConstant<?> c = QuestDBConstants.getQuestDBNullConstant(QuestDBDataType.INT);
        Assertions.assertEquals(Integer.MIN_VALUE, c.getValue());
        Assertions.assertTrue(c.isNull());
        Assertions.assertEquals("", c.toString());
        Assertions.assertEquals(QuestDBDataType.NULL, c.getType());
    }

    @Test
    public void testLong() {
        QuestDBLongConstant c = QuestDBConstants.createQuestDBConstant(QuestDBDataType.LONG, -127L);
        Assertions.assertEquals(-127L, c.getValue());
        Assertions.assertFalse(c.isNull());
        Assertions.assertEquals("-127", c.toString());
        Assertions.assertEquals(QuestDBDataType.LONG, c.getType());
    }

    @Test
    public void testLongNull() {
        QuestDBConstant<?> c = QuestDBConstants.getQuestDBNullConstant(QuestDBDataType.LONG);
        Assertions.assertEquals(Long.MIN_VALUE, c.getValue());
        Assertions.assertTrue(c.isNull());
        Assertions.assertEquals("", c.toString());
        Assertions.assertEquals(QuestDBDataType.NULL, c.getType());
    }

    @Test
    public void testFloat() {
        QuestDBFloatConstant c = QuestDBConstants.createQuestDBConstant(QuestDBDataType.FLOAT, -127F);
        Assertions.assertEquals(-127F, c.getValue());
        Assertions.assertFalse(c.isNull());
        Assertions.assertEquals("-127.0", c.toString());
        Assertions.assertEquals(QuestDBDataType.FLOAT, c.getType());
    }

    @Test
    public void testFloatNull() {
        QuestDBConstant<?> c = QuestDBConstants.getQuestDBNullConstant(QuestDBDataType.FLOAT);
        Assertions.assertEquals(Float.NaN, c.getValue());
        Assertions.assertTrue(c.isNull());
        Assertions.assertEquals("", c.toString());
        Assertions.assertEquals(QuestDBDataType.NULL, c.getType());
    }

    @Test
    public void testDouble() {
        QuestDBDoubleConstant c = QuestDBConstants.createQuestDBConstant(QuestDBDataType.DOUBLE, -127.0);
        Assertions.assertEquals(-127.0, c.getValue());
        Assertions.assertFalse(c.isNull());
        Assertions.assertEquals("-127.0", c.toString());
        Assertions.assertEquals(QuestDBDataType.DOUBLE, c.getType());
    }

    @Test
    public void testDoubleNull() {
        QuestDBConstant<?> c = QuestDBConstants.getQuestDBNullConstant(QuestDBDataType.DOUBLE);
        Assertions.assertEquals(Double.NaN, c.getValue());
        Assertions.assertTrue(c.isNull());
        Assertions.assertEquals("", c.toString());
        Assertions.assertEquals(QuestDBDataType.NULL, c.getType());
    }

    @Test
    public void testString() {
        QuestDBStringConstant c = QuestDBConstants.createQuestDBConstant(QuestDBDataType.STRING, "sqlancer");
        Assertions.assertEquals("sqlancer", c.getValue());
        Assertions.assertFalse(c.isNull());
        Assertions.assertEquals("'sqlancer'", c.toString());
        Assertions.assertEquals(QuestDBDataType.STRING, c.getType());
    }

    @Test
    public void testStringNull() {
        QuestDBConstant<?> c = QuestDBConstants.getQuestDBNullConstant(QuestDBDataType.STRING);
        Assertions.assertNull(c.getValue());
        Assertions.assertTrue(c.isNull());
        Assertions.assertEquals("", c.toString());
        Assertions.assertEquals(QuestDBDataType.NULL, c.getType());
    }

    @Test
    public void testSymbol() {
        QuestDBSymbolConstant c = QuestDBConstants.createQuestDBConstant(QuestDBDataType.SYMBOL, "sqlancer");
        Assertions.assertEquals("sqlancer", c.getValue());
        Assertions.assertFalse(c.isNull());
        Assertions.assertEquals("'sqlancer'", c.toString());
        Assertions.assertEquals(QuestDBDataType.SYMBOL, c.getType());
    }

    @Test
    public void testSymbolNull() {
        QuestDBConstant<?> c = QuestDBConstants.getQuestDBNullConstant(QuestDBDataType.SYMBOL);
        Assertions.assertNull(c.getValue());
        Assertions.assertTrue(c.isNull());
        Assertions.assertEquals("", c.toString());
        Assertions.assertEquals(QuestDBDataType.NULL, c.getType());
    }

    @Test
    public void testDate() {
        Instant now = Instant.now();
        long date = QuestDBTimestampConstant.timestampMicros(now);
        QuestDBDateConstant c = QuestDBConstants.createQuestDBConstant(QuestDBDataType.DATE, date);
        Assertions.assertEquals(date, c.getValue());
        Assertions.assertFalse(c.isNull());
        String expected = now.toString();
        int idx = expected.indexOf('T');
        Assertions.assertEquals("'" + expected.substring(0, idx) + '\'', c.toString());
        Assertions.assertEquals(QuestDBDataType.DATE, c.getType());
    }

    @Test
    public void testDateNull() {
        QuestDBConstant<?> c = QuestDBConstants.getQuestDBNullConstant(QuestDBDataType.DATE);
        Assertions.assertEquals(Long.MIN_VALUE, c.getValue());
        Assertions.assertTrue(c.isNull());
        Assertions.assertEquals("", c.toString());
        Assertions.assertEquals(QuestDBDataType.NULL, c.getType());
    }

    @Test
    public void testTimestamp() {
        Instant now = Instant.now();
        long timestamp = QuestDBTimestampConstant.timestampMicros(now);
        QuestDBTimestampConstant c = QuestDBConstants.createQuestDBConstant(QuestDBDataType.TIMESTAMP, timestamp);
        Assertions.assertEquals(timestamp, c.getValue());
        Assertions.assertFalse(c.isNull());
        Assertions.assertEquals("'" + now + '\'', c.toString());
        Assertions.assertEquals(QuestDBDataType.TIMESTAMP, c.getType());
    }

    @Test
    public void testTimestampNull() {
        QuestDBConstant<?> c = QuestDBConstants.getQuestDBNullConstant(QuestDBDataType.TIMESTAMP);
        Assertions.assertEquals(Long.MIN_VALUE, c.getValue());
        Assertions.assertTrue(c.isNull());
        Assertions.assertEquals("", c.toString());
        Assertions.assertEquals(QuestDBDataType.NULL, c.getType());
    }

    @Test
    public void testTimestampMicros() {
        Instant now = Instant.now();
        long timestampMicros = QuestDBTimestampConstant.timestampMicros(now);
        Instant again = QuestDBTimestampConstant.fromTimestampMicros(timestampMicros);
        Assertions.assertEquals(now, again);
    }
}
