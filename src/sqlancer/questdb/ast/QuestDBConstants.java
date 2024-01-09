package sqlancer.questdb.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.questdb.QuestDBDataType;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.EnumMap;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public final class QuestDBConstants {
    private static final long DAY_MICROS = TimeUnit.DAYS.toMicros(1L);
    private static final String DATE_PATTERN = "yyyy-MM-dd";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(DATE_PATTERN);
    private static final EnumMap<QuestDBDataType, QuestDBConstant<?>> NULL_CONSTANTS = new EnumMap<>(QuestDBDataType.class);
    private static final EnumMap<QuestDBDataType, Object> NULL_VALUES = new EnumMap<>(QuestDBDataType.class);

    static {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    static {
        // QuestDBDataType.NULL is not a type, it is used as a marker only.
        NULL_CONSTANTS.put(QuestDBDataType.BOOLEAN, new QuestDBBooleanConstant());
        NULL_CONSTANTS.put(QuestDBDataType.BYTE, new QuestDBByteConstant());
        NULL_CONSTANTS.put(QuestDBDataType.SHORT, new QuestDBShortConstant());
        NULL_CONSTANTS.put(QuestDBDataType.CHAR, new QuestDBCharConstant());
        NULL_CONSTANTS.put(QuestDBDataType.INT, new QuestDBIntConstant());
        NULL_CONSTANTS.put(QuestDBDataType.LONG, new QuestDBLongConstant());
        NULL_CONSTANTS.put(QuestDBDataType.DATE, new QuestDBDateConstant());
        NULL_CONSTANTS.put(QuestDBDataType.TIMESTAMP, new QuestDBTimestampConstant());
        NULL_CONSTANTS.put(QuestDBDataType.FLOAT, new QuestDBFloatConstant());
        NULL_CONSTANTS.put(QuestDBDataType.DOUBLE, new QuestDBDoubleConstant());
        NULL_CONSTANTS.put(QuestDBDataType.STRING, new QuestDBStringConstant());
        NULL_CONSTANTS.put(QuestDBDataType.SYMBOL, new QuestDBSymbolConstant());
    }

    public static QuestDBConstant<?> getQuestDBNullConstant(QuestDBDataType type) {
        return NULL_CONSTANTS.get(type);
    }

    public static QuestDBConstant<?> createRandomQuestDBConstant(Randomly randomly) {
        QuestDBDataType type = QuestDBDataType.getNonNullRandom();
        if (Randomly.getBooleanWithSmallProbability()) {
            return getQuestDBNullConstant(type);
        }
        switch (type) {
            case BOOLEAN:
                return createQuestDBConstant(type, Randomly.getBoolean());
            case BYTE:
                return createQuestDBConstant(type, (byte) randomly.getInteger());
            case SHORT:
                return createQuestDBConstant(type, (short) randomly.getInteger());
            case CHAR:
                return createQuestDBConstant(type, (char) randomly.getInteger());
            case INT:
                return createQuestDBConstant(type, (int) randomly.getInteger());
            case LONG:
                return createQuestDBConstant(type, randomly.getInteger());
            case DATE:
            case TIMESTAMP:
                return createQuestDBConstant(type, QuestDBTimestampConstant.randomTimestampMicros());
            case FLOAT:
                return createQuestDBConstant(type, (float) randomly.getDouble());
            case DOUBLE:
                return createQuestDBConstant(type, randomly.getDouble());
            case STRING:
            case SYMBOL:
                return createQuestDBConstant(type, randomly.getString());
            default:
                throw new AssertionError("unknown type: " + type);
        }
    }

    public static <VT, T extends QuestDBConstant<VT>> T createQuestDBConstant(QuestDBDataType type, VT value) {
        QuestDBConstant<?> constant;
        switch (type) {
            case BOOLEAN:
                constant = new QuestDBBooleanConstant((Boolean) value);
                break;
            case BYTE:
                constant = new QuestDBByteConstant((Byte) value);
                break;
            case SHORT:
                constant = new QuestDBShortConstant((Short) value);
                break;
            case CHAR:
                constant = new QuestDBCharConstant((Character) value);
                break;
            case INT:
                constant = new QuestDBIntConstant((Integer) value);
                break;
            case LONG:
                constant = new QuestDBLongConstant((Long) value);
                break;
            case DATE:
                constant = new QuestDBDateConstant((Long) value);
                break;
            case TIMESTAMP:
                constant = new QuestDBTimestampConstant((Long) value);
                break;
            case FLOAT:
                constant = new QuestDBFloatConstant((Float) value);
                break;
            case DOUBLE:
                constant = new QuestDBDoubleConstant((Double) value);
                break;
            case STRING:
                constant = new QuestDBStringConstant((String) value);
                break;
            case SYMBOL:
                constant = new QuestDBSymbolConstant((String) value);
                break;
            default:
                throw new AssertionError("unknown type: " + type);
        }
        return (T) constant;
    }

    public static class QuestDBConstant<T> implements Node<QuestDBExpression> {

        protected final T value;
        protected final QuestDBDataType type;

        QuestDBConstant(QuestDBDataType type, T value) {
            this.type = type;
            this.value = value;
        }

        public T getValue() {
            return value;
        }

        public QuestDBDataType getType() {
            return type;
        }

        public boolean isNull() {
            return type == QuestDBDataType.NULL ||
                    (type.isNullable && value == NULL_CONSTANTS.get(type).getValue());
        }

        @Override
        public String toString() {
            return isNull() ? "" : String.valueOf(value);
        }
    }

    public static class QuestDBBooleanConstant extends QuestDBConstant<Boolean> {
        QuestDBBooleanConstant() {
            super(QuestDBDataType.NULL, false);
        }

        QuestDBBooleanConstant(boolean value) {
            super(QuestDBDataType.BOOLEAN, value);
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }

    public static class QuestDBByteConstant extends QuestDBConstant<Byte> {
        QuestDBByteConstant() {
            super(QuestDBDataType.NULL, (byte) 0);
        }

        QuestDBByteConstant(byte value) {
            super(QuestDBDataType.BYTE, value);
        }
    }

    public static class QuestDBShortConstant extends QuestDBConstant<Short> {
        QuestDBShortConstant() {
            super(QuestDBDataType.NULL, (short) 0);
        }

        QuestDBShortConstant(short value) {
            super(QuestDBDataType.SHORT, value);
        }
    }

    public static class QuestDBCharConstant extends QuestDBConstant<Character> {
        QuestDBCharConstant() {
            super(QuestDBDataType.NULL, (char) 0);
        }

        QuestDBCharConstant(char value) {
            super(QuestDBDataType.CHAR, value);
        }
    }

    public static class QuestDBIntConstant extends QuestDBConstant<Integer> {
        QuestDBIntConstant() {
            super(QuestDBDataType.NULL, Integer.MIN_VALUE);
        }

        QuestDBIntConstant(int value) {
            super(QuestDBDataType.INT, value);
        }
    }

    public static class QuestDBLongConstant extends QuestDBConstant<Long> {
        QuestDBLongConstant() {
            super(QuestDBDataType.NULL, Long.MIN_VALUE);
        }

        QuestDBLongConstant(long value) {
            super(QuestDBDataType.LONG, value);
        }
    }

    public static class QuestDBFloatConstant extends QuestDBConstant<Float> {
        QuestDBFloatConstant() {
            super(QuestDBDataType.NULL, Float.NaN);
        }

        QuestDBFloatConstant(float value) {
            super(QuestDBDataType.FLOAT, value);
        }
    }

    public static class QuestDBDoubleConstant extends QuestDBConstant<Double> {
        QuestDBDoubleConstant() {
            super(QuestDBDataType.NULL, Double.NaN);
        }

        QuestDBDoubleConstant(double value) {
            super(QuestDBDataType.DOUBLE, value);
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "cast('Infinity' as double)";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "cast('-Infinity' as double)";
            }
            return super.toString();
        }
    }

    private static abstract class QuestDBQuotedConstant extends QuestDBConstant<String> {
        QuestDBQuotedConstant(QuestDBDataType type, String value) {
            super(type, value);
        }

        @Override
        public String toString() {
            return isNull() ? "" : "'" + value + '\'';
        }
    }

    public static class QuestDBStringConstant extends QuestDBQuotedConstant {
        QuestDBStringConstant() {
            super(QuestDBDataType.NULL, null);
        }

        QuestDBStringConstant(String value) {
            super(QuestDBDataType.STRING, value);
        }
    }

    public static class QuestDBSymbolConstant extends QuestDBQuotedConstant {
        QuestDBSymbolConstant() {
            super(QuestDBDataType.NULL, null);
        }

        QuestDBSymbolConstant(String value) {
            super(QuestDBDataType.SYMBOL, value);
        }
    }

    public static class QuestDBDateConstant extends QuestDBConstant<Long> {
        QuestDBDateConstant() {
            super(QuestDBDataType.NULL, Long.MIN_VALUE);
        }

        QuestDBDateConstant(long value) {
            super(QuestDBDataType.DATE, value);
        }

        @Override
        public String toString() {
            return isNull() ? "" : "'" + DATE_FORMAT.format(new Date(value / 1000L)) + '\'';
        }
    }

    public static class QuestDBTimestampConstant extends QuestDBConstant<Long> {
        QuestDBTimestampConstant() {
            super(QuestDBDataType.NULL, Long.MIN_VALUE);
        }

        QuestDBTimestampConstant(long value) { // in micros
            super(QuestDBDataType.TIMESTAMP, value);
        }

        public static long timestampMicros(Instant instant) {
            return TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
        }

        public static Instant fromTimestampMicros(long timestampMicros) {
            long tsSeconds = timestampMicros / 1000000L;
            long nanoAdjust = (timestampMicros - tsSeconds * 1000000L) * 1000L;
            return Instant.ofEpochSecond(tsSeconds, nanoAdjust);
        }

        public static long randomTimestampMicros() {
            long nowMicro = System.currentTimeMillis() * 1000L;
            return ThreadLocalRandom.current().nextLong(
                    nowMicro - DAY_MICROS * 10,
                    nowMicro + DAY_MICROS * 2
            ); // QuestDB time is in micros
        }

        @Override
        public String toString() {
            return isNull() ? "" : "'" + fromTimestampMicros(value) + '\''; // with micro precision
        }
    }
}
