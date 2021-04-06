package sqlancer.clickhouse.ast;

import java.math.BigInteger;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import sqlancer.IgnoreMeException;

public abstract class ClickHouseConstant extends ClickHouseExpression {

    public static class ClickHouseNullConstant extends ClickHouseConstant {

        @Override
        public String toString() {
            return "NULL";
        }

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public boolean asBooleanNotNull() {
            throw new AssertionError();
        }

        @Override
        public ClickHouseDataType getDataType() {
            return ClickHouseDataType.Nothing;
        }

        @Override
        public boolean compareInternal(Object value) {
            return false;
        }

        @Override
        public ClickHouseConstant applyEquals(ClickHouseConstant right) {
            return ClickHouseConstant.createNullConstant();
        }

        @Override
        public ClickHouseConstant applyLess(ClickHouseConstant right) {
            return ClickHouseConstant.createNullConstant();
        }

        @Override
        public Object getValue() {
            return null;
        }

        @Override
        public ClickHouseConstant cast(ClickHouseDataType type) {
            return null;
        }
    }

    public static class ClickHouseUInt8Constant extends ClickHouseConstant {

        private final int value;

        public ClickHouseUInt8Constant(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean asBooleanNotNull() {
            return value != 0;
        }

        @Override
        public ClickHouseDataType getDataType() {
            return ClickHouseDataType.UInt8;
        }

        @Override
        public boolean compareInternal(Object val) {
            return value == (int) val;
        }

        @Override
        public ClickHouseConstant applyLess(ClickHouseConstant right) {
            if (this.getDataType() == right.getDataType()) {
                return this.asInt() < right.asInt() ? ClickHouseConstant.createTrue()
                        : ClickHouseConstant.createFalse();
            }
            throw new IgnoreMeException();
        }

        @Override
        public long asInt() {
            return value;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public ClickHouseConstant cast(ClickHouseDataType type) {
            switch (type) {
            case String:
                return ClickHouseConstant.createStringConstant(this.toString());
            case UInt8:
                return ClickHouseConstant.createUInt8Constant(value);
            case Int8:
                return ClickHouseConstant.createInt8Constant(value);
            case UInt16:
                return ClickHouseConstant.createUInt16Constant(value);
            case Int16:
                return ClickHouseConstant.createInt16Constant(value);
            case UInt32:
                return ClickHouseConstant.createUInt32Constant(value);
            case Int32:
                return ClickHouseConstant.createInt32Constant(value);
            case UInt64:
                return ClickHouseConstant.createUInt64Constant(BigInteger.valueOf(value));
            case Int64:
                return ClickHouseConstant.createInt64Constant(BigInteger.valueOf(value));
            case UInt128:
                return ClickHouseConstant.createUInt128Constant(BigInteger.valueOf(value));
            case Int128:
                return ClickHouseConstant.createInt128Constant(BigInteger.valueOf(value));
            case UInt256:
                return ClickHouseConstant.createUInt256Constant(BigInteger.valueOf(value));
            case Int256:
                return ClickHouseConstant.createInt256Constant(BigInteger.valueOf(value));
            case Float32:
                return ClickHouseConstant.createFloat32Constant((float) value);
            case Float64:
                return ClickHouseConstant.createFloat64Constant((double) value);
            case Nothing:
                return ClickHouseConstant.createNullConstant();
            case IntervalYear:
            case IntervalQuarter:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalMinute:
            case IntervalSecond:
            case Date:
            case DateTime:
            case Enum8:
            case Enum16:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal:
            case UUID:
            case FixedString:
            case Nested:
            case Tuple:
            case Array:
            case AggregateFunction:
            case Unknown:
            default:
                throw new AssertionError(type);
            }
        }
    }

    public static class ClickHouseInt8Constant extends ClickHouseConstant {

        private final int value;

        public ClickHouseInt8Constant(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean asBooleanNotNull() {
            return value != 0;
        }

        @Override
        public ClickHouseDataType getDataType() {
            return ClickHouseDataType.Int8;
        }

        @Override
        public boolean compareInternal(Object val) {
            return value == (int) val;
        }

        @Override
        public ClickHouseConstant applyLess(ClickHouseConstant right) {
            if (this.getDataType() == right.getDataType()) {
                return this.asInt() < right.asInt() ? ClickHouseConstant.createTrue()
                        : ClickHouseConstant.createFalse();
            }
            throw new IgnoreMeException();
        }

        @Override
        public long asInt() {
            return value;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public ClickHouseConstant cast(ClickHouseDataType type) {
            switch (type) {
            case String:
                return ClickHouseConstant.createStringConstant(this.toString());
            case UInt8:
                return ClickHouseConstant.createUInt8Constant(value);
            case Int8:
                return ClickHouseConstant.createInt8Constant(value);
            case UInt16:
                return ClickHouseConstant.createUInt16Constant(value);
            case Int16:
                return ClickHouseConstant.createInt16Constant(value);
            case UInt32:
                return ClickHouseConstant.createUInt32Constant(value);
            case Int32:
                return ClickHouseConstant.createInt32Constant(value);
            case UInt64:
                return ClickHouseConstant.createUInt64Constant(BigInteger.valueOf(value));
            case Int64:
                return ClickHouseConstant.createInt64Constant(BigInteger.valueOf(value));
            case Float32:
                return ClickHouseConstant.createFloat32Constant((float) value);
            case Float64:
                return ClickHouseConstant.createFloat64Constant(value);
            case Nothing:
                return ClickHouseConstant.createNullConstant();
            case IntervalYear:
            case IntervalQuarter:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalMinute:
            case IntervalSecond:
            case Date:
            case DateTime:
            case Enum8:
            case Enum16:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal:
            case UUID:
            case FixedString:
            case Nested:
            case Tuple:
            case Array:
            case AggregateFunction:
            case Unknown:
            default:
                throw new AssertionError(type);
            }
        }
    }

    public static class ClickHouseUInt16Constant extends ClickHouseConstant {

        private final long value;

        public ClickHouseUInt16Constant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean asBooleanNotNull() {
            return value != 0;
        }

        @Override
        public ClickHouseDataType getDataType() {
            return ClickHouseDataType.UInt16;
        }

        @Override
        public boolean compareInternal(Object val) {
            return value == (long) val;
        }

        @Override
        public ClickHouseConstant applyLess(ClickHouseConstant right) {
            if (this.getDataType() == right.getDataType()) {
                return this.asInt() < right.asInt() ? ClickHouseConstant.createTrue()
                        : ClickHouseConstant.createFalse();
            }
            throw new IgnoreMeException();
        }

        @Override
        public long asInt() {
            return value;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public ClickHouseConstant cast(ClickHouseDataType type) {
            switch (type) {
            case String:
                return ClickHouseConstant.createStringConstant(this.toString());
            case UInt8:
                return ClickHouseConstant.createUInt8Constant(value);
            case Int8:
                return ClickHouseConstant.createInt8Constant(value);
            case UInt16:
                return ClickHouseConstant.createUInt16Constant(value);
            case Int16:
                return ClickHouseConstant.createInt16Constant(value);
            case UInt32:
                return ClickHouseConstant.createUInt32Constant(value);
            case Int32:
                return ClickHouseConstant.createInt32Constant(value);
            case UInt64:
                return ClickHouseConstant.createUInt64Constant(BigInteger.valueOf(value));
            case Int64:
                return ClickHouseConstant.createInt64Constant(BigInteger.valueOf(value));
            case Float32:
                return ClickHouseConstant.createFloat32Constant((float) value);
            case Float64:
                return ClickHouseConstant.createFloat64Constant(value);
            case Nothing:
                return ClickHouseConstant.createNullConstant();
            case IntervalYear:
            case IntervalQuarter:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalMinute:
            case IntervalSecond:
            case Date:
            case DateTime:
            case Enum8:
            case Enum16:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal:
            case UUID:
            case FixedString:
            case Nested:
            case Tuple:
            case Array:
            case AggregateFunction:
            case Unknown:
            default:
                throw new AssertionError(type);
            }
        }
    }

    public static class ClickHouseInt16Constant extends ClickHouseConstant {

        private final long value;

        public ClickHouseInt16Constant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean asBooleanNotNull() {
            return value != 0;
        }

        @Override
        public ClickHouseDataType getDataType() {
            return ClickHouseDataType.Int16;
        }

        @Override
        public boolean compareInternal(Object val) {
            return value == (long) val;
        }

        @Override
        public ClickHouseConstant applyLess(ClickHouseConstant right) {
            if (this.getDataType() == right.getDataType()) {
                return this.asInt() < right.asInt() ? ClickHouseConstant.createTrue()
                        : ClickHouseConstant.createFalse();
            }
            throw new IgnoreMeException();
        }

        @Override
        public long asInt() {
            return value;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public ClickHouseConstant cast(ClickHouseDataType type) {
            switch (type) {
            case String:
                return ClickHouseConstant.createStringConstant(this.toString());
            case UInt8:
                return ClickHouseConstant.createUInt8Constant(value);
            case Int8:
                return ClickHouseConstant.createInt8Constant(value);
            case UInt16:
                return ClickHouseConstant.createUInt16Constant(value);
            case Int16:
                return ClickHouseConstant.createInt16Constant(value);
            case UInt32:
                return ClickHouseConstant.createUInt32Constant(value);
            case Int32:
                return ClickHouseConstant.createInt32Constant(value);
            case UInt64:
                return ClickHouseConstant.createUInt64Constant(BigInteger.valueOf(value));
            case Int64:
                return ClickHouseConstant.createInt64Constant(BigInteger.valueOf(value));
            case Float32:
                return ClickHouseConstant.createFloat32Constant((float) value);
            case Float64:
                return ClickHouseConstant.createFloat64Constant(value);
            case Nothing:
                return ClickHouseConstant.createNullConstant();
            case IntervalYear:
            case IntervalQuarter:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalMinute:
            case IntervalSecond:
            case Date:
            case DateTime:
            case Enum8:
            case Enum16:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal:
            case UUID:
            case FixedString:
            case Nested:
            case Tuple:
            case Array:
            case AggregateFunction:
            case Unknown:
            default:
                throw new AssertionError(type);
            }
        }
    }

    public static class ClickHouseUInt32Constant extends ClickHouseConstant {

        private final long value;

        public ClickHouseUInt32Constant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean asBooleanNotNull() {
            return value != 0;
        }

        @Override
        public ClickHouseDataType getDataType() {
            return ClickHouseDataType.UInt32;
        }

        @Override
        public boolean compareInternal(Object val) {
            return value == (long) val;
        }

        @Override
        public ClickHouseConstant applyLess(ClickHouseConstant right) {
            if (this.getDataType() == right.getDataType()) {
                return this.asInt() < right.asInt() ? ClickHouseConstant.createTrue()
                        : ClickHouseConstant.createFalse();
            }
            throw new IgnoreMeException();
        }

        @Override
        public long asInt() {
            return value;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public ClickHouseConstant cast(ClickHouseDataType type) {
            switch (type) {
            case String:
                return ClickHouseConstant.createStringConstant(this.toString());
            case UInt8:
                return ClickHouseConstant.createUInt8Constant(value);
            case Int8:
                return ClickHouseConstant.createInt8Constant(value);
            case UInt16:
                return ClickHouseConstant.createUInt16Constant(value);
            case Int16:
                return ClickHouseConstant.createInt16Constant(value);
            case UInt32:
                return ClickHouseConstant.createUInt32Constant(value);
            case Int32:
                return ClickHouseConstant.createInt32Constant(value);
            case UInt64:
                return ClickHouseConstant.createUInt64Constant(BigInteger.valueOf(value));
            case Int64:
                return ClickHouseConstant.createInt64Constant(BigInteger.valueOf(value));
            case Float32:
                return ClickHouseConstant.createFloat32Constant((float) value);
            case Float64:
                return ClickHouseConstant.createFloat64Constant(value);
            case Nothing:
                return ClickHouseConstant.createNullConstant();
            case IntervalYear:
            case IntervalQuarter:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalMinute:
            case IntervalSecond:
            case Date:
            case DateTime:
            case Enum8:
            case Enum16:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal:
            case UUID:
            case FixedString:
            case Nested:
            case Tuple:
            case Array:
            case AggregateFunction:
            case Unknown:
            default:
                throw new AssertionError(type);
            }
        }
    }

    public static class ClickHouseInt32Constant extends ClickHouseConstant {

        private final long value;

        public ClickHouseInt32Constant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean asBooleanNotNull() {
            return value != 0;
        }

        @Override
        public ClickHouseDataType getDataType() {
            return ClickHouseDataType.Int32;
        }

        @Override
        public boolean compareInternal(Object val) {
            return value == (long) val;
        }

        @Override
        public ClickHouseConstant applyLess(ClickHouseConstant right) {
            if (this.getDataType() == right.getDataType()) {
                return this.asInt() < right.asInt() ? ClickHouseConstant.createTrue()
                        : ClickHouseConstant.createFalse();
            }
            throw new IgnoreMeException();
        }

        @Override
        public long asInt() {
            return value;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public ClickHouseConstant cast(ClickHouseDataType type) {
            switch (type) {
            case String:
                return ClickHouseConstant.createStringConstant(this.toString());
            case UInt8:
                return ClickHouseConstant.createUInt8Constant(value);
            case Int8:
                return ClickHouseConstant.createInt8Constant(value);
            case UInt16:
                return ClickHouseConstant.createUInt16Constant(value);
            case Int16:
                return ClickHouseConstant.createInt16Constant(value);
            case UInt32:
                return ClickHouseConstant.createUInt32Constant(value);
            case Int32:
                return ClickHouseConstant.createInt32Constant(value);
            case UInt64:
                return ClickHouseConstant.createUInt64Constant(BigInteger.valueOf(value));
            case Int64:
                return ClickHouseConstant.createInt64Constant(BigInteger.valueOf(value));
            case Float32:
                return ClickHouseConstant.createFloat32Constant((float) value);
            case Float64:
                return ClickHouseConstant.createFloat64Constant(value);
            case Nothing:
                return ClickHouseConstant.createNullConstant();
            case IntervalYear:
            case IntervalQuarter:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalMinute:
            case IntervalSecond:
            case Date:
            case DateTime:
            case Enum8:
            case Enum16:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal:
            case UUID:
            case FixedString:
            case Nested:
            case Tuple:
            case Array:
            case AggregateFunction:
            case Unknown:
            default:
                throw new AssertionError(type);
            }
        }
    }

    public static class ClickHouseUInt64Constant extends ClickHouseConstant {

        private final BigInteger value;

        public ClickHouseUInt64Constant(BigInteger value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean asBooleanNotNull() {
            return value != BigInteger.ZERO;
        }

        @Override
        public ClickHouseDataType getDataType() {
            return ClickHouseDataType.UInt64;
        }

        @Override
        public boolean compareInternal(Object val) {
            return value.compareTo((BigInteger) val) == 0;
        }

        @Override
        public ClickHouseConstant applyLess(ClickHouseConstant right) {
            if (this.getDataType() == right.getDataType()) {
                return this.asInt() < right.asInt() ? ClickHouseConstant.createTrue()
                        : ClickHouseConstant.createFalse();
            }
            throw new IgnoreMeException();
        }

        @Override
        public long asInt() {
            return value.longValueExact();
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public ClickHouseConstant cast(ClickHouseDataType type) {
            long val = value.longValueExact();
            switch (type) {
            case String:
                return ClickHouseConstant.createStringConstant(this.toString());
            case UInt8:
                return ClickHouseConstant.createUInt8Constant(val);
            case Int8:
                return ClickHouseConstant.createInt8Constant(val);
            case UInt16:
                return ClickHouseConstant.createUInt16Constant(val);
            case Int16:
                return ClickHouseConstant.createInt16Constant(val);
            case UInt32:
                return ClickHouseConstant.createUInt32Constant(val);
            case Int32:
                return ClickHouseConstant.createInt32Constant(val);
            case UInt64:
                return ClickHouseConstant.createUInt64Constant(value);
            case Int64:
                return ClickHouseConstant.createInt64Constant(value);
            case Float32:
                return ClickHouseConstant.createFloat32Constant(value.floatValue());
            case Float64:
                return ClickHouseConstant.createFloat64Constant(value.doubleValue());
            case Nothing:
                return ClickHouseConstant.createNullConstant();
            case IntervalYear:
            case IntervalQuarter:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalMinute:
            case IntervalSecond:
            case Date:
            case DateTime:
            case Enum8:
            case Enum16:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal:
            case UUID:
            case FixedString:
            case Nested:
            case Tuple:
            case Array:
            case AggregateFunction:
            case Unknown:
            default:
                throw new AssertionError(type);
            }
        }
    }

    public static class ClickHouseInt64Constant extends ClickHouseConstant {

        private final BigInteger value;

        public ClickHouseInt64Constant(BigInteger value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean asBooleanNotNull() {
            return value != BigInteger.ZERO;
        }

        @Override
        public ClickHouseDataType getDataType() {
            return ClickHouseDataType.Int64;
        }

        @Override
        public boolean compareInternal(Object val) {
            return value.compareTo((BigInteger) val) == 0;
        }

        @Override
        public ClickHouseConstant applyLess(ClickHouseConstant right) {
            if (this.getDataType() == right.getDataType()) {
                return this.asInt() < right.asInt() ? ClickHouseConstant.createTrue()
                        : ClickHouseConstant.createFalse();
            }
            throw new IgnoreMeException();
        }

        @Override
        public long asInt() {
            return value.longValueExact();
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public ClickHouseConstant cast(ClickHouseDataType type) {
            long val = value.longValueExact();
            switch (type) {
            case String:
                return ClickHouseConstant.createStringConstant(this.toString());
            case UInt8:
                return ClickHouseConstant.createUInt8Constant(val);
            case Int8:
                return ClickHouseConstant.createInt8Constant(val);
            case UInt16:
                return ClickHouseConstant.createUInt16Constant(val);
            case Int16:
                return ClickHouseConstant.createInt16Constant(val);
            case UInt32:
                return ClickHouseConstant.createUInt32Constant(val);
            case Int32:
                return ClickHouseConstant.createInt32Constant(val);
            case UInt64:
                return ClickHouseConstant.createUInt64Constant(value);
            case Int64:
                return ClickHouseConstant.createInt64Constant(value);
            case Float32:
                return ClickHouseConstant.createFloat32Constant(value.floatValue());
            case Float64:
                return ClickHouseConstant.createFloat64Constant(value.doubleValue());
            case Nothing:
                return ClickHouseConstant.createNullConstant();
            case IntervalYear:
            case IntervalQuarter:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalMinute:
            case IntervalSecond:
            case Date:
            case DateTime:
            case Enum8:
            case Enum16:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal:
            case UUID:
            case FixedString:
            case Nested:
            case Tuple:
            case Array:
            case AggregateFunction:
            case Unknown:
            default:
                throw new AssertionError(type);
            }
        }
    }

    public static class ClickHouseUInt128Constant extends ClickHouseConstant {

        private final BigInteger value;

        public ClickHouseUInt128Constant(BigInteger value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean asBooleanNotNull() {
            return value != BigInteger.ZERO;
        }

        @Override
        public ClickHouseDataType getDataType() {
            return ClickHouseDataType.UInt128;
        }

        @Override
        public boolean compareInternal(Object val) {
            return value.compareTo((BigInteger) val) == 0;
        }

        @Override
        public ClickHouseConstant applyLess(ClickHouseConstant right) {
            if (this.getDataType() == right.getDataType()) {
                return this.asInt() < right.asInt() ? ClickHouseConstant.createTrue()
                        : ClickHouseConstant.createFalse();
            }
            throw new IgnoreMeException();
        }

        @Override
        public long asInt() {
            return value.longValueExact();
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public ClickHouseConstant cast(ClickHouseDataType type) {
            long val = value.longValueExact();
            switch (type) {
            case String:
                return ClickHouseConstant.createStringConstant(this.toString());
            case UInt8:
                return ClickHouseConstant.createUInt8Constant(val);
            case Int8:
                return ClickHouseConstant.createInt8Constant(val);
            case UInt16:
                return ClickHouseConstant.createUInt16Constant(val);
            case Int16:
                return ClickHouseConstant.createInt16Constant(val);
            case UInt32:
                return ClickHouseConstant.createUInt32Constant(val);
            case Int32:
                return ClickHouseConstant.createInt32Constant(val);
            case UInt64:
                return ClickHouseConstant.createUInt64Constant(value);
            case Int64:
                return ClickHouseConstant.createInt64Constant(value);
            case UInt128:
                return ClickHouseConstant.createUInt128Constant(value);
            case Int128:
                return ClickHouseConstant.createInt128Constant(value);
            case UInt256:
                return ClickHouseConstant.createUInt256Constant(value);
            case Int256:
                return ClickHouseConstant.createInt256Constant(value);
            case Float32:
                return ClickHouseConstant.createFloat32Constant(value.floatValue());
            case Float64:
                return ClickHouseConstant.createFloat64Constant(value.doubleValue());
            case Nothing:
                return ClickHouseConstant.createNullConstant();
            case IntervalYear:
            case IntervalQuarter:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalMinute:
            case IntervalSecond:
            case Date:
            case DateTime:
            case Enum8:
            case Enum16:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal:
            case UUID:
            case FixedString:
            case Nested:
            case Tuple:
            case Array:
            case AggregateFunction:
            case Unknown:
            default:
                throw new AssertionError(type);
            }
        }
    }

    public static class ClickHouseInt128Constant extends ClickHouseConstant {

        private final BigInteger value;

        public ClickHouseInt128Constant(BigInteger value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean asBooleanNotNull() {
            return value != BigInteger.ZERO;
        }

        @Override
        public ClickHouseDataType getDataType() {
            return ClickHouseDataType.Int128;
        }

        @Override
        public boolean compareInternal(Object val) {
            return value.compareTo((BigInteger) val) == 0;
        }

        @Override
        public ClickHouseConstant applyLess(ClickHouseConstant right) {
            if (this.getDataType() == right.getDataType()) {
                return this.asInt() < right.asInt() ? ClickHouseConstant.createTrue()
                        : ClickHouseConstant.createFalse();
            }
            throw new IgnoreMeException();
        }

        @Override
        public long asInt() {
            return value.longValueExact();
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public ClickHouseConstant cast(ClickHouseDataType type) {
            long val = value.longValueExact();
            switch (type) {
            case String:
                return ClickHouseConstant.createStringConstant(this.toString());
            case UInt8:
                return ClickHouseConstant.createUInt8Constant(val);
            case Int8:
                return ClickHouseConstant.createInt8Constant(val);
            case UInt16:
                return ClickHouseConstant.createUInt16Constant(val);
            case Int16:
                return ClickHouseConstant.createInt16Constant(val);
            case UInt32:
                return ClickHouseConstant.createUInt32Constant(val);
            case Int32:
                return ClickHouseConstant.createInt32Constant(val);
            case UInt64:
                return ClickHouseConstant.createUInt64Constant(value);
            case Int64:
                return ClickHouseConstant.createInt64Constant(value);
            case UInt128:
                return ClickHouseConstant.createUInt128Constant(value);
            case Int128:
                return ClickHouseConstant.createInt128Constant(value);
            case UInt256:
                return ClickHouseConstant.createUInt256Constant(value);
            case Int256:
                return ClickHouseConstant.createInt256Constant(value);
            case Float32:
                return ClickHouseConstant.createFloat32Constant(value.floatValue());
            case Float64:
                return ClickHouseConstant.createFloat64Constant(value.doubleValue());
            case Nothing:
                return ClickHouseConstant.createNullConstant();
            case IntervalYear:
            case IntervalQuarter:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalMinute:
            case IntervalSecond:
            case Date:
            case DateTime:
            case Enum8:
            case Enum16:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal:
            case UUID:
            case FixedString:
            case Nested:
            case Tuple:
            case Array:
            case AggregateFunction:
            case Unknown:
            default:
                throw new AssertionError(type);
            }
        }
    }

    public static class ClickHouseUInt256Constant extends ClickHouseConstant {

        private final BigInteger value;

        public ClickHouseUInt256Constant(BigInteger value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean asBooleanNotNull() {
            return value != BigInteger.ZERO;
        }

        @Override
        public ClickHouseDataType getDataType() {
            return ClickHouseDataType.UInt256;
        }

        @Override
        public boolean compareInternal(Object val) {
            return value.compareTo((BigInteger) val) == 0;
        }

        @Override
        public ClickHouseConstant applyLess(ClickHouseConstant right) {
            if (this.getDataType() == right.getDataType()) {
                return this.asInt() < right.asInt() ? ClickHouseConstant.createTrue()
                        : ClickHouseConstant.createFalse();
            }
            throw new IgnoreMeException();
        }

        @Override
        public long asInt() {
            return value.longValueExact();
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public ClickHouseConstant cast(ClickHouseDataType type) {
            long val = value.longValueExact();
            switch (type) {
            case String:
                return ClickHouseConstant.createStringConstant(this.toString());
            case UInt8:
                return ClickHouseConstant.createUInt8Constant(val);
            case Int8:
                return ClickHouseConstant.createInt8Constant(val);
            case UInt16:
                return ClickHouseConstant.createUInt16Constant(val);
            case Int16:
                return ClickHouseConstant.createInt16Constant(val);
            case UInt32:
                return ClickHouseConstant.createUInt32Constant(val);
            case Int32:
                return ClickHouseConstant.createInt32Constant(val);
            case UInt64:
                return ClickHouseConstant.createUInt64Constant(value);
            case Int64:
                return ClickHouseConstant.createInt64Constant(value);
            case UInt128:
                return ClickHouseConstant.createUInt128Constant(value);
            case Int128:
                return ClickHouseConstant.createInt128Constant(value);
            case UInt256:
                return ClickHouseConstant.createUInt256Constant(value);
            case Int256:
                return ClickHouseConstant.createInt256Constant(value);
            case Float32:
                return ClickHouseConstant.createFloat32Constant(value.floatValue());
            case Float64:
                return ClickHouseConstant.createFloat64Constant(value.doubleValue());
            case Nothing:
                return ClickHouseConstant.createNullConstant();
            case IntervalYear:
            case IntervalQuarter:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalMinute:
            case IntervalSecond:
            case Date:
            case DateTime:
            case Enum8:
            case Enum16:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal:
            case UUID:
            case FixedString:
            case Nested:
            case Tuple:
            case Array:
            case AggregateFunction:
            case Unknown:
            default:
                throw new AssertionError(type);
            }
        }
    }

    public static class ClickHouseInt256Constant extends ClickHouseConstant {

        private final BigInteger value;

        public ClickHouseInt256Constant(BigInteger value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean asBooleanNotNull() {
            return value != BigInteger.ZERO;
        }

        @Override
        public ClickHouseDataType getDataType() {
            return ClickHouseDataType.Int256;
        }

        @Override
        public boolean compareInternal(Object val) {
            return value.compareTo((BigInteger) val) == 0;
        }

        @Override
        public ClickHouseConstant applyLess(ClickHouseConstant right) {
            if (this.getDataType() == right.getDataType()) {
                return this.asInt() < right.asInt() ? ClickHouseConstant.createTrue()
                        : ClickHouseConstant.createFalse();
            }
            throw new IgnoreMeException();
        }

        @Override
        public long asInt() {
            return value.longValueExact();
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public ClickHouseConstant cast(ClickHouseDataType type) {
            long val = value.longValueExact();
            switch (type) {
            case String:
                return ClickHouseConstant.createStringConstant(this.toString());
            case UInt8:
                return ClickHouseConstant.createUInt8Constant(val);
            case Int8:
                return ClickHouseConstant.createInt8Constant(val);
            case UInt16:
                return ClickHouseConstant.createUInt16Constant(val);
            case Int16:
                return ClickHouseConstant.createInt16Constant(val);
            case UInt32:
                return ClickHouseConstant.createUInt32Constant(val);
            case Int32:
                return ClickHouseConstant.createInt32Constant(val);
            case UInt64:
                return ClickHouseConstant.createUInt64Constant(value);
            case Int64:
                return ClickHouseConstant.createInt64Constant(value);
            case UInt128:
                return ClickHouseConstant.createUInt128Constant(value);
            case Int128:
                return ClickHouseConstant.createInt128Constant(value);
            case UInt256:
                return ClickHouseConstant.createUInt256Constant(value);
            case Int256:
                return ClickHouseConstant.createInt256Constant(value);
            case Float32:
                return ClickHouseConstant.createFloat32Constant(value.floatValue());
            case Float64:
                return ClickHouseConstant.createFloat64Constant(value.doubleValue());
            case Nothing:
                return ClickHouseConstant.createNullConstant();
            case IntervalYear:
            case IntervalQuarter:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalMinute:
            case IntervalSecond:
            case Date:
            case DateTime:
            case Enum8:
            case Enum16:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal:
            case UUID:
            case FixedString:
            case Nested:
            case Tuple:
            case Array:
            case AggregateFunction:
            case Unknown:
            default:
                throw new AssertionError(type);
            }
        }
    }

    public static class ClickHouseFloat32Constant extends ClickHouseConstant {

        private final float value;

        public ClickHouseFloat32Constant(float value) {
            this.value = value;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "'+Inf'";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "'-Inf'";
            }
            return String.valueOf(value);
        }

        @Override
        public boolean compareInternal(Object val) {
            return Float.compare(value, (float) val) == 0;
        }

        @Override
        public ClickHouseConstant applyLess(ClickHouseConstant right) {
            if (this.getDataType() == right.getDataType()) {
                return this.asDouble() < right.asDouble() ? ClickHouseConstant.createTrue()
                        : ClickHouseConstant.createFalse();
            }
            ClickHouseConstant converted = right.cast(ClickHouseDataType.Float32);
            return this.asDouble() < converted.asDouble() ? ClickHouseConstant.createTrue()
                    : ClickHouseConstant.createFalse();
        }

        @Override
        public boolean asBooleanNotNull() {
            return Float.compare(value, (float) 0) == 0;
        }

        @Override
        public ClickHouseDataType getDataType() {
            return ClickHouseDataType.Float32;
        }

        @Override
        public double asDouble() {
            return value;
        }

        @Override
        public ClickHouseConstant cast(ClickHouseDataType type) {
            switch (type) {
            case String:
                return ClickHouseConstant.createStringConstant(this.toString());
            case UInt8:
                return ClickHouseConstant.createUInt8Constant((long) value);
            case Int8:
                return ClickHouseConstant.createInt8Constant((long) value);
            case UInt16:
                return ClickHouseConstant.createUInt16Constant((long) value);
            case Int16:
                return ClickHouseConstant.createInt16Constant((long) value);
            case UInt32:
                return ClickHouseConstant.createUInt32Constant((long) value);
            case Int32:
                return ClickHouseConstant.createInt32Constant((long) value);
            case UInt64:
                return ClickHouseConstant.createUInt64Constant(BigInteger.valueOf((long) value));
            case Int64:
                return ClickHouseConstant.createInt64Constant(BigInteger.valueOf((long) value));
            case Float32:
                return ClickHouseConstant.createFloat32Constant(value);
            case Float64:
                return ClickHouseConstant.createFloat64Constant(value);
            case Nothing:
                return ClickHouseConstant.createNullConstant();
            case IntervalYear:
            case IntervalQuarter:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalMinute:
            case IntervalSecond:
            case Date:
            case DateTime:
            case Enum8:
            case Enum16:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal:
            case UUID:
            case FixedString:
            case Nested:
            case Tuple:
            case Array:
            case AggregateFunction:
            case Unknown:
            default:
                throw new AssertionError(type);
            }
        }
    }

    public static class ClickHouseFloat64Constant extends ClickHouseConstant {

        private final double value;

        public ClickHouseFloat64Constant(double value) {
            this.value = value;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "'+Inf'";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "'-Inf'";
            }
            return String.valueOf(value);
        }

        @Override
        public boolean compareInternal(Object val) {
            return Double.compare(value, (double) val) == 0;
        }

        @Override
        public ClickHouseConstant applyLess(ClickHouseConstant right) {
            if (this.getDataType() == right.getDataType()) {
                return this.asDouble() < right.asDouble() ? ClickHouseConstant.createTrue()
                        : ClickHouseConstant.createFalse();
            }
            ClickHouseConstant converted = right.cast(ClickHouseDataType.Float64);
            return this.asDouble() < converted.asDouble() ? ClickHouseConstant.createTrue()
                    : ClickHouseConstant.createFalse();
        }

        @Override
        public boolean asBooleanNotNull() {
            return Double.compare(value, 0.0) == 0;
        }

        @Override
        public ClickHouseDataType getDataType() {
            return ClickHouseDataType.Float64;
        }

        @Override
        public double asDouble() {
            return value;
        }

        @Override
        public ClickHouseConstant cast(ClickHouseDataType type) {
            switch (type) {
            case String:
                return ClickHouseConstant.createStringConstant(this.toString());
            case UInt8:
                return ClickHouseConstant.createUInt8Constant((long) value);
            case Int8:
                return ClickHouseConstant.createInt8Constant((long) value);
            case UInt16:
                return ClickHouseConstant.createUInt16Constant((long) value);
            case Int16:
                return ClickHouseConstant.createInt16Constant((long) value);
            case UInt32:
                return ClickHouseConstant.createUInt32Constant((long) value);
            case Int32:
                return ClickHouseConstant.createInt32Constant((long) value);
            case UInt64:
                return ClickHouseConstant.createUInt64Constant(BigInteger.valueOf((long) value));
            case Int64:
                return ClickHouseConstant.createInt64Constant(BigInteger.valueOf((long) value));
            case Float32:
                return ClickHouseConstant.createFloat32Constant((float) value);
            case Float64:
                return ClickHouseConstant.createFloat64Constant(value);
            case Nothing:
                return ClickHouseConstant.createNullConstant();
            case IntervalYear:
            case IntervalQuarter:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalMinute:
            case IntervalSecond:
            case Date:
            case DateTime:
            case Enum8:
            case Enum16:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal:
            case UUID:
            case FixedString:
            case Nested:
            case Tuple:
            case Array:
            case AggregateFunction:
            case Unknown:
            default:
                throw new AssertionError(type);
            }
        }
    }

    public static class ClickHouseStringConstant extends ClickHouseConstant {

        private final String value;

        public ClickHouseStringConstant(String value) {
            this.value = value;
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
        }

        @Override
        public boolean asBooleanNotNull() {
            return value.length() > 0;
        }

        @Override
        public boolean compareInternal(Object val) {
            return value.compareTo((String) val) == 0;
        }

        @Override
        public ClickHouseConstant applyLess(ClickHouseConstant right) {
            if (this.getDataType() == right.getDataType()) {
                return this.asString().compareTo(right.asString()) <= 0 ? ClickHouseConstant.createTrue()
                        : ClickHouseConstant.createFalse();
            }
            throw new IgnoreMeException();
        }

        @Override
        public ClickHouseDataType getDataType() {
            return ClickHouseDataType.String;
        }

        @Override
        public ClickHouseConstant cast(ClickHouseDataType type) {
            switch (type) {
            case String:
                return ClickHouseConstant.createStringConstant(this.toString());
            case UInt8:
                return ClickHouseConstant.createUInt8Constant(Integer.parseInt(value));
            case Int8:
                return ClickHouseConstant.createInt8Constant(Integer.parseInt(value));
            case UInt16:
                return ClickHouseConstant.createUInt16Constant(Integer.parseInt(value));
            case Int16:
                return ClickHouseConstant.createInt16Constant(Integer.parseInt(value));
            case UInt32:
                return ClickHouseConstant.createUInt32Constant(Integer.parseInt(value));
            case Int32:
                return ClickHouseConstant.createInt32Constant(Integer.parseInt(value));
            case UInt64:
                return ClickHouseConstant.createUInt64Constant(BigInteger.valueOf(Integer.parseInt(value)));
            case Int64:
                return ClickHouseConstant.createInt64Constant(BigInteger.valueOf(Integer.parseInt(value)));
            case Float32:
                return ClickHouseConstant.createFloat32Constant((float) Float.parseFloat(value));
            case Float64:
                return ClickHouseConstant.createFloat64Constant((double) Double.parseDouble(value));
            case Nothing:
                return ClickHouseConstant.createNullConstant();
            case IntervalYear:
            case IntervalQuarter:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalMinute:
            case IntervalSecond:
            case Date:
            case DateTime:
            case Enum8:
            case Enum16:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal:
            case UUID:
            case FixedString:
            case Nested:
            case Tuple:
            case Array:
            case AggregateFunction:
            case Unknown:
            default:
                throw new AssertionError(type);
            }
        }
    }

    public static ClickHouseConstant createStringConstant(String text) {
        return new ClickHouseStringConstant(text);
    }

    public static ClickHouseConstant createFloat64Constant(double val) {
        return new ClickHouseFloat64Constant(val);
    }

    public static ClickHouseConstant createFloat32Constant(float val) {
        return new ClickHouseFloat32Constant(val);
    }

    public static ClickHouseConstant createIntConstant(ClickHouseDataType type, long val) {
        switch (type) {
        case IntervalYear:
            break;
        case IntervalQuarter:
            break;
        case IntervalMonth:
            break;
        case IntervalWeek:
            break;
        case IntervalDay:
            break;
        case IntervalHour:
            break;
        case IntervalMinute:
            break;
        case IntervalSecond:
            break;
        case UInt256:
            return createUInt256Constant(BigInteger.valueOf(val));
        case UInt128:
            return createUInt128Constant(BigInteger.valueOf(val));
        case UInt64:
            return createUInt64Constant(BigInteger.valueOf(val));
        case UInt32:
            return createUInt32Constant(val);
        case UInt16:
            return createUInt16Constant(val);
        case UInt8:
            return createUInt8Constant(val);
        case Int256:
            return createInt256Constant(BigInteger.valueOf(val));
        case Int128:
            return createInt256Constant(BigInteger.valueOf(val));
        case Int64:
            return createInt64Constant(BigInteger.valueOf(val));
        case Int32:
            return createInt32Constant(val);
        case Int16:
            return createInt16Constant(val);
        case Int8:
            return createInt8Constant(val);
        case Date:
            break;
        case DateTime:
            break;
        case Enum8:
            break;
        case Enum16:
            break;
        case Float32:
            break;
        case Float64:
            break;
        case Decimal32:
            break;
        case Decimal64:
            break;
        case Decimal128:
            break;
        case Decimal:
            break;
        case UUID:
            break;
        case String:
            break;
        case FixedString:
            break;
        case Nothing:
            break;
        case Nested:
            break;
        case Tuple:
            break;
        case Array:
            break;
        case AggregateFunction:
            break;
        case Unknown:
            break;
        default:
            break;
        }
        throw new AssertionError(type);
    }

    public static ClickHouseConstant createInt256Constant(BigInteger val) {
        return new ClickHouseInt256Constant(val);
    }

    public static ClickHouseConstant createUInt256Constant(BigInteger val) {
        return new ClickHouseUInt256Constant(val);
    }

    public static ClickHouseConstant createInt128Constant(BigInteger val) {
        return new ClickHouseInt128Constant(val);
    }

    public static ClickHouseConstant createUInt128Constant(BigInteger val) {
        return new ClickHouseUInt128Constant(val);
    }

    public static ClickHouseConstant createInt64Constant(BigInteger val) {
        return new ClickHouseInt64Constant(val);
    }

    public static ClickHouseConstant createUInt64Constant(BigInteger val) {
        return new ClickHouseUInt64Constant(val);
    }

    public static ClickHouseConstant createInt32Constant(long val) {
        return new ClickHouseInt32Constant(val);
    }

    public static ClickHouseConstant createUInt32Constant(long val) {
        return new ClickHouseUInt32Constant(val);
    }

    public static ClickHouseConstant createUInt16Constant(long val) {
        return new ClickHouseUInt16Constant(val);
    }

    public static ClickHouseConstant createInt16Constant(long val) {
        return new ClickHouseInt16Constant(val);
    }

    public static ClickHouseConstant createUInt8Constant(long val) {
        return new ClickHouseUInt8Constant((int) val);
    }

    public static ClickHouseConstant createInt8Constant(long val) {
        return new ClickHouseInt8Constant((int) val);
    }

    public abstract boolean isNull();

    public static ClickHouseConstant createNullConstant() {
        return new ClickHouseNullConstant();
    }

    public static ClickHouseConstant createTrue() {
        return new ClickHouseUInt8Constant(1);
    }

    public static ClickHouseConstant createFalse() {
        return new ClickHouseUInt8Constant(0);
    }

    public static ClickHouseConstant createBoolean(boolean val) {
        return val ? createTrue() : createFalse();
    }

    public abstract ClickHouseConstant cast(ClickHouseDataType type);

    public abstract boolean asBooleanNotNull();

    public abstract ClickHouseDataType getDataType();

    public abstract boolean compareInternal(Object value);

    public ClickHouseConstant applyEquals(ClickHouseConstant right) {
        if (this.getDataType() == right.getDataType()) {
            return this.compareInternal(right.getValue()) ? ClickHouseConstant.createTrue()
                    : ClickHouseConstant.createFalse();
        } else {
            ClickHouseConstant converted = right.cast(this.getDataType());
            return this.applyEquals(converted);
        }
    }

    public abstract ClickHouseConstant applyLess(ClickHouseConstant right);

    public abstract Object getValue();

    public long asInt() {
        throw new UnsupportedOperationException(this.getDataType().toString());
    }

    public double asDouble() {
        throw new UnsupportedOperationException(this.getDataType().toString());
    }

    public String asString() {
        throw new UnsupportedOperationException(this.getDataType().toString());
    }
}
