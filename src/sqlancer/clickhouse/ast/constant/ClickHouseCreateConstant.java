package sqlancer.clickhouse.ast.constant;

import java.math.BigInteger;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.clickhouse.ast.ClickHouseConstant;
import sqlancer.clickhouse.ast.ClickHouseExpression;

public abstract class ClickHouseCreateConstant extends ClickHouseExpression {

    public static ClickHouseConstant createStringConstant(String text) {
        return new ClickHouseStringConstant(text);
    }

    public static ClickHouseConstant createFloat64Constant(double val) {
        return new ClickHouseFloat64Constant(val);
    }

    public static ClickHouseConstant createFloat32Constant(float val) {
        return new ClickHouseFloat32Constant(val);
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

    public static ClickHouseConstant createBooleanConstant(Boolean b) {
        return new ClickHouseBooleanConstant(b);
    }

    public static ClickHouseConstant createNullConstant() {
        return new ClickHouseNullConstant();
    }

    public static ClickHouseConstant createTrue() {
        return new ClickHouseBooleanConstant(true);
    }

    public static ClickHouseConstant createFalse() {
        return new ClickHouseBooleanConstant(false);
    }

    public static ClickHouseConstant createBoolean(boolean val) {
        return val ? createTrue() : createFalse();
    }

    public static ClickHouseConstant createIntConstant(ClickHouseDataType type, long val) {
        switch (type) {
        case UInt256:
            return ClickHouseCreateConstant.createUInt256Constant(BigInteger.valueOf(val));
        case UInt128:
            return ClickHouseCreateConstant.createUInt128Constant(BigInteger.valueOf(val));
        case UInt64:
            return ClickHouseCreateConstant.createUInt64Constant(BigInteger.valueOf(val));
        case UInt32:
            return ClickHouseCreateConstant.createUInt32Constant(val);
        case UInt16:
            return ClickHouseCreateConstant.createUInt16Constant(val);
        case UInt8:
            return ClickHouseCreateConstant.createUInt8Constant(val);
        case Int256:
            return ClickHouseCreateConstant.createInt256Constant(BigInteger.valueOf(val));
        case Int128:
            return ClickHouseCreateConstant.createInt128Constant(BigInteger.valueOf(val));
        case Int64:
            return ClickHouseCreateConstant.createInt64Constant(BigInteger.valueOf(val));
        case Int32:
            return ClickHouseCreateConstant.createInt32Constant(val);
        case Int16:
            return ClickHouseCreateConstant.createInt16Constant(val);
        case Int8:
            return ClickHouseCreateConstant.createInt8Constant(val);
        case Nothing:
            return ClickHouseCreateConstant.createNullConstant();
        case Bool:
        case String:
        case IntervalYear:
        case IntervalQuarter:
        case IntervalMonth:
        case IntervalWeek:
        case IntervalDay:
        case IntervalHour:
        case IntervalMinute:
        case IntervalSecond:
        case Date:
        case Date32:
        case DateTime:
        case DateTime32:
        case DateTime64:
        case Decimal:
        case Decimal32:
        case Decimal64:
        case Decimal128:
        case Decimal256:
        case UUID:
        case Enum:
        case Enum8:
        case Enum16:
        case IPv4:
        case IPv6:
        case FixedString:
        case AggregateFunction:
        case SimpleAggregateFunction:
        case Array:
        case Map:
        case Nested:
        case Tuple:
        case Point:
        case Polygon:
        case MultiPolygon:
        case Ring:
        default:
            throw new AssertionError(type);
        }
    }

}
