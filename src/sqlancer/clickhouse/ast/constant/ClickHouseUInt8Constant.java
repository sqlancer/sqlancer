package sqlancer.clickhouse.ast.constant;

import java.math.BigInteger;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.IgnoreMeException;
import sqlancer.clickhouse.ast.ClickHouseConstant;

public class ClickHouseUInt8Constant extends ClickHouseConstant {

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
            return this.asInt() < right.asInt() ? ClickHouseCreateConstant.createTrue()
                    : ClickHouseCreateConstant.createFalse();
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
            return ClickHouseCreateConstant.createStringConstant(this.toString());
        case UInt8:
            return ClickHouseCreateConstant.createUInt8Constant(value);
        case Int8:
            return ClickHouseCreateConstant.createInt8Constant(value);
        case UInt16:
            return ClickHouseCreateConstant.createUInt16Constant(value);
        case Int16:
            return ClickHouseCreateConstant.createInt16Constant(value);
        case UInt32:
            return ClickHouseCreateConstant.createUInt32Constant(value);
        case Int32:
            return ClickHouseCreateConstant.createInt32Constant(value);
        case UInt64:
            return ClickHouseCreateConstant.createUInt64Constant(BigInteger.valueOf(value));
        case Int64:
            return ClickHouseCreateConstant.createInt64Constant(BigInteger.valueOf(value));
        case UInt128:
            return ClickHouseCreateConstant.createUInt128Constant(BigInteger.valueOf(value));
        case Int128:
            return ClickHouseCreateConstant.createInt128Constant(BigInteger.valueOf(value));
        case UInt256:
            return ClickHouseCreateConstant.createUInt256Constant(BigInteger.valueOf(value));
        case Int256:
            return ClickHouseCreateConstant.createInt256Constant(BigInteger.valueOf(value));
        case Float32:
            return ClickHouseCreateConstant.createFloat32Constant((float) value);
        case Float64:
            return ClickHouseCreateConstant.createFloat64Constant((double) value);
        case Nothing:
            return ClickHouseCreateConstant.createNullConstant();
        case Bool:
            return ClickHouseCreateConstant.createBooleanConstant(value != 0);
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
