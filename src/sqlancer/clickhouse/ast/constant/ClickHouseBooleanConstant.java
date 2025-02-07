package sqlancer.clickhouse.ast.constant;

import java.math.BigInteger;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.IgnoreMeException;
import sqlancer.clickhouse.ast.ClickHouseConstant;

public class ClickHouseBooleanConstant extends ClickHouseConstant {

    private final boolean value;

    public ClickHouseBooleanConstant(boolean value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value ? "true" : "false";
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public boolean asBooleanNotNull() {
        return value;
    }

    @Override
    public ClickHouseDataType getDataType() {
        return ClickHouseDataType.Bool;
    }

    @Override
    public boolean compareInternal(Object val) {
        if (val instanceof Boolean) {
            return value == ((Boolean) val).booleanValue();
        } else {
            return value == ((int) val != 0);
        }
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
        return value ? 1 : 0;
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
            return ClickHouseCreateConstant.createUInt8Constant(asInt());
        case Int8:
            return ClickHouseCreateConstant.createInt8Constant(asInt());
        case UInt16:
            return ClickHouseCreateConstant.createUInt16Constant(asInt());
        case Int16:
            return ClickHouseCreateConstant.createInt16Constant(asInt());
        case UInt32:
            return ClickHouseCreateConstant.createUInt32Constant(asInt());
        case Int32:
            return ClickHouseCreateConstant.createInt32Constant(asInt());
        case UInt64:
            return ClickHouseCreateConstant.createUInt64Constant(BigInteger.valueOf(asInt()));
        case Int64:
            return ClickHouseCreateConstant.createInt64Constant(BigInteger.valueOf(asInt()));
        case UInt128:
            return ClickHouseCreateConstant.createUInt128Constant(BigInteger.valueOf(asInt()));
        case Int128:
            return ClickHouseCreateConstant.createInt128Constant(BigInteger.valueOf(asInt()));
        case UInt256:
            return ClickHouseCreateConstant.createUInt256Constant(BigInteger.valueOf(asInt()));
        case Int256:
            return ClickHouseCreateConstant.createInt256Constant(BigInteger.valueOf(asInt()));
        case Float32:
            return ClickHouseCreateConstant.createFloat32Constant((float) asInt());
        case Float64:
            return ClickHouseCreateConstant.createFloat64Constant((double) asInt());
        case Nothing:
            return ClickHouseCreateConstant.createNullConstant();
        case Bool:
            return ClickHouseCreateConstant.createBooleanConstant(asInt() != 0);
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
