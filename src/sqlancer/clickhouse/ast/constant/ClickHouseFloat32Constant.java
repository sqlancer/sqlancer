package sqlancer.clickhouse.ast.constant;

import java.math.BigInteger;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.clickhouse.ast.ClickHouseConstant;

public class ClickHouseFloat32Constant extends ClickHouseConstant {

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
            return this.asDouble() < right.asDouble() ? ClickHouseCreateConstant.createTrue()
                    : ClickHouseCreateConstant.createFalse();
        }
        ClickHouseConstant converted = right.cast(ClickHouseDataType.Float32);
        return this.asDouble() < converted.asDouble() ? ClickHouseCreateConstant.createTrue()
                : ClickHouseCreateConstant.createFalse();
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
            return ClickHouseCreateConstant.createStringConstant(this.toString());
        case UInt8:
            return ClickHouseCreateConstant.createUInt8Constant((long) value);
        case Int8:
            return ClickHouseCreateConstant.createInt8Constant((long) value);
        case UInt16:
            return ClickHouseCreateConstant.createUInt16Constant((long) value);
        case Int16:
            return ClickHouseCreateConstant.createInt16Constant((long) value);
        case UInt32:
            return ClickHouseCreateConstant.createUInt32Constant((long) value);
        case Int32:
            return ClickHouseCreateConstant.createInt32Constant((long) value);
        case UInt64:
            return ClickHouseCreateConstant.createUInt64Constant(BigInteger.valueOf((long) value));
        case Int64:
            return ClickHouseCreateConstant.createInt64Constant(BigInteger.valueOf((long) value));
        case Float32:
            return ClickHouseCreateConstant.createFloat32Constant(value);
        case Float64:
            return ClickHouseCreateConstant.createFloat64Constant(value);
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
