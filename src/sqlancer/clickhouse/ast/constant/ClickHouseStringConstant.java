package sqlancer.clickhouse.ast.constant;

import java.math.BigInteger;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.IgnoreMeException;
import sqlancer.clickhouse.ast.ClickHouseConstant;

public class ClickHouseStringConstant extends ClickHouseConstant {

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
            return this.asString().compareTo(right.asString()) <= 0 ? ClickHouseCreateConstant.createTrue()
                    : ClickHouseCreateConstant.createFalse();
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
            return ClickHouseCreateConstant.createStringConstant(this.toString());
        case UInt8:
            return ClickHouseCreateConstant.createUInt8Constant(Integer.parseInt(value));
        case Int8:
            return ClickHouseCreateConstant.createInt8Constant(Integer.parseInt(value));
        case UInt16:
            return ClickHouseCreateConstant.createUInt16Constant(Integer.parseInt(value));
        case Int16:
            return ClickHouseCreateConstant.createInt16Constant(Integer.parseInt(value));
        case UInt32:
            return ClickHouseCreateConstant.createUInt32Constant(Integer.parseInt(value));
        case Int32:
            return ClickHouseCreateConstant.createInt32Constant(Integer.parseInt(value));
        case UInt64:
            return ClickHouseCreateConstant.createUInt64Constant(BigInteger.valueOf(Integer.parseInt(value)));
        case Int64:
            return ClickHouseCreateConstant.createInt64Constant(BigInteger.valueOf(Integer.parseInt(value)));
        case Float32:
            return ClickHouseCreateConstant.createFloat32Constant((float) Float.parseFloat(value));
        case Float64:
            return ClickHouseCreateConstant.createFloat64Constant((double) Double.parseDouble(value));
        case Nothing:
            return ClickHouseCreateConstant.createNullConstant();
        case Bool:
            return ClickHouseCreateConstant.createBooleanConstant(value == "true");
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
