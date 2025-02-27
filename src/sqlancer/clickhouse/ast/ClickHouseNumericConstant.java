package sqlancer.clickhouse.ast;

import java.math.BigInteger;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.IgnoreMeException;
import sqlancer.clickhouse.ast.constant.ClickHouseCreateConstant;

public abstract class ClickHouseNumericConstant<T extends Number> extends ClickHouseConstant {
    protected final T value;

    public ClickHouseNumericConstant(T value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public boolean asBooleanNotNull() {
        if (this.value instanceof BigInteger) {
            return asBooleanNotNullBigInteger();
        }
        return asBooleanNotNullNumeric();
    }

    private boolean asBooleanNotNullBigInteger() {
        return this.value != BigInteger.ZERO;
    }

    private boolean asBooleanNotNullNumeric() {
        return this.value.doubleValue() != 0;
    }

    @Override
    public ClickHouseConstant applyLess(ClickHouseConstant right) {
        if (this.value instanceof Float || this.value instanceof Double) {
            return applyLessFloat(right);
        }
        return applyLessInt(right);
    }

    private ClickHouseConstant applyLessFloat(ClickHouseConstant right) {
        if (this.getDataType() == right.getDataType()) {
            return this.asDouble() < right.asDouble() ? ClickHouseCreateConstant.createTrue()
                    : ClickHouseCreateConstant.createFalse();
        }
        ClickHouseConstant converted;
        if (this.value instanceof Float) {
            converted = right.cast(ClickHouseDataType.Float32);
        } else {
            converted = right.cast(ClickHouseDataType.Float64);
        }
        return this.asDouble() < converted.asDouble() ? ClickHouseCreateConstant.createTrue()
                : ClickHouseCreateConstant.createFalse();
    }

    private ClickHouseConstant applyLessInt(ClickHouseConstant right) {
        if (this.getDataType() == right.getDataType()) {
            return this.asInt() < right.asInt() ? ClickHouseCreateConstant.createTrue()
                    : ClickHouseCreateConstant.createFalse();
        }
        throw new IgnoreMeException();
    }

    @Override
    public ClickHouseConstant cast(ClickHouseDataType type) {
        if (type == ClickHouseDataType.Nothing) {
            return ClickHouseCreateConstant.createNullConstant();
        }

        if (this.value instanceof BigInteger) {
            return castBigInteger(type);
        }
        return castNumeric(type);
    }

    private ClickHouseConstant castNumeric(ClickHouseDataType type) {
        switch (type) {
        case String:
            return ClickHouseCreateConstant.createStringConstant(this.toString());
        case UInt8:
            return ClickHouseCreateConstant.createUInt8Constant(value.byteValue());
        case Int8:
            return ClickHouseCreateConstant.createInt8Constant(value.byteValue());
        case UInt16:
            return ClickHouseCreateConstant.createUInt16Constant(value.longValue());
        case Int16:
            return ClickHouseCreateConstant.createInt16Constant(value.longValue());
        case UInt32:
            return ClickHouseCreateConstant.createUInt32Constant(value.longValue());
        case Int32:
            return ClickHouseCreateConstant.createInt32Constant(value.longValue());
        case UInt64:
            return ClickHouseCreateConstant.createUInt64Constant(BigInteger.valueOf(value.longValue()));
        case Int64:
            return ClickHouseCreateConstant.createInt64Constant(BigInteger.valueOf(value.longValue()));
        case Float32:
            return ClickHouseCreateConstant.createFloat32Constant(value.floatValue());
        case Float64:
            return ClickHouseCreateConstant.createFloat64Constant(value.doubleValue());
        case Bool:
            return ClickHouseCreateConstant.createBooleanConstant(value.longValue() != 0);
        default:
            throw new AssertionError(type);
        }
    }

    private ClickHouseConstant castBigInteger(ClickHouseDataType type) {
        BigInteger bigInt = (BigInteger) this.value;
        long val = bigInt.longValueExact();
        switch (type) {
        case String:
            return ClickHouseCreateConstant.createStringConstant(this.toString());
        case UInt8:
            return ClickHouseCreateConstant.createUInt8Constant(val);
        case Int8:
            return ClickHouseCreateConstant.createInt8Constant(val);
        case UInt16:
            return ClickHouseCreateConstant.createUInt16Constant(val);
        case Int16:
            return ClickHouseCreateConstant.createInt16Constant(val);
        case UInt32:
            return ClickHouseCreateConstant.createUInt32Constant(val);
        case Int32:
            return ClickHouseCreateConstant.createInt32Constant(val);
        case UInt64:
            return ClickHouseCreateConstant.createUInt64Constant(bigInt);
        case Int64:
            return ClickHouseCreateConstant.createInt64Constant(bigInt);
        case UInt128:
            return ClickHouseCreateConstant.createUInt128Constant(bigInt);
        case Int128:
            return ClickHouseCreateConstant.createInt128Constant(bigInt);
        case UInt256:
            return ClickHouseCreateConstant.createUInt256Constant(bigInt);
        case Int256:
            return ClickHouseCreateConstant.createInt256Constant(bigInt);
        case Float32:
            return ClickHouseCreateConstant.createFloat32Constant(bigInt.floatValue());
        case Float64:
            return ClickHouseCreateConstant.createFloat64Constant(bigInt.doubleValue());
        case Nothing:
            return ClickHouseCreateConstant.createNullConstant();
        case Bool:
            return ClickHouseCreateConstant.createBooleanConstant(val != 0);
        default:
            throw new AssertionError(type);
        }
    }
}
