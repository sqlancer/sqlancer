package sqlancer.clickhouse.ast;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.clickhouse.ast.constant.ClickHouseCreateConstant;

import java.math.BigInteger;

public abstract class ClickHouseNumericConstant<T extends Number> extends ClickHouseConstant {
    private final T value;

    public ClickHouseNumericConstant(T value) {
        this.value = value;
    }
    @Override
    public ClickHouseConstant cast(ClickHouseDataType type) {
        if (type == ClickHouseDataType.Nothing) {
            return ClickHouseCreateConstant.createNullConstant();
        }

        if (type == ClickHouseDataType.Bool) {
            return ClickHouseCreateConstant.createBooleanConstant(value.longValue() != 0);
        }
        if (value instanceof BigInteger) {
            return castBigInteger(type);
        }
        return castNumeric(type);
    }

    private ClickHouseConstant castNumeric(ClickHouseDataType type) {
        switch(type) {
            case String:
                return ClickHouseCreateConstant.createStringConstant(this.toString());
            case UInt8:
                return ClickHouseCreateConstant.createUInt8Constant(value.byteValue());
            case Int8:
                return ClickHouseCreateConstant.createInt8Constant(value.byteValue());
            case UInt16:
                return ClickHouseCreateConstant.createUInt16Constant(value.shortValue());
            case Int16:
                return ClickHouseCreateConstant.createInt16Constant(value.shortValue());
            case UInt32:
                return ClickHouseCreateConstant.createUInt32Constant(value.intValue());
            case Int32:
                return ClickHouseCreateConstant.createInt32Constant(value.intValue());
            case UInt64:
                return ClickHouseCreateConstant.createUInt64Constant(BigInteger.valueOf(value.longValue()));
            case Int64:
                return ClickHouseCreateConstant.createInt64Constant(BigInteger.valueOf(value.longValue()));
            case Float32:
                return ClickHouseCreateConstant.createFloat32Constant(value.floatValue());
            case Float64:
                return ClickHouseCreateConstant.createFloat64Constant(value.doubleValue());
        }
    }

    private ClickHouseConstant castBigInteger(ClickHouseDataType type) {
        BigInteger bigInt = (BigInteger) this.value;
        switch(type) {
            case String:
                return ClickHouseCreateConstant.createStringConstant(bigInt.toString());
            case UInt8:
                return ClickHouseCreateConstant.createUInt8Constant(bigInt.byteValueExact());
            case Int8:
                return ClickHouseCreateConstant.createInt8Constant(bigInt.byteValueExact());
            case UInt16:
                return ClickHouseCreateConstant.createUInt16Constant(bigInt.shortValueExact());
            case Int16:
                return ClickHouseCreateConstant.createInt16Constant(bigInt.shortValueExact());
            case UInt32:
                return ClickHouseCreateConstant.createUInt32Constant(bigInt.intValueExact());
            case Int32:
                return ClickHouseCreateConstant.createInt32Constant(bigInt.intValueExact());
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
        }
    }
}
