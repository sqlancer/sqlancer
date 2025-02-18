package sqlancer.clickhouse.ast.constant;

import java.math.BigInteger;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.clickhouse.ast.ClickHouseNumericConstant;

public class ClickHouseUInt128Constant extends ClickHouseNumericConstant<BigInteger> {

    public ClickHouseUInt128Constant(BigInteger value) {
        super(value);
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
    public long asInt() {
        return value.longValueExact();
    }

    @Override
    public Object getValue() {
        return value;
    }
}
