package sqlancer.clickhouse.ast.constant;

import java.math.BigInteger;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.clickhouse.ast.ClickHouseNumericConstant;

public class ClickHouseUInt256Constant extends ClickHouseNumericConstant<BigInteger> {

    public ClickHouseUInt256Constant(BigInteger value) {
        super(value);
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
    public long asInt() {
        return value.longValueExact();
    }

}
