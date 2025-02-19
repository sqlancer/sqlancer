package sqlancer.clickhouse.ast.constant;

import java.math.BigInteger;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.clickhouse.ast.ClickHouseNumericConstant;

public class ClickHouseUInt64Constant extends ClickHouseNumericConstant<BigInteger> {

    public ClickHouseUInt64Constant(BigInteger value) {
        super(value);
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
    public long asInt() {
        return value.longValueExact();
    }

}
