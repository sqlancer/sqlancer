package sqlancer.clickhouse.ast.constant;

import java.math.BigInteger;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.clickhouse.ast.ClickHouseNumericConstant;

public class ClickHouseInt256Constant extends ClickHouseNumericConstant<BigInteger> {

    public ClickHouseInt256Constant(BigInteger value) {
        super(value);
    }

    @Override
    public boolean asBooleanNotNull() {
        return value != BigInteger.ZERO;
    }

    @Override
    public ClickHouseDataType getDataType() {
        return ClickHouseDataType.Int256;
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
