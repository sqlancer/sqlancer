package sqlancer.clickhouse.ast.constant;

import java.math.BigInteger;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.IgnoreMeException;
import sqlancer.clickhouse.ast.ClickHouseConstant;
import sqlancer.clickhouse.ast.ClickHouseNumericConstant;

public class ClickHouseInt64Constant extends ClickHouseNumericConstant<BigInteger> {

    public ClickHouseInt64Constant(BigInteger value) {
        super(value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    @Override
    public boolean asBooleanNotNull() {
        return value != BigInteger.ZERO;
    }

    @Override
    public ClickHouseDataType getDataType() {
        return ClickHouseDataType.Int64;
    }

    @Override
    public boolean compareInternal(Object val) {
        return value.compareTo((BigInteger) val) == 0;
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
        return value.longValueExact();
    }

    @Override
    public Object getValue() {
        return value;
    }
}
