package sqlancer.clickhouse.ast.constant;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.clickhouse.ast.ClickHouseNumericConstant;

public class ClickHouseUInt32Constant extends ClickHouseNumericConstant<Long> {

    public ClickHouseUInt32Constant(long value) {
        super(value);
    }

    @Override
    public ClickHouseDataType getDataType() {
        return ClickHouseDataType.UInt32;
    }

    @Override
    public boolean compareInternal(Object val) {
        return value == (long) val;
    }

    @Override
    public long asInt() {
        return value;
    }

}
