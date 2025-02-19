package sqlancer.clickhouse.ast.constant;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.clickhouse.ast.ClickHouseNumericConstant;

public class ClickHouseInt32Constant extends ClickHouseNumericConstant<Long> {

    public ClickHouseInt32Constant(long value) {
        super(value);
    }

    @Override
    public boolean asBooleanNotNull() {
        return value != 0;
    }

    @Override
    public ClickHouseDataType getDataType() {
        return ClickHouseDataType.Int32;
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
