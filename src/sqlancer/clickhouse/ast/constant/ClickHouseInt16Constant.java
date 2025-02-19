package sqlancer.clickhouse.ast.constant;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.clickhouse.ast.ClickHouseNumericConstant;

public class ClickHouseInt16Constant extends ClickHouseNumericConstant<Long> {

    public ClickHouseInt16Constant(long value) {
        super(value);
    }

    @Override
    public boolean asBooleanNotNull() {
        return value != 0;
    }

    @Override
    public ClickHouseDataType getDataType() {
        return ClickHouseDataType.Int16;
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
