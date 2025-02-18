package sqlancer.clickhouse.ast.constant;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.clickhouse.ast.ClickHouseNumericConstant;

public class ClickHouseInt8Constant extends ClickHouseNumericConstant<Integer> {

    public ClickHouseInt8Constant(int value) {
        super(value);
    }

    @Override
    public boolean asBooleanNotNull() {
        return value != 0;
    }

    @Override
    public ClickHouseDataType getDataType() {
        return ClickHouseDataType.Int8;
    }

    @Override
    public boolean compareInternal(Object val) {
        return value == (int) val;
    }

    @Override
    public long asInt() {
        return value;
    }

    @Override
    public Object getValue() {
        return value;
    }
}
