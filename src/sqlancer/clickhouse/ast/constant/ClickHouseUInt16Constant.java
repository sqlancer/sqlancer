package sqlancer.clickhouse.ast.constant;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.IgnoreMeException;
import sqlancer.clickhouse.ast.ClickHouseConstant;
import sqlancer.clickhouse.ast.ClickHouseNumericConstant;

public class ClickHouseUInt16Constant extends ClickHouseNumericConstant<Long> {

    public ClickHouseUInt16Constant(long value) {
        super(value);
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public boolean asBooleanNotNull() {
        return value != 0;
    }

    @Override
    public ClickHouseDataType getDataType() {
        return ClickHouseDataType.UInt16;
    }

    @Override
    public boolean compareInternal(Object val) {
        return value == (long) val;
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
