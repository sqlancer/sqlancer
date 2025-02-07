package sqlancer.clickhouse.ast.constant;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.IgnoreMeException;
import sqlancer.clickhouse.ast.ClickHouseConstant;
import sqlancer.clickhouse.ast.ClickHouseNumericConstant;

public class ClickHouseUInt32Constant extends ClickHouseNumericConstant<Long> {

    public ClickHouseUInt32Constant(long value) {
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
        return ClickHouseDataType.UInt32;
    }

    @Override
    public boolean compareInternal(Object val) {
        return value == (long) val;
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
        return value;
    }

    @Override
    public Object getValue() {
        return value;
    }
}
