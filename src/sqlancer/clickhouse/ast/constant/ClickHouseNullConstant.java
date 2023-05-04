package sqlancer.clickhouse.ast.constant;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.clickhouse.ast.ClickHouseConstant;

public class ClickHouseNullConstant extends ClickHouseConstant {

    @Override
    public String toString() {
        return "NULL";
    }

    @Override
    public boolean isNull() {
        return true;
    }

    @Override
    public boolean asBooleanNotNull() {
        throw new AssertionError();
    }

    @Override
    public ClickHouseDataType getDataType() {
        return ClickHouseDataType.Nothing;
    }

    @Override
    public boolean compareInternal(Object value) {
        return false;
    }

    @Override
    public ClickHouseConstant applyEquals(ClickHouseConstant right) {
        return ClickHouseCreateConstant.createNullConstant();
    }

    @Override
    public ClickHouseConstant applyLess(ClickHouseConstant right) {
        return ClickHouseCreateConstant.createNullConstant();
    }

    @Override
    public Object getValue() {
        return null;
    }

    @Override
    public ClickHouseConstant cast(ClickHouseDataType type) {
        return null;
    }
}
