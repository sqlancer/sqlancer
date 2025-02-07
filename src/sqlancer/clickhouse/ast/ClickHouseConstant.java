package sqlancer.clickhouse.ast;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.clickhouse.ast.constant.ClickHouseCreateConstant;

public abstract class ClickHouseConstant extends ClickHouseExpression {

    public abstract boolean isNull();

    public abstract ClickHouseConstant cast(ClickHouseDataType type);

    public abstract boolean asBooleanNotNull();

    public abstract ClickHouseDataType getDataType();

    public abstract boolean compareInternal(Object value);

    public ClickHouseConstant applyEquals(ClickHouseConstant right) {
        if (this.getDataType() == right.getDataType()) {
            return this.compareInternal(right.getValue()) ? ClickHouseCreateConstant.createTrue()
                    : ClickHouseCreateConstant.createFalse();
        } else {
            ClickHouseConstant converted = right.cast(this.getDataType());
            return this.applyEquals(converted);
        }
    }

    public abstract ClickHouseConstant applyLess(ClickHouseConstant right);

    public abstract Object getValue();

    public long asInt() {
        throw new UnsupportedOperationException(this.getDataType().toString());
    }

    public double asDouble() {
        throw new UnsupportedOperationException(this.getDataType().toString());
    }

    public String asString() {
        throw new UnsupportedOperationException(this.getDataType().toString());
    }
}
