package sqlancer.clickhouse.ast.constant;

import java.math.BigInteger;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.clickhouse.ast.ClickHouseConstant;
import sqlancer.clickhouse.ast.ClickHouseNumericConstant;

public class ClickHouseFloat32Constant extends ClickHouseNumericConstant<Float> {

    public ClickHouseFloat32Constant(float value) {
        super(value);
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        if (value == Double.POSITIVE_INFINITY) {
            return "'+Inf'";
        } else if (value == Double.NEGATIVE_INFINITY) {
            return "'-Inf'";
        }
        return String.valueOf(value);
    }

    @Override
    public boolean compareInternal(Object val) {
        return Float.compare(value, (float) val) == 0;
    }

    @Override
    public ClickHouseConstant applyLess(ClickHouseConstant right) {
        if (this.getDataType() == right.getDataType()) {
            return this.asDouble() < right.asDouble() ? ClickHouseCreateConstant.createTrue()
                    : ClickHouseCreateConstant.createFalse();
        }
        ClickHouseConstant converted = right.cast(ClickHouseDataType.Float32);
        return this.asDouble() < converted.asDouble() ? ClickHouseCreateConstant.createTrue()
                : ClickHouseCreateConstant.createFalse();
    }

    @Override
    public boolean asBooleanNotNull() {
        return Float.compare(value, (float) 0) == 0;
    }

    @Override
    public ClickHouseDataType getDataType() {
        return ClickHouseDataType.Float32;
    }

    @Override
    public double asDouble() {
        return value;
    }
}
