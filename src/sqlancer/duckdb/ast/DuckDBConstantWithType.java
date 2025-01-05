package sqlancer.duckdb.ast;

import sqlancer.duckdb.DuckDBSchema.DuckDBCompositeDataType;
import sqlancer.duckdb.ast.DuckDBConstant.DuckDBNullConstant;

public class DuckDBConstantWithType implements DuckDBExpression {
    private DuckDBConstant constant;
    private DuckDBCompositeDataType type;

    public DuckDBConstantWithType(DuckDBConstant constant, DuckDBCompositeDataType type) {
        this.constant = constant;
        this.type = type;
    }

    public String toString() {
        if (constant instanceof DuckDBNullConstant) {
            return constant.toString();
        }
        else {
            return "(" + constant.toString() + ")::" + type.toString();
        }
    }

    public DuckDBConstant getConstant() {
        return constant;
    }

    public DuckDBCompositeDataType getType() {
        return type;
    }
}
