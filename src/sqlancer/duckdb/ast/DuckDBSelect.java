package sqlancer.duckdb.ast;

import sqlancer.common.ast.SelectBase;

public class DuckDBSelect extends SelectBase<DuckDBExpression> implements DuckDBExpression {

    private boolean isDistinct;

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

}
