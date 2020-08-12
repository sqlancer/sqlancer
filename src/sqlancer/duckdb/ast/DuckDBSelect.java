package sqlancer.duckdb.ast;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Node;

public class DuckDBSelect extends SelectBase<Node<DuckDBExpression>> implements Node<DuckDBExpression> {

    private boolean isDistinct;

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

}
