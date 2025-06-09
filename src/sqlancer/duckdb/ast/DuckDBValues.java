package sqlancer.duckdb.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewValuesNode;

public class DuckDBValues extends NewValuesNode<DuckDBExpression> implements DuckDBExpression {
    public DuckDBValues(List<DuckDBExpression> exprs) {
        super(exprs);
    }
}
