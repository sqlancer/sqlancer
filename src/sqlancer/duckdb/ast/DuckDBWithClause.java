package sqlancer.duckdb.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewWithNode;

public class DuckDBWithClause extends NewWithNode<DuckDBExpression> implements DuckDBExpression {
    public DuckDBWithClause(DuckDBExpression left, List<DuckDBExpression> right) {
        super(left, right);
    }
}
