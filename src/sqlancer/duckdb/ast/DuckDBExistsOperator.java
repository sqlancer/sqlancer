package sqlancer.duckdb.ast;

import sqlancer.common.ast.newast.NewExistsNode;

public class DuckDBExistsOperator extends NewExistsNode<DuckDBExpression> implements DuckDBExpression {
    public DuckDBExistsOperator(DuckDBExpression expr, Boolean isNot) {
        super(expr, isNot);
    }
}
