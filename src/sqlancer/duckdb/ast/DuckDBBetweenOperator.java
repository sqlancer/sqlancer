package sqlancer.duckdb.ast;

import sqlancer.common.ast.newast.NewBetweenOperatorNode;

public class DuckDBBetweenOperator extends NewBetweenOperatorNode<DuckDBExpression> implements DuckDBExpression {
    public DuckDBBetweenOperator(DuckDBExpression left, DuckDBExpression middle, DuckDBExpression right,
            boolean isTrue) {
        super(left, middle, right, isTrue);
    }
}
