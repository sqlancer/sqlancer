package sqlancer.duckdb.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewInOperatorNode;

public class DuckDBInOperator extends NewInOperatorNode<DuckDBExpression> implements DuckDBExpression {
    public DuckDBInOperator(DuckDBExpression left, List<DuckDBExpression> right, boolean isNegated) {
        super(left, right, isNegated);
    }
}
