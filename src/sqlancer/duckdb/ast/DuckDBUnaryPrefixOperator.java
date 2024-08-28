package sqlancer.duckdb.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;

public class DuckDBUnaryPrefixOperator extends NewUnaryPrefixOperatorNode<DuckDBExpression>
        implements DuckDBExpression {
    public DuckDBUnaryPrefixOperator(DuckDBExpression expr, BinaryOperatorNode.Operator operator) {
        super(expr, operator);
    }
}
