package sqlancer.duckdb.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;

public class DuckDBUnaryPostfixOperator extends NewUnaryPostfixOperatorNode<DuckDBExpression>
        implements DuckDBExpression {
    public DuckDBUnaryPostfixOperator(DuckDBExpression expr, BinaryOperatorNode.Operator op) {
        super(expr, op);
    }
}
