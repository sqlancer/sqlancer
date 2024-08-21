package sqlancer.duckdb.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewCaseOperatorNode;

public class DuckDBCaseOperator extends NewCaseOperatorNode<DuckDBExpression> implements DuckDBExpression {
    public DuckDBCaseOperator(DuckDBExpression switchCondition, List<DuckDBExpression> conditions,
            List<DuckDBExpression> expressions, DuckDBExpression elseExpr) {
        super(switchCondition, conditions, expressions, elseExpr);
    }
}
