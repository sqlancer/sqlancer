package sqlancer.duckdb.ast;

import sqlancer.common.ast.newast.NewTernaryNode;

public class DuckDBTernary extends NewTernaryNode<DuckDBExpression> implements DuckDBExpression {
    public DuckDBTernary(DuckDBExpression left, DuckDBExpression middle, DuckDBExpression right, String leftString,
            String rightString) {
        super(left, middle, right, leftString, rightString);
    }
}
