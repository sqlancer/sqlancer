package sqlancer.presto.ast;

import sqlancer.common.ast.newast.NewTernaryNode;

public class PrestoTernary extends NewTernaryNode<PrestoExpression> implements PrestoExpression {
    public PrestoTernary(PrestoExpression left, PrestoExpression middle, PrestoExpression right, String leftStr,
            String rightStr) {
        super(left, middle, right, leftStr, rightStr);
    }
}
