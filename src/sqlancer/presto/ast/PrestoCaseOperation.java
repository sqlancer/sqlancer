package sqlancer.presto.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewCaseOperatorNode;

public class PrestoCaseOperation extends NewCaseOperatorNode<PrestoExpression> implements PrestoExpression {
    public PrestoCaseOperation(PrestoExpression switchCondition, List<PrestoExpression> conditions,
            List<PrestoExpression> expressions, PrestoExpression elseExpr) {
        super(switchCondition, conditions, expressions, elseExpr);
    }
}
