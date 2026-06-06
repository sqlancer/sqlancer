package sqlancer.hive.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewCaseOperatorNode;

public class HiveCaseOperation extends NewCaseOperatorNode<HiveExpression> implements HiveExpression {

    public HiveCaseOperation(HiveExpression switchCondition, List<HiveExpression> conditions,
            List<HiveExpression> expressions, HiveExpression elseExpr) {
        super(switchCondition, conditions, expressions, elseExpr);
    }
}
