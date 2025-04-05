package sqlancer.mysql.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewCaseOperatorNode;

public class MySQLCaseOperator extends NewCaseOperatorNode<MySQLExpression> implements MySQLExpression {

    public MySQLCaseOperator(MySQLExpression switchCondition, List<MySQLExpression> whenExprs,
            List<MySQLExpression> thenExprs, MySQLExpression elseExpr) {
        super(switchCondition, whenExprs, thenExprs, elseExpr);
    }
}
