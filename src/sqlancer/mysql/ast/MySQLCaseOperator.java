package sqlancer.mysql.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewCaseOperatorNode;

public class MySQLCaseOperator extends NewCaseOperatorNode<MySQLExpression> implements MySQLExpression {

    public MySQLCaseOperator(MySQLExpression switchCondition, List<MySQLExpression> whenExprs,
            List<MySQLExpression> thenExprs, MySQLExpression elseExpr) {
        super(switchCondition, whenExprs, thenExprs, elseExpr);
    }

    @Override
    public MySQLConstant getExpectedValue() {
        int nrConditions = getConditions().size();

        MySQLExpression switchCondition = getSwitchCondition();
        List<MySQLExpression> whenExprs = getConditions();
        List<MySQLExpression> thenExprs = getExpressions();
        MySQLExpression elseExpr = getElseExpr();

        if (switchCondition != null) {
            MySQLConstant switchValue = switchCondition.getExpectedValue();

            for (int i = 0; i < nrConditions; i++) {
                MySQLConstant whenValue = whenExprs.get(i).getExpectedValue();
                MySQLConstant isConditionMatched = switchValue.isEquals(whenValue);
                if (!isConditionMatched.isNull() && isConditionMatched.asBooleanNotNull()) {
                    return thenExprs.get(i).getExpectedValue();
                }
            }
        } else {
            for (int i = 0; i < nrConditions; i++) {
                MySQLConstant whenValue = whenExprs.get(i).getExpectedValue();
                if (!whenValue.isNull() && whenValue.asBooleanNotNull()) {
                    return thenExprs.get(i).getExpectedValue();
                }
            }
        }

        if (elseExpr != null) {
            return elseExpr.getExpectedValue();
        }

        return null;
    }
}
