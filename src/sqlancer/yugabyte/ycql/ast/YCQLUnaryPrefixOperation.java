package sqlancer.yugabyte.ycql.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.questdb.ast.QuestDBExpression;

public class QuestDBUnaryPrefixOperation extends NewUnaryPrefixOperatorNode<QuestDBExpression>
        implements QuestDBExpression {
    public QuestDBUnaryPrefixOperation(QuestDBExpression expr, BinaryOperatorNode.Operator operator) {
        super(expr, operator);
    }
}
