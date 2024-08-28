package sqlancer.questdb.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;

public class QuestDBUnaryPrefixOperation extends NewUnaryPrefixOperatorNode<QuestDBExpression>
        implements QuestDBExpression {
    public QuestDBUnaryPrefixOperation(QuestDBExpression expr, BinaryOperatorNode.Operator operator) {
        super(expr, operator);
    }
}
