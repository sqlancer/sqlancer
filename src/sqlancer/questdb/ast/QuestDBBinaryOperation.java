package sqlancer.questdb.ast;

import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;

public class QuestDBBinaryOperation extends NewBinaryOperatorNode<QuestDBExpression> implements QuestDBExpression {
    public QuestDBBinaryOperation(QuestDBExpression left, QuestDBExpression right, Operator op) {
        super(left, right, op);
    }
}
