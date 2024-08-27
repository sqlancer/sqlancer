package sqlancer.yugabyte.ycql.ast;

import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.questdb.ast.QuestDBExpression;

public class QuestDBBinaryOperation extends NewBinaryOperatorNode<QuestDBExpression> implements QuestDBExpression {
    public QuestDBBinaryOperation(QuestDBExpression left, QuestDBExpression right, Operator op) {
        super(left, right, op);
    }
}
