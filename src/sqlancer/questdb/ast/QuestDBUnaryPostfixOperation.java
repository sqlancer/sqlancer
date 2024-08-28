package sqlancer.questdb.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;

public class QuestDBUnaryPostfixOperation extends NewUnaryPostfixOperatorNode<QuestDBExpression>
        implements QuestDBExpression {
    public QuestDBUnaryPostfixOperation(QuestDBExpression expr, BinaryOperatorNode.Operator op) {
        super(expr, op);
    }
}
