package sqlancer.yugabyte.ycql.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.questdb.ast.QuestDBExpression;

public class QuestDBUnaryPostfixOperation extends NewUnaryPostfixOperatorNode<QuestDBExpression>
        implements QuestDBExpression {
    public QuestDBUnaryPostfixOperation(QuestDBExpression expr, BinaryOperatorNode.Operator op) {
        super(expr, op);
    }
}
