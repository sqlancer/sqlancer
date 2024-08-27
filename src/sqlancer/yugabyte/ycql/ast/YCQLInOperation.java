package sqlancer.yugabyte.ycql.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewInOperatorNode;
import sqlancer.questdb.ast.QuestDBExpression;

public class QuestDBInOperation extends NewInOperatorNode<QuestDBExpression> implements QuestDBExpression {
    public QuestDBInOperation(QuestDBExpression left, List<QuestDBExpression> right, boolean isNegated) {
        super(left, right, isNegated);
    }
}
