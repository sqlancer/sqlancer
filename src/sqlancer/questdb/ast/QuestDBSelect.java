package sqlancer.questdb.ast;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Node;

public class QuestDBSelect extends SelectBase<Node<QuestDBExpression>> implements Node<QuestDBExpression> {
    private boolean isDistinct;

    public void setDistinct(boolean distinct) {
        isDistinct = distinct;
    }

    public boolean isDistinct() {
        return isDistinct;
    }
}
