package sqlancer.questdb.ast;

import sqlancer.common.ast.SelectBase;

public class QuestDBSelect extends SelectBase<QuestDBExpression> implements QuestDBExpression {
    private boolean isDistinct;

    public void setDistinct(boolean distinct) {
        isDistinct = distinct;
    }

    public boolean isDistinct() {
        return isDistinct;
    }
}
