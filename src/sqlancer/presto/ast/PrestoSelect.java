package sqlancer.presto.ast;

import sqlancer.common.ast.SelectBase;

public class PrestoSelect extends SelectBase<PrestoExpression> implements PrestoExpression {

    private boolean isDistinct;

    public boolean isDistinct() {
        return isDistinct;
    }

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

}
