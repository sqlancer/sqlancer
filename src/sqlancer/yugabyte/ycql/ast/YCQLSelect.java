package sqlancer.yugabyte.ycql.ast;

import sqlancer.common.ast.SelectBase;

public class YCQLSelect extends SelectBase<YCQLExpression> implements YCQLExpression {

    private boolean isDistinct;

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

}
