package sqlancer.databend.ast;

import sqlancer.common.ast.SelectBase;

public class DatabendSelect extends SelectBase<DatabendExpression> implements DatabendExpression {

    private boolean isDistinct;

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

}
