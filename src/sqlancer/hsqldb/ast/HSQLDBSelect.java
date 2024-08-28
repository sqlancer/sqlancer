package sqlancer.hsqldb.ast;

import sqlancer.common.ast.SelectBase;

public class HSQLDBSelect extends SelectBase<HSQLDBExpression> implements HSQLDBExpression {

    private boolean isDistinct;

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

}
