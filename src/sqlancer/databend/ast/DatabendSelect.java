package sqlancer.databend.ast;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Node;

public class DatabendSelect extends SelectBase<Node<DatabendExpression>> implements Node<DatabendExpression> {

    private boolean isDistinct;

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

}
