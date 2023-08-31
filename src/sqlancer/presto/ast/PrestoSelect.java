package sqlancer.presto.ast;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Node;

public class PrestoSelect extends SelectBase<Node<PrestoExpression>> implements Node<PrestoExpression> {

    private boolean isDistinct;

    public boolean isDistinct() {
        return isDistinct;
    }

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

}
