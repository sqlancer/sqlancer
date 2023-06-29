package sqlancer.stonedb.ast;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Node;

public class StoneDBSelect extends SelectBase<Node<StoneDBExpression>> implements Node<StoneDBExpression> {

    private boolean isDistinct;

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    public boolean isDistinct() {
        return isDistinct;
    }
}
