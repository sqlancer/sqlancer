package sqlancer.doris.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Node;

public class DorisSelect extends SelectBase<Node<DorisExpression>> implements Node<DorisExpression> {

    public enum DorisSelectDistinctType {

        ALL, DISTINCT, DISTINCTROW, NULL;

        public static DorisSelectDistinctType getRandomWithoutNull() {
            DorisSelectDistinctType sft;
            do {
                sft = Randomly.fromOptions(values());
            } while (sft == DorisSelectDistinctType.NULL);
            return sft;
        }
    }

    private DorisSelectDistinctType selectDistinctType = DorisSelectDistinctType.ALL;

    public void setDistinct(boolean isDistinct) {
        if (isDistinct) {
            this.selectDistinctType = DorisSelectDistinctType.DISTINCT;
        } else {
            this.selectDistinctType = DorisSelectDistinctType.ALL;
        }
    }

    public void setDistinct(DorisSelectDistinctType type) {
        this.selectDistinctType = type;
    }

    public boolean isDistinct() {
        return this.selectDistinctType == DorisSelectDistinctType.DISTINCT
                || this.selectDistinctType == DorisSelectDistinctType.DISTINCTROW;
    }

}
