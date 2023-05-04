package sqlancer.materialize.ast;

import sqlancer.Randomly;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;

public class MaterializeJoin implements MaterializeExpression {

    public enum MaterializeJoinType {
        INNER, LEFT, RIGHT, FULL, CROSS;

        public static MaterializeJoinType getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    private final MaterializeExpression tableReference;
    private final MaterializeExpression onClause;
    private final MaterializeJoinType type;

    public MaterializeJoin(MaterializeExpression tableReference, MaterializeExpression onClause,
            MaterializeJoinType type) {
        this.tableReference = tableReference;
        this.onClause = onClause;
        this.type = type;
    }

    public MaterializeExpression getTableReference() {
        return tableReference;
    }

    public MaterializeExpression getOnClause() {
        return onClause;
    }

    public MaterializeJoinType getType() {
        return type;
    }

    @Override
    public MaterializeDataType getExpressionType() {
        throw new AssertionError();
    }

    @Override
    public MaterializeConstant getExpectedValue() {
        throw new AssertionError();
    }

}
