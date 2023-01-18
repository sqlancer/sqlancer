package sqlancer.cnosdb.ast;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;

public class CnosDBJoin implements CnosDBExpression {

    public enum CnosDBJoinType {
        INNER, LEFT, RIGHT, FULL;
        // now not support
        // CROSS;

        public static CnosDBJoinType getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    private final CnosDBExpression tableReference;
    private final CnosDBExpression onClause;
    private final CnosDBJoinType type;

    public CnosDBJoin(CnosDBExpression tableReference, CnosDBExpression onClause, CnosDBJoinType type) {
        this.tableReference = tableReference;
        this.onClause = onClause;
        this.type = type;
    }

    public CnosDBExpression getTableReference() {
        return tableReference;
    }

    public CnosDBExpression getOnClause() {
        return onClause;
    }

    public CnosDBJoinType getType() {
        return type;
    }

    @Override
    public CnosDBDataType getExpressionType() {
        throw new AssertionError();
    }

    @Override
    public CnosDBConstant getExpectedValue() {
        throw new AssertionError();
    }

}
