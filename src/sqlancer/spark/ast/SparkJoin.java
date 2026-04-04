package sqlancer.spark.ast;

import sqlancer.common.ast.newast.Join;
import sqlancer.spark.SparkSchema.SparkColumn;
import sqlancer.spark.SparkSchema.SparkTable;

public class SparkJoin implements SparkExpression, Join<SparkExpression, SparkTable, SparkColumn> {

    private final SparkTableReference leftTable;
    private final SparkTableReference rightTable;
    private final JoinType joinType;
    private SparkExpression onClause;

    public enum JoinType {
        INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER, LEFT_SEMI, LEFT_ANTI, CROSS;
    }

    public SparkJoin(SparkTableReference leftTable, SparkTableReference rightTable, JoinType joinType,
            SparkExpression onClause) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.onClause = onClause;
    }

    public SparkTableReference getLeftTable() {
        return leftTable;
    }

    public SparkTableReference getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public SparkExpression getOnClause() {
        return onClause;
    }

    @Override
    public void setOnClause(SparkExpression onClause) {
        this.onClause = onClause;
    }
}
