package sqlancer.hive.ast;

import sqlancer.common.ast.newast.Join;
import sqlancer.hive.HiveSchema.HiveColumn;
import sqlancer.hive.HiveSchema.HiveTable;

public class HiveJoin implements HiveExpression, Join<HiveExpression, HiveTable, HiveColumn> {

    private final HiveTableReference leftTable;
    private final HiveTableReference rightTable;
    private final JoinType joinType;
    private HiveExpression onClause;

    // TODO: test map-join optimization

    public enum JoinType {
        INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER, LEFT_SEMI, CROSS;
    }

    public HiveJoin(HiveTableReference leftTable, HiveTableReference rightTable, JoinType joinType,
            HiveExpression onClause) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.onClause = onClause;
    }

    public HiveTableReference getLeftTable() {
        return leftTable;
    }

    public HiveTableReference getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public HiveExpression getOnClause() {
        return onClause;
    }

    @Override
    public void setOnClause(HiveExpression onClause) {
        this.onClause = onClause;
    }
}
