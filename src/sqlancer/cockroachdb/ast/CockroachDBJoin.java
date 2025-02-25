package sqlancer.cockroachdb.ast;

import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.newast.Join;

public class CockroachDBJoin extends JoinBase<CockroachDBExpression>
        implements CockroachDBExpression, Join<CockroachDBExpression, CockroachDBTable, CockroachDBColumn> {

    private final CockroachDBExpression leftTable;
    private final CockroachDBExpression rightTable;

    public CockroachDBJoin(CockroachDBExpression leftTable, CockroachDBExpression rightTable,
            CockroachDBExpression whereCondition, JoinType joinType) {
        super(joinType, whereCondition);
        this.leftTable = leftTable;
        this.rightTable = rightTable;
    }

    public CockroachDBExpression getLeftTable() {
        return leftTable;
    }

    public CockroachDBExpression getRightTable() {
        return rightTable;
    }

    public void setJoinType(JoinType joinType) {
        type = joinType;
    }

    public JoinType getJoinType() {
        return type;
    }

    @Override
    public void setOnClause(CockroachDBExpression onClause) {
        this.onClause = onClause;
    }

    @Override
    public CockroachDBExpression getOnClause() {
        return onClause;
    }

    public static CockroachDBJoin createJoin(CockroachDBExpression left, CockroachDBExpression right, JoinType type,
            CockroachDBExpression onClause) {
        if (type.compareTo(JoinType.CROSS) >= 0) {
            return new CockroachDBJoin(left, right, null, type);
        } else {
            return new CockroachDBJoin(left, right, onClause, type);
        }
    }
}
