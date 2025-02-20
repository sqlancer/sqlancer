package sqlancer.clickhouse.ast;

import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.newast.Join;

public class ClickHouseJoin extends JoinBase<ClickHouseExpression> implements ClickHouseExpression,
        Join<ClickHouseExpression, ClickHouseSchema.ClickHouseTable, ClickHouseSchema.ClickHouseColumn> {

    public ClickHouseJoin(ClickHouseTableReference leftTable, ClickHouseTableReference rightTable,
            ClickHouseJoin.JoinType type, ClickHouseJoinOnClause onClause) {
        super(leftTable, rightTable, onClause, type);

    }

    public ClickHouseJoin(ClickHouseTableReference leftTable, ClickHouseTableReference rightTable, JoinType type) {
        super(leftTable, rightTable, type);
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        if (type != JoinType.CROSS) {
            throw new AssertionError();
        }
        this.onClause = null;
        this.type = type;
    }

    public ClickHouseTableReference getLeftTable() {
        return (ClickHouseTableReference) leftTable;
    }

    public ClickHouseTableReference getRightTable() {
        return (ClickHouseTableReference) rightTable;
    }

    @Override
    public ClickHouseExpression getOnClause() {
        return onClause;
    }

    @Override
    public ClickHouseJoin.JoinType getType() {
        return type;
    }

    @Override
    public void setOnClause(ClickHouseExpression onClause) {
        this.onClause = onClause;
    }

}
