package sqlancer.clickhouse.ast;

import sqlancer.common.visitor.BinaryOperation;

public class ClickHouseJoinOnClause implements ClickHouseExpression, BinaryOperation<ClickHouseExpression> {
    private final ClickHouseExpression left;
    private final ClickHouseExpression right;

    public ClickHouseJoinOnClause(ClickHouseExpression left, ClickHouseExpression right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public final ClickHouseExpression getLeft() {
        return this.left;
    }

    @Override
    public final ClickHouseExpression getRight() {
        return this.right;
    }

    @Override
    public String getOperatorRepresentation() {
        return "=";
    }
}
