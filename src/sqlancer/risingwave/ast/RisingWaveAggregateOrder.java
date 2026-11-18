package sqlancer.risingwave.ast;

public final class RisingWaveAggregateOrder {
    public enum Direction {
        ASC, DESC
    }

    public enum NullOrder {
        FIRST, LAST
    }

    public final RisingWaveExpression expression;
    public final Direction direction;
    public final NullOrder nullOrder;

    public RisingWaveAggregateOrder(RisingWaveExpression expression, Direction direction, NullOrder nullOrder) {
        this.expression = expression;
        this.direction = direction;
        this.nullOrder = nullOrder;
    }
}
