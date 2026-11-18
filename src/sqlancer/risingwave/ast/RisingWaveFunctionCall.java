package sqlancer.risingwave.ast;

import java.util.List;

import sqlancer.risingwave.RisingWaveType;

public final class RisingWaveFunctionCall implements RisingWaveExpression {
    private final String name;
    public final RisingWaveAggregateFunction aggregate;
    public final List<RisingWaveExpression> arguments;
    public final boolean distinct;
    public final RisingWaveExpression filter;
    public final List<RisingWaveAggregateOrder> orderBy;
    private final RisingWaveType type;

    public RisingWaveFunctionCall(String name, RisingWaveType type, List<RisingWaveExpression> arguments) {
        if (arguments.isEmpty()) {
            throw new IllegalArgumentException("Function requires arguments");
        }
        this.name = name;
        this.aggregate = null;
        this.type = type;
        this.arguments = List.copyOf(arguments);
        this.distinct = false;
        this.filter = null;
        this.orderBy = List.of();
    }

    public RisingWaveFunctionCall(RisingWaveAggregateFunction aggregate, List<RisingWaveExpression> arguments) {
        this(aggregate, arguments, false, null, List.of());
    }

    public RisingWaveFunctionCall(RisingWaveAggregateFunction aggregate, List<RisingWaveExpression> arguments,
            boolean distinct, RisingWaveExpression filter, List<RisingWaveAggregateOrder> orderBy) {
        aggregate.validate(arguments, distinct, filter, orderBy);
        this.name = aggregate.getName();
        this.aggregate = aggregate;
        this.type = aggregate.resultType(arguments);
        this.arguments = List.copyOf(arguments);
        this.distinct = distinct;
        this.filter = filter;
        this.orderBy = List.copyOf(orderBy);
    }

    public String getName() {
        return name;
    }

    @Override
    public RisingWaveType getType() {
        return type;
    }
}
