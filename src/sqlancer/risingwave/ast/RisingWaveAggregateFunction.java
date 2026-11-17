package sqlancer.risingwave.ast;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import sqlancer.Randomly;
import sqlancer.risingwave.RisingWaveType;

public enum RisingWaveAggregateFunction {
    COUNT, SUM, AVG, MIN, MAX, BOOL_AND, BOOL_OR, STRING_AGG;

    private static final RisingWaveAggregateFunction[] TLP_FUNCTIONS = Arrays.stream(values())
            .filter(f -> f.getTlpCombiner() != null).toArray(RisingWaveAggregateFunction[]::new);

    public static RisingWaveAggregateFunction randomForTlp() {
        return Randomly.fromOptions(TLP_FUNCTIONS);
    }

    public String getName() {
        return name().toLowerCase(Locale.ROOT);
    }

    public RisingWaveType resultType(List<RisingWaveExpression> arguments) {
        if (this == COUNT) {
            return RisingWaveType.BIGINT;
        }
        RisingWaveType input = arguments.get(0).getType();
        switch (this) {
        case SUM:
            return input == RisingWaveType.SMALLINT || input == RisingWaveType.INT ? RisingWaveType.BIGINT
                    : RisingWaveType.DECIMAL;
        case AVG:
            return RisingWaveType.DECIMAL;
        default:
            return input;
        }
    }

    public RisingWaveAggregateFunction getTlpCombiner() {
        switch (this) {
        case COUNT:
            return SUM;
        case AVG:
        case STRING_AGG:
            return null;
        default:
            return this;
        }
    }

    public boolean acceptsTlpInput(RisingWaveType type) {
        return !type.floatingPoint() && !type.array() && !type.struct() && (this != SUM || type.numeric())
                && (this != MIN && this != MAX || type != RisingWaveType.BOOLEAN && type != RisingWaveType.INTERVAL);
    }

    public boolean requiresBooleanInput() {
        return this == BOOL_AND || this == BOOL_OR;
    }

    public boolean allowsStar() {
        return this == COUNT;
    }

    public boolean requiresOrdering() {
        return this == STRING_AGG;
    }

    void validate(List<RisingWaveExpression> arguments, boolean distinct, RisingWaveExpression filter,
            List<RisingWaveAggregateOrder> orderBy) {
        if (arguments.isEmpty() && (!allowsStar() || distinct || !orderBy.isEmpty())
                || filter != null && filter.getType() != RisingWaveType.BOOLEAN
                || distinct && orderBy.stream().anyMatch(o -> !arguments.contains(o.expression))
                || distinct && arguments.stream().skip(1).anyMatch(a -> !(a instanceof RisingWaveConstant))) {
            throw new IllegalArgumentException("Invalid aggregate modifiers");
        }
        if (requiresOrdering()
                && (arguments.size() != 2 || arguments.stream().anyMatch(a -> a.getType() != RisingWaveType.TEXT)
                        || !(arguments.get(1) instanceof RisingWaveConstant)
                        || orderBy.stream().noneMatch(o -> o.expression == arguments.get(0)))) {
            throw new IllegalArgumentException("STRING_AGG requires a constant delimiter and input ordering");
        }
    }
}
