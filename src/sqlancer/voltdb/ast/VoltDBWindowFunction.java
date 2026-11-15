package sqlancer.voltdb.ast;

import java.util.List;

/**
 * A windowed function call: {@code f([arg]) OVER ([PARTITION BY p, ...] [ORDER BY o [DESC], ...])}. VoltDB takes one
 * per SELECT list, not with GROUP BY, and orders a window only by integer and TIMESTAMP values. With an ORDER BY, an
 * aggregate covers the rows of the partition up to the current row and its peers; without one, the whole partition.
 */
public final class VoltDBWindowFunction implements VoltDBExpression {

    public enum Function {
        RANK(false), DENSE_RANK(false), ROW_NUMBER(false), COUNT(true), SUM(true), MIN(true), MAX(true);

        private final boolean aggregate;

        Function(boolean aggregate) {
            this.aggregate = aggregate;
        }

        // COUNT, SUM, MIN and MAX; the others take no argument
        public boolean isAggregate() {
            return aggregate;
        }

        // RANK and DENSE_RANK need an ORDER BY
        public boolean needsOrderBy() {
            return this == RANK || this == DENSE_RANK;
        }
    }

    private final Function function;
    private final VoltDBExpression argument;
    private final List<VoltDBExpression> partitionBy;
    private final List<VoltDBExpression> orderBy;
    private final List<Boolean> descending;

    // argument is null for COUNT(*) and the ranking functions; descending says, for each ORDER BY term, whether it is
    // DESC
    public VoltDBWindowFunction(Function function, VoltDBExpression argument, List<VoltDBExpression> partitionBy,
            List<VoltDBExpression> orderBy, List<Boolean> descending) {
        if (orderBy.size() != descending.size() || function.needsOrderBy() && orderBy.isEmpty()) {
            throw new IllegalArgumentException();
        }
        this.function = function;
        this.argument = argument;
        this.partitionBy = List.copyOf(partitionBy);
        this.orderBy = List.copyOf(orderBy);
        this.descending = List.copyOf(descending);
    }

    public Function getFunction() {
        return function;
    }

    public VoltDBExpression getArgument() {
        return argument;
    }

    public List<VoltDBExpression> getPartitionBy() {
        return partitionBy;
    }

    public List<VoltDBExpression> getOrderBy() {
        return orderBy;
    }

    public List<Boolean> getDescending() {
        return descending;
    }

}
