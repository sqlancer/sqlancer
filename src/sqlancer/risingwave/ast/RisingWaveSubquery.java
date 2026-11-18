package sqlancer.risingwave.ast;

import java.util.List;

import sqlancer.risingwave.RisingWaveType;
import sqlancer.risingwave.ast.RisingWaveOperation.Operator;

public final class RisingWaveSubquery implements RisingWaveExpression {
    public enum Kind {
        EXISTS, IN, SCALAR
    }

    public final Kind kind;
    public final RisingWaveSelect query;
    public final RisingWaveExpression operand;
    public final List<RisingWaveColumnReference> outerColumns;

    public RisingWaveSubquery(Kind kind, RisingWaveSelect query, RisingWaveExpression operand) {
        this(kind, query, operand, List.of());
    }

    public RisingWaveSubquery(Kind kind, RisingWaveSelect query, RisingWaveExpression operand,
            List<RisingWaveColumnReference> outerColumns) {
        if (outerColumns.size() > 1 || !outerColumns.isEmpty()
                && (kind != Kind.EXISTS || !hasCorrelation(query.getWhereClause(), outerColumns.get(0)))) {
            throw new IllegalArgumentException("Correlation requires one equality-bound outer column in EXISTS");
        }
        if (kind != Kind.EXISTS && query.getFetchColumns().size() != 1) {
            throw new IllegalArgumentException("RisingWaveSubquery must project one column");
        }
        if (kind == Kind.IN && (operand == null || operand.getType() != query.getType())) {
            throw new IllegalArgumentException("IN types must match");
        }
        this.kind = kind;
        this.query = query;
        this.operand = operand;
        this.outerColumns = List.copyOf(outerColumns);
    }

    private static boolean hasCorrelation(RisingWaveExpression predicate, RisingWaveColumnReference outer) {
        if (!(predicate instanceof RisingWaveOperation)) {
            return false;
        }
        RisingWaveOperation operation = (RisingWaveOperation) predicate;
        if (operation.operator == Operator.AND) {
            return operation.arguments.stream().anyMatch(p -> hasCorrelation(p, outer));
        }
        return operation.operator == Operator.EQUALS && operation.arguments.size() == 2
                && operation.arguments.contains(outer)
                && operation.arguments.stream()
                        .anyMatch(a -> a instanceof RisingWaveColumnReference
                                && !((RisingWaveColumnReference) a).qualifier.equals(outer.qualifier)
                                && a.getType() == outer.getType());
    }

    @Override
    public RisingWaveType getType() {
        return kind == Kind.SCALAR ? query.getType() : RisingWaveType.BOOLEAN;
    }
}
