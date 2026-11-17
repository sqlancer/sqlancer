package sqlancer.risingwave.ast;

import java.util.List;

import sqlancer.risingwave.RisingWaveType;

public final class RisingWaveOperation implements RisingWaveExpression {
    public enum Operator {
        EQUALS("="), NOT_EQUALS("<>"), LESS("<"), LESS_EQUALS("<="), GREATER(">"), GREATER_EQUALS(">="), AND("AND"),
        OR("OR"), NOT("NOT"), IS_NULL("IS NULL"), ADD("+"), SUBTRACT("-"), MULTIPLY("*"), MODULO("%"), CONCAT("||"),
        LIKE("LIKE"), BETWEEN("BETWEEN"), NOT_BETWEEN("NOT BETWEEN"), IN("IN"), NOT_IN("NOT IN");

        private final String text;

        Operator(String text) {
            this.text = text;
        }

        public String getText() {
            return text;
        }
    }

    public final Operator operator;
    public final List<RisingWaveExpression> arguments;
    private final RisingWaveType type;

    public RisingWaveOperation(Operator operator, RisingWaveType type, RisingWaveExpression... arguments) {
        this.operator = operator;
        this.type = type;
        this.arguments = List.of(arguments);
    }

    @Override
    public RisingWaveType getType() {
        return type;
    }
}
