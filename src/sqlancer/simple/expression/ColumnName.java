package sqlancer.simple.expression;

import java.util.List;

import sqlancer.simple.Expression;
import sqlancer.simple.Signal;
import sqlancer.simple.SignalResponse;

public class ColumnName implements Expression {
    String columnName;
    String tableName;

    public ColumnName(String columnName, String tableName) {
        this.columnName = columnName;
        this.tableName = tableName;
    }

    @Override
    public String parse() {
        return tableName + "." + columnName;
    }

    public static class Op implements Operation {

        static final List<Signal> signals = List.of(Signal.COLUMN_NAME);

        @Override
        public List<Signal> getRequestSignals() {
            return signals;
        }

        @Override
        public Expression create(List<SignalResponse> innerExpressions) {
            assert innerExpressions.size() == getRequestSignals()
                    .size() : "Column operation has 1 operand, but received " + innerExpressions.size();

            return ((SignalResponse.ExpressionResponse) innerExpressions.get(0)).getExpression();
        }
    }

}
