package sqlancer.simple;

public interface SignalResponse {
    String getReturnType();

    class ExpressionResponse implements SignalResponse {
        Expression expression;

        public ExpressionResponse(Expression expression) {
            this.expression = expression;
        }

        @Override
        public String getReturnType() {
            return "expression";
        }

        public Expression getExpression() {
            return expression;
        }
    }

    class StringResponse implements SignalResponse {
        String string;

        public StringResponse(String string) {
            this.string = string;
        }

        @Override
        public String getReturnType() {
            return "string";
        }

        public String getString() {
            return string;
        }
    }

    class ColumnResponse implements SignalResponse {
        String string;

        public ColumnResponse(String columnName, String tableName) {
            this.string = string;
        }

        @Override
        public String getReturnType() {
            return "string";
        }

        public String getString() {
            return string;
        }
    }
}
