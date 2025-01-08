package sqlancer.mysql.ast;

import sqlancer.Randomly;

public class MySQLAggregate implements MySQLExpression {
    
    public enum MySQLAggregateFunction {
        // See https://dev.mysql.com/doc/refman/8.4/en/aggregate-functions.html#function_count.
        COUNT("DISTINCT"),
        // See https://dev.mysql.com/doc/refman/8.4/en/aggregate-functions.html#function_sum.
        SUM("DISTINCT"),
        // See https://dev.mysql.com/doc/refman/8.4/en/aggregate-functions.html#function_min.
        MIN("DISTINCT"),
        // See https://dev.mysql.com/doc/refman/8.4/en/aggregate-functions.html#function_max.
        MAX("DISTINCT");

        private final String[] options;

        private MySQLAggregateFunction(String... options) {
            this.options = options.clone();
        }

        public String getRandomOption() {
            if (options.length == 0 || Randomly.getBoolean()) {
                return "";
            }

            return Randomly.fromOptions(options);
        }
    }

    private final MySQLExpression expr;
    private final MySQLAggregateFunction func;

    public MySQLAggregate(MySQLExpression expr, MySQLAggregateFunction func) {
        this.expr = expr;
        this.func = func;
    }

    public MySQLExpression getExpr() {
        return expr;
    }

    public MySQLAggregateFunction getFunc() {
        return func;
    }
}
