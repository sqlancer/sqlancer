package sqlancer.mysql.ast;

import java.util.List;

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

        private final List<String> options;

        private MySQLAggregateFunction(String... options) {
            this.options = List.of(options);
        }

        public List<String> getOptions() {
            return options;
        }
    }

    private final MySQLExpression expr;
    private final MySQLAggregateFunction func;
    private final String option;

    public MySQLAggregate(MySQLExpression expr, MySQLAggregateFunction func, String option) {
        this.expr = expr;
        this.func = func;
        this.option = option;
    }

    public MySQLExpression getExpr() {
        return expr;
    }

    public MySQLAggregateFunction getFunc() {
        return func;
    }

    public String getOption() {
        return option;
    }
}
