package sqlancer.mysql.ast;

import java.util.List;
import sqlancer.Randomly;

public class MySQLAggregate implements MySQLExpression {

    public enum MySQLAggregateFunction {
        // See https://dev.mysql.com/doc/refman/8.4/en/aggregate-functions.html#function_count.
        COUNT("COUNT", null, false), COUNT_DISTINCT("COUNT", "DISTINCT", true),
        // See https://dev.mysql.com/doc/refman/8.4/en/aggregate-functions.html#function_sum.
        SUM("SUM", null, false), SUM_DISTINCT("SUM", "DISTINCT", false),
        // See https://dev.mysql.com/doc/refman/8.4/en/aggregate-functions.html#function_min.
        MIN("MIN", null, false), MIN_DISTINCT("MIN", "DISTINCT", false),
        // See https://dev.mysql.com/doc/refman/8.4/en/aggregate-functions.html#function_max.
        MAX("MAX", null, false), MAX_DISTINCT("MAX", "DISTINCT", false),
        // See https://dev.mysql.com/doc/refman/8.4/en/aggregate-functions.html#function_avg.
        AVG("AVG", null, false), AVG_DISTINCT("AVG", "DISTINCT", false),
        // See https://dev.mysql.com/doc/refman/8.4/en/aggregate-functions.html#function_group-concat.
        GROUP_CONCAT("GROUP_CONCAT", null, true), GROUP_CONCAT_DISTINCT("GROUP_CONCAT", "DISTINCT", true);

        private final String name;
        private final String option;
        private final boolean isVariadic;

        MySQLAggregateFunction(String name, String option, boolean isVariadic) {
            this.name = name;
            this.option = option;
            this.isVariadic = isVariadic;
        }

        public String getName() {
            return this.name;
        }

        public String getOption() {
            return option;
        }

        public boolean isVariadic() {
            return this.isVariadic;
        }

        public static MySQLAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    private final List<MySQLExpression> exprs;
    private final MySQLAggregateFunction func;
    private String windowSpecification;

    public MySQLAggregate(List<MySQLExpression> exprs, MySQLAggregateFunction func) {
        this.exprs = exprs;
        this.func = func;
    }

    public List<MySQLExpression> getExprs() {
        return exprs;
    }

    public MySQLAggregateFunction getFunc() {
        return func;
    }

    public void setWindowSpecification(String windowSpec) {
        this.windowSpecification = windowSpec;
    }


    public boolean isWindowFunction() {
        return windowSpecification != null;
    }
}
