package sqlancer.mysql.ast;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.FunctionNode;
import sqlancer.mysql.ast.MySQLAggregate.MySQLAggregateFunction;

public class MySQLAggregate extends FunctionNode<MySQLAggregateFunction, MySQLExpression> implements MySQLExpression {

    public enum MySQLAggregateFunction {
        AVG(1), BIT_AND(1), BIT_OR(1), COUNT(1), SUM(1), MIN(1), MAX(1);

        private int nrArgs;

        MySQLAggregateFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        public static MySQLAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs;
        }

    }

    public MySQLAggregate(List<MySQLExpression> args, MySQLAggregateFunction func) {
        super(func, args);
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return MySQLConstant.createNullConstant();
    }
}
