package sqlancer.stonedb.ast;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.FunctionNode;
import sqlancer.stonedb.ast.StoneDBAggregate.StoneDBAggregateFunction;

public class StoneDBAggregate extends FunctionNode<StoneDBAggregateFunction, StoneDBExpression>
        implements StoneDBExpression {

    // https://stonedb.io/docs/SQL-reference/functions/aggregate-functions/
    public enum StoneDBAggregateFunction {
        MAX(1), MIN(1), AVG(1), COUNT(1), SUM(1);

        private int nrArgs;

        StoneDBAggregateFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        public static StoneDBAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs;
        }
    }

    protected StoneDBAggregate(StoneDBAggregateFunction function, List<StoneDBExpression> args) {
        super(function, args);
    }
}
