package sqlancer.stonedb.ast;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.FunctionNode;
import sqlancer.stonedb.ast.StoneDBAdvancedFunction.StoneDBAdvancedFunc;

public class StoneDBAdvancedFunction extends FunctionNode<StoneDBAdvancedFunc, StoneDBExpression>
        implements StoneDBExpression {

    // https://stonedb.io/docs/SQL-reference/functions/advanced-functions
    public enum StoneDBAdvancedFunc {
        IFNULL(2), IF(3), NULLIF(2), BIN(1), BINARY(1), CONV(3);

        private int nrArgs;

        StoneDBAdvancedFunc(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        public static StoneDBAdvancedFunc getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs;
        }
    }

    protected StoneDBAdvancedFunction(StoneDBAdvancedFunc function, List<StoneDBExpression> args) {
        super(function, args);
    }
}
