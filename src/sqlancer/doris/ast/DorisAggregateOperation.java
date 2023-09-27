package sqlancer.doris.ast;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.doris.visitor.DorisExprToNode;

public class DorisAggregateOperation extends
        NewFunctionNode<DorisExpression, DorisAggregateOperation.DorisAggregateFunction> implements DorisExpression {

    public DorisAggregateOperation(List<DorisExpression> args, DorisAggregateFunction func) {
        super(DorisExprToNode.casts(args), func);
    }

    public enum DorisAggregateFunction {
        COLLECT_SET(1), MIN(1), STDDEV_SAMP(1), AVG(1), AVG_WEIGHTED(2), PERCENTILE(1), PERCENTILE_ARRAY(2),
        HLL_UNION_AGG(1), TOPN(2), TOPN_ARRAY(2), TOPN_WEIGHTED(3), COUNT(1), SUM(1), MAX_BY(2), BITMAP_UNION(1),
        GROUP_BITMAP_XOR(1), GROUP_BIT_ADD(1), GROUP_BIT_OR(1), GROUP_BIT_XOR(1), PERCENTILE_APPROX(2), STDDEV(1),
        STDDEV_POP(1), GROUP_CONCAT(1), COLLECT_LIST(1), MIN_BY(2), MAX(1), ANY_VALUE(1), VAR_SAMP(1), VARIANCE_SAMP(1),
        APPROX_COUNT_DISTINCT(1), VARIANCE(1), VAR_POP(1), VARIANCE_POP(1), GROUPING(1), GROUPING_ID(1);
        // RETENTION(1), SEQUENCE_MATCH(1), SEQUENCE_COUNT(1), // TODO，not currently considered

        private int nrArgs;

        DorisAggregateFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        public static DorisAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs;
        }

    }
}
