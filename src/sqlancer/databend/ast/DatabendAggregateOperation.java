package sqlancer.databend.ast;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.databend.DatabendExprToNode;
import sqlancer.databend.DatabendSchema;

public class DatabendAggregateOperation
        extends NewFunctionNode<DatabendExpression, DatabendAggregateOperation.DatabendAggregateFunction>
        implements DatabendExpression {
    public DatabendAggregateOperation(List<DatabendExpression> args, DatabendAggregateFunction func) {
        super(DatabendExprToNode.casts(args), func);
    }

    public enum DatabendAggregateFunction {
        MAX(1), MIN(1), AVG(1, DatabendSchema.DatabendDataType.INT, DatabendSchema.DatabendDataType.FLOAT), COUNT(1),
        SUM(1, DatabendSchema.DatabendDataType.INT, DatabendSchema.DatabendDataType.FLOAT), STDDEV_POP(1), COVAR_POP(1),
        COVAR_SAMP(2);
        // , *_IF, *_DISTINCT

        private int nrArgs;
        private DatabendSchema.DatabendDataType[] dataTypes;

        DatabendAggregateFunction(int nrArgs, DatabendSchema.DatabendDataType... dataTypes) {
            this.nrArgs = nrArgs;
            this.dataTypes = dataTypes.clone();
        }

        public static DatabendAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public DatabendSchema.DatabendDataType getRandomType() {
            if (dataTypes.length == 0) {
                return Randomly.fromOptions(DatabendSchema.DatabendDataType.values());
            } else {
                return Randomly.fromOptions(dataTypes);
            }
        }

        public int getNrArgs() {
            return nrArgs;
        }

    }

}
