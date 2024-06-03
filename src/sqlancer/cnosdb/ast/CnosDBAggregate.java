package sqlancer.cnosdb.ast;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.ast.CnosDBAggregate.CnosDBAggregateFunction;
import sqlancer.common.ast.FunctionNode;

public class CnosDBAggregate extends FunctionNode<CnosDBAggregateFunction, CnosDBExpression>
        implements CnosDBExpression {

    public CnosDBAggregate(List<CnosDBExpression> args, CnosDBAggregateFunction func) {
        super(func, args);
    }

    public enum CnosDBAggregateFunction {
        // the first argument is the arg num of the aggregate function, the rest are the types of the arguments if limited
        // TODO: Add more supported aggregate functions, now only a few are supported(min max sum)

        // comman
        MAX(1), MIN(1), SUM(1, CnosDBDataType.INT, CnosDBDataType.DOUBLE) ,COUNT(1), MEAN(1),  MEDIAN(1), 
        AVG(1), 
        // order
        ARRAY_AGG(1), FIRST_VALUE(1), FISRT(2), LAST_VALUE(1), LAST(2), MODE(1), INCREASE(2),
        
        // statistics
        CORR(2), COVAR(2), COVAR_SAMP(2), COVAR_POP(2), 
        STDDEV(1), STDDEV_SAMP(1), STDDEV_POP(1),
        VAR(1), VAR_SAMP(1), VAR_POP(1),

        // approximate
        APPROX_MEDIAN(1),
        APPROX_PERCENTILE_CONT(3),
        APPROX_PERCENTILE_CONT_WITH_WEIGHT(3), APPROX_DISTINCT(1), SAMPLE(2);


        private int nrArgs;
        private CnosDBDataType[] supportedTypes;
        
        CnosDBAggregateFunction(int nrArgs, CnosDBDataType... supportedTypes) {
            this.nrArgs = nrArgs;
            this.supportedTypes = supportedTypes;
        }

        public CnosDBDataType getRandomType() {
            if(supportedTypes.length == 0) {
                return Randomly.fromOptions(CnosDBDataType.values());
            }else{
                return Randomly.fromOptions(supportedTypes);
            }
        }

        public static CnosDBAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs;
        }
    }

}
